/*
 * sysbench-style OLTP Read/Write Benchmark for SQLite-compatible databases
 * Implements oltp_read_write: 10 point SELECTs, 1 range SELECT, 2 UPDATEs,
 * 1 DELETE, 1 INSERT per transaction.
 * Compile: gcc -O2 -o oltp_<target> oltp_read_write.c -I./include -lsqlite3
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "sqlite3.h"

/* ---------- config ---------- */
#ifndef TABLE_SIZE
#define TABLE_SIZE 10000
#endif
#ifndef TXN_COUNT
#define TXN_COUNT 1000
#endif

/* sysbench c column: 120-char string of groups "###########-###########-..." */
#define C_LEN  120
/* sysbench pad column: 60-char string */
#define PAD_LEN 60

/* ---------- timing ---------- */
static double now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

/* ---------- helpers ---------- */
static void die(const char *msg, sqlite3 *db) {
    fprintf(stderr, "FATAL: %s — %s\n", msg, db ? sqlite3_errmsg(db) : "no db");
    if (db) sqlite3_close(db);
    exit(1);
}

static void exec(sqlite3 *db, const char *sql) {
    char *err = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n  %.120s\n", err, sql);
        sqlite3_free(err);
    }
}

static void random_string(char *buf, int len) {
    static const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    for (int i = 0; i < len; i++)
        buf[i] = charset[rand() % (sizeof(charset) - 1)];
    buf[len] = '\0';
}

/* ---------- comparison for qsort ---------- */
static int cmp_double(const void *a, const void *b) {
    double da = *(const double *)a;
    double db = *(const double *)b;
    return (da > db) - (da < db);
}

/* ---------- main ---------- */
int main(int argc, char **argv) {
    int table_size = (argc > 1) ? atoi(argv[1]) : TABLE_SIZE;
    int txn_count  = (argc > 2) ? atoi(argv[2]) : TXN_COUNT;
    const char *dbpath = (argc > 3) ? argv[3] : ":memory:";

    /* derive a human-readable label from the db path */
    const char *label = dbpath;
    {
        const char *slash = strrchr(dbpath, '/');
        if (slash) label = slash + 1;
    }

    srand(42); /* deterministic */

    printf("=== OLTP Read/Write: %s ===\n", label);
    printf("Table size: %d, Transactions: %d\n", table_size, txn_count);
    fflush(stdout);

    sqlite3 *db;
    if (sqlite3_open(dbpath, &db) != SQLITE_OK)
        die("open", db);

    exec(db, "PRAGMA journal_mode=WAL");
    exec(db, "PRAGMA synchronous=NORMAL");
    /* sqlite3_busy_timeout is not available; use PRAGMA instead */
    exec(db, "PRAGMA busy_timeout=60000");

    /* schema */
    exec(db, "DROP TABLE IF EXISTS sbtest");
    exec(db, "CREATE TABLE sbtest ("
             "id  INTEGER PRIMARY KEY,"
             "k   INTEGER NOT NULL DEFAULT 0,"
             "c   TEXT    NOT NULL DEFAULT '',"
             "pad TEXT    NOT NULL DEFAULT '')");
    exec(db, "CREATE INDEX k_1 ON sbtest(k)");

    /* load phase */
    printf("...loading...\n");
    fflush(stdout);
    {
        exec(db, "BEGIN");
        char c_buf[C_LEN + 1];
        char pad_buf[PAD_LEN + 1];
        /* buffer sized for: INSERT INTO sbtest (id, k, c, pad) VALUES (id, k, 'c...', 'pad...') */
        char sql[C_LEN + PAD_LEN + 128];
        for (int i = 1; i <= table_size; i++) {
            int k = rand() % table_size;
            random_string(c_buf, C_LEN);
            random_string(pad_buf, PAD_LEN);
            snprintf(sql, sizeof(sql),
                "INSERT INTO sbtest (id, k, c, pad) VALUES (%d, %d, '%s', '%s')",
                i, k, c_buf, pad_buf);
            exec(db, sql);
        }
        exec(db, "COMMIT");
    }

    /* track the next id to use for DELETE+INSERT cycling */
    int next_id = table_size + 1;

    /* per-transaction latency array */
    double *latencies = (double *)malloc(txn_count * sizeof(double));
    if (!latencies) die("malloc latencies", NULL);

    long long total_reads  = 0;
    long long total_writes = 0;

    double bench_t0 = now_ms();

    for (int t = 0; t < txn_count; t++) {
        double txn_t0 = now_ms();
        char sql[C_LEN + PAD_LEN + 128];

        exec(db, "BEGIN");

        /* 10 point SELECTs by PK */
        for (int i = 0; i < 10; i++) {
            int id = 1 + rand() % table_size;
            snprintf(sql, sizeof(sql),
                "SELECT id, k, c, pad FROM sbtest WHERE id=%d", id);
            exec(db, sql);
        }
        total_reads += 10;

        /* 1 range SELECT: k BETWEEN x AND x+99 ORDER BY k LIMIT 100 */
        {
            int k_lo = rand() % table_size;
            int k_hi = k_lo + 99;
            snprintf(sql, sizeof(sql),
                "SELECT id, k, c, pad FROM sbtest "
                "WHERE k BETWEEN %d AND %d ORDER BY k LIMIT 100",
                k_lo, k_hi);
            exec(db, sql);
        }
        total_reads += 1;

        /* UPDATE k=k+1 by PK */
        {
            int id = 1 + rand() % table_size;
            snprintf(sql, sizeof(sql),
                "UPDATE sbtest SET k=k+1 WHERE id=%d", id);
            exec(db, sql);
        }
        total_writes += 1;

        /* UPDATE c=? by PK */
        {
            int id = 1 + rand() % table_size;
            char c_buf[C_LEN + 1];
            random_string(c_buf, C_LEN);
            snprintf(sql, sizeof(sql),
                "UPDATE sbtest SET c='%s' WHERE id=%d", c_buf, id);
            exec(db, sql);
        }
        total_writes += 1;

        /* DELETE by PK */
        {
            int id = 1 + rand() % table_size;
            snprintf(sql, sizeof(sql),
                "DELETE FROM sbtest WHERE id=%d", id);
            exec(db, sql);
        }
        total_writes += 1;

        /* INSERT (use next_id to keep table from shrinking indefinitely) */
        {
            int k = rand() % table_size;
            char c_buf[C_LEN + 1];
            char pad_buf[PAD_LEN + 1];
            random_string(c_buf, C_LEN);
            random_string(pad_buf, PAD_LEN);
            snprintf(sql, sizeof(sql),
                "INSERT INTO sbtest (id, k, c, pad) VALUES (%d, %d, '%s', '%s')",
                next_id++, k, c_buf, pad_buf);
            exec(db, sql);
        }
        total_writes += 1;

        exec(db, "COMMIT");

        latencies[t] = now_ms() - txn_t0;

        /* 60s wall-clock timeout across all transactions */
        if (now_ms() - bench_t0 > 60000.0) {
            txn_count = t + 1; /* trim to completed count */
            fprintf(stderr, "WARNING: 60s timeout reached after %d transactions\n", txn_count);
            break;
        }
    }

    double bench_elapsed = now_ms() - bench_t0;

    sqlite3_close(db);

    /* sort latencies for percentiles */
    qsort(latencies, txn_count, sizeof(double), cmp_double);

    double p50 = latencies[(int)(txn_count * 0.50)];
    double p95 = latencies[(int)(txn_count * 0.95)];
    double p99 = latencies[(int)(txn_count * 0.99)];
    double pmax = latencies[txn_count - 1];

    free(latencies);

    double elapsed_s  = bench_elapsed / 1000.0;
    double txn_per_s  = txn_count  / elapsed_s;
    double reads_per_s  = total_reads  / elapsed_s;
    double writes_per_s = total_writes / elapsed_s;

    printf("  txn/s:    %.0f\n",   txn_per_s);
    printf("  reads/s:  %.0f\n",   reads_per_s);
    printf("  writes/s: %.0f\n",   writes_per_s);
    printf("  latency p50: %.2f ms\n", p50);
    printf("  latency p95: %.2f ms\n", p95);
    printf("  latency p99: %.2f ms\n", p99);
    printf("  latency max: %.2f ms\n", pmax);
    printf("Duration: %.1f ms\n", bench_elapsed);

    return 0;
}
