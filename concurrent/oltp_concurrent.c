/*
 * sysbench OLTP Read/Write Concurrent Benchmark for SQLite-compatible databases
 *
 * Measures MVCC scalability: N threads running oltp_read_write transactions
 * (10 point SELECTs, 1 range SELECT, 1 UPDATE k, 1 UPDATE c, 1 DELETE, 1 INSERT)
 * against a shared database. Reports throughput at each thread count (1, 2, 4, 8)
 * plus retry/abort counts.
 *
 * Two modes:
 *   - autocommit (WAL):        each txn uses BEGIN/COMMIT
 *   - BEGIN CONCURRENT:        explicit txns with BEGIN CONCURRENT (MVCC)
 *
 * Compile (system SQLite):
 *   gcc -O2 oltp_concurrent.c -I./include -lsqlite3 -lpthread -o oltp_concurrent_sqlite
 *
 * Compile (frankensqlite):
 *   gcc -O2 oltp_concurrent.c -I./include -L$KQSQL -lfsqlite_c_api -lpthread \
 *       -Wl,-rpath,$KQSQL -o oltp_concurrent_fsqlite
 *
 * Usage:
 *   ./oltp_concurrent_<target> [db_path] [mode]
 *   mode: "auto" (default) or "concurrent" (uses BEGIN CONCURRENT)
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>
#include <stdatomic.h>
#include "sqlite3.h"

/* sqlite3_sleep may not be exported by all implementations */
static void sleep_ms(int ms) { usleep(ms * 1000); }

/* ---------- config ---------- */
#ifndef TABLE_SIZE
#define TABLE_SIZE 10000
#endif
#define TXNS_PER_THREAD  500
#define MAX_THREADS      8
#define THREAD_COUNTS    4   /* test at 1, 2, 4, 8 */

/* sysbench column widths */
#define C_LEN    120
#define PAD_LEN   60

static const int thread_counts[THREAD_COUNTS] = {1, 2, 4, 8};

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

static int exec_rc(sqlite3 *db, const char *sql) {
    char *err = NULL;
    int rc = sqlite3_exec(db, sql, NULL, NULL, &err);
    if (err) sqlite3_free(err);
    return rc;
}

static void exec(sqlite3 *db, const char *sql) {
    char *err = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n  %.120s\n", err, sql);
        sqlite3_free(err);
    }
}

static void random_string_r(char *buf, int len, unsigned int *seed) {
    static const char charset[] =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    for (int i = 0; i < len; i++)
        buf[i] = charset[rand_r(seed) % (sizeof(charset) - 1)];
    buf[len] = '\0';
}

/* ---------- prepared statements (parameter-free SELECTs only) ---------- */
/*
 * select_pk and select_range take parameters, so they are executed via
 * snprintf+exec like all write statements.  No bind API is used.
 */

/* ---------- thread context ---------- */
typedef struct {
    int thread_id;
    const char *dbpath;
    int use_concurrent;    /* 1 = BEGIN CONCURRENT, 0 = BEGIN */
    int txns;
    int table_size;

    /* results */
    double elapsed_ms;
    long completed_txns;
    long retries;          /* SQLITE_BUSY retries on BEGIN or COMMIT */
    long aborts;           /* txn aborts (conflict on COMMIT) */
} thread_ctx_t;

/* ---------- worker ---------- */
static void *worker(void *arg) {
    thread_ctx_t *ctx = (thread_ctx_t *)arg;
    sqlite3 *db;
    unsigned int seed = (unsigned int)(42 + ctx->thread_id * 7);

    if (sqlite3_open(ctx->dbpath, &db) != SQLITE_OK)
        die("worker open", db);

    exec(db, "PRAGMA journal_mode=WAL");
    exec(db, "PRAGMA synchronous=NORMAL");
    exec(db, "PRAGMA busy_timeout=5000");

    ctx->completed_txns = 0;
    ctx->retries = 0;
    ctx->aborts  = 0;

    /* each thread tracks its own next_id for INSERT cycling */
    int next_id = ctx->table_size + 1 + ctx->thread_id * ctx->txns;

    /* reusable SQL buffer — large enough for any statement */
    char sql[C_LEN + PAD_LEN + 256];

    double t0 = now_ms();

    for (int t = 0; t < ctx->txns; t++) {
        const char *begin_sql = ctx->use_concurrent ? "BEGIN CONCURRENT" : "BEGIN";

        int committed = 0;
        for (int attempt = 0; attempt < 20 && !committed; attempt++) {
            int rc = exec_rc(db, begin_sql);
            if (rc == SQLITE_BUSY) {
                ctx->retries++;
                sleep_ms(1);
                continue;
            }
            if (rc != SQLITE_OK) {
                /* BEGIN CONCURRENT not supported — fall back to BEGIN */
                exec(db, "BEGIN");
            }

            /* 10 point SELECTs by PK */
            for (int i = 0; i < 10; i++) {
                int id = 1 + rand_r(&seed) % ctx->table_size;
                snprintf(sql, sizeof(sql),
                    "SELECT id, k, c, pad FROM sbtest WHERE id=%d", id);
                exec(db, sql);
            }

            /* 1 range SELECT: k BETWEEN x AND x+99 ORDER BY k LIMIT 100 */
            {
                int k_lo = rand_r(&seed) % ctx->table_size;
                int k_hi = k_lo + 99;
                snprintf(sql, sizeof(sql),
                    "SELECT id, k, c, pad FROM sbtest "
                    "WHERE k BETWEEN %d AND %d ORDER BY k LIMIT 100",
                    k_lo, k_hi);
                exec(db, sql);
            }

            /* UPDATE k=k+1 by PK */
            {
                int id = 1 + rand_r(&seed) % ctx->table_size;
                snprintf(sql, sizeof(sql),
                    "UPDATE sbtest SET k=k+1 WHERE id=%d", id);
                exec(db, sql);
            }

            /* UPDATE c=? by PK */
            {
                int id = 1 + rand_r(&seed) % ctx->table_size;
                char c_buf[C_LEN + 1];
                random_string_r(c_buf, C_LEN, &seed);
                snprintf(sql, sizeof(sql),
                    "UPDATE sbtest SET c='%s' WHERE id=%d", c_buf, id);
                exec(db, sql);
            }

            /* DELETE by PK */
            {
                int id = 1 + rand_r(&seed) % ctx->table_size;
                snprintf(sql, sizeof(sql),
                    "DELETE FROM sbtest WHERE id=%d", id);
                exec(db, sql);
            }

            /* INSERT */
            {
                int k = rand_r(&seed) % ctx->table_size;
                char c_buf[C_LEN + 1];
                char pad_buf[PAD_LEN + 1];
                random_string_r(c_buf, C_LEN, &seed);
                random_string_r(pad_buf, PAD_LEN, &seed);
                snprintf(sql, sizeof(sql),
                    "INSERT INTO sbtest (id, k, c, pad) VALUES (%d, %d, '%s', '%s')",
                    next_id++, k, c_buf, pad_buf);
                exec(db, sql);
            }

            rc = exec_rc(db, "COMMIT");
            if (rc == SQLITE_OK) {
                committed = 1;
            } else if (rc == SQLITE_BUSY || rc == SQLITE_LOCKED) {
                exec_rc(db, "ROLLBACK");
                ctx->aborts++;
            } else {
                exec_rc(db, "ROLLBACK");
                ctx->aborts++;
            }
        }

        ctx->completed_txns++;

        /* 120s timeout */
        if (now_ms() - t0 > 120000.0) break;
    }

    ctx->elapsed_ms = now_ms() - t0;
    sqlite3_close(db);
    return NULL;
}

/* ---------- load data ---------- */
static void load_data(const char *dbpath, int table_size) {
    sqlite3 *db;
    unsigned int seed = 42;

    if (sqlite3_open(dbpath, &db) != SQLITE_OK)
        die("load open", db);

    exec(db, "PRAGMA journal_mode=WAL");
    exec(db, "PRAGMA synchronous=NORMAL");

    exec(db, "DROP TABLE IF EXISTS sbtest");
    exec(db, "CREATE TABLE sbtest ("
         "id  INTEGER PRIMARY KEY,"
         "k   INTEGER NOT NULL DEFAULT 0,"
         "c   TEXT    NOT NULL DEFAULT '',"
         "pad TEXT    NOT NULL DEFAULT '')");
    exec(db, "CREATE INDEX k_1 ON sbtest(k)");

    exec(db, "BEGIN");
    char c_buf[C_LEN + 1];
    char pad_buf[PAD_LEN + 1];
    char sql[C_LEN + PAD_LEN + 128];
    for (int i = 1; i <= table_size; i++) {
        int k = rand_r(&seed) % table_size;
        random_string_r(c_buf, C_LEN, &seed);
        random_string_r(pad_buf, PAD_LEN, &seed);
        snprintf(sql, sizeof(sql),
            "INSERT INTO sbtest (id, k, c, pad) VALUES (%d, %d, '%s', '%s')",
            i, k, c_buf, pad_buf);
        exec(db, sql);
    }
    exec(db, "COMMIT");
    sqlite3_close(db);
}

/* ---------- run one thread-count level ---------- */
static void run_level(const char *dbpath, int n_threads, int use_concurrent,
                      int table_size) {
    pthread_t threads[MAX_THREADS];
    thread_ctx_t ctxs[MAX_THREADS];

    /* reload data fresh for each level */
    load_data(dbpath, table_size);

    for (int i = 0; i < n_threads; i++) {
        ctxs[i].thread_id      = i;
        ctxs[i].dbpath         = dbpath;
        ctxs[i].use_concurrent = use_concurrent;
        ctxs[i].txns           = TXNS_PER_THREAD;
        ctxs[i].table_size     = table_size;
    }

    double wall_start = now_ms();

    for (int i = 0; i < n_threads; i++)
        pthread_create(&threads[i], NULL, worker, &ctxs[i]);

    for (int i = 0; i < n_threads; i++)
        pthread_join(threads[i], NULL);

    double wall_ms = now_ms() - wall_start;

    /* aggregate */
    long total_txns = 0, total_retries = 0, total_aborts = 0;
    for (int i = 0; i < n_threads; i++) {
        total_txns    += ctxs[i].completed_txns;
        total_retries += ctxs[i].retries;
        total_aborts  += ctxs[i].aborts;
    }

    double throughput = total_txns / (wall_ms / 1000.0);

    printf("  %2d threads  %8.1f ms  %8.0f txn/s  "
           "[txns:%ld retries:%ld aborts:%ld]\n",
           n_threads, wall_ms, throughput,
           total_txns, total_retries, total_aborts);
}

/* ---------- main ---------- */
int main(int argc, char **argv) {
    const char *dbpath = (argc > 1) ? argv[1] : "/tmp/oltp_conc.db";
    const char *mode   = (argc > 2) ? argv[2] : "auto";
    int use_concurrent = (strcmp(mode, "concurrent") == 0);
    int table_size     = TABLE_SIZE;

    printf("=== OLTP Concurrent Benchmark ===\n");
    printf("Database: %s\n", dbpath);
    printf("Mode: %s\n", use_concurrent ? "BEGIN CONCURRENT (MVCC)" : "autocommit (WAL)");
    printf("Table size: %d, Txns/thread: %d\n\n", table_size, TXNS_PER_THREAD);

    printf("--- Scaling ---\n");
    for (int i = 0; i < THREAD_COUNTS; i++) {
        fflush(stdout);
        run_level(dbpath, thread_counts[i], use_concurrent, table_size);
    }

    /* If not concurrent mode, also try concurrent mode to compare */
    if (!use_concurrent) {
        printf("\n--- Retry with BEGIN CONCURRENT ---\n");
        for (int i = 0; i < THREAD_COUNTS; i++) {
            fflush(stdout);
            run_level(dbpath, thread_counts[i], 1, table_size);
        }
    }

    printf("\nDone.\n");
    return 0;
}
