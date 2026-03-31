/*
 * YCSB-A Concurrent Benchmark for SQLite-compatible databases
 *
 * Measures MVCC scalability: N threads running Workload A (50/50 read/update)
 * with Zipfian skew against a shared database. Reports throughput at each
 * thread count (1, 2, 4, 8) plus abort/retry counts.
 *
 * Two modes:
 *   - SQLite default:       each op is autocommit (WAL single-writer)
 *   - BEGIN CONCURRENT:     explicit txns with BEGIN CONCURRENT (MVCC)
 *
 * Compile (system SQLite):
 *   gcc -O2 ycsb_concurrent.c -I./include -lsqlite3 -lpthread -lm -o ycsb_concurrent_sqlite
 *
 * Compile (frankensqlite):
 *   gcc -O2 ycsb_concurrent.c -I./include -L$KQSQL -lfsqlite_c_api -lpthread -lm \
 *       -Wl,-rpath,$KQSQL -o ycsb_concurrent_fsqlite
 *
 * Usage:
 *   ./ycsb_concurrent_<target> [db_path] [mode]
 *   mode: "auto" (default) or "concurrent" (uses BEGIN CONCURRENT)
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <math.h>
#include <pthread.h>
#include <unistd.h>
#include <stdatomic.h>
#include "sqlite3.h"

/* sqlite3_sleep may not be exported by all implementations */
static void sleep_ms(int ms) { usleep(ms * 1000); }

/* ---------- config ---------- */
#ifndef RECORD_COUNT
#define RECORD_COUNT 1000
#endif
#define OPS_PER_THREAD  2000
#define FIELD_COUNT      10
#define FIELD_LENGTH     100
#define KEY_PREFIX       "user"
#define TABLE_NAME       "usertable"
#define MAX_THREADS      32
#define THREAD_COUNTS    6   /* test at 1, 2, 4, 8, 16, 32 */

static const int thread_counts[THREAD_COUNTS] = {1, 2, 4, 8, 16, 32};

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

/* ---------- FNV-64a hash ---------- */
static unsigned long long fnv64(unsigned long long val) {
    unsigned long long h = 0xCBF29CE484222325ULL;
    for (int i = 0; i < 8; i++) {
        h ^= (val & 0xFF);
        h *= 1099511628211ULL;
        val >>= 8;
    }
    return h;
}

/* ---------- Zipfian generator ---------- */
typedef struct {
    int items;
    double theta, zetan, eta, alpha, half_pow_theta;
} zipfian_t;

static double zeta(int n, double theta) {
    double sum = 0.0;
    for (int i = 1; i <= n; i++)
        sum += 1.0 / pow((double)i, theta);
    return sum;
}

static void zipfian_init(zipfian_t *z, int items) {
    z->items = items;
    z->theta = 0.99;
    z->zetan = zeta(items, z->theta);
    z->alpha = 1.0 / (1.0 - z->theta);
    z->half_pow_theta = pow(0.5, z->theta);
    double zeta2 = zeta(2, z->theta);
    z->eta = (1.0 - pow(2.0 / items, 1.0 - z->theta)) / (1.0 - zeta2 / z->zetan);
}

static int zipfian_next_r(zipfian_t *z, unsigned int *seed) {
    double u = (double)rand_r(seed) / RAND_MAX;
    double uz = u * z->zetan;
    if (uz < 1.0) return 0;
    if (uz < 1.0 + z->half_pow_theta) return 1;
    return (int)(z->items * pow(z->eta * u - z->eta + 1.0, z->alpha));
}

static int scrambled_zipfian_next_r(zipfian_t *z, unsigned int *seed) {
    int raw = zipfian_next_r(z, seed);
    return (int)(fnv64((unsigned long long)raw) % (unsigned long long)z->items);
}

/* ---------- thread context ---------- */
typedef struct {
    int thread_id;
    const char *dbpath;
    int use_concurrent;    /* 1 = BEGIN CONCURRENT, 0 = autocommit */
    int ops;
    int record_count;

    /* results */
    double elapsed_ms;
    long completed_ops;
    long read_ops;
    long update_ops;
    long retries;          /* SQLITE_BUSY retries */
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

    zipfian_t z;
    zipfian_init(&z, ctx->record_count);

    ctx->completed_ops = 0;
    ctx->read_ops = 0;
    ctx->update_ops = 0;
    ctx->retries = 0;
    ctx->aborts = 0;

    double t0 = now_ms();

    for (int i = 0; i < ctx->ops; i++) {
        int key = scrambled_zipfian_next_r(&z, &seed);
        double r = (double)rand_r(&seed) / RAND_MAX;
        int is_update = (r >= 0.50);
        int committed = 0, done = 0;

        if (ctx->use_concurrent) {
            /* BEGIN CONCURRENT transaction with retry on conflict */
            for (int attempt = 0; attempt < 10 && !committed; attempt++) {
                int rc = exec_rc(db, "BEGIN CONCURRENT");
                if (rc == SQLITE_BUSY) {
                    ctx->retries++;
                    sleep_ms(1);
                    continue;
                }
                if (rc != SQLITE_OK) {
                    /* BEGIN CONCURRENT not supported — fall back to BEGIN */
                    fprintf(stderr, "WARNING: BEGIN CONCURRENT failed (rc=%d), falling back to BEGIN\n", rc);
                    exec(db, "BEGIN");
                }

                if (is_update) {
                    char val[FIELD_LENGTH + 1];
                    int field = rand_r(&seed) % FIELD_COUNT;
                    random_string_r(val, FIELD_LENGTH, &seed);
                    char sql[512];
                    snprintf(sql, sizeof(sql),
                        "UPDATE " TABLE_NAME " SET field%d='%s' WHERE YCSB_KEY='%s%010d'",
                        field, val, KEY_PREFIX, key);
                    exec(db, sql);
                } else {
                    char sql[256];
                    snprintf(sql, sizeof(sql),
                        "SELECT * FROM " TABLE_NAME " WHERE YCSB_KEY='%s%010d'",
                        KEY_PREFIX, key);
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
        } else {
            /* Autocommit mode — each op is its own transaction */
            for (int attempt = 0; attempt < 10 && !done; attempt++) {
                int rc;
                if (is_update) {
                    char val[FIELD_LENGTH + 1];
                    int field = rand_r(&seed) % FIELD_COUNT;
                    random_string_r(val, FIELD_LENGTH, &seed);
                    char sql[512];
                    snprintf(sql, sizeof(sql),
                        "UPDATE " TABLE_NAME " SET field%d='%s' WHERE YCSB_KEY='%s%010d'",
                        field, val, KEY_PREFIX, key);
                    rc = exec_rc(db, sql);
                } else {
                    char sql[256];
                    snprintf(sql, sizeof(sql),
                        "SELECT * FROM " TABLE_NAME " WHERE YCSB_KEY='%s%010d'",
                        KEY_PREFIX, key);
                    rc = exec_rc(db, sql);
                }

                if (rc == SQLITE_OK) {
                    done = 1;
                } else if (rc == SQLITE_BUSY || rc == SQLITE_LOCKED) {
                    ctx->retries++;
                    sleep_ms(1);
                } else {
                    done = 1; /* non-retriable error */
                }
            }
        }

        if ((ctx->use_concurrent && committed) || (!ctx->use_concurrent && done)) {
            ctx->completed_ops++;
            if (is_update) ctx->update_ops++;
            else ctx->read_ops++;
        }

        /* 120s timeout */
        if (now_ms() - t0 > 120000.0) break;
    }

    ctx->elapsed_ms = now_ms() - t0;
    sqlite3_close(db);
    return NULL;
}

/* ---------- load data ---------- */
static void load_data(const char *dbpath, int record_count) {
    sqlite3 *db;
    unsigned int seed = 42;

    if (sqlite3_open(dbpath, &db) != SQLITE_OK)
        die("load open", db);

    exec(db, "PRAGMA journal_mode=WAL");
    exec(db, "PRAGMA synchronous=NORMAL");

    exec(db, "DROP TABLE IF EXISTS " TABLE_NAME);
    exec(db, "CREATE TABLE " TABLE_NAME " ("
         "YCSB_KEY TEXT PRIMARY KEY,"
         "field0 TEXT, field1 TEXT, field2 TEXT, field3 TEXT, field4 TEXT,"
         "field5 TEXT, field6 TEXT, field7 TEXT, field8 TEXT, field9 TEXT)");

    exec(db, "BEGIN");
    for (int i = 0; i < record_count; i++) {
        char fields[FIELD_COUNT][FIELD_LENGTH + 1];
        for (int f = 0; f < FIELD_COUNT; f++)
            random_string_r(fields[f], FIELD_LENGTH, &seed);

        char sql[2048];
        snprintf(sql, sizeof(sql),
            "INSERT INTO " TABLE_NAME " VALUES('%s%010d','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')",
            KEY_PREFIX, i,
            fields[0], fields[1], fields[2], fields[3], fields[4],
            fields[5], fields[6], fields[7], fields[8], fields[9]);
        exec(db, sql);
    }
    exec(db, "COMMIT");
    sqlite3_close(db);
}

/* ---------- run one thread-count level ---------- */
static void run_level(const char *dbpath, int n_threads, int use_concurrent,
                      int record_count) {
    pthread_t threads[MAX_THREADS];
    thread_ctx_t ctxs[MAX_THREADS];

    /* reload data fresh */
    load_data(dbpath, record_count);

    for (int i = 0; i < n_threads; i++) {
        ctxs[i].thread_id = i;
        ctxs[i].dbpath = dbpath;
        ctxs[i].use_concurrent = use_concurrent;
        ctxs[i].ops = OPS_PER_THREAD;
        ctxs[i].record_count = record_count;
    }

    double wall_start = now_ms();

    for (int i = 0; i < n_threads; i++)
        pthread_create(&threads[i], NULL, worker, &ctxs[i]);

    for (int i = 0; i < n_threads; i++)
        pthread_join(threads[i], NULL);

    double wall_ms = now_ms() - wall_start;

    /* aggregate */
    long total_ops = 0, total_reads = 0, total_updates = 0;
    long total_retries = 0, total_aborts = 0;
    for (int i = 0; i < n_threads; i++) {
        total_ops     += ctxs[i].completed_ops;
        total_reads   += ctxs[i].read_ops;
        total_updates += ctxs[i].update_ops;
        total_retries += ctxs[i].retries;
        total_aborts  += ctxs[i].aborts;
    }

    double throughput = total_ops / (wall_ms / 1000.0);

    printf("  %2d threads  %8.1f ms  %8.0f ops/s  "
           "[ops:%ld R:%ld U:%ld retries:%ld aborts:%ld]\n",
           n_threads, wall_ms, throughput,
           total_ops, total_reads, total_updates, total_retries, total_aborts);
}

/* ---------- main ---------- */
int main(int argc, char **argv) {
    const char *dbpath = (argc > 1) ? argv[1] : "/tmp/ycsb_concurrent.db";
    const char *mode   = (argc > 2) ? argv[2] : "auto";
    int use_concurrent = (strcmp(mode, "concurrent") == 0);
    int record_count   = RECORD_COUNT;

    printf("=== YCSB-A Concurrent Benchmark ===\n");
    printf("Database: %s\n", dbpath);
    printf("Mode: %s\n", use_concurrent ? "BEGIN CONCURRENT (MVCC)" : "autocommit (WAL)");
    printf("Records: %d, Ops/thread: %d\n", record_count, OPS_PER_THREAD);
    printf("Workload: A (50%% read, 50%% update, Zipfian θ=0.99)\n\n");

    printf("--- Scaling ---\n");
    for (int i = 0; i < THREAD_COUNTS; i++) {
        fflush(stdout);
        run_level(dbpath, thread_counts[i], use_concurrent, record_count);
    }

    /* If not concurrent mode, also try concurrent mode to compare */
    if (!use_concurrent) {
        printf("\n--- Retry with BEGIN CONCURRENT ---\n");
        for (int i = 0; i < THREAD_COUNTS; i++) {
            fflush(stdout);
            run_level(dbpath, thread_counts[i], 1, record_count);
        }
    }

    printf("\nDone.\n");
    return 0;
}
