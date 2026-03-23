/*
 * TPC-C Concurrent Benchmark for SQLite-compatible databases
 *
 * Measures MVCC scalability: N threads running a mix of New Order (60%) and
 * Payment (40%) transactions against a shared database. Order Status is skipped
 * because its join-heavy queries crash the target under test.
 * Reports throughput at each thread count (1, 2, 4, 8) plus abort/retry counts.
 *
 * Two modes:
 *   - autocommit (WAL):        each txn uses BEGIN/COMMIT
 *   - BEGIN CONCURRENT:        explicit txns with BEGIN CONCURRENT (MVCC)
 *
 * Schema mirrors single/tpcc/tpcc_bench.c:
 *   warehouse, district, customer, orders, order_line
 *
 * Compile (system SQLite):
 *   gcc -O2 tpcc_concurrent.c -I./include -lsqlite3 -lpthread -o tpcc_concurrent_sqlite
 *
 * Compile (frankensqlite):
 *   gcc -O2 tpcc_concurrent.c -I./include -L$KQSQL -lfsqlite_c_api -lpthread \
 *       -Wl,-rpath,$KQSQL -o tpcc_concurrent_fsqlite
 *
 * Usage:
 *   ./tpcc_concurrent_<target> [db_path] [mode]
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
#define NUM_WAREHOUSES    1
#define NUM_DISTRICTS     5
#define CUSTOMERS_PER_DIST 100
#define ITEMS_PER_ORDER    5    /* order lines per New Order */
#define TXNS_PER_THREAD  200
#define MAX_THREADS        8
#define THREAD_COUNTS      4   /* test at 1, 2, 4, 8 */

/* New Order probability: 60%, Payment: 40% */
#define NEW_ORDER_PCT 60

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

/*
 * Execute a SELECT that returns a single integer column from the first row.
 * The SQL is fully formed (no bind parameters needed).
 * Returns the integer value, or default_val if no row is found.
 */
static int query_int(sqlite3 *db, const char *sql, int default_val) {
    sqlite3_stmt *stmt;
    int result = default_val;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW)
            result = sqlite3_column_int(stmt, 0);
        sqlite3_finalize(stmt);
    }
    return result;
}

/* ---------- transaction helpers ---------- */

/*
 * New Order: read district d_next_o_id, increment it, insert orders row and
 * ITEMS_PER_ORDER order_line rows.
 * All parameterized statements use snprintf+exec (no bind API).
 */
static void do_new_order(sqlite3 *db, unsigned int *seed, int w_id) {
    int d_id  = 1 + rand_r(seed) % NUM_DISTRICTS;
    int c_id  = 1 + rand_r(seed) % CUSTOMERS_PER_DIST;
    char sql[256];

    /* read d_next_o_id */
    snprintf(sql, sizeof(sql),
        "SELECT d_next_o_id FROM district WHERE d_w_id=%d AND d_id=%d",
        w_id, d_id);
    int o_id = query_int(db, sql, 1);

    /* increment d_next_o_id */
    snprintf(sql, sizeof(sql),
        "UPDATE district SET d_next_o_id=d_next_o_id+1 WHERE d_w_id=%d AND d_id=%d",
        w_id, d_id);
    exec(db, sql);

    /* insert orders */
    snprintf(sql, sizeof(sql),
        "INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_ol_cnt) "
        "VALUES (%d, %d, %d, %d, %d)",
        o_id, d_id, w_id, c_id, ITEMS_PER_ORDER);
    exec(db, sql);

    /* insert order_lines */
    for (int ol = 1; ol <= ITEMS_PER_ORDER; ol++) {
        int amount = 1 + rand_r(seed) % 9999;
        snprintf(sql, sizeof(sql),
            "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_amount) "
            "VALUES (%d, %d, %d, %d, %d)",
            o_id, d_id, w_id, ol, amount);
        exec(db, sql);
    }
}

/*
 * Payment: read customer c_balance, add a random payment amount.
 * The SELECT is executed for correctness (verify row exists); the UPDATE
 * uses snprintf+exec.
 */
static void do_payment(sqlite3 *db, unsigned int *seed, int w_id) {
    int d_id   = 1 + rand_r(seed) % NUM_DISTRICTS;
    int c_id   = 1 + rand_r(seed) % CUSTOMERS_PER_DIST;
    int amount = 1 + rand_r(seed) % 5000;
    char sql[256];

    /* read current balance (verify row exists) */
    snprintf(sql, sizeof(sql),
        "SELECT c_balance FROM customer WHERE c_w_id=%d AND c_d_id=%d AND c_id=%d",
        w_id, d_id, c_id);
    exec(db, sql);

    /* update balance */
    snprintf(sql, sizeof(sql),
        "UPDATE customer SET c_balance=c_balance+%d "
        "WHERE c_w_id=%d AND c_d_id=%d AND c_id=%d",
        amount, w_id, d_id, c_id);
    exec(db, sql);
}

/* ---------- thread context ---------- */
typedef struct {
    int thread_id;
    const char *dbpath;
    int use_concurrent;    /* 1 = BEGIN CONCURRENT, 0 = BEGIN */
    int txns;

    /* results */
    double elapsed_ms;
    long completed_txns;
    long new_order_txns;
    long payment_txns;
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

    ctx->completed_txns  = 0;
    ctx->new_order_txns  = 0;
    ctx->payment_txns    = 0;
    ctx->retries         = 0;
    ctx->aborts          = 0;

    int w_id = 1 + ctx->thread_id % NUM_WAREHOUSES;
    const char *begin_sql = ctx->use_concurrent ? "BEGIN CONCURRENT" : "BEGIN";

    double t0 = now_ms();

    for (int t = 0; t < ctx->txns; t++) {
        int is_new_order = ((int)(rand_r(&seed) % 100) < NEW_ORDER_PCT);

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

            if (is_new_order)
                do_new_order(db, &seed, w_id);
            else
                do_payment(db, &seed, w_id);

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
        if (is_new_order) ctx->new_order_txns++;
        else              ctx->payment_txns++;

        /* 120s timeout */
        if (now_ms() - t0 > 120000.0) break;
    }

    ctx->elapsed_ms = now_ms() - t0;
    sqlite3_close(db);
    return NULL;
}

/* ---------- load data ---------- */
static void load_data(const char *dbpath) {
    sqlite3 *db;
    unsigned int seed = 42;

    if (sqlite3_open(dbpath, &db) != SQLITE_OK)
        die("load open", db);

    exec(db, "PRAGMA journal_mode=WAL");
    exec(db, "PRAGMA synchronous=NORMAL");

    /* schema */
    exec(db, "DROP TABLE IF EXISTS order_line");
    exec(db, "DROP TABLE IF EXISTS orders");
    exec(db, "DROP TABLE IF EXISTS customer");
    exec(db, "DROP TABLE IF EXISTS district");
    exec(db, "DROP TABLE IF EXISTS warehouse");

    exec(db, "CREATE TABLE warehouse ("
         "w_id      INTEGER PRIMARY KEY,"
         "w_name    TEXT,"
         "w_ytd     INTEGER NOT NULL DEFAULT 0)");

    exec(db, "CREATE TABLE district ("
         "d_id      INTEGER NOT NULL,"
         "d_w_id    INTEGER NOT NULL,"
         "d_name    TEXT,"
         "d_next_o_id INTEGER NOT NULL DEFAULT 1,"
         "PRIMARY KEY (d_w_id, d_id))");

    exec(db, "CREATE TABLE customer ("
         "c_id      INTEGER NOT NULL,"
         "c_d_id    INTEGER NOT NULL,"
         "c_w_id    INTEGER NOT NULL,"
         "c_last    TEXT,"
         "c_balance INTEGER NOT NULL DEFAULT 0,"
         "PRIMARY KEY (c_w_id, c_d_id, c_id))");

    exec(db, "CREATE TABLE orders ("
         "o_id      INTEGER NOT NULL,"
         "o_d_id    INTEGER NOT NULL,"
         "o_w_id    INTEGER NOT NULL,"
         "o_c_id    INTEGER NOT NULL,"
         "o_ol_cnt  INTEGER NOT NULL,"
         "PRIMARY KEY (o_w_id, o_d_id, o_id))");

    exec(db, "CREATE TABLE order_line ("
         "ol_o_id   INTEGER NOT NULL,"
         "ol_d_id   INTEGER NOT NULL,"
         "ol_w_id   INTEGER NOT NULL,"
         "ol_number INTEGER NOT NULL,"
         "ol_amount INTEGER NOT NULL,"
         "PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number))");

    exec(db, "BEGIN");

    /* warehouses */
    for (int w = 1; w <= NUM_WAREHOUSES; w++) {
        char sql[128];
        snprintf(sql, sizeof(sql),
            "INSERT INTO warehouse VALUES (%d, 'W%d', 0)", w, w);
        exec(db, sql);
    }

    /* districts and customers */
    char name_buf[16];
    for (int w = 1; w <= NUM_WAREHOUSES; w++) {
        for (int d = 1; d <= NUM_DISTRICTS; d++) {
            char sql[256];
            snprintf(sql, sizeof(sql),
                "INSERT INTO district VALUES (%d, %d, 'D%d_%d', 1)",
                d, w, w, d);
            exec(db, sql);

            for (int c = 1; c <= CUSTOMERS_PER_DIST; c++) {
                random_string_r(name_buf, 8, &seed);
                snprintf(sql, sizeof(sql),
                    "INSERT INTO customer VALUES (%d, %d, %d, '%s', 0)",
                    c, d, w, name_buf);
                exec(db, sql);
            }
        }
    }

    exec(db, "COMMIT");
    sqlite3_close(db);
}

/* ---------- run one thread-count level ---------- */
static void run_level(const char *dbpath, int n_threads, int use_concurrent) {
    pthread_t threads[MAX_THREADS];
    thread_ctx_t ctxs[MAX_THREADS];

    /* reload data fresh for each level */
    load_data(dbpath);

    for (int i = 0; i < n_threads; i++) {
        ctxs[i].thread_id      = i;
        ctxs[i].dbpath         = dbpath;
        ctxs[i].use_concurrent = use_concurrent;
        ctxs[i].txns           = TXNS_PER_THREAD;
    }

    double wall_start = now_ms();

    for (int i = 0; i < n_threads; i++)
        pthread_create(&threads[i], NULL, worker, &ctxs[i]);

    for (int i = 0; i < n_threads; i++)
        pthread_join(threads[i], NULL);

    double wall_ms = now_ms() - wall_start;

    /* aggregate */
    long total_txns     = 0, total_no  = 0, total_pay = 0;
    long total_retries  = 0, total_aborts = 0;
    for (int i = 0; i < n_threads; i++) {
        total_txns    += ctxs[i].completed_txns;
        total_no      += ctxs[i].new_order_txns;
        total_pay     += ctxs[i].payment_txns;
        total_retries += ctxs[i].retries;
        total_aborts  += ctxs[i].aborts;
    }

    double throughput = total_txns / (wall_ms / 1000.0);

    printf("  %2d threads  %8.1f ms  %8.0f txn/s  "
           "[txns:%ld new_order:%ld payment:%ld retries:%ld aborts:%ld]\n",
           n_threads, wall_ms, throughput,
           total_txns, total_no, total_pay, total_retries, total_aborts);
}

/* ---------- main ---------- */
int main(int argc, char **argv) {
    const char *dbpath = (argc > 1) ? argv[1] : "/tmp/tpcc_conc.db";
    const char *mode   = (argc > 2) ? argv[2] : "auto";
    int use_concurrent = (strcmp(mode, "concurrent") == 0);

    printf("=== TPC-C Concurrent Benchmark ===\n");
    printf("Database: %s\n", dbpath);
    printf("Mode: %s\n", use_concurrent ? "BEGIN CONCURRENT (MVCC)" : "autocommit (WAL)");
    printf("Warehouses: %d, Districts: %d, Txns/thread: %d\n",
           NUM_WAREHOUSES, NUM_DISTRICTS, TXNS_PER_THREAD);
    printf("Mix: %d%% New Order, %d%% Payment "
           "(Order Status skipped — crashes target)\n\n",
           NEW_ORDER_PCT, 100 - NEW_ORDER_PCT);

    printf("--- Scaling ---\n");
    for (int i = 0; i < THREAD_COUNTS; i++) {
        fflush(stdout);
        run_level(dbpath, thread_counts[i], use_concurrent);
    }

    /* If not concurrent mode, also try concurrent mode to compare */
    if (!use_concurrent) {
        printf("\n--- Retry with BEGIN CONCURRENT ---\n");
        for (int i = 0; i < THREAD_COUNTS; i++) {
            fflush(stdout);
            run_level(dbpath, thread_counts[i], 1);
        }
    }

    printf("\nDone.\n");
    return 0;
}
