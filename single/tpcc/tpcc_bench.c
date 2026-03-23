/*
 * TPC-C Benchmark (simplified) for SQLite-compatible databases
 * Runs New Order, Payment, and Order Status transaction types.
 * Uses fork() subprocess isolation per transaction type.
 * Compile: gcc -O2 -o tpcc_bench tpcc_bench.c -I./include -lsqlite3
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <signal.h>
#include "sqlite3.h"

/* Scale */
#define N_WAREHOUSES  1
#define N_DISTRICTS   5
#define N_CUSTOMERS   100   /* per district */
#define N_ITEMS       100

/* Transaction counts */
#define N_NEW_ORDER     500
#define N_PAYMENT       500
#define N_ORDER_STATUS  200

/* Subprocess timeout in seconds */
#define CHILD_TIMEOUT_S 30

/* Shared result written by child, read by parent */
typedef struct {
    int    completed;   /* number of txns completed before crash/exit */
    double elapsed_ms;  /* wall time for the batch */
    int    crashed;     /* 1 if child wrote this naturally; parent flips to 0 */
} batch_result;

/* ------------------------------------------------------------------ */
/* Helpers                                                              */
/* ------------------------------------------------------------------ */

static double now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

static void die(const char *msg, sqlite3 *db) {
    fprintf(stderr, "FATAL: %s — %s\n", msg, sqlite3_errmsg(db));
    sqlite3_close(db);
    exit(1);
}

static void exec_sql(sqlite3 *db, const char *sql) {
    char *err = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n  %s\n", err, sql);
        sqlite3_free(err);
    }
}

/* ------------------------------------------------------------------ */
/* Schema                                                               */
/* ------------------------------------------------------------------ */

static const char *SCHEMA =
    "CREATE TABLE IF NOT EXISTS warehouse ("
    "  w_id   INTEGER PRIMARY KEY,"
    "  w_name TEXT,"
    "  w_tax  REAL,"
    "  w_ytd  REAL);"

    "CREATE TABLE IF NOT EXISTS district ("
    "  d_id       INTEGER,"
    "  d_w_id     INTEGER,"
    "  d_name     TEXT,"
    "  d_tax      REAL,"
    "  d_next_o_id INTEGER,"
    "  PRIMARY KEY (d_id, d_w_id));"

    "CREATE TABLE IF NOT EXISTS customer ("
    "  c_id          INTEGER,"
    "  c_d_id        INTEGER,"
    "  c_w_id        INTEGER,"
    "  c_last        TEXT,"
    "  c_balance     REAL,"
    "  c_ytd_payment REAL,"
    "  PRIMARY KEY (c_id, c_d_id, c_w_id));"

    "CREATE TABLE IF NOT EXISTS orders ("
    "  o_id      INTEGER,"
    "  o_d_id    INTEGER,"
    "  o_w_id    INTEGER,"
    "  o_c_id    INTEGER,"
    "  o_entry_d TEXT,"
    "  o_ol_cnt  INTEGER,"
    "  PRIMARY KEY (o_id, o_d_id, o_w_id));"

    "CREATE TABLE IF NOT EXISTS order_line ("
    "  ol_o_id   INTEGER,"
    "  ol_d_id   INTEGER,"
    "  ol_w_id   INTEGER,"
    "  ol_number INTEGER,"
    "  ol_amount REAL,"
    "  PRIMARY KEY (ol_o_id, ol_d_id, ol_w_id, ol_number));"
;

/* ------------------------------------------------------------------ */
/* Data generation                                                      */
/* ------------------------------------------------------------------ */

static void generate_data(sqlite3 *db) {
    char buf[512];
    int w, d, c, i;

    exec_sql(db, "BEGIN");

    /* warehouses */
    for (w = 1; w <= N_WAREHOUSES; w++) {
        snprintf(buf, sizeof(buf),
            "INSERT INTO warehouse VALUES(%d,'Warehouse%d',%.4f,%.2f)",
            w, w, 0.05 + (w % 5) * 0.01, 300000.0);
        exec_sql(db, buf);
    }

    /* districts — d_next_o_id starts at 1 */
    for (w = 1; w <= N_WAREHOUSES; w++) {
        for (d = 1; d <= N_DISTRICTS; d++) {
            snprintf(buf, sizeof(buf),
                "INSERT INTO district VALUES(%d,%d,'District%d-%d',%.4f,1)",
                d, w, d, w, 0.04 + (d % 5) * 0.01);
            exec_sql(db, buf);
        }
    }

    /* customers */
    for (w = 1; w <= N_WAREHOUSES; w++) {
        for (d = 1; d <= N_DISTRICTS; d++) {
            for (c = 1; c <= N_CUSTOMERS; c++) {
                snprintf(buf, sizeof(buf),
                    "INSERT INTO customer VALUES(%d,%d,%d,'Last%04d',%.2f,0.0)",
                    c, d, w, c, 1000.0 + (c * 7 % 500));
                exec_sql(db, buf);
            }
        }
    }

    /* No initial orders — New Order transactions will create them */
    (void)i; /* suppress unused warning */

    exec_sql(db, "COMMIT");
}

/* ------------------------------------------------------------------ */
/* Transaction implementations                                          */
/* ------------------------------------------------------------------ */

/* New Order: read d_next_o_id, bump it, insert order + 5-10 order_lines */
static void run_new_order(sqlite3 *db, int txn_num) {
    char buf[512];
    sqlite3_stmt *stmt;
    int d_id, w_id, c_id, o_id, ol_cnt, j;

    d_id  = 1 + (txn_num % N_DISTRICTS);
    w_id  = 1;
    c_id  = 1 + (txn_num % N_CUSTOMERS);
    ol_cnt = 5 + (txn_num % 6); /* 5..10 */

    exec_sql(db, "BEGIN");

    /* Read d_next_o_id */
    snprintf(buf, sizeof(buf),
        "SELECT d_next_o_id FROM district WHERE d_id=%d AND d_w_id=%d",
        d_id, w_id);
    o_id = -1;
    if (sqlite3_prepare_v2(db, buf, -1, &stmt, NULL) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW)
            o_id = sqlite3_column_int(stmt, 0);
        sqlite3_finalize(stmt);
    }
    if (o_id < 0) { exec_sql(db, "ROLLBACK"); return; }

    /* Bump d_next_o_id */
    snprintf(buf, sizeof(buf),
        "UPDATE district SET d_next_o_id=d_next_o_id+1 WHERE d_id=%d AND d_w_id=%d",
        d_id, w_id);
    exec_sql(db, buf);

    /* Insert order */
    snprintf(buf, sizeof(buf),
        "INSERT INTO orders VALUES(%d,%d,%d,%d,'2026-03-20',%d)",
        o_id, d_id, w_id, c_id, ol_cnt);
    exec_sql(db, buf);

    /* Insert order_line rows */
    for (j = 1; j <= ol_cnt; j++) {
        double amount = 1.0 + ((txn_num * j) % 9999) / 100.0;
        snprintf(buf, sizeof(buf),
            "INSERT INTO order_line VALUES(%d,%d,%d,%d,%.2f)",
            o_id, d_id, w_id, j, amount);
        exec_sql(db, buf);
    }

    exec_sql(db, "COMMIT");
}

/* Payment: SELECT customer, UPDATE balance and ytd_payment */
static void run_payment(sqlite3 *db, int txn_num) {
    char buf[512];
    sqlite3_stmt *stmt;
    int c_id, d_id, w_id;
    double amount, balance;

    c_id  = 1 + (txn_num % N_CUSTOMERS);
    d_id  = 1 + (txn_num % N_DISTRICTS);
    w_id  = 1;
    amount = 1.0 + (txn_num % 500);

    exec_sql(db, "BEGIN");

    /* SELECT customer */
    snprintf(buf, sizeof(buf),
        "SELECT c_balance FROM customer WHERE c_id=%d AND c_d_id=%d AND c_w_id=%d",
        c_id, d_id, w_id);
    balance = 0.0;
    if (sqlite3_prepare_v2(db, buf, -1, &stmt, NULL) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW)
            balance = sqlite3_column_double(stmt, 0);
        sqlite3_finalize(stmt);
    }
    (void)balance;

    /* UPDATE customer */
    snprintf(buf, sizeof(buf),
        "UPDATE customer SET c_balance=c_balance-%.2f, c_ytd_payment=c_ytd_payment+%.2f "
        "WHERE c_id=%d AND c_d_id=%d AND c_w_id=%d",
        amount, amount, c_id, d_id, w_id);
    exec_sql(db, buf);

    exec_sql(db, "COMMIT");
}

/* Order Status: 3-table join — most recent order for a customer */
static void run_order_status(sqlite3 *db, int txn_num) {
    sqlite3_stmt *stmt;
    char buf[1024];
    int c_id, d_id, w_id;

    c_id = 1 + (txn_num % N_CUSTOMERS);
    d_id = 1 + (txn_num % N_DISTRICTS);
    w_id = 1;

    /* Join customer -> orders -> order_line for the most recent order */
    snprintf(buf, sizeof(buf),
        "SELECT c.c_id, c.c_last, o.o_id, o.o_entry_d, ol.ol_number, ol.ol_amount "
        "FROM customer c "
        "JOIN orders o ON o.o_c_id=c.c_id AND o.o_d_id=c.c_d_id AND o.o_w_id=c.c_w_id "
        "JOIN order_line ol ON ol.ol_o_id=o.o_id AND ol.ol_d_id=o.o_d_id AND ol.ol_w_id=o.o_w_id "
        "WHERE c.c_id=%d AND c.c_d_id=%d AND c.c_w_id=%d "
        "AND o.o_id = ("
        "  SELECT MAX(o2.o_id) FROM orders o2 "
        "  WHERE o2.o_c_id=%d AND o2.o_d_id=%d AND o2.o_w_id=%d"
        ")",
        c_id, d_id, w_id,
        c_id, d_id, w_id);

    if (sqlite3_prepare_v2(db, buf, -1, &stmt, NULL) == SQLITE_OK) {
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            /* consume rows */
        }
        sqlite3_finalize(stmt);
    }
}

/* ------------------------------------------------------------------ */
/* Fork-based batch runner                                              */
/* ------------------------------------------------------------------ */

typedef enum { TXN_NEW_ORDER, TXN_PAYMENT, TXN_ORDER_STATUS } txn_type;

static void child_run_batch(const char *dbpath, txn_type type,
                            int count, batch_result *shm) {
    sqlite3 *db;
    double t0, t1;
    int i;

    if (sqlite3_open(dbpath, &db) != SQLITE_OK) {
        fprintf(stderr, "child: cannot open db: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        exit(2);
    }

    exec_sql(db, "PRAGMA journal_mode=WAL");
    exec_sql(db, "PRAGMA synchronous=NORMAL");

    t0 = now_ms();

    for (i = 0; i < count; i++) {
        switch (type) {
            case TXN_NEW_ORDER:    run_new_order(db, i);    break;
            case TXN_PAYMENT:      run_payment(db, i);      break;
            case TXN_ORDER_STATUS: run_order_status(db, i); break;
        }
        shm->completed = i + 1;
    }

    t1 = now_ms();
    shm->elapsed_ms = t1 - t0;
    shm->crashed = 0; /* clean exit */

    sqlite3_close(db);
    exit(0);
}

/*
 * Fork a child to run `count` transactions of `type`.
 * Results written to shared mmap region.
 * Returns: 0=ok, 1=crash, 2=timeout.
 * On ok, fills *out_ms and *out_count.
 */
static int run_batch_forked(const char *dbpath, txn_type type,
                            int count,
                            double *out_ms, int *out_count, int *out_signal) {
    batch_result *shm;
    pid_t pid;
    int status;
    double deadline;

    /* Shared memory for result */
    shm = (batch_result *)mmap(NULL, sizeof(batch_result),
                               PROT_READ | PROT_WRITE,
                               MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (shm == MAP_FAILED) {
        perror("mmap");
        return 1;
    }
    shm->completed  = 0;
    shm->elapsed_ms = 0.0;
    shm->crashed    = 1; /* assume crash until child says otherwise */

    pid = fork();
    if (pid < 0) {
        perror("fork");
        munmap(shm, sizeof(batch_result));
        return 1;
    }

    if (pid == 0) {
        /* Child */
        child_run_batch(dbpath, type, count, shm);
        /* child_run_batch calls exit() */
        _exit(0);
    }

    /* Parent: wait with timeout */
    deadline = now_ms() + CHILD_TIMEOUT_S * 1000.0;
    while (1) {
        pid_t r = waitpid(pid, &status, WNOHANG);
        if (r == pid) break;
        if (r < 0) break;
        if (now_ms() >= deadline) {
            kill(pid, SIGKILL);
            waitpid(pid, &status, 0);
            *out_count  = shm->completed;
            *out_ms     = shm->elapsed_ms;
            *out_signal = 0;
            munmap(shm, sizeof(batch_result));
            return 2; /* timeout */
        }
        /* Sleep 1ms to avoid spinning */
        struct timespec ts = {0, 1000000};
        nanosleep(&ts, NULL);
    }

    *out_count  = shm->completed;
    *out_ms     = shm->elapsed_ms;

    if (WIFSIGNALED(status)) {
        *out_signal = WTERMSIG(status);
        munmap(shm, sizeof(batch_result));
        return 1; /* crash */
    }

    *out_signal = 0;

    if (shm->crashed) {
        /* Child exited 0 but did not mark crashed=0 — treat as error */
        munmap(shm, sizeof(batch_result));
        return 1;
    }

    munmap(shm, sizeof(batch_result));
    return 0; /* success */
}

/* ------------------------------------------------------------------ */
/* main                                                                 */
/* ------------------------------------------------------------------ */

int main(int argc, char **argv) {
    const char *dbpath = (argc > 1) ? argv[1] : "/tmp/tpcc.db";
    const char *label  = (argc > 2) ? argv[2] : "SQLite";
    sqlite3 *db;
    double t0, t1;
    double ms;
    int count, sig, rc;

    printf("=== TPC-C Benchmark: %s ===\n", label);
    printf("Database: %s\n\n", dbpath);

    /* Remove any stale db file so we start fresh */
    unlink(dbpath);
    /* Also remove WAL/SHM sidecars if present */
    {
        char wal[512], shm[512];
        snprintf(wal, sizeof(wal), "%s-wal", dbpath);
        snprintf(shm, sizeof(shm), "%s-shm", dbpath);
        unlink(wal);
        unlink(shm);
    }

    /* Open db and create schema */
    if (sqlite3_open(dbpath, &db) != SQLITE_OK)
        die("open", db);

    exec_sql(db, "PRAGMA journal_mode=WAL");
    exec_sql(db, "PRAGMA synchronous=NORMAL");

    printf("Creating schema...\n");
    t0 = now_ms();
    exec_sql(db, SCHEMA);
    t1 = now_ms();
    printf("  Schema: %.2f ms (5 tables)\n", t1 - t0);

    printf("Generating data...\n");
    t0 = now_ms();
    generate_data(db);
    t1 = now_ms();
    printf("  Data load: %.2f ms\n", t1 - t0);

    sqlite3_close(db);
    db = NULL;

    /* Run transaction batches in forked subprocesses */
    printf("\n--- TPC-C Transactions ---\n");

    /* New Order */
    rc = run_batch_forked(dbpath, TXN_NEW_ORDER, N_NEW_ORDER,
                          &ms, &count, &sig);
    if (rc == 0) {
        double tps = (ms > 0) ? (count / (ms / 1000.0)) : 0.0;
        printf("  New Order      %3d txns  %9.2f ms  %6.0f txn/s\n",
               count, ms, tps);
    } else if (rc == 1) {
        printf("  New Order      CRASH (signal %d) after %d txns\n", sig, count);
    } else {
        printf("  New Order      TIMEOUT after %d txns\n", count);
    }
    fflush(stdout);

    /* Payment */
    rc = run_batch_forked(dbpath, TXN_PAYMENT, N_PAYMENT,
                          &ms, &count, &sig);
    if (rc == 0) {
        double tps = (ms > 0) ? (count / (ms / 1000.0)) : 0.0;
        printf("  Payment        %3d txns  %9.2f ms  %6.0f txn/s\n",
               count, ms, tps);
    } else if (rc == 1) {
        printf("  Payment        CRASH (signal %d) after %d txns\n", sig, count);
    } else {
        printf("  Payment        TIMEOUT after %d txns\n", count);
    }
    fflush(stdout);

    /* Order Status */
    rc = run_batch_forked(dbpath, TXN_ORDER_STATUS, N_ORDER_STATUS,
                          &ms, &count, &sig);
    if (rc == 0) {
        double tps = (ms > 0) ? (count / (ms / 1000.0)) : 0.0;
        printf("  Order Status   %3d txns  %9.2f ms  %6.0f txn/s   (3-table join)\n",
               count, ms, tps);
    } else if (rc == 1) {
        printf("  Order Status   CRASH (signal %d) after %d txns\n", sig, count);
    } else {
        printf("  Order Status   TIMEOUT after %d txns\n", count);
    }
    fflush(stdout);

    printf("\nDone.\n");
    return 0;
}
