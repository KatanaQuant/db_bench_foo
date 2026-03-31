/*
 * Elle-style write skew test v2 — correct invariant.
 *
 * Classic Kleppmann write skew: two transactions both read the same
 * account balance, both decide it's safe to withdraw, both commit.
 * Under serializable isolation, one must be aborted.
 *
 * Invariant: no individual account balance may go below zero.
 * Each transaction: read account X balance, if >= 100, withdraw 100.
 * Write skew: two txns both read balance=200, both withdraw 100,
 * balance goes to 0. Under SSI one should be aborted because the
 * second saw stale data.
 *
 * Compile (SQLite):
 *   gcc -O2 elle_write_skew_v2.c -I./include -lsqlite3 -lpthread -o elle2_sqlite
 * Compile (FrankenSQLite):
 *   gcc -O2 elle_write_skew_v2.c -I./include -L$KQSQL -lfsqlite_c_api \
 *       -lpthread -Wl,-rpath,$KQSQL -o elle2_fsqlite
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>
#include <stdatomic.h>
#include "sqlite3.h"

#define NUM_ACCOUNTS     5
#define INITIAL_BALANCE  500
#define WITHDRAW_AMOUNT  100
#define NUM_THREADS      4
#define ROUNDS_PER_THREAD 500

static double now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
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
        fprintf(stderr, "SQL error: %s\n  %.200s\n", err, sql);
        sqlite3_free(err);
    }
}

static int cb_int(void *out, int ncols, char **vals, char **names) {
    (void)ncols; (void)names;
    if (vals[0]) *(int *)out = atoi(vals[0]);
    return 0;
}

static int query_int(sqlite3 *db, const char *sql, int default_val) {
    int result = default_val;
    sqlite3_exec(db, sql, cb_int, &result, NULL);
    return result;
}

typedef struct {
    int thread_id;
    const char *dbpath;
    int use_concurrent;
    long commits;
    long aborts;
    long skipped;  /* skipped because balance too low */
} thread_ctx_t;

static atomic_long g_negative_observed = 0;

static void *worker(void *arg) {
    thread_ctx_t *ctx = (thread_ctx_t *)arg;
    sqlite3 *db;
    unsigned int seed = (unsigned int)(42 + ctx->thread_id * 7);

    if (sqlite3_open(ctx->dbpath, &db) != SQLITE_OK) {
        fprintf(stderr, "Thread %d: open failed\n", ctx->thread_id);
        return NULL;
    }

    exec(db, "PRAGMA journal_mode=WAL");
    exec(db, "PRAGMA busy_timeout=5000");

    ctx->commits = 0;
    ctx->aborts = 0;
    ctx->skipped = 0;

    for (int round = 0; round < ROUNDS_PER_THREAD; round++) {
        int account = rand_r(&seed) % NUM_ACCOUNTS;
        int committed = 0;

        for (int attempt = 0; attempt < 20 && !committed; attempt++) {
            int rc;
            if (ctx->use_concurrent) {
                rc = exec_rc(db, "BEGIN CONCURRENT");
                if (rc != SQLITE_OK) {
                    fprintf(stderr, "WARNING: BEGIN CONCURRENT failed (rc=%d), falling back to BEGIN\n", rc);
                    rc = exec_rc(db, "BEGIN");
                }
            } else {
                rc = exec_rc(db, "BEGIN");
            }

            if (rc == SQLITE_BUSY) {
                usleep(100 + (rand_r(&seed) % 500));
                continue;
            }

            /*
             * THE CORRECT WRITE SKEW TEST:
             *
             * 1. Read THIS account's balance
             * 2. If balance >= WITHDRAW_AMOUNT, withdraw
             * 3. Commit
             *
             * Write skew: two threads both read account X = 200,
             * both withdraw 100, account goes to 0. Both saw
             * balance=200, both decided it was safe. Under SSI,
             * the second must be aborted because its read is stale.
             *
             * If balance < WITHDRAW_AMOUNT, skip (don't withdraw).
             *
             * Invariant: no account balance should EVER go below 0.
             * If it does, write skew occurred.
             */

            char sql[256];
            snprintf(sql, sizeof(sql),
                "SELECT balance FROM accounts WHERE id = %d", account);
            int balance = query_int(db, sql, -1);

            if (balance >= WITHDRAW_AMOUNT) {
                snprintf(sql, sizeof(sql),
                    "UPDATE accounts SET balance = balance - %d WHERE id = %d",
                    WITHDRAW_AMOUNT, account);
                rc = exec_rc(db, sql);

                if (rc == SQLITE_BUSY) {
                    exec_rc(db, "ROLLBACK");
                    usleep(100 + (rand_r(&seed) % 500));
                    continue;
                }
            } else {
                /* balance too low, just commit the read-only txn */
                ctx->skipped++;
            }

            rc = exec_rc(db, "COMMIT");
            if (rc == SQLITE_OK) {
                committed = 1;
                ctx->commits++;
            } else if (rc == SQLITE_BUSY || rc == SQLITE_LOCKED) {
                exec_rc(db, "ROLLBACK");
                ctx->aborts++;
                usleep(100 + (rand_r(&seed) % 500));
            } else {
                exec_rc(db, "ROLLBACK");
                ctx->aborts++;
            }
        }

        /* periodic check: any account negative? */
        if (round % 25 == 0) {
            int min_bal = query_int(db,
                "SELECT MIN(balance) FROM accounts", 0);
            if (min_bal < 0) {
                atomic_fetch_add(&g_negative_observed, 1);
            }
        }
    }

    sqlite3_close(db);
    return NULL;
}

int main(int argc, char **argv) {
    const char *dbpath = (argc > 1) ? argv[1] : "/tmp/elle2_write_skew.db";
    const char *mode   = (argc > 2) ? argv[2] : "auto";
    int use_concurrent = (strcmp(mode, "concurrent") == 0);

    printf("=== Elle Write Skew Test v2 ===\n");
    printf("Database: %s\n", dbpath);
    printf("Mode: %s\n", use_concurrent ? "BEGIN CONCURRENT" : "BEGIN (default)");
    printf("Accounts: %d x %d initial\n", NUM_ACCOUNTS, INITIAL_BALANCE);
    printf("Threads: %d, Rounds/thread: %d\n", NUM_THREADS, ROUNDS_PER_THREAD);
    printf("Invariant: no individual account balance may go below 0\n\n");

    remove(dbpath);
    { char p[512]; snprintf(p,sizeof(p),"%s-wal",dbpath); remove(p);
      snprintf(p,sizeof(p),"%s-shm",dbpath); remove(p); }

    sqlite3 *setup_db;
    if (sqlite3_open(dbpath, &setup_db) != SQLITE_OK) {
        fprintf(stderr, "Setup failed\n"); return 1;
    }
    exec(setup_db, "PRAGMA journal_mode=WAL");
    exec(setup_db, "PRAGMA synchronous=NORMAL");
    exec(setup_db, "CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER NOT NULL)");
    for (int i = 0; i < NUM_ACCOUNTS; i++) {
        char sql[128];
        snprintf(sql, sizeof(sql), "INSERT INTO accounts VALUES (%d, %d)", i, INITIAL_BALANCE);
        exec(setup_db, sql);
    }
    int initial_total = query_int(setup_db, "SELECT SUM(balance) FROM accounts", -1);
    printf("Initial total: %d (%d per account)\n", initial_total, INITIAL_BALANCE);
    sqlite3_close(setup_db);

    pthread_t threads[NUM_THREADS];
    thread_ctx_t ctxs[NUM_THREADS];

    double t0 = now_ms();
    for (int i = 0; i < NUM_THREADS; i++) {
        ctxs[i].thread_id = i;
        ctxs[i].dbpath = dbpath;
        ctxs[i].use_concurrent = use_concurrent;
    }
    for (int i = 0; i < NUM_THREADS; i++)
        pthread_create(&threads[i], NULL, worker, &ctxs[i]);
    for (int i = 0; i < NUM_THREADS; i++)
        pthread_join(threads[i], NULL);
    double elapsed = now_ms() - t0;

    sqlite3 *check_db;
    sqlite3_open(dbpath, &check_db);
    int final_total = query_int(check_db, "SELECT SUM(balance) FROM accounts", -1);
    int min_balance = query_int(check_db, "SELECT MIN(balance) FROM accounts", 0);

    /* count how many accounts went negative */
    int negative_accounts = query_int(check_db,
        "SELECT COUNT(*) FROM accounts WHERE balance < 0", 0);

    printf("\n--- Results ---\n");
    printf("Duration: %.1f ms\n", elapsed);
    long total_commits = 0, total_aborts = 0, total_skipped = 0;
    for (int i = 0; i < NUM_THREADS; i++) {
        total_commits += ctxs[i].commits;
        total_aborts += ctxs[i].aborts;
        total_skipped += ctxs[i].skipped;
        printf("  Thread %d: %ld commits, %ld aborts, %ld skipped\n",
               i, ctxs[i].commits, ctxs[i].aborts, ctxs[i].skipped);
    }

    printf("\nTotal commits:  %ld\n", total_commits);
    printf("Total aborts:   %ld\n", total_aborts);
    printf("Total skipped:  %ld (balance too low)\n", total_skipped);
    printf("Final total:    %d\n", final_total);
    printf("Min balance:    %d\n", min_balance);
    printf("Negative accounts: %d\n", negative_accounts);
    printf("Negative observed during run: %ld\n",
           atomic_load(&g_negative_observed));

    printf("\n--- Verdict ---\n");
    if (min_balance < 0) {
        printf("WRITE SKEW DETECTED: %d account(s) went negative (min: %d)\n",
               negative_accounts, min_balance);
        printf("Two transactions both read the same balance, both decided\n");
        printf("it was safe to withdraw, and both committed. The database\n");
        printf("did not abort the second one.\n");
    } else if (total_aborts > 0) {
        printf("SERIALIZABLE: %ld aborts prevented write skew.\n", total_aborts);
        printf("No account went below zero.\n");
    } else if (total_aborts == 0 && min_balance >= 0) {
        printf("NO ABORTS, NO VIOLATIONS.\n");
        printf("Either the mutex serialized everything (preventing overlap)\n");
        printf("or the workload didn't produce enough contention.\n");
    }

    sqlite3_close(check_db);
    printf("\nDone.\n");
    return (min_balance < 0) ? 1 : 0;
}
