/*
 * Elle-style write skew detector for SQLite-compatible databases.
 *
 * Tests whether the database allows write skew anomalies under
 * concurrent writes. Write skew occurs when two transactions both
 * read overlapping data, make decisions based on it, and both commit
 * without either being aborted.
 *
 * Classic example (Kleppmann): two doctors check the on-call schedule,
 * both see two doctors on duty, both take themselves off call, nobody
 * is left. A serializable database must abort one of them.
 *
 * This test uses a simpler numeric invariant: a table of accounts with
 * a constraint that the total balance must never go negative. Each
 * transaction reads the total, and if it's >= 100, withdraws 100 from
 * one account. Under serializability, only one of two concurrent
 * withdrawals can succeed. Under broken isolation, both succeed and
 * the total goes negative.
 *
 * Compile:
 *   gcc -O2 elle_write_skew.c -I./include -lsqlite3 -lpthread -o elle_sqlite
 *   gcc -O2 elle_write_skew.c -I./include -L$KQSQL -lfsqlite_c_api \
 *       -lpthread -Wl,-rpath,$KQSQL -o elle_fsqlite
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>
#include <stdatomic.h>
#include "sqlite3.h"

/* ---------- config ---------- */
#define NUM_ACCOUNTS     5
#define INITIAL_BALANCE  200    /* per account, total = 1000 */
#define WITHDRAW_AMOUNT  100
#define NUM_THREADS      4
#define ROUNDS_PER_THREAD 500
#define INVARIANT_MIN    0      /* total balance must never go below this */

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
        fprintf(stderr, "SQL error: %s\n  %.200s\n", err, sql);
        sqlite3_free(err);
    }
}

/* callback to read a single integer */
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

/* ---------- thread context ---------- */
typedef struct {
    int thread_id;
    const char *dbpath;
    int use_concurrent;

    /* results */
    long commits;
    long aborts;
    long retries;
    long violations_detected;   /* times we read a negative total */
} thread_ctx_t;

/* ---------- global counters ---------- */
static atomic_long g_total_violations = 0;
static atomic_long g_total_commits = 0;
static atomic_long g_total_aborts = 0;

/* ---------- worker ---------- */
static void *worker(void *arg) {
    thread_ctx_t *ctx = (thread_ctx_t *)arg;
    sqlite3 *db;

    if (sqlite3_open(ctx->dbpath, &db) != SQLITE_OK)
        die("worker open", db);

    exec(db, "PRAGMA journal_mode=WAL");
    exec(db, "PRAGMA busy_timeout=5000");

    ctx->commits = 0;
    ctx->aborts = 0;
    ctx->retries = 0;
    ctx->violations_detected = 0;

    for (int round = 0; round < ROUNDS_PER_THREAD; round++) {
        int account = rand() % NUM_ACCOUNTS;
        int committed = 0;

        for (int attempt = 0; attempt < 20 && !committed; attempt++) {
            /* begin transaction */
            int rc;
            if (ctx->use_concurrent) {
                rc = exec_rc(db, "BEGIN CONCURRENT");
                if (rc != SQLITE_OK) {
                    /* fall back if not supported */
                    rc = exec_rc(db, "BEGIN");
                }
            } else {
                rc = exec_rc(db, "BEGIN");
            }

            if (rc == SQLITE_BUSY) {
                ctx->retries++;
                usleep(100 + (rand() % 500));
                continue;
            }

            /*
             * THE WRITE SKEW TEST:
             *
             * 1. Read total balance across all accounts
             * 2. If total >= WITHDRAW_AMOUNT, withdraw from one account
             * 3. Commit
             *
             * Under serializability: if two threads both read total=200
             * and both withdraw 100, one MUST be aborted because the
             * second withdrawal would bring total to 0 (or below the
             * threshold that the first reader saw).
             *
             * Under broken isolation: both commit. Total drops by 200
             * when only 100 should have been allowed.
             */

            int total = query_int(db,
                "SELECT SUM(balance) FROM accounts", -1);

            if (total >= WITHDRAW_AMOUNT) {
                char sql[256];
                snprintf(sql, sizeof(sql),
                    "UPDATE accounts SET balance = balance - %d WHERE id = %d",
                    WITHDRAW_AMOUNT, account);
                rc = exec_rc(db, sql);

                if (rc == SQLITE_BUSY) {
                    exec_rc(db, "ROLLBACK");
                    ctx->retries++;
                    usleep(100 + (rand() % 500));
                    continue;
                }
            }

            /* try to commit */
            rc = exec_rc(db, "COMMIT");
            if (rc == SQLITE_OK) {
                committed = 1;
                ctx->commits++;
            } else if (rc == SQLITE_BUSY || rc == SQLITE_LOCKED) {
                exec_rc(db, "ROLLBACK");
                ctx->aborts++;
                usleep(100 + (rand() % 500));
            } else {
                /* some other error */
                exec_rc(db, "ROLLBACK");
                ctx->aborts++;
            }
        }

        /*
         * Periodically check the invariant from this thread's connection.
         * If total < INVARIANT_MIN, we have a write skew violation.
         */
        if (round % 50 == 0) {
            int check = query_int(db,
                "SELECT SUM(balance) FROM accounts", 0);
            if (check < INVARIANT_MIN) {
                ctx->violations_detected++;
                atomic_fetch_add(&g_total_violations, 1);
            }
        }
    }

    sqlite3_close(db);
    return NULL;
}

/* ---------- main ---------- */
int main(int argc, char **argv) {
    const char *dbpath = (argc > 1) ? argv[1] : "/tmp/elle_write_skew.db";
    const char *mode   = (argc > 2) ? argv[2] : "auto";
    int use_concurrent = (strcmp(mode, "concurrent") == 0);

    printf("=== Elle Write Skew Test ===\n");
    printf("Database: %s\n", dbpath);
    printf("Mode: %s\n", use_concurrent ? "BEGIN CONCURRENT" : "BEGIN (default)");
    printf("Accounts: %d x %d initial = %d total\n",
           NUM_ACCOUNTS, INITIAL_BALANCE, NUM_ACCOUNTS * INITIAL_BALANCE);
    printf("Threads: %d, Rounds/thread: %d\n", NUM_THREADS, ROUNDS_PER_THREAD);
    printf("Withdraw amount: %d\n", WITHDRAW_AMOUNT);
    printf("Invariant: total balance >= %d\n\n", INVARIANT_MIN);

    /* setup */
    remove(dbpath);
    {
        char wal_path[512], shm_path[512];
        snprintf(wal_path, sizeof(wal_path), "%s-wal", dbpath);
        snprintf(shm_path, sizeof(shm_path), "%s-shm", dbpath);
        remove(wal_path);
        remove(shm_path);
    }

    sqlite3 *setup_db;
    if (sqlite3_open(dbpath, &setup_db) != SQLITE_OK)
        die("setup open", setup_db);

    exec(setup_db, "PRAGMA journal_mode=WAL");
    exec(setup_db, "PRAGMA synchronous=NORMAL");
    exec(setup_db, "CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER NOT NULL)");

    for (int i = 0; i < NUM_ACCOUNTS; i++) {
        char sql[128];
        snprintf(sql, sizeof(sql),
            "INSERT INTO accounts VALUES (%d, %d)", i, INITIAL_BALANCE);
        exec(setup_db, sql);
    }

    /* verify setup */
    int initial_total = query_int(setup_db,
        "SELECT SUM(balance) FROM accounts", -1);
    printf("Initial total balance: %d\n", initial_total);
    sqlite3_close(setup_db);

    /* run concurrent workers */
    pthread_t threads[NUM_THREADS];
    thread_ctx_t ctxs[NUM_THREADS];
    srand(42);

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

    /* final check */
    sqlite3 *check_db;
    if (sqlite3_open(dbpath, &check_db) != SQLITE_OK)
        die("check open", check_db);

    int final_total = query_int(check_db,
        "SELECT SUM(balance) FROM accounts", -1);
    int min_balance = query_int(check_db,
        "SELECT MIN(balance) FROM accounts", 0);

    printf("\n--- Results ---\n");
    printf("Duration: %.1f ms\n", elapsed);

    long total_commits = 0, total_aborts = 0, total_retries = 0;
    for (int i = 0; i < NUM_THREADS; i++) {
        total_commits += ctxs[i].commits;
        total_aborts += ctxs[i].aborts;
        total_retries += ctxs[i].retries;
        printf("  Thread %d: %ld commits, %ld aborts, %ld retries\n",
               i, ctxs[i].commits, ctxs[i].aborts, ctxs[i].retries);
    }

    printf("\nTotal commits: %ld\n", total_commits);
    printf("Total aborts:  %ld\n", total_aborts);
    printf("Total retries: %ld\n", total_retries);
    printf("\nInitial total balance: %d\n", initial_total);
    printf("Final total balance:   %d\n", final_total);
    printf("Minimum account balance: %d\n", min_balance);
    printf("Violations detected during run: %ld\n",
           atomic_load(&g_total_violations));

    /* THE VERDICT */
    printf("\n--- Isolation Verdict ---\n");

    if (final_total < INVARIANT_MIN) {
        printf("WRITE SKEW DETECTED: final total %d < %d\n",
               final_total, INVARIANT_MIN);
        printf("The database allowed concurrent transactions to\n");
        printf("independently violate a global invariant.\n");
        printf("This is NOT serializable isolation.\n");
    } else if (total_aborts == 0 && total_commits > NUM_THREADS) {
        printf("SUSPICIOUS: %ld commits, 0 aborts on shared data.\n",
               total_commits);
        printf("A serializable database should produce SOME aborts\n");
        printf("when concurrent transactions read-modify-write the\n");
        printf("same rows. Zero aborts suggests conflict detection\n");
        printf("is not running.\n");
        if (final_total >= INVARIANT_MIN) {
            printf("Note: invariant was preserved, likely because the\n");
            printf("global mutex serialized all commits (preventing\n");
            printf("actual concurrent overlap).\n");
        }
    } else if (total_aborts > 0) {
        printf("SERIALIZABLE: %ld commits, %ld aborts.\n",
               total_commits, total_aborts);
        printf("The database detected conflicts and aborted\n");
        printf("transactions as expected.\n");
    } else {
        printf("INCONCLUSIVE: too few commits to determine.\n");
    }

    sqlite3_close(check_db);
    printf("\nDone.\n");
    return (final_total < INVARIANT_MIN) ? 1 : 0;
}
