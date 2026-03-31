/*
 * Elle-style write skew test for PostgreSQL.
 *
 * Same test as elle_write_skew.c but using libpq.
 * Tests whether PostgreSQL's SERIALIZABLE isolation prevents write skew.
 *
 * Compile:
 *   gcc -O2 elle_write_skew_pg.c -I/usr/include/postgresql -lpq -lpthread -o elle_pg
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>
#include <stdatomic.h>
#include <libpq-fe.h>

/* ---------- config ---------- */
#define NUM_ACCOUNTS     5
#define INITIAL_BALANCE  200
#define WITHDRAW_AMOUNT  100
#define NUM_THREADS      4
#define ROUNDS_PER_THREAD 500
#define INVARIANT_MIN    0

/* ---------- timing ---------- */
static double now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

/* ---------- helpers ---------- */
static void pg_check(PGconn *conn, PGresult *res, const char *context) {
    if (PQresultStatus(res) != PGRES_COMMAND_OK &&
        PQresultStatus(res) != PGRES_TUPLES_OK) {
        /* don't print serialization failures — they're expected */
        const char *sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
        if (sqlstate && strcmp(sqlstate, "40001") == 0) {
            PQclear(res);
            return;
        }
        fprintf(stderr, "%s: %s", context, PQerrorMessage(conn));
    }
}

static int pg_query_int(PGconn *conn, const char *sql, int default_val) {
    PGresult *res = PQexec(conn, sql);
    int val = default_val;
    if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
        val = atoi(PQgetvalue(res, 0, 0));
    }
    PQclear(res);
    return val;
}

/* ---------- thread context ---------- */
typedef struct {
    int thread_id;
    const char *conninfo;
    int use_serializable;

    long commits;
    long aborts;
    long retries;
    long violations_detected;
} thread_ctx_t;

static atomic_long g_total_violations = 0;

/* ---------- worker ---------- */
static void *worker(void *arg) {
    thread_ctx_t *ctx = (thread_ctx_t *)arg;
    PGconn *conn = PQconnectdb(ctx->conninfo);

    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Thread %d: connection failed: %s\n",
                ctx->thread_id, PQerrorMessage(conn));
        return NULL;
    }

    ctx->commits = 0;
    ctx->aborts = 0;
    ctx->retries = 0;
    ctx->violations_detected = 0;

    for (int round = 0; round < ROUNDS_PER_THREAD; round++) {
        int account = rand() % NUM_ACCOUNTS;
        int committed = 0;

        for (int attempt = 0; attempt < 20 && !committed; attempt++) {
            PGresult *res;

            /* begin with chosen isolation level */
            if (ctx->use_serializable) {
                res = PQexec(conn, "BEGIN ISOLATION LEVEL SERIALIZABLE");
            } else {
                res = PQexec(conn, "BEGIN ISOLATION LEVEL READ COMMITTED");
            }
            PQclear(res);

            /* read total balance */
            int total = pg_query_int(conn,
                "SELECT SUM(balance) FROM accounts", -1);

            if (total >= WITHDRAW_AMOUNT) {
                char sql[256];
                snprintf(sql, sizeof(sql),
                    "UPDATE accounts SET balance = balance - %d WHERE id = %d",
                    WITHDRAW_AMOUNT, account);
                res = PQexec(conn, sql);

                const char *sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
                if (sqlstate && strcmp(sqlstate, "40001") == 0) {
                    /* serialization failure */
                    PQclear(res);
                    res = PQexec(conn, "ROLLBACK");
                    PQclear(res);
                    ctx->aborts++;
                    usleep(100 + (rand() % 500));
                    continue;
                }
                PQclear(res);
            }

            /* commit */
            res = PQexec(conn, "COMMIT");
            const char *sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
            if (sqlstate && strcmp(sqlstate, "40001") == 0) {
                /* serialization failure at commit */
                PQclear(res);
                res = PQexec(conn, "ROLLBACK");
                PQclear(res);
                ctx->aborts++;
                usleep(100 + (rand() % 500));
                continue;
            }

            if (PQresultStatus(res) == PGRES_COMMAND_OK) {
                committed = 1;
                ctx->commits++;
            } else {
                PQclear(res);
                res = PQexec(conn, "ROLLBACK");
                PQclear(res);
                ctx->aborts++;
            }
            if (res) PQclear(res);
        }

        /* periodic invariant check */
        if (round % 50 == 0) {
            int check = pg_query_int(conn,
                "SELECT SUM(balance) FROM accounts", 0);
            if (check < INVARIANT_MIN) {
                ctx->violations_detected++;
                atomic_fetch_add(&g_total_violations, 1);
            }
        }
    }

    PQfinish(conn);
    return NULL;
}

/* ---------- main ---------- */
int main(int argc, char **argv) {
    const char *conninfo = (argc > 1) ? argv[1] : "dbname=elle_test";
    const char *mode = (argc > 2) ? argv[2] : "serializable";
    int use_serializable = (strcmp(mode, "serializable") == 0);

    printf("=== Elle Write Skew Test (PostgreSQL) ===\n");
    printf("Connection: %s\n", conninfo);
    printf("Isolation: %s\n", use_serializable ? "SERIALIZABLE (SSI)" : "READ COMMITTED");
    printf("Accounts: %d x %d initial = %d total\n",
           NUM_ACCOUNTS, INITIAL_BALANCE, NUM_ACCOUNTS * INITIAL_BALANCE);
    printf("Threads: %d, Rounds/thread: %d\n", NUM_THREADS, ROUNDS_PER_THREAD);
    printf("Withdraw amount: %d\n\n", WITHDRAW_AMOUNT);

    /* setup */
    PGconn *setup = PQconnectdb(conninfo);
    if (PQstatus(setup) != CONNECTION_OK) {
        fprintf(stderr, "Setup connection failed: %s\n", PQerrorMessage(setup));
        return 1;
    }

    PGresult *res;
    res = PQexec(setup, "DROP TABLE IF EXISTS accounts");
    PQclear(res);
    res = PQexec(setup, "CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER NOT NULL)");
    PQclear(res);

    for (int i = 0; i < NUM_ACCOUNTS; i++) {
        char sql[128];
        snprintf(sql, sizeof(sql),
            "INSERT INTO accounts VALUES (%d, %d)", i, INITIAL_BALANCE);
        res = PQexec(setup, sql);
        PQclear(res);
    }

    int initial_total = pg_query_int(setup,
        "SELECT SUM(balance) FROM accounts", -1);
    printf("Initial total balance: %d\n", initial_total);
    PQfinish(setup);

    /* run */
    pthread_t threads[NUM_THREADS];
    thread_ctx_t ctxs[NUM_THREADS];
    srand(42);

    double t0 = now_ms();

    for (int i = 0; i < NUM_THREADS; i++) {
        ctxs[i].thread_id = i;
        ctxs[i].conninfo = conninfo;
        ctxs[i].use_serializable = use_serializable;
    }

    for (int i = 0; i < NUM_THREADS; i++)
        pthread_create(&threads[i], NULL, worker, &ctxs[i]);
    for (int i = 0; i < NUM_THREADS; i++)
        pthread_join(threads[i], NULL);

    double elapsed = now_ms() - t0;

    /* check */
    PGconn *check = PQconnectdb(conninfo);
    int final_total = pg_query_int(check,
        "SELECT SUM(balance) FROM accounts", -1);
    int min_balance = pg_query_int(check,
        "SELECT MIN(balance) FROM accounts", 0);

    printf("\n--- Results ---\n");
    printf("Duration: %.1f ms\n", elapsed);

    long total_commits = 0, total_aborts = 0;
    for (int i = 0; i < NUM_THREADS; i++) {
        total_commits += ctxs[i].commits;
        total_aborts += ctxs[i].aborts;
        printf("  Thread %d: %ld commits, %ld aborts\n",
               i, ctxs[i].commits, ctxs[i].aborts);
    }

    printf("\nTotal commits: %ld\n", total_commits);
    printf("Total aborts:  %ld\n", total_aborts);
    printf("Initial total balance: %d\n", initial_total);
    printf("Final total balance:   %d\n", final_total);
    printf("Minimum account balance: %d\n", min_balance);

    printf("\n--- Isolation Verdict ---\n");
    if (total_aborts > 0 && final_total >= INVARIANT_MIN) {
        printf("SERIALIZABLE: %ld commits, %ld aborts.\n",
               total_commits, total_aborts);
        printf("Conflicts detected and aborted. Invariant preserved.\n");
        printf("This is what working SSI looks like.\n");
    } else if (final_total < INVARIANT_MIN) {
        printf("WRITE SKEW: final total %d < %d.\n",
               final_total, INVARIANT_MIN);
        printf("Invariant violated.\n");
    } else if (total_aborts == 0) {
        printf("NO ABORTS: %ld commits, 0 aborts.\n", total_commits);
        printf("Either no contention occurred or isolation is weak.\n");
    }

    PQfinish(check);
    printf("\nDone.\n");
    return 0;
}
