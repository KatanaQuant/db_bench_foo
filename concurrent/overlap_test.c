/*
 * Transaction overlap test.
 *
 * The simplest possible concurrency test: can two transactions
 * exist at the same time?
 *
 * Each thread: BEGIN → INSERT → sleep 100ms → COMMIT
 * If concurrent: total ≈ 100ms (they overlap)
 * If serialized: total ≈ N * 100ms (they queue)
 *
 * Compile:
 *   gcc -O2 overlap_test.c -I./include -lsqlite3 -lpthread -o overlap_sqlite
 *   gcc -O2 overlap_test.c -I./include -L$KQSQL -lfsqlite_c_api \
 *       -lpthread -Wl,-rpath,$KQSQL -o overlap_fsqlite
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>
#include "sqlite3.h"

#define NUM_THREADS 4
#define SLEEP_MS 100

static double now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

static void exec(sqlite3 *db, const char *sql) {
    char *err = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n  %.120s\n", err, sql);
        sqlite3_free(err);
    }
}

static int exec_rc(sqlite3 *db, const char *sql) {
    char *err = NULL;
    int rc = sqlite3_exec(db, sql, NULL, NULL, &err);
    if (err) sqlite3_free(err);
    return rc;
}

typedef struct {
    int thread_id;
    const char *dbpath;
    int use_concurrent;
    double start_ms;
    double begin_ms;
    double commit_ms;
    int success;
} thread_ctx_t;

static pthread_barrier_t barrier;

static void *worker(void *arg) {
    thread_ctx_t *ctx = (thread_ctx_t *)arg;
    sqlite3 *db;

    if (sqlite3_open(ctx->dbpath, &db) != SQLITE_OK) {
        fprintf(stderr, "Thread %d: open failed\n", ctx->thread_id);
        ctx->success = 0;
        return NULL;
    }

    exec(db, "PRAGMA journal_mode=WAL");
    exec(db, "PRAGMA busy_timeout=10000");

    /* all threads start together */
    pthread_barrier_wait(&barrier);

    ctx->start_ms = now_ms();

    /* BEGIN */
    int rc;
    if (ctx->use_concurrent) {
        rc = exec_rc(db, "BEGIN CONCURRENT");
        if (rc != SQLITE_OK) {
            fprintf(stderr, "WARNING: BEGIN CONCURRENT failed (rc=%d), falling back to BEGIN\n", rc);
            exec(db, "BEGIN");
        }
    } else {
        exec(db, "BEGIN");
    }
    ctx->begin_ms = now_ms();

    /* do a write so we're a write transaction */
    char sql[256];
    snprintf(sql, sizeof(sql),
        "INSERT INTO overlap_test VALUES (%d, 'thread_%d')",
        ctx->thread_id + 1000, ctx->thread_id);
    exec(db, sql);

    /* sleep — this is the overlap window */
    usleep(SLEEP_MS * 1000);

    /* COMMIT */
    rc = exec_rc(db, "COMMIT");
    ctx->commit_ms = now_ms();
    ctx->success = (rc == SQLITE_OK) ? 1 : 0;

    if (rc != SQLITE_OK) {
        exec_rc(db, "ROLLBACK");
    }

    sqlite3_close(db);
    return NULL;
}

int main(int argc, char **argv) {
    const char *dbpath = (argc > 1) ? argv[1] : "/tmp/overlap_test.db";
    const char *mode   = (argc > 2) ? argv[2] : "auto";
    int use_concurrent = (strcmp(mode, "concurrent") == 0);

    printf("=== Transaction Overlap Test ===\n");
    printf("Database: %s\n", dbpath);
    printf("Mode: %s\n", use_concurrent ? "BEGIN CONCURRENT" : "BEGIN (default)");
    printf("Threads: %d, Sleep: %d ms each\n", NUM_THREADS, SLEEP_MS);
    printf("Expected if concurrent: ~%d ms\n", SLEEP_MS);
    printf("Expected if serialized: ~%d ms\n\n", SLEEP_MS * NUM_THREADS);

    /* setup */
    remove(dbpath);
    { char p[512]; snprintf(p,sizeof(p),"%s-wal",dbpath); remove(p);
      snprintf(p,sizeof(p),"%s-shm",dbpath); remove(p); }

    sqlite3 *setup;
    sqlite3_open(dbpath, &setup);
    exec(setup, "PRAGMA journal_mode=WAL");
    exec(setup, "CREATE TABLE overlap_test (id INTEGER PRIMARY KEY, name TEXT)");
    sqlite3_close(setup);

    pthread_barrier_init(&barrier, NULL, NUM_THREADS);

    pthread_t threads[NUM_THREADS];
    thread_ctx_t ctxs[NUM_THREADS];

    for (int i = 0; i < NUM_THREADS; i++) {
        ctxs[i].thread_id = i;
        ctxs[i].dbpath = dbpath;
        ctxs[i].use_concurrent = use_concurrent;
    }

    double wall_start = now_ms();

    for (int i = 0; i < NUM_THREADS; i++)
        pthread_create(&threads[i], NULL, worker, &ctxs[i]);
    for (int i = 0; i < NUM_THREADS; i++)
        pthread_join(threads[i], NULL);

    double wall_ms = now_ms() - wall_start;

    printf("--- Per-thread timing ---\n");
    for (int i = 0; i < NUM_THREADS; i++) {
        printf("  Thread %d: begin=%.1fms commit=%.1fms total=%.1fms %s\n",
               i,
               ctxs[i].begin_ms - ctxs[i].start_ms,
               ctxs[i].commit_ms - ctxs[i].begin_ms,
               ctxs[i].commit_ms - ctxs[i].start_ms,
               ctxs[i].success ? "OK" : "FAILED");
    }

    printf("\n--- Result ---\n");
    printf("Wall clock: %.1f ms\n", wall_ms);
    printf("Expected if concurrent: ~%d ms\n", SLEEP_MS);
    printf("Expected if serialized: ~%d ms\n", SLEEP_MS * NUM_THREADS);

    double ratio = wall_ms / SLEEP_MS;
    printf("Ratio: %.1fx (1.0 = fully concurrent, %d.0 = fully serialized)\n",
           ratio, NUM_THREADS);

    if (ratio < 1.5) {
        printf("\nCONCURRENT: Transactions overlapped.\n");
    } else if (ratio > (NUM_THREADS - 0.5)) {
        printf("\nSERIALIZED: Transactions queued. No concurrency.\n");
    } else {
        printf("\nPARTIAL: Some overlap, some serialization.\n");
    }

    pthread_barrier_destroy(&barrier);
    printf("\nDone.\n");
    return 0;
}
