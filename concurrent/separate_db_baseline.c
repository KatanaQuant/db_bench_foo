/*
 * Separate-DB baseline: SQLite with one database file per thread.
 *
 * Tests the question: if FrankenSQLite's "3.7x faster" benchmark gives
 * each thread its own table (zero contention), would SQLite with
 * separate database files per thread be even faster?
 *
 * If yes, the MVCC overhead is negative-value even for the easy case.
 * You'd get better performance from N SQLite files than from one
 * FrankenSQLite instance with MVCC.
 *
 * Compile:
 *   gcc -O2 separate_db_baseline.c -I./include -lsqlite3 -lpthread -o separate_db_sqlite
 *   gcc -O2 separate_db_baseline.c -I./include /usr/lib/x86_64-linux-gnu/libsqlite3.so.0 \
 *       -lpthread -o separate_db_sqlite
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>
#include "sqlite3.h"

/* ---------- config ---------- */
#define ROWS_PER_THREAD  1000
#define MAX_THREADS      32
#define THREAD_COUNTS    6
static const int thread_counts[THREAD_COUNTS] = {1, 2, 4, 8, 16, 32};

/* ---------- timing ---------- */
static double now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

/* ---------- helpers ---------- */
static void exec(sqlite3 *db, const char *sql) {
    char *err = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n  %.120s\n", err, sql);
        sqlite3_free(err);
    }
}

/* ---------- thread context ---------- */
typedef struct {
    int thread_id;
    const char *db_dir;
    int use_separate_files;  /* 1 = separate .db per thread, 0 = shared .db */
    double elapsed_ms;
    long rows_inserted;
} thread_ctx_t;

/* ---------- worker ---------- */
static void *worker(void *arg) {
    thread_ctx_t *ctx = (thread_ctx_t *)arg;
    sqlite3 *db;
    char dbpath[512];

    if (ctx->use_separate_files) {
        snprintf(dbpath, sizeof(dbpath), "%s/thread_%d.db", ctx->db_dir, ctx->thread_id);
    } else {
        snprintf(dbpath, sizeof(dbpath), "%s/shared.db", ctx->db_dir);
    }

    if (sqlite3_open(dbpath, &db) != SQLITE_OK) {
        fprintf(stderr, "Thread %d: open failed\n", ctx->thread_id);
        return NULL;
    }

    exec(db, "PRAGMA journal_mode=WAL");
    exec(db, "PRAGMA synchronous=NORMAL");
    exec(db, "PRAGMA busy_timeout=5000");

    if (ctx->use_separate_files) {
        /* each thread owns its own db, create table */
        exec(db, "CREATE TABLE IF NOT EXISTS bench (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)");
    }
    /* for shared mode, table created by setup */

    char table_name[64];
    if (ctx->use_separate_files) {
        snprintf(table_name, sizeof(table_name), "bench");
    } else {
        snprintf(table_name, sizeof(table_name), "bench_%d", ctx->thread_id);
    }

    double t0 = now_ms();

    /* match FrankenSQLite's benchmark exactly: 1 row per transaction */
    for (int i = 0; i < ROWS_PER_THREAD; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
            "INSERT INTO %s VALUES (%d, ('t' || %d), (%d * 7))",
            table_name, i, i, i);

        /* retry on busy */
        int done = 0;
        for (int attempt = 0; attempt < 50 && !done; attempt++) {
            char *err = NULL;
            int rc = sqlite3_exec(db, sql, NULL, NULL, &err);
            if (rc == SQLITE_OK) {
                done = 1;
                ctx->rows_inserted++;
            } else if (rc == SQLITE_BUSY || rc == SQLITE_LOCKED) {
                if (err) sqlite3_free(err);
                usleep(100);
            } else {
                if (err) {
                    fprintf(stderr, "Thread %d: %s\n", ctx->thread_id, err);
                    sqlite3_free(err);
                }
                done = 1;
            }
        }
    }

    ctx->elapsed_ms = now_ms() - t0;
    sqlite3_close(db);
    return NULL;
}

/* ---------- run one level ---------- */
static void run_level(const char *db_dir, int n_threads, int use_separate_files) {
    pthread_t threads[MAX_THREADS];
    thread_ctx_t ctxs[MAX_THREADS];
    memset(ctxs, 0, sizeof(ctxs));

    /* clean up */
    for (int i = 0; i < n_threads; i++) {
        char path[512];
        snprintf(path, sizeof(path), "%s/thread_%d.db", db_dir, i);
        remove(path);
        snprintf(path, sizeof(path), "%s/thread_%d.db-wal", db_dir, i);
        remove(path);
        snprintf(path, sizeof(path), "%s/thread_%d.db-shm", db_dir, i);
        remove(path);
    }
    {
        char path[512];
        snprintf(path, sizeof(path), "%s/shared.db", db_dir);
        remove(path);
        snprintf(path, sizeof(path), "%s/shared.db-wal", db_dir);
        remove(path);
        snprintf(path, sizeof(path), "%s/shared.db-shm", db_dir);
        remove(path);
    }

    /* for shared mode, create all tables upfront */
    if (!use_separate_files) {
        char shared_path[512];
        snprintf(shared_path, sizeof(shared_path), "%s/shared.db", db_dir);
        sqlite3 *setup;
        sqlite3_open(shared_path, &setup);
        exec(setup, "PRAGMA journal_mode=WAL");
        exec(setup, "PRAGMA synchronous=NORMAL");
        for (int i = 0; i < n_threads; i++) {
            char sql[256];
            snprintf(sql, sizeof(sql),
                "CREATE TABLE bench_%d (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)", i);
            exec(setup, sql);
        }
        sqlite3_close(setup);
    }

    for (int i = 0; i < n_threads; i++) {
        ctxs[i].thread_id = i;
        ctxs[i].db_dir = db_dir;
        ctxs[i].use_separate_files = use_separate_files;
    }

    double wall_start = now_ms();

    for (int i = 0; i < n_threads; i++)
        pthread_create(&threads[i], NULL, worker, &ctxs[i]);
    for (int i = 0; i < n_threads; i++)
        pthread_join(threads[i], NULL);

    double wall_ms = now_ms() - wall_start;

    long total_rows = 0;
    for (int i = 0; i < n_threads; i++)
        total_rows += ctxs[i].rows_inserted;

    double throughput = total_rows / (wall_ms / 1000.0);
    printf("  %2d threads  %8.1f ms  %8.0f rows/s\n",
           n_threads, wall_ms, throughput);
}

/* ---------- main ---------- */
int main(int argc, char **argv) {
    const char *db_dir = (argc > 1) ? argv[1] : "/tmp/separate_db_test";

    /* create dir */
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "mkdir -p %s", db_dir);
    system(cmd);

    printf("=== Separate-DB Baseline ===\n");
    printf("Directory: %s\n", db_dir);
    printf("Rows/thread: %d\n\n", ROWS_PER_THREAD);

    printf("--- SQLite: shared DB, separate tables (FrankenSQLite's benchmark design) ---\n");
    for (int i = 0; i < THREAD_COUNTS; i++)
        run_level(db_dir, thread_counts[i], 0);

    printf("\n--- SQLite: separate DB file per thread (true zero-contention) ---\n");
    for (int i = 0; i < THREAD_COUNTS; i++)
        run_level(db_dir, thread_counts[i], 1);

    printf("\nDone.\n");
    return 0;
}
