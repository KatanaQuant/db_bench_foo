/* prof_select.c - Profile SELECT BY ID and SELECT ALL paths */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "sqlite3.h"

static double now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

static int noop_cb(void *d, int n, char **v, char **c) { (void)d;(void)n;(void)v;(void)c; return 0; }

int main(int argc, char **argv) {
    int count = argc > 1 ? atoi(argv[1]) : 10000;
    const char *dbname = argc > 2 ? argv[2] : ":memory:";
    const char *mode = argc > 3 ? argv[3] : "byid"; /* "byid" or "all" */
    sqlite3 *db;
    char sql[256];

    sqlite3_open(dbname, &db);
    sqlite3_exec(db, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, value REAL)", NULL, NULL, NULL);
    sqlite3_exec(db, "PRAGMA journal_mode=WAL", NULL, NULL, NULL);

    /* Load data */
    sqlite3_exec(db, "BEGIN", NULL, NULL, NULL);
    for (int i = 0; i < count; i++) {
        snprintf(sql, sizeof(sql),
            "INSERT INTO test VALUES(%d, 'name_%d', %f)", i, i, (double)i * 1.1);
        sqlite3_exec(db, sql, NULL, NULL, NULL);
    }
    sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);

    if (strcmp(mode, "all") == 0) {
        /* SELECT ALL */
        double t0 = now_ms();
        sqlite3_exec(db, "SELECT * FROM test", noop_cb, NULL, NULL);
        double elapsed = now_ms() - t0;
        printf("SELECT ALL %d records: %.1fms\n", count, elapsed);
    } else {
        /* SELECT BY ID */
        double t0 = now_ms();
        for (int i = 0; i < count; i++) {
            snprintf(sql, sizeof(sql), "SELECT * FROM test WHERE rowid=%d", i);
            sqlite3_exec(db, sql, noop_cb, NULL, NULL);
        }
        double elapsed = now_ms() - t0;
        printf("SELECT BY ID %d lookups: %.1fms\n", count, elapsed);
    }

    sqlite3_close(db);
    return 0;
}
