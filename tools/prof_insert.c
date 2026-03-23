/* prof_insert.c - Profile INSERT autocommit path (10K single-statement INSERTs) */
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

int main(int argc, char **argv) {
    int count = argc > 1 ? atoi(argv[1]) : 10000;
    const char *dbname = argc > 2 ? argv[2] : ":memory:";
    sqlite3 *db;
    char sql[256];

    sqlite3_open(dbname, &db);
    sqlite3_exec(db, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, value REAL)", NULL, NULL, NULL);
    sqlite3_exec(db, "PRAGMA journal_mode=WAL", NULL, NULL, NULL);

    double t0 = now_ms();
    for (int i = 0; i < count; i++) {
        snprintf(sql, sizeof(sql),
            "INSERT INTO test VALUES(%d, 'name_%d', %f)", i, i, (double)i * 1.1);
        sqlite3_exec(db, sql, NULL, NULL, NULL);
    }
    double elapsed = now_ms() - t0;
    printf("INSERT %d records: %.1fms\n", count, elapsed);

    sqlite3_close(db);
    return 0;
}
