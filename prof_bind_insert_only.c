/* prof_bind_insert_only.c - Profile bind INSERT path only */
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
    sqlite3 *db;
    char name[64];
    sqlite3_stmt *stmt;
    int rc;

    sqlite3_open(":memory:", &db);
    sqlite3_exec(db, "PRAGMA journal_mode=WAL", NULL, NULL, NULL);
    sqlite3_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, value REAL)", NULL, NULL, NULL);

    rc = sqlite3_prepare_v2(db, "INSERT INTO t VALUES(?, ?, ?)", -1, &stmt, NULL);
    if (rc != 0) { printf("prepare fail %d\n", rc); return 1; }

    sqlite3_exec(db, "BEGIN", NULL, NULL, NULL);
    double t0 = now_ms();
    for (int i = 0; i < count; i++) {
        sqlite3_bind_int(stmt, 1, i);
        snprintf(name, sizeof(name), "name_%d", i);
        sqlite3_bind_text(stmt, 2, name, -1, NULL);
        sqlite3_bind_double(stmt, 3, (double)i * 1.1);
        sqlite3_step(stmt);
        sqlite3_reset(stmt);
    }
    double elapsed = now_ms() - t0;
    sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
    sqlite3_finalize(stmt);
    printf("bind INSERT %d: %.1fms\n", count, elapsed);

    sqlite3_close(db);
    return 0;
}
