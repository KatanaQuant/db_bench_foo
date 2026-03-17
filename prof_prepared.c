/* prof_prepared.c - Compare prepared statements vs inline for SELECT BY ID and INSERT
 * Note: FrankenSQLite doesn't export sqlite3_bind_*, so we use inline values
 * even with prepare_v2. The key difference is: prepare_v2+step skips re-parse
 * on first call but still parses the SQL once per prepare. */
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
    sqlite3 *db;
    char sql[256];
    double t0, elapsed;
    int rc;

    sqlite3_open(dbname, &db);
    sqlite3_exec(db, "PRAGMA journal_mode=WAL", NULL, NULL, NULL);

    /* === INSERT: inline exec vs prepare+step === */
    printf("=== INSERT (TXN BATCH) ===\n");

    /* Inline INSERT */
    sqlite3_exec(db, "CREATE TABLE t_inline (id INTEGER PRIMARY KEY, name TEXT, value REAL)", NULL, NULL, NULL);
    sqlite3_exec(db, "BEGIN", NULL, NULL, NULL);
    t0 = now_ms();
    for (int i = 0; i < count; i++) {
        snprintf(sql, sizeof(sql),
            "INSERT INTO t_inline VALUES(%d, 'name_%d', %f)", i, i, (double)i * 1.1);
        sqlite3_exec(db, sql, NULL, NULL, NULL);
    }
    elapsed = now_ms() - t0;
    sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
    printf("  sqlite3_exec TXN INSERT %d: %.1fms\n", count, elapsed);

    /* Prepared INSERT (prepare per statement, step once) */
    sqlite3_exec(db, "CREATE TABLE t_prep (id INTEGER PRIMARY KEY, name TEXT, value REAL)", NULL, NULL, NULL);
    sqlite3_exec(db, "BEGIN", NULL, NULL, NULL);
    t0 = now_ms();
    for (int i = 0; i < count; i++) {
        snprintf(sql, sizeof(sql),
            "INSERT INTO t_prep VALUES(%d, 'name_%d', %f)", i, i, (double)i * 1.1);
        sqlite3_stmt *stmt;
        rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
        if (rc != SQLITE_OK) { printf("prep fail %d\n", rc); break; }
        sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }
    elapsed = now_ms() - t0;
    sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
    printf("  prepare+step TXN INSERT %d: %.1fms\n", count, elapsed);

    /* === SELECT BY ID: exec vs prepare+step === */
    printf("\n=== SELECT BY ID ===\n");

    /* Inline SELECT */
    t0 = now_ms();
    for (int i = 0; i < count; i++) {
        snprintf(sql, sizeof(sql), "SELECT * FROM t_inline WHERE rowid=%d", i);
        sqlite3_exec(db, sql, noop_cb, NULL, NULL);
    }
    elapsed = now_ms() - t0;
    printf("  sqlite3_exec SELECT BY ID %d: %.1fms\n", count, elapsed);

    /* Prepared SELECT (prepare per statement, step to consume) */
    t0 = now_ms();
    for (int i = 0; i < count; i++) {
        snprintf(sql, sizeof(sql), "SELECT * FROM t_inline WHERE rowid=%d", i);
        sqlite3_stmt *stmt;
        rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
        if (rc != SQLITE_OK) { printf("prep fail %d\n", rc); break; }
        while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
            (void)sqlite3_column_int(stmt, 0);
            (void)sqlite3_column_text(stmt, 1);
            (void)sqlite3_column_double(stmt, 2);
        }
        sqlite3_finalize(stmt);
    }
    elapsed = now_ms() - t0;
    printf("  prepare+step SELECT BY ID %d: %.1fms\n", count, elapsed);

    sqlite3_close(db);
    return 0;
}
