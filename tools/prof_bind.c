/* prof_bind.c - Benchmark true prepared statements: prepare once, bind+step+reset loop
 * Compares: F bind vs L bind, F bind vs F exec */
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

    /* === INSERT: bind loop vs inline exec === */
    printf("=== INSERT ===\n");

    /* Bind INSERT: prepare once, bind+step+reset 10K times */
    sqlite3_exec(db, "CREATE TABLE t_bind (id INTEGER PRIMARY KEY, name TEXT, value REAL)", NULL, NULL, NULL);
    sqlite3_exec(db, "BEGIN", NULL, NULL, NULL);
    sqlite3_stmt *ins_stmt;
    rc = sqlite3_prepare_v2(db, "INSERT INTO t_bind VALUES(?, ?, ?)", -1, &ins_stmt, NULL);
    if (rc != SQLITE_OK) { printf("prepare fail %d\n", rc); return 1; }
    t0 = now_ms();
    for (int i = 0; i < count; i++) {
        sqlite3_bind_int(ins_stmt, 1, i);
        snprintf(sql, sizeof(sql), "name_%d", i);
        sqlite3_bind_text(ins_stmt, 2, sql, -1, NULL);
        sqlite3_bind_double(ins_stmt, 3, (double)i * 1.1);
        rc = sqlite3_step(ins_stmt);
        if (rc != SQLITE_DONE && rc != SQLITE_ROW) {
            printf("step fail %d at i=%d\n", rc, i);
            break;
        }
        sqlite3_reset(ins_stmt);
    }
    elapsed = now_ms() - t0;
    sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
    sqlite3_finalize(ins_stmt);
    printf("  bind INSERT %d: %.1fms\n", count, elapsed);

    /* Inline INSERT for comparison */
    sqlite3_exec(db, "CREATE TABLE t_exec (id INTEGER PRIMARY KEY, name TEXT, value REAL)", NULL, NULL, NULL);
    sqlite3_exec(db, "BEGIN", NULL, NULL, NULL);
    t0 = now_ms();
    for (int i = 0; i < count; i++) {
        snprintf(sql, sizeof(sql),
            "INSERT INTO t_exec VALUES(%d, 'name_%d', %f)", i, i, (double)i * 1.1);
        sqlite3_exec(db, sql, NULL, NULL, NULL);
    }
    elapsed = now_ms() - t0;
    sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
    printf("  exec INSERT %d: %.1fms\n", count, elapsed);

    /* === SELECT BY ID: bind loop vs inline exec === */
    printf("\n=== SELECT BY ID ===\n");

    /* Bind SELECT: prepare once, bind+step+reset 10K times */
    sqlite3_stmt *sel_stmt;
    rc = sqlite3_prepare_v2(db, "SELECT * FROM t_bind WHERE rowid=?", -1, &sel_stmt, NULL);
    if (rc != SQLITE_OK) { printf("prepare fail %d\n", rc); return 1; }
    t0 = now_ms();
    for (int i = 0; i < count; i++) {
        sqlite3_bind_int(sel_stmt, 1, i);
        while ((rc = sqlite3_step(sel_stmt)) == SQLITE_ROW) {
            (void)sqlite3_column_int(sel_stmt, 0);
            (void)sqlite3_column_text(sel_stmt, 1);
            (void)sqlite3_column_double(sel_stmt, 2);
        }
        sqlite3_reset(sel_stmt);
    }
    elapsed = now_ms() - t0;
    sqlite3_finalize(sel_stmt);
    printf("  bind SELECT BY ID %d: %.1fms\n", count, elapsed);

    /* Inline SELECT for comparison */
    t0 = now_ms();
    for (int i = 0; i < count; i++) {
        snprintf(sql, sizeof(sql), "SELECT * FROM t_bind WHERE rowid=%d", i);
        sqlite3_exec(db, sql, noop_cb, NULL, NULL);
    }
    elapsed = now_ms() - t0;
    printf("  exec SELECT BY ID %d: %.1fms\n", count, elapsed);

    sqlite3_close(db);
    return 0;
}
