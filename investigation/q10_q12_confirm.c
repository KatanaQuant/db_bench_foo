/*
 * Final confirmation: pin down the two FrankenSQLite bugs
 * Bug 1: Multi-table FROM (implicit cross join) column resolution
 * Bug 2: sqlite3_column_text() returns NULL for all columns
 *
 * Build FrankenSQLite:
 *   FSQL=frankensqlite/target/release
 *   gcc -O2 -DENGINE_LABEL='"FrankenSQLite"' -o q10_q12_confirm_fsqlite q10_q12_confirm.c -I./include \
 *     -L$FSQL -lfsqlite_c_api -Wl,-rpath,$FSQL
 */
#include <stdio.h>
#include <stdlib.h>
#include "sqlite3.h"

static void exec_or_die(sqlite3 *db, const char *sql) {
    char *err = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n  SQL: %.200s\n", err, sql);
        sqlite3_free(err);
    }
}

static void run_show_typed(sqlite3 *db, const char *label, const char *sql) {
    sqlite3_stmt *stmt;
    int rows = 0;
    printf("\n  [%s]\n  SQL: %s\n", label, sql);
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        printf("  PREPARE ERROR: %s\n", sqlite3_errmsg(db));
        return;
    }
    int ncols = sqlite3_column_count(stmt);
    printf("  ncols=%d\n", ncols);
    while (sqlite3_step(stmt) == SQLITE_ROW && rows < 3) {
        rows++;
        printf("  row %d:", rows);
        for (int k = 0; k < ncols && k < 8; k++) {
            int type = sqlite3_column_type(stmt, k);
            const char *type_str = (type==1)?"INT":(type==2)?"FLOAT":(type==3)?"TEXT":(type==4)?"BLOB":"NULL";
            const char *v = (const char *)sqlite3_column_text(stmt, k);
            int vi = sqlite3_column_int(stmt, k);
            printf(" col%d[type=%s text=%s int=%d]", k, type_str, v ? v : "NULL", vi);
        }
        printf("\n");
    }
    sqlite3_finalize(stmt);
    if (rows == 0) printf("  => 0 rows\n");
}

int main(void) {
    sqlite3 *db;
    const char *label = ENGINE_LABEL;

    if (sqlite3_open(":memory:", &db) != SQLITE_OK) {
        fprintf(stderr, "open failed\n"); return 1;
    }
    exec_or_die(db, "CREATE TABLE t (a INTEGER, b TEXT, c REAL)");
    exec_or_die(db, "INSERT INTO t VALUES (42, 'hello', 3.14)");
    exec_or_die(db, "INSERT INTO t VALUES (7, 'world', 2.72)");

    exec_or_die(db, "CREATE TABLE u (x INTEGER, y TEXT)");
    exec_or_die(db, "INSERT INTO u VALUES (42, 'foo')");
    exec_or_die(db, "INSERT INTO u VALUES (99, 'bar')");

    printf("\n======== ENGINE: %s — BUG CONFIRMATION ========\n", label);

    /* Test 1: single-table column values */
    run_show_typed(db, "1. Single table SELECT *", "SELECT * FROM t");
    run_show_typed(db, "2. Single table SELECT a, b, c", "SELECT a, b, c FROM t");
    run_show_typed(db, "3. Single table WHERE b='hello'", "SELECT a, b FROM t WHERE b = 'hello'");

    /* Test 2: multi-table column values */
    run_show_typed(db, "4. Two-table cross join (no WHERE)", "SELECT * FROM t, u LIMIT 3");
    run_show_typed(db, "5. Two-table JOIN on a=x", "SELECT a, b, x, y FROM t, u WHERE a = x");
    run_show_typed(db, "6. Two-table JOIN SELECT *", "SELECT * FROM t, u WHERE t.a = u.x");

    /* Test 3: unqualified column in SELECT list from joined tables */
    run_show_typed(db, "7. Only SELECT a from 2-table join", "SELECT a FROM t, u WHERE a = x");
    run_show_typed(db, "8. Only SELECT y from 2-table join", "SELECT y FROM t, u WHERE a = x");

    /* Test 4: does column resolution fail on WHERE with multi-table? */
    run_show_typed(db, "9. WHERE x=42 in 2-table join", "SELECT a, x FROM t, u WHERE x = 42");
    run_show_typed(db, "10. WHERE a=42 in 2-table join", "SELECT a, x FROM t, u WHERE a = 42");

    /* Test 5: COUNT(*) to confirm rows exist in join */
    run_show_typed(db, "11. COUNT(*) join", "SELECT COUNT(*) FROM t, u WHERE a = x");
    run_show_typed(db, "12. COUNT(*) single table", "SELECT COUNT(*) FROM t");

    sqlite3_close(db);
    return 0;
}
