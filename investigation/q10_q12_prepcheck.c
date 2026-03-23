/*
 * Check what prepare returns for Q10/Q12 on FrankenSQLite
 */
#include <stdio.h>
#include <stdlib.h>
#include "sqlite3.h"

static void exec(sqlite3 *db, const char *sql) {
    char *err = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", err);
        sqlite3_free(err);
    }
}

static void check_prepare(sqlite3 *db, const char *label, const char *sql) {
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    printf("%-15s rc=%d stmt=%s errmsg=%s\n",
           label, rc, stmt ? "non-null" : "NULL",
           rc != SQLITE_OK ? sqlite3_errmsg(db) : "OK");
    if (stmt) sqlite3_finalize(stmt);
}

int main(void) {
    sqlite3 *db;
    sqlite3_open(":memory:", &db);
    exec(db, "CREATE TABLE t (a INTEGER, b TEXT)");
    exec(db, "INSERT INTO t VALUES (1, 'x')");
    exec(db, "CREATE TABLE u (x INTEGER, y TEXT)");
    exec(db, "INSERT INTO u VALUES (1, 'y')");

    printf("ENGINE: %s\n\n", ENGINE_LABEL);

    check_prepare(db, "single-table", "SELECT a, b FROM t WHERE a = 1");
    check_prepare(db, "2-table-comma", "SELECT a, x FROM t, u WHERE a = x");
    check_prepare(db, "2-table-cross", "SELECT t.a, u.x FROM t CROSS JOIN u WHERE t.a = u.x");
    check_prepare(db, "count-join", "SELECT COUNT(*) FROM t, u WHERE a = x");

    sqlite3_close(db);
    return 0;
}
