/* explain_update.c — EXPLAIN UPDATE for opcode comparison */
#include <stdio.h>
#include <sqlite3.h>

static int print_row(void *unused, int ncol, char **vals, char **cols) {
    (void)unused; (void)cols;
    for (int i = 0; i < ncol; i++) {
        printf("%-6s", vals[i] ? vals[i] : "");
    }
    printf("\n");
    return 0;
}

int main(void) {
    sqlite3 *db;
    sqlite3_open(":memory:", &db);
    sqlite3_exec(db, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, value REAL);", NULL, NULL, NULL);

    printf("=== EXPLAIN UPDATE test SET value = 1.1 WHERE id = 1 ===\n");
    sqlite3_exec(db,
        "EXPLAIN UPDATE test SET value = 1.1 WHERE id = 1;",
        print_row, NULL, NULL);

    sqlite3_close(db);
    return 0;
}
