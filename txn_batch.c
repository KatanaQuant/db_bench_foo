/* txn_batch.c — autocommit vs BEGIN..COMMIT for INSERT/UPDATE/DELETE
 *
 * Usage: ./txn_batch <db_path>
 *   db_path = ":memory:" or a file path
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <sqlite3.h>

static double get_time_ms(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (double)tv.tv_sec * 1000.0 + (double)tv.tv_usec / 1000.0;
}

#define ROWS 1000

int main(int argc, char **argv) {
    const char *db_path = argc > 1 ? argv[1] : ":memory:";
    sqlite3 *db;
    double start, elapsed;
    int i;

    sqlite3_open(db_path, &db);
    sqlite3_exec(db, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);
    sqlite3_exec(db, "PRAGMA synchronous=NORMAL;", NULL, NULL, NULL);

    sqlite3_exec(db, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, value REAL);",
                 NULL, NULL, NULL);

    /* === SEED (in txn, not timed for UPDATE/DELETE) === */
    sqlite3_exec(db, "BEGIN;", NULL, NULL, NULL);
    for (i = 0; i < ROWS; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO test (name, value) VALUES ('item_%d', %.2f);",
                 i, (double)i * 1.1);
        sqlite3_exec(db, sql, NULL, NULL, NULL);
    }
    sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL);

    printf("=== %s ===\n", db_path);

    /* === INSERT autocommit === */
    sqlite3_exec(db, "DELETE FROM test;", NULL, NULL, NULL);
    start = get_time_ms();
    for (i = 0; i < ROWS; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO test (name, value) VALUES ('item_%d', %.2f);",
                 i, (double)i * 1.1);
        sqlite3_exec(db, sql, NULL, NULL, NULL);
    }
    elapsed = get_time_ms() - start;
    printf("INSERT autocommit %d: %.2f ms (%.1f us/op)\n",
           ROWS, elapsed, elapsed * 1000.0 / ROWS);

    /* === INSERT batch === */
    sqlite3_exec(db, "DELETE FROM test;", NULL, NULL, NULL);
    start = get_time_ms();
    sqlite3_exec(db, "BEGIN;", NULL, NULL, NULL);
    for (i = 0; i < ROWS; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO test (name, value) VALUES ('item_%d', %.2f);",
                 i, (double)i * 1.1);
        sqlite3_exec(db, sql, NULL, NULL, NULL);
    }
    sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL);
    elapsed = get_time_ms() - start;
    printf("INSERT batch     %d: %.2f ms (%.1f us/op)\n",
           ROWS, elapsed, elapsed * 1000.0 / ROWS);

    /* Re-seed for UPDATE/DELETE tests */
    sqlite3_exec(db, "DELETE FROM test;", NULL, NULL, NULL);
    sqlite3_exec(db, "BEGIN;", NULL, NULL, NULL);
    for (i = 0; i < ROWS; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO test (name, value) VALUES ('item_%d', %.2f);",
                 i, (double)i * 1.1);
        sqlite3_exec(db, sql, NULL, NULL, NULL);
    }
    sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL);

    /* === UPDATE autocommit === */
    start = get_time_ms();
    for (i = 0; i < ROWS; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "UPDATE test SET value = %.2f WHERE id = %d;",
                 (double)i * 2.2, i + 1);
        sqlite3_exec(db, sql, NULL, NULL, NULL);
    }
    elapsed = get_time_ms() - start;
    printf("UPDATE autocommit %d: %.2f ms (%.1f us/op)\n",
           ROWS, elapsed, elapsed * 1000.0 / ROWS);

    /* === UPDATE batch === */
    start = get_time_ms();
    sqlite3_exec(db, "BEGIN;", NULL, NULL, NULL);
    for (i = 0; i < ROWS; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "UPDATE test SET value = %.2f WHERE id = %d;",
                 (double)i * 3.3, i + 1);
        sqlite3_exec(db, sql, NULL, NULL, NULL);
    }
    sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL);
    elapsed = get_time_ms() - start;
    printf("UPDATE batch     %d: %.2f ms (%.1f us/op)\n",
           ROWS, elapsed, elapsed * 1000.0 / ROWS);

    /* Re-seed for DELETE */
    sqlite3_exec(db, "DELETE FROM test;", NULL, NULL, NULL);
    sqlite3_exec(db, "BEGIN;", NULL, NULL, NULL);
    for (i = 0; i < ROWS; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO test (name, value) VALUES ('item_%d', %.2f);",
                 i, (double)i * 1.1);
        sqlite3_exec(db, sql, NULL, NULL, NULL);
    }
    sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL);

    /* === DELETE autocommit === */
    start = get_time_ms();
    for (i = 0; i < ROWS; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "DELETE FROM test WHERE id = %d;", i + 1);
        sqlite3_exec(db, sql, NULL, NULL, NULL);
    }
    elapsed = get_time_ms() - start;
    printf("DELETE autocommit %d: %.2f ms (%.1f us/op)\n",
           ROWS, elapsed, elapsed * 1000.0 / ROWS);

    /* Re-seed for DELETE batch */
    sqlite3_exec(db, "BEGIN;", NULL, NULL, NULL);
    for (i = 0; i < ROWS; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO test (name, value) VALUES ('item_%d', %.2f);",
                 i, (double)i * 1.1);
        sqlite3_exec(db, sql, NULL, NULL, NULL);
    }
    sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL);

    /* === DELETE batch === */
    start = get_time_ms();
    sqlite3_exec(db, "BEGIN;", NULL, NULL, NULL);
    for (i = 0; i < ROWS; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "DELETE FROM test WHERE id = %d;", i + 1);
        sqlite3_exec(db, sql, NULL, NULL, NULL);
    }
    sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL);
    elapsed = get_time_ms() - start;
    printf("DELETE batch     %d: %.2f ms (%.1f us/op)\n",
           ROWS, elapsed, elapsed * 1000.0 / ROWS);

    sqlite3_close(db);
    return 0;
}
