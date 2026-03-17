/* diagnostic.c — isolate file-backed write overhead
 *
 * Runs INSERT 10K and UPDATE 10K under three pragma configs:
 *   1. WAL + synchronous=NORMAL (default benchmark config)
 *   2. WAL + synchronous=OFF
 *   3. journal_mode=OFF (no WAL, no journal)
 *
 * Usage: ./diagnostic <db_path>
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

#define ROWS 10000

static void run_test(const char *db_path, const char *label,
                     const char *pragma1, const char *pragma2) {
    sqlite3 *db;
    int rc;
    double start, elapsed;

    /* Remove any leftover file */
    remove(db_path);
    {
        char wal[512], shm[512];
        snprintf(wal, sizeof(wal), "%s-wal", db_path);
        snprintf(shm, sizeof(shm), "%s-shm", db_path);
        remove(wal);
        remove(shm);
    }

    rc = sqlite3_open(db_path, &db);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "open failed: %s\n", sqlite3_errmsg(db));
        return;
    }

    if (pragma1) sqlite3_exec(db, pragma1, NULL, NULL, NULL);
    if (pragma2) sqlite3_exec(db, pragma2, NULL, NULL, NULL);

    /* Verify pragmas took effect */
    {
        sqlite3_stmt *s;
        sqlite3_prepare_v2(db, "PRAGMA journal_mode;", -1, &s, NULL);
        if (s && sqlite3_step(s) == SQLITE_ROW)
            printf("  [%s] journal_mode = %s", label, sqlite3_column_text(s, 0));
        sqlite3_finalize(s);

        sqlite3_prepare_v2(db, "PRAGMA synchronous;", -1, &s, NULL);
        if (s && sqlite3_step(s) == SQLITE_ROW)
            printf(", synchronous = %d\n", sqlite3_column_int(s, 0));
        sqlite3_finalize(s);
    }

    sqlite3_exec(db, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, value REAL);",
                 NULL, NULL, NULL);

    /* --- INSERT 10K --- */
    start = get_time_ms();
    for (int i = 0; i < ROWS; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO test (name, value) VALUES ('item_%d', %.2f);",
                 i, (double)i * 1.1);
        sqlite3_exec(db, sql, NULL, NULL, NULL);
    }
    elapsed = get_time_ms() - start;
    printf("  [%s] INSERT %d: %.2f ms (%.0f rec/s)\n",
           label, ROWS, elapsed, (double)ROWS / (elapsed / 1000.0));

    /* --- UPDATE 10K --- */
    start = get_time_ms();
    for (int i = 0; i < ROWS; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "UPDATE test SET value = %.2f WHERE id = %d;",
                 (double)i * 2.2, i + 1);
        sqlite3_exec(db, sql, NULL, NULL, NULL);
    }
    elapsed = get_time_ms() - start;
    printf("  [%s] UPDATE %d: %.2f ms (%.0f rec/s)\n",
           label, ROWS, elapsed, (double)ROWS / (elapsed / 1000.0));

    sqlite3_close(db);
    remove(db_path);
    {
        char wal[512], shm[512];
        snprintf(wal, sizeof(wal), "%s-wal", db_path);
        snprintf(shm, sizeof(shm), "%s-shm", db_path);
        remove(wal);
        remove(shm);
    }
}

int main(int argc, char **argv) {
    const char *db = argc > 1 ? argv[1] : "/tmp/diag_test.db";

    printf("=== DIAGNOSTIC: file-backed write overhead ===\n");
    printf("Database path: %s\n\n", db);

    run_test(db, "WAL+NORMAL",
             "PRAGMA journal_mode=WAL;",
             "PRAGMA synchronous=NORMAL;");

    printf("\n");
    run_test(db, "WAL+OFF   ",
             "PRAGMA journal_mode=WAL;",
             "PRAGMA synchronous=OFF;");

    printf("\n");
    run_test(db, "JRNL=OFF  ",
             "PRAGMA journal_mode=OFF;",
             "PRAGMA synchronous=OFF;");

    return 0;
}
