/* update_profile.c — isolated :memory: UPDATE 10K for perf profiling
 *
 * 1. Creates :memory: DB with 10K rows
 * 2. Pauses (marker for perf)
 * 3. Runs 10K individual autocommit UPDATEs
 *
 * Usage: ./update_profile
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

int main(void) {
    sqlite3 *db;
    int rc;
    double start, elapsed;

    rc = sqlite3_open(":memory:", &db);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "open failed: %s\n", sqlite3_errmsg(db));
        return 1;
    }

    /* Schema */
    sqlite3_exec(db, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, value REAL);",
                 NULL, NULL, NULL);

    /* Seed 10K rows in a transaction (not profiled) */
    sqlite3_exec(db, "BEGIN;", NULL, NULL, NULL);
    for (int i = 0; i < ROWS; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO test (name, value) VALUES ('item_%d', %.2f);",
                 i, (double)i * 1.1);
        sqlite3_exec(db, sql, NULL, NULL, NULL);
    }
    sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL);

    fprintf(stderr, "Seed complete. Starting UPDATE profile...\n");

    /* --- UPDATE 10K (autocommit, individual statements) --- */
    start = get_time_ms();
    for (int i = 0; i < ROWS; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "UPDATE test SET value = %.2f WHERE id = %d;",
                 (double)i * 2.2, i + 1);
        sqlite3_exec(db, sql, NULL, NULL, NULL);
    }
    elapsed = get_time_ms() - start;

    fprintf(stderr, "UPDATE %d: %.2f ms (%.0f rec/s)\n",
            ROWS, elapsed, (double)ROWS / (elapsed / 1000.0));

    sqlite3_close(db);
    return 0;
}
