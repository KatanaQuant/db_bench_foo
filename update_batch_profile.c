/* update_batch_profile.c — profile UPDATE inside BEGIN..COMMIT
 * Isolates per-operation overhead with commit noise removed.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <sqlite3.h>

#define ROWS 10000

int main(void) {
    sqlite3 *db;
    int i;

    sqlite3_open(":memory:", &db);

    sqlite3_exec(db, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, value REAL);",
                 NULL, NULL, NULL);

    /* Seed in txn */
    sqlite3_exec(db, "BEGIN;", NULL, NULL, NULL);
    for (i = 0; i < ROWS; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO test (name, value) VALUES ('item_%d', %.2f);",
                 i, (double)i * 1.1);
        sqlite3_exec(db, sql, NULL, NULL, NULL);
    }
    sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL);

    fprintf(stderr, "Seed complete. Starting UPDATE batch profile...\n");

    /* UPDATE 10K inside single transaction */
    sqlite3_exec(db, "BEGIN;", NULL, NULL, NULL);
    for (i = 0; i < ROWS; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "UPDATE test SET value = %.2f WHERE id = %d;",
                 (double)i * 2.2, i + 1);
        sqlite3_exec(db, sql, NULL, NULL, NULL);
    }
    sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL);

    fprintf(stderr, "Done.\n");
    sqlite3_close(db);
    return 0;
}
