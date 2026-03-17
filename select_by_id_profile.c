/* select_by_id_profile.c — focused SELECT BY ID profiler */
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sqlite3.h>

#define ROWS 10000

static double get_time_ms(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (double)tv.tv_sec * 1000.0 + (double)tv.tv_usec / 1000.0;
}

int main(void) {
    sqlite3 *db;
    sqlite3_open(":memory:", &db);
    
    sqlite3_exec(db, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, value REAL);",
                 NULL, NULL, NULL);
    
    /* Seed in txn */
    sqlite3_exec(db, "BEGIN;", NULL, NULL, NULL);
    for (int i = 0; i < ROWS; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO test (name, value) VALUES ('item_%d', %.2f);",
                 i, (double)i * 1.1);
        sqlite3_exec(db, sql, NULL, NULL, NULL);
    }
    sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL);
    
    fprintf(stderr, "Seed complete. Starting SELECT BY ID...\n");
    
    double start = get_time_ms();
    for (int i = 0; i < ROWS; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "SELECT * FROM test WHERE id = %d;", i + 1);
        sqlite3_exec(db, sql, NULL, NULL, NULL);
    }
    double end = get_time_ms();
    
    fprintf(stderr, "SELECT BY ID %d times: %.2f ms (%.1f us/op)\n",
            ROWS, end - start, (end - start) * 1000.0 / ROWS);
    
    sqlite3_close(db);
    return 0;
}
