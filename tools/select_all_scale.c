/* select_all_scale.c — SELECT ALL per-row cost at various scales */
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sqlite3.h>

static double get_time_ms(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (double)tv.tv_sec * 1000.0 + (double)tv.tv_usec / 1000.0;
}

static int count_cb(void *p, int n, char **v, char **c) {
    (void)n; (void)v; (void)c;
    (*(int*)p)++;
    return 0;
}

int main(int argc, char *argv[]) {
    int sizes[] = {1000, 5000, 10000, 50000};
    int nsizes = 4;
    sqlite3 *db;
    
    for (int s = 0; s < nsizes; s++) {
        int rows = sizes[s];
        sqlite3_open(":memory:", &db);
        sqlite3_exec(db, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, value REAL);",
                     NULL, NULL, NULL);
        sqlite3_exec(db, "BEGIN;", NULL, NULL, NULL);
        for (int i = 0; i < rows; i++) {
            char sql[256];
            snprintf(sql, sizeof(sql),
                     "INSERT INTO test (name, value) VALUES ('item_%d', %.2f);",
                     i, (double)i * 1.1);
            sqlite3_exec(db, sql, NULL, NULL, NULL);
        }
        sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL);
        
        /* Warm up */
        int dummy = 0;
        sqlite3_exec(db, "SELECT * FROM test;", count_cb, &dummy, NULL);
        
        /* Measure 5 runs */
        double best = 1e9;
        for (int r = 0; r < 5; r++) {
            int cnt = 0;
            double start = get_time_ms();
            sqlite3_exec(db, "SELECT * FROM test;", count_cb, &cnt, NULL);
            double elapsed = get_time_ms() - start;
            if (elapsed < best) best = elapsed;
        }
        
        fprintf(stderr, "rows=%5d  total=%.2fms  per_row=%.3fus\n",
                rows, best, best * 1000.0 / rows);
        
        sqlite3_close(db);
    }
    return 0;
}
