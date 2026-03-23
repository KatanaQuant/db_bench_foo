/* benchmark.c - SQLite performance baseline
 *
 * Measures INSERT, SELECT, UPDATE, DELETE, and TRANSACTION batch
 * operations against any library exposing the sqlite3 C API.
 *
 * Compile:
 *   gcc -O2 benchmark.c -lsqlite3 -o benchmark
 *
 * Run:
 *   ./benchmark [record_count] [database_file]
 *   ./benchmark 1000
 *   ./benchmark 1000 test.db
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <sqlite3.h>

/* Timing utilities */
static double get_time_ms(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (double)tv.tv_sec * 1000.0 + (double)tv.tv_usec / 1000.0;
}

static double benchmark_insert(sqlite3 *db, int count) {
    int rc;
    double start, end;

    /* Create table */
    rc = sqlite3_exec(db,
        "CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT, value REAL);",
        NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to create table: %s\n", sqlite3_errmsg(db));
        return -1.0;
    }

    /* Clear existing data */
    sqlite3_exec(db, "DELETE FROM test;", NULL, NULL, NULL);

    start = get_time_ms();

    for (int i = 0; i < count; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO test (name, value) VALUES ('item_%d', %.2f);",
                 i, (double)i * 1.5);

        rc = sqlite3_exec(db, sql, NULL, NULL, NULL);
        if (rc != SQLITE_OK) {
            fprintf(stderr, "Insert %d failed: %s\n", i, sqlite3_errmsg(db));
        }
    }

    end = get_time_ms();
    return end - start;
}

static double benchmark_select_all(sqlite3 *db, int expected_count) {
    sqlite3_stmt *stmt;
    int rc;
    double start, end;
    int row_count = 0;

    rc = sqlite3_prepare_v2(db, "SELECT * FROM test;", -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to prepare select: %s\n", sqlite3_errmsg(db));
        return -1.0;
    }

    start = get_time_ms();

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        row_count++;
        /* Access data to ensure it's actually read */
        int cols = sqlite3_column_count(stmt);
        for (int i = 0; i < cols; i++) {
            sqlite3_column_text(stmt, i);
        }
    }

    end = get_time_ms();
    sqlite3_finalize(stmt);

    if (row_count != expected_count) {
        fprintf(stderr, "Warning: Expected %d rows, got %d\n", expected_count, row_count);
    }

    return end - start;
}

static double benchmark_select_by_id(sqlite3 *db, int count) {
    int rc;
    double total_time = 0.0;

    for (int i = 1; i <= count; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql), "SELECT * FROM test WHERE id = %d;", i);

        sqlite3_stmt *stmt;
        rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
        if (rc != SQLITE_OK) continue;

        double start = get_time_ms();
        rc = sqlite3_step(stmt);
        double end = get_time_ms();

        total_time += (end - start);
        sqlite3_finalize(stmt);
    }

    return total_time;
}

static double benchmark_update(sqlite3 *db, int count) {
    int rc;
    double start, end;

    start = get_time_ms();

    for (int i = 1; i <= count; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "UPDATE test SET value = %.2f WHERE id = %d;",
                 (double)i * 2.0, i);

        rc = sqlite3_exec(db, sql, NULL, NULL, NULL);
        if (rc != SQLITE_OK) {
            fprintf(stderr, "Update %d failed: %s\n", i, sqlite3_errmsg(db));
        }
    }

    end = get_time_ms();
    return end - start;
}

static double benchmark_delete(sqlite3 *db, int count) {
    int rc;
    double start, end;

    /* Re-populate the table */
    sqlite3_exec(db, "DELETE FROM test;", NULL, NULL, NULL);

    for (int i = 0; i < count; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO test (name, value) VALUES ('item_%d', %.2f);",
                 i, (double)i * 1.5);
        sqlite3_exec(db, sql, NULL, NULL, NULL);
    }

    start = get_time_ms();

    for (int i = 1; i <= count; i++) {
        char sql[128];
        snprintf(sql, sizeof(sql), "DELETE FROM test WHERE id = %d;", i);

        rc = sqlite3_exec(db, sql, NULL, NULL, NULL);
        if (rc != SQLITE_OK) {
            fprintf(stderr, "Delete %d failed: %s\n", i, sqlite3_errmsg(db));
        }
    }

    end = get_time_ms();
    return end - start;
}

static double benchmark_transaction_batch(sqlite3 *db, int batch_size) {
    double start, end;

    sqlite3_exec(db, "DELETE FROM test;", NULL, NULL, NULL);

    start = get_time_ms();

    sqlite3_exec(db, "BEGIN TRANSACTION;", NULL, NULL, NULL);

    for (int i = 0; i < batch_size; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO test (name, value) VALUES ('batch_item_%d', %.2f);",
                 i, (double)i * 1.5);
        sqlite3_exec(db, sql, NULL, NULL, NULL);
    }

    sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL);

    end = get_time_ms();
    return end - start;
}

static int run_benchmarks(const char *db_name, int record_count) {
    sqlite3 *db;
    int rc;
    double t_insert, t_select_all, t_select_by_id, t_update, t_delete, t_transaction;

    printf("\n=== SQLite Benchmark ===\n");
    printf("Database: %s\n", db_name);
    printf("Record count: %d\n\n", record_count);

    /* Remove existing database file for clean test */
    remove(db_name);

    rc = sqlite3_open(db_name, &db);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(db));
        return 1;
    }

    /* WAL mode, synchronous=NORMAL for realistic workload */
    sqlite3_exec(db, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);
    sqlite3_exec(db, "PRAGMA synchronous=NORMAL;", NULL, NULL, NULL);

    printf("Running benchmarks...\n");

    t_insert = benchmark_insert(db, record_count);
    t_select_all = benchmark_select_all(db, record_count);
    t_select_by_id = benchmark_select_by_id(db, record_count);
    t_update = benchmark_update(db, record_count);
    t_delete = benchmark_delete(db, record_count);
    t_transaction = benchmark_transaction_batch(db, record_count);

    printf("\n--- Results ---\n");
    printf("INSERT %d records:        %.2f ms (%.1f rec/sec)\n",
           record_count, t_insert, (record_count / t_insert) * 1000.0);
    printf("SELECT ALL %d records:    %.2f ms\n", record_count, t_select_all);
    printf("SELECT BY ID %d times:    %.2f ms (%.1f queries/sec)\n",
           record_count, t_select_by_id, (record_count / t_select_by_id) * 1000.0);
    printf("UPDATE %d records:        %.2f ms (%.1f rec/sec)\n",
           record_count, t_update, (record_count / t_update) * 1000.0);
    printf("DELETE %d records:        %.2f ms (%.1f rec/sec)\n",
           record_count, t_delete, (record_count / t_delete) * 1000.0);
    printf("TRANSACTION batch %d:     %.2f ms (%.1f rec/sec)\n",
           record_count, t_transaction, (record_count / t_transaction) * 1000.0);

    double total = t_insert + t_select_all + t_select_by_id + t_update + t_delete + t_transaction;
    printf("\nTOTAL TIME: %.2f ms\n", total);

    /* Cleanup */
    sqlite3_exec(db, "DROP TABLE IF EXISTS test;", NULL, NULL, NULL);
    sqlite3_close(db);
    remove(db_name);

    return 0;
}

int main(int argc, char *argv[]) {
    int record_count = 1000;
    const char *db_name = "benchmark.db";

    if (argc > 1) {
        record_count = atoi(argv[1]);
    }
    if (argc > 2) {
        db_name = argv[2];
    }

    printf("SQLite Performance Baseline\n");
    printf("===========================\n");

    return run_benchmarks(db_name, record_count);
}
