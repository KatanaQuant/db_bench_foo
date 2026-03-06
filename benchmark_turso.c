/* benchmark_turso.c - Benchmark for Turso/libsql
 * 
 * Compile:
 *   gcc -O2 benchmark_turso.c -I./turso/bindings/c/include \
 *       -L./turso/target/release -lsql_experimental \
 *       -lpthread -ldl -lm -o bench_turso
 * 
 * Run:
 *   LD_LIBRARY_PATH=./turso/target/release ./bench_turso [record_count] [database_file]
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include "libsql.h"

/* Timing utilities */
static double get_time_ms(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (double)tv.tv_sec * 1000.0 + (double)tv.tv_usec / 1000.0;
}

static double benchmark_insert(libsql_connection_t conn, int count) {
    int retval;
    double start, end;
    const char *err = NULL;
    
    /* Create table */
    retval = libsql_execute(conn, 
        "CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT, value REAL);",
        &err);
    if (retval != 0) {
        fprintf(stderr, "Failed to create table: %s\n", err ? err : "unknown");
        return -1.0;
    }
    
    /* Clear existing data */
    libsql_execute(conn, "DELETE FROM test;", &err);
    err = NULL;
    
    start = get_time_ms();
    
    for (int i = 0; i < count; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql), 
                 "INSERT INTO test (name, value) VALUES ('item_%d', %.2f);",
                 i, (double)i * 1.5);
        
        retval = libsql_execute(conn, sql, &err);
        if (retval != 0) {
            fprintf(stderr, "Insert %d failed: %s\n", i, err ? err : "unknown");
        }
        err = NULL;
    }
    
    end = get_time_ms();
    return end - start;
}

static double benchmark_select_all(libsql_connection_t conn, int expected_count) {
    libsql_rows_t rows;
    int retval;
    double start, end;
    int row_count = 0;
    const char *err = NULL;
    
    retval = libsql_query(conn, "SELECT * FROM test;", &rows, &err);
    if (retval != 0) {
        fprintf(stderr, "Failed to select: %s\n", err ? err : "unknown");
        return -1.0;
    }
    
    start = get_time_ms();
    
    libsql_row_t row;
    while ((retval = libsql_next_row(rows, &row, &err)) == 0) {
        if (!err && !row) {
            break;
        }
        row_count++;
        /* Just touch the data */
        int col_count = libsql_column_count(rows);
        for (int i = 0; i < col_count; i++) {
            const char *val = NULL;
            const char *col_err = NULL;
            libsql_get_string(row, i, &val, &col_err);
            if (val) libsql_free_string(val);
        }
        libsql_free_row(row);
        err = NULL;
    }
    
    end = get_time_ms();
    libsql_free_rows(rows);
    
    if (row_count != expected_count) {
        fprintf(stderr, "Warning: Expected %d rows, got %d\n", expected_count, row_count);
    }
    
    return end - start;
}

static double benchmark_select_by_id(libsql_connection_t conn, int count) {
    double total_time = 0.0;
    
    for (int i = 1; i <= count; i++) {
        char sql[256];
        const char *err = NULL;
        snprintf(sql, sizeof(sql), "SELECT * FROM test WHERE id = %d;", i);
        
        double start = get_time_ms();
        libsql_rows_t rows;
        int retval = libsql_query(conn, sql, &rows, &err);
        if (retval == 0) {
            libsql_row_t row;
            const char *row_err = NULL;
            retval = libsql_next_row(rows, &row, &row_err);
            if (retval == 0 && row) {
                libsql_free_row(row);
            }
            libsql_free_rows(rows);
        }
        double end = get_time_ms();
        
        total_time += (end - start);
    }
    
    return total_time;
}

static double benchmark_update(libsql_connection_t conn, int count) {
    int retval;
    double start, end;
    const char *err = NULL;
    
    start = get_time_ms();
    
    for (int i = 1; i <= count; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql), 
                 "UPDATE test SET value = %.2f WHERE id = %d;",
                 (double)i * 2.0, i);
        
        retval = libsql_execute(conn, sql, &err);
        if (retval != 0) {
            fprintf(stderr, "Update %d failed: %s\n", i, err ? err : "unknown");
        }
        err = NULL;
    }
    
    end = get_time_ms();
    return end - start;
}

static double benchmark_delete(libsql_connection_t conn, int count) {
    int retval;
    double start, end;
    const char *err = NULL;
    
    /* First re-populate the table */
    libsql_execute(conn, "DELETE FROM test;", &err);
    err = NULL;
    
    for (int i = 0; i < count; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql), 
                 "INSERT INTO test (name, value) VALUES ('item_%d', %.2f);",
                 i, (double)i * 1.5);
        libsql_execute(conn, sql, &err);
        err = NULL;
    }
    
    start = get_time_ms();
    
    for (int i = 1; i <= count; i++) {
        char sql[128];
        snprintf(sql, sizeof(sql), "DELETE FROM test WHERE id = %d;", i);
        
        retval = libsql_execute(conn, sql, &err);
        if (retval != 0) {
            fprintf(stderr, "Delete %d failed: %s\n", i, err ? err : "unknown");
        }
        err = NULL;
    }
    
    end = get_time_ms();
    return end - start;
}

static double benchmark_transaction_batch(libsql_connection_t conn, int batch_size) {
    int retval;
    double start, end;
    const char *err = NULL;
    
    libsql_execute(conn, "DELETE FROM test;", &err);
    err = NULL;
    
    start = get_time_ms();
    
    libsql_execute(conn, "BEGIN TRANSACTION;", &err);
    err = NULL;
    
    for (int i = 0; i < batch_size; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql), 
                 "INSERT INTO test (name, value) VALUES ('batch_item_%d', %.2f);",
                 i, (double)i * 1.5);
        libsql_execute(conn, sql, &err);
        err = NULL;
    }
    
    libsql_execute(conn, "COMMIT;", &err);
    
    end = get_time_ms();
    return end - start;
}

static int run_benchmarks(const char *db_name, int record_count) {
    libsql_database_t db;
    libsql_connection_t conn;
    int retval;
    const char *err = NULL;
    double t_insert, t_select_all, t_select_by_id, t_update, t_delete, t_transaction;
    
    printf("\n=== Benchmarking Turso/libsql ===\n");
    printf("Database: %s\n", db_name);
    printf("Record count: %d\n\n", record_count);
    
    /* Remove existing database file for clean test */
    remove(db_name);
    
    /* Open database - use file mode for fair comparison */
    retval = libsql_open_ext(db_name, &db, &err);
    if (retval != 0) {
        fprintf(stderr, "Cannot open database: %s\n", err ? err : "unknown error");
        return 1;
    }
    
    /* Connect */
    retval = libsql_connect(db, &conn, &err);
    if (retval != 0) {
        fprintf(stderr, "Cannot connect: %s\n", err ? err : "unknown");
        libsql_close(db);
        return 1;
    }
    
    /* Enable WAL mode */
    libsql_execute(conn, "PRAGMA journal_mode=WAL;", &err);
    err = NULL;
    libsql_execute(conn, "PRAGMA synchronous=NORMAL;", &err);
    err = NULL;
    
    /* Run benchmarks */
    printf("Running benchmarks...\n");
    
    t_insert = benchmark_insert(conn, record_count);
    t_select_all = benchmark_select_all(conn, record_count);
    t_select_by_id = benchmark_select_by_id(conn, record_count);
    t_update = benchmark_update(conn, record_count);
    t_delete = benchmark_delete(conn, record_count);
    t_transaction = benchmark_transaction_batch(conn, record_count);
    
    /* Print results */
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
    err = NULL;
    libsql_execute(conn, "DROP TABLE IF EXISTS test;", &err);
    libsql_disconnect(conn);
    libsql_close(db);
    remove(db_name);
    
    return 0;
}

int main(int argc, char *argv[]) {
    int record_count = 100;
    const char *db_name = "benchmark_turso.db";
    
    if (argc > 1) {
        record_count = atoi(argv[1]);
    }
    if (argc > 2) {
        db_name = argv[2];
    }
    
    printf("Turso/libsql Benchmark\n");
    printf("========================\n");
    
    return run_benchmarks(db_name, record_count);
}
