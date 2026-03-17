/*
 * YCSB Benchmark for SQLite-compatible databases
 * Implements workloads A-F using sqlite3_exec (no bind API needed).
 * Compile: gcc -O2 -o ycsb_bench ycsb_bench.c -I./include -lsqlite3
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <math.h>
#include "sqlite3.h"

/* ---------- config ---------- */
#ifndef RECORD_COUNT
#define RECORD_COUNT 1000
#endif
#ifndef OP_COUNT
#define OP_COUNT 5000
#endif
#define FIELD_COUNT  10
#define FIELD_LENGTH 100
#define KEY_PREFIX   "user"
#define TABLE_NAME   "usertable"

/* ---------- timing ---------- */
static double now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

/* ---------- helpers ---------- */
static void die(const char *msg, sqlite3 *db) {
    fprintf(stderr, "FATAL: %s — %s\n", msg, db ? sqlite3_errmsg(db) : "no db");
    if (db) sqlite3_close(db);
    exit(1);
}

static void exec(sqlite3 *db, const char *sql) {
    char *err = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n  %.120s\n", err, sql);
        sqlite3_free(err);
    }
}

static void random_string(char *buf, int len) {
    static const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    for (int i = 0; i < len; i++)
        buf[i] = charset[rand() % (sizeof(charset) - 1)];
    buf[len] = '\0';
}

/* ---------- FNV-64a hash (for scrambled zipfian) ---------- */
static unsigned long long fnv64(unsigned long long val) {
    unsigned long long h = 0xCBF29CE484222325ULL;
    for (int i = 0; i < 8; i++) {
        h ^= (val & 0xFF);
        h *= 1099511628211ULL;
        val >>= 8;
    }
    return h;
}

/* ---------- Zipfian generator ---------- */
typedef struct {
    int items;
    double theta;
    double zetan;
    double eta;
    double alpha;
    double half_pow_theta;
} zipfian_t;

static double zeta(int n, double theta) {
    double sum = 0.0;
    for (int i = 1; i <= n; i++)
        sum += 1.0 / pow((double)i, theta);
    return sum;
}

static void zipfian_init(zipfian_t *z, int items) {
    z->items = items;
    z->theta = 0.99;
    z->zetan = zeta(items, z->theta);
    z->alpha = 1.0 / (1.0 - z->theta);
    z->half_pow_theta = pow(0.5, z->theta);
    double zeta2 = zeta(2, z->theta);
    z->eta = (1.0 - pow(2.0 / items, 1.0 - z->theta)) / (1.0 - zeta2 / z->zetan);
}

static int zipfian_next(zipfian_t *z) {
    double u = (double)rand() / RAND_MAX;
    double uz = u * z->zetan;
    if (uz < 1.0) return 0;
    if (uz < 1.0 + z->half_pow_theta) return 1;
    return (int)(z->items * pow(z->eta * u - z->eta + 1.0, z->alpha));
}

static int scrambled_zipfian_next(zipfian_t *z) {
    int raw = zipfian_next(z);
    return (int)(fnv64((unsigned long long)raw) % (unsigned long long)z->items);
}

/* ---------- workload operations ---------- */

static int read_count = 0, update_count = 0, insert_count = 0, scan_count = 0, rmw_count = 0;
static int next_insert_key;

static void op_read(sqlite3 *db, int key) {
    char sql[256];
    snprintf(sql, sizeof(sql),
        "SELECT * FROM " TABLE_NAME " WHERE YCSB_KEY='%s%010d'", KEY_PREFIX, key);
    exec(db, sql);
    read_count++;
}

static void op_update(sqlite3 *db, int key) {
    char val[FIELD_LENGTH + 1];
    int field = rand() % FIELD_COUNT;
    random_string(val, FIELD_LENGTH);
    char sql[512];
    snprintf(sql, sizeof(sql),
        "UPDATE " TABLE_NAME " SET field%d='%s' WHERE YCSB_KEY='%s%010d'",
        field, val, KEY_PREFIX, key);
    exec(db, sql);
    update_count++;
}

static void op_insert(sqlite3 *db) {
    char fields[FIELD_COUNT][FIELD_LENGTH + 1];
    for (int f = 0; f < FIELD_COUNT; f++)
        random_string(fields[f], FIELD_LENGTH);

    char sql[2048];
    snprintf(sql, sizeof(sql),
        "INSERT INTO " TABLE_NAME " VALUES('%s%010d','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')",
        KEY_PREFIX, next_insert_key++,
        fields[0], fields[1], fields[2], fields[3], fields[4],
        fields[5], fields[6], fields[7], fields[8], fields[9]);
    exec(db, sql);
    insert_count++;
}

static void op_scan(sqlite3 *db, int start_key, int scan_len) {
    char sql[256];
    snprintf(sql, sizeof(sql),
        "SELECT * FROM " TABLE_NAME " WHERE YCSB_KEY >= '%s%010d' ORDER BY YCSB_KEY LIMIT %d",
        KEY_PREFIX, start_key, scan_len);
    exec(db, sql);
    scan_count++;
}

static void op_readmodifywrite(sqlite3 *db, int key) {
    exec(db, "BEGIN");
    op_read(db, key);
    read_count--; /* counted as rmw, not read */
    op_update(db, key);
    update_count--; /* counted as rmw, not update */
    exec(db, "COMMIT");
    rmw_count++;
}

/* ---------- workload runners ---------- */

typedef struct {
    const char *name;
    double read_prop;
    double update_prop;
    double insert_prop;
    double scan_prop;
    double rmw_prop;
    int use_latest; /* 0 = zipfian, 1 = latest distribution */
} workload_def;

static const workload_def workloads[] = {
    {"A (Update Heavy)",   0.50, 0.50, 0.00, 0.00, 0.00, 0},
    {"B (Read Mostly)",    0.95, 0.05, 0.00, 0.00, 0.00, 0},
    {"C (Read Only)",      1.00, 0.00, 0.00, 0.00, 0.00, 0},
    {"D (Read Latest)",    0.95, 0.00, 0.05, 0.00, 0.00, 1},
    {"E (Short Ranges)",   0.00, 0.00, 0.05, 0.95, 0.00, 0},
    {"F (Read-Mod-Write)", 0.50, 0.00, 0.00, 0.00, 0.50, 0},
};
#define N_WORKLOADS (sizeof(workloads) / sizeof(workloads[0]))

static void run_workload(sqlite3 *db, const workload_def *w, int record_count, int op_count) {
    zipfian_t z;
    zipfian_init(&z, record_count);

    read_count = update_count = insert_count = scan_count = rmw_count = 0;
    int current_max = record_count;
    double t0 = now_ms();

    for (int i = 0; i < op_count; i++) {
        double r = (double)rand() / RAND_MAX;
        int key;

        if (w->use_latest) {
            /* latest distribution: bias toward most recently inserted */
            key = current_max - 1 - abs(zipfian_next(&z)) % current_max;
            if (key < 0) key = 0;
        } else {
            key = scrambled_zipfian_next(&z);
        }

        if (r < w->read_prop) {
            op_read(db, key);
        } else if (r < w->read_prop + w->update_prop) {
            op_update(db, key);
        } else if (r < w->read_prop + w->update_prop + w->insert_prop) {
            op_insert(db);
            current_max++;
        } else if (r < w->read_prop + w->update_prop + w->insert_prop + w->scan_prop) {
            int scan_len = 1 + rand() % 100;
            op_scan(db, key, scan_len);
        } else {
            op_readmodifywrite(db, key);
        }

        /* timeout: 60s per workload */
        if (now_ms() - t0 > 60000) {
            double elapsed = now_ms() - t0;
            printf("  %-22s  TIMEOUT after %.0f ms (%d/%d ops) "
                   "[R:%d U:%d I:%d S:%d RMW:%d]\n",
                   w->name, elapsed, i + 1, op_count,
                   read_count, update_count, insert_count, scan_count, rmw_count);
            return;
        }
    }

    double elapsed = now_ms() - t0;
    double throughput = op_count / (elapsed / 1000.0);
    printf("  %-22s  %8.2f ms  %8.0f ops/s  [R:%d U:%d I:%d S:%d RMW:%d]\n",
           w->name, elapsed, throughput,
           read_count, update_count, insert_count, scan_count, rmw_count);
}

/* ---------- main ---------- */
int main(int argc, char **argv) {
    const char *dbpath = (argc > 1) ? argv[1] : ":memory:";
    const char *label  = (argc > 2) ? argv[2] : "SQLite";
    int record_count = RECORD_COUNT;
    int op_count = OP_COUNT;
    sqlite3 *db;
    double t0, t1;

    srand(42); /* deterministic */

    printf("=== YCSB Benchmark: %s ===\n", label);
    printf("Database: %s\n", dbpath);
    printf("Records: %d, Operations/workload: %d\n", record_count, op_count);
    printf("Fields: %d x %d bytes = ~%d bytes/record\n\n",
           FIELD_COUNT, FIELD_LENGTH, FIELD_COUNT * FIELD_LENGTH);

    if (sqlite3_open(dbpath, &db) != SQLITE_OK)
        die("open", db);

    exec(db, "PRAGMA journal_mode=WAL");
    exec(db, "PRAGMA synchronous=NORMAL");

    /* schema */
    exec(db, "CREATE TABLE IF NOT EXISTS " TABLE_NAME " ("
         "YCSB_KEY TEXT PRIMARY KEY,"
         "field0 TEXT, field1 TEXT, field2 TEXT, field3 TEXT, field4 TEXT,"
         "field5 TEXT, field6 TEXT, field7 TEXT, field8 TEXT, field9 TEXT)");

    /* load phase */
    printf("Loading %d records...\n", record_count);
    fflush(stdout);
    exec(db, "BEGIN");
    t0 = now_ms();
    for (int i = 0; i < record_count; i++) {
        char fields[FIELD_COUNT][FIELD_LENGTH + 1];
        for (int f = 0; f < FIELD_COUNT; f++)
            random_string(fields[f], FIELD_LENGTH);

        char sql[2048];
        snprintf(sql, sizeof(sql),
            "INSERT INTO " TABLE_NAME " VALUES('%s%010d','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')",
            KEY_PREFIX, i,
            fields[0], fields[1], fields[2], fields[3], fields[4],
            fields[5], fields[6], fields[7], fields[8], fields[9]);
        exec(db, sql);
    }
    exec(db, "COMMIT");
    t1 = now_ms();
    printf("  Load: %.2f ms (%.0f inserts/s)\n\n", t1 - t0,
           record_count / ((t1 - t0) / 1000.0));
    fflush(stdout);

    next_insert_key = record_count;

    /* run workloads */
    printf("--- YCSB Workloads ---\n");
    for (int w = 0; w < (int)N_WORKLOADS; w++) {
        fflush(stdout);

        /* reset DB to clean state for each workload (reload data) */
        if (w > 0) {
            /* for workloads that insert, we need to reset */
            exec(db, "DELETE FROM " TABLE_NAME " WHERE YCSB_KEY >= '" KEY_PREFIX "' || ''");
            exec(db, "BEGIN");
            srand(42);
            for (int i = 0; i < record_count; i++) {
                char fields[FIELD_COUNT][FIELD_LENGTH + 1];
                for (int f = 0; f < FIELD_COUNT; f++)
                    random_string(fields[f], FIELD_LENGTH);
                char sql[2048];
                snprintf(sql, sizeof(sql),
                    "INSERT INTO " TABLE_NAME " VALUES('%s%010d','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')",
                    KEY_PREFIX, i,
                    fields[0], fields[1], fields[2], fields[3], fields[4],
                    fields[5], fields[6], fields[7], fields[8], fields[9]);
                exec(db, sql);
            }
            exec(db, "COMMIT");
            next_insert_key = record_count;
            srand(42 + w); /* different seed per workload for variety */
        }

        run_workload(db, &workloads[w], record_count, op_count);
    }

    sqlite3_close(db);
    printf("\nDone.\n");
    return 0;
}
