/*
 * YCSB-A Concurrent Benchmark for PostgreSQL.
 *
 * Same test as ycsb_concurrent.c but using libpq.
 * Measures concurrent read/update throughput at 1/2/4/8 threads.
 *
 * Compile:
 *   gcc -O2 ycsb_concurrent_pg.c -I/usr/include/postgresql -lpq -lpthread -lm -o ycsb_conc_pg
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <math.h>
#include <pthread.h>
#include <unistd.h>
#include <stdatomic.h>
#include <libpq-fe.h>

/* ---------- config ---------- */
#define RECORD_COUNT 1000
#define OPS_PER_THREAD 2000
#define FIELD_COUNT 10
#define FIELD_LENGTH 100
#define KEY_PREFIX "user"
#define TABLE_NAME "usertable"
#define MAX_THREADS 8
#define THREAD_COUNTS 4
static const int thread_counts[THREAD_COUNTS] = {1, 2, 4, 8};

/* ---------- timing ---------- */
static double now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

/* ---------- helpers ---------- */
static void random_string_r(char *buf, int len, unsigned int *seed) {
    static const char charset[] =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    for (int i = 0; i < len; i++)
        buf[i] = charset[rand_r(seed) % (sizeof(charset) - 1)];
    buf[len] = '\0';
}

/* ---------- Zipfian ---------- */
static unsigned long long fnv64(unsigned long long val) {
    unsigned long long h = 0xCBF29CE484222325ULL;
    for (int i = 0; i < 8; i++) { h ^= (val & 0xFF); h *= 1099511628211ULL; val >>= 8; }
    return h;
}

typedef struct {
    int items; double theta, zetan, eta, alpha, half_pow_theta;
} zipfian_t;

static double zeta(int n, double theta) {
    double sum = 0.0;
    for (int i = 1; i <= n; i++) sum += 1.0 / pow((double)i, theta);
    return sum;
}

static void zipfian_init(zipfian_t *z, int items) {
    z->items = items; z->theta = 0.99;
    z->zetan = zeta(items, z->theta);
    z->alpha = 1.0 / (1.0 - z->theta);
    z->half_pow_theta = pow(0.5, z->theta);
    double zeta2 = zeta(2, z->theta);
    z->eta = (1.0 - pow(2.0 / items, 1.0 - z->theta)) / (1.0 - zeta2 / z->zetan);
}

static int zipfian_next_r(zipfian_t *z, unsigned int *seed) {
    double u = (double)rand_r(seed) / RAND_MAX;
    double uz = u * z->zetan;
    if (uz < 1.0) return 0;
    if (uz < 1.0 + z->half_pow_theta) return 1;
    return (int)(z->items * pow(z->eta * u - z->eta + 1.0, z->alpha));
}

static int scrambled_zipfian_next_r(zipfian_t *z, unsigned int *seed) {
    int raw = zipfian_next_r(z, seed);
    return (int)(fnv64((unsigned long long)raw) % (unsigned long long)z->items);
}

/* ---------- thread context ---------- */
typedef struct {
    int thread_id;
    const char *conninfo;
    int use_serializable;
    int ops;
    int record_count;
    double elapsed_ms;
    long completed_ops;
    long read_ops;
    long update_ops;
    long retries;
    long aborts;
} thread_ctx_t;

/* ---------- worker ---------- */
static void *worker(void *arg) {
    thread_ctx_t *ctx = (thread_ctx_t *)arg;
    unsigned int seed = (unsigned int)(42 + ctx->thread_id * 7);

    PGconn *conn = PQconnectdb(ctx->conninfo);
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Thread %d: connection failed\n", ctx->thread_id);
        return NULL;
    }

    zipfian_t z;
    zipfian_init(&z, ctx->record_count);

    ctx->completed_ops = 0;
    ctx->read_ops = 0;
    ctx->update_ops = 0;
    ctx->retries = 0;
    ctx->aborts = 0;

    double t0 = now_ms();

    for (int i = 0; i < ctx->ops; i++) {
        int key = scrambled_zipfian_next_r(&z, &seed);
        double r = (double)rand_r(&seed) / RAND_MAX;
        int is_update = (r >= 0.50);
        int done = 0;

        for (int attempt = 0; attempt < 10 && !done; attempt++) {
            PGresult *res;

            if (ctx->use_serializable) {
                res = PQexec(conn, "BEGIN ISOLATION LEVEL SERIALIZABLE");
            } else {
                res = PQexec(conn, "BEGIN");
            }
            PQclear(res);

            char sql[512];
            if (is_update) {
                char val[FIELD_LENGTH + 1];
                int field = rand_r(&seed) % FIELD_COUNT;
                random_string_r(val, FIELD_LENGTH, &seed);
                snprintf(sql, sizeof(sql),
                    "UPDATE " TABLE_NAME " SET field%d='%s' WHERE ycsb_key='%s%010d'",
                    field, val, KEY_PREFIX, key);
            } else {
                snprintf(sql, sizeof(sql),
                    "SELECT * FROM " TABLE_NAME " WHERE ycsb_key='%s%010d'",
                    KEY_PREFIX, key);
            }

            res = PQexec(conn, sql);
            const char *sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
            if (sqlstate && strcmp(sqlstate, "40001") == 0) {
                PQclear(res);
                res = PQexec(conn, "ROLLBACK");
                PQclear(res);
                ctx->aborts++;
                usleep(100 + (rand_r(&seed) % 500));
                continue;
            }
            PQclear(res);

            res = PQexec(conn, "COMMIT");
            sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
            if (sqlstate && strcmp(sqlstate, "40001") == 0) {
                PQclear(res);
                res = PQexec(conn, "ROLLBACK");
                PQclear(res);
                ctx->aborts++;
                usleep(100 + (rand_r(&seed) % 500));
                continue;
            }
            PQclear(res);
            done = 1;
        }

        ctx->completed_ops++;
        if (is_update) ctx->update_ops++;
        else ctx->read_ops++;

        if (now_ms() - t0 > 120000.0) break;
    }

    ctx->elapsed_ms = now_ms() - t0;
    PQfinish(conn);
    return NULL;
}

/* ---------- load ---------- */
static void load_data(const char *conninfo, int record_count) {
    unsigned int seed = 42;
    PGconn *conn = PQconnectdb(conninfo);
    PGresult *res;

    res = PQexec(conn, "DROP TABLE IF EXISTS " TABLE_NAME);
    PQclear(res);

    res = PQexec(conn, "CREATE TABLE " TABLE_NAME " ("
        "ycsb_key TEXT PRIMARY KEY,"
        "field0 TEXT, field1 TEXT, field2 TEXT, field3 TEXT, field4 TEXT,"
        "field5 TEXT, field6 TEXT, field7 TEXT, field8 TEXT, field9 TEXT)");
    PQclear(res);

    res = PQexec(conn, "BEGIN");
    PQclear(res);

    for (int i = 0; i < record_count; i++) {
        char fields[FIELD_COUNT][FIELD_LENGTH + 1];
        for (int f = 0; f < FIELD_COUNT; f++)
            random_string_r(fields[f], FIELD_LENGTH, &seed);

        char sql[2048];
        snprintf(sql, sizeof(sql),
            "INSERT INTO " TABLE_NAME " VALUES('%s%010d','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')",
            KEY_PREFIX, i,
            fields[0], fields[1], fields[2], fields[3], fields[4],
            fields[5], fields[6], fields[7], fields[8], fields[9]);
        res = PQexec(conn, sql);
        PQclear(res);
    }

    res = PQexec(conn, "COMMIT");
    PQclear(res);
    PQfinish(conn);
}

/* ---------- run level ---------- */
static void run_level(const char *conninfo, int n_threads, int use_serializable, int record_count) {
    load_data(conninfo, record_count);

    pthread_t threads[MAX_THREADS];
    thread_ctx_t ctxs[MAX_THREADS];

    for (int i = 0; i < n_threads; i++) {
        ctxs[i].thread_id = i;
        ctxs[i].conninfo = conninfo;
        ctxs[i].use_serializable = use_serializable;
        ctxs[i].ops = OPS_PER_THREAD;
        ctxs[i].record_count = record_count;
    }

    double wall_start = now_ms();
    for (int i = 0; i < n_threads; i++)
        pthread_create(&threads[i], NULL, worker, &ctxs[i]);
    for (int i = 0; i < n_threads; i++)
        pthread_join(threads[i], NULL);
    double wall_ms = now_ms() - wall_start;

    long total_ops = 0, total_reads = 0, total_updates = 0;
    long total_retries = 0, total_aborts = 0;
    for (int i = 0; i < n_threads; i++) {
        total_ops += ctxs[i].completed_ops;
        total_reads += ctxs[i].read_ops;
        total_updates += ctxs[i].update_ops;
        total_retries += ctxs[i].retries;
        total_aborts += ctxs[i].aborts;
    }

    double throughput = total_ops / (wall_ms / 1000.0);
    printf("  %2d threads  %8.1f ms  %8.0f ops/s  "
           "[ops:%ld R:%ld U:%ld aborts:%ld]\n",
           n_threads, wall_ms, throughput,
           total_ops, total_reads, total_updates, total_aborts);
}

/* ---------- main ---------- */
int main(int argc, char **argv) {
    const char *conninfo = (argc > 1) ? argv[1] : "dbname=elle_test";
    const char *mode = (argc > 2) ? argv[2] : "serializable";
    int use_serializable = (strcmp(mode, "serializable") == 0);

    printf("=== YCSB-A Concurrent Benchmark (PostgreSQL) ===\n");
    printf("Connection: %s\n", conninfo);
    printf("Isolation: %s\n", use_serializable ? "SERIALIZABLE" : "READ COMMITTED");
    printf("Records: %d, Ops/thread: %d\n", RECORD_COUNT, OPS_PER_THREAD);
    printf("Workload: A (50%% read, 50%% update, Zipfian)\n\n");

    printf("--- Scaling ---\n");
    for (int i = 0; i < THREAD_COUNTS; i++) {
        run_level(conninfo, thread_counts[i], use_serializable, RECORD_COUNT);
    }

    printf("\nDone.\n");
    return 0;
}
