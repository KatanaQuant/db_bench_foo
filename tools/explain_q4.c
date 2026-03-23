/* explain_q4.c — EXPLAIN QUERY PLAN for TPC-H Q4 on both engines
 *
 * Compile for SQLite:
 *   SQLITE3_C=/path/to/sqlite3.c
 *   gcc -O2 explain_q4.c $SQLITE3_C -I./include -o explain_q4_sqlite -lpthread -ldl -lm
 *
 * Compile for FrankenSQLite:
 *   FSQLITE=.../frankensqlite/target/release
 *   gcc -O2 explain_q4.c -I./include -L$FSQLITE -lfsqlite_c_api \
 *       -Wl,-rpath,$FSQLITE -o explain_q4_fsqlite
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "sqlite3.h"

static const char *SCHEMA =
    "CREATE TABLE orders ("
    "  o_orderkey INTEGER PRIMARY KEY,"
    "  o_custkey INTEGER NOT NULL,"
    "  o_orderstatus TEXT NOT NULL,"
    "  o_totalprice REAL NOT NULL,"
    "  o_orderdate TEXT NOT NULL,"
    "  o_orderpriority TEXT NOT NULL,"
    "  o_clerk TEXT NOT NULL,"
    "  o_shippriority INTEGER NOT NULL,"
    "  o_comment TEXT);"

    "CREATE TABLE lineitem ("
    "  l_orderkey INTEGER NOT NULL,"
    "  l_partkey INTEGER NOT NULL,"
    "  l_suppkey INTEGER NOT NULL,"
    "  l_linenumber INTEGER NOT NULL,"
    "  l_quantity REAL NOT NULL,"
    "  l_extendedprice REAL NOT NULL,"
    "  l_discount REAL NOT NULL,"
    "  l_tax REAL NOT NULL,"
    "  l_returnflag TEXT NOT NULL,"
    "  l_linestatus TEXT NOT NULL,"
    "  l_shipdate TEXT NOT NULL,"
    "  l_commitdate TEXT NOT NULL,"
    "  l_receiptdate TEXT NOT NULL,"
    "  l_shipinstruct TEXT NOT NULL,"
    "  l_shipmode TEXT NOT NULL,"
    "  l_comment TEXT,"
    "  PRIMARY KEY (l_orderkey, l_linenumber));";

/* TPC-H Q4 */
static const char *Q4 =
    "SELECT o_orderpriority, COUNT(*) AS order_count "
    "FROM orders "
    "WHERE o_orderdate >= '1993-07-01' AND o_orderdate < '1993-10-01' "
    "AND EXISTS ("
    "  SELECT * FROM lineitem WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate"
    ") "
    "GROUP BY o_orderpriority "
    "ORDER BY o_orderpriority";

typedef struct {
    const char *label;
    int row_num;
    int ncols_printed;
} cb_ctx;

/* Callback for sqlite3_exec — prints each row */
static int row_callback(void *arg, int ncols, char **vals, char **colnames) {
    cb_ctx *ctx = (cb_ctx *)arg;
    (void)colnames;

    if (ctx->row_num == 0) {
        /* Print header on first row */
        for (int i = 0; i < ncols; i++) {
            if (i == 0)      printf("%-6s", colnames[i]);
            else if (i == 1) printf(" %-22s", colnames[i]);
            else             printf(" %-8s", colnames[i]);
        }
        printf("\n");
        for (int i = 0; i < ncols; i++) {
            if (i == 0)      printf("%-6s", "------");
            else if (i == 1) printf(" %-22s", "----------------------");
            else             printf(" %-8s", "--------");
        }
        printf("\n");
        ctx->ncols_printed = ncols;
    }

    for (int i = 0; i < ncols; i++) {
        const char *v = vals[i] ? vals[i] : "";
        if (i == 0)      printf("%-6s", v);
        else if (i == 1) printf(" %-22s", v);
        else             printf(" %-8s", v);
    }
    printf("\n");
    ctx->row_num++;
    return 0;
}

static void run_explain_exec(sqlite3 *db, const char *label, const char *prefix) {
    char sql[4096];
    snprintf(sql, sizeof(sql), "%s %s", prefix, Q4);

    printf("[%s] %s\n", label, prefix);

    cb_ctx ctx = { label, 0, 0 };
    char *errmsg = NULL;
    int rc = sqlite3_exec(db, sql, row_callback, &ctx, &errmsg);
    if (rc != SQLITE_OK) {
        printf("  ERROR: %s\n", errmsg ? errmsg : sqlite3_errmsg(db));
        if (errmsg) sqlite3_free(errmsg);
    }
    if (ctx.row_num == 0) {
        printf("  (no rows returned)\n");
    }
    printf("\n");
}

/* prepare_v2 path — only works for SQLite (fsqlite rejects EXPLAIN in prepare) */
static void run_explain_prepare(sqlite3 *db, const char *label, const char *prefix) {
    char sql[4096];
    snprintf(sql, sizeof(sql), "%s %s", prefix, Q4);

    sqlite3_stmt *stmt;
    int rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        printf("[%s] %s — prepare() rejected: %s\n", label, prefix, sqlite3_errmsg(db));
        return;
    }

    printf("[%s] %s (via prepare_v2)\n", label, prefix);

    int ncols = sqlite3_column_count(stmt);
    int first = 1;
    int rows = 0;

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        if (first) {
            printf("%-6s %-22s %-8s %-8s %-8s\n",
                   "id", "detail_or_opcode", "col2", "col3", "col4");
            printf("%-6s %-22s %-8s %-8s %-8s\n",
                   "------", "----------------------", "--------", "--------", "--------");
            first = 0;
        }
        for (int i = 0; i < ncols; i++) {
            const char *v = (const char *)sqlite3_column_text(stmt, i);
            if (!v) v = "";
            if (i == 0)      printf("%-6s", v);
            else if (i == 1) printf(" %-22s", v);
            else             printf(" %-8s", v);
        }
        printf("\n");
        rows++;
    }
    sqlite3_finalize(stmt);
    if (rows == 0) printf("  (no rows)\n");
    printf("\n");
}

static void die(const char *msg, sqlite3 *db) {
    fprintf(stderr, "FATAL: %s — %s\n", msg, sqlite3_errmsg(db));
    exit(1);
}

int main(int argc, char **argv) {
    const char *label = (argc > 1) ? argv[1] : "engine";
    sqlite3 *db;

    if (sqlite3_open(":memory:", &db) != SQLITE_OK)
        die("open", db);

    char *errmsg = NULL;
    if (sqlite3_exec(db, SCHEMA, NULL, NULL, &errmsg) != SQLITE_OK) {
        fprintf(stderr, "Schema error: %s\n", errmsg);
        sqlite3_free(errmsg);
        exit(1);
    }

    printf("=== TPC-H Q4 EXPLAIN — %s ===\n\n", label);
    printf("Query:\n  %s\n\n", Q4);

    /* Try EXPLAIN QUERY PLAN via exec (works for both) */
    run_explain_exec(db, label, "EXPLAIN QUERY PLAN");

    /* Try EXPLAIN (bytecode) via exec */
    run_explain_exec(db, label, "EXPLAIN");

    /* Also try via prepare_v2 (SQLite allows it, fsqlite rejects it) */
    run_explain_prepare(db, label, "EXPLAIN QUERY PLAN");

    sqlite3_close(db);
    return 0;
}
