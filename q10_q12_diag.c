/*
 * Diagnostic: Run Q10 and Q12 against both SQLite and FrankenSQLite
 * Prints row counts and EXPLAIN opcode output for each query on each engine.
 *
 * Build SQLite version:
 *   gcc -O2 -o q10_q12_diag_sqlite q10_q12_diag.c -I./include -lsqlite3
 *
 * Build FrankenSQLite version:
 *   gcc -O2 -o q10_q12_diag_fsqlite q10_q12_diag.c -I./include \
 *   FSQL=frankensqlite/target/release
 *     -L$FSQL -lfsqlite_c_api -Wl,-rpath,$FSQL
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "sqlite3.h"

/* ---- schema (same as tpch_bench.c) ---- */
static const char *SCHEMA =
    "CREATE TABLE region ("
    "  r_regionkey INTEGER PRIMARY KEY,"
    "  r_name TEXT NOT NULL,"
    "  r_comment TEXT);"

    "CREATE TABLE nation ("
    "  n_nationkey INTEGER PRIMARY KEY,"
    "  n_name TEXT NOT NULL,"
    "  n_regionkey INTEGER NOT NULL,"
    "  n_comment TEXT);"

    "CREATE TABLE supplier ("
    "  s_suppkey INTEGER PRIMARY KEY,"
    "  s_name TEXT NOT NULL,"
    "  s_address TEXT NOT NULL,"
    "  s_nationkey INTEGER NOT NULL,"
    "  s_phone TEXT NOT NULL,"
    "  s_acctbal REAL NOT NULL,"
    "  s_comment TEXT);"

    "CREATE TABLE customer ("
    "  c_custkey INTEGER PRIMARY KEY,"
    "  c_name TEXT NOT NULL,"
    "  c_address TEXT NOT NULL,"
    "  c_nationkey INTEGER NOT NULL,"
    "  c_phone TEXT NOT NULL,"
    "  c_acctbal REAL NOT NULL,"
    "  c_mktsegment TEXT NOT NULL,"
    "  c_comment TEXT);"

    "CREATE TABLE part ("
    "  p_partkey INTEGER PRIMARY KEY,"
    "  p_name TEXT NOT NULL,"
    "  p_mfgr TEXT NOT NULL,"
    "  p_brand TEXT NOT NULL,"
    "  p_type TEXT NOT NULL,"
    "  p_size INTEGER NOT NULL,"
    "  p_container TEXT NOT NULL,"
    "  p_retailprice REAL NOT NULL,"
    "  p_comment TEXT);"

    "CREATE TABLE partsupp ("
    "  ps_partkey INTEGER NOT NULL,"
    "  ps_suppkey INTEGER NOT NULL,"
    "  ps_availqty INTEGER NOT NULL,"
    "  ps_supplycost REAL NOT NULL,"
    "  ps_comment TEXT,"
    "  PRIMARY KEY (ps_partkey, ps_suppkey));"

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
    "  PRIMARY KEY (l_orderkey, l_linenumber));"
;

static void exec_or_die(sqlite3 *db, const char *sql) {
    char *err = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n  SQL: %s\n", err, sql);
        sqlite3_free(err);
    }
}

static void generate_data(sqlite3 *db) {
    char buf[2048];
    int i, j;
    static const char *regions[] = {"AFRICA","AMERICA","ASIA","EUROPE","MIDDLE EAST"};
    static const char *nations[] = {
        "ALGERIA","ARGENTINA","BRAZIL","CANADA","EGYPT","ETHIOPIA","FRANCE",
        "GERMANY","INDIA","INDONESIA","IRAN","IRAQ","JAPAN","JORDAN","KENYA",
        "MOROCCO","MOZAMBIQUE","PERU","CHINA","ROMANIA","SAUDI ARABIA",
        "VIETNAM","RUSSIA","UNITED KINGDOM","UNITED STATES"
    };
    static const int nation_region[] = {
        0,1,1,1,4,0,3,3,2,2,4,4,2,4,0,0,0,1,2,3,4,2,3,3,1
    };
    static const char *segments[] = {"AUTOMOBILE","BUILDING","FURNITURE","HOUSEHOLD","MACHINERY"};
    static const char *priorities[] = {"1-URGENT","2-HIGH","3-MEDIUM","4-NOT SPECIFIED","5-LOW"};
    static const char *shipmodes[] = {"REG AIR","AIR","RAIL","SHIP","TRUCK","MAIL","FOB"};
    static const char *instructions[] = {"DELIVER IN PERSON","COLLECT COD","NONE","TAKE BACK RETURN"};
    static const char *brands[] = {"Brand#11","Brand#12","Brand#13","Brand#21","Brand#22","Brand#23","Brand#31","Brand#32","Brand#33"};
    static const char *types[] = {"STANDARD ANODIZED TIN","SMALL PLATED COPPER","MEDIUM BURNISHED STEEL","LARGE POLISHED BRASS","ECONOMY BRUSHED NICKEL"};
    static const char *containers[] = {"SM CASE","SM BOX","SM PACK","SM PKG","MED BAG","MED BOX","MED PKG","LG CASE","LG BOX","LG DRUM","WRAP CASE","WRAP BOX"};

    int N_NATIONS   = 25;
    int N_SUPPLIERS = 10;
    int N_CUSTOMERS = 50;
    int N_PARTS     = 20;
    int N_ORDERS    = 150;

    exec_or_die(db, "BEGIN");
    for (i = 0; i < 5; i++) {
        snprintf(buf, sizeof(buf),
            "INSERT INTO region VALUES(%d,'%s','comment %d')", i, regions[i], i);
        exec_or_die(db, buf);
    }
    for (i = 0; i < N_NATIONS; i++) {
        snprintf(buf, sizeof(buf),
            "INSERT INTO nation VALUES(%d,'%s',%d,'comment %d')",
            i, nations[i], nation_region[i], i);
        exec_or_die(db, buf);
    }
    for (i = 1; i <= N_SUPPLIERS; i++) {
        snprintf(buf, sizeof(buf),
            "INSERT INTO supplier VALUES(%d,'Supplier#%04d','Addr %d',%d,'%02d-%03d-%03d-%04d',%.2f,'comment %d')",
            i, i, i, i % N_NATIONS, 10 + (i % 15), i % 1000, (i*7) % 1000, i % 10000,
            (double)(i * 73 % 10000) - 1000.0, i);
        exec_or_die(db, buf);
    }
    for (i = 1; i <= N_CUSTOMERS; i++) {
        snprintf(buf, sizeof(buf),
            "INSERT INTO customer VALUES(%d,'Customer#%06d','Addr %d',%d,'%02d-%03d-%03d-%04d',%.2f,'%s','comment %d')",
            i, i, i, i % N_NATIONS, 10 + (i % 15), i % 1000, (i*3) % 1000, i % 10000,
            (double)(i * 47 % 10000) - 500.0, segments[i % 5], i);
        exec_or_die(db, buf);
    }
    for (i = 1; i <= N_PARTS; i++) {
        snprintf(buf, sizeof(buf),
            "INSERT INTO part VALUES(%d,'Part %d','Manufacturer#%d','%s','%s',%d,'%s',%.2f,'comment %d')",
            i, i, 1 + (i % 5), brands[i % 9], types[i % 5], 1 + (i % 50),
            containers[i % 12], 900.0 + (i % 200), i);
        exec_or_die(db, buf);
    }
    for (i = 1; i <= N_PARTS; i++) {
        for (j = 0; j < 4; j++) {
            int sk = 1 + ((i * 4 + j) % N_SUPPLIERS);
            snprintf(buf, sizeof(buf),
                "INSERT OR IGNORE INTO partsupp VALUES(%d,%d,%d,%.2f,'comment %d-%d')",
                i, sk, 100 + (i * 7 + j * 13) % 9999,
                1.0 + (double)((i * 31 + j * 17) % 100000) / 100.0, i, j);
            exec_or_die(db, buf);
        }
    }
    for (i = 1; i <= N_ORDERS; i++) {
        const char *status = (i % 3 == 0) ? "F" : (i % 3 == 1) ? "O" : "P";
        int year = 1993 + (i % 5);
        int month = 1 + (i % 12);
        int day = 1 + (i % 28);
        snprintf(buf, sizeof(buf),
            "INSERT INTO orders VALUES(%d,%d,'%s',%.2f,'%04d-%02d-%02d','%s','Clerk#%05d',%d,'comment %d')",
            i, 1 + (i % N_CUSTOMERS), status,
            (double)(i * 127 % 50000) + 1000.0,
            year, month, day,
            priorities[i % 5], 1 + (i % 1000), i % 8, i);
        exec_or_die(db, buf);
    }
    for (i = 1; i <= N_ORDERS; i++) {
        int nitems = 1 + (i % 4);
        for (j = 1; j <= nitems; j++) {
            int pk = 1 + ((i * 3 + j * 7) % N_PARTS);
            int sk = 1 + ((pk + j * 25) % N_SUPPLIERS);
            int year = 1993 + (i % 5);
            int smonth = 1 + ((i + j) % 12);
            int sday = 1 + ((i + j * 3) % 28);
            int cmonth = 1 + ((i + j + 1) % 12);
            int cday = 1 + ((i + j * 3 + 7) % 28);
            int rmonth = 1 + ((i + j + 2) % 12);
            int rday = 1 + ((i + j * 3 + 14) % 28);
            double qty = 1.0 + (i * j) % 50;
            double price = 900.0 + (pk % 200);
            double disc = ((i + j) % 11) / 100.0;
            double tax = ((i * j) % 9) / 100.0;
            const char *rf = (year < 1995) ? "R" : (year == 1995 && smonth <= 6) ? "R" : "N";
            const char *ls = (year < 1995) ? "F" : "O";
            snprintf(buf, sizeof(buf),
                "INSERT INTO lineitem VALUES(%d,%d,%d,%d,%.1f,%.2f,%.2f,%.2f,"
                "'%s','%s','%04d-%02d-%02d','%04d-%02d-%02d','%04d-%02d-%02d',"
                "'%s','%s','comment %d-%d')",
                i, pk, sk, j, qty, price * qty, disc, tax,
                rf, ls,
                year, smonth, sday,
                year, cmonth, cday,
                year, rmonth, rday,
                instructions[j % 4], shipmodes[(i + j) % 7], i, j);
            exec_or_die(db, buf);
        }
    }
    exec_or_die(db, "COMMIT");
}

/* ---- print EXPLAIN output ---- */
static void run_explain(sqlite3 *db, const char *label, const char *query_name, const char *sql) {
    char explain_sql[8192];
    sqlite3_stmt *stmt;
    int rc;

    printf("\n--- EXPLAIN %s [%s] ---\n", query_name, label);
    snprintf(explain_sql, sizeof(explain_sql), "EXPLAIN %s", sql);

    rc = sqlite3_prepare_v2(db, explain_sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        printf("  EXPLAIN prepare error: %s\n", sqlite3_errmsg(db));
        return;
    }
    printf("  addr  opcode             p1      p2      p3      p4\n");
    printf("  ----  -----------------  ------  ------  ------  -------------------------\n");
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        int addr  = sqlite3_column_int(stmt, 0);
        const char *op = (const char *)sqlite3_column_text(stmt, 1);
        int p1    = sqlite3_column_int(stmt, 2);
        int p2    = sqlite3_column_int(stmt, 3);
        int p3    = sqlite3_column_int(stmt, 4);
        const char *p4 = (const char *)sqlite3_column_text(stmt, 5);
        printf("  %4d  %-17s  %6d  %6d  %6d  %s\n",
               addr, op ? op : "", p1, p2, p3, p4 ? p4 : "");
    }
    sqlite3_finalize(stmt);
}

/* ---- print EXPLAIN QUERY PLAN output ---- */
static void run_eqp(sqlite3 *db, const char *label, const char *query_name, const char *sql) {
    char eqp_sql[8192];
    sqlite3_stmt *stmt;
    int rc;

    printf("\n--- EXPLAIN QUERY PLAN %s [%s] ---\n", query_name, label);
    snprintf(eqp_sql, sizeof(eqp_sql), "EXPLAIN QUERY PLAN %s", sql);

    rc = sqlite3_prepare_v2(db, eqp_sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        printf("  EQP prepare error: %s\n", sqlite3_errmsg(db));
        return;
    }
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        int ncols = sqlite3_column_count(stmt);
        int k;
        for (k = 0; k < ncols; k++) {
            const char *val = (const char *)sqlite3_column_text(stmt, k);
            printf("  col[%d]=%s", k, val ? val : "NULL");
        }
        printf("\n");
    }
    sqlite3_finalize(stmt);
}

/* ---- run query and dump first 10 rows ---- */
static void run_query_rows(sqlite3 *db, const char *label, const char *query_name, const char *sql) {
    sqlite3_stmt *stmt;
    int rc, rows = 0;

    printf("\n--- ROWS %s [%s] ---\n", query_name, label);
    rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        printf("  Prepare error: %s\n", sqlite3_errmsg(db));
        return;
    }
    int ncols = sqlite3_column_count(stmt);
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        int k;
        rows++;
        printf("  row %d:", rows);
        for (k = 0; k < ncols && k < 5; k++) {
            const char *val = (const char *)sqlite3_column_text(stmt, k);
            printf("  [%s]", val ? val : "NULL");
        }
        printf("\n");
        if (rows >= 10) { printf("  ...(truncated)\n"); break; }
    }
    sqlite3_finalize(stmt);
    printf("  Total rows: %d\n", rows);
}

/* ---- probe: check date ranges in the data ---- */
static void probe_data(sqlite3 *db, const char *label) {
    sqlite3_stmt *stmt;
    int rc;

    printf("\n=== DATA PROBE [%s] ===\n", label);

    /* Q10 date window: o_orderdate >= '1993-10-01' AND o_orderdate < '1994-01-01' */
    const char *q10_orders =
        "SELECT COUNT(*) FROM orders "
        "WHERE o_orderdate >= '1993-10-01' AND o_orderdate < '1994-01-01'";

    /* rows with both date match AND l_returnflag='R' */
    const char *q10_full =
        "SELECT COUNT(*) FROM orders o JOIN lineitem l ON l_orderkey = o_orderkey "
        "WHERE o_orderdate >= '1993-10-01' AND o_orderdate < '1994-01-01' "
        "AND l_returnflag = 'R'";

    /* Q12 window: l_receiptdate >= '1994-01-01' AND l_receiptdate < '1995-01-01' */
    /* AND l_shipmode IN ('MAIL','SHIP') */
    /* AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate */
    const char *q12_lineitem =
        "SELECT COUNT(*) FROM lineitem "
        "WHERE l_receiptdate >= '1994-01-01' AND l_receiptdate < '1995-01-01' "
        "AND l_shipmode IN ('MAIL','SHIP')";

    const char *q12_dates =
        "SELECT COUNT(*) FROM lineitem "
        "WHERE l_receiptdate >= '1994-01-01' AND l_receiptdate < '1995-01-01' "
        "AND l_shipmode IN ('MAIL','SHIP') "
        "AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate";

    /* What shipmodes do we actually have? */
    const char *shipmode_dist =
        "SELECT l_shipmode, COUNT(*) FROM lineitem GROUP BY l_shipmode ORDER BY l_shipmode";

    /* What returnflag values do we have? */
    const char *rf_dist =
        "SELECT l_returnflag, COUNT(*) FROM lineitem GROUP BY l_returnflag";

    /* Sample of date ranges */
    const char *date_range =
        "SELECT MIN(o_orderdate), MAX(o_orderdate) FROM orders";

    const char *receipt_range =
        "SELECT MIN(l_receiptdate), MAX(l_receiptdate), "
        "MIN(l_shipdate), MAX(l_shipdate), "
        "MIN(l_commitdate), MAX(l_commitdate) FROM lineitem";

    struct { const char *name; const char *sql; } probes[] = {
        {"orders in Q10 date window",        q10_orders},
        {"orders+lineitem: Q10 date+returnR", q10_full},
        {"lineitem in Q12 date+shipmode",     q12_lineitem},
        {"lineitem Q12 all conditions",       q12_dates},
        {"shipmode distribution",             shipmode_dist},
        {"returnflag distribution",           rf_dist},
        {"orders date range",                 date_range},
        {"lineitem date ranges",              receipt_range},
        {NULL, NULL}
    };

    for (int i = 0; probes[i].name; i++) {
        rc = sqlite3_prepare_v2(db, probes[i].sql, -1, &stmt, NULL);
        if (rc != SQLITE_OK) {
            printf("  [%s] prepare error: %s\n", probes[i].name, sqlite3_errmsg(db));
            continue;
        }
        printf("  %s:\n", probes[i].name);
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            int ncols = sqlite3_column_count(stmt);
            printf("    ");
            for (int k = 0; k < ncols; k++) {
                const char *v = (const char *)sqlite3_column_text(stmt, k);
                printf("%s%s", k ? " | " : "", v ? v : "NULL");
            }
            printf("\n");
        }
        sqlite3_finalize(stmt);
    }
}

static const char *Q10 =
    "SELECT c_custkey, c_name, "
    "SUM(l_extendedprice * (1 - l_discount)) AS revenue, "
    "c_acctbal, n_name, c_address, c_phone, c_comment "
    "FROM customer, orders, lineitem, nation "
    "WHERE c_custkey = o_custkey "
    "AND l_orderkey = o_orderkey "
    "AND o_orderdate >= '1993-10-01' AND o_orderdate < '1994-01-01' "
    "AND l_returnflag = 'R' "
    "AND c_nationkey = n_nationkey "
    "GROUP BY c_custkey, c_name, c_acctbal, c_phone, "
    "n_name, c_address, c_comment "
    "ORDER BY revenue DESC "
    "LIMIT 20";

static const char *Q12 =
    "SELECT l_shipmode, "
    "SUM(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' "
    "THEN 1 ELSE 0 END) AS high_line_count, "
    "SUM(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' "
    "THEN 1 ELSE 0 END) AS low_line_count "
    "FROM orders, lineitem "
    "WHERE o_orderkey = l_orderkey "
    "AND l_shipmode IN ('MAIL','SHIP') "
    "AND l_commitdate < l_receiptdate "
    "AND l_shipdate < l_commitdate "
    "AND l_receiptdate >= '1994-01-01' AND l_receiptdate < '1995-01-01' "
    "GROUP BY l_shipmode "
    "ORDER BY l_shipmode";

int main(void) {
    sqlite3 *db;
    const char *label = ENGINE_LABEL;

    if (sqlite3_open(":memory:", &db) != SQLITE_OK) {
        fprintf(stderr, "open failed\n"); return 1;
    }

    exec_or_die(db, "PRAGMA journal_mode=WAL");
    exec_or_die(db, "PRAGMA synchronous=NORMAL");
    exec_or_die(db, SCHEMA);
    generate_data(db);

    printf("\n==============================\n");
    printf("ENGINE: %s\n", label);
    printf("==============================\n");

    probe_data(db, label);

    run_query_rows(db, label, "Q10", Q10);
    run_query_rows(db, label, "Q12", Q12);

    run_eqp(db, label, "Q10", Q10);
    run_eqp(db, label, "Q12", Q12);

    run_explain(db, label, "Q10", Q10);
    run_explain(db, label, "Q12", Q12);

    sqlite3_close(db);
    return 0;
}
