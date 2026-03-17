/*
 * TPC-H Benchmark for SQLite-compatible databases
 * Generates TPC-H schema at small scale and runs standard queries.
 * Compile: gcc -O2 -o tpch_bench tpch_bench.c -I./include -lsqlite3
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "sqlite3.h"

/* Scale: rows per table */
#ifndef SCALE
#define SCALE 1
#endif

#define N_REGIONS    5
#define N_NATIONS   25
#define N_SUPPLIERS (10 * SCALE)
#define N_CUSTOMERS (50 * SCALE)
#define N_PARTS     (20 * SCALE)
#define N_PARTSUPP  (80 * SCALE)
#define N_ORDERS    (150 * SCALE)
#define N_LINEITEM  (600 * SCALE)

static void die(const char *msg, sqlite3 *db) {
    fprintf(stderr, "FATAL: %s — %s\n", msg, sqlite3_errmsg(db));
    sqlite3_close(db);
    exit(1);
}

static double now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

/* ---------- schema ---------- */
static const char *SCHEMA =
    "CREATE TABLE region ("
    "  r_regionkey INTEGER PRIMARY KEY,"
    "  r_name TEXT NOT NULL,"
    "  r_comment TEXT);"

    "CREATE TABLE nation ("
    "  n_nationkey INTEGER PRIMARY KEY,"
    "  n_name TEXT NOT NULL,"
    "  n_regionkey INTEGER NOT NULL,"
    "  n_comment TEXT,"
    "  FOREIGN KEY (n_regionkey) REFERENCES region(r_regionkey));"

    "CREATE TABLE supplier ("
    "  s_suppkey INTEGER PRIMARY KEY,"
    "  s_name TEXT NOT NULL,"
    "  s_address TEXT NOT NULL,"
    "  s_nationkey INTEGER NOT NULL,"
    "  s_phone TEXT NOT NULL,"
    "  s_acctbal REAL NOT NULL,"
    "  s_comment TEXT,"
    "  FOREIGN KEY (s_nationkey) REFERENCES nation(n_nationkey));"

    "CREATE TABLE customer ("
    "  c_custkey INTEGER PRIMARY KEY,"
    "  c_name TEXT NOT NULL,"
    "  c_address TEXT NOT NULL,"
    "  c_nationkey INTEGER NOT NULL,"
    "  c_phone TEXT NOT NULL,"
    "  c_acctbal REAL NOT NULL,"
    "  c_mktsegment TEXT NOT NULL,"
    "  c_comment TEXT,"
    "  FOREIGN KEY (c_nationkey) REFERENCES nation(n_nationkey));"

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
    "  PRIMARY KEY (ps_partkey, ps_suppkey),"
    "  FOREIGN KEY (ps_partkey) REFERENCES part(p_partkey),"
    "  FOREIGN KEY (ps_suppkey) REFERENCES supplier(s_suppkey));"

    "CREATE TABLE orders ("
    "  o_orderkey INTEGER PRIMARY KEY,"
    "  o_custkey INTEGER NOT NULL,"
    "  o_orderstatus TEXT NOT NULL,"
    "  o_totalprice REAL NOT NULL,"
    "  o_orderdate TEXT NOT NULL,"
    "  o_orderpriority TEXT NOT NULL,"
    "  o_clerk TEXT NOT NULL,"
    "  o_shippriority INTEGER NOT NULL,"
    "  o_comment TEXT,"
    "  FOREIGN KEY (o_custkey) REFERENCES customer(c_custkey));"

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
    "  PRIMARY KEY (l_orderkey, l_linenumber),"
    "  FOREIGN KEY (l_orderkey) REFERENCES orders(o_orderkey),"
    "  FOREIGN KEY (l_partkey, l_suppkey) REFERENCES partsupp(ps_partkey, ps_suppkey));"
;

/* ---------- data generation ---------- */

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

static void exec(sqlite3 *db, const char *sql) {
    char *err = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n  %s\n", err, sql);
        sqlite3_free(err);
    }
}

static void generate_data(sqlite3 *db) {
    char buf[2048];
    int i, j;
    double t0;

    exec(db, "BEGIN");

    /* regions */
    t0 = now_ms();
    for (i = 0; i < N_REGIONS; i++) {
        snprintf(buf, sizeof(buf),
            "INSERT INTO region VALUES(%d,'%s','comment %d')", i, regions[i], i);
        exec(db, buf);
    }

    printf("    region:   %.0f ms\n", now_ms() - t0); fflush(stdout);

    /* nations */
    t0 = now_ms();
    for (i = 0; i < N_NATIONS; i++) {
        snprintf(buf, sizeof(buf),
            "INSERT INTO nation VALUES(%d,'%s',%d,'comment %d')",
            i, nations[i], nation_region[i], i);
        exec(db, buf);
    }

    printf("    nation:   %.0f ms\n", now_ms() - t0); fflush(stdout);

    /* suppliers */
    t0 = now_ms();
    for (i = 1; i <= N_SUPPLIERS; i++) {
        snprintf(buf, sizeof(buf),
            "INSERT INTO supplier VALUES(%d,'Supplier#%04d','Addr %d',%d,'%02d-%03d-%03d-%04d',%.2f,'comment %d')",
            i, i, i, i % N_NATIONS, 10 + (i % 15), i % 1000, (i*7) % 1000, i % 10000,
            (double)(i * 73 % 10000) - 1000.0, i);
        exec(db, buf);
    }

    printf("    supplier: %.0f ms\n", now_ms() - t0); fflush(stdout);

    /* customers */
    t0 = now_ms();
    for (i = 1; i <= N_CUSTOMERS; i++) {
        snprintf(buf, sizeof(buf),
            "INSERT INTO customer VALUES(%d,'Customer#%06d','Addr %d',%d,'%02d-%03d-%03d-%04d',%.2f,'%s','comment %d')",
            i, i, i, i % N_NATIONS, 10 + (i % 15), i % 1000, (i*3) % 1000, i % 10000,
            (double)(i * 47 % 10000) - 500.0, segments[i % 5], i);
        exec(db, buf);
    }

    printf("    customer: %.0f ms\n", now_ms() - t0); fflush(stdout);

    /* parts */
    t0 = now_ms();
    for (i = 1; i <= N_PARTS; i++) {
        snprintf(buf, sizeof(buf),
            "INSERT INTO part VALUES(%d,'Part %d','Manufacturer#%d','%s','%s',%d,'%s',%.2f,'comment %d')",
            i, i, 1 + (i % 5), brands[i % 9], types[i % 5], 1 + (i % 50),
            containers[i % 12], 900.0 + (i % 200), i);
        exec(db, buf);
    }

    printf("    part:     %.0f ms\n", now_ms() - t0); fflush(stdout);

    /* partsupp */
    t0 = now_ms();
    for (i = 1; i <= N_PARTS; i++) {
        for (j = 0; j < 4; j++) {
            int sk = 1 + ((i * 4 + j) % N_SUPPLIERS);
            snprintf(buf, sizeof(buf),
                "INSERT OR IGNORE INTO partsupp VALUES(%d,%d,%d,%.2f,'comment %d-%d')",
                i, sk, 100 + (i * 7 + j * 13) % 9999,
                1.0 + (double)((i * 31 + j * 17) % 100000) / 100.0, i, j);
            exec(db, buf);
        }
    }

    printf("    partsupp: %.0f ms\n", now_ms() - t0); fflush(stdout);

    /* orders */
    t0 = now_ms();
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
        exec(db, buf);
    }

    printf("    orders:   %.0f ms\n", now_ms() - t0); fflush(stdout);

    /* lineitem */
    t0 = now_ms();
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
            exec(db, buf);
        }
    }

    printf("    lineitem: %.0f ms\n", now_ms() - t0); fflush(stdout);

    t0 = now_ms();
    exec(db, "COMMIT");
    printf("    commit:   %.0f ms\n", now_ms() - t0); fflush(stdout);
}

/* ---------- TPC-H queries ---------- */

typedef struct {
    int num;
    const char *name;
    const char *sql;
} tpch_query;

static const tpch_query queries[] = {
    {1, "Pricing Summary Report",
     "SELECT l_returnflag, l_linestatus, "
     "SUM(l_quantity) AS sum_qty, "
     "SUM(l_extendedprice) AS sum_base_price, "
     "SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price, "
     "SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, "
     "AVG(l_quantity) AS avg_qty, "
     "AVG(l_extendedprice) AS avg_price, "
     "AVG(l_discount) AS avg_disc, "
     "COUNT(*) AS count_order "
     "FROM lineitem "
     "WHERE l_shipdate <= '1998-09-02' "
     "GROUP BY l_returnflag, l_linestatus "
     "ORDER BY l_returnflag, l_linestatus"},

    {3, "Shipping Priority",
     "SELECT l_orderkey, "
     "SUM(l_extendedprice * (1 - l_discount)) AS revenue, "
     "o_orderdate, o_shippriority "
     "FROM customer, orders, lineitem "
     "WHERE c_mktsegment = 'BUILDING' "
     "AND c_custkey = o_custkey "
     "AND l_orderkey = o_orderkey "
     "AND o_orderdate < '1995-03-15' "
     "AND l_shipdate > '1995-03-15' "
     "GROUP BY l_orderkey, o_orderdate, o_shippriority "
     "ORDER BY revenue DESC, o_orderdate "
     "LIMIT 10"},

    {4, "Order Priority Checking",
     "SELECT o_orderpriority, COUNT(*) AS order_count "
     "FROM orders "
     "WHERE o_orderdate >= '1993-07-01' AND o_orderdate < '1993-10-01' "
     "AND EXISTS ("
     "  SELECT * FROM lineitem WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate"
     ") "
     "GROUP BY o_orderpriority "
     "ORDER BY o_orderpriority"},

    {5, "Local Supplier Volume",
     "SELECT n_name, "
     "SUM(l_extendedprice * (1 - l_discount)) AS revenue "
     "FROM customer, orders, lineitem, supplier, nation, region "
     "WHERE c_custkey = o_custkey "
     "AND l_orderkey = o_orderkey "
     "AND l_suppkey = s_suppkey "
     "AND c_nationkey = s_nationkey "
     "AND s_nationkey = n_nationkey "
     "AND n_regionkey = r_regionkey "
     "AND r_name = 'ASIA' "
     "AND o_orderdate >= '1994-01-01' AND o_orderdate < '1995-01-01' "
     "GROUP BY n_name "
     "ORDER BY revenue DESC"},

    {6, "Forecasting Revenue Change",
     "SELECT SUM(l_extendedprice * l_discount) AS revenue "
     "FROM lineitem "
     "WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01' "
     "AND l_discount BETWEEN 0.05 AND 0.07 "
     "AND l_quantity < 24"},

    {10, "Returned Item Reporting",
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
     "LIMIT 20"},

    {12, "Shipping Modes and Order Priority",
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
     "ORDER BY l_shipmode"},

    {13, "Customer Distribution",
     "SELECT c_count, COUNT(*) AS custdist "
     "FROM ("
     "  SELECT c_custkey, COUNT(o_orderkey) AS c_count "
     "  FROM customer LEFT OUTER JOIN orders ON "
     "  c_custkey = o_custkey AND o_comment NOT LIKE '%%special%%requests%%' "
     "  GROUP BY c_custkey"
     ") AS c_orders "
     "GROUP BY c_count "
     "ORDER BY custdist DESC, c_count DESC"},

    {14, "Promotion Effect",
     "SELECT 100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%%' "
     "THEN l_extendedprice * (1 - l_discount) ELSE 0 END) / "
     "SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue "
     "FROM lineitem, part "
     "WHERE l_partkey = p_partkey "
     "AND l_shipdate >= '1995-09-01' AND l_shipdate < '1995-10-01'"},

    {19, "Discounted Revenue (simplified)",
     "SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue "
     "FROM lineitem, part "
     "WHERE l_partkey = p_partkey "
     "AND p_brand = 'Brand#12' "
     "AND p_container IN ('SM CASE','SM BOX','SM PACK','SM PKG') "
     "AND l_quantity >= 1 AND l_quantity <= 11 "
     "AND p_size BETWEEN 1 AND 5 "
     "AND l_shipmode IN ('AIR','REG AIR') "
     "AND l_shipinstruct = 'DELIVER IN PERSON'"},
};

#define N_QUERIES (sizeof(queries) / sizeof(queries[0]))

/* ---------- run ---------- */

static int count_rows(sqlite3 *db, const char *sql) {
    sqlite3_stmt *stmt;
    int rows = 0;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW)
            rows = sqlite3_column_int(stmt, 0);
        sqlite3_finalize(stmt);
    }
    return rows;
}

static void run_query(sqlite3 *db, const tpch_query *q, const char *label) {
    sqlite3_stmt *stmt;
    int rc, rows = 0;
    double t0, t1;

    printf("  Q%-2d %-35s  ", q->num, q->name); fflush(stdout);

    rc = sqlite3_prepare_v2(db, q->sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        printf("ERROR (prepare): %s\n", sqlite3_errmsg(db));
        return;
    }

    t0 = now_ms();
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        rows++;
        if (now_ms() - t0 > 30000) { /* 30s timeout per query */
            t1 = now_ms();
            sqlite3_finalize(stmt);
            printf("TIMEOUT after %.0f ms (%d rows so far)\n", t1 - t0, rows);
            return;
        }
    }
    t1 = now_ms();

    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE) {
        printf("ERROR (step): %s (%.2f ms)\n", sqlite3_errmsg(db), t1 - t0);
    } else {
        printf("%8.2f ms  (%d rows)\n", t1 - t0, rows);
    }
}

int main(int argc, char **argv) {
    const char *dbpath = (argc > 1) ? argv[1] : ":memory:";
    const char *label  = (argc > 2) ? argv[2] : "SQLite";
    sqlite3 *db;
    double t0, t1;
    int i;

    printf("=== TPC-H Benchmark: %s ===\n", label);
    printf("Database: %s\n\n", dbpath);

    if (sqlite3_open(dbpath, &db) != SQLITE_OK)
        die("open", db);

    exec(db, "PRAGMA journal_mode=WAL");
    exec(db, "PRAGMA synchronous=NORMAL");

    /* schema */
    printf("Creating schema...\n");
    t0 = now_ms();
    exec(db, SCHEMA);
    t1 = now_ms();
    printf("  Schema: %.2f ms\n", t1 - t0);

    /* data */
    printf("Generating data...\n");
    t0 = now_ms();
    generate_data(db);
    t1 = now_ms();
    printf("  Data load: %.2f ms\n", t1 - t0);

    /* row counts — skip if db is slow */
    printf("\nRow counts:\n"); fflush(stdout);
    t0 = now_ms();
    printf("  region:    %d", count_rows(db, "SELECT COUNT(*) FROM region"));
    t1 = now_ms();
    printf("  (%.0f ms)\n", t1 - t0); fflush(stdout);
    if (t1 - t0 > 5000) {
        printf("  (skipping remaining counts — too slow)\n");
    } else {
        printf("  nation:    %d\n", count_rows(db, "SELECT COUNT(*) FROM nation"));
        printf("  supplier:  %d\n", count_rows(db, "SELECT COUNT(*) FROM supplier"));
        printf("  customer:  %d\n", count_rows(db, "SELECT COUNT(*) FROM customer"));
        printf("  part:      %d\n", count_rows(db, "SELECT COUNT(*) FROM part"));
        printf("  partsupp:  %d\n", count_rows(db, "SELECT COUNT(*) FROM partsupp"));
        printf("  orders:    %d\n", count_rows(db, "SELECT COUNT(*) FROM orders"));
        printf("  lineitem:  %d\n", count_rows(db, "SELECT COUNT(*) FROM lineitem"));
    }

    /* queries */
    printf("\n--- TPC-H Queries ---\n");
    for (i = 0; i < (int)N_QUERIES; i++) {
        run_query(db, &queries[i], label);
    }

    sqlite3_close(db);
    printf("\nDone.\n");
    return 0;
}
