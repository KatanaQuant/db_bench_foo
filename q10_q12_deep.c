/*
 * Deep diagnostic for Q10 and Q12 0-row bug in FrankenSQLite
 * Progressively decomposes the failing queries to isolate the exact predicate
 * that causes 0 rows.
 *
 * Build SQLite:
 *   gcc -O2 -DENGINE_LABEL='"SQLite"' -o q10_q12_deep_sqlite q10_q12_deep.c -I./include \
 *     /usr/lib/x86_64-linux-gnu/libsqlite3.so.0
 *
 * Build FrankenSQLite:
 *   FSQL=frankensqlite/target/release
 *   gcc -O2 -DENGINE_LABEL='"FrankenSQLite"' -o q10_q12_deep_fsqlite q10_q12_deep.c -I./include \
 *     -L$FSQL -lfsqlite_c_api -Wl,-rpath,$FSQL
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "sqlite3.h"

static const char *SCHEMA =
    "CREATE TABLE region (r_regionkey INTEGER PRIMARY KEY, r_name TEXT NOT NULL, r_comment TEXT);"
    "CREATE TABLE nation (n_nationkey INTEGER PRIMARY KEY, n_name TEXT NOT NULL, n_regionkey INTEGER NOT NULL, n_comment TEXT);"
    "CREATE TABLE supplier (s_suppkey INTEGER PRIMARY KEY, s_name TEXT NOT NULL, s_address TEXT NOT NULL, s_nationkey INTEGER NOT NULL, s_phone TEXT NOT NULL, s_acctbal REAL NOT NULL, s_comment TEXT);"
    "CREATE TABLE customer (c_custkey INTEGER PRIMARY KEY, c_name TEXT NOT NULL, c_address TEXT NOT NULL, c_nationkey INTEGER NOT NULL, c_phone TEXT NOT NULL, c_acctbal REAL NOT NULL, c_mktsegment TEXT NOT NULL, c_comment TEXT);"
    "CREATE TABLE part (p_partkey INTEGER PRIMARY KEY, p_name TEXT NOT NULL, p_mfgr TEXT NOT NULL, p_brand TEXT NOT NULL, p_type TEXT NOT NULL, p_size INTEGER NOT NULL, p_container TEXT NOT NULL, p_retailprice REAL NOT NULL, p_comment TEXT);"
    "CREATE TABLE partsupp (ps_partkey INTEGER NOT NULL, ps_suppkey INTEGER NOT NULL, ps_availqty INTEGER NOT NULL, ps_supplycost REAL NOT NULL, ps_comment TEXT, PRIMARY KEY (ps_partkey, ps_suppkey));"
    "CREATE TABLE orders (o_orderkey INTEGER PRIMARY KEY, o_custkey INTEGER NOT NULL, o_orderstatus TEXT NOT NULL, o_totalprice REAL NOT NULL, o_orderdate TEXT NOT NULL, o_orderpriority TEXT NOT NULL, o_clerk TEXT NOT NULL, o_shippriority INTEGER NOT NULL, o_comment TEXT);"
    "CREATE TABLE lineitem (l_orderkey INTEGER NOT NULL, l_partkey INTEGER NOT NULL, l_suppkey INTEGER NOT NULL, l_linenumber INTEGER NOT NULL, l_quantity REAL NOT NULL, l_extendedprice REAL NOT NULL, l_discount REAL NOT NULL, l_tax REAL NOT NULL, l_returnflag TEXT NOT NULL, l_linestatus TEXT NOT NULL, l_shipdate TEXT NOT NULL, l_commitdate TEXT NOT NULL, l_receiptdate TEXT NOT NULL, l_shipinstruct TEXT NOT NULL, l_shipmode TEXT NOT NULL, l_comment TEXT, PRIMARY KEY (l_orderkey, l_linenumber));"
;

static void exec_or_die(sqlite3 *db, const char *sql) {
    char *err = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n  SQL: %.200s\n", err, sql);
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
    static const int nation_region[] = {0,1,1,1,4,0,3,3,2,2,4,4,2,4,0,0,0,1,2,3,4,2,3,3,1};
    static const char *segments[] = {"AUTOMOBILE","BUILDING","FURNITURE","HOUSEHOLD","MACHINERY"};
    static const char *priorities[] = {"1-URGENT","2-HIGH","3-MEDIUM","4-NOT SPECIFIED","5-LOW"};
    static const char *shipmodes[] = {"REG AIR","AIR","RAIL","SHIP","TRUCK","MAIL","FOB"};
    static const char *instructions[] = {"DELIVER IN PERSON","COLLECT COD","NONE","TAKE BACK RETURN"};
    static const char *brands[] = {"Brand#11","Brand#12","Brand#13","Brand#21","Brand#22","Brand#23","Brand#31","Brand#32","Brand#33"};
    static const char *types[] = {"STANDARD ANODIZED TIN","SMALL PLATED COPPER","MEDIUM BURNISHED STEEL","LARGE POLISHED BRASS","ECONOMY BRUSHED NICKEL"};
    static const char *containers[] = {"SM CASE","SM BOX","SM PACK","SM PKG","MED BAG","MED BOX","MED PKG","LG CASE","LG BOX","LG DRUM","WRAP CASE","WRAP BOX"};
    int N_NATIONS=25, N_SUPPLIERS=10, N_CUSTOMERS=50, N_PARTS=20, N_ORDERS=150;

    exec_or_die(db, "BEGIN");
    for (i=0; i<5; i++) {
        snprintf(buf,sizeof(buf),"INSERT INTO region VALUES(%d,'%s','comment %d')",i,regions[i],i);
        exec_or_die(db,buf);
    }
    for (i=0; i<N_NATIONS; i++) {
        snprintf(buf,sizeof(buf),"INSERT INTO nation VALUES(%d,'%s',%d,'comment %d')",i,nations[i],nation_region[i],i);
        exec_or_die(db,buf);
    }
    for (i=1; i<=N_SUPPLIERS; i++) {
        snprintf(buf,sizeof(buf),"INSERT INTO supplier VALUES(%d,'Supplier#%04d','Addr %d',%d,'%02d-%03d-%03d-%04d',%.2f,'comment %d')",
            i,i,i,i%N_NATIONS,10+(i%15),i%1000,(i*7)%1000,i%10000,(double)(i*73%10000)-1000.0,i);
        exec_or_die(db,buf);
    }
    for (i=1; i<=N_CUSTOMERS; i++) {
        snprintf(buf,sizeof(buf),"INSERT INTO customer VALUES(%d,'Customer#%06d','Addr %d',%d,'%02d-%03d-%03d-%04d',%.2f,'%s','comment %d')",
            i,i,i,i%N_NATIONS,10+(i%15),i%1000,(i*3)%1000,i%10000,(double)(i*47%10000)-500.0,segments[i%5],i);
        exec_or_die(db,buf);
    }
    for (i=1; i<=N_PARTS; i++) {
        snprintf(buf,sizeof(buf),"INSERT INTO part VALUES(%d,'Part %d','Manufacturer#%d','%s','%s',%d,'%s',%.2f,'comment %d')",
            i,i,1+(i%5),brands[i%9],types[i%5],1+(i%50),containers[i%12],900.0+(i%200),i);
        exec_or_die(db,buf);
    }
    for (i=1; i<=N_PARTS; i++) {
        for (j=0; j<4; j++) {
            int sk=1+((i*4+j)%N_SUPPLIERS);
            snprintf(buf,sizeof(buf),"INSERT OR IGNORE INTO partsupp VALUES(%d,%d,%d,%.2f,'comment %d-%d')",
                i,sk,100+(i*7+j*13)%9999,1.0+(double)((i*31+j*17)%100000)/100.0,i,j);
            exec_or_die(db,buf);
        }
    }
    for (i=1; i<=N_ORDERS; i++) {
        const char *status=(i%3==0)?"F":(i%3==1)?"O":"P";
        int year=1993+(i%5),month=1+(i%12),day=1+(i%28);
        snprintf(buf,sizeof(buf),"INSERT INTO orders VALUES(%d,%d,'%s',%.2f,'%04d-%02d-%02d','%s','Clerk#%05d',%d,'comment %d')",
            i,1+(i%N_CUSTOMERS),status,(double)(i*127%50000)+1000.0,year,month,day,priorities[i%5],1+(i%1000),i%8,i);
        exec_or_die(db,buf);
    }
    for (i=1; i<=N_ORDERS; i++) {
        int nitems=1+(i%4);
        for (j=1; j<=nitems; j++) {
            int pk=1+((i*3+j*7)%N_PARTS);
            int sk=1+((pk+j*25)%N_SUPPLIERS);
            int year=1993+(i%5);
            int smonth=1+((i+j)%12),sday=1+((i+j*3)%28);
            int cmonth=1+((i+j+1)%12),cday=1+((i+j*3+7)%28);
            int rmonth=1+((i+j+2)%12),rday=1+((i+j*3+14)%28);
            double qty=1.0+(i*j)%50,price=900.0+(pk%200),disc=((i+j)%11)/100.0,tax=((i*j)%9)/100.0;
            const char *rf=(year<1995)?"R":(year==1995&&smonth<=6)?"R":"N";
            const char *ls=(year<1995)?"F":"O";
            snprintf(buf,sizeof(buf),
                "INSERT INTO lineitem VALUES(%d,%d,%d,%d,%.1f,%.2f,%.2f,%.2f,'%s','%s','%04d-%02d-%02d','%04d-%02d-%02d','%04d-%02d-%02d','%s','%s','comment %d-%d')",
                i,pk,sk,j,qty,price*qty,disc,tax,rf,ls,year,smonth,sday,year,cmonth,cday,year,rmonth,rday,
                instructions[j%4],shipmodes[(i+j)%7],i,j);
            exec_or_die(db,buf);
        }
    }
    exec_or_die(db, "COMMIT");
}

static int run_count(sqlite3 *db, const char *sql) {
    sqlite3_stmt *stmt;
    int rows = 0;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        printf("    PREPARE ERROR: %s\n", sqlite3_errmsg(db));
        return -1;
    }
    int rc;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) rows++;
    if (rc != SQLITE_DONE) printf("    STEP ERROR: %s\n", sqlite3_errmsg(db));
    sqlite3_finalize(stmt);
    return rows;
}

/* Run query and show first row values for key columns */
static void run_show(sqlite3 *db, const char *sql, int max_rows) {
    sqlite3_stmt *stmt;
    int rows = 0;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        printf("    PREPARE ERROR: %s\n", sqlite3_errmsg(db));
        return;
    }
    int ncols = sqlite3_column_count(stmt);
    int rc;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        rows++;
        if (rows <= max_rows) {
            printf("    row %d:", rows);
            for (int k = 0; k < ncols && k < 6; k++) {
                const char *v = (const char *)sqlite3_column_text(stmt, k);
                printf(" [%s]", v ? v : "NULL");
            }
            printf("\n");
        }
    }
    if (rc != SQLITE_DONE) printf("    STEP ERROR: %s\n", sqlite3_errmsg(db));
    sqlite3_finalize(stmt);
    printf("    => %d row(s)\n", rows);
}

#define PROBE(label, sql) do { \
    printf("  [%s]\n    %s\n", label, sql); \
    run_show(db, sql, 3); \
} while(0)

int main(void) {
    sqlite3 *db;
    const char *label = ENGINE_LABEL;

    if (sqlite3_open(":memory:", &db) != SQLITE_OK) {
        fprintf(stderr, "open failed\n"); return 1;
    }
    exec_or_die(db, "PRAGMA journal_mode=WAL");
    exec_or_die(db, SCHEMA);
    generate_data(db);

    printf("\n======== ENGINE: %s ========\n\n", label);

    /* ===== Q10 DECOMPOSITION ===== */
    printf("=== Q10 DECOMPOSITION ===\n\n");

    PROBE("Q10-A: All lineitems with returnflag=R",
          "SELECT l_orderkey, l_returnflag FROM lineitem WHERE l_returnflag = 'R' LIMIT 5");

    PROBE("Q10-B: All orders in date window",
          "SELECT o_orderkey, o_orderdate FROM orders "
          "WHERE o_orderdate >= '1993-10-01' AND o_orderdate < '1994-01-01'");

    PROBE("Q10-C: JOIN orders+lineitem (no date filter)",
          "SELECT o_orderkey, l_orderkey, l_returnflag "
          "FROM orders, lineitem WHERE o_orderkey = l_orderkey LIMIT 5");

    PROBE("Q10-D: JOIN orders+lineitem with date filter only",
          "SELECT o_orderkey, o_orderdate, l_returnflag "
          "FROM orders, lineitem "
          "WHERE o_orderkey = l_orderkey "
          "AND o_orderdate >= '1993-10-01' AND o_orderdate < '1994-01-01'");

    PROBE("Q10-E: JOIN orders+lineitem with date filter + returnflag=R",
          "SELECT o_orderkey, o_orderdate, l_returnflag "
          "FROM orders, lineitem "
          "WHERE o_orderkey = l_orderkey "
          "AND o_orderdate >= '1993-10-01' AND o_orderdate < '1994-01-01' "
          "AND l_returnflag = 'R'");

    PROBE("Q10-F: Full 4-table join (no date/returnflag)",
          "SELECT c_custkey, o_orderkey, l_orderkey, n_name "
          "FROM customer, orders, lineitem, nation "
          "WHERE c_custkey = o_custkey "
          "AND l_orderkey = o_orderkey "
          "AND c_nationkey = n_nationkey "
          "LIMIT 5");

    PROBE("Q10-G: Full 4-table join with date filter",
          "SELECT c_custkey, o_orderdate, l_returnflag, n_name "
          "FROM customer, orders, lineitem, nation "
          "WHERE c_custkey = o_custkey "
          "AND l_orderkey = o_orderkey "
          "AND o_orderdate >= '1993-10-01' AND o_orderdate < '1994-01-01' "
          "AND c_nationkey = n_nationkey");

    PROBE("Q10-H: Full 4-table join with all filters",
          "SELECT c_custkey, o_orderdate, l_returnflag, n_name "
          "FROM customer, orders, lineitem, nation "
          "WHERE c_custkey = o_custkey "
          "AND l_orderkey = o_orderkey "
          "AND o_orderdate >= '1993-10-01' AND o_orderdate < '1994-01-01' "
          "AND l_returnflag = 'R' "
          "AND c_nationkey = n_nationkey");

    /* ===== Q12 DECOMPOSITION ===== */
    printf("\n=== Q12 DECOMPOSITION ===\n\n");

    PROBE("Q12-A: Lineitems with shipmode MAIL or SHIP",
          "SELECT l_orderkey, l_shipmode, l_shipdate, l_commitdate, l_receiptdate "
          "FROM lineitem WHERE l_shipmode IN ('MAIL','SHIP') LIMIT 5");

    PROBE("Q12-B: Q12 date filter on receiptdate only",
          "SELECT COUNT(*) FROM lineitem "
          "WHERE l_receiptdate >= '1994-01-01' AND l_receiptdate < '1995-01-01'");

    PROBE("Q12-C: shipmode+receiptdate filter",
          "SELECT l_orderkey, l_shipmode, l_shipdate, l_commitdate, l_receiptdate "
          "FROM lineitem "
          "WHERE l_shipmode IN ('MAIL','SHIP') "
          "AND l_receiptdate >= '1994-01-01' AND l_receiptdate < '1995-01-01' LIMIT 5");

    PROBE("Q12-D: shipmode+date+commitdate<receiptdate",
          "SELECT l_orderkey, l_shipmode, l_shipdate, l_commitdate, l_receiptdate "
          "FROM lineitem "
          "WHERE l_shipmode IN ('MAIL','SHIP') "
          "AND l_receiptdate >= '1994-01-01' AND l_receiptdate < '1995-01-01' "
          "AND l_commitdate < l_receiptdate LIMIT 5");

    PROBE("Q12-E: All 3 date conditions",
          "SELECT l_orderkey, l_shipmode, l_shipdate, l_commitdate, l_receiptdate "
          "FROM lineitem "
          "WHERE l_shipmode IN ('MAIL','SHIP') "
          "AND l_receiptdate >= '1994-01-01' AND l_receiptdate < '1995-01-01' "
          "AND l_commitdate < l_receiptdate "
          "AND l_shipdate < l_commitdate LIMIT 5");

    PROBE("Q12-F: JOIN orders+lineitem, shipmode only",
          "SELECT o_orderkey, l_orderkey, l_shipmode, o_orderpriority "
          "FROM orders, lineitem "
          "WHERE o_orderkey = l_orderkey "
          "AND l_shipmode IN ('MAIL','SHIP') LIMIT 5");

    PROBE("Q12-G: JOIN + all lineitem filters",
          "SELECT o_orderkey, l_shipmode, l_shipdate, l_commitdate, l_receiptdate "
          "FROM orders, lineitem "
          "WHERE o_orderkey = l_orderkey "
          "AND l_shipmode IN ('MAIL','SHIP') "
          "AND l_commitdate < l_receiptdate "
          "AND l_shipdate < l_commitdate "
          "AND l_receiptdate >= '1994-01-01' AND l_receiptdate < '1995-01-01'");

    /* ===== CROSS-COLUMN DATE COMPARISON ===== */
    printf("\n=== DATE COMPARISON PROBES ===\n\n");

    PROBE("DATE-A: commitdate < receiptdate (raw)",
          "SELECT COUNT(*) FROM lineitem WHERE l_commitdate < l_receiptdate");

    PROBE("DATE-B: shipdate < commitdate (raw)",
          "SELECT COUNT(*) FROM lineitem WHERE l_shipdate < l_commitdate");

    PROBE("DATE-C: Both cross-column date comparisons",
          "SELECT COUNT(*) FROM lineitem "
          "WHERE l_commitdate < l_receiptdate AND l_shipdate < l_commitdate");

    PROBE("DATE-D: Sample dates to verify format",
          "SELECT l_shipdate, l_commitdate, l_receiptdate "
          "FROM lineitem LIMIT 5");

    /* ===== IN OPERATOR TEST ===== */
    printf("\n=== IN OPERATOR PROBES ===\n\n");

    PROBE("IN-A: shipmode IN ('MAIL','SHIP') vs OR",
          "SELECT l_shipmode, COUNT(*) FROM lineitem "
          "WHERE l_shipmode = 'MAIL' OR l_shipmode = 'SHIP' "
          "GROUP BY l_shipmode");

    PROBE("IN-B: shipmode IN ('MAIL','SHIP')",
          "SELECT l_shipmode, COUNT(*) FROM lineitem "
          "WHERE l_shipmode IN ('MAIL','SHIP') "
          "GROUP BY l_shipmode");

    /* ===== BETWEEN TEST ===== */
    printf("\n=== BETWEEN OPERATOR PROBES ===\n\n");

    PROBE("BETWEEN-A: date range with BETWEEN",
          "SELECT COUNT(*) FROM lineitem "
          "WHERE l_receiptdate BETWEEN '1994-01-01' AND '1994-12-31'");

    PROBE("BETWEEN-B: date range with >= AND <",
          "SELECT COUNT(*) FROM lineitem "
          "WHERE l_receiptdate >= '1994-01-01' AND l_receiptdate < '1995-01-01'");

    /* ===== MULTI-TABLE JOIN ORDER ===== */
    printf("\n=== JOIN ORDER PROBES ===\n\n");

    PROBE("JOIN-A: orders,lineitem (original order)",
          "SELECT COUNT(*) FROM orders, lineitem WHERE o_orderkey = l_orderkey");

    PROBE("JOIN-B: lineitem,orders (reversed)",
          "SELECT COUNT(*) FROM lineitem, orders WHERE l_orderkey = o_orderkey");

    sqlite3_close(db);
    return 0;
}
