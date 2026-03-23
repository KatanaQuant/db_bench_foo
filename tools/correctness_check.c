/*
 * Correctness spot-check: show actual result rows for queries where
 * FrankenSQLite and SQLite return different row counts.
 *
 * TPC-H:  Q10 (customer, orders, lineitem, nation)
 *         Q12 (orders, lineitem)
 * TPC-DS: Q3  (date_dim, store_sales, item)
 *
 * Build SQLite:
 *   gcc -O2 -DENGINE_LABEL='"SQLite"' -o correctness_sqlite correctness_check.c \
 *     -I./include -lsqlite3
 *
 * Build FrankenSQLite:
 *   gcc -O2 -DENGINE_LABEL='"FrankenSQLite"' -o correctness_fsqlite correctness_check.c \
 *     -I./include -L$FSQL -lfsqlite_c_api \
 *     -Wl,-rpath,$FSQL
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "sqlite3.h"

/* ===================================================================
 * Shared helpers
 * ================================================================== */

static void exec_or_warn(sqlite3 *db, const char *sql) {
    char *err = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n  %.200s\n", err, sql);
        sqlite3_free(err);
    }
}

/* Run a query and print all result rows (up to max_rows).
 * Returns actual row count (regardless of max_rows). */
static int run_show(sqlite3 *db, const char *label, const char *sql, int max_rows) {
    sqlite3_stmt *stmt;
    int rows = 0;
    printf("\n  [%s]\n", label);
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        printf("  PREPARE ERROR: %s\n", sqlite3_errmsg(db));
        return -1;
    }
    int ncols = sqlite3_column_count(stmt);
    printf("  ncols=%d\n", ncols);

    int rc;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        rows++;
        if (rows <= max_rows) {
            printf("  row %d:", rows);
            for (int k = 0; k < ncols; k++) {
                int type = sqlite3_column_type(stmt, k);
                if (type == SQLITE_NULL) {
                    printf(" [NULL]");
                } else if (type == SQLITE_INTEGER) {
                    printf(" [%lld]", (long long)sqlite3_column_int64(stmt, k));
                } else if (type == SQLITE_FLOAT) {
                    printf(" [%.4f]", sqlite3_column_double(stmt, k));
                } else {
                    const char *v = (const char *)sqlite3_column_text(stmt, k);
                    printf(" [%s]", v ? v : "NULL");
                }
            }
            printf("\n");
        }
    }
    sqlite3_finalize(stmt);
    if (rc != SQLITE_DONE)
        printf("  STEP ERROR: %s\n", sqlite3_errmsg(db));
    if (rows > max_rows)
        printf("  ... (%d more rows not shown)\n", rows - max_rows);
    printf("  => TOTAL %d row(s)\n", rows);
    return rows;
}

/* ===================================================================
 * TPC-H schema + data (identical to tpch_bench.c, SCALE=1)
 * ================================================================== */

static const char *TPCH_SCHEMA =
    "CREATE TABLE region (r_regionkey INTEGER PRIMARY KEY, r_name TEXT NOT NULL, r_comment TEXT);"
    "CREATE TABLE nation (n_nationkey INTEGER PRIMARY KEY, n_name TEXT NOT NULL, n_regionkey INTEGER NOT NULL, n_comment TEXT);"
    "CREATE TABLE supplier (s_suppkey INTEGER PRIMARY KEY, s_name TEXT NOT NULL, s_address TEXT NOT NULL, s_nationkey INTEGER NOT NULL, s_phone TEXT NOT NULL, s_acctbal REAL NOT NULL, s_comment TEXT);"
    "CREATE TABLE customer (c_custkey INTEGER PRIMARY KEY, c_name TEXT NOT NULL, c_address TEXT NOT NULL, c_nationkey INTEGER NOT NULL, c_phone TEXT NOT NULL, c_acctbal REAL NOT NULL, c_mktsegment TEXT NOT NULL, c_comment TEXT);"
    "CREATE TABLE part (p_partkey INTEGER PRIMARY KEY, p_name TEXT NOT NULL, p_mfgr TEXT NOT NULL, p_brand TEXT NOT NULL, p_type TEXT NOT NULL, p_size INTEGER NOT NULL, p_container TEXT NOT NULL, p_retailprice REAL NOT NULL, p_comment TEXT);"
    "CREATE TABLE partsupp (ps_partkey INTEGER NOT NULL, ps_suppkey INTEGER NOT NULL, ps_availqty INTEGER NOT NULL, ps_supplycost REAL NOT NULL, ps_comment TEXT, PRIMARY KEY (ps_partkey, ps_suppkey));"
    "CREATE TABLE orders (o_orderkey INTEGER PRIMARY KEY, o_custkey INTEGER NOT NULL, o_orderstatus TEXT NOT NULL, o_totalprice REAL NOT NULL, o_orderdate TEXT NOT NULL, o_orderpriority TEXT NOT NULL, o_clerk TEXT NOT NULL, o_shippriority INTEGER NOT NULL, o_comment TEXT);"
    "CREATE TABLE lineitem (l_orderkey INTEGER NOT NULL, l_partkey INTEGER NOT NULL, l_suppkey INTEGER NOT NULL, l_linenumber INTEGER NOT NULL, l_quantity REAL NOT NULL, l_extendedprice REAL NOT NULL, l_discount REAL NOT NULL, l_tax REAL NOT NULL, l_returnflag TEXT NOT NULL, l_linestatus TEXT NOT NULL, l_shipdate TEXT NOT NULL, l_commitdate TEXT NOT NULL, l_receiptdate TEXT NOT NULL, l_shipinstruct TEXT NOT NULL, l_shipmode TEXT NOT NULL, l_comment TEXT, PRIMARY KEY (l_orderkey, l_linenumber));";

static void tpch_generate(sqlite3 *db) {
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

    exec_or_warn(db, "BEGIN");
    for (i=0; i<5; i++) {
        snprintf(buf,sizeof(buf),"INSERT INTO region VALUES(%d,'%s','comment %d')",i,regions[i],i);
        exec_or_warn(db,buf);
    }
    for (i=0; i<N_NATIONS; i++) {
        snprintf(buf,sizeof(buf),"INSERT INTO nation VALUES(%d,'%s',%d,'comment %d')",i,nations[i],nation_region[i],i);
        exec_or_warn(db,buf);
    }
    for (i=1; i<=N_SUPPLIERS; i++) {
        snprintf(buf,sizeof(buf),"INSERT INTO supplier VALUES(%d,'Supplier#%04d','Addr %d',%d,'%02d-%03d-%03d-%04d',%.2f,'comment %d')",
            i,i,i,i%N_NATIONS,10+(i%15),i%1000,(i*7)%1000,i%10000,(double)(i*73%10000)-1000.0,i);
        exec_or_warn(db,buf);
    }
    for (i=1; i<=N_CUSTOMERS; i++) {
        snprintf(buf,sizeof(buf),"INSERT INTO customer VALUES(%d,'Customer#%06d','Addr %d',%d,'%02d-%03d-%03d-%04d',%.2f,'%s','comment %d')",
            i,i,i,i%N_NATIONS,10+(i%15),i%1000,(i*3)%1000,i%10000,(double)(i*47%10000)-500.0,segments[i%5],i);
        exec_or_warn(db,buf);
    }
    for (i=1; i<=N_PARTS; i++) {
        snprintf(buf,sizeof(buf),"INSERT INTO part VALUES(%d,'Part %d','Manufacturer#%d','%s','%s',%d,'%s',%.2f,'comment %d')",
            i,i,1+(i%5),brands[i%9],types[i%5],1+(i%50),containers[i%12],900.0+(i%200),i);
        exec_or_warn(db,buf);
    }
    for (i=1; i<=N_PARTS; i++) {
        for (j=0; j<4; j++) {
            int sk=1+((i*4+j)%N_SUPPLIERS);
            snprintf(buf,sizeof(buf),"INSERT OR IGNORE INTO partsupp VALUES(%d,%d,%d,%.2f,'comment %d-%d')",
                i,sk,100+(i*7+j*13)%9999,1.0+(double)((i*31+j*17)%100000)/100.0,i,j);
            exec_or_warn(db,buf);
        }
    }
    for (i=1; i<=N_ORDERS; i++) {
        const char *status=(i%3==0)?"F":(i%3==1)?"O":"P";
        int year=1993+(i%5),month=1+(i%12),day=1+(i%28);
        snprintf(buf,sizeof(buf),"INSERT INTO orders VALUES(%d,%d,'%s',%.2f,'%04d-%02d-%02d','%s','Clerk#%05d',%d,'comment %d')",
            i,1+(i%N_CUSTOMERS),status,(double)(i*127%50000)+1000.0,year,month,day,priorities[i%5],1+(i%1000),i%8,i);
        exec_or_warn(db,buf);
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
            exec_or_warn(db,buf);
        }
    }
    exec_or_warn(db, "COMMIT");
}

/* ===================================================================
 * TPC-DS schema + data (identical to tpcds_bench.c, DS_SCALE=1)
 * ================================================================== */

static const char *TPCDS_SCHEMA[] = {
    "CREATE TABLE date_dim (d_date_sk INTEGER PRIMARY KEY, d_date_id TEXT, d_date TEXT, d_month_seq INTEGER, d_week_seq INTEGER, d_quarter_seq INTEGER, d_year INTEGER, d_dow INTEGER, d_moy INTEGER, d_dom INTEGER, d_qoy INTEGER, d_fy_year INTEGER, d_fy_quarter_seq INTEGER, d_fy_week_seq INTEGER, d_day_name TEXT, d_quarter_name TEXT, d_holiday TEXT, d_weekend TEXT)",
    "CREATE TABLE item (i_item_sk INTEGER PRIMARY KEY, i_item_id TEXT, i_rec_start_date TEXT, i_rec_end_date TEXT, i_item_desc TEXT, i_current_price REAL, i_wholesale_cost REAL, i_brand_id INTEGER, i_brand TEXT, i_class_id INTEGER, i_class TEXT, i_category_id INTEGER, i_category TEXT, i_manufact_id INTEGER, i_manufact TEXT, i_size TEXT, i_formulation TEXT, i_color TEXT, i_units TEXT, i_container TEXT, i_manager_id INTEGER, i_product_name TEXT)",
    "CREATE TABLE store_sales (ss_sold_date_sk INTEGER, ss_sold_time_sk INTEGER, ss_item_sk INTEGER NOT NULL, ss_customer_sk INTEGER, ss_cdemo_sk INTEGER, ss_hdemo_sk INTEGER, ss_addr_sk INTEGER, ss_store_sk INTEGER, ss_promo_sk INTEGER, ss_ticket_number INTEGER NOT NULL, ss_quantity INTEGER, ss_wholesale_cost REAL, ss_list_price REAL, ss_sales_price REAL, ss_ext_discount_amt REAL, ss_ext_sales_price REAL, ss_ext_wholesale_cost REAL, ss_ext_list_price REAL, ss_ext_tax REAL, ss_coupon_amt REAL, ss_net_paid REAL, ss_net_paid_inc_tax REAL, ss_net_profit REAL, PRIMARY KEY (ss_item_sk, ss_ticket_number))",
    NULL
};

static const char *ds_categories[] = {"Music","Sports","Electronics","Books","Home","Shoes"};
static const char *ds_brands[]     = {"amalgamalg #1","importoimporto #2","edu packscholar #3","exportimaxi #4","scholaramalgamalg #5","brandmaxi #6"};
#define DS_N_CATEGORIES 6
#define DS_N_BRANDS     6
#define DS_N_DATES      365
#define DS_N_ITEMS      100
#define DS_N_STORE_SALES 500

static void tpcds_generate(sqlite3 *db) {
    char sql[4096];
    int i;

    exec_or_warn(db, "BEGIN");

    /* date_dim */
    for (i = 0; i < DS_N_DATES; i++) {
        int year = 2000 + i / 365;
        int doy = i % 365;
        int month = doy / 30 + 1; if (month > 12) month = 12;
        int day = doy % 30 + 1;
        int qoy = (month - 1) / 3 + 1;
        const char *daynames[] = {"Sun","Mon","Tue","Wed","Thu","Fri","Sat"};
        snprintf(sql, sizeof(sql),
            "INSERT INTO date_dim VALUES(%d,'AAAAAA%04d','%04d-%02d-%02d',"
            "%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,'%s','%04dQ%d','%s','%s')",
            i+1, i, year, month, day,
            i/30, i/7, i/90, year, i%7, month, day, qoy,
            year, (year-2000)*4+qoy, i/7,
            daynames[i%7], year, qoy,
            (i%7==0||i%7==6) ? "Y" : "N",
            (i%7==0||i%7==6) ? "Y" : "N");
        exec_or_warn(db, sql);
    }

    /* item */
    for (i = 1; i <= DS_N_ITEMS; i++) {
        snprintf(sql, sizeof(sql),
            "INSERT INTO item VALUES(%d,'ITEM%06d','2000-01-01',NULL,"
            "'Item desc %d',%.2f,%.2f,%d,'%s',%d,'class%d',%d,'%s',%d,'Mfg#%d',"
            "'N/A','N/A','color%d','Each','N/A',%d,'product%d')",
            i, i, i,
            10.0 + (i * 7) % 990, 5.0 + (i * 3) % 500,
            i % DS_N_BRANDS, ds_brands[i % DS_N_BRANDS],
            i % 12, i % 12,
            i % DS_N_CATEGORIES, ds_categories[i % DS_N_CATEGORIES],
            1 + i % 5, 1 + i % 5,
            i % 20, i % 100, i);
        exec_or_warn(db, sql);
    }

    /* store_sales */
    for (i = 1; i <= DS_N_STORE_SALES; i++) {
        int item = 1 + (i * 7) % DS_N_ITEMS;
        int cust = 1 + (i * 3) % 200;
        int date = 1 + (i * 11) % DS_N_DATES;
        double price = 10.0 + (i * 13) % 990;
        double cost = price * 0.6;
        int qty = 1 + i % 10;
        snprintf(sql, sizeof(sql),
            "INSERT INTO store_sales VALUES(%d,NULL,%d,%d,%d,NULL,NULL,%d,NULL,%d,"
            "%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,0,%.2f,%.2f,%.2f)",
            date, item, cust, 1+(cust%50), 1+(i%5), i,
            qty, cost, price, price*0.9, price*0.1,
            price*0.9*qty, cost*qty, price*qty, price*qty*0.07,
            price*0.9*qty, price*0.9*qty*1.07, price*0.9*qty - cost*qty);
        exec_or_warn(db, sql);
    }

    exec_or_warn(db, "COMMIT");
}

/* ===================================================================
 * main
 * ================================================================== */

int main(void) {
    sqlite3 *db;
    const char *engine = ENGINE_LABEL;

    printf("========================================\n");
    printf("ENGINE: %s — Correctness Spot-Check\n", engine);
    printf("========================================\n");

    /* ---- TPC-H section ---- */
    printf("\n\n==== TPC-H: Q10 and Q12 ====\n");

    if (sqlite3_open(":memory:", &db) != SQLITE_OK) {
        fprintf(stderr, "open failed\n"); return 1;
    }
    exec_or_warn(db, "PRAGMA journal_mode=WAL");
    exec_or_warn(db, TPCH_SCHEMA);
    printf("Loading TPC-H data (SCALE=1)...\n");
    tpch_generate(db);

    /* Table size sanity checks */
    run_show(db, "TPC-H row counts",
        "SELECT 'orders' AS t, COUNT(*) AS n FROM orders "
        "UNION ALL SELECT 'lineitem', COUNT(*) FROM lineitem "
        "UNION ALL SELECT 'customer', COUNT(*) FROM customer "
        "UNION ALL SELECT 'nation', COUNT(*) FROM nation", 10);

    /* Q10 — Returned Item Reporting */
    run_show(db, "Q10: Full query (all rows)",
        "SELECT c_custkey, c_name, "
        "SUM(l_extendedprice * (1 - l_discount)) AS revenue, "
        "c_acctbal, n_name, c_address, c_phone, c_comment "
        "FROM customer, orders, lineitem, nation "
        "WHERE c_custkey = o_custkey "
        "AND l_orderkey = o_orderkey "
        "AND o_orderdate >= '1993-10-01' AND o_orderdate < '1994-01-01' "
        "AND l_returnflag = 'R' "
        "AND c_nationkey = n_nationkey "
        "GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment "
        "ORDER BY revenue DESC "
        "LIMIT 20",
        20);

    /* Q10 subqueries to isolate the problem */
    run_show(db, "Q10-sub1: Orders in date window [1993-10-01, 1994-01-01)",
        "SELECT COUNT(*) AS n FROM orders "
        "WHERE o_orderdate >= '1993-10-01' AND o_orderdate < '1994-01-01'", 5);

    run_show(db, "Q10-sub2: Lineitems with returnflag=R",
        "SELECT COUNT(*) AS n FROM lineitem WHERE l_returnflag = 'R'", 5);

    run_show(db, "Q10-sub3: orders+lineitem join with date+returnflag",
        "SELECT COUNT(*) AS n "
        "FROM orders, lineitem "
        "WHERE o_orderkey = l_orderkey "
        "AND o_orderdate >= '1993-10-01' AND o_orderdate < '1994-01-01' "
        "AND l_returnflag = 'R'", 5);

    run_show(db, "Q10-sub4: full 4-table join no filters",
        "SELECT COUNT(*) AS n "
        "FROM customer, orders, lineitem, nation "
        "WHERE c_custkey = o_custkey "
        "AND l_orderkey = o_orderkey "
        "AND c_nationkey = n_nationkey", 5);

    run_show(db, "Q10-sub5: full 4-table join with all filters",
        "SELECT COUNT(*) AS n "
        "FROM customer, orders, lineitem, nation "
        "WHERE c_custkey = o_custkey "
        "AND l_orderkey = o_orderkey "
        "AND o_orderdate >= '1993-10-01' AND o_orderdate < '1994-01-01' "
        "AND l_returnflag = 'R' "
        "AND c_nationkey = n_nationkey", 5);

    /* Q12 — Shipping Modes and Order Priority */
    run_show(db, "Q12: Full query (all rows)",
        "SELECT l_shipmode, "
        "SUM(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS high_line_count, "
        "SUM(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) AS low_line_count "
        "FROM orders, lineitem "
        "WHERE o_orderkey = l_orderkey "
        "AND l_shipmode IN ('MAIL','SHIP') "
        "AND l_commitdate < l_receiptdate "
        "AND l_shipdate < l_commitdate "
        "AND l_receiptdate >= '1994-01-01' AND l_receiptdate < '1995-01-01' "
        "GROUP BY l_shipmode "
        "ORDER BY l_shipmode",
        10);

    /* Q12 subqueries */
    run_show(db, "Q12-sub1: Lineitems with shipmode MAIL or SHIP",
        "SELECT COUNT(*) AS n FROM lineitem WHERE l_shipmode IN ('MAIL','SHIP')", 5);

    run_show(db, "Q12-sub2: Plus receiptdate in range",
        "SELECT COUNT(*) AS n FROM lineitem "
        "WHERE l_shipmode IN ('MAIL','SHIP') "
        "AND l_receiptdate >= '1994-01-01' AND l_receiptdate < '1995-01-01'", 5);

    run_show(db, "Q12-sub3: Plus commitdate < receiptdate",
        "SELECT COUNT(*) AS n FROM lineitem "
        "WHERE l_shipmode IN ('MAIL','SHIP') "
        "AND l_receiptdate >= '1994-01-01' AND l_receiptdate < '1995-01-01' "
        "AND l_commitdate < l_receiptdate", 5);

    run_show(db, "Q12-sub4: All 3 date conditions (full lineitem filter)",
        "SELECT COUNT(*) AS n FROM lineitem "
        "WHERE l_shipmode IN ('MAIL','SHIP') "
        "AND l_receiptdate >= '1994-01-01' AND l_receiptdate < '1995-01-01' "
        "AND l_commitdate < l_receiptdate "
        "AND l_shipdate < l_commitdate", 5);

    run_show(db, "Q12-sub5: Cross-column date compare: commitdate < receiptdate (all lineitems)",
        "SELECT COUNT(*) AS n FROM lineitem WHERE l_commitdate < l_receiptdate", 5);

    run_show(db, "Q12-sub6: Cross-column date compare: shipdate < commitdate (all lineitems)",
        "SELECT COUNT(*) AS n FROM lineitem WHERE l_shipdate < l_commitdate", 5);

    run_show(db, "Q12-sub7: Sample dates from lineitem",
        "SELECT l_orderkey, l_linenumber, l_shipdate, l_commitdate, l_receiptdate, l_shipmode "
        "FROM lineitem LIMIT 10", 10);

    sqlite3_close(db);

    /* ---- TPC-DS section ---- */
    printf("\n\n==== TPC-DS: Q3 ====\n");

    if (sqlite3_open(":memory:", &db) != SQLITE_OK) {
        fprintf(stderr, "open failed\n"); return 1;
    }
    exec_or_warn(db, "PRAGMA journal_mode=WAL");
    for (int s = 0; TPCDS_SCHEMA[s]; s++)
        exec_or_warn(db, TPCDS_SCHEMA[s]);
    printf("Loading TPC-DS data (DS_SCALE=1)...\n");
    tpcds_generate(db);

    /* Table size sanity */
    run_show(db, "TPC-DS row counts",
        "SELECT 'date_dim' AS t, COUNT(*) AS n FROM date_dim "
        "UNION ALL SELECT 'item', COUNT(*) FROM item "
        "UNION ALL SELECT 'store_sales', COUNT(*) FROM store_sales", 10);

    /* Q3 — Sales by Brand/Year */
    run_show(db, "Q3: Full query (all rows, no LIMIT)",
        "SELECT d.d_year, i.i_brand_id, i.i_brand, SUM(ss.ss_ext_sales_price) AS sum_agg "
        "FROM date_dim d, store_sales ss, item i "
        "WHERE d.d_date_sk = ss.ss_sold_date_sk "
        "AND ss.ss_item_sk = i.i_item_sk "
        "AND i.i_manufact_id = 3 "
        "AND d.d_moy = 6 "
        "GROUP BY d.d_year, i.i_brand, i.i_brand_id "
        "ORDER BY d.d_year, sum_agg DESC, i.i_brand_id "
        "LIMIT 100",
        20);

    /* Q3 subqueries to isolate */
    run_show(db, "Q3-sub1: Items with i_manufact_id=3",
        "SELECT COUNT(*) AS n FROM item WHERE i_manufact_id = 3", 5);

    run_show(db, "Q3-sub2: Dates with d_moy=6",
        "SELECT COUNT(*) AS n FROM date_dim WHERE d_moy = 6", 5);

    run_show(db, "Q3-sub3: store_sales matching items with i_manufact_id=3",
        "SELECT COUNT(*) AS n "
        "FROM store_sales ss, item i "
        "WHERE ss.ss_item_sk = i.i_item_sk AND i.i_manufact_id = 3", 5);

    run_show(db, "Q3-sub4: store_sales matching dates with d_moy=6",
        "SELECT COUNT(*) AS n "
        "FROM store_sales ss, date_dim d "
        "WHERE d.d_date_sk = ss.ss_sold_date_sk AND d.d_moy = 6", 5);

    run_show(db, "Q3-sub5: Full 3-table join, no filters",
        "SELECT COUNT(*) AS n "
        "FROM date_dim d, store_sales ss, item i "
        "WHERE d.d_date_sk = ss.ss_sold_date_sk "
        "AND ss.ss_item_sk = i.i_item_sk", 5);

    run_show(db, "Q3-sub6: 3-table join with i_manufact_id=3 filter only",
        "SELECT COUNT(*) AS n "
        "FROM date_dim d, store_sales ss, item i "
        "WHERE d.d_date_sk = ss.ss_sold_date_sk "
        "AND ss.ss_item_sk = i.i_item_sk "
        "AND i.i_manufact_id = 3", 5);

    run_show(db, "Q3-sub7: 3-table join with d_moy=6 filter only",
        "SELECT COUNT(*) AS n "
        "FROM date_dim d, store_sales ss, item i "
        "WHERE d.d_date_sk = ss.ss_sold_date_sk "
        "AND ss.ss_item_sk = i.i_item_sk "
        "AND d.d_moy = 6", 5);

    run_show(db, "Q3-sub8: Sample i_manufact_id values in item",
        "SELECT i_item_sk, i_manufact_id, i_brand_id, i_brand "
        "FROM item WHERE i_manufact_id = 3 LIMIT 10", 10);

    run_show(db, "Q3-sub9: Sample d_moy values in date_dim",
        "SELECT d_date_sk, d_date, d_year, d_moy FROM date_dim WHERE d_moy = 6 LIMIT 10", 10);

    sqlite3_close(db);

    printf("\n========================================\n");
    printf("Done.\n");
    return 0;
}
