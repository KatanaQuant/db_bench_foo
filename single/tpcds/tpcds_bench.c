/*
 * TPC-DS Benchmark (subset) for SQLite-compatible databases
 * 13 representative queries testing CTEs, window functions, INTERSECT, EXCEPT, subqueries.
 * Compile: gcc -O2 -o tpcds_bench tpcds_bench.c -I./include -lsqlite3
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "sqlite3.h"

/* ---------- timing ---------- */
static double now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

static void die(const char *msg, sqlite3 *db) {
    fprintf(stderr, "FATAL: %s — %s\n", msg, db ? sqlite3_errmsg(db) : "no db");
    if (db) sqlite3_close(db);
    exit(1);
}

static void exec(sqlite3 *db, const char *sql) {
    char *err = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "SQL exec error: %s\n  %.200s...\n", err, sql);
        sqlite3_free(err);
    }
}

/* ---------- schema ---------- */
static const char *SCHEMA[] = {
    "CREATE TABLE date_dim ("
    "  d_date_sk INTEGER PRIMARY KEY, d_date_id TEXT, d_date TEXT,"
    "  d_month_seq INTEGER, d_week_seq INTEGER, d_quarter_seq INTEGER,"
    "  d_year INTEGER, d_dow INTEGER, d_moy INTEGER, d_dom INTEGER,"
    "  d_qoy INTEGER, d_fy_year INTEGER, d_fy_quarter_seq INTEGER,"
    "  d_fy_week_seq INTEGER, d_day_name TEXT, d_quarter_name TEXT,"
    "  d_holiday TEXT, d_weekend TEXT)",

    "CREATE TABLE item ("
    "  i_item_sk INTEGER PRIMARY KEY, i_item_id TEXT, i_rec_start_date TEXT,"
    "  i_rec_end_date TEXT, i_item_desc TEXT, i_current_price REAL,"
    "  i_wholesale_cost REAL, i_brand_id INTEGER, i_brand TEXT,"
    "  i_class_id INTEGER, i_class TEXT, i_category_id INTEGER,"
    "  i_category TEXT, i_manufact_id INTEGER, i_manufact TEXT,"
    "  i_size TEXT, i_formulation TEXT, i_color TEXT, i_units TEXT,"
    "  i_container TEXT, i_manager_id INTEGER, i_product_name TEXT)",

    "CREATE TABLE customer ("
    "  c_customer_sk INTEGER PRIMARY KEY, c_customer_id TEXT,"
    "  c_current_cdemo_sk INTEGER, c_current_hdemo_sk INTEGER,"
    "  c_current_addr_sk INTEGER, c_first_shipto_date_sk INTEGER,"
    "  c_first_sales_date_sk INTEGER, c_salutation TEXT,"
    "  c_first_name TEXT, c_last_name TEXT, c_preferred_cust_flag TEXT,"
    "  c_birth_day INTEGER, c_birth_month INTEGER, c_birth_year INTEGER,"
    "  c_birth_country TEXT, c_login TEXT, c_email_address TEXT,"
    "  c_last_review_date_sk INTEGER)",

    "CREATE TABLE customer_address ("
    "  ca_address_sk INTEGER PRIMARY KEY, ca_address_id TEXT,"
    "  ca_street_number TEXT, ca_street_name TEXT, ca_street_type TEXT,"
    "  ca_suite_number TEXT, ca_city TEXT, ca_county TEXT,"
    "  ca_state TEXT, ca_zip TEXT, ca_country TEXT,"
    "  ca_gmt_offset REAL, ca_location_type TEXT)",

    "CREATE TABLE customer_demographics ("
    "  cd_demo_sk INTEGER PRIMARY KEY, cd_gender TEXT,"
    "  cd_marital_status TEXT, cd_education_status TEXT,"
    "  cd_purchase_estimate INTEGER, cd_credit_rating TEXT,"
    "  cd_dep_count INTEGER, cd_dep_employed_count INTEGER,"
    "  cd_dep_college_count INTEGER)",

    "CREATE TABLE store ("
    "  s_store_sk INTEGER PRIMARY KEY, s_store_id TEXT, s_rec_start_date TEXT,"
    "  s_rec_end_date TEXT, s_closed_date_sk INTEGER, s_store_name TEXT,"
    "  s_number_employees INTEGER, s_floor_space INTEGER,"
    "  s_hours TEXT, s_manager TEXT, s_market_id INTEGER,"
    "  s_geography_class TEXT, s_market_desc TEXT, s_market_manager TEXT,"
    "  s_division_id INTEGER, s_division_name TEXT, s_company_id INTEGER,"
    "  s_company_name TEXT, s_street_number TEXT, s_street_name TEXT,"
    "  s_street_type TEXT, s_suite_number TEXT, s_city TEXT,"
    "  s_county TEXT, s_state TEXT, s_zip TEXT, s_country TEXT,"
    "  s_gmt_offset REAL, s_tax_percentage REAL)",

    "CREATE TABLE store_sales ("
    "  ss_sold_date_sk INTEGER, ss_sold_time_sk INTEGER,"
    "  ss_item_sk INTEGER NOT NULL, ss_customer_sk INTEGER,"
    "  ss_cdemo_sk INTEGER, ss_hdemo_sk INTEGER,"
    "  ss_addr_sk INTEGER, ss_store_sk INTEGER,"
    "  ss_promo_sk INTEGER, ss_ticket_number INTEGER NOT NULL,"
    "  ss_quantity INTEGER, ss_wholesale_cost REAL,"
    "  ss_list_price REAL, ss_sales_price REAL,"
    "  ss_ext_discount_amt REAL, ss_ext_sales_price REAL,"
    "  ss_ext_wholesale_cost REAL, ss_ext_list_price REAL,"
    "  ss_ext_tax REAL, ss_coupon_amt REAL,"
    "  ss_net_paid REAL, ss_net_paid_inc_tax REAL,"
    "  ss_net_profit REAL,"
    "  PRIMARY KEY (ss_item_sk, ss_ticket_number))",

    "CREATE TABLE store_returns ("
    "  sr_returned_date_sk INTEGER, sr_return_time_sk INTEGER,"
    "  sr_item_sk INTEGER NOT NULL, sr_customer_sk INTEGER,"
    "  sr_cdemo_sk INTEGER, sr_hdemo_sk INTEGER,"
    "  sr_addr_sk INTEGER, sr_store_sk INTEGER,"
    "  sr_reason_sk INTEGER, sr_ticket_number INTEGER NOT NULL,"
    "  sr_return_quantity INTEGER, sr_return_amt REAL,"
    "  sr_return_tax REAL, sr_return_amt_inc_tax REAL,"
    "  sr_fee REAL, sr_return_ship_cost REAL,"
    "  sr_refunded_cash REAL, sr_reversed_charge REAL,"
    "  sr_store_credit REAL, sr_net_loss REAL,"
    "  PRIMARY KEY (sr_item_sk, sr_ticket_number))",

    "CREATE TABLE catalog_sales ("
    "  cs_sold_date_sk INTEGER, cs_sold_time_sk INTEGER,"
    "  cs_ship_date_sk INTEGER, cs_bill_customer_sk INTEGER,"
    "  cs_bill_cdemo_sk INTEGER, cs_bill_hdemo_sk INTEGER,"
    "  cs_bill_addr_sk INTEGER, cs_ship_customer_sk INTEGER,"
    "  cs_ship_cdemo_sk INTEGER, cs_ship_hdemo_sk INTEGER,"
    "  cs_ship_addr_sk INTEGER, cs_call_center_sk INTEGER,"
    "  cs_catalog_page_sk INTEGER, cs_ship_mode_sk INTEGER,"
    "  cs_warehouse_sk INTEGER, cs_item_sk INTEGER NOT NULL,"
    "  cs_promo_sk INTEGER, cs_order_number INTEGER NOT NULL,"
    "  cs_quantity INTEGER, cs_wholesale_cost REAL,"
    "  cs_list_price REAL, cs_sales_price REAL,"
    "  cs_ext_discount_amt REAL, cs_ext_sales_price REAL,"
    "  cs_ext_wholesale_cost REAL, cs_ext_list_price REAL,"
    "  cs_ext_tax REAL, cs_coupon_amt REAL,"
    "  cs_ext_ship_cost REAL, cs_net_paid REAL,"
    "  cs_net_paid_inc_tax REAL, cs_net_paid_inc_ship REAL,"
    "  cs_net_paid_inc_ship_tax REAL, cs_net_profit REAL,"
    "  PRIMARY KEY (cs_item_sk, cs_order_number))",

    "CREATE TABLE web_sales ("
    "  ws_sold_date_sk INTEGER, ws_sold_time_sk INTEGER,"
    "  ws_ship_date_sk INTEGER, ws_bill_customer_sk INTEGER,"
    "  ws_bill_cdemo_sk INTEGER, ws_bill_hdemo_sk INTEGER,"
    "  ws_bill_addr_sk INTEGER, ws_ship_customer_sk INTEGER,"
    "  ws_ship_cdemo_sk INTEGER, ws_ship_hdemo_sk INTEGER,"
    "  ws_ship_addr_sk INTEGER, ws_web_page_sk INTEGER,"
    "  ws_web_site_sk INTEGER, ws_ship_mode_sk INTEGER,"
    "  ws_warehouse_sk INTEGER, ws_promo_sk INTEGER,"
    "  ws_order_number INTEGER NOT NULL, ws_quantity INTEGER,"
    "  ws_wholesale_cost REAL, ws_list_price REAL,"
    "  ws_sales_price REAL, ws_ext_discount_amt REAL,"
    "  ws_ext_sales_price REAL, ws_ext_wholesale_cost REAL,"
    "  ws_ext_list_price REAL, ws_ext_tax REAL,"
    "  ws_coupon_amt REAL, ws_ext_ship_cost REAL,"
    "  ws_net_paid REAL, ws_net_paid_inc_tax REAL,"
    "  ws_net_paid_inc_ship REAL, ws_net_paid_inc_ship_tax REAL,"
    "  ws_net_profit REAL, ws_item_sk INTEGER NOT NULL,"
    "  PRIMARY KEY (ws_item_sk, ws_order_number))",

    "CREATE TABLE web_returns ("
    "  wr_returned_date_sk INTEGER, wr_return_time_sk INTEGER,"
    "  wr_item_sk INTEGER NOT NULL, wr_refunded_customer_sk INTEGER,"
    "  wr_refunded_cdemo_sk INTEGER, wr_refunded_hdemo_sk INTEGER,"
    "  wr_refunded_addr_sk INTEGER, wr_returning_customer_sk INTEGER,"
    "  wr_returning_cdemo_sk INTEGER, wr_returning_hdemo_sk INTEGER,"
    "  wr_returning_addr_sk INTEGER, wr_web_page_sk INTEGER,"
    "  wr_reason_sk INTEGER, wr_order_number INTEGER NOT NULL,"
    "  wr_return_quantity INTEGER, wr_return_amt REAL,"
    "  wr_return_tax REAL, wr_return_amt_inc_tax REAL,"
    "  wr_fee REAL, wr_return_ship_cost REAL,"
    "  wr_refunded_cash REAL, wr_reversed_charge REAL,"
    "  wr_account_credit REAL, wr_net_loss REAL,"
    "  PRIMARY KEY (wr_item_sk, wr_order_number))",

    "CREATE TABLE catalog_returns ("
    "  cr_returned_date_sk INTEGER, cr_return_time_sk INTEGER,"
    "  cr_item_sk INTEGER NOT NULL, cr_refunded_customer_sk INTEGER,"
    "  cr_refunded_cdemo_sk INTEGER, cr_refunded_hdemo_sk INTEGER,"
    "  cr_refunded_addr_sk INTEGER, cr_returning_customer_sk INTEGER,"
    "  cr_returning_cdemo_sk INTEGER, cr_returning_hdemo_sk INTEGER,"
    "  cr_returning_addr_sk INTEGER, cr_call_center_sk INTEGER,"
    "  cr_catalog_page_sk INTEGER, cr_ship_mode_sk INTEGER,"
    "  cr_warehouse_sk INTEGER, cr_reason_sk INTEGER,"
    "  cr_order_number INTEGER NOT NULL, cr_return_quantity INTEGER,"
    "  cr_return_amount REAL, cr_return_tax REAL,"
    "  cr_return_amt_inc_tax REAL, cr_fee REAL,"
    "  cr_return_ship_cost REAL, cr_refunded_cash REAL,"
    "  cr_reversed_charge REAL, cr_store_credit REAL,"
    "  cr_net_loss REAL,"
    "  PRIMARY KEY (cr_item_sk, cr_order_number))",

    "CREATE TABLE promotion ("
    "  p_promo_sk INTEGER PRIMARY KEY, p_promo_id TEXT,"
    "  p_start_date_sk INTEGER, p_end_date_sk INTEGER,"
    "  p_item_sk INTEGER, p_cost REAL, p_response_target INTEGER,"
    "  p_promo_name TEXT, p_channel_dmail TEXT, p_channel_email TEXT,"
    "  p_channel_catalog TEXT, p_channel_tv TEXT, p_channel_radio TEXT,"
    "  p_channel_press TEXT, p_channel_event TEXT, p_channel_demo TEXT,"
    "  p_channel_details TEXT, p_purpose TEXT, p_discount_active TEXT)",

    "CREATE TABLE call_center ("
    "  cc_call_center_sk INTEGER PRIMARY KEY, cc_call_center_id TEXT,"
    "  cc_rec_start_date TEXT, cc_rec_end_date TEXT, cc_closed_date_sk INTEGER,"
    "  cc_open_date_sk INTEGER, cc_name TEXT, cc_class TEXT,"
    "  cc_employees INTEGER, cc_sq_ft INTEGER, cc_hours TEXT,"
    "  cc_manager TEXT, cc_mkt_id INTEGER, cc_mkt_class TEXT,"
    "  cc_mkt_desc TEXT, cc_market_manager TEXT, cc_division INTEGER,"
    "  cc_division_name TEXT, cc_company INTEGER, cc_company_name TEXT,"
    "  cc_street_number TEXT, cc_street_name TEXT, cc_street_type TEXT,"
    "  cc_suite_number TEXT, cc_city TEXT, cc_county TEXT,"
    "  cc_state TEXT, cc_zip TEXT, cc_country TEXT,"
    "  cc_gmt_offset REAL, cc_tax_percentage REAL)",

    NULL
};

/* ---------- data generation ---------- */

static const char *states[] = {"AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
    "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO",
    "MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
    "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"};
#define N_STATES 50

static const char *categories[] = {"Music","Sports","Electronics","Books","Home","Shoes"};
#define N_CATEGORIES 6
static const char *classes[] = {"pop","rock","classical","country","jazz","blues",
    "running","football","basketball","tennis","golf","skiing"};
#define N_CLASSES 12
static const char *brands[] = {"amalgamalg #1","importoimporto #2","edu packscholar #3",
    "exportimaxi #4","scholaramalgamalg #5","brandmaxi #6"};
#define N_BRANDS 6
static const char *genders[] = {"M","F"};
static const char *marital[] = {"S","M","D","W","U"};
static const char *education[] = {"Primary","Secondary","College","2 yr Degree","4 yr Degree","Advanced Degree","Unknown"};

/* Scale factors */
#ifndef DS_SCALE
#define DS_SCALE 1
#endif
#define N_DATES       (365 * DS_SCALE)
#define N_ITEMS       (100 * DS_SCALE)
#define N_CUSTOMERS   (200 * DS_SCALE)
#define N_STORES      (5 * DS_SCALE)
#define N_ADDRESSES   (100 * DS_SCALE)
#define N_DEMOS       50
#define N_PROMOS      10
#define N_CALL_CTRS   3
#define N_STORE_SALES (500 * DS_SCALE)
#define N_STORE_RET   (100 * DS_SCALE)
#define N_CAT_SALES   (300 * DS_SCALE)
#define N_WEB_SALES   (300 * DS_SCALE)
#define N_CAT_RET     (50 * DS_SCALE)
#define N_WEB_RET     (50 * DS_SCALE)

static void generate_data(sqlite3 *db) {
    char sql[4096];
    int i;

    exec(db, "BEGIN");

    /* date_dim: 365 days starting 2000-01-01 */
    for (i = 0; i < N_DATES; i++) {
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
        exec(db, sql);
    }

    /* item */
    for (i = 1; i <= N_ITEMS; i++) {
        snprintf(sql, sizeof(sql),
            "INSERT INTO item VALUES(%d,'ITEM%06d','2000-01-01',NULL,"
            "'Item desc %d',%.2f,%.2f,%d,'%s',%d,'%s',%d,'%s',%d,'Mfg#%d',"
            "'N/A','N/A','color%d','Each','N/A',%d,'product%d')",
            i, i, i,
            10.0 + (i * 7) % 990, 5.0 + (i * 3) % 500,
            i % N_BRANDS, brands[i % N_BRANDS],
            i % N_CLASSES, classes[i % N_CLASSES],
            i % N_CATEGORIES, categories[i % N_CATEGORIES],
            1 + i % 5, 1 + i % 5,
            i % 20, i % 100, i);
        exec(db, sql);
    }

    /* customer */
    for (i = 1; i <= N_CUSTOMERS; i++) {
        snprintf(sql, sizeof(sql),
            "INSERT INTO customer VALUES(%d,'CUST%08d',%d,%d,%d,%d,%d,"
            "'Mr','First%d','Last%d','%s',%d,%d,%d,'US',NULL,'cust%d@email.com',%d)",
            i, i, 1+(i%N_DEMOS), 1+(i%10), 1+(i%N_ADDRESSES),
            1+(i%N_DATES), 1+(i%N_DATES),
            i, i, (i%3==0) ? "Y" : "N",
            1+(i%28), 1+(i%12), 1950+(i%60), i, 1+(i%N_DATES));
        exec(db, sql);
    }

    /* customer_address */
    for (i = 1; i <= N_ADDRESSES; i++) {
        snprintf(sql, sizeof(sql),
            "INSERT INTO customer_address VALUES(%d,'ADDR%08d',"
            "'%d','Street %d','Ave','Suite %d','City%d','County%d',"
            "'%s','%05d','US',-5.0,'residential')",
            i, i, 100+i, i, i%100, i%50, i%20,
            states[i % N_STATES], 10000+i);
        exec(db, sql);
    }

    /* customer_demographics */
    for (i = 1; i <= N_DEMOS; i++) {
        snprintf(sql, sizeof(sql),
            "INSERT INTO customer_demographics VALUES(%d,'%s','%s','%s',%d,'%s',%d,%d,%d)",
            i, genders[i%2], marital[i%5], education[i%7],
            500 + (i*100) % 9500,
            (i%3==0) ? "Good" : (i%3==1) ? "Low Risk" : "High Risk",
            i%5, i%4, i%3);
        exec(db, sql);
    }

    /* store */
    for (i = 1; i <= N_STORES; i++) {
        snprintf(sql, sizeof(sql),
            "INSERT INTO store VALUES(%d,'STORE%04d','2000-01-01',NULL,NULL,"
            "'Store %d',%d,%d,'8AM-10PM','Manager%d',%d,'medium','desc%d',"
            "'MktMgr%d',1,'Div1',1,'Company1','%d','Main','St','#%d',"
            "'City%d','County%d','%s','%05d','US',-5.0,0.08)",
            i, i, i, 50+i*10, 5000+i*1000, i, i%5, i, i,
            100+i, i, i%10, i%5, states[i%N_STATES], 20000+i);
        exec(db, sql);
    }

    /* store_sales */
    for (i = 1; i <= N_STORE_SALES; i++) {
        int item = 1 + (i * 7) % N_ITEMS;
        int cust = 1 + (i * 3) % N_CUSTOMERS;
        int date = 1 + (i * 11) % N_DATES;
        double price = 10.0 + (i * 13) % 990;
        double cost = price * 0.6;
        int qty = 1 + i % 10;
        snprintf(sql, sizeof(sql),
            "INSERT INTO store_sales VALUES(%d,NULL,%d,%d,%d,NULL,NULL,%d,NULL,%d,"
            "%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,0,%.2f,%.2f,%.2f)",
            date, item, cust, 1+(cust%N_DEMOS), 1+(i%N_STORES), i,
            qty, cost, price, price*0.9, price*0.1,
            price*0.9*qty, cost*qty, price*qty, price*qty*0.07,
            price*0.9*qty, price*0.9*qty*1.07, price*0.9*qty - cost*qty);
        exec(db, sql);
    }

    /* store_returns */
    for (i = 1; i <= N_STORE_RET; i++) {
        int item = 1 + (i * 7) % N_ITEMS;
        int cust = 1 + (i * 3) % N_CUSTOMERS;
        int date = 1 + (i * 13) % N_DATES;
        double amt = 10.0 + (i * 17) % 200;
        snprintf(sql, sizeof(sql),
            "INSERT INTO store_returns VALUES(%d,NULL,%d,%d,NULL,NULL,NULL,%d,NULL,%d,"
            "%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f)",
            date, item, cust, 1+(i%N_STORES), i,
            1+i%5, amt, amt*0.07, amt*1.07, amt*0.1,
            amt*0.05, amt*0.8, amt*0.1, amt*0.1, amt*0.2);
        exec(db, sql);
    }

    /* catalog_sales */
    for (i = 1; i <= N_CAT_SALES; i++) {
        int item = 1 + (i * 11) % N_ITEMS;
        int cust = 1 + (i * 5) % N_CUSTOMERS;
        int date = 1 + (i * 7) % N_DATES;
        double price = 15.0 + (i * 19) % 985;
        double cost = price * 0.55;
        int qty = 1 + i % 8;
        snprintf(sql, sizeof(sql),
            "INSERT INTO catalog_sales VALUES(%d,NULL,%d,%d,NULL,NULL,NULL,%d,NULL,NULL,"
            "NULL,NULL,NULL,NULL,NULL,%d,NULL,%d,"
            "%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,0,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f)",
            date, date+5, cust, cust, item, i,
            qty, cost, price, price*0.85, price*0.15,
            price*0.85*qty, cost*qty, price*qty, price*qty*0.07,
            price*0.1*qty, price*0.85*qty, price*0.85*qty*1.07,
            price*0.85*qty + price*0.1*qty,
            price*0.85*qty*1.07 + price*0.1*qty,
            price*0.85*qty - cost*qty);
        exec(db, sql);
    }

    /* web_sales */
    for (i = 1; i <= N_WEB_SALES; i++) {
        int item = 1 + (i * 13) % N_ITEMS;
        int cust = 1 + (i * 7) % N_CUSTOMERS;
        int date = 1 + (i * 3) % N_DATES;
        double price = 12.0 + (i * 23) % 988;
        double cost = price * 0.5;
        int qty = 1 + i % 6;
        snprintf(sql, sizeof(sql),
            "INSERT INTO web_sales VALUES(%d,NULL,%d,%d,NULL,NULL,NULL,%d,NULL,NULL,"
            "NULL,NULL,NULL,NULL,NULL,NULL,%d,%d,"
            "%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,0,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%d)",
            date, date+3, cust, cust, i, qty,
            cost, price, price*0.88, price*0.12,
            price*0.88*qty, cost*qty, price*qty, price*qty*0.06,
            price*0.08*qty, price*0.88*qty, price*0.88*qty*1.06,
            price*0.88*qty + price*0.08*qty,
            price*0.88*qty*1.06 + price*0.08*qty,
            price*0.88*qty - cost*qty, item);
        exec(db, sql);
    }

    /* catalog_returns */
    for (i = 1; i <= N_CAT_RET; i++) {
        int item = 1 + (i * 11) % N_ITEMS;
        int cust = 1 + (i * 5) % N_CUSTOMERS;
        int date = 1 + (i * 17) % N_DATES;
        double amt = 12.0 + (i * 13) % 200;
        snprintf(sql, sizeof(sql),
            "INSERT INTO catalog_returns VALUES(%d,NULL,%d,%d,NULL,NULL,NULL,%d,NULL,NULL,"
            "NULL,NULL,NULL,NULL,NULL,NULL,%d,"
            "%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f)",
            date, item, cust, cust, i,
            1+i%4, amt, amt*0.06, amt*1.06, amt*0.1,
            amt*0.05, amt*0.75, amt*0.1, amt*0.1, amt*0.25);
        exec(db, sql);
    }

    /* web_returns */
    for (i = 1; i <= N_WEB_RET; i++) {
        int item = 1 + (i * 13) % N_ITEMS;
        int cust = 1 + (i * 7) % N_CUSTOMERS;
        int date = 1 + (i * 19) % N_DATES;
        double amt = 8.0 + (i * 11) % 200;
        snprintf(sql, sizeof(sql),
            "INSERT INTO web_returns VALUES(%d,NULL,%d,%d,NULL,NULL,NULL,%d,NULL,NULL,"
            "NULL,NULL,NULL,%d,"
            "%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f)",
            date, item, cust, cust, i,
            1+i%3, amt, amt*0.05, amt*1.05, amt*0.1,
            amt*0.04, amt*0.78, amt*0.08, amt*0.14, amt*0.22);
        exec(db, sql);
    }

    /* promotion */
    for (i = 1; i <= N_PROMOS; i++) {
        snprintf(sql, sizeof(sql),
            "INSERT INTO promotion VALUES(%d,'PROMO%04d',%d,%d,%d,%.2f,%d,"
            "'Promo %d','Y','N','Y','N','N','N','N','N','Details %d','Unknown','Y')",
            i, i, 1+(i*30)%N_DATES, 1+(i*30+30)%N_DATES, 1+(i%N_ITEMS),
            100.0+i*10, 100, i, i);
        exec(db, sql);
    }

    /* call_center */
    for (i = 1; i <= N_CALL_CTRS; i++) {
        snprintf(sql, sizeof(sql),
            "INSERT INTO call_center VALUES(%d,'CC%04d','2000-01-01',NULL,NULL,%d,"
            "'Call Center %d','medium',%d,%d,'8AM-4PM','CCMgr%d',%d,'class%d',"
            "'desc%d','MktMgr%d',%d,'Div%d',%d,'Company%d',"
            "'%d','Center','Ave','#%d','City%d','County%d','%s','%05d','US',-5.0,0.06)",
            i, i, 1+(i*100)%N_DATES, i, 20+i*5, 1000+i*200, i,
            i%3, i, i, i, i, i, i, i, 200+i, i, i%5, i%3,
            states[i%N_STATES], 30000+i);
        exec(db, sql);
    }

    exec(db, "COMMIT");
}

/* ---------- TPC-DS queries ---------- */

typedef struct {
    const char *id;
    const char *name;
    const char *feature;
    const char *sql;
} tpcds_query;

static const tpcds_query queries[] = {
    /* Q3 — Baseline: simple 3-table join, GROUP BY, ORDER BY */
    {"Q3", "Sales by Brand/Year", "baseline join",
     "SELECT d.d_year, i.i_brand_id, i.i_brand, SUM(ss.ss_ext_sales_price) AS sum_agg "
     "FROM date_dim d, store_sales ss, item i "
     "WHERE d.d_date_sk = ss.ss_sold_date_sk "
     "AND ss.ss_item_sk = i.i_item_sk "
     "AND i.i_manufact_id = 3 "
     "AND d.d_moy = 6 "
     "GROUP BY d.d_year, i.i_brand, i.i_brand_id "
     "ORDER BY d.d_year, sum_agg DESC, i.i_brand_id "
     "LIMIT 100"},

    /* Q7 — 5-table join with demographics */
    {"Q7", "Promo Sales Analysis", "5-table join",
     "SELECT i.i_item_id, "
     "AVG(ss.ss_quantity) AS agg1, "
     "AVG(ss.ss_list_price) AS agg2, "
     "AVG(ss.ss_coupon_amt) AS agg3, "
     "AVG(ss.ss_sales_price) AS agg4 "
     "FROM store_sales ss, customer_demographics cd, date_dim d, item i, promotion p "
     "WHERE ss.ss_sold_date_sk = d.d_date_sk "
     "AND ss.ss_item_sk = i.i_item_sk "
     "AND ss.ss_cdemo_sk = cd.cd_demo_sk "
     "AND ss.ss_promo_sk = p.p_promo_sk "
     "AND cd.cd_gender = 'M' "
     "AND cd.cd_marital_status = 'S' "
     "AND cd.cd_education_status = 'College' "
     "AND d.d_year = 2000 "
     "AND p.p_channel_email = 'Y' "
     "GROUP BY i.i_item_id "
     "ORDER BY i.i_item_id "
     "LIMIT 100"},

    /* Q1 — CTE with correlated subquery */
    {"Q1", "Store Returns Excess", "CTE + correlated subquery",
     "WITH customer_total_return AS ("
     "  SELECT sr.sr_customer_sk AS ctr_customer_sk, sr.sr_store_sk AS ctr_store_sk, "
     "  SUM(sr.sr_return_amt) AS ctr_total_return "
     "  FROM store_returns sr, date_dim d "
     "  WHERE sr.sr_returned_date_sk = d.d_date_sk AND d.d_year = 2000 "
     "  GROUP BY sr.sr_customer_sk, sr.sr_store_sk"
     ") "
     "SELECT c.c_customer_id "
     "FROM customer_total_return ctr1, store s, customer c "
     "WHERE ctr1.ctr_total_return > ("
     "  SELECT AVG(ctr2.ctr_total_return) * 1.2 "
     "  FROM customer_total_return ctr2 "
     "  WHERE ctr2.ctr_store_sk = ctr1.ctr_store_sk"
     ") "
     "AND s.s_store_sk = ctr1.ctr_store_sk "
     "AND s.s_state = 'TN' "
     "AND ctr1.ctr_customer_sk = c.c_customer_sk "
     "ORDER BY c.c_customer_id "
     "LIMIT 100"},

    /* Q6 — Correlated scalar subquery */
    {"Q6", "Above-Avg Price Items", "correlated subquery",
     "SELECT ca.ca_state AS state, COUNT(*) AS cnt "
     "FROM customer_address ca, customer c, store_sales ss, date_dim d, item i "
     "WHERE ca.ca_address_sk = c.c_current_addr_sk "
     "AND c.c_customer_sk = ss.ss_customer_sk "
     "AND ss.ss_sold_date_sk = d.d_date_sk "
     "AND ss.ss_item_sk = i.i_item_sk "
     "AND d.d_month_seq BETWEEN 1 AND 12 "
     "AND i.i_current_price > 1.2 * ("
     "  SELECT AVG(j.i_current_price) FROM item j "
     "  WHERE j.i_category = i.i_category"
     ") "
     "GROUP BY ca.ca_state "
     "HAVING COUNT(*) >= 10 "
     "ORDER BY cnt "
     "LIMIT 100"},

    /* Q98 — Window function: SUM() OVER (PARTITION BY) */
    {"Q98", "Item Revenue Share", "window function",
     "SELECT i.i_item_id, i.i_item_desc, i.i_category, i.i_class, "
     "ss.ss_ext_sales_price, "
     "SUM(ss.ss_ext_sales_price) OVER (PARTITION BY i.i_class) AS class_total "
     "FROM store_sales ss, item i, date_dim d "
     "WHERE ss.ss_item_sk = i.i_item_sk "
     "AND ss.ss_sold_date_sk = d.d_date_sk "
     "AND d.d_moy = 6 AND d.d_year = 2000 "
     "ORDER BY i.i_category, i.i_class, i.i_item_id "
     "LIMIT 100"},

    /* Q44 — RANK() in subqueries */
    {"Q44", "Best/Worst Items by Profit", "RANK window function",
     "SELECT best.i_product_name AS best_item, worst.i_product_name AS worst_item "
     "FROM ("
     "  SELECT i.i_product_name, RANK() OVER (ORDER BY AVG(ss.ss_net_profit)) AS rnk "
     "  FROM store_sales ss, item i "
     "  WHERE ss.ss_item_sk = i.i_item_sk "
     "  AND ss.ss_store_sk = 1 "
     "  GROUP BY i.i_product_name"
     ") AS best, ("
     "  SELECT i.i_product_name, RANK() OVER (ORDER BY AVG(ss.ss_net_profit) DESC) AS rnk "
     "  FROM store_sales ss, item i "
     "  WHERE ss.ss_item_sk = i.i_item_sk "
     "  AND ss.ss_store_sk = 1 "
     "  GROUP BY i.i_product_name"
     ") AS worst "
     "WHERE best.rnk = worst.rnk AND best.rnk <= 10 "
     "ORDER BY best.rnk "
     "LIMIT 10"},

    /* Q38 — Three-way INTERSECT */
    {"Q38", "Cross-Channel Buyers", "INTERSECT",
     "SELECT COUNT(*) FROM ("
     "  SELECT DISTINCT c.c_last_name, c.c_first_name, d.d_date "
     "  FROM store_sales ss, date_dim d, customer c "
     "  WHERE ss.ss_sold_date_sk = d.d_date_sk "
     "  AND ss.ss_customer_sk = c.c_customer_sk "
     "  AND d.d_year BETWEEN 2000 AND 2000 "
     "INTERSECT "
     "  SELECT DISTINCT c.c_last_name, c.c_first_name, d.d_date "
     "  FROM catalog_sales cs, date_dim d, customer c "
     "  WHERE cs.cs_sold_date_sk = d.d_date_sk "
     "  AND cs.cs_bill_customer_sk = c.c_customer_sk "
     "  AND d.d_year BETWEEN 2000 AND 2000 "
     "INTERSECT "
     "  SELECT DISTINCT c.c_last_name, c.c_first_name, d.d_date "
     "  FROM web_sales ws, date_dim d, customer c "
     "  WHERE ws.ws_sold_date_sk = d.d_date_sk "
     "  AND ws.ws_bill_customer_sk = c.c_customer_sk "
     "  AND d.d_year BETWEEN 2000 AND 2000"
     ") cross_channel"},

    /* Q87 — Three-way EXCEPT */
    {"Q87", "Store-Only Buyers", "EXCEPT",
     "SELECT COUNT(*) FROM ("
     "  SELECT DISTINCT c.c_last_name, c.c_first_name, d.d_date "
     "  FROM store_sales ss, date_dim d, customer c "
     "  WHERE ss.ss_sold_date_sk = d.d_date_sk "
     "  AND ss.ss_customer_sk = c.c_customer_sk "
     "  AND d.d_year BETWEEN 2000 AND 2000 "
     "EXCEPT "
     "  SELECT DISTINCT c.c_last_name, c.c_first_name, d.d_date "
     "  FROM catalog_sales cs, date_dim d, customer c "
     "  WHERE cs.cs_sold_date_sk = d.d_date_sk "
     "  AND cs.cs_bill_customer_sk = c.c_customer_sk "
     "  AND d.d_year BETWEEN 2000 AND 2000 "
     "EXCEPT "
     "  SELECT DISTINCT c.c_last_name, c.c_first_name, d.d_date "
     "  FROM web_sales ws, date_dim d, customer c "
     "  WHERE ws.ws_sold_date_sk = d.d_date_sk "
     "  AND ws.ws_bill_customer_sk = c.c_customer_sk "
     "  AND d.d_year BETWEEN 2000 AND 2000"
     ") store_only"},

    /* Q35 — Multi-EXISTS subqueries */
    {"Q35", "Multi-Channel Presence", "EXISTS subqueries",
     "SELECT ca.ca_state, cd.cd_gender, cd.cd_marital_status, "
     "cd.cd_dep_count, COUNT(*) AS cnt "
     "FROM customer c, customer_address ca, customer_demographics cd "
     "WHERE c.c_current_addr_sk = ca.ca_address_sk "
     "AND cd.cd_demo_sk = c.c_current_cdemo_sk "
     "AND EXISTS ("
     "  SELECT 1 FROM store_sales ss, date_dim d "
     "  WHERE c.c_customer_sk = ss.ss_customer_sk "
     "  AND ss.ss_sold_date_sk = d.d_date_sk AND d.d_year = 2000"
     ") "
     "AND ("
     "  EXISTS ("
     "    SELECT 1 FROM web_sales ws, date_dim d "
     "    WHERE c.c_customer_sk = ws.ws_bill_customer_sk "
     "    AND ws.ws_sold_date_sk = d.d_date_sk AND d.d_year = 2000"
     "  ) OR EXISTS ("
     "    SELECT 1 FROM catalog_sales cs, date_dim d "
     "    WHERE c.c_customer_sk = cs.cs_bill_customer_sk "
     "    AND cs.cs_sold_date_sk = d.d_date_sk AND d.d_year = 2000"
     "  )"
     ") "
     "GROUP BY ca.ca_state, cd.cd_gender, cd.cd_marital_status, cd.cd_dep_count "
     "ORDER BY ca.ca_state, cd.cd_gender, cd.cd_marital_status, cd.cd_dep_count "
     "LIMIT 100"},

    /* Q5-lite — UNION ALL (ROLLUP removed for SQLite compat) */
    {"Q5*", "Cross-Channel Revenue", "UNION ALL (no ROLLUP)",
     "SELECT 'store' AS channel, s.s_store_id AS id, SUM(ss.ss_ext_sales_price) AS sales, "
     "SUM(COALESCE(sr.sr_return_amt, 0)) AS returns, "
     "SUM(ss.ss_net_profit) AS profit "
     "FROM store_sales ss "
     "LEFT JOIN store_returns sr ON ss.ss_item_sk = sr.sr_item_sk AND ss.ss_ticket_number = sr.sr_ticket_number "
     "JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk "
     "JOIN store s ON ss.ss_store_sk = s.s_store_sk "
     "WHERE d.d_year = 2000 AND d.d_moy = 6 "
     "GROUP BY s.s_store_id "
     "UNION ALL "
     "SELECT 'catalog' AS channel, 'catalog_channel' AS id, SUM(cs.cs_ext_sales_price) AS sales, "
     "SUM(COALESCE(cr.cr_return_amount, 0)) AS returns, "
     "SUM(cs.cs_net_profit) AS profit "
     "FROM catalog_sales cs "
     "LEFT JOIN catalog_returns cr ON cs.cs_item_sk = cr.cr_item_sk AND cs.cs_order_number = cr.cr_order_number "
     "JOIN date_dim d ON cs.cs_sold_date_sk = d.d_date_sk "
     "WHERE d.d_year = 2000 AND d.d_moy = 6 "
     "GROUP BY channel "
     "UNION ALL "
     "SELECT 'web' AS channel, 'web_channel' AS id, SUM(ws.ws_ext_sales_price) AS sales, "
     "SUM(COALESCE(wr.wr_return_amt, 0)) AS returns, "
     "SUM(ws.ws_net_profit) AS profit "
     "FROM web_sales ws "
     "LEFT JOIN web_returns wr ON ws.ws_item_sk = wr.wr_item_sk AND ws.ws_order_number = wr.wr_order_number "
     "JOIN date_dim d ON ws.ws_sold_date_sk = d.d_date_sk "
     "WHERE d.d_year = 2000 AND d.d_moy = 6 "
     "GROUP BY channel "
     "ORDER BY channel"},

    /* ROLLUP test — expected to fail on SQLite (unsupported) */
    {"Q5R", "ROLLUP Test", "GROUP BY ROLLUP",
     "SELECT 'store' AS channel, SUM(1) AS cnt "
     "FROM store_sales "
     "GROUP BY ROLLUP(channel)"},

    /* Subquery-in-FROM — tests derived table support */
    {"QSF", "Subquery in FROM", "derived table",
     "SELECT sq.yr, SUM(sq.sales) FROM ("
     "  SELECT d.d_year AS yr, ss.ss_ext_sales_price AS sales "
     "  FROM store_sales ss, date_dim d "
     "  WHERE ss.ss_sold_date_sk = d.d_date_sk"
     ") sq "
     "GROUP BY sq.yr ORDER BY sq.yr"},
};

#define N_QUERIES (sizeof(queries) / sizeof(queries[0]))

/* ---------- run ---------- */
static void run_query(sqlite3 *db, const tpcds_query *q) {
    sqlite3_stmt *stmt;
    int rc, rows = 0;
    double t0, t1;

    printf("  %-4s %-30s %-25s ", q->id, q->name, q->feature);
    fflush(stdout);

    rc = sqlite3_prepare_v2(db, q->sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        printf("ERROR (prepare): %s\n", sqlite3_errmsg(db));
        return;
    }

    t0 = now_ms();
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        rows++;
        if (now_ms() - t0 > 30000) {
            t1 = now_ms();
            sqlite3_finalize(stmt);
            printf("TIMEOUT after %.0f ms (%d rows)\n", t1 - t0, rows);
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

    printf("=== TPC-DS Benchmark: %s ===\n", label);
    printf("Database: %s\n", dbpath);
    printf("Scale: %d\n\n", DS_SCALE);

    if (sqlite3_open(dbpath, &db) != SQLITE_OK)
        die("open", db);

    exec(db, "PRAGMA journal_mode=WAL");
    exec(db, "PRAGMA synchronous=NORMAL");

    /* schema */
    printf("Creating schema...\n");
    fflush(stdout);
    t0 = now_ms();
    for (int i = 0; SCHEMA[i]; i++)
        exec(db, SCHEMA[i]);
    t1 = now_ms();
    printf("  Schema: %.2f ms (%d tables)\n", t1 - t0, 14);

    /* data */
    printf("Generating data...\n");
    fflush(stdout);
    t0 = now_ms();
    generate_data(db);
    t1 = now_ms();
    printf("  Data load: %.2f ms\n\n", t1 - t0);

    /* queries */
    printf("--- TPC-DS Queries ---\n");
    for (int i = 0; i < (int)N_QUERIES; i++) {
        run_query(db, &queries[i]);
    }

    sqlite3_close(db);
    printf("\nDone.\n");
    return 0;
}
