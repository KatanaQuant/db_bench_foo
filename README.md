# bench

Experiment infrastructure for benchmarking FrankenSQLite (also known as kqsqlite),
Turso, and system SQLite against a common suite of workloads. All programs use the
sqlite3 C API.

**This is not production software.** The binaries, `.so` libraries, and `perf_*.data`
files checked in here are build artifacts from ongoing experiments. Treat the C
sources as the authoritative record.

---

## Source files

### Core benchmarks

**`benchmark.c`** — Six-operation micro-benchmark against a single three-column table
(`id INTEGER PRIMARY KEY, name TEXT, value REAL`). Measures autocommit INSERT,
full-table SELECT, primary-key SELECT, UPDATE, DELETE, and a batched
`BEGIN`/`COMMIT` INSERT. Runs with `PRAGMA journal_mode=WAL; synchronous=NORMAL`.

```
Usage: ./benchmark_<target> [record_count] [db_path]
       ./benchmark_sqlite 1000
       ./benchmark_fsqlite 5000 :memory:
```

Output: one line per operation showing total ms and records/sec, then a total.

**`ycsb_bench.c`** — YCSB workloads A–F over a 10-field `usertable`. Uses a
scrambled Zipfian key distribution (θ=0.99) to model realistic access skew.
Workloads: A (50/50 read/update), B (95/5), C (100% read), D (latest), E (range
scan), F (read-modify-write).

```
Usage: ./ycsb_<target> [db_path]
       ./ycsb_sqlite :memory:
```

Output: one line per workload showing ops/sec and total ms.

**`tpch_bench.c`** — TPC-H at small scale (SCALE=1: ~600 lineitems). Generates the
eight TPC-H tables in-process, then runs a representative subset of queries:
Q1 (pricing summary), Q3 (shipping priority), Q4 (order priority checking),
Q5 (local supplier volume), Q6 (forecasting revenue change), Q10 (returned item
reporting), Q12 (shipping modes). 30-second timeout per query.

```
Usage: ./tpch_bench_<target> [db_path] [label]
       ./tpch_bench_sqlite :memory: SQLite
       ./tpch_bench_fsqlite :memory: FrankenSQLite
```

Output: per-query ms and row count, plus data-load timing broken down by table.

**`tpcds_bench.c`** — TPC-DS subset (13 queries). Covers CTEs with correlated
subqueries (Q1, Q6), window functions `SUM() OVER PARTITION BY` (Q98) and
`RANK()` (Q44), three-way `INTERSECT` (Q38), three-way `EXCEPT` (Q87),
multi-`EXISTS` subqueries (Q35), `UNION ALL` (Q5*), and `GROUP BY ROLLUP` (Q5R).
Schema includes: `date_dim`, `item`, `customer`, `customer_address`,
`customer_demographics`, `store`, `store_sales`, `store_returns`,
`catalog_sales`, `catalog_returns`, `web_sales`, `web_returns`.

```
Usage: ./tpcds_<target> [db_path] [label]
       ./tpcds_bench_sqlite :memory: SQLite
       ./tpcds_fsqlite_latest :memory: FrankenSQLite
```

Output: per-query ms and row count.

### Diagnostic and profiling tools

**`diagnostic.c`** — Isolates file-backed write overhead by running 10K INSERT and
10K UPDATE under three pragma configurations: WAL+NORMAL, WAL+OFF, journal=OFF.
Useful for separating fsync cost from engine cost.

```
Usage: ./diagnostic_<target> <db_path>
       ./diagnostic_fsqlite /tmp/diag.db
```

**`correctness_check.c`** — Spot-checks query result correctness. Runs TPC-H Q10,
Q12 and TPC-DS Q3 against both engines side-by-side and prints actual row values
for up to N rows. Used to confirm divergence when row counts differ between engines.

```
Usage: ./correctness_<target>      (no args; opens :memory:)
```

**`txn_batch.c`** — Compares autocommit vs `BEGIN`/`COMMIT` for INSERT, UPDATE,
and DELETE at 1000 rows. Accepts `:memory:` or a file path to reveal fsync
contribution.

```
Usage: ./txn_batch_<target> [db_path]
       ./txn_batch_fsqlite :memory:
```

**`select_all_scale.c`** — Measures `SELECT *` per-row cost at four scales
(1K, 5K, 10K, 50K rows) in `:memory:`. Reports best-of-five runs as μs/row.
Output goes to stderr.

```
Usage: ./select_all_scale_<target>     (no args)
```

**`select_by_id_profile.c`** — Primary-key lookup cost at varying scales.

**`update_profile.c`** / **`update_batch_profile.c`** — Single-row and batched
UPDATE profiling.

**`prof_bind.c`** — Prepared-statement bind loop vs inline `sqlite3_exec`.
Compares `prepare-once / bind / step / reset` against string-formatted exec for
both INSERT and SELECT paths.

```
Usage: ./prof_bind_f [count] [db_path]
       ./prof_bind_f 10000 :memory:
```

**`prof_bind_insert_only.c`** / **`prof_bind_select_only.c`** — Isolated insert
and select halves of the bind benchmark.

**`prof_insert.c`** / **`prof_select.c`** / **`prof_prepared.c`** /
**`prof_txn_batch.c`** — Single-purpose profiling drivers used for `perf record`
runs. Compile and run under `perf record -g` to collect flamegraph data.

### Q10/Q12 investigation suite

These were written to diagnose why FrankenSQLite returned 0 rows for TPC-H Q10
and Q12. Each narrows the failure surface:

| File | Purpose |
|------|---------|
| `q10_q12_diag.c` | Run Q10 and Q12 on both engines; print row counts + EXPLAIN opcodes |
| `q10_q12_deep.c` | Progressive decomposition of the failing queries; isolates the exact predicate causing 0 rows |
| `q10_q12_confirm.c` | Confirms two specific bugs: multi-table column resolution and `sqlite3_column_text()` returning NULL |
| `q10_q12_prepcheck.c` | Checks what `sqlite3_prepare_v2` returns for single-table vs multi-table queries |

All four take no arguments and open `:memory:`. They are compiled with
`-DENGINE_LABEL='"FrankenSQLite"'` or `'"SQLite"'` to tag output.

### Explain tools

**`explain_update.c`** / **`explain_q4.c`** — Print `EXPLAIN` opcode output for
specific statements. Used to compare vdbe plans between engines.

---

## Build

All targets compile to small binaries (~16–33 KB for fsqlite, ~1–5 MB for system
SQLite which statically links the amalgamation). The `.so` at `libfsqlite_c_api.so`
must be present at link time and at runtime.

### System SQLite

```sh
sudo apt-get install libsqlite3-dev
gcc -O2 benchmark.c -lsqlite3 -o benchmark_sqlite
gcc -O2 ycsb_bench.c -I./include -lsqlite3 -o ycsb_sqlite
gcc -O2 tpch_bench.c -I./include -lsqlite3 -o tpch_bench_sqlite
gcc -O2 tpcds_bench.c -I./include -lsqlite3 -o tpcds_bench_sqlite
```

If `libsqlite3-dev` is not installed but only the runtime `.so.0` is available,
place `sqlite3.h` in `./include/` (already present) and link explicitly:

```sh
gcc -O2 benchmark.c -I./include /usr/lib/x86_64-linux-gnu/libsqlite3.so.0 -o benchmark_sqlite
```

### FrankenSQLite

FrankenSQLite exposes a sqlite3-compatible C API via `libfsqlite_c_api.so`.
Set `FSQL` to the release directory:

```sh
# Point FSQL to the release directory of your FrankenSQLite/kqsqlite build:
# git clone https://github.com/Dicklesworthstone/frankensqlite && cd frankensqlite && cargo build --release -p fsqlite-c-api
FSQL=frankensqlite/target/release

gcc -O2 benchmark.c -I./include -L$FSQL -lfsqlite_c_api -Wl,-rpath,$FSQL \
    -o benchmark_fsqlite

gcc -O2 ycsb_bench.c -I./include -L$FSQL -lfsqlite_c_api -Wl,-rpath,$FSQL \
    -o ycsb_fsqlite

gcc -O2 tpch_bench.c -I./include -L$FSQL -lfsqlite_c_api -Wl,-rpath,$FSQL \
    -o tpch_bench_fsqlite

gcc -O2 tpcds_bench.c -I./include -L$FSQL -lfsqlite_c_api -Wl,-rpath,$FSQL \
    -o tpcds_fsqlite
```

### Turso (Limbo)

Turso's Rust SQLite reimplementation also exposes a sqlite3-compatible C API.
Build it from [github.com/tursodatabase/limbo](https://github.com/tursodatabase/limbo):

```sh
git clone https://github.com/tursodatabase/limbo && cd limbo && cargo build --release -p limbo-c
TURSO=limbo/target/release

gcc -O2 benchmark.c -I./include -L$TURSO -llimbo -Wl,-rpath,$TURSO \
    -o benchmark_turso

gcc -O2 ycsb_bench.c -I./include -L$TURSO -llimbo -Wl,-rpath,$TURSO \
    -o ycsb_turso
```

### Micro aliases

The `micro_*` binaries referenced in the article are `benchmark.c` compiled
against each engine. They are the same binary as `benchmark_*` — the naming
convention distinguishes them from the YCSB and TPC-H suites:

```sh
# These are equivalent:
gcc -O2 benchmark.c -lsqlite3 -o micro_sqlite
gcc -O2 benchmark.c -I./include -L$FSQL -lfsqlite_c_api -Wl,-rpath,$FSQL -o micro_fsqlite
gcc -O2 benchmark.c -I./include -L$TURSO -llimbo -Wl,-rpath,$TURSO -o micro_turso
```

The `-DENGINE_LABEL` flag is required for the Q10/Q12 suite:

```sh
gcc -O2 -DENGINE_LABEL='"FrankenSQLite"' q10_q12_confirm.c -I./include \
    -L$FSQL -lfsqlite_c_api -Wl,-rpath,$FSQL -o q10_q12_confirm_fsqlite

gcc -O2 -DENGINE_LABEL='"SQLite"' q10_q12_confirm.c -I./include -lsqlite3 \
    -o q10_q12_confirm_sqlite
```

### Correctness check

```sh
gcc -O2 -DENGINE_LABEL='"SQLite"' correctness_check.c -I./include -lsqlite3 \
    -o correctness_sqlite

gcc -O2 -DENGINE_LABEL='"FrankenSQLite"' correctness_check.c -I./include \
    -L$FSQL -lfsqlite_c_api \
    -Wl,-rpath,$FSQL -o correctness_fsqlite
```

### Automated build and run

`run.sh` auto-detects system SQLite and optional custom libraries, compiles
`benchmark.c`, and runs all available targets:

```sh
./run.sh           # 1000 rows
./run.sh 5000      # custom row count

# Custom sqlite3-compatible library:
CUSTOM_DIR=/path/to/lib CUSTOM_LIB_NAME=their_sqlite3 CUSTOM_LABEL=mydb ./run.sh
```

Results are saved to `results_*.txt`.

---

## Running benchmarks

### Sequential execution required

Run one benchmark at a time. CPU contention from parallel runs skews results
significantly. Wait for load average to drop below 0.5 before starting a run:

```sh
uptime   # check loadavg
```

### In-memory vs file-backed

Most benchmarks default to `:memory:`. File-backed runs add fsync overhead that
can dominate at small row counts — use `diagnostic.c` to quantify the split before
drawing conclusions from file-backed numbers.

### Perf profiling

The `prof_*` binaries are sized for `perf record`:

```sh
perf record -g ./prof_insert
perf record -g ./prof_select
perf report
```

Raw data files (`perf_*.data`) are listed in `.gitignore` and excluded from the
repo.

---

## Expected output

**`benchmark_*`:**
```
=== SQLite Benchmark ===
Database: :memory:
Record count: 1000

Running benchmarks...

--- Results ---
INSERT 1000 records:        12.34 ms (81037.3 rec/sec)
SELECT ALL 1000 records:    0.45 ms
SELECT BY ID 1000 times:    3.21 ms (311526.5 queries/sec)
UPDATE 1000 records:        9.87 ms (101317.1 rec/sec)
DELETE 1000 records:        11.23 ms (89047.2 rec/sec)
TRANSACTION batch 1000:     1.05 ms (952380.9 rec/sec)

TOTAL TIME: 38.15 ms
```

**`tpch_bench_*` / `tpcds_*`:**
```
  Q1  Pricing Summary Report               12.34 ms  (3 rows)
  Q3  Shipping Priority                     8.76 ms  (10 rows)
  Q4  Order Priority Checking               5.43 ms  (5 rows)
  ...
```

Absolute timings vary with hardware and system load. The ratios between
implementations are what matter. See `BENCHMARK_NOTE.md`.

---

## Environment

- OS: Linux (tested on Ubuntu 22.04, x86_64)
- Compiler: GCC with `-O2`
- System SQLite: `libsqlite3-dev` (or bundled `include/sqlite3.h` + runtime `.so`)
- FrankenSQLite/kqsqlite: `libfsqlite_c_api.so` built from the Rust source
  (see Build section above for clone and build instructions)
- Load average should be below 0.5 before running timed benchmarks
- Benchmarks use `:memory:` by default to eliminate disk I/O variance
- Pragma defaults: `journal_mode=WAL`, `synchronous=NORMAL`

---

## Files excluded from git

See `.gitignore`. Excluded: compiled binaries, `.so` libraries, `*.db` / `*.db-wal`
scratch databases, `perf_*.data` profiling captures, and single-letter scratch DB
files (`A`–`F`).
