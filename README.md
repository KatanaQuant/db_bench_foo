# db_bench_foo

Benchmark suite for SQLite-compatible databases. Measures single-threaded
throughput, multi-table query capability, and MVCC concurrent writer scalability.

All sqlite3-compatible benchmarks use the same C API (`sqlite3_open`,
`sqlite3_prepare_v2`, `sqlite3_step`). Swap the `.so` at link time to benchmark
different engines.

**This is not production software.** Treat the C sources as the authoritative record.

---

## Targets

| Target | Library | What it is |
|--------|---------|------------|
| **SQLite** | system `libsqlite3` | Reference implementation. Single-writer WAL. |
| **FrankenSQLite** | `libfsqlite_c_api.so` | Rust SQLite reimplementation claiming MVCC concurrent writers. |
| **Limbo** | `liblimbo.so` | Turso's Rust SQLite reimplementation. Has `BEGIN CONCURRENT`. |
| **DuckDB** | `libduckdb.so` | Embedded OLAP database. Different C API — separate benchmark files. |

---

## Directory structure

```
single/                           Single-threaded benchmarks
  micro/benchmark.c               6-op micro (INSERT/SELECT/UPDATE/DELETE/batch)
  ycsb/ycsb_bench.c               YCSB A-F (Zipfian skew, mixed read/write)
  tpch/tpch_bench.c               TPC-H subset (7 analytical queries, multi-table joins)
  tpcds/tpcds_bench.c             TPC-DS subset (CTEs, window functions, INTERSECT)
  tpcc/tpcc_bench.c               TPC-C simplified (New Order, Payment, Order Status)
  oltp/oltp_read_write.c          sysbench-style OLTP (prepared stmts, latency percentiles)

concurrent/                       Multi-threaded benchmarks (MVCC scalability)
  ycsb_concurrent.c               YCSB-A at 1-32 threads — core MVCC contention test
  ycsb_concurrent_pg.c            YCSB-A against PostgreSQL at SERIALIZABLE isolation
  oltp_concurrent.c               OLTP read/write at 1-8 threads
  tpcc_concurrent.c               TPC-C (New Order + Payment) at 1-8 threads
  overlap_test.c                   Transaction overlap detector (4 threads × 100ms sleep)
  elle_write_skew_v2.c             Elle-style isolation test (conflict detection on shared data)
  elle_write_skew_pg.c             Same isolation test against PostgreSQL SERIALIZABLE
  separate_db_baseline.c           Separate-file baseline (SQLite db/thread vs shared)

duckdb/                           DuckDB-specific (uses duckdb.h, not sqlite3.h)
  include/duckdb.h
  micro_duckdb.c
  ycsb_duckdb.c
  tpch_duckdb.c

tools/                            Diagnostics and profiling drivers
investigation/                    Q10/Q12 bug investigation suite
build/                            Compiled binaries (gitignored)
results/                          Benchmark output (gitignored)
```

---

## What each benchmark tests

### Single-threaded (engine quality)

| Benchmark | What it measures | Key insight |
|-----------|-----------------|-------------|
| **micro** | Raw per-op throughput (INSERT/SELECT/UPDATE/DELETE) | Baseline engine cost. Shows autocommit fsync penalty. |
| **ycsb** | Mixed workloads with realistic key skew (Zipfian θ=0.99) | Most-cited OLTP benchmark. Hot keys reveal index efficiency. |
| **oltp** | sysbench-compatible mixed transaction (10R + 2W + 1D + 1I) | Standard web-OLTP reference point. Latency percentiles. |
| **tpch** | Analytical joins (GROUP BY, subqueries, multi-table) | SQL coverage. Exposes join planner quality. |
| **tpcds** | Advanced SQL (CTEs, window functions, INTERSECT/EXCEPT) | Feature completeness. |
| **tpcc** | OLTP with multi-table transactions (order entry) | Realistic mixed workload. Order Status has 3-table join. |

### Multi-threaded (concurrency / MVCC)

| Benchmark | What it measures | Key insight |
|-----------|-----------------|-------------|
| **ycsb_concurrent** | YCSB-A (50/50 R/W) at 1-32 threads on hot keys | The core MVCC test. Write contention on shared data. |
| **ycsb_concurrent_pg** | Same workload against PostgreSQL SERIALIZABLE | Gold standard comparison. Shows real SSI scaling + aborts. |
| **oltp_concurrent** | OLTP txns at 1-8 threads | Multi-writer throughput scaling for realistic workloads. |
| **tpcc_concurrent** | TPC-C (New Order + Payment) at 1-8 threads | Realistic concurrent OLTP. Skips Order Status (crashes). |

### Isolation & correctness

| Benchmark | What it measures | Key insight |
|-----------|-----------------|-------------|
| **overlap_test** | Can two write transactions exist at the same time? | 4 threads × 100ms sleep. Concurrent: ~100ms. Serialized: ~400ms. |
| **elle_write_skew_v2** | Does conflict detection fire on contended data? | 4 threads withdrawing from shared accounts. Aborts = detection works. |
| **elle_write_skew_pg** | Same test against PostgreSQL SERIALIZABLE | Baseline: PG produces aborts. FrankenSQLite produces zero. |
| **separate_db_baseline** | Fair comparison: SQLite with 1 db/thread vs shared | Strips MVCC from the equation. Shows raw parallel I/O parity. |

All concurrent benchmarks test two modes:
- **autocommit/WAL** — standard SQLite behavior (single-writer serialization)
- **BEGIN CONCURRENT** — MVCC path (frankensqlite/limbo claim)

The scaling curve (threads vs throughput) reveals whether MVCC provides
actual concurrent write benefit or just adds overhead.

### DuckDB (cross-engine comparison)

DuckDB is column-store OLAP. Point lookups will be slower; analytical queries
(TPC-H) will be faster. Included as a sanity check for the TPC-H numbers and
to contextualize frankensqlite's analytical query crashes.

---

## Build

```sh
# 1. Build target libraries (only the C API crate — NOT full workspace)
#
#    frankensqlite:  cd frankensqlite && cargo build --release -p fsqlite-c-api
#    limbo:          cd limbo && cargo build --release -p limbo-c
#
#    WARNING: full workspace builds use 8-12 GB RAM and will OOM.

# 2. Compile benchmarks
FSQLITE_DIR=/path/to/frankensqlite/target/release \
LIMBO_DIR=/path/to/limbo/target/release \
DUCKDB_DIR=/path/to/duckdb/lib \
./build.sh
```

Binaries go to `build/`. Missing targets are skipped.

---

## Running

### Sequential execution required

Run one benchmark at a time. CPU contention from parallel runs skews results.

```sh
# Check load
uptime   # wait for loadavg < 0.5

# Single-threaded
build/benchmark_sqlite 1000 :memory:
build/benchmark_fsqlite 1000 :memory:

build/ycsb_bench_sqlite :memory:
build/oltp_read_write_fsqlite 10000 1000 :memory:

build/tpcc_bench_sqlite /tmp/tpcc.db SQLite
build/tpcc_bench_fsqlite /tmp/tpcc.db FrankenSQLite

# Concurrent (MUST use file path, not :memory:)
build/ycsb_concurrent_sqlite /tmp/ycsb.db
build/ycsb_concurrent_fsqlite /tmp/ycsb.db

build/oltp_concurrent_sqlite /tmp/oltp.db
build/tpcc_concurrent_fsqlite /tmp/tpcc.db

# Isolation & correctness
build/overlap_fsqlite /tmp/overlap.db concurrent
build/elle_write_skew_v2_fsqlite /tmp/elle.db concurrent

# PostgreSQL comparison (requires running PG with elle_test db)
build/ycsb_concurrent_pg
build/elle_write_skew_pg

# Separate-file baseline
build/separate_db_sqlite
build/separate_db_fsqlite
```

### Known issues

- **FrankenSQLite crashes on multi-table joins** — TPC-H Q5+ and TPC-DS OOM.
  `tpcc_bench.c` uses fork() subprocess isolation to handle this gracefully.
- **BEGIN CONCURRENT not in system SQLite** — falls back to BEGIN automatically.
- **`:memory:` does not work for concurrent benchmarks** — multiple
  `sqlite3_open(":memory:")` calls create independent databases.

---

## Environment

- Linux x86_64 (tested Ubuntu 22.04)
- GCC with `-O2`
- `PRAGMA journal_mode=WAL; synchronous=NORMAL` everywhere
- `:memory:` for single-threaded (unless noted), file-backed for concurrent
- Deterministic: `srand(42)` / `rand_r(&seed)` with fixed seeds
