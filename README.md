# sqlite-bench

A minimal C benchmark for measuring SQLite performance across six common operations. Works against any library that exposes the [sqlite3 C API](https://sqlite.org/cintro.html), plus [Turso/libsql](https://github.com/tursodatabase/libsql) via its native API.

## What it measures

| Operation | What it does |
|-----------|-------------|
| INSERT | Individual inserts (one per statement, autocommit) |
| SELECT ALL | Full table scan, touching every column |
| SELECT BY ID | Primary key lookup (`WHERE id = N`), one query per row |
| UPDATE | Update by primary key, one statement per row |
| DELETE | Delete by primary key, one statement per row |
| TRANSACTION batch | Batch insert wrapped in `BEGIN`/`COMMIT` (single fsync) |

The table schema is `(id INTEGER PRIMARY KEY, name TEXT, value REAL)`.

PRAGMA settings: `journal_mode=WAL`, `synchronous=NORMAL`.

## Quick start

```sh
./run.sh          # build and run all available targets, 1000 rows
./run.sh 5000     # custom row count
```

The run script auto-detects what's available and benchmarks everything it finds. Results are saved to `results_*.txt`.

## Files

| File | Target | API |
|------|--------|-----|
| `benchmark.c` | System SQLite / any sqlite3-compatible library | `sqlite3_*` |
| `benchmark_turso.c` | Turso/libsql | `libsql_*` |
| `run.sh` | Builds and runs all targets | — |

## Prerequisites

- GCC (or any C compiler)
- Rust toolchain (for building alternative implementations from source)
- System SQLite: `sudo apt-get install libsqlite3-dev` (headers + lib)

If `libsqlite3-dev` is not available and you only have the runtime library (`libsqlite3.so.0`), place a copy of [`sqlite3.h`](https://sqlite.org/download.html) in `./include/` and the script will find it.

## Building targets from source

### 1. System SQLite

```sh
# Debian/Ubuntu
sudo apt-get install libsqlite3-dev

# Compile and run
gcc -O2 benchmark.c -lsqlite3 -o benchmark_sqlite
./benchmark_sqlite 1000
```

### 2. Turso/libsql

[Turso/libsql](https://github.com/tursodatabase/libsql) is an open-source fork of SQLite with extensions for replication and edge hosting. It exposes its own `libsql_*` C API.

```sh
# Clone and build
git clone https://github.com/tursodatabase/libsql
cd libsql
cargo build --release
cd ..

# Compile the Turso-specific benchmark
gcc -O2 benchmark_turso.c \
    -Ilibsql/bindings/c/include \
    -Llibsql/target/release \
    -lsql_experimental -lpthread -ldl -lm \
    -o benchmark_turso

# Run
LD_LIBRARY_PATH=libsql/target/release ./benchmark_turso 1000
```

### 3. FrankenSQLite

[FrankenSQLite](https://github.com/Dicklesworthstone/frankensqlite) is a ground-up Rust reimplementation of SQLite. It exposes a sqlite3-compatible C API (`libfsqlite_c_api.so`), so the standard `benchmark.c` works against it directly.

```sh
# Clone and build
git clone https://github.com/Dicklesworthstone/frankensqlite
cd frankensqlite
cargo build --release
cd ..

# Compile benchmark against FrankenSQLite's C API
gcc -O2 benchmark.c \
    -I./include \
    -Lfrankensqlite/target/release \
    -lfsqlite_c_api \
    -o benchmark_franken

# Run
LD_LIBRARY_PATH=frankensqlite/target/release ./benchmark_franken 1000
```

### 4. Any other sqlite3-compatible library

Any library that exports the standard `sqlite3_*` symbols works with `benchmark.c`:

```sh
gcc -O2 benchmark.c \
    -I/path/to/their/include \
    -L/path/to/their/lib \
    -ltheir_sqlite3 \
    -o benchmark_other

LD_LIBRARY_PATH=/path/to/their/lib ./benchmark_other 1000
```

Or using the run script:

```sh
CUSTOM_DIR=/path/to/their/lib \
CUSTOM_LIB_NAME=their_sqlite3 \
CUSTOM_LABEL=mydb \
./run.sh 1000
```

## Comparing results

The run script saves each target's output to `results_*.txt`. Diff them:

```sh
diff results_sqlite.txt results_turso.txt
```

Or side by side:

```sh
paste results_sqlite.txt results_turso.txt | column -t
```

## Scaling tests

SELECT BY ID is the most revealing at different scales. Its cost depends on whether the implementation uses a B-tree seek or a full table scan:

```sh
for n in 100 500 1000 5000; do
    echo "=== n=$n ==="
    ./benchmark_sqlite $n
done
```

If SELECT BY ID time grows linearly with row count, the implementation uses `SeekRowid` (O(log n) per query, O(n log n) total). If it grows quadratically, every query scans the full table (O(n) per query, O(n^2) total).

## Design notes

- Each operation runs unbatched (one statement at a time) except TRANSACTION batch, which wraps all inserts in a single transaction. This is deliberate: unbatched operations expose per-statement overhead (autocommit, fsync, schema validation).
- The benchmark uses `sqlite3_exec()` with string-formatted SQL rather than `sqlite3_bind_*()`. This keeps the code simple and also exercises the parser on every call, which is part of what a real workload does.
- Results are wall-clock time via `gettimeofday()`. Run multiple times and take the median for stable numbers.
- The database file is created fresh (any existing file is removed) and cleaned up after the run.
- Absolute timings vary with system load and hardware. The ratios between implementations are what matter.

## License

Public domain. Use however you want.
