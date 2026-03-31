# Separate-DB Baseline Results — March 30, 2026

The question: if FrankenSQLite's benchmark gives each thread its own
table (zero contention), would SQLite with separate database FILES per
thread be even faster?

1,000 rows per thread, INSERT, file-backed, WAL mode, 1 row per
transaction. Same machine, same conditions.

## Full Results

| Threads | SQLite separate files | SQLite shared DB | FrankenSQLite shared DB | FrankenSQLite separate files |
|---------|----------------------|-----------------|------------------------|----------------------------|
| 1 | 35,994 rows/s | 22,006 | 5,437 | 7,020 |
| 2 | 41,127 | 32,953 | 6,725 | 11,535 |
| 4 | 75,617 | 25,687 | 7,152 | 25,514 |
| 8 | 144,303 | 21,422 | 1,054 | 34,319 |
| 16 | 175,587 | 19,785 | 1,659 | 43,073 |
| 32 | 227,743 | 23,628 | 2,383 | 47,702 |

## The 3.7x claim context

FrankenSQLite claims 3.7x faster than SQLite at 4 threads. That
comparison is: FrankenSQLite shared DB separate tables (7,152) vs
SQLite shared DB separate tables (25,687). On our machine that's
actually 3.6x SLOWER, not faster. But the discrepancy is likely
machine/config differences vs their Criterion benchmark.

The real comparison: SQLite with separate files at 4 threads does
75,617 rows/s. FrankenSQLite on its best-case benchmark does 25,514.
SQLite is 3.0x faster with zero complexity.

## SQLite separate files vs FrankenSQLite (best case for each)

| Threads | SQLite separate files | FrankenSQLite separate files | SQLite advantage |
|---------|----------------------|-----------------------------|-----------------|
| 1 | 35,994 | 7,020 | **5.1x faster** |
| 2 | 41,127 | 11,535 | **3.6x faster** |
| 4 | 75,617 | 25,514 | **3.0x faster** |
| 8 | 144,303 | 34,319 | **4.2x faster** |
| 16 | 175,587 | 43,073 | **4.1x faster** |
| 32 | 227,743 | 47,702 | **4.8x faster** |

## Key findings

1. **SQLite with separate files scales linearly.** 36K → 228K at 32
   threads (6.3x). True zero-contention parallel I/O with no MVCC needed.

2. **SQLite with separate files is 3-5x faster than FrankenSQLite at
   every thread count.** On the exact scenario FrankenSQLite's benchmark
   was designed for.

3. **FrankenSQLite on shared DB collapses at 8+ threads.** 7,152 → 1,054
   at 8 threads (6.8x regression). This matches their own published
   results (0.67x at 8t, 0.53x at 16t).

4. **FrankenSQLite on separate files scales better** (7K → 48K) but
   still loses to SQLite separate files (36K → 228K) by 3-5x.

5. **PRIMARY KEY constraint errors.** FrankenSQLite produced PK
   constraint violations on separate tables during concurrent access.
   SQLite did not. Another correctness bug under concurrency.

## The implication

If your workload has zero contention (each writer touches different
data), you don't need MVCC. Use separate SQLite database files.
You'll get 3-5x more throughput, with zero dependencies, zero MVCC
overhead, zero risk of write skew, and the full SQLite test suite
behind you.

MVCC only justifies its complexity when writers share data and need
conflict detection. FrankenSQLite's benchmark specifically avoids that
scenario. And when you DO test shared data (our YCSB-A results),
FrankenSQLite is 2,818x slower than SQLite.
