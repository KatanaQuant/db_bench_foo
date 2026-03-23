#!/usr/bin/env bash
set -uo pipefail

# run_all.sh — run all benchmarks sequentially with OOM protection
#
# Each benchmark runs inside a subshell with:
#   - ulimit -v 4GB (virtual memory cap — prevents OOM kill)
#   - timeout 120s (wall clock cap — prevents infinite hangs)
#
# Results are tee'd to results/<name>.txt
#
# Usage:
#   ./run_all.sh                    # run everything
#   ./run_all.sh single             # single-threaded only
#   ./run_all.sh concurrent         # concurrent only

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BUILD="$SCRIPT_DIR/build"
RESULTS="$SCRIPT_DIR/results"
mkdir -p "$RESULTS"

MODE="${1:-all}"
TIMESTAMP=$(date +%Y%m%dT%H%M%S)

# 4 GB virtual memory limit (in KB) — prevents OOM kills
MEMLIMIT_KB=$((4 * 1024 * 1024))

# Wall clock timeout per benchmark (seconds)
TIMEOUT=120
# Longer timeout for concurrent benchmarks (threads + retries)
TIMEOUT_CONC=300

run_bench() {
    local bin="$1"
    shift
    local name
    name="$(basename "$bin")"
    local outfile="$RESULTS/${name}_${TIMESTAMP}.txt"

    if [ ! -x "$bin" ]; then
        echo "  SKIP  $name (not built)"
        return
    fi

    local tlimit="$TIMEOUT"
    if [[ "$name" == *concurrent* ]] || [[ "$name" == *conc* ]]; then
        tlimit="$TIMEOUT_CONC"
    fi

    echo -n "  RUN   $name ... "

    # Run in subshell with memory cap
    local rc=0
    (
        ulimit -v $MEMLIMIT_KB 2>/dev/null || true
        timeout "$tlimit" "$bin" "$@"
    ) > "$outfile" 2>&1 || rc=$?

    if [ $rc -eq 0 ]; then
        echo "OK ($(wc -l < "$outfile") lines)"
    elif [ $rc -eq 124 ]; then
        echo "TIMEOUT (${tlimit}s)"
    elif [ $rc -eq 137 ]; then
        echo "OOM/KILLED (hit ${MEMLIMIT_KB}KB cap)"
    else
        echo "FAIL (exit $rc)"
    fi
}

echo "=== db_bench_foo full run ($TIMESTAMP) ==="
echo "Memory cap: $((MEMLIMIT_KB / 1024)) MB per benchmark"
echo "Timeout: ${TIMEOUT}s single, ${TIMEOUT_CONC}s concurrent"
echo ""

# ─── Single-threaded ─────────────────────────────────────────────────────

if [ "$MODE" = "all" ] || [ "$MODE" = "single" ]; then
    echo "--- Micro benchmark (1000 rows, :memory:) ---"
    run_bench "$BUILD/benchmark_sqlite"  1000 :memory:
    run_bench "$BUILD/benchmark_fsqlite" 1000 :memory:
    run_bench "$BUILD/benchmark_limbo"   1000 :memory:
    echo ""

    echo "--- YCSB A-F (:memory:) ---"
    run_bench "$BUILD/ycsb_bench_sqlite"  :memory:
    run_bench "$BUILD/ycsb_bench_fsqlite" :memory:
    run_bench "$BUILD/ycsb_bench_limbo"   :memory:
    echo ""

    echo "--- OLTP read/write (10000 rows, 1000 txns, :memory:) ---"
    run_bench "$BUILD/oltp_read_write_sqlite"  10000 1000 :memory:
    run_bench "$BUILD/oltp_read_write_fsqlite" 10000 1000 :memory:
    run_bench "$BUILD/oltp_read_write_limbo"   10000 1000 :memory:
    echo ""

    echo "--- TPC-H (:memory:) ---"
    run_bench "$BUILD/tpch_bench_sqlite"  :memory: SQLite
    run_bench "$BUILD/tpch_bench_fsqlite" :memory: FrankenSQLite
    run_bench "$BUILD/tpch_bench_limbo"   :memory: Limbo
    echo ""

    echo "--- TPC-DS (:memory:) ---"
    run_bench "$BUILD/tpcds_bench_sqlite"  :memory: SQLite
    run_bench "$BUILD/tpcds_bench_fsqlite" :memory: FrankenSQLite
    run_bench "$BUILD/tpcds_bench_limbo"   :memory: Limbo
    echo ""

    echo "--- TPC-C (file-backed, fork-isolated) ---"
    rm -f /tmp/tpcc_run_*.db /tmp/tpcc_run_*.db-wal 2>/dev/null
    run_bench "$BUILD/tpcc_bench_sqlite"  /tmp/tpcc_run_sq.db  SQLite
    run_bench "$BUILD/tpcc_bench_fsqlite" /tmp/tpcc_run_fs.db  FrankenSQLite
    run_bench "$BUILD/tpcc_bench_limbo"   /tmp/tpcc_run_li.db  Limbo
    echo ""
fi

# ─── Concurrent ──────────────────────────────────────────────────────────

if [ "$MODE" = "all" ] || [ "$MODE" = "concurrent" ]; then
    echo "--- YCSB-A concurrent (file-backed) ---"
    rm -f /tmp/ycsb_conc_*.db /tmp/ycsb_conc_*.db-wal 2>/dev/null
    run_bench "$BUILD/ycsb_concurrent_sqlite"  /tmp/ycsb_conc_sq.db
    run_bench "$BUILD/ycsb_concurrent_fsqlite" /tmp/ycsb_conc_fs.db
    run_bench "$BUILD/ycsb_concurrent_limbo"   /tmp/ycsb_conc_li.db
    echo ""

    echo "--- OLTP concurrent (file-backed) ---"
    rm -f /tmp/oltp_conc_*.db /tmp/oltp_conc_*.db-wal 2>/dev/null
    run_bench "$BUILD/oltp_concurrent_sqlite"  /tmp/oltp_conc_sq.db
    run_bench "$BUILD/oltp_concurrent_fsqlite" /tmp/oltp_conc_fs.db
    run_bench "$BUILD/oltp_concurrent_limbo"   /tmp/oltp_conc_li.db
    echo ""

    echo "--- TPC-C concurrent (file-backed) ---"
    rm -f /tmp/tpcc_conc_*.db /tmp/tpcc_conc_*.db-wal 2>/dev/null
    run_bench "$BUILD/tpcc_concurrent_sqlite"  /tmp/tpcc_conc_sq.db
    run_bench "$BUILD/tpcc_concurrent_fsqlite" /tmp/tpcc_conc_fs.db
    run_bench "$BUILD/tpcc_concurrent_limbo"   /tmp/tpcc_conc_li.db
    echo ""
fi

echo "=== Done ==="
echo "Results in: $RESULTS/"
ls -1 "$RESULTS"/*_${TIMESTAMP}.txt 2>/dev/null | wc -l | xargs -I{} echo "{} result files written"
