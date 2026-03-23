#!/usr/bin/env bash
set -euo pipefail

# build.sh — compile all benchmarks against all available targets
#
# Targets are discovered via environment variables pointing to .so directories.
# Missing targets are skipped.
#
# Usage:
#   ./build.sh                          # build all available
#   FSQLITE_DIR=/path/to/release ./build.sh
#
# RAM WARNING:
#   Do NOT build full workspaces. Build only the C API crate for each target:
#     frankensqlite: cargo build --release -p fsqlite-c-api
#     limbo:         cargo build --release -p limbo-c
#   Full workspace builds use 8-12 GB RAM and will OOM on constrained machines.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

BUILD_DIR="$SCRIPT_DIR/build"
INCLUDE="$SCRIPT_DIR/include"
mkdir -p "$BUILD_DIR"

# ─── Target discovery ───────────────────────────────────────────────────

# System SQLite
SQLITE3_SO=""
SQLITE3_FLAGS=""
if [ -f /usr/lib/x86_64-linux-gnu/libsqlite3.so ]; then
    SQLITE3_SO="-lsqlite3"
    SQLITE3_FLAGS=""
elif [ -f /usr/lib/x86_64-linux-gnu/libsqlite3.so.0 ]; then
    SQLITE3_SO="/usr/lib/x86_64-linux-gnu/libsqlite3.so.0"
    SQLITE3_FLAGS=""
elif ldconfig -p 2>/dev/null | grep -q libsqlite3; then
    SQLITE3_SO="-lsqlite3"
    SQLITE3_FLAGS=""
fi

# FrankenSQLite
FSQLITE_DIR="${FSQLITE_DIR:-}"
# Limbo (Turso)
LIMBO_DIR="${LIMBO_DIR:-}"
# DuckDB
DUCKDB_DIR="${DUCKDB_DIR:-}"

echo "=== db_bench_foo build ==="
echo ""
echo "Targets:"
[ -n "$SQLITE3_SO" ]  && echo "  sqlite:       system libsqlite3" || echo "  sqlite:       NOT FOUND"
[ -n "$FSQLITE_DIR" ] && echo "  fsqlite:      $FSQLITE_DIR" || echo "  fsqlite:      NOT SET (set FSQLITE_DIR)"
[ -n "$LIMBO_DIR" ]   && echo "  limbo:        $LIMBO_DIR" || echo "  limbo:        NOT SET (set LIMBO_DIR)"
[ -n "$DUCKDB_DIR" ]  && echo "  duckdb:       $DUCKDB_DIR" || echo "  duckdb:       NOT SET (set DUCKDB_DIR)"
echo ""

BUILT=0
SKIPPED=0
FAILED=0

# ─── Build helper ────────────────────────────────────────────────────────

build_sqlite3_target() {
    local src="$1"
    local target_name="$2"
    local lib_dir="$3"      # empty for system sqlite
    local lib_name="$4"     # -lsqlite3 or -lfsqlite_c_api etc
    local extra_flags="${5:-}"
    local base
    base="$(basename "${src%.c}")"
    local out="$BUILD_DIR/${base}_${target_name}"

    local lib_flags=""
    if [ -n "$lib_dir" ]; then
        lib_flags="-L$lib_dir -l$lib_name -Wl,-rpath,$lib_dir"
    else
        lib_flags="$lib_name"
    fi

    if gcc -O2 "$src" -I"$INCLUDE" $lib_flags -lpthread -lm $extra_flags -o "$out" 2>/dev/null; then
        echo "  OK    $out"
        BUILT=$((BUILT + 1))
    else
        echo "  FAIL  $out"
        FAILED=$((FAILED + 1))
    fi
}

build_all_sqlite3_targets() {
    local src="$1"
    local extra_flags="${2:-}"

    if [ -n "$SQLITE3_SO" ]; then
        build_sqlite3_target "$src" "sqlite" "" "$SQLITE3_SO" "$extra_flags"
    fi
    if [ -n "$FSQLITE_DIR" ]; then
        build_sqlite3_target "$src" "fsqlite" "$FSQLITE_DIR" "fsqlite_c_api" "$extra_flags"
    fi
    if [ -n "$LIMBO_DIR" ]; then
        # Limbo's .so may be liblimbo.so, liblimbo_c.so, or libturso_sqlite3.so
        if [ -f "$LIMBO_DIR/liblimbo.so" ]; then
            build_sqlite3_target "$src" "limbo" "$LIMBO_DIR" "limbo" "$extra_flags"
        elif [ -f "$LIMBO_DIR/liblimbo_c.so" ]; then
            build_sqlite3_target "$src" "limbo" "$LIMBO_DIR" "limbo_c" "$extra_flags"
        elif [ -f "$LIMBO_DIR/libturso_sqlite3.so" ]; then
            build_sqlite3_target "$src" "limbo" "$LIMBO_DIR" "turso_sqlite3" "$extra_flags"
        else
            echo "  SKIP  $(basename "${src%.c}")_limbo (no .so found in $LIMBO_DIR)"
            SKIPPED=$((SKIPPED + 1))
        fi
    fi
}

# ─── Single-threaded benchmarks ─────────────────────────────────────────

echo "--- Single-threaded benchmarks ---"

for src in \
    single/micro/benchmark.c \
    single/ycsb/ycsb_bench.c \
    single/tpch/tpch_bench.c \
    single/tpcds/tpcds_bench.c \
    single/tpcc/tpcc_bench.c \
    single/oltp/oltp_read_write.c \
; do
    [ -f "$src" ] || { echo "  SKIP  $src (not found)"; SKIPPED=$((SKIPPED+1)); continue; }
    build_all_sqlite3_targets "$src"
done

# ─── Concurrent benchmarks ──────────────────────────────────────────────

echo ""
echo "--- Concurrent benchmarks ---"

for src in \
    concurrent/ycsb_concurrent.c \
    concurrent/oltp_concurrent.c \
    concurrent/tpcc_concurrent.c \
; do
    [ -f "$src" ] || { echo "  SKIP  $src (not found)"; SKIPPED=$((SKIPPED+1)); continue; }
    build_all_sqlite3_targets "$src"
done

# ─── DuckDB benchmarks ──────────────────────────────────────────────────

echo ""
echo "--- DuckDB benchmarks ---"

if [ -n "$DUCKDB_DIR" ]; then
    for src in duckdb/micro_duckdb.c duckdb/ycsb_duckdb.c duckdb/tpch_duckdb.c; do
        [ -f "$src" ] || { echo "  SKIP  $src (not found)"; SKIPPED=$((SKIPPED+1)); continue; }
        base="$(basename "${src%.c}")"
        out="$BUILD_DIR/$base"
        if gcc -O2 "$src" -I"$SCRIPT_DIR/duckdb/include" \
            -L"$DUCKDB_DIR" -lduckdb -Wl,-rpath,"$DUCKDB_DIR" \
            -lpthread -lm -o "$out" 2>/dev/null; then
            echo "  OK    $out"
            BUILT=$((BUILT + 1))
        else
            echo "  FAIL  $out"
            FAILED=$((FAILED + 1))
        fi
    done
else
    echo "  SKIP  (DUCKDB_DIR not set)"
fi

# ─── Summary ────────────────────────────────────────────────────────────

echo ""
echo "=== Build complete ==="
echo "  Built:   $BUILT"
echo "  Skipped: $SKIPPED"
echo "  Failed:  $FAILED"
