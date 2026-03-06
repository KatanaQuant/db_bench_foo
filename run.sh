#!/usr/bin/env bash
set -euo pipefail

# sqlite-bench run script
# Builds and runs benchmarks against available implementations.
#
# Usage:
#   ./run.sh                    # 1000 rows (default)
#   ./run.sh 500                # custom row count
#   TURSO_DIR=/path/to/libsql FRANKEN_DIR=/path/to/impl ./run.sh

ROWS="${1:-1000}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Find sqlite3.h — check system first, fall back to bundled
if [ -f /usr/include/sqlite3.h ]; then
    SQLITE3_INCLUDE=""
elif [ -f "$SCRIPT_DIR/include/sqlite3.h" ]; then
    SQLITE3_INCLUDE="-I$SCRIPT_DIR/include"
else
    echo "ERROR: sqlite3.h not found."
    echo "  Install: sudo apt-get install libsqlite3-dev"
    echo "  Or place sqlite3.h in $SCRIPT_DIR/include/"
    exit 1
fi

# Find libsqlite3
SQLITE3_LIB=""
if [ -f /usr/lib/x86_64-linux-gnu/libsqlite3.so ]; then
    SQLITE3_LIB="-lsqlite3"
elif [ -f /usr/lib/x86_64-linux-gnu/libsqlite3.so.0 ]; then
    SQLITE3_LIB="/usr/lib/x86_64-linux-gnu/libsqlite3.so.0"
elif ldconfig -p 2>/dev/null | grep -q libsqlite3; then
    SQLITE3_LIB="-lsqlite3"
else
    echo "ERROR: libsqlite3 not found."
    echo "  Install: sudo apt-get install libsqlite3-dev"
    exit 1
fi

echo "=== sqlite-bench ==="
echo "Rows: $ROWS"
echo ""

# --- 1. System SQLite ---
echo "[1/3] Building against system SQLite..."
gcc -O2 benchmark.c $SQLITE3_INCLUDE $SQLITE3_LIB -o benchmark_sqlite 2>&1
echo "[1/3] Running system SQLite..."
./benchmark_sqlite "$ROWS" | tee results_sqlite.txt
echo ""

# --- 2. Turso/libsql (optional) ---
TURSO_DIR="${TURSO_DIR:-}"
if [ -z "$TURSO_DIR" ] && [ -f "$SCRIPT_DIR/turso/target/release/libsql_experimental.so" ]; then
    TURSO_DIR="$SCRIPT_DIR/turso"
fi

if [ -n "$TURSO_DIR" ] && [ -f "$TURSO_DIR/target/release/libsql_experimental.so" ]; then
    TURSO_INCLUDE="$TURSO_DIR/bindings/c/include"
    TURSO_LIB="$TURSO_DIR/target/release"

    if [ -f "$TURSO_INCLUDE/libsql.h" ]; then
        echo "[2/3] Building against Turso/libsql..."
        gcc -O2 benchmark_turso.c \
            -I"$TURSO_INCLUDE" \
            -L"$TURSO_LIB" \
            -lsql_experimental -lpthread -ldl -lm \
            -o benchmark_turso_bin 2>&1
        echo "[2/3] Running Turso/libsql..."
        LD_LIBRARY_PATH="$TURSO_LIB" ./benchmark_turso_bin "$ROWS" | tee results_turso.txt
    else
        echo "[2/3] SKIP: Turso headers not found at $TURSO_INCLUDE/libsql.h"
    fi
else
    echo "[2/3] SKIP: Turso/libsql not found."
    echo "       Set TURSO_DIR=/path/to/libsql or build it first:"
    echo "       git clone https://github.com/tursodatabase/libsql && cd libsql && cargo build --release"
fi
echo ""

# --- 3. Custom sqlite3-compatible library (optional) ---
CUSTOM_DIR="${CUSTOM_DIR:-}"
CUSTOM_LIB_NAME="${CUSTOM_LIB_NAME:-}"
CUSTOM_LABEL="${CUSTOM_LABEL:-custom}"

if [ -n "$CUSTOM_DIR" ] && [ -n "$CUSTOM_LIB_NAME" ]; then
    echo "[3/3] Building against $CUSTOM_LABEL..."
    gcc -O2 benchmark.c \
        $SQLITE3_INCLUDE \
        -L"$CUSTOM_DIR" \
        -l"$CUSTOM_LIB_NAME" \
        -o "benchmark_${CUSTOM_LABEL}" 2>&1
    echo "[3/3] Running $CUSTOM_LABEL..."
    LD_LIBRARY_PATH="$CUSTOM_DIR" ./"benchmark_${CUSTOM_LABEL}" "$ROWS" | tee "results_${CUSTOM_LABEL}.txt"
else
    echo "[3/3] SKIP: No custom implementation specified."
    echo "       Set CUSTOM_DIR and CUSTOM_LIB_NAME to benchmark another sqlite3-compatible library:"
    echo "       CUSTOM_DIR=/path/to/lib CUSTOM_LIB_NAME=their_sqlite3 CUSTOM_LABEL=mydb ./run.sh"
fi
echo ""

echo "=== Done ==="
echo "Results saved to results_*.txt"
