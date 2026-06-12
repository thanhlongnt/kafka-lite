#!/usr/bin/env bash
# sweep.sh — find the throughput/latency knee by running the benchmark at
# increasing target message rates and plotting throughput + p99 latency vs rate.
#
# Usage:
#   bash scripts/sweep.sh
#
# Override parameters via environment variables:
#   RATES="1000 5000 10000 20000 40000 80000" DURATION=120s bash scripts/sweep.sh
#
# Enable chaos (node kill/restart) mode:
#   CHAOS=1 DURATION=120s bash scripts/sweep.sh
#   (or use the convenience wrapper: bash scripts/sweep-chaos.sh)

RATES="${RATES:-2000 5000 10000 20000 40000 80000 160000}"  # target msgs/sec per level
DURATION="${DURATION:-15s}"                                 # run duration per level
PARTITIONS="${PARTITIONS:-20}"                              # keep fixed across all levels
MSG_SIZE="${MSG_SIZE:-256}"                                 # message payload size

CHAOS="${CHAOS:-0}"
CHAOS_MIN_INTERVAL="${CHAOS_MIN_INTERVAL:-10}"
CHAOS_MAX_INTERVAL="${CHAOS_MAX_INTERVAL:-30}"
CHAOS_DOWN_TIME="${CHAOS_DOWN_TIME:-5}"

RUN_PREFIX="${RUN_PREFIX:-bench}"
SWEEP_CSV="${SWEEP_CSV:-}"   # set after ROOT is known
SWEEP_PNG="${SWEEP_PNG:-}"

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$ROOT/logs"

[ -z "$SWEEP_CSV" ] && SWEEP_CSV="$LOG_DIR/sweep.csv"
[ -z "$SWEEP_PNG" ] && SWEEP_PNG="$LOG_DIR/sweep.png"

CHAOS_LABEL=""
[ "$CHAOS" = "1" ] && CHAOS_LABEL="  chaos=on (kill every ${CHAOS_MIN_INTERVAL}-${CHAOS_MAX_INTERVAL}s, down ${CHAOS_DOWN_TIME}s)"

mkdir -p "$LOG_DIR"
echo "rate_target,avg_msgs_per_sec,avg_mb_per_sec,avg_p50_ms,avg_p99_ms,errors" \
    > "$SWEEP_CSV"

echo "==> rate sweep (open-loop)${CHAOS_LABEL}"
echo "    rates     : $RATES msg/s"
echo "    duration  : $DURATION per level"
echo "    partitions: $PARTITIONS  msg-size: ${MSG_SIZE}B"
echo ""

for R in $RATES; do
    printf "==> rate=%-8d  " "$R"

    BENCH_CSV="$LOG_DIR/${RUN_PREFIX}-rate-${R}.csv"
    EVENTS_FILE="$LOG_DIR/chaos-events-${RUN_PREFIX}-rate-${R}.csv"

    # Run bench with plotting suppressed; we'll plot per-rate below.
    RUN_OUT=$(
        RATE="$R" \
        DURATION="$DURATION" \
        PARTITIONS="$PARTITIONS" \
        MSG_SIZE="$MSG_SIZE" \
        CSV_OUT="$BENCH_CSV" \
        EVENTS_FILE="$EVENTS_FILE" \
        CHAOS="$CHAOS" \
        CHAOS_MIN_INTERVAL="$CHAOS_MIN_INTERVAL" \
        CHAOS_MAX_INTERVAL="$CHAOS_MAX_INTERVAL" \
        CHAOS_DOWN_TIME="$CHAOS_DOWN_TIME" \
        PLOT=0 \
        bash "$ROOT/scripts/run-routing-bench.sh" 2>/dev/null
    )

    # Extract metrics from per-rate CSV (skip header + 2 warmup rows, average the rest).
    if [ ! -f "$BENCH_CSV" ]; then
        echo "warn: $BENCH_CSV not found, skipping"
        continue
    fi

    ROW=$(python3 - "$BENCH_CSV" "$R" <<'EOF'
import csv, sys

path, R = sys.argv[1], sys.argv[2]
rows = list(csv.DictReader(open(path)))
rows = rows[2:]  # skip first 2s warmup
if not rows:
    print(f"{R},0,0,0,0,0")
    sys.exit()

avg_msgs = sum(float(r["msgs_per_sec"]) for r in rows) / len(rows)
avg_mb   = sum(float(r["mb_per_sec"])   for r in rows) / len(rows)
avg_p50  = sum(float(r["p50_ms"])       for r in rows) / len(rows)
avg_p99  = sum(float(r["p99_ms"])       for r in rows) / len(rows)

total_errs = sum(int(r["errs_per_sec"]) for r in rows)

print(f"{R},{avg_msgs:.0f},{avg_mb:.2f},{avg_p50:.3f},{avg_p99:.3f},{total_errs}")
EOF
    )

    echo "$ROW" >> "$SWEEP_CSV"

    AVG=$(echo "$ROW" | cut -d, -f2)
    P99=$(echo "$ROW" | cut -d, -f5)
    ERRS=$(echo "$ROW" | cut -d, -f6)
    printf "avg=%6s msg/s  p99=%s ms  errors=%s\n" "$AVG" "$P99" "$ERRS"

    # Per-rate time-series plot.
    if command -v python3 >/dev/null 2>&1 && [ -f "$ROOT/scripts/plot-bench.py" ]; then
        PLOT_ARGS=(
            --data  "$BENCH_CSV"
            --out   "$LOG_DIR/${RUN_PREFIX}-rate-${R}.png"
            --title "kafka-lite: rate=${R} msg/s  ${PARTITIONS}p ${MSG_SIZE}B ${DURATION}${CHAOS_LABEL}"
        )
        [ "$CHAOS" = "1" ] && PLOT_ARGS+=(--events "$EVENTS_FILE")
        python3 "$ROOT/scripts/plot-bench.py" "${PLOT_ARGS[@]}" 2>/dev/null || true
    fi
done

echo ""
echo "==> sweep complete"
echo "    results : $SWEEP_CSV"

# Auto-plot summary
if command -v python3 >/dev/null 2>&1 && [ -f "$ROOT/scripts/plot-sweep.py" ]; then
    python3 "$ROOT/scripts/plot-sweep.py" \
        --data  "$SWEEP_CSV" \
        --out   "$SWEEP_PNG" \
        --title "kafka-lite rate sweep (${PARTITIONS}p ${MSG_SIZE}B ${DURATION}/level${CHAOS_LABEL})" \
        && echo "    plot    : $SWEEP_PNG"
fi
