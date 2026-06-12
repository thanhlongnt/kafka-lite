#!/usr/bin/env bash
# run-degradation-bench.sh — build, spin up 5 coordinators + 20 brokers, run
# throughput test while permanently killing brokers one by one.
#
# Brokers are killed in order at DEGRADE_INTERVAL-second intervals and never
# restarted. As each broker dies the coordinator promotes a backup, increasing
# the load on surviving nodes and causing latency to climb progressively.
#
# Usage:
#   bash scripts/run-degradation-bench.sh
#   RATE=20000 DURATION=300s DEGRADE_INTERVAL=20 DEGRADE_MAX_KILLS=10 \
#       bash scripts/run-degradation-bench.sh

# Throughput params
TOPIC="${TOPIC:-bench}"
PARTITIONS="${PARTITIONS:-20}"
CONCURRENCY="${CONCURRENCY:-0}"    # 0 = use GOMAXPROCS default in the binary
RATE="${RATE:-0}"                  # target msgs/sec total (0 = unlimited closed-loop)
DURATION="${DURATION:-300s}"       # 5 min default — enough to observe progression
MSG_SIZE="${MSG_SIZE:-256}"

# Degradation params
DEGRADE_INTERVAL="${DEGRADE_INTERVAL:-20}"   # seconds between permanent kills
DEGRADE_MAX_KILLS="${DEGRADE_MAX_KILLS:-10}" # how many brokers to kill (default half)

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RAFT_DIR="/tmp/kafka-lite-raft-bench"
LOG_DIR="$ROOT/results/degradation"

# 5 coordinator nodes: gRPC ports 9093-9097, Raft ports 7000-7004
NUM_COORDS=5
COORD_GRPC_BASE=9093
COORD_RAFT_BASE=7000

# 20 broker nodes: ports 9100-9119
NUM_BROKERS=20
BROKER_PORT_BASE=9100

PIDS=()
COORD_PIDS=()
BROKER_PIDS=()

CSV_OUT="${CSV_OUT:-$LOG_DIR/bench.csv}"
EVENTS_FILE="${EVENTS_FILE:-$LOG_DIR/degrade-events.csv}"

cleanup() {
    echo ""
    echo "==> stopping..."
    set +m
    for pid in "${PIDS[@]}"; do
        kill -9 "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
    rm -rf "$RAFT_DIR"
    echo "==> logs left in $LOG_DIR"
    echo "    bench CSV   : $CSV_OUT"
    [ -f "$EVENTS_FILE" ] && echo "    events CSV  : $EVENTS_FILE"
}
trap cleanup EXIT INT TERM

pkill -f kl-broker      2>/dev/null || true
pkill -f kl-coordinator 2>/dev/null || true
sleep 0.5

mkdir -p "$LOG_DIR"
rm -rf "$RAFT_DIR"

# ── build ─────────────────────────────────────────────────────────────────────
echo "==> building..."
cd "$ROOT"
go build -o /tmp/kl-coordinator ./cmd/coordinator
go build -o /tmp/kl-broker      ./cmd/broker
go build -o /tmp/kl-throughput  ./cmd/throughput
echo "    done"

# ── address lists ─────────────────────────────────────────────────────────────
COORD_GRPC_LIST=""
for i in $(seq 1 $NUM_COORDS); do
    port=$(( COORD_GRPC_BASE + i - 1 ))
    COORD_GRPC_LIST="${COORD_GRPC_LIST:+$COORD_GRPC_LIST,}localhost:$port"
done

# ── start functions ───────────────────────────────────────────────────────────

start_coordinator() {
    local i=$1
    local grpc_port=$(( COORD_GRPC_BASE + i - 1 ))
    local raft_port=$(( COORD_RAFT_BASE + i - 1 ))
    local node_id="node$i"
    local data_dir="$RAFT_DIR/$node_id"
    mkdir -p "$data_dir"

    local peers=""
    for j in $(seq 1 $NUM_COORDS); do
        if [ "$j" -ne "$i" ]; then
            local peer_raft_port=$(( COORD_RAFT_BASE + j - 1 ))
            peers="${peers:+$peers,}node$j=127.0.0.1:$peer_raft_port"
        fi
    done

    /tmp/kl-coordinator \
        -addr                "localhost:$grpc_port" \
        -raft-id             "$node_id" \
        -raft-bind           "127.0.0.1:$raft_port" \
        -data-dir            "$data_dir" \
        -peers               "$peers" \
        -failover-tick       1s \
        -failover-dead-after 5s \
        >"$LOG_DIR/coordinator-$i.log" 2>&1 &
    local pid=$!
    COORD_PIDS[$((i-1))]=$pid
    PIDS+=($pid)
}

start_broker() {
    local i=$1
    local port=$(( BROKER_PORT_BASE + i - 1 ))
    local addr="localhost:$port"

    /tmp/kl-broker \
        -addr        ":$port" \
        -id          "$i" \
        -advertise   "$addr" \
        -coordinator "$COORD_GRPC_LIST" \
        >"$LOG_DIR/broker-$i.log" 2>&1 &
    local pid=$!
    BROKER_PIDS[$((i-1))]=$pid
    PIDS+=($pid)
}

# ── degradation loop ──────────────────────────────────────────────────────────
# Kills brokers one by one in random order. Never restarts them.

degradation_loop() {
    # Build list of 1-based broker IDs and shuffle kill order via RANDOM.
    local alive_ids=()
    for i in $(seq 1 $NUM_BROKERS); do alive_ids+=($i); done

    local killed=0
    while [ $killed -lt $DEGRADE_MAX_KILLS ] && [ ${#alive_ids[@]} -gt 0 ]; do
        sleep "$DEGRADE_INTERVAL"
        local rand_pos=$(( RANDOM % ${#alive_ids[@]} ))
        local num=${alive_ids[$rand_pos]}
        alive_ids=( "${alive_ids[@]:0:$rand_pos}" "${alive_ids[@]:$((rand_pos+1))}" )

        local pid="${BROKER_PIDS[$((num-1))]}"
        local elapsed=$(( $(date +%s) - BENCH_EPOCH ))
        echo "[DEGRADE] permanently killing broker $num (pid $pid)  elapsed=${elapsed}s  killed=$(( killed + 1 ))/${DEGRADE_MAX_KILLS}"
        kill -9 "$pid" 2>/dev/null || true
        echo "$elapsed,kill_permanent,broker,$num" >> "$EVENTS_FILE"
        killed=$(( killed + 1 ))
    done
    echo "[DEGRADE] done — killed $killed brokers, $(( NUM_BROKERS - killed )) remaining"
}

# ── start cluster ─────────────────────────────────────────────────────────────
echo "==> starting $NUM_COORDS coordinators..."
for i in $(seq 1 $NUM_COORDS); do
    start_coordinator "$i"
done

echo "    waiting for Raft leader election..."
sleep 2

echo "==> starting $NUM_BROKERS brokers..."
for i in $(seq 1 $NUM_BROKERS); do
    start_broker "$i"
done

sleep 2

echo "elapsed_s,event,type,num" > "$EVENTS_FILE"

# ── throughput test ───────────────────────────────────────────────────────────
ENTRY_BROKER="localhost:$BROKER_PORT_BASE"

echo ""
echo "==> cluster ready"
echo "    coordinators : $COORD_GRPC_LIST"
printf "    brokers      : "
for i in $(seq 1 $NUM_BROKERS); do
    printf "localhost:%d " $(( BROKER_PORT_BASE + i - 1 ))
done
echo ""
echo ""
echo "==> degradation test"
echo "    topic=$TOPIC  partitions=$PARTITIONS  rate=${RATE:-unlimited}"
echo "    duration=$DURATION  msg-size=${MSG_SIZE}B  mode=route"
echo "    killing 1 broker every ${DEGRADE_INTERVAL}s, max ${DEGRADE_MAX_KILLS} kills"
echo "    CSV output  : $CSV_OUT"
echo ""

THROUGHPUT_ARGS=(
    -broker      "$ENTRY_BROKER"
    -coordinator "$COORD_GRPC_LIST"
    -topic       "$TOPIC"
    -partitions  "$PARTITIONS"
    -duration    "$DURATION"
    -msg-size    "$MSG_SIZE"
    -csv         "$CSV_OUT"
    -route
)
if [ "$CONCURRENCY" -gt 0 ] 2>/dev/null; then THROUGHPUT_ARGS+=(-concurrency "$CONCURRENCY"); fi
if [ "$RATE"        -gt 0 ] 2>/dev/null; then THROUGHPUT_ARGS+=(-rate        "$RATE");        fi

# Background throughput; start degradation loop only after the tool's per-second
# clock begins so event timestamps align with the CSV's elapsed_s column.
TPUT_LOG="$(mktemp /tmp/kl-tput-XXXX.log)"
/tmp/kl-throughput "${THROUGHPUT_ARGS[@]}" | tee "$TPUT_LOG" &
TPUT_PID=$!
PIDS+=($TPUT_PID)

until grep -qm1 '\[  *1s\]' "$TPUT_LOG" 2>/dev/null; do sleep 0.1; done
export BENCH_EPOCH=$(( $(date +%s) - 1 ))

degradation_loop &
PIDS+=($!)

wait "$TPUT_PID" 2>/dev/null || true
rm -f "$TPUT_LOG"

# ── auto-plot ─────────────────────────────────────────────────────────────────
PLOT_OUT="$LOG_DIR/bench-degradation.png"
if [ "${PLOT:-1}" != "0" ] && command -v python3 >/dev/null 2>&1 && [ -f "$ROOT/scripts/plot-bench.py" ]; then
    echo ""
    echo "==> plotting results..."
    python3 "$ROOT/scripts/plot-bench.py" \
        --data    "$CSV_OUT" \
        --events  "$EVENTS_FILE" \
        --brokers "$NUM_BROKERS" \
        --out     "$PLOT_OUT" \
        --title   "kafka-lite degradation: ${PARTITIONS}p rate=${RATE:-unlimited} ${MSG_SIZE}B (kill every ${DEGRADE_INTERVAL}s, max ${DEGRADE_MAX_KILLS})" \
        && echo "    saved to $PLOT_OUT" \
        || echo "    plot failed (run manually: python3 scripts/plot-bench.py --data $CSV_OUT --events $EVENTS_FILE --brokers $NUM_BROKERS)"
fi
