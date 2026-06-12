#!/usr/bin/env bash
# run-routing-bench.sh — build, spin up 5 coordinators + 20 brokers, run throughput test.
#
# Override any parameter via environment variable before running, e.g.:
#   CONCURRENCY=40 DURATION=30s bash scripts/run-routing-bench.sh
#
# Enable random crash-and-restart chaos:
#   CHAOS=1 bash scripts/run-routing-bench.sh
#   CHAOS=1 CHAOS_MIN_INTERVAL=5 CHAOS_MAX_INTERVAL=10 CHAOS_DOWN_TIME=3 bash scripts/run-routing-bench.sh

# Throughput params
TOPIC="${TOPIC:-bench}"
PARTITIONS="${PARTITIONS:-20}"
CONCURRENCY="${CONCURRENCY:-20}"
DURATION="${DURATION:-15s}"
MSG_SIZE="${MSG_SIZE:-256}"

# Chaos params
CHAOS="${CHAOS:-0}"
CHAOS_MIN_INTERVAL="${CHAOS_MIN_INTERVAL:-10}"   # min seconds between kill events
CHAOS_MAX_INTERVAL="${CHAOS_MAX_INTERVAL:-30}"   # max seconds between kill events
CHAOS_DOWN_TIME="${CHAOS_DOWN_TIME:-5}"          # seconds a killed process stays down

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RAFT_DIR="/tmp/kafka-lite-raft-bench"
LOG_DIR="$ROOT/logs"

# 5 coordinator nodes: gRPC ports 9093-9097, Raft ports 7000-7004
NUM_COORDS=5
COORD_GRPC_BASE=9093
COORD_RAFT_BASE=7000

# 20 broker nodes: ports 9100-9119
NUM_BROKERS=20
BROKER_PORT_BASE=9100

# PID tracking: flat array for cleanup, typed arrays for chaos targeting
PIDS=()
COORD_PIDS=()   # indexed [0..NUM_COORDS-1]
BROKER_PIDS=()  # indexed [0..NUM_BROKERS-1]

CSV_OUT="$LOG_DIR/bench.csv"
EVENTS_FILE="$LOG_DIR/chaos-events.csv"

cleanup() {
    echo ""
    echo "==> stopping..."
    set +m  # suppress "Killed" job-control notifications
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

# Kill any leftover processes from a previous run before starting fresh.
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
    local i=$1   # 1-based
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
    local i=$1   # 1-based
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

# ── chaos loop ────────────────────────────────────────────────────────────────

chaos_loop() {
    local dead_coords=0
    local interval_range=$(( CHAOS_MAX_INTERVAL - CHAOS_MIN_INTERVAL + 1 ))

    while true; do
        sleep $(( RANDOM % interval_range + CHAOS_MIN_INTERVAL ))

        # Bias 2:1 toward brokers over coordinators.
        if [ $(( RANDOM % 3 )) -lt 2 ]; then
            local idx=$(( RANDOM % NUM_BROKERS ))
            local pid="${BROKER_PIDS[$idx]}"
            local num=$(( idx + 1 ))
            local elapsed=$(( SECONDS - TEST_START ))
            echo "[CHAOS] killing broker $num (pid $pid)"
            echo "$elapsed,kill,broker,$num" >> "$EVENTS_FILE"
            kill -9 "$pid" 2>/dev/null || true
            sleep "$CHAOS_DOWN_TIME"
            local elapsed=$(( SECONDS - TEST_START ))
            echo "[CHAOS] restarting broker $num"
            echo "$elapsed,restart,broker,$num" >> "$EVENTS_FILE"
            start_broker "$num"
        else
            # Never exceed 2 dead coordinators simultaneously (quorum requires 3/5).
            if [ "$dead_coords" -ge 2 ]; then
                continue
            fi
            local idx=$(( RANDOM % NUM_COORDS ))
            local pid="${COORD_PIDS[$idx]}"
            local num=$(( idx + 1 ))
            local elapsed=$(( SECONDS - TEST_START ))
            echo "[CHAOS] killing coordinator $num (pid $pid)"
            echo "$elapsed,kill,coordinator,$num" >> "$EVENTS_FILE"
            kill -9 "$pid" 2>/dev/null || true
            dead_coords=$(( dead_coords + 1 ))
            sleep "$CHAOS_DOWN_TIME"
            local elapsed=$(( SECONDS - TEST_START ))
            echo "[CHAOS] restarting coordinator $num"
            echo "$elapsed,restart,coordinator,$num" >> "$EVENTS_FILE"
            start_coordinator "$num"
            dead_coords=$(( dead_coords - 1 ))
        fi
    done
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

# ── optionally start chaos loop ───────────────────────────────────────────────
# Initialise events file (header always written so plot script can open it safely).
echo "elapsed_s,event,type,num" > "$EVENTS_FILE"

if [ "$CHAOS" = "1" ]; then
    echo "==> chaos mode enabled (kills every ${CHAOS_MIN_INTERVAL}-${CHAOS_MAX_INTERVAL}s, down for ${CHAOS_DOWN_TIME}s)"
    chaos_loop &
    PIDS+=($!)
fi

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
echo "==> throughput test"
echo "    topic=$TOPIC  partitions=$PARTITIONS  concurrency=$CONCURRENCY"
CHAOS_LABEL=""
[ "$CHAOS" = "1" ] && CHAOS_LABEL="  chaos=on"
echo "    duration=$DURATION  msg-size=${MSG_SIZE}B  mode=route${CHAOS_LABEL}"
echo "    CSV output  : $CSV_OUT"
echo ""

TEST_START=$SECONDS
/tmp/kl-throughput \
    -broker      "$ENTRY_BROKER" \
    -coordinator "$COORD_GRPC_LIST" \
    -topic       "$TOPIC" \
    -partitions  "$PARTITIONS" \
    -concurrency "$CONCURRENCY" \
    -duration    "$DURATION" \
    -msg-size    "$MSG_SIZE" \
    -csv         "$CSV_OUT" \
    -route

# ── auto-plot ─────────────────────────────────────────────────────────────────
PLOT_OUT="$LOG_DIR/bench.png"
if command -v python3 >/dev/null 2>&1 && [ -f "$ROOT/scripts/plot-bench.py" ]; then
    echo ""
    echo "==> plotting results..."
    python3 "$ROOT/scripts/plot-bench.py" \
        --data   "$CSV_OUT" \
        --events "$EVENTS_FILE" \
        --out    "$PLOT_OUT" \
        --title  "kafka-lite: ${PARTITIONS}p ${CONCURRENCY}c ${MSG_SIZE}B${CHAOS_LABEL}" \
        && echo "    saved to $PLOT_OUT" \
        || echo "    plot failed (run manually: python3 scripts/plot-bench.py --data $CSV_OUT)"
fi
