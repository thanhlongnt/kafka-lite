#!/usr/bin/env bash
# run-routing-bench.sh — build, spin up 5 coordinators + 20 brokers, run throughput test.
#
# Override any parameter via environment variable before running, e.g.:
#   CONCURRENCY=40 DURATION=30s bash scripts/run-routing-bench.sh
#
# Params
TOPIC="${TOPIC:-bench}"
PARTITIONS="${PARTITIONS:-20}"
CONCURRENCY="${CONCURRENCY:-20}"
DURATION="${DURATION:-15s}"
MSG_SIZE="${MSG_SIZE:-256}"

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

PIDS=()

cleanup() {
    echo ""
    echo "==> stopping..."
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait "${PIDS[@]}" 2>/dev/null || true
    rm -rf "$RAFT_DIR"
    echo "==> logs left in $LOG_DIR"
}
trap cleanup EXIT INT TERM

mkdir -p "$LOG_DIR"
rm -rf "$RAFT_DIR"

# ── build ─────────────────────────────────────────────────────────────────────
echo "==> building..."
cd "$ROOT"
go build -o /tmp/kl-coordinator ./cmd/coordinator
go build -o /tmp/kl-broker      ./cmd/broker
go build -o /tmp/kl-throughput  ./cmd/throughput
echo "    done"

# ── build coordinator address lists ───────────────────────────────────────────
# COORD_GRPC_LIST: "localhost:9093,localhost:9094,..." (for broker -coordinator flag)
# COORD_PEERS:     "node2=127.0.0.1:7001,node3=127.0.0.1:7002,..." (for -peers flag,
#                  built per-node by excluding itself)
COORD_GRPC_LIST=""
for i in $(seq 1 $NUM_COORDS); do
    port=$(( COORD_GRPC_BASE + i - 1 ))
    COORD_GRPC_LIST="${COORD_GRPC_LIST:+$COORD_GRPC_LIST,}localhost:$port"
done

# ── coordinators ──────────────────────────────────────────────────────────────
echo "==> starting $NUM_COORDS coordinators..."
for i in $(seq 1 $NUM_COORDS); do
    grpc_port=$(( COORD_GRPC_BASE + i - 1 ))
    raft_port=$(( COORD_RAFT_BASE + i - 1 ))
    node_id="node$i"
    data_dir="$RAFT_DIR/$node_id"
    mkdir -p "$data_dir"

    # Build -peers list: all nodes except this one.
    peers=""
    for j in $(seq 1 $NUM_COORDS); do
        if [ "$j" -ne "$i" ]; then
            peer_raft_port=$(( COORD_RAFT_BASE + j - 1 ))
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
    PIDS+=($!)
done

# Wait for Raft to elect a leader across the 5-node cluster.
echo "    waiting for Raft leader election..."
sleep 2

# ── brokers ───────────────────────────────────────────────────────────────────
echo "==> starting $NUM_BROKERS brokers..."
for i in $(seq 1 $NUM_BROKERS); do
    port=$(( BROKER_PORT_BASE + i - 1 ))
    addr="localhost:$port"

    /tmp/kl-broker \
        -addr        ":$port" \
        -id          "$i" \
        -advertise   "$addr" \
        -coordinator "$COORD_GRPC_LIST" \
        >"$LOG_DIR/broker-$i.log" 2>&1 &
    PIDS+=($!)
done

# Give brokers time to register and send their first heartbeat.
sleep 2

# ── throughput test ───────────────────────────────────────────────────────────
ENTRY_BROKER="localhost:$BROKER_PORT_BASE"
ENTRY_COORD="localhost:$COORD_GRPC_BASE"

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
echo "    duration=$DURATION  msg-size=${MSG_SIZE}B  mode=route"
echo ""

/tmp/kl-throughput \
    -broker      "$ENTRY_BROKER" \
    -coordinator "$COORD_GRPC_LIST" \
    -topic       "$TOPIC" \
    -partitions  "$PARTITIONS" \
    -concurrency "$CONCURRENCY" \
    -duration    "$DURATION" \
    -msg-size    "$MSG_SIZE" \
    -route
