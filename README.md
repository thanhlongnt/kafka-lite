# Kafka Lite

[![CI](https://github.com/thanhlongnt/kafka-lite/actions/workflows/ci.yml/badge.svg)](https://github.com/thanhlongnt/kafka-lite/actions/workflows/ci.yml)

A lightweight Kafka-like message broker built with gRPC and Raft consensus.

## Prerequisites

- Go 1.21+
- `protoc` + `protoc-gen-go` / `protoc-gen-go-grpc` (only needed to regenerate protos)

## Build

```bash
# Build the broker binary
go build -o broker ./cmd/broker

# Build the CLI binary
go build -o cli ./cmd/cli
```

## Run the Broker

```bash
./broker -addr :9092
```

The broker listens on `:9092` by default. Use `-addr` to change the port.

### Persistent storage

By default the broker stores messages in memory (lost on restart). Pass `-data-dir` to persist messages to disk:

```bash
./broker -addr :9092 -data-dir ./data
```

Messages are written to `<data-dir>/<topic>/<partition>/segment.log`. On restart with the same `-data-dir`, all previously created topics and their messages are restored automatically — no `create-topic` call is needed again.

## CLI Usage

### Create a topic

```bash
./cli create-topic -topic my-topic -partitions 3
```

### Produce a message

```bash
./cli produce -topic my-topic -partition 0 -key my-key -value "hello world"
```

### Consume messages

```bash
./cli consume -topic my-topic -partition 0 -offset 0
```

All commands accept `-broker <host:port>` to target a non-default broker address (default: `localhost:9092`).

## Phase 2: Coordinator + Multi-Broker

### Build

```bash
go build -o broker      ./cmd/broker
go build -o coordinator ./cmd/coordinator
go build -o cli         ./cmd/cli
```

### Start the coordinator and brokers

Brokers automatically register themselves with the coordinator on startup.

```bash
# Terminal 1 — coordinator
./coordinator -addr :9093

# Terminal 2 — broker A (registers as localhost:9092)
./broker -addr :9092 -coordinator localhost:9093

# Terminal 3 — broker B (registers as localhost:9094)
./broker -addr :9094 -coordinator localhost:9093
```

If your broker listens on all interfaces (e.g. `-addr :9092`) but needs to advertise
a different hostname, use `-advertise <host:port>`:

```bash
./broker -addr :9092 -coordinator localhost:9093 -advertise broker-a.example.com:9092
```

### Create a topic

```bash
# Partitions are assigned round-robin across registered brokers
./cli create-topic -coordinator localhost:9093 -topic events -partitions 4
```

### Produce with key-based routing

The broker proxies coordinator metadata, so producers only need a broker address.
Messages are hash-routed by key to the partition owner.

```bash
./cli produce -broker localhost:9092 -topic events -key user-1 -value "hello" -route
./cli produce -broker localhost:9092 -topic events -key user-2 -value "world" -route
```

### Consume as a consumer group

Each member gets an assigned subset of partitions. Adding a second member
rebalances the assignment. Offsets are committed every N messages with `-commit-every`.

```bash
# Terminal A
./cli consume -broker localhost:9092 -topic events -group grp1 -member m1 -commit-every 5

# Terminal B (second member, triggers rebalance)
./cli consume -broker localhost:9094 -topic events -group grp1 -member m2 -commit-every 5
```

## Load Generator

`cmd/throughput` is a closed-loop throughput generator that measures sustainable msgs/sec, MB/sec, and latency percentiles against a live cluster. See [docs/load-generator.md](docs/load-generator.md) for a full explanation of how it works.

### Quick start: coordinator routing benchmark

`scripts/run-routing-bench.sh` builds all binaries, spins up **5 coordinators + 20 brokers** in the background, runs the throughput test, then tears everything down on exit.

```bash
bash scripts/run-routing-bench.sh
```

Override any parameter via environment variable:

```bash
CONCURRENCY=40 DURATION=30s MSG_SIZE=1024 bash scripts/run-routing-bench.sh
```

| Variable | Default | Description |
|----------|---------|-------------|
| `TOPIC` | `bench` | Topic name |
| `PARTITIONS` | `20` | Number of partitions |
| `CONCURRENCY` | `20` | Parallel producer goroutines |
| `DURATION` | `15s` | Measurement window |
| `MSG_SIZE` | `256` | Payload size in bytes |

Logs for each process are written to `logs/` in the project root and kept after the run.

### Chaos mode

Set `CHAOS=1` to randomly kill and restart brokers and coordinators during the run. The script maintains Raft quorum (never kills more than 2 of 5 coordinators at once) and restarts each killed process after `CHAOS_DOWN_TIME` seconds.

```bash
CHAOS=1 bash scripts/run-routing-bench.sh
```

| Variable | Default | Description |
|----------|---------|-------------|
| `CHAOS` | `0` | Set to `1` to enable crash-and-restart chaos |
| `CHAOS_MIN_INTERVAL` | `10` | Min seconds between kill events |
| `CHAOS_MAX_INTERVAL` | `30` | Max seconds between kill events |
| `CHAOS_DOWN_TIME` | `5` | Seconds a killed process stays down before restart |

### Visualising results

Every run writes `logs/bench.csv` (per-second metrics) and `logs/chaos-events.csv` (kill/restart timestamps). If `python3` with `matplotlib` and `pandas` is available, the script automatically generates `logs/bench.png` after the run — a three-panel time-series plot showing throughput, latency (p50/p99), and consumer lag, with outage windows highlighted.

**Prerequisites:**
```bash
pip install matplotlib pandas
```

**Run and auto-plot:**
```bash
# Stable baseline — flat throughput, no shaded regions
DURATION=60s bash scripts/run-routing-bench.sh

# Chaos run — outage windows appear as red shaded regions
CHAOS=1 CHAOS_MIN_INTERVAL=10 CHAOS_MAX_INTERVAL=20 DURATION=60s bash scripts/run-routing-bench.sh
```

**Re-plot from existing CSVs without re-running the test:**
```bash
python3 scripts/plot-bench.py \
  --data   logs/bench.csv \
  --events logs/chaos-events.csv \
  --out    my-run.png \
  --title  "my experiment"
```

### Concurrency sweep (finding throughput limits)

`scripts/sweep.sh` runs the benchmark at increasing concurrency levels and plots throughput vs concurrency on a dual-axis chart. The chart annotates the **knee** — the lowest concurrency that reaches 90 % of peak throughput — which is the most efficient operating point before contention takes over.

```bash
bash scripts/sweep.sh
```

Results are written to `logs/sweep.csv` and plotted to `logs/sweep.png`.

Override the levels and duration:

```bash
# Fine-grained sweep, longer runs for stable averages
LEVELS="1 4 8 16 32 64 128" DURATION=25s bash scripts/sweep.sh

# Quick check with fewer levels
LEVELS="1 10 40 160" DURATION=15s bash scripts/sweep.sh
```

| Variable | Default | Description |
|----------|---------|-------------|
| `LEVELS` | `1 5 10 20 40 80 160` | Space-separated concurrency values to test |
| `DURATION` | `15s` | Benchmark duration per level (longer = more stable) |
| `PARTITIONS` | `20` | Partition count (held fixed across all levels) |
| `MSG_SIZE` | `256` | Payload size in bytes |

> **Tip:** Use `DURATION=25s` or longer — restarting the 25-process cluster between levels adds ~5 s of overhead per run, so short durations inflate variance.

### Minikube (isolated containers)

`scripts/run-minikube-bench.sh` runs the same benchmark inside Minikube — each broker and coordinator gets its own container with dedicated cgroups and communicates over the cluster network instead of loopback, giving more realistic isolation.

**Prerequisites:** [minikube](https://minikube.sigs.k8s.io/docs/start/) and [kubectl](https://kubernetes.io/docs/tasks/tools/).

```bash
# Start Minikube with enough resources for 25 pods
minikube start --cpus=6 --memory=8g

# Build image, deploy 5 coordinators + 20 brokers, run throughput job
bash scripts/run-minikube-bench.sh
```

Override parameters the same way:

```bash
CONCURRENCY=40 DURATION=120s bash scripts/run-minikube-bench.sh
```

The script streams the throughput job's output live and tears down all pods on exit (Ctrl-C or normal finish).

To inspect individual pod logs after a run:

```bash
kubectl logs coordinator-0
kubectl logs broker-3
```

### Manual usage

```bash
go build -o throughput ./cmd/throughput

# Single broker
./throughput -broker localhost:9092 -concurrency 8 -duration 10s

# With coordinator routing
./throughput -broker localhost:9092 \
             -coordinator localhost:9093 \
             -route -partitions 6 -concurrency 12
```

## Run Tests

```bash
# All tests
go test ./...

# Phase-specific integration tests
go test ./tests/ -run Phase1
go test ./tests/ -run Phase2
go test ./tests/ -run Phase3

# Unit tests for a specific package
go test ./internal/broker/
go test ./internal/producer/
go test ./internal/consumer/
go test ./internal/log/
```