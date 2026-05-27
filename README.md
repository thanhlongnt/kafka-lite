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