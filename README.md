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