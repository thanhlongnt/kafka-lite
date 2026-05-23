# Kafka Lite — Project Proposal

## Overview

Our goal is to build a distributed application that moves data while emphasizing reliability and fault tolerance. We've chosen to implement a distributed pub-sub system similar to Kafka and RabbitMQ (a "Kafka Lite"). The project will be written in Go for rapid development, leaving ample time for testing.

## Implementation Details

- **Primary language:** Go; other languages may be used for load testing.
- Fault tolerance and consensus are treated at the user level — we will use existing tools rather than implement them from scratch.
- We will not attempt to replicate all Kafka features; instead, we focus on an MVP pub-sub with fault tolerance and Raft integration.
- Testing will prioritize correctness and reliability over performance.

## Phases

### Phase 1: Single-Broker Core

Establish core message-passing primitives with a single broker before introducing distribution complexity.

- Implement an append-only log with one producer and one consumer
- Validate end-to-end message delivery and consumer offset tracking
- Extend to multiple consumers, then multiple producers on the same broker
- **Integration tests:** assert correctness of message ordering and delivery guarantees

### Phase 2: Multi-Broker Partitioning

Distribute load across brokers by sharding topic partitions, introducing the challenges of distributed state.

- Partition topics across multiple broker nodes; add partition key to maintain destination ordering
- Implement partition-aware producer routing and consumer group coordination
- **Integration tests:** verify data availability and routing correctness across brokers

### Phase 3: Raft-Based Fault Tolerance

Introduce leader election and log replication using Hashicorp's Raft implementation to achieve high availability.

- Integrate Hashicorp Raft for broker leader election and log replication
- Handle broker failure and automated recovery without data loss
- **Integration tests:** kill broker nodes and assert continued data availability

## Testing

Automated integration tests will verify data availability upon killing nodes. Each phase has its own dedicated test suite as described above.

## Goals

### Primary Goals

- Basic pub-sub implementation with an append-only log
- Properly implemented fault tolerance and consensus
- Valid, automated test coverage

### Reach Goals

- Achieve high throughput alongside fault tolerance
