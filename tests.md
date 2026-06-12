# Tests

Snapshot of all test suites in this repo, with their current results. Re-run with:

```bash
go test -race ./internal/...                          # unit tests
go test ./tests/ -run Phase1 -v -timeout 60s          # integration phase 1
go test ./tests/ -run Phase2 -v -timeout 60s          # integration phase 2
go test ./tests/ -run Phase3 -v -timeout 180s         # integration phase 3
go test ./tests/ -run Chaos -v -timeout 300s         # chaos / auto-failover
```

Last full run: 2026-06-11 on macOS (Darwin 25.5.0), Go 1.25.5.

## Unit tests (`internal/...`)

`go test -race ./internal/... -timeout 60s` — **31 / 31 pass**.

### `internal/broker` — 7 / 7 pass

| Test | Time | What it covers |
|---|---|---|
| `TestCreateTopic` | 0.01s | `CreateTopic` rejects empty names and duplicate topics, accepts valid `partitions > 0`. |
| `TestProduceFetch` | 0.00s | A produced message is returned by `Fetch` at the expected offset. |
| `TestFetchUnknownTopic` | 0.00s | `Fetch` on a missing topic returns `NotFound`. |
| `TestFetchStreamsMessagesProducedAfterOpen` | 0.00s | Server-streaming `Fetch` blocks at EOF and resumes when a new message lands (HW advance wakes `WaitForHW`). |
| `TestConcurrentProducesOrderedOnFetch` | 0.01s | Concurrent producers on the same partition still surface monotonic offsets to a single fetcher. |
| `TestBrokerRestoreFromDataDir` | 0.01s | `NewWithDataDir` rebuilds the topic registry by scanning numeric subdirs; existing messages survive restart. |
| `TestMultiplePartitions` | 0.00s | Independent partitions accept produces/fetches without serializing on each other. |

### `internal/consumer` — 6 / 6 pass

| Test | Time | What it covers |
|---|---|---|
| `TestPollReceivesProducedMessages` | 0.01s | `Consumer.Poll` returns each produced message in order. |
| `TestPollStartOffset` | 0.00s | A non-zero `start_offset` skips earlier records. |
| `TestPollBlocksUntilProduced` | 0.02s | `Poll` blocks on an empty partition and returns once a producer writes. |
| `TestOffsetTracking` | 0.00s | `Consumer.Offset()` advances past each `Recv`. |
| `TestPollCancelledContext` | 0.02s | Cancelling the context unblocks `Poll` cleanly. |
| `TestPollUnknownTopic` | 0.00s | `Poll` against a missing topic surfaces the `NotFound` error. |

### `internal/log` — 12 / 12 pass

Same suite is run twice via a factory, once against `MemLog` and once against `FileLog`. The shared assertions are append/read ordering, partial reads, out-of-range reads, length, and concurrent append/read.

| Test | Time | What it covers |
|---|---|---|
| `TestMemLog_AppendRead` | 0.00s | `MemLog` append assigns sequential offsets; `Read` returns them. |
| `TestMemLog_PartialRead` | 0.00s | `Read(offset, n)` returns up to `n` records (or fewer at the tail). |
| `TestMemLog_OutOfRangeRead` | 0.00s | Out-of-range offset returns an error. |
| `TestMemLog_Len` | 0.00s | `Len()` reports the count. |
| `TestMemLog_ConcurrentAppendRead` | 0.00s | Concurrent writers + reader produce no race (under `-race`). |
| `TestFileLog_AppendRead` | 0.00s | Same as above against `FileLog`. |
| `TestFileLog_PartialRead` | 0.00s | Same. |
| `TestFileLog_OutOfRangeRead` | 0.00s | Same. |
| `TestFileLog_Len` | 0.00s | Same. |
| `TestFileLog_ConcurrentAppendRead` | 0.01s | Same. |
| `TestFileLog_PersistAcrossReopen` | 0.00s | Closing + reopening a `FileLog` rebuilds the index from the segment file and surfaces every committed record. |
| `TestFileLog_TruncatedWriteRecovery` | 0.00s | A partial trailing record (simulating a crash mid-write) is silently dropped on `rebuildIndex`; the log is self-healing. |

### `internal/producer` — 3 / 3 pass

| Test | Time | What it covers |
|---|---|---|
| `TestSend` | 0.01s | `Send` returns the offset the broker assigned. |
| `TestSendUnknownTopic` | 0.00s | Producing to a missing topic surfaces the `NotFound` error. |
| `TestSendMultiplePartitions` | 0.00s | The producer honors the partition supplied by the caller (no routing in Phase 1). |

## Phase 1 — single-broker basics (`tests/phase1_test.go`)

`go test ./tests/ -run Phase1 -v -timeout 60s` — **3 / 3 pass, 0.28s wall clock.**

| Test | Time | What it covers |
|---|---|---|
| `TestPhase1_SingleProducerSingleConsumer` | 0.00s | End-to-end gRPC: one producer, one consumer, single partition; all messages arrive in order. |
| `TestPhase1_MultipleConsumers` | 0.00s | Two independent consumers each read the full stream from the same partition. |
| `TestPhase1_MultipleProducers` | 0.00s | Concurrent producers on disjoint partitions don't interfere with one another. |

## Phase 2 — coordinator + multi-broker (`tests/phase2_test.go`)

`go test ./tests/ -run Phase2 -v -timeout 60s` — **2 top-level / 5 subtests pass, 0.31s wall clock.**

| Test | Time | What it covers |
|---|---|---|
| `TestPhase2_PartitionRouting` | 0.02s | Top-level; spins up 4 in-process brokers + 1 coordinator. |
| └ `MetadataRoundRobin` | — | `Coordinator.CreateTopic` assigns partition `i` to broker `i % 4`. |
| └ `WrongBrokerNotFound` | — | Producing to a broker that doesn't own the partition surfaces `NotFound`. |
| └ `ProducerRoute` | — | `producer.Route` hashes the key, consults the coordinator-proxied metadata, and sends to the owning broker. |
| └ `ConsumerCrossBroker` | — | A grouped consumer joins, gets all 4 partitions, and successfully reads from each broker via independent `Fetch` streams. |
| `TestPhase2_ConsumerGroupCoordination` | 0.01s | Adding a second member to a group triggers rebalance — each member ends up with a disjoint partition slice. |

## Phase 3 — Raft + ISR + replication (`tests/phase3_test.go`)

`go test ./tests/ -run Phase3 -v -timeout 180s` — **9 / 9 pass, 61s wall clock.**

The slow cases (`ISRFollowerCrash`, `NonISRFollowerCrash`, `FollowerRejoinsISR`) each cost ~11s because they wait through the broker's hard-coded 10s ISR shrink timeout (`internal/broker/partition.go:163`).

| Test | Time | What it covers |
|---|---|---|
| `TestPhase3_LeaderElection` | 1.59s | Three in-process Raft nodes elect exactly one leader within the deadline. |
| `TestPhase3_NoDataLossOnCrash` | 23.40s | Apply two `CmdAssign` entries through the Raft leader, kill the leader, wait for a new leader, confirm both `PartitionState` entries survive on the new leader's FSM. |
| `TestPhase3_BrokerFailover` | 0.51s | Two brokers, manual leader/follower wiring; kill the leader, manually promote the follower via `AssignRole(LEADER)`, confirm all pre-crash messages still readable. |
| `TestPhase3_BrokerReplicationBasic` | 0.01s | Leader produces 10 messages; follower's `FetchReplica` stream catches up; follower's own consumer reads all 10 (HW advanced via replication). |
| `TestPhase3_HWBlocksUntilReplicated` | 0.21s | Without a follower in the ISR, `Fetch` blocks past the leader's tail; once the follower joins and acks, HW advances and the consumer unblocks. |
| `TestPhase3_NoDataLossonBrokerLeaderCrash` | 0.51s | Same as `BrokerFailover` from the data-loss angle: all pre-crash messages readable on the new leader. |
| `TestPhase3_ISRFollowerCrash` | 11.00s | Kill a follower that was in the ISR; leader's `monitorISR` drops it after 10s; recovery time is reported in `[METRIC]` log lines. |
| `TestPhase3_NonISRFollowerCrash` | 11.01s | Kill a follower that's already out of the ISR; produces are not slowed (zero impact confirmed). |
| `TestPhase3_FollowerRejoinsISR` | 12.21s | Force a follower out of the ISR, then resume replication; the follower rejoins the ISR and all messages remain consistent. |

## Chaos suite, 20 brokers × 5 coordinators (`tests/chaos_test.go`)

`go test ./tests/ -run Chaos -v -timeout 300s` — **6 / 6 pass, ~77s wall clock.**

Opt-in; not in CI. Each test spins up a fresh `chaosEnv` fixture (5 Raft coordinator nodes + 20 brokers, all in-process on TCP loopback). Test timings override the production defaults (heartbeat 200ms, `deadAfter` 1s, `leaderGrace` 2s, failover budget 6s) to keep wall-clock short.

| Test | Time | What it covers |
|---|---|---|
| `TestChaos_FollowerCrashShrinksISR` | ~13s | Kill one backup broker for a partition; within ~12s the FSM (via the Raft leader's `FSM().Get`) drops it from the ISR while a steady-drip producer keeps live followers in-sync. Skipped under `-short`. |
| `TestChaos_LeaderCrashAutoFailover` | ~5s | Kill the partition leader; the coordinator's auto-failover loop (broker heartbeats → highest-LEO selection → `UpdatePartitionLeader` → `CmdUpdateLeader` → role fan-out) promotes a surviving backup within the failover budget. Pre-crash data is preserved; fresh writes against the new leader succeed. |
| `TestChaos_CoordinatorLeaderCrashKeepsDataPlane` | ~35s | Kill the Raft leader coordinator; a new one is elected within 10s; producers with cached broker clients keep producing directly; FSM partition placement is preserved on the new leader (polled with a 3s retry budget to avoid racing `IsLeader()` vs FSM apply). |
| `TestChaos_CoordinatorLeaderCrashBrokerAutoReconnects` | ~14s | Kill the Raft leader coordinator; within one heartbeat interval (~200ms in chaos timings) every broker's `reconnectCoordinator` probes the peer list, finds the new leader, and re-registers. `CreateTopic` must succeed on the new leader within `chaosFailoverBudget` without any manual re-registration call. Replaces `TestChaos_CoordinatorLeaderCrashLosesBrokerRegistry` which documented the pre-fix limitation. |
| `TestChaos_CoordinatorQuorumLoss` | ~2s | Kill 3 of 5 coordinators; no surviving node can become leader; `CreateTopic` against any survivor fails with `FailedPrecondition`. Skipped under `-short`. |
| `TestChaos_RollingBrokerRestart` | ~9s | Kill 3 leader brokers in succession; auto-failover (not manual `UpdatePartitionLeader`) promotes a backup each time. Total messages produced across all rounds equal those consumed from the final leader — no data loss through repeated auto-failovers. Skipped under `-short`. |

### Tests intentionally deleted or replaced

- `TestChaos_LeaderCrashWithoutFailover` — asserted "leader dies, produces fail, no recovery." Now contradicted by auto-failover.
- `TestChaos_LeaderCrashWithManualFailover` — asserted "leader dies, test manually calls `UpdatePartitionLeader`, recovery succeeds." Now redundant; the failover loop is the sole caller.
- `TestChaos_CoordinatorLeaderCrashLosesBrokerRegistry` — documented that the new coordinator's in-memory broker registry was empty after a leader change and `CreateTopic` failed until brokers re-registered manually. Replaced by `TestChaos_CoordinatorLeaderCrashBrokerAutoReconnects`, which asserts the opposite: brokers self-heal within one heartbeat tick. Note: the registry is still NOT Raft-replicated — the new leader still starts empty — but brokers automatically re-register via `reconnectCoordinator`.

## CI

`.github/workflows/ci.yml` runs `go build ./...`, `go test -race ./internal/...`, and each of Phase 1, 2, 3 (with the same timeouts as above). Phase 4 stays opt-in — its 10s ISR timeouts and chaos timings cost more than the rest of CI combined.
