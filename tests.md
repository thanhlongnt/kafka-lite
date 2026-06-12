# Tests

Snapshot of all test suites in this repo, with their current results. Re-run with:

```bash
go test -race ./internal/...                          # unit tests
go test ./tests/ -run Phase1 -v -timeout 60s          # integration phase 1
go test ./tests/ -run Phase2 -v -timeout 60s          # integration phase 2
go test ./tests/ -run Phase3 -v -timeout 180s         # integration phase 3
go test ./tests/ -run Chaos -v -timeout 300s         # chaos / auto-failover
```

Last full run: 2026-06-11 on macOS (Darwin 25.5.0), Go 1.25.5. All suites clean; chaos suite 8/8.

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

`go test ./tests/ -run Phase3 -v -timeout 180s` — **9 / 9 pass, 56s wall clock.**

`ISRFollowerCrash` now runs in ~5s: the fast-path `scheduleReplicaEviction` goroutine fires after `max(2s, isrTimeout/2) = 5s` (default `isrTimeout=10s`), well before `monitorISR`'s 10s tick. `NonISRFollowerCrash` and `FollowerRejoinsISR` each cost ~11s because they still wait through the full 10s `monitorISR` cycle (no active `FetchReplica` stream to close, so no fast path).

| Test | Time | What it covers |
|---|---|---|
| `TestPhase3_LeaderElection` | 1.59s | Three in-process Raft nodes elect exactly one leader within the deadline. |
| `TestPhase3_NoDataLossOnCrash` | 23.40s | Apply two `CmdAssign` entries through the Raft leader, kill the leader, wait for a new leader, confirm both `PartitionState` entries survive on the new leader's FSM. |
| `TestPhase3_BrokerFailover` | 0.51s | Two brokers, manual leader/follower wiring; kill the leader, manually promote the follower via `AssignRole(LEADER)`, confirm all pre-crash messages still readable. |
| `TestPhase3_BrokerReplicationBasic` | 0.01s | Leader produces 10 messages; follower's `FetchReplica` stream catches up; follower's own consumer reads all 10 (HW advanced via replication). |
| `TestPhase3_HWBlocksUntilReplicated` | 0.21s | Without a follower in the ISR, `Fetch` blocks past the leader's tail; once the follower joins and acks, HW advances and the consumer unblocks. |
| `TestPhase3_NoDataLossonBrokerLeaderCrash` | 0.51s | Same as `BrokerFailover` from the data-loss angle: all pre-crash messages readable on the new leader. |
| `TestPhase3_ISRFollowerCrash` | 5.01s | Kill a follower that was in the ISR; `scheduleReplicaEviction` fast-path fires after 5s (`isrTimeout/2`); recovery time reported in `[METRIC]` log lines. |
| `TestPhase3_NonISRFollowerCrash` | 11.01s | Kill a follower that's already out of the ISR; produces are not slowed (zero impact confirmed). |
| `TestPhase3_FollowerRejoinsISR` | 12.21s | Force a follower out of the ISR, then resume replication; the follower rejoins the ISR and all messages remain consistent. |

## Chaos suite, 20 brokers × 5 coordinators (`tests/chaos_test.go`)

`go test ./tests/ -run Chaos -v -timeout 300s` — **8 / 8 pass, ~110s wall clock.**

Opt-in; not in CI. Each test spins up a fresh `chaosEnv` fixture (5 Raft coordinator nodes + 20 brokers, all in-process on TCP loopback). Test timings override production defaults: coordinator heartbeat tick 200ms, `deadAfter` 1s, `leaderGrace` 2s, broker heartbeat 200ms, ISR timeout 4s (2s fast-path eviction grace), failover budget 6s.

Note: most of each test's wall-clock time is `newChaosEnv` setup (25 TCP servers, Raft bootstrap, 20 broker registrations). The `[METRIC]` lines in the output show actual recovery elapsed time from the moment of the kill.

Tests are organized under the four failure scenarios from `docs/architecture.md`.

### Scenario 1 — Coordinator primary (Raft leader) dies

| Test | Wall time | Recovery time (measured) | What it verifies |
|---|---|---|---|
| `TestChaos_CoordinatorLeaderCrashKeepsDataPlane` | 44s | Raft election: **2.82s**; data-plane produce: **2.82s** (coordinator not involved) | Kill Raft leader coordinator; new leader elected; producers with cached broker addresses keep writing uninterrupted; FSM partition state preserved on new leader. |
| `TestChaos_CoordinatorLeaderCrashBrokerAutoReconnects` | 23s | Raft election: **2.51s**; CreateTopic on new leader: **2.75s** | After Raft leader change, brokers detect failed heartbeat → `reconnectCoordinator` probes peer list → finds new leader → `RegisterBroker`. `CreateTopic` succeeds without manual re-registration. |

**Observed recovery time:** Raft election ~2.5s; brokers reconnect within one heartbeat tick after that; CreateTopic available ~2.75s after kill.

### Scenario 2 — Coordinator backup (Raft follower) dies

| Test | Wall time | Recovery time (measured) | What it verifies |
|---|---|---|---|
| `TestChaos_CoordinatorBackupDies` | 22s | kill #1: **347ms**; kill #2: **341ms** (includes 300ms settle sleep) | Kill followers one at a time. After each kill (while quorum holds): Raft leader unchanged, `CreateTopic` succeeds, `Produce` succeeds. Stops at 2 follower kills (3 nodes alive) to leave quorum intact. |
| `TestChaos_CoordinatorQuorumLoss` | 1.5s | **0ms** — leader gone immediately on kill | Kill 3 of 5 coordinators; quorum lost; `CreateTopic` against survivors fails with `FailedPrecondition`. Data plane unaffected (broker-direct). Skipped under `-short`. |

**Observed behavior:** Killing 1–2 followers causes zero visible disruption — leader unchanged, CreateTopic+Produce still succeed within the 300ms settle window. Killing 3 breaks Raft commit; cluster management halts immediately.

### Scenario 3 — Broker primary (partition leader) dies

| Test | Wall time | Recovery time (measured) | What it verifies |
|---|---|---|---|
| `TestChaos_LeaderCrashAutoFailover` | 4.4s | **1.71s** | Kill partition leader; coordinator's heartbeat-silence path detects it (within `chaosDeadAfter=1s`); highest-LEO backup promoted; pre-crash messages preserved; fresh writes succeed. |
| `TestChaos_LeaderCrashFastDetection` | 4.1s | **2.42s** | Kill partition leader after a follower has an active `FetchReplica` stream. Follower's stream break triggers `ReportLeaderUnreachable`; coordinator fails over without waiting for `deadAfter`. |
| `TestChaos_RollingBrokerRestart` | 7s | round 0: **2.22s**; round 1: **1.21s**; round 2: **1.10s** | Kill 3 leader brokers in succession; auto-failover promotes a backup each time. All messages from all rounds readable from the final leader — no data loss across repeated failovers. Skipped under `-short`. |

**Observed recovery time:** 1.1–2.4s across all runs. The `ReportLeaderUnreachable` fast-path and the `deadAfter=1s` heartbeat path converge on similar latency at this scale.

### Scenario 4 — Broker backup (partition follower) dies

| Test | Wall time | Recovery time (measured) | What it verifies |
|---|---|---|---|
| `TestChaos_FollowerCrashShrinksISR` | 3.4s | ISR shrunk: **2.11s** | Kill a follower broker that has an active `FetchReplica` stream. Leader's `FetchReplica` handler close triggers `scheduleReplicaEviction` (2s grace); ISR shrinks in FSM; HW unfreezes. Produces against the leader continue throughout. Skipped under `-short`. |

**Observed recovery time:** 2.11s from kill to ISR shrunk in FSM. HW unfreezes at that point, unblocking any pending `Fetch` calls.

### Recovery timing summary

| Failure | Recovery time (measured) | Mechanism |
|---|---|---|
| Coordinator primary dies | **2.75s** to CreateTopic | Raft election ~2.5s; brokers reconnect on next heartbeat tick; `leaderGrace=2s` |
| Coordinator backup dies (quorum intact) | **<350ms** (no real disruption) | Leader unchanged; the settle sleep dominates |
| Coordinator backup dies (quorum lost) | **0ms** (immediate failure) | Raft writes fail; data plane unaffected |
| Broker primary dies | **1.1–2.4s** | `ReportLeaderUnreachable` from followers + `deadAfter=1s` heartbeat path |
| Broker backup dies (ISR shrink) | **2.11s** HW freeze | `scheduleReplicaEviction` fires after 2s grace on `FetchReplica` stream close |

### Tests intentionally deleted or replaced

- `TestChaos_LeaderCrashWithoutFailover` — asserted "leader dies, produces fail, no recovery." Now contradicted by auto-failover.
- `TestChaos_LeaderCrashWithManualFailover` — asserted "leader dies, test manually calls `UpdatePartitionLeader`, recovery succeeds." Now redundant; the failover loop is the sole caller.
- `TestChaos_CoordinatorLeaderCrashLosesBrokerRegistry` — documented that `CreateTopic` fails after a coordinator leader change until brokers re-register manually. Replaced by `TestChaos_CoordinatorLeaderCrashBrokerAutoReconnects`; brokers now self-heal via `reconnectCoordinator`. The registry is still not Raft-replicated — the new leader starts empty — but brokers re-register automatically within one heartbeat tick.

## CI

`.github/workflows/ci.yml` runs `go build ./...`, `go test -race ./internal/...`, and each of Phase 1, 2, 3 (with the same timeouts as above). The chaos suite stays opt-in — spinning up 20 brokers and 5 coordinator Raft nodes costs more than the rest of CI combined.
