# Phase 3 Implementation Plan: Raft-Backed Coordinator

## Goal

Make the coordinator fault-tolerant by replicating partition assignment state
(`partition -> primary, backups`) across a Raft cluster using HashiCorp Raft.

---

## Architecture Decision

**Reuse the existing coordinator — do not rewrite from scratch.**

The existing `coordinator.go` already has the right shape:
- `topics map[string][]*pb.PartitionInfo` is the state that needs replication
- `UpdatePartitionLeader` was already stubbed for Phase 3
- The coordinator just becomes Raft-aware via composition

Changes:
- Embed a `*raft.Node` inside the coordinator struct
- Mutations go through `raftNode.Apply()` instead of direct map writes
- Reads serve from the local FSM state (no Raft round-trip needed)

---

## Step 1: Define FSM State and Commands

**File:** `internal/raft/fsm.go`

### State

```go
type partitionKey struct {
    Topic     string
    Partition int32
}

type PartitionState struct {
    Primary string   // primary broker addr
    Backups []string // replica broker addrs
}

type FSM struct {
    mu    sync.RWMutex
    state map[partitionKey]PartitionState
}
```

### Commands (serialized as JSON into raft.Log.Data)

```go
type CmdType uint8

const (
    CmdAssign      CmdType = iota // partition created, assign primary + backups
    CmdUpdateLeader               // leader changed after failover
)

type Command struct {
    Type      CmdType
    Topic     string
    Partition int32
    Primary   string
    Backups   []string
}
```

### FSM Methods

- `Apply(*raft.Log) interface{}` — unmarshal Command, update state map
- `Snapshot() (raft.FSMSnapshot, error)` — marshal full state map to JSON
- `Restore(io.ReadCloser) error` — replace state map from snapshot
- `Get(topic string, partition int32) (PartitionState, bool)` — read helper for coordinator

---

## Step 2: Implement the Raft Node

**File:** `internal/raft/node.go`

### Struct

```go
type Node struct {
    raft        *raft.Raft
    fsm         *FSM
    transport   raft.Transport
    logStore    raft.LogStore
    stableStore raft.StableStore
    snapshots   raft.SnapshotStore
}
```

### Constructor: `NewNode(id, bindAddr, dataDir string) (*Node, error)`

Wire together:
1. `raft.DefaultConfig()` with `LocalID = raft.ServerID(id)`
2. `raftboltdb.NewBoltStore` for log store and stable store (persisted to `dataDir`)
3. `raft.NewFileSnapshotStore` for snapshots
4. `raft.NewTCPTransport` on `bindAddr`
5. `raft.NewRaft(cfg, fsm, logStore, stableStore, snapshots, transport)`

### Key Methods

```go
// Apply sends a command through Raft. Only succeeds on the leader.
func (n *Node) Apply(cmd Command) error

// Bootstrap configures the initial single-node or multi-node cluster.
func (n *Node) Bootstrap(servers []raft.Server) error

// FSM exposes read access to the FSM state.
func (n *Node) FSM() *FSM

// IsLeader reports whether this node is the current Raft leader.
func (n *Node) IsLeader() bool

// LeaderAddr returns the current leader's address for redirect purposes.
func (n *Node) LeaderAddr() string
```

---

## Step 3: Integrate Raft into the Coordinator

**File:** `internal/coordinator/coordinator.go`

### Struct change

```go
type coordinator struct {
    // ... existing fields unchanged ...
    raftNode *raft.Node // add this
}
```

### CreateTopic — apply through Raft after building partition list

After building `parts []`, for each partition:
```go
c.raftNode.Apply(raft.Command{
    Type:      raft.CmdAssign,
    Topic:     req.Topic,
    Partition: int32(i),
    Primary:   parts[i].BrokerAddr,
    Backups:   selectBackups(c.brokers, parts[i].BrokerAddr, replicationFactor),
})
```

### UpdatePartitionLeader — apply through Raft

```go
return c.raftNode.Apply(raft.Command{
    Type:      raft.CmdUpdateLeader,
    Topic:     req.Topic,
    Partition: req.Partition,
    Primary:   req.BrokerAddr,
})
```

### GetMetadata — read from FSM

```go
ps, ok := c.raftNode.FSM().Get(req.Topic, partition)
// build PartitionInfo from ps.Primary
```

### Leader redirect

If `!c.raftNode.IsLeader()`, return a gRPC error with the leader address so
clients can retry against the correct node.

---

## Step 4: Replica Selection Helper

**File:** `internal/coordinator/coordinator.go` (or `internal/raft/util.go`)

```go
// selectBackups picks `n` brokers from the available list, excluding the primary.
func selectBackups(brokers []string, primary string, n int) []string
```

Simple strategy: round-robin through brokers, skip primary.

---

## Step 5: Wire Up the Coordinator Binary

**File:** `cmd/coordinator/main.go`

- Accept flags: `--id`, `--raft-addr`, `--raft-data-dir`, `--peers` (comma-separated `id=addr` pairs)
- Construct `raft.Node`, pass it to `coordinator.NewWithRaft(raftNode)`
- Bootstrap single-node or join existing cluster based on `--peers`

---

## Step 6: Update Phase 3 Tests

**File:** `tests/phase3_test.go`

### TestPhase3_LeaderElection
- Start 3 coordinator nodes
- Wait for a leader to emerge
- Assert `IsLeader()` is true on exactly one node

### TestPhase3_BrokerFailover
- Start 3 coordinator nodes + multiple brokers
- Create a topic (partition assigned to a primary)
- Kill the primary broker
- Assert the coordinator eventually calls `UpdatePartitionLeader` with a backup
- Assert producers can still write (to the new primary)

### TestPhase3_NoDataLossOnCrash
- Write messages to a topic
- Kill the leader coordinator
- Wait for re-election
- Assert all previously committed partition assignments are intact on the new leader

---

## File Change Summary

| File | Change |
|---|---|
| `internal/raft/fsm.go` | Implement FSM with state map, Apply, Snapshot, Restore |
| `internal/raft/node.go` | Implement Node with BoltDB stores, TCP transport, Apply/Bootstrap |
| `internal/coordinator/coordinator.go` | Embed `*raft.Node`, route mutations through Raft |
| `cmd/coordinator/main.go` | Add Raft flags and wiring |
| `tests/phase3_test.go` | Implement the three skipped tests |

---

## Dependencies to add to go.mod

```
github.com/hashicorp/raft
github.com/hashicorp/raft-boltdb/v2
go.etcd.io/bbolt
```
