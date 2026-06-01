package raft

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// Node wraps a Hashicorp Raft instance and exposes the operations the
// coordinator needs: Apply, Bootstrap, FSM, IsLeader, and LeaderAddr.
type Node struct {
	raft      *raft.Raft
	fsm       *FSM
	transport raft.Transport
}

// NewNode constructs and starts a Raft node.
//
//   - id       — unique server ID (e.g. "node1")
//   - bindAddr — TCP address this node listens on for Raft RPCs (e.g. "127.0.0.1:7000")
//   - dataDir  — directory for BoltDB stores and snapshots (created if absent)
func NewNode(id, bindAddr, dataDir string) (*Node, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("raft: mkdir %s: %w", dataDir, err)
	}

	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(id)

	logStore, err := raftboltdb.NewBoltStore(dataDir + "/raft-log.db")
	if err != nil {
		return nil, fmt.Errorf("raft: log store: %w", err)
	}

	stableStore, err := raftboltdb.NewBoltStore(dataDir + "/raft-stable.db")
	if err != nil {
		return nil, fmt.Errorf("raft: stable store: %w", err)
	}

	snapshots, err := raft.NewFileSnapshotStore(dataDir, 2, nil)
	if err != nil {
		return nil, fmt.Errorf("raft: snapshot store: %w", err)
	}

	transport, err := raft.NewTCPTransport(bindAddr, nil, 3, 10*time.Second, nil)
	if err != nil {
		return nil, fmt.Errorf("raft: transport: %w", err)
	}

	fsm := newFSM()
	r, err := raft.NewRaft(cfg, fsm, logStore, stableStore, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("raft: new raft: %w", err)
	}

	return &Node{raft: r, fsm: fsm, transport: transport}, nil
}

// Bootstrap configures the initial cluster membership. Call once on first
// startup; subsequent calls on a node that already has state are no-ops.
func (n *Node) Bootstrap(servers []raft.Server) error {
	cfg := raft.Configuration{Servers: servers}
	future := n.raft.BootstrapCluster(cfg)
	return future.Error()
}

// Apply serializes cmd as JSON and submits it through Raft.
// Returns an error if this node is not the leader or the apply times out.
func (n *Node) Apply(cmd Command) error {
	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("raft: marshal command: %w", err)
	}
	f := n.raft.Apply(data, 10*time.Second)
	if err := f.Error(); err != nil {
		return fmt.Errorf("raft: apply: %w", err)
	}
	if resp := f.Response(); resp != nil {
		if err, ok := resp.(error); ok {
			return err
		}
	}
	return nil
}

// FSM exposes read access to the FSM state for the coordinator.
func (n *Node) FSM() *FSM { return n.fsm }

// IsLeader reports whether this node is the current Raft leader.
func (n *Node) IsLeader() bool {
	return n.raft.State() == raft.Leader
}

// LeaderAddr returns the current leader's Raft address (empty if unknown).
func (n *Node) LeaderAddr() string {
	addr, _ := n.raft.LeaderWithID()
	return string(addr)
}

// Shutdown stops the Raft node gracefully.
func (n *Node) Shutdown() error {
	return n.raft.Shutdown().Error()
}
