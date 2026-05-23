package raft

import (
	"io"

	"github.com/hashicorp/raft"
)

// FSM implements the raft.FSM interface, applying committed log entries to partition state.
type FSM struct {
	// TODO: Phase 3 — reference to broker partitions to apply writes
}

var _ raft.FSM = (*FSM)(nil)

func (f *FSM) Apply(l *raft.Log) interface{} {
	// TODO: Phase 3 — decode and apply log entry to the partition log
	return nil
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	// TODO: Phase 3 — snapshot current log state
	return nil, nil
}

func (f *FSM) Restore(snapshot io.ReadCloser) error {
	// TODO: Phase 3 — restore log state from snapshot
	return nil
}
