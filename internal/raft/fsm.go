package raft

import (
	"encoding/json"
	"io"
	"log"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

// CmdType identifies the kind of mutation to apply to the FSM.
type CmdType uint8

const (
	CmdAssign      CmdType = iota // initial partition assignment
	CmdUpdateLeader               // leader changed after failover
	CmdUpdateISR                  // ISR membership changed (reported by leader broker)
)

// Command is serialized as JSON into raft.Log.Data.
type Command struct {
	Type      CmdType
	Topic     string
	Partition int32
	Primary   string
	Backups   []string
	Epoch     int64
	ISR       []int32 // broker IDs currently in sync (CmdUpdateISR)
}

type partitionKey struct {
	Topic     string
	Partition int32
}

// PartitionState holds the current primary and replica addresses for one partition.
type PartitionState struct {
	Primary string
	Backups []string
	Epoch   int64
	ISR     []int32 // broker IDs currently in sync; nil means all backups are assumed in sync
}

// FSM implements raft.FSM and maintains the partition assignment state.
type FSM struct {
	mu    sync.RWMutex
	state map[partitionKey]PartitionState
}

var _ raft.FSM = (*FSM)(nil)

func newFSM() *FSM {
	return &FSM{state: make(map[partitionKey]PartitionState)}
}

// Apply is called by the Raft library once a log entry is committed.
func (f *FSM) Apply(l *raft.Log) interface{} {
	start := time.Now()
	var cmd Command
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		return err
	}
	key := partitionKey{Topic: cmd.Topic, Partition: cmd.Partition}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch cmd.Type {
	case CmdAssign:
		f.state[key] = PartitionState{Primary: cmd.Primary, Backups: cmd.Backups, Epoch: cmd.Epoch}
	case CmdUpdateLeader:
		existing := f.state[key]
		existing.Primary = cmd.Primary
		existing.Backups = cmd.Backups
		existing.Epoch = cmd.Epoch
		f.state[key] = existing
	case CmdUpdateISR:
		existing := f.state[key]
		if cmd.Epoch >= existing.Epoch {
			existing.ISR = cmd.ISR
			f.state[key] = existing
		}
	}

	log.Printf("[FSM] index=%d type=%d topic=%s partition=%d applied in %v",
		l.Index, cmd.Type, cmd.Topic, cmd.Partition, time.Since(start))
	return nil
}

// Snapshot serializes the full state map so Raft can truncate old logs.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	copy := make(map[partitionKey]PartitionState, len(f.state))
	for k, v := range f.state {
		copy[k] = v
	}
	return &fsmSnapshot{state: copy}, nil
}

// Restore replaces the state map from a previously taken snapshot.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var s map[partitionKey]PartitionState
	if err := json.NewDecoder(rc).Decode(&s); err != nil {
		return err
	}
	f.mu.Lock()
	f.state = s
	f.mu.Unlock()
	return nil
}

// Get returns the partition state for (topic, partition), ok=false if absent.
func (f *FSM) Get(topic string, partition int32) (PartitionState, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	ps, ok := f.state[partitionKey{Topic: topic, Partition: partition}]
	return ps, ok
}

//getTopic returns all partition states for a topic, keyed by partition index
func (f *FSM) GetTopic(topic string) (map[int32]PartitionState, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	out := make(map[int32]PartitionState)
	for k, v := range f.state {
		if k.Topic == topic {
			out[k.Partition] = v
		}
	}
	return out, len(out) > 0
}
// fsmSnapshot implements raft.FSMSnapshot.
type fsmSnapshot struct {
	state map[partitionKey]PartitionState
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := json.NewEncoder(sink).Encode(s.state)
	if err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
