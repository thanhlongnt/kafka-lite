package log

import (
	"fmt"
	"sync"

	pb "github.com/thanhlongnt/kafka-lite/internal/proto/kafka_lite"
)

// Log is the interface for an append-only message log for a single partition.
type Log interface {
	Append(msg *pb.Message) (int64, error)
	Read(offset int64, maxMsgs int) ([]*pb.Message, error)
	Len() int64
}

// MemLog is an in-memory implementation of Log.
// Offset equals the slice index — offset 0 is the first message appended.
type MemLog struct {
	mu   sync.RWMutex
	msgs []*pb.Message
}

// Append adds msg to the log and returns the offset it was assigned.
func (l *MemLog) Append(msg *pb.Message) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	offset := int64(len(l.msgs))
	msg.Offset = offset
	l.msgs = append(l.msgs, msg)
	return offset, nil
}

// Read returns up to maxMsgs messages starting at the given offset.
// Returns an error if offset is out of range.
func (l *MemLog) Read(offset int64, maxMsgs int) ([]*pb.Message, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	n := int64(len(l.msgs))
	if offset < 0 || offset > n {
		return nil, fmt.Errorf("offset %d out of range [0, %d]", offset, n)
	}
	end := offset + int64(maxMsgs)
	if end > n {
		end = n
	}
	result := make([]*pb.Message, end-offset)
	copy(result, l.msgs[offset:end])
	return result, nil
}

// Len returns the number of messages in the log.
func (l *MemLog) Len() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return int64(len(l.msgs))
}
