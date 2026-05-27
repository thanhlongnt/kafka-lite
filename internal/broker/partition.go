package broker

import (
	"context"
	"sync"

	"github.com/thanhlongnt/kafka-lite/internal/log"
	pb "github.com/thanhlongnt/kafka-lite/internal/proto/kafka_lite"
)

// Partition owns a single append-only log and is the unit of serialization.
// Concurrent produce/consume on different partitions does not block each other.
type Partition struct {
	log  log.Log
	cond *sync.Cond
}

func newPartition(l log.Log) *Partition {
	return &Partition{
		log:  l,
		cond: sync.NewCond(&sync.Mutex{}),
	}
}

// Append adds a message to the partition log and wakes any waiting Fetch goroutines.
func (p *Partition) Append(msg *pb.Message) (int64, error) {
	offset, err := p.log.Append(msg)
	if err != nil {
		return 0, err
	}
	p.cond.Broadcast()
	return offset, nil
}

// Read returns up to maxMsgs messages starting at offset.
func (p *Partition) Read(offset int64, maxMsgs int) ([]*pb.Message, error) {
	return p.log.Read(offset, maxMsgs)
}

// Len returns the current number of messages in the partition.
func (p *Partition) Len() int64 {
	return p.log.Len()
}

// WaitForData blocks until a message exists at index >= minLen, or ctx is cancelled.
// context.AfterFunc is used to broadcast on the cond when the context is done,
// which breaks the Wait loop so the caller can check ctx.Err().
func (p *Partition) WaitForData(ctx context.Context, minLen int64) {
	stop := context.AfterFunc(ctx, func() { p.cond.Broadcast() })
	defer stop()
	p.cond.L.Lock()
	for p.log.Len() <= minLen && ctx.Err() == nil {
		p.cond.Wait()
	}
	p.cond.L.Unlock()
}
