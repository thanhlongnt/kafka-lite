package broker

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/thanhlongnt/kafka-lite/internal/log"
	pb "github.com/thanhlongnt/kafka-lite/internal/proto/kafka_lite"
)

// ErrNotLeader is returned by Append when this broker is not the partition leader.
var ErrNotLeader = errors.New("not leader for partition")

// ReplicaRole represents the role of a replica for a given partition.
type ReplicaRole int

const (
	Leader ReplicaRole = iota
	Follower
)

// ReplicaState holds the state of a follower replica from the leader's perspective.
type ReplicaState struct {
	brokerID      int32
	logEndOffset  int64
	lastFetchTime time.Time
	inSync        bool
}

// Partition owns a single append-only log and is the unit of serialization.
// Concurrent produce/consume on different partitions does not block each other.
type Partition struct {
	mu sync.RWMutex

	topic string
	id    int32

	log log.Log

	role        ReplicaRole
	leaderEpoch int64

	// Replication state
	hw           int64
	replicas     map[int32]*ReplicaState // Tracked by Leader
	leaderBroker int32                   // Tracked by Follower

	notifyCh chan struct{}
	cond     *sync.Cond

	// Background tasks
	bgCtx    context.Context
	bgCancel context.CancelFunc
	bgWg     sync.WaitGroup

	broker *Broker
}

func newPartition(topic string, id int32, l log.Log, b *Broker) *Partition {
	p := &Partition{
		topic:    topic,
		id:       id,
		log:      l,
		role:     Leader, // Defaults to Leader; Controller coordinate transitions
		hw:       l.LogEndOffset(), // Start with HW at end of log so empty partition is immediately readable
		replicas: make(map[int32]*ReplicaState),
		notifyCh: make(chan struct{}),
		broker:   b,
	}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// stopBackground stops any currently running background tasks (monitorISR or replicationLoop).
// It expects p.mu to be held by the caller.
func (p *Partition) stopBackground() {
	if p.bgCancel != nil {
		p.bgCancel()
		// Unlock to wait for goroutines to exit to avoid deadlock, then re-lock
		p.mu.Unlock()
		p.bgWg.Wait()
		p.mu.Lock()
		p.bgCancel = nil
	}
}

// BecomeLeader transitions the partition to the Leader role.
// initialISR is the set of broker IDs the coordinator considers in-sync at the
// time of assignment. Replicas are left empty here; each follower joins the ISR
// lazily via its first FetchReplica call (updateReplicaState). This lets HW
// advance immediately when no follower has yet connected (e.g. on fresh topics),
// while still converging to full replication once followers start fetching.
func (p *Partition) BecomeLeader(epoch int64, _ []int32) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Ignore older epochs to prevent stale transitions
	if epoch < p.leaderEpoch {
		return
	}

	p.stopBackground()

	p.role = Leader
	p.leaderEpoch = epoch
	p.replicas = make(map[int32]*ReplicaState)

	// Start leader background tasks
	p.bgCtx, p.bgCancel = context.WithCancel(context.Background())
	p.bgWg.Add(1)
	go p.monitorISR(p.bgCtx)
}

// BecomeFollower transitions the partition to the Follower role.
// TODO: [Controller/Metadata] This transition should be invoked by a LeaderAndIsr or UpdateMetadata request originating from the Controller.
func (p *Partition) BecomeFollower(leaderBroker int32, epoch int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Ignore older epochs
	if epoch < p.leaderEpoch {
		return
	}

	p.stopBackground()

	p.role = Follower
	p.leaderEpoch = epoch
	p.leaderBroker = leaderBroker

	// Start follower background tasks
	p.bgCtx, p.bgCancel = context.WithCancel(context.Background())
	p.bgWg.Add(1)
	go p.replicationLoop(p.bgCtx)
}

// Close gracefully stops the partition and any of its background tasks.
func (p *Partition) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stopBackground()
}

// monitorISR runs in the background while the partition is a Leader,
// tracking follower health and advancing the High Watermark (HW).
func (p *Partition) monitorISR(ctx context.Context) {
	defer p.bgWg.Done()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.mu.Lock()
			changed := false
			now := time.Now()
			for _, r := range p.replicas {
				if r.inSync && now.Sub(r.lastFetchTime) > 10*time.Second {
					// Fall out of ISR due to lag
					r.inSync = false
					changed = true
				}
			}

			if changed {
				// Collect current ISR broker IDs and epoch before reporting.
				isrIDs := p.currentISRIDs()
				epoch := p.leaderEpoch
				topic, partition := p.topic, p.id

				minISR := p.log.LogEndOffset()
				for _, r := range p.replicas {
					if r.inSync && r.logEndOffset < minISR {
						minISR = r.logEndOffset
					}
				}
				if minISR > p.hw {
					p.hw = minISR
					p.cond.Broadcast()
				}

				p.mu.Unlock()
				go p.broker.reportISRChange(topic, partition, isrIDs, epoch)
				p.mu.Lock() // re-acquire for the outer loop's defer-less Unlock below
			}
			p.mu.Unlock()
		}
	}
}

// replicationLoop runs in the background while the partition is a Follower,
// actively fetching replica data from the Leader.
func (p *Partition) replicationLoop(ctx context.Context) {
	defer p.bgWg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			p.mu.RLock()
			leaderBrokerID := p.leaderBroker
			brk := p.broker
			req := &pb.FetchReplicaRequest{
				Topic:       p.topic,
				Partition:   p.id,
				FetchOffset: p.log.LogEndOffset(),
				ReplicaId:   brk.id,
				LeaderEpoch: p.leaderEpoch,
			}
			p.mu.RUnlock()

			brk.mu.RLock()
			client, ok := brk.peerClients[leaderBrokerID]
			brk.mu.RUnlock()

			if !ok {
				// No peer client connected yet for leader, sleep and retry
				time.Sleep(1 * time.Second)
				continue
			}

			stream, err := client.FetchReplica(ctx, req)
			if err != nil {
				time.Sleep(1 * time.Second)
				continue
			}

			for {
				rec, err := stream.Recv()
				if err != nil {
					break // Stream ended or failed, restart outer loop
				}

				// Check for log divergence before appending
				if rec.Message.Offset != p.log.LogEndOffset() {
					if rec.Message.Offset < p.log.LogEndOffset() {
						_ = p.log.TruncateTo(rec.Message.Offset)
					}
				}

				err = p.AppendReplica(rec.Message)
				if err != nil {
					break
				}

				// Advance HW
				p.mu.Lock()
				if rec.HighWatermark > p.hw {
					p.hw = rec.HighWatermark
					p.cond.Broadcast()
				}
				p.mu.Unlock()
			}

			time.Sleep(500 * time.Millisecond) // Prevent busy spinning on disconnects
		}
	}
}

// Append adds a message to the partition log and wakes any waiting Fetch goroutines.
func (p *Partition) Append(msg *pb.Message) (int64, error) {
	p.mu.RLock()
	if p.role != Leader {
		p.mu.RUnlock()
		return 0, ErrNotLeader
	}
	p.mu.RUnlock()

	offset, err := p.log.Append(msg)
	if err != nil {
		return 0, err
	}
	// if no replicas, hw should advance immediately to make the message visible to consumers
	p.mu.Lock()
	if len(p.replicas) == 0 {
		p.hw = p.log.LogEndOffset()
	}
	p.mu.Unlock()
	p.cond.Broadcast()
	return offset, nil
}

// AppendReplica appending specifically via exact follower offset assignment.
func (p *Partition) AppendReplica(msg *pb.Message) error {
	err := p.log.AppendReplica(msg)
	if err != nil {
		return err
	}
	p.cond.Broadcast()
	return nil
}

// Read returns up to maxMsgs messages starting at offset.
func (p *Partition) Read(offset int64, maxMsgs int) ([]*pb.Message, error) {
	p.mu.RLock()
	hw := p.hw
	p.mu.RUnlock()

	if offset >= hw {
		return nil, nil // No committed data available yet
	}

	msgs, err := p.log.Read(offset, maxMsgs)
	if err != nil {
		return nil, err
	}

	// Clamp to HW
	var valid []*pb.Message
	for _, m := range msgs {
		if m.Offset < hw {
			valid = append(valid, m)
		} else {
			break
		}
	}
	return valid, nil
}

// HW returns the current High Watermark safely
func (p *Partition) HW() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.hw
}

// LeaderEpoch returns the current leader epoch safely
func (p *Partition) LeaderEpoch() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.leaderEpoch
}

// LogEndOffset returns the next offset to be appended (the count of records).
// Used by Broker.HeartbeatLoop to ship per-partition LEO to the coordinator
// for failover-candidate ranking.
func (p *Partition) LogEndOffset() int64 {
	return p.log.LogEndOffset()
}

// currentISRIDs returns the broker IDs of all in-sync replicas. Must be called with p.mu held.
func (p *Partition) currentISRIDs() []int32 {
	ids := make([]int32, 0, len(p.replicas))
	for id, r := range p.replicas {
		if r.inSync {
			ids = append(ids, id)
		}
	}
	return ids
}

// updateReplicaState is called by the Leader to record follower fetches
func (p *Partition) updateReplicaState(replicaID int32, fetchOffset int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.role != Leader {
		return
	}

	state, ok := p.replicas[replicaID]
	if !ok {
		state = &ReplicaState{brokerID: replicaID, inSync: true}
		p.replicas[replicaID] = state
	}
	state.logEndOffset = fetchOffset
	state.lastFetchTime = time.Now()

	// Check if a lagging follower has caught up to the leader's LEO
	if !state.inSync && state.logEndOffset >= p.log.LogEndOffset() {
		state.inSync = true
		isrIDs := p.currentISRIDs()
		epoch := p.leaderEpoch
		topic, partition := p.topic, p.id
		go p.broker.reportISRChange(topic, partition, isrIDs, epoch)
	}

	// Update High Watermark
	minISR := p.log.LogEndOffset()
	for _, r := range p.replicas {
		if r.inSync && r.logEndOffset < minISR {
			minISR = r.logEndOffset
		}
	}

	if minISR > p.hw {
		p.hw = minISR
		// Wake up consumers waiting on HW advancement
		p.cond.Broadcast()
	}
}

// ReadReplica circumvents HW clamping and allows the FetchReplica handler to drain raw local log messages.
func (p *Partition) ReadReplica(offset int64, maxMsgs int) ([]*pb.Message, error) {
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

// WaitForHW blocks until the High Watermark is greater than hw.
func (p *Partition) WaitForHW(ctx context.Context, hw int64) {
	stop := context.AfterFunc(ctx, func() { p.cond.Broadcast() })
	defer stop()
	p.cond.L.Lock()
	for p.hw <= hw && ctx.Err() == nil {
		p.cond.Wait()
	}
	p.cond.L.Unlock()
}
