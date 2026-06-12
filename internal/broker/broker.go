package broker

import (
	"context"
	"errors"
	"fmt"
	stdlog "log"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/thanhlongnt/kafka-lite/internal/log"
	pb "github.com/thanhlongnt/kafka-lite/internal/proto/kafka_lite"
	"github.com/thanhlongnt/kafka-lite/internal/telemetry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Broker manages topics and their partitions and implements the BrokerServer gRPC interface.
type Broker struct {
	pb.UnimplementedBrokerServer
	mu       sync.RWMutex
	topics   map[string]map[int32]*Partition // sparse: only partitions this broker owns
	dataDir  string                          // empty → in-memory logs; non-empty → file-backed logs
	id       int32                           // Broker ID
	selfAddr string                          // own gRPC address, set by Serve

	peerClients map[int32]pb.BrokerClient
	peerConns   map[int32]*grpc.ClientConn
	peerAddrs   map[int32]string // broker ID → address (for ISR reporting)

	coordMu     sync.RWMutex
	coordClient pb.CoordinatorClient
	coordConn   *grpc.ClientConn
	coordPeers  []string // all known coordinator gRPC addresses for leader discovery

	// ISR eviction timings for partitions created by this broker.
	isrTimeout time.Duration // 0 → partition default (10s)
	isrTick    time.Duration // 0 → partition default (1s)
}

// New returns a Broker that stores all messages in memory.
func New(id int32) *Broker {
	return &Broker{
		topics:      make(map[string]map[int32]*Partition),
		id:          id,
		peerClients: make(map[int32]pb.BrokerClient),
		peerConns:   make(map[int32]*grpc.ClientConn),
		peerAddrs:   make(map[int32]string),
	}
}

// NewWithDataDir returns a Broker that persists each partition's log under
// dataDir/<topic>/<partition>/. Existing topics and partitions are restored
// automatically by scanning dataDir on startup.
func NewWithDataDir(id int32, dataDir string) (*Broker, error) {
	b := &Broker{
		topics:      make(map[string]map[int32]*Partition),
		dataDir:     dataDir,
		id:          id,
		peerClients: make(map[int32]pb.BrokerClient),
		peerConns:   make(map[int32]*grpc.ClientConn),
		peerAddrs:   make(map[int32]string),
	}
	if err := b.restore(); err != nil {
		return nil, fmt.Errorf("restore from %s: %w", dataDir, err)
	}
	return b, nil
}

// restore scans dataDir for existing topic/partition directories and opens a
// FileLog for each one, rebuilding the in-memory topics map.
func (b *Broker) restore() error {
	topicEntries, err := os.ReadDir(b.dataDir)
	if os.IsNotExist(err) {
		return nil // fresh start, nothing to restore
	}
	if err != nil {
		return err
	}

	for _, te := range topicEntries {
		if !te.IsDir() {
			continue
		}
		topic := te.Name()
		topicDir := filepath.Join(b.dataDir, topic)

		partEntries, err := os.ReadDir(topicDir)
		if err != nil {
			return err
		}

		// Collect valid numeric partition directories.
		type partEntry struct {
			idx int
			dir string
		}
		var parts []partEntry
		for _, pe := range partEntries {
			if !pe.IsDir() {
				continue
			}
			idx, err := strconv.Atoi(pe.Name())
			if err != nil {
				continue // skip non-numeric entries
			}
			parts = append(parts, partEntry{idx, filepath.Join(topicDir, pe.Name())})
		}
		if len(parts) == 0 {
			continue
		}
		sort.Slice(parts, func(i, j int) bool { return parts[i].idx < parts[j].idx })

		partitions := make(map[int32]*Partition, len(parts))
		for _, pe := range parts {
			fl, err := log.NewFileLog(pe.dir)
			if err != nil {
				return fmt.Errorf("open log %s: %w", pe.dir, err)
			}
			partitions[int32(pe.idx)] = newPartition(topic, int32(pe.idx), fl, b)
		}
		b.topics[topic] = partitions
	}
	return nil
}

// SetCoordinatorPeers stores the full list of coordinator gRPC addresses so the
// broker can discover the current Raft leader after a coordinator failover.
func (b *Broker) SetCoordinatorPeers(peers []string) {
	b.coordMu.Lock()
	b.coordPeers = peers
	b.coordMu.Unlock()
}

// SetSelfAddr sets the address this broker advertises to the coordinator.
// Serve sets it automatically; call this in test environments that skip Serve.
func (b *Broker) SetSelfAddr(addr string) {
	b.selfAddr = addr
}

// SetISRTimeout overrides the ISR eviction timeout and tick interval for all
// partitions subsequently created by this broker. Call before topic creation.
func (b *Broker) SetISRTimeout(timeout, tick time.Duration) {
	b.isrTimeout = timeout
	b.isrTick = tick
}

// ConnectCoordinator dials the coordinator and stores the client for proxying and registration.
// Phase 1 brokers never call this; coordClient stays nil.
func (b *Broker) ConnectCoordinator(ctx context.Context, addr string, opts ...grpc.DialOption) error {
	opts = append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, opts...)
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return fmt.Errorf("dial coordinator %s: %w", addr, err)
	}
	b.coordMu.Lock()
	if b.coordConn != nil {
		_ = b.coordConn.Close()
	}
	b.coordConn = conn
	b.coordClient = pb.NewCoordinatorClient(conn)
	b.coordMu.Unlock()
	return nil
}

// coordClientSafe returns the current coordinator client under coordMu.
func (b *Broker) coordClientSafe() pb.CoordinatorClient {
	b.coordMu.RLock()
	defer b.coordMu.RUnlock()
	return b.coordClient
}

// CreateTopic creates a topic with the given number of partitions (0..N-1).
// Used in Phase 1 standalone mode. Returns AlreadyExists if the topic already exists.
func (b *Broker) CreateTopic(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	if req.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "topic name must not be empty")
	}
	if req.Partitions <= 0 {
		return nil, status.Error(codes.InvalidArgument, "partition count must be > 0")
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.topics[req.Topic]; exists {
		return nil, status.Errorf(codes.AlreadyExists, "topic %q already exists", req.Topic)
	}

	parts := make(map[int32]*Partition, req.Partitions)
	for i := int32(0); i < req.Partitions; i++ {
		var l log.Log
		if b.dataDir != "" {
			dir := filepath.Join(b.dataDir, req.Topic, fmt.Sprintf("%d", i))
			fl, err := log.NewFileLog(dir)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "create file log: %v", err)
			}
			l = fl
		} else {
			l = &log.MemLog{}
		}
		parts[i] = newPartition(req.Topic, int32(i), l, b)
	}
	b.topics[req.Topic] = parts
	return &pb.CreateTopicResponse{}, nil
}

// InitPartitions creates partition entries for only the specified indices.
// Called by the coordinator in Phase 2 to tell this broker which partitions it owns.
// Safe to call multiple times (idempotent per partition index).
func (b *Broker) InitPartitions(_ context.Context, req *pb.InitPartitionsRequest) (*pb.InitPartitionsResponse, error) {
	if req.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "topic name must not be empty")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.topics[req.Topic] == nil {
		b.topics[req.Topic] = make(map[int32]*Partition)
	}
	for _, pid := range req.Partitions {
		if _, exists := b.topics[req.Topic][pid]; !exists {
			var l log.Log
			if b.dataDir != "" {
				dir := filepath.Join(b.dataDir, req.Topic, fmt.Sprintf("%d", pid))
				fl, err := log.NewFileLog(dir)
				if err != nil {
					return nil, status.Errorf(codes.Internal, "create file log: %v", err)
				}
				l = fl
			} else {
				l = &log.MemLog{}
			}
			b.topics[req.Topic][pid] = newPartition(req.Topic, pid, l, b)
		}
	}
	return &pb.InitPartitionsResponse{}, nil
}

// Produce appends a message to the specified topic/partition and returns the assigned offset.
func (b *Broker) Produce(_ context.Context, req *pb.ProduceRequest) (*pb.ProduceResponse, error) {
	p, err := b.getPartition(req.Topic, req.Partition)
	if err != nil {
		return nil, err
	}

	msg := &pb.Message{
		Topic:     req.Topic,
		Partition: req.Partition,
		Key:       req.Key,
		Value:     req.Value,
	}
	offset, err := p.Append(msg)
	if err != nil {
		if errors.Is(err, ErrNotLeader) {
			return nil, status.Errorf(codes.FailedPrecondition, "not leader for partition %d", req.Partition)
		}
		return nil, status.Errorf(codes.Internal, "append failed: %v", err)
	}
	return &pb.ProduceResponse{Offset: offset}, nil
}

// Fetch streams messages from the specified topic/partition starting at start_offset.
// It sends existing messages immediately, then blocks and pushes new messages as they arrive.
// The stream continues until the client cancels.
func (b *Broker) Fetch(req *pb.FetchRequest, stream pb.Broker_FetchServer) error {
	p, err := b.getPartition(req.Topic, req.Partition)
	if err != nil {
		return err
	}

	ctx := stream.Context()
	offset := req.StartOffset
	const batchSize = 64

	for {
		msgs, err := p.Read(offset, batchSize)
		if err != nil {
			return status.Errorf(codes.Internal, "read failed: %v", err)
		}

		for _, msg := range msgs {
			if err := stream.Send(msg); err != nil {
				return err
			}
			offset++
		}

		if len(msgs) > 0 {
			// More might be available — loop immediately before waiting.
			continue
		}

		// Block until HW advances or client cancels.
		p.WaitForHW(ctx, offset)
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

func (b *Broker) FetchReplica(req *pb.FetchReplicaRequest, stream pb.Broker_FetchReplicaServer) error {
	p, err := b.getPartition(req.Topic, req.Partition)
	if err != nil {
		return err
	}

	// When the follower stream closes (follower died or disconnected), schedule
	// a fast-path ISR eviction. The grace window (half of isrTimeout, min 2s)
	// allows the follower to reconnect before being evicted.
	evictGrace := p.isrTimeout / 2
	if evictGrace < 2*time.Second {
		evictGrace = 2 * time.Second
	}
	defer func() { go p.scheduleReplicaEviction(req.ReplicaId, evictGrace) }()

	p.updateReplicaState(req.ReplicaId, req.FetchOffset)

	ctx := stream.Context()
	offset := req.FetchOffset
	const batchSize = 64

	for {
		msgs, err := p.ReadReplica(offset, batchSize)
		if err != nil {
			return status.Errorf(codes.Internal, "read failed: %v", err)
		}

		for _, msg := range msgs {
			if err := stream.Send(&pb.ReplicaRecord{
				Message:       msg,
				HighWatermark: p.HW(),
				LeaderEpoch:   p.LeaderEpoch(),
			}); err != nil {
				return err
			}
			offset++
		}

		if len(msgs) > 0 {
			p.updateReplicaState(req.ReplicaId, offset)
			continue
		}

		p.WaitForData(ctx, offset)
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

// AssignRole is called by the coordinator to make this broker the leader or a
// follower for the given partition. For FOLLOWER, the broker first dials the
// leader peer if not already connected.
func (b *Broker) AssignRole(_ context.Context, req *pb.AssignRoleRequest) (*pb.AssignRoleResponse, error) {
	p, err := b.getPartition(req.Topic, req.Partition)
	if err != nil {
		return nil, err
	}
	switch req.Role {
	case pb.ReplicaRole_LEADER:
		p.BecomeLeader(req.Epoch, req.IsrIds)
	case pb.ReplicaRole_FOLLOWER:
		if err := b.ConnectToPeer(req.LeaderId, req.LeaderAddr); err != nil {
			return nil, status.Errorf(codes.Internal, "connect to leader peer: %v", err)
		}
		p.BecomeFollower(req.LeaderId, req.Epoch)
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unknown role %v", req.Role)
	}
	return &pb.AssignRoleResponse{}, nil
}

// Serve starts the gRPC server on addr and blocks until it exits.
// If a coordinator client is configured, also registers a CoordinatorServer proxy so
// consumers can call JoinGroup/CommitOffsets/GetMetadata on the broker address directly.
func (b *Broker) Serve(addr string, rpcLogger *stdlog.Logger) error {
	if b.selfAddr == "" {
		b.selfAddr = addr
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}

	opts := []grpc.ServerOption{}
	if rpcLogger != nil {
		opts = append(opts, grpc.UnaryInterceptor(telemetry.LoggingUnaryInterceptor(rpcLogger)))
		opts = append(opts, grpc.StreamInterceptor(telemetry.LoggingStreamInterceptor(rpcLogger)))
	}

	srv := grpc.NewServer(opts...)
	pb.RegisterBrokerServer(srv, b)
	if b.coordClientSafe() != nil {
		pb.RegisterCoordinatorServer(srv, &coordinatorProxy{b: b})

		// TODO: [Controller/Metadata] When the broker connects to the coordinator,
		// it should fetch the initial cluster metadata (broker directory, partitions, ISRs)
		// so it knows which partitions to spin up and which peer connections to open.
	}
	return srv.Serve(lis)
}

// getPartition looks up a partition by topic and partition index.
func (b *Broker) getPartition(topic string, partition int32) (*Partition, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	parts, ok := b.topics[topic]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "topic %q not found", topic)
	}
	p, ok := parts[partition]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "partition %d not found in topic %q", partition, topic)
	}
	return p, nil
}

// coordinatorProxy forwards JoinGroup, CommitOffsets, and GetMetadata to the real coordinator.
// It is registered on the broker's gRPC server so consumers never need the coordinator's address.
// It holds a pointer to the broker so it always uses the current coordinator client, even after
// the broker reconnects to a new Raft leader.
type coordinatorProxy struct {
	pb.UnimplementedCoordinatorServer
	b *Broker
}

func (p *coordinatorProxy) JoinGroup(ctx context.Context, req *pb.JoinGroupRequest) (*pb.JoinGroupResponse, error) {
	c := p.b.coordClientSafe()
	if c == nil {
		return nil, status.Error(codes.Unavailable, "no coordinator connected")
	}
	return c.JoinGroup(ctx, req)
}

func (p *coordinatorProxy) CommitOffsets(ctx context.Context, req *pb.CommitOffsetsRequest) (*pb.CommitOffsetsResponse, error) {
	c := p.b.coordClientSafe()
	if c == nil {
		return nil, status.Error(codes.Unavailable, "no coordinator connected")
	}
	return c.CommitOffsets(ctx, req)
}

func (p *coordinatorProxy) GetMetadata(ctx context.Context, req *pb.MetadataRequest) (*pb.MetadataResponse, error) {
	c := p.b.coordClientSafe()
	if c == nil {
		return nil, status.Error(codes.Unavailable, "no coordinator connected")
	}
	return c.GetMetadata(ctx, req)
}

func (p *coordinatorProxy) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	c := p.b.coordClientSafe()
	if c == nil {
		return nil, status.Error(codes.Unavailable, "no coordinator connected")
	}
	return c.Heartbeat(ctx, req)
}

// ConnectToPeer dials a peer broker and caches the gRPC client for replication.
func (b *Broker) ConnectToPeer(peerID int32, addr string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Skip if already connected
	if _, exists := b.peerClients[peerID]; exists {
		return nil
	}

	// Dial the peer broker
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to dial peer %d at %s: %w", peerID, addr, err)
	}

	// Cache the raw connection for teardown and the stub for replication
	b.peerConns[peerID] = conn
	b.peerClients[peerID] = pb.NewBrokerClient(conn)
	b.peerAddrs[peerID] = addr

	return nil
}

// HeartbeatLoop sends a Heartbeat to the coordinator every `interval`, carrying
// this broker's per-partition LogEndOffsets. The coordinator uses the cadence to
// detect dead brokers and the LEO snapshot to pick the most-caught-up backup
// when auto-promoting a new partition leader.
//
// The loop exits when ctx is canceled. Heartbeat RPC errors are swallowed — the
// coordinator's failover loop is the source of truth for liveness, and a flaky
// network just shows up as a missed heartbeat.
func (b *Broker) HeartbeatLoop(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		b.sendHeartbeat(ctx)
	}
}

func (b *Broker) sendHeartbeat(ctx context.Context) {
	c := b.coordClientSafe()
	if c == nil {
		return
	}

	b.mu.RLock()
	leos := b.buildPartitionLEOs()
	addr := b.selfAddr
	b.mu.RUnlock()

	_, err := c.Heartbeat(ctx, &pb.HeartbeatRequest{
		BrokerId:      b.id,
		BrokerAddr:    addr,
		PartitionLeos: leos,
	})
	if err != nil {
		b.reconnectCoordinator(ctx)
	}
}

// buildPartitionLEOs returns the current LEO snapshot for all local partitions.
// Must be called with b.mu held (at least RLock).
func (b *Broker) buildPartitionLEOs() []*pb.PartitionLEO {
	leos := make([]*pb.PartitionLEO, 0, len(b.topics))
	for topic, parts := range b.topics {
		for pid, p := range parts {
			leos = append(leos, &pb.PartitionLEO{
				Topic:        topic,
				Partition:    pid,
				LogEndOffset: p.LogEndOffset(),
			})
		}
	}
	return leos
}

// reconnectCoordinator cycles through coordPeers to find the current Raft leader,
// then commits the connection and re-registers this broker. Called on any heartbeat
// error so the broker self-heals after a coordinator failover.
func (b *Broker) reconnectCoordinator(ctx context.Context) {
	b.coordMu.RLock()
	peers := make([]string, len(b.coordPeers))
	copy(peers, b.coordPeers)
	b.coordMu.RUnlock()

	if len(peers) == 0 {
		return
	}

	addr := b.selfAddr
	b.mu.RLock()
	leos := b.buildPartitionLEOs()
	b.mu.RUnlock()

	req := &pb.HeartbeatRequest{BrokerId: b.id, BrokerAddr: addr, PartitionLeos: leos}

	for _, peer := range peers {
		conn, err := grpc.NewClient(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}
		client := pb.NewCoordinatorClient(conn)
		probeCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		_, err = client.Heartbeat(probeCtx, req)
		cancel()
		if err != nil {
			conn.Close()
			continue
		}
		// This peer is the current leader — commit the connection.
		b.coordMu.Lock()
		if b.coordConn != nil {
			b.coordConn.Close()
		}
		b.coordConn = conn
		b.coordClient = client
		b.coordMu.Unlock()
		// Re-register since the new leader has an empty broker registry.
		_, _ = client.RegisterBroker(ctx, &pb.RegisterBrokerRequest{Addr: addr, Id: b.id})
		return
	}
}

// reportISRChange calls AlterIsr on the coordinator to persist the current ISR.
// It is called from partition background goroutines and must not hold p.mu.
func (b *Broker) reportISRChange(topic string, partition int32, isrIDs []int32, epoch int64) {
	c := b.coordClientSafe()
	if c == nil {
		return
	}
	_, _ = c.AlterIsr(context.Background(), &pb.AlterIsrRequest{
		Topic:     topic,
		Partition: partition,
		Epoch:     epoch,
		IsrIds:    isrIDs,
	})
}

// reportLeaderUnreachable notifies the coordinator that the leader for a partition
// is unreachable. Called by replicationLoop when a FetchReplica stream breaks.
// Best-effort: errors are silently ignored; runFailoverLoop is the safety net.
func (b *Broker) reportLeaderUnreachable(topic string, partition int32, leaderBrokerID int32) {
	c := b.coordClientSafe()
	if c == nil {
		return
	}
	b.mu.RLock()
	leaderAddr := b.peerAddrs[leaderBrokerID]
	b.mu.RUnlock()
	if leaderAddr == "" {
		return
	}
	_, _ = c.ReportLeaderUnreachable(context.Background(), &pb.ReportLeaderUnreachableRequest{
		LeaderAddr: leaderAddr,
		Topic:      topic,
		Partition:  partition,
	})
}

// Close strictly tears down all peer connections and local partitions.
func (b *Broker) Close() {
	b.mu.Lock()
	for _, conn := range b.peerConns {
		conn.Close()
	}
	for _, parts := range b.topics {
		for _, p := range parts {
			p.Close()
		}
	}
	b.mu.Unlock()

	b.coordMu.Lock()
	if b.coordConn != nil {
		b.coordConn.Close()
		b.coordConn = nil
		b.coordClient = nil
	}
	b.coordMu.Unlock()
}
