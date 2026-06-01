package broker

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"

	"github.com/thanhlongnt/kafka-lite/internal/log"
	pb "github.com/thanhlongnt/kafka-lite/internal/proto/kafka_lite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Broker manages topics and their partitions and implements the BrokerServer gRPC interface.
type Broker struct {
	pb.UnimplementedBrokerServer
	mu      sync.RWMutex
	topics  map[string]map[int32]*Partition // sparse: only partitions this broker owns
	dataDir string                          // empty → in-memory logs; non-empty → file-backed logs
	id      int32                           // Broker ID

	peerClients map[int32]pb.BrokerClient
	peerConns   map[int32]*grpc.ClientConn

	coordClient pb.CoordinatorClient
	coordConn   *grpc.ClientConn
}

// New returns a Broker that stores all messages in memory.
func New(id int32) *Broker {
	return &Broker{
		topics:      make(map[string]map[int32]*Partition),
		id:          id,
		peerClients: make(map[int32]pb.BrokerClient),
		peerConns:   make(map[int32]*grpc.ClientConn),
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

// ConnectCoordinator dials the coordinator and stores the client for proxying and registration.
// Phase 1 brokers never call this; coordClient stays nil.
func (b *Broker) ConnectCoordinator(ctx context.Context, addr string, opts ...grpc.DialOption) error {
	if b.coordConn != nil {
		_ = b.coordConn.Close()
	}
	opts = append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, opts...)
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return fmt.Errorf("dial coordinator %s: %w", addr, err)
	}
	b.coordConn = conn
	b.coordClient = pb.NewCoordinatorClient(conn)
	return nil
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

// Serve starts the gRPC server on addr and blocks until it exits.
// If a coordinator client is configured, also registers a CoordinatorServer proxy so
// consumers can call JoinGroup/CommitOffsets/GetMetadata on the broker address directly.
func (b *Broker) Serve(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	srv := grpc.NewServer()
	pb.RegisterBrokerServer(srv, b)
	if b.coordClient != nil {
		pb.RegisterCoordinatorServer(srv, &coordinatorProxy{client: b.coordClient})

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
type coordinatorProxy struct {
	pb.UnimplementedCoordinatorServer
	client pb.CoordinatorClient
}

func (p *coordinatorProxy) JoinGroup(ctx context.Context, req *pb.JoinGroupRequest) (*pb.JoinGroupResponse, error) {
	return p.client.JoinGroup(ctx, req)
}

func (p *coordinatorProxy) CommitOffsets(ctx context.Context, req *pb.CommitOffsetsRequest) (*pb.CommitOffsetsResponse, error) {
	return p.client.CommitOffsets(ctx, req)
}

func (p *coordinatorProxy) GetMetadata(ctx context.Context, req *pb.MetadataRequest) (*pb.MetadataResponse, error) {
	return p.client.GetMetadata(ctx, req)
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

	return nil
}

// Close strictly tears down all peer connections and local partitions.
func (b *Broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, conn := range b.peerConns {
		conn.Close()
	}

	for _, parts := range b.topics {
		for _, p := range parts {
			p.Close()
		}
	}

	if b.coordConn != nil {
		b.coordConn.Close()
	}
}
