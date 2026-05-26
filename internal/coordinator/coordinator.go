package coordinator

import (
	"context"
	"fmt"
	"net"
	"sort"
	"sync"

	pb "github.com/thanhlongnt/kafka-lite/internal/proto/kafka_lite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/credentials/insecure"
)

type offsetKey struct {
	groupID string
	topic string
	partition int32
}

type coordinator struct {
	pb.UnimplementedCoordinatorServer

	mu      sync.RWMutex
	brokers []string // index-driven round robin
	topics map[string][]*pb.PartitionInfo //topic -> [{parittion, broker_addr}, ...]
	groups map[string]map[string][]int32 // group -> member -> partitions
	offsets map[offsetKey]int64 // (groupID, topic, partition) -> offset

	brokersMu sync.Mutex
	brokerConns map[string]pb.BrokerClient // addr -> client

	dialBroker func(ctx context.Context, addr string) (pb.BrokerClient, error) // for testing
}

// Coordinator is the exported type alias used by tests that need to embed the
// coordinator into a combined gRPC server.
type Coordinator = coordinator

func New() *coordinator {
	c := &coordinator{
		topics: make(map[string][]*pb.PartitionInfo),
		groups: make(map[string]map[string][]int32),
		offsets: make(map[offsetKey]int64),
		brokerConns: make(map[string]pb.BrokerClient),
	}
	c.dialBroker = func(_ context.Context, addr string) (pb.BrokerClient, error) {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		return pb.NewBrokerClient(conn), nil
	}
	return c
}

// NewExported is an alias for New, used when the caller needs the exported
// *Coordinator type (e.g. to pass it to pb.RegisterCoordinatorServer from a
// test package).
func NewExported() *Coordinator { return New() }

// Test helper to set the broker dialer function.
func (c *coordinator) SetBrokerDialer(fn func(ctx context.Context, addr string) (pb.BrokerClient, error)) {
	c.dialBroker = fn
}

// gRPC server setup
func (c *coordinator) Serve(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	s := grpc.NewServer()
	pb.RegisterCoordinatorServer(s, c)
	return s.Serve(lis)
}

// RegisterBroker adds a broker to the coordinator's metadata
func (c *coordinator) RegisterBroker(ctx context.Context, req *pb.RegisterBrokerRequest) (*pb.RegisterBrokerResponse, error) {
	if req.Addr == "" {
		return nil, status.Error(codes.InvalidArgument, "addr is required")
	}
	c.mu.Lock()
	// check duplicates
	for _, b := range c.brokers {
		if b == req.Addr {
			c.mu.Unlock()
			return &pb.RegisterBrokerResponse{}, nil
		}
	}
	c.brokers = append(c.brokers, req.Addr)
	c.mu.Unlock()

	// Open connection to broker
	c.brokersMu.Lock()
	if _, exists := c.brokerConns[req.Addr]; !exists {
		client, err := c.dialBroker(ctx, req.Addr)
		if err == nil {
			c.brokerConns[req.Addr] = client
		}
		// If dial fails, we still add the broker to the list, but it will be unavailable until a successful dial.
	}
	c.brokersMu.Unlock()

	return &pb.RegisterBrokerResponse{}, nil
}

// CreateTopic creates a topic with the specified number of partitions and assigns them to brokers in round-robin fashion.
func (c *coordinator) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	if req.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "topic name must not be empty")
	}
	if req.Partitions <= 0 {
		return nil, status.Error(codes.InvalidArgument, "partition count must be > 0")
	}

	c.mu.Lock()
	if _, exists := c.topics[req.Topic]; exists {
		c.mu.Unlock()
		return nil, status.Errorf(codes.AlreadyExists, "topic %q already exists", req.Topic)
	}
	if len(c.brokers) == 0 {
		c.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, "no brokers registered")
	}

	parts := make([]*pb.PartitionInfo, req.Partitions)
	for i := range parts {
		parts[i] = &pb.PartitionInfo{
			Partition: int32(i),
			BrokerAddr: c.brokers[i%len(c.brokers)],
		}
	}
	c.topics[req.Topic] = parts
	c.mu.Unlock()

	// Tell each broker which partition index it owns
	c.initPartitions(ctx, req.Topic, parts)
	return &pb.CreateTopicResponse{}, nil
}

// GetMetadata -> called by producers 
func (c *coordinator) GetMetadata(_ context.Context, req  *pb.MetadataRequest) (*pb.MetadataResponse, error) {
	c.mu.RLock()
	parts, ok := c.topics[req.Topic]
	c.mu.RUnlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "topic %q not found", req.Topic)
	}
	return &pb.MetadataResponse{Partitions: parts}, nil
}

// Join Group -> called by consumers to join a consumer group and get assigned partitions
func (c *coordinator) JoinGroup(_ context.Context, req *pb.JoinGroupRequest) (*pb.JoinGroupResponse, error) {
	if req.GroupId == "" || req.MemberId == "" || req.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "group_id, member_id, and topic are required")
	}
	
	c.mu.Lock()
	defer c.mu.Unlock()

	parts, ok := c.topics[req.Topic]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "topic %q not found", req.Topic)
	}
	if c.groups[req.GroupId] == nil {
		c.groups[req.GroupId] = make(map[string][]int32)
	}
	group := c.groups[req.GroupId]
	if _, exists := group[req.MemberId]; !exists {
		group[req.MemberId] = nil
	}
	// Rebalance partitions among members
	members := sortedKeys(group)
	total := int32(len(parts))
	for i, m := range(members) {
		group[m] = partitionRange(int32(i), int32(len(members)), total)
	}
	return &pb.JoinGroupResponse{AssignedPartitions: group[req.MemberId]}, nil
}

// CommitOffsets -> called by consumers to commit their current offsets for their assigned partitions
func (c *coordinator) CommitOffsets(_ context.Context, req *pb.CommitOffsetsRequest) (*pb.CommitOffsetsResponse, error) {
	c.mu.Lock()
	for _, po := range req.Offsets {
		key := offsetKey{req.GroupId, req.Topic, po.Partition}
		c.offsets[key] = po.Offset
	}
	c.mu.Unlock()
	return &pb.CommitOffsetsResponse{}, nil
}

// UpdatePartitionLeader -> called in Phase 3 for Hashicorp
func (c *coordinator) UpdatePartitionLeader(_ context.Context, req *pb.UpdatePartitionLeaderRequest) (*pb.UpdatePartitionLeaderResponse, error) {
	if req.Topic == "" || req.BrokerAddr == "" {
		return nil, status.Error(codes.InvalidArgument, "topic and broker_addr are required")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	parts, ok := c.topics[req.Topic]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "topic %q not found", req.Topic)
	}
	if req.Partition < 0 || int(req.Partition) >= len(parts) {
		return nil, status.Errorf(codes.InvalidArgument, "invalid partition index %d", req.Partition)
	}
	parts[req.Partition].BrokerAddr = req.BrokerAddr
	return &pb.UpdatePartitionLeaderResponse{}, nil
}

// Helpers

// initPartitions -> sends an InitPartition RPC to the broker that owns the partition to initialize it with the topic name and partition index
func (c *coordinator) initPartitions(ctx context.Context, topic string, parts []*pb.PartitionInfo) {
	// group indices by brokers that own them
	byBroker := make(map[string][]int32)
	for _, p := range parts {
		byBroker[p.BrokerAddr] = append(byBroker[p.BrokerAddr], p.Partition)
	}
	// grab lock, snapshot clients, release lock
	c.brokersMu.Lock()
	clients := make(map[string]pb.BrokerClient, len(byBroker))
	for addr := range byBroker {
		clients[addr] = c.brokerConns[addr]
	}
	c.brokersMu.Unlock()

	for addr, partitionIDs := range byBroker {
		cl := clients[addr]
		if cl == nil {
			continue // broker not registered, dial failed
		}
		_, _ = cl.InitPartitions(ctx, &pb.InitPartitionsRequest{
			Topic: topic,
			Partitions: partitionIDs,
		})
	}
}

// sortedKeys returns the sorted keys of a map. Used to ensure deterministic partition assignment order.
func sortedKeys(m map[string][]int32) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// partitionRange returns the partition indices assigned to a member given its index, total members, and total partitions.
func partitionRange(memberIdx, totalMembers, totalPartitions int32) []int32 {
	base := totalPartitions / totalMembers
	extra := totalPartitions % totalMembers
	start := memberIdx*base + min32(memberIdx, extra)
	count := base
	if memberIdx < extra {
		count++
	}
	out := make([]int32, count)
	for i := range out {
		out[i] = start + int32(i)
	}
	return out
}

func min32(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}