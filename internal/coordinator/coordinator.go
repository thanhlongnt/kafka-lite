package coordinator

import (
	"context"
	"fmt"
	"net"
	"sort"
	"sync"

	kafkaraft "github.com/thanhlongnt/kafka-lite/internal/raft"
	pb "github.com/thanhlongnt/kafka-lite/internal/proto/kafka_lite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type offsetKey struct {
	groupID   string
	topic     string
	partition int32
}

type coordinator struct {
	pb.UnimplementedCoordinatorServer

	mu         sync.RWMutex
	brokers    []string                       // index-driven round robin
	brokerIDs  map[string]int32               // addr -> broker ID
	brokerAddrs map[int32]string              // broker ID -> addr (reverse of brokerIDs)
	topics     map[string][]*pb.PartitionInfo // topic -> [{partition, broker_addr}, ...]
	groups     map[string]map[string][]int32  // group -> member -> partitions
	offsets    map[offsetKey]int64            // (groupID, topic, partition) -> offset

	brokersMu   sync.Mutex
	brokerConns map[string]pb.BrokerClient // addr -> client

	raftNode   *kafkaraft.Node
	dialBroker func(ctx context.Context, addr string) (pb.BrokerClient, error) // for testing
}

// partitionAssignment captures the full primary+backup layout for one partition.
type partitionAssignment struct {
	partition int32
	primary   string
	backups   []string
}

// Coordinator is the exported type alias used by tests that need to embed the
// coordinator into a combined gRPC server.
type Coordinator = coordinator

func New() *coordinator {
	c := &coordinator{
		brokerIDs:   make(map[string]int32),
		brokerAddrs: make(map[int32]string),
		topics:      make(map[string][]*pb.PartitionInfo),
		groups:      make(map[string]map[string][]int32),
		offsets:     make(map[offsetKey]int64),
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

// NewWithRaft creates coordinator with Raft node as a parameter
func NewWithRaft(raftNode *kafkaraft.Node) *coordinator {
	c := New()
	c.raftNode = raftNode
	return c
}
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
	c.brokerIDs[req.Addr] = req.Id
	c.brokerAddrs[req.Id] = req.Addr
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
	if c.raftNode != nil && !c.raftNode.IsLeader() {
		return nil, status.Errorf(codes.FailedPrecondition, "not the leader; redirect to %s", c.raftNode.LeaderAddr())
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
	brokers := make([]string, len(c.brokers)) // snapshot before unlocking
	copy(brokers, c.brokers)

	// Compute primary + backup assignments for every partition.
	assignments := make([]partitionAssignment, req.Partitions)
	for i := range assignments {
		primary := brokers[i%len(brokers)]
		assignments[i] = partitionAssignment{
			partition: int32(i),
			primary:   primary,
			backups:   selectBackups(brokers, primary, len(brokers)-1),
		}
	}

	parts := make([]*pb.PartitionInfo, len(assignments))
	for i, a := range assignments {
		parts[i] = &pb.PartitionInfo{Partition: a.partition, BrokerAddr: a.primary}
	}
	c.topics[req.Topic] = parts
	c.mu.Unlock()

	if c.raftNode != nil {
		for _, a := range assignments {
			if err := c.raftNode.Apply(kafkaraft.Command{
				Type:      kafkaraft.CmdAssign,
				Topic:     req.Topic,
				Partition: a.partition,
				Primary:   a.primary,
				Backups:   a.backups,
				Epoch:     1,
			}); err != nil {
				return nil, status.Errorf(codes.Internal, "raft apply: %v", err)
			}
		}
	}

	// Tell primary brokers which partitions they own.
	c.initPartitions(ctx, req.Topic, parts)
	// Tell backup brokers which partitions they will replicate.
	c.initBackupPartitions(ctx, req.Topic, assignments)
	// Assign leader/follower roles so replication loops start.
	c.assignLeaderRoles(ctx, req.Topic, assignments, 1)
	c.assignFollowerRoles(ctx, req.Topic, assignments, 1)

	return &pb.CreateTopicResponse{}, nil
}

// GetMetadata -> called by producers 
func (c *coordinator) GetMetadata(_ context.Context, req *pb.MetadataRequest) (*pb.MetadataResponse, error) {
	// If we're using Raft, read partition info from the FSM state. Otherwise, read from in-memory map.
	if c.raftNode != nil {
		byPart, ok := c.raftNode.FSM().GetTopic(req.Topic)
		if !ok {
			return nil, status.Errorf(codes.NotFound, "topic %q not found", req.Topic)
		}
		parts := make([]*pb.PartitionInfo, 0, len(byPart))
		for idx, ps := range byPart {
			parts = append(parts, &pb.PartitionInfo{
				Partition: idx,
				BrokerAddr: ps.Primary,
			})
		}
		sort.Slice(parts, func(i, j int) bool {
			return parts[i].Partition < parts[j].Partition
		})
		return &pb.MetadataResponse{Partitions: parts}, nil
	}
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
	for i, m := range members {
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
func (c *coordinator) UpdatePartitionLeader(ctx context.Context, req *pb.UpdatePartitionLeaderRequest) (*pb.UpdatePartitionLeaderResponse, error) {
	if req.Topic == "" || req.BrokerAddr == "" {
		return nil, status.Error(codes.InvalidArgument, "topic and broker_addr are required")
	}
	if c.raftNode != nil && !c.raftNode.IsLeader() {
		return nil, status.Errorf(codes.FailedPrecondition, "not the leader; redirect to %s", c.raftNode.LeaderAddr())
	}

	// Determine new epoch and remaining backups from current FSM state.
	var newEpoch int64 = 1
	var remainingBackups []string
	if c.raftNode != nil {
		if ps, ok := c.raftNode.FSM().Get(req.Topic, req.Partition); ok {
			newEpoch = ps.Epoch + 1
			// New backups: remove promoted broker, add old primary as a backup.
			for _, b := range ps.Backups {
				if b != req.BrokerAddr {
					remainingBackups = append(remainingBackups, b)
				}
			}
			if ps.Primary != req.BrokerAddr {
				remainingBackups = append(remainingBackups, ps.Primary)
			}
		}
		if err := c.raftNode.Apply(kafkaraft.Command{
			Type:      kafkaraft.CmdUpdateLeader,
			Topic:     req.Topic,
			Partition: req.Partition,
			Primary:   req.BrokerAddr,
			Backups:   remainingBackups,
			Epoch:     newEpoch,
		}); err != nil {
			return nil, status.Errorf(codes.Internal, "raft apply: %v", err)
		}
	}

	c.mu.Lock()
	parts, ok := c.topics[req.Topic]
	if !ok {
		c.mu.Unlock()
		return nil, status.Errorf(codes.NotFound, "topic %q not found", req.Topic)
	}
	if req.Partition < 0 || int(req.Partition) >= len(parts) {
		c.mu.Unlock()
		return nil, status.Errorf(codes.InvalidArgument, "invalid partition index %d", req.Partition)
	}
	parts[req.Partition].BrokerAddr = req.BrokerAddr
	c.mu.Unlock()

	// Tell the new leader to take the leader role.
	a := partitionAssignment{
		partition: req.Partition,
		primary:   req.BrokerAddr,
		backups:   remainingBackups,
	}
	c.assignLeaderRoles(ctx, req.Topic, []partitionAssignment{a}, newEpoch)
	c.assignFollowerRoles(ctx, req.Topic, []partitionAssignment{a}, newEpoch)

	return &pb.UpdatePartitionLeaderResponse{}, nil
}

// AlterIsr is called by the partition leader to persist an ISR change through Raft.
// Stale updates (epoch < current) are silently ignored.
func (c *coordinator) AlterIsr(_ context.Context, req *pb.AlterIsrRequest) (*pb.AlterIsrResponse, error) {
	if req.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "topic is required")
	}
	if c.raftNode != nil && !c.raftNode.IsLeader() {
		return nil, status.Errorf(codes.FailedPrecondition, "not the raft leader; redirect to %s", c.raftNode.LeaderAddr())
	}
	if c.raftNode != nil {
		if ps, ok := c.raftNode.FSM().Get(req.Topic, req.Partition); ok && req.Epoch < ps.Epoch {
			// Stale report — ignore without error so the broker doesn't retry in a loop.
			return &pb.AlterIsrResponse{}, nil
		}
		if err := c.raftNode.Apply(kafkaraft.Command{
			Type:      kafkaraft.CmdUpdateISR,
			Topic:     req.Topic,
			Partition: req.Partition,
			Epoch:     req.Epoch,
			ISR:       req.IsrIds,
		}); err != nil {
			return nil, status.Errorf(codes.Internal, "raft apply: %v", err)
		}
	}
	return &pb.AlterIsrResponse{}, nil
}

// Helpers

// brokerClient returns the cached gRPC client for addr, or nil if not connected.
func (c *coordinator) brokerClient(addr string) pb.BrokerClient {
	c.brokersMu.Lock()
	cl := c.brokerConns[addr]
	c.brokersMu.Unlock()
	return cl
}

// initBackupPartitions tells every backup broker to create the partition locally.
func (c *coordinator) initBackupPartitions(ctx context.Context, topic string, assignments []partitionAssignment) {
	byBroker := make(map[string][]int32)
	for _, a := range assignments {
		for _, backup := range a.backups {
			byBroker[backup] = append(byBroker[backup], a.partition)
		}
	}
	for addr, pids := range byBroker {
		cl := c.brokerClient(addr)
		if cl == nil {
			continue
		}
		_, _ = cl.InitPartitions(ctx, &pb.InitPartitionsRequest{Topic: topic, Partitions: pids})
	}
}

// assignLeaderRoles sends AssignRole(LEADER) to each primary broker.
// The backup broker IDs are passed as the initial ISR so the leader pre-populates
// its replica state and waits for them to catch up before advancing the HW.
func (c *coordinator) assignLeaderRoles(ctx context.Context, topic string, assignments []partitionAssignment, epoch int64) {
	c.mu.RLock()
	brokerIDs := c.brokerIDs
	c.mu.RUnlock()

	for _, a := range assignments {
		cl := c.brokerClient(a.primary)
		if cl == nil {
			continue
		}
		isrIDs := make([]int32, 0, len(a.backups))
		for _, addr := range a.backups {
			if id, ok := brokerIDs[addr]; ok {
				isrIDs = append(isrIDs, id)
			}
		}
		_, _ = cl.AssignRole(ctx, &pb.AssignRoleRequest{
			Topic:     topic,
			Partition: a.partition,
			Role:      pb.ReplicaRole_LEADER,
			Epoch:     epoch,
			IsrIds:    isrIDs,
		})
	}
}

// assignFollowerRoles sends AssignRole(FOLLOWER) to each backup broker.
func (c *coordinator) assignFollowerRoles(ctx context.Context, topic string, assignments []partitionAssignment, epoch int64) {
	c.mu.RLock()
	brokerIDs := c.brokerIDs
	c.mu.RUnlock()

	for _, a := range assignments {
		leaderID, ok := brokerIDs[a.primary]
		if !ok {
			continue
		}
		for _, backupAddr := range a.backups {
			cl := c.brokerClient(backupAddr)
			if cl == nil {
				continue
			}
			_, _ = cl.AssignRole(ctx, &pb.AssignRoleRequest{
				Topic:      topic,
				Partition:  a.partition,
				Role:       pb.ReplicaRole_FOLLOWER,
				Epoch:      epoch,
				LeaderId:   leaderID,
				LeaderAddr: a.primary,
			})
		}
	}
}

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
			Topic:      topic,
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

// selectBackups picks n backup brokers from the list (excluding primary)
func selectBackups(brokers []string, primary string, n int) []string {
	backups := make([]string, 0, n)
	for _, b := range brokers {
		if len(backups) == n {
			break
		}
		if b != primary {
			backups = append(backups, b)
		}
	}
	return backups
}