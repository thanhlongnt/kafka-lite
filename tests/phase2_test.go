package tests

import (
	"context"
	"net"
	"sort"
	"testing"

	"github.com/thanhlongnt/kafka-lite/internal/broker"
	"github.com/thanhlongnt/kafka-lite/internal/consumer"
	"github.com/thanhlongnt/kafka-lite/internal/coordinator"
	pb "github.com/thanhlongnt/kafka-lite/internal/proto/kafka_lite"
	"github.com/thanhlongnt/kafka-lite/internal/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// phase2Env spins up an in-process broker and coordinator on the same gRPC
// server (via bufconn) and wires them together so the coordinator can call
// InitPartitions on the broker.
type phase2Env struct {
	dialer       func(context.Context, string) (net.Conn, error)
	brokerClient pb.BrokerClient
	coord        *coordinator.Coordinator
}

func newPhase2Env(t *testing.T) *phase2Env {
	t.Helper()
	lis := bufconn.Listen(testutil.BufSize)
	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}

	// brokerDialer lets the coordinator reach the in-process broker.
	brokerDialer := func(_ context.Context, _ string) (pb.BrokerClient, error) {
		conn, err := grpc.NewClient(testutil.BufconnAddr,
			grpc.WithContextDialer(dialer),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		return pb.NewBrokerClient(conn), nil
	}

	coord := coordinator.NewExported()
	coord.SetBrokerDialer(brokerDialer)

	srv := grpc.NewServer()
	pb.RegisterBrokerServer(srv, broker.New())
	pb.RegisterCoordinatorServer(srv, coord)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() { srv.Stop(); lis.Close() })

	ctx := context.Background()
	if _, err := coord.RegisterBroker(ctx, &pb.RegisterBrokerRequest{Addr: testutil.BufconnAddr}); err != nil {
		t.Fatalf("RegisterBroker: %v", err)
	}

	conn := testutil.NewConn(t, dialer)
	return &phase2Env{
		dialer:       dialer,
		brokerClient: pb.NewBrokerClient(conn),
		coord:        coord,
	}
}

func (e *phase2Env) createTopic(t *testing.T, topic string, partitions int32) {
	t.Helper()
	_, err := e.coord.CreateTopic(context.Background(), &pb.CreateTopicRequest{
		Topic: topic, Partitions: partitions,
	})
	if err != nil {
		t.Fatalf("CreateTopic %q: %v", topic, err)
	}
}

// produce writes n messages to the given partition.
func (e *phase2Env) produce(t *testing.T, topic string, partition int32, n int) {
	t.Helper()
	for i := range n {
		_, err := e.brokerClient.Produce(context.Background(), &pb.ProduceRequest{
			Topic: topic, Partition: partition, Value: []byte{byte(i)},
		})
		if err != nil {
			t.Fatalf("Produce(partition=%d, i=%d): %v", partition, i, err)
		}
	}
}

// newGroupConsumer creates a consumer, joins groupID as memberID, and sets the
// rebalance interval to 0 so tryRebalance fires on every Poll call.
func (e *phase2Env) newGroupConsumer(t *testing.T, groupID, memberID, topic string) *consumer.Consumer {
	t.Helper()
	opt := grpc.WithContextDialer(e.dialer)
	c, err := consumer.New(testutil.BufconnAddr, topic, 0, 0, opt)
	if err != nil {
		t.Fatalf("consumer.New %s: %v", memberID, err)
	}
	t.Cleanup(func() { c.Close() })

	brokerDialer := func(_ context.Context, _ string) (pb.BrokerClient, error) {
		conn, err := grpc.NewClient(testutil.BufconnAddr,
			grpc.WithContextDialer(e.dialer),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		return pb.NewBrokerClient(conn), nil
	}
	c.SetDialer(brokerDialer)
	c.SetRebalanceInterval(0)

	if err := c.JoinGroup(context.Background(), groupID, memberID, topic); err != nil {
		t.Fatalf("JoinGroup(%s): %v", memberID, err)
	}
	return c
}

func sortedPartitions(s []int32) []int32 {
	out := make([]int32, len(s))
	copy(out, s)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func assertPartitions(t *testing.T, name string, c *consumer.Consumer, want []int32) {
	t.Helper()
	got := sortedPartitions(c.AssignedPartitions())
	want = sortedPartitions(want)
	if len(got) != len(want) {
		t.Errorf("%s: expected partitions %v, got %v", name, want, got)
		return
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("%s: expected partitions %v, got %v", name, want, got)
			return
		}
	}
}

// mustPoll calls Poll and fails the test if it returns an error.
func mustPoll(t *testing.T, name string, c *consumer.Consumer) {
	t.Helper()
	if _, err := c.Poll(context.Background()); err != nil {
		t.Fatalf("%s Poll: %v", name, err)
	}
}

// Phase 2: multi-broker partitioning — routing correctness and data availability.
func TestPhase2_PartitionRouting(t *testing.T) {
	t.Skip("not yet implemented")
}

// TestPhase2_ConsumerGroupCoordination verifies that consumer group rebalancing
// works correctly as members join one by one (issue-018 fix).
//
// Topic has 4 partitions. The coordinator uses sorted member IDs to assign
// ranges, so with members m1…m4 the expected steady-state is one partition each.
//
// Assignment after each join (partitionRange with sorted member IDs):
//
//	1 member  — m1: [0 1 2 3]
//	2 members — m1: [0 1]   m2: [2 3]
//	3 members — m1: [0 1]   m2: [2]    m3: [3]
//	4 members — m1: [0]     m2: [1]    m3: [2]    m4: [3]
//
// Each consumer has rebalanceInterval=0, so the first Poll after a new member
// joins triggers tryRebalance and the consumer updates its reader set.
func TestPhase2_ConsumerGroupCoordination(t *testing.T) {
	const (
		topic      = "events"
		partitions = 4
		msgsEach   = 20 // messages per partition — enough that no Poll ever blocks
		group      = "grp"
	)

	e := newPhase2Env(t)
	e.createTopic(t, topic, partitions)

	// Pre-produce messages to every partition so Poll always returns immediately.
	for p := range int32(partitions) {
		e.produce(t, topic, p, msgsEach)
	}

	// ── Step 1: m1 joins alone ────────────────────────────────────────────────
	// Coordinator assigns all 4 partitions to the only member.
	m1 := e.newGroupConsumer(t, group, "m1", topic)
	assertPartitions(t, "m1 initial", m1, []int32{0, 1, 2, 3})

	// Poll triggers a rebalance check; still the only member so no change.
	mustPoll(t, "m1", m1)
	assertPartitions(t, "m1 after step-1 poll", m1, []int32{0, 1, 2, 3})

	// ── Step 2: m2 joins ──────────────────────────────────────────────────────
	// Coordinator rebalances to: m1=[0,1]  m2=[2,3].
	// m2 gets its assignment immediately from JoinGroup.
	// m1 still holds [0,1,2,3] until its next Poll triggers tryRebalance.
	m2 := e.newGroupConsumer(t, group, "m2", topic)
	assertPartitions(t, "m2 initial", m2, []int32{2, 3})
	assertPartitions(t, "m1 before step-2 poll", m1, []int32{0, 1, 2, 3})

	mustPoll(t, "m1", m1)
	assertPartitions(t, "m1 after step-2 poll", m1, []int32{0, 1})

	// ── Step 3: m3 joins ──────────────────────────────────────────────────────
	// Coordinator rebalances to: m1=[0,1]  m2=[2]  m3=[3].
	// m3 gets its assignment immediately; m1 and m2 update on next Poll.
	m3 := e.newGroupConsumer(t, group, "m3", topic)
	assertPartitions(t, "m3 initial", m3, []int32{3})
	assertPartitions(t, "m1 before step-3 poll", m1, []int32{0, 1})
	assertPartitions(t, "m2 before step-3 poll", m2, []int32{2, 3})

	mustPoll(t, "m1", m1)
	assertPartitions(t, "m1 after step-3 poll", m1, []int32{0, 1})

	mustPoll(t, "m2", m2)
	assertPartitions(t, "m2 after step-3 poll", m2, []int32{2})

	// ── Step 4: m4 joins ──────────────────────────────────────────────────────
	// Coordinator rebalances to: m1=[0]  m2=[1]  m3=[2]  m4=[3].
	// m4 gets [3] immediately. All other members update on their next Poll.
	m4 := e.newGroupConsumer(t, group, "m4", topic)
	assertPartitions(t, "m4 initial", m4, []int32{3})

	mustPoll(t, "m1", m1)
	assertPartitions(t, "m1 after step-4 poll", m1, []int32{0})

	mustPoll(t, "m2", m2)
	assertPartitions(t, "m2 after step-4 poll", m2, []int32{1})

	mustPoll(t, "m3", m3)
	assertPartitions(t, "m3 after step-4 poll", m3, []int32{2})

	// m4's assignment was correct from the start; Poll confirms no change.
	mustPoll(t, "m4", m4)
	assertPartitions(t, "m4 after step-4 poll", m4, []int32{3})
}
