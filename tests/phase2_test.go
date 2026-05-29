package tests

import (
	"context"
	"fmt"
	"net"
	"sort"
	"testing"
	"time"

	"github.com/thanhlongnt/kafka-lite/internal/broker"
	"github.com/thanhlongnt/kafka-lite/internal/consumer"
	"github.com/thanhlongnt/kafka-lite/internal/coordinator"
	"github.com/thanhlongnt/kafka-lite/internal/producer"
	pb "github.com/thanhlongnt/kafka-lite/internal/proto/kafka_lite"
	"github.com/thanhlongnt/kafka-lite/internal/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
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

// ── Routing env: four in-process brokers + one coordinator ───────────────────

const (
	routingAddrA = "passthrough://broker-a"
	routingAddrB = "passthrough://broker-b"
	routingAddrC = "passthrough://broker-c"
	routingAddrD = "passthrough://broker-d"
)

// routingAllAddrs lists the four broker addresses in registration order, which
// determines round-robin partition assignment: partition i → allAddrs[i%4].
var routingAllAddrs = []string{routingAddrA, routingAddrB, routingAddrC, routingAddrD}

// routingEnv holds four independent in-process brokers wired to a single
// coordinator. Broker A's gRPC server also registers the coordinator so that
// producers and consumers can reach it through the broker-A connection.
type routingEnv struct {
	coord   *coordinator.Coordinator
	clients map[string]pb.BrokerClient // addr → direct broker client
	dialerA func(context.Context, string) (net.Conn, error)
	dialers map[string]func(context.Context, string) (net.Conn, error)
}

func newRoutingEnv(t *testing.T) *routingEnv {
	t.Helper()

	listeners := map[string]*bufconn.Listener{
		routingAddrA: bufconn.Listen(testutil.BufSize),
		routingAddrB: bufconn.Listen(testutil.BufSize),
		routingAddrC: bufconn.Listen(testutil.BufSize),
		routingAddrD: bufconn.Listen(testutil.BufSize),
	}

	dialers := make(map[string]func(context.Context, string) (net.Conn, error), 4)
	for addr, lis := range listeners {
		lis := lis // capture
		dialers[addr] = func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}
	}

	// Coordinator's broker dialer: maps fake addresses to in-process connections.
	coordBrokerDialer := func(ctx context.Context, addr string) (pb.BrokerClient, error) {
		d, ok := dialers[addr]
		if !ok {
			return nil, fmt.Errorf("unknown broker addr: %s", addr)
		}
		conn, err := grpc.NewClient(addr,
			grpc.WithContextDialer(d),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		return pb.NewBrokerClient(conn), nil
	}

	coord := coordinator.NewExported()
	coord.SetBrokerDialer(coordBrokerDialer)

	// Broker A also serves the coordinator so producers/consumers can call
	// GetMetadata/JoinGroup through the broker-A connection.
	for addr, lis := range listeners {
		lis, addr := lis, addr // capture
		srv := grpc.NewServer()
		pb.RegisterBrokerServer(srv, broker.New())
		if addr == routingAddrA {
			pb.RegisterCoordinatorServer(srv, coord)
		}
		go func() { _ = srv.Serve(lis) }()
		t.Cleanup(func() { srv.Stop(); lis.Close() })
	}

	ctx := context.Background()
	for _, addr := range routingAllAddrs {
		if _, err := coord.RegisterBroker(ctx, &pb.RegisterBrokerRequest{Addr: addr}); err != nil {
			t.Fatalf("RegisterBroker %s: %v", addr, err)
		}
	}

	clients := make(map[string]pb.BrokerClient, 4)
	for addr, d := range dialers {
		clients[addr] = pb.NewBrokerClient(testutil.NewConn(t, d))
	}

	return &routingEnv{
		coord:   coord,
		clients: clients,
		dialerA: dialers[routingAddrA],
		dialers: dialers,
	}
}

// clientFor returns the direct broker client for the given address.
func (e *routingEnv) clientFor(addr string) pb.BrokerClient {
	return e.clients[addr]
}

// brokerClientDialer returns a dialer function suitable for producer.SetDialer
// and consumer.SetDialer. It maps fake broker addresses to in-process brokers.
func (e *routingEnv) brokerClientDialer() func(context.Context, string) (pb.BrokerClient, error) {
	return func(ctx context.Context, addr string) (pb.BrokerClient, error) {
		d, ok := e.dialers[addr]
		if !ok {
			return nil, fmt.Errorf("unknown broker addr: %s", addr)
		}
		conn, err := grpc.NewClient(addr,
			grpc.WithContextDialer(d),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		return pb.NewBrokerClient(conn), nil
	}
}

// Phase 2: multi-broker partitioning — routing correctness and data availability.
func TestPhase2_PartitionRouting(t *testing.T) {
	// ── Sub-test 1: coordinator assigns partitions in round-robin order ───────
	// With 4 brokers and 4 partitions, each broker owns exactly one partition:
	//   p0 → A, p1 → B, p2 → C, p3 → D
	t.Run("MetadataRoundRobin", func(t *testing.T) {
		e := newRoutingEnv(t)
		ctx := context.Background()

		if _, err := e.coord.CreateTopic(ctx, &pb.CreateTopicRequest{
			Topic: "rr", Partitions: 4,
		}); err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}

		meta, err := e.coord.GetMetadata(ctx, &pb.MetadataRequest{Topic: "rr"})
		if err != nil {
			t.Fatalf("GetMetadata: %v", err)
		}
		if len(meta.Partitions) != 4 {
			t.Fatalf("expected 4 partitions, got %d", len(meta.Partitions))
		}

		// Brokers registered [A, B, C, D]; partition i goes to routingAllAddrs[i%4].
		for _, pi := range meta.Partitions {
			want := routingAllAddrs[pi.Partition%4]
			if pi.BrokerAddr != want {
				t.Errorf("partition %d: want broker %s, got %s", pi.Partition, want, pi.BrokerAddr)
			}
		}
	})

	// ── Sub-test 2: produce to a partition the broker does not own → NotFound ─
	// Each broker owns exactly one partition; the other three are unowned.
	t.Run("WrongBrokerNotFound", func(t *testing.T) {
		e := newRoutingEnv(t)
		ctx := context.Background()

		if _, err := e.coord.CreateTopic(ctx, &pb.CreateTopicRequest{
			Topic: "wnf", Partitions: 4,
		}); err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}

		// For each (broker, partition) pair, produce and check the right outcome.
		for brokerIdx, addr := range routingAllAddrs {
			cl := e.clientFor(addr)
			for p := int32(0); p < 4; p++ {
				ownerIdx := int(p) % 4
				_, err := cl.Produce(ctx, &pb.ProduceRequest{
					Topic: "wnf", Partition: p, Value: []byte("v"),
				})
				if brokerIdx == ownerIdx {
					if err != nil {
						t.Errorf("broker %s p%d (owner): unexpected error: %v", addr, p, err)
					}
				} else {
					if status.Code(err) != codes.NotFound {
						t.Errorf("broker %s p%d (non-owner): expected NotFound, got %v", addr, p, err)
					}
				}
			}
		}
	})

	// ── Sub-test 3: producer.Route resolves metadata and sends to the right broker
	t.Run("ProducerRoute", func(t *testing.T) {
		e := newRoutingEnv(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if _, err := e.coord.CreateTopic(ctx, &pb.CreateTopicRequest{
			Topic: "pr", Partitions: 4,
		}); err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}

		// Build a producer connected to broker A (which also serves the coordinator).
		p, err := producer.New(routingAddrA, grpc.WithContextDialer(e.dialerA))
		if err != nil {
			t.Fatalf("producer.New: %v", err)
		}
		defer p.Close()
		p.ConnectCoordinator()
		p.SetDialer(e.brokerClientDialer())

		// Fetch metadata once so we can verify routing post-hoc.
		meta, err := e.coord.GetMetadata(ctx, &pb.MetadataRequest{Topic: "pr"})
		if err != nil {
			t.Fatalf("GetMetadata: %v", err)
		}
		partitionOwner := make(map[int32]string, len(meta.Partitions))
		for _, pi := range meta.Partitions {
			partitionOwner[pi.Partition] = pi.BrokerAddr
		}

		keys := [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma"), []byte("delta")}
		for i, key := range keys {
			partition, offset, err := p.Route(ctx, "pr", key, []byte{byte(i)})
			if err != nil {
				t.Errorf("Route(%s): %v", key, err)
				continue
			}

			// Fetch the message back from whichever broker owns the partition.
			fetchClient := e.clientFor(partitionOwner[partition])
			stream, err := fetchClient.Fetch(ctx, &pb.FetchRequest{
				Topic: "pr", Partition: partition, StartOffset: offset,
			})
			if err != nil {
				t.Errorf("Fetch(%s): %v", key, err)
				continue
			}
			msg, err := stream.Recv()
			if err != nil {
				t.Errorf("Recv(%s): %v", key, err)
				continue
			}
			if msg.Value[0] != byte(i) {
				t.Errorf("key %s: expected value %d, got %d", key, i, msg.Value[0])
			}
			if msg.Partition != partition {
				t.Errorf("key %s: expected partition %d, got %d", key, partition, msg.Partition)
			}
		}
	})

	// ── Sub-test 4: consumer dials the correct broker per assigned partition ──
	// All 4 partitions are on distinct brokers. A sole consumer gets all 4 and
	// must successfully read from each broker independently.
	t.Run("ConsumerCrossBroker", func(t *testing.T) {
		e := newRoutingEnv(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		const topic = "ccb"
		if _, err := e.coord.CreateTopic(ctx, &pb.CreateTopicRequest{
			Topic: topic, Partitions: 4,
		}); err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}

		// Pre-produce 5 messages to each partition via the owning broker's client.
		// p0 → A, p1 → B, p2 → C, p3 → D.
		for p, addr := range routingAllAddrs {
			cl := e.clientFor(addr)
			for i := range 5 {
				if _, err := cl.Produce(ctx, &pb.ProduceRequest{
					Topic: topic, Partition: int32(p), Value: []byte{byte(i)},
				}); err != nil {
					t.Fatalf("produce p%d[%d]: %v", p, i, err)
				}
			}
		}

		// Consumer connects to broker A (coordinator is co-located there).
		// SetDialer must be called before JoinGroup so readers use in-process conns.
		c, err := consumer.New(routingAddrA, topic, 0, 0, grpc.WithContextDialer(e.dialerA))
		if err != nil {
			t.Fatalf("consumer.New: %v", err)
		}
		defer c.Close()
		c.SetDialer(e.brokerClientDialer())

		if err := c.JoinGroup(ctx, "ccb-group", "m1", topic); err != nil {
			t.Fatalf("JoinGroup: %v", err)
		}

		// Sole member gets all 4 partitions.
		assertPartitions(t, "consumer", c, []int32{0, 1, 2, 3})

		// Poll all 20 pre-produced messages and verify 5 from each partition.
		seen := make(map[int32]int) // partition → count
		for range 20 {
			msg, err := c.Poll(ctx)
			if err != nil {
				t.Fatalf("Poll: %v", err)
			}
			seen[msg.Partition]++
		}
		for p := int32(0); p < 4; p++ {
			if seen[p] != 5 {
				t.Errorf("expected 5 messages from partition %d, got %d", p, seen[p])
			}
		}
	})
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
