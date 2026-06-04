package tests

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/thanhlongnt/kafka-lite/internal/broker"
	pb "github.com/thanhlongnt/kafka-lite/internal/proto/kafka_lite"
	kafkaraft "github.com/thanhlongnt/kafka-lite/internal/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// freeAddr finds an available loopback TCP address.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freeAddr: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

// startCluster spins up n Raft nodes bootstrapped together and returns them.
func startCluster(t *testing.T, n int) []*kafkaraft.Node {
	t.Helper()
	ids := make([]string, n)
	addrs := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("node%d", i+1)
		addrs[i] = freeAddr(t)
	}

	servers := make([]raft.Server, n)
	for i := range servers {
		servers[i] = raft.Server{
			ID:      raft.ServerID(ids[i]),
			Address: raft.ServerAddress(addrs[i]),
		}
	}

	nodes := make([]*kafkaraft.Node, n)
	for i := range nodes {
		node, err := kafkaraft.NewNode(ids[i], addrs[i], t.TempDir(), nil)
		if err != nil {
			t.Fatalf("NewNode %s: %v", ids[i], err)
		}
		if err := node.Bootstrap(servers); err != nil {
			t.Fatalf("Bootstrap %s: %v", ids[i], err)
		}
		nodes[i] = node
		t.Cleanup(func() { _ = node.Shutdown() })
	}
	return nodes
}

// waitForLeader polls until one node is leader or the timeout expires.
func waitForLeader(t *testing.T, nodes []*kafkaraft.Node, timeout time.Duration) *kafkaraft.Node {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.IsLeader() {
				return n
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("no leader elected within timeout")
	return nil
}

// startTCPBroker starts a broker
func startTCPBroker(t *testing.T, id int32) (*grpc.Server, pb.BrokerClient, string) {
	t.Helper()
	addr := freeAddr(t)
	b := broker.New(id)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	pb.RegisterBrokerServer(srv, b)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() { srv.Stop() })

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("startTCPBroker: dial %s: %v", addr, err)
	}
	t.Cleanup(func() { conn.Close() })
	return srv, pb.NewBrokerClient(conn), addr
}

// produceN sends n msgs to topic/partition
func produceN(t *testing.T, client pb.BrokerClient, topic string, partition int32, n int) {
	t.Helper()
	ctx := context.Background()
	for i := range n {
		if _, err := client.Produce(ctx, &pb.ProduceRequest{
			Topic:     topic,
			Partition: partition,
			Value:     []byte(fmt.Sprintf("msg-%d", i)),
		}); err != nil {
			t.Fatalf("ProduceN[%d]: %v", i, err)
		}
	}
}

// rawLogLen counts messages in a broker's raw log via FetchReplica
func rawLogLen(client pb.BrokerClient, topic string, partition int32, want int) int {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	stream, err := client.FetchReplica(ctx, &pb.FetchReplicaRequest{
		Topic:       topic,
		Partition:   partition,
		FetchOffset: 0,
		ReplicaId:   99,
		LeaderEpoch: 0,
	})
	if err != nil {
		return 0
	}
	count := 0
	for count < want {
		if _, err := stream.Recv(); err != nil {
			break
		}
		count += 1
	}
	return count
}

// waitReplicated polls until the broker's raw log has at least n messages
func waitReplicated(t *testing.T, client pb.BrokerClient, topic string, partition int32, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if rawLogLen(client, topic, partition, n) >= n {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("waitReplicated: expected at least %d messages in raw log, got %d", n, rawLogLen(client, topic, partition, n))
}

// consumeN reads n msgs from offset 0, fails if TO expires
func consumeN(t *testing.T, client pb.BrokerClient, topic string, partition int32, n int, timeout time.Duration) []*pb.Message {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	stream, err := client.Fetch(ctx, &pb.FetchRequest{
		Topic:       topic,
		Partition:   partition,
		StartOffset: 0,
	})
	if err != nil {
		t.Fatalf("consumeN Fetch: %v", err)
	}
	msgs := make([]*pb.Message, 0, n)
	for len(msgs) < n {
		msg, err := stream.Recv()
		if err != nil {
			t.Fatalf("consumeN Recv: %v", err)
		}
		msgs = append(msgs, msg)
	}
	return msgs
}
func TestPhase3_LeaderElection(t *testing.T) {
	nodes := startCluster(t, 3)

	waitForLeader(t, nodes, 5*time.Second)

	leaders := 0
	for _, n := range nodes {
		if n.IsLeader() {
			leaders++
		}
	}
	if leaders != 1 {
		t.Errorf("expected exactly 1 leader, got %d", leaders)
	}
}

func TestPhase3_NoDataLossOnCrash(t *testing.T) {
	nodes := startCluster(t, 3)
	leader := waitForLeader(t, nodes, 5*time.Second)

	// Commit two partition assignments through the leader.
	cmds := []kafkaraft.Command{
		{Type: kafkaraft.CmdAssign, Topic: "events", Partition: 0, Primary: "broker-a:9090", Backups: []string{"broker-b:9090"}},
		{Type: kafkaraft.CmdAssign, Topic: "events", Partition: 1, Primary: "broker-b:9090", Backups: []string{"broker-a:9090"}},
	}
	for _, cmd := range cmds {
		if err := leader.Apply(cmd); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}

	// Crash the leader.
	_ = leader.Shutdown()

	// Collect the two surviving nodes.
	var remaining []*kafkaraft.Node
	for _, n := range nodes {
		if n != leader {
			remaining = append(remaining, n)
		}
	}

	// Wait for one of them to become the new leader.
	newLeader := waitForLeader(t, remaining, 10*time.Second)

	// Assert the committed state survived on the new leader's FSM.
	ps0, ok := newLeader.FSM().Get("events", 0)
	if !ok {
		t.Fatal("partition events/0 missing from FSM after leader crash")
	}
	if ps0.Primary != "broker-a:9090" {
		t.Errorf("events/0 primary: want broker-a:9090, got %s", ps0.Primary)
	}

	ps1, ok := newLeader.FSM().Get("events", 1)
	if !ok {
		t.Fatal("partition events/1 missing from FSM after leader crash")
	}
	if ps1.Primary != "broker-b:9090" {
		t.Errorf("events/1 primary: want broker-b:9090, got %s", ps1.Primary)
	}
}

func TestPhase3_BrokerFailover(t *testing.T) {
	const n = 10
	const topic = "failover"
	partition := int32(0)
	ctx := context.Background()

	lSrv, lc, lAddr := startTCPBroker(t, 1)
	_, fc, _ := startTCPBroker(t, 2)

	if _, err := lc.CreateTopic(ctx, &pb.CreateTopicRequest{Topic: topic, Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic leader: %v", err)
	}
	if _, err := fc.InitPartitions(ctx, &pb.InitPartitionsRequest{Topic: topic, Partitions: []int32{partition}}); err != nil {
		t.Fatalf("InitPartitions follower: %v", err)
	}

	if _, err := lc.AssignRole(ctx, &pb.AssignRoleRequest{
		Topic: topic, Partition: partition,
		Role: pb.ReplicaRole_LEADER, Epoch: 1,
	}); err != nil {
		t.Fatalf("AssignRole LEADER: %v", err)
	}
	produceN(t, lc, topic, partition, n)

	if _, err := fc.AssignRole(ctx, &pb.AssignRoleRequest{
		Topic: topic, Partition: partition,
		Role: pb.ReplicaRole_FOLLOWER, Epoch: 1,
		LeaderId: 1, LeaderAddr: lAddr,
	}); err != nil {
		t.Fatalf("AssignRole FOLLOWER: %v", err)
	}
	waitReplicated(t, fc, topic, partition, n)

	t.Log("crashing leader broker")
	lSrv.Stop()

	if _, err := fc.AssignRole(ctx, &pb.AssignRoleRequest{
		Topic: topic, Partition: partition,
		Role: pb.ReplicaRole_LEADER, Epoch: 2,
	}); err != nil {
		t.Fatalf("promote follower to leader: %v", err)
	}

	produceN(t, fc, topic, partition, n)

	msgs := consumeN(t, fc, topic, partition, 2*n, 5*time.Second)

	for i := range n {
		want := fmt.Sprintf("msg-%d", i)
		if got := string(msgs[i].Value); got != want {
			t.Errorf("pre-crash msg[%d]: want %q, got %q", i, want, got)
		}
	}
	t.Logf("failover ok: %d pre-crash messages survived, %d post-failover messages written", n, n)
}

func TestPhase3_BrokerReplicationBasic(t *testing.T) {
	const n = 10
	const topic = "repl-basic"
	partition := int32(0)
	ctx := context.Background()

	_, lc, lAddr := startTCPBroker(t, 1)
	_, fc, _ := startTCPBroker(t, 2)

	if _, err := lc.CreateTopic(ctx, &pb.CreateTopicRequest{Topic: topic, Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic leader: %v", err)
	}
	if _, err := fc.InitPartitions(ctx, &pb.InitPartitionsRequest{Topic: topic, Partitions: []int32{partition}}); err != nil {
		t.Fatalf("InitPartitions follower: %v", err)
	}

	if _, err := lc.AssignRole(ctx, &pb.AssignRoleRequest{
		Topic: topic, Partition: partition,
		Role: pb.ReplicaRole_LEADER, Epoch: 1,
	}); err != nil {
		t.Fatalf("AssignRole LEADER: %v", err)
	}
	produceN(t, lc, topic, partition, n)

	if _, err := fc.AssignRole(ctx, &pb.AssignRoleRequest{
		Topic: topic, Partition: partition,
		Role: pb.ReplicaRole_FOLLOWER, Epoch: 1,
		LeaderId: 1, LeaderAddr: lAddr,
	}); err != nil {
		t.Fatalf("AssignRole FOLLOWER: %v", err)
	}

	// Assert 1: raw log has all N messages.
	waitReplicated(t, fc, topic, partition, n)

	// Assert 2: follower can serve consumer reads (proves HW advanced via replication).
	msgs := consumeN(t, fc, topic, partition, n, 5*time.Second)
	for i := range n {
		want := fmt.Sprintf("msg-%d", i)
		if got := string(msgs[i].Value); got != want {
			t.Errorf("msg[%d]: want %q, got %q", i, want, got)
		}
	}
	t.Logf("replication ok: follower has all %d messages and HW advanced", n)
}

func TestPhase3_HWBlocksUntilReplicated(t *testing.T) {
	const n = 5
	const topic = "hw-block"
	partition := int32(0)
	ctx := context.Background()

	_, lc, lAddr := startTCPBroker(t, 1)
	_, fc, _ := startTCPBroker(t, 2)

	if _, err := lc.CreateTopic(ctx, &pb.CreateTopicRequest{Topic: topic, Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := fc.InitPartitions(ctx, &pb.InitPartitionsRequest{Topic: topic, Partitions: []int32{partition}}); err != nil {
		t.Fatalf("InitPartitions: %v", err)
	}

	if _, err := lc.AssignRole(ctx, &pb.AssignRoleRequest{
		Topic: topic, Partition: partition,
		Role: pb.ReplicaRole_LEADER, Epoch: 1,
	}); err != nil {
		t.Fatalf("AssignRole LEADER: %v", err)
	}
	if _, err := fc.AssignRole(ctx, &pb.AssignRoleRequest{
		Topic: topic, Partition: partition,
		Role: pb.ReplicaRole_FOLLOWER, Epoch: 1,
		LeaderId: 1, LeaderAddr: lAddr,
	}); err != nil {
		t.Fatalf("AssignRole FOLLOWER: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Produce N messages after follower is registered.
	produceN(t, lc, topic, partition, n)

	// Consumer on the LEADER reads all N messages.
	msgs := consumeN(t, lc, topic, partition, n, 5*time.Second)
	for i := range n {
		want := fmt.Sprintf("msg-%d", i)
		if got := string(msgs[i].Value); got != want {
			t.Errorf("msg[%d]: want %q, got %q", i, want, got)
		}
	}
	t.Logf("%d messages visible on leader only after follower ack", n)
}

func TestPhase3_NoDataLossonBrokerLeaderCrash(t *testing.T) {
	const n = 10
	const topic = "no-data-loss"
	partition := int32(0)
	ctx := context.Background()

	lSrv, lc, lAddr := startTCPBroker(t, 1)
	_, fc, _ := startTCPBroker(t, 2)

	if _, err := lc.CreateTopic(ctx, &pb.CreateTopicRequest{Topic: topic, Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := fc.InitPartitions(ctx, &pb.InitPartitionsRequest{Topic: topic, Partitions: []int32{partition}}); err != nil {
		t.Fatalf("InitPartitions: %v", err)
	}

	// Produce N messages before wiring the follower
	if _, err := lc.AssignRole(ctx, &pb.AssignRoleRequest{
		Topic: topic, Partition: partition,
		Role: pb.ReplicaRole_LEADER, Epoch: 1,
	}); err != nil {
		t.Fatalf("AssignRole LEADER: %v", err)
	}
	produceN(t, lc, topic, partition, n)

	// Wire follower and wait until all N messages are replicated.
	if _, err := fc.AssignRole(ctx, &pb.AssignRoleRequest{
		Topic: topic, Partition: partition,
		Role: pb.ReplicaRole_FOLLOWER, Epoch: 1,
		LeaderId: 1, LeaderAddr: lAddr,
	}); err != nil {
		t.Fatalf("AssignRole FOLLOWER: %v", err)
	}
	waitReplicated(t, fc, topic, partition, n)

	// Crash  leader.
	t.Log("crashing leader")
	lSrv.Stop()

	// Promote follower to leader at higher epoch.
	if _, err := fc.AssignRole(ctx, &pb.AssignRoleRequest{
		Topic: topic, Partition: partition,
		Role: pb.ReplicaRole_LEADER, Epoch: 2,
	}); err != nil {
		t.Fatalf("promote follower to leader: %v", err)
	}

	if _, err := fc.Produce(ctx, &pb.ProduceRequest{
		Topic: topic, Partition: partition, Value: []byte("sentinel"),
	}); err != nil {
		t.Fatalf("sentinel produce: %v", err)
	}

	// Read N+1 messages from the new leader.
	msgs := consumeN(t, fc, topic, partition, n+1, 5*time.Second)

	for i := range n {
		want := fmt.Sprintf("msg-%d", i)
		if got := string(msgs[i].Value); got != want {
			t.Errorf("pre-crash msg[%d]: want %q, got %q", i, want, got)
		}
	}
	if string(msgs[n].Value) != "sentinel" {
		t.Errorf("last message: want sentinel, got %q", string(msgs[n].Value))
	}
	t.Logf("no data loss: all %d pre-crash messages intact after leader failover", n)
}

func TestPhase3_ISRFollowerCrash(t *testing.T) {
	if testing.Short() {
		t.Skip("ISR timeout test takes ~12s; skipping in short mode")
	}
	const topic = "isr-crash"
	partition := int32(0)
	ctx := context.Background()

	_, lc, lAddr := startTCPBroker(t, 1)
	_, fc, _ := startTCPBroker(t, 2)

	if _, err := lc.CreateTopic(ctx, &pb.CreateTopicRequest{Topic: topic, Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := fc.InitPartitions(ctx, &pb.InitPartitionsRequest{Topic: topic, Partitions: []int32{partition}}); err != nil {
		t.Fatalf("InitPartitions: %v", err)
	}

	if _, err := lc.AssignRole(ctx, &pb.AssignRoleRequest{
		Topic: topic, Partition: partition,
		Role: pb.ReplicaRole_LEADER, Epoch: 1,
	}); err != nil {
		t.Fatalf("AssignRole LEADER: %v", err)
	}
	if _, err := fc.AssignRole(ctx, &pb.AssignRoleRequest{
		Topic: topic, Partition: partition,
		Role: pb.ReplicaRole_FOLLOWER, Epoch: 1,
		LeaderId: 1, LeaderAddr: lAddr,
	}); err != nil {
		t.Fatalf("AssignRole FOLLOWER: %v", err)
	}

	// Produce a few messages and wait for replication
	produceN(t, lc, topic, partition, 3)
	waitReplicated(t, fc, topic, partition, 3)

	tCrash := time.Now()
	t.Logf("[METRIC] ISR follower crash at %v", tCrash.Format(time.RFC3339))
	if _, err := fc.AssignRole(ctx, &pb.AssignRoleRequest{
		Topic: topic, Partition: partition,
		Role: pb.ReplicaRole_FOLLOWER, Epoch: 1,
		LeaderId: 99, LeaderAddr: "127.0.0.1:1",
	}); err != nil {
		t.Fatalf("simulate follower crash: %v", err)
	}

	// Produce one more message
	if _, err := lc.Produce(ctx, &pb.ProduceRequest{
		Topic: topic, Partition: partition, Value: []byte("post-crash"),
	}); err != nil {
		t.Fatalf("produce after crash: %v", err)
	}

	// Open a Fetch stream starting at offset 3
	consCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	stream, err := lc.Fetch(consCtx, &pb.FetchRequest{
		Topic: topic, Partition: partition, StartOffset: 3,
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	msg, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv (did ISR shrink within 15s?): %v", err)
	}

	tRecovered := time.Now()
	recovery := tRecovered.Sub(tCrash)
	t.Logf("[METRIC] recovery time: %v (offset=%d value=%q)", recovery, msg.Offset, string(msg.Value))

	// ISR timeout is 10s so recovery must take at least that long.
	if recovery < 9*time.Second {
		t.Errorf("recovery too fast (%v): ISR timeout is 10s, got under 9s", recovery)
	}
}

func TestPhase3_NonISRFollowerCrash(t *testing.T) {
	if testing.Short() {
		t.Skip("requires ISR timeout (~11s); run without -short")
	}
	const n = 5
	const topic = "non-isr-crash"
	partition := int32(0)
	ctx := context.Background()

	_, lc, lAddr := startTCPBroker(t, 1)
	fSrv, fc, _ := startTCPBroker(t, 2)

	if _, err := lc.CreateTopic(ctx, &pb.CreateTopicRequest{Topic: topic, Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := fc.InitPartitions(ctx, &pb.InitPartitionsRequest{Topic: topic, Partitions: []int32{partition}}); err != nil {
		t.Fatalf("InitPartitions: %v", err)
	}

	if _, err := lc.AssignRole(ctx, &pb.AssignRoleRequest{
		Topic: topic, Partition: partition,
		Role: pb.ReplicaRole_LEADER, Epoch: 1,
	}); err != nil {
		t.Fatalf("AssignRole LEADER: %v", err)
	}
	if _, err := fc.AssignRole(ctx, &pb.AssignRoleRequest{
		Topic: topic, Partition: partition,
		Role: pb.ReplicaRole_FOLLOWER, Epoch: 1,
		LeaderId: 1, LeaderAddr: lAddr,
	}); err != nil {
		t.Fatalf("AssignRole FOLLOWER: %v", err)
	}

	produceN(t, lc, topic, partition, 3)
	waitReplicated(t, fc, topic, partition, 3)
	fSrv.Stop()

	t.Log("waiting for follower to fall out of ISR (~10s)...")
	time.Sleep(11 * time.Second)

	t.Log("[METRIC] non-ISR follower crash (already out of ISR)")

	t0 := time.Now()
	produceN(t, lc, topic, partition, n)
	elapsed := time.Since(t0)
	t.Logf("[METRIC] %d produces after non-ISR crash took %v", n, elapsed)

	msgs := consumeN(t, lc, topic, partition, 3+n, 3*time.Second)
	if len(msgs) != 3+n {
		t.Fatalf("expected %d messages, got %d", 3+n, len(msgs))
	}

	if elapsed > 2*time.Second {
		t.Errorf("produces took %v after non-ISR crash, expected < 2s", elapsed)
	}
	t.Logf("non-ISR follower crash: zero impact confirmed")
}

func TestPhase3_FollowerRejoinsISR(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping: requires ~12s for ISR timeout")
	}

	const (
		topic     = "rejoin-topic"
		partition = int32(0)
		n         = 5
	)
	ctx := context.Background()

	lSrv, lc, lAddr := startTCPBroker(t, 1)
	_ = lSrv
	_, fc, fAddr := startTCPBroker(t, 2)
	_ = fAddr

	for _, c := range []pb.BrokerClient{lc, fc} {
		if _, err := c.CreateTopic(ctx, &pb.CreateTopicRequest{Topic: topic, Partitions: 1}); err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}
		if _, err := c.InitPartitions(ctx, &pb.InitPartitionsRequest{Topic: topic, Partitions: []int32{partition}}); err != nil {
			t.Fatalf("InitPartitions: %v", err)
		}
	}

	if _, err := lc.AssignRole(ctx, &pb.AssignRoleRequest{
		Topic: topic, Partition: partition,
		Role: pb.ReplicaRole_LEADER, Epoch: 1,
	}); err != nil {
		t.Fatalf("AssignRole LEADER: %v", err)
	}
	if _, err := fc.AssignRole(ctx, &pb.AssignRoleRequest{
		Topic: topic, Partition: partition,
		Role: pb.ReplicaRole_FOLLOWER, Epoch: 1,
		LeaderId: 1, LeaderAddr: lAddr,
	}); err != nil {
		t.Fatalf("AssignRole FOLLOWER: %v", err)
	}

	produceN(t, lc, topic, partition, n)
	waitReplicated(t, fc, topic, partition, n)
	t.Logf("follower in ISR with %d messages", n)

	if _, err := fc.AssignRole(ctx, &pb.AssignRoleRequest{
		Topic: topic, Partition: partition,
		Role: pb.ReplicaRole_FOLLOWER, Epoch: 1,
		LeaderId: 1, LeaderAddr: "127.0.0.1:1", // unreachable
	}); err != nil {
		t.Fatalf("AssignRole FOLLOWER (pause): %v", err)
	}

	t.Log("waiting for follower to fall out of ISR (~10s)...")
	time.Sleep(11 * time.Second)

	t0 := time.Now()
	produceN(t, lc, topic, partition, 1)
	elapsed := time.Since(t0)
	t.Logf("[METRIC] produce while follower out of ISR: %v (should be fast)", elapsed)
	if elapsed > 2*time.Second {
		t.Errorf("produce took %v with follower out of ISR, expected < 2s", elapsed)
	}

	if _, err := fc.AssignRole(ctx, &pb.AssignRoleRequest{
		Topic: topic, Partition: partition,
		Role: pb.ReplicaRole_FOLLOWER, Epoch: 2,
		LeaderId: 1, LeaderAddr: lAddr,
	}); err != nil {
		t.Fatalf("AssignRole FOLLOWER (rejoin): %v", err)
	}

	waitReplicated(t, fc, topic, partition, n+1)
	t.Log("follower rejoined ISR")

	time.Sleep(200 * time.Millisecond)

	t1 := time.Now()
	produceN(t, lc, topic, partition, 1)
	msgs := consumeN(t, lc, topic, partition, n+2, 5*time.Second)
	elapsed2 := time.Since(t1)

	if len(msgs) != n+2 {
		t.Fatalf("expected %d messages after rejoin, got %d", n+2, len(msgs))
	}
	t.Logf("[METRIC] produce+consume after ISR rejoin: %v", elapsed2)
	t.Logf("follower rejoin ISR confirmed: all %d messages readable", len(msgs))
}
