package tests

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	kafkaraft "github.com/thanhlongnt/kafka-lite/internal/raft"
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
		node, err := kafkaraft.NewNode(ids[i], addrs[i], t.TempDir())
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
	t.Skip("not yet implemented: requires broker health-check mechanism")
}
