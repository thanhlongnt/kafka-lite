package tests

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/thanhlongnt/kafka-lite/internal/broker"
	"github.com/thanhlongnt/kafka-lite/internal/coordinator"
	pb "github.com/thanhlongnt/kafka-lite/internal/proto/kafka_lite"
	kafkaraft "github.com/thanhlongnt/kafka-lite/internal/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	chaosNumCoordinators = 5
	chaosNumBrokers      = 20
	chaosLeaderTimeout   = 10 * time.Second
	chaosISRTimeout      = 6 * time.Second // fast-path eviction fires in ~2s; 6s budget covers grace + reporting + FSM

	// Fast failover timings for tests. The production defaults (2s / 10s / 15s)
	// would make every test take ~25s minimum.
	chaosHeartbeatTick   = 200 * time.Millisecond
	chaosFailoverTick    = 200 * time.Millisecond
	chaosDeadAfter       = 1 * time.Second
	chaosLeaderGrace     = 2 * time.Second
	chaosFailoverBudget  = 6 * time.Second // grace + deadAfter + Raft apply + slop

	// ISR eviction timings for chaos brokers. The fast-path eviction goroutine
	// fires after evictGrace = max(2s, isrTimeout/2). With chaosISRBrokerTimeout=4s
	// the grace window is 2s, well above the 500ms replicationLoop reconnect sleep.
	chaosISRBrokerTimeout = 4 * time.Second
	chaosISRBrokerTick    = 200 * time.Millisecond
)

// chaosEnv runs 5 coordinator Raft nodes and 20 brokers, all on TCP loopback in-process.
// Tests can mutate the cluster (kill brokers, kill coordinators) without interfering
// with each other because every test calls newChaosEnv to get its own.
type chaosEnv struct {
	t *testing.T

	raftNodes  []*kafkaraft.Node
	coords     []*coordinator.Coordinator
	coordSrvs  []*grpc.Server
	coordAddrs []string

	brokers       []*broker.Broker
	brokerSrvs    []*grpc.Server
	brokerCli     []pb.BrokerClient
	brokerAddrs   []string
	brokerHBStop  []context.CancelFunc // per-broker heartbeat cancel

	mu          sync.Mutex
	deadCoords  map[int]bool
	deadBrokers map[int]bool
}

func newChaosEnv(t *testing.T) *chaosEnv {
	t.Helper()

	env := &chaosEnv{
		t:           t,
		deadCoords:  map[int]bool{},
		deadBrokers: map[int]bool{},
	}

	env.raftNodes = startCluster(t, chaosNumCoordinators)
	waitForLeader(t, env.raftNodes, chaosLeaderTimeout)

	// Stand up one gRPC server per Raft node, each serving its own coordinator.
	for i, node := range env.raftNodes {
		c := coordinator.NewWithRaft(node)
		c.SetFailoverTimings(chaosFailoverTick, chaosDeadAfter, chaosLeaderGrace)

		addr := freeAddr(t)
		lis, err := net.Listen("tcp", addr)
		if err != nil {
			t.Fatalf("coord %d listen: %v", i, err)
		}
		srv := grpc.NewServer()
		pb.RegisterCoordinatorServer(srv, c)
		go func() { _ = srv.Serve(lis) }()
		t.Cleanup(func() { srv.Stop() })

		// Spawn the failover loop manually since this test builds its own gRPC
		// server (instead of calling coordinator.Serve).
		loopCtx, loopCancel := context.WithCancel(context.Background())
		c.StartFailoverLoop(loopCtx)
		t.Cleanup(loopCancel)

		env.coords = append(env.coords, c)
		env.coordSrvs = append(env.coordSrvs, srv)
		env.coordAddrs = append(env.coordAddrs, addr)
	}

	// Find the Raft leader's coord address — brokers register with the leader so
	// the leader's in-memory broker registry is populated. (Non-leader coordinators
	// reject writes with FailedPrecondition.)
	leaderIdx, leaderAddr := env.leaderIndex()
	if leaderIdx < 0 {
		t.Fatal("no Raft leader after startup")
	}

	for i := 0; i < chaosNumBrokers; i++ {
		id := int32(i + 1)
		b := broker.New(id)
		brokerAddr := freeAddr(t)
		lis, err := net.Listen("tcp", brokerAddr)
		if err != nil {
			t.Fatalf("broker %d listen: %v", id, err)
		}
		srv := grpc.NewServer()
		pb.RegisterBrokerServer(srv, b)
		go func() { _ = srv.Serve(lis) }()
		t.Cleanup(func() { srv.Stop() })

		b.SetSelfAddr(brokerAddr)
		b.SetISRTimeout(chaosISRBrokerTimeout, chaosISRBrokerTick)
		if err := b.ConnectCoordinator(context.Background(), leaderAddr); err != nil {
			t.Fatalf("broker %d ConnectCoordinator: %v", id, err)
		}
		b.SetCoordinatorPeers(env.coordAddrs)

		conn, err := grpc.NewClient(brokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("broker %d dial self: %v", id, err)
		}
		t.Cleanup(func() { conn.Close() })

		env.brokers = append(env.brokers, b)
		env.brokerSrvs = append(env.brokerSrvs, srv)
		env.brokerCli = append(env.brokerCli, pb.NewBrokerClient(conn))
		env.brokerAddrs = append(env.brokerAddrs, brokerAddr)

		// Start the broker's heartbeat loop. killBroker cancels per-broker.
		hbCtx, hbCancel := context.WithCancel(context.Background())
		env.brokerHBStop = append(env.brokerHBStop, hbCancel)
		t.Cleanup(hbCancel)
		go b.HeartbeatLoop(hbCtx, chaosHeartbeatTick)
	}

	// Register every broker with the Raft leader coordinator.
	env.registerAllBrokers(leaderAddr)
	return env
}

// leaderIndex returns the (index, addr) of the current Raft leader, or (-1, "").
func (e *chaosEnv) leaderIndex() (int, string) {
	for i, n := range e.raftNodes {
		e.mu.Lock()
		dead := e.deadCoords[i]
		e.mu.Unlock()
		if dead {
			continue
		}
		if n.IsLeader() {
			return i, e.coordAddrs[i]
		}
	}
	return -1, ""
}

// coordClient dials a fresh gRPC client to the coordinator at idx.
// Used in tests that need to talk to a specific coordinator after a leader change.
func (e *chaosEnv) coordClient(t *testing.T, idx int) pb.CoordinatorClient {
	t.Helper()
	conn, err := grpc.NewClient(e.coordAddrs[idx], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial coord %d: %v", idx, err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewCoordinatorClient(conn)
}

// leaderClient returns a CoordinatorClient pointed at whoever is currently Raft leader.
func (e *chaosEnv) leaderClient(t *testing.T) (pb.CoordinatorClient, string) {
	t.Helper()
	idx, addr := e.leaderIndex()
	if idx < 0 {
		t.Fatal("no Raft leader")
	}
	return e.coordClient(t, idx), addr
}

// registerAllBrokers calls RegisterBroker on the given coordinator address for every
// live broker. Used at fixture setup and when a leader change has reset the registry.
func (e *chaosEnv) registerAllBrokers(coordAddr string) {
	e.t.Helper()
	conn, err := grpc.NewClient(coordAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		e.t.Fatalf("dial coord %s: %v", coordAddr, err)
	}
	defer conn.Close()
	cc := pb.NewCoordinatorClient(conn)
	for i := range e.brokers {
		e.mu.Lock()
		dead := e.deadBrokers[i]
		e.mu.Unlock()
		if dead {
			continue
		}
		_, err := cc.RegisterBroker(context.Background(), &pb.RegisterBrokerRequest{
			Addr: e.brokerAddrs[i],
			Id:   int32(i + 1),
		})
		if err != nil {
			e.t.Fatalf("RegisterBroker %d: %v", i+1, err)
		}
	}
}

// waitForNewLeader polls until a live Raft node other than excludeIdx is leader,
// or the timeout fires. Returns the new leader's index and addr.
func (e *chaosEnv) waitForNewLeader(t *testing.T, excludeIdx int, timeout time.Duration) (int, string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for i, n := range e.raftNodes {
			if i == excludeIdx {
				continue
			}
			e.mu.Lock()
			dead := e.deadCoords[i]
			e.mu.Unlock()
			if dead {
				continue
			}
			if n.IsLeader() {
				return i, e.coordAddrs[i]
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("no new leader (excluding idx=%d) within %v", excludeIdx, timeout)
	return -1, ""
}

// killCoordinator shuts down the Raft node at idx and stops its gRPC server.
// Idempotent — safe to call once per test target.
func (e *chaosEnv) killCoordinator(idx int) {
	e.mu.Lock()
	if e.deadCoords[idx] {
		e.mu.Unlock()
		return
	}
	e.deadCoords[idx] = true
	e.mu.Unlock()
	_ = e.raftNodes[idx].Shutdown()
	e.coordSrvs[idx].Stop()
}

// killBroker stops a broker's gRPC server AND closes its peer/coordinator
// connections plus partition background goroutines, AND cancels the broker's
// heartbeat loop. The Close() is necessary because srv.Stop alone leaves
// monitorISR / replicationLoop running, which would keep the "dead" broker
// fetching from leaders and being mistaken for alive. The heartbeat cancel is
// necessary so the coordinator's failover loop observes the broker as silent.
func (e *chaosEnv) killBroker(idx int) {
	e.mu.Lock()
	if e.deadBrokers[idx] {
		e.mu.Unlock()
		return
	}
	e.deadBrokers[idx] = true
	e.mu.Unlock()
	if idx < len(e.brokerHBStop) && e.brokerHBStop[idx] != nil {
		e.brokerHBStop[idx]()
	}
	e.brokerSrvs[idx].Stop()
	e.brokers[idx].Close()
}

// brokerIdxFromAddr finds the index of a broker by its registered address.
func (e *chaosEnv) brokerIdxFromAddr(addr string) int {
	for i, a := range e.brokerAddrs {
		if a == addr {
			return i
		}
	}
	return -1
}

// ── Tests ────────────────────────────────────────────────────────────────────
//
// Tests are grouped under the four failure scenarios documented in
// docs/architecture.md#failure-scenarios:
//
//   Scenario 1 — Coordinator primary (Raft leader) dies
//   Scenario 2 — Coordinator backup (Raft follower) dies
//   Scenario 3 — Broker primary (partition leader) dies
//   Scenario 4 — Broker backup (partition follower) dies

// ── Scenario 4: Broker backup dies ───────────────────────────────────────────

// TestChaos_FollowerCrashShrinksISR kills one backup broker for a partition and
// verifies that within ~12s the FSM reflects the shrunken ISR. Produces against
// the leader continue to succeed throughout.
func TestChaos_FollowerCrashShrinksISR(t *testing.T) {
	if testing.Short() {
		t.Skip("ISR shrink takes >10s; skipped under -short")
	}

	env := newChaosEnv(t)
	leaderClient, _ := env.leaderClient(t)

	const topic = "isr-shrink"
	const part = int32(0)
	const partitions = int32(2)

	if _, err := leaderClient.CreateTopic(context.Background(), &pb.CreateTopicRequest{
		Topic: topic, Partitions: partitions,
	}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	leaderIdx, leaderRaftIdx := env.partitionLeader(t, topic, part)
	if leaderIdx < 0 {
		t.Fatal("no broker leader for partition 0")
	}
	_ = leaderRaftIdx // primary's broker index, kept for clarity

	// Pick a follower (any broker that isn't the leader of partition 0) and confirm
	// it has actually replicated something — that guarantees its first FetchReplica
	// completed, which is what registers it in the leader's replicas map.
	// Without that, monitorISR has no replica entry to drop.
	produceN(t, env.brokerCli[leaderIdx], topic, part, 5)
	var followerIdx int = -1
	for i := range env.brokers {
		if i != leaderIdx {
			followerIdx = i
			break
		}
	}
	if followerIdx < 0 {
		t.Fatal("no follower available")
	}
	waitReplicated(t, env.brokerCli[followerIdx], topic, part, 5)
	killTime := time.Now()
	t.Logf("[METRIC] killing follower broker idx=%d at T=0", followerIdx)
	env.killBroker(followerIdx)

	// Keep producing during the wait. The leader's monitorISR only refreshes a
	// follower's lastFetchTime when it streams a new message to that follower —
	// with no traffic for 10s, every follower (not just the dead one) would be
	// dropped as stale. A steady drip keeps live followers in-sync so only the
	// dead one ages out.
	stopProd := make(chan struct{})
	go func() {
		i := 0
		for {
			select {
			case <-stopProd:
				return
			default:
				_, _ = env.brokerCli[leaderIdx].Produce(context.Background(), &pb.ProduceRequest{
					Topic: topic, Partition: part, Value: []byte(fmt.Sprintf("drip-%d", i)),
				})
				i++
				time.Sleep(200 * time.Millisecond)
			}
		}
	}()
	defer close(stopProd)

	// Now wait for the leader to drop the follower from ISR. monitorISR reports
	// to the coordinator via AlterIsr → FSM CmdUpdateISR.
	deadline := time.Now().Add(chaosISRTimeout)
	deadBrokerID := int32(followerIdx + 1)
	leaderRaftCoord := env.raftNodes[mustLeaderIdx(env)]
	var lastSeen []int32
	for time.Now().Before(deadline) {
		ps, ok := leaderRaftCoord.FSM().Get(topic, part)
		if ok && ps.ISR != nil {
			lastSeen = ps.ISR
			stillIn := false
			for _, id := range ps.ISR {
				if id == deadBrokerID {
					stillIn = true
					break
				}
			}
			if !stillIn {
				t.Logf("[METRIC] ISR shrunk after %v; broker %d removed; ISR=%v", time.Since(killTime).Round(time.Millisecond), deadBrokerID, ps.ISR)
				goto produceCheck
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("ISR still contains dead broker %d after %v; last seen ISR=%v", deadBrokerID, chaosISRTimeout, lastSeen)

produceCheck:
	// Leader must still accept writes after the ISR shrink.
	if _, err := env.brokerCli[leaderIdx].Produce(context.Background(), &pb.ProduceRequest{
		Topic: topic, Partition: part, Value: []byte("post-shrink"),
	}); err != nil {
		t.Fatalf("post-shrink produce: %v", err)
	}
}

// ── Scenario 3: Broker primary dies ──────────────────────────────────────────

// TestChaos_LeaderCrashAutoFailover kills a leader broker and verifies that
// the coordinator's auto-failover loop (heartbeat detection → UpdatePartitionLeader
// fan-out) promotes a backup within ~chaosFailoverBudget. Pre-crash messages
// must survive on the new leader; fresh writes against the new leader must
// succeed.
func TestChaos_LeaderCrashAutoFailover(t *testing.T) {
	env := newChaosEnv(t)
	leaderClient, _ := env.leaderClient(t)

	const topic = "lead-auto"
	const part = int32(0)
	const preCount = 5

	if _, err := leaderClient.CreateTopic(context.Background(), &pb.CreateTopicRequest{
		Topic: topic, Partitions: 1,
	}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	leaderIdx, _ := env.partitionLeader(t, topic, part)
	if leaderIdx < 0 {
		t.Fatal("no broker leader for partition 0")
	}
	oldPrimary := env.brokerAddrs[leaderIdx]
	produceN(t, env.brokerCli[leaderIdx], topic, part, preCount)

	// Wait briefly for replication so backups have content; the highest-LEO
	// selection in pickNewLeader will then have something to compare.
	time.Sleep(500 * time.Millisecond)

	killTime := time.Now()
	t.Logf("[METRIC] killing leader broker idx=%d at T=0", leaderIdx)
	env.killBroker(leaderIdx)

	// Poll FSM until the primary changes.
	newPrimary := env.waitForNewPartitionPrimary(t, topic, part, oldPrimary, chaosFailoverBudget)
	t.Logf("[METRIC] new primary %s — failover in %v", newPrimary, time.Since(killTime).Round(time.Millisecond))

	newLeaderIdx := env.brokerIdxFromAddr(newPrimary)
	if newLeaderIdx < 0 {
		t.Fatalf("new primary %s not in brokerAddrs", newPrimary)
	}

	// Wait briefly for AssignRole(LEADER) to land on the new primary, then
	// produce. The FSM flips inside Raft Apply, but the broker's local p.role
	// flips later in assignLeaderRoles' AssignRole RPC fan-out.
	env.produceUntilLeader(t, newLeaderIdx, topic, part, []byte("post-failover"), 2*time.Second)

	// All pre-crash data is still readable from the new leader.
	msgs := consumeN(t, env.brokerCli[newLeaderIdx], topic, part, preCount, 5*time.Second)
	for i := range preCount {
		want := fmt.Sprintf("msg-%d", i)
		if got := string(msgs[i].Value); got != want {
			t.Errorf("pre-crash msg[%d] lost: want %q, got %q", i, want, got)
		}
	}
}

// ── Scenario 1: Coordinator primary dies ─────────────────────────────────────

// TestChaos_CoordinatorLeaderCrashKeepsDataPlane kills the Raft leader
// coordinator and asserts that the data plane keeps working (producers and
// consumers with already-cached metadata are uninterrupted) and that the FSM
// state survives on the new leader.
func TestChaos_CoordinatorLeaderCrashKeepsDataPlane(t *testing.T) {
	env := newChaosEnv(t)
	leaderClient, _ := env.leaderClient(t)

	const topic = "coord-down-dataplane"
	const part = int32(0)

	if _, err := leaderClient.CreateTopic(context.Background(), &pb.CreateTopicRequest{
		Topic: topic, Partitions: 1,
	}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	leaderBrokerIdx, _ := env.partitionLeader(t, topic, part)
	if leaderBrokerIdx < 0 {
		t.Fatal("no broker leader for partition 0")
	}

	preFSMOldLeader, _ := env.raftNodes[mustLeaderIdx(env)].FSM().Get(topic, part)

	// Kill Raft leader coordinator.
	oldLeaderIdx := mustLeaderIdx(env)
	killTime := time.Now()
	t.Logf("[METRIC] killing Raft leader coordinator idx=%d at T=0", oldLeaderIdx)
	env.killCoordinator(oldLeaderIdx)

	newIdx, _ := env.waitForNewLeader(t, oldLeaderIdx, chaosLeaderTimeout)
	t.Logf("[METRIC] new Raft leader idx=%d elected in %v", newIdx, time.Since(killTime).Round(time.Millisecond))

	// Data plane: a producer that already has the broker client cached can keep
	// hitting the leader broker directly.
	if _, err := env.brokerCli[leaderBrokerIdx].Produce(context.Background(), &pb.ProduceRequest{
		Topic: topic, Partition: part, Value: []byte("after-coord-down"),
	}); err != nil {
		t.Fatalf("post-coord-down produce: %v", err)
	}
	t.Logf("[METRIC] data plane produce succeeded %v after coordinator kill", time.Since(killTime).Round(time.Millisecond))

	// FSM partition placement survives on the new leader. IsLeader() returns true
	// before the no-op commit finishes, so FSM entries may not be applied yet —
	// poll briefly rather than checking once.
	var postFSMNewLeader kafkaraft.PartitionState
	deadline := time.Now().Add(3 * time.Second)
	for {
		ps, ok := env.raftNodes[newIdx].FSM().Get(topic, part)
		if ok {
			postFSMNewLeader = ps
			t.Logf("[METRIC] FSM entry visible on new leader after %v", time.Since(killTime).Round(time.Millisecond))
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("FSM lost partition on new leader after 3s")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if postFSMNewLeader.Primary != preFSMOldLeader.Primary {
		t.Errorf("FSM primary changed across leader switch: was %s, now %s",
			preFSMOldLeader.Primary, postFSMNewLeader.Primary)
	}
}

// TestChaos_CoordinatorLeaderCrashBrokerAutoReconnects verifies that after a
// coordinator leader crash, brokers discover the new leader via their peer list and
// re-register automatically. CreateTopic must succeed on the new leader without
// manual re-registration once brokers have heartbeated in.
//
// Note: the coordinator's broker registry is still NOT Raft-replicated — the new
// leader starts empty — but brokers self-heal via reconnectCoordinator within one
// heartbeat interval.
func TestChaos_CoordinatorLeaderCrashBrokerAutoReconnects(t *testing.T) {
	env := newChaosEnv(t)

	oldLeaderIdx, _ := env.leaderIndex()
	killTime := time.Now()
	t.Logf("[METRIC] killing Raft leader coordinator idx=%d at T=0", oldLeaderIdx)
	env.killCoordinator(oldLeaderIdx)

	newIdx, newAddr := env.waitForNewLeader(t, oldLeaderIdx, chaosLeaderTimeout)
	t.Logf("[METRIC] new Raft leader idx=%d at %s elected in %v", newIdx, newAddr, time.Since(killTime).Round(time.Millisecond))

	newClient := env.coordClient(t, newIdx)

	// Brokers reconnect on their next heartbeat tick (chaosHeartbeatTick=200ms).
	// Poll CreateTopic until it succeeds or the budget expires.
	deadline := time.Now().Add(chaosFailoverBudget)
	var lastErr error
	for time.Now().Before(deadline) {
		_, lastErr = newClient.CreateTopic(context.Background(), &pb.CreateTopicRequest{
			Topic: "post-reconnect", Partitions: 1,
		})
		if lastErr == nil {
			t.Logf("[METRIC] brokers auto-reconnected; CreateTopic succeeded %v after coordinator kill", time.Since(killTime).Round(time.Millisecond))
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("CreateTopic still failing %v after coordinator failover: %v", chaosFailoverBudget, lastErr)
}

// ── Scenario 2: Coordinator backup dies ──────────────────────────────────────

// TestChaos_CoordinatorBackupDies kills coordinator followers one at a time and
// verifies that the system is fully operational as long as quorum holds (3 of 5
// nodes alive), then loses cluster-management writes once quorum is broken.
//
// Key invariants checked at each step:
//   - Raft leader does not change while quorum holds.
//   - CreateTopic succeeds while quorum holds.
//   - Data plane (produce/consume) is uninterrupted throughout.
//   - Once 3 of 5 nodes are dead, CreateTopic fails (no Raft quorum).
//   - Data plane still works even after quorum loss.
func TestChaos_CoordinatorBackupDies(t *testing.T) {
	env := newChaosEnv(t)
	leaderClient, _ := env.leaderClient(t)

	const topic = "coord-backup-dies"
	const part = int32(0)

	if _, err := leaderClient.CreateTopic(context.Background(), &pb.CreateTopicRequest{
		Topic: topic, Partitions: 1,
	}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	brokerLeaderIdx, _ := env.partitionLeader(t, topic, part)
	if brokerLeaderIdx < 0 {
		t.Fatal("no broker leader for partition 0")
	}

	coordLeaderIdx, coordLeaderAddr := env.leaderIndex()
	if coordLeaderIdx < 0 {
		t.Fatal("no Raft leader at start")
	}
	t.Logf("coordinator Raft leader: idx=%d", coordLeaderIdx)

	// Kill followers one at a time. With 5 nodes, quorum=3 so we can lose 2.
	killed := 0
	for i := 0; i < chaosNumCoordinators; i++ {
		if i == coordLeaderIdx {
			continue // skip the leader
		}
		killTime := time.Now()
		t.Logf("[METRIC] killing coordinator follower idx=%d (kill #%d) at T=0", i, killed+1)
		env.killCoordinator(i)
		killed++

		time.Sleep(300 * time.Millisecond) // let any accidental election settle

		// Leader must not have changed.
		newLeaderIdx, _ := env.leaderIndex()
		if newLeaderIdx != coordLeaderIdx {
			t.Errorf("kill #%d: Raft leader changed from %d to %d", killed, coordLeaderIdx, newLeaderIdx)
		}

		if killed < 3 { // quorum intact (5-killed >= 3)
			// Cluster management must still work.
			_, err := leaderClient.CreateTopic(context.Background(), &pb.CreateTopicRequest{
				Topic: fmt.Sprintf("after-kill-%d", killed), Partitions: 1,
			})
			if err != nil {
				t.Errorf("kill #%d: CreateTopic failed while quorum intact: %v", killed, err)
			}

			// Data plane must still work.
			if _, err := env.brokerCli[brokerLeaderIdx].Produce(context.Background(), &pb.ProduceRequest{
				Topic: topic, Partition: part, Value: []byte(fmt.Sprintf("msg-after-kill-%d", killed)),
			}); err != nil {
				t.Errorf("kill #%d: Produce failed while quorum intact: %v", killed, err)
			}

			t.Logf("[METRIC] kill #%d: CreateTopic+Produce succeeded %v after kill — no disruption",
				killed, time.Since(killTime).Round(time.Millisecond))
		}

		if killed == 2 {
			break // stop before losing quorum; quorum-loss is TestChaos_CoordinatorQuorumLoss
		}
	}

	// After 2 follower kills (3 nodes alive), system is fully operational.
	// Verify all messages produced above are readable.
	msgs := consumeN(t, env.brokerCli[brokerLeaderIdx], topic, part, killed, 5*time.Second)
	if len(msgs) != killed {
		t.Fatalf("expected %d messages after %d follower kills, got %d", killed, killed, len(msgs))
	}

	// Re-verify CreateTopic still works with 3-node quorum.
	if _, err := env.coordClient(t, coordLeaderIdx).CreateTopic(context.Background(), &pb.CreateTopicRequest{
		Topic: "final-quorum-check", Partitions: 1,
	}); err != nil {
		t.Fatalf("CreateTopic failed with 3-node quorum: %v", err)
	}
	t.Logf("3-node quorum healthy: coord leader=%s, %d follower(s) killed", coordLeaderAddr, killed)
}

// TestChaos_CoordinatorQuorumLoss kills 3 of 5 coordinators (majority gone).
// No surviving node can become leader; writes against survivors fail.
func TestChaos_CoordinatorQuorumLoss(t *testing.T) {
	if testing.Short() {
		t.Skip("quorum loss wait can exceed default Raft election timeout; skipped under -short")
	}

	env := newChaosEnv(t)

	killTime := time.Now()
	for i := 0; i < 3; i++ {
		t.Logf("[METRIC] killing coordinator idx=%d", i)
		env.killCoordinator(i)
	}
	t.Logf("[METRIC] 3/5 coordinators killed at T=0 (relative to first kill)")

	// Within 10s, no live node should be the leader.
	deadline := time.Now().Add(chaosLeaderTimeout)
	for time.Now().Before(deadline) {
		idx, _ := env.leaderIndex()
		if idx < 0 {
			t.Logf("[METRIC] quorum lost (no leader) %v after kills", time.Since(killTime).Round(time.Millisecond))
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if idx, _ := env.leaderIndex(); idx >= 0 {
		t.Fatalf("unexpected leader idx=%d after killing 3/5 coordinators", idx)
	}

	// CreateTopic against either survivor should fail with FailedPrecondition.
	for i := 3; i < chaosNumCoordinators; i++ {
		cc := env.coordClient(t, i)
		_, err := cc.CreateTopic(context.Background(), &pb.CreateTopicRequest{
			Topic: "quorum-loss", Partitions: 1,
		})
		if err == nil {
			t.Fatalf("survivor %d accepted CreateTopic; expected failure", i)
		}
	}
}

// (continued Scenario 3)

// TestChaos_RollingBrokerRestart kills 3 leader brokers in succession and
// relies on the coordinator's auto-failover loop to promote a backup each time.
// No manual UpdatePartitionLeader. End-state: every message produced across all
// rounds is consumable from the final leader.
func TestChaos_RollingBrokerRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("rolling restart is slow due to replication settle time")
	}

	env := newChaosEnv(t)
	leaderClient, _ := env.leaderClient(t)

	const topic = "rolling"
	const part = int32(0)
	const writesPerRound = 4
	const rounds = 3

	if _, err := leaderClient.CreateTopic(context.Background(), &pb.CreateTopicRequest{
		Topic: topic, Partitions: 1,
	}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	currentLeader, _ := env.partitionLeader(t, topic, part)
	currentAddr := env.brokerAddrs[currentLeader]
	totalWritten := 0

	for r := 0; r < rounds; r++ {
		// First message of every round: retry until accepted. On round 0 the
		// initial leader is ready; on rounds > 0 we may race AssignRole landing
		// on the new leader.
		firstVal := fmt.Sprintf("round-%d-msg-0", r)
		env.produceUntilLeader(t, currentLeader, topic, part, []byte(firstVal), 2*time.Second)
		totalWritten++

		// Subsequent writes can use plain Produce — the broker is leader by now.
		for i := 1; i < writesPerRound; i++ {
			val := fmt.Sprintf("round-%d-msg-%d", r, i)
			if _, err := env.brokerCli[currentLeader].Produce(context.Background(), &pb.ProduceRequest{
				Topic: topic, Partition: part, Value: []byte(val),
			}); err != nil {
				t.Fatalf("round %d msg %d: %v", r, i, err)
			}
			totalWritten++
		}

		// Wait until at least one live backup has caught up to totalWritten —
		// the failover loop picks highest-LEO; if no backup has the full log,
		// data is lost on the failover (real correctness issue, but not the
		// scenario this test is exercising).
		env.waitForCaughtUpBackup(t, topic, part, currentLeader, totalWritten, 5*time.Second)

		roundKillTime := time.Now()
		t.Logf("[METRIC] round %d: killing leader idx=%d (%s) at T=0", r, currentLeader, currentAddr)
		env.killBroker(currentLeader)

		// Wait for auto-failover to publish a new primary.
		newPrimary := env.waitForNewPartitionPrimary(t, topic, part, currentAddr, chaosFailoverBudget)
		newLeader := env.brokerIdxFromAddr(newPrimary)
		if newLeader < 0 {
			t.Fatalf("round %d: new primary %s not in brokerAddrs", r, newPrimary)
		}
		t.Logf("[METRIC] round %d: new leader idx=%d (%s) — failover in %v",
			r, newLeader, newPrimary, time.Since(roundKillTime).Round(time.Millisecond))

		currentLeader = newLeader
		currentAddr = newPrimary
	}

	// Final assertion: all writes consumable from final leader.
	msgs := consumeN(t, env.brokerCli[currentLeader], topic, part, totalWritten, 10*time.Second)
	if len(msgs) != totalWritten {
		t.Fatalf("expected %d messages, got %d", totalWritten, len(msgs))
	}
	for i := 0; i < totalWritten; i++ {
		round := i / writesPerRound
		within := i % writesPerRound
		want := fmt.Sprintf("round-%d-msg-%d", round, within)
		if got := string(msgs[i].Value); got != want {
			t.Errorf("msg[%d]: want %q, got %q", i, want, got)
		}
	}
}

// (continued Scenario 3)

// TestChaos_LeaderCrashFastDetection verifies that when a partition leader dies,
// its followers' broken FetchReplica streams trigger ReportLeaderUnreachable,
// causing the coordinator to fail over well within chaosFailoverBudget — without
// relying on the deadAfter heartbeat timeout.
func TestChaos_LeaderCrashFastDetection(t *testing.T) {
	env := newChaosEnv(t)
	leaderClient, _ := env.leaderClient(t)

	const topic = "fast-detect"
	const part = int32(0)
	const preCount = 3

	if _, err := leaderClient.CreateTopic(context.Background(), &pb.CreateTopicRequest{
		Topic: topic, Partitions: 1,
	}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	leaderIdx, _ := env.partitionLeader(t, topic, part)
	if leaderIdx < 0 {
		t.Fatal("no broker leader for partition 0")
	}
	oldPrimary := env.brokerAddrs[leaderIdx]
	produceN(t, env.brokerCli[leaderIdx], topic, part, preCount)

	// Wait for at least one follower to have an active FetchReplica stream so
	// the fast-path can fire when the leader dies.
	var followerIdx int = -1
	for i := range env.brokers {
		if i != leaderIdx {
			followerIdx = i
			break
		}
	}
	waitReplicated(t, env.brokerCli[followerIdx], topic, part, preCount)

	killTime := time.Now()
	t.Logf("[METRIC] killing leader broker idx=%d at T=0", leaderIdx)
	env.killBroker(leaderIdx)

	// Follower should report the dead leader immediately via ReportLeaderUnreachable.
	// Full failover must complete within chaosFailoverBudget (was deadAfter=10s, now ~1s).
	newPrimary := env.waitForNewPartitionPrimary(t, topic, part, oldPrimary, chaosFailoverBudget)
	t.Logf("[METRIC] new primary %s — fast failover in %v", newPrimary, time.Since(killTime).Round(time.Millisecond))

	newLeaderIdx := env.brokerIdxFromAddr(newPrimary)
	if newLeaderIdx < 0 {
		t.Fatalf("new primary %s not in brokerAddrs", newPrimary)
	}
	env.produceUntilLeader(t, newLeaderIdx, topic, part, []byte("post-fast-failover"), 2*time.Second)
}

// ── Helpers ──────────────────────────────────────────────────────────────────

// waitForNewPartitionPrimary polls the Raft leader's FSM until the primary for
// (topic, part) is different from oldPrimary, or the budget expires. Used to
// wait for the auto-failover loop to land.
//
// Note: the FSM flips inside Raft Apply, but the broker's local p.role flips
// slightly later, in assignLeaderRoles' AssignRole fan-out. Callers that want
// to Produce immediately should retry on FailedPrecondition for a few hundred
// ms.
func (e *chaosEnv) waitForNewPartitionPrimary(t *testing.T, topic string, part int32, oldPrimary string, budget time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		idx, _ := e.leaderIndex()
		if idx >= 0 {
			ps, ok := e.raftNodes[idx].FSM().Get(topic, part)
			if ok && ps.Primary != "" && ps.Primary != oldPrimary {
				return ps.Primary
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("partition %s/%d primary still %q after %v", topic, part, oldPrimary, budget)
	return ""
}

// waitForCaughtUpBackup polls live (non-leader, non-dead) brokers until at
// least one has the full log replicated. Without this, the failover loop's
// highest-LEO selection still picks the most-replicated broker but may be
// behind the leader.
func (e *chaosEnv) waitForCaughtUpBackup(t *testing.T, topic string, part int32, leaderIdx, want int, budget time.Duration) {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		for i := range e.brokers {
			if i == leaderIdx {
				continue
			}
			e.mu.Lock()
			dead := e.deadBrokers[i]
			e.mu.Unlock()
			if dead {
				continue
			}
			if rawLogLen(e.brokerCli[i], topic, part, want) >= want {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("no backup of %s/%d caught up to %d within %v", topic, part, want, budget)
}

// produceUntilLeader retries Produce against brokerIdx until it succeeds or
// budget expires. Use this just after a failover when the FSM says brokerIdx
// is the new primary but the broker may not yet have processed AssignRole.
func (e *chaosEnv) produceUntilLeader(t *testing.T, brokerIdx int, topic string, part int32, value []byte, budget time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	for {
		_, err := e.brokerCli[brokerIdx].Produce(ctx, &pb.ProduceRequest{
			Topic: topic, Partition: part, Value: value,
		})
		if err == nil {
			return
		}
		if ctx.Err() != nil {
			t.Fatalf("produce against new leader %d for %s/%d after %v: %v", brokerIdx, topic, part, budget, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// partitionLeader returns the broker index of the current leader of (topic, part)
// and the Raft leader's index. Resolves the leader via GetMetadata to mirror
// what a real producer would see.
func (e *chaosEnv) partitionLeader(t *testing.T, topic string, part int32) (int, int) {
	t.Helper()
	idx, addr := e.leaderIndex()
	if idx < 0 {
		t.Fatal("no Raft leader")
	}
	cc := e.coordClient(t, idx)
	_ = addr
	resp, err := cc.GetMetadata(context.Background(), &pb.MetadataRequest{Topic: topic})
	if err != nil {
		t.Fatalf("GetMetadata: %v", err)
	}
	for _, p := range resp.Partitions {
		if p.Partition == part {
			return e.brokerIdxFromAddr(p.BrokerAddr), idx
		}
	}
	t.Fatalf("partition %d not in metadata for %s", part, topic)
	return -1, idx
}

// mustLeaderIdx returns the current Raft leader index or fatals.
func mustLeaderIdx(e *chaosEnv) int {
	for i, n := range e.raftNodes {
		e.mu.Lock()
		dead := e.deadCoords[i]
		e.mu.Unlock()
		if dead {
			continue
		}
		if n.IsLeader() {
			return i
		}
	}
	e.t.Fatal("no Raft leader")
	return -1
}
