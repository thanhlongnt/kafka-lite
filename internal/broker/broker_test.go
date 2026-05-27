package broker_test

import (
	"context"
	"io"
	"sync"
	"testing"

	"github.com/thanhlongnt/kafka-lite/internal/broker"
	pb "github.com/thanhlongnt/kafka-lite/internal/proto/kafka_lite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"net"
)

const bufSize = 1 << 20

func newTestServer(t *testing.T) pb.BrokerClient {
	t.Helper()
	return serveAndConnect(t, broker.New())
}

func newTestServerWithDataDir(t *testing.T, dataDir string) (pb.BrokerClient, func()) {
	t.Helper()
	b, err := broker.NewWithDataDir(dataDir)
	if err != nil {
		t.Fatalf("NewWithDataDir: %v", err)
	}
	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	pb.RegisterBrokerServer(srv, b)
	go func() {
		if err := srv.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			t.Logf("server error: %v", err)
		}
	}()
	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	stop := func() { srv.Stop(); lis.Close(); conn.Close() }
	return pb.NewBrokerClient(conn), stop
}

func serveAndConnect(t *testing.T, b *broker.Broker) pb.BrokerClient {
	t.Helper()
	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	pb.RegisterBrokerServer(srv, b)
	go func() {
		if err := srv.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			t.Logf("server error: %v", err)
		}
	}()
	t.Cleanup(func() { srv.Stop(); lis.Close() })

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}
	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewBrokerClient(conn)
}

func mustCreateTopic(t *testing.T, client pb.BrokerClient, topic string, partitions int32) {
	t.Helper()
	_, err := client.CreateTopic(context.Background(), &pb.CreateTopicRequest{
		Topic:      topic,
		Partitions: partitions,
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
}

// TestCreateTopic verifies basic topic creation and duplicate rejection.
func TestCreateTopic(t *testing.T) {
	client := newTestServer(t)

	_, err := client.CreateTopic(context.Background(), &pb.CreateTopicRequest{Topic: "t1", Partitions: 3})
	if err != nil {
		t.Fatalf("first CreateTopic: %v", err)
	}

	_, err = client.CreateTopic(context.Background(), &pb.CreateTopicRequest{Topic: "t1", Partitions: 3})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("expected AlreadyExists, got %v", err)
	}
}

// TestProduceFetch verifies that a produced message is delivered to a Fetch stream.
func TestProduceFetch(t *testing.T) {
	client := newTestServer(t)
	mustCreateTopic(t, client, "events", 1)

	_, err := client.Produce(context.Background(), &pb.ProduceRequest{
		Topic: "events", Partition: 0, Value: []byte("hello"),
	})
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := client.Fetch(ctx, &pb.FetchRequest{Topic: "events", Partition: 0, StartOffset: 0})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	msg, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if string(msg.Value) != "hello" {
		t.Fatalf("expected \"hello\", got %q", msg.Value)
	}
	if msg.Offset != 0 {
		t.Fatalf("expected offset 0, got %d", msg.Offset)
	}
}

// TestFetchUnknownTopic verifies that Fetch on a missing topic returns NotFound.
func TestFetchUnknownTopic(t *testing.T) {
	client := newTestServer(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := client.Fetch(ctx, &pb.FetchRequest{Topic: "ghost", Partition: 0})
	if err != nil {
		t.Fatalf("Fetch call: %v", err)
	}
	_, err = stream.Recv()
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", err)
	}
}

// TestFetchStreamsMessagesProducedAfterOpen verifies that Fetch streams messages
// that are produced after the stream was opened.
func TestFetchStreamsMessagesProducedAfterOpen(t *testing.T) {
	client := newTestServer(t)
	mustCreateTopic(t, client, "live", 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := client.Fetch(ctx, &pb.FetchRequest{Topic: "live", Partition: 0, StartOffset: 0})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	// Produce after stream is open.
	go func() {
		_, _ = client.Produce(context.Background(), &pb.ProduceRequest{
			Topic: "live", Partition: 0, Value: []byte("late"),
		})
	}()

	msg, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if string(msg.Value) != "late" {
		t.Fatalf("expected \"late\", got %q", msg.Value)
	}
}

// TestConcurrentProducesOrderedOnFetch verifies that messages from concurrent
// producers arrive in append order on the Fetch stream (no gaps, no duplicates).
func TestConcurrentProducesOrderedOnFetch(t *testing.T) {
	const n = 50
	client := newTestServer(t)
	mustCreateTopic(t, client, "concurrent", 1)

	// Produce n messages concurrently.
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := client.Produce(context.Background(), &pb.ProduceRequest{
				Topic: "concurrent", Partition: 0, Value: []byte("x"),
			})
			if err != nil {
				t.Errorf("Produce: %v", err)
			}
		}()
	}
	wg.Wait()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := client.Fetch(ctx, &pb.FetchRequest{Topic: "concurrent", Partition: 0, StartOffset: 0})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	for i := 0; i < n; i++ {
		msg, err := stream.Recv()
		if err != nil {
			t.Fatalf("Recv[%d]: %v", i, err)
		}
		if msg.Offset != int64(i) {
			t.Fatalf("message %d has offset %d", i, msg.Offset)
		}
	}
}

// TestBrokerRestoreFromDataDir verifies that topics and messages survive a broker restart.
func TestBrokerRestoreFromDataDir(t *testing.T) {
	dir := t.TempDir()
	const topic = "restore-test"
	const partitions = 2
	const msgsPerPartition = 5

	// --- first broker: create topic, produce messages, stop ---
	client1, stop1 := newTestServerWithDataDir(t, dir)
	mustCreateTopic(t, client1, topic, partitions)

	for p := int32(0); p < partitions; p++ {
		for i := 0; i < msgsPerPartition; i++ {
			_, err := client1.Produce(context.Background(), &pb.ProduceRequest{
				Topic:     topic,
				Partition: p,
				Value:     []byte{byte(p*10 + int32(i))},
			})
			if err != nil {
				t.Fatalf("Produce p%d[%d]: %v", p, i, err)
			}
		}
	}
	stop1()

	// --- second broker: open same data dir, verify state is restored ---
	client2, stop2 := newTestServerWithDataDir(t, dir)
	defer stop2()

	// CreateTopic should return AlreadyExists — topic was restored from disk.
	_, err := client2.CreateTopic(context.Background(), &pb.CreateTopicRequest{
		Topic: topic, Partitions: partitions,
	})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("expected AlreadyExists for restored topic, got %v", err)
	}

	// All messages should be readable from each partition.
	for p := int32(0); p < partitions; p++ {
		ctx, cancel := context.WithCancel(context.Background())
		stream, err := client2.Fetch(ctx, &pb.FetchRequest{
			Topic: topic, Partition: p, StartOffset: 0,
		})
		if err != nil {
			cancel()
			t.Fatalf("Fetch p%d: %v", p, err)
		}
		for i := 0; i < msgsPerPartition; i++ {
			msg, err := stream.Recv()
			if err != nil {
				cancel()
				t.Fatalf("Recv p%d[%d]: %v", p, i, err)
			}
			if msg.Offset != int64(i) {
				t.Errorf("p%d[%d]: expected offset %d, got %d", p, i, i, msg.Offset)
			}
			if msg.Value[0] != byte(p*10+int32(i)) {
				t.Errorf("p%d[%d]: unexpected value %d", p, i, msg.Value[0])
			}
		}
		cancel()
	}
}

// TestMultiplePartitions verifies that partitions are independent.
func TestMultiplePartitions(t *testing.T) {
	client := newTestServer(t)
	mustCreateTopic(t, client, "multi", 2)

	for _, p := range []int32{0, 1} {
		_, err := client.Produce(context.Background(), &pb.ProduceRequest{
			Topic: "multi", Partition: p, Value: []byte{byte(p)},
		})
		if err != nil {
			t.Fatalf("Produce p%d: %v", p, err)
		}
	}

	for _, p := range []int32{0, 1} {
		ctx, cancel := context.WithCancel(context.Background())
		stream, err := client.Fetch(ctx, &pb.FetchRequest{Topic: "multi", Partition: p, StartOffset: 0})
		if err != nil {
			cancel()
			t.Fatalf("Fetch p%d: %v", p, err)
		}
		msg, err := stream.Recv()
		cancel()
		if err != nil && err != io.EOF {
			t.Fatalf("Recv p%d: %v", p, err)
		}
		if msg == nil {
			t.Fatalf("p%d: no message received", p)
		}
		if msg.Value[0] != byte(p) {
			t.Fatalf("p%d: expected value %d, got %d", p, p, msg.Value[0])
		}
	}
}
