package producer_test

import (
	"context"
	"testing"

	"github.com/thanhlongnt/kafka-lite/internal/producer"
	pb "github.com/thanhlongnt/kafka-lite/internal/proto/kafka_lite"
	"github.com/thanhlongnt/kafka-lite/internal/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newProducer(t *testing.T) (*producer.Producer, pb.BrokerClient) {
	t.Helper()
	dialer := testutil.StartBroker(t)
	opt := grpc.WithContextDialer(dialer)

	p, err := producer.New(testutil.BufconnAddr, opt)
	if err != nil {
		t.Fatalf("producer.New: %v", err)
	}
	t.Cleanup(func() { p.Close() })

	// Also return a direct client for setup (e.g. CreateTopic).
	conn := testutil.NewConn(t, dialer)
	return p, pb.NewBrokerClient(conn)
}

// TestSend verifies that Send returns a monotonically increasing offset.
func TestSend(t *testing.T) {
	p, client := newProducer(t)
	ctx := context.Background()

	_, err := client.CreateTopic(ctx, &pb.CreateTopicRequest{Topic: "t", Partitions: 1})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	for i := range 5 {
		offset, err := p.Send(ctx, "t", 0, nil, []byte("msg"))
		if err != nil {
			t.Fatalf("Send[%d]: %v", i, err)
		}
		if offset != int64(i) {
			t.Fatalf("expected offset %d, got %d", i, offset)
		}
	}
}

// TestSendUnknownTopic verifies that Send on a missing topic returns NotFound.
func TestSendUnknownTopic(t *testing.T) {
	p, _ := newProducer(t)
	_, err := p.Send(context.Background(), "ghost", 0, nil, []byte("x"))
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", err)
	}
}

// TestSendMultiplePartitions verifies offsets are independent per partition.
func TestSendMultiplePartitions(t *testing.T) {
	p, client := newProducer(t)
	ctx := context.Background()

	_, err := client.CreateTopic(ctx, &pb.CreateTopicRequest{Topic: "mp", Partitions: 3})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Produce 3 messages to partition 0, 1 to partition 1.
	for range 3 {
		if _, err := p.Send(ctx, "mp", 0, nil, []byte("a")); err != nil {
			t.Fatalf("Send p0: %v", err)
		}
	}
	offset, err := p.Send(ctx, "mp", 1, nil, []byte("b"))
	if err != nil {
		t.Fatalf("Send p1: %v", err)
	}
	if offset != 0 {
		t.Fatalf("partition 1 offset: expected 0, got %d", offset)
	}
}
