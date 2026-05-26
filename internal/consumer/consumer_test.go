package consumer_test

import (
	"context"
	"testing"
	"time"

	"github.com/thanhlongnt/kafka-lite/internal/consumer"
	pb "github.com/thanhlongnt/kafka-lite/internal/proto/kafka_lite"
	"github.com/thanhlongnt/kafka-lite/internal/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func setup(t *testing.T, topic string, partitions int32) (pb.BrokerClient, func(tp string, partition int32, startOffset int64) *consumer.Consumer) {
	t.Helper()
	dialer := testutil.StartBroker(t)
	conn := testutil.NewConn(t, dialer)
	client := pb.NewBrokerClient(conn)

	if topic != "" {
		_, err := client.CreateTopic(context.Background(), &pb.CreateTopicRequest{Topic: topic, Partitions: partitions})
		if err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}
	}

	opt := grpc.WithContextDialer(dialer)
	newConsumer := func(tp string, partition int32, startOffset int64) *consumer.Consumer {
		c, err := consumer.New(testutil.BufconnAddr, tp, partition, startOffset, opt)
		if err != nil {
			t.Fatalf("consumer.New: %v", err)
		}
		t.Cleanup(func() { c.Close() })
		return c
	}
	return client, newConsumer
}

// TestPollReceivesProducedMessages verifies basic produce → poll delivery.
func TestPollReceivesProducedMessages(t *testing.T) {
	client, newConsumer := setup(t, "events", 1)
	ctx := context.Background()

	for i, v := range []string{"a", "b", "c"} {
		_, err := client.Produce(ctx, &pb.ProduceRequest{Topic: "events", Partition: 0, Value: []byte(v)})
		if err != nil {
			t.Fatalf("Produce[%d]: %v", i, err)
		}
	}

	c := newConsumer("events", 0, 0)
	for i, want := range []string{"a", "b", "c"} {
		msg, err := c.Poll(ctx)
		if err != nil {
			t.Fatalf("Poll[%d]: %v", i, err)
		}
		if string(msg.Value) != want {
			t.Fatalf("message %d: expected %q, got %q", i, want, msg.Value)
		}
		if msg.Offset != int64(i) {
			t.Fatalf("message %d: expected offset %d, got %d", i, i, msg.Offset)
		}
	}
}

// TestPollStartOffset verifies that a consumer starting at a non-zero offset
// skips earlier messages.
func TestPollStartOffset(t *testing.T) {
	client, newConsumer := setup(t, "offsets", 1)
	ctx := context.Background()

	for _, v := range []string{"skip", "skip", "want"} {
		_, err := client.Produce(ctx, &pb.ProduceRequest{Topic: "offsets", Partition: 0, Value: []byte(v)})
		if err != nil {
			t.Fatalf("Produce: %v", err)
		}
	}

	c := newConsumer("offsets", 0, 2) // start at offset 2
	msg, err := c.Poll(ctx)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if string(msg.Value) != "want" {
		t.Fatalf("expected \"want\", got %q", msg.Value)
	}
}

// TestPollBlocksUntilProduced verifies that Poll waits for a message that
// does not yet exist when the consumer opens the stream.
func TestPollBlocksUntilProduced(t *testing.T) {
	client, newConsumer := setup(t, "live", 1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	c := newConsumer("live", 0, 0)

	received := make(chan string, 1)
	go func() {
		msg, err := c.Poll(ctx)
		if err != nil {
			received <- "error: " + err.Error()
			return
		}
		received <- string(msg.Value)
	}()

	time.Sleep(20 * time.Millisecond) // let Poll block
	_, err := client.Produce(context.Background(), &pb.ProduceRequest{
		Topic: "live", Partition: 0, Value: []byte("hello"),
	})
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}

	select {
	case got := <-received:
		if got != "hello" {
			t.Fatalf("expected \"hello\", got %q", got)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for Poll to return")
	}
}

// TestOffsetTracking verifies that Offset() advances after each Poll.
func TestOffsetTracking(t *testing.T) {
	client, newConsumer := setup(t, "track", 1)
	ctx := context.Background()

	for range 3 {
		_, err := client.Produce(ctx, &pb.ProduceRequest{Topic: "track", Partition: 0, Value: []byte("x")})
		if err != nil {
			t.Fatalf("Produce: %v", err)
		}
	}

	c := newConsumer("track", 0, 0)
	for i := range 3 {
		if _, err := c.Poll(ctx); err != nil {
			t.Fatalf("Poll[%d]: %v", i, err)
		}
		if c.Offset() != int64(i+1) {
			t.Fatalf("after Poll[%d]: expected Offset()=%d, got %d", i, i+1, c.Offset())
		}
	}
}

// TestPollCancelledContext verifies that Poll respects context cancellation.
func TestPollCancelledContext(t *testing.T) {
	_, newConsumer := setup(t, "cancel", 1)

	ctx, cancel := context.WithCancel(context.Background())
	c := newConsumer("cancel", 0, 0)

	done := make(chan error, 1)
	go func() {
		_, err := c.Poll(ctx)
		done <- err
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error after cancel, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Poll did not return after context cancellation")
	}
}

// TestPollUnknownTopic verifies that Poll on a missing topic returns NotFound.
func TestPollUnknownTopic(t *testing.T) {
	_, newConsumer := setup(t, "", 0)
	c := newConsumer("ghost", 0, 0)
	_, err := c.Poll(context.Background())
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", err)
	}
}

