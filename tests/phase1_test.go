package tests

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/thanhlongnt/kafka-lite/internal/consumer"
	"github.com/thanhlongnt/kafka-lite/internal/producer"
	pb "github.com/thanhlongnt/kafka-lite/internal/proto/kafka_lite"
	"github.com/thanhlongnt/kafka-lite/internal/testutil"
	"google.golang.org/grpc"
)

// phase1Env spins up a broker and returns factory functions for producers/consumers
// and a direct broker client for setup.
type phase1Env struct {
	client      pb.BrokerClient
	newProducer func() *producer.Producer
	newConsumer func(topic string, partition int32, startOffset int64) *consumer.Consumer
}

func newPhase1Env(t *testing.T) *phase1Env {
	t.Helper()
	dialer := testutil.StartBroker(t)
	opt := grpc.WithContextDialer(dialer)

	conn := testutil.NewConn(t, dialer)
	e := &phase1Env{
		client: pb.NewBrokerClient(conn),
		newProducer: func() *producer.Producer {
			p, err := producer.New(testutil.BufconnAddr, opt)
			if err != nil {
				t.Fatalf("producer.New: %v", err)
			}
			t.Cleanup(func() { p.Close() })
			return p
		},
		newConsumer: func(topic string, partition int32, startOffset int64) *consumer.Consumer {
			c, err := consumer.New(testutil.BufconnAddr, topic, partition, startOffset, opt)
			if err != nil {
				t.Fatalf("consumer.New: %v", err)
			}
			t.Cleanup(func() { c.Close() })
			return c
		},
	}
	return e
}

func (e *phase1Env) createTopic(t *testing.T, topic string, partitions int32) {
	t.Helper()
	_, err := e.client.CreateTopic(context.Background(), &pb.CreateTopicRequest{
		Topic: topic, Partitions: partitions,
	})
	if err != nil {
		t.Fatalf("CreateTopic %q: %v", topic, err)
	}
}

// TestPhase1_SingleProducerSingleConsumer: one producer, one consumer, N messages in order.
func TestPhase1_SingleProducerSingleConsumer(t *testing.T) {
	const n = 20
	e := newPhase1Env(t)
	e.createTopic(t, "spsc", 1)

	p := e.newProducer()
	c := e.newConsumer("spsc", 0, 0)
	ctx := context.Background()

	for i := range n {
		if _, err := p.Send(ctx, "spsc", 0, nil, []byte{byte(i)}); err != nil {
			t.Fatalf("Send[%d]: %v", i, err)
		}
	}

	for i := range n {
		msg, err := c.Poll(ctx)
		if err != nil {
			t.Fatalf("Poll[%d]: %v", i, err)
		}
		if msg.Value[0] != byte(i) {
			t.Fatalf("message %d: expected value %d, got %d", i, i, msg.Value[0])
		}
		if msg.Offset != int64(i) {
			t.Fatalf("message %d: expected offset %d, got %d", i, i, msg.Offset)
		}
	}
}

// TestPhase1_MultipleConsumers: two consumers independently read the same partition
// from offset 0 and receive all messages in the same order.
func TestPhase1_MultipleConsumers(t *testing.T) {
	const n = 10
	e := newPhase1Env(t)
	e.createTopic(t, "mc", 1)

	p := e.newProducer()
	ctx := context.Background()
	for i := range n {
		if _, err := p.Send(ctx, "mc", 0, nil, []byte{byte(i)}); err != nil {
			t.Fatalf("Send[%d]: %v", i, err)
		}
	}

	check := func(name string, c *consumer.Consumer) {
		for i := range n {
			msg, err := c.Poll(ctx)
			if err != nil {
				t.Errorf("%s Poll[%d]: %v", name, i, err)
				return
			}
			if msg.Value[0] != byte(i) {
				t.Errorf("%s message %d: expected %d, got %d", name, i, i, msg.Value[0])
			}
		}
	}

	var wg sync.WaitGroup
	for _, name := range []string{"c1", "c2"} {
		wg.Add(1)
		c := e.newConsumer("mc", 0, 0)
		go func(name string, c *consumer.Consumer) {
			defer wg.Done()
			check(name, c)
		}(name, c)
	}
	wg.Wait()
}

// TestPhase1_MultipleProducers: concurrent producers write to the same partition;
// a consumer reads all messages with no gaps and no duplicate offsets.
func TestPhase1_MultipleProducers(t *testing.T) {
	const (
		numProducers = 5
		msgsEach     = 10
	)
	e := newPhase1Env(t)
	e.createTopic(t, "mp", 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for range numProducers {
		wg.Add(1)
		p := e.newProducer()
		go func() {
			defer wg.Done()
			for range msgsEach {
				if _, err := p.Send(ctx, "mp", 0, nil, []byte("x")); err != nil {
					t.Errorf("Send: %v", err)
				}
			}
		}()
	}
	wg.Wait()

	total := numProducers * msgsEach
	c := e.newConsumer("mp", 0, 0)
	seen := make(map[int64]bool, total)
	for range total {
		msg, err := c.Poll(ctx)
		if err != nil {
			t.Fatalf("Poll: %v", err)
		}
		if seen[msg.Offset] {
			t.Fatalf("duplicate offset %d", msg.Offset)
		}
		seen[msg.Offset] = true
	}
	if len(seen) != total {
		t.Fatalf("expected %d unique offsets, got %d", total, len(seen))
	}
}
