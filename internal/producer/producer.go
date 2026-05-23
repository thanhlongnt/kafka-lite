package producer

import (
	"context"
	"fmt"

	pb "github.com/thanhlongnt/kafka-lite/internal/proto/kafka_lite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Producer sends messages to a single broker. Phase 1 — no partition routing;
// the caller specifies the target partition explicitly.
type Producer struct {
	client pb.BrokerClient
	conn   *grpc.ClientConn
}

// New dials brokerAddr and returns a ready Producer.
// Extra dial options (e.g. a bufconn dialer for tests) may be passed as opts.
func New(brokerAddr string, opts ...grpc.DialOption) (*Producer, error) {
	opts = append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, opts...)
	conn, err := grpc.NewClient(brokerAddr, opts...)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", brokerAddr, err)
	}
	return &Producer{client: pb.NewBrokerClient(conn), conn: conn}, nil
}

// Send appends a message to topic/partition and returns the committed offset.
func (p *Producer) Send(ctx context.Context, topic string, partition int32, key, value []byte) (int64, error) {
	resp, err := p.client.Produce(ctx, &pb.ProduceRequest{
		Topic:     topic,
		Partition: partition,
		Key:       key,
		Value:     value,
	})
	if err != nil {
		return 0, err
	}
	return resp.Offset, nil
}

// Close releases the underlying gRPC connection.
func (p *Producer) Close() error {
	return p.conn.Close()
}
