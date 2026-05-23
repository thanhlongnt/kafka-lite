package consumer

import (
	"context"
	"fmt"
	"io"

	pb "github.com/thanhlongnt/kafka-lite/internal/proto/kafka_lite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Consumer reads messages from a single topic/partition, tracking its own offset.
// It opens a server-streaming Fetch RPC and reconnects automatically on stream errors.
type Consumer struct {
	client     pb.BrokerClient
	conn       *grpc.ClientConn
	topic      string
	partition  int32
	nextOffset int64

	stream pb.Broker_FetchClient
}

// New dials brokerAddr and returns a Consumer ready to read from topic/partition
// starting at startOffset.
// Extra dial options (e.g. a bufconn dialer for tests) may be passed as opts.
func New(brokerAddr, topic string, partition int32, startOffset int64, opts ...grpc.DialOption) (*Consumer, error) {
	opts = append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, opts...)
	conn, err := grpc.NewClient(brokerAddr, opts...)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", brokerAddr, err)
	}
	return &Consumer{
		client:     pb.NewBrokerClient(conn),
		conn:       conn,
		topic:      topic,
		partition:  partition,
		nextOffset: startOffset,
	}, nil
}

// Poll blocks until the next message is available or ctx is cancelled.
// On stream EOF or transport error it reconnects from the last received offset.
func (c *Consumer) Poll(ctx context.Context) (*pb.Message, error) {
	for {
		if c.stream == nil {
			if err := c.connect(ctx); err != nil {
				return nil, err
			}
		}

		msg, err := c.stream.Recv()
		if err == nil {
			c.nextOffset = msg.Offset + 1
			return msg, nil
		}

		c.stream = nil

		// Context cancelled — return that to the caller.
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		// io.EOF is a graceful server close — reconnect and resume.
		if err == io.EOF {
			continue
		}

		// Non-retryable application-level errors (NotFound, InvalidArgument, …)
		// must surface immediately; only transient codes get retried.
		if st, ok := status.FromError(err); ok {
			switch st.Code() {
			case codes.Unavailable, codes.Internal, codes.Unknown:
				// transient — reconnect and retry
				continue
			default:
				return nil, err
			}
		}

		// Unknown error type — surface it.
		return nil, err
	}
}

// Offset returns the next offset that Poll will request.
func (c *Consumer) Offset() int64 {
	return c.nextOffset
}

// Close releases the underlying gRPC connection.
func (c *Consumer) Close() error {
	if c.stream != nil {
		c.stream = nil
	}
	return c.conn.Close()
}

func (c *Consumer) connect(ctx context.Context) error {
	stream, err := c.client.Fetch(ctx, &pb.FetchRequest{
		Topic:       c.topic,
		Partition:   c.partition,
		StartOffset: c.nextOffset,
	})
	if err != nil {
		return err
	}
	c.stream = stream
	return nil
}
