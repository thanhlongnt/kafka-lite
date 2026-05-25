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
// Phase 2: call joinGroup to join consumer group, poll round-robin across all partitions
type Consumer struct {
	client     pb.BrokerClient
	conn       *grpc.ClientConn
	topic      string
	partition  int32
	nextOffset int64

	stream pb.Broker_FetchClient

	// Phase 2
	coordClient pb.CoordinatorClient
	groupID    string
	memberID   string
	readers   []*partitionReader
	nextIdx int
	dialBroker func(ctx context.Context, brokerAddr string) (pb.BrokerClient, error)
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

// JoinGroup joins consumer group via coordinator and opens conn to each assigned partition. Phase 2.
func (c *Consumer) JoinGroup(ctx context.Context, groupID string, memberID string, topic string) error {
	c.coordClient = pb.NewCoordinatorClient(c.conn)
	c.groupID = groupID
	c.memberID = memberID
	c.topic = topic

	if c.dialBroker == nil {
		c.dialBroker = func(_ context.Context, brokerAddr string) (pb.BrokerClient, error) {
			conn, err := grpc.NewClient(brokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return nil, fmt.Errorf("dial %s: %w", brokerAddr, err)
			}
			return pb.NewBrokerClient(conn), nil
		}
	}
	join, err := c.coordClient.JoinGroup(ctx, &pb.JoinGroupRequest{
		GroupId: groupID,
		MemberId: memberID,
		Topic: topic,
	})
	if err != nil {
		return fmt.Errorf("join group: %w", err)
	}

	meta, err := c.coordClient.GetMetadata(ctx, &pb.MetadataRequest{Topic: topic})
	if err != nil {
		return fmt.Errorf("get metadata: %w", err)
	}
	partitionBroker := make(map[int32]string, len(meta.Partitions))
	for _, pi := range meta.Partitions {
		partitionBroker[pi.Partition] = pi.BrokerAddr
	}
	c.readers = make([]*partitionReader, 0, len(join.AssignedPartitions))
	for _, p := range join.AssignedPartitions {
		addr, ok := partitionBroker[p]
		if !ok {
			return fmt.Errorf("metadata missing partition %d", p)
		}
		brokerClient, err := c.dialBroker(ctx, addr)
		if err != nil {
			return fmt.Errorf("dial broker for partition %d: %w", p, err)
		}
		c.readers = append(c.readers, &partitionReader{
			client: brokerClient,
			topic: topic,
			partition: p,
		})
	}
	return nil
}
// SetDialer allows overriding the default gRPC dialer, used by JoinGroup to connect to assigned partition brokers. Only for testing.
func (c *Consumer) SetDialer(fn func(ctx context.Context, brokerAddr string) (pb.BrokerClient, error)) {
	c.dialBroker = fn
}


// Poll blocks until the next message is available or ctx is cancelled.
// On stream EOF or transport error it reconnects from the last received offset.
func (c *Consumer) Poll(ctx context.Context) (*pb.Message, error) {
	if len(c.readers) > 0 {
		pr := c.readers[c.nextIdx%len(c.readers)]
		c.nextIdx++
		return pr.poll(ctx)
	}
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

// CommitOffsets commits the current offset to the broker. Phase 2: commit all assigned partitions.
func (c *Consumer) CommitOffsets(ctx context.Context) error {
	offsets := make([]*pb.PartitionOffset, len(c.readers))
	for i, pr := range c.readers {
		offsets[i] = &pb.PartitionOffset{
			Partition: pr.partition,
			Offset: pr.nextOffset,
		}
	}
	_, err := c.coordClient.CommitOffsets(ctx, &pb.CommitOffsetsRequest{
		GroupId: c.groupID,
		Topic: c.topic,
		Offsets: offsets,
	})
	return err
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

// partitionReader manages a Fetch stream for a single partition, used in phase 2 when consumer joins a group and is assigned multiple partitions.
type partitionReader struct {
	client pb.BrokerClient
	topic string
	partition int32
	nextOffset int64

	stream pb.Broker_FetchClient
}

func (pr *partitionReader) poll(ctx context.Context) (*pb.Message, error) {
	for {
		if pr.stream == nil {
			stream, err := pr.client.Fetch(ctx, &pb.FetchRequest{
				Topic: pr.topic,
				Partition: pr.partition,
				StartOffset: pr.nextOffset,
			})
			if err != nil {
				return nil, err
			}
			pr.stream = stream
		}

		msg, err := pr.stream.Recv()
		if err == nil {
			pr.nextOffset = msg.Offset + 1
			return msg, nil
		}

		pr.stream = nil

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if err == io.EOF {
			continue
		}

		if st, ok := status.FromError(err); ok {
			switch st.Code() {
			case codes.Unavailable, codes.Internal, codes.Unknown:
				continue
			default:
				return nil, err
			}
		}
		return nil, err
	}
}