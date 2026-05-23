package broker

import (
	"context"
	"fmt"
	"net"
	"sync"

	pb "github.com/thanhlongnt/kafka-lite/internal/proto/kafka_lite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Broker manages topics and their partitions and implements the BrokerServer gRPC interface.
type Broker struct {
	pb.UnimplementedBrokerServer
	mu     sync.RWMutex
	topics map[string][]*Partition
}

func New() *Broker {
	return &Broker{
		topics: make(map[string][]*Partition),
	}
}

// CreateTopic creates a topic with the given number of partitions.
// Returns AlreadyExists if the topic already exists.
func (b *Broker) CreateTopic(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	if req.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "topic name must not be empty")
	}
	if req.Partitions <= 0 {
		return nil, status.Error(codes.InvalidArgument, "partition count must be > 0")
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.topics[req.Topic]; exists {
		return nil, status.Errorf(codes.AlreadyExists, "topic %q already exists", req.Topic)
	}

	parts := make([]*Partition, req.Partitions)
	for i := range parts {
		parts[i] = newPartition()
	}
	b.topics[req.Topic] = parts
	return &pb.CreateTopicResponse{}, nil
}

// Produce appends a message to the specified topic/partition and returns the assigned offset.
func (b *Broker) Produce(_ context.Context, req *pb.ProduceRequest) (*pb.ProduceResponse, error) {
	p, err := b.getPartition(req.Topic, req.Partition)
	if err != nil {
		return nil, err
	}

	msg := &pb.Message{
		Topic:     req.Topic,
		Partition: req.Partition,
		Key:       req.Key,
		Value:     req.Value,
	}
	offset, err := p.Append(msg)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "append failed: %v", err)
	}
	return &pb.ProduceResponse{Offset: offset}, nil
}

// Fetch streams messages from the specified topic/partition starting at start_offset.
// It sends existing messages immediately, then blocks and pushes new messages as they arrive.
// The stream continues until the client cancels.
func (b *Broker) Fetch(req *pb.FetchRequest, stream pb.Broker_FetchServer) error {
	p, err := b.getPartition(req.Topic, req.Partition)
	if err != nil {
		return err
	}

	ctx := stream.Context()
	offset := req.StartOffset
	const batchSize = 64

	for {
		msgs, err := p.Read(offset, batchSize)
		if err != nil {
			return status.Errorf(codes.Internal, "read failed: %v", err)
		}

		for _, msg := range msgs {
			if err := stream.Send(msg); err != nil {
				return err
			}
			offset++
		}

		if len(msgs) > 0 {
			// More might be available — loop immediately before waiting.
			continue
		}

		// No message at offset yet — block until one arrives or client cancels.
		// WaitForData waits until Len() > offset (i.e. message at offset exists).
		p.WaitForData(ctx, offset)
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

// Serve starts the gRPC server on addr and blocks until it exits.
func (b *Broker) Serve(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	srv := grpc.NewServer()
	pb.RegisterBrokerServer(srv, b)
	return srv.Serve(lis)
}

// getPartition looks up a partition by topic and partition index.
func (b *Broker) getPartition(topic string, partition int32) (*Partition, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	parts, ok := b.topics[topic]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "topic %q not found", topic)
	}
	if int(partition) < 0 || int(partition) >= len(parts) {
		return nil, status.Errorf(codes.NotFound, "partition %d not found in topic %q", partition, topic)
	}
	return parts[partition], nil
}
