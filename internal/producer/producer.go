package producer

import (
	"context"
	"fmt"
	pb "github.com/thanhlongnt/kafka-lite/internal/proto/kafka_lite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"hash/fnv"
	"strings"
	"sync"
)

// Producer sends messages to a single broker. Phase 1 — no partition routing;
// the caller specifies the target partition explicitly.
type Producer struct {
	client    pb.BrokerClient
	conn      *grpc.ClientConn
	coordConn *grpc.ClientConn // non-nil when connected directly to a coordinator

	// Phase 2: Coordinator client and cached metadata for partition routing.
	coordClient   pb.CoordinatorClient
	metaCache     map[string][]*pb.PartitionInfo // topic -> [{partition, broker_addr}, ...]
	brokerClients map[string]pb.BrokerClient     // broker_addr -> client
	mu            sync.RWMutex                   // protects metaCache and brokerClients

	dialBroker func(ctx context.Context, addr string) (pb.BrokerClient, error) // for testing
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

// ConnectCoordinator enables partition routing. With no arguments it proxies
// coordinator RPCs through the entry broker (useful for in-process tests).
// With a comma-separated list of coordinator addresses it connects directly to
// the first reachable coordinator, which is required when the entry broker may
// itself be killed during a degradation test.
func (p *Producer) ConnectCoordinator(coordAddrs ...string) {
	p.metaCache = make(map[string][]*pb.PartitionInfo)
	p.brokerClients = make(map[string]pb.BrokerClient)
	p.dialBroker = func(_ context.Context, addr string) (pb.BrokerClient, error) {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		return pb.NewBrokerClient(conn), nil
	}

	if len(coordAddrs) == 0 {
		// Proxy through the entry broker (backward-compatible path).
		p.coordClient = pb.NewCoordinatorClient(p.conn)
		return
	}

	// Connect directly to one of the provided coordinator addresses.
	for _, raw := range coordAddrs {
		for _, addr := range strings.Split(raw, ",") {
			addr = strings.TrimSpace(addr)
			if addr == "" {
				continue
			}
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				continue
			}
			p.coordConn = conn
			p.coordClient = pb.NewCoordinatorClient(conn)
			return
		}
	}
	// Fallback to broker proxy if no address was dialable.
	p.coordClient = pb.NewCoordinatorClient(p.conn)
}

// SetDialer used to open connections to partition owners
func (p *Producer) SetDialer(fn func(ctx context.Context, addr string) (pb.BrokerClient, error)) {
	p.dialBroker = fn
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

// Route selects partition by hashing key, routes to broker that owns it, and returns chosen partition and offset.
func (p *Producer) Route(ctx context.Context, topic string, key, value []byte) (int32, int64, error) {
	parts, err := p.metadata(ctx, topic)
	if err != nil {
		return 0, 0, err
	}
	partition := partitionForKey(key, len(parts))
	client, err := p.brokerClient(ctx, parts[partition].BrokerAddr)
	if err != nil {
		return 0, 0, err
	}
	resp, err := client.Produce(ctx, &pb.ProduceRequest{
		Topic:     topic,
		Partition: partition,
		Key:       key,
		Value:     value,
	})
	if err != nil {
		code := status.Code(err)
		if code == codes.FailedPrecondition || code == codes.Unavailable {
			p.mu.Lock()
			delete(p.metaCache, topic)
			if code == codes.Unavailable {
				// Evict the dead connection so the next call dials the new owner.
				delete(p.brokerClients, parts[partition].BrokerAddr)
			}
			p.mu.Unlock()
		}
		return 0, 0, err
	}
	return partition, resp.Offset, nil
}

// Close releases the underlying gRPC connection(s).
func (p *Producer) Close() error {
	if p.coordConn != nil {
		p.coordConn.Close()
	}
	return p.conn.Close()
}

// metadata fetches partition metadata for a topic, using cached data if available.
func (p *Producer) metadata(ctx context.Context, topic string) ([]*pb.PartitionInfo, error) {
	p.mu.RLock()
	parts, ok := p.metaCache[topic]
	p.mu.RUnlock()
	if ok {
		return parts, nil
	}

	resp, err := p.coordClient.GetMetadata(ctx, &pb.MetadataRequest{Topic: topic})
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	p.metaCache[topic] = resp.Partitions
	p.mu.Unlock()
	return resp.Partitions, nil
}

// brokerClient returns a client for the given broker address, opening a new connection if necessary.
func (p *Producer) brokerClient(ctx context.Context, addr string) (pb.BrokerClient, error) {
	p.mu.RLock()
	client, ok := p.brokerClients[addr]
	p.mu.RUnlock()
	if ok {
		return client, nil
	}
	client, err := p.dialBroker(ctx, addr)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	p.brokerClients[addr] = client
	p.mu.Unlock()
	return client, nil
}

// partitionForKey hashes the key and maps it to a partition index.
func partitionForKey(key []byte, numPartitions int) int32 {
	if len(key) == 0 || numPartitions <= 1 {
		return 0
	}
	hash := fnv.New32()
	hash.Write(key)
	return int32(hash.Sum32() % uint32(numPartitions))
}
