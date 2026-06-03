// Package testutil provides helpers for spinning up an in-process broker
// over a bufconn for unit and integration tests.
package testutil

import (
	"context"
	"net"
	"testing"

	"github.com/thanhlongnt/kafka-lite/internal/broker"
	pb "github.com/thanhlongnt/kafka-lite/internal/proto/kafka_lite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const BufSize = 1 << 20

// StartBroker starts an in-process broker and returns a dialer function
// suitable for grpc.WithContextDialer.
func StartBroker(t *testing.T) func(context.Context, string) (net.Conn, error) {
	t.Helper()
	lis := bufconn.Listen(BufSize)
	srv := grpc.NewServer()
	pb.RegisterBrokerServer(srv, broker.New(1))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() { srv.Stop(); lis.Close() })
	return func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}
}

// BufconnAddr is a placeholder address used with bufconn dialers.
const BufconnAddr = "passthrough://bufnet"

// NewConn returns a gRPC client connection backed by a bufconn dialer.
func NewConn(t *testing.T, dialer func(context.Context, string) (net.Conn, error)) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(BufconnAddr,
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("bufconn dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}
