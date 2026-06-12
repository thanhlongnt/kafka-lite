package telemetry

import (
	"context"
	"log"
	"time"

	pb "github.com/thanhlongnt/kafka-lite/internal/proto/kafka_lite"
	"google.golang.org/grpc"
)

// LoggingUnaryInterceptor returns a unary interceptor that logs to the provided logger.
func LoggingUnaryInterceptor(logger *log.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if logger == nil {
			return handler(ctx, req)
		}

		start := time.Now()
		resp, err := handler(ctx, req)
		latency := time.Since(start)

		topic, partition := extractReqInfo(req)

		if topic != "" {
			logger.Printf("[gRPC Unary] method=%s topic=%s partition=%d latency=%s err=%v", info.FullMethod, topic, partition, latency, err)
		} else {
			logger.Printf("[gRPC Unary] method=%s latency=%s err=%v", info.FullMethod, latency, err)
		}

		return resp, err
	}
}

// LoggingStreamInterceptor returns a stream interceptor that logs to the provided logger.
func LoggingStreamInterceptor(logger *log.Logger) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if logger == nil {
			return handler(srv, ss)
		}

		start := time.Now()
		err := handler(srv, ss)
		latency := time.Since(start)

		logger.Printf("[gRPC Stream] method=%s duration=%s err=%v", info.FullMethod, latency, err)

		return err
	}
}

func extractReqInfo(req interface{}) (string, int32) {
	switch r := req.(type) {
	case *pb.ProduceRequest:
		return r.Topic, r.Partition
	case *pb.FetchRequest:
		return r.Topic, r.Partition
	case *pb.FetchReplicaRequest:
		return r.Topic, r.Partition
	case *pb.AssignRoleRequest:
		return r.Topic, r.Partition
	case *pb.CreateTopicRequest:
		return r.Topic, 0
	case *pb.UpdatePartitionLeaderRequest:
		return r.Topic, r.Partition
	case *pb.AlterIsrRequest:
		return r.Topic, r.Partition
	}
	return "", 0
}
