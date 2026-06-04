package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/thanhlongnt/kafka-lite/internal/broker"
	pb "github.com/thanhlongnt/kafka-lite/internal/proto/kafka_lite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", ":9092", "gRPC listen address")
	dataDir := flag.String("data-dir", "", "directory for persistent log segments (omit for in-memory)")
	advertise := flag.String("advertise", "", "address to advertise to coordinator (defaults to localhost+port of -addr)")
	coordAddr := flag.String("coordinator", "", "coordinator gRPC address (phase 2, optional)")
	id := flag.Int("id", 1, "integer ID of this broker")
	rpcLog := flag.Bool("rpc-log", false, "enable rpc logging to file")
	flag.Parse()

	var rpcLogger *log.Logger
	if *rpcLog {
		f, err := os.OpenFile(fmt.Sprintf("rpc-broker-%d.log", *id), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("open rpc log file: %v", err)
		}
		rpcLogger = log.New(f, "", log.Ltime|log.Lmicroseconds)
	}

	var b *broker.Broker
	if *dataDir != "" {
		var err error
		b, err = broker.NewWithDataDir(int32(*id), *dataDir)
		if err != nil {
			log.Fatalf("broker: %v", err)
		}
	} else {
		b = broker.New(int32(*id))
	}

	if *coordAddr != "" {
		if err := b.ConnectCoordinator(context.Background(), *coordAddr); err != nil {
			log.Fatalf("connect coordinator: %v", err)
		}

		registerAddr := *advertise
		if registerAddr == "" {
			registerAddr = "localhost" + *addr
		}

		conn, err := grpc.NewClient(*coordAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("dial coordinator for registration: %v", err)
		}
		_, err = pb.NewCoordinatorClient(conn).RegisterBroker(context.Background(), &pb.RegisterBrokerRequest{Addr: registerAddr, Id: int32(*id)})
		conn.Close()
		if err != nil {
			log.Fatalf("register with coordinator: %v", err)
		}
		log.Printf("registered with coordinator %s as %s", *coordAddr, registerAddr)
	}

	if *dataDir != "" {
		log.Printf("kafka-lite broker listening on %s (data-dir=%s)", *addr, *dataDir)
	} else {
		log.Printf("kafka-lite broker listening on %s (in-memory)", *addr)
	}

	if err := b.Serve(*addr, rpcLogger); err != nil {
		log.Fatalf("broker: %v", err)
	}
}
