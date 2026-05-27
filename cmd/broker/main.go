package main

import (
	"context"
	"flag"
	"log"

	"github.com/thanhlongnt/kafka-lite/internal/broker"
	pb "github.com/thanhlongnt/kafka-lite/internal/proto/kafka_lite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", ":9092", "gRPC listen address")
	advertise := flag.String("advertise", "", "address to advertise to coordinator (defaults to localhost+port of -addr)")
	coordAddr := flag.String("coordinator", "", "coordinator gRPC address (phase 2, optional)")
	flag.Parse()

	b := broker.New()
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
		_, err = pb.NewCoordinatorClient(conn).RegisterBroker(context.Background(), &pb.RegisterBrokerRequest{Addr: registerAddr})
		conn.Close()
		if err != nil {
			log.Fatalf("register with coordinator: %v", err)
		}
		log.Printf("registered with coordinator %s as %s", *coordAddr, registerAddr)
	}

	log.Printf("kafka-lite broker listening on %s", *addr)
	if err := b.Serve(*addr); err != nil {
		log.Fatalf("broker: %v", err)
	}
}
