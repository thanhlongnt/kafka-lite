package main

import (
	"context"
	"flag"
	"log"

	"github.com/thanhlongnt/kafka-lite/internal/broker"
)

func main() {
	addr := flag.String("addr", ":9092", "gRPC listen address")
	coordAddr := flag.String("coordinator", "", "coordinator gRPC address (phase 2, optional)")
	flag.Parse()

	b := broker.New()
	if *coordAddr != "" {
		if err := b.ConnectCoordinator(context.Background(), *coordAddr); err != nil {
			log.Fatalf("connect coordinator: %v", err)
		}
		coordConn, _ := grpc.NewClient(*coordAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	log.Printf("kafka-lite broker listening on %s", *addr)
	if err := b.Serve(*addr); err != nil {
		log.Fatalf("broker: %v", err)
	}
}
