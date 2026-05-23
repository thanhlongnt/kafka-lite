package main

import (
	"flag"
	"log"

	"github.com/thanhlongnt/kafka-lite/internal/broker"
)

func main() {
	addr := flag.String("addr", ":9092", "gRPC listen address")
	flag.Parse()

	b := broker.New()
	log.Printf("kafka-lite broker listening on %s", *addr)
	if err := b.Serve(*addr); err != nil {
		log.Fatalf("broker: %v", err)
	}
}
