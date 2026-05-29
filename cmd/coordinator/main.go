package main

import (
	"flag"
	"github.com/thanhlongnt/kafka-lite/internal/coordinator"
	"log"
)

func main() {
	addr := flag.String("addr", "localhost:9093", "coordinator gRPC address")
	flag.Parse()

	c := coordinator.New()

	log.Printf("starting coordinator on %s", *addr)

	if err := c.Serve(*addr); err != nil {
		log.Fatalf("coordinator Serve: %v", err)
	}
}
