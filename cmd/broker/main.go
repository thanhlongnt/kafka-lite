package main

import (
	"flag"
	"log"

	"github.com/thanhlongnt/kafka-lite/internal/broker"
)

func main() {
	addr := flag.String("addr", ":9092", "gRPC listen address")
	dataDir := flag.String("data-dir", "", "directory for persistent log segments (omit for in-memory)")
	flag.Parse()

	var b *broker.Broker
	if *dataDir != "" {
		var err error
		b, err = broker.NewWithDataDir(*dataDir)
		if err != nil {
			log.Fatalf("broker: %v", err)
		}
		log.Printf("kafka-lite broker listening on %s (data-dir=%s)", *addr, *dataDir)
	} else {
		b = broker.New()
		log.Printf("kafka-lite broker listening on %s (in-memory)", *addr)
	}

	if err := b.Serve(*addr); err != nil {
		log.Fatalf("broker: %v", err)
	}
}
