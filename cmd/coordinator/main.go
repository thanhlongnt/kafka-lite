package main

import (
	"flag"
	"log"

	"github.com/hashicorp/raft"
	"github.com/thanhlongnt/kafka-lite/internal/coordinator"
	kafkaraft "github.com/thanhlongnt/kafka-lite/internal/raft"
)

func main() {
	addr     := flag.String("addr",      ":9093",          "coordinator gRPC address")
	raftID   := flag.String("raft-id",   "node1",          "raft node ID")
	raftBind := flag.String("raft-bind", "127.0.0.1:7000", "raft TCP bind address")
	dataDir  := flag.String("data-dir",  "./raft-data",    "raft data directory")
	flag.Parse()

	node, err := kafkaraft.NewNode(*raftID, *raftBind, *dataDir)
	if err != nil {
		log.Fatalf("raft node: %v", err)
	}

	// Bootstrap single-node cluster; no-op if state already exists.
	_ = node.Bootstrap([]raft.Server{{
		ID:      raft.ServerID(*raftID),
		Address: raft.ServerAddress(*raftBind),
	}})

	c := coordinator.NewWithRaft(node)
	log.Printf("starting raft coordinator on %s (raft: %s)", *addr, *raftBind)
	if err := c.Serve(*addr); err != nil {
		log.Fatalf("coordinator: %v", err)
	}
}