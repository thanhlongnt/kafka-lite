package main

import (
	"flag"
	"github.com/thanhlongnt/kafka-lite/internal/coordinator"
	"log"
	"strings"
	"github.com/hashicorp/raft"
	kafkaraft "github.com/thanhlongnt/kafka-lite/internal/raft"
)

func main() {
	addr     := flag.String("addr",      ":9093",          "coordinator gRPC address")
	raftID   := flag.String("raft-id",   "node1",          "raft node ID")
	raftBind := flag.String("raft-bind", "127.0.0.1:7000", "raft TCP bind address")
	dataDir  := flag.String("data-dir",  "./raft-data",    "raft data directory")
	peers    := flag.String("peers", "", "comma-separated id=addr pairs, e.g. node2=127.0.0.1:7001,node3=127.0.0.1:7002")
	flag.Parse()

	node, err := kafkaraft.NewNode(*raftID, *raftBind, *dataDir)
	if err != nil {
		log.Fatalf("raft node: %v", err)
	}

	servers := []raft.Server{{
		ID:      raft.ServerID(*raftID),
		Address: raft.ServerAddress(*raftBind),
	}}
	if *peers != "" {
		for _, peer := range strings.Split(*peers, ",") {
			parts := strings.SplitN(peer, "=", 2)
			if len(parts) != 2 {
				log.Fatalf("invalid peer format: %s", peer)
			}
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(parts[0]),
				Address: raft.ServerAddress(parts[1]),
			})
		}
	}
	// no-op if node already has raft state
	_ = node.Bootstrap(servers)

	c := coordinator.NewWithRaft(node)
	log.Printf("starting raft coordinator on %s (raft: %s)", *addr, *raftBind)
	if err := c.Serve(*addr); err != nil {
		log.Fatalf("coordinator: %v", err)
	}
}
