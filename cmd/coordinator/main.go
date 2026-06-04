package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/hashicorp/raft"
	"github.com/thanhlongnt/kafka-lite/internal/coordinator"
	kafkaraft "github.com/thanhlongnt/kafka-lite/internal/raft"
)

func main() {
	addr := flag.String("addr", ":9093", "coordinator gRPC address")
	raftID := flag.String("raft-id", "node1", "raft node ID")
	raftBind := flag.String("raft-bind", "127.0.0.1:7000", "raft TCP bind address")
	dataDir := flag.String("data-dir", "./raft-data", "raft data directory")
	peers := flag.String("peers", "", "comma-separated id=addr pairs, e.g. node2=127.0.0.1:7001,node3=127.0.0.1:7002")
	rpcLog := flag.Bool("rpc-log", false, "enable rpc logging to file")
	flag.Parse()

	var rpcLogger *log.Logger
	if *rpcLog {
		f, err := os.OpenFile(fmt.Sprintf("rpc-coordinator-%s.log", *raftID), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("open rpc log file: %v", err)
		}
		rpcLogger = log.New(f, "", log.Ltime|log.Lmicroseconds)
	}

	node, err := kafkaraft.NewNode(*raftID, *raftBind, *dataDir, rpcLogger)
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
	if err := c.Serve(*addr, rpcLogger); err != nil {
		log.Fatalf("coordinator: %v", err)
	}
}
