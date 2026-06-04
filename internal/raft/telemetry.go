package raft

import (
	"log"
	"time"

	"github.com/hashicorp/raft"
)

// ServerLoggingTransport wraps a raft.Transport to log incoming RPCs.
type ServerLoggingTransport struct {
	raft.Transport
	logger      *log.Logger
	consumeChan chan raft.RPC
}

// NewServerLoggingTransport creates a new wrapper that logs incoming Raft RPCs.
func NewServerLoggingTransport(base raft.Transport, logger *log.Logger) *ServerLoggingTransport {
	if logger == nil {
		return &ServerLoggingTransport{Transport: base}
	}

	t := &ServerLoggingTransport{
		Transport:   base,
		logger:      logger,
		consumeChan: make(chan raft.RPC),
	}

	go t.interceptConsumer()

	return t
}

func (t *ServerLoggingTransport) interceptConsumer() {
	baseConsumer := t.Transport.Consumer()
	for rpc := range baseConsumer {
		// Wrap the RespChan
		start := time.Now()
		cmdType := getRaftCommandType(rpc.Command)

		originalRespChan := rpc.RespChan
		hookedRespChan := make(chan raft.RPCResponse, 1)

		rpc.RespChan = hookedRespChan

		t.consumeChan <- rpc

		// Wait for response and forward
		go func(t *ServerLoggingTransport, cType string, start time.Time, orig chan<- raft.RPCResponse, hooked <-chan raft.RPCResponse) {
			resp := <-hooked
			latency := time.Since(start)
			t.logger.Printf("[RAFT IN] cmd=%s latency=%s err=%v", cType, latency, resp.Error)
			orig <- resp
		}(t, cmdType, start, originalRespChan, hookedRespChan)
	}
}

func (t *ServerLoggingTransport) Consumer() <-chan raft.RPC {
	if t.logger == nil {
		return t.Transport.Consumer()
	}
	return t.consumeChan
}

func getRaftCommandType(cmd interface{}) string {
	switch cmd.(type) {
	case *raft.AppendEntriesRequest:
		return "AppendEntries"
	case *raft.RequestVoteRequest:
		return "RequestVote"
	case *raft.InstallSnapshotRequest:
		return "InstallSnapshot"
	case *raft.TimeoutNowRequest:
		return "TimeoutNow"
	default:
		return "Unknown"
	}
}
