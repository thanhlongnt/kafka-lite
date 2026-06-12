package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/thanhlongnt/kafka-lite/internal/consumer"
	"github.com/thanhlongnt/kafka-lite/internal/producer"
	pb "github.com/thanhlongnt/kafka-lite/internal/proto/kafka_lite"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func main() {
	brokerAddr   := flag.String("broker", "localhost:9092", "broker gRPC address")
	coordAddr    := flag.String("coordinator", "", "coordinator gRPC address (enables -route)")
	topic        := flag.String("topic", "bench", "topic name")
	partitions   := flag.Int("partitions", 4, "number of partitions")
	concurrency  := flag.Int("concurrency", runtime.GOMAXPROCS(0), "number of producer goroutines")
	targetRate   := flag.Int("rate", 0, "target messages/sec across all producers (0 = unlimited)")
	dur          := flag.Duration("duration", 10*time.Second, "measurement window")
	msgSize      := flag.Int("msg-size", 128, "message payload size in bytes")
	route        := flag.Bool("route", false, "hash-route by key via coordinator (requires -coordinator)")
	createFlag   := flag.Bool("create", true, "create topic before benchmarking")
	numConsumers := flag.Int("consumers", 0, "consumer goroutines for e2e mode (0 = same as -partitions)")
	csvPath      := flag.String("csv", "", "write per-second metrics to this CSV file")
	flag.Parse()

	if *numConsumers == 0 {
		*numConsumers = *partitions
	}
	if *route && *coordAddr == "" {
		fatalf("-route requires -coordinator")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *createFlag {
		if err := ensureTopic(ctx, *brokerAddr, *coordAddr, *topic, int32(*partitions)); err != nil {
			fatalf("create topic: %v", err)
		}
		fmt.Printf("topic %q ready (%d partitions)\n\n", *topic, *partitions)
	}

	if err := checkPartitionLayout(ctx, *brokerAddr, *coordAddr, *topic); err != nil {
		fatalf("partition layout: %v", err)
	}

	p, err := producer.New(*brokerAddr)
	if err != nil {
		fatalf("producer: %v", err)
	}
	defer p.Close()

	if *coordAddr != "" {
		p.ConnectCoordinator(*coordAddr)
	}

	// Open CSV output file if requested.
	var csvWriter *bufio.Writer
	if *csvPath != "" {
		f, err := os.Create(*csvPath)
		if err != nil {
			fatalf("open csv: %v", err)
		}
		defer f.Close()
		csvWriter = bufio.NewWriter(f)
		fmt.Fprintln(csvWriter, "elapsed_s,msgs_per_sec,mb_per_sec,errs_per_sec,consume_lag,p50_ms,p99_ms")
	}

	// Shared counters
	var producedMsgs  atomic.Int64
	var producedBytes atomic.Int64
	var consumedMsgs  atomic.Int64
	var errCount      atomic.Int64

	// Rate limiter shared across all producer goroutines.
	var limiter *rate.Limiter
	if *targetRate > 0 {
		limiter = rate.NewLimiter(rate.Limit(*targetRate), *concurrency)
	}

	// Per-goroutine cumulative latency slices (for end-of-run percentiles).
	latBufs := make([][]time.Duration, *concurrency)
	for i := range latBufs {
		latBufs[i] = make([]time.Duration, 0, 16384)
	}

	// Per-second latency buffer — drained by the reporter each tick.
	// Producers append under secLatMu; reporter swaps the slice out each second.
	var secLatMu  sync.Mutex
	var secLatBuf []time.Duration

	// Payload filled with non-zero bytes.
	payload := make([]byte, *msgSize)
	for i := range payload {
		payload[i] = byte(i%251 + 1)
	}

	// runCtx is cancelled after -duration; outer ctx handles ctrl-c.
	runCtx, cancel := context.WithTimeout(ctx, *dur)
	defer cancel()

	// partBroker maps partition index → broker address; populated from coordinator
	// metadata so each consumer goroutine connects to the broker that owns its
	// partition rather than always hitting the entry broker.
	var partBrokerMu sync.RWMutex
	partBroker := make(map[int32]string)

	// fetchPartBroker refreshes the partition→broker map from the coordinator.
	// Falls back to the entry broker proxy if no coordinator address is available.
	fetchPartBroker := func() {
		var parts []*pb.PartitionInfo
		if *coordAddr != "" {
			for _, raw := range strings.Split(*coordAddr, ",") {
				addr := strings.TrimSpace(raw)
				if addr == "" {
					continue
				}
				conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					continue
				}
				rctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				resp, err := pb.NewCoordinatorClient(conn).GetMetadata(rctx, &pb.MetadataRequest{Topic: *topic})
				cancel()
				conn.Close()
				if err != nil {
					continue
				}
				parts = resp.Partitions
				break
			}
		}
		if parts == nil {
			conn, err := grpc.NewClient(*brokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return
			}
			rctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			resp, err := pb.NewCoordinatorClient(conn).GetMetadata(rctx, &pb.MetadataRequest{Topic: *topic})
			cancel()
			conn.Close()
			if err != nil {
				return
			}
			parts = resp.Partitions
		}
		partBrokerMu.Lock()
		for _, p := range parts {
			partBroker[p.Partition] = p.BrokerAddr
		}
		partBrokerMu.Unlock()
	}

	fetchPartBroker() // initial population before goroutines start

	// Consumer goroutines — one per partition, each routed to the broker that
	// owns that partition. On error the metadata is refreshed so failovers are
	// picked up automatically.
	var consumerWg sync.WaitGroup
	for i := 0; i < *numConsumers; i++ {
		partIdx := int32(i % *partitions)
		consumerWg.Add(1)
		go func(partIdx int32) {
			defer consumerWg.Done()
			var resumeOffset int64
			for runCtx.Err() == nil {
				partBrokerMu.RLock()
				addr := partBroker[partIdx]
				partBrokerMu.RUnlock()
				if addr == "" {
					fetchPartBroker()
					time.Sleep(time.Second)
					continue
				}
				c, err := consumer.New(addr, *topic, partIdx, resumeOffset)
				if err != nil {
					time.Sleep(time.Second)
					continue
				}
				for {
					msg, err := c.Poll(runCtx)
					if runCtx.Err() != nil {
						c.Close()
						return
					}
					if err != nil {
						c.Close()
						fetchPartBroker() // refresh in case broker changed after failover
						time.Sleep(time.Second)
						break
					}
					resumeOffset = msg.Offset + 1
					consumedMsgs.Add(1)
				}
			}
		}(partIdx)
	}

	// Producer goroutines — closed loop: send → ack → send.
	var producerWg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		partIdx := int32(i % *partitions)
		workerKey := []byte(fmt.Sprintf("worker-%d", i))
		latBuf := &latBufs[i]
		msg := make([]byte, *msgSize)
		copy(msg, payload)

		producerWg.Add(1)
		go func(partIdx int32, key []byte, latBuf *[]time.Duration) {
			defer producerWg.Done()
			for runCtx.Err() == nil {
				if limiter != nil {
					if err := limiter.Wait(runCtx); err != nil {
						return
					}
				}
				start := time.Now()
				var sendErr error
				if *route {
					_, _, sendErr = p.Route(runCtx, *topic, key, msg)
				} else {
					_, sendErr = p.Send(runCtx, *topic, partIdx, nil, msg)
				}
				elapsed := time.Since(start)
				if runCtx.Err() != nil {
					return
				}
				// Record latency for every attempt — success and failure — so
				// the percentiles reflect true observed send latency, not just
				// the happy path.
				*latBuf = append(*latBuf, elapsed)
				secLatMu.Lock()
				secLatBuf = append(secLatBuf, elapsed)
				secLatMu.Unlock()
				if sendErr != nil {
					errCount.Add(1)
					time.Sleep(10 * time.Millisecond)
					continue
				}
				producedMsgs.Add(1)
				producedBytes.Add(int64(*msgSize))
			}
		}(partIdx, workerKey, latBuf)
	}

	// Reporter — prints one line per second until runCtx expires.
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		var prevMsgs, prevBytes, prevErrs int64
		var sec int
		for {
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
				sec++
				curMsgs := producedMsgs.Load()
				curBytes := producedBytes.Load()
				curErrs := errCount.Load()
				lag := curMsgs - consumedMsgs.Load()
				deltaMsgs := curMsgs - prevMsgs
				deltaBytes := curBytes - prevBytes
				deltaErrs := curErrs - prevErrs

				// Drain per-second latency buffer.
				secLatMu.Lock()
				snap := secLatBuf
				secLatBuf = nil
				secLatMu.Unlock()

				var p50, p99 float64
				if len(snap) > 0 {
					sort.Slice(snap, func(i, j int) bool { return snap[i] < snap[j] })
					p50 = ms(snap[clamp(len(snap)*50/100, 0, len(snap)-1)])
					p99 = ms(snap[clamp(len(snap)*99/100, 0, len(snap)-1)])
				}

				fmt.Printf("[%3ds] produce: %7d msg/s  %6.2f MB/s  errs/s: %4d  p50=%5.2fms p99=%6.2fms | lag: %d\n",
					sec, deltaMsgs, float64(deltaBytes)/1024/1024,
					deltaErrs, p50, p99, lag)

				if csvWriter != nil {
					fmt.Fprintf(csvWriter, "%d,%d,%.4f,%d,%d,%.4f,%.4f\n",
						sec, deltaMsgs, float64(deltaBytes)/1024/1024,
						deltaErrs, lag, p50, p99)
					csvWriter.Flush()
				}

				prevMsgs = curMsgs
				prevBytes = curBytes
				prevErrs = curErrs
			}
		}
	}()

	producerWg.Wait()
	cancel()
	consumerWg.Wait()

	// Merge per-goroutine latency slices and compute overall percentiles.
	var all []time.Duration
	for _, buf := range latBufs {
		all = append(all, buf...)
	}

	totalMsgs := producedMsgs.Load()
	totalConsumed := consumedMsgs.Load()
	totalBytes := producedBytes.Load()
	seconds := dur.Seconds()

	fmt.Printf("\n[DONE] produced=%d  errors=%d  consumed=%d  avg=%.0f msg/s  %.2f MB/s\n",
		totalMsgs, errCount.Load(), totalConsumed,
		float64(totalMsgs)/seconds,
		float64(totalBytes)/1024/1024/seconds,
	)

	if len(all) > 0 {
		sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
		p50  := all[clamp(len(all)*50/100, 0, len(all)-1)]
		p99  := all[clamp(len(all)*99/100, 0, len(all)-1)]
		p999 := all[clamp(len(all)*999/1000, 0, len(all)-1)]
		fmt.Printf("       latency p50=%.2fms  p99=%.2fms  p99.9=%.2fms\n",
			ms(p50), ms(p99), ms(p999))
	}
}

// checkPartitionLayout fetches metadata for topic, prints the partition→broker
// table, and returns an error if the distribution is uneven (any broker holds
// more than ceil(partitions/brokers) partitions).
func checkPartitionLayout(ctx context.Context, brokerAddr, coordAddrs, topic string) error {
	var parts []*pb.PartitionInfo

	if coordAddrs != "" {
		for _, addr := range strings.Split(coordAddrs, ",") {
			addr = strings.TrimSpace(addr)
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				continue
			}
			resp, err := pb.NewCoordinatorClient(conn).GetMetadata(ctx, &pb.MetadataRequest{Topic: topic})
			conn.Close()
			if err != nil {
				continue
			}
			parts = resp.Partitions
			break
		}
	}
	if parts == nil {
		// Broker exposes GetMetadata via its coordinator proxy.
		conn, err := grpc.NewClient(brokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("dial broker: %w", err)
		}
		defer conn.Close()
		resp, err := pb.NewCoordinatorClient(conn).GetMetadata(ctx, &pb.MetadataRequest{Topic: topic})
		if err != nil {
			return fmt.Errorf("GetMetadata via broker proxy: %w", err)
		}
		parts = resp.Partitions
	}
	if len(parts) == 0 {
		return fmt.Errorf("no partition info returned")
	}

	// Sort by partition index for stable output.
	sort.Slice(parts, func(i, j int) bool { return parts[i].Partition < parts[j].Partition })

	// Count partitions per broker and track max.
	brokerCount := make(map[string]int)
	for _, p := range parts {
		brokerCount[p.BrokerAddr]++
	}

	numBrokers := len(brokerCount)
	numParts := len(parts)
	ceil := (numParts + numBrokers - 1) / numBrokers

	fmt.Printf("partition layout (%d partitions across %d brokers, expected ≤%d per broker):\n", numParts, numBrokers, ceil)
	for _, p := range parts {
		fmt.Printf("  partition %2d → %s\n", p.Partition, p.BrokerAddr)
	}

	var uneven []string
	for addr, count := range brokerCount {
		if count > ceil {
			uneven = append(uneven, fmt.Sprintf("%s has %d partitions", addr, count))
		}
	}
	if len(uneven) > 0 {
		sort.Strings(uneven)
		return fmt.Errorf("uneven distribution — not all brokers registered before topic creation: %s", strings.Join(uneven, ", "))
	}
	fmt.Printf("  distribution OK\n\n")
	return nil
}

// ensureTopic creates the topic, ignoring AlreadyExists.
// coordAddrs is a comma-separated list; each is tried in turn until one
// succeeds, which handles the case where the first entry isn't the Raft leader.
func ensureTopic(ctx context.Context, brokerAddr, coordAddrs, topic string, partitions int32) error {
	req := &pb.CreateTopicRequest{Topic: topic, Partitions: partitions}

	if coordAddrs == "" {
		conn, err := grpc.NewClient(brokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		defer conn.Close()
		_, err = pb.NewBrokerClient(conn).CreateTopic(ctx, req)
		if status.Code(err) == codes.AlreadyExists {
			return nil
		}
		return err
	}

	addrs := strings.Split(coordAddrs, ",")
	var lastErr error
	for _, addr := range addrs {
		addr = strings.TrimSpace(addr)
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			lastErr = err
			continue
		}
		_, err = pb.NewCoordinatorClient(conn).CreateTopic(ctx, req)
		conn.Close()
		if err == nil || status.Code(err) == codes.AlreadyExists {
			return nil
		}
		if status.Code(err) == codes.FailedPrecondition {
			lastErr = err
			continue
		}
		return err
	}
	return fmt.Errorf("no coordinator accepted CreateTopic: %w", lastErr)
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	os.Exit(1)
}
