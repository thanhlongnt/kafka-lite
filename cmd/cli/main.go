package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/thanhlongnt/kafka-lite/internal/consumer"
	"github.com/thanhlongnt/kafka-lite/internal/producer"
	pb "github.com/thanhlongnt/kafka-lite/internal/proto/kafka_lite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const usage = `kafka-lite CLI — interactive testing tool

Usage:
  cli <command> [flags]

Commands:
  create-topic   Create a topic on the broker
  produce        Send a single message to a topic/partition
  consume        Stream messages from a topic/partition

Run 'cli <command> -help' for flag details.
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(1)
	}

	switch os.Args[1] {
	case "create-topic":
		runCreateTopic(os.Args[2:])
	case "produce":
		runProduce(os.Args[2:])
	case "consume":
		runConsume(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n%s", os.Args[1], usage)
		os.Exit(1)
	}
}

// ── create-topic ──────────────────────────────────────────────────────────────

func runCreateTopic(args []string) {
	fs := flag.NewFlagSet("create-topic", flag.ExitOnError)
	addr := fs.String("broker", "localhost:9092", "broker gRPC address")
	topic := fs.String("topic", "", "topic name (required)")
	partitions := fs.Int("partitions", 1, "number of partitions")
	fs.Parse(args)

	if *topic == "" {
		fmt.Fprintln(os.Stderr, "error: -topic is required")
		fs.Usage()
		os.Exit(1)
	}

	client, conn := brokerClient(*addr)
	defer conn.Close()

	_, err := client.CreateTopic(context.Background(), &pb.CreateTopicRequest{
		Topic:      *topic,
		Partitions: int32(*partitions),
	})
	if err != nil {
		fatalf("create-topic: %v", err)
	}
	fmt.Printf("created topic %q with %d partition(s)\n", *topic, *partitions)
}

// ── produce ───────────────────────────────────────────────────────────────────

func runProduce(args []string) {
	fs := flag.NewFlagSet("produce", flag.ExitOnError)
	addr := fs.String("broker", "localhost:9092", "broker gRPC address")
	topic := fs.String("topic", "", "topic name (required)")
	partition := fs.Int("partition", 0, "partition index")
	key := fs.String("key", "", "message key (optional)")
	value := fs.String("value", "", "message value (required)")
	fs.Parse(args)

	if *topic == "" || *value == "" {
		fmt.Fprintln(os.Stderr, "error: -topic and -value are required")
		fs.Usage()
		os.Exit(1)
	}

	p, err := producer.New(*addr)
	if err != nil {
		fatalf("producer: %v", err)
	}
	defer p.Close()

	offset, err := p.Send(context.Background(), *topic, int32(*partition), []byte(*key), []byte(*value))
	if err != nil {
		fatalf("produce: %v", err)
	}
	fmt.Printf("produced to %s/%d at offset %d\n", *topic, *partition, offset)
}

// ── consume ───────────────────────────────────────────────────────────────────

func runConsume(args []string) {
	fs := flag.NewFlagSet("consume", flag.ExitOnError)
	addr := fs.String("broker", "localhost:9092", "broker gRPC address")
	topic := fs.String("topic", "", "topic name (required)")
	partition := fs.Int("partition", 0, "partition index")
	offset := fs.Int64("offset", 0, "start offset")
	fs.Parse(args)

	if *topic == "" {
		fmt.Fprintln(os.Stderr, "error: -topic is required")
		fs.Usage()
		os.Exit(1)
	}

	c, err := consumer.New(*addr, *topic, int32(*partition), *offset)
	if err != nil {
		fatalf("consumer: %v", err)
	}
	defer c.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Printf("consuming %s/%d from offset %d (ctrl-c to stop)\n\n", *topic, *partition, *offset)
	for {
		msg, err := c.Poll(ctx)
		if err != nil {
			if ctx.Err() != nil {
				fmt.Println("\nstopped.")
				return
			}
			fatalf("consume: %v", err)
		}
		key := string(msg.Key)
		if key == "" {
			key = "<no key>"
		}
		fmt.Printf("offset=%-6d key=%-16s value=%s\n", msg.Offset, key, msg.Value)
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func brokerClient(addr string) (pb.BrokerClient, *grpc.ClientConn) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fatalf("dial %s: %v", addr, err)
	}
	return pb.NewBrokerClient(conn), conn
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	os.Exit(1)
}
