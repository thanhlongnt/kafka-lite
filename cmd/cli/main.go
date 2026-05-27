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
  create-topic     Create a topic (broker or coordinator)
  register-broker  Register a broker with the coordinator
  produce          Send a single message to a topic/partition
  consume          Stream messages from a topic/partition

Phase 2 flags:
  create-topic  -coordinator <addr>   routes through coordinator (multi-broker)
  produce       -route                hash-routes by key via coordinator proxy
  consume       -group <id>           join consumer group (round-robin partitions)

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
	case "register-broker":
		runRegisterBroker(os.Args[2:])
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
	coordAddr := fs.String("coordinator", "", "coordinator gRPC address (phase 2; routes through coordinator instead of broker)")
	topic := fs.String("topic", "", "topic name (required)")
	partitions := fs.Int("partitions", 1, "number of partitions")
	fs.Parse(args)

	if *topic == "" {
		fmt.Fprintln(os.Stderr, "error: -topic is required")
		fs.Usage()
		os.Exit(1)
	}

	ctx := context.Background()

	if *coordAddr != "" {
		client, conn := coordinatorClient(*coordAddr)
		defer conn.Close()
		_, err := client.CreateTopic(ctx, &pb.CreateTopicRequest{
			Topic:      *topic,
			Partitions: int32(*partitions),
		})
		if err != nil {
			fatalf("create-topic (coordinator): %v", err)
		}
	} else {
		client, conn := brokerClient(*addr)
		defer conn.Close()
		_, err := client.CreateTopic(ctx, &pb.CreateTopicRequest{
			Topic:      *topic,
			Partitions: int32(*partitions),
		})
		if err != nil {
			fatalf("create-topic: %v", err)
		}
	}
	fmt.Printf("created topic %q with %d partition(s)\n", *topic, *partitions)
}

// ── register-broker ───────────────────────────────────────────────────────────

func runRegisterBroker(args []string) {
	fs := flag.NewFlagSet("register-broker", flag.ExitOnError)
	coordAddr := fs.String("coordinator", "localhost:9093", "coordinator gRPC address (required)")
	brokerAddr := fs.String("broker", "", "broker gRPC address to register (required)")
	fs.Parse(args)

	if *coordAddr == "" || *brokerAddr == "" {
		fmt.Fprintln(os.Stderr, "error: -coordinator and -broker are required")
		fs.Usage()
		os.Exit(1)
	}

	client, conn := coordinatorClient(*coordAddr)
	defer conn.Close()

	_, err := client.RegisterBroker(context.Background(), &pb.RegisterBrokerRequest{Addr: *brokerAddr})
	if err != nil {
		fatalf("register-broker: %v", err)
	}
	fmt.Printf("registered broker %q with coordinator %q\n", *brokerAddr, *coordAddr)
}

// ── produce ───────────────────────────────────────────────────────────────────

func runProduce(args []string) {
	fs := flag.NewFlagSet("produce", flag.ExitOnError)
	addr := fs.String("broker", "localhost:9092", "broker gRPC address")
	topic := fs.String("topic", "", "topic name (required)")
	partition := fs.Int("partition", 0, "partition index (ignored when -route is set)")
	key := fs.String("key", "", "message key (optional; used for routing when -route is set)")
	value := fs.String("value", "", "message value (required)")
	route := fs.Bool("route", false, "phase 2: hash-route by key via coordinator proxy on broker")
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

	ctx := context.Background()

	if *route {
		p.ConnectCoordinator()
		part, offset, err := p.Route(ctx, *topic, []byte(*key), []byte(*value))
		if err != nil {
			fatalf("produce (route): %v", err)
		}
		fmt.Printf("produced to %s/%d at offset %d\n", *topic, part, offset)
	} else {
		offset, err := p.Send(ctx, *topic, int32(*partition), []byte(*key), []byte(*value))
		if err != nil {
			fatalf("produce: %v", err)
		}
		fmt.Printf("produced to %s/%d at offset %d\n", *topic, *partition, offset)
	}
}

// ── consume ───────────────────────────────────────────────────────────────────

func runConsume(args []string) {
	fs := flag.NewFlagSet("consume", flag.ExitOnError)
	addr := fs.String("broker", "localhost:9092", "broker gRPC address")
	topic := fs.String("topic", "", "topic name (required)")
	partition := fs.Int("partition", 0, "partition index (ignored when -group is set)")
	offset := fs.Int64("offset", 0, "start offset (single-partition mode only)")
	group := fs.String("group", "", "phase 2: consumer group ID; enables group mode with round-robin across all partitions")
	member := fs.String("member", "", "phase 2: consumer member ID (required when -group is set)")
	commitEvery := fs.Int("commit-every", 0, "phase 2: commit offsets every N messages (0 = no auto-commit)")
	fs.Parse(args)

	if *topic == "" {
		fmt.Fprintln(os.Stderr, "error: -topic is required")
		fs.Usage()
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *group != "" {
		if *member == "" {
			fmt.Fprintln(os.Stderr, "error: -member is required when -group is set")
			fs.Usage()
			os.Exit(1)
		}
		runConsumeGroup(ctx, *addr, *topic, *group, *member, *commitEvery)
		return
	}

	c, err := consumer.New(*addr, *topic, int32(*partition), *offset)
	if err != nil {
		fatalf("consumer: %v", err)
	}
	defer c.Close()

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
		printMsg(msg)
	}
}

func runConsumeGroup(ctx context.Context, brokerAddr, topic, groupID, memberID string, commitEvery int) {
	// Consumer uses brokerAddr as coordinator proxy (broker must have -coordinator set).
	c, err := consumer.New(brokerAddr, topic, 0, 0)
	if err != nil {
		fatalf("consumer: %v", err)
	}
	defer c.Close()

	if err := c.JoinGroup(ctx, groupID, memberID, topic); err != nil {
		fatalf("join group: %v", err)
	}
	fmt.Printf("joined group %q as %q, consuming %s (ctrl-c to stop)\n\n", groupID, memberID, topic)

	var count int
	for {
		msg, err := c.Poll(ctx)
		if err != nil {
			if ctx.Err() != nil {
				if commitEvery > 0 {
					_ = c.CommitOffsets(context.Background())
				}
				fmt.Println("\nstopped.")
				return
			}
			fatalf("consume: %v", err)
		}
		printMsg(msg)
		count++
		if commitEvery > 0 && count%commitEvery == 0 {
			if err := c.CommitOffsets(ctx); err != nil {
				fmt.Fprintf(os.Stderr, "warn: commit offsets: %v\n", err)
			}
		}
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

func coordinatorClient(addr string) (pb.CoordinatorClient, *grpc.ClientConn) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fatalf("dial coordinator %s: %v", addr, err)
	}
	return pb.NewCoordinatorClient(conn), conn
}

func printMsg(msg *pb.Message) {
	key := string(msg.Key)
	if key == "" {
		key = "<no key>"
	}
	fmt.Printf("offset=%-6d key=%-16s value=%s\n", msg.Offset, key, msg.Value)
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	os.Exit(1)
}
