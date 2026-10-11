// Load generator for bench.v1.BenchService.
//
// Uses one persistent gRPC connection shared by -concurrency goroutines,
// so results reflect server throughput rather than connection setup.
//
// Usage:
//
//	go run . -addr 127.0.0.1:50051 -scenario io -concurrency 50 -total 5000
//	go run . -list
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "tonic-bench/proto"
)

type scenario struct {
	description string
	call        func(ctx context.Context, c pb.BenchServiceClient) error
}

func work(req *pb.WorkRequest) func(context.Context, pb.BenchServiceClient) error {
	return func(ctx context.Context, c pb.BenchServiceClient) error {
		_, err := c.Work(ctx, req)
		return err
	}
}

var scenarios = map[string]scenario{
	"ping": {"empty request and response: transport + dispatch overhead", func(ctx context.Context, c pb.BenchServiceClient) error {
		_, err := c.Ping(ctx, &pb.Empty{})
		return err
	}},
	"small":    {"one small record, no I/O", work(&pb.WorkRequest{Records: 1})},
	"io":       {"one 2ms simulated I/O call, one record", work(&pb.WorkRequest{IoCalls: 1, IoMs: 2, Records: 1})},
	"multi_io": {"three 5ms simulated I/O calls, ten records", work(&pb.WorkRequest{IoCalls: 3, IoMs: 5, Records: 10})},
	"large":    {"100 records with 256-byte blobs (~35KB response)", work(&pb.WorkRequest{Records: 100, PayloadBytes: 256})},
	"cpu":      {"20k-iteration checksum loop, one record", work(&pb.WorkRequest{CpuIterations: 20000, Records: 1})},
	"mixed":    {"2ms I/O + 5k CPU iterations + 20 records", work(&pb.WorkRequest{IoCalls: 1, IoMs: 2, CpuIterations: 5000, Records: 20, PayloadBytes: 64})},
	"stream": {"server stream of 100 records", func(ctx context.Context, c pb.BenchServiceClient) error {
		stream, err := c.StreamRecords(ctx, &pb.WorkRequest{Records: 100, PayloadBytes: 64})
		if err != nil {
			return err
		}
		for {
			if _, err := stream.Recv(); err == io.EOF {
				return nil
			} else if err != nil {
				return err
			}
		}
	}},
	"upload": {"client stream of 100 records", func(ctx context.Context, c pb.BenchServiceClient) error {
		stream, err := c.Upload(ctx)
		if err != nil {
			return err
		}
		for i := range 100 {
			if err := stream.Send(&pb.Record{Id: int64(i), Value: int64(i)}); err != nil {
				return err
			}
		}
		_, err = stream.CloseAndRecv()
		return err
	}},
}

func main() {
	addr := flag.String("addr", "127.0.0.1:50051", "server address")
	name := flag.String("scenario", "ping", "scenario to run (see -list)")
	concurrency := flag.Int("concurrency", 50, "concurrent in-flight requests")
	total := flag.Int("total", 5000, "total requests")
	warmup := flag.Int("warmup", 200, "unmeasured warm-up requests")
	list := flag.Bool("list", false, "list scenarios and exit")
	flag.Parse()

	if *list {
		names := make([]string, 0, len(scenarios))
		for n := range scenarios {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			fmt.Printf("%-10s %s\n", n, scenarios[n].description)
		}
		return
	}

	sc, ok := scenarios[*name]
	if !ok {
		log.Fatalf("unknown scenario %q (use -list)", *name)
	}

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer conn.Close()
	client := pb.NewBenchServiceClient(conn)

	if err := run(client, sc, *warmup, *concurrency); err != nil {
		log.Fatalf("warm-up failed: %v", err)
	}

	latencies := make([]time.Duration, 0, *total)
	var mu sync.Mutex
	start := time.Now()
	errors := runCollect(client, sc, *total, *concurrency, func(d time.Duration) {
		mu.Lock()
		latencies = append(latencies, d)
		mu.Unlock()
	})
	elapsed := time.Since(start)

	slices.Sort(latencies)
	pct := func(p float64) float64 {
		if len(latencies) == 0 {
			return 0
		}
		return float64(latencies[int(p*float64(len(latencies)-1))].Microseconds()) / 1000.0
	}
	fmt.Printf("%-9s c=%-4d n=%-6d %9.0f rps   p50 %7.2fms   p99 %7.2fms   errors %d\n",
		*name, *concurrency, len(latencies), float64(len(latencies))/elapsed.Seconds(), pct(0.50), pct(0.99), errors)
	if errors > 0 {
		os.Exit(1)
	}
}

func run(client pb.BenchServiceClient, sc scenario, n, concurrency int) error {
	if n == 0 {
		return nil
	}
	if errs := runCollect(client, sc, n, concurrency, func(time.Duration) {}); errs > 0 {
		return fmt.Errorf("%d errors", errs)
	}
	return nil
}

func runCollect(client pb.BenchServiceClient, sc scenario, n, concurrency int, record func(time.Duration)) int {
	jobs := make(chan struct{}, n)
	for range n {
		jobs <- struct{}{}
	}
	close(jobs)

	var wg sync.WaitGroup
	var mu sync.Mutex
	errs := 0
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range jobs {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				t := time.Now()
				err := sc.call(ctx, client)
				cancel()
				if err != nil {
					mu.Lock()
					if errs < 5 {
						fmt.Fprintf(os.Stderr, "rpc error: %v\n", err)
					}
					errs++
					mu.Unlock()
					continue
				}
				record(time.Since(t))
			}
		}()
	}
	wg.Wait()
	return errs
}
