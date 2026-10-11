// Load generator for bench.v1.BenchService.
//
// gRPC mode uses one persistent HTTP/2 connection shared by -concurrency
// goroutines, so results reflect server throughput rather than connection
// setup.  HTTP mode (-protocol http) sends the same unary requests as JSON
// POSTs to /bench.v1.BenchService/<Method> over keep-alive HTTP/1.1
// connections, for comparing against a plain Rails JSON API.
//
// Usage:
//
//	go run . -addr 127.0.0.1:50051 -scenario io -concurrency 50 -total 5000
//	go run . -protocol http -addr 127.0.0.1:50064 -scenario io
//	go run . -list
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"slices"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	pb "tonic-bench/proto"
)

type scenario struct {
	description string
	call        func(ctx context.Context, c pb.BenchServiceClient) error
	// Unary scenarios also run over HTTP/JSON; streaming ones have no
	// method here.
	method  string
	request proto.Message
}

func work(description string, req *pb.WorkRequest) scenario {
	return scenario{
		description: description,
		call: func(ctx context.Context, c pb.BenchServiceClient) error {
			_, err := c.Work(ctx, req)
			return err
		},
		method:  "Work",
		request: req,
	}
}

var scenarios = map[string]scenario{
	"ping": {
		description: "empty request and response: transport + dispatch overhead",
		call: func(ctx context.Context, c pb.BenchServiceClient) error {
			_, err := c.Ping(ctx, &pb.Empty{})
			return err
		},
		method:  "Ping",
		request: &pb.Empty{},
	},
	"small":    work("one small record, no I/O", &pb.WorkRequest{Records: 1}),
	"io":       work("one 2ms simulated I/O call, one record", &pb.WorkRequest{IoCalls: 1, IoMs: 2, Records: 1}),
	"multi_io": work("three 5ms simulated I/O calls, ten records", &pb.WorkRequest{IoCalls: 3, IoMs: 5, Records: 10}),
	"large":    work("100 records with 256-byte blobs (~35KB response)", &pb.WorkRequest{Records: 100, PayloadBytes: 256}),
	"cpu":      work("20k-iteration checksum loop, one record", &pb.WorkRequest{CpuIterations: 20000, Records: 1}),
	"mixed":    work("2ms I/O + 5k CPU iterations + 20 records", &pb.WorkRequest{IoCalls: 1, IoMs: 2, CpuIterations: 5000, Records: 20, PayloadBytes: 64}),
	"stream": {description: "server stream of 100 records (gRPC only)", call: func(ctx context.Context, c pb.BenchServiceClient) error {
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
	"upload": {description: "client stream of 100 records (gRPC only)", call: func(ctx context.Context, c pb.BenchServiceClient) error {
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
	protocol := flag.String("protocol", "grpc", "grpc, or http for JSON POSTs (unary scenarios only)")
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

	var call func(context.Context) error
	switch *protocol {
	case "grpc":
		conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("connect: %v", err)
		}
		defer conn.Close()
		client := pb.NewBenchServiceClient(conn)
		call = func(ctx context.Context) error { return sc.call(ctx, client) }
	case "http":
		if sc.method == "" {
			log.Fatalf("scenario %q is gRPC-only", *name)
		}
		call = httpCaller(*addr, sc, *concurrency)
	default:
		log.Fatalf("unknown protocol %q", *protocol)
	}

	if err := run(call, *warmup, *concurrency); err != nil {
		log.Fatalf("warm-up failed: %v", err)
	}

	latencies := make([]time.Duration, 0, *total)
	var mu sync.Mutex
	start := time.Now()
	errors := runCollect(call, *total, *concurrency, func(d time.Duration) {
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

// httpCaller posts the scenario's request as JSON (protobuf field names)
// over a pool of keep-alive connections, one per in-flight request.
func httpCaller(addr string, sc scenario, concurrency int) func(context.Context) error {
	body, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(sc.request)
	if err != nil {
		log.Fatalf("encode request: %v", err)
	}
	url := "http://" + addr + "/bench.v1.BenchService/" + sc.method
	client := &http.Client{Transport: &http.Transport{
		MaxIdleConns:        concurrency,
		MaxIdleConnsPerHost: concurrency,
		MaxConnsPerHost:     concurrency,
	}}
	return func(ctx context.Context) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if _, err := io.Copy(io.Discard, resp.Body); err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("HTTP %d", resp.StatusCode)
		}
		return nil
	}
}

func run(call func(context.Context) error, n, concurrency int) error {
	if n == 0 {
		return nil
	}
	if errs := runCollect(call, n, concurrency, func(time.Duration) {}); errs > 0 {
		return fmt.Errorf("%d errors", errs)
	}
	return nil
}

func runCollect(call func(context.Context) error, n, concurrency int, record func(time.Duration)) int {
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
				err := call(ctx)
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
