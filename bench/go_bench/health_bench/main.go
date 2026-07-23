package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:50051", "gRPC server address")
	concurrency := flag.Int("concurrency", 50, "concurrent goroutines")
	total := flag.Int("total", 2000, "total requests")
	flag.Parse()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect failed: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	client := healthpb.NewHealthClient(conn)

	// Warmup
	_, err = client.Check(context.Background(), &healthpb.HealthCheckRequest{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "warmup failed: %v\n", err)
		os.Exit(1)
	}

	var ops int64
	var latenciesNS int64
	var wg sync.WaitGroup

	reqsPerWorker := *total / *concurrency
	remainder := *total % *concurrency

	start := time.Now()

	for i := range *concurrency {
		wg.Add(1)
		n := reqsPerWorker
		if i == 0 {
			n += remainder
		}
		go func(n int) {
			defer wg.Done()
			for range n {
				reqStart := time.Now()
				_, err := client.Check(context.Background(), &healthpb.HealthCheckRequest{})
				if err != nil {
					fmt.Fprintf(os.Stderr, "rpc error: %v\n", err)
					continue
				}
				atomic.AddInt64(&latenciesNS, time.Since(reqStart).Nanoseconds())
				atomic.AddInt64(&ops, 1)
			}
		}(n)
	}

	wg.Wait()
	elapsed := time.Since(start)

	completed := atomic.LoadInt64(&ops)
	avgLatencyUS := float64(atomic.LoadInt64(&latenciesNS)) / float64(completed) / 1000.0
	rps := float64(completed) / elapsed.Seconds()

	fmt.Printf("%-40s  %6d reqs  %8.1f ms total  %8.0f rps  %7.1f µs/req\n",
		"grpc.health.v1.Health/Check", completed, float64(elapsed.Microseconds())/1000.0, rps, avgLatencyUS)
}
