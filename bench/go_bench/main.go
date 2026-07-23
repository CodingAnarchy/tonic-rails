// Benchmark client for tonic-rails.
//
// Uses a persistent gRPC channel (single TCP/H2 connection) with
// concurrent goroutines to measure throughput and latency without
// per-request connection overhead.
//
// Usage:
//   go run . -addr 127.0.0.1:50051 -rpc GetProduct -concurrency 10 -total 1000
//
// RPCs: HealthCheck, GetProduct, ListProducts, CreateProduct, BatchGetProducts
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "tonic-bench/proto"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:50051", "gRPC server address")
	rpcName := flag.String("rpc", "GetProduct", "RPC to benchmark")
	concurrency := flag.Int("concurrency", 10, "concurrent goroutines")
	total := flag.Int("total", 1000, "total requests")
	flag.Parse()

	// Single persistent channel shared by all goroutines.
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewProductServiceClient(conn)

	// Warm up the connection with a health check.
	_, err = client.HealthCheck(context.Background(), &pb.HealthCheckRequest{})
	if err != nil {
		log.Fatalf("health check failed: %v", err)
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
				if err := callRPC(client, *rpcName); err != nil {
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

	fmt.Printf("%-16s  %6d reqs  %8.1f ms total  %8.0f rps  %7.1f µs/req\n",
		*rpcName, completed, float64(elapsed.Microseconds())/1000.0, rps, avgLatencyUS)
}

func callRPC(client pb.ProductServiceClient, rpc string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	switch rpc {
	case "HealthCheck":
		_, err := client.HealthCheck(ctx, &pb.HealthCheckRequest{})
		return err
	case "GetProduct":
		_, err := client.GetProduct(ctx, &pb.GetProductRequest{Id: 42})
		return err
	case "ListProducts":
		_, err := client.ListProducts(ctx, &pb.ListProductsRequest{
			Limit: 20, Offset: 0, TagFilter: "featured",
		})
		return err
	case "CreateProduct":
		_, err := client.CreateProduct(ctx, &pb.CreateProductRequest{
			Title:       "Bench Product",
			Description: "Created by Go benchmark",
			PriceCents:  1999,
			Currency:    "USD",
			Tags:        []string{"bench", "new"},
		})
		return err
	case "BatchGetProducts":
		_, err := client.BatchGetProducts(ctx, &pb.BatchGetProductsRequest{
			Ids: []int64{1, 2, 3, 42, 100, 999},
		})
		return err
	case "GetProductDetail":
		_, err := client.GetProductDetail(ctx, &pb.GetProductDetailRequest{Id: 42})
		return err
	case "SearchProducts":
		_, err := client.SearchProducts(ctx, &pb.SearchProductsRequest{
			Query:      "product",
			Limit:      20,
			Offset:     0,
			MinPriceCents: 0,
			MaxPriceCents: 1000000,
			TagFilter:  "featured",
		})
		return err
	case "CheckoutPreview":
		_, err := client.CheckoutPreview(ctx, &pb.CheckoutPreviewRequest{
			UserId: 1,
			CartItems: []*pb.CartItem{
				{ProductId: 1, Quantity: 2},
				{ProductId: 2, Quantity: 1},
				{ProductId: 3, Quantity: 3},
			},
		})
		return err
	case "Dashboard":
		_, err := client.Dashboard(ctx, &pb.DashboardRequest{UserId: 1})
		return err
	default:
		return fmt.Errorf("unknown RPC: %s", rpc)
	}
}
