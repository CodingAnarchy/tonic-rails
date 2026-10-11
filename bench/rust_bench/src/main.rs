//! Pure Rust tonic implementation of bench.v1.BenchService.
//!
//! Mirrors bench/bench_server.rb handler-for-handler so the difference in
//! results isolates the cost of dispatching into Ruby.
//!
//!   BIND=127.0.0.1:50052 TOKIO_WORKER_THREADS=4 ./target/release/tonic-bench-server

use std::pin::Pin;
use std::time::Duration;

use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming, transport::Server};

pub mod bench {
    tonic::include_proto!("bench.v1");
}

use bench::bench_service_server::{BenchService, BenchServiceServer};
use bench::{Empty, Record, WorkRequest, WorkResponse};

const TAGS: [&str; 3] = ["alpha", "beta", "gamma"];

async fn simulate_io(req: &WorkRequest) {
    for _ in 0..req.io_calls {
        tokio::time::sleep(Duration::from_millis(req.io_ms.max(0) as u64)).await;
    }
}

/// Deterministic integer work; must match the Ruby implementation.
fn checksum(iterations: i32) -> i64 {
    let mut x: i64 = 0;
    for i in 0..iterations.max(0) as i64 {
        x = (x * 31 + i) % 1_000_003;
    }
    x
}

fn record(index: i32, payload_bytes: i32) -> Record {
    Record {
        id: index as i64,
        name: format!("record-{index}"),
        tags: TAGS.iter().map(|t| t.to_string()).collect(),
        value: index as i64 * 7,
        blob: vec![b'x'; payload_bytes.max(0) as usize],
    }
}

#[derive(Default)]
struct Bench;

type RecordStream = Pin<Box<dyn Stream<Item = Result<Record, Status>> + Send>>;

#[tonic::async_trait]
impl BenchService for Bench {
    async fn ping(&self, _req: Request<Empty>) -> Result<Response<Empty>, Status> {
        Ok(Response::new(Empty {}))
    }

    async fn work(&self, req: Request<WorkRequest>) -> Result<Response<WorkResponse>, Status> {
        let req = req.into_inner();
        simulate_io(&req).await;
        Ok(Response::new(WorkResponse {
            records: (0..req.records).map(|i| record(i, req.payload_bytes)).collect(),
            checksum: checksum(req.cpu_iterations),
        }))
    }

    type StreamRecordsStream = RecordStream;

    async fn stream_records(
        &self,
        req: Request<WorkRequest>,
    ) -> Result<Response<Self::StreamRecordsStream>, Status> {
        let req = req.into_inner();
        simulate_io(&req).await;
        let payload = req.payload_bytes;
        let stream = tokio_stream::iter((0..req.records).map(move |i| Ok(record(i, payload))));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn upload(&self, req: Request<Streaming<Record>>) -> Result<Response<WorkResponse>, Status> {
        let mut stream = req.into_inner();
        let mut total = 0;
        while let Some(record) = stream.message().await? {
            total += record.value;
        }
        Ok(Response::new(WorkResponse {
            records: Vec::new(),
            checksum: total,
        }))
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::var("BIND").unwrap_or_else(|_| "127.0.0.1:50052".into()).parse()?;
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    if let Some(n) = std::env::var("TOKIO_WORKER_THREADS").ok().and_then(|s| s.parse().ok()) {
        builder.worker_threads(n);
    }
    builder.build()?.block_on(async move {
        eprintln!("tonic reference server listening on {addr}");
        // Same flow-control windows as tonic-rails (sized to its default
        // 4 MiB max_message_size) so the comparison is like-for-like.
        Server::builder()
            .initial_stream_window_size(4 * 1024 * 1024)
            .initial_connection_window_size(8 * 1024 * 1024)
            .add_service(BenchServiceServer::new(Bench))
            .serve(addr)
            .await
    })?;
    Ok(())
}
