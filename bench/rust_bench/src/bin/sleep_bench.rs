use std::time::{Duration, Instant};

#[tokio::main]
async fn main() {
    for &ms in &[2u64, 5, 10, 20] {
        let mut times = Vec::with_capacity(500);
        for _ in 0..500 {
            let start = Instant::now();
            tokio::time::sleep(Duration::from_millis(ms)).await;
            times.push(start.elapsed().as_secs_f64() * 1000.0);
        }
        let avg: f64 = times.iter().sum::<f64>() / times.len() as f64;
        let min = times.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = times.iter().cloned().fold(0.0, f64::max);
        println!(
            "tokio sleep({}ms): avg={:.2}ms  min={:.2}ms  max={:.2}ms  overhead={:.0}µs",
            ms, avg, min, max, (avg - ms as f64) * 1000.0
        );
    }
}
