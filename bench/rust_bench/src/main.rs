// Pure Rust tonic reference server — identical handler patterns to bench_service.rb
// to isolate GVL overhead from handler-time ceiling.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{transport::Server, Request, Response, Status};

pub mod shop {
    tonic::include_proto!("shop");

    pub const FILE_DESCRIPTOR_SET: &[u8] =
        include_bytes!("shop_descriptor.bin");
}

use shop::product_service_server::{
    ProductService, ProductServiceServer,
};
use shop::*;

// ── SimulatedDB — mirrors the Ruby SimulatedDB exactly ──────────────

struct SimulatedDB {
    data: HashMap<i64, Product>,
}

impl SimulatedDB {
    fn new(count: usize) -> Self {
        let mut data = HashMap::with_capacity(count);
        let tags_pool = ["featured", "new", "sale"];
        let statuses = [1i32, 2, 2, 2, 3]; // weighted toward ACTIVE

        for i in 0..count {
            let id = (i + 1) as i64;
            // Deterministic RNG to match Ruby's rand (close enough — we just need
            // realistic-sized data; exact values don't matter for benchmarking).
            let price = ((id * 7 + 13) % 10000) as i64 + 100;

            // Pick 1-3 tags deterministically
            let tag_count = ((id as usize) % 3) + 1;
            let tags: Vec<String> = tags_pool[..tag_count]
                .iter()
                .map(|s| s.to_string())
                .collect();

            let status_idx = (i % statuses.len()) as i32;
            let status = ProductStatus::try_from(statuses[status_idx as usize])
                .unwrap_or(ProductStatus::Active);

            data.insert(id, Product {
                id,
                title: format!("Product {}", id),
                description: format!(
                    "A fine product with description {}. lorem ipsum dolor sit amet. lorem ipsum dolor sit amet. lorem ipsum dolor sit amet. ",
                    id
                ),
                price_cents: price,
                currency: "USD".to_string(),
                tags,
                status: status as i32,
                created_at_unix: 1700000000 + (i as i64) * 3600,
                updated_at_unix: 1700000000 + (i as i64) * 3600 + 1800,
            });
        }
        Self { data }
    }

    fn find(&self, id: i64) -> Option<&Product> {
        self.data.get(&id)
    }

    fn all(&self, limit: i32, offset: i32, tag_filter: Option<&str>) -> (Vec<Product>, i32) {
        let mut results: Vec<&Product> = self.data.values().collect();
        if let Some(tag) = tag_filter {
            results.retain(|p| p.tags.iter().any(|t| t == tag));
        }
        let total = results.len() as i32;
        let start = (offset as usize).min(results.len());
        let end = (start + limit as usize).min(results.len());
        let products = results[start..end].iter().map(|p| (*p).clone()).collect();
        (products, total)
    }

    fn count(&self) -> i32 {
        self.data.len() as i32
    }
}

// ── JSON CPU work — mirrors Ruby's JSON.generate calls ──────────────

#[derive(serde::Serialize)]
struct ProductJson<'a> {
    id: i64,
    title: &'a str,
    description: &'a str,
    price_cents: i64,
    currency: &'a str,
    tags: &'a [String],
    status: i32,
    created_at_unix: i64,
    updated_at_unix: i64,
}

fn json_serialize_product(p: &Product) -> String {
    // Direct serialization — no intermediate Value tree, no string cloning.
    // This mirrors Ruby's JSON.generate(hash) which serializes in a single pass.
    let pj = ProductJson {
        id: p.id,
        title: &p.title,
        description: &p.description,
        price_cents: p.price_cents,
        currency: &p.currency,
        tags: &p.tags,
        status: p.status,
        created_at_unix: p.created_at_unix,
        updated_at_unix: p.updated_at_unix,
    };
    serde_json::to_string(&pj).unwrap()
}

// ── Server implementation ───────────────────────────────────────────

struct ProductServiceImpl {
    db: Arc<SimulatedDB>,
    insert_counter: Arc<Mutex<i64>>,
}

impl ProductServiceImpl {
    fn new() -> Self {
        Self {
            db: Arc::new(SimulatedDB::new(1000)),
            insert_counter: Arc::new(Mutex::new(1000)),
        }
    }
}

#[tonic::async_trait]
impl ProductService for ProductServiceImpl {
    async fn get_product(
        &self,
        request: Request<GetProductRequest>,
    ) -> Result<Response<Product>, Status> {
        let req = request.into_inner();
        // Simulate DB I/O — tokio sleep is truly async (no GVL)
        let db_latency = get_db_latency_ms();
            if db_latency > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(db_latency)).await;
            }

        let product = self.db.find(req.id).ok_or_else(|| {
            Status::not_found(format!("product {} not found", req.id))
        })?;

        // CPU: JSON serialization (cache fragment)
        let _json = json_serialize_product(product);

        Ok(Response::new(product.clone()))
    }

    async fn list_products(
        &self,
        request: Request<ListProductsRequest>,
    ) -> Result<Response<ListProductsResponse>, Status> {
        let req = request.into_inner();
        let db_latency = get_db_latency_ms();
            if db_latency > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(db_latency)).await;
            }

        let (products, total) = self.db.all(
            req.limit,
            req.offset,
            if req.tag_filter.is_empty() { None } else { Some(&req.tag_filter) },
        );

        // CPU: view rendering (JSON per product, joined)
        let _render: String = products
            .iter()
            .map(json_serialize_product)
            .collect::<Vec<_>>()
            .join(",");

        Ok(Response::new(ListProductsResponse { products, total }))
    }

    async fn create_product(
        &self,
        request: Request<CreateProductRequest>,
    ) -> Result<Response<Product>, Status> {
        let req = request.into_inner();

        // Validation
        if req.title.trim().is_empty() {
            return Err(Status::invalid_argument("title is required"));
        }
        if req.price_cents <= 0 {
            return Err(Status::invalid_argument("price_cents must be positive"));
        }

        let db_latency = get_db_latency_ms();
            if db_latency > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(db_latency)).await;
            }

        let mut counter = self.insert_counter.lock().await;
        *counter += 1;
        let id = *counter;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let product = Product {
            id,
            title: req.title,
            description: req.description,
            price_cents: req.price_cents,
            currency: req.currency,
            tags: req.tags,
            status: ProductStatus::Draft as i32,
            created_at_unix: now,
            updated_at_unix: now,
        };

        Ok(Response::new(product))
    }

    async fn batch_get_products(
        &self,
        request: Request<BatchGetProductsRequest>,
    ) -> Result<Response<BatchGetProductsResponse>, Status> {
        let req = request.into_inner();
        let db_latency = get_db_latency_ms();
            if db_latency > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(db_latency)).await;
            }

        let mut products = Vec::new();
        let mut not_found_ids = Vec::new();

        for id in &req.ids {
            if let Some(p) = self.db.find(*id) {
                products.push(p.clone());
            } else {
                not_found_ids.push(*id);
            }
        }

        Ok(Response::new(BatchGetProductsResponse { products, not_found_ids }))
    }

    async fn health_check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        Ok(Response::new(HealthCheckResponse {
            healthy: true,
            service: "product-service".to_string(),
        }))
    }

    // ── Realistic multi-query handlers ──────────────────────────────

    async fn get_product_detail(
        &self,
        request: Request<GetProductDetailRequest>,
    ) -> Result<Response<GetProductDetailResponse>, Status> {
        let req = request.into_inner();

        // Query 1: find product (5ms)
        sleep_ms(5).await;
        let product = self.db.find(req.id).ok_or_else(|| {
            Status::not_found("not found")
        })?;

        // CPU: cache fragment serialization
        let _cache_fragment = json_serialize_product(product);

        // Query 2: load reviews (10ms)
        sleep_ms(10).await;
        let reviews: Vec<Review> = (1..=5).map(|i| Review {
            id: i as i64,
            product_id: product.id,
            rating: 4 + (i % 2) as i32,
            author: format!("user{}", i),
            body: format!("Review {} for {}", i, product.title),
            created_at_unix: 1700000000 + i as i64 * 86400,
        }).collect();
        // CPU: build response with JSON fragment caching
        let _review_json: Vec<String> = reviews.iter().map(|r| {
            serde_json::json!({
                "id": r.id, "product_id": r.product_id, "rating": r.rating,
                "author": r.author, "body": r.body, "created_at_unix": r.created_at_unix,
            }).to_string()
        }).collect();

        // Query 3: load related products (8ms)
        sleep_ms(8).await;
        let related: Vec<Product> = (1..=4).map(|i| {
            let rid = product.id + i as i64 + 1;
            self.db.find(rid).cloned()
                .or_else(|| self.db.find((i + 1) as i64).cloned())
        }).flatten().collect();

        Ok(Response::new(GetProductDetailResponse {
            product: Some(product.clone()),
            reviews,
            related_products: related,
        }))
    }

    async fn search_products(
        &self,
        request: Request<SearchProductsRequest>,
    ) -> Result<Response<SearchProductsResponse>, Status> {
        let req = request.into_inner();
        let limit = if req.limit == 0 { 20 } else { req.limit };
        let offset = req.offset;
        let min_price = req.min_price_cents;
        let max_price = if req.max_price_cents == 0 { 1_000_000 } else { req.max_price_cents };
        let tag_filter = if req.tag_filter.is_empty() { None } else { Some(req.tag_filter.as_str()) };

        // Query 1: slow search (25ms)
        sleep_ms(25).await;
        let (products, total) = self.db.all(limit, offset, tag_filter);

        // CPU: filter by price range, build facets
        let filtered: Vec<Product> = products.into_iter()
            .filter(|p| p.price_cents >= min_price && p.price_cents <= max_price)
            .collect();
        let _facets: HashMap<String, usize> = {
            let mut m = HashMap::new();
            for p in &filtered {
                let key = p.tags.first().cloned().unwrap_or("none".to_string());
                *m.entry(key).or_insert(0) += 1;
            }
            m
        };

        // Query 2: load tag associations (5ms)
        sleep_ms(5).await;
        let _tag_json: Vec<String> = filtered.iter()
            .map(|p| serde_json::to_string(&p.tags).unwrap_or_default())
            .collect();

        Ok(Response::new(SearchProductsResponse {
            products: filtered,
            total,
            query_time_ms: 30,
        }))
    }

    async fn checkout_preview(
        &self,
        request: Request<CheckoutPreviewRequest>,
    ) -> Result<Response<CheckoutPreviewResponse>, Status> {
        let req = request.into_inner();
        if req.cart_items.is_empty() {
            return Err(Status::invalid_argument("empty cart"));
        }

        // Query 1: load cart (3ms)
        sleep_ms(3).await;

        // Query 2: validate inventory (15ms)
        sleep_ms(15).await;
        let mut items = Vec::new();
        for ci in &req.cart_items {
            let product = self.db.find(ci.product_id).ok_or_else(|| {
                Status::not_found(format!("product {} not found", ci.product_id))
            })?;
            let qty = ci.quantity;
            let unit = product.price_cents;
            items.push(CheckoutItem {
                product: Some(product.clone()),
                quantity: qty,
                unit_price_cents: unit,
                line_total_cents: unit * qty as i64,
            });
        }

        // CPU: calculate subtotal
        let subtotal: i64 = items.iter().map(|i| i.line_total_cents).sum();

        // Query 3: shipping (8ms)
        sleep_ms(8).await;
        let shipping = if subtotal > 5000 { 0 } else { 599 };

        // Query 4: user prefs / tax (5ms)
        sleep_ms(5).await;
        let tax = (subtotal as f64 * 0.085) as i64;

        Ok(Response::new(CheckoutPreviewResponse {
            items,
            subtotal_cents: subtotal,
            shipping_cents: shipping,
            tax_cents: tax,
            total_cents: subtotal + shipping + tax,
        }))
    }

    async fn dashboard(
        &self,
        _request: Request<DashboardRequest>,
    ) -> Result<Response<DashboardResponse>, Status> {
        // Query 1: recent orders aggregate (25ms)
        sleep_ms(25).await;
        let total_orders: i32 = 15_382;
        let total_revenue: i64 = 2_847_293_00;

        // CPU: aggregate stats
        let avg_order = total_revenue / (total_orders as i64).max(1);
        let _stats_json = serde_json::json!({
            "orders": total_orders, "revenue": total_revenue,
        }).to_string();

        // Query 2: revenue stats (20ms)
        sleep_ms(20).await;

        // Query 3: product count (10ms)
        sleep_ms(10).await;
        let (_, active_count) = self.db.all(1, 0, None);

        // CPU: build top products list
        let (top, _) = self.db.all(5, 0, None);
        let _top_json: Vec<String> = top.iter().map(json_serialize_product).collect();

        Ok(Response::new(DashboardResponse {
            total_orders,
            total_revenue_cents: total_revenue,
            avg_order_value_cents: avg_order,
            recent_product_count: self.db.count(),
            active_product_count: active_count,
            top_products: top,
        }))
    }
}

// ── Helpers ─────────────────────────────────────────────────────────


fn get_db_latency_ms() -> u64 {
    std::env::var("SIMULATED_DB_LATENCY_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2)
}

async fn sleep_ms(ms: u64) {
    if ms > 0 {
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
    }
}

// ── Main ────────────────────────────────────────────────────────────

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr_str = std::env::var("BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:50052".to_string());
    let addr: std::net::SocketAddr = addr_str.parse()?;

    let worker_threads = std::env::var("TOKIO_WORKER_THREADS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok());

    let runtime = match worker_threads {
        Some(n) => tokio::runtime::Builder::new_multi_thread()
            .worker_threads(n)
            .enable_all()
            .build()?,
        None => tokio::runtime::Runtime::new()?,
    };

    runtime.block_on(serve(addr))?;
    Ok(())
}

async fn serve(addr: std::net::SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let service = ProductServiceImpl::new();

    // Reflection: include the shop file descriptor set
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(shop::FILE_DESCRIPTOR_SET)
        .build_v1()
        .unwrap();

    println!("Pure Rust tonic server listening on {}", addr);

    Server::builder()
        .add_service(ProductServiceServer::new(service))
        .add_service(reflection)
        .serve(addr)
        .await?;

    Ok(())
}
