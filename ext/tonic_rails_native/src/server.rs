//! `TonicRails::Native::Server` — owns the tokio runtime and the tonic
//! server, and runs Ruby dispatch workers.
//!
//! Lifecycle (driven by `Tonic::Rails::Server`):
//!
//! ```text
//! new(addr, max_message_size) → set_* options → on_request(proc)
//! start_server             bind, build tonic router, spawn on tokio
//! serve_worker × N         (Ruby threads) dispatch RPCs until stopped
//! begin_shutdown           health → NOT_SERVING, stop accepting, GOAWAY
//! wait_for_drain(secs)     wait for in-flight RPCs (workers keep running)
//! stop_workers             workers return; Ruby joins them
//! stop_server              drop the runtime, closing anything left
//! ```

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use magnus::{
    Error, RArray, RHash, RString, Ruby, TryConvert, Value, gc, typed_data::DataTypeFunctions, value::Opaque,
};
use prost::Message;
use tokio::runtime::Runtime;
use tokio::sync::{Semaphore, watch};
use tonic_health::ServingStatus;
use tonic_health::server::HealthReporter;

use http::{HeaderName, HeaderValue, Method};
use tower_http::cors::{AllowOrigin, CorsLayer};

use crate::body::ServerStats;
use crate::dispatch::{DispatchInner, DispatchService};
use crate::gvl::{self, Cancel, DispatchContext};
use crate::log::{Level, LogEvent, LogSink};
use crate::net::{LimitedIncoming, bind, listen, listen_inherited};
use crate::sync::Mutex;

/// The overall-server health entry (empty service name).
const OVERALL: &str = "";

/// HTTP/2 receive windows; see `start_server`.
const STREAM_WINDOW: u32 = 256 * 1024;
const CONNECTION_WINDOW: u32 = 64 * 1024 * 1024;

struct Options {
    tls: Option<(String, String)>,
    tls_client_ca: Option<String>,
    max_connections: usize,
    concurrency_limit_per_connection: usize,
    worker_threads: Option<usize>,
    keepalive_interval: Duration,
    keepalive_timeout: Duration,
    reflection: bool,
    grpc_web: Option<GrpcWebOptions>,
    dispatch_queue_size: usize,
    routes: Vec<String>,
    descriptors: Vec<Vec<u8>>,
    /// Accept on this inherited listening socket instead of binding.
    listener_fd: Option<i32>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            tls: None,
            tls_client_ca: None,
            max_connections: 64,
            concurrency_limit_per_connection: 32,
            worker_threads: None,
            keepalive_interval: Duration::from_secs(15),
            keepalive_timeout: Duration::from_secs(5),
            reflection: false,
            grpc_web: None,
            dispatch_queue_size: 128,
            routes: Vec::new(),
            descriptors: Vec::new(),
            listener_fd: None,
        }
    }
}

/// gRPC-Web and the CORS policy browsers need to use it.
struct GrpcWebOptions {
    /// Allowed browser origins; empty = same-origin only; ["*"] = any.
    origins: Vec<String>,
    allow_headers: Vec<String>,
    expose_headers: Vec<String>,
}

/// Request headers gRPC-Web clients send, beyond the CORS-safelisted ones.
const GRPC_WEB_REQUEST_HEADERS: [&str; 5] = [
    "content-type",
    "x-grpc-web",
    "x-user-agent",
    "grpc-timeout",
    "authorization",
];
/// Response headers browsers must be allowed to read for gRPC-Web.
const GRPC_WEB_RESPONSE_HEADERS: [&str; 3] = ["grpc-status", "grpc-message", "grpc-status-details-bin"];

fn header_names(ruby: &Ruby, defaults: &[&str], extra: &[String]) -> Result<Vec<HeaderName>, Error> {
    defaults
        .iter()
        .map(|name| name.to_string())
        .chain(extra.iter().map(|name| name.to_ascii_lowercase()))
        .map(|name| {
            HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| arg_error(ruby, format!("invalid gRPC-Web header name: {name:?}")))
        })
        .collect()
}

/// None when no cross-origin access is configured (same-origin only).
fn cors_layer(ruby: &Ruby, web: &GrpcWebOptions) -> Result<Option<CorsLayer>, Error> {
    if web.origins.is_empty() {
        return Ok(None);
    }
    let layer = CorsLayer::new()
        .allow_methods([Method::POST])
        .allow_headers(header_names(ruby, &GRPC_WEB_REQUEST_HEADERS, &web.allow_headers)?)
        .expose_headers(header_names(
            ruby,
            &GRPC_WEB_RESPONSE_HEADERS,
            &web.expose_headers,
        )?)
        .max_age(Duration::from_secs(24 * 60 * 60));
    if web.origins.iter().any(|origin| origin == "*") {
        if web.origins.len() > 1 {
            return Err(arg_error(
                ruby,
                "grpc_web_origins: \"*\" can't be combined with other origins",
            ));
        }
        // Browsers refuse credentials with a wildcard origin, so none here.
        return Ok(Some(layer.allow_origin(AllowOrigin::any())));
    }
    let origins = web
        .origins
        .iter()
        .map(|origin| {
            HeaderValue::from_str(origin.trim_end_matches('/'))
                .map_err(|_| arg_error(ruby, format!("invalid gRPC-Web origin: {origin:?}")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Some(
        layer
            .allow_origin(AllowOrigin::list(origins))
            .allow_credentials(true),
    ))
}

/// State that exists only while the server is running.
struct Running {
    runtime: Runtime,
    request_rx: crossbeam_channel::Receiver<Box<DispatchContext>>,
    stop_workers: Arc<AtomicBool>,
    shutdown_tx: watch::Sender<bool>,
    done_rx: crossbeam_channel::Receiver<()>,
    health: HealthReporter,
    services: Vec<String>,
}

#[derive(magnus::TypedData)]
#[magnus(class = "TonicRails::Native::Server", mark)]
pub struct NativeServer {
    bind_address: String,
    max_message_size: usize,
    callback: Mutex<Option<Opaque<Value>>>,
    options: Mutex<Options>,
    running: Mutex<Option<Running>>,
    stats: Arc<ServerStats>,
    compression: Arc<AtomicBool>,
    active_connections: Arc<AtomicUsize>,
    /// Transport log events waiting for Ruby (`next_log_events`).
    log_rx: crossbeam_channel::Receiver<LogEvent>,
}

impl DataTypeFunctions for NativeServer {
    fn mark(&self, marker: &gc::Marker) {
        if let Some(callback) = *self.callback.lock() {
            marker.mark(callback);
        }
    }
}

fn runtime_error(ruby: &Ruby, message: impl Into<std::borrow::Cow<'static, str>>) -> Error {
    Error::new(ruby.exception_runtime_error(), message)
}

fn arg_error(ruby: &Ruby, message: impl Into<std::borrow::Cow<'static, str>>) -> Error {
    Error::new(ruby.exception_arg_error(), message)
}

fn read_file(ruby: &Ruby, what: &str, path: &str) -> Result<Vec<u8>, Error> {
    std::fs::read(path).map_err(|e| runtime_error(ruby, format!("cannot read TLS {what} {path}: {e}")))
}

/// `TonicRails::Native.bind_listener(address)`: bind and listen the way
/// the server does, returning the socket's fd for worker processes to
/// inherit and accept on (`Server#set_listener_fd`).  The caller owns it.
#[cfg(unix)]
pub fn bind_listener(ruby: &Ruby, address: String) -> Result<i32, Error> {
    use std::os::fd::IntoRawFd;

    let addr: SocketAddr = address
        .parse()
        .map_err(|e| arg_error(ruby, format!("invalid bind address {address:?}: {e}")))?;
    let listener =
        bind(addr, 1024).map_err(|e| runtime_error(ruby, format!("failed to bind {addr}: {e}")))?;
    Ok(listener.into_raw_fd())
}

#[cfg(not(unix))]
pub fn bind_listener(ruby: &Ruby, _address: String) -> Result<i32, Error> {
    Err(Error::new(
        ruby.exception_not_imp_error(),
        "multi-process mode needs fork(), which this platform lacks",
    ))
}

fn secs(value: f64) -> Duration {
    Duration::from_secs_f64(value.max(0.0))
}

impl NativeServer {
    pub fn new(ruby: &Ruby, bind_address: String, max_message_size: usize) -> Result<Self, Error> {
        if max_message_size == 0 || max_message_size > u32::MAX as usize {
            return Err(arg_error(
                ruby,
                "max_message_size must be between 1 and 4294967295",
            ));
        }
        bind_address
            .parse::<SocketAddr>()
            .map_err(|e| arg_error(ruby, format!("invalid bind address {bind_address:?}: {e}")))?;
        let (log, log_rx) = LogSink::new();
        Ok(Self {
            bind_address,
            max_message_size,
            callback: Mutex::new(None),
            options: Mutex::new(Options::default()),
            running: Mutex::new(None),
            stats: Arc::new(ServerStats {
                log,
                ..ServerStats::default()
            }),
            compression: Arc::new(AtomicBool::new(false)),
            active_connections: Arc::new(AtomicUsize::new(0)),
            log_rx,
        })
    }

    pub fn on_request(&self, callback: Value) {
        *self.callback.lock() = Some(callback.into());
    }

    pub fn set_tls(ruby: &Ruby, this: &Self, cert_path: String, key_path: String) -> Result<(), Error> {
        for (what, path) in [("certificate", &cert_path), ("key", &key_path)] {
            if !std::path::Path::new(path).is_file() {
                return Err(runtime_error(ruby, format!("TLS {what} file not found: {path}")));
            }
        }
        this.options.lock().tls = Some((cert_path, key_path));
        Ok(())
    }

    pub fn set_tls_client_ca(ruby: &Ruby, this: &Self, ca_path: String) -> Result<(), Error> {
        if !std::path::Path::new(&ca_path).is_file() {
            return Err(runtime_error(
                ruby,
                format!("TLS client CA file not found: {ca_path}"),
            ));
        }
        this.options.lock().tls_client_ca = Some(ca_path);
        Ok(())
    }

    pub fn set_max_connections(&self, max: usize) {
        self.options.lock().max_connections = max;
    }

    pub fn set_concurrency_limit_per_connection(&self, limit: usize) {
        self.options.lock().concurrency_limit_per_connection = limit;
    }

    pub fn set_worker_threads(&self, n: usize) {
        self.options.lock().worker_threads = (n > 0).then_some(n);
    }

    pub fn set_keepalive(&self, interval: f64, timeout: f64) {
        let mut options = self.options.lock();
        options.keepalive_interval = secs(interval);
        options.keepalive_timeout = secs(timeout);
    }

    pub fn set_dispatch_queue_size(&self, size: usize) {
        self.options.lock().dispatch_queue_size = size.max(1);
    }

    pub fn set_compression(&self, enabled: bool) {
        self.compression.store(enabled, Ordering::Relaxed);
    }

    /// Enable gRPC-Web (and HTTP/1.1, which browsers use without TLS).
    pub fn set_grpc_web(
        &self,
        origins: Vec<String>,
        allow_headers: Vec<String>,
        expose_headers: Vec<String>,
    ) {
        self.options.lock().grpc_web = Some(GrpcWebOptions {
            origins,
            allow_headers,
            expose_headers,
        });
    }

    pub fn set_reflection(&self, enabled: bool) {
        self.options.lock().reflection = enabled;
    }

    /// Accept on a listening socket bound by a parent process (see
    /// `bind_listener`) instead of binding `bind_address`.
    pub fn set_listener_fd(&self, fd: i32) {
        self.options.lock().listener_fd = Some(fd);
    }

    /// gRPC paths (`/package.Service/Method`) served by Ruby handlers.
    pub fn set_routes(&self, routes: Vec<String>) {
        self.options.lock().routes = routes;
    }

    /// Serialized `google.protobuf.FileDescriptorProto`s for reflection.
    pub fn set_reflection_descriptors(&self, descriptors: RArray) -> Result<(), Error> {
        let mut out = Vec::with_capacity(descriptors.len());
        for value in descriptors.into_iter() {
            let s = RString::try_convert(value)?;
            out.push(unsafe { s.as_slice() }.to_vec());
        }
        self.options.lock().descriptors = out;
        Ok(())
    }

    pub fn start_server(ruby: &Ruby, this: &Self) -> Result<(), Error> {
        let mut running = this.running.lock();
        if running.is_some() {
            return Err(runtime_error(ruby, "server already running"));
        }
        let callback = this
            .callback
            .lock()
            .ok_or_else(|| runtime_error(ruby, "no dispatch callback set: call #on_request first"))?;

        let options = this.options.lock();
        let addr: SocketAddr = this.bind_address.parse().expect("validated in new");

        let routes: HashSet<String> = options.routes.iter().cloned().collect();
        let mut services: Vec<String> = routes
            .iter()
            .filter_map(|p| {
                p.strip_prefix('/')?
                    .split_once('/')
                    .map(|(svc, _)| svc.to_owned())
            })
            .collect();
        services.sort();
        services.dedup();

        // TLS problems must fail startup — never fall back to plaintext.
        let tls = match &options.tls {
            Some((cert, key)) => {
                let identity = tonic::transport::Identity::from_pem(
                    read_file(ruby, "certificate", cert)?,
                    read_file(ruby, "key", key)?,
                );
                let mut config = tonic::transport::ServerTlsConfig::new().identity(identity);
                if let Some(ca) = &options.tls_client_ca {
                    config = config.client_ca_root(tonic::transport::Certificate::from_pem(read_file(
                        ruby,
                        "client CA",
                        ca,
                    )?));
                }
                Some(config)
            }
            None => None,
        };

        let reflection = if options.reflection {
            let mut files = Vec::with_capacity(options.descriptors.len());
            for bytes in &options.descriptors {
                files.push(
                    prost_types::FileDescriptorProto::decode(bytes.as_slice())
                        .map_err(|e| arg_error(ruby, format!("invalid reflection descriptor: {e}")))?,
                );
            }
            let mut builder = tonic_reflection::server::Builder::configure()
                .register_file_descriptor_set(prost_types::FileDescriptorSet { file: files })
                .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
                .with_service_name("grpc.health.v1.Health")
                .with_service_name("grpc.reflection.v1.ServerReflection");
            for service in &services {
                builder = builder.with_service_name(service.clone());
            }
            Some(
                builder
                    .build_v1()
                    .map_err(|e| runtime_error(ruby, format!("failed to build reflection service: {e}")))?,
            )
        } else {
            None
        };

        crate::log::log_connection_errors();
        let thread_log = this.stats.log.clone();
        let mut rt = tokio::runtime::Builder::new_multi_thread();
        rt.enable_all()
            .thread_name("tonic-rails")
            .on_thread_start(move || crate::log::set_thread_sink(thread_log.clone()));
        if let Some(n) = options.worker_threads {
            rt.worker_threads(n);
        }
        let runtime = rt
            .build()
            .map_err(|e| runtime_error(ruby, format!("failed to create tokio runtime: {e}")))?;

        let accept = {
            let _guard = runtime.enter();
            match options.listener_fd {
                Some(fd) => listen_inherited(fd, this.active_connections.clone()).map_err(|e| {
                    runtime_error(ruby, format!("cannot use inherited listener (fd {fd}): {e}"))
                })?,
                None => listen(addr, 1024)
                    .map_err(|e| runtime_error(ruby, format!("failed to bind {addr}: {e}")))?,
            }
        };

        let (health, health_service) = tonic_health::server::health_reporter();
        runtime.block_on(async {
            health.set_service_status(OVERALL, ServingStatus::Serving).await;
            for service in &services {
                health.set_service_status(service, ServingStatus::Serving).await;
            }
        });

        let (request_tx, request_rx) = crossbeam_channel::bounded(options.dispatch_queue_size);
        let dispatch = DispatchService(Arc::new(DispatchInner {
            callback,
            routes,
            request_tx,
            stats: this.stats.clone(),
            max_message_size: this.max_message_size,
            compression: this.compression.clone(),
        }));

        let mut grpc_routes = tonic::service::Routes::new(health_service);
        if let Some(reflection) = reflection {
            grpc_routes = grpc_routes.add_service(reflection);
        }
        let grpc_routes =
            tonic::service::Routes::from(grpc_routes.into_axum_router().fallback_service(dispatch));

        // Request bodies are read ahead into a buffer of their own (see
        // `feed_request`), so the stream window only bounds what's in
        // flight past it, and stays small: h2 closes a connection whose
        // unread small DATA frames exceed a budget of half the connection
        // window (#56).  The connection window is large so that budget is:
        // unread data per stream is bounded by the stream window anyway.
        let mut builder = tonic::transport::Server::builder()
            .http2_keepalive_interval(Some(options.keepalive_interval))
            .http2_keepalive_timeout(Some(options.keepalive_timeout))
            .http2_max_header_list_size(16 * 1024)
            .http2_max_pending_accept_reset_streams(Some(64))
            .initial_stream_window_size(STREAM_WINDOW)
            .initial_connection_window_size(CONNECTION_WINDOW);
        if options.concurrency_limit_per_connection > 0 {
            let limit = options.concurrency_limit_per_connection;
            builder = builder
                .concurrency_limit_per_connection(limit)
                .max_concurrent_streams(u32::try_from(limit).unwrap_or(u32::MAX));
        }
        if let Some(tls) = tls {
            builder = builder
                .tls_config(tls)
                .map_err(|e| runtime_error(ruby, format!("invalid TLS configuration: {e}")))?;
        }
        // CORS outermost, so preflight requests are answered before the
        // gRPC-Web translation sees them.  Both are no-ops when disabled.
        let (cors, grpc_web) = match &options.grpc_web {
            Some(web) => (cors_layer(ruby, web)?, Some(tonic_web::GrpcWebLayer::new())),
            None => (None, None),
        };
        let router = builder
            .accept_http1(options.grpc_web.is_some())
            .layer(tower::util::option_layer(cors))
            .layer(tower::util::option_layer(grpc_web))
            .add_routes(grpc_routes);

        let permits = match options.max_connections {
            0 => Semaphore::MAX_PERMITS,
            n => n,
        };
        let incoming = LimitedIncoming {
            inner: accept,
            semaphore: Arc::new(Semaphore::new(permits)),
            active: this.active_connections.clone(),
            log: this.stats.log.clone(),
        };

        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let (done_tx, done_rx) = crossbeam_channel::bounded(1);
        let log = this.stats.log.clone();
        runtime.spawn(async move {
            let signal = async move {
                let _ = shutdown_rx.wait_for(|stop| *stop).await;
            };
            if let Err(e) = router.serve_with_incoming_shutdown(incoming, signal).await {
                log.log(Level::Error, &format!("server error: {e}"));
            }
            let _ = done_tx.send(());
        });

        *running = Some(Running {
            runtime,
            request_rx,
            stop_workers: Arc::new(AtomicBool::new(false)),
            shutdown_tx,
            done_rx,
            health,
            services,
        });
        Ok(())
    }

    /// Run one dispatch worker.  Blocks (GVL released while idle) until
    /// `stop_workers` is called.  Interrupts are serviced in between.
    pub fn serve_worker(ruby: &Ruby, this: &Self) -> Result<(), Error> {
        let (request_rx, stop) = match this.running.lock().as_ref() {
            Some(r) => (r.request_rx.clone(), r.stop_workers.clone()),
            None => return Err(runtime_error(ruby, "server not started")),
        };

        while !stop.load(Ordering::Acquire) {
            let cancel = Cancel::default();
            gvl::without_gvl(&cancel, || {
                while !stop.load(Ordering::Acquire) && !cancel.is_cancelled() {
                    match request_rx.recv_timeout(Duration::from_millis(100)) {
                        Ok(ctx) => gvl::dispatch(ctx, &request_rx),
                        Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                        Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return,
                    }
                }
            });
            ruby.thread_check_ints()?;
        }
        Ok(())
    }

    /// Set the grpc.health.v1 status of one service ("" = the whole
    /// server).  Returns false if the server isn't running.
    pub fn set_health(&self, service: String, serving: bool) -> bool {
        let Some(r) = self
            .running
            .lock()
            .as_ref()
            .map(|r| (r.runtime.handle().clone(), r.health.clone()))
        else {
            return false;
        };
        let (handle, health) = r;
        let status = if serving {
            ServingStatus::Serving
        } else {
            ServingStatus::NotServing
        };
        handle.block_on(health.set_service_status(service, status));
        true
    }

    /// Mark everything NOT_SERVING and stop accepting new connections.
    /// Existing connections receive GOAWAY; in-flight RPCs continue.
    pub fn begin_shutdown(&self) {
        if let Some(r) = self.running.lock().as_ref() {
            r.runtime.block_on(async {
                r.health
                    .set_service_status(OVERALL, ServingStatus::NotServing)
                    .await;
                for service in &r.services {
                    r.health
                        .set_service_status(service, ServingStatus::NotServing)
                        .await;
                }
            });
            let _ = r.shutdown_tx.send(true);
        }
    }

    /// Wait (GVL released, so workers can finish in-flight RPCs) until
    /// all connections have closed or `timeout` seconds pass.
    /// Returns true if the server drained completely.
    pub fn wait_for_drain(ruby: &Ruby, this: &Self, timeout: f64) -> Result<bool, Error> {
        let Some(done_rx) = this.running.lock().as_ref().map(|r| r.done_rx.clone()) else {
            return Ok(true);
        };
        let deadline = Instant::now() + secs(timeout);
        loop {
            let cancel = Cancel::default();
            let drained = gvl::without_gvl(&cancel, || {
                while !cancel.is_cancelled() {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Some(false);
                    }
                    match done_rx.recv_timeout(remaining.min(Duration::from_millis(100))) {
                        Ok(()) | Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return Some(true),
                        Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                    }
                }
                None
            });
            match drained.flatten() {
                Some(result) => return Ok(result),
                None => ruby.thread_check_ints()?,
            }
        }
    }

    pub fn stop_workers(&self) {
        if let Some(r) = self.running.lock().as_ref() {
            r.stop_workers.store(true, Ordering::Release);
        }
    }

    /// Tear down the runtime.  Connections still open are closed.
    pub fn stop_server(&self) {
        let Some(running) = self.running.lock().take() else {
            return;
        };
        running.stop_workers.store(true, Ordering::Release);
        let _ = running.shutdown_tx.send(true);
        let mut runtime = Some(running.runtime);
        // Shutdown joins tokio worker threads; never do that holding the GVL.
        let cancel = Cancel::default();
        gvl::without_gvl(&cancel, || {
            if let Some(rt) = runtime.take() {
                rt.shutdown_timeout(Duration::from_secs(1));
            }
        });
        // An interrupt was pending, so the closure never ran.
        if let Some(rt) = runtime {
            rt.shutdown_background();
        }
    }

    /// Wait up to `timeout` seconds (GVL released) for transport log
    /// events and return them as `[[level, message], ...]`, oldest first.
    /// Returns an empty Array on timeout.
    pub fn next_log_events(ruby: &Ruby, this: &Self, timeout: f64) -> Result<RArray, Error> {
        let cancel = Cancel::default();
        let rx = &this.log_rx;
        let deadline = Instant::now() + secs(timeout);
        let first = gvl::without_gvl(&cancel, || {
            while !cancel.is_cancelled() {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return None;
                }
                match rx.recv_timeout(remaining.min(Duration::from_millis(100))) {
                    Ok(event) => return Some(event),
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                    Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return None,
                }
            }
            None
        })
        .flatten();

        let events = ruby.ary_new();
        for event in first.into_iter().chain(rx.try_iter().take(255)) {
            events.push((ruby.to_symbol(event.level.as_str()), event.message))?;
        }
        if events.is_empty() {
            // Interrupted: let Ruby run trap handlers / raise before retrying.
            ruby.thread_check_ints()?;
        }
        Ok(events)
    }

    pub fn is_running(&self) -> bool {
        self.running.lock().is_some()
    }

    pub fn stats(ruby: &Ruby, this: &Self) -> Result<RHash, Error> {
        let s = &this.stats;
        let queued = this.running.lock().as_ref().map_or(0, |r| r.request_rx.len());
        let hash = ruby.hash_new();
        for (key, value) in [
            ("total", s.total.load(Ordering::Relaxed)),
            ("successful", s.successful.load(Ordering::Relaxed)),
            ("errors", s.errors.load(Ordering::Relaxed)),
            ("cancelled", s.cancelled.load(Ordering::Relaxed)),
            ("deadline_exceeded", s.deadline_exceeded.load(Ordering::Relaxed)),
            ("resource_exhausted", s.resource_exhausted.load(Ordering::Relaxed)),
            ("unimplemented", s.unimplemented.load(Ordering::Relaxed)),
            (
                "active_connections",
                this.active_connections.load(Ordering::Relaxed),
            ),
            ("queued", queued),
        ] {
            hash.aset(ruby.to_symbol(key), value)?;
        }
        Ok(hash)
    }
}
