//! tonic-rails native extension.
//!
//! Serves gRPC with tonic and dispatches each RPC to a Ruby callback.
//!
//! ```text
//!  tokio runtime (no GVL)                 Ruby dispatch threads
//!  ──────────────────────                 ─────────────────────
//!  tonic transport: HTTP/2, TLS,          serve_worker (GVL released
//!  keepalive, health, reflection            while idle)
//!          │                                     ▲
//!  DispatchService (fallback route)              │ crossbeam queue
//!    validate → queue DispatchContext ───────────┘
//!    spawn body reader ── request msgs ──▶ RequestReader#next
//!    ResponseBody ◀────── response msgs ── StreamSender#push / return
//! ```
//!
//! Message bytes pass through untouched: Ruby's google-protobuf does all
//! (de)serialization, so no service definitions are compiled in here.
//!
//! Modules:
//! - [`framing`]  — gRPC wire format (frames, gzip, timeouts, trailers)
//! - [`body`]     — response bodies and request stats
//! - [`dispatch`] — the fallback tower service that queues RPCs for Ruby
//! - [`gvl`]      — GVL release/acquire and the Ruby stream objects
//! - [`log`]      — transport log events for Ruby's logger
//! - [`net`]      — listener and connection cap
//! - [`server`]   — `TonicRails::Native::Server`

mod body;
mod dispatch;
mod framing;
mod gvl;
mod log;
mod net;
mod server;

use magnus::{Error, Ruby, function, method, prelude::*};

use gvl::{RequestReader, StreamSender};
use server::NativeServer;

#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), Error> {
    let native = ruby.define_module("TonicRails")?.define_module("Native")?;

    let class = native.define_class("Server", ruby.class_object())?;
    class.define_singleton_method("new", function!(NativeServer::new, 2))?;
    class.define_method("on_request", method!(NativeServer::on_request, 1))?;
    class.define_method("set_tls", method!(NativeServer::set_tls, 2))?;
    class.define_method("set_tls_client_ca", method!(NativeServer::set_tls_client_ca, 1))?;
    class.define_method(
        "set_max_connections",
        method!(NativeServer::set_max_connections, 1),
    )?;
    class.define_method(
        "set_concurrency_limit_per_connection",
        method!(NativeServer::set_concurrency_limit_per_connection, 1),
    )?;
    class.define_method("set_worker_threads", method!(NativeServer::set_worker_threads, 1))?;
    class.define_method("set_keepalive", method!(NativeServer::set_keepalive, 2))?;
    class.define_method(
        "set_dispatch_queue_size",
        method!(NativeServer::set_dispatch_queue_size, 1),
    )?;
    class.define_method("set_compression", method!(NativeServer::set_compression, 1))?;
    class.define_method("set_reflection", method!(NativeServer::set_reflection, 1))?;
    class.define_method("set_routes", method!(NativeServer::set_routes, 1))?;
    class.define_method(
        "set_reflection_descriptors",
        method!(NativeServer::set_reflection_descriptors, 1),
    )?;
    class.define_method("start_server", method!(NativeServer::start_server, 0))?;
    class.define_method("serve_worker", method!(NativeServer::serve_worker, 0))?;
    class.define_method("set_health", method!(NativeServer::set_health, 2))?;
    class.define_method("begin_shutdown", method!(NativeServer::begin_shutdown, 0))?;
    class.define_method("wait_for_drain", method!(NativeServer::wait_for_drain, 1))?;
    class.define_method("stop_workers", method!(NativeServer::stop_workers, 0))?;
    class.define_method("stop_server", method!(NativeServer::stop_server, 0))?;
    class.define_method("next_log_events", method!(NativeServer::next_log_events, 1))?;
    class.define_method("running?", method!(NativeServer::is_running, 0))?;
    class.define_method("stats", method!(NativeServer::stats, 0))?;

    let sender = native.define_class("StreamSender", ruby.class_object())?;
    sender.undef_default_alloc_func();
    sender.define_method("push", method!(StreamSender::push, 1))?;
    sender.define_method("send_headers", method!(StreamSender::send_headers, 1))?;
    sender.define_method("add_trailers", method!(StreamSender::add_trailers, 1))?;

    let reader = native.define_class("RequestReader", ruby.class_object())?;
    reader.undef_default_alloc_func();
    reader.define_method("next", method!(RequestReader::next, 0))?;
    reader.define_method("error", method!(RequestReader::error, 0))?;

    Ok(())
}
