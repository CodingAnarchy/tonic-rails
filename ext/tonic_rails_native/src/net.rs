//! Listener setup and the global connection cap.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use socket2::{Domain, Protocol, Socket, Type};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_stream::Stream;
use tonic::transport::server::{Connected, TcpConnectInfo};

use crate::log::{Level, LogSink};

/// A stream of accepted connections.
pub type Accept = Pin<Box<dyn Stream<Item = std::io::Result<TcpStream>> + Send>>;

/// Bind `addr` and accept on it.  Must be called inside a tokio runtime
/// context.
pub fn listen(addr: SocketAddr, backlog: i32) -> std::io::Result<Accept> {
    let listener = tokio::net::TcpListener::from_std(bind(addr, backlog)?)?;
    Ok(Box::pin(tokio_stream::wrappers::TcpListenerStream::new(listener)))
}

/// Accept on a listening socket inherited from a parent process, shared
/// with sibling workers (multi-process mode).  The fd is duplicated, so
/// the caller keeps ownership of the original.  `active` is this
/// process's open connection count.  Must be called inside a tokio
/// runtime context.
#[cfg(unix)]
pub fn listen_inherited(fd: i32, active: Arc<AtomicUsize>) -> std::io::Result<Accept> {
    Ok(Box::pin(SharedAccept::new(inherited(fd)?, active)?))
}

#[cfg(not(unix))]
pub fn listen_inherited(_fd: i32, _active: Arc<AtomicUsize>) -> std::io::Result<Accept> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "inherited listeners need fork(), which this platform lacks",
    ))
}

/// Bind with SO_REUSEADDR + SO_REUSEPORT so restarts don't fail on
/// TIME_WAIT.  The listener is non-blocking, ready for tokio.
pub fn bind(addr: SocketAddr, backlog: i32) -> std::io::Result<std::net::TcpListener> {
    let domain = if addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    #[cfg(all(
        unix,
        not(any(target_os = "solaris", target_os = "illumos", target_os = "cygwin"))
    ))]
    socket.set_reuse_port(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    socket.listen(backlog)?;
    Ok(socket.into())
}

#[cfg(unix)]
fn inherited(fd: i32) -> std::io::Result<std::net::TcpListener> {
    use std::os::fd::BorrowedFd;

    // SAFETY: the caller guarantees `fd` is open for the duration of
    // this call; we only duplicate it.
    let owned = unsafe { BorrowedFd::borrow_raw(fd) }.try_clone_to_owned()?;
    let socket = Socket::from(owned);
    socket.set_nonblocking(true)?;
    Ok(socket.into())
}

/// Accepts on a listening socket shared with sibling worker processes.
///
/// Every process is woken for each new connection and the first to
/// accept wins, so gRPC's few long-lived connections tend to pile onto
/// whichever process wakes fastest.  Instead, when a connection is
/// waiting, a process holds back for `backoff` per connection it already
/// has (up to `max_backoff`) before trying to accept, so the least busy
/// process usually gets it, as with Puma's `wait_for_less_busy_worker`.
#[cfg(unix)]
struct SharedAccept {
    listener: tokio::io::unix::AsyncFd<std::net::TcpListener>,
    active: Arc<AtomicUsize>,
    backoff: Duration,
    max_backoff: Duration,
    pause: Option<Pin<Box<tokio::time::Sleep>>>,
    /// The pause for the waiting connection is over: accept right away.
    paused: bool,
}

#[cfg(unix)]
impl SharedAccept {
    fn new(listener: std::net::TcpListener, active: Arc<AtomicUsize>) -> std::io::Result<Self> {
        Ok(Self {
            listener: tokio::io::unix::AsyncFd::new(listener)?,
            active,
            backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(10),
            pause: None,
            paused: false,
        })
    }
}

#[cfg(unix)]
impl Stream for SharedAccept {
    type Item = std::io::Result<TcpStream>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        loop {
            if let Some(pause) = this.pause.as_mut() {
                if pause.as_mut().poll(cx).is_pending() {
                    return Poll::Pending;
                }
                this.pause = None;
                this.paused = true;
            }
            let mut guard = match this.listener.poll_read_ready(cx) {
                Poll::Ready(Ok(guard)) => guard,
                Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
                Poll::Pending => return Poll::Pending,
            };
            let active = this.active.load(Ordering::Relaxed);
            if !this.paused && active > 0 {
                let pause = this.backoff.saturating_mul(active as u32).min(this.max_backoff);
                this.pause = Some(Box::pin(tokio::time::sleep(pause)));
                continue;
            }
            // Whether we accept or a sibling beat us to it, the next
            // connection gets a fresh pause.
            this.paused = false;
            match guard.try_io(|listener| listener.get_ref().accept()) {
                Ok(Ok((stream, _))) => {
                    return Poll::Ready(Some(
                        stream
                            .set_nonblocking(true)
                            .and_then(|()| TcpStream::from_std(stream)),
                    ));
                }
                Ok(Err(e)) => return Poll::Ready(Some(Err(e))),
                Err(_would_block) => continue,
            }
        }
    }
}

/// TCP_NODELAY for latency; TCP keepalive to reap half-open peers that
/// never answer HTTP/2 PINGs (e.g. behind a NAT that dropped state).
fn configure_accepted(stream: &TcpStream) {
    let _ = stream.set_nodelay(true);
    let keepalive = socket2::TcpKeepalive::new().with_time(Duration::from_secs(60));
    let _ = socket2::SockRef::from(stream).set_tcp_keepalive(&keepalive);
}

/// A connection holding a slot in the global connection cap.
pub struct LimitedConn {
    inner: TcpStream,
    _permit: OwnedSemaphorePermit,
    active: Arc<AtomicUsize>,
}

impl Drop for LimitedConn {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::Relaxed);
    }
}

impl AsyncRead for LimitedConn {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for LimitedConn {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl Connected for LimitedConn {
    type ConnectInfo = TcpConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.inner.connect_info()
    }
}

/// Accept stream that closes connections beyond the cap immediately.
pub struct LimitedIncoming<S> {
    pub inner: S,
    pub semaphore: Arc<Semaphore>,
    pub active: Arc<AtomicUsize>,
    pub log: LogSink,
}

impl<S> Stream for LimitedIncoming<S>
where
    S: Stream<Item = std::io::Result<TcpStream>> + Unpin,
{
    type Item = std::io::Result<LimitedConn>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let stream = match Pin::new(&mut self.inner).poll_next(cx) {
                Poll::Ready(Some(Ok(stream))) => stream,
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            };
            match self.semaphore.clone().try_acquire_owned() {
                Ok(permit) => {
                    configure_accepted(&stream);
                    self.active.fetch_add(1, Ordering::Relaxed);
                    return Poll::Ready(Some(Ok(LimitedConn {
                        inner: stream,
                        _permit: permit,
                        active: self.active.clone(),
                    })));
                }
                Err(_) => {
                    let active = self.active.load(Ordering::Relaxed);
                    self.log.log(
                        Level::Warn,
                        &format!("connection refused: max_connections reached ({active} active)"),
                    );
                }
            }
        }
    }
}
