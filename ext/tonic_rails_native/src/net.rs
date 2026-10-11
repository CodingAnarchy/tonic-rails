//! Listener setup and the global connection cap.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use socket2::{Domain, Protocol, Socket, Type};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_stream::Stream;
use tonic::transport::server::{Connected, TcpConnectInfo};

use crate::log::{Level, LogSink};

/// Bind with SO_REUSEADDR + SO_REUSEPORT so several worker processes can
/// share a port (the kernel load-balances accepts) and restarts don't
/// fail on TIME_WAIT.  Must be called inside a tokio runtime context.
pub fn bind(addr: SocketAddr, backlog: i32) -> std::io::Result<TcpListener> {
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
    TcpListener::from_std(socket.into())
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
    pub accepted: Arc<AtomicUsize>,
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
                    self.accepted.fetch_add(1, Ordering::Relaxed);
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
