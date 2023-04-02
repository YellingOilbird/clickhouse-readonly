use pin_project::pin_project;
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_native_tls::TlsStream;

#[derive(Debug)]
#[pin_project(project = StreamProj)]
pub(crate) enum InnerStream<S> {
    Plain(#[pin] S),
    Secure(#[pin] TlsStream<S>),
}

impl InnerStream<tokio::net::TcpStream> {
    pub(crate) fn set_nodelay(&mut self, nodelay: bool) -> io::Result<()> {
        match *self {
            Self::Plain(ref mut stream) => stream.set_nodelay(nodelay),
            Self::Secure(ref mut stream) => {
                stream.get_mut().get_mut().get_mut().set_nodelay(nodelay)
            }
        }
        .map_err(|err| io::Error::new(err.kind(), format!("set_nodelay error: {}", err)))
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for InnerStream<S>
where
    TlsStream<S>: AsyncRead + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let result = match self.project() {
            StreamProj::Plain(stream) => stream.poll_read(cx, buf),
            StreamProj::Secure(stream) => stream.poll_read(cx, buf),
        };

        match result {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
            Poll::Ready(Err(x)) => Poll::Ready(Err(x)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for InnerStream<S>
where
    TlsStream<S>: AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.project() {
            StreamProj::Plain(stream) => stream.poll_write(cx, buf),
            StreamProj::Secure(stream) => stream.poll_write(cx, buf),
        }
    }

    #[allow(unused)]
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        match self.project() {
            StreamProj::Plain(ref mut s) => Pin::new(s).poll_flush(cx),
            StreamProj::Secure(ref mut s) => Pin::new(s).poll_flush(cx),
        }
    }

    #[allow(unused)]
    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match self.project() {
            StreamProj::Plain(ref mut s) => Pin::new(s).poll_shutdown(cx),
            StreamProj::Secure(ref mut s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}
