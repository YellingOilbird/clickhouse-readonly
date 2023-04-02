use std::{
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
};

use futures_util::future::BoxFuture;
use futures_util::future::{select_ok, SelectOk, TryFutureExt};
use futures_util::FutureExt;

use tokio::net::TcpStream;
use tokio_native_tls::TlsStream;

use pin_project::pin_project;
use url::Url;

use crate::{
    error::{ConnectionError, ConnectionResult},
    inner_stream::InnerStream,
    pool::PoolConfig,
};

type ConnectingFuture<T> = BoxFuture<'static, ConnectionResult<T>>;

#[pin_project(project = TcpStateProj)]
enum TcpState {
    Wait(#[pin] SelectOk<ConnectingFuture<TcpStream>>),
    Fail(Option<ConnectionError>),
}

#[pin_project(project = TlsStateProj)]
enum TlsState {
    Wait(#[pin] ConnectingFuture<TlsStream<TcpStream>>),
    Fail(Option<ConnectionError>),
}

#[pin_project(project = StateProj)]
enum State {
    Tcp(#[pin] TcpState),
    Tls(#[pin] TlsState),
}

impl TcpState {
    fn poll(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<ConnectionResult<InnerStream<TcpStream>>> {
        match self.project() {
            TcpStateProj::Wait(inner) => match inner.poll(cx) {
                Poll::Ready(Ok((tcp, _))) => Poll::Ready(Ok(InnerStream::Plain(tcp))),
                Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                Poll::Pending => Poll::Pending,
            },
            TcpStateProj::Fail(ref mut err) => Poll::Ready(Err(err.take().unwrap())),
        }
    }
}

impl TlsState {
    fn poll(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<ConnectionResult<InnerStream<TcpStream>>> {
        match self.project() {
            TlsStateProj::Wait(ref mut inner) => match inner.poll_unpin(cx) {
                Poll::Ready(Ok(tls)) => Poll::Ready(Ok(InnerStream::Secure(tls))),
                Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                Poll::Pending => Poll::Pending,
            },
            TlsStateProj::Fail(ref mut err) => {
                let e = err.take().unwrap();
                Poll::Ready(Err(e))
            }
        }
    }
}

impl State {
    fn poll(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<ConnectionResult<InnerStream<TcpStream>>> {
        match self.project() {
            StateProj::Tcp(inner) => inner.poll(cx),
            StateProj::Tls(inner) => inner.poll(cx),
        }
    }
}

#[pin_project]
pub(crate) struct ConnectingStream {
    #[pin]
    state: State,
}

impl ConnectingStream {
    pub(crate) fn new(addr: &Url, config: &PoolConfig) -> Self {
        match addr.socket_addrs(|| None) {
            Ok(addresses) => {
                let streams: Vec<_> = addresses
                    .iter()
                    .copied()
                    .map(|address| -> ConnectingFuture<TcpStream> {
                        Box::pin(TcpStream::connect(address).map_err(ConnectionError::IOError))
                    })
                    .collect();

                if streams.is_empty() {
                    let err = io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Could not resolve to any address.",
                    );
                    return Self {
                        state: State::Tcp(TcpState::Fail(Some(ConnectionError::IOError(err)))),
                    };
                }

                let socket = select_ok(streams);

                if config.secure {
                    return ConnectingStream::new_tls_connection(addr, socket);
                }

                Self {
                    state: State::Tcp(TcpState::Wait(socket)),
                }
            }
            Err(err) => Self {
                state: State::Tcp(TcpState::Fail(Some(ConnectionError::IOError(err)))),
            },
        }
    }

    fn new_tls_connection(addr: &Url, socket: SelectOk<ConnectingFuture<TcpStream>>) -> Self {
        match addr.host_str().map(|host| host.to_owned()) {
            None => Self {
                state: State::Tls(TlsState::Fail(Some(ConnectionError::TlsHostNotProvided))),
            },
            Some(host) => {
                let mut builder = native_tls::TlsConnector::builder();
                builder.danger_accept_invalid_certs(true);

                Self {
                    state: State::Tls(TlsState::Wait(Box::pin(async move {
                        let (stream, _) = socket.await?;

                        let connector: native_tls::TlsConnector = builder.build()?;
                        let tokio_connector = tokio_native_tls::TlsConnector::from(connector);

                        Ok(tokio_connector.connect(&host, stream).await?)
                    }))),
                }
            }
        }
    }
}

impl Future for ConnectingStream {
    type Output = ConnectionResult<InnerStream<TcpStream>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().state.poll(cx)
    }
}
