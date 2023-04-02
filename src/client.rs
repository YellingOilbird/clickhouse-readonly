use std::fmt;

use crate::{
    block::Block,
    error::{Error, Result},
    pool::{Pool, PoolBinding, PoolConfig},
    query::{block_stream::BlockStream, *},
    stream::ConnectingStream,
    transport::ClickhouseTransport,
    types::{Cmd, Packet, ServerInfo},
};
use futures_core::{future::BoxFuture, stream::BoxStream};
use futures_util::{FutureExt, StreamExt};
use log::{info, warn};

/// Retry guard max attempts
const MAX_RETRY_ATTEMTS: usize = 3;
/// Retry guard timeout between attempts
const RETRY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

pub struct Client {
    _private: std::marker::PhantomData<()>,
}

#[derive(Clone)]
pub(crate) struct Context {
    pub(crate) server_info: ServerInfo,
    pub(crate) hostname: String,
    pub(crate) config: PoolConfig,
}

impl Default for Context {
    fn default() -> Self {
        Self {
            server_info: ServerInfo::default(),
            hostname: hostname::get().unwrap().into_string().unwrap(),
            config: PoolConfig::default(),
        }
    }
}

/// Clickhouse client handle.
pub struct ClientHandle {
    pub(crate) inner: Option<ClickhouseTransport>,
    pub(crate) context: Context,
    pub(crate) pool: PoolBinding,
}

impl fmt::Debug for ClientHandle {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ClientHandle")
            .field("server_info", &self.context.server_info)
            .finish()
    }
}

impl Client {
    pub(crate) async fn open(config: PoolConfig, pool: Option<Pool>) -> Result<ClientHandle> {
        let timeout = config.connection_timeout;

        let context = Context {
            config: config.clone(),
            ..Default::default()
        };

        crate::util::with_timeout(
            async move {
                let addr = match &pool {
                    None => &config.addr,
                    Some(p) => p.get_addr(),
                };

                info!("try to connect to {}", addr);
                if addr.port() == Some(8123) {
                    warn!("You should use port 9000 instead of 8123 because clickhouse-rs work through the binary interface.");
                }
                let mut stream = ConnectingStream::new(addr, &config).await?;
                stream.set_nodelay(true)?;

                let transport = ClickhouseTransport::new(stream, pool.clone());

                let mut handle = ClientHandle {
                    inner: Some(transport),
                    pool: match pool {
                        None => PoolBinding::None,
                        Some(p) => PoolBinding::Detached(p),
                    },
                    context
                };

                handle.hello().await?;
                Ok(handle)
            },
            timeout,
        )
        .await
    }
}

impl ClientHandle {
    async fn hello(&mut self) -> Result<()> {
        let context = self.context.clone();
        info!(
            "[hello] -> \n{:?}\n{:?}\n{:?}\n{:?}\n{:?}",
            &context.hostname,
            &context.server_info,
            &context.config.addr.host(),
            &context.config.database,
            &context.config.username,
        );

        let mut h = None;
        let mut info = None;
        let mut stream = self.inner.take().unwrap().call(Cmd::Hello(context.clone()));

        while let Some(packet) = stream.next().await {
            match packet {
                Ok(Packet::Hello(inner, server_info)) => {
                    info!("[hello] <- {:?}", &server_info);
                    h = Some(inner);
                    info = Some(server_info);
                }
                Ok(Packet::Exception(e)) => return Err(Error::Server(e)),
                Err(e) => return Err(Error::IO(e)),
                _ => {}
            }
        }

        self.inner = h;
        self.context.server_info = info.unwrap();
        Ok(())
    }

    async fn ping(&mut self) -> Result<()> {
        let timeout = std::time::Duration::from_secs(1);

        crate::util::with_timeout(
            async move {
                info!("[ping]");

                let mut h = None;

                let transport = self.inner.take().unwrap().clear().await?;
                let mut stream = transport.call(Cmd::Ping);

                while let Some(packet) = stream.next().await {
                    match packet {
                        Ok(Packet::Pong(inner)) => {
                            info!("[pong]");
                            h = Some(inner);
                        }
                        Ok(Packet::Exception(e)) => return Err(Error::Server(e)),
                        Err(e) => return Err(Error::IO(e)),
                        _ => {}
                    }
                }

                self.inner = h;
                Ok(())
            },
            timeout,
        )
        .await
    }

    /// Executes Clickhouse `query` on Conn.
    pub fn query<Q>(&mut self, sql: Q) -> QueryResult
    where
        Query: From<Q>,
    {
        let query = Query::from(sql);
        QueryResult {
            client: self,
            query,
        }
    }

    pub(crate) fn wrap_stream<'a, F>(&'a mut self, f: F) -> BoxStream<'a, Result<Block>>
    where
        F: (FnOnce(&'a mut Self) -> BlockStream<'a>) + Send + 'static,
    {
        let fut: BoxFuture<'a, BoxStream<'a, Result<Block>>> = Box::pin(async move {
            let inner: BoxStream<'a, Result<Block>> = match self.check_connection().await {
                Ok(_) => Box::pin(f(self)),
                Err(err) => Box::pin(futures_util::stream::once(futures_util::future::err(err))),
            };
            inner
        });

        Box::pin(fut.flatten_stream())
    }

    /// Check connection and try to reconnect if necessary.
    async fn check_connection(&mut self) -> Result<()> {
        self.pool.detach();

        let source = self.context.config.clone();
        let pool = self.pool.clone();

        retry(self, &source, pool.into()).await?;

        if !self.pool.is_attached() && self.pool.is_some() {
            self.pool.attach();
        }

        Ok(())
    }

    /// Switch Transport AtomicUsize status on takeing/returning connection
    pub(crate) fn set_inside(&self, value: bool) {
        if let Some(ref inner) = self.inner {
            inner.set_inside(value);
        } else {
            unreachable!()
        }
    }
}

pub(crate) async fn retry(
    handle: &mut ClientHandle,
    source: &PoolConfig,
    pool: Option<Pool>,
) -> Result<()> {
    let mut attempt = 0;
    let mut skip_check = false;

    loop {
        if skip_check {
            skip_check = false;
        } else {
            match handle.ping().await {
                Ok(()) => return Ok(()),
                Err(err) => {
                    if attempt >= MAX_RETRY_ATTEMTS {
                        return Err(err);
                    }
                }
            }
        }

        match reconnect(handle, source, pool.clone()).await {
            Ok(()) => continue,
            Err(err) => {
                skip_check = true;
                if attempt >= MAX_RETRY_ATTEMTS {
                    return Err(err);
                }

                tokio::time::sleep(RETRY_TIMEOUT).await;
            }
        }

        attempt += 1;
    }
}

async fn reconnect(conn: &mut ClientHandle, source: &PoolConfig, pool: Option<Pool>) -> Result<()> {
    warn!("[reconnect]");
    let mut new_conn = match pool {
        None => Client::open(source.clone(), pool).await?,
        Some(p) => p.get_handle().await?,
    };
    std::mem::swap(conn, &mut new_conn);
    Ok(())
}
