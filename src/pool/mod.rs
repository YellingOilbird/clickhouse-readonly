use std::{
    fmt, mem,
    pin::Pin,
    sync::atomic::{self, Ordering},
    sync::Arc,
    task::{Context, Poll, Waker},
    time::Duration,
};

use futures_util::future::BoxFuture;

use crate::{
    client::{Client, ClientHandle},
    error::Result,
};

pub use self::futures::GetHandle;
use futures_util::FutureExt;
use url::Url;

mod futures;

/// Default connection timeout
const CONN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
/// Default connection timeout
const QUERY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct PoolConfig {
    pub(crate) addr: Url,
    pub(crate) database: String,
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) connection_timeout: Option<Duration>,
    pub(crate) query_timeout: Option<Duration>,
    pub(crate) secure: bool,
}

pub struct PoolConfigBuilder(PoolConfig);

impl PoolConfigBuilder {
    pub fn new(
        addr: Url,
        database: String,
        username: String,
        password: String,
        secure: bool,
    ) -> Self {
        Self(PoolConfig {
            addr,
            database,
            username,
            password,
            connection_timeout: None,
            query_timeout: None,
            secure,
        })
    }

    pub fn with_connection_timeout(mut self, timeout: Duration) -> Self {
        self.0.connection_timeout = Some(timeout);
        self
    }

    pub fn with_query_timeout(mut self, timeout: Duration) -> Self {
        self.0.query_timeout = Some(timeout);
        self
    }

    pub fn build(mut self) -> PoolConfig {
        if self.0.connection_timeout.is_none() {
            self.0.connection_timeout = Some(CONN_TIMEOUT)
        }

        if self.0.query_timeout.is_none() {
            self.0.query_timeout = Some(QUERY_TIMEOUT)
        }

        self.0
    }
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            addr: Url::parse("tcp://127.0.0.1:9000").unwrap(),
            database: "default".to_string(),
            username: Default::default(),
            password: Default::default(),
            connection_timeout: Some(CONN_TIMEOUT),
            query_timeout: Some(QUERY_TIMEOUT),
            secure: false,
        }
    }
}

#[derive(Debug)]
pub(crate) struct Inner {
    new: crossbeam::queue::ArrayQueue<BoxFuture<'static, Result<ClientHandle>>>,
    idle: crossbeam::queue::ArrayQueue<ClientHandle>,
    tasks: crossbeam::queue::SegQueue<Waker>,
    ongoing: atomic::AtomicUsize,
    hosts: Vec<Url>,
    connections_num: atomic::AtomicUsize,
}

impl Inner {
    pub(crate) fn release_conn(&self) {
        self.ongoing.fetch_sub(1, Ordering::AcqRel);
        while let Some(task) = self.tasks.pop() {
            task.wake()
        }
    }

    fn conn_count(&self) -> usize {
        let is_new_some = self.new.len();
        let ongoing = self.ongoing.load(Ordering::Acquire);
        let idle_count = self.idle.len();
        is_new_some + idle_count + ongoing
    }
}

#[derive(Clone)]
pub(crate) enum PoolBinding {
    None,
    Attached(Pool),
    Detached(Pool),
}

impl From<PoolBinding> for Option<Pool> {
    fn from(binding: PoolBinding) -> Self {
        match binding {
            PoolBinding::None => None,
            PoolBinding::Attached(pool) | PoolBinding::Detached(pool) => Some(pool),
        }
    }
}

impl PoolBinding {
    pub(crate) fn take(&mut self) -> Self {
        mem::replace(self, PoolBinding::None)
    }

    fn return_conn(self, client: ClientHandle) {
        if let Some(mut pool) = self.into() {
            Pool::return_conn(&mut pool, client);
        }
    }

    pub(crate) fn is_attached(&self) -> bool {
        matches!(self, PoolBinding::Attached(_))
    }

    pub(crate) fn is_some(&self) -> bool {
        !matches!(self, PoolBinding::None)
    }

    pub(crate) fn attach(&mut self) {
        match self.take() {
            PoolBinding::Detached(pool) => *self = PoolBinding::Attached(pool),
            _ => unreachable!(),
        }
    }

    pub(crate) fn detach(&mut self) {
        match self.take() {
            PoolBinding::Attached(pool) => *self = PoolBinding::Detached(pool),
            _ => unreachable!(),
        }
    }
}

/// Asynchronous pool of Clickhouse connections.
#[derive(Clone)]
pub struct Pool {
    pub(crate) config: PoolConfig,
    pub(crate) inner: Arc<Inner>,
    min: usize,
    max: usize,
}

#[derive(Debug)]
struct PoolInfo {
    new_len: usize,
    idle_len: usize,
    tasks_len: usize,
    ongoing: usize,
}

impl fmt::Debug for Pool {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let info = self.info();
        f.debug_struct("Pool")
            .field("min", &self.min)
            .field("max", &self.max)
            .field("new connections count", &info.new_len)
            .field("idle connections count", &info.idle_len)
            .field("tasks count", &info.tasks_len)
            .field("ongoing connections count", &info.ongoing)
            .finish()
    }
}

impl Pool {
    /// Constructs a new Pool.
    pub fn new(config: PoolConfig) -> Self {
        let min = 5;
        let max = 10;
        let hosts = vec![config.addr.clone()];

        let inner = Arc::new(Inner {
            new: crossbeam::queue::ArrayQueue::new(1),
            idle: crossbeam::queue::ArrayQueue::new(max),
            tasks: crossbeam::queue::SegQueue::new(),
            ongoing: atomic::AtomicUsize::new(0),
            connections_num: atomic::AtomicUsize::new(0),
            hosts,
        });

        Self {
            config,
            inner,
            min,
            max,
        }
    }

    fn info(&self) -> PoolInfo {
        PoolInfo {
            new_len: self.inner.new.len(),
            idle_len: self.inner.idle.len(),
            tasks_len: self.inner.tasks.len(),
            ongoing: self.inner.ongoing.load(Ordering::Acquire),
        }
    }

    /// Returns future that resolves to `ClientHandle`.
    pub fn get_handle(&self) -> GetHandle {
        GetHandle::new(self)
    }

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<ClientHandle>> {
        self.handle_futures(cx)?;

        match self.take_conn() {
            Some(client) => Poll::Ready(Ok(client)),
            None => {
                let new_conn_created = {
                    let conn_count = self.inner.conn_count();

                    if conn_count < self.max && self.inner.new.push(self.new_connection()).is_ok() {
                        true
                    } else {
                        self.inner.tasks.push(cx.waker().clone());
                        false
                    }
                };
                if new_conn_created {
                    self.poll(cx)
                } else {
                    Poll::Pending
                }
            }
        }
    }

    fn new_connection(&self) -> BoxFuture<'static, Result<ClientHandle>> {
        let source = self.config.clone();
        let pool = Some(self.clone());
        Box::pin(async move { Client::open(source, pool).await })
    }

    fn handle_futures(&mut self, cx: &mut Context<'_>) -> Result<()> {
        if let Some(mut new) = self.inner.new.pop() {
            match new.poll_unpin(cx) {
                Poll::Ready(Ok(client)) => {
                    self.inner.idle.push(client).unwrap();
                }
                Poll::Pending => {
                    // NOTE: it is okay to drop the construction task
                    // because another construction will be attempted
                    // later in Pool::poll
                    let _ = self.inner.new.push(new);
                }
                Poll::Ready(Err(err)) => {
                    return Err(err);
                }
            }
        }

        Ok(())
    }

    fn take_conn(&mut self) -> Option<ClientHandle> {
        if let Some(mut client) = self.inner.idle.pop() {
            client.pool = PoolBinding::Attached(self.clone());
            client.set_inside(false);
            self.inner.ongoing.fetch_add(1, Ordering::AcqRel);
            Some(client)
        } else {
            None
        }
    }

    fn return_conn(&mut self, mut client: ClientHandle) {
        let min = self.min;

        let is_attached = client.pool.is_attached();
        client.pool = PoolBinding::None;
        client.set_inside(true);

        if self.inner.idle.len() < min && is_attached {
            let _ = self.inner.idle.push(client);
        }
        self.inner.ongoing.fetch_sub(1, Ordering::AcqRel);

        while let Some(task) = self.inner.tasks.pop() {
            task.wake()
        }
    }

    pub(crate) fn get_addr(&self) -> &Url {
        let n = self.inner.hosts.len();
        let index = self.inner.connections_num.fetch_add(1, Ordering::SeqCst);
        &self.inner.hosts[index % n]
    }
}

impl Drop for ClientHandle {
    fn drop(&mut self) {
        if let (pool, Some(inner)) = (self.pool.take(), self.inner.take()) {
            if !pool.is_some() {
                return;
            }

            let context = self.context.clone();
            let client = Self {
                inner: Some(inner),
                pool: pool.clone(),
                context,
            };
            pool.return_conn(client);
        }
    }
}
