use std::marker::PhantomData;

use futures_core::stream::BoxStream;
use futures_util::StreamExt;

use crate::{
    block::{Block, BlockRef, Row, Rows},
    client::ClientHandle,
    column::Simple,
    error::Result,
    query::block_stream::BlockStream,
    types::Cmd,
};

#[derive(Clone, Debug)]
pub struct Query {
    sql: String,
    id: String,
}

impl Query {
    pub fn new(sql: impl AsRef<str>) -> Self {
        Self {
            sql: sql.as_ref().to_string(),
            id: "".to_string(),
        }
    }

    pub fn id(self, id: impl AsRef<str>) -> Self {
        Self {
            id: id.as_ref().to_string(),
            ..self
        }
    }

    pub(crate) fn get_sql(&self) -> &str {
        &self.sql
    }

    pub(crate) fn get_id(&self) -> &str {
        &self.id
    }
}

impl<T> From<T> for Query
where
    T: AsRef<str>,
{
    fn from(source: T) -> Self {
        Self::new(source)
    }
}

/// Result of a query or statement execution.
pub struct QueryResult<'a> {
    pub(crate) client: &'a mut ClientHandle,
    pub(crate) query: Query,
}

impl<'a> QueryResult<'a> {
    pub fn stream_blocks(self) -> BoxStream<'a, Result<Block>> {
        self._stream_blocks(true)
    }

    fn _stream_blocks(self, skip_first_block: bool) -> BoxStream<'a, Result<Block>> {
        let query = self.query.clone();

        self.client
            .wrap_stream::<'a, _>(move |c: &'a mut ClientHandle| {
                log::info!("[send query] {}", query.get_sql());
                c.pool.detach();

                let context = c.context.clone();

                let inner = c.inner.take().unwrap().call(Cmd::SendQuery(query, context));

                BlockStream::<'a>::new(c, inner, skip_first_block)
            })
    }

    pub fn stream(self) -> BoxStream<'a, Result<Row<'static, Simple>>> {
        Box::pin(
            self.stream_blocks()
                .map(|block_ret| {
                    let result: BoxStream<'a, Result<Row<'static, Simple>>> = match block_ret {
                        Ok(block) => {
                            let block = std::sync::Arc::new(block);
                            let block_ref = BlockRef::Owned(block);

                            Box::pin(
                                futures_util::stream::iter(Rows {
                                    row: 0,
                                    block_ref,
                                    kind: PhantomData,
                                })
                                .map(|row| -> Result<Row<'static, Simple>> { Ok(row) }),
                            )
                        }
                        Err(err) => {
                            Box::pin(futures_util::stream::once(futures_util::future::err(err)))
                        }
                    };
                    result
                })
                .flatten(),
        )
    }
}

pub mod block_stream {
    use std::{
        pin::Pin,
        task::{self, Poll},
    };

    use futures_core::Stream;
    use futures_util::StreamExt;

    use crate::{
        block::Block,
        client::ClientHandle,
        error::{DriverError, Error, Result},
        transport::PacketStream,
        types::Packet,
    };

    pub(crate) struct BlockStream<'a> {
        client: &'a mut ClientHandle,
        inner: PacketStream,
        eof: bool,
        block_index: usize,
        skip_first_block: bool,
    }

    impl<'a> Drop for BlockStream<'a> {
        fn drop(&mut self) {
            if !self.eof && !self.client.pool.is_attached() {
                self.client.pool.attach();
            }

            if self.client.inner.is_none() {
                // Takeing `PacketStream` inner transport
                if let Some(mut transport) = self.inner.take_transport() {
                    transport.inconsistent = true;
                    self.client.inner = Some(transport);
                }
            }
        }
    }

    impl<'a> BlockStream<'a> {
        pub(crate) fn new(
            client: &mut ClientHandle,
            inner: PacketStream,
            skip_first_block: bool,
        ) -> BlockStream {
            BlockStream {
                client,
                inner,
                eof: false,
                block_index: 0,
                skip_first_block,
            }
        }
    }

    impl<'a> Stream for BlockStream<'a> {
        type Item = Result<Block>;

        fn poll_next(
            mut self: Pin<&mut Self>,
            cx: &mut task::Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            loop {
                if self.eof {
                    return Poll::Ready(None);
                }

                let packet = match self.inner.poll_next_unpin(cx) {
                    Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err.into()))),
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(None) => {
                        self.eof = true;
                        continue;
                    }
                    Poll::Ready(Some(Ok(packet))) => packet,
                };

                match packet {
                    Packet::Eof(inner) => {
                        self.client.inner = Some(inner);
                        if !self.client.pool.is_attached() {
                            self.client.pool.attach();
                        }
                        self.eof = true;
                    }
                    Packet::ProfileInfo(_) | Packet::Progress(_) => {}
                    Packet::Exception(exception) => {
                        self.eof = true;
                        return Poll::Ready(Some(Err(exception.into())));
                    }
                    Packet::Block(block) => {
                        self.block_index += 1;
                        if (self.block_index > 1 || !self.skip_first_block) && !block.is_empty() {
                            return Poll::Ready(Some(Ok(block)));
                        }
                    }
                    _ => {
                        return Poll::Ready(Some(Err(Error::Driver(DriverError::UnexpectedPacket))))
                    }
                }
            }
        }
    }
}
