use pin_project::pin_project;
use std::task::{Context, Poll};
use std::{future::Future, pin::Pin};

use crate::{client::ClientHandle, error::Result, pool::Pool};

/// Future that resolves to a `ClientHandle`.
#[pin_project]
pub struct GetHandle {
    #[pin]
    pool: Pool,
}

impl GetHandle {
    pub(crate) fn new(pool: &Pool) -> Self {
        Self { pool: pool.clone() }
    }
}

impl Future for GetHandle {
    type Output = Result<ClientHandle>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().pool.poll(cx)
    }
}
