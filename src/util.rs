#[macro_export]
macro_rules! try_opt {
    ($expr:expr) => {
        match $expr {
            Ok(val) => val,
            Err(err) => return Err(err),
        }
    };
}

pub(crate) async fn with_timeout<F, T>(future: F, timeout: std::time::Duration) -> F::Output
where
    F: std::future::Future<Output = crate::error::Result<T>>,
{
    tokio::time::timeout(timeout, future).await?
}
