use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

#[cfg(feature = "tokio")]
pub mod tokio_backend;
#[cfg(feature = "tokio")]
pub use tokio_backend::*;

#[cfg(all(feature = "async-std", not(feature = "tokio"),))]
pub mod async_std_backend;
#[cfg(all(feature = "async-std", not(feature = "tokio"),))]
pub use async_std_backend::*;

pub struct JoinHandler<T> {
    #[cfg(feature = "tokio")]
    inner: tokio::task::JoinHandle<T>,
    #[cfg(all(feature = "async-std", not(feature = "tokio"),))]
    inner: async_std::task::JoinHandle<T>,
}

impl<T> Future for JoinHandler<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        #[cfg(all(feature = "async-std", not(feature = "tokio")))]
        {
            Pin::new(&mut self.inner).poll(cx)
        }
        // Tokio returns a Poll<Result<..>>
        #[cfg(feature = "tokio")]
        Pin::new(&mut self.inner)
            .poll(cx)
            .map(|res| res.expect("tokio spawned task crashed"))
    }
}
