use std::future::Future;
use std::marker::{PhantomData, Unpin};
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

pub trait JoinHandleExt {
    type Output;
    fn poll_join(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output>;
}

pub struct JoinHandle<T, J> {
    inner: J,
    _marker: PhantomData<T>,
}

impl<T, J> Future for JoinHandle<T, J>
where
    T: Send + 'static + Unpin,
    J: JoinHandleExt<Output = T> + Unpin,
{
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.inner).poll_join(cx)
    }
}
