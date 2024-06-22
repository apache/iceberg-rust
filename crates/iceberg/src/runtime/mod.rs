use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

pub enum JoinHandle<T> {
    #[cfg(feature = "tokio")]
    Tokio(tokio::task::JoinHandle<T>),
    #[cfg(feature = "async-std")]
    AsyncStd(async_std::task::JoinHandle<T>),
}

impl<T: Send + 'static> Future for JoinHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut() {
            #[cfg(feature = "tokio")]
            JoinHandle::Tokio(handle) => Pin::new(handle)
                .poll(cx)
                .map(|h| h.expect("tokio spawned task failed")),
            #[cfg(feature = "async-std")]
            JoinHandle::AsyncStd(handle) => Pin::new(handle).poll(cx),
        }
    }
}

pub fn spawn<F>(f: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    #[cfg(feature = "tokio")]
    return JoinHandle::Tokio(tokio::task::spawn(f));

    #[cfg(feature = "async-std")]
    return JoinHandle::AsyncStd(async_std::task::spawn(f));
}

pub fn spawn_blocking<F, T>(f: F) -> JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    #[cfg(feature = "tokio")]
    return JoinHandle::Tokio(tokio::task::spawn_blocking(f));

    #[cfg(feature = "async-std")]
    return JoinHandle::AsyncStd(async_std::task::spawn_blocking(f));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn test_tokio_spawn() {
        let handle = spawn(async { 1 + 1 });
        assert_eq!(handle.await, 2);
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn test_tokio_spawn_blocking() {
        let handle = spawn_blocking(|| 1 + 1);
        assert_eq!(handle.await, 2);
    }

    #[cfg(feature = "async-std")]
    #[async_std::test]
    async fn test_async_std_spawn() {
        let handle = spawn(async { 1 + 1 });
        assert_eq!(handle.await, 2);
    }

    #[cfg(feature = "async-std")]
    #[async_std::test]
    async fn test_async_std_spawn_blocking() {
        let handle = spawn_blocking(|| 1 + 1);
        assert_eq!(handle.await, 2);
    }
}
