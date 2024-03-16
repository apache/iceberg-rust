use crate::runtime::{JoinHandle, JoinHandleExt};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

pub fn spawn<F>(future: F) -> JoinHandle<F::Output, async_std::task::JoinHandle<F::Output>>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    JoinHandle {
        inner: async_std::task::spawn(future),
        _marker: Default::default(),
    }
}

impl<T> JoinHandleExt for async_std::task::JoinHandle<T> {
    type Output = T;
    fn poll_join(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.as_mut().poll(cx)
    }
}
