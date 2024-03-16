use crate::runtime::{JoinHandle, JoinHandleExt};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

pub fn spawn<F>(future: F) -> JoinHandle<F::Output, tokio::task::JoinHandle<F::Output>>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    JoinHandle {
        inner: tokio::spawn(future),
        _marker: Default::default(),
    }
}

impl<T> JoinHandleExt for tokio::task::JoinHandle<T> {
    type Output = T;
    fn poll_join(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.as_mut()
            .poll(cx)
            .map(|res| res.expect("tokio spawned task crashed"))
    }
}
