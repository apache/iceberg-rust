use crate::runtime::JoinHandler;
use std::future::Future;

pub fn spawn<F>(future: F) -> JoinHandler<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    JoinHandler {
        inner: tokio::spawn(future),
    }
}
