use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use tracing::Span;

pub struct TracedStream<S> {
    stream: S,
    _span: Span,
}

impl<S> TracedStream<S> {
    pub fn new(stream: S, span: Span) -> Self {
        Self {
            stream,
            _span: span,
        }
    }
}

impl<S> Stream for TracedStream<S>
where S: Stream + Unpin
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let _entered = this._span.enter();
        Pin::new(&mut this.stream).poll_next(cx)
    }
}
