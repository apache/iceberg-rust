use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use tracing::Span;

pub struct TracedStream<S> {
    stream: S,
    _spans: Vec<Span>,
}

impl<S> TracedStream<S> {
    pub fn new(stream: S, spans: Vec<Span>) -> Self {
        Self {
            stream,
            _spans: spans,
        }
    }
}

impl<S> Stream for TracedStream<S>
where S: Stream + Unpin
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let _entered = this
            ._spans
            .iter()
            .map(|span| span.enter())
            .collect::<Vec<_>>();
        Pin::new(&mut this.stream).poll_next(cx)
    }
}
