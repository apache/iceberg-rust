use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use arrow_array::RecordBatch;
use futures::Stream;

use crate::Result;

pub(crate) struct CountRecordingRecordBatchStream<S> {
    stream: S,
    row_count: AtomicU64,
    record_batch_count: AtomicU64,
    target_span: tracing::Span,
    record_batch_count_field_name: &'static str,
    row_count_field_name: &'static str,
}

impl<S> CountRecordingRecordBatchStream<S> {
    pub(crate) fn new(
        stream: S,
        target_span: tracing::Span,
        record_batch_count_field_name: &'static str,
        row_count_field_name: &'static str,
    ) -> Self {
        Self {
            stream,
            row_count: AtomicU64::new(0),
            record_batch_count: AtomicU64::new(0),
            target_span,
            record_batch_count_field_name,
            row_count_field_name,
        }
    }
}

impl<S> Stream for CountRecordingRecordBatchStream<S>
where S: Stream<Item = Result<RecordBatch>> + Unpin
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        match Pin::new(&mut this.stream).poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let row_count = batch.num_rows() as u64;

                this.row_count.fetch_add(row_count, Ordering::Relaxed);
                this.record_batch_count.fetch_add(1, Ordering::Relaxed);

                Poll::Ready(Some(Ok(batch)))
            }
            other => other,
        }
    }
}

impl<S> Drop for CountRecordingRecordBatchStream<S> {
    fn drop(&mut self) {
        let total_record_batches = self.record_batch_count.load(Ordering::Relaxed);
        let total_rows = self.row_count.load(Ordering::Relaxed);
        self.target_span
            .record(self.record_batch_count_field_name, total_record_batches);
        self.target_span
            .record(self.row_count_field_name, total_rows);
    }
}
