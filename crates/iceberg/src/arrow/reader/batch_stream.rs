// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Bounded delivery of batches decoded on the CPU runtime.

use futures::StreamExt;

use crate::Result;
use crate::runtime::RuntimeHandle;
use crate::scan::ArrowRecordBatchStream;

pub(super) fn decode_on_cpu(
    mut stream: ArrowRecordBatchStream,
    cpu: &RuntimeHandle,
) -> ArrowRecordBatchStream {
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let task = cpu
        .spawn(async move {
            loop {
                // Reserve before polling Parquet: a slow consumer allows at most
                // one decoded batch ahead, without decoding another while full.
                let Ok(permit) = tx.reserve().await else {
                    return Ok(());
                };
                match stream.next().await {
                    Some(batch) => {
                        permit.send(batch?);
                    }
                    None => return Ok(()),
                }
            }
        })
        .abort_on_drop();

    // Retain the task with the receiver so dropping the stream also cancels a
    // pending storage read. Await it at EOF to surface decoding errors, panics,
    // and runtime shutdown instead of silently truncating the scan.
    Box::pin(futures::stream::try_unfold(
        (rx, task),
        |(mut rx, task)| async move {
            match rx.recv().await {
                Some(batch) => Ok(Some((batch, (rx, task)))),
                None => {
                    let result: Result<()> = task.await?;
                    result?;
                    Ok(None)
                }
            }
        },
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow_array::{Int32Array, RecordBatch};
    use futures::{StreamExt, TryStreamExt};

    use super::*;
    use crate::{Error, ErrorKind, Runtime};

    fn batch() -> RecordBatch {
        RecordBatch::try_from_iter(vec![("id", Arc::new(Int32Array::from(vec![1])) as _)]).unwrap()
    }

    #[test]
    fn polling_runs_on_cpu_even_when_consumer_runs_on_io() {
        let io = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("consumer-io")
            .enable_all()
            .build()
            .unwrap();
        let cpu = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("decode-cpu")
            .enable_all()
            .build()
            .unwrap();
        let runtime = Runtime::new_with_split(&io, &cpu);
        let source = futures::stream::iter(vec![Ok(batch()), Ok(batch())]).inspect(|_| {
            assert_eq!(std::thread::current().name(), Some("decode-cpu"));
        });
        let stream = decode_on_cpu(Box::pin(source), runtime.cpu());
        let batches = io
            .block_on(
                runtime
                    .io()
                    .spawn(async move { stream.try_collect::<Vec<_>>().await.unwrap() }),
            )
            .unwrap();
        assert_eq!(batches.len(), 2);
    }

    #[tokio::test]
    async fn slow_consumer_bounds_decoding_and_drop_cancels_producer() {
        let polls = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(tokio::sync::Notify::new());
        struct OnDrop(Arc<tokio::sync::Notify>);
        impl Drop for OnDrop {
            fn drop(&mut self) {
                self.0.notify_one();
            }
        }
        let guard = OnDrop(dropped.clone());
        let observed = polls.clone();
        let source = futures::stream::poll_fn(move |_| {
            let _keep_alive = &guard;
            observed.fetch_add(1, Ordering::SeqCst);
            std::task::Poll::Ready(Some(Ok(batch())))
        });
        let mut stream = decode_on_cpu(Box::pin(source), Runtime::current().cpu());
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        stream.next().await.unwrap().unwrap();
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert_eq!(polls.load(Ordering::SeqCst), 2);
        drop(stream);
        tokio::time::timeout(std::time::Duration::from_secs(5), dropped.notified())
            .await
            .unwrap();
        assert_eq!(polls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn decode_error_follows_already_buffered_batches() {
        let source = futures::stream::iter(vec![
            Ok(batch()),
            Err(Error::new(ErrorKind::DataInvalid, "bad page")),
        ]);
        let mut stream = decode_on_cpu(Box::pin(source), Runtime::current().cpu());
        assert_eq!(stream.next().await.unwrap().unwrap().num_rows(), 1);
        assert_eq!(
            stream.next().await.unwrap().unwrap_err().kind(),
            ErrorKind::DataInvalid
        );
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn producer_panic_is_not_reported_as_eof() {
        let source =
            futures::stream::poll_fn(|_| -> std::task::Poll<Option<Result<RecordBatch>>> {
                panic!("decoder panic");
            });
        let mut stream = decode_on_cpu(Box::pin(source), Runtime::current().cpu());
        let error = stream.next().await.unwrap().unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Unexpected);
        assert!(error.to_string().contains("decoder panic"));
    }

    #[tokio::test]
    async fn dropping_stream_cancels_pending_decode() {
        let started = Arc::new(tokio::sync::Notify::new());
        let dropped = Arc::new(tokio::sync::Notify::new());
        struct OnDrop(Arc<tokio::sync::Notify>);
        impl Drop for OnDrop {
            fn drop(&mut self) {
                self.0.notify_one();
            }
        }
        let guard = OnDrop(dropped.clone());
        let signal = started.clone();
        let source =
            futures::stream::poll_fn(move |_| -> std::task::Poll<Option<Result<RecordBatch>>> {
                let _keep_alive = &guard;
                signal.notify_one();
                std::task::Poll::Pending
            });
        let stream = decode_on_cpu(Box::pin(source), Runtime::current().cpu());
        started.notified().await;
        drop(stream);
        tokio::time::timeout(std::time::Duration::from_secs(5), dropped.notified())
            .await
            .unwrap();
    }
}
