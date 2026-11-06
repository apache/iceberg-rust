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

//! Dispatch storage operations without moving Parquet decoding off its caller's runtime.

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;

use crate::Result;
use crate::io::{FileIO, FileRead};
use crate::runtime::RuntimeHandle;

pub(super) struct RuntimeFileRead {
    inner: Arc<dyn FileRead>,
    io: RuntimeHandle,
}

impl RuntimeFileRead {
    pub(super) async fn open(file_io: &FileIO, path: &str, io: &RuntimeHandle) -> Result<Self> {
        let file_io = file_io.clone();
        let path = path.to_owned();
        let inner = io
            .spawn(async move { file_io.new_input(&path)?.reader().await })
            .abort_on_drop()
            .await??;
        Ok(Self {
            inner: Arc::from(inner),
            io: io.clone(),
        })
    }
}

#[async_trait::async_trait]
impl FileRead for RuntimeFileRead {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        let inner = self.inner.clone();
        self.io
            .spawn(async move { inner.read(range).await })
            .abort_on_drop()
            .await?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Error, ErrorKind, Runtime};

    struct CheckedRead;

    #[async_trait::async_trait]
    impl FileRead for CheckedRead {
        async fn read(&self, range: Range<u64>) -> Result<Bytes> {
            assert_eq!(std::thread::current().name(), Some("parquet-io"));
            tokio::task::yield_now().await;
            assert_eq!(std::thread::current().name(), Some("parquet-io"));
            if range.is_empty() {
                return Err(Error::new(ErrorKind::DataInvalid, "empty test range"));
            }
            Ok(Bytes::from_static(b"data"))
        }
    }

    #[test]
    fn reads_run_on_io_and_errors_reach_cpu() {
        let io = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("parquet-io")
            .enable_all()
            .build()
            .unwrap();
        let cpu = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("parquet-cpu")
            .enable_all()
            .build()
            .unwrap();
        let runtime = Runtime::new_with_split(&io, &cpu);
        let reader = RuntimeFileRead {
            inner: Arc::new(CheckedRead),
            io: runtime.io().clone(),
        };
        cpu.block_on(runtime.cpu().spawn(async move {
            assert_eq!(
                reader.read(0..4).await.unwrap(),
                Bytes::from_static(b"data")
            );
            assert_eq!(std::thread::current().name(), Some("parquet-cpu"));
            assert_eq!(
                reader.read(0..0).await.unwrap_err().kind(),
                ErrorKind::DataInvalid
            );
        }))
        .unwrap();
    }

    struct PendingRead {
        started: tokio::sync::Notify,
        dropped: Arc<tokio::sync::Notify>,
    }

    #[async_trait::async_trait]
    impl FileRead for PendingRead {
        async fn read(&self, _range: Range<u64>) -> Result<Bytes> {
            struct OnDrop(Arc<tokio::sync::Notify>);
            impl Drop for OnDrop {
                fn drop(&mut self) {
                    self.0.notify_one();
                }
            }
            let _guard = OnDrop(self.dropped.clone());
            self.started.notify_one();
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn dropping_read_aborts_storage_request() {
        let dropped = Arc::new(tokio::sync::Notify::new());
        let inner = Arc::new(PendingRead {
            started: tokio::sync::Notify::new(),
            dropped: dropped.clone(),
        });
        let reader = RuntimeFileRead {
            inner: inner.clone(),
            io: Runtime::current().io().clone(),
        };
        let mut read = Box::pin(reader.read(0..4));
        assert!(futures::poll!(&mut read).is_pending());
        inner.started.notified().await;
        drop(read);
        tokio::time::timeout(std::time::Duration::from_secs(5), dropped.notified())
            .await
            .unwrap();
    }
}
