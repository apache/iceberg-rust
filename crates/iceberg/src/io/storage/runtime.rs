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

//! Runtime-aware storage adapter.
//!
//! `RuntimeStorage` is a private scheduling adapter around a real `Storage`
//! backend. It is not a storage backend itself: callers never configure it, and
//! it is intentionally not registered with `typetag`.
//!
//! Async storage operations are spawned onto the configured IO runtime. The
//! synchronous `new_input` and `new_output` methods cannot do IO themselves, so
//! they preserve the backend-returned child storage and path, then wrap that
//! child storage with the same IO runtime.
//!
//! Reader and writer creation are not the final IO boundary. Returned
//! `FileRead` and `FileWrite` handles can perform later byte-range reads,
//! chunk writes, and close operations, so those handles are wrapped too.

use std::future::Future;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tokio::sync::Mutex;

use super::Storage;
use crate::io::{FileMetadata, FileRead, FileWrite, InputFile, OutputFile};
use crate::{Error, ErrorKind, Result, RuntimeHandle};

/// Storage adapter that dispatches IO-bound operations on a dedicated runtime.
#[derive(Clone, Debug)]
pub(crate) struct RuntimeStorage {
    inner: Arc<dyn Storage>,
    io_runtime: RuntimeHandle,
}

impl RuntimeStorage {
    pub(crate) fn new(inner: Arc<dyn Storage>, io_runtime: RuntimeHandle) -> Self {
        Self { inner, io_runtime }
    }

    async fn run<T, Fut>(
        &self,
        operation: &'static str,
        f: impl FnOnce(Arc<dyn Storage>) -> Fut + Send + 'static,
    ) -> Result<T>
    where
        Fut: Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        let inner = Arc::clone(&self.inner);
        spawn_on_io(&self.io_runtime, operation, f(inner)).await
    }

    async fn run_path<T, Fut>(
        &self,
        operation: &'static str,
        path: &str,
        f: impl FnOnce(Arc<dyn Storage>, String) -> Fut + Send + 'static,
    ) -> Result<T>
    where
        Fut: Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        let path = path.to_owned();
        self.run(operation, move |inner| f(inner, path)).await
    }

    fn wrap_storage(&self, storage: Arc<dyn Storage>) -> Arc<dyn Storage> {
        Arc::new(Self::new(storage, self.io_runtime.clone()))
    }

    fn wrap_reader(&self, reader: Box<dyn FileRead>) -> Box<dyn FileRead> {
        Box::new(RuntimeFileRead::new(
            Arc::from(reader),
            self.io_runtime.clone(),
        ))
    }

    fn wrap_writer(&self, writer: Box<dyn FileWrite>) -> Box<dyn FileWrite> {
        Box::new(RuntimeFileWrite::new(writer, self.io_runtime.clone()))
    }
}

impl Serialize for RuntimeStorage {
    fn serialize<S>(&self, _serializer: S) -> std::result::Result<S::Ok, S::Error>
    where S: Serializer {
        Err(serde::ser::Error::custom(
            "RuntimeStorage is a transient scheduling adapter and cannot be serialized",
        ))
    }
}

impl<'de> Deserialize<'de> for RuntimeStorage {
    fn deserialize<D>(_deserializer: D) -> std::result::Result<Self, D::Error>
    where D: Deserializer<'de> {
        Err(serde::de::Error::custom(
            "RuntimeStorage is a transient scheduling adapter and cannot be deserialized",
        ))
    }
}

#[async_trait]
impl Storage for RuntimeStorage {
    #[doc(hidden)]
    fn typetag_name(&self) -> &'static str {
        "RuntimeStorage"
    }

    #[doc(hidden)]
    fn typetag_deserialize(&self) {}

    async fn exists(&self, path: &str) -> Result<bool> {
        self.run_path("checking file existence", path, |inner, path| async move {
            inner.exists(&path).await
        })
        .await
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        self.run_path("reading file metadata", path, |inner, path| async move {
            inner.metadata(&path).await
        })
        .await
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        self.run_path("reading file", path, |inner, path| async move {
            inner.read(&path).await
        })
        .await
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let reader = self
            .run_path("opening file reader", path, |inner, path| async move {
                inner.reader(&path).await
            })
            .await?;

        Ok(self.wrap_reader(reader))
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        self.run_path("writing file", path, |inner, path| async move {
            inner.write(&path, bs).await
        })
        .await
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let writer = self
            .run_path("opening file writer", path, |inner, path| async move {
                inner.writer(&path).await
            })
            .await?;

        Ok(self.wrap_writer(writer))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.run_path("deleting file", path, |inner, path| async move {
            inner.delete(&path).await
        })
        .await
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        self.run_path("deleting file prefix", path, |inner, path| async move {
            inner.delete_prefix(&path).await
        })
        .await
    }

    async fn delete_stream(&self, paths: BoxStream<'static, String>) -> Result<()> {
        self.run("deleting file stream", |inner| async move {
            inner.delete_stream(paths).await
        })
        .await
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        let (storage, path) = self.inner.new_input(path)?.into_parts();
        Ok(InputFile::new(self.wrap_storage(storage), path))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        let (storage, path) = self.inner.new_output(path)?.into_parts();
        Ok(OutputFile::new(self.wrap_storage(storage), path))
    }
}

struct RuntimeFileRead {
    inner: Arc<dyn FileRead>,
    io_runtime: RuntimeHandle,
}

impl RuntimeFileRead {
    fn new(inner: Arc<dyn FileRead>, io_runtime: RuntimeHandle) -> Self {
        Self { inner, io_runtime }
    }
}

#[async_trait]
impl FileRead for RuntimeFileRead {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        let inner = Arc::clone(&self.inner);
        spawn_on_io(&self.io_runtime, "reading file range", async move {
            inner.read(range).await
        })
        .await
    }
}

struct RuntimeFileWrite {
    inner: Arc<Mutex<Box<dyn FileWrite>>>,
    io_runtime: RuntimeHandle,
}

impl RuntimeFileWrite {
    fn new(inner: Box<dyn FileWrite>, io_runtime: RuntimeHandle) -> Self {
        Self {
            inner: Arc::new(Mutex::new(inner)),
            io_runtime,
        }
    }
}

#[async_trait]
impl FileWrite for RuntimeFileWrite {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        spawn_on_io(&self.io_runtime, "writing file chunk", async move {
            inner.lock().await.write(bs).await
        })
        .await
    }

    async fn close(&mut self) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        spawn_on_io(&self.io_runtime, "closing file writer", async move {
            inner.lock().await.close().await
        })
        .await
    }
}

async fn spawn_on_io<F, T>(runtime: &RuntimeHandle, operation: &'static str, future: F) -> Result<T>
where
    F: Future<Output = Result<T>> + Send + 'static,
    T: Send + 'static,
{
    runtime
        .spawn_abort_on_drop(future)
        .await
        .map_err(|e| spawned_task_error(operation, e))?
}

fn spawned_task_error(operation: &'static str, error: Error) -> Error {
    Error::new(
        ErrorKind::Unexpected,
        format!("{operation} failed on the IO runtime"),
    )
    .with_source(error)
}
