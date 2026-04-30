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

// This module contains the async runtime abstraction for iceberg.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::task;

use crate::{Error, ErrorKind, Result};

/// Wrapper around tokio's JoinHandle that panics on task failure.
pub struct JoinHandle<T>(task::JoinHandle<T>);

impl<T> Unpin for JoinHandle<T> {}

impl<T: Send + 'static> Future for JoinHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut() {
            JoinHandle(handle) => Pin::new(handle)
                .poll(cx)
                .map(|r| r.expect("tokio spawned task failed")),
        }
    }
}

/// Handle to a single tokio runtime. Holds an optional `Arc` to keep the
/// runtime alive when we own it, and a `Handle` for spawning.
#[derive(Clone)]
pub struct RuntimeHandle {
    /// Keeps the tokio runtime alive when we own it (`Runtime::new`).
    /// `None` when borrowing an existing runtime via `Handle::try_current()`.
    _owned: Option<Arc<tokio::runtime::Runtime>>,
    handle: tokio::runtime::Handle,
}

impl fmt::Debug for RuntimeHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeHandle").finish()
    }
}

impl RuntimeHandle {
    /// Create a handle that owns the given tokio runtime.
    fn new(runtime: Arc<tokio::runtime::Runtime>) -> Self {
        let handle = runtime.handle().clone();
        Self {
            _owned: Some(runtime),
            handle,
        }
    }

    /// Create a handle that borrows an existing tokio runtime via its handle.
    fn from_tokio_handle(handle: tokio::runtime::Handle) -> Self {
        Self {
            _owned: None,
            handle,
        }
    }

    /// Spawn an async task.
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        JoinHandle(self.handle.spawn(future))
    }

    /// Spawn a blocking task.
    pub fn spawn_blocking<F, T>(&self, f: F) -> JoinHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        JoinHandle(self.handle.spawn_blocking(f))
    }
}

/// Iceberg's runtime abstraction.
///
/// Contains separate handles for IO-bound and CPU-bound work. When constructed
/// with a single tokio runtime, both `io()` and `cpu()` route to the same one.
/// Use `new_with_split` to provide dedicated runtimes for each category.
///
/// Cloning is cheap (Arc clones internally).
#[derive(Clone)]
pub struct Runtime {
    io: RuntimeHandle,
    cpu: RuntimeHandle,
}

impl fmt::Debug for Runtime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Runtime").finish()
    }
}

impl Runtime {
    /// Create a Runtime backed by a single tokio runtime for all work.
    pub fn new(runtime: Arc<tokio::runtime::Runtime>) -> Self {
        let handle = RuntimeHandle::new(runtime);
        Self {
            io: handle.clone(),
            cpu: handle,
        }
    }

    /// Create a Runtime with separate tokio runtimes for IO and CPU work.
    pub fn new_with_split(
        io_runtime: Arc<tokio::runtime::Runtime>,
        cpu_runtime: Arc<tokio::runtime::Runtime>,
    ) -> Self {
        Self {
            io: RuntimeHandle::new(io_runtime),
            cpu: RuntimeHandle::new(cpu_runtime),
        }
    }

    /// Create a Runtime that borrows the tokio runtime the caller is currently
    /// running in.
    ///
    /// Returns an error if this is not called from within a tokio runtime
    /// context. This is the preferred path for callers who already manage a
    /// tokio runtime (e.g. `#[tokio::main]`, datafusion, axum): iceberg will
    /// reuse that runtime rather than spawning its own.
    pub fn try_from_current() -> Result<Self> {
        let handle = tokio::runtime::Handle::try_current().map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                "no tokio runtime in context; call Runtime::try_from_current() \
                 from within a tokio runtime, or construct a Runtime explicitly \
                 via Runtime::new / Runtime::new_with_split",
            )
            .with_source(e)
        })?;
        let rh = RuntimeHandle::from_tokio_handle(handle);
        Ok(Self {
            io: rh.clone(),
            cpu: rh,
        })
    }

    /// Handle for IO-bound work (network fetches, file reads).
    pub fn io(&self) -> &RuntimeHandle {
        &self.io
    }

    /// Handle for CPU-bound work (decoding, predicate eval, projection).
    pub fn cpu(&self) -> &RuntimeHandle {
        &self.cpu
    }
}

impl Default for Runtime {
    /// Borrows the current tokio runtime via [`Handle::try_current`].
    fn default() -> Self {
        Self::try_from_current().expect(
            "Runtime::default() called outside a tokio runtime context. \
             Call it from within #[tokio::main] / #[tokio::test], or construct \
             a Runtime explicitly via Runtime::new / Runtime::new_with_split.",
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_runtime() -> Runtime {
        let tokio_rt = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("Failed to build tokio runtime"),
        );
        Runtime::new(tokio_rt)
    }

    fn block_on<F: Future>(rt: &Runtime, f: F) -> F::Output {
        rt.io._owned.as_ref().unwrap().block_on(f)
    }

    #[test]
    fn test_runtime_spawn_io() {
        let rt = test_runtime();
        let handle = rt.io().spawn(async { 1 + 1 });
        let result = block_on(&rt, handle);
        assert_eq!(result, 2);
    }

    #[test]
    fn test_runtime_spawn_cpu() {
        let rt = test_runtime();
        let handle = rt.cpu().spawn(async { 3 + 4 });
        let result = block_on(&rt, handle);
        assert_eq!(result, 7);
    }

    #[test]
    fn test_runtime_spawn_blocking() {
        let rt = test_runtime();
        let handle = rt.cpu().spawn_blocking(|| 1 + 1);
        let result = block_on(&rt, handle);
        assert_eq!(result, 2);
    }

    #[test]
    fn test_runtime_new_with_custom_runtime() {
        let tokio_rt = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("Failed to build tokio runtime"),
        );
        let rt = Runtime::new(tokio_rt);
        let handle = rt.io().spawn(async { 42 });
        let result = block_on(&rt, handle);
        assert_eq!(result, 42);
    }

    #[test]
    fn test_runtime_single_shares_handle() {
        let rt = test_runtime();
        // When built with a single runtime, io and cpu point to the same handle
        assert!(Arc::ptr_eq(
            rt.io._owned.as_ref().unwrap(),
            rt.cpu._owned.as_ref().unwrap()
        ));
    }

    #[test]
    fn test_runtime_split_uses_separate_handles() {
        let io_rt = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap(),
        );
        let cpu_rt = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap(),
        );
        let rt = Runtime::new_with_split(io_rt, cpu_rt);
        assert!(!Arc::ptr_eq(
            rt.io._owned.as_ref().unwrap(),
            rt.cpu._owned.as_ref().unwrap()
        ));
    }

    #[test]
    fn test_runtime_clone_shares_arc() {
        let rt = test_runtime();
        let rt2 = rt.clone();
        assert!(Arc::ptr_eq(
            rt.io._owned.as_ref().unwrap(),
            rt2.io._owned.as_ref().unwrap()
        ));
    }

    #[test]
    fn test_runtime_debug() {
        let rt = test_runtime();
        let debug_str = format!("{:?}", rt);
        assert!(debug_str.contains("Runtime"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_try_from_current_in_runtime() {
        let rt = Runtime::try_from_current().expect("should find current runtime");
        let result = rt.io().spawn(async { 7 }).await;
        assert_eq!(result, 7);
        // Borrowed: no owned Arc.
        assert!(rt.io._owned.is_none());
        assert!(rt.cpu._owned.is_none());
    }

    #[test]
    fn test_try_from_current_outside_runtime() {
        let err = Runtime::try_from_current().expect_err("must fail outside runtime");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
    }
}
