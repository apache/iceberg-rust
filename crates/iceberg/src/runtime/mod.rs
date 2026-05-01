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

/// Wrapper around tokio's `JoinHandle` that converts task failures into
/// [`iceberg::Error`].
///
/// Tokio's `JoinHandle<T>` resolves to `Result<T, JoinError>`, where a
/// `JoinError` means the task either panicked or was cancelled (typically from
/// runtime shutdown or `abort`). Both are surfaced here as
/// `ErrorKind::Unexpected` with the original `JoinError` preserved as the
/// source.
pub struct JoinHandle<T>(task::JoinHandle<T>);

impl<T> Unpin for JoinHandle<T> {}

impl<T: Send + 'static> Future for JoinHandle<T> {
    type Output = crate::Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().0).poll(cx).map(|r| {
            r.map_err(|e| Error::new(ErrorKind::Unexpected, "spawned task failed").with_source(e))
        })
    }
}

/// Handle to a single tokio runtime.
///
/// Wraps a [`tokio::runtime::Handle`], which is cheap to clone. The caller is
/// responsible for keeping the underlying runtime alive while this handle is
/// in use; spawning on a shut-down runtime will surface as a `JoinError` via
/// [`JoinHandle`].
#[derive(Clone)]
pub struct RuntimeHandle {
    handle: tokio::runtime::Handle,
}

impl fmt::Debug for RuntimeHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeHandle").finish()
    }
}

impl RuntimeHandle {
    fn from_tokio_handle(handle: tokio::runtime::Handle) -> Self {
        Self { handle }
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
/// Use [`Runtime::new_with_split`] to provide dedicated runtimes for each
/// category.
///
/// # Lifetime
///
/// A `Runtime` stores only `tokio::runtime::Handle`s (weak references). The
/// caller owns the tokio runtime's lifetime. If the underlying runtime is
/// dropped while iceberg is still using it, subsequent spawns will surface as
/// task cancellation errors via [`JoinHandle`].
///
/// Cloning is cheap.
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
        let handle = RuntimeHandle::from_tokio_handle(runtime.handle().clone());
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
            io: RuntimeHandle::from_tokio_handle(io_runtime.handle().clone()),
            cpu: RuntimeHandle::from_tokio_handle(cpu_runtime.handle().clone()),
        }
    }

    /// Borrows the tokio runtime the caller is currently running in.
    ///
    /// Panics if called outside a tokio runtime context. Use
    /// [`Runtime::try_current`] for a fallible version.
    ///
    /// Iceberg never implicitly spawns its own runtime; callers outside a
    /// tokio context must construct one explicitly via [`Runtime::new`] or
    /// [`Runtime::new_with_split`].
    pub fn current() -> Self {
        Self::try_current().expect(
            "Runtime::current() called outside a tokio runtime context. \
             Call it from within #[tokio::main] / #[tokio::test], or construct \
             a Runtime explicitly via Runtime::new / Runtime::new_with_split.",
        )
    }

    /// Fallible variant of [`Runtime::current`]. Returns an error if no tokio
    /// runtime is available in the current context.
    pub fn try_current() -> Result<Self> {
        let handle = tokio::runtime::Handle::try_current().map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                "no tokio runtime in context; call Runtime::try_current() \
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A test harness that owns a tokio runtime and exposes a `Runtime` handle
    /// plus a `block_on` helper for sync test bodies.
    struct TestRuntime {
        tokio: Arc<tokio::runtime::Runtime>,
        rt: Runtime,
    }

    impl TestRuntime {
        fn new() -> Self {
            let tokio = Arc::new(
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .expect("Failed to build tokio runtime"),
            );
            let rt = Runtime::new(tokio.clone());
            Self { tokio, rt }
        }

        fn block_on<F: Future>(&self, f: F) -> F::Output {
            self.tokio.block_on(f)
        }
    }

    #[test]
    fn test_runtime_spawn_io() {
        let h = TestRuntime::new();
        let handle = h.rt.io().spawn(async { 1 + 1 });
        assert_eq!(h.block_on(handle).unwrap(), 2);
    }

    #[test]
    fn test_runtime_spawn_cpu() {
        let h = TestRuntime::new();
        let handle = h.rt.cpu().spawn(async { 3 + 4 });
        assert_eq!(h.block_on(handle).unwrap(), 7);
    }

    #[test]
    fn test_runtime_spawn_blocking() {
        let h = TestRuntime::new();
        let handle = h.rt.cpu().spawn_blocking(|| 1 + 1);
        assert_eq!(h.block_on(handle).unwrap(), 2);
    }

    #[test]
    fn test_runtime_new_with_custom_runtime() {
        let h = TestRuntime::new();
        let handle = h.rt.io().spawn(async { 42 });
        assert_eq!(h.block_on(handle).unwrap(), 42);
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
        let rt = Runtime::new_with_split(io_rt.clone(), cpu_rt.clone());
        // Spawn on each and confirm both are distinct live runtimes. We use
        // `io_rt`/`cpu_rt` directly to `block_on` since our `Runtime` doesn't
        // expose one.
        let io_result = io_rt.block_on(async { rt.io().spawn(async { "io" }).await.unwrap() });
        let cpu_result = cpu_rt.block_on(async { rt.cpu().spawn(async { "cpu" }).await.unwrap() });
        assert_eq!(io_result, "io");
        assert_eq!(cpu_result, "cpu");
    }

    #[test]
    fn test_runtime_clone() {
        let h = TestRuntime::new();
        let rt2 = h.rt.clone();
        let handle = rt2.io().spawn(async { 5 });
        assert_eq!(h.block_on(handle).unwrap(), 5);
    }

    #[test]
    fn test_runtime_debug() {
        let h = TestRuntime::new();
        let debug_str = format!("{:?}", h.rt);
        assert!(debug_str.contains("Runtime"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_try_current_in_runtime() {
        let rt = Runtime::try_current().expect("should find current runtime");
        let result = rt.io().spawn(async { 7 }).await.unwrap();
        assert_eq!(result, 7);
    }

    #[test]
    fn test_try_current_outside_runtime() {
        let err = Runtime::try_current().expect_err("must fail outside runtime");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
    }

    /// Verifies that when the caller drops the underlying tokio runtime, a
    /// subsequent spawn surfaces as a `JoinError` via our `JoinHandle` rather
    /// than hanging or misbehaving.
    #[test]
    fn test_spawn_after_runtime_drop_errors() {
        let driver = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let owned = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap(),
        );
        let rt = Runtime::new(owned.clone());
        // Drop the caller's strong reference. `owned` was the only Arc.
        drop(owned);

        // Spawning after shutdown returns a handle that resolves to a cancelled
        // JoinError, which our wrapper maps to iceberg::Error.
        let handle = rt.io().spawn(async { 1 });
        let result = driver.block_on(handle);
        assert!(result.is_err(), "expected error after runtime shutdown");
    }
}
