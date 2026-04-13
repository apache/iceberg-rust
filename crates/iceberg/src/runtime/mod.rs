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
use tracing::warn;

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

/// Runtime wrapper for spawning async tasks and blocking operations.
///
/// Wraps `Arc<tokio::runtime::Runtime>` internally. Cloning is cheap (Arc clone).
/// This is the public API boundary — internals can evolve without breaking consumers.
#[derive(Clone)]
pub struct Runtime {
    /// We keep an optional owned runtime to prevent it from being dropped.
    /// When created via `Runtime::new()`, this holds the runtime alive.
    /// When created via `Default` inside an existing tokio context, this is None
    /// and we only use the handle.
    _owned: Option<Arc<tokio::runtime::Runtime>>,
    handle: tokio::runtime::Handle,
}

impl fmt::Debug for Runtime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Runtime").finish()
    }
}

impl Runtime {
    /// Create a new Runtime wrapping the given tokio Runtime.
    pub fn new(runtime: Arc<tokio::runtime::Runtime>) -> Self {
        let handle = runtime.handle().clone();
        Self {
            _owned: Some(runtime),
            handle,
        }
    }

    /// Spawn an async task on the wrapped runtime.
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        JoinHandle(self.handle.spawn(future))
    }

    /// Spawn a blocking task on the wrapped runtime.
    pub fn spawn_blocking<F, T>(&self, f: F) -> JoinHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        JoinHandle(self.handle.spawn_blocking(f))
    }
}

impl Default for Runtime {
    fn default() -> Self {
        // Try to use the current tokio runtime handle if we're already inside one.
        // This avoids creating a new runtime that would panic when dropped in an
        // async context.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            return Self {
                _owned: None,
                handle,
            };
        }

        warn!(
            "No tokio runtime found. Creating a new multi-thread runtime for iceberg. \
             Consider providing an explicit Runtime via CatalogBuilder::with_runtime() \
             or TableBuilder::runtime() to avoid unexpected resource usage."
        );

        let rt = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("Failed to build default tokio runtime"),
        );
        let handle = rt.handle().clone();
        Self {
            _owned: Some(rt),
            handle,
        }
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

    #[test]
    fn test_runtime_default_creates_working_runtime() {
        let rt = Runtime::default();
        let handle = rt.spawn(async { 1 + 1 });
        let result = rt._owned.as_ref().unwrap().block_on(handle);
        assert_eq!(result, 2);
    }

    #[test]
    fn test_runtime_spawn() {
        let rt = test_runtime();
        let handle = rt.spawn(async { 1 + 1 });
        let result = rt._owned.as_ref().unwrap().block_on(handle);
        assert_eq!(result, 2);
    }

    #[test]
    fn test_runtime_spawn_blocking() {
        let rt = test_runtime();
        let handle = rt.spawn_blocking(|| 1 + 1);
        let result = rt._owned.as_ref().unwrap().block_on(handle);
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
        let handle = rt.spawn(async { 42 });
        let result = rt._owned.as_ref().unwrap().block_on(handle);
        assert_eq!(result, 42);
    }

    #[test]
    fn test_runtime_clone_shares_arc() {
        let rt = test_runtime();
        let rt2 = rt.clone();
        assert!(Arc::ptr_eq(
            rt._owned.as_ref().unwrap(),
            rt2._owned.as_ref().unwrap()
        ));
    }

    #[test]
    fn test_runtime_debug() {
        let rt = test_runtime();
        let debug_str = format!("{:?}", rt);
        assert!(debug_str.contains("Runtime"));
    }
}
