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

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

pub enum JoinHandle<T> {
    #[cfg(feature = "tokio")]
    Tokio(tokio::task::JoinHandle<T>),
    #[cfg(all(feature = "smol", not(feature = "tokio")))]
    Smol(smol::Task<T>),
    #[cfg(all(not(feature = "smol"), not(feature = "tokio")))]
    Unimplemented(Box<T>),
}

impl<T: Send + 'static> Future for JoinHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut() {
            #[cfg(feature = "tokio")]
            JoinHandle::Tokio(handle) => Pin::new(handle)
                .poll(cx)
                .map(|h| h.expect("tokio spawned task failed")),
            #[cfg(all(feature = "smol", not(feature = "tokio")))]
            JoinHandle::Smol(handle) => Pin::new(handle).poll(cx),
            #[cfg(all(not(feature = "smol"), not(feature = "tokio")))]
            JoinHandle::Unimplemented(_) => unimplemented!("no runtime has been enabled"),
        }
    }
}

#[allow(dead_code)]
pub fn spawn<F>(f: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    #[cfg(feature = "tokio")]
    return JoinHandle::Tokio(tokio::task::spawn(f));

    #[cfg(all(feature = "smol", not(feature = "tokio")))]
    return JoinHandle::Smol(smol::spawn(f));

    #[cfg(all(not(feature = "smol"), not(feature = "tokio")))]
    unimplemented!("no runtime has been enabled")
}

#[allow(dead_code)]
pub fn spawn_blocking<F, T>(f: F) -> JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    #[cfg(feature = "tokio")]
    return JoinHandle::Tokio(tokio::task::spawn_blocking(f));

    #[cfg(all(feature = "smol", not(feature = "tokio")))]
    return JoinHandle::Smol(smol::unblock(f));

    #[cfg(all(not(feature = "smol"), not(feature = "tokio")))]
    unimplemented!("no runtime has been enabled")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn test_tokio_spawn() {
        let handle = spawn(async { 1 + 1 });
        assert_eq!(handle.await, 2);
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn test_tokio_spawn_blocking() {
        let handle = spawn_blocking(|| 1 + 1);
        assert_eq!(handle.await, 2);
    }

    #[cfg(all(feature = "smol", not(feature = "tokio")))]
    #[smol::test]
    async fn test_smol_spawn() {
        let handle = spawn(async { 1 + 1 });
        assert_eq!(handle.await, 2);
    }

    #[cfg(all(feature = "smol", not(feature = "tokio")))]
    #[smo::test]
    async fn test_smol_spawn_blocking() {
        let handle = spawn_blocking(|| 1 + 1);
        assert_eq!(handle.await, 2);
    }
}
