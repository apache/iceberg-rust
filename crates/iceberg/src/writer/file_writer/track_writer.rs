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

use std::sync::atomic::AtomicI64;
use std::sync::Arc;

use bytes::Bytes;

use crate::io::FileWrite;
use crate::Result;

/// `TrackWriter` is used to track the written size.
pub(crate) struct TrackWriter<W: FileWrite> {
    inner: W,
    written_size: Arc<AtomicI64>,
}

impl<W: FileWrite> TrackWriter<W> {
    pub fn new(writer: W, written_size: Arc<AtomicI64>) -> Self {
        Self {
            inner: writer,
            written_size,
        }
    }
}

impl<W: FileWrite> FileWrite for TrackWriter<W> {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        let size = bs.len();
        self.inner.write(bs).await.map(|v| {
            self.written_size
                .fetch_add(size as i64, std::sync::atomic::Ordering::Relaxed);
            v
        })
    }

    async fn close(&mut self) -> Result<()> {
        self.inner.close().await
    }
}
