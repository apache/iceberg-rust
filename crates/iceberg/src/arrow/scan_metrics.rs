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

//! Scan metrics and I/O counting for Parquet data file reads.

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

use crate::error::Result;
use crate::io::FileRead;

/// Wraps a [`FileRead`] to count bytes read via a shared atomic counter.
pub(crate) struct CountingFileRead {
    inner: Box<dyn FileRead>,
    bytes_read: Arc<AtomicU64>,
}

impl CountingFileRead {
    pub(crate) fn new(inner: Box<dyn FileRead>, bytes_read: Arc<AtomicU64>) -> Self {
        Self { inner, bytes_read }
    }
}

#[async_trait::async_trait]
impl FileRead for CountingFileRead {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        self.bytes_read
            .fetch_add(range.end - range.start, Ordering::Relaxed);
        self.inner.read(range).await
    }
}

/// Metrics collected during an Iceberg scan.
///
/// Returned alongside the record batch stream from [`ArrowReader::read`](super::ArrowReader::read).
/// Additional counters (e.g. positional deletes applied, row groups pruned)
/// can be added here without changing the `read()` return type.
#[derive(Clone, Debug)]
pub struct ScanMetrics {
    bytes_read: Arc<AtomicU64>,
}

impl ScanMetrics {
    pub(crate) fn new() -> Self {
        Self {
            bytes_read: Arc::new(AtomicU64::new(0)),
        }
    }

    pub(crate) fn bytes_read_counter(&self) -> &Arc<AtomicU64> {
        &self.bytes_read
    }

    /// Total bytes read from storage for data files during this scan.
    pub fn bytes_read(&self) -> u64 {
        self.bytes_read.load(Ordering::Relaxed)
    }
}
