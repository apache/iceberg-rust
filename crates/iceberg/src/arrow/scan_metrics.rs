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

//! Scan metrics and I/O counting for data and delete file reads.

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

use crate::error::Result;
use crate::io::FileRead;
use crate::scan::ArrowRecordBatchStream;

/// The kind of file whose reads are being counted.
pub(crate) enum ScanFileType {
    Data,
    Delete,
}

/// Wraps a [`FileRead`] to count bytes read in the shared scan metrics.
pub(crate) struct CountingFileRead<F: FileRead> {
    inner: F,
    scan_metrics: ScanMetrics,
    file_type: ScanFileType,
}

impl<F: FileRead> CountingFileRead<F> {
    pub(crate) fn new(inner: F, scan_metrics: ScanMetrics, file_type: ScanFileType) -> Self {
        Self {
            inner,
            scan_metrics,
            file_type,
        }
    }
}

#[async_trait::async_trait]
impl<F: FileRead> FileRead for CountingFileRead<F> {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        debug_assert!(range.end >= range.start);
        let bytes_read = range.end - range.start;
        match self.file_type {
            ScanFileType::Data => self.scan_metrics.add_data_bytes_read(bytes_read),
            ScanFileType::Delete => self.scan_metrics.add_delete_bytes_read(bytes_read),
        }
        self.inner.read(range).await
    }
}

/// Metrics collected during an Iceberg scan.
#[derive(Clone, Debug)]
pub struct ScanMetrics {
    inner: Arc<ScanMetricsInner>,
}

#[derive(Debug, Default)]
struct ScanMetricsInner {
    data_bytes_read: AtomicU64,
    delete_bytes_read: AtomicU64,
    data_files_opened: AtomicU64,
    delete_files_opened: AtomicU64,
    rows_emitted: AtomicU64,
}

impl ScanMetrics {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(ScanMetricsInner::default()),
        }
    }

    pub(crate) fn add_data_bytes_read(&self, bytes: u64) {
        self.inner
            .data_bytes_read
            .fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn add_delete_bytes_read(&self, bytes: u64) {
        self.inner
            .delete_bytes_read
            .fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn add_data_files_opened(&self, files: u64) {
        self.inner
            .data_files_opened
            .fetch_add(files, Ordering::Relaxed);
    }

    pub(crate) fn add_delete_files_opened(&self, files: u64) {
        self.inner
            .delete_files_opened
            .fetch_add(files, Ordering::Relaxed);
    }

    pub(crate) fn add_rows_emitted(&self, rows: u64) {
        self.inner.rows_emitted.fetch_add(rows, Ordering::Relaxed);
    }

    /// Total bytes read from storage during this scan, including data files and delete files.
    pub fn bytes_read(&self) -> u64 {
        self.data_bytes_read() + self.delete_bytes_read()
    }

    /// Total bytes read from data files in storage during this scan.
    pub fn data_bytes_read(&self) -> u64 {
        self.inner.data_bytes_read.load(Ordering::Relaxed)
    }

    /// Total bytes read from delete files in storage during this scan.
    pub fn delete_bytes_read(&self) -> u64 {
        self.inner.delete_bytes_read.load(Ordering::Relaxed)
    }

    /// Total number of data files opened during this scan
    pub fn data_files_opened(&self) -> u64 {
        self.inner.data_files_opened.load(Ordering::Relaxed)
    }

    /// Total number of delete files opened during this scan
    /// Delete files can be reused/shared across multiple data file scans
    /// This does *not* include delete files that were reused from cache
    pub fn delete_files_opened(&self) -> u64 {
        self.inner.delete_files_opened.load(Ordering::Relaxed)
    }

    /// Total number of rows emitted from this scan
    pub fn rows_emitted(&self) -> u64 {
        self.inner.rows_emitted.load(Ordering::Relaxed)
    }
}

/// Result of [`ArrowReader::read`](super::ArrowReader::read), containing the
/// record batch stream and metrics collected during the scan.
pub struct ScanResult {
    stream: ArrowRecordBatchStream,
    metrics: ScanMetrics,
}

impl ScanResult {
    pub(crate) fn new(stream: ArrowRecordBatchStream, metrics: ScanMetrics) -> Self {
        Self { stream, metrics }
    }

    /// Consumes the result, returning only the record batch stream.
    pub fn stream(self) -> ArrowRecordBatchStream {
        self.stream
    }

    /// Returns a reference to the scan metrics.
    pub fn metrics(&self) -> &ScanMetrics {
        &self.metrics
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cloned_metrics_share_concurrent_updates() {
        let metrics = ScanMetrics::new();

        std::thread::scope(|scope| {
            for _ in 0..4 {
                let metrics = metrics.clone();
                scope.spawn(move || {
                    for _ in 0..100 {
                        metrics.add_data_bytes_read(10);
                        metrics.add_delete_bytes_read(20);
                        metrics.add_data_files_opened(1);
                        metrics.add_delete_files_opened(2);
                        metrics.add_rows_emitted(3);
                    }
                });
            }
        });

        assert_eq!(metrics.data_bytes_read(), 4000);
        assert_eq!(metrics.delete_bytes_read(), 8000);
        assert_eq!(metrics.bytes_read(), 12000);
        assert_eq!(metrics.data_files_opened(), 400);
        assert_eq!(metrics.delete_files_opened(), 800);
        assert_eq!(metrics.rows_emitted(), 1200);
    }
}
