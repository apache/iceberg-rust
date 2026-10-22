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

use serde::{Deserialize, Serialize};

use super::ArrowReaderBuilder;

/// Reader settings captured from a [`TableScan`](crate::scan::TableScan).
///
/// Contains no I/O handles, runtime, or planning state. Apply these options to a
/// locally constructed builder, then build one reader and clone it across
/// partitions to share its delete-file cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableScanReaderOptions {
    pub(crate) batch_size: Option<usize>,
    pub(crate) data_file_concurrency_limit: usize,
    pub(crate) row_group_filtering_enabled: bool,
    pub(crate) row_selection_enabled: bool,
    pub(crate) bloom_filter_enabled: bool,
}

impl TableScanReaderOptions {
    /// The requested record batch size, or the reader default.
    pub fn batch_size(&self) -> Option<usize> {
        self.batch_size
    }

    /// The maximum number of data files read concurrently.
    pub fn data_file_concurrency_limit(&self) -> usize {
        self.data_file_concurrency_limit
    }

    /// Whether row-group filtering is enabled.
    pub fn row_group_filtering_enabled(&self) -> bool {
        self.row_group_filtering_enabled
    }

    /// Whether page-index row selection is enabled.
    pub fn row_selection_enabled(&self) -> bool {
        self.row_selection_enabled
    }

    /// Whether bloom-filter row-group filtering is enabled.
    pub fn bloom_filter_enabled(&self) -> bool {
        self.bloom_filter_enabled
    }

    /// Applies all scan reader settings, preserving the builder's local resources
    /// and Parquet I/O tuning. An unset batch size restores the reader default.
    pub fn apply_to(&self, mut builder: ArrowReaderBuilder) -> ArrowReaderBuilder {
        builder.batch_size = self.batch_size;
        builder.concurrency_limit_data_files = self.data_file_concurrency_limit;
        builder.row_group_filtering_enabled = self.row_group_filtering_enabled;
        builder.row_selection_enabled = self.row_selection_enabled;
        builder.bloom_filter_enabled = self.bloom_filter_enabled;
        builder
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIO;
    use crate::test_utils::test_runtime;

    #[test]
    fn applies_scan_settings_and_preserves_local_io_tuning() {
        for batch_size in [None, Some(17)] {
            let options = TableScanReaderOptions {
                batch_size,
                data_file_concurrency_limit: 3,
                row_group_filtering_enabled: false,
                row_selection_enabled: true,
                bloom_filter_enabled: true,
            };
            let builder = ArrowReaderBuilder::new(FileIO::new_with_memory(), test_runtime())
                .with_batch_size(1024)
                .with_metadata_size_hint(4096)
                .with_range_coalesce_bytes(8192)
                .with_range_fetch_concurrency(2);
            let reader = options.apply_to(builder).build();
            assert_eq!(
                (
                    reader.batch_size,
                    reader.concurrency_limit_data_files,
                    reader.row_group_filtering_enabled,
                    reader.row_selection_enabled,
                    reader.bloom_filter_enabled
                ),
                (batch_size, 3, false, true, true),
            );
            assert_eq!(
                (
                    reader.parquet_read_options.metadata_size_hint(),
                    reader.parquet_read_options.range_coalesce_bytes(),
                    reader.parquet_read_options.range_fetch_concurrency()
                ),
                (Some(4096), 8192, 2),
            );
        }
    }
}
