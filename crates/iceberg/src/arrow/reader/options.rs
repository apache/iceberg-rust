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
use typed_builder::TypedBuilder;

use super::ArrowReaderBuilder;

/// Configures Arrow reads independently of file planning and local I/O resources.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TypedBuilder)]
#[serde(default)]
#[builder(field_defaults(default, setter(prefix = "with_")))]
pub struct ArrowReaderOptions {
    /// Desired batch size, or the reader default when unset.
    batch_size: Option<usize>,
    /// Maximum in-flight data files, or execution-host parallelism when unset.
    data_file_concurrency_limit: Option<usize>,
    /// Whether to prune row groups using their statistics.
    #[builder(default = true)]
    row_group_filtering_enabled: bool,
    /// Whether to use page indexes for row selection.
    row_selection_enabled: bool,
    /// Whether to use bloom filters for row-group pruning.
    bloom_filter_enabled: bool,
}

impl Default for ArrowReaderOptions {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl ArrowReaderBuilder {
    /// Replaces reader settings while preserving local resources and Parquet I/O tuning.
    /// Unset batch size and concurrency restore the defaults used at reader construction.
    pub fn with_options(mut self, options: ArrowReaderOptions) -> Self {
        self.batch_size = options.batch_size;
        self.concurrency_limit_data_files = options.data_file_concurrency_limit;
        self.row_group_filtering_enabled = options.row_group_filtering_enabled;
        self.row_selection_enabled = options.row_selection_enabled;
        self.bloom_filter_enabled = options.bloom_filter_enabled;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIO;
    use crate::test_utils::test_runtime;
    use crate::util::available_parallelism;

    #[test]
    fn missing_options_use_defaults_and_unknown_options_are_ignored() {
        let decoded: ArrowReaderOptions =
            serde_json::from_str(r#"{"future_option":true}"#).unwrap();
        assert_eq!(decoded, ArrowReaderOptions::default());
        let decoded: ArrowReaderOptions = serde_json::from_str(r#"{"batch_size":17}"#).unwrap();
        assert_eq!(
            decoded,
            ArrowReaderOptions::builder()
                .with_batch_size(Some(17))
                .build()
        );
        assert_eq!(
            serde_json::to_value(ArrowReaderOptions::default()).unwrap()["data_file_concurrency_limit"],
            serde_json::Value::Null
        );
    }

    #[test]
    fn options_restore_local_defaults_or_apply_explicit_settings() {
        for explicit in [false, true] {
            let options = if explicit {
                ArrowReaderOptions::builder()
                    .with_batch_size(Some(17))
                    .with_data_file_concurrency_limit(Some(3))
                    .with_row_group_filtering_enabled(false)
                    .with_row_selection_enabled(true)
                    .with_bloom_filter_enabled(true)
                    .build()
            } else {
                ArrowReaderOptions::default()
            };
            let reader = ArrowReaderBuilder::new(FileIO::new_with_memory(), test_runtime())
                .with_batch_size(1024)
                .with_data_file_concurrency_limit(99)
                .with_metadata_size_hint(4096)
                .with_range_coalesce_bytes(8192)
                .with_range_fetch_concurrency(2)
                .with_options(options)
                .build();
            let expected = if explicit {
                (Some(17), 3, false, true, true)
            } else {
                (None, available_parallelism().get(), true, false, false)
            };
            assert_eq!(
                (
                    reader.batch_size,
                    reader.concurrency_limit_data_files,
                    reader.row_group_filtering_enabled,
                    reader.row_selection_enabled,
                    reader.bloom_filter_enabled
                ),
                expected
            );
            assert_eq!(
                (
                    reader.parquet_read_options.metadata_size_hint(),
                    reader.parquet_read_options.range_coalesce_bytes(),
                    reader.parquet_read_options.range_fetch_concurrency()
                ),
                (Some(4096), 8192, 2)
            );
        }
    }
}
