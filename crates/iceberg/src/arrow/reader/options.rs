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

use super::{ArrowReader, ParquetReadOptions};
use crate::arrow::caching_delete_file_loader::CachingDeleteFileLoader;
use crate::io::FileIO;
use crate::runtime::Runtime;
use crate::util::available_parallelism;

/// Configures Arrow reads independently of file planning and local I/O resources.
///
/// ```no_run
/// # use iceberg::{Result, arrow::{ArrowReaderOptions, ParquetReadOptions}, table::Table};
/// # async fn read(table: &Table) -> Result<()> {
/// let options = ArrowReaderOptions::builder()
///     .with_batch_size(Some(1024))
///     .with_parquet_options(
///         ParquetReadOptions::builder()
///             .with_metadata_size_hint(Some(512 * 1024))
///             .build(),
///     )
///     .build();
/// let stream = table.scan().build()?.to_arrow(options).await?;
/// # Ok(())
/// # }
/// ```
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
    /// I/O tuning applied when reading Parquet files.
    parquet_options: ParquetReadOptions,
}

impl Default for ArrowReaderOptions {
    fn default() -> Self {
        Self::builder().build()
    }
}

/// Builder to create ArrowReader
pub struct ArrowReaderBuilder {
    options: ArrowReaderOptions,
    file_io: FileIO,
    runtime: Runtime,
}

impl ArrowReaderBuilder {
    /// Create a new ArrowReaderBuilder
    pub fn new(file_io: FileIO, runtime: Runtime) -> Self {
        ArrowReaderBuilder {
            options: ArrowReaderOptions::default(),
            file_io,
            runtime,
        }
    }

    /// Sets the max number of in flight data files that are being fetched
    pub fn with_data_file_concurrency_limit(mut self, val: usize) -> Self {
        self.options.data_file_concurrency_limit = Some(val);
        self
    }

    /// Sets the desired size of batches in the response
    /// to something other than the default
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.options.batch_size = Some(batch_size);
        self
    }

    /// Determines whether to enable row group filtering.
    pub fn with_row_group_filtering_enabled(mut self, row_group_filtering_enabled: bool) -> Self {
        self.options.row_group_filtering_enabled = row_group_filtering_enabled;
        self
    }

    /// Determines whether to enable row selection.
    pub fn with_row_selection_enabled(mut self, row_selection_enabled: bool) -> Self {
        self.options.row_selection_enabled = row_selection_enabled;
        self
    }

    /// Determines whether to enable bloom filter-based row group filtering.
    ///
    /// When enabled, if a read is performed with an equality or IN predicate,
    /// the bloom filter for relevant columns in each row group is read and
    /// checked. Row groups where the bloom filter proves the value is absent
    /// are skipped entirely.
    ///
    /// Defaults to disabled. Each bloom filter is a separate read, and they are
    /// issued serially — one round trip per relevant column per row group, before
    /// any data is read. TODO(#3191)
    pub fn with_bloom_filter_enabled(mut self, bloom_filter_enabled: bool) -> Self {
        self.options.bloom_filter_enabled = bloom_filter_enabled;
        self
    }

    /// Provide a hint as to the number of bytes to prefetch for parsing the Parquet metadata
    ///
    /// This hint can help reduce the number of fetch requests. For more details see the
    /// [ParquetMetaDataReader documentation](https://docs.rs/parquet/latest/parquet/file/metadata/struct.ParquetMetaDataReader.html#method.with_prefetch_hint).
    pub fn with_metadata_size_hint(mut self, metadata_size_hint: usize) -> Self {
        self.options.parquet_options.metadata_size_hint = Some(metadata_size_hint);
        self
    }

    /// Sets the gap threshold for merging nearby byte ranges into a single request.
    /// Ranges with gaps smaller than this value will be coalesced.
    ///
    /// Defaults to 1 MiB, matching object_store's OBJECT_STORE_COALESCE_DEFAULT.
    pub fn with_range_coalesce_bytes(mut self, range_coalesce_bytes: u64) -> Self {
        self.options.parquet_options.range_coalesce_bytes = range_coalesce_bytes;
        self
    }

    /// Sets the maximum number of merged byte ranges to fetch concurrently.
    ///
    /// Defaults to 10, matching object_store's OBJECT_STORE_COALESCE_PARALLEL.
    pub fn with_range_fetch_concurrency(mut self, range_fetch_concurrency: usize) -> Self {
        self.options.parquet_options.range_fetch_concurrency = range_fetch_concurrency;
        self
    }

    /// Replaces all reader settings while preserving local I/O and runtime resources.
    pub fn with_options(mut self, options: ArrowReaderOptions) -> Self {
        self.options = options;
        self
    }

    /// Replaces Parquet I/O settings while preserving other reader settings.
    pub fn with_parquet_options(mut self, options: ParquetReadOptions) -> Self {
        self.options.parquet_options = options;
        self
    }

    /// Build the ArrowReader.
    pub fn build(self) -> ArrowReader {
        let concurrency_limit_data_files = self
            .options
            .data_file_concurrency_limit
            .unwrap_or_else(|| available_parallelism().get());
        ArrowReader {
            batch_size: self.options.batch_size,
            file_io: self.file_io.clone(),
            delete_file_loader: CachingDeleteFileLoader::new(
                self.file_io.clone(),
                concurrency_limit_data_files,
                self.runtime.clone(),
            ),
            concurrency_limit_data_files,
            row_group_filtering_enabled: self.options.row_group_filtering_enabled,
            row_selection_enabled: self.options.row_selection_enabled,
            bloom_filter_enabled: self.options.bloom_filter_enabled,
            parquet_read_options: self.options.parquet_options,
        }
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
    fn partial_parquet_options_preserve_defaults() {
        let options: ArrowReaderOptions = serde_json::from_str(
            r#"{"parquet_options":{"range_fetch_concurrency":2,"future_option":true}}"#,
        )
        .unwrap();
        assert_eq!(
            options,
            ArrowReaderOptions::builder()
                .with_parquet_options(
                    ParquetReadOptions::builder()
                        .with_range_fetch_concurrency(2)
                        .build()
                )
                .build()
        );
    }

    #[test]
    fn parquet_override_preserves_reader_settings_and_allows_individual_overrides() {
        let reader = ArrowReaderBuilder::new(FileIO::new_with_memory(), test_runtime())
            .with_options(
                ArrowReaderOptions::builder()
                    .with_batch_size(Some(17))
                    .build(),
            )
            .with_parquet_options(
                ParquetReadOptions::builder()
                    .with_metadata_size_hint(Some(4096))
                    .with_range_fetch_concurrency(2)
                    .build(),
            )
            .with_range_fetch_concurrency(3)
            .build();
        assert_eq!(
            (reader.batch_size, reader.parquet_read_options),
            (
                Some(17),
                ParquetReadOptions::builder()
                    .with_metadata_size_hint(Some(4096))
                    .with_range_fetch_concurrency(3)
                    .build()
            )
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
                    .with_parquet_options(
                        ParquetReadOptions::builder()
                            .with_metadata_size_hint(Some(4096))
                            .with_range_coalesce_bytes(8192)
                            .with_range_fetch_concurrency(2)
                            .build(),
                    )
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
                if explicit {
                    (Some(4096), 8192, 2)
                } else {
                    (Some(512 * 1024), 1024 * 1024, 10)
                }
            );
        }
    }
}
