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

use super::{
    DEFAULT_METADATA_SIZE_HINT, DEFAULT_RANGE_COALESCE_BYTES, DEFAULT_RANGE_FETCH_CONCURRENCY,
};

/// Configures Arrow reads independently of file planning and local I/O resources.
///
/// ```no_run
/// # use iceberg::{Result, arrow::{ArrowReaderOptions, ParquetReadOptions}, table::Table};
/// # async fn read(table: &Table) -> Result<()> {
/// let options = ArrowReaderOptions::builder()
///     .with_batch_size(Some(1024))
///     .with_parquet_options(
///         ParquetReadOptions::builder()
///             .with_metadata_size_hint(512 * 1024)
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

impl ArrowReaderOptions {
    /// Returns the configured batch size.
    pub fn batch_size(&self) -> Option<usize> {
        self.batch_size
    }

    pub(super) fn parquet_options_mut(&mut self) -> &mut ParquetReadOptions {
        &mut self.parquet_options
    }

    pub(super) fn set_batch_size(&mut self, value: Option<usize>) {
        self.batch_size = value;
    }
    /// Returns the configured data file concurrency limit.
    pub fn data_file_concurrency_limit(&self) -> Option<usize> {
        self.data_file_concurrency_limit
    }

    pub(super) fn set_data_file_concurrency_limit(&mut self, value: Option<usize>) {
        self.data_file_concurrency_limit = value;
    }
    /// Returns whether row-group statistics filtering is enabled.
    pub fn row_group_filtering_enabled(&self) -> bool {
        self.row_group_filtering_enabled
    }

    pub(super) fn set_row_group_filtering_enabled(&mut self, value: bool) {
        self.row_group_filtering_enabled = value;
    }
    /// Returns whether page-index row selection is enabled.
    pub fn row_selection_enabled(&self) -> bool {
        self.row_selection_enabled
    }

    pub(super) fn set_row_selection_enabled(&mut self, value: bool) {
        self.row_selection_enabled = value;
    }
    /// Returns whether bloom-filter pruning is enabled.
    pub fn bloom_filter_enabled(&self) -> bool {
        self.bloom_filter_enabled
    }

    pub(super) fn set_bloom_filter_enabled(&mut self, value: bool) {
        self.bloom_filter_enabled = value;
    }
    /// Returns the configured parquet options.
    pub fn parquet_options(&self) -> &ParquetReadOptions {
        &self.parquet_options
    }

    pub(super) fn set_parquet_options(&mut self, value: ParquetReadOptions) {
        self.parquet_options = value;
    }
}

/// Options for tuning Parquet file I/O.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize, TypedBuilder)]
#[serde(default)]
#[builder(field_defaults(setter(prefix = "with_")))]
pub struct ParquetReadOptions {
    /// Number of bytes to prefetch for parsing the Parquet metadata.
    ///
    /// This hint can help reduce the number of fetch requests. For more details see the
    /// [ParquetMetaDataReader documentation](https://docs.rs/parquet/latest/parquet/file/metadata/struct.ParquetMetaDataReader.html#method.with_prefetch_hint).
    ///
    /// Defaults to 512 KiB, matching DataFusion's default `ParquetOptions::metadata_size_hint`.
    #[builder(default = Some(DEFAULT_METADATA_SIZE_HINT), setter(strip_option))]
    metadata_size_hint: Option<usize>,
    /// Gap threshold for merging nearby byte ranges into a single request.
    /// Ranges with gaps smaller than this value will be coalesced.
    ///
    /// Defaults to 1 MiB, matching object_store's `OBJECT_STORE_COALESCE_DEFAULT`.
    #[builder(default = DEFAULT_RANGE_COALESCE_BYTES)]
    range_coalesce_bytes: u64,
    /// Maximum number of merged byte ranges to fetch concurrently.
    ///
    /// Defaults to 10, matching object_store's `OBJECT_STORE_COALESCE_PARALLEL`.
    #[builder(default = DEFAULT_RANGE_FETCH_CONCURRENCY)]
    range_fetch_concurrency: usize,
    /// Whether to preload the column index when reading Parquet metadata.
    #[builder(default = true, setter(skip))]
    #[serde(skip)]
    preload_column_index: bool,
    /// Whether to preload the offset index when reading Parquet metadata.
    #[builder(default = true, setter(skip))]
    #[serde(skip)]
    preload_offset_index: bool,
    /// Whether to preload the page index when reading Parquet metadata.
    #[builder(default = false, setter(skip))]
    #[serde(skip)]
    preload_page_index: bool,
}

impl Default for ParquetReadOptions {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl ParquetReadOptions {
    pub(super) fn set_metadata_size_hint(&mut self, value: Option<usize>) {
        self.metadata_size_hint = value;
    }
    pub(super) fn set_range_coalesce_bytes(&mut self, value: u64) {
        self.range_coalesce_bytes = value;
    }
    pub(super) fn set_range_fetch_concurrency(&mut self, value: usize) {
        self.range_fetch_concurrency = value;
    }
    pub(super) fn set_preload_page_index(&mut self, value: bool) {
        self.preload_page_index = value;
    }

    /// Returns the configured metadata size hint.
    pub fn metadata_size_hint(&self) -> Option<usize> {
        self.metadata_size_hint
    }

    /// Returns the byte-gap threshold for coalescing read requests.
    pub fn range_coalesce_bytes(&self) -> u64 {
        self.range_coalesce_bytes
    }

    /// Returns the maximum number of concurrent range requests.
    pub fn range_fetch_concurrency(&self) -> usize {
        self.range_fetch_concurrency
    }

    pub(super) fn preload_column_index(&self) -> bool {
        self.preload_column_index
    }

    pub(super) fn preload_offset_index(&self) -> bool {
        self.preload_offset_index
    }

    pub(super) fn preload_page_index(&self) -> bool {
        self.preload_page_index
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::ArrowReaderBuilder;
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
                    .with_metadata_size_hint(4096)
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
                    .with_metadata_size_hint(4096)
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
                            .with_metadata_size_hint(4096)
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
