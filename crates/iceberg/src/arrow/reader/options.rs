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
/// Serializable alongside planned file tasks so readers can be constructed later.
/// In self-describing map formats such as JSON, field names are part of the
/// serialized format: missing fields use defaults and unknown fields are ignored.
/// Compatibility across different crate versions is not otherwise guaranteed.
///
/// ```no_run
/// # use iceberg::{Result, arrow::{ArrowReaderOptions, ParquetReadOptions}, table::Table};
/// # async fn read(table: &Table) -> Result<()> {
/// let options = ArrowReaderOptions::builder()
///     .with_batch_size(1024)
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
    #[builder(default = None, setter(strip_option))]
    batch_size: Option<usize>,
    /// Maximum in-flight data files, or the construction path's default when unset.
    #[builder(default = None, setter(strip_option))]
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
    /// Supplies the scan's fallback limit without replacing an explicit reader limit.
    pub(crate) fn with_default_data_file_concurrency_limit(mut self, limit: usize) -> Self {
        self.data_file_concurrency_limit.get_or_insert(limit);
        self
    }

    /// Returns the configured batch size.
    pub fn batch_size(&self) -> Option<usize> {
        self.batch_size
    }

    /// Returns the configured data file concurrency limit.
    ///
    /// When unset, `TableScan::to_arrow` inherits the scan's data-file limit, while
    /// direct reader construction uses execution-host parallelism. Zero is treated as one.
    pub fn data_file_concurrency_limit(&self) -> Option<usize> {
        self.data_file_concurrency_limit
    }

    /// Returns whether row-group statistics filtering is enabled.
    ///
    /// Enabled by default: statistics can skip groups that cannot match the
    /// predicate, generally improving performance with little overhead.
    pub fn row_group_filtering_enabled(&self) -> bool {
        self.row_group_filtering_enabled
    }

    /// Returns whether page-index row selection is enabled.
    ///
    /// Disabled by default: parsing page indexes can cost more than the reads
    /// it avoids. Evaluate it against the table's sorting, partitioning, row-group
    /// size, and page size before enabling it.
    pub fn row_selection_enabled(&self) -> bool {
        self.row_selection_enabled
    }

    /// Returns whether bloom-filter pruning is enabled.
    ///
    /// Disabled by default: bloom filters require extra reads per relevant
    /// column and row group. They can eliminate groups for equality and IN predicates.
    pub fn bloom_filter_enabled(&self) -> bool {
        self.bloom_filter_enabled
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
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, TypedBuilder)]
#[serde(default)]
#[builder(field_defaults(setter(prefix = "with_")))]
pub struct ParquetReadOptions {
    /// Number of bytes to prefetch for parsing the Parquet metadata.
    ///
    /// This hint can help reduce the number of fetch requests. For more details see the
    /// [ParquetMetaDataReader documentation](https://docs.rs/parquet/latest/parquet/file/metadata/struct.ParquetMetaDataReader.html#method.with_prefetch_hint).
    ///
    /// Defaults to 512 KiB, matching DataFusion's default `ParquetOptions::metadata_size_hint`.
    #[builder(default = DEFAULT_METADATA_SIZE_HINT)]
    metadata_size_hint: usize,
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
}

impl Default for ParquetReadOptions {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl ParquetReadOptions {
    /// Returns the configured metadata size hint.
    pub fn metadata_size_hint(&self) -> usize {
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
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::unset(None, 7)]
    #[case::explicit(Some(3), 3)]
    #[case::zero(Some(0), 0)]
    fn default_data_file_concurrency_preserves_explicit_options(
        #[case] configured: Option<usize>,
        #[case] expected: usize,
    ) {
        // Include unrelated settings to check that resolving concurrency preserves them.
        let options: ArrowReaderOptions = serde_json::from_value(serde_json::json!({
            "batch_size": 17,
            "data_file_concurrency_limit": configured,
            "row_selection_enabled": true,
        }))
        .unwrap();
        // Resolve the scan fallback; explicit zero is normalized later by the reader.
        let options = options.with_default_data_file_concurrency_limit(7);
        let expected = ArrowReaderOptions::builder()
            .with_batch_size(17)
            .with_data_file_concurrency_limit(expected)
            .with_row_selection_enabled(true)
            .build();
        assert_eq!(options, expected);
    }

    #[test]
    fn missing_options_use_defaults() {
        let decoded: ArrowReaderOptions = serde_json::from_str(r#"{"batch_size":17}"#).unwrap();
        assert_eq!(
            decoded,
            ArrowReaderOptions::builder().with_batch_size(17).build()
        );
    }

    #[test]
    fn unknown_options_are_ignored() {
        let decoded: ArrowReaderOptions =
            serde_json::from_str(r#"{"future_option":true}"#).unwrap();
        assert_eq!(decoded, ArrowReaderOptions::default());
    }

    #[test]
    fn unset_concurrency_serializes_as_null() {
        assert_eq!(
            serde_json::to_value(ArrowReaderOptions::default()).unwrap()["data_file_concurrency_limit"],
            serde_json::Value::Null,
        );
    }

    #[test]
    fn partial_parquet_options_preserve_defaults() {
        let options: ArrowReaderOptions =
            serde_json::from_str(r#"{"parquet_options":{"range_fetch_concurrency":2}}"#).unwrap();
        assert_eq!(
            options,
            ArrowReaderOptions::builder()
                .with_parquet_options(
                    ParquetReadOptions::builder()
                        .with_range_fetch_concurrency(2)
                        .build(),
                )
                .build()
        );
    }
}
