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

//! Typed access to Iceberg table properties.
//!
//! [`TableProperties`] exposes Iceberg's string-keyed table properties as typed public
//! fields. Its JSON representation is a flat object whose keys and values are strings.
//!
//! # Create from defaults
//!
//! Start with Iceberg's defaults and modify public fields directly:
//!
//! ```
//! use iceberg::spec::{DataFileFormat, TableProperties};
//!
//! let mut properties = TableProperties::default();
//! properties.write_format_default = DataFileFormat::Orc;
//! properties.write_data_path = Some("s3://warehouse/table/data".to_string());
//!
//! assert_eq!(properties.write_format_default, DataFileFormat::Orc);
//! ```
//!
//! # Deserialize from JSON
//!
//! JSON property values must be strings, matching Iceberg's table property map:
//!
//! ```
//! use iceberg::spec::{DataFileFormat, TableProperties};
//!
//! let properties: TableProperties = serde_json::from_value(serde_json::json!({
//!     "commit.retry.num-retries": "8",
//!     "write.format.default": "orc"
//! })).unwrap();
//!
//! assert_eq!(properties.commit_retry_num_retries, 8);
//! assert_eq!(properties.write_format_default, DataFileFormat::Orc);
//! ```
//!
//! # Serialize to JSON
//!
//! Serialization converts non-default typed fields back into Iceberg property keys. Fields whose
//! values match their defaults are omitted:
//!
//! ```
//! use iceberg::spec::TableProperties;
//!
//! let mut properties = TableProperties::default();
//! properties.commit_retry_num_retries = 8;
//! properties.write_data_path = Some("s3://warehouse/table/data".to_string());
//!
//! let json = serde_json::to_value(&properties).unwrap();
//! assert_eq!(json["commit.retry.num-retries"], "8");
//! assert_eq!(json["write.data.path"], "s3://warehouse/table/data");
//! assert!(json.get("write.format.default").is_none());
//! ```

use std::collections::HashMap;

use iceberg_property_macro::Properties;
use serde_with::{DeserializeFromStr, SerializeDisplay};

use crate::compression::CompressionCodec;
use crate::error::Result;
use crate::spec::{DataFileFormat, NameMapping};

/// Parquet data page version 1.
pub const PARQUET_PAGE_VERSION_V1: &str = "v1";

/// Parquet data page version 2.
pub const PARQUET_PAGE_VERSION_V2: &str = "v2";

/// ORC compression strategy that prioritizes speed.
pub const ORC_COMPRESSION_STRATEGY_SPEED: &str = "speed";

/// ORC compression strategy that prioritizes compression ratio.
pub const ORC_COMPRESSION_STRATEGY_COMPRESSION: &str = "compression";

/// Distribution applied to rows before writing files.
#[derive(
    Debug,
    PartialEq,
    Eq,
    Clone,
    Copy,
    SerializeDisplay,
    DeserializeFromStr,
    strum::Display,
    strum::EnumString,
)]
#[strum(ascii_case_insensitive, serialize_all = "kebab-case")]
pub enum DistributionMode {
    /// Do not redistribute rows.
    None,
    /// Hash-distribute rows by partition values.
    Hash,
    /// Range-distribute rows by partition or sort values.
    Range,
}

/// Granularity used when creating position delete files.
#[derive(
    Debug,
    PartialEq,
    Eq,
    Clone,
    Copy,
    SerializeDisplay,
    DeserializeFromStr,
    strum::Display,
    strum::EnumString,
)]
#[strum(ascii_case_insensitive, serialize_all = "kebab-case")]
pub enum DeleteGranularity {
    /// Group deletes for each referenced data file separately.
    File,
    /// Group deletes for different data files within a partition.
    Partition,
}

/// Isolation level used by row-level operations.
#[derive(
    Debug,
    PartialEq,
    Eq,
    Clone,
    Copy,
    SerializeDisplay,
    DeserializeFromStr,
    strum::Display,
    strum::EnumString,
)]
#[strum(ascii_case_insensitive, serialize_all = "kebab-case")]
pub enum IsolationLevel {
    /// Fail if concurrent changes may contain rows matching the operation.
    Serializable,
    /// Validate only against data visible in the operation's snapshot.
    Snapshot,
}

/// Strategy used to apply row-level changes.
#[derive(
    Debug,
    PartialEq,
    Eq,
    Clone,
    Copy,
    SerializeDisplay,
    DeserializeFromStr,
    strum::Display,
    strum::EnumString,
)]
#[strum(ascii_case_insensitive, serialize_all = "kebab-case")]
pub enum RowLevelOperationMode {
    /// Replace affected data files immediately.
    CopyOnWrite,
    /// Write delete files and merge changes while reading.
    MergeOnRead,
}

fn parse_comma_separated_strings(value: &str) -> Result<Vec<String>> {
    Ok(value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect())
}

fn serialize_comma_separated_strings(values: &[String]) -> String {
    values.join(",")
}

/// Typed Iceberg table properties organized into documented sections.
///
/// Serde represents this struct as Iceberg's flat string-to-string property map. Property
/// definitions and descriptions are based on the pinned [Java TableProperties implementation]
/// and [Apache Iceberg configuration documentation].
///
/// [Java TableProperties implementation]: https://github.com/apache/iceberg/blob/d8c10a1608170f0ba83be740d6ab0b6a3757cb3e/core/src/main/java/org/apache/iceberg/TableProperties.java
/// [Apache Iceberg configuration documentation]: https://github.com/apache/iceberg/blob/d8c10a1608170f0ba83be740d6ab0b6a3757cb3e/docs/docs/configuration.md
#[derive(Clone, Debug, Properties)]
pub struct TableProperties {
    // General properties.
    #[key = "comment"]
    #[default(None)]
    #[doc = "Table-level description of the table's business meaning and usage context."]
    pub comment: Option<String>,

    #[key = "identifier-fields.rely"]
    #[default(false)]
    #[doc = "Whether query engines may rely on identifier fields as a primary key for optimization; this is not enforced on writes."]
    pub identifier_fields_rely: bool,

    // Commit properties.
    #[key = "commit.retry.num-retries"]
    #[default(4)]
    #[doc = "Number of times to retry a commit before failing."]
    pub commit_retry_num_retries: usize,

    #[key = "commit.retry.min-wait-ms"]
    #[default(100)]
    #[doc = "Minimum time in milliseconds to wait before retrying a commit."]
    pub commit_retry_min_wait_ms: u64,

    #[key = "commit.retry.max-wait-ms"]
    #[default(60 * 1000)]
    #[doc = "Maximum time in milliseconds to wait before retrying a commit."]
    pub commit_retry_max_wait_ms: u64,

    #[key = "commit.retry.total-timeout-ms"]
    #[default(30 * 60 * 1000)]
    #[doc = "Total commit retry timeout in milliseconds."]
    pub commit_retry_total_timeout_ms: u64,

    #[key = "commit.status-check.num-retries"]
    #[default(3)]
    #[doc = "Number of times to check whether a commit succeeded after connectivity is lost."]
    pub commit_status_check_num_retries: usize,

    #[key = "commit.status-check.min-wait-ms"]
    #[default(1000)]
    #[doc = "Minimum time in milliseconds to wait before retrying a commit status check."]
    pub commit_status_check_min_wait_ms: u64,

    #[key = "commit.status-check.max-wait-ms"]
    #[default(60 * 1000)]
    #[doc = "Maximum time in milliseconds to wait before retrying a commit status check."]
    pub commit_status_check_max_wait_ms: u64,

    #[key = "commit.status-check.total-timeout-ms"]
    #[default(30 * 60 * 1000)]
    #[doc = "Total timeout in milliseconds in which commit status checking must succeed."]
    pub commit_status_check_total_timeout_ms: u64,

    // Manifest properties.
    #[key = "commit.manifest.target-size-bytes"]
    #[default(8 * 1024 * 1024)]
    #[doc = "Target size in bytes when merging manifest files."]
    pub commit_manifest_target_size_bytes: usize,

    #[key = "commit.manifest.min-count-to-merge"]
    #[default(100)]
    #[doc = "Minimum number of manifests to accumulate before merging."]
    pub commit_manifest_min_count_to_merge: usize,

    #[key = "commit.manifest-merge.enabled"]
    #[default(true)]
    #[doc = "Whether manifests are automatically merged during writes."]
    pub commit_manifest_merge_enabled: bool,

    #[key = "write.manifest.compression-codec"]
    #[additional_key = "write.manifest.compression-level"]
    #[default(CompressionCodec::gzip_default())]
    #[parse_properties_with(CompressionCodec::parse_properties)]
    #[write_properties_with(CompressionCodec::write_properties)]
    #[doc = "Compression codec used for manifest files."]
    pub write_manifest_compression_codec: CompressionCodec,

    #[key = "write.manifest-lists.enabled"]
    #[default(true)]
    #[doc = "Deprecated flag for writing manifest lists; manifest lists are always enabled."]
    pub write_manifest_lists_enabled: bool,

    // Write properties.
    #[key = "write.format.default"]
    #[default(DataFileFormat::Parquet)]
    #[doc = "Default data file format: Parquet, Avro, or ORC."]
    pub write_format_default: DataFileFormat,

    #[key = "write.delete.format.default"]
    #[default(DataFileFormat::Parquet)]
    #[doc = "Default delete file format: Parquet, Avro, or ORC."]
    pub write_delete_format_default: DataFileFormat,

    #[key = "write.target-file-size-bytes"]
    #[default(512 * 1024 * 1024)]
    #[doc = "Target size in bytes for generated data files."]
    pub write_target_file_size_bytes: usize,

    #[key = "write.delete.target-file-size-bytes"]
    #[default(64 * 1024 * 1024)]
    #[doc = "Target size in bytes for generated delete files."]
    pub write_delete_target_file_size_bytes: usize,

    #[key = "write.object-storage.enabled"]
    #[default(false)]
    #[doc = "Whether the object-storage location provider adds a hash component to file paths."]
    pub write_object_storage_enabled: bool,

    #[key = "write.object-storage.partitioned-paths"]
    #[default(true)]
    #[doc = "Whether object-storage file paths include partition values."]
    pub write_object_storage_partitioned_paths: bool,

    #[key = "write.object-storage.path"]
    #[default(None)]
    #[doc = "Deprecated base object-storage path; use write.data.path instead."]
    pub write_object_storage_path: Option<String>,

    #[key = "write.location-provider.impl"]
    #[default(None)]
    #[doc = "Optional custom location provider implementation."]
    pub write_location_provider_impl: Option<String>,

    #[key = "write.folder-storage.path"]
    #[default(None)]
    #[doc = "Deprecated base folder-storage path; use write.data.path instead."]
    pub write_folder_storage_path: Option<String>,

    #[key = "write.data.path"]
    #[default(None)]
    #[doc = "Base location for data files written after this property is set."]
    pub write_data_path: Option<String>,

    #[key = "write.wap.enabled"]
    #[default(false)]
    #[doc = "Whether write-audit-publish writes are enabled."]
    pub write_wap_enabled: bool,

    #[key = "write.distribution-mode"]
    #[default(DistributionMode::None)]
    #[doc = "Write distribution mode: none, hash, or range."]
    pub write_distribution_mode: DistributionMode,

    #[key = "write.datafusion.fanout.enabled"]
    #[default(true)]
    #[doc = "Whether DataFusion uses a fanout writer for partitioned tables."]
    pub write_datafusion_fanout_enabled: bool,

    // Parquet properties.
    #[key = "write.parquet.row-group-size-bytes"]
    #[default(128 * 1024 * 1024)]
    #[doc = "Parquet row group size in bytes for data files."]
    pub write_parquet_row_group_size_bytes: usize,

    #[key = "write.delete.parquet.row-group-size-bytes"]
    #[default(128 * 1024 * 1024)]
    #[doc = "Parquet row group size in bytes for delete files."]
    pub write_delete_parquet_row_group_size_bytes: usize,

    #[key = "write.parquet.page-size-bytes"]
    #[default(1024 * 1024)]
    #[doc = "Parquet page size in bytes for data files."]
    pub write_parquet_page_size_bytes: usize,

    #[key = "write.delete.parquet.page-size-bytes"]
    #[default(1024 * 1024)]
    #[doc = "Parquet page size in bytes for delete files."]
    pub write_delete_parquet_page_size_bytes: usize,

    #[key = "write.parquet.page-version"]
    #[default(PARQUET_PAGE_VERSION_V1)]
    #[doc = "Parquet data page version for data files: v1 or v2."]
    pub write_parquet_page_version: String,

    #[key = "write.delete.parquet.page-version"]
    #[default(PARQUET_PAGE_VERSION_V1)]
    #[doc = "Parquet data page version for delete files: v1 or v2."]
    pub write_delete_parquet_page_version: String,

    #[key = "write.parquet.page-row-limit"]
    #[default(20_000)]
    #[doc = "Maximum number of rows per Parquet page in data files."]
    pub write_parquet_page_row_limit: usize,

    #[key = "write.delete.parquet.page-row-limit"]
    #[default(20_000)]
    #[doc = "Maximum number of rows per Parquet page in delete files."]
    pub write_delete_parquet_page_row_limit: usize,

    #[key = "write.parquet.dict-size-bytes"]
    #[default(2 * 1024 * 1024)]
    #[doc = "Parquet dictionary page size in bytes for data files."]
    pub write_parquet_dict_size_bytes: usize,

    #[key = "write.delete.parquet.dict-size-bytes"]
    #[default(2 * 1024 * 1024)]
    #[doc = "Parquet dictionary page size in bytes for delete files."]
    pub write_delete_parquet_dict_size_bytes: usize,

    #[key = "write.parquet.compression-codec"]
    #[additional_key = "write.parquet.compression-level"]
    #[default(CompressionCodec::zstd_default())]
    #[parse_properties_with(CompressionCodec::parse_properties)]
    #[write_properties_with(CompressionCodec::write_properties)]
    #[doc = "Parquet compression codec used for data files."]
    pub write_parquet_compression_codec: CompressionCodec,

    #[key = "write.delete.parquet.compression-codec"]
    #[additional_key = "write.delete.parquet.compression-level"]
    #[default(CompressionCodec::zstd_default())]
    #[parse_properties_with(CompressionCodec::parse_properties)]
    #[write_properties_with(CompressionCodec::write_properties)]
    #[doc = "Parquet compression codec used for delete files."]
    pub write_delete_parquet_compression_codec: CompressionCodec,

    #[key = "write.parquet.shred-variants"]
    #[default(false)]
    #[doc = "Whether variant columns use shredded Parquet encoding for improved query performance."]
    pub write_parquet_shred_variants: bool,

    #[key = "write.parquet.variant-inference-buffer-size"]
    #[default(100)]
    #[doc = "Number of rows buffered for schema inference when variant shredding is enabled."]
    pub write_parquet_variant_inference_buffer_size: usize,

    #[key = "write.parquet.row-group-check-min-record-count"]
    #[default(100)]
    #[doc = "Minimum record count between Parquet data-file row group size checks."]
    pub write_parquet_row_group_check_min_record_count: usize,

    #[key = "write.delete.parquet.row-group-check-min-record-count"]
    #[default(100)]
    #[doc = "Minimum record count between Parquet delete-file row group size checks."]
    pub write_delete_parquet_row_group_check_min_record_count: usize,

    #[key = "write.parquet.row-group-check-max-record-count"]
    #[default(10_000)]
    #[doc = "Maximum record count between Parquet data-file row group size checks."]
    pub write_parquet_row_group_check_max_record_count: usize,

    #[key = "write.delete.parquet.row-group-check-max-record-count"]
    #[default(10_000)]
    #[doc = "Maximum record count between Parquet delete-file row group size checks."]
    pub write_delete_parquet_row_group_check_max_record_count: usize,

    #[key = "write.parquet.row-group-size-track-uncompressed"]
    #[default(false)]
    #[doc = "Whether uncompressed data size is tracked to enforce the Parquet row group target."]
    pub write_parquet_row_group_size_track_uncompressed: bool,

    #[key = "write.parquet.bloom-filter-max-bytes"]
    #[default(1024 * 1024)]
    #[doc = "Maximum number of bytes for a Parquet bloom filter bitset."]
    pub write_parquet_bloom_filter_max_bytes: usize,

    #[key = "write.parquet.bloom-filter-adaptive-enabled"]
    #[default(false)]
    #[doc = "Whether adaptive Parquet bloom filter sizing selects the smallest suitable filter."]
    pub write_parquet_bloom_filter_adaptive_enabled: bool,

    #[prefix = "write.parquet.bloom-filter-fpp.column."]
    #[default(HashMap::new())]
    #[doc = "Per-column Parquet bloom filter false-positive probabilities, keyed by column name."]
    pub write_parquet_bloom_filter_fpp_column: HashMap<String, f64>,

    #[prefix = "write.parquet.bloom-filter-ndv.column."]
    #[default(HashMap::new())]
    #[doc = "Per-column expected distinct-value counts for Parquet bloom filters."]
    pub write_parquet_bloom_filter_ndv_column: HashMap<String, u64>,

    #[prefix = "write.parquet.bloom-filter-enabled.column."]
    #[default(HashMap::new())]
    #[doc = "Per-column flags controlling whether Parquet bloom filters are written."]
    pub write_parquet_bloom_filter_enabled_column: HashMap<String, bool>,

    #[prefix = "write.parquet.stats-enabled.column."]
    #[default(HashMap::new())]
    #[doc = "Per-column flags controlling whether Parquet column statistics are collected."]
    pub write_parquet_stats_enabled_column: HashMap<String, bool>,

    #[prefix = "write.parquet.dict-encoding-enabled.column."]
    #[default(HashMap::new())]
    #[doc = "Per-column flags controlling whether Parquet dictionary encoding is used."]
    pub write_parquet_dict_encoding_enabled_column: HashMap<String, bool>,

    #[key = "write.parquet.content-defined-chunking.enabled"]
    #[default(false)]
    #[doc = "Whether Parquet content-defined chunking is enabled."]
    pub write_parquet_content_defined_chunking_enabled: bool,

    #[key = "write.parquet.content-defined-chunking.min-chunk-size"]
    #[default(256 * 1024)]
    #[doc = "Minimum Parquet content-defined chunk size in bytes."]
    pub write_parquet_content_defined_chunking_min_chunk_size: usize,

    #[key = "write.parquet.content-defined-chunking.max-chunk-size"]
    #[default(1024 * 1024)]
    #[doc = "Maximum Parquet content-defined chunk size in bytes."]
    pub write_parquet_content_defined_chunking_max_chunk_size: usize,

    #[key = "write.parquet.content-defined-chunking.norm-level"]
    #[default(0)]
    #[doc = "Gearhash normalization level used by Parquet content-defined chunking."]
    pub write_parquet_content_defined_chunking_norm_level: i32,

    // Avro properties.
    #[key = "write.avro.compression-codec"]
    #[additional_key = "write.avro.compression-level"]
    #[default(CompressionCodec::gzip_default())]
    #[parse_properties_with(CompressionCodec::parse_properties)]
    #[write_properties_with(CompressionCodec::write_properties)]
    #[doc = "Avro compression codec used for data files."]
    pub write_avro_compression_codec: CompressionCodec,

    #[key = "write.delete.avro.compression-codec"]
    #[additional_key = "write.delete.avro.compression-level"]
    #[default(CompressionCodec::gzip_default())]
    #[parse_properties_with(CompressionCodec::parse_properties)]
    #[write_properties_with(CompressionCodec::write_properties)]
    #[doc = "Avro compression codec used for delete files."]
    pub write_delete_avro_compression_codec: CompressionCodec,

    // ORC properties.
    #[key = "write.orc.stripe-size-bytes"]
    #[default(64 * 1024 * 1024)]
    #[doc = "Default ORC stripe size in bytes for data files."]
    pub write_orc_stripe_size_bytes: u64,

    #[key = "write.delete.orc.stripe-size-bytes"]
    #[default(64 * 1024 * 1024)]
    #[doc = "Default ORC stripe size in bytes for delete files."]
    pub write_delete_orc_stripe_size_bytes: u64,

    #[key = "write.orc.bloom.filter.columns"]
    #[default(Vec::new())]
    #[parse_with(parse_comma_separated_strings)]
    #[serialize_with(serialize_comma_separated_strings)]
    #[doc = "Comma-separated column names for which ORC bloom filters are created."]
    pub write_orc_bloom_filter_columns: Vec<String>,

    #[key = "write.orc.bloom.filter.fpp"]
    #[default(0.05)]
    #[doc = "False-positive probability for ORC bloom filters."]
    pub write_orc_bloom_filter_fpp: f64,

    #[key = "write.orc.block-size-bytes"]
    #[default(256 * 1024 * 1024)]
    #[doc = "Default file-system block size in bytes for ORC data files."]
    pub write_orc_block_size_bytes: u64,

    #[key = "write.delete.orc.block-size-bytes"]
    #[default(256 * 1024 * 1024)]
    #[doc = "Default file-system block size in bytes for ORC delete files."]
    pub write_delete_orc_block_size_bytes: u64,

    #[key = "write.orc.vectorized.batch-size"]
    #[default(1024)]
    #[doc = "ORC vectorized write batch size for data files."]
    pub write_orc_vectorized_batch_size: usize,

    #[key = "write.delete.orc.vectorized.batch-size"]
    #[default(1024)]
    #[doc = "ORC vectorized write batch size for delete files."]
    pub write_delete_orc_vectorized_batch_size: usize,

    #[key = "write.orc.compression-codec"]
    #[default(CompressionCodec::Zlib)]
    #[parse_with(CompressionCodec::parse_property)]
    #[serialize_with(CompressionCodec::property_value)]
    #[doc = "ORC compression codec used for data files."]
    pub write_orc_compression_codec: CompressionCodec,

    #[key = "write.delete.orc.compression-codec"]
    #[default(CompressionCodec::Zlib)]
    #[parse_with(CompressionCodec::parse_property)]
    #[serialize_with(CompressionCodec::property_value)]
    #[doc = "ORC compression codec used for delete files."]
    pub write_delete_orc_compression_codec: CompressionCodec,

    #[key = "write.orc.compression-strategy"]
    #[default(ORC_COMPRESSION_STRATEGY_SPEED)]
    #[doc = "ORC compression strategy for data files: speed or compression."]
    pub write_orc_compression_strategy: String,

    #[key = "write.delete.orc.compression-strategy"]
    #[default(ORC_COMPRESSION_STRATEGY_SPEED)]
    #[doc = "ORC compression strategy for delete files: speed or compression."]
    pub write_delete_orc_compression_strategy: String,

    // Read properties.
    #[key = "read.split.target-size"]
    #[default(128 * 1024 * 1024)]
    #[doc = "Target size in bytes when combining data input splits."]
    pub read_split_target_size: u64,

    #[key = "read.split.metadata-target-size"]
    #[default(32 * 1024 * 1024)]
    #[doc = "Target size in bytes when combining metadata input splits."]
    pub read_split_metadata_target_size: u64,

    #[key = "read.split.planning-lookback"]
    #[default(10)]
    #[doc = "Number of bins considered when combining input splits."]
    pub read_split_planning_lookback: usize,

    #[key = "read.split.open-file-cost"]
    #[default(4 * 1024 * 1024)]
    #[doc = "Estimated file-open cost used as a minimum weight when combining splits."]
    pub read_split_open_file_cost: u64,

    #[key = "read.split.adaptive-size.enabled"]
    #[default(true)]
    #[doc = "Whether split size is adapted to the scan size."]
    pub read_split_adaptive_size_enabled: bool,

    #[key = "read.parquet.vectorization.enabled"]
    #[default(true)]
    #[doc = "Whether Parquet vectorized reads are enabled."]
    pub read_parquet_vectorization_enabled: bool,

    #[key = "read.parquet.vectorization.batch-size"]
    #[default(5000)]
    #[doc = "Batch size for Parquet vectorized reads."]
    pub read_parquet_vectorization_batch_size: usize,

    #[key = "read.orc.vectorization.enabled"]
    #[default(false)]
    #[doc = "Whether ORC vectorized reads are enabled."]
    pub read_orc_vectorization_enabled: bool,

    #[key = "read.orc.vectorization.batch-size"]
    #[default(5000)]
    #[doc = "Batch size for ORC vectorized reads."]
    pub read_orc_vectorization_batch_size: usize,

    #[key = "read.data-planning-mode"]
    #[default("auto")]
    #[doc = "Planning mode used for data files."]
    pub read_data_planning_mode: String,

    #[key = "read.delete-planning-mode"]
    #[default("auto")]
    #[doc = "Planning mode used for delete files."]
    pub read_delete_planning_mode: String,

    // Metadata properties.
    #[key = "write.metadata.path"]
    #[default(None)]
    #[doc = "Base location for metadata files written after this property is set."]
    pub write_metadata_path: Option<String>,

    #[key = "write.summary.partition-limit"]
    #[default(0)]
    #[doc = "Maximum changed-partition count for including partition-level statistics in snapshot summaries."]
    pub write_summary_partition_limit: u64,

    #[key = "write.metadata.compression-codec"]
    #[default(CompressionCodec::None)]
    #[parse_with(CompressionCodec::parse_metadata_property)]
    #[serialize_with(CompressionCodec::property_value)]
    #[doc = "Compression codec for metadata JSON files: none or gzip."]
    pub write_metadata_compression_codec: CompressionCodec,

    #[key = "write.metadata.previous-versions-max"]
    #[default(100)]
    #[doc = "Maximum number of previous metadata file versions to track."]
    pub write_metadata_previous_versions_max: usize,

    #[key = "write.metadata.delete-after-commit.enabled"]
    #[default(false)]
    #[doc = "Whether the oldest tracked metadata file is deleted after each commit."]
    pub write_metadata_delete_after_commit_enabled: bool,

    #[key = "write.metadata.metrics.max-inferred-column-defaults"]
    #[default(100)]
    #[doc = "Maximum number of columns that receive inferred metrics defaults."]
    pub write_metadata_metrics_max_inferred_column_defaults: usize,

    #[prefix = "write.metadata.metrics.column."]
    #[default(HashMap::new())]
    #[doc = "Per-column metrics modes keyed by column name."]
    pub write_metadata_metrics_column: HashMap<String, String>,

    #[key = "write.metadata.metrics.default"]
    #[default("truncate(16)")]
    #[doc = "Default metrics mode for table columns."]
    pub write_metadata_metrics_default: String,

    #[key = "schema.name-mapping.default"]
    #[default(None)]
    #[doc = "Default JSON name mapping used to resolve columns in files without field IDs."]
    pub schema_name_mapping_default: Option<NameMapping>,

    // Compatibility properties.
    #[key = "write.spark.fanout.enabled"]
    #[default(false)]
    #[doc = "Deprecated Spark fanout-writer flag; the fanout writer accepts unclustered data but uses more memory."]
    pub write_spark_fanout_enabled: bool,

    #[key = "write.spark.accept-any-schema"]
    #[default(false)]
    #[doc = "Deprecated Spark flag allowing writes with any compatible schema."]
    pub write_spark_accept_any_schema: bool,

    #[key = "write.spark.auto-schema-evolution.enabled"]
    #[default(true)]
    #[doc = "Deprecated Spark flag enabling automatic schema evolution during writes."]
    pub write_spark_auto_schema_evolution_enabled: bool,

    #[key = "write.spark.advisory-partition-size-bytes"]
    #[default(None)]
    #[doc = "Deprecated Spark advisory partition size in bytes."]
    pub write_spark_advisory_partition_size_bytes: Option<u64>,

    #[key = "compatibility.snapshot-id-inheritance.enabled"]
    #[default(false)]
    #[doc = "Whether snapshots may be committed without explicit snapshot IDs; format version 2 and later always allow this."]
    pub compatibility_snapshot_id_inheritance_enabled: bool,

    #[key = "engine.hive.enabled"]
    #[default(false)]
    #[doc = "Whether Hive engine integration behavior is enabled."]
    pub engine_hive_enabled: bool,

    #[key = "engine.hive.lock-enabled"]
    #[default(true)]
    #[doc = "Whether Hive locking is enabled."]
    pub engine_hive_lock_enabled: bool,

    // History properties.
    #[key = "gc.enabled"]
    #[default(true)]
    #[doc = "Whether garbage collection operations such as snapshot expiration and orphan-file removal are allowed."]
    pub gc_enabled: bool,

    #[key = "history.expire.max-snapshot-age-ms"]
    #[default(5 * 24 * 60 * 60 * 1000)]
    #[doc = "Default maximum snapshot age in milliseconds while expiring snapshots."]
    pub history_expire_max_snapshot_age_ms: i64,

    #[key = "history.expire.min-snapshots-to-keep"]
    #[default(1)]
    #[doc = "Default minimum number of snapshots retained per branch while expiring snapshots."]
    pub history_expire_min_snapshots_to_keep: usize,

    #[key = "history.expire.max-ref-age-ms"]
    #[default(i64::MAX)]
    #[doc = "Default maximum age in milliseconds for snapshot references other than the main branch."]
    pub history_expire_max_ref_age_ms: i64,

    // Row-level operation properties.
    #[key = "write.delete.granularity"]
    #[default(DeleteGranularity::Partition)]
    #[doc = "Granularity of generated delete files: partition or file."]
    pub write_delete_granularity: DeleteGranularity,

    #[key = "write.delete.isolation-level"]
    #[default(IsolationLevel::Serializable)]
    #[doc = "Isolation level for delete commands: serializable or snapshot."]
    pub write_delete_isolation_level: IsolationLevel,

    #[key = "write.delete.mode"]
    #[default(RowLevelOperationMode::CopyOnWrite)]
    #[doc = "Execution mode for delete commands: copy-on-write or merge-on-read."]
    pub write_delete_mode: RowLevelOperationMode,

    #[key = "write.delete.distribution-mode"]
    #[default(DistributionMode::None)]
    #[doc = "Distribution mode for delete command data."]
    pub write_delete_distribution_mode: DistributionMode,

    #[key = "write.update.isolation-level"]
    #[default(IsolationLevel::Serializable)]
    #[doc = "Isolation level for update commands: serializable or snapshot."]
    pub write_update_isolation_level: IsolationLevel,

    #[key = "write.update.mode"]
    #[default(RowLevelOperationMode::CopyOnWrite)]
    #[doc = "Execution mode for update commands: copy-on-write or merge-on-read."]
    pub write_update_mode: RowLevelOperationMode,

    #[key = "write.update.distribution-mode"]
    #[default(DistributionMode::None)]
    #[doc = "Distribution mode for update command data."]
    pub write_update_distribution_mode: DistributionMode,

    #[key = "write.merge.isolation-level"]
    #[default(IsolationLevel::Serializable)]
    #[doc = "Isolation level for merge commands: serializable or snapshot."]
    pub write_merge_isolation_level: IsolationLevel,

    #[key = "write.merge.mode"]
    #[default(RowLevelOperationMode::CopyOnWrite)]
    #[doc = "Execution mode for merge commands: copy-on-write or merge-on-read."]
    pub write_merge_mode: RowLevelOperationMode,

    #[key = "write.merge.distribution-mode"]
    #[default(DistributionMode::None)]
    #[doc = "Distribution mode for merge command data."]
    pub write_merge_distribution_mode: DistributionMode,

    #[key = "write.upsert.enabled"]
    #[default(false)]
    #[doc = "Whether upsert behavior is enabled."]
    pub write_upsert_enabled: bool,

    // Encryption properties.
    #[key = "encryption.key-id"]
    #[default(None)]
    #[doc = "Identifier of the table's master encryption key."]
    pub encryption_key_id: Option<String>,

    #[key = "encryption.data-key-length"]
    #[default(16)]
    #[doc = "Length in bytes of data-encryption keys; valid AES lengths are 16, 24, and 32 bytes."]
    pub encryption_data_key_length: usize,
}

impl TableProperties {
    /// Property key for the number of commit retries.
    pub const PROPERTY_COMMIT_NUM_RETRIES: &str = "commit.retry.num-retries";

    /// Default number of commit retries.
    pub const PROPERTY_COMMIT_NUM_RETRIES_DEFAULT: usize = 4;

    /// Property key for enabling the DataFusion fanout writer.
    pub const PROPERTY_DATAFUSION_WRITE_FANOUT_ENABLED: &str = "write.datafusion.fanout.enabled";

    /// Default value for enabling the DataFusion fanout writer.
    pub const PROPERTY_DATAFUSION_WRITE_FANOUT_ENABLED_DEFAULT: bool = true;

    /// Property key for the table encryption key identifier.
    pub const PROPERTY_ENCRYPTION_KEY_ID: &str = "encryption.key-id";

    /// Property key for the metadata compression codec.
    pub const PROPERTY_METADATA_COMPRESSION_CODEC: &str = "write.metadata.compression-codec";

    /// Property key for the maximum number of previous metadata versions to keep.
    pub const PROPERTY_METADATA_PREVIOUS_VERSIONS_MAX: &str =
        "write.metadata.previous-versions-max";

    /// Default maximum number of previous metadata versions to keep.
    pub const PROPERTY_METADATA_PREVIOUS_VERSIONS_MAX_DEFAULT: usize = 100;

    /// Property key for enabling Parquet content-defined chunking.
    pub const PROPERTY_PARQUET_CDC_ENABLED: &str = "write.parquet.content-defined-chunking.enabled";

    /// Property key for the maximum Parquet content-defined chunk size.
    pub const PROPERTY_PARQUET_CDC_MAX_CHUNK_SIZE: &str =
        "write.parquet.content-defined-chunking.max-chunk-size";

    /// Property key for the minimum Parquet content-defined chunk size.
    pub const PROPERTY_PARQUET_CDC_MIN_CHUNK_SIZE: &str =
        "write.parquet.content-defined-chunking.min-chunk-size";

    /// Property key for the Parquet content-defined chunking normalization level.
    pub const PROPERTY_PARQUET_CDC_NORM_LEVEL: &str =
        "write.parquet.content-defined-chunking.norm-level";

    /// Property key for the base data-file location.
    pub const PROPERTY_WRITE_DATA_LOCATION: &str = "write.data.path";

    /// Property key for the deprecated folder-storage location.
    pub const PROPERTY_WRITE_FOLDER_STORAGE_LOCATION: &str = "write.folder-storage.path";

    /// Property key for the base metadata-file location.
    pub const PROPERTY_WRITE_METADATA_PATH: &str = "write.metadata.path";

    /// Property key for the deprecated object-storage location.
    pub const PROPERTY_WRITE_OBJECT_STORAGE_LOCATION: &str = "write.object-storage.path";

    /// Property key for including partition values in object-storage paths.
    pub const PROPERTY_WRITE_OBJECT_STORAGE_PARTITIONED_PATHS: &str =
        "write.object-storage.partitioned-paths";

    /// Property key for the snapshot-summary partition limit.
    pub const PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT: &str = "write.summary.partition-limit";

    /// Default snapshot-summary partition limit.
    pub const PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT: u64 = 0;

    /// Property key for the target data-file size.
    pub const PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES: &str = "write.target-file-size-bytes";

    /// Default target data-file size.
    pub const PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT: usize = 512 * 1024 * 1024;

    /// Reserved table properties that must not be persisted in table metadata.
    pub const RESERVED_PROPERTIES: [&str; 9] = [
        "format-version",
        "uuid",
        "snapshot-count",
        "current-snapshot-id",
        "current-snapshot-summary",
        "current-snapshot-timestamp-ms",
        "current-schema",
        "default-partition-spec",
        "default-sort-order",
    ];
}

impl TryFrom<&HashMap<String, String>> for TableProperties {
    type Error = crate::Error;

    fn try_from(properties: &HashMap<String, String>) -> Result<Self> {
        Self::from_properties(properties)
            .map_err(|error| crate::Error::new(crate::ErrorKind::DataInvalid, error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{Error, ErrorKind};
    use crate::spec::MappedField;

    fn parse(properties: HashMap<String, String>) -> Result<TableProperties> {
        serde_json::from_value(serde_json::to_value(properties).unwrap())
            .map_err(|error| Error::new(ErrorKind::DataInvalid, error.to_string()))
    }

    #[test]
    fn creates_properties_from_defaults() {
        let properties = TableProperties::default();

        assert_eq!(properties.commit_retry_num_retries, 4);
        assert_eq!(properties.write_format_default, DataFileFormat::Parquet);
        assert_eq!(
            properties.write_manifest_compression_codec,
            CompressionCodec::gzip_default()
        );
        assert_eq!(
            properties.write_parquet_compression_codec,
            CompressionCodec::zstd_default()
        );
        assert_eq!(
            properties.write_avro_compression_codec,
            CompressionCodec::gzip_default()
        );
        assert_eq!(
            properties.write_orc_compression_codec,
            CompressionCodec::Zlib
        );
        assert_eq!(properties.write_distribution_mode, DistributionMode::None);
        assert_eq!(
            properties.write_delete_distribution_mode,
            DistributionMode::None
        );
        assert_eq!(
            properties.write_update_distribution_mode,
            DistributionMode::None
        );
        assert_eq!(
            properties.write_merge_distribution_mode,
            DistributionMode::None
        );
        assert_eq!(
            properties.write_parquet_page_version,
            PARQUET_PAGE_VERSION_V1
        );
        assert_eq!(
            properties.write_delete_parquet_page_version,
            PARQUET_PAGE_VERSION_V1
        );
        assert_eq!(PARQUET_PAGE_VERSION_V2, "v2");
        assert_eq!(
            properties.write_orc_compression_strategy,
            ORC_COMPRESSION_STRATEGY_SPEED
        );
        assert_eq!(ORC_COMPRESSION_STRATEGY_COMPRESSION, "compression");
        assert!(properties.write_orc_bloom_filter_columns.is_empty());
        assert_eq!(properties.schema_name_mapping_default, None);
        assert_eq!(
            properties.write_delete_granularity,
            DeleteGranularity::Partition
        );
        assert_eq!(
            properties.write_delete_isolation_level,
            IsolationLevel::Serializable
        );
        assert_eq!(
            properties.write_delete_mode,
            RowLevelOperationMode::CopyOnWrite
        );
        assert_eq!(
            properties.write_parquet_row_group_size_bytes,
            128 * 1024 * 1024
        );
        assert_eq!(properties.read_split_target_size, 128 * 1024 * 1024);
        assert!(properties.gc_enabled);
        assert_eq!(properties.encryption_data_key_length, 16);
    }

    #[test]
    fn serializes_to_flat_json_object() {
        let properties = TableProperties {
            commit_retry_num_retries: 9,
            write_format_default: DataFileFormat::Orc,
            write_data_path: Some("s3://warehouse/table/data".to_string()),
            write_manifest_compression_codec: CompressionCodec::Gzip(9),
            write_parquet_compression_codec: CompressionCodec::Zstd(5),
            write_delete_avro_compression_codec: CompressionCodec::Gzip(4),
            write_distribution_mode: DistributionMode::Range,
            write_orc_compression_codec: CompressionCodec::Lzo,
            write_orc_bloom_filter_columns: vec!["id".to_string(), "category".to_string()],
            schema_name_mapping_default: Some(NameMapping::new(vec![MappedField::new(
                Some(1),
                vec!["id".to_string()],
                vec![],
            )])),
            write_delete_granularity: DeleteGranularity::File,
            write_delete_isolation_level: IsolationLevel::Snapshot,
            write_delete_mode: RowLevelOperationMode::MergeOnRead,
            write_update_distribution_mode: DistributionMode::Hash,
            write_parquet_bloom_filter_fpp_column: HashMap::from([(
                "customer_id".to_string(),
                0.02,
            )]),
            write_parquet_bloom_filter_ndv_column: HashMap::from([(
                "customer_id".to_string(),
                1_000_000,
            )]),
            ..Default::default()
        };

        let json = serde_json::to_value(&properties).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "commit.retry.num-retries": "9",
                "schema.name-mapping.default": r#"[{"field-id":1,"names":["id"]}]"#,
                "write.data.path": "s3://warehouse/table/data",
                "write.delete.avro.compression-codec": "gzip",
                "write.delete.avro.compression-level": "4",
                "write.delete.granularity": "file",
                "write.delete.isolation-level": "snapshot",
                "write.delete.mode": "merge-on-read",
                "write.distribution-mode": "range",
                "write.format.default": "orc",
                "write.manifest.compression-codec": "gzip",
                "write.manifest.compression-level": "9",
                "write.orc.bloom.filter.columns": "id,category",
                "write.orc.compression-codec": "lzo",
                "write.parquet.bloom-filter-fpp.column.customer_id": "0.02",
                "write.parquet.bloom-filter-ndv.column.customer_id": "1000000",
                "write.parquet.compression-codec": "zstd",
                "write.parquet.compression-level": "5",
                "write.update.distribution-mode": "hash"
            })
        );
    }

    #[test]
    fn omits_default_values_when_serializing() {
        assert_eq!(
            serde_json::to_value(TableProperties::default()).unwrap(),
            serde_json::json!({})
        );
    }

    #[test]
    fn deserializes_from_flat_json_object() {
        let properties: TableProperties = serde_json::from_value(serde_json::json!({
            "commit.retry.num-retries": "8",
            "write.format.default": "orc",
            "write.data.path": "s3://warehouse/table/data",
            "write.manifest.compression-codec": "gzip",
            "write.manifest.compression-level": "8",
            "write.parquet.compression-level": "5",
            "write.distribution-mode": "HASH",
            "write.object-storage.partitioned-paths": "FALSE",
            "write.orc.bloom.filter.columns": "id, category",
            "write.parquet.bloom-filter-ndv.column.customer_id": "1000000",
            "schema.name-mapping.default": r#"[{"field-id":1,"names":["id"]}]"#,
            "write.delete.granularity": "FILE",
            "write.update.isolation-level": "snapshot",
            "write.merge.mode": "merge-on-read"
        }))
        .unwrap();

        assert_eq!(properties.commit_retry_num_retries, 8);
        assert_eq!(properties.write_format_default, DataFileFormat::Orc);
        assert_eq!(
            properties.write_data_path,
            Some("s3://warehouse/table/data".to_string())
        );
        assert_eq!(properties.write_distribution_mode, DistributionMode::Hash);
        assert!(!properties.write_object_storage_partitioned_paths);
        assert_eq!(
            properties.write_manifest_compression_codec,
            CompressionCodec::Gzip(8)
        );
        assert_eq!(
            properties.write_parquet_compression_codec,
            CompressionCodec::Zstd(5)
        );
        assert_eq!(properties.write_orc_bloom_filter_columns, vec![
            "id".to_string(),
            "category".to_string()
        ]);
        assert_eq!(
            properties.write_parquet_bloom_filter_ndv_column,
            HashMap::from([("customer_id".to_string(), 1_000_000)])
        );
        assert_eq!(
            properties.schema_name_mapping_default,
            Some(NameMapping::new(vec![MappedField::new(
                Some(1),
                vec!["id".to_string()],
                vec![],
            )]))
        );
        assert_eq!(properties.write_delete_granularity, DeleteGranularity::File);
        assert_eq!(
            properties.write_update_isolation_level,
            IsolationLevel::Snapshot
        );
        assert_eq!(
            properties.write_merge_mode,
            RowLevelOperationMode::MergeOnRead
        );
    }

    #[test]
    fn invalid_leaf_value_reports_its_property_key() {
        let error = parse(HashMap::from([(
            "commit.retry.num-retries".to_string(),
            "not-a-number".to_string(),
        )]))
        .unwrap_err();

        assert!(error.message().contains("commit.retry.num-retries"));
    }
}
