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
//! [`ParsedTableProperties`] organizes Iceberg's string-keyed table properties into typed groups
//! such as [`TableWriteProperties`] and [`TableCommitProperties`]. The Rust representation is
//! nested, while its JSON representation remains a flat object whose keys and values are strings.
//!
//! # Create from defaults
//!
//! Start with Iceberg's defaults and modify public fields directly:
//!
//! ```
//! use iceberg::spec::{DataFileFormat, ParsedTableProperties};
//!
//! let mut properties = ParsedTableProperties::default();
//! properties.write.format_default = DataFileFormat::Orc;
//! properties.write.data_location = Some("s3://warehouse/table/data".to_string());
//!
//! assert_eq!(properties.write.format_default, DataFileFormat::Orc);
//! ```
//!
//! # Deserialize from JSON
//!
//! JSON property values must be strings, matching Iceberg's table property map:
//!
//! ```
//! use iceberg::spec::{DataFileFormat, ParsedTableProperties};
//!
//! # fn main() -> Result<(), serde_json::Error> {
//! let properties: ParsedTableProperties = serde_json::from_value(serde_json::json!({
//!     "commit.retry.num-retries": "8",
//!     "write.format.default": "orc"
//! }))?;
//!
//! assert_eq!(properties.commit.num_retries, 8);
//! assert_eq!(properties.write.format_default, DataFileFormat::Orc);
//! # Ok(())
//! # }
//! ```
//!
//! # Serialize to JSON
//!
//! Serialization flattens the nested groups back into Iceberg property keys:
//!
//! ```
//! use iceberg::spec::ParsedTableProperties;
//!
//! # fn main() -> Result<(), serde_json::Error> {
//! let mut properties = ParsedTableProperties::default();
//! properties.commit.num_retries = 8;
//! properties.write.data_location = Some("s3://warehouse/table/data".to_string());
//!
//! let json = serde_json::to_value(&properties)?;
//! assert_eq!(json["commit.retry.num-retries"], "8");
//! assert_eq!(json["write.data.path"], "s3://warehouse/table/data");
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;

use iceberg_property_macro::Properties;

use crate::compression::CompressionCodec;
use crate::error::{Error, ErrorKind, Result};
use crate::spec::DataFileFormat;

/// Strips trailing slashes from a location, preserving a bare URI scheme root.
fn strip_trailing_slash(path: &str) -> &str {
    let mut path = path;
    while !path.ends_with("://") {
        let Some(stripped) = path.strip_suffix('/') else {
            break;
        };
        path = stripped;
    }
    path
}

fn parse_metadata_location(value: &str) -> Result<Option<String>> {
    if value.is_empty() {
        return Err(Error::new(ErrorKind::DataInvalid, "path must not be empty"));
    }

    Ok(Some(strip_trailing_slash(value).to_string()))
}

fn parse_metadata_file_compression(value: &str) -> Result<CompressionCodec> {
    if value.is_empty() {
        return Ok(CompressionCodec::None);
    }

    let codec: CompressionCodec = serde_json::from_value(serde_json::Value::String(
        value.to_lowercase(),
    ))
    .map_err(|_| {
        Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Invalid metadata compression codec: {value}. Only '{}' and '{}' are supported.",
                CompressionCodec::None.name(),
                CompressionCodec::gzip_default().name()
            ),
        )
    })?;

    match codec {
        CompressionCodec::None | CompressionCodec::Gzip(_) => Ok(codec),
        _ => Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Invalid metadata compression codec: {value}. Only '{}' and '{}' are supported for metadata files.",
                CompressionCodec::None.name(),
                CompressionCodec::gzip_default().name()
            ),
        )),
    }
}

fn serialize_compression_codec(codec: &CompressionCodec) -> String {
    codec.name().to_string()
}

/// Typed Iceberg table properties organized by behavior and file format.
///
/// Serde represents this hierarchy as Iceberg's flat string-to-string property map. Property
/// definitions and descriptions are based on the pinned [Java TableProperties implementation]
/// and [Apache Iceberg configuration documentation].
///
/// [Java TableProperties implementation]: https://github.com/apache/iceberg/blob/d8c10a1608170f0ba83be740d6ab0b6a3757cb3e/core/src/main/java/org/apache/iceberg/TableProperties.java
/// [Apache Iceberg configuration documentation]: https://github.com/apache/iceberg/blob/d8c10a1608170f0ba83be740d6ab0b6a3757cb3e/docs/docs/configuration.md
#[derive(Clone, Debug, Properties)]
pub struct ParsedTableProperties {
    #[nested]
    #[doc = "Informational properties and identifier-field semantics."]
    pub general: TableGeneralProperties,
    #[nested]
    #[doc = "Commit retry and commit status-check behavior."]
    pub commit: TableCommitProperties,
    #[nested]
    #[doc = "Manifest creation, merging, and compression behavior."]
    pub manifest: TableManifestProperties,
    #[nested]
    #[doc = "General table write behavior and output locations."]
    pub write: TableWriteProperties,
    #[nested]
    #[doc = "Parquet data and delete file behavior."]
    pub parquet: TableParquetProperties,
    #[nested]
    #[doc = "Avro data and delete file behavior."]
    pub avro: TableAvroProperties,
    #[nested]
    #[doc = "ORC data and delete file behavior."]
    pub orc: TableOrcProperties,
    #[nested]
    #[doc = "Read split planning and vectorization behavior."]
    pub read: TableReadProperties,
    #[nested]
    #[doc = "Table metadata retention, compression, and metrics behavior."]
    pub metadata: TableMetadataProperties,
    #[nested]
    #[doc = "Engine-specific and compatibility behavior."]
    pub compatibility: TableCompatibilityProperties,
    #[nested]
    #[doc = "Garbage collection and snapshot retention behavior."]
    pub history: TableHistoryProperties,
    #[nested]
    #[doc = "Delete, update, merge, and upsert behavior."]
    pub row_level: TableRowLevelProperties,
    #[nested]
    #[doc = "Table encryption behavior."]
    pub encryption: TableEncryptionProperties,
}

/// Informational properties and identifier-field semantics.
#[derive(Clone, Debug, Properties)]
pub struct TableGeneralProperties {
    #[key = "comment"]
    #[default(None)]
    #[doc = "Table-level description of the table's business meaning and usage context."]
    pub comment: Option<String>,
    #[key = "identifier-fields.rely"]
    #[default(false)]
    #[doc = "Whether query engines may rely on identifier fields as a primary key for optimization; this is not enforced on writes."]
    pub identifier_fields_rely: bool,
}

/// Commit retries and post-failure status checks.
#[derive(Clone, Debug, Properties)]
pub struct TableCommitProperties {
    #[key = "commit.retry.num-retries"]
    #[default(4)]
    #[doc = "Number of times to retry a commit before failing."]
    pub num_retries: usize,
    #[key = "commit.retry.min-wait-ms"]
    #[default(100)]
    #[doc = "Minimum time in milliseconds to wait before retrying a commit."]
    pub min_retry_wait_ms: u64,
    #[key = "commit.retry.max-wait-ms"]
    #[default(60 * 1000)]
    #[doc = "Maximum time in milliseconds to wait before retrying a commit."]
    pub max_retry_wait_ms: u64,
    #[key = "commit.retry.total-timeout-ms"]
    #[default(30 * 60 * 1000)]
    #[doc = "Total commit retry timeout in milliseconds."]
    pub total_retry_timeout_ms: u64,
    #[key = "commit.status-check.num-retries"]
    #[default(3)]
    #[doc = "Number of times to check whether a commit succeeded after connectivity is lost."]
    pub num_status_checks: usize,
    #[key = "commit.status-check.min-wait-ms"]
    #[default(1000)]
    #[doc = "Minimum time in milliseconds to wait before retrying a commit status check."]
    pub status_checks_min_wait_ms: u64,
    #[key = "commit.status-check.max-wait-ms"]
    #[default(60 * 1000)]
    #[doc = "Maximum time in milliseconds to wait before retrying a commit status check."]
    pub status_checks_max_wait_ms: u64,
    #[key = "commit.status-check.total-timeout-ms"]
    #[default(30 * 60 * 1000)]
    #[doc = "Total timeout in milliseconds in which commit status checking must succeed."]
    pub status_checks_total_wait_ms: u64,
}

/// Manifest creation, merging, and compression properties.
#[derive(Clone, Debug, Properties)]
pub struct TableManifestProperties {
    #[key = "commit.manifest.target-size-bytes"]
    #[default(8 * 1024 * 1024)]
    #[doc = "Target size in bytes when merging manifest files."]
    pub target_size_bytes: usize,
    #[key = "commit.manifest.min-count-to-merge"]
    #[default(100)]
    #[doc = "Minimum number of manifests to accumulate before merging."]
    pub min_merge_count: usize,
    #[key = "commit.manifest-merge.enabled"]
    #[default(true)]
    #[doc = "Whether manifests are automatically merged during writes."]
    pub merge_enabled: bool,
    #[key = "write.manifest.compression-codec"]
    #[default("gzip".to_string())]
    #[doc = "Compression codec used for manifest files."]
    pub compression: String,
    #[key = "write.manifest.compression-level"]
    #[default(None)]
    #[doc = "Optional compression level used for manifest files."]
    pub compression_level: Option<String>,
    #[key = "write.manifest-lists.enabled"]
    #[default(true)]
    #[doc = "Deprecated flag for writing manifest lists; manifest lists are always enabled."]
    pub lists_enabled: bool,
}

/// General write properties and output locations.
#[derive(Clone, Debug, Properties)]
pub struct TableWriteProperties {
    #[key = "write.format.default"]
    #[default(DataFileFormat::Parquet)]
    #[doc = "Default data file format: Parquet, Avro, or ORC."]
    pub format_default: DataFileFormat,
    #[key = "write.delete.format.default"]
    #[default(DataFileFormat::Parquet)]
    #[doc = "Default delete file format: Parquet, Avro, or ORC."]
    pub delete_format_default: DataFileFormat,
    #[key = "write.target-file-size-bytes"]
    #[default(512 * 1024 * 1024)]
    #[doc = "Target size in bytes for generated data files."]
    pub target_file_size_bytes: usize,
    #[key = "write.delete.target-file-size-bytes"]
    #[default(64 * 1024 * 1024)]
    #[doc = "Target size in bytes for generated delete files."]
    pub delete_target_file_size_bytes: usize,
    #[key = "write.object-storage.enabled"]
    #[default(false)]
    #[doc = "Whether the object-storage location provider adds a hash component to file paths."]
    pub object_store_enabled: bool,
    #[key = "write.object-storage.partitioned-paths"]
    #[default(true)]
    #[doc = "Whether object-storage file paths include partition values."]
    pub object_store_partitioned_paths: bool,
    #[key = "write.object-storage.path"]
    #[default(None)]
    #[doc = "Deprecated base object-storage path; use write.data.path instead."]
    pub object_store_path: Option<String>,
    #[key = "write.location-provider.impl"]
    #[default(None)]
    #[doc = "Optional custom location provider implementation."]
    pub location_provider_impl: Option<String>,
    #[key = "write.folder-storage.path"]
    #[default(None)]
    #[doc = "Deprecated base folder-storage path; use write.data.path instead."]
    pub folder_storage_location: Option<String>,
    #[key = "write.data.path"]
    #[default(None)]
    #[doc = "Base location for data files written after this property is set."]
    pub data_location: Option<String>,
    #[key = "write.wap.enabled"]
    #[default(false)]
    #[doc = "Whether write-audit-publish writes are enabled."]
    pub audit_publish_enabled: bool,
    #[key = "write.distribution-mode"]
    #[default(None)]
    #[doc = "Optional write distribution mode: none, hash, or range."]
    pub distribution_mode: Option<String>,
    #[key = "write.datafusion.fanout.enabled"]
    #[default(true)]
    #[doc = "Whether DataFusion uses a fanout writer for partitioned tables."]
    pub datafusion_fanout_enabled: bool,
}

/// Parquet data and delete file properties.
#[derive(Clone, Debug, Properties)]
pub struct TableParquetProperties {
    #[key = "write.parquet.row-group-size-bytes"]
    #[default(128 * 1024 * 1024)]
    #[doc = "Parquet row group size in bytes for data files."]
    pub row_group_size_bytes: usize,
    #[key = "write.delete.parquet.row-group-size-bytes"]
    #[default(128 * 1024 * 1024)]
    #[doc = "Parquet row group size in bytes for delete files."]
    pub delete_row_group_size_bytes: usize,
    #[key = "write.parquet.page-size-bytes"]
    #[default(1024 * 1024)]
    #[doc = "Parquet page size in bytes for data files."]
    pub page_size_bytes: usize,
    #[key = "write.delete.parquet.page-size-bytes"]
    #[default(1024 * 1024)]
    #[doc = "Parquet page size in bytes for delete files."]
    pub delete_page_size_bytes: usize,
    #[key = "write.parquet.page-version"]
    #[default("v1".to_string())]
    #[doc = "Parquet data page version for data files: v1 or v2."]
    pub page_version: String,
    #[key = "write.delete.parquet.page-version"]
    #[default("v1".to_string())]
    #[doc = "Parquet data page version for delete files: v1 or v2."]
    pub delete_page_version: String,
    #[key = "write.parquet.page-row-limit"]
    #[default(20_000)]
    #[doc = "Maximum number of rows per Parquet page in data files."]
    pub page_row_limit: usize,
    #[key = "write.delete.parquet.page-row-limit"]
    #[default(20_000)]
    #[doc = "Maximum number of rows per Parquet page in delete files."]
    pub delete_page_row_limit: usize,
    #[key = "write.parquet.dict-size-bytes"]
    #[default(2 * 1024 * 1024)]
    #[doc = "Parquet dictionary page size in bytes for data files."]
    pub dict_size_bytes: usize,
    #[key = "write.delete.parquet.dict-size-bytes"]
    #[default(2 * 1024 * 1024)]
    #[doc = "Parquet dictionary page size in bytes for delete files."]
    pub delete_dict_size_bytes: usize,
    #[key = "write.parquet.compression-codec"]
    #[default("zstd".to_string())]
    #[doc = "Parquet compression codec used for data files."]
    pub compression: String,
    #[key = "write.delete.parquet.compression-codec"]
    #[default("zstd".to_string())]
    #[doc = "Parquet compression codec used for delete files."]
    pub delete_compression: String,
    #[key = "write.parquet.compression-level"]
    #[default(None)]
    #[doc = "Optional Parquet compression level for data files."]
    pub compression_level: Option<String>,
    #[key = "write.delete.parquet.compression-level"]
    #[default(None)]
    #[doc = "Optional Parquet compression level for delete files."]
    pub delete_compression_level: Option<String>,
    #[key = "write.parquet.shred-variants"]
    #[default(false)]
    #[doc = "Whether variant columns use shredded Parquet encoding for improved query performance."]
    pub shred_variants: bool,
    #[key = "write.parquet.variant-inference-buffer-size"]
    #[default(100)]
    #[doc = "Number of rows buffered for schema inference when variant shredding is enabled."]
    pub variant_inference_buffer_size: usize,
    #[key = "write.parquet.row-group-check-min-record-count"]
    #[default(100)]
    #[doc = "Minimum record count between Parquet data-file row group size checks."]
    pub row_group_check_min_record_count: usize,
    #[key = "write.delete.parquet.row-group-check-min-record-count"]
    #[default(100)]
    #[doc = "Minimum record count between Parquet delete-file row group size checks."]
    pub delete_row_group_check_min_record_count: usize,
    #[key = "write.parquet.row-group-check-max-record-count"]
    #[default(10_000)]
    #[doc = "Maximum record count between Parquet data-file row group size checks."]
    pub row_group_check_max_record_count: usize,
    #[key = "write.delete.parquet.row-group-check-max-record-count"]
    #[default(10_000)]
    #[doc = "Maximum record count between Parquet delete-file row group size checks."]
    pub delete_row_group_check_max_record_count: usize,
    #[key = "write.parquet.row-group-size-track-uncompressed"]
    #[default(false)]
    #[doc = "Whether uncompressed data size is tracked to enforce the Parquet row group target."]
    pub row_group_size_track_uncompressed: bool,
    #[key = "write.parquet.bloom-filter-max-bytes"]
    #[default(1024 * 1024)]
    #[doc = "Maximum number of bytes for a Parquet bloom filter bitset."]
    pub bloom_filter_max_bytes: usize,
    #[key = "write.parquet.bloom-filter-adaptive-enabled"]
    #[default(false)]
    #[doc = "Whether adaptive Parquet bloom filter sizing selects the smallest suitable filter."]
    pub bloom_filter_adaptive_enabled: bool,
    #[prefix = "write.parquet.bloom-filter-fpp.column."]
    #[default(HashMap::new())]
    #[doc = "Per-column Parquet bloom filter false-positive probabilities, keyed by column name."]
    pub bloom_filter_column_fpp: HashMap<String, f64>,
    #[prefix = "write.parquet.bloom-filter-ndv.column."]
    #[default(HashMap::new())]
    #[doc = "Per-column expected distinct-value counts for Parquet bloom filters."]
    pub bloom_filter_column_ndv: HashMap<String, u64>,
    #[prefix = "write.parquet.bloom-filter-enabled.column."]
    #[default(HashMap::new())]
    #[doc = "Per-column flags controlling whether Parquet bloom filters are written."]
    pub bloom_filter_column_enabled: HashMap<String, bool>,
    #[prefix = "write.parquet.stats-enabled.column."]
    #[default(HashMap::new())]
    #[doc = "Per-column flags controlling whether Parquet column statistics are collected."]
    pub column_stats_enabled: HashMap<String, bool>,
    #[prefix = "write.parquet.dict-encoding-enabled.column."]
    #[default(HashMap::new())]
    #[doc = "Per-column flags controlling whether Parquet dictionary encoding is used."]
    pub dict_encoding_enabled_by_column: HashMap<String, bool>,
    #[key = "write.parquet.content-defined-chunking.enabled"]
    #[default(false)]
    #[doc = "Whether Parquet content-defined chunking is enabled."]
    pub content_defined_chunking_enabled: bool,
    #[key = "write.parquet.content-defined-chunking.min-chunk-size"]
    #[default(256 * 1024)]
    #[doc = "Minimum Parquet content-defined chunk size in bytes."]
    pub content_defined_chunking_min_chunk_size: usize,
    #[key = "write.parquet.content-defined-chunking.max-chunk-size"]
    #[default(1024 * 1024)]
    #[doc = "Maximum Parquet content-defined chunk size in bytes."]
    pub content_defined_chunking_max_chunk_size: usize,
    #[key = "write.parquet.content-defined-chunking.norm-level"]
    #[default(0)]
    #[doc = "Gearhash normalization level used by Parquet content-defined chunking."]
    pub content_defined_chunking_norm_level: i32,
}

/// Avro data and delete file properties.
#[derive(Clone, Debug, Properties)]
pub struct TableAvroProperties {
    #[key = "write.avro.compression-codec"]
    #[default("gzip".to_string())]
    #[doc = "Avro compression codec used for data files."]
    pub compression: String,
    #[key = "write.delete.avro.compression-codec"]
    #[default("gzip".to_string())]
    #[doc = "Avro compression codec used for delete files."]
    pub delete_compression: String,
    #[key = "write.avro.compression-level"]
    #[default(None)]
    #[doc = "Optional Avro compression level for data files."]
    pub compression_level: Option<String>,
    #[key = "write.delete.avro.compression-level"]
    #[default(None)]
    #[doc = "Optional Avro compression level for delete files."]
    pub delete_compression_level: Option<String>,
}

/// ORC data and delete file properties.
#[derive(Clone, Debug, Properties)]
pub struct TableOrcProperties {
    #[key = "write.orc.stripe-size-bytes"]
    #[default(64 * 1024 * 1024)]
    #[doc = "Default ORC stripe size in bytes for data files."]
    pub stripe_size_bytes: u64,
    #[key = "write.delete.orc.stripe-size-bytes"]
    #[default(64 * 1024 * 1024)]
    #[doc = "Default ORC stripe size in bytes for delete files."]
    pub delete_stripe_size_bytes: u64,
    #[key = "write.orc.bloom.filter.columns"]
    #[default(String::new())]
    #[doc = "Comma-separated column names for which ORC bloom filters are created."]
    pub bloom_filter_columns: String,
    #[key = "write.orc.bloom.filter.fpp"]
    #[default(0.05)]
    #[doc = "False-positive probability for ORC bloom filters."]
    pub bloom_filter_fpp: f64,
    #[key = "write.orc.block-size-bytes"]
    #[default(256 * 1024 * 1024)]
    #[doc = "Default file-system block size in bytes for ORC data files."]
    pub block_size_bytes: u64,
    #[key = "write.delete.orc.block-size-bytes"]
    #[default(256 * 1024 * 1024)]
    #[doc = "Default file-system block size in bytes for ORC delete files."]
    pub delete_block_size_bytes: u64,
    #[key = "write.orc.vectorized.batch-size"]
    #[default(1024)]
    #[doc = "ORC vectorized write batch size for data files."]
    pub write_batch_size: usize,
    #[key = "write.delete.orc.vectorized.batch-size"]
    #[default(1024)]
    #[doc = "ORC vectorized write batch size for delete files."]
    pub delete_write_batch_size: usize,
    #[key = "write.orc.compression-codec"]
    #[default("zlib".to_string())]
    #[doc = "ORC compression codec used for data files."]
    pub compression: String,
    #[key = "write.delete.orc.compression-codec"]
    #[default("zlib".to_string())]
    #[doc = "ORC compression codec used for delete files."]
    pub delete_compression: String,
    #[key = "write.orc.compression-strategy"]
    #[default("speed".to_string())]
    #[doc = "ORC compression strategy for data files: speed or compression."]
    pub compression_strategy: String,
    #[key = "write.delete.orc.compression-strategy"]
    #[default("speed".to_string())]
    #[doc = "ORC compression strategy for delete files: speed or compression."]
    pub delete_compression_strategy: String,
}

/// Read split planning and vectorization properties.
#[derive(Clone, Debug, Properties)]
pub struct TableReadProperties {
    #[key = "read.split.target-size"]
    #[default(128 * 1024 * 1024)]
    #[doc = "Target size in bytes when combining data input splits."]
    pub split_target_size: u64,
    #[key = "read.split.metadata-target-size"]
    #[default(32 * 1024 * 1024)]
    #[doc = "Target size in bytes when combining metadata input splits."]
    pub metadata_split_target_size: u64,
    #[key = "read.split.planning-lookback"]
    #[default(10)]
    #[doc = "Number of bins considered when combining input splits."]
    pub split_planning_lookback: usize,
    #[key = "read.split.open-file-cost"]
    #[default(4 * 1024 * 1024)]
    #[doc = "Estimated file-open cost used as a minimum weight when combining splits."]
    pub split_open_file_cost: u64,
    #[key = "read.split.adaptive-size.enabled"]
    #[default(true)]
    #[doc = "Whether split size is adapted to the scan size."]
    pub adaptive_split_size_enabled: bool,
    #[key = "read.parquet.vectorization.enabled"]
    #[default(true)]
    #[doc = "Whether Parquet vectorized reads are enabled."]
    pub parquet_vectorization_enabled: bool,
    #[key = "read.parquet.vectorization.batch-size"]
    #[default(5000)]
    #[doc = "Batch size for Parquet vectorized reads."]
    pub parquet_batch_size: usize,
    #[key = "read.orc.vectorization.enabled"]
    #[default(false)]
    #[doc = "Whether ORC vectorized reads are enabled."]
    pub orc_vectorization_enabled: bool,
    #[key = "read.orc.vectorization.batch-size"]
    #[default(5000)]
    #[doc = "Batch size for ORC vectorized reads."]
    pub orc_batch_size: usize,
    #[key = "read.data-planning-mode"]
    #[default("auto".to_string())]
    #[doc = "Planning mode used for data files."]
    pub data_planning_mode: String,
    #[key = "read.delete-planning-mode"]
    #[default("auto".to_string())]
    #[doc = "Planning mode used for delete files."]
    pub delete_planning_mode: String,
}

/// Table metadata retention, compression, and metrics properties.
#[derive(Clone, Debug, Properties)]
pub struct TableMetadataProperties {
    #[key = "write.metadata.path"]
    #[default(None)]
    #[parse_with(parse_metadata_location)]
    #[doc = "Base location for metadata files written after this property is set, with trailing slashes removed."]
    pub path: Option<String>,
    #[key = "write.summary.partition-limit"]
    #[default(0)]
    #[doc = "Maximum changed-partition count for including partition-level statistics in snapshot summaries."]
    pub partition_summary_limit: u64,
    #[key = "write.metadata.compression-codec"]
    #[default(CompressionCodec::None)]
    #[parse_with(parse_metadata_file_compression)]
    #[serialize_with(serialize_compression_codec)]
    #[doc = "Compression codec for metadata JSON files: none or gzip."]
    pub compression_codec: CompressionCodec,
    #[key = "write.metadata.previous-versions-max"]
    #[default(100)]
    #[doc = "Maximum number of previous metadata file versions to track."]
    pub previous_versions_max: usize,
    #[key = "write.metadata.delete-after-commit.enabled"]
    #[default(false)]
    #[doc = "Whether the oldest tracked metadata file is deleted after each commit."]
    pub delete_after_commit_enabled: bool,
    #[key = "write.metadata.metrics.max-inferred-column-defaults"]
    #[default(100)]
    #[doc = "Maximum number of columns that receive inferred metrics defaults."]
    pub metrics_max_inferred_column_defaults: usize,
    #[prefix = "write.metadata.metrics.column."]
    #[default(HashMap::new())]
    #[doc = "Per-column metrics modes keyed by column name."]
    pub metrics_mode_by_column: HashMap<String, String>,
    #[key = "write.metadata.metrics.default"]
    #[default("truncate(16)".to_string())]
    #[doc = "Default metrics mode for table columns."]
    pub default_metrics_mode: String,
    #[key = "schema.name-mapping.default"]
    #[default(None)]
    #[doc = "Default JSON name mapping used to resolve columns in files without field IDs."]
    pub default_name_mapping: Option<String>,
}

/// Engine-specific and compatibility properties.
#[derive(Clone, Debug, Properties)]
pub struct TableCompatibilityProperties {
    #[key = "write.spark.fanout.enabled"]
    #[default(false)]
    #[doc = "Deprecated Spark fanout-writer flag; the fanout writer accepts unclustered data but uses more memory."]
    pub spark_write_partitioned_fanout_enabled: bool,
    #[key = "write.spark.accept-any-schema"]
    #[default(false)]
    #[doc = "Deprecated Spark flag allowing writes with any compatible schema."]
    pub spark_write_accept_any_schema: bool,
    #[key = "write.spark.auto-schema-evolution.enabled"]
    #[default(true)]
    #[doc = "Deprecated Spark flag enabling automatic schema evolution during writes."]
    pub spark_write_auto_schema_evolution: bool,
    #[key = "write.spark.advisory-partition-size-bytes"]
    #[default(None)]
    #[doc = "Deprecated Spark advisory partition size in bytes."]
    pub spark_write_advisory_partition_size_bytes: Option<u64>,
    #[key = "compatibility.snapshot-id-inheritance.enabled"]
    #[default(false)]
    #[doc = "Whether snapshots may be committed without explicit snapshot IDs; format version 2 and later always allow this."]
    pub snapshot_id_inheritance_enabled: bool,
    #[key = "engine.hive.enabled"]
    #[default(false)]
    #[doc = "Whether Hive engine integration behavior is enabled."]
    pub engine_hive_enabled: bool,
    #[key = "engine.hive.lock-enabled"]
    #[default(true)]
    #[doc = "Whether Hive locking is enabled."]
    pub hive_lock_enabled: bool,
}

/// Garbage collection and snapshot retention properties.
#[derive(Clone, Debug, Properties)]
pub struct TableHistoryProperties {
    #[key = "gc.enabled"]
    #[default(true)]
    #[doc = "Whether garbage collection operations such as snapshot expiration and orphan-file removal are allowed."]
    pub gc_enabled: bool,
    #[key = "history.expire.max-snapshot-age-ms"]
    #[default(5 * 24 * 60 * 60 * 1000)]
    #[doc = "Default maximum snapshot age in milliseconds while expiring snapshots."]
    pub max_snapshot_age_ms: i64,
    #[key = "history.expire.min-snapshots-to-keep"]
    #[default(1)]
    #[doc = "Default minimum number of snapshots retained per branch while expiring snapshots."]
    pub min_snapshots_to_keep: usize,
    #[key = "history.expire.max-ref-age-ms"]
    #[default(i64::MAX)]
    #[doc = "Default maximum age in milliseconds for snapshot references other than the main branch."]
    pub max_ref_age_ms: i64,
}

/// Delete, update, merge, and upsert properties.
#[derive(Clone, Debug, Properties)]
pub struct TableRowLevelProperties {
    #[key = "write.delete.granularity"]
    #[default("partition".to_string())]
    #[doc = "Granularity of generated delete files: partition or file."]
    pub delete_granularity: String,
    #[key = "write.delete.isolation-level"]
    #[default("serializable".to_string())]
    #[doc = "Isolation level for delete commands: serializable or snapshot."]
    pub delete_isolation_level: String,
    #[key = "write.delete.mode"]
    #[default("copy-on-write".to_string())]
    #[doc = "Execution mode for delete commands: copy-on-write or merge-on-read."]
    pub delete_mode: String,
    #[key = "write.delete.distribution-mode"]
    #[default(None)]
    #[doc = "Optional distribution mode for delete command data."]
    pub delete_distribution_mode: Option<String>,
    #[key = "write.update.isolation-level"]
    #[default("serializable".to_string())]
    #[doc = "Isolation level for update commands: serializable or snapshot."]
    pub update_isolation_level: String,
    #[key = "write.update.mode"]
    #[default("copy-on-write".to_string())]
    #[doc = "Execution mode for update commands: copy-on-write or merge-on-read."]
    pub update_mode: String,
    #[key = "write.update.distribution-mode"]
    #[default(None)]
    #[doc = "Optional distribution mode for update command data."]
    pub update_distribution_mode: Option<String>,
    #[key = "write.merge.isolation-level"]
    #[default("serializable".to_string())]
    #[doc = "Isolation level for merge commands: serializable or snapshot."]
    pub merge_isolation_level: String,
    #[key = "write.merge.mode"]
    #[default("copy-on-write".to_string())]
    #[doc = "Execution mode for merge commands: copy-on-write or merge-on-read."]
    pub merge_mode: String,
    #[key = "write.merge.distribution-mode"]
    #[default(None)]
    #[doc = "Optional distribution mode for merge command data."]
    pub merge_distribution_mode: Option<String>,
    #[key = "write.upsert.enabled"]
    #[default(false)]
    #[doc = "Whether upsert behavior is enabled."]
    pub upsert_enabled: bool,
}

/// Table encryption properties.
#[derive(Clone, Debug, Properties)]
pub struct TableEncryptionProperties {
    #[key = "encryption.key-id"]
    #[default(None)]
    #[doc = "Identifier of the table's master encryption key."]
    pub key_id: Option<String>,
    #[key = "encryption.data-key-length"]
    #[default(16)]
    #[doc = "Length in bytes of data-encryption keys; valid AES lengths are 16, 24, and 32 bytes."]
    pub data_key_length: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(properties: HashMap<String, String>) -> Result<ParsedTableProperties> {
        serde_json::from_value(serde_json::to_value(properties).unwrap())
            .map_err(|error| Error::new(ErrorKind::DataInvalid, error.to_string()))
    }

    #[test]
    fn creates_properties_from_defaults() {
        let properties = ParsedTableProperties::default();

        assert_eq!(properties.commit.num_retries, 4);
        assert_eq!(properties.write.format_default, DataFileFormat::Parquet);
        assert_eq!(properties.parquet.row_group_size_bytes, 128 * 1024 * 1024);
        assert_eq!(properties.read.split_target_size, 128 * 1024 * 1024);
        assert!(properties.history.gc_enabled);
        assert_eq!(properties.encryption.data_key_length, 16);
    }

    #[test]
    fn serializes_to_flat_json_object() {
        let mut properties = ParsedTableProperties::default();
        properties.commit.num_retries = 9;
        properties.write.format_default = DataFileFormat::Orc;
        properties.write.data_location = Some("s3://warehouse/table/data".to_string());
        properties.parquet.bloom_filter_column_fpp =
            HashMap::from([("customer_id".to_string(), 0.02)]);

        let json = serde_json::to_value(&properties).unwrap();
        assert_eq!(json["commit.retry.num-retries"], "9");
        assert_eq!(json["write.format.default"], "orc");
        assert_eq!(json["write.data.path"], "s3://warehouse/table/data");
        assert_eq!(
            json["write.parquet.bloom-filter-fpp.column.customer_id"],
            "0.02"
        );
        assert!(json.get("commit").is_none());
        assert!(json.get("write").is_none());
    }

    #[test]
    fn deserializes_from_flat_json_object() {
        let properties: ParsedTableProperties = serde_json::from_value(serde_json::json!({
            "commit.retry.num-retries": "8",
            "write.format.default": "orc",
            "write.data.path": "s3://warehouse/table/data"
        }))
        .unwrap();

        assert_eq!(properties.commit.num_retries, 8);
        assert_eq!(properties.write.format_default, DataFileFormat::Orc);
        assert_eq!(
            properties.write.data_location,
            Some("s3://warehouse/table/data".to_string())
        );
    }

    #[test]
    fn every_default_round_trips() {
        let defaults = ParsedTableProperties::default();
        let decoded: ParsedTableProperties =
            serde_json::from_value(serde_json::to_value(&defaults).unwrap()).unwrap();

        assert_eq!(decoded.commit.num_retries, defaults.commit.num_retries);
        assert_eq!(
            decoded.metadata.compression_codec,
            defaults.metadata.compression_codec
        );
        assert_eq!(
            decoded.parquet.content_defined_chunking_max_chunk_size,
            defaults.parquet.content_defined_chunking_max_chunk_size
        );
        assert_eq!(decoded.row_level.merge_mode, defaults.row_level.merge_mode);
    }

    #[test]
    fn parses_values_across_groups() {
        let properties = parse(HashMap::from([
            ("comment".to_string(), "orders table".to_string()),
            (
                "commit.status-check.num-retries".to_string(),
                "7".to_string(),
            ),
            (
                "write.delete.avro.compression-codec".to_string(),
                "snappy".to_string(),
            ),
            ("read.split.planning-lookback".to_string(), "25".to_string()),
            (
                "history.expire.min-snapshots-to-keep".to_string(),
                "4".to_string(),
            ),
            ("write.delete.mode".to_string(), "merge-on-read".to_string()),
            ("encryption.data-key-length".to_string(), "32".to_string()),
        ]))
        .unwrap();

        assert_eq!(properties.general.comment, Some("orders table".to_string()));
        assert_eq!(properties.commit.num_status_checks, 7);
        assert_eq!(properties.avro.delete_compression, "snappy");
        assert_eq!(properties.read.split_planning_lookback, 25);
        assert_eq!(properties.history.min_snapshots_to_keep, 4);
        assert_eq!(properties.row_level.delete_mode, "merge-on-read");
        assert_eq!(properties.encryption.data_key_length, 32);
    }

    #[test]
    fn metadata_values_are_normalized_and_validated() {
        let properties = parse(HashMap::from([
            (
                "write.metadata.path".to_string(),
                "s3://warehouse/table/metadata/".to_string(),
            ),
            (
                "write.metadata.compression-codec".to_string(),
                "GZIP".to_string(),
            ),
        ]))
        .unwrap();

        assert_eq!(
            properties.metadata.path,
            Some("s3://warehouse/table/metadata".to_string())
        );
        assert_eq!(
            properties.metadata.compression_codec,
            CompressionCodec::gzip_default()
        );

        let error = parse(HashMap::from([(
            "write.metadata.compression-codec".to_string(),
            "zstd".to_string(),
        )]))
        .unwrap_err();
        assert!(
            error
                .message()
                .contains("Invalid metadata compression codec")
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
