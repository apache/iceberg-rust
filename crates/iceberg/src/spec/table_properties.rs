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

use std::collections::HashMap;
use std::fmt::Display;
use std::str::FromStr;

use iceberg_property_macro::Properties;

use crate::compression::CompressionCodec;
use crate::error::{Error, ErrorKind, Result};
use crate::spec::DataFileFormat;

fn parse_property<T: FromStr>(
    properties: &HashMap<String, String>,
    key: &str,
    default: T,
) -> Result<T>
where
    <T as FromStr>::Err: Display,
{
    properties.get(key).map_or(Ok(default), |value| {
        value.parse::<T>().map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Invalid value for {key}: {e}"),
            )
        })
    })
}

/// Strips trailing slashes from a location, preserving a bare URI scheme root
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

fn parse_location_property(
    properties: &HashMap<String, String>,
    key: &str,
) -> Result<Option<String>> {
    properties
        .get(key)
        .map(|path| {
            if path.is_empty() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("Invalid value for {key}: path must not be empty"),
                ));
            }

            Ok(strip_trailing_slash(path).to_string())
        })
        .transpose()
}

fn parse_metadata_file_compression_value(value: &str) -> Result<CompressionCodec> {
    // Handle empty string as None
    if value.is_empty() {
        return Ok(CompressionCodec::None);
    }

    // Lowercase the value for case-insensitive parsing
    let lowercase_value = value.to_lowercase();

    // Use serde to parse the codec (which has rename_all = "lowercase")
    let codec: CompressionCodec = serde_json::from_value(serde_json::Value::String(
        lowercase_value,
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

    // Validate that only None and Gzip are used for metadata
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

/// Parses the compression codec used for metadata files.
pub(crate) fn parse_metadata_file_compression(
    properties: &HashMap<String, String>,
) -> Result<CompressionCodec> {
    parse_metadata_file_compression_value(
        properties
            .get(TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC)
            .map(String::as_str)
            .unwrap_or(TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC_DEFAULT),
    )
}

fn serialize_compression_codec(codec: &CompressionCodec) -> String {
    codec.name().to_string()
}

/// Typed table properties parsed from a table's string property map.
///
/// This includes the properties defined by the pinned [Java `TableProperties`] reference as well
/// as Iceberg Rust-specific properties.
///
/// [Java `TableProperties`]: https://github.com/apache/iceberg/blob/d8c10a1608170f0ba83be740d6ab0b6a3757cb3e/core/src/main/java/org/apache/iceberg/TableProperties.java
#[derive(Debug, Properties)]
pub struct ParsedTableProperties {
    /// The number of times to retry a commit.
    #[key(TableProperties::PROPERTY_COMMIT_NUM_RETRIES)]
    #[default(TableProperties::PROPERTY_COMMIT_NUM_RETRIES_DEFAULT)]
    commit_num_retries: usize,
    /// The minimum wait time between retries.
    #[key(TableProperties::PROPERTY_COMMIT_MIN_RETRY_WAIT_MS)]
    #[default(TableProperties::PROPERTY_COMMIT_MIN_RETRY_WAIT_MS_DEFAULT)]
    commit_min_retry_wait_ms: u64,
    /// The maximum wait time between retries.
    #[key(TableProperties::PROPERTY_COMMIT_MAX_RETRY_WAIT_MS)]
    #[default(TableProperties::PROPERTY_COMMIT_MAX_RETRY_WAIT_MS_DEFAULT)]
    commit_max_retry_wait_ms: u64,
    /// The total timeout for commit retries.
    #[key(TableProperties::PROPERTY_COMMIT_TOTAL_RETRY_TIME_MS)]
    #[default(TableProperties::PROPERTY_COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT)]
    commit_total_retry_timeout_ms: u64,
    #[key(TableProperties::PROPERTY_COMMENT)]
    #[default(None)]
    #[doc = "The table's business meaning and usage context."]
    comment: Option<String>,
    #[key(TableProperties::PROPERTY_COMMIT_NUM_STATUS_CHECKS)]
    #[default(TableProperties::PROPERTY_COMMIT_NUM_STATUS_CHECKS_DEFAULT)]
    commit_num_status_checks: usize,
    #[key(TableProperties::PROPERTY_COMMIT_STATUS_CHECKS_MIN_WAIT_MS)]
    #[default(TableProperties::PROPERTY_COMMIT_STATUS_CHECKS_MIN_WAIT_MS_DEFAULT)]
    commit_status_checks_min_wait_ms: u64,
    #[key(TableProperties::PROPERTY_COMMIT_STATUS_CHECKS_MAX_WAIT_MS)]
    #[default(TableProperties::PROPERTY_COMMIT_STATUS_CHECKS_MAX_WAIT_MS_DEFAULT)]
    commit_status_checks_max_wait_ms: u64,
    #[key(TableProperties::PROPERTY_COMMIT_STATUS_CHECKS_TOTAL_WAIT_MS)]
    #[default(TableProperties::PROPERTY_COMMIT_STATUS_CHECKS_TOTAL_WAIT_MS_DEFAULT)]
    commit_status_checks_total_wait_ms: u64,
    #[key(TableProperties::PROPERTY_MANIFEST_TARGET_SIZE_BYTES)]
    #[default(TableProperties::PROPERTY_MANIFEST_TARGET_SIZE_BYTES_DEFAULT)]
    manifest_target_size_bytes: usize,
    #[key(TableProperties::PROPERTY_MANIFEST_MIN_MERGE_COUNT)]
    #[default(TableProperties::PROPERTY_MANIFEST_MIN_MERGE_COUNT_DEFAULT)]
    manifest_min_merge_count: usize,
    #[key(TableProperties::PROPERTY_MANIFEST_MERGE_ENABLED)]
    #[default(TableProperties::PROPERTY_MANIFEST_MERGE_ENABLED_DEFAULT)]
    manifest_merge_enabled: bool,
    /// The default format for files.
    #[key(TableProperties::PROPERTY_DEFAULT_FILE_FORMAT)]
    #[default(DataFileFormat::Parquet)]
    write_format_default: DataFileFormat,
    #[key(TableProperties::PROPERTY_DELETE_DEFAULT_FILE_FORMAT)]
    #[default(DataFileFormat::Parquet)]
    delete_format_default: DataFileFormat,
    #[key(TableProperties::PROPERTY_PARQUET_ROW_GROUP_SIZE_BYTES)]
    #[default(TableProperties::PROPERTY_PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT)]
    parquet_row_group_size_bytes: usize,
    #[key(TableProperties::PROPERTY_DELETE_PARQUET_ROW_GROUP_SIZE_BYTES)]
    #[default(TableProperties::PROPERTY_PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT)]
    delete_parquet_row_group_size_bytes: usize,
    #[key(TableProperties::PROPERTY_PARQUET_PAGE_SIZE_BYTES)]
    #[default(TableProperties::PROPERTY_PARQUET_PAGE_SIZE_BYTES_DEFAULT)]
    parquet_page_size_bytes: usize,
    #[key(TableProperties::PROPERTY_DELETE_PARQUET_PAGE_SIZE_BYTES)]
    #[default(TableProperties::PROPERTY_PARQUET_PAGE_SIZE_BYTES_DEFAULT)]
    delete_parquet_page_size_bytes: usize,
    #[key(TableProperties::PROPERTY_PARQUET_PAGE_VERSION)]
    #[default(TableProperties::PROPERTY_PARQUET_PAGE_VERSION_DEFAULT.to_string())]
    parquet_page_version: String,
    #[key(TableProperties::PROPERTY_DELETE_PARQUET_PAGE_VERSION)]
    #[default(TableProperties::PROPERTY_PARQUET_PAGE_VERSION_DEFAULT.to_string())]
    delete_parquet_page_version: String,
    #[key(TableProperties::PROPERTY_PARQUET_PAGE_ROW_LIMIT)]
    #[default(TableProperties::PROPERTY_PARQUET_PAGE_ROW_LIMIT_DEFAULT)]
    parquet_page_row_limit: usize,
    #[key(TableProperties::PROPERTY_DELETE_PARQUET_PAGE_ROW_LIMIT)]
    #[default(TableProperties::PROPERTY_PARQUET_PAGE_ROW_LIMIT_DEFAULT)]
    delete_parquet_page_row_limit: usize,
    #[key(TableProperties::PROPERTY_PARQUET_DICT_SIZE_BYTES)]
    #[default(TableProperties::PROPERTY_PARQUET_DICT_SIZE_BYTES_DEFAULT)]
    parquet_dict_size_bytes: usize,
    #[key(TableProperties::PROPERTY_DELETE_PARQUET_DICT_SIZE_BYTES)]
    #[default(TableProperties::PROPERTY_PARQUET_DICT_SIZE_BYTES_DEFAULT)]
    delete_parquet_dict_size_bytes: usize,
    #[key(TableProperties::PROPERTY_PARQUET_COMPRESSION)]
    #[default(TableProperties::PROPERTY_PARQUET_COMPRESSION_DEFAULT_SINCE_1_4_0.to_string())]
    parquet_compression: String,
    #[key(TableProperties::PROPERTY_DELETE_PARQUET_COMPRESSION)]
    #[default(TableProperties::PROPERTY_PARQUET_COMPRESSION_DEFAULT_SINCE_1_4_0.to_string())]
    delete_parquet_compression: String,
    #[key(TableProperties::PROPERTY_PARQUET_COMPRESSION_LEVEL)]
    #[default(None)]
    parquet_compression_level: Option<String>,
    #[key(TableProperties::PROPERTY_DELETE_PARQUET_COMPRESSION_LEVEL)]
    #[default(None)]
    delete_parquet_compression_level: Option<String>,
    #[key(TableProperties::PROPERTY_PARQUET_SHRED_VARIANTS)]
    #[default(TableProperties::PROPERTY_PARQUET_SHRED_VARIANTS_DEFAULT)]
    parquet_shred_variants: bool,
    #[key(TableProperties::PROPERTY_PARQUET_VARIANT_BUFFER_SIZE)]
    #[default(TableProperties::PROPERTY_PARQUET_VARIANT_BUFFER_SIZE_DEFAULT)]
    parquet_variant_buffer_size: usize,
    #[key(TableProperties::PROPERTY_PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT)]
    #[default(TableProperties::PROPERTY_PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT_DEFAULT)]
    parquet_row_group_check_min_record_count: usize,
    #[key(TableProperties::PROPERTY_DELETE_PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT)]
    #[default(TableProperties::PROPERTY_PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT_DEFAULT)]
    delete_parquet_row_group_check_min_record_count: usize,
    #[key(TableProperties::PROPERTY_PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT)]
    #[default(TableProperties::PROPERTY_PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT_DEFAULT)]
    parquet_row_group_check_max_record_count: usize,
    #[key(TableProperties::PROPERTY_DELETE_PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT)]
    #[default(TableProperties::PROPERTY_PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT_DEFAULT)]
    delete_parquet_row_group_check_max_record_count: usize,
    #[key(TableProperties::PROPERTY_PARQUET_ROW_GROUP_SIZE_TRACK_UNCOMPRESSED)]
    #[default(TableProperties::PROPERTY_PARQUET_ROW_GROUP_SIZE_TRACK_UNCOMPRESSED_DEFAULT)]
    parquet_row_group_size_track_uncompressed: bool,
    #[key(TableProperties::PROPERTY_PARQUET_BLOOM_FILTER_MAX_BYTES)]
    #[default(TableProperties::PROPERTY_PARQUET_BLOOM_FILTER_MAX_BYTES_DEFAULT)]
    parquet_bloom_filter_max_bytes: usize,
    #[key(TableProperties::PROPERTY_PARQUET_BLOOM_FILTER_ADAPTIVE_ENABLED)]
    #[default(TableProperties::PROPERTY_PARQUET_BLOOM_FILTER_ADAPTIVE_ENABLED_DEFAULT)]
    parquet_bloom_filter_adaptive_enabled: bool,
    #[prefix(TableProperties::PROPERTY_PARQUET_BLOOM_FILTER_COLUMN_FPP_PREFIX)]
    #[default(HashMap::new())]
    parquet_bloom_filter_column_fpp: HashMap<String, f64>,
    #[prefix(TableProperties::PROPERTY_PARQUET_BLOOM_FILTER_COLUMN_NDV_PREFIX)]
    #[default(HashMap::new())]
    parquet_bloom_filter_column_ndv: HashMap<String, u64>,
    #[prefix(TableProperties::PROPERTY_PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX)]
    #[default(HashMap::new())]
    parquet_bloom_filter_column_enabled: HashMap<String, bool>,
    #[prefix(TableProperties::PROPERTY_PARQUET_COLUMN_STATS_ENABLED_PREFIX)]
    #[default(HashMap::new())]
    parquet_column_stats_enabled: HashMap<String, bool>,
    #[prefix(TableProperties::PROPERTY_PARQUET_DICT_ENCODING_ENABLED_COLUMN_PREFIX)]
    #[default(HashMap::new())]
    parquet_dict_encoding_enabled_column: HashMap<String, bool>,
    #[key(TableProperties::PROPERTY_AVRO_COMPRESSION)]
    #[default(TableProperties::PROPERTY_AVRO_COMPRESSION_DEFAULT.to_string())]
    avro_compression: String,
    #[key(TableProperties::PROPERTY_DELETE_AVRO_COMPRESSION)]
    #[default(TableProperties::PROPERTY_AVRO_COMPRESSION_DEFAULT.to_string())]
    delete_avro_compression: String,
    #[key(TableProperties::PROPERTY_AVRO_COMPRESSION_LEVEL)]
    #[default(None)]
    avro_compression_level: Option<String>,
    #[key(TableProperties::PROPERTY_DELETE_AVRO_COMPRESSION_LEVEL)]
    #[default(None)]
    delete_avro_compression_level: Option<String>,
    #[key(TableProperties::PROPERTY_MANIFEST_COMPRESSION)]
    #[default(TableProperties::PROPERTY_MANIFEST_COMPRESSION_DEFAULT.to_string())]
    manifest_compression: String,
    #[key(TableProperties::PROPERTY_MANIFEST_COMPRESSION_LEVEL)]
    #[default(None)]
    manifest_compression_level: Option<String>,
    #[key(TableProperties::PROPERTY_ORC_STRIPE_SIZE_BYTES)]
    #[default(TableProperties::PROPERTY_ORC_STRIPE_SIZE_BYTES_DEFAULT)]
    orc_stripe_size_bytes: u64,
    #[key(TableProperties::PROPERTY_DELETE_ORC_STRIPE_SIZE_BYTES)]
    #[default(TableProperties::PROPERTY_ORC_STRIPE_SIZE_BYTES_DEFAULT)]
    delete_orc_stripe_size_bytes: u64,
    #[key(TableProperties::PROPERTY_ORC_BLOOM_FILTER_COLUMNS)]
    #[default(TableProperties::PROPERTY_ORC_BLOOM_FILTER_COLUMNS_DEFAULT.to_string())]
    orc_bloom_filter_columns: String,
    #[key(TableProperties::PROPERTY_ORC_BLOOM_FILTER_FPP)]
    #[default(TableProperties::PROPERTY_ORC_BLOOM_FILTER_FPP_DEFAULT)]
    orc_bloom_filter_fpp: f64,
    #[key(TableProperties::PROPERTY_ORC_BLOCK_SIZE_BYTES)]
    #[default(TableProperties::PROPERTY_ORC_BLOCK_SIZE_BYTES_DEFAULT)]
    orc_block_size_bytes: u64,
    #[key(TableProperties::PROPERTY_DELETE_ORC_BLOCK_SIZE_BYTES)]
    #[default(TableProperties::PROPERTY_ORC_BLOCK_SIZE_BYTES_DEFAULT)]
    delete_orc_block_size_bytes: u64,
    #[key(TableProperties::PROPERTY_ORC_WRITE_BATCH_SIZE)]
    #[default(TableProperties::PROPERTY_ORC_WRITE_BATCH_SIZE_DEFAULT)]
    orc_write_batch_size: usize,
    #[key(TableProperties::PROPERTY_DELETE_ORC_WRITE_BATCH_SIZE)]
    #[default(TableProperties::PROPERTY_ORC_WRITE_BATCH_SIZE_DEFAULT)]
    delete_orc_write_batch_size: usize,
    #[key(TableProperties::PROPERTY_ORC_COMPRESSION)]
    #[default(TableProperties::PROPERTY_ORC_COMPRESSION_DEFAULT.to_string())]
    orc_compression: String,
    #[key(TableProperties::PROPERTY_DELETE_ORC_COMPRESSION)]
    #[default(TableProperties::PROPERTY_ORC_COMPRESSION_DEFAULT.to_string())]
    delete_orc_compression: String,
    #[key(TableProperties::PROPERTY_ORC_COMPRESSION_STRATEGY)]
    #[default(TableProperties::PROPERTY_ORC_COMPRESSION_STRATEGY_DEFAULT.to_string())]
    orc_compression_strategy: String,
    #[key(TableProperties::PROPERTY_DELETE_ORC_COMPRESSION_STRATEGY)]
    #[default(TableProperties::PROPERTY_ORC_COMPRESSION_STRATEGY_DEFAULT.to_string())]
    delete_orc_compression_strategy: String,
    #[key(TableProperties::PROPERTY_SPLIT_SIZE)]
    #[default(TableProperties::PROPERTY_SPLIT_SIZE_DEFAULT)]
    split_size: u64,
    #[key(TableProperties::PROPERTY_METADATA_SPLIT_SIZE)]
    #[default(TableProperties::PROPERTY_METADATA_SPLIT_SIZE_DEFAULT)]
    metadata_split_size: u64,
    #[key(TableProperties::PROPERTY_SPLIT_LOOKBACK)]
    #[default(TableProperties::PROPERTY_SPLIT_LOOKBACK_DEFAULT)]
    split_lookback: usize,
    #[key(TableProperties::PROPERTY_SPLIT_OPEN_FILE_COST)]
    #[default(TableProperties::PROPERTY_SPLIT_OPEN_FILE_COST_DEFAULT)]
    split_open_file_cost: u64,
    #[key(TableProperties::PROPERTY_ADAPTIVE_SPLIT_SIZE_ENABLED)]
    #[default(TableProperties::PROPERTY_ADAPTIVE_SPLIT_SIZE_ENABLED_DEFAULT)]
    adaptive_split_size_enabled: bool,
    #[key(TableProperties::PROPERTY_PARQUET_VECTORIZATION_ENABLED)]
    #[default(TableProperties::PROPERTY_PARQUET_VECTORIZATION_ENABLED_DEFAULT)]
    parquet_vectorization_enabled: bool,
    #[key(TableProperties::PROPERTY_PARQUET_BATCH_SIZE)]
    #[default(TableProperties::PROPERTY_PARQUET_BATCH_SIZE_DEFAULT)]
    parquet_batch_size: usize,
    #[key(TableProperties::PROPERTY_ORC_VECTORIZATION_ENABLED)]
    #[default(TableProperties::PROPERTY_ORC_VECTORIZATION_ENABLED_DEFAULT)]
    orc_vectorization_enabled: bool,
    #[key(TableProperties::PROPERTY_ORC_BATCH_SIZE)]
    #[default(TableProperties::PROPERTY_ORC_BATCH_SIZE_DEFAULT)]
    orc_batch_size: usize,
    #[key(TableProperties::PROPERTY_DATA_PLANNING_MODE)]
    #[default(TableProperties::PROPERTY_PLANNING_MODE_DEFAULT.to_string())]
    data_planning_mode: String,
    #[key(TableProperties::PROPERTY_DELETE_PLANNING_MODE)]
    #[default(TableProperties::PROPERTY_PLANNING_MODE_DEFAULT.to_string())]
    delete_planning_mode: String,
    #[key(TableProperties::PROPERTY_IDENTIFIER_FIELDS_RELY)]
    #[default(TableProperties::PROPERTY_IDENTIFIER_FIELDS_RELY_DEFAULT)]
    identifier_fields_rely: bool,
    /// The target file size for files.
    #[key(TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES)]
    #[default(TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT)]
    write_target_file_size_bytes: usize,
    #[key(TableProperties::PROPERTY_DELETE_TARGET_FILE_SIZE_BYTES)]
    #[default(TableProperties::PROPERTY_DELETE_TARGET_FILE_SIZE_BYTES_DEFAULT)]
    delete_target_file_size_bytes: usize,
    #[key(TableProperties::PROPERTY_OBJECT_STORE_ENABLED)]
    #[default(TableProperties::PROPERTY_OBJECT_STORE_ENABLED_DEFAULT)]
    object_store_enabled: bool,
    #[key(TableProperties::PROPERTY_WRITE_OBJECT_STORE_PARTITIONED_PATHS)]
    #[default(TableProperties::PROPERTY_WRITE_OBJECT_STORE_PARTITIONED_PATHS_DEFAULT)]
    write_object_store_partitioned_paths: bool,
    #[key(TableProperties::PROPERTY_OBJECT_STORE_PATH)]
    #[default(None)]
    object_store_path: Option<String>,
    #[key(TableProperties::PROPERTY_WRITE_LOCATION_PROVIDER_IMPL)]
    #[default(None)]
    write_location_provider_impl: Option<String>,
    #[key(TableProperties::PROPERTY_WRITE_FOLDER_STORAGE_LOCATION)]
    #[default(None)]
    write_folder_storage_location: Option<String>,
    #[key(TableProperties::PROPERTY_WRITE_DATA_LOCATION)]
    #[default(None)]
    write_data_location: Option<String>,
    /// Base directory for metadata files (manifests, manifest lists), with any
    /// trailing slash trimmed. `None` if `write.metadata.path` is not set.
    #[key(TableProperties::PROPERTY_WRITE_METADATA_PATH)]
    #[default(None)]
    #[parse_with(parse_metadata_location)]
    write_metadata_path: Option<String>,
    #[key(TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT)]
    #[default(TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT)]
    write_partition_summary_limit: u64,
    #[key(TableProperties::PROPERTY_MANIFEST_LISTS_ENABLED)]
    #[default(TableProperties::PROPERTY_MANIFEST_LISTS_ENABLED_DEFAULT)]
    manifest_lists_enabled: bool,
    /// Compression codec for metadata files (JSON)
    #[key(TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC)]
    #[default(CompressionCodec::None)]
    #[parse_with(parse_metadata_file_compression_value)]
    #[serialize_with(serialize_compression_codec)]
    metadata_compression_codec: CompressionCodec,
    #[key(TableProperties::PROPERTY_METADATA_PREVIOUS_VERSIONS_MAX)]
    #[default(TableProperties::PROPERTY_METADATA_PREVIOUS_VERSIONS_MAX_DEFAULT)]
    metadata_previous_versions_max: usize,
    #[key(TableProperties::PROPERTY_METADATA_DELETE_AFTER_COMMIT_ENABLED)]
    #[default(TableProperties::PROPERTY_METADATA_DELETE_AFTER_COMMIT_ENABLED_DEFAULT)]
    metadata_delete_after_commit_enabled: bool,
    #[key(TableProperties::PROPERTY_METRICS_MAX_INFERRED_COLUMN_DEFAULTS)]
    #[default(TableProperties::PROPERTY_METRICS_MAX_INFERRED_COLUMN_DEFAULTS_DEFAULT)]
    metrics_max_inferred_column_defaults: usize,
    #[prefix(TableProperties::PROPERTY_METRICS_MODE_COLUMN_CONF_PREFIX)]
    #[default(HashMap::new())]
    metrics_mode_column_config: HashMap<String, String>,
    #[key(TableProperties::PROPERTY_DEFAULT_WRITE_METRICS_MODE)]
    #[default(TableProperties::PROPERTY_DEFAULT_WRITE_METRICS_MODE_DEFAULT.to_string())]
    default_write_metrics_mode: String,
    #[key(TableProperties::PROPERTY_DEFAULT_NAME_MAPPING)]
    #[default(None)]
    default_name_mapping: Option<String>,
    #[key(TableProperties::PROPERTY_WRITE_AUDIT_PUBLISH_ENABLED)]
    #[default(TableProperties::PROPERTY_WRITE_AUDIT_PUBLISH_ENABLED_DEFAULT)]
    write_audit_publish_enabled: bool,
    #[key(TableProperties::PROPERTY_SPARK_WRITE_PARTITIONED_FANOUT_ENABLED)]
    #[default(TableProperties::PROPERTY_SPARK_WRITE_PARTITIONED_FANOUT_ENABLED_DEFAULT)]
    spark_write_partitioned_fanout_enabled: bool,
    #[key(TableProperties::PROPERTY_SPARK_WRITE_ACCEPT_ANY_SCHEMA)]
    #[default(TableProperties::PROPERTY_SPARK_WRITE_ACCEPT_ANY_SCHEMA_DEFAULT)]
    spark_write_accept_any_schema: bool,
    #[key(TableProperties::PROPERTY_SPARK_WRITE_AUTO_SCHEMA_EVOLUTION)]
    #[default(TableProperties::PROPERTY_SPARK_WRITE_AUTO_SCHEMA_EVOLUTION_DEFAULT)]
    spark_write_auto_schema_evolution: bool,
    #[key(TableProperties::PROPERTY_SPARK_WRITE_ADVISORY_PARTITION_SIZE_BYTES)]
    #[default(None)]
    spark_write_advisory_partition_size_bytes: Option<u64>,
    #[key(TableProperties::PROPERTY_SNAPSHOT_ID_INHERITANCE_ENABLED)]
    #[default(TableProperties::PROPERTY_SNAPSHOT_ID_INHERITANCE_ENABLED_DEFAULT)]
    snapshot_id_inheritance_enabled: bool,
    #[key(TableProperties::PROPERTY_ENGINE_HIVE_ENABLED)]
    #[default(TableProperties::PROPERTY_ENGINE_HIVE_ENABLED_DEFAULT)]
    engine_hive_enabled: bool,
    #[key(TableProperties::PROPERTY_HIVE_LOCK_ENABLED)]
    #[default(TableProperties::PROPERTY_HIVE_LOCK_ENABLED_DEFAULT)]
    hive_lock_enabled: bool,
    #[key(TableProperties::PROPERTY_WRITE_DISTRIBUTION_MODE)]
    #[default(None)]
    write_distribution_mode: Option<String>,
    /// Whether to use `FanoutWriter` for partitioned tables.
    #[key(TableProperties::PROPERTY_DATAFUSION_WRITE_FANOUT_ENABLED)]
    #[default(TableProperties::PROPERTY_DATAFUSION_WRITE_FANOUT_ENABLED_DEFAULT)]
    write_datafusion_fanout_enabled: bool,
    /// Whether garbage collection is enabled on drop.
    /// When `false`, data files will not be deleted when a table is dropped.
    #[key(TableProperties::PROPERTY_GC_ENABLED)]
    #[default(TableProperties::PROPERTY_GC_ENABLED_DEFAULT)]
    gc_enabled: bool,
    /// Default maximum age of a snapshot to keep when expiring snapshots.
    #[key(TableProperties::PROPERTY_MAX_SNAPSHOT_AGE_MS)]
    #[default(TableProperties::PROPERTY_MAX_SNAPSHOT_AGE_MS_DEFAULT)]
    max_snapshot_age_ms: i64,
    /// Default minimum number of snapshots to keep per branch when expiring snapshots.
    #[key(TableProperties::PROPERTY_MIN_SNAPSHOTS_TO_KEEP)]
    #[default(TableProperties::PROPERTY_MIN_SNAPSHOTS_TO_KEEP_DEFAULT)]
    min_snapshots_to_keep: usize,
    /// Default maximum age of a snapshot reference to keep when expiring snapshots.
    #[key(TableProperties::PROPERTY_MAX_REF_AGE_MS)]
    #[default(TableProperties::PROPERTY_MAX_REF_AGE_MS_DEFAULT)]
    max_ref_age_ms: i64,
    #[key(TableProperties::PROPERTY_DELETE_GRANULARITY)]
    #[default(TableProperties::PROPERTY_DELETE_GRANULARITY_DEFAULT.to_string())]
    delete_granularity: String,
    #[key(TableProperties::PROPERTY_DELETE_ISOLATION_LEVEL)]
    #[default(TableProperties::PROPERTY_DELETE_ISOLATION_LEVEL_DEFAULT.to_string())]
    delete_isolation_level: String,
    #[key(TableProperties::PROPERTY_DELETE_MODE)]
    #[default(TableProperties::PROPERTY_DELETE_MODE_DEFAULT.to_string())]
    delete_mode: String,
    #[key(TableProperties::PROPERTY_DELETE_DISTRIBUTION_MODE)]
    #[default(None)]
    delete_distribution_mode: Option<String>,
    #[key(TableProperties::PROPERTY_UPDATE_ISOLATION_LEVEL)]
    #[default(TableProperties::PROPERTY_UPDATE_ISOLATION_LEVEL_DEFAULT.to_string())]
    update_isolation_level: String,
    #[key(TableProperties::PROPERTY_UPDATE_MODE)]
    #[default(TableProperties::PROPERTY_UPDATE_MODE_DEFAULT.to_string())]
    update_mode: String,
    #[key(TableProperties::PROPERTY_UPDATE_DISTRIBUTION_MODE)]
    #[default(None)]
    update_distribution_mode: Option<String>,
    #[key(TableProperties::PROPERTY_MERGE_ISOLATION_LEVEL)]
    #[default(TableProperties::PROPERTY_MERGE_ISOLATION_LEVEL_DEFAULT.to_string())]
    merge_isolation_level: String,
    #[key(TableProperties::PROPERTY_MERGE_MODE)]
    #[default(TableProperties::PROPERTY_MERGE_MODE_DEFAULT.to_string())]
    merge_mode: String,
    #[key(TableProperties::PROPERTY_MERGE_DISTRIBUTION_MODE)]
    #[default(None)]
    merge_distribution_mode: Option<String>,
    #[key(TableProperties::PROPERTY_UPSERT_ENABLED)]
    #[default(TableProperties::PROPERTY_UPSERT_ENABLED_DEFAULT)]
    upsert_enabled: bool,
    /// Whether content-defined chunking is enabled.
    /// `true` only when `write.parquet.content-defined-chunking.enabled = "true"`.
    #[key(TableProperties::PROPERTY_PARQUET_CDC_ENABLED)]
    #[default(TableProperties::PROPERTY_PARQUET_CDC_ENABLED_DEFAULT)]
    cdc_enabled: bool,
    /// Content-defined chunking minimum chunk size in bytes.
    #[key(TableProperties::PROPERTY_PARQUET_CDC_MIN_CHUNK_SIZE)]
    #[default(TableProperties::PROPERTY_PARQUET_CDC_MIN_CHUNK_SIZE_DEFAULT)]
    cdc_min_chunk_size: usize,
    /// Content-defined chunking maximum chunk size in bytes.
    #[key(TableProperties::PROPERTY_PARQUET_CDC_MAX_CHUNK_SIZE)]
    #[default(TableProperties::PROPERTY_PARQUET_CDC_MAX_CHUNK_SIZE_DEFAULT)]
    cdc_max_chunk_size: usize,
    /// Content-defined chunking normalization level (gearhash bit adjustment).
    #[key(TableProperties::PROPERTY_PARQUET_CDC_NORM_LEVEL)]
    #[default(TableProperties::PROPERTY_PARQUET_CDC_NORM_LEVEL_DEFAULT)]
    cdc_norm_level: i32,
    /// The master key id used to encrypt this table's manifest list and data
    /// files. `None` if `encryption.key-id` is not set.
    #[key(TableProperties::PROPERTY_ENCRYPTION_KEY_ID)]
    #[default(None)]
    encryption_key_id: Option<String>,
    /// The encryption data encryption key length in bytes.
    #[key(TableProperties::PROPERTY_ENCRYPTION_DATA_KEY_LENGTH)]
    #[default(TableProperties::PROPERTY_ENCRYPTION_DATA_KEY_LENGTH_DEFAULT)]
    encryption_data_key_length: usize,
}

/// TableProperties that contains the properties of a table.
#[derive(Debug)]
pub struct TableProperties {
    /// The number of times to retry a commit.
    pub commit_num_retries: usize,
    /// The minimum wait time between retries.
    pub commit_min_retry_wait_ms: u64,
    /// The maximum wait time between retries.
    pub commit_max_retry_wait_ms: u64,
    /// The total timeout for commit retries.
    pub commit_total_retry_timeout_ms: u64,
    /// The default format for files.
    pub write_format_default: String,
    /// The target file size for files.
    pub write_target_file_size_bytes: usize,
    /// Base directory for metadata files (manifests, manifest lists), with any
    /// trailing slash trimmed. `None` if `write.metadata.path` is not set.
    pub write_metadata_path: Option<String>,
    /// Compression codec for metadata files (JSON)
    pub metadata_compression_codec: CompressionCodec,
    /// Whether to use `FanoutWriter` for partitioned tables.
    pub write_datafusion_fanout_enabled: bool,
    /// Whether garbage collection is enabled on drop.
    /// When `false`, data files will not be deleted when a table is dropped.
    pub gc_enabled: bool,
    /// Default maximum age of a snapshot to keep when expiring snapshots.
    pub max_snapshot_age_ms: i64,
    /// Default minimum number of snapshots to keep per branch when expiring snapshots.
    pub min_snapshots_to_keep: usize,
    /// Default maximum age of a snapshot reference to keep when expiring snapshots.
    pub max_ref_age_ms: i64,
    /// Whether content-defined chunking is enabled.
    /// `true` only when `write.parquet.content-defined-chunking.enabled = "true"`.
    pub cdc_enabled: bool,
    /// Content-defined chunking minimum chunk size in bytes.
    pub cdc_min_chunk_size: usize,
    /// Content-defined chunking maximum chunk size in bytes.
    pub cdc_max_chunk_size: usize,
    /// Content-defined chunking normalization level (gearhash bit adjustment).
    pub cdc_norm_level: i32,
    /// The master key id used to encrypt this table's manifest list and data
    /// files. `None` if `encryption.key-id` is not set.
    pub encryption_key_id: Option<String>,
    /// The encryption data encryption key length in bytes.
    pub encryption_data_key_length: usize,
}

impl TableProperties {
    /// Reserved table property for table format version.
    ///
    /// Iceberg will default a new table's format version to the latest stable and recommended
    /// version. This reserved property keyword allows users to override the Iceberg format version of
    /// the table metadata.
    ///
    /// If this table property exists when creating a table, the table will use the specified format
    /// version. If a table updates this property, it will try to upgrade to the specified format
    /// version.
    pub const PROPERTY_FORMAT_VERSION: &str = "format-version";
    /// Reserved table property for table UUID.
    pub const PROPERTY_UUID: &str = "uuid";
    /// Reserved table property for the total number of snapshots.
    pub const PROPERTY_SNAPSHOT_COUNT: &str = "snapshot-count";
    /// Reserved table property for current snapshot summary.
    pub const PROPERTY_CURRENT_SNAPSHOT_SUMMARY: &str = "current-snapshot-summary";
    /// Reserved table property for current snapshot id.
    pub const PROPERTY_CURRENT_SNAPSHOT_ID: &str = "current-snapshot-id";
    /// Reserved table property for current snapshot timestamp.
    pub const PROPERTY_CURRENT_SNAPSHOT_TIMESTAMP: &str = "current-snapshot-timestamp-ms";
    /// Reserved table property for the JSON representation of current schema.
    pub const PROPERTY_CURRENT_SCHEMA: &str = "current-schema";
    /// Reserved table property for the JSON representation of current(default) partition spec.
    pub const PROPERTY_DEFAULT_PARTITION_SPEC: &str = "default-partition-spec";
    /// Reserved table property for the JSON representation of current(default) sort order.
    pub const PROPERTY_DEFAULT_SORT_ORDER: &str = "default-sort-order";

    /// Property key for max number of previous versions to keep.
    pub const PROPERTY_METADATA_PREVIOUS_VERSIONS_MAX: &str =
        "write.metadata.previous-versions-max";
    /// Default value for max number of previous versions to keep.
    pub const PROPERTY_METADATA_PREVIOUS_VERSIONS_MAX_DEFAULT: usize = 100;

    /// Property key for max number of partitions to keep summary stats for.
    pub const PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT: &str = "write.summary.partition-limit";
    /// Default value for the max number of partitions to keep summary stats for.
    pub const PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT: u64 = 0;

    /// Reserved Iceberg table properties list.
    ///
    /// Reserved table properties are only used to control behaviors when creating or updating a
    /// table. The value of these properties are not persisted as a part of the table metadata.
    pub const RESERVED_PROPERTIES: [&str; 9] = [
        Self::PROPERTY_FORMAT_VERSION,
        Self::PROPERTY_UUID,
        Self::PROPERTY_SNAPSHOT_COUNT,
        Self::PROPERTY_CURRENT_SNAPSHOT_ID,
        Self::PROPERTY_CURRENT_SNAPSHOT_SUMMARY,
        Self::PROPERTY_CURRENT_SNAPSHOT_TIMESTAMP,
        Self::PROPERTY_CURRENT_SCHEMA,
        Self::PROPERTY_DEFAULT_PARTITION_SPEC,
        Self::PROPERTY_DEFAULT_SORT_ORDER,
    ];

    /// Property key for number of commit retries.
    pub const PROPERTY_COMMIT_NUM_RETRIES: &str = "commit.retry.num-retries";
    /// Default value for number of commit retries.
    pub const PROPERTY_COMMIT_NUM_RETRIES_DEFAULT: usize = 4;

    /// Property key for minimum wait time (ms) between retries.
    pub const PROPERTY_COMMIT_MIN_RETRY_WAIT_MS: &str = "commit.retry.min-wait-ms";
    /// Default value for minimum wait time (ms) between retries.
    pub const PROPERTY_COMMIT_MIN_RETRY_WAIT_MS_DEFAULT: u64 = 100;

    /// Property key for maximum wait time (ms) between retries.
    pub const PROPERTY_COMMIT_MAX_RETRY_WAIT_MS: &str = "commit.retry.max-wait-ms";
    /// Default value for maximum wait time (ms) between retries.
    pub const PROPERTY_COMMIT_MAX_RETRY_WAIT_MS_DEFAULT: u64 = 60 * 1000; // 1 minute

    /// Property key for total maximum retry time (ms).
    pub const PROPERTY_COMMIT_TOTAL_RETRY_TIME_MS: &str = "commit.retry.total-timeout-ms";
    /// Default value for total maximum retry time (ms).
    pub const PROPERTY_COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT: u64 = 30 * 60 * 1000; // 30 minutes

    /// Default file format for data files
    pub const PROPERTY_DEFAULT_FILE_FORMAT: &str = "write.format.default";
    /// Default file format for delete files
    pub const PROPERTY_DELETE_DEFAULT_FILE_FORMAT: &str = "write.delete.format.default";
    /// Default value for data file format
    pub const PROPERTY_DEFAULT_FILE_FORMAT_DEFAULT: &str = "parquet";

    /// Target file size for newly written files.
    pub const PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES: &str = "write.target-file-size-bytes";
    /// Default target file size
    pub const PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT: usize = 512 * 1024 * 1024; // 512 MB

    /// Base location for metadata files (manifests, manifest lists, table metadata).
    /// When unset, metadata files default to the `metadata` directory under the table
    /// location.
    pub const PROPERTY_WRITE_METADATA_PATH: &str = "write.metadata.path";

    /// Compression codec for metadata files (JSON)
    pub const PROPERTY_METADATA_COMPRESSION_CODEC: &str = "write.metadata.compression-codec";
    /// Default metadata compression codec - uncompressed
    pub const PROPERTY_METADATA_COMPRESSION_CODEC_DEFAULT: &str = "none";
    /// Whether to use `FanoutWriter` for partitioned tables (handles unsorted data).
    /// If false, uses `ClusteredWriter` (requires sorted data, more memory efficient).
    pub const PROPERTY_DATAFUSION_WRITE_FANOUT_ENABLED: &str = "write.datafusion.fanout.enabled";
    /// Default value for fanout writer enabled
    pub const PROPERTY_DATAFUSION_WRITE_FANOUT_ENABLED_DEFAULT: bool = true;

    /// Property key for enabling garbage collection on drop.
    /// When set to `false`, data files will not be deleted when a table is dropped.
    /// Defaults to `true`.
    pub const PROPERTY_GC_ENABLED: &str = "gc.enabled";
    /// Default value for gc.enabled
    pub const PROPERTY_GC_ENABLED_DEFAULT: bool = true;

    /// Property key for the default maximum age of a snapshot to keep when expiring snapshots.
    pub const PROPERTY_MAX_SNAPSHOT_AGE_MS: &str = "history.expire.max-snapshot-age-ms";
    /// Default value for history.expire.max-snapshot-age-ms (5 days).
    pub const PROPERTY_MAX_SNAPSHOT_AGE_MS_DEFAULT: i64 = 5 * 24 * 60 * 60 * 1000;
    /// Property key for the default minimum number of snapshots to keep when expiring snapshots.
    pub const PROPERTY_MIN_SNAPSHOTS_TO_KEEP: &str = "history.expire.min-snapshots-to-keep";
    /// Default value for history.expire.min-snapshots-to-keep.
    pub const PROPERTY_MIN_SNAPSHOTS_TO_KEEP_DEFAULT: usize = 1;
    /// Property key for the default maximum age of a snapshot reference to keep when expiring.
    pub const PROPERTY_MAX_REF_AGE_MS: &str = "history.expire.max-ref-age-ms";
    /// Default value for history.expire.max-ref-age-ms (effectively never expire refs).
    pub const PROPERTY_MAX_REF_AGE_MS_DEFAULT: i64 = i64::MAX;

    /// Enable content-defined chunking with parquet defaults (or per-property overrides).
    pub const PROPERTY_PARQUET_CDC_ENABLED: &str = "write.parquet.content-defined-chunking.enabled";
    /// Default value for content-defined chunking enabled.
    pub const PROPERTY_PARQUET_CDC_ENABLED_DEFAULT: bool = false;
    /// Minimum chunk size in bytes for content-defined chunking.
    pub const PROPERTY_PARQUET_CDC_MIN_CHUNK_SIZE: &str =
        "write.parquet.content-defined-chunking.min-chunk-size";
    /// Default matches `parquet::file::properties::DEFAULT_CDC_MIN_CHUNK_SIZE`.
    pub const PROPERTY_PARQUET_CDC_MIN_CHUNK_SIZE_DEFAULT: usize = 256 * 1024;
    /// Maximum chunk size in bytes for content-defined chunking.
    pub const PROPERTY_PARQUET_CDC_MAX_CHUNK_SIZE: &str =
        "write.parquet.content-defined-chunking.max-chunk-size";
    /// Default matches `parquet::file::properties::DEFAULT_CDC_MAX_CHUNK_SIZE`.
    pub const PROPERTY_PARQUET_CDC_MAX_CHUNK_SIZE_DEFAULT: usize = 1024 * 1024;
    /// Normalization level (gearhash bit adjustment) for content-defined chunking.
    pub const PROPERTY_PARQUET_CDC_NORM_LEVEL: &str =
        "write.parquet.content-defined-chunking.norm-level";
    /// Default matches `parquet::file::properties::DEFAULT_CDC_NORM_LEVEL`.
    pub const PROPERTY_PARQUET_CDC_NORM_LEVEL_DEFAULT: i32 = 0;

    /// Property key for the master key id used to encrypt the table's manifest
    /// list and data files as defined in https://iceberg.apache.org/docs/nightly/encryption/.
    pub const PROPERTY_ENCRYPTION_KEY_ID: &str = "encryption.key-id";

    /// Property key for the encryption data encryption key (DEK) length in bytes.
    pub const PROPERTY_ENCRYPTION_DATA_KEY_LENGTH: &str = "encryption.data-key-length";
    /// Default value for the encryption DEK length (16 bytes = AES-128).
    pub const PROPERTY_ENCRYPTION_DATA_KEY_LENGTH_DEFAULT: usize = 16;

    /// Table property documenting the business meaning and usage context of the table.
    pub const PROPERTY_COMMENT: &str = "comment";

    /// Property key for commit status-check retries.
    pub const PROPERTY_COMMIT_NUM_STATUS_CHECKS: &str = "commit.status-check.num-retries";
    /// Default number of commit status-check retries.
    pub const PROPERTY_COMMIT_NUM_STATUS_CHECKS_DEFAULT: usize = 3;
    /// Property key for minimum wait between commit status checks.
    pub const PROPERTY_COMMIT_STATUS_CHECKS_MIN_WAIT_MS: &str = "commit.status-check.min-wait-ms";
    /// Default minimum wait between commit status checks.
    pub const PROPERTY_COMMIT_STATUS_CHECKS_MIN_WAIT_MS_DEFAULT: u64 = 1000;
    /// Property key for maximum wait between commit status checks.
    pub const PROPERTY_COMMIT_STATUS_CHECKS_MAX_WAIT_MS: &str = "commit.status-check.max-wait-ms";
    /// Default maximum wait between commit status checks.
    pub const PROPERTY_COMMIT_STATUS_CHECKS_MAX_WAIT_MS_DEFAULT: u64 = 60 * 1000;
    /// Property key for total commit status-check timeout.
    pub const PROPERTY_COMMIT_STATUS_CHECKS_TOTAL_WAIT_MS: &str =
        "commit.status-check.total-timeout-ms";
    /// Default total commit status-check timeout.
    pub const PROPERTY_COMMIT_STATUS_CHECKS_TOTAL_WAIT_MS_DEFAULT: u64 = 30 * 60 * 1000;

    /// Property key for the target manifest file size.
    pub const PROPERTY_MANIFEST_TARGET_SIZE_BYTES: &str = "commit.manifest.target-size-bytes";
    /// Default target manifest file size.
    pub const PROPERTY_MANIFEST_TARGET_SIZE_BYTES_DEFAULT: usize = 8 * 1024 * 1024;
    /// Property key for the minimum manifest count before merging.
    pub const PROPERTY_MANIFEST_MIN_MERGE_COUNT: &str = "commit.manifest.min-count-to-merge";
    /// Default minimum manifest count before merging.
    pub const PROPERTY_MANIFEST_MIN_MERGE_COUNT_DEFAULT: usize = 100;
    /// Property key controlling automatic manifest merging.
    pub const PROPERTY_MANIFEST_MERGE_ENABLED: &str = "commit.manifest-merge.enabled";
    /// Default automatic manifest merging setting.
    pub const PROPERTY_MANIFEST_MERGE_ENABLED_DEFAULT: bool = true;

    /// Property key for Parquet row group size.
    pub const PROPERTY_PARQUET_ROW_GROUP_SIZE_BYTES: &str = "write.parquet.row-group-size-bytes";
    /// Property key for delete-file Parquet row group size.
    pub const PROPERTY_DELETE_PARQUET_ROW_GROUP_SIZE_BYTES: &str =
        "write.delete.parquet.row-group-size-bytes";
    /// Default Parquet row group size.
    pub const PROPERTY_PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT: usize = 128 * 1024 * 1024;
    /// Property key for Parquet page size.
    pub const PROPERTY_PARQUET_PAGE_SIZE_BYTES: &str = "write.parquet.page-size-bytes";
    /// Property key for delete-file Parquet page size.
    pub const PROPERTY_DELETE_PARQUET_PAGE_SIZE_BYTES: &str =
        "write.delete.parquet.page-size-bytes";
    /// Default Parquet page size.
    pub const PROPERTY_PARQUET_PAGE_SIZE_BYTES_DEFAULT: usize = 1024 * 1024;
    /// Property key for Parquet page version.
    pub const PROPERTY_PARQUET_PAGE_VERSION: &str = "write.parquet.page-version";
    /// Property key for delete-file Parquet page version.
    pub const PROPERTY_DELETE_PARQUET_PAGE_VERSION: &str = "write.delete.parquet.page-version";
    /// Default Parquet page version.
    pub const PROPERTY_PARQUET_PAGE_VERSION_DEFAULT: &str = "v1";
    /// Property key for the Parquet page row limit.
    pub const PROPERTY_PARQUET_PAGE_ROW_LIMIT: &str = "write.parquet.page-row-limit";
    /// Property key for the delete-file Parquet page row limit.
    pub const PROPERTY_DELETE_PARQUET_PAGE_ROW_LIMIT: &str = "write.delete.parquet.page-row-limit";
    /// Default Parquet page row limit.
    pub const PROPERTY_PARQUET_PAGE_ROW_LIMIT_DEFAULT: usize = 20_000;
    /// Property key for Parquet dictionary size.
    pub const PROPERTY_PARQUET_DICT_SIZE_BYTES: &str = "write.parquet.dict-size-bytes";
    /// Property key for delete-file Parquet dictionary size.
    pub const PROPERTY_DELETE_PARQUET_DICT_SIZE_BYTES: &str =
        "write.delete.parquet.dict-size-bytes";
    /// Default Parquet dictionary size.
    pub const PROPERTY_PARQUET_DICT_SIZE_BYTES_DEFAULT: usize = 2 * 1024 * 1024;
    /// Property key for Parquet compression codec.
    pub const PROPERTY_PARQUET_COMPRESSION: &str = "write.parquet.compression-codec";
    /// Property key for delete-file Parquet compression codec.
    pub const PROPERTY_DELETE_PARQUET_COMPRESSION: &str = "write.delete.parquet.compression-codec";
    /// Original default Parquet compression codec.
    pub const PROPERTY_PARQUET_COMPRESSION_DEFAULT: &str = "gzip";
    /// Default Parquet compression codec since Iceberg 1.4.0.
    pub const PROPERTY_PARQUET_COMPRESSION_DEFAULT_SINCE_1_4_0: &str = "zstd";
    /// Property key for Parquet compression level.
    pub const PROPERTY_PARQUET_COMPRESSION_LEVEL: &str = "write.parquet.compression-level";
    /// Property key for delete-file Parquet compression level.
    pub const PROPERTY_DELETE_PARQUET_COMPRESSION_LEVEL: &str =
        "write.delete.parquet.compression-level";
    /// Default Parquet compression level.
    pub const PROPERTY_PARQUET_COMPRESSION_LEVEL_DEFAULT: Option<&str> = None;
    /// Property key controlling Parquet variant shredding.
    pub const PROPERTY_PARQUET_SHRED_VARIANTS: &str = "write.parquet.shred-variants";
    /// Default Parquet variant shredding setting.
    pub const PROPERTY_PARQUET_SHRED_VARIANTS_DEFAULT: bool = false;
    /// Property key for Parquet variant inference buffer size.
    pub const PROPERTY_PARQUET_VARIANT_BUFFER_SIZE: &str =
        "write.parquet.variant-inference-buffer-size";
    /// Default Parquet variant inference buffer size.
    pub const PROPERTY_PARQUET_VARIANT_BUFFER_SIZE_DEFAULT: usize = 100;
    /// Property key for minimum record-count row group checks.
    pub const PROPERTY_PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT: &str =
        "write.parquet.row-group-check-min-record-count";
    /// Delete-file property key for minimum record-count row group checks.
    pub const PROPERTY_DELETE_PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT: &str =
        "write.delete.parquet.row-group-check-min-record-count";
    /// Default minimum record-count row group check.
    pub const PROPERTY_PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT_DEFAULT: usize = 100;
    /// Property key for maximum record-count row group checks.
    pub const PROPERTY_PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT: &str =
        "write.parquet.row-group-check-max-record-count";
    /// Delete-file property key for maximum record-count row group checks.
    pub const PROPERTY_DELETE_PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT: &str =
        "write.delete.parquet.row-group-check-max-record-count";
    /// Default maximum record-count row group check.
    pub const PROPERTY_PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT_DEFAULT: usize = 10_000;
    /// Property key for tracking uncompressed Parquet row group size.
    pub const PROPERTY_PARQUET_ROW_GROUP_SIZE_TRACK_UNCOMPRESSED: &str =
        "write.parquet.row-group-size-track-uncompressed";
    /// Default uncompressed row group size tracking setting.
    pub const PROPERTY_PARQUET_ROW_GROUP_SIZE_TRACK_UNCOMPRESSED_DEFAULT: bool = false;
    /// Property key for maximum Parquet bloom filter bytes.
    pub const PROPERTY_PARQUET_BLOOM_FILTER_MAX_BYTES: &str =
        "write.parquet.bloom-filter-max-bytes";
    /// Default maximum Parquet bloom filter bytes.
    pub const PROPERTY_PARQUET_BLOOM_FILTER_MAX_BYTES_DEFAULT: usize = 1024 * 1024;
    /// Property key for adaptive Parquet bloom filters.
    pub const PROPERTY_PARQUET_BLOOM_FILTER_ADAPTIVE_ENABLED: &str =
        "write.parquet.bloom-filter-adaptive-enabled";
    /// Default adaptive Parquet bloom filter setting.
    pub const PROPERTY_PARQUET_BLOOM_FILTER_ADAPTIVE_ENABLED_DEFAULT: bool = false;
    /// Prefix for per-column Parquet bloom filter false-positive probability.
    pub const PROPERTY_PARQUET_BLOOM_FILTER_COLUMN_FPP_PREFIX: &str =
        "write.parquet.bloom-filter-fpp.column.";
    /// Default per-column Parquet bloom filter false-positive probability.
    pub const PROPERTY_PARQUET_BLOOM_FILTER_COLUMN_FPP_DEFAULT: f64 = 0.01;
    /// Prefix for per-column Parquet bloom filter distinct-value counts.
    pub const PROPERTY_PARQUET_BLOOM_FILTER_COLUMN_NDV_PREFIX: &str =
        "write.parquet.bloom-filter-ndv.column.";
    /// Prefix for enabling Parquet bloom filters by column.
    pub const PROPERTY_PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX: &str =
        "write.parquet.bloom-filter-enabled.column.";
    /// Prefix for enabling Parquet column statistics.
    pub const PROPERTY_PARQUET_COLUMN_STATS_ENABLED_PREFIX: &str =
        "write.parquet.stats-enabled.column.";
    /// Prefix for enabling Parquet dictionary encoding by column.
    pub const PROPERTY_PARQUET_DICT_ENCODING_ENABLED_COLUMN_PREFIX: &str =
        "write.parquet.dict-encoding-enabled.column.";

    /// Property key for Avro compression codec.
    pub const PROPERTY_AVRO_COMPRESSION: &str = "write.avro.compression-codec";
    /// Property key for delete-file Avro compression codec.
    pub const PROPERTY_DELETE_AVRO_COMPRESSION: &str = "write.delete.avro.compression-codec";
    /// Default Avro compression codec.
    pub const PROPERTY_AVRO_COMPRESSION_DEFAULT: &str = "gzip";
    /// Property key for Avro compression level.
    pub const PROPERTY_AVRO_COMPRESSION_LEVEL: &str = "write.avro.compression-level";
    /// Property key for delete-file Avro compression level.
    pub const PROPERTY_DELETE_AVRO_COMPRESSION_LEVEL: &str = "write.delete.avro.compression-level";
    /// Default Avro compression level.
    pub const PROPERTY_AVRO_COMPRESSION_LEVEL_DEFAULT: Option<&str> = None;
    /// Property key for manifest compression codec.
    pub const PROPERTY_MANIFEST_COMPRESSION: &str = "write.manifest.compression-codec";
    /// Default manifest compression codec.
    pub const PROPERTY_MANIFEST_COMPRESSION_DEFAULT: &str = "gzip";
    /// Property key for manifest compression level.
    pub const PROPERTY_MANIFEST_COMPRESSION_LEVEL: &str = "write.manifest.compression-level";
    /// Default manifest compression level.
    pub const PROPERTY_MANIFEST_COMPRESSION_LEVEL_DEFAULT: Option<&str> = None;

    /// Property key for ORC stripe size.
    pub const PROPERTY_ORC_STRIPE_SIZE_BYTES: &str = "write.orc.stripe-size-bytes";
    /// Property key for delete-file ORC stripe size.
    pub const PROPERTY_DELETE_ORC_STRIPE_SIZE_BYTES: &str = "write.delete.orc.stripe-size-bytes";
    /// Default ORC stripe size.
    pub const PROPERTY_ORC_STRIPE_SIZE_BYTES_DEFAULT: u64 = 64 * 1024 * 1024;
    /// Property key for ORC bloom filter columns.
    pub const PROPERTY_ORC_BLOOM_FILTER_COLUMNS: &str = "write.orc.bloom.filter.columns";
    /// Default ORC bloom filter columns.
    pub const PROPERTY_ORC_BLOOM_FILTER_COLUMNS_DEFAULT: &str = "";
    /// Property key for ORC bloom filter false-positive probability.
    pub const PROPERTY_ORC_BLOOM_FILTER_FPP: &str = "write.orc.bloom.filter.fpp";
    /// Default ORC bloom filter false-positive probability.
    pub const PROPERTY_ORC_BLOOM_FILTER_FPP_DEFAULT: f64 = 0.05;
    /// Property key for ORC block size.
    pub const PROPERTY_ORC_BLOCK_SIZE_BYTES: &str = "write.orc.block-size-bytes";
    /// Property key for delete-file ORC block size.
    pub const PROPERTY_DELETE_ORC_BLOCK_SIZE_BYTES: &str = "write.delete.orc.block-size-bytes";
    /// Default ORC block size.
    pub const PROPERTY_ORC_BLOCK_SIZE_BYTES_DEFAULT: u64 = 256 * 1024 * 1024;
    /// Property key for ORC vectorized write batch size.
    pub const PROPERTY_ORC_WRITE_BATCH_SIZE: &str = "write.orc.vectorized.batch-size";
    /// Property key for delete-file ORC vectorized write batch size.
    pub const PROPERTY_DELETE_ORC_WRITE_BATCH_SIZE: &str = "write.delete.orc.vectorized.batch-size";
    /// Default ORC vectorized write batch size.
    pub const PROPERTY_ORC_WRITE_BATCH_SIZE_DEFAULT: usize = 1024;
    /// Property key for ORC compression codec.
    pub const PROPERTY_ORC_COMPRESSION: &str = "write.orc.compression-codec";
    /// Property key for delete-file ORC compression codec.
    pub const PROPERTY_DELETE_ORC_COMPRESSION: &str = "write.delete.orc.compression-codec";
    /// Default ORC compression codec.
    pub const PROPERTY_ORC_COMPRESSION_DEFAULT: &str = "zlib";
    /// Property key for ORC compression strategy.
    pub const PROPERTY_ORC_COMPRESSION_STRATEGY: &str = "write.orc.compression-strategy";
    /// Property key for delete-file ORC compression strategy.
    pub const PROPERTY_DELETE_ORC_COMPRESSION_STRATEGY: &str =
        "write.delete.orc.compression-strategy";
    /// Default ORC compression strategy.
    pub const PROPERTY_ORC_COMPRESSION_STRATEGY_DEFAULT: &str = "speed";

    /// Property key for read split target size.
    pub const PROPERTY_SPLIT_SIZE: &str = "read.split.target-size";
    /// Default read split target size.
    pub const PROPERTY_SPLIT_SIZE_DEFAULT: u64 = 128 * 1024 * 1024;
    /// Property key for metadata split target size.
    pub const PROPERTY_METADATA_SPLIT_SIZE: &str = "read.split.metadata-target-size";
    /// Default metadata split target size.
    pub const PROPERTY_METADATA_SPLIT_SIZE_DEFAULT: u64 = 32 * 1024 * 1024;
    /// Property key for split planning lookback.
    pub const PROPERTY_SPLIT_LOOKBACK: &str = "read.split.planning-lookback";
    /// Default split planning lookback.
    pub const PROPERTY_SPLIT_LOOKBACK_DEFAULT: usize = 10;
    /// Property key for split open-file cost.
    pub const PROPERTY_SPLIT_OPEN_FILE_COST: &str = "read.split.open-file-cost";
    /// Default split open-file cost.
    pub const PROPERTY_SPLIT_OPEN_FILE_COST_DEFAULT: u64 = 4 * 1024 * 1024;
    /// Property key controlling adaptive split sizing.
    pub const PROPERTY_ADAPTIVE_SPLIT_SIZE_ENABLED: &str = "read.split.adaptive-size.enabled";
    /// Default adaptive split sizing setting.
    pub const PROPERTY_ADAPTIVE_SPLIT_SIZE_ENABLED_DEFAULT: bool = true;
    /// Property key controlling Parquet vectorized reads.
    pub const PROPERTY_PARQUET_VECTORIZATION_ENABLED: &str = "read.parquet.vectorization.enabled";
    /// Default Parquet vectorized read setting.
    pub const PROPERTY_PARQUET_VECTORIZATION_ENABLED_DEFAULT: bool = true;
    /// Property key for Parquet vectorized read batch size.
    pub const PROPERTY_PARQUET_BATCH_SIZE: &str = "read.parquet.vectorization.batch-size";
    /// Default Parquet vectorized read batch size.
    pub const PROPERTY_PARQUET_BATCH_SIZE_DEFAULT: usize = 5000;
    /// Property key controlling ORC vectorized reads.
    pub const PROPERTY_ORC_VECTORIZATION_ENABLED: &str = "read.orc.vectorization.enabled";
    /// Default ORC vectorized read setting.
    pub const PROPERTY_ORC_VECTORIZATION_ENABLED_DEFAULT: bool = false;
    /// Property key for ORC vectorized read batch size.
    pub const PROPERTY_ORC_BATCH_SIZE: &str = "read.orc.vectorization.batch-size";
    /// Default ORC vectorized read batch size.
    pub const PROPERTY_ORC_BATCH_SIZE_DEFAULT: usize = 5000;
    /// Property key for data planning mode.
    pub const PROPERTY_DATA_PLANNING_MODE: &str = "read.data-planning-mode";
    /// Property key for delete planning mode.
    pub const PROPERTY_DELETE_PLANNING_MODE: &str = "read.delete-planning-mode";
    /// Default planning mode.
    pub const PROPERTY_PLANNING_MODE_DEFAULT: &str = "auto";
    /// Property key declaring identifier fields reliable.
    pub const PROPERTY_IDENTIFIER_FIELDS_RELY: &str = "identifier-fields.rely";
    /// Default identifier-field reliability setting.
    pub const PROPERTY_IDENTIFIER_FIELDS_RELY_DEFAULT: bool = false;

    /// Property key controlling object-store locations.
    pub const PROPERTY_OBJECT_STORE_ENABLED: &str = "write.object-storage.enabled";
    /// Default object-store location setting.
    pub const PROPERTY_OBJECT_STORE_ENABLED_DEFAULT: bool = false;
    /// Property key controlling partitioned paths for object storage.
    pub const PROPERTY_WRITE_OBJECT_STORE_PARTITIONED_PATHS: &str =
        "write.object-storage.partitioned-paths";
    /// Default partitioned-path setting for object storage.
    pub const PROPERTY_WRITE_OBJECT_STORE_PARTITIONED_PATHS_DEFAULT: bool = true;
    /// Deprecated object-store path property.
    pub const PROPERTY_OBJECT_STORE_PATH: &str = "write.object-storage.path";
    /// Property key for a custom location provider implementation.
    pub const PROPERTY_WRITE_LOCATION_PROVIDER_IMPL: &str = "write.location-provider.impl";
    /// Deprecated folder-storage location property.
    pub const PROPERTY_WRITE_FOLDER_STORAGE_LOCATION: &str = "write.folder-storage.path";
    /// Property key for the table data location.
    pub const PROPERTY_WRITE_DATA_LOCATION: &str = "write.data.path";
    /// Alias matching Java's metadata location property name.
    pub const PROPERTY_WRITE_METADATA_LOCATION: &str = Self::PROPERTY_WRITE_METADATA_PATH;
    /// Deprecated property controlling manifest-list writes.
    pub const PROPERTY_MANIFEST_LISTS_ENABLED: &str = "write.manifest-lists.enabled";
    /// Default manifest-list write setting.
    pub const PROPERTY_MANIFEST_LISTS_ENABLED_DEFAULT: bool = true;
    /// Alias matching Java's metadata compression property name.
    pub const PROPERTY_METADATA_COMPRESSION: &str = Self::PROPERTY_METADATA_COMPRESSION_CODEC;
    /// Alias matching Java's metadata compression default name.
    pub const PROPERTY_METADATA_COMPRESSION_DEFAULT: &str =
        Self::PROPERTY_METADATA_COMPRESSION_CODEC_DEFAULT;
    /// Property key controlling deletion of old metadata after commit.
    pub const PROPERTY_METADATA_DELETE_AFTER_COMMIT_ENABLED: &str =
        "write.metadata.delete-after-commit.enabled";
    /// Default old-metadata deletion setting.
    pub const PROPERTY_METADATA_DELETE_AFTER_COMMIT_ENABLED_DEFAULT: bool = false;
    /// Property key for the maximum inferred column metric defaults.
    pub const PROPERTY_METRICS_MAX_INFERRED_COLUMN_DEFAULTS: &str =
        "write.metadata.metrics.max-inferred-column-defaults";
    /// Default maximum inferred column metric defaults.
    pub const PROPERTY_METRICS_MAX_INFERRED_COLUMN_DEFAULTS_DEFAULT: usize = 100;
    /// Prefix for per-column metrics modes.
    pub const PROPERTY_METRICS_MODE_COLUMN_CONF_PREFIX: &str = "write.metadata.metrics.column.";
    /// Property key for the default write metrics mode.
    pub const PROPERTY_DEFAULT_WRITE_METRICS_MODE: &str = "write.metadata.metrics.default";
    /// Default write metrics mode.
    pub const PROPERTY_DEFAULT_WRITE_METRICS_MODE_DEFAULT: &str = "truncate(16)";
    /// Property key for the default schema name mapping.
    pub const PROPERTY_DEFAULT_NAME_MAPPING: &str = "schema.name-mapping.default";
    /// Property key enabling write-audit-publish behavior.
    pub const PROPERTY_WRITE_AUDIT_PUBLISH_ENABLED: &str = "write.wap.enabled";
    /// Default write-audit-publish setting.
    pub const PROPERTY_WRITE_AUDIT_PUBLISH_ENABLED_DEFAULT: bool = false;
    /// Property key for delete-file target size.
    pub const PROPERTY_DELETE_TARGET_FILE_SIZE_BYTES: &str = "write.delete.target-file-size-bytes";
    /// Default delete-file target size.
    pub const PROPERTY_DELETE_TARGET_FILE_SIZE_BYTES_DEFAULT: usize = 64 * 1024 * 1024;

    /// Deprecated Spark fanout writer property.
    pub const PROPERTY_SPARK_WRITE_PARTITIONED_FANOUT_ENABLED: &str = "write.spark.fanout.enabled";
    /// Default deprecated Spark fanout writer setting.
    pub const PROPERTY_SPARK_WRITE_PARTITIONED_FANOUT_ENABLED_DEFAULT: bool = false;
    /// Deprecated Spark accept-any-schema property.
    pub const PROPERTY_SPARK_WRITE_ACCEPT_ANY_SCHEMA: &str = "write.spark.accept-any-schema";
    /// Default deprecated Spark accept-any-schema setting.
    pub const PROPERTY_SPARK_WRITE_ACCEPT_ANY_SCHEMA_DEFAULT: bool = false;
    /// Deprecated Spark automatic schema evolution property.
    pub const PROPERTY_SPARK_WRITE_AUTO_SCHEMA_EVOLUTION: &str =
        "write.spark.auto-schema-evolution.enabled";
    /// Default deprecated Spark automatic schema evolution setting.
    pub const PROPERTY_SPARK_WRITE_AUTO_SCHEMA_EVOLUTION_DEFAULT: bool = true;
    /// Deprecated Spark advisory partition size property.
    pub const PROPERTY_SPARK_WRITE_ADVISORY_PARTITION_SIZE_BYTES: &str =
        "write.spark.advisory-partition-size-bytes";
    /// Property key for snapshot ID inheritance compatibility.
    pub const PROPERTY_SNAPSHOT_ID_INHERITANCE_ENABLED: &str =
        "compatibility.snapshot-id-inheritance.enabled";
    /// Default snapshot ID inheritance setting.
    pub const PROPERTY_SNAPSHOT_ID_INHERITANCE_ENABLED_DEFAULT: bool = false;
    /// Property key enabling Hive engine behavior.
    pub const PROPERTY_ENGINE_HIVE_ENABLED: &str = "engine.hive.enabled";
    /// Default Hive engine setting.
    pub const PROPERTY_ENGINE_HIVE_ENABLED_DEFAULT: bool = false;
    /// Property key enabling Hive locking.
    pub const PROPERTY_HIVE_LOCK_ENABLED: &str = "engine.hive.lock-enabled";
    /// Default Hive lock setting.
    pub const PROPERTY_HIVE_LOCK_ENABLED_DEFAULT: bool = true;
    /// Property key for write distribution mode.
    pub const PROPERTY_WRITE_DISTRIBUTION_MODE: &str = "write.distribution-mode";
    /// No-distribution mode value.
    pub const PROPERTY_WRITE_DISTRIBUTION_MODE_NONE: &str = "none";
    /// Hash-distribution mode value.
    pub const PROPERTY_WRITE_DISTRIBUTION_MODE_HASH: &str = "hash";
    /// Range-distribution mode value.
    pub const PROPERTY_WRITE_DISTRIBUTION_MODE_RANGE: &str = "range";

    /// Property key for delete granularity.
    pub const PROPERTY_DELETE_GRANULARITY: &str = "write.delete.granularity";
    /// Default delete granularity.
    pub const PROPERTY_DELETE_GRANULARITY_DEFAULT: &str = "partition";
    /// Property key for delete isolation level.
    pub const PROPERTY_DELETE_ISOLATION_LEVEL: &str = "write.delete.isolation-level";
    /// Default delete isolation level.
    pub const PROPERTY_DELETE_ISOLATION_LEVEL_DEFAULT: &str = "serializable";
    /// Property key for delete operation mode.
    pub const PROPERTY_DELETE_MODE: &str = "write.delete.mode";
    /// Default delete operation mode.
    pub const PROPERTY_DELETE_MODE_DEFAULT: &str = "copy-on-write";
    /// Property key for delete distribution mode.
    pub const PROPERTY_DELETE_DISTRIBUTION_MODE: &str = "write.delete.distribution-mode";
    /// Property key for update isolation level.
    pub const PROPERTY_UPDATE_ISOLATION_LEVEL: &str = "write.update.isolation-level";
    /// Default update isolation level.
    pub const PROPERTY_UPDATE_ISOLATION_LEVEL_DEFAULT: &str = "serializable";
    /// Property key for update operation mode.
    pub const PROPERTY_UPDATE_MODE: &str = "write.update.mode";
    /// Default update operation mode.
    pub const PROPERTY_UPDATE_MODE_DEFAULT: &str = "copy-on-write";
    /// Property key for update distribution mode.
    pub const PROPERTY_UPDATE_DISTRIBUTION_MODE: &str = "write.update.distribution-mode";
    /// Property key for merge isolation level.
    pub const PROPERTY_MERGE_ISOLATION_LEVEL: &str = "write.merge.isolation-level";
    /// Default merge isolation level.
    pub const PROPERTY_MERGE_ISOLATION_LEVEL_DEFAULT: &str = "serializable";
    /// Property key for merge operation mode.
    pub const PROPERTY_MERGE_MODE: &str = "write.merge.mode";
    /// Default merge operation mode.
    pub const PROPERTY_MERGE_MODE_DEFAULT: &str = "copy-on-write";
    /// Property key for merge distribution mode.
    pub const PROPERTY_MERGE_DISTRIBUTION_MODE: &str = "write.merge.distribution-mode";
    /// Property key enabling upserts.
    pub const PROPERTY_UPSERT_ENABLED: &str = "write.upsert.enabled";
    /// Default upsert setting.
    pub const PROPERTY_UPSERT_ENABLED_DEFAULT: bool = false;
    /// Alias matching Java's encryption table key property name.
    pub const PROPERTY_ENCRYPTION_TABLE_KEY: &str = Self::PROPERTY_ENCRYPTION_KEY_ID;
    /// Alias matching Java's encryption DEK length property name.
    pub const PROPERTY_ENCRYPTION_DEK_LENGTH: &str = Self::PROPERTY_ENCRYPTION_DATA_KEY_LENGTH;
    /// Alias matching Java's encryption DEK length default name.
    pub const PROPERTY_ENCRYPTION_DEK_LENGTH_DEFAULT: usize =
        Self::PROPERTY_ENCRYPTION_DATA_KEY_LENGTH_DEFAULT;
    /// Default encryption AAD length.
    pub const PROPERTY_ENCRYPTION_AAD_LENGTH_DEFAULT: usize = 16;
}

impl TryFrom<&HashMap<String, String>> for TableProperties {
    type Error = Error;

    fn try_from(props: &HashMap<String, String>) -> Result<Self> {
        Ok(Self {
            commit_num_retries: parse_property(
                props,
                Self::PROPERTY_COMMIT_NUM_RETRIES,
                Self::PROPERTY_COMMIT_NUM_RETRIES_DEFAULT,
            )?,
            commit_min_retry_wait_ms: parse_property(
                props,
                Self::PROPERTY_COMMIT_MIN_RETRY_WAIT_MS,
                Self::PROPERTY_COMMIT_MIN_RETRY_WAIT_MS_DEFAULT,
            )?,
            commit_max_retry_wait_ms: parse_property(
                props,
                Self::PROPERTY_COMMIT_MAX_RETRY_WAIT_MS,
                Self::PROPERTY_COMMIT_MAX_RETRY_WAIT_MS_DEFAULT,
            )?,
            commit_total_retry_timeout_ms: parse_property(
                props,
                Self::PROPERTY_COMMIT_TOTAL_RETRY_TIME_MS,
                Self::PROPERTY_COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT,
            )?,
            write_format_default: parse_property(
                props,
                Self::PROPERTY_DEFAULT_FILE_FORMAT,
                Self::PROPERTY_DEFAULT_FILE_FORMAT_DEFAULT.to_string(),
            )?,
            write_target_file_size_bytes: parse_property(
                props,
                Self::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES,
                Self::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT,
            )?,
            write_metadata_path: parse_location_property(
                props,
                Self::PROPERTY_WRITE_METADATA_PATH,
            )?,
            metadata_compression_codec: parse_metadata_file_compression(props)?,
            write_datafusion_fanout_enabled: parse_property(
                props,
                Self::PROPERTY_DATAFUSION_WRITE_FANOUT_ENABLED,
                Self::PROPERTY_DATAFUSION_WRITE_FANOUT_ENABLED_DEFAULT,
            )?,
            gc_enabled: parse_property(
                props,
                Self::PROPERTY_GC_ENABLED,
                Self::PROPERTY_GC_ENABLED_DEFAULT,
            )?,
            max_snapshot_age_ms: parse_property(
                props,
                Self::PROPERTY_MAX_SNAPSHOT_AGE_MS,
                Self::PROPERTY_MAX_SNAPSHOT_AGE_MS_DEFAULT,
            )?,
            min_snapshots_to_keep: parse_property(
                props,
                Self::PROPERTY_MIN_SNAPSHOTS_TO_KEEP,
                Self::PROPERTY_MIN_SNAPSHOTS_TO_KEEP_DEFAULT,
            )?,
            max_ref_age_ms: parse_property(
                props,
                Self::PROPERTY_MAX_REF_AGE_MS,
                Self::PROPERTY_MAX_REF_AGE_MS_DEFAULT,
            )?,
            cdc_enabled: parse_property(
                props,
                Self::PROPERTY_PARQUET_CDC_ENABLED,
                Self::PROPERTY_PARQUET_CDC_ENABLED_DEFAULT,
            )?,
            cdc_min_chunk_size: parse_property(
                props,
                Self::PROPERTY_PARQUET_CDC_MIN_CHUNK_SIZE,
                Self::PROPERTY_PARQUET_CDC_MIN_CHUNK_SIZE_DEFAULT,
            )?,
            cdc_max_chunk_size: parse_property(
                props,
                Self::PROPERTY_PARQUET_CDC_MAX_CHUNK_SIZE,
                Self::PROPERTY_PARQUET_CDC_MAX_CHUNK_SIZE_DEFAULT,
            )?,
            cdc_norm_level: parse_property(
                props,
                Self::PROPERTY_PARQUET_CDC_NORM_LEVEL,
                Self::PROPERTY_PARQUET_CDC_NORM_LEVEL_DEFAULT,
            )?,
            encryption_key_id: props.get(Self::PROPERTY_ENCRYPTION_KEY_ID).cloned(),
            encryption_data_key_length: parse_property(
                props,
                Self::PROPERTY_ENCRYPTION_DATA_KEY_LENGTH,
                Self::PROPERTY_ENCRYPTION_DATA_KEY_LENGTH_DEFAULT,
            )?,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::compression::CompressionCodec;

    fn parse(properties: &HashMap<String, String>) -> Result<ParsedTableProperties> {
        serde_json::to_value(properties)
            .and_then(serde_json::from_value)
            .map_err(|error| Error::new(ErrorKind::DataInvalid, error.to_string()))
    }

    #[test]
    fn test_parsed_table_properties_default() {
        let table_properties = ParsedTableProperties::default();
        assert_eq!(
            table_properties.commit_num_retries(),
            TableProperties::PROPERTY_COMMIT_NUM_RETRIES_DEFAULT
        );
        assert_eq!(
            table_properties.commit_min_retry_wait_ms(),
            TableProperties::PROPERTY_COMMIT_MIN_RETRY_WAIT_MS_DEFAULT
        );
        assert_eq!(
            table_properties.commit_max_retry_wait_ms(),
            TableProperties::PROPERTY_COMMIT_MAX_RETRY_WAIT_MS_DEFAULT
        );
        assert_eq!(
            table_properties.write_format_default(),
            DataFileFormat::Parquet
        );
        assert_eq!(
            table_properties.write_target_file_size_bytes(),
            TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT
        );
        // Test compression defaults (none means CompressionCodec::None)
        assert_eq!(
            table_properties.metadata_compression_codec(),
            CompressionCodec::None
        );
        assert_eq!(
            table_properties.gc_enabled(),
            TableProperties::PROPERTY_GC_ENABLED_DEFAULT
        );
        assert_eq!(
            table_properties.max_snapshot_age_ms(),
            TableProperties::PROPERTY_MAX_SNAPSHOT_AGE_MS_DEFAULT
        );
        assert_eq!(
            table_properties.min_snapshots_to_keep(),
            TableProperties::PROPERTY_MIN_SNAPSHOTS_TO_KEEP_DEFAULT
        );
        assert_eq!(
            table_properties.max_ref_age_ms(),
            TableProperties::PROPERTY_MAX_REF_AGE_MS_DEFAULT
        );
    }

    #[test]
    fn test_existing_table_properties_api() {
        let properties = HashMap::from([
            (
                TableProperties::PROPERTY_COMMIT_NUM_RETRIES.to_string(),
                "8".to_string(),
            ),
            (
                TableProperties::PROPERTY_DEFAULT_FILE_FORMAT.to_string(),
                "orc".to_string(),
            ),
            (
                TableProperties::PROPERTY_WRITE_METADATA_PATH.to_string(),
                "s3://warehouse/table/metadata/".to_string(),
            ),
        ]);

        let parsed = TableProperties::try_from(&properties).unwrap();
        assert_eq!(parsed.commit_num_retries, 8);
        assert_eq!(parsed.write_format_default, "orc");
        assert_eq!(
            parsed.write_metadata_path.as_deref(),
            Some("s3://warehouse/table/metadata")
        );
    }

    #[test]
    fn test_parsed_table_properties_modifiers() {
        let table_properties = ParsedTableProperties::default()
            .with_commit_num_retries(9)
            .with_comment(Some("orders table".to_string()))
            .with_write_format_default(DataFileFormat::Avro);

        assert_eq!(table_properties.commit_num_retries(), 9);
        assert_eq!(table_properties.comment(), Some("orders table".to_string()));
        assert_eq!(
            table_properties.write_format_default(),
            DataFileFormat::Avro
        );
    }

    #[test]
    fn test_properties_from_java_table_properties() {
        let properties = parse(&HashMap::from([
            (
                TableProperties::PROPERTY_COMMIT_NUM_STATUS_CHECKS.to_string(),
                "7".to_string(),
            ),
            (
                TableProperties::PROPERTY_DELETE_DEFAULT_FILE_FORMAT.to_string(),
                "orc".to_string(),
            ),
            (
                TableProperties::PROPERTY_PARQUET_PAGE_ROW_LIMIT.to_string(),
                "1000".to_string(),
            ),
            (
                format!(
                    "{}customer_id",
                    TableProperties::PROPERTY_PARQUET_BLOOM_FILTER_COLUMN_FPP_PREFIX
                ),
                "0.02".to_string(),
            ),
            (
                TableProperties::PROPERTY_DELETE_AVRO_COMPRESSION.to_string(),
                "snappy".to_string(),
            ),
            (
                TableProperties::PROPERTY_ORC_BLOOM_FILTER_FPP.to_string(),
                "0.1".to_string(),
            ),
            (
                TableProperties::PROPERTY_SPLIT_LOOKBACK.to_string(),
                "25".to_string(),
            ),
            (
                TableProperties::PROPERTY_WRITE_OBJECT_STORE_PARTITIONED_PATHS.to_string(),
                "false".to_string(),
            ),
            (
                TableProperties::PROPERTY_METADATA_PREVIOUS_VERSIONS_MAX.to_string(),
                "20".to_string(),
            ),
            (
                format!(
                    "{}customer_id",
                    TableProperties::PROPERTY_METRICS_MODE_COLUMN_CONF_PREFIX
                ),
                "full".to_string(),
            ),
            (
                TableProperties::PROPERTY_SPARK_WRITE_ADVISORY_PARTITION_SIZE_BYTES.to_string(),
                "4096".to_string(),
            ),
            (
                TableProperties::PROPERTY_DELETE_MODE.to_string(),
                "merge-on-read".to_string(),
            ),
            (
                TableProperties::PROPERTY_UPSERT_ENABLED.to_string(),
                "true".to_string(),
            ),
        ]))
        .unwrap();

        assert_eq!(properties.commit_num_status_checks(), 7);
        assert_eq!(properties.delete_format_default(), DataFileFormat::Orc);
        assert_eq!(properties.parquet_page_row_limit(), 1000);
        assert_eq!(
            properties.parquet_bloom_filter_column_fpp()["customer_id"],
            0.02
        );
        assert_eq!(properties.delete_avro_compression(), "snappy");
        assert_eq!(properties.orc_bloom_filter_fpp(), 0.1);
        assert_eq!(properties.split_lookback(), 25);
        assert!(!properties.write_object_store_partitioned_paths());
        assert_eq!(properties.metadata_previous_versions_max(), 20);
        assert_eq!(
            properties.metrics_mode_column_config()["customer_id"],
            "full"
        );
        assert_eq!(
            properties.spark_write_advisory_partition_size_bytes(),
            Some(4096)
        );
        assert_eq!(properties.delete_mode(), "merge-on-read");
        assert!(properties.upsert_enabled());
    }

    #[test]
    fn test_empty_properties_match_default() {
        let parsed = parse(&HashMap::new()).unwrap();
        let defaults = ParsedTableProperties::default();

        assert_eq!(parsed.commit_num_retries(), defaults.commit_num_retries());
        assert_eq!(
            parsed.commit_min_retry_wait_ms(),
            defaults.commit_min_retry_wait_ms()
        );
        assert_eq!(
            parsed.commit_max_retry_wait_ms(),
            defaults.commit_max_retry_wait_ms()
        );
        assert_eq!(
            parsed.commit_total_retry_timeout_ms(),
            defaults.commit_total_retry_timeout_ms()
        );
        assert_eq!(
            parsed.write_format_default(),
            defaults.write_format_default()
        );
        assert_eq!(
            parsed.write_target_file_size_bytes(),
            defaults.write_target_file_size_bytes()
        );
        assert_eq!(parsed.write_metadata_path(), defaults.write_metadata_path());
        assert_eq!(
            parsed.metadata_compression_codec(),
            defaults.metadata_compression_codec()
        );
        assert_eq!(
            parsed.write_datafusion_fanout_enabled(),
            defaults.write_datafusion_fanout_enabled()
        );
        assert_eq!(parsed.gc_enabled(), defaults.gc_enabled());
        assert_eq!(parsed.max_snapshot_age_ms(), defaults.max_snapshot_age_ms());
        assert_eq!(
            parsed.min_snapshots_to_keep(),
            defaults.min_snapshots_to_keep()
        );
        assert_eq!(parsed.max_ref_age_ms(), defaults.max_ref_age_ms());
        assert_eq!(parsed.cdc_enabled(), defaults.cdc_enabled());
        assert_eq!(parsed.cdc_min_chunk_size(), defaults.cdc_min_chunk_size());
        assert_eq!(parsed.cdc_max_chunk_size(), defaults.cdc_max_chunk_size());
        assert_eq!(parsed.cdc_norm_level(), defaults.cdc_norm_level());
        assert_eq!(parsed.encryption_key_id(), defaults.encryption_key_id());
        assert_eq!(
            parsed.encryption_data_key_length(),
            defaults.encryption_data_key_length()
        );
    }

    #[test]
    fn test_parsed_table_properties_json_round_trip() {
        let properties = parse(&HashMap::from([
            (
                TableProperties::PROPERTY_DEFAULT_FILE_FORMAT.to_string(),
                "ORC".to_string(),
            ),
            (
                TableProperties::PROPERTY_WRITE_METADATA_PATH.to_string(),
                "s3://warehouse/table/metadata/".to_string(),
            ),
            (
                TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC.to_string(),
                "GZIP".to_string(),
            ),
            (
                TableProperties::PROPERTY_ENCRYPTION_KEY_ID.to_string(),
                "table-key".to_string(),
            ),
        ]))
        .unwrap();

        let json = serde_json::to_value(&properties).unwrap();
        assert_eq!(json[TableProperties::PROPERTY_DEFAULT_FILE_FORMAT], "orc");
        assert_eq!(
            json[TableProperties::PROPERTY_WRITE_METADATA_PATH],
            "s3://warehouse/table/metadata"
        );
        assert_eq!(
            json[TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC],
            "gzip"
        );

        let decoded: ParsedTableProperties = serde_json::from_value(json).unwrap();
        assert_eq!(decoded.write_format_default(), DataFileFormat::Orc);
        assert_eq!(
            decoded.write_metadata_path(),
            Some("s3://warehouse/table/metadata".to_string())
        );
        assert_eq!(
            decoded.metadata_compression_codec(),
            CompressionCodec::gzip_default()
        );
        assert_eq!(decoded.encryption_key_id(), Some("table-key".to_string()));
    }

    #[test]
    fn test_default_parsed_table_properties_json_round_trip() {
        let defaults = ParsedTableProperties::default();
        let json = serde_json::to_value(&defaults).unwrap();

        assert!(
            json.get(TableProperties::PROPERTY_WRITE_METADATA_PATH)
                .is_none()
        );
        assert!(
            json.get(TableProperties::PROPERTY_ENCRYPTION_KEY_ID)
                .is_none()
        );

        let decoded: ParsedTableProperties = serde_json::from_value(json).unwrap();
        assert_eq!(decoded.write_format_default(), DataFileFormat::Parquet);
        assert_eq!(decoded.write_metadata_path(), None);
        assert_eq!(decoded.encryption_key_id(), None);
    }

    #[test]
    fn test_parsed_table_properties_history_expire_overrides() {
        let props = HashMap::from([
            (
                TableProperties::PROPERTY_MAX_SNAPSHOT_AGE_MS.to_string(),
                "1234".to_string(),
            ),
            (
                TableProperties::PROPERTY_MIN_SNAPSHOTS_TO_KEEP.to_string(),
                "7".to_string(),
            ),
            (
                TableProperties::PROPERTY_MAX_REF_AGE_MS.to_string(),
                "5678".to_string(),
            ),
        ]);
        let table_properties = parse(&props).unwrap();
        assert_eq!(table_properties.max_snapshot_age_ms(), 1234);
        assert_eq!(table_properties.min_snapshots_to_keep(), 7);
        assert_eq!(table_properties.max_ref_age_ms(), 5678);
    }

    #[test]
    fn test_parsed_table_properties_write_metadata_path() {
        // Test unset
        let table_properties = parse(&HashMap::new()).unwrap();
        assert_eq!(table_properties.write_metadata_path(), None);

        // Test empty path is invalid
        let props = HashMap::from([(
            TableProperties::PROPERTY_WRITE_METADATA_PATH.to_string(),
            String::new(),
        )]);
        let error = parse(&props).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert!(
            error
                .message()
                .contains(TableProperties::PROPERTY_WRITE_METADATA_PATH)
        );

        let props = HashMap::from([(
            TableProperties::PROPERTY_WRITE_METADATA_PATH.to_string(),
            "s3://other-bucket/custom-meta/".to_string(),
        )]);
        let table_properties = parse(&props).unwrap();
        assert_eq!(
            table_properties.write_metadata_path(),
            Some("s3://other-bucket/custom-meta".to_string())
        );
    }

    #[test]
    fn test_strip_trailing_slash() {
        for (path, expected) in [
            ("s3://bucket/db/tbl", "s3://bucket/db/tbl"),
            ("s3://bucket/db/tbl/", "s3://bucket/db/tbl"),
            ("s3://bucket/db/tbl////", "s3://bucket/db/tbl"),
            ("blobstore://", "blobstore://"),
            ("blobstore:///", "blobstore://"),
            ("file:///", "file://"),
            ("////", ""),
            ("", ""),
        ] {
            assert_eq!(strip_trailing_slash(path), expected);
        }
    }

    #[test]
    fn test_parsed_table_properties_compression() {
        let props = HashMap::from([(
            TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC.to_string(),
            "gzip".to_string(),
        )]);
        let table_properties = parse(&props).unwrap();
        assert_eq!(
            table_properties.metadata_compression_codec(),
            CompressionCodec::gzip_default()
        );
    }

    #[test]
    fn test_parsed_table_properties_compression_none() {
        let props = HashMap::from([(
            TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC.to_string(),
            "none".to_string(),
        )]);
        let table_properties = parse(&props).unwrap();
        assert_eq!(
            table_properties.metadata_compression_codec(),
            CompressionCodec::None
        );
    }

    #[test]
    fn test_parsed_table_properties_compression_case_insensitive() {
        // Test uppercase
        let props_upper = HashMap::from([(
            TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC.to_string(),
            "GZIP".to_string(),
        )]);
        let table_properties = parse(&props_upper).unwrap();
        assert_eq!(
            table_properties.metadata_compression_codec(),
            CompressionCodec::gzip_default()
        );

        // Test mixed case
        let props_mixed = HashMap::from([(
            TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC.to_string(),
            "GzIp".to_string(),
        )]);
        let table_properties = parse(&props_mixed).unwrap();
        assert_eq!(
            table_properties.metadata_compression_codec(),
            CompressionCodec::gzip_default()
        );

        // Test "NONE" should also be case-insensitive
        let props_none_upper = HashMap::from([(
            TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC.to_string(),
            "NONE".to_string(),
        )]);
        let table_properties = parse(&props_none_upper).unwrap();
        assert_eq!(
            table_properties.metadata_compression_codec(),
            CompressionCodec::None
        );
    }

    #[test]
    fn test_parsed_table_properties_valid() {
        let props = HashMap::from([
            (
                TableProperties::PROPERTY_COMMIT_NUM_RETRIES.to_string(),
                "10".to_string(),
            ),
            (
                TableProperties::PROPERTY_COMMIT_MAX_RETRY_WAIT_MS.to_string(),
                "20".to_string(),
            ),
            (
                TableProperties::PROPERTY_DEFAULT_FILE_FORMAT.to_string(),
                "avro".to_string(),
            ),
            (
                TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES.to_string(),
                "512".to_string(),
            ),
            (
                TableProperties::PROPERTY_GC_ENABLED.to_string(),
                "false".to_string(),
            ),
        ]);
        let table_properties = parse(&props).unwrap();
        assert_eq!(table_properties.commit_num_retries(), 10);
        assert_eq!(table_properties.commit_max_retry_wait_ms(), 20);
        assert_eq!(
            table_properties.write_format_default(),
            DataFileFormat::Avro
        );
        assert_eq!(table_properties.write_target_file_size_bytes(), 512);
        assert!(!table_properties.gc_enabled());
    }

    #[test]
    fn test_parsed_table_properties_invalid() {
        let invalid_retries = HashMap::from([(
            TableProperties::PROPERTY_COMMIT_NUM_RETRIES.to_string(),
            "abc".to_string(),
        )]);

        let table_properties = parse(&invalid_retries).unwrap_err();
        assert!(
            table_properties.to_string().contains(
                "Invalid value for commit.retry.num-retries: invalid digit found in string"
            )
        );

        let invalid_min_wait = HashMap::from([(
            TableProperties::PROPERTY_COMMIT_MIN_RETRY_WAIT_MS.to_string(),
            "abc".to_string(),
        )]);
        let table_properties = parse(&invalid_min_wait).unwrap_err();
        assert!(
            table_properties.to_string().contains(
                "Invalid value for commit.retry.min-wait-ms: invalid digit found in string"
            )
        );

        let invalid_max_wait = HashMap::from([(
            TableProperties::PROPERTY_COMMIT_MAX_RETRY_WAIT_MS.to_string(),
            "abc".to_string(),
        )]);
        let table_properties = parse(&invalid_max_wait).unwrap_err();
        assert!(
            table_properties.to_string().contains(
                "Invalid value for commit.retry.max-wait-ms: invalid digit found in string"
            )
        );

        let invalid_target_size = HashMap::from([(
            TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES.to_string(),
            "abc".to_string(),
        )]);
        let table_properties = parse(&invalid_target_size).unwrap_err();
        assert!(table_properties.to_string().contains(
            "Invalid value for write.target-file-size-bytes: invalid digit found in string"
        ));

        let invalid_gc_enabled = HashMap::from([(
            TableProperties::PROPERTY_GC_ENABLED.to_string(),
            "notabool".to_string(),
        )]);
        let table_properties = parse(&invalid_gc_enabled).unwrap_err();
        assert!(
            table_properties
                .to_string()
                .contains("Invalid value for gc.enabled")
        );
    }

    #[test]
    fn test_parsed_table_properties_compression_invalid_rejected() {
        let invalid_codecs = ["lz4", "zstd", "snappy"];

        for codec in invalid_codecs {
            let props = HashMap::from([(
                TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC.to_string(),
                codec.to_string(),
            )]);
            let err = parse(&props).unwrap_err();
            let err_msg = err.to_string();
            assert!(
                err_msg.contains(&format!("Invalid metadata compression codec: {codec}")),
                "Expected error message to contain codec '{codec}', got: {err_msg}"
            );
            assert!(
                err_msg.contains("Only 'none' and 'gzip' are supported"),
                "Expected error message to contain supported codecs, got: {err_msg}"
            );
        }
    }

    #[test]
    fn test_parse_metadata_file_compression_valid() {
        // Test with "none"
        let props = HashMap::from([(
            TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC.to_string(),
            "none".to_string(),
        )]);
        assert_eq!(
            parse(&props).unwrap().metadata_compression_codec(),
            CompressionCodec::None
        );

        // Test with empty string
        let props = HashMap::from([(
            TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC.to_string(),
            "".to_string(),
        )]);
        assert_eq!(
            parse(&props).unwrap().metadata_compression_codec(),
            CompressionCodec::None
        );

        // Test with "gzip"
        let props = HashMap::from([(
            TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC.to_string(),
            "gzip".to_string(),
        )]);
        assert_eq!(
            parse(&props).unwrap().metadata_compression_codec(),
            CompressionCodec::gzip_default()
        );

        // Test case insensitivity - "NONE"
        let props = HashMap::from([(
            TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC.to_string(),
            "NONE".to_string(),
        )]);
        assert_eq!(
            parse(&props).unwrap().metadata_compression_codec(),
            CompressionCodec::None
        );

        // Test case insensitivity - "GZIP"
        let props = HashMap::from([(
            TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC.to_string(),
            "GZIP".to_string(),
        )]);
        assert_eq!(
            parse(&props).unwrap().metadata_compression_codec(),
            CompressionCodec::gzip_default()
        );

        // Test case insensitivity - "GzIp"
        let props = HashMap::from([(
            TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC.to_string(),
            "GzIp".to_string(),
        )]);
        assert_eq!(
            parse(&props).unwrap().metadata_compression_codec(),
            CompressionCodec::gzip_default()
        );

        // Test default when property is missing
        let props = HashMap::new();
        assert_eq!(
            parse(&props).unwrap().metadata_compression_codec(),
            CompressionCodec::None
        );
    }

    #[test]
    fn test_parse_metadata_file_compression_invalid() {
        let invalid_codecs = ["lz4", "zstd", "snappy"];

        for codec in invalid_codecs {
            let props = HashMap::from([(
                TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC.to_string(),
                codec.to_string(),
            )]);
            let err = parse(&props).unwrap_err();
            let err_msg = err.to_string();
            assert!(
                err_msg.contains("Invalid metadata compression codec"),
                "Expected error message to contain 'Invalid metadata compression codec', got: {err_msg}"
            );
            assert!(
                err_msg.contains("Only 'none' and 'gzip' are supported"),
                "Expected error message to contain supported codecs, got: {err_msg}"
            );
        }
    }

    #[test]
    fn test_cdc_disabled_by_default() {
        let props = HashMap::new();
        let tp = parse(&props).unwrap();
        assert!(!tp.cdc_enabled());
    }

    #[test]
    fn test_cdc_enabled_via_flag() {
        let props = HashMap::from([(
            TableProperties::PROPERTY_PARQUET_CDC_ENABLED.to_string(),
            "true".to_string(),
        )]);
        let tp = parse(&props).unwrap();
        assert!(tp.cdc_enabled());
        assert_eq!(tp.cdc_min_chunk_size(), 256 * 1024);
        assert_eq!(tp.cdc_max_chunk_size(), 1024 * 1024);
        assert_eq!(tp.cdc_norm_level(), 0);
    }

    #[test]
    fn test_cdc_size_props_alone_do_not_enable() {
        let props = HashMap::from([(
            TableProperties::PROPERTY_PARQUET_CDC_MIN_CHUNK_SIZE.to_string(),
            "262144".to_string(),
        )]);
        let tp = parse(&props).unwrap();
        assert!(!tp.cdc_enabled());
    }

    #[test]
    fn test_cdc_custom_values() {
        let props = HashMap::from([
            (
                TableProperties::PROPERTY_PARQUET_CDC_ENABLED.to_string(),
                "true".to_string(),
            ),
            (
                TableProperties::PROPERTY_PARQUET_CDC_MIN_CHUNK_SIZE.to_string(),
                "200000".to_string(),
            ),
            (
                TableProperties::PROPERTY_PARQUET_CDC_MAX_CHUNK_SIZE.to_string(),
                "900000".to_string(),
            ),
            (
                TableProperties::PROPERTY_PARQUET_CDC_NORM_LEVEL.to_string(),
                "1".to_string(),
            ),
        ]);
        let tp = parse(&props).unwrap();
        assert!(tp.cdc_enabled());
        assert_eq!(tp.cdc_min_chunk_size(), 200000);
        assert_eq!(tp.cdc_max_chunk_size(), 900000);
        assert_eq!(tp.cdc_norm_level(), 1);
    }

    #[test]
    fn test_cdc_partial_override() {
        let props = HashMap::from([
            (
                TableProperties::PROPERTY_PARQUET_CDC_ENABLED.to_string(),
                "true".to_string(),
            ),
            (
                TableProperties::PROPERTY_PARQUET_CDC_NORM_LEVEL.to_string(),
                "2".to_string(),
            ),
        ]);
        let tp = parse(&props).unwrap();
        assert!(tp.cdc_enabled());
        assert_eq!(tp.cdc_min_chunk_size(), 256 * 1024);
        assert_eq!(tp.cdc_max_chunk_size(), 1024 * 1024);
        assert_eq!(tp.cdc_norm_level(), 2);
    }

    #[test]
    fn test_cdc_negative_norm_level() {
        let props = HashMap::from([
            (
                TableProperties::PROPERTY_PARQUET_CDC_ENABLED.to_string(),
                "true".to_string(),
            ),
            (
                TableProperties::PROPERTY_PARQUET_CDC_NORM_LEVEL.to_string(),
                "-2".to_string(),
            ),
        ]);
        let tp = parse(&props).unwrap();
        assert_eq!(tp.cdc_norm_level(), -2);
    }

    #[test]
    fn test_cdc_invalid_min_chunk_size() {
        let props = HashMap::from([
            (
                TableProperties::PROPERTY_PARQUET_CDC_ENABLED.to_string(),
                "true".to_string(),
            ),
            (
                TableProperties::PROPERTY_PARQUET_CDC_MIN_CHUNK_SIZE.to_string(),
                "not_a_number".to_string(),
            ),
        ]);
        let err = parse(&props).unwrap_err();
        assert!(
            err.to_string().contains(
                "Invalid value for write.parquet.content-defined-chunking.min-chunk-size"
            )
        );
    }

    #[test]
    fn test_cdc_invalid_norm_level() {
        let props = HashMap::from([
            (
                TableProperties::PROPERTY_PARQUET_CDC_ENABLED.to_string(),
                "true".to_string(),
            ),
            (
                TableProperties::PROPERTY_PARQUET_CDC_NORM_LEVEL.to_string(),
                "not_a_number".to_string(),
            ),
        ]);
        let err = parse(&props).unwrap_err();
        assert!(
            err.to_string()
                .contains("Invalid value for write.parquet.content-defined-chunking.norm-level")
        );
    }

    #[test]
    fn test_cdc_no_properties() {
        let props = HashMap::from([("some.other.property".to_string(), "value".to_string())]);
        let tp = parse(&props).unwrap();
        assert!(!tp.cdc_enabled());
    }
}
