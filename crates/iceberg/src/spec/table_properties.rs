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

use crate::compression::CompressionCodec;
use crate::error::{Error, ErrorKind, Result};

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

/// Parse compression codec for metadata files from table properties.
/// Retrieves the compression codec property, applies defaults, and parses the value.
/// Only "none" (or empty string) and "gzip" are supported for metadata compression.
///
/// # Arguments
///
/// * `properties` - HashMap containing table properties
///
/// # Errors
///
/// Returns an error if the codec is not "none", "", or "gzip" (case-insensitive).
/// Lz4 and Zstd are not supported for metadata file compression.
pub(crate) fn parse_metadata_file_compression(
    properties: &HashMap<String, String>,
) -> Result<CompressionCodec> {
    let value = properties
        .get(TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC)
        .map(|s| s.as_str())
        .unwrap_or(TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC_DEFAULT);

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
    /// Compression codec for metadata files (JSON)
    pub metadata_compression_codec: CompressionCodec,
    /// Compression codec for Avro files (manifests, manifest lists)
    pub avro_compression_codec: String,
    /// Compression level for Avro files (None uses codec-specific defaults: gzip=9, zstd=1)
    pub avro_compression_level: Option<u8>,
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

    /// Compression codec for metadata files (JSON).
    pub const PROPERTY_METADATA_COMPRESSION_CODEC: &str = "write.metadata.compression-codec";
    /// Default metadata compression codec
    pub const PROPERTY_METADATA_COMPRESSION_CODEC_DEFAULT: &str = "none";

    /// Compression codec for Avro files (manifests, manifest lists)
    pub const PROPERTY_AVRO_COMPRESSION_CODEC: &str = "write.avro.compression-codec";
    /// Default Avro compression codec - gzip
    pub const PROPERTY_AVRO_COMPRESSION_CODEC_DEFAULT: &str = "gzip";

    /// Compression level for Avro files
    pub const PROPERTY_AVRO_COMPRESSION_LEVEL: &str = "write.avro.compression-level";
    /// Default Avro compression level (None, uses codec-specific defaults: gzip=9, zstd=1)
    pub const PROPERTY_AVRO_COMPRESSION_LEVEL_DEFAULT: Option<u8> = None;

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
}

impl TryFrom<&HashMap<String, String>> for TableProperties {
    // parse by entry key or use default value
    type Error = Error;

    fn try_from(props: &HashMap<String, String>) -> Result<Self> {
        Ok(TableProperties {
            commit_num_retries: parse_property(
                props,
                TableProperties::PROPERTY_COMMIT_NUM_RETRIES,
                TableProperties::PROPERTY_COMMIT_NUM_RETRIES_DEFAULT,
            )?,
            commit_min_retry_wait_ms: parse_property(
                props,
                TableProperties::PROPERTY_COMMIT_MIN_RETRY_WAIT_MS,
                TableProperties::PROPERTY_COMMIT_MIN_RETRY_WAIT_MS_DEFAULT,
            )?,
            commit_max_retry_wait_ms: parse_property(
                props,
                TableProperties::PROPERTY_COMMIT_MAX_RETRY_WAIT_MS,
                TableProperties::PROPERTY_COMMIT_MAX_RETRY_WAIT_MS_DEFAULT,
            )?,
            commit_total_retry_timeout_ms: parse_property(
                props,
                TableProperties::PROPERTY_COMMIT_TOTAL_RETRY_TIME_MS,
                TableProperties::PROPERTY_COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT,
            )?,
            write_format_default: parse_property(
                props,
                TableProperties::PROPERTY_DEFAULT_FILE_FORMAT,
                TableProperties::PROPERTY_DEFAULT_FILE_FORMAT_DEFAULT.to_string(),
            )?,
            write_target_file_size_bytes: parse_property(
                props,
                TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES,
                TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT,
            )?,
            metadata_compression_codec: parse_metadata_file_compression(props)?,
            avro_compression_codec: parse_property(
                props,
                TableProperties::PROPERTY_AVRO_COMPRESSION_CODEC,
                TableProperties::PROPERTY_AVRO_COMPRESSION_CODEC_DEFAULT.to_string(),
            )?,
<<<<<<< HEAD
            avro_compression_level: props
                .get(TableProperties::PROPERTY_AVRO_COMPRESSION_LEVEL)
                .map(|v| {
                    v.parse::<u8>().map_err(|e| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!(
                                "Invalid value for {}: {e}",
                                TableProperties::PROPERTY_AVRO_COMPRESSION_LEVEL
                            ),
                        )
                    })
                })
                .transpose()?,
            write_datafusion_fanout_enabled: parse_property(
                props,
                TableProperties::PROPERTY_DATAFUSION_WRITE_FANOUT_ENABLED,
                TableProperties::PROPERTY_DATAFUSION_WRITE_FANOUT_ENABLED_DEFAULT,
            )?,
            gc_enabled: parse_property(
                props,
                TableProperties::PROPERTY_GC_ENABLED,
                TableProperties::PROPERTY_GC_ENABLED_DEFAULT,
            )?,
            max_snapshot_age_ms: parse_property(
                props,
                TableProperties::PROPERTY_MAX_SNAPSHOT_AGE_MS,
                TableProperties::PROPERTY_MAX_SNAPSHOT_AGE_MS_DEFAULT,
            )?,
            min_snapshots_to_keep: parse_property(
                props,
                TableProperties::PROPERTY_MIN_SNAPSHOTS_TO_KEEP,
                TableProperties::PROPERTY_MIN_SNAPSHOTS_TO_KEEP_DEFAULT,
            )?,
            max_ref_age_ms: parse_property(
                props,
                TableProperties::PROPERTY_MAX_REF_AGE_MS,
                TableProperties::PROPERTY_MAX_REF_AGE_MS_DEFAULT,
            )?,
            cdc_enabled: parse_property(
                props,
                TableProperties::PROPERTY_PARQUET_CDC_ENABLED,
                TableProperties::PROPERTY_PARQUET_CDC_ENABLED_DEFAULT,
            )?,
            cdc_min_chunk_size: parse_property(
                props,
                TableProperties::PROPERTY_PARQUET_CDC_MIN_CHUNK_SIZE,
                TableProperties::PROPERTY_PARQUET_CDC_MIN_CHUNK_SIZE_DEFAULT,
            )?,
            cdc_max_chunk_size: parse_property(
                props,
                TableProperties::PROPERTY_PARQUET_CDC_MAX_CHUNK_SIZE,
                TableProperties::PROPERTY_PARQUET_CDC_MAX_CHUNK_SIZE_DEFAULT,
            )?,
            cdc_norm_level: parse_property(
                props,
                TableProperties::PROPERTY_PARQUET_CDC_NORM_LEVEL,
                TableProperties::PROPERTY_PARQUET_CDC_NORM_LEVEL_DEFAULT,
            )?,
            encryption_key_id: props
                .get(TableProperties::PROPERTY_ENCRYPTION_KEY_ID)
                .cloned(),
            encryption_data_key_length: parse_property(
                props,
                TableProperties::PROPERTY_ENCRYPTION_DATA_KEY_LENGTH,
                TableProperties::PROPERTY_ENCRYPTION_DATA_KEY_LENGTH_DEFAULT,
            )?,
=======
            avro_compression_level: {
                let level = parse_property(
                    props,
                    TableProperties::PROPERTY_AVRO_COMPRESSION_LEVEL,
                    255u8,
                )?;
                if level == 255 { None } else { Some(level) }
            },
>>>>>>> 5370f775 (remove parse optional property)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compression::CompressionCodec;

    #[test]
    fn test_table_properties_default() {
        let props = HashMap::new();
        let table_properties = TableProperties::try_from(&props).unwrap();
        assert_eq!(
            table_properties.commit_num_retries,
            TableProperties::PROPERTY_COMMIT_NUM_RETRIES_DEFAULT
        );
        assert_eq!(
            table_properties.commit_min_retry_wait_ms,
            TableProperties::PROPERTY_COMMIT_MIN_RETRY_WAIT_MS_DEFAULT
        );
        assert_eq!(
            table_properties.commit_max_retry_wait_ms,
            TableProperties::PROPERTY_COMMIT_MAX_RETRY_WAIT_MS_DEFAULT
        );
        assert_eq!(
            table_properties.write_format_default,
            TableProperties::PROPERTY_DEFAULT_FILE_FORMAT_DEFAULT.to_string()
        );
        assert_eq!(
            table_properties.write_target_file_size_bytes,
            TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT
        );
        // Test compression defaults (none means CompressionCodec::None)
        assert_eq!(
            table_properties.metadata_compression_codec,
            CompressionCodec::None
        );
        assert_eq!(
            table_properties.avro_compression_codec,
            TableProperties::PROPERTY_AVRO_COMPRESSION_CODEC_DEFAULT.to_string()
        );
        assert_eq!(
            table_properties.avro_compression_level,
            TableProperties::PROPERTY_AVRO_COMPRESSION_LEVEL_DEFAULT
        );
        assert_eq!(
            table_properties.gc_enabled,
            TableProperties::PROPERTY_GC_ENABLED_DEFAULT
        );
        assert_eq!(
            table_properties.max_snapshot_age_ms,
            TableProperties::PROPERTY_MAX_SNAPSHOT_AGE_MS_DEFAULT
        );
        assert_eq!(
            table_properties.min_snapshots_to_keep,
            TableProperties::PROPERTY_MIN_SNAPSHOTS_TO_KEEP_DEFAULT
        );
        assert_eq!(
            table_properties.max_ref_age_ms,
            TableProperties::PROPERTY_MAX_REF_AGE_MS_DEFAULT
        );
    }

    #[test]
    fn test_table_properties_history_expire_overrides() {
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
        let table_properties = TableProperties::try_from(&props).unwrap();
        assert_eq!(table_properties.max_snapshot_age_ms, 1234);
        assert_eq!(table_properties.min_snapshots_to_keep, 7);
        assert_eq!(table_properties.max_ref_age_ms, 5678);
    }

    #[test]
    fn test_table_properties_compression() {
        let props = HashMap::from([
            (
                TableProperties::PROPERTY_AVRO_COMPRESSION_CODEC.to_string(),
                "zstd".to_string(),
            ),
            (
                TableProperties::PROPERTY_AVRO_COMPRESSION_LEVEL.to_string(),
                "3".to_string(),
            ),
        ]);
        let table_properties = TableProperties::try_from(&props).unwrap();
        assert_eq!(
            table_properties.metadata_compression_codec,
            CompressionCodec::gzip_default()
        );
        assert_eq!(table_properties.avro_compression_codec, "zstd");
        assert_eq!(table_properties.avro_compression_level, Some(3));
    }

    #[test]
    fn test_table_properties_compression_none() {
        let props = HashMap::from([(
            TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC.to_string(),
            "none".to_string(),
        )]);
        let table_properties = TableProperties::try_from(&props).unwrap();
        assert_eq!(
            table_properties.metadata_compression_codec,
            CompressionCodec::None
        );
    }

    #[test]
    fn test_table_properties_compression_case_insensitive() {
        // Test uppercase
        let props_upper = HashMap::from([(
            TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC.to_string(),
            "GZIP".to_string(),
        )]);
        let table_properties = TableProperties::try_from(&props_upper).unwrap();
        assert_eq!(
            table_properties.metadata_compression_codec,
            CompressionCodec::gzip_default()
        );

        // Test mixed case
        let props_mixed = HashMap::from([(
            TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC.to_string(),
            "GzIp".to_string(),
        )]);
        let table_properties = TableProperties::try_from(&props_mixed).unwrap();
        assert_eq!(
            table_properties.metadata_compression_codec,
            CompressionCodec::gzip_default()
        );

        // Test "NONE" should also be case-insensitive
        let props_none_upper = HashMap::from([(
            TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC.to_string(),
            "NONE".to_string(),
        )]);
        let table_properties = TableProperties::try_from(&props_none_upper).unwrap();
        assert_eq!(
            table_properties.metadata_compression_codec,
            CompressionCodec::None
        );
    }

    #[test]
    fn test_table_properties_valid() {
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
        let table_properties = TableProperties::try_from(&props).unwrap();
        assert_eq!(table_properties.commit_num_retries, 10);
        assert_eq!(table_properties.commit_max_retry_wait_ms, 20);
        assert_eq!(table_properties.write_format_default, "avro".to_string());
        assert_eq!(table_properties.write_target_file_size_bytes, 512);
        assert!(!table_properties.gc_enabled);
    }

    #[test]
    fn test_table_properties_invalid() {
        let invalid_retries = HashMap::from([(
            TableProperties::PROPERTY_COMMIT_NUM_RETRIES.to_string(),
            "abc".to_string(),
        )]);

        let table_properties = TableProperties::try_from(&invalid_retries).unwrap_err();
        assert!(
            table_properties.to_string().contains(
                "Invalid value for commit.retry.num-retries: invalid digit found in string"
            )
        );

        let invalid_min_wait = HashMap::from([(
            TableProperties::PROPERTY_COMMIT_MIN_RETRY_WAIT_MS.to_string(),
            "abc".to_string(),
        )]);
        let table_properties = TableProperties::try_from(&invalid_min_wait).unwrap_err();
        assert!(
            table_properties.to_string().contains(
                "Invalid value for commit.retry.min-wait-ms: invalid digit found in string"
            )
        );

        let invalid_max_wait = HashMap::from([(
            TableProperties::PROPERTY_COMMIT_MAX_RETRY_WAIT_MS.to_string(),
            "abc".to_string(),
        )]);
        let table_properties = TableProperties::try_from(&invalid_max_wait).unwrap_err();
        assert!(
            table_properties.to_string().contains(
                "Invalid value for commit.retry.max-wait-ms: invalid digit found in string"
            )
        );

        let invalid_target_size = HashMap::from([(
            TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES.to_string(),
            "abc".to_string(),
        )]);
        let table_properties = TableProperties::try_from(&invalid_target_size).unwrap_err();
        assert!(table_properties.to_string().contains(
            "Invalid value for write.target-file-size-bytes: invalid digit found in string"
        ));

        let invalid_gc_enabled = HashMap::from([(
            TableProperties::PROPERTY_GC_ENABLED.to_string(),
            "notabool".to_string(),
        )]);
        let table_properties = TableProperties::try_from(&invalid_gc_enabled).unwrap_err();
        assert!(
            table_properties
                .to_string()
                .contains("Invalid value for gc.enabled")
        );
    }

    #[test]
<<<<<<< HEAD
    fn test_table_properties_compression_invalid_rejected() {
        let invalid_codecs = ["lz4", "zstd", "snappy"];

        for codec in invalid_codecs {
            let props = HashMap::from([(
                TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC.to_string(),
                codec.to_string(),
            )]);
            let err = TableProperties::try_from(&props).unwrap_err();
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
            parse_metadata_file_compression(&props).unwrap(),
            CompressionCodec::None
        );

        // Test with empty string
        let props = HashMap::from([(
            TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC.to_string(),
            "".to_string(),
        )]);
        assert_eq!(
            parse_metadata_file_compression(&props).unwrap(),
            CompressionCodec::None
        );

        // Test with "gzip"
        let props = HashMap::from([(
            TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC.to_string(),
            "gzip".to_string(),
        )]);
        assert_eq!(
            parse_metadata_file_compression(&props).unwrap(),
            CompressionCodec::gzip_default()
        );

        // Test case insensitivity - "NONE"
        let props = HashMap::from([(
            TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC.to_string(),
            "NONE".to_string(),
        )]);
        assert_eq!(
            parse_metadata_file_compression(&props).unwrap(),
            CompressionCodec::None
        );

        // Test case insensitivity - "GZIP"
        let props = HashMap::from([(
            TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC.to_string(),
            "GZIP".to_string(),
        )]);
        assert_eq!(
            parse_metadata_file_compression(&props).unwrap(),
            CompressionCodec::gzip_default()
        );

        // Test case insensitivity - "GzIp"
        let props = HashMap::from([(
            TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC.to_string(),
            "GzIp".to_string(),
        )]);
        assert_eq!(
            parse_metadata_file_compression(&props).unwrap(),
            CompressionCodec::gzip_default()
        );

        // Test default when property is missing
        let props = HashMap::new();
        assert_eq!(
            parse_metadata_file_compression(&props).unwrap(),
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
            let err = parse_metadata_file_compression(&props).unwrap_err();
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
        let tp = TableProperties::try_from(&props).unwrap();
        assert!(!tp.cdc_enabled);
    }

    #[test]
    fn test_cdc_enabled_via_flag() {
        let props = HashMap::from([(
            TableProperties::PROPERTY_PARQUET_CDC_ENABLED.to_string(),
            "true".to_string(),
        )]);
        let tp = TableProperties::try_from(&props).unwrap();
        assert!(tp.cdc_enabled);
        assert_eq!(tp.cdc_min_chunk_size, 256 * 1024);
        assert_eq!(tp.cdc_max_chunk_size, 1024 * 1024);
        assert_eq!(tp.cdc_norm_level, 0);
    }

    #[test]
    fn test_cdc_size_props_alone_do_not_enable() {
        let props = HashMap::from([(
            TableProperties::PROPERTY_PARQUET_CDC_MIN_CHUNK_SIZE.to_string(),
            "262144".to_string(),
        )]);
        let tp = TableProperties::try_from(&props).unwrap();
        assert!(!tp.cdc_enabled);
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
        let tp = TableProperties::try_from(&props).unwrap();
        assert!(tp.cdc_enabled);
        assert_eq!(tp.cdc_min_chunk_size, 200000);
        assert_eq!(tp.cdc_max_chunk_size, 900000);
        assert_eq!(tp.cdc_norm_level, 1);
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
        let tp = TableProperties::try_from(&props).unwrap();
        assert!(tp.cdc_enabled);
        assert_eq!(tp.cdc_min_chunk_size, 256 * 1024);
        assert_eq!(tp.cdc_max_chunk_size, 1024 * 1024);
        assert_eq!(tp.cdc_norm_level, 2);
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
        let tp = TableProperties::try_from(&props).unwrap();
        assert_eq!(tp.cdc_norm_level, -2);
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
        let err = TableProperties::try_from(&props).unwrap_err();
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
        let err = TableProperties::try_from(&props).unwrap_err();
        assert!(
            err.to_string()
                .contains("Invalid value for write.parquet.content-defined-chunking.norm-level")
        );
    }

    #[test]
    fn test_cdc_no_properties() {
        let props = HashMap::from([("some.other.property".to_string(), "value".to_string())]);
        let tp = TableProperties::try_from(&props).unwrap();
        assert!(!tp.cdc_enabled);
    }

    #[test]
=======
>>>>>>> 5370f775 (remove parse optional property)
    fn test_table_properties_optional_compression_level() {
        // Test that compression level is None when not specified
        let props = HashMap::new();
        let table_properties = TableProperties::try_from(&props).unwrap();
        assert_eq!(table_properties.avro_compression_level, None);

        // Test that compression level is Some(value) when specified
        let props = HashMap::from([(
            TableProperties::PROPERTY_AVRO_COMPRESSION_LEVEL.to_string(),
            "5".to_string(),
        )]);
        let table_properties = TableProperties::try_from(&props).unwrap();
        assert_eq!(table_properties.avro_compression_level, Some(5));

        // Test that invalid compression level returns error
        let props = HashMap::from([(
            TableProperties::PROPERTY_AVRO_COMPRESSION_LEVEL.to_string(),
            "invalid".to_string(),
        )]);
        let result = TableProperties::try_from(&props);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid value for write.avro.compression-level")
        );
    }
}
