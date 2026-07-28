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

use iceberg_property_macro::properties_view;

use crate::compression::{CompressionCodec, TABLE_METADATA_SUPPORTED_COMPRESSION};
use crate::encryption::AesKeySize;
use crate::error::{Error, ErrorKind, Result};
use crate::spec::NameMapping;
use crate::util::location::strip_trailing_slash;

fn supported_metadata_compression_names() -> String {
    let names = TABLE_METADATA_SUPPORTED_COMPRESSION
        .iter()
        .map(|codec| format!("'{}'", codec.name()))
        .collect::<Vec<_>>();
    let (last, rest) = names
        .split_last()
        .expect("metadata compression codec list must not be empty");

    if rest.is_empty() {
        last.clone()
    } else {
        format!("{}, and {last}", rest.join(", "))
    }
}

fn invalid_metadata_compression_codec(value: &str) -> Error {
    Error::new(
        ErrorKind::DataInvalid,
        format!(
            "Invalid metadata compression codec: {value}. Only {} are supported for metadata files.",
            supported_metadata_compression_names()
        ),
    )
}

fn parse_location_property(path: &str) -> Result<String> {
    if path.is_empty() {
        return Err(Error::new(ErrorKind::DataInvalid, "path must not be empty"));
    }

    Ok(strip_trailing_slash(path).to_string())
}

fn parse_metadata_compression(value: &str) -> Result<CompressionCodec> {
    // Handle empty string as None
    if value.is_empty() {
        return Ok(CompressionCodec::None);
    }

    // Lowercase the value for case-insensitive parsing
    let lowercase_value = value.to_lowercase();

    // Use serde to parse the codec (which has rename_all = "lowercase")
    let codec: CompressionCodec =
        serde_json::from_value(serde_json::Value::String(lowercase_value))
            .map_err(|_| invalid_metadata_compression_codec(value))?;

    if TABLE_METADATA_SUPPORTED_COMPRESSION.contains(&codec) {
        Ok(codec)
    } else {
        Err(invalid_metadata_compression_codec(value))
    }
}

/// Parse the Parquet data-file compression codec (`write.parquet.compression-codec`)
/// and fold in the compression level (`write.parquet.compression-level`) for the
/// codecs that accept one (`zstd`, `gzip`, `brotli`).
fn parse_parquet_compression(
    properties: &HashMap<String, String>,
    codec_key: &str,
    additional_keys: &[&str],
    default: CompressionCodec,
) -> Result<CompressionCodec> {
    let level_key = additional_keys[0];
    let codec = properties
        .get(codec_key)
        .map(|value| {
            serde_json::from_value(serde_json::Value::String(value.to_lowercase())).map_err(|_| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Invalid Parquet compression codec: {value}. Supported codecs: \
                         uncompressed, snappy, gzip, lzo, brotli, lz4, lz4_raw, zstd"
                    ),
                )
            })
        })
        .transpose()?
        .unwrap_or(default);

    let level = properties
        .get(level_key)
        .map(|value| {
            value.parse::<u8>().map_err(|error| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Invalid value for {level_key}: {error}"),
                )
            })
        })
        .transpose()?;

    Ok(match (codec, level) {
        (CompressionCodec::Zstd(_), Some(level)) => CompressionCodec::Zstd(level),
        (CompressionCodec::Gzip(_), Some(level)) => CompressionCodec::Gzip(level),
        (CompressionCodec::Brotli(_), Some(level)) => CompressionCodec::Brotli(level),
        (codec, _) => codec,
    })
}

properties_view! {
/// Typed view over the properties of a table.
#[derive(Debug)]
pub struct TableProperties {
    /// The number of times to retry a commit.
    #[property(
        key = Self::PROPERTY_COMMIT_NUM_RETRIES,
        default = Self::PROPERTY_COMMIT_NUM_RETRIES_DEFAULT,
        getter
    )]
    commit_num_retries: usize,
    /// The minimum wait time between retries.
    #[property(
        key = Self::PROPERTY_COMMIT_MIN_RETRY_WAIT_MS,
        default = Self::PROPERTY_COMMIT_MIN_RETRY_WAIT_MS_DEFAULT,
        getter
    )]
    commit_min_retry_wait_ms: u64,
    /// The maximum wait time between retries.
    #[property(
        key = Self::PROPERTY_COMMIT_MAX_RETRY_WAIT_MS,
        default = Self::PROPERTY_COMMIT_MAX_RETRY_WAIT_MS_DEFAULT,
        getter
    )]
    commit_max_retry_wait_ms: u64,
    /// The total timeout for commit retries.
    #[property(
        key = Self::PROPERTY_COMMIT_TOTAL_RETRY_TIME_MS,
        default = Self::PROPERTY_COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT,
        getter
    )]
    commit_total_retry_timeout_ms: u64,
    /// The default format for files.
    #[property(
        key = Self::PROPERTY_DEFAULT_FILE_FORMAT,
        default = Self::PROPERTY_DEFAULT_FILE_FORMAT_DEFAULT,
        getter
    )]
    write_format_default: String,
    /// The target file size for files.
    #[property(
        key = Self::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES,
        default = Self::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT,
        getter
    )]
    write_target_file_size_bytes: usize,
    /// Base directory for metadata files (manifests, manifest lists), with any
    /// trailing slash trimmed. `None` if `write.metadata.path` is not set.
    #[property(
        key = Self::PROPERTY_WRITE_METADATA_PATH,
        default = None,
        parse_with = parse_location_property,
        getter
    )]
    write_metadata_path: Option<String>,
    /// Compression codec for metadata files (JSON)
    #[property(
        key = Self::PROPERTY_METADATA_COMPRESSION_CODEC,
        default = CompressionCodec::None,
        parse_with = parse_metadata_compression,
        getter
    )]
    metadata_compression_codec: CompressionCodec,
    /// Whether to use `FanoutWriter` for partitioned tables.
    #[property(
        key = Self::PROPERTY_DATAFUSION_WRITE_FANOUT_ENABLED,
        default = Self::PROPERTY_DATAFUSION_WRITE_FANOUT_ENABLED_DEFAULT,
        getter
    )]
    write_datafusion_fanout_enabled: bool,
    /// Whether garbage collection is enabled on drop.
    /// When `false`, data files will not be deleted when a table is dropped.
    #[property(
        key = Self::PROPERTY_GC_ENABLED,
        default = Self::PROPERTY_GC_ENABLED_DEFAULT,
        getter
    )]
    gc_enabled: bool,
    /// Default maximum age of a snapshot to keep when expiring snapshots.
    #[property(
        key = Self::PROPERTY_MAX_SNAPSHOT_AGE_MS,
        default = Self::PROPERTY_MAX_SNAPSHOT_AGE_MS_DEFAULT,
        getter
    )]
    max_snapshot_age_ms: i64,
    /// Default minimum number of snapshots to keep per branch when expiring snapshots.
    #[property(
        key = Self::PROPERTY_MIN_SNAPSHOTS_TO_KEEP,
        default = Self::PROPERTY_MIN_SNAPSHOTS_TO_KEEP_DEFAULT,
        getter
    )]
    min_snapshots_to_keep: usize,
    /// Default maximum age of a snapshot reference to keep when expiring snapshots.
    #[property(
        key = Self::PROPERTY_MAX_REF_AGE_MS,
        default = Self::PROPERTY_MAX_REF_AGE_MS_DEFAULT,
        getter
    )]
    max_ref_age_ms: i64,
    /// Whether content-defined chunking is enabled.
    /// `true` only when `write.parquet.content-defined-chunking.enabled = "true"`.
    #[property(
        key = Self::PROPERTY_PARQUET_CDC_ENABLED,
        default = Self::PROPERTY_PARQUET_CDC_ENABLED_DEFAULT,
        getter
    )]
    cdc_enabled: bool,
    /// Content-defined chunking minimum chunk size in bytes.
    #[property(
        key = Self::PROPERTY_PARQUET_CDC_MIN_CHUNK_SIZE,
        default = Self::PROPERTY_PARQUET_CDC_MIN_CHUNK_SIZE_DEFAULT,
        getter
    )]
    cdc_min_chunk_size: usize,
    /// Content-defined chunking maximum chunk size in bytes.
    #[property(
        key = Self::PROPERTY_PARQUET_CDC_MAX_CHUNK_SIZE,
        default = Self::PROPERTY_PARQUET_CDC_MAX_CHUNK_SIZE_DEFAULT,
        getter
    )]
    cdc_max_chunk_size: usize,
    /// Content-defined chunking normalization level (gearhash bit adjustment).
    #[property(
        key = Self::PROPERTY_PARQUET_CDC_NORM_LEVEL,
        default = Self::PROPERTY_PARQUET_CDC_NORM_LEVEL_DEFAULT,
        getter
    )]
    cdc_norm_level: i32,
    /// Parquet compression codec for data files, with the resolved compression
    /// level folded in (from `write.parquet.compression-level`, or the codec's
    /// default when unset).
    #[property(
        key = Self::PROPERTY_PARQUET_COMPRESSION_CODEC,
        additional_keys = [Self::PROPERTY_PARQUET_COMPRESSION_LEVEL],
        default = CompressionCodec::zstd_default(),
        parse_properties_with = parse_parquet_compression,
        getter
    )]
    parquet_compression_codec: CompressionCodec,
    /// Approximate maximum Parquet row group size in bytes.
    #[property(
        key = Self::PROPERTY_PARQUET_ROW_GROUP_SIZE_BYTES,
        default = Self::PROPERTY_PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT,
        getter
    )]
    parquet_row_group_size_bytes: usize,
    /// Approximate maximum Parquet data page size in bytes.
    #[property(
        key = Self::PROPERTY_PARQUET_PAGE_SIZE_BYTES,
        default = Self::PROPERTY_PARQUET_PAGE_SIZE_BYTES_DEFAULT,
        getter
    )]
    parquet_page_size_bytes: usize,
    /// Maximum number of rows per Parquet data page.
    #[property(
        key = Self::PROPERTY_PARQUET_PAGE_ROW_LIMIT,
        default = Self::PROPERTY_PARQUET_PAGE_ROW_LIMIT_DEFAULT,
        getter
    )]
    parquet_page_row_limit: usize,
    /// Approximate maximum Parquet dictionary page size in bytes.
    #[property(
        key = Self::PROPERTY_PARQUET_DICT_SIZE_BYTES,
        default = Self::PROPERTY_PARQUET_DICT_SIZE_BYTES_DEFAULT,
        getter
    )]
    parquet_dict_size_bytes: usize,
    /// The master key id used to encrypt this table's manifest list and data
    /// files. `None` if `encryption.key-id` is not set.
    #[property(
        key = Self::PROPERTY_ENCRYPTION_KEY_ID,
        default = None,
        getter
    )]
    encryption_key_id: Option<String>,
    /// The encryption data encryption key length in bytes.
    #[property(
        key = Self::PROPERTY_ENCRYPTION_DATA_KEY_LENGTH,
        default = Self::PROPERTY_ENCRYPTION_DATA_KEY_LENGTH_DEFAULT,
        getter
    )]
    encryption_data_key_length: usize,
    /// Base directory for data files, with any trailing slash trimmed.
    #[property(
        key = Self::PROPERTY_WRITE_DATA_LOCATION,
        default = None,
        parse_with = parse_location_property,
        getter
    )]
    write_data_location: Option<String>,
    /// Deprecated table property for data file write location, with any trailing slash trimmed.
    ///
    /// Property will be removed at a later date.
    /// Superseded by [`TableProperties::write_data_location`].
    #[property(
        key = Self::PROPERTY_WRITE_FOLDER_STORAGE_LOCATION,
        default = None,
        parse_with = parse_location_property,
        getter
    )]
    write_folder_storage_location: Option<String>,
    /// Deprecated table property for data file write location for object storage location generator,
    /// with any trailing slash trimmed.
    ///
    /// Property will be removed at a later date.
    /// Superseded by [`TableProperties::write_data_location`].
    #[property(
        key = Self::PROPERTY_WRITE_OBJECT_STORAGE_LOCATION,
        default = None,
        parse_with = parse_location_property,
        getter
    )]
    write_object_storage_location: Option<String>,
    /// Whether partition values are included in object storage paths.
    #[property(
        key = Self::PROPERTY_WRITE_OBJECT_STORAGE_PARTITIONED_PATHS,
        default = Self::PROPERTY_WRITE_OBJECT_STORAGE_PARTITIONED_PATHS_DEFAULT,
        getter
    )]
    write_object_storage_partitioned_paths: bool,
    /// The table's default name mapping, used to assign field ids when reading data files
    /// that carry no field id metadata. `None` if `schema.name-mapping.default` is not set.
    #[property(
        key = Self::PROPERTY_DEFAULT_NAME_MAPPING,
        default = None,
        getter
    )]
    default_name_mapping: Option<NameMapping>,
}
}

impl TableProperties<'_> {
    /// Reserved table property for table format version.
    ///
    /// Iceberg will default a new table's format version to the latest stable and recommended
    /// version. This reserved property keyword allows users to override the Iceberg format version of
    /// the table metadata.
    ///
    /// If this table property exists when creating a table, the table will use the specified format
    /// version. If a table updates this property, it will try to upgrade to the specified format
    /// version.
    pub const PROPERTY_FORMAT_VERSION: &'static str = "format-version";
    /// Reserved table property for table UUID.
    pub const PROPERTY_UUID: &'static str = "uuid";
    /// Reserved table property for the total number of snapshots.
    pub const PROPERTY_SNAPSHOT_COUNT: &'static str = "snapshot-count";
    /// Reserved table property for current snapshot summary.
    pub const PROPERTY_CURRENT_SNAPSHOT_SUMMARY: &'static str = "current-snapshot-summary";
    /// Reserved table property for current snapshot id.
    pub const PROPERTY_CURRENT_SNAPSHOT_ID: &'static str = "current-snapshot-id";
    /// Reserved table property for current snapshot timestamp.
    pub const PROPERTY_CURRENT_SNAPSHOT_TIMESTAMP: &'static str = "current-snapshot-timestamp-ms";
    /// Reserved table property for the JSON representation of current schema.
    pub const PROPERTY_CURRENT_SCHEMA: &'static str = "current-schema";
    /// Reserved table property for the JSON representation of current(default) partition spec.
    pub const PROPERTY_DEFAULT_PARTITION_SPEC: &'static str = "default-partition-spec";
    /// Reserved table property for the JSON representation of current(default) sort order.
    pub const PROPERTY_DEFAULT_SORT_ORDER: &'static str = "default-sort-order";

    /// Property key for max number of previous versions to keep.
    pub const PROPERTY_METADATA_PREVIOUS_VERSIONS_MAX: &'static str =
        "write.metadata.previous-versions-max";
    /// Default value for max number of previous versions to keep.
    pub const PROPERTY_METADATA_PREVIOUS_VERSIONS_MAX_DEFAULT: usize = 100;

    /// Property key for max number of partitions to keep summary stats for.
    pub const PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT: &'static str =
        "write.summary.partition-limit";
    /// Default value for the max number of partitions to keep summary stats for.
    pub const PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT: u64 = 0;

    /// Reserved Iceberg table properties list.
    ///
    /// Reserved table properties are only used to control behaviors when creating or updating a
    /// table. The value of these properties are not persisted as a part of the table metadata.
    pub const RESERVED_PROPERTIES: [&'static str; 9] = [
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
    pub const PROPERTY_COMMIT_NUM_RETRIES: &'static str = "commit.retry.num-retries";
    /// Default value for number of commit retries.
    pub const PROPERTY_COMMIT_NUM_RETRIES_DEFAULT: usize = 4;

    /// Property key for minimum wait time (ms) between retries.
    pub const PROPERTY_COMMIT_MIN_RETRY_WAIT_MS: &'static str = "commit.retry.min-wait-ms";
    /// Default value for minimum wait time (ms) between retries.
    pub const PROPERTY_COMMIT_MIN_RETRY_WAIT_MS_DEFAULT: u64 = 100;

    /// Property key for maximum wait time (ms) between retries.
    pub const PROPERTY_COMMIT_MAX_RETRY_WAIT_MS: &'static str = "commit.retry.max-wait-ms";
    /// Default value for maximum wait time (ms) between retries.
    pub const PROPERTY_COMMIT_MAX_RETRY_WAIT_MS_DEFAULT: u64 = 60 * 1000; // 1 minute

    /// Property key for total maximum retry time (ms).
    pub const PROPERTY_COMMIT_TOTAL_RETRY_TIME_MS: &'static str = "commit.retry.total-timeout-ms";
    /// Default value for total maximum retry time (ms).
    pub const PROPERTY_COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT: u64 = 30 * 60 * 1000; // 30 minutes

    /// Default file format for data files
    pub const PROPERTY_DEFAULT_FILE_FORMAT: &'static str = "write.format.default";
    /// Default file format for delete files
    pub const PROPERTY_DELETE_DEFAULT_FILE_FORMAT: &'static str = "write.delete.format.default";
    /// Default value for data file format
    pub const PROPERTY_DEFAULT_FILE_FORMAT_DEFAULT: &'static str = "parquet";

    /// Target file size for newly written files.
    pub const PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES: &'static str = "write.target-file-size-bytes";
    /// Default target file size
    pub const PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT: usize = 512 * 1024 * 1024; // 512 MB

    /// Base location for metadata files (manifests, manifest lists, table metadata).
    /// When unset, metadata files default to the `metadata` directory under the table
    /// location.
    pub const PROPERTY_WRITE_METADATA_PATH: &'static str = "write.metadata.path";

    /// Property key for the table's default name mapping, stored as a JSON
    /// [`NameMapping`] document.
    pub const PROPERTY_DEFAULT_NAME_MAPPING: &'static str = "schema.name-mapping.default";

    /// Compression codec for metadata files (JSON)
    pub const PROPERTY_METADATA_COMPRESSION_CODEC: &'static str =
        "write.metadata.compression-codec";
    /// Default metadata compression codec - uncompressed
    pub const PROPERTY_METADATA_COMPRESSION_CODEC_DEFAULT: &'static str = "none";
    /// Whether to use `FanoutWriter` for partitioned tables (handles unsorted data).
    /// If false, uses `ClusteredWriter` (requires sorted data, more memory efficient).
    pub const PROPERTY_DATAFUSION_WRITE_FANOUT_ENABLED: &'static str =
        "write.datafusion.fanout.enabled";
    /// Default value for fanout writer enabled
    pub const PROPERTY_DATAFUSION_WRITE_FANOUT_ENABLED_DEFAULT: bool = true;

    /// Property key for enabling garbage collection on drop.
    /// When set to `false`, data files will not be deleted when a table is dropped.
    /// Defaults to `true`.
    pub const PROPERTY_GC_ENABLED: &'static str = "gc.enabled";
    /// Default value for gc.enabled
    pub const PROPERTY_GC_ENABLED_DEFAULT: bool = true;

    /// Property key for the default maximum age of a snapshot to keep when expiring snapshots.
    pub const PROPERTY_MAX_SNAPSHOT_AGE_MS: &'static str = "history.expire.max-snapshot-age-ms";
    /// Default value for history.expire.max-snapshot-age-ms (5 days).
    pub const PROPERTY_MAX_SNAPSHOT_AGE_MS_DEFAULT: i64 = 5 * 24 * 60 * 60 * 1000;
    /// Property key for the default minimum number of snapshots to keep when expiring snapshots.
    pub const PROPERTY_MIN_SNAPSHOTS_TO_KEEP: &'static str = "history.expire.min-snapshots-to-keep";
    /// Default value for history.expire.min-snapshots-to-keep.
    pub const PROPERTY_MIN_SNAPSHOTS_TO_KEEP_DEFAULT: usize = 1;
    /// Property key for the default maximum age of a snapshot reference to keep when expiring.
    pub const PROPERTY_MAX_REF_AGE_MS: &'static str = "history.expire.max-ref-age-ms";
    /// Default value for history.expire.max-ref-age-ms (effectively never expire refs).
    pub const PROPERTY_MAX_REF_AGE_MS_DEFAULT: i64 = i64::MAX;

    /// Enable content-defined chunking with parquet defaults (or per-property overrides).
    pub const PROPERTY_PARQUET_CDC_ENABLED: &'static str =
        "write.parquet.content-defined-chunking.enabled";
    /// Default value for content-defined chunking enabled.
    pub const PROPERTY_PARQUET_CDC_ENABLED_DEFAULT: bool = false;
    /// Minimum chunk size in bytes for content-defined chunking.
    pub const PROPERTY_PARQUET_CDC_MIN_CHUNK_SIZE: &'static str =
        "write.parquet.content-defined-chunking.min-chunk-size";
    /// Default matches `parquet::file::properties::DEFAULT_CDC_MIN_CHUNK_SIZE`.
    pub const PROPERTY_PARQUET_CDC_MIN_CHUNK_SIZE_DEFAULT: usize = 256 * 1024;
    /// Maximum chunk size in bytes for content-defined chunking.
    pub const PROPERTY_PARQUET_CDC_MAX_CHUNK_SIZE: &'static str =
        "write.parquet.content-defined-chunking.max-chunk-size";
    /// Default matches `parquet::file::properties::DEFAULT_CDC_MAX_CHUNK_SIZE`.
    pub const PROPERTY_PARQUET_CDC_MAX_CHUNK_SIZE_DEFAULT: usize = 1024 * 1024;
    /// Normalization level (gearhash bit adjustment) for content-defined chunking.
    pub const PROPERTY_PARQUET_CDC_NORM_LEVEL: &'static str =
        "write.parquet.content-defined-chunking.norm-level";
    /// Default matches `parquet::file::properties::DEFAULT_CDC_NORM_LEVEL`.
    pub const PROPERTY_PARQUET_CDC_NORM_LEVEL_DEFAULT: i32 = 0;

    /// Compression codec for Parquet data files (e.g. `zstd`, `gzip`, `snappy`,
    /// `lz4`, `lz4_raw`, `brotli`, `lzo`, `uncompressed`). The codec name is
    /// parsed into a [`CompressionCodec`] when properties are parsed; the level's
    /// range is validated when the writer is built.
    pub const PROPERTY_PARQUET_COMPRESSION_CODEC: &'static str = "write.parquet.compression-codec";
    /// Default Parquet compression codec.
    pub const PROPERTY_PARQUET_COMPRESSION_CODEC_DEFAULT: &'static str = "zstd";
    /// Compression level for Parquet data files, for codecs that take one
    /// (`gzip`, `zstd`, `brotli`). When unset, the codec's default level is used.
    pub const PROPERTY_PARQUET_COMPRESSION_LEVEL: &'static str = "write.parquet.compression-level";

    /// Approximate maximum size of a Parquet row group in bytes.
    pub const PROPERTY_PARQUET_ROW_GROUP_SIZE_BYTES: &'static str =
        "write.parquet.row-group-size-bytes";
    /// Default Parquet row group size in bytes.
    pub const PROPERTY_PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT: usize = 128 * 1024 * 1024;

    /// Approximate maximum size of a Parquet data page in bytes.
    pub const PROPERTY_PARQUET_PAGE_SIZE_BYTES: &'static str = "write.parquet.page-size-bytes";
    /// Default Parquet page size in bytes.
    pub const PROPERTY_PARQUET_PAGE_SIZE_BYTES_DEFAULT: usize = 1024 * 1024;

    /// Maximum number of rows per Parquet data page.
    pub const PROPERTY_PARQUET_PAGE_ROW_LIMIT: &'static str = "write.parquet.page-row-limit";
    /// Default Parquet page row limit.
    pub const PROPERTY_PARQUET_PAGE_ROW_LIMIT_DEFAULT: usize = 20000;

    /// Approximate maximum size of the Parquet dictionary page in bytes.
    pub const PROPERTY_PARQUET_DICT_SIZE_BYTES: &'static str = "write.parquet.dict-size-bytes";
    /// Default Parquet dictionary page size in bytes.
    pub const PROPERTY_PARQUET_DICT_SIZE_BYTES_DEFAULT: usize = 2 * 1024 * 1024;

    /// Property key for the master key id used to encrypt the table's manifest
    /// list and data files as defined in <https://iceberg.apache.org/docs/nightly/encryption/>.
    pub const PROPERTY_ENCRYPTION_KEY_ID: &'static str = "encryption.key-id";

    /// Property key for the encryption data encryption key (DEK) length in bytes.
    pub const PROPERTY_ENCRYPTION_DATA_KEY_LENGTH: &'static str = "encryption.data-key-length";
    /// Default value for the encryption DEK length (16 bytes = AES-128).
    pub const PROPERTY_ENCRYPTION_DATA_KEY_LENGTH_DEFAULT: usize = 16;
    /// Property key for the base directory for data files
    pub const PROPERTY_WRITE_DATA_LOCATION: &'static str = "write.data.path";
    /// Property key for deprecated [`TableProperties::write_folder_storage_location`]
    pub const PROPERTY_WRITE_FOLDER_STORAGE_LOCATION: &'static str = "write.folder-storage.path";
    /// Property key for deprecated object storage path, kept as a fallback for compatibility.
    pub const PROPERTY_WRITE_OBJECT_STORAGE_LOCATION: &'static str = "write.object-storage.path";
    /// Property key for controlling whether partition values are included in object storage paths.
    pub const PROPERTY_WRITE_OBJECT_STORAGE_PARTITIONED_PATHS: &'static str =
        "write.object-storage.partitioned-paths";
    /// Default value for [`TableProperties::PROPERTY_WRITE_OBJECT_STORAGE_PARTITIONED_PATHS`]
    pub const PROPERTY_WRITE_OBJECT_STORAGE_PARTITIONED_PATHS_DEFAULT: bool = true;

    /// The AES key size to use when generating data encryption keys, derived
    /// from `encryption.data-key-length`.
    pub fn data_encryption_key_size(&self) -> Result<AesKeySize> {
        AesKeySize::from_key_length(self.encryption_data_key_length()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compression::CompressionCodec;

    #[test]
    fn test_table_properties_default() {
        let props = HashMap::new();
        let table_properties = TableProperties::new(&props);
        assert_eq!(
            table_properties.commit_num_retries().unwrap(),
            TableProperties::PROPERTY_COMMIT_NUM_RETRIES_DEFAULT
        );
        assert_eq!(
            table_properties.commit_min_retry_wait_ms().unwrap(),
            TableProperties::PROPERTY_COMMIT_MIN_RETRY_WAIT_MS_DEFAULT
        );
        assert_eq!(
            table_properties.commit_max_retry_wait_ms().unwrap(),
            TableProperties::PROPERTY_COMMIT_MAX_RETRY_WAIT_MS_DEFAULT
        );
        assert_eq!(
            table_properties.write_format_default().unwrap(),
            TableProperties::PROPERTY_DEFAULT_FILE_FORMAT_DEFAULT.to_string()
        );
        assert_eq!(
            table_properties.write_target_file_size_bytes().unwrap(),
            TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT
        );
        // Test compression defaults (none means CompressionCodec::None)
        assert_eq!(
            table_properties.metadata_compression_codec().unwrap(),
            CompressionCodec::None
        );
        assert_eq!(
            table_properties.gc_enabled().unwrap(),
            TableProperties::PROPERTY_GC_ENABLED_DEFAULT
        );
        assert_eq!(
            table_properties.max_snapshot_age_ms().unwrap(),
            TableProperties::PROPERTY_MAX_SNAPSHOT_AGE_MS_DEFAULT
        );
        assert_eq!(
            table_properties.min_snapshots_to_keep().unwrap(),
            TableProperties::PROPERTY_MIN_SNAPSHOTS_TO_KEEP_DEFAULT
        );
        assert_eq!(
            table_properties.max_ref_age_ms().unwrap(),
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
        let table_properties = TableProperties::new(&props);
        assert_eq!(table_properties.max_snapshot_age_ms().unwrap(), 1234);
        assert_eq!(table_properties.min_snapshots_to_keep().unwrap(), 7);
        assert_eq!(table_properties.max_ref_age_ms().unwrap(), 5678);
    }

    #[test]
    fn test_table_properties_location_paths() {
        // Test unset.
        let raw_properties = HashMap::new();
        let table_properties = TableProperties::new(&raw_properties);
        assert_eq!(table_properties.write_metadata_path().unwrap(), None);
        assert_eq!(table_properties.write_data_location().unwrap(), None);
        assert_eq!(
            table_properties.write_folder_storage_location().unwrap(),
            None
        );
        assert_eq!(
            table_properties.write_object_storage_location().unwrap(),
            None
        );

        for key in [
            TableProperties::PROPERTY_WRITE_METADATA_PATH,
            TableProperties::PROPERTY_WRITE_DATA_LOCATION,
            TableProperties::PROPERTY_WRITE_FOLDER_STORAGE_LOCATION,
            TableProperties::PROPERTY_WRITE_OBJECT_STORAGE_LOCATION,
        ] {
            // Test empty paths are invalid and retain the property key as error context.
            let raw_properties = HashMap::from([(key.to_string(), String::new())]);
            let table_properties = TableProperties::new(&raw_properties);
            let error = match key {
                TableProperties::PROPERTY_WRITE_METADATA_PATH => {
                    table_properties.write_metadata_path().unwrap_err()
                }
                TableProperties::PROPERTY_WRITE_DATA_LOCATION => {
                    table_properties.write_data_location().unwrap_err()
                }
                TableProperties::PROPERTY_WRITE_FOLDER_STORAGE_LOCATION => table_properties
                    .write_folder_storage_location()
                    .unwrap_err(),
                TableProperties::PROPERTY_WRITE_OBJECT_STORAGE_LOCATION => table_properties
                    .write_object_storage_location()
                    .unwrap_err(),
                _ => unreachable!(),
            };
            assert_eq!(error.kind(), ErrorKind::DataInvalid);
            assert!(format!("{error}").contains(key));

            // Test all supported location properties share trailing-slash normalization.
            let raw_properties = HashMap::from([(
                key.to_string(),
                "s3://other-bucket/custom-path/".to_string(),
            )]);
            let table_properties = TableProperties::new(&raw_properties);
            let parsed = match key {
                TableProperties::PROPERTY_WRITE_METADATA_PATH => {
                    table_properties.write_metadata_path().unwrap()
                }
                TableProperties::PROPERTY_WRITE_DATA_LOCATION => {
                    table_properties.write_data_location().unwrap()
                }
                TableProperties::PROPERTY_WRITE_FOLDER_STORAGE_LOCATION => {
                    table_properties.write_folder_storage_location().unwrap()
                }
                TableProperties::PROPERTY_WRITE_OBJECT_STORAGE_LOCATION => {
                    table_properties.write_object_storage_location().unwrap()
                }
                _ => unreachable!(),
            };
            assert_eq!(parsed.as_deref(), Some("s3://other-bucket/custom-path"));
        }
    }

    #[test]
    fn test_table_properties_compression() {
        for (value, expected) in [
            ("gzip", CompressionCodec::gzip_default()),
            ("zstd", CompressionCodec::zstd_default()),
        ] {
            let props = HashMap::from([(
                TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC.to_string(),
                value.to_string(),
            )]);
            let table_properties = TableProperties::new(&props);
            assert_eq!(
                table_properties.metadata_compression_codec().unwrap(),
                expected
            );
        }
    }

    #[test]
    fn test_table_properties_compression_none() {
        let props = HashMap::from([(
            TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC.to_string(),
            "none".to_string(),
        )]);
        let table_properties = TableProperties::new(&props);
        assert_eq!(
            table_properties.metadata_compression_codec().unwrap(),
            CompressionCodec::None
        );
    }

    #[test]
    fn test_table_properties_compression_case_insensitive() {
        for (value, expected) in [
            ("GZIP", CompressionCodec::gzip_default()),
            ("GzIp", CompressionCodec::gzip_default()),
            ("ZSTD", CompressionCodec::zstd_default()),
            ("ZsTd", CompressionCodec::zstd_default()),
            ("NONE", CompressionCodec::None),
        ] {
            let props = HashMap::from([(
                TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC.to_string(),
                value.to_string(),
            )]);
            let table_properties = TableProperties::new(&props);
            assert_eq!(
                table_properties.metadata_compression_codec().unwrap(),
                expected
            );
        }
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
        let table_properties = TableProperties::new(&props);
        assert_eq!(table_properties.commit_num_retries().unwrap(), 10);
        assert_eq!(table_properties.commit_max_retry_wait_ms().unwrap(), 20);
        assert_eq!(
            table_properties.write_format_default().unwrap(),
            "avro".to_string()
        );
        assert_eq!(
            table_properties.write_target_file_size_bytes().unwrap(),
            512
        );
        assert!(!table_properties.gc_enabled().unwrap());
    }

    #[test]
    fn test_table_properties_invalid() {
        let invalid_retries = HashMap::from([(
            TableProperties::PROPERTY_COMMIT_NUM_RETRIES.to_string(),
            "abc".to_string(),
        )]);

        let table_properties = TableProperties::new(&invalid_retries);
        let error = table_properties.commit_num_retries().unwrap_err();
        assert!(
            error.to_string().contains(
                "Invalid value for commit.retry.num-retries: invalid digit found in string"
            )
        );

        let invalid_min_wait = HashMap::from([(
            TableProperties::PROPERTY_COMMIT_MIN_RETRY_WAIT_MS.to_string(),
            "abc".to_string(),
        )]);
        let table_properties = TableProperties::new(&invalid_min_wait);
        let error = table_properties.commit_min_retry_wait_ms().unwrap_err();
        assert!(
            error.to_string().contains(
                "Invalid value for commit.retry.min-wait-ms: invalid digit found in string"
            )
        );

        let invalid_max_wait = HashMap::from([(
            TableProperties::PROPERTY_COMMIT_MAX_RETRY_WAIT_MS.to_string(),
            "abc".to_string(),
        )]);
        let table_properties = TableProperties::new(&invalid_max_wait);
        let error = table_properties.commit_max_retry_wait_ms().unwrap_err();
        assert!(
            error.to_string().contains(
                "Invalid value for commit.retry.max-wait-ms: invalid digit found in string"
            )
        );

        let invalid_target_size = HashMap::from([(
            TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES.to_string(),
            "abc".to_string(),
        )]);
        let table_properties = TableProperties::new(&invalid_target_size);
        let error = table_properties.write_target_file_size_bytes().unwrap_err();
        assert!(error.to_string().contains(
            "Invalid value for write.target-file-size-bytes: invalid digit found in string"
        ));

        let invalid_gc_enabled = HashMap::from([(
            TableProperties::PROPERTY_GC_ENABLED.to_string(),
            "notabool".to_string(),
        )]);
        let table_properties = TableProperties::new(&invalid_gc_enabled);
        let error = table_properties.gc_enabled().unwrap_err();
        assert!(error.to_string().contains("Invalid value for gc.enabled"));
    }

    #[test]
    fn test_table_properties_compression_invalid_rejected() {
        let invalid_codecs = ["lz4", "snappy"];

        for codec in invalid_codecs {
            let props = HashMap::from([(
                TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC.to_string(),
                codec.to_string(),
            )]);
            let err = TableProperties::new(&props)
                .metadata_compression_codec()
                .unwrap_err();
            let err_msg = err.to_string();
            assert!(
                err_msg.contains(&format!("Invalid metadata compression codec: {codec}")),
                "Expected error message to contain codec '{codec}', got: {err_msg}"
            );
            assert!(
                err_msg.contains("Only 'none', 'gzip', and 'zstd' are supported"),
                "Expected error message to contain supported codecs, got: {err_msg}"
            );
        }
    }

    #[test]
    fn test_cdc_disabled_by_default() {
        let props = HashMap::new();
        let tp = TableProperties::new(&props);
        assert!(!tp.cdc_enabled().unwrap());
    }

    #[test]
    fn test_cdc_enabled_via_flag() {
        let props = HashMap::from([(
            TableProperties::PROPERTY_PARQUET_CDC_ENABLED.to_string(),
            "true".to_string(),
        )]);
        let tp = TableProperties::new(&props);
        assert!(tp.cdc_enabled().unwrap());
        assert_eq!(tp.cdc_min_chunk_size().unwrap(), 256 * 1024);
        assert_eq!(tp.cdc_max_chunk_size().unwrap(), 1024 * 1024);
        assert_eq!(tp.cdc_norm_level().unwrap(), 0);
    }

    #[test]
    fn test_cdc_size_props_alone_do_not_enable() {
        let props = HashMap::from([(
            TableProperties::PROPERTY_PARQUET_CDC_MIN_CHUNK_SIZE.to_string(),
            "262144".to_string(),
        )]);
        let tp = TableProperties::new(&props);
        assert!(!tp.cdc_enabled().unwrap());
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
        let tp = TableProperties::new(&props);
        assert!(tp.cdc_enabled().unwrap());
        assert_eq!(tp.cdc_min_chunk_size().unwrap(), 200000);
        assert_eq!(tp.cdc_max_chunk_size().unwrap(), 900000);
        assert_eq!(tp.cdc_norm_level().unwrap(), 1);
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
        let tp = TableProperties::new(&props);
        assert!(tp.cdc_enabled().unwrap());
        assert_eq!(tp.cdc_min_chunk_size().unwrap(), 256 * 1024);
        assert_eq!(tp.cdc_max_chunk_size().unwrap(), 1024 * 1024);
        assert_eq!(tp.cdc_norm_level().unwrap(), 2);
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
        let tp = TableProperties::new(&props);
        assert_eq!(tp.cdc_norm_level().unwrap(), -2);
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
        let err = TableProperties::new(&props)
            .cdc_min_chunk_size()
            .unwrap_err();
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
        let err = TableProperties::new(&props).cdc_norm_level().unwrap_err();
        assert!(
            err.to_string()
                .contains("Invalid value for write.parquet.content-defined-chunking.norm-level")
        );
    }

    #[test]
    fn test_cdc_no_properties() {
        let props = HashMap::from([("some.other.property".to_string(), "value".to_string())]);
        let tp = TableProperties::new(&props);
        assert!(!tp.cdc_enabled().unwrap());
    }

    #[test]
    fn test_parquet_sizing_defaults() {
        let props = HashMap::new();
        let tp = TableProperties::new(&props);
        // Default codec is zstd at its default level.
        assert_eq!(
            tp.parquet_compression_codec().unwrap(),
            CompressionCodec::zstd_default()
        );
        assert_eq!(
            tp.parquet_row_group_size_bytes().unwrap(),
            TableProperties::PROPERTY_PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT
        );
        assert_eq!(
            tp.parquet_page_size_bytes().unwrap(),
            TableProperties::PROPERTY_PARQUET_PAGE_SIZE_BYTES_DEFAULT
        );
        assert_eq!(
            tp.parquet_page_row_limit().unwrap(),
            TableProperties::PROPERTY_PARQUET_PAGE_ROW_LIMIT_DEFAULT
        );
        assert_eq!(
            tp.parquet_dict_size_bytes().unwrap(),
            TableProperties::PROPERTY_PARQUET_DICT_SIZE_BYTES_DEFAULT
        );
    }

    #[test]
    fn test_parquet_sizing_overrides() {
        let props = HashMap::from([
            (
                TableProperties::PROPERTY_PARQUET_COMPRESSION_CODEC.to_string(),
                "gzip".to_string(),
            ),
            (
                TableProperties::PROPERTY_PARQUET_COMPRESSION_LEVEL.to_string(),
                "4".to_string(),
            ),
            (
                TableProperties::PROPERTY_PARQUET_ROW_GROUP_SIZE_BYTES.to_string(),
                "1048576".to_string(),
            ),
            (
                TableProperties::PROPERTY_PARQUET_PAGE_SIZE_BYTES.to_string(),
                "65536".to_string(),
            ),
            (
                TableProperties::PROPERTY_PARQUET_PAGE_ROW_LIMIT.to_string(),
                "5000".to_string(),
            ),
            (
                TableProperties::PROPERTY_PARQUET_DICT_SIZE_BYTES.to_string(),
                "131072".to_string(),
            ),
        ]);
        let tp = TableProperties::new(&props);
        // Codec name and level are folded into a single CompressionCodec.
        assert_eq!(
            tp.parquet_compression_codec().unwrap(),
            CompressionCodec::Gzip(4)
        );
        assert_eq!(tp.parquet_row_group_size_bytes().unwrap(), 1048576);
        assert_eq!(tp.parquet_page_size_bytes().unwrap(), 65536);
        assert_eq!(tp.parquet_page_row_limit().unwrap(), 5000);
        assert_eq!(tp.parquet_dict_size_bytes().unwrap(), 131072);
    }

    #[test]
    fn test_parquet_invalid_sizing_rejected() {
        let props = HashMap::from([(
            TableProperties::PROPERTY_PARQUET_ROW_GROUP_SIZE_BYTES.to_string(),
            "not_a_number".to_string(),
        )]);
        let err = TableProperties::new(&props)
            .parquet_row_group_size_bytes()
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.to_string()
                .contains(TableProperties::PROPERTY_PARQUET_ROW_GROUP_SIZE_BYTES)
        );
    }

    #[test]
    fn test_parquet_all_codecs_parse() {
        // Every codec name parquet-java supports must parse (parity with Java's
        // `CompressionCodecName.valueOf`).
        for (name, expected) in [
            ("uncompressed", CompressionCodec::None),
            ("snappy", CompressionCodec::Snappy),
            ("gzip", CompressionCodec::gzip_default()),
            ("lzo", CompressionCodec::Lzo),
            ("brotli", CompressionCodec::brotli_default()),
            ("lz4", CompressionCodec::Lz4),
            ("lz4_raw", CompressionCodec::Lz4Raw),
            ("zstd", CompressionCodec::zstd_default()),
        ] {
            let props = HashMap::from([(
                TableProperties::PROPERTY_PARQUET_COMPRESSION_CODEC.to_string(),
                name.to_string(),
            )]);
            let tp = TableProperties::new(&props);
            assert_eq!(
                tp.parquet_compression_codec().unwrap(),
                expected,
                "codec {name}"
            );
        }
    }

    #[test]
    fn test_parquet_compression_level_ignored_for_levelless_codec() {
        // A level set alongside a codec that carries none (e.g. snappy) is
        // ignored rather than rejected, matching parquet-java.
        let props = HashMap::from([
            (
                TableProperties::PROPERTY_PARQUET_COMPRESSION_CODEC.to_string(),
                "snappy".to_string(),
            ),
            (
                TableProperties::PROPERTY_PARQUET_COMPRESSION_LEVEL.to_string(),
                "5".to_string(),
            ),
        ]);
        let tp = TableProperties::new(&props);
        assert_eq!(
            tp.parquet_compression_codec().unwrap(),
            CompressionCodec::Snappy
        );
    }

    #[test]
    fn test_parse_boolean_property_case_insensitive() {
        let false_variants = ["False", "FALSE"];
        let true_variants = ["True", "TRUE"];

        for f in false_variants {
            let props = HashMap::from([(
                TableProperties::PROPERTY_WRITE_OBJECT_STORAGE_PARTITIONED_PATHS.to_string(),
                f.to_string(),
            )]);
            let tp = TableProperties::new(&props);
            assert!(!tp.write_object_storage_partitioned_paths().unwrap());
        }

        for t in true_variants {
            let props = HashMap::from([(
                TableProperties::PROPERTY_WRITE_OBJECT_STORAGE_PARTITIONED_PATHS.to_string(),
                t.to_string(),
            )]);
            let tp = TableProperties::new(&props);
            assert!(tp.write_object_storage_partitioned_paths().unwrap());
        }
    }

    #[test]
    fn test_table_properties_default_name_mapping() {
        // Test unset.
        let properties = HashMap::new();
        assert!(
            TableProperties::new(&properties)
                .default_name_mapping()
                .unwrap()
                .is_none()
        );

        let properties = HashMap::from([(
            TableProperties::PROPERTY_DEFAULT_NAME_MAPPING.to_string(),
            r#"[{"field-id":1,"names":["id","record_id"]}]"#.to_string(),
        )]);
        let mapping = TableProperties::new(&properties)
            .default_name_mapping()
            .unwrap()
            .unwrap();
        assert_eq!(mapping.fields().len(), 1);
        assert_eq!(mapping.fields()[0].field_id(), Some(1));
        assert_eq!(mapping.fields()[0].names(), &[
            "id".to_string(),
            "record_id".to_string()
        ]);
    }

    #[test]
    fn test_table_properties_malformed_name_mapping() {
        let properties = HashMap::from([(
            TableProperties::PROPERTY_DEFAULT_NAME_MAPPING.to_string(),
            "{ not valid json".to_string(),
        )]);
        let error = TableProperties::new(&properties)
            .default_name_mapping()
            .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        // The property key must survive as error context.
        assert!(
            format!("{error}").contains(TableProperties::PROPERTY_DEFAULT_NAME_MAPPING),
            "{error}"
        );
    }
}
