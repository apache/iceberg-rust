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

// Helper function to parse a property from a HashMap
// If the property is not found, use the default value
fn parse_property<T: std::str::FromStr>(
    properties: &HashMap<String, String>,
    key: &str,
    default: T,
) -> Result<T, anyhow::Error>
where
    <T as std::str::FromStr>::Err: std::fmt::Display,
{
    properties.get(key).map_or(Ok(default), |value| {
        value
            .parse::<T>()
            .map_err(|e| anyhow::anyhow!("Invalid value for {key}: {e}"))
    })
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
    /// Whether to use `FanoutWriter` for partitioned tables.
    pub write_datafusion_fanout_enabled: bool,
    /// Master key ID for encryption. When set, all data and manifest files will be encrypted.
    pub encryption_key_id: Option<String>,
    /// Length of data encryption keys in bytes.
    pub encryption_dek_length: Option<usize>,
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
    /// Whether to use `FanoutWriter` for partitioned tables (handles unsorted data).
    /// If false, uses `ClusteredWriter` (requires sorted data, more memory efficient).
    pub const PROPERTY_DATAFUSION_WRITE_FANOUT_ENABLED: &str = "write.datafusion.fanout.enabled";
    /// Default value for fanout writer enabled
    pub const PROPERTY_DATAFUSION_WRITE_FANOUT_ENABLED_DEFAULT: bool = true;

    // Encryption properties

    /// Master key ID for encrypting data encryption keys.
    ///
    /// When set, enables table-level encryption where all data and manifest
    /// files are encrypted using data encryption keys (DEKs) that are
    /// themselves encrypted with this master key.
    pub const PROPERTY_ENCRYPTION_KEY_ID: &str = "encryption.key-id";

    /// Length of data encryption keys in bytes.
    ///
    /// Controls the key size for AES encryption. Common values are 16 (AES-128)
    /// which is the only encryption method currently supported in the parquet
    pub const PROPERTY_ENCRYPTION_DEK_LENGTH: &str = "encryption.data-key-length";
    /// Default length for data encryption keys (16 bytes = AES-128).
    pub const PROPERTY_ENCRYPTION_DEK_LENGTH_DEFAULT: usize = 16;

    /// Default AAD (Additional Authenticated Data) length for GCM encryption.
    ///
    /// AAD provides additional context for authenticated encryption modes like AES-GCM.
    /// This is hardcoded to 16 bytes for Java compatibility and is not configurable.
    pub const PROPERTY_ENCRYPTION_AAD_LENGTH_DEFAULT: usize = 16;
}

impl TryFrom<&HashMap<String, String>> for TableProperties {
    // parse by entry key or use default value
    type Error = anyhow::Error;

    fn try_from(props: &HashMap<String, String>) -> Result<Self, Self::Error> {
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
            write_datafusion_fanout_enabled: parse_property(
                props,
                TableProperties::PROPERTY_DATAFUSION_WRITE_FANOUT_ENABLED,
                TableProperties::PROPERTY_DATAFUSION_WRITE_FANOUT_ENABLED_DEFAULT,
            )?,
            // Encryption properties - all optional
            encryption_key_id: props
                .get(TableProperties::PROPERTY_ENCRYPTION_KEY_ID)
                .cloned(),
            encryption_dek_length: props
                .get(TableProperties::PROPERTY_ENCRYPTION_DEK_LENGTH)
                .and_then(|v| v.parse().ok()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        // Encryption properties should be None by default
        assert_eq!(table_properties.encryption_key_id, None);
        assert_eq!(table_properties.encryption_dek_length, None);
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
        ]);
        let table_properties = TableProperties::try_from(&props).unwrap();
        assert_eq!(table_properties.commit_num_retries, 10);
        assert_eq!(table_properties.commit_max_retry_wait_ms, 20);
        assert_eq!(table_properties.write_format_default, "avro".to_string());
        assert_eq!(table_properties.write_target_file_size_bytes, 512);
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
    }

    #[test]
    fn test_encryption_properties_valid() {
        let props = HashMap::from([
            (
                TableProperties::PROPERTY_ENCRYPTION_KEY_ID.to_string(),
                "test-key-123".to_string(),
            ),
            (
                TableProperties::PROPERTY_ENCRYPTION_DEK_LENGTH.to_string(),
                "32".to_string(),
            ),
        ]);
        let table_properties = TableProperties::try_from(&props).unwrap();
        assert_eq!(
            table_properties.encryption_key_id,
            Some("test-key-123".to_string())
        );
        assert_eq!(table_properties.encryption_dek_length, Some(32));
    }

    #[test]
    fn test_encryption_properties_partial() {
        // Test with only the key ID set, not the DEK length
        let props = HashMap::from([(
            TableProperties::PROPERTY_ENCRYPTION_KEY_ID.to_string(),
            "my-master-key".to_string(),
        )]);
        let table_properties = TableProperties::try_from(&props).unwrap();
        assert_eq!(
            table_properties.encryption_key_id,
            Some("my-master-key".to_string())
        );
        assert_eq!(table_properties.encryption_dek_length, None);
    }

    #[test]
    fn test_encryption_properties_invalid_numeric() {
        // Test that invalid numeric values are silently ignored (parsed as None)
        let props = HashMap::from([
            (
                TableProperties::PROPERTY_ENCRYPTION_KEY_ID.to_string(),
                "key-456".to_string(),
            ),
            (
                TableProperties::PROPERTY_ENCRYPTION_DEK_LENGTH.to_string(),
                "not-a-number".to_string(),
            ),
        ]);
        let table_properties = TableProperties::try_from(&props).unwrap();
        assert_eq!(
            table_properties.encryption_key_id,
            Some("key-456".to_string())
        );
        // Invalid numeric values should be parsed as None
        assert_eq!(table_properties.encryption_dek_length, None);
    }

    #[test]
    fn test_encryption_properties_with_other_properties() {
        // Test encryption properties alongside other table properties
        let props = HashMap::from([
            (
                TableProperties::PROPERTY_COMMIT_NUM_RETRIES.to_string(),
                "8".to_string(),
            ),
            (
                TableProperties::PROPERTY_DEFAULT_FILE_FORMAT.to_string(),
                "orc".to_string(),
            ),
            (
                TableProperties::PROPERTY_ENCRYPTION_KEY_ID.to_string(),
                "combined-test-key".to_string(),
            ),
            (
                TableProperties::PROPERTY_ENCRYPTION_DEK_LENGTH.to_string(),
                "16".to_string(),
            ),
        ]);
        let table_properties = TableProperties::try_from(&props).unwrap();
        // Check regular properties
        assert_eq!(table_properties.commit_num_retries, 8);
        assert_eq!(table_properties.write_format_default, "orc".to_string());
        // Check encryption properties
        assert_eq!(
            table_properties.encryption_key_id,
            Some("combined-test-key".to_string())
        );
        assert_eq!(table_properties.encryption_dek_length, Some(16));
    }
}
