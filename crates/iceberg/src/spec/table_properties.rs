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

/// Macro to define table properties with type-safe access and validation.
///
/// # Example
/// ```ignore
/// define_table_properties! {
///     TableProperties {
///         commit_num_retries: usize = "commit.retry.num-retries" => 4,
///         commit_min_retry_wait_ms: u64 = "commit.retry.min-wait-ms" => 100,
///         write_format_default: String = "write.format.default" => "parquet",
///     }
/// }
/// ```
macro_rules! define_table_properties {
    (
        $struct_name:ident {
            $(
                $(#[$field_doc:meta])*
                $field_name:ident: $field_type:ty = $key:literal => $default:expr
            ),* $(,)?
        }
    ) => {
        /// Table properties with type-safe access and validation
        #[derive(Clone)]
        pub struct $struct_name {
            $(
                $(#[$field_doc])*
                pub $field_name: $field_type,
            )*
        }

        impl $struct_name {
            $(
                paste::paste! {
                    #[doc = "Property key for " $key]
                    pub const [<PROPERTY_ $field_name:upper>]: &'static str = $key;
                }
            )*

            /// Create a new instance with default values
            pub fn new() -> Self {
                Self {
                    $($field_name: parse_default!($default, $field_type),)*
                }
            }
        }

        impl Default for $struct_name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl TryFrom<HashMap<String, String>> for $struct_name {
            type Error = anyhow::Error;

            fn try_from(properties: HashMap<String, String>) -> Result<Self, Self::Error> {
                let mut result = Self::new();

                $(
                    paste::paste! {
                        if let Some(value_str) = properties.get(Self::[<PROPERTY_ $field_name:upper>]) {
                            result.$field_name = parse_value!(
                                value_str,
                                $field_type,
                                Self::[<PROPERTY_ $field_name:upper>]
                            )?;
                        }
                    }
                )*

                Ok(result)
            }
        }

        impl From<$struct_name> for HashMap<String, String> {
            fn from(properties: $struct_name) -> Self {
                let mut map = HashMap::new();

                $(
                    paste::paste! {
                        map.insert(
                            $struct_name::[<PROPERTY_ $field_name:upper>].to_string(),
                            format_value!(properties.$field_name)
                        );
                    }
                )*

                map
            }
        }
    };
}

/// Helper macro to parse default values based on type
#[macro_export]
macro_rules! parse_default {
    ($value:expr, String) => {
        $value.to_string()
    };
    ($value:expr, $type:ty) => {
        $value
    };
}

/// Helper macro to parse values from strings based on type
#[macro_export]
macro_rules! parse_value {
    ($value:expr, String, $key:expr) => {
        Ok::<String, anyhow::Error>($value.clone())
    };
    ($value:expr, $type:ty, $key:expr) => {
        $value
            .parse::<$type>()
            .map_err(|e| anyhow::anyhow!("Invalid value for {}: {}", $key, e))
    };
}

/// Helper macro to format values for storage
#[macro_export]
macro_rules! format_value {
    ($value:expr) => {
        $value.to_string()
    };
}

// Define the actual TableProperties struct using the macro
define_table_properties! {
    TableProperties {
        /// Number of commit retries
        commit_num_retries: usize = "commit.retry.num-retries" => 4,
        /// Minimum wait time (ms) between retries
        commit_min_retry_wait_ms: u64 = "commit.retry.min-wait-ms" => 100,
        /// Maximum wait time (ms) between retries
        commit_max_retry_wait_ms: u64 = "commit.retry.max-wait-ms" => 60000,
        /// Total maximum retry time (ms)
        commit_total_retry_timeout_ms: u64 = "commit.retry.total-timeout-ms" => 1800000,
        /// Default file format for data files
        write_format_default: String = "write.format.default" => "parquet".to_string(),
        /// Target file size in bytes
        write_target_file_size_bytes: u64 = "write.target-file-size-bytes" => 536870912,
    }
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;

    use super::*;
    use crate::{Error, ErrorKind};

    #[test]
    fn test_table_properties_default() {
        let properties = TableProperties::new();
        assert_eq!(properties.commit_num_retries, 4);
        assert_eq!(properties.commit_min_retry_wait_ms, 100);
        assert_eq!(properties.commit_max_retry_wait_ms, 60000);
        assert_eq!(properties.commit_total_retry_timeout_ms, 1800000);
        assert_eq!(properties.write_format_default, "parquet");
        assert_eq!(properties.write_target_file_size_bytes, 536870912);
    }

    #[test]
    fn test_table_properties_from_map() {
        let properties = TableProperties::try_from(HashMap::from([
            ("commit.retry.num-retries".to_string(), "5".to_string()),
            ("commit.retry.min-wait-ms".to_string(), "10".to_string()),
            ("write.format.default".to_string(), "avro".to_string()),
        ]))
        .unwrap();
        assert_eq!(properties.commit_num_retries, 5);
        assert_eq!(properties.commit_min_retry_wait_ms, 10);
        assert_eq!(properties.commit_max_retry_wait_ms, 60000);
        assert_eq!(properties.commit_max_retry_wait_ms, 1800000);
        assert_eq!(properties.write_format_default, "avro");
        assert_eq!(properties.write_target_file_size_bytes, 536870912);
    }
}
