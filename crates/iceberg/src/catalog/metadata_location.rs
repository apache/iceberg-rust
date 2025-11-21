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

use uuid::Uuid;

use crate::spec::TableProperties;
use crate::{Error, ErrorKind, Result};

/// The file extension suffix for gzip compressed metadata files
const GZIP_SUFFIX: &str = ".gz";

/// Helper for parsing a location of the format: `<location>/metadata/<version>-<uuid>.metadata.json`
/// or with compression: `<location>/metadata/<version>-<uuid>.gz.metadata.json`
#[derive(Clone, Debug, PartialEq)]
pub struct MetadataLocation {
    table_location: String,
    version: i32,
    id: Uuid,
    compression_suffix: Option<String>,
}

impl MetadataLocation {
    /// Determines the compression suffix from table properties.
    fn compression_suffix_from_properties(properties: &HashMap<String, String>) -> Option<String> {
        properties
            .get(TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC)
            .and_then(|codec| match codec.to_lowercase().as_str() {
                "gzip" => Some(GZIP_SUFFIX.to_string()),
                "none" | "" => None,
                _ => None,
            })
    }

    /// Creates a completely new metadata location starting at version 0.
    /// Only used for creating a new table. For updates, see `with_next_version`.
    #[deprecated(
        since = "0.8.0",
        note = "Use new_with_properties instead to properly handle compression settings"
    )]
    pub fn new_with_table_location(table_location: impl ToString) -> Self {
        Self {
            table_location: table_location.to_string(),
            version: 0,
            id: Uuid::new_v4(),
            compression_suffix: None,
        }
    }

    /// Creates a completely new metadata location starting at version 0,
    /// with compression settings from the table's properties.
    /// Only used for creating a new table. For updates, see `with_next_version`.
    pub fn new_with_properties(
        table_location: impl ToString,
        properties: &HashMap<String, String>,
    ) -> Self {
        Self {
            table_location: table_location.to_string(),
            version: 0,
            id: Uuid::new_v4(),
            compression_suffix: Self::compression_suffix_from_properties(properties),
        }
    }

    /// Creates a new metadata location for an updated metadata file.
    /// Preserves the compression settings from the current location.
    #[deprecated(
        since = "0.8.0",
        note = "Use with_next_version_and_properties instead to properly handle compression settings changes"
    )]
    pub fn with_next_version(&self) -> Self {
        Self {
            table_location: self.table_location.clone(),
            version: self.version + 1,
            id: Uuid::new_v4(),
            compression_suffix: self.compression_suffix.clone(),
        }
    }

    /// Creates a new metadata location for an updated metadata file.
    /// Takes table properties to determine compression settings, which may have changed
    /// from the previous version.
    pub fn with_next_version_and_properties(&self, properties: &HashMap<String, String>) -> Self {
        Self {
            table_location: self.table_location.clone(),
            version: self.version + 1,
            id: Uuid::new_v4(),
            compression_suffix: Self::compression_suffix_from_properties(properties),
        }
    }

    fn parse_metadata_path_prefix(path: &str) -> Result<String> {
        let prefix = path.strip_suffix("/metadata").ok_or(Error::new(
            ErrorKind::Unexpected,
            format!("Metadata location not under \"/metadata\" subdirectory: {path}"),
        ))?;

        Ok(prefix.to_string())
    }

    /// Parses a file name of the format `<version>-<uuid>.metadata.json`
    /// or with compression: `<version>-<uuid>.gz.metadata.json`.
    fn parse_file_name(file_name: &str) -> Result<(i32, Uuid, Option<String>)> {
        let stripped = file_name.strip_suffix(".metadata.json").ok_or(Error::new(
            ErrorKind::Unexpected,
            format!("Invalid metadata file ending: {file_name}"),
        ))?;

        // Check for compression suffix (e.g., .gz)
        let (stripped, compression_suffix) = if let Some(s) = stripped.strip_suffix(GZIP_SUFFIX) {
            (s, Some(GZIP_SUFFIX.to_string()))
        } else {
            (stripped, None)
        };

        let (version, id) = stripped.split_once('-').ok_or(Error::new(
            ErrorKind::Unexpected,
            format!("Invalid metadata file name format: {file_name}"),
        ))?;

        Ok((
            version.parse::<i32>()?,
            Uuid::parse_str(id)?,
            compression_suffix,
        ))
    }
}

impl Display for MetadataLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let suffix = self.compression_suffix.as_deref().unwrap_or("");
        write!(
            f,
            "{}/metadata/{:0>5}-{}{}.metadata.json",
            self.table_location, self.version, self.id, suffix
        )
    }
}

impl FromStr for MetadataLocation {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let (path, file_name) = s.rsplit_once('/').ok_or(Error::new(
            ErrorKind::Unexpected,
            format!("Invalid metadata location: {s}"),
        ))?;

        let prefix = Self::parse_metadata_path_prefix(path)?;
        let (version, id, compression_suffix) = Self::parse_file_name(file_name)?;

        Ok(MetadataLocation {
            table_location: prefix,
            version,
            id,
            compression_suffix,
        })
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::str::FromStr;

    use uuid::Uuid;

    use crate::MetadataLocation;

    #[test]
    fn test_metadata_location_from_string() {
        let test_cases = vec![
            // No prefix
            (
                "/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
                Ok(MetadataLocation {
                    table_location: "".to_string(),
                    version: 1234567,
                    id: Uuid::from_str("2cd22b57-5127-4198-92ba-e4e67c79821b").unwrap(),
                    compression_suffix: None,
                }),
            ),
            // Some prefix
            (
                "/abc/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
                Ok(MetadataLocation {
                    table_location: "/abc".to_string(),
                    version: 1234567,
                    id: Uuid::from_str("2cd22b57-5127-4198-92ba-e4e67c79821b").unwrap(),
                    compression_suffix: None,
                }),
            ),
            // Longer prefix
            (
                "/abc/def/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
                Ok(MetadataLocation {
                    table_location: "/abc/def".to_string(),
                    version: 1234567,
                    id: Uuid::from_str("2cd22b57-5127-4198-92ba-e4e67c79821b").unwrap(),
                    compression_suffix: None,
                }),
            ),
            // Prefix with special characters
            (
                "https://127.0.0.1/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
                Ok(MetadataLocation {
                    table_location: "https://127.0.0.1".to_string(),
                    version: 1234567,
                    id: Uuid::from_str("2cd22b57-5127-4198-92ba-e4e67c79821b").unwrap(),
                    compression_suffix: None,
                }),
            ),
            // Another id
            (
                "/abc/metadata/1234567-81056704-ce5b-41c4-bb83-eb6408081af6.metadata.json",
                Ok(MetadataLocation {
                    table_location: "/abc".to_string(),
                    version: 1234567,
                    id: Uuid::from_str("81056704-ce5b-41c4-bb83-eb6408081af6").unwrap(),
                    compression_suffix: None,
                }),
            ),
            // Version 0
            (
                "/abc/metadata/00000-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
                Ok(MetadataLocation {
                    table_location: "/abc".to_string(),
                    version: 0,
                    id: Uuid::from_str("2cd22b57-5127-4198-92ba-e4e67c79821b").unwrap(),
                    compression_suffix: None,
                }),
            ),
            // With gzip compression
            (
                "/abc/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.gz.metadata.json",
                Ok(MetadataLocation {
                    table_location: "/abc".to_string(),
                    version: 1234567,
                    id: Uuid::from_str("2cd22b57-5127-4198-92ba-e4e67c79821b").unwrap(),
                    compression_suffix: Some(".gz".to_string()),
                }),
            ),
            // Negative version
            (
                "/metadata/-123-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
                Err("".to_string()),
            ),
            // Invalid uuid
            (
                "/metadata/1234567-no-valid-id.metadata.json",
                Err("".to_string()),
            ),
            // Non-numeric version
            (
                "/metadata/noversion-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
                Err("".to_string()),
            ),
            // No /metadata subdirectory
            (
                "/wrongsubdir/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
                Err("".to_string()),
            ),
            // No .metadata.json suffix
            (
                "/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata",
                Err("".to_string()),
            ),
            (
                "/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.wrong.file",
                Err("".to_string()),
            ),
        ];

        for (input, expected) in test_cases {
            match MetadataLocation::from_str(input) {
                Ok(metadata_location) => {
                    assert!(expected.is_ok());
                    assert_eq!(metadata_location, expected.unwrap());
                }
                Err(_) => assert!(expected.is_err()),
            }
        }
    }

    #[test]
    #[allow(deprecated)]
    fn test_metadata_location_with_next_version() {
        let test_cases = vec![
            MetadataLocation::new_with_table_location("/abc"),
            MetadataLocation::from_str(
                "/abc/def/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
            )
            .unwrap(),
        ];

        for input in test_cases {
            let next = MetadataLocation::from_str(&input.to_string())
                .unwrap()
                .with_next_version();
            assert_eq!(next.table_location, input.table_location);
            assert_eq!(next.version, input.version + 1);
            assert_ne!(next.id, input.id);
        }
    }

    #[test]
    fn test_metadata_location_new_with_properties() {
        // Test with no compression
        let props_none = HashMap::new();
        let location = MetadataLocation::new_with_properties("/test/table", &props_none);
        assert_eq!(location.table_location, "/test/table");
        assert_eq!(location.version, 0);
        assert_eq!(location.compression_suffix, None);
        assert_eq!(
            location.to_string(),
            format!("/test/table/metadata/00000-{}.metadata.json", location.id)
        );

        // Test with gzip compression
        let mut props_gzip = HashMap::new();
        props_gzip.insert(
            "write.metadata.compression-codec".to_string(),
            "gzip".to_string(),
        );
        let location = MetadataLocation::new_with_properties("/test/table", &props_gzip);
        assert_eq!(location.compression_suffix, Some(".gz".to_string()));
        assert_eq!(
            location.to_string(),
            format!(
                "/test/table/metadata/00000-{}.gz.metadata.json",
                location.id
            )
        );

        // Test with "none" codec (explicitly no compression)
        let mut props_explicit_none = HashMap::new();
        props_explicit_none.insert(
            "write.metadata.compression-codec".to_string(),
            "none".to_string(),
        );
        let location = MetadataLocation::new_with_properties("/test/table", &props_explicit_none);
        assert_eq!(location.compression_suffix, None);

        // Test case insensitivity
        let mut props_gzip_upper = HashMap::new();
        props_gzip_upper.insert(
            "write.metadata.compression-codec".to_string(),
            "GZIP".to_string(),
        );
        let location = MetadataLocation::new_with_properties("/test/table", &props_gzip_upper);
        assert_eq!(location.compression_suffix, Some(".gz".to_string()));
    }

    #[test]
    fn test_with_next_version_and_properties() {
        // Start with a location without compression
        let props_none = HashMap::new();
        let location = MetadataLocation::new_with_properties("/test/table", &props_none);
        assert_eq!(location.compression_suffix, None);
        assert_eq!(location.version, 0);

        // Update to next version with gzip compression
        let mut props_gzip = HashMap::new();
        props_gzip.insert(
            "write.metadata.compression-codec".to_string(),
            "gzip".to_string(),
        );
        let next_location = location.with_next_version_and_properties(&props_gzip);
        assert_eq!(next_location.compression_suffix, Some(".gz".to_string()));
        assert_eq!(next_location.version, 1);
        assert_eq!(
            next_location.to_string(),
            format!(
                "/test/table/metadata/00001-{}.gz.metadata.json",
                next_location.id
            )
        );

        // Update to next version without compression (changed from gzip)
        let props_none_again = HashMap::new();
        let final_location = next_location.with_next_version_and_properties(&props_none_again);
        assert_eq!(final_location.compression_suffix, None);
        assert_eq!(final_location.version, 2);
        assert_eq!(
            final_location.to_string(),
            format!(
                "/test/table/metadata/00002-{}.metadata.json",
                final_location.id
            )
        );
    }
}
