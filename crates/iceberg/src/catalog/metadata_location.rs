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

use crate::compression::CompressionCodec;
use crate::spec::{parse_metadata_file_compression, TableMetadata};
use crate::{Error, ErrorKind, Result};

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
    fn compression_suffix_from_properties(
        properties: &HashMap<String, String>,
    ) -> Result<Option<String>> {
        let codec = parse_metadata_file_compression(properties)?;

        Ok(if codec.is_none() {
            None
        } else {
            Some(codec.suffix()?.to_string())
        })
    }

    /// Creates a completely new metadata location starting at version 0.
    /// Only used for creating a new table. For updates, see `next_version`.
    #[deprecated(
        since = "0.8.0",
        note = "Use new_with_metadata instead to properly handle compression settings"
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
    /// with compression settings from the table metadata.
    /// Only used for creating a new table. For updates, see `next_version`.
    pub fn new_with_metadata(table_location: impl ToString, metadata: &TableMetadata) -> Self {
        Self {
            table_location: table_location.to_string(),
            version: 0,
            id: Uuid::new_v4(),
            // This will go away https://github.com/apache/iceberg-rust/issues/2028 is resolved, so for now
            // we use a default value.
            compression_suffix: Self::compression_suffix_from_properties(metadata.properties())
                .unwrap_or(None),
        }
    }

    /// Creates a new metadata location for an updated metadata file.
    /// Uses compression settings from the new metadata.
    pub fn next_version(
        current_location: impl ToString,
        new_metadata: &TableMetadata,
    ) -> Result<Self> {
        let current = Self::from_str(&current_location.to_string())?;
        let next = Self {
            table_location: current.table_location,
            version: current.version + 1,
            id: Uuid::new_v4(),
            compression_suffix: Self::compression_suffix_from_properties(
                new_metadata.properties(),
            )?,
        };
        Ok(next)
    }

    /// Creates a new metadata location for an updated metadata file.
    /// Preserves the compression settings from the current location.
    #[deprecated(
        since = "0.8.0",
        note = "Use `next_version` instead to properly handle compression settings"
    )]
    pub fn with_next_version(&self) -> Self {
        Self {
            table_location: self.table_location.clone(),
            version: self.version + 1,
            id: Uuid::new_v4(),
            compression_suffix: self.compression_suffix.clone(),
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
        let gzip_suffix = CompressionCodec::Gzip.suffix()?;
        let (stripped, compression_suffix) = if let Some(s) = stripped.strip_suffix(gzip_suffix) {
            (s, Some(gzip_suffix.to_string()))
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

    use crate::compression::CompressionCodec;
    use crate::spec::{Schema, TableMetadata, TableMetadataBuilder};
    use crate::{MetadataLocation, TableCreation};

    fn create_test_metadata(properties: HashMap<String, String>) -> TableMetadata {
        let table_creation = TableCreation::builder()
            .name("test_table".to_string())
            .location("/test/table".to_string())
            .schema(Schema::builder().build().unwrap())
            .properties(properties)
            .build();
        TableMetadataBuilder::from_table_creation(table_creation)
            .unwrap()
            .build()
            .unwrap()
            .metadata
    }

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
                    compression_suffix: Some(CompressionCodec::Gzip.suffix().unwrap().to_string()),
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
    #[allow(deprecated)]
    fn test_metadata_location_next_version() {
        let test_cases = vec![
            MetadataLocation::new_with_table_location("/abc"),
            MetadataLocation::from_str(
                "/abc/def/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
            )
            .unwrap(),
        ];

        for input in test_cases {
            let next = MetadataLocation::next_version(
                input.to_string(),
                &create_test_metadata(HashMap::new()),
            )
            .unwrap();
            assert_eq!(next.table_location, input.table_location);
            assert_eq!(next.version, input.version + 1);
            assert_ne!(next.id, input.id);
        }
    }

    #[test]
    fn test_metadata_location_new_with_metadata() {
        // Test with no compression
        let props_none = HashMap::new();
        let metadata_none = create_test_metadata(props_none);
        let location = MetadataLocation::new_with_metadata("/test/table", &metadata_none);
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
        let metadata_gzip = create_test_metadata(props_gzip);
        let location = MetadataLocation::new_with_metadata("/test/table", &metadata_gzip);
        let gzip_suffix = CompressionCodec::Gzip.suffix().unwrap();
        assert_eq!(location.compression_suffix, Some(gzip_suffix.to_string()));
        assert_eq!(
            location.to_string(),
            format!(
                "/test/table/metadata/00000-{}{gzip_suffix}.metadata.json",
                location.id
            )
        );

        // Test with "none" codec (explicitly no compression)
        let mut props_explicit_none = HashMap::new();
        props_explicit_none.insert(
            "write.metadata.compression-codec".to_string(),
            "none".to_string(),
        );
        let metadata_none_explicit = create_test_metadata(props_explicit_none);
        let location = MetadataLocation::new_with_metadata("/test/table", &metadata_none_explicit);
        assert_eq!(location.compression_suffix, None);

        // Test case insensitivity
        let mut props_gzip_upper = HashMap::new();
        props_gzip_upper.insert(
            "write.metadata.compression-codec".to_string(),
            "GZIP".to_string(),
        );
        let metadata_gzip_upper = create_test_metadata(props_gzip_upper);
        let location = MetadataLocation::new_with_metadata("/test/table", &metadata_gzip_upper);
        assert_eq!(
            location.compression_suffix,
            Some(CompressionCodec::Gzip.suffix().unwrap().to_string())
        );
    }

    #[test]
    fn test_metadata_next_version_handles_compression() {
        let gzip_suffix = CompressionCodec::Gzip.suffix().unwrap();

        // Start with a location without compression
        let props_none = HashMap::new();
        let metadata_none = create_test_metadata(props_none);
        let initial_location = MetadataLocation::new_with_metadata("/test/table", &metadata_none);
        let initial_path = initial_location.to_string();

        // Update with no compression (should stay uncompressed)
        let next_none = MetadataLocation::next_version(&initial_path, &metadata_none).unwrap();
        let next_none_str = next_none.to_string();
        assert!(next_none_str.contains("/test/table/metadata/00001-"));
        assert!(!next_none_str.contains(&format!("{gzip_suffix}.")));
        assert!(next_none_str.ends_with(".metadata.json"));

        // Update with gzip compression (should become compressed)
        let mut props_gzip = HashMap::new();
        props_gzip.insert(
            "write.metadata.compression-codec".to_string(),
            "gzip".to_string(),
        );
        let metadata_gzip = create_test_metadata(props_gzip);
        let next_gzip = MetadataLocation::next_version(&initial_path, &metadata_gzip).unwrap();
        let next_gzip_str = next_gzip.to_string();
        assert!(next_gzip_str.contains("/test/table/metadata/00001-"));
        assert!(next_gzip_str.contains(&format!("{gzip_suffix}.")));
        assert!(next_gzip_str.ends_with(".metadata.json"));

        // Start with a compressed location
        let initial_gzip = MetadataLocation::new_with_metadata("/test/table", &metadata_gzip);
        let initial_gzip_path = initial_gzip.to_string();

        // Update with no compression (should become uncompressed)
        let next_uncompressed =
            MetadataLocation::next_version(&initial_gzip_path, &metadata_none).unwrap();
        let next_uncompressed_str = next_uncompressed.to_string();
        assert!(next_uncompressed_str.contains("/test/table/metadata/00001-"));
        assert!(!next_uncompressed_str.contains(&format!("{gzip_suffix}.")));
        assert!(next_uncompressed_str.ends_with(".metadata.json"));

        // Update with "none" codec (should be uncompressed)
        let mut props_explicit_none = HashMap::new();
        props_explicit_none.insert(
            "write.metadata.compression-codec".to_string(),
            "none".to_string(),
        );
        let metadata_none_explicit = create_test_metadata(props_explicit_none);
        let next_explicit_none =
            MetadataLocation::next_version(&initial_gzip_path, &metadata_none_explicit).unwrap();
        assert!(
            !next_explicit_none
                .to_string()
                .contains(&format!("{gzip_suffix}."))
        );

        // Test case insensitivity
        let mut props_gzip_upper = HashMap::new();
        props_gzip_upper.insert(
            "write.metadata.compression-codec".to_string(),
            "GZIP".to_string(),
        );
        let metadata_gzip_upper = create_test_metadata(props_gzip_upper);
        let next_gzip_upper =
            MetadataLocation::next_version(&initial_path, &metadata_gzip_upper).unwrap();
        assert!(
            next_gzip_upper
                .to_string()
                .contains(&format!("{gzip_suffix}."))
        );
    }

    #[test]
    #[allow(deprecated)]
    fn test_with_next_version() {
        // Start with a location without compression
        let props_none = HashMap::new();
        let metadata_none = create_test_metadata(props_none);
        let location = MetadataLocation::new_with_metadata("/test/table", &metadata_none);
        assert_eq!(location.compression_suffix, None);
        assert_eq!(location.version, 0);

        // Update to next version - compression is preserved
        let next_location = location.with_next_version();
        assert_eq!(next_location.compression_suffix, None);
        assert_eq!(next_location.version, 1);
        assert_eq!(
            next_location.to_string(),
            format!(
                "/test/table/metadata/00001-{}.metadata.json",
                next_location.id
            )
        );

        // Start with a location with gzip compression
        let mut props_gzip = HashMap::new();
        props_gzip.insert(
            "write.metadata.compression-codec".to_string(),
            "gzip".to_string(),
        );
        let metadata_gzip = create_test_metadata(props_gzip);
        let location_gzip = MetadataLocation::new_with_metadata("/test/table", &metadata_gzip);
        let gzip_suffix = CompressionCodec::Gzip.suffix().unwrap();
        assert_eq!(
            location_gzip.compression_suffix,
            Some(gzip_suffix.to_string())
        );

        // Update to next version - gzip compression is preserved
        let next_location_gzip = location_gzip.with_next_version();
        assert_eq!(
            next_location_gzip.compression_suffix,
            Some(gzip_suffix.to_string())
        );
        assert_eq!(next_location_gzip.version, 1);
        assert_eq!(
            next_location_gzip.to_string(),
            format!(
                "/test/table/metadata/00001-{}{gzip_suffix}.metadata.json",
                next_location_gzip.id
            )
        );
    }
}
