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
use crate::spec::{TableMetadata, parse_metadata_file_compression};
use crate::{Error, ErrorKind, Result};

/// Helper for parsing a location of the format: `<location>/metadata/<version>-<uuid>.metadata.json`
/// or with compression: `<location>/metadata/<version>-<uuid>.gz.metadata.json`
#[derive(Clone, Debug, PartialEq)]
pub struct MetadataLocation {
    table_location: String,
    version: i32,
    id: Uuid,
    compression_codec: CompressionCodec,
}

impl MetadataLocation {
    /// Determines the compression codec from table properties.
    /// Parse errors result in CompressionCodec::None.
    fn compression_from_properties(properties: &HashMap<String, String>) -> CompressionCodec {
        parse_metadata_file_compression(properties).unwrap_or(CompressionCodec::None)
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
            compression_codec: CompressionCodec::None,
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
            compression_codec: Self::compression_from_properties(metadata.properties()),
        }
    }

    /// Creates a new metadata location for an updated metadata file.
    /// Increments the version number and generates a new UUID.
    pub fn with_next_version(&self) -> Self {
        Self {
            table_location: self.table_location.clone(),
            version: self.version + 1,
            id: Uuid::new_v4(),
            compression_codec: self.compression_codec,
        }
    }

    /// Updates the metadata location with compression settings from the new metadata.
    pub fn with_new_metadata(&self, new_metadata: &TableMetadata) -> Self {
        Self {
            table_location: self.table_location.clone(),
            version: self.version,
            id: self.id,
            compression_codec: Self::compression_from_properties(new_metadata.properties()),
        }
    }

    /// Returns the compression codec used for this metadata location.
    pub fn compression_codec(&self) -> CompressionCodec {
        self.compression_codec
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
    /// Parse errors for compression codec result in CompressionCodec::None.
    fn parse_file_name(file_name: &str) -> Result<(i32, Uuid, CompressionCodec)> {
        let stripped = file_name.strip_suffix(".metadata.json").ok_or(Error::new(
            ErrorKind::Unexpected,
            format!("Invalid metadata file ending: {file_name}"),
        ))?;

        // Check for compression suffix (e.g., .gz)
        let gzip_suffix = CompressionCodec::Gzip.suffix()?;
        let (stripped, compression_codec) = if let Some(s) = stripped.strip_suffix(gzip_suffix) {
            (s, CompressionCodec::Gzip)
        } else {
            (stripped, CompressionCodec::None)
        };

        let (version, id) = stripped.split_once('-').ok_or(Error::new(
            ErrorKind::Unexpected,
            format!("Invalid metadata file name format: {file_name}"),
        ))?;

        Ok((
            version.parse::<i32>()?,
            Uuid::parse_str(id)?,
            compression_codec,
        ))
    }
}

impl Display for MetadataLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let suffix = self.compression_codec.suffix().unwrap_or("");
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
        let (version, id, compression_codec) = Self::parse_file_name(file_name)?;

        Ok(MetadataLocation {
            table_location: prefix,
            version,
            id,
            compression_codec,
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
                    compression_codec: CompressionCodec::None,
                }),
            ),
            // Some prefix
            (
                "/abc/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
                Ok(MetadataLocation {
                    table_location: "/abc".to_string(),
                    version: 1234567,
                    id: Uuid::from_str("2cd22b57-5127-4198-92ba-e4e67c79821b").unwrap(),
                    compression_codec: CompressionCodec::None,
                }),
            ),
            // Longer prefix
            (
                "/abc/def/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
                Ok(MetadataLocation {
                    table_location: "/abc/def".to_string(),
                    version: 1234567,
                    id: Uuid::from_str("2cd22b57-5127-4198-92ba-e4e67c79821b").unwrap(),
                    compression_codec: CompressionCodec::None,
                }),
            ),
            // Prefix with special characters
            (
                "https://127.0.0.1/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
                Ok(MetadataLocation {
                    table_location: "https://127.0.0.1".to_string(),
                    version: 1234567,
                    id: Uuid::from_str("2cd22b57-5127-4198-92ba-e4e67c79821b").unwrap(),
                    compression_codec: CompressionCodec::None,
                }),
            ),
            // Another id
            (
                "/abc/metadata/1234567-81056704-ce5b-41c4-bb83-eb6408081af6.metadata.json",
                Ok(MetadataLocation {
                    table_location: "/abc".to_string(),
                    version: 1234567,
                    id: Uuid::from_str("81056704-ce5b-41c4-bb83-eb6408081af6").unwrap(),
                    compression_codec: CompressionCodec::None,
                }),
            ),
            // Version 0
            (
                "/abc/metadata/00000-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
                Ok(MetadataLocation {
                    table_location: "/abc".to_string(),
                    version: 0,
                    id: Uuid::from_str("2cd22b57-5127-4198-92ba-e4e67c79821b").unwrap(),
                    compression_codec: CompressionCodec::None,
                }),
            ),
            // With gzip compression
            (
                "/abc/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.gz.metadata.json",
                Ok(MetadataLocation {
                    table_location: "/abc".to_string(),
                    version: 1234567,
                    id: Uuid::from_str("2cd22b57-5127-4198-92ba-e4e67c79821b").unwrap(),
                    compression_codec: CompressionCodec::Gzip,
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
    fn test_metadata_location_with_next_version() {
        let metadata = create_test_metadata(HashMap::new());
        let test_cases = vec![
            MetadataLocation::new_with_metadata("/abc", &metadata),
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
    fn test_with_next_version_preserves_compression() {
        // Start from a parsed location with no compression
        let location_none = MetadataLocation::from_str(
            "/test/table/metadata/00000-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
        )
        .unwrap();
        assert_eq!(location_none.compression_codec, CompressionCodec::None);

        let next_none = location_none.with_next_version();
        assert_eq!(next_none.compression_codec, CompressionCodec::None);
        assert_eq!(next_none.version, 1);

        // Start from a parsed location with gzip compression
        let location_gzip = MetadataLocation::from_str(
            "/test/table/metadata/00005-81056704-ce5b-41c4-bb83-eb6408081af6.gz.metadata.json",
        )
        .unwrap();
        assert_eq!(location_gzip.compression_codec, CompressionCodec::Gzip);

        let next_gzip = location_gzip.with_next_version();
        assert_eq!(next_gzip.compression_codec, CompressionCodec::Gzip);
        assert_eq!(next_gzip.version, 6);
    }

    #[test]
    fn test_with_new_metadata_updates_compression() {
        // Start from a parsed location with no compression
        let location = MetadataLocation::from_str(
            "/test/table/metadata/00000-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
        )
        .unwrap();
        assert_eq!(location.compression_codec, CompressionCodec::None);

        // Update to gzip compression
        let mut props_gzip = HashMap::new();
        props_gzip.insert(
            "write.metadata.compression-codec".to_string(),
            "gzip".to_string(),
        );
        let metadata_gzip = create_test_metadata(props_gzip);
        let updated_gzip = location.with_new_metadata(&metadata_gzip);
        assert_eq!(updated_gzip.compression_codec, CompressionCodec::Gzip);
        assert_eq!(updated_gzip.version, 0);
        assert_eq!(
            updated_gzip.to_string(),
            "/test/table/metadata/00000-2cd22b57-5127-4198-92ba-e4e67c79821b.gz.metadata.json"
        );

        // Update back to no compression
        let props_none = HashMap::new();
        let metadata_none = create_test_metadata(props_none);
        let updated_none = updated_gzip.with_new_metadata(&metadata_none);
        assert_eq!(updated_none.compression_codec, CompressionCodec::None);
        assert_eq!(updated_none.version, 0);
        assert_eq!(
            updated_none.to_string(),
            "/test/table/metadata/00000-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json"
        );

        // Test explicit "none" codec
        let mut props_explicit_none = HashMap::new();
        props_explicit_none.insert(
            "write.metadata.compression-codec".to_string(),
            "none".to_string(),
        );
        let metadata_explicit_none = create_test_metadata(props_explicit_none);
        let updated_explicit = updated_gzip.with_new_metadata(&metadata_explicit_none);
        assert_eq!(updated_explicit.compression_codec, CompressionCodec::None);
        assert_eq!(
            updated_explicit.to_string(),
            "/test/table/metadata/00000-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json"
        );
    }
}
