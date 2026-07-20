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

/// Default folder name for metadata files under the table location, used when the
/// `write.metadata.path` table property is not set.
pub(crate) const METADATA_FOLDER_NAME: &str = "metadata";

/// Helper for parsing a location of the format: `<metadata-dir>/<version>-<uuid>.metadata.json`
/// or with compression: `<metadata-dir>/<version>-<uuid>.gz.metadata.json`
///
/// `<metadata-dir>` is set to the `write.metadata.path` table property and
/// it defaults to the `<location>/metadata` when the property is not set.
#[derive(Clone, Debug, PartialEq)]
pub struct MetadataLocation {
    location: String,
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

    /// Creates a completely new metadata location starting at version 0,
    /// with compression settings from the table metadata.
    /// Only used for creating a new table. For updates, see `next_version`.
    ///
    /// The metadata directory honors the `write.metadata.path` table property when set,
    /// otherwise defaults to the `metadata` subdirectory of `table_location`.
    pub fn try_new_with_metadata(
        table_location: impl ToString,
        metadata: &TableMetadata,
    ) -> Result<Self> {
        let table_location = table_location.to_string();
        let location = metadata
            .table_properties()?
            .write_metadata_path
            .unwrap_or_else(|| format!("{table_location}/{METADATA_FOLDER_NAME}"));
        Ok(Self {
            location,
            version: 0,
            id: Uuid::new_v4(),
            compression_codec: Self::compression_from_properties(metadata.properties()),
        })
    }

    /// Creates a new metadata location for an updated metadata file.
    /// Increments the version number and generates a new UUID.
    pub fn with_next_version(&self) -> Self {
        Self {
            location: self.location.clone(),
            version: self.version + 1,
            id: Uuid::new_v4(),
            compression_codec: self.compression_codec,
        }
    }

    /// Updates the metadata location with the metadata directory
    /// and compression settings from the new metadata.
    pub fn try_with_new_metadata(&self, new_metadata: &TableMetadata) -> Result<Self> {
        Ok(Self {
            location: new_metadata.metadata_location()?,
            version: self.version,
            id: self.id,
            compression_codec: Self::compression_from_properties(new_metadata.properties()),
        })
    }

    /// Returns the compression codec used for this metadata location.
    pub fn compression_codec(&self) -> CompressionCodec {
        self.compression_codec
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
        let gzip_suffix = CompressionCodec::gzip_default().suffix()?;
        let (stripped, compression_codec) = if let Some(s) = stripped.strip_suffix(gzip_suffix) {
            (s, CompressionCodec::gzip_default())
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
            "{}/{:0>5}-{}{}.metadata.json",
            self.location, self.version, self.id, suffix
        )
    }
}

impl FromStr for MetadataLocation {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let (location, file_name) = s.rsplit_once('/').ok_or(Error::new(
            ErrorKind::Unexpected,
            format!("Invalid metadata location: {s}"),
        ))?;

        let (version, id, compression_codec) = Self::parse_file_name(file_name)?;

        Ok(MetadataLocation {
            location: location.to_string(),
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
    use crate::spec::{Schema, TableMetadata, TableMetadataBuilder, TableProperties};
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
                    location: "/metadata".to_string(),
                    version: 1234567,
                    id: Uuid::from_str("2cd22b57-5127-4198-92ba-e4e67c79821b").unwrap(),
                    compression_codec: CompressionCodec::None,
                }),
            ),
            // Some prefix
            (
                "/abc/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
                Ok(MetadataLocation {
                    location: "/abc/metadata".to_string(),
                    version: 1234567,
                    id: Uuid::from_str("2cd22b57-5127-4198-92ba-e4e67c79821b").unwrap(),
                    compression_codec: CompressionCodec::None,
                }),
            ),
            // Longer prefix
            (
                "/abc/def/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
                Ok(MetadataLocation {
                    location: "/abc/def/metadata".to_string(),
                    version: 1234567,
                    id: Uuid::from_str("2cd22b57-5127-4198-92ba-e4e67c79821b").unwrap(),
                    compression_codec: CompressionCodec::None,
                }),
            ),
            // Prefix with special characters
            (
                "https://127.0.0.1/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
                Ok(MetadataLocation {
                    location: "https://127.0.0.1/metadata".to_string(),
                    version: 1234567,
                    id: Uuid::from_str("2cd22b57-5127-4198-92ba-e4e67c79821b").unwrap(),
                    compression_codec: CompressionCodec::None,
                }),
            ),
            // Another id
            (
                "/abc/metadata/1234567-81056704-ce5b-41c4-bb83-eb6408081af6.metadata.json",
                Ok(MetadataLocation {
                    location: "/abc/metadata".to_string(),
                    version: 1234567,
                    id: Uuid::from_str("81056704-ce5b-41c4-bb83-eb6408081af6").unwrap(),
                    compression_codec: CompressionCodec::None,
                }),
            ),
            // Version 0
            (
                "/abc/metadata/00000-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
                Ok(MetadataLocation {
                    location: "/abc/metadata".to_string(),
                    version: 0,
                    id: Uuid::from_str("2cd22b57-5127-4198-92ba-e4e67c79821b").unwrap(),
                    compression_codec: CompressionCodec::None,
                }),
            ),
            // With gzip compression
            (
                "/abc/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.gz.metadata.json",
                Ok(MetadataLocation {
                    location: "/abc/metadata".to_string(),
                    version: 1234567,
                    id: Uuid::from_str("2cd22b57-5127-4198-92ba-e4e67c79821b").unwrap(),
                    compression_codec: CompressionCodec::gzip_default(),
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
            // Metadata dir does not need to be named "metadata" (e.g. a `write.metadata.path`` location)
            (
                "/wrongsubdir/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
                Ok(MetadataLocation {
                    location: "/wrongsubdir".to_string(),
                    version: 1234567,
                    id: Uuid::from_str("2cd22b57-5127-4198-92ba-e4e67c79821b").unwrap(),
                    compression_codec: CompressionCodec::None,
                }),
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
            MetadataLocation::try_new_with_metadata("/abc", &metadata).unwrap(),
            MetadataLocation::from_str(
                "/abc/def/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
            )
            .unwrap(),
        ];

        for input in test_cases {
            let next = MetadataLocation::from_str(&input.to_string())
                .unwrap()
                .with_next_version();
            assert_eq!(next.location, input.location);
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
        assert_eq!(
            location_gzip.compression_codec,
            CompressionCodec::gzip_default()
        );

        let next_gzip = location_gzip.with_next_version();
        assert_eq!(
            next_gzip.compression_codec,
            CompressionCodec::gzip_default()
        );
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
        let updated_gzip = location.try_with_new_metadata(&metadata_gzip).unwrap();
        assert_eq!(
            updated_gzip.compression_codec,
            CompressionCodec::gzip_default()
        );
        assert_eq!(updated_gzip.version, 0);
        assert_eq!(
            updated_gzip.to_string(),
            "/test/table/metadata/00000-2cd22b57-5127-4198-92ba-e4e67c79821b.gz.metadata.json"
        );

        // Update back to no compression
        let props_none = HashMap::new();
        let metadata_none = create_test_metadata(props_none);
        let updated_none = updated_gzip.try_with_new_metadata(&metadata_none).unwrap();
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
        let updated_explicit = updated_gzip
            .try_with_new_metadata(&metadata_explicit_none)
            .unwrap();
        assert_eq!(updated_explicit.compression_codec, CompressionCodec::None);
        assert_eq!(
            updated_explicit.to_string(),
            "/test/table/metadata/00000-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json"
        );
    }

    #[test]
    fn test_with_new_metadata_re_derives_location() {
        // Start from a parsed location under some existing metadata directory
        let location = MetadataLocation::from_str(
            "/old/table/metadata/00003-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
        )
        .unwrap();

        // A new metadata (no `write.metadata.path`) re-derives to `<location>/metadata`,
        // preserving the version and id of the existing location
        let relocated = create_test_metadata(HashMap::new());
        let updated = location.try_with_new_metadata(&relocated).unwrap();
        assert!(
            updated.to_string().starts_with("/test/table/metadata/"),
            "unexpected location: {updated}"
        );
        assert_eq!(updated.version, location.version);
        assert_eq!(updated.id, location.id);

        // A configured `write.metadata.path` is honored on updates too
        let props = HashMap::from([(
            TableProperties::PROPERTY_WRITE_METADATA_PATH.to_string(),
            "s3://bucket/custom-meta".to_string(),
        )]);
        let with_meta_path = create_test_metadata(props);
        let updated = location.try_with_new_metadata(&with_meta_path).unwrap();
        assert!(
            updated.to_string().starts_with("s3://bucket/custom-meta/"),
            "unexpected location: {updated}"
        );
    }

    #[test]
    fn test_new_with_metadata_honors_write_metadata_path() {
        // Test metadata lives under `<location>/metadata` by default
        let default_meta = create_test_metadata(HashMap::new());
        let default_loc =
            MetadataLocation::try_new_with_metadata("/test/table", &default_meta).unwrap();
        assert!(
            default_loc
                .to_string()
                .starts_with("/test/table/metadata/00000-"),
            "unexpected location: {default_loc}"
        );

        // Test a configured `write.metadata.path` is honored
        let props = HashMap::from([(
            TableProperties::PROPERTY_WRITE_METADATA_PATH.to_string(),
            "s3://bucket/custom-meta".to_string(),
        )]);
        let custom_meta = create_test_metadata(props);
        let custom_loc =
            MetadataLocation::try_new_with_metadata("/test/table", &custom_meta).unwrap();
        assert!(
            custom_loc
                .to_string()
                .starts_with("s3://bucket/custom-meta/00000-"),
            "unexpected location: {custom_loc}"
        );
        assert!(custom_loc.to_string().ends_with(".metadata.json"));
    }
}
