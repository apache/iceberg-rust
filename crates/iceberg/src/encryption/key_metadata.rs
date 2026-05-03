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

//! Avro-serialized key metadata format compatible with Java's
//! `org.apache.iceberg.encryption.StandardKeyMetadata`.

use std::fmt;

use super::SensitiveBytes;
use crate::{Error, ErrorKind, Result};

/// Standard key metadata for Iceberg table encryption.
///
/// Contains the Data Encryption Key (DEK), AAD prefix, and optional file
/// length. Byte-compatible with Java's `StandardKeyMetadata` via Avro
/// serialization.
///
/// Wire format: `[version byte (0x01)] [Avro binary datum]`
#[derive(Clone, PartialEq, Eq)]
pub struct StandardKeyMetadata {
    encryption_key: SensitiveBytes,
    aad_prefix: Option<Box<[u8]>>,
    file_length: Option<u64>,
}

impl fmt::Debug for StandardKeyMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StandardKeyMetadata")
            .field("encryption_key", &self.encryption_key)
            .field(
                "aad_prefix",
                &self
                    .aad_prefix
                    .as_ref()
                    .map(|b| format!("[{} bytes]", b.len())),
            )
            .field("file_length", &self.file_length)
            .finish()
    }
}

impl StandardKeyMetadata {
    /// Creates a new `StandardKeyMetadata`.
    pub fn new(encryption_key: &[u8]) -> Self {
        Self {
            encryption_key: SensitiveBytes::new(encryption_key),
            aad_prefix: None,
            file_length: None,
        }
    }

    /// Adds an AAD prefix.
    pub fn with_aad_prefix(mut self, aad_prefix: &[u8]) -> Self {
        self.aad_prefix = Some(aad_prefix.into());
        self
    }

    /// Adds a file length.
    pub fn with_file_length(mut self, length: u64) -> Self {
        self.file_length = Some(length);
        self
    }

    /// Returns the plaintext Data Encryption Key.
    pub fn encryption_key(&self) -> &SensitiveBytes {
        &self.encryption_key
    }

    /// Returns the AAD prefix.
    pub fn aad_prefix(&self) -> Option<&[u8]> {
        self.aad_prefix.as_deref()
    }

    /// Returns the optional file length.
    pub fn file_length(&self) -> Option<u64> {
        self.file_length
    }

    /// Encodes to Java-compatible format: `[0x01] [Avro binary datum]`
    pub fn encode(&self) -> Result<Box<[u8]>> {
        _serde::StandardKeyMetadataV1::from(self).encode()
    }

    /// Decodes from Java-compatible format.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        _serde::StandardKeyMetadataV1::decode(bytes).map(Self::from)
    }
}

mod _serde {
    use std::io::Cursor;
    use std::sync::{Arc, LazyLock};

    use apache_avro::{Schema as AvroSchema, from_avro_datum, from_value, to_avro_datum, to_value};
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::avro::schema_to_avro_schema;
    use crate::spec::{NestedField, PrimitiveType, Schema, Type};

    pub(super) const V1: u8 = 1;

    /// Avro schema for StandardKeyMetadata V1, derived from Iceberg schema.
    pub(super) static AVRO_SCHEMA_V1: LazyLock<AvroSchema> = LazyLock::new(|| {
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    0,
                    "encryption_key",
                    Type::Primitive(PrimitiveType::Binary),
                )),
                Arc::new(NestedField::optional(
                    1,
                    "aad_prefix",
                    Type::Primitive(PrimitiveType::Binary),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "file_length",
                    Type::Primitive(PrimitiveType::Long),
                )),
            ])
            .build()
            .expect("Failed to build StandardKeyMetadata Iceberg schema");

        schema_to_avro_schema("StandardKeyMetadata", &schema)
            .expect("Failed to convert StandardKeyMetadata to Avro schema")
    });

    /// Serde struct for Avro serialization of [`StandardKeyMetadata`] V1.
    /// Field names must match [`AVRO_SCHEMA_V1`] exactly.
    #[derive(Serialize, Deserialize)]
    pub(super) struct StandardKeyMetadataV1 {
        pub encryption_key: serde_bytes::ByteBuf,
        pub aad_prefix: Option<serde_bytes::ByteBuf>,
        pub file_length: Option<u64>,
    }

    impl StandardKeyMetadataV1 {
        pub(super) fn encode(&self) -> Result<Box<[u8]>> {
            let value = to_value(self)
                .and_then(|v| v.resolve(&AVRO_SCHEMA_V1))
                .map_err(|e| {
                    Error::new(ErrorKind::Unexpected, "Failed to encode key metadata")
                        .with_source(e)
                })?;

            let datum = to_avro_datum(&AVRO_SCHEMA_V1, value).map_err(|e| {
                Error::new(ErrorKind::Unexpected, "Failed to encode key metadata").with_source(e)
            })?;

            let mut result = Vec::with_capacity(1 + datum.len());
            result.push(V1);
            result.extend_from_slice(&datum);
            Ok(result.into_boxed_slice())
        }

        pub(super) fn decode(bytes: &[u8]) -> Result<Self> {
            if bytes.is_empty() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Empty key metadata buffer",
                ));
            }

            let version = bytes[0];
            if version != V1 {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    format!("Cannot resolve schema for version: {version}"),
                ));
            }

            let mut reader = Cursor::new(&bytes[1..]);
            let value = from_avro_datum(&AVRO_SCHEMA_V1, &mut reader, None).map_err(|e| {
                Error::new(ErrorKind::DataInvalid, "Failed to decode key metadata").with_source(e)
            })?;

            from_value(&value).map_err(|e| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Failed to decode key metadata fields",
                )
                .with_source(e)
            })
        }
    }

    impl From<&StandardKeyMetadata> for StandardKeyMetadataV1 {
        fn from(metadata: &StandardKeyMetadata) -> Self {
            Self {
                encryption_key: serde_bytes::ByteBuf::from(metadata.encryption_key.as_bytes()),
                aad_prefix: metadata
                    .aad_prefix
                    .as_ref()
                    .map(|b| serde_bytes::ByteBuf::from(b.as_ref())),
                file_length: metadata.file_length,
            }
        }
    }

    impl From<StandardKeyMetadataV1> for StandardKeyMetadata {
        fn from(v1: StandardKeyMetadataV1) -> Self {
            Self {
                encryption_key: SensitiveBytes::new(v1.encryption_key.into_vec()),
                aad_prefix: v1.aad_prefix.map(|b| b.into_vec().into_boxed_slice()),
                file_length: v1.file_length,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip() {
        let key = b"0123456789012345";
        let aad = b"1234567890123456";

        let metadata = StandardKeyMetadata::new(key).with_aad_prefix(aad);
        let serialized = metadata.encode().unwrap();
        let parsed = StandardKeyMetadata::decode(&serialized).unwrap();

        assert_eq!(parsed.encryption_key().as_bytes(), key);
        assert_eq!(parsed.aad_prefix(), Some(aad.as_slice()));
        assert_eq!(parsed.file_length(), None);
    }

    #[test]
    fn test_roundtrip_with_length() {
        let key = b"0123456789012345";
        let aad = b"1234567890123456";

        let file_length = 100_000;
        let metadata = StandardKeyMetadata::new(key)
            .with_aad_prefix(aad)
            .with_file_length(file_length);
        let serialized = metadata.encode().unwrap();
        let parsed = StandardKeyMetadata::decode(&serialized).unwrap();

        assert_eq!(parsed.encryption_key().as_bytes(), key);
        assert_eq!(parsed.aad_prefix(), Some(aad.as_slice()));
        assert_eq!(parsed.file_length(), Some(file_length));
    }

    #[test]
    fn test_unsupported_version() {
        let result = StandardKeyMetadata::decode(&[0x02]);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::FeatureUnsupported);
    }

    #[test]
    fn test_empty_buffer() {
        let result = StandardKeyMetadata::decode(&[]);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::DataInvalid);
    }

    #[test]
    fn test_roundtrip_without_aad() {
        let metadata = StandardKeyMetadata::new(&[1, 2, 3, 4]);
        let serialized = metadata.encode().unwrap();
        let parsed = StandardKeyMetadata::decode(&serialized).unwrap();

        assert_eq!(parsed.encryption_key().as_bytes(), &[1, 2, 3, 4]);
        assert_eq!(parsed.aad_prefix(), None);
    }
}
