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

use super::SecureKey;
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
    encryption_key: SecureKey,
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
    /// Creates a new `StandardKeyMetadata` from raw key bytes.
    pub fn try_new(encryption_key: &[u8]) -> Result<Self> {
        Ok(Self::from(SecureKey::new(encryption_key)?))
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
    pub fn encryption_key(&self) -> &SecureKey {
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
        _serde::StandardKeyMetadataV1::decode(bytes).and_then(Self::try_from)
    }
}

impl From<SecureKey> for StandardKeyMetadata {
    /// Creates a `StandardKeyMetadata` from an already-validated key.
    fn from(encryption_key: SecureKey) -> Self {
        Self {
            encryption_key,
            aad_prefix: None,
            file_length: None,
        }
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

    impl TryFrom<StandardKeyMetadataV1> for StandardKeyMetadata {
        type Error = Error;

        fn try_from(v1: StandardKeyMetadataV1) -> Result<Self> {
            let encryption_key = SecureKey::new(&v1.encryption_key).map_err(|e| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Invalid encryption key in key metadata",
                )
                .with_source(e)
            })?;
            Ok(Self {
                encryption_key,
                aad_prefix: v1.aad_prefix.map(|b| b.into_vec().into_boxed_slice()),
                file_length: v1.file_length,
            })
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

        let metadata = StandardKeyMetadata::try_new(key)
            .unwrap()
            .with_aad_prefix(aad);
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
        let metadata = StandardKeyMetadata::try_new(key)
            .unwrap()
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
        let key = b"0123456789012345";
        let metadata = StandardKeyMetadata::try_new(key).unwrap();
        let serialized = metadata.encode().unwrap();
        let parsed = StandardKeyMetadata::decode(&serialized).unwrap();

        assert_eq!(parsed.encryption_key().as_bytes(), key);
        assert_eq!(parsed.aad_prefix(), None);
    }

    #[test]
    fn test_new_rejects_invalid_key_length() {
        // 24-byte (AES-192) and 32-byte (AES-256) keys are accepted.
        for len in [16usize, 24, 32] {
            assert!(StandardKeyMetadata::try_new(&vec![0u8; len]).is_ok());
        }

        // Invalid lengths are rejected at construction, so an invalid
        // `StandardKeyMetadata` can never exist.
        for len in [0usize, 4, 15, 20, 33] {
            assert!(StandardKeyMetadata::try_new(&vec![0u8; len]).is_err());
        }
    }

    #[test]
    fn test_decode_rejects_invalid_key_length() {
        // Craft wire bytes carrying an invalid-length DEK directly via the
        // serde struct (bypassing the validated public constructors) to prove
        // `decode` still rejects malformed key material off the wire.
        for len in [0usize, 4, 15, 20, 33] {
            let serialized = _serde::StandardKeyMetadataV1 {
                encryption_key: serde_bytes::ByteBuf::from(vec![0u8; len]),
                aad_prefix: None,
                file_length: None,
            }
            .encode()
            .unwrap();

            let err = StandardKeyMetadata::decode(&serialized).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::DataInvalid);
            assert!(
                err.to_string()
                    .contains("Invalid encryption key in key metadata")
            );
        }
    }
}
