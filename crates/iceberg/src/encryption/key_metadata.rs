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
use std::io::Cursor;
use std::sync::LazyLock;

use apache_avro::{Schema as AvroSchema, from_avro_datum, from_value, to_avro_datum, to_value};
use serde::{Deserialize, Serialize};

use super::SensitiveBytes;
use crate::{Error, ErrorKind, Result};

const V1: u8 = 1;

/// Avro schema for StandardKeyMetadata V1, matching Java's layout.
static AVRO_SCHEMA_V1: LazyLock<AvroSchema> = LazyLock::new(|| {
    AvroSchema::parse_str(
        r#"{
            "type": "record",
            "name": "StandardKeyMetadata",
            "namespace": "org.apache.iceberg.encryption",
            "fields": [
                {
                    "name": "encryption_key",
                    "type": "bytes",
                    "field-id": 0
                },
                {
                    "name": "aad_prefix",
                    "type": ["null", "bytes"],
                    "default": null,
                    "field-id": 1
                },
                {
                    "name": "file_length",
                    "type": ["null", "long"],
                    "default": null,
                    "field-id": 2
                }
            ]
        }"#,
    )
    .expect("Failed to parse StandardKeyMetadata Avro schema")
});

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
    aad_prefix: Box<[u8]>,
    file_length: Option<i64>,
}

impl fmt::Debug for StandardKeyMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StandardKeyMetadata")
            .field("encryption_key", &self.encryption_key)
            .field("aad_prefix", &format!("[{} bytes]", self.aad_prefix.len()))
            .field("file_length", &self.file_length)
            .finish()
    }
}

impl StandardKeyMetadata {
    /// Creates a new `StandardKeyMetadata`.
    pub fn new(encryption_key: &[u8], aad_prefix: &[u8]) -> Self {
        Self {
            encryption_key: SensitiveBytes::new(encryption_key),
            aad_prefix: aad_prefix.into(),
            file_length: None,
        }
    }

    /// Returns the plaintext Data Encryption Key.
    pub fn encryption_key(&self) -> &[u8] {
        self.encryption_key.as_bytes()
    }

    /// Returns the AAD prefix.
    pub fn aad_prefix(&self) -> &[u8] {
        &self.aad_prefix
    }

    /// Returns the optional file length.
    pub fn file_length(&self) -> Option<i64> {
        self.file_length
    }

    /// Serializes to Java-compatible format: `[0x01] [Avro binary datum]`
    pub fn serialize(&self) -> Result<Box<[u8]>> {
        let serde_repr = StandardKeyMetadataV1 {
            encryption_key: serde_bytes::ByteBuf::from(self.encryption_key.as_bytes()),
            aad_prefix: Some(serde_bytes::ByteBuf::from(self.aad_prefix.as_ref())),
            file_length: self.file_length,
        };

        let value = to_value(serde_repr)
            .and_then(|v| v.resolve(&AVRO_SCHEMA_V1))
            .map_err(|e| {
                Error::new(ErrorKind::Unexpected, "Failed to serialize key metadata").with_source(e)
            })?;

        let datum = to_avro_datum(&AVRO_SCHEMA_V1, value).map_err(|e| {
            Error::new(ErrorKind::Unexpected, "Failed to serialize key metadata").with_source(e)
        })?;

        let mut result = Vec::with_capacity(1 + datum.len());
        result.push(V1);
        result.extend_from_slice(&datum);
        Ok(result.into_boxed_slice())
    }

    /// Deserializes from Java-compatible format.
    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
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
            Error::new(ErrorKind::DataInvalid, "Failed to parse key metadata").with_source(e)
        })?;

        let v1: StandardKeyMetadataV1 = from_value(&value).map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                "Failed to deserialize key metadata fields",
            )
            .with_source(e)
        })?;

        Ok(Self {
            encryption_key: SensitiveBytes::new(v1.encryption_key.into_vec()),
            aad_prefix: v1
                .aad_prefix
                .map(|b| b.into_vec().into_boxed_slice())
                .unwrap_or_default(),
            file_length: v1.file_length,
        })
    }
}

/// Serde struct for Avro serialization of [`StandardKeyMetadata`] V1.
/// Field names must match [`AVRO_SCHEMA_V1`] exactly.
#[derive(Serialize, Deserialize)]
struct StandardKeyMetadataV1 {
    encryption_key: serde_bytes::ByteBuf,
    aad_prefix: Option<serde_bytes::ByteBuf>,
    file_length: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip() {
        let key = b"0123456789012345";
        let aad = b"1234567890123456";

        let metadata = StandardKeyMetadata::new(key, aad);
        let serialized = metadata.serialize().unwrap();
        let parsed = StandardKeyMetadata::deserialize(&serialized).unwrap();

        assert_eq!(parsed.encryption_key(), key);
        assert_eq!(parsed.aad_prefix(), aad);
        assert_eq!(parsed.file_length(), None);
    }

    #[test]
    fn test_unsupported_version() {
        let result = StandardKeyMetadata::deserialize(&[0x02]);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::FeatureUnsupported);
    }

    #[test]
    fn test_empty_buffer() {
        let result = StandardKeyMetadata::deserialize(&[]);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::DataInvalid);
    }

    #[test]
    fn test_roundtrip_with_empty_aad() {
        let metadata = StandardKeyMetadata::new(&[1, 2, 3, 4], &[]);
        let serialized = metadata.serialize().unwrap();
        let parsed = StandardKeyMetadata::deserialize(&serialized).unwrap();

        assert_eq!(parsed.encryption_key(), &[1, 2, 3, 4]);
        assert_eq!(parsed.aad_prefix(), &[] as &[u8]);
    }
}
