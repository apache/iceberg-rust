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

//! Key metadata serialization for Iceberg encryption.
//!
//! This module provides structures and traits for managing encryption key metadata,
//! including Avro-based serialization for Java compatibility.

use crate::{Error, ErrorKind, Result};

/// Trait for accessing encryption key metadata.
///
/// This trait provides access to the encryption key, AAD prefix, and optional file length
/// used for both stream encryption (manifests) and native Parquet encryption.
pub trait EncryptionKeyMetadata {
    /// Get the encryption key (DEK) bytes.
    fn encryption_key(&self) -> &[u8];

    /// Get the AAD (Additional Authenticated Data) prefix.
    fn aad_prefix(&self) -> &[u8];

    /// Get the file length for integrity validation (optional).
    fn file_length(&self) -> Option<i64>;
}

/// Standard implementation of encryption key metadata.
///
/// This structure stores the wrapped encryption key (DEK), AAD prefix, and optional
/// file length. It can be serialized to and from Avro format for compatibility with
/// Java Iceberg implementations.
///
/// The Avro schema is:
/// ```json
/// {
///   "type": "record",
///   "name": "StandardKeyMetadata",
///   "fields": [
///     {"name": "encryption_key", "type": "bytes"},
///     {"name": "aad_prefix", "type": "bytes"},
///     {"name": "file_length", "type": ["null", "long"], "default": null}
///   ]
/// }
/// ```
#[derive(Debug, Clone)]
pub struct StandardKeyMetadata {
    /// Wrapped encryption key from KMS
    encryption_key: Vec<u8>,
    /// AAD prefix for GCM authentication
    aad_prefix: Vec<u8>,
    /// Optional file length for validation
    file_length: Option<i64>,
}

impl StandardKeyMetadata {
    /// Creates a new StandardKeyMetadata instance.
    pub fn new(encryption_key: Vec<u8>, aad_prefix: Vec<u8>, file_length: Option<i64>) -> Self {
        Self {
            encryption_key,
            aad_prefix,
            file_length,
        }
    }

    /// Serializes the metadata to Avro bytes format.
    ///
    /// The serialization uses single-object encoding (with schema fingerprint)
    /// to ensure Java compatibility.
    pub fn serialize(&self) -> Result<Vec<u8>> {
        use apache_avro::types::Value;
        use apache_avro::{Schema, to_avro_datum};

        let schema_str = r#"{
            "type": "record",
            "name": "StandardKeyMetadata",
            "fields": [
                {"name": "encryption_key", "type": "bytes"},
                {"name": "aad_prefix", "type": "bytes"},
                {"name": "file_length", "type": ["null", "long"], "default": null}
            ]
        }"#;

        let schema = Schema::parse_str(schema_str).map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                "Failed to parse StandardKeyMetadata schema",
            )
            .with_source(e)
        })?;

        let mut fields = vec![
            (
                "encryption_key".to_string(),
                Value::Bytes(self.encryption_key.clone()),
            ),
            (
                "aad_prefix".to_string(),
                Value::Bytes(self.aad_prefix.clone()),
            ),
        ];

        let file_length_value = match self.file_length {
            Some(len) => Value::Union(1, Box::new(Value::Long(len))),
            None => Value::Union(0, Box::new(Value::Null)),
        };
        fields.push(("file_length".to_string(), file_length_value));

        let record = Value::Record(fields);

        to_avro_datum(&schema, record).map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                "Failed to serialize StandardKeyMetadata",
            )
            .with_source(e)
        })
    }

    /// Deserializes metadata from Avro bytes format.
    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        use apache_avro::{Schema, from_avro_datum};

        let schema_str = r#"{
            "type": "record",
            "name": "StandardKeyMetadata",
            "fields": [
                {"name": "encryption_key", "type": "bytes"},
                {"name": "aad_prefix", "type": "bytes"},
                {"name": "file_length", "type": ["null", "long"], "default": null}
            ]
        }"#;

        let schema = Schema::parse_str(schema_str).map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                "Failed to parse StandardKeyMetadata schema",
            )
            .with_source(e)
        })?;

        let value = from_avro_datum(&schema, &mut &bytes[..], None).map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                "Failed to deserialize StandardKeyMetadata",
            )
            .with_source(e)
        })?;

        match value {
            apache_avro::types::Value::Record(fields) => {
                let mut encryption_key = None;
                let mut aad_prefix = None;
                let mut file_length = None;

                for (name, value) in fields {
                    match name.as_str() {
                        "encryption_key" => {
                            if let apache_avro::types::Value::Bytes(bytes) = value {
                                encryption_key = Some(bytes);
                            }
                        }
                        "aad_prefix" => {
                            if let apache_avro::types::Value::Bytes(bytes) = value {
                                aad_prefix = Some(bytes);
                            }
                        }
                        "file_length" => {
                            if let apache_avro::types::Value::Union(_, boxed) = value {
                                if let apache_avro::types::Value::Long(len) = *boxed {
                                    file_length = Some(len);
                                }
                            }
                        }
                        _ => {}
                    }
                }

                let encryption_key = encryption_key.ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Missing encryption_key field in StandardKeyMetadata",
                    )
                })?;

                let aad_prefix = aad_prefix.ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Missing aad_prefix field in StandardKeyMetadata",
                    )
                })?;

                Ok(StandardKeyMetadata {
                    encryption_key,
                    aad_prefix,
                    file_length,
                })
            }
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                "Invalid StandardKeyMetadata format: expected Record",
            )),
        }
    }
}

impl EncryptionKeyMetadata for StandardKeyMetadata {
    fn encryption_key(&self) -> &[u8] {
        &self.encryption_key
    }

    fn aad_prefix(&self) -> &[u8] {
        &self.aad_prefix
    }

    fn file_length(&self) -> Option<i64> {
        self.file_length
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_standard_key_metadata_round_trip() {
        let encryption_key = b"wrapped_dek_from_kms_12345678".to_vec();
        let aad_prefix = b"iceberg_aad_prefix_".to_vec();
        let file_length = Some(1024i64);

        let metadata =
            StandardKeyMetadata::new(encryption_key.clone(), aad_prefix.clone(), file_length);

        let serialized = metadata.serialize().unwrap();
        let deserialized = StandardKeyMetadata::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.encryption_key(), &encryption_key[..]);
        assert_eq!(deserialized.aad_prefix(), &aad_prefix[..]);
        assert_eq!(deserialized.file_length(), file_length);
    }

    #[test]
    fn test_standard_key_metadata_without_file_length() {
        let encryption_key = b"wrapped_key".to_vec();
        let aad_prefix = b"aad".to_vec();

        let metadata = StandardKeyMetadata::new(encryption_key.clone(), aad_prefix.clone(), None);

        let serialized = metadata.serialize().unwrap();
        let deserialized = StandardKeyMetadata::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.encryption_key(), &encryption_key[..]);
        assert_eq!(deserialized.aad_prefix(), &aad_prefix[..]);
        assert_eq!(deserialized.file_length(), None);
    }

    #[test]
    fn test_encryption_key_metadata_trait() {
        let metadata = StandardKeyMetadata::new(b"key".to_vec(), b"aad".to_vec(), Some(2048));

        let trait_obj: &dyn EncryptionKeyMetadata = &metadata;
        assert_eq!(trait_obj.encryption_key(), b"key");
        assert_eq!(trait_obj.aad_prefix(), b"aad");
        assert_eq!(trait_obj.file_length(), Some(2048));
    }

    #[test]
    fn test_invalid_deserialization() {
        let invalid_bytes = b"not valid avro data";
        let result = StandardKeyMetadata::deserialize(invalid_bytes);
        assert!(result.is_err());
    }
}
