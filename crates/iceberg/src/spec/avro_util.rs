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

//! Utilities for working with Apache Avro in Iceberg.

use apache_avro::Codec;
use log::warn;

use crate::spec::TableProperties;

/// Settings for compression codec and level.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompressionSettings {
    /// The compression codec name (e.g., "gzip", "zstd", "snappy", "uncompressed")
    pub codec: String,
    /// The compression level (None uses codec-specific defaults)
    pub level: Option<u8>,
}

impl CompressionSettings {
    /// Create a new CompressionSettings with the specified codec and level.
    pub fn new(codec: String, level: Option<u8>) -> Self {
        Self { codec, level }
    }

    /// Convert to apache_avro::Codec using the codec_from_str helper function.
    pub(crate) fn to_codec(&self) -> Codec {
        codec_from_str(Some(&self.codec), self.level)
    }
}

impl Default for CompressionSettings {
    fn default() -> Self {
        Self {
            codec: TableProperties::PROPERTY_AVRO_COMPRESSION_CODEC_DEFAULT.to_string(),
            level: None,
        }
    }
}

/// Convert codec name and level to apache_avro::Codec.
/// Returns Codec::Null for unknown or unsupported codecs.
///
/// # Arguments
///
/// * `codec` - The name of the compression codec (e.g., "gzip", "zstd", "snappy", "uncompressed")
/// * `level` - The compression level. For gzip/deflate:
///   - 0: NoCompression
///   - 1: BestSpeed
///   - 9: BestCompression
///   - 10: UberCompression
///   - 6: DefaultLevel (balanced speed/compression)
///   - Other values: DefaultLevel
///   For zstd, level is clamped to valid range (0-22).
///   When `None`, uses codec-specific defaults.
///
/// # Supported Codecs
///
/// - `gzip`: Uses Deflate compression with specified level
/// - `zstd`: Uses Zstandard compression (level clamped to valid zstd range 0-22)
/// - `snappy`: Uses Snappy compression (level parameter ignored)
/// - `uncompressed` or `None`: No compression
/// - Any other value: Defaults to no compression (Codec::Null)
pub(crate) fn codec_from_str(codec: Option<&str>, level: Option<u8>) -> Codec {
    use apache_avro::{DeflateSettings, ZstandardSettings};

    // Use case-insensitive comparison to match Java implementation
    match codec.map(|s| s.to_lowercase()).as_deref() {
        Some("gzip") => {
            // Map compression level to miniz_oxide::deflate::CompressionLevel
            // Reference: https://docs.rs/miniz_oxide/latest/miniz_oxide/deflate/enum.CompressionLevel.html
            // Default level for gzip/deflate is 9 (BestCompression) to match Java
            use miniz_oxide::deflate::CompressionLevel;

            let compression_level = match level.unwrap_or(9) {
                0 => CompressionLevel::NoCompression,
                1 => CompressionLevel::BestSpeed,
                9 => CompressionLevel::BestCompression,
                10 => CompressionLevel::UberCompression,
                _ => CompressionLevel::DefaultLevel,
            };

            Codec::Deflate(DeflateSettings::new(compression_level))
        }
        Some("zstd") => {
            // Zstandard supports levels 0-22, clamp to valid range
            // Default level for zstd is 1 to match Java
            let zstd_level = level.unwrap_or(1).min(22);
            Codec::Zstandard(ZstandardSettings::new(zstd_level))
        }
        Some("snappy") => Codec::Snappy,
        Some("uncompressed") | None => Codec::Null,
        Some(unknown) => {
            warn!(
                "Unrecognized compression codec '{}', using no compression (Codec::Null)",
                unknown
            );
            Codec::Null
        }
    }
}

#[cfg(test)]
mod tests {
    use apache_avro::{DeflateSettings, ZstandardSettings};
    use miniz_oxide::deflate::CompressionLevel;

    use super::*;

    #[test]
    fn test_codec_from_str_gzip() {
        // Test with mixed case to verify case-insensitive matching
        let codec = codec_from_str(Some("GZip"), Some(5));
        assert_eq!(
            codec,
            Codec::Deflate(DeflateSettings::new(CompressionLevel::DefaultLevel))
        );
    }

    #[test]
    fn test_codec_from_str_snappy() {
        let codec = codec_from_str(Some("snappy"), None);
        assert_eq!(codec, Codec::Snappy);
    }

    #[test]
    fn test_codec_from_str_zstd() {
        let codec = codec_from_str(Some("zstd"), Some(3));
        assert_eq!(codec, Codec::Zstandard(ZstandardSettings::new(3)));
    }

    #[test]
    fn test_codec_from_str_uncompressed() {
        let codec = codec_from_str(Some("uncompressed"), None);
        assert!(matches!(codec, Codec::Null));
    }

    #[test]
    fn test_codec_from_str_null() {
        let codec = codec_from_str(None, None);
        assert!(matches!(codec, Codec::Null));
    }

    #[test]
    fn test_codec_from_str_unknown() {
        let codec = codec_from_str(Some("unknown"), Some(1));
        assert!(matches!(codec, Codec::Null));
    }

    #[test]
    fn test_codec_from_str_gzip_default_level() {
        // Test that None level defaults to 9 for gzip
        let codec = codec_from_str(Some("gzip"), None);
        assert_eq!(
            codec,
            Codec::Deflate(DeflateSettings::new(CompressionLevel::BestCompression))
        );
    }

    #[test]
    fn test_codec_from_str_zstd_default_level() {
        // Test that None level defaults to 1 for zstd
        let codec = codec_from_str(Some("zstd"), None);
        assert_eq!(codec, Codec::Zstandard(ZstandardSettings::new(1)));
    }

    #[test]
    fn test_codec_from_str_deflate_levels() {
        use std::collections::HashMap;

        use apache_avro::types::Record;
        use apache_avro::{Schema, Writer};

        // Create a simple schema for testing
        let schema = Schema::parse_str(r#"{"type": "record", "name": "test", "fields": [{"name": "field", "type": "string"}]}"#).unwrap();

        // Create test data
        let test_str = "test data that should compress differently at different levels. This is a longer string to ensure compression has something to work with. The quick brown fox jumps over the lazy dog. The quick brown fox jumps over the lazy dog.";

        // Test that different compression levels produce different output sizes
        let mut sizes = HashMap::new();
        for level in [0, 1, 5, 9, 10] {
            let codec = codec_from_str(Some("gzip"), Some(level));
            let mut writer = Writer::with_codec(&schema, Vec::new(), codec);

            let mut record = Record::new(&schema).unwrap();
            record.put("field", test_str);
            writer.append(record).unwrap();

            let encoded = writer.into_inner().unwrap();
            sizes.insert(level, encoded.len());
        }

        // Level 0 (NoCompression) should be largest
        // Level 10 (UberCompression) should be smallest or equal to level 9
        assert!(sizes[&0] >= sizes[&1], "Level 0 should be >= level 1");
        assert!(
            sizes[&1] >= sizes[&9] || sizes[&1] == sizes[&9],
            "Level 1 should be >= level 9"
        );
        assert!(
            sizes[&9] >= sizes[&10] || sizes[&9] == sizes[&10],
            "Level 9 should be >= level 10"
        );
    }

    #[test]
    fn test_codec_from_str_zstd_levels() {
        use apache_avro::types::Record;
        use apache_avro::{Schema, Writer};

        // Create a simple schema for testing
        let schema = Schema::parse_str(r#"{"type": "record", "name": "test", "fields": [{"name": "field", "type": "string"}]}"#).unwrap();
        let test_str = "test data that should compress differently at different levels. This is a longer string to ensure compression has something to work with. The quick brown fox jumps over the lazy dog. The quick brown fox jumps over the lazy dog.";

        // Test various levels by checking they produce valid codecs
        for level in [0, 3, 15, 22] {
            let codec = codec_from_str(Some("zstd"), Some(level));
            assert!(matches!(codec, Codec::Zstandard(_)));

            // Verify the codec actually works by compressing data
            let mut writer = Writer::with_codec(&schema, Vec::new(), codec);
            let mut record = Record::new(&schema).unwrap();
            record.put("field", test_str);
            writer.append(record).unwrap();

            let encoded = writer.into_inner().unwrap();
            assert!(!encoded.is_empty(), "Compression should produce output");
        }

        // Test clamping - higher than 22 should be clamped to 22
        let codec_100 = codec_from_str(Some("zstd"), Some(100));
        let codec_22 = codec_from_str(Some("zstd"), Some(22));

        // Both should work and produce similar results
        let mut writer_100 = Writer::with_codec(&schema, Vec::new(), codec_100);
        let mut record_100 = Record::new(&schema).unwrap();
        record_100.put("field", test_str);
        writer_100.append(record_100).unwrap();
        let encoded_100 = writer_100.into_inner().unwrap();

        let mut writer_22 = Writer::with_codec(&schema, Vec::new(), codec_22);
        let mut record_22 = Record::new(&schema).unwrap();
        record_22.put("field", test_str);
        writer_22.append(record_22).unwrap();
        let encoded_22 = writer_22.into_inner().unwrap();

        // Both should produce the same size since 100 is clamped to 22
        assert_eq!(
            encoded_100.len(),
            encoded_22.len(),
            "Level 100 should be clamped to 22"
        );
    }

    #[test]
    fn test_compression_level_differences() {
        use apache_avro::types::Record;
        use apache_avro::{Schema, Writer};

        // Create a schema and data that will compress well
        let schema = Schema::parse_str(r#"{"type": "record", "name": "test", "fields": [{"name": "field", "type": "string"}]}"#).unwrap();

        // Use highly compressible data
        let test_str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

        // Test gzip level 0 (no compression) vs level 9 (best compression)
        let codec_0 = codec_from_str(Some("gzip"), Some(0));
        let mut writer_0 = Writer::with_codec(&schema, Vec::new(), codec_0);
        let mut record_0 = Record::new(&schema).unwrap();
        record_0.put("field", test_str);
        writer_0.append(record_0).unwrap();
        let size_0 = writer_0.into_inner().unwrap().len();

        let codec_9 = codec_from_str(Some("gzip"), Some(9));
        let mut writer_9 = Writer::with_codec(&schema, Vec::new(), codec_9);
        let mut record_9 = Record::new(&schema).unwrap();
        record_9.put("field", test_str);
        writer_9.append(record_9).unwrap();
        let size_9 = writer_9.into_inner().unwrap().len();

        // Level 0 should produce larger output than level 9 for compressible data
        assert!(
            size_0 > size_9,
            "NoCompression (level 0) should produce larger output than BestCompression (level 9): {} vs {}",
            size_0,
            size_9
        );
    }
}
