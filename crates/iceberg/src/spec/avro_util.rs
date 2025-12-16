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

use apache_avro::{Codec, DeflateSettings, ZstandardSettings};
use log::warn;
use miniz_oxide::deflate::CompressionLevel;

/// Codec name for gzip compression
pub const CODEC_GZIP: &str = "gzip";
/// Codec name for zstd compression
pub const CODEC_ZSTD: &str = "zstd";
/// Codec name for snappy compression
pub const CODEC_SNAPPY: &str = "snappy";
/// Codec name for uncompressed
pub const CODEC_UNCOMPRESSED: &str = "uncompressed";

/// Default compression level for gzip (matches Java implementation)
const DEFAULT_GZIP_LEVEL: u8 = 9;
/// Default compression level for zstd (matches Java implementation)
const DEFAULT_ZSTD_LEVEL: u8 = 1;
/// Max supported level for ZSTD
const MAX_ZSTD_LEVEL: u8 = 22;

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
///
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
    // Use case-insensitive comparison to match Java implementation
    match codec.map(|s| s.to_lowercase()).as_deref() {
        Some(c) if c == CODEC_GZIP => {
            // Map compression level to miniz_oxide::deflate::CompressionLevel
            // Reference: https://docs.rs/miniz_oxide/latest/miniz_oxide/deflate/enum.CompressionLevel.html
            let compression_level = match level.unwrap_or(DEFAULT_GZIP_LEVEL) {
                0 => CompressionLevel::NoCompression,
                1 => CompressionLevel::BestSpeed,
                9 => CompressionLevel::BestCompression,
                10 => CompressionLevel::UberCompression,
                _ => CompressionLevel::DefaultLevel,
            };

            Codec::Deflate(DeflateSettings::new(compression_level))
        }
        Some(c) if c == CODEC_ZSTD => {
            // Zstandard supports levels 0-22, clamp to valid range
            let zstd_level = level.unwrap_or(DEFAULT_ZSTD_LEVEL).min(MAX_ZSTD_LEVEL);
            Codec::Zstandard(ZstandardSettings::new(zstd_level))
        }
        Some(c) if c == CODEC_SNAPPY => Codec::Snappy,
        Some(c) if c == CODEC_UNCOMPRESSED => Codec::Null,
        None => Codec::Null,
        Some(unknown) => {
            warn!(
                "Unrecognized compression codec '{unknown}', using no compression (Codec::Null)"
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
    fn test_codec_from_str_zstd_clamping() {
        let codec = codec_from_str(Some("zstd"), Some(MAX_ZSTD_LEVEL + 1));
        assert_eq!(
            codec,
            Codec::Zstandard(ZstandardSettings::new(MAX_ZSTD_LEVEL))
        );
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
}
