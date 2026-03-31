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

use crate::compression::CompressionCodec;

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

/// Parse codec name and optional level into a [`CompressionCodec`].
/// Returns `CompressionCodec::None` for unknown or unsupported codecs.
/// Codec-specific defaults are applied when `level` is `None`.
///
/// # Arguments
///
/// * `codec` - The name of the compression codec (e.g., "gzip", "zstd", "snappy", "uncompressed")
/// * `level` - Optional compression level; codec-specific defaults are applied when absent.
pub(crate) fn parse_avro_codec(codec: Option<&str>, level: Option<u8>) -> CompressionCodec {
    let Some(codec_str) = codec else {
        return CompressionCodec::None;
    };
    let lowercase = codec_str.to_lowercase();
    // "uncompressed" is Avro-specific and not in the standard CompressionCodec names
    if lowercase == CODEC_UNCOMPRESSED {
        return CompressionCodec::None;
    }
    match lowercase.as_str() {
        CODEC_GZIP => CompressionCodec::Gzip(level.unwrap_or(DEFAULT_GZIP_LEVEL)),
        CODEC_ZSTD => {
            CompressionCodec::Zstd(level.unwrap_or(DEFAULT_ZSTD_LEVEL).min(MAX_ZSTD_LEVEL))
        }
        CODEC_SNAPPY => CompressionCodec::Snappy,
        _ => {
            warn!("Unrecognized compression codec '{codec_str}', using no compression");
            CompressionCodec::None
        }
    }
}

/// Convert a [`CompressionCodec`] to an [`apache_avro::Codec`] for use in Avro writers.
pub(crate) fn to_avro_codec(codec: CompressionCodec) -> Codec {
    match codec {
        CompressionCodec::None => Codec::Null,
        CompressionCodec::Snappy => Codec::Snappy,
        CompressionCodec::Lz4 => Codec::Null,
        CompressionCodec::Gzip(level) => {
            let compression_level = match level {
                0 => CompressionLevel::NoCompression,
                1 => CompressionLevel::BestSpeed,
                9 => CompressionLevel::BestCompression,
                10 => CompressionLevel::UberCompression,
                _ => CompressionLevel::DefaultLevel,
            };
            Codec::Deflate(DeflateSettings::new(compression_level))
        }
        CompressionCodec::Zstd(level) => {
            Codec::Zstandard(ZstandardSettings::new(level.min(MAX_ZSTD_LEVEL)))
        }
    }
}

#[cfg(test)]
mod tests {
    use apache_avro::{DeflateSettings, ZstandardSettings};
    use miniz_oxide::deflate::CompressionLevel;

    use super::*;

    #[test]
    fn test_parse_avro_codec_gzip() {
        // Test with mixed case to verify case-insensitive matching
        let codec = parse_avro_codec(Some("GZip"), Some(5));
        assert_eq!(codec, CompressionCodec::Gzip(5));
    }

    #[test]
    fn test_parse_avro_codec_snappy() {
        let codec = parse_avro_codec(Some("snappy"), None);
        assert_eq!(codec, CompressionCodec::Snappy);
    }

    #[test]
    fn test_parse_avro_codec_zstd() {
        let codec = parse_avro_codec(Some("zstd"), Some(3));
        assert_eq!(codec, CompressionCodec::Zstd(3));
    }

    #[test]
    fn test_parse_avro_codec_uncompressed() {
        let codec = parse_avro_codec(Some("uncompressed"), None);
        assert_eq!(codec, CompressionCodec::None);
    }

    #[test]
    fn test_parse_avro_codec_null() {
        let codec = parse_avro_codec(None, None);
        assert_eq!(codec, CompressionCodec::None);
    }

    #[test]
    fn test_parse_avro_codec_unknown() {
        let codec = parse_avro_codec(Some("unknown"), Some(1));
        assert_eq!(codec, CompressionCodec::None);
    }

    #[test]
    fn test_parse_avro_codec_gzip_no_level() {
        // No level → codec-specific default (9)
        let codec = parse_avro_codec(Some("gzip"), None);
        assert_eq!(codec, CompressionCodec::Gzip(DEFAULT_GZIP_LEVEL));
    }

    #[test]
    fn test_parse_avro_codec_zstd_no_level() {
        // No level → codec-specific default (1)
        let codec = parse_avro_codec(Some("zstd"), None);
        assert_eq!(codec, CompressionCodec::Zstd(DEFAULT_ZSTD_LEVEL));
    }

    #[test]
    fn test_to_avro_codec_gzip_default() {
        let avro_codec = to_avro_codec(CompressionCodec::Gzip(DEFAULT_GZIP_LEVEL));
        assert_eq!(
            avro_codec,
            Codec::Deflate(DeflateSettings::new(CompressionLevel::BestCompression))
        );
    }

    #[test]
    fn test_to_avro_codec_gzip_level5() {
        let avro_codec = to_avro_codec(CompressionCodec::Gzip(5));
        assert_eq!(
            avro_codec,
            Codec::Deflate(DeflateSettings::new(CompressionLevel::DefaultLevel))
        );
    }

    #[test]
    fn test_to_avro_codec_zstd_default() {
        let avro_codec = to_avro_codec(CompressionCodec::Zstd(DEFAULT_ZSTD_LEVEL));
        assert_eq!(avro_codec, Codec::Zstandard(ZstandardSettings::new(1)));
    }

    #[test]
    fn test_to_avro_codec_zstd_level3() {
        let avro_codec = to_avro_codec(CompressionCodec::Zstd(3));
        assert_eq!(avro_codec, Codec::Zstandard(ZstandardSettings::new(3)));
    }

    #[test]
    fn test_to_avro_codec_zstd_clamping() {
        let avro_codec = to_avro_codec(CompressionCodec::Zstd(MAX_ZSTD_LEVEL + 1));
        assert_eq!(
            avro_codec,
            Codec::Zstandard(ZstandardSettings::new(MAX_ZSTD_LEVEL))
        );
    }

    #[test]
    fn test_to_avro_codec_null() {
        assert!(matches!(to_avro_codec(CompressionCodec::None), Codec::Null));
    }

    #[test]
    fn test_to_avro_codec_snappy() {
        assert!(matches!(
            to_avro_codec(CompressionCodec::Snappy),
            Codec::Snappy
        ));
    }
}
