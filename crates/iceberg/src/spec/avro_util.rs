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
use miniz_oxide::deflate::CompressionLevel;
use serde_json::Value;

use crate::compression::CompressionCodec;
use crate::error::Result;
use crate::{Error, ErrorKind};

/// Codec name for uncompressed
pub const CODEC_UNCOMPRESSED: &str = "uncompressed";

/// Default compression level for gzip (matches Java implementation)
const DEFAULT_GZIP_LEVEL: u8 = 9;
/// Default compression level for zstd (matches Java implementation)
const DEFAULT_ZSTD_LEVEL: u8 = 1;
/// Max supported level for ZSTD
const MAX_ZSTD_LEVEL: u8 = 22;

/// Parse codec name and optional level into a [`CompressionCodec`].
///
/// Returns `DataInvalid` for unrecognized codec names, matching the Java implementation which
/// throws `IllegalArgumentException("Unsupported compression codec: ...")` in the same case.
///
/// # Arguments
///
/// * `codec` - The name of the compression codec (e.g., "gzip", "zstd", "snappy", "uncompressed")
/// * `level` - Optional compression level stored as-is; codec-specific defaults are applied
///   in [`to_avro_codec`] at the point of use.
pub(crate) fn parse_avro_codec(codec: Option<&str>, level: Option<u8>) -> Result<CompressionCodec> {
    let Some(codec_str) = codec else {
        return Ok(CompressionCodec::None);
    };
    let lowercase = codec_str.to_lowercase();
    // "uncompressed" is Avro-specific and not in the standard CompressionCodec names
    if lowercase == CODEC_UNCOMPRESSED {
        return Ok(CompressionCodec::None);
    }
    let base: CompressionCodec =
        serde_json::from_value(Value::String(lowercase)).map_err(|_| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Unsupported compression codec: {codec_str}"),
            )
        })?;
    Ok(base.with_level(level))
}

/// Convert a [`CompressionCodec`] to an [`apache_avro::Codec`] for use in Avro writers.
/// Codec-specific defaults are applied here (e.g. gzip defaults to level 9, zstd to level 1).
/// Returns an error for codecs that are not supported in Avro (e.g. LZ4).
pub(crate) fn to_avro_codec(codec: CompressionCodec) -> Result<Codec> {
    match codec {
        CompressionCodec::None => Ok(Codec::Null),
        CompressionCodec::Snappy => Ok(Codec::Snappy),
        CompressionCodec::Lz4 => Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "LZ4 compression is not supported for Avro files",
        )),
        CompressionCodec::Gzip(level) => {
            // miniz_oxide::CompressionLevel only exposes a handful of named levels, so levels
            // 2–8 and 11+ all map to DefaultLevel. This is a library granularity limitation,
            // not a bug — the output is valid gzip at whatever level miniz chooses.
            let compression_level = match level.unwrap_or(DEFAULT_GZIP_LEVEL) {
                0 => CompressionLevel::NoCompression,
                1 => CompressionLevel::BestSpeed,
                9 => CompressionLevel::BestCompression,
                10 => CompressionLevel::UberCompression,
                _ => CompressionLevel::DefaultLevel,
            };
            Ok(Codec::Deflate(DeflateSettings::new(compression_level)))
        }
        CompressionCodec::Zstd(level) => {
            let zstd_level = level.unwrap_or(DEFAULT_ZSTD_LEVEL).min(MAX_ZSTD_LEVEL);
            Ok(Codec::Zstandard(ZstandardSettings::new(zstd_level)))
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
        let codec = parse_avro_codec(Some("GZip"), Some(5)).unwrap();
        assert_eq!(codec, CompressionCodec::Gzip(Some(5)));
    }

    #[test]
    fn test_parse_avro_codec_snappy() {
        let codec = parse_avro_codec(Some("snappy"), None).unwrap();
        assert_eq!(codec, CompressionCodec::Snappy);
    }

    #[test]
    fn test_parse_avro_codec_zstd() {
        let codec = parse_avro_codec(Some("zstd"), Some(3)).unwrap();
        assert_eq!(codec, CompressionCodec::Zstd(Some(3)));
    }

    #[test]
    fn test_parse_avro_codec_uncompressed() {
        let codec = parse_avro_codec(Some("uncompressed"), None).unwrap();
        assert_eq!(codec, CompressionCodec::None);
    }

    #[test]
    fn test_parse_avro_codec_null() {
        let codec = parse_avro_codec(None, None).unwrap();
        assert_eq!(codec, CompressionCodec::None);
    }

    #[test]
    fn test_parse_avro_codec_unknown() {
        let err = parse_avro_codec(Some("unknown"), Some(1)).unwrap_err();
        assert!(
            err.to_string().contains("Unsupported compression codec: unknown"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_parse_avro_codec_gzip_no_level() {
        // Level is stored as-is (None), default applied in to_avro_codec
        let codec = parse_avro_codec(Some("gzip"), None).unwrap();
        assert_eq!(codec, CompressionCodec::Gzip(None));
    }

    #[test]
    fn test_parse_avro_codec_zstd_no_level() {
        let codec = parse_avro_codec(Some("zstd"), None).unwrap();
        assert_eq!(codec, CompressionCodec::Zstd(None));
    }

    #[test]
    fn test_to_avro_codec_gzip_default() {
        // None level → default 9 (BestCompression)
        let avro_codec = to_avro_codec(CompressionCodec::Gzip(None)).unwrap();
        assert_eq!(
            avro_codec,
            Codec::Deflate(DeflateSettings::new(CompressionLevel::BestCompression))
        );
    }

    #[test]
    fn test_to_avro_codec_gzip_level5() {
        let avro_codec = to_avro_codec(CompressionCodec::Gzip(Some(5))).unwrap();
        assert_eq!(
            avro_codec,
            Codec::Deflate(DeflateSettings::new(CompressionLevel::DefaultLevel))
        );
    }

    #[test]
    fn test_to_avro_codec_zstd_default() {
        // None level → default 1
        let avro_codec = to_avro_codec(CompressionCodec::Zstd(None)).unwrap();
        assert_eq!(avro_codec, Codec::Zstandard(ZstandardSettings::new(1)));
    }

    #[test]
    fn test_to_avro_codec_zstd_level3() {
        let avro_codec = to_avro_codec(CompressionCodec::Zstd(Some(3))).unwrap();
        assert_eq!(avro_codec, Codec::Zstandard(ZstandardSettings::new(3)));
    }

    #[test]
    fn test_to_avro_codec_zstd_clamping() {
        let avro_codec = to_avro_codec(CompressionCodec::Zstd(Some(MAX_ZSTD_LEVEL + 1))).unwrap();
        assert_eq!(
            avro_codec,
            Codec::Zstandard(ZstandardSettings::new(MAX_ZSTD_LEVEL))
        );
    }

    #[test]
    fn test_to_avro_codec_null() {
        assert!(matches!(
            to_avro_codec(CompressionCodec::None).unwrap(),
            Codec::Null
        ));
    }

    #[test]
    fn test_to_avro_codec_snappy() {
        assert!(matches!(
            to_avro_codec(CompressionCodec::Snappy).unwrap(),
            Codec::Snappy
        ));
    }

    #[test]
    fn test_to_avro_codec_lz4_unsupported() {
        assert!(to_avro_codec(CompressionCodec::Lz4).is_err());
    }
}
