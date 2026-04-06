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

use crate::compression::CompressionCodec;
use crate::{Error, ErrorKind, Result};

/// Codec name for uncompressed (Avro-specific; maps to [`CompressionCodec::None`])
const CODEC_UNCOMPRESSED: &str = "uncompressed";

/// Default compression level for gzip in Avro (matches Java implementation)
const DEFAULT_GZIP_LEVEL: u8 = 9;
/// Default compression level for zstd in Avro (matches Java implementation)
const DEFAULT_ZSTD_LEVEL: u8 = 1;
/// Max supported level for ZSTD
const MAX_ZSTD_LEVEL: u8 = 22;

/// Parse a codec name and optional level into a [`CompressionCodec`].
///
/// The codec name is parsed via [`CompressionCodec`]'s standard deserialization.
/// `"uncompressed"` (Avro-specific) is mapped to [`CompressionCodec::None`].
/// Avro-specific defaults apply when `level` is `None`: gzip→9, zstd→1.
pub(crate) fn parse_avro_codec(codec: Option<&str>, level: Option<u8>) -> Result<CompressionCodec> {
    let Some(codec_str) = codec else {
        return Ok(CompressionCodec::None);
    };
    let normalized = if codec_str.eq_ignore_ascii_case(CODEC_UNCOMPRESSED) {
        "none"
    } else {
        codec_str
    };
    let parsed: CompressionCodec = serde_json::from_value(serde_json::Value::String(
        normalized.to_string(),
    ))
    .map_err(|_| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Unrecognized Avro compression codec: {codec_str}"),
        )
    })?;
    match parsed {
        CompressionCodec::None => Ok(CompressionCodec::None),
        CompressionCodec::Snappy => Ok(CompressionCodec::Snappy),
        CompressionCodec::Gzip(_) => {
            Ok(CompressionCodec::Gzip(level.unwrap_or(DEFAULT_GZIP_LEVEL)))
        }
        CompressionCodec::Zstd(_) => {
            Ok(CompressionCodec::Zstd(level.unwrap_or(DEFAULT_ZSTD_LEVEL)))
        }
        other => Err(Error::new(
            ErrorKind::DataInvalid,
            format!("Unsupported Avro compression codec: {}", other.name()),
        )),
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
    use apache_avro::{Codec, DeflateSettings, ZstandardSettings};
    use miniz_oxide::deflate::CompressionLevel;
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::gzip_case_insensitive(Some("GZip"), Some(5), CompressionCodec::Gzip(5))]
    #[case::gzip_avro_default_level(Some("gzip"), None, CompressionCodec::Gzip(DEFAULT_GZIP_LEVEL))]
    #[case::zstd_explicit_level(Some("zstd"), Some(3), CompressionCodec::Zstd(3))]
    #[case::zstd_avro_default_level(Some("zstd"), None, CompressionCodec::Zstd(DEFAULT_ZSTD_LEVEL))]
    #[case::snappy(Some("snappy"), None, CompressionCodec::Snappy)]
    #[case::uncompressed_avro_alias(Some("uncompressed"), None, CompressionCodec::None)]
    #[case::no_codec(None, None, CompressionCodec::None)]
    fn test_parse_avro_codec(
        #[case] codec: Option<&str>,
        #[case] level: Option<u8>,
        #[case] expected: CompressionCodec,
    ) {
        assert_eq!(parse_avro_codec(codec, level).unwrap(), expected);
    }

    #[rstest]
    #[case::unknown_codec(Some("unknown"), Some(1), "unknown")]
    #[case::lz4_unsupported(Some("lz4"), None, "lz4")]
    fn test_parse_avro_codec_error(
        #[case] codec: Option<&str>,
        #[case] level: Option<u8>,
        #[case] expected_msg: &str,
    ) {
        let err = parse_avro_codec(codec, level).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.to_string().contains(expected_msg),
            "expected '{expected_msg}' in error: {err}"
        );
    }

    #[rstest]
    #[case::none(CompressionCodec::None, Codec::Null)]
    #[case::snappy(CompressionCodec::Snappy, Codec::Snappy)]
    #[case::gzip_best_compression(
        CompressionCodec::Gzip(9),
        Codec::Deflate(DeflateSettings::new(CompressionLevel::BestCompression))
    )]
    #[case::gzip_default_level(
        CompressionCodec::Gzip(5),
        Codec::Deflate(DeflateSettings::new(CompressionLevel::DefaultLevel))
    )]
    #[case::zstd(CompressionCodec::Zstd(3), Codec::Zstandard(ZstandardSettings::new(3)))]
    #[case::zstd_level_clamped_to_max(CompressionCodec::Zstd(MAX_ZSTD_LEVEL + 1), Codec::Zstandard(ZstandardSettings::new(MAX_ZSTD_LEVEL)))]
    fn test_to_avro_codec(#[case] input: CompressionCodec, #[case] expected: Codec) {
        assert_eq!(to_avro_codec(input), expected);
    }
}
