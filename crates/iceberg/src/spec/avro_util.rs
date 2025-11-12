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
use crate::error::{Result, invalid_data};

// Avro manifest defaults, chosen to match Java Iceberg (GzipCodec/ZstandardCodec). Distinct from
// the data-file defaults in compression.rs (gzip=6, zstd=3).
const DEFAULT_GZIP_LEVEL: u8 = 9;
const DEFAULT_ZSTD_LEVEL: u8 = 1;
/// Max gzip level, matching the range accepted by Java's `Deflater`.
const MAX_GZIP_LEVEL: u8 = 9;
/// Max supported level for ZSTD; higher levels are clamped, as zstd itself does.
const MAX_ZSTD_LEVEL: u8 = 22;

/// Default codec for Avro files (manifests, manifest lists).
pub(crate) const DEFAULT_AVRO_CODEC: CompressionCodec = CompressionCodec::Gzip(DEFAULT_GZIP_LEVEL);

/// Parse a codec name (case-insensitive) into a [`CompressionCodec`], with the Avro-specific
/// default level for codecs that carry one.
///
/// This only resolves the name; use [`to_avro_codec`] to check that the codec (and its level) is
/// supported for Avro.
pub(crate) fn parse_avro_codec(name: &str) -> Result<CompressionCodec> {
    Ok(
        match CompressionCodec::from_name(name)
            .ok_or_else(|| invalid_data!("Unsupported compression codec: {name}"))?
        {
            CompressionCodec::Gzip(_) => CompressionCodec::Gzip(DEFAULT_GZIP_LEVEL),
            CompressionCodec::Zstd(_) => CompressionCodec::Zstd(DEFAULT_ZSTD_LEVEL),
            other => other,
        },
    )
}

/// Convert a [`CompressionCodec`] to an [`apache_avro::Codec`] for use in Avro writers.
///
/// Returns an error for codecs that are not supported in Avro (e.g. LZ4) and for gzip levels
/// above 9. Matching Java, zstd levels above 22 are clamped.
///
/// apache-avro only exposes the named deflate levels 0, 1, 6 and 9, so other gzip levels are
/// rounded to the nearest of these (1–3 → 1, 4–7 → 6, 8 → 9).
pub(crate) fn to_avro_codec(codec: CompressionCodec) -> Result<Codec> {
    match codec {
        CompressionCodec::None => Ok(Codec::Null),
        CompressionCodec::Snappy => Ok(Codec::Snappy),
        CompressionCodec::Gzip(level) => {
            let compression_level = match level {
                0 => CompressionLevel::NoCompression,
                1..=3 => CompressionLevel::BestSpeed,
                4..=7 => CompressionLevel::DefaultLevel,
                8..=MAX_GZIP_LEVEL => CompressionLevel::BestCompression,
                _ => {
                    return Err(invalid_data!(
                        "Invalid gzip compression level {level} for Avro files, expected 0-{MAX_GZIP_LEVEL}"
                    ));
                }
            };
            Ok(Codec::Deflate(DeflateSettings::new(compression_level)))
        }
        CompressionCodec::Zstd(level) => Ok(Codec::Zstandard(ZstandardSettings::new(
            level.min(MAX_ZSTD_LEVEL),
        ))),
        CompressionCodec::Lz4
        | CompressionCodec::Lz4Raw
        | CompressionCodec::Brotli(_)
        | CompressionCodec::Lzo => Err(invalid_data!(
            "Unsupported compression codec for Avro files: {}",
            codec.name()
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorKind;

    fn deflate(level: CompressionLevel) -> Codec {
        Codec::Deflate(DeflateSettings::new(level))
    }

    #[test]
    fn test_parse_avro_codec() {
        for (name, expected) in [
            // Mixed case verifies case-insensitive matching.
            ("GZip", CompressionCodec::Gzip(DEFAULT_GZIP_LEVEL)),
            ("zstd", CompressionCodec::Zstd(DEFAULT_ZSTD_LEVEL)),
            ("snappy", CompressionCodec::Snappy),
            ("uncompressed", CompressionCodec::None),
            ("none", CompressionCodec::None),
        ] {
            assert_eq!(parse_avro_codec(name).unwrap(), expected, "codec {name}");
        }
    }

    #[test]
    fn test_parse_avro_codec_unknown() {
        let err = parse_avro_codec("unknown").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.to_string()
                .contains("Unsupported compression codec: unknown"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_to_avro_codec() {
        for (codec, expected) in [
            (CompressionCodec::None, Codec::Null),
            (CompressionCodec::Snappy, Codec::Snappy),
            (
                DEFAULT_AVRO_CODEC,
                deflate(CompressionLevel::BestCompression),
            ),
            (
                CompressionCodec::Zstd(3),
                Codec::Zstandard(ZstandardSettings::new(3)),
            ),
            (
                CompressionCodec::Zstd(MAX_ZSTD_LEVEL + 1),
                Codec::Zstandard(ZstandardSettings::new(MAX_ZSTD_LEVEL)),
            ),
        ] {
            assert_eq!(to_avro_codec(codec).unwrap(), expected, "codec {codec}");
        }
    }

    #[test]
    fn test_to_avro_codec_gzip_levels_round_to_nearest() {
        let expected = [
            CompressionLevel::NoCompression,
            CompressionLevel::BestSpeed,
            CompressionLevel::BestSpeed,
            CompressionLevel::BestSpeed,
            CompressionLevel::DefaultLevel,
            CompressionLevel::DefaultLevel,
            CompressionLevel::DefaultLevel,
            CompressionLevel::DefaultLevel,
            CompressionLevel::BestCompression,
            CompressionLevel::BestCompression,
        ];
        for (level, expected) in (0..=MAX_GZIP_LEVEL).zip(expected) {
            assert_eq!(
                to_avro_codec(CompressionCodec::Gzip(level)).unwrap(),
                deflate(expected),
                "gzip level {level}"
            );
        }
    }

    #[test]
    fn test_to_avro_codec_rejected() {
        for (codec, message) in [
            (
                CompressionCodec::Gzip(MAX_GZIP_LEVEL + 1),
                "Invalid gzip compression level 10 for Avro files, expected 0-9",
            ),
            (
                CompressionCodec::Gzip(200),
                "Invalid gzip compression level 200 for Avro files, expected 0-9",
            ),
            (
                CompressionCodec::Lz4,
                "Unsupported compression codec for Avro files: lz4",
            ),
            (
                CompressionCodec::Lz4Raw,
                "Unsupported compression codec for Avro files: lz4_raw",
            ),
            (
                CompressionCodec::brotli_default(),
                "Unsupported compression codec for Avro files: brotli",
            ),
            (
                CompressionCodec::Lzo,
                "Unsupported compression codec for Avro files: lzo",
            ),
        ] {
            let err = to_avro_codec(codec).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::DataInvalid, "codec {codec}");
            assert!(err.to_string().contains(message), "unexpected error: {err}");
        }
    }
}
