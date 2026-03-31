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

//! Compression codec support for data compression and decompression.

use std::fmt;
use std::io::{Read, Write};

use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{Error, ErrorKind, Result};

/// Default compression level for Zstandard (zstd).
const ZSTD_DEFAULT_LEVEL: u8 = 3;
/// Default compression level for Gzip.
const GZIP_DEFAULT_LEVEL: u8 = 6;
/// Maximum compression level for Gzip.
const GZIP_MAX_LEVEL: u8 = 9;

/// Data compression formats
#[derive(Debug, PartialEq, Eq, Clone, Copy, Default)]
pub enum CompressionCodec {
    #[default]
    /// No compression
    None,
    /// LZ4 single compression frame with content size present
    Lz4,
    /// Zstandard single compression frame with content size present.
    /// Level range is 0–22, where 0 means default compression level (not no compression).
    /// Use [`CompressionCodec::zstd_default`] to construct with the default level.
    Zstd(u8),
    /// Gzip compression. Level range is 0–9, where 0 means no compression.
    /// Use [`CompressionCodec::gzip_default`] to construct with the default level.
    Gzip(u8),
    /// Snappy compression
    Snappy,
}

impl CompressionCodec {
    /// Returns a Zstd codec with the default compression level.
    pub const fn zstd_default() -> Self {
        CompressionCodec::Zstd(ZSTD_DEFAULT_LEVEL)
    }

    /// Returns a Gzip codec with the default compression level.
    pub const fn gzip_default() -> Self {
        CompressionCodec::Gzip(GZIP_DEFAULT_LEVEL)
    }

    /// Returns the codec name as used in serialization and error messages.
    pub fn name(&self) -> &'static str {
        match self {
            CompressionCodec::None => "none",
            CompressionCodec::Lz4 => "lz4",
            CompressionCodec::Zstd(_) => "zstd",
            CompressionCodec::Gzip(_) => "gzip",
            CompressionCodec::Snappy => "snappy",
        }
    }
}

// Note: serialize/deserialize do not round-trip the compression level. Iceberg configuration
// only the codec name (e.g. "zstd"), not the level, so deserialization always produces the
// default level. A `Zstd(5)` written to metadata will be read back as `Zstd(3)`. Some
// compression configuration (e.g. Avro metadata) has a separate level field alongside the codec name.
impl Serialize for CompressionCodec {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(self.name())
    }
}

impl<'de> Deserialize<'de> for CompressionCodec {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        match s.to_lowercase().as_str() {
            "none" => Ok(CompressionCodec::None),
            "lz4" => Ok(CompressionCodec::Lz4),
            "zstd" => Ok(CompressionCodec::zstd_default()),
            "gzip" => Ok(CompressionCodec::gzip_default()),
            "snappy" => Ok(CompressionCodec::Snappy),
            other => Err(serde::de::Error::unknown_variant(other, &[
                "none", "lz4", "zstd", "gzip", "snappy",
            ])),
        }
    }
}

impl fmt::Display for CompressionCodec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompressionCodec::None => write!(f, "None"),
            CompressionCodec::Lz4 => write!(f, "Lz4"),
            CompressionCodec::Zstd(level) => write!(f, "Zstd(level={level})"),
            CompressionCodec::Gzip(level) => write!(f, "Gzip(level={level})"),
            CompressionCodec::Snappy => write!(f, "Snappy"),
        }
    }
}

impl CompressionCodec {
    pub(crate) fn decompress(&self, bytes: Vec<u8>) -> Result<Vec<u8>> {
        match self {
            CompressionCodec::None => Ok(bytes),
            CompressionCodec::Lz4 => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "LZ4 decompression is not supported currently",
            )),
            CompressionCodec::Zstd(_) => Ok(zstd::stream::decode_all(&bytes[..])?),
            CompressionCodec::Gzip(_) => {
                let mut decoder = GzDecoder::new(&bytes[..]);
                let mut decompressed = Vec::new();
                decoder.read_to_end(&mut decompressed)?;
                Ok(decompressed)
            }
            CompressionCodec::Snappy => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Snappy decompression is not supported currently",
            )),
        }
    }

    pub(crate) fn compress(&self, bytes: Vec<u8>) -> Result<Vec<u8>> {
        match self {
            CompressionCodec::None => Ok(bytes),
            CompressionCodec::Lz4 => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "LZ4 compression is not supported currently",
            )),
            CompressionCodec::Zstd(level) => {
                let writer = Vec::<u8>::new();
                let mut encoder = zstd::stream::Encoder::new(writer, *level as i32)?;
                encoder.include_checksum(true)?;
                encoder.set_pledged_src_size(Some(bytes.len().try_into()?))?;
                std::io::copy(&mut &bytes[..], &mut encoder)?;
                Ok(encoder.finish()?)
            }
            CompressionCodec::Gzip(level) => {
                let compression = Compression::new((*level).min(GZIP_MAX_LEVEL) as u32);
                let mut encoder = GzEncoder::new(Vec::new(), compression);
                encoder.write_all(&bytes)?;
                Ok(encoder.finish()?)
            }
            CompressionCodec::Snappy => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Snappy compression is not supported currently",
            )),
        }
    }

    pub(crate) fn is_none(&self) -> bool {
        matches!(self, CompressionCodec::None)
    }

    /// Returns the file extension suffix for this compression codec.
    /// Returns empty string for None, ".gz" for Gzip.
    ///
    /// # Errors
    ///
    /// Returns an error for Lz4 and Zstd as they are not fully supported.
    pub fn suffix(&self) -> Result<&'static str> {
        match self {
            CompressionCodec::None => Ok(""),
            CompressionCodec::Gzip(_) => Ok(".gz"),
            codec @ (CompressionCodec::Lz4
            | CompressionCodec::Zstd(_)
            | CompressionCodec::Snappy) => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!("suffix not defined for {codec:?}"),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::CompressionCodec;

    #[tokio::test]
    async fn test_compression_codec_none() {
        let bytes_vec = [0_u8; 100].to_vec();

        let codec = CompressionCodec::None;
        let compressed = codec.compress(bytes_vec.clone()).unwrap();
        assert_eq!(bytes_vec, compressed);
        let decompressed = codec.decompress(compressed).unwrap();
        assert_eq!(bytes_vec, decompressed);
    }

    #[tokio::test]
    async fn test_compression_codec_compress() {
        let bytes_vec = [0_u8; 100].to_vec();

        let compression_codecs = [
            CompressionCodec::zstd_default(),
            CompressionCodec::gzip_default(),
        ];

        for codec in compression_codecs {
            let compressed = codec.compress(bytes_vec.clone()).unwrap();
            assert!(compressed.len() < bytes_vec.len());
            let decompressed = codec.decompress(compressed).unwrap();
            assert_eq!(decompressed, bytes_vec);
        }
    }

    #[tokio::test]
    async fn test_compression_codec_unsupported() {
        let unsupported_codecs = [
            (CompressionCodec::Lz4, "LZ4"),
            (CompressionCodec::Snappy, "Snappy"),
        ];
        let bytes_vec = [0_u8; 100].to_vec();

        for (codec, name) in unsupported_codecs {
            assert_eq!(
                codec.compress(bytes_vec.clone()).unwrap_err().to_string(),
                format!("FeatureUnsupported => {name} compression is not supported currently"),
            );

            assert_eq!(
                codec.decompress(bytes_vec.clone()).unwrap_err().to_string(),
                format!("FeatureUnsupported => {name} decompression is not supported currently"),
            );
        }
    }

    #[test]
    fn test_suffix() {
        assert_eq!(CompressionCodec::None.suffix().unwrap(), "");
        assert_eq!(CompressionCodec::gzip_default().suffix().unwrap(), ".gz");

        assert!(CompressionCodec::Lz4.suffix().is_err());
        assert!(CompressionCodec::zstd_default().suffix().is_err());
        assert!(CompressionCodec::Snappy.suffix().is_err());

        let lz4_err = CompressionCodec::Lz4.suffix().unwrap_err();
        assert!(lz4_err.to_string().contains("suffix not defined for Lz4"));

        let zstd_err = CompressionCodec::zstd_default().suffix().unwrap_err();
        assert!(zstd_err.to_string().contains("suffix not defined for Zstd"));
    }

    #[test]
    fn test_display() {
        assert_eq!(CompressionCodec::None.to_string(), "None");
        assert_eq!(CompressionCodec::Lz4.to_string(), "Lz4");
        assert_eq!(
            CompressionCodec::zstd_default().to_string(),
            "Zstd(level=3)"
        );
        assert_eq!(CompressionCodec::Zstd(5).to_string(), "Zstd(level=5)");
        assert_eq!(
            CompressionCodec::gzip_default().to_string(),
            "Gzip(level=6)"
        );
        assert_eq!(CompressionCodec::Gzip(9).to_string(), "Gzip(level=9)");
        assert_eq!(CompressionCodec::Snappy.to_string(), "Snappy");
    }
}
