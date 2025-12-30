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

use std::io::{Read, Write};

use lz4_flex::frame::{FrameDecoder, FrameEncoder};
use serde::{Deserialize, Serialize};

use crate::{Error, ErrorKind, Result};

/// Data compression formats
#[derive(Debug, PartialEq, Eq, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressionCodec {
    #[default]
    /// No compression
    None,
    /// LZ4 single compression frame with content size present
    Lz4,
    /// Zstandard single compression frame with content size present
    Zstd,
}

impl CompressionCodec {
    pub(crate) fn decompress(&self, bytes: Vec<u8>) -> Result<Vec<u8>> {
        match self {
            CompressionCodec::None => Ok(bytes),
            CompressionCodec::Lz4 => {
                let mut decoder = FrameDecoder::new(&bytes[..]);
                let mut decompressed = Vec::new();
                decoder
                    .read_to_end(&mut decompressed)
                    .map_err(|e| Error::new(ErrorKind::IOError, e.to_string()))?;
                Ok(decompressed)
            }
            CompressionCodec::Zstd => {
                let decompressed = zstd::stream::decode_all(&bytes[..])?;
                Ok(decompressed)
            }
        }
    }

    pub(crate) fn compress(&self, bytes: Vec<u8>) -> Result<Vec<u8>> {
        match self {
            CompressionCodec::None => Ok(bytes),
            CompressionCodec::Lz4 => {
                let mut encoder = FrameEncoder::new(Vec::new());
                encoder
                    .write_all(&bytes)
                    .map_err(|e| Error::new(ErrorKind::IOError, e.to_string()))?;
                let compressed = encoder
                    .finish()
                    .map_err(|e| Error::new(ErrorKind::IOError, e.to_string()))?;
                Ok(compressed)
            }
            CompressionCodec::Zstd => {
                let writer = Vec::<u8>::new();
                let mut encoder = zstd::stream::Encoder::new(writer, 3)?;
                encoder.include_checksum(true)?;
                encoder.set_pledged_src_size(Some(bytes.len().try_into()?))?;
                std::io::copy(&mut &bytes[..], &mut encoder)?;
                let compressed = encoder.finish()?;
                Ok(compressed)
            }
        }
    }

    pub(crate) fn is_none(&self) -> bool {
        matches!(self, CompressionCodec::None)
    }
}

#[cfg(test)]
mod tests {
    use crate::puffin::compression::CompressionCodec;

    #[tokio::test]
    async fn test_compression_codec_none() {
        let compression_codec = CompressionCodec::None;
        let bytes_vec = [0_u8; 100].to_vec();

        let compressed = compression_codec.compress(bytes_vec.clone()).unwrap();
        assert_eq!(bytes_vec, compressed);

        let decompressed = compression_codec.decompress(compressed.clone()).unwrap();
        assert_eq!(compressed, decompressed)
    }

    #[tokio::test]
    async fn test_compression_codec_lz4() {
        let compression_codec = CompressionCodec::Lz4;

        // Highly compressible data: all zeros
        let data = vec![0u8; 10_000];
        let compressed = compression_codec.compress(data.clone()).unwrap();
        assert!(compressed.len() < data.len() / 2); // Should compress to less than half the original size
        let decompressed = compression_codec.decompress(compressed).unwrap();
        assert_eq!(decompressed, data);

        // Empty input
        let empty = vec![];
        let compressed_empty = compression_codec.compress(empty.clone()).unwrap();
        let decompressed_empty = compression_codec.decompress(compressed_empty).unwrap();
        assert_eq!(decompressed_empty, empty);
    }

    #[tokio::test]
    async fn test_compression_codec_zstd() {
        let compression_codec = CompressionCodec::Zstd;
        let bytes_vec = [0_u8; 100].to_vec();

        let compressed = compression_codec.compress(bytes_vec.clone()).unwrap();
        assert!(compressed.len() < bytes_vec.len());

        let decompressed = compression_codec.decompress(compressed.clone()).unwrap();
        assert_eq!(decompressed, bytes_vec)
    }

    #[test]
    fn test_lz4_roundtrip() {
        let data: Vec<u8> = (0..10_000).map(|i| (i % 256) as u8).collect();

        let compressed = CompressionCodec::Lz4.compress(data.clone()).unwrap();
        assert!(compressed.len() < data.len());

        let decompressed = CompressionCodec::Lz4.decompress(compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_lz4_empty() {
        let empty = vec![];
        let compressed = CompressionCodec::Lz4.compress(empty.clone()).unwrap();
        let decompressed = CompressionCodec::Lz4.decompress(compressed).unwrap();
        assert_eq!(empty, decompressed);
    }
}
