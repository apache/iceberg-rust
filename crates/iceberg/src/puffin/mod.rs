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

//! Iceberg Puffin implementation.

#![deny(missing_docs)]

use crate::{Error, ErrorKind, Result};

mod blob;
pub use blob::{APACHE_DATASKETCHES_THETA_V1, Blob, DELETION_VECTOR_V1};

pub use crate::compression::CompressionCodec;

/// Validates that the compression codec is supported for Puffin files.
/// Returns an error if the codec is not supported.
fn validate_puffin_compression(codec: CompressionCodec) -> Result<()> {
    match codec {
        CompressionCodec::None | CompressionCodec::Lz4 | CompressionCodec::Zstd(_) => Ok(()),
        other => Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Compression codec {} is not supported for Puffin files. Only {}, {}, and {} are supported.",
                other.name(),
                CompressionCodec::None.name(),
                CompressionCodec::Lz4.name(),
                CompressionCodec::zstd_default().name()
            ),
        )),
    }
}

mod metadata;
pub use metadata::{BlobMetadata, CREATED_BY_PROPERTY, FileMetadata};

mod reader;
pub use reader::PuffinReader;

mod writer;
pub use writer::PuffinWriter;

#[cfg(test)]
mod test_utils;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_puffin_codec_validation() {
        // Supported codecs
        assert!(validate_puffin_compression(CompressionCodec::None).is_ok());
        assert!(validate_puffin_compression(CompressionCodec::Lz4).is_ok());
        assert!(validate_puffin_compression(CompressionCodec::zstd_default()).is_ok());
        assert!(validate_puffin_compression(CompressionCodec::Zstd(5)).is_ok());

        // Unsupported codecs
        assert!(validate_puffin_compression(CompressionCodec::gzip_default()).is_err());
    }
}
