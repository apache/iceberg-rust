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

use iceberg::io::{FileRead, InputFile};
use iceberg::Result;

use crate::blob::Blob;
use crate::metadata::{BlobMetadata, FileMetadata};

/// Puffin reader
pub struct PuffinReader {
    input_file: InputFile,
    file_metadata: Option<FileMetadata>,
}

impl PuffinReader {
    /// Returns a new Puffin reader
    pub fn new(input_file: InputFile) -> Self {
        Self {
            input_file,
            file_metadata: None,
        }
    }

    /// Returns file metadata
    pub async fn file_metadata(&mut self) -> Result<&FileMetadata> {
        if let Some(ref file_metadata) = self.file_metadata {
            Ok(file_metadata)
        } else {
            let file_metadata = FileMetadata::read(&self.input_file).await?;
            Ok(self.file_metadata.insert(file_metadata))
        }
    }

    /// Returns blob
    pub async fn blob(&self, blob_metadata: BlobMetadata) -> Result<Blob> {
        let file_read = self.input_file.reader().await?;
        let start = blob_metadata.offset;
        let end = start + u64::try_from(blob_metadata.length)?;
        let bytes = file_read.read(start..end).await?.to_vec();
        let data = blob_metadata.compression_codec.decompress(bytes)?;

        Ok(Blob {
            r#type: blob_metadata.r#type,
            input_fields: blob_metadata.input_fields,
            snapshot_id: blob_metadata.snapshot_id,
            sequence_number: blob_metadata.sequence_number,
            data,
            properties: blob_metadata.properties,
        })
    }
}

#[cfg(test)]
mod tests {

    use crate::test_utils::{
        blob_0, blob_1, rust_uncompressed_metric_input_file,
        rust_zstd_compressed_metric_input_file, uncompressed_metric_file_metadata,
        zstd_compressed_metric_file_metadata,
    };
    use crate::PuffinReader;

    #[tokio::test]
    async fn test_puffin_reader_uncompressed_metric_data() {
        let input_file = rust_uncompressed_metric_input_file();
        let mut puffin_reader = PuffinReader::new(input_file);

        let file_metadata = puffin_reader.file_metadata().await.unwrap().clone();
        assert_eq!(file_metadata, uncompressed_metric_file_metadata());

        assert_eq!(
            puffin_reader
                .blob(file_metadata.blobs.first().unwrap().clone())
                .await
                .unwrap(),
            blob_0()
        );

        assert_eq!(
            puffin_reader
                .blob(file_metadata.blobs.get(1).unwrap().clone())
                .await
                .unwrap(),
            blob_1(),
        )
    }

    #[tokio::test]
    async fn test_puffin_reader_zstd_compressed_metric_data() {
        let input_file = rust_zstd_compressed_metric_input_file();
        let mut puffin_reader = PuffinReader::new(input_file);

        let file_metadata = puffin_reader.file_metadata().await.unwrap().clone();
        assert_eq!(file_metadata, zstd_compressed_metric_file_metadata());

        assert_eq!(
            puffin_reader
                .blob(file_metadata.blobs.first().unwrap().clone())
                .await
                .unwrap(),
            blob_0()
        );

        assert_eq!(
            puffin_reader
                .blob(file_metadata.blobs.get(1).unwrap().clone())
                .await
                .unwrap(),
            blob_1(),
        )
    }
}
