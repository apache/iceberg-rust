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

//! Deletion vector writer for creating Puffin files with deletion vectors

use std::collections::HashMap;

use roaring::RoaringTreemap;

use crate::delete_vector::DeleteVector;
use crate::io::FileIO;
use crate::puffin::{
    Blob, CompressionCodec, PuffinWriter, serialize_deletion_vector, DELETION_VECTOR_V1,
};
use crate::Result;

/// Builder for creating Puffin files containing deletion vectors
pub struct DeletionVectorWriter {
    file_io: FileIO,
    snapshot_id: i64,
    sequence_number: i64,
    compress_footer: bool,
}

impl DeletionVectorWriter {
    /// Create a new deletion vector writer
    pub fn new(file_io: FileIO, snapshot_id: i64, sequence_number: i64) -> Self {
        Self {
            file_io,
            snapshot_id,
            sequence_number,
            compress_footer: false,
        }
    }

    /// Enable footer compression
    pub fn with_footer_compression(mut self, compress: bool) -> Self {
        self.compress_footer = compress;
        self
    }

    /// Write deletion vectors for multiple data files to a Puffin file
    ///
    /// Returns a map of data file paths to their deletion vector metadata
    /// (offset, length) within the Puffin file.
    pub async fn write_deletion_vectors(
        &self,
        puffin_path: &str,
        deletion_vectors: HashMap<String, DeleteVector>,
    ) -> Result<HashMap<String, DeletionVectorMetadata>> {
        let output_file = self.file_io.new_output(puffin_path)?;
        let mut puffin_writer =
            PuffinWriter::new(&output_file, HashMap::new(), self.compress_footer).await?;

        let mut metadata_map = HashMap::new();
        let mut current_offset = 4i64; // Puffin header is "PFA1" (4 bytes)

        for (data_file_path, delete_vector) in deletion_vectors {
            let dv_bytes = serialize_deletion_vector(delete_vector.inner())?;

            let mut properties = HashMap::new();
            properties.insert("referenced-data-file".to_string(), data_file_path.clone());
            properties.insert("cardinality".to_string(), delete_vector.len().to_string());

            let blob = Blob::builder()
                .r#type(DELETION_VECTOR_V1.to_string())
                .fields(vec![])
                .snapshot_id(self.snapshot_id)
                .sequence_number(self.sequence_number)
                .data(dv_bytes.clone())
                .properties(properties)
                .build();

            let content_size = dv_bytes.len() as i64;

            puffin_writer
                .add(blob, CompressionCodec::None)
                .await?;

            metadata_map.insert(
                data_file_path,
                DeletionVectorMetadata {
                    offset: current_offset,
                    length: content_size,
                },
            );

            current_offset += content_size;
        }

        puffin_writer.close().await?;

        Ok(metadata_map)
    }

    /// Write a single deletion vector to a Puffin file
    pub async fn write_single_deletion_vector(
        &self,
        puffin_path: &str,
        data_file_path: &str,
        delete_vector: DeleteVector,
    ) -> Result<DeletionVectorMetadata> {
        let mut map = HashMap::new();
        map.insert(data_file_path.to_string(), delete_vector);

        let result = self.write_deletion_vectors(puffin_path, map).await?;

        result
            .get(data_file_path)
            .cloned()
            .ok_or_else(|| {
                crate::Error::new(
                    crate::ErrorKind::Unexpected,
                    "Failed to retrieve deletion vector metadata",
                )
            })
    }

    /// Create a deletion vector from a collection of row positions
    pub fn create_deletion_vector<I: IntoIterator<Item = u64>>(
        positions: I,
    ) -> Result<DeleteVector> {
        let mut treemap = RoaringTreemap::new();
        for pos in positions {
            treemap.insert(pos);
        }
        Ok(DeleteVector::new(treemap))
    }
}

/// Metadata about a deletion vector stored in a Puffin file
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletionVectorMetadata {
    /// Offset within the Puffin file where the deletion vector blob starts
    pub offset: i64,
    /// Length of the deletion vector blob in bytes
    pub length: i64,
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::puffin::PuffinReader;

    #[tokio::test]
    async fn test_write_single_deletion_vector() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path();
        let file_io = FileIO::from_path(table_location.as_os_str().to_str().unwrap())
            .unwrap()
            .build()
            .unwrap();

        let puffin_path = format!("{}/test.puffin", table_location.to_str().unwrap());
        let data_file_path = format!("{}/data.parquet", table_location.to_str().unwrap());

        // Create deletion vector
        let positions = vec![0u64, 5, 10, 100, 1000];
        let delete_vector = DeletionVectorWriter::create_deletion_vector(positions.clone())
            .unwrap();

        // Write to Puffin file
        let writer = DeletionVectorWriter::new(file_io.clone(), 1, 1);
        let metadata = writer
            .write_single_deletion_vector(&puffin_path, &data_file_path, delete_vector)
            .await
            .unwrap();

        // Verify metadata
        assert_eq!(metadata.offset, 4); // After "PFA1" header
        assert!(metadata.length > 0);

        // Read back and verify
        let input_file = file_io.new_input(&puffin_path).unwrap();
        let puffin_reader = PuffinReader::new(input_file);
        let file_metadata = puffin_reader.file_metadata().await.unwrap();

        assert_eq!(file_metadata.blobs.len(), 1);
        let blob_metadata = &file_metadata.blobs[0];
        assert_eq!(blob_metadata.r#type, DELETION_VECTOR_V1);
        assert_eq!(blob_metadata.offset, metadata.offset as u64);
        assert_eq!(blob_metadata.length, metadata.length as u64);

        // Verify the blob content
        let blob = puffin_reader.blob(blob_metadata).await.unwrap();
        assert_eq!(blob.blob_type(), DELETION_VECTOR_V1);

        // Deserialize and verify positions
        use crate::puffin::deserialize_deletion_vector;
        let loaded_treemap = deserialize_deletion_vector(&blob.data).unwrap();
        let loaded_dv = DeleteVector::new(loaded_treemap);

        assert_eq!(loaded_dv.len(), positions.len() as u64);
        for pos in positions {
            assert!(loaded_dv.iter().any(|p| p == pos));
        }
    }

    #[tokio::test]
    async fn test_write_multiple_deletion_vectors() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path();
        let file_io = FileIO::from_path(table_location.as_os_str().to_str().unwrap())
            .unwrap()
            .build()
            .unwrap();

        let puffin_path = format!("{}/multi.puffin", table_location.to_str().unwrap());

        // Create multiple deletion vectors
        let mut deletion_vectors = HashMap::new();

        let data_file_1 = format!("{}/data1.parquet", table_location.to_str().unwrap());
        let dv1 = DeletionVectorWriter::create_deletion_vector(vec![0, 1, 2]).unwrap();
        deletion_vectors.insert(data_file_1.clone(), dv1);

        let data_file_2 = format!("{}/data2.parquet", table_location.to_str().unwrap());
        let dv2 = DeletionVectorWriter::create_deletion_vector(vec![10, 20, 30, 40]).unwrap();
        deletion_vectors.insert(data_file_2.clone(), dv2);

        let data_file_3 = format!("{}/data3.parquet", table_location.to_str().unwrap());
        let dv3 = DeletionVectorWriter::create_deletion_vector(vec![100, 200]).unwrap();
        deletion_vectors.insert(data_file_3.clone(), dv3);

        // Write to Puffin file
        let writer = DeletionVectorWriter::new(file_io.clone(), 1, 1);
        let metadata_map = writer
            .write_deletion_vectors(&puffin_path, deletion_vectors)
            .await
            .unwrap();

        // Verify all metadata entries
        assert_eq!(metadata_map.len(), 3);
        assert!(metadata_map.contains_key(&data_file_1));
        assert!(metadata_map.contains_key(&data_file_2));
        assert!(metadata_map.contains_key(&data_file_3));

        // Read back and verify
        let input_file = file_io.new_input(&puffin_path).unwrap();
        let puffin_reader = PuffinReader::new(input_file);
        let file_metadata = puffin_reader.file_metadata().await.unwrap();

        assert_eq!(file_metadata.blobs.len(), 3);

        // Verify each blob
        for blob_metadata in &file_metadata.blobs {
            assert_eq!(blob_metadata.r#type, DELETION_VECTOR_V1);
            let blob = puffin_reader.blob(blob_metadata).await.unwrap();

            let referenced_file = blob
                .properties()
                .get("referenced-data-file")
                .unwrap();

            // Verify cardinality
            let cardinality: u64 = blob
                .properties()
                .get("cardinality")
                .unwrap()
                .parse()
                .unwrap();

            let expected_cardinality = if referenced_file.contains("data1") {
                3
            } else if referenced_file.contains("data2") {
                4
            } else {
                2
            };

            assert_eq!(cardinality, expected_cardinality);
        }
    }

    #[tokio::test]
    async fn test_write_with_64bit_positions() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path();
        let file_io = FileIO::from_path(table_location.as_os_str().to_str().unwrap())
            .unwrap()
            .build()
            .unwrap();

        let puffin_path = format!("{}/64bit.puffin", table_location.to_str().unwrap());
        let data_file_path = format!("{}/data.parquet", table_location.to_str().unwrap());

        // Create deletion vector with 64-bit positions
        let positions = vec![
            0u64,
            u32::MAX as u64,
            (1u64 << 32) + 42,
            (2u64 << 32) + 1000,
        ];
        let delete_vector =
            DeletionVectorWriter::create_deletion_vector(positions.clone()).unwrap();

        // Write to Puffin file
        let writer = DeletionVectorWriter::new(file_io.clone(), 1, 1);
        writer
            .write_single_deletion_vector(&puffin_path, &data_file_path, delete_vector)
            .await
            .unwrap();

        // Read back and verify
        let input_file = file_io.new_input(&puffin_path).unwrap();
        let puffin_reader = PuffinReader::new(input_file);
        let file_metadata = puffin_reader.file_metadata().await.unwrap();

        let blob = puffin_reader
            .blob(&file_metadata.blobs[0])
            .await
            .unwrap();

        use crate::puffin::deserialize_deletion_vector;
        let loaded_treemap = deserialize_deletion_vector(&blob.data).unwrap();
        let loaded_dv = DeleteVector::new(loaded_treemap);

        assert_eq!(loaded_dv.len(), positions.len() as u64);
        for pos in positions {
            assert!(
                loaded_dv.iter().any(|p| p == pos),
                "Position {} not found",
                pos
            );
        }
    }
}
