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

//! This module provides the [`DeletionVectorFileWriter`].
//!
//! A deletion vector (DV) is the V3 merge-on-read counterpart to the position-delete file
//! ([`PositionDeleteFileWriter`](super::position_delete_writer)): instead of a parquet file of
//! `(file_path, pos)` rows, the deleted positions for ONE data file are stored as a single
//! `deletion-vector-v1` blob inside a Puffin file. V3 tables require DVs rather than position-delete
//! files. The blob bytes are produced by [`DeleteVector::serialize`] and match Java
//! `org.apache.iceberg.deletes.BitmapPositionDeleteIndex.serialize` exactly, so a DV written here is
//! readable by Java and by our own read side ([`DeleteVector::deserialize`] /
//! [`crate::arrow::caching_delete_file_loader`]).
//!
//! # The returned [`DataFile`]
//!
//! Mirroring Java [`BaseDVFileWriter.createDV`](https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/deletes/BaseDVFileWriter.java),
//! the produced delete entry is a [`DataFile`] with:
//!
//! - `content == PositionDeletes` and `file_format == Puffin`,
//! - `file_path` = the Puffin file's path and `file_size_in_bytes` = the full Puffin file size,
//! - `referenced_data_file` = the single data file the deletes apply to (required for DVs),
//! - `content_offset` / `content_size_in_bytes` = the blob's `offset` / `length` **in the Puffin
//!   footer** — these MUST index the blob exactly (a spec requirement; readers seek the blob by them),
//! - `record_count` = the cardinality of the delete vector.
//!
//! # Scope
//!
//! This writer writes ONE DV blob for ONE referenced data file per Puffin file. Batching multiple data
//! files' DVs into one Puffin file (Java's `BaseDVFileWriter` accumulating across `delete(...)` calls)
//! is out of scope here.

use std::collections::HashMap;

use crate::delete_vector::DeleteVector;
use crate::io::OutputFile;
use crate::metadata_columns::RESERVED_FIELD_ID_POS;
use crate::puffin::{Blob, CompressionCodec, DELETION_VECTOR_V1, PuffinWriter};
use crate::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, PartitionKey, Struct,
};
use crate::{Error, ErrorKind, Result};

/// Property key recording the data file a deletion vector applies to (Java
/// `BaseDVFileWriter.REFERENCED_DATA_FILE_KEY`).
const REFERENCED_DATA_FILE_KEY: &str = "referenced-data-file";
/// Property key recording the deletion vector's cardinality (Java `BaseDVFileWriter.CARDINALITY_KEY`).
const CARDINALITY_KEY: &str = "cardinality";
/// Snapshot id / sequence number stored on a DV blob — `-1` means "inherited from the snapshot that
/// commits this DV" (Java passes `-1` for both in `BaseDVFileWriter.toBlob`).
const INHERITED_BLOB_METADATA: i64 = -1;

/// Writes a single deletion-vector (`deletion-vector-v1`) Puffin file for one referenced data file.
///
/// See the [module docs](self) for the byte format and the shape of the returned [`DataFile`].
pub struct DeletionVectorFileWriter {
    output_file: OutputFile,
    partition_key: Option<PartitionKey>,
}

impl DeletionVectorFileWriter {
    /// Creates a new deletion-vector writer that will write its Puffin file to `output_file`.
    ///
    /// `partition_key` carries the partition values + spec for the resulting [`DataFile`]; pass `None`
    /// for an unpartitioned table.
    pub fn new(output_file: OutputFile, partition_key: Option<PartitionKey>) -> Self {
        Self {
            output_file,
            partition_key,
        }
    }

    /// Writes the deletion vector `delete_vector` (the deleted positions for the single data file at
    /// `referenced_data_file`) as a `deletion-vector-v1` blob into the Puffin file, and returns the
    /// resulting [`DataFile`].
    ///
    /// The blob is written uncompressed (matching Java, which passes `null` compression for DV blobs).
    /// The returned [`DataFile`]'s `content_offset` / `content_size_in_bytes` are exactly the blob's
    /// offset / length in the written Puffin file.
    ///
    /// # Errors
    ///
    /// Returns an error if the delete vector cannot be serialized, the Puffin file cannot be written,
    /// or the resulting [`DataFile`] cannot be built.
    pub async fn write(
        self,
        referenced_data_file: impl Into<String>,
        delete_vector: &DeleteVector,
    ) -> Result<DataFile> {
        let referenced_data_file = referenced_data_file.into();
        let cardinality = delete_vector.len();

        // Serialize the delete vector into the interop-critical DV blob bytes.
        let blob_data = delete_vector.serialize()?;

        // Build the deletion-vector blob (mirrors Java `BaseDVFileWriter.toBlob`).
        let mut properties = HashMap::with_capacity(2);
        properties.insert(
            REFERENCED_DATA_FILE_KEY.to_string(),
            referenced_data_file.clone(),
        );
        properties.insert(CARDINALITY_KEY.to_string(), cardinality.to_string());

        let blob = Blob::builder()
            .r#type(DELETION_VECTOR_V1.to_string())
            // The DV blob is computed over the row-position (`_pos`) metadata column.
            .fields(vec![RESERVED_FIELD_ID_POS])
            .snapshot_id(INHERITED_BLOB_METADATA)
            .sequence_number(INHERITED_BLOB_METADATA)
            .data(blob_data)
            .properties(properties)
            .build();

        // Write the blob into a fresh Puffin file and capture its footer offset/length.
        let mut puffin_writer = PuffinWriter::new(&self.output_file, HashMap::new(), false).await?;
        let blob_metadata = puffin_writer.add(blob, CompressionCodec::None).await?;
        let content_offset = blob_metadata.offset();
        let content_size_in_bytes = blob_metadata.length();
        // `close()` returns the full Puffin file size (header + blob + footer).
        let puffin_file_size = puffin_writer.close().await?;

        // Build the DV `DataFile` (mirrors Java `BaseDVFileWriter.createDV`).
        let mut builder = DataFileBuilder::default();
        builder
            .content(DataContentType::PositionDeletes)
            .file_format(DataFileFormat::Puffin)
            .file_path(self.output_file.location().to_string())
            .file_size_in_bytes(puffin_file_size)
            .record_count(cardinality)
            .referenced_data_file(Some(referenced_data_file))
            .content_offset(Some(i64::try_from(content_offset).map_err(|err| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "deletion vector blob offset exceeds i64",
                )
                .with_source(err)
            })?))
            .content_size_in_bytes(Some(i64::try_from(content_size_in_bytes).map_err(
                |err| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "deletion vector blob length exceeds i64",
                    )
                    .with_source(err)
                },
            )?));

        match self.partition_key.as_ref() {
            Some(partition_key) => {
                builder.partition(partition_key.data().clone());
                builder.partition_spec_id(partition_key.spec().spec_id());
            }
            None => {
                builder.partition(Struct::empty());
            }
        }

        builder.build().map_err(|err| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Failed to build deletion vector data file: {err}"),
            )
        })
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use tempfile::TempDir;

    use super::DeletionVectorFileWriter;
    use crate::delete_vector::DeleteVector;
    use crate::io::FileIO;
    use crate::puffin::{DELETION_VECTOR_V1, PuffinReader};
    use crate::spec::{DataContentType, DataFileFormat};

    fn dv_from(positions: &[u64]) -> DeleteVector {
        let mut dv = DeleteVector::default();
        for &pos in positions {
            dv.insert(pos);
        }
        dv
    }

    /// The crown-jewel round-trip for the DV WRITER. Risk pinned: a wrong `content_offset` /
    /// `content_size_in_bytes` (or wrong blob bytes) means a reader cannot locate / decode the DV, so
    /// the deletes are silently unreadable by Java and by our own scan.
    ///
    /// Write a DV for positions {1,3}, then:
    /// - assert the `DataFile` shape (content, format, referenced file, record_count),
    /// - prove `content_offset`/`content_size` EXACTLY index the Puffin blob (read the blob via those
    ///   bytes and via the Puffin footer; both must agree), and
    /// - deserialize the blob back into a `DeleteVector` equal to {1,3}.
    #[tokio::test]
    async fn test_deletion_vector_writer_round_trips_and_offsets_index_the_blob() {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let puffin_path = temp_dir
            .path()
            .join("deletes.puffin")
            .to_str()
            .unwrap()
            .to_string();
        let output_file = file_io.new_output(&puffin_path).unwrap();

        let referenced = "s3://bucket/data/1.parquet";
        let dv = dv_from(&[1, 3]);

        let writer = DeletionVectorFileWriter::new(output_file, None);
        let data_file = writer.write(referenced, &dv).await.unwrap();

        // DataFile shape (Java `BaseDVFileWriter.createDV`).
        assert_eq!(data_file.content, DataContentType::PositionDeletes);
        assert_eq!(data_file.file_format, DataFileFormat::Puffin);
        assert_eq!(data_file.file_path, puffin_path);
        assert_eq!(data_file.referenced_data_file.as_deref(), Some(referenced));
        assert_eq!(data_file.record_count, 2);
        let content_offset = data_file.content_offset.expect("content_offset required");
        let content_size = data_file
            .content_size_in_bytes
            .expect("content_size required");

        // Prove content_offset/content_size EXACTLY index the Puffin blob: the Puffin footer's blob
        // metadata must carry the same offset/length the DataFile recorded.
        let input_file = file_io.new_input(&puffin_path).unwrap();
        let puffin_reader = PuffinReader::new(input_file);
        let file_metadata = puffin_reader.file_metadata().await.unwrap().clone();
        assert_eq!(file_metadata.blobs().len(), 1);
        let blob_metadata = file_metadata.blobs().first().unwrap();
        assert_eq!(blob_metadata.blob_type(), DELETION_VECTOR_V1);
        assert_eq!(blob_metadata.offset() as i64, content_offset);
        assert_eq!(blob_metadata.length() as i64, content_size);

        // Read the blob bytes DIRECTLY by content_offset/content_size (slicing the raw Puffin file at
        // exactly the recorded offset/length) and deserialize → {1,3}. This proves the offset/size index
        // the blob, not merely that a blob exists somewhere in the file.
        let whole_file = file_io
            .new_input(&puffin_path)
            .unwrap()
            .read()
            .await
            .unwrap();
        let start = content_offset as usize;
        let end = start + content_size as usize;
        let blob_bytes = whole_file.slice(start..end);
        let restored = DeleteVector::deserialize(&blob_bytes).unwrap();
        let mut restored_positions: Vec<u64> = restored.iter().collect();
        restored_positions.sort_unstable();
        assert_eq!(restored_positions, vec![1, 3]);

        // And via the high-level Puffin read path the blob data is the same bytes.
        let blob = puffin_reader.blob(blob_metadata).await.unwrap();
        assert_eq!(blob.data(), blob_bytes.as_ref());
    }

    /// The blob's recorded `referenced-data-file` / `cardinality` properties must match the inputs —
    /// Java records both and downstream tooling reads them.
    #[tokio::test]
    async fn test_deletion_vector_blob_carries_referenced_file_and_cardinality_properties() {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let puffin_path = temp_dir
            .path()
            .join("deletes.puffin")
            .to_str()
            .unwrap()
            .to_string();
        let output_file = file_io.new_output(&puffin_path).unwrap();

        let referenced = "s3://bucket/data/9.parquet";
        let dv = dv_from(&[2, 4, 6, 8]);

        let writer = DeletionVectorFileWriter::new(output_file, None);
        let data_file = writer.write(referenced, &dv).await.unwrap();
        assert_eq!(data_file.record_count, 4);

        let input_file = file_io.new_input(&puffin_path).unwrap();
        let puffin_reader = PuffinReader::new(input_file);
        let file_metadata = puffin_reader.file_metadata().await.unwrap().clone();
        let blob_metadata = file_metadata.blobs().first().unwrap();

        let props: &HashMap<String, String> = blob_metadata.properties();
        assert_eq!(
            props.get("referenced-data-file").map(String::as_str),
            Some(referenced)
        );
        assert_eq!(props.get("cardinality").map(String::as_str), Some("4"));
    }

    /// A DV crossing the 32-bit boundary must round-trip through the full writer → Puffin → reader →
    /// deserialize chain, not just the in-memory serializer.
    #[tokio::test]
    async fn test_deletion_vector_writer_round_trips_high_positions() {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let puffin_path = temp_dir
            .path()
            .join("deletes.puffin")
            .to_str()
            .unwrap()
            .to_string();
        let output_file = file_io.new_output(&puffin_path).unwrap();

        let positions = [1u64, 3, 5, 1000, 2_000_000_000];
        let dv = dv_from(&positions);

        let writer = DeletionVectorFileWriter::new(output_file, None);
        let data_file = writer.write("s3://b/d/huge.parquet", &dv).await.unwrap();
        assert_eq!(data_file.record_count, positions.len() as u64);

        let input_file = file_io.new_input(&puffin_path).unwrap();
        let puffin_reader = PuffinReader::new(input_file);
        let file_metadata = puffin_reader.file_metadata().await.unwrap().clone();
        let blob = puffin_reader
            .blob(file_metadata.blobs().first().unwrap())
            .await
            .unwrap();
        let restored = DeleteVector::deserialize(blob.data()).unwrap();
        let mut got: Vec<u64> = restored.iter().collect();
        got.sort_unstable();
        assert_eq!(got, positions.to_vec());
    }
}
