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

//! This module provides the [`DVFileWriter`] — the deletion-vector (V3 Puffin DV) file writer.
//!
//! The Rust mirror of Java `BaseDVFileWriter` (`core/.../deletes/BaseDVFileWriter.java`): deleted
//! positions are accumulated PER REFERENCED DATA FILE, and `close()` writes them all into **one
//! Puffin file** holding one uncompressed `deletion-vector-v1` blob per referenced data file,
//! returning one [`DeleteFile`](DataFile) metadata entry per referenced file.
//!
//! # The on-disk contract (what Java must be able to read)
//!
//! Per `BaseDVFileWriter.toBlob` (L173-186), each blob carries:
//!
//! * blob type `deletion-vector-v1`, **uncompressed** (Java passes a null codec);
//! * `fields` = `[2147483645]` — the reserved `ROW_POSITION` (`_pos`) column id,
//!   `MetadataColumns.ROW_POSITION.fieldId()` = `Integer.MAX_VALUE - 2` (MetadataColumns.java
//!   L39-44; *not* the `pos` delete-file column 2147483545);
//! * `snapshot-id` = −1 and `sequence-number` = −1 (inherited at commit time, L177-178);
//! * properties `referenced-data-file` (the data file path) and `cardinality` (the number of
//!   deleted positions), L52-53 + L181-185.
//!
//! Per `BaseDVFileWriter.createDV` (L145-159), each returned [`DataFile`] carries: position-delete
//! content, PUFFIN format, the SHARED Puffin path + file size, the partition captured for that
//! data file, `referenced_data_file`, `content_offset`/`content_size_in_bytes` = the blob's
//! coordinates from the Puffin footer metadata, and `record_count` = the cardinality.
//!
//! # Determinism
//!
//! Blobs are written in **sorted referenced-data-file-path order**, and the returned `DeleteFile`s
//! follow the same order. Java iterates a `HashMap` here, so blob order is NOT part of the Java
//! contract — the sorting exists so that two identical writer runs produce byte-identical Puffin
//! files (our own reproducibility/testing requirement).
//!
//! # Deferred to D3 (the commit-path increment) — LOUD NOTE
//!
//! Java's `close()` additionally merges PREVIOUS deletes for each path (`loadPreviousDeletes`,
//! L117-126) and collects the superseded file-scoped delete files as `rewrittenDeleteFiles` for
//! the commit to replace. That is a COMMIT-path concern (it needs the table's live delete index);
//! this writer takes only FRESH positions. Callers that need previous-delete merge semantics must
//! wait for D3, which will also wire `RowDelta` DV adds and the V2-forbids/V3-requires gating.

use std::collections::BTreeMap;

use crate::delete_vector::DeleteVector;
use crate::io::OutputFile;
use crate::metadata_columns::RESERVED_FIELD_ID_POS;
use crate::puffin::{Blob, CompressionCodec, DELETION_VECTOR_V1, PuffinWriter};
use crate::spec::{DataContentType, DataFile, DataFileBuilder, DataFileFormat, PartitionKey};
use crate::{Error, ErrorKind, Result};

/// The largest position a deletion vector may record, mirroring Java
/// `RoaringPositionBitmap.MAX_POSITION` = `toPosition(Integer.MAX_VALUE - 1, Integer.MIN_VALUE)`
/// (RoaringPositionBitmap.java L53): the high 32 bits (the bitmap key) may be at most
/// `i32::MAX - 1`, and the low word of the constant is `Integer.MIN_VALUE` masked unsigned
/// (`0x8000_0000`) — a Java quirk faithfully mirrored, NOT `0xFFFF_FFFF`.
pub const DV_MAX_POSITION: u64 = ((i32::MAX as u64 - 1) << 32) | 0x8000_0000;

/// Snapshot id / sequence number recorded on a DV blob: −1 means "inherited at commit time"
/// (Java `BaseDVFileWriter.toBlob` L177-178).
const INHERITED: i64 = -1;

/// Puffin blob property naming the data file a deletion vector applies to
/// (Java `BaseDVFileWriter.REFERENCED_DATA_FILE_KEY`, L52).
const REFERENCED_DATA_FILE_PROPERTY: &str = "referenced-data-file";

/// Puffin blob property carrying the number of deleted positions
/// (Java `BaseDVFileWriter.CARDINALITY_KEY`, L53).
const CARDINALITY_PROPERTY: &str = "cardinality";

/// Per-referenced-data-file accumulation state (Java `BaseDVFileWriter.Deletes`, L188-216): the
/// position set plus the partition context captured at the FIRST `delete` call for the path.
#[derive(Debug)]
struct DeletesForDataFile {
    positions: DeleteVector,
    partition_key: Option<PartitionKey>,
}

/// Writer for deletion vectors (V3 Puffin DVs), mirroring Java `BaseDVFileWriter`.
///
/// Accumulate deleted positions with [`delete`](Self::delete), then call
/// [`close`](Self::close) to write ONE Puffin file and obtain the per-data-file `DeleteFile`
/// metadata ready for a (future, D3) `RowDelta` commit. If no positions were recorded, `close`
/// writes NO file at all (Java L106-109: "Only create PuffinWriter if there are deletes").
#[derive(Debug)]
pub struct DVFileWriter {
    /// Where the Puffin file goes. The underlying file is only created when `close()` actually
    /// has deletes to write (Java defers via a `Supplier<OutputFile>`; an [`OutputFile`] is
    /// equally lazy — no bytes hit storage until a writer is opened on it).
    output_file: OutputFile,
    /// Per referenced data file path, in sorted order (see the module docs on determinism).
    deletes_by_path: BTreeMap<String, DeletesForDataFile>,
}

impl DVFileWriter {
    /// Creates a new `DVFileWriter` that will write its single Puffin file to `output_file`
    /// (only if at least one position is deleted before `close`).
    pub fn new(output_file: OutputFile) -> Self {
        Self {
            output_file,
            deletes_by_path: BTreeMap::new(),
        }
    }

    /// Marks `position` of the data file at `data_file_path` as deleted, in the partition
    /// context `partition_key` (`None` for an unpartitioned table).
    ///
    /// Mirrors Java `BaseDVFileWriter.delete(path, pos, spec, partition)` (L73-79): the partition
    /// context is captured at the FIRST call for a given path (`computeIfAbsent`) — later calls
    /// for the same path only add positions, their partition argument is ignored.
    ///
    /// # Errors
    ///
    /// Rejects `position > DV_MAX_POSITION`, mirroring Java
    /// `RoaringPositionBitmap.validatePosition` (L342-348) — such a position is unrepresentable
    /// in the serialized dense bitmap layout.
    pub fn delete(
        &mut self,
        data_file_path: &str,
        position: u64,
        partition_key: Option<&PartitionKey>,
    ) -> Result<()> {
        if position > DV_MAX_POSITION {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Deletion vector supports positions that are >= 0 and <= {DV_MAX_POSITION}: \
                     {position} (Java RoaringPositionBitmap.MAX_POSITION)"
                ),
            ));
        }

        let deletes = self
            .deletes_by_path
            .entry(data_file_path.to_string())
            .or_insert_with(|| DeletesForDataFile {
                positions: DeleteVector::default(),
                partition_key: partition_key.cloned(),
            });
        deletes.positions.insert(position);
        Ok(())
    }

    /// Writes all accumulated deletion vectors into ONE Puffin file and returns one `DeleteFile`
    /// metadata entry per referenced data file (Java `BaseDVFileWriter.close` L99-143 +
    /// `createDV` L145-159).
    ///
    /// With no recorded deletes this writes NO file and returns an empty vec (Java L106-109).
    pub async fn close(self) -> Result<Vec<DataFile>> {
        if self.deletes_by_path.is_empty() {
            return Ok(Vec::new());
        }

        // One Puffin file for ALL the vectors. The footer is uncompressed; `created-by`
        // identifies this writer (Java sets `IcebergBuild.fullVersion()` — the value differs,
        // which is footer-cosmetic and reader-irrelevant).
        let mut puffin_writer = PuffinWriter::new(
            &self.output_file,
            std::collections::HashMap::from([(
                crate::puffin::CREATED_BY_PROPERTY.to_string(),
                format!("iceberg-rust {}", env!("CARGO_PKG_VERSION")),
            )]),
            false,
        )
        .await?;

        // One UNCOMPRESSED `deletion-vector-v1` blob per referenced data file, in sorted path
        // order (determinism — see the module docs). The returned BlobMetadata carries the
        // offset/length the DeleteFile must reference.
        let mut blob_coordinates: Vec<(u64, u64)> = Vec::with_capacity(self.deletes_by_path.len());
        for (data_file_path, deletes) in &self.deletes_by_path {
            let blob_data = deletes.positions.serialize_deletion_vector_v1()?;
            let blob = Blob::builder()
                .r#type(DELETION_VECTOR_V1.to_string())
                .fields(vec![RESERVED_FIELD_ID_POS])
                .snapshot_id(INHERITED)
                .sequence_number(INHERITED)
                .data(blob_data)
                .properties(std::collections::HashMap::from([
                    (
                        REFERENCED_DATA_FILE_PROPERTY.to_string(),
                        data_file_path.clone(),
                    ),
                    (
                        CARDINALITY_PROPERTY.to_string(),
                        deletes.positions.len().to_string(),
                    ),
                ]))
                .build();
            let blob_metadata = puffin_writer.add(blob, CompressionCodec::None).await?;
            blob_coordinates.push((blob_metadata.offset(), blob_metadata.length()));
        }

        // "DVs share the Puffin path and file size but have different offsets" (Java L132-134).
        let puffin_file_size = puffin_writer.close().await?;
        let puffin_path = self.output_file.location().to_string();

        self.deletes_by_path
            .iter()
            .zip(blob_coordinates)
            .map(
                |((data_file_path, deletes), (content_offset, content_size_in_bytes))| {
                    Self::create_dv_metadata(
                        &puffin_path,
                        puffin_file_size,
                        data_file_path,
                        deletes,
                        content_offset,
                        content_size_in_bytes,
                    )
                },
            )
            .collect()
    }

    /// Builds the `DeleteFile` metadata for one deletion vector — the Rust mirror of Java
    /// `BaseDVFileWriter.createDV` (L145-159): position-delete content, PUFFIN format, the
    /// shared Puffin path + file size, the captured partition, the blob coordinates, and
    /// `record_count` = the cardinality.
    fn create_dv_metadata(
        puffin_path: &str,
        puffin_file_size: u64,
        data_file_path: &str,
        deletes: &DeletesForDataFile,
        content_offset: u64,
        content_size_in_bytes: u64,
    ) -> Result<DataFile> {
        let to_signed = |value: u64, what: &str| -> Result<i64> {
            i64::try_from(value).map_err(|_| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Deletion vector {what} {value} does not fit in i64"),
                )
            })
        };

        let mut builder = DataFileBuilder::default();
        builder
            .content(DataContentType::PositionDeletes)
            .file_format(DataFileFormat::Puffin)
            .file_path(puffin_path.to_string())
            .file_size_in_bytes(puffin_file_size)
            .record_count(deletes.positions.len())
            .referenced_data_file(Some(data_file_path.to_string()))
            .content_offset(Some(to_signed(content_offset, "content_offset")?))
            .content_size_in_bytes(Some(to_signed(
                content_size_in_bytes,
                "content_size_in_bytes",
            )?));
        if let Some(partition_key) = &deletes.partition_key {
            builder
                .partition(partition_key.data().clone())
                .partition_spec_id(partition_key.spec().spec_id());
        }
        builder.build().map_err(|error| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Failed to build deletion vector DeleteFile metadata: {error}"),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::*;
    use crate::arrow::caching_delete_file_loader::CachingDeleteFileLoader;
    use crate::io::FileIO;
    use crate::scan::FileScanTaskDeleteFile;
    use crate::spec::{Literal, NestedField, PartitionSpec, PrimitiveType, Schema, Struct, Type};

    fn output_file(file_io: &FileIO, dir: &TempDir, name: &str) -> OutputFile {
        let path = dir.path().join(name);
        file_io
            .new_output(path.to_str().expect("utf-8 temp path"))
            .expect("create output file")
    }

    /// Slice the written Puffin file at the DeleteFile's blob coordinates and decode the
    /// positions — exactly the ranged read the (D1) scan-side loader performs.
    fn decode_blob_at(puffin_bytes: &[u8], delete_file: &DataFile) -> Vec<u64> {
        let offset = usize::try_from(delete_file.content_offset().expect("offset present"))
            .expect("offset fits usize");
        let size = usize::try_from(
            delete_file
                .content_size_in_bytes()
                .expect("content size present"),
        )
        .expect("size fits usize");
        let vector =
            DeleteVector::deserialize_deletion_vector_v1(&puffin_bytes[offset..offset + size])
                .expect("blob at the recorded coordinates must decode");
        vector.iter().collect()
    }

    /// Risk pinned: the FULL per-DeleteFile metadata contract of Java `createDV` (L145-159) for
    /// a MULTI-file Puffin — wrong content/format/path/size resurrects rows or breaks the read;
    /// overlapping or swapped blob coordinates silently apply the WRONG vector to a data file.
    #[tokio::test]
    async fn test_dv_writer_multi_file_delete_files_carry_blob_coordinates() {
        let temp_dir = TempDir::new().expect("temp dir");
        let file_io = FileIO::new_with_fs();
        let mut writer = DVFileWriter::new(output_file(&file_io, &temp_dir, "deletes.puffin"));

        // Insertion order is deliberately NOT sorted; the output must be (sorted by path).
        writer
            .delete("s3://b/data/b.parquet", 7, None)
            .expect("delete");
        writer
            .delete("s3://b/data/a.parquet", 0, None)
            .expect("delete");
        writer
            .delete("s3://b/data/a.parquet", 3, None)
            .expect("delete");
        writer
            .delete("s3://b/data/a.parquet", (1u64 << 32) + 1, None)
            .expect("delete");
        let delete_files = writer.close().await.expect("close");

        assert_eq!(
            delete_files.len(),
            2,
            "one DeleteFile per referenced data file"
        );
        let a = &delete_files[0];
        let b = &delete_files[1];
        assert_eq!(
            a.referenced_data_file().as_deref(),
            Some("s3://b/data/a.parquet")
        );
        assert_eq!(
            b.referenced_data_file().as_deref(),
            Some("s3://b/data/b.parquet")
        );

        let puffin_path = a.file_path().to_string();
        let puffin_bytes = std::fs::read(&puffin_path).expect("read puffin file");
        for delete_file in [a, b] {
            assert_eq!(delete_file.content_type(), DataContentType::PositionDeletes);
            assert_eq!(delete_file.file_format(), DataFileFormat::Puffin);
            assert_eq!(delete_file.file_path(), puffin_path, "shared Puffin path");
            assert_eq!(
                delete_file.file_size_in_bytes(),
                puffin_bytes.len() as u64,
                "file_size_in_bytes must be the REAL on-disk Puffin size (footer included)"
            );
        }
        assert_eq!(a.record_count(), 3, "record_count == cardinality");
        assert_eq!(b.record_count(), 1);
        assert_ne!(
            a.content_offset(),
            b.content_offset(),
            "blobs must have distinct offsets"
        );

        // The blob at each DeleteFile's coordinates decodes to exactly that file's positions.
        assert_eq!(decode_blob_at(&puffin_bytes, a), vec![
            0,
            3,
            (1u64 << 32) + 1
        ]);
        assert_eq!(decode_blob_at(&puffin_bytes, b), vec![7]);
    }

    /// Risk pinned: "no deletes ⇒ NO Puffin file" (Java L106-109) — writing an empty Puffin
    /// would litter the table location with orphan files.
    #[tokio::test]
    async fn test_dv_writer_no_deletes_writes_no_file() {
        let temp_dir = TempDir::new().expect("temp dir");
        let file_io = FileIO::new_with_fs();
        let out = output_file(&file_io, &temp_dir, "empty.puffin");
        let path = out.location().to_string();

        let writer = DVFileWriter::new(out);
        let delete_files = writer.close().await.expect("close with no deletes");

        assert!(delete_files.is_empty());
        assert!(
            !std::path::Path::new(&path).exists(),
            "no Puffin file may be created when there are no deletes"
        );
    }

    /// Risk pinned: determinism — two runs over the same logical deletes (different insertion
    /// order) must produce an identical BLOB REGION (identical blob order, bytes, and
    /// coordinates) and a structurally identical footer. Java gives no such guarantee (HashMap
    /// iteration); the sorted blob order is OUR reproducibility contract.
    ///
    /// KNOWN RESIDUE (flagged, pre-existing): the footer JSON itself is NOT byte-deterministic —
    /// `PuffinWriter` serializes `BlobMetadata.properties` from a `HashMap`, so the
    /// `referenced-data-file`/`cardinality` key ORDER varies per process. Every reader (Java's
    /// Jackson included) parses the footer as JSON, key-order-insensitive. Fixing it would touch
    /// `puffin/metadata.rs`, outside this increment's file set.
    #[tokio::test]
    async fn test_dv_writer_deterministic_output_across_runs() {
        let temp_dir = TempDir::new().expect("temp dir");
        let file_io = FileIO::new_with_fs();

        let mut first = DVFileWriter::new(output_file(&file_io, &temp_dir, "first.puffin"));
        for (path, pos) in [
            ("p/x.parquet", 5u64),
            ("p/y.parquet", 9),
            ("p/x.parquet", 2),
        ] {
            first.delete(path, pos, None).expect("delete");
        }
        let first_files = first.close().await.expect("close first");

        let mut second = DVFileWriter::new(output_file(&file_io, &temp_dir, "second.puffin"));
        for (path, pos) in [
            ("p/y.parquet", 9u64),
            ("p/x.parquet", 2),
            ("p/x.parquet", 5),
        ] {
            second.delete(path, pos, None).expect("delete");
        }
        let second_files = second.close().await.expect("close second");

        // Identical blob coordinates per referenced data file...
        let coordinates = |files: &[DataFile]| -> Vec<(String, Option<i64>, Option<i64>)> {
            files
                .iter()
                .map(|f| {
                    (
                        f.referenced_data_file().expect("referenced path"),
                        f.content_offset(),
                        f.content_size_in_bytes(),
                    )
                })
                .collect()
        };
        assert_eq!(coordinates(&first_files), coordinates(&second_files));

        // ...an identical blob REGION (header magic through the last blob byte)...
        let blob_region_end = usize::try_from(
            first_files
                .iter()
                .map(|f| {
                    f.content_offset().expect("offset") + f.content_size_in_bytes().expect("size")
                })
                .max()
                .expect("at least one blob"),
        )
        .expect("fits usize");
        let first_bytes = std::fs::read(first_files[0].file_path()).expect("read first");
        let second_bytes = std::fs::read(second_files[0].file_path()).expect("read second");
        assert_eq!(
            first_bytes[..blob_region_end],
            second_bytes[..blob_region_end],
            "the same logical deletes must produce a byte-identical blob region"
        );

        // ...and a structurally identical footer (same blobs, offsets, properties).
        let first_footer = crate::puffin::FileMetadata::read(
            &file_io
                .new_input(first_files[0].file_path())
                .expect("input first"),
        )
        .await
        .expect("read first footer");
        let second_footer = crate::puffin::FileMetadata::read(
            &file_io
                .new_input(second_files[0].file_path())
                .expect("input second"),
        )
        .await
        .expect("read second footer");
        assert_eq!(first_footer, second_footer);
    }

    /// Risk pinned: the partition context is captured at the FIRST delete per path (Java
    /// `computeIfAbsent`, L74-79) and lands on the DeleteFile (`withPartition`, L152) — losing
    /// it would let partition pruning skip the DV's data file while keeping its deletes.
    #[tokio::test]
    async fn test_dv_writer_partition_captured_at_first_delete_per_path() {
        let temp_dir = TempDir::new().expect("temp dir");
        let file_io = FileIO::new_with_fs();

        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "category", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])
                .build()
                .expect("schema"),
        );
        let spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(3)
            .add_partition_field("category", "category", crate::spec::Transform::Identity)
            .expect("partition field")
            .build()
            .expect("spec");
        let partition_a = PartitionKey::new(
            spec.clone(),
            schema.clone(),
            Struct::from_iter([Some(Literal::string("a"))]),
        );
        let partition_b = PartitionKey::new(
            spec,
            schema,
            Struct::from_iter([Some(Literal::string("b"))]),
        );

        let mut writer = DVFileWriter::new(output_file(&file_io, &temp_dir, "deletes.puffin"));
        writer
            .delete("p/x.parquet", 1, Some(&partition_a))
            .expect("delete");
        // A later, DIFFERENT partition for the same path must be ignored (first capture wins).
        writer
            .delete("p/x.parquet", 2, Some(&partition_b))
            .expect("delete");
        let delete_files = writer.close().await.expect("close");

        assert_eq!(delete_files.len(), 1);
        assert_eq!(
            delete_files[0].partition(),
            &Struct::from_iter([Some(Literal::string("a"))]),
            "the FIRST delete's partition must be captured"
        );
        assert_eq!(delete_files[0].partition_spec_id, 3);
    }

    /// Risk pinned (D2 review): deleting the SAME position twice must merge — `record_count`
    /// (and the blob's cardinality) is the count of DISTINCT positions, like Java's
    /// `PositionDeleteIndex.delete` over a bitmap. Double-counting would corrupt the
    /// `record_count` stats every planner trusts.
    #[tokio::test]
    async fn test_dv_writer_duplicate_position_counted_once() {
        let temp_dir = TempDir::new().expect("temp dir");
        let file_io = FileIO::new_with_fs();
        let mut writer = DVFileWriter::new(output_file(&file_io, &temp_dir, "deletes.puffin"));

        writer.delete("p/x.parquet", 7, None).expect("delete");
        writer
            .delete("p/x.parquet", 7, None)
            .expect("duplicate delete is a no-op, not an error");
        let delete_files = writer.close().await.expect("close");

        assert_eq!(delete_files.len(), 1);
        assert_eq!(
            delete_files[0].record_count(),
            1,
            "record_count must be the DISTINCT-position cardinality"
        );
        let puffin_bytes = std::fs::read(delete_files[0].file_path()).expect("read puffin");
        assert_eq!(decode_blob_at(&puffin_bytes, &delete_files[0]), vec![7]);
    }

    /// Risk pinned: the position door — Java `RoaringPositionBitmap.validatePosition` rejects
    /// positions above MAX_POSITION at set() time; accepting one here would fail later (or write
    /// a key Java's reader rejects). Pins the boundary EXACTLY: MAX accepted, MAX+1 rejected.
    #[tokio::test]
    async fn test_dv_writer_rejects_position_above_java_max() {
        let temp_dir = TempDir::new().expect("temp dir");
        let file_io = FileIO::new_with_fs();
        let mut writer = DVFileWriter::new(output_file(&file_io, &temp_dir, "deletes.puffin"));

        writer
            .delete("p/x.parquet", DV_MAX_POSITION, None)
            .expect("MAX_POSITION itself is legal");
        let error = writer
            .delete("p/x.parquet", DV_MAX_POSITION + 1, None)
            .expect_err("MAX_POSITION + 1 must be rejected");
        assert!(
            error.to_string().contains("positions that are >= 0 and <="),
            "error must name the bound, got: {error}"
        );
    }

    /// Risk pinned (the D1+D2 end-to-end tie, and the mutation-(d) sentinel): a Puffin file
    /// written by THIS writer, loaded through D1's REAL caching loader using only the returned
    /// DeleteFile metadata (path + offsets + record_count), must yield exactly the deleted
    /// positions under each referenced data file. An offset off by even the 4-byte header magic
    /// fails the framing/CRC here.
    #[tokio::test]
    async fn test_dv_writer_round_trips_through_d1_loader() {
        let temp_dir = TempDir::new().expect("temp dir");
        let file_io = FileIO::new_with_fs();
        let mut writer = DVFileWriter::new(output_file(&file_io, &temp_dir, "deletes.puffin"));

        let data_file_x = "mem://data/x.parquet";
        let data_file_y = "mem://data/y.parquet";
        for pos in [0u64, 5, (1u64 << 32) + 7] {
            writer.delete(data_file_x, pos, None).expect("delete x");
        }
        for pos in 100u64..200 {
            writer.delete(data_file_y, pos, None).expect("delete y");
        }
        let delete_files = writer.close().await.expect("close");

        let tasks: Vec<FileScanTaskDeleteFile> = delete_files
            .iter()
            .map(|delete_file| FileScanTaskDeleteFile {
                file_path: delete_file.file_path().to_string(),
                file_size_in_bytes: delete_file.file_size_in_bytes(),
                file_type: delete_file.content_type(),
                partition_spec_id: delete_file.partition_spec_id,
                equality_ids: None,
                file_format: delete_file.file_format(),
                referenced_data_file: delete_file.referenced_data_file(),
                content_offset: delete_file.content_offset(),
                content_size_in_bytes: delete_file.content_size_in_bytes(),
                record_count: Some(delete_file.record_count()),
            })
            .collect();

        let loader = CachingDeleteFileLoader::new(file_io.clone(), 4);
        let delete_filter = loader
            .load_deletes(
                &tasks,
                Arc::new(Schema::builder().build().expect("empty schema")),
            )
            .await
            .expect("loader future")
            .expect("the D1 loader must load what the D2 writer wrote");

        let vector_x = delete_filter
            .get_delete_vector_for_path(data_file_x)
            .expect("vector for data file x");
        let positions_x: Vec<u64> = vector_x.lock().expect("lock x").iter().collect();
        assert_eq!(positions_x, vec![0, 5, (1u64 << 32) + 7]);

        let vector_y = delete_filter
            .get_delete_vector_for_path(data_file_y)
            .expect("vector for data file y");
        let positions_y: Vec<u64> = vector_y.lock().expect("lock y").iter().collect();
        assert_eq!(positions_y, (100u64..200).collect::<Vec<_>>());
    }
}
