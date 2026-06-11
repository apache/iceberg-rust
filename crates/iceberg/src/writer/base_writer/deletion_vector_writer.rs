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
//! # Previous-deletes merge (`loadPreviousDeletes`)
//!
//! Java's `close()` additionally MERGES the previous deletes for each path
//! (`BaseDVFileWriter.close` L117-126): for each referenced data file it calls
//! `loadPreviousDeletes.apply(path)` to obtain that file's existing positions, unions them into the
//! new deletion vector (so the merged DV carries old + new positions), and collects the superseded
//! delete files that are FILE-SCOPED (`ContentFileUtil.isFileScoped`) into `rewrittenDeleteFiles`
//! for the commit to REPLACE (via `RowDelta.removeDeletes`). This is the merge-and-replace contract
//! that keeps exactly ONE live DV per data file across overwriting writes.
//!
//! This writer mirrors it via [`DVFileWriter::with_previous_deletes`]: a caller supplies, per
//! referenced data-file path, the previous positions ([`DeleteVector`]) plus the SOURCE delete
//! `DataFile`(s) they came from (Java's `PositionDeleteIndex.deleteFiles()`). On
//! [`close_with_result`](DVFileWriter::close_with_result) the previous positions are unioned into
//! the new DV (`record_count`/cardinality reflect the MERGED set) and every file-scoped source file
//! is returned as a [`rewritten delete file`](DVWriteResult::rewritten_delete_files). Files that are
//! NOT file-scoped (partition-scoped parquet position deletes) are NOT rewritten — Java leaves them
//! in place (`BaseDVFileWriter` L121-124: "only DVs and file-scoped deletes can be discarded from
//! the table state"). With no previous deletes supplied the output is byte-IDENTICAL to a
//! fresh-only run (the D2/D4 byte-parity pins are the floor).
//!
//! The classic engine flow (Spark `SparkPositionDeltaWrite` L234-256) feeds the result straight
//! into a `RowDelta`: `rowDelta.addDeletes(dv)` for each merged DV, then
//! `for (DeleteFile f : result.rewrittenDeleteFiles()) rowDelta.removeDeletes(f)`. The Rust mirror
//! is `row_delta().add_deletes(result.delete_files).remove_deletes_many(result.rewritten_delete_files)`.

use std::collections::{BTreeMap, HashMap};

use crate::delete_vector::DeleteVector;
use crate::io::OutputFile;
use crate::metadata_columns::{RESERVED_FIELD_ID_DELETE_FILE_PATH, RESERVED_FIELD_ID_POS};
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

/// A data file's PREVIOUS deletes, supplied to [`DVFileWriter::with_previous_deletes`] for merging
/// into a new deletion vector — the Rust mirror of the `PositionDeleteIndex` returned by Java's
/// `loadPreviousDeletes: Function<String, PositionDeleteIndex>` (`BaseDVFileWriter` ctor L56/L66-71).
///
/// A Java `PositionDeleteIndex` bundles a bitmap of positions WITH the source `DeleteFile`s those
/// positions came from (`BitmapPositionDeleteIndex.deleteFiles()`); this struct carries the same two
/// pieces. The `positions` are unioned into the new DV; the `source_delete_files` that are
/// [`is_file_scoped`] are returned as rewritten delete files for the commit to remove (Java
/// `BaseDVFileWriter.close` L120-125).
#[derive(Debug, Clone)]
pub struct PreviousDeletes {
    /// The data file's existing deleted positions (loaded via the production read path — e.g. a
    /// decoded previous DV blob, NOT a hand-built vector).
    positions: DeleteVector,
    /// The delete `DataFile`(s) those positions came from (Java `PositionDeleteIndex.deleteFiles()`).
    /// Each one that is file-scoped becomes a rewritten (to-be-removed) delete file after the merge.
    source_delete_files: Vec<DataFile>,
}

impl PreviousDeletes {
    /// Build a `PreviousDeletes` from a data file's existing positions and the delete file(s) they
    /// were loaded from.
    ///
    /// `positions` is the previous deletion set (typically read back through the production
    /// loader/decoder); `source_delete_files` are the delete `DataFile`s carrying those positions
    /// (the DV `DeleteFile`, or path-scoped position-delete files) — the ones the merge may mark as
    /// superseded (Java's `PositionDeleteIndex.deleteFiles()`).
    pub fn new(positions: DeleteVector, source_delete_files: Vec<DataFile>) -> Self {
        Self {
            positions,
            source_delete_files,
        }
    }
}

/// The result of [`DVFileWriter::close_with_result`] — the Rust mirror of Java
/// `org.apache.iceberg.io.DeleteWriteResult` (`DeleteWriteResult(deleteFiles, referencedDataFiles,
/// rewrittenDeleteFiles)`).
///
/// Java's `DeleteWriteResult` carries a third member, `referencedDataFiles` (a `CharSequenceSet`),
/// used by conflict validation; it is recoverable from `delete_files` (each carries its
/// `referenced_data_file`), so it is omitted here rather than duplicated — the two load-bearing
/// members for the merge-and-replace commit are the DVs and the rewritten (superseded) delete files.
#[derive(Debug)]
pub struct DVWriteResult {
    /// One DV `DeleteFile` per referenced data file (Java `DeleteWriteResult.deleteFiles()`); the
    /// same value [`DVFileWriter::close`] returns. Feed these to `RowDelta.add_deletes`.
    pub delete_files: Vec<DataFile>,
    /// The FILE-SCOPED previous delete files that the merged DVs supersede (Java
    /// `DeleteWriteResult.rewrittenDeleteFiles()`). Feed these to `RowDelta.remove_deletes_many`.
    /// Non-file-scoped previous deletes (partition-scoped parquet position deletes) are NOT included
    /// — Java leaves them in the table (`BaseDVFileWriter` L121-124).
    pub rewritten_delete_files: Vec<DataFile>,
}

/// Whether a previous delete file is FILE-SCOPED and therefore safe to discard from the table state
/// when its positions are merged into a new DV — the Rust mirror of Java
/// `ContentFileUtil.isFileScoped(DeleteFile)` (1.10.0 jar bytecode: `return referencedDataFile(df)
/// != null`), which `BaseDVFileWriter.close` (L121-124) uses to decide what goes into
/// `rewrittenDeleteFiles` ("only DVs and file-scoped deletes can be discarded from the table state").
///
/// `ContentFileUtil.referencedDataFile` (the predicate's basis) is:
///   1. EQUALITY deletes are never file-scoped → `false`;
///   2. a non-null `referenced_data_file` field (every DV carries it; a path-scoped position delete
///      may) → `true`;
///   3. otherwise, the position delete's `_file_path` (reserved id 2147483546 =
///      `RESERVED_FIELD_ID_DELETE_FILE_PATH`, Java `MetadataColumns.DELETE_FILE_PATH`) lower bound
///      and upper bound are both present and EQUAL — i.e. the file's deletes all reference ONE data
///      file (a file-scoped position delete) → `true`; unequal/absent bounds (a partition-scoped
///      delete spanning many data files) → `false`.
///
/// This reuses the same public `DataFile` accessors `is_deletion_vector` keys on; it is NOT a fork
/// of `is_deletion_vector` (`format == PUFFIN`) — Java's `isFileScoped` is the broader
/// `referencedDataFile != null` predicate (DV OR path-scoped position delete), so a DV is file-scoped
/// because it carries `referenced_data_file`, not because it is a DV.
fn is_file_scoped(delete_file: &DataFile) -> bool {
    if delete_file.content_type() == DataContentType::EqualityDeletes {
        return false;
    }
    if delete_file.referenced_data_file().is_some() {
        return true;
    }
    // The Java `referencedDataFile` fallback: a position delete whose `_file_path` bounds pin a
    // single data file is file-scoped even without the explicit field. `lower_bounds`/`upper_bounds`
    // are `HashMap<i32, Datum>`; equal Datums under the reserved path id mean a one-data-file delete.
    match (
        delete_file
            .lower_bounds()
            .get(&RESERVED_FIELD_ID_DELETE_FILE_PATH),
        delete_file
            .upper_bounds()
            .get(&RESERVED_FIELD_ID_DELETE_FILE_PATH),
    ) {
        (Some(lower), Some(upper)) => lower == upper,
        _ => false,
    }
}

/// Writer for deletion vectors (V3 Puffin DVs), mirroring Java `BaseDVFileWriter`.
///
/// Accumulate deleted positions with [`delete`](Self::delete) (optionally supplying a data file's
/// PREVIOUS deletes to merge via [`with_previous_deletes`](Self::with_previous_deletes)), then call
/// [`close`](Self::close) (just the DVs) or [`close_with_result`](Self::close_with_result) (the DVs
/// PLUS the rewritten/superseded delete files) to write ONE Puffin file and obtain the per-data-file
/// `DeleteFile` metadata ready for a `RowDelta` commit. If no positions were recorded AND no previous
/// deletes were supplied, `close` writes NO file at all (Java L106-109: "Only create PuffinWriter if
/// there are deletes").
#[derive(Debug)]
pub struct DVFileWriter {
    /// Where the Puffin file goes. The underlying file is only created when `close()` actually
    /// has deletes to write (Java defers via a `Supplier<OutputFile>`; an [`OutputFile`] is
    /// equally lazy — no bytes hit storage until a writer is opened on it).
    output_file: OutputFile,
    /// Per referenced data file path, in sorted order (see the module docs on determinism).
    deletes_by_path: BTreeMap<String, DeletesForDataFile>,
    /// Per referenced data file path, the PREVIOUS deletes to merge in at close time (Java's
    /// `loadPreviousDeletes`). Empty unless [`with_previous_deletes`](Self::with_previous_deletes)
    /// was called.
    previous_deletes_by_path: HashMap<String, PreviousDeletes>,
}

impl DVFileWriter {
    /// Creates a new `DVFileWriter` that will write its single Puffin file to `output_file`
    /// (only if at least one position is deleted before `close`).
    pub fn new(output_file: OutputFile) -> Self {
        Self {
            output_file,
            deletes_by_path: BTreeMap::new(),
            previous_deletes_by_path: HashMap::new(),
        }
    }

    /// Supply each referenced data file's PREVIOUS deletes to MERGE into the new deletion vector at
    /// close time — the Rust mirror of Java's `loadPreviousDeletes: Function<String,
    /// PositionDeleteIndex>` ctor argument (`BaseDVFileWriter` L56/L66-71).
    ///
    /// `previous_deletes_by_path` maps a data-file path to its existing positions plus the source
    /// delete file(s) they came from ([`PreviousDeletes`]). At [`close_with_result`](Self::close_with_result):
    ///
    /// * for every path THAT ALSO HAS NEW POSITIONS recorded via [`delete`](Self::delete), the
    ///   previous positions are unioned into the new DV (Java iterates `deletesByPath.values()` and
    ///   calls `loadPreviousDeletes.apply(path)` per entry — a path with ONLY previous deletes and no
    ///   new position is never visited, so its previous deletes are NOT merged, exactly as Java);
    /// * every file-scoped ([`is_file_scoped`]) source file of a merged path is returned as a
    ///   rewritten delete file for the commit to remove.
    ///
    /// Calling this with an empty map (or not at all) leaves the output BYTE-IDENTICAL to a
    /// fresh-only writer. Repeated calls REPLACE the map (last wins), mirroring a single ctor arg.
    pub fn with_previous_deletes(
        mut self,
        previous_deletes_by_path: HashMap<String, PreviousDeletes>,
    ) -> Self {
        self.previous_deletes_by_path = previous_deletes_by_path;
        self
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
    /// `createDV` L145-159) — just the DVs, discarding the rewritten-delete-files half.
    ///
    /// With no recorded deletes this writes NO file and returns an empty vec (Java L106-109).
    /// Equivalent to [`close_with_result`](Self::close_with_result)`().delete_files`; use that when
    /// previous deletes were merged and the superseded files must be removed in the commit.
    pub async fn close(self) -> Result<Vec<DataFile>> {
        Ok(self.close_with_result().await?.delete_files)
    }

    /// Writes all accumulated deletion vectors into ONE Puffin file and returns both the DV
    /// `DeleteFile`s AND the rewritten (superseded, file-scoped) previous delete files — the Rust
    /// mirror of Java `BaseDVFileWriter.close` (L99-143) producing a
    /// `DeleteWriteResult(dvs, referencedDataFiles, rewrittenDeleteFiles)`.
    ///
    /// The merge step (Java L114-129): for each referenced data file with new positions, any
    /// PREVIOUS deletes supplied via [`with_previous_deletes`](Self::with_previous_deletes) are
    /// unioned into that file's deletion vector (so `record_count`/cardinality reflect old + new),
    /// and every file-scoped ([`is_file_scoped`]) source file of those previous deletes is collected
    /// into [`rewritten_delete_files`](DVWriteResult::rewritten_delete_files). Non-file-scoped
    /// previous deletes are NOT rewritten (Java leaves them, L121-124).
    ///
    /// With no recorded deletes this writes NO file and returns empty vecs (Java L106-109). With no
    /// previous deletes the DV bytes are IDENTICAL to a fresh-only run.
    pub async fn close_with_result(mut self) -> Result<DVWriteResult> {
        // Merge each referenced file's PREVIOUS deletes into its new DV (Java L114-129). Only paths
        // with new positions are visited (Java iterates `deletesByPath.values()`); collect the
        // file-scoped superseded source files as rewritten delete files.
        let mut rewritten_delete_files: Vec<DataFile> = Vec::new();
        for (data_file_path, deletes) in &mut self.deletes_by_path {
            let Some(previous) = self.previous_deletes_by_path.get(data_file_path) else {
                continue;
            };
            deletes.positions.merge(&previous.positions);
            for source_file in &previous.source_delete_files {
                // "only DVs and file-scoped deletes can be discarded from the table state"
                // (BaseDVFileWriter L121-124).
                if is_file_scoped(source_file) {
                    rewritten_delete_files.push(source_file.clone());
                }
            }
        }

        if self.deletes_by_path.is_empty() {
            return Ok(DVWriteResult {
                delete_files: Vec::new(),
                rewritten_delete_files,
            });
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
        // offset/length the DeleteFile must reference. The positions here already include any
        // merged previous deletes (the loop above), so cardinality reflects the MERGED set.
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

        let delete_files = self
            .deletes_by_path
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
            .collect::<Result<Vec<DataFile>>>()?;

        Ok(DVWriteResult {
            delete_files,
            rewritten_delete_files,
        })
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

    /// Slice the RAW blob bytes at the DeleteFile's coordinates (for byte-level comparison, e.g. the
    /// no-previous byte-identical floor).
    fn decode_region(puffin_bytes: &[u8], delete_file: &DataFile) -> Vec<u8> {
        let offset = usize::try_from(delete_file.content_offset().expect("offset present"))
            .expect("offset fits usize");
        let size = usize::try_from(
            delete_file
                .content_size_in_bytes()
                .expect("content size present"),
        )
        .expect("size fits usize");
        puffin_bytes[offset..offset + size].to_vec()
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

    // ============================================================================================
    // Previous-deletes MERGE hook (Java `BaseDVFileWriter.loadPreviousDeletes` + `isFileScoped`).
    // ============================================================================================

    /// A synthetic DV `DeleteFile` for `referenced_data_file` (file-scoped: carries the referenced
    /// field) — the shape a previous DV's source file has.
    fn synthetic_dv_delete_file(path: &str, referenced_data_file: &str) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Puffin)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(0)
            .partition(Struct::empty())
            .referenced_data_file(Some(referenced_data_file.to_string()))
            .content_offset(Some(4))
            .content_size_in_bytes(Some(40))
            .build()
            .expect("build synthetic DV delete file")
    }

    /// A synthetic PARTITION-scoped parquet position delete (no `referenced_data_file`, no equal
    /// `_file_path` bounds) — NOT file-scoped, so the merge must NOT rewrite it.
    fn synthetic_partition_scoped_pos_delete(path: &str) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(2)
            .partition_spec_id(0)
            .partition(Struct::empty())
            .build()
            .expect("build synthetic partition-scoped pos delete")
    }

    /// Risk pinned: the previous-deletes MERGE — supplying a data file's existing positions must
    /// UNION them into the new DV (Java `BaseDVFileWriter.close` L118-119
    /// `positions.merge(previousPositions)`), so the merged blob deletes old + new and
    /// `record_count`/cardinality is the MERGED count. A broken merge writes only the new positions
    /// → the previous deletes RESURRECT.
    #[tokio::test]
    async fn test_dv_writer_merges_previous_positions_into_new_dv() {
        let temp_dir = TempDir::new().expect("temp dir");
        let file_io = FileIO::new_with_fs();
        let data_file = "mem://data/x.parquet";

        // Previous deletes: positions {1} (loaded, e.g., from a prior DV), sourced from a DV file.
        let previous = PreviousDeletes::new(DeleteVector::new([1u64].into_iter().collect()), vec![
            synthetic_dv_delete_file("mem://data/dv1.puffin", data_file),
        ]);
        let mut writer = DVFileWriter::new(output_file(&file_io, &temp_dir, "dv2.puffin"))
            .with_previous_deletes(HashMap::from([(data_file.to_string(), previous)]));
        // New delete: position {3}.
        writer
            .delete(data_file, 3, None)
            .expect("record new delete");

        let result = writer.close_with_result().await.expect("close with result");
        assert_eq!(result.delete_files.len(), 1, "one merged DV for the file");
        assert_eq!(
            result.delete_files[0].record_count(),
            2,
            "record_count must be the MERGED cardinality {{1,3}} = 2, not just the new {{3}}"
        );

        // The blob at the merged DV's coordinates decodes to the UNION {1, 3}.
        let puffin_bytes = std::fs::read(result.delete_files[0].file_path()).expect("read puffin");
        assert_eq!(
            decode_blob_at(&puffin_bytes, &result.delete_files[0]),
            vec![1, 3],
            "the merged DV must delete the UNION of previous {{1}} and new {{3}}"
        );

        // The file-scoped previous DV is returned as a rewritten (to-be-removed) delete file.
        assert_eq!(
            result.rewritten_delete_files.len(),
            1,
            "the superseded file-scoped DV must be returned for removal"
        );
        assert_eq!(
            result.rewritten_delete_files[0].file_path(),
            "mem://data/dv1.puffin"
        );
    }

    /// Risk pinned: `is_file_scoped` selectivity for rewritten files — a NON-file-scoped previous
    /// delete (a partition-scoped parquet position delete spanning many data files) must NOT be
    /// rewritten (Java `BaseDVFileWriter` L121-124 only discards file-scoped deletes); rewriting it
    /// would drop a delete still applying to OTHER data files (resurrection on those files). The new
    /// positions still merge in.
    #[tokio::test]
    async fn test_dv_writer_does_not_rewrite_partition_scoped_previous_delete() {
        let temp_dir = TempDir::new().expect("temp dir");
        let file_io = FileIO::new_with_fs();
        let data_file = "mem://data/x.parquet";

        let previous = PreviousDeletes::new(DeleteVector::new([1u64].into_iter().collect()), vec![
            synthetic_partition_scoped_pos_delete("mem://data/partition-deletes.parquet"),
        ]);
        let mut writer = DVFileWriter::new(output_file(&file_io, &temp_dir, "dv.puffin"))
            .with_previous_deletes(HashMap::from([(data_file.to_string(), previous)]));
        writer
            .delete(data_file, 3, None)
            .expect("record new delete");

        let result = writer.close_with_result().await.expect("close with result");
        // The previous positions STILL merge (positions are unioned regardless of scope).
        assert_eq!(result.delete_files[0].record_count(), 2);
        let puffin_bytes = std::fs::read(result.delete_files[0].file_path()).expect("read puffin");
        assert_eq!(
            decode_blob_at(&puffin_bytes, &result.delete_files[0]),
            vec![1, 3]
        );
        // But the partition-scoped parquet delete is NOT a rewritten file (it may apply elsewhere).
        assert!(
            result.rewritten_delete_files.is_empty(),
            "a partition-scoped (non-file-scoped) previous delete must NOT be rewritten"
        );
    }

    /// Risk pinned: an EQUALITY-delete source file is never file-scoped (Java
    /// `ContentFileUtil.referencedDataFile` returns null for EQUALITY_DELETES) — and a DV does not
    /// supersede equality deletes anyway, so it must NOT appear in rewritten files even if supplied.
    #[tokio::test]
    async fn test_dv_writer_does_not_rewrite_equality_delete_previous_source() {
        let temp_dir = TempDir::new().expect("temp dir");
        let file_io = FileIO::new_with_fs();
        let data_file = "mem://data/x.parquet";

        let eq_delete = DataFileBuilder::default()
            .content(DataContentType::EqualityDeletes)
            .file_path("mem://data/eq.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .equality_ids(Some(vec![1]))
            .partition_spec_id(0)
            .partition(Struct::empty())
            .build()
            .expect("build eq delete");
        let previous = PreviousDeletes::new(DeleteVector::new([1u64].into_iter().collect()), vec![
            eq_delete,
        ]);
        let mut writer = DVFileWriter::new(output_file(&file_io, &temp_dir, "dv.puffin"))
            .with_previous_deletes(HashMap::from([(data_file.to_string(), previous)]));
        writer
            .delete(data_file, 3, None)
            .expect("record new delete");

        let result = writer.close_with_result().await.expect("close with result");
        assert!(
            result.rewritten_delete_files.is_empty(),
            "an equality delete is never file-scoped — it must not be rewritten"
        );
    }

    /// Risk pinned: previous deletes for a path with NO new positions are IGNORED — Java iterates
    /// `deletesByPath.values()` and calls `loadPreviousDeletes` per entry, so a path never written
    /// to is never visited. Supplying previous deletes for an unwritten path must produce NO blob
    /// for it and NO rewritten file (the engine only loads previous deletes for files it rewrites).
    #[tokio::test]
    async fn test_dv_writer_ignores_previous_deletes_for_unwritten_path() {
        let temp_dir = TempDir::new().expect("temp dir");
        let file_io = FileIO::new_with_fs();
        let written = "mem://data/written.parquet";
        let unwritten = "mem://data/unwritten.parquet";

        let previous_for_unwritten =
            PreviousDeletes::new(DeleteVector::new([9u64].into_iter().collect()), vec![
                synthetic_dv_delete_file("mem://data/old-dv.puffin", unwritten),
            ]);
        let mut writer =
            DVFileWriter::new(output_file(&file_io, &temp_dir, "dv.puffin")).with_previous_deletes(
                HashMap::from([(unwritten.to_string(), previous_for_unwritten)]),
            );
        // Only `written` gets a new position; `unwritten` is never written.
        writer.delete(written, 0, None).expect("record new delete");

        let result = writer.close_with_result().await.expect("close with result");
        assert_eq!(
            result.delete_files.len(),
            1,
            "only the written path produces a DV"
        );
        assert_eq!(
            result.delete_files[0].referenced_data_file().as_deref(),
            Some(written)
        );
        assert!(
            result.rewritten_delete_files.is_empty(),
            "previous deletes for an unwritten path must be ignored (Java visits deletesByPath only)"
        );
    }

    /// Risk pinned: BYTE-IDENTICAL no-previous floor — `with_previous_deletes(empty)` (and not
    /// calling it at all) must produce the EXACT same blob bytes as a fresh-only writer, so the
    /// D2/D4 byte-parity pins stay green. A merge step that touches the bytes even when there is
    /// nothing to merge would silently break Java byte-parity.
    #[tokio::test]
    async fn test_dv_writer_no_previous_deletes_is_byte_identical_to_fresh() {
        let temp_dir = TempDir::new().expect("temp dir");
        let file_io = FileIO::new_with_fs();
        let data_file = "mem://data/x.parquet";

        let mut fresh = DVFileWriter::new(output_file(&file_io, &temp_dir, "fresh.puffin"));
        for pos in [0u64, 3, 7] {
            fresh.delete(data_file, pos, None).expect("fresh delete");
        }
        let fresh_files = fresh.close().await.expect("close fresh");

        let mut empty_prev = DVFileWriter::new(output_file(&file_io, &temp_dir, "empty.puffin"))
            .with_previous_deletes(HashMap::new());
        for pos in [0u64, 3, 7] {
            empty_prev.delete(data_file, pos, None).expect("delete");
        }
        let empty_prev_files = empty_prev.close().await.expect("close empty-prev");

        let fresh_blob = {
            let bytes = std::fs::read(fresh_files[0].file_path()).expect("read fresh");
            decode_region(&bytes, &fresh_files[0])
        };
        let empty_blob = {
            let bytes = std::fs::read(empty_prev_files[0].file_path()).expect("read empty");
            decode_region(&bytes, &empty_prev_files[0])
        };
        assert_eq!(
            fresh_blob, empty_blob,
            "an empty previous-deletes map must leave the blob bytes identical to fresh-only"
        );
    }

    /// Risk pinned: `is_file_scoped` predicate — the three Java `ContentFileUtil.referencedDataFile`
    /// branches (equality → false; explicit referenced field → true; equal `_file_path` bounds →
    /// true; absent/unequal bounds → false). A drift here misclassifies what gets rewritten,
    /// either dropping a still-applying delete (resurrection) or failing to remove a superseded one.
    #[test]
    fn test_is_file_scoped_mirrors_java_referenced_data_file() {
        use crate::spec::Datum;

        // (1) DV with explicit referenced_data_file → file-scoped.
        assert!(is_file_scoped(&synthetic_dv_delete_file(
            "dv.puffin",
            "data/x.parquet"
        )));

        // (2) equality delete → never file-scoped.
        let eq = DataFileBuilder::default()
            .content(DataContentType::EqualityDeletes)
            .file_path("eq.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(1)
            .record_count(1)
            .equality_ids(Some(vec![1]))
            .partition_spec_id(0)
            .partition(Struct::empty())
            .build()
            .expect("eq delete");
        assert!(!is_file_scoped(&eq));

        // (3) partition-scoped parquet pos delete, no bounds → NOT file-scoped.
        assert!(!is_file_scoped(&synthetic_partition_scoped_pos_delete(
            "part.parquet"
        )));

        // (4) position delete whose `_file_path` lower==upper bound pins ONE data file → file-scoped
        //     (the Java fallback when `referenced_data_file` is unset).
        let path_bound = Datum::string("data/x.parquet");
        let path_scoped = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("scoped.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(1)
            .record_count(1)
            .partition_spec_id(0)
            .partition(Struct::empty())
            .lower_bounds(HashMap::from([(
                RESERVED_FIELD_ID_DELETE_FILE_PATH,
                path_bound.clone(),
            )]))
            .upper_bounds(HashMap::from([(
                RESERVED_FIELD_ID_DELETE_FILE_PATH,
                path_bound,
            )]))
            .build()
            .expect("path-scoped pos delete");
        assert!(
            is_file_scoped(&path_scoped),
            "equal _file_path bounds pin one data file → file-scoped (Java fallback)"
        );

        // (5) position delete whose `_file_path` bounds DIFFER (spans many data files) → NOT
        //     file-scoped.
        let unequal = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("spanning.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(1)
            .record_count(1)
            .partition_spec_id(0)
            .partition(Struct::empty())
            .lower_bounds(HashMap::from([(
                RESERVED_FIELD_ID_DELETE_FILE_PATH,
                Datum::string("data/a.parquet"),
            )]))
            .upper_bounds(HashMap::from([(
                RESERVED_FIELD_ID_DELETE_FILE_PATH,
                Datum::string("data/z.parquet"),
            )]))
            .build()
            .expect("spanning pos delete");
        assert!(
            !is_file_scoped(&unequal),
            "unequal _file_path bounds span many data files → NOT file-scoped"
        );
    }
}
