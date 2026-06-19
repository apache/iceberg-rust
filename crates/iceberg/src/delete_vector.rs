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

//! Iceberg V3 deletion vectors (`deletion-vector-v1`): a roaring-bitmap-backed set
//! of deleted row positions, serialized to and from Puffin blobs and files.

use std::collections::HashMap;
use std::io::Cursor;
use std::ops::BitOrAssign;

use crc32fast::Hasher;
use roaring::RoaringTreemap;
use roaring::bitmap::Iter;
use roaring::treemap::BitmapIter;

use crate::io::FileIO;
use crate::puffin::{Blob, CompressionCodec, DELETION_VECTOR_V1, PuffinWriter};
use crate::spec::{DataContentType, DataFile, DataFileBuilder, DataFileFormat, Struct};
use crate::{Error, ErrorKind, Result};

/// Iceberg `deletion-vector-v1` Puffin blob magic bytes (Iceberg Puffin spec;
/// ported from risingwavelabs/iceberg-rust #113 — design reference only).
const DELETION_VECTOR_MAGIC_BYTES: [u8; 4] = [0xD1, 0xD3, 0x39, 0x64];
/// Minimum blob size: u32 length (4) + magic (4) + u32 crc (4).
const MIN_SERIALIZED_DELETION_VECTOR_BLOB: usize = 12;
/// Puffin blob property: deletion vector cardinality (number of deleted positions).
pub(crate) const DELETION_VECTOR_PROPERTY_CARDINALITY: &str = "cardinality";
/// Puffin blob property: referenced data file path the DV applies to.
pub(crate) const DELETION_VECTOR_PROPERTY_REFERENCED_DATA_FILE: &str = "referenced-data-file";

/// A set of deleted row positions backed by a 64-bit roaring bitmap — the in-memory
/// form of an Iceberg V3 `deletion-vector-v1`.
#[derive(Debug, Default)]
pub struct DeleteVector {
    inner: RoaringTreemap,
}

impl DeleteVector {
    /// Creates a delete vector that wraps an existing roaring treemap of positions.
    #[allow(unused)]
    pub fn new(roaring_treemap: RoaringTreemap) -> DeleteVector {
        DeleteVector {
            inner: roaring_treemap,
        }
    }

    /// Returns an iterator over the deleted row positions in ascending order.
    pub fn iter(&self) -> DeleteVectorIterator<'_> {
        let outer = self.inner.bitmaps();
        DeleteVectorIterator { outer, inner: None }
    }

    /// Marks row position `pos` as deleted; returns `true` if it was newly added.
    pub fn insert(&mut self, pos: u64) -> bool {
        self.inner.insert(pos)
    }

    /// Marks the given `positions` as deleted and returns the number of elements appended.
    ///
    /// The input slice must be strictly ordered in ascending order, and every value must be greater than all existing values already in the set.
    ///
    /// # Errors
    ///
    /// Returns an error if the precondition is not met.
    #[allow(dead_code)]
    pub fn insert_positions(&mut self, positions: &[u64]) -> Result<usize> {
        if let Err(err) = self.inner.append(positions.iter().copied()) {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "failed to marks rows as deleted".to_string(),
            )
            .with_source(err));
        }
        Ok(positions.len())
    }

    /// Returns the number of deleted row positions.
    #[allow(unused)]
    pub fn len(&self) -> u64 {
        self.inner.len()
    }

    /// Returns `true` if there are no deleted positions in this vector.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Serialize this delete vector into an Iceberg V3 `deletion-vector-v1` Puffin blob.
    ///
    /// Blob layout (Iceberg Puffin spec): `length(u32 BE = magic + bitmap) ‖ magic ‖
    /// portable 64-bit RoaringTreemap ‖ crc32(u32 BE over magic + bitmap)`.
    /// `properties` must contain `cardinality` + `referenced-data-file`.
    /// Ported from risingwavelabs/iceberg-rust #113 (design reference only).
    pub fn to_puffin_blob(&self, properties: HashMap<String, String>) -> Result<Blob> {
        Self::check_properties(&properties)?;

        let serialized_bitmap_size = self.inner.serialized_size();
        let combined_length = (DELETION_VECTOR_MAGIC_BYTES.len() + serialized_bitmap_size) as u32;
        let mut data = Vec::with_capacity(
            std::mem::size_of_val(&combined_length)
                + DELETION_VECTOR_MAGIC_BYTES.len()
                + serialized_bitmap_size
                + 4,
        );

        data.extend_from_slice(&combined_length.to_be_bytes());
        data.extend_from_slice(&DELETION_VECTOR_MAGIC_BYTES);

        let bitmap_start = data.len();
        data.resize(bitmap_start + serialized_bitmap_size, 0);
        {
            let mut cursor = Cursor::new(&mut data[bitmap_start..]);
            self.inner.serialize_into(&mut cursor).map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    "failed to serialize deletion vector bitmap".to_string(),
                )
                .with_source(err)
            })?;
        }

        let mut hasher = Hasher::new();
        hasher.update(&data[4..]);
        let crc = hasher.finalize();
        data.extend_from_slice(&crc.to_be_bytes());

        Ok(Blob::builder()
            .r#type(DELETION_VECTOR_V1.to_string())
            .fields(vec![])
            .snapshot_id(-1)
            .sequence_number(-1)
            .data(data)
            .properties(properties)
            .build())
    }

    /// Deserialize a delete vector from an Iceberg `deletion-vector-v1` Puffin blob.
    pub fn from_puffin_blob(blob: Blob) -> Result<Self> {
        if blob.blob_type() != DELETION_VECTOR_V1 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("unsupported puffin blob type: {}", blob.blob_type()),
            ));
        }

        let data = blob.data();
        if data.len() < MIN_SERIALIZED_DELETION_VECTOR_BLOB {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "serialized deletion vector blob too small".to_string(),
            ));
        }

        let magic = &data[4..8];
        if magic != DELETION_VECTOR_MAGIC_BYTES {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "invalid deletion vector magic bytes".to_string(),
            ));
        }

        let combined_length = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        let expected_len = std::mem::size_of_val(&combined_length) + combined_length as usize + 4;
        if expected_len != data.len() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "serialized deletion vector length mismatch: expected {expected_len}, actual {}",
                    data.len()
                ),
            ));
        }

        let bitmap_end = data.len() - 4;
        let bitmap_data = &data[8..bitmap_end];

        let mut hasher = Hasher::new();
        hasher.update(&data[4..bitmap_end]);
        let expected_crc = hasher.finalize();
        let stored_crc = u32::from_be_bytes([
            data[data.len() - 4],
            data[data.len() - 3],
            data[data.len() - 2],
            data[data.len() - 1],
        ]);
        if expected_crc != stored_crc {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("deletion vector crc mismatch: expected {expected_crc}, got {stored_crc}"),
            ));
        }

        let bitmap =
            RoaringTreemap::deserialize_from(&mut Cursor::new(bitmap_data)).map_err(|err| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "failed to deserialize deletion vector bitmap".to_string(),
                )
                .with_source(err)
            })?;

        Ok(DeleteVector::new(bitmap))
    }

    fn check_properties(properties: &HashMap<String, String>) -> Result<()> {
        if !properties.contains_key(DELETION_VECTOR_PROPERTY_CARDINALITY) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "deletion vector blob missing required property: {DELETION_VECTOR_PROPERTY_CARDINALITY}"
                ),
            ));
        }
        if !properties.contains_key(DELETION_VECTOR_PROPERTY_REFERENCED_DATA_FILE) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "deletion vector blob missing required property: {DELETION_VECTOR_PROPERTY_REFERENCED_DATA_FILE}"
                ),
            ));
        }
        Ok(())
    }

    /// Write this delete vector to a `deletion-vector-v1` Puffin file at `location` and
    /// return the V3 `DataFile{content=PositionDeletes, …}` to feed
    /// `RowDeltaAction::add_delete_files`. Connects DV serialization → Puffin file →
    /// delete-file metadata (offset/length) in one step (cf. RW `deletion_vector_writer.rs`).
    pub async fn write_to_puffin_file(
        &self,
        file_io: &FileIO,
        location: String,
        referenced_data_file: String,
        partition: Struct,
        partition_spec_id: i32,
    ) -> Result<DataFile> {
        let cardinality = self.len();
        let properties = HashMap::from([
            (
                DELETION_VECTOR_PROPERTY_CARDINALITY.to_string(),
                cardinality.to_string(),
            ),
            (
                DELETION_VECTOR_PROPERTY_REFERENCED_DATA_FILE.to_string(),
                referenced_data_file.clone(),
            ),
        ]);
        let blob = self.to_puffin_blob(properties)?;

        let output_file = file_io.new_output(&location)?;
        let mut writer = PuffinWriter::new(&output_file, HashMap::new(), false).await?;
        writer.add(blob, CompressionCodec::None).await?;
        let result = writer.close_with_metadata().await?;
        let file_size = result.file_size_in_bytes;
        let blob_metadata = result.blobs_metadata.first().ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "puffin metadata is empty after writing deletion vector",
            )
        })?;

        DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(location)
            .file_format(DataFileFormat::Puffin)
            .partition(partition)
            .partition_spec_id(partition_spec_id)
            .record_count(cardinality)
            .file_size_in_bytes(file_size)
            .referenced_data_file(Some(referenced_data_file))
            .content_offset(Some(blob_metadata.offset() as i64))
            .content_size_in_bytes(Some(blob_metadata.length() as i64))
            .build()
            .map_err(|err| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("failed to build deletion vector data file: {err}"),
                )
            })
    }
}

// Ideally, we'd just wrap `roaring::RoaringTreemap`'s iterator, `roaring::treemap::Iter` here.
// But right now, it does not have a corresponding implementation of `roaring::bitmap::Iter::advance_to`,
// which is very handy in ArrowReader::build_deletes_row_selection.
// There is a PR open on roaring to add this (https://github.com/RoaringBitmap/roaring-rs/pull/314)
// and if that gets merged then we can simplify `DeleteVectorIterator` here, refactoring `advance_to`
// to just a wrapper around the underlying iterator's method.
/// Iterator over the deleted row positions of a [`DeleteVector`], in ascending order.
pub struct DeleteVectorIterator<'a> {
    // NB: `BitMapIter` was only exposed publicly in https://github.com/RoaringBitmap/roaring-rs/pull/316
    // which is not yet released. As a consequence our Cargo.toml temporarily uses a git reference for
    // the roaring dependency.
    outer: BitmapIter<'a>,
    inner: Option<DeleteVectorIteratorInner<'a>>,
}

struct DeleteVectorIteratorInner<'a> {
    high_bits: u32,
    bitmap_iter: Iter<'a>,
}

impl Iterator for DeleteVectorIterator<'_> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(inner) = &mut self.inner
            && let Some(inner_next) = inner.bitmap_iter.next()
        {
            return Some(u64::from(inner.high_bits) << 32 | u64::from(inner_next));
        }

        if let Some((high_bits, next_bitmap)) = self.outer.next() {
            self.inner = Some(DeleteVectorIteratorInner {
                high_bits,
                bitmap_iter: next_bitmap.iter(),
            })
        } else {
            return None;
        }

        self.next()
    }
}

impl DeleteVectorIterator<'_> {
    /// Advances the iterator so the next yielded position is `>= pos`.
    pub fn advance_to(&mut self, pos: u64) {
        let hi = (pos >> 32) as u32;
        let lo = pos as u32;

        let Some(ref mut inner) = self.inner else {
            return;
        };

        while inner.high_bits < hi {
            let Some((next_hi, next_bitmap)) = self.outer.next() else {
                return;
            };

            *inner = DeleteVectorIteratorInner {
                high_bits: next_hi,
                bitmap_iter: next_bitmap.iter(),
            }
        }

        inner.bitmap_iter.advance_to(lo);
    }
}

impl BitOrAssign for DeleteVector {
    fn bitor_assign(&mut self, other: Self) {
        self.inner.bitor_assign(&other.inner);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insertion_and_iteration() {
        let mut dv = DeleteVector::default();
        assert!(dv.insert(42));
        assert!(dv.insert(100));
        assert!(!dv.insert(42));

        let mut items: Vec<u64> = dv.iter().collect();
        items.sort();
        assert_eq!(items, vec![42, 100]);
        assert_eq!(dv.len(), 2);
    }

    #[test]
    fn test_successful_insert_positions() {
        let mut dv = DeleteVector::default();
        let positions = vec![1, 2, 3, 1000, 1 << 33];
        assert_eq!(dv.insert_positions(&positions).unwrap(), 5);

        let mut collected: Vec<u64> = dv.iter().collect();
        collected.sort();
        assert_eq!(collected, positions);
    }

    /// Testing scenario: bulk insertion fails because input positions are not strictly increasing.
    #[test]
    fn test_failed_insertion_unsorted_elements() {
        let mut dv = DeleteVector::default();
        let positions = vec![1, 3, 5, 4];
        let res = dv.insert_positions(&positions);
        assert!(res.is_err());
    }

    /// Testing scenario: bulk insertion fails because input positions have intersection with existing ones.
    #[test]
    fn test_failed_insertion_with_intersection() {
        let mut dv = DeleteVector::default();
        let positions = vec![1, 3, 5];
        assert_eq!(dv.insert_positions(&positions).unwrap(), 3);

        let res = dv.insert_positions(&[2, 4]);
        assert!(res.is_err());
    }

    /// Testing scenario: bulk insertion fails because input positions have duplicates.
    #[test]
    fn test_failed_insertion_duplicate_elements() {
        let mut dv = DeleteVector::default();
        let positions = vec![1, 3, 5, 5];
        let res = dv.insert_positions(&positions);
        assert!(res.is_err());
    }

    fn dv_props() -> HashMap<String, String> {
        HashMap::from([
            (
                DELETION_VECTOR_PROPERTY_CARDINALITY.to_string(),
                "0".to_string(),
            ),
            (
                DELETION_VECTOR_PROPERTY_REFERENCED_DATA_FILE.to_string(),
                "s3://bucket/data/f.parquet".to_string(),
            ),
        ])
    }

    /// Self round-trip: serialize → Puffin blob → deserialize recovers the positions,
    /// validating the frame (length, magic, crc) and serialize/deserialize symmetry.
    #[test]
    fn test_dv_puffin_blob_roundtrip() {
        let positions = [1u64, 5, 42, 100, 1 << 33, (1u64 << 33) + 7];
        let mut dv = DeleteVector::default();
        for p in positions {
            dv.insert(p);
        }
        let blob = dv.to_puffin_blob(dv_props()).unwrap();
        assert_eq!(blob.blob_type(), DELETION_VECTOR_V1);

        let restored = DeleteVector::from_puffin_blob(blob).unwrap();
        let mut got: Vec<u64> = restored.iter().collect();
        got.sort();
        assert_eq!(got, positions.to_vec());
    }

    /// Spark-compatibility proxy: parse the serialized bitmap with the EXACT algorithm
    /// pyiceberg's `_deserialize_bitmap` uses — `[u64 LE bucket count]` then per bucket
    /// `[u32 LE high-key + 32-bit portable RoaringBitmap]`. If this recovers the
    /// positions, the bytes are Iceberg/Spark portable (no Spark needed for the signal).
    #[test]
    fn test_dv_blob_is_iceberg_portable() {
        use std::io::Read;

        use roaring::RoaringBitmap;

        let positions = [3u64, 7, 100, (1u64 << 33) + 5];
        let mut dv = DeleteVector::default();
        for p in positions {
            dv.insert(p);
        }
        let blob = dv.to_puffin_blob(dv_props()).unwrap();
        let data = blob.data();

        // Frame (certain): [u32 BE len][magic][bitmap][u32 BE crc]
        assert_eq!(&data[4..8], &DELETION_VECTOR_MAGIC_BYTES);
        let bitmap = &data[8..data.len() - 4];

        // pyiceberg portable parse
        let mut cur = Cursor::new(bitmap);
        let mut count_buf = [0u8; 8];
        cur.read_exact(&mut count_buf).unwrap();
        let n_buckets = u64::from_le_bytes(count_buf);

        let mut recovered: Vec<u64> = Vec::new();
        for _ in 0..n_buckets {
            let mut key_buf = [0u8; 4];
            cur.read_exact(&mut key_buf).unwrap();
            let hi = u32::from_le_bytes(key_buf) as u64;
            let bm = RoaringBitmap::deserialize_from(&mut cur).unwrap();
            for lo in bm.iter() {
                recovered.push((hi << 32) | u64::from(lo));
            }
        }
        recovered.sort();
        assert_eq!(
            recovered,
            positions.to_vec(),
            "serialized bitmap is NOT Iceberg-portable — roaring serialize_into header \
             differs from pyiceberg layout; switch to hand-rolled portable framing"
        );
    }

    /// Piece 2 — full Puffin-FILE round-trip in Rust: write a DV blob to a real
    /// Puffin file via `PuffinWriter`, read it back via `PuffinReader`, and recover
    /// the deleted positions. Proves the Puffin file framing, not just the blob bytes.
    #[tokio::test]
    async fn test_dv_puffin_file_roundtrip() {
        use tempfile::TempDir;

        use crate::io::FileIO;
        use crate::puffin::{CompressionCodec, PuffinReader, PuffinWriter};

        let positions = [2u64, 9, 256, (1u64 << 33) + 11];
        let mut dv = DeleteVector::default();
        for p in positions {
            dv.insert(p);
        }
        assert!(!dv.is_empty());

        let mut props = dv_props();
        props.insert(
            DELETION_VECTOR_PROPERTY_CARDINALITY.to_string(),
            dv.len().to_string(),
        );
        let blob = dv.to_puffin_blob(props).unwrap();

        let tmp = TempDir::new().unwrap();
        let path_buf = tmp.path().join("dv.puffin");
        let path = path_buf.to_str().unwrap();

        let file_io = FileIO::new_with_fs();
        let output = file_io.new_output(path).unwrap();
        let mut writer = PuffinWriter::new(&output, HashMap::new(), false)
            .await
            .unwrap();
        writer.add(blob, CompressionCodec::None).await.unwrap();
        writer.close().await.unwrap();

        let input = output.to_input_file();
        let reader = PuffinReader::new(input);
        let meta = reader.file_metadata().await.unwrap().clone();
        assert_eq!(meta.blobs.len(), 1);
        let read_blob = reader.blob(meta.blobs.first().unwrap()).await.unwrap();

        let restored = DeleteVector::from_puffin_blob(read_blob).unwrap();
        let mut got: Vec<u64> = restored.iter().collect();
        got.sort();
        assert_eq!(got, positions.to_vec());
    }

    /// Piece 2.5 — glue: write a DV to a Puffin file and get back a V3
    /// `DataFile{PositionDeletes}` (offset/size/referenced-file filled), then read the
    /// written file back and recover the positions.
    #[tokio::test]
    async fn test_dv_write_to_puffin_file() {
        use tempfile::TempDir;

        use crate::puffin::PuffinReader;

        let positions = [4u64, 11, 512, (1u64 << 33) + 3];
        let mut dv = DeleteVector::default();
        for p in positions {
            dv.insert(p);
        }

        let tmp = TempDir::new().unwrap();
        let path_buf = tmp.path().join("dv2.puffin");
        let location = path_buf.to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let data_file = dv
            .write_to_puffin_file(
                &file_io,
                location.clone(),
                "s3://bucket/data/x.parquet".to_string(),
                Struct::empty(),
                0,
            )
            .await
            .unwrap();

        assert_eq!(data_file.content_type(), DataContentType::PositionDeletes);
        assert_eq!(
            data_file.referenced_data_file().as_deref(),
            Some("s3://bucket/data/x.parquet")
        );
        assert!(data_file.content_offset().is_some());
        assert!(data_file.content_size_in_bytes().is_some());

        // The written Puffin file reads back to the same positions.
        let input = file_io.new_input(&location).unwrap();
        let reader = PuffinReader::new(input);
        let meta = reader.file_metadata().await.unwrap().clone();
        let blob = reader.blob(meta.blobs.first().unwrap()).await.unwrap();
        let restored = DeleteVector::from_puffin_blob(blob).unwrap();
        let mut got: Vec<u64> = restored.iter().collect();
        got.sort();
        assert_eq!(got, positions.to_vec());
    }

    /// Cross-implementation byte-parity with Apache Iceberg-Java: the serialized
    /// `deletion-vector-v1` payload for positions {1,3,5,7,9} must be byte-identical
    /// to the Java-produced golden fixture (lifted from apache/iceberg test resources
    /// `small-alternating-values-position-index.bin` via apache/iceberg-go). Proves our
    /// roaring serialization + framing exactly match the Iceberg-Java reference.
    #[test]
    fn test_dv_payload_byte_identical_to_java_golden() {
        let golden: &[u8] = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/testdata/puffin/deletion-vector-v1-payload.bin"
        ));

        let mut dv = DeleteVector::default();
        for p in [1u64, 3, 5, 7, 9] {
            dv.insert(p);
        }
        let props = HashMap::from([
            (DELETION_VECTOR_PROPERTY_CARDINALITY.to_string(), "5".to_string()),
            (
                DELETION_VECTOR_PROPERTY_REFERENCED_DATA_FILE.to_string(),
                "data/test.parquet".to_string(),
            ),
        ]);
        let blob = dv.to_puffin_blob(props).unwrap();

        assert_eq!(
            blob.data(),
            golden,
            "DV payload must be byte-identical to the apache/iceberg Java golden fixture"
        );
    }

    /// Empty deletion vector round-trips (mirrors iceberg-go `TestSerializeDVEmpty`).
    #[test]
    fn test_dv_empty_roundtrip() {
        let dv = DeleteVector::default();
        let props = HashMap::from([
            (DELETION_VECTOR_PROPERTY_CARDINALITY.to_string(), "0".to_string()),
            (
                DELETION_VECTOR_PROPERTY_REFERENCED_DATA_FILE.to_string(),
                "data/empty.parquet".to_string(),
            ),
        ]);
        let blob = dv.to_puffin_blob(props).unwrap();
        let restored = DeleteVector::from_puffin_blob(blob).unwrap();
        assert!(restored.is_empty());
        assert_eq!(restored.len(), 0);
    }

    /// Positions straddling the 2^31 (Java-signed) and 2^32 (roaring bucket)
    /// boundaries round-trip (mirrors iceberg-go `TestSerializeDVLargePositions`).
    #[test]
    fn test_dv_boundary_positions_roundtrip() {
        let positions = [100u64, 101, 2_147_483_747, 2_147_483_748, (1u64 << 32) | 42];
        let mut dv = DeleteVector::default();
        for p in positions {
            dv.insert(p);
        }
        let props = HashMap::from([
            (DELETION_VECTOR_PROPERTY_CARDINALITY.to_string(), "5".to_string()),
            (
                DELETION_VECTOR_PROPERTY_REFERENCED_DATA_FILE.to_string(),
                "data/boundary.parquet".to_string(),
            ),
        ]);
        let blob = dv.to_puffin_blob(props).unwrap();
        let restored = DeleteVector::from_puffin_blob(blob).unwrap();
        let mut got: Vec<u64> = restored.iter().collect();
        got.sort();
        assert_eq!(got, positions.to_vec());
    }
}
