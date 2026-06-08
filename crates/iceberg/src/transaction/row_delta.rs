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

//! This module contains the row-delta action — the merge-on-read write commit.
//!
//! [`RowDeltaAction`] adds data files AND adds row-level DELETE files (position / equality) in a
//! single snapshot (Java `BaseRowDelta` / `api/RowDelta.java`). The added delete files are written
//! into a DELETE manifest (`ManifestContentType::Deletes`) alongside the DATA manifest produced for
//! the added data files, both referenced from the same manifest list. The added delete entries
//! inherit the new snapshot's sequence number at read time (the same inheritance mechanism added data
//! files use), so a delete added by this snapshot applies to data written by EARLIER snapshots
//! (`data_seq <= delete_seq`) — the spec's merge-on-read sequence-number rule.
//!
//! This is the write half of merge-on-read: the produced delete files (e.g. from
//! [`crate::writer::base_writer::position_delete_writer::PositionDeleteFileWriter`]) are committed
//! here, and the read side ([`crate::arrow::delete_filter`]) applies them during a scan so the
//! deleted rows are dropped from the result.
//!
//! **Operation recorded:** dynamic, mirroring Java `BaseRowDelta.operation()` exactly — adds-data-only
//! (no delete files) → [`Operation::Append`], adds-deletes-only (no data files) → [`Operation::Delete`],
//! both → [`Operation::Overwrite`]. The snapshot summary carries the added data-file / delete-file and
//! position/equality-delete counts in every case.
//!
//! **Out of scope (deferred):**
//! - Equality-delete WRITER end-to-end (the writer exists; the RowDelta-with-equality-deletes scan
//!   application may have gaps — the end-to-end test focuses on POSITION deletes).
//! - Concurrent-commit conflict validation (`validateFromSnapshot` / `validateNoConflictingDataFiles`
//!   / `validateNoConflictingDeleteFiles` / `validateDeletedFiles` / `validateDataFilesExist`) — these
//!   implement serializable isolation and need validation-history replay across the ancestor chain.
//! - `removeRows` / `removeDeletes` (removing existing data / delete files) — RowDelta the ADD-commit
//!   primitive is this increment's deliverable.
//! - The deletion-vector (V3 Puffin) write path.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{DataFile, ManifestEntry, ManifestFile, Operation};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};

/// A transaction action that performs a row delta: it adds data files AND adds row-level DELETE files
/// (position / equality) in a single snapshot — the merge-on-read write commit (Java `BaseRowDelta`).
///
/// Use [`crate::transaction::Transaction::row_delta`] to create one. Accumulate the data files to add
/// with [`RowDeltaAction::add_data_files`] and the delete files to add with
/// [`RowDeltaAction::add_deletes`], then apply and commit the transaction.
///
/// An add-deletes-only row delta (no data files) and an add-data-only row delta (no delete files) are
/// both allowed; a truly-empty row delta (no data, no deletes, no snapshot properties) is rejected.
pub struct RowDeltaAction {
    /// Data files (rows) to add to the table — validated like fast append (`Data` content type).
    added_data_files: Vec<DataFile>,
    /// DELETE files (position / equality) to add to the table — written into a DELETE manifest.
    added_delete_files: Vec<DataFile>,
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
}

impl RowDeltaAction {
    pub(crate) fn new() -> Self {
        Self {
            added_data_files: vec![],
            added_delete_files: vec![],
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
        }
    }

    /// Add data files (rows) to the table (Java `RowDelta.addRows`). Each file must be `Data` content.
    pub fn add_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(data_files);
        self
    }

    /// Add row-level DELETE files (position / equality) to the table (Java `RowDelta.addDeletes`).
    ///
    /// In Java these are `DeleteFile`s; in this Rust model both data and delete files are [`DataFile`]s
    /// distinguished by their content type. Each file passed here must be `PositionDeletes` or
    /// `EqualityDeletes` content (a `Data` file is rejected at commit).
    pub fn add_deletes(mut self, delete_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_delete_files.extend(delete_files);
        self
    }

    /// Set the commit UUID for the snapshot (otherwise a fresh v7 UUID is generated).
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Set key metadata for manifest files.
    pub fn set_key_metadata(mut self, key_metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(key_metadata);
        self
    }

    /// Set snapshot summary properties.
    pub fn set_snapshot_properties(mut self, snapshot_properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }
}

#[async_trait]
impl TransactionAction for RowDeltaAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
        )
        .with_added_delete_files(self.added_delete_files.clone());

        // Validate the added data files like fast append (Data content type, partition-spec match,
        // partition-value compatibility) and the added delete files (position/equality content type,
        // partition-spec match) — mirroring Java `MergingSnapshotProducer.add(DataFile)` /
        // `add(DeleteFile)`.
        snapshot_producer.validate_added_data_files()?;
        snapshot_producer.validate_added_delete_files()?;

        snapshot_producer
            .commit(
                RowDeltaOperation {
                    // Classified on the REQUESTED sets, before deletes resolve against the table —
                    // matching Java `BaseRowDelta.operation()` (`addsDataFiles()` / `addsDeleteFiles()`).
                    adds_data_files: !self.added_data_files.is_empty(),
                    adds_delete_files: !self.added_delete_files.is_empty(),
                },
                DefaultManifestProcess,
            )
            .await
    }
}

/// The [`SnapshotProduceOperation`] for [`RowDeltaAction`].
///
/// A row delta only ADDS files (data + deletes), so it removes nothing from the existing manifests:
/// `delete_files` returns empty and `existing_manifest` carries every current manifest forward. The
/// added data files reach the producer via `SnapshotProducer::new` and the added delete files via
/// `with_added_delete_files`, so the single snapshot carries the new DATA manifest and the new DELETE
/// manifest alongside the carried-forward manifests.
struct RowDeltaOperation {
    /// Whether this row delta requested any added data files (Java `addsDataFiles()`).
    adds_data_files: bool,
    /// Whether this row delta requested any added delete files (Java `addsDeleteFiles()`).
    adds_delete_files: bool,
}

impl SnapshotProduceOperation for RowDeltaOperation {
    /// Classify the recorded operation exactly as Java `BaseRowDelta.operation()` does, on the
    /// REQUESTED add sets: adds-data-only (no delete files) → [`Operation::Append`], adds-deletes-only
    /// (no data files) → [`Operation::Delete`], both → [`Operation::Overwrite`]. An empty row delta
    /// (neither) is rejected by the producer before this is read.
    fn operation(&self) -> Operation {
        if self.adds_data_files && !self.adds_delete_files {
            Operation::Append
        } else if self.adds_delete_files && !self.adds_data_files {
            Operation::Delete
        } else {
            Operation::Overwrite
        }
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        Ok(vec![])
    }

    async fn delete_files(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<DataFile>> {
        // A row delta removes no existing files (it only ADDS data + deletes).
        Ok(vec![])
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        // Carry every current manifest (data AND delete) forward unchanged — a row delta adds new
        // manifests without rewriting existing ones (Java `MergingSnapshotProducer` keeps all existing
        // manifests for an add-only operation).
        let Some(snapshot) = snapshot_produce.table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_produce.table.file_io(),
                &snapshot_produce.table.metadata_ref(),
            )
            .await?;

        Ok(manifest_list.entries().to_vec())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use futures::TryStreamExt;

    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, ManifestContentType,
        ManifestStatus, Operation, Struct,
    };
    use crate::table::Table;
    use crate::transaction::tests::make_v3_minimal_table_in_catalog;
    use crate::transaction::{ApplyTransactionAction, Transaction};
    use crate::writer::base_writer::position_delete_writer::{
        PositionDeleteFileWriterBuilder, PositionDeleteWriterConfig,
    };
    use crate::writer::file_writer::ParquetWriterBuilder;
    use crate::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use crate::writer::{IcebergWriter, IcebergWriterBuilder};
    use crate::{Catalog, ErrorKind};

    /// A position-delete file describing a `DataFile` (content `PositionDeletes`) routed to partition
    /// `x = part_value`, with a unique path (NOT a real parquet file — used for manifest-only tests).
    fn synthetic_delete_file(path: &str, part_value: i64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(part_value))]))
            .build()
            .unwrap()
    }

    /// A synthetic data file routed to partition `x = part_value` (NOT a real parquet file).
    fn synthetic_data_file(path: &str, part_value: i64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(part_value))]))
            .build()
            .unwrap()
    }

    /// Append the given data files in a single fast-append commit and return the updated table.
    async fn append_files(catalog: &impl Catalog, table: &Table, files: Vec<DataFile>) -> Table {
        let tx = Transaction::new(table);
        let action = tx.fast_append().add_data_files(files);
        let tx = action.apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    /// Read a u64 total from a snapshot summary property, defaulting to 0 when absent.
    fn summary_prop(table: &Table, prop: &str) -> Option<String> {
        table
            .metadata()
            .current_snapshot()
            .unwrap()
            .summary()
            .additional_properties
            .get(prop)
            .cloned()
    }

    // ------------------------------------------------------------------------------------------------
    // THE CROWN-JEWEL END-TO-END TEST
    // ------------------------------------------------------------------------------------------------

    /// THE deliverable. Proves the entire merge-on-read WRITE → READ chain:
    /// 1. create a table, fast-append a REAL parquet data file with known rows (x=0 partition, y =
    ///    [10,20,30,40,50]);
    /// 2. produce a REAL position-delete file with the 5a `PositionDeleteFileWriter` pointing at that
    ///    data file at positions {1, 3} (the rows y=20 and y=40);
    /// 3. `row_delta().add_deletes([that delete file]).commit()`;
    /// 4. SCAN the table and assert the deleted rows are ABSENT — the result is exactly {10, 30, 50}.
    ///
    /// Risk pinned: deletes not applied = the feature silently does nothing (a scan that still returns
    /// the deleted rows means RowDelta committed a delete file the read side never honored). Mangled
    /// positions = wrong rows deleted. This is the only test that proves the write path produces delete
    /// files the scan actually applies.
    #[tokio::test]
    async fn test_row_delta_position_deletes_drop_deleted_rows_from_scan() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // 1. Write a real parquet data file with 5 rows, all in partition x=0, y = [10,20,30,40,50].
        let data_file = write_data_file(&table, "rows.parquet", 0, &[
            (0, 10, 100),
            (0, 20, 200),
            (0, 30, 300),
            (0, 40, 400),
            (0, 50, 500),
        ])
        .await;
        let data_file_path = data_file.file_path().to_string();
        let table = append_files(&catalog, &table, vec![data_file]).await;

        // Sanity: before any delete, the scan returns all five y values.
        let before: HashSet<i64> = scan_y_values(&table).await;
        assert_eq!(
            before,
            HashSet::from([10, 20, 30, 40, 50]),
            "before the row delta, the scan returns all five rows"
        );

        // 2. Produce a REAL position-delete file deleting positions 1 and 3 (y=20 and y=40).
        let delete_file = write_position_delete_file(&table, 0, &[
            (data_file_path.clone(), 1),
            (data_file_path.clone(), 3),
        ])
        .await;
        assert_eq!(delete_file.content_type(), DataContentType::PositionDeletes);

        // 3. RowDelta: add the delete file in one snapshot.
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![delete_file]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // 4. Scan: the deleted rows (y=20, y=40) must be ABSENT — the surviving rows are {10, 30, 50}.
        let after: HashSet<i64> = scan_y_values(&table).await;
        assert_eq!(
            after,
            HashSet::from([10, 30, 50]),
            "after the row delta, the scan drops the deleted rows (y=20 and y=40)"
        );
    }

    /// THE FORWARD-APPLICATION NEGATIVE (spec line 1071: a position delete applies only when
    /// `data_seq <= delete_seq`). Proves the delete's sequence number does NOT reach FORWARD to data
    /// written by a LATER snapshot — the corruption inverse of the crown jewel. Scenario:
    /// 1. append D1 (seq 1) with y = [10,20,30,40,50] in partition x=0;
    /// 2. `row_delta().add_deletes` a position-delete for D1 at positions {1,3} (seq 2);
    /// 3. append a NEW data file D2 (seq 3) in the SAME partition x=0 with y = [60,70,80,90,100] —
    ///    D2 ALSO has live rows at positions 1 and 3 (y=70, y=90).
    /// Assert: the scan drops only D1's pos {1,3} (y=20,40 gone) and keeps EVERY D2 row — the delete's
    /// seq 2 does NOT reach D2's seq 3 (`3 <= 2` is false), so D2 is fully intact even though it shares
    /// the partition AND has rows at the deleted positions. A wrong-forward (delete reaching D2) would
    /// wrongly drop y=70 and y=90. This is the test the seq-inheritance correctness hinges on: it isolates
    /// the SEQUENCE dimension (same partition, same positions) so ONLY the seq guard can save D2.
    #[tokio::test]
    async fn test_row_delta_position_delete_does_not_apply_to_later_data() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // 1. D1 (seq 1): y = [10,20,30,40,50], partition x=0.
        let d1 = write_data_file(&table, "d1.parquet", 0, &[
            (0, 10, 100),
            (0, 20, 200),
            (0, 30, 300),
            (0, 40, 400),
            (0, 50, 500),
        ])
        .await;
        let d1_path = d1.file_path().to_string();
        let table = append_files(&catalog, &table, vec![d1]).await;
        let d1_seq = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .sequence_number();

        // 2. RowDelta a position delete for D1 at positions {1,3} (seq 2).
        let delete_file =
            write_position_delete_file(&table, 0, &[(d1_path.clone(), 1), (d1_path.clone(), 3)])
                .await;
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![delete_file]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let delete_seq = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .sequence_number();

        // 3. Append D2 (seq 3) in the SAME partition x=0 with rows at positions 1 and 3 too.
        let d2 = write_data_file(&table, "d2.parquet", 0, &[
            (0, 60, 600),
            (0, 70, 700),
            (0, 80, 800),
            (0, 90, 900),
            (0, 100, 1000),
        ])
        .await;
        let table = append_files(&catalog, &table, vec![d2]).await;
        let d2_seq = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .sequence_number();

        // Sanity on the sequence ordering: data(1) < delete(2) < later-data(3).
        assert!(
            d1_seq < delete_seq && delete_seq < d2_seq,
            "expected d1_seq({d1_seq}) < delete_seq({delete_seq}) < d2_seq({d2_seq})"
        );

        // The scan must drop D1's pos {1,3} (y=20,40) but keep EVERY D2 row — the delete does not reach
        // forward to data added in a later snapshot.
        let after: HashSet<i64> = scan_y_values(&table).await;
        assert_eq!(
            after,
            HashSet::from([10, 30, 50, 60, 70, 80, 90, 100]),
            "the delete (seq 2) drops only D1's pos 1,3 (y=20,40); D2 (seq 3) is fully intact"
        );
        // Belt-and-suspenders: the rows at the SAME positions in D2 (y=70 at pos 1, y=90 at pos 3) must
        // survive — proving it is the sequence number, not the position, that spares D2.
        assert!(
            after.contains(&70) && after.contains(&90),
            "D2's rows at the deleted positions must survive (the delete's seq does not reach forward)"
        );
    }

    // ------------------------------------------------------------------------------------------------
    // Manifest / summary / sequence-number tests (use synthetic files — no scan)
    // ------------------------------------------------------------------------------------------------

    /// Pins: a row delta that adds a delete file writes a DELETE manifest (`content == Deletes`) and
    /// references it in the snapshot's manifest list (alongside any DATA manifests). Risk: the delete
    /// file silently going into a DATA manifest (Java cannot read it / the read side never indexes it),
    /// or no delete manifest being written at all.
    #[tokio::test]
    async fn test_row_delta_writes_delete_manifest_with_deletes_content() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/a-pos-del.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        let delete_manifests: Vec<_> = manifest_list
            .entries()
            .iter()
            .filter(|m| m.content == ManifestContentType::Deletes)
            .collect();
        assert_eq!(
            delete_manifests.len(),
            1,
            "exactly one DELETE manifest must be written and referenced in the manifest list"
        );

        // The delete manifest's single entry is the added position-delete file with Deletes content.
        let delete_manifest = delete_manifests[0]
            .load_manifest(table.file_io())
            .await
            .unwrap();
        let entries: Vec<_> = delete_manifest.entries().iter().collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].content_type(), DataContentType::PositionDeletes);
        assert_eq!(entries[0].file_path(), "test/a-pos-del.parquet");
        assert_eq!(entries[0].status(), ManifestStatus::Added);
    }

    /// Pins: a row delta can add data files AND delete files in ONE snapshot — a DATA manifest and a
    /// DELETE manifest both land in the same manifest list, and the operation is `Overwrite` (Java
    /// `BaseRowDelta.operation()` = OVERWRITE when both data and deletes are added). Risk: only one of
    /// the two manifests being written, or the wrong operation recorded.
    #[tokio::test]
    async fn test_row_delta_add_data_and_deletes_in_one_snapshot() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_data_files(vec![synthetic_data_file("test/b.parquet", 0)])
            .add_deletes(vec![synthetic_delete_file("test/a-pos-del.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(
            snapshot.summary().operation,
            Operation::Overwrite,
            "adds-data + adds-deletes records Overwrite (Java BaseRowDelta.operation())"
        );

        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        // Collect the live data + delete file paths across the new snapshot's manifests, keyed by the
        // manifest's content type, so we can prove the added data file landed in a DATA manifest and the
        // added delete file landed in a DELETE manifest (and the prior fast-appended file survives).
        let mut data_paths = HashSet::new();
        let mut delete_paths = HashSet::new();
        let mut delete_manifest_count = 0;
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            if manifest_file.content == ManifestContentType::Deletes {
                delete_manifest_count += 1;
            }
            for entry in manifest.entries() {
                if !entry.is_alive() {
                    continue;
                }
                match manifest_file.content {
                    ManifestContentType::Data => {
                        data_paths.insert(entry.file_path().to_string());
                    }
                    ManifestContentType::Deletes => {
                        delete_paths.insert(entry.file_path().to_string());
                    }
                }
            }
        }

        assert!(
            data_paths.contains("test/b.parquet"),
            "the added data file b.parquet lands in a DATA manifest; data paths = {data_paths:?}"
        );
        assert!(
            data_paths.contains("test/a.parquet"),
            "the prior fast-appended data file a.parquet survives"
        );
        assert_eq!(
            delete_manifest_count, 1,
            "exactly one DELETE manifest is written in the row-delta snapshot"
        );
        assert!(
            delete_paths.contains("test/a-pos-del.parquet"),
            "the added delete file lands in the DELETE manifest; delete paths = {delete_paths:?}"
        );
    }

    /// Pins: an add-deletes-only row delta (no data files) is ALLOWED (the relaxed precondition) and
    /// records `Delete` (Java `BaseRowDelta.operation()` = DELETE when only deletes are added). Risk:
    /// the producer's empty-commit precondition wrongly rejecting an add-deletes-only commit.
    #[tokio::test]
    async fn test_row_delta_add_deletes_only_allowed() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/a-pos-del.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Delete,
            "an add-deletes-only row delta records Delete (Java BaseRowDelta.operation())"
        );
    }

    /// Pins: an add-DATA-only row delta (no delete files) records `Append` (Java
    /// `BaseRowDelta.operation()` = APPEND when only data files are added and nothing is removed). Risk:
    /// the dynamic-op classifier wrongly recording `Overwrite`/`Delete` for a pure-add row delta. Note:
    /// this increment never removes files (`removeRows`/`removeDeletes` deferred), so Java's extra
    /// `!deletesDataFiles()` guard on the APPEND branch is always satisfied here.
    #[tokio::test]
    async fn test_row_delta_add_data_only_records_append() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_data_files(vec![synthetic_data_file("test/b.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Append,
            "an add-data-only row delta records Append (Java BaseRowDelta.operation())"
        );
    }

    /// Pins the row-delta SUMMARY counts: an add-data + add-position-delete row delta reports one added
    /// data file, one added delete file, one added position-delete file, and the right record/delete
    /// counts. Risk: the summary not reflecting the added delete files (downstream tooling that reads
    /// `added-delete-files`/`added-position-deletes` would under-report).
    #[tokio::test]
    async fn test_row_delta_summary_reflects_added_data_and_delete_counts() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        // The delete file carries record_count 3 (three deleted positions).
        let delete_file = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/a-pos-del.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(3)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(0))]))
            .build()
            .unwrap();

        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_data_files(vec![synthetic_data_file("test/b.parquet", 0)])
            .add_deletes(vec![delete_file]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(
            summary_prop(&table, "added-data-files").as_deref(),
            Some("1"),
            "one added data file"
        );
        assert_eq!(
            summary_prop(&table, "added-delete-files").as_deref(),
            Some("1"),
            "one added delete file"
        );
        assert_eq!(
            summary_prop(&table, "added-position-delete-files").as_deref(),
            Some("1"),
            "one added position-delete file"
        );
        assert_eq!(
            summary_prop(&table, "added-position-deletes").as_deref(),
            Some("3"),
            "three added position deletes (the delete file's record count)"
        );
    }

    /// Pins the SEQUENCE-NUMBER correctness — the wrong-seq risk: "deletes apply to wrong data". The
    /// added delete entry must carry the NEW snapshot's sequence number (inherited at read time), which
    /// is STRICTLY GREATER than the earlier data file's sequence number, so the delete applies to that
    /// earlier data (`data_seq <= delete_seq`). Risk: stamping the delete entry with an old/zero seq
    /// (so it would NOT apply to existing data) or the data file's own seq.
    #[tokio::test]
    async fn test_row_delta_added_delete_entry_inherits_new_snapshot_sequence_number() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Append data in its own snapshot → it gets data sequence number 1.
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;
        let data_snapshot = table.metadata().current_snapshot().unwrap();
        let data_seq = data_snapshot.sequence_number();

        // RowDelta the delete in a LATER snapshot.
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/a-pos-del.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let delete_snapshot = table.metadata().current_snapshot().unwrap();
        let delete_seq = delete_snapshot.sequence_number();
        assert!(
            delete_seq > data_seq,
            "the row-delta snapshot's sequence number ({delete_seq}) must exceed the data snapshot's ({data_seq})"
        );

        // The added delete entry must read back with the NEW snapshot's sequence number (inherited),
        // NOT a stale or zero seq — this is what makes the delete apply to the earlier data.
        let manifest_list = delete_snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let mut found = false;
        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Deletes {
                continue;
            }
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                if entry.file_path() == "test/a-pos-del.parquet" {
                    assert_eq!(
                        entry.sequence_number(),
                        Some(delete_seq),
                        "the added delete entry inherits the new snapshot's sequence number"
                    );
                    assert_eq!(
                        entry.snapshot_id(),
                        Some(delete_snapshot.snapshot_id()),
                        "the added delete entry carries the new snapshot id"
                    );
                    found = true;
                }
            }
        }
        assert!(
            found,
            "the added delete entry must be present in a DELETE manifest"
        );
    }

    /// Pins: `add_deletes` rejects a `Data`-content file (a delete file must be position/equality
    /// content). Risk: a data file silently committed as a delete (corrupting the table — it would be
    /// indexed as a delete file and never read as data).
    #[tokio::test]
    async fn test_row_delta_rejects_data_content_in_add_deletes() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        let tx = Transaction::new(&table);
        // A Data-content file passed to add_deletes must be rejected.
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_data_file("test/not-a-delete.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let err = tx
            .commit(&catalog)
            .await
            .expect_err("a Data-content file in add_deletes must be rejected");
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message().contains("position-delete or equality-delete"),
            "unexpected error: {}",
            err.message()
        );
    }

    /// Pins: a delete file whose partition spec id does not match the table default is rejected. Risk:
    /// a mismatched-spec delete file that the read side cannot associate to the right partition.
    #[tokio::test]
    async fn test_row_delta_rejects_partition_spec_mismatch() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        let bad_delete = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/bad-spec.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            // Wrong partition spec id (table default is 0).
            .partition_spec_id(999)
            .partition(Struct::from_iter([Some(Literal::long(0))]))
            .build()
            .unwrap();

        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![bad_delete]);
        let tx = action.apply(tx).unwrap();
        let err = tx
            .commit(&catalog)
            .await
            .expect_err("a partition-spec-mismatched delete file must be rejected");
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message().contains("partition spec id"),
            "unexpected error: {}",
            err.message()
        );
    }

    /// Pins: a truly-empty row delta (no data, no deletes, no snapshot properties) is REJECTED. Risk:
    /// the relaxed precondition being too permissive and producing an empty no-op snapshot.
    #[tokio::test]
    async fn test_empty_row_delta_is_rejected() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        let tx = Transaction::new(&table);
        let action = tx.row_delta();
        let tx = action.apply(tx).unwrap();
        let result = tx.commit(&catalog).await;
        assert!(result.is_err(), "a truly-empty row delta must be rejected");
    }

    // ------------------------------------------------------------------------------------------------
    // Crown-jewel helpers: write REAL parquet data + position-delete files into the table's FileIO.
    // ------------------------------------------------------------------------------------------------

    /// Write a real parquet DATA file with the (x, y, z) rows into the table's location and return a
    /// [`DataFile`] describing it (content `Data`, partition `x = part_value`, spec id 0). The file is
    /// written via the table's own `FileIO` so the scan can read it back.
    async fn write_data_file(
        table: &Table,
        file_name: &str,
        part_value: i64,
        rows: &[(i64, i64, i64)],
    ) -> DataFile {
        use crate::arrow::schema_to_arrow_schema;
        use crate::writer::file_writer::{FileWriter, FileWriterBuilder};

        let schema = table.metadata().current_schema();
        let arrow_schema = Arc::new(schema_to_arrow_schema(schema).unwrap());

        let xs: Vec<i64> = rows.iter().map(|(x, _, _)| *x).collect();
        let ys: Vec<i64> = rows.iter().map(|(_, y, _)| *y).collect();
        let zs: Vec<i64> = rows.iter().map(|(_, _, z)| *z).collect();
        let batch = RecordBatch::try_new(arrow_schema, vec![
            Arc::new(Int64Array::from(xs)) as ArrayRef,
            Arc::new(Int64Array::from(ys)) as ArrayRef,
            Arc::new(Int64Array::from(zs)) as ArrayRef,
        ])
        .unwrap();

        // Write the parquet directly under the table location so the scan's FileIO can read it.
        let file_path = format!("{}/data/{}", table.metadata().location(), file_name);
        let output = table.file_io().new_output(file_path.clone()).unwrap();
        let parquet_builder = ParquetWriterBuilder::new(
            parquet::file::properties::WriterProperties::builder().build(),
            schema.clone(),
        );
        let mut writer = parquet_builder.build(output).await.unwrap();
        writer.write(&batch).await.unwrap();
        let data_file_builders = writer.close().await.unwrap();

        // The parquet writer returns builders without content/partition stamped — finish them as a
        // partitioned data file.
        let mut builder = data_file_builders.into_iter().next().unwrap();
        builder
            .content(DataContentType::Data)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(part_value))]))
            .build()
            .unwrap()
    }

    /// Write a REAL position-delete parquet file (via the 5a `PositionDeleteFileWriter`) into the
    /// table's location, deleting the given `(data_file_path, pos)` pairs, in partition `x = part_value`.
    async fn write_position_delete_file(
        table: &Table,
        part_value: i64,
        deletes: &[(String, i64)],
    ) -> DataFile {
        let config = PositionDeleteWriterConfig::new().unwrap();

        let location_gen = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
        let file_name_gen = DefaultFileNameGenerator::new(
            "pos-del".to_string(),
            Some(uuid::Uuid::now_v7().to_string()),
            DataFileFormat::Parquet,
        );
        let parquet_builder = ParquetWriterBuilder::new(
            parquet::file::properties::WriterProperties::builder().build(),
            config.schema().clone(),
        );
        let rolling = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_builder,
            table.file_io().clone(),
            location_gen,
            file_name_gen,
        );

        // Build with the partition key so the delete file's partition matches the data file's (the
        // delete-file index keys position deletes by partition + spec id).
        let partition_key = crate::spec::PartitionKey::new(
            table.metadata().default_partition_spec().as_ref().clone(),
            table.metadata().current_schema().clone(),
            Struct::from_iter([Some(Literal::long(part_value))]),
        );
        let mut writer = PositionDeleteFileWriterBuilder::new(rolling, config.clone())
            .build(Some(partition_key))
            .await
            .unwrap();

        let paths: Vec<&str> = deletes.iter().map(|(p, _)| p.as_str()).collect();
        let positions: Vec<i64> = deletes.iter().map(|(_, pos)| *pos).collect();
        let batch = RecordBatch::try_new(config.arrow_schema().clone(), vec![
            Arc::new(StringArray::from(paths)) as ArrayRef,
            Arc::new(Int64Array::from(positions)) as ArrayRef,
        ])
        .unwrap();
        writer.write(batch).await.unwrap();
        writer.close().await.unwrap().into_iter().next().unwrap()
    }

    /// Scan the table and collect the `y` column values across all returned batches.
    async fn scan_y_values(table: &Table) -> HashSet<i64> {
        let stream = table
            .scan()
            .select(["y"])
            .build()
            .unwrap()
            .to_arrow()
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let mut values = HashSet::new();
        for batch in batches {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..col.len() {
                values.insert(col.value(i));
            }
        }
        values
    }
}
