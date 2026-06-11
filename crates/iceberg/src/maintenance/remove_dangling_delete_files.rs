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

//! The [`RemoveDanglingDeleteFiles`] maintenance action — the Rust port of Java's
//! `RemoveDanglingDeletesSparkAction` (spark module, MAIN-only) behind the
//! `org.apache.iceberg.actions.RemoveDanglingDeleteFiles` api interface (1.10.0 bytecode-confirmed
//! shape: `Result.removedDeleteFiles()` returning the removed delete files).
//!
//! A delete file is **dangling** when its deletes can no longer apply to ANY live data file. Removing
//! it is pure garbage collection — the read result is unchanged. The corruption hazard is the inverse:
//! removing a delete file that STILL applies resurrects the rows it was masking (the classic
//! GC-of-deletes corruption). Every dangling test below therefore pins BOTH directions — the
//! still-applicable delete is KEPT, the genuinely-dangling one is REMOVED.
//!
//! # The dangling predicate (derived verbatim from Java's `findDanglingDeletes` SQL)
//!
//! Over the **current snapshot only** (Java loads the `ENTRIES` / `DATA_FILES` / `DELETE_FILES`
//! metadata tables, which materialize the current snapshot):
//!
//! 1. Group LIVE DATA entries (content == DATA, `status < DELETED`) by `(partition, spec_id)` and take
//!    the MINIMUM data sequence number in each group (Java: `groupBy("partition", "spec_id").agg(min
//!    (sequence_number))`).
//! 2. Left-outer-join each LIVE DELETE entry (content != DATA, `status < DELETED`) to that grouping on
//!    `(spec_id, partition)`.
//! 3. A delete file is dangling when (Java `filterOnDanglingDeletes`, lines 152-165):
//!    - `min_data_sequence_number IS NULL` — there is NO live data file in the delete's partition+spec,
//!      so it applies to nothing (ANY content type), OR
//!    - it is a **position** delete (content == 1) AND `sequence_number < min_data_sequence_number`
//!      (STRICT `<`), OR
//!    - it is an **equality** delete (content == 2) AND `sequence_number <= min_data_sequence_number`
//!      (NON-strict `<=`).
//! 4. A **deletion vector** (PUFFIN-format position delete) whose `referenced_data_file` is not a live
//!    data-file path is dangling (Java `findDanglingDvs`: left-outer-join DVs to DATA_FILES on
//!    `referenced_data_file == file_path`, keep the unmatched).
//!
//! **THE OFF-BY-ONE (the corruption edge): position deletes use `<`, equality deletes use `<=`.** This
//! is the exact complement of the read-path applicability rules in [`crate::delete_file_index`]: a
//! position delete APPLIES to a data file when `delete_seq >= data_seq`
//! (`PopulatedDeleteFileIndex::get_deletes_for_data_file`, the `>=` position branch), so it applies to
//! the partition's minimum-seq data file iff `delete_seq >= min` — i.e. it dangles iff `delete_seq <
//! min`. An equality delete APPLIES STRICTLY when `delete_seq > data_seq` (the `>` equality branch), so
//! it applies to the minimum-seq data file iff `delete_seq > min` — i.e. it dangles iff `delete_seq <=
//! min`. Position deletes apply to same-seq data (`>=`), equality deletes do not (`>`); the dangling
//! conditions therefore differ by one between the two content types. Flipping `<` to `<=` (or vice
//! versa) is exactly the mutation that either resurrects same-seq rows or leaves a genuinely-dangling
//! delete behind.
//!
//! # Grouping is per `(partition, spec_id)`
//!
//! Java groups the data minimum AND joins the deletes on BOTH the partition tuple AND the spec id. Two
//! files in the same partition tuple but DIFFERENT specs do not share a minimum — a delete is compared
//! only against data files written under its own spec's partitioning. This action mirrors that exactly:
//! the grouping key is `(spec_id, partition_struct)`.
//!
//! # The commit vehicle: `RewriteFiles.deleteFile(DeleteFile)`
//!
//! Java commits via `table.newRewrite()` then `rewriteFiles.deleteFile(deleteFile)` per dangling delete,
//! committing only if the dangling set is non-empty. The recorded operation is `Replace` (Java
//! `BaseRewriteFiles.operation()` returns `"replace"` unconditionally, even for a delete-only rewrite —
//! 1.10.0 bytecode). This action drives the SAME vehicle: the deferred delete-file-removal surface on
//! [`RewriteFilesAction`](crate::transaction::rewrite_files) (landed in this increment) —
//! `tx.rewrite_files(vec![], vec![]).delete_delete_files(dangling)`. The producer resolves each path
//! against the current DELETE manifests, tombstones it, and bumps `removed-position-delete-files` /
//! `removed-equality-delete-files` / `removed-dvs`.
//!
//! # Relationship to plain `RewriteFiles` (the carry-posture)
//!
//! A plain [`RewriteFilesAction`](crate::transaction::rewrite_files) that rewrites the data file a
//! position-delete REFERENCES does NOT prune the now-dangling parquet position delete — it carries every
//! delete manifest forward unchanged (the conservative carry-unchanged posture documented on
//! `SnapshotProducer::current_manifests`; empirically confirmed against Java 1.10.0, which on a
//! `RewriteFiles` prunes only dangling *DVs*, never a dangling parquet position delete). THIS action is
//! the thing that cleans those up — it is the dedicated dangling-delete GC pass the commit path
//! deliberately does not do inline.
//!
//! # The unpartitioned single-spec early return
//!
//! Java short-circuits to an empty result when the table has exactly one spec AND it is unpartitioned:
//! "ManifestFilterManager already performs this table-wide delete on each commit." This action mirrors
//! that — on such a table there is nothing for it to do, and no snapshot is committed.
//!
//! # Removal is METADATA-ONLY — time travel is preserved
//!
//! Removing a dangling delete commits a NEW `Replace` snapshot that TOMBSTONES the delete entry in the
//! rewritten DELETE manifest (`process_deletes` → `add_delete_entry`); it does NOT physically delete the
//! delete file's bytes. Older snapshots' manifests still reference the removed delete file, and the bytes
//! stay on disk, so time-travel reads of a prior snapshot are unaffected. Physical reclamation is the job
//! of `ExpireSnapshots` / `DeleteOrphanFiles`, never this action (Java-identical — `RewriteFiles` is a
//! metadata commit).
//!
//! # Concurrency: a KNOWN Java-faithful resurrection race (NOT validated)
//!
//! The dangling determination is computed against the snapshot in hand and the removal commits with NO
//! concurrent-conflict validation: a delete-file-only `RewriteFiles` has an EMPTY replaced-DATA set, and
//! both Java's `BaseRewriteFiles.validate` (it runs `validateNoNewDeletesForDataFiles` only
//! `if (!replacedDataFiles.isEmpty())`, 1.10.0 bytecode) and the `RewriteFilesAction::validate` it drives
//! skip the check in that case. Java's `RemoveDanglingDeletesSparkAction` adds no `validateFromSnapshot`
//! either. So if a
//! CONCURRENT seq-preserving compaction (Java `RewriteDataFiles` with `use-starting-sequence-number`, the
//! Rust [`RewriteDataFiles`](crate::maintenance::RewriteDataFiles)) lands a data file at an OLD (lower)
//! data sequence number in a dangling delete's partition between the plan and the commit, that delete can
//! become APPLICABLE again at commit time and its removal then RESURRECTS the rows it masked. This race
//! exists IDENTICALLY in Java (no conflict detection on the delete-only rewrite); the Rust action is pinned
//! to Java's behavior, so it is documented here rather than guarded (a guard would diverge from Java).
//! Operationally, run dangling-delete GC when concurrent seq-preserving compaction is not in flight.
//! (Reviewer-confirmed 2026-06-11: a probe staged exactly this interleaving and observed the resurrection.)
//!
//! # Global (unpartitioned-spec) equality deletes under a multi-spec table
//!
//! A global equality delete is written under an UNPARTITIONED spec (empty partition struct). The read path
//! ([`crate::delete_file_index`], `global_equality_deletes`, and Java `DeleteFileIndex.findGlobalDeletes`)
//! applies it TABLE-WIDE — to data under ANY spec/partition when `delete_seq > data_seq`. This action keys
//! it by its own `(spec_id, empty-partition)` group, so on a table whose only live data is under a
//! DIFFERENT (partitioned) spec, the global eq delete's group has no live data (`min IS NULL`) and the
//! action flags it DANGLING even though the reader still honors it cross-spec. This is faithful to Java:
//! `findDanglingDeletes` joins on `spec_id AND partition`, so the same global eq delete misses the
//! partitioned-spec data and is flagged dangling there too — the action↔reader inconsistency is inherited
//! 1:1 from Java, not introduced here. Reachable only after an unpartitioned→partitioned spec evolution
//! that leaves a global eq delete live with no unpartitioned-spec data.

use std::collections::{HashMap, HashSet};

use crate::Catalog;
use crate::error::Result;
use crate::spec::{DataContentType, DataFile, DataFileFormat, Struct};
use crate::table::Table;
use crate::transaction::{ApplyTransactionAction, Transaction};

/// The outcome of a [`RemoveDanglingDeleteFiles::execute`] run: the delete files that were removed
/// (Java `RemoveDanglingDeleteFiles.Result.removedDeleteFiles()`), plus per-content-type counts for
/// convenient assertion. A no-op run (nothing dangled, or the unpartitioned single-spec early return)
/// returns this empty and commits no snapshot.
#[derive(Debug, Default, Clone)]
pub struct RemoveDanglingDeleteFilesResult {
    /// Every dangling delete file that was removed (Java `Result.removedDeleteFiles()`), in the order
    /// they were discovered. Each is the on-disk [`DataFile`] of the removed delete entry.
    pub removed_delete_files: Vec<DataFile>,
}

impl RemoveDanglingDeleteFilesResult {
    /// Number of removed position-delete files that are NOT deletion vectors (parquet position deletes).
    pub fn removed_position_delete_files_count(&self) -> usize {
        self.removed_delete_files
            .iter()
            .filter(|file| {
                file.content_type() == DataContentType::PositionDeletes
                    && file.file_format() != DataFileFormat::Puffin
            })
            .count()
    }

    /// Number of removed equality-delete files.
    pub fn removed_equality_delete_files_count(&self) -> usize {
        self.removed_delete_files
            .iter()
            .filter(|file| file.content_type() == DataContentType::EqualityDeletes)
            .count()
    }

    /// Number of removed deletion vectors (PUFFIN-format position deletes).
    pub fn removed_dvs_count(&self) -> usize {
        self.removed_delete_files
            .iter()
            .filter(|file| {
                file.content_type() == DataContentType::PositionDeletes
                    && file.file_format() == DataFileFormat::Puffin
            })
            .count()
    }
}

/// The `RemoveDanglingDeleteFiles` maintenance action. Build it with [`Self::new`] and run it with
/// [`Self::execute`]. It removes, in ONE `Replace` snapshot, every delete file in the current snapshot
/// that can no longer apply to any live data file (see the module docs for the exact dangling predicate
/// per content type and the commit vehicle).
pub struct RemoveDanglingDeleteFiles {
    table: Table,
}

impl RemoveDanglingDeleteFiles {
    /// Create a `RemoveDanglingDeleteFiles` action for `table`.
    pub fn new(table: Table) -> Self {
        RemoveDanglingDeleteFiles { table }
    }

    /// Find the dangling delete files in the current snapshot and remove them in one `Replace` snapshot
    /// committed through the [`RewriteFilesAction`](crate::transaction::rewrite_files) delete-file-removal
    /// surface (Java `RemoveDanglingDeletesSparkAction.doExecute`).
    ///
    /// Returns an empty [`RemoveDanglingDeleteFilesResult`] and commits NOTHING when nothing dangles, when
    /// the table has no current snapshot, or when the table is unpartitioned with a single spec (Java's
    /// early return — `ManifestFilterManager` already prunes table-wide on each commit). Returns `Err` only
    /// when reading the manifests fails or the commit fails.
    pub async fn execute(self, catalog: &dyn Catalog) -> Result<RemoveDanglingDeleteFilesResult> {
        // Java early return: an unpartitioned, single-spec table has nothing to do — every commit's
        // ManifestFilterManager already drops table-wide-applicable deletes.
        let metadata = self.table.metadata();
        if metadata.partition_specs_iter().count() == 1
            && metadata.default_partition_spec().is_unpartitioned()
        {
            return Ok(RemoveDanglingDeleteFilesResult::default());
        }

        // The action scopes to the CURRENT snapshot only (Java's metadata-table loads).
        let Some(snapshot) = metadata.current_snapshot().cloned() else {
            return Ok(RemoveDanglingDeleteFilesResult::default());
        };

        let live = self.collect_live_entries(&snapshot).await?;
        let dangling = find_dangling_deletes(&live);

        if dangling.is_empty() {
            // Empty plan — no-op, NO commit (Java commits only when `!danglingDeletes.isEmpty()`).
            return Ok(RemoveDanglingDeleteFilesResult::default());
        }

        // Commit ONE RewriteFiles delete-file removal (Java `table.newRewrite()` +
        // `deleteFile(deleteFile)` per dangling file). Operation = Replace.
        let transaction = Transaction::new(&self.table);
        let action = transaction
            .rewrite_files(Vec::new(), Vec::new())
            .delete_delete_files(dangling.clone());
        let transaction = action.apply(transaction)?;
        transaction.commit(catalog).await?;

        Ok(RemoveDanglingDeleteFilesResult {
            removed_delete_files: dangling,
        })
    }

    /// Walk the current snapshot's manifests once and collect every LIVE entry's relevant fields into a
    /// [`LiveEntries`] view (live data sequence numbers grouped by `(spec_id, partition)`, the set of live
    /// data-file paths, and the live delete entries' [`DataFile`]s). One pass over the manifest list.
    async fn collect_live_entries(&self, snapshot: &crate::spec::Snapshot) -> Result<LiveEntries> {
        let metadata = self.table.metadata();
        let manifest_list = snapshot
            .load_manifest_list(self.table.file_io(), metadata)
            .await?;

        let mut live = LiveEntries::default();
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(self.table.file_io()).await?;
            for entry in manifest.entries() {
                if !entry.is_alive() {
                    continue;
                }
                let data_file = entry.data_file();
                match entry.content_type() {
                    DataContentType::Data => {
                        // A live DATA entry: contribute its (post-inheritance) data sequence number to its
                        // partition+spec minimum, and record its path (for the DV reference check).
                        let key = (data_file.partition_spec_id, data_file.partition().clone());
                        let seq = entry.sequence_number();
                        live.min_data_seq_by_group
                            .entry(key)
                            .and_modify(|current| *current = min_option(*current, seq))
                            .or_insert(seq);
                        live.live_data_file_paths
                            .insert(data_file.file_path().to_string());
                    }
                    DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                        live.live_delete_entries.push(LiveDeleteEntry {
                            data_file: data_file.clone(),
                            sequence_number: entry.sequence_number(),
                        });
                    }
                }
            }
        }

        Ok(live)
    }
}

/// The per-spec partition tuple a data/delete file belongs to (Java groups + joins on BOTH).
type GroupKey = (i32, Struct);

/// A live DELETE entry: its [`DataFile`] (for the partition / spec / content / referenced-file fields and
/// to feed the removal set) and its post-inheritance data sequence number.
struct LiveDeleteEntry {
    data_file: DataFile,
    sequence_number: Option<i64>,
}

/// The live-entry view the dangling predicate runs over: the minimum live data sequence number per
/// `(spec_id, partition)` group, the set of live data-file paths (for DV reference matching), and the
/// live delete entries.
#[derive(Default)]
struct LiveEntries {
    /// `(spec_id, partition) -> min(data_sequence_number over live data files)` (Java `groupBy(...).agg
    /// (min(sequence_number))`). A group ABSENT from this map means "no live data file in that
    /// partition+spec" — the `min IS NULL` dangling case.
    min_data_seq_by_group: HashMap<GroupKey, Option<i64>>,
    /// Every live data file's path (Java's `DATA_FILES.file_path` column), for the DV reference check.
    live_data_file_paths: HashSet<String>,
    /// The live delete entries (position / equality / DV).
    live_delete_entries: Vec<LiveDeleteEntry>,
}

/// Take the minimum of two `Option<i64>` sequence numbers, treating `None` as "unknown / not yet set":
/// `min(None, x) = x`. Both are populated for real on-disk entries (post-inheritance), so this only
/// matters for the first insert.
fn min_option(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    match (left, right) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) => Some(a),
        (None, b) => b,
    }
}

/// Compute the dangling delete files over a [`LiveEntries`] view — the pure core of the action (Java
/// `findDanglingDeletes` + `findDanglingDvs`). Returns the [`DataFile`]s of every delete entry that can no
/// longer apply to any live data file. Pure (no IO) so the dangling predicate — the off-by-one corruption
/// edge — is unit-testable directly.
fn find_dangling_deletes(live: &LiveEntries) -> Vec<DataFile> {
    let mut dangling = Vec::new();

    for entry in &live.live_delete_entries {
        let data_file = &entry.data_file;

        // A DELETION VECTOR (PUFFIN position delete) dangles when its referenced data file is not a live
        // data-file path (Java `findDanglingDvs`). This is checked FIRST and independently of the
        // sequence-number rule: a DV is file-scoped, so the per-partition min-seq comparison does not
        // apply to it. A DV with no `referenced_data_file` is malformed; treat it as dangling-by-absence
        // (its reference can never match a live path), matching the leftouter-join-then-null semantics.
        if is_deletion_vector(data_file) {
            let referenced_live = data_file
                .referenced_data_file()
                .is_some_and(|path| live.live_data_file_paths.contains(&path));
            if !referenced_live {
                dangling.push(data_file.clone());
            }
            continue;
        }

        // Parquet position / equality delete: compare against its partition+spec group's minimum live
        // data sequence number.
        let key = (data_file.partition_spec_id, data_file.partition().clone());
        match live.min_data_seq_by_group.get(&key) {
            // `min_data_sequence_number IS NULL` — no live data file in this partition+spec. The delete
            // applies to nothing, so it dangles regardless of content type or sequence number.
            None | Some(None) => dangling.push(data_file.clone()),
            Some(Some(min_data_seq)) => {
                let delete_seq = entry.sequence_number;
                let is_dangling = match data_file.content_type() {
                    // Position delete: dangling when `sequence_number < min_data_sequence_number` (STRICT
                    // `<`). A position delete at the EXACT minimum still applies (read-path `delete_seq >=
                    // data_seq`), so it is NOT dangling.
                    DataContentType::PositionDeletes => {
                        delete_seq.is_some_and(|seq| seq < *min_data_seq)
                    }
                    // Equality delete: dangling when `sequence_number <= min_data_sequence_number`
                    // (NON-strict `<=`). An equality delete applies STRICTLY to lower-seq data
                    // (`delete_seq > data_seq`), so one at the exact minimum does NOT apply — it dangles.
                    DataContentType::EqualityDeletes => {
                        delete_seq.is_some_and(|seq| seq <= *min_data_seq)
                    }
                    DataContentType::Data => false,
                };
                if is_dangling {
                    dangling.push(data_file.clone());
                }
            }
        }
    }

    dangling
}

/// Whether a delete file is a deletion vector (Java `ContentFileUtil.isDV`: format == PUFFIN). A PUFFIN
/// position delete is file-scoped (keyed by its `referenced_data_file`), so it follows the DV dangling
/// rule, not the per-partition min-seq rule.
fn is_deletion_vector(data_file: &DataFile) -> bool {
    data_file.content_type() == DataContentType::PositionDeletes
        && data_file.file_format() == DataFileFormat::Puffin
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use futures::TryStreamExt;
    use tempfile::TempDir;

    use super::*;
    use crate::io::LocalFsStorageFactory;
    use crate::memory::MemoryCatalogBuilder;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, FormatVersion, Literal,
        ManifestStatus, NestedField, Operation, PartitionSpec, PrimitiveType, Schema, Struct,
        Transform, Type,
    };
    use crate::transaction::{ApplyTransactionAction, Transaction};
    use crate::writer::base_writer::equality_delete_writer::{
        EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig,
    };
    use crate::writer::base_writer::position_delete_writer::{
        PositionDeleteFileWriterBuilder, PositionDeleteWriterConfig,
    };
    use crate::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use crate::writer::file_writer::{FileWriter, FileWriterBuilder, ParquetWriterBuilder};
    use crate::writer::{IcebergWriter, IcebergWriterBuilder};
    use crate::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};

    // =========================================================================================
    // Pure-fn tests (the dangling predicate — the off-by-one corruption edge, IO-free)
    // =========================================================================================

    /// Build a synthetic delete [`DataFile`] of the given content / format in partition `x = part`.
    fn delete_file(
        path: &str,
        content: DataContentType,
        format: DataFileFormat,
        part: i64,
        spec_id: i32,
    ) -> DataFile {
        let mut builder = DataFileBuilder::default();
        builder
            .content(content)
            .file_path(path.to_string())
            .file_format(format)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(spec_id)
            .partition(Struct::from_iter([Some(Literal::long(part))]));
        if content == DataContentType::EqualityDeletes {
            builder.equality_ids(Some(vec![2]));
        }
        if format == DataFileFormat::Puffin {
            builder
                .content_offset(Some(4))
                .content_size_in_bytes(Some(40));
        }
        builder.build().unwrap()
    }

    /// Build a `LiveEntries` view with the given per-group data minimums and live data paths.
    fn live_entries(
        data_min_by_group: &[((i32, i64), Option<i64>)],
        data_paths: &[&str],
        deletes: Vec<LiveDeleteEntry>,
    ) -> LiveEntries {
        let mut live = LiveEntries::default();
        for ((spec_id, part), min) in data_min_by_group {
            live.min_data_seq_by_group.insert(
                (*spec_id, Struct::from_iter([Some(Literal::long(*part))])),
                *min,
            );
        }
        for path in data_paths {
            live.live_data_file_paths.insert((*path).to_string());
        }
        live.live_delete_entries = deletes;
        live
    }

    /// THE OFF-BY-ONE PIN (pos `<` vs eq `<=`). With a partition minimum data seq of 5:
    /// - a position delete at seq 5 is NOT dangling (5 < 5 is false — same-seq pos applies);
    /// - a position delete at seq 4 IS dangling (4 < 5);
    /// - an equality delete at seq 5 IS dangling (5 <= 5 — same-seq eq does NOT apply);
    /// - an equality delete at seq 6 is NOT dangling (6 <= 5 is false — it still applies).
    ///
    /// MUTATION: flip the position `<` to `<=` ⇒ the same-seq position delete is wrongly removed
    /// (resurrection); flip the equality `<=` to `<` ⇒ the same-seq equality delete is wrongly kept
    /// (dead-code dangling test). Both directions fail this test.
    #[test]
    fn test_dangling_predicate_off_by_one_between_position_and_equality() {
        let deletes = vec![
            LiveDeleteEntry {
                data_file: delete_file(
                    "pos-at-min.parquet",
                    DataContentType::PositionDeletes,
                    DataFileFormat::Parquet,
                    0,
                    0,
                ),
                sequence_number: Some(5),
            },
            LiveDeleteEntry {
                data_file: delete_file(
                    "pos-below-min.parquet",
                    DataContentType::PositionDeletes,
                    DataFileFormat::Parquet,
                    0,
                    0,
                ),
                sequence_number: Some(4),
            },
            LiveDeleteEntry {
                data_file: delete_file(
                    "eq-at-min.parquet",
                    DataContentType::EqualityDeletes,
                    DataFileFormat::Parquet,
                    0,
                    0,
                ),
                sequence_number: Some(5),
            },
            LiveDeleteEntry {
                data_file: delete_file(
                    "eq-above-min.parquet",
                    DataContentType::EqualityDeletes,
                    DataFileFormat::Parquet,
                    0,
                    0,
                ),
                sequence_number: Some(6),
            },
        ];
        let live = live_entries(&[((0, 0), Some(5))], &["data.parquet"], deletes);

        let dangling: HashSet<String> = find_dangling_deletes(&live)
            .into_iter()
            .map(|file| file.file_path().to_string())
            .collect();

        assert_eq!(
            dangling,
            HashSet::from([
                "pos-below-min.parquet".to_string(),
                "eq-at-min.parquet".to_string(),
            ]),
            "pos dangles strictly below min (NOT at min); eq dangles at-or-below min"
        );
    }

    /// A delete in a partition+spec with NO live data file dangles regardless of content type or seq
    /// (Java `min_data_sequence_number IS NULL`). Pins the join-miss branch.
    #[test]
    fn test_dangling_when_no_live_data_in_partition() {
        let deletes = vec![
            LiveDeleteEntry {
                data_file: delete_file(
                    "pos-orphan.parquet",
                    DataContentType::PositionDeletes,
                    DataFileFormat::Parquet,
                    9,
                    0,
                ),
                sequence_number: Some(100),
            },
            LiveDeleteEntry {
                data_file: delete_file(
                    "eq-orphan.parquet",
                    DataContentType::EqualityDeletes,
                    DataFileFormat::Parquet,
                    9,
                    0,
                ),
                sequence_number: Some(100),
            },
        ];
        // The data minimum exists for partition 0 ONLY; the deletes are in partition 9 (no live data).
        let live = live_entries(&[((0, 0), Some(5))], &["data.parquet"], deletes);

        let dangling: HashSet<String> = find_dangling_deletes(&live)
            .into_iter()
            .map(|file| file.file_path().to_string())
            .collect();
        assert_eq!(
            dangling,
            HashSet::from([
                "pos-orphan.parquet".to_string(),
                "eq-orphan.parquet".to_string(),
            ]),
            "a delete with no live data file in its partition+spec always dangles"
        );
    }

    /// CROSS-SPEC ISOLATION (pure-fn). The SAME partition tuple under a DIFFERENT spec id does not share
    /// a minimum: a delete under spec 1 partition 0 is compared only against spec-1 data, not spec-0
    /// data. With live data only under spec 0, a spec-1 delete dangles (no spec-1 data ⇒ min IS NULL).
    #[test]
    fn test_cross_spec_grouping_does_not_share_minimum() {
        let deletes = vec![LiveDeleteEntry {
            data_file: delete_file(
                "spec1-pos.parquet",
                DataContentType::PositionDeletes,
                DataFileFormat::Parquet,
                0,
                1,
            ),
            sequence_number: Some(100),
        }];
        // Live data minimum exists for (spec 0, part 0) only; the delete is (spec 1, part 0).
        let live = live_entries(&[((0, 0), Some(5))], &["data.parquet"], deletes);

        let dangling: Vec<String> = find_dangling_deletes(&live)
            .into_iter()
            .map(|file| file.file_path().to_string())
            .collect();
        assert_eq!(
            dangling,
            vec!["spec1-pos.parquet".to_string()],
            "a spec-1 delete must not borrow spec-0's data minimum"
        );
    }

    /// A DV whose referenced data file is not a live data-file path dangles; one whose reference IS live
    /// does not (Java `findDanglingDvs`). Pins the DV branch both directions.
    #[test]
    fn test_dv_dangles_when_referenced_data_file_gone() {
        let live_dv = {
            let mut file = delete_file(
                "live-dv.puffin",
                DataContentType::PositionDeletes,
                DataFileFormat::Puffin,
                0,
                0,
            );
            file.referenced_data_file = Some("live-data.parquet".to_string());
            file
        };
        let dangling_dv = {
            let mut file = delete_file(
                "dangling-dv.puffin",
                DataContentType::PositionDeletes,
                DataFileFormat::Puffin,
                0,
                0,
            );
            file.referenced_data_file = Some("gone-data.parquet".to_string());
            file
        };
        let deletes = vec![
            LiveDeleteEntry {
                data_file: live_dv,
                sequence_number: Some(2),
            },
            LiveDeleteEntry {
                data_file: dangling_dv,
                sequence_number: Some(2),
            },
        ];
        let live = live_entries(&[((0, 0), Some(1))], &["live-data.parquet"], deletes);

        let dangling: Vec<String> = find_dangling_deletes(&live)
            .into_iter()
            .map(|file| file.file_path().to_string())
            .collect();
        assert_eq!(
            dangling,
            vec!["dangling-dv.puffin".to_string()],
            "a DV referencing a gone data file dangles; one referencing a live file does not"
        );
    }

    // =========================================================================================
    // End-to-end tests — real parquet + real delete writers + real scans + real commits
    //
    // Harness mirrors maintenance/rewrite_data_files.rs: a local-fs memory catalog (REAL parquet
    // on disk) + a table partitioned by identity(x) with three long columns x/y/z.
    // =========================================================================================

    async fn local_fs_catalog() -> (impl Catalog, TempDir) {
        let temp_dir = TempDir::new().expect("temp dir");
        let warehouse = temp_dir
            .path()
            .to_str()
            .expect("utf8 temp path")
            .to_string();
        let catalog = MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                std::collections::HashMap::from([("warehouse".to_string(), warehouse)]),
            )
            .await
            .expect("load local-fs memory catalog");
        (catalog, temp_dir)
    }

    fn three_long_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "x",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::required(
                    2,
                    "y",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::required(
                    3,
                    "z",
                    Type::Primitive(PrimitiveType::Long),
                )),
            ])
            .build()
            .expect("build schema")
    }

    async fn create_partitioned_table(
        catalog: &impl Catalog,
        format_version: FormatVersion,
    ) -> Table {
        let schema = three_long_schema();
        let spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .add_partition_field("x", "x", Transform::Identity)
            .expect("add partition field")
            .build()
            .expect("build spec");
        let namespace = NamespaceIdent::new(format!("ns-{}", uuid::Uuid::new_v4()));
        catalog
            .create_namespace(&namespace, std::collections::HashMap::new())
            .await
            .expect("create namespace");
        let table_ident = TableIdent::new(namespace.clone(), "t".to_string());
        let creation = TableCreation::builder()
            .name(table_ident.name().to_string())
            .schema(schema)
            .partition_spec(spec)
            .format_version(format_version)
            .build();
        catalog
            .create_table(&namespace, creation)
            .await
            .expect("create table")
    }

    async fn write_data_file(
        table: &Table,
        file_name: &str,
        part_value: i64,
        rows: &[(i64, i64, i64)],
    ) -> DataFile {
        use crate::arrow::schema_to_arrow_schema;

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

        let file_path = format!("{}/data/{}", table.metadata().location(), file_name);
        let output = table.file_io().new_output(file_path).unwrap();
        let parquet_builder = ParquetWriterBuilder::new(
            parquet::file::properties::WriterProperties::builder().build(),
            schema.clone(),
        );
        let mut writer = parquet_builder.build(output).await.unwrap();
        writer.write(&batch).await.unwrap();
        let data_file_builders = writer.close().await.unwrap();

        let mut builder = data_file_builders.into_iter().next().unwrap();
        builder
            .content(DataContentType::Data)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(part_value))]))
            .build()
            .unwrap()
    }

    async fn write_equality_delete_file(
        table: &Table,
        part_value: i64,
        delete_ys: &[i64],
    ) -> DataFile {
        use crate::arrow::{arrow_schema_to_schema, schema_to_arrow_schema};

        let schema = table.metadata().current_schema().clone();
        let config = EqualityDeleteWriterConfig::new(vec![2], schema.clone()).unwrap();
        let delete_schema =
            Arc::new(arrow_schema_to_schema(config.projected_arrow_schema_ref()).unwrap());

        let location_gen = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
        let file_name_gen = DefaultFileNameGenerator::new(
            "eq-del".to_string(),
            Some(uuid::Uuid::now_v7().to_string()),
            DataFileFormat::Parquet,
        );
        let parquet_builder = ParquetWriterBuilder::new(
            parquet::file::properties::WriterProperties::builder().build(),
            delete_schema,
        );
        let rolling = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_builder,
            table.file_io().clone(),
            location_gen,
            file_name_gen,
        );

        let partition_key = crate::spec::PartitionKey::new(
            table.metadata().default_partition_spec().as_ref().clone(),
            schema.clone(),
            Struct::from_iter([Some(Literal::long(part_value))]),
        );
        let mut writer = EqualityDeleteFileWriterBuilder::new(rolling, config)
            .build(Some(partition_key))
            .await
            .unwrap();

        let arrow_schema = Arc::new(schema_to_arrow_schema(&schema).unwrap());
        let xs: Vec<i64> = delete_ys.iter().map(|_| part_value).collect();
        let ys: Vec<i64> = delete_ys.to_vec();
        let zs: Vec<i64> = delete_ys.iter().map(|_| 0).collect();
        let batch = RecordBatch::try_new(arrow_schema, vec![
            Arc::new(Int64Array::from(xs)) as ArrayRef,
            Arc::new(Int64Array::from(ys)) as ArrayRef,
            Arc::new(Int64Array::from(zs)) as ArrayRef,
        ])
        .unwrap();
        writer.write(batch).await.unwrap();
        writer.close().await.unwrap().into_iter().next().unwrap()
    }

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
        let partition_key = crate::spec::PartitionKey::new(
            table.metadata().default_partition_spec().as_ref().clone(),
            table.metadata().current_schema().clone(),
            Struct::from_iter([Some(Literal::long(part_value))]),
        );
        let mut writer = PositionDeleteFileWriterBuilder::new(rolling, config.clone())
            .build(Some(partition_key))
            .await
            .unwrap();

        let paths: Vec<&str> = deletes.iter().map(|(path, _)| path.as_str()).collect();
        let positions: Vec<i64> = deletes.iter().map(|(_, pos)| *pos).collect();
        let batch = RecordBatch::try_new(config.arrow_schema().clone(), vec![
            Arc::new(StringArray::from(paths)) as ArrayRef,
            Arc::new(Int64Array::from(positions)) as ArrayRef,
        ])
        .unwrap();
        writer.write(batch).await.unwrap();
        writer.close().await.unwrap().into_iter().next().unwrap()
    }

    async fn append_files(catalog: &impl Catalog, table: &Table, files: Vec<DataFile>) -> Table {
        let tx = Transaction::new(table);
        let action = tx.fast_append().add_data_files(files);
        let tx = action.apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    async fn add_deletes(catalog: &impl Catalog, table: &Table, deletes: Vec<DataFile>) -> Table {
        let tx = Transaction::new(table);
        let action = tx.row_delta().add_deletes(deletes);
        let tx = action.apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    /// Write a REAL Puffin deletion vector referencing `data_file_path`, deleting the given positions
    /// (mirrors `transaction::row_delta::tests::write_real_dv_file`). V3-only (DVs require V3).
    async fn write_real_dv_file(
        table: &Table,
        file_name: &str,
        data_file_path: &str,
        part_value: i64,
        positions: &[u64],
    ) -> DataFile {
        use crate::spec::PartitionKey;
        use crate::writer::base_writer::deletion_vector_writer::DVFileWriter;

        let partition_key = PartitionKey::new(
            table.metadata().default_partition_spec().as_ref().clone(),
            table.metadata().current_schema().clone(),
            Struct::from_iter([Some(Literal::long(part_value))]),
        );
        let dv_path = format!("{}/data/{}", table.metadata().location(), file_name);
        let output_file = table.file_io().new_output(&dv_path).unwrap();
        let mut dv_writer = DVFileWriter::new(output_file);
        for pos in positions {
            dv_writer
                .delete(data_file_path, *pos, Some(&partition_key))
                .unwrap();
        }
        dv_writer.close().await.unwrap().into_iter().next().unwrap()
    }

    /// Scan the table and collect the `y` column values (merge-on-read deletes applied) — the real
    /// read-side signal.
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
            for index in 0..col.len() {
                values.insert(col.value(index));
            }
        }
        values
    }

    /// The set of live delete-file paths in the current snapshot.
    async fn live_delete_paths(table: &Table) -> HashSet<String> {
        use crate::spec::ManifestContentType;
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let mut paths = HashSet::new();
        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Deletes {
                continue;
            }
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                if entry.is_alive() {
                    paths.insert(entry.file_path().to_string());
                }
            }
        }
        paths
    }

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

    /// THE CROWN JEWEL (the resurrection door). A STILL-APPLICABLE equality delete (a live data file
    /// with `data_seq < delete_seq`) must NOT be removed, and the scan must stay correct after the
    /// action. Append X (seq 1, rows y=10/20/30 in partition 0), then an equality delete at seq 2
    /// removing y=20. The delete applies (`1 < 2`), so it is NOT dangling. The action removes nothing,
    /// and the scan still drops y=20.
    ///
    /// MUTATION: flip the equality `<=` to `<` in `find_dangling_deletes` ⇒ STILL leaves this delete
    /// kept (1 < 2 either way); flip BOTH the comparison direction (`seq <= min` to `seq >= min` /
    /// removing the strictness) is the real resurrection lever — captured by the off-by-one pure-fn
    /// test. This e2e pins that the applicable delete survives a real action run + scan.
    #[tokio::test]
    async fn test_crown_jewel_still_applicable_equality_delete_not_removed() {
        let (catalog, _temp) = local_fs_catalog().await;
        let table = create_partitioned_table(&catalog, FormatVersion::V2).await;

        let x = write_data_file(&table, "x.parquet", 0, &[
            (0, 10, 100),
            (0, 20, 200),
            (0, 30, 300),
        ])
        .await;
        let table = append_files(&catalog, &table, vec![x]).await;

        let eq_delete = write_equality_delete_file(&table, 0, &[20]).await;
        let eq_path = eq_delete.file_path().to_string();
        let table = add_deletes(&catalog, &table, vec![eq_delete]).await;
        assert_eq!(
            scan_y_values(&table).await,
            HashSet::from([10, 30]),
            "before the action the equality delete drops y=20"
        );

        let result = RemoveDanglingDeleteFiles::new(table.clone())
            .execute(&catalog)
            .await
            .unwrap();
        assert!(
            result.removed_delete_files.is_empty(),
            "a still-applicable equality delete must NOT be removed: {:?}",
            result.removed_delete_files
        );

        // No snapshot was committed (empty plan) — the table head is unchanged, the delete still live.
        let reloaded = catalog.load_table(table.identifier()).await.unwrap();
        assert!(
            live_delete_paths(&reloaded).await.contains(&eq_path),
            "the applicable equality delete must still be live"
        );
        assert_eq!(
            scan_y_values(&reloaded).await,
            HashSet::from([10, 30]),
            "the scan must still drop y=20 (no resurrection)"
        );
    }

    /// POSITION-DELETE EXACT-SEQ BOUNDARY (both directions, e2e). A position delete at the SAME data
    /// sequence number as the only live data file is NOT dangling (same-seq pos applies). Append X
    /// (seq 1), then a position delete at seq 2 deleting (X, pos 1). Compact X→X' PRESERVING seq 1
    /// (so X' is seq 1). The position delete (seq 2) references the OLD X path which is now gone, but
    /// it is partition-scoped (parquet), so it is compared against the partition minimum (1): seq 2 is
    /// NOT < 1, so it is NOT dangling — and it no longer matches any live row (X is gone). The action
    /// keeps it. This pins that a position delete at/above the partition min survives.
    #[tokio::test]
    async fn test_position_delete_at_or_above_partition_min_is_not_dangling() {
        let (catalog, _temp) = local_fs_catalog().await;
        let table = create_partitioned_table(&catalog, FormatVersion::V2).await;

        let x = write_data_file(&table, "x.parquet", 0, &[(0, 10, 100), (0, 20, 200)]).await;
        let x_path = x.file_path().to_string();
        let table = append_files(&catalog, &table, vec![x]).await;
        let x_seq = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .sequence_number();

        let pos_delete = write_position_delete_file(&table, 0, &[(x_path, 1)]).await;
        let pos_path = pos_delete.file_path().to_string();
        let table = add_deletes(&catalog, &table, vec![pos_delete]).await;
        let delete_seq = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .sequence_number();
        assert!(x_seq < delete_seq, "delete is at a higher seq than X");

        // A second data file Y in the SAME partition at a HIGHER seq keeps the partition populated.
        let y = write_data_file(&table, "y.parquet", 0, &[(0, 40, 400)]).await;
        let table = append_files(&catalog, &table, vec![y]).await;

        // The partition-0 minimum data seq is still x_seq (X is live). The position delete (delete_seq)
        // is NOT < x_seq, so it is NOT dangling.
        let result = RemoveDanglingDeleteFiles::new(table.clone())
            .execute(&catalog)
            .await
            .unwrap();
        assert!(
            result.removed_delete_files.is_empty(),
            "a position delete at/above the partition minimum must not be removed: {:?}",
            result.removed_delete_files
        );
        let reloaded = catalog.load_table(table.identifier()).await.unwrap();
        assert!(
            live_delete_paths(&reloaded).await.contains(&pos_path),
            "the position delete must still be live"
        );
    }

    /// A GENUINELY DANGLING equality delete (all referencing-era data rewritten to a HIGHER seq) IS
    /// removed; the post-action scan is unchanged; the summary counter is correct. Append X (seq 1,
    /// rows y=10/20/30), equality-delete y=20 (seq 2). Then compact X→X' with a FRESH (higher) seq via
    /// a normal RewriteFiles (no seq preservation): the partition minimum jumps to X''s seq (3), so the
    /// equality delete (seq 2) now dangles (`2 <= 3`). The action removes it. The scan is unchanged
    /// because the fresh-seq rewrite already stopped the delete from applying (y=20 was already back).
    #[tokio::test]
    async fn test_genuinely_dangling_equality_delete_is_removed_with_counter() {
        let (catalog, _temp) = local_fs_catalog().await;
        let table = create_partitioned_table(&catalog, FormatVersion::V2).await;

        let x = write_data_file(&table, "x.parquet", 0, &[
            (0, 10, 100),
            (0, 20, 200),
            (0, 30, 300),
        ])
        .await;
        let table = append_files(&catalog, &table, vec![x.clone()]).await;

        let eq_delete = write_equality_delete_file(&table, 0, &[20]).await;
        let eq_path = eq_delete.file_path().to_string();
        let table = add_deletes(&catalog, &table, vec![eq_delete]).await;

        // Compact X→X' with a FRESH higher seq (no preservation) — the equality delete stops applying.
        let x_prime = write_data_file(&table, "x-prime.parquet", 0, &[
            (0, 10, 100),
            (0, 20, 200),
            (0, 30, 300),
        ])
        .await;
        let tx = Transaction::new(&table);
        let action = tx.rewrite_files(vec![x], vec![x_prime]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        assert_eq!(
            scan_y_values(&table).await,
            HashSet::from([10, 20, 30]),
            "the fresh-seq rewrite already let y=20 back (the delete dangles)"
        );

        let result = RemoveDanglingDeleteFiles::new(table.clone())
            .execute(&catalog)
            .await
            .unwrap();
        assert_eq!(
            result.removed_delete_files.len(),
            1,
            "the dangling equality delete must be removed"
        );
        assert_eq!(result.removed_equality_delete_files_count(), 1);

        let reloaded = catalog.load_table(table.identifier()).await.unwrap();
        assert!(
            !live_delete_paths(&reloaded).await.contains(&eq_path),
            "the dangling equality delete must be tombstoned"
        );
        assert_eq!(
            reloaded
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Replace,
            "the removal commits a Replace snapshot (Java BaseRewriteFiles.operation)"
        );
        assert_eq!(
            summary_prop(&reloaded, "removed-equality-delete-files").as_deref(),
            Some("1"),
            "the summary must report one removed equality delete"
        );
        // The scan is unchanged by the GC (the delete was already not applying).
        assert_eq!(
            scan_y_values(&reloaded).await,
            HashSet::from([10, 20, 30]),
            "the GC does not change the read result"
        );
    }

    /// A DANGLING POSITION-DELETE PARQUET (its referenced data file rewritten away to a higher seq) IS
    /// removed. This converges with the 1.10.0 carry-posture trace: a plain RewriteFiles KEEPS the
    /// now-dangling parquet position delete (carry-unchanged posture); THIS action is the cleaner.
    /// Append X (seq 1), position-delete (X, 1) at seq 2, then compact X→X' FRESH-seq (X' seq 3): the
    /// partition minimum jumps to 3, so the position delete (seq 2) dangles (`2 < 3`). Removed.
    #[tokio::test]
    async fn test_dangling_position_delete_parquet_removed_after_data_rewritten_away() {
        let (catalog, _temp) = local_fs_catalog().await;
        let table = create_partitioned_table(&catalog, FormatVersion::V2).await;

        let x = write_data_file(&table, "x.parquet", 0, &[
            (0, 10, 100),
            (0, 20, 200),
            (0, 30, 300),
        ])
        .await;
        let x_path = x.file_path().to_string();
        let table = append_files(&catalog, &table, vec![x.clone()]).await;

        let pos_delete = write_position_delete_file(&table, 0, &[(x_path, 1)]).await;
        let pos_path = pos_delete.file_path().to_string();
        let table = add_deletes(&catalog, &table, vec![pos_delete]).await;
        assert_eq!(
            scan_y_values(&table).await,
            HashSet::from([10, 30]),
            "the position delete drops y=20"
        );

        // Compact X→X' FRESH seq — the position delete now references a gone file and the partition min
        // jumps above the delete's seq, so it dangles.
        let x_prime = write_data_file(&table, "x-prime.parquet", 0, &[
            (0, 10, 100),
            (0, 20, 200),
            (0, 30, 300),
        ])
        .await;
        let tx = Transaction::new(&table);
        let action = tx.rewrite_files(vec![x], vec![x_prime]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // A plain RewriteFiles KEEPS the dangling position delete (carry-unchanged posture).
        assert!(
            live_delete_paths(&table).await.contains(&pos_path),
            "plain RewriteFiles keeps the now-dangling position delete (carry posture)"
        );

        let result = RemoveDanglingDeleteFiles::new(table.clone())
            .execute(&catalog)
            .await
            .unwrap();
        assert_eq!(result.removed_delete_files.len(), 1);
        assert_eq!(result.removed_position_delete_files_count(), 1);

        let reloaded = catalog.load_table(table.identifier()).await.unwrap();
        assert!(
            !live_delete_paths(&reloaded).await.contains(&pos_path),
            "the dangling position delete must be removed by this action"
        );
        assert_eq!(
            summary_prop(&reloaded, "removed-position-delete-files").as_deref(),
            Some("1"),
            "the summary must report one removed position delete"
        );
        // The scan is unchanged (y=20 was already back after the fresh-seq rewrite).
        assert_eq!(
            scan_y_values(&reloaded).await,
            HashSet::from([10, 20, 30]),
            "the GC does not change the read result"
        );
    }

    /// PARTITION ISOLATION (e2e). A delete dangling in partition A but applicable in partition B: the
    /// applicable one is KEPT, the dangling one is removed. Partition 0 has X (seq 1) + an applicable
    /// eq delete (seq 2, applies `1 < 2`). Partition 1's data is rewritten to a fresh higher seq, so its
    /// eq delete dangles. The action removes ONLY the partition-1 delete.
    #[tokio::test]
    async fn test_partition_isolation_dangling_in_one_applicable_in_other() {
        let (catalog, _temp) = local_fs_catalog().await;
        let table = create_partitioned_table(&catalog, FormatVersion::V2).await;

        // Partition 0: X0 (seq 1) + applicable equality delete (seq 2).
        let x0 = write_data_file(&table, "x0.parquet", 0, &[(0, 10, 100), (0, 20, 200)]).await;
        // Partition 1: X1 (seq 1).
        let x1 = write_data_file(&table, "x1.parquet", 1, &[(1, 60, 600), (1, 70, 700)]).await;
        let table = append_files(&catalog, &table, vec![x0, x1.clone()]).await;

        let eq0 = write_equality_delete_file(&table, 0, &[20]).await;
        let eq0_path = eq0.file_path().to_string();
        let eq1 = write_equality_delete_file(&table, 1, &[70]).await;
        let eq1_path = eq1.file_path().to_string();
        let table = add_deletes(&catalog, &table, vec![eq0, eq1]).await;

        // Rewrite partition 1's data X1→X1' with a FRESH higher seq, so eq1 (partition 1) dangles while
        // eq0 (partition 0, applies to the still-seq-1 X0) stays applicable.
        let x1_prime =
            write_data_file(&table, "x1-prime.parquet", 1, &[(1, 60, 600), (1, 70, 700)]).await;
        let tx = Transaction::new(&table);
        let action = tx.rewrite_files(vec![x1], vec![x1_prime]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let result = RemoveDanglingDeleteFiles::new(table.clone())
            .execute(&catalog)
            .await
            .unwrap();
        let removed: HashSet<String> = result
            .removed_delete_files
            .iter()
            .map(|file| file.file_path().to_string())
            .collect();
        assert_eq!(
            removed,
            HashSet::from([eq1_path.clone()]),
            "only the partition-1 dangling delete is removed; the partition-0 delete is kept"
        );

        let reloaded = catalog.load_table(table.identifier()).await.unwrap();
        let live = live_delete_paths(&reloaded).await;
        assert!(
            live.contains(&eq0_path),
            "the applicable partition-0 delete stays live"
        );
        assert!(
            !live.contains(&eq1_path),
            "the dangling partition-1 delete is gone"
        );
        // Partition 0's scan still drops y=20 (its delete still applies — no resurrection).
        assert!(
            !scan_y_values(&reloaded).await.contains(&20),
            "the applicable partition-0 delete must still drop y=20"
        );
    }

    /// EMPTY PLAN NO-OP (no commit). A table with deletes that all still apply commits NOTHING — the
    /// snapshot id is unchanged. Pins that the action does not mint an empty Replace snapshot.
    #[tokio::test]
    async fn test_empty_plan_is_a_no_op_no_commit() {
        let (catalog, _temp) = local_fs_catalog().await;
        let table = create_partitioned_table(&catalog, FormatVersion::V2).await;

        let x = write_data_file(&table, "x.parquet", 0, &[(0, 10, 100), (0, 20, 200)]).await;
        let table = append_files(&catalog, &table, vec![x]).await;
        let eq_delete = write_equality_delete_file(&table, 0, &[20]).await;
        let table = add_deletes(&catalog, &table, vec![eq_delete]).await;
        let head_before = table.metadata().current_snapshot().unwrap().snapshot_id();

        let result = RemoveDanglingDeleteFiles::new(table.clone())
            .execute(&catalog)
            .await
            .unwrap();
        assert!(result.removed_delete_files.is_empty());

        let reloaded = catalog.load_table(table.identifier()).await.unwrap();
        assert_eq!(
            reloaded
                .metadata()
                .current_snapshot()
                .unwrap()
                .snapshot_id(),
            head_before,
            "no snapshot must be committed for an empty plan"
        );
    }

    /// A table with NO current snapshot is a no-op (defensive — nothing to scan).
    #[tokio::test]
    async fn test_no_current_snapshot_is_a_no_op() {
        let (catalog, _temp) = local_fs_catalog().await;
        let table = create_partitioned_table(&catalog, FormatVersion::V2).await;

        let result = RemoveDanglingDeleteFiles::new(table)
            .execute(&catalog)
            .await
            .unwrap();
        assert!(result.removed_delete_files.is_empty());
    }

    /// PRODUCER-ROUTING MUTATION PIN: the removed delete file must actually be TOMBSTONED in the
    /// rewritten DELETE manifest (not just reported in the result). After removing a dangling equality
    /// delete, the rewritten DELETE manifest must carry it as a Deleted tombstone — the read path stops
    /// applying it. Mutation (sever the RewriteFiles producer routing — make `with_removed_delete_files`
    /// inert) ⇒ this test fails (the delete stays live).
    #[tokio::test]
    async fn test_removed_delete_is_tombstoned_in_rewritten_manifest() {
        use crate::spec::ManifestContentType;
        let (catalog, _temp) = local_fs_catalog().await;
        let table = create_partitioned_table(&catalog, FormatVersion::V2).await;

        let x = write_data_file(&table, "x.parquet", 0, &[(0, 10, 100), (0, 20, 200)]).await;
        let table = append_files(&catalog, &table, vec![x.clone()]).await;
        let eq_delete = write_equality_delete_file(&table, 0, &[20]).await;
        let eq_path = eq_delete.file_path().to_string();
        let table = add_deletes(&catalog, &table, vec![eq_delete]).await;

        // Rewrite X away to a fresh seq so the equality delete dangles.
        let x_prime =
            write_data_file(&table, "x-prime.parquet", 0, &[(0, 10, 100), (0, 20, 200)]).await;
        let tx = Transaction::new(&table);
        let action = tx.rewrite_files(vec![x], vec![x_prime]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        RemoveDanglingDeleteFiles::new(table.clone())
            .execute(&catalog)
            .await
            .unwrap();

        // The removed delete must appear as a DELETED tombstone in a DELETE manifest of the new snapshot.
        let reloaded = catalog.load_table(table.identifier()).await.unwrap();
        let snapshot = reloaded.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(reloaded.file_io(), reloaded.metadata())
            .await
            .unwrap();
        let mut found_tombstone = false;
        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Deletes {
                continue;
            }
            let manifest = manifest_file
                .load_manifest(reloaded.file_io())
                .await
                .unwrap();
            for entry in manifest.entries() {
                if entry.file_path() == eq_path && entry.status() == ManifestStatus::Deleted {
                    found_tombstone = true;
                }
            }
        }
        assert!(
            found_tombstone,
            "the dangling equality delete must be a Deleted tombstone (producer routing fired)"
        );
    }

    /// DELETION-VECTOR E2E (the builder's flagged gap, added by the reviewer). A real Puffin DV
    /// referencing data file A is carried forward (Rust carry-posture) when A is rewritten to A' via a
    /// plain RewriteFiles; the DV now references a GONE data file, so it dangles. This action removes it.
    /// The scan is correct before AND after — A' carries no DV, so the masked row is already back after
    /// the rewrite, and removing the dangling DV does not change the read result.
    ///
    /// Posture surfaced (module doc): 1.10.0 AUTO-prunes dangling DVs at RewriteFiles time
    /// (`isDanglingDV` gated on PUFFIN), so Java rarely needs the action for DVs; Rust's RewriteFiles
    /// carries the dangling DV forward and RELIES on this action to clean it. Pinned `removed-dvs: 1`.
    ///
    /// Risk pinned: the DV branch never firing e2e (the builder pinned it pure-fn only); a real DV whose
    /// referenced file is gone must be removed AND the read result must be unchanged (no resurrection,
    /// no over-clean).
    #[tokio::test]
    async fn test_dangling_deletion_vector_removed_after_referenced_data_rewritten_away() {
        use crate::spec::ManifestContentType;
        let (catalog, _temp) = local_fs_catalog().await;
        // V3 — deletion vectors require format version 3.
        let table = create_partitioned_table(&catalog, FormatVersion::V3).await;

        // Data file A in partition 0: rows y=10/20/30.
        let a = write_data_file(&table, "a.parquet", 0, &[
            (0, 10, 100),
            (0, 20, 200),
            (0, 30, 300),
        ])
        .await;
        let a_path = a.file_path().to_string();
        let table = append_files(&catalog, &table, vec![a.clone()]).await;

        // A real Puffin DV referencing A, deleting position 1 (y=20). Committed via row_delta.
        let dv = write_real_dv_file(&table, "a-dv.puffin", &a_path, 0, &[1]).await;
        let dv_path = dv.file_path().to_string();
        assert!(
            is_deletion_vector(&dv),
            "fixture sanity: the written file is a PUFFIN deletion vector"
        );
        let table = add_deletes(&catalog, &table, vec![dv]).await;
        assert_eq!(
            scan_y_values(&table).await,
            HashSet::from([10, 30]),
            "the DV drops y=20 before the rewrite"
        );

        // Plain RewriteFiles A->A' (fresh seq). A' has a NEW path, so the DV (keyed by A's path) no
        // longer applies; y=20 comes back. The carry-posture KEEPS the now-dangling DV.
        let a_prime = write_data_file(&table, "a-prime.parquet", 0, &[
            (0, 10, 100),
            (0, 20, 200),
            (0, 30, 300),
        ])
        .await;
        let tx = Transaction::new(&table);
        let action = tx.rewrite_files(vec![a], vec![a_prime]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        assert!(
            live_delete_paths(&table).await.contains(&dv_path),
            "plain RewriteFiles carries the now-dangling DV forward (Rust carry-posture)"
        );
        assert_eq!(
            scan_y_values(&table).await,
            HashSet::from([10, 20, 30]),
            "after A->A' the DV references a gone file, so y=20 is already back"
        );

        // The action removes the dangling DV (referenced file A is gone).
        let result = RemoveDanglingDeleteFiles::new(table.clone())
            .execute(&catalog)
            .await
            .unwrap();
        assert_eq!(
            result.removed_delete_files.len(),
            1,
            "the dangling DV must be removed"
        );
        assert_eq!(result.removed_dvs_count(), 1, "it is counted as a DV");
        assert_eq!(
            result.removed_position_delete_files_count(),
            0,
            "a DV is not a parquet position delete"
        );

        let reloaded = catalog.load_table(table.identifier()).await.unwrap();
        assert!(
            !live_delete_paths(&reloaded).await.contains(&dv_path),
            "the dangling DV must be tombstoned"
        );
        assert_eq!(
            summary_prop(&reloaded, "removed-dvs").as_deref(),
            Some("1"),
            "the summary must report one removed DV"
        );
        // The removed DV must be a Deleted tombstone in a DELETE manifest of the new snapshot.
        let snapshot = reloaded.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(reloaded.file_io(), reloaded.metadata())
            .await
            .unwrap();
        let mut found_tombstone = false;
        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Deletes {
                continue;
            }
            let manifest = manifest_file
                .load_manifest(reloaded.file_io())
                .await
                .unwrap();
            for entry in manifest.entries() {
                if entry.file_path() == dv_path && entry.status() == ManifestStatus::Deleted {
                    found_tombstone = true;
                }
            }
        }
        assert!(
            found_tombstone,
            "the removed DV must be a Deleted tombstone"
        );

        // The read result is UNCHANGED by the GC (the DV was already not applying).
        assert_eq!(
            scan_y_values(&reloaded).await,
            HashSet::from([10, 20, 30]),
            "removing the dangling DV does not change the read result"
        );
    }
}
