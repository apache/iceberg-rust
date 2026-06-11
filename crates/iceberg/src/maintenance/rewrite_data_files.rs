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

//! `RewriteDataFiles` — the bin-pack compaction maintenance action. The Rust port of Java's
//! `org.apache.iceberg.actions.RewriteDataFiles` bin-pack strategy: plan small-file groups per
//! partition, read each group's LIVE rows (merge-on-read deletes applied), and rewrite them into
//! target-sized files committed through the existing seq-preserving
//! [`RewriteFilesAction`](crate::transaction::Transaction::rewrite_files).
//!
//! **THIS ACTION REWRITES DATA.** A compaction that loses rows, resurrects deleted rows (a
//! sequence-number mistake breaks outstanding equality/position-delete applicability), or commits
//! the wrong replaced-set is *silent data corruption*. Every planning and commit decision below is
//! pinned against Java 1.10.0 and tested for row conservation + delete-applicability preservation.
//!
//! # Java provenance and the 1.10.0 pin
//!
//! The planning algorithm is ported from `core/actions/{SizeBasedFileRewritePlanner,
//! BinPackRewriteFilePlanner}.java` (the size-based candidate predicate, per-partition grouping,
//! bin packing, and group filter) and `api/actions/RewriteDataFiles.java` (the options + result
//! shape). The commit semantics are ported from `core/actions/RewriteDataFilesCommitManager.java`.
//! Facts pinned against 1.10.0 BYTECODE (vs the readable MAIN source where flagged):
//!
//! - `RewriteDataFiles.USE_STARTING_SEQUENCE_NUMBER_DEFAULT = true` (api bytecode).
//! - `RewriteDataFilesCommitManager.commitFileGroups` (core bytecode, offsets 81-145):
//!   `table.newRewrite().validateFromSnapshot(startingSnapshotId)`; IF `useStartingSequenceNumber`:
//!   `.dataSequenceNumber(table.snapshot(startingSnapshotId).sequenceNumber())` — the STARTING
//!   snapshot's sequence number is stamped on every added file; then add added / remove rewritten
//!   data / remove rewritten delete files; `.commit()`. (See [the sequence-number rule](#the-sequence-number-rule).)
//! - `SizeBasedFileRewritePlanner` defaults (MAIN, the literal values bytecode-confirmed):
//!   `MIN_FILE_SIZE_DEFAULT_RATIO = 0.75`, `MAX_FILE_SIZE_DEFAULT_RATIO = 1.8`,
//!   `MIN_INPUT_FILES_DEFAULT = 5`, `MAX_FILE_GROUP_SIZE_BYTES_DEFAULT = 100 GiB`,
//!   `REWRITE_ALL_DEFAULT = false`.
//! - `BinPackRewriteFilePlanner` defaults (MAIN): `DELETE_FILE_THRESHOLD_DEFAULT = Integer.MAX_VALUE`
//!   (disabled by default), `DELETE_RATIO_THRESHOLD_DEFAULT = 0.3`.
//! - `defaultTargetFileSize` = `write.target-file-size-bytes` table property (default 512 MiB —
//!   [`TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT`](crate::spec::TableProperties)).
//!
//! # The algorithm (Java `BinPackRewriteFilePlanner.plan`)
//!
//! 1. **Enumerate tasks**: scan the current snapshot (`scan().filter(filter).plan_files()`), one
//!    [`FileScanTask`](crate::scan::FileScanTask) per live data file, each carrying its size
//!    (`file_size_in_bytes`), record count, partition, partition spec, and attached delete files
//!    (Java `table.newScan().filter(filter).ignoreResiduals().planFiles()`).
//! 2. **Group by partition** (`groupByPartition`): key each task by its file's partition tuple when
//!    the file's spec id equals the table's CURRENT default spec id, else the empty struct ("a task
//!    of an incompatible partition spec could contain values belonging to multiple current
//!    partitions, so they are grouped together as un-partitioned"). Java plans PER PARTITION.
//! 3. **Candidate filter** (`filterFiles`): a task is a candidate iff
//!    `outsideDesiredFileSizeRange` (`length < min_file_size || length > max_file_size`) OR
//!    `tooManyDeletes` (`deletes.len() >= delete_file_threshold`). (`tooHighDeleteRatio` is DEFERRED
//!    — see the deferral list; with `delete_ratio_threshold` not exposed it never fires.)
//! 4. **Bin pack** within each partition: pack the candidate tasks into bins of total size
//!    `<= max_file_group_size_bytes` via the forward greedy first-fit packer (Java
//!    `BinPacking.ListPacker(maxGroupSize, lookback=1, largestBinFirst=false, maxGroupCount).pack`).
//! 5. **Group filter** (`filterFileGroups`): keep a group iff `enoughInputFiles`
//!    (`size > 1 && size >= min_input_files`) OR `enoughContent` (`size > 1 && inputSize > target`)
//!    OR `tooMuchContent` (`inputSize > max_file_size`) OR `anyMatch(tooManyDeletes)`.
//! 6. **Per group**: read the group's files' LIVE rows (a scan restricted to the group's data-file
//!    paths, so merge-on-read deletes ARE applied — the rewritten files contain only live rows),
//!    write them into target-sized file(s) via the rolling data-file writer, and commit ONE
//!    [`RewriteFilesAction`](crate::transaction::Transaction::rewrite_files) per group that REPLACES
//!    exactly the group's data files with the new ones, stamping the starting sequence number.
//!
//! ## What the rewriter READS — deletes APPLIED
//!
//! The Spark runner (`SparkBinPackFileRewriteRunner.doRewrite`, MAIN — there is no core/api
//! bytecode for the runner) reads the group via the normal Iceberg scan (`format("iceberg")`),
//! which APPLIES merge-on-read deletes, and writes only the live rows. This is why
//! `DELETE_FILE_THRESHOLD` / `DELETE_RATIO_THRESHOLD` exist: a delete-laden file is rewritten to
//! physically DROP its deletes. This port mirrors that — each group is read through the table scan
//! restricted to the group's paths, so the output files carry only live rows.
//!
//! Position deletes / deletion vectors that REFERENCE a rewritten data file then DANGLE: the
//! rewritten file is gone, the new file carries the live rows, and the delete file is harmless
//! (Java keeps it — converges with the existing
//! [`RewriteFilesAction`](crate::transaction::rewrite_files) dangling-delete carry-unchanged
//! posture). Equality deletes still apply to the rewritten rows via the preserved sequence number.
//!
//! # The sequence-number rule
//!
//! `use_starting_sequence_number` defaults TRUE (Java). When true, every added file is stamped with
//! the **starting snapshot's** data sequence number (Java
//! `dataSequenceNumber(table.snapshot(startingSnapshotId).sequenceNumber())`). Outstanding
//! merge-on-read EQUALITY deletes apply only to data with a strictly LOWER data sequence number
//! (`data_seq < delete_seq`); stamping the rewritten files with the starting snapshot's seq keeps
//! the rewritten data at the SAME seq it had, so any equality delete added at a higher seq STILL
//! applies — no resurrection. With it FALSE, the added files take a fresh, higher seq via the
//! standard add path, and outstanding equality deletes stop applying (the Java-identical hazard —
//! `BaseRewriteFiles` has no guard). This threads to
//! [`RewriteFilesAction::data_sequence_number`](crate::transaction::rewrite_files::RewriteFilesAction::data_sequence_number).
//!
//! # Defaults (Java parity)
//!
//! - `target_file_size_bytes` = `write.target-file-size-bytes` (default 512 MiB).
//! - `min_file_size_bytes` = `0.75 * target` (resolved lazily when unset).
//! - `max_file_size_bytes` = `1.8 * target` (resolved lazily when unset).
//! - `min_input_files` = 5.
//! - `delete_file_threshold` = disabled (`usize::MAX`, Java `Integer.MAX_VALUE`).
//! - `max_file_group_size_bytes` = 100 GiB.
//! - `use_starting_sequence_number` = true.
//! - `filter` = always-true (no row filter).
//!
//! Java's `sizeThresholds` preconditions are mirrored at [`Self::execute`]: `target > 0`,
//! `target > min`, `target < max` (each a `DataInvalid` with Java's verbatim message).
//!
//! # Empty plan
//!
//! If nothing qualifies (no candidate files, or no group survives the group filter), the action is
//! a NO-OP: it returns a zero-count [`RewriteDataFilesResult`] and commits NOTHING (Java's empty
//! `rewriteResults` — `RewriteDataFilesCommitManager` is never asked to commit an empty group set;
//! there is no throw on an empty plan).
//!
//! # Deferred (loudly)
//!
//! - **Partial progress** (`PARTIAL_PROGRESS_ENABLED`, default false in Java): each group commits in
//!   its OWN `RewriteFiles` transaction sequentially; there is no max-commits batching / failure
//!   tolerance. A group commit failure aborts the action (returns `Err`).
//! - **Concurrency** (`MAX_CONCURRENT_FILE_GROUP_REWRITES`, `executeWith(ExecutorService)`): the
//!   sweep is SEQUENTIAL.
//! - **Sort / Z-order strategies** (`sort()` / `zOrder()`): Java throws
//!   `UnsupportedOperationException` outside Spark; only `binPack()` is ported.
//! - **`delete_ratio_threshold` / `tooHighDeleteRatio`**: the delete-RATIO candidate clause is not
//!   exposed (it needs per-file known-deleted-record accounting); only the delete-COUNT threshold
//!   (`delete_file_threshold`) is wired. The ratio clause never fires here.
//! - **`output_spec_id` / `rewrite_all` / `max_file_group_input_files` / `max_files_to_rewrite` /
//!   `rewrite_job_order`**: not exposed (advanced knobs); the output spec is always the table's
//!   current default spec, and groups commit in plan order.
//! - **Oversized-file SPLITTING**: Java's planner does NOT split an oversized input file — it
//!   bin-packs whole `FileScanTask`s and lets the WRITE-time rolling writer (`writeMaxFileSize` /
//!   `inputSplitSize`) control OUTPUT rolling. This port likewise bin-packs whole files and rolls
//!   output at the target size; an input file larger than `max_file_size` is selected (oversized
//!   candidate) and rewritten, but never split before reading.
//! - **Java interop evidence** (the GAP_MATRIX row stays 🟡).

use std::collections::HashMap;

use crate::Catalog;
use crate::error::{Error, ErrorKind, Result};
use crate::expr::Predicate;
use crate::scan::FileScanTask;
use crate::spec::{DataFile, Struct, TableProperties};
use crate::table::Table;
use crate::transaction::{ApplyTransactionAction, Transaction};

/// Java `SizeBasedFileRewritePlanner.MIN_FILE_SIZE_DEFAULT_RATIO` — the default
/// `min_file_size_bytes` is 75% of the target file size.
const MIN_FILE_SIZE_DEFAULT_RATIO: f64 = 0.75;

/// Java `SizeBasedFileRewritePlanner.MAX_FILE_SIZE_DEFAULT_RATIO` — the default
/// `max_file_size_bytes` is 180% of the target file size.
const MAX_FILE_SIZE_DEFAULT_RATIO: f64 = 1.80;

/// Java `SizeBasedFileRewritePlanner.MIN_INPUT_FILES_DEFAULT`.
const MIN_INPUT_FILES_DEFAULT: usize = 5;

/// Java `SizeBasedFileRewritePlanner.MAX_FILE_GROUP_SIZE_BYTES_DEFAULT` = 100 GiB.
const MAX_FILE_GROUP_SIZE_BYTES_DEFAULT: u64 = 100 * 1024 * 1024 * 1024;

/// Java `BinPackRewriteFilePlanner.DELETE_FILE_THRESHOLD_DEFAULT = Integer.MAX_VALUE` — the
/// delete-count candidate clause is disabled by default. Modeled here as `usize::MAX`.
const DELETE_FILE_THRESHOLD_DEFAULT: usize = usize::MAX;

/// The outcome of a [`RewriteDataFiles::execute`] run: the aggregate counts (Java
/// `RewriteDataFiles.Result`) plus the per-group results (Java `FileGroupRewriteResult`).
///
/// A no-op plan (nothing qualified) returns this with all counts zero and `file_groups` empty —
/// no snapshot was committed.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RewriteDataFilesResult {
    /// Total number of data files added across all groups (Java `Result.addedDataFilesCount()`).
    pub added_data_files_count: usize,
    /// Total number of data files rewritten (replaced) across all groups (Java
    /// `Result.rewrittenDataFilesCount()`).
    pub rewritten_data_files_count: usize,
    /// Total bytes of the rewritten (input) data files (Java `Result.rewrittenBytesCount()`).
    pub rewritten_bytes_count: u64,
    /// Per-group results, in commit order (Java `Result.rewriteResults()`).
    pub file_groups: Vec<FileGroupRewriteResult>,
}

/// The result of rewriting a single file group (Java `RewriteDataFiles.FileGroupRewriteResult`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileGroupRewriteResult {
    /// Number of data files added for this group (Java `addedDataFilesCount()`).
    pub added_data_files_count: usize,
    /// Number of data files rewritten (replaced) in this group (Java `rewrittenDataFilesCount()`).
    pub rewritten_data_files_count: usize,
    /// Bytes of the rewritten (input) data files in this group (Java `rewrittenBytesCount()`).
    pub rewritten_bytes_count: u64,
}

/// The bin-pack compaction action. Build it with [`RewriteDataFiles::new`], configure the size /
/// count thresholds with the builder methods, and run it with [`Self::execute`]. See the module
/// docs for the full algorithm, the sequence-number rule, the defaults, and the Java provenance.
pub struct RewriteDataFiles {
    table: Table,
    /// `Some(t)` once the caller pins a target size; `None` ⇒ resolve from the table property at
    /// execute (Java `defaultTargetFileSize`).
    target_file_size_bytes: Option<u64>,
    /// `Some(min)` once the caller pins it; `None` ⇒ `0.75 * target` at execute.
    min_file_size_bytes: Option<u64>,
    /// `Some(max)` once the caller pins it; `None` ⇒ `1.8 * target` at execute.
    max_file_size_bytes: Option<u64>,
    min_input_files: usize,
    delete_file_threshold: usize,
    max_file_group_size_bytes: u64,
    use_starting_sequence_number: bool,
    filter: Predicate,
}

impl RewriteDataFiles {
    /// Create a `RewriteDataFiles` bin-pack action for `table` with Java's defaults (see the module
    /// docs). The size thresholds are resolved lazily at [`Self::execute`] from the table's
    /// `write.target-file-size-bytes` property when not overridden.
    pub fn new(table: Table) -> Self {
        RewriteDataFiles {
            table,
            target_file_size_bytes: None,
            min_file_size_bytes: None,
            max_file_size_bytes: None,
            min_input_files: MIN_INPUT_FILES_DEFAULT,
            delete_file_threshold: DELETE_FILE_THRESHOLD_DEFAULT,
            max_file_group_size_bytes: MAX_FILE_GROUP_SIZE_BYTES_DEFAULT,
            use_starting_sequence_number: true,
            filter: Predicate::AlwaysTrue,
        }
    }

    /// Set the target output file size in bytes (Java `TARGET_FILE_SIZE_BYTES`). When unset, the
    /// table's `write.target-file-size-bytes` property is used (default 512 MiB). Setting this also
    /// shifts the default `min`/`max` thresholds (0.75× / 1.8× of the target) unless those are
    /// independently overridden.
    pub fn target_file_size_bytes(mut self, target_file_size_bytes: u64) -> Self {
        self.target_file_size_bytes = Some(target_file_size_bytes);
        self
    }

    /// Files smaller than this are always candidates for rewriting (Java `MIN_FILE_SIZE_BYTES`).
    /// Defaults to 75% of the target file size.
    pub fn min_file_size_bytes(mut self, min_file_size_bytes: u64) -> Self {
        self.min_file_size_bytes = Some(min_file_size_bytes);
        self
    }

    /// Files larger than this are always candidates for rewriting (Java `MAX_FILE_SIZE_BYTES`).
    /// Defaults to 180% of the target file size.
    pub fn max_file_size_bytes(mut self, max_file_size_bytes: u64) -> Self {
        self.max_file_size_bytes = Some(max_file_size_bytes);
        self
    }

    /// A group with at least this many files is rewritten regardless of total size (Java
    /// `MIN_INPUT_FILES`, default 5). Must be `> 0` — a zero is rejected at [`Self::execute`].
    pub fn min_input_files(mut self, min_input_files: usize) -> Self {
        self.min_input_files = min_input_files;
        self
    }

    /// A file with at least this many associated delete files is a candidate regardless of size,
    /// and a group containing such a file is rewritten regardless of file count (Java
    /// `DELETE_FILE_THRESHOLD`). Defaults to disabled (`usize::MAX`, Java `Integer.MAX_VALUE`).
    pub fn delete_file_threshold(mut self, delete_file_threshold: usize) -> Self {
        self.delete_file_threshold = delete_file_threshold;
        self
    }

    /// The largest total size of input files rewritten in a single group (Java
    /// `MAX_FILE_GROUP_SIZE_BYTES`, default 100 GiB). Must be `> 0`.
    pub fn max_file_group_size_bytes(mut self, max_file_group_size_bytes: u64) -> Self {
        self.max_file_group_size_bytes = max_file_group_size_bytes;
        self
    }

    /// Whether to stamp the rewritten files with the STARTING snapshot's sequence number (Java
    /// `USE_STARTING_SEQUENCE_NUMBER`, default `true`). Keep `true` whenever the table carries
    /// outstanding merge-on-read deletes — see [the sequence-number rule](self#the-sequence-number-rule).
    pub fn use_starting_sequence_number(mut self, use_starting_sequence_number: bool) -> Self {
        self.use_starting_sequence_number = use_starting_sequence_number;
        self
    }

    /// Restrict the rewrite to files matching `filter` (Java `RewriteDataFiles.filter(Expression)`).
    /// The predicate is pushed into the planning scan; only matching data files are considered for
    /// rewriting. Defaults to [`Predicate::AlwaysTrue`] (every file).
    pub fn filter(mut self, filter: Predicate) -> Self {
        self.filter = filter;
        self
    }

    /// Plan the bin-pack compaction, rewrite each selected group into target-sized files, and
    /// commit each group through [`RewriteFilesAction`](crate::transaction::rewrite_files). Reads
    /// each group's LIVE rows (merge-on-read deletes applied) so the rewritten files carry only live
    /// rows. See the module docs for the algorithm, the sequence-number rule, and the defaults.
    ///
    /// Returns a zero-count [`RewriteDataFilesResult`] and commits NOTHING when no file qualifies
    /// (the empty-plan no-op). Returns `Err` when a `sizeThresholds` precondition is violated, when
    /// planning fails, or when a group's commit fails (no partial-progress tolerance).
    pub async fn execute(self, catalog: &dyn Catalog) -> Result<RewriteDataFilesResult> {
        let config = self.resolve_config()?;

        // 1. Enumerate the current snapshot's live data-file scan tasks (each carries size, record
        //    count, partition, spec, and attached delete files) + the full DataFile per path (for
        //    the rewrite removal set). A table with no current snapshot has nothing to compact.
        let Some(starting_snapshot) = self.table.metadata().current_snapshot().cloned() else {
            return Ok(RewriteDataFilesResult::default());
        };
        let starting_snapshot_id = starting_snapshot.snapshot_id();
        let starting_sequence_number = starting_snapshot.sequence_number();

        let tasks = self.plan_scan_tasks().await?;
        let data_files_by_path = self.collect_live_data_files().await?;

        // 2 + 3 + 4 + 5. Group by partition, candidate-filter, bin-pack, group-filter.
        let groups = plan_file_groups(
            tasks,
            &config,
            self.table.metadata().default_partition_spec(),
        );

        if groups.is_empty() {
            // Empty plan — no-op, no commit (Java's empty rewriteResults).
            return Ok(RewriteDataFilesResult::default());
        }

        // 6. Per group: read live rows (deletes applied), write target-sized files, commit one
        //    RewriteFiles replacing exactly the group's data files.
        let mut result = RewriteDataFilesResult::default();
        let mut table = self.table.clone();
        for group in groups {
            let group_result = self
                .rewrite_group(
                    catalog,
                    &table,
                    &group,
                    &data_files_by_path,
                    starting_snapshot_id,
                    starting_sequence_number,
                    config.target_file_size_bytes,
                )
                .await?;

            result.added_data_files_count += group_result.0.added_data_files_count;
            result.rewritten_data_files_count += group_result.0.rewritten_data_files_count;
            result.rewritten_bytes_count += group_result.0.rewritten_bytes_count;
            result.file_groups.push(group_result.0);
            // The committed table becomes the base for the next group's commit (sequential).
            table = group_result.1;
        }

        Ok(result)
    }

    /// Resolve the size/count thresholds, applying Java's defaults (`min = 0.75·target`,
    /// `max = 1.8·target`, `target` from the table property) and Java's `sizeThresholds`
    /// preconditions (`target > 0`, `target > min`, `target < max`, `min_input_files > 0`,
    /// `max_file_group_size_bytes > 0`).
    fn resolve_config(&self) -> Result<ResolvedConfig> {
        let target = match self.target_file_size_bytes {
            Some(target) => target,
            None => parse_target_file_size(self.table.metadata().properties())?,
        };

        if target == 0 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("'target-file-size-bytes' is set to {target} but must be > 0"),
            ));
        }

        // Java: defaultMin = (long)(target * 0.75), defaultMax = (long)(target * 1.8).
        let default_min = (target as f64 * MIN_FILE_SIZE_DEFAULT_RATIO) as u64;
        let default_max = (target as f64 * MAX_FILE_SIZE_DEFAULT_RATIO) as u64;
        let min_file_size_bytes = self.min_file_size_bytes.unwrap_or(default_min);
        let max_file_size_bytes = self.max_file_size_bytes.unwrap_or(default_max);

        if target <= min_file_size_bytes {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "'target-file-size-bytes' ({target}) must be > 'min-file-size-bytes' \
                     ({min_file_size_bytes}), all new files will be smaller than the min threshold"
                ),
            ));
        }
        if target >= max_file_size_bytes {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "'target-file-size-bytes' ({target}) must be < 'max-file-size-bytes' \
                     ({max_file_size_bytes}), all new files will be larger than the max threshold"
                ),
            ));
        }
        if self.min_input_files == 0 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "'min-input-files' is set to 0 but must be > 0",
            ));
        }
        if self.max_file_group_size_bytes == 0 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "'max-file-group-size-bytes' is set to 0 but must be > 0",
            ));
        }

        Ok(ResolvedConfig {
            target_file_size_bytes: target,
            min_file_size_bytes,
            max_file_size_bytes,
            min_input_files: self.min_input_files,
            delete_file_threshold: self.delete_file_threshold,
            max_file_group_size_bytes: self.max_file_group_size_bytes,
        })
    }

    /// Plan the current snapshot's live data-file scan tasks with the configured row filter (Java
    /// `table.newScan().filter(filter).planFiles()`). Each [`FileScanTask`] carries the file size,
    /// record count, partition, spec, and the delete files that apply to it.
    async fn plan_scan_tasks(&self) -> Result<Vec<FileScanTask>> {
        use futures::TryStreamExt;

        let stream = self
            .table
            .scan()
            .with_filter(self.filter.clone())
            .build()?
            .plan_files()
            .await?;
        stream.try_collect().await
    }

    /// Build a `path -> DataFile` map over the current snapshot's LIVE data-file manifest entries.
    /// The full [`DataFile`] is required for the rewrite removal set (the scan task only carries the
    /// path). Mirrors the `RewriteFiles` test's live-entry enumeration.
    async fn collect_live_data_files(&self) -> Result<HashMap<String, DataFile>> {
        use crate::spec::DataContentType;

        let mut by_path: HashMap<String, DataFile> = HashMap::new();
        let metadata = self.table.metadata();
        let Some(snapshot) = metadata.current_snapshot() else {
            return Ok(by_path);
        };
        let manifest_list = snapshot
            .load_manifest_list(self.table.file_io(), metadata)
            .await?;
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(self.table.file_io()).await?;
            for entry in manifest.entries() {
                if entry.is_alive() && entry.content_type() == DataContentType::Data {
                    by_path.insert(entry.file_path().to_string(), entry.data_file().clone());
                }
            }
        }
        Ok(by_path)
    }
}

/// The size/count thresholds after defaults + preconditions are applied (Java
/// `SizeBasedFileRewritePlanner`'s `init`-resolved fields).
struct ResolvedConfig {
    target_file_size_bytes: u64,
    min_file_size_bytes: u64,
    max_file_size_bytes: u64,
    min_input_files: usize,
    delete_file_threshold: usize,
    max_file_group_size_bytes: u64,
}

impl RewriteDataFiles {
    /// Rewrite a single planned group: read its files' LIVE rows (deletes applied), write them into
    /// target-sized file(s), and commit ONE `RewriteFiles` replacing exactly the group's data files
    /// with the new ones (stamping the starting sequence number when `use_starting_sequence_number`).
    /// Returns the per-group result + the committed table (the base for the next group).
    #[allow(clippy::too_many_arguments)]
    async fn rewrite_group(
        &self,
        catalog: &dyn Catalog,
        table: &Table,
        group: &[FileScanTask],
        data_files_by_path: &HashMap<String, DataFile>,
        starting_snapshot_id: i64,
        starting_sequence_number: i64,
        target_file_size_bytes: u64,
    ) -> Result<(FileGroupRewriteResult, Table)> {
        // The full DataFiles to REPLACE (resolved from the live-entry map by path). A path that
        // vanished between planning and now is a fail-loud condition (a concurrent commit removed it
        // — RewriteFiles' own `failMissingDeletePaths` would also reject it, but we surface it here
        // with the path so the operator sees which file).
        let mut files_to_delete: Vec<DataFile> = Vec::with_capacity(group.len());
        let mut rewritten_bytes_count: u64 = 0;
        for task in group {
            let data_file = data_files_by_path
                .get(task.data_file_path())
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Cannot rewrite data file {}: it is no longer a live data file in the \
                         current snapshot (a concurrent commit removed it)",
                            task.data_file_path()
                        ),
                    )
                })?;
            rewritten_bytes_count = rewritten_bytes_count.saturating_add(task.file_size_in_bytes);
            files_to_delete.push(data_file.clone());
        }

        // Read the group's LIVE rows (deletes applied) and write them into target-sized files.
        let added_files = self
            .write_compacted_files(table, group, target_file_size_bytes)
            .await?;

        let group_result = FileGroupRewriteResult {
            added_data_files_count: added_files.len(),
            rewritten_data_files_count: files_to_delete.len(),
            rewritten_bytes_count,
        };

        // Commit ONE RewriteFiles replacing the group's files (Java `RewriteDataFilesCommitManager`).
        let transaction = Transaction::new(table);
        let mut action = transaction
            .rewrite_files(files_to_delete, added_files)
            .validate_from_snapshot(starting_snapshot_id);
        if self.use_starting_sequence_number {
            // Java: dataSequenceNumber(table.snapshot(startingSnapshotId).sequenceNumber()).
            action = action.data_sequence_number(starting_sequence_number);
        }
        let transaction = action.apply(transaction)?;
        let committed = transaction.commit(catalog).await?;

        Ok((group_result, committed))
    }

    /// Read the group's live rows (a scan restricted to the group's data-file paths, deletes
    /// applied via the attached delete files each task carries) and write them into target-sized
    /// data file(s) via the rolling data-file writer. Returns the written [`DataFile`]s.
    ///
    /// All files in a single group share one partition tuple under the current spec (the planner
    /// groups by partition), so the output files are written under that one partition key.
    async fn write_compacted_files(
        &self,
        table: &Table,
        group: &[FileScanTask],
        target_file_size_bytes: u64,
    ) -> Result<Vec<DataFile>> {
        use futures::TryStreamExt;

        use crate::arrow::ArrowReaderBuilder;
        use crate::spec::{DataFileFormat, PartitionKey};
        use crate::writer::base_writer::data_file_writer::DataFileWriterBuilder;
        use crate::writer::file_writer::ParquetWriterBuilder;
        use crate::writer::file_writer::location_generator::{
            DefaultFileNameGenerator, DefaultLocationGenerator,
        };
        use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
        use crate::writer::{IcebergWriter, IcebergWriterBuilder};

        let schema = table.metadata().current_schema().clone();
        let spec = table.metadata().default_partition_spec().as_ref().clone();

        // The group's partition tuple — every task in the group shares it (planner groups by
        // partition). Use the first task's partition; an unpartitioned table has an empty struct.
        let partition = group
            .first()
            .and_then(|task| task.partition.clone())
            .unwrap_or_else(Struct::empty);
        let partition_key = if spec.is_unpartitioned() {
            None
        } else {
            Some(PartitionKey::new(spec, schema.clone(), partition))
        };

        // Build the rolling data-file writer rolling at the target size.
        let location_generator = DefaultLocationGenerator::new(table.metadata().clone())?;
        let file_name_generator = DefaultFileNameGenerator::new(
            "compacted".to_string(),
            Some(uuid::Uuid::now_v7().to_string()),
            DataFileFormat::Parquet,
        );
        let parquet_builder = ParquetWriterBuilder::new(
            parquet::file::properties::WriterProperties::builder().build(),
            schema.clone(),
        );
        let rolling_builder = RollingFileWriterBuilder::new(
            parquet_builder,
            usize::try_from(target_file_size_bytes).unwrap_or(usize::MAX),
            table.file_io().clone(),
            location_generator,
            file_name_generator,
        );
        let mut writer = DataFileWriterBuilder::new(rolling_builder)
            .build(partition_key)
            .await?;

        // Read the group's tasks (each carries its delete files), with deletes applied. STRIP the
        // per-file RESIDUAL (`task.predicate`) the planning scan attached for `self.filter`: the
        // filter is a FILE-SELECTION device (which files become candidates), NOT a row filter on the
        // rewrite read. If the residual reached the reader it would become a row-level `RowFilter`
        // (`arrow::reader`), and a file whose rows only PARTIALLY match the filter would have its
        // non-matching LIVE rows silently dropped — a compaction MUST conserve every live row. This
        // is the Rust analogue of Java's `BinPackRewriteFilePlanner.planFileGroups` building the plan
        // scan with `.ignoreResiduals()` so the runner reads all rows of every selected file. The
        // delete files each task carries are RETAINED (merge-on-read deletes still apply).
        let tasks: Vec<Result<FileScanTask>> = group
            .iter()
            .cloned()
            .map(|mut task| {
                task.predicate = None;
                Ok(task)
            })
            .collect();
        let task_stream = Box::pin(futures::stream::iter(tasks)) as crate::scan::FileScanTaskStream;
        let batch_stream = ArrowReaderBuilder::new(table.file_io().clone())
            .build()
            .read(task_stream)?;
        let batches: Vec<_> = batch_stream.try_collect().await?;

        for batch in batches {
            // The scan read schema equals the table's current schema; the data-file writer's
            // parquet schema is the same — no projection needed.
            writer.write(batch).await?;
        }

        writer.close().await
    }
}

/// Group the scan tasks by partition (current spec else empty), candidate-filter, bin-pack, and
/// group-filter (Java `BinPackRewriteFilePlanner.planFileGroups` + `filterFileGroups`). Returns the
/// surviving groups, each a `Vec<FileScanTask>` to rewrite. Groups within a partition are emitted in
/// the packer's order; partitions in arbitrary map order (commit order does not affect correctness).
fn plan_file_groups(
    tasks: Vec<FileScanTask>,
    config: &ResolvedConfig,
    default_spec: &crate::spec::PartitionSpecRef,
) -> Vec<Vec<FileScanTask>> {
    let default_spec_id = default_spec.spec_id();

    // 2. Group by partition. Java `groupByPartition`: key by the file's partition when its spec id
    //    equals the table's CURRENT default spec id, else the empty struct (un-partitioned bucket).
    let mut by_partition: HashMap<Struct, Vec<FileScanTask>> = HashMap::new();
    for task in tasks {
        let key = match (&task.partition, task_spec_id(&task)) {
            (Some(partition), Some(spec_id)) if spec_id == default_spec_id => partition.clone(),
            _ => Struct::empty(),
        };
        by_partition.entry(key).or_default().push(task);
    }

    let mut groups: Vec<Vec<FileScanTask>> = Vec::new();
    for (_partition, partition_tasks) in by_partition {
        // 3. Candidate filter (Java `filterFiles`): undersized OR oversized OR delete-laden.
        let candidates: Vec<FileScanTask> = partition_tasks
            .into_iter()
            .filter(|task| is_candidate(task, config))
            .collect();
        if candidates.is_empty() {
            continue;
        }

        // 4. Bin-pack the partition's candidates into groups of total size <= max_file_group_size.
        let bins = pack_bins(candidates, config.max_file_group_size_bytes);

        // 5. Group filter (Java `filterFileGroups`).
        for bin in bins {
            if group_qualifies(&bin, config) {
                groups.push(bin);
            }
        }
    }
    groups
}

/// The file's partition spec id, if the task carries its spec (it does after scan planning).
fn task_spec_id(task: &FileScanTask) -> Option<i32> {
    task.partition_spec.as_ref().map(|spec| spec.spec_id())
}

/// Java `BinPackRewriteFilePlanner.filterFiles`: a task is a candidate iff it is undersized/oversized
/// (`outsideDesiredFileSizeRange`) OR has at least `delete_file_threshold` delete files
/// (`tooManyDeletes`). The `tooHighDeleteRatio` clause is deferred (never fires — see module docs).
fn is_candidate(task: &FileScanTask, config: &ResolvedConfig) -> bool {
    let length = task.file_size_in_bytes;
    let outside_desired_size =
        length < config.min_file_size_bytes || length > config.max_file_size_bytes;
    let too_many_deletes = task.deletes.len() >= config.delete_file_threshold;
    outside_desired_size || too_many_deletes
}

/// Java `SizeBasedFileRewritePlanner.filterFileGroups` (+ `BinPackRewriteFilePlanner` delete clause):
/// keep a group iff `enoughInputFiles` (`size > 1 && size >= min_input_files`) OR `enoughContent`
/// (`size > 1 && inputSize > target`) OR `tooMuchContent` (`inputSize > max_file_size`) OR any file
/// is delete-laden (`anyMatch(tooManyDeletes)`).
fn group_qualifies(group: &[FileScanTask], config: &ResolvedConfig) -> bool {
    let size = group.len();
    let input_size: u64 = group.iter().fold(0u64, |sum, task| {
        sum.saturating_add(task.file_size_in_bytes)
    });

    let enough_input_files = size > 1 && size >= config.min_input_files;
    let enough_content = size > 1 && input_size > config.target_file_size_bytes;
    let too_much_content = input_size > config.max_file_size_bytes;
    let any_too_many_deletes = group
        .iter()
        .any(|task| task.deletes.len() >= config.delete_file_threshold);

    enough_input_files || enough_content || too_much_content || any_too_many_deletes
}

/// Forward greedy first-fit bin-packing — the materialized form of Java
/// `BinPacking.ListPacker(maxGroupSize, lookback=1, largestBinFirst=false, maxGroupCount).pack`
/// (`BinPacking.PackingIterator.next`). With `lookback = 1` there is a single open bin at a time:
/// each task is placed in the open bin if it still fits (`bin_weight + length <= target_weight`),
/// else the open bin is closed and a fresh one opened. (This is the SAME algorithm the fork's
/// merge-append `bin_packing::pack` implements, but that module is `pub(crate)`-private to
/// `transaction/merge_append.rs`; opening it would be a `transaction/` change, which is out of this
/// action's scope, so the lookback-1 case is reimplemented locally here.)
///
/// `maxGroupCount` (Java `MAX_FILE_GROUP_INPUT_FILES`, default `Long.MAX_VALUE`) is not exposed by
/// this action, so there is no per-bin item cap. Sizes come from the manifest (trusted within the
/// table), but the running sum is saturated defensively.
fn pack_bins(tasks: Vec<FileScanTask>, target_weight: u64) -> Vec<Vec<FileScanTask>> {
    let mut bins: Vec<Vec<FileScanTask>> = Vec::new();
    let mut open_bin: Vec<FileScanTask> = Vec::new();
    let mut open_weight: u64 = 0;

    for task in tasks {
        let weight = task.file_size_in_bytes;
        if !open_bin.is_empty() && open_weight.saturating_add(weight) <= target_weight {
            open_weight = open_weight.saturating_add(weight);
            open_bin.push(task);
        } else {
            if !open_bin.is_empty() {
                bins.push(std::mem::take(&mut open_bin));
            }
            open_weight = weight;
            open_bin.push(task);
        }
    }
    if !open_bin.is_empty() {
        bins.push(open_bin);
    }
    bins
}

/// Parse the `write.target-file-size-bytes` table property (Java `defaultTargetFileSize` via
/// `PropertyUtil.propertyAsLong`). A present-but-unparsable value is a loud error; absent yields the
/// 512 MiB default. A negative value is rejected (`target` must be `> 0`, enforced downstream too).
fn parse_target_file_size(properties: &HashMap<String, String>) -> Result<u64> {
    match properties.get(TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES) {
        None => Ok(TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT as u64),
        Some(value) => value.parse::<u64>().map_err(|error| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Invalid value '{value}' for table property \
                     '{}'",
                    TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES
                ),
            )
            .with_source(error)
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int64Array, RecordBatch};
    use futures::TryStreamExt;
    use tempfile::TempDir;

    use super::*;
    use crate::io::LocalFsStorageFactory;
    use crate::memory::MemoryCatalogBuilder;
    use crate::spec::{
        DataContentType, DataFile, DataFileFormat, Literal, NestedField, PartitionSpec,
        PrimitiveType, Schema, Struct, Transform, Type,
    };
    use crate::table::Table;
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

    // ============================================================================================
    // Test harness — a local-fs memory catalog (REAL parquet on disk) + a table partitioned by
    // identity(x) with three long columns x/y/z, mirroring the rewrite_files crown-jewel fixtures.
    // ============================================================================================

    /// A memory catalog whose FileIO is the local filesystem, rooted at a fresh `TempDir`. Returns
    /// the catalog and the temp-dir guard (kept alive for the test).
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
                HashMap::from([("warehouse".to_string(), warehouse)]),
            )
            .await
            .expect("load local-fs memory catalog");
        (catalog, temp_dir)
    }

    /// A schema of three required long columns `x`, `y`, `z`.
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

    /// A table partitioned by identity(x), format version `format_version`, under a fresh namespace.
    async fn create_partitioned_table(
        catalog: &impl Catalog,
        format_version: crate::spec::FormatVersion,
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
            .create_namespace(&namespace, HashMap::new())
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

    /// Write a REAL parquet data file with rows `(x, y, z)` into the table location, routed to
    /// partition `x = part_value`. Returns the finished partitioned [`DataFile`].
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

    /// Write a REAL equality-delete parquet file deleting rows whose `y` (field id 2) equals one of
    /// `delete_ys`, in partition `x = part_value`. Mirrors the rewrite_files fixture.
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

    /// Write a REAL parquet position-delete file deleting `(data_file_path, pos)` pairs, in
    /// partition `x = part_value`.
    async fn write_position_delete_file(
        table: &Table,
        part_value: i64,
        deletes: &[(String, i64)],
    ) -> DataFile {
        use arrow_array::StringArray;

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

    /// Append `files` in one fast-append commit, returning the updated table.
    async fn append_files(catalog: &impl Catalog, table: &Table, files: Vec<DataFile>) -> Table {
        let tx = Transaction::new(table);
        let action = tx.fast_append().add_data_files(files);
        let tx = action.apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    /// Add `deletes` (a row_delta) in one commit, returning the updated table.
    async fn add_deletes(catalog: &impl Catalog, table: &Table, deletes: Vec<DataFile>) -> Table {
        let tx = Transaction::new(table);
        let action = tx.row_delta().add_deletes(deletes);
        let tx = action.apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    /// Scan the table and collect ALL `(x, y, z)` rows the scan returns (merge-on-read deletes
    /// applied) — the real read-side signal. Sorted compare, not counts.
    async fn scan_rows(table: &Table) -> Vec<(i64, i64, i64)> {
        let stream = table
            .scan()
            .select(["x", "y", "z"])
            .build()
            .unwrap()
            .to_arrow()
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let mut rows: Vec<(i64, i64, i64)> = Vec::new();
        for batch in batches {
            let xs = column_i64(&batch, "x");
            let ys = column_i64(&batch, "y");
            let zs = column_i64(&batch, "z");
            for index in 0..xs.len() {
                rows.push((xs.value(index), ys.value(index), zs.value(index)));
            }
        }
        rows.sort_unstable();
        rows
    }

    /// Downcast a named column of `batch` to `Int64Array`.
    fn column_i64<'a>(batch: &'a RecordBatch, name: &str) -> &'a Int64Array {
        let index = batch.schema().index_of(name).unwrap();
        batch
            .column(index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
    }

    /// The set of live (Added/Existing) data-file paths in the table's current snapshot.
    async fn live_data_file_paths(table: &Table) -> HashSet<String> {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let mut paths = HashSet::new();
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                if entry.is_alive() && entry.content_type() == DataContentType::Data {
                    paths.insert(entry.file_path().to_string());
                }
            }
        }
        paths
    }

    /// The current snapshot id (or `None` for a fresh table).
    fn current_snapshot_id(table: &Table) -> Option<i64> {
        table.metadata().current_snapshot_id()
    }

    /// The EXPLICIT (pre-inheritance) on-disk data sequence number of every live data file added by
    /// the current snapshot (raw avro, NO inheritance), keyed by path. A file with no explicit seq
    /// (would re-inherit the snapshot seq) maps to `None`. Reads the raw manifest bytes via
    /// `Manifest::try_from_avro_bytes`, exactly like the rewrite_files seq pin.
    async fn on_disk_data_seqs(table: &Table) -> HashMap<String, Option<i64>> {
        use crate::spec::Manifest;

        let mut seqs = HashMap::new();
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        for manifest_file in manifest_list.entries() {
            let bytes = table
                .file_io()
                .new_input(&manifest_file.manifest_path)
                .unwrap()
                .read()
                .await
                .unwrap();
            let (_, raw_entries) = Manifest::try_from_avro_bytes(&bytes).unwrap();
            for entry in raw_entries {
                if entry.is_alive() && entry.content_type() == DataContentType::Data {
                    seqs.insert(entry.file_path().to_string(), entry.sequence_number());
                }
            }
        }
        seqs
    }

    // ============================================================================================
    // E2E tests on the local-fs MemoryCatalog + real parquet.
    // ============================================================================================

    /// CROWN JEWEL — ROW CONSERVATION (risk: a compaction that drops or duplicates rows = silent
    /// data corruption). Append N small files in one partition with known rows; compact; the FULL
    /// post-compaction scan row SET must EQUAL the pre-compaction live row set EXACTLY (sorted
    /// compare, not counts), and the file count must DROP. A group that loses or dupes any row fails.
    #[tokio::test]
    async fn test_bin_pack_compaction_conserves_every_row_exactly() {
        let (catalog, _temp) = local_fs_catalog().await;
        let table = create_partitioned_table(&catalog, crate::spec::FormatVersion::V2).await;

        // 6 small files in partition x=0, each one row; distinct y so a drop/dup is detectable.
        let mut files = Vec::new();
        for index in 0..6i64 {
            files.push(
                write_data_file(&table, &format!("small-{index}.parquet"), 0, &[(
                    0,
                    100 + index,
                    1000 + index,
                )])
                .await,
            );
        }
        let table = append_files(&catalog, &table, files).await;

        let rows_before = scan_rows(&table).await;
        let files_before = live_data_file_paths(&table).await.len();
        assert_eq!(files_before, 6, "fixture: 6 small files before compaction");

        // Target larger than the sum so all 6 pack into one group; min_input_files default 5 ⇒
        // the 6-file group qualifies via enoughInputFiles. min_file_size huge ⇒ every file undersized.
        let result = RewriteDataFiles::new(table.clone())
            .target_file_size_bytes(1_000_000)
            .execute(&catalog)
            .await
            .expect("compaction must succeed");

        let table = catalog.load_table(table.identifier()).await.unwrap();
        let rows_after = scan_rows(&table).await;
        let files_after = live_data_file_paths(&table).await.len();

        assert_eq!(
            rows_after, rows_before,
            "the post-compaction scan must return EXACTLY the pre-compaction live rows (no drop, no dup)"
        );
        assert!(
            files_after < files_before,
            "compaction must reduce the file count ({files_before} -> {files_after})"
        );
        assert_eq!(
            result.rewritten_data_files_count, 6,
            "all 6 input files were rewritten"
        );
        assert_eq!(
            result.added_data_files_count, files_after,
            "the result's added count matches the new live file count"
        );
    }

    /// RICHER-SCHEMA WRITE-PATH FIDELITY (risk: a type drifting through the arrow round-trip
    /// read→write — a decimal losing precision/scale, a timestamp losing its unit/timezone, or the
    /// iceberg field IDs not being re-stamped on the rewritten parquet so the new file is unreadable).
    /// The other conservation tests use a 3-long schema; this one compacts a table with `long` +
    /// `decimal(9,2)` + `timestamptz` columns and asserts (a) every row survives byte-exactly, (b)
    /// the rewritten parquet carries the iceberg field IDs in its arrow schema metadata, and (c) the
    /// committed DataFile's record_count is correct.
    #[tokio::test]
    async fn test_richer_schema_compaction_conserves_rows_field_ids_and_stats() {
        use arrow_array::{Decimal128Array, TimestampMicrosecondArray};
        use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

        use crate::arrow::schema_to_arrow_schema;

        let (catalog, _temp) = local_fs_catalog().await;

        // Schema: id long, amount decimal(9,2), ts timestamptz. Unpartitioned.
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::required(
                    2,
                    "amount",
                    Type::Primitive(PrimitiveType::Decimal {
                        precision: 9,
                        scale: 2,
                    }),
                )),
                Arc::new(NestedField::required(
                    3,
                    "ts",
                    Type::Primitive(PrimitiveType::Timestamptz),
                )),
            ])
            .build()
            .expect("build richer schema");
        let namespace = NamespaceIdent::new(format!("ns-{}", uuid::Uuid::new_v4()));
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .expect("create namespace");
        let table_ident = TableIdent::new(namespace.clone(), "rich".to_string());
        let creation = TableCreation::builder()
            .name(table_ident.name().to_string())
            .schema(schema.clone())
            .format_version(crate::spec::FormatVersion::V2)
            .build();
        let table = catalog
            .create_table(&namespace, creation)
            .await
            .expect("create richer table");

        let arrow_schema = Arc::new(schema_to_arrow_schema(&schema).unwrap());

        // Write 5 single-row files; amount values keep 2-decimal precision, ts are distinct micros.
        let mut files = Vec::new();
        for index in 0..5i64 {
            // Unscaled value 10_000 with scale 2 == 100.00; +index gives 100.01, 100.02, ...
            let amount = Decimal128Array::from(vec![10_000 + index as i128])
                .with_precision_and_scale(9, 2)
                .unwrap();
            let ts = TimestampMicrosecondArray::from(vec![1_700_000_000_000_000 + index])
                .with_timezone_utc();
            let batch = RecordBatch::try_new(arrow_schema.clone(), vec![
                Arc::new(Int64Array::from(vec![index])) as ArrayRef,
                Arc::new(amount) as ArrayRef,
                Arc::new(ts) as ArrayRef,
            ])
            .unwrap();

            let file_path = format!("{}/data/rich-{index}.parquet", table.metadata().location());
            let output = table.file_io().new_output(file_path).unwrap();
            let parquet_builder = ParquetWriterBuilder::new(
                parquet::file::properties::WriterProperties::builder().build(),
                Arc::new(schema.clone()),
            );
            let mut writer = parquet_builder.build(output).await.unwrap();
            writer.write(&batch).await.unwrap();
            let mut builder = writer.close().await.unwrap().into_iter().next().unwrap();
            files.push(
                builder
                    .content(DataContentType::Data)
                    .partition(Struct::empty())
                    .build()
                    .unwrap(),
            );
        }
        let table = append_files(&catalog, &table, files).await;

        // Read all rows before (id, amount-raw-i128, ts-micros) as a sortable signal.
        let read_rich = |table: Table| async move {
            let stream = table.scan().build().unwrap().to_arrow().await.unwrap();
            let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
            let mut rows: Vec<(i64, i128, i64)> = Vec::new();
            for batch in batches {
                let ids = column_i64(&batch, "id");
                let amounts = batch
                    .column(batch.schema().index_of("amount").unwrap())
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap();
                let timestamps = batch
                    .column(batch.schema().index_of("ts").unwrap())
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap();
                for index in 0..ids.len() {
                    rows.push((
                        ids.value(index),
                        amounts.value(index),
                        timestamps.value(index),
                    ));
                }
            }
            rows.sort_unstable();
            rows
        };
        let rows_before = read_rich(table.clone()).await;
        assert_eq!(rows_before.len(), 5, "fixture: 5 rows");

        let result = RewriteDataFiles::new(table.clone())
            .target_file_size_bytes(1_000_000)
            .execute(&catalog)
            .await
            .expect("richer-schema compaction must succeed");
        assert_eq!(result.rewritten_data_files_count, 5);

        let table = catalog.load_table(table.identifier()).await.unwrap();
        let rows_after = read_rich(table.clone()).await;
        assert_eq!(
            rows_after, rows_before,
            "every row survives byte-exactly through the decimal/timestamp arrow round-trip"
        );

        // The rewritten parquet must carry the iceberg field IDs (1,2,3) in its arrow schema, and the
        // committed DataFile's record_count must equal the 5 conserved rows.
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let mut total_records: u64 = 0;
        let mut checked_field_ids = false;
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                if !(entry.is_alive() && entry.content_type() == DataContentType::Data) {
                    continue;
                }
                total_records += entry.data_file().record_count();
                // Re-open the rewritten parquet and read its arrow schema's field-id metadata.
                let input = table.file_io().new_input(entry.file_path()).unwrap();
                let bytes = input.read().await.unwrap();
                let arrow_reader_builder =
                    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(bytes)
                        .unwrap();
                let field_ids: HashSet<i32> = arrow_reader_builder
                    .schema()
                    .fields()
                    .iter()
                    .filter_map(|field| {
                        field
                            .metadata()
                            .get(PARQUET_FIELD_ID_META_KEY)
                            .and_then(|value| value.parse::<i32>().ok())
                    })
                    .collect();
                assert_eq!(
                    field_ids,
                    HashSet::from([1, 2, 3]),
                    "the rewritten parquet must carry iceberg field IDs 1,2,3 in its arrow schema"
                );
                checked_field_ids = true;
            }
        }
        assert!(
            checked_field_ids,
            "at least one rewritten file was inspected"
        );
        assert_eq!(
            total_records, 5,
            "the committed DataFile record counts sum to the 5 conserved rows"
        );
    }

    /// FILTER-LEAK GUARD — THE DATA-LOSS CLASS (risk: `.filter(Predicate)` leaking into the GROUP
    /// READ as a row-level filter, so compacting a file whose rows only PARTIALLY match the filter
    /// SILENTLY DISCARDS the non-matching live rows). The filter must affect PLANNING ONLY (which
    /// files become candidates), never the rewrite read — Java builds the plan scan with
    /// `.ignoreResiduals()` (`BinPackRewriteFilePlanner.planFileGroups`) and the Spark runner reads
    /// the group's tasks with NO row filter, so it reads ALL rows of every selected file.
    ///
    /// Construct 5 undersized files in partition x=0, each holding rows ON BOTH SIDES of `y = 100`.
    /// Compact filtered by `y >= 100`: the files are selected (they have matching rows) and form a
    /// qualifying group; the rewritten table MUST still contain EVERY live row — the `y < 100` rows
    /// included. If the residual leaks into the read, those rows vanish and this test FAILS.
    ///
    /// MUTATION (run manually): restore the residual leak (drop the `task.predicate = None` strip in
    /// `write_compacted_files`) ⇒ the `y < 100` rows are dropped from the output ⇒ this test FAILS.
    #[tokio::test]
    async fn test_filtered_compaction_keeps_non_matching_live_rows() {
        use crate::expr::Reference;
        use crate::spec::Datum;

        let (catalog, _temp) = local_fs_catalog().await;
        let table = create_partitioned_table(&catalog, crate::spec::FormatVersion::V2).await;

        // 5 undersized files in partition x=0; each file has TWO rows straddling y = 100: one below
        // (y = 10 + index, all < 100) and one at/above (y = 100 + index, all >= 100). A filter of
        // `y >= 100` matches only the upper row of each file.
        let mut files = Vec::new();
        for index in 0..5i64 {
            files.push(
                write_data_file(&table, &format!("split-{index}.parquet"), 0, &[
                    (0, 10 + index, 1000 + index),
                    (0, 100 + index, 2000 + index),
                ])
                .await,
            );
        }
        let table = append_files(&catalog, &table, files).await;

        let rows_before = scan_rows(&table).await;
        assert_eq!(rows_before.len(), 10, "fixture: 10 live rows (2 per file)");
        assert!(
            rows_before.iter().any(|(_, y, _)| *y < 100)
                && rows_before.iter().any(|(_, y, _)| *y >= 100),
            "fixture: rows straddle the y=100 filter boundary"
        );

        // Compact filtered by `y >= 100`. The filter selects the files (they contain matching rows)
        // but MUST NOT drop the y<100 rows of the selected files on the rewrite read.
        let result = RewriteDataFiles::new(table.clone())
            .target_file_size_bytes(1_000_000)
            .filter(Reference::new("y").greater_than_or_equal_to(Datum::long(100)))
            .execute(&catalog)
            .await
            .expect("filtered compaction must succeed");
        assert_eq!(
            result.rewritten_data_files_count, 5,
            "all 5 selected files were rewritten"
        );

        let table = catalog.load_table(table.identifier()).await.unwrap();
        let rows_after = scan_rows(&table).await;
        assert_eq!(
            rows_after, rows_before,
            "EVERY live row survives a filtered compaction — the y<100 rows of the rewritten files \
             must NOT be dropped (the filter is a file-selection device, not a row filter on the read)"
        );
    }

    /// CANDIDATE SELECTION (risk: rewriting a well-sized file, or skipping an undersized one). An
    /// already-target-sized file must be UNTOUCHED (its path identical in the new snapshot);
    /// undersized files ARE rewritten. Pins the `outsideDesiredFileSizeRange` predicate both ways.
    #[tokio::test]
    async fn test_target_sized_file_untouched_undersized_rewritten() {
        let (catalog, _temp) = local_fs_catalog().await;
        let table = create_partitioned_table(&catalog, crate::spec::FormatVersion::V2).await;

        // 5 small files in partition x=0 (≥ min_input_files ⇒ they form a qualifying group) +
        // 1 large file in partition x=1 that is within [min, max] (well-sized ⇒ NOT a candidate).
        let mut small_files = Vec::new();
        for index in 0..5i64 {
            small_files.push(
                write_data_file(&table, &format!("s-{index}.parquet"), 0, &[(
                    0, index, index,
                )])
                .await,
            );
        }
        // A file with ~20 rows in partition x=1: bigger than the small ones.
        let big_rows: Vec<(i64, i64, i64)> = (0..200).map(|n| (1, n, n)).collect();
        let big = write_data_file(&table, "big.parquet", 1, &big_rows).await;
        let big_path = big.file_path().to_string();
        let big_size = big.file_size_in_bytes();

        let mut all = small_files;
        all.push(big);
        let table = append_files(&catalog, &table, all).await;

        let rows_before = scan_rows(&table).await;

        // Set min/max so the big file is well-sized (within [min,max]) but the small files are
        // undersized: target so that min_file_size < big_size < max_file_size, and small << min.
        let result = RewriteDataFiles::new(table.clone())
            .target_file_size_bytes(big_size)
            .min_file_size_bytes(big_size / 2)
            .max_file_size_bytes(big_size * 2)
            .execute(&catalog)
            .await
            .expect("compaction must succeed");

        let table = catalog.load_table(table.identifier()).await.unwrap();
        let live = live_data_file_paths(&table).await;

        assert!(
            live.contains(&big_path),
            "the well-sized file must be untouched (same path in the new snapshot)"
        );
        assert_eq!(
            result.rewritten_data_files_count, 5,
            "only the 5 undersized files were rewritten, not the well-sized one"
        );
        assert_eq!(
            scan_rows(&table).await,
            rows_before,
            "row conservation across the selective compaction"
        );
    }

    /// DELETE-APPLICABILITY PRESERVATION — THE RESURRECTION GUARD (risk: a compaction that bumps the
    /// rewritten data's sequence number above an outstanding equality delete RESURRECTS deleted
    /// rows). An equality delete at seq 3 removes y=20 from a data file at seq <3. After compaction
    /// WITH `use_starting_sequence_number` (default), the rewritten data carries the starting
    /// snapshot's seq, so the equality delete STILL applies — the scan still drops y=20.
    ///
    /// MUTATION (run manually): force `use_starting_sequence_number = false` in the action (or pass
    /// `.use_starting_sequence_number(false)`) ⇒ y=20 resurrects ⇒ this test FAILS.
    #[tokio::test]
    async fn test_compaction_preserves_outstanding_equality_delete_no_resurrection() {
        let (catalog, _temp) = local_fs_catalog().await;
        let table = create_partitioned_table(&catalog, crate::spec::FormatVersion::V2).await;

        // 5 small files in partition x=0 (a qualifying group), rows y = 10..50 (one per file),
        // INCLUDING y=20. A 6th file to ensure y=20's file is small/undersized too.
        let mut files = Vec::new();
        for index in 0..5i64 {
            let y = 10 + index * 10; // 10, 20, 30, 40, 50
            files.push(
                write_data_file(&table, &format!("d-{index}.parquet"), 0, &[(0, y, y * 10)]).await,
            );
        }
        let table = append_files(&catalog, &table, files).await;

        // An equality delete (equality_ids=[y]) removing y=20, at a higher seq.
        let eq_delete = write_equality_delete_file(&table, 0, &[20]).await;
        let table = add_deletes(&catalog, &table, vec![eq_delete]).await;

        // Before compaction the scan drops y=20.
        let rows_before = scan_rows(&table).await;
        assert!(
            !rows_before.iter().any(|(_, y, _)| *y == 20),
            "the equality delete drops y=20 before compaction"
        );

        // Compact with use_starting_sequence_number = TRUE (default).
        let result = RewriteDataFiles::new(table.clone())
            .target_file_size_bytes(1_000_000)
            .execute(&catalog)
            .await
            .expect("compaction must succeed on a table with outstanding deletes");
        assert_eq!(result.rewritten_data_files_count, 5);

        let table = catalog.load_table(table.identifier()).await.unwrap();
        let rows_after = scan_rows(&table).await;
        assert!(
            !rows_after.iter().any(|(_, y, _)| *y == 20),
            "with the starting seq preserved, the equality delete STILL drops y=20 — no resurrection"
        );
        assert_eq!(
            rows_after, rows_before,
            "the live row set is unchanged by the compaction (deletes still applied)"
        );
    }

    /// THE SEQUENCE-NUMBER MECHANISM PIN (risk: the seq flag is wired wrong, so concurrent equality
    /// deletes silently stop applying). The seq stamped on the rewritten files is the load-bearing
    /// resurrection guard. This pins it MECHANISTICALLY (raw on-disk seq, mutation-sensitive) BOTH
    /// directions — the scan-level no-resurrection lives in the previous test (compaction reads
    /// deletes-applied, so an EXISTING delete's rows are physically removed regardless of seq; the
    /// seq is what keeps a CONCURRENT equality delete applying — Java `useStartingSequenceNumber`).
    ///
    /// MUTATION (run manually): in `rewrite_group`, drop the `data_sequence_number` call ⇒ the
    /// `use_starting_sequence_number = true` branch below sees a FRESH (higher) seq, not the
    /// starting snapshot's seq ⇒ the equality-stamp assertion FAILS.
    #[tokio::test]
    async fn test_rewritten_file_carries_starting_seq_with_flag_else_fresh() {
        let (catalog, _temp) = local_fs_catalog().await;

        // --- WITH use_starting_sequence_number (default TRUE): on-disk seq == starting snapshot seq.
        {
            let table = create_partitioned_table(&catalog, crate::spec::FormatVersion::V2).await;
            let mut files = Vec::new();
            for index in 0..5i64 {
                files.push(
                    write_data_file(&table, &format!("d-{index}.parquet"), 0, &[(
                        0, index, index,
                    )])
                    .await,
                );
            }
            let table = append_files(&catalog, &table, files).await;
            // Land an equality delete so the table has a higher-seq delete (the seq we must preserve
            // the data BELOW). starting_seq is the head AFTER this delete commit.
            let eq_delete = write_equality_delete_file(&table, 0, &[2]).await;
            let table = add_deletes(&catalog, &table, vec![eq_delete]).await;
            let starting_seq = table
                .metadata()
                .current_snapshot()
                .unwrap()
                .sequence_number();

            let old_paths = live_data_file_paths(&table).await;
            RewriteDataFiles::new(table.clone())
                .target_file_size_bytes(1_000_000)
                .execute(&catalog)
                .await
                .expect("compaction must succeed");

            let table = catalog.load_table(table.identifier()).await.unwrap();
            let seqs = on_disk_data_seqs(&table).await;
            let new_files: Vec<_> = seqs.keys().filter(|p| !old_paths.contains(*p)).collect();
            assert!(!new_files.is_empty(), "compaction produced new data files");
            for path in new_files {
                assert_eq!(
                    seqs[path],
                    Some(starting_seq),
                    "the rewritten file must carry the STARTING snapshot's seq EXPLICITLY on disk \
                     (so a concurrent equality delete at a higher seq still applies)"
                );
            }
        }

        // --- WITHOUT use_starting_sequence_number: the rewritten file takes a FRESH (higher) seq.
        {
            let table = create_partitioned_table(&catalog, crate::spec::FormatVersion::V2).await;
            let mut files = Vec::new();
            for index in 0..5i64 {
                files.push(
                    write_data_file(&table, &format!("e-{index}.parquet"), 0, &[(
                        0, index, index,
                    )])
                    .await,
                );
            }
            let table = append_files(&catalog, &table, files).await;
            let starting_seq = table
                .metadata()
                .current_snapshot()
                .unwrap()
                .sequence_number();
            let old_paths = live_data_file_paths(&table).await;

            RewriteDataFiles::new(table.clone())
                .target_file_size_bytes(1_000_000)
                .use_starting_sequence_number(false)
                .execute(&catalog)
                .await
                .expect("compaction must succeed");

            let table = catalog.load_table(table.identifier()).await.unwrap();
            let new_seq = table
                .metadata()
                .current_snapshot()
                .unwrap()
                .sequence_number();
            assert!(
                new_seq > starting_seq,
                "the rewrite minted a new (higher) snapshot seq"
            );
            let seqs = on_disk_data_seqs(&table).await;
            for (path, seq) in &seqs {
                if !old_paths.contains(path) {
                    // No explicit seq stamp ⇒ re-inherits the new (higher) snapshot seq.
                    assert_eq!(
                        *seq, None,
                        "without the flag the rewritten file has NO explicit seq (re-inherits fresh)"
                    );
                }
            }
        }
    }

    /// DELETE-APPLICABILITY — POSITION-DELETE / DANGLE VARIANT (risk: a compaction reading a
    /// position-deleted file must drop the deleted row physically; the old position delete then
    /// dangles harmlessly). A position delete removes row 0 of a data file. After compaction the
    /// rewritten file contains only the LIVE rows; the scan does not resurrect the deleted row.
    #[tokio::test]
    async fn test_compaction_applies_position_delete_then_old_delete_dangles_harmlessly() {
        let (catalog, _temp) = local_fs_catalog().await;
        let table = create_partitioned_table(&catalog, crate::spec::FormatVersion::V2).await;

        // 5 files in partition x=0; one has 2 rows so a position delete can remove its row 0.
        let mut files = Vec::new();
        let two_row =
            write_data_file(&table, "two-row.parquet", 0, &[(0, 11, 110), (0, 22, 220)]).await;
        let two_row_path = two_row.file_path().to_string();
        files.push(two_row);
        for index in 0..4i64 {
            files.push(
                write_data_file(&table, &format!("one-{index}.parquet"), 0, &[(
                    0,
                    30 + index,
                    300,
                )])
                .await,
            );
        }
        let table = append_files(&catalog, &table, files).await;

        // A position delete removing row 0 (y=11) of two-row.parquet.
        let pos_delete = write_position_delete_file(&table, 0, &[(two_row_path.clone(), 0)]).await;
        let table = add_deletes(&catalog, &table, vec![pos_delete]).await;

        let rows_before = scan_rows(&table).await;
        assert!(
            !rows_before.iter().any(|(_, y, _)| *y == 11),
            "the position delete drops y=11 (row 0) before compaction"
        );

        let result = RewriteDataFiles::new(table.clone())
            .target_file_size_bytes(1_000_000)
            .execute(&catalog)
            .await
            .expect("compaction must succeed");
        assert!(result.rewritten_data_files_count >= 5);

        let table = catalog.load_table(table.identifier()).await.unwrap();
        let rows_after = scan_rows(&table).await;
        assert!(
            !rows_after.iter().any(|(_, y, _)| *y == 11),
            "the rewritten file contains only live rows; the dangling position delete is harmless"
        );
        assert_eq!(
            rows_after, rows_before,
            "row conservation: only the position-deleted row stays gone"
        );
        // The compacted file no longer exists, so the position delete dangles (Java keeps it).
        assert!(
            !live_data_file_paths(&table).await.contains(&two_row_path),
            "the position-deleted file was rewritten away (its delete now dangles)"
        );
    }

    /// CANDIDATE SELECTION — DELETE THRESHOLD (risk: a delete-laden but well-sized file is not
    /// rewritten, so its deletes are never physically applied). With `delete_file_threshold = 1`, a
    /// well-sized file that carries a delete is a candidate (`tooManyDeletes`) and the group qualifies
    /// (`anyMatch(tooManyDeletes)`) even though it is a lone well-sized file.
    #[tokio::test]
    async fn test_delete_threshold_triggers_rewrite_of_well_sized_delete_laden_file() {
        let (catalog, _temp) = local_fs_catalog().await;
        let table = create_partitioned_table(&catalog, crate::spec::FormatVersion::V2).await;

        // A single, reasonably-sized file in partition x=0 with several rows.
        let rows: Vec<(i64, i64, i64)> = (0..50).map(|n| (0, n, n * 10)).collect();
        let data = write_data_file(&table, "laden.parquet", 0, &rows).await;
        let data_path = data.file_path().to_string();
        let data_size = data.file_size_in_bytes();
        let table = append_files(&catalog, &table, vec![data]).await;

        // A position delete removing y=0 (row 0).
        let pos_delete = write_position_delete_file(&table, 0, &[(data_path.clone(), 0)]).await;
        let table = add_deletes(&catalog, &table, vec![pos_delete]).await;

        let rows_before = scan_rows(&table).await;
        assert!(!rows_before.iter().any(|(_, y, _)| *y == 0));

        // Size thresholds make the file WELL-SIZED (within [min,max]); only the delete threshold
        // can select it. delete_file_threshold=1 ⇒ its 1 delete makes it a candidate.
        let result = RewriteDataFiles::new(table.clone())
            .target_file_size_bytes(data_size)
            .min_file_size_bytes(data_size / 2)
            .max_file_size_bytes(data_size * 2)
            .delete_file_threshold(1)
            .execute(&catalog)
            .await
            .expect("compaction must succeed");

        assert_eq!(
            result.rewritten_data_files_count, 1,
            "the delete-laden well-sized file IS rewritten via the delete threshold"
        );
        let table = catalog.load_table(table.identifier()).await.unwrap();
        assert!(
            !live_data_file_paths(&table).await.contains(&data_path),
            "the delete-laden file was rewritten (its delete physically applied)"
        );
        assert_eq!(
            scan_rows(&table).await,
            rows_before,
            "the rewrite physically applied the delete: y=0 stays gone, all else conserved"
        );
    }

    /// CANDIDATE SELECTION — DELETE THRESHOLD NEGATIVE (risk: over-firing — a well-sized file with
    /// FEWER deletes than the threshold must NOT be rewritten). With `delete_file_threshold = 2` and
    /// a file carrying only 1 delete, the file is left alone (it is well-sized and under-threshold).
    #[tokio::test]
    async fn test_delete_threshold_under_count_leaves_well_sized_file_alone() {
        let (catalog, _temp) = local_fs_catalog().await;
        let table = create_partitioned_table(&catalog, crate::spec::FormatVersion::V2).await;

        let rows: Vec<(i64, i64, i64)> = (0..50).map(|n| (0, n, n * 10)).collect();
        let data = write_data_file(&table, "laden.parquet", 0, &rows).await;
        let data_path = data.file_path().to_string();
        let data_size = data.file_size_in_bytes();
        let table = append_files(&catalog, &table, vec![data]).await;

        let pos_delete = write_position_delete_file(&table, 0, &[(data_path.clone(), 0)]).await;
        let table = add_deletes(&catalog, &table, vec![pos_delete]).await;

        // delete_file_threshold = 2, but the file has only 1 delete ⇒ NOT a candidate; well-sized ⇒
        // not a size candidate either. Empty plan.
        let result = RewriteDataFiles::new(table.clone())
            .target_file_size_bytes(data_size)
            .min_file_size_bytes(data_size / 2)
            .max_file_size_bytes(data_size * 2)
            .delete_file_threshold(2)
            .execute(&catalog)
            .await
            .expect("execute must succeed (no-op)");

        assert_eq!(
            result,
            RewriteDataFilesResult::default(),
            "an under-threshold well-sized file is left alone (no-op)"
        );
        let table = catalog.load_table(table.identifier()).await.unwrap();
        assert!(
            live_data_file_paths(&table).await.contains(&data_path),
            "the file is untouched"
        );
    }

    /// PARTITION ISOLATION (risk: two partitions' files packed into one group ⇒ a rewritten file
    /// carrying rows of two partition values = partition corruption). 3 small files in partition x=0
    /// and 3 in partition x=1; compaction must produce per-partition outputs (no cross-partition
    /// group), and every output file's rows belong to a single partition.
    #[tokio::test]
    async fn test_partitions_never_pack_into_one_group() {
        let (catalog, _temp) = local_fs_catalog().await;
        let table = create_partitioned_table(&catalog, crate::spec::FormatVersion::V2).await;

        let mut files = Vec::new();
        for index in 0..3i64 {
            files.push(
                write_data_file(&table, &format!("p0-{index}.parquet"), 0, &[(
                    0, index, index,
                )])
                .await,
            );
            files.push(
                write_data_file(&table, &format!("p1-{index}.parquet"), 1, &[(
                    1, index, index,
                )])
                .await,
            );
        }
        let table = append_files(&catalog, &table, files).await;
        let rows_before = scan_rows(&table).await;

        // min_input_files = 3 so each 3-file partition group qualifies on its own.
        let result = RewriteDataFiles::new(table.clone())
            .target_file_size_bytes(1_000_000)
            .min_input_files(3)
            .execute(&catalog)
            .await
            .expect("compaction must succeed");
        assert_eq!(
            result.rewritten_data_files_count, 6,
            "all 6 files rewritten"
        );

        let table = catalog.load_table(table.identifier()).await.unwrap();
        assert_eq!(
            scan_rows(&table).await,
            rows_before,
            "row conservation across partitions"
        );

        // Every output file must carry exactly ONE partition value (x is identity-partitioned). A
        // cross-partition group would yield a file with a partition tuple that contradicts its rows.
        // Collect the partition value of every live output data file; with 3 files per partition
        // compacted, there must be at least one output file FOR EACH of x=0 and x=1.
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let mut output_partition_values: HashSet<i64> = HashSet::new();
        let mut output_file_count = 0usize;
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                if entry.is_alive() && entry.content_type() == DataContentType::Data {
                    output_file_count += 1;
                    // The partition struct's single field is the x value.
                    match entry.data_file().partition().iter().next() {
                        Some(Some(Literal::Primitive(prim))) => {
                            let value: i64 = format!("{prim:?}")
                                .trim_start_matches("Long(")
                                .trim_end_matches(')')
                                .parse()
                                .expect("partition x value parses");
                            output_partition_values.insert(value);
                        }
                        other => panic!("unexpected partition tuple shape: {other:?}"),
                    }
                }
            }
        }
        assert_eq!(
            output_partition_values,
            HashSet::from([0, 1]),
            "each partition (x=0, x=1) produced its own output file(s), never a mixed group"
        );
        // 6 small files → 2 partitions → 2 output files (one compacted file per partition).
        assert_eq!(
            output_file_count, 2,
            "one compacted output file per partition"
        );
    }

    /// EMPTY-PLAN NO-OP (risk: a no-qualifying-file run committing a spurious snapshot). When no file
    /// qualifies (all well-sized, no deletes), the action returns a zero-count result and the
    /// snapshot count is UNCHANGED (no commit).
    #[tokio::test]
    async fn test_empty_plan_is_a_no_op_with_no_commit() {
        let (catalog, _temp) = local_fs_catalog().await;
        let table = create_partitioned_table(&catalog, crate::spec::FormatVersion::V2).await;

        // One well-sized file in partition x=0.
        let rows: Vec<(i64, i64, i64)> = (0..100).map(|n| (0, n, n)).collect();
        let data = write_data_file(&table, "ok.parquet", 0, &rows).await;
        let data_size = data.file_size_in_bytes();
        let table = append_files(&catalog, &table, vec![data]).await;

        let snapshots_before = table.metadata().snapshots().count();
        let snapshot_id_before = current_snapshot_id(&table);

        let result = RewriteDataFiles::new(table.clone())
            .target_file_size_bytes(data_size)
            .min_file_size_bytes(data_size / 2)
            .max_file_size_bytes(data_size * 2)
            .execute(&catalog)
            .await
            .expect("execute must succeed (no-op)");

        assert_eq!(
            result,
            RewriteDataFilesResult::default(),
            "an empty plan returns a zero-count result"
        );
        let table = catalog.load_table(table.identifier()).await.unwrap();
        assert_eq!(
            table.metadata().snapshots().count(),
            snapshots_before,
            "no snapshot was committed for an empty plan"
        );
        assert_eq!(
            current_snapshot_id(&table),
            snapshot_id_before,
            "the current snapshot is unchanged"
        );
    }

    /// EMPTY-PLAN — FRESH TABLE (risk: planning crashing on a table with no current snapshot). A
    /// table with no data is a clean no-op.
    #[tokio::test]
    async fn test_fresh_table_with_no_snapshot_is_a_no_op() {
        let (catalog, _temp) = local_fs_catalog().await;
        let table = create_partitioned_table(&catalog, crate::spec::FormatVersion::V2).await;
        assert!(
            current_snapshot_id(&table).is_none(),
            "fixture: no snapshot yet"
        );

        let result = RewriteDataFiles::new(table.clone())
            .execute(&catalog)
            .await
            .expect("a fresh table is a clean no-op");
        assert_eq!(result, RewriteDataFilesResult::default());
    }

    /// MIN_INPUT_FILES (risk: rewriting a too-small group ⇒ churn with no benefit). A lone small file
    /// (1 file) below the group minimum is left alone: `enoughInputFiles` needs `size > 1`, and a
    /// single-file group also fails `enoughContent` (`size > 1`) and `tooMuchContent` (it is
    /// undersized, not oversized). So a lone undersized file does NOT get rewritten.
    #[tokio::test]
    async fn test_lone_small_file_below_group_minimum_is_left_alone() {
        let (catalog, _temp) = local_fs_catalog().await;
        let table = create_partitioned_table(&catalog, crate::spec::FormatVersion::V2).await;

        // A single tiny file in partition x=0 — undersized, but a group of ONE.
        let data = write_data_file(&table, "lone.parquet", 0, &[(0, 1, 1)]).await;
        let data_path = data.file_path().to_string();
        let table = append_files(&catalog, &table, vec![data]).await;

        let result = RewriteDataFiles::new(table.clone())
            .target_file_size_bytes(1_000_000)
            .execute(&catalog)
            .await
            .expect("execute must succeed (no-op)");

        assert_eq!(
            result,
            RewriteDataFilesResult::default(),
            "a lone undersized file (group of 1) is left alone (size > 1 required to qualify)"
        );
        let table = catalog.load_table(table.identifier()).await.unwrap();
        assert!(
            live_data_file_paths(&table).await.contains(&data_path),
            "the lone file is untouched"
        );
    }

    /// MIN_INPUT_FILES — BOUNDARY (risk: an off-by-one in `size >= min_input_files`). With
    /// `min_input_files = 3` and exactly 3 undersized files, the group qualifies via
    /// `enoughInputFiles`; with 2 files it does NOT (and inputSize < target ⇒ no `enoughContent`).
    #[tokio::test]
    async fn test_min_input_files_boundary_two_below_three_at() {
        let (catalog, _temp) = local_fs_catalog().await;

        // Case A: 2 files, min_input_files=3 ⇒ NOT enough ⇒ no-op.
        {
            let table = create_partitioned_table(&catalog, crate::spec::FormatVersion::V2).await;
            let files = vec![
                write_data_file(&table, "a.parquet", 0, &[(0, 1, 1)]).await,
                write_data_file(&table, "b.parquet", 0, &[(0, 2, 2)]).await,
            ];
            let table = append_files(&catalog, &table, files).await;
            let result = RewriteDataFiles::new(table.clone())
                .target_file_size_bytes(1_000_000)
                .min_input_files(3)
                .execute(&catalog)
                .await
                .unwrap();
            assert_eq!(
                result,
                RewriteDataFilesResult::default(),
                "2 files < min_input_files 3 and inputSize < target ⇒ no-op"
            );
        }

        // Case B: 3 files, min_input_files=3 ⇒ enough ⇒ rewritten.
        {
            let table = create_partitioned_table(&catalog, crate::spec::FormatVersion::V2).await;
            let files = vec![
                write_data_file(&table, "a.parquet", 0, &[(0, 1, 1)]).await,
                write_data_file(&table, "b.parquet", 0, &[(0, 2, 2)]).await,
                write_data_file(&table, "c.parquet", 0, &[(0, 3, 3)]).await,
            ];
            let table = append_files(&catalog, &table, files).await;
            let result = RewriteDataFiles::new(table.clone())
                .target_file_size_bytes(1_000_000)
                .min_input_files(3)
                .execute(&catalog)
                .await
                .unwrap();
            assert_eq!(
                result.rewritten_data_files_count, 3,
                "3 files == min_input_files 3 ⇒ the group qualifies and is rewritten"
            );
        }
    }

    /// RESULT COUNTS (risk: the result misreporting the work done). The aggregate counts must equal
    /// the actual files rewritten / added / bytes. 6 small files in one group → 1 output: rewritten=6,
    /// added=1, rewritten_bytes = sum of the 6 input sizes.
    #[tokio::test]
    async fn test_result_counts_match_the_actual_rewrite() {
        let (catalog, _temp) = local_fs_catalog().await;
        let table = create_partitioned_table(&catalog, crate::spec::FormatVersion::V2).await;

        let mut files = Vec::new();
        let mut expected_bytes: u64 = 0;
        for index in 0..6i64 {
            let file = write_data_file(&table, &format!("s-{index}.parquet"), 0, &[(
                0, index, index,
            )])
            .await;
            expected_bytes += file.file_size_in_bytes();
            files.push(file);
        }
        let table = append_files(&catalog, &table, files).await;

        let result = RewriteDataFiles::new(table.clone())
            .target_file_size_bytes(10_000_000)
            .execute(&catalog)
            .await
            .expect("compaction must succeed");

        assert_eq!(
            result.rewritten_data_files_count, 6,
            "6 input files rewritten"
        );
        assert_eq!(
            result.rewritten_bytes_count, expected_bytes,
            "rewritten bytes = sum of the input file sizes"
        );
        let table = catalog.load_table(table.identifier()).await.unwrap();
        let added = live_data_file_paths(&table).await.len();
        assert_eq!(
            result.added_data_files_count, added,
            "added count matches the new live file count"
        );
        assert_eq!(result.file_groups.len(), 1, "one group was rewritten");
        assert_eq!(result.file_groups[0].rewritten_data_files_count, 6);
        assert_eq!(result.file_groups[0].rewritten_bytes_count, expected_bytes);
    }

    /// CONCURRENT-SAFETY INHERITANCE (risk: a compaction committing over a concurrent conflicting
    /// delete ⇒ resurrection / lost delete). The group commit goes through `RewriteFiles`'
    /// `validate`, which rejects a concurrent NEW position delete targeting a replaced file. Build
    /// the compaction's tx, land a concurrent position delete, then the commit must FAIL.
    ///
    /// This mirrors the rewrite_files conflict idiom but exercises it through the RewriteDataFiles
    /// action — the action does not bypass the seq-preserving commit's validation.
    #[tokio::test]
    async fn test_concurrent_conflicting_delete_fails_the_compaction_commit() {
        let (catalog, _temp) = local_fs_catalog().await;
        let table = create_partitioned_table(&catalog, crate::spec::FormatVersion::V2).await;

        // 5 small files in partition x=0 (a qualifying group). One file has 2 rows so a concurrent
        // position delete can target it.
        let target =
            write_data_file(&table, "target.parquet", 0, &[(0, 10, 100), (0, 20, 200)]).await;
        let target_path = target.file_path().to_string();
        let mut files = vec![target];
        for index in 0..4i64 {
            files.push(
                write_data_file(&table, &format!("o-{index}.parquet"), 0, &[(
                    0,
                    30 + index,
                    300,
                )])
                .await,
            );
        }
        let table = append_files(&catalog, &table, files).await;

        // Manually reproduce the action's commit path so we can interleave a concurrent commit:
        // build the rewrite tx for the whole group, then land a concurrent position delete, then
        // commit — the validate must reject it. (The action commits each group in its own tx, so a
        // concurrent delete landing between plan and commit hits this same validate.)
        let action = RewriteDataFiles::new(table.clone()).target_file_size_bytes(1_000_000);
        let config = action.resolve_config().unwrap();
        let starting = table.metadata().current_snapshot().unwrap().clone();
        let tasks = action.plan_scan_tasks().await.unwrap();
        let data_files_by_path = action.collect_live_data_files().await.unwrap();
        let groups = plan_file_groups(tasks, &config, table.metadata().default_partition_spec());
        assert_eq!(groups.len(), 1, "fixture: one qualifying group");

        // Build the rewrite tx for the group (read + write new files), but do NOT commit yet.
        let group = &groups[0];
        let mut files_to_delete: Vec<DataFile> = Vec::new();
        for task in group {
            files_to_delete.push(
                data_files_by_path
                    .get(task.data_file_path())
                    .unwrap()
                    .clone(),
            );
        }
        let added = action
            .write_compacted_files(&table, group, config.target_file_size_bytes)
            .await
            .unwrap();
        let transaction = Transaction::new(&table);
        let rewrite = transaction
            .rewrite_files(files_to_delete, added)
            .validate_from_snapshot(starting.snapshot_id())
            .data_sequence_number(starting.sequence_number());
        let transaction = rewrite.apply(transaction).unwrap();

        // Concurrent: a NEW position delete targeting the replaced file lands.
        let pos_delete = write_position_delete_file(&table, 0, &[(target_path.clone(), 1)]).await;
        let _concurrent = add_deletes(&catalog, &table, vec![pos_delete]).await;

        let error = transaction
            .commit(&catalog)
            .await
            .expect_err("a concurrent position delete on a replaced file must fail the commit");
        assert!(
            error
                .message()
                .contains("found new position delete for replaced data file"),
            "unexpected error: {}",
            error.message()
        );
    }

    /// CONCURRENT EQUALITY-DELETE CROWN JEWEL — THE REAL BEHAVIOR (risk: a compaction stamping the
    /// rewritten files with a FRESH seq lets an equality delete added CONCURRENTLY — after the
    /// compaction's starting snapshot was captured but before its commit — stop applying, so the
    /// concurrently-deleted rows RESURRECT). This is the load-bearing role of
    /// `use_starting_sequence_number` (Java `RewriteDataFilesCommitManager`): the rewritten data
    /// keeps the STARTING snapshot's (lower) seq, so an equality delete landed at a HIGHER seq still
    /// applies (`data_seq < delete_seq`). The commit SUCCEEDS (no conflict) because preserving the
    /// seq sets `ignore_equality_deletes = true` in the `RewriteFiles` validate.
    ///
    /// Construction: capture starting snapshot S; plan + write the rewritten files; build the rewrite
    /// tx; THEN commit a concurrent equality delete (seq S+1) removing y=20 (which lives in a
    /// to-be-rewritten file); THEN commit the compaction. Post-compaction scan: y=20 must be GONE.
    ///
    /// MUTATION (run manually): drop the `.data_sequence_number(...)` stamp below (or set
    /// `use_starting_sequence_number(false)`) ⇒ the rewritten files take a fresh seq > S+1 ⇒ the
    /// concurrent equality delete no longer applies ⇒ y=20 RESURRECTS ⇒ this test FAILS.
    #[tokio::test]
    async fn test_concurrent_equality_delete_still_applies_after_compaction() {
        let (catalog, _temp) = local_fs_catalog().await;
        let table = create_partitioned_table(&catalog, crate::spec::FormatVersion::V2).await;

        // 5 small files in partition x=0 (a qualifying group), rows y = 10, 20, 30, 40, 50.
        let mut files = Vec::new();
        for index in 0..5i64 {
            let y = 10 + index * 10;
            files.push(
                write_data_file(&table, &format!("c-{index}.parquet"), 0, &[(0, y, y * 10)]).await,
            );
        }
        let table = append_files(&catalog, &table, files).await;

        // Pre-compaction the table has NO deletes — y=20 is live.
        let rows_before = scan_rows(&table).await;
        assert!(
            rows_before.iter().any(|(_, y, _)| *y == 20),
            "fixture: y=20 is live before the concurrent delete"
        );

        // --- Drive the action's internals to interleave a concurrent commit (the action commits each
        // group in its own tx, so a concurrent delete landing between plan and commit hits exactly
        // this path). Capture the STARTING snapshot S now.
        let action = RewriteDataFiles::new(table.clone()).target_file_size_bytes(1_000_000);
        let config = action.resolve_config().unwrap();
        let starting = table.metadata().current_snapshot().unwrap().clone();
        let tasks = action.plan_scan_tasks().await.unwrap();
        let data_files_by_path = action.collect_live_data_files().await.unwrap();
        let groups = plan_file_groups(tasks, &config, table.metadata().default_partition_spec());
        assert_eq!(groups.len(), 1, "fixture: one qualifying group");

        let group = &groups[0];
        let mut files_to_delete: Vec<DataFile> = Vec::new();
        for task in group {
            files_to_delete.push(
                data_files_by_path
                    .get(task.data_file_path())
                    .unwrap()
                    .clone(),
            );
        }
        // Write the rewritten files reading from the STARTING snapshot (deletes-applied; there are
        // none yet) — the new files carry every live row including y=20.
        let added = action
            .write_compacted_files(&table, group, config.target_file_size_bytes)
            .await
            .unwrap();

        // CONCURRENT (after S captured, before the compaction commits): an equality delete removing
        // y=20 lands at seq S+1.
        let eq_delete = write_equality_delete_file(&table, 0, &[20]).await;
        let concurrent = add_deletes(&catalog, &table, vec![eq_delete]).await;
        assert!(
            concurrent
                .metadata()
                .current_snapshot()
                .unwrap()
                .sequence_number()
                > starting.sequence_number(),
            "the concurrent equality delete is at a strictly higher seq than the starting snapshot"
        );

        // Commit the compaction stamping the STARTING snapshot's seq (use_starting_sequence_number).
        // It commits over the concurrent delete (ignore_equality_deletes ⇒ no conflict).
        let transaction = Transaction::new(&table);
        let rewrite = transaction
            .rewrite_files(files_to_delete, added)
            .validate_from_snapshot(starting.snapshot_id())
            .data_sequence_number(starting.sequence_number());
        let transaction = rewrite.apply(transaction).unwrap();
        transaction
            .commit(&catalog)
            .await
            .expect("the seq-preserving compaction commits over a concurrent equality delete");

        // Post-compaction scan: y=20 is GONE — the concurrent equality delete still applies because
        // the rewritten data kept the starting (lower) seq.
        let table = catalog.load_table(table.identifier()).await.unwrap();
        let rows_after = scan_rows(&table).await;
        assert!(
            !rows_after.iter().any(|(_, y, _)| *y == 20),
            "the concurrently-added equality delete STILL drops y=20 after compaction — no \
             resurrection (the rewritten data kept the starting seq, below the delete's seq)"
        );
        // Every OTHER row is conserved (only y=20 went away).
        let expected: Vec<(i64, i64, i64)> = rows_before
            .into_iter()
            .filter(|(_, y, _)| *y != 20)
            .collect();
        assert_eq!(
            rows_after, expected,
            "exactly y=20 is removed; all other live rows survive the compaction"
        );
    }

    /// VALIDATE-FROM-SNAPSHOT PIN — THROUGH `.execute()` (risk: production `rewrite_group` dropping
    /// `validate_from_snapshot`, so a concurrent NEW position delete on a replaced file slips through
    /// the compaction commit unnoticed ⇒ a lost delete / resurrection). The other conflict test
    /// stages the commit by hand; this one drives the PRODUCTION `.execute()` path: build the action
    /// on the catalog head, land a concurrent position delete targeting a to-be-rewritten file, then
    /// `.execute()` must surface the validation failure (the commit refreshes the base and the
    /// `RewriteFiles` validate, pinned to the starting snapshot, rejects the new delete).
    ///
    /// MUTATION (run manually): in `rewrite_group`, drop the `.validate_from_snapshot(...)` call ⇒
    /// the concurrent position delete is no longer rejected ⇒ `.execute()` SUCCEEDS ⇒ this test FAILS.
    #[tokio::test]
    async fn test_execute_rejects_concurrent_position_delete_on_replaced_file() {
        let (catalog, _temp) = local_fs_catalog().await;
        let table = create_partitioned_table(&catalog, crate::spec::FormatVersion::V2).await;

        // 5 small files in partition x=0 (a qualifying group); the first has 2 rows so a concurrent
        // position delete can target it.
        let target = write_data_file(&table, "vfs-target.parquet", 0, &[
            (0, 10, 100),
            (0, 20, 200),
        ])
        .await;
        let target_path = target.file_path().to_string();
        let mut files = vec![target];
        for index in 0..4i64 {
            files.push(
                write_data_file(&table, &format!("vfs-o-{index}.parquet"), 0, &[(
                    0,
                    30 + index,
                    300,
                )])
                .await,
            );
        }
        let table = append_files(&catalog, &table, files).await;

        // Build the action against the current head, then land a concurrent position delete on a
        // to-be-rewritten file. `.execute()` plans from `table`, writes the rewrite, and on commit
        // refreshes against the catalog head where the concurrent delete now lives — validate rejects.
        let action = RewriteDataFiles::new(table.clone()).target_file_size_bytes(1_000_000);

        let pos_delete = write_position_delete_file(&table, 0, &[(target_path.clone(), 1)]).await;
        let _concurrent = add_deletes(&catalog, &table, vec![pos_delete]).await;

        let error = action
            .execute(&catalog)
            .await
            .expect_err("a concurrent position delete on a replaced file must fail .execute()");
        assert!(
            error
                .message()
                .contains("found new position delete for replaced data file"),
            "unexpected error: {}",
            error.message()
        );
    }

    /// SIZE-THRESHOLD PRECONDITION (risk: a misconfigured threshold silently doing the wrong thing).
    /// `target >= max` is rejected at execute with Java's message (here via a `max < target` override).
    #[tokio::test]
    async fn test_invalid_size_thresholds_rejected() {
        let (catalog, _temp) = local_fs_catalog().await;
        let table = create_partitioned_table(&catalog, crate::spec::FormatVersion::V2).await;
        let data = write_data_file(&table, "a.parquet", 0, &[(0, 1, 1)]).await;
        let table = append_files(&catalog, &table, vec![data]).await;

        // target == max ⇒ rejected (`target < max` required).
        let error = RewriteDataFiles::new(table.clone())
            .target_file_size_bytes(1000)
            .max_file_size_bytes(1000)
            .execute(&catalog)
            .await
            .expect_err("target >= max must be rejected");
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert!(
            error.message().contains("must be < 'max-file-size-bytes'"),
            "unexpected message: {}",
            error.message()
        );

        // min_input_files == 0 ⇒ rejected.
        let error = RewriteDataFiles::new(table.clone())
            .target_file_size_bytes(1_000_000)
            .min_input_files(0)
            .execute(&catalog)
            .await
            .expect_err("min_input_files 0 must be rejected");
        assert!(error.message().contains("'min-input-files' is set to 0"));
    }

    // ----- pure planning-function unit tests (no table needed) -----

    /// Build a minimal [`FileScanTask`] with a given size, partition value (`x`), and delete count,
    /// for the pure planning-function unit tests. Carries the partition + a single-field identity
    /// spec so partition grouping works.
    fn synthetic_task(
        path: &str,
        size: u64,
        part_value: i64,
        delete_count: usize,
        spec: &Arc<crate::spec::PartitionSpec>,
        schema: &crate::spec::SchemaRef,
    ) -> FileScanTask {
        use crate::scan::FileScanTaskDeleteFile;

        let deletes: Vec<FileScanTaskDeleteFile> = (0..delete_count)
            .map(|index| FileScanTaskDeleteFile {
                file_path: format!("{path}.delete-{index}"),
                file_size_in_bytes: 1,
                file_type: DataContentType::PositionDeletes,
                partition_spec_id: 0,
                equality_ids: None,
                file_format: DataFileFormat::Parquet,
                referenced_data_file: None,
                content_offset: None,
                content_size_in_bytes: None,
                record_count: Some(0),
            })
            .collect();
        FileScanTask {
            file_size_in_bytes: size,
            start: 0,
            length: size,
            record_count: Some(1),
            data_file_path: path.to_string(),
            data_file_format: DataFileFormat::Parquet,
            schema: schema.clone(),
            project_field_ids: vec![1, 2, 3],
            predicate: None,
            deletes,
            partition: Some(Struct::from_iter([Some(Literal::long(part_value))])),
            partition_spec: Some(spec.clone()),
            name_mapping: None,
            case_sensitive: false,
        }
    }

    /// The identity(x) spec + schema for the synthetic tasks (spec id 0).
    fn synthetic_spec_and_schema() -> (Arc<crate::spec::PartitionSpec>, crate::spec::SchemaRef) {
        let schema: crate::spec::SchemaRef = Arc::new(three_long_schema());
        let spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(0)
                .add_partition_field("x", "x", Transform::Identity)
                .unwrap()
                .build()
                .unwrap(),
        );
        (spec, schema)
    }

    fn config_for(target: u64, min: u64, max: u64, min_input_files: usize) -> ResolvedConfig {
        ResolvedConfig {
            target_file_size_bytes: target,
            min_file_size_bytes: min,
            max_file_size_bytes: max,
            min_input_files,
            delete_file_threshold: DELETE_FILE_THRESHOLD_DEFAULT,
            max_file_group_size_bytes: 1_000_000,
        }
    }

    /// Bin-packing parity with Java `ListPacker(...).pack` (lookback-1 forward first-fit): items
    /// [3,3,3,3] target 6 ⇒ [[3,3],[3,3]]; [4,3,3] target 6 ⇒ [[4],[3,3]] (4 alone, the two 3s fit
    /// one bin). Pins the FORWARD `pack` order (NOT packEnd, which would reverse). Tests the REAL
    /// `pack_bins` on synthetic tasks.
    #[test]
    fn test_pack_bins_forward_first_fit() {
        let (spec, schema) = synthetic_spec_and_schema();
        let sizes_of = |bins: &[Vec<FileScanTask>]| -> Vec<Vec<u64>> {
            bins.iter()
                .map(|bin| bin.iter().map(|task| task.file_size_in_bytes).collect())
                .collect()
        };

        let tasks: Vec<FileScanTask> = [3u64, 3, 3, 3]
            .iter()
            .enumerate()
            .map(|(index, &size)| synthetic_task(&format!("f{index}"), size, 0, 0, &spec, &schema))
            .collect();
        assert_eq!(sizes_of(&pack_bins(tasks, 6)), vec![vec![3, 3], vec![3, 3]]);

        let tasks: Vec<FileScanTask> = [4u64, 3, 3]
            .iter()
            .enumerate()
            .map(|(index, &size)| synthetic_task(&format!("g{index}"), size, 0, 0, &spec, &schema))
            .collect();
        assert_eq!(sizes_of(&pack_bins(tasks, 6)), vec![vec![4], vec![3, 3]]);

        // A single item over target gets its own bin (Java: a too-big item opens + closes a bin).
        let tasks: Vec<FileScanTask> = [7u64, 2, 2]
            .iter()
            .enumerate()
            .map(|(index, &size)| synthetic_task(&format!("h{index}"), size, 0, 0, &spec, &schema))
            .collect();
        assert_eq!(sizes_of(&pack_bins(tasks, 6)), vec![vec![7], vec![2, 2]]);
    }

    /// The candidate predicate (Java `outsideDesiredFileSizeRange || tooManyDeletes`): undersized OR
    /// oversized OR delete-laden. A well-sized, delete-free file is NOT a candidate.
    #[test]
    fn test_is_candidate_predicate() {
        let (spec, schema) = synthetic_spec_and_schema();
        // target 100, min 75, max 180.
        let mut config = config_for(100, 75, 180, 5);
        config.delete_file_threshold = 2;

        // Well-sized (100) + no deletes ⇒ NOT a candidate.
        let well = synthetic_task("w", 100, 0, 0, &spec, &schema);
        assert!(!is_candidate(&well, &config));
        // Undersized (50 < 75) ⇒ candidate.
        let small = synthetic_task("s", 50, 0, 0, &spec, &schema);
        assert!(is_candidate(&small, &config));
        // Oversized (200 > 180) ⇒ candidate.
        let big = synthetic_task("b", 200, 0, 0, &spec, &schema);
        assert!(is_candidate(&big, &config));
        // Well-sized but 2 deletes (>= threshold 2) ⇒ candidate via tooManyDeletes.
        let laden = synthetic_task("l", 100, 0, 2, &spec, &schema);
        assert!(is_candidate(&laden, &config));
        // Well-sized + 1 delete (< threshold 2) ⇒ NOT a candidate.
        let one_delete = synthetic_task("o", 100, 0, 1, &spec, &schema);
        assert!(!is_candidate(&one_delete, &config));
    }

    /// The group filter (Java `filterFileGroups`): a single-file group of an undersized file does
    /// NOT qualify (`size > 1` required for enoughInputFiles/enoughContent; undersized ⇒ not
    /// tooMuchContent); a 5-file group does (enoughInputFiles); an oversized single file does
    /// (tooMuchContent).
    #[test]
    fn test_group_filter() {
        let (spec, schema) = synthetic_spec_and_schema();
        let config = config_for(100, 75, 180, 5);

        let lone_small = vec![synthetic_task("s", 50, 0, 0, &spec, &schema)];
        assert!(
            !group_qualifies(&lone_small, &config),
            "a lone small file does not qualify"
        );

        let five_small: Vec<FileScanTask> = (0..5)
            .map(|index| synthetic_task(&format!("s{index}"), 50, 0, 0, &spec, &schema))
            .collect();
        assert!(
            group_qualifies(&five_small, &config),
            "5 files qualify via enoughInputFiles"
        );

        let lone_big = vec![synthetic_task("b", 200, 0, 0, &spec, &schema)];
        assert!(
            group_qualifies(&lone_big, &config),
            "an oversized file qualifies via tooMuchContent"
        );

        // Two small files whose sum exceeds target qualify via enoughContent.
        let two_over_target = vec![
            synthetic_task("a", 60, 0, 0, &spec, &schema),
            synthetic_task("b", 60, 0, 0, &spec, &schema),
        ];
        assert!(
            group_qualifies(&two_over_target, &config),
            "2 small files summing > target qualify via enoughContent"
        );
    }

    /// Partition grouping (Java `groupByPartition`): tasks of different partition values never land
    /// in the same group, and a task whose spec id differs from the current default spec is bucketed
    /// as un-partitioned. Tests the REAL `plan_file_groups`.
    #[test]
    fn test_plan_file_groups_partition_isolation_and_incompatible_spec() {
        let (spec, schema) = synthetic_spec_and_schema();
        let config = config_for(100, 75, 180, 2);

        // 2 undersized files in x=0, 2 in x=1 ⇒ two groups, one per partition.
        let tasks = vec![
            synthetic_task("p0a", 10, 0, 0, &spec, &schema),
            synthetic_task("p0b", 10, 0, 0, &spec, &schema),
            synthetic_task("p1a", 10, 1, 0, &spec, &schema),
            synthetic_task("p1b", 10, 1, 0, &spec, &schema),
        ];
        let groups = plan_file_groups(tasks, &config, &spec);
        assert_eq!(groups.len(), 2, "two partitions ⇒ two groups");
        for group in &groups {
            let partitions: HashSet<String> = group
                .iter()
                .map(|task| format!("{:?}", task.partition))
                .collect();
            assert_eq!(
                partitions.len(),
                1,
                "each group holds ONE partition value only"
            );
        }

        // A task carrying a DIFFERENT spec id (not the current default) buckets as un-partitioned
        // (Java: incompatible spec ⇒ emptyStruct). Build an old spec with id 1.
        let old_spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(1)
                .add_partition_field("y", "y", Transform::Identity)
                .unwrap()
                .build()
                .unwrap(),
        );
        // Give the incompatible-spec task the SAME partition struct value as the current-spec task
        // (`[0]`). The current default spec is `spec` (id 0); the incompatible task carries spec id 1.
        // Java keys the incompatible task under the EMPTY struct (not its partition), so the two land
        // in SEPARATE buckets even though their partition structs are byte-identical. This is the
        // mutation-sensitive form: a naive "always key by partition" would CO-GROUP them (same `[0]`).
        let mut incompatible = synthetic_task("old", 10, 0, 0, &old_spec, &schema);
        incompatible.partition = Some(Struct::from_iter([Some(Literal::long(0))]));
        let current_file = synthetic_task("cur", 10, 0, 0, &spec, &schema);
        // config's min_input_files is 2, so a CO-GROUPED 2-file bucket WOULD qualify. Correct
        // bucketing (incompatible → empty struct, current → `[0]`) yields two single-file buckets,
        // neither of which qualifies ⇒ zero groups. The drop-the-spec-check mutation co-groups them
        // into one qualifying 2-file group ⇒ `groups.len() == 1` ⇒ this assertion FAILS.
        let groups = plan_file_groups(vec![incompatible, current_file], &config, &spec);
        assert!(
            groups.is_empty(),
            "an incompatible-spec file and a current-spec file with the SAME partition struct are \
             bucketed SEPARATELY (incompatible ⇒ empty struct), never merged into a qualifying group"
        );
    }
}
