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

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, RwLock};

use futures::StreamExt;
use futures::channel::mpsc::{Sender, channel};
use tokio::sync::Notify;

use crate::metadata_columns::RESERVED_FIELD_ID_DELETE_FILE_PATH;
use crate::runtime::Runtime;
use crate::scan::{DeleteFileContext, FileScanTaskDeleteFile};
use crate::spec::{DataContentType, DataFile, PrimitiveLiteral, Struct};
use crate::{Error, ErrorKind, Result};

/// Index of delete files
#[derive(Debug, Clone)]
pub(crate) struct DeleteFileIndex {
    state: Arc<RwLock<DeleteFileIndexState>>,
}

#[derive(Debug)]
enum DeleteFileIndexState {
    // Arc, not Box: a waiter clones this out and awaits `notified_owned()` after dropping the
    // read lock (a borrowed `notified()` future can't outlive the guard it was created under).
    // If multiple callers arrive while still Populating, each clones its own handle to the same
    // Notify, so one `notify_waiters()` call wakes all of them.
    Populating(Arc<Notify>),
    // Boxed because PopulatedDeleteFileIndex is large relative to the other variants; there is
    // exactly one owner (this enum, behind the RwLock), so this needs heap indirection, not
    // shared ownership.
    Populated(Box<PopulatedDeleteFileIndex>),
    // Boxed for the same reason: Error is large enough to trip the same size check, and is never
    // cloned out of the lock, only read as `&Error` via deref.
    Failed(Box<Error>),
}

#[derive(Debug)]
struct PopulatedDeleteFileIndex {
    global_equality_deletes: Vec<Arc<DeleteFileContext>>,
    eq_deletes_by_partition: HashMap<Struct, Vec<Arc<DeleteFileContext>>>,
    pos_deletes_by_partition: HashMap<Struct, Vec<Arc<DeleteFileContext>>>,
    pos_deletes_by_path: HashMap<String, Vec<Arc<DeleteFileContext>>>,
    // V3 deletion vectors, keyed by the data file they apply to (referenced_data_file). At most
    // one exists per data file per snapshot, and when one applies it supersedes any position
    // delete files for that data file, partition-scoped or path-scoped alike.
    dvs_by_referenced_data_file: HashMap<String, Arc<DeleteFileContext>>,
}

/// Determines the single data file referenced by a position delete file, if any.
///
/// Checks the explicit `referenced_data_file` field first. Falls back to the
/// lower and upper bounds of the delete file's `file_path` column: when both
/// bounds are present and equal, the delete file references exactly one data
/// file. Unequal or missing bounds mean the delete file may reference multiple
/// data files, so no single path can be inferred. A bound that is not a string
/// literal is treated the same way.
fn referenced_data_file(data_file: &DataFile) -> Option<String> {
    if let Some(path) = data_file.referenced_data_file() {
        return Some(path);
    }

    let lower_bound = data_file
        .lower_bounds()
        .get(&RESERVED_FIELD_ID_DELETE_FILE_PATH)?;
    let upper_bound = data_file
        .upper_bounds()
        .get(&RESERVED_FIELD_ID_DELETE_FILE_PATH)?;

    if lower_bound != upper_bound {
        return None;
    }

    match lower_bound.literal() {
        PrimitiveLiteral::String(path) => Some(path.clone()),
        _ => None,
    }
}

/// Rebuilds an owned `Error` from the boxed `Error` cached in `DeleteFileIndexState::Failed`.
/// `Error` isn't `Clone`, so each waiter gets its own copy of the kind and message rather than
/// sharing the original's backtrace and source chain.
fn clone_failed_index_error(err: &Error) -> Error {
    Error::new(err.kind(), err.message().to_string())
}

impl DeleteFileIndex {
    /// create a new `DeleteFileIndex` along with the sender that populates it with delete files
    pub(crate) fn new(runtime: Runtime) -> (DeleteFileIndex, Sender<DeleteFileContext>) {
        // TODO: what should the channel limit be?
        let (tx, rx) = channel(10);
        let notify = Arc::new(Notify::new());
        let state = Arc::new(RwLock::new(DeleteFileIndexState::Populating(
            notify.clone(),
        )));
        let delete_file_stream = rx.boxed();

        runtime.io().spawn({
            let state = state.clone();
            async move {
                let delete_files: Vec<DeleteFileContext> =
                    delete_file_stream.collect::<Vec<_>>().await;

                let new_state = match PopulatedDeleteFileIndex::new(delete_files) {
                    Ok(index) => DeleteFileIndexState::Populated(Box::new(index)),
                    Err(err) => DeleteFileIndexState::Failed(Box::new(err)),
                };

                {
                    let mut guard = state.write().unwrap();
                    *guard = new_state;
                }
                notify.notify_waiters();
            }
        });

        (DeleteFileIndex { state }, tx)
    }

    /// Gets all the delete files that apply to the specified data file.
    ///
    /// Fails if building the index found a spec violation, such as multiple deletion vectors
    /// referencing the same data file, or if a matched deletion vector's sequence number
    /// violates the spec relative to `seq_num`.
    pub(crate) async fn get_deletes_for_data_file(
        &self,
        data_file: &DataFile,
        seq_num: Option<i64>,
    ) -> Result<Vec<FileScanTaskDeleteFile>> {
        // Create the `Notified` while holding the read lock. The read lock ensures that
        // when we go inside it, either the state is already at Populated or it is still
        // at Populating AND `notify_waiters()` has not been called yet. Any `Notified`
        // created before the invocation of `notify_waiters()` will be notified by it
        // even if `await` has not been called on it yet.
        let notified = {
            let guard = self.state.read().unwrap();
            match &*guard {
                DeleteFileIndexState::Populating(notifier) => notifier.clone().notified_owned(),
                DeleteFileIndexState::Populated(index) => {
                    return index.get_deletes_for_data_file(data_file, seq_num);
                }
                DeleteFileIndexState::Failed(err) => {
                    return Err(clone_failed_index_error(err));
                }
            }
        };

        notified.await;

        let guard = self.state.read().unwrap();
        match guard.deref() {
            DeleteFileIndexState::Populated(index) => {
                index.get_deletes_for_data_file(data_file, seq_num)
            }
            DeleteFileIndexState::Failed(err) => Err(clone_failed_index_error(err)),
            DeleteFileIndexState::Populating(_) => {
                unreachable!("Cannot still be Populating after being notified")
            }
        }
    }
}

impl PopulatedDeleteFileIndex {
    /// Creates a new populated delete file index from a list of delete file contexts, which
    /// allows for fast lookup when determining which delete files apply to a given data file.
    ///
    /// 1. A V3 deletion vector (a `PositionDeletes` entry with `content_offset` set) is indexed
    ///    by the `referenced_data_file` field, which the spec requires for deletion vectors.
    ///    Fails if two deletion vectors reference the same data file: the spec allows at most
    ///    one deletion vector per data file per snapshot.
    /// 2. Other position deletes that reference a single data file, either through the
    ///    `referenced_data_file` field or through equal `file_path` column bounds,
    ///    are indexed by that data file's path.
    /// 3. All other position deletes are indexed by the partition extracted from
    ///    their manifest entry.
    /// 4. Equality deletes stored with an unpartitioned spec are applied as global
    ///    deletes, per the spec. All other equality deletes are indexed by partition.
    fn new(files: Vec<DeleteFileContext>) -> Result<PopulatedDeleteFileIndex> {
        let mut eq_deletes_by_partition: HashMap<Struct, Vec<Arc<DeleteFileContext>>> =
            HashMap::default();
        let mut pos_deletes_by_partition: HashMap<Struct, Vec<Arc<DeleteFileContext>>> =
            HashMap::default();
        let mut pos_deletes_by_path: HashMap<String, Vec<Arc<DeleteFileContext>>> =
            HashMap::default();
        let mut dvs_by_referenced_data_file: HashMap<String, Arc<DeleteFileContext>> =
            HashMap::default();

        let mut global_equality_deletes: Vec<Arc<DeleteFileContext>> = vec![];

        for ctx in files {
            let arc_ctx = Arc::new(ctx);

            let data_file = arc_ctx.manifest_entry.data_file();
            let partition = data_file.partition();

            match arc_ctx.manifest_entry.content_type() {
                DataContentType::PositionDeletes => {
                    if data_file.content_offset().is_some() {
                        // The spec requires referenced_data_file whenever content_offset is set
                        // (a deletion vector), so its absence here is a malformed manifest entry,
                        // not an ordinary position delete to fall back on.
                        let Some(path) = data_file.referenced_data_file() else {
                            return Err(Error::new(
                                ErrorKind::DataInvalid,
                                format!(
                                    "deletion vector {} sets content_offset but is missing referenced_data_file",
                                    arc_ctx.manifest_entry.file_path()
                                ),
                            ));
                        };

                        if let Some(existing) =
                            dvs_by_referenced_data_file.insert(path.clone(), arc_ctx)
                        {
                            let inserted = &dvs_by_referenced_data_file[&path];
                            return Err(Error::new(
                                ErrorKind::DataInvalid,
                                format!(
                                    "found multiple deletion vectors for data file {path}: {} and {}",
                                    existing.manifest_entry.file_path(),
                                    inserted.manifest_entry.file_path()
                                ),
                            ));
                        }
                        continue;
                    }

                    if let Some(path) = referenced_data_file(data_file) {
                        pos_deletes_by_path.entry(path).or_default().push(arc_ctx);
                    } else {
                        pos_deletes_by_partition
                            .entry(partition.clone())
                            .or_default()
                            .push(arc_ctx);
                    }
                }
                DataContentType::EqualityDeletes => {
                    // The spec states that "Equality delete files stored with an unpartitioned spec are applied as global deletes".
                    if partition.fields().is_empty() {
                        global_equality_deletes.push(arc_ctx);
                    } else {
                        eq_deletes_by_partition
                            .entry(partition.clone())
                            .or_default()
                            .push(arc_ctx);
                    }
                }
                _ => unreachable!(),
            }
        }

        Ok(PopulatedDeleteFileIndex {
            global_equality_deletes,
            eq_deletes_by_partition,
            pos_deletes_by_partition,
            pos_deletes_by_path,
            dvs_by_referenced_data_file,
        })
    }

    /// Determine all the delete files that apply to the provided `DataFile`.
    ///
    /// Fails if a matched deletion vector's partition or data sequence number is inconsistent
    /// with the data file's: a data file's path is permanently tied to one partition, and the
    /// spec guarantees a DV is only ever written at or after the sequence number of the data
    /// file it applies to, so either violation means the delete manifest is inconsistent, not
    /// that the DV simply doesn't apply.
    fn get_deletes_for_data_file(
        &self,
        data_file: &DataFile,
        seq_num: Option<i64>,
    ) -> Result<Vec<FileScanTaskDeleteFile>> {
        let mut results = vec![];

        self.global_equality_deletes
            .iter()
            // filter that returns true if the provided delete file's sequence number is **greater than** `seq_num`
            .filter(|&delete| {
                seq_num
                    .map(|seq_num| delete.manifest_entry.sequence_number() > Some(seq_num))
                    .unwrap_or_else(|| true)
            })
            .for_each(|delete| results.push(delete.as_ref().into()));

        if let Some(deletes) = self.eq_deletes_by_partition.get(data_file.partition()) {
            deletes
                .iter()
                // filter that returns true if the provided delete file's sequence number is **greater than** `seq_num`
                .filter(|&delete| {
                    seq_num
                        .map(|seq_num| delete.manifest_entry.sequence_number() > Some(seq_num))
                        .unwrap_or_else(|| true)
                        && data_file.partition_spec_id == delete.partition_spec_id
                })
                .for_each(|delete| results.push(delete.as_ref().into()));
        }

        // A deletion vector supersedes all position delete files for its data file, per the spec:
        // "readers ignore any position delete files that would otherwise match it, because the DV
        // subsumes them". An exact path match on referenced_data_file is sufficient proof of
        // applicability, the same as for pos_deletes_by_path below, so this is checked before
        // (and instead of) pos_deletes_by_partition and pos_deletes_by_path.
        if let Some(dv) = self.dvs_by_referenced_data_file.get(data_file.file_path()) {
            let dv_data_file = dv.manifest_entry.data_file();
            // A file path belongs to exactly one partition for its lifetime, so an exact path
            // match already implies partition equality; this checks that the manifest agrees,
            // per the spec's explicit partition-equality condition for deletion vectors.
            if data_file.partition() != dv_data_file.partition()
                || data_file.partition_spec_id != dv.partition_spec_id
            {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "deletion vector {} references data file {} but its partition (spec {}, {:?}) does not match the data file's partition (spec {}, {:?})",
                        dv.manifest_entry.file_path(),
                        data_file.file_path(),
                        dv.partition_spec_id,
                        dv_data_file.partition(),
                        data_file.partition_spec_id,
                        data_file.partition()
                    ),
                ));
            }

            if let Some(seq_num) = seq_num {
                let dv_seq = dv.manifest_entry.sequence_number();
                if dv_seq < Some(seq_num) {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "deletion vector {} has data sequence number {dv_seq:?}, which must be >= the data file's sequence number {seq_num}",
                            dv.manifest_entry.file_path()
                        ),
                    ));
                }
            }
            results.push(dv.as_ref().into());
            return Ok(results);
        }

        if let Some(deletes) = self.pos_deletes_by_partition.get(data_file.partition()) {
            deletes
                .iter()
                // filter that returns true if the provided delete file's sequence number is **greater than or equal to** `seq_num`
                .filter(|&delete| {
                    seq_num
                        .map(|seq_num| delete.manifest_entry.sequence_number() >= Some(seq_num))
                        .unwrap_or_else(|| true)
                        && data_file.partition_spec_id == delete.partition_spec_id
                })
                .for_each(|delete| results.push(delete.as_ref().into()));
        }

        // Position deletes indexed by the exact path of the data file they reference.
        // An exact path match is sufficient proof that the delete applies, so no
        // partition spec id check is performed.
        if let Some(deletes) = self.pos_deletes_by_path.get(data_file.file_path()) {
            deletes
                .iter()
                // filter that returns true if the provided delete file's sequence number is **greater than or equal to** `seq_num`
                .filter(|&delete| {
                    seq_num
                        .map(|seq_num| delete.manifest_entry.sequence_number() >= Some(seq_num))
                        .unwrap_or(true)
                })
                .for_each(|delete| results.push(delete.as_ref().into()));
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Datum, Literal, ManifestEntry,
        ManifestStatus, Struct,
    };

    #[test]
    fn test_delete_file_index_unpartitioned() {
        let deletes: Vec<ManifestEntry> = vec![
            build_added_manifest_entry(4, &build_unpartitioned_eq_delete()),
            build_added_manifest_entry(6, &build_unpartitioned_eq_delete()),
            build_added_manifest_entry(5, &build_unpartitioned_pos_delete()),
            build_added_manifest_entry(6, &build_unpartitioned_pos_delete()),
        ];

        let delete_file_paths: Vec<String> = deletes
            .iter()
            .map(|file| file.file_path().to_string())
            .collect();

        let delete_contexts: Vec<DeleteFileContext> = deletes
            .into_iter()
            .map(|entry| DeleteFileContext {
                manifest_entry: entry.into(),
                partition_spec_id: 0,
            })
            .collect();

        let delete_file_index = PopulatedDeleteFileIndex::new(delete_contexts).unwrap();

        let data_file = build_unpartitioned_data_file();

        // All deletes apply to sequence 0
        let delete_files_to_apply_for_seq_0 = delete_file_index
            .get_deletes_for_data_file(&data_file, Some(0))
            .unwrap();
        assert_eq!(delete_files_to_apply_for_seq_0.len(), 4);

        // All deletes apply to sequence 3
        let delete_files_to_apply_for_seq_3 = delete_file_index
            .get_deletes_for_data_file(&data_file, Some(3))
            .unwrap();
        assert_eq!(delete_files_to_apply_for_seq_3.len(), 4);

        // Last 3 deletes apply to sequence 4
        let delete_files_to_apply_for_seq_4 = delete_file_index
            .get_deletes_for_data_file(&data_file, Some(4))
            .unwrap();
        let actual_paths_to_apply_for_seq_4: Vec<String> = delete_files_to_apply_for_seq_4
            .into_iter()
            .map(|file| file.file_path)
            .collect();

        assert_eq!(
            actual_paths_to_apply_for_seq_4,
            delete_file_paths[delete_file_paths.len() - 3..]
        );

        // Last 3 deletes apply to sequence 5
        let delete_files_to_apply_for_seq_5 = delete_file_index
            .get_deletes_for_data_file(&data_file, Some(5))
            .unwrap();
        let actual_paths_to_apply_for_seq_5: Vec<String> = delete_files_to_apply_for_seq_5
            .into_iter()
            .map(|file| file.file_path)
            .collect();
        assert_eq!(
            actual_paths_to_apply_for_seq_5,
            delete_file_paths[delete_file_paths.len() - 3..]
        );

        // Only the last position delete applies to sequence 6
        let delete_files_to_apply_for_seq_6 = delete_file_index
            .get_deletes_for_data_file(&data_file, Some(6))
            .unwrap();
        let actual_paths_to_apply_for_seq_6: Vec<String> = delete_files_to_apply_for_seq_6
            .into_iter()
            .map(|file| file.file_path)
            .collect();
        assert_eq!(
            actual_paths_to_apply_for_seq_6,
            delete_file_paths[delete_file_paths.len() - 1..]
        );

        // The 2 global equality deletes should match against any partitioned file
        let partitioned_file =
            build_partitioned_data_file(&Struct::from_iter([Some(Literal::long(100))]), 1);

        let delete_files_to_apply_for_partitioned_file = delete_file_index
            .get_deletes_for_data_file(&partitioned_file, Some(0))
            .unwrap();
        let actual_paths_to_apply_for_partitioned_file: Vec<String> =
            delete_files_to_apply_for_partitioned_file
                .into_iter()
                .map(|file| file.file_path)
                .collect();
        assert_eq!(
            actual_paths_to_apply_for_partitioned_file,
            delete_file_paths[..2]
        );
    }

    #[test]
    fn test_delete_file_index_partitioned() {
        let partition_one = Struct::from_iter([Some(Literal::long(100))]);
        let spec_id = 1;
        let deletes: Vec<ManifestEntry> = vec![
            build_added_manifest_entry(4, &build_partitioned_eq_delete(&partition_one, spec_id)),
            build_added_manifest_entry(6, &build_partitioned_eq_delete(&partition_one, spec_id)),
            build_added_manifest_entry(5, &build_partitioned_pos_delete(&partition_one, spec_id)),
            build_added_manifest_entry(6, &build_partitioned_pos_delete(&partition_one, spec_id)),
        ];

        let delete_file_paths: Vec<String> = deletes
            .iter()
            .map(|file| file.file_path().to_string())
            .collect();

        let delete_contexts: Vec<DeleteFileContext> = deletes
            .into_iter()
            .map(|entry| DeleteFileContext {
                manifest_entry: entry.into(),
                partition_spec_id: spec_id,
            })
            .collect();

        let delete_file_index = PopulatedDeleteFileIndex::new(delete_contexts).unwrap();

        let partitioned_file =
            build_partitioned_data_file(&Struct::from_iter([Some(Literal::long(100))]), spec_id);

        // All deletes apply to sequence 0
        let delete_files_to_apply_for_seq_0 = delete_file_index
            .get_deletes_for_data_file(&partitioned_file, Some(0))
            .unwrap();
        assert_eq!(delete_files_to_apply_for_seq_0.len(), 4);

        // All deletes apply to sequence 3
        let delete_files_to_apply_for_seq_3 = delete_file_index
            .get_deletes_for_data_file(&partitioned_file, Some(3))
            .unwrap();
        assert_eq!(delete_files_to_apply_for_seq_3.len(), 4);

        // Last 3 deletes apply to sequence 4
        let delete_files_to_apply_for_seq_4 = delete_file_index
            .get_deletes_for_data_file(&partitioned_file, Some(4))
            .unwrap();
        let actual_paths_to_apply_for_seq_4: Vec<String> = delete_files_to_apply_for_seq_4
            .into_iter()
            .map(|file| file.file_path)
            .collect();

        assert_eq!(
            actual_paths_to_apply_for_seq_4,
            delete_file_paths[delete_file_paths.len() - 3..]
        );

        // Last 3 deletes apply to sequence 5
        let delete_files_to_apply_for_seq_5 = delete_file_index
            .get_deletes_for_data_file(&partitioned_file, Some(5))
            .unwrap();
        let actual_paths_to_apply_for_seq_5: Vec<String> = delete_files_to_apply_for_seq_5
            .into_iter()
            .map(|file| file.file_path)
            .collect();
        assert_eq!(
            actual_paths_to_apply_for_seq_5,
            delete_file_paths[delete_file_paths.len() - 3..]
        );

        // Only the last position delete applies to sequence 6
        let delete_files_to_apply_for_seq_6 = delete_file_index
            .get_deletes_for_data_file(&partitioned_file, Some(6))
            .unwrap();
        let actual_paths_to_apply_for_seq_6: Vec<String> = delete_files_to_apply_for_seq_6
            .into_iter()
            .map(|file| file.file_path)
            .collect();
        assert_eq!(
            actual_paths_to_apply_for_seq_6,
            delete_file_paths[delete_file_paths.len() - 1..]
        );

        // Data file with different partition tuples does not match any delete files
        let partitioned_second_file =
            build_partitioned_data_file(&Struct::from_iter([Some(Literal::long(200))]), 1);
        let delete_files_to_apply_for_different_partition = delete_file_index
            .get_deletes_for_data_file(&partitioned_second_file, Some(0))
            .unwrap();
        let actual_paths_to_apply_for_different_partition: Vec<String> =
            delete_files_to_apply_for_different_partition
                .into_iter()
                .map(|file| file.file_path)
                .collect();
        assert!(actual_paths_to_apply_for_different_partition.is_empty());

        // Data file with same tuple but different spec ID does not match any delete files
        let partitioned_different_spec = build_partitioned_data_file(&partition_one, 2);
        let delete_files_to_apply_for_different_spec = delete_file_index
            .get_deletes_for_data_file(&partitioned_different_spec, Some(0))
            .unwrap();
        let actual_paths_to_apply_for_different_spec: Vec<String> =
            delete_files_to_apply_for_different_spec
                .into_iter()
                .map(|file| file.file_path)
                .collect();
        assert!(actual_paths_to_apply_for_different_spec.is_empty());
    }

    #[test]
    fn test_pos_delete_with_referenced_data_file_applies_only_to_that_file() {
        let data_file_a = build_unpartitioned_data_file();
        let data_file_b = build_unpartitioned_data_file();

        let pos_delete = build_pos_delete_referencing(data_file_a.file_path());
        let index = PopulatedDeleteFileIndex::new(vec![DeleteFileContext {
            manifest_entry: build_added_manifest_entry(5, &pos_delete).into(),
            partition_spec_id: 0,
        }])
        .unwrap();

        let deletes_for_a = index
            .get_deletes_for_data_file(&data_file_a, Some(0))
            .unwrap();
        assert_eq!(deletes_for_a.len(), 1);
        assert_eq!(deletes_for_a[0].file_path, pos_delete.file_path());

        // The delete references data file A, so it must not apply to data file B
        // even though B shares A's partition.
        let deletes_for_b = index
            .get_deletes_for_data_file(&data_file_b, Some(0))
            .unwrap();
        assert!(deletes_for_b.is_empty());
    }

    #[test]
    fn test_pos_delete_with_equal_path_bounds_routes_by_path() {
        let data_file_a = build_unpartitioned_data_file();
        let data_file_b = build_unpartitioned_data_file();

        // No referenced_data_file field; equal file_path column bounds identify
        // the single referenced data file instead.
        let pos_delete =
            build_pos_delete_with_path_bounds(data_file_a.file_path(), data_file_a.file_path());
        let index = PopulatedDeleteFileIndex::new(vec![DeleteFileContext {
            manifest_entry: build_added_manifest_entry(5, &pos_delete).into(),
            partition_spec_id: 0,
        }])
        .unwrap();

        assert_eq!(
            index
                .get_deletes_for_data_file(&data_file_a, Some(0))
                .unwrap()
                .len(),
            1
        );
        assert!(
            index
                .get_deletes_for_data_file(&data_file_b, Some(0))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn test_pos_delete_with_unequal_path_bounds_routes_by_partition() {
        let data_file_a = build_unpartitioned_data_file();
        let data_file_b = build_unpartitioned_data_file();

        // Unequal bounds mean the delete file may reference multiple data files,
        // so it falls back to partition routing and applies to every data file
        // in the partition.
        let pos_delete = build_pos_delete_with_path_bounds("a-data.parquet", "z-data.parquet");
        let index = PopulatedDeleteFileIndex::new(vec![DeleteFileContext {
            manifest_entry: build_added_manifest_entry(5, &pos_delete).into(),
            partition_spec_id: 0,
        }])
        .unwrap();

        assert_eq!(
            index
                .get_deletes_for_data_file(&data_file_a, Some(0))
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            index
                .get_deletes_for_data_file(&data_file_b, Some(0))
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn test_pos_delete_by_path_applies_across_partition_spec_ids() {
        let data_file =
            build_partitioned_data_file(&Struct::from_iter([Some(Literal::long(100))]), 2);

        // The delete is indexed under a different spec id than the data file's.
        // An exact path match is sufficient proof that the delete applies.
        let pos_delete = build_pos_delete_referencing(data_file.file_path());
        let index = PopulatedDeleteFileIndex::new(vec![DeleteFileContext {
            manifest_entry: build_added_manifest_entry(5, &pos_delete).into(),
            partition_spec_id: 0,
        }])
        .unwrap();

        assert_eq!(
            index
                .get_deletes_for_data_file(&data_file, Some(0))
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn test_pos_delete_by_path_sequence_number_filter() {
        let data_file = build_unpartitioned_data_file();

        let pos_delete = build_pos_delete_referencing(data_file.file_path());
        let index = PopulatedDeleteFileIndex::new(vec![DeleteFileContext {
            manifest_entry: build_added_manifest_entry(5, &pos_delete).into(),
            partition_spec_id: 0,
        }])
        .unwrap();

        // Position deletes apply when the delete's sequence number is greater
        // than or equal to the data file's.
        assert_eq!(
            index
                .get_deletes_for_data_file(&data_file, Some(4))
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            index
                .get_deletes_for_data_file(&data_file, Some(5))
                .unwrap()
                .len(),
            1
        );
        assert!(
            index
                .get_deletes_for_data_file(&data_file, Some(6))
                .unwrap()
                .is_empty()
        );
        // Without a sequence number, the delete applies unconditionally.
        assert_eq!(
            index
                .get_deletes_for_data_file(&data_file, None)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn test_path_and_partition_pos_deletes_compose_for_one_data_file() {
        let data_file = build_unpartitioned_data_file();

        // One path-keyed pos delete, one partition-keyed pos delete, and one
        // global equality delete, all applying to the same data file. The
        // result must contain all three.
        let path_delete = build_pos_delete_referencing(data_file.file_path());
        let partition_delete = build_unpartitioned_pos_delete();
        let eq_delete = build_unpartitioned_eq_delete();

        let index = PopulatedDeleteFileIndex::new(vec![
            DeleteFileContext {
                manifest_entry: build_added_manifest_entry(5, &path_delete).into(),
                partition_spec_id: 0,
            },
            DeleteFileContext {
                manifest_entry: build_added_manifest_entry(5, &partition_delete).into(),
                partition_spec_id: 0,
            },
            DeleteFileContext {
                manifest_entry: build_added_manifest_entry(5, &eq_delete).into(),
                partition_spec_id: 0,
            },
        ])
        .unwrap();

        let mut actual_paths: Vec<String> = index
            .get_deletes_for_data_file(&data_file, Some(0))
            .unwrap()
            .into_iter()
            .map(|delete| delete.file_path)
            .collect();
        actual_paths.sort();

        let mut expected_paths = vec![
            path_delete.file_path().to_string(),
            partition_delete.file_path().to_string(),
            eq_delete.file_path().to_string(),
        ];
        expected_paths.sort();

        assert_eq!(actual_paths, expected_paths);
    }

    #[test]
    fn test_multiple_pos_deletes_for_the_same_referenced_path() {
        let data_file = build_unpartitioned_data_file();

        let first_delete = build_pos_delete_referencing(data_file.file_path());
        let second_delete = build_pos_delete_referencing(data_file.file_path());

        let index = PopulatedDeleteFileIndex::new(vec![
            DeleteFileContext {
                manifest_entry: build_added_manifest_entry(5, &first_delete).into(),
                partition_spec_id: 0,
            },
            DeleteFileContext {
                manifest_entry: build_added_manifest_entry(6, &second_delete).into(),
                partition_spec_id: 0,
            },
        ])
        .unwrap();

        let mut actual_paths: Vec<String> = index
            .get_deletes_for_data_file(&data_file, Some(0))
            .unwrap()
            .into_iter()
            .map(|delete| delete.file_path)
            .collect();
        actual_paths.sort();

        let mut expected_paths = vec![
            first_delete.file_path().to_string(),
            second_delete.file_path().to_string(),
        ];
        expected_paths.sort();

        assert_eq!(actual_paths, expected_paths);
    }

    #[test]
    fn test_partitioned_pos_delete_with_referenced_path_routes_by_path() {
        let partition = Struct::from_iter([Some(Literal::long(100))]);
        let other_partition = Struct::from_iter([Some(Literal::long(200))]);

        // The data file lives in a different partition than the delete file.
        let data_file = build_partitioned_data_file(&other_partition, 1);
        let same_partition_neighbor = build_partitioned_data_file(&partition, 1);

        // A partitioned pos delete with a referenced path must route by path:
        // it applies to the referenced file in another partition, and not to
        // a different file in the delete's own partition.
        let mut pos_delete = build_pos_delete_referencing(data_file.file_path());
        pos_delete.partition = partition.clone();
        pos_delete.partition_spec_id = 1;

        let index = PopulatedDeleteFileIndex::new(vec![DeleteFileContext {
            manifest_entry: build_added_manifest_entry(5, &pos_delete).into(),
            partition_spec_id: 1,
        }])
        .unwrap();

        let deletes_for_referenced = index
            .get_deletes_for_data_file(&data_file, Some(0))
            .unwrap();
        assert_eq!(deletes_for_referenced.len(), 1);
        assert_eq!(deletes_for_referenced[0].file_path, pos_delete.file_path());

        assert!(
            index
                .get_deletes_for_data_file(&same_partition_neighbor, Some(0))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn test_pos_delete_with_one_sided_path_bounds_routes_by_partition() {
        let data_file = build_unpartitioned_data_file();
        let other_data_file = build_unpartitioned_data_file();

        // Only a lower bound is present. No single referenced file can be
        // inferred, so the delete falls back to partition routing and applies
        // to every data file in the partition.
        let mut pos_delete = build_pos_delete_with_path_bounds("a-data.parquet", "unused");
        pos_delete.upper_bounds = HashMap::default();

        let index = PopulatedDeleteFileIndex::new(vec![DeleteFileContext {
            manifest_entry: build_added_manifest_entry(5, &pos_delete).into(),
            partition_spec_id: 0,
        }])
        .unwrap();

        assert_eq!(
            index
                .get_deletes_for_data_file(&data_file, Some(0))
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            index
                .get_deletes_for_data_file(&other_data_file, Some(0))
                .unwrap()
                .len(),
            1
        );
    }

    fn build_pos_delete_referencing(path: &str) -> DataFile {
        DataFileBuilder::default()
            .file_path(format!("{}-pos-delete.parquet", Uuid::new_v4()))
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::PositionDeletes)
            .record_count(1)
            .referenced_data_file(Some(path.to_string()))
            .partition(Struct::empty())
            .partition_spec_id(0)
            .file_size_in_bytes(100)
            .build()
            .unwrap()
    }

    fn build_pos_delete_with_path_bounds(lower: &str, upper: &str) -> DataFile {
        DataFileBuilder::default()
            .file_path(format!("{}-pos-delete.parquet", Uuid::new_v4()))
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::PositionDeletes)
            .record_count(1)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .file_size_in_bytes(100)
            .lower_bounds(HashMap::from([(
                RESERVED_FIELD_ID_DELETE_FILE_PATH,
                Datum::string(lower),
            )]))
            .upper_bounds(HashMap::from([(
                RESERVED_FIELD_ID_DELETE_FILE_PATH,
                Datum::string(upper),
            )]))
            .build()
            .unwrap()
    }

    #[test]
    fn test_deletion_vector_context_carries_coordinates() {
        // A deletion vector is a PositionDeletes entry stored as a Puffin blob, located by
        // content_offset / content_size_in_bytes and scoped by referenced_data_file. Those
        // three fields, and the record count its bitmap is validated against, must survive the
        // conversion into a FileScanTaskDeleteFile so the loader can find and apply the blob.
        let dv = DataFileBuilder::default()
            .file_path("s3://bucket/data/part-0.parquet-deletes.puffin".to_string())
            .file_format(DataFileFormat::Puffin)
            .content(DataContentType::PositionDeletes)
            .record_count(3)
            .referenced_data_file(Some("s3://bucket/data/part-0.parquet".to_string()))
            .content_offset(Some(4))
            .content_size_in_bytes(Some(40))
            .partition(Struct::empty())
            .partition_spec_id(0)
            .file_size_in_bytes(44)
            .build()
            .unwrap();

        let ctx = DeleteFileContext {
            manifest_entry: build_added_manifest_entry(5, &dv).into(),
            partition_spec_id: 0,
        };

        let task: FileScanTaskDeleteFile = (&ctx).into();
        assert_eq!(task.file_type, DataContentType::PositionDeletes);
        assert_eq!(task.content_offset, Some(4));
        assert_eq!(task.content_size_in_bytes, Some(40));
        assert_eq!(task.record_count, Some(3));
        assert_eq!(
            task.referenced_data_file.as_deref(),
            Some("s3://bucket/data/part-0.parquet")
        );
    }

    #[test]
    fn test_deletion_vector_supersedes_position_deletes() {
        let partition = Struct::from_iter([Some(Literal::long(100))]);
        let spec_id = 1;
        let data_file = build_partitioned_data_file(&partition, spec_id);

        let dv = build_deletion_vector(data_file.file_path(), &partition, spec_id);
        let dv_path = dv.file_path().to_string();
        let pos_del = build_partitioned_pos_delete(&partition, spec_id);

        let contexts = vec![
            DeleteFileContext {
                manifest_entry: build_added_manifest_entry(5, &dv).into(),
                partition_spec_id: spec_id,
            },
            DeleteFileContext {
                manifest_entry: build_added_manifest_entry(5, &pos_del).into(),
                partition_spec_id: spec_id,
            },
        ];

        let index = PopulatedDeleteFileIndex::new(contexts).unwrap();
        let applied: Vec<String> = index
            .get_deletes_for_data_file(&data_file, Some(0))
            .unwrap()
            .into_iter()
            .map(|f| f.file_path)
            .collect();

        // Only the deletion vector applies; the partition-scoped position delete file, which
        // would otherwise also match, is superseded.
        assert_eq!(applied, vec![dv_path]);
    }

    #[test]
    fn test_deletion_vector_with_stale_sequence_number_is_rejected() {
        let partition = Struct::from_iter([Some(Literal::long(100))]);
        let spec_id = 1;
        let data_file = build_partitioned_data_file(&partition, spec_id);

        let dv = build_deletion_vector(data_file.file_path(), &partition, spec_id);

        let contexts = vec![DeleteFileContext {
            manifest_entry: build_added_manifest_entry(3, &dv).into(),
            partition_spec_id: spec_id,
        }];

        let index = PopulatedDeleteFileIndex::new(contexts).unwrap();
        // The DV's own sequence number (3) is less than the data file's (5): the spec guarantees
        // a DV is never written before the data file it applies to, so this is an inconsistent
        // manifest rather than a case where the DV simply doesn't apply.
        let err = index
            .get_deletes_for_data_file(&data_file, Some(5))
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message()
                .contains("must be >= the data file's sequence number")
        );
    }

    #[test]
    fn test_deletion_vector_with_mismatched_partition_is_rejected() {
        let partition = Struct::from_iter([Some(Literal::long(100))]);
        let other_partition = Struct::from_iter([Some(Literal::long(200))]);
        let spec_id = 1;
        let data_file = build_partitioned_data_file(&partition, spec_id);

        // Malformed: the DV's referenced_data_file matches data_file's path exactly, but the
        // DV's own partition disagrees, a state that cannot arise from a valid writer since a
        // file path is permanently tied to one partition.
        let dv = build_deletion_vector(data_file.file_path(), &other_partition, spec_id);

        let contexts = vec![DeleteFileContext {
            manifest_entry: build_added_manifest_entry(5, &dv).into(),
            partition_spec_id: spec_id,
        }];

        let index = PopulatedDeleteFileIndex::new(contexts).unwrap();
        let err = index
            .get_deletes_for_data_file(&data_file, Some(0))
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message()
                .contains("does not match the data file's partition")
        );
    }

    #[test]
    fn test_deletion_vector_with_mismatched_partition_spec_is_rejected() {
        let partition = Struct::from_iter([Some(Literal::long(100))]);
        let data_file = build_partitioned_data_file(&partition, 1);

        // Same partition value, but the DV's context was populated under a different partition
        // spec id than the data file's: also a manifest inconsistency, since a file path is
        // permanently tied to one partition spec.
        let dv = build_deletion_vector(data_file.file_path(), &partition, 1);

        let contexts = vec![DeleteFileContext {
            manifest_entry: build_added_manifest_entry(5, &dv).into(),
            partition_spec_id: 2,
        }];

        let index = PopulatedDeleteFileIndex::new(contexts).unwrap();
        let err = index
            .get_deletes_for_data_file(&data_file, Some(0))
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message()
                .contains("does not match the data file's partition")
        );
    }

    #[test]
    fn test_deletion_vector_supersedes_path_scoped_position_delete() {
        let partition = Struct::from_iter([Some(Literal::long(100))]);
        let spec_id = 1;
        let data_file = build_partitioned_data_file(&partition, spec_id);

        let dv = build_deletion_vector(data_file.file_path(), &partition, spec_id);
        let dv_path = dv.file_path().to_string();
        let pos_del = build_pos_delete_referencing(data_file.file_path());

        let contexts = vec![
            DeleteFileContext {
                manifest_entry: build_added_manifest_entry(5, &dv).into(),
                partition_spec_id: spec_id,
            },
            DeleteFileContext {
                manifest_entry: build_added_manifest_entry(5, &pos_del).into(),
                partition_spec_id: 0,
            },
        ];

        let index = PopulatedDeleteFileIndex::new(contexts).unwrap();
        let applied: Vec<String> = index
            .get_deletes_for_data_file(&data_file, Some(0))
            .unwrap()
            .into_iter()
            .map(|f| f.file_path)
            .collect();

        // Only the deletion vector applies; the path-scoped position delete file, which would
        // otherwise also match by exact referenced_data_file path, is superseded.
        assert_eq!(applied, vec![dv_path]);
    }

    #[test]
    fn test_deletion_vector_coexists_with_equality_delete() {
        let partition = Struct::from_iter([Some(Literal::long(100))]);
        let spec_id = 1;
        let data_file = build_partitioned_data_file(&partition, spec_id);

        let dv = build_deletion_vector(data_file.file_path(), &partition, spec_id);
        let dv_path = dv.file_path().to_string();
        let eq_del = build_unpartitioned_eq_delete();
        let eq_del_path = eq_del.file_path().to_string();

        let contexts = vec![
            DeleteFileContext {
                manifest_entry: build_added_manifest_entry(5, &dv).into(),
                partition_spec_id: spec_id,
            },
            DeleteFileContext {
                manifest_entry: build_added_manifest_entry(5, &eq_del).into(),
                partition_spec_id: 0,
            },
        ];

        let index = PopulatedDeleteFileIndex::new(contexts).unwrap();
        let mut applied: Vec<String> = index
            .get_deletes_for_data_file(&data_file, Some(0))
            .unwrap()
            .into_iter()
            .map(|f| f.file_path)
            .collect();
        applied.sort();

        // A deletion vector only supersedes position deletes, not equality deletes: both apply.
        let mut expected = vec![dv_path, eq_del_path];
        expected.sort();
        assert_eq!(applied, expected);
    }

    #[test]
    fn test_deletion_vector_missing_referenced_data_file_is_rejected() {
        let malformed_dv = DataFileBuilder::default()
            .file_path("deletes.puffin".to_string())
            .file_format(DataFileFormat::Puffin)
            .content(DataContentType::PositionDeletes)
            .record_count(1)
            .content_offset(Some(4))
            .content_size_in_bytes(Some(40))
            .partition(Struct::empty())
            .partition_spec_id(0)
            .file_size_in_bytes(60)
            .build()
            .unwrap();

        let contexts = vec![DeleteFileContext {
            manifest_entry: build_added_manifest_entry(5, &malformed_dv).into(),
            partition_spec_id: 0,
        }];

        let err = PopulatedDeleteFileIndex::new(contexts).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("missing referenced_data_file"));
    }

    #[test]
    fn test_multiple_deletion_vectors_for_same_data_file_is_rejected() {
        let partition = Struct::from_iter([Some(Literal::long(100))]);
        let spec_id = 1;
        let data_file = build_partitioned_data_file(&partition, spec_id);

        let dv_1 = build_deletion_vector(data_file.file_path(), &partition, spec_id);
        let dv_2 = build_deletion_vector(data_file.file_path(), &partition, spec_id);

        let contexts = vec![
            DeleteFileContext {
                manifest_entry: build_added_manifest_entry(5, &dv_1).into(),
                partition_spec_id: spec_id,
            },
            DeleteFileContext {
                manifest_entry: build_added_manifest_entry(6, &dv_2).into(),
                partition_spec_id: spec_id,
            },
        ];

        let err = PopulatedDeleteFileIndex::new(contexts).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("multiple deletion vectors"));
    }

    #[tokio::test]
    async fn test_delete_file_index_propagates_multiple_dv_error_to_waiters() {
        let partition = Struct::from_iter([Some(Literal::long(100))]);
        let spec_id = 1;
        let data_file = build_partitioned_data_file(&partition, spec_id);

        let dv_1 = build_deletion_vector(data_file.file_path(), &partition, spec_id);
        let dv_2 = build_deletion_vector(data_file.file_path(), &partition, spec_id);

        let (index, mut tx) = DeleteFileIndex::new(Runtime::current());
        tx.try_send(DeleteFileContext {
            manifest_entry: build_added_manifest_entry(5, &dv_1).into(),
            partition_spec_id: spec_id,
        })
        .unwrap();
        tx.try_send(DeleteFileContext {
            manifest_entry: build_added_manifest_entry(6, &dv_2).into(),
            partition_spec_id: spec_id,
        })
        .unwrap();
        drop(tx);

        let err = index
            .get_deletes_for_data_file(&data_file, Some(0))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("multiple deletion vectors"));

        // A second caller, arriving after the index has already settled into the Failed state,
        // must see the same error rather than panicking on an unexpected state.
        let err = index
            .get_deletes_for_data_file(&data_file, Some(0))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    // A V3 deletion vector: a PositionDeletes entry stored as a Puffin blob (content_offset set)
    // scoped to a single data file via referenced_data_file.
    fn build_deletion_vector(
        referenced_data_file: &str,
        partition: &Struct,
        spec_id: i32,
    ) -> DataFile {
        DataFileBuilder::default()
            .file_path(format!("{}-deletes.puffin", Uuid::new_v4()))
            .file_format(DataFileFormat::Puffin)
            .content(DataContentType::PositionDeletes)
            .record_count(1)
            .referenced_data_file(Some(referenced_data_file.to_string()))
            .content_offset(Some(4))
            .content_size_in_bytes(Some(40))
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(60)
            .build()
            .unwrap()
    }

    fn build_unpartitioned_eq_delete() -> DataFile {
        build_partitioned_eq_delete(&Struct::empty(), 0)
    }

    fn build_partitioned_eq_delete(partition: &Struct, spec_id: i32) -> DataFile {
        DataFileBuilder::default()
            .file_path(format!("{}_equality_delete.parquet", Uuid::new_v4()))
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::EqualityDeletes)
            .equality_ids(Some(vec![1]))
            .record_count(1)
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(100)
            .build()
            .unwrap()
    }

    fn build_unpartitioned_pos_delete() -> DataFile {
        build_partitioned_pos_delete(&Struct::empty(), 0)
    }

    fn build_partitioned_pos_delete(partition: &Struct, spec_id: i32) -> DataFile {
        DataFileBuilder::default()
            .file_path(format!("{}-pos-delete.parquet", Uuid::new_v4()))
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::PositionDeletes)
            .record_count(1)
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(100)
            .build()
            .unwrap()
    }

    fn build_unpartitioned_data_file() -> DataFile {
        DataFileBuilder::default()
            .file_path(format!("{}-data.parquet", Uuid::new_v4()))
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::Data)
            .record_count(100)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .file_size_in_bytes(100)
            .build()
            .unwrap()
    }

    fn build_partitioned_data_file(partition_value: &Struct, spec_id: i32) -> DataFile {
        DataFileBuilder::default()
            .file_path(format!("{}-data.parquet", Uuid::new_v4()))
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::Data)
            .record_count(100)
            .partition(partition_value.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(100)
            .build()
            .unwrap()
    }

    fn build_added_manifest_entry(data_seq_number: i64, file: &DataFile) -> ManifestEntry {
        ManifestEntry::builder()
            .status(ManifestStatus::Added)
            .sequence_number(data_seq_number)
            .data_file(file.clone())
            .build()
    }
}
