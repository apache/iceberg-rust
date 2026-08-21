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

/// Index of delete files
#[derive(Debug, Clone)]
pub(crate) struct DeleteFileIndex {
    state: Arc<RwLock<DeleteFileIndexState>>,
}

#[derive(Debug)]
enum DeleteFileIndexState {
    Populating(Arc<Notify>),
    Populated(PopulatedDeleteFileIndex),
}

#[derive(Debug)]
struct PopulatedDeleteFileIndex {
    global_equality_deletes: Vec<Arc<DeleteFileContext>>,
    eq_deletes_by_partition: HashMap<Struct, Vec<Arc<DeleteFileContext>>>,
    pos_deletes_by_partition: HashMap<Struct, Vec<Arc<DeleteFileContext>>>,
    pos_deletes_by_path: HashMap<String, Vec<Arc<DeleteFileContext>>>,
    // TODO: Deletion Vector support
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

                let populated_delete_file_index = PopulatedDeleteFileIndex::new(delete_files);

                {
                    let mut guard = state.write().unwrap();
                    *guard = DeleteFileIndexState::Populated(populated_delete_file_index);
                }
                notify.notify_waiters();
            }
        });

        (DeleteFileIndex { state }, tx)
    }

    /// Gets all the delete files that apply to the specified data file.
    pub(crate) async fn get_deletes_for_data_file(
        &self,
        data_file: &DataFile,
        seq_num: Option<i64>,
    ) -> Vec<FileScanTaskDeleteFile> {
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
            }
        };

        notified.await;

        let guard = self.state.read().unwrap();
        match guard.deref() {
            DeleteFileIndexState::Populated(index) => {
                index.get_deletes_for_data_file(data_file, seq_num)
            }
            _ => unreachable!("Cannot be any other state than loaded"),
        }
    }
}

impl PopulatedDeleteFileIndex {
    /// Creates a new populated delete file index from a list of delete file contexts, which
    /// allows for fast lookup when determining which delete files apply to a given data file.
    ///
    /// 1. Position deletes that reference a single data file, either through the
    ///    `referenced_data_file` field or through equal `file_path` column bounds,
    ///    are indexed by that data file's path.
    /// 2. All other position deletes are indexed by the partition extracted from
    ///    their manifest entry.
    /// 3. Equality deletes stored with an unpartitioned spec are applied as global
    ///    deletes, per the spec. All other equality deletes are indexed by partition.
    fn new(files: Vec<DeleteFileContext>) -> PopulatedDeleteFileIndex {
        let mut eq_deletes_by_partition: HashMap<Struct, Vec<Arc<DeleteFileContext>>> =
            HashMap::default();
        let mut pos_deletes_by_partition: HashMap<Struct, Vec<Arc<DeleteFileContext>>> =
            HashMap::default();
        let mut pos_deletes_by_path: HashMap<String, Vec<Arc<DeleteFileContext>>> =
            HashMap::default();

        let mut global_equality_deletes: Vec<Arc<DeleteFileContext>> = vec![];

        files.into_iter().for_each(|ctx| {
            let arc_ctx = Arc::new(ctx);

            let partition = arc_ctx.manifest_entry.data_file().partition();

            match arc_ctx.manifest_entry.content_type() {
                DataContentType::PositionDeletes => {
                    if let Some(path) = referenced_data_file(arc_ctx.manifest_entry.data_file()) {
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
        });

        PopulatedDeleteFileIndex {
            global_equality_deletes,
            eq_deletes_by_partition,
            pos_deletes_by_partition,
            pos_deletes_by_path,
        }
    }

    /// Determine all the delete files that apply to the provided `DataFile`.
    fn get_deletes_for_data_file(
        &self,
        data_file: &DataFile,
        seq_num: Option<i64>,
    ) -> Vec<FileScanTaskDeleteFile> {
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

        results
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

        let delete_file_index = PopulatedDeleteFileIndex::new(delete_contexts);

        let data_file = build_unpartitioned_data_file();

        // All deletes apply to sequence 0
        let delete_files_to_apply_for_seq_0 =
            delete_file_index.get_deletes_for_data_file(&data_file, Some(0));
        assert_eq!(delete_files_to_apply_for_seq_0.len(), 4);

        // All deletes apply to sequence 3
        let delete_files_to_apply_for_seq_3 =
            delete_file_index.get_deletes_for_data_file(&data_file, Some(3));
        assert_eq!(delete_files_to_apply_for_seq_3.len(), 4);

        // Last 3 deletes apply to sequence 4
        let delete_files_to_apply_for_seq_4 =
            delete_file_index.get_deletes_for_data_file(&data_file, Some(4));
        let actual_paths_to_apply_for_seq_4: Vec<String> = delete_files_to_apply_for_seq_4
            .into_iter()
            .map(|file| file.file_path)
            .collect();

        assert_eq!(
            actual_paths_to_apply_for_seq_4,
            delete_file_paths[delete_file_paths.len() - 3..]
        );

        // Last 3 deletes apply to sequence 5
        let delete_files_to_apply_for_seq_5 =
            delete_file_index.get_deletes_for_data_file(&data_file, Some(5));
        let actual_paths_to_apply_for_seq_5: Vec<String> = delete_files_to_apply_for_seq_5
            .into_iter()
            .map(|file| file.file_path)
            .collect();
        assert_eq!(
            actual_paths_to_apply_for_seq_5,
            delete_file_paths[delete_file_paths.len() - 3..]
        );

        // Only the last position delete applies to sequence 6
        let delete_files_to_apply_for_seq_6 =
            delete_file_index.get_deletes_for_data_file(&data_file, Some(6));
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

        let delete_files_to_apply_for_partitioned_file =
            delete_file_index.get_deletes_for_data_file(&partitioned_file, Some(0));
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

        let delete_file_index = PopulatedDeleteFileIndex::new(delete_contexts);

        let partitioned_file =
            build_partitioned_data_file(&Struct::from_iter([Some(Literal::long(100))]), spec_id);

        // All deletes apply to sequence 0
        let delete_files_to_apply_for_seq_0 =
            delete_file_index.get_deletes_for_data_file(&partitioned_file, Some(0));
        assert_eq!(delete_files_to_apply_for_seq_0.len(), 4);

        // All deletes apply to sequence 3
        let delete_files_to_apply_for_seq_3 =
            delete_file_index.get_deletes_for_data_file(&partitioned_file, Some(3));
        assert_eq!(delete_files_to_apply_for_seq_3.len(), 4);

        // Last 3 deletes apply to sequence 4
        let delete_files_to_apply_for_seq_4 =
            delete_file_index.get_deletes_for_data_file(&partitioned_file, Some(4));
        let actual_paths_to_apply_for_seq_4: Vec<String> = delete_files_to_apply_for_seq_4
            .into_iter()
            .map(|file| file.file_path)
            .collect();

        assert_eq!(
            actual_paths_to_apply_for_seq_4,
            delete_file_paths[delete_file_paths.len() - 3..]
        );

        // Last 3 deletes apply to sequence 5
        let delete_files_to_apply_for_seq_5 =
            delete_file_index.get_deletes_for_data_file(&partitioned_file, Some(5));
        let actual_paths_to_apply_for_seq_5: Vec<String> = delete_files_to_apply_for_seq_5
            .into_iter()
            .map(|file| file.file_path)
            .collect();
        assert_eq!(
            actual_paths_to_apply_for_seq_5,
            delete_file_paths[delete_file_paths.len() - 3..]
        );

        // Only the last position delete applies to sequence 6
        let delete_files_to_apply_for_seq_6 =
            delete_file_index.get_deletes_for_data_file(&partitioned_file, Some(6));
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
        let delete_files_to_apply_for_different_partition =
            delete_file_index.get_deletes_for_data_file(&partitioned_second_file, Some(0));
        let actual_paths_to_apply_for_different_partition: Vec<String> =
            delete_files_to_apply_for_different_partition
                .into_iter()
                .map(|file| file.file_path)
                .collect();
        assert!(actual_paths_to_apply_for_different_partition.is_empty());

        // Data file with same tuple but different spec ID does not match any delete files
        let partitioned_different_spec = build_partitioned_data_file(&partition_one, 2);
        let delete_files_to_apply_for_different_spec =
            delete_file_index.get_deletes_for_data_file(&partitioned_different_spec, Some(0));
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
        }]);

        let deletes_for_a = index.get_deletes_for_data_file(&data_file_a, Some(0));
        assert_eq!(deletes_for_a.len(), 1);
        assert_eq!(deletes_for_a[0].file_path, pos_delete.file_path());

        // The delete references data file A, so it must not apply to data file B
        // even though B shares A's partition.
        let deletes_for_b = index.get_deletes_for_data_file(&data_file_b, Some(0));
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
        }]);

        assert_eq!(
            index.get_deletes_for_data_file(&data_file_a, Some(0)).len(),
            1
        );
        assert!(
            index
                .get_deletes_for_data_file(&data_file_b, Some(0))
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
        }]);

        assert_eq!(
            index.get_deletes_for_data_file(&data_file_a, Some(0)).len(),
            1
        );
        assert_eq!(
            index.get_deletes_for_data_file(&data_file_b, Some(0)).len(),
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
        }]);

        assert_eq!(
            index.get_deletes_for_data_file(&data_file, Some(0)).len(),
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
        }]);

        // Position deletes apply when the delete's sequence number is greater
        // than or equal to the data file's.
        assert_eq!(
            index.get_deletes_for_data_file(&data_file, Some(4)).len(),
            1
        );
        assert_eq!(
            index.get_deletes_for_data_file(&data_file, Some(5)).len(),
            1
        );
        assert!(
            index
                .get_deletes_for_data_file(&data_file, Some(6))
                .is_empty()
        );
        // Without a sequence number, the delete applies unconditionally.
        assert_eq!(index.get_deletes_for_data_file(&data_file, None).len(), 1);
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
        ]);

        let mut actual_paths: Vec<String> = index
            .get_deletes_for_data_file(&data_file, Some(0))
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
        ]);

        let mut actual_paths: Vec<String> = index
            .get_deletes_for_data_file(&data_file, Some(0))
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
        }]);

        let deletes_for_referenced = index.get_deletes_for_data_file(&data_file, Some(0));
        assert_eq!(deletes_for_referenced.len(), 1);
        assert_eq!(deletes_for_referenced[0].file_path, pos_delete.file_path());

        assert!(
            index
                .get_deletes_for_data_file(&same_partition_neighbor, Some(0))
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
        }]);

        assert_eq!(
            index.get_deletes_for_data_file(&data_file, Some(0)).len(),
            1
        );
        assert_eq!(
            index
                .get_deletes_for_data_file(&other_data_file, Some(0))
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
