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
    /// Equality deletes keyed by partition spec id, then by partition value.
    eq_deletes_by_partition: HashMap<i32, HashMap<Struct, Vec<Arc<DeleteFileContext>>>>,
    /// Position deletes that name no single data file, keyed the same way.
    pos_deletes_by_partition: HashMap<i32, HashMap<Struct, Vec<Arc<DeleteFileContext>>>>,
    /// Position deletes keyed by the data file path they apply to.
    pos_deletes_by_path: HashMap<String, Vec<Arc<DeleteFileContext>>>,
    // TODO: Deletion Vector support
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

/// The single data file a position delete file applies to, `None` if it is not tied
/// to a single data file.
fn position_delete_target(data_file: &DataFile) -> Option<String> {
    // data files is named directly
    if let Some(path) = data_file.referenced_data_file() {
        return Some(path);
    }

    // lower and upper bound of reserved field are equal, so all rows
    // have the same value
    let lower = data_file
        .lower_bounds()
        .get(&RESERVED_FIELD_ID_DELETE_FILE_PATH)?;
    let upper = data_file
        .upper_bounds()
        .get(&RESERVED_FIELD_ID_DELETE_FILE_PATH)?;
    if lower != upper {
        return None;
    }

    match lower.literal() {
        PrimitiveLiteral::String(path) => Some(path.clone()),
        _ => None,
    }
}

/// Whether a position delete file's sequence number lets it apply to a data file whose
/// own sequence number is `data_file_seq_num`.
fn position_delete_applies(delete_seq_num: Option<i64>, data_file_seq_num: Option<i64>) -> bool {
    data_file_seq_num
        .map(|seq| delete_seq_num >= Some(seq))
        .unwrap_or(true)
}

impl PopulatedDeleteFileIndex {
    /// Creates a new populated delete file index from a list of delete file contexts, which
    /// allows for fast lookup when determining which delete files apply to a given data file.
    ///
    /// 1. The partition information is extracted from each delete file's manifest entry.
    /// 2. If the partition is empty and the delete file is not a positional delete,
    ///    it is added to the `global_equality_deletes` vector
    /// 3. A positional delete that names a single data file is keyed by that path.
    /// 4. Any other delete file is keyed by partition, in the map for its content type.
    fn new(files: Vec<DeleteFileContext>) -> PopulatedDeleteFileIndex {
        let mut eq_deletes_by_partition: HashMap<
            i32,
            HashMap<Struct, Vec<Arc<DeleteFileContext>>>,
        > = HashMap::default();
        let mut pos_deletes_by_partition: HashMap<
            i32,
            HashMap<Struct, Vec<Arc<DeleteFileContext>>>,
        > = HashMap::default();
        let mut pos_deletes_by_path: HashMap<String, Vec<Arc<DeleteFileContext>>> =
            HashMap::default();

        let mut global_equality_deletes: Vec<Arc<DeleteFileContext>> = vec![];

        files.into_iter().for_each(|ctx| {
            let arc_ctx = Arc::new(ctx);

            if arc_ctx.manifest_entry.sequence_number().is_none() {
                tracing::warn!(
                    delete_file = arc_ctx.manifest_entry.data_file().file_path(),
                    status = ?arc_ctx.manifest_entry.status(),
                    "delete file manifest entry has no data sequence number. It will be skipped for data files with a known sequence number"
                );
            }

            let partition = arc_ctx.manifest_entry.data_file().partition();

            // The spec states that "Equality delete files stored with an unpartitioned spec are applied as global deletes".
            if partition.fields().is_empty() {
                // TODO: confirm we're good to skip here if we encounter a pos del
                if arc_ctx.manifest_entry.content_type() != DataContentType::PositionDeletes {
                    global_equality_deletes.push(arc_ctx);
                    return;
                }
            }

            let destination_map = match arc_ctx.manifest_entry.content_type() {
                DataContentType::PositionDeletes => {
                    if let Some(path) = position_delete_target(arc_ctx.manifest_entry.data_file()) {
                        pos_deletes_by_path.entry(path).or_default().push(arc_ctx);
                        return;
                    }
                    &mut pos_deletes_by_partition
                }
                DataContentType::EqualityDeletes => &mut eq_deletes_by_partition,
                _ => unreachable!(),
            };

            destination_map
                .entry(arc_ctx.partition_spec_id)
                .or_default()
                .entry(partition.clone())
                .or_default()
                .push(arc_ctx);
        });

        // A large number of delete files that are attributed globally or partition-scoped
        // _could_ explain slow reads or memory problems, so make a DEBUG trace available.
        tracing::debug!(
            pos_deletes_attributed = pos_deletes_by_path.values().map(Vec::len).sum::<usize>(),
            pos_deletes_target_files = pos_deletes_by_path.len(),
            pos_deletes_partition_scoped = pos_deletes_by_partition
                .values()
                .flat_map(HashMap::values)
                .map(Vec::len)
                .sum::<usize>(),
            eq_deletes_partition_scoped = eq_deletes_by_partition
                .values()
                .flat_map(HashMap::values)
                .map(Vec::len)
                .sum::<usize>(),
            eq_deletes_global = global_equality_deletes.len(),
            "delete file index populated"
        );

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

        if let Some(deletes) = self
            .eq_deletes_by_partition
            .get(&data_file.partition_spec_id)
            .and_then(|by_partition| by_partition.get(data_file.partition()))
        {
            deletes
                .iter()
                // filter that returns true if the provided delete file's sequence number is **greater than** `seq_num`
                .filter(|&delete| {
                    seq_num
                        .map(|seq_num| delete.manifest_entry.sequence_number() > Some(seq_num))
                        .unwrap_or_else(|| true)
                })
                .for_each(|delete| results.push(delete.as_ref().into()));
        }

        if let Some(deletes) = self.pos_deletes_by_path.get(data_file.file_path()) {
            deletes
                .iter()
                .filter(|&delete| {
                    position_delete_applies(delete.manifest_entry.sequence_number(), seq_num)
                })
                .for_each(|delete| results.push(delete.as_ref().into()));
        }

        if let Some(deletes) = self
            .pos_deletes_by_partition
            .get(&data_file.partition_spec_id)
            .and_then(|by_partition| by_partition.get(data_file.partition()))
        {
            deletes
                .iter()
                .filter(|&delete| {
                    position_delete_applies(delete.manifest_entry.sequence_number(), seq_num)
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

    /// A position delete naming `data_file_path` through the optional spec field.
    fn build_pos_delete_referencing(data_file_path: &str) -> DataFile {
        DataFileBuilder::default()
            .file_path(format!("{}-pos-delete.parquet", Uuid::new_v4()))
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::PositionDeletes)
            .record_count(1)
            .referenced_data_file(Some(data_file_path.to_string()))
            .partition(Struct::empty())
            .partition_spec_id(0)
            .file_size_in_bytes(100)
            .build()
            .unwrap()
    }

    /// A position delete pinned by the reserved `file_path` column bounds
    fn build_pos_delete_with_bounds(
        lower: &str,
        upper: &str,
        partition: &Struct,
        spec_id: i32,
    ) -> DataFile {
        DataFileBuilder::default()
            .file_path(format!("{}-pos-delete.parquet", Uuid::new_v4()))
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::PositionDeletes)
            .record_count(1)
            .lower_bounds(HashMap::from([(
                RESERVED_FIELD_ID_DELETE_FILE_PATH,
                Datum::string(lower),
            )]))
            .upper_bounds(HashMap::from([(
                RESERVED_FIELD_ID_DELETE_FILE_PATH,
                Datum::string(upper),
            )]))
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(100)
            .build()
            .unwrap()
    }

    /// Build `PopulatedDeleteFileIndex` from delete file manifest entries
    fn index_of(entries: Vec<ManifestEntry>) -> PopulatedDeleteFileIndex {
        PopulatedDeleteFileIndex::new(
            entries
                .into_iter()
                .map(|entry| {
                    let partition_spec_id = entry.data_file().partition_spec_id;
                    DeleteFileContext {
                        manifest_entry: entry.into(),
                        partition_spec_id,
                    }
                })
                .collect(),
        )
    }

    #[test]
    fn test_position_delete_attributed_by_referenced_data_file() {
        let data_file = build_unpartitioned_data_file();
        let other_data_file = build_unpartitioned_data_file();
        let index = index_of(vec![build_added_manifest_entry(
            2,
            &build_pos_delete_referencing(data_file.file_path()),
        )]);

        assert_eq!(
            index.get_deletes_for_data_file(&data_file, Some(1)).len(),
            1
        );
        assert!(
            index
                .get_deletes_for_data_file(&other_data_file, Some(1))
                .is_empty()
        );
    }

    #[test]
    fn test_position_delete_attributed_by_file_path_bounds() {
        let data_file = build_unpartitioned_data_file();
        let other_data_file = build_unpartitioned_data_file();
        let path = data_file.file_path();
        let index = index_of(vec![build_added_manifest_entry(
            2,
            &build_pos_delete_with_bounds(path, path, &Struct::empty(), 0),
        )]);

        assert_eq!(
            index.get_deletes_for_data_file(&data_file, Some(1)).len(),
            1
        );
        assert!(
            index
                .get_deletes_for_data_file(&other_data_file, Some(1))
                .is_empty()
        );
    }

    #[test]
    fn test_position_delete_with_unequal_bounds_stays_partition_scoped() {
        let data_file = build_unpartitioned_data_file();
        let other_data_file = build_unpartitioned_data_file();
        let index = index_of(vec![build_added_manifest_entry(
            2,
            &build_pos_delete_with_bounds("a-data.parquet", "z-data.parquet", &Struct::empty(), 0),
        )]);

        // The delete spans several data files, so every file in the partition has to
        // consider it.
        assert_eq!(
            index.get_deletes_for_data_file(&data_file, Some(1)).len(),
            1
        );
        assert_eq!(
            index
                .get_deletes_for_data_file(&other_data_file, Some(1))
                .len(),
            1
        );
    }

    #[test]
    fn test_position_delete_without_bounds_stays_partition_scoped() {
        let data_file = build_unpartitioned_data_file();
        let other_data_file = build_unpartitioned_data_file();
        let index = index_of(vec![build_added_manifest_entry(
            2,
            &build_unpartitioned_pos_delete(),
        )]);

        assert_eq!(
            index.get_deletes_for_data_file(&data_file, Some(1)).len(),
            1
        );
        assert_eq!(
            index
                .get_deletes_for_data_file(&other_data_file, Some(1))
                .len(),
            1
        );
    }

    #[test]
    fn test_position_delete_by_path_ignores_partition_spec_id() {
        let partition = Struct::from_iter([Some(Literal::long(100))]);
        let data_file = build_partitioned_data_file(&partition, 1);
        let path = data_file.file_path();

        // different spec_id on pos. delete, but path bound matches => should tie to data file
        let attributed = index_of(vec![build_added_manifest_entry(
            2,
            &build_pos_delete_with_bounds(path, path, &partition, 0),
        )]);
        assert_eq!(
            attributed
                .get_deletes_for_data_file(&data_file, Some(1))
                .len(),
            1
        );

        // No path bounds => goes to partition bucket => spec_id does not match => should not be tied
        // to data file
        let mismatched_spec = index_of(vec![build_added_manifest_entry(
            2,
            &build_partitioned_pos_delete(&partition, 0),
        )]);
        assert!(
            mismatched_spec
                .get_deletes_for_data_file(&data_file, Some(1))
                .is_empty()
        );

        // With same spec_id, it should be tied to data file
        let matching_spec = index_of(vec![build_added_manifest_entry(
            2,
            &build_partitioned_pos_delete(&partition, 1),
        )]);
        assert_eq!(
            matching_spec
                .get_deletes_for_data_file(&data_file, Some(1))
                .len(),
            1
        );
    }

    #[test]
    fn test_position_delete_from_earlier_commit_does_not_apply() {
        let data_file = build_unpartitioned_data_file();
        let path = data_file.file_path();

        let by_path = index_of(vec![build_added_manifest_entry(
            1,
            &build_pos_delete_referencing(path),
        )]);
        assert!(
            by_path
                .get_deletes_for_data_file(&data_file, Some(2))
                .is_empty(),
            "position delete at seq 1 must not apply to a data file at seq 2"
        );
    }

    #[test]
    fn test_position_delete_from_same_commit_applies() {
        let data_file = build_unpartitioned_data_file();
        let path = data_file.file_path();

        let by_path = index_of(vec![build_added_manifest_entry(
            2,
            &build_pos_delete_referencing(path),
        )]);
        assert_eq!(
            by_path.get_deletes_for_data_file(&data_file, Some(2)).len(),
            1,
            "position delete at seq 2 must apply to a data file at seq 2 (same commit)"
        );
    }
}
