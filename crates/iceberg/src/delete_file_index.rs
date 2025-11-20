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

use crate::runtime::spawn;
use crate::scan::{DeleteFileContext, FileScanTaskDeleteFile};
use crate::spec::{DataContentType, DataFile, Struct};

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
    #[allow(dead_code)]
    global_equality_deletes: Vec<Arc<DeleteFileContext>>,
    eq_deletes_by_partition: HashMap<Struct, Vec<Arc<DeleteFileContext>>>,
    pos_deletes_by_partition: HashMap<Struct, Vec<Arc<DeleteFileContext>>>,
    // TODO: do we need this?
    // pos_deletes_by_path: HashMap<String, Vec<Arc<DeleteFileContext>>>,

    // TODO: Deletion Vector support
}

impl DeleteFileIndex {
    /// create a new `DeleteFileIndex` along with the sender that populates it with delete files
    pub(crate) fn new() -> (DeleteFileIndex, Sender<DeleteFileContext>) {
        // TODO: what should the channel limit be?
        let (tx, rx) = channel(10);
        let notify = Arc::new(Notify::new());
        let state = Arc::new(RwLock::new(DeleteFileIndexState::Populating(
            notify.clone(),
        )));
        let delete_file_stream = rx.boxed();

        spawn({
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
        let notifier = {
            let guard = self.state.read().unwrap();
            match *guard {
                DeleteFileIndexState::Populating(ref notifier) => notifier.clone(),
                DeleteFileIndexState::Populated(ref index) => {
                    return index.get_deletes_for_data_file(data_file, seq_num);
                }
            }
        };

        notifier.notified().await;

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
    /// 1. The partition information is extracted from each delete file's manifest entry.
    /// 2. If the partition is empty and the delete file is not a positional delete,
    ///    it is added to the `global_equality_deletes` vector
    /// 3. Otherwise, the delete file is added to one of two hash maps based on its content type.
    fn new(files: Vec<DeleteFileContext>) -> PopulatedDeleteFileIndex {
        let mut eq_deletes_by_partition: HashMap<Struct, Vec<Arc<DeleteFileContext>>> =
            HashMap::default();
        let mut pos_deletes_by_partition: HashMap<Struct, Vec<Arc<DeleteFileContext>>> =
            HashMap::default();

        let mut global_equality_deletes: Vec<Arc<DeleteFileContext>> = vec![];

        files.into_iter().for_each(|ctx| {
            let arc_ctx = Arc::new(ctx);

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
                DataContentType::PositionDeletes => &mut pos_deletes_by_partition,
                DataContentType::EqualityDeletes => &mut eq_deletes_by_partition,
                _ => unreachable!(),
            };

            destination_map
                .entry(partition.clone())
                .and_modify(|entry| {
                    entry.push(arc_ctx.clone());
                })
                .or_insert(vec![arc_ctx.clone()]);
        });

        PopulatedDeleteFileIndex {
            global_equality_deletes,
            eq_deletes_by_partition,
            pos_deletes_by_partition,
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

        // Per the Iceberg spec:
        // "A position delete file is indexed by the `referenced_data_file` field of the manifest entry.
        // If the field is present, the delete file applies only to the data file with the same `file_path`.
        // If it's absent, the delete file must be scanned for each data file in the partition."
        if let Some(deletes) = self.pos_deletes_by_partition.get(data_file.partition()) {
            deletes
                .iter()
                // filter that returns true if the provided delete file's sequence number is **greater than or equal to** `seq_num`
                .filter(|&delete| {
                    let sequence_match = seq_num
                        .map(|seq_num| delete.manifest_entry.sequence_number() >= Some(seq_num))
                        .unwrap_or_else(|| true);

                    let spec_match = data_file.partition_spec_id == delete.partition_spec_id;

                    // Check referenced_data_file: if set, it must match the data file's path
                    let referenced_file_match = match delete.manifest_entry.data_file().referenced_data_file() {
                        Some(referenced_path) => referenced_path == data_file.file_path,
                        None => true, // If not set, delete applies to all data files in partition
                    };

                    sequence_match && spec_match && referenced_file_match
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
        DataContentType, DataFileBuilder, DataFileFormat, Literal, ManifestEntry, ManifestStatus,
        Struct,
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
            // No referenced_data_file - applies to all data files in partition
            .referenced_data_file(None)
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

    #[test]
    fn test_referenced_data_file_matching() {
        // Test that position deletes with referenced_data_file set only apply to matching data files
        let partition = Struct::from_iter([Some(Literal::long(100))]);
        let spec_id = 1;

        let data_file_path_1 = "/table/data/file1.parquet";
        let data_file_path_2 = "/table/data/file2.parquet";

        // Create position delete files with specific referenced_data_file values
        let pos_delete_for_file1 = DataFileBuilder::default()
            .file_path(format!("{}-pos-delete.parquet", Uuid::new_v4()))
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::PositionDeletes)
            .record_count(1)
            .referenced_data_file(Some(data_file_path_1.to_string()))
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(100)
            .build()
            .unwrap();

        let pos_delete_for_file2 = DataFileBuilder::default()
            .file_path(format!("{}-pos-delete.parquet", Uuid::new_v4()))
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::PositionDeletes)
            .record_count(1)
            .referenced_data_file(Some(data_file_path_2.to_string()))
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(100)
            .build()
            .unwrap();

        // Create position delete file without referenced_data_file (applies to all files in partition)
        let pos_delete_global_in_partition = DataFileBuilder::default()
            .file_path(format!("{}-pos-delete.parquet", Uuid::new_v4()))
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::PositionDeletes)
            .record_count(1)
            .referenced_data_file(None) // No referenced_data_file means applies to all
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(100)
            .build()
            .unwrap();

        let deletes: Vec<ManifestEntry> = vec![
            build_added_manifest_entry(1, &pos_delete_for_file1),
            build_added_manifest_entry(1, &pos_delete_for_file2),
            build_added_manifest_entry(1, &pos_delete_global_in_partition),
        ];

        let delete_contexts: Vec<DeleteFileContext> = deletes
            .into_iter()
            .map(|entry| DeleteFileContext {
                manifest_entry: entry.into(),
                partition_spec_id: spec_id,
            })
            .collect();

        let delete_file_index = PopulatedDeleteFileIndex::new(delete_contexts);

        // Create data file 1
        let data_file_1 = DataFileBuilder::default()
            .file_path(data_file_path_1.to_string())
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::Data)
            .record_count(100)
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(1000)
            .build()
            .unwrap();

        // Create data file 2
        let data_file_2 = DataFileBuilder::default()
            .file_path(data_file_path_2.to_string())
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::Data)
            .record_count(100)
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(1000)
            .build()
            .unwrap();

        // Test data_file_1: should match pos_delete_for_file1 and pos_delete_global_in_partition
        let deletes_for_file1 = delete_file_index.get_deletes_for_data_file(&data_file_1, Some(0));
        assert_eq!(
            deletes_for_file1.len(),
            2,
            "Data file 1 should have 2 matching delete files"
        );

        let delete_paths_for_file1: Vec<String> = deletes_for_file1
            .into_iter()
            .map(|d| d.file_path)
            .collect();
        assert!(delete_paths_for_file1.contains(&pos_delete_for_file1.file_path));
        assert!(delete_paths_for_file1.contains(&pos_delete_global_in_partition.file_path));
        assert!(!delete_paths_for_file1.contains(&pos_delete_for_file2.file_path));

        // Test data_file_2: should match pos_delete_for_file2 and pos_delete_global_in_partition
        let deletes_for_file2 = delete_file_index.get_deletes_for_data_file(&data_file_2, Some(0));
        assert_eq!(
            deletes_for_file2.len(),
            2,
            "Data file 2 should have 2 matching delete files"
        );

        let delete_paths_for_file2: Vec<String> = deletes_for_file2
            .into_iter()
            .map(|d| d.file_path)
            .collect();
        assert!(delete_paths_for_file2.contains(&pos_delete_for_file2.file_path));
        assert!(delete_paths_for_file2.contains(&pos_delete_global_in_partition.file_path));
        assert!(!delete_paths_for_file2.contains(&pos_delete_for_file1.file_path));
    }

    #[test]
    fn test_referenced_data_file_no_match() {
        // Test that position delete with referenced_data_file doesn't match unrelated data files
        let partition = Struct::from_iter([Some(Literal::long(100))]);
        let spec_id = 1;

        // Create position delete for a specific file
        let pos_delete = DataFileBuilder::default()
            .file_path(format!("{}-pos-delete.parquet", Uuid::new_v4()))
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::PositionDeletes)
            .record_count(1)
            .referenced_data_file(Some("/table/data/specific-file.parquet".to_string()))
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(100)
            .build()
            .unwrap();

        let deletes: Vec<ManifestEntry> = vec![build_added_manifest_entry(1, &pos_delete)];

        let delete_contexts: Vec<DeleteFileContext> = deletes
            .into_iter()
            .map(|entry| DeleteFileContext {
                manifest_entry: entry.into(),
                partition_spec_id: spec_id,
            })
            .collect();

        let delete_file_index = PopulatedDeleteFileIndex::new(delete_contexts);

        // Create data file with different path
        let data_file = DataFileBuilder::default()
            .file_path("/table/data/different-file.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::Data)
            .record_count(100)
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(1000)
            .build()
            .unwrap();

        // The delete file should NOT match this data file
        let deletes = delete_file_index.get_deletes_for_data_file(&data_file, Some(0));
        assert_eq!(
            deletes.len(),
            0,
            "Position delete with different referenced_data_file should not match"
        );
    }

    #[test]
    fn test_referenced_data_file_null_matches_all() {
        // Test that position delete without referenced_data_file matches all files in partition
        let partition = Struct::from_iter([Some(Literal::long(100))]);
        let spec_id = 1;

        // Create position delete without referenced_data_file
        let pos_delete = DataFileBuilder::default()
            .file_path(format!("{}-pos-delete.parquet", Uuid::new_v4()))
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::PositionDeletes)
            .record_count(1)
            .referenced_data_file(None) // Applies to all files in partition
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(100)
            .build()
            .unwrap();

        let deletes: Vec<ManifestEntry> = vec![build_added_manifest_entry(1, &pos_delete)];

        let delete_contexts: Vec<DeleteFileContext> = deletes
            .into_iter()
            .map(|entry| DeleteFileContext {
                manifest_entry: entry.into(),
                partition_spec_id: spec_id,
            })
            .collect();

        let delete_file_index = PopulatedDeleteFileIndex::new(delete_contexts);

        // Create multiple data files with different paths
        let data_files = vec![
            DataFileBuilder::default()
                .file_path("/table/data/file1.parquet".to_string())
                .file_format(DataFileFormat::Parquet)
                .content(DataContentType::Data)
                .record_count(100)
                .partition(partition.clone())
                .partition_spec_id(spec_id)
                .file_size_in_bytes(1000)
                .build()
                .unwrap(),
            DataFileBuilder::default()
                .file_path("/table/data/file2.parquet".to_string())
                .file_format(DataFileFormat::Parquet)
                .content(DataContentType::Data)
                .record_count(100)
                .partition(partition.clone())
                .partition_spec_id(spec_id)
                .file_size_in_bytes(1000)
                .build()
                .unwrap(),
            DataFileBuilder::default()
                .file_path("/table/data/file3.parquet".to_string())
                .file_format(DataFileFormat::Parquet)
                .content(DataContentType::Data)
                .record_count(100)
                .partition(partition.clone())
                .partition_spec_id(spec_id)
                .file_size_in_bytes(1000)
                .build()
                .unwrap(),
        ];

        // All data files should match the delete file
        for data_file in data_files {
            let deletes = delete_file_index.get_deletes_for_data_file(&data_file, Some(0));
            assert_eq!(
                deletes.len(),
                1,
                "Position delete without referenced_data_file should match all files in partition"
            );
            assert_eq!(deletes[0].file_path, pos_delete.file_path);
        }
    }
}
