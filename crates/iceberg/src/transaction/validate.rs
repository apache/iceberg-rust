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

use std::collections::HashSet;
use std::sync::Arc;

use crate::spec::{ManifestContentType, ManifestFile, Operation, SnapshotRef, TableMetadata};
use crate::table::Table;

pub(crate) trait SnapshotValidator {
    #[allow(dead_code)]
    fn validate(&self, _table: &Table, _snapshot: Option<&SnapshotRef>) {}

    #[allow(dead_code)]
    async fn validation_history(
        &self,
        base: &Table,
        to_snapshot: Option<&SnapshotRef>,
        from_snapshot: Option<&SnapshotRef>,
        matching_operations: HashSet<Operation>,
        manifest_content_type: ManifestContentType,
    ) -> (Vec<ManifestFile>, HashSet<i64>) {
        let mut manifests = vec![];
        let mut new_snapshots = HashSet::new();
        let mut last_snapshot: Option<&SnapshotRef> = None;

        let snapshots = Self::ancestors_between(to_snapshot, from_snapshot, base.metadata());
        for current_snapshot in &snapshots {
            last_snapshot = Some(current_snapshot);

            if matching_operations.contains(&current_snapshot.summary().operation) {
                new_snapshots.insert(current_snapshot.snapshot_id());
                current_snapshot
                    .load_manifest_list(base.file_io(), base.metadata())
                    .await
                    .expect("Failed to load manifest list!")
                    .entries()
                    .iter()
                    .for_each(|manifest| {
                        if manifest.content == manifest_content_type
                            && manifest.added_snapshot_id == current_snapshot.snapshot_id()
                        {
                            manifests.push(manifest.clone());
                        }
                    });
            }
        }

        if last_snapshot.is_some()
            && last_snapshot.unwrap().parent_snapshot_id()
                != from_snapshot.map(|snapshot| snapshot.snapshot_id())
        {
            panic!(
                "Cannot determine history between starting snapshot {} and the last known ancestor {}",
                from_snapshot.map_or_else(
                    || "None".to_string(),
                    |snapshot| snapshot.snapshot_id().to_string()
                ),
                last_snapshot.map_or_else(
                    || "None".to_string(),
                    |snapshot| snapshot.parent_snapshot_id().unwrap().to_string()
                )
            );
        }

        (manifests, new_snapshots)
    }

    fn ancestors_between(
        to_snapshot: Option<&SnapshotRef>,
        from_snapshot: Option<&SnapshotRef>,
        table_metadata: &TableMetadata,
    ) -> Vec<SnapshotRef> {
        let mut snapshots = Vec::new();
        let mut current_snapshot = to_snapshot;
        while let Some(snapshot) = current_snapshot {
            snapshots.push(Arc::clone(snapshot));
            match snapshot.parent_snapshot_id() {
                Some(parent_snapshot_id)
                    if from_snapshot.is_some()
                        && parent_snapshot_id == from_snapshot.unwrap().snapshot_id() =>
                {
                    break;
                }
                Some(parent_snapshot_id) => {
                    current_snapshot = table_metadata.snapshot_by_id(parent_snapshot_id)
                }
                None => break,
            }
        }

        snapshots
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::TableUpdate;
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, ManifestContentType, Operation,
        SnapshotRef, Struct,
    };
    use crate::transaction::tests::{make_v2_minimal_table, make_v2_table};
    use crate::transaction::validate::SnapshotValidator;
    use crate::transaction::{Table, Transaction};

    struct TestValidator {}

    impl SnapshotValidator for TestValidator {}

    async fn make_v2_table_with_updates() -> (Table, Vec<TableUpdate>) {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let mut action = tx.fast_append(None, vec![]).unwrap();

        let data_file_1 = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let data_file_2 = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/2.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        action.add_data_files(vec![data_file_1.clone()]).unwrap();
        let tx = action.apply().await.unwrap();
        let mut action = tx.fast_append(None, vec![]).unwrap();
        action.add_data_files(vec![data_file_2.clone()]).unwrap();
        let tx = action.apply().await.unwrap();

        (table.clone(), tx.updates)
    }

    #[tokio::test]
    async fn test_validation_history() {
        let (table, updates) = make_v2_table_with_updates().await;
        let parent_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            SnapshotRef::new(snapshot.clone())
        } else {
            unreachable!()
        };
        let current_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[2] {
            SnapshotRef::new(snapshot.clone())
        } else {
            unreachable!()
        };

        let test_validator = TestValidator {};

        // specifying from_snapshot, validating up to the from_snapshot
        let (manifests, snapshots) = test_validator
            .validation_history(
                &table,
                Some(&current_snapshot),
                Some(&parent_snapshot),
                HashSet::from([Operation::Append]),
                ManifestContentType::Data,
            )
            .await;

        manifests
            .iter()
            .for_each(|manifest| assert_eq!(manifest.content, ManifestContentType::Data));
        assert_eq!(snapshots.into_iter().collect::<Vec<_>>(), vec![
            current_snapshot.snapshot_id()
        ]);
    }

    #[test]
    fn test_ancestor_between() {
        let table = make_v2_table();
        let current_snapshot = table.metadata().current_snapshot();
        let parent_snapshot_id = current_snapshot.unwrap().parent_snapshot_id().unwrap();
        let parent_snapshot = table.metadata().snapshot_by_id(parent_snapshot_id);

        // not specifying from_snapshot, listing all ancestors
        let all_ancestors =
            TestValidator::ancestors_between(current_snapshot, None, table.metadata());
        assert_eq!(
            vec![
                current_snapshot.unwrap().snapshot_id(),
                current_snapshot.unwrap().parent_snapshot_id().unwrap()
            ],
            all_ancestors
                .iter()
                .map(|snapshot| snapshot.snapshot_id())
                .collect::<Vec<_>>()
        );

        // specifying from_snapshot, listing only 1 snapshot
        let ancestors =
            TestValidator::ancestors_between(current_snapshot, parent_snapshot, table.metadata());
        assert_eq!(
            vec![current_snapshot.unwrap().snapshot_id()],
            ancestors
                .iter()
                .map(|snapshot| snapshot.snapshot_id())
                .collect::<Vec<_>>()
        );
    }
}
