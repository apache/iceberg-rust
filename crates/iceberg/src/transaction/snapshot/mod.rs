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

//! Snapshot production for transaction actions.
//!
//! Producers (see [`simple`]) own retry-persistent state: identity, reusable
//! artifacts, and the action's intent. The functions in this module are the
//! stateless building blocks they share: they translate explicit inputs into
//! Iceberg metadata and files, never select operation policy, and never cache
//! across calls.

pub(crate) mod simple;

use std::collections::{HashMap, HashSet};

use futures::TryStreamExt;
use futures::stream::FuturesUnordered;
use uuid::Uuid;

use crate::error::{Result, invalid_data};
use crate::spec::{
    DataFile, DataFileFormat, FormatVersion, MAIN_BRANCH, ManifestContentType, ManifestFile,
    ManifestListWriter, ManifestWriter, ManifestWriterBuilder, Operation, PartitionSpecRef,
    SchemaRef, Snapshot, SnapshotReference, SnapshotRetention, SnapshotSummaryCollector, Struct,
    StructType, Summary, TableProperties, set_iceberg_version, update_snapshot_summaries,
};
use crate::table::Table;
use crate::transaction::ActionCommit;
use crate::{Error, ErrorKind, TableRequirement, TableUpdate};

/// Generates a positive snapshot id not already present in `table`.
pub(crate) fn generate_snapshot_id(table: &Table) -> i64 {
    loop {
        let (lhs, rhs) = Uuid::new_v4().as_u64_pair();
        let snapshot_id = (lhs ^ rhs) as i64 & i64::MAX;
        if table.metadata().snapshot_by_id(snapshot_id).is_none() {
            return snapshot_id;
        }
    }
}

/// Returns the metadata path for a producer-owned manifest.
///
/// `counter` must be unique per `commit_uuid` for the lifetime of the producer,
/// so that no attempt overwrites a manifest written by an earlier one.
pub(crate) fn manifest_path(table: &Table, commit_uuid: Uuid, counter: u64) -> Result<String> {
    Ok(format!(
        "{}/{}-m{}.{}",
        table.metadata().metadata_location()?,
        commit_uuid,
        counter,
        DataFileFormat::Avro
    ))
}

/// Returns the metadata path for one attempt's manifest list.
///
/// Each attempt gets a distinct `attempt` number so the path is write-once.
pub(crate) fn manifest_list_path(
    table: &Table,
    snapshot_id: i64,
    attempt: u64,
    commit_uuid: Uuid,
) -> Result<String> {
    Ok(format!(
        "{}/snap-{}-{}-{}.{}",
        table.metadata().metadata_location()?,
        snapshot_id,
        attempt,
        commit_uuid,
        DataFileFormat::Avro
    ))
}

/// Creates a manifest writer at `path`, honoring the table's format version and
/// encryption configuration, under the given schema and partition spec.
pub(crate) fn new_manifest_writer(
    table: &Table,
    path: String,
    snapshot_id: i64,
    content: ManifestContentType,
    schema: SchemaRef,
    partition_spec: PartitionSpecRef,
) -> Result<ManifestWriter> {
    let output_file = table.file_io().new_output(path)?;
    let partition_spec = partition_spec.as_ref().clone();

    let builder = if let Some(em) = table.encryption_manager() {
        ManifestWriterBuilder::new_from_encrypted(
            em.encrypt(output_file),
            Some(snapshot_id),
            schema,
            partition_spec,
        )?
    } else {
        ManifestWriterBuilder::new(output_file, Some(snapshot_id), schema, partition_spec)
    };

    match table.metadata().format_version() {
        FormatVersion::V1 => Ok(builder.build_v1()),
        FormatVersion::V2 => match content {
            ManifestContentType::Data => Ok(builder.build_v2_data()),
            ManifestContentType::Deletes => Ok(builder.build_v2_deletes()),
        },
        FormatVersion::V3 => match content {
            ManifestContentType::Data => Ok(builder.build_v3_data()),
            ManifestContentType::Deletes => Ok(builder.build_v3_deletes()),
        },
    }
}

/// Checks that a partition value is compatible with the partition type.
pub(crate) fn validate_partition_value(
    partition_value: &Struct,
    partition_type: &StructType,
) -> Result<()> {
    if partition_value.fields().len() != partition_type.fields().len() {
        return Err(invalid_data!(
            "Partition value is not compatible with partition type"
        ));
    }

    for (value, field) in partition_value.fields().iter().zip(partition_type.fields()) {
        let field = field.field_type.as_primitive_type().ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "Partition field should only be primitive type.",
            )
        })?;
        if let Some(value) = value
            && !field.compatible(&value.as_primitive_literal().unwrap())
        {
            return Err(invalid_data!(
                "Partition value is not compatible partition type"
            ));
        }
    }
    Ok(())
}

/// Returns the current snapshot's manifests that a new snapshot carries forward.
///
/// Delete-only manifests are kept too: they record which files were removed and
/// must persist across snapshots until `expire_snapshots` cleans them up.
/// Dropping them lets the removed files reappear as live data (see #2148).
pub(crate) async fn current_manifests(table: &Table) -> Result<Vec<ManifestFile>> {
    let Some(snapshot) = table.metadata().current_snapshot() else {
        return Ok(vec![]);
    };

    let manifest_list = table.manifest_list_reader(snapshot).load().await?;

    Ok(manifest_list
        .consume_entries()
        .into_iter()
        .filter(|entry| {
            entry.has_added_files() || entry.has_existing_files() || entry.has_deleted_files()
        })
        .collect())
}

/// Fails if any of `data_files` is already live in the table's current snapshot.
///
/// Must run against each attempt's base, since a concurrent commit may have
/// added one of these files. Manifest reads go through the table's object
/// cache, so manifests unchanged by a rebase are not re-read.
pub(crate) async fn validate_files_not_live(table: &Table, data_files: &[DataFile]) -> Result<()> {
    let Some(current_snapshot) = table.metadata().current_snapshot() else {
        return Ok(());
    };

    let new_files: HashSet<&str> = data_files.iter().map(|df| df.file_path()).collect();
    if new_files.is_empty() {
        return Ok(());
    }

    let manifest_list = table.manifest_list_reader(current_snapshot).load().await?;
    let runtime = table.runtime();
    let object_cache = table.object_cache();

    let new_files_ref = &new_files;
    let referenced_files: Vec<String> = manifest_list
        .consume_entries()
        .into_iter()
        .map(|entry| {
            let object_cache = object_cache.clone();
            runtime
                .io()
                .spawn(async move { object_cache.get_manifest(&entry).await })
        })
        .collect::<FuturesUnordered<_>>()
        .try_fold(Vec::new(), |mut acc, manifest| async move {
            acc.extend(
                manifest?
                    .entries()
                    .iter()
                    .filter(|e| new_files_ref.contains(e.file_path()) && e.is_alive())
                    .map(|e| e.file_path().to_string()),
            );
            Ok(acc)
        })
        .await?;

    if !referenced_files.is_empty() {
        return Err(invalid_data!(
            "Cannot add files that are already referenced by table, files: {}",
            referenced_files.join(", ")
        ));
    }

    Ok(())
}

/// Builds a snapshot summary from an explicit set of file changes.
///
/// User-supplied `properties` are applied first, then the computed metrics
/// overwrite any colliding keys. This matches iceberg-java
/// (`SnapshotProducer.summary`), where computed `added-*`/`total-*` values are
/// written after user properties so a user cannot shadow them with a bad (or
/// merely wrong) value that would corrupt the snapshot summary.
fn build_summary(
    table: &Table,
    operation: Operation,
    mut properties: HashMap<String, String>,
    added_files: &[DataFile],
    removed_files: &[DataFile],
    truncate_full_table: bool,
) -> Result<Summary> {
    let metadata = table.metadata();
    let mut collector = SnapshotSummaryCollector::default();

    collector.set_partition_summary_limit(
        metadata
            .properties()
            .get(TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT)
            .and_then(|limit| limit.parse().ok())
            .unwrap_or(TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT),
    );

    for data_file in added_files {
        collector.add_file(
            data_file,
            metadata.current_schema().clone(),
            metadata.default_partition_spec().clone(),
        );
    }

    let mut schemas_by_spec = HashMap::new();
    for data_file in removed_files {
        let partition_spec = metadata
            .partition_spec_by_id(data_file.partition_spec_id)
            .ok_or_else(|| {
                invalid_data!("Unknown partition spec id {}", data_file.partition_spec_id)
            })?;
        // A dropped source column keeps its type, such as a decimal's scale, only in an
        // older schema.
        let schema = schemas_by_spec
            .entry(data_file.partition_spec_id)
            .or_insert_with(|| {
                metadata
                    .schemas_iter()
                    .filter(|schema| {
                        let mut source_ids = partition_spec.fields().iter().map(|f| f.source_id);
                        source_ids.all(|id| schema.field_by_id(id).is_some())
                    })
                    .max_by_key(|schema| schema.schema_id())
                    .unwrap_or(metadata.current_schema())
                    .clone()
            });
        collector.remove_file(data_file, schema.clone(), partition_spec.clone());
    }

    properties.extend(collector.build());

    let mut summary = update_snapshot_summaries(
        Summary {
            operation,
            additional_properties: properties,
        },
        metadata.current_snapshot().map(|s| s.summary()),
        truncate_full_table,
    )?;
    // Recorded last so it also overrides a user-supplied `iceberg-version`.
    set_iceberg_version(&mut summary.additional_properties);
    Ok(summary)
}

/// Everything one attempt needs to turn a resulting manifest set into a snapshot.
///
/// The producer resolves identity (`snapshot_id`, `manifest_list_path`) and the
/// manifest set; the attempt-sensitive values (parent, sequence number, row-id
/// allocation) are read from `table` by [`write_snapshot_commit`].
pub(crate) struct SnapshotCommitInput<'a> {
    pub snapshot_id: i64,
    pub manifest_list_path: String,
    pub operation: Operation,
    pub properties: HashMap<String, String>,
    pub added_files: &'a [DataFile],
    /// Must match the data files marked Deleted by this commit, including their metrics.
    pub removed_files: &'a [DataFile],
    pub manifests: Vec<ManifestFile>,
    /// Whether the summary resets table totals (a full-table overwrite).
    pub truncate_full_table: bool,
}

/// Writes the attempt's manifest list, assembles the snapshot, and returns the
/// resulting updates and requirements as an [`ActionCommit`].
pub(crate) async fn write_snapshot_commit(
    table: &Table,
    input: SnapshotCommitInput<'_>,
) -> Result<ActionCommit> {
    let SnapshotCommitInput {
        snapshot_id,
        manifest_list_path,
        operation,
        properties,
        added_files,
        removed_files,
        manifests,
        truncate_full_table,
    } = input;

    debug_assert_eq!(
        manifests
            .iter()
            .filter(|m| m.added_snapshot_id == snapshot_id && m.content == ManifestContentType::Data)
            .filter_map(|m| m.deleted_files_count)
            .map(u64::from)
            .sum::<u64>(),
        removed_files.len() as u64,
        "deleted files must match this commit's manifest entries"
    );

    let metadata = table.metadata();
    let parent_snapshot_id = metadata.current_snapshot_id();
    let next_seq_num = metadata.next_sequence_number();
    let first_row_id = metadata.next_row_id();

    let summary = build_summary(
        table,
        operation,
        properties,
        added_files,
        removed_files,
        truncate_full_table,
    )
    .map_err(|err| {
        Error::new(ErrorKind::Unexpected, "Failed to create snapshot summary.").with_source(err)
    })?;

    let raw_output = table.file_io().new_output(manifest_list_path.clone())?;
    let (writer, encrypted_output) = match table.encryption_manager() {
        Some(em) => {
            let encrypted_output = em.encrypt(raw_output);
            (
                encrypted_output.writer().await?,
                Some((em.clone(), encrypted_output)),
            )
        }
        None => (raw_output.writer().await?, None),
    };

    let mut manifest_list_writer = match metadata.format_version() {
        FormatVersion::V1 => ManifestListWriter::v1(writer, snapshot_id, parent_snapshot_id),
        FormatVersion::V2 => {
            ManifestListWriter::v2(writer, snapshot_id, parent_snapshot_id, next_seq_num)
        }
        FormatVersion::V3 => ManifestListWriter::v3(
            writer,
            snapshot_id,
            parent_snapshot_id,
            next_seq_num,
            Some(first_row_id),
        ),
    };

    manifest_list_writer.add_manifests(manifests.into_iter())?;
    let writer_next_row_id = manifest_list_writer.next_row_id();
    let file_metadata = manifest_list_writer.close().await?;

    let encryption_key_id = match encrypted_output {
        Some((em, encrypted_output)) => Some(
            em.encrypt_manifest_list_key_metadata(
                &encrypted_output.key_metadata_with_saved_file_metadata(&file_metadata),
            )
            .await?,
        ),
        None => None,
    };

    let snapshot = Snapshot::builder()
        .with_manifest_list(manifest_list_path)
        .with_snapshot_id(snapshot_id)
        .with_parent_snapshot_id(parent_snapshot_id)
        .with_sequence_number(next_seq_num)
        .with_summary(summary)
        .with_schema_id(metadata.current_schema_id())
        .with_encryption_key_id(encryption_key_id)
        .with_timestamp_ms(chrono::Utc::now().timestamp_millis());

    let snapshot = match writer_next_row_id {
        Some(next_row_id) => snapshot
            .with_row_range(first_row_id, next_row_id - first_row_id)
            .build(),
        None => snapshot.build(),
    };

    let mut updates: Vec<TableUpdate> = table
        .encryption_manager()
        .map(|em| {
            em.with_encryption_keys(|keys| {
                keys.values()
                    .filter(|k| metadata.encryption_key(k.key_id()).is_none())
                    .map(|k| TableUpdate::AddEncryptionKey {
                        encryption_key: k.clone(),
                    })
                    .collect()
            })
        })
        .unwrap_or_default();

    updates.extend([
        TableUpdate::AddSnapshot { snapshot },
        TableUpdate::SetSnapshotRef {
            ref_name: MAIN_BRANCH.to_string(),
            reference: SnapshotReference::new(
                snapshot_id,
                SnapshotRetention::branch(None, None, None),
            ),
        },
    ]);

    let requirements = vec![
        TableRequirement::UuidMatch {
            uuid: metadata.uuid(),
        },
        TableRequirement::RefSnapshotIdMatch {
            r#ref: MAIN_BRANCH.to_string(),
            snapshot_id: parent_snapshot_id,
        },
    ];

    Ok(ActionCommit::new(updates, requirements))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use uuid::Uuid;

    use super::simple::{AppendIntent, SimpleSnapshotProducer};
    use super::*;
    use crate::spec::{
        DataContentType, DataFileBuilder, Literal, Manifest, NestedField, PartitionSpec,
        PrimitiveType, Schema, SnapshotRef, Transform, Type,
    };
    use crate::transaction::tests::make_v2_minimal_table;
    use crate::{ErrorKind, TableUpdate};

    fn data_file(table: &Table, path: &str, record_count: u64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(record_count)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap()
    }

    fn deleted_file(table: &Table) -> DataFile {
        data_file(table, "test/deleted.parquet", 2)
    }

    /// Commits `data_files` to `table` through a fresh producer, returning the
    /// table with that snapshot as current.
    async fn append_to(table: &Table, data_files: Vec<DataFile>) -> Table {
        let mut producer = SimpleSnapshotProducer::new(AppendIntent {
            commit_uuid: None,
            snapshot_properties: HashMap::new(),
            data_files,
        });
        let snapshot = producer
            .apply(table)
            .await
            .unwrap()
            .take_updates()
            .into_iter()
            .find_map(|u| match u {
                TableUpdate::AddSnapshot { snapshot } => Some(SnapshotRef::new(snapshot)),
                _ => None,
            })
            .expect("append should add a snapshot");
        let metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .add_snapshot(snapshot.as_ref().clone())
            .unwrap()
            .set_ref(
                MAIN_BRANCH,
                SnapshotReference::new(
                    snapshot.snapshot_id(),
                    SnapshotRetention::branch(None, None, None),
                ),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        table.clone().with_metadata(Arc::new(metadata))
    }

    #[tokio::test]
    async fn test_validate_files_not_live_sees_files_added_by_rebase() {
        // No current snapshot: nothing is live, so nothing is a duplicate.
        let table = make_v2_minimal_table();
        let files = vec![data_file(&table, "test/dup.parquet", 1)];
        validate_files_not_live(&table, &files).await.unwrap();

        // A concurrent writer commits the same file; validating against that
        // base must detect it.
        let rebased = append_to(&table, files.clone()).await;
        let err = validate_files_not_live(&rebased, &files)
            .await
            .expect_err("file committed concurrently must be detected");
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("test/dup.parquet"));

        // A different file is not flagged.
        validate_files_not_live(&rebased, &[data_file(&table, "test/other.parquet", 1)])
            .await
            .unwrap();
    }

    #[test]
    fn test_metadata_paths_carry_identity_and_attempt() {
        let table = make_v2_minimal_table();
        let uuid = Uuid::nil();

        assert!(
            manifest_path(&table, uuid, 3)
                .unwrap()
                .ends_with("00000000-0000-0000-0000-000000000000-m3.avro")
        );
        assert!(
            manifest_list_path(&table, 7, 2, uuid)
                .unwrap()
                .ends_with("snap-7-2-00000000-0000-0000-0000-000000000000.avro")
        );
    }

    #[test]
    fn test_summary_counts_deleted_data_files() {
        let table = make_v2_minimal_table();
        let deleted = deleted_file(&table);

        let summary = build_summary(
            &table,
            Operation::Delete,
            HashMap::new(),
            &[],
            &[deleted],
            false,
        )
        .unwrap();
        let properties = &summary.additional_properties;

        assert_eq!(properties.get("deleted-data-files").unwrap(), "1");
        assert_eq!(properties.get("deleted-records").unwrap(), "2");
        assert_eq!(properties.get("removed-files-size").unwrap(), "100");
    }

    #[test]
    fn test_summary_keeps_decimal_scale_of_dropped_partition_source() {
        let table = make_v2_minimal_table();
        let without_d = table.metadata().current_schema().as_ref().clone();
        let d = NestedField::required(
            4,
            "d",
            Type::Primitive(PrimitiveType::Decimal {
                precision: 9,
                scale: 2,
            }),
        );
        let x = NestedField::required(1, "x", Type::Primitive(PrimitiveType::Int));
        let with_d = Schema::builder()
            .with_fields(without_d.as_struct().fields()[1..].iter().cloned())
            .with_fields([d.into(), x.into()])
            .build()
            .unwrap();
        let metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .set_properties(HashMap::from([(
                TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT.to_string(),
                "10".to_string(),
            )]))
            .unwrap()
            .add_current_schema(with_d.clone())
            .unwrap()
            .add_partition_spec(
                PartitionSpec::builder(with_d)
                    .add_partition_field("d", "d", Transform::Identity)
                    .unwrap()
                    .add_partition_field("x", "x", Transform::Identity)
                    .unwrap()
                    .build()
                    .unwrap()
                    .into_unbound(),
            )
            .unwrap()
            .add_current_schema(without_d)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let table = table.with_metadata(Arc::new(metadata));
        let deleted = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/deleted.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(2)
            .partition_spec_id(1)
            .partition(Struct::from_iter([
                Some(Literal::decimal(123)),
                Some(Literal::long(5)),
            ]))
            .build()
            .unwrap();

        let summary = build_summary(
            &table,
            Operation::Delete,
            HashMap::new(),
            &[],
            &[deleted],
            false,
        )
        .unwrap();

        assert!(
            summary
                .additional_properties
                .contains_key("partitions.d=1.23/x=5")
        );
    }

    #[cfg(debug_assertions)]
    #[tokio::test]
    #[should_panic(expected = "deleted files must match this commit's manifest entries")]
    async fn test_commit_rejects_unrecorded_deletions() {
        let table = make_v2_minimal_table();
        let deleted = deleted_file(&table);
        let snapshot_id = generate_snapshot_id(&table);

        write_snapshot_commit(&table, SnapshotCommitInput {
            snapshot_id,
            manifest_list_path: manifest_list_path(&table, snapshot_id, 0, Uuid::now_v7()).unwrap(),
            operation: Operation::Delete,
            properties: HashMap::new(),
            added_files: &[],
            removed_files: &[deleted],
            manifests: vec![],
            truncate_full_table: false,
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_new_manifest_writer_uses_the_given_schema_and_spec() {
        let table = make_v2_minimal_table();
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(7)
                .with_fields(vec![
                    NestedField::required(1, "a", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .unwrap(),
        );
        let partition_spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(3)
                .add_partition_field("a", "a", Transform::Identity)
                .unwrap()
                .build()
                .unwrap(),
        );

        let commit_uuid = Uuid::now_v7();
        let snapshot_id = generate_snapshot_id(&table);
        let first = new_manifest_writer(
            &table,
            manifest_path(&table, commit_uuid, 0).unwrap(),
            snapshot_id,
            ManifestContentType::Data,
            schema.clone(),
            partition_spec.clone(),
        )
        .unwrap()
        .write_manifest_file()
        .await
        .unwrap();
        let second = new_manifest_writer(
            &table,
            manifest_path(&table, commit_uuid, 1).unwrap(),
            snapshot_id,
            ManifestContentType::Data,
            schema.clone(),
            partition_spec,
        )
        .unwrap()
        .write_manifest_file()
        .await
        .unwrap();

        assert_ne!(first.manifest_path, second.manifest_path);

        let bytes = table
            .file_io()
            .new_input(&first.manifest_path)
            .unwrap()
            .read()
            .await
            .unwrap();
        let metadata = Manifest::parse_avro(&bytes).unwrap().into_parts().1;

        // Not the table's defaults, which are schema 0 and spec 0.
        assert_eq!(metadata.schema_id(), 7);
        assert_eq!(metadata.schema().as_ref(), schema.as_ref());
        assert_eq!(metadata.partition_spec().spec_id(), 3);
        assert_eq!(first.partition_spec_id, 3);
    }
}
