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
use std::sync::Arc;

use async_trait::async_trait;

use crate::error::{Error, ErrorKind, Result};
use crate::spec::{
    FormatVersion, MAIN_BRANCH, PartitionSpecRef, Schema, SortOrder, SortOrderRef, StructType,
    TableMetadata, TableMetadataBuildResult, UnboundPartitionSpec,
};
use crate::table::Table;
use crate::transaction::action::{ActionCommit, StatelessAction};
use crate::{TableRequirement, TableUpdate};

/// Replaces a table's schema, partition spec and sort order, leaving it with no current
/// snapshot. Backs `REPLACE TABLE`.
///
/// Must be the first action in its transaction. Write the new contents against
/// [`ReplaceTableAction::staged_table`] and add them with a later action, such as
/// [`FastAppendAction`](crate::transaction::FastAppendAction).
///
/// Committing fails with [`ErrorKind::CatalogCommitConflicts`], without retrying, if since the
/// transaction started the table was dropped and recreated, a concurrent change assigned new
/// column or partition field ids, or a concurrent change would give the replacement's partition
/// spec or sort order a different id than its staged table.
#[derive(Clone)]
pub struct ReplaceTableAction {
    table: Table,
    schema: Schema,
    partition_spec: UnboundPartitionSpec,
    sort_order: SortOrder,
    location: Option<String>,
    format_version: Option<FormatVersion>,
    properties: HashMap<String, String>,
}

impl ReplaceTableAction {
    pub(crate) fn new(table: Table, schema: Schema) -> Self {
        Self {
            table,
            schema,
            partition_spec: UnboundPartitionSpec::builder().build(),
            sort_order: SortOrder::unsorted_order(),
            location: None,
            format_version: None,
            properties: HashMap::new(),
        }
    }

    /// Sets the partition spec. Defaults to unpartitioned.
    pub fn set_partition_spec(mut self, partition_spec: impl Into<UnboundPartitionSpec>) -> Self {
        self.partition_spec = partition_spec.into();
        self
    }

    /// Sets the sort order. Defaults to unsorted.
    pub fn set_sort_order(mut self, sort_order: SortOrder) -> Self {
        self.sort_order = sort_order;
        self
    }

    /// Moves the table to `location`.
    pub fn set_location(mut self, location: String) -> Self {
        self.location = Some(location);
        self
    }

    /// Upgrades the table to `format_version`.
    pub fn set_format_version(mut self, format_version: FormatVersion) -> Self {
        self.format_version = Some(format_version);
        self
    }

    /// Sets `properties` on top of the table's existing ones.
    pub fn set_properties(mut self, properties: HashMap<String, String>) -> Self {
        self.properties = properties;
        self
    }

    /// Returns the table as it will be after the replacement, to write its new contents against.
    ///
    /// # Errors
    /// - The table is format version 1, which is not supported.
    /// - The format version would downgrade the table.
    /// - The partition spec or sort order names a column missing from the schema.
    /// - The schema takes the name of a partition field in one of the table's specs without
    ///   being that field's source column.
    /// - The properties contain a reserved property.
    pub fn staged_table(&self) -> Result<Table> {
        let metadata = self.replace(self.table.metadata())?.metadata;
        Ok(self.table.clone().with_metadata(Arc::new(metadata)))
    }

    fn replace(&self, metadata: &TableMetadata) -> Result<TableMetadataBuildResult> {
        metadata
            .clone()
            .into_builder(None)
            .build_replacement(
                self.schema.clone(),
                self.partition_spec.clone(),
                self.sort_order.clone(),
                self.location.clone(),
                self.format_version,
                self.properties.clone(),
            )?
            .build()
    }
}

#[async_trait]
impl StatelessAction for ReplaceTableAction {
    async fn commit(&self, table: &Table) -> Result<ActionCommit> {
        let original = self.table.metadata();
        let base = table.metadata();
        if base.uuid() != original.uuid() {
            return Err(Error::new(
                ErrorKind::CatalogCommitConflicts,
                "Cannot replace table: it was recreated since the transaction started",
            )
            .with_context("expected", original.uuid())
            .with_context("found", base.uuid()));
        }
        if (base.last_column_id(), base.last_partition_id())
            != (original.last_column_id(), original.last_partition_id())
        {
            return Err(Error::new(
                ErrorKind::CatalogCommitConflicts,
                "Cannot replace table: a concurrent change assigned new column or partition field ids",
            ));
        }

        let staged = self.replace(original)?;
        let sets = |matches: fn(&TableUpdate) -> bool| staged.changes.iter().any(matches);
        // Staged changes are relative to the original table, so reset what may have moved since.
        let mut updates = Vec::new();
        if base.snapshot_for_ref(MAIN_BRANCH).is_some()
            && !sets(
                |update| matches!(update, TableUpdate::RemoveSnapshotRef { ref_name } if ref_name == MAIN_BRANCH),
            )
        {
            updates.push(TableUpdate::RemoveSnapshotRef {
                ref_name: MAIN_BRANCH.to_string(),
            });
        }
        if !sets(|update| matches!(update, TableUpdate::SetCurrentSchema { .. })) {
            updates.push(TableUpdate::SetCurrentSchema {
                schema_id: staged.metadata.current_schema_id(),
            });
        }
        if !sets(|update| matches!(update, TableUpdate::SetDefaultSpec { .. })) {
            updates.push(TableUpdate::SetDefaultSpec {
                spec_id: staged.metadata.default_partition_spec_id(),
            });
        }
        if !sets(|update| matches!(update, TableUpdate::SetDefaultSortOrder { .. })) {
            updates.push(TableUpdate::SetDefaultSortOrder {
                sort_order_id: staged.metadata.default_sort_order_id(),
            });
        }
        updates.extend(staged.changes);

        let mut builder = base.clone().into_builder(None);
        for update in &updates {
            builder = update.clone().apply(builder)?;
        }
        if layout(&staged.metadata) != layout(&builder.build()?.metadata) {
            return Err(Error::new(
                ErrorKind::CatalogCommitConflicts,
                "Cannot replace table: a concurrent change would give the replacement different ids than its staged table",
            ));
        }

        let requirements = vec![
            TableRequirement::UuidMatch { uuid: base.uuid() },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id: base.current_snapshot_id(),
            },
            TableRequirement::LastAssignedFieldIdMatch {
                last_assigned_field_id: base.last_column_id(),
            },
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: base.current_schema_id(),
            },
            TableRequirement::LastAssignedPartitionIdMatch {
                last_assigned_partition_id: base.last_partition_id(),
            },
            TableRequirement::DefaultSpecIdMatch {
                default_spec_id: base.default_partition_spec_id(),
            },
            TableRequirement::DefaultSortOrderIdMatch {
                default_sort_order_id: base.default_sort_order_id(),
            },
        ];

        Ok(ActionCommit::new(updates, requirements))
    }
}

/// The parts of the table that the staged table's data files are written against.
fn layout(metadata: &TableMetadata) -> (&StructType, &PartitionSpecRef, &SortOrderRef) {
    (
        metadata.current_schema().as_struct(),
        metadata.default_partition_spec(),
        metadata.default_sort_order(),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, FormatVersion, Literal,
        MAIN_BRANCH, NestedField, PrimitiveType, Schema, Struct, Transform, Type,
        UnboundPartitionField, UnboundPartitionSpec,
    };
    use crate::table::Table;
    use crate::test_utils::commit_with_fresh_state;
    use crate::transaction::tests::{make_v2_table, make_v3_minimal_table_in_catalog};
    use crate::transaction::{AddColumn, ApplyTransactionAction, Transaction};
    use crate::{Catalog, ErrorKind, TableCommit, TableCreation, TableRequirement, TableUpdate};

    fn replacement_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "x", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(2, "w", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap()
    }

    fn data_file(table: &Table, path: &str) -> DataFile {
        let partition = if table.metadata().default_partition_spec().is_unpartitioned() {
            Struct::empty()
        } else {
            Struct::from_iter([Some(Literal::long(1))])
        };
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition(partition)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .build()
            .unwrap()
    }

    async fn append(table: &Table, path: &str, catalog: &impl Catalog) -> Table {
        let tx = Transaction::new(table);
        tx.fast_append()
            .add_data_files(vec![data_file(table, path)])
            .apply(tx)
            .unwrap()
            .commit(catalog)
            .await
            .unwrap()
    }

    fn replace_with_new_contents(table: &Table) -> (Transaction, Table) {
        let tx = Transaction::new(table);
        let replace = tx.replace_table(replacement_schema());
        let staged = replace.staged_table().unwrap();
        let tx = replace.apply(tx).unwrap();
        let tx = tx
            .fast_append()
            .add_data_files(vec![data_file(&staged, "test/new.parquet")])
            .apply(tx)
            .unwrap();
        (tx, staged)
    }

    #[tokio::test]
    async fn test_replace_table_commits_new_contents_and_keeps_history() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append(&table, "test/old.parquet", &catalog).await;

        let (tx, staged) = replace_with_new_contents(&table);
        let replaced = tx.commit(&catalog).await.unwrap();

        let metadata = replaced.metadata();
        let current = metadata.current_snapshot().unwrap();
        assert_eq!(metadata.uuid(), table.metadata().uuid());
        assert_eq!(
            metadata.current_schema(),
            staged.metadata().current_schema()
        );
        assert_eq!(current.parent_snapshot_id(), None);
        assert_eq!(metadata.snapshots().count(), 2);
        assert_eq!(
            current.summary().additional_properties["total-data-files"],
            "1"
        );
    }

    #[tokio::test]
    async fn test_replace_table_commits_after_a_concurrent_append() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let (tx, _) = replace_with_new_contents(&table);
        append(&table, "test/concurrent.parquet", &catalog).await;
        let replaced = tx.commit(&catalog).await.unwrap();

        let metadata = replaced.metadata();
        let current = metadata.current_snapshot().unwrap();
        assert_eq!(current.parent_snapshot_id(), None);
        assert_eq!(metadata.snapshots().count(), 2);
        assert_eq!(
            current.summary().additional_properties["total-data-files"],
            "1"
        );
    }

    #[tokio::test]
    async fn test_replace_table_fails_when_a_concurrent_change_assigns_new_field_ids() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let (tx, _) = replace_with_new_contents(&table);
        let concurrent = Transaction::new(&table);
        concurrent
            .update_schema()
            .add_column(AddColumn::optional(
                "v",
                Type::Primitive(PrimitiveType::Int),
            ))
            .apply(concurrent)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();
        let err = tx.commit(&catalog).await.unwrap_err();

        assert_eq!(err.kind(), ErrorKind::CatalogCommitConflicts);
        assert_eq!(
            err.message(),
            "Cannot replace table: a concurrent change assigned new column or partition field ids"
        );
        assert!(!err.retryable());
    }

    /// Replaces a table that has an unused column `v` with `schema` while `v` is dropped
    /// concurrently, returning the staged and the replaced table.
    async fn replace_while_dropping_an_unused_column(
        schema: impl FnOnce(&Table) -> Schema,
    ) -> (Table, Table) {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let tx = Transaction::new(&table);
        let table = tx
            .update_schema()
            .add_column(AddColumn::optional(
                "v",
                Type::Primitive(PrimitiveType::Int),
            ))
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();

        let tx = Transaction::new(&table);
        let replace = tx.replace_table(schema(&table));
        let staged = replace.staged_table().unwrap();
        let tx = replace.apply(tx).unwrap();
        let tx = tx
            .fast_append()
            .add_data_files(vec![data_file(&staged, "test/new.parquet")])
            .apply(tx)
            .unwrap();

        let concurrent = Transaction::new(&table);
        concurrent
            .update_schema()
            .delete_column("v")
            .apply(concurrent)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();
        (staged, tx.commit(&catalog).await.unwrap())
    }

    #[tokio::test]
    async fn test_replace_table_keeps_the_staged_ids_of_a_concurrently_dropped_column() {
        let (staged, replaced) = replace_while_dropping_an_unused_column(|_| {
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "x", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(2, "v", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap()
        })
        .await;

        assert_eq!(
            replaced.metadata().current_schema().as_struct(),
            staged.metadata().current_schema().as_struct()
        );
    }

    #[tokio::test]
    async fn test_replace_table_restores_its_unchanged_schema_over_a_concurrent_schema_change() {
        let (staged, replaced) = replace_while_dropping_an_unused_column(|table| {
            (**table.metadata().current_schema()).clone()
        })
        .await;

        assert_eq!(
            replaced.metadata().current_schema().as_struct(),
            staged.metadata().current_schema().as_struct()
        );
    }

    #[tokio::test]
    async fn test_replace_table_fails_when_the_catalog_would_renumber_its_spec() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let (tx, _) = replace_with_new_contents(&table);
        catalog
            .update_table(
                TableCommit::builder()
                    .ident(table.identifier().clone())
                    .updates(vec![TableUpdate::AddSpec {
                        spec: UnboundPartitionSpec::builder()
                            .add_partition_field(
                                UnboundPartitionField::builder()
                                    .source_ids(vec![1])
                                    .name("x_part")
                                    .transform(Transform::Identity)
                                    .build()
                                    .unwrap(),
                            )
                            .unwrap()
                            .build(),
                    }])
                    .requirements(vec![])
                    .build(),
            )
            .await
            .unwrap();
        let err = tx.commit(&catalog).await.unwrap_err();

        assert_eq!(err.kind(), ErrorKind::CatalogCommitConflicts);
        assert_eq!(
            err.message(),
            "Cannot replace table: a concurrent change would give the replacement different ids than its staged table"
        );
        assert!(!err.retryable());
    }

    #[tokio::test]
    async fn test_replace_table_fails_when_the_table_was_recreated() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let (tx, _) = replace_with_new_contents(&table);
        let metadata = table.metadata();
        catalog.drop_table(table.identifier()).await.unwrap();
        catalog
            .create_table(
                table.identifier().namespace(),
                TableCreation::builder()
                    .name(table.identifier().name().to_string())
                    .schema((**metadata.current_schema()).clone())
                    .partition_spec((**metadata.default_partition_spec()).clone())
                    .sort_order((**metadata.default_sort_order()).clone())
                    .format_version(FormatVersion::V3)
                    .build(),
            )
            .await
            .unwrap();
        let err = tx.commit(&catalog).await.unwrap_err();

        assert_eq!(err.kind(), ErrorKind::CatalogCommitConflicts);
        assert_eq!(
            err.message(),
            "Cannot replace table: it was recreated since the transaction started"
        );
        assert!(!err.retryable());
    }

    #[tokio::test]
    async fn test_replace_table_requires_the_base_it_replaced() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);
        let mut commit = commit_with_fresh_state(tx.replace_table(replacement_schema()), &table)
            .await
            .unwrap();

        let base = table.metadata();
        assert_eq!(commit.take_requirements(), vec![
            TableRequirement::UuidMatch { uuid: base.uuid() },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id: base.current_snapshot_id(),
            },
            TableRequirement::LastAssignedFieldIdMatch {
                last_assigned_field_id: base.last_column_id(),
            },
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: base.current_schema_id(),
            },
            TableRequirement::LastAssignedPartitionIdMatch {
                last_assigned_partition_id: base.last_partition_id(),
            },
            TableRequirement::DefaultSpecIdMatch {
                default_spec_id: base.default_partition_spec_id(),
            },
            TableRequirement::DefaultSortOrderIdMatch {
                default_sort_order_id: base.default_sort_order_id(),
            },
        ]);
    }

    #[test]
    fn test_staged_table_applies_the_location_format_version_and_properties() {
        let table = make_v2_table();
        let staged = Transaction::new(&table)
            .replace_table(replacement_schema())
            .set_location("s3://bucket/moved".to_string())
            .set_format_version(FormatVersion::V3)
            .set_properties(HashMap::from([("key".to_string(), "value".to_string())]))
            .staged_table()
            .unwrap();

        let metadata = staged.metadata();
        assert_eq!(metadata.location(), "s3://bucket/moved");
        assert_eq!(metadata.format_version(), FormatVersion::V3);
        assert_eq!(metadata.properties()["key"], "value");
        assert_eq!(metadata.current_snapshot_id(), None);
    }
}
