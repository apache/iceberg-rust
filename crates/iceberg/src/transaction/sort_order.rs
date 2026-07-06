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

use std::sync::Arc;

use async_trait::async_trait;

use crate::error::Result;
use crate::spec::{NullOrder, SchemaRef, SortDirection, SortField, SortOrder, Transform};
use crate::table::Table;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, TableRequirement, TableUpdate};

/// Represents a sort field whose construction and validation are deferred until commit time.
/// This avoids the need to pass a `Table` reference into methods like `asc` or `desc` when
/// adding sort orders.
#[derive(Debug, PartialEq, Eq, Clone)]
struct PendingSortField {
    name: String,
    transform: Transform,
    direction: SortDirection,
    null_order: NullOrder,
}

impl PendingSortField {
    fn to_sort_field(&self, schema: &SchemaRef) -> Result<SortField> {
        let field_id = schema.field_id_by_name(self.name.as_str()).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Cannot find field {} in table schema", self.name),
            )
        })?;

        Ok(SortField::builder()
            .source_id(field_id)
            .transform(self.transform)
            .direction(self.direction)
            .null_order(self.null_order)
            .build())
    }
}

/// Transaction action for replacing sort order.
pub struct ReplaceSortOrderAction {
    pending_sort_fields: Vec<PendingSortField>,
}

impl ReplaceSortOrderAction {
    pub fn new() -> Self {
        ReplaceSortOrderAction {
            pending_sort_fields: vec![],
        }
    }

    /// Adds a field for sorting in ascending order, sorting by the column's raw value
    /// (an identity transform). To sort by a transform of the column instead (e.g.
    /// `bucket[N]`, `year`, `truncate[W]`), use [`Self::asc_with_transform`].
    pub fn asc(self, name: &str, null_order: NullOrder) -> Self {
        self.asc_with_transform(name, Transform::Identity, null_order)
    }

    /// Adds a field for sorting in descending order, sorting by the column's raw value
    /// (an identity transform). To sort by a transform of the column instead (e.g.
    /// `bucket[N]`, `year`, `truncate[W]`), use [`Self::desc_with_transform`].
    pub fn desc(self, name: &str, null_order: NullOrder) -> Self {
        self.desc_with_transform(name, Transform::Identity, null_order)
    }

    /// Adds a field for sorting in ascending order by a transform of the column's value
    /// (e.g. `Transform::Bucket(16)`, `Transform::Year`, `Transform::Truncate(4)`).
    ///
    /// Whether the transform is valid for the column's type is checked at commit time,
    /// once the table schema is available (mirroring Java's `SortOrder.Builder.build()`).
    pub fn asc_with_transform(
        self,
        name: &str,
        transform: Transform,
        null_order: NullOrder,
    ) -> Self {
        self.add_sort_field(name, transform, SortDirection::Ascending, null_order)
    }

    /// Adds a field for sorting in descending order by a transform of the column's value
    /// (e.g. `Transform::Bucket(16)`, `Transform::Year`, `Transform::Truncate(4)`).
    ///
    /// Whether the transform is valid for the column's type is checked at commit time,
    /// once the table schema is available (mirroring Java's `SortOrder.Builder.build()`).
    pub fn desc_with_transform(
        self,
        name: &str,
        transform: Transform,
        null_order: NullOrder,
    ) -> Self {
        self.add_sort_field(name, transform, SortDirection::Descending, null_order)
    }

    fn add_sort_field(
        mut self,
        name: &str,
        transform: Transform,
        sort_direction: SortDirection,
        null_order: NullOrder,
    ) -> Self {
        self.pending_sort_fields.push(PendingSortField {
            name: name.to_string(),
            transform,
            direction: sort_direction,
            null_order,
        });

        self
    }
}

impl Default for ReplaceSortOrderAction {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TransactionAction for ReplaceSortOrderAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let current_schema = table.metadata().current_schema();
        let sort_fields: Result<Vec<SortField>> = self
            .pending_sort_fields
            .iter()
            .map(|p| p.to_sort_field(current_schema))
            .collect();

        let bound_sort_order = SortOrder::builder()
            .with_fields(sort_fields?)
            .build(current_schema)?;

        let updates = vec![
            TableUpdate::AddSortOrder {
                sort_order: bound_sort_order,
            },
            TableUpdate::SetDefaultSortOrder { sort_order_id: -1 },
        ];

        let requirements = vec![
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: current_schema.schema_id(),
            },
            TableRequirement::DefaultSortOrderIdMatch {
                default_sort_order_id: table.metadata().default_sort_order().order_id,
            },
        ];

        Ok(ActionCommit::new(updates, requirements))
    }
}

#[cfg(test)]
mod tests {
    use as_any::Downcast;

    use crate::ErrorKind;
    use crate::spec::{NullOrder, SortDirection, Transform};
    use crate::transaction::sort_order::{PendingSortField, ReplaceSortOrderAction};
    use crate::transaction::tests::make_v2_table;
    use crate::transaction::{ApplyTransactionAction, Transaction, TransactionAction};

    #[test]
    fn test_replace_sort_order() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);
        let replace_sort_order = tx.replace_sort_order();

        let tx = replace_sort_order
            .asc("x", NullOrder::First)
            .desc("y", NullOrder::Last)
            .apply(tx)
            .unwrap();

        let replace_sort_order = (*tx.actions[0])
            .downcast_ref::<ReplaceSortOrderAction>()
            .unwrap();

        assert_eq!(replace_sort_order.pending_sort_fields, vec![
            PendingSortField {
                name: String::from("x"),
                transform: Transform::Identity,
                direction: SortDirection::Ascending,
                null_order: NullOrder::First,
            },
            PendingSortField {
                name: String::from("y"),
                transform: Transform::Identity,
                direction: SortDirection::Descending,
                null_order: NullOrder::Last,
            }
        ]);
    }

    #[test]
    fn test_replace_sort_order_with_transform() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);
        let replace_sort_order = tx.replace_sort_order();

        let tx = replace_sort_order
            .asc_with_transform("x", Transform::Bucket(16), NullOrder::First)
            .desc_with_transform("y", Transform::Truncate(4), NullOrder::Last)
            .apply(tx)
            .unwrap();

        let replace_sort_order = (*tx.actions[0])
            .downcast_ref::<ReplaceSortOrderAction>()
            .unwrap();

        assert_eq!(replace_sort_order.pending_sort_fields, vec![
            PendingSortField {
                name: String::from("x"),
                transform: Transform::Bucket(16),
                direction: SortDirection::Ascending,
                null_order: NullOrder::First,
            },
            PendingSortField {
                name: String::from("y"),
                transform: Transform::Truncate(4),
                direction: SortDirection::Descending,
                null_order: NullOrder::Last,
            }
        ]);
    }

    #[tokio::test]
    async fn test_replace_sort_order_with_transform_commits() {
        use std::sync::Arc;

        use crate::TableUpdate;

        let table = make_v2_table();
        let action = Arc::new(ReplaceSortOrderAction::new().asc_with_transform(
            "x",
            Transform::Bucket(16),
            NullOrder::First,
        ));

        let mut action_commit = TransactionAction::commit(action, &table).await.unwrap();
        let updates = action_commit.take_updates();

        let sort_order = match &updates[0] {
            TableUpdate::AddSortOrder { sort_order } => sort_order,
            other => panic!("expected AddSortOrder, got {other:?}"),
        };
        assert_eq!(sort_order.fields[0].transform, Transform::Bucket(16));
    }

    #[tokio::test]
    async fn test_replace_sort_order_rejects_incompatible_transform() {
        use std::sync::Arc;

        let table = make_v2_table();
        // `x` is a `long` column; `year` only accepts date/timestamp types.
        let action = Arc::new(ReplaceSortOrderAction::new().asc_with_transform(
            "x",
            Transform::Year,
            NullOrder::First,
        ));

        let err = match TransactionAction::commit(action, &table).await {
            Err(e) => e,
            Ok(_) => panic!("year transform on a long column should be rejected"),
        };
        assert_eq!(err.kind(), ErrorKind::Unexpected);
    }

    #[tokio::test]
    async fn test_sort_order_transform_survives_metadata_json_round_trip() {
        use crate::catalog::Catalog;
        use crate::memory::tests::new_memory_catalog;
        use crate::spec::SortDirection;
        use crate::transaction::tests::make_v3_minimal_table_in_catalog;

        // Commit a transform-based sort order through a real catalog: the memory
        // catalog serializes the updated table metadata to a metadata.json file
        // (`TableMetadata::write_to`), and `load_table` reads that file back and
        // parses it (`TableMetadata::read_from`). This exercises the full
        // JSON round-trip of the transform (e.g. `"bucket[16]"`), not just the
        // in-memory `TableUpdate`.
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let tx = Transaction::new(&table);
        let tx = tx
            .replace_sort_order()
            .asc_with_transform("x", Transform::Bucket(16), NullOrder::First)
            .desc_with_transform("y", Transform::Truncate(4), NullOrder::Last)
            .apply(tx)
            .unwrap();
        let committed = tx.commit(&catalog).await.unwrap();

        // Reload from the catalog: this parses the metadata.json written above.
        let reloaded = catalog.load_table(committed.identifier()).await.unwrap();
        let sort_order = reloaded.metadata().default_sort_order();

        assert_eq!(sort_order.fields.len(), 2);
        assert_eq!(sort_order.fields[0].transform, Transform::Bucket(16));
        assert_eq!(sort_order.fields[0].direction, SortDirection::Ascending);
        assert_eq!(sort_order.fields[1].transform, Transform::Truncate(4));
        assert_eq!(sort_order.fields[1].direction, SortDirection::Descending);
    }
}
