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
use crate::spec::{NullOrder, SortDirection, SortField, SortOrder, Transform};
use crate::table::Table;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, TableRequirement, TableUpdate};

#[derive(Debug, PartialEq, Eq, Clone)]
struct PendingSortField {
    name: String,
    direction: SortDirection,
    null_order: NullOrder,
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

    /// Adds a field for sorting in ascending order.
    pub fn asc(self, name: &str, null_order: NullOrder) -> Self {
        self.add_sort_field(name, SortDirection::Ascending, null_order)
    }

    /// Adds a field for sorting in descending order.
    pub fn desc(self, name: &str, null_order: NullOrder) -> Self {
        self.add_sort_field(name, SortDirection::Descending, null_order)
    }

    fn add_sort_field(
        mut self,
        name: &str,
        sort_direction: SortDirection,
        null_order: NullOrder,
    ) -> Self {
        self.pending_sort_fields.push(PendingSortField {
            name: name.to_string(),
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
        let pending_sort_fields = self.pending_sort_fields.clone();

        let sort_fields: Result<Vec<SortField>> = pending_sort_fields
            .iter()
            .map(|p| {
                let field_id = table
                    .metadata()
                    .current_schema()
                    .field_id_by_name(p.name.as_str())
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!("Cannot find field {} in table schema", p.name),
                        )
                    })?;

                Ok(SortField::builder()
                    .source_id(field_id)
                    .transform(Transform::Identity)
                    .direction(p.direction)
                    .null_order(p.null_order)
                    .build())
            })
            .collect();

        let unbound_sort_order = SortOrder::builder()
            .with_fields(sort_fields?)
            .build_unbound()?;

        let updates = vec![
            TableUpdate::AddSortOrder {
                sort_order: unbound_sort_order,
            },
            TableUpdate::SetDefaultSortOrder { sort_order_id: -1 },
        ];

        let requirements = vec![
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: table.metadata().current_schema().schema_id(),
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

    use crate::spec::{NullOrder, SortDirection};
    use crate::transaction::sort_order::{PendingSortField, ReplaceSortOrderAction};
    use crate::transaction::tests::make_v2_table;
    use crate::transaction::{ApplyTransactionAction, Transaction};

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
                direction: SortDirection::Ascending,
                null_order: NullOrder::First,
            },
            PendingSortField {
                name: String::from("y"),
                direction: SortDirection::Descending,
                null_order: NullOrder::Last,
            }
        ]);
    }
}
