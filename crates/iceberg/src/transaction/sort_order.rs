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
use crate::transaction::action::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, TableRequirement, TableUpdate};

/// Transaction action for replacing sort order.
pub struct ReplaceSortOrderAction {
    pub sort_fields: Vec<SortField>,
}

impl ReplaceSortOrderAction {
    /// Adds a field for sorting in ascending order.
    pub fn asc(self, table: &Table, name: &str, null_order: NullOrder) -> Result<Self> {
        self.add_sort_field(table, name, SortDirection::Ascending, null_order)
    }

    /// Adds a field for sorting in descending order.
    pub fn desc(self, table: &Table, name: &str, null_order: NullOrder) -> Result<Self> {
        self.add_sort_field(table, name, SortDirection::Descending, null_order)
    }

    fn add_sort_field(
        mut self,
        table: &Table,
        name: &str,
        sort_direction: SortDirection,
        null_order: NullOrder,
    ) -> Result<Self> {
        let field_id = table
            .metadata()
            .current_schema()
            .field_id_by_name(name)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Cannot find field {} in table schema", name),
                )
            })?;

        let sort_field = SortField::builder()
            .source_id(field_id)
            .transform(Transform::Identity)
            .direction(sort_direction)
            .null_order(null_order)
            .build();

        self.sort_fields.push(sort_field);
        Ok(self)
    }
}

#[async_trait]
impl TransactionAction for ReplaceSortOrderAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let unbound_sort_order = SortOrder::builder()
            .with_fields(self.sort_fields.clone())
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
    use crate::transaction::Transaction;
    use crate::transaction::tests::make_v2_table;
    use crate::{TableRequirement, TableUpdate};

    #[test]
    fn test_replace_sort_order() {
        let table = make_v2_table();
        let tx = Transaction::new(table);
        let tx = tx.replace_sort_order().apply().unwrap();

        assert_eq!(
            vec![
                TableUpdate::AddSortOrder {
                    sort_order: Default::default()
                },
                TableUpdate::SetDefaultSortOrder { sort_order_id: -1 }
            ],
            tx.updates
        );

        assert_eq!(
            vec![
                TableRequirement::CurrentSchemaIdMatch {
                    current_schema_id: 1
                },
                TableRequirement::DefaultSortOrderIdMatch {
                    default_sort_order_id: 3
                }
            ],
            tx.requirements
        );
    }
}
