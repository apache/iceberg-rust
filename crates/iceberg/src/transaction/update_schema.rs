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

use crate::spec::{Schema, UnboundPartitionSpec};
use crate::table::Table;
use crate::transaction::action::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, Result, TableUpdate};

#[derive(Clone)]
pub struct SchemaPartitionSpec {
    schema: Schema,
    partition_spec: Option<UnboundPartitionSpec>,
}
/// A transaction action that sets or updates the schema and partition spec of a table.
///
/// This action is used to explicitly set a new metadata schema and partition spec during a transaction.
pub struct UpdateSchemaAction {
    schema_with_partition_spec: Option<SchemaPartitionSpec>,
}

impl UpdateSchemaAction {
    /// Creates a new [`UpdateSchemaAction`] with no schema set.
    pub fn new() -> Self {
        UpdateSchemaAction {
            schema_with_partition_spec: None,
        }
    }

    /// Sets the target schema for this action and returns the updated instance.
    ///
    /// # Arguments
    ///
    /// * `schema` - A [`Schema`] representing the table's schema.
    /// * `partition_spec` - A [`UnboundPartitionSpec`] representing the table's partition spec.
    ///
    /// # Returns
    ///
    /// The [`UpdateSchemaAction`] with the new schema set.
    pub fn set_schema_with_partition_spec(
        mut self,
        schema: Schema,
        partition_spec: Option<UnboundPartitionSpec>,
    ) -> Self {
        self.schema_with_partition_spec = Some(SchemaPartitionSpec {
            schema,
            partition_spec,
        });
        self
    }
}

impl Default for UpdateSchemaAction {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TransactionAction for UpdateSchemaAction {
    async fn commit(self: Arc<Self>, _table: &Table) -> Result<ActionCommit> {
        let updates: Vec<TableUpdate>;
        if let Some(SchemaPartitionSpec {
            schema,
            partition_spec,
        }) = self.schema_with_partition_spec.clone()
        {
            let partition_spec = partition_spec.unwrap_or(UnboundPartitionSpec {
                spec_id: None,
                fields: vec![],
            });

            updates = vec![
                TableUpdate::AddSchema {
                    schema: schema.clone(),
                },
                TableUpdate::SetCurrentSchema { schema_id: -1 }, // Use -1 to reference the last added schema, since Iceberg may reassign the schema ID
                TableUpdate::AddSpec {
                    spec: partition_spec,
                },
                TableUpdate::SetDefaultSpec { spec_id: -1 },
            ];
        } else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Schema is not set for UpdateSchemaAction!",
            ));
        }

        Ok(ActionCommit::new(updates, vec![]))
    }
}
