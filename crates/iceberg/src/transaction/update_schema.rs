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

use crate::spec::{SchemaOperation, schema_update};
use crate::table::Table;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Result, TableRequirement, TableUpdate};

pub struct UpdateSchemaAction {
    operations: Vec<SchemaOperation>,
}

impl UpdateSchemaAction {
    pub fn new() -> Self {
        Self {
            operations: Vec::new(),
        }
    }

    pub fn push_operation(mut self, op: impl Into<SchemaOperation>) -> Self {
        self.operations.push(op.into());
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
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let schema = schema_update(table.current_schema_ref(), &self.operations)?;
        let current_schema_id = table.metadata().current_schema_id();
        let last_column_id = table.metadata().last_column_id();
        Ok(ActionCommit::new(
            vec![
                TableUpdate::AddSchema {
                    schema: Arc::unwrap_or_clone(schema),
                },
                TableUpdate::SetCurrentSchema { schema_id: -1 },
            ],
            vec![
                TableRequirement::CurrentSchemaIdMatch { current_schema_id },
                TableRequirement::LastAssignedFieldIdMatch {
                    last_assigned_field_id: last_column_id,
                },
            ],
        ))
    }
}
