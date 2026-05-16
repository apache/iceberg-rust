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

use crate::spec::{
    AddColumn, DeleteColumn, MoveColumn, RenameColumn, SchemaOperation, UpdateColumn, schema_update,
};
use crate::table::Table;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Result, TableRequirement, TableUpdate};

pub struct UpdateSchemaAction {
    operations: Vec<SchemaOperation>,
}

impl UpdateSchemaAction {
    pub(crate) fn new() -> Self {
        Self {
            operations: Vec::new(),
        }
    }

    pub fn add(mut self, add: AddColumn) -> Self {
        self.operations.push(add.into());
        self
    }

    pub fn update(mut self, update: UpdateColumn) -> Self {
        self.operations.push(update.into());
        self
    }

    pub fn rename(mut self, rename: RenameColumn) -> Self {
        self.operations.push(rename.into());
        self
    }

    pub fn delete(mut self, delete: DeleteColumn) -> Self {
        self.operations.push(delete.into());
        self
    }

    pub fn r#move(mut self, r#move: MoveColumn) -> Self {
        self.operations.push(r#move.into());
        self
    }

    pub fn allow_incompatible_changes(mut self) -> Self {
        self.operations
            .push(SchemaOperation::AllowIncompatibleChanges);
        self
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
