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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::{Error, ErrorKind, Result};
use crate::spec::{Schema, SchemaBuilder, SchemaRef};
use crate::table::Table;
use crate::transaction::snapshot::SnapshotProducer;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{TableRequirement, TableUpdate};

/// UpdateSchemaAction is a transaction action for performing schema evolution to the table.
pub struct UpdateSchemaAction {
    /// Schema update attributes.
    case_sensitive: bool,
    /// Current schema before update.
    schema: SchemaRef,
    /// Current field ids.
    identifier_field_ids: HashSet<i32>,
    /// Columns to drop on the table.
    deletes: HashSet<i32>,
}

impl UpdateSchemaAction {
    pub(crate) fn new(schema: SchemaRef) -> Self {
        let identifier_field_ids = schema.identifier_field_ids().collect::<HashSet<i32>>();
        Self {
            case_sensitive: false,
            schema,
            identifier_field_ids,
            deletes: HashSet::new(),
        }
    }

    /// Set case sensitivity when updating schema by column names.
    pub fn set_case_sensitivity(mut self, case_sensitivity: bool) -> Self {
        self.case_sensitive = case_sensitivity;
        self
    }

    /// Deletes a column from a table.
    ///
    /// # Arguments
    ///
    /// * `column_name` - The path to the column.
    ///
    /// # Returns
    ///
    /// Returns the `UpdateSchema` with the delete operation staged.
    pub fn delete_column(&mut self, column_name: Vec<String>) -> Result<&mut Self> {
        let full_name = column_name.join(".");

        // Get field id to drop.
        let field = if self.case_sensitive {
            self.schema.field_by_name(&full_name)
        } else {
            self.schema.field_by_name_case_insensitive(&full_name)
        }
        .ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Delete column name,'{}' , doesn't exist in the schema",
                    full_name
                ),
            )
        })?;

        // Validate columns to drop cannot be the table identifier.
        if self.identifier_field_ids.contains(&field.id) {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                format!(
                    "Column '{}' is the table identifier, which canot be dropped.",
                    full_name
                ),
            ));
        }

        self.deletes.insert(field.id);

        Ok(self)
    }

    /// Get updated schema.
    fn get_updated_schema(&self) -> Result<Schema> {
        let old_schema_id = self.schema.schema_id();
        let new_schema_id = old_schema_id + 1;

        let mut new_fields = vec![];
        for (field_id, field) in self.schema.field_id_to_fields() {
            if self.deletes.contains(field_id) {
                continue;
            }
            new_fields.push(field.clone());
        }

        let schema_builder = Schema::builder();
        let new_schema = schema_builder
            .with_schema_id(new_schema_id)
            .with_identifier_field_ids(self.identifier_field_ids.clone())
            .with_fields(new_fields)
            .build()?;
        Ok(new_schema)
    }
}

#[async_trait]
impl TransactionAction for UpdateSchemaAction {
    async fn commit(self: Arc<Self>, _table: &Table) -> Result<ActionCommit> {
        let mut updates: Vec<TableUpdate> = vec![];
        let requirements: Vec<TableRequirement> = vec![];
        if self.deletes.is_empty() {
            return Ok(ActionCommit::new(updates, requirements));
        }

        let new_schema = self.get_updated_schema()?;
        let new_schema_id = new_schema.schema_id();
        updates.push(TableUpdate::AddSchema { schema: new_schema });
        updates.push(TableUpdate::SetCurrentSchema {
            schema_id: new_schema_id,
        });
        Ok(ActionCommit::new(updates, requirements))
    }
}
