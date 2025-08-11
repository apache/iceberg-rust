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

use async_trait::async_trait;

use crate::error::{Error, ErrorKind, Result};
use crate::spec::{Schema, SchemaRef, TableMetadataBuilder};
use crate::table::Table;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{TableRequirement, TableUpdate};

/// UpdateSchemaAction is a transaction action for performing schema evolution to the table.
///
/// TODO(hjiang): Currently only drop column is supported, need to implement a few more schema evolution operation, i.e., add columns, rename columns, change nullability, update default, etc.
pub struct UpdateSchemaAction {
    /// Schema update attributes.
    case_sensitive: bool,
    /// Current schema before update.
    schema: SchemaRef,
    /// Columns names to drop from the table.
    drops: HashSet<String>,
}

impl UpdateSchemaAction {
    pub(crate) fn new(schema: SchemaRef) -> Self {
        Self {
            case_sensitive: false,
            schema,
            drops: HashSet::new(),
        }
    }

    /// Set case sensitivity when updating schema by column names.
    pub fn set_case_sensitivity(mut self, case_sensitivity: bool) -> Self {
        self.case_sensitive = case_sensitivity;
        self
    }

    /// drops a column from a table.
    ///
    /// # Arguments
    ///
    /// * `column_name` - column to drop.
    ///
    /// # Returns
    ///
    /// Returns the `UpdateSchema` with the drop operation staged.
    pub fn drop_column(mut self, column_name: String) -> Self {
        self.drops.insert(column_name);
        self
    }

    /// Validate columns to drop, and get their field ids.
    fn get_field_ids_to_drop(&self) -> Result<HashSet<i32>> {
        // Validate not all columns are dropped.
        if self.schema.field_id_to_fields().len() == self.drops.len() {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "Cannot drop all columns in the table.".to_string(),
            ));
        }

        let identifier_field_ids = self.schema.identifier_field_ids().collect::<HashSet<i32>>();
        let mut field_ids_to_drop = HashSet::with_capacity(self.drops.len());

        // Get field id to drop.
        for cur_column_name in self.drops.iter() {
            let field = if self.case_sensitive {
                self.schema.field_by_name(cur_column_name)
            } else {
                self.schema.field_by_name_case_insensitive(cur_column_name)
            }
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "drop column name,'{}' , doesn't exist in the schema",
                        cur_column_name
                    ),
                )
            })?;

            // Validate columns to drop cannot be the table identifier.
            if identifier_field_ids.contains(&field.id) {
                return Err(Error::new(
                    ErrorKind::PreconditionFailed,
                    format!(
                        "Column '{}' is the table identifier, which cannot be dropped.",
                        cur_column_name
                    ),
                ));
            }

            field_ids_to_drop.insert(field.id);
        }

        Ok(field_ids_to_drop)
    }

    /// Get updated schema.
    fn get_updated_schema(&self) -> Result<Schema> {
        let field_ids_to_drop = self.get_field_ids_to_drop()?;
        let old_schema_id = self.schema.schema_id();
        let new_schema_id = old_schema_id + 1;

        let mut new_fields = vec![];
        for (field_id, field) in self.schema.field_id_to_fields() {
            if field_ids_to_drop.contains(field_id) {
                continue;
            }
            new_fields.push(field.clone());
        }

        let schema_builder = Schema::builder();
        let new_schema = schema_builder
            .with_schema_id(new_schema_id)
            .with_identifier_field_ids(self.schema.identifier_field_ids())
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
        if self.drops.is_empty() {
            return Ok(ActionCommit::new(updates, requirements));
        }

        let new_schema = self.get_updated_schema()?;
        updates.push(TableUpdate::AddSchema { schema: new_schema });
        updates.push(TableUpdate::SetCurrentSchema {
            schema_id: TableMetadataBuilder::LAST_ADDED,
        });
        Ok(ActionCommit::new(updates, requirements))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use tempfile::TempDir;

    use crate::io::FileIOBuilder;
    use crate::spec::Schema;
    use crate::transaction::tests::make_v2_table;
    use crate::transaction::{ApplyTransactionAction, Transaction};
    use crate::{Catalog, MemoryCatalog, NamespaceIdent, TableCreation};

    /// Test util function to get [`TableCreation`].
    async fn create_table(
        catalog: &mut MemoryCatalog,
        schema: Arc<Schema>,
        warehouse_location: &str,
    ) {
        let table_name = "test1".to_string();
        let namespace_ident = NamespaceIdent::from_vec(vec!["ns1".to_string()]).unwrap();

        let table_creation = TableCreation::builder()
            .name(table_name.clone())
            .location(format!(
                "{}/{}/{}",
                warehouse_location,
                namespace_ident.to_url_string(),
                table_name
            ))
            .schema(schema.as_ref().clone())
            .build();

        catalog
            .create_namespace(&namespace_ident, /*properties=*/ HashMap::new())
            .await
            .unwrap();
        catalog
            .create_table(&namespace_ident, table_creation)
            .await
            .unwrap();
    }

    #[test]
    fn test_drop_empty_columns() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);
        let action = tx.update_schema();
        assert!(action.drops.is_empty());
    }

    #[test]
    fn test_drop_column() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);
        let mut action = tx.update_schema();
        action = action.drop_column("z".to_string());
        assert_eq!(action.drops, HashSet::from([("z".to_string())]));
    }

    // Test invalid column drop: identifier ids get dropped.
    #[tokio::test]
    async fn test_drop_identifier_ids_with_catalog() {
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let temp_dir = TempDir::new().unwrap();
        let warehouse_location = temp_dir.path().to_str().unwrap().to_string();

        let table = make_v2_table();
        let mut memory_catalog = MemoryCatalog::new(file_io, Some(warehouse_location.clone()));
        let schema = table.metadata().current_schema().clone();
        create_table(&mut memory_catalog, schema, &warehouse_location).await;

        let mut tx = Transaction::new(&table);
        let mut action = tx.update_schema();
        action = action.drop_column("x".to_string());
        tx = action.apply(tx).unwrap();

        let res = tx.commit(&memory_catalog).await;
        assert!(res.is_err());
    }

    // Test empty columns drop with memory catalog.
    #[tokio::test]
    async fn test_drop_empty_columns_with_catalog() {
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let temp_dir = TempDir::new().unwrap();
        let warehouse_location = temp_dir.path().to_str().unwrap().to_string();

        let table = make_v2_table();
        let mut memory_catalog = MemoryCatalog::new(file_io, Some(warehouse_location.clone()));
        let schema = table.metadata().current_schema().clone();
        create_table(&mut memory_catalog, schema, &warehouse_location).await;

        let mut tx = Transaction::new(&table);
        let action = tx.update_schema();
        tx = action.apply(tx).unwrap();

        let table = tx.commit(&memory_catalog).await.unwrap();
        let schema = table.metadata().current_schema();
        assert!(schema.field_by_id(/*field_id=*/ 1).is_some());
        assert!(schema.field_by_id(/*field_id=*/ 2).is_some());
        assert!(schema.field_by_id(/*field_id=*/ 3).is_some());
        assert_eq!(schema.highest_field_id(), 3);
        assert_eq!(schema.identifier_field_ids().len(), 2);
    }

    // Test column drop with memory catalog.
    #[tokio::test]
    async fn test_drop_columns_with_catalog() {
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let temp_dir = TempDir::new().unwrap();
        let warehouse_location = temp_dir.path().to_str().unwrap().to_string();

        let table = make_v2_table();
        let mut memory_catalog = MemoryCatalog::new(file_io, Some(warehouse_location.clone()));
        let schema = table.metadata().current_schema().clone();
        create_table(&mut memory_catalog, schema, &warehouse_location).await;

        let mut tx = Transaction::new(&table);
        let mut action = tx.update_schema();
        action = action.drop_column("z".to_string());
        tx = action.apply(tx).unwrap();

        let table = tx.commit(&memory_catalog).await.unwrap();
        let schema = table.metadata().current_schema();
        assert!(schema.field_by_id(/*field_id=*/ 1).is_some());
        assert!(schema.field_by_id(/*field_id=*/ 2).is_some());
        assert!(schema.field_by_id(/*field_id=*/ 3).is_none());
        assert_eq!(schema.highest_field_id(), 2);
        assert_eq!(schema.identifier_field_ids().len(), 2);
    }

    // Test case sensitive column drop with memory catalog.
    #[tokio::test]
    async fn test_drop_case_sensitive_columns_with_catalog() {
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let temp_dir = TempDir::new().unwrap();
        let warehouse_location = temp_dir.path().to_str().unwrap().to_string();

        let table = make_v2_table();
        let mut memory_catalog = MemoryCatalog::new(file_io, Some(warehouse_location.clone()));
        let schema = table.metadata().current_schema().clone();
        create_table(&mut memory_catalog, schema, &warehouse_location).await;

        let mut tx = Transaction::new(&table);
        let mut action = tx.update_schema();
        action = action.set_case_sensitivity(true);
        action = action.drop_column("Z".to_string());
        tx = action.apply(tx).unwrap();

        let res = tx.commit(&memory_catalog).await;
        assert!(res.is_err());
    }
}
