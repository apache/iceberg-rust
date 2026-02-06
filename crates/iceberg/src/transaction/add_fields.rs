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

use crate::spec::NestedFieldRef;
use crate::table::Table;
use crate::transaction::action::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, Result, TableRequirement, TableUpdate};

/// A transaction action for adding new fields to the table's current schema.
///
/// This action clones the table's current schema, appends the provided fields,
/// and emits the appropriate `AddSchema` and `SetCurrentSchema` updates along
/// with a `CurrentSchemaIdMatch` requirement to guard against concurrent schema changes.
pub struct AddFieldsAction {
    fields: Vec<NestedFieldRef>,
}

impl AddFieldsAction {
    /// Creates a new `AddFieldsAction` with the given fields.
    pub(crate) fn new(fields: Vec<NestedFieldRef>) -> Self {
        Self { fields }
    }

    /// Adds a single field to the action.
    pub fn add_field(mut self, field: NestedFieldRef) -> Self {
        self.fields.push(field);
        self
    }
}

#[async_trait]
impl TransactionAction for AddFieldsAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        // Validate that new required fields have an initial_default value.
        // Without initial_default, old Parquet files (written before the schema change)
        // cannot provide a value for the required column, leading to silent data corruption
        // (a non-nullable Arrow column filled with nulls).
        if let Some(field) = self
            .fields
            .iter()
            .find(|f| f.required && f.initial_default.is_none())
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot add required field '{}' (id={}) without an initial_default value. \
                     Existing data files do not contain this field, so a default is needed \
                     to populate it when reading older data. Either make the field optional \
                     or set an initial_default.",
                    field.name, field.id
                ),
            ));
        }

        let base_schema = table.metadata().current_schema();
        let schema = base_schema
            .as_ref()
            .clone()
            .into_builder()
            .with_fields(self.fields.clone())
            .build()?;

        let updates = vec![
            TableUpdate::AddSchema { schema },
            TableUpdate::SetCurrentSchema { schema_id: -1 },
        ];

        let requirements = vec![TableRequirement::CurrentSchemaIdMatch {
            current_schema_id: base_schema.schema_id(),
        }];

        Ok(ActionCommit::new(updates, requirements))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use as_any::Downcast;

    use crate::spec::{Literal, NestedField, NestedFieldRef, PrimitiveType, Type};
    use crate::transaction::Transaction;
    use crate::transaction::action::{ApplyTransactionAction, TransactionAction};
    use crate::transaction::add_fields::AddFieldsAction;
    use crate::transaction::tests::make_v2_table;
    use crate::{ErrorKind, TableRequirement, TableUpdate};

    #[tokio::test]
    async fn test_add_field() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);

        let new_field = NestedFieldRef::new(NestedField::optional(
            4,
            "new_field",
            Type::Primitive(PrimitiveType::Int),
        ));

        let action = tx.add_fields(vec![new_field.clone()]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let requirements = action_commit.take_requirements();

        // Verify AddSchema update
        let expected_schema = table
            .metadata()
            .current_schema()
            .as_ref()
            .clone()
            .into_builder()
            .with_fields(vec![new_field])
            .build()
            .unwrap();

        assert_eq!(updates.len(), 2);
        assert_eq!(updates[0], TableUpdate::AddSchema {
            schema: expected_schema
        });
        assert_eq!(updates[1], TableUpdate::SetCurrentSchema { schema_id: -1 });

        // Verify requirement
        assert_eq!(requirements.len(), 1);
        assert_eq!(requirements[0], TableRequirement::CurrentSchemaIdMatch {
            current_schema_id: table.metadata().current_schema().schema_id()
        });
    }

    #[test]
    fn test_add_field_apply() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);

        let new_field = NestedFieldRef::new(NestedField::optional(
            4,
            "new_field",
            Type::Primitive(PrimitiveType::Int),
        ));

        let tx = tx.add_fields(vec![new_field]).apply(tx).unwrap();

        assert_eq!(tx.actions.len(), 1);
        (*tx.actions[0])
            .downcast_ref::<AddFieldsAction>()
            .expect("AddFieldsAction was not applied to Transaction!");
    }

    #[tokio::test]
    async fn test_add_field_with_existing_field_id() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);

        // Field ID 1 already exists in the V2 test table schema
        let conflicting_field = NestedFieldRef::new(NestedField::new(
            1,
            "new_field",
            Type::Primitive(PrimitiveType::Int),
            true,
        ));

        let action = tx.add_fields(vec![conflicting_field]);
        let result = Arc::new(action).commit(&table).await;
        assert!(
            result.is_err(),
            "should fail because field_id 1 is already taken"
        );
    }

    #[tokio::test]
    async fn test_add_required_field_without_initial_default_fails() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);

        // required=true but no initial_default
        let required_field = NestedFieldRef::new(NestedField::required(
            4,
            "required_no_default",
            Type::Primitive(PrimitiveType::Int),
        ));

        let action = tx.add_fields(vec![required_field]);
        let result = Arc::new(action).commit(&table).await;
        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("should reject required field without initial_default"),
        };
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message().contains("required_no_default"),
            "error should mention the field name, got: {}",
            err.message()
        );
    }

    #[tokio::test]
    async fn test_add_required_field_with_initial_default_succeeds() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);

        // required=true with initial_default set
        let required_field_with_default = NestedFieldRef::new(
            NestedField::required(
                4,
                "required_with_default",
                Type::Primitive(PrimitiveType::Int),
            )
            .with_initial_default(Literal::int(0)),
        );

        let action = tx.add_fields(vec![required_field_with_default]);
        let result = Arc::new(action).commit(&table).await;
        assert!(
            result.is_ok(),
            "required field with initial_default should be accepted"
        );
    }
}
