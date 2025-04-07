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

//! This module contains an AddFieldsAction for adding fields to a table.

use crate::error::Result;
use crate::spec::NestedFieldRef;
use crate::transaction::Transaction;
use crate::{TableRequirement, TableUpdate};

/// Transaction action for easily adding fields to the table.
pub struct AddFieldsAction<'a> {
    tx: Transaction<'a>,
    fields: Vec<NestedFieldRef>,
}

/// Transaction action for easily adding fields to the table.
impl<'a> AddFieldsAction<'a> {
    /// Creates a new `AddFieldsAction` for the given transaction.
    pub(crate) fn new(tx: Transaction<'a>, fields: Vec<NestedFieldRef>) -> Self {
        Self { tx, fields }
    }

    /// Finish building the action and apply it to the transaction.
    pub fn apply(mut self) -> Result<Transaction<'a>> {
        let base_schema = self.tx.table.metadata().current_schema();
        let schema_builder = <crate::spec::Schema as Clone>::clone(base_schema).into_builder();
        let schema = schema_builder.with_fields(self.fields).build()?;
        let schema_id = schema.schema_id();

        let updates = vec![
            TableUpdate::AddSchema { schema },
            TableUpdate::SetCurrentSchema {
                schema_id: schema_id + 1,
            },
        ];
        let requirements = vec![TableRequirement::CurrentSchemaIdMatch {
            current_schema_id: self.tx.table.metadata().current_schema().schema_id(),
        }];
        self.tx.append_updates(updates)?;
        self.tx.append_requirements(requirements)?;
        Ok(self.tx)
    }
}

#[cfg(test)]
mod tests {
    use crate::spec::{NestedField, NestedFieldRef, PrimitiveType, Type};
    use crate::transaction::tests::make_v2_table;
    use crate::transaction::Transaction;
    use crate::TableUpdate;

    #[test]
    fn test_add_field() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);
        let add_fields = tx.add_fields(vec![NestedFieldRef::new(NestedField::new(
            4,
            "new_field",
            Type::Primitive(PrimitiveType::Int),
            true,
        ))]);
        let tx = add_fields.apply().unwrap();

        let expected_schema =
            <crate::spec::Schema as Clone>::clone(table.metadata().current_schema())
                .into_builder()
                .with_fields(vec![NestedFieldRef::new(NestedField::new(
                    4,
                    "new_field",
                    Type::Primitive(PrimitiveType::Int),
                    true,
                ))])
                .build()
                .unwrap();
        assert_eq!(expected_schema.as_struct().fields().len(), 4);
        assert_eq!(tx.updates[0], TableUpdate::AddSchema {
            schema: expected_schema
        });
        assert_eq!(tx.updates[1], TableUpdate::SetCurrentSchema {
            schema_id: 2
        });
    }

    #[test]
    fn test_add_field_with_existing_field_id() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);
        let add_fields = tx.add_fields(vec![NestedFieldRef::new(NestedField::new(
            1,
            "new_field",
            Type::Primitive(PrimitiveType::Int),
            true,
        ))]);
        assert!(
            add_fields.apply().is_err(),
            "should fail because field_id 1 is taken"
        );
    }
}
