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

    pub fn push_operation(mut self, op: SchemaOperation) -> Self {
        self.operations.push(op);
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, LazyLock};

    use crate::spec::{
        AddColumn, DeleteColumn, ListType, MapType, MoveColumn, NestedField, PrimitiveType,
        RenameColumn, Schema, SchemaOperation, StructType, UpdateColumn, schema_update,
    };

    /// Build a schema with top-level and nested fields to exercise all operations.
    ///
    /// Schema:
    ///   1  id        required int
    ///   2  name      optional string
    ///   3  address   optional struct
    ///     4  street   required string
    ///     5  city     optional string
    ///     6  zip      optional int
    ///   7  tags      optional list<string>  (element id 8)
    ///   9  metrics   optional map<string, double>  (key 10, value 11)
    static SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Int.into()).into(),
                NestedField::optional(2, "name", PrimitiveType::String.into()).into(),
                NestedField::optional(
                    3,
                    "address",
                    StructType::new(vec![
                        NestedField::required(4, "street", PrimitiveType::String.into()).into(),
                        NestedField::optional(5, "city", PrimitiveType::String.into()).into(),
                        NestedField::optional(6, "zip", PrimitiveType::Int.into()).into(),
                    ])
                    .into(),
                )
                .into(),
                NestedField::optional(
                    7,
                    "tags",
                    ListType::optional(8, PrimitiveType::String.into()).into(),
                )
                .into(),
                NestedField::optional(
                    9,
                    "metrics",
                    MapType::optional(
                        10,
                        PrimitiveType::String.into(),
                        11,
                        PrimitiveType::Double.into(),
                    )
                    .into(),
                )
                .into(),
            ])
            .build()
            .unwrap()
    });

    /// A complex schema evolution test exercising add, rename, update, move, and delete
    /// on both top-level and nested fields in a single schema_update call.
    ///
    /// Operations applied (in order):
    ///   1. Add    top-level "score" (optional float)
    ///   2. Add    nested "address.country" (optional string)
    ///   3. Rename "name" → "full_name"
    ///   4. Rename nested "address.city" → "town"
    ///   5. Update "address.zip" doc to "postal code"
    ///   6. Delete "tags"
    ///   7. Move   "id" after "name"  (so order becomes: full_name, id, ...)
    ///   8. Move   nested "address.street" after "address.city"
    ///
    /// Expected result:
    ///   2  full_name   optional string
    ///   1  id          required int
    ///   3  address     optional struct
    ///     5  town       optional string
    ///     4  street     required string
    ///     6  zip        optional int    doc="postal code"
    ///    13  country    optional string
    ///   9  metrics     optional map<string, double>
    ///  12  score       optional float
    #[test]
    fn complex_schema_evolution() {
        let schema = Arc::new(SCHEMA.clone());
        let result = schema_update(schema, &[
            // 1. Add top-level "score"
            SchemaOperation::Add(
                AddColumn::builder()
                    .name("score")
                    .r#type(PrimitiveType::Float.into())
                    .build(),
            ),
            // 2. Add nested "address.country"
            SchemaOperation::Add(
                AddColumn::builder()
                    .parent("address".to_string())
                    .name("country")
                    .r#type(PrimitiveType::String.into())
                    .build(),
            ),
            // 3. Rename "name" → "full_name"
            SchemaOperation::Rename(
                RenameColumn::builder()
                    .name("name")
                    .new_name("full_name")
                    .build(),
            ),
            // 4. Rename nested "address.city" → "town"
            SchemaOperation::Rename(
                RenameColumn::builder()
                    .name("address.city")
                    .new_name("town")
                    .build(),
            ),
            // 5. Update "address.zip" doc
            SchemaOperation::Update(
                UpdateColumn::builder()
                    .name("address.zip")
                    .new_doc(Some("postal code".into()))
                    .build(),
            ),
            // 6. Delete "tags"
            SchemaOperation::Delete(DeleteColumn::new("tags")),
            // 7. Move "id" after "name"
            SchemaOperation::Move(MoveColumn::after("id", "name")),
            // 8. Move nested "address.street" after "address.city"
            SchemaOperation::Move(MoveColumn::after("address.street", "address.city")),
        ])
        .unwrap();

        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::optional(2, "full_name", PrimitiveType::String.into()).into(),
                NestedField::required(1, "id", PrimitiveType::Int.into()).into(),
                NestedField::optional(
                    3,
                    "address",
                    StructType::new(vec![
                        NestedField::optional(5, "town", PrimitiveType::String.into()).into(),
                        NestedField::required(4, "street", PrimitiveType::String.into()).into(),
                        NestedField::optional(6, "zip", PrimitiveType::Int.into())
                            .with_doc("postal code")
                            .into(),
                        NestedField::optional(13, "country", PrimitiveType::String.into()).into(),
                    ])
                    .into(),
                )
                .into(),
                NestedField::optional(
                    9,
                    "metrics",
                    MapType::optional(
                        10,
                        PrimitiveType::String.into(),
                        11,
                        PrimitiveType::Double.into(),
                    )
                    .into(),
                )
                .into(),
                NestedField::optional(12, "score", PrimitiveType::Float.into()).into(),
            ])
            .build()
            .unwrap();

        assert_eq!(result.as_struct(), expected.as_struct());
    }
}
