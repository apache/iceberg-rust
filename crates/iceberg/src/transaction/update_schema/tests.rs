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

use std::collections::HashMap;
use std::io::BufReader;
use std::sync::Arc;

use as_any::Downcast;

use crate::spec::{
    DEFAULT_SCHEMA_ID, DEFAULT_SCHEMA_NAME_MAPPING, Literal, NameMapping, NestedField,
    PrimitiveType, Schema, Struct, StructType, TableMetadata, Type, VariantType,
};
use crate::table::Table;
use crate::transaction::Transaction;
use crate::transaction::action::{ApplyTransactionAction, TransactionAction};
use crate::transaction::tests::make_v2_table;
use crate::transaction::update_schema::{AddColumn, DEFAULT_FIELD_ID, UpdateSchemaAction};
use crate::{Error, ErrorKind, TableIdent, TableRequirement, TableUpdate};

// The V2 test table has:
//   last_column_id: 3
//   current schema (id=1): x(1, req, long), y(2, req, long), z(3, req, long)
//   identifier_field_ids: [1, 2]

/// Build a V2 test table that includes nested types:
///
///   last_column_id: 14
///   current schema (id=0):
///     x(1, req, long)           -- identifier
///     y(2, req, long)           -- identifier
///     z(3, req, long)
///     person(4, opt, struct)
///       name(5, opt, string)
///       age(6, req, int)
///     tags(7, opt, list<struct>)
///       element(8, req, struct)
///         key(9, opt, string)
///         value(10, opt, string)
///     props(11, opt, map<string, struct>)
///       key(12, req, string)
///       value(13, req, struct)
///         data(14, opt, string)
fn make_v2_table_with_nested() -> Table {
    let json = r#"{
        "format-version": 2,
        "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c2",
        "location": "s3://bucket/test/location",
        "last-sequence-number": 0,
        "last-updated-ms": 1602638573590,
        "last-column-id": 14,
        "current-schema-id": 0,
        "schemas": [
            {
                "type": "struct",
                "schema-id": 0,
                "identifier-field-ids": [1, 2],
                "fields": [
                    {"id": 1, "name": "x", "required": true, "type": "long"},
                    {"id": 2, "name": "y", "required": true, "type": "long"},
                    {"id": 3, "name": "z", "required": true, "type": "long"},
                    {"id": 4, "name": "person", "required": false, "type": {
                        "type": "struct",
                        "fields": [
                            {"id": 5, "name": "name", "required": false, "type": "string"},
                            {"id": 6, "name": "age", "required": true, "type": "int"}
                        ]
                    }},
                    {"id": 7, "name": "tags", "required": false, "type": {
                        "type": "list",
                        "element-id": 8,
                        "element": {
                            "type": "struct",
                            "fields": [
                                {"id": 9, "name": "key", "required": false, "type": "string"},
                                {"id": 10, "name": "value", "required": false, "type": "string"}
                            ]
                        },
                        "element-required": true
                    }},
                    {"id": 11, "name": "props", "required": false, "type": {
                        "type": "map",
                        "key-id": 12,
                        "key": "string",
                        "value-id": 13,
                        "value": {
                            "type": "struct",
                            "fields": [
                                {"id": 14, "name": "data", "required": false, "type": "string"}
                            ]
                        },
                        "value-required": true
                    }}
                ]
            }
        ],
        "default-spec-id": 0,
        "partition-specs": [
            {"spec-id": 0, "fields": []}
        ],
        "last-partition-id": 999,
        "default-sort-order-id": 0,
        "sort-orders": [
            {"order-id": 0, "fields": []}
        ],
        "properties": {},
        "current-snapshot-id": -1,
        "snapshots": []
    }"#;

    let reader = BufReader::new(json.as_bytes());
    let metadata = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

    Table::builder()
        .metadata(metadata)
        .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
        .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
        .file_io(crate::io::FileIO::new_with_memory())
        .runtime(crate::test_utils::test_runtime())
        .build()
        .unwrap()
}

async fn apply_schema(table: &Table, action: UpdateSchemaAction) -> Schema {
    let mut commit = Arc::new(action).commit(table).await.unwrap();
    match commit.take_updates().remove(0) {
        TableUpdate::AddSchema { schema } => schema,
        update => panic!("expected AddSchema, got {update:?}"),
    }
}

async fn commit_error(table: &Table, action: UpdateSchemaAction) -> Error {
    match Arc::new(action).commit(table).await {
        Ok(_) => panic!("expected schema update to fail"),
        Err(error) => error,
    }
}

// -----------------------------------------------------------------------
// Existing root-level tests
// -----------------------------------------------------------------------

#[test]
fn test_assign_fresh_ids_variant() {
    // Variant carries no sub-fields, so fresh-id assignment only renames the field
    // itself and leaves the type untouched.
    let mut next_id = 10;
    let field = NestedField::optional(1, "data", Type::Variant(VariantType));
    let assigned = super::apply::assign_fresh_ids(&field, &mut next_id);

    assert_eq!(assigned.id, 11);
    assert_eq!(*assigned.field_type, Type::Variant(VariantType));
    assert_eq!(next_id, 11);
}

#[tokio::test]
async fn test_add_column() {
    let table = make_v2_table();
    let tx = Transaction::new(&table);

    let action = tx.update_schema().add_column(AddColumn::optional(
        "new_col",
        Type::Primitive(PrimitiveType::Int),
    ));

    let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
    let updates = action_commit.take_updates();
    let requirements = action_commit.take_requirements();

    assert_eq!(updates.len(), 2);

    // Extract the new schema from the AddSchema update.
    let new_schema = match &updates[0] {
        TableUpdate::AddSchema { schema } => schema,
        other => panic!("expected AddSchema, got {other:?}"),
    };

    let expected_schema = table
        .metadata()
        .current_schema()
        .as_ref()
        .clone()
        .into_builder()
        .with_schema_id(DEFAULT_SCHEMA_ID)
        .with_fields([
            NestedField::optional(4, "new_col", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .unwrap();
    assert_eq!(new_schema, &expected_schema);

    assert_eq!(updates[1], TableUpdate::SetCurrentSchema { schema_id: -1 });

    // Verify requirement.
    assert_eq!(requirements.len(), 2);
    assert_eq!(requirements[0], TableRequirement::CurrentSchemaIdMatch {
        current_schema_id: table.metadata().current_schema().schema_id()
    });
    assert_eq!(
        requirements[1],
        TableRequirement::LastAssignedFieldIdMatch {
            last_assigned_field_id: table.metadata().last_column_id()
        }
    );
}

#[tokio::test]
async fn test_commit_replays_operations_against_latest_metadata() {
    let table = make_v2_table();
    let action = Arc::new(Transaction::new(&table).update_schema().add_column(
        AddColumn::optional("added", Type::Primitive(PrimitiveType::String)),
    ));

    let mut initial_commit = action.clone().commit(&table).await.unwrap();
    let initial_schema = match initial_commit.take_updates().remove(0) {
        TableUpdate::AddSchema { schema } => schema,
        update => panic!("expected AddSchema, got {update:?}"),
    };
    assert_eq!(initial_schema.field_by_name("added").unwrap().id, 4);

    let mut concurrent_fields = table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .to_vec();
    concurrent_fields.push(
        NestedField::optional(4, "concurrent", Type::Primitive(PrimitiveType::Boolean)).into(),
    );
    let concurrent_schema = Schema::builder()
        .with_fields(concurrent_fields)
        .with_identifier_field_ids(table.metadata().current_schema().identifier_field_ids())
        .build()
        .unwrap();
    let refreshed_metadata = table
        .metadata()
        .clone()
        .into_builder(None)
        .add_schema(concurrent_schema)
        .unwrap()
        .set_current_schema(-1)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let refreshed = table.with_metadata(Arc::new(refreshed_metadata));

    let mut replayed_commit = action.commit(&refreshed).await.unwrap();
    let replayed_schema = match replayed_commit.take_updates().remove(0) {
        TableUpdate::AddSchema { schema } => schema,
        update => panic!("expected AddSchema, got {update:?}"),
    };
    assert_eq!(replayed_schema.field_by_name("concurrent").unwrap().id, 4);
    assert_eq!(replayed_schema.field_by_name("added").unwrap().id, 5);
    assert_eq!(replayed_commit.take_requirements(), vec![
        TableRequirement::CurrentSchemaIdMatch {
            current_schema_id: refreshed.metadata().current_schema_id(),
        },
        TableRequirement::LastAssignedFieldIdMatch {
            last_assigned_field_id: 4,
        },
    ]);
}

#[tokio::test]
async fn test_add_column_with_doc() {
    let table = make_v2_table();
    let tx = Transaction::new(&table);

    let action = tx.update_schema().add_column(
        AddColumn::builder()
            .name("documented_col")
            .field_type(Type::Primitive(PrimitiveType::String))
            .doc("A documented column")
            .build(),
    );

    let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
    let updates = action_commit.take_updates();

    let new_schema = match &updates[0] {
        TableUpdate::AddSchema { schema } => schema,
        other => panic!("expected AddSchema, got {other:?}"),
    };

    let field = new_schema
        .field_by_name("documented_col")
        .expect("documented_col should exist");
    assert_eq!(field.id, 4);
    assert!(!field.required);
    assert_eq!(field.doc.as_deref(), Some("A documented column"));
}

#[tokio::test]
async fn test_add_required_column_with_initial_default() {
    let table = make_v2_table();
    let tx = Transaction::new(&table);

    let action = tx.update_schema().add_column(AddColumn::required(
        "req_col",
        Type::Primitive(PrimitiveType::Int),
        Literal::int(0),
    ));

    let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
    let updates = action_commit.take_updates();

    let new_schema = match &updates[0] {
        TableUpdate::AddSchema { schema } => schema,
        other => panic!("expected AddSchema, got {other:?}"),
    };

    let field = new_schema
        .field_by_name("req_col")
        .expect("req_col should exist");
    assert_eq!(field.id, 4);
    assert!(field.required);
    assert_eq!(field.initial_default, Some(Literal::int(0)));
    assert_eq!(field.write_default, Some(Literal::int(0)));
}

#[tokio::test]
async fn test_add_column_name_conflict_fails() {
    let table = make_v2_table();
    let tx = Transaction::new(&table);

    // "x" already exists in the V2 test schema.
    let action = tx.update_schema().add_column(AddColumn::optional(
        "x",
        Type::Primitive(PrimitiveType::Int),
    ));

    let result = Arc::new(action).commit(&table).await;
    let err = match result {
        Err(e) => e,
        Ok(_) => panic!("should reject adding a column with an existing name"),
    };
    assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
    assert!(
        err.message().contains("already exists"),
        "error should mention name conflict, got: {}",
        err.message()
    );
}

#[tokio::test]
async fn test_delete_column() {
    let table = make_v2_table();
    let tx = Transaction::new(&table);

    // z is not an identifier field, so we can delete it.
    let action = tx.update_schema().delete_column("z");

    let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
    let updates = action_commit.take_updates();

    let new_schema = match &updates[0] {
        TableUpdate::AddSchema { schema } => schema,
        other => panic!("expected AddSchema, got {other:?}"),
    };

    assert!(
        new_schema.field_by_name("z").is_none(),
        "z should be deleted"
    );
    assert!(new_schema.field_by_name("x").is_some());
    assert!(new_schema.field_by_name("y").is_some());
}

#[tokio::test]
async fn test_delete_missing_column_fails() {
    let table = make_v2_table();
    let tx = Transaction::new(&table);

    let action = tx.update_schema().delete_column("nonexistent");

    let result = Arc::new(action).commit(&table).await;
    let err = match result {
        Err(e) => e,
        Ok(_) => panic!("should reject deleting a non-existent column"),
    };
    assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
    assert!(
        err.message().contains("nonexistent"),
        "error should mention the missing column, got: {}",
        err.message()
    );
}

#[tokio::test]
async fn test_add_and_delete_combined() {
    let table = make_v2_table();
    let tx = Transaction::new(&table);

    // Delete z, add a new column.
    let action = tx
        .update_schema()
        .delete_column("z")
        .add_column(AddColumn::optional(
            "w",
            Type::Primitive(PrimitiveType::Boolean),
        ));

    let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
    let updates = action_commit.take_updates();

    let new_schema = match &updates[0] {
        TableUpdate::AddSchema { schema } => schema,
        other => panic!("expected AddSchema, got {other:?}"),
    };

    assert!(
        new_schema.field_by_name("z").is_none(),
        "z should be deleted"
    );
    let w = new_schema.field_by_name("w").expect("w should exist");
    assert_eq!(w.id, 4);
    assert!(!w.required);
}

#[tokio::test]
async fn test_delete_and_readd_same_name() {
    let table = make_v2_table();
    let tx = Transaction::new(&table);

    // Delete z, then add a new column named z -- should succeed.
    let action = tx
        .update_schema()
        .delete_column("z")
        .add_column(AddColumn::optional(
            "z",
            Type::Primitive(PrimitiveType::Boolean),
        ));

    let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
    let updates = action_commit.take_updates();

    let new_schema = match &updates[0] {
        TableUpdate::AddSchema { schema } => schema,
        other => panic!("expected AddSchema, got {other:?}"),
    };

    let z = new_schema
        .field_by_name("z")
        .expect("z should exist with new type");
    assert_eq!(z.id, 4); // new ID, not the old 3
    assert_eq!(*z.field_type, Type::Primitive(PrimitiveType::Boolean));
}

#[test]
fn test_apply() {
    let table = make_v2_table();
    let tx = Transaction::new(&table);

    let tx = tx
        .update_schema()
        .add_column(AddColumn::optional(
            "new_col",
            Type::Primitive(PrimitiveType::Int),
        ))
        .apply(tx)
        .unwrap();

    assert_eq!(tx.actions.len(), 1);
    (*tx.actions[0])
        .downcast_ref::<UpdateSchemaAction>()
        .expect("UpdateSchemaAction was not applied to Transaction!");
}

// -----------------------------------------------------------------------
// Nested add tests
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_add_column_to_struct() {
    let table = make_v2_table_with_nested();
    let tx = Transaction::new(&table);

    // Add "email" to the "person" struct.
    let action = tx.update_schema().add_column(
        AddColumn::builder()
            .name("email")
            .field_type(Type::Primitive(PrimitiveType::String))
            .parent("person")
            .build(),
    );

    let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
    let updates = action_commit.take_updates();

    let new_schema = match &updates[0] {
        TableUpdate::AddSchema { schema } => schema,
        other => panic!("expected AddSchema, got {other:?}"),
    };

    // "email" should be nested under "person" with ID = last_column_id + 1 = 15.
    let email = new_schema
        .field_by_name("person.email")
        .expect("person.email should exist");
    assert_eq!(email.id, 15);
    assert!(!email.required);
    assert_eq!(*email.field_type, Type::Primitive(PrimitiveType::String));

    // Original nested fields should still be there.
    assert!(new_schema.field_by_name("person.name").is_some());
    assert!(new_schema.field_by_name("person.age").is_some());
}

#[tokio::test]
async fn test_add_column_to_struct_with_doc() {
    let table = make_v2_table_with_nested();
    let tx = Transaction::new(&table);

    let action = tx.update_schema().add_column(
        AddColumn::builder()
            .name("phone")
            .field_type(Type::Primitive(PrimitiveType::String))
            .parent("person")
            .doc("Phone number")
            .build(),
    );

    let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
    let updates = action_commit.take_updates();

    let new_schema = match &updates[0] {
        TableUpdate::AddSchema { schema } => schema,
        other => panic!("expected AddSchema, got {other:?}"),
    };

    let phone = new_schema
        .field_by_name("person.phone")
        .expect("person.phone should exist");
    assert_eq!(phone.id, 15);
    assert_eq!(phone.doc.as_deref(), Some("Phone number"));
}

#[tokio::test]
async fn test_add_column_to_list_element_struct() {
    let table = make_v2_table_with_nested();
    let tx = Transaction::new(&table);

    // "tags" is a list<struct{key, value}>. Adding to the list navigates to its
    // element struct automatically.
    let action = tx.update_schema().add_column(
        AddColumn::builder()
            .name("score")
            .field_type(Type::Primitive(PrimitiveType::Double))
            .parent("tags")
            .build(),
    );

    let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
    let updates = action_commit.take_updates();

    let new_schema = match &updates[0] {
        TableUpdate::AddSchema { schema } => schema,
        other => panic!("expected AddSchema, got {other:?}"),
    };

    // The list element struct should now contain "score".
    let score = new_schema
        .field_by_name("tags.element.score")
        .expect("tags.element.score should exist");
    assert_eq!(score.id, 15);
    assert!(!score.required);

    // Existing fields preserved.
    assert!(new_schema.field_by_name("tags.element.key").is_some());
    assert!(new_schema.field_by_name("tags.element.value").is_some());
}

#[tokio::test]
async fn test_add_column_to_map_value_struct() {
    let table = make_v2_table_with_nested();
    let tx = Transaction::new(&table);

    // "props" is a map<string, struct{data}>. Adding to the map navigates to its
    // value struct automatically.
    let action = tx.update_schema().add_column(
        AddColumn::builder()
            .name("version")
            .field_type(Type::Primitive(PrimitiveType::Int))
            .parent("props")
            .build(),
    );

    let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
    let updates = action_commit.take_updates();

    let new_schema = match &updates[0] {
        TableUpdate::AddSchema { schema } => schema,
        other => panic!("expected AddSchema, got {other:?}"),
    };

    let version = new_schema
        .field_by_name("props.value.version")
        .expect("props.value.version should exist");
    assert_eq!(version.id, 15);

    // Existing map value fields preserved.
    assert!(new_schema.field_by_name("props.value.data").is_some());
}

#[tokio::test]
async fn test_add_column_to_nonexistent_parent_fails() {
    let table = make_v2_table_with_nested();
    let tx = Transaction::new(&table);

    let action = tx.update_schema().add_column(
        AddColumn::builder()
            .name("col")
            .field_type(Type::Primitive(PrimitiveType::Int))
            .parent("nonexistent")
            .build(),
    );

    let err = match Arc::new(action).commit(&table).await {
        Err(e) => e,
        Ok(_) => panic!("should reject adding to a nonexistent parent"),
    };
    assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
    assert!(
        err.message().contains("nonexistent"),
        "error should mention the missing parent, got: {}",
        err.message()
    );
}

#[tokio::test]
async fn test_add_column_to_primitive_parent_fails() {
    let table = make_v2_table_with_nested();
    let tx = Transaction::new(&table);

    // "x" is a primitive (long), not a struct.
    let action = tx.update_schema().add_column(
        AddColumn::builder()
            .name("col")
            .field_type(Type::Primitive(PrimitiveType::Int))
            .parent("x")
            .build(),
    );

    let err = match Arc::new(action).commit(&table).await {
        Err(e) => e,
        Ok(_) => panic!("should reject adding to a primitive parent"),
    };
    assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
    assert!(
        err.message().contains("not a struct"),
        "error should mention type mismatch, got: {}",
        err.message()
    );
}

#[tokio::test]
async fn test_add_column_to_nested_name_conflict_fails() {
    let table = make_v2_table_with_nested();
    let tx = Transaction::new(&table);

    // "name" already exists in the "person" struct.
    let action = tx.update_schema().add_column(
        AddColumn::builder()
            .name("name")
            .field_type(Type::Primitive(PrimitiveType::String))
            .parent("person")
            .build(),
    );

    let err = match Arc::new(action).commit(&table).await {
        Err(e) => e,
        Ok(_) => panic!("should reject adding a column with conflicting name"),
    };
    assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
    assert!(
        err.message().contains("already exists"),
        "error should mention name conflict, got: {}",
        err.message()
    );
}

#[tokio::test]
async fn test_root_and_nested_add_combined() {
    let table = make_v2_table_with_nested();
    let tx = Transaction::new(&table);

    // Add a root column and a nested column in the same action.
    let action = tx
        .update_schema()
        .add_column(AddColumn::optional(
            "root_col",
            Type::Primitive(PrimitiveType::Boolean),
        ))
        .add_column(
            AddColumn::builder()
                .name("email")
                .field_type(Type::Primitive(PrimitiveType::String))
                .parent("person")
                .build(),
        );

    let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
    let updates = action_commit.take_updates();

    let new_schema = match &updates[0] {
        TableUpdate::AddSchema { schema } => schema,
        other => panic!("expected AddSchema, got {other:?}"),
    };

    // Root column gets the first fresh ID.
    let root_col = new_schema
        .field_by_name("root_col")
        .expect("root_col should exist");
    assert_eq!(root_col.id, 15);

    // Nested column gets the next ID.
    let email = new_schema
        .field_by_name("person.email")
        .expect("person.email should exist");
    assert_eq!(email.id, 16);
}

#[tokio::test]
async fn test_add_nested_struct_type_with_fresh_ids() {
    // Adding a new column whose TYPE contains nested fields (e.g. a struct column). All sub-fields must receive
    // fresh IDs, not placeholder `DEFAULT_FIELD_ID`.
    let table = make_v2_table();
    let tx = Transaction::new(&table);

    let action = tx.update_schema().add_column(AddColumn::optional(
        "address",
        Type::Struct(StructType::new(vec![
            NestedField::optional(
                DEFAULT_FIELD_ID,
                "street",
                Type::Primitive(PrimitiveType::String),
            )
            .into(),
            NestedField::optional(
                DEFAULT_FIELD_ID,
                "city",
                Type::Primitive(PrimitiveType::String),
            )
            .into(),
        ])),
    ));

    let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
    let updates = action_commit.take_updates();

    let new_schema = match &updates[0] {
        TableUpdate::AddSchema { schema } => schema,
        other => panic!("expected AddSchema, got {other:?}"),
    };

    // "address" gets ID 4 (last_column_id=3, +1).
    let address = new_schema
        .field_by_name("address")
        .expect("address should exist");
    assert_eq!(address.id, 4);

    // Sub-fields get IDs 5 and 6.
    let street = new_schema
        .field_by_name("address.street")
        .expect("address.street should exist");
    assert_eq!(street.id, 5);

    let city = new_schema
        .field_by_name("address.city")
        .expect("address.city should exist");
    assert_eq!(city.id, 6);
}

#[tokio::test]
async fn test_rename_root_and_nested_columns_preserves_ids() {
    let table = make_v2_table_with_nested();
    let schema = apply_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .rename_column("z", "payload")
            .rename_column("person.name", "full_name"),
    )
    .await;

    assert!(schema.field_by_name("z").is_none());
    assert_eq!(schema.field_by_name("payload").unwrap().id, 3);
    assert!(schema.field_by_name("person.name").is_none());
    assert_eq!(schema.field_by_name("person.full_name").unwrap().id, 5);
    assert_eq!(schema.field_by_name("person").unwrap().id, 4);
}

#[tokio::test]
async fn test_update_type_doc_and_default_preserves_metadata() {
    let table = make_v2_table_with_nested();
    let schema = apply_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .update_column_type("person.age", PrimitiveType::Long)
            .update_column_doc("person.age", Some("age in years".to_string()))
            .update_column_default("person.age", Some(Literal::long(18))),
    )
    .await;

    let age = schema.field_by_name("person.age").unwrap();
    assert_eq!(age.id, 6);
    assert!(age.required);
    assert_eq!(
        age.field_type.as_ref(),
        &Type::Primitive(PrimitiveType::Long)
    );
    assert_eq!(age.doc.as_deref(), Some("age in years"));
    assert_eq!(age.write_default, Some(Literal::long(18)));
    assert_eq!(age.initial_default, None);
}

#[tokio::test]
async fn test_type_promotion_casts_existing_defaults() {
    let table = make_v2_table();
    let schema = apply_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .add_column(
                AddColumn::builder()
                    .name("count")
                    .field_type(Type::Primitive(PrimitiveType::Int))
                    .initial_default(Literal::int(7))
                    .write_default(Literal::int(8))
                    .build(),
            )
            .update_column_type("count", PrimitiveType::Long),
    )
    .await;

    let count = schema.field_by_name("count").unwrap();
    assert_eq!(
        count.field_type.as_ref(),
        &Type::Primitive(PrimitiveType::Long)
    );
    assert_eq!(count.initial_default, Some(Literal::long(7)));
    assert_eq!(count.write_default, Some(Literal::long(8)));
}

#[tokio::test]
async fn test_rejects_invalid_type_promotion_and_default() {
    let table = make_v2_table_with_nested();
    let type_error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .update_column_type("person.age", PrimitiveType::String),
    )
    .await;
    assert_eq!(type_error.kind(), ErrorKind::PreconditionFailed);
    assert!(type_error.message().contains("Cannot change column type"));

    let default_error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .update_column_default("person.age", Some(Literal::string("unknown"))),
    )
    .await;
    assert_eq!(default_error.kind(), ErrorKind::PreconditionFailed);
    assert!(default_error.message().contains("Invalid default"));

    let nested_default_error = commit_error(
        &table,
        Transaction::new(&table).update_schema().add_column(
            AddColumn::builder()
                .name("details")
                .field_type(Type::Struct(StructType::new(vec![
                    NestedField::optional(100, "value", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])))
                .initial_default(Literal::Struct(Struct::from_iter(vec![Some(
                    Literal::long(1),
                )])))
                .build(),
        ),
    )
    .await;
    assert_eq!(nested_default_error.kind(), ErrorKind::PreconditionFailed);
    assert!(nested_default_error.message().contains("Invalid default"));
}

#[tokio::test]
async fn test_nullability_changes_require_explicit_opt_in() {
    let table = make_v2_table_with_nested();
    let error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .require_column("person.name"),
    )
    .await;
    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
    assert!(error.message().contains("optional -> required"));

    let schema = apply_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .allow_incompatible_changes()
            .require_column("person.name")
            .make_column_optional("person.age"),
    )
    .await;
    assert!(schema.field_by_name("person.name").unwrap().required);
    assert!(!schema.field_by_name("person.age").unwrap().required);
}

#[tokio::test]
async fn test_required_add_without_default_requires_opt_in() {
    fn required_column() -> AddColumn {
        AddColumn::builder()
            .name("required_col")
            .required(true)
            .field_type(Type::Primitive(PrimitiveType::String))
            .build()
    }

    let table = make_v2_table();
    let error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .add_column(required_column()),
    )
    .await;
    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);

    let schema = apply_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .allow_incompatible_changes()
            .add_column(required_column()),
    )
    .await;
    assert!(schema.field_by_name("required_col").unwrap().required);
}

#[tokio::test]
async fn test_move_columns_at_root_and_nested_levels() {
    let table = make_v2_table_with_nested();
    let schema = apply_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .move_column_first("z")
            .move_column_after("person.name", "person.age"),
    )
    .await;

    let root_ids: Vec<i32> = schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| field.id)
        .collect();
    assert_eq!(root_ids, vec![3, 1, 2, 4, 7, 11]);
    let Type::Struct(person) = schema.field_by_name("person").unwrap().field_type.as_ref() else {
        panic!("person should remain a struct")
    };
    assert_eq!(person.fields()[0].id, 6);
    assert_eq!(person.fields()[1].id, 5);
}

#[tokio::test]
async fn test_move_added_replacement_column() {
    let table = make_v2_table();
    let schema = apply_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .delete_column("z")
            .add_column(AddColumn::optional(
                "z",
                Type::Primitive(PrimitiveType::String),
            ))
            .move_column_first("z"),
    )
    .await;

    assert_eq!(schema.as_struct().fields()[0].name, "z");
    assert_eq!(schema.as_struct().fields()[0].id, 4);
}

#[tokio::test]
async fn test_move_validation() {
    let table = make_v2_table_with_nested();
    let self_move = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .move_column_before("z", "z"),
    )
    .await;
    assert!(self_move.message().contains("itself"));

    let cross_struct = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .move_column_after("person.name", "z"),
    )
    .await;
    assert!(cross_struct.message().contains("different struct"));

    let list_element = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .move_column_first("tags.element"),
    )
    .await;
    assert!(list_element.message().contains("non-struct"));
}

#[tokio::test]
async fn test_case_insensitive_resolution() {
    let table = make_v2_table_with_nested();
    let schema = apply_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .case_sensitive(false)
            .rename_column("PERSON.NAME", "full_name")
            .update_column_type("PERSON.AGE", PrimitiveType::Long),
    )
    .await;
    assert_eq!(schema.field_by_name("person.full_name").unwrap().id, 5);
    assert_eq!(
        schema
            .field_by_name("person.age")
            .unwrap()
            .field_type
            .as_ref(),
        &Type::Primitive(PrimitiveType::Long)
    );
}

#[tokio::test]
async fn test_identifier_field_replacement_controls_deletes() {
    let table = make_v2_table();
    let error = commit_error(
        &table,
        Transaction::new(&table).update_schema().delete_column("x"),
    )
    .await;
    assert!(error.message().contains("identifier field"));

    let schema = apply_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .delete_column("x")
            .delete_column("y")
            .set_identifier_fields(["z"]),
    )
    .await;
    assert_eq!(schema.identifier_field_ids().collect::<Vec<_>>(), vec![3]);
    assert!(schema.field_by_name("x").is_none());
}

#[tokio::test]
async fn test_identifier_field_validation_uses_updated_schema() {
    let table = make_v2_table();
    let error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .add_column(AddColumn::optional(
                "candidate",
                Type::Primitive(PrimitiveType::String),
            ))
            .set_identifier_fields(["candidate"]),
    )
    .await;
    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
    assert!(error.to_string().contains("optional field"));

    let schema = apply_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .add_column(AddColumn::required(
                "candidate",
                Type::Primitive(PrimitiveType::String),
                Literal::string("unknown"),
            ))
            .set_identifier_fields(["candidate"]),
    )
    .await;
    assert_eq!(schema.identifier_field_ids().collect::<Vec<_>>(), vec![4]);
}

#[tokio::test]
async fn test_rejects_map_key_changes() {
    let table = make_v2_table_with_nested();
    let error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .update_column_doc("props.key", Some("key docs".to_string())),
    )
    .await;
    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
    assert!(error.message().contains("Cannot alter map keys"));
}

#[tokio::test]
async fn test_union_by_name_adds_and_evolves_nested_fields() {
    let table = make_v2_table_with_nested();
    let incoming = Schema::builder()
        .with_fields(vec![
            NestedField::optional(
                100,
                "person",
                Type::Struct(StructType::new(vec![
                    NestedField::optional(101, "age", Type::Primitive(PrimitiveType::Long))
                        .with_doc("age in years")
                        .into(),
                    NestedField::required(102, "country", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])),
            )
            .into(),
        ])
        .build()
        .unwrap();
    let schema = apply_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .union_by_name(incoming),
    )
    .await;

    let age = schema.field_by_name("person.age").unwrap();
    assert_eq!(age.id, 6);
    assert!(!age.required);
    assert_eq!(
        age.field_type.as_ref(),
        &Type::Primitive(PrimitiveType::Long)
    );
    assert_eq!(age.doc.as_deref(), Some("age in years"));
    let country = schema.field_by_name("person.country").unwrap();
    assert_eq!(country.id, 15);
    assert!(!country.required, "union additions must be optional");
}

#[tokio::test]
async fn test_name_mapping_tracks_renames_and_additions() {
    let table = make_v2_table();
    let mapping = r#"[
        {"field-id":1,"names":["x"]},
        {"field-id":2,"names":["y"]},
        {"field-id":3,"names":["z"]}
    ]"#;
    let metadata = table
        .metadata()
        .clone()
        .into_builder(None)
        .set_properties(HashMap::from([(
            DEFAULT_SCHEMA_NAME_MAPPING.to_string(),
            mapping.to_string(),
        )]))
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let table = table.with_metadata(Arc::new(metadata));
    let action = Transaction::new(&table)
        .update_schema()
        .rename_column("z", "payload")
        .add_column(AddColumn::optional(
            "extra",
            Type::Primitive(PrimitiveType::String),
        ));
    let mut commit = Arc::new(action).commit(&table).await.unwrap();
    let property_update = commit
        .take_updates()
        .into_iter()
        .find_map(|update| match update {
            TableUpdate::SetProperties { updates } => {
                updates.get(DEFAULT_SCHEMA_NAME_MAPPING).cloned()
            }
            _ => None,
        })
        .expect("schema update should maintain the name mapping");
    let mapping: NameMapping = serde_json::from_str(&property_update).unwrap();
    let renamed = mapping
        .fields()
        .iter()
        .find(|field| field.field_id() == Some(3))
        .unwrap();
    assert_eq!(renamed.names(), &["z".to_string(), "payload".to_string()]);
    let added = mapping
        .fields()
        .iter()
        .find(|field| field.field_id() == Some(4))
        .unwrap();
    assert_eq!(added.names(), &["extra".to_string()]);
}

#[tokio::test]
async fn test_invalid_name_mapping_does_not_block_update() {
    let table = make_v2_table();
    let metadata = table
        .metadata()
        .clone()
        .into_builder(None)
        .set_properties(HashMap::from([(
            DEFAULT_SCHEMA_NAME_MAPPING.to_string(),
            "{not valid json".to_string(),
        )]))
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let table = table.with_metadata(Arc::new(metadata));
    let action = Transaction::new(&table)
        .update_schema()
        .rename_column("z", "payload");
    let mut commit = Arc::new(action).commit(&table).await.unwrap();

    let updates = commit.take_updates();
    assert!(updates.iter().any(|update| matches!(
        update,
        TableUpdate::AddSchema { schema }
            if schema.field_by_name("payload").is_some()
    )));
    assert!(!updates.iter().any(|update| matches!(
        update,
        TableUpdate::SetProperties { updates }
            if updates.contains_key(DEFAULT_SCHEMA_NAME_MAPPING)
    )));
}

#[tokio::test]
async fn test_column_properties_follow_renames_and_deletes() {
    let table = make_v2_table_with_nested();
    let metrics_key = "write.metadata.metrics.column.z";
    let bloom_key = "write.parquet.bloom-filter-enabled.column.z";
    let deleted_key = "write.parquet.stats-enabled.column.person.name";
    let metadata = table
        .metadata()
        .clone()
        .into_builder(None)
        .set_properties(HashMap::from([
            (metrics_key.to_string(), "full".to_string()),
            (bloom_key.to_string(), "true".to_string()),
            (deleted_key.to_string(), "false".to_string()),
            (
                "write.metadata.metrics.column.x".to_string(),
                "counts".to_string(),
            ),
        ]))
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let table = table.with_metadata(Arc::new(metadata));
    let action = Transaction::new(&table)
        .update_schema()
        .rename_column("z", "payload")
        .delete_column("person.name");
    let mut commit = Arc::new(action).commit(&table).await.unwrap();

    let updates = commit.take_updates();
    let removals = updates
        .iter()
        .find_map(|update| match update {
            TableUpdate::RemoveProperties { removals } => Some(removals),
            _ => None,
        })
        .unwrap();
    assert_eq!(removals, &vec![
        metrics_key.to_string(),
        bloom_key.to_string(),
        deleted_key.to_string(),
    ]);
    let property_updates = updates
        .iter()
        .find_map(|update| match update {
            TableUpdate::SetProperties { updates } => Some(updates),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        property_updates.get("write.metadata.metrics.column.payload"),
        Some(&"full".to_string())
    );
    assert_eq!(
        property_updates.get("write.parquet.bloom-filter-enabled.column.payload"),
        Some(&"true".to_string())
    );
    assert!(!property_updates.contains_key(deleted_key));
}
