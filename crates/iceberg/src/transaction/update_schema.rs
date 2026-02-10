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

use crate::spec::{
    ListType, Literal, MapType, NestedField, NestedFieldRef, Schema, StructType, Type,
};
use crate::table::Table;
use crate::transaction::action::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, Result, TableRequirement, TableUpdate};

/// Sentinel parent ID representing the table root (top-level columns).
const TABLE_ROOT_ID: i32 = -1;

/// A pending column addition, recording the parent path and the field to add.
struct PendingAdd {
    /// `None` means a root-level addition; `Some("person")` or `Some("person.address")`
    /// identifies the nested struct to add the column to.
    parent: Option<String>,
    /// The field to add. Uses placeholder ID `0` which is auto-assigned at commit time.
    field: NestedFieldRef,
}

/// Schema evolution API modeled after the Java `SchemaUpdate` implementation.
///
/// This action accumulates schema modifications (column additions and deletions)
/// via builder methods. At commit time, it validates all operations against the
/// current table schema, auto-assigns field IDs from `table.metadata().last_column_id()`,
/// builds a new schema, and emits `AddSchema` + `SetCurrentSchema` updates with a
/// `CurrentSchemaIdMatch` requirement.
///
/// # Example
///
/// ```ignore
/// let tx = Transaction::new(&table);
/// let action = tx.update_schema()
///     .add_column("new_col", Type::Primitive(PrimitiveType::Int))
///     .add_column_to("person", "email", Type::Primitive(PrimitiveType::String))
///     .delete_column("old_col");
/// let tx = action.apply(tx).unwrap();
/// let table = tx.commit(&catalog).await.unwrap();
/// ```
pub struct UpdateSchemaAction {
    additions: Vec<PendingAdd>,
    deletes: Vec<String>,
    auto_assign_ids: bool,
}

impl UpdateSchemaAction {
    /// Creates a new empty `UpdateSchemaAction`.
    pub(crate) fn new() -> Self {
        Self {
            additions: Vec::new(),
            deletes: Vec::new(),
            auto_assign_ids: true,
        }
    }

    // --- Root-level additions ---

    /// Add a `NestedFieldRef` column to the table root.
    pub fn add_field(self, field: NestedFieldRef) -> Self {
        self.add_field_internal(None, field)
    }

    /// Add an optional column to the table root.
    ///
    /// The field ID is a placeholder (`0`) and will be auto-assigned at commit time.
    pub fn add_column(self, name: impl ToString, field_type: Type) -> Self {
        self.add_field(Arc::new(NestedField::optional(0, name, field_type)))
    }

    /// Add an optional column with a doc string to the table root.
    ///
    /// The field ID is a placeholder (`0`) and will be auto-assigned at commit time.
    pub fn add_column_with_doc(
        self,
        name: impl ToString,
        field_type: Type,
        doc: impl ToString,
    ) -> Self {
        self.add_field(Arc::new(
            NestedField::optional(0, name, field_type).with_doc(doc),
        ))
    }

    /// Add a required column to the table root.
    ///
    /// An `initial_default` value is required per the Iceberg spec: it is used to populate
    /// this field for all records that were written before the field was added.
    /// The field ID is a placeholder (`0`) and will be auto-assigned at commit time.
    pub fn add_required_column(
        self,
        name: impl ToString,
        field_type: Type,
        initial_default: Literal,
    ) -> Self {
        self.add_field(Arc::new(
            NestedField::required(0, name, field_type).with_initial_default(initial_default),
        ))
    }

    /// Add a required column with a doc string to the table root.
    ///
    /// An `initial_default` value is required per the Iceberg spec: it is used to populate
    /// this field for all records that were written before the field was added.
    /// The field ID is a placeholder (`0`) and will be auto-assigned at commit time.
    pub fn add_required_column_with_doc(
        self,
        name: impl ToString,
        field_type: Type,
        initial_default: Literal,
        doc: impl ToString,
    ) -> Self {
        self.add_field(Arc::new(
            NestedField::required(0, name, field_type)
                .with_initial_default(initial_default)
                .with_doc(doc),
        ))
    }

    // --- Nested additions ---

    /// Add a `NestedFieldRef` column under a parent struct identified by name.
    ///
    /// If the parent is a map, the column is added to the map value's struct.
    /// If the parent is a list, the column is added to the list element's struct.
    pub fn add_field_to(self, parent: impl ToString, field: NestedFieldRef) -> Self {
        self.add_field_internal(Some(parent.to_string()), field)
    }

    /// Add an optional column under a parent struct identified by name.
    ///
    /// The `parent` can be a dotted path (e.g. `"person"` or `"person.address"`).
    /// If the parent is a map, the column is added to the map value's struct.
    /// If the parent is a list, the column is added to the list element's struct.
    /// The field ID is a placeholder (`0`) and will be auto-assigned at commit time.
    pub fn add_column_to(
        self,
        parent: impl ToString,
        name: impl ToString,
        field_type: Type,
    ) -> Self {
        self.add_field_to(parent, Arc::new(NestedField::optional(0, name, field_type)))
    }

    /// Add an optional column with a doc string under a parent struct.
    ///
    /// See [`add_column_to`](Self::add_column_to) for parent path details.
    pub fn add_column_to_with_doc(
        self,
        parent: impl ToString,
        name: impl ToString,
        field_type: Type,
        doc: impl ToString,
    ) -> Self {
        self.add_field_to(
            parent,
            Arc::new(NestedField::optional(0, name, field_type).with_doc(doc)),
        )
    }

    /// Add a required column under a parent struct.
    ///
    /// See [`add_column_to`](Self::add_column_to) for parent path details.
    /// An `initial_default` value is required per the Iceberg spec.
    pub fn add_required_column_to(
        self,
        parent: impl ToString,
        name: impl ToString,
        field_type: Type,
        initial_default: Literal,
    ) -> Self {
        self.add_field_to(
            parent,
            Arc::new(
                NestedField::required(0, name, field_type).with_initial_default(initial_default),
            ),
        )
    }

    /// Add a required column with a doc string under a parent struct.
    ///
    /// See [`add_column_to`](Self::add_column_to) for parent path details.
    /// An `initial_default` value is required per the Iceberg spec.
    pub fn add_required_column_to_with_doc(
        self,
        parent: impl ToString,
        name: impl ToString,
        field_type: Type,
        initial_default: Literal,
        doc: impl ToString,
    ) -> Self {
        self.add_field_to(
            parent,
            Arc::new(
                NestedField::required(0, name, field_type)
                    .with_initial_default(initial_default)
                    .with_doc(doc),
            ),
        )
    }

    // --- Other builder methods ---

    /// Record a column deletion by name.
    ///
    /// At commit time, the column must exist in the current schema.
    pub fn delete_column(mut self, name: impl ToString) -> Self {
        self.deletes.push(name.to_string());
        self
    }

    /// Disable automatic field ID assignment. When disabled, the placeholder IDs
    /// provided in builder methods are used as-is.
    pub fn disable_id_auto_assignment(mut self) -> Self {
        self.auto_assign_ids = false;
        self
    }

    // --- Internal helpers ---

    fn add_field_internal(mut self, parent: Option<String>, field: NestedFieldRef) -> Self {
        self.additions.push(PendingAdd { parent, field });
        self
    }
}

// ---------------------------------------------------------------------------
// ID assignment helpers
// ---------------------------------------------------------------------------

/// Recursively assign fresh field IDs to a `NestedField` and all its nested sub-fields.
///
/// This follows the same recursive pattern as `ReassignFieldIds::reassign_ids_visit_type`
/// from `crate::spec::schema::id_reassigner`, but operates on new fields with placeholder
/// IDs rather than reassigning an existing schema. `ReassignFieldIds` cannot be used
/// directly here because it rejects duplicate old IDs (all new fields share placeholder
/// ID `0`).
fn assign_fresh_ids(field: &NestedField, next_id: &mut i32) -> NestedFieldRef {
    *next_id += 1;
    let new_id = *next_id;
    let new_type = assign_fresh_ids_to_type(&field.field_type, next_id);

    Arc::new(NestedField {
        id: new_id,
        name: field.name.clone(),
        required: field.required,
        field_type: Box::new(new_type),
        doc: field.doc.clone(),
        initial_default: field.initial_default.clone(),
        write_default: field.write_default.clone(),
    })
}

/// Recursively assign fresh field IDs to all nested fields within a `Type`.
fn assign_fresh_ids_to_type(field_type: &Type, next_id: &mut i32) -> Type {
    match field_type {
        Type::Primitive(_) => field_type.clone(),
        Type::Struct(struct_type) => {
            let new_fields: Vec<NestedFieldRef> = struct_type
                .fields()
                .iter()
                .map(|f| assign_fresh_ids(f, next_id))
                .collect();
            Type::Struct(StructType::new(new_fields))
        }
        Type::List(list_type) => {
            let new_element = assign_fresh_ids(&list_type.element_field, next_id);
            Type::List(ListType {
                element_field: new_element,
            })
        }
        Type::Map(map_type) => {
            let new_key = assign_fresh_ids(&map_type.key_field, next_id);
            let new_value = assign_fresh_ids(&map_type.value_field, next_id);
            Type::Map(MapType {
                key_field: new_key,
                value_field: new_value,
            })
        }
    }
}

// ---------------------------------------------------------------------------
// Parent path resolution
// ---------------------------------------------------------------------------

/// Resolve a parent path to the target struct's parent field ID and a reference
/// to its `StructType`.
///
/// If the parent is a map, navigates to the value field. If a list, navigates to
/// the element field. The final target must be a struct type.
fn resolve_parent_target<'a>(
    base_schema: &'a Schema,
    parent: &str,
) -> Result<(i32, &'a StructType)> {
    base_schema
        .field_by_name(parent)
        .ok_or_else(|| {
            Error::new(
                ErrorKind::PreconditionFailed,
                format!("Cannot add column: parent '{parent}' not found"),
            )
        })
        .and_then(|parent_field| match parent_field.field_type.as_ref() {
            Type::Struct(s) => Ok((parent_field.id, s)),
            Type::Map(m) => match m.value_field.field_type.as_ref() {
                Type::Struct(s) => Ok((m.value_field.id, s)),
                _ => Err(Error::new(
                    ErrorKind::PreconditionFailed,
                    format!("Cannot add column: map value of '{parent}' is not a struct"),
                )),
            },
            Type::List(l) => match l.element_field.field_type.as_ref() {
                Type::Struct(s) => Ok((l.element_field.id, s)),
                _ => Err(Error::new(
                    ErrorKind::PreconditionFailed,
                    format!("Cannot add column: list element of '{parent}' is not a struct"),
                )),
            },
            _ => Err(Error::new(
                ErrorKind::PreconditionFailed,
                format!("Cannot add column: parent '{parent}' is not a struct, map, or list"),
            )),
        })
}

// ---------------------------------------------------------------------------
// Schema tree rebuild
// ---------------------------------------------------------------------------

/// Rebuild a slice of fields, applying deletions and additions at every level,
/// plus any root-level additions keyed by `TABLE_ROOT_ID`.
fn rebuild_fields(
    fields: &[NestedFieldRef],
    adds: &HashMap<i32, Vec<NestedFieldRef>>,
    delete_ids: &HashSet<i32>,
    root_id: i32,
) -> Vec<NestedFieldRef> {
    fields
        .iter()
        .filter(|f| !delete_ids.contains(&f.id))
        .map(|f| rebuild_field(f, adds, delete_ids))
        .chain(adds.get(&root_id).into_iter().flatten().cloned())
        .collect()
}

/// Recursively rebuild a single field. If the field (or any descendant) is a struct
/// that has pending additions, those additions are appended to the struct's fields.
/// Fields whose IDs appear in `delete_ids` are filtered out at every struct level.
fn rebuild_field(
    field: &NestedFieldRef,
    adds: &HashMap<i32, Vec<NestedFieldRef>>,
    delete_ids: &HashSet<i32>,
) -> NestedFieldRef {
    match field.field_type.as_ref() {
        Type::Primitive(_) => field.clone(),
        Type::Struct(s) => {
            let new_fields = rebuild_fields(s.fields(), adds, delete_ids, field.id);
            Arc::new(NestedField {
                id: field.id,
                name: field.name.clone(),
                required: field.required,
                field_type: Box::new(Type::Struct(StructType::new(new_fields))),
                doc: field.doc.clone(),
                initial_default: field.initial_default.clone(),
                write_default: field.write_default.clone(),
            })
        }
        Type::List(l) => {
            let new_element = rebuild_field(&l.element_field, adds, delete_ids);
            Arc::new(NestedField {
                id: field.id,
                name: field.name.clone(),
                required: field.required,
                field_type: Box::new(Type::List(ListType {
                    element_field: new_element,
                })),
                doc: field.doc.clone(),
                initial_default: field.initial_default.clone(),
                write_default: field.write_default.clone(),
            })
        }
        Type::Map(m) => {
            let new_key = rebuild_field(&m.key_field, adds, delete_ids);
            let new_value = rebuild_field(&m.value_field, adds, delete_ids);
            Arc::new(NestedField {
                id: field.id,
                name: field.name.clone(),
                required: field.required,
                field_type: Box::new(Type::Map(MapType {
                    key_field: new_key,
                    value_field: new_value,
                })),
                doc: field.doc.clone(),
                initial_default: field.initial_default.clone(),
                write_default: field.write_default.clone(),
            })
        }
    }
}

// ---------------------------------------------------------------------------
// TransactionAction implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl TransactionAction for UpdateSchemaAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let base_schema = table.metadata().current_schema();
        let mut last_column_id = table.metadata().last_column_id();

        // --- 1. Validate deletes ---
        let delete_ids = self
            .deletes
            .iter()
            .map(|name: &String| {
                base_schema
                    .field_by_name(name)
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::PreconditionFailed,
                            format!("Cannot delete missing column: {name}"),
                        )
                    })
                    .and_then(|field| {
                        match base_schema
                            .identifier_field_ids()
                            .find(|id| *id == field.id)
                        {
                            Some(_) => Err(Error::new(
                                ErrorKind::PreconditionFailed,
                                format!("Cannot delete identifier field: {name}"),
                            )),
                            None => Ok(field.id),
                        }
                    })
            })
            .collect::<Result<HashSet<i32>>>()?;

        // --- 2. Resolve parents, validate additions, assign IDs, and group by parent ID ---
        // We assign IDs inline (before grouping) to preserve the caller's insertion order,
        // since HashMap iteration order is non-deterministic.
        let mut additions_by_parent: HashMap<i32, Vec<NestedFieldRef>> = HashMap::new();

        for pending in &self.additions {
            // Check that name does not contain ".".
            if pending.field.name.contains('.') {
                return Err(Error::new(
                    ErrorKind::PreconditionFailed,
                    format!(
                        "Cannot add column with ambiguous name: {}. Use the `add_column_to` method to add a column to a nested struct.",
                        pending.field.name
                    ),
                ));
            }

            // Required columns without an initial default need allow_incompatible_changes.
            if pending.field.required && pending.field.initial_default.is_none() {
                return Err(Error::new(
                    ErrorKind::PreconditionFailed,
                    format!(
                        "Incompatible change: cannot add required column without an initial default: {}",
                        pending.field.name
                    ),
                ));
            }

            let parent_id = match &pending.parent {
                None => {
                    // Root-level: check name conflict against root-level fields.
                    if let Some(existing) = base_schema.field_by_name(&pending.field.name)
                        && !delete_ids.contains(&existing.id)
                    {
                        return Err(Error::new(
                            ErrorKind::PreconditionFailed,
                            format!(
                                "Cannot add column, name already exists: {}",
                                pending.field.name
                            ),
                        ));
                    }
                    TABLE_ROOT_ID
                }
                Some(parent_path) => {
                    // Nested: resolve parent, check name conflict within parent struct.
                    let (parent_id, parent_struct) =
                        resolve_parent_target(base_schema, parent_path)?;

                    if parent_struct
                        .fields()
                        .iter()
                        .any(|f| f.name == pending.field.name && !delete_ids.contains(&f.id))
                    {
                        return Err(Error::new(
                            ErrorKind::PreconditionFailed,
                            format!(
                                "Cannot add column, name already exists in '{}': {}",
                                parent_path, pending.field.name
                            ),
                        ));
                    }

                    parent_id
                }
            };

            // Assign fresh IDs immediately, preserving insertion order.
            let field = if self.auto_assign_ids {
                assign_fresh_ids(&pending.field, &mut last_column_id)
            } else {
                pending.field.clone()
            };

            additions_by_parent
                .entry(parent_id)
                .or_default()
                .push(field);
        }

        // --- 4. Rebuild the schema tree with additions and deletions ---
        let new_fields = rebuild_fields(
            base_schema.as_struct().fields(),
            &additions_by_parent,
            &delete_ids,
            TABLE_ROOT_ID,
        );

        // --- 5. Build the new schema ---
        let schema = Schema::builder()
            .with_fields(new_fields)
            .with_identifier_field_ids(base_schema.identifier_field_ids())
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
    use std::io::BufReader;
    use std::sync::Arc;

    use as_any::Downcast;

    use crate::spec::{Literal, NestedField, PrimitiveType, StructType, TableMetadata, Type};
    use crate::table::Table;
    use crate::transaction::Transaction;
    use crate::transaction::action::{ApplyTransactionAction, TransactionAction};
    use crate::transaction::tests::make_v2_table;
    use crate::transaction::update_schema::UpdateSchemaAction;
    use crate::{ErrorKind, TableIdent, TableRequirement, TableUpdate};

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
            .file_io(crate::io::FileIOBuilder::new("memory").build().unwrap())
            .build()
            .unwrap()
    }

    // -----------------------------------------------------------------------
    // Existing root-level tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_add_column() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);

        let action = tx
            .update_schema()
            .add_column("new_col", Type::Primitive(PrimitiveType::Int));

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let requirements = action_commit.take_requirements();

        assert_eq!(updates.len(), 2);

        // Extract the new schema from the AddSchema update.
        let new_schema = match &updates[0] {
            TableUpdate::AddSchema { schema } => schema,
            other => panic!("expected AddSchema, got {:?}", other),
        };

        // The new field should have ID = last_column_id + 1 = 4.
        let new_field = new_schema
            .field_by_name("new_col")
            .expect("new_col should exist");
        assert_eq!(new_field.id, 4);
        assert!(!new_field.required);
        assert_eq!(*new_field.field_type, Type::Primitive(PrimitiveType::Int));
        assert!(new_field.doc.is_none());

        // Original fields should still be there.
        assert!(new_schema.field_by_name("x").is_some());
        assert!(new_schema.field_by_name("y").is_some());
        assert!(new_schema.field_by_name("z").is_some());

        assert_eq!(updates[1], TableUpdate::SetCurrentSchema { schema_id: -1 });

        // Verify requirement.
        assert_eq!(requirements.len(), 1);
        assert_eq!(requirements[0], TableRequirement::CurrentSchemaIdMatch {
            current_schema_id: table.metadata().current_schema().schema_id()
        });
    }

    #[tokio::test]
    async fn test_add_column_with_doc() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);

        let action = tx.update_schema().add_column_with_doc(
            "documented_col",
            Type::Primitive(PrimitiveType::String),
            "A documented column",
        );

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let new_schema = match &updates[0] {
            TableUpdate::AddSchema { schema } => schema,
            other => panic!("expected AddSchema, got {:?}", other),
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

        let action = tx.update_schema().add_required_column(
            "req_col",
            Type::Primitive(PrimitiveType::Int),
            Literal::int(0),
        );

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let new_schema = match &updates[0] {
            TableUpdate::AddSchema { schema } => schema,
            other => panic!("expected AddSchema, got {:?}", other),
        };

        let field = new_schema
            .field_by_name("req_col")
            .expect("req_col should exist");
        assert_eq!(field.id, 4);
        assert!(field.required);
        assert_eq!(field.initial_default, Some(Literal::int(0)));
    }

    #[tokio::test]
    async fn test_add_column_name_conflict_fails() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);

        // "x" already exists in the V2 test schema.
        let action = tx
            .update_schema()
            .add_column("x", Type::Primitive(PrimitiveType::Int));

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
            other => panic!("expected AddSchema, got {:?}", other),
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
            .add_column("w", Type::Primitive(PrimitiveType::Boolean));

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let new_schema = match &updates[0] {
            TableUpdate::AddSchema { schema } => schema,
            other => panic!("expected AddSchema, got {:?}", other),
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
            .add_column("z", Type::Primitive(PrimitiveType::Boolean));

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let new_schema = match &updates[0] {
            TableUpdate::AddSchema { schema } => schema,
            other => panic!("expected AddSchema, got {:?}", other),
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
            .add_column("new_col", Type::Primitive(PrimitiveType::Int))
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
        let action = tx.update_schema().add_column_to(
            "person",
            "email",
            Type::Primitive(PrimitiveType::String),
        );

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let new_schema = match &updates[0] {
            TableUpdate::AddSchema { schema } => schema,
            other => panic!("expected AddSchema, got {:?}", other),
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

        let action = tx.update_schema().add_column_to_with_doc(
            "person",
            "phone",
            Type::Primitive(PrimitiveType::String),
            "Phone number",
        );

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let new_schema = match &updates[0] {
            TableUpdate::AddSchema { schema } => schema,
            other => panic!("expected AddSchema, got {:?}", other),
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
        let action = tx.update_schema().add_column_to(
            "tags",
            "score",
            Type::Primitive(PrimitiveType::Double),
        );

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let new_schema = match &updates[0] {
            TableUpdate::AddSchema { schema } => schema,
            other => panic!("expected AddSchema, got {:?}", other),
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
        let action = tx.update_schema().add_column_to(
            "props",
            "version",
            Type::Primitive(PrimitiveType::Int),
        );

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let new_schema = match &updates[0] {
            TableUpdate::AddSchema { schema } => schema,
            other => panic!("expected AddSchema, got {:?}", other),
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

        let action = tx.update_schema().add_column_to(
            "nonexistent",
            "col",
            Type::Primitive(PrimitiveType::Int),
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
        let action =
            tx.update_schema()
                .add_column_to("x", "col", Type::Primitive(PrimitiveType::Int));

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
        let action = tx.update_schema().add_column_to(
            "person",
            "name",
            Type::Primitive(PrimitiveType::String),
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
            .add_column("root_col", Type::Primitive(PrimitiveType::Boolean))
            .add_column_to("person", "email", Type::Primitive(PrimitiveType::String));

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let new_schema = match &updates[0] {
            TableUpdate::AddSchema { schema } => schema,
            other => panic!("expected AddSchema, got {:?}", other),
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
        // Exercises the assign_fresh_ids bug fix: adding a new column whose TYPE
        // contains nested fields (e.g. a struct column). All sub-fields must receive
        // fresh IDs, not placeholder 0.
        let table = make_v2_table();
        let tx = Transaction::new(&table);

        let action = tx.update_schema().add_column(
            "address",
            Type::Struct(StructType::new(vec![
                NestedField::optional(0, "street", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(0, "city", Type::Primitive(PrimitiveType::String)).into(),
            ])),
        );

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let new_schema = match &updates[0] {
            TableUpdate::AddSchema { schema } => schema,
            other => panic!("expected AddSchema, got {:?}", other),
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
}
