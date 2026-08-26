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

use typed_builder::TypedBuilder;

use crate::spec::{Literal, NestedField, NestedFieldRef, PrimitiveType, Schema, Type};

mod apply;
mod metadata;
#[cfg(test)]
mod tests;

// Default ID for a new column. This will be re-assigned to a fresh ID at commit time.
const DEFAULT_FIELD_ID: i32 = 0;

/// Declarative specification for adding a column in [`UpdateSchemaAction`].
///
/// Use helper constructors such as [`AddColumn::optional`] and [`AddColumn::required`],
/// optionally setting `parent` and `doc` through [`AddColumn::builder`], then pass the value to
/// [`UpdateSchemaAction::add_column`].
#[derive(TypedBuilder)]
pub struct AddColumn {
    #[builder(default = None, setter(strip_option, into))]
    parent: Option<String>,
    #[builder(setter(into))]
    name: String,
    #[builder(default = false)]
    required: bool,
    field_type: Type,
    #[builder(default = None, setter(strip_option, into))]
    doc: Option<String>,
    #[builder(default = None, setter(strip_option))]
    initial_default: Option<Literal>,
    #[builder(default = None, setter(strip_option))]
    write_default: Option<Literal>,
}

impl AddColumn {
    /// Create a root-level optional column specification.
    pub fn optional(name: impl ToString, field_type: Type) -> Self {
        Self::builder()
            .name(name.to_string())
            .field_type(field_type)
            .required(false)
            .build()
    }

    /// Create a root-level required column specification.
    pub fn required(name: impl ToString, field_type: Type, initial_default: Literal) -> Self {
        Self::builder()
            .name(name.to_string())
            .field_type(field_type)
            .required(true)
            .initial_default(initial_default.clone())
            .write_default(initial_default)
            .build()
    }

    fn to_nested_field(&self) -> NestedFieldRef {
        let mut field = NestedField::new(
            DEFAULT_FIELD_ID,
            self.name.clone(),
            self.field_type.clone(),
            self.required,
        );

        field.doc = self.doc.clone();
        field.initial_default = self.initial_default.clone();
        field.write_default = self.write_default.clone();
        Arc::new(field)
    }
}

/// Schema evolution API modeled after Apache Iceberg Java's `SchemaUpdate` implementation.
///
/// Operations are replayed against the latest table metadata on every transaction commit attempt.
/// This keeps field-id assignment and validation correct when the transaction is retried after a
/// concurrent commit.
///
/// # Example
///
/// ```ignore
/// let tx = Transaction::new(&table);
/// let action = tx.update_schema()
///     .add_column(AddColumn::optional("new_col", Type::Primitive(PrimitiveType::Int)))
///     .add_column(
///         AddColumn::builder()
///             .parent("person")
///             .name("email")
///             .field_type(Type::Primitive(PrimitiveType::String))
///             .build()
///     )
///     .rename_column("old_col", "legacy_col")
///     .move_column_first("new_col");
/// let tx = action.apply(tx).unwrap();
/// let table = tx.commit(&catalog).await.unwrap();
/// ```
pub struct UpdateSchemaAction {
    operations: Vec<SchemaOperation>,
    allow_incompatible_changes: bool,
    case_sensitive: bool,
    identifier_field_names: Option<HashSet<String>>,
}

enum SchemaOperation {
    Add(AddColumn),
    Delete(String),
    Rename {
        name: String,
        new_name: String,
    },
    SetRequired {
        name: String,
        required: bool,
    },
    UpdateType {
        name: String,
        new_type: PrimitiveType,
    },
    UpdateDoc {
        name: String,
        doc: Option<String>,
    },
    UpdateDefault {
        name: String,
        default: Option<Literal>,
    },
    Move {
        name: String,
        position: MovePosition,
    },
    UnionByName(Schema),
}

enum MovePosition {
    First,
    Before(String),
    After(String),
}

impl UpdateSchemaAction {
    /// Creates a new empty `UpdateSchemaAction`.
    pub(crate) fn new() -> Self {
        Self {
            operations: Vec::new(),
            allow_incompatible_changes: false,
            case_sensitive: true,
            identifier_field_names: None,
        }
    }

    // --- Root-level additions ---

    /// Add a column to the table schema.
    ///
    /// To add a root-level column, leave `AddColumn::parent` as `None`.
    /// For nested additions, set `parent` with [`AddColumn::builder`].
    /// If the parent resolves to a map/list, the column is added to map value/list element.
    pub fn add_column(mut self, add_column: AddColumn) -> Self {
        self.operations.push(SchemaOperation::Add(add_column));
        self
    }

    // --- Other builder methods ---

    /// Record a column deletion by name.
    ///
    /// At commit time, the column must exist in the current schema.
    pub fn delete_column(mut self, name: impl ToString) -> Self {
        self.operations
            .push(SchemaOperation::Delete(name.to_string()));
        self
    }

    /// Rename a column while preserving its field ID and all other metadata.
    ///
    /// `new_name` is an unqualified leaf name. Nested columns are selected using a dotted `name`.
    pub fn rename_column(mut self, name: impl ToString, new_name: impl ToString) -> Self {
        self.operations.push(SchemaOperation::Rename {
            name: name.to_string(),
            new_name: new_name.to_string(),
        });
        self
    }

    /// Change an optional column to required.
    ///
    /// This is rejected unless the column was added with an initial default or
    /// [`allow_incompatible_changes`](Self::allow_incompatible_changes) is enabled.
    pub fn require_column(mut self, name: impl ToString) -> Self {
        self.operations.push(SchemaOperation::SetRequired {
            name: name.to_string(),
            required: true,
        });
        self
    }

    /// Change a required column to optional.
    pub fn make_column_optional(mut self, name: impl ToString) -> Self {
        self.operations.push(SchemaOperation::SetRequired {
            name: name.to_string(),
            required: false,
        });
        self
    }

    /// Promote a primitive column to `new_type`.
    ///
    /// Iceberg permits `int` to `long`, `float` to `double`, and decimal precision widening at
    /// an unchanged scale.
    pub fn update_column_type(mut self, name: impl ToString, new_type: PrimitiveType) -> Self {
        self.operations.push(SchemaOperation::UpdateType {
            name: name.to_string(),
            new_type,
        });
        self
    }

    /// Set or clear a column's documentation string.
    pub fn update_column_doc(mut self, name: impl ToString, doc: Option<String>) -> Self {
        self.operations.push(SchemaOperation::UpdateDoc {
            name: name.to_string(),
            doc,
        });
        self
    }

    /// Set or clear a column's write default.
    ///
    /// Updating a default does not change the initial default used for rows written before the
    /// column was added.
    pub fn update_column_default(mut self, name: impl ToString, default: Option<Literal>) -> Self {
        self.operations.push(SchemaOperation::UpdateDefault {
            name: name.to_string(),
            default,
        });
        self
    }

    /// Move a column to the first position in its containing struct.
    pub fn move_column_first(mut self, name: impl ToString) -> Self {
        self.operations.push(SchemaOperation::Move {
            name: name.to_string(),
            position: MovePosition::First,
        });
        self
    }

    /// Move a column immediately before a sibling column.
    pub fn move_column_before(mut self, name: impl ToString, before_name: impl ToString) -> Self {
        self.operations.push(SchemaOperation::Move {
            name: name.to_string(),
            position: MovePosition::Before(before_name.to_string()),
        });
        self
    }

    /// Move a column immediately after a sibling column.
    pub fn move_column_after(mut self, name: impl ToString, after_name: impl ToString) -> Self {
        self.operations.push(SchemaOperation::Move {
            name: name.to_string(),
            position: MovePosition::After(after_name.to_string()),
        });
        self
    }

    /// Add and safely evolve columns to form a union with `new_schema`, matched by name.
    pub fn union_by_name(mut self, new_schema: Schema) -> Self {
        self.operations
            .push(SchemaOperation::UnionByName(new_schema));
        self
    }

    /// Replace the table's identifier fields with the supplied column names.
    pub fn set_identifier_fields<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: ToString,
    {
        self.identifier_field_names =
            Some(names.into_iter().map(|name| name.to_string()).collect());
        self
    }

    /// Resolve column names without regard to case when `case_sensitive` is false.
    pub fn case_sensitive(mut self, case_sensitive: bool) -> Self {
        self.case_sensitive = case_sensitive;
        self
    }

    /// Permit schema changes that may be incompatible with older data files.
    pub fn allow_incompatible_changes(mut self) -> Self {
        self.allow_incompatible_changes = true;
        self
    }
}
