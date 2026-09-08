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

mod apply;

#[cfg(test)]
mod tests;

use typed_builder::TypedBuilder;

use crate::spec::{Literal, Type};

/// Declarative specification for adding a column in an [`UpdateSchemaAction`].
///
/// Use helper constructors such as [`AddColumn::optional`] and [`AddColumn::required`],
/// optionally combined with the builder's `parent` and `doc` setters via
/// [`AddColumn::builder`], then pass the value to [`UpdateSchemaAction::add_column`].
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
///     .add_column(AddColumn::optional("new_col", Type::Primitive(PrimitiveType::Int)))
///     .add_column(
///         AddColumn::optional("email", Type::Primitive(PrimitiveType::String))
///             .with_parent("person")
///     )
///     .delete_column("old_col");
/// let tx = action.apply(tx).unwrap();
/// let table = tx.commit(&catalog).await.unwrap();
/// ```
pub struct UpdateSchemaAction {
    additions: Vec<AddColumn>,
    deletes: Vec<String>,
}

impl UpdateSchemaAction {
    /// Creates a new empty `UpdateSchemaAction`.
    pub(crate) fn new() -> Self {
        Self {
            additions: Vec::new(),
            deletes: Vec::new(),
        }
    }

    // --- Root-level additions ---

    /// Add a column to the table schema.
    ///
    /// To add a root-level column, leave `AddColumn::parent` as `None`.
    /// For nested additions, set a parent path.
    /// If the parent resolves to a map/list, the column is added to map value/list element.
    pub fn add_column(mut self, add_column: AddColumn) -> Self {
        self.additions.push(add_column);
        self
    }

    // --- Other builder methods ---

    /// Record a column deletion by name.
    ///
    /// At commit time, the column must exist in the current schema.
    pub fn delete_column(mut self, name: impl ToString) -> Self {
        self.deletes.push(name.to_string());
        self
    }
}
