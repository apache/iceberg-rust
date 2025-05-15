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

use crate::spec::{index_parents, PrimitiveType, Schema, TableMetadataRef, Type};
use crate::transaction::{Arc, Transaction};
use crate::TableCommit;

pub struct SchemaUpdateAction<'a> {
    tx: Transaction<'a>,
    base: TableMetadataRef,
    schema: Arc<Schema>,
    last_column_id: i32,
    id_to_parent: HashMap<i32, i32>,
    identifier_field_names: HashSet<String>,
}

impl<'a> SchemaUpdateAction<'a> {
    pub fn new(tx: Transaction<'a>) -> Self {
        let base = tx.current().metadata_ref();
        let schema = base.schemas.get(&base.current_schema_id).unwrap().clone();
        let last_column_id = base.last_column_id;
        let id_to_parent = index_parents(schema.as_struct()).unwrap();
        let identifier_field_names = schema.identifier_field_names();
        Self {
            tx,
            base,
            schema,
            last_column_id,
            id_to_parent,
            identifier_field_names,
        }
    }

    pub fn add_column(self, parent: String, name: String, r#type: Type, doc: String) -> Self {
        self
    }

    pub fn add_required_column(
        self,
        parent: String,
        name: String,
        r#type: Type,
        doc: String,
    ) -> Self {
        self
    }

    pub fn delete_column(self, name: String) -> Self {
        self
    }

    pub fn rename_column(self, name: String, new_name: String) -> Self {
        self
    }

    pub fn require_column(self, name: String) -> Self {
        self
    }

    pub fn make_column_optional(self, name: String) -> Self {
        self
    }

    pub fn update_column(self, name: String, new_type: PrimitiveType) -> Self {
        self
    }

    pub fn update_column_doc(self, name: String, doc: String) -> Self {
        self
    }

    pub fn move_first(self, name: String) -> Self {
        self
    }

    pub fn move_before(self, name: String, before_name: String) -> Self {
        self
    }

    pub fn move_after(self, name: String, after_name: String) -> Self {
        self
    }

    pub fn union_by_name_with(self, new_schema: Schema) -> Self {
        self
    }

    pub fn set_identifier_fields(self, identifier_field_names: Vec<String>) -> Self {
        self
    }

    pub fn case_sensitive(self, case_sensitivity: bool) -> Self {
        self
    }

    pub fn apply(self) -> Schema {
        Schema::builder().build().unwrap()
    }

    pub fn commit(self) -> () {
        // TODO apply changes to metadata first

        // TODO fix this
        // let table_commit = TableCommit::builder()
        //     .ident(self.tx.current().identifier().clone())
        //     .updates(self.updates)
        //     .requirements(self.requirements)
    }
}
