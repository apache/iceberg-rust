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

//! This module defines schema in iceberg.

use crate::spec::datatypes::{NestedField, StructType};

const DEFAULT_SCHEMA_ID: i32 = 0;

/// Defines schema in iceberg.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Schema {
    r#struct: StructType,
    schema_id: i32,
    highest_field_id: i32,
}

/// Schema builder.
pub struct SchemaBuilder {
    schema_id: i32,
    fields: Vec<NestedField>,
}

impl SchemaBuilder {
    /// Add fields to schem builder
    pub fn with_fields(mut self, fields: impl IntoIterator<Item = NestedField>) -> Self {
        self.fields.extend(fields.into_iter());
        self
    }

    /// Set schema id.
    pub fn with_schema_id(mut self, schema_id: i32) -> Self {
        self.schema_id = schema_id;
        self
    }

    /// Builds the schema.
    pub fn build(self) -> Schema {
        let highest_field_id = self.fields.iter().map(|f| f.id).max().unwrap_or(0);
        Schema {
            r#struct: StructType::new(self.fields),
            schema_id: self.schema_id,
            highest_field_id,
        }
    }
}

impl Schema {
    /// Create a schema builder.
    pub fn builder() -> SchemaBuilder {
        SchemaBuilder {
            schema_id: DEFAULT_SCHEMA_ID,
            fields: vec![],
        }
    }

    /// Get field by field id.
    pub fn field_by_id(&self, field_id: i32) -> Option<&NestedField> {
        self.r#struct.field_by_id(field_id)
    }

    /// Returns [`highest_field_id`].
    #[inline]
    pub fn highest_field_id(&self) -> i32 {
        self.highest_field_id
    }

    /// Returns [`schema_id`].
    #[inline]
    pub fn schema_id(&self) -> i32 {
        self.schema_id
    }
}

#[cfg(test)]
mod tests {
    use crate::spec::datatypes::{NestedField, PrimitiveType, Type};
    use crate::spec::schema::Schema;

    #[test]
    fn test_construct_schema() {
        let field1 = NestedField::required(1, "f1", Type::Primitive(PrimitiveType::Boolean));
        let field2 = NestedField::optional(2, "f2", Type::Primitive(PrimitiveType::Int));

        let schema = Schema::builder()
            .with_fields(vec![field1.clone()])
            .with_fields(vec![field2.clone()])
            .with_schema_id(3)
            .build();

        assert_eq!(3, schema.schema_id());
        assert_eq!(2, schema.highest_field_id());
        assert_eq!(Some(&field1), schema.field_by_id(1));
        assert_eq!(Some(&field2), schema.field_by_id(2));
        assert_eq!(None, schema.field_by_id(3));
    }
}
