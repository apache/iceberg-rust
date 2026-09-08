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

use super::utils::try_insert_field;
use super::*;

pub(crate) fn assign_fresh_ids(schema: Schema, base: &Schema, start_from: i32) -> Result<Schema> {
    let mut assigner = AssignFreshIds::new(&schema, base, start_from);
    let Schema {
        r#struct,
        schema_id,
        identifier_field_ids,
        alias_to_id,
        ..
    } = schema;
    let fields = assigner.assign_fields(r#struct.fields().to_vec())?;
    let identifier_field_ids = assigner.apply_to_identifier_fields(identifier_field_ids)?;
    let alias_to_id = assigner.apply_to_aliases(alias_to_id)?;

    Schema::builder()
        .with_schema_id(schema_id)
        .with_fields(fields)
        .with_identifier_field_ids(identifier_field_ids)
        .with_alias(alias_to_id)
        .build()
}

struct AssignFreshIds {
    next_field_id: i32,
    target_names: HashMap<i32, String>,
    base_ids: HashMap<String, i32>,
    old_to_new_id: HashMap<i32, i32>,
}

impl AssignFreshIds {
    fn new(target: &Schema, base: &Schema, start_from: i32) -> Self {
        Self {
            next_field_id: start_from,
            target_names: target.field_id_to_name_map().clone(),
            base_ids: base
                .field_id_to_name_map()
                .iter()
                .map(|(id, name)| (name.clone(), *id))
                .collect(),
            old_to_new_id: HashMap::new(),
        }
    }

    fn id_for(&mut self, old_id: i32) -> Result<i32> {
        if let Some(id) = self
            .target_names
            .get(&old_id)
            .and_then(|name| self.base_ids.get(name))
        {
            return Ok(*id);
        }

        let id = self.next_field_id;
        self.next_field_id = self.next_field_id.checked_add(1).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Field ID overflowed, cannot add more fields",
            )
        })?;
        Ok(id)
    }

    fn assign_fields(&mut self, fields: Vec<NestedFieldRef>) -> Result<Vec<NestedFieldRef>> {
        let outer_fields = fields
            .into_iter()
            .map(|field| {
                let new_id = self.id_for(field.id)?;
                try_insert_field(&mut self.old_to_new_id, field.id, new_id)?;
                Ok(Arc::new(Arc::unwrap_or_clone(field).with_id(new_id)))
            })
            .collect::<Result<Vec<_>>>()?;

        outer_fields
            .into_iter()
            .map(|field| {
                if field.field_type.is_primitive() {
                    Ok(field)
                } else {
                    let mut field = Arc::unwrap_or_clone(field);
                    *field.field_type = self.assign_type(*field.field_type)?;
                    Ok(Arc::new(field))
                }
            })
            .collect()
    }

    fn assign_type(&mut self, field_type: Type) -> Result<Type> {
        match field_type {
            Type::Primitive(primitive) => Ok(Type::Primitive(primitive)),
            Type::Struct(r#struct) => Ok(Type::Struct(StructType::new(
                self.assign_fields(r#struct.fields().to_vec())?,
            ))),
            Type::List(list) => {
                let new_id = self.id_for(list.element_field.id)?;
                self.old_to_new_id.insert(list.element_field.id, new_id);
                let mut element_field = Arc::unwrap_or_clone(list.element_field);
                element_field.id = new_id;
                *element_field.field_type = self.assign_type(*element_field.field_type)?;
                Ok(Type::List(ListType {
                    element_field: Arc::new(element_field),
                }))
            }
            Type::Map(map) => {
                let new_key_id = self.id_for(map.key_field.id)?;
                self.old_to_new_id.insert(map.key_field.id, new_key_id);
                let mut key_field = Arc::unwrap_or_clone(map.key_field);
                key_field.id = new_key_id;
                *key_field.field_type = self.assign_type(*key_field.field_type)?;

                let new_value_id = self.id_for(map.value_field.id)?;
                self.old_to_new_id.insert(map.value_field.id, new_value_id);
                let mut value_field = Arc::unwrap_or_clone(map.value_field);
                value_field.id = new_value_id;
                *value_field.field_type = self.assign_type(*value_field.field_type)?;

                Ok(Type::Map(MapType {
                    key_field: Arc::new(key_field),
                    value_field: Arc::new(value_field),
                }))
            }
            Type::Variant(variant) => Ok(Type::Variant(variant)),
        }
    }

    fn apply_to_identifier_fields(&self, field_ids: HashSet<i32>) -> Result<HashSet<i32>> {
        field_ids
            .into_iter()
            .map(|id| {
                self.old_to_new_id.get(&id).copied().ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Identifier Field ID {id} not found"),
                    )
                })
            })
            .collect()
    }

    fn apply_to_aliases(&self, aliases: BiHashMap<String, i32>) -> Result<BiHashMap<String, i32>> {
        aliases
            .into_iter()
            .map(|(name, id)| {
                self.old_to_new_id
                    .get(&id)
                    .copied()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!("Field with id {id} for alias {name} not found"),
                        )
                    })
                    .map(|new_id| (name, new_id))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_assign_fresh_ids_reuses_full_names_and_assigns_new_ids_in_java_order() {
        let base = Schema::builder()
            .with_fields(vec![
                NestedField::required(
                    1,
                    "nested",
                    Type::Struct(StructType::new(vec![
                        NestedField::optional(2, "a", Type::Primitive(PrimitiveType::Int)).into(),
                    ])),
                )
                .into(),
                NestedField::optional(
                    3,
                    "items",
                    Type::List(ListType::new(
                        NestedField::list_element(4, Type::Primitive(PrimitiveType::String), false)
                            .into(),
                    )),
                )
                .into(),
                NestedField::optional(
                    5,
                    "properties",
                    Type::Map(MapType::optional(
                        6,
                        Type::Primitive(PrimitiveType::String),
                        7,
                        Type::Primitive(PrimitiveType::Long),
                    )),
                )
                .into(),
                NestedField::required(8, "x", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(9, "dropped", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();
        let replacement = Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids([18])
            .with_alias(BiHashMap::from_iter([("a_alias".to_string(), 12)]))
            .with_fields(vec![
                NestedField::required(
                    10,
                    "nested",
                    Type::Struct(StructType::new(vec![
                        NestedField::optional(11, "b", Type::Primitive(PrimitiveType::Int)).into(),
                        NestedField::optional(12, "a", Type::Primitive(PrimitiveType::Int)).into(),
                    ])),
                )
                .into(),
                NestedField::optional(
                    13,
                    "items",
                    Type::List(ListType::new(
                        NestedField::list_element(
                            14,
                            Type::Primitive(PrimitiveType::String),
                            false,
                        )
                        .into(),
                    )),
                )
                .into(),
                NestedField::optional(
                    15,
                    "properties",
                    Type::Map(MapType::optional(
                        16,
                        Type::Primitive(PrimitiveType::String),
                        17,
                        Type::Primitive(PrimitiveType::Long),
                    )),
                )
                .into(),
                NestedField::required(18, "x", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(19, "z", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();

        let assigned = assign_fresh_ids(replacement, &base, 10).unwrap();

        assert_eq!(assigned.schema_id(), 1);
        assert_eq!(assigned.field_by_name("nested").unwrap().id, 1);
        assert_eq!(assigned.field_by_name("nested.a").unwrap().id, 2);
        assert_eq!(assigned.field_by_name("items").unwrap().id, 3);
        assert_eq!(assigned.field_by_name("items.element").unwrap().id, 4);
        assert_eq!(assigned.field_by_name("properties").unwrap().id, 5);
        assert_eq!(assigned.field_by_name("properties.key").unwrap().id, 6);
        assert_eq!(assigned.field_by_name("properties.value").unwrap().id, 7);
        assert_eq!(assigned.field_by_name("x").unwrap().id, 8);
        assert_eq!(assigned.field_by_name("z").unwrap().id, 10);
        assert_eq!(assigned.field_by_name("nested.b").unwrap().id, 11);
        assert_eq!(assigned.identifier_field_ids().collect::<Vec<_>>(), vec![8]);
        assert_eq!(assigned.field_by_alias("a_alias").unwrap().id, 2);
        assert_eq!(assigned.highest_field_id(), 11);
    }
}
