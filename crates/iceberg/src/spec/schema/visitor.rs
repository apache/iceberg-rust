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

use super::*;

/// A post order schema visitor.
///
/// For order of methods called, please refer to [`visit_schema`].
pub trait SchemaVisitor {
    /// Return type of this visitor.
    type T;

    /// Called before struct field.
    fn before_struct_field(&mut self, _field: &NestedFieldRef) -> Result<()> {
        Ok(())
    }
    /// Called after struct field.
    fn after_struct_field(&mut self, _field: &NestedFieldRef) -> Result<()> {
        Ok(())
    }
    /// Called before list field.
    fn before_list_element(&mut self, _field: &NestedFieldRef) -> Result<()> {
        Ok(())
    }
    /// Called after list field.
    fn after_list_element(&mut self, _field: &NestedFieldRef) -> Result<()> {
        Ok(())
    }
    /// Called before map key field.
    fn before_map_key(&mut self, _field: &NestedFieldRef) -> Result<()> {
        Ok(())
    }
    /// Called after map key field.
    fn after_map_key(&mut self, _field: &NestedFieldRef) -> Result<()> {
        Ok(())
    }
    /// Called before map value field.
    fn before_map_value(&mut self, _field: &NestedFieldRef) -> Result<()> {
        Ok(())
    }
    /// Called after map value field.
    fn after_map_value(&mut self, _field: &NestedFieldRef) -> Result<()> {
        Ok(())
    }

    /// Called after schema's type visited.
    fn schema(&mut self, schema: &Schema, value: Self::T) -> Result<Self::T>;
    /// Called after struct's field type visited.
    fn field(&mut self, field: &NestedFieldRef, value: Self::T) -> Result<Self::T>;
    /// Called after struct's fields visited.
    fn r#struct(&mut self, r#struct: &StructType, results: Vec<Self::T>) -> Result<Self::T>;
    /// Called after list fields visited.
    fn list(&mut self, list: &ListType, value: Self::T) -> Result<Self::T>;
    /// Called after map's key and value fields visited.
    fn map(&mut self, map: &MapType, key_value: Self::T, value: Self::T) -> Result<Self::T>;
    /// Called when see a primitive type.
    fn primitive(&mut self, p: &PrimitiveType) -> Result<Self::T>;
}

/// Visiting a type in post order.
pub fn visit_type<V: SchemaVisitor>(r#type: &Type, visitor: &mut V) -> Result<V::T> {
    match r#type {
        Type::Primitive(p) => visitor.primitive(p),
        Type::List(list) => {
            visitor.before_list_element(&list.element_field)?;
            let value = visit_type(&list.element_field.field_type, visitor)?;
            visitor.after_list_element(&list.element_field)?;
            visitor.list(list, value)
        }
        Type::Map(map) => {
            let key_result = {
                visitor.before_map_key(&map.key_field)?;
                let ret = visit_type(&map.key_field.field_type, visitor)?;
                visitor.after_map_key(&map.key_field)?;
                ret
            };

            let value_result = {
                visitor.before_map_value(&map.value_field)?;
                let ret = visit_type(&map.value_field.field_type, visitor)?;
                visitor.after_map_value(&map.value_field)?;
                ret
            };

            visitor.map(map, key_result, value_result)
        }
        Type::Struct(s) => visit_struct(s, visitor),
    }
}

/// Visit struct type in post order.
pub fn visit_struct<V: SchemaVisitor>(s: &StructType, visitor: &mut V) -> Result<V::T> {
    let mut results = Vec::with_capacity(s.fields().len());
    for field in s.fields() {
        visitor.before_struct_field(field)?;
        let result = visit_type(&field.field_type, visitor)?;
        visitor.after_struct_field(field)?;
        let result = visitor.field(field, result)?;
        results.push(result);
    }

    visitor.r#struct(s, results)
}

/// Visit schema in post order.
pub fn visit_schema<V: SchemaVisitor>(schema: &Schema, visitor: &mut V) -> Result<V::T> {
    let result = visit_struct(&schema.r#struct, visitor)?;
    visitor.schema(schema, result)
}

/// A post order schema visitor with partner.
///
/// For order of methods called, please refer to [`visit_schema_with_partner`].
pub trait SchemaWithPartnerVisitor<P> {
    /// Return type of this visitor.
    type T;

    /// Called before struct field.
    fn before_struct_field(&mut self, _field: &NestedFieldRef, _partner: &P) -> Result<()> {
        Ok(())
    }
    /// Called after struct field.
    fn after_struct_field(&mut self, _field: &NestedFieldRef, _partner: &P) -> Result<()> {
        Ok(())
    }
    /// Called before list field.
    fn before_list_element(&mut self, _field: &NestedFieldRef, _partner: &P) -> Result<()> {
        Ok(())
    }
    /// Called after list field.
    fn after_list_element(&mut self, _field: &NestedFieldRef, _partner: &P) -> Result<()> {
        Ok(())
    }
    /// Called before map key field.
    fn before_map_key(&mut self, _field: &NestedFieldRef, _partner: &P) -> Result<()> {
        Ok(())
    }
    /// Called after map key field.
    fn after_map_key(&mut self, _field: &NestedFieldRef, _partner: &P) -> Result<()> {
        Ok(())
    }
    /// Called before map value field.
    fn before_map_value(&mut self, _field: &NestedFieldRef, _partner: &P) -> Result<()> {
        Ok(())
    }
    /// Called after map value field.
    fn after_map_value(&mut self, _field: &NestedFieldRef, _partner: &P) -> Result<()> {
        Ok(())
    }

    /// Called before every type, if this function return `Some`, the following visiting will be skipped.
    /// This function used to implement early return.
    fn visit_type_before(&mut self, _ty: &Type, _partner: &P) -> Result<Option<Self::T>> {
        return Ok(None);
    }

    /// Called after schema's type visited.
    fn schema(&mut self, schema: &Schema, partner: &P, value: Self::T) -> Result<Self::T>;
    /// Called after struct's field type visited.
    fn field(&mut self, field: &NestedFieldRef, partner: &P, value: Self::T) -> Result<Self::T>;
    /// Called after struct's fields visited.
    fn r#struct(
        &mut self,
        r#struct: &StructType,
        partner: &P,
        results: Vec<Self::T>,
    ) -> Result<Self::T>;
    /// Called after list fields visited.
    fn list(&mut self, list: &ListType, partner: &P, value: Vec<Self::T>) -> Result<Self::T>;
    /// Called after map's key and value fields visited.
    fn map(
        &mut self,
        map: &MapType,
        partner: &P,
        key_value: Vec<Self::T>,
        value: Vec<Self::T>,
    ) -> Result<Self::T>;
    /// Called when see a primitive type.
    fn primitive(&mut self, p: &PrimitiveType, partner: &P) -> Result<Self::T>;
}

/// Accessor used to get child partner from parent partner.
pub trait PartnerAccessor<P> {
    /// List partner iterator.
    type L: ListPartnerIterator<P>;
    /// Map partner iterator.
    type M: MapPartnerIterator<P>;

    /// Get the struct partner from schema partner.
    fn struct_parner<'a>(&self, schema_partner: &'a P) -> Result<&'a P>;
    /// Get the field partner from struct partner.
    fn field_partner<'a>(&self, struct_partner: &'a P, field_id: i32, field: &str)
        -> Result<&'a P>;
    /// Get the list element partner from list partner.
    fn list_element_partner<'a>(&self, list_partner: &'a P) -> Result<Self::L>;
    /// Get the map key partner from map partner.
    fn map_element_partner<'a>(&self, map_partner: &'a P) -> Result<Self::M>;
}

/// Iterator for list partner.
pub trait ListPartnerIterator<P> {
    /// Get the next partner.
    fn next(&mut self) -> Option<P>;
}

/// Iterator for map partner.
pub trait MapPartnerIterator<P> {
    /// Get the next partner.
    fn next(&mut self) -> Option<(P, P)>;
}

/// Visiting a type in post order.
pub fn visit_type_with_partner<P, V: SchemaWithPartnerVisitor<P>, A: PartnerAccessor<P>>(
    r#type: &Type,
    partner: &P,
    visitor: &mut V,
    accessor: &A,
) -> Result<V::T> {
    if let Some(res) = visitor.visit_type_before(r#type, partner)? {
        return Ok(res);
    }
    match r#type {
        Type::Primitive(p) => visitor.primitive(p, partner),
        Type::List(list) => {
            let mut results = Vec::new();
            let mut list_element_partner_iter = accessor.list_element_partner(partner)?;
            if let Some(list_element_partner) = list_element_partner_iter.next() {
                visitor.before_list_element(&list.element_field, &list_element_partner)?;
                let value = visit_type_with_partner(
                    &list.element_field.field_type,
                    &list_element_partner,
                    visitor,
                    accessor,
                )?;
                visitor.after_list_element(&list.element_field, &list_element_partner)?;
                results.push(value);
            }
            visitor.list(list, partner, results)
        }
        Type::Map(map) => {
            let mut k_results = Vec::new();
            let mut v_results = Vec::new();
            let mut kv_partner_iter = accessor.map_element_partner(partner)?;
            if let Some((k_partner, v_partner)) = kv_partner_iter.next() {
                let key_result = {
                    visitor.before_map_key(&map.key_field, &k_partner)?;
                    let ret = visit_type_with_partner(
                        &map.key_field.field_type,
                        &k_partner,
                        visitor,
                        accessor,
                    )?;
                    visitor.after_map_key(&map.key_field, &k_partner)?;
                    ret
                };

                let value_result = {
                    visitor.before_map_value(&map.value_field, &v_partner)?;
                    let ret = visit_type_with_partner(
                        &map.value_field.field_type,
                        &v_partner,
                        visitor,
                        accessor,
                    )?;
                    visitor.after_map_value(&map.value_field, &v_partner)?;
                    ret
                };

                k_results.push(key_result);
                v_results.push(value_result);
            }

            visitor.map(map, partner, k_results, v_results)
        }
        Type::Struct(s) => visit_struct_with_partner(s, partner, visitor, accessor),
    }
}

/// Visit struct type in post order.
pub fn visit_struct_with_partner<P, V: SchemaWithPartnerVisitor<P>, A: PartnerAccessor<P>>(
    s: &StructType,
    partner: &P,
    visitor: &mut V,
    accessor: &A,
) -> Result<V::T> {
    if let Some(res) = visitor.visit_type_before(&Type::Struct(s.clone()), partner)? {
        return Ok(res);
    }
    let mut results = Vec::with_capacity(s.fields().len());
    for field in s.fields() {
        let field_partner = accessor.field_partner(partner, field.id, &field.name)?;
        visitor.before_struct_field(field, field_partner)?;
        let result = visit_type_with_partner(&field.field_type, field_partner, visitor, accessor)?;
        visitor.after_struct_field(field, field_partner)?;
        let result = visitor.field(field, field_partner, result)?;
        results.push(result);
    }

    visitor.r#struct(s, partner, results)
}

/// Visit schema in post order.
pub fn visit_schema_with_partner<P, V: SchemaWithPartnerVisitor<P>, A: PartnerAccessor<P>>(
    schema: &Schema,
    partner: &P,
    visitor: &mut V,
    accessor: &A,
) -> Result<V::T> {
    let result = visit_struct_with_partner(
        &schema.r#struct,
        accessor.struct_parner(partner)?,
        visitor,
        accessor,
    )?;
    visitor.schema(schema, partner, result)
}