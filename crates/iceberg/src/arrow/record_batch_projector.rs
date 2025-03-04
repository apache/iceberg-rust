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
use std::sync::Arc;

use arrow_array::{
    make_array, Array, ArrayRef, BinaryArray, BooleanArray, FixedSizeListArray, Float32Array,
    Float64Array, Int32Array, Int64Array, LargeListArray, ListArray, MapArray, RecordBatch,
    RecordBatchOptions, StringArray, StructArray,
};
use arrow_buffer::NullBuffer;
use arrow_cast::cast;
use arrow_schema::{DataType, Field, Fields, Schema as ArrowSchema};

use super::type_to_arrow_type;
use crate::error::Result;
use crate::spec::{
    visit_struct_with_partner, Literal, MapType, NestedField, PartnerAccessor, PrimitiveLiteral,
    PrimitiveType, Schema, SchemaWithPartnerVisitor, StructType, Type,
};
use crate::{Error, ErrorKind};

/// This accessor used to search the field index path for each field in the schema.
///
/// # Limit of this accessor:
/// - The accessor will not search the key field of the map type or the
///   value field of the map type. It will can search the map field itself.
/// - The accessor will not search the element field of the list type. It
///   will only search the list field itself.
struct FieldIndexPathAccessor<F> {
    field_id_fetch_func: F,
    // For primitive field, if the field not found and this flag is true, the accessor will
    // return the default primitive type for the field rather than raise an error.
    // This is used for the default value process in iceberg.
    allow_default_primitive: bool,
}

impl<F> FieldIndexPathAccessor<F>
where F: Fn(&Field) -> Result<i32>
{
    pub fn new(field_id_fetch_func: F, ignore_not_found: bool) -> Self {
        Self {
            field_id_fetch_func,
            allow_default_primitive: ignore_not_found,
        }
    }

    fn fetch_field_index_path(
        &self,
        fields: &Fields,
        index_vec: &mut Vec<usize>,
        target_field_id: i32,
    ) -> Result<Option<DataType>> {
        for (pos, field) in fields.iter().enumerate() {
            let id = (self.field_id_fetch_func)(field)?;
            if target_field_id == id {
                index_vec.push(pos);
                return Ok(Some(field.data_type().clone()));
            }
            if let DataType::Struct(inner) = field.data_type() {
                if let Some(res) = self.fetch_field_index_path(inner, index_vec, target_field_id)? {
                    index_vec.push(pos);
                    return Ok(Some(res));
                }
            }
        }
        Ok(None)
    }
}

#[derive(Clone, Debug)]
enum FieldPath {
    IndexPath(Vec<usize>, DataType),
    Default(DataType),
}

impl FieldPath {
    fn data_type(&self) -> &DataType {
        match self {
            FieldPath::IndexPath(_, data_type) => data_type,
            FieldPath::Default(data_type) => data_type,
        }
    }
}

impl<F: Fn(&Field) -> Result<i32>> PartnerAccessor<FieldPath> for FieldIndexPathAccessor<F> {
    fn struct_parner(&self, schema_partner: &FieldPath) -> Result<FieldPath> {
        if !matches!(schema_partner.data_type(), DataType::Struct(_)) {
            return Err(Error::new(ErrorKind::Unexpected, "Field is not a struct"));
        }
        Ok(schema_partner.clone())
    }

    fn field_partner(
        &self,
        struct_partner: &FieldPath,
        field: &crate::spec::NestedField,
    ) -> Result<FieldPath> {
        let DataType::Struct(struct_fields) = &struct_partner.data_type() else {
            return Err(Error::new(ErrorKind::Unexpected, "Field is not a struct"));
        };
        let mut index_path = vec![];
        let Some(field) = self.fetch_field_index_path(struct_fields, &mut index_path, field.id)?
        else {
            if self.allow_default_primitive && field.field_type.is_primitive() {
                return Ok(FieldPath::Default(type_to_arrow_type(&field.field_type)?));
            } else {
                return Err(Error::new(ErrorKind::Unexpected, "Field not found")
                    .with_context("target_id", field.id.to_string())
                    .with_context("struct fields", format!("{:?}", struct_fields)));
            }
        };
        Ok(FieldPath::IndexPath(index_path, field))
    }

    fn list_element_partner(&self, list_partner: &FieldPath) -> Result<FieldPath> {
        match &list_partner.data_type() {
            DataType::List(field) => Ok(FieldPath::Default(field.data_type().clone())),
            DataType::LargeList(field) => Ok(FieldPath::Default(field.data_type().clone())),
            DataType::FixedSizeList(field, _) => Ok(FieldPath::Default(field.data_type().clone())),
            _ => Err(Error::new(ErrorKind::Unexpected, "Field is not a list")),
        }
    }

    fn map_key_partner(&self, map_partner: &FieldPath) -> Result<FieldPath> {
        let DataType::Map(field, _) = map_partner.data_type() else {
            return Err(Error::new(ErrorKind::Unexpected, "Field is not a map"));
        };
        let DataType::Struct(inner_struct_fields) = field.data_type() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "inner field of map is not a struct",
            ));
        };
        Ok(FieldPath::Default(
            inner_struct_fields[0].data_type().clone(),
        ))
    }

    fn map_value_partner(&self, map_partner: &FieldPath) -> Result<FieldPath> {
        let DataType::Map(field, _) = &map_partner.data_type() else {
            return Err(Error::new(ErrorKind::Unexpected, "Field is not a map"));
        };
        let DataType::Struct(inner_struct_fields) = field.data_type() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "inner field of map is not a struct",
            ));
        };
        Ok(FieldPath::Default(
            inner_struct_fields[1].data_type().clone(),
        ))
    }
}

/// This visitor combine with `FieldIndexPathAccessor` to search the field index path for each field in the schema
/// and collect them into a hashmap.
struct FieldIndexPathCollector {
    field_index_path_map: HashMap<i32, Vec<usize>>,
}

impl SchemaWithPartnerVisitor<FieldPath> for FieldIndexPathCollector {
    type T = ();

    fn schema(
        &mut self,
        _schema: &crate::spec::Schema,
        _partner: &FieldPath,
        _value: Self::T,
    ) -> Result<Self::T> {
        Ok(())
    }

    fn field(
        &mut self,
        field: &crate::spec::NestedFieldRef,
        partner: &FieldPath,
        _value: Self::T,
    ) -> Result<Self::T> {
        match partner {
            FieldPath::IndexPath(index_path, _) => {
                self.field_index_path_map
                    .insert(field.id, index_path.clone());
            }
            FieldPath::Default(_) => {
                // Ignore the default field
            }
        }
        Ok(())
    }

    fn r#struct(
        &mut self,
        _struct: &StructType,
        _partner: &FieldPath,
        _results: Vec<Self::T>,
    ) -> Result<Self::T> {
        Ok(())
    }

    fn list(
        &mut self,
        _list: &crate::spec::ListType,
        _partner: &FieldPath,
        _value: Self::T,
    ) -> Result<Self::T> {
        Ok(())
    }

    fn map(
        &mut self,
        _map: &crate::spec::MapType,
        _partner: &FieldPath,
        _key_value: Self::T,
        _value: Self::T,
    ) -> Result<Self::T> {
        Ok(())
    }

    fn primitive(
        &mut self,
        _p: &crate::spec::PrimitiveType,
        _partner: &FieldPath,
    ) -> Result<Self::T> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DefaultValueGenerator;

impl DefaultValueGenerator {
    fn create_column(
        &self,
        target_type: &PrimitiveType,
        prim_lit: &Option<PrimitiveLiteral>,
        num_rows: usize,
    ) -> Result<ArrayRef> {
        Ok(match (target_type, prim_lit) {
            (PrimitiveType::Boolean, Some(PrimitiveLiteral::Boolean(value))) => {
                Arc::new(BooleanArray::from(vec![*value; num_rows]))
            }
            (PrimitiveType::Boolean, None) => {
                let vals: Vec<Option<bool>> = vec![None; num_rows];
                Arc::new(BooleanArray::from(vals))
            }
            (PrimitiveType::Int, Some(PrimitiveLiteral::Int(value))) => {
                Arc::new(Int32Array::from(vec![*value; num_rows]))
            }
            (PrimitiveType::Int, None) => {
                let vals: Vec<Option<i32>> = vec![None; num_rows];
                Arc::new(Int32Array::from(vals))
            }
            (PrimitiveType::Long, Some(PrimitiveLiteral::Long(value))) => {
                Arc::new(Int64Array::from(vec![*value; num_rows]))
            }
            (PrimitiveType::Long, None) => {
                let vals: Vec<Option<i64>> = vec![None; num_rows];
                Arc::new(Int64Array::from(vals))
            }
            (PrimitiveType::Float, Some(PrimitiveLiteral::Float(value))) => {
                Arc::new(Float32Array::from(vec![value.0; num_rows]))
            }
            (PrimitiveType::Float, None) => {
                let vals: Vec<Option<f32>> = vec![None; num_rows];
                Arc::new(Float32Array::from(vals))
            }
            (PrimitiveType::Double, Some(PrimitiveLiteral::Double(value))) => {
                Arc::new(Float64Array::from(vec![value.0; num_rows]))
            }
            (PrimitiveType::Double, None) => {
                let vals: Vec<Option<f64>> = vec![None; num_rows];
                Arc::new(Float64Array::from(vals))
            }
            (PrimitiveType::String, Some(PrimitiveLiteral::String(value))) => {
                Arc::new(StringArray::from(vec![value.clone(); num_rows]))
            }
            (PrimitiveType::String, None) => {
                let vals: Vec<Option<String>> = vec![None; num_rows];
                Arc::new(StringArray::from(vals))
            }
            (PrimitiveType::Binary, Some(PrimitiveLiteral::Binary(value))) => {
                Arc::new(BinaryArray::from_vec(vec![value; num_rows]))
            }
            (PrimitiveType::Binary, None) => {
                let vals: Vec<Option<&[u8]>> = vec![None; num_rows];
                Arc::new(BinaryArray::from_opt_vec(vals))
            }
            (dt, _) => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("unexpected target column type {}", dt),
                ))
            }
        })
    }
}

/// This accessor will cached the index path of the field for the schema to
/// speed up the access of the field in the schema for next time.
///
/// # Limit of this accessor:
/// - It also means that this accessor must not used for multiple schema and the user should gurarantee that otherwise the it's unexpected behavior.
/// - The accessor will not search the key field of the map type or the value field of the map type. It will can search the map field itself.
/// - The accessor will not search the element field of the list type. It will only search the list field itself.
#[derive(Clone, Debug)]
struct CachedArrowArrayAccessor {
    field_index_path_map: HashMap<i32, Vec<usize>>,
    default_value_generator: Option<DefaultValueGenerator>,
}

impl CachedArrowArrayAccessor {
    pub fn new<F>(
        iceberg_struct: &StructType,
        arrow_fields: &Fields,
        field_id_fetch: F,
        default_value_generator: Option<DefaultValueGenerator>,
    ) -> Result<Self>
    where
        F: Fn(&Field) -> Result<i32>,
    {
        let mut field_index_path_collector = FieldIndexPathCollector {
            field_index_path_map: HashMap::new(),
        };
        visit_struct_with_partner(
            iceberg_struct,
            &FieldPath::IndexPath(vec![], DataType::Struct(arrow_fields.clone())),
            &mut field_index_path_collector,
            &FieldIndexPathAccessor::new(field_id_fetch, default_value_generator.is_some()),
        )?;
        Ok(Self {
            field_index_path_map: field_index_path_collector.field_index_path_map,
            default_value_generator,
        })
    }

    fn get_array_by_field_index_path(
        arrays: &[ArrayRef],
        field_index_path: &[usize],
    ) -> Result<ArrayRef> {
        let mut rev_iterator = field_index_path.iter().rev();
        let mut array = arrays[*rev_iterator.next().unwrap()].clone();
        let mut null_buffer = array.logical_nulls();
        for idx in rev_iterator {
            array = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Cannot convert Array to StructArray",
                    )
                })?
                .column(*idx)
                .clone();
            null_buffer = NullBuffer::union(null_buffer.as_ref(), array.logical_nulls().as_ref());
        }
        Ok(make_array(
            array.to_data().into_builder().nulls(null_buffer).build()?,
        ))
    }
}

impl PartnerAccessor<ArrayRef> for CachedArrowArrayAccessor {
    fn struct_parner(&self, schema_partner: &ArrayRef) -> Result<ArrayRef> {
        if !matches!(schema_partner.data_type(), DataType::Struct(_)) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "The schema partner is not a struct type",
            ));
        }
        Ok(schema_partner.clone())
    }

    fn field_partner(&self, struct_partner: &ArrayRef, field: &NestedField) -> Result<ArrayRef> {
        let struct_array = struct_partner
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "The struct partner is not a struct array",
                )
            })?;
        // Find the field index path for the field, get the array by the index path.
        if let Some(field_index_path) = self.field_index_path_map.get(&field.id) {
            return Self::get_array_by_field_index_path(struct_array.columns(), field_index_path);
        }

        // If the field not found, if it's a primitive field and the default value generator is set,
        // use the default value generator to create the column.
        let Some(default_value_generator) = &self.default_value_generator else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Field {} not found", field.id),
            ));
        };
        let Some(target_type) = field.field_type.as_primitive_type() else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Field {} not found", field.id),
            ));
        };
        let default_value = if let Some(default_value) = &field.initial_default {
            let Literal::Primitive(primitive_literal) = default_value else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!(
                        "Default value for column must be primitive type, but encountered {:?}",
                        field.initial_default
                    ),
                ));
            };
            Some(primitive_literal.clone())
        } else {
            None
        };
        default_value_generator.create_column(target_type, &default_value, struct_array.len())
    }

    fn list_element_partner(&self, list_partner: &ArrayRef) -> Result<ArrayRef> {
        match list_partner.data_type() {
            DataType::List(_) => {
                let list_array = list_partner
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "The list partner is not a list array",
                        )
                    })?;
                Ok(list_array.values().clone())
            }
            DataType::LargeList(_) => {
                let list_array = list_partner
                    .as_any()
                    .downcast_ref::<LargeListArray>()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "The list partner is not a large list array",
                        )
                    })?;
                Ok(list_array.values().clone())
            }
            DataType::FixedSizeList(_, _) => {
                let list_array = list_partner
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "The list partner is not a fixed size list array",
                        )
                    })?;
                Ok(list_array.values().clone())
            }
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                "The list partner is not a list type",
            )),
        }
    }

    fn map_key_partner(&self, map_partner: &ArrayRef) -> Result<ArrayRef> {
        let map_array = map_partner
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| {
                Error::new(ErrorKind::DataInvalid, "The map partner is not a map array")
            })?;
        Ok(map_array.keys().clone())
    }

    fn map_value_partner(&self, map_partner: &ArrayRef) -> Result<ArrayRef> {
        let map_array = map_partner
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| {
                Error::new(ErrorKind::DataInvalid, "The map partner is not a map array")
            })?;
        Ok(map_array.values().clone())
    }
}

#[derive(Clone, Debug)]
struct ArrowProjectVisitor;

impl SchemaWithPartnerVisitor<ArrayRef> for ArrowProjectVisitor {
    type T = ArrayRef;

    fn schema(
        &mut self,
        _schema: &crate::spec::Schema,
        _partner: &ArrayRef,
        value: ArrayRef,
    ) -> Result<ArrayRef> {
        Ok(value)
    }

    fn field(
        &mut self,
        field: &crate::spec::NestedFieldRef,
        partner: &ArrayRef,
        value: ArrayRef,
    ) -> Result<ArrayRef> {
        let field_type = type_to_arrow_type(&field.field_type)?;
        if !field_type.equals_datatype(value.data_type()) {
            return Err(
                Error::new(ErrorKind::Unexpected, "Field type is not matched")
                    .with_context("iceberg_field", format!("{:?}", field))
                    .with_context("converted_arrow_type", format!("{:?}", value.data_type()))
                    .with_context("original_arrow_type", format!("{:?}", partner.data_type())),
            );
        }
        Ok(value)
    }

    fn r#struct(
        &mut self,
        r#struct: &StructType,
        partner: &ArrayRef,
        results: Vec<ArrayRef>,
    ) -> Result<ArrayRef> {
        let DataType::Struct(new_arrow_struct_fields) =
            type_to_arrow_type(&Type::Struct(r#struct.clone()))?
        else {
            return Err(Error::new(ErrorKind::Unexpected, "Field is not a struct"));
        };

        // # TODO: Refine this code
        // For struct array, it also crash for case that nulls is none but nulls of array is valid.
        // This is a hack fix. Maybe we should fix at upstream later.
        let nulls = if results.is_empty() {
            None
        } else {
            Some(
                partner
                    .logical_nulls()
                    .unwrap_or(NullBuffer::new_valid(partner.len())),
            )
        };
        let new_struct_array = StructArray::new(new_arrow_struct_fields, results, nulls);

        Ok(Arc::new(new_struct_array))
    }

    fn list(
        &mut self,
        list: &crate::spec::ListType,
        partner: &ArrayRef,
        value: ArrayRef,
    ) -> Result<ArrayRef> {
        let nulls = partner.nulls().cloned();
        let new_element_type = type_to_arrow_type(&list.element_field.field_type)?;
        match partner.data_type() {
            DataType::List(field) => {
                let original_list = partner.as_any().downcast_ref::<ListArray>().unwrap();
                let field = Arc::new(field.as_ref().clone().with_data_type(new_element_type));
                let offsets = original_list.offsets().clone();
                let list_array = ListArray::new(field, offsets, value, nulls);
                Ok(Arc::new(list_array))
            }
            DataType::LargeList(field) => {
                let original_list = partner.as_any().downcast_ref::<LargeListArray>().unwrap();
                let field = Arc::new(field.as_ref().clone().with_data_type(new_element_type));
                let offsets = original_list.offsets().clone();
                let list_array = LargeListArray::new(field, offsets, value, nulls);
                Ok(Arc::new(list_array))
            }
            DataType::FixedSizeList(field, size) => {
                let field = Arc::new(field.as_ref().clone().with_data_type(new_element_type));
                let list_array = FixedSizeListArray::new(field, *size, value, nulls);
                Ok(Arc::new(list_array))
            }
            _ => Err(Error::new(ErrorKind::Unexpected, "Field is not a list")),
        }
    }

    fn map(
        &mut self,
        map: &MapType,
        partner: &ArrayRef,
        key_value: ArrayRef,
        value: ArrayRef,
    ) -> Result<ArrayRef> {
        let original_array = partner.as_any().downcast_ref::<MapArray>().unwrap();
        let offsets = original_array.offsets().clone();
        let nulls = original_array.nulls().cloned();

        let DataType::Map(field, ordered) = type_to_arrow_type(&Type::Map(map.clone()))? else {
            return Err(Error::new(ErrorKind::Unexpected, "Field is not a map"));
        };
        let DataType::Struct(inner_struct_fields) = field.data_type() else {
            return Err(Error::new(ErrorKind::Unexpected, "Field is not a struct"));
        };
        let entries = StructArray::new(inner_struct_fields.clone(), vec![key_value, value], None);

        Ok(Arc::new(MapArray::new(
            field, offsets, entries, nulls, ordered,
        )))
    }

    fn primitive(&mut self, ty: &PrimitiveType, partner: &ArrayRef) -> Result<ArrayRef> {
        let target_type = type_to_arrow_type(&Type::Primitive(ty.clone()))?;
        if target_type.equals_datatype(partner.data_type()) {
            Ok(partner.clone())
        } else {
            let res = cast(partner, &target_type)?;
            Ok(res)
        }
    }
}

/// It used to project record batch match iceberg schema
///
/// This projector will handle the following schema evolution actions:
/// - Add new fields
/// - Type promotion
///
/// The iceberg spec refers to other permissible schema evolution actions
/// (see https://iceberg.apache.org/spec/#schema-evolution):
/// renaming fields, deleting fields and reordering fields.
/// Renames only affect the schema of the RecordBatch rather than the
/// columns themselves, so a single updated cached schema can
/// be re-used and no per-column actions are required.
/// Deletion and Reorder can be achieved without needing this
/// post-processing step by using the projection mask.
#[derive(Clone, Debug)]
pub(crate) struct RecordBatchProjector {
    iceberg_struct_type: StructType,
    accessor: CachedArrowArrayAccessor,
    visitor: ArrowProjectVisitor,
}

impl RecordBatchProjector {
    pub(crate) fn new<F>(
        expect_iceberg_schema: &Schema,
        input_arrow_schema: &ArrowSchema,
        field_id_fetch_func: F,
        default_value_generator: Option<DefaultValueGenerator>,
    ) -> Result<Self>
    where
        F: Fn(&Field) -> Result<i32>,
    {
        let accessor = CachedArrowArrayAccessor::new(
            expect_iceberg_schema.as_struct(),
            input_arrow_schema.fields(),
            field_id_fetch_func,
            default_value_generator,
        )?;
        Ok(Self {
            iceberg_struct_type: expect_iceberg_schema.as_struct().clone(),
            accessor,
            visitor: ArrowProjectVisitor,
        })
    }

    pub(crate) fn project_batch(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        // Record the original row num of the batch. It used for select_empty case.
        let row_num = batch.num_rows();

        // Convert the batch to the iceberg schema into struct array.
        let array = visit_struct_with_partner(
            &self.iceberg_struct_type,
            &(Arc::new(StructArray::from(batch)) as ArrayRef),
            &mut self.visitor,
            &self.accessor,
        )?
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap()
        .clone();

        // Convert back to the arrow record batch.
        let (fields, columns, nulls) = array.into_parts();
        if nulls.map(|n| n.null_count()).unwrap_or_default() != 0 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Cannot convert nullable StructArray to RecordBatch, see StructArray documentation",
            ));
        }
        Ok(RecordBatch::try_new_with_options(
            Arc::new(ArrowSchema::new(fields)),
            columns,
            &RecordBatchOptions::default()
                .with_match_field_names(false)
                .with_row_count(Some(row_num)),
        )?)
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray, StructArray};
    use arrow_schema::{DataType, Field};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    use crate::arrow::record_batch_projector::RecordBatchProjector;
    use crate::arrow::{get_field_id, schema_to_arrow_schema};
    use crate::spec::{ListType, MapType, NestedField, PrimitiveType, Schema, StructType, Type};

    fn nested_schema_for_test() -> Schema {
        // Int, Struct(Int,Int), String, List(Int), Struct(Struct(Int)), Map(String, List(Int))
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(0, "col0", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(
                    1,
                    "col1",
                    Type::Struct(StructType::new(vec![
                        NestedField::required(5, "col_1_5", Type::Primitive(PrimitiveType::Long))
                            .into(),
                        NestedField::required(6, "col_1_6", Type::Primitive(PrimitiveType::Long))
                            .into(),
                    ])),
                )
                .into(),
                NestedField::required(2, "col2", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(
                    3,
                    "col3",
                    Type::List(ListType::new(
                        NestedField::required(7, "element", Type::Primitive(PrimitiveType::Long))
                            .into(),
                    )),
                )
                .into(),
                NestedField::required(
                    4,
                    "col4",
                    Type::Struct(StructType::new(vec![NestedField::required(
                        8,
                        "col_4_8",
                        Type::Struct(StructType::new(vec![NestedField::required(
                            9,
                            "col_4_8_9",
                            Type::Primitive(PrimitiveType::Long),
                        )
                        .into()])),
                    )
                    .into()])),
                )
                .into(),
                NestedField::required(
                    10,
                    "col5",
                    Type::Map(MapType::new(
                        NestedField::required(11, "key", Type::Primitive(PrimitiveType::String))
                            .into(),
                        NestedField::required(
                            12,
                            "value",
                            Type::List(ListType::new(
                                NestedField::required(
                                    13,
                                    "item",
                                    Type::Primitive(PrimitiveType::Long),
                                )
                                .into(),
                            )),
                        )
                        .into(),
                    )),
                )
                .into(),
            ])
            .build()
            .unwrap()
    }

    #[test]
    fn test_fail_case_for_index_path_collect() {
        let iceberg_schema = nested_schema_for_test();
        let arrow_schema = schema_to_arrow_schema(&iceberg_schema).unwrap();

        // project map.key
        let projected_schema = nested_schema_for_test().project(&[11]).unwrap();
        assert!(
            RecordBatchProjector::new(&projected_schema, &arrow_schema, get_field_id, None)
                .is_err()
        );

        // project map.value
        let projected_schema = nested_schema_for_test().project(&[12]).unwrap();
        assert!(
            RecordBatchProjector::new(&projected_schema, &arrow_schema, get_field_id, None)
                .is_err()
        );

        // project list.element
        let projected_schema = nested_schema_for_test().project(&[7]).unwrap();
        assert!(
            RecordBatchProjector::new(&projected_schema, &arrow_schema, get_field_id, None)
                .is_err()
        );

        // project map, list itself can success
        let projected_schema = nested_schema_for_test().project(&[3, 10]).unwrap();
        RecordBatchProjector::new(&projected_schema, &arrow_schema, get_field_id, None).unwrap();
    }

    #[test]
    fn test_record_batch_projector_nested_level() {
        let iceberg_schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                Arc::new(NestedField::new(
                    1,
                    "field1",
                    Type::Primitive(PrimitiveType::Int),
                    false,
                )),
                Arc::new(NestedField::new(
                    2,
                    "field2",
                    Type::Struct(StructType::new(vec![
                        Arc::new(NestedField::new(
                            3,
                            "inner_field1",
                            Type::Primitive(PrimitiveType::Int),
                            false,
                        )),
                        Arc::new(NestedField::new(
                            4,
                            "inner_field2",
                            Type::Primitive(PrimitiveType::String),
                            false,
                        )),
                    ])),
                    false,
                )),
            ])
            .build()
            .unwrap();
        let arrow_schema = Arc::new(schema_to_arrow_schema(&iceberg_schema).unwrap());
        let projected_iceberg_schema = iceberg_schema.project(&[1, 3]).unwrap();
        let mut projector =
            RecordBatchProjector::new(&projected_iceberg_schema, &arrow_schema, get_field_id, None)
                .unwrap();

        let int_array = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let inner_int_array = Arc::new(Int32Array::from(vec![4, 5, 6])) as ArrayRef;
        let inner_string_array = Arc::new(StringArray::from(vec!["x", "y", "z"])) as ArrayRef;
        let struct_array = Arc::new(StructArray::from(vec![
            (
                Arc::new(
                    Field::new("inner_field1", DataType::Int32, true).with_metadata(
                        HashMap::from_iter(vec![(
                            PARQUET_FIELD_ID_META_KEY.to_string(),
                            "3".to_string(),
                        )]),
                    ),
                ),
                inner_int_array as ArrayRef,
            ),
            (
                Arc::new(
                    Field::new("inner_field2", DataType::Utf8, true).with_metadata(
                        HashMap::from_iter(vec![(
                            PARQUET_FIELD_ID_META_KEY.to_string(),
                            "4".to_string(),
                        )]),
                    ),
                ),
                inner_string_array as ArrayRef,
            ),
        ])) as ArrayRef;
        let batch = RecordBatch::try_new(arrow_schema, vec![int_array, struct_array]).unwrap();

        let projected_batch = projector.project_batch(batch).unwrap();
        assert_eq!(projected_batch.num_columns(), 2);
        let projected_int_array = projected_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let projected_inner_int_array = projected_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        assert_eq!(projected_int_array.values(), &[1, 2, 3]);
        assert_eq!(projected_inner_int_array.values(), &[4, 5, 6]);
    }
}
