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

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    FixedSizeListArray, Float32Array, Float64Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeListArray, LargeStringArray, ListArray, MapArray, NullArray, StringArray, StructArray,
    Time64MicrosecondArray, TimestampMicrosecondArray, TimestampNanosecondArray,
};
use arrow_schema::DataType;
use uuid::Uuid;

use super::get_field_id;
use crate::spec::{
    visit_struct_with_partner, ListPartnerIterator, Literal, Map, MapPartnerIterator,
    PartnerAccessor, PrimitiveType, SchemaWithPartnerVisitor, Struct, StructType,
};
use crate::{Error, ErrorKind, Result};

struct ArrowArrayConverter;

impl SchemaWithPartnerVisitor<ArrayRef> for ArrowArrayConverter {
    type T = Vec<Option<Literal>>;

    fn schema(
        &mut self,
        _schema: &crate::spec::Schema,
        _partner: &ArrayRef,
        value: Vec<Option<Literal>>,
    ) -> Result<Vec<Option<Literal>>> {
        Ok(value)
    }

    fn field(
        &mut self,
        field: &crate::spec::NestedFieldRef,
        _partner: &ArrayRef,
        value: Vec<Option<Literal>>,
    ) -> Result<Vec<Option<Literal>>> {
        // Make there is no null value if the field is required
        if field.required && value.iter().any(Option::is_none) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "The field is required but has null value",
            ));
        }
        Ok(value)
    }

    fn r#struct(
        &mut self,
        _struct: &StructType,
        _partner: &ArrayRef,
        results: Vec<Vec<Option<Literal>>>,
    ) -> Result<Vec<Option<Literal>>> {
        let row_len = results.first().map(|column| column.len()).unwrap_or(0);
        if results.iter().any(|column| column.len() != row_len) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "The struct columns have different row length",
            ));
        }

        let mut struct_literals = Vec::with_capacity(row_len);
        let mut columns_iters = results
            .into_iter()
            .map(|column| column.into_iter())
            .collect::<Vec<_>>();

        for _ in 0..row_len {
            let mut literals = Vec::with_capacity(columns_iters.len());
            for column_iter in columns_iters.iter_mut() {
                literals.push(column_iter.next().unwrap());
            }
            struct_literals.push(Some(Literal::Struct(Struct::from_iter(literals))));
        }

        Ok(struct_literals)
    }

    fn list(
        &mut self,
        list: &crate::spec::ListType,
        _partner: &ArrayRef,
        results: Vec<Vec<Option<Literal>>>,
    ) -> Result<Vec<Option<Literal>>> {
        if list.element_field.required {
            if results.iter().any(|row| row.iter().any(Option::is_none)) {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "The list should not have null value",
                ));
            }
        }
        Ok(results
            .into_iter()
            .map(|row| Some(Literal::List(row)))
            .collect())
    }

    fn map(
        &mut self,
        map: &crate::spec::MapType,
        _partner: &ArrayRef,
        key_values: Vec<Vec<Option<Literal>>>,
        values: Vec<Vec<Option<Literal>>>,
    ) -> Result<Vec<Option<Literal>>> {
        // Make sure key_value and value have the same row length
        if key_values.len() != values.len() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "The key value and value of map should have the same row length",
            ));
        }

        let mut result = Vec::with_capacity(key_values.len());
        for (key, value) in key_values.into_iter().zip(values.into_iter()) {
            // Make sure key_value and value have the same length
            if key.len() != value.len() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "The key value and value of map should have the same length",
                ));
            }
            // Make sure no null value in key_value
            if key.iter().any(Option::is_none) {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "The key value of map should not have null value",
                ));
            }

            // Make sure no null value in value if value field is required
            if map.value_field.required && value.iter().any(Option::is_none) {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "The value of map should not have null value",
                ));
            }

            let mut map = Map::new();
            for (k, v) in key.into_iter().zip(value.into_iter()) {
                map.insert(k.unwrap(), v.clone());
            }
            result.push(Some(Literal::Map(map)));
        }

        Ok(result)
    }

    fn primitive(&mut self, p: &PrimitiveType, partner: &ArrayRef) -> Result<Vec<Option<Literal>>> {
        if let Some(_) = partner.as_any().downcast_ref::<NullArray>() {
            return Ok(vec![None; partner.len()]);
        }
        match p {
            PrimitiveType::Boolean => {
                let array = partner
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        Error::new(ErrorKind::DataInvalid, "The partner is not a boolean array")
                    })?;
                Ok(array.iter().map(|v| v.map(Literal::bool)).collect())
            }
            PrimitiveType::Int => {
                let array = partner
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| {
                        Error::new(ErrorKind::DataInvalid, "The partner is not a int32 array")
                    })?;
                Ok(array.iter().map(|v| v.map(Literal::int)).collect())
            }
            PrimitiveType::Long => {
                let array = partner
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        Error::new(ErrorKind::DataInvalid, "The partner is not a int64 array")
                    })?;
                Ok(array.iter().map(|v| v.map(Literal::long)).collect())
            }
            PrimitiveType::Float => {
                let array = partner
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| {
                        Error::new(ErrorKind::DataInvalid, "The partner is not a float32 array")
                    })?;
                Ok(array.iter().map(|v| v.map(Literal::float)).collect())
            }
            PrimitiveType::Double => {
                let array = partner
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        Error::new(ErrorKind::DataInvalid, "The partner is not a float64 array")
                    })?;
                Ok(array.iter().map(|v| v.map(Literal::double)).collect())
            }
            PrimitiveType::Decimal { precision, scale } => {
                let array = partner
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "The partner is not a decimal128 array",
                        )
                    })?;
                if let DataType::Decimal128(arrow_precision, arrow_scale) = array.data_type() {
                    if *arrow_precision as u32 != *precision || *arrow_scale as u32 != *scale {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!(
                                "The precision or scale ({},{}) of arrow decimal128 array is not compatitable with iceberg decimal type ({},{})",
                                arrow_precision, arrow_scale, precision, scale
                            ),
                        ));
                    }
                }
                Ok(array.iter().map(|v| v.map(Literal::decimal)).collect())
            }
            PrimitiveType::Date => {
                let array = partner
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .ok_or_else(|| {
                        Error::new(ErrorKind::DataInvalid, "The partner is not a date32 array")
                    })?;
                Ok(array.iter().map(|v| v.map(Literal::date)).collect())
            }
            PrimitiveType::Time => {
                let array = partner
                    .as_any()
                    .downcast_ref::<Time64MicrosecondArray>()
                    .ok_or_else(|| {
                        Error::new(ErrorKind::DataInvalid, "The partner is not a time64 array")
                    })?;
                Ok(array.iter().map(|v| v.map(Literal::time)).collect())
            }
            PrimitiveType::Timestamp => {
                let array = partner
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "The partner is not a timestamp array",
                        )
                    })?;
                Ok(array.iter().map(|v| v.map(Literal::timestamp)).collect())
            }
            PrimitiveType::Timestamptz => {
                let array = partner
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "The partner is not a timestamptz array",
                        )
                    })?;
                Ok(array.iter().map(|v| v.map(Literal::timestamptz)).collect())
            }
            PrimitiveType::TimestampNs => {
                let array = partner
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "The partner is not a timestamp_ns array",
                        )
                    })?;
                Ok(array
                    .iter()
                    .map(|v| v.map(Literal::timestamp_nano))
                    .collect())
            }
            PrimitiveType::TimestamptzNs => {
                let array = partner
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "The partner is not a timestamptz_ns array",
                        )
                    })?;
                Ok(array
                    .iter()
                    .map(|v| v.map(Literal::timestamptz_nano))
                    .collect())
            }
            PrimitiveType::String => {
                if let Some(array) = partner.as_any().downcast_ref::<LargeStringArray>() {
                    Ok(array.iter().map(|v| v.map(Literal::string)).collect())
                } else if let Some(array) = partner.as_any().downcast_ref::<StringArray>() {
                    Ok(array.iter().map(|v| v.map(Literal::string)).collect())
                } else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "The partner is not a string array",
                    ));
                }
            }
            PrimitiveType::Uuid => {
                if let Some(array) = partner.as_any().downcast_ref::<FixedSizeBinaryArray>() {
                    if array.value_length() != 16 {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            "The partner is not a uuid array",
                        ));
                    }
                    Ok(array
                        .iter()
                        .map(|v| {
                            v.map(|v| {
                                Ok(Literal::uuid(Uuid::from_bytes(v.try_into().map_err(
                                    |_| {
                                        Error::new(
                                            ErrorKind::DataInvalid,
                                            "Failed to convert binary to uuid",
                                        )
                                    },
                                )?)))
                            })
                            .transpose()
                        })
                        .collect::<Result<Vec<_>>>()?)
                } else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "The partner is not a uuid array",
                    ));
                }
            }
            PrimitiveType::Fixed(len) => {
                let array = partner
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .ok_or_else(|| {
                        Error::new(ErrorKind::DataInvalid, "The partner is not a fixed array")
                    })?;
                if array.value_length() != *len as i32 {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "The length of fixed size binary array is not compatitable with iceberg fixed type",
                    ));
                }
                Ok(array
                    .iter()
                    .map(|v| v.map(|v| Literal::fixed(v.iter().cloned())))
                    .collect())
            }
            PrimitiveType::Binary => {
                if let Some(array) = partner.as_any().downcast_ref::<LargeBinaryArray>() {
                    Ok(array
                        .iter()
                        .map(|v| v.map(|v| Literal::binary(v.to_vec())))
                        .collect())
                } else if let Some(array) = partner.as_any().downcast_ref::<BinaryArray>() {
                    Ok(array
                        .iter()
                        .map(|v| v.map(|v| Literal::binary(v.to_vec())))
                        .collect())
                } else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "The partner is not a binary array",
                    ));
                }
            }
        }
    }

    fn visit_type_before(
        &mut self,
        _ty: &crate::spec::Type,
        partner: &ArrayRef,
    ) -> Result<Option<Vec<Option<Literal>>>> {
        if let Some(_) = partner.as_any().downcast_ref::<NullArray>() {
            return Ok(Some(vec![None; partner.len()]));
        }
        Ok(None)
    }
}

struct ArrowArrayAccessor;

impl PartnerAccessor<ArrayRef> for ArrowArrayAccessor {
    type L = ArrowArrayListIterator;
    type M = ArrowArrayMapIterator;

    fn struct_parner<'a>(&self, schema_partner: &'a ArrayRef) -> Result<&'a ArrayRef> {
        if !matches!(schema_partner.data_type(), DataType::Struct(_)) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "The schema partner is not a struct type",
            ));
        }
        Ok(schema_partner)
    }

    fn field_partner<'a>(
        &self,
        struct_partner: &'a ArrayRef,
        field_id: i32,
        _field_name: &str,
    ) -> Result<&'a ArrayRef> {
        let struct_array = struct_partner
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "The struct partner is not a struct array",
                )
            })?;
        let field_pos = struct_array
            .fields()
            .iter()
            .position(|field| {
                get_field_id(field)
                    .map(|id| id == field_id)
                    .unwrap_or(false)
            })
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Field id {} not found in struct array", field_id),
                )
            })?;
        Ok(struct_array.column(field_pos))
    }

    fn list_element_partner<'a>(
        &self,
        list_partner: &'a ArrayRef,
    ) -> Result<ArrowArrayListIterator> {
        if !matches!(
            list_partner.data_type(),
            DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _)
        ) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "The list partner is not a list type",
            ));
        }
        Ok(ArrowArrayListIterator {
            array: list_partner.clone(),
            index: 0,
        })
    }

    fn map_element_partner<'a>(&self, map_partner: &'a ArrayRef) -> Result<Self::M> {
        if !matches!(map_partner.data_type(), DataType::Map(_, _)) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "The map partner is not a map type",
            ));
        }
        Ok(ArrowArrayMapIterator {
            array: map_partner.clone(),
            index: 0,
        })
    }
}

struct ArrowArrayListIterator {
    array: ArrayRef,
    index: usize,
}

impl ListPartnerIterator<ArrayRef> for ArrowArrayListIterator {
    fn next(&mut self) -> Option<ArrayRef> {
        if self.index >= self.array.len() {
            return None;
        }
        if let Some(array) = self.array.as_any().downcast_ref::<ListArray>() {
            let result = Some(array.value(self.index));
            self.index += 1;
            result
        } else if let Some(array) = self.array.as_any().downcast_ref::<LargeListArray>() {
            let result = Some(array.value(self.index));
            self.index += 1;
            result
        } else if let Some(array) = self.array.as_any().downcast_ref::<FixedSizeListArray>() {
            let result = Some(array.value(self.index));
            self.index += 1;
            result
        } else {
            None
        }
    }
}

struct ArrowArrayMapIterator {
    array: ArrayRef,
    index: usize,
}

impl MapPartnerIterator<ArrayRef> for ArrowArrayMapIterator {
    fn next(&mut self) -> Option<(ArrayRef, ArrayRef)> {
        if let Some(array) = self.array.as_any().downcast_ref::<MapArray>() {
            let entry = array.value(self.index);
            Some((entry.column(0).clone(), entry.column(1).clone()))
        } else {
            None
        }
    }
}

/// Convert arrow struct array to iceberg struct value array.
/// This function will assume the schema of arrow struct array is the same as iceberg struct type.
pub fn arrow_struct_to_literal(
    struct_array: &ArrayRef,
    ty: &StructType,
) -> Result<Vec<Option<Literal>>> {
    visit_struct_with_partner(
        ty,
        struct_array,
        &mut ArrowArrayConverter,
        &ArrowArrayAccessor,
    )
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
        Float64Array, Int16Array, Int32Array, Int64Array, StringArray, StructArray,
        Time64MicrosecondArray, TimestampMicrosecondArray, TimestampNanosecondArray,
    };
    use arrow_schema::{DataType, Field, Fields, TimeUnit};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    use super::*;
    use crate::spec::{Literal, NestedField, PrimitiveType, StructType, Type};

    #[test]
    fn test_arrow_struct_to_iceberg_struct() {
        let bool_array = BooleanArray::from(vec![Some(true), Some(false), None]);
        let int16_array = Int16Array::from(vec![Some(1), Some(2), None]);
        let int32_array = Int32Array::from(vec![Some(3), Some(4), None]);
        let int64_array = Int64Array::from(vec![Some(5), Some(6), None]);
        let float32_array = Float32Array::from(vec![Some(1.1), Some(2.2), None]);
        let float64_array = Float64Array::from(vec![Some(3.3), Some(4.4), None]);
        let decimal_array = Decimal128Array::from(vec![Some(1000), Some(2000), None])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let date_array = Date32Array::from(vec![Some(18628), Some(18629), None]);
        let time_array = Time64MicrosecondArray::from(vec![Some(123456789), Some(987654321), None]);
        let timestamp_micro_array = TimestampMicrosecondArray::from(vec![
            Some(1622548800000000),
            Some(1622635200000000),
            None,
        ]);
        let timestamp_nano_array = TimestampNanosecondArray::from(vec![
            Some(1622548800000000000),
            Some(1622635200000000000),
            None,
        ]);
        let string_array = StringArray::from(vec![Some("a"), Some("b"), None]);
        let binary_array =
            BinaryArray::from(vec![Some(b"abc".as_ref()), Some(b"def".as_ref()), None]);

        let struct_array = Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("bool_field", DataType::Boolean, true)),
                Arc::new(bool_array) as ArrayRef,
            ),
            (
                Arc::new(Field::new("int16_field", DataType::Int16, true)),
                Arc::new(int16_array) as ArrayRef,
            ),
            (
                Arc::new(Field::new("int32_field", DataType::Int32, true)),
                Arc::new(int32_array) as ArrayRef,
            ),
            (
                Arc::new(Field::new("int64_field", DataType::Int64, true)),
                Arc::new(int64_array) as ArrayRef,
            ),
            (
                Arc::new(Field::new("float32_field", DataType::Float32, true)),
                Arc::new(float32_array) as ArrayRef,
            ),
            (
                Arc::new(Field::new("float64_field", DataType::Float64, true)),
                Arc::new(float64_array) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "decimal_field",
                    DataType::Decimal128(10, 2),
                    true,
                )),
                Arc::new(decimal_array) as ArrayRef,
            ),
            (
                Arc::new(Field::new("date_field", DataType::Date32, true)),
                Arc::new(date_array) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "time_field",
                    DataType::Time64(TimeUnit::Microsecond),
                    true,
                )),
                Arc::new(time_array) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "timestamp_micro_field",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                )),
                Arc::new(timestamp_micro_array) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "timestamp_nano_field",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    true,
                )),
                Arc::new(timestamp_nano_array) as ArrayRef,
            ),
            (
                Arc::new(Field::new("string_field", DataType::Utf8, true)),
                Arc::new(string_array) as ArrayRef,
            ),
            (
                Arc::new(Field::new("binary_field", DataType::Binary, true)),
                Arc::new(binary_array) as ArrayRef,
            ),
        ])) as ArrayRef;

        let iceberg_struct_type = StructType::new(vec![
            Arc::new(NestedField::optional(
                0,
                "bool_field",
                Type::Primitive(PrimitiveType::Boolean),
            )),
            Arc::new(NestedField::optional(
                1,
                "int16_field",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(NestedField::optional(
                2,
                "int32_field",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(NestedField::optional(
                3,
                "int64_field",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                4,
                "float32_field",
                Type::Primitive(PrimitiveType::Float),
            )),
            Arc::new(NestedField::optional(
                5,
                "float64_field",
                Type::Primitive(PrimitiveType::Double),
            )),
            Arc::new(NestedField::optional(
                6,
                "decimal_field",
                Type::Primitive(PrimitiveType::Decimal {
                    precision: 10,
                    scale: 2,
                }),
            )),
            Arc::new(NestedField::optional(
                7,
                "date_field",
                Type::Primitive(PrimitiveType::Date),
            )),
            Arc::new(NestedField::optional(
                8,
                "time_field",
                Type::Primitive(PrimitiveType::Time),
            )),
            Arc::new(NestedField::optional(
                9,
                "timestamp_micro_field",
                Type::Primitive(PrimitiveType::Timestamp),
            )),
            Arc::new(NestedField::optional(
                10,
                "timestamp_nao_field",
                Type::Primitive(PrimitiveType::TimestampNs),
            )),
            Arc::new(NestedField::optional(
                11,
                "string_field",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                12,
                "binary_field",
                Type::Primitive(PrimitiveType::Binary),
            )),
        ]);

        let result = arrow_struct_to_literal(&struct_array, &iceberg_struct_type).unwrap();

        assert_eq!(result, vec![
            Some(Literal::Struct(Struct::from_iter(vec![
                Some(Literal::bool(true)),
                Some(Literal::int(1)),
                Some(Literal::int(3)),
                Some(Literal::long(5)),
                Some(Literal::float(1.1)),
                Some(Literal::double(3.3)),
                Some(Literal::decimal(1000)),
                Some(Literal::date(18628)),
                Some(Literal::time(123456789)),
                Some(Literal::timestamp(1622548800000000)),
                Some(Literal::timestamp_nano(1622548800000000000)),
                Some(Literal::string("a".to_string())),
                Some(Literal::binary(b"abc".to_vec())),
            ]))),
            Some(Literal::Struct(Struct::from_iter(vec![
                Some(Literal::bool(false)),
                Some(Literal::int(2)),
                Some(Literal::int(4)),
                Some(Literal::long(6)),
                Some(Literal::float(2.2)),
                Some(Literal::double(4.4)),
                Some(Literal::decimal(2000)),
                Some(Literal::date(18629)),
                Some(Literal::time(987654321)),
                Some(Literal::timestamp(1622635200000000)),
                Some(Literal::timestamp_nano(1622635200000000000)),
                Some(Literal::string("b".to_string())),
                Some(Literal::binary(b"def".to_vec())),
            ]))),
            Some(Literal::Struct(Struct::from_iter(vec![
                None, None, None, None, None, None, None, None, None, None, None, None, None,
            ]))),
        ]);
    }

    #[test]
    fn test_single_column_nullable_struct() {
        let struct_array = Arc::new(StructArray::new_null(
            Fields::from(vec![Field::new("bool_field", DataType::Boolean, true)]),
            3,
        )) as ArrayRef;
        let iceberg_struct_type = StructType::new(vec![Arc::new(NestedField::optional(
            0,
            "bool_field",
            Type::Primitive(PrimitiveType::Boolean),
        ))]);
        let result = arrow_struct_to_literal(&struct_array, &iceberg_struct_type).unwrap();
        assert_eq!(result, vec![None; 3]);
    }

    #[test]
    fn test_empty_struct() {
        let struct_array = Arc::new(StructArray::new_null(Fields::empty(), 3)) as ArrayRef;
        let iceberg_struct_type = StructType::new(vec![]);
        let result = arrow_struct_to_literal(&struct_array, &iceberg_struct_type).unwrap();
        assert_eq!(result, vec![None; 0]);
    }

    #[test]
    fn test_arrow_struct_to_iceberg_struct_from_field_id() {
        let bool_array = BooleanArray::from(vec![Some(true), Some(false), None]);
        let int16_array = Int16Array::from(vec![Some(1), Some(2), None]);
        let int32_array = Int32Array::from(vec![Some(3), Some(4), None]);
        let int64_array = Int64Array::from(vec![Some(5), Some(6), None]);
        let float32_array = Float32Array::from(vec![Some(1.1), Some(2.2), None]);
        let struct_array = Arc::new(StructArray::from(vec![
            (
                Arc::new(
                    Field::new("bool_field", DataType::Boolean, true).with_metadata(HashMap::from(
                        [(PARQUET_FIELD_ID_META_KEY.to_string(), "2".to_string())],
                    )),
                ),
                Arc::new(bool_array) as ArrayRef,
            ),
            (
                Arc::new(
                    Field::new("int16_field", DataType::Int16, true).with_metadata(HashMap::from(
                        [(PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string())],
                    )),
                ),
                Arc::new(int16_array) as ArrayRef,
            ),
            (
                Arc::new(
                    Field::new("int32_field", DataType::Int32, true).with_metadata(HashMap::from(
                        [(PARQUET_FIELD_ID_META_KEY.to_string(), "4".to_string())],
                    )),
                ),
                Arc::new(int32_array) as ArrayRef,
            ),
            (
                Arc::new(
                    Field::new("int64_field", DataType::Int64, true).with_metadata(HashMap::from(
                        [(PARQUET_FIELD_ID_META_KEY.to_string(), "3".to_string())],
                    )),
                ),
                Arc::new(int64_array) as ArrayRef,
            ),
            (
                Arc::new(
                    Field::new("float32_field", DataType::Float32, true).with_metadata(
                        HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "5".to_string())]),
                    ),
                ),
                Arc::new(float32_array) as ArrayRef,
            ),
        ])) as ArrayRef;
        let struct_type = StructType::new(vec![
            Arc::new(NestedField::optional(
                1,
                "int16_field",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(NestedField::optional(
                2,
                "bool_field",
                Type::Primitive(PrimitiveType::Boolean),
            )),
            Arc::new(NestedField::optional(
                3,
                "int64_field",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                4,
                "int32_field",
                Type::Primitive(PrimitiveType::Int),
            )),
        ]);
        let result = arrow_struct_to_literal(&struct_array, &struct_type).unwrap();
        assert_eq!(result, vec![
            Some(Literal::Struct(Struct::from_iter(vec![
                Some(Literal::int(1)),
                Some(Literal::bool(true)),
                Some(Literal::long(5)),
                Some(Literal::int(3)),
            ]))),
            Some(Literal::Struct(Struct::from_iter(vec![
                Some(Literal::int(2)),
                Some(Literal::bool(false)),
                Some(Literal::long(6)),
                Some(Literal::int(4)),
            ]))),
            Some(Literal::Struct(Struct::from_iter(vec![
                None, None, None, None,
            ]))),
        ]);
    }
}
