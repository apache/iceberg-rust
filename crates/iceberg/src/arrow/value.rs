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
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, NullArray, StringArray,
    StructArray, Time64MicrosecondArray, TimestampMicrosecondArray, TimestampNanosecondArray,
};
use arrow_schema::{DataType, TimeUnit};
use itertools::Itertools;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::spec::{Literal, PrimitiveType, Struct, StructType, Type};
use crate::{Error, ErrorKind, Result};

/// A post order arrow array visitor.
/// # TODO
/// - Add support for ListArray, MapArray
trait ArrowArrayVistor {
    type T;
    fn null(&self, array: &NullArray, iceberg_type: &PrimitiveType) -> Result<Vec<Self::T>>;
    fn boolean(&self, array: &BooleanArray, iceberg_type: &PrimitiveType) -> Result<Vec<Self::T>>;
    fn int16(&self, array: &Int16Array, iceberg_type: &PrimitiveType) -> Result<Vec<Self::T>>;
    fn int32(&self, array: &Int32Array, iceberg_type: &PrimitiveType) -> Result<Vec<Self::T>>;
    fn int64(&self, array: &Int64Array, iceberg_type: &PrimitiveType) -> Result<Vec<Self::T>>;
    fn float(&self, array: &Float32Array, iceberg_type: &PrimitiveType) -> Result<Vec<Self::T>>;
    fn double(&self, array: &Float64Array, iceberg_type: &PrimitiveType) -> Result<Vec<Self::T>>;
    fn decimal(
        &self,
        array: &Decimal128Array,
        iceberg_type: &PrimitiveType,
    ) -> Result<Vec<Self::T>>;
    fn date(&self, array: &Date32Array, iceberg_type: &PrimitiveType) -> Result<Vec<Self::T>>;
    fn time(
        &self,
        array: &Time64MicrosecondArray,
        iceberg_type: &PrimitiveType,
    ) -> Result<Vec<Self::T>>;
    fn timestamp(
        &self,
        array: &TimestampMicrosecondArray,
        iceberg_type: &PrimitiveType,
    ) -> Result<Vec<Self::T>>;
    fn timestamp_nano(
        &self,
        array: &TimestampNanosecondArray,
        iceberg_type: &PrimitiveType,
    ) -> Result<Vec<Self::T>>;
    fn string(&self, array: &StringArray, iceberg_type: &PrimitiveType) -> Result<Vec<Self::T>>;
    fn large_string(
        &self,
        array: &LargeStringArray,
        iceberg_type: &PrimitiveType,
    ) -> Result<Vec<Self::T>>;
    fn binary(&self, array: &BinaryArray, iceberg_type: &PrimitiveType) -> Result<Vec<Self::T>>;
    fn large_binary(
        &self,
        array: &LargeBinaryArray,
        iceberg_type: &PrimitiveType,
    ) -> Result<Vec<Self::T>>;
    fn r#struct(
        &self,
        array: &StructArray,
        iceberg_type: &StructType,
        childs: Vec<Vec<Self::T>>,
    ) -> Result<Vec<Self::T>>;
}

struct ArrowArrayConvert;

impl ArrowArrayVistor for ArrowArrayConvert {
    type T = Option<Literal>;

    fn null(&self, array: &NullArray, _iceberg_type: &PrimitiveType) -> Result<Vec<Self::T>> {
        Ok(vec![None; array.len()])
    }

    fn boolean(&self, array: &BooleanArray, iceberg_type: &PrimitiveType) -> Result<Vec<Self::T>> {
        match iceberg_type {
            PrimitiveType::Boolean => Ok(array.iter().map(|v| v.map(Literal::bool)).collect()),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "The type of arrow boolean array is not compatitable with iceberg type {}",
                    iceberg_type
                ),
            )),
        }
    }

    fn int16(&self, array: &Int16Array, iceberg_type: &PrimitiveType) -> Result<Vec<Self::T>> {
        match iceberg_type {
            PrimitiveType::Int => Ok(array.iter().map(|v| v.map(Literal::int)).collect()),
            PrimitiveType::Long => Ok(array.iter().map(|v| v.map(Literal::long)).collect()),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "The type of arrow int16 array is not compatitable with iceberg type {}",
                    iceberg_type
                ),
            )),
        }
    }

    fn int32(&self, array: &Int32Array, iceberg_type: &PrimitiveType) -> Result<Vec<Self::T>> {
        match iceberg_type {
            PrimitiveType::Int => Ok(array.iter().map(|v| v.map(Literal::int)).collect()),
            PrimitiveType::Long => Ok(array.iter().map(|v| v.map(Literal::long)).collect()),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "The type of arrow int32 array is not compatitable with iceberg type {}",
                    iceberg_type
                ),
            )),
        }
    }

    fn int64(&self, array: &Int64Array, iceberg_type: &PrimitiveType) -> Result<Vec<Self::T>> {
        match iceberg_type {
            PrimitiveType::Long => Ok(array.iter().map(|v| v.map(Literal::long)).collect()),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "The type of arrow int64 array is not compatitable with iceberg type {}",
                    iceberg_type
                ),
            )),
        }
    }

    fn float(&self, array: &Float32Array, iceberg_type: &PrimitiveType) -> Result<Vec<Self::T>> {
        match iceberg_type {
            PrimitiveType::Float => Ok(array.iter().map(|v| v.map(Literal::float)).collect()),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "The type of arrow float16 array is not compatitable with iceberg type {}",
                    iceberg_type
                ),
            )),
        }
    }

    fn double(&self, array: &Float64Array, iceberg_type: &PrimitiveType) -> Result<Vec<Self::T>> {
        match iceberg_type {
            PrimitiveType::Double => Ok(array.iter().map(|v| v.map(Literal::double)).collect()),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "The type of arrow float64 array is not compatitable with iceberg type {}",
                    iceberg_type
                ),
            )),
        }
    }

    fn decimal(
        &self,
        array: &Decimal128Array,
        iceberg_type: &PrimitiveType,
    ) -> Result<Vec<Self::T>> {
        let DataType::Decimal128(arrow_precision, arrow_scale) = array.data_type() else {
            unreachable!()
        };
        match iceberg_type {
            PrimitiveType::Decimal { precision, scale } => {
                if *arrow_precision as u32 != *precision || *arrow_scale as u32 != *scale {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "The precision or scale ({},{}) of arrow decimal128 array is not compatitable with iceberg decimal type ({},{})",
                            arrow_precision, arrow_scale, precision, scale
                        ),
                    ));
                }
                Ok(array.iter().map(|v| v.map(Literal::decimal)).collect())
            }
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "The type of arrow decimal128 array is not compatitable with iceberg type {}",
                    iceberg_type
                ),
            )),
        }
    }

    fn date(&self, array: &Date32Array, iceberg_type: &PrimitiveType) -> Result<Vec<Self::T>> {
        match iceberg_type {
            PrimitiveType::Date => Ok(array.iter().map(|v| v.map(Literal::date)).collect()),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "The type of arrow date32 array is not compatitable with iceberg type {}",
                    iceberg_type
                ),
            )),
        }
    }

    fn time(
        &self,
        array: &Time64MicrosecondArray,
        iceberg_type: &PrimitiveType,
    ) -> Result<Vec<Self::T>> {
        match iceberg_type {
            PrimitiveType::Time => Ok(array
                .iter()
                .map(|v| v.map(Literal::time))
                .collect()),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "The type of arrow time64 microsecond array is not compatitable with iceberg type {}",
                    iceberg_type
                ),
            )),
        }
    }

    fn timestamp(
        &self,
        array: &TimestampMicrosecondArray,
        iceberg_type: &PrimitiveType,
    ) -> Result<Vec<Self::T>> {
        match iceberg_type {
            PrimitiveType::Timestamp => Ok(array
                .iter()
                .map(|v| v.map(Literal::timestamp))
                .collect()),
            PrimitiveType::Timestamptz => Ok(array
                .iter()
                .map(|v| v.map(Literal::timestamptz))
                .collect()),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "The type of arrow timestamp microsecond array is not compatitable with iceberg type {}",
                    iceberg_type
                ),
            )),
        }
    }

    fn timestamp_nano(
        &self,
        array: &TimestampNanosecondArray,
        iceberg_type: &PrimitiveType,
    ) -> Result<Vec<Self::T>> {
        match iceberg_type {
            PrimitiveType::TimestampNs => Ok(array
                .iter()
                .map(|v| v.map(Literal::timestamp_nano))
                .collect()),
            PrimitiveType::TimestamptzNs => Ok(array
                .iter()
                .map(|v| v.map(Literal::timestamptz_nano))
                .collect()),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "The type of arrow timestamp nanosecond array is not compatitable with iceberg type {}",
                    iceberg_type
                ),
            )),
        }
    }

    fn string(&self, array: &StringArray, iceberg_type: &PrimitiveType) -> Result<Vec<Self::T>> {
        match iceberg_type {
            PrimitiveType::String => Ok(array.iter().map(|v| v.map(Literal::string)).collect()),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "The type of arrow string array is not compatitable with iceberg type {}",
                    iceberg_type
                ),
            )),
        }
    }

    fn large_string(
        &self,
        array: &LargeStringArray,
        iceberg_type: &PrimitiveType,
    ) -> Result<Vec<Self::T>> {
        match iceberg_type {
            PrimitiveType::String => Ok(array.iter().map(|v| v.map(Literal::string)).collect()),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "The type of arrow large string array is not compatitable with iceberg type {}",
                    iceberg_type
                ),
            )),
        }
    }

    fn binary(&self, array: &BinaryArray, iceberg_type: &PrimitiveType) -> Result<Vec<Self::T>> {
        match iceberg_type {
            PrimitiveType::Binary => Ok(array
                .iter()
                .map(|v| v.map(|v| Literal::binary(v.to_vec())))
                .collect()),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "The type of arrow binary array is not compatitable with iceberg type {}",
                    iceberg_type
                ),
            )),
        }
    }

    fn large_binary(
        &self,
        array: &LargeBinaryArray,
        iceberg_type: &PrimitiveType,
    ) -> Result<Vec<Self::T>> {
        match iceberg_type {
            PrimitiveType::Binary => Ok(array
                .iter()
                .map(|v| v.map(|v| Literal::binary(v.to_vec())))
                .collect()),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "The type of arrow binary array is not compatitable with iceberg type {}",
                    iceberg_type
                ),
            )),
        }
    }

    fn r#struct(
        &self,
        array: &StructArray,
        _iceberg_type: &StructType,
        columns: Vec<Vec<Self::T>>,
    ) -> Result<Vec<Self::T>> {
        let struct_literal_len = columns.first().map(|column| column.len()).unwrap_or(0);
        let mut struct_literals = Vec::with_capacity(struct_literal_len);
        let mut columns_iters = columns
            .into_iter()
            .map(|column| column.into_iter())
            .collect::<Vec<_>>();

        for row_idx in 0..struct_literal_len {
            if array.is_null(row_idx) {
                struct_literals.push(None);
                continue;
            }
            let mut literals = Vec::with_capacity(columns_iters.len());
            for column_iter in columns_iters.iter_mut() {
                literals.push(column_iter.next().unwrap());
            }
            struct_literals.push(Some(Literal::Struct(Struct::from_iter(literals))));
        }

        Ok(struct_literals)
    }
}

fn visit_arrow_struct_array<V: ArrowArrayVistor>(
    array: &StructArray,
    iceberg_type: &StructType,
    visitor: &V,
) -> Result<Vec<V::T>> {
    let DataType::Struct(arrow_struct_fields) = array.data_type() else {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "The type of arrow struct array is not a struct type",
        ));
    };

    if array.columns().len() != iceberg_type.fields().len()
        || arrow_struct_fields.len() != iceberg_type.fields().len()
    {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "The type of arrow struct array is not compatitable with iceberg struct type",
        ));
    }

    let mut columns = Vec::with_capacity(array.columns().len());

    for ((array, arrow_type), iceberg_field) in array
        .columns()
        .iter()
        .zip_eq(arrow_struct_fields.iter().map(|field| field.data_type()))
        .zip_eq(iceberg_type.fields().iter())
    {
        if array.is_nullable() == iceberg_field.required {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "The nullable field of arrow struct array is not compatitable with iceberg type",
            ));
        }
        match (arrow_type, iceberg_field.field_type.as_ref()) {
            (DataType::Null, Type::Primitive(primitive_type)) => {
                if iceberg_field.required {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "column in arrow array should not be optional",
                    ));
                }
                let array = array.as_any().downcast_ref::<NullArray>().unwrap();
                columns.push(visitor.null(array, primitive_type)?);
            }
            (DataType::Boolean, Type::Primitive(primitive_type)) => {
                let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                columns.push(visitor.boolean(array, primitive_type)?);
            }
            (DataType::Int16, Type::Primitive(primitive_type)) => {
                let array = array.as_any().downcast_ref::<Int16Array>().unwrap();
                columns.push(visitor.int16(array, primitive_type)?);
            }
            (DataType::Int32, Type::Primitive(primitive_type)) => {
                let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
                columns.push(visitor.int32(array, primitive_type)?);
            }
            (DataType::Int64, Type::Primitive(primitive_type)) => {
                let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                columns.push(visitor.int64(array, primitive_type)?);
            }
            (DataType::Float32, Type::Primitive(primitive_type)) => {
                let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                columns.push(visitor.float(array, primitive_type)?);
            }
            (DataType::Float64, Type::Primitive(primitive_type)) => {
                let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                columns.push(visitor.double(array, primitive_type)?);
            }
            (DataType::Decimal128(_, _), Type::Primitive(primitive_type)) => {
                let array = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
                columns.push(visitor.decimal(array, primitive_type)?);
            }
            (DataType::Date32, Type::Primitive(primitive_type)) => {
                let array = array.as_any().downcast_ref::<Date32Array>().unwrap();
                columns.push(visitor.date(array, primitive_type)?);
            }
            (DataType::Time64(TimeUnit::Microsecond), Type::Primitive(primitive_type)) => {
                let array = array
                    .as_any()
                    .downcast_ref::<Time64MicrosecondArray>()
                    .unwrap();
                columns.push(visitor.time(array, primitive_type)?);
            }
            (DataType::Timestamp(TimeUnit::Microsecond, _), Type::Primitive(primitive_type)) => {
                let array = array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap();
                columns.push(visitor.timestamp(array, primitive_type)?);
            }
            (DataType::Timestamp(TimeUnit::Nanosecond, _), Type::Primitive(primitive_type)) => {
                let array = array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();
                columns.push(visitor.timestamp_nano(array, primitive_type)?);
            }
            (DataType::Utf8, Type::Primitive(primitive_type)) => {
                let array = array.as_any().downcast_ref::<StringArray>().unwrap();
                columns.push(visitor.string(array, primitive_type)?);
            }
            (DataType::LargeUtf8, Type::Primitive(primitive_type)) => {
                let array = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
                columns.push(visitor.large_string(array, primitive_type)?);
            }
            (DataType::Binary, Type::Primitive(primitive_type)) => {
                let array = array.as_any().downcast_ref::<BinaryArray>().unwrap();
                columns.push(visitor.binary(array, primitive_type)?);
            }
            (DataType::LargeBinary, Type::Primitive(primitive_type)) => {
                let array = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
                columns.push(visitor.large_binary(array, primitive_type)?);
            }
            (DataType::Struct(_), Type::Struct(struct_type)) => {
                let array = array.as_any().downcast_ref::<StructArray>().unwrap();
                columns.push(visit_arrow_struct_array(array, struct_type, visitor)?);
            }
            (arrow_type, iceberg_field_type) => {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    format!(
                        "Unsupported convert arrow type {} to iceberg type: {}",
                        arrow_type, iceberg_field_type
                    ),
                ))
            }
        }
    }

    visitor.r#struct(array, iceberg_type, columns)
}

// # TODO
// Add support for fullfill the missing field in arrow struct array
fn visit_arrow_struct_array_from_field_id<V: ArrowArrayVistor>(
    array: &StructArray,
    iceberg_type: &StructType,
    visitor: &V,
) -> Result<Vec<V::T>> {
    let DataType::Struct(arrow_struct_fields) = array.data_type() else {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "The type of arrow struct array is not a struct type",
        ));
    };

    if array.columns().len() < iceberg_type.fields().len()
        || arrow_struct_fields.len() < iceberg_type.fields().len()
    {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "The type of arrow struct array is not compatitable with iceberg struct type",
        ));
    }

    let mut columns = Vec::with_capacity(array.columns().len());

    for iceberg_field in iceberg_type.fields() {
        let Some((idx, field)) = arrow_struct_fields.iter().enumerate().find(|(_idx, f)| {
            f.metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .and_then(|id| id.parse::<i32>().ok().map(|id: i32| id == iceberg_field.id))
                .unwrap_or(false)
        }) else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "The field {} in iceberg struct type is not found in arrow struct type",
                    iceberg_field.name
                ),
            ));
        };
        let array = array.column(idx);
        let arrow_type = field.data_type();
        if array.is_nullable() == iceberg_field.required {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "The nullable field of arrow struct array is not compatitable with iceberg type",
            ));
        }
        match (arrow_type, iceberg_field.field_type.as_ref()) {
            (DataType::Null, Type::Primitive(primitive_type)) => {
                if iceberg_field.required {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "column in arrow array should not be optional",
                    ));
                }
                let array = array.as_any().downcast_ref::<NullArray>().unwrap();
                columns.push(visitor.null(array, primitive_type)?);
            }
            (DataType::Boolean, Type::Primitive(primitive_type)) => {
                let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                columns.push(visitor.boolean(array, primitive_type)?);
            }
            (DataType::Int16, Type::Primitive(primitive_type)) => {
                let array = array.as_any().downcast_ref::<Int16Array>().unwrap();
                columns.push(visitor.int16(array, primitive_type)?);
            }
            (DataType::Int32, Type::Primitive(primitive_type)) => {
                let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
                columns.push(visitor.int32(array, primitive_type)?);
            }
            (DataType::Int64, Type::Primitive(primitive_type)) => {
                let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                columns.push(visitor.int64(array, primitive_type)?);
            }
            (DataType::Float32, Type::Primitive(primitive_type)) => {
                let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                columns.push(visitor.float(array, primitive_type)?);
            }
            (DataType::Float64, Type::Primitive(primitive_type)) => {
                let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                columns.push(visitor.double(array, primitive_type)?);
            }
            (DataType::Decimal128(_, _), Type::Primitive(primitive_type)) => {
                let array = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
                columns.push(visitor.decimal(array, primitive_type)?);
            }
            (DataType::Date32, Type::Primitive(primitive_type)) => {
                let array = array.as_any().downcast_ref::<Date32Array>().unwrap();
                columns.push(visitor.date(array, primitive_type)?);
            }
            (DataType::Time64(TimeUnit::Microsecond), Type::Primitive(primitive_type)) => {
                let array = array
                    .as_any()
                    .downcast_ref::<Time64MicrosecondArray>()
                    .unwrap();
                columns.push(visitor.time(array, primitive_type)?);
            }
            (DataType::Timestamp(TimeUnit::Microsecond, _), Type::Primitive(primitive_type)) => {
                let array = array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap();
                columns.push(visitor.timestamp(array, primitive_type)?);
            }
            (DataType::Timestamp(TimeUnit::Nanosecond, _), Type::Primitive(primitive_type)) => {
                let array = array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();
                columns.push(visitor.timestamp_nano(array, primitive_type)?);
            }
            (DataType::Utf8, Type::Primitive(primitive_type)) => {
                let array = array.as_any().downcast_ref::<StringArray>().unwrap();
                columns.push(visitor.string(array, primitive_type)?);
            }
            (DataType::LargeUtf8, Type::Primitive(primitive_type)) => {
                let array = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
                columns.push(visitor.large_string(array, primitive_type)?);
            }
            (DataType::Binary, Type::Primitive(primitive_type)) => {
                let array = array.as_any().downcast_ref::<BinaryArray>().unwrap();
                columns.push(visitor.binary(array, primitive_type)?);
            }
            (DataType::LargeBinary, Type::Primitive(primitive_type)) => {
                let array = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
                columns.push(visitor.large_binary(array, primitive_type)?);
            }
            (DataType::Struct(_), Type::Struct(struct_type)) => {
                let array = array.as_any().downcast_ref::<StructArray>().unwrap();
                columns.push(visit_arrow_struct_array(array, struct_type, visitor)?);
            }
            (arrow_type, iceberg_field_type) => {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    format!(
                        "Unsupported convert arrow type {} to iceberg type: {}",
                        arrow_type, iceberg_field_type
                    ),
                ))
            }
        }
    }

    visitor.r#struct(array, iceberg_type, columns)
}

/// Convert arrow struct array to iceberg struct value array.
/// This function will assume the schema of arrow struct array is the same as iceberg struct type.
pub fn arrow_struct_to_literal(
    struct_array: &StructArray,
    ty: StructType,
) -> Result<Vec<Option<Literal>>> {
    visit_arrow_struct_array(struct_array, &ty, &ArrowArrayConvert)
}

/// Convert arrow struct array to iceberg struct value array.
/// This function will use field id to find the corresponding field in arrow struct array.
pub fn arrow_struct_to_literal_from_field_id(
    struct_array: &StructArray,
    ty: StructType,
) -> Result<Vec<Option<Literal>>> {
    visit_arrow_struct_array_from_field_id(struct_array, &ty, &ArrowArrayConvert)
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

        let struct_array = StructArray::from(vec![
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
        ]);

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

        let result = arrow_struct_to_literal(&struct_array, iceberg_struct_type).unwrap();

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
        let struct_array = StructArray::new_null(
            Fields::from(vec![Field::new("bool_field", DataType::Boolean, true)]),
            3,
        );
        let iceberg_struct_type = StructType::new(vec![Arc::new(NestedField::optional(
            0,
            "bool_field",
            Type::Primitive(PrimitiveType::Boolean),
        ))]);
        let result = arrow_struct_to_literal(&struct_array, iceberg_struct_type).unwrap();
        assert_eq!(result, vec![None; 3]);
    }

    #[test]
    fn test_empty_struct() {
        let struct_array = StructArray::new_null(Fields::empty(), 3);
        let iceberg_struct_type = StructType::new(vec![]);
        let result = arrow_struct_to_literal(&struct_array, iceberg_struct_type).unwrap();
        assert_eq!(result, vec![None; 0]);
    }

    #[test]
    fn test_arrow_struct_to_iceberg_struct_from_field_id() {
        let bool_array = BooleanArray::from(vec![Some(true), Some(false), None]);
        let int16_array = Int16Array::from(vec![Some(1), Some(2), None]);
        let int32_array = Int32Array::from(vec![Some(3), Some(4), None]);
        let int64_array = Int64Array::from(vec![Some(5), Some(6), None]);
        let float32_array = Float32Array::from(vec![Some(1.1), Some(2.2), None]);
        let struct_array = StructArray::from(vec![
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
        ]);
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
        let result = arrow_struct_to_literal_from_field_id(&struct_array, struct_type).unwrap();
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
