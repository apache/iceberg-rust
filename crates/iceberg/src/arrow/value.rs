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
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float16Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray,
    StringArray, StructArray, Time64MicrosecondArray, TimestampMicrosecondArray,
    TimestampNanosecondArray,
};
use arrow_schema::{DataType, TimeUnit};
use itertools::Itertools;

use crate::spec::{Literal, PrimitiveType, Struct, StructType, Type};
use crate::{Error, ErrorKind, Result};

trait ToIcebergLiteralArray {
    fn to_primitive_literal_array(
        &self,
        _arrow_type: &DataType,
        _iceberg_type: &PrimitiveType,
    ) -> Result<Vec<Option<Literal>>>;
    fn to_struct_literal_array(
        &self,
        _arrow_type: &DataType,
        _iceberg_type: &StructType,
    ) -> Result<Vec<Option<Literal>>>;
}

impl ToIcebergLiteralArray for BooleanArray {
    fn to_primitive_literal_array(
        &self,
        _arrow_type: &DataType,
        iceberg_type: &PrimitiveType,
    ) -> Result<Vec<Option<Literal>>> {
        match iceberg_type {
            PrimitiveType::Boolean => Ok(self.iter().map(|v| v.map(Literal::bool)).collect()),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "The type of arrow boolean array is not compatitable with iceberg type {}",
                    iceberg_type
                ),
            )),
        }
    }

    fn to_struct_literal_array(
        &self,
        _arrow_type: &DataType,
        _iceberg_type: &StructType,
    ) -> Result<Vec<Option<Literal>>> {
        unreachable!()
    }
}

impl ToIcebergLiteralArray for Int16Array {
    fn to_primitive_literal_array(
        &self,
        _arrow_type: &DataType,
        iceberg_type: &PrimitiveType,
    ) -> Result<Vec<Option<Literal>>> {
        match iceberg_type {
            PrimitiveType::Int => Ok(self.iter().map(|v| v.map(Literal::int)).collect()),
            PrimitiveType::Long => Ok(self.iter().map(|v| v.map(Literal::long)).collect()),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "The type of arrow int16 array is not compatitable with iceberg type {}",
                    iceberg_type
                ),
            )),
        }
    }

    fn to_struct_literal_array(
        &self,
        _arrow_type: &DataType,
        _iceberg_type: &StructType,
    ) -> Result<Vec<Option<Literal>>> {
        unreachable!()
    }
}

impl ToIcebergLiteralArray for Int32Array {
    fn to_primitive_literal_array(
        &self,
        _arrow_type: &DataType,
        iceberg_type: &PrimitiveType,
    ) -> Result<Vec<Option<Literal>>> {
        match iceberg_type {
            PrimitiveType::Int => Ok(self.iter().map(|v| v.map(Literal::int)).collect()),
            PrimitiveType::Long => Ok(self.iter().map(|v| v.map(Literal::long)).collect()),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "The type of arrow int32 array is not compatitable with iceberg type {}",
                    iceberg_type
                ),
            )),
        }
    }

    fn to_struct_literal_array(
        &self,
        _arrow_type: &DataType,
        _iceberg_type: &StructType,
    ) -> Result<Vec<Option<Literal>>> {
        unreachable!()
    }
}

impl ToIcebergLiteralArray for Int64Array {
    fn to_primitive_literal_array(
        &self,
        _arrow_type: &DataType,
        iceberg_type: &PrimitiveType,
    ) -> Result<Vec<Option<Literal>>> {
        match iceberg_type {
            PrimitiveType::Long => Ok(self.iter().map(|v| v.map(Literal::long)).collect()),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "The type of arrow int64 array is not compatitable with iceberg type {}",
                    iceberg_type
                ),
            )),
        }
    }

    fn to_struct_literal_array(
        &self,
        _arrow_type: &DataType,
        _iceberg_type: &StructType,
    ) -> Result<Vec<Option<Literal>>> {
        unreachable!()
    }
}

impl ToIcebergLiteralArray for Float16Array {
    fn to_primitive_literal_array(
        &self,
        _arrow_type: &DataType,
        iceberg_type: &PrimitiveType,
    ) -> Result<Vec<Option<Literal>>> {
        match iceberg_type {
            PrimitiveType::Float => Ok(self
                .iter()
                .map(|v| v.map(|v| Literal::float(v.to_f32())))
                .collect()),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "The type of arrow float16 array is not compatitable with iceberg type {}",
                    iceberg_type
                ),
            )),
        }
    }

    fn to_struct_literal_array(
        &self,
        _arrow_type: &DataType,
        _iceberg_type: &StructType,
    ) -> Result<Vec<Option<Literal>>> {
        unreachable!()
    }
}

impl ToIcebergLiteralArray for Float32Array {
    fn to_primitive_literal_array(
        &self,
        _arrow_type: &DataType,
        iceberg_type: &PrimitiveType,
    ) -> Result<Vec<Option<Literal>>> {
        match iceberg_type {
            PrimitiveType::Float => Ok(self.iter().map(|v| v.map(Literal::float)).collect()),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "The type of arrow float32 array is not compatitable with iceberg type {}",
                    iceberg_type
                ),
            )),
        }
    }

    fn to_struct_literal_array(
        &self,
        _arrow_type: &DataType,
        _iceberg_type: &StructType,
    ) -> Result<Vec<Option<Literal>>> {
        unreachable!()
    }
}

impl ToIcebergLiteralArray for Float64Array {
    fn to_primitive_literal_array(
        &self,
        _arrow_type: &DataType,
        iceberg_type: &PrimitiveType,
    ) -> Result<Vec<Option<Literal>>> {
        match iceberg_type {
            PrimitiveType::Double => Ok(self.iter().map(|v| v.map(Literal::double)).collect()),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "The type of arrow float64 array is not compatitable with iceberg type {}",
                    iceberg_type
                ),
            )),
        }
    }

    fn to_struct_literal_array(
        &self,
        _arrow_type: &DataType,
        _iceberg_type: &StructType,
    ) -> Result<Vec<Option<Literal>>> {
        unreachable!()
    }
}

impl ToIcebergLiteralArray for Decimal128Array {
    fn to_primitive_literal_array(
        &self,
        arrow_type: &DataType,
        iceberg_type: &PrimitiveType,
    ) -> Result<Vec<Option<Literal>>> {
        let DataType::Decimal128(arrow_precision, arrow_scale) = arrow_type else {
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
                Ok(self.iter().map(|v| v.map(Literal::decimal)).collect())
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

    fn to_struct_literal_array(
        &self,
        _arrow_type: &DataType,
        _iceberg_type: &StructType,
    ) -> Result<Vec<Option<Literal>>> {
        unreachable!()
    }
}

impl ToIcebergLiteralArray for Date32Array {
    fn to_primitive_literal_array(
        &self,
        _arrow_type: &DataType,
        iceberg_type: &PrimitiveType,
    ) -> Result<Vec<Option<Literal>>> {
        match iceberg_type {
            PrimitiveType::Date => Ok(self.iter().map(|v| v.map(Literal::date)).collect()),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "The type of arrow date32 array is not compatitable with iceberg type {}",
                    iceberg_type
                ),
            )),
        }
    }

    fn to_struct_literal_array(
        &self,
        _arrow_type: &DataType,
        _iceberg_type: &StructType,
    ) -> Result<Vec<Option<Literal>>> {
        unreachable!()
    }
}

impl ToIcebergLiteralArray for Time64MicrosecondArray {
    fn to_primitive_literal_array(
        &self,
        _arrow_type: &DataType,
        iceberg_type: &PrimitiveType,
    ) -> Result<Vec<Option<Literal>>> {
        match iceberg_type {
            PrimitiveType::Time => Ok(self
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

    fn to_struct_literal_array(
        &self,
        _arrow_type: &DataType,
        _iceberg_type: &StructType,
    ) -> Result<Vec<Option<Literal>>> {
        unreachable!()
    }
}

impl ToIcebergLiteralArray for TimestampMicrosecondArray {
    fn to_primitive_literal_array(
        &self,
        _arrow_type: &DataType,
        iceberg_type: &PrimitiveType,
    ) -> Result<Vec<Option<Literal>>> {
        match iceberg_type {
            PrimitiveType::Timestamp => Ok(self
                .iter()
                .map(|v| v.map(Literal::timestamp))
                .collect()),
            PrimitiveType::Timestamptz => Ok(self
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

    fn to_struct_literal_array(
        &self,
        _arrow_type: &DataType,
        _iceberg_type: &StructType,
    ) -> Result<Vec<Option<Literal>>> {
        unreachable!()
    }
}

impl ToIcebergLiteralArray for TimestampNanosecondArray {
    fn to_primitive_literal_array(
        &self,
        _arrow_type: &DataType,
        iceberg_type: &PrimitiveType,
    ) -> Result<Vec<Option<Literal>>> {
        match iceberg_type {
            PrimitiveType::TimestampNs => Ok(self
                .iter()
                .map(|v| v.map(Literal::timestamp_nano))
                .collect()),
            PrimitiveType::TimestamptzNs => Ok(self
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

    fn to_struct_literal_array(
        &self,
        _arrow_type: &DataType,
        _iceberg_type: &StructType,
    ) -> Result<Vec<Option<Literal>>> {
        unreachable!()
    }
}

impl ToIcebergLiteralArray for StringArray {
    fn to_primitive_literal_array(
        &self,
        _arrow_type: &DataType,
        iceberg_type: &PrimitiveType,
    ) -> Result<Vec<Option<Literal>>> {
        match iceberg_type {
            PrimitiveType::String => Ok(self.iter().map(|v| v.map(Literal::string)).collect()),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "The type of arrow string array is not compatitable with iceberg type {}",
                    iceberg_type
                ),
            )),
        }
    }

    fn to_struct_literal_array(
        &self,
        _arrow_type: &DataType,
        _iceberg_type: &StructType,
    ) -> Result<Vec<Option<Literal>>> {
        unreachable!()
    }
}

impl ToIcebergLiteralArray for LargeStringArray {
    fn to_primitive_literal_array(
        &self,
        _arrow_type: &DataType,
        iceberg_type: &PrimitiveType,
    ) -> Result<Vec<Option<Literal>>> {
        match iceberg_type {
            PrimitiveType::String => Ok(self.iter().map(|v| v.map(Literal::string)).collect()),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "The type of arrow large string array is not compatitable with iceberg type {}",
                    iceberg_type
                ),
            )),
        }
    }

    fn to_struct_literal_array(
        &self,
        _arrow_type: &DataType,
        _iceberg_type: &StructType,
    ) -> Result<Vec<Option<Literal>>> {
        unreachable!()
    }
}

impl ToIcebergLiteralArray for BinaryArray {
    fn to_primitive_literal_array(
        &self,
        _arrow_type: &DataType,
        iceberg_type: &PrimitiveType,
    ) -> Result<Vec<Option<Literal>>> {
        match iceberg_type {
            PrimitiveType::Binary => Ok(self
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

    fn to_struct_literal_array(
        &self,
        _arrow_type: &DataType,
        _iceberg_type: &StructType,
    ) -> Result<Vec<Option<Literal>>> {
        unreachable!()
    }
}

impl ToIcebergLiteralArray for LargeBinaryArray {
    fn to_primitive_literal_array(
        &self,
        _arrow_type: &DataType,
        iceberg_type: &PrimitiveType,
    ) -> Result<Vec<Option<Literal>>> {
        match iceberg_type {
            PrimitiveType::Binary => Ok(self
                .iter()
                .map(|v| v.map(|v| Literal::binary(v.to_vec())))
                .collect()),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "The type of arrow large binary array is not compatitable with iceberg type {}",
                    iceberg_type
                ),
            )),
        }
    }

    fn to_struct_literal_array(
        &self,
        _arrow_type: &DataType,
        _iceberg_type: &StructType,
    ) -> Result<Vec<Option<Literal>>> {
        unreachable!()
    }
}

impl ToIcebergLiteralArray for StructArray {
    fn to_primitive_literal_array(
        &self,
        _arrow_type: &DataType,
        _iceberg_type: &PrimitiveType,
    ) -> Result<Vec<Option<Literal>>> {
        unreachable!()
    }

    fn to_struct_literal_array(
        &self,
        arrow_type: &DataType,
        iceberg_type: &StructType,
    ) -> Result<Vec<Option<Literal>>> {
        let DataType::Struct(arrow_struct_fields) = arrow_type else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "The type of arrow struct array is not a struct type",
            ));
        };

        if self.columns().len() != iceberg_type.fields().len()
            || arrow_struct_fields.len() != iceberg_type.fields().len()
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "The type of arrow struct array is not compatitable with iceberg struct type",
            ));
        }

        let mut columns = Vec::with_capacity(self.columns().len());

        for ((array, arrow_type), iceberg_field) in self
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
                (DataType::Null, _) => {
                    if iceberg_field.required {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            "column in arrow array should not be optional",
                        ));
                    }
                    columns.push(vec![None; array.len()]);
                }
                (DataType::Boolean, Type::Primitive(primitive_type)) => {
                    let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                    columns.push(array.to_primitive_literal_array(arrow_type, primitive_type)?);
                }
                (DataType::Int16, Type::Primitive(primitive_type)) => {
                    let array = array.as_any().downcast_ref::<Int16Array>().unwrap();
                    columns.push(array.to_primitive_literal_array(arrow_type, primitive_type)?);
                }
                (DataType::Int32, Type::Primitive(primitive_type)) => {
                    let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
                    columns.push(array.to_primitive_literal_array(arrow_type, primitive_type)?);
                }
                (DataType::Int64, Type::Primitive(primitive_type)) => {
                    let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                    columns.push(array.to_primitive_literal_array(arrow_type, primitive_type)?);
                }
                (DataType::Float32, Type::Primitive(primitive_type)) => {
                    let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                    columns.push(array.to_primitive_literal_array(arrow_type, primitive_type)?);
                }
                (DataType::Float64, Type::Primitive(primitive_type)) => {
                    let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                    columns.push(array.to_primitive_literal_array(arrow_type, primitive_type)?);
                }
                (DataType::Decimal128(_, _), Type::Primitive(primitive_type)) => {
                    let array = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
                    columns.push(array.to_primitive_literal_array(arrow_type, primitive_type)?);
                }
                (DataType::Date32, Type::Primitive(primitive_type)) => {
                    let array = array.as_any().downcast_ref::<Date32Array>().unwrap();
                    columns.push(array.to_primitive_literal_array(arrow_type, primitive_type)?);
                }
                (DataType::Time64(TimeUnit::Microsecond), Type::Primitive(primitive_type)) => {
                    let array = array
                        .as_any()
                        .downcast_ref::<Time64MicrosecondArray>()
                        .unwrap();
                    columns.push(array.to_primitive_literal_array(arrow_type, primitive_type)?);
                }
                (
                    DataType::Timestamp(TimeUnit::Microsecond, _),
                    Type::Primitive(primitive_type),
                ) => {
                    let array = array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap();
                    columns.push(array.to_primitive_literal_array(arrow_type, primitive_type)?);
                }
                (DataType::Timestamp(TimeUnit::Nanosecond, _), Type::Primitive(primitive_type)) => {
                    let array = array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap();
                    columns.push(array.to_primitive_literal_array(arrow_type, primitive_type)?);
                }
                (DataType::Utf8, Type::Primitive(primitive_type)) => {
                    let array = array.as_any().downcast_ref::<StringArray>().unwrap();
                    columns.push(array.to_primitive_literal_array(arrow_type, primitive_type)?);
                }
                (DataType::LargeUtf8, Type::Primitive(primitive_type)) => {
                    let array = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
                    columns.push(array.to_primitive_literal_array(arrow_type, primitive_type)?);
                }
                (DataType::Binary, Type::Primitive(primitive_type)) => {
                    let array = array.as_any().downcast_ref::<BinaryArray>().unwrap();
                    columns.push(array.to_primitive_literal_array(arrow_type, primitive_type)?);
                }
                (DataType::LargeBinary, Type::Primitive(primitive_type)) => {
                    let array = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
                    columns.push(array.to_primitive_literal_array(arrow_type, primitive_type)?);
                }
                (DataType::Struct(_), Type::Struct(struct_type)) => {
                    let array = array.as_any().downcast_ref::<StructArray>().unwrap();
                    columns.push(array.to_struct_literal_array(arrow_type, struct_type)?);
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

        let struct_literal_len = columns.first().map(|column| column.len()).unwrap_or(0);
        let mut struct_literals = Vec::with_capacity(struct_literal_len);
        let mut columns_iters = columns
            .into_iter()
            .map(|column| column.into_iter())
            .collect::<Vec<_>>();

        for row_idx in 0..struct_literal_len {
            if self.is_null(row_idx) {
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

/// Convert arrow struct array to iceberg struct value array.
pub fn arrow_struct_to_iceberg_struct(
    struct_array: &StructArray,
    ty: StructType,
) -> Result<Vec<Option<Literal>>> {
    struct_array.to_struct_literal_array(struct_array.data_type(), &ty)
}

#[cfg(test)]
mod test {
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

        let result = arrow_struct_to_iceberg_struct(&struct_array, iceberg_struct_type).unwrap();

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
        let result = arrow_struct_to_iceberg_struct(&struct_array, iceberg_struct_type).unwrap();
        assert_eq!(result, vec![None; 3]);
    }

    #[test]
    fn test_empty_struct() {
        let struct_array = StructArray::new_null(Fields::empty(), 3);
        let iceberg_struct_type = StructType::new(vec![]);
        let result = arrow_struct_to_iceberg_struct(&struct_array, iceberg_struct_type).unwrap();
        assert_eq!(result, vec![None; 0]);
    }
}
