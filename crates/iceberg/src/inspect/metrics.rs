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

use std::any::type_name;
use std::sync::Arc;

use arrow_array::builder::{
    ArrayBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, FixedSizeBinaryBuilder,
    Float32Builder, Float64Builder, Int32Builder, Int64Builder, LargeBinaryBuilder, StringBuilder,
    StructBuilder, TimestampMicrosecondBuilder, TimestampNanosecondBuilder,
};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Decimal128Type, Float32Type, Float64Type, Int32Type, Int64Type,
    TimestampMicrosecondType, TimestampNanosecondType,
};
use arrow_array::{ArrayRef, StructArray};
use arrow_schema::{DataType, FieldRef, Fields};
use itertools::Itertools;
use rust_decimal::prelude::ToPrimitive;

use crate::arrow::{get_arrow_datum, schema_to_arrow_schema, type_to_arrow_type};
use crate::spec::{
    DataFile, Datum, NestedField, NestedFieldRef, PrimitiveType, Schema, StructType, Type,
};
use crate::{Error, ErrorKind, Result};

/// Metrics for a column in a data file.
struct ColumnMetrics {
    column_size: Option<i64>,
    value_count: Option<i64>,
    null_value_count: Option<i64>,
    nan_value_count: Option<i64>,
    lower_bound: Option<Datum>,
    upper_bound: Option<Datum>,
}

/// Builder for the `readable_metrics` struct in metadata tables.
pub(crate) struct ReadableMetricsStructBuilder {
    column_builders: Vec<ReadableColumnMetricsStructBuilder>,
    column_fields: Fields,
}

impl ReadableMetricsStructBuilder {
    /// Calculates a dynamic schema for `readable_metrics` to add to metadata tables. The type
    /// will be a nested struct containing all primitive columns in the data table. Within the
    /// struct's fields are structs that represent [`ColumnMetrics`].
    ///
    ///
    /// We take the table's schema to get the set of fields in the table. We also take the manifest
    /// entry schema to get the highest field ID in the entries metadata table to know which field
    /// ID to begin with.
    pub fn readable_metrics_schema(
        data_table_schema: &Schema,
        manifest_entry_schema: &Schema,
    ) -> Result<Schema> {
        let mut field_ids = IncrementingFieldId(manifest_entry_schema.highest_field_id() + 1);
        let mut column_metrics_fields: Vec<NestedFieldRef> = Vec::new();

        let mut primitive_fields: Vec<&NestedFieldRef> = data_table_schema
            .as_struct()
            .fields()
            .iter()
            .filter(|field| field.field_type.is_primitive())
            .collect_vec();
        primitive_fields.sort_by_key(|field| field.name.clone());

        for field in primitive_fields {
            // We can expect a primitive type because we filtered for primitive fields above
            let primitive_type = field.field_type.as_primitive_type().expect("is primitive");
            let metrics_schema_for_field =
                ReadableColumnMetricsStructBuilder::schema(&mut field_ids, primitive_type)?;

            column_metrics_fields.push(Arc::new(NestedField::required(
                field_ids.next_id(),
                &field.name,
                Type::Struct(metrics_schema_for_field.as_struct().clone()),
            )));
        }

        Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                field_ids.next_id(),
                "readable_metrics",
                Type::Struct(StructType::new(column_metrics_fields)),
            ))])
            .build()
    }

    /// Takes a table schema and a readable metrics schema built by
    /// [`Self::readable_metrics_schema`].
    pub fn new(
        data_table_schema: &Schema,
        readable_metrics_schema: &StructType,
    ) -> Result<ReadableMetricsStructBuilder> {
        let DataType::Struct(column_fields) =
            type_to_arrow_type(&Type::Struct(readable_metrics_schema.clone()))?
        else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Converted Arrow type was not struct",
            ));
        };

        let column_builders = readable_metrics_schema
            .fields()
            .iter()
            .map(|column_metrics_field| {
                let fields = column_metrics_field
                    .field_type
                    .clone()
                    .to_struct_type()
                    .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "Expected struct type"))?
                    .fields()
                    .iter()
                    .cloned()
                    .collect_vec();
                let column_metrics_schema = Schema::builder().with_fields(fields).build()?;
                let data_field = data_table_schema
                    .field_by_name(&column_metrics_field.name)
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!(
                                "{} in readable metrics schema does not exist in table",
                                &column_metrics_field.name
                            ),
                        )
                    })?;
                let primitive_type = data_field
                    .field_type
                    .as_primitive_type()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::FeatureUnsupported,
                            "Readable metrics only supported for primitive types",
                        )
                    })?
                    .clone();

                ReadableColumnMetricsStructBuilder::new(
                    data_field.id,
                    primitive_type,
                    column_metrics_schema,
                )
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            column_fields,
            column_builders,
        })
    }

    pub fn append(&mut self, data_file: &DataFile) -> Result<()> {
        for column_builder in &mut self.column_builders {
            column_builder.append_data_file(data_file)?;
        }
        Ok(())
    }

    pub fn finish(&mut self) -> StructArray {
        let arrays: Vec<ArrayRef> = self
            .column_builders
            .iter_mut()
            .map::<ArrayRef, _>(|builder| Arc::new(builder.finish()))
            .collect();

        let inner_arrays: Vec<(FieldRef, ArrayRef)> = self
            .column_fields
            .into_iter()
            .cloned()
            .zip_eq(arrays)
            .collect_vec();

        StructArray::from(inner_arrays)
    }
}

struct ReadableColumnMetricsStructBuilder {
    /// Field id of the column in the data table.
    field_id: i32,
    /// Type of the column in the data table for which these are the metrics.
    primitive_type: PrimitiveType,
    /// The struct builder for this column's readable metrics.
    struct_builder: StructBuilder,
}

/// Builds a readable metrics struct for a single column.
///
/// For reference, see [Java][1] and [Python][2] implementations.
///
/// [1]: https://github.com/apache/iceberg/blob/4a432839233f2343a9eae8255532f911f06358ef/core/src/main/java/org/apache/iceberg/MetricsUtil.java#L337
/// [2]: https://github.com/apache/iceberg-python/blob/a051584a3684392d2db6556449eb299145d47d15/pyiceberg/table/inspect.py#L101-L110
impl ReadableColumnMetricsStructBuilder {
    /// Return the readable metrics schema for a column of the given data type.
    fn schema(field_ids: &mut IncrementingFieldId, data_type: &PrimitiveType) -> Result<Schema> {
        Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::optional(
                    field_ids.next_id(),
                    "column_size",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    field_ids.next_id(),
                    "value_count",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    field_ids.next_id(),
                    "null_value_count",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    field_ids.next_id(),
                    "nan_value_count",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    field_ids.next_id(),
                    "lower_bound",
                    Type::Primitive(data_type.clone()),
                )),
                Arc::new(NestedField::optional(
                    field_ids.next_id(),
                    "upper_bound",
                    Type::Primitive(data_type.clone()),
                )),
            ])
            .build()
    }

    fn new(field_id: i32, primitive_type: PrimitiveType, schema: Schema) -> Result<Self> {
        Ok(Self {
            field_id,
            primitive_type,
            struct_builder: StructBuilder::from_fields(schema_to_arrow_schema(&schema)?.fields, 0),
        })
    }

    fn append_data_file(&mut self, data_file: &DataFile) -> Result<()> {
        let column_metrics = Self::get_column_metrics_from_data_file(&self.field_id, data_file);
        self.append_column_metrics(column_metrics)
    }

    fn get_column_metrics_from_data_file(field_id: &i32, data_file: &DataFile) -> ColumnMetrics {
        ColumnMetrics {
            column_size: data_file.column_sizes().get(field_id).map(|&v| v as i64),
            value_count: data_file.value_counts().get(field_id).map(|&v| v as i64),
            null_value_count: data_file
                .null_value_counts()
                .get(field_id)
                .map(|&v| v as i64),
            nan_value_count: data_file
                .nan_value_counts()
                .get(field_id)
                .map(|&v| v as i64),
            lower_bound: data_file.lower_bounds().get(field_id).cloned(),
            upper_bound: data_file.upper_bounds().get(field_id).cloned(),
        }
    }

    fn append_column_metrics(&mut self, column_metrics: ColumnMetrics) -> Result<()> {
        let ColumnMetrics {
            column_size,
            value_count,
            null_value_count,
            nan_value_count,
            lower_bound,
            upper_bound,
        } = column_metrics;

        self.field_builder::<Int64Builder>(0)
            .append_option(column_size);
        self.field_builder::<Int64Builder>(1)
            .append_option(value_count);
        self.field_builder::<Int64Builder>(2)
            .append_option(null_value_count);
        self.field_builder::<Int64Builder>(3)
            .append_option(nan_value_count);
        self.append_bounds(4, lower_bound)?;
        self.append_bounds(5, upper_bound)?;
        self.struct_builder.append(true);
        Ok(())
    }

    fn append_bounds(&mut self, index: usize, datum: Option<Datum>) -> Result<()> {
        let datum = datum.map(|datum| get_arrow_datum(&datum)).transpose()?;
        let array = if let Some(datum) = &datum {
            let (array, is_scalar) = datum.get();
            if is_scalar {
                Some(array)
            } else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Can only append scalar datum",
                ));
            }
        } else {
            None
        };

        match self.primitive_type {
            PrimitiveType::Boolean => {
                self.field_builder::<BooleanBuilder>(index)
                    .append_option(array.map(|array| array.as_boolean().value(0)));
            }
            PrimitiveType::Int => {
                self.field_builder::<Int32Builder>(index)
                    .append_option(array.map(|array| array.as_primitive::<Int32Type>().value(0)));
            }
            PrimitiveType::Long => {
                self.field_builder::<Int64Builder>(index)
                    .append_option(array.map(|array| array.as_primitive::<Int64Type>().value(0)));
            }
            PrimitiveType::Float => {
                self.field_builder::<Float32Builder>(index)
                    .append_option(array.map(|array| array.as_primitive::<Float32Type>().value(0)));
            }
            PrimitiveType::Double => {
                self.field_builder::<Float64Builder>(index)
                    .append_option(array.map(|array| array.as_primitive::<Float64Type>().value(0)));
            }
            PrimitiveType::Date => {
                self.field_builder::<Date32Builder>(index)
                    .append_option(array.map(|array| array.as_primitive::<Date32Type>().value(0)));
            }
            PrimitiveType::Time | PrimitiveType::Timestamp | PrimitiveType::Timestamptz => {
                self.field_builder::<TimestampMicrosecondBuilder>(index)
                    .append_option(
                        array
                            .map(|array| array.as_primitive::<TimestampMicrosecondType>().value(0)),
                    );
            }
            PrimitiveType::TimestampNs | PrimitiveType::TimestamptzNs => {
                self.field_builder::<TimestampNanosecondBuilder>(index)
                    .append_option(
                        array.map(|array| array.as_primitive::<TimestampNanosecondType>().value(0)),
                    );
            }
            PrimitiveType::String => {
                self.field_builder::<StringBuilder>(index)
                    .append_option(array.map(|array| array.as_string::<i32>().value(0)));
            }
            PrimitiveType::Binary => {
                self.field_builder::<LargeBinaryBuilder>(index)
                    .append_option(array.map(|array| array.as_binary::<i64>().value(0)));
            }
            PrimitiveType::Decimal { .. } => {
                self.field_builder::<Decimal128Builder>(index)
                    .append_option(
                        array.map(|array| array.as_primitive::<Decimal128Type>().value(0)),
                    );
            }
            PrimitiveType::Fixed(len) => {
                if len.to_i32().is_some() {
                    let builder = self.field_builder::<FixedSizeBinaryBuilder>(index);
                    // FixedSizeBinaryBuilder does not have append_option
                    match array {
                        Some(array) => {
                            builder.append_value(array.as_fixed_size_binary().value(0))?;
                        }
                        None => {
                            builder.append_null();
                        }
                    }
                } else {
                    self.field_builder::<LargeBinaryBuilder>(index)
                        .append_option(array.map(|array| array.as_binary::<i64>().value(0)));
                }
            }
            PrimitiveType::Uuid => {
                let builder = self.field_builder::<FixedSizeBinaryBuilder>(index);
                // FixedSizeBinaryBuilder does not have append_option
                match array {
                    Some(array) => {
                        builder.append_value(array.as_fixed_size_binary().value(0))?;
                    }
                    None => {
                        builder.append_null();
                    }
                }
            }
        };
        Ok(())
    }

    fn finish(&mut self) -> StructArray {
        self.struct_builder.finish()
    }

    // Shorthand to select a field builder with a specific type.
    fn field_builder<T: ArrayBuilder>(&mut self, index: usize) -> &mut T {
        match self.struct_builder.field_builder::<T>(index) {
            Some(builder) => builder,
            None => panic!(
                "Field builder not found for index {index} and type {}",
                type_name::<T>(),
            ),
        }
    }
}

/// Helper to serve increment field ids.
struct IncrementingFieldId(i32);

impl IncrementingFieldId {
    fn next_id(&mut self) -> i32 {
        let current = self.0;
        self.0 += 1;
        current
    }
}
