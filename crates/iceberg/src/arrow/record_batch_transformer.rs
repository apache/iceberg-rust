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
    Array as ArrowArray, ArrayRef, BinaryArray, BooleanArray, Date32Array, Float32Array,
    Float64Array, Int32Array, Int64Array, NullArray, RecordBatch, RecordBatchOptions, StringArray,
};
use arrow_cast::cast;
use arrow_schema::{
    DataType, FieldRef, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef, SchemaRef,
};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::arrow::schema_to_arrow_schema;
use crate::spec::{Literal, PrimitiveLiteral, Schema as IcebergSchema};
use crate::{Error, ErrorKind, Result};

/// Indicates how a particular column in a processed RecordBatch should
/// be sourced.
#[derive(Debug)]
pub(crate) enum ColumnSource {
    // signifies that a column should be passed through unmodified
    // from the file's RecordBatch
    PassThrough {
        source_index: usize,
    },

    // signifies that a column from the file's RecordBatch has undergone
    // type promotion so the source column with the given index needs
    // to be promoted to the specified type
    Promote {
        target_type: DataType,
        source_index: usize,
    },

    // Signifies that a new column has been inserted before the column
    // with index `index`. (we choose "before" rather than "after" so
    // that we can use usize; if we insert after, then we need to
    // be able to store -1 here to signify that a new
    // column is to be added at the front of the column list).
    // If multiple columns need to be inserted at a given
    // location, they should all be given the same index, as the index
    // here refers to the original RecordBatch, not the interim state after
    // a preceding operation.
    Add {
        target_type: DataType,
        value: Option<PrimitiveLiteral>,
    },
    // The iceberg spec refers to other permissible schema evolution actions
    // (see https://iceberg.apache.org/spec/#schema-evolution):
    // renaming fields, deleting fields and reordering fields.
    // Renames only affect the schema of the RecordBatch rather than the
    // columns themselves, so a single updated cached schema can
    // be re-used and no per-column actions are required.
    // Deletion and Reorder can be achieved without needing this
    // post-processing step by using the projection mask.
}

#[derive(Debug)]
enum BatchTransform {
    // Indicates that no changes need to be performed to the RecordBatches
    // coming in from the stream and that they can be passed through
    // unmodified
    PassThrough,

    Modify {
        // Every transformed RecordBatch will have the same schema. We create the
        // target just once and cache it here. Helpfully, Arc<Schema> is needed in
        // the constructor for RecordBatch, so we don't need an expensive copy
        // each time we build a new RecordBatch
        target_schema: Arc<ArrowSchema>,

        // Indicates how each column in the target schema is derived.
        operations: Vec<ColumnSource>,
    },

    // Sometimes only the schema will need modifying, for example when
    // the column names have changed vs the file, but not the column types.
    // we can avoid a heap allocation per RecordBach in this case by retaining
    // the existing column Vec.
    ModifySchema {
        target_schema: Arc<ArrowSchema>,
    },
}

#[derive(Debug)]
enum SchemaComparison {
    Equivalent,
    NameChangesOnly,
    Different,
}

#[derive(Debug)]
pub(crate) struct RecordBatchTransformer {
    snapshot_schema: Arc<IcebergSchema>,
    projected_iceberg_field_ids: Vec<i32>,

    // BatchTransform gets lazily constructed based on the schema of
    // the first RecordBatch we receive from the file
    batch_transform: Option<BatchTransform>,
}

impl RecordBatchTransformer {
    /// Build a RecordBatchTransformer for a given
    /// Iceberg snapshot schema and list of projected field ids.
    pub(crate) fn build(
        snapshot_schema: Arc<IcebergSchema>,
        projected_iceberg_field_ids: &[i32],
    ) -> Self {
        let projected_iceberg_field_ids = projected_iceberg_field_ids.to_vec();

        Self {
            snapshot_schema,
            projected_iceberg_field_ids,
            batch_transform: None,
        }
    }

    pub(crate) fn process_record_batch(
        &mut self,
        record_batch: RecordBatch,
    ) -> Result<RecordBatch> {
        Ok(match &self.batch_transform {
            Some(BatchTransform::PassThrough) => record_batch,
            Some(BatchTransform::Modify {
                target_schema,
                operations,
            }) => {
                let options = RecordBatchOptions::default()
                    .with_match_field_names(false)
                    .with_row_count(Some(record_batch.num_rows()));
                RecordBatch::try_new_with_options(
                    target_schema.clone(),
                    self.transform_columns(record_batch.columns(), operations)?,
                    &options,
                )?
            }
            Some(BatchTransform::ModifySchema { target_schema }) => {
                let options = RecordBatchOptions::default()
                    .with_match_field_names(false)
                    .with_row_count(Some(record_batch.num_rows()));
                RecordBatch::try_new_with_options(
                    target_schema.clone(),
                    record_batch.columns().to_vec(),
                    &options,
                )?
            }
            None => {
                self.batch_transform = Some(Self::generate_batch_transform(
                    record_batch.schema_ref(),
                    self.snapshot_schema.as_ref(),
                    &self.projected_iceberg_field_ids,
                )?);

                self.process_record_batch(record_batch)?
            }
        })
    }

    // Compare the schema of the incoming RecordBatches to the schema of
    // the Iceberg snapshot to determine what, if any, transformation
    // needs to be applied. If the schemas match, we return BatchTransform::PassThrough
    // to indicate that no changes need to be made. Otherwise, we return a
    // BatchTransform::Modify containing the target RecordBatch schema and
    // the list of `ColumnSource`s that indicate how to source each column in
    // the resulting RecordBatches.
    fn generate_batch_transform(
        source_schema: &ArrowSchemaRef,
        snapshot_schema: &IcebergSchema,
        projected_iceberg_field_ids: &[i32],
    ) -> Result<BatchTransform> {
        let mapped_unprojected_arrow_schema = Arc::new(schema_to_arrow_schema(snapshot_schema)?);
        let field_id_to_mapped_schema_map =
            Self::build_field_id_to_arrow_schema_map(&mapped_unprojected_arrow_schema)?;

        // Create a new arrow schema by selecting fields from mapped_unprojected,
        // in the order of the field ids in projected_iceberg_field_ids
        let fields: Result<Vec<_>> = projected_iceberg_field_ids
            .iter()
            .map(|field_id| {
                Ok(field_id_to_mapped_schema_map
                    .get(field_id)
                    .ok_or(Error::new(ErrorKind::Unexpected, "field not found"))?
                    .0
                    .clone())
            })
            .collect();

        let target_schema = Arc::new(ArrowSchema::new(fields?));

        match Self::compare_schemas(source_schema, &target_schema) {
            SchemaComparison::Equivalent => Ok(BatchTransform::PassThrough),
            SchemaComparison::NameChangesOnly => Ok(BatchTransform::ModifySchema { target_schema }),
            SchemaComparison::Different => Ok(BatchTransform::Modify {
                operations: Self::generate_transform_operations(
                    source_schema,
                    snapshot_schema,
                    projected_iceberg_field_ids,
                    field_id_to_mapped_schema_map,
                )?,
                target_schema,
            }),
        }
    }

    /// Compares the source and target schemas
    /// Determines if they have changed in any meaningful way:
    ///  * If they have different numbers of fields, then we need to modify
    ///    the incoming RecordBatch schema AND columns
    ///  * If they have the same number of fields, but some of them differ in
    ///    either data type or nullability, then we need to modify the
    ///    incoming RecordBatch schema AND columns
    ///  * If the schemas differ only in the column names, then we need
    ///    to modify the RecordBatch schema BUT we can keep the
    ///    original column data unmodified
    ///  * If the schemas are identical (or differ only in inconsequential
    ///    ways) then we can pass through the original RecordBatch unmodified
    fn compare_schemas(
        source_schema: &ArrowSchemaRef,
        target_schema: &ArrowSchemaRef,
    ) -> SchemaComparison {
        if source_schema.fields().len() != target_schema.fields().len() {
            return SchemaComparison::Different;
        }

        let mut names_changed = false;

        for (source_field, target_field) in source_schema
            .fields()
            .iter()
            .zip(target_schema.fields().iter())
        {
            if source_field.data_type() != target_field.data_type()
                || source_field.is_nullable() != target_field.is_nullable()
            {
                return SchemaComparison::Different;
            }

            if source_field.name() != target_field.name() {
                names_changed = true;
            }
        }

        if names_changed {
            SchemaComparison::NameChangesOnly
        } else {
            SchemaComparison::Equivalent
        }
    }

    fn generate_transform_operations(
        source_schema: &ArrowSchemaRef,
        snapshot_schema: &IcebergSchema,
        projected_iceberg_field_ids: &[i32],
        field_id_to_mapped_schema_map: HashMap<i32, (FieldRef, usize)>,
    ) -> Result<Vec<ColumnSource>> {
        let field_id_to_source_schema_map =
            Self::build_field_id_to_arrow_schema_map(source_schema)?;

        projected_iceberg_field_ids.iter().map(|field_id|{
            let (target_field, _) = field_id_to_mapped_schema_map.get(field_id).ok_or(
                Error::new(ErrorKind::Unexpected, "could not find field in schema")
            )?;
            let target_type = target_field.data_type();

            Ok(if let Some((source_field, source_index)) = field_id_to_source_schema_map.get(field_id) {
                // column present in source

                if source_field.data_type().equals_datatype(target_type) {
                    // no promotion required
                    ColumnSource::PassThrough {
                        source_index: *source_index
                    }
                } else {
                    // promotion required
                    ColumnSource::Promote {
                        target_type: target_type.clone(),
                        source_index: *source_index,
                    }
                }
            } else {
                // column must be added
                let iceberg_field = snapshot_schema.field_by_id(*field_id).ok_or(
                    Error::new(ErrorKind::Unexpected, "Field not found in snapshot schema")
                )?;

                let default_value = if let Some(iceberg_default_value) =
                    &iceberg_field.initial_default
                {
                    let Literal::Primitive(primitive_literal) = iceberg_default_value else {
                        return Err(Error::new(
                            ErrorKind::Unexpected,
                            format!("Default value for column must be primitive type, but encountered {:?}", iceberg_default_value)
                        ));
                    };
                    Some(primitive_literal.clone())
                } else {
                    None
                };

                ColumnSource::Add {
                    value: default_value,
                    target_type: target_type.clone(),
                }
            })
        }).collect()
    }

    fn build_field_id_to_arrow_schema_map(
        source_schema: &SchemaRef,
    ) -> Result<HashMap<i32, (FieldRef, usize)>> {
        let mut field_id_to_source_schema = HashMap::new();
        for (source_field_idx, source_field) in source_schema.fields.iter().enumerate() {
            let this_field_id = source_field
                .metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "field ID not present in parquet metadata",
                    )
                })?
                .parse()
                .map_err(|e| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("field id not parseable as an i32: {}", e),
                    )
                })?;

            field_id_to_source_schema
                .insert(this_field_id, (source_field.clone(), source_field_idx));
        }

        Ok(field_id_to_source_schema)
    }

    fn transform_columns(
        &self,
        columns: &[Arc<dyn ArrowArray>],
        operations: &[ColumnSource],
    ) -> Result<Vec<Arc<dyn ArrowArray>>> {
        if columns.is_empty() {
            return Ok(columns.to_vec());
        }
        let num_rows = columns[0].len();

        operations
            .iter()
            .map(|op| {
                Ok(match op {
                    ColumnSource::PassThrough { source_index } => columns[*source_index].clone(),

                    ColumnSource::Promote {
                        target_type,
                        source_index,
                    } => cast(&*columns[*source_index], target_type)?,

                    ColumnSource::Add { target_type, value } => {
                        Self::create_column(target_type, value, num_rows)?
                    }
                })
            })
            .collect()
    }

    fn create_column(
        target_type: &DataType,
        prim_lit: &Option<PrimitiveLiteral>,
        num_rows: usize,
    ) -> Result<ArrayRef> {
        Ok(match (target_type, prim_lit) {
            (DataType::Boolean, Some(PrimitiveLiteral::Boolean(value))) => {
                Arc::new(BooleanArray::from(vec![*value; num_rows]))
            }
            (DataType::Boolean, None) => {
                let vals: Vec<Option<bool>> = vec![None; num_rows];
                Arc::new(BooleanArray::from(vals))
            }
            (DataType::Int32, Some(PrimitiveLiteral::Int(value))) => {
                Arc::new(Int32Array::from(vec![*value; num_rows]))
            }
            (DataType::Int32, None) => {
                let vals: Vec<Option<i32>> = vec![None; num_rows];
                Arc::new(Int32Array::from(vals))
            }
            (DataType::Date32, Some(PrimitiveLiteral::Int(value))) => {
                Arc::new(Date32Array::from(vec![*value; num_rows]))
            }
            (DataType::Date32, None) => {
                let vals: Vec<Option<i32>> = vec![None; num_rows];
                Arc::new(Date32Array::from(vals))
            }
            (DataType::Int64, Some(PrimitiveLiteral::Long(value))) => {
                Arc::new(Int64Array::from(vec![*value; num_rows]))
            }
            (DataType::Int64, None) => {
                let vals: Vec<Option<i64>> = vec![None; num_rows];
                Arc::new(Int64Array::from(vals))
            }
            (DataType::Float32, Some(PrimitiveLiteral::Float(value))) => {
                Arc::new(Float32Array::from(vec![value.0; num_rows]))
            }
            (DataType::Float32, None) => {
                let vals: Vec<Option<f32>> = vec![None; num_rows];
                Arc::new(Float32Array::from(vals))
            }
            (DataType::Float64, Some(PrimitiveLiteral::Double(value))) => {
                Arc::new(Float64Array::from(vec![value.0; num_rows]))
            }
            (DataType::Float64, None) => {
                let vals: Vec<Option<f64>> = vec![None; num_rows];
                Arc::new(Float64Array::from(vals))
            }
            (DataType::Utf8, Some(PrimitiveLiteral::String(value))) => {
                Arc::new(StringArray::from(vec![value.clone(); num_rows]))
            }
            (DataType::Utf8, None) => {
                let vals: Vec<Option<String>> = vec![None; num_rows];
                Arc::new(StringArray::from(vals))
            }
            (DataType::Binary, Some(PrimitiveLiteral::Binary(value))) => {
                Arc::new(BinaryArray::from_vec(vec![value; num_rows]))
            }
            (DataType::Binary, None) => {
                let vals: Vec<Option<&[u8]>> = vec![None; num_rows];
                Arc::new(BinaryArray::from_opt_vec(vals))
            }
            (DataType::Null, _) => Arc::new(NullArray::new(num_rows)),
            (dt, _) => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("unexpected target column type {}", dt),
                ));
            }
        })
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{
        Array, Date32Array, Float32Array, Float64Array, Int32Array, Int64Array, RecordBatch,
        StringArray,
    };
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    use crate::arrow::record_batch_transformer::RecordBatchTransformer;
    use crate::spec::{Literal, NestedField, PrimitiveType, Schema, Type};

    #[test]
    fn build_field_id_to_source_schema_map_works() {
        let arrow_schema = arrow_schema_already_same_as_target();

        let result =
            RecordBatchTransformer::build_field_id_to_arrow_schema_map(&arrow_schema).unwrap();

        let expected = HashMap::from_iter([
            (10, (arrow_schema.fields()[0].clone(), 0)),
            (11, (arrow_schema.fields()[1].clone(), 1)),
            (12, (arrow_schema.fields()[2].clone(), 2)),
            (14, (arrow_schema.fields()[3].clone(), 3)),
            (15, (arrow_schema.fields()[4].clone(), 4)),
        ]);

        assert!(result.eq(&expected));
    }

    #[test]
    fn processor_returns_properly_shaped_record_batch_when_no_schema_migration_required() {
        let snapshot_schema = Arc::new(iceberg_table_schema());
        let projected_iceberg_field_ids = [13, 14];

        let mut inst = RecordBatchTransformer::build(snapshot_schema, &projected_iceberg_field_ids);

        let result = inst
            .process_record_batch(source_record_batch_no_migration_required())
            .unwrap();

        let expected = source_record_batch_no_migration_required();

        assert_eq!(result, expected);
    }

    #[test]
    fn processor_returns_properly_shaped_record_batch_when_schema_migration_required() {
        let snapshot_schema = Arc::new(iceberg_table_schema());
        let projected_iceberg_field_ids = [10, 11, 12, 14, 15]; // a, b, c, e, f

        let mut inst = RecordBatchTransformer::build(snapshot_schema, &projected_iceberg_field_ids);

        let result = inst.process_record_batch(source_record_batch()).unwrap();

        let expected = expected_record_batch_migration_required();

        assert_eq!(result, expected);
    }

    #[test]
    fn schema_evolution_adds_date_column_with_nulls() {
        // Reproduces TestSelect.readAndWriteWithBranchAfterSchemaChange from iceberg-spark.
        // When reading old snapshots after adding a DATE column, the transformer must
        // populate the new column with NULL values since old files lack this field.
        let snapshot_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::optional(3, "date_col", Type::Primitive(PrimitiveType::Date))
                        .into(),
                ])
                .build()
                .unwrap(),
        );
        let projected_iceberg_field_ids = [1, 2, 3];

        let mut transformer =
            RecordBatchTransformer::build(snapshot_schema, &projected_iceberg_field_ids);

        let file_schema = Arc::new(ArrowSchema::new(vec![
            simple_field("id", DataType::Int32, false, "1"),
            simple_field("name", DataType::Utf8, true, "2"),
        ]));

        let file_batch = RecordBatch::try_new(file_schema, vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![
                Some("Alice"),
                Some("Bob"),
                Some("Charlie"),
            ])),
        ])
        .unwrap();

        let result = transformer.process_record_batch(file_batch).unwrap();

        assert_eq!(result.num_columns(), 3);
        assert_eq!(result.num_rows(), 3);

        let id_column = result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_column.values(), &[1, 2, 3]);

        let name_column = result
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_column.value(0), "Alice");
        assert_eq!(name_column.value(1), "Bob");
        assert_eq!(name_column.value(2), "Charlie");

        let date_column = result
            .column(2)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        assert!(date_column.is_null(0));
        assert!(date_column.is_null(1));
        assert!(date_column.is_null(2));
    }

    pub fn source_record_batch() -> RecordBatch {
        RecordBatch::try_new(
            arrow_schema_promotion_addition_and_renaming_required(),
            vec![
                Arc::new(Int32Array::from(vec![Some(1001), Some(1002), Some(1003)])), // b
                Arc::new(Float32Array::from(vec![
                    Some(12.125),
                    Some(23.375),
                    Some(34.875),
                ])), // c
                Arc::new(Int32Array::from(vec![Some(2001), Some(2002), Some(2003)])), // d
                Arc::new(StringArray::from(vec![
                    Some("Apache"),
                    Some("Iceberg"),
                    Some("Rocks"),
                ])), // e
            ],
        )
        .unwrap()
    }

    pub fn source_record_batch_no_migration_required() -> RecordBatch {
        RecordBatch::try_new(
            arrow_schema_no_promotion_addition_or_renaming_required(),
            vec![
                Arc::new(Int32Array::from(vec![Some(2001), Some(2002), Some(2003)])), // d
                Arc::new(StringArray::from(vec![
                    Some("Apache"),
                    Some("Iceberg"),
                    Some("Rocks"),
                ])), // e
            ],
        )
        .unwrap()
    }

    pub fn expected_record_batch_migration_required() -> RecordBatch {
        RecordBatch::try_new(arrow_schema_already_same_as_target(), vec![
            Arc::new(StringArray::from(Vec::<Option<String>>::from([
                None, None, None,
            ]))), // a
            Arc::new(Int64Array::from(vec![Some(1001), Some(1002), Some(1003)])), // b
            Arc::new(Float64Array::from(vec![
                Some(12.125),
                Some(23.375),
                Some(34.875),
            ])), // c
            Arc::new(StringArray::from(vec![
                Some("Apache"),
                Some("Iceberg"),
                Some("Rocks"),
            ])), // e (d skipped by projection)
            Arc::new(StringArray::from(vec![
                Some("(╯°□°）╯"),
                Some("(╯°□°）╯"),
                Some("(╯°□°）╯"),
            ])), // f
        ])
        .unwrap()
    }

    pub fn iceberg_table_schema() -> Schema {
        Schema::builder()
            .with_schema_id(2)
            .with_fields(vec![
                NestedField::optional(10, "a", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(11, "b", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(12, "c", Type::Primitive(PrimitiveType::Double)).into(),
                NestedField::required(13, "d", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(14, "e", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(15, "f", Type::Primitive(PrimitiveType::String))
                    .with_initial_default(Literal::string("(╯°□°）╯"))
                    .into(),
            ])
            .build()
            .unwrap()
    }

    fn arrow_schema_already_same_as_target() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            simple_field("a", DataType::Utf8, true, "10"),
            simple_field("b", DataType::Int64, false, "11"),
            simple_field("c", DataType::Float64, false, "12"),
            simple_field("e", DataType::Utf8, true, "14"),
            simple_field("f", DataType::Utf8, false, "15"),
        ]))
    }

    fn arrow_schema_promotion_addition_and_renaming_required() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            simple_field("b", DataType::Int32, false, "11"),
            simple_field("c", DataType::Float32, false, "12"),
            simple_field("d", DataType::Int32, false, "13"),
            simple_field("e_old", DataType::Utf8, true, "14"),
        ]))
    }

    fn arrow_schema_no_promotion_addition_or_renaming_required() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            simple_field("d", DataType::Int32, false, "13"),
            simple_field("e", DataType::Utf8, true, "14"),
        ]))
    }

    /// Create a simple arrow field with metadata.
    fn simple_field(name: &str, ty: DataType, nullable: bool, value: &str) -> Field {
        Field::new(name, ty, nullable).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            value.to_string(),
        )]))
    }
}
