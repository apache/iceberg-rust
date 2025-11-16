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
    StructArray,
};
use arrow_buffer::NullBuffer;
use arrow_cast::cast;
use arrow_schema::{
    DataType, FieldRef, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef, SchemaRef,
};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::arrow::schema_to_arrow_schema;
use crate::spec::{
    Literal, PartitionSpec, PrimitiveLiteral, Schema as IcebergSchema, Struct, Transform,
};
use crate::{Error, ErrorKind, Result};

/// Build a map of field ID to constant value for identity-partitioned fields.
///
/// Implements Iceberg spec "Column Projection" rule #1: use partition metadata constants
/// only for identity-transformed fields. Non-identity transforms (bucket, truncate, year, etc.)
/// store derived values in partition metadata, so source columns must be read from data files.
///
/// Example: For `bucket(4, id)`, partition metadata has `id_bucket = 2` (bucket number),
/// but the actual `id` values (100, 200, 300) are only in the data file.
///
/// Matches Java's `PartitionUtil.constantsMap()` which filters `if (field.transform().isIdentity())`.
///
/// # References
/// - Spec: https://iceberg.apache.org/spec/#column-projection
/// - Java: core/src/main/java/org/apache/iceberg/util/PartitionUtil.java:constantsMap()
fn constants_map(
    partition_spec: &PartitionSpec,
    partition_data: &Struct,
) -> HashMap<i32, PrimitiveLiteral> {
    let mut constants = HashMap::new();

    for (pos, field) in partition_spec.fields().iter().enumerate() {
        // Only identity transforms should use constant values from partition metadata
        if matches!(field.transform, Transform::Identity) {
            // Get the partition value for this field
            if let Some(Literal::Primitive(value)) = &partition_data[pos] {
                constants.insert(field.source_id, value.clone());
            }
        }
    }

    constants
}

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

/// Builder for RecordBatchTransformer to improve ergonomics when constructing with optional parameters.
///
/// See [`RecordBatchTransformer`] for details on partition spec and partition data.
#[derive(Debug)]
pub(crate) struct RecordBatchTransformerBuilder {
    snapshot_schema: Arc<IcebergSchema>,
    projected_iceberg_field_ids: Vec<i32>,
    partition_spec: Option<Arc<PartitionSpec>>,
    partition_data: Option<Struct>,
}

impl RecordBatchTransformerBuilder {
    pub(crate) fn new(
        snapshot_schema: Arc<IcebergSchema>,
        projected_iceberg_field_ids: &[i32],
    ) -> Self {
        Self {
            snapshot_schema,
            projected_iceberg_field_ids: projected_iceberg_field_ids.to_vec(),
            partition_spec: None,
            partition_data: None,
        }
    }

    /// Set partition spec and data together for identifying identity-transformed partition columns.
    ///
    /// Both partition_spec and partition_data must be provided together since the spec defines
    /// which fields are identity-partitioned, and the data provides their constant values.
    /// One without the other cannot produce a valid constants map.
    pub(crate) fn with_partition(
        mut self,
        partition_spec: Arc<PartitionSpec>,
        partition_data: Struct,
    ) -> Self {
        self.partition_spec = Some(partition_spec);
        self.partition_data = Some(partition_data);
        self
    }

    pub(crate) fn build(self) -> RecordBatchTransformer {
        RecordBatchTransformer {
            snapshot_schema: self.snapshot_schema,
            projected_iceberg_field_ids: self.projected_iceberg_field_ids,
            partition_spec: self.partition_spec,
            partition_data: self.partition_data,
            batch_transform: None,
        }
    }
}

/// Transforms RecordBatches from Parquet files to match the Iceberg table schema.
///
/// Handles schema evolution, column reordering, type promotion, and implements the Iceberg spec's
/// "Column Projection" rules for resolving field IDs "not present" in data files:
/// 1. Return the value from partition metadata if an Identity Transform exists
/// 2. Use schema.name-mapping.default metadata to map field id to columns without field id (applied in ArrowReader)
/// 3. Return the default value if it has a defined initial-default
/// 4. Return null in all other cases
///
/// # Field ID Resolution
///
/// Field ID resolution happens in ArrowReader before data is read (matching Java's ReadConf):
/// - If file has embedded field IDs: trust them (ParquetSchemaUtil.hasIds() = true)
/// - If file lacks IDs and name_mapping exists: apply name mapping (ParquetSchemaUtil.applyNameMapping())
/// - If file lacks IDs and no name_mapping: use position-based fallback (ParquetSchemaUtil.addFallbackIds())
///
/// By the time RecordBatchTransformer processes data, all field IDs are trustworthy.
/// This transformer only handles remaining projection rules (#1, #3, #4) for fields still "not present".
///
/// # Partition Spec and Data
///
/// **Bucket partitioning**: Distinguish identity transforms (use partition metadata constants)
/// from non-identity transforms like bucket (read from data file) to enable runtime filtering on
/// bucket-partitioned columns. For example, `bucket(4, id)` stores only the bucket number in
/// partition metadata, so actual `id` values must be read from the data file.
///
/// # References
/// - Spec: https://iceberg.apache.org/spec/#column-projection
/// - Java: parquet/src/main/java/org/apache/iceberg/parquet/ReadConf.java (field ID resolution)
/// - Java: core/src/main/java/org/apache/iceberg/util/PartitionUtil.java (partition constants)
#[derive(Debug)]
pub(crate) struct RecordBatchTransformer {
    snapshot_schema: Arc<IcebergSchema>,
    projected_iceberg_field_ids: Vec<i32>,

    /// Partition spec for identifying identity-transformed partition columns (spec rule #1).
    /// Only fields with identity transforms use partition data constants; non-identity transforms
    /// (bucket, truncate, etc.) must read source columns from data files.
    partition_spec: Option<Arc<PartitionSpec>>,

    /// Partition data providing constant values for identity-transformed partition columns (spec rule #1).
    /// For example, in a file at path `dept=engineering/file.parquet`, this would contain
    /// the value "engineering" for the dept field.
    partition_data: Option<Struct>,

    // BatchTransform gets lazily constructed based on the schema of
    // the first RecordBatch we receive from the file
    batch_transform: Option<BatchTransform>,
}

impl RecordBatchTransformer {
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
                    Arc::clone(target_schema),
                    self.transform_columns(record_batch.columns(), operations)?,
                    &options,
                )?
            }
            Some(BatchTransform::ModifySchema { target_schema }) => {
                let options = RecordBatchOptions::default()
                    .with_match_field_names(false)
                    .with_row_count(Some(record_batch.num_rows()));
                RecordBatch::try_new_with_options(
                    Arc::clone(target_schema),
                    record_batch.columns().to_vec(),
                    &options,
                )?
            }
            None => {
                self.batch_transform = Some(Self::generate_batch_transform(
                    record_batch.schema_ref(),
                    self.snapshot_schema.as_ref(),
                    &self.projected_iceberg_field_ids,
                    self.partition_spec.as_ref().map(|s| s.as_ref()),
                    self.partition_data.as_ref(),
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
        partition_spec: Option<&PartitionSpec>,
        partition_data: Option<&Struct>,
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

        let constants_map = if let (Some(spec), Some(data)) = (partition_spec, partition_data) {
            constants_map(spec, data)
        } else {
            HashMap::new()
        };

        match Self::compare_schemas(source_schema, &target_schema) {
            SchemaComparison::Equivalent => Ok(BatchTransform::PassThrough),
            SchemaComparison::NameChangesOnly => Ok(BatchTransform::ModifySchema { target_schema }),
            SchemaComparison::Different => Ok(BatchTransform::Modify {
                operations: Self::generate_transform_operations(
                    source_schema,
                    snapshot_schema,
                    projected_iceberg_field_ids,
                    field_id_to_mapped_schema_map,
                    constants_map,
                    partition_spec,
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
        constants_map: HashMap<i32, PrimitiveLiteral>,
        _partition_spec: Option<&PartitionSpec>,
    ) -> Result<Vec<ColumnSource>> {
        let field_id_to_source_schema_map =
            Self::build_field_id_to_arrow_schema_map(source_schema)?;

        projected_iceberg_field_ids
            .iter()
            .map(|field_id| {
                let (target_field, _) =
                    field_id_to_mapped_schema_map
                        .get(field_id)
                        .ok_or(Error::new(
                            ErrorKind::Unexpected,
                            "could not find field in schema",
                        ))?;
                let target_type = target_field.data_type();

                let iceberg_field = snapshot_schema.field_by_id(*field_id).ok_or(Error::new(
                    ErrorKind::Unexpected,
                    "Field not found in snapshot schema",
                ))?;

                // Iceberg spec's "Column Projection" rules (https://iceberg.apache.org/spec/#column-projection).
                // For fields "not present" in data files:
                // 1. Use partition metadata (identity transforms only)
                // 2. Use name mapping
                // 3. Use initial_default
                // 4. Return null
                //
                // Why check partition constants before Parquet field IDs (Java: BaseParquetReaders.java:299):
                // In add_files scenarios, partition columns may exist in BOTH Parquet AND partition metadata.
                // Partition metadata is authoritative - it defines which partition this file belongs to.

                // Field ID resolution now happens in ArrowReader via:
                // 1. Embedded field IDs (ParquetSchemaUtil.hasIds() = true) - trust them
                // 2. Name mapping (ParquetSchemaUtil.applyNameMapping()) - applied upfront
                // 3. Position-based fallback (ParquetSchemaUtil.addFallbackIds()) - applied upfront
                //
                // At this point, all field IDs in the source schema are trustworthy.
                // No conflict detection needed - schema resolution happened in reader.rs.
                let field_by_id = field_id_to_source_schema_map.get(field_id).map(
                    |(source_field, source_index)| {
                        if source_field.data_type().equals_datatype(target_type) {
                            ColumnSource::PassThrough {
                                source_index: *source_index,
                            }
                        } else {
                            ColumnSource::Promote {
                                target_type: target_type.clone(),
                                source_index: *source_index,
                            }
                        }
                    },
                );

                // Apply spec's fallback steps for "not present" fields.
                let column_source = if let Some(constant_value) = constants_map.get(field_id) {
                    // Rule #1: Identity partition constant
                    ColumnSource::Add {
                        value: Some(constant_value.clone()),
                        target_type: target_type.clone(),
                    }
                } else if let Some(source) = field_by_id {
                    source
                } else {
                    // Rules #2, #3 and #4:
                    // Rule #2 (name mapping) was already applied in reader.rs if needed.
                    // If field_id is still not found, the column doesn't exist in the Parquet file.
                    // Fall through to rule #3 (initial_default) or rule #4 (null).
                    let default_value = iceberg_field.initial_default.as_ref().and_then(|lit| {
                        if let Literal::Primitive(prim) = lit {
                            Some(prim.clone())
                        } else {
                            None
                        }
                    });
                    ColumnSource::Add {
                        value: default_value,
                        target_type: target_type.clone(),
                    }
                };

                Ok(column_source)
            })
            .collect()
    }

    fn build_field_id_to_arrow_schema_map(
        source_schema: &SchemaRef,
    ) -> Result<HashMap<i32, (FieldRef, usize)>> {
        let mut field_id_to_source_schema = HashMap::new();
        for (source_field_idx, source_field) in source_schema.fields.iter().enumerate() {
            // Check if field has a field ID in metadata
            if let Some(field_id_str) = source_field.metadata().get(PARQUET_FIELD_ID_META_KEY) {
                let this_field_id = field_id_str.parse().map_err(|e| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("field id not parseable as an i32: {}", e),
                    )
                })?;

                field_id_to_source_schema
                    .insert(this_field_id, (source_field.clone(), source_field_idx));
            }
            // If field doesn't have a field ID, skip it - name mapping will handle it
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
            (DataType::Struct(fields), None) => {
                // Create a StructArray filled with nulls. Per Iceberg spec, optional struct fields
                // default to null when added to the schema. We defer non-null default struct values
                // and leave them as not implemented yet.
                let null_arrays: Vec<ArrayRef> = fields
                    .iter()
                    .map(|field| Self::create_column(field.data_type(), &None, num_rows))
                    .collect::<Result<Vec<_>>>()?;

                Arc::new(StructArray::new(
                    fields.clone(),
                    null_arrays,
                    Some(NullBuffer::new_null(num_rows)),
                ))
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

    use crate::arrow::record_batch_transformer::{
        RecordBatchTransformer, RecordBatchTransformerBuilder,
    };
    use crate::spec::{Literal, NestedField, PrimitiveType, Schema, Struct, Type};

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

        let mut inst =
            RecordBatchTransformerBuilder::new(snapshot_schema, &projected_iceberg_field_ids)
                .build();

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

        let mut inst =
            RecordBatchTransformerBuilder::new(snapshot_schema, &projected_iceberg_field_ids)
                .build();

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
            RecordBatchTransformerBuilder::new(snapshot_schema, &projected_iceberg_field_ids)
                .build();

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

    #[test]
    fn schema_evolution_adds_struct_column_with_nulls() {
        // Test that when a struct column is added after data files are written,
        // the transformer can materialize the missing struct column with null values.
        // This reproduces the scenario from Iceberg 1.10.0 TestSparkReaderDeletes tests
        // where binaryData and structData columns were added to the schema.
        let snapshot_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "data", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::optional(
                        3,
                        "struct_col",
                        Type::Struct(crate::spec::StructType::new(vec![
                            NestedField::optional(
                                100,
                                "inner_field",
                                Type::Primitive(PrimitiveType::String),
                            )
                            .into(),
                        ])),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let projected_iceberg_field_ids = [1, 2, 3];

        let mut transformer =
            RecordBatchTransformerBuilder::new(snapshot_schema, &projected_iceberg_field_ids)
                .build();

        let file_schema = Arc::new(ArrowSchema::new(vec![
            simple_field("id", DataType::Int32, false, "1"),
            simple_field("data", DataType::Utf8, false, "2"),
        ]));

        let file_batch = RecordBatch::try_new(file_schema, vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
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

        let data_column = result
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(data_column.value(0), "a");
        assert_eq!(data_column.value(1), "b");
        assert_eq!(data_column.value(2), "c");

        let struct_column = result
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::StructArray>()
            .unwrap();
        assert!(struct_column.is_null(0));
        assert!(struct_column.is_null(1));
        assert!(struct_column.is_null(2));
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

    /// Test for add_files with Parquet files that have NO field IDs (Hive tables).
    ///
    /// This reproduces the scenario from Iceberg spec where:
    /// - Hive-style partitioned Parquet files are imported via add_files procedure
    /// - Parquet files originally DO NOT have field IDs (typical for Hive tables)
    /// - ArrowReader applies name mapping to assign correct Iceberg field IDs
    /// - Iceberg schema assigns field IDs: id (1), name (2), dept (3), subdept (4)
    /// - Partition columns (id, dept) have initial_default values
    ///
    /// Per the Iceberg spec (https://iceberg.apache.org/spec/#column-projection),
    /// this scenario requires `schema.name-mapping.default` from table metadata
    /// to correctly map Parquet columns by name to Iceberg field IDs.
    /// This mapping is now applied in ArrowReader before data is processed.
    ///
    /// Expected behavior:
    /// 1. id=1 (from initial_default) - spec rule #3
    /// 2. name="John Doe" (from Parquet with field_id=2 assigned by reader) - found by field ID
    /// 3. dept="hr" (from initial_default) - spec rule #3
    /// 4. subdept="communications" (from Parquet with field_id=4 assigned by reader) - found by field ID
    #[test]
    fn add_files_with_name_mapping_applied_in_reader() {
        // Iceberg schema after add_files: id (partition), name, dept (partition), subdept
        let snapshot_schema = Arc::new(
            Schema::builder()
                .with_schema_id(0)
                .with_fields(vec![
                    NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int))
                        .with_initial_default(Literal::int(1))
                        .into(),
                    NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::optional(3, "dept", Type::Primitive(PrimitiveType::String))
                        .with_initial_default(Literal::string("hr"))
                        .into(),
                    NestedField::optional(4, "subdept", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])
                .build()
                .unwrap(),
        );

        // Simulate ArrowReader having applied name mapping:
        // Original Parquet: name, subdept (NO field IDs)
        // After reader.rs applies name mapping: name (field_id=2), subdept (field_id=4)
        //
        // Note: Partition columns (id, dept) are NOT in the Parquet file - they're in directory paths
        use std::collections::HashMap;
        let parquet_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("name", DataType::Utf8, true).with_metadata(HashMap::from([(
                "PARQUET:field_id".to_string(),
                "2".to_string(),
            )])),
            Field::new("subdept", DataType::Utf8, true).with_metadata(HashMap::from([(
                "PARQUET:field_id".to_string(),
                "4".to_string(),
            )])),
        ]));

        let projected_field_ids = [1, 2, 3, 4]; // id, name, dept, subdept

        let mut transformer =
            RecordBatchTransformerBuilder::new(snapshot_schema, &projected_field_ids).build();

        // Create a Parquet RecordBatch with data for: name="John Doe", subdept="communications"
        let parquet_batch = RecordBatch::try_new(parquet_schema, vec![
            Arc::new(StringArray::from(vec!["John Doe"])),
            Arc::new(StringArray::from(vec!["communications"])),
        ])
        .unwrap();

        let result = transformer.process_record_batch(parquet_batch).unwrap();

        // Verify the transformed RecordBatch has:
        // - id=1 (from initial_default, not from Parquet)
        // - name="John Doe" (from Parquet with correct field_id=2)
        // - dept="hr" (from initial_default, not from Parquet)
        // - subdept="communications" (from Parquet with correct field_id=4)
        assert_eq!(result.num_columns(), 4);
        assert_eq!(result.num_rows(), 1);

        let id_column = result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_column.value(0), 1);

        let name_column = result
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_column.value(0), "John Doe");

        let dept_column = result
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(dept_column.value(0), "hr");

        let subdept_column = result
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(subdept_column.value(0), "communications");
    }

    /// Test for bucket partitioning where source columns must be read from data files.
    ///
    /// This test verifies correct implementation of the Iceberg spec's "Column Projection" rules:
    /// > "Return the value from partition metadata if an **Identity Transform** exists for the field"
    ///
    /// # Why this test is critical
    ///
    /// The key insight is that partition metadata stores TRANSFORMED values, not source values:
    /// - For `bucket(4, id)`, partition metadata has `id_bucket = 2` (the bucket number)
    /// - The actual `id` column values (100, 200, 300) are ONLY in the data file
    ///
    /// If iceberg-rust incorrectly treated bucket-partitioned fields as constants, it would:
    /// 1. Replace all `id` values with the constant `2` from partition metadata
    /// 2. Break runtime filtering (e.g., `WHERE id = 100` would match no rows)
    /// 3. Return incorrect query results
    ///
    /// # What this test verifies
    ///
    /// - Bucket-partitioned fields (e.g., `bucket(4, id)`) are read from the data file
    /// - The source column `id` contains actual values (100, 200, 300), not constants
    /// - Java's `PartitionUtil.constantsMap()` behavior is correctly replicated:
    ///   ```java
    ///   if (field.transform().isIdentity()) {  // FALSE for bucket transforms
    ///       idToConstant.put(field.sourceId(), converted);
    ///   }
    ///   ```
    ///
    /// # Real-world impact
    ///
    /// This reproduces the failure scenario from Iceberg Java's TestRuntimeFiltering:
    /// - Tables partitioned by `bucket(N, col)` are common for load balancing
    /// - Queries filter on the source column: `SELECT * FROM tbl WHERE col = value`
    /// - Runtime filtering pushes predicates down to Iceberg file scans
    /// - Without this fix, the filter would match against constant partition values instead of data
    ///
    /// # References
    /// - Iceberg spec: format/spec.md "Column Projection" + "Partition Transforms"
    /// - Java impl: core/src/main/java/org/apache/iceberg/util/PartitionUtil.java
    /// - Java test: spark/src/test/java/.../TestRuntimeFiltering.java
    #[test]
    fn bucket_partitioning_reads_source_column_from_file() {
        use crate::spec::{Struct, Transform};

        // Table schema: id (data column), name (data column), id_bucket (partition column)
        let snapshot_schema = Arc::new(
            Schema::builder()
                .with_schema_id(0)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap(),
        );

        // Partition spec: bucket(4, id) - the id field is bucketed
        let partition_spec = Arc::new(
            crate::spec::PartitionSpec::builder(snapshot_schema.clone())
                .with_spec_id(0)
                .add_partition_field("id", "id_bucket", Transform::Bucket(4))
                .unwrap()
                .build()
                .unwrap(),
        );

        // Partition data: bucket value is 2
        // In Iceberg, partition data is a Struct where each field corresponds to a partition field
        let partition_data = Struct::from_iter(vec![Some(Literal::int(2))]);

        // Parquet file contains both id and name columns
        let parquet_schema = Arc::new(ArrowSchema::new(vec![
            simple_field("id", DataType::Int32, false, "1"),
            simple_field("name", DataType::Utf8, true, "2"),
        ]));

        let projected_field_ids = [1, 2]; // id, name

        let mut transformer =
            RecordBatchTransformerBuilder::new(snapshot_schema, &projected_field_ids)
                .with_partition(partition_spec, partition_data)
                .build();

        // Create a Parquet RecordBatch with actual data
        // The id column MUST be read from here, not treated as a constant
        let parquet_batch = RecordBatch::try_new(parquet_schema, vec![
            Arc::new(Int32Array::from(vec![100, 200, 300])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ])
        .unwrap();

        let result = transformer.process_record_batch(parquet_batch).unwrap();

        // Verify the transformed RecordBatch correctly reads id from the file
        // (NOT as a constant from partition metadata)
        assert_eq!(result.num_columns(), 2);
        assert_eq!(result.num_rows(), 3);

        let id_column = result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        // These values MUST come from the Parquet file, not be replaced by constants
        assert_eq!(id_column.value(0), 100);
        assert_eq!(id_column.value(1), 200);
        assert_eq!(id_column.value(2), 300);

        let name_column = result
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_column.value(0), "Alice");
        assert_eq!(name_column.value(1), "Bob");
        assert_eq!(name_column.value(2), "Charlie");
    }

    /// Test that identity-transformed partition fields ARE treated as constants.
    ///
    /// This is the complement to `bucket_partitioning_reads_source_column_from_file`,
    /// verifying that constants_map() correctly identifies identity-transformed
    /// partition fields per the Iceberg spec.
    ///
    /// # Spec requirement (format/spec.md "Column Projection")
    ///
    /// > "Return the value from partition metadata if an Identity Transform exists for the field
    /// >  and the partition value is present in the `partition` struct on `data_file` object
    /// >  in the manifest. This allows for metadata only migrations of Hive tables."
    ///
    /// # Why identity transforms use constants
    ///
    /// Unlike bucket/truncate/year/etc., identity transforms don't modify the value:
    /// - `identity(dept)` stores the actual `dept` value in partition metadata
    /// - Partition metadata has `dept = "engineering"` (the real value, not a hash/bucket)
    /// - This value can be used directly without reading the data file
    ///
    /// # Performance benefit
    ///
    /// For Hive migrations where partition columns aren't in data files:
    /// - Partition metadata provides the column values
    /// - No need to read from data files (metadata-only query optimization)
    /// - Common pattern: `dept=engineering/subdept=backend/file.parquet`
    ///   - `dept` and `subdept` are in directory structure, not in `file.parquet`
    ///   - Iceberg populates these from partition metadata as constants
    ///
    /// # What this test verifies
    ///
    /// - Identity-partitioned fields use constants from partition metadata
    /// - The `dept` column is populated with `"engineering"` (not read from file)
    /// - Java's `PartitionUtil.constantsMap()` behavior is matched:
    ///   ```java
    ///   if (field.transform().isIdentity()) {  // TRUE for identity
    ///       idToConstant.put(field.sourceId(), converted);
    ///   }
    ///   ```
    ///
    /// # References
    /// - Iceberg spec: format/spec.md "Column Projection"
    /// - Java impl: core/src/main/java/org/apache/iceberg/util/PartitionUtil.java
    #[test]
    fn identity_partition_uses_constant_from_metadata() {
        use crate::spec::{Struct, Transform};

        // Table schema: id (data column), dept (partition column), name (data column)
        let snapshot_schema = Arc::new(
            Schema::builder()
                .with_schema_id(0)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "dept", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::optional(3, "name", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap(),
        );

        // Partition spec: identity(dept) - the dept field uses identity transform
        let partition_spec = Arc::new(
            crate::spec::PartitionSpec::builder(snapshot_schema.clone())
                .with_spec_id(0)
                .add_partition_field("dept", "dept", Transform::Identity)
                .unwrap()
                .build()
                .unwrap(),
        );

        // Partition data: dept="engineering"
        let partition_data = Struct::from_iter(vec![Some(Literal::string("engineering"))]);

        // Parquet file contains only id and name (dept is in partition path)
        let parquet_schema = Arc::new(ArrowSchema::new(vec![
            simple_field("id", DataType::Int32, false, "1"),
            simple_field("name", DataType::Utf8, true, "3"),
        ]));

        let projected_field_ids = [1, 2, 3]; // id, dept, name

        let mut transformer =
            RecordBatchTransformerBuilder::new(snapshot_schema, &projected_field_ids)
                .with_partition(partition_spec, partition_data)
                .build();

        let parquet_batch = RecordBatch::try_new(parquet_schema, vec![
            Arc::new(Int32Array::from(vec![100, 200])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
        ])
        .unwrap();

        let result = transformer.process_record_batch(parquet_batch).unwrap();

        // Verify the dept column is populated with the constant from partition metadata
        assert_eq!(result.num_columns(), 3);
        assert_eq!(result.num_rows(), 2);

        let id_column = result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_column.value(0), 100);
        assert_eq!(id_column.value(1), 200);

        let dept_column = result
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        // This value MUST come from partition metadata (constant)
        assert_eq!(dept_column.value(0), "engineering");
        assert_eq!(dept_column.value(1), "engineering");

        let name_column = result
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_column.value(0), "Alice");
        assert_eq!(name_column.value(1), "Bob");
    }

    /// Test bucket partitioning with renamed source column.
    ///
    /// This verifies correct behavior for TestRuntimeFiltering.testRenamedSourceColumnTable() in Iceberg Java.
    /// When a source column is renamed after partitioning is established, field-ID-based mapping
    /// must still correctly identify the column in Parquet files.
    ///
    /// # Scenario
    ///
    /// 1. Table created with `bucket(4, id)` partitioning
    /// 2. Data written to Parquet files (field_id=1, name="id")
    /// 3. Column renamed: `ALTER TABLE ... RENAME COLUMN id TO row_id`
    /// 4. Iceberg schema now has: field_id=1, name="row_id"
    /// 5. Parquet files still have: field_id=1, name="id"
    ///
    /// # Expected Behavior Per Iceberg Spec
    ///
    /// Per the Iceberg spec "Column Projection" section and Java's PartitionUtil.constantsMap():
    /// - Bucket transforms are NON-identity, so partition metadata stores bucket numbers (0-3), not source values
    /// - Source columns for non-identity transforms MUST be read from data files
    /// - Field-ID-based mapping should find the column by field_id=1 (ignoring name mismatch)
    /// - Runtime filtering on `row_id` should work correctly
    ///
    /// # What This Tests
    ///
    /// This test ensures that when FileScanTask provides partition_spec and partition_data:
    /// - constants_map() correctly identifies that bucket(4, row_id) is NOT an identity transform
    /// - The source column (field_id=1) is NOT added to constants_map
    /// - Field-ID-based mapping reads actual values from the Parquet file
    /// - Values [100, 200, 300] are read, not replaced with bucket constant 2
    ///
    /// # References
    /// - Java test: spark/src/test/java/.../TestRuntimeFiltering.java::testRenamedSourceColumnTable
    /// - Java impl: core/src/main/java/org/apache/iceberg/util/PartitionUtil.java::constantsMap()
    /// - Iceberg spec: format/spec.md "Column Projection" section
    #[test]
    fn test_bucket_partitioning_with_renamed_source_column() {
        use crate::spec::{Struct, Transform};

        // Iceberg schema after rename: row_id (was id), name
        let snapshot_schema = Arc::new(
            Schema::builder()
                .with_schema_id(0)
                .with_fields(vec![
                    NestedField::required(1, "row_id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap(),
        );

        // Partition spec: bucket(4, row_id) - but source_id still points to field_id=1
        let partition_spec = Arc::new(
            crate::spec::PartitionSpec::builder(snapshot_schema.clone())
                .with_spec_id(0)
                .add_partition_field("row_id", "row_id_bucket", Transform::Bucket(4))
                .unwrap()
                .build()
                .unwrap(),
        );

        // Partition data: bucket value is 2
        let partition_data = Struct::from_iter(vec![Some(Literal::int(2))]);

        // Parquet file has OLD column name "id" but SAME field_id=1
        // Field-ID-based mapping should find this despite name mismatch
        let parquet_schema = Arc::new(ArrowSchema::new(vec![
            simple_field("id", DataType::Int32, false, "1"),
            simple_field("name", DataType::Utf8, true, "2"),
        ]));

        let projected_field_ids = [1, 2]; // row_id (field_id=1), name (field_id=2)

        let mut transformer =
            RecordBatchTransformerBuilder::new(snapshot_schema, &projected_field_ids)
                .with_partition(partition_spec, partition_data)
                .build();

        // Create a Parquet RecordBatch with actual data
        // Despite column rename, data should be read via field_id=1
        let parquet_batch = RecordBatch::try_new(parquet_schema, vec![
            Arc::new(Int32Array::from(vec![100, 200, 300])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ])
        .unwrap();

        let result = transformer.process_record_batch(parquet_batch).unwrap();

        // Verify the transformed RecordBatch correctly reads data despite name mismatch
        assert_eq!(result.num_columns(), 2);
        assert_eq!(result.num_rows(), 3);

        let row_id_column = result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        // These values MUST come from the Parquet file via field_id=1,
        // not be replaced by the bucket constant (2)
        assert_eq!(row_id_column.value(0), 100);
        assert_eq!(row_id_column.value(1), 200);
        assert_eq!(row_id_column.value(2), 300);

        let name_column = result
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_column.value(0), "Alice");
        assert_eq!(name_column.value(1), "Bob");
        assert_eq!(name_column.value(2), "Charlie");
    }

    /// Comprehensive integration test that verifies all 4 Iceberg spec rules work correctly.
    ///
    /// Per the Iceberg spec (https://iceberg.apache.org/spec/#column-projection),
    /// "Values for field ids which are not present in a data file must be resolved
    /// according the following rules:"
    ///
    /// This test creates a scenario where each rule is exercised:
    /// - Rule #1: dept (identity-partitioned) -> constant from partition metadata
    /// - Rule #2: data (via name mapping) -> read from Parquet file by name
    /// - Rule #3: category (initial_default) -> use default value
    /// - Rule #4: notes (no default) -> return null
    ///
    /// # References
    /// - Iceberg spec: format/spec.md "Column Projection" section
    #[test]
    fn test_all_four_spec_rules() {
        use crate::spec::Transform;

        // Iceberg schema with columns designed to exercise each spec rule
        let snapshot_schema = Arc::new(
            Schema::builder()
                .with_schema_id(0)
                .with_fields(vec![
                    // Field in Parquet by field ID (normal case)
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    // Rule #1: Identity-partitioned field - should use partition metadata
                    NestedField::required(2, "dept", Type::Primitive(PrimitiveType::String)).into(),
                    // Rule #2: Field resolved by name mapping (ArrowReader already applied)
                    NestedField::required(3, "data", Type::Primitive(PrimitiveType::String)).into(),
                    // Rule #3: Field with initial_default
                    NestedField::optional(4, "category", Type::Primitive(PrimitiveType::String))
                        .with_initial_default(Literal::string("default_category"))
                        .into(),
                    // Rule #4: Field with no default - should be null
                    NestedField::optional(5, "notes", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])
                .build()
                .unwrap(),
        );

        // Partition spec: identity transform on dept
        let partition_spec = Arc::new(
            crate::spec::PartitionSpec::builder(snapshot_schema.clone())
                .with_spec_id(0)
                .add_partition_field("dept", "dept", Transform::Identity)
                .unwrap()
                .build()
                .unwrap(),
        );

        // Partition data: dept="engineering"
        let partition_data = Struct::from_iter(vec![Some(Literal::string("engineering"))]);

        // Parquet schema: simulates post-ArrowReader state where name mapping already applied
        // Has id (field_id=1) and data (field_id=3, assigned by ArrowReader via name mapping)
        // Missing: dept (in partition), category (has default), notes (no default)
        let parquet_schema = Arc::new(ArrowSchema::new(vec![
            simple_field("id", DataType::Int32, false, "1"),
            simple_field("data", DataType::Utf8, false, "3"),
        ]));

        let projected_field_ids = [1, 2, 3, 4, 5]; // id, dept, data, category, notes

        let mut transformer =
            RecordBatchTransformerBuilder::new(snapshot_schema, &projected_field_ids)
                .with_partition(partition_spec, partition_data)
                .build();

        let parquet_batch = RecordBatch::try_new(parquet_schema, vec![
            Arc::new(Int32Array::from(vec![100, 200])),
            Arc::new(StringArray::from(vec!["value1", "value2"])),
        ])
        .unwrap();

        let result = transformer.process_record_batch(parquet_batch).unwrap();

        assert_eq!(result.num_columns(), 5);
        assert_eq!(result.num_rows(), 2);

        // Verify each column demonstrates the correct spec rule:

        // Normal case: id from Parquet by field ID
        let id_column = result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_column.value(0), 100);
        assert_eq!(id_column.value(1), 200);

        // Rule #1: dept from partition metadata (identity transform)
        let dept_column = result
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(dept_column.value(0), "engineering");
        assert_eq!(dept_column.value(1), "engineering");

        // Rule #2: data from Parquet via name mapping
        let data_column = result
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(data_column.value(0), "value1");
        assert_eq!(data_column.value(1), "value2");

        // Rule #3: category from initial_default
        let category_column = result
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(category_column.value(0), "default_category");
        assert_eq!(category_column.value(1), "default_category");

        // Rule #4: notes is null (no default, not in Parquet, not in partition)
        let notes_column = result
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(notes_column.is_null(0));
        assert!(notes_column.is_null(1));
    }
}
