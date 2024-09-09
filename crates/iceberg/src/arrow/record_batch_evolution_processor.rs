use std::collections::HashMap;
use std::sync::Arc;

use arrow::compute::cast;
use arrow_array::{
    Array as ArrowArray, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array,
    Int32Array, Int64Array, NullArray, RecordBatch, StringArray,
};
use arrow_schema::{
    DataType, FieldRef, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef, SchemaRef,
};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::arrow::schema_to_arrow_schema;
use crate::spec::{Literal, PrimitiveLiteral, Schema as IcebergSchema};
use crate::{Error, ErrorKind, Result};

/// Represents an operation that needs to be performed
/// to transform a RecordBatch coming from a Parquet file record
/// batch stream so that it conforms to an Iceberg schema that has
/// evolved from the one that was used when the file was written.
#[derive(Debug)]
pub(crate) enum EvolutionAction {
    // signifies that a column should be passed through unmodified
    PassThrough {
        source_index: usize,
    },

    // signifies particular column has undergone type promotion, and so
    // the source column with the given index needs to be promoted to the
    // specified type
    Promote {
        target_type: DataType,
        source_index: usize,
    },

    // Signifies that a new column has been inserted before the row
    // with index `index`. (we choose "before" rather than "after" so
    // that we can use usize; if we insert after, then we need to
    // be able to store -1 here when we want to indicate that the new
    // column is to be added at the front of the list).
    // If multiple columns need to be inserted at a given
    // location, they should all be given the same index, as the index
    // here refers to the original record batch, not the interim state after
    // a preceding operation.
    Add {
        target_type: DataType,
        value: Option<PrimitiveLiteral>,
    },
    // The iceberg spec refers to other permissible schema evolution actions
    // (see https://iceberg.apache.org/spec/#schema-evolution):
    // renaming fields, deleting fields and reordering fields.
    // Renames only affect the RecordBatch schema rather than the
    // columns themselves, so a single updated cached schema can
    // be re-used and no per-column actions are required.
    // Deletion and Reorder can be achieved without needing this
    // post-processing step by using the projection mask.
}

#[derive(Debug)]
struct SchemaAndOps {
    // Every transformed RecordBatch will have the same schema. We create the
    // target just once and cache it here. Helpfully, Arc<Schema> is needed in
    // the constructor for RecordBatch, so we don't need an expensive copy
    // each time.
    pub target_schema: Arc<ArrowSchema>,

    // Indicates how each column in the target schema is derived.
    pub operations: Vec<EvolutionAction>,
}

#[derive(Debug)]
pub(crate) struct RecordBatchEvolutionProcessor {
    snapshot_schema: Arc<IcebergSchema>,
    projected_iceberg_field_ids: Vec<i32>,
    schema_and_ops: Option<SchemaAndOps>,
}

impl RecordBatchEvolutionProcessor {
    /// Fallibly try to build a RecordBatchEvolutionProcessor for a given parquet file schema
    /// and Iceberg snapshot schema. Returns Ok(None) if the processor would not be required
    /// due to the file schema already matching the snapshot schema
    pub(crate) fn build(
        // source_schema: &ArrowSchemaRef,
        snapshot_schema: Arc<IcebergSchema>,
        projected_iceberg_field_ids: &[i32],
    ) -> Self {
        let projected_iceberg_field_ids = if projected_iceberg_field_ids.is_empty() {
            // project all fields in table schema order
            snapshot_schema
                .as_struct()
                .fields()
                .iter()
                .map(|field| field.id)
                .collect()
        } else {
            projected_iceberg_field_ids.to_vec()
        };

        Self {
            snapshot_schema,
            projected_iceberg_field_ids,
            schema_and_ops: None,
        }

        // let (operations, target_schema) = Self::generate_operations_and_schema(
        //     source_schema,
        //     snapshot_schema,
        //     projected_iceberg_field_ids,
        // )?;
        //
        // Ok(if target_schema.as_ref() == source_schema.as_ref() {
        //     None
        // } else {
        //     Some(Self {
        //         operations,
        //         target_schema,
        //     })
        // })
    }

    pub(crate) fn process_record_batch(
        &mut self,
        record_batch: RecordBatch,
    ) -> Result<RecordBatch> {
        if self.schema_and_ops.is_none() {
            self.schema_and_ops = Some(Self::generate_operations_and_schema(
                record_batch.schema_ref(),
                self.snapshot_schema.as_ref(),
                &self.projected_iceberg_field_ids,
            )?);
        }

        let Some(SchemaAndOps {
            ref target_schema, ..
        }) = self.schema_and_ops
        else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "schema_and_ops always created at this point",
            ));
        };

        Ok(RecordBatch::try_new(
            target_schema.clone(),
            self.transform_columns(record_batch.columns())?,
        )?)
    }

    // create the (possibly empty) list of `EvolutionOp`s that we need
    // to apply to the arrays in a record batch with `source_schema` so
    // that it matches the `snapshot_schema`
    fn generate_operations_and_schema(
        source_schema: &ArrowSchemaRef,
        snapshot_schema: &IcebergSchema,
        projected_iceberg_field_ids: &[i32],
    ) -> Result<SchemaAndOps> {
        let mapped_unprojected_arrow_schema = Arc::new(schema_to_arrow_schema(snapshot_schema)?);
        let field_id_to_source_schema_map =
            Self::build_field_id_to_arrow_schema_map(source_schema)?;
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
        let target_schema = ArrowSchema::new(fields?);

        let operations: Result<Vec<_>> = projected_iceberg_field_ids.iter().map(|field_id|{
            let (target_field, _) = field_id_to_mapped_schema_map.get(field_id).ok_or(
                Error::new(ErrorKind::Unexpected, "could not find field in schema")
            )?;
            let target_type = target_field.data_type();

            Ok(if let Some((source_field, source_index)) = field_id_to_source_schema_map.get(field_id) {
                // column present in source

                if source_field.data_type().equals_datatype(target_type) {
                    // no promotion required
                    EvolutionAction::PassThrough {
                        source_index: *source_index
                    }
                } else {
                    // promotion required
                    EvolutionAction::Promote {
                        target_type: target_type.clone(),
                        source_index: *source_index,
                    }
                }
            } else {
                // column must be added
                let iceberg_field = snapshot_schema.field_by_id(*field_id).ok_or(
                    Error::new(ErrorKind::Unexpected, "Field not found in snapshot schema")
                )?;

                let default_value = if let Some(ref iceberg_default_value) =
                    &iceberg_field.initial_default
                {
                    let Literal::Primitive(prim_value) = iceberg_default_value else {
                        return Err(Error::new(
                            ErrorKind::Unexpected,
                            format!("Default value for column must be primitive type, but encountered {:?}", iceberg_default_value)
                        ));
                    };
                    Some(prim_value.clone())
                } else {
                    None
                };

                EvolutionAction::Add {
                    value: default_value,
                    target_type: target_type.clone(),
                }
            })
        }).collect();

        Ok(SchemaAndOps {
            operations: operations?,
            target_schema: Arc::new(target_schema),
        })
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
    ) -> Result<Vec<Arc<dyn ArrowArray>>> {
        if columns.is_empty() {
            return Ok(columns.to_vec());
        }
        let num_rows = columns[0].len();

        let Some(ref schema_and_ops) = self.schema_and_ops else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "schema_and_ops was None, but should be present",
            ));
        };

        let result: Result<Vec<_>> = schema_and_ops
            .operations
            .iter()
            .map(|op| {
                Ok(match op {
                    EvolutionAction::PassThrough { source_index } => columns[*source_index].clone(),
                    EvolutionAction::Promote {
                        target_type,
                        source_index,
                    } => cast(&*columns[*source_index], target_type)?,
                    EvolutionAction::Add { target_type, value } => {
                        Self::create_column(target_type, value, num_rows)?
                    }
                })
            })
            .collect();

        result
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
                ))
            }
        })
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{
        Float32Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    };
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    use crate::arrow::record_batch_evolution_processor::RecordBatchEvolutionProcessor;
    use crate::spec::{Literal, NestedField, PrimitiveType, Schema, Type};

    #[test]
    fn build_field_id_to_source_schema_map_works() {
        let arrow_schema = arrow_schema_already_same_as_target();

        let result =
            RecordBatchEvolutionProcessor::build_field_id_to_arrow_schema_map(&arrow_schema)
                .unwrap();

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
            RecordBatchEvolutionProcessor::build(snapshot_schema, &projected_iceberg_field_ids);

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
            RecordBatchEvolutionProcessor::build(snapshot_schema, &projected_iceberg_field_ids);

        let result = inst.process_record_batch(source_record_batch()).unwrap();

        let expected = expected_record_batch_migration_required();

        assert_eq!(result, expected);
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
