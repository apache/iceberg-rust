use std::sync::Arc;

use arrow::compute::cast;
use arrow_array::{
    Array as ArrowArray, ArrayRef, Datum as ArrowDatum, Float32Array, Float64Array, Int32Array,
    Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::arrow::schema_to_arrow_schema;
use crate::spec::{Literal, PrimitiveLiteral, Schema as IcebergSchema};
use crate::{Error, ErrorKind, Result};

/// Represents an operation that may need to be performed
/// to transform a RecordBatch coming from a Parquet file record
/// batch stream to match a newer Iceberg schema that has evolved from
/// the one that was used to write the parquet file.
#[derive(Debug)]
pub(crate) struct EvolutionOp {
    index: usize,
    action: EvolutionAction,
}

#[derive(Debug)]
pub(crate) enum EvolutionAction {
    // signifies that a particular column has undergone type promotion,
    // thus the column with the given index needs to be promoted to the
    // specified type
    Promote {
        target_type: DataType,
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
pub(crate) struct RecordBatchEvolutionProcessor {
    operations: Vec<EvolutionOp>,

    // Every transformed RecordBatch will have the same schema. We create the
    // target just once and cache it here. Helpfully, Arc<Schema> is needed in
    // the constructor for RecordBatch, so we don't need an expensive copy
    // each time.
    target_schema: Arc<ArrowSchema>,
}

impl RecordBatchEvolutionProcessor {
    pub(crate) fn build(
        source_schema: &ArrowSchemaRef,
        snapshot_schema: &IcebergSchema,
        projected_iceberg_field_ids: &[i32],
    ) -> Result<Option<Self>> {
        let operations: Vec<_> =
            Self::generate_operations(source_schema, snapshot_schema, projected_iceberg_field_ids)?;

        Ok(if operations.is_empty() {
            None
        } else {
            Some(Self {
                operations,
                target_schema: Arc::new(schema_to_arrow_schema(snapshot_schema)?),
            })
        })
    }

    fn target_schema(&self) -> Arc<ArrowSchema> {
        self.target_schema.clone()
    }

    pub(crate) fn process_record_batch(&self, record_batch: RecordBatch) -> Result<RecordBatch> {
        Ok(RecordBatch::try_new(
            self.target_schema.clone(),
            self.transform_columns(record_batch.columns())?,
        )?)
    }

    fn generate_operations(
        source_schema: &ArrowSchemaRef,
        snapshot_schema: &IcebergSchema,
        projected_iceberg_field_ids: &[i32],
    ) -> Result<Vec<EvolutionOp>> {
        // create the (possibly empty) list of `EvolutionOp`s that we need
        // to apply to the arrays in a record batch with `source_schema` so
        // that it matches the `snapshot_schema`
        let mut ops = vec![];

        let mapped_arrow_schema = schema_to_arrow_schema(snapshot_schema)?;

        let mut arrow_schema_index: usize = 0;
        for (projected_field_idx, &field_id) in projected_iceberg_field_ids.iter().enumerate() {
            let iceberg_field = snapshot_schema.field_by_id(field_id).unwrap();
            let mapped_arrow_field = mapped_arrow_schema.field(projected_field_idx);

            let (arrow_field, add_op_required) =
                if arrow_schema_index < source_schema.fields().len() {
                    let arrow_field = source_schema.field(arrow_schema_index);
                    let arrow_field_id: i32 = arrow_field
                        .metadata()
                        .get(PARQUET_FIELD_ID_META_KEY)
                        .unwrap()
                        .parse()
                        .map_err(|_| {
                            Error::new(ErrorKind::DataInvalid, "field id not parseable as an i32")
                        })?;
                    (Some(arrow_field), arrow_field_id != field_id)
                } else {
                    (None, true)
                };

            if add_op_required {
                let default_value =
                    if let Some(ref iceberg_default_value) = &iceberg_field.initial_default {
                        let Literal::Primitive(prim_value) = iceberg_default_value else {
                            panic!();
                        };
                        Some(prim_value.clone())
                    } else {
                        None
                    };

                ops.push(EvolutionOp {
                    index: arrow_schema_index,
                    action: EvolutionAction::Add {
                        value: default_value,
                        target_type: mapped_arrow_field.data_type().clone(),
                    },
                })
            } else {
                if !arrow_field
                    .unwrap()
                    .data_type()
                    .equals_datatype(mapped_arrow_field.data_type())
                {
                    ops.push(EvolutionOp {
                        index: arrow_schema_index,
                        action: EvolutionAction::Promote {
                            target_type: mapped_arrow_field.data_type().clone(),
                        },
                    })
                }

                arrow_schema_index += 1;
            }
        }

        Ok(ops)
    }

    fn transform_columns(
        &self,
        columns: &[Arc<dyn ArrowArray>],
    ) -> Result<Vec<Arc<dyn ArrowArray>>> {
        let mut result = Vec::with_capacity(columns.len() + self.operations.len());
        let num_rows = if columns.is_empty() {
            0
        } else {
            columns[0].len()
        };

        let mut col_idx = 0;
        let mut op_idx = 0;
        while op_idx < self.operations.len() || col_idx < columns.len() {
            if self.operations[op_idx].index == col_idx {
                match &self.operations[op_idx].action {
                    EvolutionAction::Add { target_type, value } => {
                        result.push(Self::create_column(target_type, value, num_rows)?);
                    }
                    EvolutionAction::Promote { target_type } => {
                        result.push(cast(&*columns[col_idx], target_type)?);
                        col_idx += 1;
                    }
                }
                op_idx += 1;
            } else {
                result.push(columns[col_idx].clone());
                col_idx += 1;
            }
        }

        Ok(result)
    }

    fn create_column(
        target_type: &DataType,
        prim_lit: &Option<PrimitiveLiteral>,
        num_rows: usize,
    ) -> Result<ArrayRef> {
        Ok(match (target_type, prim_lit) {
            (DataType::Utf8, Some(PrimitiveLiteral::String(value))) => {
                Arc::new(StringArray::from(vec![value.clone(); num_rows]))
            }
            (DataType::Utf8, None) => {
                let vals: Vec<Option<String>> = vec![None; num_rows];
                Arc::new(StringArray::from(vals))
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
            _ => {
                todo!();
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
    fn build_returns_none_when_no_schema_migration_required() {
        let snapshot_schema = iceberg_table_schema();
        let arrow_schema = arrow_schema_already_same_as_target();
        let projected_iceberg_field_ids = [10, 11, 12, 13, 14];

        let inst = RecordBatchEvolutionProcessor::build(
            &arrow_schema,
            &snapshot_schema,
            &projected_iceberg_field_ids,
        )
        .unwrap();

        assert!(inst.is_none());
    }

    #[test]
    fn processor_returns_correct_arrow_schema_when_schema_migration_required() {
        let snapshot_schema = iceberg_table_schema();
        let arrow_schema = arrow_schema_promotion_addition_and_renaming_required();
        let projected_iceberg_field_ids = [10, 11, 12, 13, 14];

        let inst = RecordBatchEvolutionProcessor::build(
            &arrow_schema,
            &snapshot_schema,
            &projected_iceberg_field_ids,
        )
        .unwrap()
        .unwrap();

        let result = inst.target_schema();

        assert_eq!(result, arrow_schema_already_same_as_target());
    }

    #[test]
    fn processor_returns_properly_shaped_record_batch_when_schema_migration_required() {
        let snapshot_schema = iceberg_table_schema();
        let arrow_schema = arrow_schema_promotion_addition_and_renaming_required();
        let projected_iceberg_field_ids = [10, 11, 12, 13, 14];

        let inst = RecordBatchEvolutionProcessor::build(
            &arrow_schema,
            &snapshot_schema,
            &projected_iceberg_field_ids,
        )
        .unwrap()
        .unwrap();

        let result = inst.process_record_batch(source_record_batch()).unwrap();

        let expected = expected_record_batch();

        assert_eq!(result, expected);
    }

    pub fn source_record_batch() -> RecordBatch {
        RecordBatch::try_new(
            arrow_schema_promotion_addition_and_renaming_required(),
            vec![
                Arc::new(Int32Array::from(vec![Some(1001), Some(1002), Some(1003)])),
                Arc::new(Float32Array::from(vec![
                    Some(12.125),
                    Some(23.375),
                    Some(34.875),
                ])),
                Arc::new(StringArray::from(vec![
                    Some("Apache"),
                    Some("Iceberg"),
                    Some("Rocks"),
                ])),
            ],
        )
        .unwrap()
    }

    pub fn expected_record_batch() -> RecordBatch {
        RecordBatch::try_new(arrow_schema_already_same_as_target(), vec![
            Arc::new(StringArray::from(Vec::<Option<String>>::from([
                None, None, None,
            ]))),
            Arc::new(Int64Array::from(vec![Some(1001), Some(1002), Some(1003)])),
            Arc::new(Float64Array::from(vec![
                Some(12.125),
                Some(23.375),
                Some(34.875),
            ])),
            Arc::new(StringArray::from(vec![
                Some("Apache"),
                Some("Iceberg"),
                Some("Rocks"),
            ])),
            Arc::new(StringArray::from(vec![
                Some("(╯°□°）╯"),
                Some("(╯°□°）╯"),
                Some("(╯°□°）╯"),
            ])),
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
                NestedField::optional(13, "d", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(14, "e", Type::Primitive(PrimitiveType::String))
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
            simple_field("d", DataType::Utf8, true, "13"),
            simple_field("e", DataType::Utf8, false, "14"),
        ]))
    }

    fn arrow_schema_promotion_addition_and_renaming_required() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            simple_field("b", DataType::Int32, false, "11"),
            simple_field("c", DataType::Float32, false, "12"),
            simple_field("d_old", DataType::Utf8, true, "13"),
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
