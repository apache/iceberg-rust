use std::cell::OnceCell;
use std::sync::Arc;

use arrow_array::{Array as ArrowArray, ArrayRef, RecordBatch};
use arrow_schema::{DataType, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};

use crate::spec::Schema as IcebergSchema;
use crate::Result;

/// Represents an operation that may need to be performed
/// to transform a RecordBatch coming from a Parquet file record
/// batch stream to match a newer Iceberg schema that has evolved from
/// the one that was used to write the parquet file.
pub(crate) enum EvolutionOp {
    // signifies that a particular column has undergone type promotion,
    // thus the column with the given index needs to be promoted to the
    // specified type
    Promote {
        index: usize,
        target_type: DataType,
    },

    // Signifies that a new column has been inserted before the row
    // with index `index`. (we choose "before" rather than "after" so
    // that we can use a usize;  if we insert after, then we need to
    // be able to store -1 here when we want to indicate that the new
    // column is to be added at the front of the list).
    // If multiple columns need to be inserted at a given
    // location, they should all be given the same index, as the index
    // here refers to the original record batch, not the interim state after
    // a preceding operation.
    Add {
        index: usize,
        target_type: DataType,
        value: Option<ArrayRef>, // A Scalar
    },

    // signifies that a column has been renamed from one schema to the next.
    // this requires no change to the data within a record batch, only to its
    // schema.
    Rename {
        index: usize,
        target_name: String,
    }, // The iceberg spec refers to other permissible schema evolution actions
       // (see https://iceberg.apache.org/spec/#schema-evolution):
       // deleting fields and reordering fields.
       // However, these actions can be achieved without needing this
       // slower post-processing step by using the projection mask.
}

pub(crate) struct RecordBatchEvolutionProcessor {
    operations: Vec<EvolutionOp>,

    // Every transformed RecordBatch will have the same schema. We create the
    // target just once and cache it here. Helpfully, Arc<Schema> is needed in
    // the constructor for RecordBatch, so we don't need an expensive copy
    // each time.
    target_schema: OnceCell<Arc<ArrowSchema>>, // Caching any columns that we need to add is harder as the number of rows
                                               // in the record batches can vary from batch to batch within the stream,
                                               // so rather than storing cached added columns here too, we have to
                                               // generate them on the fly.
}

impl RecordBatchEvolutionProcessor {
    pub(crate) fn build(
        source_schema: &ArrowSchemaRef,
        snapshot_schema: &IcebergSchema,
        projected_iceberg_field_ids: &[i32],
    ) -> Option<Self> {
        let operations: Vec<_> =
            Self::generate_operations(source_schema, snapshot_schema, projected_iceberg_field_ids);

        if operations.is_empty() {
            None
        } else {
            Some(Self {
                operations,
                target_schema: OnceCell::default(),
            })
        }
    }

    pub(crate) fn process_record_batch(&self, record_batch: RecordBatch) -> Result<RecordBatch> {
        let new_batch_schema = self
            .target_schema
            .get_or_init(|| self.create_target_schema());

        Ok(RecordBatch::try_new(
            new_batch_schema.clone(),
            self.transform_columns(record_batch.columns()),
        )?)
    }

    fn generate_operations(
        _source_schema: &ArrowSchemaRef,
        _snapshot_schema: &IcebergSchema,
        _projected_iceberg_field_ids: &[i32],
    ) -> Vec<EvolutionOp> {
        // create the (possibly empty) list of `EvolutionOp`s that we need
        // to apply to the arrays in a record batch with `source_schema` so
        // that it matches the `snapshot_schema`
        todo!();
    }

    fn transform_columns(&self, _columns: &[Arc<dyn ArrowArray>]) -> Vec<Arc<dyn ArrowArray>> {
        // iterate over source_columns and self.operations,
        // populating a Vec::with_capacity as we go
        todo!();
    }

    fn create_target_schema(&self) -> Arc<ArrowSchema> {
        todo!();
    }
}
