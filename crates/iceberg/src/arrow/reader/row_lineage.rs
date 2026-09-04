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

//! Reader-level synthesis of the v3 `_row_id` metadata column.
//!
//! `_row_id` is `first_row_id + pos`, overridden by a physically-stored `_row_id` where the
//! file carries one. The position comes from the reader-produced `_pos` (`RowNumber`)
//! column, which is the true global file position under filter pushdown, row-group pruning,
//! and page-index pruning -- so synthesis is done here, over the record-batch stream, using
//! that position rather than recomputing it.

use std::sync::Arc;

use arrow_arith::boolean::is_not_null;
use arrow_arith::numeric::add;
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use arrow_select::zip::zip;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::metadata_columns::{
    RESERVED_COL_NAME_ROW_ID, RESERVED_FIELD_ID_POS, RESERVED_FIELD_ID_ROW_ID,
};
use crate::{Error, ErrorKind, Result};

/// Appends the synthesized `_row_id` column to `batch`.
///
/// - `first_row_id == None`: the file carries no row lineage, so `_row_id` is all-null
///   (matching Java `ValueReaders.rowIds`).
/// - `first_row_id == Some(base)`: `_row_id` is the physically-stored value where the file
///   carries one and it is non-null, else `base + pos` (from the `_pos`/`RowNumber` column).
///
/// A physically-stored `_row_id` leaf already in `batch` is replaced by the synthesized
/// column (its values are folded in via the coalesce), so the result carries exactly one
/// column tagged with the reserved `_row_id` field id.
pub(crate) fn synthesize_row_id_column(
    batch: RecordBatch,
    first_row_id: Option<i64>,
) -> Result<RecordBatch> {
    let row_id: ArrayRef = match first_row_id {
        None => Arc::new(Int64Array::new_null(batch.num_rows())),
        Some(base) => {
            let pos = column_by_field_id(&batch, RESERVED_FIELD_ID_POS).ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "_row_id synthesis requires the _pos position column in the record batch",
                )
            })?;
            if pos.data_type() != &DataType::Int64 {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!(
                        "_pos position column must be Int64, got {}",
                        pos.data_type()
                    ),
                ));
            }

            // base + pos, the fallback for every row.
            let fallback = add(pos, &Int64Array::new_scalar(base))?;
            match column_by_field_id(&batch, RESERVED_FIELD_ID_ROW_ID) {
                Some(id) => {
                    if id.data_type() != &DataType::Int64 {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!("_row_id source must be Int64, got {}", id.data_type()),
                        ));
                    }

                    zip(&is_not_null(id)?, id, &fallback)?
                }
                None => fallback,
            }
        }
    };

    append_row_id(batch, row_id)
}

/// Returns the batch column tagged with `field_id` (via `PARQUET_FIELD_ID_META_KEY`), if any.
fn column_by_field_id(batch: &RecordBatch, field_id: i32) -> Option<&ArrayRef> {
    batch
        .schema()
        .fields()
        .iter()
        .position(|f| field_id_of(f) == Some(field_id))
        .map(|idx| batch.column(idx))
}

fn field_id_of(field: &Field) -> Option<i32> {
    field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .and_then(|id| id.parse::<i32>().ok())
}

/// Rebuilds `batch` with `row_id` as its `_row_id` column: any existing `_row_id`-tagged
/// column is dropped (already folded into `row_id`) and the synthesized column is appended.
fn append_row_id(batch: RecordBatch, row_id: ArrayRef) -> Result<RecordBatch> {
    let schema = batch.schema();
    let mut fields: Vec<Arc<Field>> = Vec::with_capacity(schema.fields().len() + 1);
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len() + 1);

    for (idx, field) in schema.fields().iter().enumerate() {
        if field_id_of(field) != Some(RESERVED_FIELD_ID_ROW_ID) {
            fields.push(field.clone());
            columns.push(batch.column(idx).clone());
        }
    }

    fields.push(Arc::new(
        Field::new(RESERVED_COL_NAME_ROW_ID, DataType::Int64, true).with_metadata(
            [(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                RESERVED_FIELD_ID_ROW_ID.to_string(),
            )]
            .into(),
        ),
    ));
    columns.push(row_id);

    Ok(RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
        columns,
    )?)
}

#[cfg(test)]
mod tests {
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int64Type;
    use arrow_array::{Int32Array, Int64Array};

    use super::*;
    use crate::metadata_columns::RESERVED_COL_NAME_POS;

    fn field_with_id(name: &str, dt: DataType, id: i32) -> Arc<Field> {
        Arc::new(
            Field::new(name, dt, true)
                .with_metadata([(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string())].into()),
        )
    }

    /// A batch with `id`, a `_pos` column, and (when `physical` is `Some`) a physically
    /// stored `_row_id` column — mirroring what the Parquet reader produces.
    fn batch(pos: Vec<i64>, physical: Option<Vec<Option<i64>>>) -> RecordBatch {
        let n = pos.len() as i32;
        let mut fields = vec![
            field_with_id("id", DataType::Int32, 1),
            field_with_id(
                RESERVED_COL_NAME_POS,
                DataType::Int64,
                RESERVED_FIELD_ID_POS,
            ),
        ];
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from((0..n).collect::<Vec<_>>())),
            Arc::new(Int64Array::from(pos)),
        ];
        if let Some(vals) = physical {
            fields.push(field_with_id(
                RESERVED_COL_NAME_ROW_ID,
                DataType::Int64,
                RESERVED_FIELD_ID_ROW_ID,
            ));
            columns.push(Arc::new(Int64Array::from(vals)));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    fn row_ids(rb: &RecordBatch) -> Vec<Option<i64>> {
        let v = rb
            .column_by_name(RESERVED_COL_NAME_ROW_ID)
            .unwrap()
            .as_primitive::<Int64Type>();
        (0..v.len())
            .map(|i| (!v.is_null(i)).then(|| v.value(i)))
            .collect()
    }

    #[test]
    fn pure_synthesis() {
        let out = synthesize_row_id_column(batch(vec![0, 1, 2], None), Some(100)).unwrap();
        assert_eq!(row_ids(&out), vec![Some(100), Some(101), Some(102)]);
    }

    #[test]
    fn non_zero_start_position() {
        let out = synthesize_row_id_column(batch(vec![10, 11, 12], None), Some(100)).unwrap();
        assert_eq!(row_ids(&out), vec![Some(110), Some(111), Some(112)]);
    }

    #[test]
    fn coalesce_with_physical_column() {
        let out = synthesize_row_id_column(
            batch(vec![0, 1, 2], Some(vec![Some(5), None, Some(8)])),
            Some(100),
        )
        .unwrap();
        // Physical value where non-null, else first_row_id + pos.
        assert_eq!(row_ids(&out), vec![Some(5), Some(101), Some(8)]);
        // The physical leaf was folded in -- exactly one _row_id column (id, _pos, _row_id).
        assert_eq!(out.schema().fields().len(), 3);
    }

    #[test]
    fn all_physical_present() {
        let out = synthesize_row_id_column(
            batch(vec![0, 1, 2], Some(vec![Some(5), Some(6), Some(7)])),
            Some(100),
        )
        .unwrap();
        assert_eq!(row_ids(&out), vec![Some(5), Some(6), Some(7)]);
    }

    #[test]
    fn all_fallback() {
        let out =
            synthesize_row_id_column(batch(vec![0, 1, 2], Some(vec![None, None, None])), Some(50))
                .unwrap();
        assert_eq!(row_ids(&out), vec![Some(50), Some(51), Some(52)]);
    }

    #[test]
    fn null_first_row_id_is_all_null() {
        let out = synthesize_row_id_column(batch(vec![0, 1, 2], None), None).unwrap();
        assert_eq!(row_ids(&out), vec![None, None, None]);
    }

    #[test]
    fn empty_batch() {
        let out = synthesize_row_id_column(batch(vec![], None), Some(100)).unwrap();
        assert_eq!(out.num_rows(), 0);
        assert_eq!(row_ids(&out), Vec::<Option<i64>>::new());
    }

    #[test]
    fn non_int64_physical_column_is_rejected() {
        let fields = vec![
            field_with_id(
                RESERVED_COL_NAME_POS,
                DataType::Int64,
                RESERVED_FIELD_ID_POS,
            ),
            field_with_id(
                RESERVED_COL_NAME_ROW_ID,
                DataType::Int32,
                RESERVED_FIELD_ID_ROW_ID,
            ),
        ];
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![0i64, 1, 2])),
            Arc::new(Int32Array::from(vec![Some(5), None, Some(8)])),
        ];
        let rb = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();

        let err = synthesize_row_id_column(rb, Some(100)).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(format!("{err}").contains("_row_id source must be Int64"));
    }

    #[test]
    fn missing_pos_is_rejected() {
        let fields = vec![field_with_id("id", DataType::Int32, 1)];
        let columns: Vec<ArrayRef> = vec![Arc::new(Int32Array::from(vec![0, 1, 2]))];
        let rb = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();

        let err = synthesize_row_id_column(rb, Some(100)).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(format!("{err}").contains("_pos position column"));
    }
}
