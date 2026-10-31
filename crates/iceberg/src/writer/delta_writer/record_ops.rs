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

//! Splitting a change-type [`RecordBatch`] into insert and delete batches.
//!
//! A delta writer receives a single stream of row-level changes where each row
//! carries a `_change_type` indicator describing what kind of change it is. This
//! is the repo's [`_change_type`](RESERVED_COL_NAME_CHANGE_TYPE) convention: a
//! non-nullable [`Utf8`] column whose value is one of [`CHANGE_TYPE_INSERT`],
//! [`CHANGE_TYPE_DELETE`], [`CHANGE_TYPE_UPDATE_BEFORE`], or
//! [`CHANGE_TYPE_UPDATE_AFTER`].
//!
//! [`split_by_change_type`] partitions such a batch into the insert and delete
//! payloads that the underlying data and delete writers consume, collapsing the
//! four change types onto two sides the way Java's `BaseDeltaTaskWriter` does:
//! `INSERT` and `UPDATE_AFTER` become inserts, `DELETE` and `UPDATE_BEFORE`
//! become deletes. The `_change_type` column is stripped from both outputs.
//!
//! [`RecordBatch`]: arrow_array::RecordBatch
//! [`Utf8`]: arrow_schema::DataType::Utf8

use arrow_arith::boolean::not;
use arrow_array::{Array, BooleanArray, RecordBatch, StringArray};
use arrow_buffer::BooleanBufferBuilder;
use arrow_schema::DataType;
use arrow_select::filter::filter_record_batch;

use crate::metadata_columns::RESERVED_COL_NAME_CHANGE_TYPE;
use crate::{Error, ErrorKind, Result};

/// `_change_type` value marking a row as an insert.
pub(crate) const CHANGE_TYPE_INSERT: &str = "INSERT";
/// `_change_type` value marking a row as a delete.
pub(crate) const CHANGE_TYPE_DELETE: &str = "DELETE";
/// `_change_type` value marking the pre-image of an updated row.
pub(crate) const CHANGE_TYPE_UPDATE_BEFORE: &str = "UPDATE_BEFORE";
/// `_change_type` value marking the post-image of an updated row.
pub(crate) const CHANGE_TYPE_UPDATE_AFTER: &str = "UPDATE_AFTER";

/// The result of splitting a change-type batch with [`split_by_change_type`].
///
/// Both batches share the same schema: the schema of the input batch with the
/// `_change_type` column removed. Field metadata (including Parquet field ids)
/// and the schema-level metadata are preserved.
#[derive(Debug)]
pub(crate) struct SplitBatches {
    /// Rows whose `_change_type` was [`CHANGE_TYPE_INSERT`] or
    /// [`CHANGE_TYPE_UPDATE_AFTER`], with the `_change_type` column removed.
    pub(crate) inserts: RecordBatch,
    /// Rows whose `_change_type` was [`CHANGE_TYPE_DELETE`] or
    /// [`CHANGE_TYPE_UPDATE_BEFORE`], with the `_change_type` column removed.
    pub(crate) deletes: RecordBatch,
}

/// Split a change-type [`RecordBatch`] into separate insert and delete batches.
///
/// The batch must contain a [`_change_type`](RESERVED_COL_NAME_CHANGE_TYPE)
/// column, located **by name** wherever it sits in the schema (not by position).
/// It must be a non-nullable [`Utf8`] column whose values are all one of the four
/// spec change types. The rows are collapsed onto two sides the way Java's
/// `BaseDeltaTaskWriter` does:
/// - [`CHANGE_TYPE_INSERT`] and [`CHANGE_TYPE_UPDATE_AFTER`] go to `inserts`;
/// - [`CHANGE_TYPE_DELETE`] and [`CHANGE_TYPE_UPDATE_BEFORE`] go to `deletes`.
///
/// Every other column is treated as payload. The returned [`SplitBatches`] carry
/// the payload columns (and their field-id metadata) but not the `_change_type`
/// column.
///
/// # Returns
///
/// A [`SplitBatches`] whose `inserts` and `deletes` preserve the input row order
/// within each side. Either side may be empty when all rows share a side (e.g. an
/// all-insert batch yields an empty `deletes` that still carries the payload
/// schema). This splitter neither reorders rows nor enforces any
/// `UPDATE_BEFORE`/`UPDATE_AFTER` pairing or ordering; that is the `DeltaWriter`'s
/// concern.
///
/// # Errors
///
/// Returns [`ErrorKind::DataInvalid`] when:
/// - the batch has no `_change_type` column;
/// - removing the `_change_type` column would leave no payload columns (a batch
///   consisting of only the `_change_type` column has nothing to write);
/// - the `_change_type` column is not of type [`Utf8`];
/// - the `_change_type` column is declared nullable or contains null values;
/// - the `_change_type` column holds a value other than one of the four spec
///   change types.
///
/// Returns [`ErrorKind::Unexpected`] if the `_change_type` column passes the
/// [`Utf8`] type check but cannot be downcast to a [`StringArray`], which would
/// be an internal invariant violation.
///
/// [`RecordBatch`]: arrow_array::RecordBatch
/// [`Utf8`]: arrow_schema::DataType::Utf8
pub(crate) fn split_by_change_type(batch: RecordBatch) -> Result<SplitBatches> {
    let schema = batch.schema();

    // Locate the change-type column by name, wherever it sits in the schema.
    let (change_type_idx, change_type_field) = schema
        .column_with_name(RESERVED_COL_NAME_CHANGE_TYPE)
        .ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "the batch has no '{RESERVED_COL_NAME_CHANGE_TYPE}' column required to split delta changes"
                ),
            )
        })?;

    // There must be at least one payload column besides the change-type column.
    // A batch consisting of only the change-type column carries no payload to
    // write, which is a caller error.
    if batch.num_columns() < 2 {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "removing the '{RESERVED_COL_NAME_CHANGE_TYPE}' column leaves no payload columns to write"
            ),
        ));
    }

    if change_type_field.data_type() != &DataType::Utf8 {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "the '{RESERVED_COL_NAME_CHANGE_TYPE}' column must be of type Utf8, but found {}",
                change_type_field.data_type()
            ),
        ));
    }

    let change_type = batch
        .column(change_type_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                format!(
                    "internal: the '{RESERVED_COL_NAME_CHANGE_TYPE}' column passed the Utf8 type check but could not be downcast to a StringArray"
                ),
            )
        })?;

    // The contract is a non-nullable column with no nulls. Reject any actual
    // nulls first (so `value(i)` below is safe), then reject a nullable-declared
    // column even when it happens to carry no nulls.
    if change_type.null_count() > 0 {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!("the '{RESERVED_COL_NAME_CHANGE_TYPE}' column must not contain null values"),
        ));
    }
    if change_type_field.is_nullable() {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!("the '{RESERVED_COL_NAME_CHANGE_TYPE}' column must be declared non-nullable"),
        ));
    }

    // Build the insert mask in a single pass, validating the change-type domain
    // as we go. `change_type` has no nulls, so `value(i)` is safe.
    let mut insert_builder = BooleanBufferBuilder::new(change_type.len());
    for i in 0..change_type.len() {
        let value = change_type.value(i);
        let is_insert = match value {
            CHANGE_TYPE_INSERT | CHANGE_TYPE_UPDATE_AFTER => true,
            CHANGE_TYPE_DELETE | CHANGE_TYPE_UPDATE_BEFORE => false,
            other => {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "unexpected '{RESERVED_COL_NAME_CHANGE_TYPE}' value {other:?}: expected one of {CHANGE_TYPE_INSERT}, {CHANGE_TYPE_DELETE}, {CHANGE_TYPE_UPDATE_BEFORE}, {CHANGE_TYPE_UPDATE_AFTER}"
                    ),
                ));
            }
        };
        insert_builder.append(is_insert);
    }
    let insert_mask = BooleanArray::new(insert_builder.finish(), None);
    // After validation every row is exactly one side, so the delete side is the
    // complement of the insert side.
    let delete_mask = not(&insert_mask)?;

    // Drop the change-type column with `project`, which carries over each
    // surviving field's metadata (field ids) and the schema-level metadata.
    let payload_indices: Vec<usize> = (0..batch.num_columns())
        .filter(|idx| *idx != change_type_idx)
        .collect();
    let payload = batch.project(&payload_indices)?;

    // `filter_record_batch` preserves field-id metadata on the surviving columns.
    let inserts = filter_record_batch(&payload, &insert_mask)?;
    let deletes = filter_record_batch(&payload, &delete_mask)?;

    Ok(SplitBatches { inserts, deletes })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::Int32Array;
    use arrow_schema::{Field, Schema};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    use super::*;

    fn field_with_id(name: &str, data_type: DataType, id: i32) -> Field {
        Field::new(name, data_type, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            id.to_string(),
        )]))
    }

    /// Build a batch with two payload columns (`id`: Int32 field-id 1, `name`:
    /// Utf8 field-id 2) followed by a non-nullable Utf8 `_change_type` column.
    fn make_batch(ids: Vec<i32>, names: Vec<&str>, change_types: Vec<&str>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            field_with_id("id", DataType::Int32, 1),
            field_with_id("name", DataType::Utf8, 2),
            Field::new(RESERVED_COL_NAME_CHANGE_TYPE, DataType::Utf8, false),
        ]));
        RecordBatch::try_new(schema, vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(change_types)),
        ])
        .unwrap()
    }

    fn ids_of(batch: &RecordBatch) -> Vec<i32> {
        batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .map(|v| v.unwrap())
            .collect()
    }

    fn names_of(batch: &RecordBatch) -> Vec<String> {
        batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|v| v.unwrap().to_string())
            .collect()
    }

    fn field_id_of(batch: &RecordBatch, name: &str) -> String {
        batch
            .schema()
            .column_with_name(name)
            .unwrap()
            .1
            .metadata()
            .get(PARQUET_FIELD_ID_META_KEY)
            .unwrap()
            .clone()
    }

    #[test]
    fn test_split_mixed_four_way() {
        let batch = make_batch(vec![1, 2, 3, 4], vec!["a", "b", "c", "d"], vec![
            CHANGE_TYPE_INSERT,
            CHANGE_TYPE_DELETE,
            CHANGE_TYPE_UPDATE_BEFORE,
            CHANGE_TYPE_UPDATE_AFTER,
        ]);

        let SplitBatches { inserts, deletes } = split_by_change_type(batch).unwrap();

        // The change-type column is stripped from both outputs.
        assert_eq!(inserts.num_columns(), 2);
        assert_eq!(deletes.num_columns(), 2);
        assert!(
            inserts
                .schema()
                .column_with_name(RESERVED_COL_NAME_CHANGE_TYPE)
                .is_none()
        );
        assert!(
            deletes
                .schema()
                .column_with_name(RESERVED_COL_NAME_CHANGE_TYPE)
                .is_none()
        );

        // INSERT + UPDATE_AFTER collapse to inserts; DELETE + UPDATE_BEFORE to deletes.
        assert_eq!(ids_of(&inserts), vec![1, 4]);
        assert_eq!(names_of(&inserts), vec!["a", "d"]);
        assert_eq!(ids_of(&deletes), vec![2, 3]);
        assert_eq!(names_of(&deletes), vec!["b", "c"]);

        // Field-id metadata is preserved on the surviving columns.
        assert_eq!(field_id_of(&inserts, "id"), "1");
        assert_eq!(field_id_of(&inserts, "name"), "2");
        assert_eq!(field_id_of(&deletes, "id"), "1");
        assert_eq!(field_id_of(&deletes, "name"), "2");
    }

    #[test]
    fn test_all_insert() {
        // INSERT and UPDATE_AFTER only: deletes is empty but keeps the payload schema.
        let batch = make_batch(vec![1, 2, 3], vec!["a", "b", "c"], vec![
            CHANGE_TYPE_INSERT,
            CHANGE_TYPE_UPDATE_AFTER,
            CHANGE_TYPE_INSERT,
        ]);

        let SplitBatches { inserts, deletes } = split_by_change_type(batch).unwrap();

        assert_eq!(inserts.num_rows(), 3);
        assert_eq!(deletes.num_rows(), 0);
        assert_eq!(ids_of(&inserts), vec![1, 2, 3]);
        assert_eq!(deletes.num_columns(), 2);
    }

    #[test]
    fn test_all_delete() {
        // DELETE and UPDATE_BEFORE only: inserts is empty but keeps the payload schema.
        let batch = make_batch(vec![1, 2, 3], vec!["a", "b", "c"], vec![
            CHANGE_TYPE_DELETE,
            CHANGE_TYPE_UPDATE_BEFORE,
            CHANGE_TYPE_DELETE,
        ]);

        let SplitBatches { inserts, deletes } = split_by_change_type(batch).unwrap();

        assert_eq!(inserts.num_rows(), 0);
        assert_eq!(deletes.num_rows(), 3);
        assert_eq!(ids_of(&deletes), vec![1, 2, 3]);
        assert_eq!(inserts.num_columns(), 2);
    }

    #[test]
    fn test_update_pair_routes_and_preserves_order() {
        // An update pair (UPDATE_BEFORE then UPDATE_AFTER) plus surrounding rows.
        // BEFORE routes to deletes, AFTER routes to inserts, and input row order
        // is preserved within each side.
        let batch = make_batch(vec![1, 2, 3, 4], vec!["a", "before", "after", "d"], vec![
            CHANGE_TYPE_INSERT,
            CHANGE_TYPE_UPDATE_BEFORE,
            CHANGE_TYPE_UPDATE_AFTER,
            CHANGE_TYPE_DELETE,
        ]);

        let SplitBatches { inserts, deletes } = split_by_change_type(batch).unwrap();

        // Row 0 (INSERT) then row 2 (UPDATE_AFTER), in that order.
        assert_eq!(ids_of(&inserts), vec![1, 3]);
        assert_eq!(names_of(&inserts), vec!["a", "after"]);
        // Row 1 (UPDATE_BEFORE) then row 3 (DELETE), in that order.
        assert_eq!(ids_of(&deletes), vec![2, 4]);
        assert_eq!(names_of(&deletes), vec!["before", "d"]);
    }

    #[test]
    fn test_change_type_column_not_last() {
        // The change-type column sits in the middle; it is located by name.
        let schema = Arc::new(Schema::new(vec![
            field_with_id("id", DataType::Int32, 1),
            Field::new(RESERVED_COL_NAME_CHANGE_TYPE, DataType::Utf8, false),
            field_with_id("name", DataType::Utf8, 2),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![
                CHANGE_TYPE_INSERT,
                CHANGE_TYPE_DELETE,
                CHANGE_TYPE_UPDATE_AFTER,
            ])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ])
        .unwrap();

        let SplitBatches { inserts, deletes } = split_by_change_type(batch).unwrap();

        assert_eq!(ids_of(&inserts), vec![1, 3]);
        assert_eq!(names_of(&inserts), vec!["a", "c"]);
        assert_eq!(ids_of(&deletes), vec![2]);
        assert_eq!(names_of(&deletes), vec!["b"]);

        // The change-type column is stripped and the surviving order (id, name)
        // and their field ids are preserved.
        assert_eq!(inserts.num_columns(), 2);
        assert!(
            inserts
                .schema()
                .column_with_name(RESERVED_COL_NAME_CHANGE_TYPE)
                .is_none()
        );
        assert_eq!(inserts.schema().field(0).name(), "id");
        assert_eq!(inserts.schema().field(1).name(), "name");
        assert_eq!(field_id_of(&inserts, "id"), "1");
        assert_eq!(field_id_of(&inserts, "name"), "2");
    }

    #[test]
    fn test_missing_change_type_column() {
        let schema = Arc::new(Schema::new(vec![
            field_with_id("id", DataType::Int32, 1),
            field_with_id("name", DataType::Utf8, 2),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ])
        .unwrap();

        let err = split_by_change_type(batch).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    #[test]
    fn test_wrong_change_type_column_type() {
        // The change-type column is Int32 rather than Utf8.
        let schema = Arc::new(Schema::new(vec![
            field_with_id("id", DataType::Int32, 1),
            Field::new(RESERVED_COL_NAME_CHANGE_TYPE, DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Int32Array::from(vec![1, -1])),
        ])
        .unwrap();

        let err = split_by_change_type(batch).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    #[test]
    fn test_nullable_declared_column_rejected() {
        // A nullable-declared change-type column is rejected even with no nulls.
        let schema = Arc::new(Schema::new(vec![
            field_with_id("id", DataType::Int32, 1),
            Field::new(RESERVED_COL_NAME_CHANGE_TYPE, DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![
                CHANGE_TYPE_INSERT,
                CHANGE_TYPE_DELETE,
            ])),
        ])
        .unwrap();

        let err = split_by_change_type(batch).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    #[test]
    fn test_actual_null_rejected() {
        // A change-type column that actually contains a null is rejected.
        let schema = Arc::new(Schema::new(vec![
            field_with_id("id", DataType::Int32, 1),
            Field::new(RESERVED_COL_NAME_CHANGE_TYPE, DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some(CHANGE_TYPE_INSERT), None])),
        ])
        .unwrap();

        let err = split_by_change_type(batch).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    #[test]
    fn test_invalid_change_type_value() {
        for v in ["FOO", "insert", "Insert", "UPDATE", ""] {
            let batch = make_batch(vec![1, 2], vec!["a", "b"], vec![CHANGE_TYPE_INSERT, v]);
            let result = split_by_change_type(batch);
            assert!(result.is_err(), "value {v}");
            assert_eq!(
                result.unwrap_err().kind(),
                ErrorKind::DataInvalid,
                "value {v}"
            );
        }
    }

    #[test]
    fn test_empty_batch() {
        // Zero rows with a valid change-type column: both outputs are empty and
        // the change-type column is stripped.
        let batch = make_batch(vec![], vec![], vec![]);

        let SplitBatches { inserts, deletes } = split_by_change_type(batch).unwrap();

        assert_eq!(inserts.num_rows(), 0);
        assert_eq!(deletes.num_rows(), 0);
        assert_eq!(inserts.num_columns(), 2);
        assert_eq!(deletes.num_columns(), 2);
        assert!(
            inserts
                .schema()
                .column_with_name(RESERVED_COL_NAME_CHANGE_TYPE)
                .is_none()
        );
    }

    #[test]
    fn test_only_change_type_column_rejected() {
        // A batch with only the change-type column has no payload to write.
        let schema = Arc::new(Schema::new(vec![Field::new(
            RESERVED_COL_NAME_CHANGE_TYPE,
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec![
            CHANGE_TYPE_INSERT,
            CHANGE_TYPE_DELETE,
        ]))])
        .unwrap();

        let err = split_by_change_type(batch).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    #[test]
    fn test_schema_level_metadata_preserved() {
        let metadata = HashMap::from([("custom-key".to_string(), "custom-value".to_string())]);
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                field_with_id("id", DataType::Int32, 1),
                field_with_id("name", DataType::Utf8, 2),
                Field::new(RESERVED_COL_NAME_CHANGE_TYPE, DataType::Utf8, false),
            ],
            metadata,
        ));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(StringArray::from(vec![
                CHANGE_TYPE_INSERT,
                CHANGE_TYPE_DELETE,
            ])),
        ])
        .unwrap();

        let SplitBatches { inserts, deletes } = split_by_change_type(batch).unwrap();

        assert_eq!(
            inserts
                .schema()
                .metadata()
                .get("custom-key")
                .map(String::as_str),
            Some("custom-value")
        );
        assert_eq!(
            deletes
                .schema()
                .metadata()
                .get("custom-key")
                .map(String::as_str),
            Some("custom-value")
        );
    }
}
