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

//! UTC timestamp coercion for Arrow RecordBatches.
//!
//! Arrow engines may produce timestamps with timezone "UTC" while Iceberg's
//! canonical Arrow schema uses "+00:00". This module handles the lossless cast
//! between UTC-equivalent timezone representations so the parquet writer can
//! accept data from either convention.

use arrow_array::RecordBatch;
use arrow_cast::cast;
use arrow_schema::SchemaRef as ArrowSchemaRef;

use crate::{Error, ErrorKind, Result};

/// Coerce timestamp columns in `batch` to match `target_schema` when the only
/// difference is a UTC-equivalent timezone alias (e.g. "UTC" vs "+00:00").
pub(crate) fn coerce_timestamp_columns(
    batch: &RecordBatch,
    target_schema: &ArrowSchemaRef,
) -> Result<RecordBatch> {
    if batch.schema() == *target_schema {
        return Ok(batch.clone());
    }

    let mut cols = batch.columns().to_vec();
    let mut changed = false;

    for (idx, (col, target_field)) in batch
        .columns()
        .iter()
        .zip(target_schema.fields())
        .enumerate()
    {
        if col.data_type() != target_field.data_type()
            && differs_only_by_utc_timezone(col.data_type(), target_field.data_type())
        {
            cols[idx] = cast(col, target_field.data_type())?;
            changed = true;
        }
    }

    if !changed {
        return Ok(batch.clone());
    }

    RecordBatch::try_new(target_schema.clone(), cols).map_err(|err| {
        Error::new(
            ErrorKind::DataInvalid,
            "Failed to rebuild record batch after casting to target schema.",
        )
        .with_source(err)
    })
}

/// Returns true if `source` and `target` differ only by UTC-equivalent timezone aliases
/// at any nesting depth. Recurses into List, LargeList, FixedSizeList, Struct, and Map.
fn differs_only_by_utc_timezone(
    source: &arrow_schema::DataType,
    target: &arrow_schema::DataType,
) -> bool {
    use arrow_schema::DataType;
    match (source, target) {
        (s, t) if s == t => false,

        (DataType::Timestamp(s_unit, Some(s_tz)), DataType::Timestamp(t_unit, Some(t_tz)))
            if s_unit == t_unit =>
        {
            matches!(
                (s_tz.as_ref(), t_tz.as_ref()),
                ("UTC", "+00:00") | ("+00:00", "UTC")
            )
        }

        (DataType::List(s_field), DataType::List(t_field))
        | (DataType::LargeList(s_field), DataType::LargeList(t_field)) => {
            s_field.name() == t_field.name()
                && s_field.is_nullable() == t_field.is_nullable()
                && differs_only_by_utc_timezone(s_field.data_type(), t_field.data_type())
        }

        (DataType::FixedSizeList(s_field, s_size), DataType::FixedSizeList(t_field, t_size))
            if s_size == t_size =>
        {
            s_field.name() == t_field.name()
                && s_field.is_nullable() == t_field.is_nullable()
                && differs_only_by_utc_timezone(s_field.data_type(), t_field.data_type())
        }

        (DataType::Struct(s_fields), DataType::Struct(t_fields)) => {
            s_fields.len() == t_fields.len()
                && s_fields.iter().zip(t_fields.iter()).all(|(sf, tf)| {
                    sf.name() == tf.name()
                        && sf.is_nullable() == tf.is_nullable()
                        && (sf.data_type() == tf.data_type()
                            || differs_only_by_utc_timezone(sf.data_type(), tf.data_type()))
                })
        }

        (DataType::Map(s_field, s_sorted), DataType::Map(t_field, t_sorted))
            if s_sorted == t_sorted =>
        {
            differs_only_by_utc_timezone(s_field.data_type(), t_field.data_type())
        }

        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int32Array, ListArray, RecordBatch, StructArray};
    use arrow_schema::{DataType, Field, Fields, TimeUnit};

    use super::*;

    #[test]
    fn test_noop_when_matching() {
        let schema = Arc::new(arrow_schema::Schema::new(vec![Field::new(
            "x",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![
            1, 2, 3,
        ])) as ArrayRef])
        .unwrap();
        let result = coerce_timestamp_columns(&batch, &schema).unwrap();
        assert_eq!(result.schema(), batch.schema());
    }

    #[test]
    fn test_passes_through_non_utc_mismatches() {
        let source_schema = Arc::new(arrow_schema::Schema::new(vec![Field::new(
            "x",
            DataType::Int32,
            false,
        )]));
        let target_schema = Arc::new(arrow_schema::Schema::new(vec![Field::new(
            "x",
            DataType::Utf8,
            false,
        )]));
        let batch =
            RecordBatch::try_new(source_schema.clone(), vec![
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
            ])
            .unwrap();
        let result = coerce_timestamp_columns(&batch, &target_schema).unwrap();
        assert_eq!(result.schema(), source_schema);
    }

    #[test]
    fn test_differs_only_by_utc_timezone_flat_timestamp() {
        assert!(differs_only_by_utc_timezone(
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
        ));
        assert!(differs_only_by_utc_timezone(
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        ));
        // Same timezone — no mismatch
        assert!(!differs_only_by_utc_timezone(
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ));
        // Different units — not a UTC alias mismatch
        assert!(!differs_only_by_utc_timezone(
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
        ));
        // Non-UTC timezone mismatch
        assert!(!differs_only_by_utc_timezone(
            &DataType::Timestamp(TimeUnit::Microsecond, Some("America/New_York".into())),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
        ));
    }

    #[test]
    fn test_differs_only_by_utc_timezone_list() {
        let source = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        )));
        let target = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            true,
        )));
        assert!(differs_only_by_utc_timezone(&source, &target));

        // LargeList
        let source_large = DataType::LargeList(Arc::new(Field::new(
            "item",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        )));
        let target_large = DataType::LargeList(Arc::new(Field::new(
            "item",
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            true,
        )));
        assert!(differs_only_by_utc_timezone(&source_large, &target_large));

        // List with non-timestamp element — no mismatch
        let source_int = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let target_str = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        assert!(!differs_only_by_utc_timezone(&source_int, &target_str));
    }

    #[test]
    fn test_differs_only_by_utc_timezone_struct() {
        let source = DataType::Struct(Fields::from(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ]));
        let target = DataType::Struct(Fields::from(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                true,
            ),
        ]));
        assert!(differs_only_by_utc_timezone(&source, &target));

        // Struct with a genuinely incompatible field — should return false
        let bad_target = DataType::Struct(Fields::from(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                true,
            ),
        ]));
        assert!(!differs_only_by_utc_timezone(&source, &bad_target));
    }

    #[test]
    fn test_differs_only_by_utc_timezone_map() {
        let entries_source = Field::new_struct(
            "entries",
            vec![
                Field::new("key", DataType::Utf8, false),
                Field::new(
                    "value",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    true,
                ),
            ],
            false,
        );
        let entries_target = Field::new_struct(
            "entries",
            vec![
                Field::new("key", DataType::Utf8, false),
                Field::new(
                    "value",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                    true,
                ),
            ],
            false,
        );
        let source = DataType::Map(Arc::new(entries_source), false);
        let target = DataType::Map(Arc::new(entries_target), false);
        assert!(differs_only_by_utc_timezone(&source, &target));

        // Map with incompatible key type — should return false
        let bad_entries_target = Field::new_struct(
            "entries",
            vec![
                Field::new("key", DataType::Int32, false),
                Field::new(
                    "value",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                    true,
                ),
            ],
            false,
        );
        let bad_target = DataType::Map(Arc::new(bad_entries_target), false);
        assert!(!differs_only_by_utc_timezone(&source, &bad_target));
    }

    #[test]
    fn test_coerce_timestamp_columns_with_struct() {
        let source_struct_field = Field::new(
            "s",
            DataType::Struct(Fields::from(vec![
                Field::new("id", DataType::Int32, false),
                Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    true,
                ),
            ])),
            false,
        );
        let target_struct_field = Field::new(
            "s",
            DataType::Struct(Fields::from(vec![
                Field::new("id", DataType::Int32, false),
                Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                    true,
                ),
            ])),
            false,
        );

        let source_schema = Arc::new(arrow_schema::Schema::new(vec![source_struct_field]));
        let target_schema = Arc::new(arrow_schema::Schema::new(vec![target_struct_field]));

        let id_array = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let ts_array = Arc::new(
            arrow_array::TimestampMicrosecondArray::from(vec![
                Some(1_000_000),
                Some(2_000_000),
                Some(3_000_000),
            ])
            .with_timezone("UTC"),
        ) as ArrayRef;
        let struct_array = Arc::new(StructArray::from(vec![
            (Arc::new(Field::new("id", DataType::Int32, false)), id_array),
            (
                Arc::new(Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    true,
                )),
                ts_array,
            ),
        ])) as ArrayRef;

        let batch = RecordBatch::try_new(source_schema, vec![struct_array]).unwrap();
        let result = coerce_timestamp_columns(&batch, &target_schema).unwrap();

        assert_eq!(result.schema(), target_schema);
        assert_eq!(result.num_rows(), 3);
    }

    #[test]
    fn test_coerce_timestamp_columns_with_list() {
        let source_field = Field::new(
            "ts_list",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ))),
            false,
        );
        let target_field = Field::new(
            "ts_list",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                true,
            ))),
            false,
        );

        let source_schema = Arc::new(arrow_schema::Schema::new(vec![source_field]));
        let target_schema = Arc::new(arrow_schema::Schema::new(vec![target_field]));

        let ts_values = arrow_array::TimestampMicrosecondArray::from(vec![
            Some(1_000_000),
            Some(2_000_000),
            Some(3_000_000),
        ])
        .with_timezone("UTC");
        let offsets = arrow_buffer::OffsetBuffer::from_lengths([2, 1]);
        let list_array = Arc::new(ListArray::new(
            Arc::new(Field::new(
                "item",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            )),
            offsets,
            Arc::new(ts_values),
            None,
        )) as ArrayRef;

        let batch = RecordBatch::try_new(source_schema, vec![list_array]).unwrap();
        let result = coerce_timestamp_columns(&batch, &target_schema).unwrap();

        assert_eq!(result.schema(), target_schema);
        assert_eq!(result.num_rows(), 2);
    }
}
