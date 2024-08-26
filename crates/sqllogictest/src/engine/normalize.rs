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

use crate::engine::output::DFColumnType;
use anyhow::anyhow;
use arrow_array::{ArrayRef, BooleanArray, Decimal128Array, Decimal256Array, Float16Array, Float32Array, Float64Array, LargeStringArray, RecordBatch, StringArray, StringViewArray};
use arrow_schema::{DataType, Fields};
use datafusion::arrow::util::display::ArrayFormatter;
use datafusion_common::format::DEFAULT_FORMAT_OPTIONS;

use crate::engine::conversion::*;

/// Converts `batches` to a result as expected by sqllogicteset.
pub(crate) fn convert_batches(batches: Vec<RecordBatch>) -> anyhow::Result<Vec<Vec<String>>> {
    if batches.is_empty() {
        Ok(vec![])
    } else {
        let schema = batches[0].schema();
        let mut rows = vec![];
        for batch in batches {
            // Verify schema
            if !schema.contains(&batch.schema()) {
                return Err(anyhow!(
                        "Schema mismatch. Previously had\n{:#?}\n\nGot:\n{:#?}",
                        &schema,
                        batch.schema()
                    ),
                );
            }

            let new_rows = convert_batch(batch)?
                .into_iter()
                .flat_map(expand_row);
            rows.extend(new_rows);
        }
        Ok(rows)
    }
}

/// special case rows that have newlines in them (like explain plans)
//
/// Transform inputs like:
/// ```text
/// [
///   "logical_plan",
///   "Sort: d.b ASC NULLS LAST\n  Projection: d.b, MAX(d.a) AS max_a",
/// ]
/// ```
///
/// Into one cell per line, adding lines if necessary
/// ```text
/// [
///   "logical_plan",
/// ]
/// [
///   "Sort: d.b ASC NULLS LAST",
/// ]
/// [ <--- newly added row
///   "|-- Projection: d.b, MAX(d.a) AS max_a",
/// ]
/// ```
fn expand_row(mut row: Vec<String>) -> impl Iterator<Item=Vec<String>> {
    use itertools::Either;
    use std::iter::once;

    // check last cell
    if let Some(cell) = row.pop() {
        let lines: Vec<_> = cell.split('\n').collect();

        // no newlines in last cell
        if lines.len() < 2 {
            row.push(cell);
            return Either::Left(once(row));
        }

        // form new rows with each additional line
        let new_lines: Vec<_> = lines
            .into_iter()
            .enumerate()
            .map(|(idx, l)| {
                // replace any leading spaces with '-' as
                // `sqllogictest` ignores whitespace differences
                //
                // See https://github.com/apache/datafusion/issues/6328
                let content = l.trim_start();
                let new_prefix = "-".repeat(l.len() - content.len());
                // maintain for each line a number, so
                // reviewing explain result changes is easier
                let line_num = idx + 1;
                vec![format!("{line_num:02}){new_prefix}{content}")]
            })
            .collect();

        Either::Right(once(row).chain(new_lines))
    } else {
        Either::Left(once(row))
    }
}

/// Convert a single batch to a `Vec<Vec<String>>` for comparison
fn convert_batch(batch: RecordBatch) -> anyhow::Result<Vec<Vec<String>>> {
    (0..batch.num_rows())
        .map(|row| {
            batch
                .columns()
                .iter()
                .map(|col| cell_to_string(col, row))
                .collect::<anyhow::Result<Vec<String>>>()
        })
        .collect()
}

macro_rules! get_row_value {
    ($array_type:ty, $column: ident, $row: ident) => {{
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();

        array.value($row)
    }};
}

/// Normalizes the content of a single cell in RecordBatch prior to printing.
///
/// This is to make the output comparable to the semi-standard .slt format
///
/// Normalizations applied to [NULL Values and empty strings]
///
/// [NULL Values and empty strings]: https://duckdb.org/dev/sqllogictest/result_verification#null-values-and-empty-strings
///
/// Floating numbers are rounded to have a consistent representation with the Postgres runner.
///
pub fn cell_to_string(col: &ArrayRef, row: usize) -> anyhow::Result<String> {
    if !col.is_valid(row) {
        // represent any null value with the string "NULL"
        Ok(NULL_STR.to_string())
    } else {
        match col.data_type() {
            DataType::Null => Ok(NULL_STR.to_string()),
            DataType::Boolean => {
                Ok(bool_to_str(get_row_value!(BooleanArray, col, row)))
            }
            DataType::Float16 => {
                Ok(f16_to_str(get_row_value!(Float16Array, col, row)))
            }
            DataType::Float32 => {
                Ok(f32_to_str(get_row_value!(Float32Array, col, row)))
            }
            DataType::Float64 => {
                Ok(f64_to_str(get_row_value!(Float64Array, col, row)))
            }
            DataType::Decimal128(precision, scale) => {
                let value = get_row_value!(Decimal128Array, col, row);
                Ok(i128_to_str(value, precision, scale))
            }
            DataType::Decimal256(precision, scale) => {
                let value = get_row_value!(Decimal256Array, col, row);
                Ok(i256_to_str(value, precision, scale))
            }
            DataType::LargeUtf8 => Ok(varchar_to_str(get_row_value!(
                LargeStringArray,
                col,
                row
            ))),
            DataType::Utf8 => {
                Ok(varchar_to_str(get_row_value!(StringArray, col, row)))
            }
            DataType::Utf8View => Ok(varchar_to_str(get_row_value!(
                StringViewArray,
                col,
                row
            ))),
            _ => {
                let f = ArrayFormatter::try_new(col.as_ref(), &DEFAULT_FORMAT_OPTIONS);
                Ok(f.unwrap().value(row).to_string())
            }
        }
    }
}

/// Converts columns to a result as expected by sqllogicteset.
pub(crate) fn convert_schema_to_types(columns: &Fields) -> Vec<DFColumnType> {
    columns
        .iter()
        .map(|f| f.data_type())
        .map(|data_type| match data_type {
            DataType::Boolean => DFColumnType::Boolean,
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => DFColumnType::Integer,
            DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _) => DFColumnType::Float,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                DFColumnType::Text
            }
            DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_) => DFColumnType::DateTime,
            DataType::Timestamp(_, _) => DFColumnType::Timestamp,
            _ => DFColumnType::Another,
        })
        .collect()
}