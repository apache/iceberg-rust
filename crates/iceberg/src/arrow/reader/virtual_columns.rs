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

//! Maps Iceberg reserved metadata field ids to Apache Arrow virtual columns
//! consumed via [`parquet::arrow::arrow_reader::ArrowReaderOptions::with_virtual_columns`].
//!
//! Today only `_pos` is wired (mapped to the `RowNumber` extension type).

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use parquet::arrow::{PARQUET_FIELD_ID_META_KEY, RowNumber};

use crate::Result;
use crate::error::Error;
use crate::metadata_columns::{
    RESERVED_COL_NAME_POS, RESERVED_FIELD_ID_POS, is_reader_supplied_metadata_field,
};

/// Returns the Arrow virtual columns to request for the given projected
/// Iceberg field ids; empty when none are reader-supplied.
pub(crate) fn collect_arrow_virtual_columns(project_field_ids: &[i32]) -> Result<Vec<FieldRef>> {
    project_field_ids
        .iter()
        .copied()
        .filter(|id| is_reader_supplied_metadata_field(*id))
        .map(iceberg_metadata_field_to_arrow_virtual)
        .collect()
}

/// Builds the Arrow `Field` for a single reader-supplied metadata field id.
///
/// The `PARQUET:field_id` metadata key lets `RecordBatchTransformer` route
/// the column by id; the extension type makes arrow-rs accept it as a
/// virtual column.
fn iceberg_metadata_field_to_arrow_virtual(field_id: i32) -> Result<FieldRef> {
    let metadata = HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string())]);

    let mut field = match field_id {
        RESERVED_FIELD_ID_POS => {
            Field::new(RESERVED_COL_NAME_POS, DataType::Int64, false).with_metadata(metadata)
        }
        _ => {
            return Err(Error::new(
                crate::ErrorKind::Unexpected,
                format!(
                    "Iceberg metadata field id {field_id} is not produced by the Parquet reader"
                ),
            ));
        }
    };

    if field_id == RESERVED_FIELD_ID_POS {
        field.try_with_extension_type(RowNumber)?;
    }

    Ok(Arc::new(field))
}

#[cfg(test)]
mod tests {
    use parquet::arrow::is_virtual_column;

    use super::*;
    use crate::metadata_columns::RESERVED_FIELD_ID_FILE;

    #[test]
    fn collect_returns_empty_for_no_metadata_fields() {
        let virtuals = collect_arrow_virtual_columns(&[1, 2, 3]).unwrap();
        assert!(virtuals.is_empty());
    }

    #[test]
    fn collect_returns_pos_field_when_requested() {
        let virtuals = collect_arrow_virtual_columns(&[1, RESERVED_FIELD_ID_POS, 2]).unwrap();
        assert_eq!(virtuals.len(), 1);

        let pos = &virtuals[0];
        assert_eq!(pos.name(), RESERVED_COL_NAME_POS);
        assert_eq!(pos.data_type(), &DataType::Int64);
        assert!(!pos.is_nullable());
        assert!(is_virtual_column(pos));
        assert_eq!(
            pos.metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&RESERVED_FIELD_ID_POS.to_string()),
        );
    }

    #[test]
    fn collect_skips_constant_metadata_fields() {
        // `_file` is a metadata column but is added as a constant, not by the reader.
        let virtuals = collect_arrow_virtual_columns(&[RESERVED_FIELD_ID_FILE, 1]).unwrap();
        assert!(virtuals.is_empty());
    }
}
