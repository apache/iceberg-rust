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

//! Metadata columns (virtual/reserved fields) for Iceberg tables.
//!
//! This module defines metadata columns that can be requested in projections
//! but are not stored in data files. Instead, they are computed on-the-fly
//! during reading. Examples include the _file column (file path) and future
//! columns like partition values or row numbers.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field};
use once_cell::sync::Lazy;
use parquet::arrow::{PARQUET_FIELD_ID_META_KEY, RowNumber};

use crate::{Error, ErrorKind, Result};

/// Reserved field ID for the file path (_file) column per Iceberg spec
pub const RESERVED_FIELD_ID_FILE: i32 = i32::MAX - 1;

/// Reserved column name for the file path metadata column
pub const RESERVED_COL_NAME_FILE: &str = "_file";

/// Reserved field ID for the row position (_pos) metadata column
pub const RESERVED_FIELD_ID_UNDERSCORE_POS: i32 = i32::MAX - 2;

/// Reserved column name for the row position metadata column
pub const RESERVED_COL_NAME_UNDERSCORE_POS: &str = "_pos";

/// Reserved field ID for the file_path column used in delete file reading (positional deletes)
pub const RESERVED_FIELD_ID_FILE_PATH: i32 = i32::MAX - 200;

/// Column name for the file_path column used in delete file reading (positional deletes)
pub const RESERVED_COL_NAME_FILE_PATH: &str = "file_path";

/// Reserved field ID for the pos column used in delete file reading (positional deletes)
pub const RESERVED_FIELD_ID_POS: i32 = i32::MAX - 201;

/// Column name for the pos column used in delete file reading (positional deletes)
pub const RESERVED_COL_NAME_POS: &str = "pos";

/// Lazy-initialized Arrow Field definition for the _file metadata column.
static FILE_FIELD: Lazy<Arc<Field>> = Lazy::new(|| {
    Arc::new(
        Field::new(RESERVED_COL_NAME_FILE, DataType::Utf8, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            RESERVED_FIELD_ID_FILE.to_string(),
        )])),
    )
});

/// Returns the Arrow Field definition for the _file metadata column.
///
/// # Returns
/// A reference to the _file field definition (RunEndEncoded type)
pub fn file_field() -> &'static Arc<Field> {
    &FILE_FIELD
}

/// Lazy-initialized Arrow Field definition for the _pos metadata column.
/// Used for row position within a file.
static ROW_POS_FIELD: Lazy<Arc<Field>> = Lazy::new(|| {
    Arc::new(
        Field::new(RESERVED_COL_NAME_UNDERSCORE_POS, DataType::Int64, false)
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                RESERVED_FIELD_ID_UNDERSCORE_POS.to_string(),
            )]))
            .with_extension_type(RowNumber),
    )
});

/// Returns the Arrow Field definition for the _pos metadata column.
///
/// # Returns
/// A reference to the _pos field definition
pub fn row_pos_field() -> &'static Arc<Field> {
    &ROW_POS_FIELD
}

/// Lazy-initialized Arrow Field definition for the pos metadata column.
/// Used in positional delete records.
static POS_FIELD: Lazy<Arc<Field>> = Lazy::new(|| {
    Arc::new(
        Field::new(RESERVED_COL_NAME_POS, DataType::UInt64, false).with_metadata(HashMap::from([
            (
                PARQUET_FIELD_ID_META_KEY.to_string(),
                RESERVED_FIELD_ID_POS.to_string(),
            ),
        ])),
    )
});

/// Returns the Arrow Field definition for the pos metadata column.
///
/// # Returns
/// A reference to the pos field definition
pub fn pos_field() -> &'static Arc<Field> {
    &POS_FIELD
}

/// Lazy-initialized Arrow Field definition for the file_path metadata column.
/// Used in positional delete records to track which file each delete applies to.
static FILE_PATH_FIELD: Lazy<Arc<Field>> = Lazy::new(|| {
    Arc::new(
        Field::new(RESERVED_COL_NAME_FILE_PATH, DataType::Utf8, false).with_metadata(
            HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                RESERVED_FIELD_ID_FILE_PATH.to_string(),
            )]),
        ),
    )
});

/// Returns the Arrow Field definition for the file_path metadata column.
///
/// # Returns
/// A reference to the file_path field definition
pub fn file_path_field() -> &'static Arc<Field> {
    &FILE_PATH_FIELD
}

/// Returns the Arrow Field definition for a metadata field ID.
///
/// # Arguments
/// * `field_id` - The metadata field ID
///
/// # Returns
/// The Arrow Field definition for the metadata column, or an error if not a metadata field
pub fn get_metadata_field(field_id: i32) -> Result<Arc<Field>> {
    match field_id {
        RESERVED_FIELD_ID_FILE => Ok(Arc::clone(file_field())),
        RESERVED_FIELD_ID_UNDERSCORE_POS => Ok(Arc::clone(row_pos_field())),
        RESERVED_FIELD_ID_FILE_PATH => Ok(Arc::clone(file_path_field())),
        RESERVED_FIELD_ID_POS => Ok(Arc::clone(pos_field())),
        _ => Err(Error::new(
            ErrorKind::Unexpected,
            format!("Field ID {} is not a (supported) metadata field", field_id),
        )),
    }
}

/// Returns the field ID for a metadata column name.
///
/// # Arguments
/// * `column_name` - The metadata column name
///
/// # Returns
/// The field ID of the metadata column, or an error if the column name is not recognized
pub fn get_metadata_field_id(column_name: &str) -> Result<i32> {
    match column_name {
        RESERVED_COL_NAME_FILE => Ok(RESERVED_FIELD_ID_FILE),
        RESERVED_COL_NAME_UNDERSCORE_POS => Ok(RESERVED_FIELD_ID_UNDERSCORE_POS),
        RESERVED_COL_NAME_FILE_PATH => Ok(RESERVED_FIELD_ID_FILE_PATH),
        RESERVED_COL_NAME_POS => Ok(RESERVED_FIELD_ID_POS),
        _ => Err(Error::new(
            ErrorKind::Unexpected,
            format!("Unknown metadata column name: {column_name}"),
        )),
    }
}

/// Checks if a field ID is a metadata field.
///
/// # Arguments
/// * `field_id` - The field ID to check
///
/// # Returns
/// `true` if the field ID is a metadata field, `false` otherwise
pub fn is_metadata_field(field_id: i32) -> bool {
    matches!(
        field_id,
        RESERVED_FIELD_ID_FILE
            | RESERVED_FIELD_ID_UNDERSCORE_POS
            | RESERVED_FIELD_ID_FILE_PATH
            | RESERVED_FIELD_ID_POS
    )
}

/// Checks if a column name is a metadata column.
///
/// # Arguments
/// * `column_name` - The column name to check
///
/// # Returns
/// `true` if the column name is a metadata column, `false` otherwise
pub fn is_metadata_column_name(column_name: &str) -> bool {
    get_metadata_field_id(column_name).is_ok()
}
