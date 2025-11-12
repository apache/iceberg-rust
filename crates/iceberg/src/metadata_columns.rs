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

use crate::{Error, ErrorKind, Result};

/// Reserved field ID for the file path (_file) column per Iceberg spec
pub const RESERVED_FIELD_ID_FILE: i32 = 2147483646;

/// Reserved column name for the file path metadata column
pub const RESERVED_COL_NAME_FILE: &str = "_file";

/// Returns the column name for a metadata field ID.
///
/// # Arguments
/// * `field_id` - The metadata field ID
///
/// # Returns
/// The name of the metadata column, or an error if the field ID is not recognized
pub fn get_metadata_column_name(field_id: i32) -> Result<&'static str> {
    match field_id {
        RESERVED_FIELD_ID_FILE => Ok(RESERVED_COL_NAME_FILE),
        _ => {
            if field_id > 2147483447 {
                Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("Unsupported metadata field ID: {field_id}"),
                ))
            } else {
                Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("Field ID {field_id} is not a metadata field"),
                ))
            }
        }
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
        _ => Err(Error::new(
            ErrorKind::Unexpected,
            format!("Unknown/unsupported metadata column name: {column_name}"),
        )),
    }
}

/// Checks if a field ID is a metadata field.
///
/// # Arguments
/// * `field_id` - The field ID to check
///
/// # Returns
/// `true` if the field ID is a (currently supported) metadata field, `false` otherwise
pub fn is_metadata_field(field_id: i32) -> bool {
    field_id == RESERVED_FIELD_ID_FILE
    // Additional metadata fields can be checked here in the future
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
