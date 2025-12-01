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

use std::sync::Arc;

use once_cell::sync::Lazy;

use crate::spec::{NestedField, NestedFieldRef, PrimitiveType, Type};
use crate::{Error, ErrorKind, Result};

/// Reserved field ID for the file path (_file) column per Iceberg spec
pub const RESERVED_FIELD_ID_FILE: i32 = i32::MAX - 1;

/// Reserved column name for the file path metadata column
pub const RESERVED_COL_NAME_FILE: &str = "_file";

/// Lazy-initialized Iceberg field definition for the _file metadata column.
/// This field represents the file path as a required string field.
static FILE_FIELD: Lazy<NestedFieldRef> = Lazy::new(|| {
    Arc::new(NestedField::required(
        RESERVED_FIELD_ID_FILE,
        RESERVED_COL_NAME_FILE,
        Type::Primitive(PrimitiveType::String),
    ))
});

/// Returns the Iceberg field definition for the _file metadata column.
///
/// # Returns
/// A reference to the _file field definition as an Iceberg NestedField
pub fn file_field() -> &'static NestedFieldRef {
    &FILE_FIELD
}

/// Extracts the primitive type from a metadata field.
///
/// # Arguments
/// * `field` - The metadata field
///
/// # Returns
/// The PrimitiveType of the field, or an error if the field is not a primitive type
pub fn metadata_field_primitive_type(field: &NestedFieldRef) -> Result<PrimitiveType> {
    field
        .field_type
        .as_primitive_type()
        .cloned()
        .ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Metadata field '{}' must be a primitive type", field.name),
            )
        })
}

/// Returns the Iceberg field definition for a metadata field ID.
///
/// # Arguments
/// * `field_id` - The metadata field ID
///
/// # Returns
/// The Iceberg field definition for the metadata column, or an error if not a metadata field
pub fn get_metadata_field(field_id: i32) -> Result<NestedFieldRef> {
    match field_id {
        RESERVED_FIELD_ID_FILE => Ok(Arc::clone(file_field())),
        _ if is_metadata_field(field_id) => {
            // Future metadata fields can be added here
            Err(Error::new(
                ErrorKind::Unexpected,
                format!(
                    "Metadata field ID {} recognized but field definition not implemented",
                    field_id
                ),
            ))
        }
        _ => Err(Error::new(
            ErrorKind::Unexpected,
            format!("Field ID {} is not a metadata field", field_id),
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
