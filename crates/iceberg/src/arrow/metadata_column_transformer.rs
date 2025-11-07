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

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, RunArray, StringArray};
use arrow_schema::{DataType, Field};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::{Error, ErrorKind, Result};

/// Represents different types of metadata column transformations that can be applied to a RecordBatch.
/// Each variant encapsulates the data and logic needed for a specific type of metadata column.
#[derive(Debug, Clone)]
pub(crate) enum MetadataColumnTransformation {
    /// Adds a _file column with the file path using Run-End Encoding (REE) for memory efficiency.
    /// The _file column stores the file path from which each row was read, with REE ensuring
    /// that the same file path value is not repeated in memory for every row.
    FilePath {
        file_path: String,
        field_name: String,
        field_id: i32,
    },
    // Future metadata columns can be added here, e.g.:
    // PartitionValue { partition_values: HashMap<String, String>, ... },
    // RowNumber { start: u64, ... },
}

impl MetadataColumnTransformation {
    /// Applies the transformation to a RecordBatch, adding the metadata column at the specified position.
    ///
    /// # Arguments
    /// * `batch` - The input RecordBatch to transform
    /// * `position` - The position at which to insert the metadata column
    ///
    /// # Returns
    /// A new RecordBatch with the metadata column inserted at the given position
    pub(crate) fn apply(&self, batch: RecordBatch, position: usize) -> Result<RecordBatch> {
        match self {
            Self::FilePath {
                file_path,
                field_name,
                field_id,
            } => Self::apply_file_path(batch, file_path, field_name, *field_id, position),
        }
    }

    /// Applies the file path transformation using Run-End Encoding.
    fn apply_file_path(
        batch: RecordBatch,
        file_path: &str,
        field_name: &str,
        field_id: i32,
        position: usize,
    ) -> Result<RecordBatch> {
        let num_rows = batch.num_rows();

        // Use Run-End Encoded array for optimal memory efficiency
        let run_ends = if num_rows == 0 {
            Int32Array::from(Vec::<i32>::new())
        } else {
            Int32Array::from(vec![num_rows as i32])
        };
        let values = if num_rows == 0 {
            StringArray::from(Vec::<&str>::new())
        } else {
            StringArray::from(vec![file_path])
        };

        let file_array = RunArray::try_new(&run_ends, &values).map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                "Failed to create RunArray for _file column",
            )
            .with_source(e)
        })?;

        let run_ends_field = Arc::new(Field::new("run_ends", DataType::Int32, false));
        let values_field = Arc::new(Field::new("values", DataType::Utf8, true));
        let file_field = Field::new(
            field_name,
            DataType::RunEndEncoded(run_ends_field, values_field),
            false,
        );

        Self::insert_column_at_position(batch, Arc::new(file_array), file_field, field_id, position)
    }

    /// Inserts a column at the specified position in a RecordBatch.
    fn insert_column_at_position(
        batch: RecordBatch,
        column_array: arrow_array::ArrayRef,
        field: Field,
        field_id: i32,
        position: usize,
    ) -> Result<RecordBatch> {
        let field_with_metadata = Arc::new(field.with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            field_id.to_string(),
        )])));

        // Build columns vector in a single pass without insert
        let original_columns = batch.columns();
        let mut columns = Vec::with_capacity(original_columns.len() + 1);
        columns.extend_from_slice(&original_columns[..position]);
        columns.push(column_array);
        columns.extend_from_slice(&original_columns[position..]);

        // Build fields vector in a single pass without insert
        let schema = batch.schema();
        let original_fields = schema.fields();
        let mut fields = Vec::with_capacity(original_fields.len() + 1);
        fields.extend(original_fields[..position].iter().cloned());
        fields.push(field_with_metadata);
        fields.extend(original_fields[position..].iter().cloned());

        let schema = Arc::new(arrow_schema::Schema::new(fields));
        RecordBatch::try_new(schema, columns).map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                "Failed to add metadata column to RecordBatch",
            )
            .with_source(e)
        })
    }
}

/// Composes multiple metadata column transformations.
///
/// This allows us to apply multiple metadata columns in sequence, where each transformation
/// builds on the previous one. For example, adding both _file and partition value columns.
pub(crate) struct MetadataTransformer {
    transformations: Vec<(MetadataColumnTransformation, usize)>,
}

impl MetadataTransformer {
    /// Creates a new empty MetadataTransformer.
    pub(crate) fn new() -> Self {
        Self {
            transformations: Vec::new(),
        }
    }

    /// Creates a builder for constructing a MetadataTransformer from projected field IDs.
    pub(crate) fn builder(projected_field_ids: Vec<i32>) -> MetadataTransformerBuilder {
        MetadataTransformerBuilder::new(projected_field_ids)
    }

    /// Applies all registered transformations to the given RecordBatch.
    ///
    /// Transformations are applied in the order they were added. Each transformation
    /// inserts a column at its specified position, so later transformations see the
    /// updated batch with previously inserted columns.
    pub(crate) fn apply(&self, mut batch: RecordBatch) -> Result<RecordBatch> {
        for (transformation, position) in &self.transformations {
            batch = transformation.apply(batch, *position)?;
        }
        Ok(batch)
    }

    /// Returns true if there are any transformations to apply.
    pub(crate) fn has_transformations(&self) -> bool {
        !self.transformations.is_empty()
    }
}

impl Default for MetadataTransformer {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for constructing a MetadataTransformer from projected field IDs.
///
/// This builder analyzes projected field IDs to identify metadata columns (reserved fields)
/// and builds the appropriate transformations. Reserved fields have special handling and
/// are inserted into the RecordBatch at their projected positions.
pub(crate) struct MetadataTransformerBuilder {
    projected_field_ids: Vec<i32>,
    file_path: Option<String>,
}

impl MetadataTransformerBuilder {
    /// Creates a new MetadataTransformerBuilder for the given projected field IDs.
    ///
    /// # Arguments
    /// * `projected_field_ids` - The list of field IDs being projected, including any reserved fields
    pub(crate) fn new(projected_field_ids: Vec<i32>) -> Self {
        Self {
            projected_field_ids,
            file_path: None,
        }
    }

    /// Sets the file path for the _file metadata column.
    ///
    /// # Arguments
    /// * `file_path` - The file path to use for the _file column
    ///
    /// # Returns
    /// Self for method chaining
    pub(crate) fn with_file_path(mut self, file_path: String) -> Self {
        self.file_path = Some(file_path);
        self
    }

    /// Builds the MetadataTransformer by analyzing projected field IDs and creating appropriate transformations.
    ///
    /// This method:
    /// 1. Iterates through projected field IDs to find reserved fields
    /// 2. Calculates the correct position for each reserved field in the final output
    /// 3. Creates transformations for each reserved field found
    pub(crate) fn build(self) -> MetadataTransformer {
        let mut transformations = Vec::new();

        // Iterate through the projected field IDs and check for reserved fields
        for (position, field_id) in self.projected_field_ids.iter().enumerate() {
            // Check if this is a reserved field ID for the _file column
            if *field_id == RESERVED_FIELD_ID_FILE {
                if let Some(ref path) = self.file_path {
                    let transformation = MetadataColumnTransformation::FilePath {
                        file_path: path.clone(),
                        field_name: RESERVED_COL_NAME_FILE.to_string(),
                        field_id: *field_id,
                    };
                    transformations.push((transformation, position));
                }
            }
            // Additional reserved fields can be handled here in the future
        }

        MetadataTransformer { transformations }
    }

    /// Returns the projected field IDs with virtual/reserved fields filtered out.
    ///
    /// This is used to determine which regular (non-virtual) fields should be read from the data file.
    /// Virtual fields are handled by the metadata transformer and should not be included in the
    /// Parquet projection.
    pub(crate) fn project_field_ids_without_virtual(&self) -> Vec<i32> {
        self.projected_field_ids
            .iter()
            .filter(|&&field_id| !Self::is_reserved_field(field_id))
            .copied()
            .collect()
    }

    /// Checks if a field ID is reserved (virtual).
    fn is_reserved_field(field_id: i32) -> bool {
        field_id == RESERVED_FIELD_ID_FILE
        // Additional reserved fields can be checked here
    }
}

impl Default for MetadataTransformerBuilder {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

// Reserved field IDs and names for metadata columns

/// Reserved field ID for the file path (_file) column per Iceberg spec
pub(crate) const RESERVED_FIELD_ID_FILE: i32 = 2147483646;

/// Reserved column name for the file path metadata column
pub(crate) const RESERVED_COL_NAME_FILE: &str = "_file";
