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

//! Column projection for `ArrowReader`: building the Parquet projection mask
//! from Iceberg field IDs, and mapping field IDs between Iceberg and Parquet
//! (including fallback handling for files without embedded IDs).

use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;

use arrow_schema::{Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use parquet::arrow::{PARQUET_FIELD_ID_META_KEY, ProjectionMask};
use parquet::schema::types::{SchemaDescriptor, Type as ParquetType};

use super::{ArrowReader, CollectFieldIdVisitor};
use crate::arrow::arrow_schema_to_schema;
use crate::error::Result;
use crate::expr::BoundPredicate;
use crate::expr::visitors::bound_predicate_visitor::visit;
use crate::spec::{NameMapping, NestedField, PrimitiveType, Schema, Type};
use crate::{Error, ErrorKind};

impl ArrowReader {
    pub(super) fn build_field_id_set_and_map(
        parquet_schema: &SchemaDescriptor,
        predicate: &BoundPredicate,
    ) -> Result<(HashSet<i32>, HashMap<i32, usize>)> {
        // Collects all Iceberg field IDs referenced in the filter predicate
        let mut collector = CollectFieldIdVisitor {
            field_ids: HashSet::default(),
        };
        visit(&mut collector, predicate)?;

        let iceberg_field_ids = collector.field_ids();

        // Without embedded field IDs, we fall back to position-based mapping for compatibility
        let field_id_map = match build_field_id_map(parquet_schema)? {
            Some(map) => map,
            None => build_fallback_field_id_map(parquet_schema),
        };

        Ok((iceberg_field_ids, field_id_map))
    }

    /// Recursively extract leaf field IDs because Parquet projection works at the leaf column level.
    /// Nested types (struct/list/map) are flattened in Parquet's columnar format.
    fn include_leaf_field_id(field: &NestedField, field_ids: &mut Vec<i32>) {
        match field.field_type.as_ref() {
            Type::Primitive(_) => {
                field_ids.push(field.id);
            }
            Type::Struct(struct_type) => {
                for nested_field in struct_type.fields() {
                    Self::include_leaf_field_id(nested_field, field_ids);
                }
            }
            Type::List(list_type) => {
                Self::include_leaf_field_id(&list_type.element_field, field_ids);
            }
            Type::Map(map_type) => {
                Self::include_leaf_field_id(&map_type.key_field, field_ids);
                Self::include_leaf_field_id(&map_type.value_field, field_ids);
            }
        }
    }

    pub(super) fn get_arrow_projection_mask(
        field_ids: &[i32],
        iceberg_schema_of_task: &Schema,
        parquet_schema: &SchemaDescriptor,
        arrow_schema: &ArrowSchemaRef,
        use_fallback: bool, // Whether file lacks embedded field IDs (e.g., migrated from Hive/Spark)
    ) -> Result<ProjectionMask> {
        fn type_promotion_is_valid(
            file_type: Option<&PrimitiveType>,
            projected_type: Option<&PrimitiveType>,
        ) -> bool {
            match (file_type, projected_type) {
                (Some(lhs), Some(rhs)) if lhs == rhs => true,
                (Some(PrimitiveType::Int), Some(PrimitiveType::Long)) => true,
                (Some(PrimitiveType::Float), Some(PrimitiveType::Double)) => true,
                (
                    Some(PrimitiveType::Decimal {
                        precision: file_precision,
                        scale: file_scale,
                    }),
                    Some(PrimitiveType::Decimal {
                        precision: requested_precision,
                        scale: requested_scale,
                    }),
                ) if requested_precision >= file_precision && file_scale == requested_scale => true,
                // Uuid will be store as Fixed(16) in parquet file, so the read back type will be Fixed(16).
                (Some(PrimitiveType::Fixed(16)), Some(PrimitiveType::Uuid)) => true,
                _ => false,
            }
        }

        if field_ids.is_empty() {
            return Ok(ProjectionMask::all());
        }

        if use_fallback {
            // Position-based projection necessary because file lacks embedded field IDs
            Self::get_arrow_projection_mask_fallback(field_ids, parquet_schema)
        } else {
            // Field-ID-based projection using embedded field IDs from Parquet metadata

            // Parquet's columnar format requires leaf-level (not top-level struct/list/map) projection
            let mut leaf_field_ids = vec![];
            for field_id in field_ids {
                let field = iceberg_schema_of_task.field_by_id(*field_id);
                if let Some(field) = field {
                    Self::include_leaf_field_id(field, &mut leaf_field_ids);
                }
            }

            Self::get_arrow_projection_mask_with_field_ids(
                &leaf_field_ids,
                iceberg_schema_of_task,
                parquet_schema,
                arrow_schema,
                type_promotion_is_valid,
            )
        }
    }

    /// Standard projection using embedded field IDs from Parquet metadata.
    /// For iceberg-java compatibility with ParquetSchemaUtil.pruneColumns().
    fn get_arrow_projection_mask_with_field_ids(
        leaf_field_ids: &[i32],
        iceberg_schema_of_task: &Schema,
        parquet_schema: &SchemaDescriptor,
        arrow_schema: &ArrowSchemaRef,
        type_promotion_is_valid: fn(Option<&PrimitiveType>, Option<&PrimitiveType>) -> bool,
    ) -> Result<ProjectionMask> {
        let mut column_map = HashMap::new();
        let fields = arrow_schema.fields();

        // Pre-project only the fields that have been selected, possibly avoiding converting
        // some Arrow types that are not yet supported.
        let mut projected_fields: HashMap<arrow_schema::FieldRef, i32> = HashMap::new();
        let projected_arrow_schema = ArrowSchema::new_with_metadata(
            fields.filter_leaves(|_, f| {
                f.metadata()
                    .get(PARQUET_FIELD_ID_META_KEY)
                    .and_then(|field_id| i32::from_str(field_id).ok())
                    .is_some_and(|field_id| {
                        projected_fields.insert((*f).clone(), field_id);
                        leaf_field_ids.contains(&field_id)
                    })
            }),
            arrow_schema.metadata().clone(),
        );
        let iceberg_schema = arrow_schema_to_schema(&projected_arrow_schema)?;

        fields.filter_leaves(|idx, field| {
            let Some(field_id) = projected_fields.get(field).cloned() else {
                return false;
            };

            let iceberg_field = iceberg_schema_of_task.field_by_id(field_id);
            let parquet_iceberg_field = iceberg_schema.field_by_id(field_id);

            if iceberg_field.is_none() || parquet_iceberg_field.is_none() {
                return false;
            }

            if !type_promotion_is_valid(
                parquet_iceberg_field
                    .unwrap()
                    .field_type
                    .as_primitive_type(),
                iceberg_field.unwrap().field_type.as_primitive_type(),
            ) {
                return false;
            }

            column_map.insert(field_id, idx);
            true
        });

        // Schema evolution: New columns may not exist in old Parquet files.
        // We only project existing columns; RecordBatchTransformer adds default/NULL values.
        let mut indices = vec![];
        for field_id in leaf_field_ids {
            if let Some(col_idx) = column_map.get(field_id) {
                indices.push(*col_idx);
            }
        }

        if indices.is_empty() {
            // Edge case: All requested columns are new (don't exist in file).
            // Project all columns so RecordBatchTransformer has a batch to transform.
            Ok(ProjectionMask::all())
        } else {
            Ok(ProjectionMask::leaves(parquet_schema, indices))
        }
    }

    /// Fallback projection for Parquet files without field IDs.
    /// Uses position-based matching: field ID N → column position N-1.
    /// Projects entire top-level columns (including nested content) for iceberg-java compatibility.
    fn get_arrow_projection_mask_fallback(
        field_ids: &[i32],
        parquet_schema: &SchemaDescriptor,
    ) -> Result<ProjectionMask> {
        // Position-based: field_id N → column N-1 (field IDs are 1-indexed)
        let parquet_root_fields = parquet_schema.root_schema().get_fields();
        let mut root_indices = vec![];

        for field_id in field_ids.iter() {
            let parquet_pos = (*field_id - 1) as usize;

            if parquet_pos < parquet_root_fields.len() {
                root_indices.push(parquet_pos);
            }
            // RecordBatchTransformer adds missing columns with NULL values
        }

        if root_indices.is_empty() {
            Ok(ProjectionMask::all())
        } else {
            Ok(ProjectionMask::roots(parquet_schema, root_indices))
        }
    }
}

/// Build the map of parquet field id to Parquet column index in the schema.
/// Returns None if the Parquet file doesn't have field IDs embedded (e.g., migrated tables).
pub(super) fn build_field_id_map(
    parquet_schema: &SchemaDescriptor,
) -> Result<Option<HashMap<i32, usize>>> {
    let mut column_map = HashMap::new();

    for (idx, field) in parquet_schema.columns().iter().enumerate() {
        let field_type = field.self_type();
        match field_type {
            ParquetType::PrimitiveType { basic_info, .. } => {
                if !basic_info.has_id() {
                    return Ok(None);
                }
                column_map.insert(basic_info.id(), idx);
            }
            ParquetType::GroupType { .. } => {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Leaf column in schema should be primitive type but got {field_type:?}"
                    ),
                ));
            }
        };
    }

    Ok(Some(column_map))
}

/// Build a fallback field ID map for Parquet files without embedded field IDs.
///
/// Returns the number of primitive (leaf) columns in a Parquet type, recursing into groups.
fn leaf_count(ty: &parquet::schema::types::Type) -> usize {
    if ty.is_primitive() {
        1
    } else {
        ty.get_fields().iter().map(|f| leaf_count(f)).sum()
    }
}

/// Builds a mapping from fallback field IDs to leaf column indices for Parquet files
/// without embedded field IDs. Returns entries only for primitive top-level fields.
///
/// Must use top-level field positions (not leaf column positions) to stay consistent
/// with `add_fallback_field_ids_to_arrow_schema`, which assigns ordinal IDs to
/// top-level Arrow fields. Using leaf positions instead would produce wrong indices
/// when nested types (struct/list/map) expand into multiple leaf columns.
///
/// Mirrors iceberg-java's ParquetSchemaUtil.addFallbackIds() which iterates
/// fileSchema.getFields() assigning ordinal IDs to top-level fields.
pub(super) fn build_fallback_field_id_map(
    parquet_schema: &SchemaDescriptor,
) -> HashMap<i32, usize> {
    let mut column_map = HashMap::new();
    let mut leaf_idx = 0;

    for (top_pos, field) in parquet_schema.root_schema().get_fields().iter().enumerate() {
        let field_id = (top_pos + 1) as i32;
        if field.is_primitive() {
            column_map.insert(field_id, leaf_idx);
        }
        leaf_idx += leaf_count(field);
    }

    column_map
}

/// Apply name mapping to Arrow schema for Parquet files lacking field IDs.
///
/// Assigns Iceberg field IDs based on column names using the name mapping,
/// enabling correct projection on migrated files (e.g., from Hive/Spark via add_files).
///
/// Per Iceberg spec Column Projection rule #2:
/// "Use schema.name-mapping.default metadata to map field id to columns without field id"
/// https://iceberg.apache.org/spec/#column-projection
///
/// Corresponds to Java's ParquetSchemaUtil.applyNameMapping() and ApplyNameMapping visitor.
/// The key difference is Java operates on Parquet MessageType, while we operate on Arrow Schema.
///
/// # Arguments
/// * `arrow_schema` - Arrow schema from Parquet file (without field IDs)
/// * `name_mapping` - Name mapping from table metadata (TableProperties.DEFAULT_NAME_MAPPING)
///
/// # Returns
/// Arrow schema with field IDs assigned based on name mapping
pub(super) fn apply_name_mapping_to_arrow_schema(
    arrow_schema: ArrowSchemaRef,
    name_mapping: &NameMapping,
) -> Result<Arc<ArrowSchema>> {
    debug_assert!(
        arrow_schema
            .fields()
            .iter()
            .next()
            .is_none_or(|f| f.metadata().get(PARQUET_FIELD_ID_META_KEY).is_none()),
        "Schema already has field IDs - name mapping should not be applied"
    );

    let fields_with_mapped_ids: Vec<_> = arrow_schema
        .fields()
        .iter()
        .map(|field| {
            // Look up this column name in name mapping to get the Iceberg field ID.
            // Corresponds to Java's ApplyNameMapping visitor which calls
            // nameMapping.find(currentPath()) and returns field.withId() if found.
            //
            // If the field isn't in the mapping, leave it WITHOUT assigning an ID
            // (matching Java's behavior of returning the field unchanged).
            // Later, during projection, fields without IDs are filtered out.
            let mapped_field_opt = name_mapping
                .fields()
                .iter()
                .find(|f| f.names().contains(&field.name().to_string()));

            let mut metadata = field.metadata().clone();

            if let Some(mapped_field) = mapped_field_opt
                && let Some(field_id) = mapped_field.field_id()
            {
                // Field found in mapping with a field_id → assign it
                metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string());
            }
            // If field_id is None, leave the field without an ID (will be filtered by projection)

            Field::new(field.name(), field.data_type().clone(), field.is_nullable())
                .with_metadata(metadata)
        })
        .collect();

    Ok(Arc::new(ArrowSchema::new_with_metadata(
        fields_with_mapped_ids,
        arrow_schema.metadata().clone(),
    )))
}

/// Add position-based fallback field IDs to Arrow schema for Parquet files lacking them.
/// Enables projection on migrated files (e.g., from Hive/Spark).
///
/// Why at schema level (not per-batch): Efficiency - avoids repeated schema modification.
/// Why only top-level: Nested projection uses leaf column indices, not parent struct IDs.
/// Why 1-indexed: Compatibility with iceberg-java's ParquetSchemaUtil.addFallbackIds().
pub(super) fn add_fallback_field_ids_to_arrow_schema(
    arrow_schema: &ArrowSchemaRef,
) -> Arc<ArrowSchema> {
    debug_assert!(
        arrow_schema
            .fields()
            .iter()
            .next()
            .is_none_or(|f| f.metadata().get(PARQUET_FIELD_ID_META_KEY).is_none()),
        "Schema already has field IDs"
    );

    let fields_with_fallback_ids: Vec<_> = arrow_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(pos, field)| {
            let mut metadata = field.metadata().clone();
            let field_id = (pos + 1) as i32; // 1-indexed for Java compatibility
            metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string());

            Field::new(field.name(), field.data_type().clone(), field.is_nullable())
                .with_metadata(metadata)
        })
        .collect();

    Arc::new(ArrowSchema::new_with_metadata(
        fields_with_fallback_ids,
        arrow_schema.metadata().clone(),
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::sync::Arc;

    use arrow_array::cast::AsArray;
    use arrow_array::{ArrayRef, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};
    use futures::TryStreamExt;
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY, ProjectionMask};
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    use parquet::schema::parser::parse_message_type;
    use parquet::schema::types::SchemaDescriptor;
    use tempfile::TempDir;

    use crate::ErrorKind;
    use crate::arrow::{ArrowReader, ArrowReaderBuilder};
    use crate::expr::{Bind, Reference};
    use crate::io::FileIO;
    use crate::scan::{FileScanTask, FileScanTaskStream};
    use crate::spec::{DataFileFormat, Datum, NestedField, PrimitiveType, Schema, Type};

    #[test]
    fn test_arrow_projection_mask() {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_identifier_field_ids(vec![1])
                .with_fields(vec![
                    NestedField::required(1, "c1", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::optional(2, "c2", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(
                        3,
                        "c3",
                        Type::Primitive(PrimitiveType::Decimal {
                            precision: 38,
                            scale: 3,
                        }),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("c1", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            // Type not supported
            Field::new("c2", DataType::Duration(TimeUnit::Microsecond), true).with_metadata(
                HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "2".to_string())]),
            ),
            // Precision is beyond the supported range
            Field::new("c3", DataType::Decimal128(39, 3), true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "3".to_string(),
            )])),
        ]));

        let message_type = "
message schema {
  required binary c1 (STRING) = 1;
  optional int32 c2 (INTEGER(8,true)) = 2;
  optional fixed_len_byte_array(17) c3 (DECIMAL(39,3)) = 3;
}
    ";
        let parquet_type = parse_message_type(message_type).expect("should parse schema");
        let parquet_schema = SchemaDescriptor::new(Arc::new(parquet_type));

        // Try projecting the fields c2 and c3 with the unsupported data types
        let err = ArrowReader::get_arrow_projection_mask(
            &[1, 2, 3],
            &schema,
            &parquet_schema,
            &arrow_schema,
            false,
        )
        .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            err.to_string(),
            "DataInvalid => Unsupported Arrow data type: Duration(µs)".to_string()
        );

        // Omitting field c2, we still get an error due to c3 being selected
        let err = ArrowReader::get_arrow_projection_mask(
            &[1, 3],
            &schema,
            &parquet_schema,
            &arrow_schema,
            false,
        )
        .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            err.to_string(),
            "DataInvalid => Failed to create decimal type, source: DataInvalid => Decimals with precision larger than 38 are not supported: 39".to_string()
        );

        // Finally avoid selecting fields with unsupported data types
        let mask = ArrowReader::get_arrow_projection_mask(
            &[1],
            &schema,
            &parquet_schema,
            &arrow_schema,
            false,
        )
        .expect("Some ProjectionMask");
        assert_eq!(mask, ProjectionMask::leaves(&parquet_schema, vec![0]));
    }

    /// Test schema evolution: reading old Parquet file (with only column 'a')
    /// using a newer table schema (with columns 'a' and 'b').
    /// This tests that:
    /// 1. get_arrow_projection_mask allows missing columns
    /// 2. RecordBatchTransformer adds missing column 'b' with NULL values
    #[tokio::test]
    async fn test_schema_evolution_add_column() {
        use arrow_array::{Array, Int32Array};

        // New table schema: columns 'a' and 'b' (b was added later, file only has 'a')
        let new_schema = Arc::new(
            Schema::builder()
                .with_schema_id(2)
                .with_fields(vec![
                    NestedField::required(1, "a", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(2, "b", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        // Create Arrow schema for old Parquet file (only has column 'a')
        let arrow_schema_old = Arc::new(ArrowSchema::new(vec![
            Field::new("a", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
        ]));

        // Write old Parquet file with only column 'a'
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let data_a = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let to_write = RecordBatch::try_new(arrow_schema_old.clone(), vec![data_a]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let file = File::create(format!("{table_location}/old_file.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();
        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        // Read the old Parquet file using the NEW schema (with column 'b')
        let reader = ArrowReaderBuilder::new(file_io).build();
        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask {
                file_size_in_bytes: std::fs::metadata(format!("{table_location}/old_file.parquet"))
                    .unwrap()
                    .len(),
                start: 0,
                length: 0,
                record_count: None,
                data_file_path: format!("{table_location}/old_file.parquet"),
                data_file_format: DataFileFormat::Parquet,
                schema: new_schema.clone(),
                project_field_ids: vec![1, 2], // Request both columns 'a' and 'b'
                predicate: None,
                deletes: vec![],
                partition: None,
                partition_spec: None,
                name_mapping: None,
                case_sensitive: false,
            })]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        // Verify we got the correct data
        assert_eq!(result.len(), 1);
        let batch = &result[0];

        // Should have 2 columns now
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 3);

        // Column 'a' should have the original data
        let col_a = batch
            .column(0)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(col_a.values(), &[1, 2, 3]);

        // Column 'b' should be all NULLs (it didn't exist in the old file)
        let col_b = batch
            .column(1)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(col_b.null_count(), 3);
        assert!(col_b.is_null(0));
        assert!(col_b.is_null(1));
        assert!(col_b.is_null(2));
    }

    /// Test reading Parquet files without field ID metadata (e.g., migrated tables).
    /// This exercises the position-based fallback path.
    ///
    /// Corresponds to Java's ParquetSchemaUtil.addFallbackIds() + pruneColumnsFallback()
    /// in /parquet/src/main/java/org/apache/iceberg/parquet/ParquetSchemaUtil.java
    #[tokio::test]
    async fn test_read_parquet_file_without_field_ids() {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(2, "age", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        // Parquet file from a migrated table - no field ID metadata
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, false),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let name_data = vec!["Alice", "Bob", "Charlie"];
        let age_data = vec![30, 25, 35];

        use arrow_array::Int32Array;
        let name_col = Arc::new(StringArray::from(name_data.clone())) as ArrayRef;
        let age_col = Arc::new(Int32Array::from(age_data.clone())) as ArrayRef;

        let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![name_col, age_col]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let file = File::create(format!("{table_location}/1.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();

        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        let reader = ArrowReaderBuilder::new(file_io).build();

        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask {
                file_size_in_bytes: std::fs::metadata(format!("{table_location}/1.parquet"))
                    .unwrap()
                    .len(),
                start: 0,
                length: 0,
                record_count: None,
                data_file_path: format!("{table_location}/1.parquet"),
                data_file_format: DataFileFormat::Parquet,
                schema: schema.clone(),
                project_field_ids: vec![1, 2],
                predicate: None,
                deletes: vec![],
                partition: None,
                partition_spec: None,
                name_mapping: None,
                case_sensitive: false,
            })]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);

        // Verify position-based mapping: field_id 1 → position 0, field_id 2 → position 1
        let name_array = batch.column(0).as_string::<i32>();
        assert_eq!(name_array.value(0), "Alice");
        assert_eq!(name_array.value(1), "Bob");
        assert_eq!(name_array.value(2), "Charlie");

        let age_array = batch
            .column(1)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(age_array.value(0), 30);
        assert_eq!(age_array.value(1), 25);
        assert_eq!(age_array.value(2), 35);
    }

    /// Test reading Parquet files without field IDs with partial projection.
    /// Only a subset of columns are requested, verifying position-based fallback
    /// handles column selection correctly.
    #[tokio::test]
    async fn test_read_parquet_without_field_ids_partial_projection() {
        use arrow_array::Int32Array;

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "col1", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(2, "col2", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(3, "col3", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(4, "col4", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("col1", DataType::Utf8, false),
            Field::new("col2", DataType::Int32, false),
            Field::new("col3", DataType::Utf8, false),
            Field::new("col4", DataType::Int32, false),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let col1_data = Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef;
        let col2_data = Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef;
        let col3_data = Arc::new(StringArray::from(vec!["c", "d"])) as ArrayRef;
        let col4_data = Arc::new(Int32Array::from(vec![30, 40])) as ArrayRef;

        let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![
            col1_data, col2_data, col3_data, col4_data,
        ])
        .unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let file = File::create(format!("{table_location}/1.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();

        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        let reader = ArrowReaderBuilder::new(file_io).build();

        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask {
                file_size_in_bytes: std::fs::metadata(format!("{table_location}/1.parquet"))
                    .unwrap()
                    .len(),
                start: 0,
                length: 0,
                record_count: None,
                data_file_path: format!("{table_location}/1.parquet"),
                data_file_format: DataFileFormat::Parquet,
                schema: schema.clone(),
                project_field_ids: vec![1, 3],
                predicate: None,
                deletes: vec![],
                partition: None,
                partition_spec: None,
                name_mapping: None,
                case_sensitive: false,
            })]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);

        let col1_array = batch.column(0).as_string::<i32>();
        assert_eq!(col1_array.value(0), "a");
        assert_eq!(col1_array.value(1), "b");

        let col3_array = batch.column(1).as_string::<i32>();
        assert_eq!(col3_array.value(0), "c");
        assert_eq!(col3_array.value(1), "d");
    }

    /// Test reading Parquet files without field IDs with schema evolution.
    /// The Iceberg schema has more fields than the Parquet file, testing that
    /// missing columns are filled with NULLs.
    #[tokio::test]
    async fn test_read_parquet_without_field_ids_schema_evolution() {
        use arrow_array::{Array, Int32Array};

        // Schema with field 3 added after the file was written
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(2, "age", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(3, "city", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, false),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let name_data = Arc::new(StringArray::from(vec!["Alice", "Bob"])) as ArrayRef;
        let age_data = Arc::new(Int32Array::from(vec![30, 25])) as ArrayRef;

        let to_write =
            RecordBatch::try_new(arrow_schema.clone(), vec![name_data, age_data]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let file = File::create(format!("{table_location}/1.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();

        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        let reader = ArrowReaderBuilder::new(file_io).build();

        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask {
                file_size_in_bytes: std::fs::metadata(format!("{table_location}/1.parquet"))
                    .unwrap()
                    .len(),
                start: 0,
                length: 0,
                record_count: None,
                data_file_path: format!("{table_location}/1.parquet"),
                data_file_format: DataFileFormat::Parquet,
                schema: schema.clone(),
                project_field_ids: vec![1, 2, 3],
                predicate: None,
                deletes: vec![],
                partition: None,
                partition_spec: None,
                name_mapping: None,
                case_sensitive: false,
            })]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);

        let name_array = batch.column(0).as_string::<i32>();
        assert_eq!(name_array.value(0), "Alice");
        assert_eq!(name_array.value(1), "Bob");

        let age_array = batch
            .column(1)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(age_array.value(0), 30);
        assert_eq!(age_array.value(1), 25);

        // Verify missing column filled with NULLs
        let city_array = batch.column(2).as_string::<i32>();
        assert_eq!(city_array.null_count(), 2);
        assert!(city_array.is_null(0));
        assert!(city_array.is_null(1));
    }

    /// Test reading Parquet files without field IDs that have multiple row groups.
    /// This ensures the position-based fallback works correctly across row group boundaries.
    #[tokio::test]
    async fn test_read_parquet_without_field_ids_multiple_row_groups() {
        use arrow_array::Int32Array;

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(2, "value", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        // Small row group size to create multiple row groups
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_write_batch_size(2)
            .set_max_row_group_row_count(Some(2))
            .build();

        let file = File::create(format!("{table_location}/1.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props)).unwrap();

        // Write 6 rows in 3 batches (will create 3 row groups)
        for batch_num in 0..3 {
            let name_data = Arc::new(StringArray::from(vec![
                format!("name_{}", batch_num * 2),
                format!("name_{}", batch_num * 2 + 1),
            ])) as ArrayRef;
            let value_data =
                Arc::new(Int32Array::from(vec![batch_num * 2, batch_num * 2 + 1])) as ArrayRef;

            let batch =
                RecordBatch::try_new(arrow_schema.clone(), vec![name_data, value_data]).unwrap();
            writer.write(&batch).expect("Writing batch");
        }
        writer.close().unwrap();

        let reader = ArrowReaderBuilder::new(file_io).build();

        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask {
                file_size_in_bytes: std::fs::metadata(format!("{table_location}/1.parquet"))
                    .unwrap()
                    .len(),
                start: 0,
                length: 0,
                record_count: None,
                data_file_path: format!("{table_location}/1.parquet"),
                data_file_format: DataFileFormat::Parquet,
                schema: schema.clone(),
                project_field_ids: vec![1, 2],
                predicate: None,
                deletes: vec![],
                partition: None,
                partition_spec: None,
                name_mapping: None,
                case_sensitive: false,
            })]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        assert!(!result.is_empty());

        let mut all_names = Vec::new();
        let mut all_values = Vec::new();

        for batch in &result {
            let name_array = batch.column(0).as_string::<i32>();
            let value_array = batch
                .column(1)
                .as_primitive::<arrow_array::types::Int32Type>();

            for i in 0..batch.num_rows() {
                all_names.push(name_array.value(i).to_string());
                all_values.push(value_array.value(i));
            }
        }

        assert_eq!(all_names.len(), 6);
        assert_eq!(all_values.len(), 6);

        for i in 0..6 {
            assert_eq!(all_names[i], format!("name_{i}"));
            assert_eq!(all_values[i], i as i32);
        }
    }

    /// Test reading Parquet files without field IDs with nested types (struct).
    /// Java's pruneColumnsFallback() projects entire top-level columns including nested content.
    /// This test verifies that a top-level struct field is projected correctly with all its nested fields.
    #[tokio::test]
    async fn test_read_parquet_without_field_ids_with_struct() {
        use arrow_array::{Int32Array, StructArray};
        use arrow_schema::Fields;

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(
                        2,
                        "person",
                        Type::Struct(crate::spec::StructType::new(vec![
                            NestedField::required(
                                3,
                                "name",
                                Type::Primitive(PrimitiveType::String),
                            )
                            .into(),
                            NestedField::required(4, "age", Type::Primitive(PrimitiveType::Int))
                                .into(),
                        ])),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "person",
                DataType::Struct(Fields::from(vec![
                    Field::new("name", DataType::Utf8, false),
                    Field::new("age", DataType::Int32, false),
                ])),
                false,
            ),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let id_data = Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef;
        let name_data = Arc::new(StringArray::from(vec!["Alice", "Bob"])) as ArrayRef;
        let age_data = Arc::new(Int32Array::from(vec![30, 25])) as ArrayRef;
        let person_data = Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("name", DataType::Utf8, false)),
                name_data,
            ),
            (
                Arc::new(Field::new("age", DataType::Int32, false)),
                age_data,
            ),
        ])) as ArrayRef;

        let to_write =
            RecordBatch::try_new(arrow_schema.clone(), vec![id_data, person_data]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let file = File::create(format!("{table_location}/1.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();

        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        let reader = ArrowReaderBuilder::new(file_io).build();

        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask {
                file_size_in_bytes: std::fs::metadata(format!("{table_location}/1.parquet"))
                    .unwrap()
                    .len(),
                start: 0,
                length: 0,
                record_count: None,
                data_file_path: format!("{table_location}/1.parquet"),
                data_file_format: DataFileFormat::Parquet,
                schema: schema.clone(),
                project_field_ids: vec![1, 2],
                predicate: None,
                deletes: vec![],
                partition: None,
                partition_spec: None,
                name_mapping: None,
                case_sensitive: false,
            })]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);

        let id_array = batch
            .column(0)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(id_array.value(0), 1);
        assert_eq!(id_array.value(1), 2);

        let person_array = batch.column(1).as_struct();
        assert_eq!(person_array.num_columns(), 2);

        let name_array = person_array.column(0).as_string::<i32>();
        assert_eq!(name_array.value(0), "Alice");
        assert_eq!(name_array.value(1), "Bob");

        let age_array = person_array
            .column(1)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(age_array.value(0), 30);
        assert_eq!(age_array.value(1), 25);
    }

    /// Test reading Parquet files without field IDs with schema evolution - column added in the middle.
    /// When a new column is inserted between existing columns in the schema order,
    /// the fallback projection must correctly map field IDs to output positions.
    #[tokio::test]
    async fn test_read_parquet_without_field_ids_schema_evolution_add_column_in_middle() {
        use arrow_array::{Array, Int32Array};

        let arrow_schema_old = Arc::new(ArrowSchema::new(vec![
            Field::new("col0", DataType::Int32, true),
            Field::new("col1", DataType::Int32, true),
        ]));

        // New column added between existing columns: col0 (id=1), newCol (id=5), col1 (id=2)
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::optional(1, "col0", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(5, "newCol", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(2, "col1", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let col0_data = Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef;
        let col1_data = Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef;

        let to_write =
            RecordBatch::try_new(arrow_schema_old.clone(), vec![col0_data, col1_data]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let file = File::create(format!("{table_location}/1.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();
        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        let reader = ArrowReaderBuilder::new(file_io).build();

        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask {
                file_size_in_bytes: std::fs::metadata(format!("{table_location}/1.parquet"))
                    .unwrap()
                    .len(),
                start: 0,
                length: 0,
                record_count: None,
                data_file_path: format!("{table_location}/1.parquet"),
                data_file_format: DataFileFormat::Parquet,
                schema: schema.clone(),
                project_field_ids: vec![1, 5, 2],
                predicate: None,
                deletes: vec![],
                partition: None,
                partition_spec: None,
                name_mapping: None,
                case_sensitive: false,
            })]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);

        let result_col0 = batch
            .column(0)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(result_col0.value(0), 1);
        assert_eq!(result_col0.value(1), 2);

        // New column should be NULL (doesn't exist in old file)
        let result_newcol = batch
            .column(1)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(result_newcol.null_count(), 2);
        assert!(result_newcol.is_null(0));
        assert!(result_newcol.is_null(1));

        let result_col1 = batch
            .column(2)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(result_col1.value(0), 10);
        assert_eq!(result_col1.value(1), 20);
    }

    /// Test reading Parquet files without field IDs with a filter that eliminates all row groups.
    /// During development of field ID mapping, we saw a panic when row_selection_enabled=true and
    /// all row groups are filtered out.
    #[tokio::test]
    async fn test_read_parquet_without_field_ids_filter_eliminates_all_rows() {
        use arrow_array::{Float64Array, Int32Array};

        // Schema with fields that will use fallback IDs 1, 2, 3
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(3, "value", Type::Primitive(PrimitiveType::Double))
                        .into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        // Write data where all ids are >= 10
        let id_data = Arc::new(Int32Array::from(vec![10, 11, 12])) as ArrayRef;
        let name_data = Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef;
        let value_data = Arc::new(Float64Array::from(vec![100.0, 200.0, 300.0])) as ArrayRef;

        let to_write =
            RecordBatch::try_new(arrow_schema.clone(), vec![id_data, name_data, value_data])
                .unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let file = File::create(format!("{table_location}/1.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();
        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        // Filter that eliminates all row groups: id < 5
        let predicate = Reference::new("id").less_than(Datum::int(5));

        // Enable both row_group_filtering and row_selection - triggered the panic
        let reader = ArrowReaderBuilder::new(file_io)
            .with_row_group_filtering_enabled(true)
            .with_row_selection_enabled(true)
            .build();

        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask {
                file_size_in_bytes: std::fs::metadata(format!("{table_location}/1.parquet"))
                    .unwrap()
                    .len(),
                start: 0,
                length: 0,
                record_count: None,
                data_file_path: format!("{table_location}/1.parquet"),
                data_file_format: DataFileFormat::Parquet,
                schema: schema.clone(),
                project_field_ids: vec![1, 2, 3],
                predicate: Some(predicate.bind(schema, true).unwrap()),
                deletes: vec![],
                partition: None,
                partition_spec: None,
                name_mapping: None,
                case_sensitive: false,
            })]
            .into_iter(),
        )) as FileScanTaskStream;

        // Should no longer panic
        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        // Should return empty results
        assert!(result.is_empty() || result.iter().all(|batch| batch.num_rows() == 0));
    }

    /// Test bucket partitioning reads source column from data file (not partition metadata).
    ///
    /// This is an integration test verifying the complete ArrowReader pipeline with bucket partitioning.
    /// It corresponds to TestRuntimeFiltering tests in Iceberg Java (e.g., testRenamedSourceColumnTable).
    ///
    /// # Iceberg Spec Requirements
    ///
    /// Per the Iceberg spec "Column Projection" section:
    /// > "Return the value from partition metadata if an **Identity Transform** exists for the field"
    ///
    /// This means:
    /// - Identity transforms (e.g., `identity(dept)`) use constants from partition metadata
    /// - Non-identity transforms (e.g., `bucket(4, id)`) must read source columns from data files
    /// - Partition metadata for bucket transforms stores bucket numbers (0-3), NOT source values
    ///
    /// Java's PartitionUtil.constantsMap() implements this via:
    /// ```java
    /// if (field.transform().isIdentity()) {
    ///     idToConstant.put(field.sourceId(), converted);
    /// }
    /// ```
    ///
    /// # What This Test Verifies
    ///
    /// This test ensures the full ArrowReader → RecordBatchTransformer pipeline correctly handles
    /// bucket partitioning when FileScanTask provides partition_spec and partition_data:
    ///
    /// - Parquet file has field_id=1 named "id" with actual data [1, 5, 9, 13]
    /// - FileScanTask specifies partition_spec with bucket(4, id) and partition_data with bucket=1
    /// - RecordBatchTransformer.constants_map() excludes bucket-partitioned field from constants
    /// - ArrowReader correctly reads [1, 5, 9, 13] from the data file
    /// - Values are NOT replaced with constant 1 from partition metadata
    ///
    /// # Why This Matters
    ///
    /// Without correct handling:
    /// - Runtime filtering would break (e.g., `WHERE id = 5` would fail)
    /// - Query results would be incorrect (all rows would have id=1)
    /// - Bucket partitioning would be unusable for query optimization
    ///
    /// # References
    /// - Iceberg spec: format/spec.md "Column Projection" + "Partition Transforms"
    /// - Java test: spark/src/test/java/.../TestRuntimeFiltering.java
    /// - Java impl: core/src/main/java/org/apache/iceberg/util/PartitionUtil.java
    #[tokio::test]
    async fn test_bucket_partitioning_reads_source_column_from_file() {
        use arrow_array::Int32Array;

        use crate::spec::{Literal, PartitionSpec, Struct, Transform};

        // Iceberg schema with id and name columns
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(0)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap(),
        );

        // Partition spec: bucket(4, id)
        let partition_spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(0)
                .add_partition_field("id", "id_bucket", Transform::Bucket(4))
                .unwrap()
                .build()
                .unwrap(),
        );

        // Partition data: bucket value is 1
        let partition_data = Struct::from_iter(vec![Some(Literal::int(1))]);

        // Create Arrow schema with field IDs for Parquet file
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            Field::new("name", DataType::Utf8, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
        ]));

        // Write Parquet file with data
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let id_data = Arc::new(Int32Array::from(vec![1, 5, 9, 13])) as ArrayRef;
        let name_data =
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie", "Dave"])) as ArrayRef;

        let to_write =
            RecordBatch::try_new(arrow_schema.clone(), vec![id_data, name_data]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let file = File::create(format!("{}/data.parquet", &table_location)).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();
        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        // Read the Parquet file with partition spec and data
        let reader = ArrowReaderBuilder::new(file_io).build();
        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask {
                file_size_in_bytes: std::fs::metadata(format!("{table_location}/data.parquet"))
                    .unwrap()
                    .len(),
                start: 0,
                length: 0,
                record_count: None,
                data_file_path: format!("{table_location}/data.parquet"),
                data_file_format: DataFileFormat::Parquet,
                schema: schema.clone(),
                project_field_ids: vec![1, 2],
                predicate: None,
                deletes: vec![],
                partition: Some(partition_data),
                partition_spec: Some(partition_spec),
                name_mapping: None,
                case_sensitive: false,
            })]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        // Verify we got the correct data
        assert_eq!(result.len(), 1);
        let batch = &result[0];

        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 4);

        // The id column MUST contain actual values from the Parquet file [1, 5, 9, 13],
        // NOT the constant partition value 1
        let id_col = batch
            .column(0)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(id_col.value(0), 1);
        assert_eq!(id_col.value(1), 5);
        assert_eq!(id_col.value(2), 9);
        assert_eq!(id_col.value(3), 13);

        let name_col = batch.column(1).as_string::<i32>();
        assert_eq!(name_col.value(0), "Alice");
        assert_eq!(name_col.value(1), "Bob");
        assert_eq!(name_col.value(2), "Charlie");
        assert_eq!(name_col.value(3), "Dave");
    }

    /// Regression for <https://github.com/apache/iceberg-rust/issues/2306>:
    /// predicate on a column after nested types in a migrated file (no field IDs).
    /// Schema has struct, list, and map columns before the predicate target (`id`),
    /// exercising the fallback field ID mapping across all nested type variants.
    #[tokio::test]
    async fn test_predicate_on_migrated_file_with_nested_types() {
        use serde::{Deserialize, Serialize};
        use serde_arrow::schema::{SchemaLike, TracingOptions};

        #[derive(Serialize, Deserialize)]
        struct Person {
            name: String,
            age: i32,
        }

        #[derive(Serialize, Deserialize)]
        struct Row {
            person: Person,
            people: Vec<Person>,
            props: std::collections::BTreeMap<String, String>,
            id: i32,
        }

        let rows = vec![
            Row {
                person: Person {
                    name: "Alice".into(),
                    age: 30,
                },
                people: vec![Person {
                    name: "Alice".into(),
                    age: 30,
                }],
                props: [("k1".into(), "v1".into())].into(),
                id: 1,
            },
            Row {
                person: Person {
                    name: "Bob".into(),
                    age: 25,
                },
                people: vec![Person {
                    name: "Bob".into(),
                    age: 25,
                }],
                props: [("k2".into(), "v2".into())].into(),
                id: 2,
            },
            Row {
                person: Person {
                    name: "Carol".into(),
                    age: 40,
                },
                people: vec![Person {
                    name: "Carol".into(),
                    age: 40,
                }],
                props: [("k3".into(), "v3".into())].into(),
                id: 3,
            },
        ];

        let tracing_options = TracingOptions::default()
            .map_as_struct(false)
            .strings_as_large_utf8(false)
            .sequence_as_large_list(false);
        let fields = Vec::<arrow_schema::FieldRef>::from_type::<Row>(tracing_options).unwrap();
        let arrow_schema = Arc::new(ArrowSchema::new(fields.clone()));
        let batch = serde_arrow::to_record_batch(&fields, &rows).unwrap();

        // Fallback field IDs: person=1, people=2, props=3, id=4
        let iceberg_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(
                        1,
                        "person",
                        Type::Struct(crate::spec::StructType::new(vec![
                            NestedField::required(
                                5,
                                "name",
                                Type::Primitive(PrimitiveType::String),
                            )
                            .into(),
                            NestedField::required(6, "age", Type::Primitive(PrimitiveType::Int))
                                .into(),
                        ])),
                    )
                    .into(),
                    NestedField::required(
                        2,
                        "people",
                        Type::List(crate::spec::ListType {
                            element_field: NestedField::required(
                                7,
                                "element",
                                Type::Struct(crate::spec::StructType::new(vec![
                                    NestedField::required(
                                        8,
                                        "name",
                                        Type::Primitive(PrimitiveType::String),
                                    )
                                    .into(),
                                    NestedField::required(
                                        9,
                                        "age",
                                        Type::Primitive(PrimitiveType::Int),
                                    )
                                    .into(),
                                ])),
                            )
                            .into(),
                        }),
                    )
                    .into(),
                    NestedField::required(
                        3,
                        "props",
                        Type::Map(crate::spec::MapType {
                            key_field: NestedField::required(
                                10,
                                "key",
                                Type::Primitive(PrimitiveType::String),
                            )
                            .into(),
                            value_field: NestedField::required(
                                11,
                                "value",
                                Type::Primitive(PrimitiveType::String),
                            )
                            .into(),
                        }),
                    )
                    .into(),
                    NestedField::required(4, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_path = format!("{table_location}/1.parquet");

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let file = File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema, Some(props)).unwrap();
        writer.write(&batch).expect("Writing batch");
        writer.close().unwrap();

        let predicate = Reference::new("id").greater_than(Datum::int(1));

        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs())
            .with_row_group_filtering_enabled(true)
            .with_row_selection_enabled(true)
            .build();

        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask {
                file_size_in_bytes: std::fs::metadata(&file_path).unwrap().len(),
                start: 0,
                length: 0,
                record_count: None,
                data_file_path: file_path,
                data_file_format: DataFileFormat::Parquet,
                schema: iceberg_schema.clone(),
                project_field_ids: vec![4],
                predicate: Some(predicate.bind(iceberg_schema, true).unwrap()),
                deletes: vec![],
                partition: None,
                partition_spec: None,
                name_mapping: None,
                case_sensitive: false,
            })]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        let ids: Vec<i32> = result
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_primitive::<arrow_array::types::Int32Type>()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(ids, vec![2, 3]);
    }
}
