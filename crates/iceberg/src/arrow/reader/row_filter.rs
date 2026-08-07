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

//! Predicate-driven row filtering for `ArrowReader`: constructing Arrow `RowFilter`s
//! from Iceberg predicates, row-group selection based on column statistics, and
//! row-selection via the Parquet page index. Also includes byte-range row-group
//! filtering used for file splitting.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{Array, BooleanArray, RecordBatch};
use arrow_schema::{ArrowError, SchemaRef as ArrowSchemaRef};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowPredicate, ArrowPredicateFn, RowSelection};
use parquet::file::metadata::ParquetMetaData;
use parquet::schema::types::SchemaDescriptor;

use super::{ArrowReader, PredicateConverter};
use crate::arrow::caching_delete_file_loader::{EqDeleteKey, EqDeleteSet};
use crate::arrow::{arrow_primitive_to_literal, arrow_type_to_type};
use crate::error::Result;
use crate::expr::BoundPredicate;
use crate::expr::visitors::bound_predicate_visitor::visit;
use crate::expr::visitors::page_index_evaluator::PageIndexEvaluator;
use crate::expr::visitors::row_group_metrics_evaluator::RowGroupMetricsEvaluator;
use crate::spec::{Datum, Literal, PrimitiveType, Schema, Type};

/// One equality-delete key column of a decoded batch
struct KeyColumn {
    literals: Vec<Option<Literal>>,
    /// Iceberg type of the column as stored in this data file.
    source_primitive: PrimitiveType,
    /// Whether the file type differs from the table type, so values need `Datum::to`.
    needs_promotion: bool,
}

impl ArrowReader {
    /// Builds the Arrow row-filter predicate for a bound scan predicate.
    pub(super) fn build_scan_predicate(
        predicates: &BoundPredicate,
        parquet_schema: &SchemaDescriptor,
        iceberg_field_ids: &HashSet<i32>,
        field_id_map: &HashMap<i32, usize>,
    ) -> Result<Box<dyn ArrowPredicate>> {
        // Collect Parquet column indices from field ids.
        // If the field id is not found in Parquet schema, it will be ignored due to schema evolution.
        let mut column_indices = iceberg_field_ids
            .iter()
            .filter_map(|field_id| field_id_map.get(field_id).cloned())
            .collect::<Vec<_>>();
        column_indices.sort();

        // The converter that converts `BoundPredicates` to `ArrowPredicates`
        let mut converter = PredicateConverter {
            parquet_schema,
            column_map: field_id_map,
            column_indices: &column_indices,
        };

        // After collecting required leaf column indices used in the predicate,
        // creates the projection mask for the Arrow predicates.
        let projection_mask = ProjectionMask::leaves(parquet_schema, column_indices.clone());
        let predicate_func = visit(&mut converter, predicates)?;
        Ok(Box::new(ArrowPredicateFn::new(
            projection_mask,
            predicate_func,
        )))
    }

    /// Builds one Arrow row-filter predicate per equality-delete set. The predicate is based
    /// on a hash-set lookup (see `EqDeleteSet`). It keeps a row unless its key tuple is present
    /// in that set. A row is deleted when it matches any set (the predicates are AND-ed by the `RowFilter`).
    pub(super) fn build_equality_delete_predicates(
        sets: &[Arc<EqDeleteSet>],
        parquet_schema: &SchemaDescriptor,
        arrow_schema: &ArrowSchemaRef,
        use_position_fallback: bool,
    ) -> Result<Vec<Box<dyn ArrowPredicate>>> {
        let field_id_map =
            Self::resolve_field_id_map(parquet_schema, arrow_schema, use_position_fallback)?;

        let mut predicates: Vec<Box<dyn ArrowPredicate>> = Vec::new();
        for set in sets {
            if set.is_empty() {
                continue;
            }

            // Parquet leaf index for each key column, in `fields` order; a column dropped
            // from this file by schema evolution has no entry.
            let leaf_indices: Vec<Option<usize>> = set
                .fields
                .iter()
                .map(|(_, id, _)| field_id_map.get(id).copied())
                .collect();

            // Every key column may be absent from this file, leaving `column_indices`
            // empty. `ProjectionMask::leaves(schema, [])` predicate with a zero-column
            // batch carrying and correct row count, so the probe still returns one boolean
            // per row and the selection.
            let mut column_indices: Vec<usize> = leaf_indices.iter().flatten().copied().collect();
            column_indices.sort_unstable();
            column_indices.dedup();
            let projection_mask = ProjectionMask::leaves(parquet_schema, column_indices.clone());

            // Position of each key column within the projected batch (parquet-rs presents the
            // masked leaves in ascending leaf-index order).
            let batch_positions: Vec<Option<usize>> = leaf_indices
                .iter()
                .map(|leaf| leaf.and_then(|idx| column_indices.binary_search(&idx).ok()))
                .collect();

            let target_types: Vec<Type> = set.fields.iter().map(|(_, _, ty)| ty.clone()).collect();
            let num_cols = set.fields.len();
            let set = set.clone();

            let predicate_func =
                move |batch: RecordBatch| -> std::result::Result<BooleanArray, ArrowError> {
                    let num_rows = batch.num_rows();

                    let mut columns: Vec<Option<KeyColumn>> = Vec::with_capacity(num_cols);
                    for (i, target_type) in target_types.iter().enumerate() {
                        // A column absent from this file (schema evolution)
                        let Some(pos) = batch_positions[i] else {
                            columns.push(None);
                            continue;
                        };
                        let array = batch.column(pos);
                        let source_type = arrow_type_to_type(array.data_type())
                            .map_err(|e| ArrowError::ComputeError(e.to_string()))?;
                        let source_primitive = source_type
                            .as_primitive_type()
                            .ok_or_else(|| {
                                ArrowError::ComputeError(
                                    "equality delete key column is not a primitive type"
                                        .to_string(),
                                )
                            })?
                            .clone();
                        columns.push(Some(KeyColumn {
                            // Promotion to the table type keeps these comparable with the
                            // parsed delete keys under schema evolution.
                            needs_promotion: source_type != *target_type,
                            literals: arrow_primitive_to_literal(array, &source_type)
                                .map_err(|e| ArrowError::ComputeError(e.to_string()))?,
                            source_primitive,
                        }));
                    }

                    // One hash lookup per row.
                    let mut keep = Vec::with_capacity(num_rows);
                    let mut probe = EqDeleteKey(vec![None; num_cols]);
                    for row in 0..num_rows {
                        for (i, column) in columns.iter_mut().enumerate() {
                            let Some(column) = column else {
                                probe.0[i] = None;
                                continue;
                            };
                            // we can `take` because each cell is probed once.
                            probe.0[i] = match column.literals[row].take() {
                                Some(Literal::Primitive(primitive)) => {
                                    let datum =
                                        Datum::new(column.source_primitive.clone(), primitive);
                                    Some(if column.needs_promotion {
                                        datum
                                            .to(&target_types[i])
                                            .map_err(|e| ArrowError::ComputeError(e.to_string()))?
                                    } else {
                                        datum
                                    })
                                }
                                Some(other) => {
                                    return Err(ArrowError::ComputeError(format!(
                                        "equality delete key column {i} is not a primitive \
                                         literal: {other:?}"
                                    )));
                                }
                                None => None,
                            };
                        }
                        keep.push(!set.keys.contains(&probe));
                    }
                    Ok(BooleanArray::from(keep))
                };

            predicates.push(Box::new(ArrowPredicateFn::new(
                projection_mask,
                predicate_func,
            )));
        }

        Ok(predicates)
    }

    pub(super) fn get_selected_row_group_indices(
        predicate: &BoundPredicate,
        parquet_metadata: &Arc<ParquetMetaData>,
        field_id_map: &HashMap<i32, usize>,
        snapshot_schema: &Schema,
    ) -> Result<Vec<usize>> {
        let row_groups_metadata = parquet_metadata.row_groups();
        let mut results = Vec::with_capacity(row_groups_metadata.len());

        for (idx, row_group_metadata) in row_groups_metadata.iter().enumerate() {
            if RowGroupMetricsEvaluator::eval(
                predicate,
                row_group_metadata,
                field_id_map,
                snapshot_schema,
            )? {
                results.push(idx);
            }
        }

        Ok(results)
    }

    /// Computes a [`RowSelection`] by evaluating the filter predicate against
    /// the Parquet page index (column index + offset index).
    ///
    /// Returns `Ok(None)` when the Parquet file lacks column or offset index
    /// metadata (common with older files written before page indexes became
    /// standard). In that case page-level pruning is simply skipped; row-group
    /// filtering and the Arrow row filter still apply the predicate.
    ///
    /// `Ok(Some(empty))` case means that all rows were filtered by the predicate - returning zero rows
    pub(super) fn get_row_selection_for_filter_predicate(
        predicate: &BoundPredicate,
        parquet_metadata: &Arc<ParquetMetaData>,
        selected_row_groups: &Option<Vec<usize>>,
        field_id_map: &HashMap<i32, usize>,
        snapshot_schema: &Schema,
    ) -> Result<Option<RowSelection>> {
        let Some(column_index) = parquet_metadata.column_index() else {
            tracing::debug!("ColumnIndex was absent while reading this file");
            return Ok(None);
        };

        let Some(offset_index) = parquet_metadata.offset_index() else {
            tracing::debug!("OffsetIndex was absent while reading this file");
            return Ok(None);
        };

        // If all row groups were filtered out, return an empty RowSelection (select no rows)
        //
        if let Some(selected_row_groups) = selected_row_groups
            && selected_row_groups.is_empty()
        {
            return Ok(Some(RowSelection::from(Vec::new())));
        }

        let mut selected_row_groups_idx = 0;

        let page_index = column_index
            .iter()
            .enumerate()
            .zip(offset_index)
            .zip(parquet_metadata.row_groups());

        let mut results = Vec::new();
        for (((idx, column_index), offset_index), row_group_metadata) in page_index {
            if let Some(selected_row_groups) = selected_row_groups {
                // skip row groups that aren't present in selected_row_groups
                if idx == selected_row_groups[selected_row_groups_idx] {
                    selected_row_groups_idx += 1;
                } else {
                    continue;
                }
            }

            let selections_for_page = PageIndexEvaluator::eval(
                predicate,
                column_index,
                offset_index,
                row_group_metadata,
                field_id_map,
                snapshot_schema,
            )?;

            results.push(selections_for_page);

            if let Some(selected_row_groups) = selected_row_groups
                && selected_row_groups_idx == selected_row_groups.len()
            {
                break;
            }
        }

        Ok(Some(
            results.into_iter().flatten().collect::<Vec<_>>().into(),
        ))
    }

    /// Filters row groups by byte range to support Iceberg's file splitting.
    ///
    /// Engines split a data file into multiple scan tasks, each covering a byte range
    /// `[start, start+length)`. Normally Iceberg planning aligns these splits to row group
    /// boundaries using the data file's `split_offsets` metadata, so a task's range never
    /// bisects a row group. But when `split_offsets` is missing (e.g. a manually written or
    /// non-conforming file), planning falls back to tiling the file at the requested split
    /// size, and a task's range can land in the middle of a row group.
    ///
    /// A row group must be read by exactly one task, otherwise its rows are duplicated. We
    /// assign ownership by the row group's midpoint: a task owns a row group only if its range
    /// contains that midpoint. Because the tasks tile the file contiguously and disjointly,
    /// each midpoint falls in exactly one task. This matches parquet-mr's `BlockMetaData`
    /// midpoint semantics. For a whole-file task (`start=0, length=fileSize`, as iceberg-rust's
    /// own planner emits) every midpoint lies in range, so all row groups are selected.
    pub(super) fn filter_row_groups_by_byte_range(
        parquet_metadata: &Arc<ParquetMetaData>,
        start: u64,
        length: u64,
    ) -> Result<Vec<usize>> {
        let row_groups = parquet_metadata.row_groups();
        let mut selected = Vec::new();
        let end = start + length;

        // Row groups are stored sequentially after the 4-byte magic header.
        let mut current_byte_offset = 4u64;

        for (idx, row_group) in row_groups.iter().enumerate() {
            let row_group_size = row_group.compressed_size() as u64;
            let row_group_midpoint = current_byte_offset + row_group_size / 2;

            // Half-open ownership: a midpoint on a task boundary belongs to the upper task,
            // so exactly one task ever claims a given row group.
            if start <= row_group_midpoint && row_group_midpoint < end {
                selected.push(idx);
            }

            current_byte_offset += row_group_size;
        }

        Ok(selected)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::sync::Arc;

    use arrow_array::cast::AsArray;
    use arrow_array::{
        ArrayRef, Decimal128Array, Float64Array, Int32Array, Int64Array, LargeStringArray,
        RecordBatch, StringArray,
    };
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use futures::TryStreamExt;
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
    use parquet::basic::Compression;
    use parquet::file::metadata::{FileMetaData, ParquetMetaData, ParquetMetaDataBuilder};
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    use parquet::schema::parser::parse_message_type;
    use parquet::schema::types::SchemaDescriptor;
    use tempfile::TempDir;

    use crate::Runtime;
    use crate::arrow::{ArrowReader, ArrowReaderBuilder};
    use crate::expr::{Bind, BoundPredicate, Predicate, Reference};
    use crate::io::FileIO;
    use crate::scan::{FileScanTask, FileScanTaskDeleteFile, FileScanTaskStream};
    use crate::spec::{
        DataContentType, DataFileFormat, Datum, Literal, NestedField, PrimitiveType, Schema,
        SchemaRef, Type,
    };

    async fn test_perform_read(
        predicate: Predicate,
        schema: SchemaRef,
        table_location: String,
        reader: ArrowReader,
    ) -> Vec<Option<String>> {
        let tasks = {
            let task = FileScanTask::builder()
                .with_file_size_in_bytes(
                    std::fs::metadata(format!("{table_location}/1.parquet"))
                        .unwrap()
                        .len(),
                )
                .with_start(0)
                .with_length(0)
                .with_data_file_path(format!("{table_location}/1.parquet"))
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(schema.clone())
                .with_project_field_ids(vec![1])
                .with_predicate(Some(predicate.bind(schema, true).unwrap()))
                .with_case_sensitive(false)
                .build();
            Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream
        };

        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        result[0].columns()[0]
            .as_string_opt::<i32>()
            .unwrap()
            .iter()
            .map(|v| v.map(ToOwned::to_owned))
            .collect::<Vec<_>>()
    }

    fn setup_kleene_logic(
        data_for_col_a: Vec<Option<String>>,
        col_a_type: DataType,
    ) -> (FileIO, SchemaRef, String, TempDir) {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::optional(1, "a", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("a", col_a_type.clone(), true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();

        let file_io = FileIO::new_with_fs();

        let col = match col_a_type {
            DataType::Utf8 => Arc::new(StringArray::from(data_for_col_a)) as ArrayRef,
            DataType::LargeUtf8 => Arc::new(LargeStringArray::from(data_for_col_a)) as ArrayRef,
            _ => panic!("unexpected col_a_type"),
        };

        let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![col]).unwrap();

        // Write the Parquet files
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let file = File::create(format!("{table_location}/1.parquet")).unwrap();
        let mut writer =
            ArrowWriter::try_new(file, to_write.schema(), Some(props.clone())).unwrap();

        writer.write(&to_write).expect("Writing batch");

        // writer must be closed to write footer
        writer.close().unwrap();

        (file_io, schema, table_location, tmp_dir)
    }

    #[tokio::test]
    async fn test_kleene_logic_or_behaviour() {
        // a IS NULL OR a = 'foo'
        let predicate = Reference::new("a")
            .is_null()
            .or(Reference::new("a").equal_to(Datum::string("foo")));

        // Table data: [NULL, "foo", "bar"]
        let data_for_col_a = vec![None, Some("foo".to_string()), Some("bar".to_string())];

        // Expected: [NULL, "foo"].
        let expected = vec![None, Some("foo".to_string())];

        let (file_io, schema, table_location, _temp_dir) =
            setup_kleene_logic(data_for_col_a, DataType::Utf8);
        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();

        let result_data = test_perform_read(predicate, schema, table_location, reader).await;

        assert_eq!(result_data, expected);
    }

    #[tokio::test]
    async fn test_kleene_logic_and_behaviour() {
        // a IS NOT NULL AND a != 'foo'
        let predicate = Reference::new("a")
            .is_not_null()
            .and(Reference::new("a").not_equal_to(Datum::string("foo")));

        // Table data: [NULL, "foo", "bar"]
        let data_for_col_a = vec![None, Some("foo".to_string()), Some("bar".to_string())];

        // Expected: ["bar"].
        let expected = vec![Some("bar".to_string())];

        let (file_io, schema, table_location, _temp_dir) =
            setup_kleene_logic(data_for_col_a, DataType::Utf8);
        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();

        let result_data = test_perform_read(predicate, schema, table_location, reader).await;

        assert_eq!(result_data, expected);
    }

    #[tokio::test]
    async fn test_predicate_cast_literal() {
        let predicates = vec![
            // a == 'foo'
            (Reference::new("a").equal_to(Datum::string("foo")), vec![
                Some("foo".to_string()),
            ]),
            // a != 'foo'
            (
                Reference::new("a").not_equal_to(Datum::string("foo")),
                vec![Some("bar".to_string())],
            ),
            // STARTS_WITH(a, 'foo')
            (Reference::new("a").starts_with(Datum::string("f")), vec![
                Some("foo".to_string()),
            ]),
            // NOT STARTS_WITH(a, 'foo')
            (
                Reference::new("a").not_starts_with(Datum::string("f")),
                vec![Some("bar".to_string())],
            ),
            // a < 'foo'
            (Reference::new("a").less_than(Datum::string("foo")), vec![
                Some("bar".to_string()),
            ]),
            // a <= 'foo'
            (
                Reference::new("a").less_than_or_equal_to(Datum::string("foo")),
                vec![Some("foo".to_string()), Some("bar".to_string())],
            ),
            // a > 'foo'
            (
                Reference::new("a").greater_than(Datum::string("bar")),
                vec![Some("foo".to_string())],
            ),
            // a >= 'foo'
            (
                Reference::new("a").greater_than_or_equal_to(Datum::string("foo")),
                vec![Some("foo".to_string())],
            ),
            // a IN ('foo', 'bar')
            (
                Reference::new("a").is_in([Datum::string("foo"), Datum::string("baz")]),
                vec![Some("foo".to_string())],
            ),
            // a NOT IN ('foo', 'bar')
            (
                Reference::new("a").is_not_in([Datum::string("foo"), Datum::string("baz")]),
                vec![Some("bar".to_string())],
            ),
        ];

        // Table data: ["foo", "bar"]
        let data_for_col_a = vec![Some("foo".to_string()), Some("bar".to_string())];

        let (file_io, schema, table_location, _temp_dir) =
            setup_kleene_logic(data_for_col_a, DataType::LargeUtf8);
        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();

        for (predicate, expected) in predicates {
            println!("testing predicate {predicate}");
            let result_data = test_perform_read(
                predicate.clone(),
                schema.clone(),
                table_location.clone(),
                reader.clone(),
            )
            .await;

            assert_eq!(result_data, expected, "predicate={predicate}");
        }
    }

    /// Verifies that file splits respect byte ranges and only read specific row groups.
    #[tokio::test]
    async fn test_file_splits_respect_byte_ranges() {
        use arrow_array::Int32Array;
        use parquet::file::reader::{FileReader, SerializedFileReader};

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_path = format!("{table_location}/multi_row_group.parquet");

        // Force each batch into its own row group for testing byte range filtering.
        let batch1 = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(Int32Array::from(
            (0..100).collect::<Vec<i32>>(),
        ))])
        .unwrap();
        let batch2 = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(Int32Array::from(
            (100..200).collect::<Vec<i32>>(),
        ))])
        .unwrap();
        let batch3 = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(Int32Array::from(
            (200..300).collect::<Vec<i32>>(),
        ))])
        .unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_max_row_group_row_count(Some(100))
            .build();

        let file = File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props)).unwrap();
        writer.write(&batch1).expect("Writing batch 1");
        writer.write(&batch2).expect("Writing batch 2");
        writer.write(&batch3).expect("Writing batch 3");
        writer.close().unwrap();

        // Read the file metadata to get row group byte positions
        let file = File::open(&file_path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        let metadata = reader.metadata();

        println!("File has {} row groups", metadata.num_row_groups());
        assert_eq!(metadata.num_row_groups(), 3, "Expected 3 row groups");

        // Get byte positions for each row group
        let row_group_0 = metadata.row_group(0);
        let row_group_1 = metadata.row_group(1);
        let row_group_2 = metadata.row_group(2);

        let rg0_start = 4u64; // Parquet files start with 4-byte magic "PAR1"
        let rg1_start = rg0_start + row_group_0.compressed_size() as u64;
        let rg2_start = rg1_start + row_group_1.compressed_size() as u64;
        let file_end = rg2_start + row_group_2.compressed_size() as u64;

        println!(
            "Row group 0: {} rows, starts at byte {}, {} bytes compressed",
            row_group_0.num_rows(),
            rg0_start,
            row_group_0.compressed_size()
        );
        println!(
            "Row group 1: {} rows, starts at byte {}, {} bytes compressed",
            row_group_1.num_rows(),
            rg1_start,
            row_group_1.compressed_size()
        );
        println!(
            "Row group 2: {} rows, starts at byte {}, {} bytes compressed",
            row_group_2.num_rows(),
            rg2_start,
            row_group_2.compressed_size()
        );

        let file_io = FileIO::new_with_fs();
        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();

        // Task 1: read only the first row group
        let task1 = FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(&file_path).unwrap().len())
            .with_start(rg0_start)
            .with_length(row_group_0.compressed_size() as u64)
            .with_data_file_path(file_path.clone())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema.clone())
            .with_project_field_ids(vec![1])
            .with_record_count(Some(100))
            .with_case_sensitive(false)
            .build();

        // Task 2: read the second and third row groups
        let task2 = FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(&file_path).unwrap().len())
            .with_start(rg1_start)
            .with_length(file_end - rg1_start)
            .with_data_file_path(file_path.clone())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema.clone())
            .with_project_field_ids(vec![1])
            .with_record_count(Some(200))
            .with_case_sensitive(false)
            .build();

        let tasks1 = Box::pin(futures::stream::iter(vec![Ok(task1)])) as FileScanTaskStream;
        let result1 = reader
            .clone()
            .read(tasks1)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        let total_rows_task1: usize = result1.iter().map(|b| b.num_rows()).sum();
        println!(
            "Task 1 (bytes {}-{}) returned {} rows",
            rg0_start,
            rg0_start + row_group_0.compressed_size() as u64,
            total_rows_task1
        );

        let tasks2 = Box::pin(futures::stream::iter(vec![Ok(task2)])) as FileScanTaskStream;
        let result2 = reader
            .read(tasks2)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        let total_rows_task2: usize = result2.iter().map(|b| b.num_rows()).sum();
        println!("Task 2 (bytes {rg1_start}-{file_end}) returned {total_rows_task2} rows");

        assert_eq!(
            total_rows_task1, 100,
            "Task 1 should read only the first row group (100 rows), but got {total_rows_task1} rows"
        );

        assert_eq!(
            total_rows_task2, 200,
            "Task 2 should read only the second+third row groups (200 rows), but got {total_rows_task2} rows"
        );

        // Verify the actual data values are correct (not just the row count)
        if total_rows_task1 > 0 {
            let first_batch = &result1[0];
            let id_col = first_batch
                .column(0)
                .as_primitive::<arrow_array::types::Int32Type>();
            let first_val = id_col.value(0);
            let last_val = id_col.value(id_col.len() - 1);
            println!("Task 1 data range: {first_val} to {last_val}");

            assert_eq!(first_val, 0, "Task 1 should start with id=0");
            assert_eq!(last_val, 99, "Task 1 should end with id=99");
        }

        if total_rows_task2 > 0 {
            let first_batch = &result2[0];
            let id_col = first_batch
                .column(0)
                .as_primitive::<arrow_array::types::Int32Type>();
            let first_val = id_col.value(0);
            println!("Task 2 first value: {first_val}");

            assert_eq!(first_val, 100, "Task 2 should start with id=100, not id=0");
        }
    }

    /// A single data file split into multiple sub-row-group byte ranges (as Spark/Iceberg
    /// planning produces when split-size is smaller than a row group) must still yield each
    /// row exactly once. The previous overlap-based selection let every split whose byte range
    /// touched a row group read it, duplicating rows; ownership by midpoint reads each row group
    /// from exactly one split.
    #[tokio::test]
    async fn test_sub_row_group_splits_do_not_duplicate_rows() {
        use arrow_array::Int32Array;

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_path = format!("{table_location}/sub_split.parquet");

        // Three row groups of 100 rows each (ids 0..300).
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_max_row_group_row_count(Some(100))
            .build();
        let file = File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props)).unwrap();
        for chunk in [0..100, 100..200, 200..300] {
            let batch = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(
                Int32Array::from(chunk.collect::<Vec<i32>>()),
            )])
            .unwrap();
            writer.write(&batch).expect("Writing batch");
        }
        writer.close().unwrap();

        let file_size = std::fs::metadata(&file_path).unwrap().len();
        let file_io = FileIO::new_with_fs();
        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();

        // Tile the whole file into 64-byte splits, mirroring Spark's split-size planning, and
        // read every split. A 64-byte split is far smaller than a row group, so each row group
        // is touched by several splits but must be owned (read) by exactly one.
        let mut ids = Vec::new();
        let split_size = 64u64;
        let mut start = 0u64;
        while start < file_size {
            let length = split_size.min(file_size - start);
            let task = FileScanTask::builder()
                .with_file_size_in_bytes(file_size)
                .with_start(start)
                .with_length(length)
                .with_data_file_path(file_path.clone())
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(schema.clone())
                .with_project_field_ids(vec![1])
                .with_case_sensitive(false)
                .build();

            let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
            let batches = reader
                .clone()
                .read(tasks)
                .unwrap()
                .stream()
                .try_collect::<Vec<RecordBatch>>()
                .await
                .unwrap();

            for batch in &batches {
                let col = batch
                    .column(0)
                    .as_primitive::<arrow_array::types::Int32Type>();
                ids.extend(col.values().iter().copied());
            }

            start += length;
        }

        ids.sort_unstable();
        assert_eq!(
            ids,
            (0..300).collect::<Vec<i32>>(),
            "each row must be read exactly once across all splits, got {} rows",
            ids.len()
        );
    }

    /// When a split boundary lands exactly on a row group's midpoint, half-open ownership
    /// (`start <= midpoint < end`) must hand that row group to the upper split only: the lower
    /// split ends at the midpoint and so excludes it, the upper split starts at the midpoint and
    /// so claims it. Two splits meeting exactly at the middle row group's midpoint must therefore
    /// read every row once, with the middle row group going to the upper split.
    #[tokio::test]
    async fn test_split_boundary_on_row_group_midpoint() {
        use arrow_array::Int32Array;
        use parquet::file::reader::{FileReader, SerializedFileReader};

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_path = format!("{table_location}/midpoint.parquet");

        // Three row groups of 100 rows each (ids 0..300).
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_max_row_group_row_count(Some(100))
            .build();
        let file = File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props)).unwrap();
        for chunk in [0..100, 100..200, 200..300] {
            let batch = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(
                Int32Array::from(chunk.collect::<Vec<i32>>()),
            )])
            .unwrap();
            writer.write(&batch).expect("Writing batch");
        }
        writer.close().unwrap();

        // Locate the middle row group's exact midpoint. Row groups are stored back to back after
        // the 4-byte magic header.
        let metadata = SerializedFileReader::new(File::open(&file_path).unwrap())
            .unwrap()
            .metadata()
            .clone();
        assert_eq!(metadata.num_row_groups(), 3);
        let rg1_start = 4 + metadata.row_group(0).compressed_size() as u64;
        let rg1_size = metadata.row_group(1).compressed_size() as u64;
        let rg1_midpoint = rg1_start + rg1_size / 2;
        let file_end = rg1_start + rg1_size + metadata.row_group(2).compressed_size() as u64;

        let file_size = std::fs::metadata(&file_path).unwrap().len();
        let file_io = FileIO::new_with_fs();
        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();

        // Two splits meeting exactly at rg1's midpoint. The lower split ends there, the upper
        // starts there; the middle row group must fall to the upper split alone.
        let mut per_split = Vec::new();
        for (start, end) in [(0, rg1_midpoint), (rg1_midpoint, file_end)] {
            let task = FileScanTask::builder()
                .with_file_size_in_bytes(file_size)
                .with_start(start)
                .with_length(end - start)
                .with_data_file_path(file_path.clone())
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(schema.clone())
                .with_project_field_ids(vec![1])
                .with_case_sensitive(false)
                .build();

            let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
            let batches = reader
                .clone()
                .read(tasks)
                .unwrap()
                .stream()
                .try_collect::<Vec<RecordBatch>>()
                .await
                .unwrap();

            let mut ids = Vec::new();
            for batch in &batches {
                let col = batch
                    .column(0)
                    .as_primitive::<arrow_array::types::Int32Type>();
                ids.extend(col.values().iter().copied());
            }
            per_split.push(ids);
        }

        assert_eq!(
            per_split[0],
            (0..100).collect::<Vec<i32>>(),
            "lower split, ending at rg1's midpoint, must read only rg0"
        );
        assert_eq!(
            per_split[1],
            (100..300).collect::<Vec<i32>>(),
            "upper split, starting at rg1's midpoint, must read rg1 and rg2"
        );
    }

    fn int_schema() -> SchemaRef {
        Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "x",
                    Type::Primitive(PrimitiveType::Int),
                ))])
                .build()
                .unwrap(),
        )
    }

    fn simple_predicate(schema: SchemaRef) -> BoundPredicate {
        Reference::new("x")
            .greater_than(Datum::int(0))
            .bind(schema.clone(), false)
            .unwrap()
    }

    fn field_id_map() -> HashMap<i32, usize> {
        let mut m = HashMap::new();
        m.insert(1_i32, 0_usize);
        m
    }

    fn metadata_no_page_indexes() -> Arc<ParquetMetaData> {
        let msg_type = parse_message_type("message schema { REQUIRED INT32 x; }").unwrap();
        let schema_desc = Arc::new(SchemaDescriptor::new(Arc::new(msg_type)));
        let file_meta = FileMetaData::new(2, 0, None, None, schema_desc.clone(), None);
        Arc::new(ParquetMetaDataBuilder::new(file_meta).build())
    }

    fn metadata_column_index_only() -> Arc<ParquetMetaData> {
        let msg_type = parse_message_type("message schema { REQUIRED INT32 x; }").unwrap();
        let schema_desc = Arc::new(SchemaDescriptor::new(Arc::new(msg_type)));
        let file_meta = FileMetaData::new(2, 0, None, None, schema_desc, None);
        Arc::new(
            ParquetMetaDataBuilder::new(file_meta)
                .set_column_index(Some(vec![]))
                .build(),
        )
    }

    fn metadata_with_both_indexes() -> Arc<ParquetMetaData> {
        let msg_type = parse_message_type("message schema { REQUIRED INT32 x; }").unwrap();
        let schema_desc = Arc::new(SchemaDescriptor::new(Arc::new(msg_type)));
        let file_meta = FileMetaData::new(2, 0, None, None, schema_desc, None);
        Arc::new(
            ParquetMetaDataBuilder::new(file_meta)
                .set_column_index(Some(vec![]))
                .set_offset_index(Some(vec![]))
                .build(),
        )
    }

    /// Testing suite regarding: https://github.com/apache/iceberg-rust/issues/2452
    /// Testing when: both indices are absent, some present, both present
    #[test]
    fn test_absent_column_index_returns_ok_none() {
        let schema = int_schema();
        let predicate = simple_predicate(schema.clone());
        let metadata = metadata_no_page_indexes();
        let field_id_map = field_id_map();

        let result = ArrowReader::get_row_selection_for_filter_predicate(
            &predicate,
            &metadata,
            &None,
            &field_id_map,
            schema.as_ref(),
        );

        assert!(
            result.is_ok(),
            "expected Ok(_), got Err: {:?}",
            result.unwrap_err()
        );
        assert!(
            result.unwrap().is_none(),
            "expected Ok(None) when column index is absent"
        );
    }

    #[test]
    fn test_absent_offset_index_returns_ok_none() {
        let schema = int_schema();
        let predicate = simple_predicate(schema.clone());
        let metadata = metadata_column_index_only();
        let field_id_map = field_id_map();

        let result = ArrowReader::get_row_selection_for_filter_predicate(
            &predicate,
            &metadata,
            &None,
            &field_id_map,
            schema.as_ref(),
        );

        assert!(
            result.is_ok(),
            "expected Ok(_), got Err: {:?}",
            result.unwrap_err()
        );
        assert!(
            result.unwrap().is_none(),
            "expected Ok(None) when offset index is absent"
        );
    }

    #[test]
    fn test_absent_column_index_with_selected_row_groups_returns_ok_none() {
        let schema = int_schema();
        let predicate = simple_predicate(schema.clone());
        let metadata = metadata_no_page_indexes();
        let field_id_map = field_id_map();
        let selected = Some(vec![0usize, 1]);

        let result = ArrowReader::get_row_selection_for_filter_predicate(
            &predicate,
            &metadata,
            &selected,
            &field_id_map,
            schema.as_ref(),
        );

        assert!(result.is_ok());
        assert!(
            result.unwrap().is_none(),
            "absent column index must short-circuit before selected_row_groups is inspected"
        );
    }

    #[test]
    fn test_absent_offset_index_with_selected_row_groups_returns_ok_none() {
        let schema = int_schema();
        let predicate = simple_predicate(schema.clone());
        let metadata = metadata_column_index_only();
        let field_id_map = field_id_map();
        let selected = Some(vec![0usize]);

        let result = ArrowReader::get_row_selection_for_filter_predicate(
            &predicate,
            &metadata,
            &selected,
            &field_id_map,
            schema.as_ref(),
        );

        assert!(result.is_ok());
        assert!(
            result.unwrap().is_none(),
            "absent offset index must short-circuit before selected_row_groups is inspected"
        );
    }

    #[test]
    fn test_absent_column_index_with_empty_selected_row_groups_returns_ok_none() {
        let schema = int_schema();
        let predicate = simple_predicate(schema.clone());
        let metadata = metadata_no_page_indexes();
        let field_id_map = field_id_map();
        let selected: Option<Vec<usize>> = Some(vec![]);

        let result = ArrowReader::get_row_selection_for_filter_predicate(
            &predicate,
            &metadata,
            &selected,
            &field_id_map,
            schema.as_ref(),
        );

        assert!(result.is_ok());
        assert!(
            result.unwrap().is_none(),
            "column index check must fire before the empty-selected-row-groups branch"
        );
    }

    #[test]
    fn test_both_indexes_present_empty_selected_row_groups_returns_ok_some_empty() {
        let schema = int_schema();
        let predicate = simple_predicate(schema.clone());
        let metadata = metadata_with_both_indexes();
        let field_id_map = field_id_map();
        let selected: Option<Vec<usize>> = Some(vec![]);

        let result = ArrowReader::get_row_selection_for_filter_predicate(
            &predicate,
            &metadata,
            &selected,
            &field_id_map,
            schema.as_ref(),
        );

        assert!(
            result.is_ok(),
            "expected Ok(_), got Err: {:?}",
            result.unwrap_err()
        );

        let row_selection = result.unwrap().expect(
            "expected Ok(Some(_)) when both indexes are present and all row groups are filtered",
        );

        assert_eq!(
            row_selection.row_count(),
            0,
            "RowSelection must be empty (zero rows selected) when selected_row_groups is empty"
        );
    }

    /// Full-suite regression test for issue: https://github.com/apache/iceberg-rust/issues/2452
    #[tokio::test]
    async fn test_scan_without_page_indexes_does_not_error() {
        // Building schema (both iceberg and arrow for RecordBatch)
        let iceberg_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap(),
        );

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

        let tmp_dir = TempDir::new().unwrap();
        let file_path = format!("{}/data.parquet", tmp_dir.path().to_str().unwrap());

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            // Disabling page statistics
            .set_statistics_enabled(EnabledStatistics::None)
            .build();

        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("alice"),
                Some("bob"),
                None,
                Some("dana"),
                Some("eve"),
            ])) as ArrayRef,
        ])
        .unwrap();

        let file = File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Truly exercising a file without column/offset index
        {
            use parquet::file::reader::{FileReader, SerializedFileReader};
            let f = File::open(&file_path).unwrap();
            let rdr = SerializedFileReader::new(f).unwrap();
            assert!(
                rdr.metadata().column_index().is_none(),
                "test fixture must produce a file without a column index"
            );
            assert!(
                rdr.metadata().offset_index().is_none(),
                "test fixture must a product a file without offset index"
            )
        }

        // Predicate: id > 2
        let predicate = Reference::new("id")
            .greater_than(Datum::int(2))
            .bind(iceberg_schema.clone(), false)
            .unwrap();

        let file_io = FileIO::new_with_fs();
        let reader = ArrowReaderBuilder::new(file_io.clone(), Runtime::current())
            // Enabling row selection ()
            .with_row_selection_enabled(true)
            .build();

        let file_size = std::fs::metadata(&file_path).unwrap().len();
        let task = FileScanTask {
            file_size_in_bytes: file_size,
            start: 0,
            length: 0,
            record_count: None,
            first_row_id: None,
            data_sequence_number: None,
            data_file_path: file_path.clone(),
            data_file_format: DataFileFormat::Parquet,
            schema: iceberg_schema.clone(),
            project_field_ids: vec![1, 2],
            predicate: Some(predicate),
            deletes: vec![],
            partition: None,
            partition_spec: None,
            name_mapping: None,
            unified_partition_type: None,
            case_sensitive: false,
            key_metadata: None,
        };

        let stream = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let batches: Vec<RecordBatch> = reader
            .read(stream)
            .unwrap()
            .stream()
            .try_collect()
            .await
            .unwrap();

        let ids: Vec<i32> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();

        assert_eq!(
            ids,
            vec![3, 4, 5],
            "predicate must still be enforced via Arrow row filter even without page indexes"
        );

        // Absent index + position delete field present
        let pos_del_path = format!("{}/pos-del.parquet", tmp_dir.path().to_str().unwrap());

        // Build the position delete Arrow schema (standard Iceberg layout).
        let pos_del_arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("file_path", DataType::Utf8, false),
            Field::new("pos", DataType::Int64, false),
        ]));

        let pos_del_batch = RecordBatch::try_new(pos_del_arrow_schema.clone(), vec![
            // Both deletions reference the same data file
            Arc::new(StringArray::from(vec![
                file_path.as_str(),
                file_path.as_str(),
            ])) as ArrayRef,
            // Delete by index - index-0 (`1` in test case) and index-2 (`3` in test case)
            Arc::new(Int64Array::from(vec![0i64, 2i64])) as ArrayRef,
        ])
        .unwrap();

        // Write position delete file also without indices
        let pos_del_props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_statistics_enabled(EnabledStatistics::None)
            .build();

        let pos_del_file = File::create(&pos_del_path).unwrap();
        let mut pos_del_writer = ArrowWriter::try_new(
            pos_del_file,
            pos_del_arrow_schema.clone(),
            Some(pos_del_props),
        )
        .unwrap();
        pos_del_writer.write(&pos_del_batch).unwrap();
        pos_del_writer.close().unwrap();

        // Predicate: id > 1 (note that `3` was also removed by position delete)
        let predicate_sub2 = Reference::new("id")
            .greater_than(Datum::int(1))
            .bind(iceberg_schema.clone(), false)
            .unwrap();

        let reader_sub2 = ArrowReaderBuilder::new(file_io.clone(), Runtime::current())
            .with_row_selection_enabled(true)
            .build();

        let task_sub2 = FileScanTask {
            file_size_in_bytes: file_size,
            start: 0,
            length: 0,
            record_count: None,
            first_row_id: None,
            data_sequence_number: None,
            data_file_path: file_path.clone(),
            data_file_format: DataFileFormat::Parquet,
            schema: iceberg_schema.clone(),
            project_field_ids: vec![1, 2],
            predicate: Some(predicate_sub2),
            deletes: vec![FileScanTaskDeleteFile {
                file_path: pos_del_path.clone(),
                file_type: DataContentType::PositionDeletes,
                partition_spec_id: 0,
                equality_ids: None,
                file_size_in_bytes: std::fs::metadata(&pos_del_path).unwrap().len(),
                key_metadata: None,
            }],
            partition: None,
            partition_spec: None,
            name_mapping: None,
            unified_partition_type: None,
            case_sensitive: false,
            key_metadata: None,
        };

        let stream_sub2 =
            Box::pin(futures::stream::iter(vec![Ok(task_sub2)])) as FileScanTaskStream;
        let batches_sub2: Vec<RecordBatch> = reader_sub2
            .read(stream_sub2)
            .unwrap()
            .stream()
            .try_collect()
            .await
            .unwrap();

        let ids_sub2: Vec<i32> = batches_sub2
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();

        assert_eq!(
            ids_sub2,
            vec![2, 4, 5],
            "positional deletes must be applied correctly even when page indexes are absent"
        );
    }

    fn eqd_field(name: &str, dt: DataType, id: i32, nullable: bool) -> Field {
        Field::new(name, dt, nullable).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            id.to_string(),
        )]))
    }

    fn eqd_write(path: &str, schema: Arc<ArrowSchema>, columns: Vec<ArrayRef>) {
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let file = File::create(path).unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn eqd_delete_file(path: &str, equality_ids: Vec<i32>) -> FileScanTaskDeleteFile {
        FileScanTaskDeleteFile::builder()
            .with_file_path(path.to_string())
            .with_file_size_in_bytes(std::fs::metadata(path).unwrap().len())
            .with_file_type(DataContentType::EqualityDeletes)
            .with_equality_ids(Some(equality_ids))
            .with_partition_spec_id(0)
            .build()
    }

    async fn eqd_read(
        data_path: &str,
        table_schema: SchemaRef,
        project_field_ids: Vec<i32>,
        deletes: Vec<FileScanTaskDeleteFile>,
    ) -> Vec<RecordBatch> {
        eqd_read_with(
            data_path,
            table_schema,
            project_field_ids,
            deletes,
            None,
            false,
        )
        .await
    }

    /// `eqd_read` plus the two things that interact with equality-delete predicates: a
    /// scan predicate (pushed as a second `ArrowPredicate`) and the row-selection path.
    async fn eqd_read_with(
        data_path: &str,
        table_schema: SchemaRef,
        project_field_ids: Vec<i32>,
        deletes: Vec<FileScanTaskDeleteFile>,
        predicate: Option<BoundPredicate>,
        row_selection_enabled: bool,
    ) -> Vec<RecordBatch> {
        let file_io = FileIO::new_with_fs();
        let reader = ArrowReaderBuilder::new(file_io, Runtime::current())
            .with_row_selection_enabled(row_selection_enabled)
            .build();
        let task = FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(data_path).unwrap().len())
            .with_start(0)
            .with_length(0)
            .with_data_file_path(data_path.to_string())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(table_schema)
            .with_project_field_ids(project_field_ids)
            .with_deletes(deletes)
            .with_case_sensitive(false)
            .with_predicate(predicate)
            .build();
        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap()
    }

    fn eqd_collect_i64(batches: &[RecordBatch], col: usize) -> Vec<i64> {
        batches
            .iter()
            .flat_map(|b| {
                b.column(col)
                    .as_primitive::<arrow_array::types::Int64Type>()
                    .iter()
                    .flatten()
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    // A single-column equality delete removes exactly the matching rows.
    #[tokio::test]
    async fn test_eq_delete_single_column_filters_matching_rows() {
        let tmp = TempDir::new().unwrap();
        let loc = tmp.path().to_str().unwrap();

        let table_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(2, "val", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap(),
        );

        let data_path = format!("{loc}/data.parquet");
        eqd_write(
            &data_path,
            Arc::new(ArrowSchema::new(vec![
                eqd_field("id", DataType::Int64, 1, false),
                eqd_field("val", DataType::Utf8, 2, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])) as ArrayRef,
            ],
        );

        let del_path = format!("{loc}/eq-del.parquet");
        eqd_write(
            &del_path,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "id",
                DataType::Int64,
                1,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![3])) as ArrayRef],
        );

        let result = eqd_read(&data_path, table_schema, vec![1, 2], vec![eqd_delete_file(
            &del_path,
            vec![1],
        )])
        .await;

        assert_eq!(eqd_collect_i64(&result, 0), vec![1, 2, 4, 5]);
    }

    // A multi-column key deletes only exact tuple matches and a row with a null in a key column
    // (and a row differing in any column) is kept.
    #[tokio::test]
    async fn test_eq_delete_multi_column_keeps_null_and_partial_matches() {
        let tmp = TempDir::new().unwrap();
        let loc = tmp.path().to_str().unwrap();

        let table_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(2, "status", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])
                .build()
                .unwrap(),
        );

        let data_path = format!("{loc}/data.parquet");
        eqd_write(
            &data_path,
            Arc::new(ArrowSchema::new(vec![
                eqd_field("id", DataType::Int64, 1, false),
                eqd_field("status", DataType::Utf8, 2, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![2, 2, 2, 3])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("X"),
                    Some("Y"),
                    None,
                    Some("X"),
                ])) as ArrayRef,
            ],
        );

        let del_path = format!("{loc}/eq-del.parquet");
        eqd_write(
            &del_path,
            Arc::new(ArrowSchema::new(vec![
                eqd_field("id", DataType::Int64, 1, false),
                eqd_field("status", DataType::Utf8, 2, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![2])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("X")])) as ArrayRef,
            ],
        );

        let result = eqd_read(&data_path, table_schema, vec![1, 2], vec![eqd_delete_file(
            &del_path,
            vec![1, 2],
        )])
        .await;

        let mut pairs = Vec::new();
        for b in &result {
            let ids: Vec<i64> = b
                .column(0)
                .as_primitive::<arrow_array::types::Int64Type>()
                .iter()
                .flatten()
                .collect();
            let statuses: Vec<Option<String>> = b
                .column(1)
                .as_string::<i32>()
                .iter()
                .map(|o| o.map(|s| s.to_string()))
                .collect();
            for (id, status) in ids.into_iter().zip(statuses) {
                pairs.push((id, status));
            }
        }

        assert_eq!(pairs, vec![
            (2, Some("Y".to_string())),
            (2, None),
            (3, Some("X".to_string())),
        ]);
    }

    // The data file stored `id` as int32 but the table type is long, so it must be promoted
    // to the table type.
    #[tokio::test]
    async fn test_eq_delete_promotes_data_type_before_probe() {
        let tmp = TempDir::new().unwrap();
        let loc = tmp.path().to_str().unwrap();

        let table_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .unwrap(),
        );

        let data_path = format!("{loc}/data.parquet");
        eqd_write(
            &data_path,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "id",
                DataType::Int32,
                1,
                false,
            )])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef],
        );

        let del_path = format!("{loc}/eq-del.parquet");
        eqd_write(
            &del_path,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "id",
                DataType::Int64,
                1,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![3])) as ArrayRef],
        );

        let result = eqd_read(&data_path, table_schema, vec![1], vec![eqd_delete_file(
            &del_path,
            vec![1],
        )])
        .await;

        assert_eq!(eqd_collect_i64(&result, 0), vec![1, 2]);
    }

    // Two delete files that key on different columns each apply independently: a row is deleted
    // if it matches either.
    #[tokio::test]
    async fn test_eq_delete_distinct_layouts_apply_independently() {
        let tmp = TempDir::new().unwrap();
        let loc = tmp.path().to_str().unwrap();

        let table_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::required(2, "id2", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(3, "val", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap(),
        );

        let data_path = format!("{loc}/data.parquet");
        eqd_write(
            &data_path,
            Arc::new(ArrowSchema::new(vec![
                eqd_field("id", DataType::Int64, 1, false),
                eqd_field("id2", DataType::Int64, 2, false),
                eqd_field("val", DataType::Utf8, 3, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(Int64Array::from(vec![10, 20, 30])) as ArrayRef,
                Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
            ],
        );

        let del_a = format!("{loc}/eq-del-a.parquet");
        eqd_write(
            &del_a,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "id",
                DataType::Int64,
                1,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
        );
        let del_b = format!("{loc}/eq-del-b.parquet");
        eqd_write(
            &del_b,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "id2",
                DataType::Int64,
                2,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![20])) as ArrayRef],
        );

        let result = eqd_read(&data_path, table_schema, vec![1, 2, 3], vec![
            eqd_delete_file(&del_a, vec![1]),
            eqd_delete_file(&del_b, vec![2]),
        ])
        .await;

        assert_eq!(eqd_collect_i64(&result, 0), vec![3]);
    }

    // A scan predicate and an equality delete are pushed as two separate `ArrowPredicate`s
    // (they used to be ANDed into one bound `Predicate`), so the result must be the
    // intersection of both.
    #[tokio::test]
    async fn test_eq_delete_with_scan_predicate_intersects() {
        let tmp = TempDir::new().unwrap();
        let loc = tmp.path().to_str().unwrap();

        let table_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .unwrap(),
        );

        let data_path = format!("{loc}/data.parquet");
        eqd_write(
            &data_path,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "id",
                DataType::Int64,
                1,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])) as ArrayRef],
        );

        let del_path = format!("{loc}/eq-del.parquet");
        eqd_write(
            &del_path,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "id",
                DataType::Int64,
                1,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![2, 5])) as ArrayRef],
        );

        for row_selection_enabled in [false, true] {
            let predicate = Reference::new("id")
                .greater_than_or_equal_to(Datum::long(3))
                .bind(table_schema.clone(), false)
                .unwrap();

            let result = eqd_read_with(
                &data_path,
                table_schema.clone(),
                vec![1],
                vec![eqd_delete_file(&del_path, vec![1])],
                Some(predicate),
                row_selection_enabled,
            )
            .await;

            assert_eq!(
                eqd_collect_i64(&result, 0),
                vec![3, 4, 6],
                "row_selection_enabled={row_selection_enabled}"
            );
        }
    }

    #[tokio::test]
    async fn test_pos_and_eq_delete_with_scan_predicate() {
        let tmp = TempDir::new().unwrap();
        let loc = tmp.path().to_str().unwrap();

        let table_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .unwrap(),
        );

        let data_path = format!("{loc}/data.parquet");
        eqd_write(
            &data_path,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "id",
                DataType::Int64,
                1,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])) as ArrayRef],
        );

        // Positional delete removes row 2 (id 3), equality delete removes id 5, and the
        // predicate keeps id >= 2. Each removes something the others do not.
        let pos_del_path = format!("{loc}/pos-del.parquet");
        {
            let pos_schema = crate::arrow::delete_filter::tests::create_pos_del_schema();
            let batch = RecordBatch::try_new(pos_schema.clone(), vec![
                Arc::new(StringArray::from(vec![data_path.as_str()])) as ArrayRef,
                Arc::new(Int64Array::from(vec![2i64])) as ArrayRef,
            ])
            .unwrap();
            let file = File::create(&pos_del_path).unwrap();
            let props = WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build();
            let mut writer = ArrowWriter::try_new(file, pos_schema, Some(props)).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }

        let eq_del_path = format!("{loc}/eq-del.parquet");
        eqd_write(
            &eq_del_path,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "id",
                DataType::Int64,
                1,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![5])) as ArrayRef],
        );

        let pos_del = FileScanTaskDeleteFile::builder()
            .with_file_path(pos_del_path.clone())
            .with_file_size_in_bytes(std::fs::metadata(&pos_del_path).unwrap().len())
            .with_file_type(DataContentType::PositionDeletes)
            .with_partition_spec_id(0)
            .build();

        for row_selection_enabled in [false, true] {
            let predicate = Reference::new("id")
                .greater_than_or_equal_to(Datum::long(2))
                .bind(table_schema.clone(), false)
                .unwrap();

            let result = eqd_read_with(
                &data_path,
                table_schema.clone(),
                vec![1],
                vec![pos_del.clone(), eqd_delete_file(&eq_del_path, vec![1])],
                Some(predicate),
                row_selection_enabled,
            )
            .await;

            assert_eq!(
                eqd_collect_i64(&result, 0),
                vec![2, 4, 6],
                "row_selection_enabled={row_selection_enabled}"
            );
        }
    }

    // An equality-delete key column absent from the data file (added by later schema
    // evolution). The probe reads it as null for every row, so nothing matches and the
    // delete file removes no rows -- it does NOT resolve `initial_default`, which the spec
    // asks for via normal projection rules. Pinned here so the divergence is visible and a
    // later fix has something to change.
    #[tokio::test]
    async fn test_eq_delete_on_column_absent_from_data_file() {
        let tmp = TempDir::new().unwrap();
        let loc = tmp.path().to_str().unwrap();

        // `added` exists in the table schema but not in the data file below.
        let required_with_default = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::required(2, "added", Type::Primitive(PrimitiveType::Long))
                        .with_initial_default(Literal::long(7))
                        .into(),
                ])
                .build()
                .unwrap(),
        );
        let optional_no_default = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(2, "added", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .unwrap(),
        );

        let data_path = format!("{loc}/data.parquet");
        eqd_write(
            &data_path,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "id",
                DataType::Int64,
                1,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef],
        );

        // Keyed on `added` with the value that IS the initial default: if the probe ever
        // resolves defaults, every row matches and this expectation flips to empty.
        let del_path = format!("{loc}/eq-del.parquet");
        eqd_write(
            &del_path,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "added",
                DataType::Int64,
                2,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![7])) as ArrayRef],
        );

        for (label, schema) in [
            ("required with initial_default", required_with_default),
            ("optional without default", optional_no_default),
        ] {
            let result = eqd_read(&data_path, schema, vec![1], vec![eqd_delete_file(
                &del_path,
                vec![2],
            )])
            .await;

            assert_eq!(
                eqd_collect_i64(&result, 0),
                vec![1, 2, 3],
                "{label}: absent key column probes as null, so nothing is deleted"
            );
        }
    }

    #[tokio::test]
    async fn test_eq_delete_files_with_different_physical_types_merge() {
        let tmp = TempDir::new().unwrap();
        let loc = tmp.path().to_str().unwrap();

        let table_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .unwrap(),
        );

        let data_path = format!("{loc}/data.parquet");
        eqd_write(
            &data_path,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "id",
                DataType::Int64,
                1,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef],
        );

        let del_i64 = format!("{loc}/eq-del-i64.parquet");
        eqd_write(
            &del_i64,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "id",
                DataType::Int64,
                1,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![2])) as ArrayRef],
        );

        // Written as int32: an older file from before the column was widened to long.
        let del_i32 = format!("{loc}/eq-del-i32.parquet");
        eqd_write(
            &del_i32,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "id",
                DataType::Int32,
                1,
                false,
            )])),
            vec![Arc::new(Int32Array::from(vec![3])) as ArrayRef],
        );

        let result = eqd_read(&data_path, table_schema, vec![1], vec![
            eqd_delete_file(&del_i64, vec![1]),
            eqd_delete_file(&del_i32, vec![1]),
        ])
        .await;

        // Both keys apply, so the two files really did end up in one group.
        assert_eq!(eqd_collect_i64(&result, 0), vec![1, 4]);
    }

    // A decimal equality-delete key whose precision differs from the table's. Precision
    // widening is legal Iceberg schema evolution and goes through a different `Datum::to`
    // arm than the integer promotion already covered.
    #[tokio::test]
    async fn test_eq_delete_decimal_precision_widening() {
        let tmp = TempDir::new().unwrap();
        let loc = tmp.path().to_str().unwrap();

        let table_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::required(
                        2,
                        "amount",
                        Type::Primitive(PrimitiveType::Decimal {
                            precision: 18,
                            scale: 2,
                        }),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        // Data file written at the narrower precision it had when it was created.
        let data_path = format!("{loc}/data.parquet");
        eqd_write(
            &data_path,
            Arc::new(ArrowSchema::new(vec![
                eqd_field("id", DataType::Int64, 1, false),
                eqd_field("amount", DataType::Decimal128(9, 2), 2, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(
                    Decimal128Array::from(vec![100i128, 250, 375])
                        .with_precision_and_scale(9, 2)
                        .unwrap(),
                ) as ArrayRef,
            ],
        );

        let del_path = format!("{loc}/eq-del.parquet");
        eqd_write(
            &del_path,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "amount",
                DataType::Decimal128(18, 2),
                2,
                false,
            )])),
            vec![Arc::new(
                Decimal128Array::from(vec![250i128])
                    .with_precision_and_scale(18, 2)
                    .unwrap(),
            ) as ArrayRef],
        );

        let result = eqd_read(&data_path, table_schema, vec![1, 2], vec![eqd_delete_file(
            &del_path,
            vec![2],
        )])
        .await;

        assert_eq!(eqd_collect_i64(&result, 0), vec![1, 3]);
    }

    // Double delete columns are out of spec. Nothing rejects them today, so this
    // pins what actually happens: `PrimitiveLiteral` stores doubles as
    // `OrderedFloat`, whose `Hash`/`Eq` canonicalise NaN and signed zero.
    // A NaN key therefore matches NaN data, and a -0.0 key also removes 0.0.
    #[tokio::test]
    async fn test_eq_delete_double_column_canonicalises_nan_and_signed_zero() {
        let tmp = TempDir::new().unwrap();
        let loc = tmp.path().to_str().unwrap();

        let table_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::required(2, "d", Type::Primitive(PrimitiveType::Double)).into(),
                ])
                .build()
                .unwrap(),
        );

        let data_path = format!("{loc}/data.parquet");
        eqd_write(
            &data_path,
            Arc::new(ArrowSchema::new(vec![
                eqd_field("id", DataType::Int64, 1, false),
                eqd_field("d", DataType::Float64, 2, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
                Arc::new(Float64Array::from(vec![1.5, f64::NAN, 0.0, -0.0, 2.5])) as ArrayRef,
            ],
        );

        // A NaN key matches the NaN row (IEEE-754 says NaN != NaN; OrderedFloat says
        // otherwise), and a -0.0 key removes both signed zeros.
        let del_path = format!("{loc}/eq-del.parquet");
        eqd_write(
            &del_path,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "d",
                DataType::Float64,
                2,
                false,
            )])),
            vec![Arc::new(Float64Array::from(vec![f64::NAN, -0.0])) as ArrayRef],
        );

        let result = eqd_read(&data_path, table_schema, vec![1, 2], vec![eqd_delete_file(
            &del_path,
            vec![2],
        )])
        .await;

        assert_eq!(eqd_collect_i64(&result, 0), vec![1, 5]);
    }

    #[tokio::test]
    async fn test_empty_eq_delete_file_deletes_nothing() {
        let tmp = TempDir::new().unwrap();
        let loc = tmp.path().to_str().unwrap();

        let table_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .unwrap(),
        );

        let data_path = format!("{loc}/data.parquet");
        eqd_write(
            &data_path,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "id",
                DataType::Int64,
                1,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef],
        );

        let del_path = format!("{loc}/eq-del-empty.parquet");
        eqd_write(
            &del_path,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "id",
                DataType::Int64,
                1,
                false,
            )])),
            vec![Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef],
        );

        let result = eqd_read(&data_path, table_schema, vec![1], vec![eqd_delete_file(
            &del_path,
            vec![1],
        )])
        .await;

        assert_eq!(eqd_collect_i64(&result, 0), vec![1, 2, 3]);
    }
}
