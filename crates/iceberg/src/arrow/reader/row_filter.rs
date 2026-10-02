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

use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowPredicateFn, RowFilter, RowSelection};
use parquet::file::metadata::ParquetMetaData;
use parquet::schema::types::SchemaDescriptor;

use super::{ArrowReader, PredicateConverter};
use crate::error::Result;
use crate::expr::BoundPredicate;
use crate::expr::visitors::bound_predicate_visitor::visit;
use crate::expr::visitors::page_index_evaluator::PageIndexEvaluator;
use crate::expr::visitors::row_group_metrics_evaluator::RowGroupMetricsEvaluator;
use crate::spec::Schema;

impl ArrowReader {
    pub(super) fn get_row_filter(
        predicates: &BoundPredicate,
        parquet_schema: &SchemaDescriptor,
        iceberg_field_ids: &HashSet<i32>,
        field_id_map: &HashMap<i32, usize>,
    ) -> Result<RowFilter> {
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
        let arrow_predicate = ArrowPredicateFn::new(projection_mask, predicate_func);
        Ok(RowFilter::new(vec![Box::new(arrow_predicate)]))
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
        ArrayRef, Decimal128Array, Float32Array, Int32Array, Int64Array, LargeStringArray,
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
        DataContentType, DataFileFormat, Datum, NestedField, PrimitiveType, Schema, SchemaRef, Type,
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
                .build()
                .unwrap();
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
            .build()
            .unwrap();

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
            .build()
            .unwrap();

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
                .build()
                .unwrap();

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
                .build()
                .unwrap();

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
        let task = FileScanTask::builder()
            .with_file_size_in_bytes(file_size)
            .with_start(0)
            .with_length(0)
            .with_data_file_path(file_path.clone())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(iceberg_schema.clone())
            .with_project_field_ids(vec![1, 2])
            .with_predicate(Some(predicate))
            .with_case_sensitive(false)
            .build()
            .unwrap();

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

        let task_sub2 = FileScanTask::builder()
            .with_file_size_in_bytes(file_size)
            .with_start(0)
            .with_length(0)
            .with_data_file_path(file_path.clone())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(iceberg_schema.clone())
            .with_project_field_ids(vec![1, 2])
            .with_predicate(Some(predicate_sub2))
            .with_deletes(vec![FileScanTaskDeleteFile {
                file_path: pos_del_path.clone(),
                file_type: DataContentType::PositionDeletes,
                file_format: DataFileFormat::Parquet,
                partition_spec_id: 0,
                equality_ids: None,
                file_size_in_bytes: std::fs::metadata(&pos_del_path).unwrap().len(),
                referenced_data_file: None,
                content_offset: None,
                content_size_in_bytes: None,
                record_count: None,
                key_metadata: None,
            }])
            .with_case_sensitive(false)
            .build()
            .unwrap();

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

    /// End-to-end regression for issue #2432: a predicate on a primitive leaf nested in a
    /// struct (`person.age > 25`) must build a row filter and prune rows, rather than
    /// failing because the leaf's Parquet column root is a group. Reads a real Parquet
    /// file so the projected `RecordBatch` shape (a `StructArray` holding the leaf) comes
    /// from arrow-rs, not a hand-built batch.
    #[tokio::test]
    async fn test_predicate_on_nested_struct_leaf_reads_real_parquet() {
        use arrow_array::StructArray;
        use arrow_schema::Fields;

        use crate::spec::StructType;

        // Schema: id: int (1), person: struct<age: int (3)> (2)
        let iceberg_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(
                        2,
                        "person",
                        Type::Struct(StructType::new(vec![
                            NestedField::required(3, "age", Type::Primitive(PrimitiveType::Int))
                                .into(),
                        ])),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        // Arrow schema carrying field ids so the reader resolves by id, not position.
        let age_field = field_with_id("age", DataType::Int32, 3);
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            field_with_id("id", DataType::Int32, 1),
            field_with_id(
                "person",
                DataType::Struct(Fields::from(vec![age_field.clone()])),
                2,
            ),
        ]));

        let id = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let person = Arc::new(StructArray::from(vec![(
            Arc::new(age_field),
            Arc::new(Int32Array::from(vec![30, 20, 40])) as ArrayRef,
        )])) as ArrayRef;

        // person.age > 25 keeps id=1 (30) and id=3 (40); id=2 (20) is pruned.
        let ids = ids_kept_by_predicate(
            iceberg_schema,
            arrow_schema,
            vec![id, person],
            Reference::new("person.age").greater_than(Datum::int(25)),
        )
        .await;
        assert_eq!(ids, vec![1, 3]);
    }

    /// A predicate on a field nested two structs deep (`person.address.zip`) must resolve
    /// through both struct levels. This exercises `project_column`'s descent loop more than
    /// once, which the single-level `person.age` case does not. Regression for issue #2432.
    #[tokio::test]
    async fn test_predicate_on_doubly_nested_struct_leaf() {
        use arrow_array::StructArray;
        use arrow_schema::Fields;

        use crate::spec::StructType;

        // Schema: id: int (1), person: struct<address: struct<zip: int (4)> (3)> (2)
        let iceberg_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(
                        2,
                        "person",
                        Type::Struct(StructType::new(vec![
                            NestedField::required(
                                3,
                                "address",
                                Type::Struct(StructType::new(vec![
                                    NestedField::required(
                                        4,
                                        "zip",
                                        Type::Primitive(PrimitiveType::Int),
                                    )
                                    .into(),
                                ])),
                            )
                            .into(),
                        ])),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        let zip_field = field_with_id("zip", DataType::Int32, 4);
        let address_field = field_with_id(
            "address",
            DataType::Struct(Fields::from(vec![zip_field.clone()])),
            3,
        );
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            field_with_id("id", DataType::Int32, 1),
            field_with_id(
                "person",
                DataType::Struct(Fields::from(vec![address_field.clone()])),
                2,
            ),
        ]));

        let id = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let address = Arc::new(StructArray::from(vec![(
            Arc::new(zip_field),
            Arc::new(Int32Array::from(vec![10001, 20002, 30003])) as ArrayRef,
        )])) as ArrayRef;
        let person =
            Arc::new(StructArray::from(vec![(Arc::new(address_field), address)])) as ArrayRef;

        // person.address.zip >= 20002 keeps id=2 (20002) and id=3 (30003).
        let ids = ids_kept_by_predicate(
            iceberg_schema,
            arrow_schema,
            vec![id, person],
            Reference::new("person.address.zip").greater_than_or_equal_to(Datum::int(20002)),
        )
        .await;
        assert_eq!(ids, vec![2, 3]);
    }

    /// Writes `columns` as one Parquet file, pushes `predicate` down while reading it back,
    /// and returns the surviving rows' `id`s (the file's first column). `predicate` binds
    /// against `iceberg_schema`. Shared write-then-read scaffolding for the nested-leaf tests.
    async fn ids_kept_by_predicate(
        iceberg_schema: SchemaRef,
        arrow_schema: Arc<ArrowSchema>,
        columns: Vec<ArrayRef>,
        predicate: Predicate,
    ) -> Vec<i32> {
        let tmp_dir = TempDir::new().unwrap();
        let file_path = format!("{}/1.parquet", tmp_dir.path().to_str().unwrap());
        let batch = RecordBatch::try_new(arrow_schema.clone(), columns).unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let file = File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let predicate = predicate.bind(iceberg_schema.clone(), false).unwrap();
        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current()).build();
        let task = FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(&file_path).unwrap().len())
            .with_start(0)
            .with_length(0)
            .with_data_file_path(file_path)
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(iceberg_schema)
            .with_project_field_ids(vec![1])
            .with_predicate(Some(predicate))
            .with_case_sensitive(false)
            .build()
            .unwrap();

        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let batches = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();
        batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_primitive::<arrow_array::types::Int32Type>()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    /// A null parent struct implies null leaves (spec: "if a parent struct column is null it
    /// implies the leaf column is null"). With `person` optional and a null row, `project_column`
    /// must carry the parent's validity into the child, so a required leaf reads as null there.
    /// Before the `flatten` fix these predicates over-returned the null row.
    #[tokio::test]
    async fn test_predicate_on_leaf_under_null_parent_struct() {
        use arrow_array::StructArray;
        use arrow_buffer::NullBuffer;
        use arrow_schema::Fields;

        use crate::spec::StructType;

        // id: int (1), person: optional struct<age: required int (3), score: int (4)> (2)
        let iceberg_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(
                        2,
                        "person",
                        Type::Struct(StructType::new(vec![
                            NestedField::required(3, "age", Type::Primitive(PrimitiveType::Int))
                                .into(),
                            NestedField::optional(4, "score", Type::Primitive(PrimitiveType::Int))
                                .into(),
                        ])),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let age_field = field_with_id("age", DataType::Int32, 3);
        let score_field = Field::new("score", DataType::Int32, true).with_metadata(HashMap::from(
            [(PARQUET_FIELD_ID_META_KEY.to_string(), "4".to_string())],
        ));
        let person_field = Field::new(
            "person",
            DataType::Struct(Fields::from(vec![age_field.clone(), score_field.clone()])),
            true,
        )
        .with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "2".to_string(),
        )]));
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            field_with_id("id", DataType::Int32, 1),
            person_field,
        ]));

        // Row id=2 has a null `person`; its stored age/score are placeholders to be masked.
        let id = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let person = Arc::new(StructArray::new(
            Fields::from(vec![age_field, score_field]),
            vec![
                Arc::new(Int32Array::from(vec![30, 0, 40])) as ArrayRef,
                Arc::new(Int32Array::from(vec![Some(100), Some(0), Some(300)])) as ArrayRef,
            ],
            Some(NullBuffer::from(vec![true, false, true])),
        )) as ArrayRef;
        // person.age < 1000 holds for the stored 30/40 but the null row's age reads as null.
        let ids = ids_kept_by_predicate(
            iceberg_schema.clone(),
            arrow_schema.clone(),
            vec![id.clone(), person.clone()],
            Reference::new("person.age").less_than(Datum::int(1000)),
        )
        .await;
        assert_eq!(
            ids,
            vec![1, 3],
            "required leaf under a null parent must read as null"
        );

        // person.age != 30 keeps only id=3 (40); the null row is null, not "!= 30".
        let ids = ids_kept_by_predicate(
            iceberg_schema.clone(),
            arrow_schema.clone(),
            vec![id.clone(), person.clone()],
            Reference::new("person.age").not_equal_to(Datum::int(30)),
        )
        .await;
        assert_eq!(ids, vec![3]);

        // An optional leaf under the null parent reads as null too: person.score < 1000
        // holds for the stored 100/300 but not the null row.
        let ids = ids_kept_by_predicate(
            iceberg_schema,
            arrow_schema,
            vec![id, person],
            Reference::new("person.score").less_than(Datum::int(1000)),
        )
        .await;
        assert_eq!(
            ids,
            vec![1, 3],
            "optional leaf under a null parent must read as null"
        );
    }

    /// The parent-null rule holds through two struct levels: a null outer `person` makes
    /// `person.address.zip` null, since each level merges its own validity into its children.
    #[tokio::test]
    async fn test_predicate_on_leaf_under_null_outer_struct_doubly_nested() {
        use arrow_array::StructArray;
        use arrow_buffer::NullBuffer;
        use arrow_schema::Fields;

        use crate::spec::StructType;

        // id: int (1), person: optional struct<address: struct<zip: int (4)> (3)> (2)
        let iceberg_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(
                        2,
                        "person",
                        Type::Struct(StructType::new(vec![
                            NestedField::required(
                                3,
                                "address",
                                Type::Struct(StructType::new(vec![
                                    NestedField::required(
                                        4,
                                        "zip",
                                        Type::Primitive(PrimitiveType::Int),
                                    )
                                    .into(),
                                ])),
                            )
                            .into(),
                        ])),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let zip_field = field_with_id("zip", DataType::Int32, 4);
        let address_field = field_with_id(
            "address",
            DataType::Struct(Fields::from(vec![zip_field.clone()])),
            3,
        );
        let person_field = Field::new(
            "person",
            DataType::Struct(Fields::from(vec![address_field.clone()])),
            true,
        )
        .with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "2".to_string(),
        )]));
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            field_with_id("id", DataType::Int32, 1),
            person_field,
        ]));

        let id = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let address = Arc::new(StructArray::from(vec![(
            Arc::new(zip_field),
            Arc::new(Int32Array::from(vec![10001, 0, 30003])) as ArrayRef,
        )])) as ArrayRef;
        // Row id=2's `person` is null, so `address` and its `zip` under it read as null.
        let person = Arc::new(StructArray::new(
            Fields::from(vec![address_field]),
            vec![address],
            Some(NullBuffer::from(vec![true, false, true])),
        )) as ArrayRef;

        // person.address.zip < 100000 holds for the stored 10001/30003 but not the null row.
        let ids = ids_kept_by_predicate(
            iceberg_schema,
            arrow_schema,
            vec![id, person],
            Reference::new("person.address.zip").less_than(Datum::int(100000)),
        )
        .await;
        assert_eq!(ids, vec![1, 3]);
    }

    /// A predicate that references two leaves of the same struct
    /// (`person.age > 25 AND person.score < 250`) makes `ProjectionMask::leaves` project the
    /// struct with more than one child, so `project_column` must find each child by name and
    /// not by position. The other nested tests project a single child per struct, where the
    /// lookup always lands on index 0 and would still pass if the child were taken positionally.
    #[tokio::test]
    async fn test_predicate_on_two_leaves_of_same_struct() {
        use arrow_array::StructArray;
        use arrow_schema::Fields;

        use crate::spec::StructType;

        // id: int (1), person: optional struct<age: required int (3), score: optional int (4)> (2)
        let iceberg_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(
                        2,
                        "person",
                        Type::Struct(StructType::new(vec![
                            NestedField::required(3, "age", Type::Primitive(PrimitiveType::Int))
                                .into(),
                            NestedField::optional(4, "score", Type::Primitive(PrimitiveType::Int))
                                .into(),
                        ])),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let age_field = field_with_id("age", DataType::Int32, 3);
        let score_field = Field::new("score", DataType::Int32, true).with_metadata(HashMap::from(
            [(PARQUET_FIELD_ID_META_KEY.to_string(), "4".to_string())],
        ));
        let person_field = Field::new(
            "person",
            DataType::Struct(Fields::from(vec![age_field.clone(), score_field.clone()])),
            true,
        )
        .with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "2".to_string(),
        )]));
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            field_with_id("id", DataType::Int32, 1),
            person_field,
        ]));

        let id = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])) as ArrayRef;
        let person = Arc::new(StructArray::from(vec![
            (
                Arc::new(age_field),
                Arc::new(Int32Array::from(vec![30, 10, 40, 50, 20, 35])) as ArrayRef,
            ),
            (
                Arc::new(score_field),
                Arc::new(Int32Array::from(vec![100, 300, 300, 200, 50, 240])) as ArrayRef,
            ),
        ])) as ArrayRef;

        // age > 25 AND score < 250. If project_column took the child positionally instead of by
        // name, the score comparison would read `age` and keep id=3 too (age 40, score 300).
        let ids = ids_kept_by_predicate(
            iceberg_schema,
            arrow_schema,
            vec![id, person],
            Reference::new("person.age")
                .greater_than(Datum::int(25))
                .and(Reference::new("person.score").less_than(Datum::int(250))),
        )
        .await;
        assert_eq!(ids, vec![1, 4, 6]);
    }

    /// Fields inside a list or map have no accessor (see `Schema::build_accessors`), so a
    /// predicate referencing one fails at bind time and never reaches the row-filter
    /// conversion. This pins the assumption `project_column` relies on: every predicate
    /// path segment before the leaf is a struct, never a list/map interior.
    #[test]
    fn test_predicate_on_list_and_map_interior_fails_to_bind() {
        use crate::spec::{ListType, MapType, StructType};

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    // tags: list<struct<name: string (4)>> (2, element 3)
                    NestedField::required(
                        2,
                        "tags",
                        Type::List(ListType::new(
                            NestedField::required(
                                3,
                                "element",
                                Type::Struct(StructType::new(vec![
                                    NestedField::required(
                                        4,
                                        "name",
                                        Type::Primitive(PrimitiveType::String),
                                    )
                                    .into(),
                                ])),
                            )
                            .into(),
                        )),
                    )
                    .into(),
                    // props: map<string (6), int (7)> (5)
                    NestedField::required(
                        5,
                        "props",
                        Type::Map(MapType::new(
                            NestedField::required(6, "key", Type::Primitive(PrimitiveType::String))
                                .into(),
                            NestedField::required(7, "value", Type::Primitive(PrimitiveType::Int))
                                .into(),
                        )),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        // A field inside the list element and a map value: both must fail to bind.
        assert!(
            Reference::new("tags.element.name")
                .is_null()
                .bind(schema.clone(), false)
                .is_err(),
            "a predicate on a field inside a list must not bind"
        );
        assert!(
            Reference::new("props.value")
                .greater_than(Datum::int(0))
                .bind(schema.clone(), false)
                .is_err(),
            "a predicate on a map value must not bind"
        );
    }

    // Bloom filter pushdown: on-vs-off equivalence
    // Pushdown must never change results. An encoding bug in the probe shows up as
    // rows the bloom filter drops and the row filter keeps, so every case below
    // reads the same file twice and compares. Fixtures spread each row group's
    // values across the whole domain, so min/max statistics cannot prune and any
    // reduction in bytes read is attributable to the bloom filter.

    const ROWS_PER_GROUP: i32 = 20_000;
    const GROUPS: i32 = 3;
    /// Gap between consecutive values in a row group, so a value can be absent from
    /// the file yet still sit inside every row group's min/max range.
    const STRIDE: i32 = 30;

    /// The `i`th value of row group `group`, spread across the whole domain.
    fn interleaved(group: i32, i: i32) -> i32 {
        i * STRIDE + group
    }

    fn field_with_id(name: &str, ty: DataType, id: i32) -> Field {
        Field::new(name, ty, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            id.to_string(),
        )]))
    }

    /// Writes one row group per batch, optionally with bloom filters.
    fn write_row_groups(
        path: &str,
        arrow_schema: Arc<ArrowSchema>,
        row_groups: Vec<RecordBatch>,
        with_bloom_filters: bool,
    ) {
        let mut props = WriterProperties::builder().set_compression(Compression::UNCOMPRESSED);
        if with_bloom_filters {
            props = props
                .set_bloom_filter_enabled(true)
                .set_bloom_filter_max_ndv(ROWS_PER_GROUP as u64);
        }

        let file = File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema, Some(props.build())).unwrap();
        for batch in row_groups {
            writer.write(&batch).unwrap();
            // Force a row group boundary so each batch is independently prunable.
            writer.flush().unwrap();
        }
        writer.close().unwrap();
    }

    async fn read_once(
        file_path: &str,
        schema: Arc<Schema>,
        project_field_ids: Vec<i32>,
        predicate: Predicate,
        bloom_enabled: bool,
    ) -> (Vec<RecordBatch>, u64) {
        let file_io = FileIO::new_with_fs();
        let reader = ArrowReaderBuilder::new(file_io, Runtime::current())
            .with_bloom_filter_enabled(bloom_enabled)
            // Keep the fixed footer prefetch small so the byte measurement reflects
            // row group I/O rather than a constant metadata read.
            .with_metadata_size_hint(8 * 1024)
            .build();

        let task = FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(file_path).unwrap().len())
            .with_start(0)
            .with_length(0)
            .with_data_file_path(file_path.to_string())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema.clone())
            .with_project_field_ids(project_field_ids)
            .with_predicate(Some(predicate.bind(schema, true).unwrap()))
            .with_case_sensitive(false)
            .build()
            .unwrap();

        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let result = reader.read(tasks).unwrap();
        let metrics = result.metrics().clone();
        let batches: Vec<RecordBatch> = result.stream().try_collect().await.unwrap();

        (batches, metrics.bytes_read())
    }

    /// Batch boundaries differ when fewer row groups are read, so collapse to a
    /// single batch before comparing. `None` means no rows at all.
    fn collapse(batches: &[RecordBatch]) -> Option<RecordBatch> {
        let first = batches.first()?;
        Some(arrow_select::concat::concat_batches(&first.schema(), batches).unwrap())
    }

    /// Reads the file with pushdown on and off, asserts the rows are identical, and
    /// returns the rows plus (bytes_on, bytes_off).
    async fn assert_pushdown_agrees(
        case: &str,
        file_path: &str,
        schema: Arc<Schema>,
        project_field_ids: Vec<i32>,
        predicate: Predicate,
    ) -> (Option<RecordBatch>, u64, u64) {
        let (off, bytes_off) = read_once(
            file_path,
            schema.clone(),
            project_field_ids.clone(),
            predicate.clone(),
            false,
        )
        .await;
        let (on, bytes_on) = read_once(file_path, schema, project_field_ids, predicate, true).await;

        let off = collapse(&off);
        let on = collapse(&on);
        assert_eq!(
            on, off,
            "{case}: bloom filter pushdown changed the rows returned"
        );

        (on, bytes_on, bytes_off)
    }

    fn rows(batch: &Option<RecordBatch>) -> usize {
        batch.as_ref().map_or(0, |b| b.num_rows())
    }

    fn value_schema(iceberg_type: Type) -> Arc<Schema> {
        Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![NestedField::required(1, "v", iceberg_type).into()])
                .build()
                .unwrap(),
        )
    }

    /// Writes a single-column file, one row group per group index.
    fn write_value_fixture(
        path: &str,
        data_type: DataType,
        make_group: impl Fn(i32) -> ArrayRef,
        with_bloom_filters: bool,
    ) {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![field_with_id("v", data_type, 1)]));
        let row_groups = (0..GROUPS)
            .map(|g| RecordBatch::try_new(arrow_schema.clone(), vec![make_group(g)]).unwrap())
            .collect();
        write_row_groups(path, arrow_schema, row_groups, with_bloom_filters);
    }

    /// The core check for one physical encoding: a value that is present must
    /// survive pushdown, and one that is absent must prune every row group. The
    /// first catches a probe that encodes wrongly and drops real rows; the second
    /// catches a probe that silently never prunes.
    async fn assert_prunes_and_agrees(
        case: &str,
        file_path: &str,
        schema: Arc<Schema>,
        present: Datum,
        absent: Datum,
    ) {
        let (on, bytes_on, bytes_off) = assert_pushdown_agrees(
            &format!("{case}/present"),
            file_path,
            schema.clone(),
            vec![1],
            Reference::new("v").equal_to(present),
        )
        .await;
        assert_eq!(rows(&on), 1, "{case}/present: expected one matching row");
        assert!(
            bytes_on < bytes_off,
            "{case}/present: pushdown must skip row groups: {bytes_on} vs {bytes_off}"
        );

        let (on, bytes_on, bytes_off) = assert_pushdown_agrees(
            &format!("{case}/absent"),
            file_path,
            schema,
            vec![1],
            Reference::new("v").equal_to(absent),
        )
        .await;
        assert_eq!(rows(&on), 0, "{case}/absent: expected no matching rows");
        assert!(
            bytes_on * 2 < bytes_off,
            "{case}/absent: pruning every row group should cut reads sharply: \
             {bytes_on} vs {bytes_off}"
        );
    }

    /// Value present in exactly one row group: `interleaved(1, 51)`.
    const PRESENT_INT: i32 = 51 * STRIDE + 1;
    /// Not congruent to any group index mod STRIDE, so absent from the file while
    /// still inside every row group's min/max range.
    const ABSENT_INT: i32 = 7;

    fn int_group(g: i32) -> ArrayRef {
        Arc::new(Int32Array::from(
            (0..ROWS_PER_GROUP)
                .map(|i| interleaved(g, i))
                .collect::<Vec<_>>(),
        ))
    }

    #[tokio::test]
    async fn test_bloom_pushdown_int32_eq() {
        let tmp = TempDir::new().unwrap();
        let path = format!("{}/int32.parquet", tmp.path().to_str().unwrap());
        write_value_fixture(&path, DataType::Int32, int_group, true);

        assert_prunes_and_agrees(
            "int32",
            &path,
            value_schema(Type::Primitive(PrimitiveType::Int)),
            Datum::int(PRESENT_INT),
            Datum::int(ABSENT_INT),
        )
        .await;
    }

    /// `IN` takes a different evaluator path from `eq`: it must keep the row group
    /// when any literal might be present, and prune only when all are absent.
    #[tokio::test]
    async fn test_bloom_pushdown_int32_in() {
        let tmp = TempDir::new().unwrap();
        let path = format!("{}/int32_in.parquet", tmp.path().to_str().unwrap());
        write_value_fixture(&path, DataType::Int32, int_group, true);
        let schema = value_schema(Type::Primitive(PrimitiveType::Int));

        // One present literal keeps its row group; the absent one must not suppress it.
        let (on, bytes_on, bytes_off) = assert_pushdown_agrees(
            "int32_in/mixed",
            &path,
            schema.clone(),
            vec![1],
            Reference::new("v").is_in([Datum::int(PRESENT_INT), Datum::int(ABSENT_INT)]),
        )
        .await;
        assert_eq!(rows(&on), 1);
        assert!(bytes_on < bytes_off, "{bytes_on} vs {bytes_off}");

        // All literals absent: every row group prunes.
        let (on, bytes_on, bytes_off) = assert_pushdown_agrees(
            "int32_in/all_absent",
            &path,
            schema,
            vec![1],
            Reference::new("v").is_in([Datum::int(ABSENT_INT), Datum::int(ABSENT_INT + 1)]),
        )
        .await;
        assert_eq!(rows(&on), 0);
        assert!(bytes_on * 2 < bytes_off, "{bytes_on} vs {bytes_off}");
    }

    #[tokio::test]
    async fn test_bloom_pushdown_string_eq() {
        let tmp = TempDir::new().unwrap();
        let path = format!("{}/string.parquet", tmp.path().to_str().unwrap());
        write_value_fixture(
            &path,
            DataType::Utf8,
            |g| {
                Arc::new(StringArray::from(
                    (0..ROWS_PER_GROUP)
                        .map(|i| format!("{:016}", interleaved(g, i)))
                        .collect::<Vec<_>>(),
                ))
            },
            true,
        );

        assert_prunes_and_agrees(
            "string",
            &path,
            value_schema(Type::Primitive(PrimitiveType::String)),
            Datum::string(format!("{PRESENT_INT:016}")),
            Datum::string(format!("{ABSENT_INT:016}")),
        )
        .await;
    }

    /// Decimal precision decides the Parquet physical type, and the probe has to be
    /// encoded to match: precision 9 -> INT32, 15 -> INT64, 25 -> FIXED_LEN_BYTE_ARRAY.
    /// Each width is a separate arm of `check_in_bloom_filter`.
    async fn check_decimal_width(name: &str, precision: u32) {
        const SCALE: u32 = 2;
        let tmp = TempDir::new().unwrap();
        let path = format!("{}/{name}.parquet", tmp.path().to_str().unwrap());

        write_value_fixture(
            &path,
            DataType::Decimal128(precision as u8, SCALE as i8),
            |g| {
                let values: Vec<i128> = (0..ROWS_PER_GROUP)
                    .map(|i| i128::from(interleaved(g, i)))
                    .collect();
                Arc::new(
                    Decimal128Array::from(values)
                        .with_precision_and_scale(precision as u8, SCALE as i8)
                        .unwrap(),
                )
            },
            true,
        );

        let datum = |mantissa: i32| {
            Datum::decimal_with_precision(
                crate::spec::decimal_utils::decimal_from_i128_with_scale(
                    i128::from(mantissa),
                    SCALE,
                ),
                precision,
            )
            .unwrap()
        };

        assert_prunes_and_agrees(
            name,
            &path,
            value_schema(Type::Primitive(PrimitiveType::Decimal {
                precision,
                scale: SCALE,
            })),
            datum(PRESENT_INT),
            datum(ABSENT_INT),
        )
        .await;
    }

    #[tokio::test]
    async fn test_bloom_pushdown_decimal_int32() {
        check_decimal_width("decimal_int32", 9).await;
    }

    #[tokio::test]
    async fn test_bloom_pushdown_decimal_int64() {
        check_decimal_width("decimal_int64", 15).await;
    }

    #[tokio::test]
    async fn test_bloom_pushdown_decimal_fixed_len_byte_array() {
        check_decimal_width("decimal_flba", 25).await;
    }

    /// Widening a decimal's precision does not rewrite existing files, so the file
    /// keeps its original physical width while the bound literal arrives at the new
    /// precision. Deriving the probe encoding from the schema instead of the file
    /// would miss every entry the writer inserted and prune matching row groups.
    #[tokio::test]
    async fn test_bloom_pushdown_decimal_widened_precision() {
        const SCALE: u32 = 2;
        let tmp = TempDir::new().unwrap();
        let path = format!("{}/decimal_widened.parquet", tmp.path().to_str().unwrap());

        // File written at precision 9 -> INT32.
        write_value_fixture(
            &path,
            DataType::Decimal128(9, SCALE as i8),
            |g| {
                let values: Vec<i128> = (0..ROWS_PER_GROUP)
                    .map(|i| i128::from(interleaved(g, i)))
                    .collect();
                Arc::new(
                    Decimal128Array::from(values)
                        .with_precision_and_scale(9, SCALE as i8)
                        .unwrap(),
                )
            },
            true,
        );

        // Table schema has since widened to precision 30.
        let schema = value_schema(Type::Primitive(PrimitiveType::Decimal {
            precision: 30,
            scale: SCALE,
        }));
        let datum = |mantissa: i32| {
            Datum::decimal_with_precision(
                crate::spec::decimal_utils::decimal_from_i128_with_scale(
                    i128::from(mantissa),
                    SCALE,
                ),
                30,
            )
            .unwrap()
        };

        assert_prunes_and_agrees(
            "decimal_widened",
            &path,
            schema,
            datum(PRESENT_INT),
            datum(ABSENT_INT),
        )
        .await;
    }

    /// `Sbbf` hashes raw IEEE bytes, so `-0.0` and `0.0` are distinct entries. Arrow's
    /// float equality is bitwise too, for an independent reason: `ArrowNativeTypeOp::is_eq`
    /// implements total-order (`total_cmp`) semantics as `to_bits() == to_bits()`, so
    /// `-0.0 != 0.0` and `NaN == NaN` there. That agreement is what makes it safe to prune
    /// a row group holding only the opposite zero: if `is_eq` ever became IEEE-lenient,
    /// those pruned rows would be rows the row filter should have returned. A probe that
    /// canonicalized the sign instead only over-keeps, costing pruning rather than
    /// correctness — but it would also hide such a change from this test.
    ///
    /// Each row group pairs its zero with a larger filler value so its min/max range spans
    /// the other zero. Neither statistics path can prune it, so the bloom-off read really
    /// does evaluate `is_eq` against `ROWS_PER_GROUP - 1` copies of the opposite zero: that
    /// arm is the control pinning arrow's semantics, and IEEE-lenient equality would double
    /// the row count it returns. Pairing with a filler also avoids relying on the writer
    /// widening a single-value group's bounds to `[-0.0, +0.0]`.
    #[tokio::test]
    async fn test_bloom_pushdown_float_signed_zero() {
        const FILLER: f32 = 5.0;
        let tmp = TempDir::new().unwrap();
        let path = format!("{}/float_zero.parquet", tmp.path().to_str().unwrap());

        // Row group 0 holds -0.0 and never +0.0; row group 1 the reverse; row group 2
        // neither zero.
        write_value_fixture(
            &path,
            DataType::Float32,
            |g| {
                let zero = match g {
                    0 => -0.0f32,
                    1 => 0.0f32,
                    _ => 7.5f32,
                };
                let mut values = vec![zero; ROWS_PER_GROUP as usize - 1];
                values.push(FILLER);
                Arc::new(Float32Array::from(values))
            },
            true,
        );

        let schema = value_schema(Type::Primitive(PrimitiveType::Float));
        let zero_rows = ROWS_PER_GROUP as usize - 1;

        for (label, probe) in [("positive_zero", 0.0f32), ("negative_zero", -0.0f32)] {
            let (on, _, _) = assert_pushdown_agrees(
                label,
                &path,
                schema.clone(),
                vec![1],
                Reference::new("v").equal_to(Datum::float(probe)),
            )
            .await;
            // Only the group holding this exact zero matches, on both paths.
            assert_eq!(
                rows(&on),
                zero_rows,
                "{label}: expected only the row group holding this zero to match"
            );
        }
    }

    /// A file with no bloom filters at all must read identically with the option on,
    /// exercising the conservative might-match path end to end.
    #[tokio::test]
    async fn test_bloom_pushdown_file_without_bloom_filters() {
        let tmp = TempDir::new().unwrap();
        let path = format!("{}/no_filters.parquet", tmp.path().to_str().unwrap());
        write_value_fixture(&path, DataType::Int32, int_group, false);

        let (on, _, _) = assert_pushdown_agrees(
            "no_filters",
            &path,
            value_schema(Type::Primitive(PrimitiveType::Int)),
            vec![1],
            Reference::new("v").equal_to(Datum::int(PRESENT_INT)),
        )
        .await;
        assert_eq!(rows(&on), 1);
    }
}
