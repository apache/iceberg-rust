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

//! Positional delete handling for `ArrowReader`: converting a `DeleteVector`
//! into a Parquet `RowSelection` that skips the deleted rows, while respecting
//! any row-group selection made by the predicate evaluator.

use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use parquet::file::metadata::RowGroupMetaData;

use super::ArrowReader;
use crate::delete_vector::DeleteVector;
use crate::error::Result;

impl ArrowReader {
    /// computes a `RowSelection` from positional delete indices.
    ///
    /// Using the Parquet page index, we build a `RowSelection` that rejects rows that are indicated
    /// as having been deleted by a positional delete, taking into account any row groups that have
    /// been skipped entirely by the filter predicate
    pub(super) fn build_deletes_row_selection(
        row_group_metadata_list: &[RowGroupMetaData],
        selected_row_groups: &Option<Vec<usize>>,
        positional_deletes: &DeleteVector,
    ) -> Result<RowSelection> {
        let mut results: Vec<RowSelector> = Vec::new();
        let mut selected_row_groups_idx = 0;
        let mut current_row_group_base_idx: u64 = 0;
        let mut delete_vector_iter = positional_deletes.iter();
        let mut next_deleted_row_idx_opt = delete_vector_iter.next();

        for (idx, row_group_metadata) in row_group_metadata_list.iter().enumerate() {
            let row_group_num_rows = row_group_metadata.num_rows() as u64;
            let next_row_group_base_idx = current_row_group_base_idx + row_group_num_rows;

            // if row group selection is enabled,
            if let Some(selected_row_groups) = selected_row_groups {
                // if we've consumed all the selected row groups, we're done
                if selected_row_groups_idx == selected_row_groups.len() {
                    break;
                }

                if idx == selected_row_groups[selected_row_groups_idx] {
                    // we're in a selected row group. Increment selected_row_groups_idx
                    // so that next time around the for loop we're looking for the next
                    // selected row group
                    selected_row_groups_idx += 1;
                } else {
                    // Advance iterator past all deletes in the skipped row group.
                    // advance_to() positions the iterator to the first delete >= next_row_group_base_idx.
                    // However, if our cached next_deleted_row_idx_opt is in the skipped range,
                    // we need to call next() to update the cache with the newly positioned value.
                    delete_vector_iter.advance_to(next_row_group_base_idx);
                    // Only update the cache if the cached value is stale (in the skipped range)
                    if let Some(cached_idx) = next_deleted_row_idx_opt
                        && cached_idx < next_row_group_base_idx
                    {
                        next_deleted_row_idx_opt = delete_vector_iter.next();
                    }

                    // still increment the current page base index but then skip to the next row group
                    // in the file
                    current_row_group_base_idx += row_group_num_rows;
                    continue;
                }
            }

            let mut next_deleted_row_idx = match next_deleted_row_idx_opt {
                Some(next_deleted_row_idx) => {
                    // if the index of the next deleted row is beyond this row group, add a selection for
                    // the remainder of this row group and skip to the next row group
                    if next_deleted_row_idx >= next_row_group_base_idx {
                        results.push(RowSelector::select(row_group_num_rows as usize));
                        current_row_group_base_idx += row_group_num_rows;
                        continue;
                    }

                    next_deleted_row_idx
                }

                // If there are no more pos deletes, add a selector for the entirety of this row group.
                _ => {
                    results.push(RowSelector::select(row_group_num_rows as usize));
                    current_row_group_base_idx += row_group_num_rows;
                    continue;
                }
            };

            let mut current_idx = current_row_group_base_idx;
            'chunks: while next_deleted_row_idx < next_row_group_base_idx {
                // `select` all rows that precede the next delete index
                if current_idx < next_deleted_row_idx {
                    let run_length = next_deleted_row_idx - current_idx;
                    results.push(RowSelector::select(run_length as usize));
                    current_idx += run_length;
                }

                // `skip` all consecutive deleted rows in the current row group
                let mut run_length = 0;
                while next_deleted_row_idx == current_idx
                    && next_deleted_row_idx < next_row_group_base_idx
                {
                    run_length += 1;
                    current_idx += 1;

                    next_deleted_row_idx_opt = delete_vector_iter.next();
                    next_deleted_row_idx = match next_deleted_row_idx_opt {
                        Some(next_deleted_row_idx) => next_deleted_row_idx,
                        _ => {
                            // We've processed the final positional delete.
                            // Conclude the skip and then break so that we select the remaining
                            // rows in the row group and move on to the next row group
                            results.push(RowSelector::skip(run_length));
                            break 'chunks;
                        }
                    };
                }
                if run_length > 0 {
                    results.push(RowSelector::skip(run_length));
                }
            }

            if current_idx < next_row_group_base_idx {
                results.push(RowSelector::select(
                    (next_row_group_base_idx - current_idx) as usize,
                ));
            }

            current_row_group_base_idx += row_group_num_rows;
        }

        Ok(results.into())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::sync::Arc;

    use arrow_array::cast::AsArray;
    use arrow_array::{RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use futures::TryStreamExt;
    use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
    use parquet::basic::Compression;
    use parquet::file::metadata::{ColumnChunkMetaData, RowGroupMetaData};
    use parquet::file::properties::WriterProperties;
    use parquet::schema::types::{SchemaDescPtr, SchemaDescriptor};
    use roaring::RoaringTreemap;
    use tempfile::TempDir;

    use crate::arrow::{ArrowReader, ArrowReaderBuilder};
    use crate::delete_vector::DeleteVector;
    use crate::io::FileIO;
    use crate::scan::{FileScanTask, FileScanTaskDeleteFile, FileScanTaskStream};
    use crate::spec::{DataContentType, DataFileFormat, NestedField, PrimitiveType, Schema, Type};

    fn build_test_row_group_meta(
        schema_descr: SchemaDescPtr,
        columns: Vec<ColumnChunkMetaData>,
        num_rows: i64,
        ordinal: i16,
    ) -> RowGroupMetaData {
        RowGroupMetaData::builder(schema_descr.clone())
            .set_num_rows(num_rows)
            .set_total_byte_size(2000)
            .set_column_metadata(columns)
            .set_ordinal(ordinal)
            .build()
            .unwrap()
    }

    fn get_test_schema_descr() -> SchemaDescPtr {
        use parquet::schema::types::Type as SchemaType;

        let schema = SchemaType::group_type_builder("schema")
            .with_fields(vec![
                Arc::new(
                    SchemaType::primitive_type_builder("a", parquet::basic::Type::INT32)
                        .build()
                        .unwrap(),
                ),
                Arc::new(
                    SchemaType::primitive_type_builder("b", parquet::basic::Type::INT32)
                        .build()
                        .unwrap(),
                ),
            ])
            .build()
            .unwrap();

        Arc::new(SchemaDescriptor::new(Arc::new(schema)))
    }

    #[test]
    fn test_build_deletes_row_selection() {
        let schema_descr = get_test_schema_descr();

        let mut columns = vec![];
        for ptr in schema_descr.columns() {
            let column = ColumnChunkMetaData::builder(ptr.clone()).build().unwrap();
            columns.push(column);
        }

        let row_groups_metadata = vec![
            build_test_row_group_meta(schema_descr.clone(), columns.clone(), 1000, 0),
            build_test_row_group_meta(schema_descr.clone(), columns.clone(), 500, 1),
            build_test_row_group_meta(schema_descr.clone(), columns.clone(), 500, 2),
            build_test_row_group_meta(schema_descr.clone(), columns.clone(), 1000, 3),
            build_test_row_group_meta(schema_descr.clone(), columns.clone(), 500, 4),
        ];

        let selected_row_groups = Some(vec![1, 3]);

        /* cases to cover:
           * {skip|select} {first|intermediate|last} {one row|multiple rows} in
             {first|intermediate|last} {skipped|selected} row group
           * row group selection disabled
        */

        let positional_deletes = RoaringTreemap::from_iter(&[
            1, // in skipped rg 0, should be ignored
            3, // run of three consecutive items in skipped rg0
            4, 5, 998, // two consecutive items at end of skipped rg0
            999, 1000, // solitary row at start of selected rg1 (1, 9)
            1010, // run of 3 rows in selected rg1
            1011, 1012, // (3, 485)
            1498, // run of two items at end of selected rg1
            1499, 1500, // run of two items at start of skipped rg2
            1501, 1600, // should ignore, in skipped rg2
            1999, // single row at end of skipped rg2
            2000, // run of two items at start of selected rg3
            2001, // (4, 98)
            2100, // single row in selected row group 3 (1, 99)
            2200, // run of 3 consecutive rows in selected row group 3
            2201, 2202, // (3, 796)
            2999, // single item at end of selected rg3 (1)
            3000, // single item at start of skipped rg4
        ]);

        let positional_deletes = DeleteVector::new(positional_deletes);

        // using selected row groups 1 and 3
        let result = ArrowReader::build_deletes_row_selection(
            &row_groups_metadata,
            &selected_row_groups,
            &positional_deletes,
        )
        .unwrap();

        let expected = RowSelection::from(vec![
            RowSelector::skip(1),
            RowSelector::select(9),
            RowSelector::skip(3),
            RowSelector::select(485),
            RowSelector::skip(4),
            RowSelector::select(98),
            RowSelector::skip(1),
            RowSelector::select(99),
            RowSelector::skip(3),
            RowSelector::select(796),
            RowSelector::skip(1),
        ]);

        assert_eq!(result, expected);

        // selecting all row groups
        let result = ArrowReader::build_deletes_row_selection(
            &row_groups_metadata,
            &None,
            &positional_deletes,
        )
        .unwrap();

        let expected = RowSelection::from(vec![
            RowSelector::select(1),
            RowSelector::skip(1),
            RowSelector::select(1),
            RowSelector::skip(3),
            RowSelector::select(992),
            RowSelector::skip(3),
            RowSelector::select(9),
            RowSelector::skip(3),
            RowSelector::select(485),
            RowSelector::skip(4),
            RowSelector::select(98),
            RowSelector::skip(1),
            RowSelector::select(398),
            RowSelector::skip(3),
            RowSelector::select(98),
            RowSelector::skip(1),
            RowSelector::select(99),
            RowSelector::skip(3),
            RowSelector::select(796),
            RowSelector::skip(2),
            RowSelector::select(499),
        ]);

        assert_eq!(result, expected);
    }

    /// Test for bug where position deletes in later row groups are not applied correctly.
    ///
    /// When a file has multiple row groups and a position delete targets a row in a later
    /// row group, the `build_deletes_row_selection` function had a bug where it would
    /// fail to increment `current_row_group_base_idx` when skipping row groups.
    ///
    /// This test creates:
    /// - A data file with 200 rows split into 2 row groups (0-99, 100-199)
    /// - A position delete file that deletes row 199 (last row in second row group)
    ///
    /// Expected behavior: Should return 199 rows (with id=200 deleted)
    /// Bug behavior: Returns 200 rows (delete is not applied)
    ///
    /// This bug was discovered while running Apache Spark + Apache Iceberg integration tests
    /// through DataFusion Comet. The following Iceberg Java tests failed due to this bug:
    /// - `org.apache.iceberg.spark.extensions.TestMergeOnReadDelete::testDeleteWithMultipleRowGroupsParquet`
    /// - `org.apache.iceberg.spark.extensions.TestMergeOnReadUpdate::testUpdateWithMultipleRowGroupsParquet`
    #[tokio::test]
    async fn test_position_delete_across_multiple_row_groups() {
        use arrow_array::{Int32Array, Int64Array};
        use parquet::file::reader::{FileReader, SerializedFileReader};

        // Field IDs for positional delete schema
        const FIELD_ID_POSITIONAL_DELETE_FILE_PATH: u64 = 2147483546;
        const FIELD_ID_POSITIONAL_DELETE_POS: u64 = 2147483545;

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();

        // Create table schema with a single 'id' column
        let table_schema = Arc::new(
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

        // Step 1: Create data file with 200 rows in 2 row groups
        // Row group 0: rows 0-99 (ids 1-100)
        // Row group 1: rows 100-199 (ids 101-200)
        let data_file_path = format!("{table_location}/data.parquet");

        let batch1 = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(
            Int32Array::from_iter_values(1..=100),
        )])
        .unwrap();

        let batch2 = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(
            Int32Array::from_iter_values(101..=200),
        )])
        .unwrap();

        // Force each batch into its own row group
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_max_row_group_row_count(Some(100))
            .build();

        let file = File::create(&data_file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props)).unwrap();
        writer.write(&batch1).expect("Writing batch 1");
        writer.write(&batch2).expect("Writing batch 2");
        writer.close().unwrap();

        // Verify we created 2 row groups
        let verify_file = File::open(&data_file_path).unwrap();
        let verify_reader = SerializedFileReader::new(verify_file).unwrap();
        assert_eq!(
            verify_reader.metadata().num_row_groups(),
            2,
            "Should have 2 row groups"
        );

        // Step 2: Create position delete file that deletes row 199 (id=200, last row in row group 1)
        let delete_file_path = format!("{table_location}/deletes.parquet");

        let delete_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("file_path", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                FIELD_ID_POSITIONAL_DELETE_FILE_PATH.to_string(),
            )])),
            Field::new("pos", DataType::Int64, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                FIELD_ID_POSITIONAL_DELETE_POS.to_string(),
            )])),
        ]));

        // Delete row at position 199 (0-indexed, so it's the last row: id=200)
        let delete_batch = RecordBatch::try_new(delete_schema.clone(), vec![
            Arc::new(StringArray::from_iter_values(vec![data_file_path.clone()])),
            Arc::new(Int64Array::from_iter_values(vec![199i64])),
        ])
        .unwrap();

        let delete_props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let delete_file = File::create(&delete_file_path).unwrap();
        let mut delete_writer =
            ArrowWriter::try_new(delete_file, delete_schema, Some(delete_props)).unwrap();
        delete_writer.write(&delete_batch).unwrap();
        delete_writer.close().unwrap();

        // Step 3: Read the data file with the delete applied
        let file_io = FileIO::new_with_fs();
        let reader = ArrowReaderBuilder::new(file_io).build();

        let task = FileScanTask {
            file_size_in_bytes: std::fs::metadata(&data_file_path).unwrap().len(),
            start: 0,
            length: 0,
            record_count: Some(200),
            data_file_path: data_file_path.clone(),
            data_file_format: DataFileFormat::Parquet,
            schema: table_schema.clone(),
            project_field_ids: vec![1],
            predicate: None,
            deletes: vec![FileScanTaskDeleteFile {
                file_size_in_bytes: std::fs::metadata(&delete_file_path).unwrap().len(),
                file_path: delete_file_path,
                file_type: DataContentType::PositionDeletes,
                partition_spec_id: 0,
                equality_ids: None,
            }],
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: false,
        };

        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let result = reader
            .read(tasks)
            .unwrap()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        // Step 4: Verify we got 199 rows (not 200)
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();

        println!("Total rows read: {total_rows}");
        println!("Expected: 199 rows (deleted row 199 which had id=200)");

        // This assertion will FAIL before the fix and PASS after the fix
        assert_eq!(
            total_rows, 199,
            "Expected 199 rows after deleting row 199, but got {total_rows} rows. \
             The bug causes position deletes in later row groups to be ignored."
        );

        // Verify the deleted row (id=200) is not present
        let all_ids: Vec<i32> = result
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_primitive::<arrow_array::types::Int32Type>()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();

        assert!(
            !all_ids.contains(&200),
            "Row with id=200 should be deleted but was found in results"
        );

        // Verify we have all other ids (1-199)
        let expected_ids: Vec<i32> = (1..=199).collect();
        assert_eq!(
            all_ids, expected_ids,
            "Should have ids 1-199 but got different values"
        );
    }

    /// Test for bug where position deletes are lost when skipping unselected row groups.
    ///
    /// This is a variant of `test_position_delete_across_multiple_row_groups` that exercises
    /// the row group selection code path (`selected_row_groups: Some([...])`).
    ///
    /// When a file has multiple row groups and only some are selected for reading,
    /// the `build_deletes_row_selection` function must correctly skip over deletes in
    /// unselected row groups WITHOUT consuming deletes that belong to selected row groups.
    ///
    /// This test creates:
    /// - A data file with 200 rows split into 2 row groups (0-99, 100-199)
    /// - A position delete file that deletes row 199 (last row in second row group)
    /// - Row group selection that reads ONLY row group 1 (rows 100-199)
    ///
    /// Expected behavior: Should return 99 rows (with row 199 deleted)
    /// Bug behavior: Returns 100 rows (delete is lost when skipping row group 0)
    ///
    /// The bug occurs when processing row group 0 (unselected):
    /// ```rust
    /// delete_vector_iter.advance_to(next_row_group_base_idx); // Position at first delete >= 100
    /// next_deleted_row_idx_opt = delete_vector_iter.next(); // BUG: Consumes delete at 199!
    /// ```
    ///
    /// The fix is to NOT call `next()` after `advance_to()` when skipping unselected row groups,
    /// because `advance_to()` already positions the iterator correctly without consuming elements.
    #[tokio::test]
    async fn test_position_delete_with_row_group_selection() {
        use arrow_array::{Int32Array, Int64Array};
        use parquet::file::reader::{FileReader, SerializedFileReader};

        // Field IDs for positional delete schema
        const FIELD_ID_POSITIONAL_DELETE_FILE_PATH: u64 = 2147483546;
        const FIELD_ID_POSITIONAL_DELETE_POS: u64 = 2147483545;

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();

        // Create table schema with a single 'id' column
        let table_schema = Arc::new(
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

        // Step 1: Create data file with 200 rows in 2 row groups
        // Row group 0: rows 0-99 (ids 1-100)
        // Row group 1: rows 100-199 (ids 101-200)
        let data_file_path = format!("{table_location}/data.parquet");

        let batch1 = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(
            Int32Array::from_iter_values(1..=100),
        )])
        .unwrap();

        let batch2 = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(
            Int32Array::from_iter_values(101..=200),
        )])
        .unwrap();

        // Force each batch into its own row group
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_max_row_group_row_count(Some(100))
            .build();

        let file = File::create(&data_file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props)).unwrap();
        writer.write(&batch1).expect("Writing batch 1");
        writer.write(&batch2).expect("Writing batch 2");
        writer.close().unwrap();

        // Verify we created 2 row groups
        let verify_file = File::open(&data_file_path).unwrap();
        let verify_reader = SerializedFileReader::new(verify_file).unwrap();
        assert_eq!(
            verify_reader.metadata().num_row_groups(),
            2,
            "Should have 2 row groups"
        );

        // Step 2: Create position delete file that deletes row 199 (id=200, last row in row group 1)
        let delete_file_path = format!("{table_location}/deletes.parquet");

        let delete_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("file_path", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                FIELD_ID_POSITIONAL_DELETE_FILE_PATH.to_string(),
            )])),
            Field::new("pos", DataType::Int64, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                FIELD_ID_POSITIONAL_DELETE_POS.to_string(),
            )])),
        ]));

        // Delete row at position 199 (0-indexed, so it's the last row: id=200)
        let delete_batch = RecordBatch::try_new(delete_schema.clone(), vec![
            Arc::new(StringArray::from_iter_values(vec![data_file_path.clone()])),
            Arc::new(Int64Array::from_iter_values(vec![199i64])),
        ])
        .unwrap();

        let delete_props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let delete_file = File::create(&delete_file_path).unwrap();
        let mut delete_writer =
            ArrowWriter::try_new(delete_file, delete_schema, Some(delete_props)).unwrap();
        delete_writer.write(&delete_batch).unwrap();
        delete_writer.close().unwrap();

        // Step 3: Get byte ranges to read ONLY row group 1 (rows 100-199)
        // This exercises the row group selection code path where row group 0 is skipped
        let metadata_file = File::open(&data_file_path).unwrap();
        let metadata_reader = SerializedFileReader::new(metadata_file).unwrap();
        let metadata = metadata_reader.metadata();

        let row_group_0 = metadata.row_group(0);
        let row_group_1 = metadata.row_group(1);

        let rg0_start = 4u64; // Parquet files start with 4-byte magic "PAR1"
        let rg1_start = rg0_start + row_group_0.compressed_size() as u64;
        let rg1_length = row_group_1.compressed_size() as u64;

        println!(
            "Row group 0: starts at byte {}, {} bytes compressed",
            rg0_start,
            row_group_0.compressed_size()
        );
        println!(
            "Row group 1: starts at byte {}, {} bytes compressed",
            rg1_start,
            row_group_1.compressed_size()
        );

        let file_io = FileIO::new_with_fs();
        let reader = ArrowReaderBuilder::new(file_io).build();

        // Create FileScanTask that reads ONLY row group 1 via byte range filtering
        let task = FileScanTask {
            file_size_in_bytes: std::fs::metadata(&data_file_path).unwrap().len(),
            start: rg1_start,
            length: rg1_length,
            record_count: Some(100), // Row group 1 has 100 rows
            data_file_path: data_file_path.clone(),
            data_file_format: DataFileFormat::Parquet,
            schema: table_schema.clone(),
            project_field_ids: vec![1],
            predicate: None,
            deletes: vec![FileScanTaskDeleteFile {
                file_size_in_bytes: std::fs::metadata(&delete_file_path).unwrap().len(),
                file_path: delete_file_path,
                file_type: DataContentType::PositionDeletes,
                partition_spec_id: 0,
                equality_ids: None,
            }],
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: false,
        };

        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let result = reader
            .read(tasks)
            .unwrap()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        // Step 4: Verify we got 99 rows (not 100)
        // Row group 1 has 100 rows (ids 101-200), minus 1 delete (id=200) = 99 rows
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();

        println!("Total rows read from row group 1: {total_rows}");
        println!("Expected: 99 rows (row group 1 has 100 rows, 1 delete at position 199)");

        // This assertion will FAIL before the fix and PASS after the fix
        assert_eq!(
            total_rows, 99,
            "Expected 99 rows from row group 1 after deleting position 199, but got {total_rows} rows. \
             The bug causes position deletes to be lost when advance_to() is followed by next() \
             when skipping unselected row groups."
        );

        // Verify the deleted row (id=200) is not present
        let all_ids: Vec<i32> = result
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_primitive::<arrow_array::types::Int32Type>()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();

        assert!(
            !all_ids.contains(&200),
            "Row with id=200 should be deleted but was found in results"
        );

        // Verify we have ids 101-199 (not 101-200)
        let expected_ids: Vec<i32> = (101..=199).collect();
        assert_eq!(
            all_ids, expected_ids,
            "Should have ids 101-199 but got different values"
        );
    }

    /// Test for bug where stale cached delete causes infinite loop when skipping row groups.
    ///
    /// This test exposes the inverse scenario of `test_position_delete_with_row_group_selection`:
    /// - Position delete targets a row in the SKIPPED row group (not the selected one)
    /// - After calling advance_to(), the cached delete index is stale
    /// - Without updating the cache, the code enters an infinite loop
    ///
    /// This test creates:
    /// - A data file with 200 rows split into 2 row groups (0-99, 100-199)
    /// - A position delete file that deletes row 0 (first row in SKIPPED row group 0)
    /// - Row group selection that reads ONLY row group 1 (rows 100-199)
    ///
    /// The bug occurs when skipping row group 0:
    /// ```rust
    /// let mut next_deleted_row_idx_opt = delete_vector_iter.next(); // Some(0)
    /// // ... skip to row group 1 ...
    /// delete_vector_iter.advance_to(100); // Iterator advances past delete at 0
    /// // BUG: next_deleted_row_idx_opt is still Some(0) - STALE!
    /// // When processing row group 1:
    /// //   current_idx = 100, next_deleted_row_idx = 0, next_row_group_base_idx = 200
    /// //   Loop condition: 0 < 200 (true)
    /// //   But: current_idx (100) > next_deleted_row_idx (0)
    /// //   And: current_idx (100) != next_deleted_row_idx (0)
    /// //   Neither branch executes -> INFINITE LOOP!
    /// ```
    ///
    /// Expected behavior: Should return 100 rows (delete at 0 doesn't affect row group 1)
    /// Bug behavior: Infinite loop in build_deletes_row_selection
    #[tokio::test]
    async fn test_position_delete_in_skipped_row_group() {
        use arrow_array::{Int32Array, Int64Array};
        use parquet::file::reader::{FileReader, SerializedFileReader};

        // Field IDs for positional delete schema
        const FIELD_ID_POSITIONAL_DELETE_FILE_PATH: u64 = 2147483546;
        const FIELD_ID_POSITIONAL_DELETE_POS: u64 = 2147483545;

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();

        // Create table schema with a single 'id' column
        let table_schema = Arc::new(
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

        // Step 1: Create data file with 200 rows in 2 row groups
        // Row group 0: rows 0-99 (ids 1-100)
        // Row group 1: rows 100-199 (ids 101-200)
        let data_file_path = format!("{table_location}/data.parquet");

        let batch1 = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(
            Int32Array::from_iter_values(1..=100),
        )])
        .unwrap();

        let batch2 = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(
            Int32Array::from_iter_values(101..=200),
        )])
        .unwrap();

        // Force each batch into its own row group
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_max_row_group_row_count(Some(100))
            .build();

        let file = File::create(&data_file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props)).unwrap();
        writer.write(&batch1).expect("Writing batch 1");
        writer.write(&batch2).expect("Writing batch 2");
        writer.close().unwrap();

        // Verify we created 2 row groups
        let verify_file = File::open(&data_file_path).unwrap();
        let verify_reader = SerializedFileReader::new(verify_file).unwrap();
        assert_eq!(
            verify_reader.metadata().num_row_groups(),
            2,
            "Should have 2 row groups"
        );

        // Step 2: Create position delete file that deletes row 0 (id=1, first row in row group 0)
        let delete_file_path = format!("{table_location}/deletes.parquet");

        let delete_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("file_path", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                FIELD_ID_POSITIONAL_DELETE_FILE_PATH.to_string(),
            )])),
            Field::new("pos", DataType::Int64, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                FIELD_ID_POSITIONAL_DELETE_POS.to_string(),
            )])),
        ]));

        // Delete row at position 0 (0-indexed, so it's the first row: id=1)
        let delete_batch = RecordBatch::try_new(delete_schema.clone(), vec![
            Arc::new(StringArray::from_iter_values(vec![data_file_path.clone()])),
            Arc::new(Int64Array::from_iter_values(vec![0i64])),
        ])
        .unwrap();

        let delete_props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let delete_file = File::create(&delete_file_path).unwrap();
        let mut delete_writer =
            ArrowWriter::try_new(delete_file, delete_schema, Some(delete_props)).unwrap();
        delete_writer.write(&delete_batch).unwrap();
        delete_writer.close().unwrap();

        // Step 3: Get byte ranges to read ONLY row group 1 (rows 100-199)
        // This exercises the row group selection code path where row group 0 is skipped
        let metadata_file = File::open(&data_file_path).unwrap();
        let metadata_reader = SerializedFileReader::new(metadata_file).unwrap();
        let metadata = metadata_reader.metadata();

        let row_group_0 = metadata.row_group(0);
        let row_group_1 = metadata.row_group(1);

        let rg0_start = 4u64; // Parquet files start with 4-byte magic "PAR1"
        let rg1_start = rg0_start + row_group_0.compressed_size() as u64;
        let rg1_length = row_group_1.compressed_size() as u64;

        let file_io = FileIO::new_with_fs();
        let reader = ArrowReaderBuilder::new(file_io).build();

        // Create FileScanTask that reads ONLY row group 1 via byte range filtering
        let task = FileScanTask {
            file_size_in_bytes: std::fs::metadata(&data_file_path).unwrap().len(),
            start: rg1_start,
            length: rg1_length,
            record_count: Some(100), // Row group 1 has 100 rows
            data_file_path: data_file_path.clone(),
            data_file_format: DataFileFormat::Parquet,
            schema: table_schema.clone(),
            project_field_ids: vec![1],
            predicate: None,
            deletes: vec![FileScanTaskDeleteFile {
                file_size_in_bytes: std::fs::metadata(&delete_file_path).unwrap().len(),
                file_path: delete_file_path,
                file_type: DataContentType::PositionDeletes,
                partition_spec_id: 0,
                equality_ids: None,
            }],
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: false,
        };

        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let result = reader
            .read(tasks)
            .unwrap()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        // Step 4: Verify we got 100 rows (all of row group 1)
        // The delete at position 0 is in row group 0, which is skipped, so it doesn't affect us
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();

        assert_eq!(
            total_rows, 100,
            "Expected 100 rows from row group 1 (delete at position 0 is in skipped row group 0). \
             If this hangs or fails, it indicates the cached delete index was not updated after advance_to()."
        );

        // Verify we have all ids from row group 1 (101-200)
        let all_ids: Vec<i32> = result
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_primitive::<arrow_array::types::Int32Type>()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();

        let expected_ids: Vec<i32> = (101..=200).collect();
        assert_eq!(
            all_ids, expected_ids,
            "Should have ids 101-200 (all of row group 1)"
        );
    }
}
