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
        // Deleted positions as half-open ranges, so a run of deletes costs one step rather than
        // one per row.
        let mut deleted_ranges = positional_deletes.ranges();
        let mut next_deleted_range = deleted_ranges.next();

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
                    // Skip the deletes in this row group without walking them. A range that
                    // continues into the next row group is cut to start there.
                    match next_deleted_range {
                        Some((_, end)) if end <= next_row_group_base_idx => {
                            deleted_ranges.skip_to(next_row_group_base_idx);
                            next_deleted_range = deleted_ranges.next();
                        }
                        Some((start, end)) if start < next_row_group_base_idx => {
                            next_deleted_range = Some((next_row_group_base_idx, end));
                        }
                        _ => {}
                    }

                    // still increment the current page base index but then skip to the next row group
                    // in the file
                    current_row_group_base_idx += row_group_num_rows;
                    continue;
                }
            }

            let mut current_idx = current_row_group_base_idx;
            while let Some((start, end)) = next_deleted_range {
                if start >= next_row_group_base_idx {
                    break;
                }

                // `select` the rows before the deleted range, then `skip` the part of the range
                // that lies in this row group
                let skip_start = start.max(current_idx);
                let skip_end = end.min(next_row_group_base_idx);
                if skip_start > current_idx {
                    results.push(RowSelector::select((skip_start - current_idx) as usize));
                }
                if skip_end > skip_start {
                    results.push(RowSelector::skip((skip_end - skip_start) as usize));
                }
                current_idx = current_idx.max(skip_end);

                if end > next_row_group_base_idx {
                    // the range continues into the next row group
                    break;
                }
                next_deleted_range = deleted_ranges.next();
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

    use crate::Runtime;
    use crate::arrow::{ArrowReader, ArrowReaderBuilder};
    use crate::delete_vector::{DeleteVector, mixed_delete_vectors};
    use crate::io::FileIO;
    use crate::scan::{FileScanTask, FileScanTaskDeleteFile, FileScanTaskStream};
    use crate::spec::{DataContentType, DataFileFormat, NestedField, PrimitiveType, Schema, Type};
    use crate::test_utils::encode_dv_blob;

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

    /// Row groups and deleted runs that cross the 2^32 position boundary, with and without row
    /// group selection.
    #[test]
    fn test_build_deletes_row_selection_across_2_pow_32() {
        let schema_descr = get_test_schema_descr();
        let columns: Vec<_> = schema_descr
            .columns()
            .iter()
            .map(|ptr| ColumnChunkMetaData::builder(ptr.clone()).build().unwrap())
            .collect();
        let row_groups = |sizes: &[i64]| -> Vec<RowGroupMetaData> {
            sizes
                .iter()
                .enumerate()
                .map(|(i, &n)| {
                    build_test_row_group_meta(schema_descr.clone(), columns.clone(), n, i as i16)
                })
                .collect()
        };
        let select = RowSelector::select;
        let skip = RowSelector::skip;
        let high = 1u64 << 32;
        let big = high as usize;

        // Row group 1 starts at 2^32 - 2. The run 2^32 - 3 ..= 2^32 crosses both boundaries.
        let meta = row_groups(&[(1 << 32) - 2, 8]);
        let deletes = DeleteVector::new(RoaringTreemap::from_iter([
            1,
            high - 3,
            high - 2,
            high - 1,
            high,
            high + 3,
        ]));
        let build = |selected: Option<Vec<usize>>| {
            ArrowReader::build_deletes_row_selection(&meta, &selected, &deletes).unwrap()
        };
        assert_eq!(
            build(None),
            RowSelection::from(vec![
                select(1),
                skip(1),
                select(big - 5),
                skip(4),
                select(2),
                skip(1),
                select(2),
            ])
        );
        assert_eq!(
            build(Some(vec![0])),
            RowSelection::from(vec![select(1), skip(1), select(big - 5), skip(1)])
        );
        assert_eq!(
            build(Some(vec![1])),
            RowSelection::from(vec![skip(3), select(2), skip(1), select(2)])
        );

        // Skipping row group 0 lands past 2^32, in the next bitmap.
        let meta = row_groups(&[(1 << 32) + 1, 4]);
        let deletes = DeleteVector::new(RoaringTreemap::from_iter([5, high - 1, high, high + 2]));
        assert_eq!(
            ArrowReader::build_deletes_row_selection(&meta, &Some(vec![1]), &deletes).unwrap(),
            RowSelection::from(vec![select(1), skip(1), select(2)])
        );
    }

    /// The selection for `deleted` built one deleted position at a time.
    fn expected_deletes_row_selection(
        row_group_sizes: &[u64],
        selected_row_groups: &Option<Vec<usize>>,
        deleted: &[u64],
    ) -> RowSelection {
        let mut selectors = Vec::new();
        let mut base = 0;
        for (idx, &num_rows) in row_group_sizes.iter().enumerate() {
            let end = base + num_rows;
            if selected_row_groups
                .as_ref()
                .is_none_or(|selected| selected.contains(&idx))
            {
                let mut row = base;
                let first = deleted.partition_point(|&p| p < base);
                for &pos in deleted[first..].iter().take_while(|&&p| p < end) {
                    selectors.push(RowSelector::select((pos - row) as usize));
                    selectors.push(RowSelector::skip(1));
                    row = pos + 1;
                }
                selectors.push(RowSelector::select((end - row) as usize));
            }
            base = end;
        }
        selectors.into()
    }

    /// Random row group layouts and selections over the delete vectors the iterator tests use,
    /// compared with a selection built one deleted position at a time.
    #[test]
    fn test_build_deletes_row_selection_matches_model() {
        let mut state = 3u64;
        let mut rand = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let schema_descr = get_test_schema_descr();
        let columns: Vec<_> = schema_descr
            .columns()
            .iter()
            .map(|ptr| ColumnChunkMetaData::builder(ptr.clone()).build().unwrap())
            .collect();

        for deletes in mixed_delete_vectors() {
            let deleted: Vec<u64> = deletes.iter().collect();
            for _ in 0..8 {
                // Half the time the file ends at or before a deleted position, so the deletes
                // past its end must be ignored.
                let num_rows = match deleted.len() {
                    n if n > 0 && rand() % 2 == 0 => {
                        deleted[(rand() % n as u64) as usize].saturating_add(rand() % 2)
                    }
                    _ => deleted.last().map_or(0, |&last| last + 1) + rand() % 100,
                }
                .max(1);
                // Row group boundaries, half of them next to a deleted position so that they
                // fall inside runs.
                let mut cuts: Vec<u64> = (0..rand() % 8)
                    .map(|_| match deleted.len() {
                        n if n > 0 && rand() % 2 == 0 => {
                            (deleted[(rand() % n as u64) as usize] + rand() % 3).saturating_sub(1)
                        }
                        _ => rand() % num_rows,
                    })
                    .filter(|&cut| cut > 0 && cut < num_rows)
                    .collect();
                cuts.extend([0, num_rows]);
                cuts.sort_unstable();
                cuts.dedup();
                let sizes: Vec<u64> = cuts.windows(2).map(|w| w[1] - w[0]).collect();
                let row_groups: Vec<RowGroupMetaData> = sizes
                    .iter()
                    .enumerate()
                    .map(|(i, &n)| {
                        build_test_row_group_meta(
                            schema_descr.clone(),
                            columns.clone(),
                            n as i64,
                            i as i16,
                        )
                    })
                    .collect();
                let selected = (rand() % 4 != 0)
                    .then(|| (0..sizes.len()).filter(|_| rand() % 2 == 0).collect());

                assert_eq!(
                    ArrowReader::build_deletes_row_selection(&row_groups, &selected, &deletes)
                        .unwrap(),
                    expected_deletes_row_selection(&sizes, &selected, &deleted),
                    "row groups {sizes:?}, selected {selected:?}"
                );
            }
        }
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
        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();

        let task = FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(&data_file_path).unwrap().len())
            .with_start(0)
            .with_length(0)
            .with_record_count(Some(200))
            .with_data_file_path(data_file_path.clone())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(table_schema.clone())
            .with_project_field_ids(vec![1])
            .with_deletes(vec![
                FileScanTaskDeleteFile::builder()
                    .with_file_size_in_bytes(std::fs::metadata(&delete_file_path).unwrap().len())
                    .with_file_path(delete_file_path)
                    .with_file_type(DataContentType::PositionDeletes)
                    .with_file_format(DataFileFormat::Parquet)
                    .with_partition_spec_id(0)
                    .build()
                    .unwrap(),
            ])
            .with_case_sensitive(false)
            .build()
            .unwrap();

        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
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
        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();

        // Create FileScanTask that reads ONLY row group 1 via byte range filtering
        let task = FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(&data_file_path).unwrap().len())
            .with_start(rg1_start)
            .with_length(rg1_length)
            .with_record_count(Some(100)) // Row group 1 has 100 rows
            .with_data_file_path(data_file_path.clone())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(table_schema.clone())
            .with_project_field_ids(vec![1])
            .with_deletes(vec![
                FileScanTaskDeleteFile::builder()
                    .with_file_size_in_bytes(std::fs::metadata(&delete_file_path).unwrap().len())
                    .with_file_path(delete_file_path)
                    .with_file_type(DataContentType::PositionDeletes)
                    .with_file_format(DataFileFormat::Parquet)
                    .with_partition_spec_id(0)
                    .build()
                    .unwrap(),
            ])
            .with_case_sensitive(false)
            .build()
            .unwrap();

        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
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
        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();

        // Create FileScanTask that reads ONLY row group 1 via byte range filtering
        let task = FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(&data_file_path).unwrap().len())
            .with_start(rg1_start)
            .with_length(rg1_length)
            .with_record_count(Some(100)) // Row group 1 has 100 rows
            .with_data_file_path(data_file_path.clone())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(table_schema.clone())
            .with_project_field_ids(vec![1])
            .with_deletes(vec![
                FileScanTaskDeleteFile::builder()
                    .with_file_size_in_bytes(std::fs::metadata(&delete_file_path).unwrap().len())
                    .with_file_path(delete_file_path)
                    .with_file_type(DataContentType::PositionDeletes)
                    .with_file_format(DataFileFormat::Parquet)
                    .with_partition_spec_id(0)
                    .build()
                    .unwrap(),
            ])
            .with_case_sensitive(false)
            .build()
            .unwrap();

        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
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

    /// End-to-end read of a data file with a V3 deletion vector applied. Exercises the whole
    /// deletion-vector read path together: the DV coordinates on the scan task, the loader
    /// reading the blob by range from its Puffin file, decoding it with DeleteVector::deserialize,
    /// validating cardinality against record_count, and ArrowReader filtering the deleted rows.
    #[tokio::test]
    async fn test_deletion_vector_applied_end_to_end() {
        use arrow_array::Int32Array;

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();

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

        // Data file: ids 1..=5 at positions 0..=4.
        let data_file_path = format!("{table_location}/data.parquet");
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(
            Int32Array::from_iter_values(1..=5),
        )])
        .unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let file = File::create(&data_file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Deletion vector deleting positions 1 and 3 (ids 2 and 4), serialized as a
        // deletion-vector-v1 blob and embedded in a Puffin-like file at a non-zero offset.
        let blob = encode_dv_blob([1u64, 3]);
        let content_offset = 12u64;
        let content_size = blob.len() as u64;
        let mut dv_file_bytes = vec![0u8; content_offset as usize];
        dv_file_bytes.extend_from_slice(&blob);
        let dv_path = format!("{table_location}/deletes.puffin");
        std::fs::write(&dv_path, &dv_file_bytes).unwrap();

        let file_io = FileIO::new_with_fs();
        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();

        let task = FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(&data_file_path).unwrap().len())
            .with_start(0)
            .with_length(0)
            .with_record_count(Some(5))
            .with_data_file_path(data_file_path.clone())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(table_schema.clone())
            .with_project_field_ids(vec![1])
            .with_deletes(vec![
                FileScanTaskDeleteFile::builder()
                    .with_file_path(dv_path.clone())
                    .with_file_size_in_bytes(std::fs::metadata(&dv_path).unwrap().len())
                    .with_file_type(DataContentType::PositionDeletes)
                    .with_file_format(DataFileFormat::Puffin)
                    .with_partition_spec_id(0)
                    .with_referenced_data_file(Some(data_file_path.clone()))
                    .with_content_offset(Some(content_offset))
                    .with_content_size_in_bytes(Some(content_size))
                    .with_record_count(Some(2))
                    .build()
                    .unwrap(),
            ])
            .with_case_sensitive(false)
            .build()
            .unwrap();

        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        let ids: Vec<i32> = result
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

        // Positions 1 and 3 (ids 2 and 4) are deleted; ids 1, 3, 5 remain.
        assert_eq!(ids, vec![1, 3, 5]);
    }

    /// A deletion vector whose decoded cardinality disagrees with the manifest entry's
    /// `record_count` must fail the read rather than silently applying the wrong deletes, the
    /// same invariant Iceberg-Java enforces in `BitmapPositionDeleteIndex.deserializeBitmap`.
    #[tokio::test]
    async fn test_deletion_vector_cardinality_mismatch_fails_read() {
        use arrow_array::Int32Array;

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();

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

        let data_file_path = format!("{table_location}/data.parquet");
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(
            Int32Array::from_iter_values(1..=5),
        )])
        .unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let file = File::create(&data_file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // The blob decodes to 2 positions, but the manifest entry claims 5.
        let blob = encode_dv_blob([1u64, 3]);
        let dv_path = format!("{table_location}/deletes.puffin");
        std::fs::write(&dv_path, &blob).unwrap();

        let file_io = FileIO::new_with_fs();
        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();

        let task = FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(&data_file_path).unwrap().len())
            .with_start(0)
            .with_length(0)
            .with_record_count(Some(5))
            .with_data_file_path(data_file_path.clone())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(table_schema.clone())
            .with_project_field_ids(vec![1])
            .with_deletes(vec![
                FileScanTaskDeleteFile::builder()
                    .with_file_path(dv_path.clone())
                    .with_file_size_in_bytes(std::fs::metadata(&dv_path).unwrap().len())
                    .with_file_type(DataContentType::PositionDeletes)
                    .with_file_format(DataFileFormat::Puffin)
                    .with_partition_spec_id(0)
                    .with_referenced_data_file(Some(data_file_path.clone()))
                    .with_content_offset(Some(0))
                    .with_content_size_in_bytes(Some(blob.len() as u64))
                    .with_record_count(Some(5))
                    .build()
                    .unwrap(),
            ])
            .with_case_sensitive(false)
            .build()
            .unwrap();

        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let result: Result<Vec<RecordBatch>, _> =
            reader.read(tasks).unwrap().stream().try_collect().await;

        let err = result.unwrap_err();
        assert_eq!(err.kind(), crate::ErrorKind::DataInvalid);
        assert!(err.message().contains("expected 5 from record_count"));
    }
}
