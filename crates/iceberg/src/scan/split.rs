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

//! File splitting and adjacent-task merging for scan planning.

use crate::scan::FileScanTask;
use crate::spec::DataFileFormat;

/// Split a [`FileScanTask`] into smaller tasks based on `target_split_size`.
///
/// For Parquet files with `split_offsets`, splits at row group boundaries.
/// For other formats or missing offsets, splits at fixed byte intervals.
/// Tasks already at or below `target_split_size` are returned unchanged.
pub(crate) fn split_scan_task(task: FileScanTask, target_split_size: u64) -> Vec<FileScanTask> {
    if task.length <= target_split_size {
        return vec![task];
    }

    if task.data_file_format == DataFileFormat::Parquet
        && let Some(ref offsets) = task.split_offsets
        && !offsets.is_empty()
    {
        return split_by_offsets(&task, offsets, target_split_size);
    }

    split_fixed_size(&task, target_split_size)
}

/// Split at row group boundaries defined by `split_offsets`.
fn split_by_offsets(
    task: &FileScanTask,
    offsets: &[i64],
    target_split_size: u64,
) -> Vec<FileScanTask> {
    let file_end = task.start + task.length;

    // Filter offsets to those within our byte range and collect boundaries
    let mut boundaries: Vec<u64> = offsets
        .iter()
        .map(|&o| o as u64)
        .filter(|&o| o >= task.start && o < file_end)
        .collect();

    // Ensure the range start is included as the first boundary
    if boundaries.is_empty() || boundaries[0] != task.start {
        boundaries.insert(0, task.start);
    }

    let mut result = Vec::new();
    let mut group_start = boundaries[0];
    let mut group_size: u64 = 0;

    for i in 1..boundaries.len() {
        let segment_size = boundaries[i] - (if i > 0 { boundaries[i - 1] } else { group_start });
        group_size += segment_size;

        if group_size >= target_split_size {
            result.push(make_split(task, group_start, boundaries[i] - group_start));
            group_start = boundaries[i];
            group_size = 0;
        }
    }

    // Final segment: from group_start to end of task
    let final_length = file_end - group_start;
    if final_length > 0 {
        result.push(make_split(task, group_start, final_length));
    }

    if result.is_empty() {
        result.push(make_split(task, task.start, task.length));
    }

    result
}

/// Split at fixed byte intervals.
fn split_fixed_size(task: &FileScanTask, target_split_size: u64) -> Vec<FileScanTask> {
    let mut result = Vec::new();
    let end = task.start + task.length;
    let mut offset = task.start;

    while offset < end {
        let length = (end - offset).min(target_split_size);
        result.push(make_split(task, offset, length));
        offset += length;
    }

    result
}

/// Create a new `FileScanTask` covering a sub-range of the original.
fn make_split(task: &FileScanTask, start: u64, length: u64) -> FileScanTask {
    FileScanTask {
        file_size_in_bytes: task.file_size_in_bytes,
        start,
        length,
        // Record count is unknown for partial file reads
        record_count: if start == task.start && length == task.length {
            task.record_count
        } else {
            None
        },
        data_file_path: task.data_file_path.clone(),
        data_file_format: task.data_file_format,
        schema: task.schema.clone(),
        project_field_ids: task.project_field_ids.clone(),
        predicate: task.predicate.clone(),
        deletes: task.deletes.clone(),
        partition: task.partition.clone(),
        partition_spec: task.partition_spec.clone(),
        name_mapping: task.name_mapping.clone(),
        case_sensitive: task.case_sensitive,
        split_offsets: None, // Splits don't need offsets anymore
    }
}

/// Merge adjacent splits from the same file within a group of tasks.
///
/// Two tasks are merged if they have the same `data_file_path` and their
/// byte ranges are contiguous (`task_a.start + task_a.length == task_b.start`).
pub(crate) fn merge_adjacent_tasks(mut tasks: Vec<FileScanTask>) -> Vec<FileScanTask> {
    if tasks.len() <= 1 {
        return tasks;
    }

    // Sort by file path then by start offset for deterministic merging
    tasks.sort_by(|a, b| a.data_file_path.cmp(&b.data_file_path).then(a.start.cmp(&b.start)));

    let mut result: Vec<FileScanTask> = Vec::with_capacity(tasks.len());
    let mut iter = tasks.into_iter();
    let mut current = iter.next().unwrap();

    for next in iter {
        if current.data_file_path == next.data_file_path
            && current.start + current.length == next.start
        {
            // Merge: extend current to cover both ranges
            current.length += next.length;
            // Merged record count is unknown
            current.record_count = None;
        } else {
            result.push(current);
            current = next;
        }
    }
    result.push(current);

    result
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::spec::{DataFileFormat, Schema, SchemaRef};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::builder().build().unwrap())
    }

    fn make_task(start: u64, length: u64, format: DataFileFormat) -> FileScanTask {
        FileScanTask {
            file_size_in_bytes: start + length,
            start,
            length,
            record_count: Some(1000),
            data_file_path: "s3://bucket/data/file.parquet".to_string(),
            data_file_format: format,
            schema: test_schema(),
            project_field_ids: vec![],
            predicate: None,
            deletes: vec![],
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: true,
            split_offsets: None,
        }
    }

    fn make_parquet_task_with_offsets(
        start: u64,
        length: u64,
        offsets: Vec<i64>,
    ) -> FileScanTask {
        let mut task = make_task(start, length, DataFileFormat::Parquet);
        task.split_offsets = Some(offsets);
        task
    }

    #[test]
    fn test_task_smaller_than_target_not_split() {
        let task = make_task(0, 50, DataFileFormat::Parquet);
        let result = split_scan_task(task, 100);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].start, 0);
        assert_eq!(result[0].length, 50);
        assert_eq!(result[0].record_count, Some(1000));
    }

    #[test]
    fn test_task_equal_to_target_not_split() {
        let task = make_task(0, 100, DataFileFormat::Parquet);
        let result = split_scan_task(task, 100);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_fixed_size_split() {
        let task = make_task(0, 250, DataFileFormat::Avro);
        let result = split_scan_task(task, 100);
        assert_eq!(result.len(), 3);
        assert_eq!((result[0].start, result[0].length), (0, 100));
        assert_eq!((result[1].start, result[1].length), (100, 100));
        assert_eq!((result[2].start, result[2].length), (200, 50));
        // Splits don't have record counts
        assert_eq!(result[0].record_count, None);
    }

    #[test]
    fn test_parquet_without_offsets_uses_fixed_split() {
        let task = make_task(0, 250, DataFileFormat::Parquet);
        // No split_offsets set
        let result = split_scan_task(task, 100);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_parquet_offset_aware_split() {
        // File: 300 bytes, row groups at 0, 100, 200
        let task = make_parquet_task_with_offsets(0, 300, vec![0, 100, 200]);
        let result = split_scan_task(task, 150);
        // Groups: [0..200) = 200 bytes (>=150, split), [200..300) = 100 bytes
        assert_eq!(result.len(), 2);
        assert_eq!((result[0].start, result[0].length), (0, 200));
        assert_eq!((result[1].start, result[1].length), (200, 100));
    }

    #[test]
    fn test_parquet_offset_aware_split_single_large_group() {
        // File: 500 bytes, but only one row group
        let task = make_parquet_task_with_offsets(0, 500, vec![0]);
        let result = split_scan_task(task, 100);
        // Can't split at row group boundaries (only one), so entire file is one task
        assert_eq!(result.len(), 1);
        assert_eq!((result[0].start, result[0].length), (0, 500));
    }

    #[test]
    fn test_split_with_nonzero_start() {
        let task = make_task(100, 250, DataFileFormat::Avro);
        let result = split_scan_task(task, 100);
        assert_eq!(result.len(), 3);
        assert_eq!((result[0].start, result[0].length), (100, 100));
        assert_eq!((result[1].start, result[1].length), (200, 100));
        assert_eq!((result[2].start, result[2].length), (300, 50));
    }

    #[test]
    fn test_merge_adjacent_contiguous() {
        let tasks = vec![
            make_task(0, 100, DataFileFormat::Parquet),
            make_task(100, 100, DataFileFormat::Parquet),
        ];
        let result = merge_adjacent_tasks(tasks);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].start, 0);
        assert_eq!(result[0].length, 200);
    }

    #[test]
    fn test_merge_non_contiguous_not_merged() {
        let tasks = vec![
            make_task(0, 100, DataFileFormat::Parquet),
            make_task(200, 100, DataFileFormat::Parquet),
        ];
        let result = merge_adjacent_tasks(tasks);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_merge_different_files_not_merged() {
        let mut task1 = make_task(0, 100, DataFileFormat::Parquet);
        task1.data_file_path = "file1.parquet".to_string();
        let mut task2 = make_task(100, 100, DataFileFormat::Parquet);
        task2.data_file_path = "file2.parquet".to_string();

        let result = merge_adjacent_tasks(vec![task1, task2]);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_merge_empty_and_single() {
        assert!(merge_adjacent_tasks(vec![]).is_empty());

        let single = vec![make_task(0, 100, DataFileFormat::Parquet)];
        let result = merge_adjacent_tasks(single);
        assert_eq!(result.len(), 1);
    }
}
