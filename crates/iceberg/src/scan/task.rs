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

use futures::stream::BoxStream;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::Result;
use crate::expr::BoundPredicate;
use crate::spec::{DataContentType, DataFileFormat, ManifestEntryRef, Schema, SchemaRef};

/// A stream of [`FileScanTask`].
pub type FileScanTaskStream = BoxStream<'static, Result<FileScanTask>>;

/// Row groups to scan in a Parquet file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ParquetFileScanGroup {
    /// The indexes of row groups to scan.
    pub row_group_indexes: Vec<usize>,
}

impl ParquetFileScanGroup {
    /// Split the file scan range into multiple file scan ranges.
    pub fn split(&mut self, target_count: usize) -> FileScanGroup {
        let mut new_group_indexes = Vec::new();
        let take_count = target_count.min(self.row_group_indexes.len());
        new_group_indexes.extend(self.row_group_indexes.drain(..take_count));
        FileScanGroup::Parquet(ParquetFileScanGroup {
            row_group_indexes: new_group_indexes,
        })
    }

    /// Merge multiple file scan ranges into one file scan range.
    pub fn merge<'a, I>(ranges: I) -> Option<FileScanGroup>
    where I: IntoIterator<Item = &'a Option<FileScanGroup>> {
        let mut merged_row_groups = Vec::new();

        for range_opt in ranges {
            // If any range is None, return None
            let range = range_opt.as_ref()?;

            // Extract row group indexes from Parquet ranges
            match range {
                FileScanGroup::Parquet(parquet_range) => {
                    merged_row_groups.extend(&parquet_range.row_group_indexes);
                }
            }
        }

        // Sort and deduplicate the row group indexes
        merged_row_groups.sort_unstable();
        merged_row_groups.dedup();

        Some(FileScanGroup::Parquet(ParquetFileScanGroup {
            row_group_indexes: merged_row_groups,
        }))
    }
}

/// A group of file to scan.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FileScanGroup {
    /// A group of row groups to scan in a Parquet file.
    Parquet(ParquetFileScanGroup),
}

impl FileScanGroup {
    /// Returns the number of row groups in the file scan range.
    pub fn len(&self) -> usize {
        match self {
            FileScanGroup::Parquet(range) => range.row_group_indexes.len(),
        }
    }

    /// Returns true if the file scan range is empty.
    pub fn is_empty(&self) -> bool {
        match self {
            FileScanGroup::Parquet(range) => range.row_group_indexes.is_empty(),
        }
    }

    /// Split the file scan range into multiple file scan ranges.
    pub fn split(&mut self, target_count: usize) -> Self {
        match self {
            FileScanGroup::Parquet(range) => range.split(target_count),
        }
    }
}

/// A task to scan part of file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FileScanTask {
    /// A FileScanTask contain multiple file scan groups.
    /// A group is a unit work to scan and it can not be split further.
    pub file_range: Option<FileScanGroup>,
    /// The number of records in the file to scan.
    ///
    /// This is an optional field, and only available if we are
    /// reading the entire data file.
    pub record_count: Option<u64>,

    /// The data file path corresponding to the task.
    pub data_file_path: String,

    /// The format of the file to scan.
    pub data_file_format: DataFileFormat,

    /// The schema of the file to scan.
    pub schema: SchemaRef,
    /// The field ids to project.
    pub project_field_ids: Vec<i32>,
    /// The predicate to filter.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub predicate: Option<BoundPredicate>,

    /// The list of delete files that may need to be applied to this data file
    pub deletes: Vec<FileScanTaskDeleteFile>,
}

impl FileScanTask {
    /// Returns the data file path of this file scan task.
    pub fn data_file_path(&self) -> &str {
        &self.data_file_path
    }

    /// Returns the project field id of this file scan task.
    pub fn project_field_ids(&self) -> &[i32] {
        &self.project_field_ids
    }

    /// Returns the predicate of this file scan task.
    pub fn predicate(&self) -> Option<&BoundPredicate> {
        self.predicate.as_ref()
    }

    /// Returns the schema of this file scan task as a reference
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Returns the schema of this file scan task as a SchemaRef
    pub fn schema_ref(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Split out a task with group count equals to target_count
    pub fn split(&mut self, target_count: usize) -> Result<Option<Self>> {
        let Some(range) = &mut self.file_range else {
            // Can't split if we don't have range info
            return Ok(None);
        };

        // Use reference to self to avoid partial move error, clone self for each range
        let new_range = range.split(target_count);

        Ok(Some(Self {
            file_range: Some(new_range),
            ..self.clone()
        }))
    }

    /// Merge multiple file scan tasks into one file scan task.
    pub fn merge<I>(file_scan_tasks: I) -> Result<Vec<Self>>
    where I: IntoIterator<Item = Self> {
        let tasks = file_scan_tasks
            .into_iter()
            .into_group_map_by(|task| task.data_file_path.clone());

        let mut merged_tasks = Vec::with_capacity(tasks.len());
        for (_, tasks) in tasks {
            if tasks.is_empty() {
                continue;
            }

            let file_type = tasks.first().unwrap().data_file_format;
            let merged_range = match file_type {
                DataFileFormat::Parquet => {
                    ParquetFileScanGroup::merge(tasks.iter().map(|task| &task.file_range))
                }
                _ => todo!(),
            };

            if let Some(merged_range) = &merged_range {
                if merged_range.is_empty() {
                    continue;
                }
            }

            merged_tasks.push(Self {
                file_range: merged_range,
                ..tasks.first().unwrap().clone()
            });
        }

        Ok(merged_tasks)
    }
}

#[derive(Debug)]
pub(crate) struct DeleteFileContext {
    pub(crate) manifest_entry: ManifestEntryRef,
    pub(crate) partition_spec_id: i32,
}

impl From<&DeleteFileContext> for FileScanTaskDeleteFile {
    fn from(ctx: &DeleteFileContext) -> Self {
        FileScanTaskDeleteFile {
            file_path: ctx.manifest_entry.file_path().to_string(),
            file_type: ctx.manifest_entry.content_type(),
            partition_spec_id: ctx.partition_spec_id,
            equality_ids: ctx.manifest_entry.data_file.equality_ids.clone(),
        }
    }
}

/// A task to scan part of file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FileScanTaskDeleteFile {
    /// The delete file path
    pub file_path: String,

    /// delete file type
    pub file_type: DataContentType,

    /// partition id
    pub partition_spec_id: i32,

    /// equality ids for equality deletes (empty for positional deletes)
    pub equality_ids: Vec<i32>,
}
