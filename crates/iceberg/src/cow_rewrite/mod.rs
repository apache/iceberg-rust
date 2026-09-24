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

//! Copy-on-write rewrite primitives.
//!
//! This module plans candidate data files, reads their visible rows, applies a
//! caller-provided batch rewriter, and writes replacement data files. It returns
//! old and new file sets that can be committed by an overwrite-style transaction
//! action.
//!
//! The primitive does not parse SQL and does not commit metadata by itself.
//! Rewriters must emit batches compatible with the schema rows were read in
//! (the planned snapshot's schema) and must preserve each source file's
//! partition values; this primitive does not repartition rewritten rows.
//!
//! The result carries data files only. A commit adapter consuming these file
//! lists may remove position delete files and deletion vectors only when they
//! exclusively reference removed data files. Equality delete files must be
//! retained while they can still apply to other live data files by sequence
//! number.
//!
//! ```rust,no_run
//! # use std::sync::Arc;
//! # use arrow_array::RecordBatch;
//! # use iceberg::cow_rewrite::{CowBatchRewrite, CowBatchRewriter, CowRewriteBuilder};
//! # use iceberg::table::Table;
//! # use iceberg::Result;
//! struct KeepAll;
//!
//! impl CowBatchRewriter for KeepAll {
//!     fn rewrite_batch(&self, batch: RecordBatch) -> Result<CowBatchRewrite> {
//!         Ok(CowBatchRewrite {
//!             output: Some(batch),
//!             changed: false,
//!         })
//!     }
//! }
//!
//! # async fn example(table: &Table) -> Result<()> {
//! let result = CowRewriteBuilder::new(table)
//!     .with_rewriter(Arc::new(KeepAll))
//!     .rewrite()
//!     .await?;
//!
//! assert!(!result.has_changes());
//! # Ok(())
//! # }
//! ```

mod plan;
mod rewriter;
pub(crate) mod writer;

use std::sync::Arc;

use arrow_array::RecordBatch;
use futures::TryStreamExt;
pub use plan::CowRewriteFile;
pub use rewriter::{CowBatchRewrite, CowBatchRewriter};

use crate::expr::Predicate;
use crate::scan::FileScanTaskStream;
use crate::spec::{DataFile, PartitionKey};
use crate::table::Table;
use crate::{Error, ErrorKind, Result};

/// Counters produced by a copy-on-write rewrite.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CowRewriteStats {
    /// Number of candidate files selected by planning.
    pub candidate_files: usize,
    /// Number of old files that have replacement output or are fully removed.
    pub rewritten_files: usize,
    /// Number of candidate files that did not change after row rewriting.
    pub unchanged_files: usize,
    /// Visible input row count read from candidate files.
    pub input_rows: u64,
    /// Output row count written to replacement files.
    ///
    /// Rows the rewriter emitted for files that turned out unchanged are not
    /// counted, so this always matches the row counts of `added_data_files`.
    pub output_rows: u64,
    /// Number of input batches that changed, including batches the rewriter
    /// dropped entirely (`output: None`) even if it did not flag them.
    pub changed_batches: u64,
}

/// Result of a copy-on-write rewrite operation.
#[derive(Debug, Default)]
pub struct CowRewriteResult {
    /// Old data files that should be removed by the commit action.
    pub removed_data_files: Vec<DataFile>,
    /// New data files that should be added by the commit action.
    pub added_data_files: Vec<DataFile>,
    /// Candidate files that were read and left unchanged.
    ///
    /// Files whose visible rows were all removed by delete files are NOT
    /// included here: they read as zero rows and are reported in
    /// `removed_data_files` with no replacement. A commit adapter may remove
    /// position deletes and deletion vectors that exclusively reference these
    /// removed files, but must retain equality deletes that can still apply to
    /// other live files.
    pub unchanged_data_files: Vec<DataFile>,
    /// Rewrite counters.
    pub stats: CowRewriteStats,
}

impl CowRewriteResult {
    /// Returns true if the rewrite produced any table changes.
    pub fn has_changes(&self) -> bool {
        !self.removed_data_files.is_empty() || !self.added_data_files.is_empty()
    }
}

/// Builder for orchestrating copy-on-write data file rewrites.
pub struct CowRewriteBuilder<'a> {
    table: &'a Table,
    predicate: Predicate,
    snapshot_id: Option<i64>,
    batch_size: Option<usize>,
    case_sensitive: bool,
    rewriter: Option<Arc<dyn CowBatchRewriter>>,
}

impl<'a> CowRewriteBuilder<'a> {
    /// Creates a copy-on-write rewrite builder for `table`.
    pub fn new(table: &'a Table) -> Self {
        Self {
            table,
            predicate: Predicate::AlwaysTrue,
            snapshot_id: None,
            batch_size: None,
            case_sensitive: true,
            rewriter: None,
        }
    }

    /// Sets the row predicate used to plan candidate files.
    pub fn with_predicate(mut self, predicate: Predicate) -> Self {
        self.predicate = predicate;
        self
    }

    /// Sets the snapshot id used to plan candidate files.
    pub fn with_snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = Some(snapshot_id);
        self
    }

    /// Sets the Arrow reader batch size.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    /// Sets the case sensitivity used to bind the planning predicate.
    pub fn with_case_sensitive(mut self, case_sensitive: bool) -> Self {
        self.case_sensitive = case_sensitive;
        self
    }

    /// Sets the record batch rewriter.
    pub fn with_rewriter(mut self, rewriter: Arc<dyn CowBatchRewriter>) -> Self {
        self.rewriter = Some(rewriter);
        self
    }

    /// Plans, reads, rewrites, and writes replacement data files.
    pub async fn rewrite(self) -> Result<CowRewriteResult> {
        let rewriter = self.rewriter.ok_or_else(|| {
            Error::new(
                ErrorKind::PreconditionFailed,
                "COW rewrite requires a batch rewriter",
            )
        })?;
        let files = plan::plan_cow_rewrite_files(
            self.table,
            Some(self.predicate),
            self.snapshot_id,
            self.case_sensitive,
        )
        .await?;

        let mut result = CowRewriteResult {
            stats: CowRewriteStats {
                candidate_files: files.len(),
                ..CowRewriteStats::default()
            },
            ..CowRewriteResult::default()
        };

        for file in files {
            let CowRewriteFile {
                old_data_file,
                scan_task,
            } = file;
            // Schema the rows are read in (the planned snapshot's schema). The
            // replacement files must be written with this schema so that batches
            // remain compatible when the table's current schema has evolved past
            // the snapshot the source files belong to. This preserves the older
            // schema in replacement files rather than promoting them to the
            // table's current schema during this rewrite.
            let write_schema = scan_task.schema_ref();
            let has_delete_files = !scan_task.deletes().is_empty();

            // Batches produced before the first changed batch. They are buffered
            // rather than written immediately because the primitive must not
            // emit a replacement file for a source file that turns out to be
            // unchanged. Once a changed batch is observed the buffered prefix is
            // flushed to the writer and all subsequent batches stream straight
            // through.
            //
            // Worst-case footprint: for a file that never changes (or whose
            // first change sits at its very end) the prefix holds the entire
            // decoded source file in memory. Files are processed sequentially,
            // so peak usage is one file at a time, but that can still be several
            // GB for a compaction-sized file. A size-capped fallback that starts
            // writing the replacement once the buffer crosses a threshold is
            // left for follow-up work.
            let mut prefix: Vec<RecordBatch> = Vec::new();
            let mut file_changed = false;
            let mut file_input_rows = 0_u64;
            let mut file_output_rows = 0_u64;
            let mut writer: Option<Box<dyn crate::writer::IcebergWriter>> = None;

            // Planning already cleared the row predicate (see
            // `ManifestEntryContext::into_cow_rewrite_file`), so this task
            // reads every row of the source file.
            let tasks = Box::pin(futures::stream::iter(vec![Ok(scan_task)])) as FileScanTaskStream;

            // Each candidate file gets its own reader so the per-file prefix
            // and lazy-writer semantics stay intact; the delete-file cache is
            // therefore also per file, and equality deletes shared by several
            // candidates are fetched once per file.
            let mut reader_builder = self.table.reader_builder();
            if let Some(batch_size) = self.batch_size {
                reader_builder = reader_builder.with_batch_size(batch_size);
            }

            let mut batches = reader_builder.build().read(tasks)?.stream();
            while let Some(batch) = batches.try_next().await? {
                result.stats.input_rows += batch.num_rows() as u64;
                file_input_rows += batch.num_rows() as u64;

                let rewrite = rewriter.rewrite_batch(batch)?;
                // `output: None` means the batch is fully removed, which is
                // itself a change. Derive the effective flag instead of
                // trusting every rewriter to keep `changed` consistent with
                // `output` — otherwise a `{changed: false, output: None}`
                // batch would silently drop its rows while leaving the file
                // marked unchanged.
                let changed = rewrite.changed || rewrite.output.is_none();
                if changed {
                    file_changed = true;
                    result.stats.changed_batches += 1;
                }

                // A dropped batch can be the first change. Flush any kept
                // prefix even when this batch has no output; defer opening the
                // writer only if there are no rows to preserve yet.
                if file_changed
                    && writer.is_none()
                    && (!prefix.is_empty() || rewrite.output.is_some())
                {
                    let partition_key =
                        source_partition_key(self.table, &old_data_file, &write_schema)?;
                    writer = Some(
                        writer::build_replacement_writer(
                            self.table,
                            write_schema.clone(),
                            Some(partition_key),
                        )
                        .await?,
                    );
                }

                if let Some(writer) = writer.as_mut() {
                    for prefix_batch in prefix.drain(..) {
                        writer.write(prefix_batch).await?;
                    }
                    if let Some(output) = rewrite.output {
                        file_output_rows += output.num_rows() as u64;
                        writer.write(output).await?;
                    }
                } else if let Some(output) = rewrite.output {
                    file_output_rows += output.num_rows() as u64;
                    prefix.push(output);
                }
            }

            // A candidate whose visible rows were all removed by its delete
            // files reads as zero rows and the loop above never runs. Treat it
            // the same as a rewriter that dropped every batch — changed with
            // no replacement — so the file and its delete files can be
            // compacted away instead of being pinned in the table forever.
            let fully_removed_by_deletes =
                !file_changed && file_input_rows == 0 && has_delete_files;

            if file_changed || fully_removed_by_deletes {
                result.stats.rewritten_files += 1;
                result.stats.output_rows += file_output_rows;
                result.removed_data_files.push(old_data_file);

                if let Some(mut writer) = writer {
                    let added_data_files = writer.close().await?;
                    result.added_data_files.extend(added_data_files);
                }
                // If `writer` is `None`, no visible rows remain after delete
                // files and batch rewriting, so no replacement is written.
            } else {
                result.stats.unchanged_files += 1;
                result.unchanged_data_files.push(old_data_file);
                // `prefix` is dropped here; no replacement file was written.
            }
        }

        Ok(result)
    }
}

fn source_partition_key(
    table: &Table,
    data_file: &DataFile,
    schema: &crate::spec::SchemaRef,
) -> Result<PartitionKey> {
    let spec = table
        .metadata()
        .partition_spec_by_id(data_file.partition_spec_id)
        .ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Missing partition spec {} for COW rewrite source file",
                    data_file.partition_spec_id
                ),
            )
        })?
        .as_ref()
        .clone();
    // `PartitionKey::new` does not bind the spec to the schema, so validate the
    // binding here: a spec that is incompatible with the planned snapshot
    // schema should fail with a clear error instead of producing a bad
    // partition path when the writer later calls `PartitionKey::to_path`.
    spec.partition_type(schema).map_err(|err| {
        Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Cannot bind partition spec {} to the planned snapshot schema for COW rewrite",
                data_file.partition_spec_id
            ),
        )
        .with_source(err)
    })?;

    Ok(PartitionKey::new(
        spec,
        schema.clone(),
        data_file.partition().clone(),
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use arrow_array::{Array, ArrayRef, BooleanArray, Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use futures::TryStreamExt;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use tempfile::TempDir;

    use crate::cow_rewrite::{CowBatchRewrite, CowBatchRewriter, CowRewriteBuilder};
    use crate::io::LocalFsStorageFactory;
    use crate::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use crate::scan::tests::TableTestFixture;
    use crate::scan::{FileScanTask, FileScanTaskStream};
    use crate::spec::{
        DataFile, Literal, NestedField, PrimitiveType, Schema, Struct, TableProperties, Transform,
        Type,
    };
    use crate::table::Table;
    use crate::transaction::{AddColumn, ApplyTransactionAction, Transaction};
    use crate::{Catalog, CatalogBuilder, Error, ErrorKind, NamespaceIdent, Result, TableCreation};

    struct KeepAll;

    impl CowBatchRewriter for KeepAll {
        fn rewrite_batch(&self, batch: RecordBatch) -> Result<CowBatchRewrite> {
            Ok(CowBatchRewrite {
                output: Some(batch),
                changed: false,
            })
        }
    }

    struct DeleteEvenIds;

    impl CowBatchRewriter for DeleteEvenIds {
        fn rewrite_batch(&self, batch: RecordBatch) -> Result<CowBatchRewrite> {
            let ids = batch
                .column_by_name("id")
                .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "missing id column"))?
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "id must be Int32"))?;

            let keep =
                BooleanArray::from_iter((0..ids.len()).map(|row| Some(ids.value(row) % 2 != 0)));
            let filtered = arrow_select::filter::filter_record_batch(&batch, &keep)
                .map_err(|err| Error::new(ErrorKind::Unexpected, err.to_string()))?;

            Ok(CowBatchRewrite {
                changed: filtered.num_rows() != batch.num_rows(),
                output: (filtered.num_rows() > 0).then_some(filtered),
            })
        }
    }

    struct IncrementValueForEvenIds;

    impl CowBatchRewriter for IncrementValueForEvenIds {
        fn rewrite_batch(&self, batch: RecordBatch) -> Result<CowBatchRewrite> {
            let ids = batch
                .column_by_name("id")
                .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "missing id column"))?
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "id must be Int32"))?;
            let values = batch
                .column_by_name("value")
                .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "missing value column"))?
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "value must be Int32"))?;

            let mut changed = false;
            let updated_values = Int32Array::from_iter((0..values.len()).map(|row| {
                let value = values.value(row);
                if ids.value(row) % 2 == 0 {
                    changed = true;
                    Some(value + 10)
                } else {
                    Some(value)
                }
            }));
            let output = RecordBatch::try_new(batch.schema(), vec![
                batch.column(0).clone(),
                Arc::new(updated_values),
            ])
            .map_err(|err| Error::new(ErrorKind::Unexpected, err.to_string()))?;

            Ok(CowBatchRewrite {
                output: Some(output),
                changed,
            })
        }
    }

    struct CowRewriteFixture {
        _temp_dir: TempDir,
        table: Table,
    }

    async fn test_table_with_ids(ids: Vec<i32>) -> Result<CowRewriteFixture> {
        let temp_dir = TempDir::new().unwrap();
        let warehouse = format!("file://{}", temp_dir.path().join("warehouse").display());
        let catalog = MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse)]),
            )
            .await?;
        let namespace = NamespaceIdent::new("ns".to_string());
        catalog.create_namespace(&namespace, HashMap::new()).await?;

        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()?;
        let table = catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("cow_rewrite_fixture".to_string())
                    .schema(schema)
                    .build(),
            )
            .await?;

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
        ]));
        let batch = RecordBatch::try_new(arrow_schema, vec![
            Arc::new(Int32Array::from(ids)) as ArrayRef
        ])?;
        let data_files = super::writer::write_replacement_batches(
            &table,
            table.metadata().current_schema().clone(),
            None,
            futures::stream::iter(vec![Ok(batch)]),
        )
        .await?;

        let tx = Transaction::new(&table);
        let tx = tx.fast_append().add_data_files(data_files).apply(tx)?;
        let table = tx.commit(&catalog).await?;

        Ok(CowRewriteFixture {
            _temp_dir: temp_dir,
            table,
        })
    }

    async fn test_table_with_id_batches(batches: Vec<Vec<i32>>) -> Result<CowRewriteFixture> {
        let temp_dir = TempDir::new().unwrap();
        let warehouse = format!("file://{}", temp_dir.path().join("warehouse").display());
        let catalog = MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse)]),
            )
            .await?;
        let namespace = NamespaceIdent::new("ns".to_string());
        catalog.create_namespace(&namespace, HashMap::new()).await?;

        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()?;
        let table = catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("cow_rewrite_fixture".to_string())
                    .schema(schema)
                    .properties(HashMap::from([(
                        TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES.to_string(),
                        "1".to_string(),
                    )]))
                    .build(),
            )
            .await?;

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
        ]));
        let input = batches.into_iter().map(|ids| {
            Ok(RecordBatch::try_new(arrow_schema.clone(), vec![
                Arc::new(Int32Array::from(ids)) as ArrayRef,
            ])?)
        });
        let data_files = super::writer::write_replacement_batches(
            &table,
            table.metadata().current_schema().clone(),
            None,
            futures::stream::iter(input),
        )
        .await?;

        let tx = Transaction::new(&table);
        let tx = tx.fast_append().add_data_files(data_files).apply(tx)?;
        let table = tx.commit(&catalog).await?;

        Ok(CowRewriteFixture {
            _temp_dir: temp_dir,
            table,
        })
    }

    async fn test_table_with_id_value_rows(rows: Vec<(i32, i32)>) -> Result<CowRewriteFixture> {
        let temp_dir = TempDir::new().unwrap();
        let warehouse = format!("file://{}", temp_dir.path().join("warehouse").display());
        let catalog = MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse)]),
            )
            .await?;
        let namespace = NamespaceIdent::new("ns".to_string());
        catalog.create_namespace(&namespace, HashMap::new()).await?;

        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "value", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()?;
        let table = catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("cow_rewrite_fixture".to_string())
                    .schema(schema)
                    .build(),
            )
            .await?;

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            Field::new("value", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
        ]));
        let ids = rows.iter().map(|(id, _)| *id).collect::<Vec<_>>();
        let values = rows.iter().map(|(_, value)| *value).collect::<Vec<_>>();
        let batch = RecordBatch::try_new(arrow_schema, vec![
            Arc::new(Int32Array::from(ids)) as ArrayRef,
            Arc::new(Int32Array::from(values)) as ArrayRef,
        ])?;
        let data_files = super::writer::write_replacement_batches(
            &table,
            table.metadata().current_schema().clone(),
            None,
            futures::stream::iter(vec![Ok(batch)]),
        )
        .await?;

        let tx = Transaction::new(&table);
        let tx = tx.fast_append().add_data_files(data_files).apply(tx)?;
        let table = tx.commit(&catalog).await?;

        Ok(CowRewriteFixture {
            _temp_dir: temp_dir,
            table,
        })
    }

    async fn read_ids(table: &Table, files: &[DataFile]) -> Result<Vec<i32>> {
        let schema = table.metadata().current_schema().clone();
        let project_field_ids = schema
            .as_struct()
            .fields()
            .iter()
            .map(|field| field.id)
            .collect::<Vec<_>>();
        let tasks = files
            .iter()
            .map(|data_file| {
                FileScanTask::builder()
                    .with_file_size_in_bytes(data_file.file_size_in_bytes())
                    .with_start(0)
                    .with_length(data_file.file_size_in_bytes())
                    .with_record_count(Some(data_file.record_count()))
                    .with_data_file_path(data_file.file_path().to_string())
                    .with_data_file_format(data_file.file_format())
                    .with_schema(schema.clone())
                    .with_project_field_ids(project_field_ids.clone())
                    .with_case_sensitive(true)
                    .build()
            })
            .collect::<Vec<_>>();
        let task_stream = Box::pin(futures::stream::iter(tasks)) as FileScanTaskStream;
        let batches = table
            .reader_builder()
            .build()
            .read(task_stream)?
            .stream()
            .try_collect::<Vec<_>>()
            .await?;

        let mut ids = Vec::new();
        for batch in batches {
            let column = batch
                .column_by_name("id")
                .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "missing id column"))?
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "id must be Int32"))?;
            ids.extend((0..column.len()).map(|row| column.value(row)));
        }
        ids.sort_unstable();
        Ok(ids)
    }

    async fn read_ids_and_values(table: &Table, files: &[DataFile]) -> Result<Vec<(i32, i32)>> {
        let schema = table.metadata().current_schema().clone();
        let project_field_ids = schema
            .as_struct()
            .fields()
            .iter()
            .map(|field| field.id)
            .collect::<Vec<_>>();
        let tasks = files
            .iter()
            .map(|data_file| {
                FileScanTask::builder()
                    .with_file_size_in_bytes(data_file.file_size_in_bytes())
                    .with_start(0)
                    .with_length(data_file.file_size_in_bytes())
                    .with_record_count(Some(data_file.record_count()))
                    .with_data_file_path(data_file.file_path().to_string())
                    .with_data_file_format(data_file.file_format())
                    .with_schema(schema.clone())
                    .with_project_field_ids(project_field_ids.clone())
                    .with_case_sensitive(true)
                    .build()
            })
            .collect::<Vec<_>>();
        let task_stream = Box::pin(futures::stream::iter(tasks)) as FileScanTaskStream;
        let batches = table
            .reader_builder()
            .build()
            .read(task_stream)?
            .stream()
            .try_collect::<Vec<_>>()
            .await?;

        let mut rows = Vec::new();
        for batch in batches {
            let ids = batch
                .column_by_name("id")
                .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "missing id column"))?
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "id must be Int32"))?;
            let values = batch
                .column_by_name("value")
                .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "missing value column"))?
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "value must be Int32"))?;
            rows.extend((0..ids.len()).map(|row| (ids.value(row), values.value(row))));
        }
        rows.sort_unstable_by_key(|(id, _)| *id);
        Ok(rows)
    }

    #[tokio::test]
    async fn cow_rewrite_keep_all_produces_no_changes() -> Result<()> {
        let fixture = test_table_with_ids(vec![1, 2, 3]).await?;

        let result = CowRewriteBuilder::new(&fixture.table)
            .with_predicate(crate::expr::Predicate::AlwaysTrue)
            .with_rewriter(Arc::new(KeepAll))
            .rewrite()
            .await?;

        assert!(!result.has_changes());
        assert_eq!(result.removed_data_files.len(), 0);
        assert_eq!(result.added_data_files.len(), 0);
        assert_eq!(result.unchanged_data_files.len(), 1);
        assert_eq!(result.stats.candidate_files, 1);
        assert_eq!(result.stats.unchanged_files, 1);
        assert_eq!(result.stats.input_rows, 3);
        // Nothing was written, so rows emitted for the unchanged file are not
        // counted as output.
        assert_eq!(result.stats.output_rows, 0);

        Ok(())
    }

    #[tokio::test]
    async fn cow_rewrite_requires_rewriter() -> Result<()> {
        let fixture = test_table_with_ids(vec![1]).await?;

        let err = CowRewriteBuilder::new(&fixture.table)
            .rewrite()
            .await
            .expect_err("missing rewriter should fail");

        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);

        Ok(())
    }

    #[tokio::test]
    async fn cow_rewrite_delete_rows_removes_old_file_and_adds_replacement() -> Result<()> {
        let fixture = test_table_with_ids(vec![1, 2, 3, 4]).await?;

        let result = CowRewriteBuilder::new(&fixture.table)
            .with_predicate(crate::expr::Predicate::AlwaysTrue)
            .with_rewriter(Arc::new(DeleteEvenIds))
            .rewrite()
            .await?;

        assert!(result.has_changes());
        assert_eq!(result.removed_data_files.len(), 1);
        assert_eq!(result.added_data_files.len(), 1);
        assert_eq!(result.stats.input_rows, 4);
        assert_eq!(result.stats.output_rows, 2);

        let ids = read_ids(&fixture.table, &result.added_data_files).await?;
        assert_eq!(ids, vec![1, 3]);

        Ok(())
    }

    #[tokio::test]
    async fn cow_rewrite_update_rows_rewrites_file_with_updated_values() -> Result<()> {
        let fixture =
            test_table_with_id_value_rows(vec![(1, 10), (2, 20), (3, 30), (4, 40)]).await?;

        let result = CowRewriteBuilder::new(&fixture.table)
            .with_predicate(crate::expr::Predicate::AlwaysTrue)
            .with_rewriter(Arc::new(IncrementValueForEvenIds))
            .rewrite()
            .await?;

        assert_eq!(result.removed_data_files.len(), 1);
        assert_eq!(result.added_data_files.len(), 1);
        assert_eq!(result.stats.input_rows, 4);
        assert_eq!(result.stats.output_rows, 4);

        let rows = read_ids_and_values(&fixture.table, &result.added_data_files).await?;
        assert_eq!(rows, vec![(1, 10), (2, 30), (3, 30), (4, 50)]);

        Ok(())
    }

    #[tokio::test]
    async fn cow_rewrite_full_file_delete_removes_old_file_without_replacement() -> Result<()> {
        let fixture = test_table_with_ids(vec![2, 4]).await?;

        let result = CowRewriteBuilder::new(&fixture.table)
            .with_predicate(crate::expr::Predicate::AlwaysTrue)
            .with_rewriter(Arc::new(DeleteEvenIds))
            .rewrite()
            .await?;

        assert!(result.has_changes());
        assert_eq!(result.removed_data_files.len(), 1);
        assert_eq!(result.added_data_files.len(), 0);
        assert_eq!(result.stats.input_rows, 2);
        assert_eq!(result.stats.output_rows, 0);

        Ok(())
    }

    #[tokio::test]
    async fn cow_rewrite_removes_file_fully_covered_by_position_deletes() -> Result<()> {
        let mut fixture = TableTestFixture::new();
        let positions = (0..300).collect::<Vec<i64>>();
        fixture.setup_multi_row_group_manifest(&positions).await;

        let result = CowRewriteBuilder::new(&fixture.table)
            .with_predicate(crate::expr::Predicate::AlwaysTrue)
            .with_rewriter(Arc::new(KeepAll))
            .rewrite()
            .await?;

        assert_eq!(result.stats.candidate_files, 1);
        assert_eq!(result.stats.rewritten_files, 1);
        assert_eq!(result.stats.input_rows, 0);
        assert_eq!(result.stats.output_rows, 0);
        assert_eq!(result.removed_data_files.len(), 1);
        assert!(result.added_data_files.is_empty());
        assert!(result.unchanged_data_files.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn cow_rewrite_delete_no_matching_rows_keeps_old_file() -> Result<()> {
        let fixture = test_table_with_ids(vec![1, 3]).await?;

        let result = CowRewriteBuilder::new(&fixture.table)
            .with_predicate(crate::expr::Predicate::AlwaysTrue)
            .with_rewriter(Arc::new(DeleteEvenIds))
            .rewrite()
            .await?;

        assert!(!result.has_changes());
        assert_eq!(result.removed_data_files.len(), 0);
        assert_eq!(result.added_data_files.len(), 0);
        assert_eq!(result.unchanged_data_files.len(), 1);
        assert_eq!(result.stats.input_rows, 2);
        assert_eq!(result.stats.output_rows, 0);

        Ok(())
    }

    /// Drops every batch while reporting `changed: false`, violating the
    /// documented contract. The orchestrator must derive the change from
    /// `output: None` itself, otherwise the file would be kept as "unchanged"
    /// while its rows were meant to be dropped.
    struct SilentFullDrop;

    impl CowBatchRewriter for SilentFullDrop {
        fn rewrite_batch(&self, _batch: RecordBatch) -> Result<CowBatchRewrite> {
            Ok(CowBatchRewrite {
                output: None,
                changed: false,
            })
        }
    }

    #[tokio::test]
    async fn cow_rewrite_none_output_implies_change() -> Result<()> {
        let fixture = test_table_with_ids(vec![1, 2]).await?;

        let result = CowRewriteBuilder::new(&fixture.table)
            .with_predicate(crate::expr::Predicate::AlwaysTrue)
            .with_rewriter(Arc::new(SilentFullDrop))
            .rewrite()
            .await?;

        assert!(result.has_changes());
        assert_eq!(result.removed_data_files.len(), 1);
        assert_eq!(result.added_data_files.len(), 0);
        assert!(result.unchanged_data_files.is_empty());
        assert_eq!(result.stats.rewritten_files, 1);
        assert_eq!(result.stats.input_rows, 2);
        assert_eq!(result.stats.output_rows, 0);
        assert_eq!(result.stats.changed_batches, 1);

        Ok(())
    }

    /// Returns `output: None` with `changed: false` for the first batch, then
    /// flags the second batch as changed. The replacement must contain exactly
    /// the second batch's rows — the silently dropped first batch must not
    /// resurrect, and the original file must be removed.
    struct DropFirstBatchSilently {
        batches_seen: std::sync::atomic::AtomicUsize,
    }

    impl CowBatchRewriter for DropFirstBatchSilently {
        fn rewrite_batch(&self, batch: RecordBatch) -> Result<CowBatchRewrite> {
            let seen = self
                .batches_seen
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if seen == 0 {
                Ok(CowBatchRewrite {
                    output: None,
                    changed: false,
                })
            } else {
                Ok(CowBatchRewrite {
                    output: Some(batch),
                    changed: true,
                })
            }
        }
    }

    #[tokio::test]
    async fn cow_rewrite_silent_drop_before_change_loses_no_rows() -> Result<()> {
        let fixture = test_table_with_ids(vec![1, 2, 3, 4]).await?;

        let result = CowRewriteBuilder::new(&fixture.table)
            .with_predicate(crate::expr::Predicate::AlwaysTrue)
            .with_batch_size(2)
            .with_rewriter(Arc::new(DropFirstBatchSilently {
                batches_seen: std::sync::atomic::AtomicUsize::new(0),
            }))
            .rewrite()
            .await?;

        assert!(result.has_changes());
        assert_eq!(result.removed_data_files.len(), 1);
        assert_eq!(result.added_data_files.len(), 1);
        assert_eq!(result.stats.input_rows, 4);
        assert_eq!(result.stats.output_rows, 2);

        let ids = read_ids(&fixture.table, &result.added_data_files).await?;
        assert_eq!(ids, vec![3, 4]);

        Ok(())
    }

    #[tokio::test]
    async fn cow_rewrite_kept_prefix_survives_later_full_batch_delete() -> Result<()> {
        let fixture = test_table_with_ids(vec![1, 3, 2, 4]).await?;

        let result = CowRewriteBuilder::new(&fixture.table)
            .with_predicate(crate::expr::Predicate::AlwaysTrue)
            .with_batch_size(2)
            .with_rewriter(Arc::new(DeleteEvenIds))
            .rewrite()
            .await?;

        assert_eq!(result.removed_data_files.len(), 1);
        assert_eq!(result.stats.input_rows, 4);
        assert_eq!(result.stats.output_rows, 2);
        assert_eq!(result.added_data_files.len(), 1);
        assert!(result.unchanged_data_files.is_empty());
        assert_eq!(
            read_ids(&fixture.table, &result.added_data_files).await?,
            vec![1, 3]
        );

        Ok(())
    }

    #[tokio::test]
    async fn cow_rewrite_uses_unique_replacement_paths_for_multiple_source_files() -> Result<()> {
        let fixture = test_table_with_id_batches(vec![vec![1, 2], vec![3, 4]]).await?;

        let result = CowRewriteBuilder::new(&fixture.table)
            .with_predicate(crate::expr::Predicate::AlwaysTrue)
            .with_rewriter(Arc::new(DeleteEvenIds))
            .rewrite()
            .await?;

        assert_eq!(result.stats.candidate_files, 2);
        assert_eq!(result.removed_data_files.len(), 2);
        assert_eq!(result.added_data_files.len(), 2);

        let added_paths = result
            .added_data_files
            .iter()
            .map(|file| file.file_path().to_string())
            .collect::<HashSet<_>>();
        let removed_paths = result
            .removed_data_files
            .iter()
            .map(|file| file.file_path().to_string())
            .collect::<HashSet<_>>();

        assert_eq!(added_paths.len(), result.added_data_files.len());
        assert!(added_paths.is_disjoint(&removed_paths));

        let ids = read_ids(&fixture.table, &result.added_data_files).await?;
        assert_eq!(ids, vec![1, 3]);

        Ok(())
    }

    #[test]
    fn cow_batch_rewriter_is_object_safe() {
        let _rewriter: Arc<dyn CowBatchRewriter> = Arc::new(KeepAll);
    }

    #[test]
    fn cow_rewrite_result_reports_no_changes() {
        let result = crate::cow_rewrite::CowRewriteResult {
            removed_data_files: vec![],
            added_data_files: vec![],
            unchanged_data_files: vec![],
            stats: crate::cow_rewrite::CowRewriteStats {
                candidate_files: 0,
                rewritten_files: 0,
                unchanged_files: 0,
                input_rows: 0,
                output_rows: 0,
                changed_batches: 0,
            },
        };

        assert!(!result.has_changes());
        assert_eq!(result.stats.candidate_files, 0);
    }

    /// `DeleteIfModThree` keeps rows whose `id` is not divisible by 3.
    struct DeleteIfModThree;

    impl CowBatchRewriter for DeleteIfModThree {
        fn rewrite_batch(&self, batch: RecordBatch) -> Result<CowBatchRewrite> {
            let ids = batch
                .column_by_name("id")
                .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "missing id column"))?
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "id must be Int32"))?;

            let keep =
                BooleanArray::from_iter((0..ids.len()).map(|row| Some(ids.value(row) % 3 != 0)));
            let filtered = arrow_select::filter::filter_record_batch(&batch, &keep)
                .map_err(|err| Error::new(ErrorKind::Unexpected, err.to_string()))?;

            Ok(CowBatchRewrite {
                changed: filtered.num_rows() != batch.num_rows(),
                output: (filtered.num_rows() > 0).then_some(filtered),
            })
        }
    }

    /// After adding an optional column (`value`) to the schema, a COW rewrite
    /// of the pre-existing data must not fail. The replacement writer must use
    /// the schema the batches were read in (the snapshot schema), not the
    /// table's evolved current schema, otherwise the parquet writer rejects
    /// the input batch whose column set does not include `value`.
    #[tokio::test]
    async fn cow_rewrite_after_optional_column_add() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let warehouse = format!("file://{}", temp_dir.path().join("warehouse").display());
        let catalog: Arc<dyn Catalog> = Arc::new(
            MemoryCatalogBuilder::default()
                .with_storage_factory(Arc::new(LocalFsStorageFactory))
                .load(
                    "memory",
                    HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse)]),
                )
                .await?,
        );
        let namespace = NamespaceIdent::new("ns".to_string());
        catalog.create_namespace(&namespace, HashMap::new()).await?;

        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()?;
        let table = catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("evolved".to_string())
                    .schema(schema)
                    .build(),
            )
            .await?;

        // Write the original data file with the {id}-only schema.
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
        ]));
        let batch = RecordBatch::try_new(arrow_schema, vec![Arc::new(Int32Array::from(vec![
            1, 2, 3, 4,
        ])) as ArrayRef])?;
        let data_files = super::writer::write_replacement_batches(
            &table,
            table.metadata().current_schema().clone(),
            None,
            futures::stream::iter(vec![Ok(batch)]),
        )
        .await?;
        let tx = Transaction::new(&table);
        let tx = tx.fast_append().add_data_files(data_files).apply(tx)?;
        let table = tx.commit(&*catalog).await?;

        // Evolve the schema: add an optional `value` column.
        let tx = Transaction::new(&table);
        let tx = tx
            .update_schema()
            .add_column(AddColumn::optional(
                "value",
                Type::Primitive(PrimitiveType::Int),
            ))
            .apply(tx)?;
        let table = tx.commit(&*catalog).await?;

        // The current schema now has {id, value} but the current snapshot's
        // schema is still the original {id} schema.
        let current_snapshot = table.metadata().current_snapshot().unwrap();
        assert_ne!(
            table.metadata().current_schema_id(),
            current_snapshot.schema_id().unwrap()
        );

        // Before the fix this fails when the parquet writer rejects the {id}-only
        // batch while configured with the {id, value} current schema.
        let result = CowRewriteBuilder::new(&table)
            .with_predicate(crate::expr::Predicate::AlwaysTrue)
            .with_rewriter(Arc::new(DeleteIfModThree))
            .rewrite()
            .await?;

        // id 3 is divisible by 3 and is dropped; remaining ids are 1, 2, 4.
        assert!(result.has_changes());
        assert_eq!(result.removed_data_files.len(), 1);
        assert_eq!(result.added_data_files.len(), 1);

        let ids = read_ids(&table, &result.added_data_files).await?;
        assert_eq!(ids, vec![1, 2, 4]);

        Ok(())
    }

    /// Builds a two-file table whose files have disjoint id ranges so predicate
    /// metrics pruning can exclude one file entirely.
    async fn two_file_table_disjoint_ids() -> Result<CowRewriteFixture> {
        let temp_dir = TempDir::new().unwrap();
        let warehouse = format!("file://{}", temp_dir.path().join("warehouse").display());
        let catalog = MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse)]),
            )
            .await?;
        let namespace = NamespaceIdent::new("ns".to_string());
        catalog.create_namespace(&namespace, HashMap::new()).await?;

        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()?;
        let table = catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("disjoint".to_string())
                    .schema(schema)
                    .properties(HashMap::from([(
                        TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES.to_string(),
                        "1".to_string(),
                    )]))
                    .build(),
            )
            .await?;

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
        ]));
        // First batch ids 1..=4, second batch ids 100..=103. Force a split via
        // tiny target file size so each batch lands in its own data file.
        let input = vec![vec![1, 2, 3, 4], vec![100, 101, 102, 103]]
            .into_iter()
            .map(|ids| {
                Ok(RecordBatch::try_new(arrow_schema.clone(), vec![
                    Arc::new(Int32Array::from(ids)) as ArrayRef,
                ])?)
            });
        let data_files = super::writer::write_replacement_batches(
            &table,
            table.metadata().current_schema().clone(),
            None,
            futures::stream::iter(input),
        )
        .await?;

        let tx = Transaction::new(&table);
        let tx = tx.fast_append().add_data_files(data_files).apply(tx)?;
        let table = tx.commit(&catalog).await?;

        Ok(CowRewriteFixture {
            _temp_dir: temp_dir,
            table,
        })
    }

    /// Predicate-based planning must exclude files whose metrics cannot match
    /// the predicate. With `id > 50`, the file holding ids 1..=4 must never be
    /// planned as a candidate.
    #[tokio::test]
    async fn cow_rewrite_predicate_prunes_non_matching_files() -> Result<()> {
        let fixture = two_file_table_disjoint_ids().await?;

        let predicate = crate::expr::Reference::new("id").greater_than(crate::spec::Datum::int(50));

        let result = CowRewriteBuilder::new(&fixture.table)
            .with_predicate(predicate)
            .with_rewriter(Arc::new(KeepAll))
            .rewrite()
            .await?;

        // Only the high-id file should be a candidate.
        assert_eq!(result.stats.candidate_files, 1);
        // KeepAll does not change anything, so the single candidate is unchanged.
        assert_eq!(result.removed_data_files.len(), 0);
        assert_eq!(result.added_data_files.len(), 0);
        assert_eq!(result.unchanged_data_files.len(), 1);

        // The surviving candidate must be the high-id file (ids 100..=103).
        let ids = read_ids(&fixture.table, &result.unchanged_data_files).await?;
        assert_eq!(ids, vec![100, 101, 102, 103]);

        Ok(())
    }

    /// Rewrites every batch without altering rows, so every candidate file is
    /// replaced. Useful for exercising the replacement-writing path itself.
    struct TouchEveryBatch;

    impl CowBatchRewriter for TouchEveryBatch {
        fn rewrite_batch(&self, batch: RecordBatch) -> Result<CowBatchRewrite> {
            Ok(CowBatchRewrite {
                output: Some(batch),
                changed: true,
            })
        }
    }

    /// An end-to-end rewrite on an identity-partitioned table must preserve
    /// each source file's partition: the replacement file carries the same
    /// partition values and partition spec id, and lands under the same
    /// partition directory — including the null partition.
    #[tokio::test]
    async fn cow_rewrite_preserves_partitions_end_to_end() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let warehouse = format!("file://{}", temp_dir.path().join("warehouse").display());
        let catalog = MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse)]),
            )
            .await?;
        let namespace = NamespaceIdent::new("ns".to_string());
        catalog.create_namespace(&namespace, HashMap::new()).await?;

        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "value", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()?;
        let partition_spec = crate::spec::PartitionSpec::builder(Arc::new(schema.clone()))
            .add_partition_field("value", "value", Transform::Identity)?
            .build()?;
        let table = catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("partitioned".to_string())
                    .schema(schema)
                    .partition_spec(partition_spec.into_unbound())
                    .build(),
            )
            .await?;

        // Two source files: ids 1, 2 in the `value=1` partition and ids 3, 4
        // in the null partition.
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            Field::new("value", DataType::Int32, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
        ]));
        let current_schema = table.metadata().current_schema().clone();
        let default_spec = table.metadata().default_partition_spec().as_ref().clone();

        let batch_one = RecordBatch::try_new(arrow_schema.clone(), vec![
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(Int32Array::from(vec![1, 1])),
        ])?;
        let one_partition = crate::spec::PartitionKey::new(
            default_spec.clone(),
            current_schema.clone(),
            Struct::from_iter([Some(Literal::int(1))]),
        );
        let files_one = super::writer::write_replacement_batches(
            &table,
            current_schema.clone(),
            Some(one_partition),
            futures::stream::iter(vec![Ok(batch_one)]),
        )
        .await?;

        let batch_null = RecordBatch::try_new(arrow_schema.clone(), vec![
            Arc::new(Int32Array::from(vec![3, 4])) as ArrayRef,
            Arc::new(Int32Array::from(vec![None::<i32>, None])),
        ])?;
        let null_partition = crate::spec::PartitionKey::new(
            default_spec.clone(),
            current_schema.clone(),
            Struct::from_iter([None::<Literal>]),
        );
        let files_null = super::writer::write_replacement_batches(
            &table,
            current_schema.clone(),
            Some(null_partition),
            futures::stream::iter(vec![Ok(batch_null)]),
        )
        .await?;

        let tx = Transaction::new(&table);
        let tx = tx
            .fast_append()
            .add_data_files([files_one, files_null].concat())
            .apply(tx)?;
        let table = tx.commit(&catalog).await?;

        let result = CowRewriteBuilder::new(&table)
            .with_rewriter(Arc::new(TouchEveryBatch))
            .rewrite()
            .await?;

        assert_eq!(result.stats.candidate_files, 2);
        assert_eq!(result.stats.rewritten_files, 2);
        assert_eq!(result.removed_data_files.len(), 2);
        assert_eq!(result.added_data_files.len(), 2);
        assert!(result.unchanged_data_files.is_empty());

        // Partition values survive the rewrite unchanged.
        let one_struct = Struct::from_iter([Some(Literal::int(1))]);
        let null_struct = Struct::from_iter([None::<Literal>]);
        let mut removed_partitions = result
            .removed_data_files
            .iter()
            .map(|file| file.partition().clone())
            .collect::<Vec<_>>();
        let mut added_partitions = result
            .added_data_files
            .iter()
            .map(|file| file.partition().clone())
            .collect::<Vec<_>>();
        removed_partitions.sort_by_key(|partition| partition.is_null_at_index(0));
        added_partitions.sort_by_key(|partition| partition.is_null_at_index(0));
        assert_eq!(removed_partitions, added_partitions);
        assert_eq!(added_partitions[0], one_struct);
        assert_eq!(added_partitions[1], null_struct);

        for file in &result.added_data_files {
            assert_eq!(file.partition_spec_id, 0);
            if file.partition() == &one_struct {
                assert!(file.file_path().contains("value=1"), "{}", file.file_path());
            } else {
                assert!(
                    file.file_path().contains("value=null"),
                    "{}",
                    file.file_path()
                );
            }
        }

        // Replacement content matches the source rows.
        let ids = read_ids(&table, &result.added_data_files).await?;
        assert_eq!(ids, vec![1, 2, 3, 4]);

        Ok(())
    }
}
