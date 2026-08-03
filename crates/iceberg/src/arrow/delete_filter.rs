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
use std::sync::{Arc, Mutex, RwLock};

use tokio::sync::{Notify, watch};

use crate::delete_vector::DeleteVector;
use crate::expr::Predicate::AlwaysTrue;
use crate::expr::{Bind, BoundPredicate, Predicate};
use crate::scan::FileScanTask;
use crate::spec::DataContentType;
use crate::{Error, ErrorKind, Result};

#[derive(Debug)]
enum EqDelState {
    Loading(watch::Receiver<Option<EqDelLoadResult>>),
    Loaded(Predicate),
    /// The load finished with an error. The entry is kept in the cache so that
    /// consumers arriving after the failure still observe the original error
    /// (with its `retryable` flag) instead of a generic "missing predicate".
    /// Retryable failures may be reclaimed by [`DeleteFilter::try_start_eq_del_load`];
    /// non-retryable failures stay cached to avoid re-reading a file that
    /// cannot be read successfully.
    Failed(EqDelLoadError),
}

type EqDelLoadResult = std::result::Result<Predicate, EqDelLoadError>;

#[derive(Clone, Debug)]
struct EqDelLoadError {
    kind: ErrorKind,
    message: String,
    retryable: bool,
}

impl EqDelLoadError {
    fn from_error(error: &Error) -> Self {
        Self {
            kind: error.kind(),
            message: error.message().to_string(),
            retryable: error.retryable(),
        }
    }

    fn cancelled() -> Self {
        Self {
            kind: ErrorKind::Unexpected,
            message: "Equality delete load was cancelled before completion".to_string(),
            retryable: true,
        }
    }

    fn into_error(self, file_path: &str) -> Error {
        Error::new(self.kind, self.message)
            .with_context("file_path", file_path)
            .with_retryable(self.retryable)
    }
}

/// State tracking for positional delete files.
/// Unlike equality deletes, positional deletes must be fully loaded before
/// the ArrowReader proceeds because retrieval is synchronous and non-blocking.
#[derive(Debug)]
enum PosDelState {
    /// The file is currently being loaded by a task.
    /// The notifier allows other tasks to wait for completion.
    Loading(Arc<Notify>),
    /// The file has been fully loaded and merged into the delete vector map.
    Loaded,
}

#[derive(Debug, Default)]
struct DeleteFileFilterState {
    delete_vectors: HashMap<String, Arc<Mutex<DeleteVector>>>,
    equality_deletes: HashMap<String, EqDelState>,
    positional_deletes: HashMap<String, PosDelState>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct DeleteFilter {
    state: Arc<RwLock<DeleteFileFilterState>>,
}

pub(crate) struct EqDelLoadGuard {
    filter: DeleteFilter,
    file_path: String,
    sender: watch::Sender<Option<EqDelLoadResult>>,
    completed: bool,
}

impl EqDelLoadGuard {
    pub(crate) fn finish(mut self, predicate: Predicate) {
        self.filter
            .complete_eq_del_load(&self.file_path, &self.sender, Ok(predicate));
        self.completed = true;
    }

    pub(crate) fn fail(mut self, error: &Error) {
        self.filter.complete_eq_del_load(
            &self.file_path,
            &self.sender,
            Err(EqDelLoadError::from_error(error)),
        );
        self.completed = true;
    }
}

impl Drop for EqDelLoadGuard {
    fn drop(&mut self) {
        if self.completed {
            return;
        }

        self.filter.complete_eq_del_load(
            &self.file_path,
            &self.sender,
            Err(EqDelLoadError::cancelled()),
        );
    }
}

/// Action to take when trying to start loading a positional delete file
pub(crate) enum PosDelLoadAction {
    /// The file is not loaded, the caller should load it.
    Load,
    /// The file is already loaded, nothing to do.
    AlreadyLoaded,
    /// The file is currently being loaded by another task.
    /// The caller *must* wait for this notifier to ensure data availability
    /// before returning, as subsequent access (get_delete_vector) is synchronous.
    WaitFor(Arc<Notify>),
}

impl DeleteFilter {
    /// Retrieve a delete vector for the data file associated with a given file scan task
    pub(crate) fn get_delete_vector(
        &self,
        file_scan_task: &FileScanTask,
    ) -> Option<Arc<Mutex<DeleteVector>>> {
        self.get_delete_vector_for_path(file_scan_task.data_file_path())
    }

    /// Retrieve a delete vector for a data file
    pub(crate) fn get_delete_vector_for_path(
        &self,
        data_file_path: &str,
    ) -> Option<Arc<Mutex<DeleteVector>>> {
        self.state
            .read()
            .ok()
            .and_then(|st| st.delete_vectors.get(data_file_path).cloned())
    }

    pub(crate) fn try_start_eq_del_load(&self, file_path: &str) -> Option<EqDelLoadGuard> {
        let mut state = self.state.write().unwrap();

        match state.equality_deletes.get(file_path) {
            // Skip if already loaded/loading - another task owns it
            Some(EqDelState::Loading(_) | EqDelState::Loaded(_)) => return None,
            // A non-retryable failure is cached: re-reading the file would fail
            // identically, so keep serving the recorded error instead of
            // re-issuing doomed reads.
            Some(EqDelState::Failed(error)) if !error.retryable => return None,
            // A retryable failure may be reclaimed so that this attempt can
            // retry the load.
            Some(EqDelState::Failed(_)) | None => {}
        }

        // Mark as loading to prevent duplicate work
        let (sender, receiver) = watch::channel(None);
        state
            .equality_deletes
            .insert(file_path.to_string(), EqDelState::Loading(receiver));

        Some(EqDelLoadGuard {
            filter: self.clone(),
            file_path: file_path.to_string(),
            sender,
            completed: false,
        })
    }

    /// Attempts to mark a positional delete file as "loading".
    ///
    /// Returns an action dictating whether the caller should load the file,
    /// wait for another task to load it, or do nothing.
    pub(crate) fn try_start_pos_del_load(&self, file_path: &str) -> PosDelLoadAction {
        let mut state = self.state.write().unwrap();

        if let Some(state) = state.positional_deletes.get(file_path) {
            match state {
                PosDelState::Loaded => return PosDelLoadAction::AlreadyLoaded,
                PosDelState::Loading(notify) => return PosDelLoadAction::WaitFor(notify.clone()),
            }
        }

        let notifier = Arc::new(Notify::new());
        state
            .positional_deletes
            .insert(file_path.to_string(), PosDelState::Loading(notifier));

        PosDelLoadAction::Load
    }

    /// Marks a positional delete file as successfully loaded and notifies any waiting tasks.
    pub(crate) fn finish_pos_del_load(&self, file_path: &str) {
        let notify = {
            let mut state = self.state.write().unwrap();
            if let Some(PosDelState::Loading(notify)) = state
                .positional_deletes
                .insert(file_path.to_string(), PosDelState::Loaded)
            {
                Some(notify)
            } else {
                None
            }
        };

        if let Some(notify) = notify {
            notify.notify_waiters();
        }
    }

    /// Retrieve the equality delete predicate for a given eq delete file path
    pub(crate) async fn get_equality_delete_predicate_for_delete_file_path(
        &self,
        file_path: &str,
    ) -> Result<Option<Predicate>> {
        let mut receiver = {
            match self.state.read().unwrap().equality_deletes.get(file_path) {
                None => return Ok(None),
                Some(EqDelState::Loading(receiver)) => receiver.clone(),
                Some(EqDelState::Loaded(predicate)) => {
                    return Ok(Some(predicate.clone()));
                }
                Some(EqDelState::Failed(error)) => {
                    return Err(error.clone().into_error(file_path));
                }
            }
        };

        loop {
            if let Some(result) = { receiver.borrow_and_update().clone() } {
                return result
                    .map(Some)
                    .map_err(|error| error.into_error(file_path));
            }

            receiver.changed().await.map_err(|_| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Equality delete load ended without publishing a result",
                )
                .with_context("file_path", file_path)
                .with_retryable(true)
            })?;
        }
    }

    /// Builds eq delete predicate for the provided task.
    pub(crate) async fn build_equality_delete_predicate(
        &self,
        file_scan_task: &FileScanTask,
    ) -> Result<Option<BoundPredicate>> {
        // * Filter the task's deletes into just the Equality deletes
        // * Retrieve the unbound predicate for each from self.state.equality_deletes
        // * Logical-AND them all together to get a single combined `Predicate`
        // * Bind the predicate to the task's schema to get a `BoundPredicate`

        let mut combined_predicate = AlwaysTrue;
        for delete in &file_scan_task.deletes {
            if !is_equality_delete(delete) {
                continue;
            }

            let Some(predicate) = self
                .get_equality_delete_predicate_for_delete_file_path(delete.data_file_path())
                .await?
            else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!(
                        "Missing predicate for equality delete file '{}'",
                        delete.data_file_path()
                    ),
                ));
            };

            combined_predicate = combined_predicate.and(predicate);
        }

        if combined_predicate == AlwaysTrue {
            return Ok(None);
        }

        let bound_predicate = combined_predicate
            .bind(file_scan_task.schema.clone(), file_scan_task.case_sensitive)?;
        Ok(Some(bound_predicate))
    }

    pub(crate) fn upsert_delete_vector(
        &mut self,
        data_file_path: String,
        delete_vector: DeleteVector,
    ) {
        let mut state = self.state.write().unwrap();

        let Some(entry) = state.delete_vectors.get_mut(&data_file_path) else {
            state
                .delete_vectors
                .insert(data_file_path, Arc::new(Mutex::new(delete_vector)));
            return;
        };

        *entry.lock().unwrap() |= delete_vector;
    }

    fn complete_eq_del_load(
        &self,
        delete_file_path: &str,
        sender: &watch::Sender<Option<EqDelLoadResult>>,
        result: EqDelLoadResult,
    ) {
        // This runs from `EqDelLoadGuard::drop` as well, so it must not panic on a
        // poisoned lock: panicking while unwinding would abort the process. The
        // guarded state stays consistent under poison recovery because the update
        // below is a plain map insert.
        let mut state = self
            .state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match &result {
            Ok(predicate) => {
                state.equality_deletes.insert(
                    delete_file_path.to_string(),
                    EqDelState::Loaded(predicate.clone()),
                );
            }
            Err(error) => {
                state.equality_deletes.insert(
                    delete_file_path.to_string(),
                    EqDelState::Failed(error.clone()),
                );
            }
        }
        sender.send_replace(Some(result));
    }
}

pub(crate) fn is_equality_delete(f: &FileScanTask) -> bool {
    matches!(f.data_file_content, DataContentType::EqualityDeletes)
}

#[cfg(test)]
pub(crate) mod tests {
    use std::fs::File;
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::Schema as ArrowSchema;
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use super::*;
    use crate::arrow::caching_delete_file_loader::CachingDeleteFileLoader;
    use crate::expr::Reference;
    use crate::io::FileIO;
    use crate::spec::{DataFileFormat, Datum, NestedField, PrimitiveType, Schema, Type};

    type ArrowSchemaRef = Arc<ArrowSchema>;

    const FIELD_ID_POSITIONAL_DELETE_FILE_PATH: u64 = 2147483546;
    const FIELD_ID_POSITIONAL_DELETE_POS: u64 = 2147483545;

    #[tokio::test]
    async fn test_equality_delete_load_failure_wakes_waiters_and_allows_retry() {
        let filter = DeleteFilter::default();
        let load_guard = filter.try_start_eq_del_load("eq-del.parquet").unwrap();

        let waiter_one =
            filter.get_equality_delete_predicate_for_delete_file_path("eq-del.parquet");
        let waiter_two =
            filter.get_equality_delete_predicate_for_delete_file_path("eq-del.parquet");
        tokio::pin!(waiter_one);
        tokio::pin!(waiter_two);

        assert!(futures::poll!(&mut waiter_one).is_pending());
        assert!(futures::poll!(&mut waiter_two).is_pending());

        let load_error =
            Error::new(ErrorKind::Unexpected, "io timeout reached").with_retryable(true);
        load_guard.fail(&load_error);

        for waiter in [waiter_one, waiter_two] {
            let error = tokio::time::timeout(Duration::from_secs(1), waiter)
                .await
                .expect("equality delete waiter hung")
                .expect_err("failed equality delete load should propagate an error");
            assert_eq!(error.kind(), ErrorKind::Unexpected);
            assert_eq!(error.message(), "io timeout reached");
            assert!(error.retryable());
        }

        let predicate = Reference::new("id").equal_to(Datum::long(10));
        filter
            .try_start_eq_del_load("eq-del.parquet")
            .expect("failed loads must be retryable")
            .finish(predicate.clone());

        assert_eq!(
            filter
                .get_equality_delete_predicate_for_delete_file_path("eq-del.parquet")
                .await
                .unwrap(),
            Some(predicate)
        );

        let cancelled_guard = filter
            .try_start_eq_del_load("cancelled-eq-del.parquet")
            .unwrap();
        drop(cancelled_guard);
        assert!(
            filter
                .try_start_eq_del_load("cancelled-eq-del.parquet")
                .is_some(),
            "dropping an in-flight load must not leave a stale Loading entry"
        );
    }

    #[tokio::test]
    async fn test_late_consumers_observe_original_error_and_non_retryable_failures_are_cached() {
        let filter = DeleteFilter::default();
        let load_guard = filter.try_start_eq_del_load("eq-del.parquet").unwrap();

        // Fail with a non-retryable error before any waiter registers.
        let load_error = Error::new(ErrorKind::DataInvalid, "malformed equality delete file");
        assert!(!load_error.retryable());
        load_guard.fail(&load_error);

        // A consumer arriving only after the failure (it never observed the
        // `Loading` state) must still receive the original error with its
        // retryable flag, not a generic "missing predicate" error.
        let error = filter
            .get_equality_delete_predicate_for_delete_file_path("eq-del.parquet")
            .await
            .expect_err("cached equality delete failure should propagate");
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert_eq!(error.message(), "malformed equality delete file");
        assert!(!error.retryable());

        // Non-retryable failures are cached: re-reading the file would fail
        // identically, so no new load may be claimed.
        assert!(
            filter.try_start_eq_del_load("eq-del.parquet").is_none(),
            "non-retryable failures must not be reloaded"
        );

        // The cached error keeps being served on subsequent lookups.
        let error = filter
            .get_equality_delete_predicate_for_delete_file_path("eq-del.parquet")
            .await
            .expect_err("cached equality delete failure should keep propagating");
        assert_eq!(error.message(), "malformed equality delete file");
    }

    #[tokio::test]
    async fn test_delete_file_filter_load_deletes() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path();
        let file_io = FileIO::from_path(table_location.as_os_str().to_str().unwrap())
            .unwrap()
            .build()
            .unwrap();

        let delete_file_loader = CachingDeleteFileLoader::new(file_io.clone(), 10);

        let file_scan_tasks = setup(table_location);

        let delete_filter = delete_file_loader
            .load_deletes(&file_scan_tasks[0].deletes, file_scan_tasks[0].schema_ref())
            .await
            .unwrap()
            .unwrap();

        let result = delete_filter
            .get_delete_vector(&file_scan_tasks[0])
            .unwrap();
        assert_eq!(result.lock().unwrap().len(), 12); // pos dels from pos del file 1 and 2

        let delete_filter = delete_file_loader
            .load_deletes(&file_scan_tasks[1].deletes, file_scan_tasks[1].schema_ref())
            .await
            .unwrap()
            .unwrap();

        let result = delete_filter
            .get_delete_vector(&file_scan_tasks[1])
            .unwrap();
        assert_eq!(result.lock().unwrap().len(), 8); // no pos dels for file 3
    }

    pub(crate) fn setup(table_location: &Path) -> Vec<FileScanTask> {
        let data_file_schema = Arc::new(Schema::builder().build().unwrap());
        let positional_delete_schema = create_pos_del_schema();

        let file_path_values = [
            vec![format!("{}/1.parquet", table_location.to_str().unwrap()); 8],
            vec![format!("{}/1.parquet", table_location.to_str().unwrap()); 8],
            vec![format!("{}/2.parquet", table_location.to_str().unwrap()); 8],
        ];
        let pos_values = [
            vec![0i64, 1, 3, 5, 6, 8, 1022, 1023],
            vec![0i64, 1, 3, 5, 20, 21, 22, 23],
            vec![0i64, 1, 3, 5, 6, 8, 1022, 1023],
        ];

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        for n in 1..=3 {
            let file_path_vals = file_path_values.get(n - 1).unwrap();
            let file_path_col = Arc::new(StringArray::from_iter_values(file_path_vals));

            let pos_vals = pos_values.get(n - 1).unwrap();
            let pos_col = Arc::new(Int64Array::from_iter_values(pos_vals.clone()));

            let positional_deletes_to_write =
                RecordBatch::try_new(positional_delete_schema.clone(), vec![
                    file_path_col.clone(),
                    pos_col.clone(),
                ])
                .unwrap();

            let file = File::create(format!(
                "{}/pos-del-{}.parquet",
                table_location.to_str().unwrap(),
                n
            ))
            .unwrap();
            let mut writer = ArrowWriter::try_new(
                file,
                positional_deletes_to_write.schema(),
                Some(props.clone()),
            )
            .unwrap();

            writer
                .write(&positional_deletes_to_write)
                .expect("Writing batch");

            // writer must be closed to write footer
            writer.close().unwrap();
        }

        // Helper to build a positional delete task with minimal fields
        let make_pos_del_task = |n: u8| FileScanTask {
            file_size_in_bytes: std::fs::metadata(format!(
                "{}/pos-del-{}.parquet",
                table_location.to_str().unwrap(),
                n
            ))
            .unwrap()
            .len(),
            start: 0,
            length: 0,
            record_count: None,
            data_file_path: format!("{}/pos-del-{}.parquet", table_location.to_str().unwrap(), n),
            referenced_data_file: None,
            data_file_content: DataContentType::PositionDeletes,
            data_file_format: DataFileFormat::Parquet,
            schema: data_file_schema.clone(),
            project_field_ids: vec![],
            predicate: None,
            deletes: vec![],
            sequence_number: 0,
            equality_ids: None,
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: false,
        };

        let file_scan_tasks = vec![
            FileScanTask {
                file_size_in_bytes: 0,
                start: 0,
                length: 0,
                record_count: None,
                data_file_path: format!("{}/1.parquet", table_location.to_str().unwrap()),
                referenced_data_file: None,
                data_file_content: DataContentType::Data,
                data_file_format: DataFileFormat::Parquet,
                schema: data_file_schema.clone(),
                project_field_ids: vec![],
                predicate: None,
                deletes: vec![make_pos_del_task(1).into(), make_pos_del_task(2).into()],
                sequence_number: 0,
                equality_ids: None,
                partition: None,
                partition_spec: None,
                name_mapping: None,
                case_sensitive: false,
            },
            FileScanTask {
                file_size_in_bytes: 0,
                start: 0,
                length: 0,
                record_count: None,
                data_file_path: format!("{}/2.parquet", table_location.to_str().unwrap()),
                referenced_data_file: None,
                data_file_content: DataContentType::Data,
                data_file_format: DataFileFormat::Parquet,
                schema: data_file_schema.clone(),
                project_field_ids: vec![],
                predicate: None,
                deletes: vec![make_pos_del_task(3).into()],
                sequence_number: 0,
                equality_ids: None,
                partition: None,
                partition_spec: None,
                name_mapping: None,
                case_sensitive: false,
            },
        ];

        file_scan_tasks
    }

    pub(crate) fn create_pos_del_schema() -> ArrowSchemaRef {
        let fields = vec![
            arrow_schema::Field::new("file_path", arrow_schema::DataType::Utf8, false)
                .with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    FIELD_ID_POSITIONAL_DELETE_FILE_PATH.to_string(),
                )])),
            arrow_schema::Field::new("pos", arrow_schema::DataType::Int64, false).with_metadata(
                HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    FIELD_ID_POSITIONAL_DELETE_POS.to_string(),
                )]),
            ),
        ];
        Arc::new(arrow_schema::Schema::new(fields))
    }

    #[tokio::test]
    async fn test_build_equality_delete_predicate_case_sensitive() {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "Id", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .unwrap(),
        );

        // ---------- fake FileScanTask ----------
        let task = FileScanTask {
            file_size_in_bytes: 0,
            start: 0,
            length: 0,
            record_count: None,
            data_file_path: "data.parquet".to_string(),
            referenced_data_file: None,
            data_file_content: DataContentType::Data,
            data_file_format: crate::spec::DataFileFormat::Parquet,
            schema: schema.clone(),
            project_field_ids: vec![],
            predicate: None,
            deletes: vec![
                FileScanTask {
                    file_size_in_bytes: 1, // never read; this test fails before opening the file
                    start: 0,
                    length: 0,
                    record_count: None,
                    data_file_path: "eq-del.parquet".to_string(),
                    referenced_data_file: None,
                    data_file_content: DataContentType::EqualityDeletes,
                    data_file_format: crate::spec::DataFileFormat::Parquet,
                    schema: schema.clone(),
                    project_field_ids: vec![],
                    predicate: None,
                    deletes: vec![],
                    sequence_number: 0,
                    equality_ids: None,
                    partition: None,
                    partition_spec: None,
                    name_mapping: None,
                    case_sensitive: true,
                }
                .into(),
            ],
            sequence_number: 0,
            equality_ids: None,
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: true,
        };

        let filter = DeleteFilter::default();

        // ---------- insert equality delete predicate ----------
        let pred = Reference::new("id").equal_to(Datum::long(10));

        filter
            .try_start_eq_del_load("eq-del.parquet")
            .unwrap()
            .finish(pred);

        // ---------- should FAIL ----------
        let result = filter.build_equality_delete_predicate(&task).await;

        assert!(
            result.is_err(),
            "case_sensitive=true should fail when column case mismatches"
        );
    }
}
