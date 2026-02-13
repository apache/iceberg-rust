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

use std::future::Future;
use std::sync::{Arc, Mutex};

use opendal::raw::*;

use super::refreshable_storage::RefreshableOpenDalStorage;
use crate::Result;

/// An OpenDAL accessor that wraps another accessor and retries on PermissionDenied
/// after refreshing credentials.
///
/// Each instance has its own inner accessor and shares credential state with
/// other accessors via `Arc<RefreshableOpenDalStorage>`. Credentials are only
/// refreshed when an operation fails with PermissionDenied, not proactively.
///
/// Concurrency: if multiple accessors hit PermissionDenied simultaneously, only
/// one will call the external credential loader (via double-checked locking on
/// `RefreshableOpenDalStorage::refresh_on_permission_denied`). The others will
/// detect the version bump and simply rebuild their accessor from the already-refreshed
/// credentials.
pub(crate) struct RefreshableAccessor {
    /// The current backend's accessor paired with the credential version it was built from.
    inner: Mutex<(Accessor, u64)>,

    /// The full original path (e.g. "memory:/some-file") used to create the operator.
    /// Needed to rebuild the accessor after credential refresh.
    original_path: String,

    /// Shared storage holding credentials and configuration
    storage: Arc<RefreshableOpenDalStorage>,
}

impl RefreshableAccessor {
    pub(crate) fn new(
        accessor: Accessor,
        credential_version: u64,
        original_path: String,
        storage: Arc<RefreshableOpenDalStorage>,
    ) -> Self {
        Self {
            inner: Mutex::new((accessor, credential_version)),
            original_path,
            storage,
        }
    }

    /// Get the current inner accessor and its credential version.
    fn get_accessor(&self) -> (Accessor, u64) {
        let guard = self.inner.lock().unwrap();
        guard.clone()
    }

    /// Rebuild the inner accessor from the shared storage after a credential refresh.
    ///
    /// Uses `original_path` (the full path passed to `refreshable_create_operator`)
    /// to call `create_operator` on the refreshed `inner_storage`.
    fn rebuild_accessor(&self, new_version: u64) -> Result<Accessor> {
        let storage_guard = self.storage.lock_inner_storage();
        let (operator, _) = storage_guard.create_operator(&self.original_path)?;
        drop(storage_guard);

        let new_accessor = operator.into_inner();
        *self.inner.lock().unwrap() = (new_accessor.clone(), new_version);
        Ok(new_accessor)
    }

    /// Run an operation with automatic retry on PermissionDenied after credential refresh.
    ///
    /// 1. Gets the current accessor (no refresh) and runs the operation.
    /// 2. If it fails with PermissionDenied, calls `refresh_on_permission_denied`
    ///    with the accessor's credential version.
    /// 3. If credentials were refreshed (by us or another concurrent accessor),
    ///    rebuilds our accessor and retries the operation once.
    /// 4. Otherwise, returns the original error.
    async fn with_credential_retry<F, Fut, T>(&self, op: F) -> opendal::Result<T>
    where
        F: Fn(Accessor) -> Fut,
        Fut: Future<Output = opendal::Result<T>>,
    {
        let (accessor, version) = self.get_accessor();
        let result = op(accessor).await;

        match result {
            Err(err) if err.kind() == opendal::ErrorKind::PermissionDenied => {
                let new_version = self
                    .storage
                    .refresh_on_permission_denied(version)
                    .await
                    .map_err(|e| {
                        opendal::Error::new(
                            opendal::ErrorKind::PermissionDenied,
                            format!(
                                "Operation failed with PermissionDenied and credential \
                                 refresh also failed: {e}"
                            ),
                        )
                        .set_source(err)
                    })?;

                let new_accessor = self.rebuild_accessor(new_version).map_err(|e| {
                    opendal::Error::new(
                        opendal::ErrorKind::Unexpected,
                        "Failed to rebuild accessor after credential refresh",
                    )
                    .set_source(e)
                })?;
                op(new_accessor).await
            }
            other => other,
        }
    }
}

impl std::fmt::Debug for RefreshableAccessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RefreshableAccessor").finish()
    }
}

impl Access for RefreshableAccessor {
    type Reader = oio::Reader;
    type Writer = oio::Writer;
    type Lister = oio::Lister;
    type Deleter = oio::Deleter;

    fn info(&self) -> Arc<AccessorInfo> {
        let info_guard = self.storage.lock_cached_info();
        if let Some(info) = info_guard.as_ref() {
            Arc::clone(info)
        } else {
            drop(info_guard);
            AccessorInfo::default().into()
        }
    }

    async fn stat(&self, path: &str, args: OpStat) -> opendal::Result<RpStat> {
        self.with_credential_retry(|accessor| {
            let args = args.clone();
            async move { accessor.stat(path, args).await }
        })
        .await
    }

    async fn read(&self, path: &str, args: OpRead) -> opendal::Result<(RpRead, Self::Reader)> {
        self.with_credential_retry(|accessor| {
            let args = args.clone();
            async move { accessor.read(path, args).await }
        })
        .await
    }

    async fn write(&self, path: &str, args: OpWrite) -> opendal::Result<(RpWrite, Self::Writer)> {
        self.with_credential_retry(|accessor| {
            let args = args.clone();
            async move { accessor.write(path, args).await }
        })
        .await
    }

    async fn delete(&self) -> opendal::Result<(RpDelete, Self::Deleter)> {
        self.with_credential_retry(|accessor| async move { accessor.delete().await })
            .await
    }

    async fn list(&self, path: &str, args: OpList) -> opendal::Result<(RpList, Self::Lister)> {
        self.with_credential_retry(|accessor| {
            let args = args.clone();
            async move { accessor.list(path, args).await }
        })
        .await
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> opendal::Result<RpCreateDir> {
        self.with_credential_retry(|accessor| {
            let args = args.clone();
            async move { accessor.create_dir(path, args).await }
        })
        .await
    }

    async fn rename(&self, from: &str, to: &str, args: OpRename) -> opendal::Result<RpRename> {
        self.with_credential_retry(|accessor| {
            let args = args.clone();
            async move { accessor.rename(from, to, args).await }
        })
        .await
    }
}

/// Tests for the `with_credential_retry` logic in `RefreshableAccessor`.
///
/// `with_credential_retry` works as follows:
/// 1. Gets the current accessor (no refresh) and runs the operation.
/// 2. On `PermissionDenied`, calls `refresh_on_permission_denied` with the
///    accessor's credential version.
/// 3. If credentials were refreshed, rebuilds the accessor and retries once.
/// 4. Otherwise, returns the original error.
///
/// To test this, we inject a `FailingAccessor` (returns a configurable error on `stat`)
/// as the initial inner accessor, while the shared storage's `inner_storage` is a real
/// memory backend. When credential refresh triggers a rebuild, the accessor switches
/// from `FailingAccessor` to the real memory backend — observable as a change in error
/// kind (e.g. `PermissionDenied` → `NotFound`).
///
/// A `SequenceLoader` controls exactly which loader calls trigger refresh (`Some`) and
/// which don't (`None`), so we can test each branch of the retry logic.
#[cfg(all(test, feature = "storage-memory"))]
mod tests {
    use std::collections::{HashMap, VecDeque};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::io::refreshable_storage::RefreshableOpenDalStorageBuilder;
    use crate::io::{StorageCredential, StorageCredentialsLoader};

    // --- Test helpers ---

    /// Returns pre-configured credentials in order from a `VecDeque`. Tracks call count.
    struct SequenceLoader {
        responses: Mutex<VecDeque<StorageCredential>>,
        call_count: AtomicUsize,
    }

    impl std::fmt::Debug for SequenceLoader {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("SequenceLoader").finish()
        }
    }

    impl SequenceLoader {
        fn new(responses: Vec<StorageCredential>) -> Self {
            Self {
                responses: Mutex::new(VecDeque::from(responses)),
                call_count: AtomicUsize::new(0),
            }
        }

        fn call_count(&self) -> usize {
            self.call_count.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl StorageCredentialsLoader for SequenceLoader {
        async fn load_credentials(&self, _location: &str) -> crate::Result<StorageCredential> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            let mut responses = self.responses.lock().unwrap();
            Ok(responses.pop_front().unwrap_or_else(dummy_credential))
        }
    }

    /// `Access` impl that always returns a configurable `opendal::ErrorKind` on `stat`.
    /// All other methods return `Unexpected` (not expected to be called by these tests).
    struct FailingAccessor {
        error_kind: opendal::ErrorKind,
        info: Arc<AccessorInfo>,
    }

    impl std::fmt::Debug for FailingAccessor {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("FailingAccessor").finish()
        }
    }

    impl FailingAccessor {
        fn new(error_kind: opendal::ErrorKind, info: Arc<AccessorInfo>) -> Self {
            Self { error_kind, info }
        }
    }

    impl Access for FailingAccessor {
        type Reader = oio::Reader;
        type Writer = oio::Writer;
        type Lister = oio::Lister;
        type Deleter = oio::Deleter;

        fn info(&self) -> Arc<AccessorInfo> {
            Arc::clone(&self.info)
        }

        async fn stat(&self, _path: &str, _args: OpStat) -> opendal::Result<RpStat> {
            Err(opendal::Error::new(self.error_kind, "test error"))
        }

        async fn read(
            &self,
            _path: &str,
            _args: OpRead,
        ) -> opendal::Result<(RpRead, Self::Reader)> {
            Err(opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                "not implemented in test",
            ))
        }

        async fn write(
            &self,
            _path: &str,
            _args: OpWrite,
        ) -> opendal::Result<(RpWrite, Self::Writer)> {
            Err(opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                "not implemented in test",
            ))
        }

        async fn delete(&self) -> opendal::Result<(RpDelete, Self::Deleter)> {
            Err(opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                "not implemented in test",
            ))
        }

        async fn list(
            &self,
            _path: &str,
            _args: OpList,
        ) -> opendal::Result<(RpList, Self::Lister)> {
            Err(opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                "not implemented in test",
            ))
        }

        async fn create_dir(
            &self,
            _path: &str,
            _args: OpCreateDir,
        ) -> opendal::Result<RpCreateDir> {
            Err(opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                "not implemented in test",
            ))
        }

        async fn rename(
            &self,
            _from: &str,
            _to: &str,
            _args: OpRename,
        ) -> opendal::Result<RpRename> {
            Err(opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                "not implemented in test",
            ))
        }
    }

    fn dummy_credential() -> StorageCredential {
        StorageCredential {
            prefix: "memory:/".to_string(),
            config: HashMap::from([("dummy".to_string(), "cred".to_string())]),
        }
    }

    /// Builds a `RefreshableAccessor` whose initial inner accessor is a `FailingAccessor`
    /// (returns `error_kind` on stat), but whose shared storage is a real memory backend.
    /// After credential refresh + rebuild, the accessor switches from `FailingAccessor`
    /// to the real memory backend.
    fn build_refreshable_storage_and_accessor(
        loader: Arc<dyn StorageCredentialsLoader>,
        error_kind: opendal::ErrorKind,
    ) -> RefreshableAccessor {
        let storage = RefreshableOpenDalStorageBuilder::new()
            .scheme("memory".to_string())
            .base_props(HashMap::new())
            .credentials_loader(Arc::clone(&loader))
            .build()
            .expect("Failed to build storage");

        let info = {
            let inner = storage.lock_inner_storage();
            let path = "memory:/dummy".to_string();
            let (op, _) = inner.create_operator(&path).unwrap();
            op.into_inner().info()
        };

        *storage.lock_cached_info() = Some(Arc::clone(&info));

        let version = storage.credential_version();
        let failing_accessor: Accessor = Arc::new(FailingAccessor::new(error_kind, info));
        RefreshableAccessor::new(
            failing_accessor,
            version,
            "memory:/dummy".to_string(),
            storage,
        )
    }

    // --- Tests ---

    /// Core retry scenario: when temporary credentials expire mid-operation,
    /// the accessor should transparently refresh and retry.
    ///
    /// Flow:
    /// 1. `get_accessor` → no refresh → FailingAccessor used
    /// 2. `stat` → PermissionDenied
    /// 3. `refresh_on_permission_denied` → loader call #1 → do_refresh
    /// 4. `rebuild_accessor` → memory accessor used
    /// 5. Memory backend `stat("nonexistent")` → NotFound (not PermissionDenied)
    #[tokio::test]
    async fn test_retry_on_permission_denied_with_successful_refresh() {
        let loader = Arc::new(SequenceLoader::new(vec![dummy_credential()]));

        let accessor = build_refreshable_storage_and_accessor(
            Arc::clone(&loader) as _,
            opendal::ErrorKind::PermissionDenied,
        );

        let result = accessor.stat("nonexistent", OpStat::new()).await;

        // The retry should have happened — the error should be NotFound
        // (from the memory backend), not PermissionDenied
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(
            err.kind(),
            opendal::ErrorKind::NotFound,
            "Expected NotFound after retry, got {:?}",
            err.kind()
        );

        // Only 1 loader call: the retry on PermissionDenied
        assert_eq!(loader.call_count(), 1);
    }

    /// Only PermissionDenied should trigger credential retry. Other errors (network,
    /// not-found, etc.) should not — retrying with fresh credentials wouldn't help.
    ///
    /// Flow:
    /// 1. `get_accessor` → no refresh → FailingAccessor → NotFound
    /// 2. `with_credential_retry` sees NotFound → no retry → returns error immediately
    #[tokio::test]
    async fn test_non_permission_denied_error_is_not_retried() {
        let loader = Arc::new(SequenceLoader::new(vec![]));

        let accessor = build_refreshable_storage_and_accessor(
            Arc::clone(&loader) as _,
            opendal::ErrorKind::NotFound,
        );

        let result = accessor.stat("nonexistent", OpStat::new()).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), opendal::ErrorKind::NotFound);

        // No loader calls at all — only PermissionDenied triggers refresh
        assert_eq!(loader.call_count(), 0);
    }

    /// When multiple concurrent callers hit PermissionDenied, only one should
    /// call the external credential loader. The others should detect the version
    /// bump and skip the loader call.
    #[tokio::test]
    async fn test_concurrent_permission_denied_calls_loader_only_once() {
        let loader = Arc::new(SequenceLoader::new(vec![dummy_credential()]));

        let storage = RefreshableOpenDalStorageBuilder::new()
            .scheme("memory".to_string())
            .base_props(HashMap::new())
            .credentials_loader(Arc::clone(&loader) as _)
            .build()
            .expect("Failed to build storage");

        let version = storage.credential_version();

        // Spawn 10 concurrent refresh_on_permission_denied calls with the same version
        let mut handles = Vec::new();
        for _ in 0..10 {
            let storage = Arc::clone(&storage);
            handles.push(tokio::spawn(async move {
                storage.refresh_on_permission_denied(version).await
            }));
        }

        for handle in handles {
            let new_version = handle.await.unwrap().unwrap();
            assert_eq!(new_version, 1, "Version should be 1 after one refresh");
        }

        // Only 1 loader call should have been made
        assert_eq!(loader.call_count(), 1);
    }
}
