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

//! Serialization helpers and runtime bridges shared by the Ballista Iceberg
//! logical and physical extension codecs.
//!
//! The central problem this module solves is that Ballista serializes physical
//! and logical plans to ship them to remote nodes, but the Iceberg plan nodes
//! hold live, non-serializable state (an `Arc<dyn Catalog>` and a `Table` with
//! an open `FileIO`). We side-step this by serializing only the minimal,
//! self-contained [`IcebergCatalogConfig`] plus the [`TableIdent`], and then
//! *reconstructing* the catalog and table on the receiving node by building the
//! catalog from the config and loading the table from it.
//!
//! Reconstruction is asynchronous (catalog clients and table loads do I/O) but
//! the codec entry points are synchronous, so [`block_on`] bridges the two.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::{Arc, LazyLock, Mutex};

use datafusion::common::DataFusionError;
use iceberg::table::Table;
use iceberg::{Catalog, TableIdent};
use iceberg_datafusion::IcebergCatalogConfig;
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use serde::{Deserialize, Serialize};

/// Converts an arbitrary error into a [`DataFusionError`].
pub(crate) fn to_df_err<E: std::error::Error + Send + Sync + 'static>(e: E) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

/// Runs an async future to completion from a synchronous context, whatever
/// runtime (if any) the caller happens to be on.
///
/// - **Multi-threaded runtime** (the normal case on a Ballista executor or
///   scheduler): [`tokio::task::block_in_place`] hands this worker thread back
///   to the scheduler while we block on `fut`, so the rest of the runtime keeps
///   making progress.
/// - **Current-thread runtime**: blocking the sole worker is not allowed
///   (`block_in_place` panics and re-entering `block_on` deadlocks), so `fut`
///   runs on a dedicated thread with its own runtime and we wait for it.
/// - **No runtime** (e.g. some unit tests): a temporary current-thread runtime
///   on this thread is enough; no extra thread needed.
pub(crate) fn block_on<F>(fut: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    use tokio::runtime::{Handle, RuntimeFlavor};

    match Handle::try_current() {
        Ok(handle) if matches!(handle.runtime_flavor(), RuntimeFlavor::MultiThread) => {
            tokio::task::block_in_place(move || handle.block_on(fut))
        }
        // Inside a single-threaded (or otherwise non-multi-thread) runtime we
        // must get off the runtime's worker thread entirely.
        Ok(_) => run_on_dedicated_thread(fut),
        Err(_) => build_temp_runtime().block_on(fut),
    }
}

/// Runs `fut` to completion on a freshly spawned OS thread with its own
/// current-thread runtime. Used when the caller is already on a runtime whose
/// worker thread we are not allowed to block.
fn run_on_dedicated_thread<F>(fut: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    std::thread::scope(|scope| {
        scope
            .spawn(|| build_temp_runtime().block_on(fut))
            .join()
            .expect("iceberg catalog access thread panicked")
    })
}

fn build_temp_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build temporary tokio runtime for iceberg catalog access")
}

/// Whether this thread is on a durable (multi-thread) tokio runtime — one that
/// outlives the current call.
///
/// This is exactly the case where [`block_on`] runs the future on the caller's
/// own long-lived runtime (via `block_in_place`). On a current-thread runtime,
/// or with no runtime at all, `block_on` instead builds a temporary runtime that
/// is dropped when the call returns — so anything bound to it (a catalog's
/// HTTP/connection pool) must not be cached and reused later.
fn on_durable_runtime() -> bool {
    use tokio::runtime::{Handle, RuntimeFlavor};

    matches!(
        Handle::try_current().map(|h| h.runtime_flavor()),
        Ok(RuntimeFlavor::MultiThread)
    )
}

/// Process-wide cache of reconstructed catalogs, keyed by their config.
///
/// Building a catalog client (and its underlying HTTP/connection pool) is
/// relatively expensive, and the codec may decode many plan nodes that share
/// one catalog, so we cache by config. The cache only serves callers on a
/// durable runtime (see [`on_durable_runtime`]); on an ephemeral runtime
/// `get_catalog` bypasses it so a catalog can never outlive the runtime its
/// connection pool is bound to.
static CATALOGS: LazyLock<Mutex<HashMap<String, Arc<dyn Catalog>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn catalog_cache_key(config: &IcebergCatalogConfig) -> String {
    // BTreeMap gives a stable ordering for the props so the key is deterministic.
    let props: BTreeMap<_, _> = config.props.iter().collect();
    format!("{}|{}|{:?}", config.r#type, config.name, props)
}

/// Builds a catalog from its config.
///
/// The catalog type is resolved through [`iceberg_catalog_loader`], so any
/// catalog it supports (`rest`, `sql`, `glue`, `hms`, `s3tables`) works here.
/// Storage is provided by [`OpenDalResolvingStorageFactory`], which picks the
/// object-store backend (S3, GCS, Azure, local fs, …) from each file's path
/// scheme, configured from the same `props`. So a single code path covers every
/// catalog/storage combination the workspace supports.
pub(crate) async fn build_catalog(
    config: &IcebergCatalogConfig,
) -> Result<Arc<dyn Catalog>, DataFusionError> {
    iceberg_catalog_loader::load(&config.r#type)
        .map_err(to_df_err)?
        .with_storage_factory(Arc::new(OpenDalResolvingStorageFactory::new()))
        .load(config.name.clone(), config.props.clone())
        .await
        .map_err(to_df_err)
}

/// Returns a catalog built from `config`, cached when on a durable runtime.
pub(crate) fn get_catalog(
    config: &IcebergCatalogConfig,
) -> Result<Arc<dyn Catalog>, DataFusionError> {
    // On an ephemeral runtime the catalog would be bound to a runtime that is
    // dropped when this call returns, so bypass the cache entirely (both read and
    // write) and build a fresh, short-lived catalog instead.
    if !on_durable_runtime() {
        return block_on(build_catalog(config));
    }

    let key = catalog_cache_key(config);
    if let Some(catalog) = CATALOGS.lock().unwrap().get(&key) {
        return Ok(catalog.clone());
    }
    let catalog = block_on(build_catalog(config))?;
    CATALOGS.lock().unwrap().insert(key, catalog.clone());
    Ok(catalog)
}

/// Loads a fresh [`Table`] from the catalog described by `config`.
pub(crate) fn load_table(
    config: &IcebergCatalogConfig,
    ident: &TableIdent,
) -> Result<Table, DataFusionError> {
    let catalog = get_catalog(config)?;
    block_on(catalog.load_table(ident)).map_err(to_df_err)
}

// ---------------------------------------------------------------------------
// Wire format
// ---------------------------------------------------------------------------

/// Leading tag byte that frames every blob this crate's codecs produce.
///
/// Both the logical and physical codecs handle some nodes themselves and
/// delegate the rest to an inner Ballista codec. We write a tag for *both* branches. Decode
/// then dispatches on a value we always control, and the inner payload is nested
/// after the tag — never content-inspected. An unknown or missing tag is a hard
/// error instead of a silent misparse.
pub(crate) const TAG_DELEGATED: u8 = 0;
/// Tag for a payload owned by this crate's Iceberg codecs (JSON follows).
pub(crate) const TAG_ICEBERG: u8 = 1;

/// Serializable mirror of [`IcebergCatalogConfig`] (which is intentionally not
/// serde-aware in the iceberg crate to avoid a serde dependency there).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct CatalogConfigProto {
    pub r#type: String,
    pub name: String,
    // BTreeMap keeps a stable field order so encode→decode→encode round-trips.
    pub props: BTreeMap<String, String>,
}

impl From<&IcebergCatalogConfig> for CatalogConfigProto {
    fn from(c: &IcebergCatalogConfig) -> Self {
        Self {
            r#type: c.r#type.clone(),
            name: c.name.clone(),
            props: c.props.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        }
    }
}

impl From<CatalogConfigProto> for IcebergCatalogConfig {
    fn from(p: CatalogConfigProto) -> Self {
        IcebergCatalogConfig::new(p.r#type, p.name, p.props.into_iter().collect())
    }
}
