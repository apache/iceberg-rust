<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# RFC: Extract `iceberg-kernel` for Pluggable Execution Layers

## Background

Issue #1819 proposes decoupling the protocol/metadata/plan logic that currently lives inside the `iceberg` crate so that it can serve as a reusable “kernel,” similar to the approach taken by [delta-kernel-rs](https://github.com/delta-io/delta-kernel-rs). Today the `iceberg` crate simultaneously exposes the public trait surface and the default engine (Tokio runtime, opendal-backed FileIO, Arrow readers, etc.). This tight coupling makes it difficult for downstream projects to embed Iceberg metadata while providing their own storage, runtime, or execution stack.

## Goals and Scope

- **Full read & write coverage**: the kernel must contain every protocol component required for both scan planning and transactional writes (append, rewrite, commit, etc.).
- **No default runtime dependency**: the kernel defines a `Runtime` trait instead of depending on Tokio or Smol.
- **No default storage dependency**: the kernel defines `FileIO` traits only; concrete implementations (for example `iceberg-fileio-opendal`) live in dedicated crates.
- **Stable facade for existing users**: the top-level `iceberg` crate continues to expose the familiar API by re-exporting the kernel plus a default engine feature.

Out of scope: changes to the Iceberg table specification or rewriting catalog adapters.

## Architecture Overview

### Workspace Layout

```
crates/
  kernel/                 # new: pure protocols & planning logic
    spec/ expr/ catalog/ table/ transaction/ scan/ runtime_api
    io/traits.rs          # FileIO traits (no opendal)
  fileio/
    opendal/             # e.g. `iceberg-fileio-opendal`
    fs/                  # other FileIO implementations
  runtime/
    tokio/               # e.g. `iceberg-runtime-tokio`
    smol/
  iceberg/                # facade re-exporting kernel + default engine
  catalog/*               # depend on kernel (+ chosen FileIO/Runtime crates)
  integrations/*          # e.g. datafusion using facade or composing crates
```

### Trait Surfaces

#### FileIO

```rust
pub struct FileMetadata {
    pub size: u64,
    ...
}

pub type FileReader = Box<dyn FileRead>;

#[async_trait::async_trait]
pub trait FileRead: Send + Sync + 'static {
    async fn read(&self, range: Range<u64>) -> Result<Bytes>;
}

pub type FileWriter = Box<dyn FileWrite>;

#[async_trait::async_trait]
pub trait FileWrite: Send + Unpin + 'static {
    async fn write(&mut self, bs: Bytes) -> Result<()>;
    async fn close(&mut self) -> Result<FileMetadata>;
}

pub type StorageFactory = fn(attrs: HashMap<String, String> -> Result<Arc<dyn Storage>>);

#[async_trait::async_trait]
pub trait Storage: Clone + Send + Sync {
    async fn reader(&self, path: &str) -> Result<FileReader>;
    async fn writer(&self, path: &str) -> Result<FileWriter>;
    async fn delete(&self, path: &str) -> Result<()>;
    async fn exists(&self, path: &str) -> Result<bool>;

    ...
}

pub struct FileIO {
    registry: DashMap<String, StorageFactory>,
}

impl FileIO {
    fn register(scheme: &str, factory: StorageFactory);

    async fn read(path: &str) -> Result<Bytes>;
    async fn reader(path: &str) -> Result<FileReader>;
    async fn write(path: &str, bs: Bytes) -> Result<FileMetadata>;
    async fn writer(path: &str) -> Result<FileWriter>;

    async fn delete(&self, path: &str) -> Result<()>;
    ...
}
```

- The kernel only defines the trait and error types.
- `iceberg-fileio-opendal` (new crate) ships an opendal-based implementation; other backends can publish their own crates.

#### Runtime

```rust
pub trait Runtime: Send + Sync + 'static {
    type JoinHandle<T>: Future<Output = T> + Send + 'static;

    fn spawn<F, T>(&self, fut: F) -> Self::JoinHandle<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static;

    fn spawn_blocking<F, T>(&self, f: F) -> Self::JoinHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static;

    fn sleep(&self, dur: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>>;
}
```

- `TableScan` planning, metadata refresh, and `Transaction::commit` depend only on this trait.
- Crates such as `iceberg-runtime-tokio` provide concrete schedulers; consumers pick whichever runtime crate fits their stack.

#### Catalog / Table / Transaction

- The `Catalog` trait moves into the kernel and returns lightweight `TableHandle` objects (metadata + FileIO + Runtime).
- `TableHandle` no longer embeds Arrow helpers; Arrow-specific logic lives in engine crates.
- Transactions and their actions remain in the kernel, but rely on injected `Runtime` for retries/backoff.

#### Scan / Planner

- The kernel produces pure `TableScanPlan` descriptions (manifests, data-files, predicates, task graph).
- Engines provide executors (e.g., `ArrowExecutor`) that transform plans into record batches or other runtime-specific artifacts.

### Facade Behavior

- The top-level `iceberg` crate becomes a facade (`pub use iceberg_kernel::*`) that enables a *composition* of default crates (e.g. `iceberg-runtime-tokio`, `iceberg-fileio-opendal`, and a reference executor) behind feature flags.
- Existing convenience APIs (`Table::scan().to_arrow()`, `MemoryCatalog`, etc.) stay available but internally assemble the kernel with those default building blocks.

## Migration Plan

1. **Phase 1 – Create the kernel crate**
   - Add `crates/kernel` and move `spec`, `expr`, `catalog`, `table`, `transaction`, `scan`, and supporting modules.
   - Introduce temporary shim modules in the facade so existing imports keep working (mark them deprecated).

2. **Phase 2 – Abstract runtime & IO**
   - Define the `Runtime` and `FileIO` traits inside the kernel.
   - Remove direct `tokio`/`opendal` dependencies from kernel modules.
   - Introduce standalone crates (`iceberg-runtime-tokio`, `iceberg-fileio-opendal`, etc.) that implement the new traits.

3. **Phase 3 – Detach Arrow/execution**
   - Move `arrow` helpers and `ArrowReaderBuilder` into a reference executor crate (e.g. `iceberg-engine-arrow`).
   - Update the DataFusion integration to depend on the facade or directly compose kernel + runtime + fileio + executor crates.

4. **Phase 4 – Catalog & integration updates**
   - Point catalog crates and other integrations to the kernel interfaces; depend on specific FileIO/Runtime crates only when required.
   - Keep `iceberg-catalog-loader` kernel-only so users can inject their preferred combinations.

5. **Phase 5 – Release & documentation**
   - Finish the split within the 0.y.z series, provide an upgrade guide, and add kernel acceptance tests to guarantee trait stability.

## Compatibility

- Users who stick with the `iceberg` facade keep their existing API surface; the facade simply composes kernel + default runtime + default FileIO + reference executor under the hood.
- Advanced integrators can depend solely on `iceberg-kernel` and mix in whichever `FileIO`, `Runtime`, and executor crates they need (or author their own).
- CI keeps running the current integration tests and adds kernel-specific acceptance suites.

## Risks and Mitigations

| Risk | Description | Mitigation |
| ---- | ----------- | ---------- |
| Trait churn | Updating catalog/scan traits could break downstream crates | Maintain shim modules, use `#[deprecated]` windows, and document migration steps |
| Generic complexity | New traits may introduce complicated type bounds | Prefer `Arc<dyn Trait>` and `BoxFuture` to keep signatures manageable |
| Documentation gap | Users may not know which engine to pick | Publish new docs, diagrams, and “custom engine” tutorials alongside the split |

## Open Questions

1. Should the kernel expose any Arrow helpers, or should every Arrow-specific function live exclusively in engine crates?
2. Do we need a `ScanExecutor` trait inside the kernel for non-Arrow consumers?

## Conclusion

Extracting `iceberg-kernel` plus a pluggable engine layer lets the project:

- Offer a lightweight, embeddable implementation of the Iceberg protocol,
- Enable external engines (DataFusion, Spark Connect, custom services) to reuse Iceberg metadata without inheriting specific runtime/storage dependencies,
