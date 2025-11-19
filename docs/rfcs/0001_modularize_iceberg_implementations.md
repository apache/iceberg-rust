# RFC: Modularize `iceberg` Implementations

## Background

Issue #1819 highlighted that the current `iceberg` crate mixes the Iceberg protocol abstractions (catalog/table/plan/transaction) with concrete runtime, storage, and execution implementations (Tokio runtime wrappers, opendal-based `FileIO`, Arrow readers, DataFusion helpers, etc.). This makes the crate heavy, couples unrelated dependencies, and prevents users from bringing their own engines or storage stacks.

After recent maintainer discussions we agreed on two principles:
1. The `iceberg` crate itself remains the single source of truth for all protocol traits and data structures.
2. All concrete integrations (Tokio runtime, opendal `FileIO`, Arrow/DataFusion executors, catalog adapters, etc.) move out of `iceberg` into dedicated companion crates. Users who need a ready-made execution path can depend on those crates (for example `iceberg-datafusion`) while users building custom stacks can depend solely on `iceberg`.

This RFC describes the plan to slim down `iceberg` into a pure protocol crate and to reorganize the workspace around pluggable companion crates.

## Goals and Scope

- **Keep `iceberg` as the protocol crate**: it exposes all traits (`Catalog`, `Table`, `Transaction`, `FileIO`, `Runtime`, `ScanPlan`, etc.) plus metadata/plan logic, but no longer ships concrete runtimes or storage adapters.
- **Detach embedded implementations**: move opendal-based IO, Tokio runtime helpers, Arrow converters, and similar code into separate crates under `crates/fileio/*`, `crates/runtime/*`, `crates/engine/*`, or existing integration crates.
- **Enable composable combinations**: users assemble the stack they need by combining `iceberg` with specific implementation crates (e.g., `iceberg-fileio-opendal`, `iceberg-runtime-tokio`, `iceberg-engine-arrow`, `iceberg-datafusion`).
- **Minimize breaking surfaces**: trait APIs stay in `iceberg`; downstream crates only adjust their dependency graph.

Out of scope: changing the Iceberg table specification or rewriting catalog adapters’ external behavior.

## Architecture Overview

### Workspace Layout

```
crates/
  iceberg/                # core traits, metadata, planning, transactions
  fileio/
    opendal/             # e.g. `iceberg-fileio-opendal`
    fs/                  # other FileIO implementations
  runtime/
    tokio/               # e.g. `iceberg-runtime-tokio`
    smol/
  engine/
    arrow/               # Arrow executor & schema helpers
  catalog/*              # catalog adapters (REST, HMS, Glue, etc.)
  integrations/
    datafusion/          # combines core + implementations for DF
    cache-moka/
    playground/
```

- `crates/iceberg` no longer depends on opendal, Tokio, Arrow, or DataFusion.
- Implementation crates depend on `iceberg` to get the trait surfaces they implement.
- Higher-level crates (e.g., `iceberg-datafusion`) pull in the required runtime/FileIO/executor crates and expose an opinionated combination.

### Core Trait Surfaces (within `iceberg`)

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

- `FileRead` / `FileWrite` remain Iceberg-specific traits (range reads, metrics hooks, abort/commit) and live inside `iceberg`.
- Concrete implementations (opendal, local FS, custom stores) live in companion crates and return trait objects.

#### Runtime

```rust
pub trait Runtime: Send + Sync + 'static {
    type JoinHandle<T>: Future<Output = T> + Send + 'static;

    fn spawn<F, T>(&self, fut: F) -> Self::JoinHandle<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static;

    fn sleep(&self, dur: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>>;
}
```

- The trait lives in `iceberg`; crates like `iceberg-runtime-tokio` implement it and expose constructors.

#### Catalog / Table / Transaction / Scan

- All existing traits and data structures remain in `crates/iceberg`.
- `TableScan` continues to emit pure plan descriptors; executors interpret them.
- `Transaction` uses injected `Runtime` for retry/backoff but otherwise stays unchanged.

### Usage Modes

- **Custom stacks**: depend solely on `iceberg` plus self-authored implementations that satisfy the traits.
- **Pre-built stacks**: depend on crates such as `iceberg-datafusion` that bundle `iceberg` with `iceberg-runtime-tokio`, `iceberg-fileio-opendal`, and `iceberg-engine-arrow` (and expose higher-level APIs).
- `iceberg` itself does not re-export any of the companion crates; users compose them explicitly.

## Migration Plan

1. **Phase 1 – Slim down `crates/iceberg`**
   - Remove direct dependencies on opendal, Tokio, Arrow, and DataFusion from `iceberg`.
   - Move the concrete implementations into new crates (while keeping the same code initially).

2. **Phase 2 – Stabilize trait surfaces**
   - Finalize dyn-friendly `FileIO`, `FileRead`, `FileWrite`, and `Runtime` traits inside `iceberg`.
   - Provide shims (deprecation warnings) for any APIs that previously returned concrete types (e.g., `InputFile`, `OutputFile`) so downstream integrations can migrate.

3. **Phase 3 – Arrow/execution extraction**
   - Relocate `crates/iceberg/src/arrow/*` and related helpers into `crates/engine/arrow`.
   - Update `iceberg-datafusion` to consume the new executor crate plus whichever runtime/fileio implementations it needs.

4. **Phase 4 – Catalog and integration updates**
   - Ensure catalog crates compile against the new dependency graph (they now depend on `iceberg` plus whichever FileIO/runtime crates they require).
   - Provide documentation/examples showing how to assemble `iceberg` with the desired implementations.

5. **Phase 5 – Documentation & release**
   - Publish a migration guide explaining the new crate layout and how to replace previous helper APIs with the new building blocks.
   - Tag a breaking-release (e.g., 0.8.0) and coordinate with downstream projects (`iceberg-datafusion`, Python bindings, etc.).

## Compatibility

- Existing users who depended on `iceberg`’s built-in Arrow/Tokio/FileIO helpers must now add explicit dependencies on the relevant implementation crates (`iceberg-fileio-opendal`, `iceberg-runtime-tokio`, `iceberg-engine-arrow`, or `iceberg-datafusion`).
- Users implementing custom stacks continue to depend on `iceberg` only; they implement the required traits themselves.
- Tests and examples that previously lived inside `crates/iceberg` move to whichever crate now hosts the implementation.

## Risks and Mitigations

| Risk | Description | Mitigation |
| ---- | ----------- | ---------- |
| Discoverability | Users may not know which combination of crates to pick | Provide clear docs linking to `iceberg-datafusion`, `iceberg-fileio-opendal`, etc., plus template examples |
| Trait churn | Adjusting `FileIO`/`Runtime` APIs could break downstream code | Introduce deprecation shims and publish migration notes before removal |
| Duplicate dependencies | Some crates might accidentally pull multiple implementations | Document recommended combos; enforce mutually exclusive features where practical |

## Open Questions

1. How do we version the companion crates relative to `iceberg` to signal compatibility (same version numbers vs. independent)?
2. What is the timeline for removing deprecated APIs (e.g., `Table::scan().to_arrow()`), and do we provide temporary re-exports via `iceberg-datafusion`?

## Conclusion

By keeping `iceberg` focused on core traits and moving concrete implementations into companion crates, we reduce unnecessary coupling, make it easier for the community to plug in custom runtimes and storage layers, and still provide ready-to-use stacks such as `iceberg-datafusion`. This RFC outlines the restructuring needed to accomplish that while keeping the trait surfaces centralized in `iceberg`.
