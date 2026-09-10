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

# RFC: File Format API for Apache Iceberg Rust

**Authors:** Kurtis C. Wright
**Last updated:** 2026-05-20

## Background

### Current state

The `iceberg` crate (version 0.9.1, Rust 1.92) is the core library of the Apache Iceberg Rust project. The crate depends directly on the `parquet` crate (with the `async` feature) and on the `arrow-*` crates. It has no feature flags today.

For data file writing, the crate provides `FileWriter` / `FileWriterBuilder` traits that are format-agnostic at the type level, but `ParquetWriterBuilder` and `ParquetWriter` are the only implementation. Higher-level writers (`DataFileWriterBuilder`, `EqualityDeleteFileWriterBuilder`) are generic over any `FileWriterBuilder`, but every instantiation uses Parquet.

For data file reading, `ArrowReaderBuilder` and `ArrowReader` are Parquet-specific despite the generic name. `TableScan::to_arrow` wires `ArrowReaderBuilder` as the only reader path. `FileScanTask` carries a `data_file_format` field, but the reader ignores it.

| Format  | Data file read | Data file write | Manifests |
|---------|----------------|-----------------|-----------|
| Parquet | Yes            | Yes             | No        |
| Avro    | No             | No              | Yes       |
| ORC     | No             | No              | No        |

A table containing ORC or Avro data files cannot be read by the `iceberg` crate today, even though both are valid per the Iceberg spec.

### Pain points

1. **No extension point for new formats.** Adding ORC means editing `ArrowReader` and threading format-specific logic through every layer.

2. **Parquet assumptions leak into generic code.** `ArrowReaderBuilder` exposes Parquet-specific options meaningless for other formats. The name conflates the in-memory representation with the on-disk format.

3. **No format-agnostic statistics.** Statistics computation is tightly coupled to Parquet's `Statistics` type.

4. **V3 types will need per-format serialization.** Variant uses shredding in Parquet, binary in ORC, unions in Avro. Without a format abstraction, each new type means new `match` arms everywhere.

5. **Arrow version coupling.** The core crate depends on specific `arrow-*` versions. Upgrading Arrow in `datafusion` or other integrations forces lockstep upgrades across the dependency graph.

### Prior work

The Java project shipped `FormatModel<D, S>` in February 2026 (PR [#12774](https://github.com/apache/iceberg/pull/12774)). Java's design uses two generic parameters (data type `D` and engine schema `S`) with a registry keyed by `(FileFormat, Class<?>)`. PyIceberg has an open proposal ([#3100](https://github.com/apache/iceberg-python/issues/3100)) that drops generics entirely, keying on file format alone.

This RFC proposes a composable three-layer architecture that separates the in-memory processing representation from the file format layer, using Rust's trait system for static dispatch within a layer and dynamic dispatch at layer boundaries. It aligns with the kernel architecture proposed in [#1817](https://github.com/apache/iceberg-rust/issues/1817) and the modularization tracked in [#1819](https://github.com/apache/iceberg-rust/issues/1819).

## Goals

1. Define a composable three-layer architecture where the file format, in-memory processing representation, and engine are independent axes of variation. A `DataBatch` trait defines the processing contract. `FormatReader` and `FormatWriter` traits bridge file formats to batch types. No layer imposes a conversion on another.

2. Remove hard-coded Parquet assumptions from scan and write orchestration.

3. Establish the crate architecture that allows the core `iceberg` kernel to be representation-agnostic, decoupling Arrow version pinning from the core library.

4. Provide interoperability with Java and Python Iceberg implementations at the conceptual level (same registry key semantics, same TCK coverage) while using Rust's trait system for zero-cost abstraction within a layer.

## Non-Goals

1. **Ship new format implementations.** This RFC lands the abstraction and a Parquet-with-Arrow implementation. ORC, Avro, Vortex, and Lance are follow-up work.

2. **Runtime library loading.** Rust has no stable ABI. No format under discussion requires this.

3. **Puffin support.** Puffin files have a different lifecycle and are handled separately.

4. **Redesign the writer trait hierarchy.** The existing `IcebergWriter` and `FileWriter` layering is sound. This RFC adds beneath it, not a replacement.

5. **Implement variant shredding.** The hooks are provided. Implementation depends on [#2188](https://github.com/apache/iceberg-rust/pull/2188).

6. **Complete crate separation.** This RFC establishes trait boundaries. Extraction is follow-up work per [#1817](https://github.com/apache/iceberg-rust/issues/1817) and [#1819](https://github.com/apache/iceberg-rust/issues/1819).

7. **Change the Iceberg table spec.** Rust-only API change.

8. **Modify manifest paths.** Manifests remain in Avro via existing code.

## Design

### Architecture

Three independent axes determine how data flows through the system:

| Axis | Controlled by | Determined when | Can change mid-session? |
|------|---------------|-----------------|-------------------------|
| **In-memory representation** | The engine embedding Iceberg (DataFusion, Spark, Comet) or the direct library user | Session start | No |
| **Processing operations** | The Iceberg kernel | N/A (always available) | N/A |
| **File format** | The table creator, stored in table metadata | Table creation | No (one format per table) |

An Iceberg session has one in-memory representation. A table has one data file format. A session may scan multiple tables with different formats, and a proposed multi-table transaction could span Parquet and ORC tables. In all cases, batches of data flow through three layers with no intermediate conversions:

```
┌─────────────────────────────────────────────────────────────────┐
│                       Engine Layer                                │
│  The engine's native data representation.                        │
│  Examples: Arrow RecordBatch, Vortex compressed arrays           │
│  The engine provides a type that implements DataBatch.            │
│  This choice is fixed for the session.                           │
└─────────────────────────────────────┬───────────────────────────┘
                                      │ (same concrete type throughout)
┌─────────────────────────────────────┼───────────────────────────┐
│                    Processing Layer                               │
│  Iceberg operations on in-memory data: expression evaluation,    │
│  partition transforms, schema evolution, metrics collection,     │
│  constant injection, delete application.                         │
│  Works with the concrete batch type chosen by the engine.        │
└─────────────────────────────────────┬───────────────────────────┘
                                      │ (same concrete type throughout)
┌─────────────────────────────────────┼───────────────────────────┐
│                    File Format Layer                              │
│  FormatReader/FormatWriter implementations read/write physical   │
│  files, producing/consuming the engine's batch type directly.    │
│  One implementation per (format, batch type) pair.               │
└─────────────────────────────────────┬───────────────────────────┘
                                      │
                           Physical Files
                      (Parquet, ORC, Avro, ...)
```

### Separation of responsibilities

Each implementor has a clearly defined contract. The kernel provides processing operations that format and engine developers do not need to reimplement.

**Format implementor** (for example, adding ORC):

| MUST implement | Handled by the kernel (unless the format opts in) |
|---|---|
| Decode requested columns from disk (projection is an I/O decision) | Residual row-level filtering |
| Encode batches to disk | Schema evolution (add columns, widen types, reorder) |
| Collect file-level metrics during write | Constant injection (metadata columns like `_file`, `_pos`) |
| Handle format-specific optimizations (variant shredding, statistics-based chunk skipping) | Delete application (merge-on-read) |
| Report what operations the reader already handled | Partition routing, file rolling, location generation |

A format reader MAY handle operations from the right column during I/O if it can do so more efficiently (for example, Parquet reading an `int32` column directly into `int64` for schema evolution). When it does, it reports what it handled via `ReadResult` so the kernel skips the redundant pass. The kernel handles anything the format does not.

**DataBatch implementor** (for example, adding GPU-native buffers):

| MUST implement | Gets for free |
|---|---|
| `filter` — evaluate a predicate against the data | All kernel orchestration |
| `project` — subset columns by field ID | All format readers that produce the type |
| `evolve_schema` — widen types, add columns with defaults | TCK to validate correctness |
| `inject_constants` — add metadata column values | |
| `column_metrics` — compute min/max/null/NaN stats | |

**Engine implementor** (for example, adding Comet):

| What you want | What you implement |
|---|---|
| Arrow works for your engine | Nothing. Use the shipped `RecordBatch` default and existing format readers. |
| Custom in-memory type | `impl DataBatch for YourType`. Pass TCK Layer 1. Register format readers that produce your type. |
| Full control over execution | Bypass the kernel's default scan loop. Use format readers as a stream source and drive your own processing. |

### Core traits

**`DataBatch`** defines the contract for an in-memory data representation that the Iceberg kernel can process. The engine chooses a concrete batch type at session start and that choice does not change. `DataBatch` methods (`filter`, `project`, `evolve_schema`, `inject_constants`) return `Self` — they operate on the concrete type. Implementations have full freedom to fuse operations internally for performance. The kernel does not decompose operations into steps or dictate intermediate materializations.

The shipped default is `impl DataBatch for RecordBatch`. Supporting a new representation requires implementing `DataBatch` and passing the TCK Layer 1 suite.

**`FormatReader`** reads a file and produces a stream of batches. Its contract:

- The reader MUST decode only the columns in `ReadOptions::schema` (projection is an I/O concern).
- The reader MAY skip chunks that cannot match `ReadOptions::predicate` using format-level statistics.
- The reader MAY support byte-range splits via `ReadOptions::split_start` / `split_length`.
- The reader MAY handle schema evolution or other kernel operations during decode if the format can do so more efficiently (e.g., Parquet type promotion during decompression).
- The reader MUST NOT handle partitioning, transaction management, or name mapping (field ID resolution for files not written by Iceberg). Those are always kernel responsibilities.

The reader returns a `ReadResult` containing the batch stream and reporting what operations the reader handled. The kernel applies any remaining operations (residual filtering, schema evolution, constant injection, delete application) that the format did not.

**`FormatWriter`** writes batches to a file. Its contract:

- The writer MUST encode batches into the file format.
- The writer MUST collect file-level metrics per `WriteOptions::metrics_config`.
- The writer MUST handle format-specific encoding concerns (like variant shredding layout for Parquet).
- The writer MUST NOT handle partitioning, sorting, or transaction management. The caller sends pre-partitioned, pre-sorted batches. The kernel handles commit.

Both traits are dyn-compatible: no associated types, no generic parameters. The registry stores them as `Arc<dyn FormatReader>` and `Arc<dyn FormatWriter>`. This is the same pattern as `Storage` and `StorageFactory` in the IO layer.

**`ReadOptions`** configures a read: Iceberg projection schema, filter predicate for pushdown, byte-range splits, case sensitivity, batch size, format-specific properties, and constant values for metadata columns.

**`WriteOptions`** configures a write: Iceberg schema, format-specific properties, file-level metadata, content type, metrics collection configuration, overwrite flag, and encryption parameters.

Both structs are `#[non_exhaustive]`. Fields are added without a breaking change.

**`ReadResult`** contains the batch stream and communicates what the reader handled. Additional fields for reporting other handled operations (schema evolution, constant injection) will be added as formats opt in to handling them. The struct is `#[non_exhaustive]` so fields are added without a breaking change.

```rust
#[non_exhaustive]
pub struct ReadResult {
    /// The stream of batches with requested columns decoded.
    pub stream: DataStream,
    /// The portion of the predicate the reader could NOT evaluate.
    /// None means the reader fully handled the predicate.
    /// Some(predicate) means the kernel must apply residual filtering.
    pub residual_predicate: Option<BoundPredicate>,
    /// Whether the reader applied schema evolution during decode.
    /// If true, the kernel skips the post-read schema evolution pass.
    pub schema_evolved: bool,
}
```

**`FormatFileWriter`** accepts batches via `write_batch(&dyn DataBatch)` and returns a `WriterResult` on `close()`. `WriterResult` contains `Vec<DataFileBuilder>` — builders rather than completed `DataFile` values because the format layer fills format-specific fields (file size, column sizes, metrics from its own metadata) while the kernel fills Iceberg-level fields (partition values, sequence number, snapshot ID).

**`FormatRegistry`** maps `(DataFileFormat, TypeId)` pairs to reader and writer implementations. The first dimension is the file format (determined at runtime from table metadata). The second dimension is the batch type (fixed at session start by the engine). This matches Java's `FormatModelRegistry` key of `(FileFormat, Class<?>)`.

The registry exposes `reader::<B>(format)` and `writer::<B>(format)`, where `B` is the batch type the engine chose. Errors are eager: if `(format, B)` is not registered, the call fails immediately at scan planning time rather than mid-stream during I/O. This is a runtime contract — the `TypeId` tells the registry what batch type the caller expects, but the type system does not enforce that the reader actually produces that type. A reader that returns the wrong type fails at the first downcast with a clear error. Java makes the same tradeoff with `Class<?>`.

A process-wide default registry is available via `OnceLock`. Custom registries can be provided to `TableScanBuilder` for tests or restricted configurations.

**Default scan loop (hybrid execution model).** The kernel provides a default scan loop that pulls batches from the format reader, checks `ReadResult` to see what the format already handled, and applies any remaining operations (residual filtering, schema evolution, constant injection, delete application). Engines that want full control (DataFusion, Comet) bypass this loop and drive execution themselves — they use the format reader as a stream source and apply their own processing. The `DataBatch` methods are available as a toolbox, not mandated as a pipeline.

**Supporting types:** `ColumnMetrics` stores bounds as typed `Datum` values, matching how `DataFile` already stores bounds. `MetricsConfig` and `MetricsMode` control collection granularity. `FileContent` distinguishes data from equality and position deletes. All config structs and enums are `#[non_exhaustive]`.

### Feature flags

Format implementations are gated behind feature flags (`format-parquet` on by default, `format-orc` when ORC lands). The default feature set includes every implemented format. No configuration is needed for downstream crates using default features. Feature flags exist for crates that want to minimize binary size or compile time. This matches `datafusion`, `opendal`, and `reqwest`.

### Module layout

```
crates/iceberg/src/formats/
├── mod.rs              # Module declarations and public re-exports
├── traits.rs           # DataBatch, FormatReader, FormatWriter,
│                       #   FormatFileWriter, ReadOptions, WriteOptions,
│                       #   ColumnMetrics, MetricsConfig, MetricsMode,
│                       #   FileContent, WriterResult,
│                       #   VariantShredding (future extension, not yet wired)
├── registry.rs         # FormatRegistry, default_format_registry()
└── parquet/
    ├── mod.rs          # Submodule declarations and re-exports
    ├── model.rs        # ParquetArrowModel (implements FormatReader + FormatWriter)
    ├── reader.rs       # Parquet reader internals
    └── writer.rs       # Parquet writer (implements FormatFileWriter)
```

Additional formats land as `formats/orc/`, `formats/avro_data/`, each following the same structure.

### Target crate architecture

The end-state after crate separation (follow-up work, not part of Phase 1). All crates live in the same repository and workspace. Phase 1 ships everything in the existing `iceberg` crate.

```
iceberg (core kernel)         - traits only, no arrow/parquet dependency
iceberg-arrow                 - impl DataBatch for RecordBatch
iceberg-format-parquet        - ParquetArrowModel
iceberg-format-orc            - OrcArrowModel
iceberg-datafusion            - TableProvider, depends on above
```

This separation enables engines to pin their own Arrow version independently. For example, a Comet integration could depend on `iceberg` (the kernel) and provide its own `DataBatch` implementation and format readers using a newer Arrow version, without conflicting with `iceberg-arrow` or `iceberg-format-parquet`.

## Design Rationale

### Three layers, Arrow as default

Java's File Format API has two layers: format and engine. Each format-engine combination requires a direct conversion path, producing an N-by-M matrix. The Java community's performance analysis ([benchmark branch](https://github.com/pvary/iceberg/tree/perf_bench)) measured 20% overhead for double conversion (ORC to Spark ColumnarBatch versus direct VectorizedRowBatch access) and recommended a hybrid: provide an Arrow-based intermediate for interoperability, allow engines to replace it for performance-sensitive paths.

This RFC adopts the hybrid. Arrow is the shipped default. The `DataBatch` trait is the extension point for representations that cannot or should not convert to Arrow. If both format and engine happen to use Arrow (the common case today), there is zero intermediate conversion.

### Why DataBatch is a trait

The kernel ambition ([#1817](https://github.com/apache/iceberg-rust/issues/1817)) requires support for multiple in-memory representations. Arrow version coupling forces lockstep upgrades across the dependency graph — an engine that wants Arrow 60 cannot coexist with a kernel pinned to Arrow 58 unless the kernel is representation-agnostic. `DataBatch` as a trait solves both while preserving Arrow as the shipped default.

The original version of this RFC fixed the data type to `RecordBatch`. Community feedback raised two concerns: that Arrow's dominance in the Rust ecosystem may reflect selection pressure rather than a permanent constraint, and that a kernel-level API should remain open to other ecosystems. This rewrite makes the in-memory type a trait boundary rather than a fixed type.

The name `DataBatch` was chosen over alternatives like `ProcessableBatch` or `ExecutionBatch` because it names what flows through the system (batches of data) rather than what the trait demands (processability). The trait's contract is documented in the method signatures and the TCK; the name stays short and general since it will appear in every engine integration, error message, and type signature.

### Concrete types for processing, trait objects for dispatch

The processing layer works with the concrete batch type the engine chose (like `RecordBatch`). The registry uses trait objects (`Arc<dyn FormatReader>`) because it must hand back a reader for any format at runtime — Parquet for one table, ORC for another — without the caller naming the concrete reader type.

The cost of trait objects is one type check per batch when pulling from the reader stream. The benefit is that the processing layer can fuse operations, inline method calls, and avoid heap allocations for intermediate results. This is the same split `object_store` uses (`dyn ObjectStore` at the boundary, concrete streams returned) and `datafusion` uses (`dyn TableProvider` at the boundary, concrete `RecordBatchStream` returned).

`async fn` in traits does not work through `dyn Trait`. Since the registry returns trait objects, async methods return `BoxFuture`. The allocation happens once per file operation, not per batch.

### Coarse trait methods, rich TCK

`DataBatch::filter` accepts the full `BoundPredicate` tree. `DataBatch::evolve_schema` takes source and target schemas. The trait does not decompose operations into evaluate-per-leaf-then-compose-masks or compute-diff-then-apply-ops. Decomposition forces intermediate materializations. The existing Arrow implementation evaluates predicates as fused closures during I/O, avoiding materialized boolean masks. A decomposed interface would force Arrow to abandon this optimization or work around the trait.

The cost of coarse interfaces is implementor burden. A new `DataBatch` implementation must correctly evaluate recursive predicates and handle schema evolution semantics. The mitigation is the TCK Layer 1 suite, which covers every operator, every primitive type, compound predicates, null handling, and schema evolution scenarios. A new implementation iterates against the TCK until all scenarios pass. Correctness is validated by testing, not constrained by interface shape.

### No engine_schema parameter

Java's `ReadBuilder.engineProjection(S)` and `ModelWriteBuilder.engineSchema(S)` serve two purposes: engine type widening and variant shredding configuration.

Variant shredding is a format-layer concern. The physical shredding layout is defined by the Parquet spec ([VariantShredding.md](https://github.com/apache/parquet-format/blob/master/VariantShredding.md)). The decision of what to shred comes from table properties, data statistics, or engine-provided configuration — but in all cases it is expressed as typed Iceberg-level fields (which paths to extract, as what types), not as an opaque engine-specific schema. Java's Spark implements its own shredding inference internally (PR [#49234](https://github.com/apache/spark/pull/49234), [#52406](https://github.com/apache/spark/pull/52406)); Iceberg later built a shared analyzer (PR [#14297](https://github.com/apache/iceberg/pull/14297)). Neither passes a shredding schema through `engineSchema`. The format handles the physical encoding.

Engine type widening (read an `int` column as `long`) is expressible as a typed `HashMap<i32, PrimitiveType>` on `ReadOptions` when needed.

A type-erased `Box<dyn Any>` was rejected because it allows hidden coupling: a caller going through the registry could pass an object only one format understands. The failure mode is code that works in tests (Parquet-only) and breaks in production (table has ORC files), with a runtime error that surfaces in the reader, not where the wrong object was passed. Both `ReadOptions` and `WriteOptions` are `#[non_exhaustive]`; typed fields are added without a breaking change.

### Options structs, not builder traits

An earlier iteration used `FormatReadBuilder` and `FormatWriteBuilder` traits with fluent `&mut self` methods. Options structs were chosen instead: fewer traits, matches `object_store` precedent (`GetOptions`, `PutOptions`), composable without `Box<dyn>` allocations at each handoff point.

### Divergence from Java

| Aspect | Java | This RFC | Rationale |
|--------|------|----------|-----------|
| Type parameters | `FormatModel<D, S>` | Dyn-compatible traits, no type params | Registry requires trait objects |
| Processing layer | Implicit (engine-specific) | Explicit `DataBatch` trait | Separates concerns, avoids N*M |
| Registry key | `(FileFormat, Class<?>)` | `(DataFileFormat, TypeId)` | Same concept; batch type is the second dimension |
| Format traits | Single `FormatModel` | Separate `FormatReader` + `FormatWriter` | A format may support one without the other |
| Engine schema | Generic parameter `S` | Deferred; typed Iceberg-level fields when needed | Shredding is a format-layer concern, not engine |
| Residual predicate | Not reported | `ReadResult::residual_predicate` | Kernel needs to know what format handled |
| Old API coexistence | Kept indefinitely | Hard cutover (pre-1.0) | Clean end state while cost is low |

## Conformance Tests (TCK)

### Layered test architecture

```
Layer 3: Integration    (format x batch x scenario)
Layer 2: Format         (one format, known-good batch type)
Layer 1: DataBatch      (one batch type, no format involvement)
```

When a Layer 3 test fails, Layer 1 and Layer 2 results identify whether the bug is in the batch implementation, the format implementation, or their interaction.

### Layer 1: DataBatch conformance

Layer 1 validates that a `DataBatch` implementation correctly handles Iceberg semantics without any file format involvement. A new batch type must pass every Layer 1 test before it can participate in the format API.

**Filter correctness:**
- Every binary operator (Eq, NotEq, Lt, LtEq, Gt, GtEq, StartsWith, NotStartsWith) against every primitive type.
- Every unary operator (IsNull, NotNull, IsNan, NotNan).
- Set operators (In, NotIn) with varying set sizes.
- Compound predicates: AND, OR, NOT at multiple nesting depths.
- Edge cases: all-null columns, all-matching, none-matching, NaN handling, empty batches.
- Property-based: for random data and random predicates, verify `filter(p).num_rows()` matches naive row-by-row evaluation.

**Schema evolution correctness:**
- Type promotion: int to long, float to double, decimal widening.
- Add column with default value (every primitive type).
- Add column with null (optional field).
- Column reordering (same fields, different order).
- Drop column (project narrows).
- Combined: promote + add + reorder in one operation.

**Inject constants correctness:**
- Every primitive Datum type as constant.
- Multiple constants at once.
- Zero-row batch.

**Project correctness:**
- Single column, multiple columns, all columns.
- Non-existent field ID produces an error.
- Idempotency: project(ids).project(ids) == project(ids).

**Metrics correctness:**
- Every primitive type: verify min/max match hand-computed values.
- Null handling: null_count matches actual nulls.
- NaN handling: nan_count for float/double.
- All-null column: bounds are None.
- Single-value column: lower == upper.

### Layer 2: Format conformance

Layer 2 validates that a format reader/writer correctly round-trips data and produces correct metadata. Uses Arrow RecordBatch (a known-good Layer 1 implementation) as the batch type.

**Phase 1 scenarios (land with initial PR):**

1. Round-trip every `PrimitiveType` with null and non-null values.
2. Nested types: struct, list, map, combinations.
3. Projection: write wide, read narrow.
4. Null handling: all-null, mixed, required versus optional.
5. Filter pushdown with row group elimination.
6. Split reading across multiple splits.
7. Statistics: column sizes, value counts, null counts, NaN counts, min/max bounds.
8. Metrics modes: None, Counts, Full, Truncate.

**Follow-up scenarios (before 1.0):**

9. Schema evolution with defaults.
10. Delete files (equality and position).
11. Metadata columns.
12. Case sensitivity.
13. Encryption round-trip.
14. Name mapping (reading files without embedded field IDs).
15. Variant ([#2188](https://github.com/apache/iceberg-rust/pull/2188)).

### Merge gates

- New `DataBatch` implementation: must pass all Layer 1 tests.
- New `FormatReader`/`FormatWriter` implementation: must pass all Layer 2 tests.
- New format-batch combination: must pass all Layer 3 tests.

## Migration / Rollout Plan

### Phase 1: Land the abstraction (additive)

Add `formats/` module with all traits, registry, and Phase 1 TCK. No existing code changes. The `ParquetArrowModel` implementation wraps the existing `ParquetWriter` and `ArrowReader` internally.

### Phase 2: Migrate internal callers (breaking)

Modify `TableScan` to dispatch through `FormatRegistry`. Modify `DataFileWriterBuilder` and `EqualityDeleteFileWriterBuilder` to use the registry. Update the DataFusion integration.

### Phase 3: Relocate and separate (breaking)

Move Parquet types into `formats/parquet/`. Establish crate boundaries per [#1817](https://github.com/apache/iceberg-rust/issues/1817) and [#1819](https://github.com/apache/iceberg-rust/issues/1819). Add `#[non_exhaustive]` to `DataFileFormat`.

## Breaking Changes

| # | What breaks | Phase | Migration |
|---|-------------|-------|-----------|
| 1 | `DataFileWriterBuilder` generic parameters removed | 2 | Use registry-based constructor |
| 2 | `EqualityDeleteFileWriterBuilder` generic parameters removed | 2 | Use registry-based constructor |
| 3 | `ParquetWriterBuilder` module path changes | 3 | Update `use` statements |
| 4 | `ArrowReaderBuilder` path changes, renamed to `ParquetReaderBuilder` | 3 | Update `use` statements |
| 5 | `DataFileFormat` gains `#[non_exhaustive]` | 3 | Add wildcard arm to `match` blocks |
| 6 | `TableScan::to_arrow` dispatches through registry (behavior change) | 2 | No change for default registry users |

## Open Questions

### 1. Delete application

**Current lean:** Delete application is a kernel responsibility, not part of `DataBatch` or the format reader. The kernel's default scan loop loads delete files, builds equality delete predicates and positional delete row selections, and applies them after the format reader produces batches. The format reader never sees delete files.

**Tradeoff:** Handling deletes in the kernel means every `DataBatch` implementation gets merge-on-read for free — delete files are resolved before the engine sees batches. The downside is that a format reader cannot fuse delete application with I/O (for example, applying positional deletes during Parquet decompression to skip rows before materializing them). If a format implementation demonstrates that fusing delete application with I/O provides meaningful benefit, a future `ReadOptions` field could pass delete information to the format reader as an optimization hint.

**Question:** Is kernel-owned delete application the right starting point, or should format readers have the option to handle deletes during I/O from the start?

### 2. Registry scope

**Current lean:** Process-wide default via `OnceLock`, with `TableScanBuilder::with_format_registry` as override.

**Tradeoff:** A process-wide default is convenient for single-configuration applications but makes testing harder (tests cannot isolate registry state without global mutation). Explicit construction is more testable and embeddable but adds a parameter to every scan and write call.

**Question:** Is the process-wide default acceptable, or should the registry always be provided explicitly?

### 3. Crate separation timing

**Current lean:** Phase 1 ships in the existing `iceberg` crate. Separation is follow-up per [#1817](https://github.com/apache/iceberg-rust/issues/1817).

**Question:** Should crate boundaries be established in Phase 1 or deferred?

## Alternatives Considered

### Fix data type to RecordBatch

Rejected. The kernel ambition requires representation-agnosticism. Arrow version coupling forces lockstep upgrades. See "Why DataBatch is a trait" in Design Rationale.

### Associated types on format traits

Rejected. Associated types prevent `dyn` usage. The registry requires trait objects. See "Concrete types for processing, trait objects for dispatch" in Design Rationale.

### Builder traits for read/write configuration

Rejected. Options structs are simpler and match `object_store` precedent. See "Options structs, not builder traits" in Design Rationale.

### Type-erased engine_schema parameter

Rejected. Allows hidden coupling between callers and specific format implementations. See "No engine_schema parameter" in Design Rationale.

### Decomposed DataBatch interface

Rejected. Forces intermediate materializations. See "Coarse trait methods, rich TCK" in Design Rationale.

### Auto-registration via `inventory` crate

Rejected. Items stripped silently at library boundaries, hostile to analysis tools, registers formats the caller did not depend on.

### Non-breaking additive-only rollout

Rejected. Leaves two paths indefinitely, retains misleading names, keeps format-specific generics on orchestration types. Pre-1.0 is the right time to remove the old API.

## References

### Apache Iceberg (Java)

- [PR #12774](https://github.com/apache/iceberg/pull/12774): Core File Format API interfaces
- [PR #14297](https://github.com/apache/iceberg/pull/14297): Spark variant shredding writer (demonstrates shredding without engineSchema parameter)
- [PR #15253](https://github.com/apache/iceberg/pull/15253): ParquetFormatModel
- [PR #15254](https://github.com/apache/iceberg/pull/15254): AvroFormatModel
- [PR #15255](https://github.com/apache/iceberg/pull/15255): ORCFormatModel
- [PR #15258](https://github.com/apache/iceberg/pull/15258): ArrowFormatModel
- [PR #15441](https://github.com/apache/iceberg/pull/15441): TCK
- [Issue #15415](https://github.com/apache/iceberg/issues/15415): TCK tracking (open)
- [Issue #15416](https://github.com/apache/iceberg/issues/15416): Vortex format (open)

### Apache Spark

- [PR #49234](https://github.com/apache/spark/pull/49234): Variant shredding support for Parquet (Spark-internal implementation)
- [PR #52406](https://github.com/apache/spark/pull/52406): Infer variant shredding schema at write time

### Apache Parquet

- [VariantShredding.md](https://github.com/apache/parquet-format/blob/master/VariantShredding.md): Parquet spec for physical shredding layout

### Apache Iceberg (Python)

- [Issue #3100](https://github.com/apache/iceberg-python/issues/3100): File Format API proposal

### Apache Iceberg Rust

- [Issue #1817](https://github.com/apache/iceberg-rust/issues/1817): Build Iceberg Kernel
- [Issue #1819](https://github.com/apache/iceberg-rust/issues/1819): Modularize iceberg Implementations
- [Issue #1314](https://github.com/apache/iceberg-rust/issues/1314): Make FileIO a Trait
- [PR #2188](https://github.com/apache/iceberg-rust/pull/2188): Variant type support
