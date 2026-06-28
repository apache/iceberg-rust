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

//! Core trait definitions for the File Format API.

use std::any::Any;
use std::collections::HashMap;

use futures::future::BoxFuture;
use futures::stream::BoxStream;

use crate::expr::BoundPredicate;
use crate::io::{InputFile, OutputFile};
use crate::spec::{DataFileBuilder, DataFileFormat, Datum, Schema};
use crate::Result;

// ---------------------------------------------------------------------------
// DataBatch
// ---------------------------------------------------------------------------

/// The contract for any in-memory data representation that the Iceberg kernel
/// can process.
///
/// Implementing this trait is the cost of entry for bringing a new in-memory
/// format. Implementations have full freedom to fuse operations internally
/// for performance. Correctness is validated by the TCK (Layer 1).
///
/// The `iceberg` crate ships `impl DataBatch for RecordBatch` as the default.
pub trait DataBatch: Send + 'static {
    /// Number of rows in this batch.
    fn num_rows(&self) -> usize;

    /// The Iceberg field IDs present in this batch, in column order.
    fn field_ids(&self) -> &[i32];

    /// Project to a subset of columns by Iceberg field ID.
    fn project(&self, field_ids: &[i32]) -> Result<Self>
    where
        Self: Sized;

    /// Evaluate a filter predicate, returning only matching rows.
    ///
    /// Implementations may use any strategy: vectorized array ops, visitor
    /// pattern, fused I/O closures, etc. The kernel does NOT dictate
    /// evaluation strategy.
    fn filter(&self, predicate: &BoundPredicate) -> Result<Self>
    where
        Self: Sized;

    /// Compute column-level metrics for the given field.
    ///
    /// Returns `None` if the field is not present in this batch.
    fn column_metrics(&self, field_id: i32) -> Option<ColumnMetrics>;

    /// Inject constant values for metadata columns (`_file`, `_pos`, `_partition`).
    fn inject_constants(&self, constants: &[(i32, Datum)]) -> Result<Self>
    where
        Self: Sized;

    /// Apply schema evolution: add columns with defaults, widen types, rename.
    ///
    /// The `source` schema describes what this batch currently contains.
    /// The `target` schema describes what the caller needs.
    fn evolve_schema(&self, source: &Schema, target: &Schema) -> Result<Self>
    where
        Self: Sized;

    /// Downcast support for runtime type resolution.
    fn as_any(&self) -> &dyn Any;
}

// ---------------------------------------------------------------------------
// Supporting types
// ---------------------------------------------------------------------------

/// A stream of batches produced by a format reader.
pub type DataStream = BoxStream<'static, Result<Box<dyn DataBatch>>>;

/// The result of a format read operation.
///
/// Contains the batch stream and communicates what the reader handled
/// from the [`ReadOptions`]. The kernel uses `residual_predicate` to
/// determine whether it needs to apply row-level filtering via
/// [`DataBatch::filter`].
#[non_exhaustive]
pub struct ReadResult {
    /// The stream of batches with requested columns decoded.
    pub stream: DataStream,
    /// The portion of the predicate the reader could NOT evaluate.
    ///
    /// `None` means the reader fully handled the predicate — all rows
    /// in the stream match. `Some(predicate)` means the kernel must
    /// apply residual filtering using [`DataBatch::filter`].
    pub residual_predicate: Option<BoundPredicate>,
}

/// Column-level metrics produced by a writer or computed from a batch.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ColumnMetrics {
    /// Number of values in the column (including nulls).
    pub value_count: u64,
    /// Number of null values.
    pub null_count: u64,
    /// Number of NaN values (for float and double columns).
    pub nan_count: Option<u64>,
    /// Lower bound value as a typed Datum.
    pub lower_bound: Option<Datum>,
    /// Upper bound value as a typed Datum.
    pub upper_bound: Option<Datum>,
    /// Column size in bytes.
    pub column_size: Option<u64>,
}

impl ColumnMetrics {
    /// Create metrics with only counts populated.
    pub fn counts(value_count: u64, null_count: u64) -> Self {
        Self {
            value_count,
            null_count,
            nan_count: None,
            lower_bound: None,
            upper_bound: None,
            column_size: None,
        }
    }
}

/// The content type of a file being written.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum FileContent {
    /// Data file containing table rows.
    #[default]
    Data,
    /// Equality delete file.
    EqualityDeletes,
    /// Position delete file.
    PositionDeletes,
}

/// Configuration for metrics collection during writes.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct MetricsConfig {
    /// Per-column metrics mode overrides keyed by column name.
    pub column_modes: HashMap<String, MetricsMode>,
    /// Default mode for columns without an explicit override.
    pub default_mode: MetricsMode,
}

/// The metrics collection mode for a column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum MetricsMode {
    /// Do not collect any metrics.
    None,
    /// Collect only value counts and null counts.
    Counts,
    /// Collect full metrics including bounds.
    Full,
    /// Collect truncated bounds at the given length.
    Truncate(u32),
}

// ---------------------------------------------------------------------------
// ReadOptions / WriteOptions
// ---------------------------------------------------------------------------

/// Configuration for a format read operation.
///
/// Passed to [`FormatReader::read`]. Format implementations apply what they
/// can (best-effort pushdown) and ignore unsupported options.
///
/// # Engine schema / variant shredding
///
/// This struct intentionally does NOT carry an `engine_schema` parameter.
/// Java's `ReadBuilder.engineProjection(S)` serves two purposes: engine-
/// requested type widening (e.g., read an `int` column as `long`) and
/// variant shredding layout hints. Both are deferred from Phase 1 because:
///
/// 1. **Variant shredding** depends on the variant type landing first
///    ([#2188](https://github.com/apache/iceberg-rust/pull/2188)). When it
///    does, shredding can be modeled as a format-agnostic Iceberg-level
///    concept (which paths to shred and what types to extract) rather than
///    an opaque engine-specific blob.
///
/// 2. **Engine type widening** ("give me `long` even though the schema says
///    `int`") can be expressed as a typed `HashMap<i32, PrimitiveType>`
///    mapping field IDs to desired output types — no type erasure needed.
///
/// 3. **A type-erased `Box<dyn Any>` field would allow hidden coupling.**
///    A caller going through the registry could pass a format-specific
///    schema object that only one format understands, silently breaking
///    when a different format is registered. By deferring this field, we
///    force the eventual design to be format-agnostic and compiler-checked.
///
/// When these features land, `#[non_exhaustive]` allows adding fields
/// without a breaking change.
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct ReadOptions {
    /// Iceberg projection schema.
    pub schema: Option<Schema>,
    /// Filter predicate for pushdown.
    pub predicate: Option<BoundPredicate>,
    /// Byte range start for split reading.
    pub split_start: Option<u64>,
    /// Byte range length for split reading.
    pub split_length: Option<u64>,
    /// Case sensitivity for predicate evaluation. Defaults to true.
    pub case_sensitive: bool,
    /// Number of rows per batch.
    pub batch_size: Option<usize>,
    /// Format-specific configuration. Unknown keys are ignored.
    pub properties: HashMap<String, String>,
    /// Constant values for metadata columns not stored in the data file.
    pub id_to_constant: HashMap<i32, Datum>,
}

/// Configuration for a format write operation.
///
/// Passed to [`FormatWriter::write`]. Format implementations apply what they
/// can and ignore unsupported options.
///
/// # Engine schema
///
/// Like [`ReadOptions`], this struct intentionally omits an `engine_schema`
/// parameter. See the [`ReadOptions`] doc comment for the full rationale:
/// a type-erased field would allow hidden coupling between callers and
/// specific format implementations through the generic registry path.
/// When engine type narrowing is needed, it will be expressed as typed,
/// format-agnostic fields that the compiler can check.
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct WriteOptions {
    /// Iceberg schema for the file being written.
    pub schema: Option<Schema>,
    /// Format-specific configuration. Unknown keys are ignored.
    pub properties: HashMap<String, String>,
    /// File-level metadata (for example, delete-type, equality field IDs).
    pub metadata: HashMap<String, String>,
    /// Content type: data, equality deletes, or position deletes.
    pub content: FileContent,
    /// Metrics collection configuration.
    pub metrics_config: Option<MetricsConfig>,
    /// Allow overwriting an existing file.
    pub overwrite: bool,
    /// File encryption key.
    pub encryption_key: Option<Vec<u8>>,
    /// AAD prefix for encryption.
    pub aad_prefix: Option<Vec<u8>>,
}

// ---------------------------------------------------------------------------
// FormatReader / FormatWriter
// ---------------------------------------------------------------------------

/// A format that can read data files.
///
/// This trait is dyn-compatible, enabling the [`FormatRegistry`](super::FormatRegistry)
/// to store readers without callers knowing the concrete type.
/// Follows the same pattern as [`Storage`](crate::io::Storage) in the IO layer.
///
/// # Contract
///
/// The reader MUST:
/// - Decode only the columns in [`ReadOptions::schema`] (projection is an I/O concern).
///
/// The reader MAY:
/// - Skip chunks that cannot match [`ReadOptions::predicate`] using format-level statistics.
/// - Support byte-range splits via [`ReadOptions::split_start`] / [`ReadOptions::split_length`].
///
/// The reader MUST NOT handle:
/// - Residual row-level filtering (kernel responsibility)
/// - Schema evolution (kernel responsibility)
/// - Constant injection (kernel responsibility)
/// - Delete application (kernel responsibility)
/// - Name mapping / field ID resolution (kernel responsibility)
pub trait FormatReader: Send + Sync + 'static {
    /// Which file format this reader handles.
    fn format(&self) -> DataFileFormat;

    /// Read the given input file, returning a [`ReadResult`] containing the
    /// batch stream and the residual predicate the reader could not evaluate.
    fn read(
        &self,
        input: InputFile,
        options: ReadOptions,
    ) -> BoxFuture<'static, Result<ReadResult>>;
}

/// A format that can write data files.
///
/// This trait is dyn-compatible, enabling the [`FormatRegistry`](super::FormatRegistry)
/// to store writers without callers knowing the concrete type.
///
/// # Contract
///
/// The writer MUST:
/// - Encode batches into the file format.
/// - Collect file-level metrics per [`WriteOptions::metrics_config`].
/// - Handle format-specific encoding concerns (e.g., variant shredding layout for Parquet).
///
/// The writer MUST NOT handle:
/// - Partitioning (caller sends pre-partitioned batches)
/// - Sorting (caller sends pre-sorted batches)
/// - Transaction management (kernel handles commit)
pub trait FormatWriter: Send + Sync + 'static {
    /// Which file format this writer handles.
    fn format(&self) -> DataFileFormat;

    /// Write batches to the given output file.
    fn write(
        &self,
        output: OutputFile,
        options: WriteOptions,
    ) -> BoxFuture<'static, Result<Box<dyn FormatFileWriter>>>;
}

// ---------------------------------------------------------------------------
// FormatFileWriter
// ---------------------------------------------------------------------------

/// A writer that accepts batches and produces file metadata on close.
///
/// Implementations downcast `&dyn DataBatch` to their expected concrete
/// batch type via [`DataBatch::as_any`]. A type mismatch is a runtime error.
pub trait FormatFileWriter: Send {
    /// Write a batch of data.
    fn write_batch(&mut self, batch: &dyn DataBatch) -> BoxFuture<'_, Result<()>>;

    /// Close the writer and return file metadata for the manifest.
    fn close(self: Box<Self>) -> BoxFuture<'static, Result<WriterResult>>;
}

/// The result of closing a format writer.
#[non_exhaustive]
pub struct WriterResult {
    /// Data file builders representing the files written.
    pub data_files: Vec<DataFileBuilder>,
}

// ---------------------------------------------------------------------------
// Future extensions (design examples)
// ---------------------------------------------------------------------------

/// Variant shredding configuration — format-agnostic.
///
/// This type is NOT yet wired into [`ReadOptions`] or [`WriteOptions`].
/// It exists to demonstrate how variant shredding will be expressed when
/// the variant type ([#2188](https://github.com/apache/iceberg-rust/pull/2188))
/// lands. The key property: this is an Iceberg-level concept, not a
/// format-specific one. Any format that supports shredding interprets
/// the same struct identically.
///
/// When ready, `ReadOptions` gains:
/// ```rust,ignore
/// pub variant_shredding: Vec<VariantShredding>,
/// ```
#[derive(Debug, Clone)]
pub struct VariantShredding {
    /// The variant column's Iceberg field ID.
    pub variant_field_id: i32,
    /// Paths to extract as physical columns.
    pub shredded_paths: Vec<ShredPath>,
}

/// A single path within a variant to shred into a physical column.
#[derive(Debug, Clone)]
pub struct ShredPath {
    /// Dotted path into the variant (e.g., `"name"`, `"address.city"`).
    pub path: String,
    /// The Iceberg primitive type to extract this path as.
    pub data_type: crate::spec::PrimitiveType,
}

/// Engine-requested type overrides for specific columns.
///
/// This type is NOT yet wired into [`ReadOptions`]. It exists to demonstrate
/// how engine type widening will be expressed. The use case: an engine wants
/// a column read as `long` even though the Iceberg schema defines it as `int`.
/// This is distinct from schema evolution (where the TABLE schema changed) —
/// here the engine simply prefers a wider type for its own processing.
///
/// When ready, `ReadOptions` gains:
/// ```rust,ignore
/// pub type_overrides: HashMap<i32, PrimitiveType>,
/// ```
///
/// Format implementations that support widening during read (Parquet, ORC)
/// apply these overrides. Formats that don't support it ignore them.
pub type TypeOverrides = HashMap<i32, crate::spec::PrimitiveType>;

#[cfg(test)]
mod tests {
    use super::*;

    /// Demonstrates how variant shredding will integrate with ReadOptions
    /// when the variant type lands. The shredding request is format-agnostic:
    /// it uses Iceberg field IDs and primitive types, not Arrow schemas or
    /// Parquet MessageTypes.
    #[test]
    fn example_variant_shredding_config() {
        let _shredding = VariantShredding {
            variant_field_id: 7,
            shredded_paths: vec![
                ShredPath {
                    path: "name".to_string(),
                    data_type: crate::spec::PrimitiveType::String,
                },
                ShredPath {
                    path: "age".to_string(),
                    data_type: crate::spec::PrimitiveType::Int,
                },
                ShredPath {
                    path: "address.city".to_string(),
                    data_type: crate::spec::PrimitiveType::String,
                },
            ],
        };

        // When wired into ReadOptions, usage looks like:
        //
        // let options = ReadOptions {
        //     schema: Some(projected_schema),
        //     variant_shredding: vec![shredding],
        //     ..Default::default()
        // };
        //
        // Any format that supports shredding (Parquet, ORC) reads the
        // shredded columns directly. Formats that don't (Avro) ignore
        // the field and return the full binary variant.
    }

    /// Demonstrates how engine type widening will integrate with ReadOptions.
    /// The engine requests specific columns be widened during read, expressed
    /// as a typed HashMap — no Box<dyn Any>, no format-specific coupling.
    #[test]
    fn example_type_overrides_config() {
        let mut overrides: TypeOverrides = HashMap::new();

        // Engine wants field 3 (Iceberg schema says `int`) read as `long`
        overrides.insert(3, crate::spec::PrimitiveType::Long);

        // Engine wants field 9 (Iceberg schema says `float`) read as `double`
        overrides.insert(9, crate::spec::PrimitiveType::Double);

        // When wired into ReadOptions, usage looks like:
        //
        // let options = ReadOptions {
        //     schema: Some(projected_schema),
        //     type_overrides: overrides,
        //     ..Default::default()
        // };
        //
        // Formats that support widening during read (Parquet, ORC) apply
        // the overrides at decode time — no post-read conversion needed.
        // Formats that don't support it ignore the field; the kernel can
        // apply widening post-read via DataBatch::evolve_schema if needed.

        assert_eq!(overrides.len(), 2);
        assert_eq!(overrides[&3], crate::spec::PrimitiveType::Long);
    }
}
