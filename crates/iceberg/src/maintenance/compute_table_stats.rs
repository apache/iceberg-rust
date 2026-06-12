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

//! `ComputeTableStats` — per-column NDV (number of distinct values) statistics via Apache
//! DataSketches theta sketches, written as one `apache-datasketches-theta-v1` Puffin blob per column
//! into a single statistics file and registered into the table metadata.
//!
//! The Rust port of Java 1.10.0 `org.apache.iceberg.actions.ComputeTableStats` (the engine-agnostic
//! contract) + the `ComputeTableStatsSparkAction` / `NDVSketchUtil` compute behavior, minus the Spark
//! distribution layer (the scan runs through the in-process Iceberg [`TableScan`](crate::scan)).
//!
//! # The cross-engine byte contract
//!
//! Each non-null column value is fed to the sketch as its **Iceberg single-value serialization** form
//! — the exact bytes Java's `Conversions.toByteBuffer` produces (verified against the
//! `iceberg-api-1.10.0` bytecode): a `long`/`time`/`timestamp` as 8 little-endian bytes, an
//! `int`/`date` as 4 little-endian bytes, a `string` as unprefixed UTF-8, a `decimal` as the
//! big-endian two's-complement minimal encoding of the unscaled value, etc. That form is exactly what
//! [`Datum::to_bytes`](crate::spec::Datum::to_bytes) yields, so the feed is
//! `sketch.update_bytes(&datum.to_bytes()?)`. A single divergent byte would make every NDV estimate
//! silently disagree with Spark/Trino/Flink — hence the per-type byte-form pins in the tests.
//!
//! The puffin blob payload is the DataSketches theta `CompactSketch` serialized format produced by the
//! [`iceberg_sketches`] crate (byte-pinned against Java DataSketches).
//!
//! # The sketch update family is `Alpha` (Y3 — closes the Y2 family gap)
//!
//! **Java's NDV pipeline builds an `Alpha`-family update sketch, not `QuickSelect`.** Three independent
//! sources agree: the puffin spec (`format/puffin-spec.md`: "constructing **Alpha family sketch** with
//! default seed"), the Spark `ThetaSketchAgg.createAggregationBuffer` source
//! (`UpdateSketch.builder.setFamily(Family.ALPHA).build()`, identical across spark v3.5/v4.0/v4.1), and
//! the `datasketches-java-3.3.0` bytecode (`UpdateSketchBuilder`'s default family is `QUICKSELECT`, so
//! the explicit `.setFamily(ALPHA)` is load-bearing). This action therefore builds an
//! [`AlphaSketch`](iceberg_sketches::AlphaSketch) (the Rust port of `HeapAlphaSketch`) at the
//! Iceberg-default lgK 12 / seed 9001. In **exact mode** (`theta == Long.MAX_VALUE`) Alpha and
//! QuickSelect are byte-identical; in **estimation mode** they diverge (n=1M → Alpha `ndv` 1 004 032 vs
//! QuickSelect 1 002 714), and Alpha is what every other engine writes — so on high-cardinality columns
//! this action now produces cross-engine-matching bytes + `ndv`.
//!
//! # The `ndv` property reads the COMPACT sketch, not the update sketch
//!
//! The blob's `ndv` property is `String.valueOf((long) sketch.getEstimate())`, but **`sketch` is the
//! COMPACT sketch** Java reads back from the serialized bytes, not the live Alpha update sketch. Java
//! `NDVSketchUtil.toBlob` does `Sketch sketch = CompactSketch.wrap(Memory.wrap(bytes))` then reads
//! `getEstimate()` off that — the family-COMPACT *standard* estimator `compactRetained * (2^63/theta)`
//! (truncated toward zero to an `i64`, decimal digits, no leading/trailing spaces — puffin spec). This
//! differs from the Alpha update sketch's own *sampling* estimator (`nominal * 2^63/theta`): for n=1M
//! the update sketch estimates 1 002 319 but the compact sketch (and thus the `ndv`) is 1 004 032. So
//! this action feeds `ndv` from `sketch.compact().estimate()`, never from the update form's estimate.

use std::collections::HashMap;

use futures::TryStreamExt;
use iceberg_sketches::{AlphaSketch, CompactThetaSketch};
use uuid::Uuid;

use crate::arrow::arrow_primitive_to_literal;
use crate::io::FileIO;
use crate::puffin::{APACHE_DATASKETCHES_THETA_V1, Blob, CompressionCodec, PuffinWriter};
use crate::spec::{Datum, Literal, PrimitiveType, Snapshot, StatisticsFile, TableMetadata, Type};
use crate::table::Table;
use crate::transaction::{ApplyTransactionAction, Transaction};
use crate::{Catalog, Error, ErrorKind, Result};

/// The Puffin blob property key carrying the sketch's NDV estimate (Java
/// `NDVSketchUtil.APACHE_DATASKETCHES_THETA_V1_NDV_PROPERTY`).
const NDV_PROPERTY: &str = "ndv";
/// The table property overriding the metadata directory (Java
/// `TableProperties.WRITE_METADATA_LOCATION`). Stats files land here when set.
const WRITE_METADATA_PATH_PROPERTY: &str = "write.metadata.path";
/// The statistics-file extension (Java `String.format("%s-%s.stats", snapshotId, uuid)`).
const STATS_FILE_EXTENSION: &str = "stats";

/// Computes per-column NDV statistics over a table snapshot and writes them to a Puffin statistics
/// file — the Rust port of Java's `ComputeTableStats` action.
///
/// Builder semantics mirror Java's `ComputeTableStats` interface:
///
/// - [`ComputeTableStats::columns`] — the set of columns to collect stats for. **Default: all
///   top-level primitive columns** (Java `schema.columns().filter(type.isPrimitiveType())`); nested
///   struct/list/map columns are excluded and rejected if named explicitly.
/// - [`ComputeTableStats::snapshot_id`] — the snapshot to compute over. **Default: the current
///   snapshot.**
/// - [`ComputeTableStats::execute`] — runs the scan (merge-on-read deletes APPLIED), feeds each
///   selected column's non-null values into a per-column theta sketch, writes one Puffin file with one
///   `apache-datasketches-theta-v1` blob per column, and registers the resulting [`StatisticsFile`]
///   through the existing [`UpdateStatisticsAction`](crate::transaction::Transaction::update_statistics).
pub struct ComputeTableStats {
    table: Table,
    columns: Option<Vec<String>>,
    snapshot_id: Option<i64>,
}

impl ComputeTableStats {
    /// Creates a new action for `table`. With no further configuration, [`Self::execute`] computes
    /// NDV for all top-level primitive columns over the current snapshot.
    pub fn new(table: Table) -> Self {
        Self {
            table,
            columns: None,
            snapshot_id: None,
        }
    }

    /// Restricts the computation to the named columns. Each must be a top-level primitive column;
    /// [`Self::execute`] errors on a non-existent or non-primitive column (Java `validateColumns`).
    pub fn columns(mut self, columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.columns = Some(columns.into_iter().map(Into::into).collect());
        self
    }

    /// Computes stats over the given snapshot instead of the current one (Java `snapshot(long)`).
    pub fn snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = Some(snapshot_id);
        self
    }

    /// Resolves the snapshot to compute over — the configured one, else the current snapshot.
    fn resolve_snapshot(&self) -> Result<Snapshot> {
        let metadata = self.table.metadata();
        let snapshot = match self.snapshot_id {
            Some(snapshot_id) => metadata.snapshot_by_id(snapshot_id).ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Snapshot id {snapshot_id} does not exist in the table"),
                )
            })?,
            None => metadata.current_snapshot().ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Cannot compute stats: the table has no current snapshot",
                )
            })?,
        };
        Ok(snapshot.as_ref().clone())
    }

    /// Resolves the columns to collect stats for — the configured set (validated to exist and be
    /// top-level primitive), else all top-level primitive columns (Java
    /// `ComputeTableStatsSparkAction.statsColumns` / `validateColumns`).
    ///
    /// Returns each column's `(name, field_id, primitive_type)` so the feed loop can build a
    /// [`Datum`] per value without re-resolving the schema.
    fn resolve_columns(&self) -> Result<Vec<StatsColumn>> {
        let schema = self.table.metadata().current_schema();
        match &self.columns {
            Some(names) => names
                .iter()
                .map(|name| {
                    let field = schema.field_by_name(name).ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!("Can't find column {name} in the table schema"),
                        )
                    })?;
                    let primitive = field.field_type.as_primitive_type().ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!(
                                "Can't compute stats on non-primitive type column: {name} ({})",
                                field.field_type
                            ),
                        )
                    })?;
                    Ok(StatsColumn {
                        name: field.name.clone(),
                        field_id: field.id,
                        primitive_type: primitive.clone(),
                    })
                })
                .collect(),
            None => Ok(schema
                .as_struct()
                .fields()
                .iter()
                .filter_map(|field| {
                    field
                        .field_type
                        .as_primitive_type()
                        .map(|primitive| StatsColumn {
                            name: field.name.clone(),
                            field_id: field.id,
                            primitive_type: primitive.clone(),
                        })
                })
                .collect()),
        }
    }

    /// Runs the action: scan → per-column theta sketches → Puffin file → register the
    /// [`StatisticsFile`]. Returns the refreshed [`Table`] and the registered [`StatisticsFile`].
    ///
    /// # Errors
    ///
    /// - `DataInvalid` for an unknown snapshot id, a missing current snapshot, or a non-existent /
    ///   non-primitive named column.
    /// - Propagates scan / IO / Puffin-write / catalog-commit errors.
    pub async fn execute(self, catalog: &dyn Catalog) -> Result<ComputeTableStatsResult> {
        let snapshot = self.resolve_snapshot()?;
        let columns = self.resolve_columns()?;

        let sketches = self.build_sketches(&snapshot, &columns).await?;

        let path = stats_file_path(self.table.metadata(), snapshot.snapshot_id());
        let statistics_file =
            write_stats_file(self.table.file_io(), &path, &snapshot, &columns, sketches).await?;

        let transaction = Transaction::new(&self.table);
        let transaction = transaction
            .update_statistics()
            .set_statistics(statistics_file.clone())
            .apply(transaction)?;
        let table = transaction.commit(catalog).await?;

        Ok(ComputeTableStatsResult {
            table,
            statistics_file,
        })
    }

    /// Scans the snapshot (merge-on-read deletes applied) for the selected columns and accumulates one
    /// theta sketch per column, feeding only non-null values via the single-value byte form.
    async fn build_sketches(
        &self,
        snapshot: &Snapshot,
        columns: &[StatsColumn],
    ) -> Result<Vec<AlphaSketch>> {
        let mut sketches: Vec<AlphaSketch> = columns.iter().map(|_| AlphaSketch::new()).collect();

        // No columns ⇒ nothing to scan; an empty stats file (no blobs) is still valid.
        if columns.is_empty() {
            return Ok(sketches);
        }

        let column_names: Vec<&str> = columns.iter().map(|column| column.name.as_str()).collect();
        let scan = self
            .table
            .scan()
            .snapshot_id(snapshot.snapshot_id())
            .select(column_names)
            .build()?;
        let mut stream = scan.to_arrow().await?;

        while let Some(batch) = stream.try_next().await? {
            // The scan projects exactly the selected columns, in `columns` order, so column index i
            // in the batch corresponds to sketch i. Guard the shape rather than trusting it.
            if batch.num_columns() != columns.len() {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!(
                        "Scan returned {} columns but {} were selected for stats",
                        batch.num_columns(),
                        columns.len()
                    ),
                ));
            }
            for (index, column) in columns.iter().enumerate() {
                let array = batch.column(index);
                let iceberg_type = Type::Primitive(column.primitive_type.clone());
                let literals = arrow_primitive_to_literal(array, &iceberg_type)?;
                feed_column(&mut sketches[index], &column.primitive_type, literals)?;
            }
        }

        Ok(sketches)
    }
}

/// The result of [`ComputeTableStats::execute`]: the refreshed table and the registered stats file.
#[derive(Debug)]
pub struct ComputeTableStatsResult {
    /// The table refreshed after the statistics file was committed.
    pub table: Table,
    /// The statistics file that was written and registered.
    pub statistics_file: StatisticsFile,
}

/// A resolved stats column: its name (for the scan projection), Iceberg field id (the blob's
/// `fields`), and primitive type (to build a [`Datum`] per value).
struct StatsColumn {
    name: String,
    field_id: i32,
    primitive_type: PrimitiveType,
}

/// Feeds a column's non-null values into `sketch` using the Iceberg single-value byte serialization
/// (Java `Conversions.toByteBuffer` + `update(byte[])`). A `None` literal (a null cell) never updates
/// the sketch — distinct-value counting excludes nulls, matching Java's `theta_sketch_agg`.
fn feed_column(
    sketch: &mut AlphaSketch,
    primitive_type: &PrimitiveType,
    literals: Vec<Option<Literal>>,
) -> Result<()> {
    for literal in literals {
        let Some(Literal::Primitive(primitive)) = literal else {
            // None ⇒ null cell (skipped); a non-primitive literal cannot occur for a primitive
            // column, but is defensively skipped rather than fed with the wrong bytes.
            continue;
        };
        let datum = Datum::new(primitive_type.clone(), primitive);
        let bytes = datum.to_bytes()?;
        sketch.update_bytes(&bytes);
    }
    Ok(())
}

/// Builds the stats-file path — the Rust port of Java `String.format("%s-%s.stats", snapshotId,
/// UUID.randomUUID())` + `TableOperations.metadataFileLocation`.
///
/// Location: if `write.metadata.path` is set → `<stripTrailingSlash(write.metadata.path)>/<name>`,
/// else `<location()>/metadata/<name>`.
fn stats_file_path(metadata: &TableMetadata, snapshot_id: i64) -> String {
    let uuid = Uuid::new_v4();
    let name = format!("{snapshot_id}-{uuid}.{STATS_FILE_EXTENSION}");
    match metadata.properties().get(WRITE_METADATA_PATH_PROPERTY) {
        Some(write_metadata_path) => {
            let base = write_metadata_path.trim_end_matches('/');
            format!("{base}/{name}")
        }
        None => format!("{}/metadata/{name}", metadata.location()),
    }
}

/// Writes one Puffin file with one `apache-datasketches-theta-v1` blob per column and returns the
/// [`StatisticsFile`] describing it.
///
/// Each blob carries `fields = [field_id]`, the snapshot id + sequence number of the computed
/// snapshot, the compact-sketch payload, and the `ndv` property = `(estimate as i64)` decimal string
/// (Java `NDVSketchUtil`). The returned [`StatisticsFile`]'s `file_size_in_bytes` /
/// `file_footer_size_in_bytes` are the writer's total / footer sizes (Java
/// `PuffinWriter.fileSize()` / `footerSize()`).
async fn write_stats_file(
    file_io: &FileIO,
    path: &str,
    snapshot: &Snapshot,
    columns: &[StatsColumn],
    sketches: Vec<AlphaSketch>,
) -> Result<StatisticsFile> {
    let output_file = file_io.new_output(path)?;
    let mut writer = PuffinWriter::new(&output_file, HashMap::new(), false).await?;
    let mut blob_metadata = Vec::with_capacity(columns.len());

    for (column, sketch) in columns.iter().zip(sketches.into_iter()) {
        let payload = sketch.serialize_compact().map_err(|error| {
            Error::new(
                ErrorKind::Unexpected,
                format!(
                    "Failed to serialize the theta sketch for column field id {}",
                    column.field_id
                ),
            )
            .with_source(error)
        })?;
        // The `ndv` reads the COMPACT sketch's estimate (Java `NDVSketchUtil.toBlob`:
        // `CompactSketch.wrap(bytes).getEstimate()`), NOT the Alpha update sketch's sampling estimate —
        // the two diverge in estimation mode (n=1M → compact 1004032 vs update 1002319). Parse the
        // freshly-serialized payload through the same reader Java uses (`CompactSketch.wrap`).
        let ndv_estimate = CompactThetaSketch::deserialize(&payload)
            .map_err(|error| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!(
                        "Failed to read back the compact theta sketch for column field id {} to \
                         compute its ndv",
                        column.field_id
                    ),
                )
                .with_source(error)
            })?
            .estimate() as i64;
        let blob = Blob::builder()
            .r#type(APACHE_DATASKETCHES_THETA_V1.to_string())
            .fields(vec![column.field_id])
            .snapshot_id(snapshot.snapshot_id())
            .sequence_number(snapshot.sequence_number())
            .data(payload)
            .properties(HashMap::from([(
                NDV_PROPERTY.to_string(),
                ndv_estimate.to_string(),
            )]))
            .build();
        let written = writer.add(blob, CompressionCodec::None).await?;
        blob_metadata.push(crate::spec::BlobMetadata {
            r#type: written.blob_type().to_string(),
            snapshot_id: written.snapshot_id(),
            sequence_number: written.sequence_number(),
            fields: written.fields().to_vec(),
            properties: written.properties().clone(),
        });
    }

    let footer_size = i64::try_from(writer.footer_size()?).unwrap_or(i64::MAX);
    let file_size = i64::try_from(writer.close().await?).unwrap_or(i64::MAX);

    Ok(StatisticsFile {
        snapshot_id: snapshot.snapshot_id(),
        statistics_path: path.to_string(),
        file_size_in_bytes: file_size,
        file_footer_size_in_bytes: footer_size,
        key_metadata: None,
        blob_metadata,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{
        ArrayRef, Date32Array, Decimal128Array, Int64Array, RecordBatch, StringArray,
    };
    use iceberg_sketches::{AlphaSketch, CompactThetaSketch, ThetaSketch};
    use tempfile::TempDir;

    use super::*;
    use crate::io::{FileIO, LocalFsStorageFactory};
    use crate::memory::MemoryCatalogBuilder;
    use crate::puffin::PuffinReader;
    use crate::spec::{
        DataContentType, DataFile, DataFileFormat, FormatVersion, Literal, NestedField,
        PartitionSpec, PrimitiveType, Schema, Struct, Type,
    };
    use crate::writer::base_writer::equality_delete_writer::{
        EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig,
    };
    use crate::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use crate::writer::file_writer::{FileWriter, FileWriterBuilder, ParquetWriterBuilder};
    use crate::writer::{IcebergWriter, IcebergWriterBuilder};
    use crate::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};

    // ============================================================================================
    // Test harness — a local-fs memory catalog (REAL parquet on disk) + a multi-type table:
    //   id      (long,   field 1)   — duplicates + distinct
    //   name    (string, field 2)   — duplicates
    //   day     (date,   field 3)   — distinct
    //   amount  (decimal(9,2), 4)   — distinct
    //   maybe   (long,   field 5, OPTIONAL) — contains nulls
    // ============================================================================================

    const DECIMAL_PRECISION: u32 = 9;
    const DECIMAL_SCALE: u32 = 2;

    async fn local_fs_catalog() -> (impl Catalog, TempDir) {
        let temp_dir = TempDir::new().expect("temp dir");
        let warehouse = temp_dir
            .path()
            .to_str()
            .expect("utf8 temp path")
            .to_string();
        let catalog = MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([("warehouse".to_string(), warehouse)]),
            )
            .await
            .expect("load local-fs memory catalog");
        (catalog, temp_dir)
    }

    fn multi_type_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::required(
                    2,
                    "name",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::required(
                    3,
                    "day",
                    Type::Primitive(PrimitiveType::Date),
                )),
                Arc::new(NestedField::required(
                    4,
                    "amount",
                    Type::Primitive(PrimitiveType::Decimal {
                        precision: DECIMAL_PRECISION,
                        scale: DECIMAL_SCALE,
                    }),
                )),
                Arc::new(NestedField::optional(
                    5,
                    "maybe",
                    Type::Primitive(PrimitiveType::Long),
                )),
            ])
            .build()
            .expect("build schema")
    }

    async fn create_unpartitioned_table(catalog: &impl Catalog, schema: Schema) -> Table {
        let namespace = NamespaceIdent::new(format!("ns-{}", uuid::Uuid::new_v4()));
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .expect("create namespace");
        let table_ident = TableIdent::new(namespace.clone(), "t".to_string());
        let creation = TableCreation::builder()
            .name(table_ident.name().to_string())
            .schema(schema)
            .partition_spec(PartitionSpec::unpartition_spec())
            .format_version(FormatVersion::V2)
            .build();
        catalog
            .create_table(&namespace, creation)
            .await
            .expect("create table")
    }

    /// A row for the multi-type fixture. `maybe` is `Option<i64>` to drive null cells.
    #[derive(Clone)]
    struct Row {
        id: i64,
        name: &'static str,
        day: i32,
        amount: i128,
        maybe: Option<i64>,
    }

    /// Writes a REAL parquet data file with `rows` into the (unpartitioned) table location.
    async fn write_data_file(table: &Table, file_name: &str, rows: &[Row]) -> DataFile {
        use crate::arrow::schema_to_arrow_schema;

        let schema = table.metadata().current_schema();
        let arrow_schema = Arc::new(schema_to_arrow_schema(schema).expect("arrow schema"));

        let ids: Vec<i64> = rows.iter().map(|row| row.id).collect();
        let names: Vec<&str> = rows.iter().map(|row| row.name).collect();
        let days: Vec<i32> = rows.iter().map(|row| row.day).collect();
        let amounts: Vec<i128> = rows.iter().map(|row| row.amount).collect();
        let maybes: Vec<Option<i64>> = rows.iter().map(|row| row.maybe).collect();

        let amount_array = Decimal128Array::from(amounts)
            .with_precision_and_scale(DECIMAL_PRECISION as u8, DECIMAL_SCALE as i8)
            .expect("decimal array");
        let batch = RecordBatch::try_new(arrow_schema, vec![
            Arc::new(Int64Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(names)) as ArrayRef,
            Arc::new(Date32Array::from(days)) as ArrayRef,
            Arc::new(amount_array) as ArrayRef,
            Arc::new(Int64Array::from(maybes)) as ArrayRef,
        ])
        .expect("record batch");

        let file_path = format!("{}/data/{}", table.metadata().location(), file_name);
        let output = table.file_io().new_output(file_path).expect("new output");
        let parquet_builder = ParquetWriterBuilder::new(
            parquet::file::properties::WriterProperties::builder().build(),
            schema.clone(),
        );
        let mut writer = parquet_builder.build(output).await.expect("build writer");
        writer.write(&batch).await.expect("write batch");
        let data_file_builders = writer.close().await.expect("close writer");

        let mut builder = data_file_builders
            .into_iter()
            .next()
            .expect("one data file");
        builder
            .content(DataContentType::Data)
            .partition_spec_id(0)
            .partition(Struct::empty())
            .build()
            .expect("build data file")
    }

    /// Writes a REAL equality-delete file deleting rows whose `id` (field id 1) is one of `delete_ids`.
    async fn write_equality_delete_file(table: &Table, delete_ids: &[i64]) -> DataFile {
        use crate::arrow::{arrow_schema_to_schema, schema_to_arrow_schema};

        let schema = table.metadata().current_schema().clone();
        let config = EqualityDeleteWriterConfig::new(vec![1], schema.clone()).expect("config");
        let delete_schema =
            Arc::new(arrow_schema_to_schema(config.projected_arrow_schema_ref()).expect("schema"));

        let location_gen = DefaultLocationGenerator::new(table.metadata().clone()).expect("locgen");
        let file_name_gen = DefaultFileNameGenerator::new(
            "eq-del".to_string(),
            Some(uuid::Uuid::now_v7().to_string()),
            DataFileFormat::Parquet,
        );
        let parquet_builder = ParquetWriterBuilder::new(
            parquet::file::properties::WriterProperties::builder().build(),
            delete_schema.clone(),
        );
        let rolling = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_builder,
            table.file_io().clone(),
            location_gen,
            file_name_gen,
        );
        let mut writer = EqualityDeleteFileWriterBuilder::new(rolling, config)
            .build(None)
            .await
            .expect("build delete writer");

        let arrow_schema = Arc::new(schema_to_arrow_schema(&delete_schema).expect("arrow schema"));
        let ids: Vec<i64> = delete_ids.to_vec();
        let batch = RecordBatch::try_new(arrow_schema, vec![
            Arc::new(Int64Array::from(ids)) as ArrayRef
        ])
        .expect("delete batch");
        writer.write(batch).await.expect("write deletes");
        writer
            .close()
            .await
            .expect("close delete writer")
            .into_iter()
            .next()
            .expect("one delete file")
    }

    async fn append_files(catalog: &impl Catalog, table: &Table, files: Vec<DataFile>) -> Table {
        let transaction = Transaction::new(table);
        let action = transaction.fast_append().add_data_files(files);
        let transaction = action.apply(transaction).expect("apply append");
        transaction.commit(catalog).await.expect("commit append")
    }

    async fn add_deletes(catalog: &impl Catalog, table: &Table, deletes: Vec<DataFile>) -> Table {
        let transaction = Transaction::new(table);
        let action = transaction.row_delta().add_deletes(deletes);
        let transaction = action.apply(transaction).expect("apply row_delta");
        transaction.commit(catalog).await.expect("commit row_delta")
    }

    /// The canonical 6-row fixture used by several tests. Hand-counted distinct values per column:
    ///   id      = {1, 1, 2, 3, 3, 4}            → 4 distinct (duplicates 1 and 3)
    ///   name    = {"a","a","b","c","c","c"}     → 3 distinct
    ///   day     = {10, 11, 12, 13, 14, 15}      → 6 distinct
    ///   amount  = {100,200,300,400,500,600}     → 6 distinct
    ///   maybe   = {Some(7),None,Some(7),Some(8),None,Some(9)} → 3 distinct (nulls excluded)
    fn canonical_rows() -> Vec<Row> {
        vec![
            Row {
                id: 1,
                name: "a",
                day: 10,
                amount: 100,
                maybe: Some(7),
            },
            Row {
                id: 1,
                name: "a",
                day: 11,
                amount: 200,
                maybe: None,
            },
            Row {
                id: 2,
                name: "b",
                day: 12,
                amount: 300,
                maybe: Some(7),
            },
            Row {
                id: 3,
                name: "c",
                day: 13,
                amount: 400,
                maybe: Some(8),
            },
            Row {
                id: 3,
                name: "c",
                day: 14,
                amount: 500,
                maybe: None,
            },
            Row {
                id: 4,
                name: "c",
                day: 15,
                amount: 600,
                maybe: Some(9),
            },
        ]
    }

    /// Reopens a written puffin file and returns each blob's (field id, type, ndv-property, estimate).
    async fn reopen(file_io: &FileIO, path: &str) -> Vec<(Vec<i32>, String, String, f64)> {
        let input_file = file_io.new_input(path).expect("input file");
        let reader = PuffinReader::new(input_file);
        let metadata = reader.file_metadata().await.expect("footer").clone();
        let mut result = Vec::new();
        for blob_metadata in &metadata.blobs().to_vec() {
            let blob = reader.blob(blob_metadata).await.expect("blob");
            let sketch = CompactThetaSketch::deserialize(blob.data()).expect("deserialize sketch");
            let ndv = blob.properties().get("ndv").cloned().unwrap_or_default();
            result.push((
                blob.fields().to_vec(),
                blob.blob_type().to_string(),
                ndv,
                sketch.estimate(),
            ));
        }
        result
    }

    // ============================================================================================
    // Crown jewel — multi-column table, hand-counted NDV per column, full round-trip.
    // ============================================================================================

    #[tokio::test]
    async fn test_crown_jewel_per_column_ndv_round_trips_against_hand_counts() {
        let (catalog, _guard) = local_fs_catalog().await;
        let table = create_unpartitioned_table(&catalog, multi_type_schema()).await;
        let table = append_files(&catalog, &table, vec![
            write_data_file(&table, "data.parquet", &canonical_rows()).await,
        ])
        .await;

        let snapshot_id = table.metadata().current_snapshot_id().unwrap();
        let file_io = table.file_io().clone();
        let result = ComputeTableStats::new(table)
            .execute(&catalog)
            .await
            .expect("compute table stats");

        // RISK: the blob set / field-ids / ndv must equal the hand counts (exact mode), and the
        // serialized sketch must round-trip to the same estimate.
        let statistics_file = &result.statistics_file;
        assert_eq!(statistics_file.snapshot_id, snapshot_id);
        assert_eq!(statistics_file.blob_metadata.len(), 5);
        // StatisticsFile size invariant (Java fileSize()/footerSize()): 0 < footer STRICTLY < total.
        // RISK: a footer_size that returned the TOTAL (or anything >= total) corrupts every reader —
        // Java locates the blob region as fileSize - footerSize, so footer must exclude the leading
        // MAGIC + the blob payloads. With 5 non-empty blobs the footer is always strictly smaller.
        // The `<=` form let a footer_size==total mutation survive (Y2-reviewer); `<` kills it.
        assert!(statistics_file.file_footer_size_in_bytes > 0);
        assert!(
            statistics_file.file_footer_size_in_bytes < statistics_file.file_size_in_bytes,
            "footer ({}) must be strictly smaller than the whole file ({}) — blob payloads precede it",
            statistics_file.file_footer_size_in_bytes,
            statistics_file.file_size_in_bytes
        );

        let blobs = reopen(&file_io, &statistics_file.statistics_path).await;
        assert_eq!(blobs.len(), 5, "one blob per primitive column");

        // Expected (field_id → hand-counted distinct count). All columns are small enough to be in
        // theta EXACT mode, so estimate == exact count to the integer.
        let expected: HashMap<i32, u64> = HashMap::from([(1, 4), (2, 3), (3, 6), (4, 6), (5, 3)]);
        for (fields, blob_type, ndv, estimate) in &blobs {
            assert_eq!(blob_type, APACHE_DATASKETCHES_THETA_V1);
            assert_eq!(fields.len(), 1, "one field id per column blob");
            let field_id = fields[0];
            let want = *expected.get(&field_id).expect("known field id");
            // Read-back estimate == exact count.
            assert_eq!(*estimate, want as f64, "field {field_id} estimate");
            // ndv property == (estimate as i64) decimal string == hand count (exact mode).
            assert_eq!(ndv, &want.to_string(), "field {field_id} ndv property");
        }

        // Registration re-parse: the committed metadata carries the statistics file.
        let committed = result
            .table
            .metadata()
            .statistics_for_snapshot(snapshot_id)
            .expect("registered statistics");
        assert_eq!(committed.statistics_path, statistics_file.statistics_path);
        assert_eq!(committed.blob_metadata.len(), 5);
    }

    // ============================================================================================
    // End-to-end estimation-mode ndv pin — drives the PRODUCTION `write_stats_file` so the blob's
    // `ndv` property is computed by the real code path (NOT reconstructed inline). RISK: the crown
    // jewel only feeds exact-mode (≤6 distinct) data, where the Alpha update estimate and the COMPACT
    // estimate coincide, so it cannot see which object `write_stats_file` reads. This test feeds a
    // high-cardinality (1M distinct) Alpha sketch where the two estimators DIVERGE (compact 1004032 vs
    // update sampling 1002319) and asserts the on-disk blob carries the COMPACT value — so a mutation
    // in `write_stats_file` that reads the update sketch's `estimate()` fails here with 1002319.
    // ============================================================================================

    #[tokio::test]
    async fn test_write_stats_file_ndv_property_reads_compact_estimate_in_estimation_mode() {
        // 1M distinct longs ⇒ Alpha is in estimation mode (theta < MAX, sampling phase). Cross-checked
        // against datasketches-java-3.3.0 (lgK 12 / seed 9001): COMPACT getEstimate 1004032.297 → ndv
        // 1004032; the live Alpha UPDATE sampling estimate is the DIFFERENT 1002319. NDVSketchUtil reads
        // the compact one — the on-disk `ndv` property MUST be 1004032.
        const DISTINCT: u64 = 1_000_000;
        const JAVA_ALPHA_COMPACT_NDV: i64 = 1_004_032;
        const ALPHA_UPDATE_SAMPLING_NDV: i64 = 1_002_319; // the WRONG value an update-estimate read emits

        let (catalog, _guard) = local_fs_catalog().await;
        let table = create_unpartitioned_table(&catalog, multi_type_schema()).await;
        // Append one tiny data file just to mint a real snapshot to stamp the blob with.
        let table = append_files(&catalog, &table, vec![
            write_data_file(&table, "seed.parquet", &canonical_rows()).await,
        ])
        .await;
        let snapshot = table
            .metadata()
            .current_snapshot()
            .expect("a current snapshot")
            .clone();

        // Build the action's per-column Alpha sketch directly, fed the same LE8 byte form the action
        // feeds, then call the PRODUCTION write path.
        let mut sketch = AlphaSketch::new();
        for value in 0..DISTINCT {
            sketch.update_bytes(&value.to_le_bytes());
        }
        // Sanity: the live update sketch's own estimate IS the divergent value (so the assertion below
        // genuinely discriminates the two objects, not a coincidence).
        assert_eq!(sketch.estimate() as i64, ALPHA_UPDATE_SAMPLING_NDV);

        let columns = vec![StatsColumn {
            name: "id".to_string(),
            field_id: 1,
            primitive_type: PrimitiveType::Long,
        }];
        let path = format!("{}/metadata/est-ndv.stats", table.metadata().location());
        let statistics_file =
            write_stats_file(table.file_io(), &path, &snapshot, &columns, vec![sketch])
                .await
                .expect("write stats file");

        let blobs = reopen(table.file_io(), &statistics_file.statistics_path).await;
        assert_eq!(blobs.len(), 1);
        let (fields, _blob_type, ndv, _estimate) = &blobs[0];
        assert_eq!(fields, &vec![1]);
        assert_eq!(
            ndv,
            &JAVA_ALPHA_COMPACT_NDV.to_string(),
            "the on-disk ndv property must be the COMPACT estimate (Java NDVSketchUtil), not the Alpha \
             update sketch's sampling estimate {ALPHA_UPDATE_SAMPLING_NDV}"
        );
        assert_ne!(
            ndv,
            &ALPHA_UPDATE_SAMPLING_NDV.to_string(),
            "reading the update sketch's estimate would emit {ALPHA_UPDATE_SAMPLING_NDV} — the bug this pins"
        );
    }

    // ============================================================================================
    // Per-type byte-form pins — the Conversions.toByteBuffer single-value serialization, declared
    // by hand. RISK: feeding the JSON / display form (or BE longs, or length-prefixed strings)
    // diverges every NDV silently. A sketch fed the hand-declared bytes must equal the action's.
    // ============================================================================================

    fn datum_bytes(datum: &Datum) -> Vec<u8> {
        datum.to_bytes().expect("to_bytes").to_vec()
    }

    #[test]
    fn test_byte_form_long_is_8_byte_little_endian() {
        // Java Conversions.toByteBuffer: allocate(8).order(LITTLE_ENDIAN).putLong.
        let datum = Datum::new(PrimitiveType::Long, crate::spec::PrimitiveLiteral::Long(5));
        assert_eq!(datum_bytes(&datum), vec![5, 0, 0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn test_byte_form_date_is_4_byte_little_endian_int() {
        // Java: date is encoded exactly like int — allocate(4).order(LITTLE_ENDIAN).putInt(days).
        let datum = Datum::new(
            PrimitiveType::Date,
            crate::spec::PrimitiveLiteral::Int(18_000),
        );
        assert_eq!(datum_bytes(&datum), 18_000i32.to_le_bytes().to_vec());
        assert_eq!(datum_bytes(&datum).len(), 4);
    }

    #[test]
    fn test_byte_form_string_is_unprefixed_utf8() {
        // Java: CharBuffer.wrap(value) → UTF-8 encoder → raw bytes, NO length prefix.
        let datum = Datum::new(
            PrimitiveType::String,
            crate::spec::PrimitiveLiteral::String("abc".to_string()),
        );
        assert_eq!(datum_bytes(&datum), vec![0x61, 0x62, 0x63]);
    }

    #[test]
    fn test_byte_form_decimal_is_big_endian_minimal_unscaled() {
        // Java: BigDecimal.unscaledValue().toByteArray() → big-endian two's-complement minimal.
        // unscaled 300 = 0x012C → minimal BE two's-complement [0x01, 0x2C].
        let datum = Datum::new(
            PrimitiveType::Decimal {
                precision: 9,
                scale: 2,
            },
            crate::spec::PrimitiveLiteral::Int128(300),
        );
        assert_eq!(datum_bytes(&datum), vec![0x01, 0x2C]);
    }

    #[test]
    fn test_byte_form_decimal_negative_and_zero_match_java_biginteger() {
        // EDGE: the 4 hand pins skip negative + zero unscaled values. Java emits
        // BigInteger.unscaledValue().toByteArray() — big-endian two's-complement MINIMAL — for these.
        // Expected bytes derived from a Java probe (BigInteger.valueOf(v).toByteArray()):
        //   -1 → ff ; 0 → 00 ; -300 → fe d4 ; -10^18 (scale-0) → f2 1f 49 4c 58 9c 00 00.
        // A wrong sign-extension or a non-minimal/padded encoding would hash a different byte string
        // and diverge every negative-decimal column's NDV from Java/Spark/Trino.
        let make = |unscaled: i128, precision: u32, scale: u32| {
            Datum::new(
                PrimitiveType::Decimal { precision, scale },
                crate::spec::PrimitiveLiteral::Int128(unscaled),
            )
        };
        assert_eq!(datum_bytes(&make(-1, 9, 2)), vec![0xFF]);
        assert_eq!(datum_bytes(&make(0, 9, 2)), vec![0x00]);
        assert_eq!(datum_bytes(&make(-300, 9, 2)), vec![0xFE, 0xD4]);
        // -10^18 at a precision wide enough to hold it (precision 19 ⇒ 9 required bytes, the minimal
        // 8-byte two's-complement form fits unpadded).
        assert_eq!(datum_bytes(&make(-1_000_000_000_000_000_000, 19, 0)), vec![
            0xF2, 0x1F, 0x49, 0x4C, 0x58, 0x9C, 0x00, 0x00,
        ]);
    }

    #[test]
    fn test_byte_form_uuid_is_16_byte_big_endian() {
        // EDGE: uuid feeds 16 bytes big-endian (Java ByteBuffer.allocate(16).putLong(msb).putLong(lsb)).
        // The fork stores a uuid literal as PrimitiveLiteral::UInt128 (the 128-bit value, MSB-first via
        // to_be_bytes). For 12345678-1234-5678-1234-567812345678 the BE bytes are the nibble sequence
        // itself — a LE emission (or a Hyphenated/string form) would diverge every uuid column.
        let uuid = uuid::Uuid::parse_str("12345678-1234-5678-1234-567812345678").expect("uuid");
        let datum = Datum::uuid(uuid);
        assert_eq!(datum_bytes(&datum), vec![
            0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34,
            0x56, 0x78,
        ]);
        assert_eq!(datum_bytes(&datum).len(), 16);
    }

    #[test]
    fn test_byte_form_float_feeds_raw_bits_le_including_nan_and_negative_zero() {
        // EDGE: Java Conversions.toByteBuffer emits putFloat (LE) which writes RAW IEEE-754 bits — it
        // does NOT canonicalize NaN and preserves -0.0. Rust f32::to_le_bytes() also writes raw bits
        // (to_bits = transmute). A canonicalizing encoder (or floatToIntBits, which is also raw here)
        // that ever NORMALIZED NaN would hash a different value than Java for any NaN/-0.0 cell.
        // Expected (Java probe, LE): NaN → 00 00 c0 7f ; -0.0 → 00 00 00 80 ; 1.5 → 00 00 c0 3f.
        let nan = Datum::float(f32::NAN);
        assert_eq!(datum_bytes(&nan), vec![0x00, 0x00, 0xC0, 0x7F]);
        let neg_zero = Datum::float(-0.0f32);
        assert_eq!(datum_bytes(&neg_zero), vec![0x00, 0x00, 0x00, 0x80]);
        let one_five = Datum::float(1.5f32);
        assert_eq!(datum_bytes(&one_five), vec![0x00, 0x00, 0xC0, 0x3F]);
    }

    #[test]
    fn test_byte_form_double_feeds_raw_bits_le_including_nan() {
        // EDGE: double = putDouble (LE), raw 8-byte IEEE-754 bits. Rust f64::to_le_bytes is identical.
        // Expected (Java probe, LE): NaN → 00 00 00 00 00 00 f8 7f ; -0.0 → ...00 80 ; 1.5 → ...f8 3f.
        let nan = Datum::double(f64::NAN);
        assert_eq!(datum_bytes(&nan), vec![
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF8, 0x7F,
        ]);
        assert_eq!(datum_bytes(&Datum::double(-0.0f64)), vec![
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80,
        ]);
        assert_eq!(datum_bytes(&Datum::double(1.5f64)), vec![
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF8, 0x3F,
        ]);
    }

    #[test]
    fn test_byte_form_boolean_is_single_byte() {
        // EDGE: boolean feeds a single byte 0x01 (true) / 0x00 (false). A multi-byte (e.g. 4-byte int)
        // emission would diverge every boolean column.
        assert_eq!(datum_bytes(&Datum::bool(true)), vec![0x01]);
        assert_eq!(datum_bytes(&Datum::bool(false)), vec![0x00]);
    }

    #[test]
    fn test_feed_column_matches_hand_fed_byte_sketch_for_strings() {
        // The action feeds update_bytes(Datum::to_bytes()). A hand-built sketch fed the SAME bytes
        // must produce the identical serialized payload — pins that the action uses the byte form,
        // not the display/JSON form.
        let mut action_sketch = AlphaSketch::new();
        let literals: Vec<Option<Literal>> = ["x", "y", "x"]
            .iter()
            .map(|s| Some(Literal::string(*s)))
            .collect();
        feed_column(&mut action_sketch, &PrimitiveType::String, literals).expect("feed");

        let mut hand_sketch = AlphaSketch::new();
        for value in ["x", "y", "x"] {
            hand_sketch.update_bytes(value.as_bytes());
        }

        assert_eq!(
            action_sketch.serialize_compact().unwrap(),
            hand_sketch.serialize_compact().unwrap(),
            "action must feed the single-value byte form, identically to a hand-fed sketch"
        );
        assert_eq!(action_sketch.estimate(), 2.0);
    }

    #[test]
    fn test_feed_column_rejects_json_form_drift() {
        // MUTATION BAIT: if the action fed the JSON/display form ("x" → "\"x\"" or similar) the bytes
        // would differ. A sketch fed the JSON-quoted bytes must NOT match the byte-form sketch.
        let mut byte_form = AlphaSketch::new();
        feed_column(&mut byte_form, &PrimitiveType::String, vec![Some(
            Literal::string("x"),
        )])
        .expect("feed");

        let mut json_form = AlphaSketch::new();
        json_form.update_bytes("\"x\"".as_bytes()); // the JSON-serialized form

        assert_ne!(
            byte_form.serialize_compact().unwrap(),
            json_form.serialize_compact().unwrap(),
            "the JSON form must NOT collide with the single-value byte form"
        );
    }

    // ============================================================================================
    // Estimation-mode value pin — the Y2 STOP-finding CLOSED on Alpha (Y3). The action now builds an
    // Alpha sketch and reads the `ndv` off the COMPACT sketch, so a high-cardinality column's `ndv`
    // matches what Spark/Trino/Flink write. This pins the Alpha value AND proves the suite discriminates
    // families: a revert to QuickSelect would break this pin while the exact-mode pins survive.
    // ============================================================================================

    #[test]
    fn test_estimation_mode_ndv_matches_java_alpha_not_quickselect() {
        // 1_000_000 distinct longs fed in the action's single-value byte form (LE8 — same input as the
        // Java probe). Cross-checked against datasketches-java-3.3.0 probes (lgK 12 / seed 9001):
        //   Java ALPHA pipeline (UpdateSketch.builder().setFamily(ALPHA).build() → compact()):
        //     compact retained 4103, theta 37691512080307240, COMPACT getEstimate 1004032.297 → ndv 1004032.
        //   Java QUICKSELECT (UpdateSketch.builder().build() → compact()): ndv 1002714 — DIFFERENT.
        // The action reads the COMPACT sketch's estimate (Java NDVSketchUtil), which for Alpha is 1004032.
        const DISTINCT: u64 = 1_000_000;
        const JAVA_ALPHA_NDV: i64 = 1_004_032; // == what Spark/Trino/Flink actually write
        const JAVA_QUICKSELECT_NDV: i64 = 1_002_714; // the OLD (wrong-family) value Y2 emitted

        // The action's exact code path: feed an Alpha sketch the LE8 bytes, serialize, read the ndv
        // off the COMPACT sketch.
        let mut alpha = AlphaSketch::new();
        for value in 0..DISTINCT {
            alpha.update_bytes(&value.to_le_bytes());
        }
        let payload = alpha.serialize_compact().unwrap();
        let action_ndv = CompactThetaSketch::deserialize(&payload)
            .unwrap()
            .estimate() as i64;

        assert_eq!(
            action_ndv, JAVA_ALPHA_NDV,
            "the action's ndv must equal the Java Alpha NDV pipeline (compact estimate)"
        );
        assert_eq!(
            action_ndv.to_string(),
            JAVA_ALPHA_NDV.to_string(),
            "the ndv property string in estimation mode"
        );

        // FAMILY DISCRIMINATION (the mutation proof): reverting the action to QuickSelect would make
        // this same 1M input render 1002714, NOT 1004032 — so the pin fails on a family revert. Confirm
        // the QuickSelect value really is different (and equals the OLD Y2 ndv).
        let mut quickselect = ThetaSketch::new();
        for value in 0..DISTINCT {
            quickselect.update_bytes(&value.to_le_bytes());
        }
        let quickselect_ndv =
            CompactThetaSketch::deserialize(&quickselect.serialize_compact().unwrap())
                .unwrap()
                .estimate() as i64;
        assert_eq!(
            quickselect_ndv, JAVA_QUICKSELECT_NDV,
            "QuickSelect ndv (the reverted value)"
        );
        assert_ne!(
            action_ndv, quickselect_ndv,
            "Alpha and QuickSelect ndv MUST differ in estimation mode — the gap Y3 closes"
        );
    }

    // ============================================================================================
    // Nulls excluded — a null cell never updates the sketch.
    // ============================================================================================

    #[test]
    fn test_nulls_are_excluded_from_distinct_count() {
        let mut sketch = AlphaSketch::new();
        let literals = vec![
            Some(Literal::long(7)),
            None, // null cell
            Some(Literal::long(7)),
            None,
            Some(Literal::long(8)),
        ];
        feed_column(&mut sketch, &PrimitiveType::Long, literals).expect("feed");
        // 2 distinct non-null values; nulls contribute nothing.
        assert_eq!(sketch.estimate(), 2.0);
    }

    #[tokio::test]
    async fn test_nullable_column_ndv_excludes_nulls_end_to_end() {
        let (catalog, _guard) = local_fs_catalog().await;
        let table = create_unpartitioned_table(&catalog, multi_type_schema()).await;
        let table = append_files(&catalog, &table, vec![
            write_data_file(&table, "data.parquet", &canonical_rows()).await,
        ])
        .await;

        let file_io = table.file_io().clone();
        let result = ComputeTableStats::new(table)
            .columns(["maybe"])
            .execute(&catalog)
            .await
            .expect("compute");

        let blobs = reopen(&file_io, &result.statistics_file.statistics_path).await;
        assert_eq!(blobs.len(), 1);
        // maybe = {7, null, 7, 8, null, 9} → 3 distinct non-null.
        assert_eq!(blobs[0].0, vec![5]);
        assert_eq!(blobs[0].3, 3.0);
        assert_eq!(blobs[0].2, "3");
    }

    // ============================================================================================
    // Deletes applied — a deleted row's value is absent from the NDV (MoR fixture).
    // ============================================================================================

    #[tokio::test]
    async fn test_deleted_rows_are_excluded_from_distinct_count() {
        let (catalog, _guard) = local_fs_catalog().await;
        let table = create_unpartitioned_table(&catalog, multi_type_schema()).await;
        // Rows with id = {1,2,3,4,5}, all distinct. Delete id=4 and id=5 via equality delete.
        let rows: Vec<Row> = (1..=5)
            .map(|n| Row {
                id: n,
                name: "x",
                day: 10 + n as i32,
                amount: 100 * n as i128,
                maybe: Some(n),
            })
            .collect();
        let table = append_files(&catalog, &table, vec![
            write_data_file(&table, "data.parquet", &rows).await,
        ])
        .await;
        let delete_file = write_equality_delete_file(&table, &[4, 5]).await;
        let table = add_deletes(&catalog, &table, vec![delete_file]).await;

        let file_io = table.file_io().clone();
        let result = ComputeTableStats::new(table)
            .columns(["id"])
            .execute(&catalog)
            .await
            .expect("compute");

        let blobs = reopen(&file_io, &result.statistics_file.statistics_path).await;
        assert_eq!(blobs.len(), 1);
        // id ∈ {1,2,3} survive (4 and 5 deleted) → 3 distinct. RISK: if deletes were NOT applied the
        // NDV would be 5.
        assert_eq!(blobs[0].3, 3.0, "deleted ids must be absent from the NDV");
        assert_eq!(blobs[0].2, "3");
    }

    // ============================================================================================
    // Column selection + non-existent / non-primitive column errors.
    // ============================================================================================

    #[tokio::test]
    async fn test_column_selection_limits_blobs() {
        let (catalog, _guard) = local_fs_catalog().await;
        let table = create_unpartitioned_table(&catalog, multi_type_schema()).await;
        let table = append_files(&catalog, &table, vec![
            write_data_file(&table, "data.parquet", &canonical_rows()).await,
        ])
        .await;

        let file_io = table.file_io().clone();
        let result = ComputeTableStats::new(table)
            .columns(["id", "name"])
            .execute(&catalog)
            .await
            .expect("compute");

        let blobs = reopen(&file_io, &result.statistics_file.statistics_path).await;
        let mut field_ids: Vec<i32> = blobs.iter().flat_map(|b| b.0.clone()).collect();
        field_ids.sort_unstable();
        assert_eq!(field_ids, vec![1, 2], "only id and name selected");
    }

    #[tokio::test]
    async fn test_non_existent_column_errors() {
        let (catalog, _guard) = local_fs_catalog().await;
        let table = create_unpartitioned_table(&catalog, multi_type_schema()).await;
        let table = append_files(&catalog, &table, vec![
            write_data_file(&table, "data.parquet", &canonical_rows()).await,
        ])
        .await;

        let error = ComputeTableStats::new(table)
            .columns(["nope"])
            .execute(&catalog)
            .await
            .expect_err("non-existent column must error");
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert!(error.to_string().contains("Can't find column nope"));
    }

    // ============================================================================================
    // Multi-snapshot — stats are keyed to the COMPUTED snapshot.
    // ============================================================================================

    #[tokio::test]
    async fn test_stats_keyed_to_requested_snapshot() {
        let (catalog, _guard) = local_fs_catalog().await;
        let table = create_unpartitioned_table(&catalog, multi_type_schema()).await;

        // Snapshot 1: 3 rows, id = {1,2,3} (3 distinct).
        let rows_one: Vec<Row> = (1..=3)
            .map(|n| Row {
                id: n,
                name: "x",
                day: 10,
                amount: 100,
                maybe: Some(n),
            })
            .collect();
        let table = append_files(&catalog, &table, vec![
            write_data_file(&table, "s1.parquet", &rows_one).await,
        ])
        .await;
        let snapshot_one = table.metadata().current_snapshot_id().unwrap();

        // Snapshot 2: append 3 more rows, id = {4,5,6} → total 6 distinct.
        let rows_two: Vec<Row> = (4..=6)
            .map(|n| Row {
                id: n,
                name: "x",
                day: 10,
                amount: 100,
                maybe: Some(n),
            })
            .collect();
        let table = append_files(&catalog, &table, vec![
            write_data_file(&table, "s2.parquet", &rows_two).await,
        ])
        .await;
        let snapshot_two = table.metadata().current_snapshot_id().unwrap();
        assert_ne!(snapshot_one, snapshot_two);

        // Compute over snapshot ONE: must see only the first 3 distinct ids.
        let file_io = table.file_io().clone();
        let result = ComputeTableStats::new(table.clone())
            .snapshot_id(snapshot_one)
            .columns(["id"])
            .execute(&catalog)
            .await
            .expect("compute s1");
        assert_eq!(result.statistics_file.snapshot_id, snapshot_one);
        let blobs = reopen(&file_io, &result.statistics_file.statistics_path).await;
        assert_eq!(blobs[0].3, 3.0, "snapshot 1 sees 3 distinct ids");

        // Compute over the CURRENT (second) snapshot: 6 distinct.
        let result_two = ComputeTableStats::new(table)
            .columns(["id"])
            .execute(&catalog)
            .await
            .expect("compute current");
        assert_eq!(result_two.statistics_file.snapshot_id, snapshot_two);
        let blobs_two = reopen(&file_io, &result_two.statistics_file.statistics_path).await;
        assert_eq!(blobs_two[0].3, 6.0, "current snapshot sees 6 distinct ids");
    }

    #[tokio::test]
    async fn test_unknown_snapshot_id_errors() {
        let (catalog, _guard) = local_fs_catalog().await;
        let table = create_unpartitioned_table(&catalog, multi_type_schema()).await;
        let table = append_files(&catalog, &table, vec![
            write_data_file(&table, "data.parquet", &canonical_rows()).await,
        ])
        .await;

        let error = ComputeTableStats::new(table)
            .snapshot_id(999_999)
            .execute(&catalog)
            .await
            .expect_err("unknown snapshot must error");
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert!(error.to_string().contains("999999"));
    }

    // ============================================================================================
    // Mutation bait — swapping two columns' sketches must break the field-id ↔ NDV pairing.
    // ============================================================================================

    #[tokio::test]
    async fn test_field_id_pairing_is_load_bearing() {
        // id has 4 distinct, name has 3 distinct. The blob for field 1 (id) MUST carry ndv 4 and the
        // blob for field 2 (name) MUST carry ndv 3 — a swap would surface here.
        let (catalog, _guard) = local_fs_catalog().await;
        let table = create_unpartitioned_table(&catalog, multi_type_schema()).await;
        let table = append_files(&catalog, &table, vec![
            write_data_file(&table, "data.parquet", &canonical_rows()).await,
        ])
        .await;

        let file_io = table.file_io().clone();
        let result = ComputeTableStats::new(table)
            .columns(["id", "name"])
            .execute(&catalog)
            .await
            .expect("compute");
        let blobs = reopen(&file_io, &result.statistics_file.statistics_path).await;

        let by_field: HashMap<i32, f64> = blobs.iter().map(|b| (b.0[0], b.3)).collect();
        assert_eq!(by_field.get(&1), Some(&4.0), "field 1 (id) ndv");
        assert_eq!(by_field.get(&2), Some(&3.0), "field 2 (name) ndv");
    }
}
