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

//! The `partitions` metadata table — a per-partition rollup over the current snapshot.
//!
//! This is the first AGGREGATING inspection table. It reads the current snapshot's LIVE manifest entries
//! (data AND delete, Added/Existing — [`crate::spec::ManifestEntry::is_alive`]), groups them by partition
//! value, and emits one row per partition with the rolled-up data/position-delete/equality-delete counts,
//! the total data-file size, and the spec/commit-time of the most-recent snapshot that touched the
//! partition — mirroring Java `PartitionsTable` / `PartitionsTable.Partition.update`.
//!
//! References:
//! - <https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/PartitionsTable.java>
//!
//! ## Scoping decisions (Java divergences, tested + documented)
//!
//! 1. **Unpartitioned partition column.** Java `PartitionsTable.schema()` DROPS the `partition` column for
//!    an unpartitioned table (`TypeUtil.select` excludes it). This port KEEPS an empty-struct (`Struct([])`)
//!    `partition` column — matching the `files`-family precedent
//!    (`inspect/files.rs`, `test_files_table_unpartitioned_keeps_empty_partition_struct_known_divergence`)
//!    so the whole `inspect` module has ONE consistent, documented unpartitioned-column divergence rather
//!    than two different behaviors. Non-corrupting (the single root row + every other column is correct);
//!    pinned by `test_partitions_table_unpartitioned_keeps_empty_partition_struct_known_divergence`.
//!
//! 2. **Multi-spec partition evolution.** Java unifies all of a table's partition specs into ONE partition
//!    type via `Partitioning.partitionType(table)` and coerces each file's partition into it
//!    (`PartitionUtil.coercePartition`). The Rust core has NO cross-spec partition-type unifier (only
//!    `TableMetadata::default_partition_type`), so this table keys rows by the file's OWN partition `Struct`
//!    and uses the DEFAULT partition type for the `partition` column's schema. This is correct for the
//!    single-spec (no partition-evolution) case. Under partition EVOLUTION (multiple specs with differently
//!    shaped partition tuples) the rows are not coerced into a unified type — a known divergence deferred
//!    until a `Partitioning.partitionType` analogue lands (tracked in GAP_MATRIX + `task/todo.md`). The
//!    per-file `spec_id` is still reported, so no data is silently misattributed within a single spec.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::builder::{
    Int32Builder, Int64Builder, StructBuilder, TimestampMicrosecondBuilder,
};
use arrow_schema::{DataType, Fields};
use futures::{StreamExt, stream};

use super::data_file::append_partition;
use crate::arrow::schema_to_arrow_schema;
use crate::scan::ArrowRecordBatchStream;
use crate::spec::{
    DataContentType, Literal, NestedField, PrimitiveLiteral, PrimitiveType, Schema, Struct,
    StructType, Type,
};
use crate::table::Table;
use crate::{Error, ErrorKind, Result};

/// The `partitions` metadata table (Java `PartitionsTable`).
pub struct PartitionsTable<'a> {
    table: &'a Table,
}

impl<'a> PartitionsTable<'a> {
    /// Create a new `partitions` table instance.
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Whether the table is unpartitioned (its default partition type has no fields).
    fn is_unpartitioned(&self) -> bool {
        self.table
            .metadata()
            .default_partition_type()
            .fields()
            .is_empty()
    }

    /// Returns the iceberg schema of the `partitions` metadata table.
    ///
    /// The 11 fields are in Java `PartitionsTable` COLUMN order with the EXACT (non-sequential) Java field
    /// ids: `partition`/1 (struct = the table's default partition type, REQUIRED), `spec_id`/4 int,
    /// `record_count`/2 long, `file_count`/3 int, `total_data_file_size_in_bytes`/11 long,
    /// `position_delete_record_count`/5 long, `position_delete_file_count`/6 int,
    /// `equality_delete_record_count`/7 long, `equality_delete_file_count`/8 int, `last_updated_at`/9
    /// timestamptz OPTIONAL, `last_updated_snapshot_id`/10 long OPTIONAL.
    ///
    /// For an UNPARTITIONED table the `partition` column is kept as an empty struct (a documented
    /// divergence from Java, which drops it — see the module doc, decision 1).
    pub fn schema(&self) -> Schema {
        let partition_type = self.table.metadata().default_partition_type().clone();

        let mut fields = Vec::with_capacity(11);
        if !self.is_unpartitioned() {
            fields.push(Arc::new(NestedField::required(
                1,
                "partition",
                Type::Struct(partition_type),
            )));
        } else {
            // Decision 1: keep an empty-struct `partition` column (matches the `files` family), so the
            // unpartitioned table still has a `partition` column rather than dropping it like Java.
            fields.push(Arc::new(NestedField::required(
                1,
                "partition",
                Type::Struct(StructType::new(vec![])),
            )));
        }
        fields.extend([
            Arc::new(NestedField::required(
                4,
                "spec_id",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(NestedField::required(
                2,
                "record_count",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::required(
                3,
                "file_count",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(NestedField::required(
                11,
                "total_data_file_size_in_bytes",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::required(
                5,
                "position_delete_record_count",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::required(
                6,
                "position_delete_file_count",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(NestedField::required(
                7,
                "equality_delete_record_count",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::required(
                8,
                "equality_delete_file_count",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(NestedField::optional(
                9,
                "last_updated_at",
                Type::Primitive(PrimitiveType::Timestamptz),
            )),
            Arc::new(NestedField::optional(
                10,
                "last_updated_snapshot_id",
                Type::Primitive(PrimitiveType::Long),
            )),
        ]);

        Schema::builder()
            .with_fields(fields)
            .build()
            .expect("partitions metadata table schema is statically valid")
    }

    /// Scans the `partitions` metadata table.
    ///
    /// Reads the current snapshot's manifest list → all manifests (data AND delete) → live entries, groups
    /// them by partition value, and rolls each group up per Java `PartitionsTable.Partition.update`:
    /// DATA files contribute record/file/size; POSITION_DELETES and EQUALITY_DELETES contribute their
    /// respective counts; and `last_updated_at`/`last_updated_snapshot_id`/`spec_id` come from the file
    /// whose committing snapshot has the most-recent commit time (`>` strictly, so the first writer wins a
    /// tie — matching Java). Rows are sorted deterministically by partition value. An empty table (no
    /// current snapshot) yields one empty batch.
    pub async fn scan(&self) -> Result<ArrowRecordBatchStream> {
        let metadata = self.table.metadata();
        let partition_type = metadata.default_partition_type().clone();

        // Aggregate the live entries by partition value.
        let mut partitions: HashMap<Struct, Partition> = HashMap::new();
        if let Some(snapshot) = metadata.current_snapshot() {
            let manifest_list = snapshot
                .load_manifest_list(self.table.file_io(), metadata)
                .await?;
            for manifest_file in manifest_list.entries() {
                let manifest = manifest_file.load_manifest(self.table.file_io()).await?;
                for entry in manifest.entries() {
                    if !entry.is_alive() {
                        continue;
                    }
                    let data_file = entry.data_file();
                    let key = data_file.partition().clone();
                    let partition = partitions
                        .entry(key.clone())
                        .or_insert_with(|| Partition::new(key));

                    // last_updated_* + spec_id are set from the most-recent-commit file (Java
                    // `Partition.update`: snapshot commit time in MICROS, strict `>` so first-writer wins).
                    if let Some(snapshot_id) = entry.snapshot_id()
                        && let Some(committing) = metadata.snapshot_by_id(snapshot_id)
                    {
                        let commit_time_micros = committing.timestamp_ms() * 1000;
                        if partition
                            .last_updated_at
                            .is_none_or(|current| commit_time_micros > current)
                        {
                            partition.spec_id = data_file.partition_spec_id;
                            partition.last_updated_at = Some(commit_time_micros);
                            partition.last_updated_snapshot_id = Some(committing.snapshot_id());
                        }
                    }

                    match data_file.content_type() {
                        DataContentType::Data => {
                            partition.data_record_count += data_file.record_count();
                            partition.data_file_count += 1;
                            partition.total_data_file_size_in_bytes +=
                                data_file.file_size_in_bytes();
                        }
                        DataContentType::PositionDeletes => {
                            partition.position_delete_record_count += data_file.record_count();
                            partition.position_delete_file_count += 1;
                        }
                        DataContentType::EqualityDeletes => {
                            partition.equality_delete_record_count += data_file.record_count();
                            partition.equality_delete_file_count += 1;
                        }
                    }
                }
            }
        }

        // Deterministic output: sort the partition rows by their partition tuple value.
        let mut rows: Vec<Partition> = partitions.into_values().collect();
        rows.sort_by(|left, right| compare_partition_values(&left.key, &right.key));

        let arrow_schema = Arc::new(schema_to_arrow_schema(&self.schema())?);
        let partition_fields = partition_arrow_fields(&arrow_schema)?;
        let batch = self.build_batch(arrow_schema, &partition_type, &partition_fields, &rows)?;
        Ok(stream::iter(vec![Ok(batch)]).boxed())
    }

    /// Builds the single output [`RecordBatch`] from the rolled-up partition rows.
    fn build_batch(
        &self,
        arrow_schema: Arc<arrow_schema::Schema>,
        partition_type: &StructType,
        partition_fields: &Fields,
        rows: &[Partition],
    ) -> Result<RecordBatch> {
        let mut partition = StructBuilder::from_fields(partition_fields.clone(), rows.len());
        let mut spec_id = Int32Builder::new();
        let mut record_count = Int64Builder::new();
        let mut file_count = Int32Builder::new();
        let mut total_data_file_size_in_bytes = Int64Builder::new();
        let mut position_delete_record_count = Int64Builder::new();
        let mut position_delete_file_count = Int32Builder::new();
        let mut equality_delete_record_count = Int64Builder::new();
        let mut equality_delete_file_count = Int32Builder::new();
        let mut last_updated_at = TimestampMicrosecondBuilder::new().with_timezone("+00:00");
        let mut last_updated_snapshot_id = Int64Builder::new();

        for row in rows {
            append_partition(&mut partition, partition_type, &row.key)?;
            spec_id.append_value(row.spec_id);
            record_count.append_value(row.data_record_count as i64);
            file_count.append_value(row.data_file_count);
            total_data_file_size_in_bytes.append_value(row.total_data_file_size_in_bytes as i64);
            position_delete_record_count.append_value(row.position_delete_record_count as i64);
            position_delete_file_count.append_value(row.position_delete_file_count);
            equality_delete_record_count.append_value(row.equality_delete_record_count as i64);
            equality_delete_file_count.append_value(row.equality_delete_file_count);
            last_updated_at.append_option(row.last_updated_at);
            last_updated_snapshot_id.append_option(row.last_updated_snapshot_id);
        }

        let batch = RecordBatch::try_new(arrow_schema, vec![
            Arc::new(partition.finish()),
            Arc::new(spec_id.finish()),
            Arc::new(record_count.finish()),
            Arc::new(file_count.finish()),
            Arc::new(total_data_file_size_in_bytes.finish()),
            Arc::new(position_delete_record_count.finish()),
            Arc::new(position_delete_file_count.finish()),
            Arc::new(equality_delete_record_count.finish()),
            Arc::new(equality_delete_file_count.finish()),
            Arc::new(last_updated_at.finish()),
            Arc::new(last_updated_snapshot_id.finish()),
        ])?;
        Ok(batch)
    }
}

/// One partition's rolled-up aggregates (Java `PartitionsTable.Partition`).
///
/// Counts use `u64` because the underlying `DataFile::{record_count, file_size_in_bytes}` are `u64`; the
/// Arrow output narrows the record/size counts to `i64` (matching Java's `long` columns).
struct Partition {
    /// The partition tuple value (the aggregation key).
    key: Struct,
    spec_id: i32,
    data_record_count: u64,
    data_file_count: i32,
    total_data_file_size_in_bytes: u64,
    position_delete_record_count: u64,
    position_delete_file_count: i32,
    equality_delete_record_count: u64,
    equality_delete_file_count: i32,
    /// Commit time (MICROS) of the most-recent snapshot that touched this partition; `None` until set.
    last_updated_at: Option<i64>,
    last_updated_snapshot_id: Option<i64>,
}

impl Partition {
    fn new(key: Struct) -> Self {
        Self {
            key,
            spec_id: 0,
            data_record_count: 0,
            data_file_count: 0,
            total_data_file_size_in_bytes: 0,
            position_delete_record_count: 0,
            position_delete_file_count: 0,
            equality_delete_record_count: 0,
            equality_delete_file_count: 0,
            last_updated_at: None,
            last_updated_snapshot_id: None,
        }
    }
}

/// Extracts the `partition` column's Arrow child [`Fields`] from the table's Arrow schema, so the
/// partition [`StructBuilder`] is built with the exact child fields (and their field-id metadata) the
/// output schema declares.
fn partition_arrow_fields(arrow_schema: &arrow_schema::Schema) -> Result<Fields> {
    let partition_field = arrow_schema.field_with_name("partition").map_err(|error| {
        Error::new(
            ErrorKind::Unexpected,
            format!("partitions metadata table is missing its `partition` column: {error}"),
        )
    })?;
    match partition_field.data_type() {
        DataType::Struct(fields) => Ok(fields.clone()),
        other => Err(Error::new(
            ErrorKind::Unexpected,
            format!("partitions metadata table `partition` column is not a struct: {other:?}"),
        )),
    }
}

/// Compares two partition tuples field-by-field for a deterministic row order.
///
/// Mirrors Java's `Comparators.forType(partitionType)` ordering for the common case: nulls sort first,
/// then each field's primitive value is compared via [`PrimitiveLiteral`]'s `PartialOrd`. Any incomparable
/// pair (e.g. a `NaN`, or a non-primitive partition literal — neither of which is a valid partition value)
/// falls back to `Equal`, so the order stays total + deterministic under a stable sort. The first field
/// that differs decides the order.
fn compare_partition_values(left: &Struct, right: &Struct) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    let left_fields = left.fields();
    let right_fields = right.fields();
    let len = left_fields.len().min(right_fields.len());
    for index in 0..len {
        let ordering = compare_partition_field(&left_fields[index], &right_fields[index]);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    left_fields.len().cmp(&right_fields.len())
}

/// Compares one optional partition field value; `None` (null) sorts before any value.
fn compare_partition_field(left: &Option<Literal>, right: &Option<Literal>) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(Literal::Primitive(left)), Some(Literal::Primitive(right))) => {
            compare_primitive(left, right)
        }
        // Non-primitive partition literals are not valid partition values; keep order stable.
        _ => Ordering::Equal,
    }
}

/// Compares two [`PrimitiveLiteral`]s, falling back to `Equal` for an incomparable pair.
fn compare_primitive(left: &PrimitiveLiteral, right: &PrimitiveLiteral) -> std::cmp::Ordering {
    left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::RecordBatch;
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Int32Type, Int64Type, TimestampMicrosecondType};
    use futures::TryStreamExt;

    use crate::scan::ArrowRecordBatchStream;
    use crate::scan::tests::TableTestFixture;
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, ManifestEntry,
        ManifestListWriter, ManifestStatus, ManifestWriterBuilder, Snapshot, Struct,
    };
    use crate::table::Table;

    /// A known, fixed file size used for the data files in the fixtures.
    const FILE_SIZE: u64 = 1024;

    /// Concatenates a partitions-table scan into a single batch.
    async fn scan_single_batch(stream: ArrowRecordBatchStream) -> RecordBatch {
        let batches: Vec<_> = stream.try_collect().await.unwrap();
        arrow_select::concat::concat_batches(&batches[0].schema(), &batches).unwrap()
    }

    /// Returns the `partition.x` (long) value of each row, paired with the row index, so a test can find a
    /// specific partition row regardless of the (sorted) row order.
    fn partition_x_values(batch: &RecordBatch) -> Vec<i64> {
        let partition = batch.column_by_name("partition").unwrap().as_struct();
        let x = partition.column(0).as_primitive::<Int64Type>();
        (0..x.len()).map(|index| x.value(index)).collect()
    }

    /// Builds a manifest output file under the fixture's metadata dir.
    fn manifest_output(fixture: &TableTestFixture) -> crate::io::OutputFile {
        fixture
            .table
            .file_io()
            .new_output(format!(
                "{}/metadata/manifest_{}.avro",
                fixture.table_location,
                uuid::Uuid::new_v4()
            ))
            .unwrap()
    }

    /// Stitches the given manifests into the current snapshot's manifest list.
    async fn write_manifest_list(
        fixture: &TableTestFixture,
        manifests: Vec<crate::spec::ManifestFile>,
    ) {
        let metadata = fixture.table.metadata();
        let current_snapshot = metadata.current_snapshot().unwrap();
        let mut writer = ManifestListWriter::v2(
            fixture
                .table
                .file_io()
                .new_output(current_snapshot.manifest_list())
                .unwrap(),
            current_snapshot.snapshot_id(),
            current_snapshot.parent_snapshot_id(),
            current_snapshot.sequence_number(),
        );
        writer.add_manifests(manifests.into_iter()).unwrap();
        writer.close().await.unwrap();
    }

    /// Builds an `Added` data-file entry in partition `x` with the given record count, committed by the
    /// current snapshot (so it inherits the current snapshot's id/seq at read time).
    fn added_data_entry(
        fixture: &TableTestFixture,
        x: i64,
        suffix: &str,
        records: u64,
    ) -> ManifestEntry {
        ManifestEntry::builder()
            .status(ManifestStatus::Added)
            .data_file(
                DataFileBuilder::default()
                    .partition_spec_id(0)
                    .content(DataContentType::Data)
                    .file_path(format!("{}/{suffix}", &fixture.table_location))
                    .file_format(DataFileFormat::Parquet)
                    .file_size_in_bytes(FILE_SIZE)
                    .record_count(records)
                    .partition(Struct::from_iter([Some(Literal::long(x))]))
                    .build()
                    .unwrap(),
            )
            .build()
    }

    /// Writes a single DATA manifest holding the given entries (committed by the current snapshot).
    async fn write_data_manifest(
        fixture: &TableTestFixture,
        entries: Vec<ManifestEntry>,
    ) -> crate::spec::ManifestFile {
        let metadata = fixture.table.metadata();
        let current_snapshot = metadata.current_snapshot().unwrap();
        let current_schema = current_snapshot.schema(metadata).unwrap();
        let current_partition_spec = metadata.default_partition_spec();
        let mut writer = ManifestWriterBuilder::new(
            manifest_output(fixture),
            Some(current_snapshot.snapshot_id()),
            None,
            current_schema.clone(),
            current_partition_spec.as_ref().clone(),
        )
        .build_v2_data();
        for entry in entries {
            writer.add_entry(entry).unwrap();
        }
        writer.write_manifest_file().await.unwrap()
    }

    #[tokio::test]
    async fn test_partitions_table_same_partition_files_are_summed_into_one_row() {
        // RISK: aggregation key — two DATA files in the SAME partition must collapse into ONE row with
        // summed record_count, file_count == 2, summed total size. A per-file (un-grouped) emission would
        // produce two rows and the wrong counts.
        let fixture = TableTestFixture::new();
        let entries = vec![
            added_data_entry(&fixture, 100, "a.parquet", 3),
            added_data_entry(&fixture, 100, "b.parquet", 5),
        ];
        let manifest = write_data_manifest(&fixture, entries).await;
        write_manifest_list(&fixture, vec![manifest]).await;

        let batch =
            scan_single_batch(fixture.table.inspect().partitions().scan().await.unwrap()).await;

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(partition_x_values(&batch), vec![100]);
        let record_count = batch
            .column_by_name("record_count")
            .unwrap()
            .as_primitive::<Int64Type>();
        let file_count = batch
            .column_by_name("file_count")
            .unwrap()
            .as_primitive::<Int32Type>();
        let total_size = batch
            .column_by_name("total_data_file_size_in_bytes")
            .unwrap()
            .as_primitive::<Int64Type>();
        assert_eq!(record_count.value(0), 8);
        assert_eq!(file_count.value(0), 2);
        assert_eq!(total_size.value(0), 2 * FILE_SIZE as i64);
    }

    #[tokio::test]
    async fn test_partitions_table_multiple_partitions_one_row_each_sorted() {
        // RISK: row-per-partition + deterministic order — distinct partitions must each get one row, and
        // the rows must be sorted by partition value (300/100/200 in → 100/200/300 out).
        let fixture = TableTestFixture::new();
        let entries = vec![
            added_data_entry(&fixture, 300, "c.parquet", 1),
            added_data_entry(&fixture, 100, "a.parquet", 1),
            added_data_entry(&fixture, 200, "b.parquet", 1),
        ];
        let manifest = write_data_manifest(&fixture, entries).await;
        write_manifest_list(&fixture, vec![manifest]).await;

        let batch =
            scan_single_batch(fixture.table.inspect().partitions().scan().await.unwrap()).await;

        assert_eq!(batch.num_rows(), 3);
        assert_eq!(partition_x_values(&batch), vec![100, 200, 300]);
        let file_count = batch
            .column_by_name("file_count")
            .unwrap()
            .as_primitive::<Int32Type>();
        for index in 0..file_count.len() {
            assert_eq!(file_count.value(index), 1);
        }
    }

    #[tokio::test]
    async fn test_partitions_table_delete_files_counted_separately_from_data() {
        // RISK: content-type switch — a position-delete and an equality-delete file in the same partition
        // as a data file must increment the pos/eq delete columns ONLY, leaving the DATA columns for that
        // partition untouched. A misrouted content branch (counting a delete as data) is the headline bug.
        let fixture = TableTestFixture::new();

        let data = added_data_entry(&fixture, 100, "a.parquet", 4);
        let pos_delete = ManifestEntry::builder()
            .status(ManifestStatus::Added)
            .data_file(
                DataFileBuilder::default()
                    .partition_spec_id(0)
                    .content(DataContentType::PositionDeletes)
                    .file_path(format!("{}/pos-delete.parquet", &fixture.table_location))
                    .file_format(DataFileFormat::Parquet)
                    .file_size_in_bytes(FILE_SIZE)
                    .record_count(2)
                    .partition(Struct::from_iter([Some(Literal::long(100))]))
                    .build()
                    .unwrap(),
            )
            .build();
        let eq_delete = ManifestEntry::builder()
            .status(ManifestStatus::Added)
            .data_file(
                DataFileBuilder::default()
                    .partition_spec_id(0)
                    .content(DataContentType::EqualityDeletes)
                    .file_path(format!("{}/eq-delete.parquet", &fixture.table_location))
                    .file_format(DataFileFormat::Parquet)
                    .file_size_in_bytes(FILE_SIZE)
                    .record_count(7)
                    .partition(Struct::from_iter([Some(Literal::long(100))]))
                    .build()
                    .unwrap(),
            )
            .build();

        // Data in a DATA manifest, the two delete files in a DELETE manifest.
        let data_manifest = write_data_manifest(&fixture, vec![data]).await;
        let metadata = fixture.table.metadata();
        let current_snapshot = metadata.current_snapshot().unwrap();
        let current_schema = current_snapshot.schema(metadata).unwrap();
        let current_partition_spec = metadata.default_partition_spec();
        let mut delete_writer = ManifestWriterBuilder::new(
            manifest_output(&fixture),
            Some(current_snapshot.snapshot_id()),
            None,
            current_schema.clone(),
            current_partition_spec.as_ref().clone(),
        )
        .build_v2_deletes();
        delete_writer.add_entry(pos_delete).unwrap();
        delete_writer.add_entry(eq_delete).unwrap();
        let delete_manifest = delete_writer.write_manifest_file().await.unwrap();
        write_manifest_list(&fixture, vec![data_manifest, delete_manifest]).await;

        let batch =
            scan_single_batch(fixture.table.inspect().partitions().scan().await.unwrap()).await;

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(partition_x_values(&batch), vec![100]);
        let record_count = batch
            .column_by_name("record_count")
            .unwrap()
            .as_primitive::<Int64Type>();
        let file_count = batch
            .column_by_name("file_count")
            .unwrap()
            .as_primitive::<Int32Type>();
        let pos_record = batch
            .column_by_name("position_delete_record_count")
            .unwrap()
            .as_primitive::<Int64Type>();
        let pos_file = batch
            .column_by_name("position_delete_file_count")
            .unwrap()
            .as_primitive::<Int32Type>();
        let eq_record = batch
            .column_by_name("equality_delete_record_count")
            .unwrap()
            .as_primitive::<Int64Type>();
        let eq_file = batch
            .column_by_name("equality_delete_file_count")
            .unwrap()
            .as_primitive::<Int32Type>();
        let total_size = batch
            .column_by_name("total_data_file_size_in_bytes")
            .unwrap()
            .as_primitive::<Int64Type>();
        // DATA columns reflect ONLY the data file.
        assert_eq!(record_count.value(0), 4);
        assert_eq!(file_count.value(0), 1);
        // `total_data_file_size_in_bytes` accumulates DATA-file sizes ONLY (Java sums fileSize only in the
        // DATA branch). Both delete files also have size FILE_SIZE; if either leaked in, this would be
        // 2*FILE_SIZE or 3*FILE_SIZE. Pinning it to exactly FILE_SIZE catches a delete-size-accumulation bug.
        assert_eq!(total_size.value(0), FILE_SIZE as i64);
        // Delete columns reflect ONLY the delete files.
        assert_eq!(pos_record.value(0), 2);
        assert_eq!(pos_file.value(0), 1);
        assert_eq!(eq_record.value(0), 7);
        assert_eq!(eq_file.value(0), 1);
    }

    #[tokio::test]
    async fn test_partitions_table_last_updated_reflects_most_recent_commit_file() {
        // RISK: the `>` comparison in `Partition.update` — last_updated_at/snapshot_id/spec_id must come
        // from the file committed by the MOST-RECENT snapshot (max commit time), NOT the oldest. The
        // fixture's PARENT snapshot (3051729675574597004 @ ts 1515100955770) is older than the CURRENT
        // snapshot (3055729675574597004 @ ts 1555100955770). Both files are in partition 100: one is an
        // EXISTING entry stamped with the PARENT's id (written via `add_existing_entry`, which preserves
        // the snapshot id) so the parent genuinely participates; the other is ADDED → committed by the
        // current snapshot. The NEWER (max-time) snapshot must win. This pins the `<`-vs-`>` direction
        // (the data-correctness bug): flipping `>` to `<` makes the OLDEST snapshot win → the assertion
        // fails. The test asserts BOTH the newer time and the newer id, and the OLDER snapshot's value is
        // explicitly NOT the answer — so a min-wins regression cannot pass.
        let fixture = TableTestFixture::new();
        let metadata = fixture.table.metadata().clone();
        let current_snapshot = metadata.current_snapshot().unwrap();
        let parent_snapshot = current_snapshot.parent_snapshot(&metadata).unwrap();
        assert!(
            parent_snapshot.timestamp_ms() < current_snapshot.timestamp_ms(),
            "fixture invariant: the parent snapshot must commit before the current one"
        );

        // Older file: committed by the PARENT snapshot. Written via `add_existing_entry`, which PRESERVES
        // the entry's snapshot id (unlike `add_entry`, which restamps it to the manifest's snapshot).
        let older = ManifestEntry::builder()
            .status(ManifestStatus::Existing)
            .snapshot_id(parent_snapshot.snapshot_id())
            .sequence_number(parent_snapshot.sequence_number())
            .file_sequence_number(parent_snapshot.sequence_number())
            .data_file(
                DataFileBuilder::default()
                    .partition_spec_id(0)
                    .content(DataContentType::Data)
                    .file_path(format!("{}/older.parquet", &fixture.table_location))
                    .file_format(DataFileFormat::Parquet)
                    .file_size_in_bytes(FILE_SIZE)
                    .record_count(1)
                    .partition(Struct::from_iter([Some(Literal::long(100))]))
                    .build()
                    .unwrap(),
            )
            .build();
        // Newer file: ADDED → committed by the CURRENT snapshot (inherits the current id at read time).
        let newer = added_data_entry(&fixture, 100, "newer.parquet", 1);

        let current_schema = current_snapshot.schema(&metadata).unwrap();
        let current_partition_spec = metadata.default_partition_spec();
        let mut writer = ManifestWriterBuilder::new(
            manifest_output(&fixture),
            Some(current_snapshot.snapshot_id()),
            None,
            current_schema.clone(),
            current_partition_spec.as_ref().clone(),
        )
        .build_v2_data();
        // `add_existing_entry` keeps the parent's snapshot id; `add_entry` stamps the current id.
        writer.add_existing_entry(older).unwrap();
        writer.add_entry(newer).unwrap();
        let manifest = writer.write_manifest_file().await.unwrap();
        write_manifest_list(&fixture, vec![manifest]).await;

        let batch =
            scan_single_batch(fixture.table.inspect().partitions().scan().await.unwrap()).await;

        assert_eq!(batch.num_rows(), 1);
        let last_updated_at = batch
            .column_by_name("last_updated_at")
            .unwrap()
            .as_primitive::<TimestampMicrosecondType>();
        let last_updated_snapshot_id = batch
            .column_by_name("last_updated_snapshot_id")
            .unwrap()
            .as_primitive::<Int64Type>();
        // The CURRENT (newer, max-time) snapshot wins.
        assert_eq!(
            last_updated_at.value(0),
            current_snapshot.timestamp_ms() * 1000
        );
        assert_eq!(
            last_updated_snapshot_id.value(0),
            current_snapshot.snapshot_id()
        );
        // And it is NOT the older snapshot (a min-wins `<` regression would report this instead).
        assert_ne!(
            last_updated_snapshot_id.value(0),
            parent_snapshot.snapshot_id()
        );
    }

    #[tokio::test]
    async fn test_partitions_table_last_updated_exact_commit_time_tie_keeps_first_seen_file() {
        // RISK: the STRICT `>` tie-break in `Partition.update`. Java updates last_updated_*/spec_id only
        // when `commitMicros > lastUpdatedAt` (strict), so on an EXACT commit-time tie the FIRST-seen file
        // keeps ownership and a later equal-time file does NOT overwrite it. A `>=` regression would let the
        // LAST equal-time file win instead — silently misattributing the partition to the wrong snapshot.
        //
        // Construction: the parent snapshot's commit time is rewritten to EQUAL the current snapshot's, so
        // the two snapshots tie. Both files live in partition 100; the PARENT's file is iterated FIRST
        // (added first to the single manifest), the CURRENT's file second. With strict `>`, the first-seen
        // (parent) wins; `>=` would report the current snapshot. This is the only test that distinguishes
        // `>` from `>=`.
        let mut fixture = TableTestFixture::new();
        let mut metadata = fixture.table.metadata().clone();
        let current_snapshot = metadata.current_snapshot().unwrap().clone();
        let parent_snapshot = current_snapshot.parent_snapshot(&metadata).unwrap();
        let tied_timestamp_ms = current_snapshot.timestamp_ms();

        // Rebuild the parent snapshot with the SAME timestamp as the current one (an exact tie), preserving
        // every other field, and splice it back into the metadata snapshot map.
        let tied_parent = Snapshot::builder()
            .with_snapshot_id(parent_snapshot.snapshot_id())
            .with_parent_snapshot_id(parent_snapshot.parent_snapshot_id())
            .with_sequence_number(parent_snapshot.sequence_number())
            .with_timestamp_ms(tied_timestamp_ms)
            .with_manifest_list(parent_snapshot.manifest_list().to_string())
            .with_summary(parent_snapshot.summary().clone())
            .build();
        let tied_parent_id = tied_parent.snapshot_id();
        // `snapshots` is a `pub(crate)` field; this in-crate test splices the tied parent back in directly
        // (the same field-level access `table_metadata_builder` uses) rather than adding a spec accessor.
        metadata
            .snapshots
            .insert(tied_parent_id, Arc::new(tied_parent));
        let mutated_metadata = Arc::new(metadata);

        // Rebuild the fixture's table over the mutated metadata (same location / file_io / identifier).
        fixture.table = Table::builder()
            .metadata(mutated_metadata.clone())
            .identifier(fixture.table.identifier().clone())
            .file_io(fixture.table.file_io().clone())
            .metadata_location(
                fixture
                    .table
                    .metadata_location()
                    .expect("fixture has a metadata location")
                    .to_string(),
            )
            .build()
            .unwrap();

        // Parent's file FIRST (committed by the now-tied parent snapshot; `add_existing_entry` preserves its
        // snapshot id). Current's file SECOND (`add_entry` stamps the current snapshot id).
        let parent_file = ManifestEntry::builder()
            .status(ManifestStatus::Existing)
            .snapshot_id(tied_parent_id)
            .sequence_number(parent_snapshot.sequence_number())
            .file_sequence_number(parent_snapshot.sequence_number())
            .data_file(
                DataFileBuilder::default()
                    .partition_spec_id(0)
                    .content(DataContentType::Data)
                    .file_path(format!("{}/tie-parent.parquet", &fixture.table_location))
                    .file_format(DataFileFormat::Parquet)
                    .file_size_in_bytes(FILE_SIZE)
                    .record_count(1)
                    .partition(Struct::from_iter([Some(Literal::long(100))]))
                    .build()
                    .unwrap(),
            )
            .build();
        let current_file = added_data_entry(&fixture, 100, "tie-current.parquet", 1);

        let current_schema = current_snapshot.schema(&mutated_metadata).unwrap();
        let current_partition_spec = mutated_metadata.default_partition_spec();
        let mut writer = ManifestWriterBuilder::new(
            manifest_output(&fixture),
            Some(current_snapshot.snapshot_id()),
            None,
            current_schema.clone(),
            current_partition_spec.as_ref().clone(),
        )
        .build_v2_data();
        writer.add_existing_entry(parent_file).unwrap();
        writer.add_entry(current_file).unwrap();
        let manifest = writer.write_manifest_file().await.unwrap();
        write_manifest_list(&fixture, vec![manifest]).await;

        let batch =
            scan_single_batch(fixture.table.inspect().partitions().scan().await.unwrap()).await;

        assert_eq!(batch.num_rows(), 1);
        let last_updated_at = batch
            .column_by_name("last_updated_at")
            .unwrap()
            .as_primitive::<TimestampMicrosecondType>();
        let last_updated_snapshot_id = batch
            .column_by_name("last_updated_snapshot_id")
            .unwrap()
            .as_primitive::<Int64Type>();
        // Both snapshots tie on commit time; strict `>` keeps the FIRST-seen (parent) file's ownership.
        assert_eq!(last_updated_at.value(0), tied_timestamp_ms * 1000);
        assert_eq!(
            last_updated_snapshot_id.value(0),
            tied_parent_id,
            "on an exact commit-time tie the FIRST-seen file must win (strict `>`); a `>=` regression would \
             report the current snapshot instead"
        );
        assert_ne!(
            last_updated_snapshot_id.value(0),
            current_snapshot.snapshot_id()
        );
    }

    #[tokio::test]
    async fn test_partitions_table_partition_with_only_delete_files_present_with_zero_data() {
        // RISK: a partition that has ONLY delete files (no data file) must still emit a row, with the DATA
        // columns at 0 and the delete columns populated. Dropping such a row (e.g. only emitting on a data
        // file) would hide partitions that are pure deletes.
        let fixture = TableTestFixture::new();
        let metadata = fixture.table.metadata();
        let current_snapshot = metadata.current_snapshot().unwrap();
        let current_schema = current_snapshot.schema(metadata).unwrap();
        let current_partition_spec = metadata.default_partition_spec();

        let pos_delete = ManifestEntry::builder()
            .status(ManifestStatus::Added)
            .data_file(
                DataFileBuilder::default()
                    .partition_spec_id(0)
                    .content(DataContentType::PositionDeletes)
                    .file_path(format!("{}/only-delete.parquet", &fixture.table_location))
                    .file_format(DataFileFormat::Parquet)
                    .file_size_in_bytes(FILE_SIZE)
                    .record_count(9)
                    .partition(Struct::from_iter([Some(Literal::long(100))]))
                    .build()
                    .unwrap(),
            )
            .build();
        let mut delete_writer = ManifestWriterBuilder::new(
            manifest_output(&fixture),
            Some(current_snapshot.snapshot_id()),
            None,
            current_schema.clone(),
            current_partition_spec.as_ref().clone(),
        )
        .build_v2_deletes();
        delete_writer.add_entry(pos_delete).unwrap();
        let delete_manifest = delete_writer.write_manifest_file().await.unwrap();
        write_manifest_list(&fixture, vec![delete_manifest]).await;

        let batch =
            scan_single_batch(fixture.table.inspect().partitions().scan().await.unwrap()).await;

        // The partition row is present even though it has no data file.
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(partition_x_values(&batch), vec![100]);
        let record_count = batch
            .column_by_name("record_count")
            .unwrap()
            .as_primitive::<Int64Type>();
        let file_count = batch
            .column_by_name("file_count")
            .unwrap()
            .as_primitive::<Int32Type>();
        let total_size = batch
            .column_by_name("total_data_file_size_in_bytes")
            .unwrap()
            .as_primitive::<Int64Type>();
        let pos_record = batch
            .column_by_name("position_delete_record_count")
            .unwrap()
            .as_primitive::<Int64Type>();
        let pos_file = batch
            .column_by_name("position_delete_file_count")
            .unwrap()
            .as_primitive::<Int32Type>();
        // DATA columns are zero; the delete columns carry the only file.
        assert_eq!(record_count.value(0), 0);
        assert_eq!(file_count.value(0), 0);
        assert_eq!(total_size.value(0), 0);
        assert_eq!(pos_record.value(0), 9);
        assert_eq!(pos_file.value(0), 1);
    }

    #[tokio::test]
    async fn test_partitions_table_excludes_deleted_tombstones() {
        // RISK: the `is_alive()` live-entry filter — a Deleted tombstone (status==2) must NOT contribute
        // to the rollup. The partition has one Added data file and one Deleted data file (same partition);
        // only the Added file may be counted. Removing the `is_alive()` filter would count the tombstone
        // (file_count==2, record_count==3) — this pins it.
        let fixture = TableTestFixture::new();
        let metadata = fixture.table.metadata().clone();
        let current_snapshot = metadata.current_snapshot().unwrap();
        let parent_snapshot = current_snapshot.parent_snapshot(&metadata).unwrap();
        let current_schema = current_snapshot.schema(&metadata).unwrap();
        let current_partition_spec = metadata.default_partition_spec();

        let mut writer = ManifestWriterBuilder::new(
            manifest_output(&fixture),
            Some(current_snapshot.snapshot_id()),
            None,
            current_schema.clone(),
            current_partition_spec.as_ref().clone(),
        )
        .build_v2_data();
        // Added (live) data file: record_count 1.
        writer
            .add_entry(added_data_entry(&fixture, 100, "live.parquet", 1))
            .unwrap();
        // Deleted tombstone in the SAME partition: record_count 2 (must be ignored).
        writer
            .add_delete_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Deleted)
                    .snapshot_id(parent_snapshot.snapshot_id())
                    .sequence_number(parent_snapshot.sequence_number())
                    .file_sequence_number(parent_snapshot.sequence_number())
                    .data_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::Data)
                            .file_path(format!("{}/tombstone.parquet", &fixture.table_location))
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(FILE_SIZE)
                            .record_count(2)
                            .partition(Struct::from_iter([Some(Literal::long(100))]))
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .unwrap();
        let manifest = writer.write_manifest_file().await.unwrap();
        write_manifest_list(&fixture, vec![manifest]).await;

        let batch =
            scan_single_batch(fixture.table.inspect().partitions().scan().await.unwrap()).await;

        assert_eq!(batch.num_rows(), 1);
        let record_count = batch
            .column_by_name("record_count")
            .unwrap()
            .as_primitive::<Int64Type>();
        let file_count = batch
            .column_by_name("file_count")
            .unwrap()
            .as_primitive::<Int32Type>();
        // Only the live file is counted; the tombstone is excluded.
        assert_eq!(record_count.value(0), 1);
        assert_eq!(file_count.value(0), 1);
    }

    #[tokio::test]
    async fn test_partitions_table_empty_table_yields_zero_rows() {
        // RISK: panic / spurious row on an empty table — no current snapshot must yield zero rows.
        let fixture = TableTestFixture::new_empty();
        let batches: Vec<_> = fixture
            .table
            .inspect()
            .partitions()
            .scan()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0);
    }

    #[tokio::test]
    async fn test_partitions_table_unpartitioned_keeps_empty_partition_struct_known_divergence() {
        // RISK / KNOWN DIVERGENCE (decision 1): Java `PartitionsTable.schema()` DROPS the `partition`
        // column for an UNPARTITIONED table. The Rust port KEEPS an empty-struct `partition` column
        // (matching the `files`-family precedent) so the module has ONE consistent unpartitioned-column
        // behavior. There must be exactly ONE root row, and the partition column must be a 0-field struct.
        // When the Java drop-empty-partition rule is implemented module-wide, this flips to assert-absent.
        let fixture = TableTestFixture::new_unpartitioned();
        let metadata = fixture.table.metadata();
        let current_snapshot = metadata.current_snapshot().unwrap();
        let current_schema = current_snapshot.schema(metadata).unwrap();
        let current_partition_spec = metadata.default_partition_spec();

        let mut writer = ManifestWriterBuilder::new(
            manifest_output(&fixture),
            Some(current_snapshot.snapshot_id()),
            None,
            current_schema.clone(),
            current_partition_spec.as_ref().clone(),
        )
        .build_v2_data();
        for suffix in ["u1.parquet", "u2.parquet"] {
            writer
                .add_entry(
                    ManifestEntry::builder()
                        .status(ManifestStatus::Added)
                        .data_file(
                            DataFileBuilder::default()
                                .partition_spec_id(0)
                                .content(DataContentType::Data)
                                .file_path(format!("{}/{suffix}", &fixture.table_location))
                                .file_format(DataFileFormat::Parquet)
                                .file_size_in_bytes(FILE_SIZE)
                                .record_count(1)
                                .partition(Struct::empty())
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .unwrap();
        }
        let manifest = writer.write_manifest_file().await.unwrap();
        write_manifest_list(&fixture, vec![manifest]).await;

        let batch =
            scan_single_batch(fixture.table.inspect().partitions().scan().await.unwrap()).await;

        // Exactly one root row aggregating both files.
        assert_eq!(batch.num_rows(), 1);
        let record_count = batch
            .column_by_name("record_count")
            .unwrap()
            .as_primitive::<Int64Type>();
        let file_count = batch
            .column_by_name("file_count")
            .unwrap()
            .as_primitive::<Int32Type>();
        assert_eq!(record_count.value(0), 2);
        assert_eq!(file_count.value(0), 2);
        // CURRENT (divergent) behavior: the partition column is present as an empty struct.
        let partition = batch.column_by_name("partition").unwrap().as_struct();
        assert_eq!(
            partition.num_columns(),
            0,
            "unpartitioned partitions table currently keeps an empty-struct partition column \
             (Java drops it) — see the GAP_MATRIX deferral"
        );
    }

    #[tokio::test]
    async fn test_partitions_table_arrow_schema_columns_types_and_field_ids() {
        // RISK: wrong column set / types / field ids — the interop contract. Assert the 11 columns in
        // Java order, the leading types (partition Struct, the counts, last_updated_at = Timestamp(µs,UTC)
        // OPTIONAL), and the EXACT non-sequential Java field ids.
        use arrow_schema::DataType;
        use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

        let fixture = TableTestFixture::new();
        let schema = fixture.table.inspect().partitions().schema();
        let arrow = crate::arrow::schema_to_arrow_schema(&schema).unwrap();

        let names: Vec<&str> = arrow.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec![
            "partition",
            "spec_id",
            "record_count",
            "file_count",
            "total_data_file_size_in_bytes",
            "position_delete_record_count",
            "position_delete_file_count",
            "equality_delete_record_count",
            "equality_delete_file_count",
            "last_updated_at",
            "last_updated_snapshot_id",
        ]);

        // Field ids (the non-sequential Java ids).
        let field_id = |name: &str| -> &str {
            arrow
                .field_with_name(name)
                .unwrap()
                .metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .unwrap()
                .clone()
                .leak()
        };
        assert_eq!(field_id("partition"), "1");
        assert_eq!(field_id("spec_id"), "4");
        assert_eq!(field_id("record_count"), "2");
        assert_eq!(field_id("file_count"), "3");
        assert_eq!(field_id("total_data_file_size_in_bytes"), "11");
        assert_eq!(field_id("position_delete_record_count"), "5");
        assert_eq!(field_id("position_delete_file_count"), "6");
        assert_eq!(field_id("equality_delete_record_count"), "7");
        assert_eq!(field_id("equality_delete_file_count"), "8");
        assert_eq!(field_id("last_updated_at"), "9");
        assert_eq!(field_id("last_updated_snapshot_id"), "10");

        // Types + optionality.
        assert!(matches!(
            arrow.field_with_name("partition").unwrap().data_type(),
            DataType::Struct(_)
        ));
        assert_eq!(
            arrow.field_with_name("spec_id").unwrap().data_type(),
            &DataType::Int32
        );
        assert_eq!(
            arrow.field_with_name("record_count").unwrap().data_type(),
            &DataType::Int64
        );
        assert_eq!(
            arrow.field_with_name("file_count").unwrap().data_type(),
            &DataType::Int32
        );
        let last_updated_at = arrow.field_with_name("last_updated_at").unwrap();
        assert_eq!(
            last_updated_at.data_type(),
            &DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("+00:00".into()))
        );
        assert!(last_updated_at.is_nullable());
        assert!(
            arrow
                .field_with_name("last_updated_snapshot_id")
                .unwrap()
                .is_nullable()
        );
        // The count/file columns are required.
        assert!(!arrow.field_with_name("record_count").unwrap().is_nullable());
        assert!(!arrow.field_with_name("partition").unwrap().is_nullable());
    }

    #[tokio::test]
    async fn test_partitions_table_reports_file_spec_id() {
        // RISK (decision 2 — single-spec correctness): the reported spec_id must be the FILE's own
        // partition_spec_id (here 0), not a hard-coded value — so under partition evolution each row at
        // least carries its true spec id even though cross-spec partition-type UNIFICATION is deferred.
        let fixture = TableTestFixture::new();
        let entries = vec![added_data_entry(&fixture, 100, "a.parquet", 1)];
        let manifest = write_data_manifest(&fixture, entries).await;
        write_manifest_list(&fixture, vec![manifest]).await;

        let batch =
            scan_single_batch(fixture.table.inspect().partitions().scan().await.unwrap()).await;
        let spec_id = batch
            .column_by_name("spec_id")
            .unwrap()
            .as_primitive::<Int32Type>();
        assert_eq!(spec_id.value(0), 0);
    }

    #[test]
    fn test_compare_partition_values_multi_field_and_null_first_ordering() {
        // RISK: the hand-rolled `compare_partition_values` comparator. The aggregating tests only exercise
        // SINGLE long partitions, so this pins the comparator's harder cases directly: (1) a leading-field
        // difference decides regardless of later fields; (2) when the first field ties, the SECOND field
        // breaks it; (3) a null first field sorts BEFORE any value (null-first); (4) a non-long primitive
        // (String) compares by value, not by enum-variant order. A miswired field loop, a flipped null
        // ordering, or a wrong string comparison would change the sorted output of the partitions table.
        use std::cmp::Ordering;

        use super::compare_partition_values;

        let two_long =
            |a: i64, b: i64| Struct::from_iter([Some(Literal::long(a)), Some(Literal::long(b))]);

        // (1) Leading field decides even though the second field would order the other way.
        assert_eq!(
            compare_partition_values(&two_long(1, 99), &two_long(2, 0)),
            Ordering::Less
        );
        // (2) Tie on the first field → the second field breaks the tie.
        assert_eq!(
            compare_partition_values(&two_long(5, 1), &two_long(5, 2)),
            Ordering::Less
        );
        assert_eq!(
            compare_partition_values(&two_long(5, 2), &two_long(5, 2)),
            Ordering::Equal
        );
        // (3) Null first field sorts before a present value (null-first).
        let null_then_long = Struct::from_iter([None, Some(Literal::long(0))]);
        assert_eq!(
            compare_partition_values(&null_then_long, &two_long(0, 0)),
            Ordering::Less
        );
        assert_eq!(
            compare_partition_values(&two_long(0, 0), &null_then_long),
            Ordering::Greater
        );
        // (4) A non-long primitive (String) compares by value (`"apple" < "banana"`).
        let one_string =
            |value: &str| Struct::from_iter([Some(Literal::string(value.to_string()))]);
        assert_eq!(
            compare_partition_values(&one_string("apple"), &one_string("banana")),
            Ordering::Less
        );
        assert_eq!(
            compare_partition_values(&one_string("banana"), &one_string("banana")),
            Ordering::Equal
        );
    }
}
