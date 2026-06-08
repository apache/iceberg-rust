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

//! The `all_manifests` metadata table (Java `AllManifestsTable`).
//!
//! `all_manifests` exposes every manifest referenced by ANY snapshot currently tracked by the table,
//! one row per (manifest × referencing snapshot), tagged with a `reference_snapshot_id` column. Unlike
//! the file/entry `all_*` tables (which deduplicate manifests across snapshots), `all_manifests` is NOT
//! deduplicated — a manifest shared by two snapshots produces TWO rows (Java javadoc: "may return
//! duplicate rows"). It iterates `metadata.snapshots()` directly, loading each snapshot's manifest list,
//! and never reuses the 5a `manifest_source` dedup helper (which drops the snapshot identity).

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::builder::{LargeBinaryBuilder, PrimitiveBuilder, StringBuilder};
use arrow_array::types::{Int32Type, Int64Type};
use futures::{StreamExt, stream};

use super::partition_summary::{append_partition_summaries, partition_summary_builder};
use crate::Result;
use crate::arrow::schema_to_arrow_schema;
use crate::scan::ArrowRecordBatchStream;
use crate::spec::{
    ListType, ManifestContentType, NestedField, PrimitiveType, Schema, StructType, Type,
};
use crate::table::Table;

/// All manifests table.
///
/// One row per (manifest × referencing snapshot) across ALL snapshots, NOT deduplicated.
pub struct AllManifestsTable<'a> {
    table: &'a Table,
}

impl<'a> AllManifestsTable<'a> {
    /// Create a new `all_manifests` table instance.
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Returns the iceberg schema of the `all_manifests` table.
    ///
    /// The 14 columns are in Java `AllManifestsTable.MANIFEST_FILE_SCHEMA` order with the Java field
    /// ids AND nullability, with ONE deliberate divergence. The three `*_delete_files_count` columns
    /// are REQUIRED (as in Java), and the table adds `reference_snapshot_id`/18 (required) +
    /// `key_metadata`/19 (optional, binary).
    ///
    /// DIVERGENCE — the partition-summary `contains_nan`/11 field is OPTIONAL here, where Java's
    /// `AllManifestsTable.MANIFEST_FILE_SCHEMA` nominally marks it `required`. Java's
    /// `PartitionFieldSummary.containsNaN()` returns a nullable `Boolean` that is `null` when the
    /// manifest carries no NaN information (V1 manifests, or V2 manifests written without it), and
    /// `manifestFileToRow` puts that `null` straight into the row — Java's loosely-typed
    /// `StaticDataTask.Row` never enforces the `required` flag. Arrow DOES enforce non-nullability at
    /// array-build time, so copying the `required` flag verbatim panics on the common `contains_nan ==
    /// None` case. Marking it optional faithfully carries Java's emitted value (null when unset) and is
    /// consistent with the regular `manifests` table, whose Java schema marks the same field optional.
    pub fn schema(&self) -> Schema {
        let fields = vec![
            NestedField::required(14, "content", Type::Primitive(PrimitiveType::Int)),
            NestedField::required(1, "path", Type::Primitive(PrimitiveType::String)),
            NestedField::required(2, "length", Type::Primitive(PrimitiveType::Long)),
            NestedField::optional(3, "partition_spec_id", Type::Primitive(PrimitiveType::Int)),
            NestedField::optional(4, "added_snapshot_id", Type::Primitive(PrimitiveType::Long)),
            NestedField::optional(
                5,
                "added_data_files_count",
                Type::Primitive(PrimitiveType::Int),
            ),
            NestedField::optional(
                6,
                "existing_data_files_count",
                Type::Primitive(PrimitiveType::Int),
            ),
            NestedField::optional(
                7,
                "deleted_data_files_count",
                Type::Primitive(PrimitiveType::Int),
            ),
            NestedField::required(
                15,
                "added_delete_files_count",
                Type::Primitive(PrimitiveType::Int),
            ),
            NestedField::required(
                16,
                "existing_delete_files_count",
                Type::Primitive(PrimitiveType::Int),
            ),
            NestedField::required(
                17,
                "deleted_delete_files_count",
                Type::Primitive(PrimitiveType::Int),
            ),
            NestedField::optional(
                8,
                "partition_summaries",
                Type::List(ListType {
                    element_field: Arc::new(NestedField::required(
                        9,
                        "item",
                        Type::Struct(StructType::new(vec![
                            Arc::new(NestedField::required(
                                10,
                                "contains_null",
                                Type::Primitive(PrimitiveType::Boolean),
                            )),
                            Arc::new(NestedField::optional(
                                11,
                                "contains_nan",
                                Type::Primitive(PrimitiveType::Boolean),
                            )),
                            Arc::new(NestedField::optional(
                                12,
                                "lower_bound",
                                Type::Primitive(PrimitiveType::String),
                            )),
                            Arc::new(NestedField::optional(
                                13,
                                "upper_bound",
                                Type::Primitive(PrimitiveType::String),
                            )),
                        ])),
                    )),
                }),
            ),
            NestedField::required(
                18,
                "reference_snapshot_id",
                Type::Primitive(PrimitiveType::Long),
            ),
            NestedField::optional(19, "key_metadata", Type::Primitive(PrimitiveType::Binary)),
        ];

        Schema::builder()
            .with_fields(fields.into_iter().map(|f| f.into()))
            .build()
            .expect("all_manifests metadata table schema is statically valid")
    }

    /// Scans the `all_manifests` table.
    ///
    /// Iterates ALL snapshots (`metadata.snapshots()`, NO dedup); per snapshot loads its manifest list
    /// and emits one row per manifest tagged with that snapshot's id as `reference_snapshot_id`. Counts
    /// are CONTENT-GATED per Java `AllManifestsTable.manifestFileToRow`. An empty table (no snapshots)
    /// yields a single empty batch.
    pub async fn scan(&self) -> Result<ArrowRecordBatchStream> {
        let arrow_schema = schema_to_arrow_schema(&self.schema())?;
        let metadata = self.table.metadata_ref();

        let mut content = PrimitiveBuilder::<Int32Type>::new();
        let mut path = StringBuilder::new();
        let mut length = PrimitiveBuilder::<Int64Type>::new();
        let mut partition_spec_id = PrimitiveBuilder::<Int32Type>::new();
        let mut added_snapshot_id = PrimitiveBuilder::<Int64Type>::new();
        let mut added_data_files_count = PrimitiveBuilder::<Int32Type>::new();
        let mut existing_data_files_count = PrimitiveBuilder::<Int32Type>::new();
        let mut deleted_data_files_count = PrimitiveBuilder::<Int32Type>::new();
        let mut added_delete_files_count = PrimitiveBuilder::<Int32Type>::new();
        let mut existing_delete_files_count = PrimitiveBuilder::<Int32Type>::new();
        let mut deleted_delete_files_count = PrimitiveBuilder::<Int32Type>::new();
        let mut partition_summaries = partition_summary_builder(&self.schema())?;
        let mut reference_snapshot_id = PrimitiveBuilder::<Int64Type>::new();
        let mut key_metadata = LargeBinaryBuilder::new();

        // Iterate EVERY snapshot (NO dedup): one row per manifest per referencing snapshot's list.
        for snapshot in metadata.snapshots() {
            let manifest_list = snapshot
                .load_manifest_list(self.table.file_io(), &metadata)
                .await?;
            for manifest in manifest_list.entries() {
                content.append_value(manifest.content as i32);
                path.append_value(manifest.manifest_path.clone());
                length.append_value(manifest.manifest_length);
                partition_spec_id.append_value(manifest.partition_spec_id);
                added_snapshot_id.append_value(manifest.added_snapshot_id);

                // Counts are CONTENT-GATED, mirroring Java `AllManifestsTable.manifestFileToRow`: a DATA
                // manifest carries its added/existing/deleted counts in the *_data_files_count columns
                // (the *_delete_files_count columns are 0), and a DELETE manifest the reverse.
                let added = manifest.added_files_count.unwrap_or(0) as i32;
                let existing = manifest.existing_files_count.unwrap_or(0) as i32;
                let deleted = manifest.deleted_files_count.unwrap_or(0) as i32;
                let is_data = manifest.content == ManifestContentType::Data;
                added_data_files_count.append_value(if is_data { added } else { 0 });
                existing_data_files_count.append_value(if is_data { existing } else { 0 });
                deleted_data_files_count.append_value(if is_data { deleted } else { 0 });
                added_delete_files_count.append_value(if is_data { 0 } else { added });
                existing_delete_files_count.append_value(if is_data { 0 } else { existing });
                deleted_delete_files_count.append_value(if is_data { 0 } else { deleted });

                let spec = metadata
                    .partition_spec_by_id(manifest.partition_spec_id)
                    .ok_or_else(|| {
                        crate::Error::new(
                            crate::ErrorKind::DataInvalid,
                            format!(
                                "partition spec id {} referenced by manifest {} not found",
                                manifest.partition_spec_id, manifest.manifest_path
                            ),
                        )
                    })?;
                let spec_struct = spec.partition_type(metadata.current_schema())?;
                append_partition_summaries(
                    &mut partition_summaries,
                    manifest.partitions.as_deref().unwrap_or(&[]),
                    &spec_struct,
                )?;

                reference_snapshot_id.append_value(snapshot.snapshot_id());
                key_metadata.append_option(manifest.key_metadata.as_deref());
            }
        }

        let batch = RecordBatch::try_new(Arc::new(arrow_schema), vec![
            Arc::new(content.finish()),
            Arc::new(path.finish()),
            Arc::new(length.finish()),
            Arc::new(partition_spec_id.finish()),
            Arc::new(added_snapshot_id.finish()),
            Arc::new(added_data_files_count.finish()),
            Arc::new(existing_data_files_count.finish()),
            Arc::new(deleted_data_files_count.finish()),
            Arc::new(added_delete_files_count.finish()),
            Arc::new(existing_delete_files_count.finish()),
            Arc::new(deleted_delete_files_count.finish()),
            Arc::new(partition_summaries.finish()),
            Arc::new(reference_snapshot_id.finish()),
            Arc::new(key_metadata.finish()),
        ])?;
        Ok(stream::iter(vec![Ok(batch)]).boxed())
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Int32Type, Int64Type};
    use arrow_array::{Array, RecordBatch, StringArray};
    use futures::TryStreamExt;

    use crate::arrow::schema_to_arrow_schema;
    use crate::scan::ArrowRecordBatchStream;
    use crate::scan::tests::TableTestFixture;
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, ManifestEntry, ManifestFile,
        ManifestListWriter, ManifestStatus, ManifestWriterBuilder, Struct,
    };

    const FILE_SIZE: u64 = 1024;
    /// The PARENT snapshot id in `example_table_metadata_v2.json`.
    const PARENT_SNAPSHOT_ID: i64 = 3051729675574597004;
    /// The CURRENT snapshot id in `example_table_metadata_v2.json`.
    const CURRENT_SNAPSHOT_ID: i64 = 3055729675574597004;
    /// Bytes used for the DELETE manifest's `key_metadata` (present-when-set test).
    const DELETE_KEY_METADATA: [u8; 4] = [0xDE, 0xAD, 0xBE, 0xEF];

    /// Builds a MULTI-SNAPSHOT fixture exercising every `all_manifests` risk:
    ///
    /// - A SHARED DATA manifest (`added_snapshot_id` == PARENT, `key_metadata` == None, partition
    ///   values 100/300) is referenced by BOTH the PARENT and the CURRENT snapshots' manifest lists
    ///   (written once, listed twice). It pins NON-dedup (two rows, different `reference_snapshot_id`)
    ///   and the ref-id-vs-added-snapshot-id distinction (its `added_snapshot_id` is PARENT, so the
    ///   current-snapshot row's `reference_snapshot_id` != `added_snapshot_id`).
    /// - A CURRENT-only DELETE manifest (`added_snapshot_id` == CURRENT, `key_metadata` == Some,
    ///   one Added position-delete entry) pins content gating (the data-count columns must be 0, the
    ///   delete-count columns carry its counts) and `key_metadata` present.
    ///
    /// Returns nothing — the fixture's two snapshots are wired with their lists on disk.
    async fn setup_all_manifests_fixture(fixture: &TableTestFixture) {
        let metadata = fixture.table.metadata().clone();
        let current_snapshot = metadata.current_snapshot().unwrap();
        let parent_snapshot = current_snapshot.parent_snapshot(&metadata).unwrap();
        let current_schema = current_snapshot.schema(&metadata).unwrap();
        let current_partition_spec = metadata.default_partition_spec();

        let output = |file_name: &str| {
            fixture
                .table
                .file_io()
                .new_output(format!("{}/metadata/{file_name}", fixture.table_location))
                .unwrap()
        };

        // SHARED DATA manifest, committed by the PARENT snapshot (added_snapshot_id == PARENT), with
        // two Added data files in partitions 100 and 300 (for the partition_summaries assertion). No
        // key_metadata.
        let mut shared_writer = ManifestWriterBuilder::new(
            output("shared_data.avro"),
            Some(PARENT_SNAPSHOT_ID),
            None,
            current_schema.clone(),
            current_partition_spec.as_ref().clone(),
        )
        .build_v2_data();
        shared_writer
            .add_entry(added_data_entry(
                &fixture.table_location,
                "shared-1.parquet",
                100,
            ))
            .unwrap();
        shared_writer
            .add_entry(added_data_entry(
                &fixture.table_location,
                "shared-2.parquet",
                300,
            ))
            .unwrap();
        let mut shared_manifest = shared_writer.write_manifest_file().await.unwrap();
        // The shared manifest is carried forward into the CURRENT list (a later snapshot's list), so it
        // must already carry an assigned sequence number (only the list that ADDED it assigns one).
        shared_manifest.sequence_number = parent_snapshot.sequence_number();
        shared_manifest.min_sequence_number = parent_snapshot.sequence_number();

        // CURRENT-only DELETE manifest (added_snapshot_id == CURRENT) with key_metadata set.
        let mut delete_writer = ManifestWriterBuilder::new(
            output("cur_delete.avro"),
            Some(CURRENT_SNAPSHOT_ID),
            Some(DELETE_KEY_METADATA.to_vec()),
            current_schema.clone(),
            current_partition_spec.as_ref().clone(),
        )
        .build_v2_deletes();
        delete_writer
            .add_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::PositionDeletes)
                            .file_path(format!("{}/delete-1.parquet", &fixture.table_location))
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(FILE_SIZE)
                            .record_count(1)
                            .partition(Struct::from_iter([Some(Literal::long(100))]))
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .unwrap();
        let delete_manifest = delete_writer.write_manifest_file().await.unwrap();

        // PARENT list: just the shared manifest.
        write_manifest_list(fixture, &parent_snapshot, vec![shared_manifest.clone()]).await;
        // CURRENT list: the shared manifest (again — NOT deduped) + the delete manifest.
        write_manifest_list(fixture, current_snapshot, vec![
            shared_manifest,
            delete_manifest,
        ])
        .await;
    }

    /// Builds an Added DATA manifest entry for a single data file at the given partition value.
    fn added_data_entry(table_location: &str, name: &str, partition: i64) -> ManifestEntry {
        ManifestEntry::builder()
            .status(ManifestStatus::Added)
            .data_file(
                DataFileBuilder::default()
                    .partition_spec_id(0)
                    .content(DataContentType::Data)
                    .file_path(format!("{table_location}/{name}"))
                    .file_format(DataFileFormat::Parquet)
                    .file_size_in_bytes(FILE_SIZE)
                    .record_count(1)
                    .partition(Struct::from_iter([Some(Literal::long(partition))]))
                    .build()
                    .unwrap(),
            )
            .build()
    }

    /// Writes a manifest list for `snapshot` referencing the given manifests, at its own location.
    async fn write_manifest_list(
        fixture: &TableTestFixture,
        snapshot: &crate::spec::Snapshot,
        manifests: Vec<ManifestFile>,
    ) {
        let mut writer = ManifestListWriter::v2(
            fixture
                .table
                .file_io()
                .new_output(snapshot.manifest_list())
                .unwrap(),
            snapshot.snapshot_id(),
            snapshot.parent_snapshot_id(),
            snapshot.sequence_number(),
        );
        writer.add_manifests(manifests.into_iter()).unwrap();
        writer.close().await.unwrap();
    }

    /// Collects an `all_manifests` scan into one concatenated batch.
    async fn scan_to_batch(stream: ArrowRecordBatchStream) -> RecordBatch {
        let batches: Vec<_> = stream.try_collect().await.unwrap();
        arrow_select::concat::concat_batches(&batches[0].schema(), &batches).unwrap()
    }

    /// Returns `(path, reference_snapshot_id)` pairs from a batch, sorted, for set assertions.
    fn path_ref_pairs(batch: &RecordBatch) -> Vec<(String, i64)> {
        let paths = batch.column_by_name("path").unwrap().as_string::<i32>();
        let refs = batch
            .column_by_name("reference_snapshot_id")
            .unwrap()
            .as_primitive::<Int64Type>();
        let mut pairs: Vec<(String, i64)> = (0..batch.num_rows())
            .map(|index| (paths.value(index).to_string(), refs.value(index)))
            .collect();
        pairs.sort();
        pairs
    }

    /// Finds the single row index whose `path` ends with `suffix` AND whose `reference_snapshot_id`
    /// equals `reference_snapshot_id`.
    fn row_index(batch: &RecordBatch, suffix: &str, reference_snapshot_id: i64) -> usize {
        let paths = batch.column_by_name("path").unwrap().as_string::<i32>();
        let refs = batch
            .column_by_name("reference_snapshot_id")
            .unwrap()
            .as_primitive::<Int64Type>();
        (0..batch.num_rows())
            .find(|&index| {
                paths.value(index).ends_with(suffix) && refs.value(index) == reference_snapshot_id
            })
            .unwrap_or_else(|| panic!("no row for {suffix} @ ref {reference_snapshot_id}"))
    }

    #[tokio::test]
    async fn test_all_manifests_does_not_dedup_shared_manifest_across_snapshots() {
        // RISK: dedup or current-snapshot-only reads. The SHARED manifest is referenced by BOTH the
        // parent and current snapshots' lists, so it must yield TWO rows with DIFFERENT
        // reference_snapshot_id — never one (dedup would collapse it; reading only the current snapshot
        // would drop the parent row).
        let fixture = TableTestFixture::new();
        setup_all_manifests_fixture(&fixture).await;

        let batch = scan_to_batch(
            fixture
                .table
                .inspect()
                .all_manifests()
                .scan()
                .await
                .unwrap(),
        )
        .await;
        let pairs = path_ref_pairs(&batch);

        // shared appears under BOTH snapshots; delete only under current.
        assert_eq!(
            pairs.len(),
            3,
            "expected 3 rows (shared×2 + delete×1), got {pairs:?}"
        );
        let shared_refs: Vec<i64> = pairs
            .iter()
            .filter(|(path, _)| path.ends_with("shared_data.avro"))
            .map(|(_, reference)| *reference)
            .collect();
        assert_eq!(shared_refs, vec![PARENT_SNAPSHOT_ID, CURRENT_SNAPSHOT_ID]);
    }

    #[tokio::test]
    async fn test_all_manifests_content_gates_counts_per_manifest_content() {
        // RISK: ungating — copying a manifest's counts into BOTH the data AND delete column families.
        // A DATA manifest must report its counts ONLY in the *_data_files_count columns (delete columns
        // 0); a DELETE manifest the reverse.
        let fixture = TableTestFixture::new();
        setup_all_manifests_fixture(&fixture).await;

        let batch = scan_to_batch(
            fixture
                .table
                .inspect()
                .all_manifests()
                .scan()
                .await
                .unwrap(),
        )
        .await;
        let i32col = |name: &str, index: usize| {
            batch
                .column_by_name(name)
                .unwrap()
                .as_primitive::<Int32Type>()
                .value(index)
        };

        // SHARED DATA manifest under the current snapshot: 2 Added data files.
        let data_row = row_index(&batch, "shared_data.avro", CURRENT_SNAPSHOT_ID);
        assert_eq!(i32col("added_data_files_count", data_row), 2);
        assert_eq!(i32col("existing_data_files_count", data_row), 0);
        assert_eq!(i32col("deleted_data_files_count", data_row), 0);
        assert_eq!(i32col("added_delete_files_count", data_row), 0);
        assert_eq!(i32col("existing_delete_files_count", data_row), 0);
        assert_eq!(i32col("deleted_delete_files_count", data_row), 0);

        // DELETE manifest under the current snapshot: 1 Added delete file.
        let delete_row = row_index(&batch, "cur_delete.avro", CURRENT_SNAPSHOT_ID);
        assert_eq!(i32col("added_data_files_count", delete_row), 0);
        assert_eq!(i32col("existing_data_files_count", delete_row), 0);
        assert_eq!(i32col("deleted_data_files_count", delete_row), 0);
        assert_eq!(i32col("added_delete_files_count", delete_row), 1);
        assert_eq!(i32col("existing_delete_files_count", delete_row), 0);
        assert_eq!(i32col("deleted_delete_files_count", delete_row), 0);
    }

    #[tokio::test]
    async fn test_all_manifests_reference_snapshot_id_distinct_from_added_snapshot_id() {
        // RISK: emitting added_snapshot_id where reference_snapshot_id belongs. The shared manifest's
        // added_snapshot_id is the PARENT, but it is referenced by the CURRENT snapshot's list too, so
        // that row's reference_snapshot_id must be CURRENT while its added_snapshot_id stays PARENT.
        let fixture = TableTestFixture::new();
        setup_all_manifests_fixture(&fixture).await;

        let batch = scan_to_batch(
            fixture
                .table
                .inspect()
                .all_manifests()
                .scan()
                .await
                .unwrap(),
        )
        .await;
        let added = batch
            .column_by_name("added_snapshot_id")
            .unwrap()
            .as_primitive::<Int64Type>();

        let current_row = row_index(&batch, "shared_data.avro", CURRENT_SNAPSHOT_ID);
        assert_eq!(added.value(current_row), PARENT_SNAPSHOT_ID);

        let parent_row = row_index(&batch, "shared_data.avro", PARENT_SNAPSHOT_ID);
        assert_eq!(added.value(parent_row), PARENT_SNAPSHOT_ID);
    }

    #[tokio::test]
    async fn test_all_manifests_key_metadata_present_when_set_null_when_absent() {
        // RISK: dropping or wrongly wiring key_metadata. The DELETE manifest was written with key_metadata
        // bytes; the SHARED DATA manifest with none.
        let fixture = TableTestFixture::new();
        setup_all_manifests_fixture(&fixture).await;

        let batch = scan_to_batch(
            fixture
                .table
                .inspect()
                .all_manifests()
                .scan()
                .await
                .unwrap(),
        )
        .await;
        let key_metadata = batch
            .column_by_name("key_metadata")
            .unwrap()
            .as_binary::<i64>();

        let delete_row = row_index(&batch, "cur_delete.avro", CURRENT_SNAPSHOT_ID);
        assert!(key_metadata.is_valid(delete_row));
        assert_eq!(key_metadata.value(delete_row), &DELETE_KEY_METADATA);

        let data_row = row_index(&batch, "shared_data.avro", CURRENT_SNAPSHOT_ID);
        assert!(key_metadata.is_null(data_row));
    }

    #[tokio::test]
    async fn test_all_manifests_renders_partition_summary_bounds_as_strings() {
        // RISK: partition-summary conversion. The shared DATA manifest spans partitions 100 and 300, so
        // its single FieldSummary's lower/upper bound must render to the strings "100" and "300".
        let fixture = TableTestFixture::new();
        setup_all_manifests_fixture(&fixture).await;

        let batch = scan_to_batch(
            fixture
                .table
                .inspect()
                .all_manifests()
                .scan()
                .await
                .unwrap(),
        )
        .await;
        let data_row = row_index(&batch, "shared_data.avro", CURRENT_SNAPSHOT_ID);

        let summaries = batch
            .column_by_name("partition_summaries")
            .unwrap()
            .as_list::<i32>();
        let element = summaries.value(data_row);
        let element = element.as_struct();
        let lower = element
            .column_by_name("lower_bound")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let upper = element
            .column_by_name("upper_bound")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(lower.value(0), "100");
        assert_eq!(upper.value(0), "300");
    }

    #[tokio::test]
    async fn test_all_manifests_arrow_schema_columns_ids_and_nullability() {
        // RISK: wrong column order / field ids / nullability. Pin the 14 columns in Java
        // AllManifestsTable order with exact ids AND nullability — especially the REQUIRED delete-count
        // columns (15/16/17), reference_snapshot_id/18 (required), key_metadata/19 (optional, Binary).
        let fixture = TableTestFixture::new();
        let arrow_schema =
            schema_to_arrow_schema(&fixture.table.inspect().all_manifests().schema()).unwrap();

        let columns: Vec<(String, String, bool)> = arrow_schema
            .fields()
            .iter()
            .map(|field| {
                (
                    field.name().clone(),
                    field.metadata().get("PARQUET:field_id").unwrap().clone(),
                    field.is_nullable(),
                )
            })
            .collect();

        let expected: Vec<(String, String, bool)> = vec![
            ("content", "14", false),
            ("path", "1", false),
            ("length", "2", false),
            ("partition_spec_id", "3", true),
            ("added_snapshot_id", "4", true),
            ("added_data_files_count", "5", true),
            ("existing_data_files_count", "6", true),
            ("deleted_data_files_count", "7", true),
            ("added_delete_files_count", "15", false),
            ("existing_delete_files_count", "16", false),
            ("deleted_delete_files_count", "17", false),
            ("partition_summaries", "8", true),
            ("reference_snapshot_id", "18", false),
            ("key_metadata", "19", true),
        ]
        .into_iter()
        .map(|(name, id, nullable)| (name.to_string(), id.to_string(), nullable))
        .collect();
        assert_eq!(columns, expected);

        // key_metadata maps to Arrow LargeBinary (Iceberg Binary).
        let key_metadata = arrow_schema.field_with_name("key_metadata").unwrap();
        assert_eq!(
            key_metadata.data_type(),
            &arrow_schema::DataType::LargeBinary
        );
    }

    #[tokio::test]
    async fn test_all_manifests_renders_null_contains_nan_without_panicking() {
        // RISK: a production PANIC. Java's `PartitionFieldSummary.containsNaN()` returns a nullable
        // `Boolean` that is null when the manifest carries no NaN info (V1 manifests, or V2 written
        // without it), and `manifestFileToRow` puts that null into the `contains_nan` cell even though
        // Java's schema nominally marks the field `required`. Arrow ENFORCES non-nullability at
        // array-build time, so a verbatim-`required` Rust schema panics ("Found unmasked nulls for
        // non-nullable StructArray field \"contains_nan\"") on the common `contains_nan == None` case.
        // This pins that the scan instead emits a NULL `contains_nan` cell, mirroring Java's value.
        let fixture = TableTestFixture::new();

        // Single DATA manifest committed by the current snapshot, then its sole field summary's
        // contains_nan is forced to None (the writer otherwise defaults it to Some(false)).
        let metadata = fixture.table.metadata().clone();
        let current_snapshot = metadata.current_snapshot().unwrap();
        let current_schema = current_snapshot.schema(&metadata).unwrap();
        let current_partition_spec = metadata.default_partition_spec();

        let mut writer = ManifestWriterBuilder::new(
            fixture
                .table
                .file_io()
                .new_output(format!("{}/metadata/nan_none.avro", fixture.table_location))
                .unwrap(),
            Some(current_snapshot.snapshot_id()),
            None,
            current_schema.clone(),
            current_partition_spec.as_ref().clone(),
        )
        .build_v2_data();
        writer
            .add_entry(added_data_entry(
                &fixture.table_location,
                "nan-1.parquet",
                100,
            ))
            .unwrap();
        let mut manifest = writer.write_manifest_file().await.unwrap();

        // Force contains_nan == None on the (single) partition field summary.
        let mut partitions = manifest.partitions.clone().unwrap();
        assert_eq!(partitions.len(), 1, "fixture spec has one partition field");
        partitions[0].contains_nan = None;
        manifest.partitions = Some(partitions);

        // The scan iterates ALL snapshots, so the parent's (empty) list must exist on disk too.
        let parent_snapshot = current_snapshot.parent_snapshot(&metadata).unwrap();
        write_manifest_list(&fixture, &parent_snapshot, vec![]).await;
        write_manifest_list(&fixture, current_snapshot, vec![manifest]).await;

        // Must NOT panic.
        let batch = scan_to_batch(
            fixture
                .table
                .inspect()
                .all_manifests()
                .scan()
                .await
                .unwrap(),
        )
        .await;

        let row = row_index(&batch, "nan_none.avro", current_snapshot.snapshot_id());
        let summaries = batch
            .column_by_name("partition_summaries")
            .unwrap()
            .as_list::<i32>();
        let element = summaries.value(row);
        let element = element.as_struct();
        let contains_nan = element.column_by_name("contains_nan").unwrap().as_boolean();
        // Java would emit null here; the Arrow cell must be null, not a coalesced false.
        assert!(
            contains_nan.is_null(0),
            "contains_nan must be NULL when the manifest summary has none"
        );
    }

    #[tokio::test]
    async fn test_all_manifests_empty_table_yields_no_rows() {
        // RISK: panic / spurious rows on a table with no snapshots.
        let fixture = TableTestFixture::new_empty();
        let batch = scan_to_batch(
            fixture
                .table
                .inspect()
                .all_manifests()
                .scan()
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(batch.num_rows(), 0);
        // Schema is still the full 14-column shape.
        assert_eq!(batch.num_columns(), 14);
    }
}
