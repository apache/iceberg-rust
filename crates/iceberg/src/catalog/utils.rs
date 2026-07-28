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

//! Utility functions for catalog operations.

use std::collections::{HashMap, HashSet};

use futures::{TryStreamExt, stream};

use crate::Result;
use crate::io::FileIO;
use crate::spec::ManifestFile;
use crate::table::Table;

const DELETE_CONCURRENCY: usize = 10;

/// Deletes all data and metadata files referenced by the given table metadata.
///
/// This mirrors the Java implementation's `CatalogUtil.dropTableData`.
/// It collects all manifest files, manifest lists, previous metadata files,
/// statistics files, and partition statistics files, then deletes them.
///
/// Data files within manifests are only deleted if the `gc.enabled` table
/// property is `true` (the default), to avoid corrupting other tables that
/// may share the same data files.
pub async fn drop_table_data(table_info: &Table) -> Result<()> {
    let mut manifest_lists_to_delete: HashSet<String> = HashSet::new();
    let mut manifests_to_delete: HashMap<String, ManifestFile> = HashMap::new();

    let metadata = table_info.metadata_ref();
    let io = table_info.file_io();
    // Load all manifest lists concurrently
    let results: Vec<_> =
        futures::future::try_join_all(metadata.snapshots().map(|snapshot| async {
            let manifest_list = table_info.manifest_list_reader(snapshot).load().await?;
            Ok::<_, crate::Error>((snapshot.manifest_list().to_string(), manifest_list))
        }))
        .await?;

    for (manifest_list_location, manifest_list) in results {
        if !manifest_list_location.is_empty() {
            manifest_lists_to_delete.insert(manifest_list_location);
        }
        for manifest_file in manifest_list.entries() {
            manifests_to_delete.insert(manifest_file.manifest_path.clone(), manifest_file.clone());
        }
    }

    // Delete data files only if gc.enabled is true, to avoid corrupting shared tables
    if metadata.table_properties()?.gc_enabled {
        delete_data_files(io, &manifests_to_delete).await?;
    }

    // Delete manifest files
    let manifest_paths: Vec<String> = manifests_to_delete.into_keys().collect();
    io.delete_stream(stream::iter(manifest_paths)).await?;

    // Delete manifest lists
    io.delete_stream(stream::iter(manifest_lists_to_delete))
        .await?;

    // Delete previous metadata files
    let prev_metadata_paths: Vec<String> = metadata
        .metadata_log()
        .iter()
        .map(|m| m.metadata_file.clone())
        .collect();
    io.delete_stream(stream::iter(prev_metadata_paths)).await?;

    // Delete statistics files
    let stats_paths: Vec<String> = metadata
        .statistics_iter()
        .map(|s| s.statistics_path.clone())
        .collect();
    io.delete_stream(stream::iter(stats_paths)).await?;

    // Delete partition statistics files
    let partition_stats_paths: Vec<String> = metadata
        .partition_statistics_iter()
        .map(|s| s.statistics_path.clone())
        .collect();
    io.delete_stream(stream::iter(partition_stats_paths))
        .await?;

    // Delete the current metadata file
    if let Some(location) = table_info.metadata_location() {
        io.delete(location).await?;
    }

    Ok(())
}

/// Reads manifests concurrently and deletes the data files referenced within.
async fn delete_data_files(
    io: &FileIO,
    manifest_files: &HashMap<String, ManifestFile>,
) -> Result<()> {
    stream::iter(manifest_files.values().map(Ok))
        .try_for_each_concurrent(DELETE_CONCURRENCY, |manifest_file| async move {
            let manifest = manifest_file.load_manifest(io).await?;
            let data_file_paths = manifest
                .entries()
                .iter()
                .map(|entry| entry.data_file.file_path().to_string())
                .collect::<Vec<_>>();

            io.delete_stream(stream::iter(data_file_paths)).await
        })
        .await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::encryption::encrypt::FileEncryptionProperties;
    use parquet::file::properties::WriterProperties;

    use super::*;
    use crate::encryption::StandardKeyMetadata;
    use crate::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct, TableMetadataRef};
    use crate::test_utils::make_encrypted_table;
    use crate::transaction::{Transaction, TransactionAction};

    #[tokio::test]
    async fn drop_table_data_deletes_data_files_from_encrypted_manifest() {
        let table = make_encrypted_table().await;
        let data_key = b"0123456789abcdef";
        let aad_prefix = b"aad_prefix";

        let arrow_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(Int32Array::from(
            vec![1, 2, 3],
        ))])
        .unwrap();

        let encryption_properties = FileEncryptionProperties::builder(data_key.to_vec())
            .with_aad_prefix(aad_prefix.to_vec())
            .build()
            .unwrap();
        let writer_properties = WriterProperties::builder()
            .with_file_encryption_properties(encryption_properties)
            .build();

        let mut parquet_bytes = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut parquet_bytes, arrow_schema, Some(writer_properties))
                .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let data_file_path = "memory:///table/data/00000.parquet".to_string();
        let io = table.file_io().clone();
        io.new_output(&data_file_path)
            .unwrap()
            .write(parquet_bytes.clone().into())
            .await
            .unwrap();
        assert!(io.exists(&data_file_path).await.unwrap());

        let key_metadata = StandardKeyMetadata::try_new(data_key)
            .unwrap()
            .with_aad_prefix(aad_prefix)
            .encode()
            .unwrap();

        let new_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(data_file_path.clone())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .record_count(3)
            .file_size_in_bytes(parquet_bytes.len() as u64)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .key_metadata(Some(key_metadata.to_vec()))
            .build()
            .unwrap();

        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![new_file]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let mut builder = table.metadata().clone().into_builder(None);
        for update in updates {
            builder = update.apply(builder).unwrap();
        }
        let table = table.with_metadata(TableMetadataRef::new(builder.build().unwrap().metadata));

        drop_table_data(&table).await.unwrap();

        assert!(
            !io.exists(&data_file_path).await.unwrap(),
            "purge should have deleted the data file referenced by the encrypted manifest"
        );
    }
}
