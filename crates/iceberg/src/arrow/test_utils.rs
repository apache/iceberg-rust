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

use std::fs::File;

use arrow_array::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::basic::Compression;
use parquet::encryption::decrypt::FileDecryptionProperties;
use parquet::encryption::encrypt::FileEncryptionProperties;
use parquet::file::properties::WriterProperties;

use crate::io::FileIO;
use crate::spec::{
    FormatVersion, ManifestContentType, ManifestFile, ManifestListWriter,
    UNASSIGNED_SEQUENCE_NUMBER,
};

/// A manifest list entry that a manifest list writer for snapshot 1
/// assigns sequence numbers to.
pub(crate) fn listed_manifest(path: &str, content: ManifestContentType) -> ManifestFile {
    ManifestFile {
        manifest_path: path.to_string(),
        manifest_length: 100,
        partition_spec_id: 0,
        content,
        sequence_number: UNASSIGNED_SEQUENCE_NUMBER,
        min_sequence_number: UNASSIGNED_SEQUENCE_NUMBER,
        added_snapshot_id: 1,
        added_files_count: Some(1),
        existing_files_count: Some(2),
        deleted_files_count: Some(3),
        added_rows_count: Some(10),
        existing_rows_count: Some(20),
        deleted_rows_count: Some(30),
        partitions: None,
        key_metadata: None,
        first_row_id: None,
    }
}

/// Writes a manifest list for snapshot 1 with sequence number 5 and, in
/// v3, first row ID 100.
pub(crate) async fn write_manifest_list(
    version: FormatVersion,
    manifests: Vec<ManifestFile>,
) -> Vec<u8> {
    let io = FileIO::new_with_memory();
    let path = "memory:///manifest-list.avro";
    let output = io.new_output(path).unwrap().writer().await.unwrap();
    let mut writer = match version {
        FormatVersion::V1 => ManifestListWriter::v1(output, 1, None),
        FormatVersion::V2 => ManifestListWriter::v2(output, 1, None, 5),
        FormatVersion::V3 => ManifestListWriter::v3(output, 1, None, 5, Some(100)),
    };
    writer.add_manifests(manifests.into_iter()).unwrap();
    writer.close().await.unwrap();
    io.new_input(path).unwrap().read().await.unwrap().to_vec()
}

/// Writes `batch` to `path` as a Parquet file encrypted with `key` and `aad_prefix`.
pub(crate) fn write_encrypted_parquet(
    path: &str,
    batch: &RecordBatch,
    key: &[u8],
    aad_prefix: Option<&[u8]>,
) {
    let mut builder = FileEncryptionProperties::builder(key.to_vec());
    if let Some(aad) = aad_prefix {
        builder = builder.with_aad_prefix(aad.to_vec());
    }
    let encryption_properties = builder.build().unwrap();

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .with_file_encryption_properties(encryption_properties)
        .build();

    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
    writer.write(batch).expect("Writing batch");
    writer.close().unwrap();
}

/// Reads the Parquet file at `path` encrypted with `key` and `aad_prefix`, returning
/// all record batches.
pub(crate) fn read_encrypted_parquet(
    path: &str,
    key: &[u8],
    aad_prefix: Option<&[u8]>,
) -> Vec<RecordBatch> {
    let mut builder = FileDecryptionProperties::builder(key.to_vec());
    if let Some(aad) = aad_prefix {
        builder = builder.with_aad_prefix(aad.to_vec());
    }
    let options =
        ArrowReaderOptions::new().with_file_decryption_properties(builder.build().unwrap());

    let file = File::open(path).unwrap();
    ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)
        .unwrap()
        .build()
        .unwrap()
        .map(|b| b.unwrap())
        .collect()
}
