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

//! Conversion between Iceberg and Arrow schema

mod schema;
pub use schema::*;

mod nan_val_cnt_visitor;
pub(crate) use nan_val_cnt_visitor::*;
pub(crate) mod caching_delete_file_loader;
/// Delete File loader
pub mod delete_file_loader;
pub(crate) mod delete_filter;

mod int96;
mod reader;
/// RecordBatch projection utilities
pub mod record_batch_projector;
pub(crate) mod record_batch_transformer;
mod scan_metrics;
mod value;

pub use reader::*;
pub use scan_metrics::{ScanMetrics, ScanResult};
pub use value::*;
/// Partition value calculator for computing partition values
pub mod partition_value_calculator;
pub use partition_value_calculator::*;
/// Record batch partition splitter for partitioned tables
pub mod record_batch_partition_splitter;
pub use record_batch_partition_splitter::*;

#[cfg(test)]
pub(crate) mod test_utils {
    use std::fs::File;

    use arrow_array::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::encryption::encrypt::FileEncryptionProperties;
    use parquet::file::properties::WriterProperties;

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
}
