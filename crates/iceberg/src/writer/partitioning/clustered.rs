// // Licensed to the Apache Software Foundation (ASF) under one
// // or more contributor license agreements.  See the NOTICE file
// // distributed with this work for additional information
// // regarding copyright ownership.  The ASF licenses this file
// // to you under the Apache License, Version 2.0 (the
// // "License"); you may not use this file except in compliance
// // with the License.  You may obtain a copy of the License at
// //
// //   http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing,
// // software distributed under the License is distributed on an
// // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// // KIND, either express or implied.  See the License for the
// // specific language governing permissions and limitations
// // under the License.
//
// //! This module provides the `ClusteredWriter` implementation.
//
// use std::collections::HashSet;
//
// use arrow_array::RecordBatch;
// use async_trait::async_trait;
//
// use crate::spec::{PartitionKey, Struct};
// use crate::writer::{DefaultInput, DefaultOutput, IcebergWriter, IcebergWriterBuilder};
// use crate::writer::file_writer::location_generator::{FileNameGenerator, LocationGenerator};
// use crate::{Error, ErrorKind, Result};
// use crate::writer::partitioning::PartitioningWriter;
//
// /// A writer that writes data to a single partition at a time.
// ///
// /// When data for a new partition arrives, it closes the current writer
// /// and creates a new one for the new partition.
// ///
// /// Once a partition has been written to and closed, any further attempts
// /// to write to that partition will result in an error.
// pub struct ClusteredWriter<B: IcebergWriterBuilder, I: Default + Send = DefaultInput, O: Default + Send = DefaultOutput>
// {
//     inner_builder: B,
//     current_writer: Option<B::R>,
//     current_partition: Option<Struct>,
//     closed_partitions: HashSet<Struct>,
//     output: O,
// }
//
// impl<B: IcebergWriterBuilder, I: Default + Send, O: Default + Send> ClusteredWriter<B, I, O> {
//     /// todo doc
//     pub fn new(
//         inner_builder: B
//     ) -> Self {
//         Self {
//             inner_builder,
//             current_writer: None,
//             current_partition: None,
//             closed_partitions: HashSet::new(),
//             output: O::default(),
//         }
//     }
//
//     /// Close the current writer if it exists
//     async fn close_current_writer(&mut self) -> Result<()> {
//         if let Some(mut writer) = self.current_writer.take() {
//             self.output.extend(writer.close().await?);
//
//             // Add the current partition to the set of closed partitions
//             if let Some(current_partition) = self.current_partition.take() {
//                 self.closed_partitions.insert(current_partition);
//             }
//         }
//
//         Ok(())
//     }
//
//     async fn do_write(&mut self, input: I) -> Result<()> {
//         if let Some(writer) = &mut self.current_writer {
//             writer.write(input).await?;
//             Ok(())
//         } else {
//             Err(Error::new(
//                 ErrorKind::Unexpected,
//                 "Writer is not initialized!",
//             ))
//         }
//     }
// }
//
// #[async_trait]
// impl<B: IcebergWriterBuilder, I: Default + Send, O: Default + Send> PartitioningWriter for ClusteredWriter<B, I, O> {
//     async fn write(&mut self, partition_key: Option<PartitionKey>, input: I) -> Result<()> {
//         if let Some(partition_key) = partition_key {
//             let partition_value = partition_key.data();
//
//             // Check if this partition has been closed already
//             if self.closed_partitions.contains(partition_value) {
//                 return Err(Error::new(
//                     ErrorKind::Unexpected,
//                     format!(
//                         "Cannot write to partition that was previously closed: {:?}",
//                         partition_key
//                     ),
//                 ));
//             }
//
//             // Check if we need to switch to a new partition
//             let need_new_writer = match &self.current_partition {
//                 Some(current) => current != partition_value,
//                 None => true,
//             };
//
//             if need_new_writer {
//                 self.close_current_writer().await?;
//
//                 // Create a new writer for the new partition
//                 self.current_writer = Some(self.inner_builder.clone().build().await?);
//                 self.current_partition = Some(partition_value.clone());
//             }
//         }
//
//         self.do_write(input).await
//     }
//
//     async fn close(&mut self) -> Result<O> {
//         self.close_current_writer().await?;
//
//         // Return all collected data files
//         Ok(std::mem::take(&mut self.output))
//     }
// }
//
// #[cfg(test)]
// mod tests {
//     use std::sync::Arc;
//
//     use arrow_array::{Int32Array, StringArray};
//     use arrow_schema::{DataType, Field, Schema};
//     use parquet::file::properties::WriterProperties;
//     use tempfile::TempDir;
//
//     use super::*;
//     use crate::arrow::FieldMatchMode;
//     use crate::io::FileIOBuilder;
//     use crate::spec::{DataFileFormat, Literal, NestedField, PrimitiveType, Struct, Type};
//     use crate::writer::base_writer::data_file_writer::DataFileWriterBuilder;
//     use crate::writer::file_writer::ParquetWriterBuilder;
//     use crate::writer::file_writer::location_generator::DefaultFileNameGenerator;
//     use crate::writer::file_writer::location_generator::test::MockLocationGenerator;
//
//     #[tokio::test]
//     async fn test_clustered_writer_basic() -> Result<()> {
//         let temp_dir = TempDir::new()?;
//         let file_io = FileIOBuilder::new_fs_io().build()?;
//         let location_gen =
//             MockLocationGenerator::new(temp_dir.path().to_str().unwrap().to_string());
//         let file_name_gen =
//             DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);
//
//         // Create schema
//         let schema = Arc::new(
//             crate::spec::Schema::builder()
//                 .with_schema_id(1)
//                 .with_fields(vec![
//                     NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
//                     NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
//                     NestedField::required(3, "category", Type::Primitive(PrimitiveType::String))
//                         .into(),
//                 ])
//                 .build()?,
//         );
//
//         // Create writer builder
//         let parquet_writer_builder = ParquetWriterBuilder::new_with_match_mode(
//             WriterProperties::builder().build(),
//             schema.clone(),
//             FieldMatchMode::Name,
//         );
//
//         // Create data file writer builder
//         let data_file_writer_builder = DataFileWriterBuilder::new(parquet_writer_builder, None, 0);
//
//         // Create clustered writer
//         let mut writer = ClusteredWriter::new(
//             data_file_writer_builder,
//             location_gen,
//             file_name_gen,
//             file_io.clone(),
//             1024 * 1024, // 1MB
//         );
//
//         // Create test data for partition A
//         let arrow_schema = Schema::new(vec![
//             Field::new("id", DataType::Int32, false),
//             Field::new("name", DataType::Utf8, false),
//             Field::new("category", DataType::Utf8, false),
//         ]);
//
//         let batch_a1 = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
//             Arc::new(Int32Array::from(vec![1, 2, 3])),
//             Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
//             Arc::new(StringArray::from(vec!["A", "A", "A"])),
//         ])?;
//
//         let batch_a2 = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
//             Arc::new(Int32Array::from(vec![4, 5])),
//             Arc::new(StringArray::from(vec!["Dave", "Eve"])),
//             Arc::new(StringArray::from(vec!["A", "A"])),
//         ])?;
//
//         // Create test data for partition B
//         let batch_b = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
//             Arc::new(Int32Array::from(vec![6, 7, 8])),
//             Arc::new(StringArray::from(vec!["Frank", "Grace", "Heidi"])),
//             Arc::new(StringArray::from(vec!["B", "B", "B"])),
//         ])?;
//
//         // Create partition keys
//         let partition_a = PartitionKey::new(
//             crate::spec::PartitionSpec::unpartition_spec().into(),
//             schema.clone(),
//             Struct::from_iter([Some(Literal::string("A"))]),
//         );
//
//         let partition_b = PartitionKey::new(
//             crate::spec::PartitionSpec::unpartition_spec().into(),
//             schema.clone(),
//             Struct::from_iter([Some(Literal::string("B"))]),
//         );
//
//         // Write data to partition A
//         writer.write(partition_a.clone(), batch_a1.clone()).await?;
//         writer.write(partition_a.clone(), batch_a2.clone()).await?;
//
//         // Write data to partition B
//         writer.write(partition_b.clone(), batch_b.clone()).await?;
//
//         // Try to write to partition A again (should fail)
//         let result = writer.write(partition_a.clone(), batch_a1.clone()).await;
//         assert!(
//             result.is_err(),
//             "Expected error when writing to closed partition"
//         );
//
//         // Close writer and get data files
//         let data_files = writer.close().await?;
//
//         // Verify files were created
//         assert_eq!(data_files.len(), 2, "Expected two data files to be created");
//
//         Ok(())
//     }
// }
