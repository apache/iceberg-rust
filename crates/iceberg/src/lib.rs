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

//! Apache Iceberg Official Native Rust Implementation
//!
//! # Examples
//!
//! ## Scan A Table
//!
//! ```rust, no_run
//! use futures::TryStreamExt;
//! use iceberg::io::{FileIO, FileIOBuilder};
//! use iceberg::{Catalog, Result, TableIdent};
//! use iceberg_catalog_memory::MemoryCatalog;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Build your file IO.
//!     let file_io = FileIOBuilder::new("memory").build()?;
//!     // Connect to a catalog.
//!     let catalog = MemoryCatalog::new(file_io, None);
//!     // Load table from catalog.
//!     let table = catalog
//!         .load_table(&TableIdent::from_strs(["hello", "world"])?)
//!         .await?;
//!     // Build table scan.
//!     let stream = table
//!         .scan()
//!         .select(["name", "id"])
//!         .build()?
//!         .to_arrow()
//!         .await?;
//!
//!     // Consume this stream like arrow record batch stream.
//!     let _data: Vec<_> = stream.try_collect().await?;
//!     Ok(())
//! }
//! ```
//!
//! ## Fast append data to table
//!
//! ```rust, no_run
//! use std::sync::Arc;
//!
//! use arrow_array::{ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray};
//! use async_trait::async_trait;
//! use iceberg::io::{FileIO, FileIOBuilder};
//! use iceberg::spec::DataFile;
//! use iceberg::transaction::Transaction;
//! use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
//! use iceberg::writer::file_writer::location_generator::{
//!     DefaultFileNameGenerator, DefaultLocationGenerator,
//! };
//! use iceberg::writer::file_writer::ParquetWriterBuilder;
//! use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
//! use iceberg::{Catalog, Result, TableIdent};
//! use iceberg_catalog_memory::MemoryCatalog;
//! use parquet::file::properties::WriterProperties;
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Build your file IO.
//!     let file_io = FileIOBuilder::new("memory").build()?;
//!     // Connect to a catalog.
//!     let catalog = MemoryCatalog::new(file_io, None);
//!     // Load table from catalog.
//!     let table = catalog
//!         .load_table(&TableIdent::from_strs(["hello", "world"])?)
//!         .await?;
//!
//!     // Create the data file writer.
//!     let schema: Arc<arrow_schema::Schema> = Arc::new(
//!         table
//!             .metadata()
//!             .current_schema()
//!             .as_ref()
//!             .try_into()
//!             .unwrap(),
//!     );
//!     let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
//!     let file_name_generator = DefaultFileNameGenerator::new(
//!         "test".to_string(),
//!         None,
//!         iceberg::spec::DataFileFormat::Parquet,
//!     );
//!     let parquet_writer_builder = ParquetWriterBuilder::new(
//!         WriterProperties::default(),
//!         table.metadata().current_schema().clone(),
//!         table.file_io().clone(),
//!         location_generator.clone(),
//!         file_name_generator.clone(),
//!     );
//!     let data_file_writer_builder = DataFileWriterBuilder::new(parquet_writer_builder, None);
//!     let mut data_file_writer = data_file_writer_builder.build().await.unwrap();
//!
//!     // Write new data.
//!     let col1 = StringArray::from(vec![Some("foo"), Some("bar"), None, Some("baz")]);
//!     let col2 = Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4)]);
//!     let col3 = BooleanArray::from(vec![Some(true), Some(false), None, Some(false)]);
//!     let batch = RecordBatch::try_new(schema.clone(), vec![
//!         Arc::new(col1) as ArrayRef,
//!         Arc::new(col2) as ArrayRef,
//!         Arc::new(col3) as ArrayRef,
//!     ])
//!     .unwrap();
//!     data_file_writer.write(batch.clone()).await.unwrap();
//!
//!     // Close writer and get the DataFile.
//!     let data_file = data_file_writer.close().await.unwrap();
//!
//!     // Append the DataFile.
//!     let tx = Transaction::new(&table);
//!     let mut fast_append = tx.fast_append(None, vec![]).unwrap();
//!     fast_append.add_data_files(data_file.clone()).unwrap();
//!     let tx = fast_append.apply().await.unwrap();
//!     let _table = tx.commit(&catalog).await.unwrap();
//!
//!     Ok(())
//! }
//! ```

#![deny(missing_docs)]

#[macro_use]
extern crate derive_builder;
extern crate core;

mod error;
pub use error::{Error, ErrorKind, Result};

mod catalog;

pub use catalog::{
    Catalog, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent, TableRequirement,
    TableUpdate, ViewCreation,
};

pub mod table;

mod avro;
pub mod io;
pub mod spec;

pub mod inspect;
pub mod scan;

pub mod expr;
pub mod transaction;
pub mod transform;

mod runtime;

pub mod arrow;
pub(crate) mod delete_file_index;
mod utils;
pub mod writer;

mod puffin;
