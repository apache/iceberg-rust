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

//! Iceberg File Writer

use super::{CurrentFileStatus, IcebergWriteResult};
use crate::Result;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

/// File writer builder trait.
#[async_trait::async_trait]
pub trait FileWriterBuilder: Send + Clone + 'static {
    /// The associated file writer type.
    type R: FileWriter;
    /// Build file writer.
    async fn build(self, schema: &SchemaRef) -> Result<Self::R>;
}

/// File writer focus on writing record batch to different physical file format.(Such as parquet. orc)
#[async_trait::async_trait]
pub trait FileWriter: Send + 'static + CurrentFileStatus {
    /// The associated file write result type.
    type R: FileWriteResult;
    /// Write record batch to file.
    async fn write(&mut self, batch: &RecordBatch) -> Result<()>;
    /// Close file writer.
    async fn close(self) -> Result<Vec<Self::R>>;
}

/// File write result.
pub trait FileWriteResult: Send + 'static {
    /// The associated iceberg write result type.
    type R: IcebergWriteResult;
    /// Convert to iceberg write result.
    fn to_iceberg_result(self) -> Self::R;
}
