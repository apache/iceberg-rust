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

pub mod clustered_data_writer;
pub mod fanout_data_writer;

use crate::Result;
use crate::spec::PartitionKey;
use crate::writer::{DefaultInput, DefaultOutput};

/// A writer that can write data to partitioned tables.
///
/// This trait provides methods for writing data with optional partition keys and
/// closing the writer to retrieve the output.
#[async_trait::async_trait]
pub trait PartitioningWriter<I = DefaultInput, O = DefaultOutput>: Send + 'static {
    /// Write data with an optional partition key.
    ///
    /// # Parameters
    ///
    /// * `partition_key` - Optional partition key to determine which partition to write to
    /// * `input` - The input data to write
    ///
    /// # Returns
    ///
    /// `Ok(())` on success, or an error if the write operation fails.
    async fn write(&mut self, partition_key: Option<PartitionKey>, input: I) -> Result<()>;

    /// Close the writer and return the output.
    ///
    /// # Returns
    ///
    /// The accumulated output from all write operations.
    async fn close(&mut self) -> Result<O>;
}
