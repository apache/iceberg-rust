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

//! Task writer module for high-level Iceberg table writing operations.
//!
//! This module provides the [`TaskWriter`] trait and [`BaseTaskWriter`] implementation
//! for writing data to Iceberg tables with automatic partition handling.

use crate::Result;
use crate::writer::{DefaultInput, DefaultOutput};

/// High-level async trait for writing tasks to Iceberg tables.
///
/// The `TaskWriter` trait provides a simplified interface for writing data and retrieving
/// results, abstracting away the complexity of partition handling and writer selection.
///
/// # Type Parameters
///
/// * `I` - Input type (defaults to `DefaultInput` which is `RecordBatch`)
/// * `O` - Output type (defaults to `DefaultOutput` which is `Vec<DataFile>`)
#[async_trait::async_trait]
pub trait TaskWriter<I = DefaultInput, O = DefaultOutput>: Send + 'static {
    /// Write input data to the task writer.
    ///
    /// # Arguments
    ///
    /// * `input` - The input data to write
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success, or an error if the write operation fails.
    async fn write(&mut self, input: I) -> Result<()>;

    /// Close the writer and return the accumulated output.
    ///
    /// # Returns
    ///
    /// Returns the accumulated output (e.g., `Vec<DataFile>`) on success,
    /// or an error if the close operation fails.
    async fn close(self) -> Result<O>;
}
