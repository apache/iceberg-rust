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

//! Format registry for runtime format resolution.

use std::any::TypeId;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use crate::spec::DataFileFormat;
use crate::{Error, ErrorKind, Result};

use super::{DataBatch, FormatReader, FormatWriter};

/// Maps `(DataFileFormat, TypeId)` pairs to format reader/writer implementations.
///
/// The two-dimensional key matches the Java `FormatModelRegistry` pattern where
/// the key is `(FileFormat, Class<?>)`. The first dimension is the file format
/// (determined at runtime from manifest metadata). The second dimension is the
/// batch type the caller wants to produce or consume (determined at compile time
/// by the engine integration).
///
/// This design supports the pattern where multiple engines register their own
/// optimized readers for the same format. Spark registers a Parquet reader that
/// produces `ColumnarBatch`. Flink registers one that produces `RowData`. In
/// Rust, DataFusion registers a reader producing `RecordBatch`. A future Comet
/// integration could register one producing its own batch type.
///
/// Callers specify the batch type they expect via the generic parameter on
/// `reader::<B>(format)` and `writer::<B>(format)`. They never name the
/// concrete reader/writer implementation.
pub struct FormatRegistry {
    readers: HashMap<(DataFileFormat, TypeId), Arc<dyn FormatReader>>,
    writers: HashMap<(DataFileFormat, TypeId), Arc<dyn FormatWriter>>,
}

impl FormatRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            readers: HashMap::new(),
            writers: HashMap::new(),
        }
    }

    /// Register a format reader for a specific batch type.
    ///
    /// Panics if a reader for the same (format, batch type) is already registered.
    pub fn register_reader<B: DataBatch>(&mut self, reader: Arc<dyn FormatReader>) {
        let key = (reader.format(), TypeId::of::<B>());
        if self.readers.contains_key(&key) {
            panic!(
                "FormatReader already registered for {:?} with batch type {:?}",
                key.0, key.1
            );
        }
        self.readers.insert(key, reader);
    }

    /// Register a format writer for a specific batch type.
    ///
    /// Panics if a writer for the same (format, batch type) is already registered.
    pub fn register_writer<B: DataBatch>(&mut self, writer: Arc<dyn FormatWriter>) {
        let key = (writer.format(), TypeId::of::<B>());
        if self.writers.contains_key(&key) {
            panic!(
                "FormatWriter already registered for {:?} with batch type {:?}",
                key.0, key.1
            );
        }
        self.writers.insert(key, writer);
    }

    /// Get the reader for a format that produces batch type `B`.
    ///
    /// The generic parameter `B` is the batch type the caller wants to consume.
    /// The registry returns the reader registered for `(format, TypeId::of::<B>())`.
    pub fn reader<B: DataBatch>(&self, format: DataFileFormat) -> Result<&dyn FormatReader> {
        let key = (format, TypeId::of::<B>());
        self.readers.get(&key).map(|r| r.as_ref()).ok_or_else(|| {
            Error::new(
                ErrorKind::FeatureUnsupported,
                format!(
                    "No reader registered for {format:?} producing batch type {:?}",
                    TypeId::of::<B>()
                ),
            )
        })
    }

    /// Get the writer for a format that accepts batch type `B`.
    ///
    /// The generic parameter `B` is the batch type the caller will feed to the writer.
    /// The registry returns the writer registered for `(format, TypeId::of::<B>())`.
    pub fn writer<B: DataBatch>(&self, format: DataFileFormat) -> Result<&dyn FormatWriter> {
        let key = (format, TypeId::of::<B>());
        self.writers.get(&key).map(|w| w.as_ref()).ok_or_else(|| {
            Error::new(
                ErrorKind::FeatureUnsupported,
                format!(
                    "No writer registered for {format:?} accepting batch type {:?}",
                    TypeId::of::<B>()
                ),
            )
        })
    }
}

impl Default for FormatRegistry {
    fn default() -> Self {
        // Format registrations will be added here as implementations land.
        // #[cfg(feature = "format-parquet")]
        // {
        //     let mut registry = Self::new();
        //     let model = Arc::new(ParquetArrowModel::new());
        //     registry.register_reader::<RecordBatch>(model.clone());
        //     registry.register_writer::<RecordBatch>(model);
        //     return registry;
        // }
        Self::new()
    }
}

/// Process-wide default registry, initialized on first access.
pub fn default_format_registry() -> &'static FormatRegistry {
    static REGISTRY: OnceLock<FormatRegistry> = OnceLock::new();
    REGISTRY.get_or_init(FormatRegistry::default)
}
