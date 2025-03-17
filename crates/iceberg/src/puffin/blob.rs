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

use std::collections::HashMap;

/// A serialized form of a "compact" Theta sketch produced by the Apache DataSketches library.
pub(crate) const APACHE_DATASKETCHES_THETA_V1: &str = "apache-datasketches-theta-v1";

/// The blob
#[derive(Debug, PartialEq, Clone)]
pub(crate) struct Blob {
    /// See blob types: https://iceberg.apache.org/puffin-spec/#blob-types
    pub(crate) r#type: String,
    /// List of field IDs the blob was computed for; the order of items is used to compute sketches stored in the blob.
    pub(crate) fields: Vec<i32>,
    /// ID of the Iceberg table's snapshot the blob was computed from
    pub(crate) snapshot_id: i64,
    /// Sequence number of the Iceberg table's snapshot the blob was computed from
    pub(crate) sequence_number: i64,
    /// The uncompressed blob data
    pub(crate) data: Vec<u8>,
    /// Arbitrary meta-information about the blob
    pub(crate) properties: HashMap<String, String>,
}
