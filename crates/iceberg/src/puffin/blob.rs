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
pub const APACHE_DATASKETCHES_THETA_V1: &str = "apache-datasketches-theta-v1";
/// A serialized form of a deletion vector.
pub const DELETION_VECTOR_V1: &str = "deletion-vector-v1";

/// The blob
#[derive(Debug, PartialEq, Clone)]
pub struct Blob {
    pub(crate) r#type: String,
    pub(crate) fields: Vec<i32>,
    pub(crate) snapshot_id: i64,
    pub(crate) sequence_number: i64,
    pub(crate) data: Vec<u8>,
    pub(crate) properties: HashMap<String, String>,
}

impl Blob {
    /// Returns a BlobBuilder to build a build
    pub fn builder() -> BlobBuilder {
        BlobBuilder::default()
    }

    #[inline]
    /// See blob types: https://iceberg.apache.org/puffin-spec/#blob-types
    pub fn blob_type(&self) -> &str {
        &self.r#type
    }

    #[inline]
    /// List of field IDs the blob was computed for; the order of items is used to compute sketches stored in the blob.
    pub fn fields(&self) -> &[i32] {
        &self.fields
    }

    #[inline]
    /// ID of the Iceberg table's snapshot the blob was computed from
    pub fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }

    #[inline]
    /// Sequence number of the Iceberg table's snapshot the blob was computed from
    pub fn sequence_number(&self) -> i64 {
        self.sequence_number
    }

    #[inline]
    /// The uncompressed blob data
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    #[inline]
    /// Arbitrary meta-information about the blob
    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }
}

/// Builder to create a Blob.
#[derive(Debug, Default, PartialEq, Clone)]
pub struct BlobBuilder {
    r#type: String,
    fields: Vec<i32>,
    snapshot_id: i64,
    sequence_number: i64,
    data: Vec<u8>,
    properties: HashMap<String, String>,
}

impl BlobBuilder {
    pub fn r#type(mut self, t: impl Into<String>) -> Self {
        self.r#type = t.into();
        self
    }

    pub fn fields(mut self, fields: Vec<i32>) -> Self {
        self.fields = fields;
        self
    }

    pub fn snapshot_id(mut self, id: i64) -> Self {
        self.snapshot_id = id;
        self
    }

    pub fn sequence_number(mut self, num: i64) -> Self {
        self.sequence_number = num;
        self
    }

    pub fn data(mut self, data: Vec<u8>) -> Self {
        self.data = data;
        self
    }

    pub fn properties(mut self, props: HashMap<String, String>) -> Self {
        self.properties = props;
        self
    }

    pub fn build(self) -> Blob {
        Blob {
            r#type: self.r#type,
            fields: self.fields,
            snapshot_id: self.snapshot_id,
            sequence_number: self.sequence_number,
            data: self.data,
            properties: self.properties,
        }
    }
}
