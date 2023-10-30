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

//! Table API for Apache Iceberg

use typed_builder::TypedBuilder;
use crate::io::FileIO;
use crate::spec::TableMetadata;
use crate::TableIdent;

/// Table represents a table in the catalog.
#[derive(TypedBuilder)]
pub struct Table {
    file_io: FileIO,
    #[builder(default, setter(strip_option))]
    metadata_location: Option<String>,
    metadata: TableMetadata,
    identifier: TableIdent,
}

impl Table {
    pub fn identifier(&self) -> &TableIdent {
        &self.identifier
    }
    pub fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }

    pub fn metadata_location(&self) -> Option<&str> {
        self.metadata_location.as_deref()
    }
}
