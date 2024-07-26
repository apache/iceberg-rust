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

//! Native Rust implementation of Apache Iceberg

#![deny(missing_docs)]

#[macro_use]
extern crate derive_builder;

mod error;
pub use error::{Error, ErrorKind, Result};

mod catalog;

pub use catalog::{
    Catalog, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent, TableRequirement,
    TableUpdate,
};

pub mod table;

mod avro;
pub mod io;
pub mod spec;

pub mod scan;

pub mod expr;
pub mod transaction;
pub mod transform;

mod runtime;

pub mod arrow;
pub mod writer;
