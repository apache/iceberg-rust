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

//! OpenDAL-based storage implementation for Apache Iceberg.
//!
//! This crate provides [`OpenDalStorage`] and [`OpenDalStorageFactory`],
//! which implement the [`Storage`](iceberg::io::Storage) and
//! [`StorageFactory`](iceberg::io::StorageFactory) traits from the `iceberg` crate
//! using [OpenDAL](https://opendal.apache.org/) as the backend.

mod storage;
mod utils;

#[cfg(feature = "storage-azdls")]
mod azdls;
#[cfg(feature = "storage-fs")]
mod fs;
#[cfg(feature = "storage-gcs")]
mod gcs;
#[cfg(feature = "storage-memory")]
mod memory;
#[cfg(feature = "storage-oss")]
mod oss;
#[cfg(feature = "storage-s3")]
mod s3;

pub use storage::{OpenDalStorage, OpenDalStorageFactory};

#[cfg(feature = "storage-s3")]
pub use s3::CustomAwsCredentialLoader;
