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

use thiserror::Error;

#[derive(Error, Debug)]
pub enum IcebergError {
    #[error("The type `{0}` cannot be stored as bytes.")]
    ValueByteConversion(String),
    #[error("Failed to convert slice to array")]
    TryFromSlice(#[from] std::array::TryFromSliceError),
    #[error("Failed to convert u8 to string")]
    Utf8(#[from] std::str::Utf8Error),
}
