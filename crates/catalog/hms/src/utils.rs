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

use anyhow::anyhow;
use iceberg::{Error, ErrorKind};
use std::fmt::Debug;
use std::io;

/// Format a thrift error into iceberg error.
pub fn from_thrift_error<T>(error: volo_thrift::error::ResponseError<T>) -> Error
where
    T: Debug,
{
    Error::new(
        ErrorKind::Unexpected,
        "operation failed for hitting thrift error".to_string(),
    )
    .with_source(anyhow!("thrift error: {:?}", error))
}

/// Format an io error into iceberg error.
pub fn from_io_error(error: io::Error) -> Error {
    Error::new(
        ErrorKind::Unexpected,
        "operation failed for hitting io error".to_string(),
    )
    .with_source(error)
}
