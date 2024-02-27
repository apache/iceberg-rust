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
use aws_sdk_dynamodb::error::{BuildError, SdkError};
use iceberg::{Error, ErrorKind, Result};
use std::fmt::Debug;

pub(crate) trait SdkResultExt<T> {
    fn wrap_err(self) -> Result<T>;
}

impl<T, E: Debug> SdkResultExt<T> for std::result::Result<T, SdkError<E>> {
    fn wrap_err(self) -> Result<T> {
        self.map_err(from_sdk_error)
    }
}

impl<T> SdkResultExt<T> for std::result::Result<T, BuildError> {
    fn wrap_err(self) -> Result<T> {
        self.map_err(from_build_error)
    }
}

pub fn from_build_error(error: BuildError) -> Error {
    Error::new(
        ErrorKind::Unexpected,
        "operation failed for dynamodb sdk error".to_string(),
    )
    .with_source(anyhow!("dynamodb sdk error: {:?}", error))
}

pub fn from_sdk_error<E>(error: SdkError<E>) -> Error
where
    E: Debug,
{
    Error::new(
        ErrorKind::Unexpected,
        "operation failed for dynamodb sdk error".to_string(),
    )
    .with_source(anyhow!("dynamodb sdk error: {:?}", error))
}
