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

//! Shared helpers for the vortex file format integration.

use vortex::error::VortexError;
use vortex::extension::datetime::TimeUnit;

use crate::{Error, ErrorKind, Result};

/// Converts a [`VortexError`] into an iceberg [`Error`].
pub(crate) fn to_iceberg_error(err: VortexError) -> Error {
    Error::new(ErrorKind::Unexpected, "Vortex error").with_source(err)
}

/// Losslessly converts a temporal value between units; conversions that would
/// lose precision (e.g. microseconds to seconds) are rejected.
pub(crate) fn convert_temporal_value(value: i64, from: TimeUnit, to: TimeUnit) -> Result<i64> {
    fn nanos_per(unit: TimeUnit) -> i64 {
        match unit {
            TimeUnit::Nanoseconds => 1,
            TimeUnit::Microseconds => 1_000,
            TimeUnit::Milliseconds => 1_000_000,
            TimeUnit::Seconds => 1_000_000_000,
            TimeUnit::Days => 86_400_000_000_000,
        }
    }

    let (from_nanos, to_nanos) = (nanos_per(from), nanos_per(to));
    if from_nanos % to_nanos != 0 {
        return Err(Error::new(
            ErrorKind::FeatureUnsupported,
            format!("Lossy temporal unit conversion from {from} to {to}"),
        ));
    }
    value.checked_mul(from_nanos / to_nanos).ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Temporal value {value} overflows when converted from {from} to {to}"),
        )
    })
}
