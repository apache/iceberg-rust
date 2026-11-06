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

//! Reusable types for parsing catalog and table properties.

use std::convert::Infallible;
use std::str::FromStr;
use std::time::Duration;

use crate::{Error, ErrorKind, Result};

/// A boolean property that is true only for a case-insensitive `true` value.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BoolProp(bool);

impl BoolProp {
    /// Returns the boolean value.
    pub fn get(self) -> bool {
        self.0
    }
}

impl From<bool> for BoolProp {
    fn from(value: bool) -> Self {
        Self(value)
    }
}

impl FromStr for BoolProp {
    type Err = Infallible;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        Ok(Self(value.eq_ignore_ascii_case("true")))
    }
}

/// An unsigned integer property within the inclusive bounds `MIN` and `MAX`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct U32Prop<const MIN: u32, const MAX: u32>(u32);

impl<const MIN: u32, const MAX: u32> U32Prop<MIN, MAX> {
    /// Returns the integer value.
    pub fn get(self) -> u32 {
        self.0
    }
}

impl<const MIN: u32, const MAX: u32> TryFrom<u32> for U32Prop<MIN, MAX> {
    type Error = Error;

    fn try_from(value: u32) -> Result<Self> {
        if !(MIN..=MAX).contains(&value) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Property value {value} must be between {MIN} and {MAX}"),
            ));
        }
        Ok(Self(value))
    }
}

impl<const MIN: u32, const MAX: u32> FromStr for U32Prop<MIN, MAX> {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        value
            .parse::<u32>()
            .map_err(|error| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Invalid integer property: {value}"),
                )
                .with_source(error)
            })?
            .try_into()
    }
}

/// A duration property parsed as milliseconds within inclusive bounds.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DurationMsProp<const MIN_MS: u64, const MAX_MS: u64>(Duration);

impl<const MIN_MS: u64, const MAX_MS: u64> DurationMsProp<MIN_MS, MAX_MS> {
    /// Returns the duration value.
    pub fn get(self) -> Duration {
        self.0
    }
}

impl<const MIN_MS: u64, const MAX_MS: u64> TryFrom<u64> for DurationMsProp<MIN_MS, MAX_MS> {
    type Error = Error;

    fn try_from(millis: u64) -> Result<Self> {
        if !(MIN_MS..=MAX_MS).contains(&millis) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Property value {millis} must be between {MIN_MS} and {MAX_MS} milliseconds"
                ),
            ));
        }
        Ok(Self(Duration::from_millis(millis)))
    }
}

impl<const MIN_MS: u64, const MAX_MS: u64> FromStr for DurationMsProp<MIN_MS, MAX_MS> {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        value
            .parse::<u64>()
            .map_err(|error| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Invalid duration property: {value}"),
                )
                .with_source(error)
            })?
            .try_into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bool_property() {
        for (value, expected) in [
            ("true", true),
            ("TRUE", true),
            ("True", true),
            ("false", false),
            ("FALSE", false),
            ("", false),
            ("invalid", false),
            ("1", false),
            (" true ", false),
        ] {
            assert_eq!(value.parse::<BoolProp>().unwrap().get(), expected);
        }
    }

    #[test]
    fn test_integer_property_bounds() {
        for value in [10, 15, 20] {
            assert_eq!(
                value.to_string().parse::<U32Prop<10, 20>>().unwrap().get(),
                value
            );
        }
        for value in ["", "0", "9", "21", "-1", "1.5", "invalid", "4294967296"] {
            assert_eq!(
                value.parse::<U32Prop<10, 20>>().unwrap_err().kind(),
                ErrorKind::DataInvalid
            );
        }
        assert_eq!(
            "4294967295"
                .parse::<U32Prop<0, { u32::MAX }>>()
                .unwrap()
                .get(),
            u32::MAX
        );
    }

    #[test]
    fn test_duration_property_bounds() {
        for value in [10, 15, 20] {
            assert_eq!(
                value
                    .to_string()
                    .parse::<DurationMsProp<10, 20>>()
                    .unwrap()
                    .get(),
                Duration::from_millis(value)
            );
        }
        for value in [
            "",
            "0",
            "9",
            "21",
            "-1",
            "1.5",
            "invalid",
            "18446744073709551616",
        ] {
            assert_eq!(
                value.parse::<DurationMsProp<10, 20>>().unwrap_err().kind(),
                ErrorKind::DataInvalid
            );
        }
        assert_eq!(
            "18446744073709551615"
                .parse::<DurationMsProp<0, { u64::MAX }>>()
                .unwrap()
                .get(),
            Duration::from_millis(u64::MAX)
        );
    }
}
