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

//! This module contains types to keep sensitive data in memory.

use std::fmt;

use zeroize::Zeroizing;

/// A string-like type containing sensitive information such as passwords or tokens.
///
/// It is redacted from debug logs and automatically zeroized.
///
/// # Example
/// ```
/// use iceberg::sensitive::SensitiveString;
///
/// let sensitive_value = "my-pw-12345";
/// let sensitive_string = SensitiveString::from(sensitive_value.to_string());
///
/// // Not contained in debug logs.
/// assert!(!format!("{:?}", sensitive_string).contains(sensitive_value));
/// ```
///
/// # Display
/// [`SensitiveString`] does **not** implement [`Display`] to prevent bugs like:
///
/// ```compile_fail
/// # use iceberg::sensitive::SensitiveString;
/// // We don't want to send a redacted `Bearer: *****`.
/// let auth_header = format!("Bearer {}", SensitiveString::from("token".to_string()));
/// ```
///
/// Instead use an explicit [`SensitiveString::expose`] when you need it:
///
/// ```
/// # use iceberg::sensitive::SensitiveString;
/// let auth_header = format!(
///     "Bearer: {}",
///     SensitiveString::from("token".to_string()).expose()
/// );
/// ```
#[derive(Clone, PartialEq, Eq)]
pub struct SensitiveString(Zeroizing<String>);

impl SensitiveString {
    /// Returns the raw value of the sensitive string.
    pub fn expose(&self) -> &str {
        &self.0
    }

    /// Returns `true` if the string value is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for SensitiveString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SensitiveString([REDACTED])")
    }
}

impl From<String> for SensitiveString {
    fn from(value: String) -> Self {
        Self(Zeroizing::new(value))
    }
}

/// Wrapper for sensitive byte data (encryption keys, DEKs, etc.) that:
/// - Zeroizes memory on drop
/// - Redacts content in [`Debug`] and [`Display`] output
/// - Provides only `&[u8]` access via [`as_bytes()`](Self::as_bytes)
/// - Uses `Box<[u8]>` (immutable boxed slice) since key bytes never grow
///
/// Use this type for any struct field that holds plaintext key material.
/// Because its [`Debug`] impl always prints `[N bytes REDACTED]`, structs
/// containing `SensitiveBytes` can safely derive or implement `Debug`
/// without risk of leaking key material.
#[derive(Clone, PartialEq, Eq)]
pub struct SensitiveBytes(Zeroizing<Box<[u8]>>);

impl SensitiveBytes {
    /// Wraps the given bytes as sensitive material.
    pub fn new(bytes: impl Into<Box<[u8]>>) -> Self {
        Self(Zeroizing::new(bytes.into()))
    }

    /// Returns the underlying bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Returns the number of bytes.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if the byte slice is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for SensitiveBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{} bytes REDACTED]", self.0.len())
    }
}

impl fmt::Display for SensitiveBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{} bytes REDACTED]", self.0.len())
    }
}

#[cfg(test)]
mod tests {
    use crate::sensitive::{SensitiveBytes, SensitiveString};

    #[test]
    fn test_sensitive_string_redacts_debug_value() {
        let sensitive_value = "my-pw-12346";

        let logged = format!("{:?}", SensitiveString::from(sensitive_value.to_string()));
        assert!(!logged.contains(sensitive_value));
    }

    #[test]
    fn test_sensitive_bytes_redacts_debug_value() {
        let sensitive_value = b"my-secret-bytes";

        let logged = format!("{:?}", SensitiveBytes::new(&sensitive_value[..]));
        assert!(
            !logged
                .as_bytes()
                .windows(sensitive_value.len())
                .any(|window| window == sensitive_value)
        );
    }

    #[test]
    fn test_sensitive_bytes_redacts_display_value() {
        let sensitive_value = b"my-secret-bytes";

        let logged = format!("{}", SensitiveBytes::new(&sensitive_value[..]));
        assert!(
            !logged
                .as_bytes()
                .windows(sensitive_value.len())
                .any(|window| window == sensitive_value)
        );
    }
}
