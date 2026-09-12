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

//! Compatibility layer for decimal operations.
//!
//! Provides rust_decimal-compatible API using fastnum's D128 internally.
//! D128 supports 38-digit precision, meeting the Iceberg spec requirement.

use fastnum::D128;
use fastnum::decimal::Context;

use crate::{Error, ErrorKind, Result};

/// Re-export D128 as the Decimal type for use throughout the crate.
pub type Decimal = D128;

/// Create a D128 from mantissa (i128) and scale (u32).
///
/// This is equivalent to rust_decimal's `Decimal::from_i128_with_scale`.
/// The value is computed as: mantissa * 10^(-scale)
///
/// For example:
/// - mantissa=12345, scale=2 => 123.45
/// - mantissa=-456, scale=3 => -0.456
pub fn decimal_from_i128_with_scale(mantissa: i128, scale: u32) -> Decimal {
    if scale == 0 {
        return D128::from_i128(mantissa).expect("i128 always fits in D128");
    }

    // Convert mantissa to string and insert decimal point at the right position
    let is_negative = mantissa < 0;
    let abs_str = mantissa.unsigned_abs().to_string();
    let scale_usize = scale as usize;

    let decimal_str = if abs_str.len() <= scale_usize {
        // Need leading zeros: e.g., mantissa=456, scale=3 => "0.456"
        // Or mantissa=5, scale=3 => "0.005"
        let zeros_needed = scale_usize - abs_str.len();
        format!(
            "{}0.{}{}",
            if is_negative { "-" } else { "" },
            "0".repeat(zeros_needed),
            abs_str
        )
    } else {
        // Insert decimal point: e.g., mantissa=12345, scale=2 => "123.45"
        let decimal_pos = abs_str.len() - scale_usize;
        format!(
            "{}{}.{}",
            if is_negative { "-" } else { "" },
            &abs_str[..decimal_pos],
            &abs_str[decimal_pos..]
        )
    };

    D128::from_str(&decimal_str, Context::default())
        .expect("constructed decimal string is always valid")
}

/// Try to create a D128 from mantissa and scale, with validation.
///
/// This is equivalent to rust_decimal's `Decimal::try_from_i128_with_scale`.
/// Currently always succeeds for 38-digit decimals.
pub fn try_decimal_from_i128_with_scale(mantissa: i128, scale: u32) -> Result<Decimal> {
    // For now, always succeeds since D128 supports full 38-digit precision
    Ok(decimal_from_i128_with_scale(mantissa, scale))
}

/// Create a D128 from i64 mantissa and scale.
///
/// This is equivalent to rust_decimal's `Decimal::new`.
#[allow(dead_code)]
pub fn decimal_new(mantissa: i64, scale: u32) -> Decimal {
    decimal_from_i128_with_scale(mantissa as i128, scale)
}

/// Parse a decimal from string with exact representation.
///
/// This is equivalent to rust_decimal's `Decimal::from_str_exact`.
pub fn decimal_from_str_exact(s: &str) -> Result<Decimal> {
    D128::from_str(s, Context::default())
        .map_err(|e| Error::new(ErrorKind::DataInvalid, format!("Can't parse decimal: {e}")))
}

/// Get the mantissa (unscaled coefficient) as i128.
///
/// This is equivalent to rust_decimal's `decimal.mantissa()`.
///
/// The mantissa is signed: negative decimals return negative mantissa.
pub fn decimal_mantissa(d: &Decimal) -> i128 {
    // digits() returns unsigned coefficient as UInt<N>
    // For Iceberg decimals (max 38 digits), this always fits in u128/i128
    let digits = d.digits();

    // Convert UInt<2> to u128 - this always succeeds for Iceberg-compliant decimals
    // since 38 digits requires ~127 bits and u128 has 128 bits
    let unsigned: u128 = digits
        .to_u128()
        .expect("Iceberg decimals (max 38 digits) always fit in u128");

    let signed = unsigned as i128;
    if d.is_sign_negative() {
        -signed
    } else {
        signed
    }
}

/// Get the decimal digit precision of a mantissa.
///
/// A mantissa of 0 has a precision of 1.
pub fn decimal_precision(mantissa: i128) -> u32 {
    mantissa
        .unsigned_abs()
        .checked_ilog10()
        .map_or(1, |x| x + 1)
}

/// Get the scale (number of digits after decimal point).
///
/// This is equivalent to rust_decimal's `decimal.scale()`.
pub fn decimal_scale(d: &Decimal) -> u32 {
    let frac = d.fractional_digits_count();
    if frac < 0 { 0 } else { frac as u32 }
}

/// Rescale a decimal to the given scale, returning the rescaled value.
///
/// This is equivalent to rust_decimal's `decimal.rescale(scale)`.
pub fn decimal_rescale(d: Decimal, scale: u32) -> Decimal {
    d.rescale(scale as i16)
}

/// Convert big-endian signed bytes to i128.
///
/// This handles variable-length byte arrays (up to 16 bytes) with sign extension.
/// Returns None if the byte array is longer than 16 bytes.
pub fn i128_from_be_bytes(bytes: &[u8]) -> Option<i128> {
    if bytes.is_empty() {
        return Some(0);
    }
    if bytes.len() > 16 {
        return None; // Too large for i128
    }

    // Check sign bit (most significant bit of first byte)
    let is_negative = bytes[0] & 0x80 != 0;

    // Pad to 16 bytes with sign extension
    let mut padded = if is_negative { [0xFF; 16] } else { [0; 16] };
    let start = 16 - bytes.len();
    padded[start..].copy_from_slice(bytes);

    Some(i128::from_be_bytes(padded))
}

/// Convert i128 to big-endian signed bytes with minimum length.
///
/// This produces the shortest two's complement representation of the value.
/// The result is suitable for Iceberg decimal binary serialization.
pub fn i128_to_be_bytes_min(value: i128) -> Vec<u8> {
    let bytes = value.to_be_bytes();

    // Find the first significant byte
    // For positive numbers, skip leading 0x00 bytes (but keep sign bit)
    // For negative numbers, skip leading 0xFF bytes (but keep sign bit)
    let is_negative = value < 0;
    let skip_byte = if is_negative { 0xFF } else { 0x00 };

    let mut start = 0;
    while start < 15 && bytes[start] == skip_byte {
        // Check if the next byte has the correct sign bit
        let next_byte = bytes[start + 1];
        let next_is_negative = (next_byte & 0x80) != 0;
        if next_is_negative == is_negative {
            start += 1;
        } else {
            break;
        }
    }

    bytes[start..].to_vec()
}

/// Encode an i128 as exactly `len` big-endian two's complement bytes, matching a
/// Parquet `FIXED_LEN_BYTE_ARRAY` column of that declared `type_length`.
///
/// Returns `None` if `value` does not fit in `len` bytes, since a truncated
/// encoding would represent a different number.
pub(crate) fn decimal_to_fixed_length_bytes_exact(value: i128, len: usize) -> Option<Vec<u8>> {
    if len == 0 || len > 16 {
        return None;
    }

    let be_bytes = value.to_be_bytes();
    let offset = 16 - len;
    let sign_byte = if value < 0 { 0xFF } else { 0x00 };

    // The bytes being dropped must be pure sign extension.
    if be_bytes[..offset].iter().any(|&b| b != sign_byte) {
        return None;
    }

    // The retained leading byte must carry the value's sign.
    if (be_bytes[offset] & 0x80 != 0) != (value < 0) {
        return None;
    }

    Some(be_bytes[offset..].to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decimal_from_i128_with_scale() {
        let d = decimal_from_i128_with_scale(12345, 2);
        assert_eq!(d.to_string(), "123.45");

        let d = decimal_from_i128_with_scale(-12345, 2);
        assert_eq!(d.to_string(), "-123.45");

        let d = decimal_from_i128_with_scale(0, 5);
        assert_eq!(d.to_string(), "0.00000");
    }

    #[test]
    fn test_decimal_new() {
        let d = decimal_new(123, 2);
        assert_eq!(d.to_string(), "1.23");

        let d = decimal_new(-456, 3);
        assert_eq!(d.to_string(), "-0.456");
    }

    #[test]
    fn test_decimal_from_str_exact() {
        let d = decimal_from_str_exact("123.45").unwrap();
        assert_eq!(d.to_string(), "123.45");

        let d = decimal_from_str_exact("-0.001").unwrap();
        assert_eq!(d.to_string(), "-0.001");

        let d = decimal_from_str_exact("99999999999999999999999999999999999999").unwrap();
        assert_eq!(d.to_string(), "99999999999999999999999999999999999999");
    }

    #[test]
    fn test_decimal_mantissa() {
        let d = decimal_from_i128_with_scale(12345, 2);
        assert_eq!(decimal_mantissa(&d), 12345);

        let d = decimal_from_i128_with_scale(-12345, 2);
        assert_eq!(decimal_mantissa(&d), -12345);
    }

    #[test]
    fn test_decimal_precision() {
        assert_eq!(decimal_precision(0), 1);
        assert_eq!(decimal_precision(5), 1);
        assert_eq!(decimal_precision(42), 2);
        assert_eq!(decimal_precision(-42), 2);
        // power-of-10 boundaries, where off-by-one bugs in log10 logic live
        assert_eq!(decimal_precision(9), 1);
        assert_eq!(decimal_precision(10), 2);
        assert_eq!(decimal_precision(99), 2);
        assert_eq!(decimal_precision(100), 3);

        // max Iceberg decimal precision
        assert_eq!(
            decimal_precision(99999999999999999999999999999999999999),
            38
        );
        assert_eq!(
            decimal_precision(-99999999999999999999999999999999999999),
            38
        );

        // i128 extremes
        assert_eq!(decimal_precision(i128::MAX), 39);
        assert_eq!(decimal_precision(i128::MIN), 39);
    }

    #[test]
    fn test_decimal_scale() {
        let d = decimal_from_i128_with_scale(12345, 2);
        assert_eq!(decimal_scale(&d), 2);

        let d = decimal_from_i128_with_scale(12345, 0);
        assert_eq!(decimal_scale(&d), 0);
    }

    #[test]
    fn test_decimal_rescale() {
        let d = decimal_from_str_exact("123.45").unwrap();
        let rescaled = decimal_rescale(d, 4);
        assert_eq!(decimal_scale(&rescaled), 4);
        assert_eq!(decimal_mantissa(&rescaled), 1234500);
    }

    #[test]
    fn test_38_digit_precision() {
        // Test that we can handle 38-digit decimals (Iceberg spec requirement)
        let max_38_digits = "99999999999999999999999999999999999999";
        let d = decimal_from_str_exact(max_38_digits).unwrap();
        assert_eq!(d.to_string(), max_38_digits);

        let min_38_digits = "-99999999999999999999999999999999999999";
        let d = decimal_from_str_exact(min_38_digits).unwrap();
        assert_eq!(d.to_string(), min_38_digits);
    }

    #[test]
    fn test_i128_from_be_bytes() {
        // Empty bytes
        assert_eq!(i128_from_be_bytes(&[]), Some(0));

        // Positive values
        assert_eq!(i128_from_be_bytes(&[0x01]), Some(1));
        assert_eq!(i128_from_be_bytes(&[0x7F]), Some(127));
        assert_eq!(i128_from_be_bytes(&[0x00, 0xFF]), Some(255));
        assert_eq!(i128_from_be_bytes(&[0x04, 0xD2]), Some(1234));

        // Negative values (sign extension)
        assert_eq!(i128_from_be_bytes(&[0xFF]), Some(-1));
        assert_eq!(i128_from_be_bytes(&[0x80]), Some(-128));
        assert_eq!(i128_from_be_bytes(&[0xFB, 0x2E]), Some(-1234));

        // Too large (> 16 bytes)
        assert_eq!(i128_from_be_bytes(&[0; 17]), None);
    }

    #[test]
    fn test_i128_to_be_bytes_min() {
        // Positive values
        assert_eq!(i128_to_be_bytes_min(0), vec![0x00]);
        assert_eq!(i128_to_be_bytes_min(1), vec![0x01]);
        assert_eq!(i128_to_be_bytes_min(127), vec![0x7F]);
        assert_eq!(i128_to_be_bytes_min(128), vec![0x00, 0x80]);
        assert_eq!(i128_to_be_bytes_min(255), vec![0x00, 0xFF]);
        assert_eq!(i128_to_be_bytes_min(1234), vec![0x04, 0xD2]);

        // Negative values
        assert_eq!(i128_to_be_bytes_min(-1), vec![0xFF]);
        assert_eq!(i128_to_be_bytes_min(-128), vec![0x80]);
        assert_eq!(i128_to_be_bytes_min(-129), vec![0xFF, 0x7F]);
        assert_eq!(i128_to_be_bytes_min(-1234), vec![0xFB, 0x2E]);

        // Round trip test
        for val in [
            0i128,
            1,
            -1,
            127,
            -128,
            255,
            -256,
            12345,
            -12345,
            i128::MAX,
            i128::MIN,
        ] {
            let bytes = i128_to_be_bytes_min(val);
            assert_eq!(
                i128_from_be_bytes(&bytes),
                Some(val),
                "Round trip failed for {val}"
            );
        }
    }

    #[test]
    fn test_decimal_to_fixed_length_bytes_exact_positive() {
        let bytes = decimal_to_fixed_length_bytes_exact(12345, 9).unwrap();
        assert_eq!(bytes.len(), 9);
        // Big-endian, zero-padded on the left: 12345 = 0x3039
        assert_eq!(bytes[7], 0x30);
        assert_eq!(bytes[8], 0x39);
        assert!(bytes[..7].iter().all(|&b| b == 0x00));
    }

    #[test]
    fn test_decimal_to_fixed_length_bytes_exact_negative() {
        let bytes = decimal_to_fixed_length_bytes_exact(-1, 9).unwrap();
        assert_eq!(bytes.len(), 9);
        assert!(bytes.iter().all(|&b| b == 0xFF));
    }

    #[test]
    fn test_decimal_to_fixed_length_bytes_exact_round_trip() {
        for value in [
            0i128,
            1,
            -1,
            12345,
            -12345,
            i64::MAX as i128,
            i64::MIN as i128,
        ] {
            let bytes = decimal_to_fixed_length_bytes_exact(value, 16).unwrap();
            assert_eq!(bytes.len(), 16);
            assert_eq!(
                i128_from_be_bytes(&bytes),
                Some(value),
                "Round trip failed for value={value}"
            );
        }
    }

    /// The length must be honoured exactly: a value needing more bytes cannot be
    /// truncated into the column's width, because the truncation would encode a
    /// different number and probe the wrong bloom filter slot.
    #[test]
    fn test_decimal_to_fixed_length_bytes_exact_rejects_overflow() {
        // 200 needs a leading zero byte to stay positive in two's complement.
        assert_eq!(decimal_to_fixed_length_bytes_exact(200, 1), None);
        assert_eq!(
            decimal_to_fixed_length_bytes_exact(200, 2),
            Some(vec![0x00, 0xC8])
        );

        assert_eq!(decimal_to_fixed_length_bytes_exact(-200, 1), None);
        assert_eq!(
            decimal_to_fixed_length_bytes_exact(-200, 2),
            Some(vec![0xFF, 0x38])
        );

        // Exactly representable boundaries.
        assert_eq!(
            decimal_to_fixed_length_bytes_exact(127, 1),
            Some(vec![0x7F])
        );
        assert_eq!(decimal_to_fixed_length_bytes_exact(128, 1), None);
        assert_eq!(
            decimal_to_fixed_length_bytes_exact(-128, 1),
            Some(vec![0x80])
        );
        assert_eq!(decimal_to_fixed_length_bytes_exact(-129, 1), None);

        assert_eq!(decimal_to_fixed_length_bytes_exact(i128::MAX, 15), None);
        assert_eq!(decimal_to_fixed_length_bytes_exact(0, 0), None);
        assert_eq!(decimal_to_fixed_length_bytes_exact(0, 17), None);
    }
}
