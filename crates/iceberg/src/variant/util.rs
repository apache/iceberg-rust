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

//! Bounds-checked little-endian readers over untrusted variant bytes — the Rust counterpart of
//! Java's `VariantUtil` read helpers. Java relies on `ByteBuffer`'s own range checks (unchecked
//! exceptions); here every read returns [`Result`] so malformed input can never panic.

use std::cmp::Ordering;

use crate::{Error, ErrorKind, Result};

/// Builds the [`ErrorKind::DataInvalid`] error every malformed-input path in this module uses.
pub(super) fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::DataInvalid, message)
}

/// Returns the `length` bytes at `offset`, or a named [`ErrorKind::DataInvalid`] error if the
/// range escapes `bytes` (the checked counterpart of Java `VariantUtil.slice`).
pub(super) fn slice(bytes: &[u8], offset: usize, length: usize) -> Result<&[u8]> {
    let end = offset.checked_add(length).ok_or_else(|| {
        invalid(format!(
            "Invalid variant: byte range {offset}+{length} overflows"
        ))
    })?;
    bytes.get(offset..end).ok_or_else(|| {
        invalid(format!(
            "Invalid variant: byte range {offset}..{end} is out of bounds for {} bytes",
            bytes.len()
        ))
    })
}

/// Reads the unsigned byte at `offset` (Java `VariantUtil.readByte`).
pub(super) fn read_u8(bytes: &[u8], offset: usize) -> Result<u8> {
    bytes.get(offset).copied().ok_or_else(|| {
        invalid(format!(
            "Invalid variant: cannot read byte at offset {offset}, only {} bytes",
            bytes.len()
        ))
    })
}

/// Reads a fixed-size little-endian array at `offset` (the shared bounds check for the typed
/// readers below).
fn read_array<const N: usize>(bytes: &[u8], offset: usize) -> Result<[u8; N]> {
    Ok(slice(bytes, offset, N)?
        .try_into()
        .expect("slice() returned exactly N bytes"))
}

/// Reads the signed byte at `offset` (Java `VariantUtil.readLittleEndianInt8`).
pub(super) fn read_i8(bytes: &[u8], offset: usize) -> Result<i8> {
    Ok(read_u8(bytes, offset)? as i8)
}

/// Reads a little-endian `i16` (Java `VariantUtil.readLittleEndianInt16`).
pub(super) fn read_i16_le(bytes: &[u8], offset: usize) -> Result<i16> {
    Ok(i16::from_le_bytes(read_array(bytes, offset)?))
}

/// Reads a little-endian `i32` (Java `VariantUtil.readLittleEndianInt32`).
pub(super) fn read_i32_le(bytes: &[u8], offset: usize) -> Result<i32> {
    Ok(i32::from_le_bytes(read_array(bytes, offset)?))
}

/// Reads a little-endian `i64` (Java `VariantUtil.readLittleEndianInt64`).
pub(super) fn read_i64_le(bytes: &[u8], offset: usize) -> Result<i64> {
    Ok(i64::from_le_bytes(read_array(bytes, offset)?))
}

/// Reads a little-endian `i128` — the 16-byte two's-complement decimal16 unscaled value. Java
/// (`SerializedPrimitive.read`, DECIMAL16 case) reverses the 16 little-endian payload bytes
/// into a big-endian `BigInteger`; `i128::from_le_bytes` is the identical value.
pub(super) fn read_i128_le(bytes: &[u8], offset: usize) -> Result<i128> {
    Ok(i128::from_le_bytes(read_array(bytes, offset)?))
}

/// Reads a little-endian `f32` (Java `VariantUtil.readFloat`).
pub(super) fn read_f32_le(bytes: &[u8], offset: usize) -> Result<f32> {
    Ok(f32::from_le_bytes(read_array(bytes, offset)?))
}

/// Reads a little-endian `f64` (Java `VariantUtil.readDouble`).
pub(super) fn read_f64_le(bytes: &[u8], offset: usize) -> Result<f64> {
    Ok(f64::from_le_bytes(read_array(bytes, offset)?))
}

/// Reads a `size`-byte (1..=4) little-endian unsigned integer (Java
/// `VariantUtil.readLittleEndianUnsigned`).
///
/// For `size == 4` Java reads a SIGNED `int`, so a value with the high bit set turns negative
/// and fails every downstream Java use (array allocation, slice bounds); values above
/// `i32::MAX` are rejected here to mirror that signed-int domain instead of silently accepting
/// lengths Java cannot represent.
pub(super) fn read_le_unsigned(bytes: &[u8], offset: usize, size: usize) -> Result<u32> {
    debug_assert!((1..=4).contains(&size), "header math yields sizes 1..=4");
    let raw = slice(bytes, offset, size)?;
    let mut padded = [0u8; 4];
    padded[..size].copy_from_slice(raw);
    let value = u32::from_le_bytes(padded);
    if size == 4 && value > i32::MAX as u32 {
        return Err(invalid(format!(
            "Invalid variant: unsigned field {value} exceeds Java's signed 32-bit domain"
        )));
    }
    Ok(value)
}

/// Compares two strings in Java `String.compareTo` order — UTF-16 code units, which differs
/// from Rust's byte-wise `str` ordering for code points beyond the basic multilingual plane
/// (a supplementary character sorts BELOW U+E000..=U+FFFF in UTF-16, above in UTF-8). The
/// sorted-dictionary and object-field binary searches must use Java's order or lookups diverge.
pub(super) fn java_string_compare(left: &str, right: &str) -> Ordering {
    left.encode_utf16().cmp(right.encode_utf16())
}

/// Binary search returning the index whose `resolve(index)` equals `key` under
/// [`java_string_compare`], or `None` — the port of Java `VariantUtil.find`, reproducing its
/// EXACT probe sequence (inclusive `high`, `mid = (low + high) >>> 1`): on data that is NOT
/// actually sorted the probe path determines which entries can be found at all, so a
/// "merely equivalent" binary search (or a linear fallback) would hit where Java misses.
pub(super) fn find<'a>(
    size: usize,
    key: &str,
    mut resolve: impl FnMut(usize) -> &'a str,
) -> Option<usize> {
    let mut low: isize = 0;
    // `size` derives from a parsed element count bounded by i32::MAX, so the cast is lossless.
    let mut high: isize = size as isize - 1;
    while low <= high {
        let mid = (low + high) / 2;
        match java_string_compare(key, resolve(mid as usize)) {
            Ordering::Equal => return Some(mid as usize),
            Ordering::Less => high = mid - 1,
            Ordering::Greater => low = mid + 1,
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Risk pinned: a byte-order comparator would sort supplementary characters ABOVE
    /// U+FFFF where Java's UTF-16 comparator sorts them BELOW — flipping binary-search
    /// hits to misses for names beyond the BMP.
    #[test]
    fn test_java_string_compare_orders_supplementary_below_bmp_max() {
        // U+10000 encodes as the surrogate pair D800 DC00; 0xD800 < 0xFFFF, so Java says
        // "\u{10000}" < "\u{FFFF}". Rust's byte order says the opposite (F0... > EF...).
        assert_eq!(java_string_compare("\u{10000}", "\u{FFFF}"), Ordering::Less);
        assert_eq!(("\u{10000}").cmp("\u{FFFF}"), Ordering::Greater);
        // ASCII ordering is unaffected.
        assert_eq!(java_string_compare("a", "b"), Ordering::Less);
        assert_eq!(java_string_compare("b", "b"), Ordering::Equal);
    }

    /// Risk pinned: Java's size-4 read goes through a SIGNED int — a length/count/offset of
    /// 2^31 is unrepresentable there and must be rejected here, not silently accepted on
    /// 64-bit Rust.
    #[test]
    fn test_read_le_unsigned_size_4_rejects_values_beyond_java_int() {
        let bytes = 0x8000_0000u32.to_le_bytes();
        assert!(read_le_unsigned(&bytes, 0, 4).is_err());
        let max_ok = 0x7FFF_FFFFu32.to_le_bytes();
        assert_eq!(
            read_le_unsigned(&max_ok, 0, 4).expect("i32::MAX is in domain"),
            i32::MAX as u32
        );
    }

    /// Risk pinned: 3-byte reads are the easy off-by-one (no native Rust type) — the third
    /// byte must land in bits 16..24 (Java: `(getShort & 0xFFFF) | ((get & 0xFF) << 16)`).
    #[test]
    fn test_read_le_unsigned_three_byte_value_uses_all_three_bytes() {
        let bytes = [0x01, 0x02, 0x03];
        assert_eq!(
            read_le_unsigned(&bytes, 0, 3).expect("3-byte read in bounds"),
            0x030201
        );
    }
}
