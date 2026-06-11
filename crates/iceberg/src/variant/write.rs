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

//! Variant value and metadata serialization — the WRITE side. A port of the Java 1.10.0
//! construction/serialization surface (all bytecode-verified against `iceberg-core-1.10.0.jar`
//! / `iceberg-api-1.10.0.jar`; the MAIN-branch sources matched 1.10.0 for the entire write
//! surface at port time):
//!
//! - `Variants.metadata(Collection)` → [`VariantMetadata::from_field_names`]: the dictionary
//!   keeps the INSERTION order (no dedup, no re-sort); the sorted flag is set only when the
//!   input is already STRICTLY ascending in Java `String.compareTo` (UTF-16 code unit) order
//!   (`last.compareTo(name) >= 0` clears it, so duplicates also clear it); the offset size is
//!   `VariantUtil.sizeOf(dataSize)` over the total UTF-8 byte length of all names.
//! - `PrimitiveWrapper.sizeInBytes`/`writeTo` → the primitive arms of
//!   [`VariantValue::size_in_bytes`]/[`VariantValue::write_to`]: a string is written as a
//!   SHORT string when its UTF-8 byte length is at most 63 (`MAX_SHORT_STRING_LENGTH`,
//!   decided at write time) and spills to the long `STRING` form above that; decimals write
//!   the raw scale byte then the little-endian two's-complement unscaled value (Java reverses
//!   `BigInteger.toByteArray()` and sign-pads to 16 bytes for decimal16 — identical to
//!   `i128::to_le_bytes`).
//! - `ValueArray` → [`VariantArray::push`](crate::variant::VariantArray::push) + the array
//!   arm here: `isLarge = numElements > 0xFF` (4-byte count, header bit 4),
//!   `offsetSize = sizeOf(dataSize)`.
//! - `ShreddedObject` (its plain object-writing core; see the module doc for what is left
//!   out) → [`VariantObjectBuilder`] + the object arm here: fields are written sorted by name
//!   in Java `String.compareTo` (UTF-16) order; `fieldIdSize = sizeOf(metadata.
//!   dictionarySize())` — the dictionary SIZE, not the largest id used, so a 256-entry
//!   dictionary forces 2-byte field ids even though the largest id (255) would fit one byte;
//!   `isLarge = numElements > 0xFF` (4-byte count, header bit 6); field ids are re-resolved
//!   from the metadata BY NAME at write time exactly like Java's
//!   `ShreddedObject.SerializationState.writeTo` (`metadata.id(field)` +
//!   `checkState(id >= 0, "Invalid metadata, missing: %s")`).
//! - `Variants.of(...)` → the `VariantValue::of_*` constructors, including the
//!   `of(BigDecimal)` precision rule: decimal4 for precision 1..=9, decimal8 for 10..=18,
//!   decimal16 for <=38, `UnsupportedOperationException` ("Unsupported decimal precision:
//!   %s") above that. The Rust decimal input form is `(unscaled: i128, scale: u8)` — the
//!   unscaled two's-complement integer and the number of fractional digits, matching how
//!   [`VariantPrimitive`] (and `spec::values::PrimitiveLiteral::Int128`) represent decimals
//!   where Java uses `BigDecimal`; precision is the decimal digit count of `|unscaled|`
//!   (`BigDecimal.precision()`, which reports 1 for zero).
//! - `VariantMetadata.writeTo` / `Variant` emission → [`VariantMetadata::to_bytes`] and
//!   [`Variant::to_bytes`] (metadata bytes immediately followed by value bytes — the layout
//!   [`Variant::from_bytes`] parses).
//!
//! # Differences from Java (write side)
//!
//! - **Re-serializing a PARSED value canonicalizes.** Java's `SerializedValue.writeTo` /
//!   `SerializedMetadata.writeTo` copy the original backing buffer verbatim (non-minimal
//!   offset widths and all); the eager representation has no backing buffer, so this writer
//!   re-encodes with the canonical Java-WRITER widths (`sizeOf`-minimal) and, for objects,
//!   re-resolves field ids by name through the writer's path. For anything Java's own writer
//!   produced the output is byte-identical (pinned against 1.10.0 fixtures); only
//!   non-canonical third-party input re-encodes differently (same decoded value).
//! - **Width overflow is an error, not a silent mask.** `VariantUtil.
//!   writeLittleEndianUnsigned` masks the value to the requested width, so Java silently
//!   corrupts the one reachable pathology — a dictionary of >255 names whose total UTF-8
//!   data size still fits one byte (only possible with empty/duplicated empty names):
//!   1.10.0 truncates the count byte and emits a metadata that has LOST every name
//!   (probe-verified: 256 empty names serialize to `01 00 00`). This port rejects it loudly
//!   ([`DataInvalid`](crate::ErrorKind::DataInvalid)).
//! - **Java's `int` domain is enforced.** Every size, length, and count is doored at
//!   `i32::MAX` (Java buffers and `sizeInBytes()` are `int`-addressed), so a value Java
//!   could never serialize is rejected instead of silently emitted.
//! - **Serialization recursion is depth-guarded** by [`MAX_NESTING_DEPTH`], mirroring the
//!   parse side: values deeper than the parser would accept cannot be constructed by parsing,
//!   but they CAN be constructed manually (each nesting level is an explicit `push`/`put`
//!   call), and an unbounded write recursion over such a value would overflow the stack.
//!   Java has no limit (its writer recursion equals the constructed depth).
//! - **`ShreddedObject`'s shredding overlay is NOT ported** (the `unshredded` backing object,
//!   buffer-slice reuse via `SerializedObject.sliceValue`, `remove()`/`removedFields`, and
//!   `Variants.object(metadata, object)` re-wrapping). [`VariantObjectBuilder`] is the plain
//!   writer (`Variants.object(metadata)` + `put`) only; shredding is deferred with the rest
//!   of the shredding surface (see `docs/parity/GAP_MATRIX.md`).
//! - **ISO-string convenience factories are not ported** (`Variants.ofIsoDate` /
//!   `ofIsoTimestamptz` / ... are `DateTimeUtil` parsing conveniences, not format surface);
//!   the numeric `of_date`/`of_timestamptz`/... constructors are the byte-format-bearing
//!   equivalents.

use std::collections::HashMap;

use crate::variant::metadata::{
    HEADER_SIZE as METADATA_HEADER_SIZE, OFFSET_SIZE_SHIFT as METADATA_OFFSET_SIZE_SHIFT,
    SORTED_STRINGS, SUPPORTED_VERSION, VariantMetadata,
};
use crate::variant::types::PhysicalType;
use crate::variant::util;
use crate::variant::value::{
    ARRAY_IS_LARGE, FIELD_ID_SIZE_SHIFT, HEADER_SIZE, MAX_NESTING_DEPTH, OBJECT_IS_LARGE,
    OFFSET_SIZE_SHIFT, PRIMITIVE_TYPE_SHIFT, Variant, VariantArray, VariantObject,
    VariantObjectField, VariantPrimitive, VariantValue,
};
use crate::{Error, ErrorKind, Result};

/// The longest string written in the SHORT-STRING form, in UTF-8 bytes (Java
/// `PrimitiveWrapper.MAX_SHORT_STRING_LENGTH` = 63; the spill decision is made at write
/// time from `buffer.remaining()`, i.e. the UTF-8 byte length, not the char count).
const MAX_SHORT_STRING_LENGTH: usize = 63;

/// Java's `int` ceiling: every serialized size, length, count, and offset must fit a signed
/// 32-bit integer (Java buffers and `sizeInBytes()` are `int`-addressed). Values beyond it
/// are unrepresentable in Java and are rejected here by name.
const JAVA_INT_MAX: usize = i32::MAX as usize;

/// The basic-type tag of an object header (Java `VariantUtil.BASIC_TYPE_OBJECT`, the low two
/// bits of `objectHeader`).
const BASIC_TYPE_OBJECT: u8 = 0b10;
/// The basic-type tag of an array header (Java `VariantUtil.BASIC_TYPE_ARRAY`).
const BASIC_TYPE_ARRAY: u8 = 0b11;
/// The basic-type tag of a short-string header (Java `VariantUtil.BASIC_TYPE_SHORT_STRING`).
const BASIC_TYPE_SHORT_STRING: u8 = 0b01;

/// Width selection — the number of bytes needed to store `max_value` unsigned: the exact
/// Java `VariantUtil.sizeOf` thresholds (1 for <= 0xFF, 2 for <= 0xFFFF, 3 for <= 0xFFFFFF,
/// else 4). Callers door their inputs at [`JAVA_INT_MAX`] first.
fn size_of_unsigned(max_value: usize) -> usize {
    if max_value <= 0xFF {
        1
    } else if max_value <= 0xFFFF {
        2
    } else if max_value <= 0xFF_FFFF {
        3
    } else {
        4
    }
}

/// Writes the byte at `offset`, bounds-checked (Java `VariantUtil.writeByte` relies on
/// `ByteBuffer`'s unchecked exception).
fn write_u8(buffer: &mut [u8], offset: usize, value: u8) -> Result<()> {
    let buffer_len = buffer.len();
    let slot = buffer.get_mut(offset).ok_or_else(|| {
        util::invalid(format!(
            "Invalid variant write: offset {offset} is out of bounds for {buffer_len} bytes"
        ))
    })?;
    *slot = value;
    Ok(())
}

/// Copies `data` to `offset`, bounds-checked (Java `VariantUtil.writeBufferAbsolute`).
fn write_bytes(buffer: &mut [u8], offset: usize, data: &[u8]) -> Result<()> {
    let end = offset.checked_add(data.len()).ok_or_else(|| {
        util::invalid(format!(
            "Invalid variant write: byte range {offset}+{} overflows",
            data.len()
        ))
    })?;
    let buffer_len = buffer.len();
    let slot = buffer.get_mut(offset..end).ok_or_else(|| {
        util::invalid(format!(
            "Invalid variant write: byte range {offset}..{end} is out of bounds for \
             {buffer_len} bytes"
        ))
    })?;
    slot.copy_from_slice(data);
    Ok(())
}

/// Writes a `size`-byte (1..=4) little-endian unsigned integer (Java
/// `VariantUtil.writeLittleEndianUnsigned`).
///
/// Java MASKS the value to the requested width (`(byte) (value & 0xFF)` etc.), silently
/// corrupting anything too large; here an oversized value is a named error (see the module
/// doc — by construction every internal caller selects the width with [`size_of_unsigned`],
/// so this door only fires on the count-wider-than-data pathology).
fn write_le_unsigned(buffer: &mut [u8], value: usize, offset: usize, size: usize) -> Result<()> {
    debug_assert!(
        (1..=4).contains(&size),
        "width selection yields sizes 1..=4"
    );
    let value = value as u64;
    if size < 8 && (value >> (8 * size as u32)) != 0 {
        return Err(util::invalid(format!(
            "Invalid variant write: value {value} does not fit {size} byte(s) \
             (Java would silently truncate it)"
        )));
    }
    write_bytes(buffer, offset, &value.to_le_bytes()[..size])
}

/// Doors a container write up front: `offset + total_size` must fit the buffer (checked
/// math, so a hostile offset cannot wrap). After this door every interior offset is bounded
/// by `buffer.len() <= isize::MAX` and plain offset arithmetic cannot overflow.
fn door_value_span(buffer: &[u8], offset: usize, total_size: usize, what: &str) -> Result<()> {
    let fits = offset
        .checked_add(total_size)
        .is_some_and(|end| end <= buffer.len());
    if !fits {
        return Err(util::invalid(format!(
            "Invalid variant write: {what} needs {total_size} bytes at offset {offset}, \
             but the buffer has {} bytes",
            buffer.len()
        )));
    }
    Ok(())
}

/// Converts a width (1..=4) to its header bit field value, `width - 1` (shared by the
/// metadata, object, and array header builders).
fn width_bits(width: usize) -> u8 {
    debug_assert!(
        (1..=4).contains(&width),
        "width selection yields sizes 1..=4"
    );
    u8::try_from(width.saturating_sub(1) & 0b11).expect("a 2-bit value fits a byte")
}

/// Builds a metadata header byte (Java `VariantUtil.metadataHeader`:
/// `((offsetSize - 1) << 6) | (isSorted ? 0b10000 : 0) | 0b0001`).
fn metadata_header(is_sorted: bool, offset_size: usize) -> u8 {
    (width_bits(offset_size) << METADATA_OFFSET_SIZE_SHIFT)
        | (if is_sorted { SORTED_STRINGS } else { 0 })
        | SUPPORTED_VERSION
}

/// Builds an object header byte (Java `VariantUtil.objectHeader`:
/// `(isLarge ? 0b1000000 : 0) | ((fieldIdSize - 1) << 4) | ((offsetSize - 1) << 2) | 0b10`).
fn object_header(is_large: bool, field_id_size: usize, offset_size: usize) -> u8 {
    (if is_large { OBJECT_IS_LARGE } else { 0 })
        | (width_bits(field_id_size) << FIELD_ID_SIZE_SHIFT)
        | (width_bits(offset_size) << OFFSET_SIZE_SHIFT)
        | BASIC_TYPE_OBJECT
}

/// Builds an array header byte (Java `VariantUtil.arrayHeader`:
/// `(isLarge ? 0b10000 : 0) | (offsetSize - 1) << 2 | 0b11`).
fn array_header(is_large: bool, offset_size: usize) -> u8 {
    (if is_large { ARRAY_IS_LARGE } else { 0 })
        | (width_bits(offset_size) << OFFSET_SIZE_SHIFT)
        | BASIC_TYPE_ARRAY
}

/// Builds a primitive header byte (Java `VariantUtil.primitiveHeader`:
/// `primitiveType << 2`; the low basic-type bits are 0b00 = PRIMITIVE).
fn primitive_header(physical_type: PhysicalType) -> Result<u8> {
    let type_info = physical_type.to_type_info().ok_or_else(|| {
        util::invalid(format!(
            "Invalid variant write: {physical_type:?} has no primitive type id"
        ))
    })?;
    Ok(type_info << PRIMITIVE_TYPE_SHIFT)
}

/// Builds a short-string header byte (Java `VariantUtil.shortStringHeader`:
/// `(length << 2) | BASIC_TYPE_SHORT_STRING`); the caller guarantees `length <= 63`.
fn short_string_header(length: usize) -> u8 {
    (u8::try_from(length).expect("short-string lengths are at most 63") << PRIMITIVE_TYPE_SHIFT)
        | BASIC_TYPE_SHORT_STRING
}

/// The computed serialized layout of a metadata dictionary (`Variants.metadata`'s size math).
struct MetadataLayout {
    data_size: usize,
    offset_size: usize,
    total_size: usize,
}

/// Computes the serialized layout for a dictionary, mirroring `Variants.metadata` (1.10.0):
/// `dataSize` = total UTF-8 bytes of all names, `offsetSize = VariantUtil.sizeOf(dataSize)`,
/// `totalSize = 1 + offsetSize + (1 + numElements) * offsetSize + dataSize`.
///
/// # Errors
///
/// [`crate::ErrorKind::DataInvalid`] when a size escapes Java's `int` domain, or when the
/// name COUNT does not fit `offsetSize` — the pathology Java silently truncates (module doc).
fn metadata_layout(names: &[String]) -> Result<MetadataLayout> {
    let mut data_size = 0usize;
    for name in names {
        data_size = data_size
            .checked_add(name.len())
            .filter(|size| *size <= JAVA_INT_MAX)
            .ok_or_else(|| {
                util::invalid(format!(
                    "Invalid variant metadata: total dictionary string data exceeds {JAVA_INT_MAX} bytes"
                ))
            })?;
    }
    let offset_size = size_of_unsigned(data_size);
    // Java writes the dictionary size with offsetSize bytes and MASKS it; reachable only
    // with empty names (any non-empty names make dataSize >= numElements).
    if (names.len() as u64) >> (8 * offset_size as u32) != 0 {
        return Err(util::invalid(format!(
            "Invalid variant metadata: {} dictionary entries do not fit the {offset_size}-byte \
             offset size selected for {data_size} data byte(s) (Java 1.10.0 silently truncates \
             this dictionary; refusing to write corrupt metadata)",
            names.len()
        )));
    }
    let offsets_len = names
        .len()
        .checked_add(1)
        .and_then(|count| count.checked_mul(offset_size))
        .ok_or_else(|| util::invalid("Invalid variant metadata: offset list size overflows"))?;
    let total_size = METADATA_HEADER_SIZE
        .checked_add(offset_size)
        .and_then(|size| size.checked_add(offsets_len))
        .and_then(|size| size.checked_add(data_size))
        .filter(|size| *size <= JAVA_INT_MAX)
        .ok_or_else(|| {
            util::invalid(format!(
                "Invalid variant metadata: serialized size exceeds {JAVA_INT_MAX} bytes"
            ))
        })?;
    Ok(MetadataLayout {
        data_size,
        offset_size,
        total_size,
    })
}

// ===== metadata building and serialization ==================================================
// The write-side surface of `VariantMetadata` (`Variants.metadata(Collection)` and
// `VariantMetadata.writeTo`, 1.10.0).

impl VariantMetadata {
    /// Builds metadata from field names exactly as Java `Variants.metadata(Collection)`
    /// (1.10.0, bytecode-verified): the dictionary keeps the INSERTION order (no dedup, no
    /// re-sort), and the sorted flag is set only when the input is already STRICTLY ascending
    /// in Java `String.compareTo` (UTF-16 code unit) order — `last.compareTo(name) >= 0`
    /// clears it, so duplicate names also clear it. An empty input is the empty-v1 metadata
    /// (`01 00 00`, sorted flag NOT set — Java's `EMPTY_V1_METADATA`).
    ///
    /// # Errors
    ///
    /// [`crate::ErrorKind::DataInvalid`] when the dictionary cannot be serialized in Java's
    /// `int` domain, or on the count-truncation pathology (see [`Self::to_bytes`]'s door and
    /// the module doc).
    pub fn from_field_names<I, S>(field_names: I) -> Result<VariantMetadata>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let dictionary: Vec<String> = field_names.into_iter().map(Into::into).collect();
        if dictionary.is_empty() {
            // Variants.metadata returns EMPTY_V1_METADATA (buffer `01 00 00`): the sorted
            // flag is NOT set on the empty dictionary.
            return Ok(VariantMetadata::from_parts(false, dictionary, 3));
        }
        let is_sorted = dictionary
            .windows(2)
            .all(|pair| util::java_string_compare(&pair[0], &pair[1]) == std::cmp::Ordering::Less);
        let layout = metadata_layout(&dictionary)?;
        Ok(VariantMetadata::from_parts(
            is_sorted,
            dictionary,
            layout.total_size,
        ))
    }

    /// Serializes this metadata to the on-disk layout (header byte, dictionary size,
    /// `dictionarySize + 1` offsets, concatenated UTF-8 strings — `Variants.metadata`'s
    /// buffer construction, 1.10.0).
    ///
    /// For metadata built by [`Self::from_field_names`] (and for anything Java's own writer
    /// produced) the output is byte-identical to Java's. A PARSED metadata re-encodes with
    /// the canonical writer widths — Java's `SerializedMetadata.writeTo` copies its original
    /// buffer verbatim instead, so a non-canonical third-party encoding (e.g. an oversized
    /// offset width) canonicalizes here; the sorted flag is preserved as parsed. In that case
    /// the output length can differ from [`Self::size_in_bytes`] (which reports the PARSED
    /// size).
    ///
    /// # Errors
    ///
    /// [`crate::ErrorKind::DataInvalid`] per [`Self::from_field_names`]'s doors.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let names = self.dictionary();
        let layout = metadata_layout(names)?;
        let offset_size = layout.offset_size;
        let offset_list_offset = METADATA_HEADER_SIZE + offset_size;
        let data_offset = offset_list_offset + (1 + names.len()) * offset_size;

        let mut buffer = vec![0u8; layout.total_size];
        write_u8(
            &mut buffer,
            0,
            metadata_header(self.is_sorted(), offset_size),
        )?;
        write_le_unsigned(&mut buffer, names.len(), METADATA_HEADER_SIZE, offset_size)?;
        let mut next_offset = 0usize;
        for (index, name) in names.iter().enumerate() {
            write_le_unsigned(
                &mut buffer,
                next_offset,
                offset_list_offset + index * offset_size,
                offset_size,
            )?;
            write_bytes(&mut buffer, data_offset + next_offset, name.as_bytes())?;
            next_offset += name.len();
        }
        // The final offset entry is the total string-data length.
        write_le_unsigned(
            &mut buffer,
            next_offset,
            offset_list_offset + names.len() * offset_size,
            offset_size,
        )?;
        debug_assert_eq!(next_offset, layout.data_size);
        Ok(buffer)
    }
}

// ===== `Variants.of(...)` factory constructors ==============================================
// The write-side construction surface of `VariantValue`.

impl VariantValue {
    /// A JSON-style null (Java `Variants.ofNull()`).
    pub fn of_null() -> VariantValue {
        VariantValue::Primitive(VariantPrimitive::Null)
    }

    /// A boolean (Java `Variants.of(boolean)`; the physical type — `BOOLEAN_TRUE` or
    /// `BOOLEAN_FALSE` — is derived from the value, as `PrimitiveWrapper`'s constructor
    /// normalizes it).
    pub fn of_boolean(value: bool) -> VariantValue {
        VariantValue::Primitive(VariantPrimitive::Boolean(value))
    }

    /// An 8-bit integer (Java `Variants.of(byte)`).
    pub fn of_int8(value: i8) -> VariantValue {
        VariantValue::Primitive(VariantPrimitive::Int8(value))
    }

    /// A 16-bit integer (Java `Variants.of(short)`).
    pub fn of_int16(value: i16) -> VariantValue {
        VariantValue::Primitive(VariantPrimitive::Int16(value))
    }

    /// A 32-bit integer (Java `Variants.of(int)`).
    pub fn of_int32(value: i32) -> VariantValue {
        VariantValue::Primitive(VariantPrimitive::Int32(value))
    }

    /// A 64-bit integer (Java `Variants.of(long)`).
    pub fn of_int64(value: i64) -> VariantValue {
        VariantValue::Primitive(VariantPrimitive::Int64(value))
    }

    /// A single-precision float (Java `Variants.of(float)`).
    pub fn of_float(value: f32) -> VariantValue {
        VariantValue::Primitive(VariantPrimitive::Float(value))
    }

    /// A double-precision float (Java `Variants.of(double)`).
    pub fn of_double(value: f64) -> VariantValue {
        VariantValue::Primitive(VariantPrimitive::Double(value))
    }

    /// A date as days from the unix epoch (Java `Variants.ofDate(int)`).
    pub fn of_date(days_from_epoch: i32) -> VariantValue {
        VariantValue::Primitive(VariantPrimitive::Date(days_from_epoch))
    }

    /// A UTC-adjusted timestamp in microseconds from the unix epoch (Java
    /// `Variants.ofTimestamptz(long)`).
    pub fn of_timestamptz(micros_from_epoch: i64) -> VariantValue {
        VariantValue::Primitive(VariantPrimitive::Timestamptz(micros_from_epoch))
    }

    /// A local (zone-less) timestamp in microseconds from the unix epoch (Java
    /// `Variants.ofTimestampntz(long)`).
    pub fn of_timestampntz(micros_from_epoch: i64) -> VariantValue {
        VariantValue::Primitive(VariantPrimitive::Timestampntz(micros_from_epoch))
    }

    /// A time of day in microseconds from midnight (Java `Variants.ofTime(long)`).
    pub fn of_time(micros_from_midnight: i64) -> VariantValue {
        VariantValue::Primitive(VariantPrimitive::Time(micros_from_midnight))
    }

    /// A UTC-adjusted timestamp in nanoseconds from the unix epoch (Java
    /// `Variants.ofTimestamptzNanos(long)`).
    pub fn of_timestamptz_nanos(nanos_from_epoch: i64) -> VariantValue {
        VariantValue::Primitive(VariantPrimitive::TimestamptzNanos(nanos_from_epoch))
    }

    /// A local timestamp in nanoseconds from the unix epoch (Java
    /// `Variants.ofTimestampntzNanos(long)`).
    pub fn of_timestampntz_nanos(nanos_from_epoch: i64) -> VariantValue {
        VariantValue::Primitive(VariantPrimitive::TimestampntzNanos(nanos_from_epoch))
    }

    /// An opaque byte sequence (Java `Variants.of(ByteBuffer)`).
    pub fn of_binary(data: Vec<u8>) -> VariantValue {
        VariantValue::Primitive(VariantPrimitive::Binary(data))
    }

    /// A UTF-8 string (Java `Variants.of(String)`); whether it serializes as a SHORT string
    /// or the long `STRING` form is decided at write time by its UTF-8 byte length, exactly
    /// like Java (`PrimitiveWrapper.writeTo`, `MAX_SHORT_STRING_LENGTH` = 63).
    pub fn of_string(value: impl Into<String>) -> VariantValue {
        VariantValue::Primitive(VariantPrimitive::String(value.into()))
    }

    /// A UUID from its 16 big-endian (RFC 4122) bytes (Java `Variants.ofUUID`, which writes
    /// `UUIDUtil.convertToByteBuffer(uuid)` — the same big-endian byte order).
    pub fn of_uuid(big_endian_bytes: [u8; 16]) -> VariantValue {
        VariantValue::Primitive(VariantPrimitive::Uuid(big_endian_bytes))
    }

    /// A decimal from its two's-complement unscaled value and scale, picking the SMALLEST
    /// physical decimal type by PRECISION exactly like Java `Variants.of(BigDecimal)`
    /// (1.10.0, bytecode-verified): precision 1..=9 → decimal4, 10..=18 → decimal8,
    /// <=38 → decimal16. Precision is the decimal digit count of `|unscaled|`
    /// (`BigDecimal.precision()`; zero has precision 1).
    ///
    /// # Errors
    ///
    /// [`crate::ErrorKind::FeatureUnsupported`] for precision above 38 (Java throws
    /// `UnsupportedOperationException("Unsupported decimal precision: %s")`). A 39-digit
    /// `i128` (e.g. `i128::MIN`) is therefore not constructible here, exactly as it is not
    /// via `Variants.of(BigDecimal)`; [`VariantPrimitive::Decimal16`] can still represent it
    /// directly (mirroring Java's width-explicit `Variants.of(PhysicalType, value)`).
    pub fn of_decimal(unscaled: i128, scale: u8) -> Result<VariantValue> {
        let precision = decimal_precision(unscaled);
        let primitive = if precision <= 9 {
            VariantPrimitive::Decimal4 {
                scale,
                unscaled: i32::try_from(unscaled)
                    .expect("a value of at most 9 decimal digits fits i32"),
            }
        } else if precision <= 18 {
            VariantPrimitive::Decimal8 {
                scale,
                unscaled: i64::try_from(unscaled)
                    .expect("a value of at most 18 decimal digits fits i64"),
            }
        } else if precision <= 38 {
            VariantPrimitive::Decimal16 { scale, unscaled }
        } else {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!("Unsupported decimal precision: {precision}"),
            ));
        };
        Ok(VariantValue::Primitive(primitive))
    }
}

/// Returns the decimal digit count of `|unscaled|` — Java `BigDecimal.precision()` for an
/// integer unscaled value (zero reports 1).
fn decimal_precision(unscaled: i128) -> u32 {
    let mut magnitude = unscaled.unsigned_abs();
    let mut digits = 1u32;
    while magnitude >= 10 {
        magnitude /= 10;
        digits += 1;
    }
    digits
}

// ===== value serialization ==================================================================
// The write-side surface of `VariantValue` (`VariantValue.sizeInBytes()`/`writeTo`,
// implemented by `PrimitiveWrapper`/`ValueArray`/`ShreddedObject` in 1.10.0).

impl VariantValue {
    /// Returns the serialized size in bytes of this value (Java `sizeInBytes()`). Objects
    /// need the `metadata` because their field-id width is `sizeOf(metadata.
    /// dictionarySize())` (`ShreddedObject.SerializationState`, 1.10.0).
    ///
    /// # Errors
    ///
    /// [`crate::ErrorKind::DataInvalid`] when the value cannot be serialized in Java's `int`
    /// domain or exceeds [`MAX_NESTING_DEPTH`].
    pub fn size_in_bytes(&self, metadata: &VariantMetadata) -> Result<usize> {
        value_size(self, metadata, 0)
    }

    /// Writes this value into `buffer` at `offset` and returns the number of bytes written —
    /// the port of Java `VariantValue.writeTo(ByteBuffer, int)` (absolute offsets; the
    /// buffer's little-endian order is implicit here).
    ///
    /// # Errors
    ///
    /// [`crate::ErrorKind::DataInvalid`] when the buffer is too small, a field name is
    /// missing from `metadata` (Java `checkState`: "Invalid metadata, missing: %s"), the
    /// value escapes Java's `int` domain, or nesting exceeds [`MAX_NESTING_DEPTH`].
    pub fn write_to(
        &self,
        metadata: &VariantMetadata,
        buffer: &mut [u8],
        offset: usize,
    ) -> Result<usize> {
        // Door the caller-supplied offset before any offset arithmetic: slices are bounded
        // by isize::MAX, so once offset <= buffer.len() (and each container doors
        // offset + total_size against the buffer below), interior offset math cannot
        // overflow.
        if offset > buffer.len() {
            return Err(util::invalid(format!(
                "Invalid variant write: offset {offset} is out of bounds for {} bytes",
                buffer.len()
            )));
        }
        write_value(self, metadata, buffer, offset, 0)
    }

    /// Serializes this value to its on-disk bytes (an exact-size buffer filled by
    /// [`Self::write_to`] — the `ByteBuffer.allocate(sizeInBytes())` + `writeTo(buffer, 0)`
    /// pattern every Java caller uses).
    ///
    /// # Errors
    ///
    /// Any [`Self::size_in_bytes`] / [`Self::write_to`] error.
    pub fn to_bytes(&self, metadata: &VariantMetadata) -> Result<Vec<u8>> {
        let size = self.size_in_bytes(metadata)?;
        let mut buffer = vec![0u8; size];
        let written = self.write_to(metadata, &mut buffer, 0)?;
        debug_assert_eq!(written, size, "write_to must fill exactly size_in_bytes");
        Ok(buffer)
    }
}

// ===== whole-variant emission ===============================================================

impl Variant {
    /// Serializes the variant as metadata bytes immediately followed by value bytes — the
    /// single-buffer layout [`Variant::from_bytes`] (Java `Variant.from(ByteBuffer)`)
    /// parses.
    ///
    /// # Errors
    ///
    /// Any [`VariantMetadata::to_bytes`] / [`VariantValue::to_bytes`] error.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut bytes = self.metadata().to_bytes()?;
        let value_bytes = self.value().to_bytes(self.metadata())?;
        bytes.extend_from_slice(&value_bytes);
        Ok(bytes)
    }
}

/// Recursive size computation (Java: `PrimitiveWrapper.sizeInBytes` /
/// `ValueArray.SerializationState` / `ShreddedObject.SerializationState`). Depth-bounded by
/// [`MAX_NESTING_DEPTH`] — the format is genuinely recursive and the bound is checked before
/// any child walk, so stack usage is provably capped (see the module doc).
fn value_size(value: &VariantValue, metadata: &VariantMetadata, depth: usize) -> Result<usize> {
    if depth > MAX_NESTING_DEPTH {
        return Err(util::invalid(format!(
            "Invalid variant: nesting depth exceeds the supported maximum {MAX_NESTING_DEPTH}"
        )));
    }
    match value {
        VariantValue::Primitive(primitive) => primitive_size(primitive),
        VariantValue::Object(object) => {
            let layout = object_layout(object, metadata, depth)?;
            Ok(layout.total_size)
        }
        VariantValue::Array(array) => {
            let layout = array_layout(array, metadata, depth)?;
            Ok(layout.total_size)
        }
    }
}

/// Primitive serialized sizes — the exact `PrimitiveWrapper.sizeInBytes()` table (1.10.0).
fn primitive_size(primitive: &VariantPrimitive) -> Result<usize> {
    let size = match primitive {
        VariantPrimitive::Null | VariantPrimitive::Boolean(_) => 1,
        VariantPrimitive::Int8(_) => 2,
        VariantPrimitive::Int16(_) => 3,
        VariantPrimitive::Int32(_) | VariantPrimitive::Date(_) | VariantPrimitive::Float(_) => 5,
        VariantPrimitive::Int64(_)
        | VariantPrimitive::Double(_)
        | VariantPrimitive::Timestamptz(_)
        | VariantPrimitive::Timestampntz(_)
        | VariantPrimitive::TimestamptzNanos(_)
        | VariantPrimitive::TimestampntzNanos(_)
        | VariantPrimitive::Time(_) => 9,
        VariantPrimitive::Decimal4 { .. } => 6,
        VariantPrimitive::Decimal8 { .. } => 10,
        VariantPrimitive::Decimal16 { .. } => 18,
        VariantPrimitive::Uuid(_) => 17,
        VariantPrimitive::Binary(data) => length_prefixed_size(data.len(), "binary")?,
        VariantPrimitive::String(value) => {
            let utf8_length = value.len();
            if utf8_length <= MAX_SHORT_STRING_LENGTH {
                // 1 header byte, the length packed into it (the SHORT-STRING form).
                HEADER_SIZE + utf8_length
            } else {
                length_prefixed_size(utf8_length, "string")?
            }
        }
    };
    Ok(size)
}

/// Size of a length-prefixed payload (binary / long string): 1 header + 4 length + payload,
/// doored at Java's `int` domain.
fn length_prefixed_size(payload_length: usize, what: &str) -> Result<usize> {
    (HEADER_SIZE + 4)
        .checked_add(payload_length)
        .filter(|size| *size <= JAVA_INT_MAX)
        .ok_or_else(|| {
            util::invalid(format!(
                "Invalid variant {what}: serialized size exceeds {JAVA_INT_MAX} bytes"
            ))
        })
}

/// The computed serialized layout of a container value.
struct ContainerLayout {
    is_large: bool,
    offset_size: usize,
    /// Field-id width for objects; 0 for arrays (no field-id list).
    field_id_size: usize,
    data_size: usize,
    total_size: usize,
}

/// Object layout per `ShreddedObject.SerializationState` (1.10.0): `fieldIdSize =
/// sizeOf(metadata.dictionarySize())` (the dictionary SIZE, not the largest id),
/// `isLarge = numElements > 0xFF`, `offsetSize = sizeOf(dataSize)`,
/// `size = 1 + (4|1) + n*fieldIdSize + (1+n)*offsetSize + dataSize`.
fn object_layout(
    object: &VariantObject,
    metadata: &VariantMetadata,
    depth: usize,
) -> Result<ContainerLayout> {
    let num_elements = object.num_fields();
    let field_id_size = size_of_unsigned(metadata.dictionary_size());
    let mut data_size = 0usize;
    for field in object.fields() {
        data_size = checked_data_size(data_size, value_size(&field.value, metadata, depth + 1)?)?;
    }
    let is_large = num_elements > 0xFF;
    let offset_size = size_of_unsigned(data_size);
    let count_size = if is_large { 4 } else { 1 };
    let total_size = HEADER_SIZE
        .checked_add(count_size)
        .and_then(|size| size.checked_add(num_elements.checked_mul(field_id_size)?))
        .and_then(|size| size.checked_add(num_elements.checked_add(1)?.checked_mul(offset_size)?))
        .and_then(|size| size.checked_add(data_size))
        .filter(|size| *size <= JAVA_INT_MAX)
        .ok_or_else(|| {
            util::invalid(format!(
                "Invalid variant object: serialized size exceeds {JAVA_INT_MAX} bytes"
            ))
        })?;
    Ok(ContainerLayout {
        is_large,
        offset_size,
        field_id_size,
        data_size,
        total_size,
    })
}

/// Array layout per `ValueArray.SerializationState` (1.10.0): `isLarge = numElements > 0xFF`,
/// `offsetSize = sizeOf(dataSize)`, `size = 1 + (4|1) + (1+n)*offsetSize + dataSize`.
fn array_layout(
    array: &VariantArray,
    metadata: &VariantMetadata,
    depth: usize,
) -> Result<ContainerLayout> {
    let num_elements = array.num_elements();
    let mut data_size = 0usize;
    for element in array.elements() {
        data_size = checked_data_size(data_size, value_size(element, metadata, depth + 1)?)?;
    }
    let is_large = num_elements > 0xFF;
    let offset_size = size_of_unsigned(data_size);
    let count_size = if is_large { 4 } else { 1 };
    let total_size = HEADER_SIZE
        .checked_add(count_size)
        .and_then(|size| size.checked_add(num_elements.checked_add(1)?.checked_mul(offset_size)?))
        .and_then(|size| size.checked_add(data_size))
        .filter(|size| *size <= JAVA_INT_MAX)
        .ok_or_else(|| {
            util::invalid(format!(
                "Invalid variant array: serialized size exceeds {JAVA_INT_MAX} bytes"
            ))
        })?;
    Ok(ContainerLayout {
        is_large,
        offset_size,
        field_id_size: 0,
        data_size,
        total_size,
    })
}

/// Accumulates a child size into a container data size, doored at Java's `int` domain.
fn checked_data_size(accumulated: usize, child_size: usize) -> Result<usize> {
    accumulated
        .checked_add(child_size)
        .filter(|size| *size <= JAVA_INT_MAX)
        .ok_or_else(|| {
            util::invalid(format!(
                "Invalid variant: container data exceeds {JAVA_INT_MAX} bytes"
            ))
        })
}

/// Recursive write dispatch (the `writeTo` side of the three serialization states). The depth
/// bound mirrors [`value_size`].
fn write_value(
    value: &VariantValue,
    metadata: &VariantMetadata,
    buffer: &mut [u8],
    offset: usize,
    depth: usize,
) -> Result<usize> {
    if depth > MAX_NESTING_DEPTH {
        return Err(util::invalid(format!(
            "Invalid variant: nesting depth exceeds the supported maximum {MAX_NESTING_DEPTH}"
        )));
    }
    match value {
        VariantValue::Primitive(primitive) => write_primitive(primitive, buffer, offset),
        VariantValue::Object(object) => write_object(object, metadata, buffer, offset, depth),
        VariantValue::Array(array) => write_array(array, metadata, buffer, offset, depth),
    }
}

/// Writes a primitive (the exact `PrimitiveWrapper.writeTo` payload layouts, 1.10.0): header
/// byte, then the little-endian payload (decimals: raw scale byte + LE unscaled value — for
/// decimal16 Java reverses `BigInteger.toByteArray()` into little-endian and sign-pads to 16
/// bytes, which is exactly `i128::to_le_bytes`; UUID: the 16 big-endian bytes as stored;
/// strings: the short form when the UTF-8 length is at most 63, else the long form).
fn write_primitive(
    primitive: &VariantPrimitive,
    buffer: &mut [u8],
    offset: usize,
) -> Result<usize> {
    let header = match primitive {
        // The string header depends on the spill decision below.
        VariantPrimitive::String(_) => 0,
        other => primitive_header(other.physical_type())?,
    };
    let payload_offset = offset + HEADER_SIZE;
    match primitive {
        VariantPrimitive::Null | VariantPrimitive::Boolean(_) => {
            write_u8(buffer, offset, header)?;
            Ok(1)
        }
        VariantPrimitive::Int8(value) => {
            write_u8(buffer, offset, header)?;
            write_bytes(buffer, payload_offset, &value.to_le_bytes())?;
            Ok(2)
        }
        VariantPrimitive::Int16(value) => {
            write_u8(buffer, offset, header)?;
            write_bytes(buffer, payload_offset, &value.to_le_bytes())?;
            Ok(3)
        }
        VariantPrimitive::Int32(value) | VariantPrimitive::Date(value) => {
            write_u8(buffer, offset, header)?;
            write_bytes(buffer, payload_offset, &value.to_le_bytes())?;
            Ok(5)
        }
        VariantPrimitive::Float(value) => {
            write_u8(buffer, offset, header)?;
            write_bytes(buffer, payload_offset, &value.to_le_bytes())?;
            Ok(5)
        }
        VariantPrimitive::Int64(value)
        | VariantPrimitive::Timestamptz(value)
        | VariantPrimitive::Timestampntz(value)
        | VariantPrimitive::Time(value)
        | VariantPrimitive::TimestamptzNanos(value)
        | VariantPrimitive::TimestampntzNanos(value) => {
            write_u8(buffer, offset, header)?;
            write_bytes(buffer, payload_offset, &value.to_le_bytes())?;
            Ok(9)
        }
        VariantPrimitive::Double(value) => {
            write_u8(buffer, offset, header)?;
            write_bytes(buffer, payload_offset, &value.to_le_bytes())?;
            Ok(9)
        }
        VariantPrimitive::Decimal4 { scale, unscaled } => {
            write_u8(buffer, offset, header)?;
            write_u8(buffer, payload_offset, *scale)?;
            write_bytes(buffer, payload_offset + 1, &unscaled.to_le_bytes())?;
            Ok(6)
        }
        VariantPrimitive::Decimal8 { scale, unscaled } => {
            write_u8(buffer, offset, header)?;
            write_u8(buffer, payload_offset, *scale)?;
            write_bytes(buffer, payload_offset + 1, &unscaled.to_le_bytes())?;
            Ok(10)
        }
        VariantPrimitive::Decimal16 { scale, unscaled } => {
            write_u8(buffer, offset, header)?;
            write_u8(buffer, payload_offset, *scale)?;
            write_bytes(buffer, payload_offset + 1, &unscaled.to_le_bytes())?;
            Ok(18)
        }
        VariantPrimitive::Binary(data) => {
            let size = length_prefixed_size(data.len(), "binary")?;
            write_u8(buffer, offset, header)?;
            write_le_unsigned(buffer, data.len(), payload_offset, 4)?;
            write_bytes(buffer, payload_offset + 4, data)?;
            Ok(size)
        }
        VariantPrimitive::String(value) => {
            let utf8 = value.as_bytes();
            if utf8.len() <= MAX_SHORT_STRING_LENGTH {
                write_u8(buffer, offset, short_string_header(utf8.len()))?;
                write_bytes(buffer, payload_offset, utf8)?;
                Ok(HEADER_SIZE + utf8.len())
            } else {
                let size = length_prefixed_size(utf8.len(), "string")?;
                write_u8(buffer, offset, primitive_header(PhysicalType::String)?)?;
                write_le_unsigned(buffer, utf8.len(), payload_offset, 4)?;
                write_bytes(buffer, payload_offset + 4, utf8)?;
                Ok(size)
            }
        }
        VariantPrimitive::Uuid(big_endian_bytes) => {
            write_u8(buffer, offset, header)?;
            write_bytes(buffer, payload_offset, big_endian_bytes)?;
            Ok(17)
        }
    }
}

/// Writes an object (`ShreddedObject.SerializationState.writeTo`, 1.10.0): header, count,
/// field-id list, offset list (one extra entry holding the data length), then the field
/// values. Fields are emitted in their STORED order — [`VariantObjectBuilder::build`]
/// guarantees Java's name-sorted (UTF-16) order, and a PARSED spec-conforming object is
/// already name-sorted; each field id is re-resolved from the metadata BY NAME exactly like
/// Java (`metadata.id(field)` + `checkState`).
fn write_object(
    object: &VariantObject,
    metadata: &VariantMetadata,
    buffer: &mut [u8],
    offset: usize,
    depth: usize,
) -> Result<usize> {
    let layout = object_layout(object, metadata, depth)?;
    // Door the whole span up front (checked): afterwards every interior offset is bounded
    // by buffer.len() <= isize::MAX, so the plain offset arithmetic below cannot overflow.
    door_value_span(buffer, offset, layout.total_size, "object")?;
    let num_elements = object.num_fields();
    let count_size = if layout.is_large { 4 } else { 1 };
    let field_id_list_offset = offset + HEADER_SIZE + count_size;
    let offset_list_offset = field_id_list_offset + num_elements * layout.field_id_size;
    let data_offset = offset_list_offset + (1 + num_elements) * layout.offset_size;

    write_u8(
        buffer,
        offset,
        object_header(layout.is_large, layout.field_id_size, layout.offset_size),
    )?;
    write_le_unsigned(buffer, num_elements, offset + HEADER_SIZE, count_size)?;

    let mut next_value_offset = 0usize;
    for (index, field) in object.fields().iter().enumerate() {
        // Java: int id = metadata.id(field); checkState(id >= 0, "Invalid metadata,
        // missing: %s", field);
        let id = metadata
            .id(&field.name)
            .ok_or_else(|| util::invalid(format!("Invalid metadata, missing: {}", field.name)))?;
        write_le_unsigned(
            buffer,
            id,
            field_id_list_offset + index * layout.field_id_size,
            layout.field_id_size,
        )?;
        write_le_unsigned(
            buffer,
            next_value_offset,
            offset_list_offset + index * layout.offset_size,
            layout.offset_size,
        )?;
        let value_size = write_value(
            &field.value,
            metadata,
            buffer,
            data_offset + next_value_offset,
            depth + 1,
        )?;
        next_value_offset += value_size;
    }
    // The final offset entry is the total size of the data section.
    write_le_unsigned(
        buffer,
        next_value_offset,
        offset_list_offset + num_elements * layout.offset_size,
        layout.offset_size,
    )?;
    debug_assert_eq!(next_value_offset, layout.data_size);
    Ok((data_offset - offset) + layout.data_size)
}

/// Writes an array (`ValueArray.SerializationState.writeTo`, 1.10.0): header, count, offset
/// list (one extra entry holding the data length), then the elements in insertion order.
fn write_array(
    array: &VariantArray,
    metadata: &VariantMetadata,
    buffer: &mut [u8],
    offset: usize,
    depth: usize,
) -> Result<usize> {
    let layout = array_layout(array, metadata, depth)?;
    // Door the whole span up front (checked) — see `write_object`.
    door_value_span(buffer, offset, layout.total_size, "array")?;
    let num_elements = array.num_elements();
    let count_size = if layout.is_large { 4 } else { 1 };
    let offset_list_offset = offset + HEADER_SIZE + count_size;
    let data_offset = offset_list_offset + (1 + num_elements) * layout.offset_size;

    write_u8(
        buffer,
        offset,
        array_header(layout.is_large, layout.offset_size),
    )?;
    write_le_unsigned(buffer, num_elements, offset + HEADER_SIZE, count_size)?;

    let mut next_value_offset = 0usize;
    for (index, element) in array.elements().iter().enumerate() {
        write_le_unsigned(
            buffer,
            next_value_offset,
            offset_list_offset + index * layout.offset_size,
            layout.offset_size,
        )?;
        let value_size = write_value(
            element,
            metadata,
            buffer,
            data_offset + next_value_offset,
            depth + 1,
        )?;
        next_value_offset += value_size;
    }
    write_le_unsigned(
        buffer,
        next_value_offset,
        offset_list_offset + num_elements * layout.offset_size,
        layout.offset_size,
    )?;
    debug_assert_eq!(next_value_offset, layout.data_size);
    Ok((data_offset - offset) + layout.data_size)
}

/// A plain variant-object writer — the object-writing core of Java's `ShreddedObject`
/// (`Variants.object(metadata)` + `put(field, value)`), WITHOUT the shredding overlay (see
/// the module doc for the exact left-out surface).
///
/// `put` validates the field name against the metadata dictionary up front (Java's
/// `Preconditions.checkArgument(metadata.id(field) >= 0, "Cannot find field name in
/// metadata: %s")`) and replaces any previous value for the same name (Java's `HashMap`
/// semantics). [`Self::build`] produces a [`VariantObject`] whose fields are sorted by name
/// in Java `String.compareTo` (UTF-16 code unit) order — the on-disk field order Java's
/// writer emits (`SortedMerge` of name-sorted streams in `ShreddedObject.writeTo`).
#[derive(Debug)]
pub struct VariantObjectBuilder<'a> {
    metadata: &'a VariantMetadata,
    fields: HashMap<String, VariantValue>,
}

impl<'a> VariantObjectBuilder<'a> {
    /// Starts an object against the given metadata dictionary (Java
    /// `Variants.object(metadata)`).
    pub fn new(metadata: &'a VariantMetadata) -> VariantObjectBuilder<'a> {
        VariantObjectBuilder {
            metadata,
            fields: HashMap::new(),
        }
    }

    /// Sets a field, replacing any previous value for the same name (Java
    /// `ShreddedObject.put`).
    ///
    /// # Errors
    ///
    /// [`crate::ErrorKind::DataInvalid`] when the name is not in the metadata dictionary
    /// (Java throws `IllegalArgumentException("Cannot find field name in metadata: %s")`).
    pub fn put(&mut self, name: impl Into<String>, value: VariantValue) -> Result<()> {
        let name = name.into();
        if self.metadata.id(&name).is_none() {
            return Err(util::invalid(format!(
                "Cannot find field name in metadata: {name}"
            )));
        }
        self.fields.insert(name, value);
        Ok(())
    }

    /// Finishes the object: fields are sorted by name in Java `String.compareTo` (UTF-16)
    /// order with their dictionary ids resolved — the exact on-disk order and ids
    /// `ShreddedObject.writeTo` emits.
    pub fn build(self) -> VariantObject {
        let VariantObjectBuilder { metadata, fields } = self;
        let mut named: Vec<(String, VariantValue)> = fields.into_iter().collect();
        named.sort_by(|left, right| util::java_string_compare(&left.0, &right.0));
        let fields = named
            .into_iter()
            .map(|(name, value)| {
                let id = metadata
                    .id(&name)
                    .expect("field names are validated against the metadata at put time");
                VariantObjectField {
                    field_id: u32::try_from(id)
                        .expect("dictionary ids are bounded by Java's signed 32-bit domain"),
                    name,
                    value,
                }
            })
            .collect();
        VariantObject::from_fields(fields)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Risk pinned: the width-selection thresholds are Java's UNSIGNED `sizeOf` boundaries
    /// (0xFF/0xFFFF/0xFFFFFF) — an off-by-one here flips every offset/count width at the
    /// boundary and corrupts the whole container layout.
    #[test]
    fn test_size_of_unsigned_matches_java_thresholds() {
        assert_eq!(size_of_unsigned(0), 1);
        assert_eq!(size_of_unsigned(0xFF), 1);
        assert_eq!(size_of_unsigned(0x100), 2);
        assert_eq!(size_of_unsigned(0xFFFF), 2);
        assert_eq!(size_of_unsigned(0x1_0000), 3);
        assert_eq!(size_of_unsigned(0xFF_FFFF), 3);
        assert_eq!(size_of_unsigned(0x100_0000), 4);
        assert_eq!(size_of_unsigned(JAVA_INT_MAX), 4);
    }

    /// Risk pinned: Java's `writeLittleEndianUnsigned` MASKS an oversized value into the
    /// requested width (silent corruption); the Rust door must reject it by name instead.
    #[test]
    fn test_write_le_unsigned_rejects_oversized_values_java_would_truncate() {
        let mut buffer = [0u8; 4];
        let error =
            write_le_unsigned(&mut buffer, 0x100, 0, 1).expect_err("256 does not fit one byte");
        assert!(
            error.to_string().contains("does not fit"),
            "error must name the truncation, got: {error}"
        );
        write_le_unsigned(&mut buffer, 0xFF, 0, 1).expect("255 fits one byte");
        assert_eq!(buffer[0], 0xFF);
        write_le_unsigned(&mut buffer, 0x030201, 0, 3).expect("3-byte value");
        assert_eq!(&buffer[..3], &[0x01, 0x02, 0x03]);
    }

    /// Risk pinned: `BigDecimal.precision()` semantics — zero reports 1 (decimal4), the
    /// 9/10 and 18/19 digit boundaries flip the physical width, and 39 digits must error
    /// with Java's message (so `i128::MIN` is not constructible, exactly like Java).
    #[test]
    fn test_decimal_precision_boundaries_match_java_bigdecimal() {
        assert_eq!(decimal_precision(0), 1);
        assert_eq!(decimal_precision(-9), 1);
        assert_eq!(decimal_precision(999_999_999), 9);
        assert_eq!(decimal_precision(1_000_000_000), 10);
        assert_eq!(decimal_precision(999_999_999_999_999_999), 18);
        assert_eq!(decimal_precision(1_000_000_000_000_000_000), 19);
        assert_eq!(decimal_precision(i128::MIN), 39);

        match VariantValue::of_decimal(999_999_999, 2).expect("precision 9") {
            VariantValue::Primitive(VariantPrimitive::Decimal4 { scale: 2, .. }) => {}
            other => panic!("precision 9 must be decimal4, got {other:?}"),
        }
        match VariantValue::of_decimal(1_000_000_000, 2).expect("precision 10") {
            VariantValue::Primitive(VariantPrimitive::Decimal8 { scale: 2, .. }) => {}
            other => panic!("precision 10 must be decimal8, got {other:?}"),
        }
        match VariantValue::of_decimal(-1_000_000_000_000_000_000, 38).expect("precision 19") {
            VariantValue::Primitive(VariantPrimitive::Decimal16 { scale: 38, .. }) => {}
            other => panic!("precision 19 must be decimal16, got {other:?}"),
        }
        let error = VariantValue::of_decimal(i128::MIN, 38).expect_err("precision 39");
        assert_eq!(error.kind(), ErrorKind::FeatureUnsupported);
        assert!(
            error
                .to_string()
                .contains("Unsupported decimal precision: 39"),
            "error must carry Java's message, got: {error}"
        );
    }
}
