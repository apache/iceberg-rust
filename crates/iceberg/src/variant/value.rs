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

//! Variant value decoding — the port of Java 1.10.0 `VariantValue.from` and the
//! `SerializedPrimitive` / `SerializedShortString` / `SerializedObject` / `SerializedArray`
//! classes, plus the `Variant` / `VariantData` container. Parsed EAGERLY (see the module doc).

use std::ops::Range;

use crate::Result;
use crate::variant::metadata::VariantMetadata;
use crate::variant::types::{BasicType, PhysicalType};
use crate::variant::util;

/// Maximum nesting depth of objects/arrays the parser accepts.
///
/// Java has no equivalent limit because its LAZY parse only recurses as deep as the caller
/// traverses; the eager parse here recurses to the data's full depth, so a hostile
/// arrays-within-arrays value could otherwise overflow the stack (each nesting level costs as
/// little as 4 bytes on disk). 128 matches the common JSON-parser default (e.g. serde_json)
/// and is far above any real schema's nesting.
pub const MAX_NESTING_DEPTH: usize = 128;

/// Right shift to extract a primitive header's 6-bit type id (Java
/// `Primitives.PRIMITIVE_TYPE_SHIFT`; also the short-string length shift). Shared with the
/// write side (`VariantUtil.primitiveHeader` / `shortStringHeader` shift left by the same 2).
pub(super) const PRIMITIVE_TYPE_SHIFT: u8 = 2;
/// Offset of a primitive's payload, right after the header (Java
/// `SerializedPrimitive.PRIMITIVE_OFFSET` / `SerializedShortString.HEADER_SIZE`).
const PRIMITIVE_OFFSET: usize = 1;
/// Mask of an object header's offset-size field (Java `SerializedObject.OFFSET_SIZE_MASK`;
/// the array header uses the same bits, Java `SerializedArray.OFFSET_SIZE_MASK`).
const OFFSET_SIZE_MASK: u8 = 0b1100;
/// Right shift of the offset-size field (Java `OFFSET_SIZE_SHIFT`, object and array alike);
/// shared with the write side (`VariantUtil.objectHeader` / `arrayHeader`).
pub(super) const OFFSET_SIZE_SHIFT: u8 = 2;
/// Mask of an object header's field-id-size field (Java `SerializedObject.FIELD_ID_SIZE_MASK`).
const FIELD_ID_SIZE_MASK: u8 = 0b110000;
/// Right shift of the field-id-size field (Java `SerializedObject.FIELD_ID_SIZE_SHIFT`);
/// shared with the write side (`VariantUtil.objectHeader`).
pub(super) const FIELD_ID_SIZE_SHIFT: u8 = 4;
/// An object header's is-large bit: a 4-byte instead of 1-byte field count (Java
/// `SerializedObject.IS_LARGE`). Shared with the write side (`VariantUtil.objectHeader`).
pub(super) const OBJECT_IS_LARGE: u8 = 0b100_0000;
/// An array header's is-large bit (Java `SerializedArray.IS_LARGE` — note it differs from the
/// object's bit position). Shared with the write side (`VariantUtil.arrayHeader`).
pub(super) const ARRAY_IS_LARGE: u8 = 0b1_0000;
/// Size of the value header byte (Java `HEADER_SIZE` on the serialized classes); shared with
/// the write side.
pub(super) const HEADER_SIZE: usize = 1;

/// A decoded variant primitive — the Rust analogue of Java's `VariantPrimitive<T>` (covering
/// both `SerializedPrimitive` and `SerializedShortString`, which also reports
/// `PhysicalType.STRING`). Decimal values carry the raw scale byte and the little-endian
/// two's-complement unscaled value (`i32`/`i64`/`i128`), mirroring how
/// `crate::spec::values` represents decimal unscaled values (`PrimitiveLiteral::Int128`)
/// where Java uses `BigDecimal`.
#[derive(Debug, Clone, PartialEq)]
pub enum VariantPrimitive {
    /// JSON-style null.
    Null,
    /// A boolean ([`PhysicalType::BooleanTrue`] / [`PhysicalType::BooleanFalse`] — the value
    /// IS the physical type in the encoding).
    Boolean(bool),
    /// An 8-bit signed integer.
    Int8(i8),
    /// A 16-bit signed integer.
    Int16(i16),
    /// A 32-bit signed integer.
    Int32(i32),
    /// A 64-bit signed integer.
    Int64(i64),
    /// An IEEE-754 single-precision float.
    Float(f32),
    /// An IEEE-754 double-precision float.
    Double(f64),
    /// A decimal with a 32-bit unscaled value: `unscaled * 10^(-scale)`.
    Decimal4 {
        /// The scale byte (number of fractional digits).
        scale: u8,
        /// The two's-complement unscaled value.
        unscaled: i32,
    },
    /// A decimal with a 64-bit unscaled value: `unscaled * 10^(-scale)`.
    Decimal8 {
        /// The scale byte (number of fractional digits).
        scale: u8,
        /// The two's-complement unscaled value.
        unscaled: i64,
    },
    /// A decimal with a 128-bit unscaled value: `unscaled * 10^(-scale)`.
    Decimal16 {
        /// The scale byte (number of fractional digits).
        scale: u8,
        /// The two's-complement unscaled value.
        unscaled: i128,
    },
    /// A date as days from the unix epoch.
    Date(i32),
    /// A UTC-adjusted timestamp as microseconds from the unix epoch.
    Timestamptz(i64),
    /// A local (zone-less) timestamp as microseconds from the unix epoch.
    Timestampntz(i64),
    /// A time of day as microseconds from midnight.
    Time(i64),
    /// A UTC-adjusted timestamp as nanoseconds from the unix epoch.
    TimestamptzNanos(i64),
    /// A local timestamp as nanoseconds from the unix epoch.
    TimestampntzNanos(i64),
    /// An opaque byte sequence.
    Binary(Vec<u8>),
    /// A UTF-8 string (long form or short-string form — both report
    /// [`PhysicalType::String`], like Java).
    String(String),
    /// A UUID as its 16 big-endian (RFC 4122) bytes, exactly as stored; Java converts the
    /// same bytes to a `java.util.UUID`.
    Uuid([u8; 16]),
}

impl VariantPrimitive {
    /// Returns the physical type this primitive decodes (Java `VariantValue.type()`).
    pub fn physical_type(&self) -> PhysicalType {
        match self {
            VariantPrimitive::Null => PhysicalType::Null,
            VariantPrimitive::Boolean(true) => PhysicalType::BooleanTrue,
            VariantPrimitive::Boolean(false) => PhysicalType::BooleanFalse,
            VariantPrimitive::Int8(_) => PhysicalType::Int8,
            VariantPrimitive::Int16(_) => PhysicalType::Int16,
            VariantPrimitive::Int32(_) => PhysicalType::Int32,
            VariantPrimitive::Int64(_) => PhysicalType::Int64,
            VariantPrimitive::Float(_) => PhysicalType::Float,
            VariantPrimitive::Double(_) => PhysicalType::Double,
            VariantPrimitive::Decimal4 { .. } => PhysicalType::Decimal4,
            VariantPrimitive::Decimal8 { .. } => PhysicalType::Decimal8,
            VariantPrimitive::Decimal16 { .. } => PhysicalType::Decimal16,
            VariantPrimitive::Date(_) => PhysicalType::Date,
            VariantPrimitive::Timestamptz(_) => PhysicalType::Timestamptz,
            VariantPrimitive::Timestampntz(_) => PhysicalType::Timestampntz,
            VariantPrimitive::Time(_) => PhysicalType::Time,
            VariantPrimitive::TimestamptzNanos(_) => PhysicalType::TimestamptzNanos,
            VariantPrimitive::TimestampntzNanos(_) => PhysicalType::TimestampntzNanos,
            VariantPrimitive::Binary(_) => PhysicalType::Binary,
            VariantPrimitive::String(_) => PhysicalType::String,
            VariantPrimitive::Uuid(_) => PhysicalType::Uuid,
        }
    }
}

/// One decoded object field: the dictionary id it was stored with, the resolved name, and the
/// value. Fields keep their stored order (the spec sorts them by name; this port does not
/// re-sort — see [`VariantObject::get`]).
#[derive(Debug, Clone, PartialEq)]
pub struct VariantObjectField {
    /// The metadata dictionary id the field name was stored as.
    pub field_id: u32,
    /// The field name (resolved from the metadata dictionary at parse time).
    pub name: String,
    /// The field value.
    pub value: VariantValue,
}

/// A decoded variant object — the Rust analogue of Java's `VariantObject` /
/// `SerializedObject`.
#[derive(Debug, Clone, PartialEq)]
pub struct VariantObject {
    fields: Vec<VariantObjectField>,
}

impl VariantObject {
    /// Assembles an object from already-resolved fields — the write side's constructor seam
    /// (`VariantObjectBuilder::build` in `write.rs` sorts the fields by name in Java
    /// `String.compareTo` order and resolves ids before constructing through here).
    pub(super) fn from_fields(fields: Vec<VariantObjectField>) -> VariantObject {
        VariantObject { fields }
    }

    /// Returns the number of fields (Java `numFields()`).
    pub fn num_fields(&self) -> usize {
        self.fields.len()
    }

    /// Returns the value of the field named `name`, or `None` if absent — Java
    /// `get(String)`, which returns `null` on a miss.
    ///
    /// Exactly like Java, the lookup BINARY-SEARCHES the stored field order assuming the spec's
    /// sort-by-name invariant (in Java `String.compareTo` order); for a non-conforming object
    /// whose fields are not name-sorted the search can miss a present field, identically to
    /// Java 1.10.0 (`SerializedObject.get` → `VariantUtil.find`).
    pub fn get(&self, name: &str) -> Option<&VariantValue> {
        util::find(self.fields.len(), name, |index| {
            self.fields[index].name.as_str()
        })
        .map(|index| &self.fields[index].value)
    }

    /// Iterates the field names in stored order (Java `fieldNames()`).
    pub fn field_names(&self) -> impl Iterator<Item = &str> {
        self.fields.iter().map(|field| field.name.as_str())
    }

    /// Returns the decoded fields in stored order.
    pub fn fields(&self) -> &[VariantObjectField] {
        &self.fields
    }
}

/// A decoded variant array — the Rust analogue of Java's `VariantArray` / `SerializedArray`.
#[derive(Debug, Clone, PartialEq)]
pub struct VariantArray {
    elements: Vec<VariantValue>,
}

impl Default for VariantArray {
    fn default() -> VariantArray {
        VariantArray::new()
    }
}

impl VariantArray {
    /// Creates an empty, writable array — the Rust analogue of Java `Variants.array()`
    /// returning a fresh `ValueArray`. Fill it with [`VariantArray::push`], then serialize
    /// via [`VariantValue::to_bytes`](crate::variant::VariantValue::to_bytes).
    pub fn new() -> VariantArray {
        VariantArray {
            elements: Vec::new(),
        }
    }

    /// Appends an element (Java `ValueArray.add`).
    pub fn push(&mut self, value: VariantValue) {
        self.elements.push(value);
    }

    /// Returns the number of elements (Java `numElements()`).
    pub fn num_elements(&self) -> usize {
        self.elements.len()
    }

    /// Returns the element at `index`, or `None` when out of range (Java `get(int)` throws an
    /// unchecked `ArrayIndexOutOfBoundsException` there).
    pub fn get(&self, index: usize) -> Option<&VariantValue> {
        self.elements.get(index)
    }

    /// Returns the decoded elements.
    pub fn elements(&self) -> &[VariantValue] {
        &self.elements
    }
}

/// A decoded variant value — the Rust analogue of Java's `VariantValue` hierarchy
/// (`VariantPrimitive` / `VariantObject` / `VariantArray` as an enum instead of interfaces).
#[derive(Debug, Clone, PartialEq)]
pub enum VariantValue {
    /// A primitive (including short strings, which Java also reports as primitives).
    Primitive(VariantPrimitive),
    /// An object.
    Object(VariantObject),
    /// An array.
    Array(VariantArray),
}

impl VariantValue {
    /// Decodes a variant value from untrusted bytes against its `metadata` dictionary — the
    /// port of Java `VariantValue.from(VariantMetadata, ByteBuffer)`, parsed eagerly. Trailing
    /// bytes after the encoded value are ignored, exactly as Java's lazy reads never touch
    /// them.
    ///
    /// # Errors
    ///
    /// [`crate::ErrorKind::DataInvalid`] for structural corruption (truncation, ranges past
    /// the buffer, bad offsets, field ids outside the dictionary, invalid UTF-8) and
    /// [`crate::ErrorKind::FeatureUnsupported`] for an unknown primitive type id; never
    /// panics. Nesting beyond [`MAX_NESTING_DEPTH`] is rejected (see the constant's doc).
    pub fn parse(metadata: &VariantMetadata, bytes: &[u8]) -> Result<VariantValue> {
        parse_value(metadata, bytes, 0)
    }

    /// Returns the physical type of this value (Java `VariantValue.type()`).
    pub fn physical_type(&self) -> PhysicalType {
        match self {
            VariantValue::Primitive(primitive) => primitive.physical_type(),
            VariantValue::Object(_) => PhysicalType::Object,
            VariantValue::Array(_) => PhysicalType::Array,
        }
    }

    /// Returns this value as a primitive (Java `asPrimitive()`).
    ///
    /// # Errors
    ///
    /// [`crate::ErrorKind::DataInvalid`] if the value is not a primitive (Java throws
    /// `IllegalArgumentException("Not a primitive: ...")`).
    pub fn as_primitive(&self) -> Result<&VariantPrimitive> {
        match self {
            VariantValue::Primitive(primitive) => Ok(primitive),
            other => Err(util::invalid(format!(
                "Not a primitive: {:?}",
                other.physical_type()
            ))),
        }
    }

    /// Returns this value as an object (Java `asObject()`).
    ///
    /// # Errors
    ///
    /// [`crate::ErrorKind::DataInvalid`] if the value is not an object (Java throws
    /// `IllegalArgumentException("Not an object: ...")`).
    pub fn as_object(&self) -> Result<&VariantObject> {
        match self {
            VariantValue::Object(object) => Ok(object),
            other => Err(util::invalid(format!(
                "Not an object: {:?}",
                other.physical_type()
            ))),
        }
    }

    /// Returns this value as an array (Java `asArray()`).
    ///
    /// # Errors
    ///
    /// [`crate::ErrorKind::DataInvalid`] if the value is not an array (Java throws
    /// `IllegalArgumentException("Not an array: ...")`).
    pub fn as_array(&self) -> Result<&VariantArray> {
        match self {
            VariantValue::Array(array) => Ok(array),
            other => Err(util::invalid(format!(
                "Not an array: {:?}",
                other.physical_type()
            ))),
        }
    }
}

/// A complete variant: the metadata dictionary plus the value decoded against it — the Rust
/// analogue of Java's `Variant` interface / `VariantData` pair.
#[derive(Debug, Clone, PartialEq)]
pub struct Variant {
    metadata: VariantMetadata,
    value: VariantValue,
}

impl Variant {
    /// The current version of the variant spec (Java `Variant.VARIANT_SPEC_VERSION`).
    pub const SPEC_VERSION: u8 = 1;

    /// Combines already-decoded metadata and value (Java `Variant.of`; the null checks are
    /// unrepresentable here).
    pub fn of(metadata: VariantMetadata, value: VariantValue) -> Variant {
        Variant { metadata, value }
    }

    /// Decodes a variant from a single buffer holding the metadata immediately followed by the
    /// value — the port of Java `Variant.from(ByteBuffer)`, which slices the value at
    /// `metadata.sizeInBytes()`.
    ///
    /// # Errors
    ///
    /// Any metadata or value parse error ([`VariantMetadata::parse`] /
    /// [`VariantValue::parse`]).
    pub fn from_bytes(bytes: &[u8]) -> Result<Variant> {
        let metadata = VariantMetadata::parse(bytes)?;
        let value_bytes = util::slice(
            bytes,
            metadata.size_in_bytes(),
            bytes.len() - metadata.size_in_bytes(),
        )?;
        let value = VariantValue::parse(&metadata, value_bytes)?;
        Ok(Variant { metadata, value })
    }

    /// Returns the metadata for all values in the variant (Java `metadata()`).
    pub fn metadata(&self) -> &VariantMetadata {
        &self.metadata
    }

    /// Returns the variant value (Java `value()`).
    pub fn value(&self) -> &VariantValue {
        &self.value
    }
}

/// Recursive decode dispatch — Java `VariantValue.from(metadata, value, header)`.
///
/// Recursion is justified per the manuals: the format is genuinely recursive, and depth is
/// explicitly bounded by [`MAX_NESTING_DEPTH`] (checked here, before any child parse), so the
/// stack usage is provably capped.
fn parse_value(metadata: &VariantMetadata, bytes: &[u8], depth: usize) -> Result<VariantValue> {
    if depth > MAX_NESTING_DEPTH {
        return Err(util::invalid(format!(
            "Invalid variant: nesting depth exceeds the supported maximum {MAX_NESTING_DEPTH}"
        )));
    }
    let header = util::read_u8(bytes, 0)
        .map_err(|error| error.with_context("context", "variant value header"))?;
    match BasicType::from_header(header) {
        BasicType::Primitive => parse_primitive(bytes, header).map(VariantValue::Primitive),
        BasicType::ShortString => parse_short_string(bytes, header).map(VariantValue::Primitive),
        BasicType::Object => {
            parse_object(metadata, bytes, header, depth, None).map(VariantValue::Object)
        }
        BasicType::Array => parse_array(metadata, bytes, header, depth),
    }
}

/// Parses a top-level object value AND records each stored-order field's value byte range
/// within `bytes` — the seam the shredding overlay (`shredded.rs`) uses to mirror Java
/// `SerializedObject.sliceValue(index)` verbatim-slice reuse (the range of field `i` is
/// exactly the slice Java's `sliceValue(i)` returns: `dataOffset + offsets[i]` for
/// `lengths[i]` bytes, lengths derived from the sorted-distinct-offsets scheme).
///
/// Non-object bytes are rejected with Java's `asObject()` contract ("Not an object: %s") —
/// the typed `Variants.object(VariantMetadata, VariantObject)` signature makes the case
/// unrepresentable in Java; parsing raw bytes and then casting hits exactly `asObject()`.
///
/// # Errors
///
/// Any [`VariantValue::parse`] error, plus the non-object rejection above.
pub(super) fn parse_object_with_value_ranges(
    metadata: &VariantMetadata,
    bytes: &[u8],
) -> Result<(VariantObject, Vec<Range<usize>>)> {
    let header = util::read_u8(bytes, 0)
        .map_err(|error| error.with_context("context", "variant value header"))?;
    if BasicType::from_header(header) != BasicType::Object {
        let value = parse_value(metadata, bytes, 0)?;
        return Err(util::invalid(format!(
            "Not an object: {:?}",
            value.physical_type()
        )));
    }
    let mut value_ranges = Vec::new();
    let object = parse_object(metadata, bytes, header, 0, Some(&mut value_ranges))?;
    Ok((object, value_ranges))
}

/// Decodes a primitive value (Java `SerializedPrimitive.from` + its `read()` payload layouts;
/// every case bytecode-verified against 1.10.0).
fn parse_primitive(bytes: &[u8], header: u8) -> Result<VariantPrimitive> {
    let physical_type = PhysicalType::from_type_info(header >> PRIMITIVE_TYPE_SHIFT)?;
    let primitive = match physical_type {
        PhysicalType::Null => VariantPrimitive::Null,
        PhysicalType::BooleanTrue => VariantPrimitive::Boolean(true),
        PhysicalType::BooleanFalse => VariantPrimitive::Boolean(false),
        PhysicalType::Int8 => VariantPrimitive::Int8(util::read_i8(bytes, PRIMITIVE_OFFSET)?),
        PhysicalType::Int16 => VariantPrimitive::Int16(util::read_i16_le(bytes, PRIMITIVE_OFFSET)?),
        PhysicalType::Int32 => VariantPrimitive::Int32(util::read_i32_le(bytes, PRIMITIVE_OFFSET)?),
        PhysicalType::Int64 => VariantPrimitive::Int64(util::read_i64_le(bytes, PRIMITIVE_OFFSET)?),
        PhysicalType::Float => VariantPrimitive::Float(util::read_f32_le(bytes, PRIMITIVE_OFFSET)?),
        PhysicalType::Double => {
            VariantPrimitive::Double(util::read_f64_le(bytes, PRIMITIVE_OFFSET)?)
        }
        PhysicalType::Date => VariantPrimitive::Date(util::read_i32_le(bytes, PRIMITIVE_OFFSET)?),
        PhysicalType::Timestamptz => {
            VariantPrimitive::Timestamptz(util::read_i64_le(bytes, PRIMITIVE_OFFSET)?)
        }
        PhysicalType::Timestampntz => {
            VariantPrimitive::Timestampntz(util::read_i64_le(bytes, PRIMITIVE_OFFSET)?)
        }
        PhysicalType::Time => VariantPrimitive::Time(util::read_i64_le(bytes, PRIMITIVE_OFFSET)?),
        PhysicalType::TimestamptzNanos => {
            VariantPrimitive::TimestamptzNanos(util::read_i64_le(bytes, PRIMITIVE_OFFSET)?)
        }
        PhysicalType::TimestampntzNanos => {
            VariantPrimitive::TimestampntzNanos(util::read_i64_le(bytes, PRIMITIVE_OFFSET)?)
        }
        // Decimals: a scale byte then the little-endian two's-complement unscaled value.
        PhysicalType::Decimal4 => VariantPrimitive::Decimal4 {
            scale: util::read_u8(bytes, PRIMITIVE_OFFSET)?,
            unscaled: util::read_i32_le(bytes, PRIMITIVE_OFFSET + 1)?,
        },
        PhysicalType::Decimal8 => VariantPrimitive::Decimal8 {
            scale: util::read_u8(bytes, PRIMITIVE_OFFSET)?,
            unscaled: util::read_i64_le(bytes, PRIMITIVE_OFFSET + 1)?,
        },
        PhysicalType::Decimal16 => VariantPrimitive::Decimal16 {
            scale: util::read_u8(bytes, PRIMITIVE_OFFSET)?,
            unscaled: util::read_i128_le(bytes, PRIMITIVE_OFFSET + 1)?,
        },
        // Binary and (long-form) string: a little-endian i32 length then the payload. Java
        // reads the length as a SIGNED int; a negative length fails its slice, so reject it
        // by name here.
        PhysicalType::Binary => {
            let length = util::read_i32_le(bytes, PRIMITIVE_OFFSET)?;
            let length = usize::try_from(length).map_err(|_| {
                util::invalid(format!("Invalid variant binary: negative length {length}"))
            })?;
            VariantPrimitive::Binary(util::slice(bytes, PRIMITIVE_OFFSET + 4, length)?.to_vec())
        }
        PhysicalType::String => {
            let length = util::read_i32_le(bytes, PRIMITIVE_OFFSET)?;
            let length = usize::try_from(length).map_err(|_| {
                util::invalid(format!("Invalid variant string: negative length {length}"))
            })?;
            let raw = util::slice(bytes, PRIMITIVE_OFFSET + 4, length)?;
            VariantPrimitive::String(decode_utf8(raw, "string")?.to_string())
        }
        // UUID: 16 bytes, stored big-endian (RFC 4122); Java wraps the same bytes into a
        // java.util.UUID via a BIG_ENDIAN slice.
        PhysicalType::Uuid => VariantPrimitive::Uuid(
            util::slice(bytes, PRIMITIVE_OFFSET, 16)?
                .try_into()
                .expect("slice() returned exactly 16 bytes"),
        ),
        PhysicalType::Array | PhysicalType::Object => {
            // Unreachable: from_type_info never returns container types (ids stop at 20).
            return Err(util::invalid(
                "Invalid variant: container type in a primitive header",
            ));
        }
    };
    Ok(primitive)
}

/// Decodes a short string: the 6-bit length lives in the header (Java
/// `SerializedShortString`); reports `PhysicalType::String` like Java.
fn parse_short_string(bytes: &[u8], header: u8) -> Result<VariantPrimitive> {
    let length = usize::from(header >> PRIMITIVE_TYPE_SHIFT);
    let raw = util::slice(bytes, HEADER_SIZE, length)?;
    Ok(VariantPrimitive::String(
        decode_utf8(raw, "short string")?.to_string(),
    ))
}

/// Validates UTF-8, failing loud where Java's `readString` would silently substitute U+FFFD
/// (the documented divergence — see the module doc).
fn decode_utf8<'a>(raw: &'a [u8], what: &str) -> Result<&'a str> {
    std::str::from_utf8(raw).map_err(|source| {
        util::invalid(format!("Invalid variant {what}: not valid UTF-8")).with_source(source)
    })
}

/// Decodes an object (Java `SerializedObject`): header field sizes, a field count, a field-id
/// list, an offset list (one extra entry holding the data length), then the field values.
/// `record_value_ranges`, when given, receives each stored-order field's value byte range
/// within `bytes` (the [`parse_object_with_value_ranges`] seam — `None` everywhere else, so
/// nested objects never pay for it).
fn parse_object(
    metadata: &VariantMetadata,
    bytes: &[u8],
    header: u8,
    depth: usize,
    mut record_value_ranges: Option<&mut Vec<Range<usize>>>,
) -> Result<VariantObject> {
    let offset_size = 1 + usize::from((header & OFFSET_SIZE_MASK) >> OFFSET_SIZE_SHIFT);
    let field_id_size = 1 + usize::from((header & FIELD_ID_SIZE_MASK) >> FIELD_ID_SIZE_SHIFT);
    let num_elements_size = if header & OBJECT_IS_LARGE == OBJECT_IS_LARGE {
        4
    } else {
        1
    };
    let num_elements = util::read_le_unsigned(bytes, HEADER_SIZE, num_elements_size)? as usize;
    let field_id_list_offset = HEADER_SIZE + num_elements_size;
    // offsetListOffset = fieldIdListOffset + numElements * fieldIdSize, and
    // dataOffset = offsetListOffset + (1 + numElements) * offsetSize — checked, and the offset
    // list's end is bounds-checked against the buffer BEFORE any per-element work, so a
    // hostile count fails fast without allocation.
    let offset_list_offset = num_elements
        .checked_mul(field_id_size)
        .and_then(|len| field_id_list_offset.checked_add(len))
        .ok_or_else(|| util::invalid("Invalid variant object: field-id list overflows"))?;
    let data_offset = num_elements
        .checked_add(1)
        .and_then(|count| count.checked_mul(offset_size))
        .and_then(|len| offset_list_offset.checked_add(len))
        .ok_or_else(|| util::invalid("Invalid variant object: offset list overflows"))?;
    if data_offset > bytes.len() {
        return Err(util::invalid(format!(
            "Invalid variant object: {num_elements} fields declare {data_offset} header bytes, \
             but the value is {} bytes",
            bytes.len()
        )));
    }

    // Read the field ids and offsets, plus the final data-length entry.
    let mut field_ids = Vec::with_capacity(num_elements);
    for index in 0..num_elements {
        field_ids.push(util::read_le_unsigned(
            bytes,
            field_id_list_offset + index * field_id_size,
            field_id_size,
        )?);
    }
    let mut offsets = Vec::with_capacity(num_elements);
    for index in 0..num_elements {
        offsets.push(util::read_le_unsigned(
            bytes,
            offset_list_offset + index * offset_size,
            offset_size,
        )? as usize);
    }
    let data_length = util::read_le_unsigned(
        bytes,
        offset_list_offset + num_elements * offset_size,
        offset_size,
    )? as usize;

    // Field lengths, Java-exactly (`SerializedObject.initOffsetsAndLengths`): field order is
    // by NAME, data order can differ, so each field's length is the distance to the NEXT
    // DISTINCT offset in sorted order (the data length included). Java's scheme requires all
    // offsets + the data length to be pairwise distinct — duplicates make its
    // `sortedOffsets.get(index + 1)` throw — so duplicates are rejected here by name.
    let mut sorted_offsets: Vec<usize> = offsets.iter().copied().chain([data_length]).collect();
    sorted_offsets.sort_unstable();
    if sorted_offsets.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(util::invalid(
            "Invalid variant object: duplicate field offsets",
        ));
    }
    let length_of = |offset: usize| -> usize {
        // Every entry is in sorted_offsets by construction; the LAST sorted entry gets length
        // 0 (Java leaves it 0), which the child parse then rejects as an empty value.
        match sorted_offsets.binary_search(&offset) {
            Ok(position) if position + 1 < sorted_offsets.len() => {
                sorted_offsets[position + 1] - offset
            }
            _ => 0,
        }
    };

    let mut fields = Vec::with_capacity(num_elements);
    for (field_id, offset) in field_ids.into_iter().zip(offsets) {
        let name = metadata
            .get(field_id as usize)
            .map_err(|error| error.with_context("context", "variant object field id"))?
            .to_string();
        let start = data_offset
            .checked_add(offset)
            .ok_or_else(|| util::invalid("Invalid variant object: field offset overflows"))?;
        let length = length_of(offset);
        let field_bytes = util::slice(bytes, start, length)?;
        if let Some(ranges) = record_value_ranges.as_deref_mut() {
            ranges.push(start..start + length);
        }
        let value = parse_value(metadata, field_bytes, depth + 1)?;
        fields.push(VariantObjectField {
            field_id,
            name,
            value,
        });
    }

    Ok(VariantObject { fields })
}

/// Decodes an array (Java `SerializedArray`): an element count, then `count + 1` offsets
/// delimiting CONSECUTIVE elements (unlike object fields, array elements are stored in offset
/// order).
fn parse_array(
    metadata: &VariantMetadata,
    bytes: &[u8],
    header: u8,
    depth: usize,
) -> Result<VariantValue> {
    let offset_size = 1 + usize::from((header & OFFSET_SIZE_MASK) >> OFFSET_SIZE_SHIFT);
    let num_elements_size = if header & ARRAY_IS_LARGE == ARRAY_IS_LARGE {
        4
    } else {
        1
    };
    let num_elements = util::read_le_unsigned(bytes, HEADER_SIZE, num_elements_size)? as usize;
    let offset_list_offset = HEADER_SIZE + num_elements_size;
    let data_offset = num_elements
        .checked_add(1)
        .and_then(|count| count.checked_mul(offset_size))
        .and_then(|len| offset_list_offset.checked_add(len))
        .ok_or_else(|| util::invalid("Invalid variant array: offset list overflows"))?;
    if data_offset > bytes.len() {
        return Err(util::invalid(format!(
            "Invalid variant array: {num_elements} elements declare {data_offset} header bytes, \
             but the value is {} bytes",
            bytes.len()
        )));
    }

    let mut elements = Vec::with_capacity(num_elements);
    let mut offset = util::read_le_unsigned(bytes, offset_list_offset, offset_size)? as usize;
    for index in 0..num_elements {
        let next = util::read_le_unsigned(
            bytes,
            offset_list_offset + offset_size * (index + 1),
            offset_size,
        )? as usize;
        // Java slices `next - offset`; a negative length fails its slice, reject by name.
        let length = next.checked_sub(offset).ok_or_else(|| {
            util::invalid(format!(
                "Invalid variant array: offsets not monotonically increasing ({next} follows \
                 {offset} at element {index})"
            ))
        })?;
        let start = data_offset
            .checked_add(offset)
            .ok_or_else(|| util::invalid("Invalid variant array: element offset overflows"))?;
        let element_bytes = util::slice(bytes, start, length)?;
        elements.push(parse_value(metadata, element_bytes, depth + 1)?);
        offset = next;
    }

    Ok(VariantValue::Array(VariantArray { elements }))
}
