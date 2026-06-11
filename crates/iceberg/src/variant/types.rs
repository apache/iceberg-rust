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

//! The variant type enums: basic types (the 2-bit header tag), physical types (the encoded
//! representation), and logical types (the semantic grouping) — ports of Java 1.10.0
//! `BasicType`, `PhysicalType`, and `LogicalType` (all bytecode-verified against
//! `iceberg-api-1.10.0.jar`).

use crate::{Error, ErrorKind, Result};

/// The 2-bit basic type carried in the low two bits of every value header byte
/// (Java `BasicType` + `VariantUtil.basicType`, 1.10.0: PRIMITIVE=0, SHORT_STRING=1,
/// OBJECT=2, ARRAY=3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BasicType {
    /// A primitive value; the remaining 6 header bits are the primitive type id.
    Primitive,
    /// A short string; the remaining 6 header bits are the string length (0..=63).
    ShortString,
    /// An object; the remaining header bits encode field-id size, offset size, and is-large.
    Object,
    /// An array; the remaining header bits encode offset size and is-large.
    Array,
}

impl BasicType {
    /// Decodes the basic type from a value header byte (Java `VariantUtil.basicType`:
    /// `header & BASIC_TYPE_MASK`). Infallible — the 2-bit mask covers all four variants.
    pub fn from_header(header: u8) -> BasicType {
        match header & 0b11 {
            0 => BasicType::Primitive,
            1 => BasicType::ShortString,
            2 => BasicType::Object,
            // The mask leaves only 0..=3; the remaining value is 3.
            _ => BasicType::Array,
        }
    }
}

/// The logical (semantic) type of a variant value — Java's package-private `LogicalType`
/// enum (1.10.0). Several physical encodings can share one logical type (for example
/// `Int8`/`Int16`/`Int32`/`Int64` and the decimals are all `ExactNumeric`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LogicalType {
    /// JSON-style null.
    Null,
    /// Boolean (true or false).
    Boolean,
    /// An exact numeric: the integer widths and the decimals.
    ExactNumeric,
    /// IEEE-754 single precision.
    Float,
    /// IEEE-754 double precision.
    Double,
    /// A calendar date.
    Date,
    /// A UTC-adjusted timestamp (micros or nanos precision).
    Timestamptz,
    /// A local, zone-less timestamp (micros or nanos precision).
    Timestampntz,
    /// An opaque byte sequence.
    Binary,
    /// A UTF-8 string.
    String,
    /// A time of day.
    Time,
    /// A UUID.
    Uuid,
    /// An array.
    Array,
    /// An object.
    Object,
}

/// The physical encoding of a variant value — the EXACT Java 1.10.0 `PhysicalType` set
/// (23 constants, bytecode-verified). Primitive physical types map to the 6-bit type id of
/// a primitive header via [`PhysicalType::from_type_info`] (Java `PhysicalType.from(int)` over
/// the `Primitives.TYPE_*` constants, values 0..=20); [`PhysicalType::Array`] and
/// [`PhysicalType::Object`] have no type id — they are container basic types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PhysicalType {
    /// JSON-style null (type id 0; no payload).
    Null,
    /// Boolean true (type id 1; the value is in the type id, no payload).
    BooleanTrue,
    /// Boolean false (type id 2; the value is in the type id, no payload).
    BooleanFalse,
    /// 8-bit signed integer (type id 3).
    Int8,
    /// 16-bit signed integer, little-endian (type id 4).
    Int16,
    /// 32-bit signed integer, little-endian (type id 5).
    Int32,
    /// 64-bit signed integer, little-endian (type id 6).
    Int64,
    /// IEEE-754 double, little-endian (type id 7).
    Double,
    /// Decimal with a 1-byte scale and a little-endian 32-bit unscaled value (type id 8).
    Decimal4,
    /// Decimal with a 1-byte scale and a little-endian 64-bit unscaled value (type id 9).
    Decimal8,
    /// Decimal with a 1-byte scale and a little-endian 128-bit unscaled value (type id 10).
    Decimal16,
    /// Date as a little-endian 32-bit day ordinal from the unix epoch (type id 11).
    Date,
    /// UTC-adjusted timestamp as little-endian 64-bit microseconds from the epoch (type id 12).
    Timestamptz,
    /// Local (no zone) timestamp as little-endian 64-bit microseconds from the epoch (type id 13).
    Timestampntz,
    /// IEEE-754 float, little-endian (type id 14).
    Float,
    /// Binary: a little-endian 32-bit length then the bytes (type id 15).
    Binary,
    /// String: a little-endian 32-bit length then UTF-8 bytes (type id 16). Short strings
    /// (length <= 63) are usually encoded via [`BasicType::ShortString`] instead, which also
    /// reports this physical type.
    String,
    /// Time of day as little-endian 64-bit microseconds from midnight (type id 17).
    Time,
    /// UTC-adjusted timestamp as little-endian 64-bit nanoseconds from the epoch (type id 18).
    TimestamptzNanos,
    /// Local timestamp as little-endian 64-bit nanoseconds from the epoch (type id 19).
    TimestampntzNanos,
    /// UUID as 16 big-endian (RFC 4122) bytes (type id 20).
    Uuid,
    /// An array value ([`BasicType::Array`]; no primitive type id).
    Array,
    /// An object value ([`BasicType::Object`]; no primitive type id).
    Object,
}

impl PhysicalType {
    /// Decodes a primitive physical type from the 6-bit type id of a primitive value header
    /// (Java `PhysicalType.from(int)`, 1.10.0 tableswitch over 0..=20).
    ///
    /// # Errors
    ///
    /// [`ErrorKind::FeatureUnsupported`] for ids above 20 (Java throws
    /// `UnsupportedOperationException("Unknown primitive physical type: ...")` — a later spec
    /// revision could define them, so unknown is "unsupported", not "corrupt").
    pub fn from_type_info(type_info: u8) -> Result<PhysicalType> {
        match type_info {
            0 => Ok(PhysicalType::Null),
            1 => Ok(PhysicalType::BooleanTrue),
            2 => Ok(PhysicalType::BooleanFalse),
            3 => Ok(PhysicalType::Int8),
            4 => Ok(PhysicalType::Int16),
            5 => Ok(PhysicalType::Int32),
            6 => Ok(PhysicalType::Int64),
            7 => Ok(PhysicalType::Double),
            8 => Ok(PhysicalType::Decimal4),
            9 => Ok(PhysicalType::Decimal8),
            10 => Ok(PhysicalType::Decimal16),
            11 => Ok(PhysicalType::Date),
            12 => Ok(PhysicalType::Timestamptz),
            13 => Ok(PhysicalType::Timestampntz),
            14 => Ok(PhysicalType::Float),
            15 => Ok(PhysicalType::Binary),
            16 => Ok(PhysicalType::String),
            17 => Ok(PhysicalType::Time),
            18 => Ok(PhysicalType::TimestamptzNanos),
            19 => Ok(PhysicalType::TimestampntzNanos),
            20 => Ok(PhysicalType::Uuid),
            unknown => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!("Unknown primitive physical type: {unknown}"),
            )),
        }
    }

    /// Returns the 6-bit primitive type id this physical type encodes as, or `None` for the
    /// container types ([`PhysicalType::Array`] / [`PhysicalType::Object`], which have no
    /// primitive type id) — the inverse of [`PhysicalType::from_type_info`]. Java has no
    /// single method for this; `PrimitiveWrapper` bakes the `Primitives.TYPE_*` constants
    /// into its per-type headers (1.10.0).
    pub fn to_type_info(self) -> Option<u8> {
        match self {
            PhysicalType::Null => Some(0),
            PhysicalType::BooleanTrue => Some(1),
            PhysicalType::BooleanFalse => Some(2),
            PhysicalType::Int8 => Some(3),
            PhysicalType::Int16 => Some(4),
            PhysicalType::Int32 => Some(5),
            PhysicalType::Int64 => Some(6),
            PhysicalType::Double => Some(7),
            PhysicalType::Decimal4 => Some(8),
            PhysicalType::Decimal8 => Some(9),
            PhysicalType::Decimal16 => Some(10),
            PhysicalType::Date => Some(11),
            PhysicalType::Timestamptz => Some(12),
            PhysicalType::Timestampntz => Some(13),
            PhysicalType::Float => Some(14),
            PhysicalType::Binary => Some(15),
            PhysicalType::String => Some(16),
            PhysicalType::Time => Some(17),
            PhysicalType::TimestamptzNanos => Some(18),
            PhysicalType::TimestampntzNanos => Some(19),
            PhysicalType::Uuid => Some(20),
            PhysicalType::Array | PhysicalType::Object => None,
        }
    }

    /// Returns the logical type this physical encoding represents (Java
    /// `PhysicalType.toLogicalType`).
    pub fn to_logical_type(self) -> LogicalType {
        match self {
            PhysicalType::Null => LogicalType::Null,
            PhysicalType::BooleanTrue | PhysicalType::BooleanFalse => LogicalType::Boolean,
            PhysicalType::Int8
            | PhysicalType::Int16
            | PhysicalType::Int32
            | PhysicalType::Int64
            | PhysicalType::Decimal4
            | PhysicalType::Decimal8
            | PhysicalType::Decimal16 => LogicalType::ExactNumeric,
            PhysicalType::Double => LogicalType::Double,
            PhysicalType::Float => LogicalType::Float,
            PhysicalType::Date => LogicalType::Date,
            PhysicalType::Timestamptz | PhysicalType::TimestamptzNanos => LogicalType::Timestamptz,
            PhysicalType::Timestampntz | PhysicalType::TimestampntzNanos => {
                LogicalType::Timestampntz
            }
            PhysicalType::Binary => LogicalType::Binary,
            PhysicalType::String => LogicalType::String,
            PhysicalType::Time => LogicalType::Time,
            PhysicalType::Uuid => LogicalType::Uuid,
            PhysicalType::Array => LogicalType::Array,
            PhysicalType::Object => LogicalType::Object,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Risk pinned: a transposed type-id mapping decodes EVERY value of that type as the wrong
    /// type — the full 21-entry table must match Java 1.10.0 `Primitives.TYPE_*` (javap
    /// -constants) + `PhysicalType.from(int)` exactly.
    #[test]
    fn test_physical_type_from_type_info_matches_java_1_10_0_table() {
        let expected = [
            (0, PhysicalType::Null),
            (1, PhysicalType::BooleanTrue),
            (2, PhysicalType::BooleanFalse),
            (3, PhysicalType::Int8),
            (4, PhysicalType::Int16),
            (5, PhysicalType::Int32),
            (6, PhysicalType::Int64),
            (7, PhysicalType::Double),
            (8, PhysicalType::Decimal4),
            (9, PhysicalType::Decimal8),
            (10, PhysicalType::Decimal16),
            (11, PhysicalType::Date),
            (12, PhysicalType::Timestamptz),
            (13, PhysicalType::Timestampntz),
            (14, PhysicalType::Float),
            (15, PhysicalType::Binary),
            (16, PhysicalType::String),
            (17, PhysicalType::Time),
            (18, PhysicalType::TimestamptzNanos),
            (19, PhysicalType::TimestampntzNanos),
            (20, PhysicalType::Uuid),
        ];
        for (type_info, physical_type) in expected {
            assert_eq!(
                PhysicalType::from_type_info(type_info).expect("known type id must decode"),
                physical_type,
                "type id {type_info} must map per the 1.10.0 table"
            );
        }
    }

    /// Risk pinned: a type id outside Java's 0..=20 set must be a clean error (Java throws
    /// `UnsupportedOperationException`), never a panic or a silent default — 21 is the first
    /// unknown id, 63 the largest a 6-bit header can carry.
    #[test]
    fn test_physical_type_from_type_info_rejects_unknown_ids() {
        for unknown in [21u8, 32, 63] {
            let error = PhysicalType::from_type_info(unknown)
                .expect_err("unknown type id must be rejected");
            assert_eq!(error.kind(), ErrorKind::FeatureUnsupported);
            assert!(
                error
                    .to_string()
                    .contains("Unknown primitive physical type"),
                "error must name the unknown type id, got: {error}"
            );
        }
    }

    /// Risk pinned: the basic type must come from the LOW TWO bits only (Java
    /// `header & BASIC_TYPE_MASK`) — high header bits (type info / length) must not leak into
    /// the tag.
    #[test]
    fn test_basic_type_from_header_uses_low_two_bits_only() {
        assert_eq!(BasicType::from_header(0b0000_0000), BasicType::Primitive);
        assert_eq!(BasicType::from_header(0b0000_0001), BasicType::ShortString);
        assert_eq!(BasicType::from_header(0b0000_0010), BasicType::Object);
        assert_eq!(BasicType::from_header(0b0000_0011), BasicType::Array);
        // High bits set (a 63-length short string, a large object) leave the tag intact.
        assert_eq!(BasicType::from_header(0b1111_1101), BasicType::ShortString);
        assert_eq!(BasicType::from_header(0b0100_0010), BasicType::Object);
    }

    /// Risk pinned: a transposed entry in the WRITE-side id table would emit every value of
    /// that type under the wrong header — `to_type_info` must be the exact inverse of the
    /// Java-pinned `from_type_info` for all 21 primitive ids, and `None` for containers.
    #[test]
    fn test_to_type_info_is_inverse_of_from_type_info() {
        for type_info in 0..=20u8 {
            let physical_type =
                PhysicalType::from_type_info(type_info).expect("known type id must decode");
            assert_eq!(
                physical_type.to_type_info(),
                Some(type_info),
                "{physical_type:?} must encode back to type id {type_info}"
            );
        }
        assert_eq!(PhysicalType::Array.to_type_info(), None);
        assert_eq!(PhysicalType::Object.to_type_info(), None);
    }

    /// Risk pinned: the physical→logical grouping must match Java 1.10.0's enum declarations
    /// (notably: decimals + ints are EXACT_NUMERIC; the nanos timestamps share the micros
    /// timestamps' logical types).
    #[test]
    fn test_to_logical_type_matches_java_1_10_0_groupings() {
        assert_eq!(
            PhysicalType::Decimal16.to_logical_type(),
            LogicalType::ExactNumeric
        );
        assert_eq!(
            PhysicalType::Int8.to_logical_type(),
            LogicalType::ExactNumeric
        );
        assert_eq!(
            PhysicalType::TimestamptzNanos.to_logical_type(),
            LogicalType::Timestamptz
        );
        assert_eq!(
            PhysicalType::TimestampntzNanos.to_logical_type(),
            LogicalType::Timestampntz
        );
        assert_eq!(
            PhysicalType::BooleanFalse.to_logical_type(),
            LogicalType::Boolean
        );
        assert_eq!(PhysicalType::Array.to_logical_type(), LogicalType::Array);
        assert_eq!(PhysicalType::Object.to_logical_type(), LogicalType::Object);
    }
}
