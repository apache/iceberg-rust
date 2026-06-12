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

//! Variant value decode AND encode tests: hand-built vectors per primitive layout,
//! object/array semantics, the malformed-input (security boundary) suite, the
//! Java-1.10.0-pinned fixture bytes, and the write-side byte-exactness suite.
//!
//! # Provenance of the `JAVA_*` / write-side fixture constants
//!
//! READ-side fixtures generated on 2026-06-11 by
//! `/tmp/variant-fixture-gen/VariantFixtureGen.java`, WRITE-side fixtures (the `w_*` /
//! CRC-pinned constants in the write section below) on 2026-06-11 by
//! `/tmp/variant-fixture-gen/VariantWriteFixtureGen.java` (each quoted per constant below),
//! compiled and run against the PINNED 1.10.0 jars:
//!
//! ```text
//! CP=~/.m2/repository/org/apache/iceberg/iceberg-api/1.10.0/iceberg-api-1.10.0.jar:\
//!    ~/.m2/repository/org/apache/iceberg/iceberg-core/1.10.0/iceberg-core-1.10.0.jar:\
//!    ~/.m2/repository/org/apache/iceberg/iceberg-bundled-guava/1.10.0/iceberg-bundled-guava-1.10.0.jar:\
//!    ~/.m2/repository/org/slf4j/slf4j-api/2.0.17/slf4j-api-2.0.17.jar
//! javac -encoding UTF-8 -cp "$CP" VariantFixtureGen.java && java -cp "$CP:." VariantFixtureGen
//! # the write generator additionally needs avro + caffeine on the classpath
//! # (ShreddedObject.writeTo -> SortedMerge's CloseableGroup statics):
//! CPW="$CP":~/.m2/repository/org/apache/avro/avro/1.12.0/avro-1.12.0.jar:\
//!     ~/.m2/repository/com/github/ben-manes/caffeine/caffeine/3.0.5/caffeine-3.0.5.jar
//! javac -encoding UTF-8 -cp "$CPW" VariantWriteFixtureGen.java \
//!   && java -Dfile.encoding=UTF-8 -cp "$CPW:." VariantWriteFixtureGen
//! ```
//!
//! Each value was produced by `Variants.<factory>(...)` (iceberg-core 1.10.0), serialized via
//! the public `VariantValue.writeTo` / `VariantMetadata.writeTo` into a little-endian buffer
//! of `sizeInBytes()` bytes, hex-dumped (or, for large fixtures, pinned as the
//! `java.util.zip.CRC32` checksum + total length + first-64-bytes hex), and round-trip
//! re-read by Java 1.10.0 itself (`Variants.value(metadata, bytes)` /
//! `Variants.metadata(buffer)`, asserted equal) before being pinned here.

use super::*;

/// Decodes a fixture hex string into bytes (test helper).
fn hex(hex_string: &str) -> Vec<u8> {
    assert!(
        hex_string.len().is_multiple_of(2),
        "hex fixtures have even length"
    );
    (0..hex_string.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&hex_string[index..index + 2], 16).expect("valid fixture hex")
        })
        .collect()
}

/// The empty metadata (Java `SerializedMetadata.EMPTY_V1_BUFFER`), for values that never
/// touch the dictionary.
fn empty_metadata() -> VariantMetadata {
    VariantMetadata::parse(&[0x01, 0x00, 0x00]).expect("the empty metadata must parse")
}

/// Parses a value against the empty metadata, panicking the test on error.
fn parse_ok(bytes: &[u8]) -> VariantValue {
    VariantValue::parse(&empty_metadata(), bytes).expect("test vector must parse")
}

/// Asserts the bytes decode to the given primitive.
fn assert_primitive(bytes: &[u8], expected: VariantPrimitive) {
    match parse_ok(bytes) {
        VariantValue::Primitive(primitive) => assert_eq!(primitive, expected),
        other => panic!("expected a primitive, got {other:?}"),
    }
}

/// Asserts the bytes are rejected with an error (and, by running, that they never panic).
fn assert_rejects(bytes: &[u8]) {
    assert!(
        VariantValue::parse(&empty_metadata(), bytes).is_err(),
        "malformed value {bytes:02x?} must be rejected"
    );
}

// ===== hand-built per-primitive vectors =====================================================
// Layouts per Java `SerializedPrimitive.read()` (1.10.0 bytecode-verified): header byte
// `type_info << 2`, then the payload at offset 1.

/// Risk pinned: null/true/false carry their value in the TYPE ID (ids 0/1/2, no payload) — a
/// transposed id silently flips booleans.
#[test]
fn test_primitive_null_true_false_decode_from_type_id_alone() {
    assert_primitive(&[0x00], VariantPrimitive::Null);
    assert_primitive(&[0x04], VariantPrimitive::Boolean(true));
    assert_primitive(&[0x08], VariantPrimitive::Boolean(false));
}

/// Risk pinned: integer payloads are little-endian two's complement at offset 1 — the most
/// negative values are the sign-extension/byte-order sentinels (i64::MIN's only set bit is in
/// the LAST byte).
#[test]
fn test_primitive_integers_decode_boundary_values() {
    // int8 (id 3): header 0x0C.
    assert_primitive(&[0x0C, 0x80], VariantPrimitive::Int8(i8::MIN));
    assert_primitive(&[0x0C, 0x7F], VariantPrimitive::Int8(i8::MAX));
    // int16 (id 4): header 0x10.
    assert_primitive(&[0x10, 0x00, 0x80], VariantPrimitive::Int16(i16::MIN));
    // int32 (id 5): header 0x14.
    assert_primitive(
        &[0x14, 0x00, 0x00, 0x00, 0x80],
        VariantPrimitive::Int32(i32::MIN),
    );
    // int64 (id 6): header 0x18.
    let mut int64_min = vec![0x18];
    int64_min.extend_from_slice(&i64::MIN.to_le_bytes());
    assert_primitive(&int64_min, VariantPrimitive::Int64(i64::MIN));
}

/// Risk pinned: float/double are IEEE-754 little-endian — compared at exact bit precision so
/// an encoding drift cannot hide behind float tolerance.
#[test]
fn test_primitive_float_double_decode_exact_bits() {
    let mut float_bytes = vec![0x38]; // id 14
    float_bytes.extend_from_slice(&(-1.25f32).to_le_bytes());
    match parse_ok(&float_bytes) {
        VariantValue::Primitive(VariantPrimitive::Float(value)) => {
            assert_eq!(value.to_bits(), (-1.25f32).to_bits());
        }
        other => panic!("expected a float, got {other:?}"),
    }

    let mut double_bytes = vec![0x1C]; // id 7
    double_bytes.extend_from_slice(&2.5f64.to_le_bytes());
    match parse_ok(&double_bytes) {
        VariantValue::Primitive(VariantPrimitive::Double(value)) => {
            assert_eq!(value.to_bits(), 2.5f64.to_bits());
        }
        other => panic!("expected a double, got {other:?}"),
    }
}

/// Risk pinned: decimals are a raw scale byte + a little-endian unscaled value — negative
/// unscaled values, the max scale byte (255 — Java accepts any byte, no validation), and
/// i128::MIN (decimal16's byte-order sentinel) must all survive.
#[test]
fn test_primitive_decimals_decode_negative_and_max_scale_and_i128_min() {
    // decimal4 (id 8): header 0x20; scale 255 (Java reads the raw byte, never validates).
    let mut decimal4 = vec![0x20, 0xFF];
    decimal4.extend_from_slice(&(-7i32).to_le_bytes());
    assert_primitive(&decimal4, VariantPrimitive::Decimal4 {
        scale: 255,
        unscaled: -7,
    });

    // decimal8 (id 9): header 0x24.
    let mut decimal8 = vec![0x24, 0x09];
    decimal8.extend_from_slice(&i64::MIN.to_le_bytes());
    assert_primitive(&decimal8, VariantPrimitive::Decimal8 {
        scale: 9,
        unscaled: i64::MIN,
    });

    // decimal16 (id 10): header 0x28; 16 little-endian payload bytes — Java reverses them
    // into a big-endian BigInteger (`SerializedPrimitive.read`, offsets 17 down to 2), which
    // is exactly i128::from_le_bytes.
    let mut decimal16 = vec![0x28, 0x26];
    decimal16.extend_from_slice(&i128::MIN.to_le_bytes());
    assert_primitive(&decimal16, VariantPrimitive::Decimal16 {
        scale: 38,
        unscaled: i128::MIN,
    });
}

/// Risk pinned: date is an i32 day ordinal, the temporal types are i64 — all signed (pre-epoch
/// values are negative), each with its own type id (a transposed id mislabels micros as
/// nanos, a silent 1000x error).
#[test]
fn test_primitive_temporal_types_decode_signed_values() {
    let mut date = vec![0x2C]; // id 11
    date.extend_from_slice(&(-3000i32).to_le_bytes());
    assert_primitive(&date, VariantPrimitive::Date(-3000));

    let mut timestamptz = vec![0x30]; // id 12
    timestamptz.extend_from_slice(&(-1_000_000i64).to_le_bytes());
    assert_primitive(&timestamptz, VariantPrimitive::Timestamptz(-1_000_000));

    let mut timestampntz = vec![0x34]; // id 13
    timestampntz.extend_from_slice(&7i64.to_le_bytes());
    assert_primitive(&timestampntz, VariantPrimitive::Timestampntz(7));

    let mut time = vec![0x44]; // id 17
    time.extend_from_slice(&86_399_999_999i64.to_le_bytes());
    assert_primitive(&time, VariantPrimitive::Time(86_399_999_999));

    let mut timestamptz_nanos = vec![0x48]; // id 18
    timestamptz_nanos.extend_from_slice(&(-5i64).to_le_bytes());
    assert_primitive(&timestamptz_nanos, VariantPrimitive::TimestamptzNanos(-5));

    let mut timestampntz_nanos = vec![0x4C]; // id 19
    timestampntz_nanos.extend_from_slice(&9i64.to_le_bytes());
    assert_primitive(&timestampntz_nanos, VariantPrimitive::TimestampntzNanos(9));
}

/// Risk pinned: the UUID payload is stored big-endian (RFC 4122) and must come back byte-for-
/// byte — any reordering scrambles every UUID read.
#[test]
fn test_primitive_uuid_preserves_stored_byte_order() {
    let uuid_bytes: [u8; 16] = [
        0xF2, 0x4F, 0x9B, 0x64, 0x81, 0xFA, 0x49, 0xD1, 0xB7, 0x4E, 0x8C, 0x09, 0xA6, 0xE3, 0x1C,
        0x56,
    ];
    let mut value = vec![0x50]; // id 20
    value.extend_from_slice(&uuid_bytes);
    assert_primitive(&value, VariantPrimitive::Uuid(uuid_bytes));
}

/// Risk pinned: binary and long-form string carry an i32 length at offset 1 and the payload at
/// offset 5 — including the empty cases (length 0).
#[test]
fn test_primitive_binary_and_long_string_decode_including_empty() {
    // binary (id 15): header 0x3C.
    let mut binary = vec![0x3C];
    binary.extend_from_slice(&4u32.to_le_bytes());
    binary.extend_from_slice(&[0x0A, 0x0B, 0x0C, 0x0D]);
    assert_primitive(
        &binary,
        VariantPrimitive::Binary(vec![0x0A, 0x0B, 0x0C, 0x0D]),
    );

    let mut empty_binary = vec![0x3C];
    empty_binary.extend_from_slice(&0u32.to_le_bytes());
    assert_primitive(&empty_binary, VariantPrimitive::Binary(vec![]));

    // string (id 16): header 0x40.
    let mut string = vec![0x40];
    string.extend_from_slice(&7u32.to_le_bytes());
    string.extend_from_slice(b"iceberg");
    assert_primitive(&string, VariantPrimitive::String("iceberg".to_string()));

    let mut empty_string = vec![0x40];
    empty_string.extend_from_slice(&0u32.to_le_bytes());
    assert_primitive(&empty_string, VariantPrimitive::String(String::new()));
}

/// Risk pinned: the short-string length is the high 6 header bits — empty (0), 1, and the
/// 63-byte maximum are the mask/shift boundary cases, and multi-byte UTF-8 must survive.
#[test]
fn test_short_string_lengths_empty_one_and_max_63() {
    // length 0: header 0b000001.
    assert_primitive(&[0x01], VariantPrimitive::String(String::new()));
    // length 1: header (1 << 2) | 1 = 0x05.
    assert_primitive(&[0x05, b'x'], VariantPrimitive::String("x".to_string()));
    // length 63 (the 6-bit max): header (63 << 2) | 1 = 0xFD.
    let body = "y".repeat(63);
    let mut value = vec![0xFD];
    value.extend_from_slice(body.as_bytes());
    assert_primitive(&value, VariantPrimitive::String(body));
    // multi-byte UTF-8 (6 bytes, 2 chars).
    let mut utf8 = vec![(6 << 2) | 1];
    utf8.extend_from_slice("日本".as_bytes());
    assert_primitive(&utf8, VariantPrimitive::String("日本".to_string()));
}

// ===== malformed-input suite (the security boundary) ========================================
// Every case must return Err — and by RUNNING these, a panic in any of them fails the test.

/// Risk pinned: the degenerate empty input must error cleanly at the header read.
#[test]
fn test_empty_value_rejects() {
    assert_rejects(&[]);
}

/// Risk pinned: a truncated payload for EVERY fixed-size primitive must be a clean error —
/// these are the exact reads that would be out-of-bounds panics if unchecked.
#[test]
fn test_primitive_truncated_payloads_reject() {
    // header-only for every payload-carrying type id.
    for type_id in [3u8, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 17, 18, 19, 20] {
        assert_rejects(&[type_id << 2]);
    }
    // one byte short of each fixed payload.
    assert_rejects(&[0x10, 0x01]); // int16 with 1 of 2 bytes
    assert_rejects(&[0x14, 0x01, 0x02, 0x03]); // int32 with 3 of 4
    assert_rejects(&[0x18, 0, 0, 0, 0, 0, 0, 0]); // int64 with 7 of 8
    assert_rejects(&[0x28, 0x05, 0, 0, 0, 0, 0, 0, 0, 0, 0]); // decimal16 with 9 of 16
    assert_rejects(&[0x50, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9]); // uuid with 10 of 16
    // binary/string: a length prefix but a short payload.
    let mut binary = vec![0x3C];
    binary.extend_from_slice(&10u32.to_le_bytes());
    binary.extend_from_slice(&[1, 2, 3]);
    assert_rejects(&binary);
}

/// Risk pinned: type ids above Java's 0..=20 set (a future spec or garbage) must error, never
/// decode as something else.
#[test]
fn test_unknown_primitive_type_id_rejects() {
    assert_rejects(&[21 << 2]);
    assert_rejects(&[63 << 2, 0x00]);
}

/// Risk pinned: a hostile binary/string length — negative in Java's signed domain
/// (0x8000_0000) or absurdly larger than the buffer — must fail fast by name, with no
/// allocation sized from the untrusted value.
#[test]
fn test_string_and_binary_hostile_lengths_reject() {
    for header in [0x3Cu8, 0x40] {
        let mut negative = vec![header];
        negative.extend_from_slice(&0x8000_0000u32.to_le_bytes());
        negative.extend_from_slice(&[0u8; 8]);
        assert_rejects(&negative);

        let mut absurd = vec![header];
        absurd.extend_from_slice(&(i32::MAX as u32).to_le_bytes());
        absurd.extend_from_slice(&[0u8; 8]);
        assert_rejects(&absurd);
    }
}

/// Risk pinned: a short string whose 6-bit length exceeds the actual bytes must error.
#[test]
fn test_short_string_truncated_rejects() {
    assert_rejects(&[(5 << 2) | 1, b'a', b'b']);
}

/// Risk pinned (documented divergence): invalid UTF-8 in string payloads errors loudly here
/// (Java silently substitutes U+FFFD).
#[test]
fn test_invalid_utf8_in_strings_rejects() {
    // short string, 2 bytes of invalid UTF-8.
    assert_rejects(&[(2 << 2) | 1, 0xC3, 0x28]);
    // long string, same payload.
    let mut long = vec![0x40];
    long.extend_from_slice(&2u32.to_le_bytes());
    long.extend_from_slice(&[0xC3, 0x28]);
    assert_rejects(&long);
}

// ===== objects ===============================================================================
// Layout per Java `SerializedObject` (1.10.0): header bits — offset size (bits 2..4), field-id
// size (bits 4..6), is-large (bit 6) — then the field count, the field-id list, the offset
// list (one extra entry = data length), then the concatenated field values.

/// Builds metadata with the given dictionary, sorted-flagged, offset size 1 (test helper).
fn metadata_with(names: &[&str]) -> VariantMetadata {
    let bytes = crate::variant::metadata::tests::encode_metadata(names, 1, true);
    VariantMetadata::parse(&bytes).expect("test dictionary must parse")
}

/// Risk pinned: the empty object (0 fields, only the data-length offset entry) must decode —
/// it is the `{}` of every sparse row.
#[test]
fn test_object_empty_decodes() {
    let object_bytes = [0x02u8, 0x00, 0x00];
    let value = VariantValue::parse(&metadata_with(&[]), &object_bytes).expect("empty object");
    let object = value.as_object().expect("must be an object");
    assert_eq!(object.num_fields(), 0);
    assert_eq!(object.get("a"), None);
    assert_eq!(value.physical_type(), PhysicalType::Object);
}

/// Risk pinned: the single-field happy path plus the `get` hit/miss contract (Java returns
/// null on a miss → `None` here).
#[test]
fn test_object_single_field_get_hit_and_miss() {
    // {a: int8 34}: header 0x02, count 1, field ids [0], offsets [0, 2], data 0x0C 0x22.
    let object_bytes = [0x02u8, 0x01, 0x00, 0x00, 0x02, 0x0C, 0x22];
    let metadata = metadata_with(&["a"]);
    let value = VariantValue::parse(&metadata, &object_bytes).expect("must parse");
    let object = value.as_object().expect("must be an object");
    assert_eq!(object.num_fields(), 1);
    assert_eq!(
        object.get("a"),
        Some(&VariantValue::Primitive(VariantPrimitive::Int8(34)))
    );
    assert_eq!(object.get("missing"), None);
    assert_eq!(object.fields()[0].field_id, 0);
    assert_eq!(object.fields()[0].name, "a");
}

/// Risk pinned: the non-trivial header combination — is-large (4-byte count), 2-byte field
/// ids, 2-byte offsets — exercises every size field's mask/shift at once; a wrong shift
/// misreads all of them.
#[test]
fn test_object_large_with_two_byte_ids_and_offsets_decodes() {
    // header: is_large | (field_id_size 2 - 1) << 4 | (offset_size 2 - 1) << 2 | 0b10 = 0x56.
    let mut object_bytes = vec![0x56u8];
    object_bytes.extend_from_slice(&2u32.to_le_bytes()); // count (4 bytes, is_large)
    object_bytes.extend_from_slice(&0u16.to_le_bytes()); // field id "a"
    object_bytes.extend_from_slice(&1u16.to_le_bytes()); // field id "b"
    object_bytes.extend_from_slice(&0u16.to_le_bytes()); // offset of a
    object_bytes.extend_from_slice(&2u16.to_le_bytes()); // offset of b
    object_bytes.extend_from_slice(&10u16.to_le_bytes()); // data length
    object_bytes.extend_from_slice(&[0x0C, 0xDE]); // a: int8 -34
    object_bytes.extend_from_slice(&[(7 << 2) | 1]); // b: short string "iceberg"
    object_bytes.extend_from_slice(b"iceberg");

    let metadata = metadata_with(&["a", "b"]);
    let value = VariantValue::parse(&metadata, &object_bytes).expect("must parse");
    let object = value.as_object().expect("must be an object");
    assert_eq!(object.num_fields(), 2);
    assert_eq!(
        object.get("a"),
        Some(&VariantValue::Primitive(VariantPrimitive::Int8(-34)))
    );
    assert_eq!(
        object.get("b"),
        Some(&VariantValue::Primitive(VariantPrimitive::String(
            "iceberg".to_string()
        )))
    );
    assert_eq!(object.field_names().collect::<Vec<_>>(), vec!["a", "b"]);
}

/// Risk pinned: object field values are located by SORTED-DISTINCT offset spans (Java
/// `initOffsetsAndLengths`), not consecutive entries — fields whose data order differs from
/// name order must still decode correctly.
#[test]
fn test_object_field_data_order_differs_from_field_order() {
    // Fields in name order [a, b], but a's bytes AFTER b's: offsets a=8, b=0.
    // data: b = "iceberg" (8 bytes), a = int8 34 (2 bytes); data length 10.
    let object_bytes = [
        0x02u8,
        0x02,
        0x00,
        0x01,
        0x08,
        0x00,
        0x0A, // header, count, ids, offsets [8, 0, 10]
        (7 << 2) | 1,
        b'i',
        b'c',
        b'e',
        b'b',
        b'e',
        b'r',
        b'g', // b at 0
        0x0C,
        0x22, // a at 8
    ];
    let metadata = metadata_with(&["a", "b"]);
    let value = VariantValue::parse(&metadata, &object_bytes).expect("must parse");
    let object = value.as_object().expect("must be an object");
    assert_eq!(
        object.get("a"),
        Some(&VariantValue::Primitive(VariantPrimitive::Int8(34)))
    );
    assert_eq!(
        object.get("b"),
        Some(&VariantValue::Primitive(VariantPrimitive::String(
            "iceberg".to_string()
        )))
    );
}

/// Risk pinned (Java-miss parity): `get(name)` binary-searches assuming name-sorted fields —
/// on a NON-conforming object Java 1.10.0 misses some present fields, and a "helpful" linear
/// fallback here would diverge by finding them.
#[test]
fn test_object_get_on_unsorted_fields_misses_exactly_like_java() {
    // Fields stored [b, a] (NOT name-sorted): ids [1, 0], values int8 1 / int8 2.
    let object_bytes = [
        0x02u8, 0x02, 0x01, 0x00, 0x00, 0x02, 0x04, 0x0C, 0x01, 0x0C, 0x02,
    ];
    let metadata = metadata_with(&["a", "b"]);
    let value = VariantValue::parse(&metadata, &object_bytes).expect("must parse");
    let object = value.as_object().expect("must be an object");
    // Java's probe for "a" over names [b, a]: mid 0 -> "b", "a" < "b" -> miss.
    assert_eq!(object.get("a"), None, "Java's binary search misses here");
    // Java's probe for "b": mid 0 -> "b" -> hit.
    assert!(object.get("b").is_some());
    // The field IS present in the decoded structure (only the lookup mirrors Java's miss).
    assert_eq!(object.fields()[1].name, "a");
}

/// Risk pinned: nesting object → array → object must decode through the recursion path with
/// names resolved at every level.
#[test]
fn test_object_array_object_nesting_decodes() {
    let metadata = metadata_with(&["a", "b", "c"]);
    // inner object {c: int8 9}: 02 01 02 00 02 0c 09 (7 bytes)
    let inner_object = [0x02u8, 0x01, 0x02, 0x00, 0x02, 0x0C, 0x09];
    // array [inner_object, short "x"]: 03 02 00 07 09 <inner> 05 78 (14 bytes)
    let mut array = vec![0x03u8, 0x02, 0x00, 0x07, 0x09];
    array.extend_from_slice(&inner_object);
    array.extend_from_slice(&[0x05, b'x']);
    // outer object {a: array, b: double 4.5}
    let mut outer = vec![0x02u8, 0x02, 0x00, 0x01, 0x00, 0x0E, 0x17];
    outer.extend_from_slice(&array);
    outer.push(0x1C);
    outer.extend_from_slice(&4.5f64.to_le_bytes());

    let value = VariantValue::parse(&metadata, &outer).expect("nested value must parse");
    let object = value.as_object().expect("outer object");
    let array_value = object.get("a").expect("field a").as_array().expect("array");
    assert_eq!(array_value.num_elements(), 2);
    let inner = array_value
        .get(0)
        .expect("element 0")
        .as_object()
        .expect("inner object");
    assert_eq!(
        inner.get("c"),
        Some(&VariantValue::Primitive(VariantPrimitive::Int8(9)))
    );
    assert_eq!(
        array_value.get(1),
        Some(&VariantValue::Primitive(VariantPrimitive::String(
            "x".to_string()
        )))
    );
    assert_eq!(
        object.get("b"),
        Some(&VariantValue::Primitive(VariantPrimitive::Double(4.5)))
    );
}

/// Risk pinned (malformed): a field id outside the dictionary must error at parse (Java
/// throws on access) — a wrong-but-in-range id would silently rename a field, so the id is
/// validated against the dictionary, not clamped.
#[test]
fn test_object_field_id_past_dictionary_rejects() {
    // {<id 5>: int8 1} against a 1-entry dictionary.
    let object_bytes = [0x02u8, 0x01, 0x05, 0x00, 0x02, 0x0C, 0x01];
    let metadata = metadata_with(&["a"]);
    assert!(VariantValue::parse(&metadata, &object_bytes).is_err());
}

/// Risk pinned (malformed): duplicate field offsets break Java's sorted-distinct length
/// scheme (its `sortedOffsets.get(index + 1)` throws) — rejected by name here.
#[test]
fn test_object_duplicate_field_offsets_reject() {
    // 2 fields, both at offset 0, data length 2.
    let object_bytes = [0x02u8, 0x02, 0x00, 0x01, 0x00, 0x00, 0x02, 0x0C, 0x01];
    let metadata = metadata_with(&["a", "b"]);
    let error = VariantValue::parse(&metadata, &object_bytes).expect_err("duplicate offsets");
    assert!(
        error.to_string().contains("duplicate field offsets"),
        "error must name the duplicates, got: {error}"
    );
}

/// Risk pinned (malformed): a declared data length running past the end of the value must
/// error at the field slice, not read out of bounds.
#[test]
fn test_object_field_range_past_end_rejects() {
    // 1 field at offset 0, data length 9, but only 2 data bytes follow.
    let object_bytes = [0x02u8, 0x01, 0x00, 0x00, 0x09, 0x0C, 0x01];
    let metadata = metadata_with(&["a"]);
    assert!(VariantValue::parse(&metadata, &object_bytes).is_err());
}

/// Risk pinned (malformed/DoS): an is-large object declaring i32::MAX fields over a tiny
/// buffer must fail FAST on the header-region bound — before any allocation sized from the
/// untrusted count.
#[test]
fn test_object_absurd_field_count_rejects_fast() {
    let mut object_bytes = vec![0x42u8]; // is_large, 1-byte ids, 1-byte offsets
    object_bytes.extend_from_slice(&(i32::MAX as u32).to_le_bytes());
    object_bytes.extend_from_slice(&[0u8; 16]);
    let metadata = metadata_with(&["a"]);
    assert!(VariantValue::parse(&metadata, &object_bytes).is_err());
}

// ===== arrays ================================================================================
// Layout per Java `SerializedArray` (1.10.0): header bits — offset size (bits 2..4), is-large
// (bit 4) — then the element count and `count + 1` offsets delimiting consecutive elements.

/// Risk pinned: the empty array must decode (count 0, single offset entry).
#[test]
fn test_array_empty_decodes() {
    let value = parse_ok(&[0x03, 0x00, 0x00]);
    let array = value.as_array().expect("must be an array");
    assert_eq!(array.num_elements(), 0);
    assert_eq!(array.get(0), None);
    assert_eq!(value.physical_type(), PhysicalType::Array);
}

/// Risk pinned: mixed-type elements with consecutive offsets, plus the out-of-range `get`
/// contract (Java throws unchecked; `None` here).
#[test]
fn test_array_mixed_types_and_out_of_range_get() {
    // [int8 -34, "iceberg", null, true]: offsets [0, 2, 10, 11, 12].
    let mut array_bytes = vec![0x03u8, 0x04, 0x00, 0x02, 0x0A, 0x0B, 0x0C];
    array_bytes.extend_from_slice(&[0x0C, 0xDE]);
    array_bytes.push((7 << 2) | 1);
    array_bytes.extend_from_slice(b"iceberg");
    array_bytes.extend_from_slice(&[0x00, 0x04]);

    let value = parse_ok(&array_bytes);
    let array = value.as_array().expect("must be an array");
    assert_eq!(array.num_elements(), 4);
    assert_eq!(
        array.get(0),
        Some(&VariantValue::Primitive(VariantPrimitive::Int8(-34)))
    );
    assert_eq!(
        array.get(1),
        Some(&VariantValue::Primitive(VariantPrimitive::String(
            "iceberg".to_string()
        )))
    );
    assert_eq!(
        array.get(2),
        Some(&VariantValue::Primitive(VariantPrimitive::Null))
    );
    assert_eq!(
        array.get(3),
        Some(&VariantValue::Primitive(VariantPrimitive::Boolean(true)))
    );
    assert_eq!(
        array.get(4),
        None,
        "out of range is None, like Java's throw"
    );
}

/// Risk pinned (malformed): descending array offsets (Java's `next - offset` slice would be
/// negative) must reject by name, never wrap.
#[test]
fn test_array_descending_offsets_reject() {
    // 2 elements, offsets [2, 0, 4].
    let array_bytes = [0x03u8, 0x02, 0x02, 0x00, 0x04, 0x0C, 0x01, 0x0C, 0x02];
    assert_rejects(&array_bytes);
}

/// Risk pinned (malformed): an element span past the end of the buffer must error at the
/// slice bound.
#[test]
fn test_array_element_past_end_rejects() {
    // 1 element, offsets [0, 9], but only 2 data bytes.
    let array_bytes = [0x03u8, 0x01, 0x00, 0x09, 0x0C, 0x01];
    assert_rejects(&array_bytes);
}

/// Risk pinned (malformed/DoS): an is-large array declaring i32::MAX elements over a tiny
/// buffer fails fast on the header-region bound, before any allocation.
#[test]
fn test_array_absurd_element_count_rejects_fast() {
    let mut array_bytes = vec![0x13u8]; // is_large array, 1-byte offsets
    array_bytes.extend_from_slice(&(i32::MAX as u32).to_le_bytes());
    array_bytes.extend_from_slice(&[0u8; 16]);
    assert_rejects(&array_bytes);
}

/// Risk pinned (DoS — the explicit recursion guard): nesting at exactly
/// [`MAX_NESTING_DEPTH`] parses; one deeper is rejected, NOT a stack overflow. Each wrapper
/// is a 1-element array with 2-byte offsets.
#[test]
fn test_nesting_depth_guard_boundary() {
    // header 0b0111: array, offset_size 2, not large.
    let wrap = |inner: &[u8]| -> Vec<u8> {
        let mut wrapped = vec![0x07u8, 0x01];
        wrapped.extend_from_slice(&0u16.to_le_bytes());
        wrapped.extend_from_slice(&(inner.len() as u16).to_le_bytes());
        wrapped.extend_from_slice(inner);
        wrapped
    };

    // MAX_NESTING_DEPTH wrappers put the innermost null exactly AT the depth limit.
    let mut at_limit = vec![0x00u8];
    for _ in 0..MAX_NESTING_DEPTH {
        at_limit = wrap(&at_limit);
    }
    assert!(
        VariantValue::parse(&empty_metadata(), &at_limit).is_ok(),
        "nesting at the limit must parse"
    );

    let beyond = wrap(&at_limit);
    let error = VariantValue::parse(&empty_metadata(), &beyond)
        .expect_err("nesting beyond the limit must be rejected");
    assert!(
        error.to_string().contains("nesting depth"),
        "error must name the depth guard, got: {error}"
    );
}

/// Risk pinned (Java parity): trailing bytes after a top-level value are IGNORED — Java's
/// lazy reads never touch them, and rejecting them would refuse buffers Java accepts.
#[test]
fn test_top_level_trailing_bytes_tolerated_like_java() {
    let value = parse_ok(&[0x0C, 0x22, 0xDE, 0xAD, 0xBE, 0xEF]);
    assert_eq!(value, VariantValue::Primitive(VariantPrimitive::Int8(34)));
}

/// Risk pinned: the `as_*` accessors mirror Java's `asPrimitive`/`asObject`/`asArray` throws
/// — the wrong kind is an error, not a panic or a silent None.
#[test]
fn test_as_accessors_reject_wrong_kind() {
    let primitive = parse_ok(&[0x00]);
    assert!(primitive.as_object().is_err());
    assert!(primitive.as_array().is_err());
    assert!(primitive.as_primitive().is_ok());

    let array = parse_ok(&[0x03, 0x00, 0x00]);
    assert!(array.as_primitive().is_err());
    assert!(array.as_object().is_err());
    assert!(array.as_array().is_ok());
}

// ===== Variant (metadata + value) ============================================================

/// Risk pinned: `Variant::from_bytes` must slice the value at the metadata's TRUE end (Java
/// `Variant.from` slices at `metadata.sizeInBytes()`) — an off-by-one reads the value header
/// out of the dictionary bytes.
#[test]
fn test_variant_from_bytes_concatenated_metadata_then_value() {
    let metadata_bytes = crate::variant::metadata::tests::encode_metadata(&["a"], 1, true);
    let object_bytes = [0x02u8, 0x01, 0x00, 0x00, 0x02, 0x0C, 0x22];
    let mut buffer = metadata_bytes.clone();
    buffer.extend_from_slice(&object_bytes);

    let variant = Variant::from_bytes(&buffer).expect("concatenated variant must parse");
    assert_eq!(variant.metadata().dictionary_size(), 1);
    let object = variant.value().as_object().expect("object value");
    assert_eq!(
        object.get("a"),
        Some(&VariantValue::Primitive(VariantPrimitive::Int8(34)))
    );

    // The truncated buffer (metadata only — no value) must error, not panic.
    assert!(Variant::from_bytes(&metadata_bytes).is_err());
}

// ===== Java 1.10.0 pinned fixtures ===========================================================
// See the module doc for generation provenance. Every constant is the EXACT byte output of
// iceberg 1.10.0, so these pin byte-level decode compatibility with the Java implementation.

/// `Variants.emptyMetadata()` → `writeTo` (1.10.0).
const JAVA_METADATA_EMPTY: &str = "010000";
/// `Variants.metadata("a", "b", "c")` → `writeTo` (1.10.0) — sorted, offset size 1.
const JAVA_METADATA_ABC: &str = "110300010203616263";

/// Risk pinned: metadata bytes as Java 1.10.0 actually writes them (header flags included)
/// must decode with the same dictionary and lookup behavior.
#[test]
fn test_java_fixture_metadata_decodes() {
    let empty = VariantMetadata::parse(&hex(JAVA_METADATA_EMPTY)).expect("empty metadata");
    assert_eq!(empty.dictionary_size(), 0);
    assert_eq!(empty.size_in_bytes(), 3);

    let abc = VariantMetadata::parse(&hex(JAVA_METADATA_ABC)).expect("abc metadata");
    assert!(abc.is_sorted(), "1.10.0 writes the sorted flag");
    assert_eq!(abc.dictionary_size(), 3);
    assert_eq!(abc.get(0).expect("id 0"), "a");
    assert_eq!(abc.get(2).expect("id 2"), "c");
    assert_eq!(abc.id("b"), Some(1));
    assert_eq!(abc.size_in_bytes(), hex(JAVA_METADATA_ABC).len());
}

/// Risk pinned: every 1.10.0 primitive physical type decodes from Java's EXACT bytes to the
/// expected value — the cross-implementation decode contract, one (fixture, expected) pair
/// per type id. Provenance: each hex is the `writeTo` output of the quoted `Variants` call.
#[test]
fn test_java_fixture_primitives_decode() {
    let cases: Vec<(&str, &str, VariantPrimitive)> = vec![
        // Variants.ofNull()
        ("primitive_null", "00", VariantPrimitive::Null),
        // Variants.of(true)
        ("primitive_true", "04", VariantPrimitive::Boolean(true)),
        // Variants.of(false)
        ("primitive_false", "08", VariantPrimitive::Boolean(false)),
        // Variants.of((byte) -34)
        ("primitive_int8", "0cde", VariantPrimitive::Int8(-34)),
        // Variants.of((short) -1234)
        ("primitive_int16", "102efb", VariantPrimitive::Int16(-1234)),
        // Variants.of(-12345678)
        (
            "primitive_int32",
            "14b29e43ff",
            VariantPrimitive::Int32(-12345678),
        ),
        // Variants.of(Long.MIN_VALUE)
        (
            "primitive_int64_min",
            "180000000000000080",
            VariantPrimitive::Int64(i64::MIN),
        ),
        // Variants.of(-1.25f)
        (
            "primitive_float",
            "380000a0bf",
            VariantPrimitive::Float(-1.25),
        ),
        // Variants.of(2.5d)
        (
            "primitive_double",
            "1c0000000000000440",
            VariantPrimitive::Double(2.5),
        ),
        // Variants.of(new BigDecimal("-123.4567"))
        (
            "primitive_decimal4",
            "20047929edff",
            VariantPrimitive::Decimal4 {
                scale: 4,
                unscaled: -1234567,
            },
        ),
        // Variants.of(new BigDecimal("-12345678.901234567"))
        (
            "primitive_decimal8",
            "240979b494a2ab23d4ff",
            VariantPrimitive::Decimal8 {
                scale: 9,
                unscaled: -12345678901234567,
            },
        ),
        // Variants.of(new BigDecimal("-9876543210.123456789123456789012345678"))
        (
            "primitive_decimal16",
            "281bb20c9b7ac45ac2fef7a1c7ecd2d891f8",
            VariantPrimitive::Decimal16 {
                scale: 27,
                unscaled: -9876543210123456789123456789012345678,
            },
        ),
        // Variants.ofIsoDate("2024-11-07")
        (
            "primitive_date",
            "2c424e0000",
            VariantPrimitive::Date(20034),
        ),
        // Variants.ofIsoTimestamptz("2024-11-07T12:33:54.123456+00:00")
        (
            "primitive_timestamptz",
            "30c0b2f0d851260600",
            VariantPrimitive::Timestamptz(1730982834123456),
        ),
        // Variants.ofIsoTimestampntz("2024-11-07T12:33:54.123456")
        (
            "primitive_timestampntz",
            "34c0b2f0d851260600",
            VariantPrimitive::Timestampntz(1730982834123456),
        ),
        // Variants.ofIsoTime("12:33:54.123456")
        (
            "primitive_time",
            "44c0f229880a000000",
            VariantPrimitive::Time(45234123456),
        ),
        // Variants.ofIsoTimestamptzNanos("2024-11-07T12:33:54.123456789+00:00")
        (
            "primitive_timestamptz_nanos",
            "4815413a6cb7af0518",
            VariantPrimitive::TimestamptzNanos(1730982834123456789),
        ),
        // Variants.ofIsoTimestampntzNanos("2024-11-07T12:33:54.123456789")
        (
            "primitive_timestampntz_nanos",
            "4c15413a6cb7af0518",
            VariantPrimitive::TimestampntzNanos(1730982834123456789),
        ),
        // Variants.ofUUID("f24f9b64-81fa-49d1-b74e-8c09a6e31c56")
        (
            "primitive_uuid",
            "50f24f9b6481fa49d1b74e8c09a6e31c56",
            VariantPrimitive::Uuid([
                0xF2, 0x4F, 0x9B, 0x64, 0x81, 0xFA, 0x49, 0xD1, 0xB7, 0x4E, 0x8C, 0x09, 0xA6, 0xE3,
                0x1C, 0x56,
            ]),
        ),
        // Variants.of(ByteBuffer.wrap(new byte[] {0x0a, 0x0b, 0x0c, 0x0d}))
        (
            "primitive_binary",
            "3c040000000a0b0c0d",
            VariantPrimitive::Binary(vec![0x0A, 0x0B, 0x0C, 0x0D]),
        ),
        // Variants.of("iceberg") — 7 chars, written as a SHORT string by 1.10.0
        (
            "primitive_short_string",
            "1d69636562657267",
            VariantPrimitive::String("iceberg".to_string()),
        ),
        // Variants.of("x".repeat(70)) — 70 chars, written as a LONG string
        (
            "primitive_long_string",
            "404600000078787878787878787878787878787878787878787878787878787878787878787878\
             787878787878787878787878787878787878787878787878787878787878787878787878787878\
             7878",
            VariantPrimitive::String("x".repeat(70)),
        ),
    ];
    for (name, fixture_hex, expected) in cases {
        let bytes = hex(&fixture_hex.replace(char::is_whitespace, ""));
        match VariantValue::parse(&empty_metadata(), &bytes) {
            Ok(VariantValue::Primitive(primitive)) => {
                assert_eq!(primitive, expected, "fixture {name} decoded wrong");
            }
            other => panic!("fixture {name} must decode to a primitive, got {other:?}"),
        }
    }
}

/// Risk pinned: an OBJECT as 1.10.0 writes it (`Variants.object(metadata)` with
/// `put("a", Variants.of((byte) -34))`, `put("b", Variants.of("iceberg"))`, against
/// `Variants.metadata("a", "b", "c")`) decodes with the right ids, names, and values.
#[test]
fn test_java_fixture_object_decodes() {
    let metadata = VariantMetadata::parse(&hex(JAVA_METADATA_ABC)).expect("abc metadata");
    let value = VariantValue::parse(&metadata, &hex("0202000100020a0cde1d69636562657267"))
        .expect("object fixture must parse");
    let object = value.as_object().expect("must be an object");
    assert_eq!(object.num_fields(), 2);
    assert_eq!(
        object.get("a"),
        Some(&VariantValue::Primitive(VariantPrimitive::Int8(-34)))
    );
    assert_eq!(
        object.get("b"),
        Some(&VariantValue::Primitive(VariantPrimitive::String(
            "iceberg".to_string()
        )))
    );
    assert_eq!(
        object.get("c"),
        None,
        "in the dictionary but not the object"
    );

    // The Java `Variant.from(ByteBuffer)` layout: metadata immediately followed by the value.
    let mut concatenated = hex(JAVA_METADATA_ABC);
    concatenated.extend_from_slice(&hex("0202000100020a0cde1d69636562657267"));
    let variant = Variant::from_bytes(&concatenated).expect("concatenated Java bytes");
    assert_eq!(variant.value().physical_type(), PhysicalType::Object);
}

/// Risk pinned: an ARRAY as 1.10.0 writes it (`Variants.array()` with int8 -34, "iceberg",
/// null, true) decodes element-for-element in order.
#[test]
fn test_java_fixture_array_decodes() {
    let metadata = VariantMetadata::parse(&hex(JAVA_METADATA_ABC)).expect("abc metadata");
    let value = VariantValue::parse(&metadata, &hex("030400020a0b0c0cde1d696365626572670004"))
        .expect("array fixture must parse");
    let array = value.as_array().expect("must be an array");
    assert_eq!(array.num_elements(), 4);
    assert_eq!(
        array.get(0),
        Some(&VariantValue::Primitive(VariantPrimitive::Int8(-34)))
    );
    assert_eq!(
        array.get(1),
        Some(&VariantValue::Primitive(VariantPrimitive::String(
            "iceberg".to_string()
        )))
    );
    assert_eq!(
        array.get(2),
        Some(&VariantValue::Primitive(VariantPrimitive::Null))
    );
    assert_eq!(
        array.get(3),
        Some(&VariantValue::Primitive(VariantPrimitive::Boolean(true)))
    );
}

// ===== Java 1.10.0 reviewer probes =========================================================
// Bytes below were generated by / fed to Java 1.10.0 via /tmp/variant-probe/VariantProbe.java
// (same classpath as the fixture generator, 2026-06-11); each test quotes the observed Java
// behavior it pins.

/// Risk pinned (THE UTF-16 comparator trap, Java-generated bytes): Java's writer sorts object
/// fields by `String.compareTo` — UTF-16 code units — so the supplementary 😀 (U+1F600,
/// surrogate D83D) sorts BEFORE the BMP U+FFFF. `Variants.object` on 1.10.0 wrote the fields
/// in that order, and its `get` found both (probe p1: get_bmp=1, get_supp=2). A byte-order
/// comparator would probe left at "😀" and silently MISS U+FFFF — the corruption class this
/// module exists to avoid.
#[test]
fn test_java_fixture_object_utf16_field_order_lookup_finds_both_names() {
    // Variants.metadata("\u{FFFF}", "\u{1F600}") — 1.10.0 wrote it UNSORTED (insertion order:
    // the input is not compareTo-sorted), dictionary [U+FFFF, U+1F600].
    let metadata = VariantMetadata::parse(&hex("0102000307efbfbff09f9880"))
        .expect("Java-written metadata must parse");
    assert!(!metadata.is_sorted());
    assert_eq!(metadata.id("\u{FFFF}"), Some(0));
    assert_eq!(metadata.id("\u{1F600}"), Some(1));

    // Object {U+FFFF: int8 1, U+1F600: int8 2} — field order on disk is [😀, ￿] (UTF-16).
    let value = VariantValue::parse(&metadata, &hex("020201000002040c020c01"))
        .expect("Java-written object must parse");
    let object = value.as_object().expect("must be an object");
    assert_eq!(
        object.field_names().collect::<Vec<_>>(),
        vec!["\u{1F600}", "\u{FFFF}"],
        "Java sorts fields in UTF-16 order: supplementary below U+FFFF"
    );
    assert_eq!(
        object.get("\u{FFFF}"),
        Some(&VariantValue::Primitive(VariantPrimitive::Int8(1))),
        "a byte-order comparator would walk left past 😀 and miss this BMP name"
    );
    assert_eq!(
        object.get("\u{1F600}"),
        Some(&VariantValue::Primitive(VariantPrimitive::Int8(2)))
    );
}

/// Risk pinned: the array is-large bit is bit 4 (`SerializedArray.IS_LARGE = 16`) — a
/// transposed bit test (e.g. the object's bit 6) reads a garbage count for every large array.
/// Java 1.10.0 decoded these bytes as a 1-element array of int8 5 (probe p13).
#[test]
fn test_java_probe_large_array_four_byte_count_decodes() {
    let value = parse_ok(&hex("130100000000020c05"));
    let array = value.as_array().expect("must be an array");
    assert_eq!(array.num_elements(), 1);
    assert_eq!(
        array.get(0),
        Some(&VariantValue::Primitive(VariantPrimitive::Int8(5)))
    );
}

/// Risk pinned: the object is-large bit is bit 6 (`SerializedObject.IS_LARGE = 64`), NOT bit 4
/// — bit 4 belongs to the field-id-size field, so a NON-large object with 2-byte field ids
/// (header 0x12) is the input that exposes a transposed bit (it would be misread as large,
/// consuming a 4-byte count). Java 1.10.0 decoded these bytes as {a: int8 34} (probe p14).
#[test]
fn test_java_probe_object_two_byte_field_ids_not_large_decodes() {
    let metadata = VariantMetadata::parse(&hex(JAVA_METADATA_ABC)).expect("abc metadata");
    let value = VariantValue::parse(&metadata, &hex("1201000000020c22"))
        .expect("non-large object with 2-byte field ids must parse");
    let object = value.as_object().expect("must be an object");
    assert_eq!(object.num_fields(), 1);
    assert_eq!(
        object.get("a"),
        Some(&VariantValue::Primitive(VariantPrimitive::Int8(34)))
    );
}

/// Risk pinned (accepted-set parity): an array whose FIRST offset is nonzero (gap bytes before
/// the element data) is legal — Java 1.10.0 reads element 0 from `dataOffset + offset[0]` and
/// accepted these bytes (probe p8: elem0=7). Rejecting the gap would refuse Java-readable data.
#[test]
fn test_array_gap_before_first_offset_tolerated_like_java() {
    let value = parse_ok(&hex("03010204eeee0c07"));
    let array = value.as_array().expect("must be an array");
    assert_eq!(
        array.get(0),
        Some(&VariantValue::Primitive(VariantPrimitive::Int8(7)))
    );
}

/// Risk pinned (accepted-set parity): an object field whose offset SPAN is larger than the
/// value inside it (short string "x" + 2 slack bytes) is legal — Java's lazy reads never touch
/// the slack and 1.10.0 accepted these bytes (probe p9: a="x"); the eager parse must ignore
/// trailing bytes inside a field span exactly as it does at top level.
#[test]
fn test_object_field_span_slack_tolerated_like_java() {
    let metadata = VariantMetadata::parse(&hex(JAVA_METADATA_ABC)).expect("abc metadata");
    let value = VariantValue::parse(&metadata, &hex("02010000040578eeee"))
        .expect("field span slack must parse");
    let object = value.as_object().expect("must be an object");
    assert_eq!(
        object.get("a"),
        Some(&VariantValue::Primitive(VariantPrimitive::String(
            "x".to_string()
        )))
    );
}

/// Risk pinned (DOCUMENTED DIVERGENCE — see the module doc): Java 1.10.0 ACCEPTS a zero-count
/// object/array whose mandatory final offset entry is truncated away (`[0x02, 0x00]` /
/// `[0x03, 0x00]`) and an empty-dictionary metadata whose declared string-data end overruns
/// the buffer (`[0x01, 0x00, 0x05]`), because its lazy reader never reads those regions when
/// the count is zero (probes p2/p3/p4: numFields=0 / numElements=0 / dictionarySize=0, no
/// throw). These are spec-violating shapes no Java writer emits; this port deliberately
/// rejects all three at the door.
#[test]
fn test_truncated_empty_containers_reject_documented_divergence() {
    assert_rejects(&[0x02, 0x00]);
    assert_rejects(&[0x03, 0x00]);
    assert!(VariantMetadata::parse(&[0x01, 0x00, 0x05]).is_err());
}

/// Risk pinned: the metadata's DECLARED data length (the final offset entry) — not the buffer
/// length — locates the value region. Java 1.10.0 truncated `[01 00 05]` + 10-byte buffer to
/// metadata size 8 and read the value at offset 8 (probe p15: metadataSize=8, value=34);
/// a `size_in_bytes` that reported the un-truncated buffer length would misread every
/// concatenated variant carrying trailing dictionary slack.
#[test]
fn test_variant_from_bytes_declared_metadata_end_shifts_value_start_like_java() {
    let variant = Variant::from_bytes(&hex("010005eeeeeeeeee0c22"))
        .expect("over-declared (in-bounds) metadata data length must parse");
    assert_eq!(variant.metadata().size_in_bytes(), 8);
    assert_eq!(variant.metadata().dictionary_size(), 0);
    assert_eq!(
        variant.value(),
        &VariantValue::Primitive(VariantPrimitive::Int8(34))
    );
}

/// Risk pinned (DoS, the wide axis): the depth guard bounds DEEP inputs; a WIDE input
/// (70,000 sibling nulls, ~280 KB of offsets) must decode in linear time with allocation
/// clamped by the buffer length — no quadratic blowup, no count-driven pre-allocation.
#[test]
fn test_wide_array_70000_elements_decodes_cheaply() {
    const COUNT: usize = 70_000;
    // header 0x1B: array, is-large (bit 4), offset size 3.
    let mut bytes = vec![0x1Bu8];
    bytes.extend_from_slice(&(COUNT as u32).to_le_bytes());
    for offset in 0..=COUNT {
        bytes.extend_from_slice(&offset.to_le_bytes()[..3]);
    }
    bytes.extend_from_slice(&vec![0x00u8; COUNT]); // one null per element
    let value = parse_ok(&bytes);
    let array = value.as_array().expect("must be an array");
    assert_eq!(array.num_elements(), COUNT);
    assert_eq!(
        array.get(COUNT - 1),
        Some(&VariantValue::Primitive(VariantPrimitive::Null))
    );
}

/// Risk pinned: 1.10.0's NESTED bytes — outer object {a: [{c: 9}, "x"], b: 4.5} — decode
/// through all three container levels (`Variants.object` / `Variants.array` / inner
/// `Variants.object`, names from `Variants.metadata("a", "b", "c")`).
#[test]
fn test_java_fixture_nested_object_decodes() {
    let metadata = VariantMetadata::parse(&hex(JAVA_METADATA_ABC)).expect("abc metadata");
    let value = VariantValue::parse(
        &metadata,
        &hex("02020001000e17030200070902010200020c0905781c0000000000001240"),
    )
    .expect("nested fixture must parse");
    let outer = value.as_object().expect("outer object");
    assert_eq!(outer.num_fields(), 2);
    assert_eq!(
        outer.get("b"),
        Some(&VariantValue::Primitive(VariantPrimitive::Double(4.5)))
    );
    let array = outer.get("a").expect("field a").as_array().expect("array");
    assert_eq!(array.num_elements(), 2);
    let inner = array
        .get(0)
        .expect("element 0")
        .as_object()
        .expect("inner object");
    assert_eq!(
        inner.get("c"),
        Some(&VariantValue::Primitive(VariantPrimitive::Int8(9)))
    );
    assert_eq!(
        array.get(1),
        Some(&VariantValue::Primitive(VariantPrimitive::String(
            "x".to_string()
        )))
    );
}

// ===== write side: Java 1.10.0 byte-exact fixtures ==========================================
// Provenance: /tmp/variant-fixture-gen/VariantWriteFixtureGen.java against the pinned 1.10.0
// jars (module doc above). Every constant is the EXACT serialized output of iceberg 1.10.0,
// round-trip re-read by Java at generation time; large fixtures are pinned as
// (java.util.zip.CRC32, length, first-64-bytes) — flate2::Crc computes the identical CRC-32.

/// CRC-32 over the serialized bytes (`flate2::Crc` == `java.util.zip.CRC32`).
fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = flate2::Crc::new();
    crc.update(bytes);
    crc.sum()
}

/// The deterministic binary payload the Java write generator uses
/// (`data[i] = (byte) (i * 7)` in `VariantWriteFixtureGen.binaryOf`).
fn java_binary_payload(length: usize) -> Vec<u8> {
    (0..length).map(|index| ((index * 7) % 256) as u8).collect()
}

/// Asserts a built value serializes EXACTLY to Java's bytes, that Java's bytes parse back to
/// the built value, and that re-serializing the parsed value reproduces Java's bytes (the
/// canonical-input re-serialization contract of `write.rs`).
fn assert_write_fixture(
    name: &str,
    metadata: &VariantMetadata,
    value: &VariantValue,
    java_hex: &str,
) {
    let java_bytes = hex(java_hex);
    let written = value
        .to_bytes(metadata)
        .unwrap_or_else(|error| panic!("{name}: built value must serialize: {error}"));
    assert_eq!(
        written, java_bytes,
        "{name}: built value must serialize byte-for-byte to Java's output"
    );
    assert_eq!(
        value
            .size_in_bytes(metadata)
            .expect("sized fixture must size"),
        java_bytes.len(),
        "{name}: size_in_bytes must equal Java's sizeInBytes"
    );
    let parsed = VariantValue::parse(metadata, &java_bytes)
        .unwrap_or_else(|error| panic!("{name}: Java bytes must parse: {error}"));
    assert_eq!(
        &parsed, value,
        "{name}: Java bytes must decode to the input"
    );
    assert_eq!(
        parsed
            .to_bytes(metadata)
            .expect("parsed fixture must re-serialize"),
        java_bytes,
        "{name}: re-serializing canonical Java bytes must be byte-identical"
    );
}

/// The write-side primitive fixture table: (name, Java 1.10.0 hex, the Rust construction).
/// Covers every primitive type id 0..=20 at boundary values, both string forms, and the
/// decimal width/precision boundaries. Provenance comments quote the exact Java call.
fn write_fixture_primitives() -> Vec<(&'static str, String, VariantValue)> {
    vec![
        // Variants.ofNull()
        ("primitive_null", "00".to_string(), VariantValue::of_null()),
        // Variants.of(true)
        (
            "primitive_true",
            "04".to_string(),
            VariantValue::of_boolean(true),
        ),
        // Variants.of(false) — PrimitiveWrapper's ctor normalizes BOOLEAN_TRUE + false
        (
            "primitive_false",
            "08".to_string(),
            VariantValue::of_boolean(false),
        ),
        // Variants.of((byte) -34)
        (
            "primitive_int8",
            "0cde".to_string(),
            VariantValue::of_int8(-34),
        ),
        // Variants.of((short) -1234)
        (
            "primitive_int16",
            "102efb".to_string(),
            VariantValue::of_int16(-1234),
        ),
        // Variants.of(-12345678)
        (
            "primitive_int32",
            "14b29e43ff".to_string(),
            VariantValue::of_int32(-12345678),
        ),
        // Variants.of(Long.MIN_VALUE)
        (
            "primitive_int64_min",
            "180000000000000080".to_string(),
            VariantValue::of_int64(i64::MIN),
        ),
        // Variants.of(-1.25f)
        (
            "primitive_float",
            "380000a0bf".to_string(),
            VariantValue::of_float(-1.25),
        ),
        // Variants.of(2.5d)
        (
            "primitive_double",
            "1c0000000000000440".to_string(),
            VariantValue::of_double(2.5),
        ),
        // Variants.of(new BigDecimal("-123.4567")) — precision 7 => decimal4
        (
            "primitive_decimal4",
            "20047929edff".to_string(),
            VariantValue::of_decimal(-1234567, 4).expect("precision 7"),
        ),
        // Variants.of(new BigDecimal("-12345678.901234567")) — precision 17 => decimal8
        (
            "primitive_decimal8",
            "240979b494a2ab23d4ff".to_string(),
            VariantValue::of_decimal(-12345678901234567, 9).expect("precision 17"),
        ),
        // Variants.of(new BigDecimal("-9876543210.123456789123456789012345678")) — 37 digits
        (
            "primitive_decimal16",
            "281bb20c9b7ac45ac2fef7a1c7ecd2d891f8".to_string(),
            VariantValue::of_decimal(-9876543210123456789123456789012345678, 27)
                .expect("precision 37"),
        ),
        // Variants.of(new BigDecimal(BigInteger.valueOf(-7), 255)) — the max scale byte
        (
            "w_decimal4_scale255",
            "20fff9ffffff".to_string(),
            VariantValue::of_decimal(-7, 255).expect("precision 1"),
        ),
        // Variants.of(new BigDecimal("9999999.99")) — precision 9: the LAST decimal4
        (
            "w_decimal_precision9",
            "2002ffc99a3b".to_string(),
            VariantValue::of_decimal(999_999_999, 2).expect("precision 9"),
        ),
        // Variants.of(new BigDecimal("99999999.99")) — precision 10: the FIRST decimal8
        (
            "w_decimal_precision10",
            "2402ffe30b5402000000".to_string(),
            VariantValue::of_decimal(9_999_999_999, 2).expect("precision 10"),
        ),
        // Variants.of(new BigDecimal("9999999999999999.99")) — precision 18: the LAST decimal8
        (
            "w_decimal_precision18",
            "2402ffff63a7b3b6e00d".to_string(),
            VariantValue::of_decimal(999_999_999_999_999_999, 2).expect("precision 18"),
        ),
        // Variants.of(new BigDecimal("99999999999999999.99")) — precision 19: the FIRST decimal16
        (
            "w_decimal_precision19",
            "2802ffffe7890423c78a0000000000000000".to_string(),
            VariantValue::of_decimal(9_999_999_999_999_999_999, 2).expect("precision 19"),
        ),
        // Variants.of(PhysicalType.DECIMAL16, new BigDecimal(new BigInteger(
        // "-170141183460469231731687303715884105728"), 38)) — i128::MIN, 39 digits: NOT
        // constructible via the precision factory (Java's of(BigDecimal) rejects it too);
        // built width-explicitly like Java's of(PhysicalType, value).
        (
            "w_decimal16_i128_min",
            "282600000000000000000000000000000080".to_string(),
            VariantValue::Primitive(VariantPrimitive::Decimal16 {
                scale: 38,
                unscaled: i128::MIN,
            }),
        ),
        // Variants.ofIsoDate("2024-11-07")
        (
            "primitive_date",
            "2c424e0000".to_string(),
            VariantValue::of_date(20034),
        ),
        // Variants.ofDate(-3000) — pre-epoch
        (
            "w_date_pre_epoch",
            "2c48f4ffff".to_string(),
            VariantValue::of_date(-3000),
        ),
        // Variants.ofIsoTimestamptz("2024-11-07T12:33:54.123456+00:00")
        (
            "primitive_timestamptz",
            "30c0b2f0d851260600".to_string(),
            VariantValue::of_timestamptz(1730982834123456),
        ),
        // Variants.ofTimestamptz(-1L) — pre-epoch
        (
            "w_timestamptz_pre_epoch",
            "30ffffffffffffffff".to_string(),
            VariantValue::of_timestamptz(-1),
        ),
        // Variants.ofIsoTimestampntz("2024-11-07T12:33:54.123456")
        (
            "primitive_timestampntz",
            "34c0b2f0d851260600".to_string(),
            VariantValue::of_timestampntz(1730982834123456),
        ),
        // Variants.ofIsoTime("12:33:54.123456")
        (
            "primitive_time",
            "44c0f229880a000000".to_string(),
            VariantValue::of_time(45234123456),
        ),
        // Variants.ofTime(86399999999L) — the last microsecond of the day
        (
            "w_time_max",
            "44ff5fd71d14000000".to_string(),
            VariantValue::of_time(86_399_999_999),
        ),
        // Variants.ofIsoTimestamptzNanos("2024-11-07T12:33:54.123456789+00:00")
        (
            "primitive_timestamptz_nanos",
            "4815413a6cb7af0518".to_string(),
            VariantValue::of_timestamptz_nanos(1730982834123456789),
        ),
        // Variants.ofIsoTimestampntzNanos("2024-11-07T12:33:54.123456789")
        (
            "primitive_timestampntz_nanos",
            "4c15413a6cb7af0518".to_string(),
            VariantValue::of_timestampntz_nanos(1730982834123456789),
        ),
        // Variants.ofUUID("f24f9b64-81fa-49d1-b74e-8c09a6e31c56")
        (
            "primitive_uuid",
            "50f24f9b6481fa49d1b74e8c09a6e31c56".to_string(),
            VariantValue::of_uuid([
                0xF2, 0x4F, 0x9B, 0x64, 0x81, 0xFA, 0x49, 0xD1, 0xB7, 0x4E, 0x8C, 0x09, 0xA6, 0xE3,
                0x1C, 0x56,
            ]),
        ),
        // Variants.of(ByteBuffer.wrap(new byte[] {0x0a, 0x0b, 0x0c, 0x0d}))
        (
            "primitive_binary",
            "3c040000000a0b0c0d".to_string(),
            VariantValue::of_binary(vec![0x0A, 0x0B, 0x0C, 0x0D]),
        ),
        // Variants.of(ByteBuffer.wrap(new byte[0])) — empty binary
        (
            "w_binary_empty",
            "3c00000000".to_string(),
            VariantValue::of_binary(vec![]),
        ),
        // Variants.of("") — a zero-length SHORT string (header byte only)
        (
            "w_short_string_empty",
            "01".to_string(),
            VariantValue::of_string(""),
        ),
        // Variants.of("a")
        (
            "w_short_string_1",
            "0561".to_string(),
            VariantValue::of_string("a"),
        ),
        // Variants.of("iceberg") — 7 UTF-8 bytes, SHORT form
        (
            "primitive_short_string",
            "1d69636562657267".to_string(),
            VariantValue::of_string("iceberg"),
        ),
        // Variants.of("x".repeat(63)) — the LAST short string (generator: "fd" + "78"*63)
        (
            "w_short_string_63",
            format!("fd{}", "78".repeat(63)),
            VariantValue::of_string("x".repeat(63)),
        ),
        // Variants.of("x".repeat(64)) — the FIRST spill to the long STRING form
        // (generator: "4040000000" + "78"*64)
        (
            "w_string_64_spills",
            format!("4040000000{}", "78".repeat(64)),
            VariantValue::of_string("x".repeat(64)),
        ),
        // Variants.of("x".repeat(70)) (generator: "4046000000" + "78"*70)
        (
            "primitive_long_string",
            format!("4046000000{}", "78".repeat(70)),
            VariantValue::of_string("x".repeat(70)),
        ),
    ]
}

/// Risk pinned (the write-side core): every primitive type id, the decimal precision→width
/// boundaries (9/10, 18/19 digits, scale 255, i128::MIN), the short-string 0/1/63 lengths
/// and the 64-byte spill to STRING, empty + non-trivial binary, pre-epoch temporals, and
/// the UUID byte order ALL serialize byte-for-byte to Java 1.10.0's output — a single wrong
/// header bit or payload byte silently corrupts tables for every other engine. Each fixture
/// also re-serializes from its parsed form byte-identically (canonical-input contract).
#[test]
fn test_write_java_fixture_primitives_byte_exact() {
    let metadata = empty_metadata();
    for (name, java_hex, value) in write_fixture_primitives() {
        assert_write_fixture(name, &metadata, &value, &java_hex);
    }
}

/// Risk pinned: metadata building must mirror `Variants.metadata` exactly — INSERTION order
/// (never re-sorted, never deduped), the sorted flag only for strictly-compareTo-ascending
/// input (UTF-16 order, where a supplementary character sorts BELOW U+FFFF — a byte-order
/// comparator would set the flag wrongly for `w_metadata_utf16_sorted`), and the offset-size
/// escalation at 255→256 data bytes. Byte-for-byte vs Java 1.10.0, plus parse→re-serialize
/// identity.
#[test]
fn test_write_java_fixture_metadata_byte_exact() {
    // (name, Java hex, input names, the sorted flag Java computed)
    let cases: Vec<(&str, String, Vec<String>, bool)> = vec![
        // Variants.metadata() with no names → EMPTY_V1_METADATA
        (
            "metadata_empty",
            JAVA_METADATA_EMPTY.to_string(),
            vec![],
            false,
        ),
        // Variants.metadata("a", "b", "c") — strictly ascending → sorted
        (
            "metadata_abc",
            JAVA_METADATA_ABC.to_string(),
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
            true,
        ),
        // Variants.metadata(List.of("b", "a", "c")) — kept in INSERTION order, flag unset
        (
            "w_metadata_unsorted_insertion",
            "010300010203626163".to_string(),
            vec!["b".to_string(), "a".to_string(), "c".to_string()],
            false,
        ),
        // Variants.metadata(List.of("a", "a")) — NO dedup; compareTo >= 0 clears the flag
        (
            "w_metadata_duplicate_name",
            "01020001026161".to_string(),
            vec!["a".to_string(), "a".to_string()],
            false,
        ),
        // Variants.metadata(List.of("\u{10000}", "\u{FFFF}")) — ascending in UTF-16 order
        // (surrogate 0xD800 < 0xFFFF), DESCENDING in byte order → the flag must be SET
        (
            "w_metadata_utf16_sorted",
            "1102000407f0908080efbfbf".to_string(),
            vec!["\u{10000}".to_string(), "\u{FFFF}".to_string()],
            true,
        ),
        // Variants.metadata(List.of("x".repeat(255))) — 255 data bytes: 1-byte offsets
        (
            "w_metadata_offsets_255",
            format!("110100ff{}", "78".repeat(255)),
            vec!["x".repeat(255)],
            true,
        ),
        // Variants.metadata(List.of("x".repeat(256))) — 256 data bytes: 2-byte offsets
        (
            "w_metadata_offsets_256",
            format!("51010000000001{}", "78".repeat(256)),
            vec!["x".repeat(256)],
            true,
        ),
    ];
    for (name, java_hex, names, expected_sorted) in cases {
        let java_bytes = hex(&java_hex);
        let built = VariantMetadata::from_field_names(names).expect("metadata must build");
        assert_eq!(
            built.is_sorted(),
            expected_sorted,
            "{name}: the sorted flag must match Java's compareTo computation"
        );
        assert_eq!(
            built.to_bytes().expect("metadata must serialize"),
            java_bytes,
            "{name}: built metadata must serialize byte-for-byte to Java's output"
        );
        assert_eq!(
            built.size_in_bytes(),
            java_bytes.len(),
            "{name}: a built metadata's size_in_bytes is its serialized size"
        );
        let parsed = VariantMetadata::parse(&java_bytes).expect("Java metadata must parse");
        assert_eq!(
            parsed
                .to_bytes()
                .expect("parsed metadata must re-serialize"),
            java_bytes,
            "{name}: re-serializing canonical Java metadata must be byte-identical"
        );
    }
}

/// Risk pinned (documented divergence): >255 names whose total data size still selects a
/// 1-byte offset width (only reachable with empty names) is the pathology Java 1.10.0
/// SILENTLY corrupts — probe-verified: 256 empty names serialize to `01 00 00`, losing
/// every name (the masked count truncates to 0). The Rust door must reject it loudly
/// instead of writing corrupt metadata.
#[test]
fn test_write_metadata_count_door_where_java_truncates() {
    let names = vec![String::new(); 256];
    let error = VariantMetadata::from_field_names(names)
        .expect_err("the count-truncation pathology must be rejected");
    assert_eq!(error.kind(), crate::ErrorKind::DataInvalid);
    assert!(
        error.to_string().contains("do not fit"),
        "error must name the truncation, got: {error}"
    );
    // 255 empty names still fit the 1-byte count — the boundary stays writable.
    let at_boundary = VariantMetadata::from_field_names(vec![String::new(); 255])
        .expect("255 entries fit a 1-byte count");
    assert_eq!(at_boundary.dictionary_size(), 255);
}

/// Risk pinned: array serialization — empty, mixed types (the B1 `array_mixed` fixture,
/// rebuilt via push), and the offset-width escalation at 255→256 data bytes — byte-for-byte
/// vs Java 1.10.0 `ValueArray.writeTo`.
#[test]
fn test_write_java_fixture_arrays_byte_exact() {
    let metadata = empty_metadata();

    // Variants.array() with no elements
    assert_write_fixture(
        "w_array_empty",
        &metadata,
        &VariantValue::Array(VariantArray::new()),
        "030000",
    );

    // Variants.array() + add(of((byte) -34)), add(of("iceberg")), add(ofNull()), add(of(true))
    let mut mixed = VariantArray::new();
    mixed.push(VariantValue::of_int8(-34));
    mixed.push(VariantValue::of_string("iceberg"));
    mixed.push(VariantValue::of_null());
    mixed.push(VariantValue::of_boolean(true));
    assert_write_fixture(
        "array_mixed",
        &metadata,
        &VariantValue::Array(mixed),
        "030400020a0b0c0cde1d696365626572670004",
    );

    // One binary element sized so dataSize is exactly 255 (1-byte offsets) vs 256 (2-byte).
    let mut offsets_255 = VariantArray::new();
    offsets_255.push(VariantValue::of_binary(java_binary_payload(250)));
    assert_write_fixture(
        "w_array_offsets_255",
        &metadata,
        &VariantValue::Array(offsets_255),
        &format!(
            "030100ff3cfa000000{}",
            hex_string(&java_binary_payload(250))
        ),
    );
    let mut offsets_256 = VariantArray::new();
    offsets_256.push(VariantValue::of_binary(java_binary_payload(251)));
    assert_write_fixture(
        "w_array_offsets_256",
        &metadata,
        &VariantValue::Array(offsets_256),
        &format!(
            "0701000000013cfb000000{}",
            hex_string(&java_binary_payload(251))
        ),
    );
}

/// Renders bytes as lowercase hex (the generator's output format, for composing the
/// deterministic-payload fixture constants).
fn hex_string(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// A large write fixture pinned as (CRC-32, total length, first-64-bytes hex) — the exact
/// values `VariantWriteFixtureGen` printed from Java 1.10.0's serialization.
struct JavaCrcPin {
    name: &'static str,
    crc32: u32,
    length: usize,
    prefix_hex: &'static str,
}

/// Asserts the serialized bytes match a Java CRC pin (length + first 64 bytes + CRC-32) and
/// that the bytes round-trip through the B1 parser.
fn assert_crc_pin(pin: &JavaCrcPin, bytes: &[u8]) {
    assert_eq!(
        bytes.len(),
        pin.length,
        "{}: length must match Java",
        pin.name
    );
    assert_eq!(
        hex_string(&bytes[..64.min(bytes.len())]),
        pin.prefix_hex,
        "{}: the header region must match Java byte-for-byte",
        pin.name
    );
    assert_eq!(
        crc32(bytes),
        pin.crc32,
        "{}: the CRC-32 over all bytes must match java.util.zip.CRC32",
        pin.name
    );
}

/// Serializes a constructed value, asserts it matches a Java CRC pin, AND asserts the bytes
/// B1-parse back to the constructed value (the round-trip property for large fixtures).
fn assert_value_crc_pin(pin: &JavaCrcPin, metadata: &VariantMetadata, value: &VariantValue) {
    let bytes = value
        .to_bytes(metadata)
        .unwrap_or_else(|error| panic!("{}: must serialize: {error}", pin.name));
    assert_crc_pin(pin, &bytes);
    let parsed = VariantValue::parse(metadata, &bytes)
        .unwrap_or_else(|error| panic!("{}: must parse back: {error}", pin.name));
    assert_eq!(
        &parsed, value,
        "{}: serialize→parse must reproduce the input",
        pin.name
    );
}

/// Risk pinned (width-selection boundaries, Java fixtures AT the boundaries): the array
/// count boundary 255 (1-byte count) vs 256 (IS-LARGE bit 4, 4-byte count) and the offset
/// width escalation at dataSize 65535 (2-byte) vs 65536 (3-byte) — each pinned against the
/// CRC/length/prefix of Java 1.10.0's exact output. A transposed is-large bit or an
/// off-by-one width threshold changes these bytes.
#[test]
fn test_write_java_array_count_and_offset_width_boundaries_crc_pinned() {
    let metadata = empty_metadata();

    // 255 * add(ofNull()) — count 255 is NOT large (1-byte count 0xff)
    let mut count_255 = VariantArray::new();
    for _ in 0..255 {
        count_255.push(VariantValue::of_null());
    }
    assert_value_crc_pin(
        &JavaCrcPin {
            name: "w_array_count_255",
            crc32: 832470751,
            length: 513,
            prefix_hex: "03ff000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d",
        },
        &metadata,
        &VariantValue::Array(count_255),
    );

    // 256 * add(of((byte) i)) — count 256 IS large (header bit 4, 4-byte count)
    let mut large_256 = VariantArray::new();
    for index in 0..256usize {
        large_256.push(VariantValue::of_int8(
            u8::try_from(index % 256).expect("bounded") as i8,
        ));
    }
    assert_value_crc_pin(
        &JavaCrcPin {
            name: "w_array_large_256",
            crc32: 2411156565,
            length: 1031,
            prefix_hex: "1700010000000002000400060008000a000c000e00100012001400160018001a001c001e00200022002400260028002a002c002e00300032003400360038003a",
        },
        &metadata,
        &VariantValue::Array(large_256),
    );

    // One binary element sized so dataSize is exactly 65535 (2-byte offsets) vs 65536
    // (3-byte offsets).
    let mut data_65535 = VariantArray::new();
    data_65535.push(VariantValue::of_binary(java_binary_payload(65530)));
    assert_value_crc_pin(
        &JavaCrcPin {
            name: "w_array_data_65535",
            crc32: 3403222046,
            length: 65541,
            prefix_hex: "07010000ffff3cfaff000000070e151c232a31383f464d545b626970777e858c939aa1a8afb6bdc4cbd2d9e0e7eef5fc030a11181f262d343b424950575e656c",
        },
        &metadata,
        &VariantValue::Array(data_65535),
    );
    let mut data_65536 = VariantArray::new();
    data_65536.push(VariantValue::of_binary(java_binary_payload(65531)));
    assert_value_crc_pin(
        &JavaCrcPin {
            name: "w_array_data_65536",
            crc32: 1457332204,
            length: 65544,
            prefix_hex: "0b010000000000013cfbff000000070e151c232a31383f464d545b626970777e858c939aa1a8afb6bdc4cbd2d9e0e7eef5fc030a11181f262d343b424950575e",
        },
        &metadata,
        &VariantValue::Array(data_65536),
    );
}

/// Risk pinned: object serialization through the builder — empty object, the B1 `object_ab`
/// fixture, UNSORTED put order coming out name-sorted on disk, and the data-driven offset
/// width escalation — byte-for-byte vs Java 1.10.0 `ShreddedObject.writeTo`.
#[test]
fn test_write_java_fixture_objects_byte_exact() {
    let metadata =
        VariantMetadata::from_field_names(["a", "b", "c"]).expect("abc metadata must build");

    // Variants.object(abc) with no puts
    let empty_object = VariantObjectBuilder::new(&metadata).build();
    assert_write_fixture(
        "w_object_empty",
        &metadata,
        &VariantValue::Object(empty_object),
        "020000",
    );

    // put("a", of((byte) -34)); put("b", of("iceberg")) — the B1 object_ab fixture
    let mut object_ab = VariantObjectBuilder::new(&metadata);
    object_ab
        .put("a", VariantValue::of_int8(-34))
        .expect("a is in the metadata");
    object_ab
        .put("b", VariantValue::of_string("iceberg"))
        .expect("b is in the metadata");
    assert_write_fixture(
        "object_ab",
        &metadata,
        &VariantValue::Object(object_ab.build()),
        "0202000100020a0cde1d69636562657267",
    );

    // put("b", ...) FIRST, then put("a", ...) — Java writes name-sorted regardless of the
    // put order (SortedMerge in ShreddedObject.writeTo); the bytes put field a first.
    let mut unsorted_puts = VariantObjectBuilder::new(&metadata);
    unsorted_puts
        .put("b", VariantValue::of_int8(1))
        .expect("b is in the metadata");
    unsorted_puts
        .put("a", VariantValue::of_int8(2))
        .expect("a is in the metadata");
    assert_write_fixture(
        "w_object_unsorted_puts",
        &metadata,
        &VariantValue::Object(unsorted_puts.build()),
        "020200010002040c020c01",
    );

    // put("a", binary(251 bytes)) — dataSize 256 forces 2-byte offsets with 1-byte ids.
    let mut data_offsets_256 = VariantObjectBuilder::new(&metadata);
    data_offsets_256
        .put("a", VariantValue::of_binary(java_binary_payload(251)))
        .expect("a is in the metadata");
    assert_write_fixture(
        "w_object_data_offsets_256",
        &metadata,
        &VariantValue::Object(data_offsets_256.build()),
        &format!(
            "060100000000013cfb000000{}",
            hex_string(&java_binary_payload(251))
        ),
    );
}

/// Risk pinned (THE B1 killer-probe class, write direction): on-disk object field order is
/// Java `String.compareTo` — UTF-16 code units — so U+10000 (𐀀, surrogate 0xD800) sorts
/// BEFORE U+FFFF even though its UTF-8 bytes sort AFTER. Puts arrive in the OPPOSITE order;
/// a byte-order sort would emit the fields (and their values) transposed and produce
/// different bytes than Java 1.10.0.
#[test]
fn test_write_java_fixture_object_utf16_field_order_byte_exact() {
    let metadata = VariantMetadata::from_field_names(["\u{10000}", "\u{FFFF}"])
        .expect("utf16 metadata must build");
    assert!(metadata.is_sorted(), "Java order is ascending here");
    assert_eq!(
        metadata.to_bytes().expect("metadata must serialize"),
        hex("1102000407f0908080efbfbf"),
        "the metadata itself must match w_metadata_utf16_sorted"
    );

    let mut builder = VariantObjectBuilder::new(&metadata);
    builder
        .put("\u{FFFF}", VariantValue::of_int8(1))
        .expect("U+FFFF is in the metadata");
    builder
        .put("\u{10000}", VariantValue::of_int8(2))
        .expect("U+10000 is in the metadata");
    // Java's bytes order the fields [U+10000 (id 0, value 2), U+FFFF (id 1, value 1)].
    assert_write_fixture(
        "w_object_utf16_order",
        &metadata,
        &VariantValue::Object(builder.build()),
        "020200010002040c020c01",
    );
}

/// Risk pinned: `fieldIdSize = sizeOf(metadata.dictionarySize())` — the dictionary SIZE,
/// not the largest id used: a 255-name dictionary writes 1-byte field ids (id 254 = `fe`),
/// a 256-name dictionary writes 2-byte field ids (id 255 = `ff 00`) even though 255 fits
/// one byte. Byte-for-byte vs Java 1.10.0; the dictionaries themselves are CRC-pinned.
#[test]
fn test_write_java_fixture_object_field_id_width_follows_dictionary_size() {
    let names_255: Vec<String> = (0..255).map(|index| format!("k{index:03}")).collect();
    let dict_255 = VariantMetadata::from_field_names(names_255).expect("dict255 must build");
    let dict_255_bytes = dict_255.to_bytes().expect("dict255 must serialize");
    assert_crc_pin(
        &JavaCrcPin {
            name: "w_metadata_dict255",
            crc32: 3863234208,
            length: 1535,
            prefix_hex: "51ff000000040008000c001000140018001c002000240028002c003000340038003c004000440048004c005000540058005c006000640068006c007000740078",
        },
        &dict_255_bytes,
    );
    assert_eq!(
        VariantMetadata::parse(&dict_255_bytes).expect("dict255 bytes must parse"),
        dict_255,
        "dict255: serialize→parse must reproduce the built metadata"
    );
    let mut object_255 = VariantObjectBuilder::new(&dict_255);
    object_255
        .put("k254", VariantValue::of_int8(7))
        .expect("k254 is in the metadata");
    assert_write_fixture(
        "w_object_dict255",
        &dict_255,
        &VariantValue::Object(object_255.build()),
        "0201fe00020c07",
    );

    let names_256: Vec<String> = (0..256).map(|index| format!("k{index:03}")).collect();
    let dict_256 = VariantMetadata::from_field_names(names_256).expect("dict256 must build");
    let dict_256_bytes = dict_256.to_bytes().expect("dict256 must serialize");
    assert_crc_pin(
        &JavaCrcPin {
            name: "w_metadata_dict256",
            crc32: 3288150088,
            length: 1541,
            prefix_hex: "5100010000040008000c001000140018001c002000240028002c003000340038003c004000440048004c005000540058005c006000640068006c007000740078",
        },
        &dict_256_bytes,
    );
    assert_eq!(
        VariantMetadata::parse(&dict_256_bytes).expect("dict256 bytes must parse"),
        dict_256,
        "dict256: serialize→parse must reproduce the built metadata"
    );
    let mut object_256 = VariantObjectBuilder::new(&dict_256);
    object_256
        .put("k255", VariantValue::of_int8(7))
        .expect("k255 is in the metadata");
    assert_write_fixture(
        "w_object_dict256",
        &dict_256,
        &VariantValue::Object(object_256.build()),
        "1201ff0000020c07",
    );

    // 256 fields: the IS-LARGE object (header bit 6, 4-byte count) with 2-byte ids.
    let mut large_object = VariantObjectBuilder::new(&dict_256);
    for index in 0..256usize {
        large_object
            .put(
                format!("k{index:03}"),
                VariantValue::of_int8(u8::try_from(index % 256).expect("bounded") as i8),
            )
            .expect("every kNNN is in the metadata");
    }
    assert_value_crc_pin(
        &JavaCrcPin {
            name: "w_object_large_256_fields",
            crc32: 251652010,
            length: 1543,
            prefix_hex: "560001000000000100020003000400050006000700080009000a000b000c000d000e000f0010001100120013001400150016001700180019001a001b001c001d",
        },
        &dict_256,
        &VariantValue::Object(large_object.build()),
    );
}

/// Risk pinned: nested container serialization — object → array → object (the B1
/// `object_nested` fixture rebuilt: outer {a: [{c: 9}, "x"], b: 4.5}) — byte-for-byte vs
/// Java 1.10.0, exercising recursive size computation and offset assignment across all
/// three container levels.
#[test]
fn test_write_java_fixture_nested_object_byte_exact() {
    let metadata =
        VariantMetadata::from_field_names(["a", "b", "c"]).expect("abc metadata must build");
    let mut inner = VariantObjectBuilder::new(&metadata);
    inner
        .put("c", VariantValue::of_int8(9))
        .expect("c is in the metadata");
    let mut array = VariantArray::new();
    array.push(VariantValue::Object(inner.build()));
    array.push(VariantValue::of_string("x"));
    let mut outer = VariantObjectBuilder::new(&metadata);
    outer
        .put("a", VariantValue::Array(array))
        .expect("a is in the metadata");
    outer
        .put("b", VariantValue::of_double(4.5))
        .expect("b is in the metadata");
    assert_write_fixture(
        "object_nested",
        &metadata,
        &VariantValue::Object(outer.build()),
        "02020001000e17030200070902010200020c0905781c0000000000001240",
    );
}

/// Risk pinned: `Variant::to_bytes` emits metadata bytes immediately followed by value
/// bytes — exactly the Java `Variant.from(ByteBuffer)` layout — and `Variant::from_bytes`
/// parses its own output back to an equal variant (the whole-variant round trip).
#[test]
fn test_write_variant_to_bytes_concatenated_layout_round_trips() {
    let metadata =
        VariantMetadata::from_field_names(["a", "b", "c"]).expect("abc metadata must build");
    let object = {
        let mut builder = VariantObjectBuilder::new(&metadata);
        builder
            .put("a", VariantValue::of_int8(-34))
            .expect("a is in the metadata");
        builder
            .put("b", VariantValue::of_string("iceberg"))
            .expect("b is in the metadata");
        builder.build()
    };
    let variant = Variant::of(metadata, VariantValue::Object(object));

    let bytes = variant.to_bytes().expect("variant must serialize");
    let mut expected = hex(JAVA_METADATA_ABC);
    expected.extend_from_slice(&hex("0202000100020a0cde1d69636562657267"));
    assert_eq!(
        bytes, expected,
        "metadata bytes ++ value bytes, both byte-exact vs Java"
    );

    let reparsed = Variant::from_bytes(&bytes).expect("own output must parse");
    assert_eq!(&reparsed, &variant, "whole-variant round trip");
}

/// Risk pinned (round-trip property): a structured sweep of constructed values — every
/// factory primitive plus nested arrays/objects — serializes and B1-parses back to semantic
/// equality. Catches any write/read disagreement the byte fixtures might not cover.
#[test]
fn test_write_round_trip_sweep_constructed_values() {
    let metadata =
        VariantMetadata::from_field_names(["a", "b", "c"]).expect("abc metadata must build");

    let mut primitives: Vec<VariantValue> = write_fixture_primitives()
        .into_iter()
        .map(|(_, _, value)| value)
        .collect();
    primitives.push(VariantValue::of_decimal(0, 0).expect("zero has precision 1"));
    primitives.push(VariantValue::of_float(f32::MIN_POSITIVE));
    primitives.push(VariantValue::of_double(-0.0));

    let mut array = VariantArray::new();
    for value in &primitives {
        array.push(value.clone());
    }
    let mut object = VariantObjectBuilder::new(&metadata);
    object
        .put("a", VariantValue::Array(array))
        .expect("a is in the metadata");
    object
        .put("c", VariantValue::of_string("nested"))
        .expect("c is in the metadata");
    let mut outer_array = VariantArray::new();
    outer_array.push(VariantValue::Object(object.build()));
    for value in primitives {
        outer_array.push(value);
    }
    let sweep = VariantValue::Array(outer_array);

    let bytes = sweep.to_bytes(&metadata).expect("sweep must serialize");
    let parsed = VariantValue::parse(&metadata, &bytes).expect("sweep must parse");
    assert_eq!(parsed, sweep, "serialize→parse must reproduce the input");
}

/// Risk pinned: NaN floats round-trip at exact bit precision (the derived `PartialEq` treats
/// NaN != NaN, so the sweep test above cannot carry them — compare the bits instead).
#[test]
fn test_write_nan_floats_round_trip_exact_bits() {
    let metadata = empty_metadata();
    let float_bytes = VariantValue::of_float(f32::NAN)
        .to_bytes(&metadata)
        .expect("NaN float must serialize");
    match VariantValue::parse(&metadata, &float_bytes).expect("must parse") {
        VariantValue::Primitive(VariantPrimitive::Float(value)) => {
            assert_eq!(value.to_bits(), f32::NAN.to_bits());
        }
        other => panic!("expected a float, got {other:?}"),
    }
    let double_bytes = VariantValue::of_double(f64::NAN)
        .to_bytes(&metadata)
        .expect("NaN double must serialize");
    match VariantValue::parse(&metadata, &double_bytes).expect("must parse") {
        VariantValue::Primitive(VariantPrimitive::Double(value)) => {
            assert_eq!(value.to_bits(), f64::NAN.to_bits());
        }
        other => panic!("expected a double, got {other:?}"),
    }
}

/// Risk pinned: the write recursion is depth-guarded exactly like the parse side — 128
/// nested arrays serialize (and parse back), 129 error cleanly instead of overflowing the
/// stack (a manually-constructed bomb never reaches the parser's guard).
#[test]
fn test_write_nesting_depth_guard_boundary() {
    let metadata = empty_metadata();
    let nest = |levels: usize| {
        let mut value = VariantValue::of_null();
        for _ in 0..levels {
            let mut array = VariantArray::new();
            array.push(value);
            value = VariantValue::Array(array);
        }
        value
    };

    let at_limit = nest(MAX_NESTING_DEPTH);
    let bytes = at_limit
        .to_bytes(&metadata)
        .expect("128 levels must serialize");
    assert_eq!(
        VariantValue::parse(&metadata, &bytes).expect("128 levels must parse"),
        at_limit
    );

    let beyond = nest(MAX_NESTING_DEPTH + 1);
    let error = beyond
        .size_in_bytes(&metadata)
        .expect_err("129 levels must be rejected");
    assert!(
        error.to_string().contains("nesting depth"),
        "error must name the depth guard, got: {error}"
    );
    let mut buffer = vec![0u8; 1024];
    assert!(
        beyond.write_to(&metadata, &mut buffer, 0).is_err(),
        "the write path carries the same guard"
    );
}

/// Risk pinned: builder error paths — an unknown field name is rejected at `put` with
/// Java's exact message ("Cannot find field name in metadata: %s"), and re-putting a name
/// REPLACES the previous value (Java HashMap semantics).
#[test]
fn test_write_builder_put_unknown_name_rejects_and_put_replaces() {
    let metadata = VariantMetadata::from_field_names(["a", "b"]).expect("metadata must build");
    let mut builder = VariantObjectBuilder::new(&metadata);
    let error = builder
        .put("missing", VariantValue::of_int8(1))
        .expect_err("unknown names are rejected");
    assert_eq!(error.kind(), crate::ErrorKind::DataInvalid);
    assert!(
        error
            .to_string()
            .contains("Cannot find field name in metadata: missing"),
        "error must carry Java's message, got: {error}"
    );

    builder
        .put("a", VariantValue::of_int8(1))
        .expect("a is in the metadata");
    builder
        .put("a", VariantValue::of_int8(2))
        .expect("re-put replaces");
    let object = builder.build();
    assert_eq!(object.num_fields(), 1, "put replaces, never duplicates");
    assert_eq!(
        object.get("a"),
        Some(&VariantValue::Primitive(VariantPrimitive::Int8(2)))
    );
}

/// Risk pinned: writing an object against a metadata that lacks its field names fails loud
/// with Java's `checkState` message ("Invalid metadata, missing: %s") — the write-time
/// re-resolution `ShreddedObject.writeTo` performs — instead of emitting dangling ids.
#[test]
fn test_write_object_with_wrong_metadata_rejects() {
    let metadata = VariantMetadata::from_field_names(["a"]).expect("metadata must build");
    let mut builder = VariantObjectBuilder::new(&metadata);
    builder
        .put("a", VariantValue::of_int8(1))
        .expect("a is in the metadata");
    let object = VariantValue::Object(builder.build());

    let wrong_metadata = empty_metadata();
    let error = object
        .to_bytes(&wrong_metadata)
        .expect_err("a missing name must be rejected at write time");
    assert!(
        error.to_string().contains("Invalid metadata, missing: a"),
        "error must carry Java's checkState message, got: {error}"
    );
}

/// Risk pinned: `write_to` into an undersized buffer (or past its end) is a clean error,
/// never a panic and never a partial silent write being reported as success.
#[test]
fn test_write_buffer_too_small_or_bad_offset_rejects() {
    let metadata = empty_metadata();
    let value = VariantValue::of_string("iceberg");
    let needed = value.size_in_bytes(&metadata).expect("sized");

    let mut too_small = vec![0u8; needed - 1];
    assert!(
        value.write_to(&metadata, &mut too_small, 0).is_err(),
        "an undersized buffer must be rejected"
    );

    let mut exact = vec![0u8; needed];
    assert!(
        value.write_to(&metadata, &mut exact, 1).is_err(),
        "offset 1 leaves one byte too few"
    );
    assert!(
        value.write_to(&metadata, &mut exact, usize::MAX).is_err(),
        "a hostile offset must not wrap"
    );
    assert_eq!(
        value
            .write_to(&metadata, &mut exact, 0)
            .expect("exact-size buffer at offset 0"),
        needed
    );

    // Containers door the WHOLE span up front: a failed container write leaves the buffer
    // UNTOUCHED (fail-fast, no partial header/count/offset bytes) — without the up-front
    // door the per-write bounds checks would still error but only AFTER mutating the
    // caller's buffer (B2-review mutation: removing the door survived every is_err pin).
    let mut array = VariantArray::new();
    array.push(VariantValue::of_int8(1));
    let array_value = VariantValue::Array(array);
    let mut short = vec![0u8; 3];
    assert!(array_value.write_to(&metadata, &mut short, 0).is_err());
    assert_eq!(
        short,
        vec![0u8; 3],
        "a doored array write must not partially mutate the buffer"
    );

    let object_metadata = VariantMetadata::from_field_names(["a"]).expect("metadata must build");
    let mut object_builder = VariantObjectBuilder::new(&object_metadata);
    object_builder
        .put("a", VariantValue::of_int8(1))
        .expect("a is in the metadata");
    let object_value = VariantValue::Object(object_builder.build());
    let mut object_short = vec![0u8; 5];
    assert!(
        object_value
            .write_to(&object_metadata, &mut object_short, 0)
            .is_err()
    );
    assert_eq!(
        object_short,
        vec![0u8; 5],
        "a doored object write must not partially mutate the buffer"
    );
}

/// Risk pinned (documented divergence): re-serializing a PARSED NON-canonical metadata
/// (oversized offset width) canonicalizes to the Java-writer widths — Java's
/// `SerializedMetadata.writeTo` would copy the original buffer verbatim. The decoded
/// dictionary is unchanged; only the encoding is normalized, and `size_in_bytes` keeps
/// reporting the PARSED size.
#[test]
fn test_write_parsed_noncanonical_metadata_canonicalizes() {
    // 4-byte offsets for a 3-byte dictionary — legal to PARSE, never written by Java.
    let oversized = metadata::tests::encode_metadata(&["a", "bc"], 4, false);
    let parsed = VariantMetadata::parse(&oversized).expect("oversized widths parse");
    assert_eq!(parsed.size_in_bytes(), oversized.len());

    let reserialized = parsed.to_bytes().expect("must re-serialize");
    assert_ne!(
        reserialized, oversized,
        "the non-canonical input re-encodes with minimal widths"
    );
    assert_eq!(
        reserialized,
        metadata::tests::encode_metadata(&["a", "bc"], 1, false),
        "the re-encoding is the canonical 1-byte-offset form"
    );
    let reparsed = VariantMetadata::parse(&reserialized).expect("canonical form parses");
    assert_eq!(reparsed.dictionary_size(), 2);
    assert_eq!(reparsed.get(0).expect("id 0"), "a");
    assert_eq!(reparsed.get(1).expect("id 1"), "bc");
}

/// Risk pinned (B2 review, the OBJECT-side offset-width escalation the array pair above
/// cannot catch): an object whose dataSize is exactly 65535 keeps 2-byte offsets and 65536
/// escalates to 3-byte — `offsetSize = sizeOf(dataSize)` feeds `objectHeader`, so an
/// off-by-one here flips the object header byte AND every offset-list byte. Pinned against
/// Java 1.10.0 (`/tmp/variant-fixture-gen/ReviewerProbe.java`: `Variants.object(abc)` +
/// `put("a", binaryOf(65530|65531))`, full bytes diffed end-to-end at review time).
#[test]
fn test_write_java_object_offset_width_65535_65536_boundary_crc_pinned() {
    let metadata =
        VariantMetadata::from_field_names(["a", "b", "c"]).expect("abc metadata must build");
    let mut data_65535 = VariantObjectBuilder::new(&metadata);
    data_65535
        .put("a", VariantValue::of_binary(java_binary_payload(65530)))
        .expect("a is in the metadata");
    assert_value_crc_pin(
        &JavaCrcPin {
            name: "r_object_data_65535",
            crc32: 2602592650,
            length: 65542,
            prefix_hex: "0601000000ffff3cfaff000000070e151c232a31383f464d545b626970777e858c939aa1a8afb6bdc4cbd2d9e0e7eef5fc030a11181f262d343b424950575e65",
        },
        &metadata,
        &VariantValue::Object(data_65535.build()),
    );
    let mut data_65536 = VariantObjectBuilder::new(&metadata);
    data_65536
        .put("a", VariantValue::of_binary(java_binary_payload(65531)))
        .expect("a is in the metadata");
    assert_value_crc_pin(
        &JavaCrcPin {
            name: "r_object_data_65536",
            crc32: 898782542,
            length: 65545,
            prefix_hex: "0a01000000000000013cfbff000000070e151c232a31383f464d545b626970777e858c939aa1a8afb6bdc4cbd2d9e0e7eef5fc030a11181f262d343b42495057",
        },
        &metadata,
        &VariantValue::Object(data_65536.build()),
    );
}

/// Risk pinned (B2 review, the ONLY fixture forcing offsetSize = 4): an array whose
/// dataSize is exactly 0xFFFFFF keeps 3-byte offsets and 0x1000000 escalates to 4-byte —
/// Java serializes this legally (`sizeOf` returns 4 up to `Integer.MAX_VALUE`), so the Rust
/// `int`-domain doors must NOT reject it, and the width-bits `0b11` header path plus the
/// 4-byte little-endian offset write must match Java exactly. Pinned against Java 1.10.0
/// (`/tmp/variant-fixture-gen/ReviewerProbe.java`: `Variants.array()` +
/// `add(binaryOf(0xFFFFFF - 5 | - 4))`, full bytes diffed end-to-end at review time).
#[test]
fn test_write_java_array_4_byte_offset_width_boundary_crc_pinned() {
    let metadata = empty_metadata();
    let mut data_16777215 = VariantArray::new();
    data_16777215.push(VariantValue::of_binary(java_binary_payload(0xFF_FFFF - 5)));
    assert_value_crc_pin(
        &JavaCrcPin {
            name: "r_array_data_16777215",
            crc32: 2229981161,
            length: 16777223,
            prefix_hex: "0b01000000ffffff3cfaffff0000070e151c232a31383f464d545b626970777e858c939aa1a8afb6bdc4cbd2d9e0e7eef5fc030a11181f262d343b424950575e",
        },
        &metadata,
        &VariantValue::Array(data_16777215),
    );
    let mut data_16777216 = VariantArray::new();
    data_16777216.push(VariantValue::of_binary(java_binary_payload(0xFF_FFFF - 4)));
    assert_value_crc_pin(
        &JavaCrcPin {
            name: "r_array_data_16777216",
            crc32: 1758722787,
            length: 16777226,
            prefix_hex: "0f0100000000000000013cfbffff0000070e151c232a31383f464d545b626970777e858c939aa1a8afb6bdc4cbd2d9e0e7eef5fc030a11181f262d343b424950",
        },
        &metadata,
        &VariantValue::Array(data_16777216),
    );
}

/// Risk pinned (B2 review, the duplicate-name id-resolution contract): a dictionary may
/// legally contain DUPLICATE names (insertion order, no dedup). Java's unsorted
/// `SerializedMetadata.id` linear-scans from index 0 → the FIRST duplicate wins, and
/// `ShreddedObject.writeTo` resolves through it, so Java's WRITER emits id 0
/// (probe-verified: `02010000020c09`). Rust must resolve identically. A PARSED object
/// referencing the SECOND duplicate (id 1) re-serializes here with id 0 — the documented
/// canonicalization divergence (Java's `SerializedValue.writeTo` copies the original
/// buffer verbatim, keeping id 1); the decoded value (name → value) is unchanged.
#[test]
fn test_write_duplicate_name_dictionary_resolves_first_id_like_java() {
    let dup = VariantMetadata::from_field_names(["a", "a"]).expect("duplicates are legal");
    assert_eq!(
        dup.id("a"),
        Some(0),
        "linear scan returns the FIRST duplicate, like Java"
    );

    let mut writer = VariantObjectBuilder::new(&dup);
    writer
        .put("a", VariantValue::of_int8(9))
        .expect("a is in the metadata");
    assert_write_fixture(
        "r_dup_writer",
        &dup,
        &VariantValue::Object(writer.build()),
        "02010000020c09",
    );

    // A third-party object referencing the SECOND duplicate id parses fine and
    // CANONICALIZES on re-serialization (id 1 → id 0; Java would copy verbatim instead).
    let second_id_object = hex("02010100020c09");
    let parsed = VariantValue::parse(&dup, &second_id_object).expect("id 1 is in range");
    assert_eq!(
        parsed.to_bytes(&dup).expect("must re-serialize"),
        hex("02010000020c09"),
        "the re-resolved field id is the FIRST duplicate's"
    );
}

// ===== shredding overlay: Java 1.10.0 byte-exact fixtures (Wave-4 F2) ======================
// Provenance: /tmp/variant-fixture-gen/VariantShredFixtureGen.java against the pinned 1.10.0
// jars (generated 2026-06-11; classpath recipe in the module doc above — the shred generator
// uses the same CPW with avro + caffeine). Every overlay constant is the EXACT serialized
// output of iceberg-core 1.10.0's `ShreddedObject.writeTo`, round-trip re-read by Java at
// generation time; the probe lines quoted per test are the same program's output.

/// Java's `f_base_sorted` — `Variants.object(mdAbcd)` + put a=Variants.of(1) (int32),
/// b=Variants.of("x"), c=Variants.of(true), over `Variants.metadata("a","b","c","d")`.
const JAVA_SHRED_BASE_SORTED: &str = "0203000102000507081401000000057804";

/// The metadata every sorted-base fixture uses (`Variants.metadata("a","b","c","d")`).
fn shred_metadata_abcd() -> VariantMetadata {
    VariantMetadata::from_field_names(["a", "b", "c", "d"]).expect("4-name dictionary")
}

/// Rebuilds the shared sorted backing through the Rust plain writer and pins it against
/// Java's bytes — every overlay fixture then runs over a PROVEN-identical backing.
fn shred_sorted_base_bytes(metadata: &VariantMetadata) -> Vec<u8> {
    let mut builder = VariantObjectBuilder::new(metadata);
    builder
        .put("a", VariantValue::of_int32(1))
        .expect("a is in the metadata");
    builder
        .put("b", VariantValue::of_string("x"))
        .expect("b is in the metadata");
    builder
        .put("c", VariantValue::of_boolean(true))
        .expect("c is in the metadata");
    let bytes = VariantValue::Object(builder.build())
        .to_bytes(metadata)
        .expect("the base must serialize");
    assert_eq!(
        bytes,
        hex(JAVA_SHRED_BASE_SORTED),
        "the Rust-built backing must equal Java's f_base_sorted before any overlay runs"
    );
    bytes
}

/// Asserts an overlay serializes EXACTLY to Java's bytes (size, write, and a B1 re-parse of
/// the output as the read-back sanity check).
fn assert_overlay_fixture(
    name: &str,
    metadata: &VariantMetadata,
    overlay: &ShreddedObject<'_>,
    java_hex: &str,
) -> VariantValue {
    let java_bytes = hex(java_hex);
    assert_eq!(
        overlay
            .size_in_bytes()
            .unwrap_or_else(|error| panic!("{name}: must size: {error}")),
        java_bytes.len(),
        "{name}: size_in_bytes must equal Java's sizeInBytes"
    );
    let written = overlay
        .to_bytes()
        .unwrap_or_else(|error| panic!("{name}: must serialize: {error}"));
    assert_eq!(
        written, java_bytes,
        "{name}: the overlay must serialize byte-for-byte to Java's output"
    );
    VariantValue::parse(metadata, &java_bytes)
        .unwrap_or_else(|error| panic!("{name}: the overlay output must re-parse: {error}"))
}

/// Risk pinned: the core merge semantics in one fixture — a `put` OVERRIDES the backing's
/// field, a new `put` is merged in, `remove` excludes the backing's field, and the
/// header/ids/offsets are recomputed over the MERGED set. A wrong merge silently rewrites
/// object content for every shredded write.
#[test]
fn test_overlay_override_add_remove_matches_java_bytes() {
    let metadata = shred_metadata_abcd();
    let base = shred_sorted_base_bytes(&metadata);
    let mut overlay =
        ShreddedObject::over_serialized_object(&metadata, &base).expect("base must parse");
    overlay
        .put("b", VariantValue::of_string("yy"))
        .expect("override");
    overlay.put("d", VariantValue::of_int8(2)).expect("add");
    overlay.remove("a");
    let parsed = assert_overlay_fixture(
        "f_overlay_override_add_remove",
        &metadata,
        &overlay,
        "020301020300030406097979040c02",
    );
    let object = parsed.as_object().expect("an object");
    assert_eq!(object.num_fields(), 3);
    assert_eq!(
        object.get("b"),
        Some(&VariantValue::of_string("yy")),
        "the shredded override must win"
    );
    assert!(object.get("a").is_none(), "the removed field must be gone");
}

/// Risk pinned: over an UNSORTED dictionary with a duplicate name, write-time field-id
/// re-resolution is Java's linear scan (FIRST matching id) — and the untouched field stays
/// verbatim. A sorted-assuming or last-match resolution diverges from Java's bytes.
#[test]
fn test_overlay_over_unsorted_duplicate_name_metadata_matches_java() {
    let metadata =
        VariantMetadata::from_field_names(["b", "a", "b"]).expect("duplicates are legal");
    // Java's f_base_unsorted_dup: Variants.object(mdDup) + a=(byte)34, b="zz".
    let mut builder = VariantObjectBuilder::new(&metadata);
    builder.put("a", VariantValue::of_int8(34)).expect("a");
    builder.put("b", VariantValue::of_string("zz")).expect("b");
    let base = VariantValue::Object(builder.build())
        .to_bytes(&metadata)
        .expect("base");
    assert_eq!(base, hex("020201000002050c22097a7a"), "f_base_unsorted_dup");

    let mut overlay =
        ShreddedObject::over_serialized_object(&metadata, &base).expect("base must parse");
    overlay
        .put("a", VariantValue::of_int8(35))
        .expect("override a");
    assert_overlay_fixture(
        "f_overlay_unsorted_dup_metadata",
        &metadata,
        &overlay,
        "020201000002050c23097a7a",
    );
}

/// Java's hand-built NON-CANONICAL backing: field `a` is a LONG-form string "hi" (the
/// canonical writer would use the short form) and the object header declares an OVERSIZED
/// 2-byte offset width; field `b` is int8 7.
const JAVA_SHRED_BASE_NON_CANONICAL: &str = "06020001000007000900400200000068690c07";
/// The long-form "hi" encoding inside it — the byte run that must survive verbatim.
const LONG_FORM_HI: &str = "40020000006869"; // header, 4-byte length, "hi"

/// Risk pinned: THE divergence-class pin. Untouched unshredded fields must serialize as
/// VERBATIM slices of the original buffer (Java `SerializedObject.sliceValue`) — a
/// re-encode through the canonicalizing writer would shrink the long-form string to the
/// short form and silently produce different bytes than Java for every third-party
/// non-canonical input.
#[test]
fn test_overlay_preserves_non_canonical_unshredded_field_bytes_verbatim() {
    let metadata = shred_metadata_abcd();
    let base = hex(JAVA_SHRED_BASE_NON_CANONICAL);
    let mut overlay =
        ShreddedObject::over_serialized_object(&metadata, &base).expect("base must parse");
    overlay.put("c", VariantValue::of_boolean(true)).expect("c");
    let java_hex = "02030001020007090a400200000068690c0704";
    assert_overlay_fixture(
        "f_overlay_noncanonical_put_c",
        &metadata,
        &overlay,
        java_hex,
    );
    assert!(
        java_hex.contains(LONG_FORM_HI),
        "the fixture itself must carry the long-form encoding verbatim"
    );
    // The canonical writer would have emitted the SHORT form instead — prove the
    // re-encoding path really differs, so the fixture pins verbatim copy, not a no-op.
    let parsed = VariantValue::parse(&metadata, &base).expect("must parse");
    let canonical = parsed.to_bytes(&metadata).expect("must re-serialize");
    assert_ne!(
        canonical, base,
        "re-encoding canonicalizes; only the overlay preserves the original bytes"
    );
}

/// Risk pinned: an EMPTY overlay over a non-canonical backing re-encodes the outer
/// header/ids/offsets canonically (Java recomputes them) while every field VALUE byte stays
/// verbatim — pinning exactly which bytes Java preserves and which it rewrites.
#[test]
fn test_empty_overlay_over_non_canonical_backing_reheaders_but_preserves_field_bytes() {
    let metadata = shred_metadata_abcd();
    let base = hex(JAVA_SHRED_BASE_NON_CANONICAL);
    let overlay =
        ShreddedObject::over_serialized_object(&metadata, &base).expect("base must parse");
    let java_hex = "02020001000709400200000068690c07";
    assert_overlay_fixture(
        "f_overlay_noncanonical_empty",
        &metadata,
        &overlay,
        java_hex,
    );
    assert_ne!(
        hex(java_hex),
        base,
        "the oversized offset width must be re-headered away"
    );
    assert!(
        java_hex.contains(LONG_FORM_HI),
        "the long-form field value must survive the re-header verbatim"
    );
}

/// Risk pinned: an EMPTY overlay over a CANONICAL Java-written backing is byte-identical to
/// the backing (Java probe: `probe_empty_overlay_equals_base true`) — the overlay must not
/// perturb already-canonical data.
#[test]
fn test_empty_overlay_over_canonical_backing_is_byte_identical_to_the_backing() {
    let metadata = shred_metadata_abcd();
    let base = shred_sorted_base_bytes(&metadata);
    let overlay =
        ShreddedObject::over_serialized_object(&metadata, &base).expect("base must parse");
    assert_overlay_fixture(
        "f_overlay_empty_canonical",
        &metadata,
        &overlay,
        JAVA_SHRED_BASE_SORTED,
    );
}

/// Risk pinned: an add-only overlay keeps every backing field verbatim and merges the new
/// field in UTF-16 name order.
#[test]
fn test_overlay_add_only_matches_java_bytes() {
    let metadata = shred_metadata_abcd();
    let base = shred_sorted_base_bytes(&metadata);
    let mut overlay =
        ShreddedObject::over_serialized_object(&metadata, &base).expect("base must parse");
    overlay.put("d", VariantValue::of_int8(2)).expect("add d");
    assert_overlay_fixture(
        "f_overlay_add_only",
        &metadata,
        &overlay,
        "020400010203000507080a14010000000578040c02",
    );
}

/// Risk pinned: a remove-only overlay drops exactly the removed field and recomputes the
/// widths over the SHRUNK set (a stale dataSize would misplace every offset).
#[test]
fn test_overlay_remove_only_matches_java_bytes() {
    let metadata = shred_metadata_abcd();
    let base = shred_sorted_base_bytes(&metadata);
    let mut overlay =
        ShreddedObject::over_serialized_object(&metadata, &base).expect("base must parse");
    overlay.remove("b");
    let parsed = assert_overlay_fixture(
        "f_overlay_remove_only",
        &metadata,
        &overlay,
        "02020002000506140100000004",
    );
    assert_eq!(parsed.as_object().expect("object").num_fields(), 2);
}

/// Risk pinned: Java's remove-then-put contract is INCONSISTENT on purpose — `put` does not
/// clear `removedFields`, so the views (`get` → null, `numFields`/`fieldNames` exclude the
/// name) disagree with serialization (which INCLUDES the new value). Java probe:
/// `probe_remove_then_put_view numFields=2 get_b=null`, yet the bytes carry b=9. Mirroring
/// only the views (or only the bytes) silently diverges.
#[test]
fn test_overlay_remove_then_put_same_name_serializes_but_views_exclude_it() {
    let metadata = shred_metadata_abcd();
    let base = shred_sorted_base_bytes(&metadata);
    let mut overlay =
        ShreddedObject::over_serialized_object(&metadata, &base).expect("base must parse");
    overlay.remove("b");
    overlay
        .put("b", VariantValue::of_int8(9))
        .expect("re-put b");
    assert_eq!(overlay.get("b"), None, "removed wins in get, like Java");
    assert_eq!(overlay.num_fields(), 2, "the views exclude the name");
    assert_eq!(overlay.field_names(), vec!["a", "c"]);
    let parsed = assert_overlay_fixture(
        "f_overlay_remove_then_put_same",
        &metadata,
        &overlay,
        "02030001020005070814010000000c0904",
    );
    assert_eq!(
        parsed.as_object().expect("object").get("b"),
        Some(&VariantValue::of_int8(9)),
        "serialization includes the re-put value, like Java"
    );
}

/// Java's `f_overlay_data_255` — merged dataSize exactly 255 keeps 1-byte offsets.
const SHRED_DATA_255_HEX: &str = "02040001020300050708ff14010000000578043cf200000000070e151c232a31383f464d545b626970777e858c939aa1a8afb6bdc4cbd2d9e0e7eef5fc030a11181f262d343b424950575e656c737a81888f969da4abb2b9c0c7ced5dce3eaf1f8ff060d141b222930373e454c535a61686f767d848b9299a0a7aeb5bcc3cad1d8dfe6edf4fb020910171e252c333a41484f565d646b727980878e959ca3aab1b8bfc6cdd4dbe2e9f0f7fe050c131a21282f363d444b525960676e757c838a91989fa6adb4bbc2c9d0d7dee5ecf3fa01080f161d242b323940474e555c636a71787f868d949ba2a9b0b7bec5ccd3dae1e8eff6fd040b121920272e353c434a51585f666d747b82899097";

/// Java's `f_overlay_data_256` — merged dataSize 256 escalates to 2-byte offsets.
const SHRED_DATA_256_HEX: &str = "0604000102030000050007000800000114010000000578043cf300000000070e151c232a31383f464d545b626970777e858c939aa1a8afb6bdc4cbd2d9e0e7eef5fc030a11181f262d343b424950575e656c737a81888f969da4abb2b9c0c7ced5dce3eaf1f8ff060d141b222930373e454c535a61686f767d848b9299a0a7aeb5bcc3cad1d8dfe6edf4fb020910171e252c333a41484f565d646b727980878e959ca3aab1b8bfc6cdd4dbe2e9f0f7fe050c131a21282f363d444b525960676e757c838a91989fa6adb4bbc2c9d0d7dee5ecf3fa01080f161d242b323940474e555c636a71787f868d949ba2a9b0b7bec5ccd3dae1e8eff6fd040b121920272e353c434a51585f666d747b828990979e";

/// Risk pinned: the offset width is selected over the MERGED data size — crossing 255 bytes
/// flips every offset entry from 1 to 2 bytes (Java probe fixtures straddle the boundary).
#[test]
fn test_overlay_merged_data_size_width_escalation_at_255_boundary() {
    let metadata = shred_metadata_abcd();
    let base = shred_sorted_base_bytes(&metadata);
    // Base dataSize is 8; a binary put adds 5 + N — N = 242/243 lands dataSize 255/256.
    for (n, java_hex, expected_offset_size) in [
        (242usize, SHRED_DATA_255_HEX, 1usize),
        (243usize, SHRED_DATA_256_HEX, 2usize),
    ] {
        let mut overlay =
            ShreddedObject::over_serialized_object(&metadata, &base).expect("base must parse");
        overlay
            .put("d", VariantValue::of_binary(java_binary_payload(n)))
            .expect("put d");
        let parsed = assert_overlay_fixture(
            &format!("f_overlay_data_{}", 13 + n),
            &metadata,
            &overlay,
            java_hex,
        );
        assert_eq!(parsed.as_object().expect("object").num_fields(), 4);
        let header = hex(java_hex)[0];
        assert_eq!(
            1 + usize::from((header & 0b1100) >> 2),
            expected_offset_size,
            "dataSize {} must select {expected_offset_size}-byte offsets",
            13 + n
        );
    }
}

/// Risk pinned: the 65535→65536 crossing flips offsets from 2 to 3 bytes; pinned as
/// CRC32+length+prefix like the other large write fixtures (full-byte diffed out-of-band at
/// generation time by Java's own round-trip read).
#[test]
fn test_overlay_merged_data_size_width_escalation_at_65535_boundary() {
    let metadata = shred_metadata_abcd();
    let base = shred_sorted_base_bytes(&metadata);
    for (n, expected_crc, expected_len, expected_prefix) in [
        (
            65522usize,
            1698129659u32,
            65551usize,
            "0604000102030000050007000800ffff14010000000578043cf2ff000000070e151c232a31383f464d545b626970777e858c939aa1a8afb6bdc4cbd2d9e0e7ee",
        ),
        (
            65523usize,
            1883417984u32,
            65557usize,
            "0a040001020300000005000007000008000000000114010000000578043cf3ff000000070e151c232a31383f464d545b626970777e858c939aa1a8afb6bdc4cb",
        ),
    ] {
        let mut overlay =
            ShreddedObject::over_serialized_object(&metadata, &base).expect("base must parse");
        overlay
            .put("d", VariantValue::of_binary(java_binary_payload(n)))
            .expect("put d");
        let bytes = overlay.to_bytes().expect("must serialize");
        assert_eq!(bytes.len(), expected_len, "length for n={n}");
        assert_eq!(crc32(&bytes), expected_crc, "Java CRC32 for n={n}");
        assert_eq!(
            &bytes[..64],
            hex(expected_prefix).as_slice(),
            "prefix for n={n}"
        );
        VariantValue::parse(&metadata, &bytes).expect("the output must re-parse");
    }
}

/// Risk pinned: on a name collision the SHREDDED value wins (Java: a replaced backing field
/// never enters the unshredded map) — flipping the precedence would resurrect overwritten
/// data.
#[test]
fn test_overlay_shredded_value_wins_collision_matches_java() {
    let metadata = shred_metadata_abcd();
    let base = shred_sorted_base_bytes(&metadata);
    let mut overlay =
        ShreddedObject::over_serialized_object(&metadata, &base).expect("base must parse");
    overlay
        .put("c", VariantValue::of_boolean(false))
        .expect("override c");
    let parsed = assert_overlay_fixture(
        "f_overlay_shredded_wins",
        &metadata,
        &overlay,
        "0203000102000507081401000000057808",
    );
    assert_eq!(
        parsed.as_object().expect("object").get("c"),
        Some(&VariantValue::of_boolean(false)),
        "the shredded false must win over the backing's true"
    );
}

/// Risk pinned: `fieldIdSize = sizeOf(metadata.dictionarySize())` — the dictionary SIZE,
/// not the largest id used; a 256-name dictionary forces 2-byte field ids in the overlay
/// output even though every used id fits one byte.
#[test]
fn test_overlay_field_id_width_follows_dictionary_size_not_max_id() {
    let names: Vec<String> = (0..256).map(|index| format!("k{index:03}")).collect();
    let metadata = VariantMetadata::from_field_names(names).expect("256-name dictionary");
    let mut builder = VariantObjectBuilder::new(&metadata);
    builder
        .put("k000", VariantValue::of_int32(1))
        .expect("k000");
    builder.put("k001", VariantValue::of_int8(2)).expect("k001");
    let base = VariantValue::Object(builder.build())
        .to_bytes(&metadata)
        .expect("base");
    assert_eq!(base, hex("12020000010000050714010000000c02"), "f_base_wide");
    let mut overlay =
        ShreddedObject::over_serialized_object(&metadata, &base).expect("base must parse");
    overlay
        .put("k002", VariantValue::of_boolean(true))
        .expect("k002");
    assert_overlay_fixture(
        "f_overlay_fieldid_width_dict256",
        &metadata,
        &overlay,
        "12030000010002000005070814010000000c0204",
    );
}

/// Risk pinned: duplicate resolved field names in the backing reject at serialization time
/// (Java's ImmutableMap throws "Multiple entries with same key") UNLESS the duplicated name
/// is replaced/removed — then BOTH occurrences leave the unshredded side and the overlay
/// serializes (Java fixture). An eager constructor-time rejection would over-reject.
#[test]
fn test_overlay_duplicate_backing_field_names_reject_unless_replaced() {
    let metadata =
        VariantMetadata::from_field_names(["b", "a", "b"]).expect("duplicates are legal");
    // Java's f_base_dup_field: ids ["b", "a", "b"] -> int8 1 / 2 / 3.
    let base = hex("0203000102000204060c010c020c03");
    let unreplaced =
        ShreddedObject::over_serialized_object(&metadata, &base).expect("base must parse");
    let error = unreplaced
        .size_in_bytes()
        .expect_err("surviving duplicate names must reject");
    assert!(
        error
            .to_string()
            .contains("duplicate unshredded field name: b"),
        "the error must name the duplicate, got: {error}"
    );

    let mut replaced =
        ShreddedObject::over_serialized_object(&metadata, &base).expect("base must parse");
    replaced
        .put("b", VariantValue::of_int8(9))
        .expect("replace b");
    assert_overlay_fixture(
        "f_overlay_dup_field_replaced",
        &metadata,
        &replaced,
        "020201000002040c020c09",
    );

    // REMOVING the duplicated name also drops both occurrences and serializes — Java probe
    // (ReviewerShredProbe r5): `r5_dup_removed 02010100020c02`.
    let mut removed =
        ShreddedObject::over_serialized_object(&metadata, &base).expect("base must parse");
    removed.remove("b");
    assert_overlay_fixture("r5_dup_removed", &metadata, &removed, "02010100020c02");

    // The VIEWS over the unreplaced dup backing dedup the name and resolve `get` through the
    // backing's binary probe — Java probe: `r5_dup_views numFields=2 fieldNames=[a, b]
    // get_b=Variant(type=INT8, value=3)` (the THIRD stored field wins the probe).
    let views = ShreddedObject::over_serialized_object(&metadata, &base).expect("base must parse");
    assert_eq!(views.num_fields(), 2);
    assert_eq!(views.field_names(), vec!["a", "b"]);
    assert_eq!(
        views.get("b"),
        Some(&VariantValue::of_int8(3)),
        "Java's binary probe resolves the third stored field"
    );
}

// Reviewer probe provenance (2026-06-11): /tmp/variant-probe/ReviewerShredProbe.java against
// the same pinned-1.10.0 CPW classpath as VariantShredFixtureGen; the r*_ hex lines quoted
// below are that program's verbatim output.

/// Risk pinned: THE slice-range-math pin — a backing whose field DATA order differs from
/// field-name order with DISTINCT value widths (fields a/b/c stored in name order, data
/// placed c-first/a-second/b-third at widths 2/7/5). B1's sorted-distinct-offsets length
/// scheme gives each untouched field its true span; an adjacent-field-subtraction or
/// off-by-one range recorder would copy the WRONG bytes for every disordered third-party
/// object (silent value corruption Java reads as different data). Java probe r1.
#[test]
fn test_overlay_disordered_backing_preserves_untouched_fields_verbatim() {
    let metadata = shred_metadata_abcd();
    // r1_base: ids a,b,c with offsets a@2, b@9, c@0 — a is LONG-form "hi" (7 bytes), b is
    // int32 42 (5 bytes), c is int8 7 (2 bytes); data length 14.
    let base = hex("02030001020209000e0c0740020000006869142a000000");
    let mut overlay =
        ShreddedObject::over_serialized_object(&metadata, &base).expect("base must parse");
    overlay
        .put("b", VariantValue::of_int8(9))
        .expect("replace b");
    let java_hex = "02030001020007090b400200000068690c090c07";
    let parsed = assert_overlay_fixture("r1_overlay_put_b", &metadata, &overlay, java_hex);
    assert!(
        java_hex.contains(LONG_FORM_HI),
        "untouched a must keep its long-form bytes verbatim"
    );
    assert_eq!(
        parsed.as_object().expect("object").get("c"),
        Some(&VariantValue::of_int8(7)),
        "untouched c must keep its value"
    );

    // The EMPTY overlay re-orders the scrambled data into name order (offsets recomputed)
    // while every field's VALUE bytes stay verbatim — incl. b's int32 run "142a000000".
    let empty = ShreddedObject::over_serialized_object(&metadata, &base).expect("base must parse");
    let java_empty_hex = "020300010200070c0e40020000006869142a0000000c07";
    assert_overlay_fixture("r1_overlay_empty", &metadata, &empty, java_empty_hex);
    assert!(
        java_empty_hex.contains("142a000000"),
        "untouched b's int32 bytes must survive the re-ordering verbatim"
    );
}

/// Risk pinned: a NESTED object with a non-minimal (2-byte) offset width as an untouched
/// field VALUE survives a sibling replacement byte-verbatim — the verbatim contract must
/// hold for whole non-canonical subtrees, not just leaf encodings. Java probe r2.
#[test]
fn test_overlay_preserves_nested_non_canonical_object_field_verbatim() {
    let metadata = shred_metadata_abcd();
    // r2_base: a = int8 1, b = the inner object {c: int8 5} with OVERSIZED 2-byte offsets.
    let base = hex("0202000100020b0c01060102000002000c05");
    let mut overlay =
        ShreddedObject::over_serialized_object(&metadata, &base).expect("base must parse");
    overlay
        .put("a", VariantValue::of_int8(2))
        .expect("replace a");
    let java_hex = "0202000100020b0c02060102000002000c05";
    assert_overlay_fixture("r2_overlay_put_a", &metadata, &overlay, java_hex);
    assert!(
        java_hex.contains("060102000002000c05"),
        "the nested object's oversized-width encoding must survive verbatim"
    );
    // The canonical writer would re-encode the inner object with 1-byte offsets.
    let parsed = VariantValue::parse(&metadata, &base).expect("must parse");
    let canonical = parsed.to_bytes(&metadata).expect("must re-serialize");
    assert_ne!(
        canonical, base,
        "re-encoding canonicalizes the inner widths; only the overlay preserves them"
    );
}

/// Risk pinned: a backing with 4-byte OUTER offsets carrying small data parses, records the
/// wide-offset slice ranges correctly, and re-headers to canonical widths while the field
/// values (incl. a long-form string) stay verbatim. Java probe r3.
#[test]
fn test_overlay_over_4_byte_offset_backing_re_headers_and_preserves_values() {
    let metadata = shred_metadata_abcd();
    // r3_base: header 0x0e (offsetSize 4), a = LONG-form "hi", b = int8 7.
    let base = hex("0e020001000000000700000009000000400200000068690c07");
    let mut overlay =
        ShreddedObject::over_serialized_object(&metadata, &base).expect("base must parse");
    overlay.put("c", VariantValue::of_boolean(true)).expect("c");
    assert_overlay_fixture(
        "r3_overlay_put_c",
        &metadata,
        &overlay,
        "02030001020007090a400200000068690c0704",
    );
}

/// Risk pinned (DOCUMENTED DIVERGENCE, not parity): a backing with a MALFORMED untouched
/// field — lazy Java slices it blind and `writeTo` SUCCEEDS, copying the bad bytes verbatim
/// (probe r4: `r4_overlay_untouched_malformed 020200010002030c02fc`; only a later
/// `get("b")` throws `UnsupportedOperationException: Unknown primitive physical type: 63`).
/// The eager Rust door rejects at `over_serialized_object` for the untouched AND the
/// replaced-field twin (Java serializes the replaced twin fine too: probe
/// `r4_overlay_replaced_malformed 020200010002040c010c09`) — the accepted-set divergence
/// the module doc declares; loud-vs-blind, never silent corruption.
#[test]
fn test_overlay_rejects_malformed_untouched_field_where_java_serializes_blind() {
    let metadata = shred_metadata_abcd();
    // r4_base: a = int8 1, b = the undefined primitive type id 63 (header 0xFC).
    let base = hex("020200010002030c01fc");
    let error = ShreddedObject::over_serialized_object(&metadata, &base)
        .expect_err("the eager parse must reject the malformed field");
    assert!(
        error
            .to_string()
            .contains("Unknown primitive physical type"),
        "the rejection must name the malformed field's type, got: {error}"
    );
}

/// Risk pinned: the constructed (non-serialized) backing branch materializes untouched
/// fields consistently for size AND write — MAIN's semantics. 1.10.0's FIRST serialization
/// of this shape is corrupt (probe p1: the SerializationState ctor merges into the caller's
/// live map but writeTo iterates the pre-merge copy; re-read fails with
/// IndexOutOfBoundsException); the pinned bytes are Java's SELF-HEALED second serialization
/// (`f_overlay_constructed_backing`), which equals what MAIN emits directly.
#[test]
fn test_overlay_constructed_backing_matches_java_self_healed_output() {
    let metadata = shred_metadata_abcd();
    let mut builder = VariantObjectBuilder::new(&metadata);
    builder.put("a", VariantValue::of_int32(1)).expect("a");
    builder.put("b", VariantValue::of_string("x")).expect("b");
    let mut overlay = ShreddedObject::over_object(&metadata, builder.build());
    overlay.put("c", VariantValue::of_boolean(true)).expect("c");
    assert_overlay_fixture(
        "f_overlay_constructed_backing",
        &metadata,
        &overlay,
        "0203000102000507081401000000057804",
    );
}

/// Risk pinned: `put` of a name outside the metadata dictionary rejects with Java's exact
/// precondition message (probe: `IllegalArgumentException: Cannot find field name in
/// metadata: zzz`) — silently accepting it would emit a dangling field id.
#[test]
fn test_overlay_put_unknown_name_rejects_with_javas_message() {
    let metadata = shred_metadata_abcd();
    let base = shred_sorted_base_bytes(&metadata);
    let mut overlay =
        ShreddedObject::over_serialized_object(&metadata, &base).expect("base must parse");
    let error = overlay
        .put("zzz", VariantValue::of_int32(1))
        .expect_err("unknown names must reject");
    assert_eq!(error.kind(), crate::ErrorKind::DataInvalid);
    assert!(
        error
            .to_string()
            .contains("Cannot find field name in metadata: zzz"),
        "the error must carry Java's message, got: {error}"
    );
}

/// Risk pinned: `remove` of a name present nowhere is a harmless no-op (Java has no
/// precondition; probe: `probe_remove_nonexistent_equals_base true`) — the serialization
/// still equals the untouched backing.
#[test]
fn test_overlay_remove_nonexistent_name_is_a_no_op() {
    let metadata = shred_metadata_abcd();
    let base = shred_sorted_base_bytes(&metadata);
    let mut overlay =
        ShreddedObject::over_serialized_object(&metadata, &base).expect("base must parse");
    overlay.remove("zzz");
    assert_eq!(
        overlay.to_bytes().expect("must serialize"),
        base,
        "removing an absent name must not change the output"
    );
}

/// Risk pinned: overlaying non-object bytes rejects with Java's `asObject()` contract
/// ("Not an object: %s") — Java's typed signature makes the case unrepresentable, so the
/// byte-taking Rust door mirrors the closest runtime contract (documented in shredded.rs).
#[test]
fn test_overlay_over_non_object_bytes_rejects() {
    let metadata = shred_metadata_abcd();
    let error = ShreddedObject::over_serialized_object(&metadata, &[0x0C, 0x07])
        .expect_err("an int8 value is not an object");
    assert!(
        error.to_string().contains("Not an object: Int8"),
        "the error must mirror asObject(), got: {error}"
    );
    // An array is not an object either ([true] — header 0x03, count 1, offsets [0, 1]).
    let error = ShreddedObject::over_serialized_object(&metadata, &[0x03, 0x01, 0x00, 0x01, 0x04])
        .expect_err("an array value is not an object");
    assert!(
        error.to_string().contains("Not an object: Array"),
        "the error must mirror asObject(), got: {error}"
    );
}

/// Risk pinned: like Java, sizing does NOT resolve field ids but writing does — over a
/// LYING-sorted dictionary (get(0) resolves a name the sorted-path id() lookup cannot
/// re-find) `size_in_bytes` succeeds and `write_to` fails with Java's checkState message
/// (probe: `IllegalStateException: Invalid metadata, missing: c`).
#[test]
fn test_overlay_write_time_id_resolution_fails_like_java_on_lying_sorted_dictionary() {
    // ["c", "a", "b"] with the sorted flag SET — id("c") binary-search misses.
    let metadata = VariantMetadata::parse(&hex("110300010203636162")).expect("must parse");
    let overlay = ShreddedObject::over_serialized_object(&metadata, &hex("02010000020c05"))
        .expect("the backing parses (get(0) resolves the name)");
    let size = overlay
        .size_in_bytes()
        .expect("sizing must succeed, like Java (no id resolution)");
    let mut buffer = vec![0u8; size];
    let error = overlay
        .write_to(&mut buffer, 0)
        .expect_err("write-time id re-resolution must fail");
    assert!(
        error.to_string().contains("Invalid metadata, missing: c"),
        "the error must carry Java's checkState message, got: {error}"
    );
}

/// Risk pinned: the up-front span door is about ATOMICITY — an undersized buffer must
/// reject BEFORE any byte is written (the B2 reviewer rule: pin the no-side-effect
/// property, not just `is_err`).
#[test]
fn test_overlay_write_to_undersized_buffer_leaves_buffer_untouched() {
    let metadata = shred_metadata_abcd();
    let base = shred_sorted_base_bytes(&metadata);
    let overlay =
        ShreddedObject::over_serialized_object(&metadata, &base).expect("base must parse");
    let size = overlay.size_in_bytes().expect("must size");
    let mut buffer = vec![0u8; size - 1];
    assert!(overlay.write_to(&mut buffer, 0).is_err());
    assert!(
        buffer.iter().all(|byte| *byte == 0),
        "the buffer must be untouched after the door rejects"
    );
}

/// Risk pinned: the overlay's serialization recursion is depth-guarded — a manually built
/// arrays-within-arrays bomb as a shredded value must error, never overflow the stack
/// (Java recurses unbounded; same guard family as write.rs).
#[test]
fn test_overlay_serialization_depth_guard_rejects_nesting_bombs() {
    let metadata = shred_metadata_abcd();
    let mut bomb = VariantValue::Array(VariantArray::new());
    for _ in 0..(MAX_NESTING_DEPTH + 2) {
        let mut outer = VariantArray::new();
        outer.push(bomb);
        bomb = VariantValue::Array(outer);
    }
    let mut overlay = ShreddedObject::new(&metadata);
    overlay.put("a", bomb).expect("a is in the metadata");
    let error = overlay
        .size_in_bytes()
        .expect_err("a nesting bomb must be rejected");
    assert!(
        error.to_string().contains("nesting depth"),
        "the error must name the depth guard, got: {error}"
    );
}

/// Risk pinned: the read-side views follow Java's exact precedence — removed FIRST, then
/// shredded, then the backing lookup; `field_names` is the UTF-16-sorted dedup of the
/// merged set (Java's TreeSet `nameSet()`).
#[test]
fn test_overlay_get_and_field_names_follow_javas_precedence() {
    let metadata = shred_metadata_abcd();
    let base = shred_sorted_base_bytes(&metadata);
    let mut overlay =
        ShreddedObject::over_serialized_object(&metadata, &base).expect("base must parse");
    overlay
        .put("b", VariantValue::of_string("yy"))
        .expect("override b");
    overlay.put("d", VariantValue::of_int8(2)).expect("add d");
    overlay.remove("a");
    assert_eq!(overlay.get("a"), None, "removed wins");
    assert_eq!(
        overlay.get("b"),
        Some(&VariantValue::of_string("yy")),
        "shredded beats the backing"
    );
    assert_eq!(
        overlay.get("c"),
        Some(&VariantValue::of_boolean(true)),
        "the backing answers untouched names"
    );
    assert_eq!(overlay.get("zzz"), None, "unknown names miss");
    assert_eq!(overlay.field_names(), vec!["b", "c", "d"]);
    assert_eq!(overlay.num_fields(), 3);
}

// ===== VariantVisitor: traversal order, defaults, finally, depth ===========================
// The traversal-order oracle is the Java 1.10.0 event log printed by
// VariantShredFixtureGen's LoggingVisitor over the f_visit_value fixture (provenance above).

/// Java's `f_visit_value`: `{ arr: [int8 1, "s", {x: true}], prim: int32 3 }` over
/// `Variants.metadata("arr", "prim", "x")`, parsed from serialized bytes so the field order
/// is the stored order.
const JAVA_VISIT_VALUE: &str = "0202000100101503030002040a0c0105730201020001041403000000";

/// A recording visitor mirroring the Java generator's `LoggingVisitor` line format.
#[derive(Default)]
struct LoggingVisitor {
    log: Vec<String>,
}

/// Java `PhysicalType.name()` for the types the visit fixture touches.
fn java_physical_type_name(physical_type: PhysicalType) -> &'static str {
    match physical_type {
        PhysicalType::Int8 => "INT8",
        PhysicalType::Int32 => "INT32",
        PhysicalType::String => "STRING",
        PhysicalType::BooleanTrue => "BOOLEAN_TRUE",
        other => panic!("the visit fixture does not contain {other:?}"),
    }
}

impl VariantVisitor for LoggingVisitor {
    type Output = String;

    fn object(
        &mut self,
        _object: &VariantObject,
        field_names: &[&str],
        field_results: Vec<Option<String>>,
    ) -> Option<String> {
        let results: Vec<String> = field_results
            .into_iter()
            .map(|result| result.unwrap_or_else(|| "null".to_string()))
            .collect();
        self.log.push(format!(
            "object(names=[{}], results=[{}])",
            field_names.join(", "),
            results.join(", ")
        ));
        Some("OBJ".to_string())
    }

    fn array(
        &mut self,
        _array: &VariantArray,
        element_results: Vec<Option<String>>,
    ) -> Option<String> {
        let results: Vec<String> = element_results
            .into_iter()
            .map(|result| result.unwrap_or_else(|| "null".to_string()))
            .collect();
        self.log
            .push(format!("array(results=[{}])", results.join(", ")));
        Some("ARR".to_string())
    }

    fn primitive(&mut self, primitive: &VariantPrimitive) -> Option<String> {
        let name = java_physical_type_name(primitive.physical_type());
        self.log.push(format!("primitive({name})"));
        Some(name.to_string())
    }

    fn before_array_element(&mut self, index: usize) {
        self.log.push(format!("beforeArrayElement({index})"));
    }

    fn after_array_element(&mut self, index: usize) {
        self.log.push(format!("afterArrayElement({index})"));
    }

    fn before_object_field(&mut self, field_name: &str) {
        self.log.push(format!("beforeObjectField({field_name})"));
    }

    fn after_object_field(&mut self, field_name: &str) {
        self.log.push(format!("afterObjectField({field_name})"));
    }
}

/// Risk pinned: the traversal ORDER — object fields in stored order with hooks around each,
/// array elements by index, results threaded post-order — must match Java's bytecode-pinned
/// sequence exactly (the Java-generated event log is the oracle); a reordering silently
/// changes every traversal-built artifact (shredded parquet writers build on this).
#[test]
fn test_visitor_traversal_order_matches_javas_event_log() {
    let metadata =
        VariantMetadata::from_field_names(["arr", "prim", "x"]).expect("3-name dictionary");
    let value = VariantValue::parse(&metadata, &hex(JAVA_VISIT_VALUE))
        .expect("the visit fixture must parse");
    let mut visitor = LoggingVisitor::default();
    let result = visit_value(&value, &mut visitor).expect("the traversal must succeed");
    assert_eq!(result, Some("OBJ".to_string()), "Java: visitresult OBJ");
    let expected: Vec<&str> = vec![
        "beforeObjectField(arr)",
        "beforeArrayElement(0)",
        "primitive(INT8)",
        "afterArrayElement(0)",
        "beforeArrayElement(1)",
        "primitive(STRING)",
        "afterArrayElement(1)",
        "beforeArrayElement(2)",
        "beforeObjectField(x)",
        "primitive(BOOLEAN_TRUE)",
        "afterObjectField(x)",
        "object(names=[x], results=[BOOLEAN_TRUE])",
        "afterArrayElement(2)",
        "array(results=[INT8, STRING, OBJ])",
        "afterObjectField(arr)",
        "beforeObjectField(prim)",
        "primitive(INT32)",
        "afterObjectField(prim)",
        "object(names=[arr, prim], results=[ARR, INT32])",
    ];
    assert_eq!(
        visitor.log, expected,
        "the event sequence must equal Java's"
    );
}

/// Risk pinned: every default matches Java's — the callbacks return `null` (`None`) and the
/// hooks no-op, so a defaults-only visitor traverses any tree and yields `None`.
#[test]
fn test_visitor_defaults_match_javas_nulls() {
    struct DefaultsOnly;
    impl VariantVisitor for DefaultsOnly {
        type Output = u32;
    }
    let metadata =
        VariantMetadata::from_field_names(["arr", "prim", "x"]).expect("3-name dictionary");
    let value = VariantValue::parse(&metadata, &hex(JAVA_VISIT_VALUE)).expect("must parse");
    let result = visit_value(&value, &mut DefaultsOnly).expect("must traverse");
    assert_eq!(result, None, "Java's defaults return null at every level");
}

/// Risk pinned: `visit_variant` delegates to the value exactly like Java's
/// `visit(Variant, visitor)` overload.
#[test]
fn test_visitor_visit_variant_delegates_to_the_value() {
    let metadata =
        VariantMetadata::from_field_names(["arr", "prim", "x"]).expect("3-name dictionary");
    let value = VariantValue::parse(&metadata, &hex(JAVA_VISIT_VALUE)).expect("must parse");
    let variant = Variant::of(metadata, value);
    let mut by_variant = LoggingVisitor::default();
    let mut by_value = LoggingVisitor::default();
    let variant_result = visit_variant(&variant, &mut by_variant).expect("must traverse");
    let value_result = visit_value(variant.value(), &mut by_value).expect("must traverse");
    assert_eq!(variant_result, value_result);
    assert_eq!(by_variant.log, by_value.log);
}

/// Risk pinned: Java recurses into `object.get(fieldName)` — the NAME LOOKUP — and a
/// non-name-sorted object's unfindable field makes it throw a NullPointerException; the
/// Rust driver must fail LOUD with a named error, and (Java `finally`) the after-hook must
/// still run for the failing field before the error propagates.
#[test]
fn test_visitor_lookup_miss_errors_where_java_npes_and_after_hook_still_runs() {
    // Stored order ["b", "a"] is NOT name-sorted: find("b") hits at index 0, find("a")
    // probes index 0 ("b"), walks left, and misses — exactly Java's binary-search miss.
    let object = VariantObject::from_fields(vec![
        VariantObjectField {
            field_id: 1,
            name: "b".to_string(),
            value: VariantValue::of_int8(1),
        },
        VariantObjectField {
            field_id: 0,
            name: "a".to_string(),
            value: VariantValue::of_int8(2),
        },
    ]);
    let value = VariantValue::Object(object);
    let mut visitor = LoggingVisitor::default();
    let error = visit_value(&value, &mut visitor).expect_err("the miss must fail loud");
    assert!(
        error.to_string().contains("field a cannot be re-found"),
        "the error must name the field, got: {error}"
    );
    assert_eq!(
        visitor.log,
        vec![
            "beforeObjectField(b)",
            "primitive(INT8)",
            "afterObjectField(b)",
            "beforeObjectField(a)",
            "afterObjectField(a)", // Java finally: the after-hook fires on the error path
        ],
        "the after-hook must run for the failing field, like Java's finally"
    );
}

/// Risk pinned: the visit recursion shares the parser's depth budget — a constructed
/// 130-deep bomb errors (never a stack overflow) while a tree at the parser's maximum depth
/// is visitable (the accepted sets must stay equal so every parseable value can be
/// visited).
#[test]
fn test_visitor_depth_guard_matches_the_parsers_budget() {
    // 130 nesting levels: rejected.
    let mut bomb = VariantValue::Array(VariantArray::new());
    for _ in 0..(MAX_NESTING_DEPTH + 2) {
        let mut outer = VariantArray::new();
        outer.push(bomb);
        bomb = VariantValue::Array(outer);
    }
    struct DefaultsOnly;
    impl VariantVisitor for DefaultsOnly {
        type Output = ();
    }
    let error = visit_value(&bomb, &mut DefaultsOnly).expect_err("a bomb must be rejected");
    assert!(
        error.to_string().contains("nesting depth"),
        "the error must name the depth guard, got: {error}"
    );

    // A tree the PARSER accepts (depth == MAX_NESTING_DEPTH) must be visitable: build the
    // deepest parseable shape — nested arrays — through the writer and re-parse it.
    let metadata = empty_metadata();
    let mut deepest = VariantValue::of_int8(7);
    for _ in 0..MAX_NESTING_DEPTH {
        let mut outer = VariantArray::new();
        outer.push(deepest);
        deepest = VariantValue::Array(outer);
    }
    let bytes = deepest.to_bytes(&metadata).expect("the writer accepts it");
    let parsed = VariantValue::parse(&metadata, &bytes).expect("the parser accepts it");
    visit_value(&parsed, &mut DefaultsOnly).expect("the visitor must accept what parse accepts");
}
