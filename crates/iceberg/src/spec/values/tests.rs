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

//! Tests for Iceberg value types

use apache_avro::to_value;
use apache_avro::types::Value;
use chrono::DateTime;
use ordered_float::OrderedFloat;
use rust_decimal::Decimal;
use serde_bytes::ByteBuf;
use serde_json::Value as JsonValue;
use uuid::Uuid;

use crate::ErrorKind;
use crate::avro::schema_to_avro_schema;
use crate::spec::Schema;
use crate::spec::Type::Primitive;
use crate::spec::datatypes::{ListType, MapType, NestedField, PrimitiveType, StructType, Type};
use crate::spec::values::datum::{INT_MAX, INT_MIN, LONG_MAX, LONG_MIN};
use crate::spec::values::serde::_serde;
use crate::spec::values::{Datum, Literal, Map, PrimitiveLiteral, RawLiteral, Struct};

fn check_json_serde(json: &str, expected_literal: Literal, expected_type: &Type) {
    let raw_json_value = serde_json::from_str::<JsonValue>(json).unwrap();
    let desered_literal = Literal::try_from_json(raw_json_value.clone(), expected_type).unwrap();
    assert_eq!(desered_literal, Some(expected_literal.clone()));

    let expected_json_value: JsonValue = expected_literal.try_into_json(expected_type).unwrap();
    let sered_json = serde_json::to_string(&expected_json_value).unwrap();
    let parsed_json_value = serde_json::from_str::<JsonValue>(&sered_json).unwrap();

    assert_eq!(parsed_json_value, raw_json_value);
}

fn check_avro_bytes_serde(input: Vec<u8>, expected_datum: Datum, expected_type: &PrimitiveType) {
    let raw_schema = r#""bytes""#;
    let schema = apache_avro::Schema::parse_str(raw_schema).unwrap();

    let bytes = ByteBuf::from(input);
    let datum = Datum::try_from_bytes(&bytes, expected_type.clone()).unwrap();
    assert_eq!(datum, expected_datum);

    let mut writer = apache_avro::Writer::new(&schema, Vec::new());
    writer.append_ser(datum.to_bytes().unwrap()).unwrap();
    let encoded = writer.into_inner().unwrap();
    let reader = apache_avro::Reader::with_schema(&schema, &*encoded).unwrap();

    for record in reader {
        let result = apache_avro::from_value::<ByteBuf>(&record.unwrap()).unwrap();
        let desered_datum = Datum::try_from_bytes(&result, expected_type.clone()).unwrap();
        assert_eq!(desered_datum, expected_datum);
    }
}

fn check_convert_with_avro(expected_literal: Literal, expected_type: &Type) {
    let fields = vec![NestedField::required(1, "col", expected_type.clone()).into()];
    let schema = Schema::builder()
        .with_fields(fields.clone())
        .build()
        .unwrap();
    let avro_schema = schema_to_avro_schema("test", &schema).unwrap();
    let struct_type = Type::Struct(StructType::new(fields));
    let struct_literal = Literal::Struct(Struct::from_iter(vec![Some(expected_literal.clone())]));

    let mut writer = apache_avro::Writer::new(&avro_schema, Vec::new());
    let raw_literal = RawLiteral::try_from(struct_literal.clone(), &struct_type).unwrap();
    writer.append_ser(raw_literal).unwrap();
    let encoded = writer.into_inner().unwrap();

    let reader = apache_avro::Reader::new(&*encoded).unwrap();
    for record in reader {
        let result = apache_avro::from_value::<RawLiteral>(&record.unwrap()).unwrap();
        let desered_literal = result.try_into(&struct_type).unwrap().unwrap();
        assert_eq!(desered_literal, struct_literal);
    }
}

fn check_serialize_avro(literal: Literal, ty: &Type, expect_value: Value) {
    let expect_value = Value::Record(vec![("col".to_string(), expect_value)]);

    let fields = vec![NestedField::required(1, "col", ty.clone()).into()];
    let schema = Schema::builder()
        .with_fields(fields.clone())
        .build()
        .unwrap();
    let avro_schema = schema_to_avro_schema("test", &schema).unwrap();
    let struct_type = Type::Struct(StructType::new(fields));
    let struct_literal = Literal::Struct(Struct::from_iter(vec![Some(literal.clone())]));
    let mut writer = apache_avro::Writer::new(&avro_schema, Vec::new());
    let raw_literal = RawLiteral::try_from(struct_literal.clone(), &struct_type).unwrap();
    let value = to_value(raw_literal)
        .unwrap()
        .resolve(&avro_schema)
        .unwrap();
    writer.append_value_ref(&value).unwrap();
    let encoded = writer.into_inner().unwrap();

    let reader = apache_avro::Reader::new(&*encoded).unwrap();
    for record in reader {
        assert_eq!(record.unwrap(), expect_value);
    }
}

#[test]
fn json_boolean() {
    let record = r#"true"#;

    check_json_serde(
        record,
        Literal::Primitive(PrimitiveLiteral::Boolean(true)),
        &Type::Primitive(PrimitiveType::Boolean),
    );
}

#[test]
fn json_int() {
    let record = r#"32"#;

    check_json_serde(
        record,
        Literal::Primitive(PrimitiveLiteral::Int(32)),
        &Type::Primitive(PrimitiveType::Int),
    );
}

#[test]
fn json_long() {
    let record = r#"32"#;

    check_json_serde(
        record,
        Literal::Primitive(PrimitiveLiteral::Long(32)),
        &Type::Primitive(PrimitiveType::Long),
    );
}

#[test]
fn json_float() {
    let record = r#"1.0"#;

    check_json_serde(
        record,
        Literal::Primitive(PrimitiveLiteral::Float(OrderedFloat(1.0))),
        &Type::Primitive(PrimitiveType::Float),
    );
}

#[test]
fn json_double() {
    let record = r#"1.0"#;

    check_json_serde(
        record,
        Literal::Primitive(PrimitiveLiteral::Double(OrderedFloat(1.0))),
        &Type::Primitive(PrimitiveType::Double),
    );
}

#[test]
fn json_date() {
    let record = r#""2017-11-16""#;

    check_json_serde(
        record,
        Literal::Primitive(PrimitiveLiteral::Int(17486)),
        &Type::Primitive(PrimitiveType::Date),
    );
}

#[test]
fn json_time() {
    let record = r#""22:31:08.123456""#;

    check_json_serde(
        record,
        Literal::Primitive(PrimitiveLiteral::Long(81068123456)),
        &Type::Primitive(PrimitiveType::Time),
    );
}

#[test]
fn json_timestamp() {
    let record = r#""2017-11-16T22:31:08.123456""#;

    check_json_serde(
        record,
        Literal::Primitive(PrimitiveLiteral::Long(1510871468123456)),
        &Type::Primitive(PrimitiveType::Timestamp),
    );
}

#[test]
fn json_timestamptz() {
    let record = r#""2017-11-16T22:31:08.123456+00:00""#;

    check_json_serde(
        record,
        Literal::Primitive(PrimitiveLiteral::Long(1510871468123456)),
        &Type::Primitive(PrimitiveType::Timestamptz),
    );
}

#[test]
fn json_string() {
    let record = r#""iceberg""#;

    check_json_serde(
        record,
        Literal::Primitive(PrimitiveLiteral::String("iceberg".to_string())),
        &Type::Primitive(PrimitiveType::String),
    );
}

#[test]
fn json_uuid() {
    let record = r#""f79c3e09-677c-4bbd-a479-3f349cb785e7""#;

    check_json_serde(
        record,
        Literal::Primitive(PrimitiveLiteral::UInt128(
            Uuid::parse_str("f79c3e09-677c-4bbd-a479-3f349cb785e7")
                .unwrap()
                .as_u128(),
        )),
        &Type::Primitive(PrimitiveType::Uuid),
    );
}

#[test]
fn json_decimal() {
    let record = r#""14.20""#;

    check_json_serde(
        record,
        Literal::Primitive(PrimitiveLiteral::Int128(1420)),
        &Type::decimal(28, 2).unwrap(),
    );
}

#[test]
fn json_struct() {
    let record = r#"{"1": 1, "2": "bar", "3": null}"#;

    check_json_serde(
        record,
        Literal::Struct(Struct::from_iter(vec![
            Some(Literal::Primitive(PrimitiveLiteral::Int(1))),
            Some(Literal::Primitive(PrimitiveLiteral::String(
                "bar".to_string(),
            ))),
            None,
        ])),
        &Type::Struct(StructType::new(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(3, "address", Type::Primitive(PrimitiveType::String)).into(),
        ])),
    );
}

#[test]
fn json_list() {
    let record = r#"[1, 2, 3, null]"#;

    check_json_serde(
        record,
        Literal::List(vec![
            Some(Literal::Primitive(PrimitiveLiteral::Int(1))),
            Some(Literal::Primitive(PrimitiveLiteral::Int(2))),
            Some(Literal::Primitive(PrimitiveLiteral::Int(3))),
            None,
        ]),
        &Type::List(ListType {
            element_field: NestedField::list_element(0, Type::Primitive(PrimitiveType::Int), true)
                .into(),
        }),
    );
}

#[test]
fn json_map() {
    let record = r#"{ "keys": ["a", "b", "c"], "values": [1, 2, null] }"#;

    check_json_serde(
        record,
        Literal::Map(Map::from([
            (
                Literal::Primitive(PrimitiveLiteral::String("a".to_string())),
                Some(Literal::Primitive(PrimitiveLiteral::Int(1))),
            ),
            (
                Literal::Primitive(PrimitiveLiteral::String("b".to_string())),
                Some(Literal::Primitive(PrimitiveLiteral::Int(2))),
            ),
            (
                Literal::Primitive(PrimitiveLiteral::String("c".to_string())),
                None,
            ),
        ])),
        &Type::Map(MapType {
            key_field: NestedField::map_key_element(0, Type::Primitive(PrimitiveType::String))
                .into(),
            value_field: NestedField::map_value_element(
                1,
                Type::Primitive(PrimitiveType::Int),
                true,
            )
            .into(),
        }),
    );
}

#[test]
fn avro_bytes_boolean() {
    let bytes = vec![1u8];

    check_avro_bytes_serde(bytes, Datum::bool(true), &PrimitiveType::Boolean);
}

#[test]
fn avro_bytes_int() {
    let bytes = vec![32u8, 0u8, 0u8, 0u8];

    check_avro_bytes_serde(bytes, Datum::int(32), &PrimitiveType::Int);
}

#[test]
fn avro_bytes_long() {
    let bytes = vec![32u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];

    check_avro_bytes_serde(bytes, Datum::long(32), &PrimitiveType::Long);
}

#[test]
fn avro_bytes_long_from_int() {
    let bytes = vec![32u8, 0u8, 0u8, 0u8];

    check_avro_bytes_serde(bytes, Datum::long(32), &PrimitiveType::Long);
}

#[test]
fn avro_bytes_float() {
    let bytes = vec![0u8, 0u8, 128u8, 63u8];

    check_avro_bytes_serde(bytes, Datum::float(1.0), &PrimitiveType::Float);
}

#[test]
fn avro_bytes_double() {
    let bytes = vec![0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 240u8, 63u8];

    check_avro_bytes_serde(bytes, Datum::double(1.0), &PrimitiveType::Double);
}

#[test]
fn avro_bytes_double_from_float() {
    let bytes = vec![0u8, 0u8, 128u8, 63u8];

    check_avro_bytes_serde(bytes, Datum::double(1.0), &PrimitiveType::Double);
}

#[test]
fn avro_bytes_string() {
    let bytes = vec![105u8, 99u8, 101u8, 98u8, 101u8, 114u8, 103u8];

    check_avro_bytes_serde(bytes, Datum::string("iceberg"), &PrimitiveType::String);
}

#[test]
fn avro_bytes_decimal() {
    // (input_bytes, decimal_num, expect_scale, expect_precision)
    let cases = vec![
        (vec![4u8, 210u8], 1234, 2, 38),
        (vec![251u8, 46u8], -1234, 2, 38),
        (vec![4u8, 210u8], 1234, 3, 38),
        (vec![251u8, 46u8], -1234, 3, 38),
        (vec![42u8], 42, 2, 1),
        (vec![214u8], -42, 2, 1),
    ];

    for (input_bytes, decimal_num, expect_scale, expect_precision) in cases {
        check_avro_bytes_serde(
            input_bytes,
            Datum::decimal_with_precision(
                Decimal::new(decimal_num, expect_scale),
                expect_precision,
            )
            .unwrap(),
            &PrimitiveType::Decimal {
                precision: expect_precision,
                scale: expect_scale,
            },
        );
    }
}

#[test]
fn avro_bytes_decimal_expect_error() {
    // (decimal_num, expect_scale, expect_precision)
    let cases = vec![(1234, 2, 1)];

    for (decimal_num, expect_scale, expect_precision) in cases {
        let result = Datum::decimal_with_precision(
            Decimal::new(decimal_num, expect_scale),
            expect_precision,
        );
        assert!(result.is_err(), "expect error but got {result:?}");
        assert_eq!(
            result.unwrap_err().kind(),
            ErrorKind::DataInvalid,
            "expect error DataInvalid",
        );
    }
}

fn check_raw_literal_bytes_serde_via_avro(
    input_bytes: Vec<u8>,
    expected_literal: Literal,
    expected_type: &Type,
) {
    use apache_avro::types::Value;

    // Create an Avro bytes value and deserialize it through the RawLiteral path
    let avro_value = Value::Bytes(input_bytes);
    let raw_literal: _serde::RawLiteral = apache_avro::from_value(&avro_value).unwrap();
    let result = raw_literal.try_into(expected_type).unwrap();
    assert_eq!(result, Some(expected_literal));
}

fn check_raw_literal_bytes_error_via_avro(input_bytes: Vec<u8>, expected_type: &Type) {
    use apache_avro::types::Value;

    let avro_value = Value::Bytes(input_bytes);
    let raw_literal: _serde::RawLiteral = apache_avro::from_value(&avro_value).unwrap();
    let result = raw_literal.try_into(expected_type);
    assert!(result.is_err(), "Expected error but got: {result:?}");
}

#[test]
fn test_raw_literal_bytes_binary() {
    let bytes = vec![1u8, 2u8, 3u8, 4u8, 5u8];
    check_raw_literal_bytes_serde_via_avro(
        bytes.clone(),
        Literal::binary(bytes),
        &Type::Primitive(PrimitiveType::Binary),
    );
}

#[test]
fn test_raw_literal_bytes_binary_empty() {
    let bytes = vec![];
    check_raw_literal_bytes_serde_via_avro(
        bytes.clone(),
        Literal::binary(bytes),
        &Type::Primitive(PrimitiveType::Binary),
    );
}

#[test]
fn test_raw_literal_bytes_fixed_correct_length() {
    let bytes = vec![1u8, 2u8, 3u8, 4u8];
    check_raw_literal_bytes_serde_via_avro(
        bytes.clone(),
        Literal::fixed(bytes),
        &Type::Primitive(PrimitiveType::Fixed(4)),
    );
}

#[test]
fn test_raw_literal_bytes_fixed_wrong_length() {
    let bytes = vec![1u8, 2u8, 3u8]; // 3 bytes, but expecting 4
    check_raw_literal_bytes_error_via_avro(bytes, &Type::Primitive(PrimitiveType::Fixed(4)));
}

#[test]
fn test_raw_literal_bytes_fixed_empty_correct_length() {
    let bytes = vec![];
    check_raw_literal_bytes_serde_via_avro(
        bytes.clone(),
        Literal::fixed(bytes),
        &Type::Primitive(PrimitiveType::Fixed(0)),
    );
}

#[test]
fn test_raw_literal_bytes_uuid_correct_length() {
    let uuid_bytes = vec![
        0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd,
        0xef,
    ];
    let expected_uuid = u128::from_be_bytes([
        0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd,
        0xef,
    ]);
    check_raw_literal_bytes_serde_via_avro(
        uuid_bytes,
        Literal::Primitive(PrimitiveLiteral::UInt128(expected_uuid)),
        &Type::Primitive(PrimitiveType::Uuid),
    );
}

#[test]
fn test_raw_literal_bytes_uuid_wrong_length() {
    let bytes = vec![1u8, 2u8, 3u8]; // 3 bytes, but UUID needs 16
    check_raw_literal_bytes_error_via_avro(bytes, &Type::Primitive(PrimitiveType::Uuid));
}

#[test]
fn test_raw_literal_bytes_decimal_precision_4_scale_2() {
    // Precision 4 requires 2 bytes
    let decimal_bytes = vec![0x04, 0xd2]; // 1234 in 2 bytes
    let expected_decimal = 1234i128;
    check_raw_literal_bytes_serde_via_avro(
        decimal_bytes,
        Literal::Primitive(PrimitiveLiteral::Int128(expected_decimal)),
        &Type::Primitive(PrimitiveType::Decimal {
            precision: 4,
            scale: 2,
        }),
    );
}

#[test]
fn test_raw_literal_bytes_decimal_precision_4_negative() {
    // Precision 4 requires 2 bytes, negative number
    let decimal_bytes = vec![0xfb, 0x2e]; // -1234 in 2 bytes
    let expected_decimal = -1234i128;
    check_raw_literal_bytes_serde_via_avro(
        decimal_bytes,
        Literal::Primitive(PrimitiveLiteral::Int128(expected_decimal)),
        &Type::Primitive(PrimitiveType::Decimal {
            precision: 4,
            scale: 2,
        }),
    );
}

#[test]
fn test_raw_literal_bytes_decimal_precision_9_scale_2() {
    // Precision 9 requires 4 bytes
    let decimal_bytes = vec![0x00, 0x12, 0xd6, 0x87]; // 1234567 in 4 bytes
    let expected_decimal = 1234567i128;
    check_raw_literal_bytes_serde_via_avro(
        decimal_bytes,
        Literal::Primitive(PrimitiveLiteral::Int128(expected_decimal)),
        &Type::Primitive(PrimitiveType::Decimal {
            precision: 9,
            scale: 2,
        }),
    );
}

#[test]
fn test_raw_literal_bytes_decimal_precision_18_scale_2() {
    // Precision 18 requires 8 bytes
    let decimal_bytes = vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0xd2]; // 1234 in 8 bytes
    let expected_decimal = 1234i128;
    check_raw_literal_bytes_serde_via_avro(
        decimal_bytes,
        Literal::Primitive(PrimitiveLiteral::Int128(expected_decimal)),
        &Type::Primitive(PrimitiveType::Decimal {
            precision: 18,
            scale: 2,
        }),
    );
}

#[test]
fn test_raw_literal_bytes_decimal_precision_38_scale_2() {
    // Precision 38 requires 16 bytes (maximum precision)
    let decimal_bytes = vec![
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
        0xd2, // 1234 in 16 bytes
    ];
    let expected_decimal = 1234i128;
    check_raw_literal_bytes_serde_via_avro(
        decimal_bytes,
        Literal::Primitive(PrimitiveLiteral::Int128(expected_decimal)),
        &Type::Primitive(PrimitiveType::Decimal {
            precision: 38,
            scale: 2,
        }),
    );
}

#[test]
fn test_raw_literal_bytes_decimal_precision_1_scale_0() {
    // Precision 1 requires 1 byte
    let decimal_bytes = vec![0x07]; // 7 in 1 byte
    let expected_decimal = 7i128;
    check_raw_literal_bytes_serde_via_avro(
        decimal_bytes,
        Literal::Primitive(PrimitiveLiteral::Int128(expected_decimal)),
        &Type::Primitive(PrimitiveType::Decimal {
            precision: 1,
            scale: 0,
        }),
    );
}

#[test]
fn test_raw_literal_bytes_decimal_precision_1_negative() {
    // Precision 1 requires 1 byte, negative number
    let decimal_bytes = vec![0xf9]; // -7 in 1 byte (two's complement)
    let expected_decimal = -7i128;
    check_raw_literal_bytes_serde_via_avro(
        decimal_bytes,
        Literal::Primitive(PrimitiveLiteral::Int128(expected_decimal)),
        &Type::Primitive(PrimitiveType::Decimal {
            precision: 1,
            scale: 0,
        }),
    );
}

#[test]
fn test_raw_literal_bytes_decimal_wrong_length() {
    // 3 bytes provided, but precision 4 requires 2 bytes
    let bytes = vec![1u8, 2u8, 3u8];
    check_raw_literal_bytes_error_via_avro(
        bytes,
        &Type::Primitive(PrimitiveType::Decimal {
            precision: 4,
            scale: 2,
        }),
    );
}

#[test]
fn test_raw_literal_bytes_decimal_wrong_length_too_few() {
    // 1 byte provided, but precision 9 requires 4 bytes
    let bytes = vec![0x42];
    check_raw_literal_bytes_error_via_avro(
        bytes,
        &Type::Primitive(PrimitiveType::Decimal {
            precision: 9,
            scale: 2,
        }),
    );
}

#[test]
fn test_raw_literal_bytes_unsupported_type() {
    let bytes = vec![1u8, 2u8, 3u8, 4u8];
    check_raw_literal_bytes_error_via_avro(bytes, &Type::Primitive(PrimitiveType::Int));
}

#[test]
fn avro_convert_test_int() {
    check_convert_with_avro(
        Literal::Primitive(PrimitiveLiteral::Int(32)),
        &Type::Primitive(PrimitiveType::Int),
    );
}

#[test]
fn avro_convert_test_long() {
    check_convert_with_avro(
        Literal::Primitive(PrimitiveLiteral::Long(32)),
        &Type::Primitive(PrimitiveType::Long),
    );
}

#[test]
fn avro_convert_test_float() {
    check_convert_with_avro(
        Literal::Primitive(PrimitiveLiteral::Float(OrderedFloat(1.0))),
        &Type::Primitive(PrimitiveType::Float),
    );
}

#[test]
fn avro_convert_test_double() {
    check_convert_with_avro(
        Literal::Primitive(PrimitiveLiteral::Double(OrderedFloat(1.0))),
        &Type::Primitive(PrimitiveType::Double),
    );
}

#[test]
fn avro_convert_test_string() {
    check_convert_with_avro(
        Literal::Primitive(PrimitiveLiteral::String("iceberg".to_string())),
        &Type::Primitive(PrimitiveType::String),
    );
}

#[test]
fn avro_convert_test_date() {
    check_convert_with_avro(
        Literal::Primitive(PrimitiveLiteral::Int(17486)),
        &Type::Primitive(PrimitiveType::Date),
    );
}

#[test]
fn avro_convert_test_time() {
    check_convert_with_avro(
        Literal::Primitive(PrimitiveLiteral::Long(81068123456)),
        &Type::Primitive(PrimitiveType::Time),
    );
}

#[test]
fn avro_convert_test_timestamp() {
    check_convert_with_avro(
        Literal::Primitive(PrimitiveLiteral::Long(1510871468123456)),
        &Type::Primitive(PrimitiveType::Timestamp),
    );
}

#[test]
fn avro_convert_test_timestamptz() {
    check_convert_with_avro(
        Literal::Primitive(PrimitiveLiteral::Long(1510871468123456)),
        &Type::Primitive(PrimitiveType::Timestamptz),
    );
}

#[test]
fn avro_convert_test_list() {
    check_convert_with_avro(
        Literal::List(vec![
            Some(Literal::Primitive(PrimitiveLiteral::Int(1))),
            Some(Literal::Primitive(PrimitiveLiteral::Int(2))),
            Some(Literal::Primitive(PrimitiveLiteral::Int(3))),
            None,
        ]),
        &Type::List(ListType {
            element_field: NestedField::list_element(0, Type::Primitive(PrimitiveType::Int), false)
                .into(),
        }),
    );

    check_convert_with_avro(
        Literal::List(vec![
            Some(Literal::Primitive(PrimitiveLiteral::Int(1))),
            Some(Literal::Primitive(PrimitiveLiteral::Int(2))),
            Some(Literal::Primitive(PrimitiveLiteral::Int(3))),
        ]),
        &Type::List(ListType {
            element_field: NestedField::list_element(0, Type::Primitive(PrimitiveType::Int), true)
                .into(),
        }),
    );
}

fn check_convert_with_avro_map(expected_literal: Literal, expected_type: &Type) {
    let fields = vec![NestedField::required(1, "col", expected_type.clone()).into()];
    let schema = Schema::builder()
        .with_fields(fields.clone())
        .build()
        .unwrap();
    let avro_schema = schema_to_avro_schema("test", &schema).unwrap();
    let struct_type = Type::Struct(StructType::new(fields));
    let struct_literal = Literal::Struct(Struct::from_iter(vec![Some(expected_literal.clone())]));

    let mut writer = apache_avro::Writer::new(&avro_schema, Vec::new());
    let raw_literal = RawLiteral::try_from(struct_literal.clone(), &struct_type).unwrap();
    writer.append_ser(raw_literal).unwrap();
    let encoded = writer.into_inner().unwrap();

    let reader = apache_avro::Reader::new(&*encoded).unwrap();
    for record in reader {
        let result = apache_avro::from_value::<RawLiteral>(&record.unwrap()).unwrap();
        let desered_literal = result.try_into(&struct_type).unwrap().unwrap();
        match (&desered_literal, &struct_literal) {
            (Literal::Struct(desered), Literal::Struct(expected)) => {
                match (&desered.fields()[0], &expected.fields()[0]) {
                    (Some(Literal::Map(desered)), Some(Literal::Map(expected))) => {
                        assert!(desered.has_same_content(expected))
                    }
                    _ => {
                        unreachable!()
                    }
                }
            }
            _ => {
                panic!("unexpected literal type");
            }
        }
    }
}

#[test]
fn avro_convert_test_map() {
    check_convert_with_avro_map(
        Literal::Map(Map::from([
            (
                Literal::Primitive(PrimitiveLiteral::Int(1)),
                Some(Literal::Primitive(PrimitiveLiteral::Long(1))),
            ),
            (
                Literal::Primitive(PrimitiveLiteral::Int(2)),
                Some(Literal::Primitive(PrimitiveLiteral::Long(2))),
            ),
            (Literal::Primitive(PrimitiveLiteral::Int(3)), None),
        ])),
        &Type::Map(MapType {
            key_field: NestedField::map_key_element(2, Type::Primitive(PrimitiveType::Int)).into(),
            value_field: NestedField::map_value_element(
                3,
                Type::Primitive(PrimitiveType::Long),
                false,
            )
            .into(),
        }),
    );

    check_convert_with_avro_map(
        Literal::Map(Map::from([
            (
                Literal::Primitive(PrimitiveLiteral::Int(1)),
                Some(Literal::Primitive(PrimitiveLiteral::Long(1))),
            ),
            (
                Literal::Primitive(PrimitiveLiteral::Int(2)),
                Some(Literal::Primitive(PrimitiveLiteral::Long(2))),
            ),
            (
                Literal::Primitive(PrimitiveLiteral::Int(3)),
                Some(Literal::Primitive(PrimitiveLiteral::Long(3))),
            ),
        ])),
        &Type::Map(MapType {
            key_field: NestedField::map_key_element(2, Type::Primitive(PrimitiveType::Int)).into(),
            value_field: NestedField::map_value_element(
                3,
                Type::Primitive(PrimitiveType::Long),
                true,
            )
            .into(),
        }),
    );
}

#[test]
fn avro_convert_test_string_map() {
    check_convert_with_avro_map(
        Literal::Map(Map::from([
            (
                Literal::Primitive(PrimitiveLiteral::String("a".to_string())),
                Some(Literal::Primitive(PrimitiveLiteral::Int(1))),
            ),
            (
                Literal::Primitive(PrimitiveLiteral::String("b".to_string())),
                Some(Literal::Primitive(PrimitiveLiteral::Int(2))),
            ),
            (
                Literal::Primitive(PrimitiveLiteral::String("c".to_string())),
                None,
            ),
        ])),
        &Type::Map(MapType {
            key_field: NestedField::map_key_element(2, Type::Primitive(PrimitiveType::String))
                .into(),
            value_field: NestedField::map_value_element(
                3,
                Type::Primitive(PrimitiveType::Int),
                false,
            )
            .into(),
        }),
    );

    check_convert_with_avro_map(
        Literal::Map(Map::from([
            (
                Literal::Primitive(PrimitiveLiteral::String("a".to_string())),
                Some(Literal::Primitive(PrimitiveLiteral::Int(1))),
            ),
            (
                Literal::Primitive(PrimitiveLiteral::String("b".to_string())),
                Some(Literal::Primitive(PrimitiveLiteral::Int(2))),
            ),
            (
                Literal::Primitive(PrimitiveLiteral::String("c".to_string())),
                Some(Literal::Primitive(PrimitiveLiteral::Int(3))),
            ),
        ])),
        &Type::Map(MapType {
            key_field: NestedField::map_key_element(2, Type::Primitive(PrimitiveType::String))
                .into(),
            value_field: NestedField::map_value_element(
                3,
                Type::Primitive(PrimitiveType::Int),
                true,
            )
            .into(),
        }),
    );
}

#[test]
fn avro_convert_test_record() {
    check_convert_with_avro(
        Literal::Struct(Struct::from_iter(vec![
            Some(Literal::Primitive(PrimitiveLiteral::Int(1))),
            Some(Literal::Primitive(PrimitiveLiteral::String(
                "bar".to_string(),
            ))),
            None,
        ])),
        &Type::Struct(StructType::new(vec![
            NestedField::required(2, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(3, "name", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(4, "address", Type::Primitive(PrimitiveType::String)).into(),
        ])),
    );
}

// # TODO:https://github.com/apache/iceberg-rust/issues/86
// rust avro don't support deserialize any bytes representation now:
// - binary
// - decimal
#[test]
fn avro_convert_test_binary_ser() {
    let literal = Literal::Primitive(PrimitiveLiteral::Binary(vec![1, 2, 3, 4, 5]));
    let ty = Type::Primitive(PrimitiveType::Binary);
    let expect_value = Value::Bytes(vec![1, 2, 3, 4, 5]);
    check_serialize_avro(literal, &ty, expect_value);
}

#[test]
fn avro_convert_test_decimal_ser() {
    let literal = Literal::decimal(12345);
    let ty = Type::Primitive(PrimitiveType::Decimal {
        precision: 9,
        scale: 8,
    });
    let expect_value = Value::Decimal(apache_avro::Decimal::from(12345_i128.to_be_bytes()));
    check_serialize_avro(literal, &ty, expect_value);
}

// # TODO:https://github.com/apache/iceberg-rust/issues/86
// rust avro can't support to convert any byte-like type to fixed in avro now.
// - uuid ser/de
// - fixed ser/de

#[test]
fn test_parse_timestamp() {
    let value = Datum::timestamp_from_str("2021-08-01T01:09:00.0899").unwrap();
    assert_eq!(&format!("{value}"), "2021-08-01 01:09:00.089900");

    let value = Datum::timestamp_from_str("2023-01-06T00:00:00").unwrap();
    assert_eq!(&format!("{value}"), "2023-01-06 00:00:00");

    let value = Datum::timestamp_from_str("2021-08-01T01:09:00.0899+0800");
    assert!(value.is_err(), "Parse timestamp with timezone should fail!");

    let value = Datum::timestamp_from_str("dfa");
    assert!(
        value.is_err(),
        "Parse timestamp with invalid input should fail!"
    );
}

#[test]
fn test_parse_timestamptz() {
    let value = Datum::timestamptz_from_str("2021-08-01T09:09:00.0899+0800").unwrap();
    assert_eq!(&format!("{value}"), "2021-08-01 01:09:00.089900 UTC");

    let value = Datum::timestamptz_from_str("2021-08-01T01:09:00.0899");
    assert!(
        value.is_err(),
        "Parse timestamptz without timezone should fail!"
    );

    let value = Datum::timestamptz_from_str("dfa");
    assert!(
        value.is_err(),
        "Parse timestamptz with invalid input should fail!"
    );
}

#[test]
fn test_datum_ser_deser() {
    let test_fn = |datum: Datum| {
        let json = serde_json::to_value(&datum).unwrap();
        let desered_datum: Datum = serde_json::from_value(json).unwrap();
        assert_eq!(datum, desered_datum);
    };
    let datum = Datum::int(1);
    test_fn(datum);
    let datum = Datum::long(1);
    test_fn(datum);

    let datum = Datum::float(1.0);
    test_fn(datum);
    let datum = Datum::float(0_f32);
    test_fn(datum);
    let datum = Datum::float(-0_f32);
    test_fn(datum);
    let datum = Datum::float(f32::MAX);
    test_fn(datum);
    let datum = Datum::float(f32::MIN);
    test_fn(datum);

    // serde_json can't serialize f32::INFINITY, f32::NEG_INFINITY, f32::NAN
    let datum = Datum::float(f32::INFINITY);
    let json = serde_json::to_string(&datum).unwrap();
    assert!(serde_json::from_str::<Datum>(&json).is_err());
    let datum = Datum::float(f32::NEG_INFINITY);
    let json = serde_json::to_string(&datum).unwrap();
    assert!(serde_json::from_str::<Datum>(&json).is_err());
    let datum = Datum::float(f32::NAN);
    let json = serde_json::to_string(&datum).unwrap();
    assert!(serde_json::from_str::<Datum>(&json).is_err());

    let datum = Datum::double(1.0);
    test_fn(datum);
    let datum = Datum::double(f64::MAX);
    test_fn(datum);
    let datum = Datum::double(f64::MIN);
    test_fn(datum);

    // serde_json can't serialize f32::INFINITY, f32::NEG_INFINITY, f32::NAN
    let datum = Datum::double(f64::INFINITY);
    let json = serde_json::to_string(&datum).unwrap();
    assert!(serde_json::from_str::<Datum>(&json).is_err());
    let datum = Datum::double(f64::NEG_INFINITY);
    let json = serde_json::to_string(&datum).unwrap();
    assert!(serde_json::from_str::<Datum>(&json).is_err());
    let datum = Datum::double(f64::NAN);
    let json = serde_json::to_string(&datum).unwrap();
    assert!(serde_json::from_str::<Datum>(&json).is_err());

    let datum = Datum::string("iceberg");
    test_fn(datum);
    let datum = Datum::bool(true);
    test_fn(datum);
    let datum = Datum::date(17486);
    test_fn(datum);
    let datum = Datum::time_from_hms_micro(22, 15, 33, 111).unwrap();
    test_fn(datum);
    let datum = Datum::timestamp_micros(1510871468123456);
    test_fn(datum);
    let datum = Datum::timestamptz_micros(1510871468123456);
    test_fn(datum);
    let datum = Datum::uuid(Uuid::parse_str("f79c3e09-677c-4bbd-a479-3f349cb785e7").unwrap());
    test_fn(datum);
    let datum = Datum::decimal(1420).unwrap();
    test_fn(datum);
    let datum = Datum::binary(vec![1, 2, 3, 4, 5]);
    test_fn(datum);
    let datum = Datum::fixed(vec![1, 2, 3, 4, 5]);
    test_fn(datum);
}

macro_rules! datum_convert_tests {
        ($($name:ident: ($source:expr => $target_type:ident, $expected:expr),)*) => {
        $(
            #[test]
            fn $name() {
                let source = $source;
                let target_type = PrimitiveType::$target_type;
                let expected = $expected;

                let result = source.to(&Primitive(target_type)).unwrap();
                assert_eq!(result, expected);
            }
        )*
        }
    }

datum_convert_tests! {
    // Date conversions
    convert_date_to_int: (
        Datum::date(12345) => Int,
        Datum::int(12345)
    ),
    convert_int_to_date: (
        Datum::int(12345) => Date,
        Datum::date(12345)
    ),
    convert_date_to_timestamp: (
        Datum::date(1) => Timestamp,
        Datum::timestamp_from_datetime(
            DateTime::parse_from_rfc3339("1970-01-02T00:00:00Z")
                .unwrap()
                .naive_utc()
        )
    ),
    convert_date_to_timestamptz: (
        Datum::date(1) => Timestamptz,
        Datum::timestamptz_from_str("1970-01-02T00:00:00Z").unwrap()
    ),
    convert_date_to_timestamp_nanos: (
        Datum::date(1) => TimestampNs,
        Datum::timestamp_from_datetime(
            DateTime::parse_from_rfc3339("1970-01-02T00:00:00Z")
                .unwrap()
                .naive_utc()
        )
        .to(&Primitive(PrimitiveType::TimestampNs))
        .unwrap()
    ),
    convert_date_to_timestamptz_nanos: (
        Datum::date(1) => TimestamptzNs,
        Datum::timestamptz_from_datetime(
            DateTime::parse_from_rfc3339("1970-01-02T00:00:00Z").unwrap()
        )
        .to(&Primitive(PrimitiveType::TimestamptzNs))
        .unwrap()
    ),
    convert_date_negative_to_timestamp: (
        Datum::date(-1) => Timestamp,
        Datum::timestamp_from_datetime(
            DateTime::parse_from_rfc3339("1969-12-31T00:00:00Z")
                .unwrap()
                .naive_utc()
        )
    ),
    convert_date_to_timestamp_year_9999: (
        Datum::date(2932896) => Timestamp,
        Datum::timestamp_from_datetime(
            DateTime::parse_from_rfc3339("9999-12-31T00:00:00Z")
                .unwrap()
                .naive_utc()
        )
    ),
    // Long conversions
    convert_long_to_int: (
        Datum::long(12345) => Int,
        Datum::int(12345)
    ),
    convert_long_to_int_above_max: (
        Datum::long(INT_MAX as i64 + 1) => Int,
        Datum::new(PrimitiveType::Int, PrimitiveLiteral::AboveMax)
    ),
    convert_long_to_int_below_min: (
        Datum::long(INT_MIN as i64 - 1) => Int,
        Datum::new(PrimitiveType::Int, PrimitiveLiteral::BelowMin)
    ),
    convert_long_to_timestamp: (
        Datum::long(12345) => Timestamp,
        Datum::timestamp_micros(12345)
    ),
    convert_long_to_timestamptz: (
        Datum::long(12345) => Timestamptz,
        Datum::timestamptz_micros(12345)
    ),
    convert_long_to_timestamp_nanos: (
        Datum::long(12345) => TimestampNs,
        Datum::timestamp_nanos(12345)
    ),
    convert_long_to_timestamptz_nanos: (
        Datum::long(12345) => TimestamptzNs,
        Datum::timestamptz_nanos(12345)
    ),
    // Decimal conversions
    convert_decimal_to_long: (
        Datum::decimal(12345).unwrap() => Long,
        Datum::long(12345)
    ),
    convert_decimal_to_long_above_max: (
        Datum::decimal(LONG_MAX as i128 + 1).unwrap() => Long,
        Datum::new(PrimitiveType::Long, PrimitiveLiteral::AboveMax)
    ),
    convert_decimal_to_long_below_min: (
        Datum::decimal(LONG_MIN as i128 - 1).unwrap() => Long,
        Datum::new(PrimitiveType::Long, PrimitiveLiteral::BelowMin)
    ),
    // String conversions
    convert_string_to_boolean: (
        Datum::string("true") => Boolean,
        Datum::bool(true)
    ),
    convert_string_to_int: (
        Datum::string("12345") => Int,
        Datum::int(12345)
    ),
    convert_string_to_long: (
        Datum::string("12345") => Long,
        Datum::long(12345)
    ),
    convert_string_to_timestamp: (
        Datum::string("1925-05-20T19:25:00.000") => Timestamp,
        Datum::timestamp_micros(-1407990900000000)
    ),
    convert_string_to_timestamptz: (
        Datum::string("1925-05-20T19:25:00.000 UTC") => Timestamptz,
        Datum::timestamptz_micros(-1407990900000000)
    ),
    // Timestamp conversions (micros to nanos)
    convert_timestamp_micros_to_timestamp_nanos: (
        Datum::timestamp_micros(12345) => TimestampNs,
        Datum::timestamp_nanos(12345000)
    ),
    convert_timestamp_micros_to_timestamptz_nanos: (
        Datum::timestamp_micros(12345) => TimestamptzNs,
        Datum::timestamptz_nanos(12345000)
    ),
    convert_timestamptz_micros_to_timestamp_nanos: (
        Datum::timestamptz_micros(12345) => TimestampNs,
        Datum::timestamp_nanos(12345000)
    ),
    convert_timestamptz_micros_to_timestamptz_nanos: (
        Datum::timestamptz_micros(12345) => TimestamptzNs,
        Datum::timestamptz_nanos(12345000)
    ),
    // Timestamp conversions (nanos to micros)
    convert_timestamp_nanos_to_timestamp_micros: (
        Datum::timestamp_nanos(12345000) => Timestamp,
        Datum::timestamp_micros(12345)
    ),
    test_datum_timestamp_micros_to_nanos: (
        Datum::timestamp_nanos(12345678) => Timestamp,
        Datum::timestamp_micros(12345)
    ),
    convert_timestamp_nanos_to_timestamptz_micros: (
        Datum::timestamp_nanos(12345000) => Timestamptz,
        Datum::timestamptz_micros(12345)
    ),
    convert_timestamptz_nanos_to_timestamp_micros: (
        Datum::timestamptz_nanos(12345000) => Timestamp,
        Datum::timestamp_micros(12345)
    ),
    convert_timestamptz_nanos_to_timestamptz_micros: (
        Datum::timestamptz_nanos(12345000) => Timestamptz,
        Datum::timestamptz_micros(12345)
    ),
    // Timestamp conversions (nanos to nanos)
    convert_timestamp_nanos_to_timestamp_nanos: (
        Datum::timestamp_nanos(12345) => TimestampNs,
        Datum::timestamp_nanos(12345)
    ),
    convert_timestamp_nanos_to_timestamptz_nanos: (
        Datum::timestamp_nanos(12345) => TimestamptzNs,
        Datum::timestamptz_nanos(12345)
    ),
    convert_timestamptz_nanos_to_timestamp_nanos: (
        Datum::timestamptz_nanos(12345) => TimestampNs,
        Datum::timestamp_nanos(12345)
    ),
    convert_timestamptz_nanos_to_timestamptz_nanos: (
        Datum::timestamptz_nanos(12345) => TimestamptzNs,
        Datum::timestamptz_nanos(12345)
    ),
    // Timestamp to date conversions
    convert_timestamp_to_date: (
        Datum::timestamp_micros(24 * 60 * 60 * 1_000_000) => Date,
        Datum::date(1)
    ),
    convert_timestamptz_to_date: (
        Datum::timestamptz_micros(24 * 60 * 60 * 1_000_000) => Date,
        Datum::date(1)
    ),
    convert_timestamp_nanos_to_date: (
        Datum::timestamp_nanos(24 * 60 * 60 * 1_000_000_000) => Date,
        Datum::date(1)
    ),
    convert_timestamptz_nanos_to_date: (
        Datum::timestamptz_nanos(24 * 60 * 60 * 1_000_000_000) => Date,
        Datum::date(1)
    ),
}

#[test]
fn test_convert_date_to_timestamp_overflow() {
    let date = Datum::date(i32::MAX / 2);

    let result = date.clone().to(&Primitive(PrimitiveType::Timestamp));
    let result = result.expect_err("should overflow");
    assert!(result.to_string().contains("integer overflow"));

    let result = date.clone().to(&Primitive(PrimitiveType::Timestamptz));
    let result = result.expect_err("should overflow");
    assert!(result.to_string().contains("integer overflow"));

    let result = date.clone().to(&Primitive(PrimitiveType::TimestampNs));
    let result = result.expect_err("should overflow");
    assert!(result.to_string().contains("integer overflow"));

    let result = date.clone().to(&Primitive(PrimitiveType::TimestamptzNs));
    let result = result.expect_err("should overflow");
    assert!(result.to_string().contains("integer overflow"));
}

#[test]
fn test_convert_timestamp_to_timestamp_overflow() {
    let ts = Datum::timestamp_micros(i64::MAX / 10);
    let result = ts.clone().to(&Primitive(PrimitiveType::TimestampNs));
    let result = result.expect_err("should overflow");
    assert!(result.to_string().contains("integer overflow"));

    let tstz = Datum::timestamptz_micros(i64::MAX / 10);
    let result = tstz.clone().to(&Primitive(PrimitiveType::TimestamptzNs));
    let result = result.expect_err("should overflow");
    assert!(result.to_string().contains("integer overflow"));
}

#[test]
fn test_iceberg_float_order() {
    // Test float ordering
    let float_values = vec![
        Datum::float(f32::NAN),
        Datum::float(-f32::NAN),
        Datum::float(f32::MAX),
        Datum::float(f32::MIN),
        Datum::float(f32::INFINITY),
        Datum::float(-f32::INFINITY),
        Datum::float(1.0),
        Datum::float(-1.0),
        Datum::float(0.0),
        Datum::float(-0.0),
    ];

    let mut float_sorted = float_values.clone();
    float_sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let float_expected = vec![
        Datum::float(-f32::NAN),
        Datum::float(-f32::INFINITY),
        Datum::float(f32::MIN),
        Datum::float(-1.0),
        Datum::float(-0.0),
        Datum::float(0.0),
        Datum::float(1.0),
        Datum::float(f32::MAX),
        Datum::float(f32::INFINITY),
        Datum::float(f32::NAN),
    ];

    assert_eq!(float_sorted, float_expected);

    // Test double ordering
    let double_values = vec![
        Datum::double(f64::NAN),
        Datum::double(-f64::NAN),
        Datum::double(f64::INFINITY),
        Datum::double(-f64::INFINITY),
        Datum::double(f64::MAX),
        Datum::double(f64::MIN),
        Datum::double(1.0),
        Datum::double(-1.0),
        Datum::double(0.0),
        Datum::double(-0.0),
    ];

    let mut double_sorted = double_values.clone();
    double_sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let double_expected = vec![
        Datum::double(-f64::NAN),
        Datum::double(-f64::INFINITY),
        Datum::double(f64::MIN),
        Datum::double(-1.0),
        Datum::double(-0.0),
        Datum::double(0.0),
        Datum::double(1.0),
        Datum::double(f64::MAX),
        Datum::double(f64::INFINITY),
        Datum::double(f64::NAN),
    ];

    assert_eq!(double_sorted, double_expected);
}

#[test]
fn test_negative_zero_less_than_positive_zero() {
    {
        let neg_zero = Datum::float(-0.0);
        let pos_zero = Datum::float(0.0);

        assert_eq!(
            neg_zero.partial_cmp(&pos_zero),
            Some(std::cmp::Ordering::Less),
            "IEEE 754 totalOrder requires -0.0 < +0.0 on F32"
        );
    }

    {
        let neg_zero = Datum::double(-0.0);
        let pos_zero = Datum::double(0.0);

        assert_eq!(
            neg_zero.partial_cmp(&pos_zero),
            Some(std::cmp::Ordering::Less),
            "IEEE 754 totalOrder requires -0.0 < +0.0 on F64"
        );
    }
}

/// Test Date deserialization from JSON as number (days since epoch).
///
/// This reproduces the scenario from Iceberg Java's TestAddFilesProcedure where:
/// - Date partition columns have initial_default values in manifests
/// - These values are serialized as days since epoch (e.g., 18628 for 2021-01-01)
/// - The JSON schema includes: {"type":"date","initial-default":18628}
///
/// Prior to this fix, Date values in JSON were only parsed from String format ("2021-01-01"),
/// causing initial_default values to be lost during schema deserialization.
///
/// This test ensures both formats are supported:
/// - String format: "2021-01-01" (used in table metadata)
/// - Number format: 18628 (used in initial-default values from add_files)
///
/// See: Iceberg Java TestAddFilesProcedure.addDataPartitionedByDateToPartitioned()
#[test]
fn test_date_from_json_as_number() {
    use serde_json::json;

    // Test Date as number (days since epoch) - used in initial-default from add_files
    let date_number = json!(18628); // 2021-01-01 is 18628 days since 1970-01-01
    let result =
        Literal::try_from_json(date_number, &Type::Primitive(PrimitiveType::Date)).unwrap();
    assert_eq!(
        result,
        Some(Literal::Primitive(PrimitiveLiteral::Int(18628)))
    );

    // Test Date as string - traditional format
    let date_string = json!("2021-01-01");
    let result =
        Literal::try_from_json(date_string, &Type::Primitive(PrimitiveType::Date)).unwrap();
    assert_eq!(
        result,
        Some(Literal::Primitive(PrimitiveLiteral::Int(18628)))
    );

    // Both formats should produce the same Literal value
}
