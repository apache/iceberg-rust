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

use std::sync::LazyLock;

use chrono::{DateTime, NaiveDate, NaiveDateTime};
use regex::RegexSet;

use crate::expr::{
    BinaryExpression, BoundPredicate, Predicate, Reference, SetExpression, UnaryExpression,
};
use crate::spec::{Datum, PrimitiveLiteral, PrimitiveType};

// Java binds before sanitizing so pushed-down numeric date/timestamp literals retain their types.
// https://github.com/apache/iceberg/pull/8149
pub(crate) fn unbind(predicate: &BoundPredicate) -> Predicate {
    match predicate {
        BoundPredicate::AlwaysTrue => Predicate::AlwaysTrue,
        BoundPredicate::AlwaysFalse => Predicate::AlwaysFalse,
        BoundPredicate::And(expr) => {
            let [left, right] = expr.inputs();
            unbind(left).and(unbind(right))
        }
        BoundPredicate::Or(expr) => {
            let [left, right] = expr.inputs();
            unbind(left).or(unbind(right))
        }
        BoundPredicate::Not(expr) => !unbind(expr.inputs()[0]),
        BoundPredicate::Unary(expr) => Predicate::Unary(UnaryExpression::new(
            expr.op(),
            Reference::new(expr.term().to_string()),
        )),
        BoundPredicate::Binary(expr) => Predicate::Binary(BinaryExpression::new(
            expr.op(),
            Reference::new(expr.term().to_string()),
            expr.literal().clone(),
        )),
        BoundPredicate::Set(expr) => Predicate::Set(SetExpression::new(
            expr.op(),
            Reference::new(expr.term().to_string()),
            expr.literals().clone(),
        )),
    }
}

pub(crate) fn sanitize(predicate: &Predicate, now_micros: i64) -> Predicate {
    match predicate {
        Predicate::AlwaysTrue | Predicate::AlwaysFalse | Predicate::Unary(_) => predicate.clone(),
        Predicate::And(expr) => {
            let [left, right] = expr.inputs();
            sanitize(left, now_micros).and(sanitize(right, now_micros))
        }
        Predicate::Or(expr) => {
            let [left, right] = expr.inputs();
            sanitize(left, now_micros).or(sanitize(right, now_micros))
        }
        Predicate::Not(expr) => !sanitize(expr.inputs()[0], now_micros),
        Predicate::Binary(expr) => Predicate::Binary(BinaryExpression::new(
            expr.op(),
            expr.term().clone(),
            Datum::string(sanitize_literal(expr.literal(), now_micros)),
        )),
        Predicate::Set(expr) => Predicate::Set(SetExpression::new(
            expr.op(),
            expr.term().clone(),
            expr.literals()
                .iter()
                .map(|value| Datum::string(sanitize_literal(value, now_micros)))
                .collect(),
        )),
    }
}

fn sanitize_literal(value: &Datum, now_micros: i64) -> String {
    match (value.data_type(), value.literal()) {
        (PrimitiveType::Date, PrimitiveLiteral::Int(days)) => sanitize_date(*days, now_micros),
        (PrimitiveType::Time, _) => "(time)".into(),
        (PrimitiveType::Timestamp | PrimitiveType::Timestamptz, PrimitiveLiteral::Long(micros)) => {
            sanitize_timestamp(*micros, now_micros)
        }
        (
            PrimitiveType::TimestampNs | PrimitiveType::TimestamptzNs,
            PrimitiveLiteral::Long(nanos),
        ) => sanitize_timestamp(nanos.div_euclid(1000), now_micros),
        (_, PrimitiveLiteral::Int(value)) => sanitize_number(f64::from(*value), "int"),
        (_, PrimitiveLiteral::Long(value)) => sanitize_number(*value as f64, "int"),
        (_, PrimitiveLiteral::Float(value)) => sanitize_number(f64::from(value.0), "float"),
        (_, PrimitiveLiteral::Double(value)) => sanitize_number(value.0, "float"),
        (_, PrimitiveLiteral::String(value)) => sanitize_string(value, now_micros),
        (_, PrimitiveLiteral::Boolean(value)) => hash(&value.to_string()),
        (_, PrimitiveLiteral::UInt128(value)) => hash(&uuid::Uuid::from_u128(*value).to_string()),
        (PrimitiveType::Decimal { scale, .. }, PrimitiveLiteral::Int128(value)) => {
            hash(&decimal_string(*value, *scale))
        }
        (_, PrimitiveLiteral::Binary(value)) => {
            // Java sanitizes ByteBuffer.toString(), which describes the buffer, not its contents.
            hash(&format!(
                "java.nio.HeapByteBuffer[pos=0 lim={0} cap={0}]",
                value.len()
            ))
        }
        _ => "(redacted)".into(),
    }
}

fn sanitize_number(value: f64, kind: &str) -> String {
    let digits = if value == 0.0 {
        1
    } else {
        (value.abs().log10() as i32).wrapping_add(1)
    };
    format!("({digits}-digit-{kind})")
}

fn sanitize_date(days: i32, now_micros: i64) -> String {
    let today = now_micros.div_euclid(86_400_000_000);
    let diff = (today - i64::from(days)).abs();
    let direction = if today > i64::from(days) {
        "ago"
    } else {
        "from-now"
    };
    if diff == 0 {
        "(date-today)".into()
    } else if diff < 90 {
        format!("(date-{diff}-days-{direction})")
    } else {
        "(date)".into()
    }
}

fn sanitize_timestamp(micros: i64, now_micros: i64) -> String {
    let diff = (i128::from(now_micros) - i128::from(micros)).abs();
    let direction = if now_micros > micros {
        "ago"
    } else {
        "from-now"
    };
    let hours = diff / 3_600_000_000;
    if diff < 300_000_000 {
        "(timestamp-about-now)".into()
    } else if hours <= 72 {
        format!("(timestamp-{hours}-hours-{direction})")
    } else if hours < 90 * 24 {
        format!("(timestamp-{}-days-{direction})", hours / 24)
    } else {
        "(timestamp)".into()
    }
}

fn sanitize_string(value: &str, now_micros: i64) -> String {
    static PATTERNS: LazyLock<Result<RegexSet, regex::Error>> = LazyLock::new(|| {
        RegexSet::new([
            r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$",
            r"^[0-9]{2}:[0-9]{2}(:[0-9]{2}(.[0-9]{1,9})?)?$",
            r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}(:[0-9]{2}(.[0-9]{1,9})?)?$",
            r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}(:[0-9]{2}(.[0-9]{1,9})?)?([-+][0-9]{2}:[0-9]{2}|Z)$",
        ])
    });
    if let Ok(patterns) = &*PATTERNS {
        let matched = patterns.matches(value);
        if matched.matched(0) {
            if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
                let days = date
                    .signed_duration_since(DateTime::UNIX_EPOCH.date_naive())
                    .num_days();
                return sanitize_date(days as i32, now_micros);
            }
        } else if matched.matched(1) {
            return "(time)".into();
        } else if matched.matched(2) || matched.matched(3) {
            let timestamp = if value.get(16..17) == Some(":") {
                value.to_owned()
            } else {
                format!("{}:00{}", &value[..16], &value[16..])
            };
            let micros = if matched.matched(2) {
                timestamp
                    .parse::<NaiveDateTime>()
                    .map(|t| t.and_utc().timestamp_micros())
            } else {
                DateTime::parse_from_rfc3339(&timestamp).map(|t| t.timestamp_micros())
            };
            if let Ok(micros) = micros {
                return sanitize_timestamp(micros, now_micros);
            }
        }
    }
    hash(value)
}

fn hash(value: &str) -> String {
    match murmur3::murmur3_32(&mut value.as_bytes(), 0) {
        Ok(hash) => format!("(hash-{:08x})", (hash & 0x7fff_ffff) % 0x7fff_ffff),
        Err(_) => "(redacted)".into(),
    }
}

fn decimal_string(value: i128, scale: u32) -> String {
    let digits = value.unsigned_abs().to_string();
    let sign = if value < 0 { "-" } else { "" };
    let exponent = digits.len() as i64 - i64::from(scale) - 1;
    if scale == 0 {
        value.to_string()
    } else if exponent < -6 {
        let (first, rest) = digits.split_at(1);
        let fraction = if rest.is_empty() {
            String::new()
        } else {
            format!(".{rest}")
        };
        format!("{sign}{first}{fraction}E{exponent}")
    } else if digits.len() > scale as usize {
        let (whole, fraction) = digits.split_at(digits.len() - scale as usize);
        format!("{sign}{whole}.{fraction}")
    } else {
        format!(
            "{sign}0.{}{digits}",
            "0".repeat(scale as usize - digits.len())
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::{Value, json};

    use super::*;
    use crate::expr::{Bind, PredicateOperator};
    use crate::spec::{NestedField, Schema, Type};

    fn datum(kind: &PrimitiveType, value: &Value) -> Datum {
        let literal = match kind {
            PrimitiveType::Boolean => PrimitiveLiteral::Boolean(value.as_bool().unwrap()),
            PrimitiveType::Int | PrimitiveType::Date => {
                PrimitiveLiteral::Int(value.as_i64().unwrap() as i32)
            }
            PrimitiveType::Long
            | PrimitiveType::Time
            | PrimitiveType::Timestamp
            | PrimitiveType::Timestamptz
            | PrimitiveType::TimestampNs
            | PrimitiveType::TimestamptzNs => PrimitiveLiteral::Long(value.as_i64().unwrap()),
            PrimitiveType::Float => PrimitiveLiteral::Float(
                value
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| value.to_string())
                    .parse::<f32>()
                    .unwrap()
                    .into(),
            ),
            PrimitiveType::Double => PrimitiveLiteral::Double(
                value
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| value.to_string())
                    .parse::<f64>()
                    .unwrap()
                    .into(),
            ),
            PrimitiveType::String => PrimitiveLiteral::String(value.as_str().unwrap().into()),
            PrimitiveType::Uuid => PrimitiveLiteral::UInt128(
                uuid::Uuid::parse_str(value.as_str().unwrap())
                    .unwrap()
                    .as_u128(),
            ),
            PrimitiveType::Decimal { .. } => {
                PrimitiveLiteral::Int128(value.as_str().unwrap().replace('.', "").parse().unwrap())
            }
            PrimitiveType::Binary | PrimitiveType::Fixed(_) => {
                let value = value.as_str().unwrap();
                PrimitiveLiteral::Binary(
                    (0..value.len())
                        .step_by(2)
                        .map(|i| u8::from_str_radix(&value[i..i + 2], 16).unwrap())
                        .collect(),
                )
            }
            _ => panic!("Unsupported test literal {kind}"),
        };
        Datum::new(kind.clone(), literal)
    }

    fn operations() -> [(PredicateOperator, &'static str); 14] {
        use PredicateOperator::*;
        [
            (IsNull, "is-null"),
            (NotNull, "not-null"),
            (IsNan, "is-nan"),
            (NotNan, "not-nan"),
            (LessThan, "lt"),
            (LessThanOrEq, "lt-eq"),
            (GreaterThan, "gt"),
            (GreaterThanOrEq, "gt-eq"),
            (Eq, "eq"),
            (NotEq, "not-eq"),
            (StartsWith, "starts-with"),
            (NotStartsWith, "not-starts-with"),
            (In, "in"),
            (NotIn, "not-in"),
        ]
    }

    fn expression_json(predicate: &Predicate) -> Value {
        let name = |op| {
            operations()
                .iter()
                .find(|(candidate, _)| *candidate == op)
                .unwrap()
                .1
        };
        match predicate {
            Predicate::AlwaysTrue => json!(true),
            Predicate::AlwaysFalse => json!(false),
            Predicate::And(expr) | Predicate::Or(expr) => {
                let [left, right] = expr.inputs();
                json!({"type":if matches!(predicate,Predicate::And(_)) {"and"} else {"or"},"left":expression_json(left),"right":expression_json(right)})
            }
            Predicate::Not(expr) => json!({"type":"not","child":expression_json(expr.inputs()[0])}),
            Predicate::Unary(expr) => json!({"type":name(expr.op()),"term":expr.term().name()}),
            Predicate::Binary(expr) => {
                json!({"type":name(expr.op()),"term":expr.term().name(),"value":expr.literal().to_human_string()})
            }
            Predicate::Set(expr) => {
                json!({"type":name(expr.op()),"term":expr.term().name(),"values":expr.literals().iter().map(Datum::to_human_string).collect::<Vec<_>>()})
            }
        }
    }

    fn normalize_sets(value: &mut Value) {
        if let Some(object) = value.as_object_mut() {
            for (key, value) in object {
                if key == "values" {
                    let values = value.as_array_mut().unwrap();
                    values.sort_by_key(Value::to_string);
                    values.dedup();
                } else {
                    normalize_sets(value);
                }
            }
        }
    }

    #[test]
    fn test_sanitized_filters_against_pinned_java() {
        let fixtures: Value = serde_json::from_str(include_str!(
            "../../testdata/metrics/sanitized-filters.json"
        ))
        .unwrap();
        let now = fixtures["now_micros"].as_i64().unwrap();
        for case in fixtures["cases"].as_array().unwrap() {
            let input_type: PrimitiveType = serde_json::from_value(case["type"].clone()).unwrap();
            let field_type: PrimitiveType =
                serde_json::from_value(case.get("field_type").unwrap_or(&case["type"]).clone())
                    .unwrap();
            let schema = Arc::new(
                Schema::builder()
                    .with_fields(vec![
                        NestedField::optional(1, "value", Type::Primitive(field_type)).into(),
                    ])
                    .build()
                    .unwrap(),
            );
            let reference = Reference::new(case["reference"].as_str().unwrap_or("value"));
            let op = case["op"].as_str().unwrap_or("eq");
            let predicate = if op == "compound" {
                reference
                    .clone()
                    .less_than(datum(&input_type, &case["lower"]))
                    .and(
                        !reference
                            .clone()
                            .greater_than_or_equal_to(datum(&input_type, &case["upper"])),
                    )
                    .or(reference.is_null())
            } else {
                let op = operations().iter().find(|(_, name)| *name == op).unwrap().0;
                if op.is_unary() {
                    Predicate::Unary(UnaryExpression::new(op, reference))
                } else if op.is_set() {
                    Predicate::Set(SetExpression::new(
                        op,
                        reference,
                        case["value"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|v| datum(&input_type, v))
                            .collect(),
                    ))
                } else {
                    Predicate::Binary(BinaryExpression::new(
                        op,
                        reference,
                        datum(&input_type, &case["value"]),
                    ))
                }
            };
            let bound = predicate
                .bind(schema, case["case_sensitive"].as_bool().unwrap_or(true))
                .unwrap();
            let mut actual = expression_json(&sanitize(&unbind(&bound), now));
            let mut expected = case
                .get("rust_expected")
                .unwrap_or(&case["expected"])
                .clone();
            if case.get("rust_expected").is_some() {
                assert!(case["known_difference"].is_string());
                assert_ne!(expected, case["expected"]);
            }
            normalize_sets(&mut actual);
            normalize_sets(&mut expected);
            assert_eq!(actual, expected, "fixture {}", case["name"]);
        }
    }
}
