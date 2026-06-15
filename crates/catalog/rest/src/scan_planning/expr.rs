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

//! Serialization of an [`iceberg::expr::Predicate`] to the Iceberg expression
//! JSON wire format (matching Java `org.apache.iceberg.expressions.ExpressionParser`),
//! used to push a scan filter down to the planning server.
//!
//! This is intentionally conservative: any literal whose JSON encoding we
//! cannot reproduce faithfully (temporals, decimals, binary, uuid, …) makes the
//! whole conversion return `None`, in which case the caller simply omits the
//! `filter` from the request. Server-side filtering is an optimization — the
//! client still applies its own bound predicate when reading — so omitting a
//! filter never changes results.

use iceberg::expr::{Predicate, PredicateOperator};
use iceberg::spec::{Datum, PrimitiveLiteral, PrimitiveType};
use serde_json::{Value, json};

/// Convert a predicate to Iceberg expression JSON, or `None` if it contains a
/// literal we cannot encode losslessly.
pub(crate) fn predicate_to_json(predicate: &Predicate) -> Option<Value> {
    match predicate {
        Predicate::AlwaysTrue => Some(json!({ "type": "true" })),
        Predicate::AlwaysFalse => Some(json!({ "type": "false" })),
        Predicate::And(expr) => {
            let [left, right] = expr.inputs();
            Some(json!({
                "type": "and",
                "left": predicate_to_json(left)?,
                "right": predicate_to_json(right)?,
            }))
        }
        Predicate::Or(expr) => {
            let [left, right] = expr.inputs();
            Some(json!({
                "type": "or",
                "left": predicate_to_json(left)?,
                "right": predicate_to_json(right)?,
            }))
        }
        Predicate::Not(expr) => {
            let [child] = expr.inputs();
            Some(json!({
                "type": "not",
                "child": predicate_to_json(child)?,
            }))
        }
        Predicate::Unary(expr) => {
            let op = op_str(expr.op())?;
            Some(json!({ "type": op, "term": expr.term().name() }))
        }
        Predicate::Binary(expr) => {
            let op = op_str(expr.op())?;
            Some(json!({
                "type": op,
                "term": expr.term().name(),
                "value": datum_to_json(expr.literal())?,
            }))
        }
        Predicate::Set(expr) => {
            let op = op_str(expr.op())?;
            let mut values = Vec::with_capacity(expr.literals().len());
            for datum in expr.literals() {
                values.push(datum_to_json(datum)?);
            }
            Some(json!({
                "type": op,
                "term": expr.term().name(),
                "values": values,
            }))
        }
    }
}

/// Maps an operator to its Iceberg JSON tag, or `None` for any operator we
/// don't recognize (the enum is `#[non_exhaustive]`).
fn op_str(op: PredicateOperator) -> Option<&'static str> {
    Some(match op {
        PredicateOperator::IsNull => "is-null",
        PredicateOperator::NotNull => "not-null",
        PredicateOperator::IsNan => "is-nan",
        PredicateOperator::NotNan => "not-nan",
        PredicateOperator::LessThan => "lt",
        PredicateOperator::LessThanOrEq => "lt-eq",
        PredicateOperator::GreaterThan => "gt",
        PredicateOperator::GreaterThanOrEq => "gt-eq",
        PredicateOperator::Eq => "eq",
        PredicateOperator::NotEq => "not-eq",
        PredicateOperator::StartsWith => "starts-with",
        PredicateOperator::NotStartsWith => "not-starts-with",
        PredicateOperator::In => "in",
        PredicateOperator::NotIn => "not-in",
        _ => return None,
    })
}

/// Encode a literal as Iceberg single-value JSON for the "plain" primitive
/// types. Returns `None` for types whose single-value encoding we don't
/// reproduce here (dates/times/timestamps/decimals/binary/uuid/fixed).
fn datum_to_json(datum: &Datum) -> Option<Value> {
    match (datum.data_type(), datum.literal()) {
        (PrimitiveType::Boolean, PrimitiveLiteral::Boolean(b)) => Some(json!(b)),
        (PrimitiveType::Int, PrimitiveLiteral::Int(i)) => Some(json!(i)),
        (PrimitiveType::Long, PrimitiveLiteral::Long(l)) => Some(json!(l)),
        (PrimitiveType::Float, PrimitiveLiteral::Float(f)) => Some(json!(f.0)),
        (PrimitiveType::Double, PrimitiveLiteral::Double(f)) => Some(json!(f.0)),
        (PrimitiveType::String, PrimitiveLiteral::String(s)) => Some(json!(s)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use iceberg::expr::Reference;
    use iceberg::spec::Datum;

    use super::*;

    #[test]
    fn binary_eq_predicate() {
        let p = Reference::new("id").equal_to(Datum::long(5));
        let json = predicate_to_json(&p).unwrap();
        assert_eq!(json["type"], "eq");
        assert_eq!(json["term"], "id");
        assert_eq!(json["value"], 5);
    }

    #[test]
    fn and_of_unary_and_binary() {
        let p = Reference::new("a")
            .is_null()
            .and(Reference::new("b").greater_than(Datum::int(3)));
        let json = predicate_to_json(&p).unwrap();
        assert_eq!(json["type"], "and");
        assert_eq!(json["left"]["type"], "is-null");
        assert_eq!(json["right"]["type"], "gt");
        assert_eq!(json["right"]["value"], 3);
    }

    #[test]
    fn unsupported_literal_yields_none() {
        // A binary predicate on a date literal cannot be encoded -> None.
        let p = Reference::new("d").equal_to(Datum::date(1));
        assert!(predicate_to_json(&p).is_none());
    }
}
