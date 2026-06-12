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

//! Type-promotion compatibility checks for schema evolution.
//!
//! This module answers a single question: may an existing column whose type is `from` have its type
//! changed to the primitive type `to` during an `UpdateSchema`? It is the Rust port of the Java
//! reference's `org.apache.iceberg.types.TypeUtil.isPromotionAllowed(Type from, Type.PrimitiveType to)`
//! (and the equivalent gate inside `SchemaUpdate.updateColumn`), kept deliberately 1:1 with the Java
//! switch so that schemas evolved here and by Java agree on exactly which promotions are legal.
//!
//! Java promotes a primitive only along three edges, plus the trivial identity case:
//!
//! | from                  | to                    | allowed when                                   |
//! |-----------------------|-----------------------|------------------------------------------------|
//! | `int`                 | `long`                | always                                         |
//! | `float`               | `double`              | always                                         |
//! | `decimal(p, s)`       | `decimal(p2, s)`      | `s == s` (equal scale) and `p <= p2`           |
//! | any type `t`          | the same type `t`     | identity is always a no-op                     |
//!
//! Everything else is rejected. In particular `date -> timestamp` is **not** a Java promotion (the
//! switch has no `DATE` branch), nor is any timestamp/timestamp-with-tz/nanosecond conversion,
//! `fixed -> binary`, any string/uuid/binary widening, any numeric widening beyond the two above, any
//! decimal change that alters the scale or shrinks the precision, and any promotion of a primitive to a
//! nested type. Mirroring this exactly is a correctness requirement: a promotion Rust allows but Java
//! forbids would let a writer evolve a table that Java then rejects (or worse, misreads).

use crate::ensure_data_valid;
use crate::error::Result;
use crate::spec::datatypes::{PrimitiveType, Type};

/// Returns whether the existing type `from` may be promoted to the primitive type `to` during schema
/// evolution, exactly per the Java `TypeUtil.isPromotionAllowed` switch.
///
/// This is the boolean core shared by [`ensure_promotion_allowed`]; callers that want an actionable
/// error (with the offending `from`/`to` in the message) should prefer [`ensure_promotion_allowed`].
///
/// The allowed edges are: the identity case (`from` already equals `to`), `int -> long`,
/// `float -> double`, and `decimal(p, s) -> decimal(p2, s)` with equal scale and non-decreasing
/// precision. Every other pair returns `false`.
pub fn is_promotion_allowed(from: &Type, to: &PrimitiveType) -> bool {
    // A nested `from` (struct/list/map) is never a promotable source: Java's switch has no branch for
    // those type ids, and the identity short-circuit below cannot match a primitive `to` either. Reject
    // it up front so the rest of the function reasons only about a primitive `from`.
    let Type::Primitive(from_primitive) = from else {
        return false;
    };

    // Identity short-circuit, mirroring Java's `from.equals(to)`. Caught before the precision/scale
    // arithmetic so that, e.g., a `decimal(10, 2) -> decimal(10, 2)` no-op is accepted directly.
    if from_primitive == to {
        return true;
    }

    // Only INTEGER, FLOAT, and DECIMAL have non-`false` branches in the Java switch.
    match from_primitive {
        PrimitiveType::Int => matches!(to, PrimitiveType::Long),
        PrimitiveType::Float => matches!(to, PrimitiveType::Double),
        PrimitiveType::Decimal {
            precision: from_precision,
            scale: from_scale,
        } => match to {
            PrimitiveType::Decimal {
                precision: to_precision,
                scale: to_scale,
            } => from_scale == to_scale && from_precision <= to_precision,
            _ => false,
        },
        _ => false,
    }
}

/// Validates that the existing type `from` may be promoted to the primitive type `to`, returning a
/// descriptive [`crate::Error`] of kind [`crate::ErrorKind::DataInvalid`] when it may not.
///
/// `Ok(())` means the change is a legal Iceberg type promotion (see [`is_promotion_allowed`] for the
/// exact rule set); the error message mirrors the Java reference's
/// `CheckCompatibility.primitive` text — `"{from} cannot be promoted to {to}"` — so failures read the
/// same on both implementations. This is the gate `UpdateSchema` calls before recording a column-type
/// change, equivalent to Java `SchemaUpdate.updateColumn`'s `Preconditions.checkArgument(...)`.
///
/// Note: like Java, this gate is **not** relaxed by an "allow incompatible changes" flag — forbidden
/// type changes (e.g. `long -> int`, `string -> int`) are rejected unconditionally.
pub fn ensure_promotion_allowed(from: &Type, to: &PrimitiveType) -> Result<()> {
    ensure_data_valid!(
        is_promotion_allowed(from, to),
        "{from} cannot be promoted to {to}",
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorKind;
    use crate::spec::datatypes::{ListType, MapType, NestedField, PrimitiveType, StructType, Type};

    /// Builds a one-field `Type::Struct` so the nested-type rejection tests have a realistic value.
    fn sample_struct() -> Type {
        Type::Struct(StructType::new(vec![
            NestedField::required(1, "field", Type::Primitive(PrimitiveType::Int)).into(),
        ]))
    }

    /// Builds a `Type::List` of optional ints for the nested-type rejection tests.
    fn sample_list() -> Type {
        Type::List(ListType::new(
            NestedField::list_element(2, Type::Primitive(PrimitiveType::Int), true).into(),
        ))
    }

    /// Builds a `Type::Map<int, int>` for the nested-type rejection tests.
    fn sample_map() -> Type {
        Type::Map(MapType::new(
            NestedField::map_key_element(3, Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::map_value_element(4, Type::Primitive(PrimitiveType::Int), true).into(),
        ))
    }

    /// Asserts the rejection carries the exact `ErrorKind::DataInvalid` and the Java-shaped message,
    /// so a forbidden promotion fails loudly with the offending pair named — not silently.
    fn assert_rejected(from: &Type, to: &PrimitiveType) {
        assert!(
            !is_promotion_allowed(from, to),
            "expected {from} -> {to} to be forbidden by is_promotion_allowed"
        );
        let error = ensure_promotion_allowed(from, to)
            .expect_err("expected a forbidden promotion to return Err");
        assert_eq!(
            error.kind(),
            ErrorKind::DataInvalid,
            "forbidden promotion must report DataInvalid"
        );
        assert_eq!(
            error.message(),
            format!("{from} cannot be promoted to {to}"),
            "error message must mirror Java's CheckCompatibility.primitive text"
        );
    }

    // ----- Allowed promotions: one assertion per Java switch edge -----

    /// Pins the `INTEGER` switch branch: `int -> long` must always be allowed.
    /// Risk: dropping this edge would reject the single most common widening and block valid evolution.
    #[test]
    fn promotes_int_to_long() {
        let from = Type::Primitive(PrimitiveType::Int);
        assert!(is_promotion_allowed(&from, &PrimitiveType::Long));
        assert!(ensure_promotion_allowed(&from, &PrimitiveType::Long).is_ok());
    }

    /// Pins the `FLOAT` switch branch: `float -> double` must always be allowed.
    /// Risk: a missing branch would reject the only legal floating-point widening.
    #[test]
    fn promotes_float_to_double() {
        let from = Type::Primitive(PrimitiveType::Float);
        assert!(is_promotion_allowed(&from, &PrimitiveType::Double));
        assert!(ensure_promotion_allowed(&from, &PrimitiveType::Double).is_ok());
    }

    /// Pins the `DECIMAL` widening branch: equal scale, strictly larger precision is allowed.
    /// Risk: an off-by-one in the `<=` precision check would reject a legal `decimal(10,2)->(11,2)`.
    #[test]
    fn promotes_decimal_widening_precision_with_equal_scale() {
        let from = Type::Primitive(PrimitiveType::Decimal {
            precision: 10,
            scale: 2,
        });
        let to = PrimitiveType::Decimal {
            precision: 11,
            scale: 2,
        };
        assert!(is_promotion_allowed(&from, &to));
        assert!(ensure_promotion_allowed(&from, &to).is_ok());
    }

    /// Pins the upper boundary of the precision check: `<=` admits equal precision with equal scale,
    /// which the identity short-circuit also accepts. Risk: a strict `<` would wrongly reject this.
    #[test]
    fn promotes_decimal_equal_precision_equal_scale_identity() {
        let from = Type::Primitive(PrimitiveType::Decimal {
            precision: 10,
            scale: 2,
        });
        let to = PrimitiveType::Decimal {
            precision: 10,
            scale: 2,
        };
        assert!(is_promotion_allowed(&from, &to));
        assert!(ensure_promotion_allowed(&from, &to).is_ok());
    }

    /// Pins the identity short-circuit for a non-numeric primitive (`from.equals(to)` in Java).
    /// Risk: forgetting the identity case would reject a no-op type "change" the schema layer issues.
    #[test]
    fn promotes_string_to_string_identity() {
        let from = Type::Primitive(PrimitiveType::String);
        assert!(is_promotion_allowed(&from, &PrimitiveType::String));
        assert!(ensure_promotion_allowed(&from, &PrimitiveType::String).is_ok());
    }

    /// Pins identity for `long`, the type that has NO outgoing promotion edge: `long -> long` is the
    /// only legal `long` target. Risk: conflating "no branch" with "reject everything" would also
    /// reject the legal identity no-op.
    #[test]
    fn promotes_long_to_long_identity() {
        let from = Type::Primitive(PrimitiveType::Long);
        assert!(is_promotion_allowed(&from, &PrimitiveType::Long));
        assert!(ensure_promotion_allowed(&from, &PrimitiveType::Long).is_ok());
    }

    // ----- Forbidden promotions: one rejected pair per forbidden case -----

    /// `long -> int` is forbidden (no narrowing). Risk: a symmetric "numbers widen" rule would allow
    /// this narrowing and silently truncate stored 64-bit values to 32 bits.
    #[test]
    fn rejects_long_to_int_narrowing() {
        assert_rejected(&Type::Primitive(PrimitiveType::Long), &PrimitiveType::Int);
    }

    /// `double -> float` is forbidden (no narrowing). Risk: allowing it would drop floating precision.
    #[test]
    fn rejects_double_to_float_narrowing() {
        assert_rejected(
            &Type::Primitive(PrimitiveType::Double),
            &PrimitiveType::Float,
        );
    }

    /// `int -> double` is forbidden — the INTEGER branch only targets LONG. Risk: a "wider numeric is
    /// fine" shortcut would admit a cross-family change Java rejects.
    #[test]
    fn rejects_int_to_double_cross_family() {
        assert_rejected(&Type::Primitive(PrimitiveType::Int), &PrimitiveType::Double);
    }

    /// `boolean -> int` is forbidden — BOOLEAN has no switch branch. Risk: treating bool as a 1-bit
    /// integer would let it widen into the numeric tower.
    #[test]
    fn rejects_boolean_to_int() {
        assert_rejected(
            &Type::Primitive(PrimitiveType::Boolean),
            &PrimitiveType::Int,
        );
    }

    /// `string -> int` is forbidden. Risk: a permissive "reparse on read" assumption would corrupt
    /// data; Java has no STRING branch.
    #[test]
    fn rejects_string_to_int() {
        assert_rejected(&Type::Primitive(PrimitiveType::String), &PrimitiveType::Int);
    }

    /// `date -> timestamp` is forbidden. Risk (the headline one): the Iceberg spec *discusses* this
    /// promotion, but Java's `isPromotionAllowed` has NO `DATE` branch, so implementing it would
    /// diverge from Java and fail interop. This test pins the Java-faithful rejection.
    #[test]
    fn rejects_date_to_timestamp() {
        assert_rejected(
            &Type::Primitive(PrimitiveType::Date),
            &PrimitiveType::Timestamp,
        );
    }

    /// `date -> timestamptz` is forbidden for the same reason as `date -> timestamp` (no DATE branch).
    /// Risk: a partial fix that special-cased only the no-tz target would still diverge from Java.
    #[test]
    fn rejects_date_to_timestamptz() {
        assert_rejected(
            &Type::Primitive(PrimitiveType::Date),
            &PrimitiveType::Timestamptz,
        );
    }

    /// `timestamp -> timestamptz` is forbidden (no timezone-attachment promotion). Risk: silently
    /// reinterpreting naive timestamps as UTC would shift every stored value.
    #[test]
    fn rejects_timestamp_to_timestamptz() {
        assert_rejected(
            &Type::Primitive(PrimitiveType::Timestamp),
            &PrimitiveType::Timestamptz,
        );
    }

    /// `timestamptz -> timestamp` is forbidden (no timezone-dropping promotion). Risk: the reverse of
    /// the above; allowing it would discard timezone semantics.
    #[test]
    fn rejects_timestamptz_to_timestamp() {
        assert_rejected(
            &Type::Primitive(PrimitiveType::Timestamptz),
            &PrimitiveType::Timestamp,
        );
    }

    /// `timestamp -> timestamp_ns` is forbidden (micros to nanos is not a Java promotion). Risk: a
    /// "precision only increases" intuition would admit a change Java's switch does not list.
    #[test]
    fn rejects_timestamp_to_timestamp_ns() {
        assert_rejected(
            &Type::Primitive(PrimitiveType::Timestamp),
            &PrimitiveType::TimestampNs,
        );
    }

    /// `fixed(16) -> binary` is forbidden. Risk: treating fixed as a length-bounded binary would admit
    /// a promotion Java's switch (no FIXED branch) rejects.
    #[test]
    fn rejects_fixed_to_binary() {
        assert_rejected(
            &Type::Primitive(PrimitiveType::Fixed(16)),
            &PrimitiveType::Binary,
        );
    }

    /// `uuid -> string` is forbidden. Risk: a "uuid is just text" assumption would admit a change Java
    /// rejects and break the 16-byte storage contract.
    #[test]
    fn rejects_uuid_to_string() {
        assert_rejected(
            &Type::Primitive(PrimitiveType::Uuid),
            &PrimitiveType::String,
        );
    }

    /// Decimal scale change is forbidden even when precision grows: `decimal(10,2) -> decimal(10,3)`.
    /// Risk (the decimal headline): implementers routinely allow scale changes when precision rises;
    /// Java requires `scale == scale` strictly, so any scale drift must be rejected.
    #[test]
    fn rejects_decimal_scale_change_even_with_more_precision() {
        assert_rejected(
            &Type::Primitive(PrimitiveType::Decimal {
                precision: 10,
                scale: 2,
            }),
            &PrimitiveType::Decimal {
                precision: 11,
                scale: 3,
            },
        );
    }

    /// Decimal precision reduction is forbidden: `decimal(11,2) -> decimal(10,2)`. Risk: allowing a
    /// narrowing precision could overflow already-stored values; the rule is `from.precision <= to`.
    #[test]
    fn rejects_decimal_precision_reduction() {
        assert_rejected(
            &Type::Primitive(PrimitiveType::Decimal {
                precision: 11,
                scale: 2,
            }),
            &PrimitiveType::Decimal {
                precision: 10,
                scale: 2,
            },
        );
    }

    /// A primitive promoted to a struct is forbidden — the `from` is primitive but no switch branch
    /// targets a nested type, and `to` is constrained to `PrimitiveType` anyway. This pins that a
    /// nested `from` value never sneaks through. Risk: a stray `true` for nested types would let a
    /// scalar column be "promoted" into a struct.
    #[test]
    fn rejects_struct_from_to_primitive() {
        // A nested `from` can never be promoted to any primitive `to`.
        assert_rejected(&sample_struct(), &PrimitiveType::Int);
        assert_rejected(&sample_list(), &PrimitiveType::Int);
        assert_rejected(&sample_map(), &PrimitiveType::Int);
    }

    /// `variant -> anything` is forbidden: Java 1.10.0 `TypeUtil.isPromotionAllowed` switches only
    /// on INTEGER/FLOAT/DECIMAL (default false), and `Type::Variant` is not even a primitive, so
    /// it takes the same up-front rejection as struct/list/map. The REVERSE direction
    /// (`anything -> variant`) is unrepresentable in both languages — the `to` parameter is
    /// `PrimitiveType` here exactly as in Java's signature, so no test can (or need) express it.
    /// Risk: a "variant absorbs any value" intuition would let a column be silently re-typed,
    /// changing how every existing data file is read.
    #[test]
    fn rejects_variant_promotion_to_any_primitive() {
        assert_rejected(&Type::Variant, &PrimitiveType::Int);
        assert_rejected(&Type::Variant, &PrimitiveType::Long);
        assert_rejected(&Type::Variant, &PrimitiveType::String);
        assert_rejected(&Type::Variant, &PrimitiveType::Binary);
    }
}
