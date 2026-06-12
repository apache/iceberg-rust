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

//! The variant traversal utility — the port of Java 1.10.0 `VariantVisitor<R>` (core), the
//! entry point shredded parquet writers build on.
//!
//! Bytecode-verified against `iceberg-core-1.10.0.jar` (identical to the MAIN source):
//!
//! - the per-shape callbacks `object(VariantObject, List<String>, List<R>)`,
//!   `array(VariantArray, List<R>)`, `primitive(VariantPrimitive<?>)`, all defaulting to
//!   `null` ([`None`] here);
//! - the pre/post hooks `beforeArrayElement(int)` / `afterArrayElement(int)` /
//!   `beforeObjectField(String)` / `afterObjectField(String)`, all defaulting to no-ops;
//! - the two static drivers `visit(Variant, VariantVisitor)` (delegates to the value) and
//!   `visit(VariantValue, VariantVisitor)` — [`visit_variant`] / [`visit_value`]. (1.10.0
//!   has NO metadata-taking driver overload.)
//!
//! Traversal order (the bytecode's exact sequence, post-order results):
//!
//! - **Array:** for each index in `0..numElements`: `beforeArrayElement(index)`, recurse
//!   into `array.get(index)`, `afterArrayElement(index)` — the after-hook runs in a Java
//!   `finally`, so it fires on the error path too; then `array(array, element_results)`.
//! - **Object:** for each name yielded by `object.fieldNames()` (STORED order): record the
//!   name, `beforeObjectField(name)`, recurse into `object.get(name)` — the NAME LOOKUP,
//!   not the positional field — `afterObjectField(name)` (`finally` again); then
//!   `object(object, field_names, field_results)`.
//! - **Anything else:** `primitive(value.asPrimitive())`.
//!
//! # Differences from Java 1.10.0
//!
//! - **The drivers return [`Result`]** for two Rust-side fail-loud doors with no Java
//!   counterpart: recursion depth is bounded by
//!   [`MAX_NESTING_DEPTH`](crate::variant::MAX_NESTING_DEPTH) (Java recurses to the data's
//!   full depth — a manually constructed bomb would overflow the stack; the bound and the
//!   depth accounting equal the parser's, so every PARSEABLE value is visitable), and an
//!   object field whose `get(name)` lookup misses (possible only for a non-name-sorted
//!   object, where Java's `visit(object.get(name), ...)` throws a `NullPointerException`)
//!   is a named [`DataInvalid`](crate::ErrorKind::DataInvalid) error. Per Java's `finally`,
//!   the after-hook still runs before either error propagates.
//! - **`null` results are [`Option`]s.** Java's `R` is nullable and every default returns
//!   `null`; here the callbacks return `Option<Self::Output>` and collect
//!   `Vec<Option<Self::Output>>`.
//! - `visit` is split into [`visit_variant`] / [`visit_value`] (Rust has no overloading),
//!   and the visitor is `&mut` (Java visitors are stateful objects).

use crate::Result;
use crate::variant::util;
use crate::variant::value::{
    MAX_NESTING_DEPTH, Variant, VariantArray, VariantObject, VariantPrimitive, VariantValue,
};

/// A visitor over a decoded variant value tree — the Rust `VariantVisitor<R>`. All methods
/// have defaults matching Java's (callbacks return `None` = Java `null`; hooks no-op), so an
/// implementor overrides only what it needs.
pub trait VariantVisitor {
    /// The per-value result type (Java's `R`; `None` plays Java's `null`).
    type Output;

    /// Called after all of an object's fields were visited, with the field names and the
    /// per-field results in visit order (Java `object(VariantObject, List<String>,
    /// List<R>)`; defaults to `null`).
    fn object(
        &mut self,
        object: &VariantObject,
        field_names: &[&str],
        field_results: Vec<Option<Self::Output>>,
    ) -> Option<Self::Output> {
        let _ = (object, field_names, field_results);
        None
    }

    /// Called after all of an array's elements were visited, with the per-element results
    /// in index order (Java `array(VariantArray, List<R>)`; defaults to `null`).
    fn array(
        &mut self,
        array: &VariantArray,
        element_results: Vec<Option<Self::Output>>,
    ) -> Option<Self::Output> {
        let _ = (array, element_results);
        None
    }

    /// Called for a primitive (including short strings, which Java also reports as
    /// primitives; Java `primitive(VariantPrimitive<?>)`, defaults to `null`).
    fn primitive(&mut self, primitive: &VariantPrimitive) -> Option<Self::Output> {
        let _ = primitive;
        None
    }

    /// Called before visiting array element `index` (Java `beforeArrayElement(int)`;
    /// defaults to a no-op).
    fn before_array_element(&mut self, index: usize) {
        let _ = index;
    }

    /// Called after visiting array element `index`, INCLUDING on the error path — Java runs
    /// it in a `finally` (defaults to a no-op).
    fn after_array_element(&mut self, index: usize) {
        let _ = index;
    }

    /// Called before visiting the object field named `field_name` (Java
    /// `beforeObjectField(String)`; defaults to a no-op).
    fn before_object_field(&mut self, field_name: &str) {
        let _ = field_name;
    }

    /// Called after visiting the object field named `field_name`, INCLUDING on the error
    /// path — Java runs it in a `finally` (defaults to a no-op).
    fn after_object_field(&mut self, field_name: &str) {
        let _ = field_name;
    }
}

/// Visits a whole variant by visiting its value — Java
/// `VariantVisitor.visit(Variant, VariantVisitor)`.
///
/// # Errors
///
/// See [`visit_value`].
pub fn visit_variant<V: VariantVisitor>(
    variant: &Variant,
    visitor: &mut V,
) -> Result<Option<V::Output>> {
    visit_value(variant.value(), visitor)
}

/// Visits a decoded value tree in Java's exact traversal order — Java
/// `VariantVisitor.visit(VariantValue, VariantVisitor)` (the module doc spells out the
/// order and hook contract).
///
/// # Errors
///
/// [`crate::ErrorKind::DataInvalid`] when nesting exceeds
/// [`MAX_NESTING_DEPTH`](crate::variant::MAX_NESTING_DEPTH) (a Rust-side guard — Java
/// recurses unbounded) or when an object field cannot be re-found by name lookup (where
/// Java throws a `NullPointerException`); never panics.
pub fn visit_value<V: VariantVisitor>(
    value: &VariantValue,
    visitor: &mut V,
) -> Result<Option<V::Output>> {
    visit_value_at_depth(value, visitor, 0)
}

/// The recursive driver. Recursion is justified per the manuals: the structure is genuinely
/// recursive and depth is explicitly bounded by [`MAX_NESTING_DEPTH`] (checked before any
/// child visit, with the same accounting as the parser's `parse_value` — root at depth 0,
/// children at `depth + 1` — so the visitable set equals the parseable set).
fn visit_value_at_depth<V: VariantVisitor>(
    value: &VariantValue,
    visitor: &mut V,
    depth: usize,
) -> Result<Option<V::Output>> {
    if depth > MAX_NESTING_DEPTH {
        return Err(util::invalid(format!(
            "Invalid variant: nesting depth exceeds the supported maximum {MAX_NESTING_DEPTH}"
        )));
    }
    match value {
        VariantValue::Array(array) => {
            let mut element_results = Vec::with_capacity(array.num_elements());
            for (index, element) in array.elements().iter().enumerate() {
                visitor.before_array_element(index);
                let result = visit_value_at_depth(element, visitor, depth + 1);
                // Java `finally`: the after-hook runs before an error propagates.
                visitor.after_array_element(index);
                element_results.push(result?);
            }
            Ok(visitor.array(array, element_results))
        }
        VariantValue::Object(object) => {
            let mut field_names: Vec<&str> = Vec::with_capacity(object.num_fields());
            let mut field_results = Vec::with_capacity(object.num_fields());
            for name in object.field_names() {
                field_names.push(name);
                visitor.before_object_field(name);
                // Java recurses into object.get(fieldName) — the NAME LOOKUP. For a
                // non-name-sorted object the lookup can miss a present field; Java then
                // throws a NullPointerException, mirrored here as a named error.
                let result = match object.get(name) {
                    Some(child) => visit_value_at_depth(child, visitor, depth + 1),
                    None => Err(util::invalid(format!(
                        "Invalid variant object: field {name} cannot be re-found by name \
                         lookup (Java fails with a NullPointerException here)"
                    ))),
                };
                // Java `finally`: the after-hook runs before an error propagates.
                visitor.after_object_field(name);
                field_results.push(result?);
            }
            Ok(visitor.object(object, &field_names, field_results))
        }
        VariantValue::Primitive(primitive) => Ok(visitor.primitive(primitive)),
    }
}
