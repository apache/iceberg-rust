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

//! The Iceberg V3 variant binary format — READ side (decoding) and WRITE side
//! (construction + serialization).
//!
//! A Rust port of Java's `org.apache.iceberg.variants` package, pinned to **iceberg-api /
//! iceberg-core 1.10.0** (bytecode-verified; the MAIN-branch sources matched 1.10.0 for the
//! entire read AND write surface at port time). A variant is a (metadata, value) pair: the
//! metadata carries a dictionary of field-name strings, the value is a self-describing binary
//! encoding of a primitive, a short string, an object (fields keyed by dictionary ids), or an
//! array.
//!
//! # Java → Rust mapping
//!
//! | Java (`org.apache.iceberg.variants`) | Rust |
//! |---|---|
//! | `BasicType` | [`BasicType`] |
//! | `PhysicalType` (+ package-private `LogicalType`) | [`PhysicalType`], [`LogicalType`] |
//! | `VariantMetadata` / `SerializedMetadata` | [`VariantMetadata`] |
//! | `VariantValue.from` + the `Serialized*` classes | [`VariantValue::parse`] |
//! | `VariantPrimitive<T>` / `SerializedPrimitive` / `SerializedShortString` | [`VariantPrimitive`] |
//! | `VariantObject` / `SerializedObject` | [`VariantObject`] |
//! | `VariantArray` / `SerializedArray` | [`VariantArray`] |
//! | `Variant` / `VariantData` | [`Variant`] |
//! | `Variants.metadata(Collection)` (core) | [`VariantMetadata::from_field_names`] (`write.rs`) |
//! | `VariantMetadata.writeTo` | [`VariantMetadata::to_bytes`] (`write.rs`) |
//! | `Variants.of(...)` factories (core) | the `VariantValue::of_*` constructors (`write.rs`) |
//! | `PrimitiveWrapper` / `ValueArray` / `ShreddedObject` `sizeInBytes()`+`writeTo` (core) | [`VariantValue::size_in_bytes`] / [`VariantValue::write_to`] / [`VariantValue::to_bytes`] (`write.rs`) |
//! | `Variants.array()` → `ValueArray.add` (core) | [`VariantArray::new`] + [`VariantArray::push`] |
//! | `Variants.object(metadata)` + `ShreddedObject.put` (core, plain-writer core) | [`VariantObjectBuilder`] (`write.rs`, builds a nestable [`VariantObject`] value) or [`ShreddedObject::new`] (`shredded.rs`, the direct serializer with `remove` semantics) |
//! | `Variants.object(metadata, object)` — the partial-shred overlay (core) | [`ShreddedObject::over_serialized_object`] (the `SerializedObject` branch: untouched fields serialize as VERBATIM slices via the `sliceValue` spans) / [`ShreddedObject::over_object`] (the non-serialized branch, canonicalizing) — `shredded.rs` |
//! | `ShreddedObject.put`/`remove`/`get`/`fieldNames`/`numFields`/`sizeInBytes`/`writeTo` (core) | the same-named [`ShreddedObject`] methods (`shredded.rs`) |
//! | `Variants.object(object)` (metadata derived from the object) | not ported — the parsed [`VariantObject`] carries no metadata reference; pass the metadata explicitly |
//! | `VariantVisitor<R>` + `visit(Variant/VariantValue, visitor)` (core) | [`VariantVisitor`] + [`visit_variant`] / [`visit_value`] (`visitor.rs`) |
//! | `Variant` emission (metadata bytes ++ value bytes) | [`Variant::to_bytes`] (`write.rs`) |
//!
//! Semantic parity is the bar, not class-hierarchy mimicry: Java's `VariantValue` interface
//! hierarchy becomes the [`VariantValue`] enum; a short string decodes to
//! [`VariantPrimitive::String`] exactly as Java's `SerializedShortString` reports
//! `PhysicalType.STRING`.
//!
//! # Security posture — this parser is a boundary
//!
//! Value and metadata bytes come from UNTRUSTED storage. Every read is bounds-checked and every
//! malformed input returns [`Error`](crate::Error) (kind
//! [`DataInvalid`](crate::ErrorKind::DataInvalid) for structural corruption,
//! [`FeatureUnsupported`](crate::ErrorKind::FeatureUnsupported) for an unsupported metadata
//! version or unknown primitive type id, mirroring Java's `IllegalArgumentException` vs
//! `UnsupportedOperationException` split) — never a panic. Offsets are validated against slice bounds before slicing; all
//! offset arithmetic is checked. Nested values recurse, which the manuals allow only with a
//! provable bound: depth is **explicitly guarded** by [`MAX_NESTING_DEPTH`] (a Rust-side DoS
//! guard; lazily-parsing Java has no such limit because its parse depth equals the caller's
//! traversal depth).
//!
//! # Deliberate divergences from Java 1.10.0
//!
//! - **Eager parse.** Java parses lazily (headers at construction, payloads/names on access);
//!   this port parses the entire tree up front, chosen so that malformed bytes never cross the
//!   boundary. Outside the degenerate family below, an input that errors here also errors in
//!   Java under a full traversal — the difference is only *when* (at the [`VariantValue::parse`]
//!   / [`VariantMetadata::parse`] door instead of on first access). Java additionally tolerates
//!   a malformed nested value a *partial* traversal never accesses; the eager parse rejects it
//!   immediately.
//! - **Declared sizes Java never reads are still validated here.** Java's lazy reader skips the
//!   mandatory final offset entry of a ZERO-count container, and keeps (un-truncated) a metadata
//!   buffer whose declared string-data end overruns it — so a full 1.10.0 traversal ACCEPTS the
//!   truncated empty object `[0x02, 0x00]`, the truncated empty array `[0x03, 0x00]`, and an
//!   empty-dictionary metadata declaring data past the buffer end (e.g. `[0x01, 0x00, 0x05]`,
//!   where Java reports `sizeInBytes()` clamped to the buffer). All three are spec-violating
//!   shapes no Java writer emits (the spec requires `count + 1` offset entries and an in-bounds
//!   data region); this port rejects them at the door
//!   ([`DataInvalid`](crate::ErrorKind::DataInvalid)). Probe-verified against 1.10.0
//!   (2026-06-11; pinned in `tests.rs`).
//! - **Invalid UTF-8 is an error.** Java's `new String(bytes, UTF_8)` silently substitutes
//!   U+FFFD for malformed sequences; this port fails loud
//!   ([`DataInvalid`](crate::ErrorKind::DataInvalid)). Java's writer only emits valid UTF-8,
//!   so no Java-written variant is affected.
//! - **Misses return `Option`/`Result` instead of unchecked throws.** Java's
//!   `VariantObject.get(name)` returns `null` on a miss ([`VariantObject::get`] returns
//!   `None` — exact); `SerializedArray.get` / `SerializedMetadata.get` throw unchecked
//!   `ArrayIndexOutOfBoundsException` on out-of-range ([`VariantArray::get`] returns `None`,
//!   [`VariantMetadata::get`] returns `Err`).
//! - **Value `sizeInBytes()`/`writeTo` live on the write side** ([`VariantValue::size_in_bytes`]
//!   / [`VariantValue::write_to`] in `write.rs`, which re-encode from the eager representation
//!   with the canonical Java-WRITER layout; Java's `SerializedValue` instead reports/copies its
//!   backing buffer — see `write.rs`'s module doc for the canonicalization divergence).
//!   [`VariantMetadata::size_in_bytes`] reports the PARSED size (it delimits the metadata
//!   region inside a concatenated variant buffer, which [`Variant::from_bytes`] needs).
//! - **Floats compare with Rust semantics.** The derived `PartialEq` treats `NaN != NaN`;
//!   Java's `Objects.equals` on boxed doubles treats `NaN == NaN`.
//!
//! The WRITE-side divergence list (canonicalizing re-serialization, the width-overflow error
//! door where Java silently masks, the Java-`int` domain doors, the depth-guarded write
//! recursion) lives in `write.rs`'s module doc. The canonicalization divergence is BOUNDED by
//! the shredding overlay (`shredded.rs`, Wave-4 F2): an overlay over a serialized backing
//! copies untouched fields' original bytes VERBATIM (Java `SerializedObject.sliceValue`), so
//! non-canonical third-party encodings survive a partial-shred rewrite exactly as in Java —
//! only values that go through the canonical writer (constructed values, `put` overrides,
//! whole-value re-serialization) re-encode. The overlay's own divergence list (the two
//! probe-verified 1.10.0 bugs this port does not mirror — the stale `remove()` cache and the
//! corrupt first serialization over a constructed backing — plus the eager backing parse)
//! lives in `shredded.rs`'s module doc; the visitor's (Result-typed drivers, the depth
//! guard, the name-lookup-miss error where Java NPEs) in `visitor.rs`'s.
//!
//! Sorted-dictionary and object-field binary searches reproduce Java's `String.compareTo`
//! (UTF-16 code unit) ordering — see `util::java_string_compare` — because a comparator
//! divergence silently flips lookup hits to misses for names beyond the basic multilingual
//! plane.
//!
//! The schema-type integration landed 2026-06-11 (Wave-4 F1): `variant` is a first-class schema
//! type ([`Type::Variant`](crate::spec::Type) in `spec/datatypes.rs`), with JSON schema serde
//! (the bare string `"variant"`, `SchemaParser` parity), the `MIN_FORMAT_VERSIONS` V3 gate
//! (`spec/schema/mod.rs::min_format_version` → `check_compatibility`), the
//! partition/sort/identifier/promotion legality doors, the Java-shaped Avro record conversion
//! (`avro/schema.rs`), and the loud-error Arrow conversion (`arrow/schema.rs`).
//!
//! The shredding-overlay WRITE side (`ShreddedObject`'s partial-shred surface, `shredded.rs`)
//! and the `VariantVisitor` traversal utility (`visitor.rs`) landed 2026-06-11 (Wave-4 F2).
//! Out of scope for this module (deferred, tracked in `docs/parity/GAP_MATRIX.md`):
//! shredded-parquet FILE I/O and file-level Java↔Rust interop (blocked at the pinned
//! parquet 57.1 / arrow-rs 57.1 crates, which expose no variant type; the Arrow conversion
//! errors loudly) beyond the 1.10.0-generated byte pins in `tests.rs`.

mod metadata;
mod shredded;
mod types;
mod util;
mod value;
mod visitor;
mod write;

pub use metadata::VariantMetadata;
pub use shredded::ShreddedObject;
pub use types::{BasicType, LogicalType, PhysicalType};
pub use value::{
    MAX_NESTING_DEPTH, Variant, VariantArray, VariantObject, VariantObjectField, VariantPrimitive,
    VariantValue,
};
pub use visitor::{VariantVisitor, visit_value, visit_variant};
pub use write::VariantObjectBuilder;

#[cfg(test)]
mod tests;
