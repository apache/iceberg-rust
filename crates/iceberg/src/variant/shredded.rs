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

//! The variant shredding overlay — the port of Java 1.10.0 `ShreddedObject`'s PARTIAL-SHRED
//! surface (`Variants.object(metadata, object)` + `put`/`remove`/`removedFields` over an
//! unshredded backing), completing the plain-writer core B2 ported as
//! [`VariantObjectBuilder`](crate::variant::VariantObjectBuilder).
//!
//! All semantics are bytecode-verified against `iceberg-core-1.10.0.jar`
//! (`ShreddedObject` + `ShreddedObject$SerializationState`) and
//! `iceberg-api-1.10.0.jar` (`SerializedObject.fields()` / `sliceValue(int)`):
//!
//! - **The verbatim-slice contract (the load-bearing part).** When the unshredded backing was
//!   parsed from serialized bytes (Java's `SerializedObject` branch), every field that is
//!   neither overridden (`put`) nor removed serializes as a VERBATIM byte slice of the
//!   original buffer — Java `SerializedObject.sliceValue(index)` returns
//!   `value[dataOffset + offsets[i] .. + lengths[i]]` and `writeTo` copies it with
//!   `VariantUtil.writeBufferAbsolute`, never re-encoding. A NON-CANONICAL field encoding
//!   (oversized widths, a long-form string that would fit the short form) is therefore
//!   PRESERVED, where the canonicalizing writer in `write.rs` would re-encode it. This is
//!   exactly the place the B2 canonicalization divergence becomes load-bearing — see
//!   "Interaction with the write.rs canonicalization divergence" below.
//! - **Field merge.** The serialized field set is the union of the surviving unshredded
//!   fields and the shredded (`put`) fields, written in Java `String.compareTo` (UTF-16 code
//!   unit) name order — Java's `SortedMerge.of(unshredded.keySet().stream().sorted(),
//!   shredded.keySet().stream().sorted())` over DISJOINT key sets (a replaced backing field
//!   never enters the unshredded map), which one UTF-16 sort over the union reproduces
//!   exactly. Shredded values WIN on collision; removed names are excluded from the
//!   unshredded side.
//! - **Width and header math over the MERGED set** (`SerializationState`): `fieldIdSize =
//!   VariantUtil.sizeOf(metadata.dictionarySize())` (the dictionary SIZE, not the largest id
//!   used), `dataSize` = verbatim slice lengths + shredded `sizeInBytes()`, `offsetSize =
//!   sizeOf(dataSize)`, `isLarge = numElements > 0xFF`,
//!   `size = 1 + (4|1) + n*fieldIdSize + (1+n)*offsetSize + dataSize`.
//! - **Field ids are re-resolved BY NAME at write time** (`metadata.id(field)` +
//!   `checkState(id >= 0, "Invalid metadata, missing: %s")`) for verbatim and shredded
//!   fields alike — only the field VALUE bytes are verbatim; ids, offsets, and the header are
//!   always recomputed.
//! - **`put` precondition:** the name must be in the metadata dictionary
//!   (`"Cannot find field name in metadata: %s"`). `put` does NOT clear a previous `remove`
//!   of the same name (Java keeps the name in `removedFields`): after `remove(x)` + `put(x,
//!   v)` the field IS serialized with the new value while [`ShreddedObject::get`] returns
//!   `None` and [`ShreddedObject::field_names`] excludes it — Java's exact (inconsistent)
//!   contract, probe-verified and pinned in `tests.rs`. MAIN behaves identically (its `put`
//!   does not clear `removedFields` either), so this is not a 1.10.0-only quirk.
//! - **`remove` of an absent name is a no-op** for serialization (the name simply joins
//!   `removedFields`, which only filters backing fields).
//! - **Duplicate names in a serialized backing** are rejected at serialization time unless
//!   replaced/removed — Java's `ImmutableMap.Builder.build()` throws
//!   `IllegalArgumentException` ("Multiple entries with same key") when the surviving
//!   unshredded fields collide; replacing or removing the duplicated name removes BOTH
//!   occurrences from the unshredded side and serializes fine in both languages.
//!
//! # Differences from Java 1.10.0
//!
//! - **No cached serialization state.** [`ShreddedObject::size_in_bytes`] and
//!   [`ShreddedObject::write_to`] compute the layout fresh on every call. 1.10.0 caches a
//!   `SerializationState` that `put` resets but `remove` does NOT (the reset was added on
//!   MAIN after 1.10.0) — so a 1.10.0 `sizeInBytes()` → `remove(x)` → `writeTo` sequence
//!   serializes the STALE state with `x` still present (probe-verified 2026-06-11,
//!   `/tmp/variant-probe/ShredProbe.java` p2). This port always reflects the current
//!   overlay, matching 1.10.0's fresh-state output (and MAIN's intent); the stale-cache bug
//!   is deliberately not mirrored.
//! - **The constructed (non-serialized) backing branch follows MAIN's semantics.** 1.10.0's
//!   compiled `SerializationState` constructor merges a non-`SerializedObject` backing's
//!   fields into the caller's LIVE map while keeping a pre-merge copy for `writeTo`
//!   (a parameter-shadowing bug fixed on MAIN), so the FIRST serialization of such an
//!   overlay emits corrupt bytes — count and data size include the merged fields but the
//!   field list does not (probe-verified 2026-06-11, p1: re-read fails with
//!   `IndexOutOfBoundsException`; a later state rebuild self-heals because the live map was
//!   polluted). [`ShreddedObject::over_object`] materializes the backing's fields into the
//!   effective shredded set consistently for BOTH size and write — byte-identical to Java's
//!   self-healed (second) serialization, which `tests.rs` pins as the oracle.
//! - **Eager backing parse.** [`ShreddedObject::over_serialized_object`] parses (and
//!   validates) the backing bytes at the door, so a backing with a malformed UNTOUCHED field
//!   is rejected here while lazy Java slices it verbatim and `writeTo` SUCCEEDS blind,
//!   copying the bad bytes into the output (probe-verified 2026-06-11, ReviewerShredProbe
//!   r4 — only a later Java `get` on the field throws). A malformed REPLACED/removed field
//!   diverges the same way: Java skips it before slicing and serializes fine; the Rust door
//!   rejects regardless, because it parses the whole backing before any `put`/`remove`
//!   exists. Same eager-parse divergence family as the read side (`mod.rs`) — accepted-set
//!   only, loud-vs-blind, never silent corruption.
//! - **A constructed-backing field that cannot be re-found by name lookup is a loud error**
//!   (Java materializes with `unshredded.get(name)`, which returns `null` for a
//!   non-name-sorted object's unfindable field and later fails with a
//!   `NullPointerException`).
//!
//! # Interaction with the write.rs canonicalization divergence
//!
//! `write.rs` documents that re-serializing a PARSED [`VariantValue`] canonicalizes
//! (the eager representation has no backing buffer). The overlay is the path where Java's
//! verbatim-copy behavior is load-bearing — an overlay over a third-party object with
//! non-canonical field encodings must preserve those fields' original bytes — so
//! [`ShreddedObject::over_serialized_object`] keeps the original buffer and per-field value
//! ranges (the `sliceValue` analogue) and copies untouched fields verbatim. Net: untouched
//! fields are byte-preserved exactly like Java; only fields the caller `put` (and, for a
//! constructed backing, materialized fields) go through the canonical writer, which is also
//! what Java does (`VariantValue.writeTo` on the shredded value).

use std::collections::{HashMap, HashSet};
use std::ops::Range;

use crate::Result;
use crate::variant::metadata::VariantMetadata;
use crate::variant::util;
use crate::variant::value::{
    HEADER_SIZE, VariantObject, VariantValue, parse_object_with_value_ranges,
};
use crate::variant::write::{
    JAVA_INT_MAX, checked_data_size, door_value_span, object_header, size_of_unsigned, value_size,
    write_bytes, write_le_unsigned, write_u8, write_value,
};

/// The unshredded backing of a [`ShreddedObject`] — Java's `unshredded` field, split by
/// provenance because the Rust parsed representation carries no backing buffer.
#[derive(Debug, Clone)]
enum UnshreddedBacking {
    /// Parsed from serialized bytes — Java's `SerializedObject` branch: untouched fields
    /// serialize as VERBATIM slices of `bytes` (`SerializedObject.sliceValue(index)`).
    Serialized {
        /// The parsed view (name lookups, like Java's lazy `SerializedObject` reads).
        object: VariantObject,
        /// The original VALUE bytes (Java's backing `ByteBuffer`).
        bytes: Vec<u8>,
        /// Each stored-order field's value byte range within `bytes`, parallel to
        /// `object.fields()` — the `sliceValue(index)` spans.
        value_ranges: Vec<Range<usize>>,
    },
    /// A constructed object with no backing buffer — Java's non-`SerializedObject` branch:
    /// untouched fields are materialized by name lookup at serialization time and re-encoded
    /// by the canonical writer (MAIN-consistent semantics; see the module doc for 1.10.0's
    /// parameter-shadowing bug on this branch).
    Constructed(VariantObject),
}

impl UnshreddedBacking {
    /// The parsed object view backing `get`/`field_names` (Java reads through the
    /// `VariantObject` interface for both branches).
    fn object(&self) -> &VariantObject {
        match self {
            UnshreddedBacking::Serialized { object, .. } => object,
            UnshreddedBacking::Constructed(object) => object,
        }
    }
}

/// Where a merged field's bytes come from at write time.
enum FieldSource<'plan> {
    /// Serialize through the canonical writer (`write.rs`): a `put` value, or a materialized
    /// constructed-backing field (Java `shreddedValue.writeTo(...)`).
    Value(&'plan VariantValue),
    /// Copy this range of the serialized backing verbatim (Java
    /// `VariantUtil.writeBufferAbsolute(buffer, ..., unshreddedFields.get(field))`).
    Verbatim(Range<usize>),
}

/// The computed serialization plan — Java `ShreddedObject$SerializationState`, recomputed
/// fresh per call instead of cached (module doc).
struct SerializationPlan<'plan> {
    /// Merged fields in Java's `SortedMerge` order: UTF-16 name order over the disjoint
    /// union of surviving unshredded fields and shredded fields.
    fields: Vec<(&'plan str, FieldSource<'plan>)>,
    data_size: usize,
    is_large: bool,
    field_id_size: usize,
    offset_size: usize,
    total_size: usize,
}

/// A variant object that handles full or partial shredding — the Rust `ShreddedObject`.
///
/// Constructed bare ([`ShreddedObject::new`] — Java `Variants.object(metadata)`), over
/// serialized object bytes ([`ShreddedObject::over_serialized_object`] — Java
/// `Variants.object(metadata, serializedObject)`, the verbatim-slice path), or over an
/// in-memory [`VariantObject`] ([`ShreddedObject::over_object`] — Java's
/// non-`SerializedObject` branch, canonicalizing). Mutate with [`ShreddedObject::put`] /
/// [`ShreddedObject::remove`], then serialize with [`ShreddedObject::to_bytes`] (or
/// [`ShreddedObject::size_in_bytes`] + [`ShreddedObject::write_to`]).
///
/// Like Java, the metadata must already contain every field name that will be `put` — the
/// overlay never rewrites the dictionary ("Metadata stored for an object must be the same
/// regardless of whether the object is shredded").
#[derive(Debug, Clone)]
pub struct ShreddedObject<'a> {
    metadata: &'a VariantMetadata,
    unshredded: Option<UnshreddedBacking>,
    shredded_fields: HashMap<String, VariantValue>,
    removed_fields: HashSet<String>,
}

impl<'a> ShreddedObject<'a> {
    /// Starts an overlay with no unshredded backing (Java `Variants.object(metadata)`; the
    /// `metadata != null` precondition is unrepresentable here).
    pub fn new(metadata: &'a VariantMetadata) -> ShreddedObject<'a> {
        ShreddedObject {
            metadata,
            unshredded: None,
            shredded_fields: HashMap::new(),
            removed_fields: HashSet::new(),
        }
    }

    /// Starts an overlay over an object parsed from `value_bytes` — the
    /// `Variants.object(metadata, object)` re-wrap for a SERIALIZED backing. Untouched
    /// fields later serialize as verbatim slices of `value_bytes` (the module doc's
    /// `sliceValue` contract). The bytes are parsed (and fully validated) at this door.
    ///
    /// # Errors
    ///
    /// Any [`VariantValue::parse`] error for malformed bytes, and Java's `asObject()`
    /// contract ("Not an object: %s") when the bytes encode a non-object value (the typed
    /// Java signature makes that case unrepresentable there).
    pub fn over_serialized_object(
        metadata: &'a VariantMetadata,
        value_bytes: &[u8],
    ) -> Result<ShreddedObject<'a>> {
        let (object, value_ranges) = parse_object_with_value_ranges(metadata, value_bytes)?;
        Ok(ShreddedObject {
            metadata,
            unshredded: Some(UnshreddedBacking::Serialized {
                object,
                bytes: value_bytes.to_vec(),
                value_ranges,
            }),
            shredded_fields: HashMap::new(),
            removed_fields: HashSet::new(),
        })
    }

    /// Starts an overlay over an in-memory object with no backing buffer — Java's
    /// non-`SerializedObject` constructor branch. Untouched fields are materialized by name
    /// lookup at serialization time and re-encoded canonically (no verbatim reuse is
    /// possible without original bytes); see the module doc for the 1.10.0 bug on this
    /// branch and why this port follows MAIN's semantics.
    pub fn over_object(metadata: &'a VariantMetadata, object: VariantObject) -> ShreddedObject<'a> {
        ShreddedObject {
            metadata,
            unshredded: Some(UnshreddedBacking::Constructed(object)),
            shredded_fields: HashMap::new(),
            removed_fields: HashSet::new(),
        }
    }

    /// Sets a field, overriding the backing's value or any previous `put` for the same name
    /// (Java `ShreddedObject.put`). Exactly like Java, this does NOT undo a previous
    /// [`ShreddedObject::remove`] of the same name for the read-side views — see the module
    /// doc's remove-then-put note.
    ///
    /// # Errors
    ///
    /// [`crate::ErrorKind::DataInvalid`] when the name is not in the metadata dictionary
    /// (Java `Preconditions.checkArgument(metadata.id(field) >= 0, "Cannot find field name
    /// in metadata: %s")`).
    pub fn put(&mut self, name: impl Into<String>, value: VariantValue) -> Result<()> {
        let name = name.into();
        if self.metadata.id(&name).is_none() {
            return Err(util::invalid(format!(
                "Cannot find field name in metadata: {name}"
            )));
        }
        self.shredded_fields.insert(name, value);
        Ok(())
    }

    /// Removes a field: drops any `put` for the name and excludes the backing's field from
    /// the merge (Java `ShreddedObject.remove`). Removing a name that is present nowhere is
    /// a harmless no-op, exactly as in Java (no precondition).
    pub fn remove(&mut self, name: &str) {
        self.shredded_fields.remove(name);
        self.removed_fields.insert(name.to_string());
    }

    /// Returns the field's current value (Java `ShreddedObject.get`): a removed name returns
    /// `None` FIRST (even when a later `put` re-added it — Java's contract), then the
    /// shredded value wins over the backing, then the backing's (binary-search) lookup.
    pub fn get(&self, name: &str) -> Option<&VariantValue> {
        if self.removed_fields.contains(name) {
            return None;
        }
        if let Some(value) = self.shredded_fields.get(name) {
            return Some(value);
        }
        self.unshredded
            .as_ref()
            .and_then(|backing| backing.object().get(name))
    }

    /// Returns the merged field names — shredded plus backing, minus removed — sorted and
    /// de-duplicated in Java `String.compareTo` (UTF-16) order (Java `nameSet()`, a
    /// `TreeSet`).
    pub fn field_names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.shredded_fields.keys().map(String::as_str).collect();
        if let Some(backing) = &self.unshredded {
            names.extend(backing.object().field_names());
        }
        names.sort_by(|left, right| util::java_string_compare(left, right));
        names.dedup();
        names.retain(|name| !self.removed_fields.contains(*name));
        names
    }

    /// Returns the merged field count (Java `numFields()` — `nameSet().size()`). NOTE: after
    /// a remove-then-put of the same name this view EXCLUDES the name even though
    /// serialization includes it, mirroring Java exactly (module doc).
    pub fn num_fields(&self) -> usize {
        self.field_names().len()
    }

    /// Returns the serialized size in bytes (Java `sizeInBytes()`), computed fresh (no
    /// cached state — module doc). Like Java, this does NOT resolve field ids, so a
    /// dictionary whose `id()` lookup cannot find a written name fails only at
    /// [`ShreddedObject::write_to`].
    ///
    /// # Errors
    ///
    /// [`crate::ErrorKind::DataInvalid`] per [`Self::write_to`]'s layout doors (Java `int`
    /// domain, nesting depth, duplicate/unresolvable backing fields).
    pub fn size_in_bytes(&self) -> Result<usize> {
        Ok(self.serialization_plan()?.total_size)
    }

    /// Writes the overlay into `buffer` at `offset` and returns the number of bytes written
    /// — Java `ShreddedObject.writeTo(ByteBuffer, int)` (the little-endian byte-order
    /// precondition is implicit here). Untouched serialized-backing fields are copied
    /// verbatim; everything else goes through the canonical writer.
    ///
    /// # Errors
    ///
    /// [`crate::ErrorKind::DataInvalid`] when the buffer is too small (checked up front —
    /// the buffer is untouched on that failure), when a written name is missing from the
    /// metadata dictionary (Java `checkState`: "Invalid metadata, missing: %s" — like Java
    /// this can fire mid-write, after some bytes were written), or per the layout doors.
    pub fn write_to(&self, buffer: &mut [u8], offset: usize) -> Result<usize> {
        let plan = self.serialization_plan()?;
        // Door the whole span up front (checked math): afterwards every interior offset is
        // bounded by buffer.len() <= isize::MAX, so plain offset arithmetic cannot overflow.
        door_value_span(buffer, offset, plan.total_size, "shredded object")?;
        let num_elements = plan.fields.len();
        let count_size = if plan.is_large { 4 } else { 1 };
        let field_id_list_offset = offset + HEADER_SIZE + count_size;
        let offset_list_offset = field_id_list_offset + num_elements * plan.field_id_size;
        let data_offset = offset_list_offset + (1 + num_elements) * plan.offset_size;

        write_u8(
            buffer,
            offset,
            object_header(plan.is_large, plan.field_id_size, plan.offset_size),
        )?;
        write_le_unsigned(buffer, num_elements, offset + HEADER_SIZE, count_size)?;

        let mut next_value_offset = 0usize;
        for (index, (name, source)) in plan.fields.iter().enumerate() {
            // Java: int id = metadata.id(field); checkState(id >= 0, "Invalid metadata,
            // missing: %s", field); — ids are re-resolved BY NAME for verbatim fields too.
            let id = self
                .metadata
                .id(name)
                .ok_or_else(|| util::invalid(format!("Invalid metadata, missing: {name}")))?;
            write_le_unsigned(
                buffer,
                id,
                field_id_list_offset + index * plan.field_id_size,
                plan.field_id_size,
            )?;
            write_le_unsigned(
                buffer,
                next_value_offset,
                offset_list_offset + index * plan.offset_size,
                plan.offset_size,
            )?;
            let value_size = match source {
                FieldSource::Value(value) => write_value(
                    value,
                    self.metadata,
                    buffer,
                    data_offset + next_value_offset,
                    1,
                )?,
                FieldSource::Verbatim(range) => {
                    let backing_bytes = match &self.unshredded {
                        Some(UnshreddedBacking::Serialized { bytes, .. }) => bytes,
                        // Verbatim sources are only ever planned from a Serialized backing.
                        _ => {
                            return Err(util::invalid(
                                "Invalid variant write: verbatim field without serialized backing",
                            ));
                        }
                    };
                    let slice = backing_bytes.get(range.clone()).ok_or_else(|| {
                        util::invalid(format!(
                            "Invalid variant write: verbatim range {range:?} escapes the \
                             {}-byte backing",
                            backing_bytes.len()
                        ))
                    })?;
                    write_bytes(buffer, data_offset + next_value_offset, slice)?;
                    slice.len()
                }
            };
            next_value_offset += value_size;
        }
        // The final offset entry is the total size of the data section.
        write_le_unsigned(
            buffer,
            next_value_offset,
            offset_list_offset + num_elements * plan.offset_size,
            plan.offset_size,
        )?;
        debug_assert_eq!(next_value_offset, plan.data_size);
        Ok((data_offset - offset) + plan.data_size)
    }

    /// Serializes the overlay to its on-disk bytes — the `ByteBuffer.allocate(sizeInBytes())`
    /// + `writeTo(buffer, 0)` pattern every Java caller uses.
    ///
    /// # Errors
    ///
    /// Any [`Self::size_in_bytes`] / [`Self::write_to`] error.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let size = self.size_in_bytes()?;
        let mut buffer = vec![0u8; size];
        let written = self.write_to(&mut buffer, 0)?;
        debug_assert_eq!(written, size, "write_to must fill exactly size_in_bytes");
        Ok(buffer)
    }

    /// Computes the serialization plan — the `SerializationState` constructor's math
    /// (1.10.0 bytecode): collect the surviving unshredded fields (verbatim ranges for a
    /// serialized backing; materialized values for a constructed one), merge with the
    /// shredded fields, and derive the widths over the MERGED set.
    fn serialization_plan(&self) -> Result<SerializationPlan<'_>> {
        let field_id_size = size_of_unsigned(self.metadata.dictionary_size());
        let mut data_size = 0usize;
        let mut fields: Vec<(&str, FieldSource<'_>)> = Vec::new();

        // The effective shredded set: the caller's puts, plus — for a constructed backing —
        // the materialized untouched fields (Java merges those into its map copy; HashMap
        // semantics collapse duplicate backing names silently there).
        let mut effective_shredded: HashMap<&str, &VariantValue> = self
            .shredded_fields
            .iter()
            .map(|(name, value)| (name.as_str(), value))
            .collect();

        match &self.unshredded {
            Some(UnshreddedBacking::Serialized {
                object,
                value_ranges,
                ..
            }) => {
                // Java iterates `serialized.fields()` (stored order) and skips replaced
                // names; duplicates among the SURVIVORS throw (ImmutableMap), while a
                // replaced/removed duplicate is skipped before it can collide.
                let mut seen_names: HashSet<&str> = HashSet::new();
                for (field, range) in object.fields().iter().zip(value_ranges) {
                    let name = field.name.as_str();
                    let replaced = self.shredded_fields.contains_key(name)
                        || self.removed_fields.contains(name);
                    if !replaced {
                        if !seen_names.insert(name) {
                            return Err(util::invalid(format!(
                                "Invalid variant object: duplicate unshredded field name: \
                                 {name} (Java 1.10.0 rejects multiple entries with the same \
                                 key)"
                            )));
                        }
                        data_size = checked_data_size(data_size, range.len())?;
                        fields.push((name, FieldSource::Verbatim(range.clone())));
                    }
                }
            }
            Some(UnshreddedBacking::Constructed(object)) => {
                // Java: shreddedFields.put(name, unshredded.get(name)) for unreplaced names
                // (MAIN semantics — module doc). The get(name) LOOKUP is deliberate: a field
                // a name lookup cannot find fails loud where Java would NPE.
                for field in object.fields() {
                    let name = field.name.as_str();
                    let replaced = self.shredded_fields.contains_key(name)
                        || self.removed_fields.contains(name);
                    if !replaced {
                        let value = object.get(name).ok_or_else(|| {
                            util::invalid(format!(
                                "Invalid variant object: unshredded field {name} cannot be \
                                 resolved by name lookup (Java fails with a \
                                 NullPointerException here)"
                            ))
                        })?;
                        effective_shredded.insert(name, value);
                    }
                }
            }
            None => {}
        }

        for (name, value) in &effective_shredded {
            data_size = checked_data_size(data_size, value_size(value, self.metadata, 1)?)?;
            fields.push((*name, FieldSource::Value(value)));
        }

        // Java's SortedMerge over two name-sorted streams with DISJOINT keys == one UTF-16
        // sort over the union.
        fields.sort_by(|left, right| util::java_string_compare(left.0, right.0));

        let num_elements = fields.len();
        let is_large = num_elements > 0xFF;
        let offset_size = size_of_unsigned(data_size);
        let count_size = if is_large { 4 } else { 1 };
        let total_size = HEADER_SIZE
            .checked_add(count_size)
            .and_then(|size| size.checked_add(num_elements.checked_mul(field_id_size)?))
            .and_then(|size| {
                size.checked_add(num_elements.checked_add(1)?.checked_mul(offset_size)?)
            })
            .and_then(|size| size.checked_add(data_size))
            .filter(|size| *size <= JAVA_INT_MAX)
            .ok_or_else(|| {
                util::invalid(format!(
                    "Invalid variant object: serialized size exceeds {JAVA_INT_MAX} bytes"
                ))
            })?;
        Ok(SerializationPlan {
            fields,
            data_size,
            is_large,
            field_id_size,
            offset_size,
            total_size,
        })
    }
}
