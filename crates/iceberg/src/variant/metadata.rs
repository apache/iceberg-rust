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

//! Variant metadata: the field-name dictionary — the port of Java 1.10.0 `SerializedMetadata`
//! (+ the `VariantMetadata` interface), parsed EAGERLY (see the module doc for the divergence
//! note).

use crate::variant::util;
use crate::{Error, ErrorKind, Result};

/// Bit mask of the version in the metadata header byte (Java `SerializedMetadata.VERSION_MASK`).
const VERSION_MASK: u8 = 0b1111;
/// The only supported metadata version (Java `SerializedMetadata.SUPPORTED_VERSION`); shared
/// with the write side (`VariantUtil.metadataHeader`'s `| 0b0001`).
pub(super) const SUPPORTED_VERSION: u8 = 1;
/// Header bit signalling the dictionary strings are sorted (Java `SORTED_STRINGS`); shared
/// with the write side (`VariantUtil.metadataHeader`).
pub(super) const SORTED_STRINGS: u8 = 0b10000;
/// Bit mask of the offset-size field in the header (Java `OFFSET_SIZE_MASK`).
const OFFSET_SIZE_MASK: u8 = 0b1100_0000;
/// Right shift of the offset-size field (Java `OFFSET_SIZE_SHIFT`); shared with the write side.
pub(super) const OFFSET_SIZE_SHIFT: u8 = 6;
/// Size in bytes of the metadata header (Java `SerializedMetadata.HEADER_SIZE`); shared with
/// the write side.
pub(super) const HEADER_SIZE: usize = 1;

/// The parsed variant metadata: a dictionary of field-name strings addressed by id, plus the
/// sorted flag that governs name→id lookup. The Rust analogue of Java's `VariantMetadata` /
/// `SerializedMetadata`, decoded eagerly from untrusted bytes by [`VariantMetadata::parse`].
///
/// On-disk layout (`SerializedMetadata`, 1.10.0): a header byte (low 4 bits: version, must be
/// 1; bit 4: sorted-strings; bits 6..8: offset size minus one), a dictionary size in
/// `offset_size` little-endian bytes, `dictionary_size + 1` offsets of `offset_size` bytes
/// each (the last is the total string-data length), then the concatenated UTF-8 strings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VariantMetadata {
    is_sorted: bool,
    dictionary: Vec<String>,
    size_in_bytes: usize,
}

impl VariantMetadata {
    /// Parses variant metadata from untrusted bytes (Java `SerializedMetadata.from` + the
    /// lazy reads its constructor defers; eager here — every offset, range, and string is
    /// validated at the door).
    ///
    /// Trailing bytes after the encoded metadata are ignored exactly as Java ignores them
    /// (Java truncates its buffer to the computed end offset): [`Self::size_in_bytes`] reports
    /// where the metadata ends, which is how [`super::Variant::from_bytes`] locates the value.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::FeatureUnsupported`] for a version other than 1 (Java:
    /// "Unsupported version: %s"); [`ErrorKind::DataInvalid`] for any structural corruption —
    /// truncation, ranges past the end of the buffer, non-monotonic string offsets, or
    /// invalid UTF-8 (a documented divergence: Java substitutes U+FFFD).
    pub fn parse(bytes: &[u8]) -> Result<VariantMetadata> {
        let header = util::read_u8(bytes, 0)
            .map_err(|error| error.with_context("context", "variant metadata header"))?;
        let version = header & VERSION_MASK;
        if version != SUPPORTED_VERSION {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!("Unsupported version: {version}"),
            ));
        }
        let is_sorted = (header & SORTED_STRINGS) == SORTED_STRINGS;
        let offset_size = 1 + usize::from((header & OFFSET_SIZE_MASK) >> OFFSET_SIZE_SHIFT);

        let dictionary_size = util::read_le_unsigned(bytes, HEADER_SIZE, offset_size)? as usize;
        let offset_list_offset = HEADER_SIZE + offset_size;

        // dataOffset = offsetListOffset + (1 + dictSize) * offsetSize — checked so a hostile
        // dictionary size cannot overflow the math (Java's int math would go negative and fail
        // the subsequent buffer reads instead).
        let offsets_len = dictionary_size
            .checked_add(1)
            .and_then(|count| count.checked_mul(offset_size))
            .ok_or_else(|| util::invalid("Invalid variant metadata: dictionary size overflows"))?;
        let data_offset = offset_list_offset
            .checked_add(offsets_len)
            .ok_or_else(|| util::invalid("Invalid variant metadata: offset list overflows"))?;

        // The final offset entry is the total string-data length; the metadata ends there.
        let data_length = util::read_le_unsigned(
            bytes,
            offset_list_offset + offset_size * dictionary_size,
            offset_size,
        )? as usize;
        let end_offset = data_offset
            .checked_add(data_length)
            .ok_or_else(|| util::invalid("Invalid variant metadata: data length overflows"))?;
        if end_offset > bytes.len() {
            return Err(util::invalid(format!(
                "Invalid variant metadata: declared end offset {end_offset} exceeds the \
                 {} bytes provided",
                bytes.len()
            )));
        }

        // Decode every dictionary string eagerly. Each string i spans
        // dataOffset + offset[i] .. dataOffset + offset[i + 1] (Java `get(int)`); ranges are
        // validated against the declared end offset — the region Java truncates its buffer to —
        // so an intermediate offset cannot escape the metadata even inside a longer buffer.
        let mut dictionary = Vec::with_capacity(dictionary_size);
        let mut offset = util::read_le_unsigned(bytes, offset_list_offset, offset_size)? as usize;
        for index in 0..dictionary_size {
            let next = util::read_le_unsigned(
                bytes,
                offset_list_offset + offset_size * (index + 1),
                offset_size,
            )? as usize;
            let length = next.checked_sub(offset).ok_or_else(|| {
                util::invalid(format!(
                    "Invalid variant metadata: string offsets not monotonically increasing \
                     ({next} follows {offset} at dictionary index {index})"
                ))
            })?;
            let start = data_offset + offset;
            if start + length > end_offset {
                return Err(util::invalid(format!(
                    "Invalid variant metadata: dictionary string {index} range \
                     {start}..{} exceeds the declared end offset {end_offset}",
                    start + length
                )));
            }
            let raw = util::slice(bytes, start, length)?;
            let name = std::str::from_utf8(raw).map_err(|source| {
                util::invalid(format!(
                    "Invalid variant metadata: dictionary string {index} is not valid UTF-8"
                ))
                .with_source(source)
            })?;
            dictionary.push(name.to_string());
            offset = next;
        }

        Ok(VariantMetadata {
            is_sorted,
            dictionary,
            size_in_bytes: end_offset,
        })
    }

    /// Assembles an already-validated metadata (the write side's constructor seam —
    /// [`VariantMetadata::from_field_names`] in `write.rs` builds the parts per Java
    /// `Variants.metadata` and constructs through here; parse keeps its own path above).
    pub(super) fn from_parts(
        is_sorted: bool,
        dictionary: Vec<String>,
        size_in_bytes: usize,
    ) -> VariantMetadata {
        VariantMetadata {
            is_sorted,
            dictionary,
            size_in_bytes,
        }
    }

    /// Returns the dictionary strings in id order (the write side iterates them; external
    /// callers use [`Self::get`] / [`Self::id`]).
    pub(super) fn dictionary(&self) -> &[String] {
        &self.dictionary
    }

    /// Returns the number of strings in the dictionary (Java `dictionarySize()`).
    pub fn dictionary_size(&self) -> usize {
        self.dictionary.len()
    }

    /// Returns whether the dictionary declares its strings sorted (Java `isSorted()`), which
    /// switches [`Self::id`] between binary search and a linear scan.
    pub fn is_sorted(&self) -> bool {
        self.is_sorted
    }

    /// Returns the serialized size in bytes of the metadata (Java `sizeInBytes()` after its
    /// constructor truncates the buffer to the computed end offset) — the value region of a
    /// concatenated variant buffer starts here.
    pub fn size_in_bytes(&self) -> usize {
        self.size_in_bytes
    }

    /// Returns the dictionary id for `name`, or `None` if not found — Java `id(String)`
    /// (which returns -1 on a miss). Exactly like Java, a sorted dictionary is binary-searched
    /// in `String.compareTo` order (so a dictionary that LIES about being sorted can miss,
    /// identically to Java) and an unsorted one is scanned linearly.
    pub fn id(&self, name: &str) -> Option<usize> {
        if self.is_sorted {
            util::find(self.dictionary.len(), name, |index| {
                self.dictionary[index].as_str()
            })
        } else {
            self.dictionary.iter().position(|entry| entry == name)
        }
    }

    /// Returns the string for dictionary id `id` (Java `get(int)`).
    ///
    /// # Errors
    ///
    /// [`ErrorKind::DataInvalid`] when `id` is out of range (Java throws an unchecked
    /// `ArrayIndexOutOfBoundsException`).
    pub fn get(&self, id: usize) -> Result<&str> {
        self.dictionary.get(id).map(String::as_str).ok_or_else(|| {
            util::invalid(format!(
                "Invalid dictionary id {id}: the dictionary has {} entries",
                self.dictionary.len()
            ))
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Encodes metadata bytes for the given dictionary with a chosen offset size and sorted
    /// flag — the test-side inverse of `parse`, following the `SerializedMetadata` layout.
    pub(crate) fn encode_metadata(names: &[&str], offset_size: usize, sorted: bool) -> Vec<u8> {
        let header = ((offset_size - 1) << 6) as u8 | if sorted { SORTED_STRINGS } else { 0 } | 0x1;
        let mut bytes = vec![header];
        let write_unsigned = |bytes: &mut Vec<u8>, value: usize| {
            bytes.extend_from_slice(&value.to_le_bytes()[..offset_size]);
        };
        write_unsigned(&mut bytes, names.len());
        let mut offset = 0usize;
        for name in names {
            write_unsigned(&mut bytes, offset);
            offset += name.len();
        }
        write_unsigned(&mut bytes, offset);
        for name in names {
            bytes.extend_from_slice(name.as_bytes());
        }
        bytes
    }

    /// Risk pinned: a future or corrupt version byte parsed as v1 would wrongly decode every later
    /// field — versions other than 1 must be rejected with Java's contract (1.10.0
    /// `SUPPORTED_VERSION == 1`, "Unsupported version: %s").
    #[test]
    fn test_metadata_rejects_unsupported_versions() {
        for version in [0u8, 2, 15] {
            let bytes = [version, 0x00, 0x00];
            let error = VariantMetadata::parse(&bytes).expect_err("bad version must be rejected");
            assert_eq!(error.kind(), crate::ErrorKind::FeatureUnsupported);
            assert!(
                error.to_string().contains("Unsupported version"),
                "error must name the version, got: {error}"
            );
        }
    }

    /// Risk pinned: the empty dictionary (Java `SerializedMetadata.EMPTY_V1_BUFFER` =
    /// `01 00 00`) is the metadata of every field-less variant — it must parse, report size 0,
    /// and miss every lookup.
    #[test]
    fn test_metadata_empty_dictionary_parses() {
        let metadata =
            VariantMetadata::parse(&[0x01, 0x00, 0x00]).expect("the empty dictionary must parse");
        assert_eq!(metadata.dictionary_size(), 0);
        assert_eq!(metadata.size_in_bytes(), 3);
        assert!(!metadata.is_sorted());
        assert_eq!(metadata.id("a"), None);
        assert!(metadata.get(0).is_err());
    }

    /// Risk pinned: the offset-size header field ((header & 0b11000000) >> 6, plus one) drives
    /// ALL integer widths in the encoding — each of the four sizes must decode the same
    /// dictionary.
    #[test]
    fn test_metadata_offset_sizes_1_through_4_decode_identically() {
        for offset_size in 1..=4usize {
            let bytes = encode_metadata(&["a", "bc", "def"], offset_size, false);
            let metadata = VariantMetadata::parse(&bytes)
                .unwrap_or_else(|error| panic!("offset size {offset_size} must parse: {error}"));
            assert_eq!(metadata.dictionary_size(), 3);
            assert_eq!(metadata.get(0).expect("id 0"), "a");
            assert_eq!(metadata.get(1).expect("id 1"), "bc");
            assert_eq!(metadata.get(2).expect("id 2"), "def");
            assert_eq!(metadata.size_in_bytes(), bytes.len());
            assert_eq!(metadata.id("def"), Some(2));
        }
    }

    /// Risk pinned: `get(id)` past the dictionary must be a clean error (Java throws
    /// `ArrayIndexOutOfBoundsException`), never a panic.
    #[test]
    fn test_metadata_get_out_of_range_errors() {
        let bytes = encode_metadata(&["a"], 1, false);
        let metadata = VariantMetadata::parse(&bytes).expect("must parse");
        let error = metadata.get(1).expect_err("id 1 of 1-entry dictionary");
        assert_eq!(error.kind(), crate::ErrorKind::DataInvalid);
        assert!(
            error.to_string().contains("Invalid dictionary id 1"),
            "error must name the id, got: {error}"
        );
    }

    /// Risk pinned: the sorted flag flips `id()` to binary search. A dictionary that LIES
    /// about being sorted must MISS exactly like Java's binary search does (a linear fallback
    /// would find entries Java cannot — a lookup divergence, not a robustness win).
    #[test]
    fn test_metadata_sorted_flag_lookup_and_lying_sorted_dictionary_misses_like_java() {
        // Genuinely sorted + flagged sorted: binary search finds every entry.
        let sorted = VariantMetadata::parse(&encode_metadata(&["a", "b", "c"], 1, true))
            .expect("sorted dictionary must parse");
        assert!(sorted.is_sorted());
        assert_eq!(sorted.id("a"), Some(0));
        assert_eq!(sorted.id("c"), Some(2));
        assert_eq!(sorted.id("missing"), None);

        // Same strings unsorted + UNFLAGGED: the linear scan finds them all.
        let unsorted = VariantMetadata::parse(&encode_metadata(&["c", "a", "b"], 1, false))
            .expect("unsorted dictionary must parse");
        assert_eq!(unsorted.id("c"), Some(0));
        assert_eq!(unsorted.id("b"), Some(2));

        // Unsorted but FLAGGED sorted: Java's probe sequence over ["c", "a", "b"] for key "c"
        // probes index 1 ("a", go right) then 2 ("b", go right) and misses, even though "c"
        // is at index 0. Mirror the miss.
        let lying = VariantMetadata::parse(&encode_metadata(&["c", "a", "b"], 1, true))
            .expect("must parse");
        assert_eq!(lying.id("c"), None, "Java's binary search misses here");
        assert_eq!(lying.id("a"), Some(1), "found where the probe lands on it");
    }

    /// Risk pinned: Java compares with `String.compareTo` (UTF-16 units) — a dictionary sorted
    /// in JAVA order containing a supplementary character is binary-searched correctly only
    /// with that comparator; byte-order comparison would miss.
    #[test]
    fn test_metadata_sorted_lookup_uses_java_utf16_order() {
        // In Java order "\u{10000}" < "\u{FFFF}" (surrogate 0xD800 < 0xFFFF); in byte order
        // the opposite. The dictionary is laid out in JAVA order, as Java's writer would.
        let bytes = encode_metadata(&["\u{10000}", "\u{FFFF}"], 1, true);
        let metadata = VariantMetadata::parse(&bytes).expect("must parse");
        assert_eq!(
            metadata.id("\u{FFFF}"),
            Some(1),
            "a byte-order comparator would walk left and miss this entry"
        );
        assert_eq!(metadata.id("\u{10000}"), Some(0));
    }

    /// Risk pinned: trailing bytes after the encoded metadata are LEGAL (Java truncates its
    /// buffer to the computed end offset) — `size_in_bytes` must report the true end so the
    /// value region of a concatenated buffer can be located.
    #[test]
    fn test_metadata_ignores_trailing_bytes_and_reports_true_end() {
        let mut bytes = encode_metadata(&["a", "b"], 1, false);
        let true_end = bytes.len();
        bytes.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
        let metadata = VariantMetadata::parse(&bytes).expect("trailing bytes are legal");
        assert_eq!(metadata.size_in_bytes(), true_end);
        assert_eq!(metadata.get(1).expect("id 1"), "b");
    }

    /// Risk pinned (malformed-input suite): every truncation point must produce a clean Err —
    /// empty input, a header alone, a cut inside the offset list, and a cut inside the string
    /// data. Running these also proves the no-panic contract.
    #[test]
    fn test_metadata_truncations_reject_cleanly() {
        assert!(VariantMetadata::parse(&[]).is_err(), "empty input");
        assert!(VariantMetadata::parse(&[0x01]).is_err(), "header only");
        let full = encode_metadata(&["a", "bc"], 2, false);
        for cut in 1..full.len() {
            assert!(
                VariantMetadata::parse(&full[..cut]).is_err(),
                "metadata cut to {cut} of {} bytes must be rejected",
                full.len()
            );
        }
    }

    /// Risk pinned (malformed-input suite): a dictionary size far beyond the actual buffer
    /// (the absurd-declared-size attack, including the i32::MAX flavor that would overflow or
    /// allocate wildly if trusted) must fail fast with an error, not panic or OOM.
    #[test]
    fn test_metadata_absurd_dictionary_size_rejects() {
        // offset_size 4, dictionary size i32::MAX, then nothing behind it.
        let mut bytes = vec![0b1100_0001u8];
        bytes.extend_from_slice(&(i32::MAX as u32).to_le_bytes());
        assert!(VariantMetadata::parse(&bytes).is_err());

        // Size that fails Java's signed-int domain outright.
        let mut bytes = vec![0b1100_0001u8];
        bytes.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(VariantMetadata::parse(&bytes).is_err());
    }

    /// Risk pinned (malformed-input suite): non-monotonic string offsets (Java computes
    /// `next - offset` as a length and would fail the slice) must reject with the offsets
    /// named, never wrap around.
    #[test]
    fn test_metadata_non_monotonic_offsets_reject() {
        // Hand-build: 2 strings, offsets [2, 0], data length 2 — offset[1] < offset[0].
        let bytes = [0x01u8, 0x02, 0x02, 0x00, 0x02, b'h', b'i'];
        let error = VariantMetadata::parse(&bytes).expect_err("descending offsets");
        assert!(
            error.to_string().contains("not monotonically increasing"),
            "error must name the offset order, got: {error}"
        );
    }

    /// Risk pinned (malformed-input suite): a declared string-data length running past the end
    /// of the buffer must reject BEFORE any string is sliced (Java's lazy reads fail on access;
    /// the eager parse rejects at the door). An intermediate offset cannot escape on its own:
    /// the monotonic chain ends at the final offset (= the data length), so any escape attempt
    /// is caught either as non-monotonic or by this end-offset bound.
    #[test]
    fn test_metadata_declared_data_length_past_buffer_end_rejects() {
        // 1 string, offsets [0, 9]: declared data length 9, but only 4 data bytes follow.
        let bytes = [0x01u8, 0x01, 0x00, 0x09, b'd', b'a', b't', b'a'];
        let error = VariantMetadata::parse(&bytes).expect_err("data length past buffer end");
        assert!(
            error.to_string().contains("end offset"),
            "error must name the bound, got: {error}"
        );

        // A first offset jumping past the (smaller) final offset is the non-monotonic case.
        let bytes = [0x01u8, 0x01, 0x07, 0x02, b'h', b'i'];
        assert!(VariantMetadata::parse(&bytes).is_err());
    }

    /// Risk pinned (malformed-input suite + documented divergence): invalid UTF-8 in the
    /// dictionary is rejected loudly (Java silently substitutes U+FFFD — a name-mangling
    /// behavior we refuse to mirror; Java writers never emit invalid UTF-8).
    #[test]
    fn test_metadata_invalid_utf8_in_dictionary_rejects() {
        // 1 string of 2 bytes: 0xC3 0x28 is an invalid UTF-8 sequence.
        let bytes = [0x01u8, 0x01, 0x00, 0x02, 0xC3, 0x28];
        let error = VariantMetadata::parse(&bytes).expect_err("invalid UTF-8");
        assert!(
            error.to_string().contains("not valid UTF-8"),
            "error must name the UTF-8 failure, got: {error}"
        );
    }
}
