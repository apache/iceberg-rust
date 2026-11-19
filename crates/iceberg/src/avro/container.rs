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

//! Reading Avro object container files, reusing parsed writer schemas across
//! files whose headers carry the same `avro.schema` bytes.
//!
//! `apache_avro::Reader` parses the writer schema embedded in every file's
//! header: JSON decoding, `Schema::parse` and name resolution. The manifests of
//! one table version all carry the same schema, and that parse dominated the
//! cost of reading a manifest. apache-avro 0.22 has no way to hand `Reader` a
//! parsed writer schema, so [`ObjectContainerFile`] reads the container format
//! itself and decodes records with `GenericDatumReader`, which uses the same
//! schema-aware deserializer as `Reader::into_deser_iter`. Once `Reader` accepts
//! a pre-parsed writer schema, this module can go.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use apache_avro::error::Details;
use apache_avro::reader::datum::GenericDatumReader;
use apache_avro::schema::{MapSchema, ResolvedSchema};
use apache_avro::types::Value as AvroValue;
use apache_avro::{AvroResult, Codec, Schema as AvroSchema};
use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;

use super::named_types::define_named_types_once;
use crate::error::{Error, Result, invalid_data};
use crate::util::parsed_cache::ParsedCache;

pub(super) const MAGIC: &[u8] = b"Obj\x01";
pub(super) const SCHEMA_KEY: &str = "avro.schema";
const CODEC_KEY: &str = "avro.codec";
const RESERVED_KEY_PREFIX: &str = "avro.";
const SYNC_MARKER_LEN: usize = 16;

/// A parsed writer schema together with what parsing it had to work around.
#[derive(Debug)]
struct WriterSchema {
    schema: AvroSchema,
    /// Full names of named types the schema defined more than once.
    repeated_definitions: Vec<String>,
}

/// Parsed writer schemas keyed by the exact `avro.schema` bytes they came from.
static WRITER_SCHEMAS: ParsedCache<WriterSchema> = ParsedCache::new();

/// An Avro object container file whose header has been read.
#[derive(Debug)]
pub(crate) struct ObjectContainerFile<'a> {
    writer_schema: Arc<WriterSchema>,
    codec: Codec,
    user_metadata: HashMap<String, Vec<u8>>,
    sync_marker: [u8; SYNC_MARKER_LEN],
    blocks: &'a [u8],
}

impl<'a> ObjectContainerFile<'a> {
    /// Reads the header of the file in `bs`, parsing its writer schema unless an
    /// earlier file carried the same schema bytes.
    pub(crate) fn parse(bs: &'a [u8]) -> Result<Self> {
        let body = bs
            .strip_prefix(MAGIC)
            .ok_or_else(|| invalid_data!("Not an Avro object container file"))?;
        let (metadata, rest) = read_metadata(body)?;
        let (sync_marker, blocks) = rest
            .split_first_chunk::<SYNC_MARKER_LEN>()
            .ok_or_else(|| invalid_data!("Avro file header ends before its sync marker"))?;

        let mut writer_schema = None;
        let mut codec = Codec::Null;
        let mut user_metadata = HashMap::new();
        for (key, value) in metadata {
            let AvroValue::Bytes(value) = value else {
                return Err(invalid_data!("Avro file metadata {key} is not bytes"));
            };
            match key.as_str() {
                SCHEMA_KEY => writer_schema = Some(cached_writer_schema(&value)?),
                CODEC_KEY => codec = parse_codec(&value)?,
                // Other reserved keys, such as the compression level, don't
                // affect reading.
                key if key.starts_with(RESERVED_KEY_PREFIX) => {}
                _ => {
                    user_metadata.insert(key, value);
                }
            }
        }
        let writer_schema =
            writer_schema.ok_or_else(|| invalid_data!("Avro file metadata has no {SCHEMA_KEY}"))?;

        Ok(Self {
            writer_schema,
            codec,
            user_metadata,
            sync_marker: *sync_marker,
            blocks,
        })
    }

    /// The header's metadata without the reserved `avro.` keys.
    pub(crate) fn user_metadata(&self) -> &HashMap<String, Vec<u8>> {
        &self.user_metadata
    }

    /// Full names of named types the writer schema defined more than once. The
    /// file was read with each repeated definition replaced by a reference to
    /// the first.
    pub(crate) fn repeated_named_type_definitions(&self) -> &[String] {
        &self.writer_schema.repeated_definitions
    }

    /// Deserializes every record in the file.
    pub(crate) fn records<T: DeserializeOwned>(&self) -> Result<Vec<T>> {
        let reader = GenericDatumReader::builder(&self.writer_schema.schema).build()?;
        let long = AvroSchema::Long;
        let long_reader = GenericDatumReader::builder(&long).build()?;
        let mut records = Vec::new();
        let mut rest = self.blocks;
        while !rest.is_empty() {
            let count = read_len(&long_reader, &mut rest, "record count")?;
            let size = read_len(&long_reader, &mut rest, "block size")?;
            let (data, after) = rest
                .split_at_checked(size)
                .ok_or_else(|| invalid_data!("Avro block extends past the end of the file"))?;
            let (sync_marker, after) = after
                .split_first_chunk::<SYNC_MARKER_LEN>()
                .ok_or_else(|| invalid_data!("Avro block ends before its sync marker"))?;
            if *sync_marker != self.sync_marker {
                return Err(invalid_data!(
                    "Avro block sync marker doesn't match the file header"
                ));
            }
            rest = after;

            let mut block = data.to_vec();
            self.codec.decompress(&mut block)?;
            let mut block = block.as_slice();
            // `count` comes from the file. Every record takes at least one
            // byte, so a count beyond the block's length is corrupt and must
            // not size an allocation; the decode loop below reports it.
            records.reserve(count.min(block.len()));
            for _ in 0..count {
                let before = block.len();
                records.push(reader.read_deser::<T>(&mut block)?);
                if before != 0 && block.len() == before {
                    return Err(invalid_data!(
                        "Avro record consumed no bytes; block record count is corrupt"
                    ));
                }
            }
        }
        Ok(records)
    }
}

/// The schema of an object container file's header metadata: a `map<bytes>`.
pub(super) fn metadata_schema() -> AvroSchema {
    AvroSchema::Map(MapSchema {
        types: Box::new(AvroSchema::Bytes),
        attributes: Default::default(),
    })
}

/// Decodes the header metadata at the start of `body`, the bytes after the
/// magic. Returns it with the bytes that follow it.
pub(super) fn read_metadata(body: &[u8]) -> Result<(HashMap<String, AvroValue>, &[u8])> {
    let metadata_schema = metadata_schema();
    let mut rest = body;
    let AvroValue::Map(metadata) = GenericDatumReader::builder(&metadata_schema)
        .build()?
        .read_value(&mut rest)?
    else {
        return Err(invalid_data!("Avro file metadata is not a map"));
    };
    Ok((metadata, rest))
}

/// The codec named by the header's `avro.codec` value.
fn parse_codec(bs: &[u8]) -> Result<Codec> {
    let name = std::str::from_utf8(bs)
        .map_err(|_| invalid_data!("Avro codec name is not UTF-8: {bs:?}"))?;
    Codec::from_str(name).map_err(|_| invalid_data!("Avro codec {name} is not supported"))
}

/// Reads the `long` that opens a block (its record count or size) as a `usize`.
fn read_len(reader: &GenericDatumReader<'_>, rest: &mut &[u8], what: &str) -> Result<usize> {
    let len = reader
        .read_deser::<i64>(rest)
        .map_err(|e| Error::from(e).with_context("reading", format!("Avro block {what}")))?;
    usize::try_from(len).map_err(|_| invalid_data!("Avro block {what} {len} is negative"))
}

/// The parsed writer schema for `json`, from the cache when an earlier file
/// carried the same bytes.
fn cached_writer_schema(json: &[u8]) -> Result<Arc<WriterSchema>> {
    WRITER_SCHEMAS.get_or_parse(json, || parse_writer_schema(json))
}

/// Parses `json` and validates its named types, falling back to defining each
/// named type once for files that define one repeatedly.
fn parse_writer_schema(json: &[u8]) -> Result<WriterSchema> {
    let mut schema: JsonValue = serde_json::from_slice(json)?;
    let error = match parse_and_resolve(&schema) {
        Ok(schema) => {
            return Ok(WriterSchema {
                schema,
                repeated_definitions: Vec::new(),
            });
        }
        // iceberg-rust repeated `decimal` definitions before
        // `schema_to_avro_schema` defined each named type once, so this
        // fallback stays while tables can contain manifests it wrote.
        Err(e) if matches!(e.details(), Details::AmbiguousSchemaDefinition(_)) => e,
        Err(e) => return Err(e.into()),
    };
    let Some(repeated_definitions) = define_named_types_once(&mut schema) else {
        return Err(error.into());
    };
    let schema = parse_and_resolve(&schema).map_err(|retry_error| {
        Error::from(error).with_context(
            "error after defining each named type once",
            retry_error.to_string(),
        )
    })?;
    Ok(WriterSchema {
        schema,
        repeated_definitions,
    })
}

/// Parses an Avro schema and checks that its named types resolve, which
/// `Schema::parse` alone doesn't.
fn parse_and_resolve(json: &JsonValue) -> AvroResult<AvroSchema> {
    let schema = AvroSchema::parse(json)?;
    ResolvedSchema::try_from(&schema)?;
    Ok(schema)
}

#[cfg(test)]
mod tests {
    use apache_avro::Writer;
    use serde_derive::Deserialize;
    use serde_json::json;

    use super::*;
    use crate::ErrorKind;
    use crate::avro::Resolved;

    #[derive(Deserialize, Debug, PartialEq)]
    struct Pair {
        a: i64,
        b: Option<String>,
    }

    /// Reads the file's records as `Pair`s, resolving the writer's record name
    /// as manifests are read.
    fn read_pairs(file: &ObjectContainerFile<'_>) -> Result<Vec<Pair>> {
        Ok(file
            .records::<Resolved<Pair>>()?
            .into_iter()
            .map(|pair| pair.0)
            .collect())
    }

    const PAIR: &str = r#"{"type": "record", "name": "pair", "fields": [
        {"name": "a", "type": "long"},
        {"name": "b", "type": ["null", "string"], "default": null}
    ]}"#;

    fn pairs() -> Vec<Pair> {
        vec![
            Pair {
                a: 1,
                b: Some("x".to_string()),
            },
            Pair { a: 2, b: None },
            Pair {
                a: 3,
                b: Some("z".to_string()),
            },
        ]
    }

    fn pair_value(pair: &Pair) -> AvroValue {
        AvroValue::Record(vec![
            ("a".to_string(), AvroValue::Long(pair.a)),
            ("b".to_string(), match &pair.b {
                Some(b) => AvroValue::Union(1, Box::new(AvroValue::String(b.clone()))),
                None => AvroValue::Union(0, Box::new(AvroValue::Null)),
            }),
        ])
    }

    /// Writes `pairs()` with `codec`, in `blocks` blocks, and `user_metadata`.
    fn write_pairs(codec: Codec, blocks: usize, user_metadata: &[(&str, &[u8])]) -> Vec<u8> {
        let schema = AvroSchema::parse_str(PAIR).unwrap();
        let mut writer = Writer::builder()
            .schema(&schema)
            .writer(Vec::new())
            .codec(codec)
            .build()
            .unwrap();
        for (key, value) in user_metadata {
            writer.add_user_metadata(key.to_string(), *value).unwrap();
        }
        let pairs = pairs();
        for chunk in pairs.chunks(pairs.len().div_ceil(blocks)) {
            for pair in chunk {
                writer.append_value(pair_value(pair)).unwrap();
            }
            writer.flush().unwrap();
        }
        writer.into_inner().unwrap()
    }

    /// Where the header's sync marker starts in a file written by `write_pairs`.
    fn header_sync_marker_offset(bs: &[u8]) -> usize {
        let body = bs.strip_prefix(MAGIC).unwrap();
        let (_, rest) = read_metadata(body).unwrap();
        bs.len() - rest.len()
    }

    /// Looks `json` up twice in a row and returns both results, retrying until
    /// the pair agrees; see [`ParsedCache::get_or_parse_twice`].
    fn lookup_twice(json: &[u8]) -> (Arc<WriterSchema>, Arc<WriterSchema>) {
        WRITER_SCHEMAS.get_or_parse_twice(json, || parse_writer_schema(json))
    }

    #[test]
    fn test_writer_schema_cache_returns_same_schema_for_same_bytes() {
        let json = br#"{"type": "record", "name": "same_bytes", "fields": [{"name": "a", "type": "long"}]}"#;

        let (first, second) = lookup_twice(json);

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(
            first.schema,
            AvroSchema::parse_str(std::str::from_utf8(json).unwrap()).unwrap()
        );
        assert!(first.repeated_definitions.is_empty());
    }

    #[test]
    fn test_writer_schema_cache_keeps_different_schemas_apart() {
        let long =
            br#"{"type": "record", "name": "apart", "fields": [{"name": "a", "type": "long"}]}"#;
        let string =
            br#"{"type": "record", "name": "apart", "fields": [{"name": "a", "type": "string"}]}"#;

        let first = cached_writer_schema(long).unwrap();
        let second = cached_writer_schema(string).unwrap();

        assert!(!Arc::ptr_eq(&first, &second));
        assert_ne!(first.schema, second.schema);
        assert_eq!(
            second.schema,
            AvroSchema::parse_str(std::str::from_utf8(string).unwrap()).unwrap()
        );
    }

    #[test]
    fn test_writer_schema_defines_repeated_named_types_once() {
        let decimal = json!({"type": "fixed", "name": "decimal_10_2", "size": 5,
                             "logicalType": "decimal", "precision": 10, "scale": 2});
        let repeated = json!({"type": "record", "name": "repeated", "fields": [
            {"name": "a", "type": decimal},
            {"name": "b", "type": decimal},
        ]});
        let json = serde_json::to_vec(&repeated).unwrap();

        let (schema, again) = lookup_twice(&json);

        assert_eq!(schema.repeated_definitions, vec![
            "decimal_10_2".to_string()
        ]);
        assert!(ResolvedSchema::try_from(&schema.schema).is_ok());
        // The cache is keyed by the bytes as written, so the next file with the
        // same repeated definitions is a hit.
        assert!(Arc::ptr_eq(&schema, &again));
    }

    #[test]
    fn test_writer_schema_reports_original_error_when_definitions_differ() {
        let json = br#"{"type": "record", "name": "differ", "fields": [
            {"name": "a", "type": {"type": "fixed", "name": "f", "size": 4}},
            {"name": "b", "type": {"type": "fixed", "name": "f", "size": 8}}
        ]}"#;

        let err = cached_writer_schema(json).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        let message = err.to_string();
        assert!(
            message.contains("Two named schema defined for same fullname"),
            "{message}"
        );
        assert!(
            !message.contains("after defining each named type once"),
            "{message}"
        );
    }

    #[test]
    fn test_parse_reads_user_metadata_and_records() {
        for codec in [Codec::Null, Codec::Deflate(Default::default())] {
            for blocks in [1, 2] {
                let bs = write_pairs(codec, blocks, &[("k1", b"v1"), ("k2", b"")]);

                let file = ObjectContainerFile::parse(&bs).unwrap();

                assert_eq!(
                    *file.user_metadata(),
                    HashMap::from([
                        ("k1".to_string(), b"v1".to_vec()),
                        ("k2".to_string(), Vec::new()),
                    ]),
                    "{codec:?} {blocks}"
                );
                assert!(file.repeated_named_type_definitions().is_empty());
                assert_eq!(read_pairs(&file).unwrap(), pairs(), "{codec:?} {blocks}");
            }
        }
    }

    #[test]
    fn test_parse_reads_empty_file() {
        let schema = AvroSchema::parse_str(PAIR).unwrap();
        let bs = Writer::new(&schema, Vec::new())
            .unwrap()
            .into_inner()
            .unwrap();

        let file = ObjectContainerFile::parse(&bs).unwrap();

        assert!(file.user_metadata().is_empty());
        assert_eq!(read_pairs(&file).unwrap(), Vec::<Pair>::new());
    }

    #[test]
    fn test_parse_rejects_bad_magic() {
        let mut bs = write_pairs(Codec::Null, 1, &[]);
        bs[3] = 2;

        let err = ObjectContainerFile::parse(&bs).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.to_string()
                .contains("Not an Avro object container file")
        );
    }

    #[test]
    fn test_parse_rejects_truncated_header() {
        let bs = write_pairs(Codec::Null, 1, &[]);
        let truncated = &bs[..header_sync_marker_offset(&bs) + SYNC_MARKER_LEN / 2];

        let err = ObjectContainerFile::parse(truncated).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.to_string().contains("sync marker"), "{err}");
    }

    #[test]
    fn test_records_rejects_block_past_end() {
        let bs = write_pairs(Codec::Null, 1, &[]);
        let truncated = &bs[..bs.len() - SYNC_MARKER_LEN - 1];

        let err = read_pairs(&ObjectContainerFile::parse(truncated).unwrap()).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.to_string().contains("past the end"), "{err}");
    }

    #[test]
    fn test_records_rejects_sync_marker_mismatch() {
        let mut bs = write_pairs(Codec::Null, 1, &[]);
        let last = bs.len() - 1;
        bs[last] ^= 0xff;

        let err = read_pairs(&ObjectContainerFile::parse(&bs).unwrap()).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.to_string().contains("sync marker"), "{err}");
    }

    #[test]
    fn test_records_rejects_negative_count() {
        let bs = write_pairs(Codec::Null, 1, &[]);
        let blocks = header_sync_marker_offset(&bs) + SYNC_MARKER_LEN;
        // Zig-zag 0x01 is -1.
        let mut bs = bs;
        bs[blocks] = 0x01;

        let err = read_pairs(&ObjectContainerFile::parse(&bs).unwrap()).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.to_string().contains("negative"), "{err}");
    }

    #[test]
    fn test_records_rejects_count_beyond_block() {
        let bs = write_pairs(Codec::Null, 1, &[]);
        let blocks = header_sync_marker_offset(&bs) + SYNC_MARKER_LEN;
        // The three records were written as one block, so the count is the
        // single zig-zag byte 0x06. Replace it with a count that, trusted,
        // would size an allocation of count * size_of::<T>() bytes.
        assert_eq!(bs[blocks], 0x06);
        let huge = apache_avro::writer::datum::GenericDatumWriter::builder(&AvroSchema::Long)
            .build()
            .unwrap()
            .write_value_to_vec(AvroValue::Long(i64::MAX >> 8))
            .unwrap();
        let bs = [&bs[..blocks], &huge, &bs[blocks + 1..]].concat();

        let err = read_pairs(&ObjectContainerFile::parse(&bs).unwrap()).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    #[test]
    fn test_records_rejects_record_that_consumes_no_bytes() {
        // A record with no fields encodes to zero bytes, so a block holding
        // one byte and claiming two such records never gets past that byte.
        let empty =
            AvroSchema::parse_str(r#"{"type": "record", "name": "empty", "fields": []}"#).unwrap();
        let mut writer = Writer::new(&empty, Vec::new()).unwrap();
        writer.append_value(AvroValue::Record(Vec::new())).unwrap();
        let bs = writer.into_inner().unwrap();
        let blocks = header_sync_marker_offset(&bs) + SYNC_MARKER_LEN;
        // Written as count 1 (0x02), size 0 (0x00). Claim count 2 and size 1
        // with one byte of data in front of the block's sync marker.
        assert_eq!(&bs[blocks..blocks + 2], &[0x02, 0x00]);
        let (header, block) = bs.split_at(blocks);
        let bs = [header, &[0x04, 0x02, 0x00], &block[2..]].concat();

        #[derive(Deserialize, Debug)]
        struct Empty {}

        let err = ObjectContainerFile::parse(&bs)
            .unwrap()
            .records::<Resolved<Empty>>()
            .map(|records| records.len())
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.to_string().contains("consumed no bytes"), "{err}");
    }

    #[test]
    fn test_parse_rejects_unknown_codec() {
        let bs = write_pairs(Codec::Null, 1, &[]);
        // Rewrite the header through the test-only rewrite helper's building
        // blocks: decode the metadata, swap the codec, re-encode.
        let body = bs.strip_prefix(MAGIC).unwrap();
        let (mut metadata, rest) = read_metadata(body).unwrap();
        metadata.insert(CODEC_KEY.to_string(), AvroValue::Bytes(b"lz4".to_vec()));
        let metadata = apache_avro::writer::datum::GenericDatumWriter::builder(&metadata_schema())
            .build()
            .unwrap()
            .write_value_to_vec(AvroValue::Map(metadata))
            .unwrap();
        let bs = [MAGIC, &metadata, rest].concat();

        let err = ObjectContainerFile::parse(&bs).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.to_string().contains("lz4"), "{err}");
    }
}
