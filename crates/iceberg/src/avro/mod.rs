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

//! Avro related codes.
mod deserializer;
mod named_types;
mod schema;
pub(crate) use deserializer::Resolved;
pub(crate) use named_types::define_named_types_once;
#[cfg(test)]
pub(crate) use named_types::define_named_types_repeatedly;
pub(crate) use schema::*;

/// Creates an Avro writer with deflate compression.
pub(crate) fn new_writer<W: std::io::Write>(
    schema: &apache_avro::Schema,
    writer: W,
) -> apache_avro::AvroResult<apache_avro::Writer<'_, W>> {
    apache_avro::Writer::with_codec(
        schema,
        writer,
        apache_avro::Codec::Deflate(Default::default()),
    )
}

#[cfg(test)]
pub(crate) fn assert_deflate_codec(bytes: &[u8]) {
    use apache_avro::Schema;
    use apache_avro::reader::datum::GenericDatumReader;
    use apache_avro::schema::MapSchema;
    use apache_avro::types::Value;

    let schema = Schema::Map(MapSchema {
        types: Box::new(Schema::Bytes),
        attributes: Default::default(),
    });
    let mut body = bytes.strip_prefix(b"Obj\x01").unwrap();
    let Value::Map(metadata) = GenericDatumReader::builder(&schema)
        .build()
        .unwrap()
        .read_value(&mut body)
        .unwrap()
    else {
        panic!("Avro header metadata must be a map");
    };
    assert_eq!(
        metadata.get("avro.codec"),
        Some(&Value::Bytes(b"deflate".to_vec()))
    );
}
