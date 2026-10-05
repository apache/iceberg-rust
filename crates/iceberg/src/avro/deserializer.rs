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

//! Deserialization straight from an Avro writer schema, with the parts of Avro
//! schema resolution that apache-avro's schema-aware deserializer leaves out.
//!
//! `Reader::into_deser_iter` decodes into serde types without building
//! `apache_avro::types::Value`s, but it requires the reader schema to equal the
//! writer schema and applies no resolution rules. [`ResolvingDeserializer`]
//! wraps it and routes every value through `deserialize_any`, which follows the
//! writer schema. As a result:
//!
//! - Avro record names don't have to match serde type names.
//! - A writer value that isn't a union reads into an `Option`.
//! - serde's numeric visitors convert between numeric types, so an `int` reads
//!   into an `i64` and a `float` into an `f64`. An integer that doesn't fit the
//!   target, such as a `long` above `i32::MAX` read into an `i32`, returns an
//!   error. Conversions into `f32` or `f64` use `as` and can lose precision.
//!
//! apache-avro plans a `SchemaAwareResolvingDeserializer` that resolves against
//! a reader schema (<https://github.com/apache/avro-rs/issues/575>). Once a
//! release includes it, readers can pass their reader schema to
//! `Reader::builder` and drop this module, provided it doesn't reject writer
//! record names that differ from the reader's. The Avro spec requires record
//! names to match, and the writers this crate reads from don't all agree on
//! them.

use std::fmt;

use serde::de::value::{
    BorrowedBytesDeserializer, BorrowedStrDeserializer, BytesDeserializer, EnumAccessDeserializer,
    MapAccessDeserializer, SeqAccessDeserializer,
};
use serde::de::{
    DeserializeSeed, Deserializer, EnumAccess, Error, IntoDeserializer, MapAccess, SeqAccess,
    Visitor,
};
use serde::{Deserialize, forward_to_deserialize_any};

/// Deserializes the wrapped type through [`ResolvingDeserializer`], for use
/// with `Reader::into_deser_iter`.
pub(crate) struct Resolved<T>(pub T);

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Resolved<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        T::deserialize(ResolvingDeserializer(deserializer)).map(Resolved)
    }
}

struct ResolvingDeserializer<D>(D);

impl<'de, D: Deserializer<'de>> Deserializer<'de> for ResolvingDeserializer<D> {
    type Error = D::Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
        self.0.deserialize_any(ResolvingVisitor(visitor))
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
        self.0.deserialize_any(OptionVisitor(visitor))
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
        self.0.deserialize_ignored_any(visitor)
    }

    fn is_human_readable(&self) -> bool {
        self.0.is_human_readable()
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf
        unit unit_struct newtype_struct seq tuple tuple_struct map struct enum identifier
    }
}

/// Wraps the sequences and maps a visitor receives, so that their elements are
/// deserialized through [`ResolvingDeserializer`] too.
struct ResolvingVisitor<V>(V);

/// Forwards `visit_*` methods for values that contain no nested values.
macro_rules! forward_visit {
    ($($method:ident($ty:ty)),* $(,)?) => {
        $(
            fn $method<E: Error>(self, v: $ty) -> Result<Self::Value, E> {
                self.0.$method(v)
            }
        )*
    };
}

impl<'de, V: Visitor<'de>> Visitor<'de> for ResolvingVisitor<V> {
    type Value = V::Value;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        self.0.expecting(formatter)
    }

    forward_visit! {
        visit_bool(bool), visit_i8(i8), visit_i16(i16), visit_i32(i32), visit_i64(i64),
        visit_i128(i128), visit_u8(u8), visit_u16(u16), visit_u32(u32), visit_u64(u64),
        visit_u128(u128), visit_f32(f32), visit_f64(f64), visit_char(char), visit_str(&str),
        visit_borrowed_str(&'de str), visit_string(String), visit_bytes(&[u8]),
        visit_borrowed_bytes(&'de [u8]), visit_byte_buf(Vec<u8>),
    }

    fn visit_none<E: Error>(self) -> Result<Self::Value, E> {
        self.0.visit_none()
    }

    fn visit_unit<E: Error>(self) -> Result<Self::Value, E> {
        self.0.visit_unit()
    }

    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        self.0.visit_some(ResolvingDeserializer(deserializer))
    }

    fn visit_newtype_struct<D: Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        self.0
            .visit_newtype_struct(ResolvingDeserializer(deserializer))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, seq: A) -> Result<Self::Value, A::Error> {
        self.0.visit_seq(ResolvingAccess(seq))
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
        self.0.visit_map(ResolvingAccess(map))
    }

    fn visit_enum<A: EnumAccess<'de>>(self, data: A) -> Result<Self::Value, A::Error> {
        self.0.visit_enum(data)
    }
}

/// Reads an `Option` from any writer value. A null becomes `None`, and anything
/// else becomes `Some`, whether or not the writer schema is a union.
struct OptionVisitor<V>(V);

/// Implements `visit_*` methods that hand a value to `visit_some` through the
/// deserializer that `deserializer` builds from it.
macro_rules! visit_some {
    ($($method:ident($ty:ty) => $deserializer:expr),* $(,)?) => {
        $(
            fn $method<E: Error>(self, v: $ty) -> Result<Self::Value, E> {
                self.0.visit_some($deserializer(v))
            }
        )*
    };
}

impl<'de, V: Visitor<'de>> Visitor<'de> for OptionVisitor<V> {
    type Value = V::Value;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        self.0.expecting(formatter)
    }

    visit_some! {
        visit_bool(bool) => IntoDeserializer::into_deserializer,
        visit_i8(i8) => IntoDeserializer::into_deserializer,
        visit_i16(i16) => IntoDeserializer::into_deserializer,
        visit_i32(i32) => IntoDeserializer::into_deserializer,
        visit_i64(i64) => IntoDeserializer::into_deserializer,
        visit_i128(i128) => IntoDeserializer::into_deserializer,
        visit_u8(u8) => IntoDeserializer::into_deserializer,
        visit_u16(u16) => IntoDeserializer::into_deserializer,
        visit_u32(u32) => IntoDeserializer::into_deserializer,
        visit_u64(u64) => IntoDeserializer::into_deserializer,
        visit_u128(u128) => IntoDeserializer::into_deserializer,
        visit_f32(f32) => IntoDeserializer::into_deserializer,
        visit_f64(f64) => IntoDeserializer::into_deserializer,
        visit_char(char) => IntoDeserializer::into_deserializer,
        visit_str(&str) => IntoDeserializer::into_deserializer,
        visit_borrowed_str(&'de str) => BorrowedStrDeserializer::new,
        visit_string(String) => IntoDeserializer::into_deserializer,
        visit_bytes(&[u8]) => BytesDeserializer::new,
        visit_borrowed_bytes(&'de [u8]) => BorrowedBytesDeserializer::new,
    }

    fn visit_byte_buf<E: Error>(self, v: Vec<u8>) -> Result<Self::Value, E> {
        self.0.visit_some(BytesDeserializer::new(&v))
    }

    fn visit_none<E: Error>(self) -> Result<Self::Value, E> {
        self.0.visit_none()
    }

    fn visit_unit<E: Error>(self) -> Result<Self::Value, E> {
        self.0.visit_none()
    }

    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        self.0.visit_some(ResolvingDeserializer(deserializer))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, seq: A) -> Result<Self::Value, A::Error> {
        self.0
            .visit_some(SeqAccessDeserializer::new(ResolvingAccess(seq)))
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
        self.0
            .visit_some(MapAccessDeserializer::new(ResolvingAccess(map)))
    }

    fn visit_enum<A: EnumAccess<'de>>(self, data: A) -> Result<Self::Value, A::Error> {
        self.0.visit_some(EnumAccessDeserializer::new(data))
    }
}

/// Deserializes the elements of a sequence, or the keys and values of a map,
/// through [`ResolvingDeserializer`]. Keys need it because apache-avro's record
/// field names only support `deserialize_identifier` and `deserialize_any`,
/// and a key read as a `String` calls `deserialize_string`.
struct ResolvingAccess<A>(A);

impl<'de, A: SeqAccess<'de>> SeqAccess<'de> for ResolvingAccess<A> {
    type Error = A::Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, A::Error> {
        self.0.next_element_seed(ResolvingSeed(seed))
    }

    fn size_hint(&self) -> Option<usize> {
        self.0.size_hint()
    }
}

impl<'de, A: MapAccess<'de>> MapAccess<'de> for ResolvingAccess<A> {
    type Error = A::Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, A::Error> {
        self.0.next_key_seed(ResolvingSeed(seed))
    }

    fn next_value_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<T::Value, A::Error> {
        self.0.next_value_seed(ResolvingSeed(seed))
    }

    fn size_hint(&self) -> Option<usize> {
        self.0.size_hint()
    }
}

struct ResolvingSeed<S>(S);

impl<'de, S: DeserializeSeed<'de>> DeserializeSeed<'de> for ResolvingSeed<S> {
    type Value = S::Value;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<S::Value, D::Error> {
        self.0.deserialize(ResolvingDeserializer(deserializer))
    }
}
