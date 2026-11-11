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

#![doc = include_str!("../README.md")]

use proc_macro::TokenStream;
use syn::{DeriveInput, ItemStruct, parse_macro_input};

mod properties;
mod properties_view;

/// Derives property-map parsing and opt-in read-only accessors for a struct.
#[proc_macro_derive(Properties, attributes(property))]
pub fn derive_properties(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    match properties::expand_properties(input) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.into_compile_error().into(),
    }
}

/// Defines a borrowed view that parses properties independently through generated getters.
#[proc_macro]
pub fn properties_view(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);

    match properties_view::expand_properties_view(input) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.into_compile_error().into(),
    }
}
