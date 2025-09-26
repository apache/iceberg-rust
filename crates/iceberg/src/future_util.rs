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

//! A module for `futures`-related utilities.
//!
//! The primary purpose of this module is to provide a type alias to maintain
//! compatibility between WebAssembly (WASM) and native targets.
//!
//! `BoxedStream` is defined as a different type depending on the compilation target:
//! - **Native environments (`not(target_arch = "wasm32")`)**: Uses `futures::stream::BoxStream`.
//!   This stream implements the `Send` trait, allowing it to be safely sent across threads.
//! - **WASM environments (`target_arch = "wasm32"`)**: Uses `futures::stream::LocalBoxStream`.
//!   Since WASM is typically a single-threaded environment, `LocalBoxStream`, which is `!Send`,
//!   is the appropriate choice.
//!
//! This conditional compilation allows for seamless support of various platforms
//! while maintaining a single codebase.

/// BoxedStream is the type alias of [`futures::stream::BoxStream`].
#[cfg(not(target_arch = "wasm32"))]
pub type BoxedStream<'a, T> = futures::stream::BoxStream<'a, T>;
#[cfg(target_arch = "wasm32")]
/// BoxedStream is the type alias of [`futures::stream::LocalBoxStream`].
pub type BoxedStream<'a, T> = futures::stream::LocalBoxStream<'a, T>;
