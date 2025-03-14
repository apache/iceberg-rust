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

//! Iceberg FFI based of iceberg rust which allows to interact with iceberg from C.

#![warn(missing_docs)]
// This crate is the C binding for the iceberg project.
// So it's type node can't meet camel case.
#![allow(non_camel_case_types)]
// This crate is the C binding for the iceberg project.
// Nearly all the functions exposed to C FFI are unsafe.
#![allow(clippy::missing_safety_doc)]

mod specs;
