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

//! Iceberg Spec FFI

use std::ffi::CString;
use std::mem::ManuallyDrop;
use std::os::raw::c_char;

use iceberg as core;

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub enum PrimitiveLiteralTag {
    Boolean,
    Int,
    Long,
    Float,
    Double,
    String,
    Binary,
    Int128,
    UInt128,
    AboveMax,
    BelowMin,
}

#[repr(C)]
pub union PrimitiveLiteralUnion {
    boolean: bool,
    int: i32,
    long: i64,
    float: f32,
    double: f64,
    int128: i128,
    uint128: u128,
    string: *const c_char,
    binary: ManuallyDrop<BinarySlice>,
}

#[repr(C)]
pub struct BinarySlice {
    ptr: *const u8,
    len: usize,
}

#[repr(C)]
pub struct PrimitiveLiteral {
    tag: PrimitiveLiteralTag,
    data: PrimitiveLiteralUnion,
}

impl From<core::spec::PrimitiveLiteral> for PrimitiveLiteral {
    fn from(literal: core::spec::PrimitiveLiteral) -> Self {
        match literal {
            core::spec::PrimitiveLiteral::Boolean(b) => Self {
                tag: PrimitiveLiteralTag::Boolean,
                data: PrimitiveLiteralUnion { boolean: b },
            },
            core::spec::PrimitiveLiteral::Int(i) => Self {
                tag: PrimitiveLiteralTag::Int,
                data: PrimitiveLiteralUnion { int: i },
            },
            core::spec::PrimitiveLiteral::Long(l) => Self {
                tag: PrimitiveLiteralTag::Long,
                data: PrimitiveLiteralUnion { long: l },
            },
            core::spec::PrimitiveLiteral::Float(f) => Self {
                tag: PrimitiveLiteralTag::Float,
                data: PrimitiveLiteralUnion {
                    float: f.into_inner(),
                },
            },
            core::spec::PrimitiveLiteral::Double(d) => Self {
                tag: PrimitiveLiteralTag::Double,
                data: PrimitiveLiteralUnion {
                    double: d.into_inner(),
                },
            },
            core::spec::PrimitiveLiteral::String(s) => {
                let c_string = CString::new(s).expect("CString::new failed");
                Self {
                    tag: PrimitiveLiteralTag::String,
                    data: PrimitiveLiteralUnion {
                        string: c_string.into_raw(),
                    },
                }
            }
            core::spec::PrimitiveLiteral::Binary(b) => {
                let ptr = b.as_ptr();
                let len = b.len();
                std::mem::forget(b);
                Self {
                    tag: PrimitiveLiteralTag::Binary,
                    data: PrimitiveLiteralUnion {
                        binary: ManuallyDrop::new(BinarySlice { ptr, len }),
                    },
                }
            }
            core::spec::PrimitiveLiteral::Int128(i) => Self {
                tag: PrimitiveLiteralTag::Int128,
                data: PrimitiveLiteralUnion { int128: i },
            },
            core::spec::PrimitiveLiteral::UInt128(u) => Self {
                tag: PrimitiveLiteralTag::UInt128,
                data: PrimitiveLiteralUnion { uint128: u },
            },
            core::spec::PrimitiveLiteral::AboveMax => Self {
                tag: PrimitiveLiteralTag::AboveMax,
                data: PrimitiveLiteralUnion { int: 0 },
            },
            core::spec::PrimitiveLiteral::BelowMin => Self {
                tag: PrimitiveLiteralTag::BelowMin,
                data: PrimitiveLiteralUnion { int: 0 },
            },
        }
    }
}

#[no_mangle]
pub extern "C" fn iceberg_spec_primitive_literal_free(literal: *mut PrimitiveLiteral) {
    if literal.is_null() {
        return;
    }
    let lit = unsafe { &*literal };
    unsafe {
        if let PrimitiveLiteralTag::String = lit.tag {
            let _ = CString::from_raw(lit.data.string as *mut c_char);
        }
        if let PrimitiveLiteralTag::Binary = lit.tag {
            let _ = Vec::from_raw_parts(
                lit.data.binary.ptr as *mut u8,
                lit.data.binary.len,
                lit.data.binary.len,
            );
        }
        drop(Box::from_raw(literal));
    }
}
