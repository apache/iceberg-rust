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

//! Fixtures for tests that need encoded deletion vectors.

use roaring::RoaringTreemap;

/// Encodes a `deletion-vector-v1` Puffin blob for the given positions, matching the framing in
/// [`DeleteVector::deserialize`](crate::delete_vector::DeleteVector::deserialize).
pub(crate) fn encode_dv_blob(positions: impl IntoIterator<Item = u64>) -> Vec<u8> {
    let mut bitmap = RoaringTreemap::new();
    for pos in positions {
        bitmap.insert(pos);
    }
    let mut vector = Vec::new();
    bitmap.serialize_into(&mut vector).unwrap();
    crate::delete_vector::frame_dv_blob(&vector)
}
