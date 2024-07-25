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

use crate::Result;
use opendal::{Operator, Scheme};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};

#[derive(Default, Clone)]
pub(crate) struct MemoryConfig {}

impl Debug for MemoryConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryConfig").finish()
    }
}

impl MemoryConfig {
    /// Decode from iceberg props.
    pub fn new(_: HashMap<String, String>) -> Self {
        Self::default()
    }

    /// Build new opendal operator from given path.
    pub fn build(&self, _: &str) -> Result<Operator> {
        let m = HashMap::new();
        Ok(Operator::via_map(Scheme::Memory, m)?)
    }
}
