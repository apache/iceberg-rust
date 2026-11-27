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

use iceberg::Result;
use opendal::Operator;
use opendal::services::MemoryConfig;

use crate::utils::from_opendal_error;

pub(crate) fn memory_config_build() -> Result<Operator> {
    Operator::from_config(MemoryConfig::default()).map_err(from_opendal_error)
}

#[cfg(test)]
mod tests {
    use crate::{OpenDalClientConfig, OpenDalStorage, default_memory_operator};

    #[test]
    fn test_default_memory_operator() {
        let op = default_memory_operator();
        assert_eq!(op.info().scheme().to_string(), "memory");
    }

    #[test]
    fn test_relativize_path_memory() {
        let storage = OpenDalStorage::Memory {
            operator: default_memory_operator(),
            client_config: OpenDalClientConfig::default(),
        };

        assert_eq!(
            storage.relativize_path("memory:/path/to/file").unwrap(),
            "path/to/file"
        );
        // Without the scheme prefix, falls back to stripping the leading slash
        assert_eq!(
            storage.relativize_path("/path/to/file").unwrap(),
            "path/to/file"
        );
    }
}
