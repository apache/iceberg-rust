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

//! This module contains the location generator and file name generator for generating path of data file.

use std::sync::{atomic::AtomicU64, Arc};

use crate::spec::DataFileFormat;

/// `LocationGenerator` used to generate the location of data file.
pub trait LocationGenerator: Clone + Send + 'static {
    /// Generate an absolute path for the given file name.
    /// e.g
    /// For file name "part-00000.parquet", the generated location maybe "/table/data/part-00000.parquet"
    fn generate_location(&self, file_name: &str) -> String;
}

/// `FileNameGeneratorTrait` used to generate file name for data file. The file name can be passed to `LocationGenerator` to generate the location of the file.
pub trait FileNameGenerator: Clone + Send + 'static {
    /// Generate a file name.
    fn generate_file_name(&self) -> String;
}

/// `DefaultFileNameGenerator` used to generate file name for data file. The file name can be
/// passed to `LocationGenerator` to generate the location of the file.
/// The file name format is "{prefix}-{file_count}[-{suffix}].{file_format}".
#[derive(Clone)]
pub struct DefaultFileNameGenerator {
    prefix: String,
    suffix: String,
    format: String,
    file_count: Arc<AtomicU64>,
}

impl DefaultFileNameGenerator {
    /// Create a new `FileNameGenerator`.
    pub fn new(prefix: String, suffix: Option<String>, format: DataFileFormat) -> Self {
        let suffix = if let Some(suffix) = suffix {
            format!("-{}", suffix)
        } else {
            "".to_string()
        };

        Self {
            prefix,
            suffix,
            format: format.to_string(),
            file_count: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl FileNameGenerator for DefaultFileNameGenerator {
    fn generate_file_name(&self) -> String {
        let file_id = self
            .file_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        format!(
            "{}-{:05}{}.{}",
            self.prefix, file_id, self.suffix, self.format
        )
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::LocationGenerator;

    #[derive(Clone)]
    pub(crate) struct MockLocationGenerator {
        root: String,
    }

    impl MockLocationGenerator {
        pub(crate) fn new(root: String) -> Self {
            Self { root }
        }
    }

    impl LocationGenerator for MockLocationGenerator {
        fn generate_location(&self, file_name: &str) -> String {
            format!("{}/{}", self.root, file_name)
        }
    }
}
