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

/// `LocationGenerator` used to generate the location of data file.
pub trait LocationGenerator: Clone + Send + 'static {
    /// Generate a absolute path for the given file name.
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
/// The rule of file name is aligned with the OutputFileFactory in iceberg-java
#[derive(Clone)]
pub struct DefaultFileNameGenerator {
    partition_id: u64,
    task_id: u64,
    // The purpose of this id is to be able to know from two paths that they were written by the
    // same operation.
    // That's useful, for example, if a Spark job dies and leaves files in the file system, you can
    // identify them all
    // with a recursive listing and grep.
    operator_id: String,
    suffix: String,
    file_count: Arc<AtomicU64>,
}

impl DefaultFileNameGenerator {
    /// Create a new `FileNameGenerator`.
    pub fn new(
        partition_id: u64,
        task_id: u64,
        operator_id: String,
        suffix: Option<String>,
    ) -> Self {
        let suffix = if let Some(suffix) = suffix {
            format!("--{}", suffix)
        } else {
            "".to_string()
        };
        Self {
            partition_id,
            task_id,
            operator_id,
            suffix,
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
            "{:05}-{}-{}-{:05}{}",
            self.partition_id, self.task_id, self.operator_id, file_id, self.suffix
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
