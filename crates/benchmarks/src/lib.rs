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

use std::{env::{set_current_dir, temp_dir}, fs::{create_dir_all, read}, path::PathBuf, process, str::FromStr};

use bytes::Bytes;
use iceberg::io::FileIO;
use rand::{thread_rng, RngCore};
use tokio::runtime::Runtime;
use walkdir::WalkDir;

/// runs the python construction script, provided just the file name without .py
/// 
/// ("sql-catalog-querying-taxicab" etc.)
pub fn run_construction_script(script_name: &str) -> PathBuf {
    let script_filename = [script_name, "py"].join(".");
    let mut script_path = PathBuf::from_str(env!("CARGO_MANIFEST_DIR")).unwrap();
    script_path.push("src");
    script_path.push("construction_scripts");

    set_current_dir(&script_path).unwrap();

    script_path.push(script_filename);

    // should look like /tmp/iceberg_benchmark_tables/<script_name>-<random number>>
    let mut working_dir = temp_dir();
    working_dir.push("iceberg_benchmark_tables");
    working_dir.push([script_name, &thread_rng().next_u64().to_string()].join("-"));

    create_dir_all(&working_dir).unwrap();

    process::Command::new("python")
        .arg(script_path.to_str().unwrap())
        .arg(working_dir.to_str().unwrap())
        .spawn()
        .unwrap()
        .wait()
        .unwrap();

    working_dir
}

pub async fn copy_dir_to_fileio(dir: PathBuf, fileio: &FileIO) {
    for entry in WalkDir::new(dir).into_iter()
        .map(|res| res.unwrap())
        .filter(|entry| entry.file_type().is_file()) {
            let path = entry.into_path();
            let output = fileio.new_output(path.to_str().unwrap()).unwrap();
            let bytes = Bytes::from(read(path).unwrap());
            output.write(bytes).await.unwrap();
        }
}
