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

use std::fs;
use std::path::PathBuf;

use iceberg_sqllogictest::schedule::Schedule;
use libtest_mimic::{Arguments, Trial};
use tokio::runtime::Handle;

pub fn main() {
    env_logger::init();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    // Parse command line arguments
    let args = Arguments::from_args();

    let tests = collect_trials(rt.handle().clone()).unwrap();
    let result = libtest_mimic::run(&args, tests);

    drop(rt);

    result.exit();
}

pub(crate) fn collect_trials(handle: Handle) -> anyhow::Result<Vec<Trial>> {
    let schedule_files = collect_schedule_files()?;
    log::debug!(
        "Found {} schedules files: {:?}",
        schedule_files.len(),
        &schedule_files
    );
    let mut trials = Vec::with_capacity(schedule_files.len());
    for schedule_file in schedule_files {
        let h = handle.clone();
        let trial_name = format!(
            "schedule: {}",
            schedule_file
                .file_name()
                .expect("Schedule file should have a name")
                .to_string_lossy()
        );
        let trial = Trial::test(trial_name, move || {
            Ok(h.block_on(run_schedule(schedule_file.clone()))?)
        });
        trials.push(trial);
    }
    Ok(trials)
}

pub(crate) fn collect_schedule_files() -> anyhow::Result<Vec<PathBuf>> {
    let dir = PathBuf::from(format!("{}/testdata/schedules", env!("CARGO_MANIFEST_DIR")));
    let mut schedule_files = Vec::with_capacity(32);
    for entry in fs::read_dir(&dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            schedule_files.push(fs::canonicalize(dir.join(path))?);
        }
    }
    Ok(schedule_files)
}

pub(crate) async fn run_schedule(schedule_file: PathBuf) -> anyhow::Result<()> {
    let schedules = Schedule::from_file(schedule_file).await?;
    schedules.run().await?;

    Ok(())
}

#[cfg(test)]
mod tests {

    #[tokio::test]
    async fn test_catalog_config_schedule_execution() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        let schedule_path = PathBuf::from(format!(
            "{}/testdata/schedules/catalog_config.toml",
            env!("CARGO_MANIFEST_DIR")
        ));

        let result = rt.block_on(run_schedule(schedule_path));
        assert!(
            result.is_ok(),
            "Catalog config schedule should execute successfully"
        );
    }

    #[tokio::test]
    async fn test_shared_catalog_schedule_execution() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        let schedule_path = PathBuf::from(format!(
            "{}/testdata/schedules/shared_catalog.toml",
            env!("CARGO_MANIFEST_DIR")
        ));

        let result = rt.block_on(run_schedule(schedule_path));
        assert!(
            result.is_ok(),
            "Shared catalog schedule should execute successfully"
        );
    }

    #[tokio::test]
    async fn test_error_cases_schedule_execution() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        let schedule_path = PathBuf::from(format!(
            "{}/testdata/schedules/error_cases.toml",
            env!("CARGO_MANIFEST_DIR")
        ));

        let result = rt.block_on(run_schedule(schedule_path));
        assert!(
            result.is_ok(),
            "Error cases schedule should execute successfully"
        );
    }

    #[test]
    fn test_collect_schedule_files() {
        let files = collect_schedule_files().unwrap();
        assert!(
            !files.is_empty(),
            "Should find at least some schedule files"
        );

        // Verify that the newly created files are included
        let file_names: Vec<String> = files
            .iter()
            .filter_map(|p| p.file_name()?.to_str())
            .map(|s| s.to_string())
            .collect();

        assert!(file_names.contains(&"catalog_config.toml".to_string()));
        assert!(file_names.contains(&"shared_catalog.toml".to_string()));
        assert!(file_names.contains(&"error_cases.toml".to_string()));
    }
}
