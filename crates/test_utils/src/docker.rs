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


use tokio::sync::Mutex;

use crate::cmd::{get_cmd_output, run_command};
use std::{
    future::Future,
    process::Command,
    sync::{Arc, OnceLock, Weak},
};

pub async fn lazy_dc<T, F, Fut>(
    reuse_resource: &'static OnceLock<Mutex<Weak<T>>>,
    init_func: F,
) -> Arc<T>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
{
    let mut guard = reuse_resource
        .get_or_init(|| Mutex::new(Weak::new()))
        .lock()
        .await;

    let maybe_tf = guard.upgrade();

    if let Some(tf) = maybe_tf {
        tf
    } else {
        let tf = Arc::new(init_func().await);
        *guard = Arc::downgrade(&tf);

        tf
    }
}

/// A utility to manage the lifecycle of `docker compose`.
///
/// It will start `docker compose` when calling the `run` method and will be stopped via [`Drop`].
#[derive(Debug)]
pub struct DockerCompose {
    project_name: String,
    docker_compose_dir: String,
}

impl DockerCompose {
    pub fn new(project_name: impl ToString, docker_compose_dir: impl ToString) -> Self {
        Self {
            project_name: project_name.to_string(),
            docker_compose_dir: docker_compose_dir.to_string(),
        }
    }

    pub fn project_name(&self) -> &str {
        self.project_name.as_str()
    }

    pub fn run(&self) {
        let mut cmd = Command::new("docker");
        cmd.current_dir(&self.docker_compose_dir);

        cmd.args(vec![
            "compose",
            "-p",
            self.project_name.as_str(),
            "up",
            "-d",
            "--wait",
            "--timeout",
            "1200000",
        ]);

        run_command(
            cmd,
            format!(
                "Starting docker compose in {}, project name: {}",
                self.docker_compose_dir, self.project_name
            ),
        )
    }

    pub fn get_container_ip(&self, service_name: impl AsRef<str>) -> String {
        let container_name = format!("{}-{}-1", self.project_name, service_name.as_ref());
        let mut cmd = Command::new("docker");
        cmd.arg("inspect")
            .arg("-f")
            .arg("{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}")
            .arg(&container_name);

        get_cmd_output(cmd, format!("Get container ip of {container_name}"))
            .trim()
            .to_string()
    }
}

impl Drop for DockerCompose {
    fn drop(&mut self) {
        let mut cmd = Command::new("docker");
        cmd.current_dir(&self.docker_compose_dir);

        cmd.args(vec![
            "compose",
            "-p",
            self.project_name.as_str(),
            "down",
            "-v",
            "--remove-orphans",
        ]);

        run_command(
            cmd,
            format!(
                "Stopping docker compose in {}, project name: {}",
                self.docker_compose_dir, self.project_name
            ),
        )
    }
}
