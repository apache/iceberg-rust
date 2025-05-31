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

use std::sync::OnceLock;

use tokio::runtime::{Handle, Runtime};

pub fn runtime() -> Handle {
    if let Ok(h) = Handle::try_current() {
        h
    } else {
        static RUNTIME: OnceLock<Runtime> = OnceLock::new();
        static PID: OnceLock<u32> = OnceLock::new();
        let pid = std::process::id();
        let runtime_pid = *PID.get_or_init(|| pid);
        if pid != runtime_pid {
            panic!(
                "Forked process detected - current PID is {pid} but the tokio runtime was created by {runtime_pid}. The tokio \
                runtime does not support forked processes https://github.com/tokio-rs/tokio/issues/4301. If you are \
                seeing this message while using Python multithreading make sure to use the `spawn` or `forkserver` \
                mode.",
            );
        }
        RUNTIME
            .get_or_init(|| Runtime::new().expect("Failed to create a tokio runtime."))
            .handle()
            .clone()
    }
}
