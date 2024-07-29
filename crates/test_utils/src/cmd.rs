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

use std::process::Command;

pub fn run_command(mut cmd: Command, desc: impl ToString) {
    let desc = desc.to_string();
    log::info!("Starting to {}, command: {:?}", &desc, cmd);
    let exit = cmd.status().unwrap();
    if exit.success() {
        log::info!("{} succeed!", desc)
    } else {
        panic!("{} failed: {:?}", desc, exit);
    }
}

pub fn get_cmd_output_result(mut cmd: Command, desc: impl ToString) -> Result<String, String> {
    let desc = desc.to_string();
    log::info!("Starting to {}, command: {:?}", &desc, cmd);
    let result = cmd.output();
    match result {
        Ok(output) => {
            if output.status.success() {
                log::info!("{} succeed!", desc);
                Ok(String::from_utf8(output.stdout).unwrap())
            } else {
                Err(format!("{} failed with rc: {:?}", desc, output.status))
            }
        }
        Err(_err) => Err(format!("{} failed to execute", desc)),
    }
}

pub fn get_cmd_output(cmd: Command, desc: impl ToString) -> String {
    let result = get_cmd_output_result(cmd, desc);
    match result {
        Ok(output_str) => output_str,
        Err(err) => panic!("{}", err),
    }
}
