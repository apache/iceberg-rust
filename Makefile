# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

.EXPORT_ALL_VARIABLES:

RUST_LOG = debug

build:
	cargo build

check-fmt:
	cargo fmt --all -- --check

check-clippy:
	cargo clippy --all-targets --all-features --workspace -- -D warnings

cargo-sort:
	cargo install cargo-sort
	cargo sort -c -w

fix-toml:
	cargo install taplo-cli --locked
	taplo fmt

check-toml:
	cargo install taplo-cli --locked
	taplo check

check: check-fmt check-clippy cargo-sort check-toml

unit-test:
	cargo test --no-fail-fast --lib --all-features --workspace

test:
	cargo test --no-fail-fast --all-targets --all-features --workspace
	cargo test --no-fail-fast --doc --all-features --workspace