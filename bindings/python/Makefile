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

.PHONY: build install test install-cargo-about cargo-about

build:
	uv run maturin develop

# uv sync may remove local editable pyiceberg-core, so install rebuilds it afterwards.
install:
	uv sync --group dev --no-install-project
	$(MAKE) build

test:
	uv run --no-sync pytest

# Keep version in sync with .github/actions/generate-python-license/action.yml.
install-cargo-about:
	cargo install --locked --features cli cargo-about@0.9.2

cargo-about: install-cargo-about
	cargo about generate -c about.toml --locked --fail --manifest-path Cargo.toml about.hbs
