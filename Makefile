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

build:
	cargo build --all-targets --all-features --workspace

check-fmt:
	cargo fmt --all -- --check

check-clippy:
	cargo clippy --all-targets --all-features --workspace -- -D warnings

install-cargo-machete:
	cargo install cargo-machete@0.7.0

cargo-machete: install-cargo-machete
	cargo machete

install-cargo-nextest:
	cargo install --locked cargo-nextest

nextest: install-cargo-nextest
	cargo nextest run --all-targets --all-features --workspace

install-taplo-cli:
	cargo install taplo-cli@0.9.3

fix-toml: install-taplo-cli
	taplo fmt

check-toml: install-taplo-cli
	taplo fmt --check

NIGHTLY_VERSION := $(shell awk -F'"' '/^channel/ {print $$2}' rust-toolchain.toml)
MSRV_VERSION    := $(shell awk -F'"' '/^rust-version/ {print $$2}' Cargo.toml)

check-msrv:
	cargo +$(MSRV_VERSION) check --workspace

PUBLIC_API_CRATES := $(shell cargo metadata --no-deps --format-version 1 | \
	jq -r '.packages[] | select(.publish == null) | .name')

install-cargo-public-api:
	cargo install --locked cargo-public-api@0.51.0

generate-public-api: install-cargo-public-api
	@for crate in $(PUBLIC_API_CRATES); do \
		echo "Generating public API for $$crate..."; \
		cargo public-api -p "$$crate" --all-features > "public-api/$$crate.txt"; \
	done

check-public-api: install-cargo-public-api
	@fail=0; \
	for crate in $(PUBLIC_API_CRATES); do \
		echo "Checking public API for $$crate..."; \
		cargo public-api -p "$$crate" --all-features | diff - "public-api/$$crate.txt" || { \
			echo "ERROR: Public API for $$crate has changed. Run 'make generate-public-api' to update."; \
			fail=1; \
		}; \
	done; \
	if [ $$fail -ne 0 ]; then exit 1; fi

check: check-fmt check-clippy check-toml cargo-machete

doc-test:
	cargo test --no-fail-fast --doc --all-features --workspace

unit-test: doc-test
	cargo test --no-fail-fast --lib --all-features --workspace

test: docker-up
	@trap '$(MAKE) docker-down' EXIT; \
	$(MAKE) nextest

clean:
	cargo clean

install-mdbook:
	cargo install mdbook@0.4.36

site: install-mdbook
	cd website && mdbook serve

# Docker targets for integration tests
docker-up:
	docker compose -f dev/docker-compose.yaml up -d --build --wait

docker-down:
	docker compose -f dev/docker-compose.yaml down -v --remove-orphans --timeout 0

docker-logs:
	docker compose -f dev/docker-compose.yaml logs -f
