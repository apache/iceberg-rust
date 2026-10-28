<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# LICENSE and NOTICE for pyiceberg-core

The `pyiceberg-core` wheels are a convenience artifact. We build them and upload them to PyPI as part of every release.

The Rust crates that `pyiceberg-core` depends on are statically linked into each wheel. So the `LICENSE` and `NOTICE` inside a wheel must cover those crates as well as iceberg-rust itself.

The sdist only contains iceberg-rust's own source, so it ships the root `LICENSE` and `NOTICE` unchanged.

## How it works

`LICENSE` and `NOTICE` are generated at build time, before `maturin` builds and packages each wheel.

The [`generate-python-license`](../../.github/actions/generate-python-license/action.yml) action regenerates the wheel's `LICENSE` and `NOTICE` in `bindings/python/`:

- `LICENSE`: the root `LICENSE`, then the [`cargo-about`](https://github.com/EmbarkStudios/cargo-about) output, then the manual entries in `third-party-licenses.txt`.
- `NOTICE`: the root `NOTICE`, then the manual entries in `third-party-notices.txt`.

The action is used in these workflows:

- [`release_python.yml`](../../.github/workflows/release_python.yml): the release wheels published to PyPI.
- [`release_python_nightly.yml`](../../.github/workflows/release_python_nightly.yml): the nightly wheels published to TestPyPI.
- [`bindings_python_ci.yml`](../../.github/workflows/bindings_python_ci.yml): pull requests. It only checks that generation doesn't fail. The generated files are not published.

Note that in the repository, `bindings/python/LICENSE` and `NOTICE` are symlinks to the root files, so builds outside the release workflows ship the plain root files.

## Relevant files

- `about.hbs`: the `cargo-about` output template. Keep it a plain template.
- `about.toml`: configures `cargo-about`.
  - `accepted`: the licenses we bundle, matching `deny.toml`. Add new ones at the end, as the order is the preference when a crate offers a choice.
  - `targets`: the wheel targets. Keep in sync with the build matrices in `release_python.yml` and `release_python_nightly.yml`.
  - Clarifications, for crates whose license files `cargo-about` can't read (such as `libm`). They pin the file by checksum (`shasum -a 256 <file>`). See the [`cargo-about` docs](https://embarkstudios.github.io/cargo-about/cli/clarify.html).
- `third-party-licenses.txt`: otherwise, add the license text here. For example, `brotli-decompressor`'s MIT license, which the crate doesn't ship. It names the crate and version, so re-check it when the crate is upgraded.
- `third-party-notices.txt`: the `NOTICE` entries of dependencies that ship one (Arrow, Avro, moka). Find them from `bindings/python` (the repository's own crates appear too):

  ```shell
  cargo about generate -c about.toml --locked --fail --manifest-path Cargo.toml --format json \
    | jq -r '.crates[].package.manifest_path' | xargs -n1 dirname \
    | xargs -I{} find {} -maxdepth 1 -iname 'NOTICE*'
  ```

## Keeping entries up to date

Pull request CI checks `about.toml`. It fails on a license that isn't in `accepted`, on a `cargo about` warning such as a stale clarification, and on `<year>` in the output, which means no license file was found. To reproduce a failure, run this from `bindings/python`:

```shell
make -s cargo-about | grep -n '<year>'
```

CI doesn't check the other entries. Before each release, from `bindings/python`, check that:

- `targets` in `about.toml` matches the wheel targets in `release_python.yml` and `release_python_nightly.yml`.
- Each crate in `third-party-licenses.txt` is in the `make -s cargo-about` output at the version the entry names. If the version changed, re-check the entry against the new version. If the crate is gone, remove the entry.
- `third-party-notices.txt` matches the `NOTICE` files of the dependencies, found with the command in [Relevant files](#relevant-files).
