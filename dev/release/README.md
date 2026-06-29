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

# Release Helpers

These scripts support local Apache Iceberg Rust release candidate creation and verification.

## Create an RC

```shell
dev/release/create_rc.sh 0.9.1 2
```

This creates:

- source archive `dist/apache-iceberg-rust-0.9.1-rc2/apache-iceberg-rust-0.9.1.tar.gz`
- detached signature `.asc`
- SHA-512 checksum `.sha512`
- signed annotated tag `v0.9.1-rc.2`

The license header check runs against the generated source archive, not the live Git worktree. The optional SVN upload runs after local artifact verification and before RC tag creation. The signed RC tag is created as the final release step, then the script prints a draft VOTE email for `dev@iceberg.apache.org`. The script logs every step before it runs and after it succeeds. If a step fails, it prints the failed step and stops.

Common options:

```shell
dev/release/create_rc.sh 0.9.1 2 --release_ref <commit-ish>
dev/release/create_rc.sh 0.9.1 2 --create_rc_tag 0 --sign 0
dev/release/create_rc.sh 0.9.1 2 --upload_svn 1
dev/release/create_rc.sh 0.9.1 2 --check_headers 0 --check_deps 0
```

Defaults:

- `--release_ref HEAD`: git commit-ish to archive and tag.
- `--dist_dir dist`: artifact output root.
- `--create_rc_tag 1`: create the signed annotated RC tag as the final release step.
- `--check_headers 1`: check Apache license headers against the source archive.
- `--check_deps 1`: run dependency license checks before artifact creation.
- `--sign 1`: create and verify the detached GPG signature.
- `--upload_svn 0`: upload RC artifacts to the ASF dev dist SVN repository.
- `--svn_dist_url https://dist.apache.org/repos/dist/dev/iceberg`: SVN directory URL where the RC artifact directory will be uploaded.

`--sign 1` or `--create_rc_tag 1` requires a local GPG secret key. See the website's GPG setup guide before creating a real release candidate.

`--check_deps 1` requires `cargo-deny`. Install it with:

```shell
cargo install cargo-deny
```

## Verify an RC

```shell
dev/release/verify_rc.sh 0.9.1 2
```

By default this downloads from `https://dist.apache.org/repos/dist/dev/iceberg/apache-iceberg-rust-0.9.1-rc2/`, verifies checksum and signature with the local GPG keyring, checks source headers, and runs Rust and Python build/tests.

To verify artifacts already created under local `dist/`:

```shell
dev/release/verify_rc.sh 0.9.1 2 --download 0
```

Common options:

```shell
dev/release/verify_rc.sh 0.9.1 2 --verify_signature 0
dev/release/verify_rc.sh 0.9.1 2 --import_gpg_keys 1
dev/release/verify_rc.sh 0.9.1 2 --build 0 --python 0 --check_headers 0
dev/release/verify_rc.sh 0.9.1 2 --python_bin /opt/homebrew/bin/python3.11
```

Defaults:

- `--dist_dir dist`: local artifact root used when `--download 0`.
- `--download 1`: download artifacts from ASF dev dist.
- `--verify_signature 1`: verify the `.asc` signature with the local GPG keyring.
- `--import_gpg_keys 0`: download and import Apache Iceberg release keys before signature verification.
- `--check_headers 1`: check Apache license headers against the extracted source archive.
- `--build 1`: build and test the Rust source distribution.
- `--python 1`: build and test pyiceberg-core.
- `--python_bin <unset>`: set `PYO3_PYTHON` and `UV_PYTHON` for Python-linked Rust and pyiceberg-core checks.
- `--tmp_dir <auto>`: verification sandbox; auto-created and deleted on success when omitted.

## Promote an RC to a Release

After the VOTE passes, convert the approved RC to the official release:

```shell
dev/release/release.sh 0.9.1 2
```

This creates the signed annotated final release tag `v0.9.1` from the RC tag `v0.9.1-rc.2`, then moves the ASF SVN artifacts from `dev/iceberg/apache-iceberg-rust-0.9.1-rc2` to `release/iceberg/apache-iceberg-rust-0.9.1`.

Common options:

```shell
dev/release/release.sh 0.9.1 2 --create_release_tag 0
dev/release/release.sh 0.9.1 2 --move_svn 0
```

Defaults:

- `--create_release_tag 1`: create the signed annotated final release git tag.
- `--move_svn 1`: move the RC artifacts from ASF dev dist to ASF release dist.
- `--tag_ref <rc tag commit>`: git commit-ish to tag as the final release.
- `--dev_dist_url https://dist.apache.org/repos/dist/dev/iceberg`: SVN directory URL containing RC artifact directories.
- `--release_dist_url https://dist.apache.org/repos/dist/release/iceberg`: SVN directory URL where final release artifact directories are published.

The script does not push the final release tag. Push it manually after reviewing the output:

```shell
git push origin "v0.9.1"
```

## Dependencies

Use the dependency helper to update or verify dependency license lists:

```shell
dev/release/dependencies.sh generate
dev/release/dependencies.sh check
```
