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

This document explains how the release process works for Apache Iceberg Rust in accordance with Apache requirements.

## Introduction

`Source Release` is the key point which Apache values, and is also necessary for an ASF release.

Please remember that publishing software has legal consequences.

This guide complements the foundation-wide policies and guides:

- [Release Policy](https://www.apache.org/legal/release-policy.html)
- [Release Distribution Policy](https://infra.apache.org/release-distribution)
- [Release Creation Process](https://infra.apache.org/release-publishing.html)

## Terminology

In this guide:

- `iceberg_version`: the final Iceberg Rust version, like `0.9.1`.
- `rc`: the numeric release candidate voting round, like `2`.
- `rc_tag`: the git tag for a release candidate, like `v0.9.1-rc.2`.
- `rc_dist_dir`: the ASF dev distribution directory, like `apache-iceberg-rust-0.9.1-rc2`.
- `source_archive`: the source tarball, like `apache-iceberg-rust-0.9.1.tar.gz`.

The RC tag includes `rc.<number>`. The ASF dev distribution directory uses `rc<number>`. The source archive name uses only the final version.

## Release Manager

The release manager is the person taking ownership of a particular Iceberg Rust release.

This is usually a committer for the Apache Iceberg project, however the role can be supported by non-committers in the early stages of coordinating a release.
Where this can be supported by a non-committer, this will be mentioned in the guide.

## Preparation

<div class="warning">

This section is the requirements for individuals who are new to the role of release manager.

</div>

Refer to [Setup GPG Key](reference/setup_gpg.md) to make sure the GPG key has been set up.
The RC creation script requires a local GPG secret key when artifact signing or tag creation is enabled.

Install the release tooling used by the local scripts:

- `cargo-deny`
- `docker`
- `gpg`
- `mise` 2026.6.14 or newer
- `svn`

The local release helpers are under `dev/release/`. They log every step before it runs and after it succeeds. If a step fails, the script prints the failed step and stops.

## Start a tracking issue about the next release

Start a tracking issue on GitHub for the upcoming release to track all tasks that need to be completed.
You should own this tracking issue and coordinate with the community when to target a new release and what changes need to be included.
You do not need to be a committer to create and own the tracking issue.

Title:

```
Tracking issues of Iceberg Rust ${iceberg_version} Release
```

Content:

```markdown
This issue is used to track tasks of the iceberg rust ${iceberg_version} release.

## Tasks

### Blockers

> Blockers are the tasks that must be completed before the release.

### Build Release

#### GitHub Side

- [ ] Draft GitHub release
- [ ] Bump version in project, update dependencies list, and update changelog
- [ ] Create and push release candidate tag

#### ASF Side

- [ ] Create ASF source release artifacts
- [ ] Upload artifacts to the SVN dist repo

### Voting

- [ ] Start VOTE at iceberg community

### Official Release

- [ ] Publish artifacts to SVN RELEASE branch
- [ ] Change Iceberg Rust Website download link
- [ ] Publish GitHub release, automatically pushing the Git tag
- [ ] Send the announcement

For details of each step, please refer to: https://rust.iceberg.apache.org/release
```

## GitHub Side

The following steps should be followed once the release is ready to begin.

### Draft GitHub release

- [Draft a new GitHub Release using the GitHub web UI](https://github.com/apache/iceberg-rust/releases/new).
- Enter the git tag of this release version, of the form `v0.y.z`. For example, `v0.9.0`.
  The tag should not exist at this stage and GitHub will offer to create it when the release is published.
- Make sure the branch target is `main` for minor release (such as `0.9.0`), or the minor version branch for a patch release (such as `0.9.1`).
- Generate the release note by clicking the `Generate release notes` button.
- Save the draft.

### Update crate versions, dependencies list, and changelog

The following changes can be made in one pull request.

#### Bump crate versions

Bump all components' version in the project to the new Iceberg Rust version.
This version is the final version, not the release candidate version.

- Rust core and Python binding: bump version in root `Cargo.toml` under `[workspace.package]`.

#### Update CHANGELOG.md

Use the content of the draft GitHub release to update `CHANGELOG.md`.
Since drafting a GitHub release requires `content: write` GitHub permissions, this step must be owned by a committer.

#### Update dependency lists

Run the following command to update the dependencies list of every package:

```shell
dev/release/dependencies.sh generate
```

Run the following command to verify the licenses meet the project's policy.

```shell
dev/release/dependencies.sh check
```

#### Open pull request

Open a pull request with all three changes.

### Create release candidate tag and artifacts

After the version bump PR gets merged, check out the exact commit to release and run:

```shell
dev/release/create_rc.sh ${iceberg_version} ${rc}
```

For example:

```shell
dev/release/create_rc.sh 0.9.1 2
```

Useful options include:

- `--release_ref HEAD`: git commit-ish to archive and tag.
- `--dist_dir dist`: artifact output root.
- `--create_rc_tag 1`: create the signed annotated RC tag as the final release step.
- `--check_headers 1`: check Apache license headers against the source archive.
- `--check_deps 1`: run dependency license checks before artifact creation.
- `--sign 1`: create and verify the detached GPG signature.
- `--upload_svn 0`: upload RC artifacts to the ASF dev dist SVN repository.
- `--svn_dist_url https://dist.apache.org/repos/dist/dev/iceberg`: SVN directory URL where the RC artifact directory will be uploaded.

This script creates:

- Local artifact directory: `dist/apache-iceberg-rust-${iceberg_version}-rc${rc}/`
- Source archive: `apache-iceberg-rust-${iceberg_version}.tar.gz`
- Signature: `apache-iceberg-rust-${iceberg_version}.tar.gz.asc`
- SHA-512 checksum: `apache-iceberg-rust-${iceberg_version}.tar.gz.sha512`
- Signed annotated RC tag: `v${iceberg_version}-rc.${rc}`

The script checks license headers against the generated source archive, not the live Git worktree. If enabled, SVN upload runs after local artifact verification and before RC tag creation. The script creates the signed RC tag as the final release step, then prints a draft VOTE email for `dev@iceberg.apache.org`.

To upload artifacts to ASF dev dist as part of RC creation, pass:

```shell
dev/release/create_rc.sh ${iceberg_version} ${rc} --upload_svn 1
```

The script does not push the RC tag. Review the output, then push the tag manually:

```shell
git push origin "v${iceberg_version}-rc.${rc}"
```

If an RC has a problem, abandon that RC and increment the RC number.

## ASF Side

If any step in the ASF release process fails and requires code changes, abandon that RC and prepare a new RC number.
Our release page displays ASF releases instead of GitHub Releases.

### Verify the release candidate locally

Before uploading artifacts to ASF dev dist, verify the local artifacts:

```shell
dev/release/verify_rc.sh ${iceberg_version} ${rc} --download 0
```

To skip expensive build steps during a quick local check:

```shell
dev/release/verify_rc.sh ${iceberg_version} ${rc} --download 0 --build 0 --python 0
```

### Upload artifacts to the SVN dist repo

SVN is required for this step.

The SVN repository of the dev branch is: <https://dist.apache.org/repos/dist/dev/iceberg/>

First, check out Iceberg to a local directory:

```shell
svn co https://dist.apache.org/repos/dist/dev/iceberg/ /tmp/iceberg-dist-dev
```

If the artifacts were not uploaded by `dev/release/create_rc.sh --upload_svn 1`, upload them manually:

```shell
rc_dist_dir="apache-iceberg-rust-${iceberg_version}-rc${rc}"

mkdir "/tmp/iceberg-dist-dev/${rc_dist_dir}/"
cp "./dist/${rc_dist_dir}/"* "/tmp/iceberg-dist-dev/${rc_dist_dir}/"

cd /tmp/iceberg-dist-dev/
svn status
svn add "${rc_dist_dir}"
svn commit -m "Prepare Apache Iceberg Rust ${iceberg_version} RC${rc}"
```

Visit <https://dist.apache.org/repos/dist/dev/iceberg/> to make sure the artifacts are uploaded correctly.

### Verify the uploaded release candidate

After uploading the artifacts, verify them from ASF dev dist:

```shell
dev/release/verify_rc.sh ${iceberg_version} ${rc}
```

### Rescue

If you accidentally publish wrong or unexpected artifacts, like wrong signature files or checksum files, cancel the current RC, increment the RC number, and initiate a new release candidate.
Remember to delete the wrong artifacts from the SVN dist repo.

## Voting

Send the Iceberg community VOTE email to <dev@iceberg.apache.org>.

Title:

```
[VOTE] Release Apache Iceberg Rust ${iceberg_version} RC${rc}
```

Content:

```
Hello Apache Iceberg Rust Community,

This is a call for a vote to release Apache Iceberg Rust version ${iceberg_version}.

The tag to be voted on is: v${iceberg_version}-rc.${rc}.

The release candidate:

https://dist.apache.org/repos/dist/dev/iceberg/apache-iceberg-rust-${iceberg_version}-rc${rc}/

Keys to verify the release candidate:

https://downloads.apache.org/iceberg/KEYS

Git tag for the release:

https://github.com/apache/iceberg-rust/releases/tag/v${iceberg_version}-rc.${rc}

Please download, verify, and test the release candidate.

This vote will be open for at least 72 hours and will remain open until the required number of votes is reached.

Please vote accordingly:
[ ] +1 Approve
[ ] +0 No opinion
[ ] -1 Disapprove (please provide a reason)

To learn more about Apache Iceberg, please visit:
https://rust.iceberg.apache.org/

Checklist for reference:
[ ] Download links are valid
[ ] Checksums and signatures are correct
[ ] LICENSE and NOTICE files are present
[ ] No unexpected binary files are included
[ ] All source files have ASF headers
[ ] The project builds successfully from source
[ ] pyiceberg-core builds and tests successfully

For more details, please refer to:
https://rust.iceberg.apache.org/release.html#how-to-verify-a-release

Thanks,
${name}
```

Example: <https://lists.apache.org/thread/c211gqq2yl15jbxqk4rcnq1bdqltjm5l>

After at least 3 `+1` binding votes from Iceberg PMC members, claim the vote result.

Title:

```
[RESULT][VOTE] Release Apache Iceberg Rust ${iceberg_version} RC${rc}
```

Content:

```
Hello Apache Iceberg Rust Community,

The vote to release Apache Iceberg Rust ${iceberg_version} RC${rc} has passed.

The vote PASSED with 3 +1 binding and 1 +1 non-binding votes, no +0 or -1 votes:

Binding votes:

- xxx
- yyy
- zzz

Non-Binding votes:

- aaa

Vote thread: ${vote_thread_url}

Thanks,
${name}
```

Example: <https://lists.apache.org/thread/xk5myl10mztcfotn59oo59s4ckvojds6>

## How to verify a release

### Validate with the helper script

Run:

```shell
dev/release/verify_rc.sh ${iceberg_version} ${rc}
```

The helper downloads the source archive, signature, and checksum from ASF dev dist, verifies the signature with the local GPG keyring, verifies the checksum, extracts the archive, checks source headers, and runs Rust and Python build/tests.

To import Apache Iceberg release keys before signature verification, run:

```shell
dev/release/verify_rc.sh ${iceberg_version} ${rc} --import_gpg_keys 1
```

### Validate manually

A release candidate contains links to following things:

- A source tarball
- A signature (`.asc`)
- A checksum (`.sha512`)

After downloading them, here are the instructions on how to verify them.

- Import keys:

  ```bash
  curl https://downloads.apache.org/iceberg/KEYS -o KEYS
  gpg --import KEYS
  ```

- Verify the `.asc` file:

  ```bash
  gpg --verify apache-iceberg-rust-*.tar.gz.asc
  ```

  Expects: `gpg: Good signature from ...`

- Verify the checksum:

  ```bash
  shasum -a 512 -c apache-iceberg-rust-*.tar.gz.sha512
  ```

  Expects: `"apache-iceberg-rust-...tar.gz: OK"`

- Verify build and test:

  ```bash
  tar -xzf apache-iceberg-rust-*.tar.gz
  cd apache-iceberg-rust-*/
  # Review mise.toml before trusting the release candidate.
  mise trust
  mise install
  mise run build
  mise run test
  ```

- Verify pyiceberg-core build and tests:

  ```bash
  mise run python:install
  mise run python:test
  ```

- Verify license headers:

  ```bash
  docker run --rm -v $(pwd):/github/workspace apache/skywalking-eyes header check
  ```

  Expects: `INFO Totally checked _ files, valid: _, invalid: 0, ignored: _, fixed: 0`

## Official Release

### Promote the RC

After the VOTE passes, create the final release tag and move the ASF artifacts from dev dist to release dist:

```shell
dev/release/release.sh ${iceberg_version} ${rc}
```

Useful options include:

- `--create_release_tag 1`: create the signed annotated final release git tag.
- `--move_svn 1`: move the RC artifacts from ASF dev dist to ASF release dist.
- `--tag_ref <rc tag commit>`: git commit-ish to tag as the final release.
- `--dev_dist_url https://dist.apache.org/repos/dist/dev/iceberg`: SVN directory URL containing RC artifact directories.
- `--release_dist_url https://dist.apache.org/repos/dist/release/iceberg`: SVN directory URL where final release artifact directories are published.

The release script does not push the final release tag.
Review the output and then move on to the next step to publish the GitHub release and tag.

### Publish the GitHub Release

A GitHub release should have been drafted earlier in the release process.
Open the release and publish it now.

On publish, the Git tag will be created for the release.

The creation of the final release tag triggers the publish workflow for crates and pyiceberg-core.

### Send the announcement

Send the release announcement to `dev@iceberg.apache.org` and CC `announce@apache.org`.

Instead of adding breaking changes, include the new features as "notable changes" in the announcement.

Title:

```
[ANNOUNCE] Release Apache Iceberg Rust ${iceberg_version}
```

Content:

```
Hi all,

The Apache Iceberg Rust community is pleased to announce
that Apache Iceberg Rust ${iceberg_version} has been released!

Iceberg is a data access layer that allows users to easily and efficiently
retrieve data from various storage services in a unified way.

The notable changes since the previous release include:
1. xxxxx
2. yyyyyy
3. zzzzzz

Please refer to the change log for the complete list of changes:
https://github.com/apache/iceberg-rust/releases/tag/v${iceberg_version}

Apache Iceberg Rust website: https://rust.iceberg.apache.org/

Download Links: https://rust.iceberg.apache.org/download

From official ASF distribution: https://dist.apache.org/repos/dist/release/iceberg/apache-iceberg-rust-${iceberg_version}/

Iceberg Resources:
- Issue: https://github.com/apache/iceberg-rust/issues
- Mailing list: dev@iceberg.apache.org

Thanks
On behalf of Apache Iceberg Community
```

Example: <https://lists.apache.org/thread/oy77n55brvk72tnlb2bjzfs9nz3cfd0s>
