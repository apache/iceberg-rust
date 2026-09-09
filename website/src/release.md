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
- `iceberg_minor_release_branch`: the release branch for the minor version, like `0.9.x`.
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

This section is the requirements for committers or PMC members who are new to the role of release manager.

</div>

Refer to [Setup GPG Key](reference/setup_gpg.md) to make sure the GPG key has been set up.
The RC creation script requires a local GPG secret key when artifact signing or tag creation is enabled.

Install the release tooling used by the local scripts:

- `cargo-deny`
- `docker`
- `gpg`
- `svn`

The local release helpers are under `dev/release/`. They log every step before it runs and after it succeeds. If a step fails, the script prints the failed step and stops.

## How to propose a new release

Ultimately, it is up to the community on when to cut a new release.
You can propose this via the [Apache Iceberg dev mailing list](https://lists.apache.org/list.html?dev@iceberg.apache.org)
or simply open a tracking issue, mentioned later in this guide.

## Start a tracking issue about the next release

Start a tracking issue on GitHub for the upcoming release to track all the changes needed to be merged/addressed for the release.

If you are acting as release manager, you should own this issue and coordinate with the community when to target a new release and what changes need to be included.
You do not need to be a committer to create and own the tracking issue.

### Template

You may use the template below to create the issue.

Title:

```
Tracking issues of Iceberg Rust ${iceberg_version} Release
```

Content:

```markdown
This issue is used to track tasks of the iceberg rust ${iceberg_version} release.

### Blockers

> Blockers are the tasks, such as bugs, that should be completed before the release.

- TBD

### Community-desired features / changes

> These are features or other changes that the community would prefer to go into the next release,
> but should not block it.

- TBD

### Release Guide

For details on how to run a release, please refer to: https://rust.iceberg.apache.org/release
```

## GitHub Side

The following steps should be followed once the release is ready to begin.

### Create a minor version release branch, if it doesn't already exist

A committer must ensure that a release branch exists for each minor version.

If the release is for an existing minor version (such as the `.1` patch release in `v0.10.1`), the release branch should already exist with the name `v0.10.x`.

If the release is for a new minor version (such as the `.10` release in `v0.10.0`), you should create the release branch now.

```
git switch -c ${iceberg_minor_release_branch} && git push upstream ${iceberg_minor_release_branch}
```

### Draft GitHub release

- [Draft a new GitHub Release using the GitHub web UI](https://github.com/apache/iceberg-rust/releases/new).
- Enter the git tag of this release version, of the form `v0.y.z`. For example, `v0.9.0`.
  It is correct that the tag does not exist at this stage, as it will be created later in the release process.
- Use the minor version branch created earlier as the branch target. For example, `v0.9.x`.
- Save the draft.

### Update crate versions, dependencies list, and changelog

The following changes can be made in one pull request against the minor version branch (e.g. `v0.9.x`).

#### Bump crate versions

Bump all components' version in the project to the new Iceberg Rust version.
This version is the final version, not the release candidate version.

- Rust core and Python binding: bump version in root `Cargo.toml` under `[workspace.package]`.

If you are preparing changes for later release candidates (2+), bumping crate versions should not be necessary.

#### Update CHANGELOG.md

Update `CHANGELOG.md` based on the changes since the previous version.
You may use generative AI to assist making this update, but please review the proposed changes for correctness.
The changelog should reflect a summary of each commit in the new release.

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

Open a pull request with all the changes.
The release branch should be used as the base for the pull request.

### Create release candidate tag and artifacts

This step must be completed by a committer.

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

### Trigger release candidate PyPI publish

Python packages based on the release candidate are published to PyPI.
These are published by a workflow that must be triggered manually.

Trigger this now using the following GitHub CLI command, or the equivalent on the GitHub website.
It must use the published release tag as the reference for the workflow run.

```shell
gh workflow run --repo apache/iceberg-rust release_python.yml -f release_tag=${rc_tag} --ref refs/tags/${rc_tag}
```

### Draft an Apache Iceberg blog post

The [Apache Iceberg blog](https://iceberg.apache.org/blog/) is one mechanism to share news about the project,
however it is not a strict requirement for any release.
Blog posts are published on the website, and shared via the project's social media.

A blog post is typically published covering the whole minor release (i.e. `v0.9`), and communicates the changes since the last minor version.
It may be updated for patch versions if deemed appropriate.
For example, the [0.10 blog post was updated](https://iceberg.apache.org/blog/apache-iceberg-rust-0.10.0-release/#patch-releases) to communicate why `0.10.1` was released.

If someone would like to volunteer to author this blog post, now is a good time to draft it based on the proposed changes to be released.
The blog post should be a curated summary of the biggest changes to go in.
Drafting the initial body of the post by hand is recommended, as this allows for a more authentic contribution and avoids mistakes where generative AI may summarize changes incorrectly.

Blog posts are created by opening a PR to the main Apache Iceberg repository in the [`site/docs/blog/posts` directory](https://github.com/apache/iceberg/tree/main/site/docs/blog/posts).
It should not be merged until the release has completed.

## ASF Side

All ASF-side steps must be performed by a project committer.

**If any step in the ASF release process fails and requires code changes, abandon that RC and prepare a new RC number.**

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

To learn more about Apache Iceberg Rust, please visit:
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

After at least 72 hours and 3 `+1` binding votes from Iceberg PMC members, claim the vote result.

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
  make build && make test
  ```

- Verify pyiceberg-core build and tests:

  ```bash
  (
    cd bindings/python
    make install
    make test
  )
  ```

- Verify license headers:

  ```bash
  docker run --rm -v $(pwd):/github/workspace apache/skywalking-eyes header check
  ```

  Expects: `INFO Totally checked _ files, valid: _, invalid: 0, ignored: _, fixed: 0`

## Official Release

All steps in this section must be performed by a project committer,
except the announcement e-mail which must be performed by a PMC member.

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

The release script does not push the final signed release tag.
Review the output and then manually publish the release tag.

```shell
git push origin "v${iceberg_version}"
```

The creation of the final release tag triggers the publish workflow for crates.
Python packages are manually triggered later.
Please verify that the triggered workflows for the crates succeeded.

Note, this workflow for crates is expected to fail if new crates are being published for the first time.
In this instance, a committer must manually publish the crate in order to continue.
See [publishing a crate for the first time](#publishing-a-crate-for-the-first-time) for what steps to take.
Once all the crates are published, Python publishing should start.

Python publishing is performed by a GitHub workflow, however the trigger is manual.
Trigger this now using the following GitHub CLI command, or the equivalent on the GitHub website.
It must use the published release tag as the reference for the workflow run.

```shell
gh workflow run --repo apache/iceberg-rust release_python.yml -f release_tag=v${iceberg_version} --ref refs/tags/v${iceberg_version}
```

Verify that the workflow succeeds, indicating that the Python packages are released.
If there is any issue, you may continue with the release however please note in any relevant steps that the Python package publish failed and open a patch release tracking issue to plan addressing the problem.
Note, this should be an exceptional case.

### Publish the GitHub Release

A GitHub release should have been drafted earlier in the release process.

Open the GitHub release and update the body with the contents of the changelog.
Then, publish the GitHub release.

### Send the announcement

Send the release announcement to `dev@iceberg.apache.org` and CC `announce@apache.org`.

You must be a PMC member to send e-mails to `announce@apache.org`.
The e-mail must be plain text.
Disable HTML formatting if your e-mail client enables it by default.

Instead of adding breaking changes, we include the new features as "notable changes" in the announcement.

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

### Publish the release blog post

If a release blog post has been drafted (introduced earlier in this guide), now is the time to ensure it is ready to merge and publish.
It does not need to be published immediately for the release to be considered complete,
however it is in the community's best interest to publish it soon after.

## Appendix

### Publishing a crate for the first time

Publishing the Iceberg crates is automated using GitHub Actions,
which authenticates with crates.io using trusted publishing.

Trusted publishing must first be configured for each crate.
If the crate has never been published, crates.io rejects attempts to publish.

When a release workflow fails due to the presence of a new crate, a committer must perform steps enumerated below.
Once complete, future versions can be published automatically using GitHub Actions.

#### Initial crate publish

Manually publish the crate using the following command.
You **must** have the source code checked out matching the pushed Git tag for the release.

```shell
cargo publish --package <package-name>
```

#### Configure crate permissions

After publishing succeeds, the crate must be configured to allow other committers to publish.

Add the GitHub team that Apache Iceberg committers are a member of.

```shell
cargo owner --add github:<github-team-org>:<github-team-name>
```

Additionally, add two PMC members (excluding yourself) as owners of the crate.
A GitHub team cannot manage permissions for the crate, so it is important that individuals have ownership to continue being able to manage the crate should a PMC member become inactive.
See the [Cargo documentation for `cargo owner`](https://doc.rust-lang.org/cargo/reference/publishing.html#cargo-owner) for reference.

```shell
cargo owner --add <github-handle>
```

#### Configure trusted publishing

Review the [crates.io trusted publishing documentation](https://crates.io/docs/trusted-publishing) for the latest instructions on how to configure it.
You should also review another already-existing crate for reference.
