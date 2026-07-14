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

# Contributing

First, thank you for contributing to Iceberg Rust! The goal of this document is to provide everything you need to start contributing to iceberg-rust. The following TOC is sorted progressively, starting with the basics and expanding into more specifics.

## Your First Contribution

1. [Fork the iceberg-rust repository](https://github.com/apache/iceberg-rust/fork) into your own GitHub account.
1. [Create a new Git branch](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-and-deleting-branches-within-your-repository).
1. Make your changes.
1. [Submit the branch as a pull request](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork) to the main iceberg-rust repo. An iceberg-rust team member should comment and/or review your pull request within a few days. Although, depending on the circumstances, it may take longer.

## Workflow

### Git Branches

*All* changes must be made in a branch and submitted as [pull requests](#github-pull-requests). iceberg-rust does not adopt any type of branch naming style, but please use something descriptive of your changes.

### GitHub Pull Requests

Once your changes are ready you must submit your branch as a [pull request](https://github.com/apache/iceberg-rust/pulls).

#### Title

The pull request title must follow the format outlined in the [conventional commits spec](https://www.conventionalcommits.org). [Conventional commits](https://www.conventionalcommits.org) is a standardized format for commit messages. iceberg-rust only requires this format for commits on the `main` branch. And because iceberg-rust squashes commits before merging branches, this means that only the pull request title must conform to this format.

The following are all good examples of pull request titles:

```text
feat(schema): Add last_updated_ms in schema
docs: add hdfs classpath related troubleshoot
ci: Mark job as skipped if owner is not apache
fix(schema): Ignore prefix if it's empty
refactor: Polish the implementation of read parquet
```

#### Reviews & Approvals

All pull requests should be reviewed by at least one iceberg-rust committer.

#### Merge Style

All pull requests are squash merged. We generally discourage large pull requests that are over 300-500 lines of diff. If you would like to propose a change that is larger we suggest coming onto [Iceberg's DEV mailing list](mailto:dev@iceberg.apache.org) or [Slack #rust Channel](https://join.slack.com/t/apache-iceberg/shared_invite/zt-1zbov3k6e-KtJfoaxp97YfX6dPz1Bk7A) and discuss it with us. This way we can talk through the solution and discuss if a change that large is even needed! This will produce a quicker response to the change and likely produce code that aligns better with our process.

When a pull request is under review, please avoid using force push as it makes it difficult for reviewer to track changes. If you need to keep the branch up to date with the main branch, consider using `git merge` instead. 

### CI

iceberg-rust uses GitHub Actions to orchestrate CI. Developer-facing build, lint, and test checks use the same mise tasks contributors run locally, while GitHub-only security analysis, artifact handling, deployment, and publishing remain workflow actions.

## Setup

For small or first-time contributions, we recommend the dev container method. Prefer to set up the tools on your host? That's fine too.

The project uses [mise](https://mise.en.dev/) to keep development tool versions and task definitions in one place. The pinned environment makes local and CI runs consistent, installs Rust, Python, and task-specific utilities automatically, and selects a Python distribution flavor that provides the shared `libpython` needed to compile the Python/DataFusion bindings.

This does add mise as a bootstrap dependency, and the first installation requires network access and can take some time. The repository configuration must also be reviewed and trusted before mise will use it. mise does not install system applications such as Docker, Podman, or GPG.

### Using a dev container environment

iceberg-rust provides a pre-configured [dev container](https://containers.dev/) that can be used in [GitHub Codespaces](https://github.com/features/codespaces), [VS Code](https://code.visualstudio.com/), [JetBrains](https://www.jetbrains.com/remote-development/gateway/), or [JupyterLab](https://jupyterlab.readthedocs.io/en/stable/). The container installs mise, trusts this repository's configuration, and installs the pinned development tools automatically.

The fastest way is:

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/apache/iceberg-rust?quickstart=1&machine=standardLinux32gb)

### Bring your own toolbox

#### Install mise and project tools

Install mise 2026.6.14 or newer by following the [official installation guide](https://mise.en.dev/getting-started.html). Clone the repository, change to its root directory, and review the root and relevant subproject `mise.toml` files before trusting them. Trusting the monorepo root also trusts its listed subprojects. Then install the pinned tools:

```bash
mise trust
mise doctor
mise install
mise run build
```

Shell activation is optional. `mise run ...` executes project tasks with the configured tools, and `mise exec -- cargo ...` can be used for ad hoc commands. Use `mise current` to inspect the selected versions.

#### Python toolchain

Python is a workspace build dependency because the workspace contains the Python bindings and their DataFusion integration. The mise-managed Python uses a pinned distribution flavor with the shared library PyO3 needs on Linux, macOS, and Windows. Python tasks explicitly pass the selected interpreter to uv, so uv will not silently select or download a different Python.

#### Install Docker or Podman

iceberg-rust uses containers to set up services for integration tests. Install Docker or Podman separately and see [Container Runtimes](website/src/reference/container-runtimes.md) for setup instructions.

## Build

Run all commands from the repository root. `mise tasks --all` lists tasks from the root and its subprojects.

| Task | Purpose |
| --- | --- |
| `mise run build` | Compile all workspace targets and features. |
| `mise run lint` | Run the complete GitHub Actions lint job locally. |
| `mise run check` | Run the lint job followed by clippy. |
| `mise run check-standalone` | Check every workspace crate without workspace feature unification. |
| `mise run build-no-default-features` | Build the core crate without default features. |
| `mise run check-msrv` | Check the workspace with the minimum supported Rust version. |
| `mise run check-public-api` | Compare the public API against the checked-in snapshots. |
| `mise run check-audit` | Audit Rust dependencies against the RustSec advisory database. |
| `mise run unit-test` | Run Rust unit and documentation tests. |
| `mise run nextest` | Run the Rust test suite with cargo-nextest. |
| `mise run test` | Start the integration-test services, run all tests, and tear the services down. |
| `mise run docker-up` | Start the integration-test services without running tests. |
| `mise run docker-down` | Stop and remove the integration-test services. |
| `mise run docker-logs` | Follow logs from the integration-test services. |

### Python bindings

The Python tasks are defined in `bindings/python/mise.toml` and share the repository's managed tool versions. From the repository root, run:

```shell
mise run //bindings/python:install
mise run //bindings/python:build
mise run //bindings/python:check-format
mise run //bindings/python:check-style
mise run //bindings/python:test
mise run //bindings/python:test-wheel
```

Inside `bindings/python`, the shorter form is `mise run :test`, `mise run :test-wheel`, and so on. The `test-wheel` task mirrors the native-wheel build, installation, and test sequence used by the Python CI matrix. Cross-compiled release wheels remain a release-workflow responsibility.

### Website

Preview the documentation site locally with `mise run site`. Use `mise run site-build` to reproduce the complete CI artifact, including Rust API documentation.

### Troubleshooting mise

- Run `mise doctor` to check the installation and `mise current` to confirm the tools selected for this repository.
- If GitHub API rate limits interrupt installation, set `GITHUB_TOKEN` to a GitHub personal access token and retry `mise install`. Verification should remain enabled.
- If Cargo or PyO3 reports a missing `libpython`, confirm that `mise current python` selects the configured interpreter and reinstall it with `mise install --force python`. Do not substitute a uv-managed or system interpreter.

## Dependencies

`Cargo.lock` is committed, and regularly updated by dependabot to make sure the latest dependency versions are
tested in CI and developers have reproducible builds.

In `Cargo.toml`, we specify the minimum version required to use iceberg-rust. This allows users to choose their
dependency versions without always upgrading to the latest.

## Code of Conduct

We expect all community members to follow our [Code of Conduct](https://www.apache.org/foundation/policies/conduct.html).
