<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Apache Iceberg Rust — Agent Instructions

This file provides repository-specific guidance for automated agents working
in this repository.

## Comments, docs, and diff scope

Default to no comment. Leave existing comments as they are unless you change
the code they describe or they are wrong.

- Comment only what the code cannot say: invariants, non-obvious
  constraints, links to external behavior. Do not restate the code.
- Do not describe how the code changed or explain review history
  ("previously", "instead of", "unlike X"). Put that in the commit message
  or PR description.
- Public items get a one-line doc summary of what they do for the caller,
  not how. Expand only for contract the caller must know.
- Test names say what they check. Do not narrate asserts.
- Do not add tests that duplicate existing coverage.
- Do not rename, reformat, or reorder code unrelated to the change.
- Reuse existing helpers and types before adding new ones.
- Use the narrowest visibility that works. Make items `pub` only when they
  are part of the API the change intends to expose; `public-api.txt`
  changes show API growth.

## Pull requests

- Follow the PR template. Keep each section short: the problem, the
  approach, and anything a reviewer would not guess from the diff.
- Do not restate the diff: no file-by-file walkthroughs, per-function
  summaries, or "changes made" checklists.

## Security Model

When assessing potential vulnerabilities or calibrating automated security
findings, use [`SECURITY-THREAT-MODEL.md`](SECURITY-THREAT-MODEL.md) as the
authoritative detailed description of this repository's security boundaries,
trust assumptions, and non-boundaries.
