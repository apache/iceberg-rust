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

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

This is an **owned fork of [Apache Iceberg™ Rust](https://github.com/apache/iceberg-rust)** — the Rust
implementation of the [Apache Iceberg](https://iceberg.apache.org/) open table format. We maintain it to
reach **1:1 capability parity with the Java `iceberg-core` / `iceberg-api` library** (the engine-agnostic
table-format core, **not** the Spark engine surface). Upstream `apache/iceberg-rust` is a **sync baseline
we cherry-pick from, not a mergeability constraint** — we diverge freely in service of parity. The
deliverable is a **Rust-native library**; Python / PySpark is deferred (there is no Python layer in this
repo). **Glue + S3 Tables** are the first-priority catalogs.

The authoritative plan is **[Roadmap.md](Roadmap.md)** (phase plan + sequencing); the living capability
checklist is **[docs/parity/GAP_MATRIX.md](docs/parity/GAP_MATRIX.md)**. When this file and the Roadmap
disagree on direction, the **Roadmap wins**; when the Roadmap and the GAP_MATRIX disagree on a
capability's *status*, the **GAP_MATRIX** (re-audited against the live base) wins.

> **A note on the XML tags in this file.** A few sections are wrapped in semantic tags
> (`<read_order>`, `<map_md_navigation>`, `<subagent_policy>`). They mark the load-bearing
> "do this / don't skip this" regions so an agent can locate them unambiguously; they carry no
> meaning beyond "read this bounded region as a unit." Reference sections (snapshot, architecture,
> build/test, repo layout) are intentionally left untagged.

<read_order>

## Read order (every session)

1. **This file (CLAUDE.md)** — repository intent, prohibitions, and the navigation contract.
2. **[Roadmap.md](Roadmap.md)** — the parity phase plan and the current phase; then
   **[docs/parity/GAP_MATRIX.md](docs/parity/GAP_MATRIX.md)** for per-capability status.
3. **The operating manual for your model tier** — [skills/Fable.md](skills/Fable.md) (Mythos-class),
   [skills/Opus.md](skills/Opus.md), [skills/Sonnet.md](skills/Sonnet.md), or
   [skills/Haiku.md](skills/Haiku.md) (the portable engineering contract; see
   [skills/map.md](skills/map.md)). CLAUDE.md wins on any conflict.
4. **[task/lessons.md](task/lessons.md) in full, then [task/todo.md](task/todo.md)** — accumulated
   lessons and any mid-flight plan to pick up.
5. **The `map.md` of every directory your task will touch** (where present — see the navigation
   rule below).
6. **Upstream docs when you need depth:** [README.md](README.md), [CONTRIBUTING.md](CONTRIBUTING.md),
   the per-crate `README.md` files, and the [Iceberg Rust site](https://rust.iceberg.apache.org/).

</read_order>

## Parity mandate

The north star is behavioral 1:1 parity with Java `iceberg-core` / `iceberg-api`. Concretely:

- **The Java repo is the spec-by-example.** Keep a reference checkout of `apache/iceberg` and re-crawl on
  each Java release. A capability is "done" only when the Rust API matches the Java contract's behavior.
- **Tests land with the code, in the same change.** Behavior added without tests is a hard block.
- **Interop is the only true 1:1 evidence.** Where applicable, prove byte-level round-trips: read tables
  Java wrote, and prove Java reads what we write. A GAP_MATRIX row flips to ✅ only with unit tests **and**
  an interop test.
- **Re-audit the GAP_MATRIX after every upstream sync and every phase**, and date-stamp the provenance.
- **Order by dependency, then value:** metadata correctness underpins writes; writes underpin maintenance.

<map_md_navigation>

## `map.md` navigation — a convention this fork adopts

This fork uses a guiding-agent navigation pattern: a directory may carry a single `map.md`
documenting what lives there and where to go next. **It is opt-in and incremental** — upstream
Iceberg Rust does not ship `map.md` files, so coverage grows as you work, not all at once.

Each `map.md` has two parts in one file:

- **The map** (top) — `Purpose`, `Contents`, an `I want to... → go to` intent table, and
  `Pointers` (Up / Related) to neighboring directories.
- **`## Debug`** (bottom) — `Known failure modes` table, `First checks`, and `Escalate to` pointers.

**The contract:**

- **Before reading or editing a file in a directory that has a `map.md`,** open the `map.md` first
  and use it to navigate. The maps are the index; the code is the truth.
- **If the code and a `map.md` disagree, the code wins** — the `map.md` is stale.
- **When your change makes a directory's `map.md` inaccurate, update it in the same change**
  (always in scope, even though §6 of the manuals otherwise forbids touching unplanned files).
- **When you create a new source directory, add its `map.md` in the same change** — but only if the
  surrounding tree already uses the convention. Do not litter `map.md` files across pristine
  upstream directories you are only reading.

</map_md_navigation>

## Project snapshot

Apache Iceberg Rust implements the **Iceberg table format spec** in Rust: reading and writing table
metadata and data, expression/predicate handling, partition transforms, snapshot and schema
evolution, and pluggable catalogs and object storage. It is a **library workspace**, not an
application — most code is library crates consumed by downstream projects. **Rust** edition 2024, MSRV
**1.92** (see [Cargo.toml](Cargo.toml) `rust-version`). Base synced to upstream **0.9.1** (datafusion
52.2 / arrow 57.1 / parquet 57.1).

## Big-picture architecture

### The workspace crates

| Crate | Path | Role |
|---|---|---|
| **iceberg** | [crates/iceberg/](crates/iceberg/) | The core: spec types, catalog trait, table scans, transactions, writers, Arrow/Avro/Parquet IO, expressions, partition transforms, metadata inspection, Puffin, deletion vectors. |
| **iceberg-datafusion** | [crates/integrations/datafusion/](crates/integrations/datafusion/) | DataFusion integration — `TableProvider` / `CatalogProvider` / physical plans so Iceberg tables are queryable from DataFusion SQL. |
| **catalog/{rest,hms,glue,s3tables,sql}** | [crates/catalog/](crates/catalog/) | Concrete `Catalog` implementations: REST, Hive Metastore, AWS Glue, S3 Tables, and SQL-backed. **Glue + S3 Tables are the parity priority.** |
| **catalog/loader** | [crates/catalog/loader/](crates/catalog/loader/) | Config-driven catalog construction (pick a catalog impl at runtime). |
| **storage/opendal** | [crates/storage/opendal/](crates/storage/opendal/) | OpenDAL-backed FileIO storage (extracted from the core in the 0.8/0.9 cycle). |
| **integrations/cache-moka** | [crates/integrations/cache-moka/](crates/integrations/cache-moka/) | Moka-backed object/metadata cache. |
| **integrations/playground** | [crates/integrations/playground/](crates/integrations/playground/) | `iceberg-playground` — scratch crate for experimentation. |
| **examples, sqllogictest, test_utils, integration_tests** | [crates/](crates/) | Runnable examples, SQL logic tests, shared test helpers, end-to-end integration suites. |

### Inside the `iceberg` crate

```
crates/iceberg/src/
├── spec/         table/manifest/schema/snapshot/partition/view metadata types (the on-disk format)
├── catalog/      the Catalog trait + table/view identifiers + creation/update types
├── scan/         table scan planning → Arrow record batches
├── transaction/  atomic metadata updates (append, sort-order, properties, location, statistics,
│                 upgrade-format-version) + the TransactionAction / ApplyTransactionAction seam
├── writer/       data + equality-delete writers, file/rolling/partitioning writers
├── arrow/        Arrow ⇄ Iceberg schema/value conversion + merge-on-read delete application
├── avro/         Avro encoding for manifests/metadata
├── io/           object storage abstraction (FileIO; OpenDAL impl in crates/storage/opendal)
├── expr/         predicate / boolean expression trees + binding + visitors
├── transform/    partition transforms (identity, bucket, truncate, year/month/day/hour, void)
├── inspect/      metadata tables (snapshots, manifests — more variants are a parity gap)
├── puffin/       Puffin file format (stats / deletion vectors)
├── delete_vector.rs / delete_file_index.rs   merge-on-read delete handling
└── metadata_columns.rs                        reserved metadata columns (_file, _pos, ...)
```

Patterns to internalize: **the spec module is the source of truth** for the on-disk format —
changes there ripple through every reader and writer. **Catalogs are pluggable** behind one trait;
**FileIO is pluggable** behind OpenDAL. **Arrow is the in-memory currency** — scans produce Arrow,
writers consume it. **Transactions extend via `TransactionAction`** (`transaction/action.rs`); the
trait is currently `pub(crate)` — since we own this fork, opening it is the sanctioned path to new
write actions in Phase 2 (see [Roadmap.md](Roadmap.md)).

## Build & test commands

The canonical entry points are in the [Makefile](Makefile) (run from the repo root):

```bash
make build         # cargo build --all-targets --all-features --workspace
make check         # fmt --check + clippy -D warnings + taplo TOML check + cargo-machete (unused deps)
make unit-test     # doc tests + lib tests only (faster)
make test          # docker-up + doc tests + cargo test --no-fail-fast --all-targets --all-features --workspace
make check-msrv    # cargo +<MSRV> check --workspace
```

Or the underlying cargo commands directly:

```bash
cargo build --workspace
cargo test --workspace --no-fail-fast
cargo clippy --all-targets --workspace -- -D warnings
cargo fmt --all -- --check
```

- **Toolchain:** the lint gate runs on a pinned nightly ([rust-toolchain.toml](rust-toolchain.toml),
  currently `nightly-2025-10-27`, which `rustup` fetches automatically); downstream only needs MSRV
  **1.92**. The pinned nightly declares the `rustfmt` and `clippy` components.
- **`protoc` prerequisite:** `crates/sqllogictest` transitively pulls `datafusion-substrait` →
  `substrait`, whose build needs the Protobuf compiler. If `protoc` is unavailable, the core surface
  still builds/tests via `cargo test --workspace --exclude iceberg-sqllogictest`. Install
  `protobuf-compiler` to run the full suite.
- **`make test` starts Docker** (`docker-up`) for integration suites (REST fixture, MinIO, etc.).
  AWS/Glue/S3-Tables integration tests need real credentials and are not part of the offline gate.
- **Formatter:** [rustfmt.toml](rustfmt.toml) (`StdExternalCrate` import grouping, module granularity).
  **TOML:** `taplo`. **Unused deps:** `cargo machete`.

CI lives in [.github/workflows/](.github/workflows/) — `ci.yml` (Rust), `ci_typos.yml`, `audit.yml`,
`codeql.yml`, `publish.yml`, plus website jobs. (Python binding CI/release workflows were removed with
the Python layer.)

## Absolute prohibitions

These are irreversible or hard-block. The operating manuals (Non-Negotiables) reference this section.

- **No destructive or irreversible operations without explicit approval** — no `git push --force`
  to shared branches, no history rewrite, no mass file deletion, no dropping/truncating data in a
  live catalog, no resource teardown. There is no rollback.
- **Never commit or log secrets, credentials, or tokens** — not in code, tests, fixtures, or
  `tracing` output. Treat AWS keys, catalog tokens, and S3 URIs with embedded creds as radioactive.
- **Do not break the on-disk format without explicit approval** — a changed spec encoding silently
  corrupts already-written tables. This is a table-format library; format stability is the product.
  (The public *Rust* API may evolve in service of parity — this is an owned fork — but call out any
  breaking surface change so downstream pins can follow.)
- **Never edit dependency files** — [Cargo.toml](Cargo.toml), `Cargo.lock`, any crate `Cargo.toml` —
  without explicit approval. (The Phase 0 version-family sync to 0.9.1 was the sanctioned exception and
  is complete; routine work does not touch these.)

## Rust conventions

The full engineering contract lives in the [skills/](skills/) manuals; this section is only the
repo-specific house style they point to.

- **Error handling:** library crates define error types with `thiserror` (the `iceberg` crate uses a
  central `Error` in [crates/iceberg/src/error.rs](crates/iceberg/src/error.rs)); binaries/examples
  may use `anyhow`. **No bare `.unwrap()` / `.unwrap_err()` in production paths** — carry context.
- **Imports & formatting:** let `cargo fmt` own layout (config in [rustfmt.toml](rustfmt.toml)); do
  not hand-format imports — the `StdExternalCrate` grouping and module granularity are automatic.
- **Lints:** code must pass `cargo clippy --all-targets --workspace -- -D warnings`.
- **House style — section banners + one blank line between top-level items.** For large modules,
  group related items under a banner: a `///` doc block followed by a `///` + space + a run of `=`
  characters out to the formatter width, with the closing banner directly above the item (no blank
  line between). Banners are hand-authored and `cargo fmt`-compatible. (Adopt this only where the
  surrounding module already uses it.)
- **Logging:** `tracing` with structured fields (`?error`, ids, durations), never `println!` in
  library code, and never log secrets.

<subagent_policy>

## Agent orchestration — current policy

**Single agent by default — do the work in the main thread.** Favor cost control and determinism.
Do **not** spawn sub-agents (`Agent` / Task fan-out, `Workflow` orchestration, plan-mode
`Explore` / `Plan` helpers) unless the user explicitly asks. Need to search or read broadly? Use
the read/search tools inline. The two heavy parity phases — **Phase 2 (write engine)** and
**Phase 4 (formats & V3 types)** — are the natural fan-out candidates **if** the user lifts this
policy; everything else is comfortably single-agent. When the user *does* ask for sub-agents, default
them to Sonnet or Haiku; only run an Opus sub-agent on a direct, explicit instruction naming Opus.

</subagent_policy>

## Working conventions

- **Chain the verification gate to the commit in ONE `&&` chain** — `typos . && cargo fmt --all --
  check && git add -A && git commit …` — never put `git commit` on a separate line from the gate: a
  failed gate on its own line still lets the commit run. (Promoted 2026-06-09 from a twice-repeated
  lessons entry.)
- **Upstream is a sync baseline, not a constraint.** This is an owned fork for Java `iceberg-core`
  parity — edit freely; sync up from upstream and cherry-pick wins, but mergeability is not required.
- **Tests ship with the change**, plus interop tests where applicable (see the Parity mandate and the
  manuals' §4 Done gate).
- **Keep `map.md` in lockstep** with the directories that use it (see `<map_md_navigation>`).
- **Follow the operating manual for your tier** ([skills/](skills/)) — Risk-First, naming, the
  Rust rules, the debugging protocol, and the verification gate. CLAUDE.md wins on conflict.
</content>
</invoke>
