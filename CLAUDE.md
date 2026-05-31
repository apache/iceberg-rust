# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Orientation for sessions working in this repository. This is a **fork of
[Apache Iceberg™ Rust](https://github.com/apache/iceberg-rust)** — the Rust implementation of the
[Apache Iceberg](https://iceberg.apache.org/) open table format. The upstream crates are tracked
as-is; the fork adds a Python layer (`iceberg-spark-python` / `iceberg-spark-pyspark`) on top.
The goal is to **build on and experiment with** Iceberg Rust while staying mergeable with upstream.

> **A note on the XML tags in this file.** A few sections are wrapped in semantic tags
> (`<read_order>`, `<map_md_navigation>`, `<subagent_policy>`). They mark the load-bearing
> "do this / don't skip this" regions so an agent can locate them unambiguously; they carry no
> meaning beyond "read this bounded region as a unit." Reference sections (snapshot, architecture,
> build/test, repo layout) are intentionally left untagged.

<read_order>

## Read order (every session)

1. **This file (CLAUDE.md)** — repository intent, prohibitions, and the navigation contract.
2. **The operating manual for your model tier** — [skills/Opus.md](skills/Opus.md),
   [skills/Sonnet.md](skills/Sonnet.md), or [skills/Haiku.md](skills/Haiku.md) (the portable
   engineering contract; see [skills/map.md](skills/map.md)). CLAUDE.md wins on any conflict.
3. **[task/lessons.md](task/lessons.md) in full, then [task/todo.md](task/todo.md)** — accumulated
   lessons and any mid-flight plan to pick up.
4. **The `map.md` of every directory your task will touch** (where present — see the navigation
   rule below).
5. **Upstream docs when you need depth:** [README.md](README.md), [CONTRIBUTING.md](CONTRIBUTING.md),
   the per-crate `README.md` files, and the [Iceberg Rust site](https://rust.iceberg.apache.org/).

</read_order>

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
evolution, and pluggable catalogs and object storage. It is a **library workspace** (plus a Python
binding), not an application — most code is `#![no_main]` library crates consumed by downstream
projects. Primarily **Rust** (edition 2024, MSRV 1.87 on stable), with a **Python** binding via
PyO3/maturin and, in this fork, a pure-Python PySpark-compatible layer.

## Big-picture architecture

### The workspace crates

| Crate | Path | Role |
|---|---|---|
| **iceberg** | [crates/iceberg/](crates/iceberg/) | The core: spec types, catalog trait, table scans, transactions, writers, Arrow/Avro/Parquet IO, expressions, partition transforms, metadata inspection, Puffin. |
| **iceberg-datafusion** | [crates/integrations/datafusion/](crates/integrations/datafusion/) | DataFusion integration — `TableProvider` / `CatalogProvider` / physical plans so Iceberg tables are queryable from DataFusion SQL. |
| **catalog/{rest,hms,glue,s3tables,sql}** | [crates/catalog/](crates/catalog/) | Concrete `Catalog` implementations: REST, Hive Metastore, AWS Glue, S3 Tables, and SQL-backed. |
| **catalog/loader** | [crates/catalog/loader/](crates/catalog/loader/) | Config-driven catalog construction (pick a catalog impl at runtime). |
| **integrations/cache-moka** | [crates/integrations/cache-moka/](crates/integrations/cache-moka/) | Moka-backed object/metadata cache. |
| **integrations/playground** | [crates/integrations/playground/](crates/integrations/playground/) | `iceberg-playground` — scratch crate for experimentation. |
| **examples, sqllogictest, test_utils, integration_tests** | [crates/](crates/) | Runnable examples, SQL logic tests, shared test helpers, end-to-end integration suites. |

### Fork additions (not upstream)

| Path | What it is |
|---|---|
| [bindings/python/](bindings/python/) | `pyiceberg-core` — PyO3/maturin Python binding to the Rust core (this is upstream, but the fork tracks it closely). |
| [iceberg-spark-python/](iceberg-spark-python/) | Pure-Python PySpark-compatible SQL/DataFrame layer over DataFusion + PyIceberg + `pyiceberg-core`. |
| [iceberg-spark-pyspark/](iceberg-spark-pyspark/) | A thin PySpark import-shim package that re-exports the layer above. |

### Inside the `iceberg` crate

```
crates/iceberg/src/
├── spec/         table/manifest/schema/snapshot/partition spec types (the on-disk format)
├── catalog/      the Catalog trait + table identifiers + metadata
├── scan/         table scan planning → Arrow record batches
├── transaction/  atomic metadata updates (append, overwrite, schema change)
├── writer/       data + position/equality delete writers
├── arrow/        Arrow ⇄ Iceberg schema and value conversions
├── io/           object storage abstraction (OpenDAL-backed FileIO)
├── expr/         predicate / boolean expression trees + binding
├── transform/    partition transforms (identity, bucket, truncate, year/month/day/hour)
├── inspect/      metadata tables (snapshots, manifests) — more variants live in the Python layer
└── puffin/       Puffin file format (stats / deletion vectors)
```

Patterns to internalize: **the spec module is the source of truth** for the on-disk format —
changes there ripple through every reader and writer. **Catalogs are pluggable** behind one trait;
**FileIO is pluggable** behind OpenDAL. **Arrow is the in-memory currency** — scans produce Arrow,
writers consume it.

## Build & test commands

The canonical entry points are in the [Makefile](Makefile) (run from the repo root):

```bash
make build         # cargo build --all-targets --all-features --workspace
make check         # fmt --check + clippy -D warnings + taplo TOML check + cargo-machete (unused deps)
make test          # doc tests + cargo test --no-fail-fast --all-targets --all-features --workspace
make unit-test     # doc tests + lib tests only (faster)
make check-msrv    # cargo +<MSRV> check --workspace
```

Or the underlying cargo commands directly:

```bash
cargo build --all-targets --all-features --workspace
cargo fmt --all -- --check
cargo clippy --all-targets --all-features --workspace -- -D warnings
cargo test --no-fail-fast --all-targets --all-features --workspace
```

- **Toolchain:** stable Rust, MSRV **1.87** (see [Cargo.toml](Cargo.toml) `rust-version`); a pinned
  nightly ([rust-toolchain.toml](rust-toolchain.toml)) runs the lint gate — both `cargo fmt` and
  `cargo clippy` (it declares the `rustfmt` and `clippy` components).
- **Formatter:** [rustfmt.toml](rustfmt.toml) (`StdExternalCrate` import grouping, module-granularity
  imports, doc-comment formatting). **There is no `[workspace.lints]` table** — the clippy gate is
  `-D warnings` from the Makefile / CI, not a lints config.
- **TOML:** `taplo fmt` / `taplo check` ([.taplo.toml](.taplo.toml)). **Unused deps:** `cargo machete`.
- **Python** (per package, not workspace): run from the package directory.
  - `bindings/python` — maturin/PyO3 (`maturin develop`, then `pytest`).
  - `iceberg-spark-python` — uses `uv`: `uv run ruff check .` · `uv run ruff format --check .` ·
    `uv run pytest`. Ruff config lives in that package's [pyproject.toml](iceberg-spark-python/pyproject.toml).

CI lives in [.github/workflows/](.github/workflows/) — `ci.yml` (Rust), `bindings_python_ci.yml`,
`ci_typos.yml`, `audit.yml`, plus release/website jobs.

## Absolute prohibitions

These are irreversible or hard-block. The operating manuals (Non-Negotiables) reference this section.

- **No destructive or irreversible operations without explicit approval** — no `git push --force`
  to shared branches, no history rewrite, no mass file deletion, no dropping/truncating data in a
  live catalog, no resource teardown. There is no rollback.
- **Never commit or log secrets, credentials, or tokens** — not in code, tests, fixtures, or
  `tracing` output. Treat AWS keys, catalog tokens, and S3 URIs with embedded creds as radioactive.
- **Do not break the public API or the on-disk format without explicit approval** — a changed
  trait, signature, or spec encoding silently breaks downstream crates and already-written tables.
  This is a table-format library; format stability is the product.
- **Never edit dependency files** — [Cargo.toml](Cargo.toml), `Cargo.lock`, any crate `Cargo.toml`,
  or a Python `pyproject.toml` — without explicit approval.
- **Keep the fork mergeable with upstream.** Prefer additive changes (new crates/modules under the
  fork additions) over edits to upstream files; when you must edit upstream code, keep the diff
  minimal and surface it.

## Rust conventions

The full engineering contract lives in the [skills/](skills/) manuals; this section is only the
repo-specific house style they point to.

- **Error handling:** library crates define error types with `thiserror` (the `iceberg` crate uses a
  central `Error` in [crates/iceberg/src/error.rs](crates/iceberg/src/error.rs)); binaries/examples
  may use `anyhow`. **No bare `.unwrap()` / `.unwrap_err()` in production paths** — carry context.
- **Imports & formatting:** let `cargo fmt` own layout (config in [rustfmt.toml](rustfmt.toml)); do
  not hand-format imports — the `StdExternalCrate` grouping and module granularity are automatic.
- **Lints:** code must pass `cargo clippy --all-targets --all-features --workspace -- -D warnings`.
- **House style — section banners + one blank line between top-level items.** For large modules,
  group related items under a banner: a `///` doc block followed by a `///` + space + a run of `=`
  characters out to the formatter width, with the closing banner directly above the item (no blank
  line between). Banners are hand-authored and `cargo fmt`-compatible. One blank line between
  top-level items. (Adopt this only where the surrounding module already uses it.)
- **Logging:** `tracing` with structured fields (`?error`, ids, durations), never `println!` in
  library code, and never log secrets.

<subagent_policy>

## Agent orchestration — current policy

**Single agent by default — do the work in the main thread.** Favor cost control and determinism.
Do **not** spawn sub-agents (`Agent` / Task fan-out, `Workflow` orchestration, plan-mode
`Explore` / `Plan` helpers) unless the user explicitly asks. Need to search or read broadly? Use
the read/search tools inline. When the user *does* ask for sub-agents, default them to Sonnet or
Haiku; only run an Opus sub-agent on a direct, explicit instruction naming Opus.

</subagent_policy>

## Working conventions

- **Upstream is the baseline.** This is a fork; keep changes additive and mergeable, and surface any
  edit to upstream files. Don't "fix" or refactor upstream code unless the user explicitly asks.
- **Tests ship with the change.** Behavior added without tests is a hard block (see the manuals'
  §4 Done gate and [docs/testing.md](docs/testing.md)), not a "strong default."
- **Keep `map.md` in lockstep** with the directories that use it (see `<map_md_navigation>`).
- **Follow the operating manual for your tier** ([skills/](skills/)) — Risk-First, naming, the
  Rust/Python rules, the debugging protocol, and the verification gate. CLAUDE.md wins on conflict.
