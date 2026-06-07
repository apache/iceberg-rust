# map.md — skills/

## Purpose

Per-model-tier operating manuals: the portable engineering contract (reasoning
and planning, naming, Rust/Python rules, debugging protocol, verification
gates, the `map.md` convention itself). Read the one matching the model you
are running as, **after** root [CLAUDE.md](../CLAUDE.md). CLAUDE.md wins on any
conflict (it is repo-specific; these are portable defaults).

## Contents

| File | For |
|---|---|
| `Opus.md` | Opus-tier sessions (fullest prose; richest reasoning guidance) |
| `Sonnet.md` | Sonnet-tier sessions |
| `Haiku.md` | Haiku-tier sessions (most explicit + procedural — concrete examples, spelled-out steps) |

The three are variants of the same contract; **the rules are identical** — only the level of detail differs (Opus = fullest prose, Haiku = most explicit/procedural). All three share one skeleton: Identity & Priority Stack → Non-Negotiables → Mode Handling → Risk-First → Workflow Orchestration §1–§9 → Navigation → Naming → Language-Specific Rules → Function Length & Recursion → Pre-Flight → Core Principles (TL;DR).

## I want to...

| I want to... | go to |
|---|---|
| Read my manual | the file matching my model tier |
| Find the `map.md` convention spec | any of the three (Navigation section) — but the repo's authoritative rule is in [CLAUDE.md](../CLAUDE.md) `<map_md_navigation>` |
| Find the testing-discipline contract (mandatory before any code change) | [docs/testing.md](../docs/testing.md) (referenced from each manual's verification gate) |
| Resolve a manual-vs-repo conflict | [CLAUDE.md](../CLAUDE.md) (it wins) |
| Know the sub-agent / parallelism policy | [CLAUDE.md](../CLAUDE.md) `<subagent_policy>` — single-agent by default; spawned sub-agents are Sonnet/Haiku only (each manual's Workflow Orchestration section points here) |
| Find the plan / lessons workflow | [task/todo.md](../task/todo.md) + [task/lessons.md](../task/lessons.md) (each manual's Workflow Storage section) |

## Pointers

- **Up:** repo root [CLAUDE.md](../CLAUDE.md).
- **Related:** [docs/testing.md](../docs/testing.md) (the verification gate); [task/](../task/) (plan + lessons). CLAUDE.md's read-order section sequences these into a session.

## Debug

### Known failure modes

| Symptom | Likely cause |
|---|---|
| Following a manual rule that contradicts the repo | CLAUDE.md overrides the manual — re-check CLAUDE.md. |
| Unsure which manual applies | Match the running model tier; if unknown, default to `Opus.md` (fullest). |
| A manual link doesn't resolve | The bundle expects to live at the repo root (`CLAUDE.md`, `skills/`, `docs/`, `task/` as siblings). Confirm the layout. |

### First checks

- Did you read root `CLAUDE.md` first? It sets precedence and the `map.md` rule.

### Escalate to

- Conflicts / precedence → [CLAUDE.md](../CLAUDE.md).
- Unresolved → open an issue.
