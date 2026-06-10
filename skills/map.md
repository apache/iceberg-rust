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

# map.md — skills/

## Purpose

Per-model-tier operating manuals: the portable engineering contract (reasoning
and planning, naming, Rust/Python rules, debugging protocol, verification
gates, the `map.md` convention itself) — plus the lessons-compaction procedure
the manuals' §2 points to. Read the manual matching the model you are running
as, **after** root [CLAUDE.md](../CLAUDE.md). CLAUDE.md wins on any conflict
(it is repo-specific; these are portable defaults).

## Contents

| File | For |
|---|---|
| `Fable.md` | Mythos-class sessions (Fable / Mythos — tersest variant; adds the Frontier Addendum: cost discipline, restricted-domain fallback awareness, sub-agent economics, calibration) |
| `Opus.md` | Opus-tier sessions (fullest prose; richest reasoning guidance) |
| `Sonnet.md` | Sonnet-tier sessions |
| `Haiku.md` | Haiku-tier sessions (most explicit + procedural — concrete examples, spelled-out steps) |
| `compaction.md` | The lessons-compaction procedure for [task/lessons.md](../task/lessons.md): lifecycle (PROMOTE / KEEP / ARCHIVE), triggers, archive layout, conservation gate |

The four manuals are variants of the same contract; **the rules are identical** — only the level of detail differs (Haiku = most explicit/procedural, Opus = fullest prose, Fable = tersest, plus its tier-specific Frontier Addendum, which adds operating notes without changing any rule). All four share one skeleton: Identity & Priority Stack → Non-Negotiables → Mode Handling → Risk-First → Workflow Orchestration §1–§9 → Navigation → Naming → Language-Specific Rules → Function Length & Recursion → Pre-Flight → Core Principles (TL;DR). (`Fable.md` inserts the Frontier Addendum between Non-Negotiables and Mode Handling.)

## I want to...

| I want to... | go to |
|---|---|
| Read my manual | the file matching my model tier (Mythos-class → `Fable.md`) |
| Find the `map.md` convention spec | any of the four (Navigation section) — but the repo's authoritative rule is in [CLAUDE.md](../CLAUDE.md) `<map_md_navigation>` |
| Find the testing-discipline contract (mandatory before any code change) | [docs/testing.md](../docs/testing.md) (referenced from each manual's verification gate) |
| Resolve a manual-vs-repo conflict | [CLAUDE.md](../CLAUDE.md) (it wins) |
| Know the sub-agent / parallelism policy | [CLAUDE.md](../CLAUDE.md) `<subagent_policy>` — single-agent by default; spawned sub-agents are Sonnet/Haiku only (each manual's Workflow Orchestration section points here; `Fable.md`'s Frontier Addendum adds: never spawn Fable-tier sub-agents unless the user names the tier) |
| Find the plan / lessons workflow | [task/todo.md](../task/todo.md) + [task/lessons.md](../task/lessons.md) (each manual's Workflow Storage section) |
| Compact the lessons file / decide what to archive | [compaction.md](compaction.md) — its own scoped change, interactive-approval-only |
| Find an old archived lesson | [task/lessons-archive/map.md](../task/lessons-archive/map.md) (archives are read on demand, never by default — see [compaction.md](compaction.md)) |

## Pointers

- **Up:** repo root [CLAUDE.md](../CLAUDE.md).
- **Related:** [docs/testing.md](../docs/testing.md) (the verification gate); [task/](../task/) (plan + lessons + lessons-archive). CLAUDE.md's read-order section sequences these into a session.

## Debug

### Known failure modes

| Symptom | Likely cause |
|---|---|
| Following a manual rule that contradicts the repo | CLAUDE.md overrides the manual — re-check CLAUDE.md. |
| Unsure which manual applies | Match the running model tier (Mythos-class → `Fable.md`); if unknown, default to `Opus.md` (fullest). |
| Session-start lessons read is consuming excessive context | A compaction trigger has likely fired — check [compaction.md](compaction.md) triggers; propose a pass (do not run one mid-increment). |
| A needed lesson seems to be missing from `task/lessons.md` | It may have been promoted or archived — check the compaction header at the top of [task/lessons.md](../task/lessons.md), then [task/lessons-archive/map.md](../task/lessons-archive/map.md). |
| A manual link doesn't resolve | The bundle expects to live at the repo root (`CLAUDE.md`, `skills/`, `docs/`, `task/` as siblings). Confirm the layout. |

### First checks

- Did you read root `CLAUDE.md` first? It sets precedence and the `map.md` rule.

### Escalate to

- Conflicts / precedence → [CLAUDE.md](../CLAUDE.md).
- Unresolved → open an issue.
