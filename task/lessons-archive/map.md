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

# map.md — task/lessons-archive/

## Purpose

Verbatim archives produced by [skills/compaction.md](../../skills/compaction.md) passes over
[../lessons.md](../lessons.md). **Not read by default** — sessions grep here on demand (a past-issue
smell, a superseded reference, a `_promoted_` stub trail). Nothing is ever deleted: every entry is
in the active file, in exactly one archive file, or promoted with a stub here.

## Contents

| File | Covers | Entries |
|---|---|---|
| `2026-06_wave3-wave4-overnight.md` | Pass 3 (2026-06-12, size trigger): the 2026-06-08→10 entries pass 2 kept live (residual scan-wiring, E1/Inc4 interop, the DV arc D1–D4, the post-arc audit, Arc E/F/G) + Waves 3–4 (multi-spec writes, constants-map activation, removeRows, ExpireSnapshots B1/B2 + A3 interop, DeleteOrphanFiles A1/A2, variant B1/B2 + F1/F2 builder halves, O1–O3 builder halves, the S1–S3 Sonnet arc, V2 builder, W1/W2 process halves). 3 rules promoted (stubs inline). | 47 |
| `2026-06_phase2-completion.md` | Pass 2 (2026-06-11, size trigger): the Phase-2 write-engine completion arc (RewriteManifests, RewriteFiles seq preservation, the delete-manifest-carry fix, MergeAppend, the 8-step interop extension, the matrix repair, PR #20) + the E1/E2 metadata-interop sprint + the 2026-06-08/09 scan, metrics, and conflict-validation increments. 6 rules promoted (stubs inline). | 25 |
| `2026-06_phase1-phase3.md` | Pass 1 (2026-06-09, size trigger): Phase 0 reset + Phase 1 evolution actions (ManageSnapshots / UpdatePartitionSpec / UpdateSchema, increments 1–11) + Phase 2 write-engine increments (DeleteFiles / OverwriteFiles / ReplacePartitions / RewriteFiles / PositionDeleteWriter) + Phase 3 inspection tables, residual evaluator, incremental scans — BUILDER/REVIEWER narratives dated 2026-06-07 → 2026-06-09 | 52 (31 with promotion stubs, 21 plain) |

## I want to...

| I want to... | go to |
|---|---|
| Find why a rule exists in a map.md `## Debug` / docs/testing.md | grep this directory for the rule's keywords; the `_promoted 2026-06-09 → <target>_` stub marks the source entry |
| Read a landed increment's full builder/reviewer narrative | `2026-06_phase1-phase3.md`, ordered chronologically by the original `### YYYY-MM-DD (…)` headings |
| Check whether a lesson was lost | run the conservation check: `grep -c '^### 20'` over active + archives vs the pre-pass git history |

## Pointers

- **Up:** [../lessons.md](../lessons.md) (the active file, read every session) ·
  procedure: [skills/compaction.md](../../skills/compaction.md)

## Debug

### Known failure modes

| Symptom | Likely cause |
|---|---|
| A session routinely needs an archive entry | The pass gave it the wrong verdict — it was a KEEP or a PROMOTE; restore it to the active file (or promote it) in a follow-up change, don't read archives habitually |
| An archive entry contradicts a current map/manual rule | The promotion distilled or superseded it — the canonical home wins; the archive is provenance, not authority |

### First checks

- Check the compaction log at the top of [../lessons.md](../lessons.md) for which pass moved what.

### Escalate to

- Procedure questions → [skills/compaction.md](../../skills/compaction.md).
