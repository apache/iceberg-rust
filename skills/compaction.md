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

# Lessons Compaction — keeping `task/lessons.md` readable in one sitting

This document is the canonical procedure for compacting [task/lessons.md](../task/lessons.md). The
tier manuals' §2 (Self-Improvement Loop) and Workflow Storage tables point here; this file owns
the rule.

## The problem this solves

The manuals require reading `lessons.md` **in full at the start of every session**. The file is
append-only by design, and a single working day of BUILDER/REVIEWER increments appends dozens of
entries. Left alone, the session-start read cost grows linearly forever — eventually crowding out
the context the session actually needs (the code, the maps, the plan). An unbounded memory that
must be read in full is a memory that will eventually be skimmed, and a skimmed lessons file is
how already-fixed mistakes come back.

Compaction bounds the read cost **without ever destroying a lesson**. Nothing is deleted —
lessons are *promoted*, *kept*, or *archived*, and the archive remains grep-able forever.

## The lifecycle model — every lesson has one of three fates

When a compaction pass runs, every entry in the active file is assigned exactly one verdict:

| Verdict | What it means | Where it goes |
|---|---|---|
| **PROMOTE** | The lesson is a *durable rule* — it will apply to work in this repo indefinitely and is not tied to one increment. | Its canonical home (see promotion targets below), with a `_promoted YYYY-MM-DD → <target>_` stub left in the archive entry. The active file drops it. |
| **KEEP** | Still *live*: recent (within the recency window), tied to in-flight or upcoming work, or a rule whose durability is not yet clear. | Stays in the active `task/lessons.md`, verbatim. |
| **ARCHIVE** | *Spent*: an increment-scoped narrative whose work has shipped, a superseded rule, or a one-off whose generalizable core was promoted. | Moved verbatim to the archive (layout below). |

**Default-deny on deletion:** there is no fourth verdict. A lesson that seems worthless is
ARCHIVE, not gone — judgment about worth is exactly what a future session may need to revisit.

### What distinguishes a PROMOTE from a KEEP

A lesson is promotable when it has stopped being "a thing that happened" and become "how this repo
works." Tests for promotability:

- It would still be true if the increment that produced it had never existed.
- It names a *class* of mistake, not one instance ("strict `<` on wall-clock millis is flaky" —
  class; "Increment 9's third test was flaky" — instance).
- It has *recurred*, or its first occurrence was expensive enough that one occurrence suffices.
- It contradicts or refines a rule already in a manual / `CLAUDE.md` / a `map.md` — in which case
  promotion is mandatory, because two sources now disagree and the precedence chain only works if
  the canonical home is correct.

When in doubt, KEEP — a lesson can be promoted on the next pass; an over-eager promotion bloats
the manuals, which are read even more often than the lessons file.

### Promotion targets — where a durable rule lives

| The lesson is about... | Promote into... |
|---|---|
| A repo-wide engineering rule (testing discipline, error handling, style) | The relevant section of the tier manuals ([skills/](.)) — all variants, since the rules are identical across tiers |
| Repo intent, precedence, prohibitions, build/test behavior | [CLAUDE.md](../CLAUDE.md) (it wins all conflicts, so it must stay correct) |
| A failure mode, first-check, or escalation specific to one directory | That directory's `map.md` **`## Debug`** section — this is the highest-value target; most REVIEWER lessons are really debug knowledge with a home address |
| A library API correction (§7) | The directory `map.md` of the code that uses it, or `CLAUDE.md` if repo-wide |
| Testing-specific discipline | [docs/testing.md](../docs/testing.md) |
| A capability-status fact ("X actually works / doesn't") | [docs/parity/GAP_MATRIX.md](../docs/parity/GAP_MATRIX.md), date-stamped per its provenance rule |

A promotion is an **edit to the target file in the same compaction change** — not a TODO to edit
it later. The `map.md` same-change rule applies as usual.

## Triggers — when to run a pass

Run a compaction pass when **any** of these holds; do not run one mid-increment.

1. **Size:** the active `task/lessons.md` exceeds **~800 lines or ~50 KB**. (At typical entry
   size, that keeps the session-start read to roughly a fifth of where the file stood when this
   procedure was written.)
2. **Phase boundary:** a Roadmap phase completes. Phase-scoped narratives are spent by
   definition; this is the natural archive line and pairs with the GAP_MATRIX re-audit that
   `CLAUDE.md` already requires at phase end.
3. **Staleness:** the oldest KEEP-by-recency entry is more than **30 days** old and the file has
   grown since the last pass.
4. **On request:** the user asks for one.

**Recency window:** entries from the last **7 days** default to KEEP regardless of verdict
analysis (they may describe in-flight work whose context is not yet fully visible), unless they
are already explicitly superseded.

> **Agentic-pace amendment (2026-06-09, approved with the first pass).** Seven calendar days
> assumes human pace. When the project moves at agent pace (multiple increments per day), measure
> recency in WORK, not wall-clock: an entry defaults to KEEP if it is from the **current calendar
> day**, OR describes work that is still **open / deferred / directly feeding the next planned
> increments**. An increment-scoped narrative whose work has landed, been merged, and (where
> applicable) been superseded or interop-proven is eligible for ARCHIVE/PROMOTE even when only a
> day or two old — otherwise a compressed timeline makes every pass a no-op while the file grows
> past readability.

## Archive layout

```
task/
├── lessons.md                      # the active file — read in full every session
└── lessons-archive/
    ├── map.md                      # standard map.md: which archive covers what
    ├── 2026-06_phase2-phase3.md    # one file per pass: YYYY-MM_<scope>.md
    └── ...
```

- Archive files are **verbatim moves** — entries keep their original date stamps, headings, and
  wording. No paraphrasing on the way out: a paraphrased archive is a lossy archive.
- Archive files are **append-closed**: a pass creates one file and never reopens an old one.
- The archive directory carries its own `map.md` (per the navigation convention) whose Contents
  table says which increments/phases/date-ranges each file covers — so a future session can
  grep-then-read the one relevant archive instead of all of them.
- **Sessions do not read archives by default.** They read them on demand: when debugging
  something that smells like a past issue, when a KEEP entry references a superseded one, or when
  the archive `map.md`'s intent table routes them there.

## The compaction header — provenance in the active file

The active `task/lessons.md` carries, directly under its intro, a small provenance block that
every session sees:

```markdown
> **Compaction log.** Last pass: 2026-06-09 (Phase 2/3 boundary) →
> [lessons-archive/2026-06_phase2-phase3.md](lessons-archive/2026-06_phase2-phase3.md).
> Promoted that pass: 4 rules (2 → skills manuals, 1 → crates/iceberg/src/transaction/map.md#debug,
> 1 → docs/testing.md). Archives are not read by default — see
> [skills/compaction.md](../skills/compaction.md).
```

One line per pass, newest first, capped at the last five passes (older log lines move to the
archive `map.md`).

## Procedure — a pass is its own scoped change

A compaction pass edits the agent's own memory and several canonical documents at once. It is
treated with the same care as a destructive operation, even though nothing is destroyed:

1. **Never bundle.** A pass is its own change — its own plan in [task/todo.md](../task/todo.md),
   its own commit/PR. Never fold compaction into feature work; a reviewer must be able to see
   *only* memory edits in the diff.
2. **Plan first (§1).** List the trigger that fired, the proposed archive filename, and the
   expected promotion targets before touching anything.
3. **Verdict every entry.** Walk the active file top to bottom; assign PROMOTE / KEEP / ARCHIVE to
   each dated entry. Record the tally in the plan (e.g. "61 entries: 4 PROMOTE, 19 KEEP,
   38 ARCHIVE").
4. **Execute promotions** — edit each target file; leave the `_promoted YYYY-MM-DD → <target>_`
   stub on the entry as it moves to the archive, so the trail survives.
5. **Move ARCHIVE entries verbatim** into the new archive file; update (or create) the archive
   `map.md` in the same change.
6. **Rebuild the active file:** intro + compaction header + KEEP entries in original order.
7. **Conservation check (the gate):** every dated entry that existed before the pass exists after
   it — in the active file, in the new archive, or as a promoted rule with an archive stub.
   `grep -c '^### 20'` across the old file vs. (new active + new archive) must reconcile. A pass
   that loses an entry is reverted, not patched.
8. **Human review (interactive mode):** present the tally and the promotion diffs for approval
   before committing. In delegated mode, a pass may be *prepared* but not committed — compaction
   is interactive-approval-only, because it edits the documents that govern every future session.

## Done gate for a compaction pass

- [ ] Trigger named in the plan; pass is a standalone change (nothing else in the diff).
- [ ] Every pre-pass entry accounted for (conservation check reconciles).
- [ ] All promotions landed in their target files **in this change**, each with an archive stub.
- [ ] No entry was paraphrased, reworded, or merged on its way to the archive.
- [ ] Archive `map.md` updated/created; compaction header updated in the active file.
- [ ] Active file is under the size trigger with headroom (target: well under 800 lines).
- [ ] All entries from the 7-day recency window retained as KEEP unless explicitly superseded.
- [ ] User approved the tally and promotion diffs (interactive), or the pass is staged-not-committed (delegated).

## Anti-patterns — what a pass must never do

- **Summarize instead of archive.** "Condensed 30 entries into 5 themes" destroys the specific
  reproduction details that made the lessons useful. Promotion distills; archiving preserves.
- **Promote eagerly to shrink the file.** The manuals and `CLAUDE.md` are read more often than
  the lessons file — moving noise upstream makes the problem worse everywhere.
- **Compact mid-increment.** In-flight context is exactly what the recency window protects.
- **Skip the conservation check because the diff "looks right".** The check exists because a
  large mechanical move is where an entry silently disappears.
- **Let archives become required reading.** If sessions routinely need an archive, the pass that
  created it archived something that was actually a KEEP or a PROMOTE — fix the verdict, don't
  grow the read order.

---

# Todo Archival — keeping `task/todo.md` actionable in one sitting

The todo-file analogue of lessons compaction. [task/todo.md](../task/todo.md) is the in-flight
plan; the manuals require reading it to pick up mid-flight work. Like `lessons.md` it grows
append-only — every shipped increment leaves its full narrative behind — so without periodic
archival the "what's next" signal drowns in "what already happened." A todo file a session must
read to find the open items, but that is 90% completed-increment history, is a todo file that gets
skimmed, and a skimmed plan is how in-flight work gets dropped or redone.

Archival bounds the read cost **without destroying a narrative.** Nothing is deleted — increment
narratives are *archived* (and remain grep-able forever); open work is *kept* live.

## The lifecycle model — two verdicts, no PROMOTE

Unlike lessons compaction there is **no PROMOTE verdict** (a todo narrative is not a durable rule
with a canonical home elsewhere; it is a record of work). Every section gets one of two fates:

| Verdict | What it means | Where it goes |
|---|---|---|
| **KEEP** | Live work: the intro, the current active/hardening-sprint plan, and every genuinely-OPEN (`[ ]`) item with enough context to act on. | Stays in the active `task/todo.md`, verbatim (or, for an open item buried in an otherwise-shipped section, lifted into a live "Carried-forward open items" section with a pointer to its archived narrative). |
| **ARCHIVE** | A *completed* increment narrative — its work has shipped (and usually merged). | Moved verbatim to `task/todo-archive/<phase>.md`, grouped by Roadmap phase. |

**Default-deny on deletion:** there is no third verdict. A spent narrative is ARCHIVE, not gone.

### Stale vs genuinely-open checkboxes

A shipped section often carries `[ ]` boxes that were never ticked but whose work *did* land under
a renumbered/superseded increment (planning churn). These are **not** live work: archive the
section verbatim (the unticked box is preserved in context, so a future reader sees it was
superseded) and do **not** surface it in the live file. Only a box whose work is genuinely
outstanding is carried forward. When unsure whether a box is stale, verify against the
GAP_MATRIX / the code / the `[x]` siblings in the same section before deciding — surfacing a
done item as "open" is as misleading as burying a real one.

## Archive layout

```
task/
├── todo.md                        # the active file — intro + active sprint + open items + pointers
└── todo-archive/
    ├── map.md                     # standard map.md: which phase archive covers what
    ├── phase1.md                  # one file per Roadmap phase THAT HAS shipped narratives
    ├── phase2.md
    └── phase3.md
```

- One file **per Roadmap phase** (phase0, phase1, …) — but only create a file for a phase that
  actually has archived narratives (an empty-phase file is noise). A section is assigned to the
  phase its header names; a write-engine validation done during a later phase's calendar window
  is still Phase-2 work — assign by the capability, and when a header is explicit ("Phase 3")
  trust it.
- Archive files are **verbatim moves** — sections keep their original headings, dates, checkbox
  states, and wording. No paraphrasing, no checkbox flips on the way out.
- The archive directory carries its own `map.md` (navigation convention) whose Contents table
  says which increments/capabilities each phase file covers — so a future session greps the one
  relevant archive instead of all of them.
- **Sessions do not read archives by default.** They read them on demand — when the live file's
  Carried-forward pointers route them there, or when reconstructing why a shipped increment did
  what it did.

## The active file after a pass

```
intro (purpose + how-to-use)
## Active: <the current sprint / hardening section>       ← KEEP verbatim
## Carried-forward open items (detail in todo-archive/)   ← every genuinely-open [ ] not in the sprint,
                                                             lifted with a one-line archive pointer each
## Archived increment narratives                          ← one pointer line per phase archive file
```

A genuinely-open item buried in a shipped section is **lifted** into "Carried-forward open items"
(verbatim where short; a faithful one-line summary + archive pointer where it is a long sub-tree),
so the live file stays the actionable plan. Its originating narrative still archives in full — the
lifted bullet is the single carve-out from "the live file and the archive never overlap," and it is
intentional: an open item must be visible in the plan, its history visible in the archive.

## Procedure — a pass is its own scoped change

Treated with the same care as lessons compaction (it edits the plan every future session reads):

1. **Never bundle.** Its own branch, its own commit/PR — a reviewer sees *only* the archival move.
2. **Plan first (§1).** Name the trigger (size), the archive filenames, the phase assignment of
   each section, and which `[ ]` items are genuinely-open (carry forward) vs stale (archive).
3. **Script the split — never hand-copy.** Drive the move with a script (Python over the `## ` /
   `### ` heading boundaries) so a section is moved as an exact byte range, not retyped. Hand-copy
   is where a line silently drops.
4. **Conservation check (the gate):** every `## ` and `### ` heading present BEFORE the pass must
   exist AFTER it in exactly one place (the active file or exactly one archive). `grep -c '^##'`
   and `grep -c '^###'` across the old file vs (new active + all new archives) must reconcile, and
   no heading may appear twice. Record the tally in the commit message (e.g. "18 ##/42 ###: 1 ##
   kept live, 17 ## archived → phase1 1, phase2 4, phase3 12"). A pass that loses a heading is
   reverted, not patched.
5. **Build the active file:** intro + the active-sprint section(s) + the Carried-forward section +
   the Archived-narratives pointer section. Target: **well under ~500 lines.**
6. **Create/update `task/todo-archive/map.md`** in the same change.
7. **`typos` clean** and the chained-gate commit (CLAUDE.md Working conventions).

## Done gate for a todo-archival pass

- [ ] Trigger named in the plan; pass is a standalone change (nothing else in the diff).
- [ ] Every pre-pass `## ` / `### ` heading accounted for (conservation reconciles, none duplicated).
- [ ] No section was paraphrased, reworded, or had a checkbox flipped on its way to the archive.
- [ ] Every genuinely-open `[ ]` item is reachable from the active file (kept in the sprint section
      or lifted into Carried-forward with an archive pointer); no stale box surfaced as live work.
- [ ] `task/todo-archive/map.md` created/updated; pointer section in the active file lists each archive.
- [ ] Active file is well under ~500 lines.

## Anti-patterns — what a pass must never do

- **Flip a checkbox to "tidy up."** Archival preserves state verbatim; deciding a stale `[ ]` is
  done is a separate, explicit judgment recorded as such — not a silent edit during the move.
- **Hand-copy a section.** A 700-line section retyped by hand is a section with a dropped line.
  Script the byte-range move and conservation-check it.
- **Create an empty per-phase file** for a phase with no shipped narratives — pointers to nothing.
- **Archive a section that still has live work** just to hit the line target — lift its open items
  first, then archive the spent narrative.
