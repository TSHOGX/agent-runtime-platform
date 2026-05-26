# Phase 11: Trajectory → Memory → Skill Pipeline

> Status: design only / future. Not actively planned.
> Roadmap entry: [PLAN.md → Phase 11](./PLAN.md#phase-11-trajectory--memory--skill-pipeline-future).
> Depends on [Phase 9c system-skills mount](./phase9/system-skills-mount.md). Phase 11 may add a reviewed skills release layer on top of that mount.

## Goal

Use historical session trajectories to continuously improve the shared skills and memory available to future sandbox sessions.

The design assumes all data stays on internal servers. Therefore, this pipeline keeps full raw trajectories and does not include a redaction stage.

The core rule:

```text
raw trajectory -> episode memory -> semantic memory -> skill candidate -> versioned skills release
```

Do not compile raw sessions directly into skills. Skills should be stable, actionable, and supported by evidence across trajectories.

## Research-Inspired Shape

A few ideas borrowed from agent memory and skill-evolution work:

- Reflexion: turn outcomes and feedback into natural-language lessons that future agents can use.
- Generative Agents: maintain raw observations, reflect into higher-level memories, and retrieve relevant memories later.
- Voyager: grow a reusable skill library from repeated environment feedback and task success/failure.
- MemGPT-style memory hierarchy: keep short context separate from long-term archival memory.
- Experience replay / CER / ERL style systems: synthesize repeated trajectories into reusable heuristics rather than stuffing raw history into every prompt.
- SkillOS / MemSkill style systems: treat skill curation as a first-class lifecycle with selection, editing, promotion, and evaluation.

Practical takeaway: a two-track output:

- **Memory:** many small facts, heuristics, preferences, pitfalls, bug signals.
- **Skills:** fewer, curated, versioned operating procedures mounted into every new container.

## Data Levels

### 1. Raw Trajectory

The full session record.

Sources:

- `sessions`
- `messages`
- `turns`
- `events`
- `artifacts`
- selected artifact contents when useful and small enough

Storage:

```text
/var/lib/harness/trajectories/raw/<session_id>.json
```

Index table:

```text
trajectory_snapshots
- session_id
- raw_json_path
- status
- agent
- started_at
- ended_at
- event_min_id
- event_max_id
- artifact_manifest_json
- skills_digest
- skills_release_id (nullable; populated only if Phase 11 introduces a release layer above the 9c mount)
- fidelity: full | partial
- created_at
- processed_at
```

`fidelity=partial` means historical events were already pruned and the snapshot was built from the remaining messages/artifacts.

### 2. Episode Memory

A structured summary of one session or one major task inside a session.

Table:

```text
episode_memories
- id
- session_id
- task
- outcome: success | partial | failed | abandoned | unknown
- domains_json
- worked_steps_json
- failed_steps_json
- user_frustration: none | mild | high | unknown
- candidate_memories_json
- evidence_json
- model_version
- created_at
```

Candidate memory shape:

```json
{
  "type": "fact | heuristic | procedure | bug | preference",
  "scope": "global | doris | gvisor | frontend | agent-runtime",
  "statement": "When Doris SELECT fails with compute-group permission, treat it as environment permission rather than SQL syntax.",
  "when_to_apply": "Doris SQL execution returns compute-group or backend permission errors.",
  "evidence": [
    {"session_id": "...", "message_id": 12},
    {"session_id": "...", "event_id": 991}
  ],
  "confidence": 0.7
}
```

### 3. Semantic Memory

Cross-session consolidated knowledge.

Table:

```text
semantic_memories
- id
- type: fact | heuristic | procedure | bug | preference
- scope
- statement
- when_to_apply
- support_count
- contradiction_count
- evidence_session_ids_json
- evidence_refs_json
- confidence
- status: candidate | accepted | deprecated
- first_seen_at
- last_seen_at
- updated_at
```

Memory promotion rules:

- Repeated and consistent evidence increases confidence.
- Contradictory evidence decreases confidence.
- Recent repeated failures increase urgency.
- User frustration increases priority, especially for bugs and missing context.
- A memory can be accepted without becoming a skill.

### 4. Skill Candidate

A proposed change to the shared skills pack.

Table:

```text
skill_candidates
- id
- target_skill_name
- operation: create | update | deprecate
- title
- body_md
- source_memory_ids_json
- evidence_session_ids_json
- support_count
- confidence
- status: draft | needs_review | accepted | rejected | published
- created_at
- reviewed_at
- published_release_id
```

Skill candidates should be human-reviewable Markdown, not opaque model output.

### 5. Skill Release

A versioned skills bundle consumed by [Phase 9c](./phase9/system-skills-mount.md).

Table:

```text
skill_releases
- release_id
- digest
- parent_release_id
- source: manual | nightly | mixed
- path
- status: draft | current | archived
- created_at
- activated_at
```

## Nightly Pipeline

### Step 1: Snapshot

Select sessions that are terminal or idle long enough:

```text
status in failed/destroyed/checkpointed/running_idle
last_activity_at < now - cooldown
not already snapshotted at current event_max_id
```

Build raw JSON:

```json
{
  "session": {...},
  "messages": [...],
  "turns": [...],
  "events": [...],
  "artifacts": [
    {
      "path": "report.md",
      "size": 1234,
      "content_preview": "...",
      "content_path": "/var/lib/harness/sessions/<id>/report.md"
    }
  ]
}
```

Write `trajectory_snapshots`.

Important: run this before event pruning, or raise event retention enough that nightly snapshots always have a complete event window.

### Step 2: Episode Extraction

For each unsummarized snapshot:

1. Identify task boundaries.
2. Summarize user intent.
3. Determine outcome.
4. Extract worked steps and failed steps.
5. Detect frustration and correction signals.
6. Emit candidate memories.

Useful signals:

- Explicit user dissatisfaction: "不对", "不是这个", "你没理解", "算了", "怎么又错了".
- Repeated correction by the user.
- User asks agent to redo same work multiple times.
- Agent hits the same command/database error repeatedly.
- Session ends right after an error.
- User switches from agent to shell to fix something.
- Tool/runtime errors: failed start, bridge issues, proxy failures, compute-group permission, missing schema, missing dependency.

### Step 3: Consolidation

Group candidate memories by:

- normalized statement embedding
- scope/domain
- error class
- mentioned table/file/path/command
- repeated user intent

Then merge into `semantic_memories`.

Consolidation should keep evidence, not just the merged statement. A useful memory must be traceable back to sessions.

### Step 4: Skill Candidate Generation

Promote semantic memories to skill candidates only when they are:

- repeated enough,
- operationally useful,
- stable enough,
- directly actionable by future agents.

Examples:

- Good memory only:
  "Many users ask about battery charging analysis."
- Good skill:
  "When asked for charging analysis in Doris, inspect schema-pack first, use these candidate tables, run this metadata query, then produce CSV/PNG/report.md."

Suggested threshold for the first implementation:

```text
support_count >= 3
confidence >= 0.75
contradiction_count == 0
type in procedure/heuristic/fact
```

Bug memories become bug candidates, not skills, unless a stable workaround exists.

### Step 5: Draft Skill Pack

Write draft skills to:

```text
/var/lib/harness/system-skills/drafts/<date>-nightly/
```

Each generated skill includes:

```markdown
# Skill Name

## When To Use

## Context

## Procedure

## Known Pitfalls

## Examples

## Evidence
- support_count:
- source_sessions:
- generated_at:
```

Drafts are not mounted by default (see [Phase 9c](./phase9/system-skills-mount.md) — drafts require an explicit development flag).

### Step 6: Review and Publish

Review flow:

1. Human reviews `skill_candidates`.
2. Accepted candidates update a draft skills bundle.
3. Run a small replay/eval set.
4. Publish to `releases/<release_id>`.
5. Atomically update `current`.

The eval can start simple:

- Given historical prompt, does the agent choose the intended skill?
- Does generated SQL mention the expected database/table family?
- Does the output avoid known bad steps from the historical failure?
- For runtime skills, does the agent use the documented path/env/command?

## Runtime Integration

The easiest landing path is static skills refresh:

1. Nightly pipeline generates draft skills.
2. Human publishes a reviewed skills payload.
3. Phase 11 either commits the accepted payload back to `sandbox-image/system-skills/` or introduces a formal `releases/<release_id>` tree above the Phase 9c mount.
4. New sessions mount the selected skills payload.
5. Existing sessions stay pinned to their original skills digest.

This avoids changing the turn execution path. Dynamic memory retrieval (injecting relevant `semantic_memories` per turn as hidden context) is a later sub-phase and should not block the first working loop.

## Historical Backfill

Backfill command:

```text
harness-memory backfill --since 2026-05-01 --until 2026-05-25
```

Behavior:

- Build snapshots for old sessions.
- Mark `fidelity=full` if events are still present.
- Mark `fidelity=partial` if only messages/artifacts remain.
- Run extraction and consolidation in batches.
- Generate an initial memory report but do not publish skills automatically.

## Reports

Write daily reports under:

```text
/var/lib/harness/analytics/reports/YYYY-MM-DD.md
```

Report sections:

- Top repeated user intents.
- Top repeated failures.
- Highest-frustration sessions.
- New accepted semantic memories.
- Skill candidates created.
- Bug candidates created.
- Skills published or pending review.

## Implementation Sub-Phases

### 11A: Snapshot and Episode Memory

- Add `trajectory_snapshots`.
- Export raw JSON for completed/idle sessions.
- Add `episode_memories`.
- Produce a daily Markdown report.

### 11B: Semantic Memory

- Add `semantic_memories`.
- Cluster and merge candidate memories.
- Track support, contradiction, confidence, and evidence.

### 11C: Skill Candidates

- Add `skill_candidates`.
- Generate Markdown draft skill changes.
- Keep all candidates in review state by default.

### 11D: Publish to Mounted Skills

- Integrate with the Phase 9c skills mount.
- Publish accepted drafts into `sandbox-image/system-skills/`, or introduce a formal `releases/<release_id>` tree if independent rollback/review requires it.
- Ensure new sessions resolve the selected payload while existing sessions remain digest-pinned.

### 11E: Runtime Retrieval (optional)

- Retrieve relevant semantic memories at turn time.
- Keep this separate from static system skills to avoid coupling nightly memory updates to every active session.

## Design Principle

Memory can evolve quickly. Skills should evolve slowly.

Use memory for observations and emerging patterns. Use skills only for procedures and facts that are stable enough to affect every future session.
