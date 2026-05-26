# P0: Session and Runtime Lifetime Separation

> Status: planned. Blocks Phase 9 work that depends on long-lived sessions.
> Roadmap entry: [PLAN.md → P0 fixes](./PLAN.md#p0-session-and-runtime-lifetime-separation).

## Goal

User sessions, conversation history, and workspace files must persist effectively forever. Live gVisor runtime resources (sandbox processes, netns/veth, checkpoint images, `/30` slots) must be flexibly releasable and reloadable on a separate lifecycle.

The two lifetimes are conceptually independent but the current code couples them in three places. This document specifies what to unwind so the rest of the roadmap can build on long-lived sessions.

## Background: What Is and Is Not Already Decoupled

Already decoupled (no change needed):

- `messages` and `artifacts` SQLite rows are never deleted by sweep/reaper.
- Workspace directory `/var/lib/harness/sessions/<id>` and agent-home directory `/var/lib/harness/agent-homes/<id>` are never touched by sweep/reaper.
- `MonitorIdleSessions` acts only on generations.
- `harness.reaper.failed_retention` governs only generation-scoped host resources (netns, veth, control/bundle/bridge/log dirs).
- Event log retention (`harness.events.retention_*`) is independent of conversation resumability; resumability only requires `messages` and workspace.

Currently coupled (must be fixed):

- `SweepExpiredSessions` at `orchestrator/internal/store/resources.go:559` cascades session expiry into generation failure and reclaimable allocations. The session row becomes `destroyed` and `sessionstate.CanAcceptInput` rejects all further input via the API.
- `failGenerationBeforeTurn` at `orchestrator/internal/server/server.go:440` unconditionally calls `FailSession` on any generation start failure, leaving the session permanently terminal even when conversation history and workspace are intact.
- `reserved_checkpointed` allocations are explicitly skipped by the reaper, so long-idle checkpointed sessions accumulate checkpoint images and `/30` network slots indefinitely.

## Required Changes

### 1. Hard rename `harness.session_ttl` → `harness.session_retention`

`session_ttl` overloads two distinct lifetimes. The new name expresses intent: how long the session row, history, and workspace persist. `0s` is the new default and means no automatic expiry. No backwards-compat alias — lab configs migrate in one step.

`harness.checkpoint.idle_threshold` already drives generation idle lifetime separately and does not change.

Touch points:

- `orchestrator/internal/config/config.go`: rename field on `Phase7Config`; relax validation from `<= 0` to `< 0` at the validation block around line 408; rename env override `HARNESS_SESSION_TTL` → `HARNESS_SESSION_RETENTION`.
- `orchestrator/internal/server/server.go:269`: guard `ExpiresAt` assignment behind `SessionRetention > 0`. When zero, set `ExpiresAt = nil`. The existing sweep query (`WHERE expires_at IS NOT NULL ...`) already skips NULL rows.
- `config/harness.yaml`: rename `session_ttl: 2h` → `session_retention: 0s`.
- `docs/architecture.md`, `docs/phase7/schema.md`: update prose. Currently both describe `session_ttl` as an absolute deadline.

### 2. Decouple generation start failure from session failure

`failGenerationBeforeTurn` currently calls both `FailGeneration` and `FailSession`. Generation start failures (probe failure, manifest mismatch, OCI bundle errors) are recoverable by retrying with a fresh generation, but today they leave the session permanently terminal.

Required behavior:

- Generation start failure: mark the generation `failed` and leave the session in its previous status (`created` or `running_idle`) so the next user message triggers a cold-start allocation via the existing `ensureActiveGeneration` path at `server.go:495`.
- Session-level invariant violation (corrupt session row, store-level CAS impossibility): still fail the session. This is the rare path and the only one that should call `FailSession`.

Touch points:

- `orchestrator/internal/server/server.go:440`: split the two paths. Most existing callers of `failGenerationBeforeTurn` are recoverable.
- `orchestrator/internal/server/server_test.go`: the test around line 1333 that asserts the session enters `failed` after generation failure must be revised to reflect retryable semantics. Add a new test that posts a second message after a generation start failure and asserts a fresh generation is allocated.

### 3. Add `harness.reaper.checkpoint_image_retention`

The reaper skips `reserved_checkpointed` allocations entirely (see `docs/phase7/runtime-resources.md` reaper-visibility list). A session that goes `checkpointed` and is never touched again retains its checkpoint image and `/30` slot forever.

New config:

```yaml
harness:
  reaper:
    failed_retention: 10m
    checkpoint_image_retention: 30d   # new
```

Behavior: when a `reserved_checkpointed` allocation's owning session has `last_activity_at < now - checkpoint_image_retention`, the Phase 7 maintenance job transitions the allocation to `reclaimable`. The reaper then runs its normal cleanup path (`runtime.DestroyGenerationResources`), which deletes the checkpoint image and frees the network slot.

This does not destroy the session row. After the checkpoint image is reaped, the next user message cold-starts a new generation: `ensureActiveGeneration` already handles the case where `active_generation_id` points to a `failed` or destroyed generation by allocating a fresh one.

Touch points:

- `orchestrator/internal/config/config.go`: add `Reaper.CheckpointImageRetention Duration`.
- `orchestrator/internal/store/resources.go`: add a maintenance query that selects `reserved_checkpointed` allocations whose session `last_activity_at` exceeds the threshold and updates them to `reclaimable`.
- `orchestrator/internal/server/server.go`: invoke from `RunPhase7Maintenance`.
- `config/harness.yaml`: add the field with `30d` default.

## Out of Scope (Follow-up)

- A "resume failed/destroyed session" UX path. The data is recoverable; the API gates on `sessionstate.CanAcceptInput`. A future change can add `POST /api/sessions/<id>/resume` that resets the session status to `running_idle` and clears `active_generation_id`. Tracked as a follow-up, not a P0 blocker.
- Workspace artifact GC policy. Workspaces grow forever today. Disk pressure is bounded by deployment scale for now.

## Tests to Add or Update

- `orchestrator/internal/config/config_test.go:206`: rename the `session ttl` test; update its expected error message; add a positive case for `session_retention: 0s`.
- `orchestrator/internal/server/server_test.go:43, 80`: rename field on the test harness; add a `SessionRetention = 0` case asserting `ExpiresAt = nil` and that a subsequent sweep does not destroy the session.
- `orchestrator/internal/store/resources_test.go:1524, 1604`: keep the positive-TTL sweep tests; add a companion test asserting NULL `expires_at` is not swept.
- New test for `failGenerationBeforeTurn` decoupling: simulate a generation start failure, post another message, assert a fresh generation is allocated and the session remains usable.
- New test for `checkpoint_image_retention` maintenance: insert a `reserved_checkpointed` allocation whose session `last_activity_at` is older than the threshold; run maintenance; assert the allocation is now `reclaimable`.
