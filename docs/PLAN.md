# gVisor Data Agent Harness - Plan

> This is the active roadmap. Current baseline and implementation notes live in [current-status.md](./current-status.md). Phase 7 architecture details live in [phase7/README.md](./phase7/README.md).

## Phases

- [x] **Phase 0**: local LLM harness + Doris connectivity + `vhr_data` schema packaging + runtime selection
- [x] **Phase 1**: manual single sandbox DEMO with `runsc run`
- [x] **Phase 2**: scripted rootfs build, bundle bake, and restore smoke path
- [x] **Phase 3**: Go orchestrator MVP with session API, checkpoint/restore, artifact metadata, and event hub
- [x] **Phase 4**: Next.js workbench with same-origin proxy, SSE event stream, and fallback/refresh behavior
- [x] **Phase 5**: per-container `OutputHub`, stream-parser turn completion routing, and interactive shell sessions
- [x] **Phase 6**: artifact UX hardening, live file tree, and richer previews
- [x] **Phase 7a**: control-plane skeleton — per-generation resources, durable schema, per-generation network and bundle, no shared `phase1-demo` / `phase2-template` state.
- [x] **Phase 7b**: turn execution refactor — Agent Bridge claim/ack, durable turn ledger with `ack_started_at` semantics, durable event log, cold Claude resume, checkpoint-safe restore, and checkpoint policy.
- [ ] **P0 fixes**: rename `harness.session_ttl` to `harness.session_retention` with `0s = no expiry` as default, decouple generation start failure from session failure, add checkpoint image retention.
- [ ] **Phase 9**: configurable harness system prompt, proactive context compaction driven by proxy-reported token usage, versioned system-skills mount.
- [ ] **Phase 10**: multi-user auth, credential storage/rotation/GC, tenant egress policy enforcement, cgroup limits, observability, multi-orchestrator HA.
- [ ] **Phase 11** (future, design only): trajectory → memory → skill pipeline.

## Current Target

The checkpoint-safe Phase 7 baseline is qualified. Active engineering work is the P0 lifetime fix, then Phase 9. Phase 10 (production operations) follows Phase 9. Phase 11 is design-only for now.

### P0: session and runtime lifetime separation

User sessions, conversation history, and workspace files must persist effectively forever. Live gVisor runtime resources (sandbox processes, netns/veth, checkpoint images, `/30` slots) should be flexibly released and reloaded independently of session lifetime. The current code couples these in three places that must be unwound first:

1. Hard rename `harness.session_ttl` to `harness.session_retention`. `0s` is the new default and means no automatic session expiry. No backwards-compat alias — lab configs migrate in one step. The existing `harness.checkpoint.idle_threshold` already drives generation idle lifetime separately and does not need renaming.
2. Decouple generation start failure from session failure. `failGenerationBeforeTurn` currently cascades to `FailSession` unconditionally, making the session permanently terminal even when conversation history and workspace are intact. Only true session-level invariant violations should be terminal; generation start failures should leave the session retryable.
3. Add `harness.reaper.checkpoint_image_retention`. `reserved_checkpointed` allocations move to `reclaimable` once the session `last_activity_at` exceeds the configured retention. Without this, long-idle sessions accumulate checkpoint images and `/30` network slots indefinitely (the reaper today explicitly skips `reserved_checkpointed`).

Detailed design: [p0-session-lifetime.md](./p0-session-lifetime.md).

### Phase 9: agent capability and UX

1. **9a — Configurable harness system prompt.** Inject an operator-controlled system prompt into every session — agent identity (e.g., "BatteryGPT"), capability bounds (no image reading), and sandbox resource constraints (1 GiB memory, no `fetchall()` on wide tables). Propagated through the per-generation control manifest. Detailed design: [phase9/system-prompt.md](./phase9/system-prompt.md).
2. **9b — Proactive context compaction driven by proxy-reported usage.** The pinned `claude-code-proxy` already correlates every upstream request to a session/turn via sandbox source IP and posts a `finish` observation to the orchestrator. Extend the finish payload with token counts; the orchestrator stores them inside the existing `proxy.request.completed` event payload (no schema migration). The orchestrator sums tokens per turn and instructs Claude Code to compact before the deployed model's real context window is exhausted. Detailed design: [phase9/context-compaction.md](./phase9/context-compaction.md).
3. **9c — Versioned system-skills mount.** Read-only `/harness-skills` bind mount with `skills_release_id` and `skills_digest` persisted in the control manifest so checkpoint/restore stays digest-pinned. Skill files stay outside `/workspace`. Detailed design: [phase9/system-skills-mount.md](./phase9/system-skills-mount.md).

### Phase 10: production operations

Scope: multi-user auth, credential storage/rotation/GC, tenant egress policy enforcement, cgroup limits, cleanup/resource observability, multi-orchestrator HA. Re-ordered behind Phase 9 because Phase 9 directly addresses the most immediate user-visible failures (sessions dying mid-conversation from context overflow, missing safety prompts, no shared operational skills) while Phase 10 is operator-facing.

### Phase 11: trajectory → memory → skill pipeline (future)

Design only. Folds raw session trajectories into versioned skills via episode memory, semantic memory, and human-reviewed skill candidates. Sits on top of 9c's versioned-skills release plumbing. Detailed design: [phase11-trajectory-pipeline.md](./phase11-trajectory-pipeline.md). Not currently planned in detail.

## Ongoing Guardrails

Standing constraints that must hold throughout the P0 fixes, Phase 9, Phase 10, and any later work. These are not deliverables — there is no "done" state — but any change that violates one should be revisited before it lands.

1. Maintain the supported Claude Code and shell session paths.
2. Keep Phase 7 release gates blocking for runtime, proxy, or config changes, including the pinned proxy contract, gVisor bridge durability lab, secret permission lab, and live turn-start latency gate.
3. Keep artifact browsing in regression coverage while preserving the existing read-only metadata-backed UX.
4. Defer a second harness adapter until the operational surface and adapter completion contract are stable.

## Current Architecture

```text
Browser
  -> Next.js same-origin proxy
  -> Go orchestrator
  -> gVisor runtime
  -> per-session workspace + long-lived sandbox
```

The browser reads live events from SSE at `/api/events/stream`. The orchestrator still keeps the WebSocket endpoint for compatibility and manual debugging.

The current runtime keeps active generation sandboxes alive across turns and routes user work through the Agent Bridge claim/ack protocol. Turns, lifecycle events, output, proxy request correlation, and generation/resource state are durable in SQLite before in-memory publish.

## What Is Done

Phases 0–7 are complete. Highlights below; full per-phase notes live in [current-status.md](./current-status.md).

- **Phase 0–2**: host capability check (no `/dev/kvm`), `vhr_data` schema packaging, manual rootfs + bundle bake + `runsc` smoke path; standard restore latency in low-hundreds of ms.
- **Phase 3–4**: Go orchestrator with SQLite store, session API, artifact scanning, event hub, checkpoint/restore primitives; Next.js workbench with same-origin proxy and SSE.
- **Phase 5**: per-container `OutputHub` fan-out, stream-json turn completion, PTY-backed shell sessions with interrupt support.
- **Phase 6**: artifact serving hardened (traversal/symlink/non-regular rejection, `artifact.deleted` events), live frontend file tree with search and per-file download, richer previews for Markdown/code/text/images/JSON/CSV/PDF.
- **Phase 7**: per-generation resources and network, strict Phase 7 config schema, immutable mounted secrets, Agent Bridge claim/ack turn execution, durable event log/SSE replay, proxy request correlation, cold fallback, checkpoint-safe restore, checkpoint policy, and release-gate evidence.

## Remaining Risks

- Automatic checkpointing is policy-gated and disabled in the checked-in lab config. The checkpoint/restore path is control-plane-safe, but default enablement should be a deliberate operations decision after retention, resource pressure, and restore SLOs are measured.
- Phase 7 uses `secret_id` / `secret_version` indirection and per-generation mounted secret files. Real upstream credential storage, rotation, and GC remain Phase 10.
- Reclaimable generation resources are retained for `harness.reaper.failed_retention` before physical cleanup. That is intentional for debugging, but production observability should make retained resources visible.

## Notes on Prior Docs

The older phase status documents remain useful as implementation history, but they are no longer the source of truth for current behavior. Use:

- `current-status.md` for the live baseline.
- `architecture.md` for system design.
- `phase7/` for Phase 7 architecture and release qualification history (start at `phase7/README.md`).
- `p0-session-lifetime.md`, `phase9/`, `phase11-trajectory-pipeline.md` for upcoming-work designs.
- `PLAN.md` for roadmap only.
