# Agent Runtime Platform - Plan

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
- [x] **P0 fixes**: rename `harness.session_ttl` to `harness.session_retention` with `0s = no expiry` as default, decouple retryable runtime/turn failures from terminal session failure, add checkpoint image retention, and close the generation cleanup/quota documentation gaps.
- [x] **Phase 8**: runtime isolation hardening — exact per-session/per-driver mounts, unified generation resource reconciliation, non-root shell, read-only rootfs, and host-side model credential boundary.
- [ ] **Phase 9**: configurable agent system prompt, proactive context compaction driven by proxy-reported token usage, system-skills mount, control-plane-managed Claude Code settings (hooks + remote MCP).
- [ ] **Phase 10**: multi-user auth, credential storage/rotation/GC, tenant egress policy enforcement, cgroup limits, observability, multi-orchestrator HA.
- [ ] **Phase 11** (future, design only): trajectory → memory → skill pipeline.

## Current Target

The checkpoint-safe Phase 7 baseline, P0 lifetime separation, and Phase 8
runtime isolation baseline are qualified. Active engineering work moves to
Phase 9 agent capability work on top of the Phase 8 mount and credential
boundary. Phase 10 (production operations) follows Phase 9. Phase 11 is
design-only for now.

## Completed Baselines

Implementation history and release evidence for completed work live outside
this roadmap:

- Current baseline and qualification notes: [current-status.md](./current-status.md).
- P0 session/runtime lifetime separation: [p0-session-lifetime.md](./p0-session-lifetime.md).
- Phase 8 runtime isolation design and release evidence:
  [phase8/README.md](./phase8/README.md).

## Phase 9: agent capability and UX

Phase 9 starts from the completed Phase 8 mount and credential boundary; this
applies to 9a and 9b as well as the sandbox-content work in 9c/9d.

1. **9a — Configurable agent system prompt.** Inject an operator-controlled system prompt into every session — agent identity (e.g., "BatteryGPT"), capability bounds (no image reading), and sandbox resource constraints (1 GiB memory, no `fetchall()` on wide tables). Propagated through the per-generation control manifest. Detailed design: [phase9/system-prompt.md](./phase9/system-prompt.md).
2. **9b — Proactive context compaction driven by proxy-reported usage.** Use the Phase 8 proxy correlation path to tie upstream requests to a session/turn, extend the proxy `finish` observation with token counts, and store them inside the existing `proxy.request.completed` event payload (no schema migration). The orchestrator sums tokens per turn and instructs Claude Code to compact before the deployed model's real context window is exhausted. Detailed design: [phase9/context-compaction.md](./phase9/context-compaction.md).
3. **9c — System-skills mount.** Read-only `/harness-skills` bind mount with `skills_digest` persisted in the control manifest so checkpoint/restore stays digest-pinned. Skill content lives in this repository under `sandbox-image/system-skills/`; versioning is the codebase's git history, not a separate `releases/` tree. Skill files stay outside `/workspace`. Detailed design: [phase9/system-skills-mount.md](./phase9/system-skills-mount.md).
4. **9d — Control-plane-managed Claude Code settings (hooks + remote MCP).** Render `/etc/claude-code/managed-settings.json` inside the sandbox from operator-controlled config in this repo. Carries `hooks` (with optional `disableAllHooks` / `allowManagedHooksOnly` policy gates) and `mcpServers` (remote `http`/`sse` transport only — MCP servers are deployed elsewhere). Credential-bearing MCP delivery requires a separate broker/token design; managed settings must not put upstream bearer tokens into Claude-visible files or env. `managed_settings_digest` joins the projected control-manifest digest. Detailed design: [phase9/managed-settings.md](./phase9/managed-settings.md).

## Phase 10: production operations

Scope: multi-user auth, credential storage/rotation/GC, tenant egress policy enforcement, cgroup limits, cleanup/resource observability, multi-orchestrator HA. Re-ordered behind Phase 9 because Phase 9 directly addresses the most immediate user-visible failures (sessions dying mid-conversation from context overflow, missing safety prompts, no shared operational skills) while Phase 10 is operator-facing.

## Phase 11: trajectory → memory → skill pipeline (future)

Design only. Folds raw session trajectories into versioned skills via episode memory, semantic memory, and human-reviewed skill candidates. Sits on top of 9c's skills mount (a `releases/` tree and human-review flow are part of Phase 11, not 9c). Detailed design: [phase11-trajectory-pipeline.md](./phase11-trajectory-pipeline.md). Not currently planned in detail.

## Ongoing Guardrails

Standing constraints that must hold after P0 and Phase 8, throughout Phase 9,
Phase 10, and any later work. These are not deliverables — there is no "done"
state — but any change that violates one should be revisited before it lands.

1. Maintain the supported Claude Code and shell session paths.
2. Keep Phase 7 release gates blocking for runtime, proxy, or config changes
   until a later phase explicitly retires or replaces a gate. Phase 8's gate
   compatibility and retired-gate mapping live in
   [phase8/README.md](./phase8/README.md#phase-7-boundary) and
   [phase8/release-gates.md](./phase8/release-gates.md).
3. Keep artifact browsing in regression coverage while preserving the existing read-only metadata-backed UX.
4. Defer a second harness adapter until the operational surface and adapter completion contract are stable.
5. Treat bridge clients, turn runners, and sandboxes as restartable at any
   turn boundary. No correctness rule may depend only on in-process flags such
   as "first turn"; Claude logical resume must be derived from durable session
   state, control-manifest intent, or driver-home evidence.
6. Keep the sandbox-to-proxy compatibility key as an explicit contract. A proxy
   health check is not enough: gates must prove the key mode used by Claude
   Code (`no key` or the fixed dummy key) is accepted or ignored exactly as the
   pinned proxy expects, and that model dispatch still requires active turn
   context and contract authorization.
7. After changing files under `sandbox-image/files/`, rebuild or overlay-sync
   the active rootfs before live testing. The repo overlay is source of truth,
   but gVisor launches the files currently present under `sandbox-image/rootfs`
   or the configured `HARNESS_ROOTFS_PATH`.
8. For runtime, bridge, Claude CLI, proxy, or session-lifecycle changes, run a
   live two-turn Claude smoke on a fresh session and verify both turns complete
   under the same Claude session UUID.
