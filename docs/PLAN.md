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
- [x] **Pre-Phase 9 cleanup**: model proxy port moved into `harness.model_proxy`, main/publish runtime resources isolated, and product-visible naming kept on Agent Runtime Platform wording. Details: [pre-phase-runtime-cleanup.md](./pre-phase-runtime-cleanup.md).
- [x] **Phase 9**: Agent Driver abstraction, runtime provider contract, deployment-selected agent driver, and Pi Agent integration.
- [ ] **Phase 10**: configurable agent system prompt, proactive context compaction driven by proxy-reported token usage, system-skills mount, control-plane-managed driver settings/hooks/MCP.
- [ ] **Phase 11**: multi-user auth, credential storage/rotation/GC, tenant egress policy enforcement, cgroup limits, observability, multi-orchestrator HA.
- [ ] **Phase 12** (future, design only): trajectory → memory → skill pipeline.

## Current Target

The checkpoint-safe Phase 7 baseline, P0 lifetime separation, Phase 8 runtime
isolation baseline, pre-Phase 9 runtime cleanup gate, and Phase 9 driver/Pi
baseline are complete. Active engineering work moves to Phase 10 agent
capability work behind the driver adapters. Phase 11 production operations
follows Phase 10. Phase 12 is design-only for now.

## Completed Baselines

Implementation history and release evidence for completed work live outside
this roadmap:

- Current baseline and qualification notes: [current-status.md](./current-status.md).
- P0 session/runtime lifetime separation: [p0-session-lifetime.md](./p0-session-lifetime.md).
- Phase 8 runtime isolation design and release evidence:
  [phase8/README.md](./phase8/README.md).
- Pre-Phase 9 runtime cleanup and local publish isolation:
  [pre-phase-runtime-cleanup.md](./pre-phase-runtime-cleanup.md).
- Phase 9 driver/provider contract and Pi integration:
  [phase9/README.md](./phase9/README.md).

## Phase 9: Agent Driver and Pi integration

Phase 9 made "agent" a deployment-selected driver contract instead of a
Claude-shaped string branch. Detailed design and release gates:
[phase9/README.md](./phase9/README.md).
Phase 9 uses an automatic destructive cutover for obsolete pre-9a state: old
rows may be deleted and constrained SQLite tables rebuilt without a manual
data-preservation gate, but live provider/isolation resources must be proven
absent or durably quarantined before their ownership rows are removed.

1. Add `AgentDriverSpec` for Claude Code, shell, and Pi. Do not register
   legacy `claude` as a runtime alias; only the temporary 9a/9b public API
   boundary may translate it to `claude_code` before the 9c mode cutover, and
   only the protocol-v1 sandbox projection may emit `claude` to the current
   runner until 9d replaces that bridge path.
2. Add `RuntimeProviderSpec` for `local_runsc` and validate driver capabilities before allocation.
3. Move model/provider config into `harness.agents` and `harness.model_profiles`; `harness.default_agent` selects the deployed driver.
4. Keep the frontend product surface as "Agent" and deployment-capable "Shell"; users do not choose or see whether "Agent" is Claude Code, Pi, or another deployed driver.
5. Build the sandbox image from the deployed driver set so only selected driver CLIs are pinned into the rootfs; deployments that omit `sh` must not advertise Shell.
6. Add generic driver state, runtime profile, sandbox contract, runner, and output-normalizer slots.
7. Add Pi as a long-lived RPC driver through the Phase 8 model-proxy boundary.

## Phase 10: agent capability and UX

Phase 10 starts from Phase 9's driver contract. These features must use driver
adapters, with Claude and Pi renderers where supported.

1. **10a — Configurable agent system prompt.** Inject an operator-controlled system prompt into every session — agent identity (e.g., "BatteryGPT"), capability bounds (no image reading), and sandbox resource constraints (1 GiB memory, no `fetchall()` on wide tables). Detailed design: [phase10/system-prompt.md](./phase10/system-prompt.md).
2. **10b — Proactive context compaction driven by proxy-reported usage.** Use the Phase 8 proxy correlation path and driver compaction adapters to compact before the deployed model's real context window is exhausted. Detailed design: [phase10/context-compaction.md](./phase10/context-compaction.md).
3. **10c — System-skills mount.** Read-only `/harness-skills` bind mount with `skills_digest` persisted in the control manifest and exposed through per-driver discovery adapters. Detailed design: [phase10/system-skills-mount.md](./phase10/system-skills-mount.md).
4. **10d — Control-plane-managed driver settings, hooks, and remote MCP.** Render non-secret policy/MCP config through per-driver adapters. Credential-bearing MCP delivery requires a separate broker/token design. Detailed design: [phase10/managed-settings.md](./phase10/managed-settings.md).

## Phase 11: production operations

Scope: multi-user auth, credential storage/rotation/GC, tenant egress policy enforcement, cgroup limits, cleanup/resource observability, multi-orchestrator HA.

## Phase 12: trajectory → memory → skill pipeline (future)

Design only. Folds raw session trajectories into reviewed skills via episode memory, semantic memory, and human-reviewed skill candidates. Sits on top of 10c's skills mount; a human-review flow and optional `releases/` layer are Phase 12 concerns, not 10c. Detailed design: [phase12-trajectory-pipeline.md](./phase12-trajectory-pipeline.md). Not currently planned in detail.

## Ongoing Guardrails

Standing constraints that must hold after P0, Phase 8, and Phase 9, throughout
Phase 10, Phase 11, and any later work. These are not deliverables — there is no "done"
state — but any change that violates one should be revisited before it lands.

1. Maintain the supported Claude Code and shell session paths.
2. Keep Phase 7 release gates blocking for runtime, proxy, or config changes
   until a later phase explicitly retires or replaces a gate. Phase 8's gate
   compatibility and retired-gate mapping live in
   [phase8/README.md](./phase8/README.md#phase-7-boundary) and
   [phase8/release-gates.md](./phase8/release-gates.md).
3. Keep artifact browsing in regression coverage while preserving the existing read-only metadata-backed UX.
4. Do not add driver-specific one-off branches. New agent adapters, including
   Pi, must enter through the Phase 9 driver/provider contracts and satisfy the
   adapter release gates before deployment.
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
