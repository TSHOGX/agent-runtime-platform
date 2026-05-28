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
- [x] **P0 fixes**: rename `harness.session_ttl` to `harness.session_retention` with `0s = no expiry` as default, decouple retryable runtime/turn failures from terminal session failure, add checkpoint image retention, and close the generation cleanup/quota documentation gaps.
- [ ] **Phase 8**: runtime isolation hardening — exact per-session/per-driver mounts, unified generation resource reconciliation, non-root shell, read-only rootfs, and host-side model credential boundary.
- [ ] **Phase 9**: configurable harness system prompt, proactive context compaction driven by proxy-reported token usage, system-skills mount, harness-managed Claude Code settings (hooks + remote MCP).
- [ ] **Phase 10**: multi-user auth, credential storage/rotation/GC, tenant egress policy enforcement, cgroup limits, observability, multi-orchestrator HA.
- [ ] **Phase 11** (future, design only): trajectory → memory → skill pipeline.

## Current Target

The checkpoint-safe Phase 7 baseline and P0 lifetime separation are qualified.
Active engineering work is Phase 8 runtime isolation hardening. Phase 9 agent
capability work is deliberately behind Phase 8 because 9c/9d add more
sandbox-visible content and managed settings; those should not land on top of
the current over-broad mount and credential boundary. Phase 10 (production
operations) follows Phase 9. Phase 11 is design-only for now.

### P0: session and runtime lifetime separation

User sessions, conversation history, and workspace files now persist independently from live gVisor runtime resources (sandbox processes, netns/veth, checkpoint images, `/30` slots). P0 established this contract through these completed changes:

1. Renamed `harness.session_ttl` to `harness.session_retention`. `0s` is the default and means no automatic session expiry. No backwards-compat alias — lab configs migrate in one step. The existing `harness.checkpoint.idle_threshold` continues to drive generation idle lifetime separately.
2. Decoupled retryable runtime failures from terminal session failure. Generation start failures, restore fallback, and failed/canceled bridge `ack_turn_completed` outcomes leave the session retryable or correctly input-blocking, publish durable non-terminal events, and keep the frontend from marking the session failed.
3. Added `harness.reaper.checkpoint_image_retention`. Expired checkpointed generations are atomically retired before their `reserved_checkpointed` allocations move to `reclaimable`, and retirement events carry committed session fields so the frontend clears stale checkpoint/restore metadata.
4. Finished generation cleanup coverage. Checkpoint-retired generations become physically destroyable without waiting for ordinary failed-retention, and `DestroyGenerationResources` removes generation-scoped checkpoint/control/runtime/bridge/log directories independently of sandbox network metadata.
5. Documented `harness.max_sessions` with `session_retention: 0s` as an explicit P0 release constraint. The cap remains a non-terminal session ceiling, and docs plus UI/API close paths make the behavior recoverable and visible.

Completed design record and regression checklist: [p0-session-lifetime.md](./p0-session-lifetime.md).

### Phase 8: runtime isolation hardening

Phase 8 closes the sandbox visibility and credential-delivery gaps captured in
`.scratch/runtime-security-followups.md`, excluding only the R5 microVM-vs-gVisor
residual risk. The current ECS host has no KVM path, so this phase keeps direct
`runsc` workers and tightens the gVisor sandbox contract instead of switching to
Firecracker/Kata.

The target architecture is contract-first:

```text
Config + Session + Driver + Generation
  -> SandboxContractCompiler
  -> immutable SandboxContract + digest
  -> MountPlan + DataVolume provisioning
  -> RuntimeAdapter(runsc OCI spec)
  -> RuntimeResourceInstance state
  -> HostStateReconciler
  -> ModelAccessAuthorizer
```

Each generation has one persisted `sandbox-isolation-v1` contract. Runtime
launch, restore, cleanup, checkpointing, bridge polling, proxy authorization,
migration, and release evidence load that contract instead of reconstructing
security facts from paths, env vars, session IDs, or current config.

The sandbox-visible layout is exact and small: `/workspace`, `/agent-home`,
`/harness-control`, `/harness-control/bridge`, optional read-only content mounts,
and explicit tmp/cache mounts. Parent `/sessions` and `/agent-homes` mounts are
removed. Configured roots must be canonical and mutually disjoint so an exact
workspace bind cannot expose DB, run, checkpoint, prepared-bundle, rootfs, schema,
or future content roots.

Phase 8 is a destructive clean cutover for this lab. Old sessions, workspaces,
agent homes, runtime rows, checkpoint images, prepared bundles, and legacy
manifests are wiped or quarantined from active roots instead of upgraded. New
sessions start from fresh `session_workspaces` and per-driver
`session_driver_homes` rows with host-trusted provisioning evidence.

Runtime host-resource cleanup is unified under
`runtime_resource_instances.state`. Split Phase 7 resource states become
diagnostic mirrors only; allocator reuse requires `absent_verified`, never a
direct `destroyed` row. `runsc_container_id` is generation-scoped and read from
the resource instance, not from `sessions.restore_id`.

Model credentials move fully host-side. Sandbox model requests are authorized by
the proxy from actual TCP peer source IP, live turn context, durable driver
entitlement, copied active-context entitlement, live resource state, and the
verified contract. Sandbox-sent source-IP claims are diagnostics only. Phase 8
does not add a sandbox-visible proxy-token path.

Security-relevant changes to sandbox identity, root paths, runtime adapter
requirements, rootfs/content digests, credential policy, or
`model_access_allowed` are allocation fences: affected generations are
drained/retired/reallocated rather than mutated in place.

Detailed design entrypoint and document map:
[phase8/README.md](./phase8/README.md).

### Phase 9: agent capability and UX

Phase 9 starts only after Phase 8's mount and credential boundary is in place;
this applies to 9a and 9b as well as the sandbox-content work in 9c/9d.

1. **9a — Configurable harness system prompt.** Inject an operator-controlled system prompt into every session — agent identity (e.g., "BatteryGPT"), capability bounds (no image reading), and sandbox resource constraints (1 GiB memory, no `fetchall()` on wide tables). Propagated through the per-generation control manifest. Detailed design: [phase9/system-prompt.md](./phase9/system-prompt.md).
2. **9b — Proactive context compaction driven by proxy-reported usage.** Use the Phase 8 proxy correlation path to tie upstream requests to a session/turn, extend the proxy `finish` observation with token counts, and store them inside the existing `proxy.request.completed` event payload (no schema migration). The orchestrator sums tokens per turn and instructs Claude Code to compact before the deployed model's real context window is exhausted. Detailed design: [phase9/context-compaction.md](./phase9/context-compaction.md).
3. **9c — System-skills mount.** Read-only `/harness-skills` bind mount with `skills_digest` persisted in the control manifest so checkpoint/restore stays digest-pinned. Skill content lives in this repository under `sandbox-image/system-skills/`; versioning is the codebase's git history, not a separate `releases/` tree. Skill files stay outside `/workspace`. Detailed design: [phase9/system-skills-mount.md](./phase9/system-skills-mount.md).
4. **9d — Harness-managed Claude Code settings (hooks + remote MCP).** Render `/etc/claude-code/managed-settings.json` inside the sandbox from operator-controlled config in this repo. Carries `hooks` (with optional `disableAllHooks` / `allowManagedHooksOnly` policy gates) and `mcpServers` (remote `http`/`sse` transport only — MCP servers are deployed elsewhere). Credential-bearing MCP delivery requires a separate broker/token design after Phase 8; managed settings must not put upstream bearer tokens into Claude-visible files or env. `managed_settings_digest` joins the projected control-manifest digest. Detailed design: [phase9/managed-settings.md](./phase9/managed-settings.md).

### Phase 10: production operations

Scope: multi-user auth, credential storage/rotation/GC, tenant egress policy enforcement, cgroup limits, cleanup/resource observability, multi-orchestrator HA. Re-ordered behind Phase 9 because Phase 9 directly addresses the most immediate user-visible failures (sessions dying mid-conversation from context overflow, missing safety prompts, no shared operational skills) while Phase 10 is operator-facing.

### Phase 11: trajectory → memory → skill pipeline (future)

Design only. Folds raw session trajectories into versioned skills via episode memory, semantic memory, and human-reviewed skill candidates. Sits on top of 9c's skills mount (a `releases/` tree and human-review flow are part of Phase 11, not 9c). Detailed design: [phase11-trajectory-pipeline.md](./phase11-trajectory-pipeline.md). Not currently planned in detail.

## Ongoing Guardrails

Standing constraints that must hold after P0 and throughout Phase 8, Phase 9,
Phase 10, and any later work. These are not deliverables — there is no "done"
state — but any change that violates one should be revisited before it lands.

1. Maintain the supported Claude Code and shell session paths.
2. Keep Phase 7 release gates blocking for runtime, proxy, or config changes
   until a later phase explicitly retires or replaces a gate. Phase 8 must
   preserve the Phase 7 session/turn/bridge/event semantics, keep the gVisor
   bridge durability and live turn-start latency gates blocking, and replace
   Phase 7 gates that depend on removed contracts: the legacy sandbox secret
   permission lab and authenticated malformed `/v1/messages` proxy probe are
   retired only after the named Phase 8 host-only credential,
   no-`/harness-secrets`, stable proxy alias, active-context authorization, and
   re-pinned proxy gates pass.
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

## Current Architecture

```text
Browser
  -> Next.js same-origin proxy
  -> Go orchestrator
  -> gVisor runtime
  -> long-lived sandbox with exact /workspace and /agent-home binds
```

The active runtime no longer mounts parent `/sessions` or `/agent-homes`
directories into the sandbox. Each generation receives exact DataVolume-backed
`/workspace` and `/agent-home` binds, plus its generation-scoped
`/harness-control` surfaces and explicit read-only content mounts.

The browser reads live events from SSE at `/api/events/stream`. The orchestrator still keeps the WebSocket endpoint for compatibility and manual debugging.

The current runtime keeps active generation sandboxes alive across turns and routes user work through the Agent Bridge claim/ack protocol. Turns, lifecycle events, output, proxy request correlation, and generation/resource state are durable in SQLite before in-memory publish.

## What Is Done

Phases 0–7 and P0 lifetime separation are complete. Highlights below; full per-phase notes live in [current-status.md](./current-status.md).

- **Phase 0–2**: host capability check (no `/dev/kvm`), `vhr_data` schema packaging, manual rootfs + bundle bake + `runsc` smoke path; standard restore latency in low-hundreds of ms.
- **Phase 3–4**: Go orchestrator with SQLite store, session API, artifact scanning, event hub, checkpoint/restore primitives; Next.js workbench with same-origin proxy and SSE.
- **Phase 5**: per-container `OutputHub` fan-out, stream-json turn completion, PTY-backed shell sessions with interrupt support.
- **Phase 6**: artifact serving hardened (traversal/symlink/non-regular rejection, `artifact.deleted` events), live frontend file tree with search and per-file download, richer previews for Markdown/code/text/images/JSON/CSV/PDF.
- **Phase 7**: per-generation resources and network, strict Phase 7 config schema, immutable mounted secrets, Agent Bridge claim/ack turn execution, durable event log/SSE replay, proxy request correlation, cold fallback, checkpoint-safe restore, checkpoint policy, and release-gate evidence.

## Remaining Risks

- Automatic checkpointing is policy-gated and disabled in the checked-in lab config. The checkpoint/restore path is control-plane-safe, but default enablement should be a deliberate operations decision after retention, resource pressure, and restore SLOs are measured.
- Phase 8 runtime isolation is active. Destructive cutover, host
  reconciliation, deterministic release, rootfs image, and proxy contract
  evidence have passed on the target host, but Phase 8 is not release-complete
  until adversarial lab and the full `docs/phase8/release-gates.md` audit pass
  with evidence.
- Provider credentials are host/proxy-side for the active runtime. Real upstream
  credential storage, rotation, and GC remain Phase 10.
- Reclaimable generation resources are retained for `harness.reaper.failed_retention` before physical cleanup. That is intentional for debugging, but production observability should make retained resources visible.

## Notes on Prior Docs

The older phase status documents remain useful as implementation history, but they are no longer the source of truth for current behavior. Use:

- `current-status.md` for the live baseline.
- `architecture.md` for system design.
- `phase7/` for Phase 7 architecture and release qualification history (start at `phase7/README.md`).
- `p0-session-lifetime.md`, `phase9/`, `phase11-trajectory-pipeline.md` for upcoming-work designs.
- `PLAN.md` for roadmap only.
