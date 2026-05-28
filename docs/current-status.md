# Current Status

> Last updated: 2026-05-28
> Scope: current baseline after the Phase 7 checkpoint-safe control-plane refactor, release qualification, and completed P0 lifetime separation.

## Baseline

Harness Platform now has a working end-to-end lab stack:

- Next.js frontend workbench on port `8000`.
- Go orchestrator API on port `8090`.
- gVisor `runsc` runtime using per-generation OCI specs, control manifests, bridge dirs, and network profiles.
- SQLite persistence for sessions, messages, runtime generations, turns, durable events, proxy request context, resources, and artifact metadata.
- Per-session workspace and per-session+driver HOME are provisioned through DataVolume rows with host-side evidence. Sandboxes receive exact `/workspace` and `/agent-home` binds, not parent `/sessions` or `/agent-homes` mounts.
- Claude Code stream-json parsing into durable `emit_output` events, persisted assistant messages, and live UI deltas.
- Shell sessions through the bridge-aware shell shim, with shell output persisted as assistant messages and interrupt support for running turns.
- Phase 7 typed `config/harness.yaml` is loaded with strict YAML validation and drives per-generation network, probe, bridge, reaper, and checkpoint settings. Phase 8 removes the legacy sandbox secret config keys from the active schema.
- Agent Bridge claim/ack is the live turn execution path. Turns transition through `queued`, `leased`, `running`, and terminal states with CAS fencing and durable events.
- Checkpoint/restore primitives are behind the Phase 7 control plane. Automatic idle checkpointing is policy-gated and disabled in the checked-in lab config.
- Session/history retention is decoupled from runtime resource lifetime. `harness.session_retention: 0s` is the checked-in default, active sessions store `expires_at = NULL`, and retryable generation/runtime/turn failures no longer make the session terminal.
- Checkpoint image retirement is controlled by `harness.reaper.checkpoint_image_retention`; checkpoint-retired and restore-fallback generations clear checkpoint metadata through durable non-terminal events and can cold-start replacement generations for the same session.
- `harness.max_sessions` is a non-terminal session ceiling independent of live `/30` pool capacity. `/api/quota` reports the session ceiling and live pool ceiling separately, and `DELETE /api/sessions/<id>` closes a session while preserving history/workspace state.
- Artifact browsing is a metadata-backed live file tree with search, safe downloads, delete/rename event handling, and richer previews for Markdown, code, text, images, JSON, CSV/TSV, and PDF.

## Notable Commits And Qualification

### P0 Lifetime Separation - `20a8c07`

The P0 lifetime baseline is complete at `20a8c07`. The roadmap now treats
Phase 8 runtime isolation hardening as active work on top of long-lived
sessions. Key commits in this slice:

- `5cf0ddb fix(config): rename session ttl to retention` - renames the user-facing retention knob and makes `0s` the no-expiry default.
- `c46c3db feat(config): add checkpoint image retention` and `fd01372 feat(store): retire expired checkpoint metadata` - add explicit checkpoint-image retirement.
- `bb28e0c fix(server): retire restore fallback checkpoints` and `62ec903 fix(server): refetch stale checkpoint restore` - make restore fallback non-terminal and retryable.
- `c1c59a4 fix(bridge): keep failed turns session-retryable` and `1f98f47 test(bridge): cover claude turn failures` - keep agent execution failures at turn scope.
- `02c0384 fix(server): renew generation start leases`, `b538ef0 fix(server): require runtime cleanup before recovery`, and `7f9dcfa fix(store): reject stale generation start failures` - tighten retryable generation lifecycle fencing.
- `fc08a33 fix(server): clean resources on session close`, `0e317eb fix(config): decouple session ceiling from pool capacity`, and `dac665c feat(frontend): expose session close action` - make retained-session quota recoverable without deleting history.

### Phase 7 Release Qualification - `d0cdaf6`

The Phase 7 lab candidate is qualified at `d0cdaf6` after these final follow-up commits:

- `a7754da fix(runtime): tune bridge polling for live latency` - passes the 5 ms bridge poll interval through to sandbox bridge clients and keeps live turn-start latency under the 50 ms budget.
- `108aa65 docs(plan): mark phase7 qualified` - updates the roadmap/status docs from pre-Phase-7 wording to the qualified Phase 7 baseline.
- `d0cdaf6 fix(runtime): preserve secret root owner` - historical Phase 7 qualification work for the old sandbox secret root. Phase 8 supersedes that path by rejecting the legacy secret config keys.

Latest release evidence: `/tmp/harness-phase7-external-gates.json`.

Last observed result on this host: `passed`, clean worktree, commit `d0cdaf608b9397e5bcae7f93daf2b6550a5654c5`, live turn-start max `27.284 ms`.

### `e8b84f0` - Same-Origin SSE Event Stream

The frontend no longer opens a browser WebSocket directly to the orchestrator. It uses `EventSource` against the frontend origin:

```text
Browser -> Next.js route handler -> Orchestrator /api/events/stream
```

The orchestrator still serves the legacy WebSocket endpoint at `/api/events`, but the frontend path is now:

- `GET /api/events/stream?session_id=<id>` for SSE events.
- `GET /api/healthz` and `/api/*` through the same-origin proxy.
- Polling of messages/session/artifacts after message submission, so the UI can recover state if an SSE frame is missed.

### `9b803b6` - Runtime Output Routed Through Session State

Runtime output is now decoupled from a single callback:

- Each active container owns an `OutputHub`.
- stdout/stderr scanner goroutines publish `OutputEvent` values to that hub.
- Each `runSession()` call subscribes while its current turn is active.
- The stream parser closes the turn when Claude emits a `result` or `error` frame.
- Assistant text is persisted in SQLite and published as `agent.message`; partial Claude text is published as `agent.delta`.
- Runtime lifecycle lines use a separate `runtime` stream and become `system.status` events.

This fixes the previous multi-turn issue where only the first `Start()` callback could receive container output.

### `051f251` - Interactive Shell Sessions

The frontend now exposes `Shell` as a first-class session mode instead of a smoke-only shortcut:

- `sh` sessions run through the PTY-backed `harness-shell-agent` shim.
- Shell turns emit `harness.shell_output` and `harness.turn_done` frames.
- Shell output is persisted as assistant messages and published as `agent.output`.
- `POST /api/sessions/<id>/interrupt` interrupts a running shell turn.
- The session picker offers `Shell` and `Agent`, where `Agent` maps to Claude Code.

### `a422e44` - Checkpoint Safety Recovery

The orchestrator no longer treats automatic idle checkpoint/restore as the default path:

- `MonitorIdleSessions()` reconciles `checkpointing` and `checkpointed` sessions on startup.
- At that point, automatic idle checkpointing was disabled because `runsc restore` could not reliably reconnect the long-lived stdin turn channel. Phase 7 replaces that turn channel with the Agent Bridge.
- `Runtime.Start()` only restores from checkpoint when `RestoreFromCheckpoint` is explicitly enabled.
- Replacement container cleanup only removes the current container instance, avoiding stale cleanup races.

The practical result is that active sessions stay on the live-container path, and stale checkpoint states are recovered back to usable session states instead of leaving the UI stuck.

### `90a5f32` - Phase 6 Artifact Browser

Artifact handling moved from a flat metadata list to a read-only file browser:

- Artifact downloads reject traversal, symlink escape, directories, and non-regular files.
- Host-side artifact scanning skips symlink artifacts.
- File/directory remove and rename events delete stored metadata and publish `artifact.deleted`.
- The frontend derives a live folder tree from artifact metadata and keeps open tabs in sync when files disappear.
- Artifact previews now cover Markdown, code, text, images, JSON, CSV/TSV, and PDF.

### Phase 7 Config Loader

The current codebase loads `config/harness.yaml` through the Phase 7 typed `harness:` schema. The full file shape and per-field semantics live in [architecture.md → Configuration](./architecture.md#configuration). Legacy `runtime:` / `claude:`-only files still parse for compatibility, but cannot be mixed with `harness:`.

### Phase 7 Release Qualification

Phase 7 release qualification is evidence-producing. The current candidate records deterministic repo gates, the pinned `claude-code-proxy` contract, the gVisor bridge durability lab, the secret permission lab, and live turn-start latency. The latest evidence file is `/tmp/harness-phase7-external-gates.json` and is tied to commit `d0cdaf608b9397e5bcae7f93daf2b6550a5654c5`.

## Current Flow

```text
POST /api/sessions
  -> session status: created

POST /api/sessions/<id>/messages
  -> persist user message
  -> status: running_active
  -> ensure active runtime generation
     -> cold path: allocate per-generation resources and runsc run
     -> restore path: recreate compatible resources and runsc restore
     -> live path: reuse the active bridge generation
  -> bridge claim_next_turn / ack_turn_started
  -> bridge emit_output / ack_turn_completed
  -> stream parser persists assistant message from durable output
  -> artifact watcher scans workspace
  -> status: running_idle

Shell turns follow the same bridge lifecycle, but they complete through the shell runner and can be interrupted with `POST /api/sessions/<id>/interrupt`.

Idle monitor
  -> reconcile stale checkpointing/checkpointed rows
  -> checkpoint eligible idle generations only when policy is enabled
```

Canonical session statuses and per-state semantics live in [architecture.md → Session State Machine](./architecture.md#session-state-machine). Note: `running`, `idle`, and `completed` are not current session statuses.

## Public Interfaces

HTTP routes, SSE/WebSocket endpoints, and the canonical event-name set are documented in [architecture.md → API Surface](./architecture.md#api-surface) and [architecture.md → Event Model](./architecture.md#event-model). This document does not duplicate those lists.

## Current Constraints

- Claude Code is the primary supported analysis path.
- `Shell` is the supported interactive command path and has its own `turn_done`/`interrupt` contract; future adapters still need their own completion protocol before they are first-class multi-turn citizens.
- The active Go runtime launches `runsc` directly. `bundle/bake-bundle.sh` and
  `bundle/restore-sandbox.sh` are quarantined legacy Phase 2 smoke tools: they
  fail closed and are not Phase 8 release evidence.
- The current Go runtime uses `runsc -network sandbox -overlay2 none` with per-generation network profiles. The runtime creates the allocated netns/veth pair, configures host and sandbox addresses from the persisted `/30`, applies the static lab egress allow-list, writes the stable sandbox-visible model proxy alias into the control manifest, and maps that alias through the generated `/etc/hosts` projection.
- Runtime specs now use read-only rootfs, exact `/workspace` and `/agent-home` DataVolume binds, exact `/harness-control` and bridge binds, no `/harness-secrets`, no parent `/sessions` or `/agent-homes` mounts, empty OCI capabilities, and `noNewPrivileges`.
- Claude provider credentials are host/proxy-side. Sandbox startup probes only health/bridge readiness before turns; model endpoints require a committed active model context, source-IP match, contract entitlement, and proxy correlation through the authenticated UDS.
- Shell and Claude bridge claim-loop paths run under the configured non-root sandbox identity. Legacy session `workspace`, `agent_home_path`, and `restore_id` columns remain internal compatibility storage and are omitted from public API/event DTOs.
- Phase 8 is not release-complete until destructive cutover, proxy re-pin evidence, host reconciliation evidence, and every release gate in `docs/phase8/release-gates.md` passes on the target lab host.
- Automatic idle checkpointing is disabled by the checked-in policy. It can be enabled only after operators accept the measured restore/resource-retention behavior for the lab.
- Reclaimable runtime resources are retained for `harness.reaper.failed_retention` before physical cleanup, so recently failed/destroyed generations can remain visible briefly by design.
- Phase 8 is planned as a destructive clean cutover for this lab, not an
  in-place compatibility migration. Old sessions, workspaces, agent homes,
  runtime rows, checkpoints, and prepared bundles are wiped or quarantined from
  active roots before `sandbox-isolation-v1` is enabled. Phase 8 also replaces
  split network/resource cleanup state with one `runtime_resource_instances`
  lifecycle; Phase 7 session/turn/generation execution semantics remain
  authoritative.
- Retained non-terminal sessions count toward `harness.max_sessions` even when they have no live runtime resources. Use the workbench close action or `DELETE /api/sessions/<id>` to free session quota while keeping history and workspace files.
- Artifact metadata is recorded by host-side scanning/watching and rendered as a read-only live file tree. Direct file mutation operations remain outside the UI; use the sandbox agent or shell path to create, rename, or delete files.
- Auth is lab shared-password cookie auth when `HARNESS_LAB_PASSWORD` is set.

## Next Architecture Target

Active architecture work is Phase 8: runtime isolation hardening. It narrows
each sandbox to exact per-session/per-driver mounts, runs shell non-root, makes
the rootfs read-only, moves upstream model credentials host-side, and unifies
generation-owned host resource reconciliation before allocator reuse. Phase 9
agent capability work follows Phase 8; production auth/authorization, real
secret storage and rotation, tenant-level egress policy, resource limits,
observability, and multi-orchestrator HA are Phase 10.

## Checks

Backend:

```bash
cd orchestrator
go test ./...
```

Frontend:

```bash
cd frontend
npm run lint
npm run typecheck
npm test
npm run build
```

Sandbox bridge client:

```bash
python3 -m unittest sandbox-image/tests/test_harness_bridge_client.py
```

Phase 7 release gates:

```bash
PHASE7_LATENCY_SESSION_IDS=<prewarmed_running_idle_session> \
PHASE7_LATENCY_CONTENT='Reply exactly OK. Probe {nonce}' \
tools/phase7/release-gates.py \
  --include-proxy \
  --include-bridge-lab \
  --include-secret-lab \
  --include-live-latency \
  --output /tmp/harness-phase7-external-gates.json
```
