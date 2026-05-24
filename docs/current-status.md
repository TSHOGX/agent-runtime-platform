# Current Status

> Last updated: 2026-05-22
> Scope: current baseline after the stream routing, explicit proxy config, sandbox network, checkpoint-safety changes, and Phase 6 artifact browser hardening.

## Baseline

Harness Platform now has a working end-to-end lab stack:

- Next.js frontend workbench on port `8000`.
- Go orchestrator API on port `8090`.
- gVisor `runsc` runtime using the baked Phase 2 OCI bundle.
- SQLite persistence for sessions, messages, and artifact metadata.
- Per-session workspace under `/var/lib/harness/sessions/<session_id>`.
- Per-session Claude HOME under `/var/lib/harness/agent-homes/<session_id>`, mounted in gVisor as `/agent-homes/<session_id>` and kept outside `/workspace`.
- Claude Code stream-json parsing into persisted assistant messages and live UI deltas.
- PTY-backed shell sessions through `harness-shell-agent`, with shell output persisted as assistant messages and interrupt support for running turns.
- Phase 7 typed `config/harness.yaml` is loaded with strict YAML validation; the current runtime still uses the local Claude proxy defaults with sandbox networking as the default path.
- Checkpoint/restore primitives remain in the codebase, but automatic idle checkpointing is disabled until the turn channel is checkpoint-safe.
- Artifact browsing is a metadata-backed live file tree with search, safe downloads, delete/rename event handling, and richer previews for Markdown, code, text, images, JSON, CSV/TSV, and PDF.

## Recent Commits

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
- Automatic idle checkpointing is disabled because `runsc restore` cannot reliably reconnect the long-lived stdin turn channel.
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

The current codebase loads `config/harness.yaml` through the Phase 7 typed `harness:` schema. The full file shape and per-field semantics live in [architecture.md → Configuration](./architecture.md#configuration). Legacy `runtime:` / `claude:`-only files still parse during the cutover, but cannot be mixed with `harness:`.

## Current Flow

```text
POST /api/sessions
  -> session status: created

POST /api/sessions/<id>/messages
  -> persist user message
  -> status: running_active
  -> Runtime.Start()
     -> hot path: existing container + stdin write
     -> opt-in resume path: runsc restore from checkpoint only when explicitly enabled
     -> cold path: runsc run from OCI bundle
  -> stream parser persists assistant message
  -> artifact watcher scans workspace
  -> status: running_idle

Shell turns follow the same session lifecycle, but they complete on `harness.turn_done` and can be interrupted with `POST /api/sessions/<id>/interrupt`.

Idle monitor
  -> reconcile stale checkpointing/checkpointed rows
  -> exit because automatic checkpointing is disabled
```

Canonical session statuses and per-state semantics live in [architecture.md → Session State Machine](./architecture.md#session-state-machine). Note: `running`, `idle`, and `completed` are not current session statuses.

## Public Interfaces

HTTP routes, SSE/WebSocket endpoints, and the canonical event-name set are documented in [architecture.md → API Surface](./architecture.md#api-surface) and [architecture.md → Event Model](./architecture.md#event-model). This document does not duplicate those lists.

## Current Constraints

- Claude Code is the primary supported analysis path.
- `Shell` is the supported interactive command path and has its own `turn_done`/`interrupt` contract; future adapters still need their own completion protocol before they are first-class multi-turn citizens.
- The active Go runtime launches `runsc` directly. `bundle/restore-sandbox.sh` remains a useful Phase 2 smoke tool, not the main orchestrator runtime path.
- The current Go runtime uses `runsc -network sandbox -overlay2 none` with per-generation network profiles. The runtime creates the allocated netns/veth pair, configures host and sandbox addresses from the persisted `/30`, applies the static lab egress allow-list, probes the local proxy, and writes the generation-specific sandbox-visible Anthropic base URL into the control manifest. The local proxy key remains `123` for the lab path.
- Automatic idle checkpointing is disabled. Checkpoint/restore must move behind the Phase 7 checkpoint-safe control plane before it becomes the default resource-release path.
- Current live multi-turn behavior still depends on a container-local stdin/PTY turn channel. It is reliable for active containers, but not enough for robust restore/reconnect semantics.
- Artifact metadata is recorded by host-side scanning/watching and rendered as a read-only live file tree. Direct file mutation operations remain outside the UI; use the sandbox agent or shell path to create, rename, or delete files.
- Auth is lab shared-password cookie auth when `HARNESS_LAB_PASSWORD` is set.

## Next Architecture Target

The next major architecture phase is [phase7/README.md](./phase7/README.md). The target is to add runtime generations, durable turns, durable events, explicit network profiles, and a reconnectable agent bridge so sessions can be added, checkpointed, restored, reconnected, and continued across multiple turns without depending on one live host pipe.

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
npm run build
```
