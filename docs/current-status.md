# Current Status

> Last updated: 2026-05-21
> Scope: current baseline after commits `e8b84f0`, `9b803b6`, and `051f251`.

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

## Current Flow

```text
POST /api/sessions
  -> session status: created

POST /api/sessions/<id>/messages
  -> persist user message
  -> status: running_active
  -> Runtime.Start()
     -> hot path: existing container + stdin write
     -> resume path: runsc restore from checkpoint
     -> cold path: runsc run from OCI bundle
  -> stream parser persists assistant message
  -> artifact watcher scans workspace
  -> status: running_idle

Shell turns follow the same session lifecycle, but they complete on `harness.turn_done` and can be interrupted with `POST /api/sessions/<id>/interrupt`.

Idle monitor
  -> checkpointable sandbox network:
     -> after 30 minutes running_idle
     -> status: checkpointing
     -> runsc checkpoint -overlay2 none
     -> status: checkpointed
```

Canonical session statuses:

- `created`
- `running_active`
- `running_idle`
- `checkpointing`
- `checkpointed`
- `failed`
- `destroyed`

`running`, `idle`, and `completed` are not current session statuses.

## Public Interfaces

HTTP:

- `GET /healthz`
- `POST /api/login`
- `GET /api/sessions`
- `POST /api/sessions`
- `GET /api/sessions/<id>`
- `DELETE /api/sessions/<id>`
- `GET /api/sessions/<id>/messages`
- `POST /api/sessions/<id>/messages`
- `POST /api/sessions/<id>/interrupt`
- `GET /api/sessions/<id>/artifacts`
- `GET /artifacts/<session_id>/<path>`

Events:

- `GET /api/events/stream?session_id=<id>` - SSE, current frontend path
- `GET /api/events?session_id=<id>` - WebSocket compatibility path

Common event types:

- `session.created`
- `session.running_active`
- `session.running_idle`
- `session.checkpointing`
- `session.checkpointed`
- `session.failed`
- `session.destroyed`
- `message.created`
- `agent.delta`
- `agent.message`
- `agent.output`
- `system.status`
- `artifact.updated`

## Current Constraints

- Claude Code is the primary supported analysis path.
- `Shell` is the supported interactive command path and has its own `turn_done`/`interrupt` contract; future adapters still need their own completion protocol before they are first-class multi-turn citizens.
- The active Go runtime launches `runsc` directly. `bundle/restore-sandbox.sh` remains a useful Phase 2 smoke tool, not the main orchestrator runtime path.
- The current Go runtime uses `runsc -network sandbox -overlay2 none` with the fixed `/var/run/netns/phase1-demo` network namespace for the checkpointable lab path. It writes an explicit control manifest with the host proxy bind URL `http://0.0.0.0:8082`, the sandbox-visible Anthropic base URL `http://10.200.1.1:8082`, and the local key `123`. The target hardened design is still sandbox networking plus host-side egress policy.
- Artifact metadata is recorded by host-side scanning/watching. A richer live artifact tree and previews remain future work.
- Auth is lab shared-password cookie auth when `HARNESS_LAB_PASSWORD` is set.

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
