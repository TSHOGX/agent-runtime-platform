# Harness Platform Architecture

> Last updated: 2026-05-22

## Overview

Harness Platform runs one AI data-analysis agent per gVisor sandbox session. The orchestrator owns session state, starts the sandbox, bridges user turns through stdin/PTY, parses agent stdout/stderr, persists messages, and publishes events to the frontend.

The current baseline uses:

- gVisor `runsc` with the `systrap` platform.
- A baked OCI bundle under `bundle/out/phase2-template-bundle`.
- Long-lived per-session containers while the conversation is active.
- Checkpoint/restore primitives, with automatic idle checkpointing disabled until the turn channel is checkpoint-safe.
- Same-origin Server-Sent Events for the browser event path.
- Per-container `OutputHub` for multi-turn output routing.
- PTY-backed shell sessions with interrupt support.
- Explicit local Claude proxy configuration loaded from `config/harness.yaml`.

The target checkpoint-safe architecture is tracked separately in [checkpoint-safe-control-plane-architecture.md](./checkpoint-safe-control-plane-architecture.md).

## Component Model

```text
Browser
  | HTTP + SSE
  v
Next.js frontend
  | same-origin route handlers
  v
Go orchestrator
  |-- HTTP API / auth
  |-- global Event Hub
  |-- canonical Session State
  |-- SQLite Store
  |-- Artifact Watcher
  `-- Runtime
        |-- active container map
        |-- per-container OutputHub
        `-- runsc run / restore / checkpoint / delete
              |
              v
        gVisor sandbox
          |-- harness-agent-entrypoint
          |-- Claude Code / PTY-backed shell agent
          |-- /workspace -> /var/lib/harness/sessions/<session_id>
          `-- /agent-homes/<session_id> -> /var/lib/harness/agent-homes/<session_id>
```

The frontend talks to its own origin. Route handlers forward API calls to the orchestrator, including the SSE stream:

```text
GET /api/events/stream -> orchestrator GET /api/events/stream
```

The orchestrator still exposes `/api/events` as a WebSocket endpoint for compatibility and manual debugging.

## Session State Machine

```text
created
  -> running_active
  -> running_idle

Any active state can fail or be destroyed.
```

The canonical session status set is:

```text
created
running_active
running_idle
checkpointing
checkpointed
failed
destroyed
```

State meanings:

| State | Meaning |
| --- | --- |
| `created` | Session exists but no sandbox has been started for it yet. |
| `running_active` | A user turn is being processed. |
| `running_idle` | The container is still alive and ready for another turn. |
| `checkpointing` | Legacy/experimental busy state for a checkpoint in progress. Startup reconciliation recovers stale rows. |
| `checkpointed` | Legacy/experimental state for persisted runtime images. Startup reconciliation currently re-enables these sessions as `running_idle`. |
| `failed` | Runtime or parser error. |
| `destroyed` | User or API explicitly ended the session. |

Input is accepted only in `created`, `running_idle`, and `checkpointed`. `running_active` and `checkpointing` are busy states. `failed` and `destroyed` are terminal states. The older `running`, `idle`, and `completed` names are not part of the current schema or API contract.

## Runtime Flow

`Runtime.Start()` chooses one of three paths:

1. **Hot path**: if `containers[sessionID]` exists, subscribe to that container's `OutputHub`, write the user turn to stdin, and forward output until the parser marks the turn complete.
2. **Opt-in restore path**: if `RestoreFromCheckpoint` is explicitly enabled and a checkpoint exists under `HARNESS_CHECKPOINTS_ROOT/<session_id>`, run `runsc restore`, recreate stdio pipes, create a new `OutputHub`, then write the user turn. The production path does not enable this by default.
3. **Cold path**: run `runsc run` from `HARNESS_BUNDLE_ROOT/phase2-template-bundle`, create stdio pipes, create a new `OutputHub`, then write the first user turn.

The active Go runtime now drives `runsc` directly. `bundle/restore-sandbox.sh` remains valuable for Phase 2 smoke tests and restore experiments, but it is no longer the primary request path.

Current limitation: stdin/PTY is still the lower-level turn transport. It is reliable for live multi-turn containers, but it is not a checkpoint-safe control plane because a restored container cannot rely on the original host pipe still being logically connected.

## Output Routing

Each container has its own pub/sub hub:

```go
type OutputEvent struct {
    Stream string // stdout, stderr, runtime
    Line   string
}

type Container struct {
    SessionID string
    RestoreID string
    Agent     string
    Stdin     io.WriteCloser
    OutputHub *OutputHub
}
```

Scanner goroutines publish container stdout and stderr lines into `OutputHub`. A `runSession()` call subscribes only for its current turn. This is the key change that fixed the old multi-turn callback bug.

Publishing is non-blocking. A slow subscriber may drop output lines instead of blocking the container scanner.

## Turn Completion

For Claude Code, user turns are written as one JSONL frame because the sandbox entrypoint launches Claude with `--input-format stream-json`:

```json
{"type":"user","message":{"role":"user","content":"..."}}
```

The stream parser marks a turn complete when it sees:

- `{"type":"result","subtype":"success",...}`
- `{"type":"error",...}`
- a non-success result subtype, which is reported as turn output while the session returns to `running_idle`

For shell sessions, the `harness-shell-agent` shim runs a PTY-backed shell and emits `harness.shell_output` frames for command output plus `harness.turn_done` when the prompt returns. The orchestrator persists shell output as assistant text, publishes it as `agent.output`, and exposes `POST /api/sessions/{id}/interrupt` for the running turn.

For other future agent adapters, stdin is still the lower-level fallback. Those adapters need their own completion contract before they are first-class multi-turn citizens.

## Event Model

Runtime output becomes higher-level events:

| Source | Parser behavior | Event |
| --- | --- | --- |
| `runtime` stream | lifecycle/status line | `system.status` |
| `stderr` | debug/log output | `agent.output` |
| Claude `stream_event` text delta | append to pending assistant text | `agent.delta` |
| Claude `assistant` message | persist final assistant text | `agent.message` |
| Claude `result` with text | persist result if not duplicate | `agent.message` |
| Shell `harness.shell_output` | publish shell output and persist assistant text | `agent.output` / `agent.message` |
| Shell `harness.turn_done` | mark the shell turn complete | completion |
| Plain stdout | persist as assistant text | `agent.message` |
| Artifact watcher | file create/write metadata | `artifact.updated` |
| Artifact watcher | file/directory remove or rename metadata cleanup | `artifact.deleted` |

The frontend keeps an SSE connection open and also polls session/messages/artifacts after sending a message to recover from transient stream issues.

## API Surface

HTTP:

- `GET /healthz`
- `POST /api/login`
- `GET /api/sessions`
- `POST /api/sessions`
- `GET /api/sessions/{id}`
- `DELETE /api/sessions/{id}`
- `GET /api/sessions/{id}/messages`
- `POST /api/sessions/{id}/messages`
- `POST /api/sessions/{id}/interrupt`
- `GET /api/sessions/{id}/artifacts`
- `GET /artifacts/{session_id}/{path}`

Events:

- `GET /api/events/stream?session_id=<id>` - SSE
- `GET /api/events?session_id=<id>` - WebSocket compatibility

Session lifecycle event names use the canonical status values:

- `session.created`
- `session.running_active`
- `session.running_idle`
- `session.checkpointing`
- `session.checkpointed`
- `session.failed`
- `session.destroyed`
- `artifact.updated`
- `artifact.deleted`

## Artifact Browser

Artifact metadata is still persisted as a flat SQLite list keyed by session and path. The frontend derives a live file tree from that list, so the API stays simple while users get a folder-oriented browser with search, tabs, file sizes, update times, and per-file download actions.

Artifact serving is intentionally read-only and constrained to regular files under the session workspace. Download requests reject path traversal, symlink components, symlink escape, directories, and non-regular files. The watcher also skips symlink artifacts during scans.

The artifact watcher publishes:

- `artifact.updated` for create/write metadata upserts.
- `artifact.deleted` for remove/rename cleanup, including directory-prefix metadata deletion.

The current preview set covers Markdown, code, text, images, JSON, CSV/TSV, and PDF. Unknown binary files remain downloadable but are not rendered inline.

## Configuration

Orchestrator:

| Variable | Default | Purpose |
| --- | --- | --- |
| `HARNESS_ORCHESTRATOR_ADDR` | `:8090` | HTTP bind address |
| `HARNESS_LAB_PASSWORD` | empty | Enables shared-password auth when set |
| `HARNESS_COOKIE_NAME` | `harness_auth` | Auth cookie name |
| `HARNESS_SESSION_TTL` | `2h` | Session expiry horizon |
| `HARNESS_REPO_ROOT` | repo root | Repository root used to derive bundle paths |
| `HARNESS_SESSIONS_ROOT` | `/var/lib/harness/sessions` | Host workspace root |
| `HARNESS_AGENT_HOMES_ROOT` | `/var/lib/harness/agent-homes` | Host root for per-session agent HOME state, mounted outside `/workspace` |
| `HARNESS_CHECKPOINTS_ROOT` | `/var/lib/harness/checkpoints` | Checkpoint image root |
| `HARNESS_BUNDLE_ROOT` | `<repo>/bundle/out` | OCI bundle root |
| `HARNESS_DB_PATH` | `<sessions_root>/orchestrator.db` | SQLite DB path |
| `HARNESS_DEFAULT_AGENT` | `claude` | Default session agent |
| `HARNESS_MAX_SESSIONS` | `30` | Active session cap |
| `RUNSC_ROOT` | `/var/lib/harness/runsc` | runsc state root |

`HARNESS_RESTORE_SCRIPT` is still parsed by config for compatibility, but the current direct `runsc` path does not execute the script.

Project config:

| File | Purpose |
| --- | --- |
| `config/harness.yaml` | Explicit lab runtime and Claude proxy profile. |

The current config loader reads `config/harness.yaml` first for runtime network and Claude proxy values, then applies hardcoded safe defaults. General orchestrator paths such as session roots and DB path still use the environment variables above.

Current `config/harness.yaml` values:

```yaml
runtime:
  runsc_network: sandbox
  runsc_overlay2: none

claude:
  proxy_bind_url: http://0.0.0.0:8082
  sandbox_base_url: http://10.200.1.1:8082
  api_key: "123"
  auth_token: "123"
  model: sonnet
  output_format: stream-json
  disable_nonessential_traffic: true
```

Claude control manifest:

| Field | Purpose |
| --- | --- |
| `proxy_bind_url` | Explicit host bind URL for the local proxy, `http://0.0.0.0:8082` |
| `anthropic_base_url` | Sandbox-visible proxy URL, `http://10.200.1.1:8082` in the current netns layout |
| `anthropic_api_key` / `anthropic_auth_token` | Local proxy credential, fixed to `123` for the lab stack |
| `claude_model` | Claude model alias, default `sonnet` |
| `claude_code_disable_nonessential_traffic` | Keep Claude Code from making nonessential traffic during sandbox turns |
| `session_workspace` | In-container sessions mount, default `/sessions/<session_id>` |
| `harness_agent_home` | In-container agent home mount, default `/agent-homes/<session_id>` |

Frontend:

| Variable | Default | Purpose |
| --- | --- | --- |
| `HARNESS_API_BASE_URL` | `http://127.0.0.1:8090` | Server-side proxy target |
| `ORCHESTRATOR_URL` | fallback alias | Server-side proxy target |
| `PORT` | Next.js default unless set | Local frontend port; use `8000` in docs |

## Network Model

The target security model remains gVisor sandbox networking plus host-side egress controls for Doris and the local LLM proxy.

The current Go runtime launches `runsc` with `-network sandbox -overlay2 none`. It writes an explicit control manifest that fixes the host proxy bind URL at `http://0.0.0.0:8082`, the sandbox-visible Anthropic base URL at `http://10.200.1.1:8082`, and the lab proxy key at `123`. The template bundle uses the fixed `/var/run/netns/phase1-demo` network namespace on this host.

## Checkpointing

`MonitorIdleSessions()` currently performs startup reconciliation and then exits because `autoCheckpointEnabled` is false. It recovers stale `checkpointing` rows and re-enables `checkpointed` rows as `running_idle`, so the UI/API do not stay stuck in checkpoint states after a restart.

Automatic idle checkpointing is disabled because `runsc restore` can restore the container while the long-lived stdin turn channel used by the agent entrypoint is no longer reliably reconnectable.

The checkpoint code still exists for experiments:

```text
running_idle -> checkpointing -> checkpointed
```

`Runtime.Checkpoint()` keeps the active container in the map until `runsc checkpoint -overlay2 <mode> -image-path` succeeds, then deletes the runtime container. On failure the container stays live and the session falls back to `running_idle`, so a later idle pass can retry in a checkpointable mode.

The intended future path is Phase 7:

```text
durable turn ledger
  -> runtime generation idle
  -> checkpoint generation
  -> restore generation
  -> reconnect agent bridge
  -> claim next turn
```

Until that control plane exists, checkpoint/restore should be treated as experimental and opt-in.

## Current Limitations

- Additional agent adapters beyond Claude Code and the shell shim need their own completion contract before they are first-class multi-turn citizens.
- Artifact browsing is read-only. File creation, renaming, and deletion should still happen through the sandbox agent or shell session, with the UI reflecting those changes through metadata events.
- Resource limits and egress policy are not yet documented as production-ready defaults.
- The current output hub intentionally drops lines for slow subscribers; that is acceptable for UI logs but should be revisited before using the stream as an audit log.
- Automatic checkpoint/restore is not a default resource-release path until the Phase 7 checkpoint-safe control plane is implemented.

## File Map

```text
orchestrator/
├── cmd/orchestrator/main.go
├── internal/agents/agents.go
├── internal/artifacts/watcher.go
├── internal/events/hub.go
├── internal/runtime/runtime.go
├── internal/runtime/output_hub.go
├── internal/server/server.go
├── internal/server/stream_parser.go
├── internal/sessionstate/state.go
└── internal/store/store.go

frontend/
├── app/api/[...path]/route.ts
├── components/harness-provider.tsx
├── components/workbench/
├── lib/artifact-tree.ts
└── lib/

sandbox-image/files/usr/local/bin/
├── harness-shell-agent
└── harness-agent-entrypoint

config/
└── harness.yaml
```
