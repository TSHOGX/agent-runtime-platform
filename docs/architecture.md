# Harness Platform Architecture

> Last updated: 2026-05-21

## Overview

Harness Platform runs one AI data-analysis agent per gVisor sandbox session. The orchestrator owns session state, starts or restores the sandbox, bridges user turns through stdin, parses agent stdout/stderr, persists messages, and publishes events to the frontend.

The current baseline uses:

- gVisor `runsc` with the `systrap` platform.
- A baked OCI bundle under `bundle/out/phase2-template-bundle`.
- Long-lived per-session containers while the conversation is active.
- Checkpoint/restore after idle time.
- Same-origin Server-Sent Events for the browser event path.
- Per-container `OutputHub` for multi-turn output routing.

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
          |-- Claude Code / smoke agent
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
  -> checkpointing
  -> checkpointed
  -> running_active

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
| `checkpointing` | Idle monitor is checkpointing and releasing runtime resources. |
| `checkpointed` | Runtime state is persisted and can be restored on next message. |
| `failed` | Runtime or parser error. |
| `destroyed` | User or API explicitly ended the session. |

Input is accepted only in `created`, `running_idle`, and `checkpointed`. `running_active` and `checkpointing` are busy states. `failed` and `destroyed` are terminal states. The older `running`, `idle`, and `completed` names are not part of the current schema or API contract.

## Runtime Flow

`Runtime.Start()` chooses one of three paths:

1. **Hot path**: if `containers[sessionID]` exists, subscribe to that container's `OutputHub`, write the user turn to stdin, and forward output until the parser marks the turn complete.
2. **Resume path**: if a checkpoint exists under `HARNESS_CHECKPOINTS_ROOT/<session_id>`, run `runsc restore`, recreate stdio pipes, create a new `OutputHub`, then write the user turn.
3. **Cold path**: run `runsc run` from `HARNESS_BUNDLE_ROOT/phase2-template-bundle`, create stdio pipes, create a new `OutputHub`, then write the first user turn.

The active Go runtime now drives `runsc` directly. `bundle/restore-sandbox.sh` remains valuable for Phase 2 smoke tests and restore experiments, but it is no longer the primary request path.

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

For non-Claude agents, stdin is raw text. These agents are suitable for smoke tests but do not emit Claude result frames, so they are not equivalent to the full multi-turn Claude path.

## Event Model

Runtime output becomes higher-level events:

| Source | Parser behavior | Event |
| --- | --- | --- |
| `runtime` stream | lifecycle/status line | `system.status` |
| `stderr` | debug/log output | `agent.output` |
| Claude `stream_event` text delta | append to pending assistant text | `agent.delta` |
| Claude `assistant` message | persist final assistant text | `agent.message` |
| Claude `result` with text | persist result if not duplicate | `agent.message` |
| Plain stdout | persist as assistant text | `agent.message` |
| Artifact watcher | file create/write metadata | `artifact.updated` |

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

## Configuration

Orchestrator:

| Variable | Default | Purpose |
| --- | --- | --- |
| `HARNESS_ORCHESTRATOR_ADDR` | `:8090` | HTTP bind address |
| `HARNESS_LAB_PASSWORD` | empty | Enables shared-password auth when set |
| `HARNESS_COOKIE_NAME` | `harness_auth` | Auth cookie name |
| `HARNESS_SESSION_TTL` | `2h` | Session expiry horizon |
| `HARNESS_SESSIONS_ROOT` | `/var/lib/harness/sessions` | Host workspace root |
| `HARNESS_AGENT_HOMES_ROOT` | `/var/lib/harness/agent-homes` | Host root for per-session agent HOME state, mounted outside `/workspace` |
| `HARNESS_CHECKPOINTS_ROOT` | `/var/lib/harness/checkpoints` | Checkpoint image root |
| `HARNESS_BUNDLE_ROOT` | `<repo>/bundle/out` | OCI bundle root |
| `HARNESS_DB_PATH` | `<sessions_root>/orchestrator.db` | SQLite DB path |
| `HARNESS_DEFAULT_AGENT` | `demo` | Default session agent |
| `HARNESS_MAX_SESSIONS` | `30` | Active session cap |
| `RUNSC_ROOT` | `/var/lib/harness/runsc` | runsc state root |

Claude control env:

| Variable | Purpose |
| --- | --- |
| `HARNESS_CLAUDE_BASE_URL` / `HARNESS_ANTHROPIC_BASE_URL` | Preferred Anthropic-compatible proxy URL |
| `HARNESS_CLAUDE_API_KEY` / `HARNESS_CLAUDE_AUTH_TOKEN` | Preferred local Claude proxy credential |
| `HARNESS_CONTAINER_SESSIONS_ROOT` | In-container sessions mount, default `/sessions` |
| `HARNESS_CONTAINER_AGENT_HOMES_ROOT` | In-container agent home mount, default `/agent-homes` |
| `CLAUDE_MODEL` | Claude model alias, default `sonnet` |

Frontend:

| Variable | Default | Purpose |
| --- | --- | --- |
| `HARNESS_API_BASE_URL` | `http://127.0.0.1:8090` | Server-side proxy target |
| `ORCHESTRATOR_URL` | fallback alias | Server-side proxy target |
| `PORT` | Next.js default unless set | Local frontend port; use `8000` in docs |

## Network Model

The target security model remains gVisor sandbox networking plus host-side egress controls for Doris and the local LLM proxy.

The current Go runtime launches `runsc` with `-network host` in the lab path to simplify local proxy connectivity. This should be treated as an implementation shortcut, not the final hardening state. Phase 2 scripts still document and exercise `RUNSC_NETWORK=sandbox` for checkpoint/restore experiments.

## Checkpointing

`MonitorIdleSessions()` runs every 5 minutes. A `running_idle` session whose `last_activity_at` is older than 30 minutes is checkpointed:

```text
running_idle -> checkpointing -> checkpointed
```

`Runtime.Checkpoint()` removes the active container from the map, runs `runsc checkpoint -image-path`, then deletes the runtime container.

## Current Limitations

- OpenCode is verified locally but is not launched by the sandbox entrypoint yet.
- Non-Claude agents do not emit structured turn-completion frames.
- Artifact UX is still basic: metadata, preview, and download are present; richer live tree and file operations are future work.
- Resource limits and egress policy are not yet documented as production-ready defaults.
- The current output hub intentionally drops lines for slow subscribers; that is acceptable for UI logs but should be revisited before using the stream as an audit log.

## File Map

```text
orchestrator/
├── cmd/orchestrator/main.go
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
└── lib/

sandbox-image/files/usr/local/bin/
└── harness-agent-entrypoint
```
