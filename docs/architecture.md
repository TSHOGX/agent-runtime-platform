# Harness Platform Architecture

> Last updated: 2026-05-25

## Overview

Harness Platform runs one AI data-analysis agent per gVisor sandbox session. The orchestrator owns session state, starts per-generation sandboxes, routes user turns through the Agent Bridge claim/ack protocol, persists durable turn and event records, correlates model proxy requests, and publishes events to the frontend.

The current baseline uses:

- gVisor `runsc` with the `systrap` platform.
- Per-generation OCI specs, bundles, control manifests, bridge dirs, and network profiles under `harness.run_dir`.
- Long-lived active runtime generations while the conversation is active.
- Checkpoint/restore primitives behind the Phase 7 control plane, with automatic idle checkpointing disabled by the checked-in policy.
- Same-origin Server-Sent Events for the browser event path.
- Durable `events` table replay for multi-turn output routing.
- Bridge-backed Claude and shell sessions with interrupt support.
- Explicit local Claude proxy configuration loaded from `config/harness.yaml`.

The checkpoint-safe architecture is tracked in [phase7/README.md](./phase7/README.md).

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
        |-- runtime generation manager
        |-- resource allocator/reaper
        |-- bridge processor
        `-- runsc run / restore / checkpoint / delete
              |
              v
        gVisor sandbox
          |-- harness-agent-entrypoint
          |-- harness-bridge-client
          |-- Claude Code / PTY-backed shell agent
          |-- /workspace -> /sessions/<session_id>
          `-- /agent-homes/<session_id>
```

Current implementation note: `/sessions` is a bind mount of the whole host
workspace root and `/agent-homes` is a bind mount of the whole host agent-home
root. That gives the sandbox a broader filesystem view than the target
architecture. Phase 8 replaces these parent-root mounts with exact binds of the
current session workspace at `/workspace` and persistent session+driver home at
`/agent-home`.

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
| `checkpointed` | A generation has a persisted checkpoint and reserved allocation identity. The next accepted turn claims it for restore, validates metadata, recreates resources, and requires a bridge probe before claiming work. Legacy pre-Phase-7 checkpoint rows are fenced by migration as unrestorable. |
| `failed` | Runtime or parser error. |
| `destroyed` | User or API explicitly ended the session. |

Input is accepted only in `created`, `running_idle`, and `checkpointed`. `running_active` and `checkpointing` are busy states. `failed` and `destroyed` are terminal states. The older `running`, `idle`, and `completed` names are not part of the current schema or API contract.

## Runtime Flow

`Runtime.Start()` chooses one of three paths:

1. **Live path**: if the session has an active live generation, enqueue the turn and let the bridge claim it.
2. **Restore path**: if the session is `checkpointed`, claim the checkpointed generation for restore, recreate compatible resources, validate checkpoint metadata, run `runsc restore`, and require the bridge probe before claim.
3. **Cold path**: allocate a new runtime generation, render per-generation resources, run the host-side proxy probe, start `runsc`, require the in-sandbox bridge probe, then let the bridge claim turns.

The active Go runtime now drives `runsc` directly. `bundle/restore-sandbox.sh` remains valuable for Phase 2 smoke tests and restore experiments, but it is no longer the primary request path.

The bridge is the lower-level turn transport. It uses a file-backed queue so reconnect and checkpoint/restore do not depend on a live host pipe.

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

For future agent adapters, the bridge remains the session-level transport. Each adapter still needs its own completion/output contract before it is a first-class multi-turn citizen.

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

Phase 7 adds:

- `GET /api/quota`
- JSON envelope `{"error_class":"...","error":"..."}` for typed runtime/control-plane failures such as pool exhaustion or probe failure. Generic validation and not-found handlers may still return `{"error":"..."}`.

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
| `HARNESS_SESSION_RETENTION` | `harness.session_retention` (`0s`) | Session/history retention horizon; `0s` disables automatic expiry |
| `HARNESS_REPO_ROOT` | repo root | Repository root used to derive bundle paths |
| `HARNESS_SESSIONS_ROOT` | `/var/lib/harness/sessions` | Host workspace root |
| `HARNESS_AGENT_HOMES_ROOT` | `/var/lib/harness/agent-homes` | Host root for per-session agent HOME state, mounted outside `/workspace` |
| `HARNESS_CHECKPOINTS_ROOT` | `/var/lib/harness/checkpoints` | Checkpoint image root |
| `HARNESS_BUNDLE_ROOT` | `<repo>/bundle/out` | OCI bundle root |
| `HARNESS_DB_PATH` | `<sessions_root>/orchestrator.db` | SQLite DB path |
| `HARNESS_DEFAULT_AGENT` | `claude` | Default session agent |
| `HARNESS_MAX_SESSIONS` | `harness.max_sessions` (`30`) | Non-terminal session ceiling; independent of live `/30` capacity |
| `RUNSC_ROOT` | `/var/lib/harness/runsc` | runsc state root |

`HARNESS_RESTORE_SCRIPT` is still parsed by config for compatibility, but the current direct `runsc` path does not execute the script.

Project config:

| File | Purpose |
| --- | --- |
| `config/harness.yaml` | Phase 7 typed control-plane schema and explicit lab runtime profile. |

The config loader reads `config/harness.yaml` through a strict `gopkg.in/yaml.v3` decoder. The primary shape is the Phase 7 `harness:` schema: `run_dir`, `session_retention`, `max_sessions`, nested network egress, event retention, probe status, bridge lease, reaper, and secret-root settings are decoded into typed config and validated before startup. Legacy files containing only top-level `runtime:` / `claude:` sections still load for compatibility, but mixing legacy sections with `harness:` is rejected.

General orchestrator paths such as session roots and DB path still use the environment variables above. `HARNESS_SESSION_RETENTION` and `HARNESS_MAX_SESSIONS` override their `harness:` values and are revalidated against the Phase 7 schema. `HARNESS_SESSION_TTL` is obsolete and fails startup if present so deployments do not silently switch to no-expiry retention.

With `session_retention: 0s`, sessions, messages, artifacts, workspaces, and agent-home paths do not expire automatically. `harness.max_sessions` still counts non-terminal sessions (`created`, `running_active`, `running_idle`, `checkpointing`, `checkpointed`) rather than live gVisor resources, so it is not validated against the configured `/30` pool. `GET /api/quota` reports `soft_session_ceiling` and `live_pool_ceiling` separately. Operators should close sessions explicitly with `DELETE /api/sessions/{id}` to free session quota; close preserves history and workspace state while reclaiming runtime resources.

Current `config/harness.yaml` values:

```yaml
harness:
  run_dir: /var/lib/harness/run
  session_retention: 0s
  max_sessions: 30
  network:
    cidr_pool: 10.200.0.0/16
    egress:
      doris_fe_hosts: [172.16.0.138]
      doris_be_hosts: [172.16.0.138]
      doris_ports: [9030]
      dns_policy: hostnames_only
  events:
    retention_window: 24h
    retention_rows: 1000000
    emit_output_batch_max_rows: 64
    emit_output_batch_max_age: 100ms
  probe:
    accept_status:
      get_healthz: [200]
      post_v1_messages:
        unauthorized: [401]
        malformed_authenticated: [400]
    pre_start_attempts: 3
    pre_start_interval: 500ms
    post_start_attempts: 5
    post_start_interval: 1s
  bridge:
    lease_ttl: 60s
    heartbeat_interval: 30s
    poll_interval: 5ms
    ack_started_grace: 90s
    reconnect_grace: 30s
  checkpoint:
    auto_enabled: false
    idle_threshold: 30m
    monitor_interval: 5m
  reaper:
    failed_retention: 10m
    checkpoint_image_retention: 720h
```

Claude control manifest:

| Field | Purpose |
| --- | --- |
| `proxy_bind_url` | Explicit host bind URL for the local proxy, `http://0.0.0.0:8082` |
| `anthropic_base_url` | Sandbox-visible proxy URL, derived from the generation's allocated `host_gateway_ip` |
| `anthropic_api_key_secret_id` / `anthropic_auth_token_secret_id` | References to sandbox-readable local proxy credentials, fixed to `123` in the lab stack after secret materialization; Phase 8 sunsets this provider-secret path, moves upstream credentials host-side, and authorizes model requests through source-IP/generation/turn context plus driver entitlement |
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

The current Go runtime launches `runsc` with `-network sandbox -overlay2 none`. The allocator persists a generation-specific netns/veth pair, `/30`, gateway, proxy URL, and egress policy; the runtime recreates those host resources before start/restore, runs a host-side netns probe against the local proxy, and renders the generation's netns path into the OCI spec. The host proxy bind URL remains `http://0.0.0.0:8082`, and the lab proxy key remains `123`.

## Checkpointing

`MonitorIdleSessions()` performs startup reconciliation and then obeys the checkpoint policy. The checked-in lab config has `harness.checkpoint.auto_enabled: false`, so automatic checkpointing is disabled by policy, not because the turn transport is still stdin-coupled.

The checkpoint code still exists for experiments:

```text
running_idle -> checkpointing -> checkpointed
```

`Runtime.Checkpoint()` writes a checkpoint image manifest and persists the runtime artifact digests needed for restore validation. On failure the generation returns to `running_idle`, so a later idle pass can retry if policy permits it.

The Phase 7 path is:

```text
durable turn ledger
  -> runtime generation idle
  -> checkpoint generation
  -> restore generation
  -> reconnect agent bridge
  -> claim next turn
```

Checkpoint/restore remains policy-gated. Operators should enable automatic checkpointing only after restore behavior, resource retention, and SLOs are acceptable for the target deployment.

## Current Limitations

- Runtime filesystem isolation is not yet at the intended boundary: the sandbox
  can see parent `/sessions` and `/agent-homes` mounts, not only the current
  session's workspace and persistent session+driver home. This is Phase 8's
  highest-priority fix.
- Claude-visible model credentials are still read from sandbox-mounted secret
  files and exported before execing Claude. They are not stored in the
  manifest/spec/rootfs, but they are visible to the Claude process and its tool
  subprocesses. Phase 8 moves upstream credentials host-side.
- Shell sessions currently start from the root entrypoint path and need the
  Phase 8 non-root driver contract before being treated as least-privilege.
- The OCI rootfs is currently writable. Phase 8 changes it to read-only with
  explicit writable mounts for tmp/cache/workspace/home/bridge.
- Additional agent adapters beyond Claude Code and the shell shim need their own completion contract before they are first-class multi-turn citizens.
- Artifact browsing is read-only. File creation, renaming, and deletion should still happen through the sandbox agent or shell session, with the UI reflecting those changes through metadata events.
- Tenant-level resource limits and production egress policy management are Phase 10 work.
- The current output hub intentionally drops lines for slow subscribers; that is acceptable for UI logs but should be revisited before using the stream as an audit log.
- Automatic checkpoint/restore is not enabled by default in the lab config; it is a policy decision on top of the Phase 7 control plane.

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
