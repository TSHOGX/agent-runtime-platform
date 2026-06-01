# Agent Runtime Platform Architecture

> Last updated: 2026-06-01

## Overview

This project is an Agent Runtime Platform for long-lived, sandboxed AI agent
sessions. The host-side control plane runs one selected driver per gVisor
sandbox session. The orchestrator owns durable session state, starts
per-generation sandboxes, routes turns through the Agent Bridge claim/ack
protocol, correlates model proxy requests, records artifacts, and publishes
events to the frontend workbench.

Architecturally, `runtime` is the execution and isolation layer; `control
plane` is the durable state, scheduling, policy, and observability layer.

Planning docs:

- [PLAN.md](./PLAN.md) is the current operating plan.
- [next-stage.md](./next-stage.md) is the detailed generation-plan and
  capability-plane target.

## Component Model

```text
Browser
  | HTTP + SSE
  v
Next.js frontend
  | same-origin route handlers
  v
Go orchestrator
  |-- HTTP API / lab auth
  |-- SQLite store
  |-- in-memory event hub + durable event store
  |-- artifact watcher
  |-- proxy correlation UDS
  `-- runtime manager
        |-- generation/resource reconciler
        |-- bridge processor
        `-- runsc run / restore / checkpoint / delete
              |
              v
        gVisor sandbox
          |-- harness-agent-entrypoint
          |-- harness-bridge-client
          |-- Claude Code, Pi, or shell shim
          |-- /workspace exact bind
          `-- /agent-home exact bind
```

The browser talks to the frontend origin. Frontend route handlers proxy API and
artifact requests to the orchestrator. Live browser events use same-origin SSE:

```text
GET /api/events/stream -> orchestrator GET /api/events/stream
```

## Runtime Vs Control Plane

Runtime is the execution substrate: gVisor `runsc`, the sandbox process,
rootfs, mounts, network namespace, bridge client, selected agent driver, and
checkpoint/restore mechanics. Its concerns are isolation, resource shape,
execution validation, and recovery behavior.

Control plane is the host-side manager: session and turn state, generation
allocation, resource reconciliation, sandbox contract and launch artifact
rendering, bridge claim/ack processing, event persistence, artifact metadata,
proxy correlation, quota, retention, and policy. Its concerns are correctness,
durable state, governance, observability, and operational scale.

## Sandbox Boundary

The active runtime boundary is `sandbox-isolation-v1`, with sandbox contract
metadata for driver/provider identity, credential policy, runtime capabilities,
DataVolume evidence, driver-state fences, source deployment config digests, and
image agent-manifest digests.

- gVisor uses `runsc` with the `systrap` platform.
- The runtime launches `runsc` directly with sandbox networking and
  `overlay2=none`.
- Each generation receives a sandbox contract plus rendered OCI spec, control
  manifest, bridge dir, resource identity, and network profile.
- New allocations require the selected driver to be enabled in deployment
  config and present in the current rootfs `/etc/harness-image/agents.json`
  manifest or host-visible equivalent.
- Workspace and agent HOME are provisioned through host-trusted DataVolume rows
  and mounted as exact `/workspace` and `/agent-home` binds.
- Parent `/sessions` and `/agent-homes` mounts are forbidden.
- `/harness-secrets` is absent; removed sandbox secret config keys are rejected.
- Rootfs is read-only; writable surfaces are explicit binds or scratch mounts.
- Sandbox processes run as the configured non-root identity with empty
  capabilities and `noNewPrivileges`.
- Provider credentials remain host/proxy-side. The sandbox sees only the
  configured stable model proxy alias
  (`harness.model_proxy.sandbox_base_url`) when model access is enabled.

Normal model dispatch requires active turn context, source-IP/resource/contract
match, driver entitlement, and proxy correlation through the authenticated
host-side UDS. Sandbox-sent source-IP claims are diagnostics, not authority.

## Session State

Canonical statuses:

```text
created
running_active
running_idle
checkpointing
checkpointed
failed
destroyed
```

Input is accepted only in `created`, `running_idle`, and `checkpointed`.
`running_active` and `checkpointing` are busy states. `failed` and `destroyed`
are terminal states. The older `running`, `idle`, and `completed` names are not
current API statuses.

`DELETE /api/sessions/{id}` moves a session to `destroyed`, reclaims runtime
resources, and preserves retained history plus DataVolume state.

## Runtime And Turns

For new generations, the server allocates resources, prepares runtime
artifacts, persists the sandbox contract, and creates a runtime resource
instance before handing off to runtime.

`Runtime.Start()` then has three paths:

1. Reuse the active live generation.
2. Restore a `checkpointed` generation after metadata/resource validation.
3. Start a fresh `runsc` container from prepared generation artifacts.

Turns are stored before execution. The bridge claims queued turns, acks start,
emits output, and acks completion with CAS fencing so bridge clients, turn
runners, and sandboxes can restart at turn boundaries.

Agent mode selects the enabled `harness.default_agent` driver after provider
capability and image-manifest validation. Claude Code runs through the bridge
with stream-json output. Pi runs as a long-lived RPC process through the
model-proxy boundary, emits normalized Pi events, and persists logical restore
state through the driver-state store after completed turns. Shell is exposed
only when `sh` is enabled and present in the selected
image manifest; the shell shim emits `harness.shell_output` and
`harness.turn_done`; shell sessions support
`POST /api/sessions/{id}/interrupt`.

Claude logical sessions are stored in `/agent-home`. Once driver state marks a
Claude session initialized, later turns must use Claude Code `--resume`;
correctness cannot depend only on an in-memory "first turn" flag.

## Events And Output

State mutations are committed to SQLite before any SSE publish. Durable event
records can be replayed over SSE with `last_event_id`; if the requested cursor
has fallen outside retention, the stream emits `replay_gap`.

Durable replay event names:

- `ack_turn_started`
- `emit_output`
- `ack_turn_completed`
- `session.destroyed`
- `generation.error`
- `session.checkpoint_retired`
- `proxy.request.started`
- `proxy.request.completed`
- `proxy.request.failed`

Live/UI SSE also includes events backed by canonical tables or by parsed bridge
output, not standalone replay rows:

- `session.created`
- `message.created`
- `session.running_active`
- `session.running_idle`
- `session.checkpointed`
- `agent.delta`
- `agent.message`
- `agent.output`
- `system.status`
- `artifact.updated`
- `artifact.deleted`

Durable bridge completion and generation-error payloads can also carry session
status updates.

The frontend keeps an SSE connection open and also refetches session, messages,
and artifacts after message submission so missed frames do not permanently
desynchronize the UI.

## API Surface

HTTP routes:

- `GET /healthz`
- `POST /api/login`
- `GET /api/quota`
- `GET /api/deployment-capabilities`
- `GET /api/sessions`
- `POST /api/sessions`
- `GET /api/sessions/{id}`
- `DELETE /api/sessions/{id}`
- `GET /api/sessions/{id}/messages`
- `POST /api/sessions/{id}/messages`
- `POST /api/sessions/{id}/interrupt`
- `GET /api/sessions/{id}/artifacts`
- `GET /artifacts/{session_id}/{path}`

Event and internal routes:

- `GET /api/events/stream?session_id=<id>` - SSE
- `POST /internal/proxy/requests/start` and `/finish` - authenticated UDS only
- `GET /api/agents` - operator route, not mounted on the product API server

Typed runtime/control-plane failures generally return:

```json
{"error_class":"...","error":"..."}
```

Generic validation and not-found handlers may still return `{"error":"..."}`.

## Persistence And Config

SQLite stores sessions, messages, turns, durable events, proxy request context,
runtime generations, runtime resource instances, network profiles, sandbox
contracts, driver state, driver input evidence, DataVolume rows, and artifact
metadata.

Primary project config is `config/harness.yaml` under the `harness:` schema.
The config loader decodes only that top-level `harness:` document with strict
known-field validation, so unknown top-level or nested keys fail startup.
The checked-in lab profile currently sets:

- `default_agent: pi`
- enabled `agents.pi`, `agents.claude_code`, and `agents.sh` entries
- `model_profiles.anthropic_default` with `proxy_ref: model_proxy`
- `runtime_providers.local_runsc`
- `run_dir: /var/lib/harness/run`
- `session_retention: 0s`
- `max_sessions: 30`
- `model_proxy.bind_url: http://0.0.0.0:8082`
- `model_proxy.sandbox_base_url: http://harness-model-proxy.internal:8082`
- `sandbox_identity: 65534:65534`
- `network.cidr_pool: 10.200.0.0/16`
- Doris egress allow-list: `172.16.0.138:9030`
- bridge poll interval: `5ms`
- automatic checkpointing: `false`
- failed resource retention: `10m`
- checkpoint image retention: `720h`

The active rootfs image manifest is an operational artifact, not only source
config. The checked-in lab default selects Pi for `Agent` and exposes `Shell`,
so the selected rootfs must include `pi` and `sh` entries in
`/etc/harness-image/agents.json`. `claude_code` remains configured for explicit
overrides and smokes, but any deployment that selects it must use a manifest
containing `claude_code`.

Important environment overrides:

- `HARNESS_ORCHESTRATOR_ADDR`
- `HARNESS_LAB_PASSWORD`
- `HARNESS_DEFAULT_AGENT`
- `HARNESS_SESSION_RETENTION`
- `HARNESS_MAX_SESSIONS`
- `HARNESS_SESSIONS_ROOT`
- `HARNESS_AGENT_HOMES_ROOT`
- `HARNESS_ROOTFS_PATH`
- `HARNESS_DB_PATH`
- `RUNSC_ROOT`

`HARNESS_SESSION_TTL` has been removed and fails startup if present. Removed
top-level `runtime:` / `claude:` config no longer loads, and removed
`claude.proxy_bind_url` / `claude.sandbox_base_url` settings no longer map into
the active schema. Configure model proxy settings only under
`harness.model_proxy`. Removed `harness.session_ttl` and
`harness.secrets.*` keys are rejected.

With `session_retention: 0s`, sessions, messages, artifacts, workspaces, and
agent homes do not expire automatically. `harness.max_sessions` counts
non-terminal sessions, not live gVisor resources; `/api/quota` reports session
and live network `/30` pool ceilings separately.

## Network And Checkpointing

Each generation owns a persisted `/30`, netns/veth pair, host gateway,
sandbox IP, and static lab egress policy. The runtime recreates those host
resources before start/restore, runs a host-side proxy probe, and renders the
generation's network namespace into the OCI spec.

Checkpoint/restore remains policy-gated. The checked-in lab config disables
automatic idle checkpointing, but the restore path is still available for
validated `checkpointed` generations:

```text
running_idle -> checkpointing -> checkpointed -> restore -> running_idle
```

Operators should enable automatic checkpointing only after restore behavior,
resource retention, and SLOs are acceptable for the deployment.

## Artifacts

Artifacts are metadata-backed records for files under the verified session
workspace. Downloads reject traversal, symlink components, symlink escape,
directories, and non-regular files. The watcher records create/write metadata
as `artifact.updated` and remove/rename cleanup as `artifact.deleted`.

The UI presents a read-only live file tree. File creation, deletion, and rename
operations happen through the agent or shell path, then the UI reflects the
metadata events.

## Current Limitations

- Supported agent paths are Claude Code, Pi, and the shell shim. Other adapters
  must enter through the existing driver/provider contracts.
- The artifact UI is read-only.
- Canonical tables and durable events are the audit source; live SSE is only
  for UI synchronization.
- Automatic checkpointing is disabled by default.
- Removed `workspace`, `agent_home_path`, `claude_session_uuid`, and
  `restore_id` session columns are rejected at store open; cutover cleanup must
  remove old DB state.
- Tenant authz, credential storage/rotation/GC, tenant egress policy, resource
  limits, observability, and multi-orchestrator HA are later production work.

## Source Map

- `orchestrator/internal/server/`: HTTP API, auth, turn orchestration.
- `orchestrator/internal/runtime/`: runsc rendering and lifecycle.
- `orchestrator/internal/bridge/`: bridge claim/ack processing.
- `orchestrator/internal/store/`: SQLite schema and control-plane mutations.
- `orchestrator/internal/artifacts/`: host-side artifact scanning.
- `frontend/`: Next.js workbench and same-origin proxy routes.
- `sandbox-image/files/usr/local/bin/`: sandbox entrypoint, bridge client, and
  shell shim.
- `config/harness.yaml`: checked-in lab profile.
