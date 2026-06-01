# Agent Runtime Platform Architecture

> Last updated: 2026-06-01

## Overview

This project is an Agent Runtime Platform for long-lived, sandboxed AI agent
sessions. The host-side control plane runs one AI data-analysis agent per
gVisor sandbox session. The orchestrator owns durable session state, starts
per-generation sandboxes, routes user turns through the Agent Bridge claim/ack
protocol, correlates model proxy requests, records artifacts, and publishes
events to the frontend workbench.

Architecturally, `runtime` is the execution and isolation layer; `control
plane` is the durable state, scheduling, policy, and observability layer.

Active planning docs:

- [PLAN.md](./PLAN.md) for the current operating plan.
- [next-stage.md](./next-stage.md) for the next capability target.

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
  |-- durable event hub
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
          |-- Claude Code, Pi, or PTY shell agent
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
execution compatibility, and recovery behavior.

Control plane is the host-side manager: session and turn state, generation
allocation, resource reconciliation, control manifest rendering, bridge
claim/ack processing, event persistence, artifact metadata, proxy correlation,
quota, retention, and policy. Its concerns are correctness, durable state,
governance, observability, and operational scale.

## Sandbox Boundary

The active runtime boundary is `sandbox-isolation-v1`, with sandbox contract
metadata for driver/provider identity, credential policy, runtime capabilities,
DataVolume evidence, driver-state fences, source deployment config digests, and
image agent-manifest digests.

- gVisor uses `runsc` with the `systrap` platform.
- The runtime launches `runsc` directly with sandbox networking and
  `overlay2=none`.
- Each generation receives an immutable sandbox contract and rendered OCI spec,
  control manifest, bridge dir, resource identity, and network profile.
- New allocations require the selected driver to be enabled in deployment
  config and present in the current rootfs `/etc/harness-image/agents.json`
  manifest or host-visible equivalent.
- Workspace and agent HOME are provisioned through host-trusted DataVolume rows
  and mounted as exact `/workspace` and `/agent-home` binds.
- Parent `/sessions` and `/agent-homes` mounts are forbidden.
- `/harness-secrets` is absent; legacy sandbox secret config keys are rejected.
- Rootfs is read-only; writable surfaces are explicit binds or scratch mounts.
- Agent processes run as the configured non-root sandbox identity.
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

`DELETE /api/sessions/{id}` moves a session to `destroyed` and reclaims runtime
resources while preserving retained history and workspace state.

## Runtime And Turns

`Runtime.Start()` takes one of three paths:

1. Reuse the active live generation.
2. Restore a `checkpointed` generation after metadata/resource validation.
3. Cold-start a new generation by allocating resources, rendering artifacts,
   probing the proxy/bridge path, and starting `runsc`.

Turns are stored before execution. The bridge claims queued turns, acks start,
emits output, and acks completion with CAS fencing so bridge clients, turn
runners, and sandboxes can restart at turn boundaries.

Agent mode selects the enabled `harness.default_agent` driver after provider
capability and image-manifest validation. Claude Code runs with stream-json
input. Pi runs as a long-lived RPC process through the model-proxy boundary,
emits normalized Pi events, and persists logical restore state through the
generic driver-state sidecar after successful completed turns. Shell is exposed
only when `sh` is enabled and present in the selected image manifest; the shell
shim emits `harness.shell_output` and `harness.turn_done`; shell sessions
support `POST /api/sessions/{id}/interrupt`.

Claude logical sessions are stored in `/agent-home`. Once a Claude UUID exists,
later turns must use Claude Code `--resume`; correctness cannot depend only on
an in-memory "first turn" flag.

## Events And Output

Runtime stdout/stderr is fanned out through a per-container `OutputHub`; the
current turn subscriber receives only its own output. Publishing is
non-blocking, so slow UI subscribers may drop log lines.

Durable events are written before in-memory publish. Important event names:

- `session.created`
- `session.running_active`
- `session.running_idle`
- `session.checkpointing`
- `session.checkpointed`
- `session.failed`
- `session.destroyed`
- `agent.delta`
- `agent.message`
- `agent.output`
- `system.status`
- `artifact.updated`
- `artifact.deleted`

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

Event routes:

- `GET /api/events/stream?session_id=<id>` - SSE

Typed runtime/control-plane failures generally return:

```json
{"error_class":"...","error":"..."}
```

Generic validation and not-found handlers may still return `{"error":"..."}`.

## Persistence And Config

SQLite stores sessions, messages, turns, durable events, proxy request context,
runtime generations, runtime resource instances, network profiles, sandbox
contracts, driver input evidence, DataVolume rows, and artifact metadata.

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

`HARNESS_SESSION_TTL` is obsolete and fails startup if present. Legacy top-level
`runtime:` / `claude:` config no longer loads, and legacy
`claude.proxy_bind_url` / `claude.sandbox_base_url` settings no longer map into
the active schema. Configure model proxy settings only under
`harness.model_proxy`. Obsolete `harness.session_ttl` and legacy
`harness.secrets.*` keys are rejected.

With `session_retention: 0s`, sessions, messages, artifacts, workspaces, and
agent homes do not expire automatically. `harness.max_sessions` counts
non-terminal sessions, not live gVisor resources; `/api/quota` reports session
and live-pool ceilings separately.

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
- The output hub is appropriate for UI streaming but not an audit log.
- Automatic checkpointing is disabled by default.
- Obsolete `workspace`, `agent_home_path`, and `restore_id` session columns are
  rejected at store open; cutover cleanup must remove old DB state.
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
  shell agent.
- `config/harness.yaml`: checked-in lab profile.
