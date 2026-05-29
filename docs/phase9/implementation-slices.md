# Phase 9 Implementation Slices

This is the execution index for [README.md](./README.md). It keeps the phase
sequence and shared constraints in one place; detailed deliverables, code
touchpoints, and gates live in one file per slice under `slices/`.

Reference documents:

- [sandbox-contract-v2.md](./sandbox-contract-v2.md)
- [driver-state.md](./driver-state.md)
- [runtime-capabilities.md](./runtime-capabilities.md)
- [secret-grants.md](./secret-grants.md)
- [pi-driver.md](./pi-driver.md)
- [current-code-map.md](./current-code-map.md)

Clean-cutover rule: slices may use the automatic destructive cutover defined in
[README.md](./README.md) for obsolete pre-gate state. Old runtime rows and old
`sessions`, `messages`, `artifacts`, `turns`, and `events` rows are disposable,
constrained SQLite tables may be rebuilt directly, and old interfaces may be
removed when that produces a cleaner driver/provider implementation. Provider
cleanup is automatic and coordinated by startup after the orchestrator owner
lock is held. The 9a store-open cutover coordinator owns marker capture,
cleanup replay, and final schema/deletion transactions around the existing
transaction-only migration runner; injected cleanup helpers perform idempotent
provider/filesystem cleanup outside DB transactions while the durable
in-progress marker is present. Discoverable live provider/isolation resources
must be cleaned, proven absent, or durably quarantined before their DB ownership
rows are deleted; otherwise the in-progress marker remains and runtime startup
stays blocked. Durable quarantine means an active
`runtime_resource_quarantine_tombstones` row that allocator, restore, and
host-state reconciliation paths enforce until explicit absence-evidence
release. There is no preflight manifest, manual approval, retained-session
quarantine, or reactivation path.

## Slice Map

| Slice | Detail | New product capability | Primary gate |
| --- | --- | --- | --- |
| 9a | [Contract and schema shape](./slices/9a-contract-schema.md) | No | Canonical IDs, request/config-boundary legacy translation, automatic destructive cutover, sidecar/checkpoint fence, and v2 validation pass before v2 writes |
| 9b | [Driver and provider registries](./slices/9b-driver-provider-registries.md) | No | Unsupported driver/provider pairs fail before allocation and 9a digest bytes stay stable |
| 9c | [Generic config and frontend surface](./slices/9c-generic-config-frontend.md) | No | Current config maps cleanly and selected drivers match the built image manifest |
| 9d | [Bridge and output refactor](./slices/9d-bridge-output.md) | No | Bridge protocol v2 negotiation gates `RunTurn`, and Claude/shell public event output remains unchanged |
| 9e | [Strict secret grants](./slices/9e-secret-grants.md) | No | Only `domain: model_provider` with `proxy_only` passes in Phase 9 |
| 9f | [Pi driver integration](./slices/9f-pi-driver.md) | Yes | Pi gates pass after 9a-9e and Claude/shell gates remain green |

## Ordering Rules

9a is intentionally split into sub-gates so the v2 write cutover is not the
first time schema, loader, sidecar, checkpoint, projection, and proxy
authorization changes meet. 9b promotes 9a hard-coded driver/provider facts into
registries without changing digest bytes for unchanged facts. 9c adds the
product-facing mode/config layer and image-manifest gate before 9f can select
Pi.

9a, 9b, and 9c are independently releasable. Until 9c lands, 9a/9b may keep a
public API compatibility adapter for the existing `agent: "claude" | "sh"`
request/response shape. That adapter maps only at the HTTP DTO boundary; it
accepts legacy input and derives any legacy response field from canonical
`sessions.driver_id`. It does not register `claude` as a driver alias and must
not feed runtime selectors, v2 contracts, grants, image manifests, sidecars,
restore, or proxy authorization. 9a also owns the deployment-default boundary:
legacy
`HARNESS_DEFAULT_AGENT=claude` and any config-derived omitted create-session
default must be normalized to canonical `claude_code` before runtime selection,
or rejected if it cannot be mapped. No lower layer may receive `claude` from
config defaults. The only post-boundary legacy-token exception before 9d is the
protocol-v1 sandbox projection described below. 9c replaces the public adapter
with product `mode` and removes or rejects legacy `agent` input.

The only exception is 9a.1 staging before the destructive cutover: if a
deployable build can still see pre-cutover rows with nullable/missing
`sessions.driver_id`, it must keep the old-row selector and old-row legacy DTO
projection for those rows until 9a.4 deletes or rebuilds them. That selector
and DTO projection are quarantined to pre-cutover rows only. They must not
create v2 contracts, sidecars, grants, image manifests, restore evidence,
proxy authorization, or new response fields from legacy aliases, and they are
removed or made unreachable by the 9a.4 cutover. If 9a.1 and 9a.4 ship
atomically, this exception is unnecessary and all legacy response `agent`
fields are derived only from canonical `sessions.driver_id`.

9a chooses protocol-v1 sandbox compatibility rather than teaching the current
runner to consume `claude_code`. 9a may add the new driver/provider control
projection before 9d, but the sandbox bridge runner still consumes the
protocol-v1 manifest/env shape until the 9d bridge refactor lands. Therefore
9a-9c must keep the old sandbox-visible compatibility fields required by the
current `harness-agent-entrypoint` and `harness-bridge-client`: canonical
`driver_id: "claude_code"` is projected to sandbox-visible `agent: "claude"` /
`HARNESS_AGENT=claude`, and canonical `driver_id: "sh"` is projected to `sh`.
Those values are projection-only compatibility output derived from canonical
state; they are not accepted as host runtime selectors, persisted aliases, v2
contract values, grants, image-manifest IDs, sidecar IDs, restore inputs, or
proxy authorization facts. Because 9a also claims the Claude command-plan
fresh/resume selector is sidecar-backed, 9a owns a narrow patch to the current
`harness-bridge-client` so runner-local `first_turn` state and filesystem
probing cannot override that projection. 9d owns removing the compatibility
fields and legacy env values when the runner reads the driver/provider
projection and bridge protocol v2 is required.

Contract v2 uses `contract_schema_version: 2` for the structural payload and a
separate `contract_gate_version` for validation profile. 9a/9b writes use
`contract_gate_version: "phase9a"` and must carry null runtime-config,
agent-manifest, and rootfs-image input digests. 9c+ writes use
`contract_gate_version: "phase9c"` and must satisfy the runtime-config and
image-manifest digest gates. The shared loader keys version-specific validation
from the persisted gate version, not from the current binary version or release
date.

9d is the protocol-v2 bridge cutover and owns the release reset for any
active/checkpointed generation that cannot prove bridge protocol v2 from
persisted allocation-time manifest evidence. That reset includes both 9c rows
whose persisted manifest proves only protocol v1 and older `phase9a`
generations with missing/null manifest evidence; a missing manifest is not
treated as implicit protocol-v2 support. Any reset that deletes or fails
active/checkpointed generations must run under the same owner-lock cleanup gate
as 9a: live provider, network, bridge/control, checkpoint, bundle, and
filesystem resources are cleaned, proven absent, or durably quarantined before
their DB ownership rows are removed.

Pi work in 9f depends on the generic 9a-9e contract path. Pi cannot be selected
from `/latest` documentation assumptions alone; exact CLI version, event schema,
RPC/session behavior, and normalizer corpus must be checked in as versioned
release evidence. The 9f schema update is a preserving constraint-widening
migration: it must not delete valid post-9a/9c/9e Claude Code or shell rows
unless a separately named release-reset gate explicitly owns active/checkpointed
session deletion.

## Later Work

Phase 10 adapter expectations and out-of-scope fanout semantics are tracked in
[Phase 10 and later](./slices/phase10-and-later.md).
