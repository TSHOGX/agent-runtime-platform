# Phase 9: Driver/Provider Contract and Pi Integration

> Status: implemented baseline on top of the completed [Phase 8 runtime isolation hardening](../phase8/README.md).
> Roadmap entry: [PLAN.md -> Phase 9](../PLAN.md#phase-9-agent-driver-and-pi-integration).

Phase 9 made "agent" a deployment-selected path built from two contracts:

- `AgentDriverSpec`: conversation semantics, runner protocol, output schema,
  model access, driver-private state, and Phase 10 adapter support.
- `RuntimeProviderSpec`: execution semantics, template/rootfs identity,
  process/filesystem/network primitives, resource identity, and snapshots.

The goal is not to add Pi as a special case. Pi is the first non-Claude coding
driver that proves the contract after Claude Code and shell continue to pass.

## Scope

Historical archive migration, backup tooling, down migrations, and old-data
safety are out of scope. Phase 9 uses an automatic destructive cutover: it may
delete pre-9a sessions, messages, artifacts, turns, events, runtime rows, v1
sandbox contracts, checkpoints, driver homes, and sidecars; constrained SQLite
tables may be rebuilt directly under the migration lock. Provider and
filesystem cleanup is coordinated by startup after the orchestrator owner lock
is held: cleanup inputs are captured in a durable in-progress marker, cleanup
helpers are idempotent, and already-absent resources are accepted on retry.
There is no manual approval, retained-history matrix, or reactivation workflow.
Discoverable live provider/isolation resources must be cleaned, proven absent,
or durably quarantined before their DB ownership rows are deleted; otherwise
the in-progress marker remains and runtime startup stays blocked. Disposable
old-data cleanup failures may be recorded as orphan inventory, but they do not
make pre-9a state resumable. Durable quarantine is represented by active
`runtime_resource_quarantine_tombstones` rows defined in the 9a slice; those
rows preserve the old allocator uniqueness guard until an owner-lock-held
reconciler release records absence evidence.

Persisted v1 sandbox contracts are not a compatibility target after the 9a v2
cutover. Old v1 rows may be deleted or fail closed; they do not require
backfill, restore, or proxy-authorization support. Exact v2 rules live in
[sandbox-contract-v2.md](./sandbox-contract-v2.md).

## Release Slices

The 9a-9f order, gates, and code touchpoints live in
[implementation-slices.md](./implementation-slices.md). The short version:
9a establishes the clean v2 contract/schema shape, 9b promotes hard-coded facts
into registries, 9c adds product-mode config and image-manifest gates, 9d
removes driver-specific bridge/output branching, 9e tightens secret grants, and
9f adds Pi through that established path.

9a and 9b can ship before 9c. If they do, public compatibility is boundary
translation: public API request and response DTOs may keep
`agent: "claude" | "sh"` only as a temporary compatibility adapter. Input maps
`agent: "claude"` to internal `driver_id: "claude_code"` and `agent: "sh"` to
`driver_id: "sh"`; output derives the legacy `agent` field back from canonical
`sessions.driver_id`. Config-default normalization maps
`HARNESS_DEFAULT_AGENT=claude` to `claude_code`. The public DTO adapter and
config-default adapter are removed or rejected at the 9c product-mode API
cutover. The only lower-layer legacy-token exception before 9d is the
sandbox-visible protocol-v1 projection required by the current runner:
canonical `claude_code` is projected to `agent: "claude"` /
`HARNESS_AGENT=claude` for the sandbox process, but that projected value is not
a host runtime selector, persisted alias, contract value, grant,
image-manifest ID, sidecar ID, restore input, or proxy authorization fact.

## Target Flow

```text
Session request
  -> product mode: Agent | Shell
  -> AgentDriverSpec + model profile
  -> RuntimeProviderSpec
  -> capability match
  -> DataVolumeProvisioner
  -> MountPlan
  -> SandboxContractCompiler
  -> RuntimeProvider(local_runsc today)
  -> sandbox AgentRunner
  -> OutputNormalizer
  -> control-plane events
```

Product users see `Agent` and, when the deployment includes the `sh`
capability, `Shell`. The deployed coding driver can be Claude Code, Pi, Codex,
OpenCode, or another registered driver without exposing that raw driver ID in
the workbench.

## Non-Negotiable Decisions

- `claude_code` is the canonical runtime driver ID. `claude` is a retired
  legacy token, not a registered runtime alias. Except for temporary 9a/9b
  public API compatibility DTOs, the config-default boundary adapter, and the
  protocol-v1 sandbox projection kept until 9d, it must not appear in new
  config, DB rows, v2 contracts, grant allowlists, driver homes, image
  manifests, restore paths, proxy authorization, or public DTOs. Any 9a/9b
  public `agent: "claude"` response field is derived from canonical
  `sessions.driver_id` and is removed at the 9c product-mode DTO cutover.
- Driver homes use an explicit `driver_home_key`; in the clean Phase 9 schema
  that key is canonical and matches the selected runtime driver ID.
- From the 9c cutover onward, public session APIs use product mode:
  `mode: "agent" | "shell"`. Internal runtime selection uses DB-enforced
  canonical `sessions.driver_id`; public DTOs must not expose raw driver IDs,
  runtime generation IDs, host paths, DataVolume evidence, restore IDs, or
  driver-private state. Product `mode: "shell"` always selects
  `driver_id: "sh"` when the deployment enables
  Shell and the image manifest contains `sh`; otherwise Shell is hidden/disabled
  and API creation fails before persistence. Product `mode: "agent"` selects an
  enabled non-shell agent driver from config; `sh` is not a valid
  `harness.default_agent`.
- For clean post-cutover rows, allocation, DataVolume provisioning, runtime
  start, output parsing, interrupt support checks, restore, and proxy
  authorization read canonical `sessions.driver_id`. Legacy `sessions.agent`
  is not a migration source and is not read by v2 runtime selectors. If a
  deployable 9a.1 staging build can still encounter pre-cutover rows, it must
  keep a quarantined legacy-row selector and legacy DTO projection for those
  rows until 9a.4, or ship 9a.1 and 9a.4 atomically. The 9a cutover deletes
  pre-9a session rows instead of backfilling `driver_id`; any 9a/9b public
  `agent` field for clean/new rows is derived from canonical
  `sessions.driver_id`.
- Model access and provider credential posture come from the selected driver
  spec, model profile, runtime provider, and strict grants. New code must not
  infer entitlement from legacy checks such as `agent == "claude"`.
- Sandbox credentials stay host/proxy-only. Phase 9 rejects non-model secret
  domains and all OS-visible secret exposure modes.
- `/harness-control/driver/<driver_id>/` is generated inside the existing
  read-only `/harness-control` projection. It is not a new bind mount.
- Driver-private state is mutable sidecar evidence in `session_driver_states`.
  Contracts snapshot only the initial state digest for a generation. First
  allocation for a brand-new session/driver pair creates the generation row and
  bootstrap sidecar in one transaction; session creation alone does not create
  sidecar state.
- New physical checkpoint restore is fenced by persisted
  `checkpoint_driver_states_digest` metadata and current sidecar validation.
  Pre-9a checkpoints without the fence are deleted or rejected rather than
  restored through a legacy path.
- Runtime and driver capability digests are versioned allocation fences. Moving
  9a hard-coded facts into 9b registries must not change bytes for unchanged
  facts.
- `contract_schema_version: 2` is paired with a durable
  `contract_gate_version`. 9a/9b writes use `phase9a`; 9c+ writes use
  `phase9c` so nullable pre-9c input digests are not confused with invalid
  9c+ contracts.
- Runtime config and image/template digests are allocation-time evidence for a
  generation. Current deployment config changes block or change only new
  allocations; existing active/checkpointed generations validate against their
  persisted preimage/artifact evidence and checkpoint fences.
- Current config disables are not historical deletion gates. Automatic cutovers
  may delete missing, legacy, unknown, or malformed rows, but purging valid rows
  for a now-disabled driver requires a separately named destructive reset with
  explicit provider cleanup/quarantine behavior.
- The image manifest gate landed in 9c before Pi selection. It applies to every
  selected driver, including `sh`.

## Document Map

- [implementation-slices.md](./implementation-slices.md): 9a-9f execution
  index. Detailed checklists, code touchpoints, and gates live under
  `slices/`.
- [current-code-map.md](./current-code-map.md): pre-Phase 9 code facts and the
  Phase 9 slice that changed each area.
- [sandbox-contract-v2.md](./sandbox-contract-v2.md): contract schema v2
  index and v1 sunset rules. Focused contract references live under
  `contract/`.
- [driver-state.md](./driver-state.md): mutable sidecar state, CAS updates,
  driver-state digests, and checkpoint fencing.
- [runtime-capabilities.md](./runtime-capabilities.md): capability vocabulary
  v1, allocation enforcement, and digest rules.
- [secret-grants.md](./secret-grants.md): `credential_policy.secret_grants[]`,
  digest coverage, and Phase 9 credential boundary.
- [pi-driver.md](./pi-driver.md): Pi rootfs, generated config, RPC runner,
  restore state, output mapping, and Pi-specific gates.

## Gate Sources

Release gates are maintained in their owning documents:

- 9a-9f slice gates: [implementation-slices.md](./implementation-slices.md)
  and the linked per-slice files.
- Contract validation gates: [contract/validation-gates.md](./contract/validation-gates.md).
- Credential gates: [secret-grants.md](./secret-grants.md).
- Pi-specific gates: [pi-driver.md](./pi-driver.md).

Phase 10 must consume this adapter surface. System prompt, compaction, skills,
hooks, MCP, interrupt, and output handling must route through explicit driver
adapters or normalizers; silent no-op behavior is not allowed.

## References

- Phase 8 runtime isolation baseline: [docs/phase8/README.md](../phase8/README.md)
- Phase 8 sandbox contract: [docs/phase8/sandbox-contract.md](../phase8/sandbox-contract.md)
- Phase 10 adapter plan: [docs/phase10/README.md](../phase10/README.md)
- Pi discovery references are intentionally non-normative because `/latest`
  can drift. Pinned CLI/schema evidence is committed for the selected Pi
  version, and future Pi upgrades must refresh that evidence before rollout;
  see [pi-driver.md](./pi-driver.md#release-evidence).
