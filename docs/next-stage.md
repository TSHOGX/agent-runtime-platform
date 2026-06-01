# Next Stage: Generation Plan And Capability Plane

The next stage is a behavior-preserving architecture refactor. A new runtime
generation should launch from one immutable, host-only `GenerationPlan`; the
sandbox contract, control manifest, OCI spec, mount plan, bridge config, and
driver config artifacts become deterministic projections of that plan.

This is not a service split and not a rewrite of the session, turn, bridge, or
runtime lifecycle state machines. Existing mutable tables continue to own
leases, ready/live state, checkpoint transitions, post-start proof, cleanup
evidence, output, and events.

## Scope

In scope:

- Persist a canonical generation plan before runtime materialization.
- Keep first-stage browser API, bridge protocol, control manifest wire shape,
  `Agent` mode, and `Shell` mode compatible.
- Make restored generations reuse their original plan, capability snapshots,
  content digests, and runsc pin.
- Validate enabled platform features against typed driver/provider
  capabilities before runtime prepare.
- Route driver-specific prompt, settings, hook, MCP registration, compaction,
  interrupt, and config rendering through driver adapters.
- Mount platform-managed content only through immutable, content-addressed
  snapshots.

Out of scope:

- Multi-orchestrator HA, multi-tenant authz, or a new storage backend.
- Permanent compatibility migrations for old runtime generation data.
- Credential-bearing MCP. Only non-secret managed settings and non-secret
  remote MCP registration are in scope.
- Bridge protocol, rootfs entrypoint, or control manifest semantic changes in
  the first behavior-preserving slice.

## Target Flow

```text
deployment config
  + product mode
  + selected driver/provider specs
  + image manifest
  + allocated generation/resource/profile/network rows
  + provisioned workspace and driver-home volumes
  + runsc provider snapshot
  + content snapshots
    -> GenerationPlanBuilder
    -> canonical GenerationPlan + plan digest
    -> projection renderers
         sandbox contract
         control manifest
         OCI spec
         mount plan
         bridge config
         driver config artifacts
    -> runtime materialization
    -> runsc start or restore
    -> bridge startup proof
    -> live generation
```

Projection renderers are pure functions of the persisted plan. They must not
read live deployment config, mutable repo paths, host secrets, or mutable
lifecycle state.

## Core Invariants

- The plan is persisted before files, network, resource instances, or runsc
  state are materialized.
- The plan contains immutable launch inputs only.
- Projection artifact digests are stored outside the plan to avoid digest
  cycles.
- Runtime launch, restore, and checkpoint verify the current provider snapshot
  against the plan's runsc pin.
- Unsupported enabled features fail before runtime prepare.
- Driver-specific behavior enters only through driver adapters.
- Live secrets never appear in prompts, snapshots, argv, env, logs, bridge
  queues, `/workspace`, or `/agent-home`.

## Plan Boundary

`GenerationPlan` records immutable launch facts:

- version
- session ID, generation ID, product mode, selected driver, and selected model
- runtime provider ID/profile and runsc pin
- network identity and egress policy inputs
- workspace and driver-home DataVolume evidence
- allowed mount facts, including generated driver config mounts
- initial driver state token and digest
- selected driver/provider capability snapshots
- normalized feature policy
- content snapshot references for skills and non-secret managed settings
- source deployment config, image manifest, and adapter input digests

It does not record:

- generation leases
- runtime resource state such as allocated, ready, live, or reconciling
- post-start proof
- checkpoint state transitions
- retained output, events, or cleanup evidence

Those remain in `runtime_generations`, `runtime_resource_instances`, event
tables, and existing checkpoint/resource records.

The initial type should be explicit and versioned:

```text
Plan{
  Version,
  Session,
  Generation,
  ProductMode,
  Driver,
  Model,
  RuntimeProvider,
  RunscPin,
  Network,
  DataVolumes,
  Mounts,
  DriverState,
  FeaturePolicy,
  ContentSnapshots,
}
```

The `generationplan` package owns:

- `BuilderInput`
- `Plan`
- `Build(input)`
- `Canonicalize(plan)`
- `Digest(plan)`
- `Validate(plan)`
- `RenderSandboxContract(plan)`
- `RenderControlManifest(plan)`
- `RenderOCISpec(plan)`
- `RenderMountPlan(plan)`
- `RenderBridgeConfig(plan)`
- `RenderDriverConfigs(plan)`

Validation rejects missing identity, unsupported plan versions, malformed
digests, non-canonical paths, mutable content references, unsupported feature
policy, and driver/provider capability mismatches.

## Persistence

Add immutable plan storage:

```sql
CREATE TABLE generation_plans (
  generation_id TEXT PRIMARY KEY
    REFERENCES runtime_generations(generation_id) ON DELETE CASCADE,
  plan_version INTEGER NOT NULL CHECK(plan_version = 1),
  canonical_payload TEXT NOT NULL,
  plan_digest TEXT NOT NULL CHECK(plan_digest LIKE 'sha256:%'),
  created_at TEXT NOT NULL
);
```

Store API:

- `StoreGenerationPlan(ctx, params)`
- `GetGenerationPlan(ctx, generationID)`
- `RequireGenerationPlanForLaunch(ctx, generationID)`

Insert semantics:

- First insert stores the canonical payload and digest.
- Duplicate insert is idempotent only when payload and digest match exactly.
- Payload or digest changes fail instead of updating the row.
- After plan-backed launch is enabled, every non-terminal generation must have
  a plan row before prepare, restore, checkpoint, or live use.

## Launch Ordering

Fresh generation launch:

1. Allocate generation, resource paths, runtime profile, and network profile.
2. Provision workspace and selected driver-home data volumes.
3. Resolve deployment config, rootfs image manifest, selected driver, and
   runtime provider.
4. Discover the runtime provider snapshot, including runsc pin.
5. Build, canonicalize, validate, and persist `GenerationPlan`.
6. Render projections and record projection digests.
7. Create or claim the runtime resource instance.
8. Materialize files and network from rendered projections.
9. Start runsc only when the current provider snapshot matches the plan pin.
10. Verify bridge startup proof and mark the resource live.

Checkpoint restore loads the original plan and stored artifacts. It must not
rebuild launch policy from current deployment config.

## Responsibilities

- `server`: session lifecycle, orchestration, CAS, and cutover policy. It does
  not hand-build sandbox-contract or driver-policy JSON.
- `generationplan`: build, validate, canonicalize, digest, and render pure
  projections.
- `driver adapters`: declare capabilities and render driver-specific artifacts
  from validated plan sections.
- `runtime`: write rendered files, prepare network, verify runsc pin, and
  execute start, restore, checkpoint, or delete.
- `store`: persist immutable plans separately from mutable lifecycle state.

`runtime.StartRequest` should carry the persisted plan digest or rendered
projection references. Runtime should stop recomputing launch facts from live
config.

## Runtime Provider Pin

Runtime should expose read-only discovery:

```text
Runtime.ProviderSnapshot(ctx) -> {
  provider_id,
  platform,
  runsc_version,
  runsc_binary_path,
  runsc_binary_digest
}
```

The builder stores that snapshot in `Plan.RunscPin`. Launch, restore, and
checkpoint compare the current snapshot with the plan pin and fail before runsc
execution if they differ.

## Capability And Adapter Rules

Replace string feature markers with typed declarations:

- `FeatureID`
- `SubCapabilityID`
- `DriverCapabilities`
- runtime-provider capabilities where needed

First feature set:

- operator policy prompt
- context usage reporting
- hard context budgets
- compaction
- skills snapshot
- managed settings
- hooks
- remote MCP registration
- interrupt

Adapters are the only driver-specific rendering boundary. They consume
validated plan sections and return rendered artifacts or typed errors. They do
not read live deployment config, host secrets, mutable repo paths, or lifecycle
state.

Initial compatibility expectations:

- Claude Code keeps current behavior while supported features move behind its
  adapter.
- Pi fails closed for unsupported features until its adapter declares support.
- Shell receives no model, skills, managed settings, or agent policy unless it
  explicitly supports those capabilities.

## Content Snapshots

Add a shared snapshot subsystem for platform-managed, non-secret content such as
skills and managed settings. Snapshot output is:

- kind
- digest
- read-only host path
- mount destination
- source evidence digest

The plan stores snapshot references and digests only. Skills mount as a
read-only exact bind at `/harness-skills`; mutable source repo paths are never
mounted directly into the sandbox.

Artifact watching remains scoped to verified workspace paths. Skills and
managed settings are not scanned, listed, downloaded, or persisted as
artifacts.

## Compatibility And Cutover

First-stage external compatibility:

- Browser API remains compatible.
- `GET /api/deployment-capabilities` remains product-mode oriented.
- Operator `GET /api/agents` may add a non-breaking `capabilities` field while
  retaining `supports_interrupt` and `supports_compaction`.
- Bridge protocol and control manifest wire shape remain compatible.

Cutover rule:

- Plan-backed launch supports new generations only.
- After enablement, non-terminal generations without `generation_plans` rows
  are incompatible and should be removed by one operational cold cutover.
- Do not add long-lived compatibility paths for old generation rows.

If a later stage changes control manifest semantics, bridge protocol, runtime
scripts, rootfs-contained code, or rootfs dependencies, follow the local
cold-cutover rule: rebuild or resync rootfs, restart the main backend/frontend
panes in `hp:serve`, destroy old runsc containers, remove stale main sandbox
network resources, clear incompatible runtime dirs under `/var/lib/harness/run`,
and smoke test a fresh session.

## Test Plan

Store tests:

- plan payload and digest are canonical and stable
- duplicate insert is idempotent only for identical payload and digest
- plan row is immutable
- launch/restore guards reject non-terminal generations without plans

Projection tests:

- identical plans render identical sandbox contracts, control manifests, OCI
  specs, mount plans, bridge config, and driver configs
- sandbox contract mount facts and OCI spec mount facts match
- projection digests do not affect plan digests

Launch flow tests:

- fresh launch uses plan-backed projections
- restore reuses the original plan
- runsc pin mismatch blocks launch, restore, and checkpoint
- unsupported features block before runtime prepare

Capability and snapshot tests:

- Pi fails closed for unsupported policy prompt, skills, compaction, and related
  policy
- Claude Code supported features render only through its adapter
- Shell receives no model, skills, or managed settings unless supported
- skills snapshot digest is stable
- `/harness-skills` is read-only and exact
- artifact list/download remain workspace-only

Release gates:

- update static manifests for plan and capability requirements
- run existing Go tests
- run sandbox isolation gates
- run control-plane gates
- run a fresh-session live smoke for the selected deployment driver

## Implementation Order

1. Add immutable plan persistence and build current launch facts into
   `GenerationPlan` without behavior changes.
2. Move sandbox contract, control manifest, OCI spec, mount plan, bridge config,
   and driver config rendering behind projection functions.
3. Make runtime prepare/start consume persisted plan digests or projection
   references.
4. Move runsc provider discovery before plan persistence and enforce pin
   matching on launch, restore, and checkpoint.
5. Replace string feature markers with typed feature and sub-capability
   declarations.
6. Add adapter-backed policy prompt, context budget, skills snapshot, managed
   settings, hooks, and non-secret remote MCP support.
7. Execute the one-time cold cutover for incompatible old generation data.

## Acceptance Bar

- `Agent` and `Shell` still launch through current product-mode selection.
- Existing browser API, bridge protocol, and first-stage control manifest wire
  shape remain compatible.
- Sandbox contract, control manifest, OCI spec, mount plan, bridge config, and
  driver config artifacts are projections of one persisted plan.
- Prepared, live, checkpointed, and restored generations keep their original
  plan digest, capability snapshots, runsc pin, and content digests.
- Unsupported enabled features fail before runtime prepare.
- Artifact browsing remains workspace-scoped and never records skills or
  managed-settings content.
