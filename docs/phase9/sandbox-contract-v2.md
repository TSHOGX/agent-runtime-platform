# Sandbox Contract Schema v2

Phase 9 keeps the Phase 8 outer contract label:

```json
"sandbox_contract_version": "sandbox-isolation-v1"
```

The JSON payload schema changes from `contract_schema_version: 1` to
`contract_schema_version: 2`. After the 9a cutover, new writes are v2 only and
old v1 rows are not a compatibility target.

Contract schema version 2 has a separate top-level `contract_gate_version` for
validation profile. Phase 9 uses:

- `phase9a`: 9a/9b v2 payloads. Runtime-config, agent-manifest, and
  rootfs-image input digests are not available yet and must be null.
- `phase9c`: 9c+ v2 payloads. Runtime-config and image-manifest digest gates
  are active for new writes and validation.

`contract_gate_version` is a durable discriminator, not a replacement for the
schema major version. Unknown gate versions fail closed.

Historical v1 data migration, backup, down migration, and backfill plans are
not part of Phase 9. 9a may delete existing v1 `sandbox_contracts`,
`runtime_generations`, `network_profiles`, `runtime_generation_resources`,
`runtime_resource_instances`, sessions, messages, artifacts, turns, events,
driver homes, and sidecars as part of the cutover to the clean v2 shape. The
cutover may read `runtime_resource_instances.resource_identity_payload` for
automatic provider cleanup before deleting rows. Provider/filesystem cleanup is
coordinated by the 9a startup cutover coordinator after the orchestrator owner
lock is acquired, not by a DB-only migration callback. It writes a durable
in-progress marker before non-transactional cleanup and retries idempotently if
cleanup succeeds but a later DB transaction rolls back or the process crashes.
Discoverable live isolation resources must be cleaned, proven absent, or
durably quarantined before their ownership rows are removed. If that live
cleanup gate cannot be satisfied, the in-progress marker remains and runtime
startup stays blocked. Old cleanup evidence is disposable unless it is the
active `runtime_resource_quarantine_tombstones` row for a quarantined live
resource; those tombstones are first-class schema rows and continue to block
allocator/reconciler reuse after the old ownership rows are deleted.

Before removing v1 `runtime_generations`, the cutover may either unlink
references or delete the referencing `sessions`, `messages`, `artifacts`,
`turns`, and `events` rows directly. There is no retained-session matrix,
whole-session purge choice, or reactivation path; live-resource quarantine is
only a safety tombstone that prevents identity reuse. After the destructive
cutover, runtime allocation, reconnect, restore, proxy authorization, and
driver-state bootstrap operate only on post-cutover v2 rows.

9a must persist `sandbox_contracts.contract_schema_version` and
`sandbox_contracts.contract_gate_version` for new rows and require both columns
to match the payload. Store, runtime, restore, and proxy authorization paths
must use one audited contract-loading helper, and that helper is v2-only after
cutover. Restore and proxy authorization must not independently parse contract
payloads or reconstruct partial contract facts. Persisted v1 rows fail closed
or are removed; there is no Phase 8 read/auth compatibility branch and no
checkpoint restore path for pre-9a no-fence generations.

9c must write `contract_gate_version: "phase9c"` for new contracts. Existing
`phase9a` rows with null 9c-only digests remain distinguishable as pre-9c
contracts, or an automatic destructive cutover may delete them. If that cutover
deletes active/checkpointed generations or their runtime ownership rows, it
must use the owner-lock cleanup/quarantine gate before row deletion. The loader
must not silently reinterpret a `phase9a` row as `phase9c`.

9d cannot infer bridge protocol support from those nullable 9a digests. Any
active/checkpointed generation without persisted allocation-time image-manifest
evidence must be reset, deleted, or explicitly failed before the host requires
bridge protocol v2. That reset uses the same owner-lock cleanup/quarantine
gate as the 9a destructive cutover before deleting DB ownership rows for live
provider, network, bridge/control, checkpoint, bundle, or filesystem resources;
missing manifest evidence is incompatible with v2 reconnect/restore.

The shared v2 loader validates immutable contract shape and allocation-time
evidence. It must not silently turn mutable evidence into live equality checks:
current driver sidecar equality is an allocation/start or checkpoint-restore
gate, and current deployment config applies to new allocations only.

## Contract Reference Map

The v2 contract is split into focused reference files so agents can load only
the part they are changing:

- [Target shape](./contract/target-shape.md): full canonical payload example.
- [Driver object](./contract/driver-object.md): canonical driver identity and
  command/config digest algorithms.
- [Runtime provider](./contract/runtime-provider.md): provider-specific runtime
  facts, resource identity digest, and template digest algorithm.
- [DataVolume evidence](./contract/data-volumes.md): workspace and agent-home
  DB/evidence checks.
- [Snapshot policy](./contract/snapshot-policy.md): provider-derived snapshot
  projection and fanout exclusions.
- [Driver runtime and credentials](./contract/driver-runtime-and-credentials.md):
  initial state digest, mutable sidecar boundary, and credential-policy summary.
- [Input and artifact digests](./contract/input-and-artifact-digests.md):
  runtime config, rootfs, agent manifest, and rendered artifact digest rules.
- [Control projections](./contract/projections.md): sandbox-visible control
  manifest and generated driver config projection.
- [Validation gates](./contract/validation-gates.md): reject conditions for v2
  validation and old v1 payloads.

## Ownership Notes

Driver-state CAS rules, sidecar digests, and checkpoint fencing are normative in
[driver-state.md](./driver-state.md). Credential grant field coverage and Phase
9 exposure limits are normative in [secret-grants.md](./secret-grants.md).
Runtime capability vocabulary and provider capability digests are normative in
[runtime-capabilities.md](./runtime-capabilities.md). Pi-specific config,
runner, state, and release evidence rules are normative in
[pi-driver.md](./pi-driver.md).

Implementation order and release gates are tracked from the slice index in
[implementation-slices.md](./implementation-slices.md).
