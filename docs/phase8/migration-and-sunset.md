# Phase 8 Migration and Sunset

Phase 8 is a destructive one-way cutover to `sandbox-isolation-v1`. The lab does
not preserve or upgrade pre-Phase-8 sessions, workspaces, agent homes,
checkpoint images, prepared bundles, manifests, live containers, or runtime DB
rows.

Optional backups are operator inspection artifacts only. They are not imported
into Phase 8 bind sources and are not release evidence for the new contract.

## Cutover Sequence

1. Stop orchestrator, proxy, bridge processors, and sandbox workers.
2. Optionally take an offline host/DB backup.
3. Move or recreate the orchestrator DB outside every sandbox-bindable root.
   Preferred path: `<state_root>/db/orchestrator.db`, configured by
   `HARNESS_DB_PATH`.
4. Inventory all old runtime identities, legacy secret roots, provider
   credential roots, and proxy-internal sockets.
5. Kill/delete old `runsc` containers.
6. Delete old netns, veth pairs, nft tables, and verify host absence.
7. Delete or quarantine old runtime/control/bridge/log/checkpoint/prepared
   directories and legacy secret roots outside every Phase 8 root.
8. Wipe old session rows, messages, turns, artifacts, durable events, active
   proxy contexts, runtime generations, runtime profiles, workspaces, and agent
   homes.
9. Apply Phase 8 schema and remove the public/API dependency on legacy session
   path fields and `restore_id`.
10. Recreate empty inspected roots with Phase 8 ownership and permissions.
11. Start only new `sandbox-isolation-v1` sessions and generations.

There is no compatibility mode for old manifests, old checkpoints, old parent
mount specs, old workspace layouts, or old driver homes.

## DB and Root Policy

The DB/control-plane root must be disjoint from:

- `<sessions_root>`
- `<agent_homes_root>`
- runtime roots
- checkpoint roots
- prepared-bundle roots
- rootfs/content roots
- schema-pack and future Phase 9 content roots
- DataVolume provisioning evidence roots, unless configured as a reserved
  host-only subroot under the DB/control-plane root
- file-backed provider credential roots
- proxy-internal socket roots

After cutover, `<sessions_root>` contains only per-session workspace
directories. No `orchestrator.db` exception is allowed.

Phase 8 removes the legacy `harness.secrets.root` configuration and sandbox
secret mount. Existing secret roots are not active roots; they are deleted or
quarantined before enablement and are not imported into Phase 8 credential
storage.

## Runtime Resource State

Generation-owned host resources are governed by `runtime_resource_instances`.
Split Phase 7 states such as `network_profiles.allocation_state` and
`runtime_generation_resources.resource_state` may remain as diagnostic mirrors,
but they do not authorize reuse.

Required row shape:

```text
runtime_resource_instances(
  generation_id PRIMARY KEY,
  session_id NOT NULL,
  contract_id NOT NULL,
  sandbox_contract_version NOT NULL,
  worker_id,
  host_id NOT NULL,
  state NOT NULL,
  runsc_container_id NOT NULL,
  runsc_platform NOT NULL,
  runsc_version NOT NULL,
  runsc_binary_path NOT NULL,
  runsc_binary_digest NOT NULL,
  network_profile_id NOT NULL,
  netns_name NOT NULL,
  netns_path NOT NULL,
  host_veth NOT NULL,
  sandbox_veth NOT NULL,
  host_gateway_ip NOT NULL,
  sandbox_ip NOT NULL,
  sandbox_ip_cidr NOT NULL,
  host_side_cidr NOT NULL,
  nft_table_name NOT NULL,
  control_dir_path NOT NULL,
  control_manifest_path NOT NULL,
  bundle_dir_path NOT NULL,
  spec_path NOT NULL,
  checkpoint_path,
  bridge_dir_path NOT NULL,
  network_hosts_path,
  log_dir_path NOT NULL,
  resource_identity_payload NOT NULL,
  resource_identity_digest NOT NULL,
  evidence_json,
  evidence_digest,
  verified_at,
  updated_at NOT NULL
)
```

`resource_identity_payload` is immutable canonical JSON for the destructive
cleanup identities in the row: container, network, generation-owned paths,
host/root identifiers, and typed prefixes. Its digest is verified before any
delete, quarantine, or `absent_verified` transition. Bare path/container/network
columns are mirrors only; if they disagree with the payload, automated cleanup
and identity reuse stop.

Authoritative state graph:

```text
allocated
  -> materializing
  -> ready
  -> live

live
  -> checkpoint_reserved
  -> materializing
  -> ready
  -> live

allocated | materializing | ready | live | checkpoint_reserved
  -> retiring
  -> reconciling
  -> absent_verified
  -> destroyed
```

State meanings:

- `allocated`: DB identity is reserved; host resources are not materialized.
- `materializing`: worker is creating or restoring host resources.
- `ready`: artifacts are prepared for the same generation and current contract;
  no other generation may reuse the identities, but proxy authorization and
  bridge polling are still disabled.
- `live`: runtime/network/bridge identity is active after post-start proof for
  the expected `runsc_container_id`, matching `runsc` platform, version, and
  binary digest, network namespace/veth/nft evidence, and bridge startup
  readiness.
- `checkpoint_reserved`: live container is gone, checkpoint identity is retained
  for same-generation restore.
- `retiring`: execution state has fenced normal use and cleanup is required.
- `reconciling`: host resources are being deleted, quarantined, or verified.
- `absent_verified`: durable host evidence proves resources are absent or moved
  outside all Phase 8 roots. Only this state permits allocator identity reuse.
- `destroyed`: archival terminal state reached only after `absent_verified`.

Forbidden transitions include direct `ready -> materializing`, direct
`checkpoint_reserved -> destroyed`, direct `checkpoint_reserved ->
absent_verified`, cross-generation restore, and any direct transition to
`destroyed` that releases identity without absence evidence.

Partial unique indexes on generation-owned identities exclude only
`absent_verified` and `destroyed`. The same rule applies to sandbox IPs, `/30`
slots, netns names/paths, veth names, nft tables, `runsc_container_id`, control
paths, spec paths, bundle paths, checkpoint paths, bridge paths, network hosts
paths, and log paths.

`runsc_container_id` is generation-scoped, for example
`harness-gen-<generation_id>`. Runtime start, restore, cleanup, fallback
destruction, and release evidence must not use `sessions.restore_id`,
`session_id`, or `phase3-<session_id>` as the runsc identity.

## Store Fences

Every normal store-driven runtime path requires:

- `sandbox_contract_version = sandbox-isolation-v1`;
- verified canonical contract payload and matching digest;
- the expected `runtime_resource_instances.state`.

Required states:

- fresh materialization claim: worker-owned CAS from `allocated` to
  `materializing`, recording `worker_id`, `host_id`, lease/deadline, and an
  idempotency token before creating any host resource;
- fresh materialization completion: only the owning worker may CAS
  `materializing` to `ready` after all generation-owned artifacts are rendered,
  DataVolume rows and provisioning evidence are verified, and structural
  pre-start validation passes for the same contract;
- prepared launch or restore start: `ready`;
- startup bridge readiness probe: `ready`, limited to the startup-only bridge
  path described below;
- normal bridge polling, heartbeat renewal, live turn claims, proxy
  authorization, and checkpoint creation: `live`;
- checkpoint restore claim: `checkpoint_reserved`, then CAS to `materializing`;
- cleanup claim: CAS from `allocated`, `materializing`, `ready`, `live`, or
  `checkpoint_reserved` to `retiring`;
- cleanup execution: `retiring` or `reconciling`;
- allocator identity reuse: previous owner reached `absent_verified`.

Old or null contract rows after cutover are store drift and are rejected for
launch, restore, checkpoint, bridge polling, live turn claims, and proxy
authorization. Cleanup may enter corruption-mode reconciliation: it uses only
identities from a verified `runtime_resource_instances.resource_identity_payload`,
records contract drift evidence, validates root/prefix ownership before each
destructive action, and cannot release allocator identity until
`absent_verified`. If resource identity proof is missing or corrupt, cleanup is
manual/operator reconciliation only.

Fresh materialization is single-owner. A worker that loses its lease or whose
`worker_id` no longer matches may not complete `materializing -> ready`; it must
stop and let cleanup/reconciliation or a lease-steal path take ownership. A
lease-steal path is allowed only after the previous owner is proven dead or the
lease deadline has expired, and it must inventory every generation-owned path,
network identity, and rendered artifact before continuing. If any partial
artifact has an unexpected digest, owner, type, symlink target, mountpoint, or
root/prefix mismatch, the row moves to cleanup/reconciliation instead of
continuing materialization in place. This same rule covers crash recovery after
`allocated -> materializing`: either the new owner proves the partial state is
exactly the expected idempotent output for the same contract or it reconciles
and re-materializes from `absent_verified`/fresh allocation.

If the current worker `runsc` platform, version, or binary digest differs from
the recorded contract, cleanup first tries the recorded compatible `runsc`
binary only when that binary is still present at a canonical path and its digest
matches the contract. If no compatible binary is available, or if the compatible
binary cannot delete the old container or checkpoint, `HostStateReconciler`
quarantines only the host resources it can verify independently and records a
manual reconciliation requirement for the unresolved `runsc` identity. The
resource remains `retiring` or `reconciling`; partial unique indexes continue to
block reuse until durable absence or quarantine evidence permits
`absent_verified`.

Workers must not mark `ready -> live` before `RuntimeAdapter.Start` or restore
returns post-start proof and the bridge startup readiness probe passes. This
readiness probe is a one-shot startup proof, not normal live bridge polling.
While the resource is `ready`, the startup probe may read only the expected
bridge outbox for the same generation and may answer only bootstrap `hello`,
bootstrap heartbeat, and `probe_network` envelopes. It must not lease turns,
resume turns, create active model contexts, accept model proxy correlation,
renew normal live heartbeats, checkpoint, or publish user output. The probe
records the observed envelope IDs, bridge ACL evidence, and digest/contract
identity in startup evidence, then the owning worker CASes `ready -> live`.
Normal bridge processors start only after that CAS succeeds. A failed
start/restore after `ready` goes to cleanup/reconciliation; it must not leave
network, bridge, proxy, or resource rows live.

## Schema Cutover

Apply schema for:

- `sandbox_contracts`;
- `sandbox_contract_artifacts` sidecar evidence for rendered network-hosts,
  manifest, OCI spec, bundle, and checkpoint metadata digests;
- contract references on generations and resource instances;
- `runtime_resource_instances`;
- generation-scoped `runsc_container_id`;
- generation-owned `network_hosts_path`, when the alias projection is rendered;
- `runsc_platform`, `runsc_version`, `runsc_binary_path`, and
  `runsc_binary_digest` mirrors used for start/restore proof;
- `checkpoint_runsc_platform`, `checkpoint_runsc_version`,
  `checkpoint_runsc_binary_path`, and `checkpoint_runsc_binary_digest` metadata
  used for checkpoint compatibility;
- sandbox identity fields on runtime profiles;
- model entitlement fields on runtime profiles and active contexts;
- `session_workspaces`;
- `session_driver_homes`;
- artifact metadata, watcher indexes, and download paths that resolve through
  verified `session_workspaces` evidence instead of public session rows or
  reconstructed root-plus-session-ID paths;
- public API DTOs and frontend types that omit `workspace`, `agent_home_path`,
  `restore_id`, and host runtime paths.

Migration does not backfill old provider-secret profile references into normal
Phase 8 Claude profile identity.

## Identity and Entitlement Changes

Changing sandbox UID/GID/groups, root paths, `runsc` platform, version, binary
path, or binary digest, `model_access_allowed`, or
`sandbox_model_proxy_base_url` is a data cutover:

1. Stop new allocations and checkpoint creation.
2. Let live generations finish or retire them.
3. Reconcile generation-owned resources to `absent_verified`.
4. Clear active model contexts.
5. Wipe and reprovision affected persistent workspace/home rows and roots when
   identity or root layout changed.
6. Allocate fresh contracts under the new policy.

A future production-preserve path must be a trusted host-side reprovision flow
with explicit inventory of hardlinks, xattrs, capabilities, ACLs, sockets,
FIFOs, device nodes, and marker evidence. Entrypoint-time `chown -R` is not a
Phase 8 migration policy.

## Phase 2 Tooling

Before Phase 8 is complete, `bundle/bake-bundle.sh` and
`bundle/restore-sandbox.sh` must either emit/restore `sandbox-isolation-v1`
specs or be quarantined as pre-Phase-8-only smoke tooling. Legacy Phase 2
bundles cannot be release evidence.

## Rollback

Rollback to pre-Phase-8 runtime code is unsupported after migration. Recovery is
restore from an operator backup into a pre-Phase-8 environment, or continue
forward by fixing Phase 8 state and rerunning gates.

## Required Evidence

Cutover evidence records:

- optional offline backup metadata;
- old runtime inventory;
- host absence checks for old `runsc`, netns, veth, nft, runtime, control,
  bridge, network projection, log, checkpoint, and prepared resources;
- legacy secret root deletion/quarantine output;
- provider credential root and proxy-internal root validation output;
- destructive session/workspace/agent-home wipe output;
- clean root provisioning output;
- DB path and root disjointness validation;
- DataVolume provisioning evidence root validation;
- resolved `runsc` platform, version, binary path, and binary digest;
- DataVolume provisioning evidence;
- canonical contract fixtures and digest verification;
- resource identity payload and digest verification;
- resource transition evidence proving reuse requires `absent_verified`.
