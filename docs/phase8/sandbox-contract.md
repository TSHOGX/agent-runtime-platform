# Phase 8 Sandbox Contract

`sandbox-isolation-v1` is the persisted runtime contract for each generation.
It is the source of truth for sandbox content mounts, runtime identity, network
identity, data volume ownership, runtime adapter requirements, credential
policy, and model entitlement.

Runtime paths load and verify the stored payload. They do not reconstruct
security facts from current config, host paths, env vars, script defaults, or
session IDs after allocation.

## Payload Shape

The persisted payload is canonical JSON produced by one repo-owned
canonicalizer with golden fixtures. Rules: UTF-8 JSON object keys sorted
lexicographically, stable list order, no insignificant whitespace, no
wall-clock-dependent fields, explicit null/absence rules per field, and no
floating-point values in security-relevant payloads. A future implementation in
another language must match the fixtures byte-for-byte. The payload does not
contain its own digest.

Minimum shape:

```text
sandbox_contract_version: sandbox-isolation-v1
contract_schema_version: 1
contract_id: <contract id>
session_id: <session id>
generation_id: <generation id>
driver: claude | sh | future
runtime_profile_id: <agent_runtime_profile_id>
network_profile_id: <network_profile_id>

identity:
  sandbox_uid: <positive non-root uid>
  sandbox_gid: <positive non-root gid>
  sandbox_supplemental_gids: [...]

mount_plan:
  workspace:   session_workspaces[session_id].host_path -> /workspace rw
  agent_home:  session_driver_homes[session_id, driver].host_path -> /agent-home rw
  control:     <run dir>/control/gen-<generation_id> -> /harness-control ro
  bridge:      <run dir>/bridge/gen-<generation_id> -> /harness-control/bridge rw
  network_hosts: optional <run dir>/network/gen-<generation_id>/hosts -> /etc/hosts ro
  schema_pack: optional ro exact content bind
  skills:      optional Phase 9 ro exact content bind
  scratch:     explicit tmpfs/cache mounts

network_identity:
  runsc_network: sandbox
  sandbox_ip: <exact sandbox address>
  sandbox_ip_cidr: <netns config input>
  host_gateway_ip: <host side address>
  netns_name, netns_path, host_veth, sandbox_veth, nft_table_name

runtime_adapter:
  kind: runsc
  runsc_platform: systrap
  runsc_version: <captured runsc --version output>
  runsc_binary_path: <canonical resolved runsc executable path>
  runsc_binary_digest: sha256:<resolved runsc executable bytes>
  runsc_container_id: harness-gen-<generation_id>
  no_new_privileges: true
  ambient_capabilities: []
  forbidden_capabilities: [CAP_NET_ADMIN, CAP_NET_RAW, CAP_SYS_ADMIN]
  required_annotations:
    /harness-control/bridge:
      dev.gvisor.spec.mount./harness-control/bridge.type: bind
      dev.gvisor.spec.mount./harness-control/bridge.share: exclusive

resource_identity:
  resource_identity_digest: sha256:<canonical allocation payload bytes>

data_volumes:
  workspace: session_workspaces[session_id] -> /workspace
  agent_home: session_driver_homes[session_id, driver] -> /agent-home
  control: generation-owned control dir
  bridge: generation-owned bridge dir
  provisioning_evidence_root: host-only root-owned marker root

model_access:
  model_access_allowed: true | false
  active_turn_required: true
  sandbox_model_proxy_base_url: http://harness-model-proxy.internal:8082 | null

credential_policy:
  provider_credentials: host-only
  sandbox_secret_mount: absent
  proxy_token: absent

input_digests:
  runtime_config_digest: sha256:...
  rootfs_image_digest: sha256:...
  schema_pack_digest: sha256:... | null
  future_content_digests: [...]
```

## Digest Rules

Allowed digest direction:

```text
config/content/runtime inputs
  -> canonical SandboxContract payload
  -> sandbox_contract_digest
  -> rendered network hosts / control manifest / OCI spec / bundle /
     checkpoint metadata digests
```

Rendered artifact digests are sidecar outputs, not contract inputs:

```text
sandbox_contract_artifacts:
  contract_id
  sandbox_contract_digest
  network_hosts_digest | null
  control_manifest_digest
  oci_spec_digest
  bundle_digest
  checkpoint_metadata_digest | null
```

A digest without the canonical payload is not a valid Phase 8 contract. A
payload whose stored digest does not match is contract corruption.

## Sandbox-Visible Manifest Projection

The canonical `SandboxContract` is host-only. It may contain host bind sources,
configured roots, `netns_path`, veth names, nft table names, proxy-internal
paths, and provider credential root identities because launch, cleanup, and
release evidence need those facts.

The control manifest mounted at `/harness-control/*` is not a serialized copy of
that contract. It is an explicit projection rendered from the verified contract
through an allow-list. The projection may contain only:

- contract/version identifiers and digests needed for compatibility checks;
- sandbox-visible paths such as `/workspace`, `/agent-home`,
  `/harness-control`, `/harness-control/bridge`, `/schema-pack`, and future
  Phase 9 content paths;
- sandbox-visible feature flags, driver settings, and content digests;
- `sandbox_model_proxy_base_url`, when model access is enabled. This value is a
  stable sandbox-visible alias URL from the verified contract, for example
  `http://harness-model-proxy.internal:8082`, not a per-generation gateway IP
  URL. It is absent or null when `model_access_allowed = false`;
- bridge protocol fields and relative queue names needed by the entrypoint.

The projection must not contain:

- configured host roots or host bind sources;
- `netns_name`, `netns_path`, `host_veth`, `sandbox_veth`, nft table names, or
  `host_gateway_ip`;
- literal gateway, loopback, host-local, or provider upstream URLs for
  `sandbox_model_proxy_base_url`;
- runtime adapter host paths, including `runsc_binary_path`;
- proxy-internal socket paths;
- provider credential paths or secret-manager locations;
- bundle, spec, checkpoint, log, DB, or rootfs host paths.

Every new manifest field is added to the projection allow-list deliberately and
joins the projected-control-manifest digest. A release gate scans host-rendered
control artifacts under `/harness-control` and fails if any forbidden host root,
network host identity, literal gateway model-proxy URL, proxy-internal path, or
provider path appears. The sandbox-writable bridge outbox is not used as
evidence that the host projection is clean.

The stable model-proxy alias is sandbox API surface, not authorization input.
When `model_access_allowed = true`, RuntimeAdapter/network setup must make that
alias resolve inside the sandbox to the local proxy endpoint for the generation,
but `/harness-control/*` exposes only the alias URL. Proxy authorization still
uses the observed TCP peer source IP and verified contract state, never the
alias, URL, headers, or bridge payloads.

Alias resolution is a typed MountPlan artifact. The default implementation
renders a root-owned generated hosts file at
`<run_dir>/network/gen-<generation_id>/hosts` and binds it read-only to
`/etc/hosts`. The file may contain the local proxy endpoint IP needed for name
resolution, but it is network plumbing, not an authorization fact and not part
of the `/harness-control` projection. Its canonical bytes join the sidecar
`network_hosts_digest`, and release gates verify that it contains only expected
localhost/base image entries, runtime hostname entries, and the configured
stable alias mapping, with no provider credentials, host roots, proxy-internal
paths, or DB/control paths.

The alias value is part of the canonical contract so projection rendering does
not consult current config. Changing it is an allocation fence for affected
generations.

## Resource Identity Payload

`runtime_resource_instances` stores destructive cleanup identities, so bare
columns are not enough when the canonical contract is corrupt. At allocation the
worker writes a canonical `resource_identity_payload` and
`resource_identity_digest` covering:

- `host_id`, `session_id`, `generation_id`, `contract_id`, and
  `sandbox_contract_version`;
- generation-scoped `runsc_container_id`, `runsc` platform, version, canonical
  binary path, and binary digest;
- allocated network identities: netns name/path, veth names, sandbox and host
  IP/CIDR values, and nft table name;
- generation-owned control, bridge, network projection, bundle, spec,
  checkpoint, and log paths;
- the configured root identifiers and typed path prefixes that make each path
  valid for this allocation.

The payload is immutable after allocation. Row mirrors are diagnostics unless
they match the verified resource identity payload and digest exactly.
Before delete, quarantine, or `absent_verified`, `HostStateReconciler` must
validate the payload digest, `host_id`, root ownership, canonical path prefixes,
typed generation IDs, symlink-safe traversal, and "not a root" constraints. If
the payload or prefix proof is missing or corrupt, automated destructive cleanup
and allocator reuse stop for that identity and operator reconciliation is
required.

## Schema Additions

Preferred first-class contract rows:

```text
sandbox_contracts(
  contract_id PRIMARY KEY,
  generation_id UNIQUE NOT NULL,
  session_id NOT NULL,
  sandbox_contract_version NOT NULL,
  canonical_payload NOT NULL,
  sandbox_contract_digest NOT NULL,
  created_at NOT NULL
)
```

Rendered artifact rows are first-class sidecar evidence, keyed by the immutable
contract row or equivalent immutable contract owner:

```text
sandbox_contract_artifacts(
  contract_id PRIMARY KEY REFERENCES sandbox_contracts(contract_id),
  sandbox_contract_digest NOT NULL,
  network_hosts_digest,
  control_manifest_digest NOT NULL,
  oci_spec_digest NOT NULL,
  bundle_digest NOT NULL,
  checkpoint_metadata_digest,
  created_at NOT NULL
)
```

If the canonical payload is stored on an existing generation or resource row,
the artifact row references that immutable contract owner instead of a separate
`sandbox_contracts` row.

Required references and mirrors:

- `runtime_generations.sandbox_contract_id`
- `runtime_generations.sandbox_contract_version`
- `runtime_generations.checkpoint_runsc_platform`
- `runtime_generations.checkpoint_runsc_version`
- `runtime_generations.checkpoint_runsc_binary_path`
- `runtime_generations.checkpoint_runsc_binary_digest`
- `runtime_resource_instances.contract_id`
- `runtime_resource_instances.sandbox_contract_version`
- `runtime_resource_instances.runsc_container_id`
- `runtime_resource_instances.runsc_platform`
- `runtime_resource_instances.runsc_version`
- `runtime_resource_instances.runsc_binary_path`
- `runtime_resource_instances.runsc_binary_digest`
- `runtime_resource_instances.sandbox_ip`
- `runtime_resource_instances.network_hosts_path`
- `runtime_resource_instances.resource_identity_payload`
- `runtime_resource_instances.resource_identity_digest`
- `agent_runtime_profiles.sandbox_uid`
- `agent_runtime_profiles.sandbox_gid`
- `agent_runtime_profiles.sandbox_supplemental_gids`
- `agent_runtime_profiles.model_access_allowed`
- `active_model_request_contexts.agent_runtime_profile_id`
- `active_model_request_contexts.model_access_allowed`
- `session_workspaces.*`
- `session_driver_homes.*`

If the implementation stores the canonical payload on an existing generation or
resource row instead of a separate table, the same invariants apply: immutable
after allocation, durable, and digest-derived from exactly the stored bytes.

## Data Volumes

Workspace is session-scoped:

```text
session_workspaces(
  session_id PRIMARY KEY,
  host_path UNIQUE NOT NULL,
  layout_version NOT NULL,
  sandbox_uid NOT NULL,
  sandbox_gid NOT NULL,
  sandbox_supplemental_gids NOT NULL,
  runtime_identity_digest NOT NULL,
  provisioned_at NOT NULL,
  provisioning_marker_path NOT NULL,
  provisioning_marker_digest NOT NULL
)
```

Driver home is session+driver-scoped:

```text
session_driver_homes(
  session_id NOT NULL,
  driver NOT NULL,
  host_path UNIQUE NOT NULL,
  layout_version NOT NULL,
  sandbox_uid NOT NULL,
  sandbox_gid NOT NULL,
  sandbox_supplemental_gids NOT NULL,
  runtime_identity_digest NOT NULL,
  provisioned_at NOT NULL,
  provisioning_marker_path NOT NULL,
  provisioning_marker_digest NOT NULL,
  PRIMARY KEY(session_id, driver)
)
```

`host_path` is a checked mirror of the path derived from configured roots and
typed IDs. A row that points to another session, another driver, a symlink
escape, a non-canonical path, or mismatched provisioning evidence is rejected
before launch or restore.

Provisioning marker fields are mandatory in Phase 8. The marker path points to
a root-owned file outside the sandbox-writable bind source, and the digest is
computed from canonical marker bytes that bind the volume type, session ID,
driver when applicable, canonical host path, layout version, sandbox identity,
and provisioning time. Protected DB rows are the durable index for the evidence;
they are not a DB-only replacement for the marker.

Marker paths are derived under the configured DataVolume provisioning evidence
root, for example
`<volume_evidence_root>/workspaces/<session_id>.json` and
`<volume_evidence_root>/driver-homes/<session_id>/<driver>.json`. That root is
host-only, root-owned, canonical, and disjoint from every sandbox content bind
source, provider credential root, and proxy-internal root.

`runtime_identity_digest` is computed from normalized
`sandbox_uid`, `sandbox_gid`, and `sandbox_supplemental_gids`.

## Artifact Serving and Scanning

Artifact scanning and downloads are DataVolume operations. Phase 8 resolves the
workspace for a `session_id` by loading the matching `session_workspaces` row and
verifying its canonical host path, layout version, runtime identity digest, and
provisioning marker evidence.

The artifact watcher:

- watches only verified `session_workspaces.host_path` roots;
- computes stored artifact paths as symlink-safe relative paths under that
  verified workspace;
- does not reconstruct `<sessions_root>/<session_id>` or trust `sessions.*`
  workspace mirrors.

The download path:

- loads and verifies the `session_workspaces` row for the requested session
  before opening any file;
- joins the requested relative artifact path under the verified workspace with
  canonical, symlink-safe containment checks;
- rejects missing workspace evidence, public session workspace fields,
  reconstructed root-plus-ID paths, absolute paths, path escapes, and symlinks;
- returns only regular files and never exposes the host workspace root.

## Sandbox Identity

The sandbox identity comes from strict config, not entrypoint defaults:

```yaml
harness:
  sandbox_identity:
    uid: 65534
    gid: 65534
    supplemental_gids: []
```

Validation:

- UID and GID are required after defaults, positive, and non-zero.
- Supplemental GIDs are sorted and duplicate-free before digesting.
- Group `0` is rejected.
- Unknown fields fail config loading.
- Provider-secret reader groups are not included in the default sandbox
  identity.

`agent_runtime_profiles` identity includes:

```text
agent, model, output_format, disable_nonessential_traffic,
sandbox_uid, sandbox_gid, sandbox_supplemental_gids,
model_access_allowed,
future scoped-credential policy fields
```

Old provider-secret profile references are not part of normal Phase 8 Claude
profile identity. `requires_secret_drop` and `/harness-secrets` are legacy
pre-Phase-8 concepts and are not part of normal Phase 8 profile identity.

## Public API Boundary

Phase 8 stops treating store rows as public response DTOs. Public session
responses and session event payloads are rendered through a dedicated DTO that
omits host paths and legacy runtime identities.

Public session DTOs must not include:

- `workspace`;
- `agent_home_path`;
- `restore_id`;
- checkpoint, bundle, control, bridge, log, or rootfs host paths;
- provider credential or proxy-internal paths.

The frontend `ApiSession` type and fixtures must remove those fields. UI code
uses sandbox paths such as `/workspace` only as product-visible labels, not as
host path echoes. Runtime cleanup, restore, and diagnostics use
`runtime_resource_instances.runsc_container_id` and contract/resource APIs
instead of public session fields.

`store.Session` may keep legacy columns temporarily as internal migration
state, but it must not be directly JSON-serialized by handlers, events, or test
fixtures after the Phase 8 API DTO lands.

## Lifecycle Rules

Allocation:

- Validate config roots and sandbox identity.
- Resolve the `runsc` executable that the worker will execute, record its
  canonical path, platform, and version, and compute a binary digest from the
  exact resolved executable bytes.
- Create `SessionWorkspace` and `SessionDriverHome` rows on first provisioning,
  or select existing valid rows for later generations of the same session.
- Select an `AgentRuntimeProfile` including UID/GID/groups and model
  entitlement.
- Allocate network identity and generation-owned host resource identities.
- Compile and persist the canonical contract, then compute its digest.
- Create `runtime_resource_instances` in `allocated` state.

Launch and restore:

- Load the persisted contract and verify its digest.
- Verify the current `runsc` platform, version, binary path, and binary digest
  match the contract before fresh start or same-generation restore.
- Verify checkpoint metadata includes matching `runsc` platform, version,
  binary path, and binary digest before restore.
- Verify DataVolume rows and host-trusted provisioning evidence.
- Render network hosts, control manifest, and OCI spec from the contract through
  `MountPlan` and `RuntimeAdapter`.
- Validate rendered artifacts structurally before `runsc` starts.
- Use `runtime_resource_instances.runsc_container_id` for fresh run and
  same-generation restore.
- Transition `ready -> live` only after `runsc` start/restore proof and bridge
  startup readiness proof are recorded for the same generation and contract.

Reuse, polling, checkpointing, normal cleanup, and proxy authorization:

- Require `sandbox_contract_version = sandbox-isolation-v1`.
- Require the operation-specific `runtime_resource_instances.state` from
  [Migration and sunset](./migration-and-sunset.md).
- Reject missing payloads, malformed payloads, digest-only rows, digest
  mismatches, or mirror rows that disagree with the verified payload.

Corruption-mode cleanup is the only exception. If the canonical payload is
missing or corrupt, launch, restore, checkpoint, bridge polling, and proxy
authorization stay blocked, but `HostStateReconciler` may use
`runtime_resource_instances` identities to delete or quarantine host resources
only after verifying the immutable `resource_identity_payload` and
`resource_identity_digest`. It must also validate every destructive path and
network identity against the expected host roots, typed prefixes, `host_id`, and
symlink-safe canonical form before delete/quarantine and again before
`absent_verified`. It records contract-corruption evidence and cannot release
allocator identity until host absence is proven. If the resource identity
payload or root/prefix proof is corrupt, automated cleanup stops and the row
cannot reach `absent_verified`.

## Security Change Policy

Contracts are immutable. These changes are allocation fences:

- sandbox UID/GID/groups;
- configured root paths;
- runtime adapter requirements;
- the `runsc` platform, version, binary path, or binary digest;
- rootfs or content digests;
- credential policy;
- `sandbox_model_proxy_base_url`;
- `model_access_allowed`.

Changing one of those values does not mutate existing contracts. The Phase 8 lab
policy is drain/retire/reallocate affected generations, clear active model
contexts, and allocate fresh contracts. Emergency global provider disablement is
a host/proxy stop switch plus generation retirement, not a contract mutation.

Phase 8 does not implement dynamic per-process or per-tool model authorization.
