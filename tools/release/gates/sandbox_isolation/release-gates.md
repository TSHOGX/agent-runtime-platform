# Runtime Isolation Release Gates

These gates define the acceptance criteria for any `sandbox-isolation-v1`
runtime-isolation release candidate. Current HEAD changes must rerun the final
evidence bundle before a new runtime-isolation release. Unit tests are
necessary but not sufficient because the risky behavior involves actual runsc
mounts, network namespaces, proxy peer addresses, filesystem metadata, and
crash recovery.

## Contract Gates

- Generated manifests/specs and persisted rows declare
  `sandbox_contract_version = sandbox-isolation-v1`.
- Each generation references an immutable canonical `SandboxContract` payload.
- `sandbox_contract_digest` is computed from the stored payload bytes.
- Removing, corrupting, or reformatting the payload while leaving the digest
  blocks launch, restore, checkpoint validation, bridge polling, and proxy
  authorization.
- Contract corruption switches cleanup to `HostStateReconciler`
  corruption-mode: it may delete or quarantine host resources only from the
  verified `resource_identity_payload`, records corruption evidence, and cannot
  release identity reuse without host absence proof.
- Changing current config after allocation does not silently re-render old
  generations from new config.
- Contract payload includes runtime identity, MountPlan, network identity,
  `resource_identity_digest`, runtime adapter requirements, capability policy,
  generation-scoped `runsc_container_id`, `runsc_platform`, `runsc_version`,
  `runsc_binary_path`, `runsc_binary_digest`, credential policy, DataVolume
  ownership, model entitlement, `sandbox_model_proxy_base_url`, and input
  content digests.
- Changing the worker's `runsc` platform, resolved binary, or reported version
  after allocation does not mutate existing contracts. Fresh launch, restore,
  and checkpoint validation for the stored contract reject the mismatch; live
  generations pinned to the old value are drained, retired, and reallocated
  before the new runsc pin is considered active. Cleanup remains allowed, uses
  the contract `runsc_container_id`, and records mismatch evidence before
  reconciliation. If current `runsc` cannot delete an old container or
  checkpoint, cleanup uses the recorded pinned `runsc` binary only when its
  recorded path is still canonical and its digest matches the contract;
  otherwise the resource is quarantined or marked for manual reconciliation,
  and identity reuse stays blocked until independent evidence reaches
  `absent_verified`.
- Rendered network-hosts/manifest/spec/bundle/checkpoint digests are sidecar
  artifact digests and do not feed back into `sandbox_contract_digest`.
- `/harness-control/*` is rendered from an explicit sandbox-visible projection,
  not by serializing the full contract.
- Scanning host-rendered control artifacts under `/harness-control` finds no
  host roots, host bind sources, `netns_path`, veth names, `host_gateway_ip`,
  nft table names, `runsc_binary_path`, proxy-internal paths, provider
  credential paths, bundle/spec/checkpoint/log paths, DB paths, or rootfs host
  paths.
- When model access is enabled, `/harness-control/*` may contain only
  `sandbox_model_proxy_base_url` as the sandbox-visible model endpoint. Its
  value is a stable alias URL, not `http://<host_gateway_ip>:<port>`, loopback,
  host-local, proxy-internal, or provider upstream URL.
- Sandbox-writable bridge outbox contents are excluded from host projection
  cleanliness evidence.
- Runtime profile identity includes UID, GID, supplemental groups, and
  `model_access_allowed`.
- Public API responses and session event payloads are rendered through
  `sandbox-isolation-v1` DTOs and do not expose `workspace`, `agent_home_path`, `restore_id`,
  checkpoint paths, bridge/control paths, credential paths, or proxy-internal
  paths.
- Frontend `ApiSession` types, fixtures, and reducers do not require
  `workspace`, `agent_home_path`, or `restore_id`.
- No release gate or runtime helper uses a historical stage name as a code-level
  runtime contract.

## Root and Mount Gates

- Configured top-level roots are canonical, symlink-safe, and mutually disjoint:
  sessions, agent homes, run, checkpoint, prepared bundle, rootfs/content,
  schema pack, agent capability content, DB/control-plane, and any file-backed provider
  credential roots.
- The proxy-internal socket root is canonical, symlink-safe, host-only, and not
  equal to, contained by, or containing any sandbox content bind source.
- The DataVolume provisioning evidence root is canonical, symlink-safe,
  host-only, root-owned, and either a disjoint top-level root or a reserved
  host-only subroot under the DB/control-plane state root. It is not equal to,
  contained by, or containing any sandbox content bind source, provider
  credential root, or proxy-internal root.
- Removed `harness.secrets.root` and `harness.secrets.readers_gid` are rejected
  by `sandbox-isolation-v1` config; old secret roots are absent from active roots or
  quarantined outside all `sandbox-isolation-v1` roots before enablement.
- The DB lives outside every sandbox-bindable root.
- Provider credential roots and proxy-internal roots live outside every
  sandbox-bindable root.
- `<sessions_root>` contains only per-session workspace directories after
  cutover.
- No generated spec contains parent `/sessions` or `/agent-homes` mounts.
- Every sandbox content bind or tmp/cache destination is in the MountPlan
  allow-list.
- The generated network alias projection, when present, is an exact read-only
  MountPlan bind to `/etc/hosts`; its digest matches the recorded sidecar
  artifact digest for the contract and its contents contain only expected
  localhost/base-image entries, runtime hostname entries, and the configured
  stable proxy alias mapping.
- Every OCI pseudo mount destination is in the RuntimeAdapter pseudo-mount
  allow-list with the exact type, source, mode, and options from the pinned
  adapter fixture.
- RuntimeAdapter does not create host binds for product data outside MountPlan.
- No generated spec exposes `/dev/net/tun` or a tun/tap-equivalent host device.
- Session A cannot list, read, write, delete, or mount Session B's workspace.
- Session A cannot list, read, write, delete, or mount Session B's driver home.
- Corrupted workspace/home rows pointing at another owner, root, symlink escape,
  or non-canonical path are rejected before launch.
- `rm -rf /workspace` damages only the current session workspace.
- `rm -rf /agent-home` damages only the current session+driver home.
- Destroying a generation does not delete persistent workspace or driver-home
  data.
- Writable binds carry `nosuid,nodev`; bridge/control and read-only content
  binds carry `noexec`. Workspace and driver-home binds carry `noexec` only when
  product-compatible.
- Nested mountpoints under exact bind sources are rejected or hidden by private
  propagation.
- A host submount created after launch under a recursive bind source is not
  visible in the sandbox.
- The bridge placeholder exists, is empty, is not a symlink, is not a
  mountpoint, and is mounted over by the real bridge source.
- Bridge bind includes the pinned gVisor exclusive/share annotation.
- Rootfs is read-only outside declared writable mounts.
- `/workspace` and `/agent-home` remain writable.
- Rootfs image inspection proves `/workspace` and `/agent-home` are real empty
  directories, `/etc/hosts` is a real non-symlink file suitable for exact bind
  overlay, and baked `/sessions`, `/agent-homes`, `/root/.claude`, and
  `/root/.cache` are absent or harmless.

## Runtime and Resource Gates

- `bridge-protocol-v2` session, generation, turn, bridge, and event semantics remain intact.
- `runtime_resource_instances.state` is never used as a substitute for session
  state, turn state, generation execution status, or bridge claim ownership.
- Resource state follows only the allowed graph in the runtime resource state
  contract.
- Direct `ready -> materializing`, direct transition to `destroyed` from any
  state other than `absent_verified`, direct `checkpoint_reserved ->
  absent_verified`, direct `checkpoint_reserved -> destroyed`,
  cross-generation restore, and direct identity reuse are rejected.
- Partial unique indexes on generation-owned identities exclude only
  `absent_verified` and `destroyed`.
- Reconciliation evidence records `runsc state`, `ip netns`, `ip link`, `nft`,
  and filesystem `lstat` checks before `absent_verified`.
- Cleanup verifies immutable `resource_identity_payload` /
  `resource_identity_digest` and root/prefix validation before delete,
  quarantine, and `absent_verified`.
- `resource_identity_payload` covers the generated network alias projection
  path whenever `network_hosts_path` is present, and cleanup/reconciliation
  validates that path before deleting or quarantining it.
- Corrupt or missing resource identity proof blocks automated destructive
  cleanup and allocator reuse.
- A stale host object with the same generation identity blocks fresh launch,
  restore, and allocator reuse until reconciled.
- Evidence includes `host_id` and is rejected from the wrong worker/host.
- Runtime start, restore, cleanup, forced destruction, and release evidence use
  `runtime_resource_instances.runsc_container_id`, not `sessions.restore_id`,
  session ID, or historical stage-prefixed session IDs.
- `ready -> live` occurs only after the worker records post-start proof for the
  same generation and contract: `runsc state` confirms the expected
  `runsc_container_id`, network namespace/veth/nft evidence exists, the
  resolved `runsc` platform, version, binary path, and binary digest still
  match the contract, and the bridge startup readiness probe passes.
- The bridge startup readiness probe is allowed only in `ready`, consumes only
  bootstrap `hello`, bootstrap heartbeat, and `probe_network` envelopes for the
  expected generation, records startup evidence, and cannot lease/resume turns,
  create active model contexts, accept proxy correlation, checkpoint, renew
  normal live heartbeats, or publish user output before `ready -> live`.
- Fresh start or restore failure before post-start proof moves the resource to
  cleanup/reconciliation and never leaves a `live` row behind.
- Fresh materialization is claimed by CAS from `allocated` to `materializing`
  with `worker_id`, `host_id`, lease/deadline, and idempotency token before host
  resources are created.
- `materializing -> ready` is accepted only from the owning live worker after
  all generation-owned artifacts, DataVolume evidence, and structural
  pre-start validation match the same contract.
- Crash recovery or lease steal during `materializing` inventories partial
  artifacts and either proves exact idempotent output for the same contract or
  moves the row to cleanup/reconciliation; unexpected partial artifacts never
  advance to `ready`.
- Shell, Claude, bridge claim loop, and turn runner run as the configured
  non-root sandbox identity.
- OCI capability sets contain no `CAP_NET_ADMIN`, `CAP_NET_RAW`, or
  `CAP_SYS_ADMIN`; ambient capabilities are empty and `noNewPrivileges` is set.
- Entrypoint tests fail on hardcoded UID/GID defaults.
- Bridge queue permissions are directional and not world-writable.
- Sandbox cannot create, replace, rename, or unlink host-owned bridge inbox,
  host-temp, or host-heartbeat files.
- Artifact watcher and download paths resolve workspace files only through a
  verified `session_workspaces` row and its provisioning evidence. Gates fail if
  artifact code uses public session fields, `sessions.workspace`,
  `agent_home_path`, or reconstructed `<sessions_root>/<session_id>` paths.
- Artifact downloads reject missing workspace evidence, symlink escapes,
  absolute paths, path traversal, non-regular files, and requests that would
  expose host workspace roots.

## Model Proxy Gates

- Provider credentials are absent from sandbox env, files, `/proc`, manifests,
  bridge queues, workspace, agent home, schema packs, skills/settings, DB rows,
  events, stdout/stderr, runtime logs, proxy logs, command lines, checkpoint
  metadata, and release evidence.
- Provider credential roots and `<run_dir>/proxy-internal` are host-only and
  fail canonical root validation if they overlap any sandbox-bindable root.
- Pre-turn sandbox startup can call health/bridge probes but model endpoints
  reject without an active context.
- Sandbox startup and Claude use `sandbox_model_proxy_base_url` with the stable
  in-sandbox proxy alias for `/healthz` and model endpoint discovery. Contract
  tests prove RuntimeAdapter/network setup resolves the alias to the local proxy
  and does not weaken exact peer IP authorization.
- Sandbox egress gates expose the stable alias as the supported model API
  surface. nft/egress policy limits traffic to the local proxy endpoint, and
  proxy-side `Host` validation rejects default gateway-literal `Host` values,
  wrong `Host`, and non-alias names that reach the same listener; provider
  upstreams, loopback, other host-local addresses, and proxy-internal sockets
  remain denied. A gateway-literal connection with a manually spoofed alias
  `Host` is not treated as proof of alias enforcement and is authorized only by
  exact source IP, active context, entitlement, and contract checks.
- The stable alias and HTTP `Host` value are not authorization inputs; changing
  them cannot authorize a request without exact source IP, active context,
  entitlement, and contract matches.
- Proxy authorization uses the actual TCP peer source IP, not sandbox-sent
  headers, env, bridge payloads, or `ack_turn_started.sandbox_source_ip`.
- Active model contexts are written from the verified contract/resource sandbox
  IP, not from sandbox ack payloads.
- Active model contexts are created only after `ack_turn_started` commits a turn
  to `running`; leased-only turns, running turns without a committed active
  context, stale active contexts, and active contexts for a different
  turn/profile/generation deny model requests.
- Authorization requires exact sandbox IP equality, not CIDR membership.
- Gateway, loopback, host-local, mismatched, and non-sandbox network sources do
  not authorize model requests.
- Sandbox attempts to add routes/addresses, create raw sockets or tun/tap
  devices, bind the gateway IP, bind a non-sandbox source IP, or spoof source
  headers fail and do not authorize model requests.
- Durable profile entitlement, active-context entitlement, and contract
  entitlement must all be true.
- Profile false/context true, profile true/context false, null entitlement, and
  generation/profile mismatches deny.
- Injected multiple in-flight turns for one generation/source IP deny.
- Proxy correlation APIs are reachable only over the authenticated UDS and reject
  sandbox or unauthenticated host-local callers.
- The re-pinned `claude-code-proxy` contract tests replace the removed
  authenticated malformed `/v1/messages` pre-turn probe: `/healthz` passes
  through the stable alias; pre-turn model endpoints reject without active
  context; malformed `/v1/messages` is tested only after a running turn has a
  committed active context; wrong `Host`, gateway literal with default literal
  `Host`, non-alias names, spoofed headers, leased-only turn, stale context,
  wrong entitlement, and wrong source IP deny before upstream dispatch. A
  gateway-literal connection with manually spoofed alias `Host` must be tested
  as indistinguishable from the alias path and still requires the normal
  authorization checks. Evidence records the proxy commit.
- The selected Claude CLI auth-key mode is gate-checked end to end:
  bridge-client/rootfs behavior, proxy configuration, and pinned proxy contract
  tests agree on `no key` or the fixed non-secret dummy key. `/healthz` alone
  cannot qualify proxy readiness.
- Claude CLI starts with the selected no-key or non-secret dummy-key mode, and
  the proxy ignores that dummy value for authorization.
- Changing `model_access_allowed` is gated as drain/retire/reallocate, not a
  silent mutation of live contracts.
- Changing `sandbox_model_proxy_base_url` is gated as drain/retire/reallocate,
  not a silent projection rewrite for live contracts.

## Cutover Gates

- Cutover inventories old sessions, workspaces, agent homes, runtime rows,
  active proxy contexts, checkpoints, prepared bundles, netns/veth/nft, runsc
  containers, generation directories, removed secret roots, provider credential
  roots, and proxy-internal sockets before enablement.
- Old host runtime resources block `sandbox-isolation-v1` enablement until reconciled and
  absence evidence is recorded.
- Existing session/workspace/agent-home data is wiped from active roots before
  the first `sandbox-isolation-v1` allocation.
- Removed `harness.secrets.root` data is deleted or quarantined outside all
  `sandbox-isolation-v1` roots and cannot be mounted as `/harness-secrets`.
- The old secret permission lab is retired. `sandbox-isolation-v1` gates prove
  `harness.secrets.*` config is rejected, `/harness-secrets` is absent, provider
  credentials are host/proxy-only, and no credential material appears in
  sandbox-visible files, env, `/proc`, logs, DB rows, checkpoints, or release
  evidence.
- Fresh workspace and per-driver home provisioning records host-trusted evidence
  in protected DB rows and mandatory root-owned markers outside
  sandbox-writable bind sources, under the DataVolume provisioning evidence
  root.
- Artifact metadata, watcher scans, and download handlers use
  verified `session_workspaces` evidence; any removed artifact path derived from
  public session rows or `<sessions_root>/<session_id>` is rejected or
  re-indexed through the verified workspace row before enablement.
- Later generations for the same session select valid existing workspace/home
  rows without wiping or rejecting user-created contents.
- Provisioning does not carry stale hardlinks, xattrs, capabilities, sockets,
  FIFOs, device nodes, setuid/setgid bits, unsupported ACLs, or root-owned
  stale content into bind sources.
- Shell-first startup creates/uses only
  `<agent_homes_root>/<session_id>/sh` and cannot read a Claude driver home.
- A second cutover/provisioning dry run is a no-op over clean active roots.
- `session_workspaces` is unique by `session_id` and `host_path`.
- `session_driver_homes` is unique by `(session_id, driver)` and `host_path`.
- Sandbox identity changes are gated as drain/wipe/reprovision or a separately
  designed trusted host-side reprovision flow.
- Optional backup evidence is outside active roots and is not treated as `sandbox-isolation-v1`
  runtime data.
- Removed bake/restore smoke tooling is rewritten for `sandbox-isolation-v1` or
  quarantined from release evidence.

## Documentation Gates

- `docs/PLAN.md`, `docs/architecture.md`, and `docs/next-stage.md` do not
  imply stronger isolation than the implementation provides.
- Next-stage skills and managed-settings docs use the MountPlan exact-bind
  contract and bind content-addressed/generated snapshots rather than mutable
  repo authoring paths.
- Managed settings do not render upstream bearer tokens or remote MCP bearer
  tokens into Claude-visible files unless a separate broker/token design exists.

## Acceptance

A runtime-isolation release candidate is complete only when:

- `sandbox-isolation-v1` is enabled atomically after contract schema, root
  validation, DataVolume provisioning, MountPlan, RuntimeAdapter,
  RuntimeResourceInstance, HostStateReconciler, non-root execution, read-only
  rootfs, public API DTO cleanup, post-start live proof, host-side model
  credentials, authenticated proxy correlation, driver entitlement, proxy
  re-pin, destructive cutover, and sunset fences are active;
- runtime launch/restore/checkpoint/poll/normal-cleanup/proxy paths consume the
  shared contract components instead of reconstructing paths, identity, state,
  annotations, or authorization locally; corruption-mode cleanup uses only
  verified resource identity payloads, root/prefix validation, and absence
  evidence;
- every gate in this file passes on the target lab host;
- evidence records image digest, proxy commit, cutover output, reconciliation
  output, resolved `runsc` platform, version, binary path, binary digest, and
  gate results.
