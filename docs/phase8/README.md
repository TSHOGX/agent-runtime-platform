# Phase 8: Runtime Isolation Hardening

> Status: release-qualified baseline. Final target-lab evidence passed for
> `345f684b6a6b146311efcb3b3d7a5d7ebb607822`; current HEAD changes must rerun
> final evidence before a new runtime-isolation release.
> Source audit: `.scratch/runtime-security-followups.md`.

Phase 8 is a contract-first refactor of the runtime boundary. The roadmap phase
is "Phase 8"; the code and persisted data contract is `sandbox-isolation-v1`.

The qualified baseline removes the previous sandbox overexposure without
changing the Phase 7 session, turn, bridge, or event semantics. Each generation
gets one immutable runtime contract. Launch, restore, cleanup, checkpointing,
bridge polling, proxy authorization, migration, and release evidence consume
that same contract.

Final evidence for the qualified baseline:

- `/tmp/harness-runtime-isolation-final-gates-with-compat.json`
- `release_complete: true`
- supplied evidence: cutover, reconciliation, rootfs image, proxy contract, and
  adversarial lab
- proxy commit: `c74d5e0485b8457de68c2e5ac2b32877fbbb3932`
- rootfs digest:
  `sha256:192e6982a36016113633e258947c5a7302a820649cbf91195a34101e590886a5`
- `runsc` binary digest:
  `sha256:82b042a8f27f9dd65c58ef6adf87a905ec6c377ec0259ccaf34dd9028b55dc5a`

## Design Decision

Use the current gVisor/runsc worker model, but replace implicit runtime facts
with a persisted sandbox contract:

```text
Config + runsc executable + Session + Driver + Generation
  -> DataVolumeProvisioner + RuntimeResourceInstance allocation
  -> SandboxContractCompiler
  -> immutable SandboxContract + digest
  -> MountPlan
  -> RuntimeAdapter(runsc OCI spec)
  -> RuntimeResourceInstance state
  -> HostStateReconciler
  -> ModelAccessAuthorizer
```

This is a destructive clean cutover for the lab. Old sessions, workspaces,
agent homes, runtime rows, checkpoints, prepared bundles, and legacy manifests
are wiped or quarantined from active roots instead of upgraded.

## Goals

- A sandbox sees only the current session workspace, the selected driver home,
  its generation control/bridge surfaces, the generated network alias projection
  when model access is enabled, and explicit tmp/cache mounts.
- A sandbox cannot enumerate or mutate another session's workspace or another
  driver's agent state.
- Upstream model provider credentials remain host/proxy-only.
- Shell, Claude, the bridge claim loop, and turn runners execute as the
  configured non-root sandbox identity.
- The OCI rootfs is read-only; writable surfaces are explicit.
- Host runtime identity reuse is blocked until the reconciler records host-side
  absence evidence.

R5, "gVisor is weaker than microVM isolation," remains out of scope. The current
host has no KVM path, so Phase 8 hardens the gVisor contract rather than moving
to Firecracker or Kata.

## Sandbox Layout

```text
/workspace                 rw exact bind: current session workspace
/agent-home                rw exact bind: current session + driver home
/harness-control           ro exact bind: current generation control dir
/harness-control/bridge    rw exact bind: current generation bridge queue
/etc/hosts                 ro exact bind: proxy alias projection, if enabled
/schema-pack               ro exact bind, if configured
/harness-skills            ro exact bind, when Phase 9c lands
/tmp, /var/tmp, caches     tmpfs or explicit scratch mounts
/harness-secrets           absent; legacy secret root config removed
```

The sandbox must not have `/sessions` or `/agent-homes` parent mounts. Control
manifests expose a sandbox-visible projection, not the full contract. The
projection is allow-listed and contains only sandbox-facing fields such as
sandbox paths, bridge protocol settings, feature flags, content digests, and the
stable `sandbox_model_proxy_base_url` alias when model access is enabled. It
must not include host roots, host bind sources, `netns_path`, veth names, host
gateway IP URLs, proxy-internal paths, provider credential paths, or other
host-only allocation facts.

## Component Ownership

| Component | Owns |
| --- | --- |
| `SandboxContractCompiler` | Contract payload, version, digest inputs, derived identities, and profile selection. |
| `MountPlan` | Every sandbox content bind and scratch/cache mount. No runtime path hand-builds product mounts. |
| `DataVolumeProvisioner` | Creation or selection of `SessionWorkspace` and `SessionDriverHome` rows, directories, permissions, and host-trusted evidence. |
| `RuntimeAdapter` | runsc platform, version, and binary pinning; OCI pseudo mounts; runsc-specific rendering; required annotations; rendered spec artifact digest. |
| `RuntimeResourceInstance` | Generation-owned host resource identity and lifecycle state. |
| `HostStateReconciler` | Host inventory, deletion, quarantine, and absence proof before identity reuse. |
| `ModelAccessAuthorizer` | Host-side model authorization from trusted proxy identity, live turn context, entitlement, and contract match. |
| `PublicSessionDTO` | Public API/event shape that excludes host paths and legacy restore IDs. |

Any implementation path that bypasses these components is pre-Phase-8
compatibility code and cannot be used as release evidence.

## Phase 7 Boundary

Phase 8 does not replace:

- session input eligibility or terminal/non-terminal semantics;
- `runtime_generations.status` execution lifecycle;
- turn `queued -> leased -> running -> completed/failed/canceled` CAS fencing;
- `ack_started_at` unknown-after-started behavior;
- bridge claim/ack and heartbeat lease semantics;
- durable event persistence before in-memory publish;
- retryable runtime/start/restore/turn failures.

The new `runtime_resource_instances.state` proves only whether generation-owned
host resources are allocated, live, retained for checkpoint, reconciling, or
absent. It is not a substitute for session, turn, generation execution, or
bridge ownership state.

Phase 7 release gates remain blocking unless Phase 8 explicitly replaces them.
Phase 8 keeps deterministic turn-start latency coverage in the
runtime-isolation release runner and includes the gVisor bridge durability lab
in the final compatibility evidence. The external Phase 7 live turn-start
latency gate remains a release-only gate for runtime/proxy/config changes when
prewarmed sessions are available. Phase 8 replaces the legacy sandbox secret
permission lab and authenticated malformed `/v1/messages` pre-turn proxy probe
because their contracts are removed. The replacements are the host-only
credential/no-`/harness-secrets` gates, stable proxy alias plus proxy-side
`Host` validation, active-context model authorization, and the re-pinned proxy
contract in [Release gates](./release-gates.md).

## Landed Implementation Order

1. Add the canonical `SandboxContract` payload/table, version fields, digest
   verification, `runsc` platform, version, and binary digest inputs, and
   sidecar artifact digests.
2. Add strict config validation, including sandbox identity and canonical root
   disjointness checks; remove the legacy `harness.secrets.*` config contract.
3. Add `SessionWorkspace` and `SessionDriverHome` DataVolume rows and
   host-side provisioning/selection evidence.
4. Add `RuntimeResourceInstance` and `HostStateReconciler`; stop authorizing
   reuse from split Phase 7 resource states.
5. Build `MountPlan` for sandbox content binds and keep OCI pseudo mounts in a
   separate RuntimeAdapter allow-list.
6. Move shell through the new contract first: exact mounts, non-root execution,
   read-only rootfs, no secrets.
7. Move Claude through the host-side model credential boundary and entitlement
   authorizer.
8. Add public API/event DTOs and frontend type cleanup so host paths and legacy
   restore IDs are internal only.
9. Fence restore, checkpoint, bridge polling, cleanup, proxy auth, and prepared
   artifact reuse on `sandbox-isolation-v1` plus the expected resource state.
10. Move `ready -> live` after runsc start/restore proof and bridge startup
    readiness proof.
11. Run destructive cutover and adversarial release gates on the target lab
    host.

Release gates reject `sandbox-isolation-v1` enablement while:

- the DB or any control-plane state is under a sandbox-bindable root;
- configured roots overlap or nest unexpectedly;
- parent `/sessions` or `/agent-homes` mounts remain;
- provider credentials can be mounted, rendered, exported, logged, or probed
  from sandbox-visible surfaces;
- legacy `harness.secrets.root` can still feed a sandbox content mount;
- `sandbox_model_proxy_base_url` is missing for Claude/model access, renders a
  gateway IP URL in `/harness-control`, or cannot be constrained to the local
  proxy endpoint plus stable-alias `Host` validation;
- model authorization can use sandbox-sent source-IP claims or a leased-only
  turn without a committed active model context;
- the sandbox-visible `/harness-control/*` projection has not been proven free
  of host roots, runtime host identities, proxy-internal paths, and provider
  paths;
- artifact watcher or download paths still resolve files from public session
  fields or reconstructed root-plus-session-ID paths instead of verified
  `session_workspaces` evidence;
- runtime cleanup still uses `sessions.restore_id` or session-scoped runsc IDs;
- runsc mismatch cleanup can reuse identities without a recorded compatible
  binary or independent quarantine/absence evidence;
- public API responses or event payloads expose `workspace`,
  `agent_home_path`, `restore_id`, or host runtime paths;
- old host runtime resources can survive while allocator identity is reusable.

## Change Policy

Security-relevant config changes are allocation fences, not silent mutations of
existing contracts. Changes to sandbox UID/GID/groups, root paths,
`model_access_allowed`, `sandbox_model_proxy_base_url`, runtime adapter
requirements, `runsc` platform, version, binary path, or binary digest, or
host-only credential policy require a drain/retire/reallocate cutover for
affected generations. Phase 8 does not implement dynamic per-process model
revocation.

## Document Map

- [Sandbox contract](./sandbox-contract.md): immutable contract payload, schema,
  digest rules, and security-change policy.
- [Mounts and runtime](./mounts-and-runtime.md): exact binds, root layout,
  non-root execution, read-only rootfs, and bridge ACLs.
- [Model proxy boundary](./model-proxy-boundary.md): host-only provider
  credentials, trusted source-IP authorization, entitlement, and proxy
  correlation.
- [Migration and sunset](./migration-and-sunset.md): destructive cutover,
  resource-state machine, store fences, and rollback policy.
- [Release gates](./release-gates.md): adversarial acceptance checklist.

## Phase 9 Boundary

Phase 9 may add read-only sandbox-visible content such as system skills or
managed Claude settings. Those mounts must enter through the Phase 8
`MountPlan`, use exact read-only binds, include content digests, pass nested
mount checks, and never render bearer tokens into Claude-visible files.
