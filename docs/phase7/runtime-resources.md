# Runtime Resources

Per-generation host artifacts: control manifest, secret materialization, allocator, reaper, recovery sweep. The DB rows that back these artifacts (`runtime_generations`, `network_profiles`, `runtime_generation_resources`) are defined in [schema.md](./schema.md). This file owns *what lives on disk and how it is created, validated, and destroyed*.

Current reading note: this is the Phase 7 resource design. The active Phase 8
baseline rejects legacy `harness.secrets.*` config, has no `/harness-secrets`
mount, and keeps provider credentials host/proxy-side. Treat the secret
materialization sections below as historical Phase 7 context unless a later
phase explicitly introduces a new scoped-secret design.

Step 2 only lands the row/state/reaper skeleton; the concrete bundle/control and network objects arrive in Steps 3 and 4.

## Control Manifest

Each generation gets an isolated control directory and manifest. The manifest must include enough identity to make stale or cross-wired starts fail closed:

```text
session_id
generation_id
created_at
attempt_id
network_profile_id
agent_runtime_profile_id
agent
claude_session_uuid
resume_claude
runsc_platform
runsc_version
anthropic_base_url
anthropic_api_key_secret_id
anthropic_auth_token_secret_id
secret_version
secret_mount_path
model
workspace_path
agent_home_path
host_hostname
netns_name
host_gateway_ip
bridge_dir_path
bundle_digest
runtime_config_digest
spec_digest
egress_policy_digest
manifest_version
manifest_digest
```

`manifest_digest` is computed over the canonical manifest payload excluding the digest field itself. The on-disk `session.json` is a single top-level object `{ "payload": <manifest content>, "digest": "<hex>" }`; splitting `payload` and `digest` at the top level removes ordering ambiguity around the digest field, since verifiers feed `payload` bytes into the canonicalizer and never the wrapper. The Phase 7 canonicalization rule is deliberately narrower than full RFC 8785 / JCS because the manifest schema contains only object fields with strings, booleans, and integers: `parse → serialize object keys lexicographically with no insignificant whitespace and UTF-8 preserved → sha256 → constant-time compare`. Both the Go host code and sandbox Python verifier implement this same rule, and a shared fixture cross-checks the exact canonical bytes and digest. The same digest rule is reused for `control_manifest_digest` in checkpoint metadata.

This is the canonical control-manifest field set for Phase 7. `checkpoint-restore.md` only defines which of these names are regenerable or strict at restore time; it does not introduce any extra manifest fields of its own.

The sandbox rootfs is therefore required to ship: `python3` for manifest verification and an HTTP client usable from sh + Python (`curl` is sufficient) for the in-sandbox `probe_network()`. These are hard dependencies of `harness-agent-entrypoint`, not optional extras; sandbox-image build must fail closed if either is missing.

The control plane writes the manifest atomically:

```text
write session.json.tmp
fsync file
rename session.json.tmp -> session.json
fsync parent directory
```

The three per-generation roots are derived from `harness.run_dir`, not configured independently: `control_root = <run_dir>/control`, `bundle_root = <run_dir>/runtime`, and `bridge_root = <run_dir>/bridge`. All path-bearing Phase 7 docs should use those derived roots instead of hard-coded `/var/lib/harness/...` prefixes.

The entrypoint must validate `session_id`, `generation_id`, `network_profile_id`, `agent_runtime_profile_id`, `anthropic_api_key_secret_id`, `anthropic_auth_token_secret_id`, `manifest_version`, `secret_version`, and `manifest_digest` before starting the agent. Resolved credentials are read from `${SECRET_DIR}/<secret_id>/<secret_version>` per the secret-mount contract below. A mismatch on any of these fields exits non-zero with a code distinguishable from agent crashes; the host marks the generation `failed` with `error_class = manifest_digest_mismatch` (or the matching `*_mismatch` class for the offending field).

## Secret Materialization

Secret values are referenced only by `secret_id` + `secret_version`. In Phase 7a the "secret store" is a host-local directory: `<host_secrets_root>/<secret_id>/<secret_version>` containing the plaintext value as a single file. The on-disk permission model must let the in-sandbox agent (UID `65534` per `harness-agent-entrypoint`) read the file while keeping it unreadable to anything else on the host, including other local users:

```text
<host_secrets_root>                 mode 0750, owner orchestrator,
                                     group harness-secret-readers
<host_secrets_root>/<secret_id>     mode 0750, same owner/group
<host_secrets_root>/<secret_id>/
  <secret_version>                  mode 0440, owner orchestrator,
                                     group harness-secret-readers
                                     (immutable after publish — see below)
```

`harness-secret-readers` is a dedicated host group (GID baked into the sandbox image at build time as `HARNESS_SECRET_READERS_GID`) whose only member is UID `65534` (the same UID the sandbox maps the agent to, since gVisor with the default `--file-access=exclusive` does not user-namespace-remap and the in-sandbox UID is the host UID). The orchestrator chowns secret files to `orchestrator:harness-secret-readers` at write time and runs `chmod 0440`; the agent reads as `65534` via the group bit. Mode `0400` owned by the orchestrator is **not** acceptable — the sandbox would silently fail to read it; the `0440 group=harness-secret-readers` contract is what makes the cross-UID read succeed.

**Secret version immutability (hard rule).** A `<secret_id>/<secret_version>` file is **immutable after publish**. Once written, the orchestrator never reopens it for write — neither for rotation, nor to "re-encrypt," nor as a fixup path. Rotation publishes a *new* `<secret_version>` row and a *new* file at `<host_secrets_root>/<secret_id>/<new_version>`; consumers that should pick up the rotation get a new generation that references the new `secret_version`. Phase 7 keeps old local secret-version files in place because checkpoint restore and forensic replay depend on byte stability; retention/GC for a real rotating secret backend is Phase 11 secret-store work. This is what lets `secret_version` be a stable component of the checkpoint digest: a restored generation that materializes `<secret_id>/<v17>` is guaranteed to see the exact bytes that the original `<v17>` saw at allocation time. The mode is therefore `0440` (no owner-write) rather than `0640`; the orchestrator's write-once flow uses `O_CREAT|O_EXCL` with mode `0440`.

POSIX still lets a file owner add write permission with `chmod`; Phase 7 does not treat owner-`chmod` failure as a security boundary. The enforced contract is that the materializer refuses to publish the same `(secret_id, secret_version)` twice, never opens a published file with write flags, and host users outside `orchestrator` / `harness-secret-readers` cannot read it. Deployments that need kernel-enforced owner immutability may add `chattr +i`, but the portable test matrix does not depend on it.

**Materialization into the per-generation control dir.** `hardlink` from `<host_secrets_root>/<secret_id>/<version>` to `<control_dir>/secrets/<secret_id>/<version>` is **safe under the immutability rule** and is the preferred materialization. A hardlink shares inode with the source; since the source is immutable, every generation that hardlinks it observes identical bytes for the lifetime of that version. `copy` is the cross-filesystem fallback (when `<host_secrets_root>` and `<control_dir>` are on different mounts); copy preserves the same byte-for-byte invariant by construction. **Bind-mount-of-the-version-file is explicitly rejected** as a third option: a future operator who issues `mount --bind` over the per-generation file from a fresh source would silently change the bytes a running generation sees without changing `secret_version`, breaking the digest invariant.

For this group bit to actually grant read in the sandbox, the agent process must hold `harness-secret-readers` either as its primary GID or as a supplementary group at `execve` time. The Claude path uses the preferred supplementary-group form, and Phase 7 treats that as a hard contract on any agent that receives a secret mount:

```sh
# Preferred: explicit supplementary group list, primary GID stays 65534.
setpriv --reuid "$AGENT_UID" --regid "$AGENT_GID" \
        --groups "$HARNESS_SECRET_READERS_GID" -- env … "$@"

# Acceptable fallback when only one group is needed: make
# harness-secret-readers the agent's primary GID. Loses the ability to
# chown agent-owned files to a distinct GID, so reserve for environments
# that don't need that.
setpriv --reuid "$AGENT_UID" --regid "$HARNESS_SECRET_READERS_GID" \
        --clear-groups -- env … "$@"
```

`--clear-groups` without an accompanying `--groups <secret-readers-gid>` is an explicit defect for any sandbox that mounts a secret. Tests assert that the Claude runner invokes `setpriv --groups "$HARNESS_SECRET_READERS_GID"` and reads `${SECRET_DIR}/<secret_id>/<secret_version>` after dropping privileges.

Phase 7's credential contract is an at-rest and source-of-truth contract for the
legacy, pre-Phase-8 provider-secret mount path: the manifest/spec/rootfs do not
carry plaintext credentials, and the entrypoint must not fall back to ambient
host Claude configuration. It is **not** a secrecy boundary against Claude
itself. The current Claude runner reads the mounted secret after dropping
privileges and exports it for Claude Code, so the Claude process and
prompt-injected tool children can observe the runtime value through
environment/proc inspection. Phase 8 retires this mounted provider-credential
path and replaces it with a host-side upstream credential boundary plus
source-IP/generation/turn proxy authorization and driver entitlement.

In the active Phase 8 baseline, upstream model provider credentials and remote
MCP bearer tokens must not be mounted into the sandbox, exported through the
driver environment, or made readable by the sandbox entrypoint. There is no
current `/harness-secrets` interface. Any future scoped non-provider secret
delivery needs a new post-Phase-8 design, including non-root execution,
least-privilege access, and explicit release gates.

**Shell agent (`HARNESS_AGENT=sh`) does not mount secrets.** The shell shim does not need upstream model credentials, and Phase 7 forbids shell secret mounts by construction: the shell generation's `agent_runtime_profile` is `agent = sh`, `requires_secret_drop = false`, carries no `anthropic_api_key_secret_id` / `anthropic_auth_token_secret_id`, the per-generation control dir for a shell generation has no `secrets/` subdirectory materialized, and `secret_mount_path` is unset so no bind-mount is added to the runtime spec. The orchestrator validates this at generation-start time: a shell generation whose manifest carries any secret reference is rejected with `error_class = shell_secret_disallowed`. After Phase 8, shell or BYO-agent model access must use the host-side proxy credential boundary, not a provider-secret mount. If a future shell or BYO-agent variant ever needs scoped non-provider secrets, it must first land its own `setpriv --groups "$HARNESS_SECRET_READERS_GID"` drop in the entrypoint and explicitly opt in via `agent_runtime_profile.requires_secret_drop = true` — the doc-level rule is "no secret mount unless the entrypoint demonstrably runs the agent under a non-root UID with the readers group."

For the legacy provider-secret mount path, the per-generation secrets directory
under the control dir is created mode `0750` owned by
`orchestrator:harness-secret-readers`; each `<secret_id>` subdirectory uses the
same mode and ownership, and the `<secret_version>` file is hard-linked or
copied into it preserving owner/group/mode. The bind-mount into the sandbox at
`secret_mount_path` is read-only (`ro,nosuid,nodev,noexec`); read-only bind
enforces that the agent cannot mutate the file, while `0440
group=harness-secret-readers` is what makes the in-sandbox read succeed. This
mount option set is part of the historical Phase 7 contract, not a current
Phase 8 interface.

Phase 11 KMS must not preserve or reintroduce sandbox entrypoint reads for
upstream model provider credentials; those remain host-side after Phase 8. If a
future Phase 11 design supports scoped non-provider driver secrets, it may reuse
only the write-once/versioned materialization idea from Phase 7 after defining a
new mount name, policy, and gate set. If that design uses gVisor
`--file-access=shared` with idmap mounts, the contract becomes "the materialized
file must be readable by the sandbox-mapped UID" and the host group convention
is replaced by idmap remapping.

On the legacy provider-secret path, generation start materializes the
per-generation secrets dir under the control dir and bind-mounts it read-only
into the sandbox at `secret_mount_path`; the entrypoint reads
`${SECRET_DIR}/<secret_id>/<secret_version>` rather than the manifest. The
manifest carries only the secret-reference fields
(`anthropic_api_key_secret_id` / `anthropic_auth_token_secret_id`) plus
`secret_version`, never plaintext, and the entrypoint must not fall back to
host-level Claude configuration. Phase 8 migration must reject or recreate
generations whose manifest/spec still depends on this provider-secret mount
contract.

## Resource Allocator And Reaper

Per-generation resources create a new leak surface, so allocation and cleanup are part of Phase 7, not hardening work later.

Minimum behavior:

```text
create generation row and resource rows in a DB transaction
create host netns/veth/egress/control/bundle resources
mark allocation ready only after host resources exist
start runsc only from a ready allocation
on startup scan DB and host resources
reclaim orphan netns/veth/egress/control/bundle resources
make cleanup idempotent
on failed startup either roll back or mark resources reclaimable
```

### Reaper Ownership Boundary

The reaper owns only resources it can prove this orchestrator created. To make that decision deterministic without a DB row, every host resource is namespaced and tagged with a fixed prefix that no operator-managed resource is allowed to use:

```text
netns:        harness-gen-<generation_id>
veth host:    hgen<short>-h
veth peer:    hgen<short>-s
nft table:    harness_gen_<generation_id with non-identifier chars replaced by _>
control dir:  <run_dir>/control/gen-<generation_id>/
bundle dir:   <run_dir>/runtime/gen-<generation_id>/
bridge dir:   <run_dir>/bridge/gen-<generation_id>/
runsc id:     harness-gen-<generation_id>
```

The reaper selects DB-backed resources by allocation/resource state, not solely by network names. Allocations in `allocating`, `ready`, `live`, `reserved_checkpointed`, or `recreating` are reaper-invisible regardless of whether a process is currently attached: `reserved_checkpointed` has no live process but still owns its identity for restore, and `recreating` is mid-rebuild under an active lease. For reclaimable rows, filesystem cleanup is driven by the stored generation paths after root/generation validation; sandbox network cleanup only runs when the recorded netns/veth/nft names match the `harness-gen-` family. This protects operator-created resources while still cleaning non-sandbox generations and rows with incomplete network metadata.

Legacy `phase1-demo` / `phase2-template` resources are removed once by a one-time Phase 7 migration step and are out of the reaper's domain afterwards.

### Failed-retention window

A `failed` generation moves to `reclaimable` after a configurable retention window (`harness.reaper.failed_retention`, default 10 minutes) so an operator can inspect netns/control/spec/log artifacts before they disappear. The allocation identity (`/30`, netns name, veth pair, control/bundle dirs) stays reserved for the duration of `reclaimable` — it is **not** returned to the pool until the next reaper sweep advances it to `destroyed`. The retention window therefore does not block N+1: cold fallback uses a fresh allocation identity drawn from the next free `/30` slot, which is independent of N's still-occupied identity. After retention expires, the next reaper sweep moves the allocation to `destroyed` on the normal path, and only then is the identity available for reuse.

## Resource Allocation Lifecycle

```text
allocating
  -> ready
  -> live
  -> reclaimable
  -> destroyed

checkpoint path:

live
  -> reserved_checkpointed
  -> recreating
  -> ready
  -> live
  -> reclaimable
  -> destroyed

Failure fallback:

allocating/ready/live/reserved_checkpointed/recreating
  -> reclaimable
  -> destroyed
```

An allocation identity is reusable only after it reaches `destroyed`. `reclaimable` is **not** a release state — it marks an allocation that is no longer holding live host resources but whose identity is still pinned to retention/audit (failed-generation forensics, partially-cleaned-up host objects awaiting the reaper's idempotent pass). The reaper's job is to advance `reclaimable -> destroyed` once retention expires and host artifacts are cleaned, *and only then* can the allocator hand the same `/30` / netns name / veth pair / control dir / bundle dir to a future generation. The `recreating` state is used during physical restore after checkpoint; it holds the generation lease and prevents the reaper from deleting or reassigning the reserved network/control/spec identity while host resources are being rebuilt.

## Allocation Recovery On Startup

Turn-ledger restart recovery covers turns ([schema.md](./schema.md#turns)). Generation-level and allocation-level recovery is symmetric and runs in the same startup sweep, before the reaper opens for business and before any new generation is allocated:

```text
For every runtime_generations row whose lease has expired:

  status in (allocating, starting, probing, restoring, checkpointing):
    -> failed (error_class = orchestrator_restart_during_<status>)
    -> owning allocations move to reclaimable; any allocation in
       allocation_state = recreating is moved to reclaimable in the
       same transaction (recreating is an *allocation* state, never a
       generation status; the generation row was in `restoring` while
       its allocation was being rebuilt).
    -> session enters cold fallback per Step 7 on its next queued turn

  status = active with a running turn whose ack_started_at is set:
    -> remains non-terminal with an expired lease; ordinary output /
       completion writes are rejected, but hello/resume may renew the
       same generation and turn through the recovery CAS until
       lease_expires_at + ack_started_grace.
       If that deadline expires, the turn is marked failed
       (error_class = unknown_after_ack_started), the generation moves
       to failed, and cold fallback applies only after the fence.

  status = active or idle with no ack_started_at running turn:
    -> remains; the bridge is expected to reconnect via hello.
       If the bridge does not reconnect within
       lease_expires_at + reconnect_grace, the generation transitions
       to failed and cold fallback applies.

  status in (failed, destroyed):
    -> no-op; allocations already on their terminal path.

  status = checkpointed:
    -> no-op; this generation status has no live lease by design
       (its allocation_state is reserved_checkpointed).
```

The reaper does not run this sweep — it only knows how to reclaim resources whose allocation is already in `reclaimable`/`destroyed`. Recovery is what *moves* rows into those states after a crash. Without this sweep, a crashed-mid-restore generation would sit in `restoring` (with its allocation in `recreating`) indefinitely, holding its reserved netns/IP/control identity and shrinking the pool.

If the stored `orchestrator_owner.boot_id` differs from the current `/proc/sys/kernel/random/boot_id`, the host rebooted. In that case the startup sweep treats every expired lease as a hard fence immediately, because no pre-reboot process, mount, or socket can still be alive to reconnect through the grace windows.
