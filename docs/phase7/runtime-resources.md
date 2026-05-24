# Runtime Resources

Per-generation host artifacts: control manifest, secret materialization, allocator, reaper, recovery sweep. The DB rows that back these artifacts (`runtime_generations`, `network_profiles`, `runtime_generation_resources`) are defined in [schema.md](./schema.md). This file owns *what lives on disk and how it is created, validated, and destroyed*.

## Control Manifest

Each generation gets an isolated control directory and manifest. The manifest must include enough identity to make stale or cross-wired starts fail closed:

```text
session_id
generation_id
network_profile_id
agent_runtime_profile_id
agent
claude_session_uuid
resume_claude
anthropic_base_url
anthropic_api_key_secret_id
anthropic_auth_token_secret_id
secret_version
secret_mount_path
model
workspace_path
agent_home_path
manifest_version
manifest_digest
```

`manifest_digest` is computed over the canonical manifest payload excluding the digest field itself. The on-disk `session.json` is a single top-level object `{ "payload": <manifest content>, "digest": "<hex>" }`; splitting `payload` and `digest` at the top level removes ordering ambiguity around the digest field, since verifiers feed `payload` bytes into the canonicalizer and never the wrapper. The canonicalization rule is RFC 8785 / JCS: UTF-8, lexicographic key order, no insignificant whitespace, shortest round-trip decimal numbers, JSON-spec string escapes with no `\u` escapes for printable ASCII. Verification is `parse → JCS-canonicalize payload → sha256 → constant-time compare`. Both the host and the sandbox entrypoint (sh + Python) implement the same rule; the same digest is reused for `control_manifest_digest` in checkpoint metadata.

The sandbox rootfs is therefore required to ship: `python3` with a vendored JCS implementation (Python's standard library does not provide one), and an HTTP client usable from sh + Python (`curl` is sufficient) for the in-sandbox `probe_network()`. These are hard dependencies of `harness-agent-entrypoint`, not optional extras; sandbox-image build must fail closed if either is missing.

The control plane writes the manifest atomically:

```text
write session.json.tmp
fsync file
rename session.json.tmp -> session.json
fsync parent directory
```

The entrypoint must validate `session_id`, `generation_id`, `network_profile_id`, `agent_runtime_profile_id`, `manifest_version`, `secret_version`, and `manifest_digest` before starting the agent. Resolved credentials are read from `${SECRET_DIR}/<secret_id>` per the secret-mount contract below. A mismatch on any of these fields exits non-zero with a code distinguishable from agent crashes; the host marks the generation `failed` with `error_class = manifest_digest_mismatch` (or the matching `*_mismatch` class for the offending field).

## Secret Materialization

Secret values are referenced only by `secret_id` + `secret_version`. In Phase 7a the "secret store" is a host-local directory: `<host_secrets_root>/<secret_id>/<secret_version>` containing the plaintext value as a single file. The on-disk permission model must let the in-sandbox agent (UID `65534` per `harness-agent-entrypoint`) read the file while keeping it unreadable to anything else on the host, including other local users:

```text
<host_secrets_root>                 mode 0750, owner orchestrator,
                                     group harness-secret-readers
<host_secrets_root>/<secret_id>     mode 0750, same owner/group
<host_secrets_root>/<secret_id>/    mode 0440, owner orchestrator,
  <secret_version>                   group harness-secret-readers
                                     (immutable after publish — see below)
```

`harness-secret-readers` is a dedicated host group (GID baked into the sandbox image at build time as `HARNESS_SECRET_READERS_GID`) whose only member is UID `65534` (the same UID the sandbox maps the agent to, since gVisor with the default `--file-access=exclusive` does not user-namespace-remap and the in-sandbox UID is the host UID). The orchestrator chowns secret files to `orchestrator:harness-secret-readers` at write time and runs `chmod 0440`; the agent reads as `65534` via the group bit. Mode `0400` owned by the orchestrator is **not** acceptable — the sandbox would silently fail to read it; the `0440 group=harness-secret-readers` contract is what makes the cross-UID read succeed.

**Secret version immutability (hard rule).** A `<secret_id>/<secret_version>` file is **immutable after publish**. Once written, the orchestrator never reopens it for write — neither for rotation, nor to "re-encrypt," nor as a fixup path. Rotation publishes a *new* `<secret_version>` row and a *new* file at `<host_secrets_root>/<secret_id>/<new_version>`; consumers that should pick up the rotation get a new generation that references the new `secret_version`. Old version files are removed only by the GC pass after every generation that referenced them has reached `destroyed` (and after the configured grace window for in-flight checkpoints to finish referencing them). This is what lets `secret_version` be a stable component of the checkpoint digest: a restored generation that materializes `<secret_id>/<v17>` is guaranteed to see the exact bytes that the original `<v17>` saw at allocation time. The mode is therefore `0440` (no owner-write) rather than `0640`; the orchestrator's write-once flow uses `O_CREAT|O_EXCL` with mode `0440` and the file is never `chmod +w`'d again.

**Materialization into the per-generation control dir.** `hardlink` from `<host_secrets_root>/<secret_id>/<version>` into `<control_dir>/secrets/` is **safe under the immutability rule** and is the preferred materialization. A hardlink shares inode with the source; since the source is immutable, every generation that hardlinks it observes identical bytes for the lifetime of that version. `copy` is the cross-filesystem fallback (when `<host_secrets_root>` and `<control_dir>` are on different mounts); copy preserves the same byte-for-byte invariant by construction. **Bind-mount-of-the-version-file is explicitly rejected** as a third option: a future operator who issues `mount --bind` over the per-generation file from a fresh source would silently change the bytes a running generation sees without changing `secret_version`, breaking the digest invariant.

For this group bit to actually grant read in the sandbox, the agent process must hold `harness-secret-readers` either as its primary GID or as a supplementary group at `execve` time. The current `harness-agent-entrypoint` invocation `setpriv --reuid 65534 --regid 65534 --clear-groups …` strips supplementary groups and would fail the read. The required shape is one of the following, and Phase 7a treats this as a hard contract on the entrypoint:

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

`--clear-groups` without an accompanying `--groups <secret-readers-gid>` is an explicit defect for any sandbox that mounts a secret. The integration test for secret read **must** assert that an in-sandbox `id -G` lists `HARNESS_SECRET_READERS_GID` and that `cat <secret_mount_path>/<secret_id>/<version>` succeeds; today that test would fail and that is the point — Phase 7a's secret materializer ships only after the entrypoint is fixed to one of the two shapes above.

A root-entrypoint-then-drop alternative (entrypoint reads the secret as root and injects via env before dropping to UID 65534) is **not** part of this contract: it would put plaintext in the agent process environment, which is observable via `/proc/self/environ` and survives across `execve`, and it would break the bind-mount-only model that lets Phase 8 swap directory storage for KMS without touching the entrypoint.

**Shell agent (`HARNESS_AGENT=sh`) does not mount secrets.** The current `harness-agent-entrypoint:132` `exec`s the shell shim as root and does not run the `setpriv` drop the `claude` branch uses, so a root shell would bypass the `0440 group=harness-secret-readers` containment if the shell sandbox were also offered the secret bind-mount. Phase 7a forbids this by construction: the shell generation's `agent_runtime_profile` carries no `anthropic_api_key_secret_id` / `anthropic_auth_token_secret_id`, the per-generation control dir for a shell generation has no `secrets/` subdirectory materialized, and `secret_mount_path` is unset so no bind-mount is added to the runtime spec. The orchestrator validates this at generation-start time: a shell generation whose manifest carries any secret reference is rejected with `error_class = shell_secret_disallowed`. If a future shell or BYO-agent variant ever needs upstream credentials, it must first land its own `setpriv --groups "$HARNESS_SECRET_READERS_GID"` drop in the entrypoint and explicitly opt in via `agent_runtime_profile.requires_secret_drop = true` — the doc-level rule is "no secret mount unless the entrypoint demonstrably runs the agent under a non-root UID with the readers group." Until then, the only way to give a shell session model access is via a separate Claude generation in the same session.

The per-generation secrets directory under the control dir is created mode `0750` owned by `orchestrator:harness-secret-readers`, and the per-secret file is hard-linked or copied into it preserving the same owner/group/mode. Hardlink is preferred when secrets root and control dir share a filesystem; copy is the fallback across filesystem boundaries. The bind-mount into the sandbox at `secret_mount_path` is read-only (`ro,nosuid,nodev,noexec`); read-only bind enforces that the agent cannot mutate the file, while `0440 group=harness-secret-readers` is what makes the in-sandbox read succeed.

Phase 8 replaces the directory backend with KMS without changing this contract — the entrypoint still reads `${SECRET_DIR}/<secret_id>` as UID `65534`, and the KMS-backed materializer is responsible for writing files with the same owner/group/mode and for choosing whether to materialize via tmpfs to keep plaintext off persistent storage. If a future Phase 8 design uses gVisor `--file-access=shared` with idmap mounts, the contract becomes "the materialized file must be readable by the sandbox-mapped UID" and the host group convention is replaced by idmap remapping; the entrypoint contract is unchanged.

At generation start the host materializes the per-generation secrets dir under the control dir and bind-mounts it read-only into the sandbox at `secret_mount_path`; the entrypoint reads `${SECRET_DIR}/<secret_id>` rather than the manifest. The manifest carries only `secret_id` + `secret_version`, never plaintext, and the entrypoint must not fall back to host-level Claude configuration.

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
nft chain:    harness-gen-<generation_id>
control dir:  /var/lib/harness/control/gen-<generation_id>/
bundle dir:   /var/lib/harness/runtime/gen-<generation_id>/
runsc id:     harness-gen-<generation_id>
```

The reaper considers a resource for reclamation only if its name matches the `harness-gen-` family **and** its allocation row is in `reclaimable` or `destroyed`. Allocations in `allocating`, `ready`, `live`, `reserved_checkpointed`, or `recreating` are reaper-invisible regardless of whether a process is currently attached: `reserved_checkpointed` has no live process but still owns its identity for restore, and `recreating` is mid-rebuild under an active lease. Anything that does not match the `harness-gen-` family is invisible to the reaper, even if it lives in the same directories or namespaces. This protects operator-created or externally provisioned resources sharing the host.

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

  status = active or idle:
    -> remains; the bridge is expected to reconnect via hello.
       If the bridge does not reconnect within bridge_reconnect_grace,
       the generation transitions to failed and cold fallback applies.

  status in (failed, destroyed):
    -> no-op; allocations already on their terminal path.

  status = checkpointed:
    -> no-op; this generation status has no live lease by design
       (its allocation_state is reserved_checkpointed).
```

The reaper does not run this sweep — it only knows how to reclaim resources whose allocation is already in `reclaimable`/`destroyed`. Recovery is what *moves* rows into those states after a crash. Without this sweep, a crashed-mid-restore generation would sit in `restoring` (with its allocation in `recreating`) indefinitely, holding its reserved netns/IP/control identity and shrinking the pool.
