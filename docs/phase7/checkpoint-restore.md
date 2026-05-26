# Checkpoint, Restore, And Recovery

There are two independent resume layers:

```text
Logical resume:
  ClaudeSessionUUID + Claude home + persisted transcript + workspace

Physical resume:
  runsc checkpoint image + runtime generation restore
```

The target architecture treats logical resume as required for correctness and physical resume as an optimization.

## Hot Path

```text
turn queued
  -> active generation exists
  -> bridge claims turn
  -> Claude Code receives stream-json input
  -> parser records result
  -> turn completed
```

## Restore Path

```text
turn queued
  -> session has checkpointed generation
  -> recreate generation N's compatible network resources + apply egress policy
  -> pre-restore host-side netns probe passes
  -> runsc restore generation N
  -> bridge reconnects and announces generation N
  -> post-restore in-sandbox probe passes
  -> bridge claims queued turn -> Claude continues
```

## Cold Fallback Path

```text
turn queued or leased but not started
  -> restore fails or bridge fails to reconnect
  -> mark generation N failed
  -> start generation N+1 from bundle with same ClaudeSessionUUID / resume flag
  -> pre-start netns probe + post-start in-sandbox probe pass
  -> Claude resumes logical conversation; bridge claims queued turn
```

This fallback is what makes the system reliable; the user should not be blocked on perfect `runsc restore` behavior. Retry eligibility is governed by the [turn ledger restart recovery rules](./schema.md#turns) — `ack_started_at` turns are never auto-replayed.

## User-Visible Recovery For `unknown_after_ack_started`

A generation crash or bridge outage while a turn is `running` with `ack_started_at` set is the one case the orchestrator cannot resolve unilaterally — the prompt may already be billed and partially answered. The default action is **await reconnect** for `ack_started_grace` (default 90 s) after the turn/generation lease expires.

The timers have separate jobs:

- `lease_ttl` controls normal write fencing. Once it expires, ordinary output/completion writes are rejected until the bridge reconnects and renews the lease.
- `reconnect_grace` applies only to expired `active`/`idle` generations with no `ack_started_at` running turn.
- `ack_started_grace` applies to expired `running` turns with `ack_started_at` set. During this window the generation is recoverable but not fenced, and N+1 is not started.

A reconnect during `ack_started_grace` uses the standard `hello` → `hello_ack` → `resume_turn` flow. The host renews the same generation and turn lease through a recovery CAS keyed by `session_id`, `generation_id`, `turn_id`, and `orchestrator_owner.uuid`; after renewal the normal active-lease predicates apply again.

`unknown_after_ack_started` is written only when `ack_started_grace` expires or the user explicitly abandons/resubmits. That transaction marks the turn `failed`, fences the generation with `error_class = unknown_after_ack_started`, and only then allows cold fallback N+1. A late reconnect after that fence is rejected as stale. Resubmit creates a brand-new `turn_id`; the original remains failed for audit and is never auto-replayed.

## Checkpoint Policy

Checkpoint should be allowed only when all of these are true:

```text
session status is running_idle / accepting_input
generation status is idle
no queued eligible turn exists for the session
no leased/running turn exists for the session or generation
bridge heartbeat is healthy
bridge is checkpoint-ready, with no active host control request that must
  survive restore
all output events for the previous turn are durably flushed
network profile is known
checkpoint timeout budget is available
```

Checkpoint should write these metadata fields onto the `runtime_generations` row; the checkpoint image path itself lives on `runtime_generation_resources`:

```text
generation_id                 -- checkpoint_generation_id
checkpoint_created_at
checkpoint_network_profile_id
checkpoint_agent_runtime_profile_id
checkpoint_runsc_version
checkpoint_runsc_platform
checkpoint_bundle_digest
checkpoint_runtime_config_digest
checkpoint_control_manifest_digest
```

The checkpoint image path, control dir, spec path, and bridge/log paths are loaded from `runtime_generation_resources` by `generation_id`; there is no separate resource-allocation ID.

Restore should validate the stored fields before `runsc restore`:

```text
checkpoint exists and has required image files
bundle digest matches checkpoint metadata
runsc version exactly matches checkpoint metadata
network profile is still valid
compatible netns/veth/IP/egress resources are recreated before runsc restore
control manifest/spec digests match the checkpoint metadata or, for a
  bounded set of regenerable fields, recompute to an equivalent digest
pre-restore host-side netns probe passes
bridge reconnects within timeout
post-restore bridge/in-sandbox network probe passes
```

## Digest Equivalence Rules

What "safely regenerated" actually means.

Two digest pairs are validated separately:

- **`checkpoint_bundle_digest`** — covers the OCI runtime bundle (`config.json` + rootfs reference). Strict-match; a mismatch always fails restore (`reclaimable` + cold fallback).
- **`checkpoint_runtime_config_digest`** — covers the canonicalized executor runtime config (effectively the same content the bundle hashes plus normalized environment). Strict-match, same fallback rules as bundle digest.
- **`checkpoint_control_manifest_digest`** — covers the canonicalized control manifest *with regenerable fields stripped before hashing*. The manifest digest stored at checkpoint time and the manifest digest computed at restore time are both calculated over the same projected canonical form; if both digests match, the manifest is considered equivalent even if other fields (the regenerable set) differ. If the projected digests do not match, restore fails with `reclaimable` + cold fallback.

The regenerable set — and **only** this set — may differ between checkpoint-time and restore-time without forcing cold fallback. These field names are the canonical control-manifest fields listed in [runtime-resources.md](./runtime-resources.md#control-manifest); this section only partitions them into regenerable vs strict. Each field listed here is justified by the fact that its value is mechanically reproducible from non-manifest sources (allocation row, host config, runsc build) and re-deriving it is required *because* the host identity legitimately changes across restore (timestamps, attempt counters, host hostnames):

```text
regenerable (excluded from manifest digest projection):
  - manifest.created_at         (regenerated from current wall clock)
  - manifest.attempt_id          (regenerated per restore attempt)
  - manifest.host_hostname       (regenerated from current host)
  - manifest.bridge_dir_path    (regenerated from allocation row;
                                 includes the heartbeat subdir)
  - manifest.netns_name          (regenerated from allocation row)
  - manifest.host_gateway_ip     (regenerated from allocation row)

Every payload field other than `manifest_digest` is either regenerable or strict.

strict (included in manifest digest projection — any mismatch -> cold fallback):
  - manifest.session_id
  - manifest.generation_id (NOTE: a fresh restore reuses the same
      generation_id; only cold fallback issues a new one)
  - manifest.network_profile_id
  - manifest.agent_runtime_profile_id
  - manifest.agent
  - manifest.claude_session_uuid
  - manifest.resume_claude
  - manifest.anthropic_base_url
  - manifest.runsc_version
  - manifest.runsc_platform
  - manifest.bundle_digest                (transitively binds OCI bundle)
  - manifest.runtime_config_digest        (transitively binds runtime cfg)
  - manifest.anthropic_api_key_secret_id
  - manifest.anthropic_auth_token_secret_id
  - manifest.secret_version (rotation invalidates the checkpoint by
      design — Phase 8 will lift this with a separate indirection)
  - manifest.secret_mount_path
  - manifest.model
  - manifest.workspace_path
  - manifest.agent_home_path
  - manifest.manifest_version
  - manifest.egress_policy_digest         (Doris/DNS allow-list contents)
  - manifest.system_prompt_enabled      (Phase 9a addition)
  - manifest.system_prompt_text         (Phase 9a addition)
  - manifest.system_prompt_digest       (Phase 9a addition)
  - manifest.spec_digest                  (the config.json that runsc
      restore will see after re-rendering)
```

The manifest writer at checkpoint time and the restore validator both project the manifest through the same field-allowlist filter before computing the digest, so the comparison is `digest(strict_fields_at_checkpoint) == digest(strict_fields_at_restore)`. The orchestrator must reject a restore where the projected digest mismatches **and** must reject any code path that adds a new manifest field without explicitly classifying it as regenerable or strict; the migration that adds a new manifest field is required to update the projection function and ship the matching test.

> **Phase 9a compatibility note.** Checkpoint manifests created before Phase 9a do not contain `manifest.system_prompt_enabled`, `manifest.system_prompt_text`, or `manifest.system_prompt_digest`. After Phase 9a adds those fields to the strict projection, the first restore attempt for such a checkpoint will fail the projected-digest comparison and cold-fall back, consistent with the `runsc`-version invalidation policy below. The fallback allocates a replacement generation for the same migrated session using that session's stored disabled/empty prompt snapshot; it does not create a new session and does not read the current operator prompt config.

## `runsc` Version Exact-Match

`runsc version` is exact-match (see strict list). The current deployment pins `runsc release-20260511.0` (see [../runsc-warm-sentry-research.md](../runsc-warm-sentry-research.md)); a checkpoint produced under any other build is treated as incompatible. gVisor does not promise cross-build checkpoint compatibility, and a silently-restored mismatch is the worst failure mode (silent sentry state corruption). On any validation failure, the generation moves to `failed`, `network_profiles` / `runtime_generation_resources` move to `reclaimable`, and the session cold-starts a new generation with Claude logical resume.

Checkpointed generation network strategy: the live process and netns may be released after checkpoint, but the recorded netns/IP/veth/egress/spec/manifest identity remains reserved (see `reserved_checkpointed` allocation state). Before `runsc restore`, the runtime manager recreates that identity and runs the pre-restore probe; after restore, the bridge runs the post-restore probe. Failure to recreate or probe -> generation N failed, cold fallback N+1.

> **Operational note.** runsc upgrades invalidate every checkpoint. After a runsc upgrade in the lab, every `reserved_checkpointed` allocation will fail validation on next restore; this is expected, not an incident. The orchestrator marks each affected checkpoint `reclaimable` and cold-starts. Operationally: drain checkpointed sessions before upgrade if cold-start latency matters, otherwise let restore fail and fall back. There is no in-place migration path for checkpoint payloads.
