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

A generation crash or permanent bridge unreachability while a turn is `running` with `ack_started_at` set is the one case the orchestrator cannot resolve unilaterally — the prompt may already be billed and partially answered. The default action is **await reconnect** for a bounded grace window (`ack_started_grace`, default **90 s = bridge heartbeat × 3**, configurable via `harness.bridge.ack_started_grace`). A reconnect during the grace window is not a special path: the bridge runs the standard `hello` → `hello_ack` (which returns `leased_turn_id` and `last_output_sequence_by_turn`) → `resume_turn` flow, and the turn continues. The grace window is a host-side timer that fences the generation if it expires; the protocol is unchanged.

`ack_started_grace` and `lease_ttl` (default 60 s) are deliberately distinct timers and **must not collapse to the same value**. `lease_ttl` decides "is this generation's lease expired and recoverable by the startup sweep"; `ack_started_grace` decides "should the user keep waiting for the bridge to come back, or surface `unknown_after_ack_started`." The invariant is `ack_started_grace > lease_ttl`: lease expiry without reconnect must happen first, so the generation is fenced and N+1 can be cold-started inside the user-facing grace window if needed. Setting them equal collapses "lease still valid, awaiting reconnect" with "lease expired, declare failure," which loses the grace-window UX. Heartbeat cadence (default 30 s = `lease_ttl / 2`) is the third timer and is the unit `ack_started_grace` is expressed in multiples of, so any retune of one of the three should be evaluated against all three together.

If the grace window expires without reconnect, the turn becomes `failed (error_class = unknown_after_ack_started)` and the UI offers two actions: **abandon** (keep partial output as labeled-partial audit data) or **resubmit** (a brand-new `turn_id`; original remains failed for audit; no automatic replay). The session is never blocked — a new turn always provisions a fresh generation. `unknown_after_ack_started` turns are audit records, not queue heads.

Resubmit closes the grace window. The first action — abandon, resubmit, or grace expiry — fences the original generation with `error_class = unknown_after_ack_started` in the same transaction, so a late bridge reconnect is rejected as stale and N+1 is the only generation servicing the session. This preserves the single-active-generation invariant when the user does not wait out the grace window.

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

Checkpoint should produce:

```text
checkpoint_path
checkpoint_created_at
checkpoint_generation_id
checkpoint_network_profile_id
checkpoint_agent_runtime_profile_id
checkpoint_resource_allocation_id
checkpoint_runsc_version
checkpoint_runsc_platform
checkpoint_bundle_digest
checkpoint_runtime_config_digest
checkpoint_control_manifest_digest
```

Restore should validate:

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

- **`checkpoint_bundle_digest`** — covers the OCI runtime bundle (`config.json` + rootfs reference). This is **strict-match**. No fields in the bundle are regenerable; a mismatch always fails restore (`reclaimable` + cold fallback).
- **`checkpoint_runtime_config_digest`** — covers the JCS-canonicalized executor runtime config (effectively the same content the bundle hashes plus normalized environment). Strict-match, same fallback rules as bundle digest.
- **`checkpoint_control_manifest_digest`** — covers the JCS-canonicalized control manifest *with regenerable fields stripped before hashing*. The manifest digest stored at checkpoint time and the manifest digest computed at restore time are both calculated over the same projected JCS form; if both digests match, the manifest is considered equivalent even if other fields (the regenerable set) differ. If the projected digests do not match, restore fails with `reclaimable` + cold fallback.

The regenerable set — and **only** this set — may differ between checkpoint-time and restore-time without forcing cold fallback. Each field listed here is justified by the fact that its value is mechanically reproducible from non-manifest sources (allocation row, host config, runsc build) and re-deriving it is required *because* the host identity legitimately changes across restore (timestamps, attempt counters, host hostnames):

```text
regenerable (excluded from manifest digest projection):
  - manifest.created_at         (regenerated from current wall clock)
  - manifest.attempt_id          (regenerated per restore attempt)
  - manifest.host_hostname       (regenerated from current host)
  - manifest.bridge_socket_paths (regenerated from allocation row)
  - manifest.heartbeat_paths     (regenerated from allocation row)
  - manifest.netns_name          (regenerated from allocation row)
  - manifest.host_gateway_ip     (regenerated from allocation row)

strict (included in manifest digest projection — any mismatch -> cold fallback):
  - manifest.session_id
  - manifest.generation_id (NOTE: a fresh restore reuses the same
      generation_id; only cold fallback issues a new one)
  - manifest.agent_runtime_profile_id
  - manifest.runsc_version
  - manifest.runsc_platform
  - manifest.bundle_digest                (transitively binds OCI bundle)
  - manifest.runtime_config_digest        (transitively binds runtime cfg)
  - manifest.secret_id and secret_version (rotation invalidates the
      checkpoint by design — Phase 8 will lift this with a separate
      indirection)
  - manifest.egress_policy_digest         (Doris/DNS allow-list contents)
  - manifest.spec_digest                  (the config.json that runsc
      restore will see after re-rendering)
```

The manifest writer at checkpoint time and the restore validator both project the manifest through the same field-allowlist filter before computing the digest, so the comparison is `digest(strict_fields_at_checkpoint) == digest(strict_fields_at_restore)`. The orchestrator must reject a restore where the projected digest mismatches **and** must reject any code path that adds a new manifest field without explicitly classifying it as regenerable or strict; the migration that adds a new manifest field is required to update the projection function and ship the matching test.

## `runsc` Version Exact-Match

`runsc version` is exact-match (see strict list). The current deployment pins `runsc release-20260511.0` (see [../runsc-warm-sentry-research.md](../runsc-warm-sentry-research.md)); a checkpoint produced under any other build is treated as incompatible. gVisor does not promise cross-build checkpoint compatibility, and a silently-restored mismatch is the worst failure mode (silent sentry state corruption). On any validation failure, the checkpoint row -> `reclaimable`, the allocation transitions through `recreating` to `destroyed`, and the session cold-starts a new generation with Claude logical resume.

Checkpointed generation network strategy: the live process and netns may be released after checkpoint, but the recorded netns/IP/veth/egress/spec/manifest identity remains reserved (see `reserved_checkpointed` allocation state). Before `runsc restore`, the runtime manager recreates that identity and runs the pre-restore probe; after restore, the bridge runs the post-restore probe. Failure to recreate or probe -> generation N failed, cold fallback N+1.

> **Operational note.** runsc upgrades invalidate every checkpoint. After a runsc upgrade in the lab, every `reserved_checkpointed` allocation will fail validation on next restore; this is expected, not an incident. The orchestrator marks each affected checkpoint `reclaimable` and cold-starts. Operationally: drain checkpointed sessions before upgrade if cold-start latency matters, otherwise let restore fail and fall back. There is no in-place migration path for checkpoint payloads.
