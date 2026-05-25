# Implementation Plan

This file owns the Phase 7 configuration schema (Step 0 prerequisite), the 10-step PR roadmap with prerequisites and acceptance signals, and Phase-7-wide operational notes. Behavioral detail lives in the linked sections of the other phase7 docs.

## Phase 7 Configuration Schema

Phase 7 introduces a non-trivial set of operator-facing configuration keys. The checked-in `config/harness.yaml` uses the `harness:` schema below, and `orchestrator/internal/config/config.go` loads it through a typed `Phase7Config` YAML parser rather than the pre-Phase-7 scalar scanner.

The parser is `gopkg.in/yaml.v3`, fed into structs with `yaml` tags. The unmarshaler runs in strict mode (`KnownFields(true)`) so any key not present in the struct fails the load with a typed error pointing at the file/line; the previous `unknown config key` behavior is preserved. Durations decode through a custom `Duration` wrapper that wraps `time.ParseDuration`; CIDRs decode through `net/netip.ParsePrefix`; `dns_policy` decodes through a typed `DnsPolicy` enum. Tests cover a concrete `harness:` YAML fixture, the checked-in config file, legacy-only compatibility, mixed-section rejection, unknown-key rejection, and validation failures.

Strict mode applies to a top-level wrapper that knows both the target `harness:` section and legacy `runtime:` / `claude:` sections. The loader still accepts a legacy-only file during the 7a cutover and translates defaults into `Phase7Config`. Mixed `harness:` plus legacy sections are rejected to avoid ambiguous precedence.

Treat the keys below as a contract between the architecture doc and the loader: a new key referenced anywhere in this Phase 7 doc set must also be added here, with a default and a validation rule, in the same change.

```yaml
harness:
  run_dir: /var/lib/harness/run            # parent of orchestrator.pid and the
                                           # derived control/runtime/bridge roots
  session_ttl: 2h                          # absolute deadline applied at session create;
                                           # the session sweep reaps expired rows before
                                           # the first turn, independently of lease_ttl
  max_sessions: 30                         # soft policy ceiling reported by /api/quota;
                                           # must be < CIDR pool /30 capacity

  network:
    cidr_pool: 10.200.0.0/16               # pool from which /30 per-generation
                                           # subnets are carved
    egress:                                # static lab-wide allow-list applied to
                                           # every 7a-allocated network_profile
      doris_fe_hosts: []                   # required non-empty in production-like mode
      doris_be_hosts: []                   # required non-empty in production-like mode
      doris_ports: []                      # required non-empty in production-like mode
      dns_policy: hostnames_only           # off | hostnames_only | always

  events:
    retention_window: 24h                  # rolling time window
    retention_rows: 1_000_000              # row ceiling, whichever first
    emit_output_batch_max_rows: 64         # bounds per-batch transaction width
    emit_output_batch_max_age: 100ms       # bounds per-batch transaction wait

  probe:
    accept_status:
      get_healthz: [200]
      post_v1_messages:
        unauthorized: [401]                # missing or wrong key
        malformed_authenticated: [400]    # configured key + malformed body
    pre_start_attempts: 3
    pre_start_interval: 500ms
    post_start_attempts: 5
    post_start_interval: 1s

  bridge:
    lease_ttl: 60s
    heartbeat_interval: 30s                # nominal half-life; warn above half,
                                           # reject at >= lease_ttl
    poll_interval: 10ms                    # queue poll cadence for inbox/outbox
    ack_started_grace: 90s                 # after lease expiry, only for
                                           # ack_started running turns
    reconnect_grace: 30s                   # after lease expiry, only when
                                           # no ack_started turn is running

  checkpoint:
    auto_enabled: false                    # per-session policy default; env override
                                           # HARNESS_AUTO_CHECKPOINT_ENABLED applies
                                           # only to newly created sessions
    idle_threshold: 30m                    # running_idle age before eligibility; 0
                                           # means immediately eligible
    monitor_interval: 5m                   # host-side candidate scan cadence

  reaper:
    failed_retention: 10m

  secrets:
    root: /var/lib/harness/secrets         # <host_secrets_root>; mode 0750
    readers_gid: 65501                     # HARNESS_SECRET_READERS_GID, baked into
                                           # the sandbox image at build
```

The per-generation roots are derived from `harness.run_dir`, not configured separately: `control_root = <run_dir>/control`, `bundle_root = <run_dir>/runtime`, and `bridge_root = <run_dir>/bridge`. Every path-bearing Phase 7 doc should use those derived roots, not hard-coded `/var/lib/harness/...` prefixes.

### Validation rules

Enforced by the loader, asserted by unit tests:

```text
- harness.network.cidr_pool must be a valid CIDR with prefix length <=
  30 and must be wide enough to host harness.max_sessions /30 slots
  (loader rejects if max_sessions exceeds the pool's /30 count).
- harness.session_ttl must be > 0; the loader rejects `0` to prevent
  sessions that are reaped before they accept their first turn.
- For 7a "production-like mode" (the only mode the lab supports),
  egress doris_fe_hosts / doris_be_hosts / doris_ports must be
  non-empty; the loader fails with a typed error if any is missing.
  dns_policy != off is required when any Doris host is a hostname
  rather than an IP.
- bridge.ack_started_grace must be > 0 and >= bridge.reconnect_grace.
  It starts after lease expiry for `ack_started_at` running turns; it
  does not change the active-lease predicate.
- bridge.reconnect_grace must be >= 0.
- bridge.heartbeat_interval must be > 0 and < bridge.lease_ttl.
  Values in [bridge.lease_ttl / 2, bridge.lease_ttl) are accepted but
  warn; values < bridge.lease_ttl / 2 are accepted quietly; values
  >= bridge.lease_ttl are rejected.
- bridge.poll_interval must be > 0. The default is sized to keep the
  end-to-end turn-start budget under 50 ms at lab load; larger values
  require a benchmark re-check before deployment.
- probe.accept_status.get_healthz, probe.accept_status.post_v1_messages.unauthorized,
  and probe.accept_status.post_v1_messages.malformed_authenticated must each be
  non-empty.
- checkpoint.idle_threshold must be >= 0. A zero value is valid and makes
  eligible idle generations checkpointable on the next monitor tick.
- checkpoint.monitor_interval must be > 0. checkpoint.auto_enabled defaults
  false during 7b validation and can be overridden for newly created sessions
  with HARNESS_AUTO_CHECKPOINT_ENABLED.
- events.retention_window and events.retention_rows must both be set;
  the earlier-hit bound governs trim. Setting one to zero disables
  that bound; setting both to zero is rejected.
- reaper.failed_retention must be >= 0; 0 disables the inspection
  window and moves failed generations directly to `reclaimable` on the
  next sweep.
- secrets.root must exist with mode 0750 and group readers_gid before
  the flock is taken; loader fails fast if not.
```

The loader produces a single typed `Phase7Config` struct that is passed by value into the allocator, reaper, bridge, and probe components. There is no ambient/global config access path.

A "Phase 7 config + loader + validation" change is a prerequisite of Step 1 (it appears in Step 1's *Depends on* column). It is small enough to land as a separate PR ahead of Step 1, but it is not optional and the table treats it as a hard dependency.

## Implementation Order

One architecture target, landed in small PRs. Partial states are scaffolding, not a supported architecture: cold fallback may not retry turns before the claim/ack protocol exists, no generation claims a turn before its bridge probe, no automatic checkpoint before restore/reconnect/fallback are correct. **Steps 1–4 are Phase 7a; Steps 5–10 are Phase 7b.** A PR can land before the full 7a set; only Steps 1–4 together are 7a-complete.

Each step lists its PR boundary, prerequisites, and acceptance signal. Behavioral detail lives in the linked sections of the other phase7 docs.

### Step 1 — Schema, migrations, helper module, flock

**Depends on:** Phase 7 Config (separately landed, see above).

**PR boundary.** Replace `Store.migrate`'s single `IF NOT EXISTS` pass with the versioned migration runner described in [schema.md](./schema.md#sqlite-migration-strategy) (introduce `schema_migrations` and migrations v1–v6 in order); add tables/indexes for `runtime_generations`, `runtime_generation_resources`, `turns`, `events`, `active_model_request_contexts`, `network_profiles`, `agent_runtime_profiles`, `egress_policies`, `orchestrator_owner`; add Phase 7 session columns (`active_generation_id`, `agent_home_path`, `failure_reason`, `error_class`) while keeping legacy `workspace` as the workspace path; add the partial unique index `runtime_generations_one_nonterminal_per_session`; acquire the `<run_dir>/orchestrator.pid` flock at startup.

**Acceptance.** Schema covers generation fencing, allocation states, turn attempts, claim idempotency, lease expiry, bridge ack state, event replay, and proxy active-context observability. The "at most one non-terminal generation per session" invariant is enforced by the partial unique index and by CAS on `sessions.active_generation_id`; migration tests assert both. The migration runner applies in version order, is idempotent under re-run, and ships with the legacy-fixture suite from [schema.md](./schema.md#migration-tests) (v1 baseline tag, v4 session-column backfill, v6 backfill of pre-Phase-7 running/checkpointed sessions to `failed` with typed `error_class` / `failure_reason`; `created` rows stay `created`). The orchestrator acquires the `<run_dir>/orchestrator.pid` flock before opening SQLite, writes the `orchestrator_owner` row, and refreshes `heartbeat_at` every 5 s; a second orchestrator process pointed at the same run dir fails fast, and any allocator/recovery sweep whose `orchestrator_owner.uuid` no longer matches the in-process value aborts and exits. The single helper module's API surface (claim, heartbeat renew, ack_started, completion, generation failure) and unit tests land in this PR, but it is not yet the live turn executor. Step 2 is the first step that writes generation rows and resource rows; Step 6 is where the live claim/ack call sites move onto the helper. Migration tests assert (a) flock contention rejects the second startup, (b) tampered `orchestrator_owner.uuid` aborts the next sweep, (c) inserting a second non-terminal generation row for one session fails with the partial-index uniqueness error, and (d) the legacy-fixture suite cleanly converges after a single migration pass.

**Detail:** [invariants.md](./invariants.md#concurrency-and-storage-model), [schema.md](./schema.md), [schema.md](./schema.md#single-helper-contract).

### Step 2 — Resource allocator + reaper

**Depends on:** 1.

**PR boundary.** Resource allocator + reaper before per-generation runtime changes, plus the 7a record-keeping turn-ledger shim on the existing stdin/PTY path.

**Acceptance.** Generation row + resource rows allocated transactionally; Step 2 only reserves the row/state skeleton and does not create the concrete bundle/control/network host objects yet; orphan reclaim on startup; cleanup idempotent; reaper respects active leases and `recreating` allocations; expired sessions are destroyed on the next sweep and queued work on those sessions is rejected before allocation. During 7a the runtime-manager goroutine remains the lease renewer on the existing stdin/PTY path and keeps refreshing `runtime_generations.lease_expires_at` on the half-life cadence; the bridge is not yet in the renewal path. Existing `sendMessage` / `runSession` writes a non-authoritative ledger row for each submitted turn and marks it `completed` / `failed` when the current stream parser finishes; this shim is covered by tests but does not claim to recover in-flight 7a turns.

**Detail:** [runtime-resources.md](./runtime-resources.md#resource-allocator-and-reaper).

### Step 3 — Per-generation bundle, spec, control manifest

**Depends on:** 2.

**PR boundary.** Per-generation bundle, spec, and control manifest. Remove `phase2-template` from live path.

**Acceptance.** Per-generation `config.json` + control dir; static rootfs/base reused without copy; atomic `session.json` write; entrypoint validates identity + JCS digest; entrypoint resolves `anthropic_api_key` / `anthropic_auth_token` from `${SECRET_DIR}/<secret_id>/<secret_version>` (manifest carries no plaintext credential field, no fallback to host-level Claude config); concurrent session startup writes to distinct `control_manifest_path` values.

**Detail:** [runtime-resources.md](./runtime-resources.md#control-manifest), [runtime-resources.md](./runtime-resources.md#secret-materialization).

### Step 4 — Per-generation network and egress

**Depends on:** 3.

**PR boundary.** Per-generation network and egress rendered into the per-generation `config.json` from Step 3. Remove `phase1-demo` from the live path and stop baking `/var/run/netns/phase1-demo` into runtime specs.

**Acceptance.** Unique netns/veth/IP/gateway/CIDR per generation; `sandbox_base_url` derived from allocated `host_gateway_ip`; egress policy applied (local proxy + configured Doris FE/BE + DNS as required by `harness.network.egress`); pre-start host-side netns probe passes; no live netns reconfigured after `runsc` attaches; no `runscSandboxNetnsName` / `runscSandboxNetnsCIDR` / `runscSandboxGatewayIP` Go constants remain in the runtime package (every value read from the allocation row); `GET /api/quota` reports the soft session ceiling and the live pool ceiling; pool exhaustion surfaces as `503` with `error_class = pool_exhausted` in the shared `{"error_class","error"}` envelope rather than a stuck queue.

**Detail:** [network-and-probes.md](./network-and-probes.md).

> **End of Phase 7a.** After Step 4, every session is on its own resources and the schema is in place for 7b. The existing stdin/PTY turn path still runs with Step 2's record-keeping ledger shim. Acceptance: zero references to `phase1-demo` or `phase2-template` in the runtime hot path; reaper cleans only namespaced resources; restart recovery works against the new schema; full claim/ack CAS coverage remains a Step 6 deliverable.

### Step 5 — Agent Bridge protocol over file-backed transport

**Depends on:** 3, 4.

**PR boundary.** Agent Bridge protocol over file-backed transport.

**Acceptance.** Bridge exposes `hello`/`heartbeat`/`probe_network`/`claim_next_turn`/`ack_turn_started`/`emit_output`/`ack_turn_completed`; a generation cannot claim until bridge has connected, identified itself, and passed the in-sandbox probe. Bridge dir mount is declared with the runsc-equivalent of `file-access=exclusive` (current pin: `dev.gvisor.spec.mount.<dest>.share=exclusive` for `runsc release-20260511.0`); generated `config.json` is asserted to carry this annotation and the induced-crash durability test (host process killed between bridge fsync and host read) passes.

**Detail:** [bridge-protocol.md](./bridge-protocol.md).

### Step 6 — Turn execution via claim/ack with durable events

**Depends on:** 5.

**PR boundary.** Turn execution via claim/ack with DB lease/CAS fencing and durable event recording per [schema.md](./schema.md#events) transaction rules (lifecycle acks committed with the turn-state CAS; `emit_output` appended in its own bounded-batch transactions, both before any in-memory publish).

**Acceptance.** `queued → leased → running (ack_turn_started) → completed (ack_turn_completed)`; claim CAS binds `turns.session_id`, `runtime_generations.session_id`, and `sessions.active_generation_id` in one transaction; duplicate `claim_next_turn(request_id)` returns the original grant; bridge heartbeat renews both generation and current turn leases; stale-generation events rejected; output dedup by `(turn_id, generation_id, output_sequence)`; the legacy `sendMessage` / `runSession` ledger shim stops writing authoritative `turns` / `runtime_generations` rows in the same cutover that moves the live claim/ack path onto the bridge; lifecycle/output transaction rules per [schema.md](./schema.md#events).

**Detail:** [schema.md](./schema.md#turns), [invariants.md](./invariants.md#lease-and-cas-fencing), [schema.md](./schema.md#events).

### Step 7 — Cold Claude resume fallback

**Depends on:** 6.

**PR boundary.** Cold Claude resume fallback.

**Acceptance.** Failed generation fenced; N+1 reuses `ClaudeSessionUUID` / agent home / workspace; only queued or leased-but-not-started turns retry; `ack_started_at` turns enter `unknown_after_ack_started` flow.

**Detail:** [checkpoint-restore.md](./checkpoint-restore.md#cold-fallback-path).

### Step 8 — Durable event log + SSE replay

**Depends on:** 6.

**PR boundary.** Durable event log + SSE replay on the existing `/api/events/stream` endpoint, plus the proxy observability boundary: orchestrator implements `/internal/proxy/requests/start` and `/finish`, and the pinned `claude-code-proxy` build used by the lab calls them. If the proxy is released separately, Step 8 depends on that pinned version and is not accepted until the contract test passes.

**Acceptance.** Replay-from-`last_event_id` against the global stream, retention, proxy correlation join; host assigns `event_id` (globally monotonic per orchestrator), bridge owns per-turn `output_sequence`; proxy request IDs joined through the active-context API backed by `active_model_request_contexts`, with TTL cleanup on heartbeat/terminal turn/restart. SSE frames emit `id:` (host event_id) and `event:` (event type) lines; server honors the `Last-Event-ID` HTTP header and the `?last_event_id=` query-string fallback; the optional `?session_id=` filter narrows replay/live frames, but the cursor is only valid for the same filter value; retention-gap reconnects produce a single `replay_gap` synthetic event before the live tail resumes.

**Detail:** [bridge-protocol.md](./bridge-protocol.md#sse-wire-protocol-step-8), [network-and-probes.md](./network-and-probes.md#proxy-and-upstream-observability).

### Step 9 — Checkpoint-safe restore

**Depends on:** 7, 8.

**PR boundary.** Checkpoint-safe restore.

**Acceptance.** Generation status `checkpointed` moves to `restoring` under lease (allocation_state `reserved_checkpointed → recreating`); compatible netns/veth/IP/egress/control/spec recreated; pre-restore host probe and post-restore in-sandbox probe pass before claim; any failure uses the lifecycle failure CAS to move generation N from `restoring` / `checkpointing` to `failed` (allocation_state -> `reclaimable`) + cold fallback N+1.

**Detail:** [checkpoint-restore.md](./checkpoint-restore.md#checkpoint-policy).

### Step 10 — Automatic checkpoint policy

**Depends on:** 9.

**PR boundary.** Automatic checkpoint policy.

**Acceptance.** Triggers only on idle generation with empty turn queue, bridge checkpoint-ready, output flushed; `autoCheckpointEnabled` promoted from Go const to a per-session policy (default off during 7b validation, on for the lab profile after Step 10). The policy gates whether the next idle generation of a session is eligible for auto-checkpoint, not the in-flight generation. Config lives under `harness.checkpoint` (`auto_enabled`, `idle_threshold`, `monitor_interval`), with `HARNESS_AUTO_CHECKPOINT_ENABLED` as the lab override for newly created sessions. Checkpoint remains an executor optimization, not a correctness mechanism.

**Detail:** [checkpoint-restore.md](./checkpoint-restore.md#checkpoint-policy).

## Operational Notes

- **runsc upgrades invalidate every checkpoint.** Checkpoints carry the runsc build version (see [checkpoint-restore.md](./checkpoint-restore.md#runsc-version-exact-match)). After a runsc upgrade in the lab, every `reserved_checkpointed` allocation will fail validation on next restore; this is expected, not an incident. The orchestrator marks each affected checkpoint `reclaimable` and cold-starts. Operationally: drain checkpointed sessions before upgrade if cold-start latency matters, otherwise let restore fail and fall back. There is no in-place migration path for checkpoint payloads.
