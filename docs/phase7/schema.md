# Schema, CAS Helpers, And Migrations

This file owns every Phase 7 SQL fragment: table definitions, indexes, the four-call-site CAS helper, and the migration runner. The semantics for each row (when fields are set, what guards they enforce) are defined here once; other docs link to the section that owns the rule.

## Tables

### `sessions`

```text
session_id
user_id
agent
status
claude_session_uuid
workspace_path
agent_home_path
active_generation_id        -- FK -> runtime_generations.generation_id, nullable
created_at
updated_at
last_activity_at
expires_at
```

The session row must not imply that a specific process is alive. It only says whether the session is eligible to accept input and what recovery policy applies. `expires_at` is the absolute deadline beyond which the session moves to `destroyed` regardless of activity; it is set on session create as `created_at + harness.session_ttl` (the lab default surfaces as `HARNESS_SESSION_TTL=2h`) and refreshed on explicit user extension, not on every turn. A session whose `expires_at` is in the past is reaped on the next sweep alongside its allocations.

`active_generation_id` is updated by CAS predicates (`update sessions set active_generation_id = :new where active_generation_id = :old_or_null`) so a buggy concurrent allocator collides on the row, not just on the partial unique index defined under `runtime_generations` below.

### `turns`

```text
turn_id
session_id
sequence
role = user
content
status = queued | leased | running | completed | failed | canceled
generation_id
lease_owner
lease_expires_at
attempt
ack_started_at
completed_by_generation
retry_policy
created_at
started_at
completed_at
error_class
error
```

The rule is:

```text
No user message is sent to a sandbox until it has a durable turn row.
No turn is considered complete until a durable completion event is
recorded.
```

**Claim ordering invariant.** `claim_next_turn` always picks `MIN(sequence)` over `WHERE session_id = :session_id AND status = 'queued' AND lease_owner IS NULL`. This rule is independent of which generation is claiming and survives cold fallback: when a previous generation N fails and the startup sweep requeues its expired-leased-without-`ack_started_at` turn (attempt+1), and the user injects a fresh turn between N's failure and N+1's first claim, both rows land in `queued` and the `sequence` column resolves which one N+1 picks first. The session never has two concurrently-claimed turns from different generations, because `runtime_generations` has at most one row in `(active, idle)` at a time per session and the helper's generation CAS rejects claims against transient-state generations (Single-Helper Contract, case 1).

**Restart recovery rules.** These also govern cold fallback retry eligibility (cold fallback may retry only what restart recovery would requeue):

```text
queued:
  keep queued

leased, ack_started_at is null, lease expired:
  requeue and increment attempt according to retry_policy

running, ack_started_at is set:
  do not auto-retry; wait for bridge reconnect or transition to
  unknown_after_ack_started for user-visible resolution
  (see [checkpoint-restore.md](./checkpoint-restore.md#user-visible-recovery-for-unknown_after_ack_started))

completed | failed | canceled:
  never auto-retry
```

### `events`

```text
event_id          -- INTEGER PRIMARY KEY AUTOINCREMENT, globally monotonic per orchestrator
session_id
turn_id
generation_id
sequence
dedupe_key
proxy_request_id
stream
severity
type
payload
created_at
```

`event_id` is allocated only by the host event store as `INTEGER PRIMARY KEY AUTOINCREMENT` under the orchestrator's single-writer SQLite, making it monotonic globally per orchestrator process (not per session). This is what makes the global SSE stream's `Last-Event-ID` cursor meaningful across session-filter changes — see [bridge-protocol.md](./bridge-protocol.md#sse-wire-protocol-step-8). Sandbox bridge messages must not supply a global event ID. Bridge output messages use a per-turn sequence; the host deduplicates bridge output by `(turn_id, generation_id, output_sequence)` and rejects re-emits silently (see [bridge-protocol.md](./bridge-protocol.md#idempotency-and-sequence-recovery)). `(session_id, sequence)` is the per-session ordering key used by replay consumers.

`dedupe_key` is an optional bridge-supplied idempotency token for non-output messages whose `(turn_id, generation_id, output_sequence)` triple does not apply (e.g. lifecycle re-emits after a transport replay). When set, the host enforces uniqueness over `(session_id, dedupe_key)` and a duplicate insert is dropped silently — same semantics as the output dedup. Lifecycle messages typically rely on the turn-state CAS for idempotency and leave `dedupe_key` NULL; future bridge messages that need transport-level dedup without a CAS guard set it explicitly.

Proxy metrics are stored as typed event payloads, usually `proxy.request.started`, `proxy.request.completed`, or `proxy.request.failed`. The correlation fields `proxy_request_id`, `turn_id`, and `generation_id` are first-class columns; latency, retry, timeout, and upstream details can remain in `payload` or be promoted to generated/query columns if needed for dashboards.

Event durability is a hard invariant, but the transaction boundary is per message kind, not per turn:

- **Lifecycle messages** (`ack_turn_started`, `ack_turn_completed`, generation status changes, failure marks) are appended to the event log in the same transaction as the turn-state CAS, before any in-memory hub publish. There is no race where a UI sees a turn complete that the durable ledger does not.
- **`emit_output` messages** carry no turn-state transition. They are appended in their own transaction. Implementations may batch a bounded number of consecutive `emit_output` messages from one bridge call into one transaction — bound it by row count or wall time, not by turn — to keep SQLite single-writer throughput viable for Claude stream-json's hundreds of partial deltas per turn. Batching is bounded; a turn's lifecycle ack always commits in its own transaction.

### `runtime_generations`

```text
generation_id
session_id
runsc_container_id
status = allocating | starting | probing | active | idle | checkpointing
       | checkpointed | restoring | failed | destroyed
checkpoint_path
network_profile_id
agent_runtime_profile_id
resource_allocation_id
runtime_bundle_dir
runtime_config_path
runsc_platform = systrap
runsc_version
control_dir
control_manifest_path
control_manifest_digest
workspace_path
agent_home_path
lease_owner
lease_expires_at
started_at
last_seen_at
ended_at
failure_reason
```

The generation ID prevents an old restored container from writing events into a newer session execution. Every event and turn ack must carry the generation ID. `runsc_platform` and `runsc_version` are recorded on the generation row and must equal the binary that started it; the same fields are copied into checkpoint metadata to make exact-match restore validation cheap.

Generated bundle/spec data is per generation. Rootfs and base bundle assets should not be copied per generation:

```text
static rootfs / base bundle assets
  + per-generation bundle dir
  + generated config.json
  + per-generation control dir bind mount
  + session workspace bind mount
  + agent home bind mount
```

The generation-scoped `config.json` owns the netns path, control mount source, workspace mount, agent home mount, and generation metadata. The static template is only an input to generation, not the runtime spec for live containers.

**Partial unique index — at most one non-terminal generation per session.** Claim ordering and lease fencing rely on the property that a session has at most one row in non-terminal status. The migration ships:

```sql
create unique index runtime_generations_one_nonterminal_per_session
  on runtime_generations (session_id)
  where status not in ('failed', 'destroyed');
```

`failed` and `destroyed` are the only terminal generation statuses; every other status (`allocating`, `starting`, `probing`, `active`, `idle`, `checkpointing`, `checkpointed`, `restoring`) is non-terminal and therefore subject to the index. Cold fallback inserting N+1 must therefore happen in the same transaction that moves N out of any non-terminal status — concretely: N is CAS-updated to `failed` and N+1 is inserted in one DB transaction, which makes the index a fail-fast safety net for any code path that tries to start a second live generation while the first is still alive. The session row also carries `sessions.active_generation_id`; both mechanisms are required: the index protects the invariant from arbitrary writers; the CAS gives orchestrator paths a deterministic conflict signal without relying on the index error class.

The Phase 7a migration ships both the partial unique index and `sessions.active_generation_id` together; tests against the migration assert (a) inserting a second non-terminal generation row for the same session fails with a uniqueness error, and (b) cold fallback's "fail N + insert N+1" transaction succeeds because the CAS step on N runs first.

### `network_profiles`

```text
network_profile_id
session_id
generation_id
runsc_network = sandbox
runsc_overlay2 = none
host_proxy_bind_url = http://0.0.0.0:8082
proxy_port = 8082
host_gateway_ip = allocated per generation
sandbox_base_url = http://{host_gateway_ip}:8082
probe_url = http://{host_gateway_ip}:8082
netns_name
netns_path
host_veth
sandbox_veth
sandbox_ip_cidr
egress_policy_id
allowed_egress_rules
doris_fe_hosts
doris_be_hosts
doris_ports
dns_policy
host_side_cidr
allocation_state = allocating | ready | live | reserved_checkpointed
                 | recreating | reclaimable | destroyed
created_at
destroyed_at
```

Network resources are allocations, not just text. The allocator enforces uniqueness over every non-destroyed row for: `netns_name`, `netns_path`, `host_veth`, `sandbox_veth`, `host_gateway_ip`, `host_side_cidr`, `sandbox_ip_cidr`, `control_dir`, `control_manifest_path`, `runtime_bundle_dir`. `reserved_checkpointed` allocations are not live processes, but their identity stays reserved for physical restore and must not be reused.

Field semantics, the `/30` allocator, and the egress-list contract are defined in [network-and-probes.md](./network-and-probes.md).

### `agent_runtime_profiles`

```text
agent_runtime_profile_id
agent = claude
model = sonnet
output_format = stream-json
disable_nonessential_traffic = true
manifest_anthropic_base_url = http://{host_gateway_ip}:8082   (templated from network profile)
anthropic_api_key_secret_id
anthropic_auth_token_secret_id
secret_version
created_at
```

Each `runtime_generations` row references exactly one `network_profile_id` and one `agent_runtime_profile_id`. **Both references are immutable for the lifetime of the generation row** — written at allocation time and never mutated. Any change that requires a new generation also requires a new `network_profile` row drawn from a fresh allocation; the predecessor's allocation moves through the standard `live -> reclaimable -> destroyed` path and its identity is not reused until the row is `destroyed`. This holds even when only the agent runtime profile is changing (model swap, `disable_nonessential_traffic` flip): the new generation gets a freshly-allocated `/30`, netns, veth pair, and control/bundle dirs.

The single exception that reuses host resources is **physical restore of the same `generation_id`** ([invariants.md](./invariants.md#hard-invariants), [runtime-resources.md](./runtime-resources.md#resource-allocation-lifecycle)): the same generation row's allocation transitions `reserved_checkpointed -> recreating -> ready -> live` while keeping its `network_profile_id` and `agent_runtime_profile_id` bindings; it is not "another generation" and therefore not subject to the no-reuse rule.

`manifest_anthropic_base_url` is templated from `host_gateway_ip` at manifest emission — the agent inside the sandbox sees only the gateway IP, never `0.0.0.0`.

### `runtime_generation_resources`

`network_profiles` and `agent_runtime_profiles` describe *what was allocated* (CIDR, netns name, veth names, model, secret refs) — i.e., the contract a future generation must reproduce on restore. They do not capture *the host filesystem and process artifacts that exist only while the allocation is live* (per-generation control dir, bundle dir, `runsc` `config.json` path, sandbox PID, log paths, lock paths). Phase 7a writes those into a dedicated row keyed by `generation_id`:

```text
runtime_generation_resources

generation_id              -- PK, also FK to runtime_generations(generation_id)
network_profile_id         -- FK; redundant with runtime_generations for join speed
agent_runtime_profile_id   -- FK; redundant with runtime_generations for join speed

control_dir_path           -- per-generation control dir (Control Manifest)
control_manifest_path      -- absolute path to session.json under control_dir
control_manifest_digest    -- JCS digest the entrypoint validates against
bundle_dir_path            -- per-generation runsc bundle dir
spec_path                  -- absolute path to bundle config.json
secrets_dir_path           -- per-generation secrets dir under control_dir, or NULL
                             (NULL for shell-agent generations; see Secret Materialization)

bridge_dir_path            -- per-generation file-backed bridge transport
                              root: <bridge_root>/<generation_id>/
                              with inbox/, outbox/, heartbeat/ subdirs
                              (Agent Bridge Protocol)
log_dir_path               -- per-generation log dir

runsc_pid                  -- sandbox PID at last observed start; nullable
runsc_version              -- exact version string at start; immutable after first write

resource_state             -- allocating | ready | live |
                             reserved_checkpointed | recreating |
                             reclaimable | destroyed
                             (mirrors network_profile.allocation_state for fast lookup;
                              the network profile remains the source of truth for
                              the network-only subset of state)
created_at
destroyed_at
```

**Relationship to `network_profiles`.** `network_profiles` owns the *network* allocation lifecycle (CIDR slot, netns, veth, gateway, egress) — that table's `allocation_state` continues to be the authoritative state machine for releasing the `/30` and netns. `runtime_generation_resources` owns the *non-network* host artifacts (control/bundle/log dirs, secret dir, bridge dir, runsc bundle paths) and mirrors `allocation_state` into `resource_state` so that the reaper and recovery sweeps can answer "is everything for this generation reclaimable yet?" in one query without joining three tables. Both rows are written in the same allocation transaction and both rows are deleted (or moved to `destroyed`) in the same reclaim transaction; the partial unique index that constrains "one non-terminal generation per session" sits on `runtime_generations`, not on either resource table, so resource-row uniqueness is enforced by the FK alone.

**Relationship to `runtime_generations`.** `runtime_generations` is the fencing row (status, lease_owner, generation_id identity); it does not store paths or PIDs. Implementations that want to grow new per-generation host artifacts (e.g. a checkpoint payload directory at Step 9) add a column to `runtime_generation_resources`, not to `runtime_generations`, so that the fencing row stays narrow and the resource row carries everything the reaper needs to delete from disk.

**Why a separate row, not extra columns on `runtime_generations` or on `network_profiles`.** Adding these fields to `runtime_generations` would mix fencing identity with disk-path bookkeeping and make the partial unique index harder to reason about. Adding them to `network_profiles` would conflate "network shape contract" with "this-allocation host artifacts," and would make the shell-agent case (no secrets dir, no `secrets_dir_path`) misshape the network profile. A separate resources table also lets the Step 9 checkpoint code add a `checkpoint_payload_path` column without modifying network or agent profiles.

**Lifecycle.** The allocator inserts the `runtime_generation_resources` row in the same transaction that inserts `runtime_generations` and `network_profiles`; the reaper deletes the row when its companion `network_profiles.allocation_state` reaches `destroyed`. The row never moves between generations (no FK churn), and `runsc_version` is `O_CREAT|O_EXCL`-equivalent: written once at first observed sandbox start and never overwritten, which is what the Step 9 restore-validation rule keys off of.

### `egress_policies`

Materialized allow-rules referenced by `network_profiles.egress_policy_id`. On 7a, every `network_profiles` row gets an `egress_policies` row whose contents are the lab-wide static allow-list; Phase 8 turns this into per-tenant policy. The schema is identical between 7a and 8; what changes in 8 is the *source* of values and the enforcement strength. See [network-and-probes.md](./network-and-probes.md#egress-policy).

### `orchestrator_owner`

Singleton meta row written after the `<run_dir>/orchestrator.pid` flock is acquired. Defined in [invariants.md](./invariants.md#concurrency-and-storage-model).

```text
orchestrator_owner
  uuid                -- random per process start
  boot_id             -- /proc/sys/kernel/random/boot_id
  host_run_dir        -- the run_dir whose flock is held
  acquired_at
  heartbeat_at        -- refreshed every 5 s
```

Every recovery sweep, allocator commit, and reaper pass reads `orchestrator_owner.uuid` and asserts it equals the in-process value. A mismatch aborts the sweep and exits the process.

## Single-Helper Contract

The helper has exactly four call sites. Each call site is one DB transaction containing both the turn CAS and the `runtime_generations.status` CAS. The generation CAS predicate must explicitly **exclude** transient generation statuses (`allocating`, `checkpointing`, `restoring`, `failed`, `destroyed`) so that no race can flip a generation to `active` while it is mid-allocation, mid-checkpoint, or mid-restore. (`recreating` does not appear in this list because it is an *allocation* state, not a generation status; the generation row of an allocation in `recreating` is in `restoring`, which is already excluded.)

### 1. `claim_next_turn` — bridge picks up a queued turn

```sql
-- Turn CAS: claim the lowest-sequence queued turn for this session.
-- See Turn Ledger for the MIN(sequence) ordering invariant.
update turns
set status = 'leased',
    generation_id = :generation_id,
    lease_owner = :owner,
    lease_expires_at = :now + :lease_ttl,
    attempt = attempt + (case when status = 'leased' then 1 else 0 end)
where id = (
    select id from turns
    where session_id = :session_id
      and status = 'queued'
      and lease_owner is null
    order by sequence asc
    limit 1
)
  and not exists (                          -- single-in-flight invariant
      select 1 from turns
      where generation_id = :generation_id
        and status in ('leased', 'running')
  )
returning id;

-- Generation CAS: idle -> active. Must reject checkpointing/restoring/etc.
update runtime_generations
set status = 'active'
where generation_id = :generation_id
  and status in ('idle', 'active')   -- already-active is a no-op CAS
  and lease_owner = :owner
  and lease_expires_at > :now;
```

If the turn CAS returns no row (no queued work, or the single-in-flight predicate matched an already-running turn), the generation CAS is skipped. If the generation CAS affects zero rows, the transaction aborts and the turn claim is rolled back — the generation was concurrently moved to `checkpointing` / `restoring` / `failed` and is not eligible to claim. The bridge's caller treats both as "no work for now" and retries on next poll; neither is an error. The single-in-flight predicate is what makes proxy source-IP correlation sound (see [network-and-probes.md](./network-and-probes.md#proxy-and-upstream-observability)).

### 2. `ack_started` — bridge confirms the turn started executing

```sql
update turns
set status = 'running',
    started_at = :now,
    ack_started_at = :now
where id = :turn_id
  and status = 'leased'
  and session_id = :session_id
  and generation_id = :generation_id
  and lease_owner = :owner
  and lease_expires_at > :now;

-- Generation cache is already 'active' from claim. No flip needed,
-- but the helper asserts it: any other status here is a bug, not a race.
update runtime_generations
set last_seen_at = :now
where generation_id = :generation_id
  and status = 'active';
```

The second statement is a guard, not a state flip. If it affects zero rows the transaction aborts: the generation was not `active` when ack_started fired, which means the cache and ledger had already drifted before this call.

### 3. `completion` — `ack_turn_completed`, including `completed`, `failed (turn-level)`, and `canceled`

```sql
update turns
set status = :terminal_status,    -- completed | failed | canceled
    completed_at = :now,
    completed_by_generation = :generation_id,
    error_class = :error_class,    -- NULL for completed
    error = :error_text            -- NULL for completed
where id = :turn_id
  and status in ('leased', 'running')
  and session_id = :session_id
  and generation_id = :generation_id
  and lease_owner = :owner
  and lease_expires_at > :now;     -- active-lease predicate (see note below)

-- Generation CAS: active -> idle iff no other leased/running turn remains.
update runtime_generations
set status = 'idle'
where generation_id = :generation_id
  and status = 'active'
  and lease_owner = :owner
  and lease_expires_at > :now
  and not exists (
      select 1 from turns
      where generation_id = :generation_id
        and status in ('leased', 'running')
  );
```

The `NOT EXISTS` subquery is the predicate that captures "this is the last live turn for this generation." If another turn is concurrently leased on the same generation (allowed under the protocol once a generation is `active`, even if Phase 7 currently runs one turn at a time per generation), the generation stays `active` and the cache remains correct. The CAS does not match `checkpointing` / `restoring` — those states cannot host a running turn by [Hard Invariants](./invariants.md#hard-invariants), so observing one here would already be a bug; the predicate's narrowness makes it self-checking.

**Active-lease predicate (applies to all four call sites except recovery).** Every non-recovery write path includes both `lease_owner = :owner` *and* `lease_expires_at > :now`. The owner predicate alone is not sufficient: an orchestrator-internal scheduler stall, an SQLite write that beats a stuck goroutine to the row, or a long-running bridge call that returns past the lease deadline can each produce a "late completion" landing after the recovery sweep has already requeued the turn under a fresh attempt. Without the expiry check, that late completion would silently overwrite the requeued attempt's row. With the expiry check it is rejected, the helper bubbles up "expired-lease completion ignored," and the durable record is the recovery sweep's outcome — which is the correct one because the orchestrator already declared the original attempt dead. The recovery sweep itself is the only path allowed to write under an expired lease, and it does so under a different CAS predicate (`lease_expires_at <= :now`) keyed on `orchestrator_owner.uuid`, never on the prior `lease_owner` string.

### 4. `failure / cancel of the generation while a turn is in flight`

Used by the grace-window expiry path, the lease-expiry sweep, and explicit operator cancel.

```sql
-- Fence the turn (single statement; status depends on case).
update turns
set status = 'failed',
    completed_at = :now,
    error_class = :error_class,    -- e.g. unknown_after_ack_started, lease_expired
    error = :error_text
where id = :turn_id
  and status in ('leased', 'running')
  and session_id = :session_id
  and generation_id = :generation_id
  and lease_owner = :owner
  and lease_expires_at > :now;     -- active-lease predicate; lease-expiry sweep
                                   --   uses the recovery path instead (see below).

-- Generation CAS: active -> failed (or idle if other turns survive).
-- Generation-level failure (not per-turn) takes the generation to failed:
update runtime_generations
set status = 'failed',
    failure_reason = :reason,
    ended_at = :now,
    lease_owner = null
where generation_id = :generation_id
  and status in ('active', 'idle', 'starting', 'probing')
  and lease_owner = :owner
  and lease_expires_at > :now;
```

Operator cancel and grace-window expiry both run on the active orchestrator with a still-valid lease, so the `lease_expires_at > :now` predicate matches and these paths use the CAS above. The lease-expiry sweep is structurally different — by definition the lease is no longer active — and runs through [allocation recovery's expired-lease path](./runtime-resources.md#allocation-recovery-on-startup), whose CAS predicate is `lease_expires_at <= :now AND orchestrator_owner.uuid = :current_owner_uuid` (not the prior lease string). Recovery is therefore the *only* code path that can move a turn or generation forward without an active lease; every other helper call is rejected by the predicate.

For per-turn failure that does not condemn the generation (e.g. a turn-level error that the agent itself raised cleanly), the helper falls back to the completion CAS in case 3 with `:terminal_status = 'failed'` and the generation stays `active`/`idle` per the same NOT EXISTS predicate. The distinction — turn-failure vs generation-failure — is decided by the caller, not by the helper.

Across all four call sites, **`lease_owner` is keyed on `<orchestrator_owner.uuid>:<role_tag>`** (see [invariants.md](./invariants.md#generation-lease)). A restarted orchestrator never matches a prior owner string, so a stale call site whose code was somehow still running across restart cannot mutate the generation; only the startup-recovery sweep can fence an expired lease.

## SQLite Migration Strategy

The current `Store.migrate` in `orchestrator/internal/store/store.go:78` is a single bootstrap pass of `CREATE TABLE IF NOT EXISTS` / `CREATE INDEX IF NOT EXISTS` statements. It is sufficient for fresh installs but does *not* run any `ALTER TABLE`, does not add new columns to pre-existing tables, and has no notion of schema versions. A lab DB carried over from Phase 6 will not pick up `sessions.active_generation_id`, the `runtime_generations_one_nonterminal_per_session` partial index, or any of the new Phase 7 tables (`runtime_generations`, `network_profiles`, `agent_runtime_profiles`, `egress_policies`, `turns`, `events`, `orchestrator_owner`) just because the binary is upgraded. Phase 7a therefore replaces the bootstrap pass with an explicit, ordered, version-tracked migration runner.

### `schema_migrations` table

The first migration writes a singleton tracking table:

```sql
create table if not exists schema_migrations (
    version    integer primary key,    -- monotonic; gaps not allowed
    name       text    not null,
    applied_at text    not null        -- RFC3339 UTC
);
```

The runner is wrapped in `BEGIN IMMEDIATE … COMMIT`, with `PRAGMA foreign_keys=ON` and the existing `MaxOpenConns(1)` single-writer guarantee. Each migration body runs in its own transaction; on partial failure the transaction is rolled back and `schema_migrations` is unchanged so a re-run resumes from the same version.

### Required Phase 7a migrations

The migrations land in this order, each as one `version` row, and each one must be idempotent under re-run (the runner re-checks `schema_migrations` and a re-applied no-op is allowed for any migration that previously committed):

```text
v1  baseline_schema
      Capture the current Phase 6 schema as version 1 so existing DBs
      can be tagged: `INSERT OR IGNORE INTO schema_migrations VALUES
      (1, ...)` after asserting the legacy tables (sessions, messages,
      artifacts, etc.) exist with their current columns. Fresh installs
      run the identical CREATE TABLE statements.

v2  phase7_baseline_tables
      CREATE TABLE: orchestrator_owner, runtime_generations,
      network_profiles, agent_runtime_profiles, egress_policies,
      runtime_generation_resources. All new tables come up empty.

v3  phase7_turn_and_event_log
      CREATE TABLE: turns, events.
      Both tables come up empty for legacy DBs; events.event_id is the
      orchestrator-global monotonic primary key (Durable Event Log).

v4  phase7_session_columns
      ALTER TABLE sessions ADD COLUMN active_generation_id TEXT
        REFERENCES runtime_generations(generation_id);
      The column starts NULL on every legacy row. No backfill in this
      migration — Phase 7a keeps the existing stdin/PTY path running,
      and the existing path is what eventually writes a generation row
      and patches active_generation_id via the standard CAS predicate.

v5  phase7_indexes
      CREATE INDEX statements for the per-table indexes referenced
      throughout this document, plus the partial unique index
      `runtime_generations_one_nonterminal_per_session`. The partial
      index can be created without a migration of the empty
      runtime_generations table because no rows yet exist; v6's legacy
      session backfill is what first inserts into runtime_generations.

v6  phase7_legacy_session_backfill
      For every legacy session row that the existing code path would
      treat as still-running (status in sessionstate.ActiveStatuses,
      ended_at IS NULL):
        - Mark status = 'failed' with a typed reason
          ('legacy_pre_phase7_no_generation') and set ended_at = now.
        - DO NOT synthesize a runtime_generations row. The legacy
          stdin/PTY container that backed the row is gone after
          orchestrator restart and there is no fenced generation_id to
          attach. The frontend already knows how to render `failed`.
      For sessions that were `checkpointing` or `checkpointed` in the
      pre-Phase-7 schema:
        - Their existing checkpoint_path / restore_id fields stay on
          the row for forensic value, but the row is moved to
          `failed` with reason 'legacy_checkpoint_unrestorable'.
          Pre-Phase-7 checkpoint images do not carry the
          runsc-version / bundle-digest / manifest-digest metadata
          that Phase 7 restore validates against
          (see [checkpoint-restore.md](./checkpoint-restore.md)),
          so they cannot be restored under the Phase 7 contract. This
          is the explicit one-time cost of the Phase 7 cutover; the
          user can start a fresh session and Claude logical resume
          preserves their conversation history if the original
          `claude_session_uuid` was recorded.
      Idempotent: re-running v6 finds no eligible rows (the prior pass
      already moved them to a terminal state).
```

`v6` is intentionally aggressive about not inventing fenced generations. Manufacturing a fake `runtime_generations` row to satisfy `sessions.active_generation_id` would create a row that no allocator owns, no reaper will reclaim by name (it would fail the `harness-gen-<id>` ownership filter), and no resource allocation row backs. It is structurally safer to declare the legacy session terminal and let the user resume via Claude's conversation UUID than to forge generation rows.

### Migration tests

The `store` package ships a migration test suite that exercises the runner against real legacy fixtures, not just an empty in-memory DB:

```text
- test/migration_fixtures/v1_phase6_clean.sqlite
    A snapshot of a Phase 6 lab DB taken from a running instance.
    Test: open under the Phase 7 Store, assert all six versions apply
    to completion, schema_migrations ends at version 6, every Phase 7
    table exists, every legacy session row is either still-active
    (no eligible candidates in this fixture) or `failed` with the
    documented reason.

- test/migration_fixtures/v1_phase6_with_running.sqlite
    A snapshot with mid-flight running sessions and one
    `checkpointed` session. Test: v6 backfill moves the running
    session to `failed (legacy_pre_phase7_no_generation)` and the
    checkpointed session to `failed (legacy_checkpoint_unrestorable)`,
    leaves messages / artifacts / claude_session_uuid intact for
    forensic value, and never creates a phantom runtime_generations
    row.

- test/migration_resume_after_partial_failure
    A fault-injection runner that aborts each migration mid-statement
    and asserts that on re-run the runner resumes from the
    last-committed version with no double-applied DDL.

- test/migration_idempotence
    Runs every migration twice in a row against a fresh DB and
    asserts the second pass is a no-op (no `ALTER TABLE` errors, no
    duplicate-row errors, schema_migrations row count unchanged).
```

`go test ./orchestrator/internal/store/...` runs these on every CI build; a fresh fixture is captured from the lab as part of the Phase 7a cutover so the suite tracks the actual on-disk shape an operator will see, not an idealized one.

### Operational note

A pre-Phase-7 DB that fails the v6 backfill (e.g., a row that violates a `CHECK` constraint introduced in v2) blocks orchestrator startup with a typed error pointing at the offending row. The operator's choice is to repair the row by hand or to start fresh by removing the SQLite file; there is no auto-skip path, because silently dropping the row would lose audit data the operator may want to keep.
