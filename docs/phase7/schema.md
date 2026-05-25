# Schema, CAS Helpers, And Migrations

This file owns every Phase 7 SQL fragment: table definitions, indexes, the CAS helper, heartbeat renewal, and the migration runner. The semantics for each row (when fields are set, what guards they enforce) are defined here once; other docs link to the section that owns the rule.

## Tables

### `sessions`

```text
id                           -- existing PK; exposed as session_id in APIs/protocols
user_id
status
agent
claude_session_uuid
workspace                   -- legacy column name; canonical workspace path
restore_id                  -- legacy logical restore id; retained for compatibility/forensics
restore_ms                  -- legacy restore duration; retained for compatibility/forensics
checkpoint_path             -- legacy session-level checkpoint path; retained for forensics
agent_home_path
active_generation_id        -- FK -> runtime_generations.generation_id, nullable
auto_checkpoint_enabled     -- default copied to newly allocated generations
created_at
updated_at
last_activity_at
expires_at
ended_at
failure_reason
error_class
```

The session row must not imply that a specific process is alive. It only says whether the session is eligible to accept input and what recovery policy applies. `expires_at` is the absolute deadline beyond which the session moves to `destroyed` regardless of activity; it is set on session create as `created_at + harness.session_ttl` (the lab default surfaces as `HARNESS_SESSION_TTL=2h`) and refreshed on explicit user extension, not on every turn. A session whose `expires_at` is in the past is reaped on the next sweep alongside its allocations. That sweep rejects queued work on the expired session before any new generation is allocated; active turn deadlines are still governed by `turn.lease_expires_at`, not the session TTL.

`active_generation_id` is updated by CAS predicates (`update sessions set active_generation_id = :new where active_generation_id = :old_or_null`) so a buggy concurrent allocator collides on the row, not just on the partial unique index defined under `runtime_generations` below.

`workspace` and `agent_home_path` are session-scoped and survive cold fallback; a new generation reuses them rather than inventing new ones. Phase 7 keeps the existing `sessions.workspace` column name instead of introducing a second `workspace_path` column; manifests may still call the value `workspace_path`.

SQL examples use `:session_id` as a bind parameter whose value is `sessions.id`; Phase 7 does not add a `sessions.session_id` column or a compatibility view.

### `turns`

```text
id                           -- PK; exposed as turn_id in APIs/protocols
session_id
sequence
role = user
content
status = queued | leased | running | completed | failed | canceled
generation_id
lease_owner
lease_expires_at
claim_request_id
claim_granted_at
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

`claim_request_id` is the idempotency key for `claim_next_turn`. It is set in the same CAS that leases the turn, and a duplicate `claim_next_turn` with the same request_id returns the original grant while the lease is active. The migration enforces a partial UNIQUE index on `(session_id, claim_request_id)` where `claim_request_id IS NOT NULL`.

### `events`

```text
event_id          -- INTEGER PRIMARY KEY AUTOINCREMENT, globally monotonic per orchestrator
session_id
turn_id             -- FK -> turns.id
generation_id
output_sequence
dedupe_key
proxy_request_id
stream
severity
type
payload
created_at
```

`event_id` is the replay cursor. It is allocated only by the host event store as `INTEGER PRIMARY KEY AUTOINCREMENT` under the orchestrator's single-writer SQLite, so it is monotonic globally per orchestrator process (not per session). Filtered replays use `WHERE session_id = :session_id AND event_id > :last_event_id ORDER BY event_id`; the `events(session_id, event_id)` index makes that seek cheap without introducing a second ordering column. Sandbox bridge messages must not supply a global event ID.

`output_sequence` is the bridge-owned per-turn sequence. The host deduplicates bridge output by `(turn_id, generation_id, output_sequence)` and rejects re-emits silently (see [bridge-protocol.md](./bridge-protocol.md#idempotency-and-sequence-recovery)). `output_sequence` is only populated for `emit_output` rows.

`dedupe_key` is an optional bridge-supplied idempotency token for non-output messages whose `(turn_id, generation_id, output_sequence)` triple does not apply (e.g. lifecycle re-emits after a transport replay). When set, the host enforces uniqueness over `(session_id, dedupe_key)` and a duplicate insert is dropped silently — same semantics as the output dedup. Lifecycle messages typically rely on the turn-state CAS for idempotency and leave `dedupe_key` NULL; future bridge messages that need transport-level dedup without a CAS guard set it explicitly.

The migration ships a partial UNIQUE index on `(turn_id, generation_id, output_sequence)` for `emit_output` rows, a partial UNIQUE index on `(session_id, dedupe_key)` when `dedupe_key` is present, an index on `(session_id, event_id)` for filtered replay scans, and an index on `proxy_request_id` for proxy finish-event correlation.

Proxy metrics are stored as typed event payloads, usually `proxy.request.started`, `proxy.request.completed`, or `proxy.request.failed`. The correlation fields `proxy_request_id`, `turn_id`, and `generation_id` are first-class columns; latency, retry, timeout, and upstream details can remain in `payload` or be promoted to generated/query columns if needed for dashboards.

Event durability is a hard invariant, but the transaction boundary is per message kind, not per turn:

- **Lifecycle messages** (`ack_turn_started`, `ack_turn_completed`, generation status changes, failure marks) are appended to the event log in the same transaction as the turn-state CAS, before any in-memory hub publish. There is no race where a UI sees a turn complete that the durable ledger does not.
- **`emit_output` messages** carry no turn-state transition. They are appended in their own transaction. Implementations may batch a bounded number of consecutive `emit_output` messages from one bridge call into one transaction — bound it by row count or wall time, not by turn — to keep SQLite single-writer throughput viable for Claude stream-json's hundreds of partial deltas per turn. Batching is bounded; a turn's lifecycle ack always commits in its own transaction.

### `active_model_request_contexts`

Ephemeral table backing the proxy-context API described in [network-and-probes.md](./network-and-probes.md#proxy-and-upstream-observability):

```text
sandbox_source_ip            -- PRIMARY KEY while one turn is active
session_id
generation_id
turn_id                      -- FK -> turns.id
lease_owner
expires_at                   -- copied from the current turn lease
next_request_sequence
registered_at
updated_at
```

Rows are created in the same transaction as `ack_started`, renewed by bridge heartbeat with the turn lease, and deleted when the turn reaches a terminal state. Proxy lookup ignores expired rows, and startup recovery deletes every row whose `lease_owner` is not owned by the current `orchestrator_owner.uuid`.

### `runtime_generations`

```text
generation_id
session_id
status = allocating | starting | probing | active | idle | checkpointing
       | checkpointed | restoring | failed | destroyed
checkpoint_created_at
checkpoint_network_profile_id
checkpoint_agent_runtime_profile_id
checkpoint_runsc_version
checkpoint_runsc_platform
checkpoint_bundle_digest
checkpoint_runtime_config_digest
checkpoint_control_manifest_digest
network_profile_id
agent_runtime_profile_id
runsc_platform = systrap
runsc_version
auto_checkpoint_enabled     -- session policy snapshot captured at allocation
lease_owner
lease_expires_at
started_at
last_seen_at
ended_at
failure_reason
error_class
```

The generation ID prevents an old restored container from writing events into a newer session execution. Every event and turn ack must carry the generation ID. `runsc_platform` and `runsc_version` are recorded on the generation row and copied into the checkpoint fields on the same row to make exact-match restore validation cheap. The generation row is intentionally narrow: it carries fencing, status, lease, profile, and checkpoint metadata only. Generation-scoped path-bearing host artifacts live on `runtime_generation_resources`; session-scoped paths such as `workspace` and `agent_home_path` stay on `sessions`.

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

Network resources are allocations, not just text. The allocator enforces uniqueness over every non-destroyed row for the network identity fields: `netns_name`, `netns_path`, `host_veth`, `sandbox_veth`, `host_gateway_ip`, `host_side_cidr`, `sandbox_ip_cidr`. `reserved_checkpointed` allocations are not live processes, but their identity stays reserved for physical restore and must not be reused.

Field semantics, the `/30` allocator, and the egress-list contract are defined in [network-and-probes.md](./network-and-probes.md).

### `agent_runtime_profiles`

```text
agent_runtime_profile_id
agent = claude | sh
model = sonnet | null
output_format = stream-json | shell_pty
disable_nonessential_traffic = true
requires_secret_drop = true | false
manifest_anthropic_base_url = http://{host_gateway_ip}:8082   (templated from network profile; null for sh)
anthropic_api_key_secret_id         (nullable; null for sh)
anthropic_auth_token_secret_id      (nullable; null for sh)
secret_version                      (nullable; null for sh)
created_at
```

`agent_runtime_profiles` is a dictionary table, not a per-generation allocation pool. The allocator upserts by the full agent-runtime tuple; if an identical tuple already exists, it reuses the existing `agent_runtime_profile_id`, and if not, it inserts a new row. Generations never mutate the referenced row in place.

Each `runtime_generations` row references exactly one `network_profile_id` and one `agent_runtime_profile_id`. **Both references are immutable for the lifetime of the generation row** — written at allocation time and never mutated. Any change that requires a new generation also requires a new `network_profile` row drawn from a fresh allocation; the predecessor's allocation moves through the standard `live -> reclaimable -> destroyed` path and its identity is not reused until the row is `destroyed`. This holds even when only the agent runtime profile is changing (model swap, `disable_nonessential_traffic` flip): the new generation gets a freshly-allocated `/30`, netns, veth pair, and control/bundle dirs.

`claude` profiles carry `requires_secret_drop = true` and non-null secret refs. `sh` profiles carry `requires_secret_drop = false`, null secret refs, and no upstream credential dependency.

The single exception that reuses host resources is **physical restore of the same `generation_id`** ([invariants.md](./invariants.md#hard-invariants), [runtime-resources.md](./runtime-resources.md#resource-allocation-lifecycle)): the same generation row's allocation transitions `reserved_checkpointed -> recreating -> live` while keeping its `network_profile_id` and `agent_runtime_profile_id` bindings; it is not "another generation" and therefore not subject to the no-reuse rule.

`manifest_anthropic_base_url` is templated from `host_gateway_ip` at manifest emission — the agent inside the sandbox sees only the gateway IP, never `0.0.0.0`.

### `runtime_generation_resources`

`network_profiles` and `agent_runtime_profiles` describe *what was allocated* (CIDR, netns name, veth names, model, secret refs) — i.e., the contract a future generation must reproduce on restore. They do not capture *the host filesystem and process artifacts that exist only while the allocation is live* (per-generation control dir, bundle dir, `runsc` `config.json` path, sandbox PID, log paths, lock paths, checkpoint payload path). Phase 7a writes those into a dedicated row keyed by `generation_id`:

```text
runtime_generation_resources

generation_id              -- PK, also FK to runtime_generations(generation_id)
network_profile_id         -- FK; redundant with runtime_generations for join speed
agent_runtime_profile_id   -- FK; redundant with runtime_generations for join speed

control_dir_path           -- per-generation control dir (Control Manifest)
control_manifest_path      -- absolute path to session.json under control_dir
control_manifest_digest    -- JCS digest the entrypoint validates against
projected_control_manifest_digest
                           -- restore/checkpoint digest after removing regenerable fields
bundle_digest              -- digest of the generated runsc bundle
runtime_config_digest      -- digest of the generated runtime config payload
spec_digest                -- digest of config.json written to spec_path
bundle_dir_path            -- per-generation runsc bundle dir
spec_path                  -- absolute path to bundle config.json
checkpoint_path            -- checkpoint payload path, if present
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

**Relationship to `network_profiles`.** `network_profiles` owns the *network* allocation lifecycle (CIDR slot, netns, veth, gateway, egress) — that table's `allocation_state` continues to be the authoritative state machine for releasing the `/30` and netns. `runtime_generation_resources` owns the *non-network* host artifacts (control/bundle/spec/checkpoint/secret/bridge/log paths) and mirrors `allocation_state` into `resource_state` so that the reaper and recovery sweeps can answer "is everything for this generation reclaimable yet?" in one query without joining three tables. Both rows are written in the same allocation transaction and both rows advance to `destroyed` in the same reclaim transaction; the partial unique index that constrains "one non-terminal generation per session" sits on `runtime_generations`, not on either resource table.

**Uniqueness.** The allocator enforces uniqueness over every non-destroyed row for `control_dir_path`, `control_manifest_path`, `bundle_dir_path`, `spec_path`, `checkpoint_path`, `bridge_dir_path`, and `log_dir_path`; `secrets_dir_path` is unique when present.

**Relationship to `runtime_generations`.** `runtime_generations` is the fencing row (status, lease_owner, generation_id identity); it does not store paths or PIDs. Implementations that want to grow new per-generation host artifacts add a column to `runtime_generation_resources`, not to `runtime_generations`, so that the fencing row stays narrow and the resource row carries everything the reaper needs to delete from disk.

**Lifecycle.** The allocator inserts the `runtime_generation_resources` row in the same transaction that inserts `runtime_generations` and `network_profiles`; the reaper removes host artifacts, sets `resource_state = destroyed`, and writes `destroyed_at` when its companion `network_profiles.allocation_state` reaches `destroyed`. Phase 7 retains destroyed rows indefinitely for audit/checkpoint forensics; only host filesystem artifacts are garbage-collected. Path uniqueness is enforced only for rows whose `resource_state != 'destroyed'`; destroyed rows keep their path values as historical data. `runsc_version` is `O_CREAT|O_EXCL`-equivalent: written once at first observed sandbox start and never overwritten, which is what the Step 9 restore-validation rule keys off of.

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

The helper owns four state-changing call sites plus heartbeat renewal. Each state-changing call site is one DB transaction containing both the turn CAS and the `runtime_generations.status` CAS. Every transaction binds all three identities: `turns.session_id = :session_id`, `runtime_generations.session_id = :session_id`, and `sessions.active_generation_id = :generation_id`. A generation from session B must never be able to claim, ack, complete, or fail a turn from session A.

Heartbeat renewal is not a state transition, but it is part of the same lease contract:

```sql
update runtime_generations
set lease_expires_at = :now + :lease_ttl,
    last_seen_at = :now
where generation_id = :generation_id
  and session_id = :session_id
  and status in ('starting', 'probing', 'active', 'idle', 'checkpointing', 'restoring')
  and lease_owner = :owner
  and lease_expires_at > :now
  and exists (
      select 1 from sessions
      where id = :session_id
        and active_generation_id = :generation_id
  );

update turns
set lease_expires_at = :now + :lease_ttl
where session_id = :session_id
  and generation_id = :generation_id
  and status in ('leased', 'running')
  and lease_owner = :owner
  and lease_expires_at > :now;

update active_model_request_contexts
set expires_at = :now + :lease_ttl,
    updated_at = :now
where session_id = :session_id
  and generation_id = :generation_id
  and turn_id in (
      select id from turns
      where session_id = :session_id
        and generation_id = :generation_id
        and status = 'running'
  );
```

The turn renewal may affect zero rows only when the generation has no leased/running turn. The context renewal applies only after `ack_started` has created a running-turn context. If a running turn exists and either its turn lease or context TTL is not renewed, heartbeat is considered failed and recovery takes over: leased-but-not-started turns may be requeued, while `ack_started_at` running turns wait through `ack_started_grace` before `unknown_after_ack_started` is written.

### 1. `claim_next_turn` — bridge picks up a queued turn

```sql
-- Idempotent replay: a host crash after the turn CAS but before grant
-- delivery must return the original grant, not no_work.
select id from turns
where session_id = :session_id
  and generation_id = :generation_id
  and claim_request_id = :request_id
  and status in ('leased', 'running')
  and lease_owner = :owner
  and lease_expires_at > :now;

-- Turn CAS: claim the lowest-sequence queued turn for this session.
-- See Turn Ledger for the MIN(sequence) ordering invariant.
update turns
set status = 'leased',
    generation_id = :generation_id,
    lease_owner = :owner,
    lease_expires_at = :now + :lease_ttl,
    claim_request_id = :request_id,
    claim_granted_at = :now,
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
  and exists (
      select 1 from runtime_generations g
      join sessions s on s.id = g.session_id
      where g.generation_id = :generation_id
        and g.session_id = :session_id
        and g.status in ('idle', 'active')
        and g.lease_owner = :owner
        and g.lease_expires_at > :now
        and s.active_generation_id = :generation_id
  )
returning id;

-- Generation CAS: idle -> active. Must reject checkpointing/restoring/etc.
update runtime_generations
set status = 'active'
where generation_id = :generation_id
  and session_id = :session_id
  and status in ('idle', 'active')   -- already-active is a no-op CAS
  and lease_owner = :owner
  and lease_expires_at > :now
  and exists (
      select 1 from sessions
      where id = :session_id
        and active_generation_id = :generation_id
  );
```

If the replay query returns a row, the helper returns the same `grant` and performs no update. If the new-claim CAS returns no row (no queued work, single-in-flight already matched, stale generation, or session/generation mismatch), the generation CAS is skipped. If the generation CAS affects zero rows after a new claim, the transaction aborts and rolls the turn claim back. The bridge's caller treats this as "no work for now" and retries on next poll. The single-in-flight predicate is what makes proxy source-IP correlation sound (see [network-and-probes.md](./network-and-probes.md#proxy-and-upstream-observability)).

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
  and session_id = :session_id
  and status = 'active'
  and lease_owner = :owner
  and lease_expires_at > :now
  and exists (
      select 1 from sessions
      where id = :session_id
        and active_generation_id = :generation_id
  );

insert into active_model_request_contexts (
    sandbox_source_ip, session_id, generation_id, turn_id,
    lease_owner, expires_at, next_request_sequence, registered_at, updated_at
) values (
    :sandbox_source_ip, :session_id, :generation_id, :turn_id,
    :owner, :now + :lease_ttl, 1, :now, :now
);
```

The second statement is a guard on the active lease, not just on the status flag. If it affects zero rows the transaction aborts: the generation was not `active` or not lease-fenced when ack_started fired, which means the cache and ledger had already drifted before this call.

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
  and session_id = :session_id
  and status = 'active'
  and lease_owner = :owner
  and lease_expires_at > :now
  and exists (
      select 1 from sessions
      where id = :session_id
        and active_generation_id = :generation_id
  )
  and not exists (
      select 1 from turns
      where generation_id = :generation_id
        and status in ('leased', 'running')
  );

delete from active_model_request_contexts
where session_id = :session_id
  and generation_id = :generation_id
  and turn_id = :turn_id;
```

The `NOT EXISTS` subquery is a defensive guard on the last live turn for this generation. Phase 7 still enforces at most one leased/running turn per generation, so the predicate should normally be empty; if a future protocol ever relaxes that invariant, the generation stays `active` and the cache remains correct. The CAS does not match `checkpointing` / `restoring` — those states cannot host a running turn by [Hard Invariants](./invariants.md#hard-invariants), so observing one here would already be a bug; the predicate's narrowness makes it self-checking.

**Active-lease predicate (applies to all four call sites except recovery).** Every non-recovery write path includes both `lease_owner = :owner` *and* `lease_expires_at > :now`. The owner predicate alone is not sufficient: an orchestrator-internal scheduler stall, an SQLite write that beats a stuck goroutine to the row, or a long-running bridge call that returns past the lease deadline can each produce a "late completion" landing after the recovery sweep has already requeued the turn under a fresh attempt. Without the expiry check, that late completion would silently overwrite the requeued attempt's row. With the expiry check it is rejected, the helper bubbles up "expired-lease completion ignored," and the durable record is the recovery sweep's outcome — which is the correct one because the orchestrator already declared the original attempt dead. The recovery sweep itself is the only path allowed to write under an expired lease, and it does so under a different CAS predicate (`lease_expires_at <= :now`) keyed on `orchestrator_owner.uuid`, never on the prior `lease_owner` string.

### 4. `failure / cancel of the generation`

Used by lifecycle failures (`allocating` / `starting` / `probing` / `checkpointing` / `restoring`) and explicit operator cancel while the lease is still active. The turn update runs only when a turn is in flight; restore/checkpoint failures usually skip it and run the generation CAS. Expired-lease recovery and `ack_started_grace` expiry use the recovery CAS described below, not this active-lease helper.

```sql
-- Fence the turn (single statement; status depends on case).
update turns
set status = 'failed',
    completed_at = :now,
    error_class = :error_class,    -- e.g. operator_cancel, lifecycle_failure
    error = :error_text
where id = :turn_id
  and status in ('leased', 'running')
  and session_id = :session_id
  and generation_id = :generation_id
  and lease_owner = :owner
  and lease_expires_at > :now;     -- active-lease predicate; lease-expiry sweep
                                   --   uses the recovery path instead (see below).

-- Generation-level failure takes the generation to failed:
update runtime_generations
set status = 'failed',
    error_class = :error_class,
    failure_reason = :reason,
    ended_at = :now,
    lease_owner = null
where generation_id = :generation_id
  and session_id = :session_id
  and status in ('allocating', 'starting', 'probing', 'active', 'idle', 'checkpointing', 'restoring')
  and lease_owner = :owner
  and lease_expires_at > :now
  and exists (
      select 1 from sessions
      where id = :session_id
        and active_generation_id = :generation_id
  );

update network_profiles
set allocation_state = 'reclaimable'
where generation_id = :generation_id
  and session_id = :session_id
  and allocation_state in ('allocating', 'ready', 'live', 'reserved_checkpointed', 'recreating');

update runtime_generation_resources
set resource_state = 'reclaimable'
where generation_id = :generation_id
  and resource_state in ('allocating', 'ready', 'live', 'reserved_checkpointed', 'recreating');

delete from active_model_request_contexts
where session_id = :session_id
  and generation_id = :generation_id;
```

Operator cancel uses the CAS above if the lease is still valid. The lease-expiry sweep is structurally different — by definition the lease is no longer active — and runs through [allocation recovery's expired-lease path](./runtime-resources.md#allocation-recovery-on-startup), whose CAS predicate is `lease_expires_at <= :now AND orchestrator_owner.uuid = :current_owner_uuid` (not the prior lease string). For `ack_started_at` running turns, that recovery path first waits until `lease_expires_at + ack_started_grace`; only then does it write `unknown_after_ack_started`, fail the generation, and allow cold fallback. Recovery is therefore the *only* code path that can move a turn or generation forward without an active lease; every other helper call is rejected by the predicate.

For per-turn failure that does not condemn the generation (e.g. a turn-level error that the agent itself raised cleanly), the helper falls back to the completion CAS in case 3 with `:terminal_status = 'failed'` and the generation stays `active`/`idle` per the same NOT EXISTS predicate. The distinction — turn-failure vs generation-failure — is decided by the caller, not by the helper.

Across all four call sites, **`lease_owner` is keyed on `<orchestrator_owner.uuid>:<role_tag>`** (see [invariants.md](./invariants.md#generation-lease)). A restarted orchestrator never matches a prior owner string, so a stale call site whose code was somehow still running across restart cannot mutate the generation; only the startup-recovery sweep can fence an expired lease.

### 5. `recovery_resume_turn` / `recovery_requeue_unstarted` / `restore_claim_checkpointed_generation`

The recovery path is the only write path allowed after a lease has expired. It is keyed on the current `orchestrator_owner.uuid`, not the stale `lease_owner` string from the previous process, and it is the only place where `lease_expires_at <= :now` is an accepted predicate. The same helper covers three cases: reconnecting a running turn during `ack_started_grace`, requeuing an expired leased turn that never reached `ack_started_at`, and reclaiming the lease for a checkpointed generation before physical restore.

```sql
-- Recovery reconnect: resume the same running turn after lease expiry.
update runtime_generations
set lease_owner = :owner,
    lease_expires_at = :now + :lease_ttl,
    last_seen_at = :now
where generation_id = :generation_id
  and session_id = :session_id
  and status = 'active'
  and lease_expires_at <= :now
  and exists (
      select 1 from sessions
      where id = :session_id
        and active_generation_id = :generation_id
  )
  and exists (
      select 1 from orchestrator_owner
      where uuid = :current_owner_uuid
  );

update turns
set lease_owner = :owner,
    lease_expires_at = :now + :lease_ttl
where id = :turn_id
  and session_id = :session_id
  and generation_id = :generation_id
  and status = 'running'
  and ack_started_at is not null
  and lease_expires_at <= :now;

update active_model_request_contexts
set lease_owner = :owner,
    expires_at = :now + :lease_ttl,
    updated_at = :now
where session_id = :session_id
  and generation_id = :generation_id
  and turn_id = :turn_id;

-- Recovery requeue: turn was leased but never reached ack_started.
update turns
set status = 'queued',
    generation_id = null,
    lease_owner = null,
    lease_expires_at = null,
    claim_request_id = null,
    claim_granted_at = null,
    started_at = null,
    ack_started_at = null,
    completed_by_generation = null,
    completed_at = null,
    error_class = null,
    error = null,
    attempt = attempt + 1
where id = :turn_id
  and session_id = :session_id
  and generation_id = :generation_id
  and status = 'leased'
  and ack_started_at is null
  and lease_expires_at <= :now
  and exists (
      select 1 from orchestrator_owner
      where uuid = :current_owner_uuid
  );

-- Physical restore reacquires the generation lease before recreating the
-- checkpointed allocation identity.
update runtime_generations
set status = 'restoring',
    lease_owner = :owner,
    lease_expires_at = :now + :lease_ttl,
    last_seen_at = :now
where generation_id = :generation_id
  and session_id = :session_id
  and status = 'checkpointed'
  and lease_owner is null
  and exists (
      select 1 from sessions
      where id = :session_id
        and active_generation_id = :generation_id
  )
  and exists (
      select 1 from orchestrator_owner
      where uuid = :current_owner_uuid
  );

update network_profiles
set allocation_state = 'recreating'
where generation_id = :generation_id
  and session_id = :session_id
  and allocation_state = 'reserved_checkpointed';

update runtime_generation_resources
set resource_state = 'recreating'
where generation_id = :generation_id
  and resource_state = 'reserved_checkpointed';
```

The requeue helper is only invoked when the turn's `retry_policy` permits another attempt; otherwise the caller uses the generation-failure path above. The requeue update clears the stale lease fields and bumps `attempt` so the next `claim_next_turn` sees a fresh queued row. The restore-acquire path keeps the same `generation_id` and only reopens the lease and allocation state for the same physical generation.

## SQLite Migration Strategy

`Store.migrate` in `orchestrator/internal/store/store.go` enables SQLite's WAL, foreign keys, and busy timeout, then runs the ordered, version-tracked migration runner defined in `orchestrator/internal/store/migrations.go`. A lab DB carried over from Phase 6 is advanced through explicit migrations that add Phase 7 columns, tables, indexes, retention helpers, and checkpoint-policy metadata; fresh installs run the same migration list and land at the same current version.

### `schema_migrations` table

The runner creates the migration tracking table before applying the first pending migration:

```sql
create table if not exists schema_migrations (
    version    integer primary key,    -- one row per applied migration
    name       text    not null,
    applied_at text    not null        -- RFC3339 UTC
);
```

The runner is wrapped in `BEGIN IMMEDIATE … COMMIT`, with `PRAGMA foreign_keys=ON` and the existing `MaxOpenConns(1)` single-writer guarantee. Each migration body runs in its own transaction; on partial failure the transaction is rolled back and `schema_migrations` is unchanged so a re-run resumes from the same version.

### Required migrations

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

v3  phase7_turn_event_and_proxy_context
      CREATE TABLE: turns, events, active_model_request_contexts.
      All three tables come up empty for legacy DBs; events.event_id is the
      orchestrator-global monotonic primary key (Durable Event Log).
      active_model_request_contexts is empty and is repopulated only by
      live ack_started transactions.

v4  phase7_session_columns
      ALTER TABLE sessions ADD COLUMN active_generation_id TEXT
        REFERENCES runtime_generations(generation_id);
      ALTER TABLE sessions ADD COLUMN agent_home_path TEXT;
      ALTER TABLE sessions ADD COLUMN failure_reason TEXT;
      ALTER TABLE sessions ADD COLUMN error_class TEXT;
      ALTER TABLE sessions ADD COLUMN auto_checkpoint_enabled INTEGER
        NOT NULL DEFAULT 0 CHECK(auto_checkpoint_enabled IN (0,1));
      Keep the existing `workspace` column as the canonical workspace
      path; do not add a parallel `workspace_path` column. Backfill
      agent_home_path from the resolved agent-homes root and session id.
      active_generation_id starts NULL on legacy rows and is patched by
      the allocator / 7a ledger shim via the standard CAS predicate.
      auto_checkpoint_enabled starts disabled for migrated sessions and
      is controlled by the current checkpoint policy for new sessions.

v5  phase7_indexes
      CREATE INDEX statements for the per-table indexes referenced
      throughout this document, plus the partial unique index
      `runtime_generations_one_nonterminal_per_session`, the partial
      unique index on `(session_id, claim_request_id)` for non-NULL
      claim_request_id values, and lookup indexes for
      active_model_request_contexts. The runtime_generations partial
      index can be created against the empty table. The network-profile
      partial uniques are explicit one-column indexes, one per field,
      because the allocator must not accidentally "optimize" them into
      a composite constraint:

      create unique index network_profiles_netns_name_non_destroyed_uq
        on network_profiles (netns_name)
        where allocation_state != 'destroyed';

      create unique index network_profiles_netns_path_non_destroyed_uq
        on network_profiles (netns_path)
        where allocation_state != 'destroyed';

      create unique index network_profiles_host_veth_non_destroyed_uq
        on network_profiles (host_veth)
        where allocation_state != 'destroyed';

      create unique index network_profiles_sandbox_veth_non_destroyed_uq
        on network_profiles (sandbox_veth)
        where allocation_state != 'destroyed';

      create unique index network_profiles_host_gateway_ip_non_destroyed_uq
        on network_profiles (host_gateway_ip)
        where allocation_state != 'destroyed';

      create unique index network_profiles_host_side_cidr_non_destroyed_uq
        on network_profiles (host_side_cidr)
        where allocation_state != 'destroyed';

      create unique index network_profiles_sandbox_ip_cidr_non_destroyed_uq
        on network_profiles (sandbox_ip_cidr)
        where allocation_state != 'destroyed';

      v6 does not backfill legacy sessions into runtime_generations;
      the first rows appear only when the post-upgrade allocator
      creates new generations.

v6  phase7_legacy_session_backfill
      Only legacy `running_*` / `checkpoint*` rows are fenced here;
      `created` rows remain `created`.
      For every legacy session row whose pre-Phase-7 status implies a
      live sandbox or checkpoint (`running_active`, `running_idle`,
      `checkpointing`, `checkpointed`) and `ended_at IS NULL`:
        - Mark status = 'failed', error_class =
          'legacy_pre_phase7_no_generation', failure_reason =
          'legacy_pre_phase7_no_generation', and ended_at = now.
        - DO NOT synthesize a runtime_generations row. The legacy
          stdin/PTY container that backed the row is gone after
          orchestrator restart and there is no fenced generation_id to
          attach. The frontend already knows how to render `failed`.
      Rows still in `created` are left untouched; they had no live
      sandbox to fence and will allocate a fresh generation on the
      first turn.
      For sessions that were `checkpointing` or `checkpointed` in the
      pre-Phase-7 schema:
        - Their existing checkpoint_path / restore_id fields stay on
          the row for forensic value, but the row is moved to
          `failed` with error_class / failure_reason
          'legacy_checkpoint_unrestorable'.
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

v7  phase7_proxy_event_uniqueness
      Add partial UNIQUE indexes that make proxy request start and
      terminal events idempotent per proxy_request_id:
      `events_proxy_started_request_uq` for `proxy.request.started`
      and `events_proxy_finished_request_uq` for
      `proxy.request.completed` / `proxy.request.failed`.

v8  phase7_event_retention_index
      Normalize existing `events.created_at` values to the event-log
      timestamp format and add `events_created_at_idx` so retention
      sweeps can seek by creation time without scanning the event log.

v9  phase7_checkpoint_policy
      Add the Step 10 automatic-checkpoint policy and artifact digest
      columns to upgraded databases:
        - sessions.auto_checkpoint_enabled
        - runtime_generations.auto_checkpoint_enabled
        - runtime_generation_resources.projected_control_manifest_digest
        - runtime_generation_resources.bundle_digest
        - runtime_generation_resources.runtime_config_digest
        - runtime_generation_resources.spec_digest
      The migration checks for each column before issuing ALTER TABLE,
      so databases that already picked up sessions.auto_checkpoint_enabled
      from v4 or fresh schema creation remain idempotent.
```

`v6` is intentionally aggressive about not inventing fenced generations. Manufacturing a fake `runtime_generations` row to satisfy `sessions.active_generation_id` would create a row that no allocator owns, no reaper will reclaim by name (it would fail the `harness-gen-<id>` ownership filter), and no resource allocation row backs. It is structurally safer to declare the legacy session terminal and let the user resume via Claude's conversation UUID than to forge generation rows.

### Migration tests

The `store` package ships a migration test suite that exercises the runner against real legacy fixtures, not just an empty in-memory DB:

```text
- test/migration_fixtures/v1_phase6_clean.sqlite
    A snapshot of a Phase 6 lab DB taken from a running instance.
    Test: open under the Phase 7 Store, assert all migrations apply to
    completion, schema_migrations ends at the current version (v9),
    every Phase 7 table, index, session column, generation policy
    column, and resource digest column exists, every legacy session row
    has `agent_home_path` backfilled, and every row is either
    still-active (no eligible candidates in this fixture) or `failed`
    with the documented error_class / failure_reason; any legacy
    `created` row stays `created` and keeps active_generation_id set
    to NULL.

- test/migration_fixtures/v1_phase6_with_running.sqlite
    A snapshot with mid-flight running sessions and one
    `checkpointed` session. Test: v6 backfill moves the running
    session to `failed (legacy_pre_phase7_no_generation)` and the
    checkpointed session to `failed (legacy_checkpoint_unrestorable)`,
    writes both `error_class` and `failure_reason`, leaves messages /
    artifacts / claude_session_uuid intact for forensic value, keeps any
    legacy `created` row untouched, and never creates a phantom
    runtime_generations row.

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
