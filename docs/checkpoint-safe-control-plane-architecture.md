# Checkpoint-Safe Control Plane Architecture

> Date: 2026-05-22  
> Status: Phase 7 target architecture  
> Scope: make sessions addable, checkpointable, recoverable, reconnectable, network-correct, and multi-turn reliable.

## Executive Summary

The current system already proves the core product path: browser session, Go orchestrator, gVisor sandbox, Claude Code, stream parsing, artifacts, and same-origin SSE. The weak point is that a session is still too tightly coupled to a live container process and its attached stdin/stdout pipes.

The optimal architecture is to move session correctness into a durable host-side control plane, and treat each gVisor container as a replaceable runtime generation.

Phase 7 is not a patch on top of `phase2-template`. It must remove generation-specific mutable state from the template and make the control plane allocate, record, validate, and reclaim that state.

The target rule is:

```text
The session is the durable DB state, turn log, event log, network profile,
Claude conversation identity, and runtime generation lease.

The container is only the current executor for that session.
```

This makes `runsc checkpoint/restore` a performance and resource optimization, not the only mechanism that keeps a conversation correct. Claude resume, persisted messages, durable turn state, and explicit network profiles provide correctness. Checkpoint/restore provides fast restart when it works.

## Starting Point

Current baseline behavior, network values, and `config/harness.yaml` contents live in [architecture.md](./architecture.md) and [current-status.md](./current-status.md); this document does not restate them. Two upstream conclusions carry forward:

- [gvisor-decision.md](./gvisor-decision.md): KVM is unavailable on this host, so gVisor `runsc` with `systrap` is the selected runtime.
- [runsc-warm-sentry-research.md](./runsc-warm-sentry-research.md): `runsc release-20260511.0` has no warm sentry; low-latency startup must come from orchestrator-level pooling or normal checkpoint tuning.

The architecture gap this document addresses: automatic idle checkpointing is unsafe today because `runsc restore` can restore the sandbox while the attached stdin turn channel is no longer reliably reconnectable. Legacy shared resources to sunset are enumerated in Hard Invariants and are not compatibility constraints for the target.

## Concurrency And Storage Model

Phase 7 explicitly assumes a single orchestrator process per host. The current SQLite store (`MaxOpenConns(1)`, WAL, `busy_timeout=5000`) is a single-writer transactional state machine. The lease + CAS rules later in this document do not relax that assumption; they exist to keep one orchestrator's concurrent goroutines (HTTP handlers, idle monitor, reaper, bridge listener) from racing on the same generation, allocation, or turn, and to make crash recovery deterministic.

CAS predicates do not protect host-level resource creation (netns/veth/control dir/bundle dir). "Allocate row in DB → create host resources" is sequenced by the orchestrator process, not the SQL transaction; two orchestrators against the same `/var/lib/harness` and same SQLite file would race on `ip netns add` and veth name allocation before any CAS could fire. The single-orchestrator assumption is therefore enforced at process startup, not by the schema:

```text
1. On startup the orchestrator opens an exclusive flock on
   <run_dir>/orchestrator.pid (default <run_dir> = /var/lib/harness/run).
   The PID file is created if absent; flock is held for the process lifetime
   and released by the kernel on exit/crash. A second process attempting
   the flock fails fast with a typed error and refuses to open the SQLite
   file.

2. After the flock is held, the orchestrator writes its identity into a
   singleton meta row:
     orchestrator_owner.uuid          (random per process start)
     orchestrator_owner.boot_id       (/proc/sys/kernel/random/boot_id)
     orchestrator_owner.host_run_dir  (the run_dir whose flock is held)
     orchestrator_owner.acquired_at
     orchestrator_owner.heartbeat_at  (refreshed every 5 s)
   The row is upserted under a fixed primary key so it is unique by
   construction.

3. Every recovery sweep, allocator commit, and reaper pass reads
   orchestrator_owner.uuid and asserts it equals the in-process value. A
   mismatch (which can only happen if the flock was bypassed by an
   operator removing the PID file or pointing a second orchestrator at a
   shared NFS-style mount where flock semantics are unreliable) aborts
   the sweep and exits the process.
```

The flock is the primary defense; the meta row is for diagnostics and for catching the case where flock is silently broken (network filesystems, container bind mounts that strip locks). Multi-orchestrator HA is deferred — see Out Of Scope. The architectural rule that every critical transition is expressed as one SQL statement whose `WHERE` clause encodes the precondition holds independently of the deployment substrate; whether that statement runs against SQLite or a future Postgres deployment is a deployment detail, and the same CAS predicates carry over unchanged.

## Phase 7 Scope

### Hard Invariants

These rules apply to every live and checkpointed generation. They are the contract the rest of the document expands on:

```text
No two live runtime generations may share a network namespace.
No live generation's network namespace may be reconfigured after runsc attaches.
No two live runtime generations may share a writable control manifest path.
No two live runtime generations may share a generated runtime spec path.
No non-destroyed resource allocation may be reused by another generation.
No control manifest at rest contains plaintext upstream credentials; secrets are
  referenced by secret_id + secret_version and read from a per-generation mounted
  file, never from ambient host Claude config or implicit environment variables.
A non-destroyed allocation belongs to exactly one generation. The single
  documented exception is **physical restore of the same `generation_id`** — the
  reserved-checkpointed -> recreating -> ready transition (Resource Allocation
  Lifecycle) keeps the original `generation_id` and therefore is not "another
  generation." Any other path that wants the resources of a non-destroyed
  allocation must wait for the allocation to reach `destroyed` and allocate
  fresh. Changing the model, agent runtime profile, egress policy, or any other
  field that requires a new generation always allocates a new network profile.
Every generation has isolated network resources, isolated control resources,
  and generation fencing.
Every turn claim and completion update is guarded by the active lease and
  generation fence. (Phase 7b — first effective at Step 6, when turn execution
  moves onto bridge claim/ack. See "Phase 7a vs 7b applicability" below.)
```

#### Phase 7a vs 7b applicability

The invariants above split cleanly along the 7a/7b boundary:

```text
Phase 7a (Steps 1–4) — must hold from the first 7a deploy:
  - Per-generation netns / veth / IP / gateway / CIDR.
  - Per-generation control dir + control manifest path + runtime spec path.
  - Per-generation bundle dir.
  - No reuse of non-destroyed allocation identity.
  - No plaintext upstream credentials in the on-disk manifest; secrets read
    from ${SECRET_DIR}/<secret_id>.
  - Single-orchestrator flock + orchestrator_owner heartbeat (Step 1).
  - Schema present for the 7b invariants (turns, leases, events tables and
    their indexes), but the helper that performs the turn-state CAS is
    *not yet on the live execution path*.

Phase 7b (Steps 5–10) — first effective when bridge claim/ack lands:
  - Every turn claim and completion update is guarded by the active lease
    and generation fence (Step 6).
  - Stale-generation events / acks are rejected by CAS predicates.
  - Cold fallback retry eligibility is enforced by the same CAS as
    restart recovery (Step 7).
  - SSE replay against the durable event log (Step 8).
  - Checkpoint-safe restore (Step 9), automatic checkpoint policy (Step 10).
```

7a deliberately keeps the existing stdin/PTY turn path running on top of the new per-generation resources. The turn ledger and `runtime_generations.status` rows are written by the existing turn-execution code in 7a as a *thin record-keeping shim* (turn row inserted at submit, marked `completed`/`failed` when the stream parser observes turn completion); the ledger is correct enough that 7b's bridge can take over without a schema migration, but the `claim_next_turn` / `ack_started` / `ack_turn_completed` CAS helpers are not on the hot path until Step 6. The two consequences worth calling out:

- A 7a deploy that crashes mid-turn cannot recover the in-flight turn from the ledger — the existing stdin path owns it. This is unchanged from the pre-Phase-7 behavior; restart recovery on the 7a ledger only reconciles `queued` rows and timestamps. 7b is what makes turn recovery durable.
- The single-helper contract (Single-Helper Contract) is a 7b commitment. In 7a the only writers to `turns` are session-create (insert `queued`) and the existing turn-completion path (insert terminal status); these are also routed through the same helper module to keep cache/ledger agreement on `runtime_generations.status`, but the four-call-site CAS protocol (claim, ack_started, completion, generation-failure) only runs once the bridge is the executor.

Step 1's acceptance therefore lists the schema and the helper module's *interface*; Step 6's acceptance is the first time all four CAS sites are exercised against a live executor.

The lab today violates these rules with three legacy patterns that must be gone before checkpoint/restore is treated as a normal path: the fixed shared netns `/run/netns/phase1-demo`, the shared writable control manifest `/var/lib/harness/control/phase2-template/session.json`, and the shared static runtime spec under `bundle/out/phase2-template-bundle/config.json` reused as live mutable state. Plaintext `anthropic_api_key` / `anthropic_auth_token` in the manifest is tolerated only because the lab proxy key is the literal `123` with no upstream value; Phase 7 ships the `secret_id`/`secret_version` indirection, Phase 8 wires it to a real secret store.

### Out Of Scope (Deferred To Phase 8)

Phase 7 establishes fencing and persistence; the following must not gate Phase 7 acceptance:

- multi-orchestrator HA / shared-DB clustering / leader election.
- real upstream credentials with KMS-backed storage and rotation. Phase 7 ships the `secret_id`/`secret_version` indirection and per-generation mount; Phase 8 wires the actual store.
- per-tenant egress policy at the host firewall. Phase 7 ships per-generation netns and a static allowed-egress list; Phase 8 adds tenant-level policy and quotas.
- authentication and authorization on the orchestrator HTTP/SSE surface. Phase 7 assumes a trusted operator.

## Target Architecture

```text
Browser
  |
  | HTTP + SSE with last_event_id
  v
Next.js frontend
  |
  | same-origin API proxy
  v
Control Plane
  |
  | session store
  | turn ledger
  | durable event log
  | runtime generation table
  | runtime resource allocator
  | network profile table
  | runtime lease manager
  | artifact metadata
  v
Runtime Manager
  |
  | runsc driver
  | checkpoint manager
  | restore/cold-start fallback
  | sandbox pool, optional
  | network probe
  v
runsc generation N
  |
  | Agent Bridge
  |   - reconnectable control client
  |   - per-turn ack/completion
  |   - stdout/stderr/event forwarding
  |
  | Claude Code / shell shim
  | workspace mount
  | agent home mount
  v
Durable event log
```

The main difference is the `Agent Bridge`. Instead of treating container stdin as the session's source of truth, the sandbox starts a small bridge process that talks to the host control plane using a reconnectable protocol. The bridge can be implemented over a Unix socket bind mount, a local HTTP long-poll endpoint reachable through the host gateway, or a simple file-backed queue with atomic claim/ack semantics. The exact transport can vary, but the protocol semantics must be durable.

## Control Plane Responsibilities

### Session Store

Stores the durable identity of the conversation:

```text
session_id
user_id
agent
status
claude_session_uuid
workspace_path
agent_home_path
created_at
updated_at
last_activity_at
expires_at
```

The session row must not imply that a specific process is alive. It only says whether the session is eligible to accept input and what recovery policy applies. `expires_at` is the absolute deadline beyond which the session moves to `destroyed` regardless of activity; it is set on session create as `created_at + harness.session_ttl` (the lab default surfaces as `HARNESS_SESSION_TTL=2h`) and refreshed on explicit user extension, not on every turn. A session whose `expires_at` is in the past is reaped on the next sweep alongside its allocations.

### Turn Ledger

Stores every user turn before execution:

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
No turn is considered complete until a durable completion event is recorded.
```

**Claim ordering invariant.** `claim_next_turn` always picks `MIN(sequence)` over `WHERE session_id = :session_id AND status = 'queued' AND lease_owner IS NULL`. This rule is independent of which generation is claiming and survives cold fallback: when a previous generation N fails and the startup sweep requeues its expired-leased-without-`ack_started_at` turn (attempt+1), and the user injects a fresh turn between N's failure and N+1's first claim, both rows land in `queued` and the sequence column resolves which one N+1 picks first. The session never has two concurrently-claimed turns from different generations, because `runtime_generations` has at most one row in `(active, idle)` at a time per session and the helper's generation CAS rejects claims against transient-state generations (Single-Helper Contract, case 1).

This "at most one non-terminal generation per session" property is enforced at the schema level — it is a correctness premise for claim ordering and lease fencing, not an emergent property of orchestrator code paths. The migration ships a partial unique index:

```sql
create unique index runtime_generations_one_nonterminal_per_session
  on runtime_generations (session_id)
  where status not in ('failed', 'destroyed');
```

`failed` and `destroyed` are the only terminal generation statuses; every other status (`allocating`, `starting`, `probing`, `active`, `idle`, `checkpointing`, `checkpointed`, `restoring`) is non-terminal and therefore subject to the index. Cold fallback inserting N+1 must therefore happen in the same transaction that moves N out of any non-terminal status — concretely: N is CAS-updated to `failed` and N+1 is inserted in one DB transaction, which makes the index a fail-fast safety net for any code path that tries to start a second live generation while the first is still alive. The session row also carries `sessions.active_generation_id`; updates to it are CAS predicates (`update sessions set active_generation_id = :new where active_generation_id = :old_or_null`) so a buggy concurrent allocator collides on the row, not just on the index. Both mechanisms are required: the index protects the invariant from arbitrary writers; the CAS gives orchestrator paths a deterministic conflict signal without relying on the index error class.

The Phase 7a migration ships both the partial unique index and `sessions.active_generation_id` together; tests against the migration assert (a) inserting a second non-terminal generation row for the same session fails with a uniqueness error, and (b) cold fallback's "fail N + insert N+1" transaction succeeds because the CAS step on N runs first.

Restart recovery rules — these also govern cold fallback retry eligibility (cold fallback may retry only what restart recovery would requeue):

```text
queued:
  keep queued

leased, ack_started_at is null, lease expired:
  requeue and increment attempt according to retry_policy

running, ack_started_at is set:
  do not auto-retry; wait for bridge reconnect or transition to
  unknown_after_ack_started for user-visible resolution

completed | failed | canceled:
  never auto-retry
```

### User-Visible Recovery For unknown_after_ack_started

A generation crash or permanent bridge unreachability while a turn is `running` with `ack_started_at` set is the one case the orchestrator cannot resolve unilaterally — the prompt may already be billed and partially answered. The default action is **await reconnect** for a bounded grace window (`ack_started_grace`, default **90 s = bridge heartbeat × 3**, configurable via `harness.bridge.ack_started_grace`). A reconnect during the grace window is not a special path: the bridge runs the standard `hello` → `hello_ack` (which returns `leased_turn_id` and `last_output_sequence_by_turn`) → `resume_turn` flow, and the turn continues. The grace window is a host-side timer that fences the generation if it expires; the protocol is unchanged.

`ack_started_grace` and `lease_ttl` (default 60 s) are deliberately distinct timers and **must not collapse to the same value**. `lease_ttl` decides "is this generation's lease expired and recoverable by the startup sweep"; `ack_started_grace` decides "should the user keep waiting for the bridge to come back, or surface `unknown_after_ack_started`." The invariant is `ack_started_grace > lease_ttl`: lease expiry without reconnect must happen first, so the generation is fenced and N+1 can be cold-started inside the user-facing grace window if needed. Setting them equal collapses "lease still valid, awaiting reconnect" with "lease expired, declare failure," which loses the grace-window UX. Heartbeat cadence (default 30 s = `lease_ttl / 2`) is the third timer and is the unit `ack_started_grace` is expressed in multiples of, so any retune of one of the three should be evaluated against all three together.

If the grace window expires without reconnect, the turn becomes `failed (error_class = unknown_after_ack_started)` and the UI offers two actions: **abandon** (keep partial output as labeled-partial audit data) or **resubmit** (a brand-new `turn_id`; original remains failed for audit; no automatic replay). The session is never blocked — a new turn always provisions a fresh generation. `unknown_after_ack_started` turns are audit records, not queue heads.

Resubmit closes the grace window. The first action — abandon, resubmit, or grace expiry — fences the original generation with `error_class = unknown_after_ack_started` in the same transaction, so a late bridge reconnect is rejected as stale and N+1 is the only generation servicing the session. This preserves the single-active-generation invariant when the user does not wait out the grace window.

### Durable Event Log

Stores runtime and agent events with monotonic event IDs:

```text
event_id
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

`event_id` is allocated only by the host event store. Sandbox bridge messages must not supply a global event ID. Bridge output messages use a per-turn sequence, and the host deduplicates bridge output by:

```text
(turn_id, generation_id, output_sequence)
```

Proxy metrics are stored as typed event payloads, usually `proxy.request.started`, `proxy.request.completed`, or `proxy.request.failed`. The correlation fields `proxy_request_id`, `turn_id`, and `generation_id` are first-class columns; latency, retry, timeout, and upstream details can remain in `payload` or be promoted to generated/query columns if needed for dashboards.

SSE becomes a view over this log. The frontend reconnects with `last_event_id`, and the server replays missed events. The existing polling fallback remains useful, but it is no longer the only recovery path for missed live output.

**Global SSE stream with optional session filter (current model, retained).** The orchestrator already exposes a single global SSE endpoint at `/api/events/stream` that the frontend opens once per browser session and demultiplexes client-side; the endpoint accepts an optional `?session_id=` filter for views that only want one session's frames (`orchestrator/internal/server/server.go:523`, `frontend/components/harness-provider.tsx:455`). Phase 7 keeps this shape rather than switching to a per-session endpoint, because the workbench has a sidebar that needs to show status changes for sessions other than the currently-selected one (created/idle/failed transitions, expiry, completion) and a per-session SSE would force either N parallel `EventSource`s or a cumbersome reconnect on every selection change. The Phase 7b deliverable is therefore:

```text
GET /api/events/stream
  (open new global stream from the head of the log;
   server replays nothing — first frame is the next event after connect)

GET /api/events/stream
Last-Event-ID: 482917
  (replay events with id > 482917 in monotonic order, then continue live)

GET /api/events/stream?session_id={id}
Last-Event-ID: 482917
  (same replay semantics, but only emit frames whose session_id matches)

GET /api/events/stream?last_event_id=482917
  (header-stripped fallback, same semantics as Last-Event-ID)
```

For this to work, **`event_id` must be globally monotonic per orchestrator process**, not per session. The host event store assigns `event_id` from a single sequence; the SQL is `INSERT INTO events (...) RETURNING event_id` against an `INTEGER PRIMARY KEY AUTOINCREMENT` column under the orchestrator's single-writer SQLite. The cursor a client sends in `Last-Event-ID` is therefore one integer that is meaningful across every session the client has open or will open; reconnecting with `Last-Event-ID: 482917` on the global stream, then later switching the filter to `?session_id=X`, both replay the correct slice of the same monotonic sequence.

This is also what makes "non-selected session" state stay correct on reconnect: since the global stream replays every session's lifecycle frames after the cursor, the sidebar's session list re-converges from SSE alone after a transient disconnect; the periodic polling of `/api/sessions` (already wired in `harness-provider`) continues to be the long-window backstop for clients whose `Last-Event-ID` falls outside retention.

`event_id` is monotonic globally; `(session_id, sequence)` (or `(turn_id, generation_id, output_sequence)` for output) remains the per-session/per-turn ordering keys used by replay consumers and dedup paths.

**Phase scoping for the wire protocol below.** Phase 7a lands the `events` table and indexes only (Step 1) and continues to use the existing `data:`-only SSE writer in `orchestrator/internal/server/server.go` and the existing `EventSource(url)` consumer in `frontend/components/harness-provider.tsx`. The typed `id:`/`event:` wire format, `Last-Event-ID` handling, the `?last_event_id=` query-string fallback, and the `replay_gap` synthetic event all land at **Step 8 (Phase 7b)** together with the replay support and retention enforcement on the existing global `/api/events/stream` endpoint, since they require the bridge to be the executor and the host event store to be the source of truth for `event_id`. The contract below is the Step 8 deliverable; it appears in the architecture document up-front because the schema in Step 1 must allocate `event_id` as the cursor (host-assigned, monotonic globally per orchestrator) so that the Step 8 wire format can use it without a second migration.

**SSE wire protocol (Step 8 contract).** Every frame the server emits carries an `id:` line whose value is the host-assigned `event_id`:

```text
id: 482917
event: emit_output
data: {"session_id":"…","turn_id":"…","output_sequence":17,"payload":{…}}

id: 482918
event: ack_turn_completed
data: {"session_id":"…","turn_id":"…","status":"completed"}
```

Every `data:` JSON payload carries `session_id` so client-side demultiplexing on the global stream is unambiguous. The `event:` line carries the `events.type` value so the browser's `EventSource.addEventListener('emit_output', …)` form works for typed handlers. Frames without a meaningful event type still carry `id:`; clients without typed handlers fall through to the default `message` listener.

**Resume on reconnect.** The browser's native `EventSource` automatically sends `Last-Event-ID: <event_id>` on reconnect, so the server treats `Last-Event-ID` as the authoritative cursor against the global sequence. Some intermediaries (corporate proxies, the user's edge proxy fronting the orchestrator) strip the header; for those the client also accepts `?last_event_id=<event_id>` as a query-string fallback, used by the `harness-provider` frontend code path that is not the raw `EventSource`. When both header and query are present, header wins. The `?session_id=` filter is orthogonal to the cursor — it narrows which frames the client receives, not which frames count toward the cursor.

```text
GET /api/events/stream
  (open new global stream; first frame is the next event after connect)

GET /api/events/stream
Last-Event-ID: 482917
  (replay events with id > 482917 in monotonic order, then continue live)

GET /api/events/stream?session_id={id}
Last-Event-ID: 482917
  (replay only this session's events with id > 482917, then continue live)

GET /api/events/stream?last_event_id=482917
  (header-stripped fallback, same semantics)
```

The cursor is global per orchestrator: one integer survives across session selection changes and across reconnects without coordination.

**Retention gap.** The event log has finite retention (configured per `harness.events.retention`, default 24h or N rows whichever first). If a client resumes with a `last_event_id` older than the oldest retained row, the server cannot replay losslessly. The defined response is:

```text
id: <oldest_retained_event_id - 1>
event: replay_gap
data: {"requested_last_event_id": 482917, "oldest_available": 600000,
       "session_id_filter": "…",
       "reason": "retention_window_exceeded"}
```

`session_id_filter` echoes the active `?session_id=` filter (or null if no filter is in effect). Then the server resumes from the oldest retained event matching the filter. The frontend treats `replay_gap` as a directive to drop its in-memory hub state and refetch via `/api/sessions` (and per-session `/api/sessions/{id}` for the currently-selected session) and the polling endpoint, then re-attach the SSE stream from the gap event's id forward. This is also the contract for the rare case where a client's first event is older than retention because of an unusually long-lived idle session — the gap event still fires.

**Frontend implementation note (Step 8).** `frontend/components/harness-provider.tsx` currently constructs `new EventSource(buildEventsStreamUrl())` without retaining the last seen id and the orchestrator handler in `orchestrator/internal/server/server.go` writes `data:` frames only. Step 8 updates both: the server's SSE writer emits `id:` and `event:` lines (and a `replay_gap` synthetic event when needed), and the provider tracks the last seen id (a single integer in the global cursor space) to populate the query-string fallback when the browser does not send `Last-Event-ID` on the wire (e.g., across a navigation that recreates the EventSource). Tests assert (a) reconnect after a server restart resumes from `Last-Event-ID + 1` with no duplicates and no gaps across all sessions in the stream, (b) reconnect with a stale id past retention triggers exactly one `replay_gap` event followed by live tail, (c) typed listeners on the client receive `emit_output` frames as that event type, not as the default `message` channel, and (d) lifecycle frames for non-selected sessions arrive on the global stream and the sidebar's session list re-converges from those frames after a transient disconnect.

Event durability is a hard invariant, but the transaction boundary is per message kind, not per turn:

- **Lifecycle messages** (`ack_turn_started`, `ack_turn_completed`, generation status changes, failure marks) are appended to the event log in the same transaction as the turn-state CAS, before any in-memory hub publish. There is no race where a UI sees a turn complete that the durable ledger does not.
- **`emit_output` messages** carry no turn-state transition. They are appended to the event log in their own transaction. Implementations may batch a bounded number of consecutive `emit_output` messages from one bridge call into one transaction — bound it by row count or wall time, not by turn — to keep SQLite single-writer throughput viable for Claude stream-json's hundreds of partial deltas per turn. Batching is bounded; a turn's lifecycle ack always commits in its own transaction.

The host-assigned `event_id` is monotonic globally per orchestrator across both kinds.

### Runtime Generation Table

Tracks each executor instance:

```text
generation_id
session_id
runsc_container_id
status = allocating | starting | probing | active | idle | checkpointing | checkpointed | restoring | failed | destroyed
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

The generation ID is essential. It prevents an old restored container from writing events into a newer session execution. Every event and turn ack must carry the generation ID. `runsc_platform` and `runsc_version` are recorded on the generation row and must equal the binary that started it; the same fields are copied into checkpoint metadata to make exact-match restore validation cheap.

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

### Network Profile

Network config must be explicit and persisted, not inferred from ambient host settings. The schema separates **network** (the host-resource shape: netns, veth, egress, gateway) from **agent runtime** (model, output format, traffic shaping policy). Both are durable; both are fenced by generation; but they have independent lifecycles, and a change to one should not force re-allocation of the other.

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
allocation_state = allocating | ready | live | reserved_checkpointed | recreating | reclaimable | destroyed
created_at
destroyed_at
```

Agent runtime profile is a separate record and a separate `runtime_generations` foreign key:

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

Each `runtime_generations` row references exactly one `network_profile_id` and one `agent_runtime_profile_id`. **Both references are immutable for the lifetime of the generation row** — they are written at allocation time and never mutated. Any change that requires a new generation also requires a new `network_profile` row drawn from a fresh allocation; the predecessor's allocation moves through the standard `live -> reclaimable -> destroyed` path and its identity is not reused until the row is `destroyed`. This holds even when only the agent runtime profile is changing (model swap, `disable_nonessential_traffic` flip): the new generation gets a freshly-allocated `/30`, netns, veth pair, and control/bundle dirs. The single exception that reuses host resources is **physical restore of the same `generation_id`** (Hard Invariants and Resource Allocation Lifecycle): the same generation row's allocation transitions `reserved_checkpointed -> recreating -> ready -> live` while keeping its `network_profile_id` and `agent_runtime_profile_id` bindings; it is not "another generation" and therefore not subject to the no-reuse rule. `manifest_anthropic_base_url` is templated from `host_gateway_ip` at manifest emission — the agent inside the sandbox sees only the gateway IP, never `0.0.0.0`.

#### Runtime Generation Resources Table

`network_profiles` and `agent_runtime_profiles` describe *what was allocated* (CIDR, netns name, veth names, model, secret refs) — i.e., the contract a future generation must reproduce on restore. They do not capture *the host filesystem and process artifacts that exist only while the allocation is live* (per-generation control dir, bundle dir, `runsc` `config.json` path, sandbox PID, log paths, lock paths). Phase 7a writes those into a single dedicated row keyed by `generation_id`:

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

bridge_socket_path         -- file-backed bridge transport (Agent Bridge Protocol)
log_dir_path               -- per-generation log dir

runsc_pid                  -- sandbox PID at last observed start; nullable
runsc_version              -- exact version string at start; immutable after first write

resource_state             -- allocating | ready | live | recreating |
                             reclaimable | destroyed
                             (mirrors network_profile.allocation_state for fast lookup;
                              the network profile remains the source of truth for
                              the network-only subset of state)
created_at
destroyed_at
```

**Relationship to `network_profiles`.** `network_profiles` owns the *network* allocation lifecycle (CIDR slot, netns, veth, gateway, egress) — that table's `allocation_state` continues to be the authoritative state machine for releasing the `/30` and netns. `runtime_generation_resources` owns the *non-network* host artifacts (control/bundle/log dirs, secret dir, bridge socket, runsc bundle paths) and mirrors `allocation_state` into `resource_state` so that the reaper and recovery sweeps can answer "is everything for this generation reclaimable yet?" in one query without joining three tables. Both rows are written in the same allocation transaction and both rows are deleted (or moved to `destroyed`) in the same reclaim transaction; the partial unique index that constrains "one non-terminal generation per session" sits on `runtime_generations`, not on either resource table, so resource-row uniqueness is enforced by the FK alone.

**Relationship to `runtime_generations`.** `runtime_generations` is the fencing row (status, lease_owner, generation_id identity); it does not store paths or PIDs. Implementations that want to grow new per-generation host artifacts (e.g. a checkpoint payload directory at Step 9) add a column to `runtime_generation_resources`, not to `runtime_generations`, so that the fencing row stays narrow and the resource row carries everything the reaper needs to delete from disk.

**Why a separate row, not extra columns on `runtime_generations` or on `network_profiles`.** Adding these fields to `runtime_generations` would mix fencing identity with disk-path bookkeeping and make the partial unique index harder to reason about. Adding them to `network_profiles` would conflate "network shape contract" with "this-allocation host artifacts," and would make the shell-agent case (no secrets dir, no `secrets_dir_path`) misshape the network profile. A separate resources table also lets the Step 9 checkpoint code add a `checkpoint_payload_path` column without modifying network or agent profiles.

**Lifecycle.** The allocator inserts the `runtime_generation_resources` row in the same transaction that inserts `runtime_generations` and `network_profiles`; the reaper deletes the row when its companion `network_profiles.allocation_state` reaches `destroyed`. The row never moves between generations (no FK churn), and `runsc_version` is `O_CREAT|O_EXCL`-equivalent: written once at first observed sandbox start and never overwritten, which is what the Step 9 restore-validation rule keys off of.

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

The implication for the per-generation control dir is that `hardlink` from `<host_secrets_root>/<secret_id>/<version>` into `<control_dir>/secrets/` is **safe under the immutability rule** and is the preferred materialization. A hardlink shares inode with the source; since the source is immutable, every generation that hardlinks it observes identical bytes for the lifetime of that version. `copy` is the cross-filesystem fallback (when `<host_secrets_root>` and `<control_dir>` are on different mounts); copy preserves the same byte-for-byte invariant by construction. **Bind-mount-of-the-version-file is explicitly rejected** as a third option: a future operator who issues `mount --bind` over the per-generation file from a fresh source would silently change the bytes a running generation sees without changing `secret_version`, breaking the digest invariant.

For this group bit to actually grant read in the sandbox, the agent process must hold `harness-secret-readers` either as its primary GID or as a supplementary group at `execve` time. The current `harness-agent-entrypoint` invocation `setpriv --reuid 65534 --regid 65534 --clear-groups …` strips supplementary groups and would fail the read. The required shape is one of the following, and Phase 7a treats this as a hard contract on the entrypoint:

```sh
# Preferred: explicit supplementary group list, primary GID stays 65534.
setpriv --reuid "$AGENT_UID" --regid "$AGENT_GID" \
        --groups "$HARNESS_SECRET_READERS_GID" -- env … "$@"

# Acceptable fallback when only one group is needed: make harness-secret-readers
# the agent's primary GID. Loses the ability to chown agent-owned files to a
# distinct GID, so reserve for environments that don't need that.
setpriv --reuid "$AGENT_UID" --regid "$HARNESS_SECRET_READERS_GID" \
        --clear-groups -- env … "$@"
```

`--clear-groups` without an accompanying `--groups <secret-readers-gid>` is an explicit defect for any sandbox that mounts a secret. The integration test for secret read **must** assert that an in-sandbox `id -G` lists `HARNESS_SECRET_READERS_GID` and that `cat <secret_mount_path>/<secret_id>/<version>` succeeds; today that test would fail and that is the point — Phase 7a's secret materializer ships only after the entrypoint is fixed to one of the two shapes above.

A root-entrypoint-then-drop alternative (entrypoint reads the secret as root and injects via env before dropping to UID 65534) is **not** part of this contract: it would put plaintext in the agent process environment, which is observable via `/proc/self/environ` and survives across `execve`, and it would break the bind-mount-only model that lets Phase 8 swap directory storage for KMS without touching the entrypoint.

**Shell agent (`HARNESS_AGENT=sh`) does not mount secrets.** The current `harness-agent-entrypoint:132` `exec`s the shell shim as root and does not run the `setpriv` drop the `claude` branch uses, so a root shell would bypass the `0440 group=harness-secret-readers` containment if the shell sandbox were also offered the secret bind-mount. Phase 7a forbids this by construction: the shell generation's `agent_runtime_profile` carries no `anthropic_api_key_secret_id` / `anthropic_auth_token_secret_id`, the per-generation control dir for a shell generation has no `secrets/` subdirectory materialized, and `secret_mount_path` is unset so no bind-mount is added to the runtime spec. The orchestrator validates this at generation-start time: a shell generation whose manifest carries any secret reference is rejected with `error_class = shell_secret_disallowed`. If a future shell or BYO-agent variant ever needs upstream credentials, it must first land its own `setpriv --groups "$HARNESS_SECRET_READERS_GID"` drop in the entrypoint and explicitly opt in via `agent_runtime_profile.requires_secret_drop = true` — the doc-level rule is "no secret mount unless the entrypoint demonstrably runs the agent under a non-root UID with the readers group." Until then, the only way to give a shell session model access is via a separate Claude generation in the same session.

The per-generation secrets directory under the control dir is created mode `0750` owned by `orchestrator:harness-secret-readers`, and the per-secret file is hard-linked or copied into it preserving the same owner/group/mode. Hardlink is preferred when secrets root and control dir share a filesystem (it avoids duplicating plaintext on disk and is safe given the version-immutability rule above); copy is the fallback across filesystem boundaries. The bind-mount into the sandbox at `secret_mount_path` is read-only (`ro,nosuid,nodev,noexec`); read-only bind enforces that the agent cannot mutate the file, while `0440 group=harness-secret-readers` is what makes the in-sandbox read succeed.

Phase 8 replaces the directory backend with KMS without changing this contract — the entrypoint still reads `${SECRET_DIR}/<secret_id>` as UID `65534`, and the KMS-backed materializer is responsible for writing files with the same owner/group/mode and for choosing whether to materialize via tmpfs to keep plaintext off persistent storage. If a future Phase 8 design uses gVisor `--file-access=shared` with idmap mounts, the contract becomes "the materialized file must be readable by the sandbox-mapped UID" and the host group convention is replaced by idmap remapping; the entrypoint contract is unchanged.

At generation start the host materializes the per-generation secrets dir under the control dir and bind-mounts it read-only into the sandbox at `secret_mount_path`; the entrypoint reads `${SECRET_DIR}/<secret_id>` rather than the manifest. The manifest carries only `secret_id` + `secret_version`, never plaintext, and the entrypoint must not fall back to host-level Claude configuration.

Network resources are allocations, not just text. The allocator enforces uniqueness over every non-destroyed row for: `netns_name`, `netns_path`, `host_veth`, `sandbox_veth`, `host_gateway_ip`, `host_side_cidr`, `sandbox_ip_cidr`, `control_dir`, `control_manifest_path`, `runtime_bundle_dir`. `reserved_checkpointed` allocations are not live processes, but their identity stays reserved for physical restore and must not be reused.

The host carves IPs from a configured CIDR pool (`harness.network.cidr_pool`, default `10.200.0.0/16`) into per-generation `/30` subnets — gateway IP is `.1`, sandbox IP is `.2`. The allocator hands out the lowest free `/30`. Identity (the `/30`, the netns name, the veth pair, the control/bundle dirs) is released back to the pool **only when the allocation reaches `destroyed`** — never on `reclaimable`. This is consistent with the `failed_retention` window: a `failed` generation moves to `reclaimable` after retention so an operator can inspect netns/control/spec/log artifacts; if the `/30` were reused while in `reclaimable`, those artifacts would either disappear (defeating retention) or collide with the new generation. Pool capacity is therefore the count of allocations *not in `destroyed`* (i.e. `allocating + ready + live + reserved_checkpointed + recreating + reclaimable`); `harness.max_sessions` is the soft policy ceiling and must be set below the pool ceiling. Both ceilings are reported by `/api/quota`.

Pool exhaustion: the allocator returns a typed error before any generation row is created. The session-create / first-turn POST surfaces this as `503` with `error_class = pool_exhausted`, and the queued turn is rejected rather than left waiting. Reaper urgency does not change — a stuck `recreating` allocation is still recovered only by the startup sweep, not by allocator pressure. The `harness.max_sessions` soft ceiling produces the same error class but is hit first under the lab's defaults (`harness.max_sessions=30` vs ~16K `/30` slots in `10.200.0.0/16`).

7a populates every network-profile field needed to run the data-analysis path end-to-end without `phase1-demo`: netns / veth / IP / gateway / CIDR, plus the static egress allow-list covering the local LLM proxy at `host_gateway_ip:8082`, the configured Doris FE/BE hosts and ports (`doris_fe_hosts`, `doris_be_hosts`, `doris_ports`), and DNS (`dns_policy`) when any of those targets are expressed as hostnames. These are *static, lab-wide* values read from `harness.network.egress` config — every 7a-allocated network profile gets the same Doris/DNS allow-list. This is what 7a's "production-like mode" actually requires; without Doris egress, the local LLM proxy works but `vhr_data` queries break, which is the project's primary product path.

Phase 8 turns the same fields into per-tenant policy: tenant-scoped Doris ACLs, per-session egress quotas, and rotation of the host-firewall enforcement layer. The schema is identical between 7a and 8; what changes in 8 is the *source* of the values (per-tenant policy rows feeding `egress_policies` instead of a single host-config block) and the enforcement strength (host firewall + nft chains rather than just per-netns nft rules).

Schema-only fields not yet wired are not silently optional — the test matrix asserts that on 7a, every `network_profiles` row is populated with the lab-wide Doris/DNS values exactly as configured (not NULL), and that `egress_policies` rows materialize the corresponding allow-rules. An accidental partial implementation that leaves Doris fields empty fails loudly.

### Control Manifest

Each generation gets an isolated control directory and manifest. The manifest must include enough identity to make stale or cross-wired starts fail closed:

```text
session_id
generation_id
network_profile_id
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

The entrypoint must validate `session_id`, `generation_id`, `network_profile_id`, `agent_runtime_profile_id`, `manifest_version`, `secret_version`, and `manifest_digest` before starting the agent. Resolved credentials are read from `${SECRET_DIR}/<secret_id>` per the secret-mount contract above. A mismatch on any of these fields exits non-zero with a code distinguishable from agent crashes; the host marks the generation `failed` with `error_class = manifest_digest_mismatch` (or the matching `*_mismatch` class for the offending field).

### Resource Allocator And Reaper

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

#### Reaper Ownership Boundary

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

A `failed` generation moves to `reclaimable` after a configurable retention window (`harness.reaper.failed_retention`, default 10 minutes) so an operator can inspect netns/control/spec/log artifacts before they disappear. The allocation identity (`/30`, netns name, veth pair, control/bundle dirs) stays reserved for the duration of `reclaimable` — it is **not** returned to the pool until the next reaper sweep advances it to `destroyed`. The retention window therefore does not block N+1: cold fallback uses a fresh allocation identity drawn from the next free `/30` slot, which is independent of N's still-occupied identity. After retention expires, the next reaper sweep moves the allocation to `destroyed` on the normal path, and only then is the identity available for reuse.

Resource allocation lifecycle:

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

### Lease And CAS Fencing

Within the single-orchestrator scope defined under Concurrency And Storage Model, lease and CAS are the correctness mechanism across goroutines and across orchestrator restarts. They are not a multi-process clustering mechanism; the database is the single owner. All critical transitions must be database-guarded by lease and generation:

```sql
update turns
set status = 'leased',
    generation_id = :generation_id,
    lease_owner = :owner,
    lease_expires_at = :lease_expires_at
where id = :turn_id
  and status = 'queued'
  and session_id = :session_id
  and lease_owner is null;

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
```

An implementation can collapse this into one `queued -> running` CAS, but it must still claim from `lease_owner is null`, set the owner/expiry in the same statement, and check `lease_expires_at` on subsequent updates. The same pattern applies to generation activation, turn completion, failure marking, checkpoint state transitions, and event writes. In-memory locks can reduce local contention, but they are not a correctness mechanism.

### Generation Lease

The generation row carries its own `lease_owner` / `lease_expires_at` (see Runtime Generation Table). The generation lease guards transitions on the **generation and its allocations** — `starting → probing → active`, `idle → checkpointing`, `checkpointed → restoring → active` (with the underlying allocation moving `reserved_checkpointed → recreating → ready → live` in lock-step), and `failed` marking. It is independent of the per-turn lease; a generation can hold its own lease while no turn is leased.

Rules:

```text
Acquired by the Runtime Manager goroutine that creates the generation row,
  in the same DB transaction as the row insert.

  lease_owner = "<orchestrator_owner.uuid>:<role_tag>"
    where role_tag is a fixed string identifying the in-process role
    that owns generation transitions. Phase 7 defines exactly one
    role_tag for the generation lease: "runtime_manager".

  lease_expires_at = now + lease_ttl.

  The bridge consumer is not a separate lease owner. It is another
  execution point inside the same orchestrator process and the same
  role; on each heartbeat/inbox poll it renews under the same
  "<uuid>:runtime_manager" string the Runtime Manager goroutine
  used. CAS therefore matches whether the renewal is driven by the
  Runtime Manager goroutine or by the bridge consumer.

Renewed at half lease_ttl by any execution point in the runtime_manager
  role within the owning orchestrator process. Renewal is a CAS:
    update runtime_generations
    set lease_expires_at = :now + lease_ttl
    where generation_id = :gid
      and lease_owner = :owner          -- "<orchestrator_owner.uuid>:runtime_manager"
      and lease_expires_at > :now;

Released by setting lease_owner = null in the same transaction that moves
  the row to a terminal state (failed, reclaimable, destroyed) or to
  generation status `checkpointed` (no live process owns the lease while
  checkpointed; the underlying allocation_state moves to
  `reserved_checkpointed` in the same transaction).

Expired (lease_expires_at <= now) means the previous orchestrator process
  crashed or stalled. Because lease_owner is keyed on
  orchestrator_owner.uuid (a fresh UUID per process start), a restarted
  orchestrator never matches the prior owner string under CAS, even if it
  reuses the same role_tag. The startup-recovery sweep (Allocation
  Recovery On Startup) is the only caller allowed to fence an expired
  lease.
```

`lease_ttl` defaults to 60 s; renewal cadence is 30 s. Both are config, not architecture, and are reported on the generation row for debuggability.

### Allocation Recovery On Startup

Turn Ledger restart recovery covers turns. Generation-level and allocation-level recovery is symmetric and runs in the same startup sweep, before the reaper opens for business and before any new generation is allocated:

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

## New Lifecycle Model

### Session Lifecycle

```text
created
  -> accepting_input
  -> turn_running
  -> accepting_input
  -> destroyed

Any state can become failed if the session itself is not recoverable.
```

The current public statuses can remain compatible for the UI:

```text
created
running_active
running_idle
checkpointing
checkpointed
failed
destroyed
```

But internally, the runtime generation status should be separated from the session status. Two enums travel together but live on different rows: `runtime_generations.status` and `network_profiles.allocation_state`. They are distinct: a generation status describes *the executor instance*; an allocation state describes *the host resource identity*. The mapping from internal generation status to public session status is:

| Internal generation status                   | Public session status |
| -------------------------------------------- | --------------------- |
| `allocating`, `starting`, `probing`          | `running_active`      |
| `active`                                     | `running_active`      |
| `idle`                                       | `running_idle`        |
| `checkpointing`                              | `checkpointing`       |
| `checkpointed` (no live process)             | `checkpointed`        |
| `restoring`                                  | `running_active`      |
| `failed` (terminal)                          | `failed`              |
| no generation row, session destroyed         | `destroyed`           |

Note the absence of `recreating` from this column: `recreating` is an *allocation* state on `network_profiles`, not a generation status. While the host is rebuilding a checkpointed generation's netns/veth/IP/control identity, the generation row's `status` is `restoring`; the underlying allocation row's `allocation_state` moves `reserved_checkpointed -> recreating -> ready` independently. Likewise, `reserved_checkpointed` is *only* an allocation state — the generation row of a checkpointed session has `status = checkpointed`, never `status = reserved_checkpointed`. Schema enums, CAS predicates, recovery sweeps, and tests must all keep this split: the schema migration enumerates `checkpointed` as a `runtime_generations.status` value and `reserved_checkpointed` as a `network_profiles.allocation_state` value, never the other way around.

A session in `checkpointed` accepts new turns by triggering the restore path automatically; no UI action is required.

Restore-time generation statuses (`restoring`, `probing` on a previously-checkpointed session — including the window during which the underlying allocation is in `recreating`) deliberately surface as `running_active` so the UI shows "session is coming back up" rather than flapping through `checkpointed → starting → running`. The session is not accepting new turns until it returns to `running_idle`, and that gate is enforced by the turn ledger and bridge `claim_next_turn`, not by the public status string.

**Phase 7 follow-up: `phase` sub-field on the session row (not a Phase 7a blocker).** Collapsing six internal states (`allocating`, `starting`, `probing`, `active`, `restoring`, plus the period during which the allocation is in `recreating`) into one public `running_active` is correct for API stability but loses UX-relevant detail: a restore averages 100–200 ms while a cold start can take 1–2 s, and the UI cannot distinguish "warming up" from "resuming" in the progress indicator. The follow-up (slated for Phase 7b or Phase 8) is to expose an additional `phase` subfield on the session-row JSON — values `cold_start | restore | live | idle | failing` — without altering the existing `status` enum or its API contract. Existing clients that ignore unknown fields keep working; UIs that opt-in show a more granular progress signal. This is intentionally split from Phase 7a so the lifecycle work stays focused; tracked in `docs/PLAN.md` as a Phase 7b candidate.

### Runtime Generation Lifecycle

```text
none
  -> allocating         (host resources being claimed; no runsc yet)
  -> starting           (runsc start invoked)
  -> probing
  -> active
  <-> idle              (active <-> idle: turn ledger drives this transition)
  -> checkpointing      (only from idle)
  -> checkpointed
  -> restoring          (allocation moves reserved_checkpointed -> recreating
                         -> ready in parallel; generation status stays restoring
                         throughout, never `recreating`)
  -> probing
  -> active

Failure fallback:

allocating/starting/probing/restoring/checkpointing
  -> failed
  -> cold_start_new_generation
```

`active` and `idle` are both reachable from each other for the lifetime of one generation. A generation enters `active` whenever its turn ledger has at least one `leased` or `running` turn, and returns to `idle` when no such turn remains and all bridge output has been ack'd. The turn ledger is the source of truth; `runtime_generations.status` is a denormalized cache that lets the reaper, idle monitor, and checkpoint trigger answer "is this generation idle right now" without scanning turns. Every turn-state transition that can change the predicate (claim, ack_started, completion, failure, cancel) must update `runtime_generations.status` in the same transaction as the turn-state CAS.

The cache and ledger agreement is enforced at two layers: (a) every turn-state mutation goes through one helper that performs the turn CAS and the matching `runtime_generations.status` update in the same transaction (no other write path is allowed); (b) the test matrix asserts cache/ledger agreement after every transition class. SQLite triggers were considered but rejected — a trigger cannot easily express the predicate "no other leased/running turns for this generation," and silent re-entry on the trigger would obscure the fencing rules. The single-helper rule is the architectural commitment; the test matrix is what makes it a runtime guarantee instead of a code-review hope.

#### Single-Helper Contract

The helper has exactly four call sites. Each call site is one DB transaction containing both the turn CAS and the `runtime_generations.status` CAS. The generation CAS predicate must explicitly **exclude** transient generation statuses (`allocating`, `checkpointing`, `restoring`, `failed`, `destroyed`) so that no race can flip a generation to `active` while it is mid-allocation, mid-checkpoint, or mid-restore. (`recreating` does not appear in this list because it is an *allocation* state, not a generation status; the generation row of an allocation in `recreating` is in `restoring`, which is already excluded.) The four call sites:

**1. claim_next_turn** — bridge picks up a queued turn.

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

If the turn CAS returns no row (no queued work, or the single-in-flight predicate matched an already-running turn), the generation CAS is skipped. If the generation CAS affects zero rows, the transaction aborts and the turn claim is rolled back — the generation was concurrently moved to `checkpointing` / `restoring` / `failed` and is not eligible to claim. The bridge's caller treats both as "no work for now" and retries on next poll; neither is an error. The single-in-flight predicate is what makes proxy source-IP correlation sound (see Proxy And Upstream Observability).

**2. ack_started** — bridge confirms the turn started executing.

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

**3. completion** — `ack_turn_completed`, including `completed`, `failed (turn-level)`, and `canceled`.

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

The `NOT EXISTS` subquery is the predicate that captures "this is the last live turn for this generation." If another turn is concurrently leased on the same generation (allowed under the protocol once a generation is `active`, even if Phase 7 currently runs one turn at a time per generation), the generation stays `active` and the cache remains correct. The CAS does not match `checkpointing` / `restoring` — those states cannot host a running turn by Hard Invariants, so observing one here would already be a bug; the predicate's narrowness makes it self-checking.

**Active-lease predicate (applies to all four call sites except recovery).** Every non-recovery write path includes both `lease_owner = :owner` *and* `lease_expires_at > :now`. The owner predicate alone is not sufficient: an orchestrator-internal scheduler stall, an SQLite write that beats a stuck goroutine to the row, or a long-running bridge call that returns past the lease deadline can each produce a "late completion" landing after the recovery sweep has already requeued the turn under a fresh attempt. Without the expiry check, that late completion would silently overwrite the requeued attempt's row. With the expiry check it is rejected, the helper bubbles up "expired-lease completion ignored," and the durable record is the recovery sweep's outcome — which is the correct one because the orchestrator already declared the original attempt dead. The recovery sweep itself is the only path allowed to write under an expired lease, and it does so under a different CAS predicate (`lease_expires_at <= :now`) keyed on `orchestrator_owner.uuid`, never on the prior `lease_owner` string.

**4. failure / cancel of the generation while a turn is in flight** — used by the grace-window expiry path, the lease-expiry sweep, and explicit operator cancel.

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

Operator cancel and grace-window expiry both run on the active orchestrator with a still-valid lease, so the `lease_expires_at > :now` predicate matches and these paths use the CAS above. The lease-expiry sweep is structurally different — by definition the lease is no longer active — and runs through `Allocation Recovery On Startup`'s expired-lease path, whose CAS predicate is `lease_expires_at <= :now AND orchestrator_owner.uuid = :current_owner_uuid` (not the prior lease string). Recovery is therefore the *only* code path that can move a turn or generation forward without an active lease; every other helper call is rejected by the predicate.

For per-turn failure that does not condemn the generation (e.g. a turn-level error that the agent itself raised cleanly), the helper falls back to the completion CAS in case 3 with `:terminal_status = 'failed'` and the generation stays `active`/`idle` per the same NOT EXISTS predicate. The distinction — turn-failure vs generation-failure — is decided by the caller, not by the helper.

Across all four call sites, **`lease_owner` is keyed on `<orchestrator_owner.uuid>:<role_tag>`** (see Generation Lease). A restarted orchestrator never matches a prior owner string, so a stale call site whose code was somehow still running across restart cannot mutate the generation; only the startup-recovery sweep can fence an expired lease.

The important rule is:

```text
Only checkpoint an idle generation with no running turn and no unacked output.
```

### Container Lifecycle

Current baseline (long-lived container per session, automatic checkpoint disabled) is documented in [architecture.md](./architecture.md#runtime-flow). Target:

```text
create session
  -> no container required yet
first queued turn
  -> acquire or create runtime generation
  -> network probe
  -> bridge claims turn
  -> Claude/shell executes turn
  -> durable completion
  -> generation becomes idle
idle policy
  -> either keep alive
  -> or checkpoint then destroy process resources
next queued turn
  -> restore checkpoint if valid
  -> otherwise cold start new generation
  -> Claude resume provides logical continuity
```

The session survives every container transition.

## Claude Resume Strategy

There are two independent resume layers:

```text
Logical resume:
  ClaudeSessionUUID + Claude home + persisted transcript + workspace

Physical resume:
  runsc checkpoint image + runtime generation restore
```

The target architecture treats logical resume as required for correctness and physical resume as an optimization.

### Normal Hot Path

```text
turn queued
  -> active generation exists
  -> bridge claims turn
  -> Claude Code receives stream-json input
  -> parser records result
  -> turn completed
```

### Restore Path

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

### Cold Fallback Path

```text
turn queued or leased but not started
  -> restore fails or bridge fails to reconnect
  -> mark generation N failed
  -> start generation N+1 from bundle with same ClaudeSessionUUID / resume flag
  -> pre-start netns probe + post-start in-sandbox probe pass
  -> Claude resumes logical conversation; bridge claims queued turn
```

This fallback is what makes the system reliable; the user should not be blocked on perfect `runsc restore` behavior. Retry eligibility is governed by Turn Ledger restart recovery — `ack_started_at` turns are never auto-replayed.

## Agent Bridge Protocol

The bridge exposes a per-turn protocol. The host owns ordering; the bridge never invents a turn sequence.

```text
Handshake:
  hello(session_id, generation_id, agent, protocol_version)
    -> hello_ack(last_output_sequence_by_turn, leased_turn_id?, server_time)
  probe_network()

Turn lifecycle:
  claim_next_turn(session_id, generation_id)        # only when leased_turn_id is null
  resume_turn(turn_id)                              # only when hello_ack returned leased_turn_id
  ack_turn_started(turn_id)
  emit_output(turn_id, output_sequence, payload)
  ack_turn_completed(turn_id, result)

Concurrent with everything above:
  heartbeat(session_id, generation_id)
```

`claim_next_turn` and `resume_turn` are mutually exclusive per `hello_ack`: if `hello_ack.leased_turn_id` is non-null the bridge must call `resume_turn(leased_turn_id)` and may not call `claim_next_turn` until that turn reaches a terminal state.

`probe_network()` is the post-start / post-restore in-sandbox probe and must pass before `claim_next_turn()`. The host can run only the pre-start netns probe on its own; it cannot prove agent-visible config inside the sandbox.

### Transport Layout

The transport is a per-generation directory shared between host and sandbox: `<bridge_root>/<generation_id>/{inbox,outbox,heartbeat}`. Queue names are written from the bridge's perspective: `inbox/` is what the bridge receives (host writes here, bridge reads), `outbox/` is what the bridge sends (bridge writes here, host reads). Both queues use `tmp/<uuid> -> <queue>/<seq>.json` atomic rename plus `fsync` on the destination directory, identical to the control-manifest contract. The reader on each queue processes files in `seq` order and unlinks after the message is persisted or applied — bridge unlinks `inbox/` files; host unlinks `outbox/` files.

Concretely:

```text
inbox/    host -> bridge   (claim grants, resume_turn, host control messages)
outbox/   bridge -> host   (ack_turn_started, emit_output, ack_turn_completed,
                            generation status, failure marks)
```

Heartbeats are written as `heartbeat/bridge` (bridge-side) and `heartbeat/host` (host-side) by overwriting via tmp+rename; liveness is judged by file `mtime` polled at the heartbeat cadence. No inotify dependency.

Host consumer ordering for any bridge message that drives a turn-state transition (`ack_turn_started`, `ack_turn_completed`, completion failure, generation status changes):

```text
read outbox file
  -> DB transaction (turn-state CAS + durable event append)
  -> in-memory hub publish (best-effort, after commit)
  -> unlink outbox file
```

Bridge consumer ordering for any host message in `inbox/` (claim grants, resume directives) is the mirror image: read, apply locally (claim a turn, start agent execution), then unlink. The unlink is always last on both sides.

Unlink is intentionally last. A host crash between commit and unlink replays the same `outbox/` file on restart; the CAS predicate makes the replay a no-op, and the `(turn_id, generation_id, output_sequence)` dedup or the event's `dedupe_key` rejects the duplicate event. Bridge-side at-least-once delivery is therefore the assumed semantics, and the durable schema is what makes idempotency hold. The same is true for `inbox/`: a bridge crash between applying a host directive and unlinking is recovered via the bridge's `hello` flow on reconnect (`hello_ack.leased_turn_id` tells the bridge whether the directive landed), so a duplicate `inbox/` file is detectable as already-applied work.

Bridge-side write ordering is the mirror image: write under `tmp/`, fsync the file, rename into the queue, fsync the queue dir. The sandbox bind-mount of `<bridge_root>/<generation_id>` must use a gVisor file-access mode that propagates `fsync` to the host filesystem on commit. Under runsc's gofer/VFS2, this requires the bridge dir's mount to be declared with `file-access=exclusive` (the option name on `runsc release-20260511.0`; if a future runsc rev renames the option, the runtime spec generator must be updated to emit the equivalent annotation in lock-step). Concretely, the per-generation `config.json` must set the bridge mount's annotation rather than relying on the runtime-wide default:

```json
{
  "destination": "/harness-control/bridge",
  "type": "bind",
  "source": "<bridge_root>/<generation_id>",
  "options": ["rbind", "rw"],
  "annotations": {
    "dev.gvisor.spec.mount./harness-control/bridge.type": "bind",
    "dev.gvisor.spec.mount./harness-control/bridge.share": "exclusive"
  }
}
```

(Annotation key spelling tracks the runsc release pinned in `runsc-warm-sentry-research.md`; the generator emits whatever the pinned binary documents and the test matrix asserts the annotation is present and equals the binary's "exclusive" token.) Shared-mode mounts have known fsync-propagation quirks where the gofer batches metadata back to the host and an `fsync` on the bridge side does not guarantee the rename is durable on host fs. The failure mode is silent: lifecycle messages appear written from the bridge's view, but a host crash before the gofer flushes loses them — the host never reads the file, the turn never transitions, the user sees a stuck queue with no error log. Because the failure is invisible at write time, the test matrix asserts the cache mode at config-emission time and runs an induced-crash test (Bridge-Side Durability) that fsyncs from inside the sandbox and asserts host visibility after a host process restart. Single bridge process per generation makes `exclusive` safe; if a future design ever runs two writers on the same bridge dir, this contract must be revisited together with the gVisor docs for the runsc rev in use.

`hello_ack`'s `last_output_sequence_by_turn` is computed by the host as `MAX(output_sequence)` over the durable event log filtered by `(session_id, generation_id, turn_id)` for every non-terminal turn this generation owns. Only **committed** event-log rows are visible; in-flight `emit_output` batch transactions are not. The committed boundary therefore lags the bridge's locally-observed last-emitted sequence by up to one batch window — this is by construction (see Idempotency And Sequence Recovery for why this lag is the protocol's primary reconnect path, not an edge case). The bridge's local view is discarded on reconnect; it must trust the host-returned `last_output_sequence_by_turn` and re-emit anything past it.

End-to-end turn-start latency budget (claim observed in `inbox/` to `ack_turn_started` durable in events): under 50 ms at lab load. This bounds how aggressively the bridge can poll and how the host batches `emit_output` writes.

### Idempotency And Sequence Recovery

Every bridge message carries `session_id`, `turn_id`, `generation_id`, and (for output) `output_sequence`. The bridge owns only per-turn `output_sequence`; the host event store assigns the global `event_id` after dedupe. Stale-generation events are rejected. Duplicate output is deduplicated by `(turn_id, generation_id, output_sequence)`; lifecycle messages are made idempotent by their CAS predicates per Transport Layout.

A bridge process can crash and restart while the sandbox is still alive. On restart it has no memory of the next `output_sequence`, which would either collide or skip. The bridge therefore re-runs `hello`, applies the `last_output_sequence_by_turn` returned by the host, resumes each non-terminal turn from `last + 1`, and uses `resume_turn` for the leased turn instead of `claim_next_turn`.

#### Reconnect During An emit_output Batch (primary path, not an edge case)

`emit_output` batches and lifecycle acks have different transaction boundaries (see Durable Event Log: "Event durability is a hard invariant, but the transaction boundary is per message kind, not per turn"). A burst of partial deltas from Claude stream-json is committed in one batch transaction; a `ack_turn_started` / `ack_turn_completed` is committed in its own transaction together with the turn-state CAS. When a bridge reconnect lands between the two — or anywhere inside a batch window — the host's committed boundary trails the bridge's local "last emitted" by up to the size of the in-flight batch. This is the **expected** state on every reconnect during streaming output, not a corner case.

The protocol resolves it deterministically:

```text
1. Bridge crashes / loses connection mid-batch. Host has committed
   output_sequence ≤ S_commit; bridge had locally emitted up through
   S_local where S_local ≥ S_commit.

2. Bridge reconnects, sends hello, host returns
   last_output_sequence_by_turn = S_commit (per the rule above —
   in-flight batches are not visible to MAX(output_sequence)).

3. Bridge ignores its local view and re-emits every output from
   S_commit + 1 onward, including outputs whose original transmission
   was already accepted by the host but lost in the in-flight batch
   (or already committed and merely invisible because of batching
   semantics — the bridge cannot tell which).

4. The host applies the (turn_id, generation_id, output_sequence)
   dedup predicate. Any re-emit whose sequence is already committed
   is dropped silently and is NOT logged as an error or warning;
   any re-emit whose sequence is genuinely new is appended.
```

Implementations must treat this dedup as a successful no-op: it is the steady-state behavior any time a bridge reconnects while output is flowing. Logging it as a warn or error would produce one log line per delta on every reconnect under load. The host's `(turn_id, generation_id, output_sequence)` UNIQUE constraint is the load-bearing piece; the dedupe path is exercised on every reconnect that interrupts a streaming turn, not only on truly anomalous duplicate traffic.

The same logic applies to a bridge that did not crash but momentarily lost transport (file-queue rename failure, host crash between commit and unlink). The unlink-after-commit ordering described in Transport Layout makes a replayed inbox file collide with the same dedup predicate; replays are silent no-ops by design.

### Turn Completion

Claude completion: stream-json `result success`, `result non-success`, or `error`. Shell completion: `harness.turn_done`. Completion is written to the durable turn ledger before the session is marked idle.

## Checkpoint Policy

Checkpoint should be allowed only when all of these are true:

```text
session status is running_idle / accepting_input
generation status is idle
no queued eligible turn exists for the session
no leased/running turn exists for the session or generation
bridge heartbeat is healthy
bridge is checkpoint-ready, with no active host control request that must survive restore
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

**Digest equivalence rules — what "safely regenerated" actually means.**

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

`runsc version` is exact-match (see strict list). The current deployment pins `runsc release-20260511.0` (see `runsc-warm-sentry-research.md`); a checkpoint produced under any other build is treated as incompatible. gVisor does not promise cross-build checkpoint compatibility, and a silently-restored mismatch is the worst failure mode (silent sentry state corruption). On any validation failure, the checkpoint row -> `reclaimable`, the allocation transitions through `recreating` to `destroyed`, and the session cold-starts a new generation with Claude logical resume.

Checkpointed generation network strategy: the live process and netns may be released after checkpoint, but the recorded netns/IP/veth/egress/spec/manifest identity remains reserved (see `reserved_checkpointed` allocation state). Before `runsc restore`, the runtime manager recreates that identity and runs the pre-restore probe; after restore, the bridge runs the post-restore probe. Failure to recreate or probe -> generation N failed, cold fallback N+1.

## Network Path And Probes

Target network path:

```text
Claude Code inside sandbox
  -> http://{allocated_host_gateway_ip}:8082/v1/messages?beta=true
  -> host namespace listener at http://0.0.0.0:8082
  -> claude-code-proxy
  -> upstream model provider
```

In the lab today `{allocated_host_gateway_ip}` resolves to `10.200.1.1` because the netns is fixed; under the target architecture it is generation-scoped allocation data per the Network Profile schema.

Two probe phases:

- **Pre-start / pre-restore host-side netns probe.** Host runs it from the recreated netns once netns/veth/IP/egress resources exist. Validates route, gateway, egress policy, and proxy reachability without requiring a sandbox process.
- **Post-start / post-restore in-sandbox probe.** Run by the Agent Bridge after `hello` and before `claim_next_turn`. Validates the agent-visible Anthropic base URL and auth config.

Both probes target the local proxy at `http://{host_gateway_ip}:8082`, never the upstream Anthropic API: the proxy is the actual boundary, probing upstream would consume real quota and reveal nothing about netns/egress policy, and the proxy short-circuits auth so a 401 is a deterministic local failure.

**Probe contract with `claude-code-proxy`.** Phase 7a pins a dedicated reachability route, `GET /healthz` returning `200` with body `{"status":"ok"}`, on the proxy itself; the host-side netns probe and the in-sandbox probe both call it. This replaces the previous reliance on `HEAD /` returning `405`, which depended on undocumented behavior of the current proxy version and would silently start "passing" if a future proxy build returned `200` for `HEAD /` — a probe that passes when routing is broken is worse than no probe.

Auth-path reachability is verified by the second probe call: `POST /v1/messages` with the configured key, which the proxy must short-circuit and reject locally. The accept set is configurable per profile (`harness.probe.accept_status`, default `{401}` for `POST /v1/messages` and `{200}` for `GET /healthz`); any other status, refused connection, timeout, or socket failure fails the probe. The proxy must reject locally — a probe must never reach upstream. The current proxy key is `123`; any other key is misconfigured.

A contract test in the harness CI build runs against the pinned `claude-code-proxy` version on every release: it asserts `GET /healthz → 200`, `POST /v1/messages` (no key) `→ 401`, and `POST /v1/messages` (configured key with empty body) `→ 401` (because the proxy rejects malformed bodies before forwarding). Any divergence in proxy behavior fails the build, forcing an explicit re-pin of the proxy version or a probe-config update before deployment.

Probe failure must not leave the session silently queued. Each probe gets a bounded retry budget (default: 3 attempts, 500 ms apart for the host-side netns probe; 5 attempts, 1 s apart for the in-sandbox probe). On exhaustion the generation is marked `failed` with `error_class = probe_failed_pre_start` or `probe_failed_post_start`; the session surfaces the error through the standard generation-failure path so the UI sees a concrete failure instead of a stuck queue. Cold fallback applies as it does for any generation failure.

Egress policy is part of the network profile, not later hardening: allow the local proxy at `host_gateway_ip:8082`, allow Doris FE/BE hosts and ports explicitly, allow DNS only if the profile uses hostnames, deny everything else by default.

## Proxy And Upstream Observability

Sandbox-to-proxy reachability is only one part of the model path. Phase 7 also makes the proxy/upstream hop observable enough to debug slow or stuck turns:

```text
proxy_request_id
session_id
turn_id
generation_id
upstream_model
upstream_base_url
proxy_connect_latency_ms
upstream_first_byte_latency_ms
upstream_total_latency_ms
retry_count
timeout_kind = connect | first_byte | total | idle_stream  -- non-NULL iff error_class = timeout, NULL otherwise
http_status
error_class = auth | network | upstream_5xx | rate_limit | timeout | malformed_stream | canceled
```

Correlation must not depend on Claude Code exposing response headers. When the bridge starts a turn, the control plane registers `active_model_request_context` (`session_id`, `turn_id`, `generation_id`, `sandbox_source_ip`, `lease_owner`, `expires_at`); claude-code-proxy matches inbound requests by `sandbox_source_ip` against the active context, assigns `proxy_request_id` and a per-turn `request_sequence`, and emits `proxy.request.started` / `proxy.request.completed` events that the orchestrator joins to `turn_id` / `generation_id`.

**Source-IP correlation requires single-in-flight-turn-per-generation.** `sandbox_source_ip` uniquely identifies a generation, not a turn — every turn that runs against generation G shares the same per-generation sandbox IP. The IP-based join is therefore only sound while there is **at most one turn in `leased`/`running` state per generation at a time**. Phase 7 enforces this with the `claim_next_turn` CAS defined in the Single-Helper Contract: queued turns are unbound (no `generation_id`), the lowest-sequence queued turn for the session is selected by `MIN(sequence)`, the helper's update binds `generation_id = :generation_id` at claim time, and the same statement carries a `NOT EXISTS (… status in ('leased','running') for this generation_id)` predicate that fails the claim if another turn is already in-flight on the same generation. The proxy-correlation guarantee follows directly from that predicate; the canonical SQL lives in the Single-Helper Contract / `claim_next_turn` block above and is not restated here, so future implementers cannot copy a divergent variant from this section by mistake.

If at any future point we want to support concurrent turns per generation (the protocol is structurally close — see the `NOT EXISTS` predicate in completion CAS case 3), the source-IP join is no longer unique and the **fallback bridge-managed local gateway becomes the default** rather than an alternative: the bridge intercepts the agent's outbound request to the proxy, attaches a turn-correlation header (e.g. `X-Harness-Turn-Id` signed with a per-generation secret to prevent in-sandbox spoofing), and the proxy uses the header-derived turn id rather than `sandbox_source_ip` for the join. The header path is the right answer because the bridge is inside the sandbox and is the only component that knows which turn each agent process invocation is servicing; the source-IP path is only correct when "the turn currently running on this generation" is unambiguous.

For Phase 7 the single-in-flight invariant holds and the source-IP join is the documented mechanism. The header-based fallback is described here so that the schema and proxy-event payload reserve a `correlation_mode` enum (`source_ip` | `header`) on the `proxy.request.started` event from day one, even though the value is always `source_ip` until concurrent turns ship.

## Phase 7 Configuration Schema

Phase 7 introduces a non-trivial set of operator-facing configuration keys that the current `config/harness.yaml` loader does not yet expose. The current loader in `orchestrator/internal/config/config.go:108` is a hand-rolled "section header + scalar key/value" scanner: it understands one level of indentation, only top-level scalars under a section, and only the known `runtime.*` / `claude.*` keys it switch-matches. It cannot parse the Phase 7 schema below, which uses nested maps (`harness.network.egress.*`, `harness.probe.accept_status.*`, `harness.bridge.*`), lists (`doris_fe_hosts`, `doris_be_hosts`, `doris_ports`, accepted-status arrays), durations (`session_ttl`, `lease_ttl`, `failed_retention`), CIDRs, and string-enum fields (`dns_policy`).

Phase 7a therefore replaces the hand-rolled scanner with a real YAML parser. The chosen parser is `gopkg.in/yaml.v3` (already a transitive dependency in similar Go projects of this size, well-tested, supports strict-mode unknown-key detection); it is fed into a typed `Phase7Config` struct with `yaml` tags. The unmarshaler runs in strict mode (`KnownFields(true)`) so any key not present in the struct fails the load with a typed error pointing at the file/line — the previous `unknown config key` behavior is preserved. Durations decode through a custom `Duration` wrapper that wraps `time.ParseDuration`; CIDRs decode through `net/netip.ParsePrefix`; `dns_policy` decodes through a typed `DnsPolicy` enum. Tests pass a constructed struct rather than a YAML fixture, so config drift between code and doc is caught at compile time.

The schema must land before any of the host-resource code that consumes it; otherwise 7a allocates network profiles with NULL Doris fields, the reaper ships without a retention window, and the bridge probes have no `accept_status` to compare against. Treat the keys below as a contract between the architecture doc and the loader: a new key referenced anywhere in this document must also be added here, with a default and a validation rule, in the same change.

```yaml
harness:
  run_dir: /var/lib/harness/run            # parent of orchestrator.pid (flock target)
  session_ttl: 2h                          # absolute deadline applied at session create
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
      post_v1_messages: [401]
    pre_start_attempts: 3
    pre_start_interval: 500ms
    post_start_attempts: 5
    post_start_interval: 1s

  bridge:
    lease_ttl: 60s
    heartbeat_interval: 30s                # = lease_ttl / 2
    ack_started_grace: 90s                 # > lease_ttl by construction
    reconnect_grace: 30s

  reaper:
    failed_retention: 10m

  secrets:
    root: /var/lib/harness/secrets         # <host_secrets_root>; mode 0750
    readers_gid: 65501                     # HARNESS_SECRET_READERS_GID, baked into
                                           # the sandbox image at build
```

Validation rules (enforced by the loader, asserted by unit tests):

```text
- harness.network.cidr_pool must be a valid CIDR with prefix length <= 30 and
  must be wide enough to host harness.max_sessions /30 slots (loader rejects
  if max_sessions exceeds the pool's /30 count).
- harness.session_ttl must be > 0; the loader rejects `0` to prevent sessions
  that are reaped before they accept their first turn.
- For 7a "production-like mode" (the only mode the lab supports), egress
  doris_fe_hosts / doris_be_hosts / doris_ports must be non-empty; the
  loader fails with a typed error if any is missing. dns_policy != off is
  required when any Doris host is a hostname rather than an IP.
- bridge.ack_started_grace > bridge.lease_ttl. Equality is rejected — see
  "User-Visible Recovery For unknown_after_ack_started" for the rationale.
- bridge.heartbeat_interval ≈ bridge.lease_ttl / 2 (loader warns on
  divergence > 25 %).
- probe.accept_status sets must be non-empty for every defined probe path.
- events.retention_window and events.retention_rows must both be set; the
  earlier-hit bound governs trim. Setting one to zero disables that bound;
  setting both to zero is rejected.
- reaper.failed_retention must be >= 0; 0 disables the inspection window
  and moves failed generations directly to `reclaimable` on the next sweep.
- secrets.root must exist with mode 0750 and group readers_gid before the
  flock is taken; loader fails fast if not.
```

The loader produces a single typed `Phase7Config` struct that is passed by value into the allocator, reaper, bridge, and probe components. There is no ambient/global config access path; tests pass a constructed struct rather than a YAML fixture, so config drift between code and doc is caught at compile time.

A "Phase 7 config + loader + validation" change is a prerequisite of Step 1 (it appears in Step 1's *Depends on* column). It is small enough to land as a separate PR ahead of Step 1, but it is not optional and the table treats it as a hard dependency.

## SQLite Migration Strategy

The current `Store.migrate` in `orchestrator/internal/store/store.go:78` is a single bootstrap pass of `CREATE TABLE IF NOT EXISTS` / `CREATE INDEX IF NOT EXISTS` statements. It is sufficient for fresh installs but does *not* run any `ALTER TABLE`, does not add new columns to pre-existing tables, and has no notion of schema versions. A lab DB carried over from Phase 6 will not pick up `sessions.active_generation_id`, the `runtime_generations_one_nonterminal_per_session` partial index, or any of the new Phase 7 tables (`runtime_generations`, `network_profiles`, `agent_runtime_profiles`, `egress_policies`, `turns`, `events`, `orchestrator_owner`) just because the binary is upgraded. Phase 7a therefore replaces the bootstrap pass with an explicit, ordered, version-tracked migration runner.

### schema_migrations table

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
      Capture the current Phase 6 schema as version 1 so existing DBs can
      be tagged: `INSERT OR IGNORE INTO schema_migrations VALUES (1, ...)`
      after asserting the legacy tables (sessions, messages, artifacts,
      etc.) exist with their current columns. Fresh installs run the
      identical CREATE TABLE statements.

v2  phase7_baseline_tables
      CREATE TABLE: orchestrator_owner, runtime_generations,
      network_profiles, agent_runtime_profiles, egress_policies,
      runtime_generation_resources (see "Runtime Generation Resources
      Table" below for whether this is one row per generation or a
      separate table; v2 ships the schema chosen by that section).
      All new tables come up empty.

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
      treat as still-running (status in the live set per
      sessionstate.AllStatuses, ended_at IS NULL):
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
          Pre-Phase-7 checkpoint images do not carry the runsc-version
          / bundle-digest / manifest-digest metadata that Phase 7
          restore validates against (Checkpoint Policy), so they cannot
          be restored under the Phase 7 contract. This is the explicit
          one-time cost of the Phase 7 cutover; the user can start a
          fresh session and Claude logical resume preserves their
          conversation history if the original `claude_session_uuid`
          was recorded.
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

## Implementation Order

One architecture target, landed in small PRs. Partial states are scaffolding, not a supported architecture: cold fallback may not retry turns before the claim/ack protocol exists, no generation claims a turn before its bridge probe, no automatic checkpoint before restore/reconnect/fallback are correct. Steps 1–4 are Phase 7a; Steps 5–10 are Phase 7b.

Each step lists its PR boundary, prerequisites, and acceptance signal. Behavioral detail lives in the linked section above.

| # | PR boundary | Depends on | Acceptance | Detail |
| --- | --- | --- | --- | --- |
| 1 | Replace `Store.migrate`'s single `IF NOT EXISTS` pass with the versioned migration runner described in "SQLite Migration Strategy" (introduce `schema_migrations` and migrations v1–v6 in order); add tables/indexes for `runtime_generations`, `runtime_generation_resources`, `turns`, `events`, `network_profiles`, `agent_runtime_profiles`, `egress_policies`, `orchestrator_owner`; add `sessions.active_generation_id` column and the partial unique index `runtime_generations_one_nonterminal_per_session`; acquire the `<run_dir>/orchestrator.pid` flock at startup. | Phase 7 Config (separately landed, see Phase 7 Configuration Schema) | Schema covers generation fencing, allocation states, turn attempts, lease expiry, bridge ack state, event replay, proxy request observability. The "at most one non-terminal generation per session" invariant is enforced by the partial unique index and by CAS on `sessions.active_generation_id`; migration tests assert both. The migration runner applies in version order, is idempotent under re-run, and ships with the legacy-fixture suite from "SQLite Migration Strategy" (v1 baseline tag, v6 backfill of pre-Phase-7 sessions to `failed` with documented reasons). The orchestrator acquires the `<run_dir>/orchestrator.pid` flock before opening SQLite, writes the `orchestrator_owner` row, and refreshes `heartbeat_at` every 5 s; a second orchestrator process pointed at the same run dir fails fast, and any allocator/recovery sweep whose `orchestrator_owner.uuid` no longer matches the in-process value aborts and exits. The single helper module's API surface (claim, ack_started, completion, failure) and unit tests land in this PR; the live claim/ack call sites move onto the helper at Step 6. The 7a hot path that goes through the helper today is limited to session-create insert and existing-turn-completion writes routed through the same module, which is what keeps `runtime_generations.status` and the turn ledger in agreement during the 7a window. Migration tests assert (a) flock contention rejects the second startup, (b) tampered `orchestrator_owner.uuid` aborts the next sweep, (c) inserting a second non-terminal generation row for one session fails with the partial-index uniqueness error, and (d) the legacy-fixture suite cleanly converges after a single migration pass. | Concurrency And Storage Model, Control Plane Responsibilities, SQLite Migration Strategy, Single-Helper Contract |
| 2 | Resource allocator + reaper before any runtime change. | 1 | Generation row + resource rows allocated transactionally; host resources created before `ready`; orphan reclaim on startup; cleanup idempotent; reaper respects active leases and `recreating` allocations. | Resource Allocator And Reaper |
| 3 | Per-generation bundle, spec, and control manifest. Remove `phase2-template` from live path. | 2 | Per-generation `config.json` + control dir; static rootfs/base reused without copy; atomic `session.json` write; entrypoint validates identity + JCS digest; entrypoint resolves `anthropic_api_key` / `anthropic_auth_token` from `${SECRET_DIR}/<secret_id>` (manifest carries no plaintext credential field, no fallback to host-level Claude config); concurrent session startup writes to distinct `control_manifest_path` values. | Control Manifest |
| 4 | Per-generation network and egress. Remove `phase1-demo` from live path. | 2 | Unique netns/veth/IP/gateway/CIDR per generation; `sandbox_base_url` derived from allocated `host_gateway_ip`; egress policy applied (local proxy + configured Doris FE/BE + DNS as required by `harness.network.egress`); pre-start host-side netns probe passes; no live netns reconfigured after `runsc` attaches; no `runscSandboxNetnsName` / `runscSandboxNetnsCIDR` / `runscSandboxGatewayIP` Go constants remain in the runtime package (every value read from the allocation row); pool exhaustion surfaces as `503` with `error_class = pool_exhausted` rather than a stuck queue. | Network Profile, Network Path And Probes |
| 5 | Agent Bridge protocol over file-backed transport. | 3, 4 | Bridge exposes `hello`/`heartbeat`/`probe_network`/`claim_next_turn`/`ack_turn_started`/`emit_output`/`ack_turn_completed`; a generation cannot claim until bridge has connected, identified itself, and passed the in-sandbox probe. Bridge dir mount is declared with the runsc-equivalent of `file-access=exclusive` (current pin: `dev.gvisor.spec.mount.<dest>.share=exclusive` for `runsc release-20260511.0`); generated `config.json` is asserted to carry this annotation and the induced-crash durability test (host process killed between bridge fsync and host read) passes. | Agent Bridge Protocol |
| 6 | Turn execution via claim/ack with durable events. | 5 | `queued → leased → running (ack_turn_started) → completed (ack_turn_completed)`; stale-generation events rejected; output dedup by `(turn_id, generation_id, output_sequence)`; lifecycle/output transaction rules per Durable Event Log. | Turn Ledger, Lease And CAS Fencing, Durable Event Log |
| 7 | Cold Claude resume fallback. | 6 | Failed generation fenced; N+1 reuses `ClaudeSessionUUID` / agent home / workspace; only queued or leased-but-not-started turns retry; `ack_started_at` turns enter `unknown_after_ack_started` flow. | Cold Fallback Path |
| 8 | Durable event log + SSE replay on the existing `/api/events/stream` endpoint. | 6 | Replay-from-`last_event_id` against the global stream, retention, proxy correlation join; host assigns `event_id` (globally monotonic per orchestrator), bridge owns per-turn `output_sequence`; proxy request IDs joined via active turn registration. SSE frames emit `id:` (host event_id) and `event:` (event type) lines; server honors the `Last-Event-ID` HTTP header and the `?last_event_id=` query-string fallback; the optional `?session_id=` filter narrows replay/live frames without changing cursor semantics; retention-gap reconnects produce a single `replay_gap` synthetic event before the live tail resumes. | Durable Event Log, Proxy And Upstream Observability |
| 9 | Checkpoint-safe restore. | 7, 8 | Generation status `checkpointed` moves to `restoring` under lease (allocation_state `reserved_checkpointed → recreating`); compatible netns/veth/IP/egress/control/spec recreated; pre-restore host probe and post-restore in-sandbox probe pass before claim; any failure → generation N `failed` (allocation_state -> `reclaimable`) + cold fallback N+1. | Checkpoint Policy |
| 10 | Automatic checkpoint policy. | 9 | Triggers only on idle generation with empty turn queue, bridge checkpoint-ready, output flushed; `autoCheckpointEnabled` promoted from Go const to a per-session policy (default off during 7b validation, on for the lab profile after Step 10). The policy gates whether the next idle generation of a session is eligible for auto-checkpoint, not the in-flight generation. Checkpoint remains an executor optimization, not a correctness mechanism. | Checkpoint Policy |

## Operational Notes

- **runsc upgrades invalidate every checkpoint.** Checkpoints carry the runsc build version (Checkpoint Policy / `runsc version` exact-match). After a runsc upgrade in the lab, every `reserved_checkpointed` allocation will fail validation on next restore; this is expected, not an incident. The orchestrator marks each affected checkpoint `reclaimable` and cold-starts. Operationally: drain checkpointed sessions before upgrade if cold-start latency matters, otherwise let restore fail and fall back. There is no in-place migration path for checkpoint payloads.

## Test Matrix

Bullets are observable assertions; rules and rationale live in the linked sections above. Each bullet should fail loudly if the corresponding invariant regresses.

### DB And Lease Semantics

- Restart recovery transitions match the Turn Ledger table exactly (queued kept; expired leased without `ack_started_at` requeues with attempt+1; `ack_started_at` running is never auto-retried; terminal states never retry).
- Concurrent claim attempts on the same turn: one wins the CAS; losers do not steal the lease.
- Stale-generation events / acks / completions / checkpoint updates are rejected.
- For every turn-state transition class (claim, ack_started, completion, failure, cancel) the matching `runtime_generations.status` update commits in the same transaction; querying status after a partial-write injection always agrees with `turns`.
- A second orchestrator process pointed at the same SQLite file is rejected at startup by the `<run_dir>/orchestrator.pid` flock; on a deployment where flock is silently broken (e.g. a shared NFS-style mount), the `orchestrator_owner.uuid` meta-row check aborts the next recovery sweep / allocator commit and exits the process. Multi-process HA is Phase 8.

### Resource Allocation

- Concurrent generation startup never shares any of the uniqueness fields enumerated in Network Profile.
- `reserved_checkpointed`, `recreating`, and `reclaimable` allocations are not reused; reaper preserves resources held by an active or recreating lease; orphans are reclaimed on restart; cleanup is idempotent.
- Identity (`/30`, netns name, veth pair, control/bundle dirs) is released back to the allocator pool only when the row reaches `destroyed`. A test that holds a generation in `reclaimable` (under failed-retention) and concurrently starts a new generation asserts the new generation receives a different `/30` and a different `netns_name`.

### Generated Spec And Manifest

- Runtime spec is per-generation; rootfs/base assets are reused without per-generation copying.
- Manifest write is atomic; entrypoint never observes a partial `session.json`.
- Two sessions started concurrently write to distinct `control_manifest_path` values; neither's `session.json` is observable from the other generation's mount.
- Entrypoint reads credentials from `${SECRET_DIR}/<secret_id>` and never from manifest fields or ambient host config; a manifest carrying a plaintext credential field is rejected at validation.
- Materialized secret files have mode `0440`, owner `orchestrator`, group `harness-secret-readers`; UID `65534` is the only non-orchestrator member of that group; the in-sandbox `read()` of `${SECRET_DIR}/<secret_id>` succeeds and a host-side `read()` as any other UID fails. Bind-mount options on `secret_mount_path` include `ro,nosuid,nodev,noexec`. The mode is `0440` and not `0640`: `secret_version` is immutable post-publish (Network Profile -> "Secret version immutability"), so the orchestrator must never reopen a published file for write, and the test asserts that an attempted `chmod +w` or write-mode `open(2)` by the orchestrator user fails.
- Shell agent (`HARNESS_AGENT=sh`) generations have no `secrets/` directory materialized under their control dir, no `secret_mount_path` in the runtime spec, and no `*_secret_id` fields populated in the agent runtime profile. A manufactured-by-test shell generation whose manifest references a secret is rejected at generation-start with `error_class = shell_secret_disallowed`. (This guards the entrypoint asymmetry: `harness-agent-entrypoint` execs the shell shim as root and does not run the `setpriv --groups "$HARNESS_SECRET_READERS_GID"` drop the Claude branch uses.)
- Entrypoint rejects manifests with wrong `generation_id` / `network_profile_id` / `agent_runtime_profile_id` / `secret_version`, missing secret mount, or non-matching JCS `manifest_digest`. Rejection exits non-zero with a code distinguishable from agent crashes; host marks the generation `failed` with `error_class = manifest_digest_mismatch`.
- Re-emitting `(session_id, generation_id, manifest_version)` is a no-op; bumping `manifest_version` rotates atomically.

### Network And Egress

- Sandbox base URL is derived from `http://{allocated_host_gateway_ip}:8082`; `0.0.0.0` is never written as a sandbox base URL.
- Wrong proxy key or proxy-down fails the in-sandbox probe and prevents `claim_next_turn` (turn stays queued or generation marked failed before claim).
- Probe contract test runs against the pinned `claude-code-proxy` version: `GET /healthz` returns `200`, `POST /v1/messages` returns `401` for missing/configured-but-malformed-body keys. A divergent proxy build fails the contract test before it can ship.
- Egress policy allows the local proxy and configured Doris FE/BE hosts/ports, allows DNS only when hostnames are used, denies arbitrary public destinations.
- On 7a, every `network_profiles` row is populated with the configured `doris_fe_hosts` / `doris_be_hosts` / `doris_ports` / `dns_policy` values from `harness.network.egress` (not NULL); a corresponding `egress_policies` row materializes the matching allow-rules. A 7a-allocated row with empty Doris fields fails the test.
- CIDR pool exhaustion produces `503` with `error_class = pool_exhausted`; no `runtime_generations` row is created and no turn is left queued.

### Agent Bridge

- `hello_ack` returns `last_output_sequence_by_turn` computed from committed event-log rows only and the active `leased_turn_id`; a restarted bridge resumes from `last + 1` without gap or collision.
- Mid-streaming reconnect is exercised: a bridge interrupted while an `emit_output` batch is in-flight reconnects, receives `last_output_sequence_by_turn` lower than its locally-emitted sequence, re-emits from `last + 1`, and the host silently dedupes overlap on `(turn_id, generation_id, output_sequence)` with no warn/error logs. This test must run under load (≥100 partial deltas/turn) so that batch windows are non-empty when the reconnect fires.
- Heartbeat keeps the generation lease alive; probe failure prevents `claim_next_turn`.
- `claim_next_turn` leases exactly one eligible turn; `resume_turn` is the only path used when `hello_ack.leased_turn_id` is non-null; output dedup by `(turn_id, generation_id, output_sequence)`; host assigns durable `event_id`.
- Cross-turn / cross-generation forgery is rejected and recorded as a fenced violation; reconnect after orchestrator restart resumes from durable state.
- Bridge messages written from inside the sandbox are durably visible to the host across an induced orchestrator restart between bridge-side fsync and host-side read; the unlink-after-commit ordering makes replays a no-op.
- End-to-end turn-start latency (`POST` enqueue to `ack_turn_started`) stays under 50 ms at lab load; the test fails the build, not just a dashboard, when the budget regresses.

### Multi-Turn And Restart Recovery

- Claude and shell sessions both handle multiple turns through bridge claim/ack; active generation transitions active <-> idle without losing generation identity.
- Slow SSE subscribers do not block durable event recording; frontend reconnect replays from `last_event_id` without duplicating completed events.
- Restart during `checkpointing` leaves the generation recoverable or reclaimable; restart after `checkpointed` preserves the reserved allocation identity for restore.

### Cold Resume

- Cold fallback starts N+1 only after N is fenced `failed`; reuses `ClaudeSessionUUID`, agent home, and workspace.
- Retry eligibility is enforced by the same `claim_next_turn` CAS predicates as restart recovery — `running` turns with `ack_started_at` set are unmatched by construction and surface as `unknown_after_ack_started`, never re-leased by N+1.
- Mixed-source claim ordering: when N fails with one requeued turn and the user posts a fresh turn before N+1 starts, N+1's first `claim_next_turn` picks the lower `sequence` row regardless of which one was inserted first by wall clock. Test asserts the requeued turn (lower sequence) wins when it predates the user's new turn, and the user's new turn wins when it does not.

### Checkpoint/Restore

- Checkpoint preconditions and recorded fields match Checkpoint Policy.
- Restore moves generation status `checkpointed -> restoring -> active` while the underlying allocation_state moves `reserved_checkpointed -> recreating -> ready -> live`; pre/post probes both pass before `claim_next_turn`.
- Mismatched `runsc version` / platform / `manifest_digest` or a partial checkpoint image (verified against the recorded image manifest, not just file presence) is rejected; checkpoint row -> `reclaimable`; cold fallback starts N+1. A stale restored generation cannot write events once a newer generation exists.

### Proxy Observability

- Every model request has a `proxy_request_id` visible in both proxy logs and orchestrator events, assigned by claude-code-proxy through active turn registration (or the bridge-managed local gateway fallback).
- Event logs carry first-byte latency, total latency, retry count, timeout kind, and error class; connect / first-byte / total / stream-idle timeouts produce distinct failure events.

## Design Choices

**Bridge transport: file-backed queue.** The checkpoint-ready precondition ("no active host control request that must survive restore") rules out any transport with a live host-side handle by construction. A Unix socket bind mount and an HTTP long-poll through the sandbox gateway both require an explicit quiesce step (stop accepting, drain in-flight, close all fds) before `runsc checkpoint`, and long-poll additionally couples turn control to the per-generation gateway. The file-backed queue accepts higher latency and filesystem edge cases (rename atomicity, fsync cadence) in exchange for transparent checkpoint/restore. The protocol stays transport-neutral so a low-latency variant can be added later, but only if it implements the quiesce step and proves clean reconnect across restore. The default for the lab and for any path that participates in checkpoint stays the file-backed queue.

**Event log storage: SQLite events table.** Same database as `turns`, `runtime_generations`, and resource allocations, so durability and ordering are a single-transaction concern. Append-only per-session log files give better streaming but require custom tooling; an embedded queue adds more moving parts than this lab phase needs. Migration to Postgres is a Phase 8 deployment change, not a Phase 7 architecture change.

**Checkpoint policy rollout: feature flag, gated on the bridge.** Start with manual/test sessions, then idle threshold, then memory-pressure policy. Re-enable behind a flag only after the bridge exists.

**Manifest digest verification: JCS in both host and sandbox.** Two simpler alternatives were considered: (a) ship a `session.canonical` side-file containing the exact bytes the host hashed, so the sandbox verifies `sha256(file) == digest` without a canonicalizer, and (b) make the on-disk format already canonical (sorted keys, no whitespace) so verification is a raw byte hash. Both eliminate the vendored Python JCS dependency. JCS-in-sandbox was kept because the host already uses JCS for downstream checkpoint and audit signing where a side-file would not help, and using one canonicalizer everywhere removes a class of subtle host/sandbox divergence bugs that a side-file design recreates the moment a third consumer (e.g. a future enclave attestor) appears. The cost is one vendored library in the sandbox image; the test matrix cross-checks host JCS output against sandbox JCS output on every release build to keep them in lock-step.
