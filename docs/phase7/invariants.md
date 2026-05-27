# Hard Invariants And Lifecycle Semantics

> Read this first. Every other file in `docs/phase7/` assumes these rules.

## Concurrency And Storage Model

Phase 7 explicitly assumes a **single orchestrator process per host**. The current SQLite store (`MaxOpenConns(1)`, WAL, `busy_timeout=5000`) is a single-writer transactional state machine. The lease + CAS rules below do not relax that assumption; they exist to keep one orchestrator's concurrent goroutines (HTTP handlers, idle monitor, reaper, bridge listener) from racing on the same generation, allocation, or turn, and to make crash recovery deterministic.

CAS predicates do not protect host-level resource creation (netns/veth/control dir/bundle dir). "Allocate row in DB → create host resources" is sequenced by the orchestrator process, not the SQL transaction; two orchestrators against the same `/var/lib/harness` and same SQLite file would race on `ip netns add` and veth name allocation before any CAS could fire. The single-orchestrator assumption is therefore enforced at process startup, not by the schema:

```text
1. On startup the orchestrator opens an exclusive flock on
   <run_dir>/orchestrator.pid (default <run_dir> = /var/lib/harness/run).
   The PID file is created if absent; flock is held for the process
   lifetime and released by the kernel on exit/crash. A second process
   attempting the flock fails fast with a typed error and refuses to
   open the SQLite file.

2. After the flock is held, the orchestrator writes its identity into a
   singleton meta row:
     orchestrator_owner.uuid          (random per process start)
     orchestrator_owner.boot_id       (/proc/sys/kernel/random/boot_id)
     orchestrator_owner.host_run_dir  (the run_dir whose flock is held)
     orchestrator_owner.acquired_at
     orchestrator_owner.heartbeat_at  (refreshed every 5 s)
   The row is upserted under a fixed primary key so it is unique by
   construction.

   `boot_id` is the host-reboot discriminator for startup recovery: if
   it changes, the orchestrator treats every expired lease as a hard
   fence immediately, because no pre-reboot process, mount, or socket
   can still be alive to reconnect through a grace window.

3. Every recovery sweep, allocator commit, and reaper pass reads
   orchestrator_owner.uuid and asserts it equals the in-process value.
   A mismatch (which can only happen if the flock was bypassed by an
   operator removing the PID file or pointing a second orchestrator at
   a shared NFS-style mount where flock semantics are unreliable)
   aborts the sweep and exits the process.
```

The flock is the primary defense; the meta row is for diagnostics and for catching the case where flock is silently broken (network filesystems, container bind mounts that strip locks). Multi-orchestrator HA is deferred — see [README.md](./README.md#out-of-scope-deferred-to-phase-10). The architectural rule that every critical transition is expressed as one SQL statement whose `WHERE` clause encodes the precondition holds independently of the deployment substrate; whether that statement runs against SQLite or a future Postgres deployment is a deployment detail, and the same CAS predicates carry over unchanged.

## Hard Invariants

These rules apply to every live and checkpointed generation. They are the contract every other Phase 7 doc expands on:

```text
No two live runtime generations may share a network namespace.
No live generation's network namespace may be reconfigured after runsc
  attaches.
No two live runtime generations may share a writable control manifest
  path.
No two live runtime generations may share a generated runtime spec path.
No non-destroyed resource allocation may be reused by another generation.
No control manifest at rest contains plaintext upstream credentials;
  secrets are referenced by secret_id + secret_version and read from a
  per-generation mounted file, never from ambient host Claude config or
  implicit environment variables.
A non-destroyed allocation belongs to exactly one generation. The single
  documented exception is **physical restore of the same generation_id**
  — the reserved-checkpointed -> recreating -> ready transition (see
  [runtime-resources.md](./runtime-resources.md#resource-allocation-lifecycle))
  keeps the original generation_id and therefore is not "another
  generation." Any other path that wants the resources of a non-destroyed
  allocation must wait for the allocation to reach `destroyed` and
  allocate fresh. Changing the model, agent runtime profile, egress
  policy, or any other field that requires a new generation always
  allocates a new network profile.
Every generation has isolated network resources, isolated control
  resources, and generation fencing.
Every turn claim and completion update is guarded by the active lease
  and generation fence. (Phase 7b — first effective at Step 6, when turn
  execution moves onto bridge claim/ack. See "Phase 7a vs 7b
  applicability" below.)
```

The credential invariant above is about persisted control-plane artifacts and
credential source selection. It does not mean Claude cannot see the credential
after the current entrypoint exports it for Claude Code. Phase 8 owns the
stronger runtime secrecy boundary.

### Phase 7a vs 7b applicability

The invariants above split cleanly along the 7a/7b boundary:

```text
Phase 7a (Steps 1–4) — must hold in the first 7a-complete deploy (after Step 4):
  - Per-generation netns / veth / IP / gateway / CIDR.
  - Per-generation control dir + control manifest path + runtime spec
    path.
  - Per-generation bundle dir.
  - No reuse of non-destroyed allocation identity.
  - No plaintext upstream credentials in the on-disk manifest; secrets
    read from ${SECRET_DIR}/<secret_id>/<secret_version>.
  - Single-orchestrator flock + orchestrator_owner heartbeat (Step 1).
  - Schema present for the 7b invariants (turns, leases, events tables
    and their indexes), but the helper that performs the turn-state
    CAS is *not yet on the live execution path*.

Phase 7b (Steps 5–10) — first effective when bridge claim/ack lands:
  - Every turn claim and completion update is guarded by the active
    lease and generation fence (Step 6).
  - Stale-generation events / acks are rejected by CAS predicates.
  - Cold fallback retry eligibility is enforced by the same CAS as
    restart recovery (Step 7).
  - SSE replay against the durable event log (Step 8).
  - Checkpoint-safe restore (Step 9), automatic checkpoint policy
    (Step 10).
```

7a deliberately keeps the existing stdin/PTY turn path running on top of the new per-generation resources. Step 1 lands the schema and helper only; Step 2 is the first step that can write generation rows. From Step 2 onward, the turn ledger and `runtime_generations.status` rows are written by the existing turn-execution code as a *thin record-keeping shim* (turn row inserted at user-message submit, marked `completed`/`failed` when the stream parser observes turn completion); this is correct enough that 7b's bridge can take over without a schema migration. The claim/heartbeat/ack/completion/failure protocol is not on the hot path until Step 6 lands the bridge as the executor — see Steps 1 and 6 in [implementation-plan.md](./implementation-plan.md) for what the 7a helper writes and the cutover.

During 7a the runtime-manager goroutine itself remains the lease renewer on the half-life cadence; the bridge is not yet part of the renewal path. Step 6 is where bridge heartbeat takes over the same renewal contract.

The one consequence worth calling out: a 7a deploy that crashes mid-turn cannot recover the in-flight turn from the ledger — the existing stdin path owns it. This is unchanged from the pre-Phase-7 behavior; restart recovery on the 7a ledger only reconciles `queued` rows and timestamps. 7b is what makes turn recovery durable.

The pre-Phase-7 lab violated these rules with three legacy patterns: the fixed shared netns `/run/netns/phase1-demo`, the shared writable control manifest `/var/lib/harness/control/phase2-template/session.json`, and the shared static runtime spec under `bundle/out/phase2-template-bundle/config.json` reused as live mutable state. The runtime hot path now renders per-generation netns/spec/control paths; tests assert those legacy identifiers do not reappear. Phase 7 also ships the `secret_id`/`secret_version` indirection so the manifest does not carry plaintext upstream credentials; Phase 10 wires that contract to a real secret store.

## Lease And CAS Fencing

Within the single-orchestrator scope above, lease and CAS are the correctness mechanism across goroutines and across orchestrator restarts. They are not a multi-process clustering mechanism; the database is the single owner. All critical transitions must be database-guarded by lease and generation:

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

An implementation can collapse this into one `queued -> running` CAS, but it must still claim from `lease_owner is null`, set the owner/expiry in the same statement, and check `lease_expires_at` on subsequent updates. The same pattern applies to generation activation, turn completion, failure marking, checkpoint state transitions, and event writes. In-memory locks can reduce local contention, but they are not a correctness mechanism. The canonical SQL lives in [schema.md](./schema.md#single-helper-contract).

## Generation Lease

Every `runtime_generations` row carries `lease_owner` / `lease_expires_at`. The generation lease guards transitions on the **generation and its allocations** — `starting → probing → active`, `idle → checkpointing`, `checkpointed → restoring → active` (with the underlying allocation moving `reserved_checkpointed → recreating → ready → live` in lock-step), and `failed` marking. It is independent of the per-turn lease; a generation can hold its own lease while no turn is leased.

```text
Acquired by the Runtime Manager goroutine that creates the generation
row, in the same DB transaction as the row insert.

  lease_owner = "<orchestrator_owner.uuid>:<role_tag>"
    where role_tag is a fixed string identifying the in-process role
    that owns generation transitions. Phase 7 defines exactly one
    role_tag for the generation lease: "runtime_manager".

  lease_expires_at = now + lease_ttl.

  The bridge consumer is not a separate lease owner. It is another
  execution point inside the same orchestrator process and the same
  role; on each heartbeat/inbox poll it renews under the same
  "<uuid>:runtime_manager" string the Runtime Manager goroutine used.
  CAS therefore matches whether the renewal is driven by the Runtime
  Manager goroutine or by the bridge consumer.

Renewed at half lease_ttl by any execution point in the runtime_manager
  role within the owning orchestrator process. Renewal is a CAS:
    update runtime_generations
    set lease_expires_at = :now + lease_ttl
    where generation_id = :gid
      and lease_owner = :owner
      and lease_expires_at > :now;

  The same bridge heartbeat also renews the current `turns` lease and
  the active proxy context for a leased/running turn. A turn lease is an
  execution lease, not a start-only deadline; completion is accepted
  only while heartbeat has kept that lease current.

Released by setting lease_owner = null in the same transaction that
  moves the generation row to a terminal state (failed, destroyed) or
  to generation status `checkpointed` (no live process owns the lease
  while checkpointed; the underlying allocation_state moves to
  `reserved_checkpointed` in the same transaction, and the allocation
  row may later advance through `reclaimable` to `destroyed`).

Expired (lease_expires_at <= now) means the previous orchestrator
  process crashed, stalled, or stopped receiving bridge heartbeat.
  Because lease_owner is keyed on
  orchestrator_owner.uuid (a fresh UUID per process start), a restarted
  orchestrator never matches the prior owner string under CAS, even if
  it reuses the same role_tag. The startup-recovery sweep (see
  [runtime-resources.md](./runtime-resources.md#allocation-recovery-on-startup))
  is the only caller allowed to handle an expired lease: it may renew
  the same `ack_started_at` running turn inside `ack_started_grace`, or
  fence after the relevant grace deadline.
```

`lease_ttl` defaults to 60 s; renewal cadence is 30 s. Both are config (see [implementation-plan.md](./implementation-plan.md#phase-7-configuration-schema)), not architecture, and are reported on the generation row for debuggability.

## Lifecycle States

### Session Lifecycle

```text
created
  -> accepting_input
  -> turn_running
  -> accepting_input
  -> destroyed

Any state can become failed if the session itself is not recoverable.
```

The current public statuses remain compatible for the UI: `created`, `running_active`, `running_idle`, `checkpointing`, `checkpointed`, `failed`, `destroyed`. Internally, two enums travel together but live on different rows: `runtime_generations.status` (executor instance) and `network_profiles.allocation_state` (host resource identity). They are distinct.

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

`recreating` is absent from this column: it is an *allocation* state on `network_profiles`, not a generation status. While the host is rebuilding a checkpointed generation's netns/veth/IP/control identity, the generation row's `status` is `restoring`; the underlying allocation row's `allocation_state` moves `reserved_checkpointed -> recreating -> ready` independently. Likewise, `reserved_checkpointed` is *only* an allocation state — the generation row of a checkpointed session has `status = checkpointed`, never `status = reserved_checkpointed`. Schema enums, CAS predicates, recovery sweeps, and tests must all keep this split: the migration enumerates `checkpointed` as a `runtime_generations.status` value and `reserved_checkpointed` as a `network_profiles.allocation_state` value, never the other way around.

A session in `checkpointed` accepts new turns by triggering the restore path automatically; no UI action is required. Restore-time generation statuses (`restoring`, `probing` on a previously-checkpointed session — including the window during which the underlying allocation is in `recreating`) deliberately surface as `running_active` so the UI shows "session is coming back up" rather than flapping through `checkpointed → starting → running`. The session is not accepting new turns until it returns to `running_idle`, and that gate is enforced by the turn ledger and bridge `claim_next_turn`, not by the public status string.

**Follow-up: `phase` sub-field on the session row.** Collapsing six internal states into one public `running_active` is correct for API stability but loses UX-relevant detail: a restore averages 100–200 ms while a cold start can take 1–2 s, and the UI cannot distinguish "warming up" from "resuming" in the progress indicator. A future UX/API pass can expose an additional `phase` subfield on the session-row JSON — values `cold_start | restore | live | idle | failing` — without altering the existing `status` enum or its API contract. Existing clients that ignore unknown fields keep working.

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

The cache and ledger agreement is enforced at two layers: (a) every turn-state mutation goes through one helper (see [schema.md](./schema.md#single-helper-contract)) that performs the turn CAS and the matching `runtime_generations.status` update in the same transaction (no other write path is allowed); (b) the test matrix asserts cache/ledger agreement after every transition class. SQLite triggers were considered but rejected — a trigger cannot easily express the predicate "no other leased/running turns for this generation," and silent re-entry on the trigger would obscure the fencing rules.

The important rule is:

```text
Only checkpoint an idle generation with no running turn and no unacked
output.
```

### Container Lifecycle

Current baseline (long-lived container per session, automatic checkpoint disabled) is documented in [../architecture.md](../architecture.md#runtime-flow). Target:

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
