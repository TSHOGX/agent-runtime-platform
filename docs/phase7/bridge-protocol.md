# Agent Bridge Protocol

The bridge is the boundary between the host control plane and the in-sandbox agent. It owns the durable claim/ack semantics that survive container checkpoint and restore, and it is the producer side of the durable event log that drives SSE.

## Wire Protocol

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

`probe_network()` is the post-start / post-restore in-sandbox probe and must pass before `claim_next_turn()`. The host can run only the pre-start netns probe on its own; it cannot prove agent-visible config inside the sandbox. See [network-and-probes.md](./network-and-probes.md#probes).

`heartbeat()` renews both leases owned by the live bridge path: the generation lease and, when a turn is leased/running, that turn's lease plus its proxy active-context TTL. Long Claude turns therefore remain completable past the default 60 s lease as long as bridge heartbeat is healthy.

## Message Envelope

Every queued JSON file carries one envelope:

```text
message_id     -- unique per file write
request_id     -- stable RPC correlation id; responses echo the request's value
type           -- hello | hello_ack | probe_network | claim_next_turn | grant | no_work | error | ...
session_id
generation_id
turn_id        -- optional; present when the message is turn-scoped
payload
```

Request files set a fresh `message_id`. Response files echo the caller's `request_id` and use a response `type`. `claim_next_turn` returns one of three responses:

- `grant`: includes the leased `turn_id` and the lease metadata needed to start work.
- `no_work`: there is no eligible turn, or the generation is fenced/busy.
- `error`: includes `error_class` and `error`.

A duplicate `grant` for the same `request_id` is idempotent: the host must not advance the lease twice, and the bridge treats repeated delivery as a no-op.

The host persists `claim_request_id` on the leased turn before writing the `grant`. If the host crashes after the DB claim but before reliable grant delivery, replaying the same `claim_next_turn` request_id returns the original `grant`; it must not return `no_work` for that already-leased turn.

## Transport Layout

The transport is a per-generation directory shared between host and sandbox: `<bridge_root>/<generation_id>/{inbox,outbox,heartbeat}`. `bridge_root` is the derived `<run_dir>/bridge` root from [implementation-plan.md](./implementation-plan.md#phase-7-configuration-schema); it is not a separate config key. Queue names are written from the bridge's perspective: `inbox/` is what the bridge receives (host writes here, bridge reads), `outbox/` is what the bridge sends (bridge writes here, host reads). Both queues use `tmp/<uuid> -> <queue>/<seq>.json` atomic rename plus `fsync` on the destination directory, identical to the control-manifest contract. The reader on each queue processes files in `seq` order and unlinks after the message is persisted or applied — bridge unlinks `inbox/` files; host unlinks `outbox/` files.

`seq` is a 20-digit zero-padded unsigned decimal (`00000000000000000001.json`). Readers sort by parsed numeric value, not by filesystem iteration order; zero padding makes lexical order match numeric order for diagnostics. Each queue has exactly one writer: host for `inbox/`, bridge for `outbox/`. On writer start/restart, scan existing `*.json`, parse valid decimal basenames, and set `next_seq = max(existing_seq) + 1` or `1` when the queue is empty. A writer must never overwrite an existing target; use `renameat2(RENAME_NOREPLACE)` or an equivalent no-replace reservation. If the target already exists, rescan and retry with `max + 1`. Invalid filenames are ignored by the protocol reader and surfaced as a health error for the owning generation.

Concretely:

```text
inbox/    host -> bridge   (claim grants, resume_turn, host control messages)
outbox/   bridge -> host   (ack_turn_started, emit_output, ack_turn_completed,
                            generation status, failure marks)
```

Heartbeats are written as `heartbeat/bridge` (bridge-side) and `heartbeat/host` (host-side) by overwriting via tmp+rename; liveness is judged by file `mtime` polled at the heartbeat cadence. No inotify dependency.

### Host consumer ordering

For any bridge message that drives a turn-state transition (`ack_turn_started`, `ack_turn_completed`, completion failure, generation status changes):

```text
read outbox file
  -> DB transaction (turn-state CAS + durable event append)
  -> in-memory hub publish (best-effort, after commit)
  -> unlink outbox file
```

Bridge consumer ordering for any host message in `inbox/` (claim grants, resume directives) is the mirror image: read, apply locally (claim a turn, start agent execution), then unlink. The unlink is always last on both sides.

Unlink is intentionally last. A host crash between commit and unlink replays the same `outbox/` file on restart; the CAS predicate makes the replay a no-op, and the `(turn_id, generation_id, output_sequence)` dedup or the event's `dedupe_key` rejects the duplicate event. Bridge-side at-least-once delivery is therefore the assumed semantics, and the durable schema is what makes idempotency hold. The same is true for `inbox/`: a bridge crash between applying a host directive and unlinking is recovered via the bridge's `hello` flow on reconnect (`hello_ack.leased_turn_id` tells the bridge whether the directive landed), so a duplicate `inbox/` file is detectable as already-applied work.

### gVisor file-access mode

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

(Annotation key spelling tracks the runsc release pinned in [../runsc-warm-sentry-research.md](../runsc-warm-sentry-research.md); the generator emits whatever the pinned binary documents and the test matrix asserts the annotation is present and equals the binary's "exclusive" token.) Shared-mode mounts have known fsync-propagation quirks where the gofer batches metadata back to the host and an `fsync` on the bridge side does not guarantee the rename is durable on host fs. The failure mode is silent: lifecycle messages appear written from the bridge's view, but a host crash before the gofer flushes loses them — the host never reads the file, the turn never transitions, the user sees a stuck queue with no error log. Because the failure is invisible at write time, the test matrix asserts the cache mode at config-emission time and runs an induced-crash test that fsyncs from inside the sandbox and asserts host visibility after a host process restart. Single bridge process per generation makes `exclusive` safe; if a future design ever runs two writers on the same bridge dir, this contract must be revisited together with the gVisor docs for the runsc rev in use.

### `hello_ack` semantics

`hello_ack`'s `last_output_sequence_by_turn` is computed by the host as `MAX(output_sequence)` over the durable event log filtered by `(session_id, generation_id, turn_id)` for every non-terminal turn this generation owns. Only **committed** event-log rows are visible; in-flight `emit_output` batch transactions are not. The committed boundary therefore lags the bridge's locally-observed last-emitted sequence by up to one batch window — this is by construction (see Idempotency And Sequence Recovery for why this lag is the protocol's primary reconnect path, not an edge case). The bridge's local view is discarded on reconnect; it must trust the host-returned `last_output_sequence_by_turn` and re-emit anything past it.

End-to-end turn-start latency budget (claim observed in `inbox/` to `ack_turn_started` durable in events): under 50 ms at lab load. The default `harness.bridge.poll_interval` is sized to leave room for the host's durable write path and `emit_output` batching; if that interval is raised, this budget must be remeasured, not assumed.

## Idempotency And Sequence Recovery

Every bridge message carries `session_id`, `turn_id`, `generation_id`, and (for output) `output_sequence`. The bridge owns only per-turn `output_sequence`; the host event store assigns the global `event_id` after dedupe. Stale-generation events are rejected. Duplicate output is deduplicated by `(turn_id, generation_id, output_sequence)`; lifecycle messages are made idempotent by their CAS predicates per Transport Layout.

A bridge process can crash and restart while the sandbox is still alive. On restart it has no memory of the next `output_sequence`, which would either collide or skip. The bridge therefore re-runs `hello`, applies the `last_output_sequence_by_turn` returned by the host, resumes each non-terminal turn from `last + 1`, and uses `resume_turn` for the leased turn instead of `claim_next_turn`.

### Reconnect During An `emit_output` Batch (primary path, not an edge case)

`emit_output` batches and lifecycle acks have different transaction boundaries (see [schema.md](./schema.md#events): "Event durability is a hard invariant, but the transaction boundary is per message kind, not per turn"). A burst of partial deltas from Claude stream-json is committed in one batch transaction; an `ack_turn_started` / `ack_turn_completed` is committed in its own transaction together with the turn-state CAS. When a bridge reconnect lands between the two — or anywhere inside a batch window — the host's committed boundary trails the bridge's local "last emitted" by up to the size of the in-flight batch. This is the **expected** state on every reconnect during streaming output, not a corner case.

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

## Turn Completion

Claude completion: stream-json `result success`, `result non-success`, or `error`. Shell completion: `harness.turn_done`. Completion is written to the durable turn ledger before the session is marked idle.

## SSE Wire Protocol (Step 8)

Step 8 promotes the existing global SSE endpoint at `/api/events/stream` from `data:`-only frames to a typed protocol with `id:` (host event_id) and `event:` (event type) lines. The frontend reconnects with `Last-Event-ID` and the server replays missed events from the durable event log.

### Why a global stream

The orchestrator exposes one global SSE endpoint that the frontend opens once per browser session and demultiplexes client-side. The optional `?session_id=` filter is for narrow views only; it is not a portable cursor namespace (`orchestrator/internal/server/server.go:523`, `frontend/components/harness-provider.tsx:459`). The workbench keeps the global stream open so sidebar updates for other sessions do not require reconnecting or re-seeding cursor state.

For the global stream to work, **`event_id` must be globally monotonic per orchestrator process**, not per session. The host event store assigns `event_id` from a single sequence; the SQL is `INSERT INTO events (...) RETURNING event_id` against an `INTEGER PRIMARY KEY AUTOINCREMENT` column under the orchestrator's single-writer SQLite. `Last-Event-ID` on the global stream is therefore one cursor that survives session selection changes. A cursor captured under one `?session_id=` filter may only be reused with the same filter; widening to a different filter, or to the global stream, starts from a fresh cursor.

### Phase 7a vs 7b scoping

Phase 7a lands the `events` table and indexes only (Step 1) and continues to use the existing `data:`-only SSE writer in `orchestrator/internal/server/server.go` and the existing `EventSource(url)` consumer in `frontend/components/harness-provider.tsx`. The typed `id:`/`event:` wire format, `Last-Event-ID` handling, the `?last_event_id=` query-string fallback, and the `replay_gap` synthetic event all land at **Step 8 (Phase 7b)** together with the replay support and retention enforcement on the existing global `/api/events/stream` endpoint, since they require the bridge to be the executor and the host event store to be the source of truth for `event_id`. The contract below is the Step 8 deliverable; it is written up front because Step 1 must already allocate `event_id` as the cursor.

### Wire format

Every frame the server emits carries an `id:` line whose value is the host-assigned `event_id`:

```text
id: 482917
event: emit_output
data: {"session_id":"…","turn_id":"…","output_sequence":17,"payload":{…}}

id: 482918
event: ack_turn_completed
data: {"session_id":"…","turn_id":"…","status":"completed"}
```

Every `data:` JSON payload carries `session_id` so client-side demultiplexing on the global stream is unambiguous. The `event:` line carries the `events.type` value so the browser's `EventSource.addEventListener('emit_output', …)` form works for typed handlers. Frames without a meaningful event type still carry `id:`; clients without typed handlers fall through to the default `message` listener.

### Resume on reconnect

The browser's native `EventSource` automatically sends `Last-Event-ID: <event_id>` on reconnect, so the server treats `Last-Event-ID` as the authoritative cursor against the global sequence. Some intermediaries (corporate proxies, the user's edge proxy fronting the orchestrator) strip the header; for those the client also accepts `?last_event_id=<event_id>` as a query-string fallback, used by the `harness-provider` frontend code path that is not the raw `EventSource`. When both header and query are present, header wins. The `?session_id=` filter narrows which frames the client receives, but its cursor is only valid for the same filter value that produced it.

```text
GET /api/events/stream
  (open new global stream; first frame is the next event after connect)

GET /api/events/stream
Last-Event-ID: 482917
  (replay events with id > 482917 in monotonic order, then continue live)

GET /api/events/stream?session_id={id}
Last-Event-ID: 482917
  (replay only this session's events with id > 482917, then continue live;
   safe only if 482917 came from the same `session_id` filter)

GET /api/events/stream?last_event_id=482917
  (header-stripped fallback, same semantics)
```

The global cursor is one integer per orchestrator; filtered cursors stay scoped to their original `session_id` filter.

### Retention gap

The event log has finite retention (configured per `harness.events.retention`, default 24 h or N rows whichever first). If a client resumes with a `last_event_id` older than the oldest retained row, the server cannot replay losslessly. The defined response is:

```text
id: <oldest_retained_event_id - 1>
event: replay_gap
data: {"requested_last_event_id": 482917, "oldest_available": 600000,
       "session_id_filter": "…",
       "reason": "retention_window_exceeded"}
```

`session_id_filter` echoes the active `?session_id=` filter (or null if no filter is in effect). The server resumes from the oldest retained event matching the filter; it never reuses a filtered cursor across a different filter scope. The frontend treats `replay_gap` as a directive to drop its in-memory hub state and refetch via `/api/sessions` (and per-session `/api/sessions/{id}` for the currently-selected session) and the polling endpoint, then re-attach the SSE stream from the gap event's id forward. This is also the contract for the rare case where a client's first event is older than retention because of an unusually long-lived idle session — the gap event still fires.

### Frontend implementation note (Step 8)

`frontend/components/harness-provider.tsx` currently constructs `new EventSource(buildEventsStreamUrl())` without retaining the last seen id, and the orchestrator handler in `orchestrator/internal/server/server.go` writes `data:` frames only. Step 8 updates both: the server's SSE writer emits `id:` and `event:` lines (and a `replay_gap` synthetic event when needed), and the provider tracks the last seen id (a single integer in the global cursor space) to populate the query-string fallback when the browser does not send `Last-Event-ID` on the wire (e.g., across a navigation that recreates the EventSource). Tests assert (a) reconnect after a server restart resumes from `Last-Event-ID + 1` with no duplicates and no gaps across all sessions in the stream, (b) reconnect with a stale id past retention triggers exactly one `replay_gap` event followed by live tail, (c) typed listeners on the client receive `emit_output` frames as that event type, not as the default `message` channel, and (d) lifecycle frames for non-selected sessions arrive on the global stream and the sidebar's session list re-converges from those frames after a transient disconnect.
