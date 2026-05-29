# Driver State and Checkpoint Fence

Driver-private state is mutable sidecar evidence. A sandbox contract records
only the state digest that a generation starts from:

```text
session_driver_states.current -> contract.driver_runtime.initial_driver_state_digest
```

The contract is immutable after allocation. Later turns update the sidecar, and
later generations snapshot the latest accepted sidecar digest into their own
contracts.

Validation is context-specific:

- Generation allocation is the first CAS boundary. In the
  `AllocateGeneration` transaction, the store validates/claims the active
  generation lease, inserts the new `runtime_generations` row, and reads the
  selected driver's current sidecar digest/version. The only missing-row
  exception is first allocation for a brand-new session/driver pair: after the
  new generation row exists, that same allocation transaction may insert the
  canonical bootstrap sidecar with `updated_generation_id` set to the new
  generation, `updated_turn_id = NULL`, and `state_version = 1`. Allocation
  returns a typed start-state token containing the selected driver, sidecar
  digest, and sidecar version for contract compilation; it does not write the
  sandbox contract.
- Contract persistence is the second CAS boundary. `StoreSandboxContract`
  runs in its own transaction after artifact rendering, validates that the
  claimed generation still owns the session lease and selected driver, and
  re-reads `session_driver_states` for that driver. The write succeeds only
  when the current sidecar digest/version still matches the allocation
  start-state token and the payload's
  `driver_runtime.initial_driver_state_digest`. A stale lease, driver
  mismatch, missing sidecar, digest/version mismatch, or existing different
  contract rejects the write; rendered artifacts are discarded or the
  generation is failed/retired by the caller.
- Ordinary v2 contract reads, restore planning, and proxy authorization validate
  the contract's digest shape and selected driver, but must not compare
  `initial_driver_state_digest` to the mutable current sidecar.
- Physical checkpoint restore validates
  `runtime_generations.checkpoint_driver_states_digest` and checkpoint metadata
  against the current sidecar before invoking runsc.

## Sidecar Row

`session_driver_states` has one current row per `(session_id, driver_id)`:

```text
CREATE TABLE session_driver_states (
  session_id TEXT NOT NULL,
  driver_id TEXT NOT NULL CHECK(driver_id <> '' AND driver_id IN ('claude_code','sh')),
  state_payload TEXT NOT NULL CHECK(state_payload <> ''),
  state_digest TEXT NOT NULL CHECK(state_digest LIKE 'sha256:%'),
  state_version INTEGER NOT NULL CHECK(state_version > 0),
  updated_generation_id TEXT NOT NULL,
  updated_turn_id INTEGER,
  updated_at TEXT NOT NULL,
  PRIMARY KEY(session_id, driver_id),
  FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE,
  FOREIGN KEY(updated_generation_id) REFERENCES runtime_generations(generation_id) ON DELETE RESTRICT,
  FOREIGN KEY(updated_turn_id) REFERENCES turns(id) ON DELETE SET NULL
);
```

9a keeps the SQL canonical-driver check hard-coded to `claude_code` and `sh`.
9b moves the vocabulary to registries; 9f widens it for `pi` only in the Pi
registration update and must preserve valid post-9a sidecar rows while doing
so. Application validation still owns full canonical JSON validation and digest
recomputation before writes.

`updated_generation_id` and `updated_turn_id` are not independent references.
Every sidecar insert, CAS update, and idempotent replay must prove the updated
generation belongs to `session_id`, selects the same `driver_id`, and still owns
the active generation lease. When `updated_turn_id` is non-null, the referenced
turn must also belong to the same `session_id` and the same
`updated_generation_id`; a turn from another session, another generation, or a
history row whose generation pointer was cleared is rejected before the sidecar
write. SQLite FKs only prove that a row exists, so this same-session and
same-generation check must be part of the store transaction.

The `session_id` foreign key owns the row lifetime. `updated_generation_id` is
evidence for the latest accepted writer, not ownership of the current sidecar
state, so generation pruning must not cascade-delete the session's current
driver state. Use `ON DELETE RESTRICT` or equivalent no-cascade behavior.
Because first allocation creates the bootstrap sidecar before the sandbox
contract is persisted, 9a must also provide explicit store operations for
failed or prunable generations referenced by `updated_generation_id`:

- `DiscardFailedBootstrapDriverState` deletes the bootstrap sidecar and the
  failed generation in one transaction only when no sandbox contract, turn,
  checkpoint, or runtime artifact has consumed that sidecar state. The owning
  session returns to the no-sidecar first-allocation state; the next allocation
  bootstraps again.
- `RefreshDriverStateEvidence` moves the sidecar's generation FK to a later
  successfully contracted generation for the same session/driver without
  changing `state_payload`, `state_digest`, or `state_version`. It is allowed
  only when that later generation's contract snapshots the same sidecar
  digest/version and still owns the session lease. This is the pruning path for
  old generations whose sidecar state remains current, including `sh`
  generations whose canonical empty sidecar may never otherwise advance.

Without one of these paths, the foreign key can make failed allocations or old
empty-state generations permanently unprunable.
Deletion-order tests must cover the exact FK shape:

- deleting a session cascades the session-owned sidecar rows without requiring
  a separate sidecar delete;
- pruning an old generation that is still referenced by
  `session_driver_states.updated_generation_id` fails until an explicit store
  operation discards a never-consumed failed bootstrap sidecar or refreshes the
  evidence anchor to a later contracted generation with the same digest/version,
  including the empty-`sh` case; and
- destructive table rebuilds preserve that ordering so `ON DELETE RESTRICT`
  does not accidentally block intended session cleanup or silently delete
  generation evidence.

Writes are compare-and-swap operations. The writer supplies the previous
digest, next canonical payload, next digest, next state version, updating
generation, updating turn, and current generation lease. The store updates only
when the previous digest still matches, the generation still owns the session
lease, the updating turn belongs to that same session/generation when present,
and `next_state_version == current_state_version + 1`.

Transition rules:

- Session creation does not insert `session_driver_states`; there is no
  generation row yet to satisfy `updated_generation_id NOT NULL`.
- First allocation for a brand-new session/driver pair creates the generation
  row, inserts the canonical bootstrap sidecar in the same transaction, and
  returns the resulting digest/version as the allocation start-state token.
  The later contract write must revalidate that token before persisting
  `driver_runtime.initial_driver_state_digest`. This bootstrap insert is
  allowed only when no sidecar exists, the session has no prior generation for
  that driver, and the session was created after the 9a destructive cutover. A
  v1-derived row that somehow survives the cutover must fail closed instead of
  using the new-session bootstrap path.
- Later v2 generation allocation reads the existing `session_driver_states`
  row and fails closed if the selected driver's row is missing.
  `sessions.claude_session_uuid` is not a source of truth and may be removed
  with the old schema.
- Claude uses a canonical bootstrap sidecar payload at first allocation for a
  newly created session.
- `sh` uses a canonical empty sidecar payload:
  `{"schema_version":1,"driver_id":"sh","state_kind":"empty"}`.
- 9f creates the canonical Pi uninitialized sidecar only during first
  allocation for a new Pi session/driver pair. Later missing Pi sidecars are
  consistency errors.

## State Digest

The sidecar payload is driver-owned canonical JSON. It must include
`schema_version` and canonical `driver_id`; aliases are not accepted in the
canonical payload.

Current Claude bootstrap payload shape:

```json
{
  "schema_version": 1,
  "driver_id": "claude_code",
  "state_kind": "claude_session",
  "claude_session_uuid": "...",
  "initialized": false,
  "last_completed_turn_id": null
}
```

After the first successful Claude Code `completed` turn, the host validator
CAS-updates the same sidecar to the initialized shape:

```json
{
  "schema_version": 1,
  "driver_id": "claude_code",
  "state_kind": "claude_session",
  "claude_session_uuid": "...",
  "initialized": true,
  "last_completed_turn_id": "<turn-id>"
}
```

Claude command selection is durable state, not process-local memory. The
fresh-session argv variant with `--session-id ${driver_state.session_uuid}` is
valid only while the accepted sidecar has `initialized: false`. The resume argv
variant with `--resume ${driver_state.session_uuid}` is valid only after a
successful completion has advanced the sidecar to `initialized: true`.
Reconnects, cold restarts, and same-process later turns must derive the
selector from the sidecar. The protocol-v1 `resume_claude` / `CLAUDE_RESUME`
projection kept during 9a-9c is derived from this field and is authoritative;
the bridge runner must not use an in-process `first_turn` flag or filesystem
probing to override the sidecar-backed selector.

`driver_state_digest_v1`:

1. Validate the payload against the selected driver's state schema.
2. Normalize enum-like strings to canonical IDs.
3. Emit deterministic JSON with lexical object keys, no insignificant
   whitespace, UTF-8 encoding, and schema-defined array ordering.
4. Prefix with `driver_state_digest_v1\n` and compute `sha256:<hex>`.

9a fixtures cover canonical `sh` empty state and Claude state. 9f adds Pi
uninitialized and initialized fixtures; Pi payload shapes and path validation
live in [pi-driver.md](./pi-driver.md).

## Turn Completion

The bridge carries sidecar changes on terminal turn completion:

```json
{
  "driver_id": "<canonical-driver-id>",
  "previous_state_digest": "sha256:...",
  "state_payload": {},
  "state_digest": "sha256:...",
  "state_version": 2
}
```

`ack_turn_completed.payload.driver_state_update` is host-only input. The bridge
processor strips it before public event persistence, logs, or replay. The host
maps it to `CompleteTurnParams.DriverStateUpdate`.

9a must include a public-event leak test: a bridge completion that contains
`driver_state_update` may update the sidecar, but the persisted
`ack_turn_completed` event and any replayed public event payload must not
contain `driver_state_update`, state payload bytes, state digests, session
files, driver UUIDs, or other driver-private state.

`CompleteTurn` validates the payload, dispatches to the selected host-side
driver-state validator, checks the current generation lease, applies sidecar
CAS, records terminal turn state, and clears active model-request context in one
transaction.

`ack_turn_completed` is the only bridge-authored success/failure commit marker
for a turn. It is persisted only inside the successful `CompleteTurn`
transaction. If driver-state validation, lease checks, sidecar CAS, or terminal
turn-state persistence fails, the host must not persist or replay
`ack_turn_completed`.

Output events emitted before `ack_turn_completed` are a durable public prefix
once committed. A later completion failure does not delete, rewrite, or
reinterpret those output events as a successful turn. The bridge processor must
convert the failed completion commit into a host-authored turn/generation
failure path:

- keep already-persisted output events visible and replayable in event order;
- mark the current turn failed with a stable error class such as
  `turn_completion_commit_failed`, `driver_state_validation_failed`, or
  `driver_state_cas_failed`;
- fail/retire the current generation, clear active model-request context, and
  make runtime resources reclaimable in a dedicated idempotent failure
  transaction;
- append the existing public generation/session failure event surface after the
  retained output prefix, without including `driver_state_update` or any
  driver-private state;
- do not automatically grant the same turn again on the failed generation.

Retries after streamed output require a new turn attempt/generation according
to the normal retry policy; they are not an implicit replay of the old
`ack_turn_completed`. Reconnect and bridge replay must use
`output_sequence` as the dedupe anchor for the failed generation too. An exact
duplicate output event for the same `(generation_id, turn_id, output_sequence)`
is ignored; a duplicate sequence with different normalized type, stream, or
payload is a protocol failure and follows the same generation-failure path.
After the generation is failed, no further `emit_output` or
`ack_turn_completed` from that generation is accepted.

If the process exits after `CompleteTurn` rejects the completion but before the
failure transaction commits, recovery must not grant more work to that
generation. The lease-expiry/reconciliation path must classify the generation
with the same completion-failure reason before it can allocate replacement
runtime work for the session.

Terminal completion is the transport point for sidecar updates, not a blanket
rule that every terminal status advances every driver. A driver-state validator
may reject `driver_state_update` for terminal statuses that driver does not
treat as durable continuity points. Pi uses this to accept sidecar advancement
only for successful `completed` turns.

Replay after a successful commit is idempotent only when the turn is already
terminal and the current sidecar row exactly matches the requested next digest,
state version, generation, and turn, and that turn still belongs to the same
session/generation. The replay exception does not advance the version again; it
accepts only the exact already-committed result. Stale digests,
skipped/non-monotonic versions, wrong generation owners, wrong turn owners, or
different next payloads fail closed.

## Checkpoint Fence

Physical checkpoints are fenced through the idle checkpoint path, not through
`CompleteTurn`.

`runtime_generations.checkpoint_driver_states_digest` is the durable fence for
new physical checkpoints. Add it as `TEXT` in the 9a schema cutover. A non-null
value is required before any post-9a.2 generation enters `checkpointing` or
`checkpointed`, and every v2 checkpoint restore must validate it.

Pre-9a checkpointed v1 generations that lack
`checkpoint_driver_states_digest` are not restorable in Phase 9. The 9a cutover
may delete or fail closed on those rows rather than backfilling state.

`BeginGenerationCheckpoint` snapshots `checkpoint_driver_states_digest` over
the selected driver's current sidecar digest set when it moves a generation
from `idle` to `checkpointing`, and stores the digest in the same CAS update.
The begin query must join/read `session_driver_states` for the canonical
`sessions.driver_id` selected by the generation contract. Missing or malformed
sidecar rows fail closed before the status transition.

`CompleteGenerationCheckpoint` verifies that the sidecar digest set still
matches `runtime_generations.checkpoint_driver_states_digest` and records
checkpoint metadata with the same value. Completion parameters must include the
begin fence value or the store must re-read it in the completion transaction;
either way, the update succeeds only when the recomputed current digest equals
the stored begin digest. `AbortGenerationCheckpoint` clears
`checkpoint_driver_states_digest` when moving the generation back to `idle`.

The fence is also copied into checkpoint artifact metadata. Phase 9 may add a
dedicated artifact field such as
`sandbox_contract_artifacts.checkpoint_driver_states_digest` or include it in a
schema-versioned checkpoint metadata artifact, but restore must have a typed
query path that returns the value independently of ad hoc payload parsing.

Restore from a physical checkpoint verifies current sidecar state against the
checkpoint metadata before using the checkpoint image. If the sidecar changed,
restore fails closed or uses a logical cold-start path that allocates a new
generation from the current sidecar digest.

Fenced restore validation gates:

- `GetRuntimeGenerationDetails` or its replacement returns
  `checkpoint_driver_states_digest` for the claimed checkpointed generation.
- The checkpoint artifact metadata contains the same
  `checkpoint_driver_states_digest` as `runtime_generations`.
- The current `session_driver_states` digest set recomputes to that value at
  restore time.
- Missing, empty, or mismatched values reject physical restore before runsc is
  invoked.

`checkpoint_driver_states_digest_v1`:

1. Read the selected driver's sidecar rows that are part of the generation
   restore contract. Phase 9 has exactly one row for `driver.driver_id`; the
   list form is reserved for future multi-driver generations.
2. Build canonical JSON:

   ```json
   {
     "schema_version": 1,
     "generation_id": "<generation-id>",
     "drivers": [
       {
         "driver_id": "<canonical-driver-id>",
         "state_version": 1,
         "state_digest": "sha256:..."
       }
     ]
   }
   ```

3. Sort `drivers` by canonical `driver_id`; emit deterministic lexical object
   keys with no insignificant whitespace.
4. Prefix with `checkpoint_driver_states_digest_v1\n` and compute
   `sha256:<hex>`.

9a fixtures cover single-driver `claude_code` and `sh`. 9f adds Pi
uninitialized and initialized checkpoint fixtures.
