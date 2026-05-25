# Test Matrix

Bullets are observable assertions; rules and rationale live in the linked sections of the other phase7 docs. Each bullet should fail loudly if the corresponding invariant regresses.

## DB And Lease Semantics

- Restart recovery transitions match the [Turn Ledger restart rules](./schema.md#turns) exactly (queued kept; expired leased without `ack_started_at` requeues with attempt+1; `ack_started_at` running is never auto-retried; terminal states never retry).
- Concurrent claim attempts on the same turn: one wins the CAS; losers do not steal the lease.
- A generation for session B cannot claim, ack, complete, fail, or proxy-correlate a turn from session A; helper tests tamper `sessions.active_generation_id` and `runtime_generations.session_id` independently.
- Duplicate `claim_next_turn` with the same `request_id` returns the original grant while the lease is active; it never returns `no_work` for a turn already leased by that request.
- Stale-generation events / acks / completions / checkpoint updates are rejected.
- Session TTL expiry is enforced by the session sweep: an expired session moves to `destroyed` on the next sweep, queued work on that session is rejected before allocation, and active turn timeout still comes from `lease_ttl`, not `session_ttl`.
- A 7a generation that is otherwise idle but still served by the runtime-manager goroutine does not expire within 5 minutes; the half-life renewal keeps `lease_expires_at` moving even before the bridge exists.
- v4 adds/backfills Phase 7 session columns; v6 leaves legacy `created` sessions untouched (`status = created`, `active_generation_id = NULL`) while running/checkpointed legacy rows are fenced to `failed` with typed `error_class` / `failure_reason`.
- For every turn-state transition class (claim, ack_started, completion, failure, cancel) the matching `runtime_generations.status` update commits in the same transaction; querying status after a partial-write injection always agrees with `turns`. (See [Single-Helper Contract](./schema.md#single-helper-contract).)
- A second orchestrator process pointed at the same SQLite file is rejected at startup by the `<run_dir>/orchestrator.pid` flock; on a deployment where flock is silently broken (e.g. a shared NFS-style mount), the `orchestrator_owner.uuid` meta-row check aborts the next recovery sweep / allocator commit and exits the process. (See [Concurrency And Storage Model](./invariants.md#concurrency-and-storage-model).) Multi-process HA is Phase 8.

## Resource Allocation

- Concurrent generation startup never shares any of the uniqueness fields enumerated in [Network Profile](./schema.md#network_profiles).
- `reserved_checkpointed`, `recreating`, and `reclaimable` allocations are not reused; reaper preserves resources held by an active or recreating lease; orphans are reclaimed on restart; cleanup is idempotent. (See [Resource Allocation Lifecycle](./runtime-resources.md#resource-allocation-lifecycle).)
- Identity (`/30`, netns name, veth pair, control/bundle dirs) is released back to the allocator pool only when the row reaches `destroyed`. A test that holds a generation in `reclaimable` (under failed-retention) and concurrently starts a new generation asserts the new generation receives a different `/30` and a different `netns_name`. (See [CIDR Pool And `/30` Allocation](./network-and-probes.md#cidr-pool-and-30-allocation).)

## Generated Spec And Manifest

- Runtime spec is per-generation; rootfs/base assets are reused without per-generation copying.
- Manifest write is atomic; entrypoint never observes a partial `session.json`. (See [Control Manifest](./runtime-resources.md#control-manifest).)
- Two sessions started concurrently write to distinct `control_manifest_path` values; neither's `session.json` is observable from the other generation's mount.
- Entrypoint reads credentials from `${SECRET_DIR}/<secret_id>/<secret_version>` and never from manifest fields or ambient host config; a manifest carrying a plaintext credential field is rejected at validation.
- Materialized secret files have mode `0440`, owner `orchestrator`, group `harness-secret-readers`; UID `65534` is the only non-orchestrator member of that group; the in-sandbox `read()` of `${SECRET_DIR}/<secret_id>/<secret_version>` succeeds and a host-side `read()` as any other UID fails. Bind-mount options on `secret_mount_path` include `ro,nosuid,nodev,noexec`. The materializer refuses to publish the same `(secret_id, secret_version)` twice and never opens a published version with write flags; tests assert write-mode `open(2)` without chmod fails, but do not rely on owner `chmod +w` failure.
- Publishing an existing `(secret_id, secret_version)` never overwrites bytes: the materializer validates the existing file's mode/group and reuses it. A new secret version is a new filename referenced by a new generation; local secret-version GC/rotation policy is deferred to the Phase 8 secret store.
- Shell agent (`HARNESS_AGENT=sh`) generations have `agent = sh`, `requires_secret_drop = false`, no `secrets/` directory materialized under their control dir, no `secret_mount_path` in the runtime spec, and no `*_secret_id` fields populated in the agent runtime profile. A manufactured-by-test shell generation whose manifest references a secret is rejected at generation-start with `error_class = shell_secret_disallowed`.
- Entrypoint rejects manifests with wrong `generation_id` / `network_profile_id` / `agent_runtime_profile_id` / `anthropic_api_key_secret_id` / `anthropic_auth_token_secret_id` / `manifest_version` / `secret_version`, missing secret mount, or non-matching JCS `manifest_digest`. Rejection exits non-zero with a code distinguishable from agent crashes; host marks the generation `failed` with `error_class = manifest_digest_mismatch`.
- The runtime pins `manifest_version = 1` in the generated manifest and passes `HARNESS_EXPECTED_MANIFEST_VERSION=1` to the entrypoint; a mismatch is rejected before the agent starts. Manifest-version rotation is a future schema change, not an implicit runtime rewrite.

## Network And Egress

- Sandbox base URL is derived from `http://{allocated_host_gateway_ip}:8082`; `0.0.0.0` is never written as a sandbox base URL.
- Wrong proxy key or proxy-down fails the in-sandbox probe and prevents `claim_next_turn` (turn stays queued or generation marked failed before claim).
- The in-repo probe tests assert the orchestrator and bridge call `GET /healthz` and authenticated malformed `POST /v1/messages` with the configured accept sets. Release qualification must also run the pinned `claude-code-proxy` contract: `GET /healthz` returns `200`, `POST /v1/messages` returns `401` for missing or wrong keys, and the same route returns `400` for a well-authenticated but malformed body. A divergent proxy build fails the release gate before it can ship. (See [Probe contract](./network-and-probes.md#probe-contract-with-claude-code-proxy).)
- Egress policy allows the local proxy and configured Doris FE/BE hosts/ports, allows DNS only when hostnames are used, denies arbitrary public destinations.
- On 7a, every `network_profiles` row is populated with the configured `doris_fe_hosts` / `doris_be_hosts` / `doris_ports` / `dns_policy` values from `harness.network.egress` (not NULL); a corresponding `egress_policies` row materializes the matching allow-rules. A 7a-allocated row with empty Doris fields fails the test.
- `GET /api/quota` reports the soft session ceiling and the live pool ceiling, and the lower bound matches the allocator's rejection threshold.
- CIDR pool exhaustion produces `503` with `error_class = pool_exhausted`; no `runtime_generations` row is created and no turn is left queued.

## Agent Bridge

- `hello_ack` returns `last_output_sequence_by_turn` computed from committed event-log rows only and the active `leased_turn_id`; a restarted bridge resumes from `last + 1` without gap or collision. (See [hello_ack semantics](./bridge-protocol.md#hello_ack-semantics).)
- Mid-streaming reconnect is exercised: a bridge interrupted while an `emit_output` batch is in-flight reconnects, receives `last_output_sequence_by_turn` lower than its locally-emitted sequence, re-emits from `last + 1`, and the host silently dedupes overlap on `(turn_id, generation_id, output_sequence)` with no warn/error logs. This test must run under load (≥100 partial deltas/turn) so that batch windows are non-empty when the reconnect fires. (See [Reconnect During An emit_output Batch](./bridge-protocol.md#reconnect-during-an-emit_output-batch-primary-path-not-an-edge-case).)
- Heartbeat keeps the generation lease, current turn lease, and proxy active-context TTL alive; probe failure prevents `claim_next_turn`.
- `ack_started` writes `active_model_request_contexts.sandbox_source_ip` to the exact host address derived from the generation's `network_profiles.sandbox_ip_cidr`; a mismatched source IP fails the transaction instead of poisoning the proxy join.
- `claim_next_turn` leases exactly one eligible turn; `resume_turn` is the only path used when `hello_ack.leased_turn_id` is non-null; output dedup by `(turn_id, generation_id, output_sequence)`; host assigns durable `event_id`.
- Cross-turn / cross-generation forgery is rejected and recorded as a fenced violation; reconnect after orchestrator restart resumes from durable state.
- In-repo tests assert bridge queue writes use tmp-file fsync, atomic publish, ordered reads, and unlink-after-commit replay safety, and runtime spec tests assert the bridge mount carries the pinned gVisor `file-access=exclusive` annotation. Release qualification must also run the lab induced-crash test: a bridge message written from inside the sandbox and fsynced before a host-process restart is durably visible to the host. (See [gVisor file-access mode](./bridge-protocol.md#gvisor-file-access-mode).)
- End-to-end turn-start latency (`POST` enqueue to `ack_turn_started`) stays under 50 ms at lab load. This is a release benchmark gate: config changes that affect `harness.bridge.poll_interval`, event durability, or bridge batching require remeasurement and must fail or block deployment when the budget regresses.

## Multi-Turn And Restart Recovery

- Claude and shell sessions both handle multiple turns through bridge claim/ack; active generation transitions active <-> idle without losing generation identity.
- Slow SSE subscribers do not block durable event recording; frontend reconnect replays from `last_event_id` on the global stream without duplicating completed events. (See [SSE Wire Protocol](./bridge-protocol.md#sse-wire-protocol-step-8).)
- Restart during `checkpointing` leaves the generation recoverable or reclaimable; restart after `checkpointed` preserves the reserved allocation identity for restore.

## Cold Resume

- Cold fallback starts N+1 only after N is fenced `failed`; reuses `ClaudeSessionUUID`, agent home, and workspace.
- Retry eligibility is enforced by the same `claim_next_turn` CAS predicates as restart recovery — `running` turns with `ack_started_at` set are unmatched by construction and surface as `unknown_after_ack_started`, never re-leased by N+1. (See [User-Visible Recovery for unknown_after_ack_started](./checkpoint-restore.md#user-visible-recovery-for-unknown_after_ack_started).)
- If a lease expires while a turn is `running` with `ack_started_at` set, the generation is recoverable but not fenced until `ack_started_grace` expires or the user abandons/resubmits; reconnect within that window renews the same generation/turn lease and resumes from durable output sequence.
- Mixed-source claim ordering: when N fails with one requeued turn and the user posts a fresh turn before N+1 starts, N+1's first `claim_next_turn` picks the lower `sequence` row regardless of which one was inserted first by wall clock. Test asserts the requeued turn (lower sequence) wins when it predates the user's new turn, and the user's new turn wins when it does not.

## Checkpoint/Restore

- Checkpoint preconditions and recorded fields match [Checkpoint Policy](./checkpoint-restore.md#checkpoint-policy).
- Restore moves generation status `checkpointed -> restoring -> active` while the underlying allocation_state moves `reserved_checkpointed -> recreating -> ready -> live`; pre/post probes both pass before `claim_next_turn`.
- Mismatched `runsc version` / platform / `manifest_digest` or a partial checkpoint image (verified against the recorded image manifest, not just file presence) is rejected through the lifecycle failure CAS; generation moves to `failed`, resource rows move to `reclaimable`, and cold fallback starts N+1. A stale restored generation cannot write events once a newer generation exists. (See [Digest Equivalence Rules](./checkpoint-restore.md#digest-equivalence-rules).)

## Proxy Observability

- Every model request has a `proxy_request_id` visible in both proxy logs and orchestrator events, assigned by claude-code-proxy through active turn registration (or the bridge-managed local gateway fallback). (See [Proxy And Upstream Observability](./network-and-probes.md#proxy-and-upstream-observability).)
- Active proxy context lookup is through the localhost control-plane API, ignores expired rows, increments per-turn request_sequence atomically, and is cleaned on terminal turn and orchestrator restart.
- Event logs carry first-byte latency, total latency, retry count, timeout kind, and error class; connect / first-byte / total / stream-idle timeouts produce distinct failure events.
