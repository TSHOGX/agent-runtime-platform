# 9a: Contract and Schema Shape

> Parent: [Phase 9 implementation slices](../implementation-slices.md).

Goal: reshape Claude/shell runtime state into driver/provider-shaped slots
without changing user-visible behavior for newly created sessions.

9a and 9b are independently releasable before the 9c product-mode API. During
that window, the public request/response boundary may keep the legacy
`agent: "claude" | "sh"` shape as a compatibility adapter. That adapter must
immediately translate `agent: "claude"` to persisted
`driver_id: "claude_code"` and `agent: "sh"` to `driver_id: "sh"`, and any
legacy response `agent` field must be derived back from canonical
`sessions.driver_id`. The legacy token is not a runtime alias and must not
appear below the API DTO boundary, except as the explicit protocol-v1 sandbox
projection used by the current bridge runner until 9d.

The same boundary rule applies to config-derived defaults. `HARNESS_DEFAULT_AGENT`
and `Config.DefaultAgent` may accept the legacy value `claude` only long enough
to normalize it to the canonical default driver `claude_code`; invalid defaults
fail config validation/startup. Omitted create-session input must flow through
that normalized default and must not pass `claude` into allocation or stores.

9a uses the automatic destructive cutover for old persisted history. It may
delete old sessions, messages, artifacts, turns, events, runtime/checkpoint
rows, driver homes, sidecars, v1 contracts, and old compatibility code instead
of preserving old state. Constrained SQLite tables may be dropped, recreated, or
rebuilt directly during store-open startup, but the cutover is not a normal
single-callback migration. The current migration runner wraps each migration
function in one `BEGIN IMMEDIATE` transaction and commits only after the
callback returns; 9a cleanup needs committed marker state, non-transactional
provider/filesystem cleanup, and a later DB rebuild transaction. Therefore 9a
must add an owner-lock-protected store-open cutover coordinator around the
transaction-only migration runner. There is no preflight/apply tool, action
manifest, approval step, retained-history matrix, or reactivation workflow.

Live isolation cleanup is a separate safety gate, not an old-data preservation
gate. Before deleting DB rows that own discoverable runsc/container, netns,
veth, nft, bridge/control, bundle, checkpoint, or network identity, the cutover
must prove each live resource is already absent, clean it up, or quarantine its
identity with an active `runtime_resource_quarantine_tombstones` row that
prevents future reuse. If a discoverable live resource cannot be proven absent
or quarantined, the cutover keeps the in-progress marker, preserves the
ownership rows needed for retry, and runtime startup remains blocked.
Disposable old history and non-live DataVolume cleanup failures may be recorded
as orphan inventory and do not make pre-9a sessions resumable. After the
cutover, no pre-9a runtime continuity is supported: allocation, reconnect,
restore, proxy authorization, and driver-state bootstrap operate only on
post-cutover v2 rows.

Sub-gates:

1. **9a.1: Schema shape and canonical selector staging.** Define
   `sessions.driver_id` as the canonical selector, add it only for clean/new
   9a rows or as a nullable shadow column before destructive rebuild, add
   `sandbox_contracts.contract_schema_version`, add
   `sandbox_contracts.contract_gate_version`, add the shared v2 contract
   loader, and add request/config boundary legacy API translation. For any
   deployable build where pre-cutover rows can still exist, 9a.1 must keep a
   quarantined legacy-row selector and legacy DTO projection for those old rows
   until 9a.4 deletes or rebuilds them; new rows and all v2 contract writes use
   `sessions.driver_id`.
   This gate must not backfill `driver_id` from `sessions.agent`, must not
   enforce `NOT NULL` on a non-empty legacy `sessions` table, and must not let a
   missing nullable `driver_id` make a legacy row newly un-runnable. If 9a.1
   and 9a.4 ship atomically, the legacy-row exception is not needed.
2. **9a.2: Driver-state sidecar and checkpoint fence.** Add
   `session_driver_states`, host-only turn-completion state updates, sidecar
   CAS, and checkpoint driver-state fencing for new checkpoints.
3. **9a.3: V2 validation and projection.** Build and validate v2 payloads,
   proxy authorization snapshots, control projections, DataVolume evidence, and
   digest fixtures for new allocations.
4. **9a.4: Automatic destructive cutover.** During normal startup, run the
   store-open cutover coordinator before `OpenWithOptions` returns a usable
   store and before server construction accepts runtime work. The process has
   already acquired the orchestrator owner file lock before calling
   `OpenWithOptions`, and it passes that owner identity plus
   provider/DataVolume cleanup helpers into the store-open cutover. Store open
   writes or validates the DB `orchestrator_owner` row for that owner identity
   before marker capture, tombstone insert/validation, or cleanup-state
   operations that rely on DB-owner-checked store helpers. The file lock is the
   cross-process exclusion primitive; the DB owner row is made current inside
   store open so cutover code sees the same owner identity as later runtime
   operations. The coordinator captures cleanup inputs in a committed DB
   transaction, attempts cleanup outside that transaction for discoverable v1
   runsc/container, netns, veth, nft, bundle, bridge, control, checkpoint, log,
   workspace, and driver-home resources, requires live isolation resources to
   be absent or quarantined before their ownership rows are deleted, then opens
   a later DB transaction to delete/truncate old v1 runtime rows plus old
   `sessions`, `messages`, `artifacts`, `turns`, and `events` rows, or rebuild
   their tables into the clean v2 shape. This is the gate that enforces the final
   `sessions.driver_id TEXT NOT NULL CHECK(...)` table shape and the
   first-class quarantine tombstone table used to preserve live-resource
   uniqueness after old ownership rows are deleted.
   This step records only deterministic schema/cutover metadata; it does not
   wait for manual decisions. A non-quarantined live cleanup failure leaves the
   in-progress marker in place and prevents runtime startup.
5. **9a.5: V2-only write cutover.** Persist v2 for new allocations and remove
   or fail closed on any old v1 contract payloads only after 9a.4 has passed.

Deliverables:

1. Add canonical `claude_code` identity. `claude` is a retired legacy token and
   is rejected in runtime selectors, v2 contracts, grant allowlists, image
   manifests, and driver homes. 9a may accept or emit `agent: "claude"` in the
   temporary public API compatibility adapter described above, may accept
   `HARNESS_DEFAULT_AGENT=claude` only at config normalization, and may project
   canonical `claude_code` to sandbox-visible protocol-v1 `agent: "claude"` /
   `HARNESS_AGENT=claude` only for the current
   `harness-agent-entrypoint`/`harness-bridge-client` runner. No host-side
   runtime selector may consume that projected token.
2. Add `sessions.driver_id TEXT NOT NULL CHECK(driver_id IN
   ('claude_code','sh'))` as the final post-cutover table shape and make it the
   authoritative runtime selector for post-cutover rows during 9a. Migration
   sequencing is explicit: 9a.1 may introduce only a clean/new-row selector path
   or nullable shadow column while legacy rows still exist, and it must preserve
   the old-row selector for those legacy rows unless 9a.1 and 9a.4 are shipped
   atomically in the same release; 9a.4 deletes pre-9a `sessions` rows and
   rebuilds/enforces the `NOT NULL` selector; 9a.5 writes only v2 rows with the
   enforced selector. No gate backfills `driver_id` from legacy
   `sessions.agent`. New 9a rows are created through the request/config
   boundary adapter, which maps `agent: "claude"` to `claude_code` and
   `agent: "sh"` to `sh`. 9c adds public product `sessions.mode`; 9f widens
   this constrained slot for `pi`.
3. Add `sandbox_contracts.contract_schema_version` and
   `sandbox_contracts.contract_gate_version` for new rows. 9a writes
   `contract_schema_version: 2` and `contract_gate_version: "phase9a"`, requires
   both persisted columns to match the payload, and routes
   store/runtime/restore/proxy reads through one audited v2 loader. Existing v1
   contracts are deleted or rejected; no v1 read/auth branch remains after
   cutover.
4. Emit contract schema v2 for new writes: driver object, runtime provider
   object, `snapshot_policy`, DataVolume evidence,
   `driver_runtime.initial_driver_state_digest`, `credential_policy.digest`,
   `secret_grants[]`, and source-only `input_digests`.
5. Define 9a digest fixtures for `claude_code`, `sh`, and `local_runsc`:
   command-plan argv variants, driver config, runtime capabilities, runtime
   template, credential policy, driver state, and checkpoint driver-state
   fence. Claude Code fixtures must include both fresh-session and resume
   argv variants keyed by the persisted sidecar `initialized` state.
6. Rebuild `agent_runtime_profiles` with canonical driver IDs and generic
   model-proxy column names:

   ```text
   agent_runtime_profiles(
     agent_runtime_profile_id TEXT PRIMARY KEY,
     driver_id TEXT NOT NULL CHECK(driver_id IN ('claude_code','sh')),
     model TEXT,
     output_format TEXT NOT NULL,
     disable_nonessential_traffic INTEGER NOT NULL CHECK(disable_nonessential_traffic IN (0,1)),
     sandbox_uid INTEGER NOT NULL CHECK(sandbox_uid > 0),
     sandbox_gid INTEGER NOT NULL CHECK(sandbox_gid > 0),
     sandbox_supplemental_gids TEXT NOT NULL,
     requires_secret_drop INTEGER NOT NULL CHECK(requires_secret_drop IN (0,1)),
     model_access_allowed INTEGER NOT NULL CHECK(model_access_allowed IN (0,1)),
     manifest_model_proxy_base_url TEXT,
     model_proxy_api_key_secret_id TEXT,
     model_proxy_auth_token_secret_id TEXT,
     secret_version TEXT,
     created_at TEXT NOT NULL
   )
   ```

7. Replace `agent_runtime_profiles_tuple_uq` with an expression unique index
   that matches the allocator lookup semantics over canonical/model-proxy
   columns. A plain SQLite unique index over nullable columns is not sufficient
   because duplicate tuples are allowed when any indexed value is `NULL`.

   ```sql
   CREATE UNIQUE INDEX agent_runtime_profiles_tuple_uq
   ON agent_runtime_profiles (
     driver_id,
     COALESCE(model, ''),
     output_format,
     disable_nonessential_traffic,
     sandbox_uid,
     sandbox_gid,
     sandbox_supplemental_gids,
     requires_secret_drop,
     model_access_allowed,
     COALESCE(manifest_model_proxy_base_url, ''),
     COALESCE(model_proxy_api_key_secret_id, ''),
     COALESCE(model_proxy_auth_token_secret_id, ''),
     COALESCE(secret_version, '')
   );
   ```

   If any nullable identity column later permits the empty string as a real
   value, replace `''` with a reserved NOT NULL sentinel plus `CHECK` clauses.
   The allocator write path must change with this index. SQLite cannot target
   this expression index with the existing
   `ON CONFLICT(driver_id, model, ...) DO NOTHING` style clause. 9a must use
   one of these explicit strategies and cover it in tests:

   - query first using the exact `COALESCE(...)` lookup semantics, then insert
     only when absent and handle a duplicate-key race by re-querying;
   - use `INSERT OR IGNORE` or targetless `ON CONFLICT DO NOTHING`, then
     re-query using the exact `COALESCE(...)` lookup semantics;
   - replace nullable identity columns with generated/sentinel-backed NOT NULL
     columns and target a normal unique constraint.

   The selected strategy must be race-safe inside the allocation transaction,
   must return the existing profile ID for equivalent nullable tuples, and must
   not silently create duplicate profiles when model-proxy fields are `NULL`.
   9a rejects `pi`; 9f owns the widening update.
8. Add `session_driver_states` with compare-and-swap writes tied to the active
   generation lease. The table DDL in [driver-state.md](../driver-state.md) is
   the implementation contract. Claude and shell both write sidecar state
   through the generic path; `sessions.claude_session_uuid` is not a source of
   truth. Allocation and contract persistence use two explicit CAS boundaries:
   `AllocateGeneration` owns lease claim, generation insert, and sidecar
   read/bootstrap, returning a start-state token with selected driver,
   sidecar digest, and sidecar version; `StoreSandboxContract` owns the later
   contract insert and must revalidate that the generation still owns the
   session lease and that the current sidecar digest/version still matches the
   token and payload before persisting.
   9a must also add the explicit generation-evidence cleanup operation required
   by the `ON DELETE RESTRICT` edge from
   `session_driver_states.updated_generation_id` to `runtime_generations`.
   Failed first-allocation rows that created a bootstrap sidecar but never
   persisted a contract must be cleaned by `DiscardFailedBootstrapDriverState`,
   which deletes the never-consumed sidecar and failed generation in one
   transaction and returns the session to the no-sidecar first-allocation
   state. Pruning an older generation that still anchors current sidecar state
   uses `RefreshDriverStateEvidence`, which moves the sidecar's generation FK to
   a later successfully contracted generation for the same session/driver
   without changing `state_payload`, `state_digest`, or `state_version`. The
   pruning path must not leave an uncontracted bootstrap generation permanently
   unprunable or strand `sh` sessions whose empty sidecar may never advance.
9. Extend `ack_turn_completed.payload` and `CompleteTurnParams` with optional
   host-only `driver_state_update`. Strip it before public event persistence or
   replay, validate it through the selected host driver-state validator, and
   commit sidecar CAS with terminal turn state and active model-request cleanup.
10. Fence new physical checkpoints with
    `checkpoint_driver_states_digest`. Persist the fence on
    `runtime_generations.checkpoint_driver_states_digest`; begin stores it,
    completion verifies it, abort clears it, and restore rejects missing or
    mismatched fences before invoking runsc. Pre-9a checkpoint rows without this
    fence are deleted or rejected.
11. Add the automatic 9a destructive cutover path as an owner-lock-protected
    store-open coordinator, not a DB-only migration callback. This requires an
    explicit store startup refactor: `OpenWithOptions` accepts a Phase 9
    cutover options object, opens the SQLite handle, writes or validates the DB
    `orchestrator_owner` row from the already-acquired owner file lock, invokes
    the coordinator before returning a usable `Store`, and returns an error
    while the final schema/cutover marker is absent or
    `phase9_cutover_in_progress` is present.
    The existing `runMigration` contract remains transaction-only: no
    `migration.fn` may try to write the marker, commit, perform
    non-transactional cleanup, and re-enter the migration lock in one callback.
    Coordinator-owned transactions are the only place where the marker capture
    and final deletion/rebuild phases run. Sequencing is:

    1. Acquire the existing orchestrator owner file lock before opening runtime
       work or constructing providers, then pass the owner-lock-held startup
       context and owner identity into store open. Store open writes or
       validates the DB `orchestrator_owner` row for that identity before the
       coordinator captures markers, writes tombstones, or calls
       DB-owner-checked store helpers.
    2. Open the store with a Phase 9 cutover options object that contains
       cleanup helpers for local-runsc resources, network artifacts, bundle /
       control / checkpoint / log paths, workspace paths, and driver-home
       paths. Unit tests may inject no-op/failing helpers; production wires the
       helpers from the same config roots later used by the runtime provider.
       Runtime providers, allocation loops, restore recovery, proxy
       authorization, bridge processing, artifact watching, and HTTP handlers
       are not started until this store-open coordinator returns successfully.
    3. In the coordinator's first `BEGIN IMMEDIATE` transaction, stop new
       runtime work, load cleanup identity from
       `runtime_resource_instances.resource_identity_payload` when present,
       load `session_workspaces.host_path`,
       `session_driver_homes.host_path`, legacy `sessions.workspace`, and
       legacy `sessions.agent_home_path` into a deterministic deduplicated
       cutover cleanup set before deleting any of those rows, classify cleanup
       entries as live-isolation resources or disposable old-data paths, write a
       durable
       `phase9_cutover_in_progress` marker containing the deterministic cleanup
       set and cleanup attempt IDs, then commit that marker before any
       non-transactional filesystem/runsc cleanup.
    4. Attempt cleanup outside the DB transaction but while the owner lock is
       held. Cleanup helpers must be idempotent: if runsc/netns or filesystem
       cleanup succeeded and the process later crashes or a DB transaction rolls
       back, the next startup sees the in-progress marker, replays cleanup,
       treats already-absent resources as success, and then repeats the DB
       deletion/rebuild step. A live-isolation cleanup entry can advance only to
       `cleaned`, `already_absent`, or `quarantined`; `quarantined` is valid
       only when a corresponding active
       `runtime_resource_quarantine_tombstones` row keeps the old provider
       identity or host path unavailable to future
       allocation/reconciliation. Any other live cleanup failure keeps the
       in-progress marker and blocks runtime startup.
    5. Open the coordinator's final `BEGIN IMMEDIATE` transaction only after
       the live-isolation cleanup gate has passed. Record cleanup results,
       retained-orphan inventory, and insert or confirm every required active
       quarantine tombstone before deleting any ownership row whose uniqueness
       it replaces; delete ephemeral
       `active_model_request_contexts`; delete or truncate old `messages`,
       `artifacts`, `turns`, `events`, `sessions`,
       `session_driver_homes`, `session_workspaces`,
       `runtime_resource_instances`, `runtime_generation_resources`,
       `sandbox_contracts` / `sandbox_contract_artifacts`, `network_profiles`,
       and `runtime_generations`; rebuild constrained tables into the v2 shape;
       write the schema/cutover marker; clear the in-progress marker only in
       the same transaction that commits the clean v2 schema.

    Allocation, restore, proxy authorization, bridge processing, artifact
    watching, and runtime recovery must not start until the final
    schema/cutover marker is present and the in-progress marker is absent.
    Disposable old-data cleanup failures are recorded when practical but do not
    block the cutover solely to preserve old state. Non-quarantined live
    isolation cleanup failures do block the final cutover marker and runtime
    startup.
12. Add `runtime_resource_quarantine_tombstones` as the first-class quarantine
    guard for live-resource identities whose ownership rows are deleted. The
    table is part of the clean 9a schema, not disposable cutover scratch state:

    ```text
    runtime_resource_quarantine_tombstones(
      tombstone_id TEXT PRIMARY KEY,
      provider_id TEXT NOT NULL,
      host_id TEXT NOT NULL,
      resource_kind TEXT NOT NULL,
      identity_key TEXT NOT NULL,
      identity_digest TEXT NOT NULL,
      source_table TEXT NOT NULL,
      source_session_id TEXT,
      source_generation_id TEXT,
      source_contract_id TEXT,
      source_identity_digest TEXT,
      source_identity_payload TEXT,
      cleanup_attempt_id TEXT NOT NULL,
      quarantine_reason TEXT NOT NULL,
      quarantine_evidence_payload TEXT NOT NULL,
      quarantined_at TEXT NOT NULL,
      last_checked_at TEXT,
      released_at TEXT,
      release_evidence_payload TEXT,
      expires_at TEXT
    )
    ```

    Add an active uniqueness guard:

    ```sql
    CREATE UNIQUE INDEX runtime_resource_quarantine_active_uq
    ON runtime_resource_quarantine_tombstones (
      provider_id,
      host_id,
      resource_kind,
      identity_digest
    )
    WHERE released_at IS NULL;
    ```

    `identity_digest` is the digest of a canonical object containing
    `provider_id`, `host_id`, `resource_kind`, and the normalized
    `identity_key`; `identity_key` remains stored so cleanup/release code can
    verify the actual host resource. Required `resource_kind` values cover
    every live identity that was previously protected by
    `runtime_resource_instances` or `network_profiles` active-row uniqueness:
    `runsc_container_id`, `netns_name`, `netns_path`, `host_veth`,
    `sandbox_veth`, `host_gateway_ip`, `sandbox_ip`, `sandbox_ip_cidr`,
    `host_side_cidr`, `nft_table_name`, `control_dir_path`,
    `control_manifest_path`, `bundle_dir_path`, `spec_path`,
    `checkpoint_path`, `bridge_dir_path`, `network_hosts_path`, and
    `log_dir_path`. If a retained workspace, driver-home, or provisioning
    marker path could be selected again by a future allocator, it must also be
    tombstoned under `workspace_host_path`, `driver_home_host_path`,
    `workspace_marker_path`, or `driver_home_marker_path` instead of being only
    retained-orphan inventory.

    The final cutover transaction inserts or confirms the active tombstones for
    every `quarantined` live cleanup result before deleting the old
    `runtime_resource_instances`, `network_profiles`, workspace, or driver-home
    rows that carried the old uniqueness guarantees. If a tombstone insert
    fails, or an existing tombstone for the same identity does not match the
    same canonical identity/source evidence, the cutover keeps the in-progress
    marker, preserves ownership rows, and blocks runtime startup.

    Active tombstones have `released_at IS NULL` and `expires_at IS NULL`; they
    block reuse indefinitely. They may be released only by an explicit
    owner-lock-held reconciler operation that re-runs the same provider/path
    absence, host-root, typed-prefix, and symlink-safe checks required for
    cleanup, records `release_evidence_payload`, and sets `released_at`.
    Released rows remain as audit evidence for at least 90 days before pruning;
    normal destructive cutovers never prune active tombstones.

    Allocation and recovery enforce the table, not just the cutover. Before any
    host-side allocation effect or `runtime_resource_instances` /
    `network_profiles` insert, the allocator derives the full candidate
    identity set and rejects or regenerates any candidate with an active
    tombstone using the same normalization and digest algorithm. Restore and
    host-state reconciliation treat a host resource whose identity matches an
    active tombstone as quarantined, not adoptable or reusable. Only the release
    operation above can make that identity available again.
13. Define the no-retained-session rule. Phase 9 does not carry a retained
    pre-9a session matrix. Pre-9a sessions, message/event history, and artifact
    metadata are deleted during the cutover. The first-allocation bootstrap
    exception applies only to sessions created after the 9a cutover under the v2
    schema; no v1-derived row is treated as a resumable session.
14. Add `DriverHomeKeyFor(driverID)` and use the canonical key for
    `ProvisionSessionDriverHome`, DataVolume evidence, and mount planning. The
    default 9a cutover deletes old driver homes, so it must not auto-provision
    a new `session_driver_homes(driver='claude_code')` beside an old
    `driver='claude'` row. Old `driver='claude'` rows are deleted during the
    destructive cutover. New allocations reject `driver='claude'`.
15. Generate `/harness-control/driver/<driver_id>/` inside the existing
    read-only `/harness-control` projection. Classify every emitted control
    manifest field as strict or regenerable before cutover. Until 9d changes
    the sandbox runner to read the driver/provider projection, 9a-9c must also
    keep the protocol-v1 sandbox-visible fields that the current bridge client
    consumes (`agent`, `HARNESS_AGENT`, Claude resume/session fields, and
    traffic-disable flags). Canonical `driver_id: "claude_code"` is projected
    to sandbox-visible protocol-v1 `agent: "claude"` / `HARNESS_AGENT=claude`
    because the current runner only accepts `claude` and `sh`; canonical
    `driver_id: "sh"` is projected as `sh`. Those fields are projection-only
    compatibility fields derived from canonical `sessions.driver_id` and
    sidecar/config state; they are not new sources of truth and may be removed
    only in 9d. Claude Code
    `resume_claude` / `CLAUDE_RESUME` compatibility output must be derived from
    the sidecar `initialized` field. The current bridge runner's in-process
    `first_turn` flag and `claude_session_initialized()` filesystem probe must
    not be able to force a fresh/resume decision that contradicts the
    persisted sidecar-backed selector.

Code touchpoints:

- `orchestrator/cmd/orchestrator/main.go`
- `orchestrator/internal/agents/agents.go`
- `orchestrator/internal/config/config.go`
- `orchestrator/internal/store/migrations.go`
- `orchestrator/internal/store/store.go`
- `orchestrator/internal/store/controlplane.go`
- `orchestrator/internal/store/resources.go`
- `orchestrator/internal/store/sandbox_contract.go`
- `orchestrator/internal/store/proxy.go`
- `orchestrator/internal/bridge/filequeue.go`
- `orchestrator/internal/bridge/processor.go`
- `orchestrator/internal/server/server.go`
- `orchestrator/internal/runtime/runtime.go`
- `sandbox-image/files/usr/local/bin/harness-agent-entrypoint`
- `sandbox-image/files/usr/local/bin/harness-bridge-client`
- `docs/phase9/fixtures/*`
- `docs/phase8/fixtures/control-manifest-payload.json`

Gates:

- Schema/cutover tests prove clean databases and destructively rebuilt databases
  contain canonical `sessions.driver_id`, delete pre-9a session rows before
  enforcing the `NOT NULL` selector, keep legacy rows runnable through the
  quarantined old-row selector and legacy DTO projection in any 9a.1-only
  staging build, remove that selector/projection after 9a.4, have no
  post-cutover runtime dependency on
  `sessions.agent`, no accepted `claude` runtime selector, and an
  `agent_runtime_profiles_tuple_uq` expression index or sentinel-backed
  equivalent that rejects duplicate nullable-profile tuples. They also prove
  the clean schema contains `runtime_resource_quarantine_tombstones` with the
  active partial unique index over provider, host, resource kind, and identity
  digest.
- Runtime-profile allocator tests prove the write strategy matches the selected
  uniqueness implementation: duplicate nullable tuples with `NULL` model-proxy
  fields return the existing profile ID, no duplicate row is inserted, and the
  code does not use a conflict target that cannot match the expression unique
  index.
- API compatibility tests prove legacy create-session input is translated at
  the request/config boundary only: omitted input with
  `HARNESS_DEFAULT_AGENT=claude` and explicit `agent: "claude"` allocate
  `driver_id: "claude_code"`, `agent: "sh"` allocates `driver_id: "sh"`, invalid
  defaults fail validation/startup, any temporary 9a/9b legacy response
  `agent` field is derived from canonical `sessions.driver_id`, and no v2
  contract, sidecar, grant, image manifest, driver home, restore path, or proxy
  authorization code consumes `claude` as a runtime selector.
- Runtime selector tests prove allocation config, DataVolume provisioning,
  driver-home key resolution, runtime start requests, bridge output parsing,
  interrupt support checks, restore, and proxy authorization read canonical
  `sessions.driver_id`.
- Contract loader tests prove v2 rows validate through the shared loader and
  v1 payloads are rejected or deleted after cutover. Loader tests also prove
  missing/unknown `contract_gate_version` fails closed and persisted
  `contract_schema_version` / `contract_gate_version` columns must match the
  payload. Restore and proxy authorization tests must prove they call the
  shared loader and have no independent contract-payload parsing path; add a
  grep or static-analysis gate that fails on new ad hoc parsing in those
  packages.
- Startup cutover tests prove normal startup may perform the automatic 9a
  destructive cutover through the owner-lock-protected store-open coordinator,
  not through a single migration callback: a clean post-9a database opens, a
  pre-9a database with runtime-owned rows or old `sessions`, `messages`,
  `artifacts`, `turns`, and `events` rows is cut over without a manual action,
  old rows are gone or rebuilt into the v2 shape, injected cleanup helpers are
  invoked from the captured cleanup set, store open writes or validates the DB
  owner row from the already-held owner file lock before owner-checked marker
  or tombstone operations, `OpenWithOptions` does not return a usable store
  while the in-progress marker is present or the final cutover marker is
  absent, startup retries idempotently when cleanup succeeds but the DB
  deletion/rebuild transaction rolls back, and an injected non-quarantined live
  cleanup failure preserves ownership rows while keeping runtime startup
  blocked.
- Automatic cutover tests prove there is no preflight/apply manifest dependency:
  legacy agents, unknown/conflicting selectors, driver homes,
  active/checkpointed generations, resource identities, and session-history
  rows are handled by deterministic deletion/rebuild or live-resource
  quarantine rules, not by explicit manual choices.
- Quarantine tombstone cutover tests seed a live cleanup result for each
  tombstone kind that can lose old-row uniqueness, including runsc container
  ID, netns name/path, host and sandbox veth names, host and sandbox IP/CIDR
  values, nft table name, generation-owned host paths, and any retained
  workspace/driver-home paths that an allocator could otherwise select again.
  They prove the final cutover transaction inserts or confirms active
  `runtime_resource_quarantine_tombstones` before deleting the old
  `runtime_resource_instances`, `network_profiles`, workspace, or driver-home
  rows; a missing, conflicting, or non-idempotent tombstone preserves ownership
  rows, keeps the in-progress marker, and blocks runtime startup. Schema tests
  prove active tombstones block duplicate identities while released tombstones
  require release evidence and no longer satisfy the active uniqueness index.
- Quarantine enforcement tests prove allocation derives the full candidate
  runsc/network/path identity set before host mutation, checks active
  tombstones with the same canonical identity digest used by cutover, and
  rejects or regenerates candidates instead of inserting
  `runtime_resource_instances` or `network_profiles` rows that reuse
  quarantined identities. Restore, runtime recovery, and host-state
  reconciliation tests prove resources matching active tombstones are treated
  as quarantined and are never adopted, authorized, or considered available
  until an owner-lock-held release operation records absence evidence and
  `released_at`.
- V1 destructive-cutover tests seed live and already-absent
  `runtime_resource_instances`, prove cleanup attempts to use
  `resource_identity_payload` when available before deleting old rows, prove
  live runsc/netns/veth/nft/network cleanup must be cleaned, already absent, or
  durably quarantined before ownership rows are deleted, prove a
  non-quarantined live cleanup failure leaves the in-progress marker present,
  preserves the rows needed for retry, and blocks allocation, restore, proxy,
  and bridge processing,
  prove workspace and driver-home host paths are captured from
  `session_workspaces` / `session_driver_homes` and from legacy
  `sessions.workspace` / `sessions.agent_home_path` before row deletion, prove
  each captured DataVolume path is cleaned up, verified absent, or recorded in
  a retained-orphan inventory, prove old `sessions`, `messages`, `artifacts`,
  `turns`, `events`, runtime rows, contracts, and driver homes can be deleted
  or truncated after the live-resource safety gate, and prove the cutover still
  completes without a retained-session history or reactivation workflow.
- Sidecar DDL tests assert primary keys, foreign keys, canonical `driver_id`
  gates, not-null digest/payload/update fields, positive `state_version`,
  session-owned cascade, and non-cascading generation evidence. Deletion-order
  tests cover deleting a session, pruning an old generation that is still
  referenced by `updated_generation_id`, and destructive table rebuilds, proving
  the `ON DELETE RESTRICT` edge never unexpectedly blocks session cleanup and
  never cascades away generation evidence.
- Sidecar bootstrap tests prove session creation does not insert a sidecar,
  first allocation creates the generation row and bootstrap sidecar in one
  transaction, the bootstrap row uses that generation as
  `updated_generation_id` with `state_version = 1`, allocation returns a
  start-state token with the inserted digest/version, later allocation fails on
  a missing sidecar, pre-9a rows deleted by the cutover cannot use the
  first-allocation bootstrap exception, and `StoreSandboxContract` revalidates
  lease ownership plus sidecar digest/version before the contract snapshots the
  inserted digest. Tests must force a sidecar update between allocation and
  contract write and prove the contract write fails closed.
- Failed-bootstrap cleanup tests force a failure after first allocation creates
  a generation and bootstrap sidecar but before contract persistence, then
  prove `DiscardFailedBootstrapDriverState` deletes the never-consumed sidecar
  and failed generation in one transaction and allows a later allocation to
  bootstrap again. The same test matrix must include `sh` with the canonical
  empty sidecar.
- Sidecar evidence-refresh tests prove pruning an old generation referenced by
  `session_driver_states.updated_generation_id` first requires
  `RefreshDriverStateEvidence` to move the FK to a later successfully
  contracted generation for the same session/driver with the same
  digest/version, without mutating state bytes or version. Tests cover the
  empty-`sh` sidecar so generation pruning is not blocked forever merely
  because the sidecar never advances.
- Sidecar CAS tests cover success, stale digest, skipped or repeated
  `state_version`, wrong generation owner, wrong turn owner, missing canonical
  driver, and exact replay after commit without advancing the version again.
- `ack_turn_completed` public event tests prove persisted and replayed payloads
  never contain `driver_state_update` or driver-private state.
- Turn completion failure tests emit output before completion, then force
  driver-state validation failure and sidecar CAS failure. They prove retained
  output is replayed once, no `ack_turn_completed` is persisted, a public
  generation/session failure event follows without driver-private state, and
  the failed generation rejects later output or completion envelopes.
- Checkpoint tests prove begin persists
  `runtime_generations.checkpoint_driver_states_digest`, completion fails if
  the sidecar changes after begin, metadata must match the stored fence, restore
  queries the persisted fence, and missing/no-fence checkpoints are rejected.
- V2 validation rejects malformed driver/provider objects, missing or
  mismatched driver command/config/capability digests, missing or mismatched
  `credential_policy.digest`, non-`proxy_only` grants, non-`model_provider`
  grants, empty model grant allowlists on model-access-enabled contracts,
  selected driver/runtime-provider values absent from active model-provider
  grant allowlists on those contracts, mismatched DataVolume/resource identity
  evidence, missing driver state, incomplete bridge/network mount-plan
  evidence, and host-only fields in sandbox-visible projections. Shell/no-model
  contracts carry no model grant and do not need to appear in model-provider
  allowlists.
- V2 proxy authorization fails closed for model-provider requests when the
  selected `driver.driver_id` or `runtime_provider.provider_id` is not a member
  of the active model-provider grant allowlists. Contracts without a
  model-provider grant have no authorized model-proxy path.
- Capability/template/command-plan/config/credential/state/checkpoint digest
  fixtures cover exact canonical preimages and expected hashes, including
  Claude Code fresh-session and resume argv variants.
- Driver-home tests prove `claude_code` uses the canonical `claude_code` home
  key, old `driver='claude'` rows are deleted by the destructive cutover, no new
  canonical home is auto-created beside an old legacy home, and new allocations
  reject `driver='claude'`.
- Projection compatibility tests prove 9a-9c sandbox-visible projections and
  control manifests keep the protocol-v1 fields still read by
  `harness-bridge-client` while also emitting the new driver/provider
  projection, and that those legacy fields are derived from canonical state
  rather than `sessions.agent`. The tests must assert the chosen v1 projection
  explicitly: canonical `claude_code` reaches the sandbox runner as legacy
  `agent: "claude"` / `HARNESS_AGENT=claude`, and no v2 contract, host runtime
  selector, sidecar, grant, image manifest, restore path, or proxy
  authorization code accepts that projected value.
- Bridge-client compatibility tests exercise the current
  `harness-bridge-client` Claude runner until the 9d replacement lands: the
  sidecar-derived `resume_claude` / `CLAUDE_RESUME` projection is the only
  fresh/resume selector, and the runner's `first_turn` flag plus
  `claude_session_initialized()` filesystem probe cannot force a command-plan
  variant that contradicts persisted sidecar state.
- Claude continuity tests prove the canonical bootstrap sidecar starts with
  `initialized: false`, the first successful `completed` turn advances it to
  `initialized: true` with the turn ID, fresh-session argv is selected only for
  the uninitialized sidecar, resume argv is selected for initialized sidecars
  across same-process turns, reconnect, and cold restart, and runner-local
  `first_turn`, `CLAUDE_RESUME`, or filesystem probing cannot override the
  persisted selector.
- New contracts write schema v2 with `contract_gate_version: "phase9a"` only;
  new v1 writes and old v1 restores are rejected after cutover. Claude/shell API
  and public event tests for newly created sessions remain green.
