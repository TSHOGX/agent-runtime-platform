# 9f: Pi Driver Integration

> Parent: [Phase 9 implementation slices](../implementation-slices.md).

Goal: add Pi as a registered driver through the established 9a-9e contracts.

Deliverables:

1. Add rootfs build support for `SANDBOX_AGENT_DRIVERS=pi`.
2. Add a 9f schema update that widens every SQL-constrained canonical-driver
   slot introduced by 9a to include `pi`:
   `sessions.driver_id`, `agent_runtime_profiles.driver_id`, and
   `session_driver_states.driver_id`. This is a preserving schema-widening
   migration, not a release reset: valid post-9a/9c/9e Claude Code and shell
   rows, including active and checkpointed sessions, must survive the rebuild.
   If SQLite requires table rebuilds to widen `CHECK` constraints, the migration
   must copy valid canonical rows into the rebuilt tables inside the migration
   transaction and keep foreign keys consistent. The migration must still reject
   aliases and empty values in the resulting schema and be the only path that
   makes `pi` insertable in those tables.
   `session_driver_homes.driver` is not a SQL-constrained canonical-driver slot
   in the Phase 9 plan; it remains app-validated through `DriverHomeKeyFor` and
   DataVolume allocation/validation. 9f therefore does not widen a SQL `CHECK`
   on that column unless 9a adds one during implementation. If 9a does add a
   SQL `CHECK` for `session_driver_homes.driver`, this 9f widening gate must
   include it in the same preserving migration before Pi is selectable.
3. Record Pi image-manifest facts, commit pinned Pi release evidence fixtures,
   and pass the generic 9c image gate.
4. Generate and materialize Pi config as specified in
   [pi-driver.md](../pi-driver.md).
5. Add the long-lived Pi RPC `AgentRunner` and `OutputNormalizer`.
6. Declare Pi Phase 10 support modes; unsupported features reject explicitly.
7. Wire Pi sidecar bootstrap, successful-completed-turn sidecar advancement,
   failed/canceled no-advance handling, and cold-restart validation through the
   generic 9a driver-state APIs.

Gates:

- All Pi-specific gates in [pi-driver.md](../pi-driver.md) pass.
- Pi materialization contract tests prove generated
  `/harness-control/driver/pi/models.json` and `settings.json` are the source
  projection, the v2 contract records exact read-only file-bind entries for
  `/agent-home/.pi/agent/models.json` and
  `/agent-home/.pi/agent/settings.json`, `driver_runtime.materialized_driver_config`
  records matching source projection paths and digests, and the sandbox UID/GID
  cannot create, replace, rename, unlink, chmod, or write either config
  pathname. Tests must reject copied files, writable binds, missing
  materialization evidence, symlink materialization inside a sandbox-writable
  parent, and digest/source mismatches before Pi startup, before turn submit,
  and before reconnect/cold-restart attach.
- Pi writable-state tests prove writable Pi state is limited to declared
  writable subpaths under `/agent-home/.pi/agent`, including the session
  directory, while the two config pathnames stay read-only and
  `/harness-control` remains read-only.
- Pi schema update tests start from a clean already-9a/9c/9e database
  containing valid Claude Code and shell sessions, messages, artifacts, runtime
  profile rows, runtime generations, and sidecar rows. They prove existing rows
  are preserved, `pi` can be inserted into `sessions.driver_id`,
  `agent_runtime_profiles.driver_id`, and `session_driver_states.driver_id`
  only after the 9f schema update, and runtime profile uniqueness, sidecar
  primary keys, foreign keys, and `foreign_key_check` remain green after any
  table rebuild.
- Driver-home validation tests prove `DriverHomeKeyFor("pi") == "pi"`,
  Pi DataVolume evidence maps to `driver_home_key: "pi"`, and
  `session_driver_homes.driver` rejects legacy aliases, unknown drivers, and
  empty values through the app validation path. If implementation adds a SQL
  `CHECK` to `session_driver_homes.driver`, the schema-widening tests above
  must also prove that check admits `pi` only after 9f.
- 9f is not allowed to delete valid post-9a/9c/9e rows for convenience. Any
  future release-reset alternative must be a separately named gate that deletes
  active/checkpointed sessions, message history, artifacts, runtime rows, and
  sidecars explicitly before enabling Pi.
- Pi cannot be selected until the 9c image gate and 9e strict grant gate pass.
- Pi cannot be selected until the 9f schema-widening gate has run and the
  registered Pi `DriverSpec` is accepted by the selected `RuntimeProviderSpec`.
- Pi cannot be selected from `/latest` documentation assumptions alone; the
  exact CLI version, event schema, RPC/session behavior, and normalizer corpus
  must be checked in as versioned release evidence.
- Pi-backed proxy authorization cannot come from public `mode` labels or
  legacy `agent == "claude"` checks.
- Pi failed/canceled terminal acknowledgements record terminal turn state only
  when they omit `driver_state_update`; any failed/canceled acknowledgement that
  includes a Pi state update fails closed and does not mutate the sidecar.
- Claude/shell functional gates remain green after Pi registration.
