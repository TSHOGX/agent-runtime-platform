# DataVolume Evidence

> Parent: [Sandbox contract schema v2](../sandbox-contract-v2.md).

`data_volumes` preserves the Phase 8 proof that `/workspace` and `/agent-home`
come from verified DataVolume rows and root-owned provisioning markers. The v2
contract must not collapse this proof into only `mount_plan.*.source`.

For launch and restore, the allocator must verify the matching
`session_workspaces` and `session_driver_homes` rows, canonical host paths,
layout versions, runtime identity digests, provisioning marker paths, and
provisioning marker digests before runtime creation. The verified
`data_volumes.workspace.host_path` must equal `mount_plan.workspace.source`, and
`data_volumes.agent_home.host_path` must equal `mount_plan.agent_home.source`.

`driver_home_key` is the persisted `session_driver_homes.driver` value. It must
come from an explicit allocation resolver, not from `session.Agent` and not
from public product mode. In the clean Phase 9 schema it is canonical:
`claude_code` resolves to `claude_code`, `pi` resolves to `pi`, and `sh`
resolves to `sh`.

Phase 9 treats `session_driver_homes.driver` as app-validated canonical state,
not a SQL-constrained driver slot. `DriverHomeKeyFor(driver_id)` is the only
producer for new rows, DataVolume validation rejects aliases, unknown drivers,
and empty values, and 9f adds Pi coverage for `driver_home_key: "pi"`. If 9a
implementation adds a SQL `CHECK` to this column, 9f must widen that check to
include `pi` in the same preserving schema update that widens the other
canonical-driver slots.

During the 9a cutover, existing `session_driver_homes(driver='claude')` rows
are deleted with the rest of the old runtime state. The cutover must not create a
fresh `driver='claude_code'` home beside a legacy row, and it does not preserve
old DataVolume continuity. New allocations reject `driver='claude'`.

Before deleting `session_workspaces`, `session_driver_homes`, or legacy
`sessions` rows, the cutover must copy DataVolume paths from
`session_workspaces.host_path`, `session_driver_homes.host_path`,
`sessions.workspace`, and `sessions.agent_home_path` into a bounded,
deduplicated cleanup set. For each workspace and driver-home path in that set,
the cutover must either perform best-effort filesystem cleanup under the
managed data root, verify it is already absent, or record a retained-orphan
inventory entry when the path is missing, outside the managed root,
intentionally retained, or cleanup fails. The row deletion happens only after
this cleanup/orphan decision has been made, because those rows are the old host
path inventory. Orphan inventory is operational evidence only; it does not make
the old DataVolume resumable after cutover.

This DataVolume orphan rule applies only after the 9a live-isolation cleanup
gate has passed. It is not permission to delete DB ownership for a discoverable
running sandbox or reusable provider/network identity that could not be proven
absent or durably quarantined.

The canonical host-side sandbox contract may contain DataVolume host paths and
host-only evidence paths. The sandbox-visible control manifest must not project
those fields.
