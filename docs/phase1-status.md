# Phase 1 Status

> Date: 2026-05-19
> Scope: manual `runsc` + rootfs + single sandbox end-to-end smoke test.

## Completed

- Installed `runsc` from the Ubuntu Noble package repository after the official Google Storage binary download stalled on this host.
- Built a manual Ubuntu Noble rootfs at `sandbox-image/rootfs/` with Python, Node.js, Claude Code, `pymysql`, `pandas`, and `matplotlib`.
- Created a local Phase 1 OCI bundle under `/var/lib/harness/phase1-bundle`.
- Bound the host session workspace `/var/lib/harness/sessions/phase1-demo` into the sandbox as `/workspace`.
- Bound `schema-pack/` read-only into the sandbox as `/schema-pack`.
- Created a minimal veth/netns/NAT setup for `runsc --network=sandbox`.
- Verified a `runsc --network=sandbox` container can connect to Doris and run metadata queries.
- Verified Claude Code can run inside `runsc` with `--network=host` against the local proxy when `ANTHROPIC_BASE_URL` is set to the proxy root URL, not the `/v1` URL.

## Artifacts

The latest sandbox run wrote these files directly to the host workspace:

- `/var/lib/harness/sessions/phase1-demo/tables.csv`
- `/var/lib/harness/sessions/phase1-demo/tables.png`
- `/var/lib/harness/sessions/phase1-demo/report.md`
- `/var/lib/harness/sessions/phase1-demo/charge_query_error.txt`

## Current Blocker

The Doris account can connect and run `SHOW TABLES` / `SELECT 1`, but table data queries currently fail with:

```text
CURRENT_USER_NO_AUTH_TO_USE_ANY_COMPUTE_GROUP
```

The Phase 1 gate "Doris data -> CSV -> PNG -> report.md" is therefore only partially satisfied: sandbox networking, workspace artifact output, and metadata CSV/PNG/report generation work, but business table SELECT is blocked by Doris compute group permissions.

Needed DBA action:

```sql
GRANT USAGE_PRIV ON COMPUTE GROUP <compute_group_name> TO ro_user_batt;
```

After that grant, rerun the Phase 1 demo and replace the metadata fallback artifact with the real aggregate query output.
