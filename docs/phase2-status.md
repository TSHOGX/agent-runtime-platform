# Phase 2 Status

> Date: 2026-05-19
> Scope: scripted rootfs build, OCI bundle baking, checkpoint/restore sandbox startup.

## Completed

- Added `sandbox-image/build-rootfs.sh`.
  - Reuses the existing `sandbox-image/rootfs/` by default.
  - Rebuilds from Ubuntu Noble with `FORCE=1`.
  - Installs Python, Node.js, Claude Code CLI, `pymysql`, `pandas`, and `matplotlib`.
  - Copies versioned harness files from `sandbox-image/files/` into the rootfs.
- Added `bundle/bake-bundle.sh`.
  - Generates an OCI `config.json` under `bundle/out/phase2-template-bundle/`.
  - Mounts `/var/lib/harness/sessions` as `/sessions`, a host control dir as `/harness-control`, and `schema-pack/` read-only.
  - Starts a template `runsc` sandbox and checkpoints it while it waits for control input.
- Added `bundle/restore-sandbox.sh`.
  - Writes per-session runtime env into the host control file.
  - Restores a fresh sandbox from `bundle/checkpoints/phase2-template/`.
  - Records restore timing in `/var/lib/harness/sessions/<session_id>/restore_ms.txt`.
- Added rootfs entrypoint source files under `sandbox-image/files/usr/local/bin/`.
  - `harness-agent-entrypoint` waits for control input, binds `/workspace` to `/sessions/<session_id>`, then runs the selected agent.
  - `phase1_demo.py` is now versioned outside the ignored generated rootfs.

## Verified

Smoke commands:

```bash
sandbox-image/build-rootfs.sh
bundle/bake-bundle.sh
HARNESS_AGENT=sh \
HARNESS_COMMAND='echo phase2-ok > /workspace/ok.txt' \
SESSION_ID=phase2-smoke \
bundle/restore-sandbox.sh
```

Result:

- `/var/lib/harness/sessions/phase2-smoke/ok.txt` was written from inside the restored sandbox.
- Latest standard restore timing observed: `139 ms`.

## Important Notes

- The installed `runsc` is `0.0~20230807.0`.
- This version supports `checkpoint` / `restore`, but does **not** expose the planned `--warm-sentry` flag.
- `runsc checkpoint` on this host does not support `--network=host`, so Phase 2 defaults to `RUNSC_NETWORK=sandbox`.
- `runsc restore` failed with the default `overlay2=root:self`, so Phase 2 explicitly uses `RUNSC_OVERLAY2=none`.
- The `<100 ms` warm restore gate should be re-tested after upgrading `runsc` to a build with warm sentry support. Current standard restore is close but above target.

## Next Step

Phase 3 can now consume these scripts as the runtime boundary:

- `build-rootfs.sh` prepares the base filesystem.
- `bake-bundle.sh` prepares the reusable checkpoint.
- `restore-sandbox.sh` proves the restore path and session workspace contract before moving this logic into Go.
