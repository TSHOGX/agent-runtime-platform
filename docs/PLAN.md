# gVisor Data Agent Harness - Plan

> This is the active roadmap. Current baseline and implementation notes live in [current-status.md](./current-status.md).

## Phases

- [x] **Phase 0**: local LLM harness + Doris connectivity + `vhr_data` schema packaging + runtime selection
- [x] **Phase 1**: manual single sandbox DEMO with `runsc run`
- [x] **Phase 2**: scripted rootfs build, bundle bake, and restore smoke path
- [x] **Phase 3**: Go orchestrator MVP with session API, checkpoint/restore, artifact metadata, and event hub
- [x] **Phase 4**: Next.js workbench with same-origin proxy, SSE event stream, and fallback/refresh behavior
- [x] **Phase 5**: per-container `OutputHub` and stream-parser turn completion routing
- [ ] **Phase 6**: artifact UX hardening, live file tree, and richer previews
- [ ] **Phase 7**: multi-user auth, egress policy, cgroup limits, observability, and a second harness adapter

## Current Target

The project is now past the "prove the runtime" stage. The immediate goals are:

1. Keep the current Claude Code multi-turn path stable.
2. Harden the runtime boundary so the lab shortcut network model can be replaced with the intended sandbox egress policy.
3. Expand artifact handling from metadata listing to a more interactive file browser.
4. Prepare the runtime abstraction for a second agent harness.

## Current Architecture

```text
Browser
  -> Next.js same-origin proxy
  -> Go orchestrator
  -> gVisor runtime
  -> per-session workspace + checkpoint images
```

The browser reads live events from SSE at `/api/events/stream`. The orchestrator still keeps the WebSocket endpoint for compatibility and manual debugging.

## What Is Done

### Phase 0

- Verified the host cannot use Firecracker because `/dev/kvm` is unavailable and nested virtualization is blocked.
- Verified Claude Code proxy / Claude Code / OpenCode local connectivity.
- Packaged `vhr_data` schema into `schema-pack/`.

### Phase 1

- Built a manual Ubuntu Noble rootfs.
- Proved `runsc --network=sandbox` can reach Doris metadata endpoints.
- Proved the sandbox can write files into the host workspace.

### Phase 2

- Added `build-rootfs.sh`.
- Added `bake-bundle.sh`.
- Added `restore-sandbox.sh`.
- Confirmed standard restore timing is in the low hundreds of milliseconds on this host.

### Phase 3

- Added the orchestrator service and SQLite metadata store.
- Added session lifecycle APIs.
- Added artifact scanning and event publication.
- Added idle checkpointing.

### Phase 4

- Added the frontend workbench.
- Added same-origin proxy routes.
- Added streaming UI state and manual retry paths.

### Phase 5

- Added `OutputHub` so each container can fan out output to multiple subscribers.
- Moved runtime output transport away from a single callback.
- Added turn completion handling for Claude stream-json frames.
- Added same-origin SSE on the frontend side.

## Remaining Risks

- `runsc -network host` is still a lab shortcut.
- Non-Claude agents need their own completion contract before they are first-class multi-turn citizens.
- Artifact browsing is still metadata-first, not file-explorer-first.
- `OutputHub` drops lines for slow subscribers by design; that is fine for UI logs but not for a forensic audit stream.

## Notes on Prior Docs

The older phase status documents remain useful as implementation history, but they are no longer the source of truth for current behavior. Use:

- `current-status.md` for the live baseline.
- `architecture.md` for system design.
- `PLAN.md` for roadmap only.
