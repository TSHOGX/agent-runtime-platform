# gVisor Data Agent Harness - Plan

> This is the active roadmap. Current baseline and implementation notes live in [current-status.md](./current-status.md). The checkpoint-safe refactor target is described in [checkpoint-safe-control-plane-architecture.md](./checkpoint-safe-control-plane-architecture.md).

## Phases

- [x] **Phase 0**: local LLM harness + Doris connectivity + `vhr_data` schema packaging + runtime selection
- [x] **Phase 1**: manual single sandbox DEMO with `runsc run`
- [x] **Phase 2**: scripted rootfs build, bundle bake, and restore smoke path
- [x] **Phase 3**: Go orchestrator MVP with session API, checkpoint/restore, artifact metadata, and event hub
- [x] **Phase 4**: Next.js workbench with same-origin proxy, SSE event stream, and fallback/refresh behavior
- [x] **Phase 5**: per-container `OutputHub`, stream-parser turn completion routing, and interactive shell sessions
- [ ] **Phase 6**: artifact UX hardening, live file tree, and richer previews
- [ ] **Phase 7**: checkpoint-safe control plane, runtime generations, durable turns, and verified sandbox networking
- [ ] **Phase 8**: multi-user auth, egress policy, cgroup limits, observability, and later additional harness adapters

## Current Target

The project is now past the "prove the runtime" stage. The immediate goals are:

1. Keep the current Claude Code and shell session paths stable.
2. Make session recovery independent of a single live `stdin` / PTY pipe.
3. Introduce the Phase 7 control-plane shape: runtime generations, durable turn state, explicit network profiles, and cold resume fallback.
4. Expand artifact handling from metadata listing to a more interactive file browser.
5. Defer the second harness adapter until the runtime control plane is stable.

## Current Architecture

```text
Browser
  -> Next.js same-origin proxy
  -> Go orchestrator
  -> gVisor runtime
  -> per-session workspace + long-lived sandbox
```

The browser reads live events from SSE at `/api/events/stream`. The orchestrator still keeps the WebSocket endpoint for compatibility and manual debugging.

The current runtime keeps active containers alive across turns and writes turns through attached stdin/PTY. Automatic idle checkpointing is disabled until the turn channel is moved behind a checkpoint-safe control plane.

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
- Added checkpoint/restore primitives and checkpoint session states. Automatic idle checkpointing is currently disabled because restored containers cannot reliably reconnect the attached stdin turn channel.

### Phase 4

- Added the frontend workbench.
- Added same-origin proxy routes.
- Added streaming UI state and manual retry paths.

### Phase 5

- Added `OutputHub` so each container can fan out output to multiple subscribers.
- Moved runtime output transport away from a single callback.
- Added turn completion handling for Claude stream-json frames.
- Added same-origin SSE on the frontend side.
- Added the PTY-backed shell session path and shell interrupt support.

## Phase 7 Target

Phase 7 is the new architecture phase for checkpoint-safe session recovery. It is intentionally before the "second harness adapter" work because additional adapters should not be built on the current stdin-coupled runtime boundary.

The target properties are:

- sessions can be created without immediately depending on a live container;
- old sessions can recover through cold start plus Claude resume when no healthy container is attached;
- every runtime instance has a generation ID so stale restored containers cannot write into a newer session;
- every user turn has durable state before it is sent to a sandbox;
- sandbox network configuration is explicit and verified before a turn starts;
- checkpoint/restore is re-enabled only when an idle generation can reconnect through the control plane.

Suggested delivery order:

1. Add runtime generation records and stale-generation fencing.
2. Persist explicit network profiles from `config/harness.yaml` and probe the sandbox path before sending turns.
3. Add cold resume fallback for idle sessions without live containers.
4. Add durable turn ledger and deterministic startup reconciliation.
5. Add durable event log with SSE replay.
6. Add an in-sandbox agent bridge with claim/ack turn delivery.
7. Re-enable automatic checkpoint/restore behind the bridge, with cold fallback on restore failure.

## Remaining Risks

- `runsc` network mode is sandbox by default, and the template bundle uses the fixed `/var/run/netns/phase1-demo` namespace so the local Claude proxy stays reachable at `http://10.200.1.1:8082`. The host-visible proxy URL is `http://0.0.0.0:8082`, and the lab proxy key is explicitly `123`.
- Automatic idle checkpointing is disabled in the current code because `runsc restore` cannot reliably reconnect the long-lived stdin turn channel. Checkpoint/restore must move behind the Phase 7 control plane before it is treated as the default resource-release path.
- Additional agent adapters beyond Claude Code and the shell shim need their own completion contract before they are first-class multi-turn citizens.
- Artifact browsing is still metadata-first, not file-explorer-first.
- `OutputHub` drops lines for slow subscribers by design; that is fine for UI logs but not for a forensic audit stream.

## Notes on Prior Docs

The older phase status documents remain useful as implementation history, but they are no longer the source of truth for current behavior. Use:

- `current-status.md` for the live baseline.
- `architecture.md` for system design.
- `checkpoint-safe-control-plane-architecture.md` for the Phase 7 target architecture.
- `PLAN.md` for roadmap only.
