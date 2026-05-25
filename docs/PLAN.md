# gVisor Data Agent Harness - Plan

> This is the active roadmap. Current baseline and implementation notes live in [current-status.md](./current-status.md). The checkpoint-safe refactor target is described in [phase7/README.md](./phase7/README.md).

## Phases

- [x] **Phase 0**: local LLM harness + Doris connectivity + `vhr_data` schema packaging + runtime selection
- [x] **Phase 1**: manual single sandbox DEMO with `runsc run`
- [x] **Phase 2**: scripted rootfs build, bundle bake, and restore smoke path
- [x] **Phase 3**: Go orchestrator MVP with session API, checkpoint/restore, artifact metadata, and event hub
- [x] **Phase 4**: Next.js workbench with same-origin proxy, SSE event stream, and fallback/refresh behavior
- [x] **Phase 5**: per-container `OutputHub`, stream-parser turn completion routing, and interactive shell sessions
- [x] **Phase 6**: artifact UX hardening, live file tree, and richer previews
- [x] **Phase 7a**: control-plane skeleton — per-generation resources, durable schema, per-generation network and bundle, no shared `phase1-demo` / `phase2-template` state.
- [x] **Phase 7b**: turn execution refactor — Agent Bridge claim/ack, durable turn ledger with `ack_started_at` semantics, durable event log, cold Claude resume, checkpoint-safe restore, and checkpoint policy.
- [ ] **Phase 8**: multi-user auth, secret rotation, egress policy enforcement, cgroup limits, observability, multi-orchestrator HA, and later additional harness adapters

## Current Target

The project is now past the checkpoint-safe control-plane refactor. The immediate goals are:

1. Keep the current Claude Code and shell session paths stable.
2. Keep Phase 7 release gates green on the lab host, including the pinned proxy contract, gVisor bridge durability lab, secret permission lab, and live turn-start latency gate.
3. Harden production operations around Phase 7 resources: retention windows, cleanup observability, credential rotation, and egress enforcement.
4. Keep artifact browsing stable while the runtime control plane changes underneath it.
5. Defer the second harness adapter until the Phase 7 operational surface is stable.

## Current Architecture

```text
Browser
  -> Next.js same-origin proxy
  -> Go orchestrator
  -> gVisor runtime
  -> per-session workspace + long-lived sandbox
```

The browser reads live events from SSE at `/api/events/stream`. The orchestrator still keeps the WebSocket endpoint for compatibility and manual debugging.

The current runtime keeps active generation sandboxes alive across turns and routes user work through the Agent Bridge claim/ack protocol. Turns, lifecycle events, output, proxy request correlation, and generation/resource state are durable in SQLite before in-memory publish.

## What Is Done

Phases 0–6 are complete. Highlights below; full per-phase notes live in [current-status.md](./current-status.md).

- **Phase 0–2**: host capability check (no `/dev/kvm`), `vhr_data` schema packaging, manual rootfs + bundle bake + `runsc` smoke path; standard restore latency in low-hundreds of ms.
- **Phase 3–4**: Go orchestrator with SQLite store, session API, artifact scanning, event hub, checkpoint/restore primitives; Next.js workbench with same-origin proxy and SSE.
- **Phase 5**: per-container `OutputHub` fan-out, stream-json turn completion, PTY-backed shell sessions with interrupt support.
- **Phase 6**: artifact serving hardened (traversal/symlink/non-regular rejection, `artifact.deleted` events), live frontend file tree with search and per-file download, richer previews for Markdown/code/text/images/JSON/CSV/PDF.
- **Phase 7**: per-generation resources and network, strict Phase 7 config schema, immutable mounted secrets, Agent Bridge claim/ack turn execution, durable event log/SSE replay, proxy request correlation, cold fallback, checkpoint-safe restore, checkpoint policy, and release-gate evidence.

## Phase 7 Target

Phase 7 is the architecture phase for checkpoint-safe session recovery. It came before any "second harness adapter" work because additional adapters should not be built on a stdin-coupled runtime boundary.

It was split into two delivery slices because the work was large enough that landing it as one phase risked long-lived branches and stdin/bridge dual-write detours. **7a** removed shared `phase1-demo` / `phase2-template` state and gave every generation its own resources. **7b** moved turn execution onto the Agent Bridge and put checkpoint/restore behind the durable control plane.

The target properties (per-generation isolation, generation fencing, durable turn ledger, claim/ack, cold resume on non-started turns only, checkpoint-safe restore) are specified as Hard Invariants in [phase7/invariants.md](./phase7/invariants.md#hard-invariants); this document does not restate them.

### Phase 7a: control-plane skeleton

0. Add the Phase 7 config schema (`harness.run_dir`, `harness.session_ttl`, `harness.max_sessions`, `harness.network.cidr_pool`, `harness.network.egress.*`, `harness.events.*`, `harness.probe.*`, `harness.bridge.*`, `harness.reaper.failed_retention`, `harness.secrets.*`) to the loader with defaults and validation, per the architecture doc's "Phase 7 Configuration Schema" section. The current hand-rolled section/scalar parser in `orchestrator/internal/config/config.go` cannot express the nested maps, lists, durations, CIDRs, or status arrays in the schema; this step replaces it with a real YAML parser (`gopkg.in/yaml.v3` in strict-unknown-fields mode) decoding into a typed `Phase7Config` struct. This is a hard prerequisite of Step 1 — the per-generation network/secret/probe/event code in Steps 1–4 reads through this struct.
1. Add DB schema for generations, resource allocations, turns, events, leases, network profiles, agent runtime profiles, and egress policies. Add the `orchestrator_owner` singleton meta row and acquire the `<run_dir>/orchestrator.pid` flock at orchestrator startup; allocator and recovery sweep both assert their `orchestrator_owner.uuid` match before writing. The single helper module that performs every turn-state CAS is introduced here with all four call sites (claim, ack_started, completion, failure) wired and unit-tested, even though only the session-insert and existing-turn-completion writers reach it on the 7a hot path.
2. Add resource allocator/reaper with allocation states and idempotent cleanup. Reaper recognizes only `harness-gen-<id>-*` named resources; legacy `phase1-demo` / `phase2-template` are removed by a one-time migration step.
3. Replace shared `phase2-template` runtime state with per-generation bundle/spec/control manifest, including atomic manifest write and digest validation in the entrypoint. Secret versions are immutable post-publish: rotation is a new `<secret_version>` row + new file, never an in-place rewrite of an existing version file. Per-generation materialization hardlinks the version file (or copies across filesystems) into the per-generation control dir.
4. Replace shared `phase1-demo` networking with per-generation netns, veth, IP, gateway, and CIDR, plus a static lab-wide egress policy covering the local LLM proxy, Doris FE/BE hosts/ports, and DNS (when targets are hostnames). Per-tenant egress policy and quotas remain Phase 8.

During the transitional 7a slice, the existing stdin/PTY turn path still ran, but every session was on its own resources and the schema was in place for 7b. Acceptance: zero references to `phase1-demo` or `phase2-template` in the runtime hot path; reaper cleans only namespaced resources; restart recovery works against the new schema. Full claim/ack CAS coverage on the live execution path landed in Step 6.

### Phase 7b: turn execution and checkpoint

5. Add the Agent Bridge protocol (hello with `last_output_sequence_by_turn`, heartbeat, probe, claim, ack started, output, ack completed) over the file-backed transport.
6. Move turn execution to bridge claim/ack with DB lease/CAS fencing and durable event recording per the architecture doc's Durable Event Log transaction rules (lifecycle acks committed with the turn-state CAS; `emit_output` appended in its own bounded-batch transactions, both before any in-memory publish).
7. Add cold Claude resume fallback only for queued or leased-but-not-started turns. `ack_started_at` turns enter the `unknown_after_ack_started` recovery flow described in the architecture doc, and never block the session.
8. Add SSE replay against the durable event log (Step 6 already persists events; Step 8 is the replay API, retention, and proxy correlation).
9. Add checkpoint-safe restore by recreating compatible generation resources, running a pre-restore host-side netns probe, and running a post-restore bridge probe. Restore is rejected if `runsc version`, `runsc platform`, `bundle_digest`, `runtime_config_digest`, or the projected `control_manifest_digest` (computed over the strict-field set defined in the architecture doc) do not exactly match the checkpoint metadata. The regenerable subset of the control manifest is excluded from both the stored and the recomputed digest by the same projection, so a digest match means strict fields are bit-equivalent; any mismatch forces cold fallback.
10. Re-enable automatic checkpoint/restore only after restore/reconnect/fallback semantics are correct, and promote checkpoint enablement from a Go const to a runtime-tunable generation policy.
11. Follow-up (non-blocking, can land in 7b late or slip to Phase 8): expose a `phase` sub-field on the session-row JSON (`cold_start | restore | live | idle | failing`) without altering the existing public `status` enum, so the UI can distinguish ~100–200 ms restores from ~1–2 s cold starts in progress feedback. Existing API clients that ignore unknown fields are unaffected.

## Remaining Risks

- Automatic checkpointing is policy-gated and disabled in the checked-in lab config. The checkpoint/restore path is control-plane-safe, but default enablement should be a deliberate operations decision after retention, resource pressure, and restore SLOs are measured.
- Phase 7 uses `secret_id` / `secret_version` indirection and per-generation mounted secret files. Real upstream credential storage, rotation, and GC remain Phase 8.
- Reclaimable generation resources are retained for `harness.reaper.failed_retention` before physical cleanup. That is intentional for debugging, but production observability should make retained resources visible.
- Additional agent adapters beyond Claude Code and the shell shim need their own completion contract before they are first-class multi-turn citizens.
- Artifact browsing is now a metadata-backed live tree, but it is intentionally read-only; file mutation operations are still left to the sandbox agent or shell.
- `OutputHub` drops lines for slow subscribers by design; that is fine for UI logs but not for a forensic audit stream.

## Notes on Prior Docs

The older phase status documents remain useful as implementation history, but they are no longer the source of truth for current behavior. Use:

- `current-status.md` for the live baseline.
- `architecture.md` for system design.
- `phase7/` for the Phase 7 target architecture (start at `phase7/README.md`).
- `PLAN.md` for roadmap only.
