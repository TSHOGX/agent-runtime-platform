# Phase 7 — Checkpoint-Safe Control Plane

> Date: 2026-05-22 (split from `docs/checkpoint-safe-control-plane-architecture.md` on 2026-05-24)
> Status: Phase 7 target architecture
> Scope: make sessions addable, checkpointable, recoverable, reconnectable, network-correct, and multi-turn reliable.

This directory is the reading order for Phase 7. Each file owns a slice of the design; rules are defined once and linked from every place that needs them. If a fact appears in two files, the one named in the heading is authoritative and the other links to it.

## Goal

Move session correctness off the live container process and into a durable host-side control plane. Every gVisor container becomes a replaceable runtime generation; the session is the durable DB state, turn log, event log, network profile, Claude conversation identity, and runtime generation lease. `runsc checkpoint/restore` becomes a performance optimization, not the only mechanism that keeps a conversation correct.

## Starting point

Current baseline behavior, network values, and `config/harness.yaml` contents live in [../architecture.md](../architecture.md) and [../current-status.md](../current-status.md); Phase 7 docs do not restate them. Two upstream conclusions carry forward:

- [../gvisor-decision.md](../gvisor-decision.md): KVM is unavailable on this host, so gVisor `runsc` with `systrap` is the selected runtime.
- [../runsc-warm-sentry-research.md](../runsc-warm-sentry-research.md): `runsc release-20260511.0` has no warm sentry; low-latency startup must come from orchestrator-level pooling or normal checkpoint tuning.

The architecture gap Phase 7 addresses: pre-Phase-7 automatic idle checkpointing was unsafe because `runsc restore` could bring the sandbox back while the attached stdin turn channel was no longer reliably reconnectable. Phase 7 replaces that coupling with bridge claim/ack, durable events, checkpoint metadata validation, and cold fallback. Legacy shared resources (`/run/netns/phase1-demo`, `/var/lib/harness/control/phase2-template/session.json`, the static `bundle/out/phase2-template-bundle/config.json` reused as live mutable state, plaintext `anthropic_api_key` / `anthropic_auth_token` in the manifest) are sunset by 7a — see [invariants.md](./invariants.md) and [runtime-resources.md](./runtime-resources.md).

## 7a vs 7b boundary

Phase 7 ships in two slices because the work is large enough that a single PR risks long-lived branches and stdin/bridge dual-write detours.

- **Phase 7a — control-plane skeleton.** Per-generation resources, durable schema, per-generation network and bundle, no shared `phase1-demo` / `phase2-template` state. The transitional 7a implementation kept the existing stdin/PTY turn path running on top of this. Steps 1–4 in [implementation-plan.md](./implementation-plan.md).
- **Phase 7b — turn execution refactor.** Agent Bridge claim/ack, durable turn ledger with `ack_started_at` semantics, durable event log, cold Claude resume, checkpoint-safe restore, and automatic checkpoint policy. Steps 5–10 in [implementation-plan.md](./implementation-plan.md).

The hard invariants split cleanly along this boundary; see [invariants.md](./invariants.md#phase-7a-vs-7b-applicability).

## Target architecture

```text
Browser
  |
  | HTTP + SSE with last_event_id
  v
Next.js frontend
  |
  | same-origin API proxy
  v
Control Plane
  |
  | session store
  | turn ledger
  | durable event log
  | runtime generation table
  | runtime resource allocator
  | network profile table
  | runtime lease manager
  | artifact metadata
  v
Runtime Manager
  |
  | runsc driver
  | checkpoint manager
  | restore/cold-start fallback
  | sandbox pool, optional
  | network probe
  v
runsc generation N
  |
  | Agent Bridge
  |   - reconnectable control client
  |   - per-turn ack/completion
  |   - stdout/stderr/event forwarding
  |
  | Claude Code / shell shim
  | workspace mount
  | agent home mount
  v
Durable event log
```

The main difference from the current architecture is the **Agent Bridge**: instead of treating container stdin as the session's source of truth, the sandbox starts a small bridge process that talks to the host control plane over a reconnectable, file-backed transport. Protocol semantics are durable; transport is replaceable.

## Reading order

1. [invariants.md](./invariants.md) — hard invariants, concurrency model, lease/CAS rules, lifecycle states. Read first; everything else assumes these.
2. [schema.md](./schema.md) — every table, index, and migration. The CAS SQL the orchestrator runs lives here.
3. [runtime-resources.md](./runtime-resources.md) — per-generation host resources: bundle, spec, control manifest, secret materialization, allocator, reaper, recovery sweep.
4. [network-and-probes.md](./network-and-probes.md) — per-generation netns/veth/IP, egress policy, host-side and in-sandbox probes, proxy/upstream observability.
5. [bridge-protocol.md](./bridge-protocol.md) — Agent Bridge wire protocol, file-backed transport, idempotency and sequence recovery, SSE wire protocol.
6. [checkpoint-restore.md](./checkpoint-restore.md) — Claude logical resume, physical checkpoint/restore, digest equivalence rules, `unknown_after_ack_started` UX.
7. [implementation-plan.md](./implementation-plan.md) — Phase 7 configuration schema, Step 1–10 PR boundaries with prerequisites and acceptance signals, operational notes.
8. [test-matrix.md](./test-matrix.md) — observable assertions that make every invariant a runtime guarantee. Cross-references every other file.
9. [release-qualification.md](./release-qualification.md) — release-only gates for the pinned proxy contract, gVisor bridge durability lab, and live turn-start latency benchmark.

## Out of scope (deferred to Phase 8)

Phase 7 establishes fencing and persistence; the following must not gate Phase 7 acceptance:

- multi-orchestrator HA / shared-DB clustering / leader election.
- real upstream credentials with KMS-backed storage and rotation. Phase 7 ships the `secret_id`/`secret_version` indirection and per-generation mount; Phase 8 wires the actual store.
- per-tenant egress policy at the host firewall. Phase 7 ships per-generation netns and a static allowed-egress list; Phase 8 adds tenant-level policy and quotas.
- authentication and authorization on the orchestrator HTTP/SSE surface. Phase 7 assumes a trusted operator.

## Key design choices

- **Bridge transport: file-backed queue.** The checkpoint-ready precondition rules out any transport with a live host-side handle. A Unix socket bind mount or HTTP long-poll requires an explicit quiesce step before `runsc checkpoint`; the file-backed queue accepts higher latency and filesystem edge cases (rename atomicity, fsync cadence) in exchange for transparent checkpoint/restore. Protocol is transport-neutral; a low-latency variant can be added later if it implements quiesce and proves clean reconnect across restore.
- **Event log storage: SQLite events table.** Same database as `turns` and `runtime_generations`, so durability and ordering are a single-transaction concern. Postgres is a Phase 8 deployment change, not an architecture change.
- **Checkpoint policy rollout: feature flag, gated on the bridge.** Automatic checkpointing is controlled by `harness.checkpoint.auto_enabled` / `HARNESS_AUTO_CHECKPOINT_ENABLED`, snapshots the per-session policy onto each generation at allocation, and only checkpoints idle generations whose bridge heartbeat and `checkpoint-ready` marker are fresh. Memory-pressure policy remains a future rollout layer.
- **Manifest digest verification: canonical JSON in both host and sandbox.** A side-file or pre-canonicalized on-disk format would eliminate the sandbox-side verifier, but using the same deterministic JSON rule everywhere removes a class of host/sandbox divergence bugs that recur the moment a third consumer appears. Phase 7's manifest schema is restricted to JSON object payloads with strings, booleans, and integers; both the Go host code and sandbox Python verifier sort keys, remove insignificant whitespace, preserve UTF-8, and hash the resulting bytes. The test matrix carries a shared fixture that both suites must pass in release builds.
