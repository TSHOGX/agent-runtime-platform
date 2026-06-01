# Agent Runtime Platform Plan

> Active planning starts from the current codebase. Current architecture is in
> [architecture.md](./architecture.md); detailed next-stage design is in
> [next-stage.md](./next-stage.md).

## Current Baseline

- The Go orchestrator owns sessions, turns, runtime generations, durable
  events, artifact metadata, proxy correlation, quota, and retention.
- The Next.js workbench talks to the orchestrator through same-origin route
  handlers and streams live updates over SSE.
- Each generation runs in a gVisor `runsc` sandbox with exact workspace and
  agent-home binds, non-root process identity, read-only rootfs, per-generation
  networking, and host-side model credentials.
- Product mode `Agent` resolves through deployment config to the selected
  driver; the checked-in lab default resolves it to Pi. `Shell` is available
  only when enabled and present in the active image manifest.
- Claude Code, Pi, and the shell shim are supported through the existing
  driver/provider registry and bridge protocol paths.
- Checkpoint/restore exists but automatic idle checkpointing is disabled in the
  checked-in lab config.

## Next Stage

The next stage introduces a host-only immutable generation plan, then adds an
agent capability plane on top of that plan and the existing driver/provider
contract. Platform-managed agent behavior must flow through explicit driver
adapters and immutable per-session or per-generation snapshots. Unsupported
enabled features must fail during deployment or generation preparation.

Primary work:

1. Add a host-only `GenerationPlan` so the sandbox contract, control manifest,
   OCI spec, mount plan, and driver config artifacts are derived from the same
   immutable launch facts.
2. Add typed capability declarations to driver specs, validate enabled features
   and required sub-capabilities against the selected driver, and render launch
   artifacts only from validated plan and manifest inputs.
3. Persist an operator policy prompt snapshot per session and deliver it only
   through the selected driver's prompt adapter.
4. Record proxy-reported model-context usage, enforce configured context
   budgets, and call driver compaction adapters only when supported.
5. Mount shared operational skills as a read-only content-addressed snapshot at
   `/harness-skills`, outside `/workspace` and artifact watcher paths.
6. Render non-secret managed driver settings, hooks, and remote MCP
   registrations through driver policy adapters. Credential-bearing MCP needs a
   later broker/token design.

## Guardrails

- Keep `Agent` and `Shell` as product modes; do not expose raw driver IDs in
  normal user workflows.
- Do not add driver-specific branches in server, runtime, bridge, or frontend
  code. New behavior must enter through shared driver/provider contracts and
  adapter interfaces.
- Keep provider credentials host-side. Do not put live secrets in prompts,
  skills, managed settings, `/workspace`, `/agent-home`, argv, env, logs, or
  bridge queues.
- Treat bridge clients, turn runners, and sandboxes as restartable at any turn
  boundary. Correctness must come from durable state and rendered artifacts,
  not in-process flags.
- After changing runtime scripts or files under `sandbox-image/files/`, rebuild
  or overlay-sync the active rootfs before live testing.
- Treat contract-breaking runtime, bridge, proxy, deployment-config, rootfs, or
  session-lifecycle changes as a cold cutover, then smoke test the selected
  deployment driver.

## Later Work

- Production operations: multi-user auth, credential storage/rotation/GC,
  tenant egress policy, cgroup limits, observability, and multi-orchestrator
  high availability.
- Trajectory-to-memory-to-skill pipeline: design work for turning reviewed
  session evidence into shared skills after the skills mount exists.
