# Phase 10: Agent Capability and UX

> Status: planned on top of the completed [Phase 8 runtime isolation hardening](../phase8/README.md).
> Builds on the completed [P0 lifetime baseline](../p0-session-lifetime.md).
> Builds on [Phase 9 Agent Driver and Pi integration](../phase9/README.md).
> Roadmap entry: [PLAN.md -> Phase 10](../PLAN.md#phase-10-agent-capability-and-ux).

Phase 10 starts from the Phase 8 runtime boundary and the Phase 9 driver
contract. Exact per-session/per-driver mounts, host-side model credentials,
read-only rootfs, non-root sandbox identity, and `sandbox-isolation-v1`
control-manifest projection still apply. System skills and managed settings
add more sandbox-visible content, so they must continue to use the Phase 8
MountPlan and credential boundary.

## Destructive Cutover Principle

Phase 10 is allowed to sunset all pre-10 runtime/session state instead of
preserving compatibility for old prepared artifacts. The Phase 10 startup or
release cutover may delete pre-10 sessions, messages, artifacts, turns, events,
active model contexts, runtime generations, checkpoints, control directories,
bridge directories, prepared bundles, and driver homes. It may rebuild
constrained SQLite tables directly under the orchestrator owner lock.

This reduces implementation scope by eliminating pre-10 manifest normalization,
pre-10 checkpoint restore compatibility, and prompt/skills/settings backfills.
It does not relax runtime cleanup: discoverable live `runsc` containers,
network namespaces, veth pairs, nftables state, bridge workers, and other
provider/isolation resources must be stopped, proven absent, or durably
quarantined before their DB ownership rows are removed.

After the cutover, every created generation must be Phase 10-shaped. Missing
Phase 10 manifest fields are corruption in the new chain, not legacy
compatibility input. Restore compatibility applies only among post-cutover
generations whose manifests and pinned content digests were produced by Phase
10 code.

Phase 10 addresses the user-visible agent capability gaps:

- Claude Code can hit the deployed proxy backend's real context limit before
  its built-in compaction trigger fires.
- The control plane cannot inject identity, capability bounds, or sandbox
  resource constraints into the agent; the 1 GiB OOM from a wide-table
  `fetchall()` export is the reference incident.
- Each sandbox session starts without shared operational knowledge such as
  Doris conventions and harness runtime pitfalls.
- The control plane cannot enforce agent-side hooks or provide shared remote MCP
  registrations without per-session user setup.

## Versioning convention for 10c and 10d

10c and 10d both author sandbox-visible content in this repo:
`sandbox-image/system-skills/` for 10c and
`sandbox-image/managed-settings/` for 10d. Versioning of those authoring
payloads is **the repository git history itself**: there is no separate
`releases/` tree, no manifest layer, and no `current` symlink in Phase 10.
Generation preparation computes a content digest, materializes a
content-addressed runtime snapshot, and mounts that snapshot rather than a
mutable repo working tree path. The per-generation control manifest pins
`skills_digest` and `managed_settings_digest`; both join the strict-field
projection used by checkpoint/restore (see `../phase7/checkpoint-restore.md`).
Operators publish updates by deploying a new commit. New generations pick up the
new digest; post-cutover prepared, live, and checkpointed generations continue
to reference the digest they were prepared with. Restore uses cold fallback if
the pinned content is no longer available.

This is intentionally simpler than what Phase 12 may need. Trajectory-authored
skills, human review, and any `releases/` layer belong to Phase 12, not 10c.

## Sub-targets

- **[10a - Configurable agent system prompt](./system-prompt.md).** Add
  `harness.system_prompt.*`, persist a session snapshot, emit prompt manifest
  fields, verify the sidecar, and inject it through the driver.
- **[10b - Proactive context compaction](./context-compaction.md).** Use
  proxy-reported token usage to trigger the selected driver's compaction adapter
  before the deployed model's real context window is exhausted.
- **[10c - System-skills mount](./system-skills-mount.md).** Mount a read-only
  `/harness-skills` content snapshot, pin `skills_digest`, and keep skills out
  of `/workspace`.
- **[10d - Control-plane-managed driver settings](./managed-settings.md).**
  Render non-secret hooks and remote `mcpServers` config through driver
  adapters. MCP credentials require a separate broker/token design.

Phase 9 keeps these features driver-adapted instead of Claude-only. 10c and 10d
share the same content-digest-in-manifest pattern and can be implemented in
either order. 10c is the foundation for
[Phase 12 trajectory-to-skill evolution](../phase12-trajectory-pipeline.md).
