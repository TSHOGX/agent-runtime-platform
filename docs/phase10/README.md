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

Phase 10 addresses the user-visible agent capability gaps:

- Sessions die mid-conversation because Claude Code's built-in compaction trigger assumes the official model's context window, not the deployed proxy backend's. Users see "session has failed" with no warning.
- The control plane has no way to inject identity, capability bounds, or sandbox resource constraints into the agent. Repeated incidents like the 1 GiB OOM during a wide-table CSV export (`fetchall()` on 218k rows) cannot be prevented prompt-side.
- Every sandbox session starts with no shared operational knowledge. Doris schema conventions, harness runtime quirks, and known pitfalls have to be re-learned per session.
- The control plane cannot enforce any agent-side guardrails (e.g. block writes outside `/workspace`, log every Bash command, limit which databases the agent reaches) and cannot offer agent-shared MCP capabilities (e.g. a managed schema/Doris MCP) without the user wiring them up per session.

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
new digest; live and checkpointed generations continue to reference the digest
they were prepared with, and restore cold-falls back if the pinned content is no
longer available.

This is intentionally simpler than what Phase 12 may need. When the trajectory pipeline starts authoring skills, that flow will sit on top of 10c with human review and may add its own `releases/` layer; that belongs to Phase 12, not 10c.

## Sub-targets

- **[10a — Configurable agent system prompt](./system-prompt.md).** Add a `harness.system_prompt.*` config section and propagate into the control manifest. Host artifact rendering verifies the prompt sidecar before start, the entrypoint revalidates it, and the default bridge `ClaudeTurnRunner` injects it via `--append-system-prompt-file`; the shell shim only inherits `HARNESS_SYSTEM_PROMPT_FILE` for sub-agents and performs no shell-layer injection in 10a. Includes the recorded 1 GiB OOM incident as a case study.
- **[10b — Proactive context compaction](./context-compaction.md).** Plumb proxy-reported token usage into the orchestrator, sum per turn, and trigger the selected driver's compaction adapter before the deployed model's real context window is exhausted. Claude Code is the first renderer.
- **[10c — System-skills mount](./system-skills-mount.md).** Read-only `/harness-skills` bind mount of a content-addressed snapshot materialized from `sandbox-image/system-skills/`; `skills_digest` pinned in the control manifest. Skills stay outside `/workspace` so they don't pollute the user-visible Files pane.
- **[10d — Control-plane-managed driver settings](./managed-settings.md).** Render non-secret driver policy/settings from `sandbox-image/managed-settings/` in this repo. Claude Code's `/etc/claude-code/managed-settings.json` is the first renderer. Single entry point for both `hooks` (operator-mandatory) and remote `mcpServers` (`http`/`sse` only — MCP servers are deployed elsewhere); credential-bearing MCP paths require a separate broker/token design, not provider secrets from `/harness-secrets` or Phase 8 model proxy tokens.

Phase 9 is the architectural prerequisite that keeps these features from becoming Claude-only. 10a is the smallest direct mitigation for current incidents; 10b unblocks recurring context-overflow failures; 10c provides shared operational knowledge; 10d closes the policy/MCP gap. 10c and 10d share the same content-digest-in-manifest pattern and can be implemented in either order. 10c is the foundation for [Phase 12 trajectory->skill evolution](../phase12-trajectory-pipeline.md).
