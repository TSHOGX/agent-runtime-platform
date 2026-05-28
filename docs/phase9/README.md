# Phase 9: Agent Capability and UX

> Status: planned on top of the completed [Phase 8 runtime isolation hardening](../phase8/README.md).
> Builds on the completed [P0 lifetime baseline](../p0-session-lifetime.md).
> Roadmap entry: [PLAN.md → Phase 9](../PLAN.md#phase-9-agent-capability-and-ux).

Phase 9 starts from the Phase 8 baseline: exact per-session/per-driver mounts,
host-side model credentials, read-only rootfs, non-root sandbox identity, and
`sandbox-isolation-v1` control-manifest projection. System skills and managed
Claude settings add more sandbox-visible content, so they must continue to use
the Phase 8 MountPlan and credential boundary.

Phase 9 addresses the agent-side capability gaps that are most immediately user-visible:

- Sessions die mid-conversation because Claude Code's built-in compaction trigger assumes the official model's context window, not the deployed proxy backend's. Users see "session has failed" with no warning.
- The harness has no way to inject identity, capability bounds, or sandbox resource constraints into the agent. Repeated incidents like the 1 GiB OOM during a wide-table CSV export (`fetchall()` on 218k rows) cannot be prevented prompt-side.
- Every sandbox session starts with no shared operational knowledge. Doris schema conventions, harness runtime quirks, and known pitfalls have to be re-learned per session.
- The harness cannot enforce any agent-side guardrails (e.g. block writes outside `/workspace`, log every Bash command, limit which databases the agent reaches) and cannot offer agent-shared MCP capabilities (e.g. a managed schema/Doris MCP) without the user wiring them up per session.

## Versioning convention for 9c and 9d

9c and 9d both deliver content into the sandbox from this repo: `sandbox-image/system-skills/` for 9c, `sandbox-image/managed-settings/` for 9d. Versioning of these payloads is **the harness-platform git history itself** — there is no separate `releases/` tree, no manifest layer, no `current` symlink. What gets pinned in the per-generation control manifest is a content-hash digest (`skills_digest`, `managed_settings_digest`); both join the strict-field projection used by checkpoint/restore (see `../phase7/checkpoint-restore.md`). Operators publish updates by deploying a new commit; the next new generation picks them up; existing checkpointed generations stay pinned to their original digest and trigger cold fallback if the host content changes underneath them.

This is intentionally simpler than what Phase 11 will eventually need. When the trajectory pipeline starts authoring skills, that flow will sit on top of 9c with its own `releases/` and review process — but that is Phase 11's problem, not 9c's.

## Sub-targets

- **[9a — Configurable harness system prompt](./system-prompt.md).** Add a `harness.system_prompt.*` config section and propagate into the control manifest. Host artifact rendering verifies the prompt sidecar before start, the entrypoint revalidates it, and the default bridge `ClaudeTurnRunner` injects it via `--append-system-prompt-file`; the shell shim only inherits `HARNESS_SYSTEM_PROMPT_FILE` for sub-agents and performs no shell-layer injection in 9a. Includes the recorded 1 GiB OOM incident as a case study.
- **[9b — Proactive context compaction](./context-compaction.md).** Plumb token usage from `claude-code-proxy` `finish` observations into the orchestrator, sum per turn, and trigger compaction earlier than Claude Code's own threshold.
- **[9c — System-skills mount](./system-skills-mount.md).** Read-only `/harness-skills` bind mount of `sandbox-image/system-skills/`; `skills_digest` pinned in the control manifest. Skills stay outside `/workspace` so they don't pollute the user-visible Files pane.
- **[9d — Harness-managed Claude Code settings](./managed-settings.md).** Render `/etc/claude-code/managed-settings.json` inside the sandbox from `sandbox-image/managed-settings/` in this repo. Single entry point for both `hooks` (operator-mandatory) and remote `mcpServers` (`http`/`sse` only — MCP servers are deployed elsewhere); credential-bearing MCP paths require a separate broker/token design, not provider secrets from `/harness-secrets` or Phase 8 model proxy tokens.

9a is the smallest and most directly mitigates current incidents; 9b unblocks the recurring context-overflow failures; 9c provides shared operational knowledge; 9d closes the policy/MCP gap. 9c and 9d share the same content-digest-in-manifest pattern and can be implemented in either order. 9c is the foundation for [Phase 11 trajectory→skill evolution](../phase11-trajectory-pipeline.md).
