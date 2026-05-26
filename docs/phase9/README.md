# Phase 9: Agent Capability and UX

> Status: planned. Follows the [P0 lifetime fixes](../p0-session-lifetime.md).
> Roadmap entry: [PLAN.md → Phase 9](../PLAN.md#phase-9-agent-capability-and-ux).

Phase 9 addresses the agent-side capability gaps that are most immediately user-visible:

- Sessions die mid-conversation because Claude Code's built-in compaction trigger assumes the official model's context window, not the deployed proxy backend's. Users see "session has failed" with no warning.
- The harness has no way to inject identity, capability bounds, or sandbox resource constraints into the agent. Repeated incidents like the 1 GiB OOM during a wide-table CSV export (`fetchall()` on 218k rows) cannot be prevented prompt-side.
- Every sandbox session starts with no shared operational knowledge. Doris schema conventions, harness runtime quirks, and known pitfalls have to be re-learned per session.

## Sub-targets

- **[9a — Configurable harness system prompt](./system-prompt.md).** Add a `harness.system_prompt.*` config section and propagate into the control manifest. Sandbox entrypoint injects via `--append-system-prompt` for Claude Code and prompt boilerplate for shell. Includes the recorded 1 GiB OOM incident as a case study.
- **[9b — Proactive context compaction](./context-compaction.md).** Plumb token usage from `claude-code-proxy` `finish` observations into the orchestrator, sum per turn, and trigger compaction earlier than Claude Code's own threshold.
- **[9c — Versioned system-skills mount](./system-skills-mount.md).** Read-only `/harness-skills` bind mount with `skills_release_id` / `skills_digest` persisted in the control manifest. Skills stay outside `/workspace` so they don't pollute the user-visible Files pane.

The three are largely independent and can land in any order, but 9a is the smallest and most directly mitigates current incidents; 9b unblocks the recurring context-overflow failures; 9c is foundational for [Phase 11 trajectory→skill evolution](../phase11-trajectory-pipeline.md).
