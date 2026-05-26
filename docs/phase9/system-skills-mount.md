# Phase 9c: System-Skills Mount

> Status: planned. Part of [Phase 9](./README.md).
> Foundation for [Phase 11 trajectory→skill evolution](../phase11-trajectory-pipeline.md).

## Goal

Provide a shared set of internal skills to every sandbox session while keeping those files out of the user-visible `/workspace` file tree and the right-side Files pane.

This is a runtime packaging and delivery problem:

- Skills are maintained in this repository.
- New containers mount the repository's current skill content read-only.
- The agent can discover and read the skills.
- The user workspace remains clean.
- Existing checkpointed generations remain pinned to the skill content digest they started with.

Phase 9c does **not** introduce a separate skills release system. Versioning is the `harness-platform` git history: operators update skills by changing this repo and deploying a new commit. Phase 11 may add a reviewed `releases/` flow when the trajectory pipeline starts generating skill candidates.

## Current Runtime Facts

The current container layout already has the right separation:

- `/workspace` is a symlink to `/sessions/<session_id>`.
- The agent HOME is `/agent-homes/<session_id>`, outside `/workspace`.
- The artifact watcher only scans the sessions root and ignores symlinks.
- Runtime spec generation already centralizes mounts for `/sessions`, `/agent-homes`, `/harness-control`, `/schema-pack`, and `/harness-secrets`.

The skills mount must not live under `/sessions` or `/workspace`. It is a separate read-only mount, then optionally linked into the agent's private HOME for a conventional discovery path.

## Directory Layout

Repository layout:

```text
sandbox-image/system-skills/
  skills/
    doris-query/
      SKILL.md
    harness-runtime/
      SKILL.md
```

Deployment resolves that repository path to a host path, for example:

```text
/opt/harness-platform/sandbox-image/system-skills
```

Container-side layout:

```text
/harness-skills              # read-only bind mount
/harness-skills/skills/...

/agent-homes/<session_id>/   # existing private agent HOME
```

Optional per-agent links created by the entrypoint:

```text
$AGENT_HOME/.claude/skills/harness -> /harness-skills/skills
```

The exact link target follows the real discovery path of the selected agent runtime. The mount path stays agent-agnostic.

## Config Shape

```yaml
harness:
  skills:
    enabled: true
    source_path: ./sandbox-image/system-skills
    mount_path: /harness-skills
    agent_link_mode: symlink
```

Go side:

```go
type SkillsConfig struct {
    Enabled       bool
    SourcePath    string
    MountPath     string
    AgentLinkMode string
}
```

`source_path` is resolved relative to the harness repo root unless absolute. It must point at a directory in the deployed codebase, not an operator-managed release tree.

Recommended defaults:

- `enabled=false` in local tests unless the directory exists.
- `source_path=./sandbox-image/system-skills`.
- `mount_path=/harness-skills`.
- `agent_link_mode=symlink`.

## Digest Rules

Compute `skills_digest` directly from the mounted directory contents:

1. Walk all regular files under `source_path`.
2. Reject symlinks, directories outside the root, device files, and sockets.
3. Sort files by slash-normalized relative path.
4. For each file, hash `relative_path + NUL + sha256(file_contents)` into a canonical digest stream.
5. Hash the stream with SHA-256 and store as `sha256:<hex>`.

No `manifest.json`, `release_id`, `drafts/`, or `current` symlink is required in Phase 9c. The git commit is the version; `skills_digest` is the runtime compatibility pin.

## Runtime Spec Mount

During generation artifact rendering:

1. Resolve `harness.skills.source_path`.
2. Validate it exists and is a directory.
3. Compute `skills_digest`.
4. Add a read-only bind mount:

```json
{
  "destination": "/harness-skills",
  "type": "bind",
  "source": "/opt/harness-platform/sandbox-image/system-skills",
  "options": ["rbind", "ro", "nosuid", "nodev", "noexec"]
}
```

Recommended behavior:

- `enabled=false`: no mount and no skills fields except `skills_enabled=false`.
- `enabled=true` with missing directory: fail generation preparation.
- Digest calculation error or invalid file type: fail generation preparation.

## Control Manifest Fields

Add to the per-generation control manifest:

```json
{
  "skills_enabled": true,
  "skills_digest": "sha256:3e7f...",
  "skills_mount_path": "/harness-skills"
}
```

Include these fields in the strict-field projection used by the control-manifest digest (see `../phase7/checkpoint-restore.md`), so checkpoint/restore enforces that:

- Existing live sessions keep their original mounted content.
- Restored checkpointed sessions must see the same `skills_digest`.
- New sessions use whatever skill content is present in the deployed repo at generation creation time.

If the repo's skill files change after a checkpoint, restore should reject the stale projected digest and fall back cold. That is correct: the old process image expected a different read-only mount payload.

## Entrypoint Integration

After `AGENT_HOME` is determined and before launching the agent:

```sh
if [ "${HARNESS_SKILLS_ENABLED:-0}" = "1" ] && [ -d "${HARNESS_SKILLS_MOUNT_PATH:-/harness-skills}/skills" ]; then
  mkdir -p "$AGENT_HOME/.claude/skills"
  ln -sfn "${HARNESS_SKILLS_MOUNT_PATH:-/harness-skills}/skills" "$AGENT_HOME/.claude/skills/harness"
fi
```

Make the path generic:

- Export `HARNESS_SKILLS_ENABLED` and `HARNESS_SKILLS_MOUNT_PATH` from the control manifest.
- Choose per-agent link locations in a small `case "$HARNESS_AGENT"` block.
- Keep the original mount read-only; only the symlink is written into agent HOME.

The agent HOME is already outside `/workspace`, so this does not appear in the right-side Files pane.

## UI and Artifact Visibility

Do not mount skills below:

- `/workspace`
- `/sessions`
- any path scanned by the artifact watcher

With the proposed `/harness-skills` mount:

- The agent can read the skills.
- The user-visible Files pane remains scoped to artifacts written under `/workspace`.
- Skill files do not show up as artifacts.

This is a visibility boundary, not a secrecy boundary. If the agent can read a skill, a user can ask the agent to explain it. Therefore skills should contain operational knowledge and procedures, not credentials or secrets.

## Implementation Steps

1. Add `harness.skills` config with validation and defaults.
2. Add runtime config fields for skills source path and mount path.
3. Add canonical directory digest calculation.
4. Add read-only skills bind mount in runtime spec generation.
5. Add skills fields to the control manifest and projected digest logic.
6. Update the entrypoint to link skills into the selected agent's private HOME.
7. Tests:
   - `enabled=false` means no mount.
   - `enabled=true` with missing source directory fails.
   - Valid source directory adds read-only mount.
   - Digest changes when any skill file content or relative path changes.
   - Invalid file types under the skills root are rejected.
   - Generated control manifest includes skills metadata and the projected digest reflects them.
   - Artifact watcher does not record skills as artifacts.

## Open Decisions

- Exact Claude Code skills discovery path inside the sandbox. Confirm against the pinned Claude Code version.
- Whether shell sessions should see `/harness-skills` by default. Recommendation: mount for all agents, link into HOME only for agents that use skills.
- Whether Phase 11 should introduce a formal skills release tree. Recommendation: yes, but only when trajectory-generated skill candidates need human review and rollback independent of repo deploys.
