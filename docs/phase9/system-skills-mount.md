# Phase 9c: Versioned System-Skills Mount

> Status: planned. Part of [Phase 9](./README.md).
> Foundation for [Phase 11 trajectory→skill evolution](../phase11-trajectory-pipeline.md).

## Goal

Provide a shared set of internal skills to every sandbox session while keeping those files out of the user-visible `/workspace` file tree and the right-side Files pane.

This is a runtime packaging and delivery problem:

- Skills are maintained as versioned releases.
- New containers mount a selected skills release read-only.
- The agent can discover and read the skills.
- The user workspace remains clean.
- Existing sessions remain pinned to the skills version they started with.

## Current Runtime Facts

The current container layout already has the right separation:

- `/workspace` is a symlink to `/sessions/<session_id>`.
- The agent HOME is `/agent-homes/<session_id>`, outside `/workspace`.
- The artifact watcher only scans the sessions root and ignores symlinks.
- Runtime spec generation already centralizes mounts for `/sessions`, `/agent-homes`, `/harness-control`, `/schema-pack`, and `/harness-secrets`.

The skills mount must not live under `/sessions` or `/workspace`. It is a separate read-only mount, then optionally linked into the agent's private HOME for a conventional discovery path.

## Directory Layout

Host-side layout:

```text
/var/lib/harness/system-skills/
  releases/
    2026-05-25T120000Z-<digest>/
      manifest.json
      skills/
        doris-query/
          SKILL.md
        harness-runtime/
          SKILL.md
    2026-05-26T120000Z-<digest>/
      manifest.json
      skills/
        ...
  drafts/
    2026-05-25-nightly/
      manifest.json
      skills/
        ...
  current -> releases/2026-05-25T120000Z-<digest>
```

Container-side layout:

```text
/harness-skills              # read-only bind mount to selected release
/harness-skills/manifest.json
/harness-skills/skills/...

/agent-homes/<session_id>/   # existing private agent HOME
```

Optional per-agent links created by the entrypoint:

```text
$AGENT_HOME/.codex/skills/harness -> /harness-skills/skills
$AGENT_HOME/.claude/skills/harness -> /harness-skills/skills
```

The exact link target follows the real discovery path of the selected agent runtime. The mount path stays agent-agnostic.

## Release Manifest

Each release carries a small, deterministic, digestable manifest:

```json
{
  "manifest_version": 1,
  "release_id": "2026-05-25T120000Z-3e7f...",
  "created_at": "2026-05-25T12:00:00Z",
  "source": "manual",
  "parent_release_id": "2026-05-24T120000Z-a9c1...",
  "skills": [
    {
      "name": "doris-query",
      "path": "skills/doris-query/SKILL.md",
      "sha256": "..."
    }
  ],
  "digest": "..."
}
```

Digest rules:

- Hash canonical JSON for the manifest payload.
- Hash file contents for every skill file.
- Include relative paths in the digest input so renames are visible.
- Keep `digest` outside the digest payload, or use a `{payload, digest}` wrapper matching the existing control-manifest pattern in Phase 7.

## Config Shape

```yaml
harness:
  skills:
    enabled: true
    root: /var/lib/harness/system-skills
    selected_release: current
    mount_path: /harness-skills
    agent_link_mode: symlink
```

Go side:

```go
type SkillsConfig struct {
    Enabled         bool
    Root            string
    SelectedRelease string
    MountPath       string
    AgentLinkMode   string
}
```

`selected_release` can be:

- `current` (symlink in the release tree)
- a concrete release id
- an absolute release path, only if explicitly allowed by config validation

For production behavior, prefer `current` for new sessions and persist the resolved release id/digest in the generation metadata.

## Runtime Spec Mount

During generation artifact rendering:

1. Resolve the selected release path.
2. Validate it has a manifest.
3. Validate all manifest file hashes.
4. Add a read-only bind mount:

```json
{
  "destination": "/harness-skills",
  "type": "bind",
  "source": "/var/lib/harness/system-skills/releases/<release_id>",
  "options": ["rbind", "ro", "nosuid", "nodev", "noexec"]
}
```

Recommended behavior:

- Missing skills root with `enabled=false`: no mount.
- Missing skills root with `enabled=true`: fail generation preparation.
- Digest mismatch: fail generation preparation.
- Release path under `drafts/`: only allowed if an explicit development flag is set.

## Control Manifest Fields

Add to the per-generation control manifest:

```json
{
  "skills_enabled": true,
  "skills_release_id": "2026-05-25T120000Z-3e7f...",
  "skills_digest": "3e7f...",
  "skills_mount_path": "/harness-skills"
}
```

Include these fields in the strict-field projection used by the control-manifest digest (see `../phase7/checkpoint-restore.md`), so checkpoint/restore enforces that:

- Existing live sessions keep their original mounted release.
- Restored checkpointed sessions must use the same `skills_digest`.
- New sessions use whatever `current` resolves to at generation creation time.

## Entrypoint Integration

After `AGENT_HOME` is determined and before launching the agent:

```sh
if [ -d "${HARNESS_SKILLS_MOUNT_PATH:-/harness-skills}/skills" ]; then
  mkdir -p "$AGENT_HOME/.codex/skills"
  ln -sfn "${HARNESS_SKILLS_MOUNT_PATH:-/harness-skills}/skills" "$AGENT_HOME/.codex/skills/harness"
fi
```

Make the path generic:

- Export `HARNESS_SKILLS_MOUNT_PATH` from the control manifest.
- Choose per-agent link locations in a small `case "$HARNESS_AGENT"` block.
- Keep the original mount read-only; only the symlink is written into agent HOME.

The agent HOME is already outside `/workspace`, so this does not appear in the right-side Files pane.

## Version Selection and Rollback

Publish flow:

1. Generate or manually create a draft release under `drafts/`.
2. Validate manifest and file hashes.
3. Copy or hardlink into `releases/<release_id>`.
4. Atomically update `current` symlink:

```text
current.tmp -> releases/<new_release_id>
rename(current.tmp, current)
```

Rollback:

```text
current -> releases/<previous_release_id>
```

Because each generation stores `skills_release_id` and `skills_digest`, rollback affects only new sessions. Existing sessions are not silently changed.

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
2. Add runtime config fields for skills root, selected release, and mount path.
3. Add release resolver and manifest validator.
4. Add read-only skills bind mount in runtime spec generation.
5. Add skills fields to the control manifest and projected digest logic.
6. Update the entrypoint to link skills into the selected agent's private HOME.
7. Tests:
   - `enabled=false` means no mount.
   - `enabled=true` with missing release fails.
   - Valid release adds read-only mount.
   - Manifest digest mismatch fails.
   - Generated control manifest includes skills metadata and the projected digest reflects them.
   - Artifact watcher does not record skills as artifacts.

## Open Decisions

- Exact Codex/Claude skills discovery path inside the sandbox. Confirm against the pinned Claude Code version.
- Whether shell sessions should see `/harness-skills` by default. Recommendation: mount for all agents, link into HOME only for agents that use skills.
- Whether canary skills releases should be selected per user/session. Recommendation: defer until after the first stable release flow.
