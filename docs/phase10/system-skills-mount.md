# Phase 10c: System-Skills Mount

> Status: planned on top of the completed [Phase 8 runtime isolation hardening](../phase8/README.md).
> Part of [Phase 10](./README.md).
> Foundation for [Phase 12 trajectory-to-skill evolution](../phase12-trajectory-pipeline.md).

## Goal

Provide shared internal skills to supported agent sessions while keeping those
files out of the user-visible `/workspace` file tree and Files pane.

Phase 10c is runtime packaging, not a skills release system. Operators update
skills by changing this repo and deploying a new commit. Generation preparation
pins a content digest and mounts a content-addressed snapshot; post-cutover
prepared, live, and checkpointed generations keep the digest they started with.

## Runtime Boundary

Phase 10c uses the Phase 8 MountPlan contract. The skills mount must be a
read-only exact bind, separate from `/workspace`, `/agent-home`, DataVolume
roots, and any path watched for artifacts.

The repo path is only the authoring source:

```text
sandbox-image/system-skills/
  skills/
    doris-query/
      SKILL.md
```

Generation preparation copies validated content into a runtime-owned snapshot:

```text
/var/lib/harness/content/skills/sha256-3e7f.../
```

Container layout:

```text
/harness-skills/skills/...
```

The entrypoint may link that directory into the selected driver's private HOME,
for example:

```text
$AGENT_HOME/.claude/skills/harness -> /harness-skills/skills
```

## Config

```yaml
harness:
  skills:
    enabled: true
    source_path: ./sandbox-image/system-skills
    mount_path: /harness-skills
    agent_link_mode: symlink
```

```go
type SkillsConfig struct {
    Enabled       bool
    SourcePath    string
    MountPath     string
    AgentLinkMode string
}
```

`source_path` is resolved relative to the repository root unless absolute. It
must point at a directory in the deployed codebase, not an operator-managed
release tree.

Because Phase 10 uses destructive cutover, 10c does not backfill skill metadata
into pre-10 sessions or preserve pre-10 prepared generations. Missing `skills_*`
fields after cutover are artifact corruption.

Recommended defaults:

- `enabled=false` in local tests unless the directory exists;
- `source_path=./sandbox-image/system-skills`;
- `mount_path=/harness-skills`;
- `agent_link_mode=symlink`.

## Digest And Snapshot

Compute `skills_digest` from validated source directory contents:

1. Walk all regular files under `source_path`.
2. Reject symlinks, escapes outside the root, device files, and sockets.
3. Sort slash-normalized relative paths.
4. Hash `relative_path + NUL + sha256(file_contents)` for each file.
5. Hash the canonical stream with SHA-256 and store `sha256:<hex>`.

No `manifest.json`, release directory, or `current` symlink is required in
Phase 10c. The git commit is the version; `skills_digest` is the post-cutover
restore pin.

Generation preparation must materialize and verify the content-addressed
snapshot before binding it. If the pinned snapshot is missing or fails digest
verification at restore time, restore uses cold fallback instead of binding the
current repo path.

## Mount And Manifest

Bind the snapshot read-only through Phase 8 MountPlan:

```json
{
  "destination": "/harness-skills",
  "type": "bind",
  "source": "/var/lib/harness/content/skills/sha256-3e7f...",
  "options": ["bind", "ro", "nosuid", "nodev", "noexec"]
}
```

If a worker can only use a recursive fallback, it must reject nested source
submounts and satisfy the Phase 8 post-launch submount gate.

Manifest fields:

```json
{
  "skills_enabled": true,
  "skills_digest": "sha256:3e7f...",
  "skills_mount_path": "/harness-skills"
}
```

Include these fields in the strict projected control-manifest digest. Restored
checkpointed generations must see the same `skills_digest`; new generations use
the deployed repo content at generation creation time.

## Entrypoint And Visibility

The entrypoint exports `HARNESS_SKILLS_ENABLED` and
`HARNESS_SKILLS_MOUNT_PATH` from the control manifest, then links skills into
the selected driver's private HOME according to the driver adapter. The original
mount remains read-only.

Do not mount or link skills below:

- `/workspace`
- `/agent-home`
- any artifact watcher path

This is a visibility boundary, not a secrecy boundary. Skills should contain
operational knowledge and procedures, not credentials or secrets.

## Implementation Checklist

1. Add `harness.skills` config, validation, and defaults.
2. Add canonical directory digest calculation.
3. Materialize validated skills into a content-addressed runtime snapshot.
4. Add the read-only exact bind through Phase 8 MountPlan.
5. Add skills manifest fields and projected digest coverage.
6. Update entrypoint driver-specific skill linking.
7. Add retention/GC that preserves snapshots referenced by live, prepared, or
   checkpointed generations.

## Acceptance Tests

- `enabled=false` means no mount.
- Missing or invalid source directory with `enabled=true` fails generation
  preparation.
- Digest changes when any skill file content or relative path changes.
- Runtime spec binds the content-addressed snapshot, not the mutable repo path.
- Invalid file types under the skills root are rejected.
- Manifest metadata joins the projected digest.
- Artifact watcher does not record skills as artifacts.
- Snapshot GC does not remove content referenced by prepared, live, or
  checkpointed generations.

## Open Decisions

- Confirm the exact Claude Code skills discovery path for the pinned CLI.
- Shell sessions may mount `/harness-skills`, but only agents that use skills
  should get HOME links.
