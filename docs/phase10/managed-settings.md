# Phase 10d: Control-Plane-Managed Driver Settings

> Status: planned on top of the completed [Phase 8 runtime isolation hardening](../phase8/README.md).
> Part of [Phase 10](./README.md).

Phase 8 is the completed baseline for this design. Earlier drafts allowed MCP
bearer tokens to be rendered from `/harness-secrets` into Claude-visible managed
settings. That is no longer acceptable for upstream or remote-service secrets.
Phase 10d may mount settings and non-secret endpoint config, but any
credential-bearing MCP path requires a separate broker/token design. Phase 8
moves model credentials host-side and uses source-IP plus driver entitlement
for model proxy authorization; it does not provide scoped proxy tokens for
managed settings.

## Goal

Give the platform operator a single, mandatory driver settings/policy surface
for every supported model-backed agent session:

- system hooks (`hooks`) that enforce control-plane policy or observability;
- remote MCP servers (`mcpServers`) that are deployed outside this repo but should be available to every agent;
- optional MCP credentials only after a separate broker/token design exists,
  not upstream bearer tokens rendered from `/harness-secrets` and not Phase 8
  model proxy tokens.

The first renderer targets Claude Code's Linux enterprise managed settings path:

```text
/etc/claude-code/managed-settings.json
/etc/claude-code/managed-settings.d/   # optional future drop-ins
```

Managed settings are a better fit than writing `$AGENT_HOME/.claude/settings.json`:

- They are container-global for the Claude Code process, independent of per-session HOME.
- They have higher priority than user/project/local settings.
- They can enforce hook policy such as `disableAllHooks` or `allowManagedHooksOnly`.
- They avoid merge/symlink logic in `$AGENT_HOME`.

## Scope

In Phase 10d, MCP servers themselves are **not** packaged in this repository and are **not** run as stdio child processes inside the sandbox. They are deployed elsewhere. The sandbox only receives Claude Code config that tells it how to reach those remote MCP endpoints.

Supported MCP transport in this phase:

- `http` (Streamable HTTP) — recommended for new remote MCP servers.
- `sse` — allowed for legacy remote MCP servers.

Out of scope for Phase 10d:

- stdio MCP servers bundled in the sandbox image;
- MCP server lifecycle management;
- independent managed-settings release trees;
- per-user MCP catalogs or UI toggles.

Versioning of the managed-settings authoring payload is the repository git
history. Generation preparation pins a content digest and renders from a
content-addressed snapshot, not from a mutable repo working tree path.

## Repository Layout

```text
sandbox-image/managed-settings/
  managed-settings.json
```

Example template:

```json
{
  "allowManagedHooksOnly": true,
  "hooks": {
    "PreToolUse": [
      {
        "matcher": "Bash",
        "hooks": [
          {
            "type": "command",
            "command": "/usr/local/bin/harness-hook-guard pre-tool-use"
          }
        ]
      }
    ]
  },
  "mcpServers": {
    "doris-readonly": {
      "type": "http",
      "url": "https://mcp.internal.example/doris/mcp"
    },
    "schema-pack": {
      "type": "sse",
      "url": "https://mcp.internal.example/schema/sse"
    }
  }
}
```

The file committed to git must not contain live credentials. `${TOKEN:...}`
placeholders are allowed only after the separate broker/token design exists;
until then, credential-bearing headers should be omitted.

The repository file is the authoring source. During generation preparation the
orchestrator validates it, computes `managed_settings_digest`, and copies it
into a runtime-owned content-addressed snapshot before any rendering. The
generated OCI spec may bind the rendered output, and may bind the source
snapshot only when the selected driver needs to inspect it. It must not bind the
mutable repository path directly.

## Credential Placeholder Resolution

Managed settings may reference credentials only after a separate broker/token
design has provided scoped, short-lived, non-upstream tokens for that trust
domain. The legacy model-provider `/harness-secrets` mount is not part of the
Phase 8 runtime contract and must not be reintroduced for Phase 10d. Phase 8
model proxy authorization is not a token source for this file.

The placeholder shape is reserved for that future broker design:

```text
${TOKEN:<token_id>}
```

Example:

```json
"Authorization": "Bearer ${TOKEN:mcp_doris_generation_token}"
```

Rendering behavior:

1. Read the managed-settings template from the source snapshot.
2. If no broker/token design is enabled, reject any `${TOKEN:...}` placeholder
   during generation preparation.
3. If a future broker is enabled, replace each token placeholder using that
   broker for the current generation/turn.
4. Write the rendered JSON to a per-generation rendered artifact that is mounted
   at `/etc/claude-code/managed-settings.json` before `setpriv` launches
   Claude Code.
5. Keep the rendered file outside `/workspace` and outside `/agent-home`.

Security notes:

- The rendered file is inside the sandbox and can be read by the Claude Code process. Any token placed there must therefore be scoped, short-lived, revocable, and safe to expose to the agent.
- The rendered file must not be copied into `/workspace` or artifacts.
- The source template digest should be computed before token substitution so
  token rotation does not force a code digest change. Token policy identity must
  still be represented in the control manifest so restore/cold-fallback behavior
  is deterministic.

## Config Shape

```yaml
harness:
  managed_settings:
    enabled: true
    source_path: ./sandbox-image/managed-settings/managed-settings.json
    mount_path: /harness-managed-settings
    rendered_path: /etc/claude-code/managed-settings.json
    token_refs: []  # reserved until a separate broker/token design exists
```

Go side:

```go
type ManagedSettingsConfig struct {
    Enabled      bool
    SourcePath   string
    MountPath    string
    RenderedPath string
    TokenRefs    []TokenRef
}

type TokenRef struct {
    ID string
}
```

`source_path` is resolved relative to the repository root unless absolute.

`token_refs` makes MCP credential use explicit at generation preparation time.
Until a separate broker/token design exists, any configured `token_refs` or
`${TOKEN:...}` placeholders fail generation preparation. After that design
lands, the runtime should validate each token ref against the broker's allowlist
and include the token policy identity in the digest pin.

## Runtime Spec Mount

Mount only generation-pinned material through the Phase 8 MountPlan exact-bind
contract. For example, a source snapshot may be mounted read-only if the driver
needs it:

```json
{
  "destination": "/harness-managed-settings",
  "type": "bind",
  "source": "/var/lib/harness/content/managed-settings/sha256-10a4c...",
  "options": ["bind", "ro", "nosuid", "nodev", "noexec"]
}
```

If a worker can only use a recursive fallback, it must satisfy the Phase 8
nested-mount rejection, private/slave propagation, and post-launch adversarial
submount gate. Phase 10d must not mount a repository root or parent config
directory into the sandbox.

`/etc/claude-code/managed-settings.json` must not depend on a writable rootfs.
Preferred Phase 10d behavior is host-side rendering from the source snapshot
into a per-generation runtime/control directory, followed by a read-only bind
mount of the rendered file or containing directory. If any rendering remains
inside the sandbox, the destination must be an explicit tmpfs or bind mount
declared by the Phase 8 mount plan, not an arbitrary rootfs write.

## Control Manifest Fields

Add to the per-generation control manifest:

```json
{
  "managed_settings_enabled": true,
  "managed_settings_digest": "sha256:10a4c...",
  "managed_settings_mount_path": "/harness-managed-settings",
  "managed_settings_source_file": "managed-settings.json",
  "managed_settings_rendered_path": "/etc/claude-code/managed-settings.json",
  "managed_settings_token_refs": []
}
```

Include these fields in the strict-field projection used by the control-manifest digest (see `../phase7/checkpoint-restore.md`). This enforces that:

- A checkpointed generation restores only with the same managed-settings template digest.
- A change to managed hooks or MCP endpoint config forces cold fallback for checkpointed generations.
- A change to declared MCP token policy also forces cold fallback after a
  separate broker/token design exists.
- New sessions pick up the managed settings from the deployed repo at generation creation time.
- Restore uses the pinned source snapshot or rendered artifact; it must not bind
  the current repo path as a substitute.

## Digest Rules

Compute `managed_settings_digest` from the source template bytes, not from the rendered file containing token values:

```text
sha256:<hex canonical UTF-8 bytes of managed-settings.json template>
```

If the managed-settings directory later grows helper files, switch to the same directory digest algorithm as 10c. For the first cut, a single template file is enough.

## Entrypoint Integration

Extend the manifest-to-env loader with:

```sh
HARNESS_MANAGED_SETTINGS_ENABLED
HARNESS_MANAGED_SETTINGS_MOUNT_PATH
HARNESS_MANAGED_SETTINGS_SOURCE_FILE
HARNESS_MANAGED_SETTINGS_RENDERED_PATH
```

Before launching Claude Code and before `setpriv`, the entrypoint should only
validate that the rendered settings mount exists and is readable. Host-side
rendering is preferred because Phase 8 makes rootfs read-only:

```sh
if [ "${HARNESS_MANAGED_SETTINGS_ENABLED:-0}" = "1" ]; then
  dst="${HARNESS_MANAGED_SETTINGS_RENDERED_PATH:-/etc/claude-code/managed-settings.json}"
  python3 -m json.tool "$dst" >/dev/null
fi
```

The host-side renderer should:

- parse JSON before and after substitution;
- replace only `${TOKEN:id}` placeholders;
- fail closed if token placeholders are present before the broker/token design
  is available;
- fail closed if a referenced token policy is unavailable after the broker
  exists;
- reject placeholders in JSON object keys;
- never log substituted token values.

## Remote MCP and Network Policy

Remote MCP endpoints must be reachable from the sandbox network namespace. Phase 10d should reuse Phase 7's per-generation egress controls:

- add MCP host/port entries to the allowed egress config;
- keep MCP endpoints internal where possible;
- prefer `http` transport for new servers;
- use `sse` only for existing servers that have not migrated.

If the MCP host is not in the generation egress allowlist, Claude Code will see MCP connection failures. That is expected and should be surfaced as configuration error during deployment/lab validation.

## Hooks Policy

Managed settings can carry platform-owned hooks. Recommended first posture:

```json
{
  "allowManagedHooksOnly": true,
  "hooks": {
    "PreToolUse": [...],
    "PostToolUse": [...],
    "UserPromptSubmit": [...],
    "Stop": [...]
  }
}
```

`allowManagedHooksOnly=true` means user/project/local hooks do not run. Use this only if product policy requires harness hooks to be the only trusted hooks. If users should be allowed to add their own hooks, leave it false and rely on managed settings precedence for harness-provided hooks.

Hooks are powerful: they can observe tool inputs/outputs and block tool use. Product docs should state whether managed hooks are active and what they record.

## Implementation Steps

1. Add `harness.managed_settings` config with validation and defaults.
2. Add runtime config fields for source path, mount path, rendered path, and token refs.
3. Add template digest calculation.
4. Materialize the validated template into a content-addressed runtime snapshot.
5. Reject declared MCP token refs unless a separate broker/token design is
   enabled; after that design lands, validate token refs against the broker
   allowlist.
6. Add read-only source-snapshot and rendered managed-settings exact binds through the
   Phase 8 MountPlan.
7. Add managed-settings fields to the control manifest and projected digest logic.
8. Add host-side managed-settings rendering during generation preparation.
9. Add egress config/lab validation for remote MCP hosts.
10. Add snapshot/rendered-artifact retention/GC that never removes content still
    referenced by a live, prepared, or checkpointed generation.
11. Update the entrypoint to validate `/etc/claude-code/managed-settings.json` before launching Claude Code.
12. Tests:
    - `enabled=false` means no mount and no rendered managed settings.
    - Missing template with `enabled=true` fails generation preparation.
    - Template digest changes when hooks or MCP config changes.
    - The generated spec binds a content-addressed source snapshot and/or
      generation-rendered file, not the mutable repo authoring path.
    - Unsupported token placeholders fail generation preparation before Claude
      launches.
    - After a separate broker/token design exists, missing declared token policy
      fails generation preparation and placeholder substitution renders valid
      JSON without logging token values.
    - Generated control manifest includes managed-settings metadata and the projected digest reflects it.
    - Remote MCP host not in egress allowlist fails the lab validation.
    - User workspace and artifact watcher never expose the source or rendered settings file.
    - Snapshot/rendered-artifact GC does not remove content referenced by live
      or checkpointed generations.

## Open Decisions

- Whether `allowManagedHooksOnly` should default true. Recommendation: true for production policy; false in local development if users need custom hooks.
- Whether token policies should be pinned per MCP server or reused through a
  single MCP token issuer. Recommendation: one token policy per remote MCP trust
  domain, designed separately from Phase 8 model proxy auth.
- Whether the rendered managed settings file should be `0600` root-owned. Claude Code runs as uid 65534, so it needs read access unless Claude Code reads before privilege drop; current entrypoint launches Claude after `setpriv`, so use `0644` unless Claude Code supports another managed settings location readable by the configured sandbox group.
- Whether future Phase 11 credential storage should back token issuance with real per-tenant secret versions. Recommendation: yes, but do not block non-secret Phase 10d settings.
