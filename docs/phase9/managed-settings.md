# Phase 9d: Harness-Managed Claude Code Settings

> Status: planned. Part of [Phase 9](./README.md).

## Goal

Give the harness operator a single, mandatory Claude Code settings surface inside every sandbox session for:

- system hooks (`hooks`) that enforce harness-side policy or observability;
- remote MCP servers (`mcpServers`) that are deployed outside this repo but should be available to every agent;
- MCP bearer tokens or other credentials resolved from the existing `/harness-secrets` mount, not committed to this repo or written into the visible workspace.

The target file is Claude Code's Linux enterprise managed settings path:

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

In Phase 9d, MCP servers themselves are **not** packaged in `harness-platform` and are **not** run as stdio child processes inside the sandbox. They are deployed elsewhere. The sandbox only receives Claude Code config that tells it how to reach those remote MCP endpoints.

Supported MCP transport in this phase:

- `http` (Streamable HTTP) — recommended for new remote MCP servers.
- `sse` — allowed for legacy remote MCP servers.

Out of scope for Phase 9d:

- stdio MCP servers bundled in the sandbox image;
- MCP server lifecycle management;
- independent managed-settings release trees;
- per-user MCP catalogs or UI toggles.

Versioning of the managed-settings payload is the `harness-platform` git history. The runtime pins only a content digest.

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
      "url": "https://mcp.internal.example/doris/mcp",
      "headers": {
        "Authorization": "Bearer ${SECRET:mcp_doris_bearer_token:local}"
      }
    },
    "schema-pack": {
      "type": "sse",
      "url": "https://mcp.internal.example/schema/sse",
      "headers": {
        "Authorization": "Bearer ${SECRET:mcp_schema_bearer_token:local}"
      }
    }
  }
}
```

The file committed to git may contain placeholders but must not contain live credentials.

## Secret Placeholder Resolution

Managed settings may reference secrets already mounted under `/harness-secrets`:

```text
${SECRET:<secret_id>:<secret_version>}
```

Example:

```json
"Authorization": "Bearer ${SECRET:mcp_doris_bearer_token:local}"
```

Entrypoint behavior:

1. Read the managed-settings template from the read-only mount.
2. Replace each `${SECRET:id:version}` placeholder by reading:

   ```text
   ${SECRET_MOUNT_PATH}/id/version
   ```

3. Write the rendered JSON to `/etc/claude-code/managed-settings.json` before `setpriv` launches Claude Code.
4. Keep the rendered file outside `/workspace` and outside `/agent-homes`.

Security notes:

- The rendered file is inside the sandbox and can be read by the Claude Code process. This is acceptable because MCP tokens grant exactly the remote MCP capabilities exposed to the agent.
- The rendered file must not be copied into `/workspace` or artifacts.
- The source template digest should be computed before secret substitution so secret rotation does not force a code digest change. Secret identity/version fields are already represented by the existing secret control-manifest fields; if MCP tokens use additional secret IDs, those IDs and versions must also be pinned in the control manifest.

## Config Shape

```yaml
harness:
  managed_settings:
    enabled: true
    source_path: ./sandbox-image/managed-settings/managed-settings.json
    mount_path: /harness-managed-settings
    rendered_path: /etc/claude-code/managed-settings.json
    secret_refs:
      - id: mcp_doris_bearer_token
        version: local
      - id: mcp_schema_bearer_token
        version: local
```

Go side:

```go
type ManagedSettingsConfig struct {
    Enabled      bool
    SourcePath   string
    MountPath    string
    RenderedPath string
    SecretRefs   []SecretRef
}

type SecretRef struct {
    ID      string
    Version string
}
```

`source_path` is resolved relative to the harness repo root unless absolute.

`secret_refs` makes MCP credential use explicit at generation preparation time. The runtime should validate that every declared secret exists in the generation secret root. The entrypoint still resolves placeholders dynamically from `/harness-secrets`, but the orchestrator owns the allowlist and digest pin.

## Runtime Spec Mount

Mount the template file or containing directory read-only:

```json
{
  "destination": "/harness-managed-settings",
  "type": "bind",
  "source": "/opt/harness-platform/sandbox-image/managed-settings",
  "options": ["rbind", "ro", "nosuid", "nodev", "noexec"]
}
```

`/etc/claude-code/managed-settings.json` itself is rendered inside the writable rootfs by the root entrypoint before dropping privileges.

## Control Manifest Fields

Add to the per-generation control manifest:

```json
{
  "managed_settings_enabled": true,
  "managed_settings_digest": "sha256:9a4c...",
  "managed_settings_mount_path": "/harness-managed-settings",
  "managed_settings_source_file": "managed-settings.json",
  "managed_settings_rendered_path": "/etc/claude-code/managed-settings.json",
  "managed_settings_secret_refs": [
    {"id": "mcp_doris_bearer_token", "version": "local"},
    {"id": "mcp_schema_bearer_token", "version": "local"}
  ]
}
```

Include these fields in the strict-field projection used by the control-manifest digest (see `../phase7/checkpoint-restore.md`). This enforces that:

- A checkpointed generation restores only with the same managed-settings template digest.
- A change to managed hooks or MCP endpoint config forces cold fallback for checkpointed generations.
- A change to declared MCP token secret ID/version also forces cold fallback.
- New sessions pick up the managed settings from the deployed repo at generation creation time.

## Digest Rules

Compute `managed_settings_digest` from the source template bytes, not from the rendered file containing secret values:

```text
sha256:<hex canonical UTF-8 bytes of managed-settings.json template>
```

If the managed-settings directory later grows helper files, switch to the same directory digest algorithm as 9c. For the first cut, a single template file is enough.

## Entrypoint Integration

Extend the manifest-to-env loader with:

```sh
HARNESS_MANAGED_SETTINGS_ENABLED
HARNESS_MANAGED_SETTINGS_MOUNT_PATH
HARNESS_MANAGED_SETTINGS_SOURCE_FILE
HARNESS_MANAGED_SETTINGS_RENDERED_PATH
```

Before launching Claude Code and before `setpriv`:

```sh
if [ "${HARNESS_MANAGED_SETTINGS_ENABLED:-0}" = "1" ]; then
  src="${HARNESS_MANAGED_SETTINGS_MOUNT_PATH:-/harness-managed-settings}/${HARNESS_MANAGED_SETTINGS_SOURCE_FILE:-managed-settings.json}"
  dst="${HARNESS_MANAGED_SETTINGS_RENDERED_PATH:-/etc/claude-code/managed-settings.json}"
  mkdir -p "$(dirname "$dst")"
  python3 /usr/local/bin/harness-render-managed-settings \
    --source "$src" \
    --dest "$dst" \
    --secret-root "$SECRET_MOUNT_PATH"
  chown root:root "$dst"
  chmod 0644 "$dst"
fi
```

The renderer should:

- parse JSON before and after substitution;
- replace only `${SECRET:id:version}` placeholders;
- fail closed if a referenced secret file is absent;
- reject placeholders in JSON object keys;
- never log substituted secret values.

## Remote MCP and Network Policy

Remote MCP endpoints must be reachable from the sandbox network namespace. Phase 9d should reuse Phase 7's per-generation egress controls:

- add MCP host/port entries to the allowed egress config;
- keep MCP endpoints internal where possible;
- prefer `http` transport for new servers;
- use `sse` only for existing servers that have not migrated.

If the MCP host is not in the generation egress allowlist, Claude Code will see MCP connection failures. That is expected and should be surfaced as configuration error during deployment/lab validation.

## Hooks Policy

Managed settings can carry harness-owned hooks. Recommended first posture:

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

Hooks are powerful: they can observe tool inputs/outputs and block tool use. Product docs should state whether harness-managed hooks are active and what they record.

## Implementation Steps

1. Add `harness.managed_settings` config with validation and defaults.
2. Add runtime config fields for source path, mount path, rendered path, and secret refs.
3. Add template digest calculation.
4. Validate declared MCP token secrets exist in the generation secret root.
5. Add read-only managed-settings bind mount in runtime spec generation.
6. Add managed-settings fields to the control manifest and projected digest logic.
7. Add `harness-render-managed-settings` to the sandbox image.
8. Update the entrypoint to render `/etc/claude-code/managed-settings.json` before launching Claude Code.
9. Add egress config/lab validation for remote MCP hosts.
10. Tests:
    - `enabled=false` means no mount and no rendered managed settings.
    - Missing template with `enabled=true` fails generation preparation.
    - Template digest changes when hooks or MCP config changes.
    - Missing declared secret fails generation preparation or entrypoint startup before Claude launches.
    - Placeholder substitution renders valid JSON without logging secrets.
    - Generated control manifest includes managed-settings metadata and the projected digest reflects it.
    - Remote MCP host not in egress allowlist fails the lab validation.
    - User workspace and artifact watcher never expose the source or rendered settings file.

## Open Decisions

- Whether `allowManagedHooksOnly` should default true. Recommendation: true for production harness policy; false in local development if users need custom hooks.
- Whether token secret versions should be pinned per MCP server or reused through a single `mcp_bearer_token` secret. Recommendation: one secret per remote MCP trust domain.
- Whether the rendered managed settings file should be `0600` root-owned. Claude Code runs as uid 65534, so it needs read access unless Claude Code reads before privilege drop; current entrypoint launches Claude after `setpriv`, so use `0644` unless Claude Code supports another managed settings location readable by the harness group.
- Whether future Phase 10 credential storage should replace `secret_refs` with real per-tenant secret versions. Recommendation: yes, but do not block Phase 9d.
