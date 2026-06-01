# Phase 10d: Control-Plane-Managed Driver Settings

> Status: planned on top of the completed [Phase 8 runtime isolation hardening](../phase8/README.md).
> Part of [Phase 10](./README.md).

## Goal

Provide one operator-managed settings surface for supported model-backed agent
drivers:

- mandatory hooks for policy or observability;
- remote MCP server registrations available to every agent;
- optional MCP credentials only after a separate broker/token design exists.

Claude Code is the first renderer, using its Linux enterprise managed settings
path:

```text
/etc/claude-code/managed-settings.json
```

Do not write these settings into `$AGENT_HOME/.claude/settings.json`; enterprise
managed settings have higher priority and avoid per-session merge/symlink
behavior.

## Scope

In Phase 10d, MCP servers are deployed elsewhere. The sandbox receives only
driver config that registers remote endpoints.

In scope:

- operator-managed `hooks`;
- remote `mcpServers` using `http` or `sse`;
- host-side validation of hook command paths, MCP transport/URL shape, and MCP
  egress allowlist coverage;
- non-secret settings materialized as content-addressed snapshots and rendered
  artifacts through Phase 8 MountPlan.

Out of scope:

- stdio MCP servers or MCP server lifecycle management inside the sandbox;
- per-user MCP catalogs or UI toggles;
- credential-bearing MCP headers/env/auth fields before a broker/token design;
- reviving `/harness-secrets` for managed settings.

Pi and later drivers must either provide an equivalent managed-settings adapter
or declare `hooks_mcp:unsupported`. Enabling managed settings with an
unsupported driver fails explicitly; silent no-op behavior is not allowed.

## Authoring Shape

Repository source:

```text
sandbox-image/managed-settings/managed-settings.json
```

Example:

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
    }
  }
}
```

The file committed to git must not contain live credentials. `${TOKEN:...}` is
reserved for a future broker/token design and fails generation preparation until
that design exists.

## Config

```yaml
harness:
  managed_settings:
    enabled: true
    source_path: ./sandbox-image/managed-settings/managed-settings.json
    mount_path: /harness-managed-settings
    rendered_path: /etc/claude-code/managed-settings.json
    token_refs: []
```

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

`source_path` is resolved relative to the repository root unless absolute. It
is the authoring source only; the runtime must use a validated
content-addressed snapshot.

## Rendering And Mounts

Generation preparation must:

1. Validate the JSON template and non-secret policy constraints.
2. Reject token placeholders or configured `token_refs` unless a broker/token
   design is enabled.
3. Compute `managed_settings_digest` from the source template bytes.
4. Copy the template into a runtime-owned content-addressed snapshot.
5. Render the driver artifact host-side and mount it at
   `/etc/claude-code/managed-settings.json` through an exact Phase 8 MountPlan
   bind.

The mutable repo path must never be mounted. If a future driver needs the
source snapshot, mount only the content-addressed snapshot read-only. Rendering
must not depend on a writable rootfs; if sandbox-side rendering is ever needed,
the destination must be an explicit tmpfs or bind mount declared by MountPlan.

## Control Manifest And Restore

Add these per-generation manifest fields:

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

Include them in the strict projected control-manifest digest. Post-cutover
checkpoint restore must use the same managed-settings digest and pinned
snapshot/rendered artifact; it must not bind the current repo path as a
substitute. Changes to hooks, MCP endpoints, or future token policy force cold
fallback for checkpointed generations.

## MCP Policy

Supported transports:

- `http` for new remote MCP servers;
- `sse` only for existing remote MCP servers that still require it.

Reject during generation preparation or deployment lab validation:

- `stdio`, `file:`, `unix:`, local path, or shell-command transports;
- credential-bearing `headers`, `env`, or auth fields before broker support;
- `${TOKEN:...}` placeholders in object keys, or anywhere when no broker is
  enabled;
- remote MCP URL host/port pairs missing from the generation egress allowlist.

MCP connection failures caused by egress policy should surface as configuration
errors before a user starts a session.

## Hooks Policy

Hooks are operator policy, not secrets. They may observe prompts, tool inputs,
tool outputs, and stop events depending on the driver. Product docs must say
which managed hooks are active and what they record or block.

Validation rules:

- command hooks must use absolute command paths;
- command paths must resolve to rootfs content or a declared read-only MountPlan
  bind;
- commands under `/workspace`, `/agent-home`, `/tmp`, or any sandbox-writable
  tree are rejected;
- missing, non-regular, non-executable, or sandbox-writable commands fail
  generation preparation;
- hook definitions must not contain token placeholders or secret values.

Use `allowManagedHooksOnly=true` when product policy requires operator hooks to
be authoritative. Local development may choose `false` if user hooks are needed.

## Entrypoint

The entrypoint should only verify that the rendered settings file exists,
is readable, and parses as JSON before launching the driver. Policy validation
and rendering happen on the host before launch.

## Implementation Checklist

1. Add `harness.managed_settings` config, defaults, and validation.
2. Add source template digesting and content-addressed snapshot materialization.
3. Add host-side validation for hooks, MCP transports/URLs, credentials, token
   placeholders, and egress coverage.
4. Add host-side rendering for Claude Code managed settings.
5. Bind the rendered file and any needed source snapshot through Phase 8
   MountPlan.
6. Add manifest fields and projected digest coverage.
7. Add retention/GC that preserves snapshots referenced by live, prepared, or
   checkpointed generations.
8. Expose supported/unsupported mode through the driver registry.
9. Update entrypoint JSON validation.

## Acceptance Tests

- `enabled=false` means no mount and no rendered file.
- Missing or invalid template with `enabled=true` fails generation preparation.
- Template digest changes when hooks or MCP config changes.
- Runtime spec binds content-addressed/generated files, not the mutable repo
  authoring path.
- Token placeholders and credential-bearing MCP config fail until broker support
  exists.
- `stdio`, local transports, and remote MCP hosts outside the egress allowlist
  fail closed.
- Valid `http` and `sse` remote MCP registrations render without deploying MCP
  servers inside the sandbox.
- Hook commands from writable paths are rejected; rootfs/read-only executable
  hook commands are accepted.
- Manifest metadata joins the projected digest.
- Workspace/artifact watchers never expose source or rendered settings files.
- GC does not remove content referenced by prepared, live, or checkpointed
  generations.

## Open Decisions

- Default for `allowManagedHooksOnly`: recommended true for production policy,
  false for local development that needs user hooks.
- Future token policy granularity: recommended one token policy per remote MCP
  trust domain.
- Rendered file mode: use `0644` unless Claude Code can read managed settings
  before privilege drop or via a sandbox group-readable path.
