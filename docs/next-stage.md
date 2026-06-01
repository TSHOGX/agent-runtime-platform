# Next Stage: Agent Capability Plane

The next stage turns platform-managed agent behavior into explicit
control-plane capabilities. Operator config is normalized into immutable
session or generation snapshots; driver adapters render driver-specific
artifacts only from those snapshots.

Enabled capabilities must be declared as supported by the selected driver.
Unsupported capabilities fail validation before launch or generation
preparation. They must not silently no-op.

## Goals

- Capability foundation: typed support declarations on driver specs, stable
  manifest fields, and fail-closed validation for enabled capabilities.
- Operator policy prompt: a small per-session policy prompt snapshot delivered
  through the selected driver's prompt adapter.
- Context budget: proxy-reported model usage, hard budget enforcement, and
  optional compaction adapter calls when the driver supports them.
- System skills: read-only `/harness-skills` from a content-addressed snapshot,
  with no mutable repo path mounted directly into the sandbox.
- Managed settings: non-secret settings, hooks, and remote MCP registrations
  rendered through driver policy adapters.

## Non-Goals

- No driver-specific one-off branches in server, runtime, bridge, or frontend
  code.
- No live secrets in prompts, skills, managed settings, `/workspace`,
  `/agent-home`, argv, env, logs, or bridge queues.
- Credential-bearing MCP needs a later broker/token design; do not deliver it
  directly in this stage.
- No skills release system in this stage; repo-authored content snapshots are
  enough for the current implementation path.

## Acceptance Bar

- `Agent` and `Shell` still create and run through the current product mode and
  driver selection path.
- Enabled capabilities are validated against the selected driver before any
  sandbox launch.
- Prepared, live, and restored generations keep the capability snapshots and
  content digests they started with.
- Artifact browsing remains scoped to the verified workspace and never records
  skills or managed settings content.
