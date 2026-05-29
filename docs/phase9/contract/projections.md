# Control Manifest Projection

> Parent: [Sandbox contract schema v2](../sandbox-contract-v2.md).

The sandbox-visible control manifest may contain:

- `driver_id`
- `bridge_protocol`
- generated driver config paths and digests
- model proxy alias when model access is allowed
- Phase 10 adapter metadata after Phase 10 lands

It must not expose:

- host roots
- host gateway internals beyond sandbox-required network projection
- netns paths when not required by the sandbox
- veth names when not required by the sandbox
- DB paths
- bundle/spec/checkpoint host paths
- proxy-internal paths
- provider credential paths

During 9a, sandbox-visible projections should add driver/provider fields, but
9a-9c must keep the protocol-v1 fields still consumed by the current sandbox
bridge runner. Top-level fields such as `agent`, `claude_session_uuid`,
`resume_claude`, and `claude_code_disable_nonessential_traffic` are
projection-only compatibility fields during that window. They must be derived
from canonical `sessions.driver_id`, sidecar state, and config, not from
`sessions.agent`, and they may be removed only in 9d after the bridge runner
reads the generated driver projection and bridge protocol v2 is required.
Because the current `harness-agent-entrypoint` and `harness-bridge-client`
accept only `claude` and `sh`, the 9a-9c protocol-v1 sandbox projection maps
canonical `driver_id: "claude_code"` to sandbox-visible `agent: "claude"` /
`HARNESS_AGENT=claude`; `driver_id: "sh"` projects as `sh`. This mapping is
not a host alias and must not be used by host allocation, contracts, grants,
image manifests, sidecars, restore, or proxy authorization.
For Claude Code, `resume_claude` is derived from
`session_driver_states.state_payload.initialized`; runner-local `first_turn`
state, `CLAUDE_RESUME`, and filesystem probes are not sources of truth for the
fresh/resume selector.

## Driver Config Projection

`/harness-control/driver/<driver_id>/` is generated inside the existing
read-only `/harness-control` projection. It is not a new bind mount and does
not change the Phase 8 MountPlan allow-list.

Examples:

```text
/harness-control/driver/claude_code/settings.json
/harness-control/driver/pi/models.json
/harness-control/driver/pi/settings.json
```

Drivers whose native config loader reads from writable agent-home config still
use this projection as the source of truth. Driver-specific materialization
rules, including Pi's `PI_CODING_AGENT_DIR`, live in
[pi-driver.md](../pi-driver.md). Those materializations are separate from the
control projection: if a driver needs generated config to appear at
`/agent-home`, the v2 contract must declare exact
`mount_plan.driver_config_materializations` and
`driver_runtime.materialized_driver_config` evidence for those pathnames. The
generated `/harness-control/driver/<driver_id>/` directory remains the source
of truth, but the extra pathnames are launch evidence, not implicit behavior.

Any driver that writes config, sessions, caches, sockets, or package data must
declare exact writable paths under `/agent-home` or explicit scratch mounts. No
driver may rely on writable rootfs paths.
