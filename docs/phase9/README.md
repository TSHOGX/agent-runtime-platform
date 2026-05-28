# Phase 9: Agent Driver and Pi Integration

> Status: proposed architecture refactor on top of the completed [Phase 8 runtime isolation hardening](../phase8/README.md).
> Roadmap entry: [PLAN.md -> Phase 9](../PLAN.md#phase-9-agent-driver-and-pi-integration).

## Decision

Introduce first-class `AgentDriverSpec` and `RuntimeProviderSpec` contracts before integrating Pi and before landing Phase 10 agent capability work.

Recommended flow:

```text
Session request
  -> AgentDriverSpec + model profile
  -> RuntimeProviderSpec
  -> capability match
  -> SandboxContractCompiler
  -> DataVolumeProvisioner
  -> MountPlan
  -> RuntimeProvider(local_runsc today)
  -> sandbox AgentRunner
  -> OutputNormalizer
  -> control-plane events
```

`AgentDriver` owns conversation semantics: launch protocol, turns, output normalization, system prompt, skills, hooks, MCP, compaction, interrupt, and driver-private session state.

`RuntimeProvider` owns execution semantics: rootfs/template, process/PTY, filesystem, network, ports, snapshots, pause/resume, cleanup, and host resource identity.

Pi should enter as a registered driver. It should not be added by copying the Claude path through config, schema, control manifests, bridge runners, parser branches, and frontend options.

The selected coding driver is a deployment-time YAML option. Product users still see a generic `Agent` entry in the frontend; they do not choose or see whether that deployed agent is Claude Code, Pi, or another registered driver.

## Current Problem

Phase 8 fixed the runtime boundary: exact `/workspace` and `/agent-home` mounts, per-session/per-driver homes, host-side model credentials, immutable sandbox contracts, live runtime-resource identity, and strict sandbox-visible projections.

The remaining shape is not yet pluggable:

- `orchestrator/internal/agents/agents.go` is only `claude`, `sh`, and a protocol enum.
- `config.Config` still has `ClaudeConfig` instead of generic `agents` and `model_profiles`.
- `agent_runtime_profiles` still has Claude/Anthropic-named columns and a SQL check for `('claude','sh')`.
- `controlManifest` still exposes `claude_session_uuid`, `resume_claude`, and `claude_code_disable_nonessential_traffic`.
- `harness-bridge-client` has `make_turn_runner(agent)` instead of a runner registry.
- `stream_parser.go` is a Claude/shell parser, not an output-normalizer registry.
- `frontend/lib/agents.ts` hardcodes `"claude" | "sh"`.

Adding Pi directly to these surfaces would produce a second special case and make Phase 10 system prompt, compaction, skills, hooks, and MCP harder to generalize.

## Rejected Alternatives

**Copy the Claude branch for Pi.** Fastest smoke path, but it duplicates model config, manifest fields, parser behavior, frontend options, and sandbox runner logic. This is not a release-quality path.

**Treat the runtime provider as the agent abstraction.** E2B, Runloop, Modal, Morph, and OpenHands all separate sandbox/runtime lifecycle from agent behavior. The runtime can execute Claude, Pi, Codex, OpenCode, shell, or fan-out children; it should not own their conversation semantics.

**Add a hidden one-turn Pi spike and evolve it in place.** Acceptable only as non-release evidence. It proves Pi can run, but skips driver home persistence, durable driver state, output normalization, catalog metadata, and restore compatibility.

## Phase 8 Constraints

The refactor must preserve these boundaries:

- Sandbox-visible paths stay limited to exact binds and generated projections.
- `/workspace` is the session workspace; `/agent-home` is the selected `(session_id, driver_id)` home.
- Parent `/sessions`, parent `/agent-homes`, and `/harness-secrets` remain absent.
- Provider model credentials stay host/proxy-only.
- Sandbox-visible compatibility keys are non-secret dummy values and are ignored by proxy authorization.
- Model access remains authorized by trusted host/proxy facts: active turn context, observed sandbox source IP, live runtime resource, driver entitlement, and verified `sandbox-isolation-v1` contract.
- Driver security facts, model entitlement, proxy alias, sandbox identity, runtime provider pin, and template digest are allocation fences.
- Startup probes may call health endpoints, but must not dispatch upstream model requests before a turn is active.

## Core Concepts

- `driver_id`: selected agent runtime, for example `claude_code`, `pi`, `opencode`, `codex`, or `sh`.
- `bridge_protocol`: bridge-to-runner protocol, for example `claude_stream_json_per_turn`, `pi_rpc_jsonl`, or `shell_pty_json`.
- `output_schema`: native stdout schema consumed by an `OutputNormalizer`.
- `model_profile`: host/proxy model access policy and sandbox-visible proxy alias. It is not an upstream credential.
- `driver_home`: persistent `/agent-home` volume keyed by `(session_id, driver_id)`.
- `driver_state`: durable per-session driver metadata, for example Claude session UUID or Pi session metadata.
- `runtime_provider_id`: execution backend; `local_runsc` first.
- `runtime_capability`: typed provider feature such as `exec_stream`, `pty`, `filesystem_rw`, `network_policy`, `snapshot_disk`, `branch`, `port_expose`, `secret_gateway`, or `mcp_gateway`.

## Agent Driver Contract

The registry should hold static, non-secret driver metadata. Deployment choices such as model, enabled drivers, and proxy alias come from config.

```go
type DriverSpec struct {
    ID              agents.ID
    LegacyIDs       []agents.ID
    Label           string
    Kind            DriverKind
    BridgeProtocol  BridgeProtocol
    OutputSchema    OutputSchema
    RequiredRuntime []RuntimeCapabilityRequirement
    Capabilities    DriverCapabilities
    ModelAccess     ModelAccessSpec
    Process         ProcessSpec
}

type DriverCapabilities struct {
    SystemPrompt SupportMode
    Skills       SupportMode
    Hooks        SupportMode
    MCP          SupportMode
    Compaction   SupportMode
    Interrupt    SupportMode
}
```

Every first-class driver must declare:

- identity and version discovery;
- command argv/env template, cwd, writable paths, cache/session paths, and readiness criteria;
- required runtime capabilities;
- turn input shape and whether it is per-turn process or long-lived RPC;
- deterministic turn completion signal;
- native output schema and normalizer;
- model access protocol, sandbox proxy config, dummy/no-key compatibility mode, and usage source;
- system prompt, skills, hooks, MCP, compaction, and interrupt support mode;
- release evidence for non-root execution, no sandbox-visible real credentials, bridge reconnect behavior, and restore/checkpoint compatibility.

Unsupported is an explicit capability state, not a silent no-op.

## Runtime Provider Contract

The provider contract is agent-neutral. Initial provider:

```text
runtime_provider_id: local_runsc
isolation_kind: gvisor
capabilities:
  exec_stream: true
  pty: true
  stdin: true
  kill: true
  filesystem_rw: true
  network_policy: true
  logs: true
  snapshot_disk: true
  snapshot_memory: false
  branch: false
  port_expose: false
  secret_gateway: false
  mcp_gateway: false
```

```go
type RuntimeProviderSpec struct {
    ID               string
    IsolationKind    string
    ProviderVersion  string
    TemplateRef      string
    TemplateDigest   string
    Capabilities     []RuntimeCapability
    CapabilityDigest string
}
```

Phase 8 `RuntimeResourceInstance`, `MountPlan`, and `RuntimeAdapter(runsc)` become the local provider implementation. Future Docker, Kubernetes, E2B, Runloop, Modal, Morph, or microVM providers should satisfy the same lifecycle, process, filesystem, network, gateway, and snapshot contract.

Allocation must validate `driver.required_runtime_capabilities` against `provider.capabilities` before generation preparation.

## Config Direction

Add generic config under `harness:`. Keep legacy Claude config only as a compatibility input until removal.

```yaml
harness:
  default_agent: pi

  agents:
    claude_code:
      enabled: true
      model_profile: anthropic_proxy
      model: sonnet
      output_format: stream-json
      disable_nonessential_traffic: true

    pi:
      enabled: true
      model_profile: anthropic_proxy
      provider: anthropic
      model: claude-sonnet-4-20250514  # example; deployment-selected
      runner_mode: rpc
      no_session: false

    sh:
      enabled: true
      model_profile: none

  model_profiles:
    none:
      access_allowed: false

    anthropic_proxy:
      access_allowed: true
      provider_protocol: anthropic_messages
      sandbox_base_url: http://harness-model-proxy.internal:8082
      compatibility_key_mode: dummy

  runtime_providers:
    local_runsc:
      enabled: true
      default: true
      isolation: gvisor
```

`model_profiles` describe control-plane authorization and proxy projection, not durable upstream secrets.

`default_agent` is an operator/deployment choice. The public UI continues to expose only product modes such as `Agent` and `Shell`; it does not expose `claude_code` vs `pi` as an end-user selector.

## Sandbox Image Composition

Build the sandbox rootfs from the deployed driver set, not from every supported
driver. This keeps images smaller, avoids cold-start-time package installs, and
keeps driver versions reproducible.

Example build inputs:

```bash
SANDBOX_AGENT_DRIVERS=pi FORCE=1 sandbox-image/build-rootfs.sh
SANDBOX_AGENT_DRIVERS=claude_code FORCE=1 sandbox-image/build-rootfs.sh
SANDBOX_AGENT_DRIVERS=pi,claude_code FORCE=1 sandbox-image/build-rootfs.sh
```

The base entrypoint, bridge client, and lightweight shell runner remain base
image content. Driver CLIs are optional image content:

- `claude_code` installs the pinned Claude Code CLI.
- `pi` installs the pinned Pi CLI.
- `sh` does not require a model CLI.

The build writes a rootfs-local image manifest, for example
`/etc/harness-image/agents.json`, containing installed driver IDs, versions,
binary paths, and package digests. At startup or generation allocation,
orchestrator validates that `harness.default_agent` and all enabled
model-backed agents are present in the image manifest. A missing driver is a
configuration error and must fail before allocation.

## Store Direction

Prefer additive migrations first. Remove Claude-specific mirrors only after evidence passes.

Target shape:

```text
agent_drivers(
  driver_id,
  label,
  kind,
  bridge_protocol,
  output_schema,
  enabled,
  capability_payload,
  capability_digest
)

runtime_provider_profiles(
  runtime_provider_profile_id,
  runtime_provider_id,
  provider_version,
  isolation_kind,
  template_ref,
  template_digest,
  capability_payload,
  capability_digest
)

agent_runtime_profiles(
  agent_runtime_profile_id,
  driver_id,
  runtime_provider_profile_id,
  model_profile_id,
  model_access_allowed,
  provider_protocol,
  provider_name,
  model,
  output_format,
  driver_config_payload,
  driver_config_digest,
  required_runtime_capabilities_payload,
  required_runtime_capabilities_digest,
  sandbox_uid,
  sandbox_gid,
  sandbox_supplemental_gids
)

session_driver_states(
  session_id,
  driver_id,
  state_payload,
  state_digest,
  primary key(session_id, driver_id)
)
```

Migration notes:

- Replace `agent_runtime_profiles.agent CHECK(agent IN ('claude','sh'))` with registered-driver validation.
- Keep accepting legacy `claude` as an alias; normalize new sessions to `claude_code` when the migration is ready.
- Move `sessions.claude_session_uuid` into `session_driver_states`.
- Treat public `sessions.agent` as a compatibility field backed by `driver_id`.
- Keep `session_driver_homes(session_id, driver)` as the authoritative driver-home volume, with `driver` renamed to `driver_id` when practical.

## Sandbox Contract and Manifest

Extend the immutable contract with driver/provider identity:

```json
{
  "driver": {
    "driver_id": "pi",
    "driver_version": "...",
    "bridge_protocol": "pi_rpc_jsonl",
    "output_schema": "pi_rpc_events_v1",
    "driver_config_digest": "sha256:...",
    "required_runtime_capabilities_digest": "sha256:..."
  },
  "runtime_provider": {
    "provider_id": "local_runsc",
    "provider_profile_id": "...",
    "isolation_kind": "gvisor",
    "template_digest": "sha256:...",
    "capability_digest": "sha256:..."
  },
  "model_access": {
    "model_access_allowed": true,
    "active_turn_required": true,
    "provider_protocol": "anthropic_messages",
    "sandbox_model_proxy_base_url": "http://harness-model-proxy.internal:8082"
  },
  "driver_runtime": {
    "driver_home_mount": "/agent-home",
    "generated_driver_config_mount": "/harness-control/driver/pi",
    "driver_state_digest": "sha256:..."
  }
}
```

The sandbox-visible control manifest exposes only `driver_id`, `bridge_protocol`, sandbox paths, driver config paths/digests, model proxy alias when allowed, and Phase 10 adapter metadata.

It must not expose host roots, host gateway IP, netns paths, veth names, DB paths, bundle/spec/checkpoint paths, proxy-internal paths, or provider credential paths.

## Runtime Layout

Keep Phase 8 exact mounts:

```text
/workspace
/agent-home
/harness-control
/harness-control/bridge
/etc/hosts when model proxy alias projection is enabled
/schema-pack when configured
/harness-skills when Phase 10c is enabled
explicit tmp/cache mounts
```

Add driver-generated config under:

```text
/harness-control/driver/<driver_id>
```

For Pi:

```text
/agent-home/.pi/agent/                 writable Pi config/session/cache root
/harness-control/driver/pi/models.json read-only generated model/provider map
/harness-control/driver/pi/settings.json optional read-only baseline settings
```

Any driver that writes config, sessions, caches, sockets, or package data must declare exact writable paths under `/agent-home` or scratch mounts. No driver may rely on writable rootfs paths.

## Bridge and Output

Refactor the sandbox Python bridge from `make_turn_runner(agent)` to driver classes:

```python
class AgentRunner:
    protocol = "..."
    def start(self): ...
    def run_turn(self, content, emit): ...
    def interrupt(self): ...
    def compact(self, instructions=None): ...
    def close(self): ...
```

The bridge client remains responsible for heartbeat, `hello`, claim/resume, `ack_turn_started`, `emit_output`, `ack_turn_completed`, and checkpoint-ready markers. The runner owns driver process control and native-to-harness output framing.

Replace `stream_parser.go` branches with an output-normalizer registry:

```go
type OutputNormalizer interface {
    Handle(output runtime.Output) (events []NormalizedEvent, complete bool, err error)
    Flush() []NormalizedEvent
}
```

Initial schemas:

```text
claude_stream_json_v1
shell_harness_json_v1
pi_rpc_events_v1
harness_native_events_v1
raw_text_v1
```

Add `harness_native_events_v1` early so future drivers can normalize in the sandbox runner without adding Go parser branches.

## Pi Driver Shape

Pi should be a long-lived RPC driver, not a per-turn process clone of Claude.

Use:

```text
pi --mode rpc
  --provider <provider>
  --model <model>
  --session-dir /agent-home/.pi/agent/sessions
```

Set:

```text
HOME=/agent-home
PI_CODING_AGENT_DIR=/agent-home/.pi/agent
PI_CODING_AGENT_SESSION_DIR=/agent-home/.pi/agent/sessions
```

Do not use `--no-session` for the production driver; the platform already provides persistent per-driver home. Use `--no-session` only for smoke tests.

Turn flow:

```text
send {"id":"<turn-id>","type":"prompt","message":"..."}
wait for acceptance for that id
stream events until `turn_end` or `agent_end`, according to the runner mode
emit normalized output
ack completed/failed/canceled
```

Pi mapping:

- text deltas -> `agent.delta`;
- final assistant message -> `agent.message`;
- tool execution events -> `agent.output` initially, future typed tool events later;
- `turn_end` / `agent_end` -> completion;
- RPC `abort` -> interrupt;
- RPC `compact` -> native compaction.

Pi model configuration should use a generated non-secret `models.json` pointing at the stable model proxy alias. If Pi requires an API key field, use a dummy value only. Real upstream provider credentials remain host/proxy-only.

## Phase 10 Adapter Impact

Phase 10 features should target driver adapters:

```text
10a system prompt -> DriverSystemPromptAdapter
10b compaction    -> DriverCompactionAdapter
10c skills        -> shared /harness-skills mount + DriverSkillsAdapter
10d hooks/MCP     -> DriverPolicyAdapter + DriverMCPAdapter
interrupt        -> DriverControlAdapter
output           -> OutputNormalizer
```

Claude renderers:

- system prompt via `--append-system-prompt-file`;
- compaction via prompt directive or CLI-specific command until native support exists;
- skills via Claude discovery path;
- hooks/MCP via managed settings.

Pi renderers:

- system prompt via pinned Pi support for `--system-prompt`,
  `--append-system-prompt`, or generated `SYSTEM.md` / `APPEND_SYSTEM.md`
  under `PI_CODING_AGENT_DIR`;
- compaction via native RPC `compact` with optional `customInstructions`;
- skills via `--skill` or settings/resource paths pointing at
  `/harness-skills` when supported;
- hooks/MCP via native support if safe, otherwise explicit `unsupported`.

## API and Frontend

Add an internal/operator-facing catalog endpoint if needed for diagnostics and admin UI:

```text
GET /api/agents
```

Shape:

```json
{
  "default_agent": "pi",
  "agents": [
    {
      "id": "pi",
      "label": "Pi Agent",
      "kind": "coding_agent",
      "enabled": true,
      "supports_interrupt": true,
      "supports_compaction": true
    }
  ]
}
```

The end-user workbench should not use this catalog as a picker. It should continue to show product-level options:

```text
Agent -> configured harness.default_agent
Shell -> sh
```

Public session DTOs can keep the field name `agent` until an API version bump, but the frontend label should remain `Agent` for any coding driver. DTOs and events must never expose driver homes, host paths, restore IDs, or driver-private state. If exposing raw `driver_id` in public API becomes undesirable, add a private/internal field for control-plane use and keep public `agent` as the product-mode value.

## Implementation Sequence

1. Expand `agents` into a static `DriverSpec` registry and keep Claude/shell behavior unchanged.
2. Add `RuntimeProviderSpec` for `local_runsc` and capability validation before allocation.
3. Introduce generic config with compatibility mapping from current Claude config.
4. Add additive store fields/payloads for `driver_id`, driver config digest, runtime provider profile, and driver state.
5. Extend `SandboxContract` and control manifest with driver/provider slots while preserving Phase 8 projection rules.
6. Refactor sandbox `AgentRunner` registry for current Claude and shell.
7. Refactor Go output parsing into `OutputNormalizer` registry.
8. Add an internal `GET /api/agents` catalog if needed, but keep the frontend product picker as `Agent`/`Shell`.
9. Make `sandbox-image/build-rootfs.sh` driver-scoped, pin each selected driver CLI in the rootfs, and add rootfs gates.
10. Add Pi config rendering, RPC runner, output normalizer, and release gates.
11. Rework 10a/10b/10c/10d implementation plans to call driver adapters.
12. Add fan-out only after provider snapshot/branch capabilities exist and are proven.

## Release Gates

Phase 9 gates:

- Claude and shell behavior remains compatible at API/event level.
- Unsupported driver/provider capability pairs fail before allocation.
- Sandbox contracts include driver/provider identity and digests.
- Control manifest projection remains free of host-only fields.
- Public DTOs do not expose driver home, host paths, restore IDs, or driver-private state.
- Output normalizers produce existing `agent.delta`, `agent.message`, `agent.output`, and `system.status` events for Claude/shell.
- Frontend still presents generic `Agent` and does not expose deployment-selected driver identity as a user choice.
- The rootfs image manifest records only selected drivers and their pinned versions.
- Configured drivers that are absent from the rootfs image fail before allocation.
- No driver package install happens during sandbox cold start.

Pi gates:

- Pinned Pi version is installed and recorded from the rootfs.
- `pi --mode rpc --no-session` smoke proves JSONL framing without model credentials.
- Production Pi runner stores state only under `/agent-home/.pi/agent`.
- Pi reaches the model only through the stable sandbox proxy alias during an active turn.
- No real provider credentials appear in env, argv, `/agent-home`, `/harness-control`, bridge queues, process listing, logs, or artifacts.
- Pi completes a turn with a deterministic completion signal.
- Pi interrupt/compaction are implemented through RPC `abort`/`compact` or marked unsupported.
- Pi session/home isolation is proven across two sessions.
- Restore/cold restart uses only `/agent-home` plus persisted driver state.

## References

- Pi RPC mode: https://pi.dev/docs/latest/rpc
- Pi models and provider compatibility: https://pi.dev/docs/latest/models
- Pi usage, sessions, context files, and system prompt files: https://pi.dev/docs/latest/usage
- Pi settings, session directory, skills, and resources: https://pi.dev/docs/latest/settings
- Pi providers: https://pi.dev/docs/latest/providers
- Phase 8 runtime isolation baseline: [docs/phase8/README.md](../phase8/README.md)
