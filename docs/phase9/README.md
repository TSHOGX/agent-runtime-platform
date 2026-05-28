# Phase 9: Driver/Provider Contract and Pi Integration

> Status: architecture plan on top of the completed [Phase 8 runtime isolation hardening](../phase8/README.md).
> Roadmap entry: [PLAN.md -> Phase 9](../PLAN.md#phase-9-agent-driver-and-pi-integration).

Phase 9 turns "agent" from a Claude-shaped string branch into two explicit
contracts:

- `AgentDriverSpec`: conversation semantics, runner protocol, output schema,
  model access, driver-private state, and Phase 10 adapters.
- `RuntimeProviderSpec`: execution semantics, rootfs/template identity,
  process and filesystem primitives, network policy, resource identity, and
  snapshot semantics.

Historical data migration, backups, and down-migration paths are out of scope
for this phase plan. Phase 9 may reshape local/dev schema and contracts
directly. The required compatibility is behavioral: current Claude Code and
shell session APIs, events, mounts, and credential boundaries must not regress.

## Decision

Split Phase 9 into small release slices. The first four slices are intended to
be no-op refactors for existing Claude/shell behavior; Pi enters only after the
contract shape, registry, config, bridge, and output layers are driver-neutral.

| Slice | Scope | Behavior change | Primary gate |
| --- | --- | --- | --- |
| 9a | Contract/schema shape cleanup: contract schema v2, driver/provider objects, `snapshot_policy`, `secret_grants[]` slot, `runtime_provider.provider_specific`, generated `/harness-control/driver/<id>` projection | No | Claude/shell API and event compatibility; new contracts write v2 only |
| 9b | `DriverSpec` and `RuntimeProviderSpec` registries, capability vocabulary v1, allocation-time capability enforcement, internal `GET /api/agents` catalog | No | Unsupported driver/provider pairs fail before allocation |
| 9c | Generic `harness.agents`, `harness.model_profiles`, and `harness.runtime_providers` config; frontend consumes product-mode catalog | No | Existing deployment config maps cleanly; UI still exposes `Agent` and `Shell`, not raw driver IDs |
| 9d | Sandbox `AgentRunner` classes, driver-neutral `RunTurn`, Go `OutputNormalizer` registry, early `harness_native_events_v1` validation | No | Claude/shell output remains byte-for-byte compatible at the event boundary |
| 9e | Pi rootfs contents, Pi RPC runner, Pi normalizer, Pi release gates | Yes | Pi passes its own gates; Claude/shell gates remain green |
| 9f | Make `secret_grants[]` semantics strict: `grant_id`, `scope`, `ttl_seconds`, `allowed_drivers`, `allowed_runtime_providers`, and digest rules | No | Phase 9 still allows only `proxy_only`; broker/gateway modes stay deferred |

Phase 10 must build on the Phase 9 adapter surface. System prompt,
compaction, skills, hooks, MCP, interrupt, and output handling must all route
through `Driver*Adapter` or `OutputNormalizer` implementations. Non-Claude
drivers must be marked `supported`, `unsupported`, or `externalized`; silent
no-op behavior is not allowed.

## Current Code State

The current codebase still has Claude-shaped surfaces that Phase 9 must remove
or contain:

- [`orchestrator/internal/agents/agents.go`](../../orchestrator/internal/agents/agents.go)
  has only `claude`, `sh`, and a protocol enum.
- [`orchestrator/internal/config/config.go`](../../orchestrator/internal/config/config.go)
  exposes `ClaudeConfig` and `ModelProxyConfig`; generic `harness.agents` and
  `harness.model_profiles` do not exist yet.
- [`orchestrator/internal/store/migrations.go`](../../orchestrator/internal/store/migrations.go)
  creates `agent_runtime_profiles.agent CHECK(agent IN ('claude','sh'))` and
  stores Anthropic-named model proxy columns.
- [`orchestrator/internal/store/sandbox_contract.go`](../../orchestrator/internal/store/sandbox_contract.go)
  validates `contract_schema_version == 1`.
- [`orchestrator/internal/server/server.go`](../../orchestrator/internal/server/server.go)
  builds a v1 sandbox contract with a string `driver`, top-level
  `runtime_adapter`, and Linux `forbidden_capabilities`.
- [`orchestrator/internal/runtime/runtime.go`](../../orchestrator/internal/runtime/runtime.go)
  projects `claude_session_uuid`, `resume_claude`, and
  `claude_code_disable_nonessential_traffic` into the control manifest.
- [`sandbox-image/files/usr/local/bin/harness-bridge-client`](../../sandbox-image/files/usr/local/bin/harness-bridge-client)
  selects runners with `make_turn_runner(agent)`.
- [`orchestrator/internal/server/stream_parser.go`](../../orchestrator/internal/server/stream_parser.go)
  mixes Claude stream-json parsing, shell events, and raw text fallback in one
  parser.
- [`frontend/lib/agents.ts`](../../frontend/lib/agents.ts)
  hardcodes `"claude" | "sh"`.

This plan is written against that current state. Pi is the first driver that
will prove the abstraction; it should not be the reason the abstraction is
created in a Pi-shaped way.

## Target Flow

```text
Session request
  -> product mode: Agent | Shell
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

`AgentDriver` owns conversation semantics: launch protocol, turn format,
output normalization, system prompt, skills, hooks, MCP, compaction, interrupt,
and driver-private session state.

`RuntimeProvider` owns execution semantics: rootfs/template, process/PTY,
filesystem, network, ports, snapshots, pause/resume, cleanup, and host
resource identity.

The selected coding driver is a deployment-time choice. Product users still
see a generic `Agent` entry in the workbench; they do not choose or see whether
that deployed agent is Claude Code, Pi, Codex, OpenCode, or another registered
driver.

## Phase 8 Boundaries

Phase 9 must preserve these Phase 8 guarantees:

- Sandbox-visible paths stay limited to exact binds and generated projections.
- `/workspace` is the session workspace; `/agent-home` is the selected
  `(session_id, driver)` home.
- Parent `/sessions`, parent `/agent-homes`, and `/harness-secrets` remain
  absent.
- Provider model credentials stay host/proxy-only.
- Sandbox-visible compatibility keys are non-secret dummy values and are
  ignored by proxy authorization.
- Model access remains authorized by trusted host/proxy facts: active turn
  context, observed sandbox source IP, live runtime resource, driver
  entitlement, and verified `sandbox-isolation-v1` contract.
- Driver security facts, model entitlement, proxy alias, sandbox identity,
  runtime provider pin, and template digest are allocation fences.
- Startup probes may call health endpoints, but must not dispatch upstream
  model requests before a turn is active.

## Detailed Documents

- [implementation-slices.md](./implementation-slices.md): exact 9a-9f
  deliverables, code touchpoints, and gates.
- [sandbox-contract-v2.md](./sandbox-contract-v2.md): contract schema v2,
  manifest projection, snapshot policy, and mount projection rules.
- [runtime-capabilities.md](./runtime-capabilities.md): capability vocabulary
  v1, allocation enforcement, and future provider API families.
- [secret-grants.md](./secret-grants.md): `credential_policy.secret_grants[]`
  shape and Phase 9 enforcement.
- [pi-driver.md](./pi-driver.md): Pi rootfs, RPC runner, normalizer, config
  projection, and release gates.
- [current-code-map.md](./current-code-map.md): current code facts and their
  owning Phase 9 slices.

## Config Direction

`harness.model_proxy` is the single source of truth for the sandbox proxy alias.
`model_profiles` reference it; they do not carry their own
`sandbox_base_url`.

```yaml
harness:
  default_agent: pi

  model_proxy:
    bind_url: http://0.0.0.0:8082
    sandbox_base_url: http://harness-model-proxy.internal:8082

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
      model: claude-sonnet-4-20250514
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
      proxy_ref: default
      compatibility_key_mode: dummy
      usage_source: claude_code_proxy

  runtime_providers:
    local_runsc:
      enabled: true
      default: true
      isolation_kind: gvisor
```

Legacy `claude:` config can remain as a compatibility input while the generic
config lands, but new implementation surfaces should read the normalized
`harness.agents.<id>` model.

## Driver Identity

Do not collapse every identity layer into the same string.

| Layer | Phase 9 value | Reason |
| --- | --- | --- |
| Registry canonical ID | `claude_code` with `LegacyIDs: ["claude"]` | New driver specs should name the actual implementation |
| Existing persisted driver-home key | `claude` | Avoid changing `/agent-home` continuity as part of Phase 9 |
| Public session DTO field | `agent` remains product-compatible | Public API version is not changing in Phase 9 |
| Generic config key | `harness.agents.claude_code` | New config should not carry old shorthand |
| Contract v2 driver object | `driver.driver_id: "claude_code"` | Immutable contracts should describe the selected driver spec |

Persistent key cleanup is not a Phase 9 deliverable. If it happens later, it
should be a fresh schema window with its own proof for driver-home continuity;
old row migration and backups are not part of this Phase 9 plan.

## Sandbox Image Composition

Build the sandbox rootfs from the deployed driver set, not from every supported
driver:

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

The build writes `/etc/harness-image/agents.json` with installed driver IDs,
versions, binary paths, package digests, and event schema pins where relevant.
At allocation, the orchestrator must fail before runtime creation if the
configured driver is absent from the image manifest.

## Bridge and Output

Phase 9d changes the host-to-sandbox bridge boundary from driver-specific input
frames to a driver-neutral turn request:

```text
RunTurn {
  turn_id,
  content,
  options
}
```

The sandbox `AgentRunner` converts that request into Claude stream-json, Pi
RPC JSONL, shell PTY input, or another native driver protocol. The Go side
consumes normalized output through an `OutputNormalizer` registry.

Initial output schemas:

```text
claude_stream_json_v1
shell_harness_json_v1
pi_rpc_events_v1
harness_native_events_v1
raw_text_v1
```

`harness_native_events_v1` must be exercised by at least one driver in 9d, so
future drivers can normalize inside the sandbox runner without adding another
Go parser branch.

## Pi Driver Shape

Pi should be a long-lived RPC driver, not a per-turn clone of Claude Code.

```text
pi --mode rpc
  --provider <provider>
  --model <model>
  --session-dir /agent-home/.pi/agent/sessions
```

Sandbox environment:

```text
HOME=/agent-home
PI_CODING_AGENT_DIR=/agent-home/.pi/agent
PI_CODING_AGENT_SESSION_DIR=/agent-home/.pi/agent/sessions
```

Production Pi must not use `--no-session`; the platform already provides a
persistent per-driver home. `--no-session` is only a smoke-test mode.

Pi release gates:

- Pinned Pi CLI version is recorded in the rootfs image manifest.
- Pinned Pi event schema version, for example `pi_rpc_events_v1.0`, is recorded
  next to the CLI version.
- Pi normalizer rejects unknown event types instead of silently passing them
  through.
- Pi CLI upgrades require paired event-schema review.
- `pi --mode rpc --no-session` smoke proves JSONL framing without model
  credentials.
- Production Pi stores state only under `/agent-home/.pi/agent`.
- Pi reaches the model only through the stable sandbox proxy alias during an
  active turn.
- No real provider credentials appear in env, argv, `/agent-home`,
  `/harness-control`, bridge queues, process listings, logs, or artifacts.
- Pi completes a turn with a deterministic completion signal.
- Pi interrupt/compaction use RPC `abort`/`compact` or are explicitly
  `unsupported`.
- Pi session/home isolation is proven across two sessions.
- Restore/cold restart uses only `/agent-home` plus persisted driver state.

## Product Lessons Coverage

| Source | Phase 9 implements | Phase 10 depends on | Future only |
| --- | --- | --- | --- |
| OpenHands action/observation | 9d output normalizer and driver-neutral `RunTurn` | Adapter typed events for 10a-10d | Typed tool events and replay |
| E2B templates | 9a `template_ref` and `template_digest`; 9e first Pi template | None | Multi-template registry and marketplace |
| E2B/Morph base-to-branch fanout | 9a `snapshot_policy.snapshot_semantic` slot | None | Fanout objects after provider `branch` support |
| Modal orthogonal resources | 9b capability vocabulary v1 and allocation enforcement | None | `tunnel`, `wake_on_http`, and `metrics` vocabulary v2 |
| Runloop Agent Gateway | 9a `secret_grants[]` slot; 9f strict grants with `proxy_only` only | 10d can introduce `gateway_url` after broker design | Brokered short-term tokens |
| Runloop devbox process API | 9d driver-neutral turn execution | 10d hooks/MCP can depend on explicit driver support | Full provider process API interfaces |

## Release Gates

Phase 9 gates:

- Claude Code and shell behavior remain compatible at API and event level.
- Unsupported driver/provider capability pairs fail before allocation.
- Sandbox contracts include driver/provider identity, capability digests,
  template digest, snapshot policy, and credential policy.
- Control manifest projection remains free of host-only fields.
- `/harness-control/driver/<driver_id>/` is generated inside the existing
  read-only `/harness-control` projection; it is not a new bind mount.
- Public DTOs do not expose driver home, host paths, restore IDs, or
  driver-private state.
- Output normalizers produce the existing `agent.delta`, `agent.message`,
  `agent.output`, and `system.status` event surface for Claude/shell.
- The frontend still presents generic `Agent` and `Shell`.
- The rootfs image manifest records only selected drivers and their pinned
  versions.
- Configured drivers that are absent from the rootfs image fail before
  allocation.
- No driver package install happens during sandbox cold start.
- `secret_grants[].exposure_mode != "proxy_only"` is rejected throughout
  Phase 9.

## References

- Phase 8 runtime isolation baseline: [docs/phase8/README.md](../phase8/README.md)
- Phase 8 sandbox contract: [docs/phase8/sandbox-contract.md](../phase8/sandbox-contract.md)
- Phase 10 adapter plan: [docs/phase10/README.md](../phase10/README.md)
- Pi RPC mode: https://pi.dev/docs/latest/rpc
- Pi models and provider compatibility: https://pi.dev/docs/latest/models
- Pi usage, sessions, context files, and system prompt files: https://pi.dev/docs/latest/usage
- Pi settings, session directory, skills, and resources: https://pi.dev/docs/latest/settings
- Pi providers: https://pi.dev/docs/latest/providers
