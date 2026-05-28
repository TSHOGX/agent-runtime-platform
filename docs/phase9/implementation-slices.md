# Phase 9 Implementation Slices

This document is the implementation checklist for [README.md](./README.md).
It is intentionally split by release slice so schema cleanup, no-op refactors,
bridge abstraction, Pi integration, and secret-grant semantics do not block one
another.

Old-data migration, backups, backfills, and down paths are not Phase 9 work.
When a slice changes schema or contract shape, it can target the current
working schema directly. Compatibility gates are behavioral and contract-level:
Claude Code and shell must keep working, and new v2 contracts must be
internally consistent.

## 9a: Contract and Schema Shape

Goal: reshape current Claude/shell surfaces into driver/provider-shaped slots
without changing behavior.

Deliverables:

1. Add `claude_code` as the canonical registry/config/contract ID while
   keeping `claude` as a legacy alias for current public and persisted surfaces.
   This is a constants and lookup change only; the full `DriverSpec` registry
   belongs to 9b.
2. Change new sandbox contract payloads to `contract_schema_version: 2`.
   Keep `sandbox_contract_version: sandbox-isolation-v1` as the Phase 8
   boundary label.
3. Replace string `driver` in new contract payloads with a `driver` object:
   `driver_id`, `driver_version`, `bridge_protocol`, `output_schema`,
   command/config/runtime-capability digests, and capability booleans.
4. Replace top-level `runtime_adapter` with `runtime_provider`:
   `provider_id`, `provider_profile_id`, `isolation_kind`, `template_ref`,
   `template_digest`, `capability_digest`, and `provider_specific` for runsc
   facts.
5. Add `snapshot_policy` with
   `snapshot_semantic: generation_checkpoint_restore` for `local_runsc`.
6. Add `credential_policy.secret_grants[]` with the full field shape, but allow
   only `domain: model_provider` and `exposure_mode: proxy_only`.
7. Remove SQL-level `agent IN ('claude','sh')` coupling from the active schema.
   9a may keep app-layer validation as a hard-coded set until 9b replaces it
   with the registry.
8. Rename Anthropic-specific runtime profile fields to model-proxy names in the
   active schema and code paths. Real provider credentials remain outside the
   sandbox and outside runtime profiles.
9. Introduce a generic driver state slot, for example
   `session_driver_states(session_id, driver_id, state_payload, state_digest)`,
   for current and future driver-private state. Historical `claude_session_uuid`
   row migration is not required.
10. Generate `/harness-control/driver/<driver_id>/` inside the existing
    read-only `/harness-control` projection. This does not add a bind mount and
    does not change the Phase 8 MountPlan allow-list.

Code touchpoints:

- `orchestrator/internal/agents/agents.go`
- `orchestrator/internal/store/migrations.go`
- `orchestrator/internal/store/resources.go`
- `orchestrator/internal/store/sandbox_contract.go`
- `orchestrator/internal/store/proxy.go`
- `orchestrator/internal/server/server.go`
- `orchestrator/internal/runtime/runtime.go`
- `docs/phase8/fixtures/control-manifest-payload.json`

Gates:

- New contracts write schema v2.
- Contract validation rejects malformed driver/provider objects and
  non-`proxy_only` grants.
- Claude/shell API and event compatibility tests still pass.
- Control manifest contains no host-only paths. The canonical host-side
  sandbox contract may contain bind/resource identity paths, but those fields
  must not be projected into the sandbox. Neither surface may contain provider
  credentials.

## 9b: Driver and Provider Registries

Goal: make driver and provider facts first-class without changing runtime
behavior.

Deliverables:

1. Replace the constants-only agent map with a `DriverSpec` registry:

   ```go
   type DriverSpec struct {
       ID              agents.ID
       LegacyIDs       []agents.ID
       Label           string
       Kind            DriverKind
       BridgeProtocol  BridgeProtocol
       OutputSchema    OutputSchema
       RequiredRuntime []RuntimeCapability
       Capabilities    DriverCapabilities
       ModelAccess     ModelAccessSpec
       Process         ProcessSpec
   }
   ```

2. Register `claude_code`, legacy `claude`, and `sh`.
3. Add `RuntimeProviderSpec(local_runsc)` with capability vocabulary v1.
4. Enforce `driver.required_runtime_capabilities` as a subset of
   `runtime_provider.capabilities` before data-volume provisioning and
   MountPlan generation.
5. Add an internal/operator-facing `GET /api/agents` catalog after the registry
   exists.

Gates:

- Missing provider capabilities fail before allocation.
- Legacy `claude` resolves to `claude_code` for registry facts while public
  compatibility remains intact.
- Capability digests are stable and included in new contracts.

## 9c: Generic Config and Frontend Product Surface

Goal: move deployment choices to generic config while keeping the workbench
product model unchanged.

Deliverables:

1. Add `harness.agents.<id>`, `harness.model_profiles.<id>`, and
   `harness.runtime_providers.<id>`.
2. Treat current `claude:` config as a compatibility input that normalizes into
   `harness.agents.claude_code`.
3. Keep `harness.model_proxy.sandbox_base_url` as the only sandbox proxy alias
   source of truth.
4. Remove any per-profile `sandbox_base_url`; `model_profiles` reference the
   global proxy by `proxy_ref`.
5. Change the frontend to use product modes:

   ```text
   Agent -> configured harness.default_agent
   Shell -> sh
   ```

   Raw `driver_id` is not a user-facing picker value.

Gates:

- Existing deployment config maps to the same Claude Code behavior.
- The frontend still labels coding sessions as `Agent` and shell sessions as
  `Shell`.
- Public DTOs do not expose driver-private state, host paths, or restore IDs.

## 9d: Bridge and Output Refactor

Goal: remove host-side driver-specific turn framing and parser branching.

Deliverables:

1. Replace sandbox `make_turn_runner(agent)` with an `AgentRunner` registry:

   ```python
   class AgentRunner:
       protocol = "..."
       def start(self): ...
       def run_turn(self, content, emit): ...
       def interrupt(self): ...
       def compact(self, instructions=None): ...
       def close(self): ...
   ```

2. Change host-to-sandbox input to driver-neutral `RunTurn` data:

   ```text
   RunTurn { turn_id, content, options }
   ```

   The sandbox runner renders Claude stream-json, Pi RPC JSONL, shell PTY
   input, or future native frames.

3. Replace the Go `stream_parser.go` branching model with an
   `OutputNormalizer` registry:

   ```go
   type OutputNormalizer interface {
       Handle(output runtime.Output) (events []NormalizedEvent, complete bool, err error)
       Flush() []NormalizedEvent
   }
   ```

4. Add and exercise `harness_native_events_v1` for at least one driver so the
   path is real before Pi depends on it.

Gates:

- Claude/shell event output remains compatible at the existing event surface.
- Turn completion is deterministic for each registered driver.
- Unknown native event types fail closed for structured schemas.

## 9e: Pi Driver Integration

Goal: add Pi as a registered driver through the established 9a-9d contracts.

Deliverables:

1. Add rootfs build support for `SANDBOX_AGENT_DRIVERS=pi`.
2. Record Pi CLI version and Pi event schema version in
   `/etc/harness-image/agents.json`.
3. Add generated Pi config under `/harness-control/driver/pi/`.
4. Run Pi as a long-lived RPC process using `/agent-home/.pi/agent` for all
   writable state.
5. Add a Pi `AgentRunner`.
6. Add a Pi `OutputNormalizer`.
7. Add Pi support declarations for system prompt, compaction, skills, hooks,
   MCP, and interrupt. Unsupported capabilities must be explicit.

Gates:

- Pi RPC smoke works without model credentials.
- Pi only reaches models through the sandbox proxy alias during active turns.
- Pi state remains under `/agent-home/.pi/agent`.
- No real provider credentials are visible in sandbox env, argv, control
  files, logs, bridge queues, artifacts, or process listings.
- Pi interrupt/compaction are implemented or explicitly unsupported.
- Pi restore/cold restart uses only `/agent-home` plus persisted driver state.

## 9f: Strict Secret Grants

Goal: make the `secret_grants[]` schema introduced in 9a semantically strict
without enabling new secret exposure modes.

Deliverables:

1. Require `grant_id`.
2. Normalize and validate `scope`.
3. Validate optional `ttl_seconds` with a configured upper bound.
4. Require `allowed_drivers` and validate against the driver registry.
5. Require `allowed_runtime_providers` and validate against the provider
   registry.
6. Include grants in the credential-policy digest.
7. Allow future domains such as `git`, `package_registry`, `mcp_remote`, and
   `webhook` as schema values only when their exposure mode remains
   `proxy_only`.

Gates:

- Phase 9 still rejects `gateway_url`, `brokered_token_env`,
  `brokered_token_file`, and any OS-visible exposure mode.
- Credential-policy digest changes whenever any effective grant field changes.
- Phase 10d must introduce a broker/gateway contract before enabling
  credential-bearing MCP, Git, package registry, or webhook paths.

## Phase 10 and Later

Phase 10 features must land through driver adapters:

```text
10a system prompt -> DriverSystemPromptAdapter
10b compaction    -> DriverCompactionAdapter
10c skills        -> shared /harness-skills mount + DriverSkillsAdapter
10d hooks/MCP     -> DriverPolicyAdapter + DriverMCPAdapter
interrupt         -> DriverControlAdapter
output            -> OutputNormalizer
```

Fanout and base-to-N child branch semantics are not Phase 9 work. They require
provider `branch: true` evidence and a separate object model after
`snapshot_policy.snapshot_semantic == base_branch_fanout` is real.
