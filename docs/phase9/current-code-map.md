# Current Code Map

This map records the current worktree facts that Phase 9 is designed to
change. It should be updated as slices land so the plan stays anchored to code,
not only target architecture.

Old data migration, backup, and down-path work is intentionally excluded from
this map. The code changes listed here target the active schema and runtime
shape.

| Area | Current code fact | Phase 9 owner | Target |
| --- | --- | --- | --- |
| Agent constants | `orchestrator/internal/agents/agents.go` defines only `claude`, `sh`, `ProtocolClaudeStreamJSON`, and `ProtocolShellPTY`. | 9a, 9b | 9a adds `claude_code` canonical ID and legacy alias lookup. 9b replaces constants-only metadata with `DriverSpec`. |
| Driver registry | No `DriverSpec` structure exists. | 9b | Register `claude_code`, legacy `claude`, `sh`, and later `pi` with capabilities, bridge protocol, output schema, model access, process facts, and support modes. |
| Generic config | `orchestrator/internal/config/config.go` exposes `ClaudeConfig` and `ModelProxyConfig`; generic `harness.agents`, `harness.model_profiles`, and `harness.runtime_providers` are absent. | 9c | Normalize legacy `claude:` config into generic agent/model/runtime-provider config. |
| Model proxy alias | `harness.model_proxy` exists and is mirrored into legacy Claude config. | 9c | Keep `harness.model_proxy.sandbox_base_url` as the only sandbox proxy alias source. `model_profiles` reference it by `proxy_ref`. |
| Runtime profile schema | `agent_runtime_profiles.agent` has a SQL check for `('claude','sh')`; profile identity includes Anthropic-named model proxy/secret columns. | 9a, 9b | Remove SQL-level driver enum coupling from the active schema. Rename model-proxy fields away from Anthropic naming. Registry validation replaces hard-coded checks in 9b. |
| Sandbox contract validation | `orchestrator/internal/store/sandbox_contract.go` accepts only `contract_schema_version == 1`. | 9a | New writes use contract schema v2 with driver/provider objects, snapshot policy, and secret grants. |
| Sandbox contract builder | `orchestrator/internal/server/server.go` builds a v1 contract with string `driver`, top-level `runtime_adapter`, runsc facts at that level, and Linux `forbidden_capabilities`. | 9a | Build v2 payloads with `driver`, `runtime_provider.provider_specific`, product capability digests, `snapshot_policy`, and `credential_policy.secret_grants[]`. |
| Proxy authorization contract | `orchestrator/internal/store/proxy.go` reads current credential policy and contract fields for model-proxy authorization. | 9a | Read v2 `driver.driver_id` and secret grant posture while preserving Phase 8 host/proxy-only checks. |
| Control manifest | `orchestrator/internal/runtime/runtime.go` projects `agent`, `claude_session_uuid`, `resume_claude`, and `claude_code_disable_nonessential_traffic`. | 9a, 9d | Add driver/provider projection and generated driver config path. Keep current Claude fields only as temporary compatibility mirrors until runner code no longer needs them. |
| Driver state | `sessions.claude_session_uuid` is the only durable driver-private session state. | 9a | Add a generic `session_driver_states(session_id, driver_id, state_payload, state_digest)` slot for new driver state. Historical row backfill is not required. |
| Driver homes | `session_driver_homes(session_id, driver)` is the authoritative persistent `/agent-home` source. | 9a, later phase | Keep existing persisted driver key behavior during Phase 9. Do not mix this with `claude_code` persisted key cleanup. |
| Host turn input | `runtime.writeUserTurn` chooses Claude vs shell input frame by protocol. | 9d | Host bridge sends driver-neutral `RunTurn`; sandbox runners render native frames. |
| Sandbox runner | `sandbox-image/files/usr/local/bin/harness-bridge-client` uses `make_turn_runner(agent)` with explicit `claude` and `sh` branches. | 9d | Replace with an `AgentRunner` registry and add Pi after the registry exists. |
| Output parsing | `orchestrator/internal/server/stream_parser.go` mixes Claude stream-json, shell events, and raw text fallback in one parser. | 9d | Replace with an `OutputNormalizer` registry. Exercise `harness_native_events_v1` before Pi depends on it. |
| Frontend picker | `frontend/lib/agents.ts` hardcodes `RuntimeAgent = "claude" \| "sh"` and maps them to `Agent`/`Shell`. | 9c | Continue presenting product modes `Agent` and `Shell`; do not expose raw deployment-selected driver IDs as user choices. |
| Rootfs driver content | The current docs describe driver-scoped rootfs composition, but the code does not yet gate configured drivers against an image manifest. | 9e | Build rootfs from selected drivers and verify `/etc/harness-image/agents.json` before allocation. |
| Phase 10 docs | Phase 10 already states it builds on Phase 9, but individual sub-targets still need implementation to use driver adapters. | Phase 10 | Phase 9 docs define the adapter boundary; Phase 10 implementation plans should consume it rather than adding Claude-only paths. |
