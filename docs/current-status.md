# Current Status

> Last updated: 2026-05-30
> Scope: live baseline after Phase 7 control-plane qualification, completed P0
> lifetime separation, Phase 8 runtime-isolation qualification, the
> pre-Phase 9 runtime cleanup, and Phase 9 driver/Pi integration.

## Snapshot

Project positioning: this repository is best described as an Agent Runtime
Platform: a host-side control plane plus per-session sandbox runtimes for
long-lived AI agent work.

- Frontend workbench: Next.js on port `8000`.
- Orchestrator API: Go service on port `8090`, with optional shared-password
  lab auth via `HARNESS_LAB_PASSWORD`.
- Runtime: gVisor `runsc` direct launch with per-generation OCI specs,
  control manifests, bridge dirs, network profiles, and SQLite persistence.
- Turn execution: Agent Bridge protocol v2 claim/ack is the live path for
  Claude Code, Pi, and shell sessions. Durable turns, events, output, proxy
  correlation, generation state, resource state, messages, sessions, and
  artifact metadata are stored in SQLite before in-memory publish.
- Sandbox boundary: Phase 9 v2 contracts run on the Phase 8
  `sandbox-isolation-v1` runtime boundary. Sandboxes get exact `/workspace`,
  `/agent-home`, `/harness-control`, and bridge binds; no parent `/sessions` or
  `/agent-homes` mounts; no `/harness-secrets`; read-only rootfs; empty OCI
  capabilities; `noNewPrivileges`; non-root agent execution.
- Driver selection: product `Agent`/`Shell` modes resolve through
  `harness.default_agent`, `harness.agents`, `harness.model_profiles`,
  `harness.runtime_providers`, provider capabilities, and the selected
  rootfs `/etc/harness-image/agents.json` manifest. New sessions and
  allocations reject selected drivers missing from the current image manifest.
- Model boundary: upstream provider credentials stay host/proxy-side. Sandbox
  model access uses the stable proxy alias and proxy authorization based on
  live turn context, source IP, contract/resource identity, and entitlement.
  The listener and sandbox alias are configured by `harness.model_proxy`;
  checked-in defaults use host port `8082`.
- Session lifetime: `harness.session_retention: 0s` is the checked-in default,
  so sessions/history/workspaces do not expire automatically. Runtime resource
  lifetime is separate from session/history lifetime.
- Quota: `harness.max_sessions` is a non-terminal session ceiling, independent
  of live `/30` pool capacity. `DELETE /api/sessions/<id>` closes a session and
  frees session quota while preserving history/workspace state.
- Checkpointing: primitives exist behind the Phase 7 control plane, but
  automatic idle checkpointing is disabled in checked-in config.
- Artifacts: host-side metadata scanning backs a read-only live file tree with
  safe downloads and previews for Markdown, code, text, images, JSON, CSV/TSV,
  and PDF.

## Qualification

Phase 8 `sandbox-isolation-v1` is release-qualified at
`345f684b6a6b146311efcb3b3d7a5d7ebb607822`.

Final evidence:
`/tmp/harness-runtime-isolation-final-gates-with-compat.json`

Recorded pins:

- rootfs digest: `sha256:192e6982a36016113633e258947c5a7302a820649cbf91195a34101e590886a5`
- `runsc`: `release-20260511.0`, binary digest `sha256:82b042a8f27f9dd65c58ef6adf87a905ec6c377ec0259ccaf34dd9028b55dc5a`
- proxy commit: `c74d5e0485b8457de68c2e5ac2b32877fbbb3932`

The evidence records `result: passed`, `release_complete: true`, supplied
cutover/reconciliation/rootfs/proxy/adversarial evidence, prior deterministic
compatibility, and gVisor bridge durability compatibility. Runtime/proxy/config
release candidates after `345f684` must regenerate final Phase 8 evidence before
publishing.

Other completed baselines:

- Phase 7 checkpoint-safe control plane: qualified at `d0cdaf6`; details in
  [phase7/README.md](./phase7/README.md) and
  [phase7/release-qualification.md](./phase7/release-qualification.md).
- P0 lifetime separation: complete at `20a8c07`; details in
  [p0-session-lifetime.md](./p0-session-lifetime.md).
- Phase 8 design and gate map: [phase8/README.md](./phase8/README.md).
- Phase 9 driver/provider contract and Pi integration: implemented through
  driver/provider registries, product modes, bridge protocol v2, strict
  proxy-only model grants, Pi rootfs evidence, Pi generated config
  materialization, Pi RPC runner, Pi output normalization, and host-validated Pi
  sidecar restore. The completed baseline includes generic deployment config
  and manifest-backed `runtime_config_digest` /
  `agent_manifest_digest` contract inputs through `7a0843e`.

## Current Flow

```text
POST /api/sessions
  -> resolve product mode through deployment config and image manifest
  -> created

POST /api/sessions/<id>/messages
  -> persist user message
  -> running_active
  -> ensure active generation
     -> reuse live generation, restore checkpointed generation, or cold-start
  -> bridge claim_next_turn / ack_turn_started
  -> bridge emit_output / ack_turn_completed
  -> persist assistant output and artifact metadata
  -> running_idle
```

Agent mode selects the enabled deployment default agent driver (`claude_code`
or `pi`) only when that driver is present in the selected image manifest.
Claude Code uses stream-json. Pi runs as a long-lived RPC process through the
same model-proxy boundary and advances its logical restore sidecar only after
successful completed turns pass runner and host validation. Shell is available
only when enabled in config and present in the image manifest; shell turns use
the same bridge lifecycle, complete through `harness.turn_done`, and can be
interrupted with `POST /api/sessions/<id>/interrupt`.

Canonical session states and public API/event details live in
[architecture.md](./architecture.md). Historical phase logs remain useful as
implementation history, but they are not the source of truth for current
behavior.

## Constraints

- Supported interactive paths are Claude Code, Pi, and the shell shim. Future
  agent adapters must enter through the Phase 9 driver/provider contracts.
- Phase 2 bundle scripts are quarantined smoke tooling and are not
  `sandbox-isolation-v1` release evidence.
- Legacy public session path fields (`workspace`, `agent_home_path`,
  `restore_id`) remain internal compatibility storage and are omitted from
  public DTOs/events.
- Claude logical resume is durable. After a Claude UUID exists in
  `/agent-home`, later turns must use `--resume`; correctness must not depend
  only on an in-memory "first turn" flag.
- Reclaimable runtime resources remain visible for
  `harness.reaper.failed_retention` before physical cleanup by design.
- Phase 10 is the active architecture target after the Phase 9 driver/Pi gate:
  configurable system prompt, context compaction, system skills, and managed
  driver settings behind explicit driver adapters. Production
  auth/authorization, credential rotation, tenant egress policy, resource
  limits, observability, and multi-orchestrator HA are Phase 11.

## Checks

Common regression checks:

```bash
(cd orchestrator && go test ./...)
(cd frontend && npm run lint && npm run typecheck && npm test && npm run build)
python3 -m unittest sandbox-image/tests/test_harness_bridge_client.py
python3 tools/phase8/release-gates.py --static-only
```

After runtime, bridge, Claude CLI, proxy, or session-lifecycle changes, also run
a live two-turn Claude smoke on a fresh session and verify both turns complete
under the same Claude session UUID.

For publishable runtime-isolation candidates, rerun the full evidence-producing
Phase 8 gate sequence in [phase8/release-gates.md](./phase8/release-gates.md).
