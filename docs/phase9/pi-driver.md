# Pi Driver

> Status: implemented Phase 9 baseline. The current checked-in lab profile uses
> Pi as the product `Agent` default.

Pi is a registered driver on top of the driver/provider contract, capability
check, bridge runner abstraction, output normalizer registry, and strict
model-provider secret grants. It is not a second Claude-shaped branch.

The 9f Pi schema-widening gate made Pi selectable. The 9a schema intentionally
constrained `sessions.driver_id`, `agent_runtime_profiles.driver_id`, and
`session_driver_states.driver_id` to Claude Code and shell; the Pi schema update
widened those canonical-driver checks to include `pi`. This widening is
preserving, not a release reset. If future SQLite constraints require table
rebuilds, the migration must copy valid post-9a/9c/9e canonical rows into the
rebuilt tables inside the migration transaction, keep foreign keys consistent,
and leave valid Claude Code and shell sessions, sidecars, runtime profiles,
active generations, checkpointed generations, message history, and artifact
metadata intact. Any future choice to make Pi enablement a release reset must
be explicit and gated separately before it deletes active or checkpointed
state.

## Rootfs and Image Manifest

For the checked-in lab default:

```bash
SANDBOX_AGENT_DRIVERS=pi,sh ./sandbox-image/build-rootfs.sh
```

The active rootfs manifest must include `pi` and `sh`. Use `FORCE=1` with the
same driver set when the existing rootfs lacks a selected driver CLI or
otherwise requires a full rebuild:

```bash
SANDBOX_AGENT_DRIVERS=pi,sh FORCE=1 ./sandbox-image/build-rootfs.sh
```

For a Pi-only deployment, `SANDBOX_AGENT_DRIVERS=pi` is valid, but Shell must
then be disabled or unavailable in deployment capabilities.

9c introduced the generic image-manifest generator and Claude/shell rootfs
parameterization. 9f extended that same build input and manifest schema with
Pi; there is no separate hand-written Pi manifest path.

The image manifest at `/etc/harness-image/agents.json` must record:

- `driver_id: pi`
- pinned Pi CLI version
- pinned Pi event schema version, for example `pi_rpc_events_v1.0`
- binary path
- package or binary digest
- installed config/resource paths

The image manifest digest is recorded separately as
`input_digests.agent_manifest_digest`. It is not the full rootfs/template
content fence; `input_digests.rootfs_image_digest` keeps that role when the
selected runtime provider exposes one.

The generic allocation gate landed in 9c: allocation fails before runtime
creation if the selected driver is not present in the image manifest. 9f added
the Pi-specific manifest entries above and proved that
`harness.default_agent: pi` and enabled Pi profiles pass that gate only when
the image contains Pi.

## Runtime Command

Production Pi runs as a long-lived RPC process:

```text
pi --mode rpc
  --provider <harness_proxy_provider_id>
  --model <model>
  --session-dir /agent-home/.pi/agent/sessions
```

Cold restart session selection is conditional on the pinned Pi release
evidence. The current `/latest` RPC docs list startup session-directory and
no-session controls and expose session switching through RPC, so Phase 9 must
not treat an undocumented `--session` startup flag as normative. For the exact
Pi CLI version installed in the image, release evidence must prove one of these
restore paths:

- If pinned CLI evidence documents and verifies a startup session selector,
  start Pi with `--session-dir` plus that selector and record the exact argv,
  accepted selector value, and `get_session_stats` result.
- Otherwise, start Pi with `--session-dir`, call RPC `switch_session` once with
  the host-validated persisted session file or ID, then verify the selected
  session with `get_session_stats` before accepting work.
- If the pinned CLI proves neither path, Pi physical/logical restore from a
  persisted session fails closed and that Pi version is not selectable for
  resumable sessions.

Sandbox environment:

```text
HOME=/agent-home
PI_CODING_AGENT_DIR=/agent-home/.pi/agent
PI_CODING_AGENT_SESSION_DIR=/agent-home/.pi/agent/sessions
PI_OFFLINE=1
PI_SKIP_VERSION_CHECK=1
PI_TELEMETRY=0
```

Production must not use `--no-session`. That flag is reserved for smoke tests
that prove JSONL framing without persistent state.

`PI_OFFLINE=1` is the primary startup egress gate. `PI_SKIP_VERSION_CHECK=1`
and `PI_TELEMETRY=0` are required defense-in-depth settings because Pi exposes
separate controls for version checks and telemetry. If a future Pi version
requires startup network activity for package, provider, or model discovery,
that work must move behind an explicit Phase 10 broker/gateway contract or the
active-turn model-proxy path; it is not allowed during sandbox cold start.

## Generated Config

Pi generated source config is written under the existing control projection:

```text
/harness-control/driver/pi/models.json
/harness-control/driver/pi/settings.json
```

These files are read-only from the sandbox. They may contain:

- model/provider map
- sandbox model proxy alias
- non-secret placeholder key if Pi requires an API key field
- driver settings selected by deployment config

They must not contain real upstream provider credentials. Pi must reach the
model through `harness.model_proxy.sandbox_base_url` during an active turn.

Pi's config loader reads custom models and settings from
`PI_CODING_AGENT_DIR`, which is `/agent-home/.pi/agent` in production. The
runtime setup must therefore materialize the generated control projection at
that path before Pi starts without making the config file pathnames mutable by
the sandbox:

```text
/agent-home/.pi/agent/models.json   -> /harness-control/driver/pi/models.json
/agent-home/.pi/agent/settings.json -> /harness-control/driver/pi/settings.json
```

Phase 9 materializes those two pathnames as exact read-only file bind entries
in the v2 `mount_plan.driver_config_materializations` object. Each entry must
also appear in `driver_runtime.materialized_driver_config` with the generated
source projection path, source digest, sandbox destination, and
`destination_mutable_by_sandbox: false`. The bind sources are the generated
`/harness-control/driver/pi/` projection files, and the bind destinations are
exactly:

```text
/agent-home/.pi/agent/models.json
/agent-home/.pi/agent/settings.json
```

Symlinks inside a sandbox-writable `/agent-home` tree are not allowed for Pi
config materialization in Phase 9 because the sandbox could otherwise retarget,
replace, or unlink the pathname before Pi opens it. Copies into
`/agent-home/.pi/agent` are also not allowed. A future symlink-based option
would need a host-owned immutable parent path, separately declared writable
subpaths, and validation evidence equivalent to the file-bind contract.

The runner validates the materialized config before Pi startup, before every
turn is submitted, and before every reconnect/cold-restart attach. It rejects
the operation if `PI_CODING_AGENT_DIR` is not `/agent-home/.pi/agent`, if the
materialized files are missing, if either destination is not an exact
read-only file bind backed by the generated `/harness-control/driver/pi/`
projection source and digest in the v2 contract, or if the sandbox can mutate
either config pathname.

The generated `models.json` must use Pi's native custom-provider object schema
with a harness-specific provider ID such as `harness_anthropic_proxy`:

```json
{
  "providers": {
    "harness_anthropic_proxy": {
      "baseUrl": "http://harness-model-proxy.internal:8082",
      "api": "anthropic-messages",
      "apiKey": "harness-model-proxy-dummy-key",
      "models": [
        {
          "id": "<model>"
        }
      ]
    }
  }
}
```

The provider definition points at `harness.model_proxy.sandbox_base_url`, uses
Pi's Anthropic Messages custom-provider API when the upstream model profile is
Anthropic Messages, and includes only a non-secret dummy API key. It must not
use Pi's built-in `anthropic` provider ID, a top-level harness
`schema_version`, a legacy top-level `models` array, or real upstream provider
credentials, because those either fail against the pinned Pi schema or bypass
the harness proxy boundary.

## Writable State

All Pi writable state must stay under:

```text
/agent-home/.pi/agent
```

The writable state contract must enumerate Pi's writable subpaths, including at
least `/agent-home/.pi/agent/sessions`, separately from the two read-only
config file binds above. No Pi cache, socket, session, package data, or
settings write may target the read-only rootfs, `/harness-control`,
`/agent-home/.pi/agent/models.json`, or
`/agent-home/.pi/agent/settings.json`.

## Restore State

Pi restore state is the generic `session_driver_states` sidecar for
`driver_id: pi`. New Pi-backed sessions do not create a sidecar at session
creation, because `session_driver_states.updated_generation_id` must reference
an existing `runtime_generations` row. First allocation for a new Pi
session/driver pair creates the generation row, inserts the canonical
uninitialized sidecar in that same allocation transaction with
`state_version: 1`, and returns the digest/version as the allocation
start-state token. The later contract write revalidates that token against the
still-owned lease and current Pi sidecar before snapshotting the digest into
the v2 contract. The canonical bootstrap payload is:

```json
{
  "schema_version": 1,
  "driver_id": "pi",
  "state_kind": "pi_uninitialized",
  "session_dir": "/agent-home/.pi/agent/sessions"
}
```

The first Pi generation snapshots this digest into
`driver_runtime.initial_driver_state_digest` and starts Pi with `--session-dir`
but no explicit session selector. Production still must not use `--no-session`.
After the first successful `completed` turn, the runner CAS-updates the sidecar
to the initialized payload with `state_version: 2`.

The initialized canonical payload is:

```json
{
  "schema_version": 1,
  "driver_id": "pi",
  "state_kind": "pi_session",
  "session_dir": "/agent-home/.pi/agent/sessions",
  "selected_session_relpath": "<session>.jsonl",
  "selected_session_file": "/agent-home/.pi/agent/sessions/<session>.jsonl",
  "selected_session_id": "<pi-session-id>",
  "last_completed_turn_id": "<turn-id>"
}
```

After every successful `completed` turn, the Pi runner must call RPC
`get_session_stats`, read `sessionFile` and `sessionId`, and validate
`sessionFile` before sending a driver-state update. The runner check is not
trusted persistence validation. The accepted runner value must be a clean
relative session path under `session_dir` or a canonical path whose resolved
target stays under `/agent-home/.pi/agent/sessions`. Absolute paths outside that
root, `..` segments, empty names, symlinks that escape the session root, and
paths whose realpath cannot be verified are rejected. The runner sends both the
normalized relative session path and the canonical sandbox path in
`driver_state_update.state_payload`; it never writes the sidecar directly.

Pi does not advance sidecar state for `failed` or `canceled` terminal turns,
even if the Pi process created or switched a session file before the failure or
cancel. Those filesystem changes remain ordinary agent-home data and may be
captured by later physical checkpoints, but they are not a trusted logical
restore point until a later successful `completed` turn passes
`get_session_stats`, runner path validation, host DataVolume-backed validation,
digest recomputation, and CAS. The bridge must omit
`driver_state_update` on failed/canceled Pi acknowledgements; if one is present,
the host Pi validator rejects the completion before sidecar mutation.

After runner validation for a successful `completed` turn, the runner includes
the new payload in `ack_turn_completed.payload.driver_state_update`. The host
applies that update with the generic driver-state compare-and-swap API in the
same transaction as terminal turn state and active model-request cleanup, but
only after `CompleteTurn` invokes the host Pi driver-state validator. That
validator is the trusted boundary. It loads the current contract and verified
`data_volumes.agent_home` row, maps the sandbox session path to the canonical
host agent-home path, re-runs clean-relative/canonical-path validation using
host `lstat`/`realpath`, rejects symlink escape and missing-file races,
normalizes the persisted relative path, requires non-empty `sessionId`, and
recomputes `state_digest` from canonical payload bytes. CAS runs only on the
host-validated payload and digest; any mismatch between runner-supplied digest
and host recomputation fails closed. Physical checkpoint metadata is fenced
separately by the idle checkpoint path through
`checkpoint_driver_states_digest`. A missing session file, empty session ID,
host validation failure, digest mismatch, or CAS miss fails the turn completion
path without replacing newer sidecar state.

On cold restart, restore first runs the same host Pi driver-state validator
against the persisted sidecar and current verified agent-home DataVolume before
runtime launch. The runner then re-validates the persisted normalized session
path and selects that session using the mechanism proven by the pinned release
evidence: either a verified startup selector, or startup with `--session-dir`
followed by one RPC `switch_session`. It then calls `get_session_stats` before
accepting work. The returned `sessionFile` must pass the same runner validation
and the validated `sessionFile` and `sessionId` must match the host-validated
sidecar payload; otherwise restore fails closed and the generation is not
allowed to run turns.

## Turn Flow

The sandbox `AgentRunner` receives driver-neutral `RunTurn` input and writes Pi
RPC JSONL to the long-lived process:

```text
send prompt for turn_id
wait for acceptance for that turn_id
stream events until turn_end or agent_end
emit normalized output
ack completed, failed, or canceled
```

Initial normalizer mapping:

| Pi signal | Harness result |
| --- | --- |
| successful `response` | `system.status` |
| failed `response` / `error` | fail closed |
| assistant `message_update` `text_delta` | `agent.delta` |
| assistant `message_update` `text_start` / `text_end` | `agent.output` |
| final assistant message | `agent.message` |
| non-assistant `message_end` | `system.status` |
| `agent_start`, `turn_start`, `message_start`, queue/compaction/retry lifecycle | `system.status` |
| `tool_execution_*` | `agent.output` initially |
| `turn_end` / `agent_end` | `system.status` and turn completion |
| RPC `abort` command | interrupt |
| RPC `compact` command | compaction |

Unknown event types must fail closed. The normalizer should not silently pass
through unknown structured events because Pi CLI upgrades can change the event
schema.

## Phase 10 Adapter Declarations

Pi must declare support mode for every Phase 10 feature:

| Feature | Initial Pi stance |
| --- | --- |
| system prompt | Use pinned Pi support such as `--system-prompt`, `--append-system-prompt`, or generated files under `PI_CODING_AGENT_DIR`; otherwise `unsupported` |
| compaction | Native RPC `compact` when available; otherwise `unsupported` |
| skills | Native `--skill` or settings/resource paths to `/harness-skills` when available; otherwise `unsupported` |
| hooks/MCP | Native support only after credential and policy gates pass; otherwise `unsupported` |
| interrupt | RPC `abort` when available; otherwise `unsupported` |

Unsupported means the adapter explicitly rejects the feature. It does not mean
silently ignoring operator policy.

## Release Evidence

Pi docs under `/latest` are discovery references only. Release evidence for the
exact pinned Pi CLI and event schema that the sandbox image installs is stored
under a versioned fixture directory, currently `0.77.0`:

```text
docs/phase9/fixtures/pi/<pi-cli-version>/
  cli-version.txt
  image-manifest-agent-entry.json
  rpc-no-session-smoke.jsonl
  rpc-session-stats.json
  rpc-session-selection.jsonl
  event-schema.json
  event-normalizer-corpus.jsonl
  verified-behavior.md
```

The evidence must record the exact CLI version, package or binary digest,
event schema identifier, observed RPC framing, completion event behavior,
`get_session_stats` fields, documented startup options, session selection or
switch behavior, startup egress result, and the documentation URLs plus
retrieval date used during the verification. Normalizer tests consume the
checked-in event corpus; they do not depend on live `/latest` docs.

## Release Gates

- Pinned Pi CLI version is recorded in the agent image manifest.
- Pinned Pi event schema version is recorded next to the CLI version.
- Versioned Pi release evidence exists in `docs/phase9/fixtures/pi/` for the
  pinned CLI, pinned event schema, RPC/session behavior, and normalizer corpus.
- Pi CLI upgrades require paired event-schema review.
- `pi --mode rpc --no-session` smoke proves JSONL framing without model
  credentials.
- Production Pi stores state only under `/agent-home/.pi/agent`.
- Production Pi sets `PI_OFFLINE=1`, `PI_SKIP_VERSION_CHECK=1`, and
  `PI_TELEMETRY=0`.
- Pi cold start performs no version-check, telemetry, package, provider, model,
  or other non-model-proxy egress.
- Pi uses generated Pi-native `models.json` custom-provider config with a
  harness proxy provider ID, never a Pi built-in upstream provider ID, and
  reaches models only through the sandbox proxy alias during active turns.
- Pi startup proves that `/agent-home/.pi/agent/models.json` and
  `/agent-home/.pi/agent/settings.json` are materialized from
  `/harness-control/driver/pi/`, match the generated config digests, and are
  not mutable by the sandbox UID/GID.
- Pi runner revalidates the materialized config before each turn and before
  reconnect/cold-restart attach; symlink materialization, exact file binds that
  no longer resolve to the generated projection, missing files, or
  sandbox-mutable config pathnames fail closed before Pi consumes config.
- Pi selection requires the strict model-provider grant to include `pi` in
  `allowed_drivers` and the selected runtime provider ID in
  `allowed_runtime_providers`.
- A new Pi-backed session can allocate its first generation by creating the
  canonical `pi_uninitialized` sidecar only after the generation row exists and
  in the same allocation transaction that returns the start-state token; the
  later contract write must revalidate lease ownership plus sidecar
  digest/version before snapshotting it into the contract. Later missing Pi
  sidecars fail closed before launch or restore.
- No real provider credentials appear in env, argv, `/agent-home`,
  `/harness-control`, bridge queues, process listings, logs, or artifacts.
- Pi completes a turn with a deterministic completion signal.
- Pi advances sidecar state only on successful `completed` turns. Failed or
  canceled terminal turns, including cases where Pi created or switched a
  session file before the terminal status, do not update
  `session_driver_states`; any `driver_state_update` attached to those statuses
  fails closed.
- Pi interrupt/compaction are implemented through RPC `abort`/`compact` or
  explicitly marked `unsupported`.
- Pi session/home isolation is proven across two sessions.
- Restore/cold restart uses only `/agent-home` plus persisted driver state,
  selects the Pi session only through a pinned-evidence startup selector or RPC
  `switch_session`, and validates the selected Pi `sessionFile` / `sessionId`
  on both runner and host, including canonical path and symlink containment
  under the verified agent-home DataVolume.

## References

These links are non-normative discovery references. The normative 9f gate is
the pinned release evidence above.

- Pi RPC mode: https://pi.dev/docs/latest/rpc
- Pi models and provider compatibility: https://pi.dev/docs/latest/models
- Pi usage, sessions, context files, and system prompt files: https://pi.dev/docs/latest/usage
- Pi settings, session directory, skills, and resources: https://pi.dev/docs/latest/settings
- Pi providers: https://pi.dev/docs/latest/providers
