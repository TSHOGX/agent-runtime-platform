# Pi Driver

Pi enters Phase 9 as a registered driver after the driver/provider contract,
capability check, bridge runner abstraction, and output normalizer registry are
in place. It should not add a second Claude-shaped branch.

## Rootfs and Image Manifest

Rootfs build input:

```bash
SANDBOX_AGENT_DRIVERS=pi FORCE=1 sandbox-image/build-rootfs.sh
```

The image manifest at `/etc/harness-image/agents.json` must record:

- `driver_id: pi`
- pinned Pi CLI version
- pinned Pi event schema version, for example `pi_rpc_events_v1.0`
- binary path
- package digest
- installed config/resource paths

Allocation fails before runtime creation if `harness.default_agent` or any
enabled Pi profile is not present in that manifest.

## Runtime Command

Production Pi runs as a long-lived RPC process:

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

Production must not use `--no-session`. That flag is reserved for smoke tests
that prove JSONL framing without persistent state.

## Generated Config

Pi config is generated under the existing control projection:

```text
/harness-control/driver/pi/models.json
/harness-control/driver/pi/settings.json
```

These files are read-only from the sandbox. They may contain:

- model/provider map
- sandbox model proxy alias
- non-secret compatibility key if Pi requires an API key field
- driver settings selected by deployment config

They must not contain real upstream provider credentials. Pi must reach the
model through `harness.model_proxy.sandbox_base_url` during an active turn.

## Writable State

All Pi writable state must stay under:

```text
/agent-home/.pi/agent
```

No Pi cache, socket, session, package data, or settings write may target the
read-only rootfs or `/harness-control`.

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

Initial mapping:

| Pi event | Harness event |
| --- | --- |
| text delta | `agent.delta` |
| final assistant message | `agent.message` |
| tool execution event | `agent.output` initially |
| `turn_end` / `agent_end` | turn completion |
| RPC `abort` | interrupt |
| RPC `compact` | compaction |

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

## Release Gates

- Pinned Pi CLI version is recorded in the rootfs image manifest.
- Pinned Pi event schema version is recorded next to the CLI version.
- Pi CLI upgrades require paired event-schema review.
- `pi --mode rpc --no-session` smoke proves JSONL framing without model
  credentials.
- Production Pi stores state only under `/agent-home/.pi/agent`.
- Pi reaches models only through the sandbox proxy alias during active turns.
- No real provider credentials appear in env, argv, `/agent-home`,
  `/harness-control`, bridge queues, process listings, logs, or artifacts.
- Pi completes a turn with a deterministic completion signal.
- Pi interrupt/compaction are implemented through RPC `abort`/`compact` or
  explicitly marked `unsupported`.
- Pi session/home isolation is proven across two sessions.
- Restore/cold restart uses only `/agent-home` plus persisted driver state.

## References

- Pi RPC mode: https://pi.dev/docs/latest/rpc
- Pi models and provider compatibility: https://pi.dev/docs/latest/models
- Pi usage, sessions, context files, and system prompt files: https://pi.dev/docs/latest/usage
- Pi settings, session directory, skills, and resources: https://pi.dev/docs/latest/settings
- Pi providers: https://pi.dev/docs/latest/providers
