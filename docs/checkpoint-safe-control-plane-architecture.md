# Checkpoint-Safe Control Plane Architecture

> Date: 2026-05-22  
> Status: Phase 7 target architecture  
> Scope: make sessions addable, checkpointable, recoverable, reconnectable, network-correct, and multi-turn reliable.

## Executive Summary

The current system already proves the core product path: browser session, Go orchestrator, gVisor sandbox, Claude Code, stream parsing, artifacts, and same-origin SSE. The weak point is that a session is still too tightly coupled to a live container process and its attached stdin/stdout pipes.

The optimal architecture is to move session correctness into a durable host-side control plane, and treat each gVisor container as a replaceable runtime generation.

The target rule is:

```text
The session is the durable DB state, turn log, event log, network profile,
Claude conversation identity, and runtime generation lease.

The container is only the current executor for that session.
```

This makes `runsc checkpoint/restore` a performance and resource optimization, not the only mechanism that keeps a conversation correct. Claude resume, persisted messages, durable turn state, and explicit network profiles provide correctness. Checkpoint/restore provides fast restart when it works.

## Source Conclusions

This proposal follows the conclusions in the existing docs:

- [gvisor-decision.md](./gvisor-decision.md): Firecracker is not viable on this host because KVM is unavailable; gVisor `runsc` with `systrap` is the selected runtime.
- [runsc-warm-sentry-research.md](./runsc-warm-sentry-research.md): official `runsc release-20260511.0` does not include warm sentry. Low-latency startup should be achieved with orchestrator-level pooling or normal checkpoint/restore tuning, not an unavailable `--warm-sentry` feature.
- [architecture.md](./architecture.md): the current system already has canonical session states, `runsc` direct control, per-container `OutputHub`, stream-json parsing, artifact watching, and SSE.
- [current-status.md](./current-status.md): the current sandbox network path is `runsc -network sandbox -overlay2 none`, with the local LLM proxy reachable from the sandbox through the fixed gateway path.
- [PLAN.md](./PLAN.md): Phase 7 is now dedicated to checkpoint-safe control plane work; multi-user hardening and later additional harness adapters have moved to Phase 8.

There is one important update to the older checkpoint descriptions: automatic idle checkpointing is currently not safe as the primary path because `runsc restore` can restore the container while the attached stdin turn channel is no longer reliably reconnectable. That is the architecture gap this document addresses.

## Current Codebase Alignment

As of the 2026-05-22 docs pass, the current implementation has these important properties:

```text
active sessions:
  long-lived runsc containers across live turns

turn transport:
  host writes Claude stream-json frames or shell turn frames to stdin/PTY

checkpoint:
  primitives exist, but automatic idle checkpointing is disabled

restore:
  only used when Runtime.RestoreFromCheckpoint is explicitly enabled

startup reconciliation:
  stale checkpointing/checkpointed rows are recovered so the UI/API remain usable

network config:
  config/harness.yaml is the explicit lab source of truth
```

The current `config/harness.yaml` profile is:

```yaml
runtime:
  runsc_network: sandbox
  runsc_overlay2: none

claude:
  proxy_bind_url: http://0.0.0.0:8082
  sandbox_base_url: http://10.200.1.1:8082
  api_key: "123"
  auth_token: "123"
  model: sonnet
  output_format: stream-json
  disable_nonessential_traffic: true
```

## Current Architecture

```text
Browser
  |
  | HTTP + SSE
  v
Next.js frontend
  |
  | same-origin API proxy
  v
Go orchestrator
  |
  | session table
  | message table
  | artifact watcher
  | global event hub
  v
Runtime
  |
  | active containers[session_id]
  | per-container OutputHub
  | stdin JSONL / PTY writes
  v
runsc container
  |
  | harness entrypoint
  | Claude Code or shell shim
  | stdout/stderr
  v
Runtime stream parser
```

### Current Turn Flow

```text
POST /api/sessions/<id>/messages
  -> persist user message
  -> mark session running_active
  -> Runtime.Start()
       hot path:
         find active container
         write JSONL turn to container stdin
       opt-in restore path:
         runsc restore
         create new host pipes
         write first JSONL turn
       cold path:
         runsc run
         create stdin/stdout/stderr pipes
         write first JSONL turn
  -> parse stdout until result/error/turn_done
  -> persist assistant message
  -> mark session running_idle
```

For Claude Code, the turn frame is logically:

```json
{"type":"user","message":{"role":"user","content":[{"type":"text","text":"..."}]}}
```

For shell sessions, the shell shim receives a `{"type":"turn","content":"..."}` frame and emits `harness.turn_done` when the command completes.

### Current Problem

The current lower-level turn transport is an attached stdin or PTY pipe owned by the host `runsc run` or `runsc restore` process.

That pipe is not a checkpoint-safe control plane.

`runsc checkpoint` can persist the sandbox's runtime state, but it does not turn the host-side stdin pipe into a durable, replayable, reconnectable session protocol. After restore, the process inside the sandbox may observe stdin EOF or the orchestrator may have no reliable way to bind the restored process back to the exact pending turn semantics.

This creates several practical failure modes:

- A session can be marked `checkpointing` while the actual checkpoint path stalls or the restored entrypoint exits.
- A restored container can exist but not accept the next turn.
- An old session can look `running_idle` in the DB while no healthy executor is attached.
- The frontend can reconnect to SSE, but missed runtime output is not durably replayable.
- Network correctness is currently mostly verified at startup time by behavior, not enforced as a first-class persisted contract per runtime generation.

## Target Architecture

```text
Browser
  |
  | HTTP + SSE with last_event_id
  v
Next.js frontend
  |
  | same-origin API proxy
  v
Control Plane
  |
  | session store
  | turn ledger
  | durable event log
  | runtime generation table
  | network profile table
  | runtime lease manager
  | artifact metadata
  v
Runtime Manager
  |
  | runsc driver
  | checkpoint manager
  | restore/cold-start fallback
  | sandbox pool, optional
  | network probe
  v
runsc generation N
  |
  | Agent Bridge
  |   - reconnectable control client
  |   - per-turn ack/completion
  |   - stdout/stderr/event forwarding
  |
  | Claude Code / shell shim
  | workspace mount
  | agent home mount
  v
Durable event log
```

The main difference is the `Agent Bridge`. Instead of treating container stdin as the session's source of truth, the sandbox starts a small bridge process that talks to the host control plane using a reconnectable protocol. The bridge can be implemented over a Unix socket bind mount, a local HTTP long-poll endpoint reachable through the host gateway, or a simple file-backed queue with atomic claim/ack semantics. The exact transport can vary, but the protocol semantics must be durable.

## Control Plane Responsibilities

### Session Store

Stores the durable identity of the conversation:

```text
session_id
user_id
agent
status
claude_session_uuid
workspace_path
agent_home_path
created_at
updated_at
last_activity_at
expires_at
```

The session row must not imply that a specific process is alive. It only says whether the session is eligible to accept input and what recovery policy applies.

### Turn Ledger

Stores every user turn before execution:

```text
turn_id
session_id
sequence
role = user
content
status = queued | leased | running | completed | failed | canceled
runtime_generation
created_at
started_at
completed_at
error
```

The rule is:

```text
No user message is sent to a sandbox until it has a durable turn row.
No turn is considered complete until a durable completion event is recorded.
```

This makes retry and recovery deterministic.

### Durable Event Log

Stores runtime and agent events with monotonic event IDs:

```text
event_id
session_id
turn_id
runtime_generation
type
payload
created_at
```

SSE becomes a view over this log. The frontend can reconnect with `last_event_id`, and the server can replay missed events. The existing polling fallback remains useful, but it is no longer the only recovery path for missed live output.

### Runtime Generation Table

Tracks each executor instance:

```text
generation_id
session_id
runsc_container_id
status = starting | probing | active | idle | checkpointing | checkpointed | restoring | failed | destroyed
checkpoint_path
network_profile_id
started_at
last_seen_at
ended_at
failure_reason
```

The generation ID is essential. It prevents an old restored container from writing events into a newer session execution. Every event and turn ack must carry the generation ID.

### Network Profile

Network config must be explicit and persisted, not inferred from ambient host settings:

```text
network_profile_id
runsc_network = sandbox
runsc_overlay2 = none
host_proxy_bind_url = http://0.0.0.0:8082
sandbox_base_url = http://10.200.1.1:8082
manifest_anthropic_base_url = http://10.200.1.1:8082
anthropic_api_key = 123
anthropic_auth_token = 123
disable_nonessential_traffic = true
model = sonnet
output_format = stream-json
probe_url = http://10.200.1.1:8082
```

For this lab host, the expected explicit values are:

```text
Host-visible proxy:     http://0.0.0.0:8082
Sandbox-visible proxy:  http://10.200.1.1:8082
Client API key:         123
runsc network:          sandbox
runsc overlay2:         none
```

The current code already writes these values into the session control manifest. Phase 7 should also persist them into a network profile / runtime generation record. They should not be passed through implicit environment variables as the only source of truth.

## New Lifecycle Model

### Session Lifecycle

```text
created
  -> accepting_input
  -> turn_running
  -> accepting_input
  -> destroyed

Any state can become failed if the session itself is not recoverable.
```

The current public statuses can remain compatible for the UI:

```text
created
running_active
running_idle
checkpointing
checkpointed
failed
destroyed
```

But internally, the runtime generation status should be separated from the session status.

### Runtime Generation Lifecycle

```text
none
  -> starting
  -> probing
  -> active
  -> idle
  -> checkpointing
  -> checkpointed
  -> restoring
  -> probing
  -> active

Failure fallback:

starting/probing/restoring/checkpointing
  -> failed
  -> cold_start_new_generation
```

The important rule is:

```text
Only checkpoint an idle generation with no running turn and no unacked output.
```

### Container Lifecycle

#### Current

```text
create session
  -> first message starts runsc container
  -> container stays alive between turns
  -> idle monitor reconciles stale checkpoint states
  -> automatic checkpointing is disabled
```

#### Target

```text
create session
  -> no container required yet
first queued turn
  -> acquire or create runtime generation
  -> network probe
  -> bridge claims turn
  -> Claude/shell executes turn
  -> durable completion
  -> generation becomes idle
idle policy
  -> either keep alive
  -> or checkpoint then destroy process resources
next queued turn
  -> restore checkpoint if valid
  -> otherwise cold start new generation
  -> Claude resume provides logical continuity
```

The session survives every container transition.

## Claude Resume Strategy

There are two independent resume layers:

```text
Logical resume:
  ClaudeSessionUUID + Claude home + persisted transcript + workspace

Physical resume:
  runsc checkpoint image + runtime generation restore
```

The target architecture treats logical resume as required for correctness and physical resume as an optimization.

### Normal Hot Path

```text
turn queued
  -> active generation exists
  -> bridge claims turn
  -> Claude Code receives stream-json input
  -> parser records result
  -> turn completed
```

### Restore Path

```text
turn queued
  -> session has checkpointed generation
  -> runsc restore generation N
  -> bridge reconnects and announces generation N
  -> network probe passes
  -> bridge claims queued turn
  -> Claude continues
```

### Cold Fallback Path

```text
turn queued
  -> restore fails or bridge fails to reconnect
  -> mark generation N failed
  -> start generation N+1 from bundle
  -> write same ClaudeSessionUUID / resume flag into manifest
  -> network probe passes
  -> Claude resumes logical conversation
  -> bridge claims queued turn
```

This fallback is what makes the system reliable. The user should not be blocked on perfect `runsc restore` behavior.

## Multi-Turn Protocol

The bridge should expose a simple per-turn protocol:

```text
1. hello(session_id, generation_id, agent, protocol_version)
2. probe_network()
3. claim_next_turn(session_id, generation_id)
4. ack_turn_started(turn_id)
5. emit_output(turn_id, event_id, payload)
6. ack_turn_completed(turn_id, result)
7. heartbeat(session_id, generation_id)
```

The host side owns ordering. The bridge never invents a turn sequence.

### Idempotency

Every message from the bridge must include:

```text
session_id
turn_id
generation_id
sequence or event_id
```

The host drops events from stale generations. If a bridge retries the same output event, the host deduplicates by event ID or by `(turn_id, generation_id, sequence)`.

### Turn Completion

Claude completion remains based on stream-json:

```text
result success
result non-success
error
```

Shell completion remains based on:

```text
harness.turn_done
```

The difference is that completion is now written to the durable turn ledger before the session is marked idle.

## Checkpoint Policy

Checkpoint should be allowed only when all of these are true:

```text
session status is running_idle / accepting_input
generation status is idle
no turn is leased/running for the session
bridge heartbeat is healthy
bridge is checkpoint-ready, with no active host control request that must survive restore
all output events for the previous turn are durably flushed
network profile is known
checkpoint timeout budget is available
```

Checkpoint should produce:

```text
checkpoint_path
checkpoint_created_at
checkpoint_runtime_generation
checkpoint_network_profile_id
checkpoint_runsc_version
checkpoint_bundle_digest
```

Restore should validate:

```text
checkpoint exists and has required image files
bundle digest is compatible
runsc version is compatible enough for this deployment
network profile is still valid
bridge reconnects within timeout
network probe passes after restore
```

If any restore validation fails, the system should start a new generation and use Claude logical resume.

## Network Model

The target network path is:

```text
Claude Code inside sandbox
  -> http://10.200.1.1:8082/v1/messages?beta=true
  -> host namespace listener at http://0.0.0.0:8082
  -> claude-code-proxy
  -> upstream model provider
```

The orchestrator should enforce this in three places:

1. Persist the network profile.
2. Write the profile explicitly into the control manifest for each runtime generation.
3. Probe from the sandbox before accepting the first turn for that generation.

The probe should distinguish:

```text
network path reachable but endpoint method rejected:
  HEAD / returns 405, acceptable as reachability proof

auth wrong:
  POST /v1/messages returns 401, not acceptable

connection failure:
  refused / timeout / failed socket, not acceptable
```

The current proxy key for this host is explicitly `123`; a generation using any other key is misconfigured.

## Architecture Comparison

```text
Current
-------
Session correctness depends on live runsc process + stdin pipe.

Browser
  -> Orchestrator
    -> containers[session_id]
      -> stdin JSONL
      -> Claude Code
      -> stdout parser

Checkpoint captures container memory, but not a durable host-side turn protocol.


Target
------
Session correctness depends on durable control plane.

Browser
  -> Orchestrator Control Plane
    -> turn ledger
    -> event log
    -> runtime generation lease
    -> network profile
    -> Runtime Manager
      -> runsc generation
        -> Agent Bridge
          -> Claude Code

Checkpoint captures an idle executor generation.
Restore reconnects the bridge.
Cold start fallback preserves conversation through Claude resume.
```

## Refactor Size

This proposal has two practical implementation levels.

### Medium Refactor

This level improves reliability quickly but does not fully solve checkpoint-safe stdin.

Scope:

- Detect `running_idle` sessions with no active container and start a fresh generation on the next turn.
- Keep `ClaudeSessionUUID` and `ResumeClaude` as the logical resume path.
- Promote the current `config/harness.yaml` profile into persisted network profile records and keep writing the values into every control manifest.
- Add startup network probe before sending a turn.
- Add runtime generation IDs to active containers.
- Reconcile stale `running_active`, `checkpointing`, and `checkpointed` states on startup.
- Keep checkpoint disabled or opt-in until restore has a reconnectable bridge.

This can make sessions addable, recoverable, reconnectable, network-correct, and multi-turn reliable enough for the lab path. It does not make physical gVisor restore the source of truth.

### Full Architecture Refactor

This is the optimal architecture described in this document.

Scope:

- Add turn ledger.
- Add durable event log and SSE replay.
- Add runtime generation table and leases.
- Add agent bridge inside the sandbox.
- Replace direct host stdin turn writes with claim/ack semantics.
- Make checkpoint depend on idle turn ledger state.
- Restore checkpointed generations by reconnecting the bridge.
- Add cold fallback when restore or bridge reconnect fails.
- Add stale generation fencing.
- Add integration tests for process restart, frontend reconnect, checkpoint/restore, cold fallback, and network failure.

This touches the orchestrator, runtime, store schema, bundle entrypoint, Claude adapter, shell adapter, frontend event semantics, and tests. It is a full architecture refactor.

## Recommended Migration Plan

### Step 1: Document and Freeze Current Invariants

Define invariants before changing code:

```text
one running turn per session
every user message persists before execution
every assistant message persists before running_idle
network config is explicit
old generations cannot write into new sessions
restore failure must not make a session unusable
```

### Step 2: Introduce Runtime Generation IDs

Add a runtime generation concept without changing transport yet.

```text
sessions
runtime_generations
```

Attach every active container to a generation ID. Include the generation ID in runtime logs and events.

### Step 3: Persist Explicit Network Profiles and Probes

The current code already has explicit lab values in `config/harness.yaml`. Phase 7 should persist the selected profile, attach it to each runtime generation, and write the same values into `session.json`.

Before sending the first turn:

```text
start container
run sandbox-local probe
only then send user turn
```

This prevents a session from appearing active when it cannot reach `claude-code-proxy`.

### Step 4: Add Cold Resume Fallback

When a session says `running_idle` but no live container exists:

```text
start a new generation
set ClaudeResume = true
reuse ClaudeSessionUUID
send the queued turn
```

This makes old sessions usable even when the old physical container is gone.

### Step 5: Add Durable Turn Ledger

Move from "message implies execution" to explicit turn state:

```text
queued -> running -> completed
```

At this point, recovery after orchestrator restart becomes deterministic.

### Step 6: Add Durable Event Log and SSE Replay

Record output events before publishing them to live subscribers.

Frontend reconnects should use a last seen event ID.

### Step 7: Add Agent Bridge

Move turn delivery out of direct host stdin writes and into a reconnectable protocol.

Initially, the bridge can still write to Claude Code stdin inside the sandbox. The important change is that the host talks to the bridge with durable claim/ack semantics.

### Step 8: Re-enable Checkpoint

Only after the bridge exists:

```text
idle generation
  -> checkpoint
  -> mark generation checkpointed
  -> release process resources

next turn
  -> runsc restore
  -> bridge reconnects
  -> network probe
  -> claim queued turn
```

If any step fails, cold start a new generation and use Claude resume.

## Test Matrix

### Session Creation

- Create Claude session.
- Create shell session.
- Reject unsupported agent.
- Create session without starting a container.

### Network

- New generation with correct profile reaches `http://10.200.1.1:8082`.
- Wrong key fails probe and does not accept user turn.
- Proxy down fails probe and keeps turn queued or marks generation failed.
- Host URL `http://0.0.0.0:8082` never gets written as the sandbox base URL.

### Multi-Turn

- Active container handles multiple Claude turns.
- Active container handles multiple shell turns.
- Slow SSE subscriber does not block durable event recording.
- Frontend reconnect replays missed events.

### Orchestrator Restart

- Restart during idle session.
- Restart during queued turn.
- Restart during running turn.
- Restart during checkpointing.
- Restart after checkpointed.

### Checkpoint/Restore

- Checkpoint only occurs when no turn is running.
- Restore reconnects bridge and accepts next turn.
- Restore failure falls back to cold start.
- Stale restored generation cannot write events after a newer generation exists.

### Claude Resume

- Cold fallback preserves conversation using the same `ClaudeSessionUUID`.
- Failed old session can start a new generation and continue.
- Agent home is mounted consistently across generations.

### Resource Cleanup

- Destroyed session kills active generation.
- Failed generation is deleted without deleting a newer generation for the same session.
- Expired sessions are eventually destroyed or made inactive.

## Open Decisions

### Bridge Transport

Options:

```text
Unix socket bind mount:
  strong local semantics and no model-network dependency, but any live connection must be reopened after restore

HTTP long-poll through sandbox gateway:
  simple to debug, but turn control depends on the sandbox network path and must quiesce before checkpoint

File-backed queue:
  most checkpoint-friendly because there is no live host socket to preserve, but higher latency and more filesystem edge cases
```

Recommendation: start Phase 7 with a file-backed queue or another transport that can quiesce cleanly before checkpoint. The protocol should stay transport-neutral, but the first implementation should favor correctness and checkpoint safety over latency. A Unix socket or HTTP long-poll transport can be introduced later if it keeps the same claim/ack semantics and proves clean reconnect behavior after restore.

### Event Log Storage

Options:

```text
SQLite events table:
  easiest current fit

append-only log files per session:
  good streaming characteristics, more custom tooling

embedded queue:
  more moving parts than needed for this lab phase
```

Recommendation: SQLite first. The current store already uses SQLite and event volume is manageable.

### Checkpoint Scope

Options:

```text
checkpoint every idle session after threshold
checkpoint only large/expensive sessions
checkpoint only when memory pressure requires it
checkpoint manually for test sessions
```

Recommendation: re-enable checkpoint behind a feature flag only after the bridge exists. Start with manual/test sessions, then idle threshold, then memory-pressure policy.

## Final Recommendation

The best long-term architecture is the full control-plane refactor:

```text
durable session + durable turn ledger + durable event log
  + runtime generation fencing
  + explicit network profile
  + reconnectable agent bridge
  + checkpoint/restore as idle executor optimization
  + cold start fallback through Claude resume
```

The safest delivery path is staged:

1. Medium refactor first: generation IDs, explicit network profile, network probe, cold resume fallback.
2. Then full refactor: turn ledger, event log replay, bridge, checkpoint-safe restore.
3. Re-enable automatic checkpoint only after restore can reconnect through the bridge and stale generations are fenced.

This path preserves current working behavior while moving the system toward the properties required for sustained architecture optimization:

```text
new sessions can be added
old sessions can recover
containers can checkpoint
containers can restore
clients can reconnect
networks are explicit and verified
multi-turn conversation remains correct
```
