# Orchestrator

Go service for the local gVisor harness. It owns session metadata, starts or restores per-session sandboxes, parses agent output, publishes events, and records artifacts written under `/var/lib/harness/sessions/<session_id>/`.

## Run

```bash
cd orchestrator
go run ./cmd/orchestrator
```

For a built binary:

```bash
cd orchestrator
mkdir -p bin
go build -o bin/orchestrator ./cmd/orchestrator
./bin/orchestrator
```

## Rebuild / Restart

Use `go run` while iterating locally. When code changes require a full rebuild, rebuild the binary and restart the process:

```bash
cd orchestrator
go build -o bin/orchestrator ./cmd/orchestrator
```

Then stop the current process and start the new binary again:

```bash
./bin/orchestrator
```

If you started it in another terminal, kill that process first and relaunch the rebuilt binary.

Useful environment variables:

- `HARNESS_ORCHESTRATOR_ADDR` defaults to `:8090`.
- `HARNESS_LAB_PASSWORD` enables shared-password auth. Leave empty for local no-auth smoke tests.
- `HARNESS_COOKIE_NAME` defaults to `harness_auth`.
- `HARNESS_SESSION_RETENTION` defaults to `0s`; `0s` disables automatic session expiry.
- `HARNESS_REPO_ROOT` defaults to the repository root.
- `HARNESS_SESSIONS_ROOT` defaults to `/var/lib/harness/sessions`.
- `HARNESS_AGENT_HOMES_ROOT` defaults to `/var/lib/harness/agent-homes`.
- `HARNESS_CHECKPOINTS_ROOT` defaults to `/var/lib/harness/checkpoints`.
- `HARNESS_BUNDLE_ROOT` defaults to `<repo>/bundle/out`.
- `HARNESS_DB_PATH` defaults to `/var/lib/harness/state/orchestrator.db`.
- `HARNESS_DEFAULT_AGENT` defaults to `claude`.
- `HARNESS_MAX_SESSIONS` defaults to `30` and caps non-terminal sessions, not live `/30` slots.
- `RUNSC_ROOT` defaults to `/var/lib/harness/runsc`.
- `HARNESS_RESTORE_SCRIPT` is loaded for compatibility, but the current direct `runsc` path does not execute it.

`HARNESS_SESSION_TTL` is obsolete and fails startup if present; use `HARNESS_SESSION_RETENTION`.

Runtime network and Claude proxy settings are explicit in `config/harness.yaml`:

```yaml
harness:
  run_dir: /var/lib/harness/run
  session_retention: 0s
  max_sessions: 30
  network:
    cidr_pool: 10.200.0.0/16
    egress:
      doris_fe_hosts: [172.16.0.138]
      doris_be_hosts: [172.16.0.138]
      doris_ports: [9030]
      dns_policy: hostnames_only
  events:
    retention_window: 24h
    retention_rows: 1000000
    emit_output_batch_max_rows: 64
    emit_output_batch_max_age: 100ms
  probe:
    accept_status:
      get_healthz: [200]
      post_v1_messages:
        unauthorized: [401]
        malformed_authenticated: [400]
    pre_start_attempts: 3
    pre_start_interval: 500ms
    post_start_attempts: 5
    post_start_interval: 1s
  bridge:
    lease_ttl: 60s
    heartbeat_interval: 30s
    poll_interval: 5ms
    ack_started_grace: 90s
    reconnect_grace: 30s
  checkpoint:
    auto_enabled: false
    idle_threshold: 30m
    monitor_interval: 5m
  reaper:
    failed_retention: 10m
    checkpoint_image_retention: 720h
```

The loader uses strict YAML decoding for the Phase 7 `harness:` schema. Legacy files containing only top-level `runtime:` / `claude:` sections still load for compatibility, but mixing them with `harness:` is rejected. Legacy `harness.secrets.*` keys are rejected; provider credentials stay host-side.

With `session_retention: 0s`, retained non-terminal sessions keep counting toward `max_sessions` even after runtime resources are retired. The `/api/quota` response reports the session ceiling and live `/30` pool ceiling separately. Use `DELETE /api/sessions/<id>` to close sessions and free session quota; close preserves messages, artifacts, workspace, and agent-home paths while reclaiming runtime resources.

The runtime currently launches `runsc` directly in sandbox mode and keeps containers alive across turns. Each generation gets its own network profile, netns/veth pair, `/30`, generated runtime spec, control manifest, and file-queue bridge. Automatic idle checkpointing is a per-session policy: the checked-in default is off, `HARNESS_AUTO_CHECKPOINT_ENABLED=true` enables the policy for newly created sessions, and only the next idle generation with an empty turn queue plus fresh bridge heartbeat/checkpoint-ready markers can checkpoint. `Shell` sessions use the PTY-backed shell shim and can be interrupted with `POST /api/sessions/<id>/interrupt`. The legacy `bundle/bake-bundle.sh` and `bundle/restore-sandbox.sh` smoke tools fail closed and are not Phase 8 release evidence.

## Event Streams

- `GET /api/events` - WebSocket compatibility endpoint
- `GET /api/events/stream?session_id=<id>` - SSE endpoint used by the frontend
- Artifact watcher events include `artifact.updated` for file create/write metadata and `artifact.deleted` for remove/rename cleanup.

## Session Control

- `POST /api/sessions/<id>/interrupt` - interrupt a running shell session
- `DELETE /api/sessions/<id>` - close a session, preserve history/workspace state, and reclaim runtime resources

## Curl Smoke Test

When `HARNESS_LAB_PASSWORD` is set:

```bash
curl -c /tmp/harness.cookies \
  -X POST http://127.0.0.1:8090/api/login \
  -H 'content-type: application/json' \
  -d '{"password":"YOUR_PASSWORD"}'
```

Create a session:

```bash
curl -b /tmp/harness.cookies \
  -X POST http://127.0.0.1:8090/api/sessions \
  -H 'content-type: application/json' \
  -d '{"agent":"claude"}'
```

Send the first message:

```bash
curl -b /tmp/harness.cookies \
  -X POST http://127.0.0.1:8090/api/sessions/<session_id>/messages \
  -H 'content-type: application/json' \
  -d '{"content":"what tables are available?"}'
```

Open the event stream from a browser-friendly client:

```bash
curl --no-buffer \
  http://127.0.0.1:8090/api/events/stream?session_id=<session_id>
```

List artifacts:

```bash
curl -b /tmp/harness.cookies \
  http://127.0.0.1:8090/api/sessions/<session_id>/artifacts
```

Download artifacts:

```bash
curl -b /tmp/harness.cookies \
  http://127.0.0.1:8090/artifacts/<session_id>/<path>
```

Artifact downloads are read-only and limited to regular files under the session workspace. The server rejects traversal, symlink components, symlink escape, directories, and non-regular files.

## Notes

- The current Claude path uses stream-json turns and a per-container output hub.
- `sh` is the interactive shell session path; it is still useful for smoke tests and shell-style debugging.
- Checkpoint/restore is generation-aware. Automatic checkpointing remains default-off while Phase 7b validates the policy, and should be enabled only for the lab profile or explicit test environments.
