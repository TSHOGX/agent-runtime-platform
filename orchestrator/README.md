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
- `HARNESS_SESSION_TTL` defaults to `2h`.
- `HARNESS_REPO_ROOT` defaults to the repository root.
- `HARNESS_SESSIONS_ROOT` defaults to `/var/lib/harness/sessions`.
- `HARNESS_AGENT_HOMES_ROOT` defaults to `/var/lib/harness/agent-homes`.
- `HARNESS_CHECKPOINTS_ROOT` defaults to `/var/lib/harness/checkpoints`.
- `HARNESS_BUNDLE_ROOT` defaults to `<repo>/bundle/out`.
- `HARNESS_DB_PATH` defaults to `<sessions_root>/orchestrator.db`.
- `HARNESS_DEFAULT_AGENT` defaults to `claude`.
- `HARNESS_MAX_SESSIONS` defaults to `30`.
- `RUNSC_ROOT` defaults to `/var/lib/harness/runsc`.
- `HARNESS_RESTORE_SCRIPT` is loaded for compatibility, but the current direct `runsc` path does not execute it.

Runtime network and Claude proxy settings are explicit in `config/harness.yaml`:

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

The runtime currently launches `runsc` directly in sandbox mode and keeps containers alive across turns. It uses the fixed `/var/run/netns/phase1-demo` network namespace so the local Claude proxy stays reachable at `http://10.200.1.1:8082`. Automatic idle checkpointing is disabled because `runsc restore` cannot reliably reconnect the current stdin-based turn channel. `Shell` sessions use the PTY-backed shell shim and can be interrupted with `POST /api/sessions/<id>/interrupt`. `bundle/restore-sandbox.sh` is still useful as a smoke-test boundary, but it is not the main request path anymore.

## Event Streams

- `GET /api/events` - WebSocket compatibility endpoint
- `GET /api/events/stream?session_id=<id>` - SSE endpoint used by the frontend
- Artifact watcher events include `artifact.updated` for file create/write metadata and `artifact.deleted` for remove/rename cleanup.

## Session Control

- `POST /api/sessions/<id>/interrupt` - interrupt a running shell session

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
- Checkpoint/restore is present as experimental runtime plumbing, but automatic checkpointing should wait for the Phase 7 checkpoint-safe control plane.
