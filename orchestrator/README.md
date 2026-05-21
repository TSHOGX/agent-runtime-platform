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
- `HARNESS_SESSIONS_ROOT` defaults to `/var/lib/harness/sessions`.
- `HARNESS_CHECKPOINTS_ROOT` defaults to `/var/lib/harness/checkpoints`.
- `HARNESS_BUNDLE_ROOT` defaults to `<repo>/bundle/out`.
- `HARNESS_DEFAULT_AGENT` defaults to `demo`.
- `HARNESS_MAX_SESSIONS` defaults to `30`.
- `RUNSC_ROOT` defaults to `/var/lib/harness/runsc`.

The runtime currently launches `runsc` directly and keeps containers alive across turns. `bundle/restore-sandbox.sh` is still useful as a smoke-test boundary, but it is not the main request path anymore.

## Event Streams

- `GET /api/events` - WebSocket compatibility endpoint
- `GET /api/events/stream?session_id=<id>` - SSE endpoint used by the frontend

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

## Notes

- The current Claude path uses stream-json turns and a per-container output hub.
- `sh` is still available as a smoke agent.
- OpenCode is not launched by the entrypoint yet.
