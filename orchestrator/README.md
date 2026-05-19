# Orchestrator MVP

Phase 3 Go service for the local gVisor harness. It keeps session metadata in SQLite, restores per-session sandboxes through the Phase 2 runtime script, streams agent output over WebSocket, and records artifacts written under `/var/lib/harness/sessions/<session_id>/`.

## Run

```bash
cd orchestrator
go mod tidy
go run ./cmd/orchestrator
```

Useful environment variables:

- `HARNESS_ORCHESTRATOR_ADDR` defaults to `:8090`.
- `HARNESS_LAB_PASSWORD` enables shared-password auth. Leave empty for local no-auth smoke tests.
- `HARNESS_SESSIONS_ROOT` defaults to `/var/lib/harness/sessions`.
- `HARNESS_RESTORE_SCRIPT` defaults to `../bundle/restore-sandbox.sh`.
- `HARNESS_DEFAULT_AGENT` defaults to `demo`; use `claude` once the proxy credentials are ready.
- `HARNESS_MAX_SESSIONS` defaults to `30`.

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
  -d '{"agent":"sh"}'
```

Send the first message:

```bash
curl -b /tmp/harness.cookies \
  -X POST http://127.0.0.1:8090/api/sessions/<session_id>/messages \
  -H 'content-type: application/json' \
  -d '{"content":"echo phase3-ok > /workspace/ok.txt"}'
```

Open the event stream from a WebSocket client:

```bash
websocat ws://127.0.0.1:8090/api/events?session_id=<session_id>
```

List artifacts:

```bash
curl -b /tmp/harness.cookies \
  http://127.0.0.1:8090/api/sessions/<session_id>/artifacts
```

## MVP Notes

This version intentionally consumes the Phase 2 `restore-sandbox.sh` boundary instead of reimplementing checkpoint/restore in Go immediately. The first `POST /messages` starts the restored sandbox with `FIRST_MESSAGE`; additional in-sandbox multi-turn routing should replace the entrypoint contract in a later iteration.
