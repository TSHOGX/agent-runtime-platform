# Phase 3 Status

> Date: 2026-05-20
> Scope: Go orchestrator MVP for session API, sandbox restore boundary, WebSocket events, artifact metadata, and lab auth.

## Completed

- Added `orchestrator/` Go module.
  - HTTP API under `/api`.
  - WebSocket event stream at `/api/events`.
  - Shared-password login via `HARNESS_LAB_PASSWORD`.
- Added SQLite metadata store.
  - `users`, `sessions`, `messages`, and `artifacts` tables.
  - Session lifecycle states: `created`, `running`, `completed`, `failed`, `destroyed`.
- Added runtime adapter around `bundle/restore-sandbox.sh`.
  - Creates one restored sandbox on the first session message.
  - Emits session lifecycle events around restore execution.
  - Calls `runsc kill` / `runsc delete` for destroy.
- Added artifact watcher for `/var/lib/harness/sessions`.
  - Records file create/write events.
  - Scans the session workspace after sandbox completion.
  - Publishes `artifact.updated` events.

## API Gate

Target smoke flow:

```bash
cd orchestrator
go mod tidy
go run ./cmd/orchestrator

curl -X POST http://127.0.0.1:8090/api/sessions \
  -H 'content-type: application/json' \
  -d '{"agent":"sh"}'

curl -X POST http://127.0.0.1:8090/api/sessions/<session_id>/messages \
  -H 'content-type: application/json' \
  -d '{"content":"echo phase3-ok > /workspace/ok.txt"}'

curl http://127.0.0.1:8090/api/sessions/<session_id>/artifacts
```

If `HARNESS_LAB_PASSWORD` is set, first call `/api/login` and pass the returned cookie.

## Verified

- Installed Go 1.22 on the host.
- `GOPROXY=https://goproxy.cn,direct go mod tidy`
- `gofmt -w ./cmd ./internal`
- `go test ./...`

## Important Notes

- The MVP reuses the Phase 2 shell script as the runtime boundary. This keeps the checkpoint/restore path stable while the API and event contract are introduced.
- For deterministic curl smoke tests, use `agent:"sh"` and put the shell command in the first message; the runtime passes it through `HARNESS_COMMAND`.
- The upgraded `runsc release-20260511.0` no longer reproduces the long-running service restore panic on this host, so the MVP now uses restore directly and keeps the cold fallback out of the path.
- Multi-turn stdio routing is not fully implemented yet because the current sandbox entrypoint consumes `FIRST_MESSAGE` and runs the selected agent as a one-shot process. The first Phase 3 gate is still covered: create session, send one message, stream output, and inspect artifacts.
