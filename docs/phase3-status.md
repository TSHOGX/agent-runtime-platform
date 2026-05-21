# Phase 3 Status

> Date: 2026-05-21
> Scope: Go orchestrator MVP for session API, sandbox restore boundary, event publication, artifact metadata, and lab auth.

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

## Current Note

The Phase 3 foundation is now running with the newer runtime path from commits `e8b84f0` and `9b803b6`:

- The runtime uses a per-container `OutputHub` instead of a single callback.
- Claude turns are written as stream-json user frames and complete on `result` / `error`.
- The orchestrator now drives `runsc` directly for fresh starts and restores.
- Browser event delivery is now SSE on the frontend origin, with WebSocket kept only for compatibility.
- The old "multi-turn routing is not implemented yet" note is no longer current.

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

- The MVP originally reused the Phase 2 shell script as the runtime boundary. That note is now historical; the active orchestrator path drives `runsc` directly.
- For deterministic curl smoke tests, `sh` is still useful, but Claude is the supported multi-turn path.
- The upgraded `runsc release-20260511.0` no longer reproduces the long-running service restore panic on this host, so the direct runtime path stays on restore instead of a cold fallback script.
- Multi-turn routing is now handled by the per-container `OutputHub` and stream parser completion logic.
