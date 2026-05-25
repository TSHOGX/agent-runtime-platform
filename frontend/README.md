# Frontend

Next.js workbench for the harness platform.

## Run

```bash
cd frontend
npm install
PORT=8000 npm run dev
```

Open <http://127.0.0.1:8000>.

## Rebuild / Restart

Development rebuilds are handled by Next.js hot reload, but you still need to restart the process when environment variables, dependencies, or route-handler wiring changes.

```bash
# stop the current dev server with Ctrl+C, then rerun
PORT=8000 npm run dev
```

For a production-style rebuild:

```bash
cd frontend
npm run build
PORT=8000 npm run start
```

If you need a clean rebuild after dependency changes, rerun `npm install` first, then `npm run build`.

## Checks

```bash
npm run lint
npm run typecheck
npm run build
```

## Backend Bridge

The frontend talks to the orchestrator through same-origin route handlers. This keeps browser requests on the frontend origin and avoids direct CORS/cookie coupling.

Implemented proxy routes:

- `GET /api/healthz`
- `/api/*` forwarded to orchestrator `/api/*`
- `/artifacts/:session_id/:path` forwarded to orchestrator artifact downloads

The proxy target defaults to `http://127.0.0.1:8090`.

Environment overrides:

- `HARNESS_API_BASE_URL` or `ORCHESTRATOR_URL` for the server-side proxy.
- `PORT=8000` for local frontend development.

## Current UI Flow

- The UI checks `/api/healthz` first.
- If the real backend is unavailable or a proxied HTTP request fails, the app shows the backend-unreachable state instead of a mock workspace.
- Live events come from the global `GET /api/events/stream` SSE endpoint through the frontend proxy, with `last_event_id` replay fallback when the stream reconnects.
- The stream renders `agent.delta`, `agent.message`, `agent.output`, `system.status`, and session lifecycle events.
- After a successful message post, the provider also polls session/messages/artifacts for a short period so the view can recover from missed events.
- The same-origin SSE path replaced the earlier direct browser WebSocket connection.
- The artifact pane renders a live metadata-backed file tree with search, folder expand/collapse, tabs, download actions, and previews for Markdown, code, text, images, JSON, CSV/TSV, and PDF.
- `artifact.updated` adds or refreshes files in the tree; `artifact.deleted` removes files or deleted directory prefixes and closes stale tabs.

## Current Agent Flow

- Create a session through `POST /api/sessions`.
- `Agent` maps to Claude Code; `Shell` maps to the PTY-backed shell session.
- Send a task prompt through `POST /api/sessions/:id/messages`.
- Interrupt a running shell session through `POST /api/sessions/:id/interrupt`.
- The orchestrator keeps per-generation resources alive across turns once the sandbox is running.
- Automatic idle checkpointing is a default-off Phase 7 policy; `HARNESS_AUTO_CHECKPOINT_ENABLED=true` enables it for newly created sessions in lab/test environments.
- `sh` remains useful for smoke tests and shell-style debugging.
