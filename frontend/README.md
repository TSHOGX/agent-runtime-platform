# Harness Frontend

Phase 4 Next.js workbench for the harness platform.

## Run

```bash
cd frontend
npm install
PORT=8000 npm run dev
```

Open <http://127.0.0.1:8000>.

## Checks

```bash
npm run lint
npm run typecheck
npm run build
```

## Backend Bridge

The frontend calls the Phase 3 orchestrator through same-origin route handlers. This keeps browser requests on the frontend origin and avoids direct CORS/cookie coupling.

Implemented proxy routes:

- `GET /api/healthz`
- `/api/*` forwarded to orchestrator `/api/*`
- `/artifacts/:session_id/:path` forwarded to orchestrator artifact downloads

The proxy target defaults to `http://127.0.0.1:8090`.

Environment overrides:

- `HARNESS_API_BASE_URL` or `ORCHESTRATOR_URL` for the server-side proxy.
- `PORT=8000` for local frontend development.

The UI checks `/api/healthz` first. If the real backend is unavailable or a proxied HTTP request fails, the dashboard switches to a clearly labeled mock fallback and provides a `Retry real` action.

## Current MVP Flow

- Pick an agent and create a session through `POST /api/sessions`.
- Send one task prompt through `POST /api/sessions/:id/messages`.
- The Phase 3 backend currently accepts only the first message for a sandbox, so the UI treats each session as a one-shot run.
- Event streaming is connected to `GET /api/events/stream?session_id=<id>` through the frontend proxy.
- The stream renders `agent.output` lines as separate thinking, tool-call, answer, and runtime entries.
- If the WS endpoint fails, the UI falls back to cached mock stream data and keeps the backend mode badge visible.
