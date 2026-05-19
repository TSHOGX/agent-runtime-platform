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
- `NEXT_PUBLIC_HARNESS_WS_URL` for direct browser WebSocket connections.
- `PORT=8000` for local frontend development.

The UI checks `/api/healthz` first. If the real backend is unavailable or a proxied HTTP request fails, the dashboard switches to a clearly labeled mock fallback and provides a `Retry real` action.
