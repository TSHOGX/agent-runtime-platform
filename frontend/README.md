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

## Backend Defaults

The frontend will target the Phase 3 orchestrator at `http://127.0.0.1:8090` as the real backend. Later Phase 4 rounds will add the same-origin proxy, WebSocket client, and mock fallback.

Planned environment overrides:

- `HARNESS_API_BASE_URL` or `ORCHESTRATOR_URL` for the server-side proxy.
- `NEXT_PUBLIC_HARNESS_WS_URL` for direct browser WebSocket connections.
- `PORT=8000` for local frontend development.
