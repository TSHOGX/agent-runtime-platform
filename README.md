# Agent Runtime Platform

Control plane for long-lived, sandboxed AI agent sessions.

This project runs each AI agent session inside an isolated gVisor sandbox and
uses a Go orchestrator to manage sessions, turns, runtime generations, event
streams, proxy correlation, and artifacts. The Next.js workbench provides the
browser UI for session control, live output, and artifact browsing.

## Positioning

Agent Runtime Platform is split into three main surfaces:

- **Control plane**: session and turn lifecycle, runtime generations, Agent
  Bridge, durable events, artifact metadata, proxy correlation, quota, and
  retention.
- **Sandbox runtime**: gVisor `runsc`, rootfs, mounts, network namespaces,
  agent processes, and checkpoint/restore mechanics.
- **Workbench**: a Next.js browser UI for sessions, live output, and read-only
  artifact inspection.

## Stack

- **Backend**: Go orchestrator, SQLite, HTTP API, SSE event endpoint.
- **Frontend**: Next.js, TypeScript, same-origin API proxy, live artifact
  browser.
- **Runtime**: gVisor `runsc`, OCI bundle/rootfs, per-generation sandbox
  resources.
- **Agent paths**: Pi, Claude Code, and PTY-backed shell; deployment config
  selects the product `Agent` default.
- **Control protocol**: Agent Bridge file-queue claim/ack protocol.
- **Artifacts**: host-side metadata watcher with safe read-only downloads and
  previews.

## Quick Start

In the checked-in lab config, Workbench `Agent` resolves to Pi and `Shell` is
also enabled. Before starting the app, make sure the active rootfs
`/etc/harness-image/agents.json` covers both product modes:

```bash
SANDBOX_AGENT_DRIVERS=pi,sh ./sandbox-image/build-rootfs.sh
```

If an existing rootfs is missing those CLIs, use the same driver set with
`FORCE=1` for a full rebuild. Without `FORCE`, the script syncs the overlay and
regenerates the manifest.

Start the orchestrator:

```bash
cd orchestrator
go run ./cmd/orchestrator
```

Start the frontend:

```bash
cd frontend
npm install
PORT=8000 npm run dev
```

Default local endpoints:

- Frontend: <http://127.0.0.1:8000>
- Orchestrator: <http://127.0.0.1:8090>

Health checks:

```bash
curl -sS http://127.0.0.1:8090/healthz
curl -sS http://127.0.0.1:8000/api/healthz
curl -sS http://127.0.0.1:8090/api/deployment-capabilities
```

## Repository Layout

```text
agent-runtime-platform/
├── config/             # runtime, proxy, and lab config
├── docs/               # current architecture, plan, and next-stage target
├── orchestrator/       # Go control plane
├── frontend/           # Next.js workbench
├── sandbox-image/      # rootfs build scripts, OCI template, sandbox entrypoint
├── bundle/             # runsc bundle/checkpoint helper scripts
└── schema-pack/        # sandbox-mountable schema/documentation pack
```
