# Release Qualification

This file lists the Phase 7 gates that are intentionally outside the default unit-test suite. A release candidate is blocked if any required gate is skipped, fails, or cannot produce evidence tied to the candidate commit.

## Deterministic Repo Gates

Run from `orchestrator/`:

```bash
go test -count=1 ./internal/config ./internal/store ./internal/runtime ./internal/bridge ./internal/server ./internal/events
go test -tags phase7bench -count=1 ./internal/server -run TestPhase7TurnStartLatencyGate
```

Run from the repository root:

```bash
python3 -m unittest sandbox-image/tests/test_harness_bridge_client.py
```

The `phase7bench` gate measures the in-repo control-plane path from HTTP enqueue to committed `ack_turn_started` with a connected and probed bridge. It is not a replacement for the live lab load measurement below.

## Pinned Proxy Contract

Run against the pinned `claude-code-proxy` checkout used by the lab:

```bash
cd /root/claude-code-proxy
.venv/bin/python -m pytest -q tests/test_harness_probe_contract.py
```

Required behavior:

- `GET /healthz` returns `200`.
- `POST /v1/messages` without a key returns `401`.
- `POST /v1/messages` with a wrong key returns `401`.
- `POST /v1/messages` with the configured key and malformed JSON returns `400`.

Any proxy behavior drift blocks the release until the proxy is re-pinned or the Phase 7 probe contract is deliberately changed.

## gVisor Bridge Durability Lab

This gate must run on the target lab host with the pinned `runsc` build. It verifies the bridge mount's `file-access=exclusive` durability contract that unit tests can only inspect structurally.

Run from the repository root:

```bash
tools/phase7/bridge-durability-lab.sh
```

The script writes an OCI bundle under a temporary workdir, starts a minimal `runsc` sandbox, writes one bridge heartbeat envelope from inside the sandbox using file `fsync`, rename into `outbox/`, and directory `fsync`, then starts the host-side bridge queue reader after the sandbox writer exits. This models a host bridge process restart before the message is read and leaves `evidence.json`, sandbox stdout/stderr, and host-reader logs in the workdir.

Required evidence:

- Candidate git commit.
- `runsc --version`.
- Generated runtime spec showing the bridge mount annotation `dev.gvisor.spec.mount./harness-control/bridge.share = exclusive`.
- Session ID, generation ID, and bridge directory.
- A bridge message written from inside the sandbox after file `fsync`, rename into `outbox/`, and directory `fsync`.
- Host process restart before the host bridge processor reads that message.
- After restart, the host observes and commits the message exactly once.

Passing condition: a fsynced sandbox-side bridge message remains visible to the host after host-process restart and is processed through the normal idempotent queue path. Losing the message, double-processing it, or needing a manual repair fails the gate.

## Live Turn-Start Latency

The live release benchmark measures `POST /api/sessions/{id}/messages` enqueue to committed `ack_turn_started` under lab load. The bridge must already be connected and probed so the measurement covers turn-start control-plane latency, not cold sandbox startup.

Run from the repository root with one or more prewarmed `running_idle` sessions. When multiple session IDs are provided, the tool posts to them concurrently and reports per-session samples plus p50/p95/p99/max:

```bash
PHASE7_LATENCY_SESSION_IDS=sess_a,sess_b tools/phase7/live-turn-start-latency.py
```

Useful overrides:

- `PHASE7_ORCHESTRATOR_URL` defaults to `http://127.0.0.1:8090`.
- `PHASE7_DB` defaults to `/var/lib/harness/sessions/orchestrator.db`.
- `PHASE7_SHARED_SECRET` logs in through `/login` when orchestrator auth is enabled.
- `PHASE7_AUTH_COOKIE` sends an existing raw `Cookie` header instead of logging in.
- `PHASE7_LATENCY_CONTENT` changes the message body; `{session_id}` and `{nonce}` are replaced per sample.

Required evidence:

- Candidate git commit.
- `config/harness.yaml` values for `harness.bridge.poll_interval`, event batching, and durability settings.
- Number of concurrent sessions and turns used for load.
- Per-turn latency samples or summary with p50, p95, p99, and max.

Passing condition: every qualifying sample remains under 50 ms. If the benchmark harness reports percentiles only, p99 and max must both be under 50 ms. Any config change that affects polling, bridge batching, or durable event writes requires remeasurement.
