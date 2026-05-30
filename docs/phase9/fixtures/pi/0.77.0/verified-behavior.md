# Pi 0.77.0 Phase 9 Evidence

Retrieved on 2026-05-30 from:

- https://registry.npmjs.org/@earendil-works%2Fpi-coding-agent/0.77.0
- https://pi.dev/docs/latest/rpc
- https://pi.dev/docs/latest/usage
- https://pi.dev/docs/latest/settings

Pinned package:

- Package: `@earendil-works/pi-coding-agent`
- Version: `0.77.0`
- NPM shasum: `627664c042507babf8a134a3770285272ccae5d8`
- NPM integrity: `sha512-huS+k+dhQRR9PlTK7crLfeSRUw3a96V6JYfP0ZH3Zkko/m10gsYk8dKQmwScSy5Dll516pXorz19BURfD6S2qQ==`
- Node engine: `>=22.19.0`

Runtime evidence:

- RPC mode starts with `pi --mode rpc`.
- Smoke tests may add `--no-session`; production must not.
- Production starts with `--session-dir /agent-home/.pi/agent/sessions`.
- The harness uses RPC `switch_session` with `sessionPath`, followed by `get_session_stats`, for restore selection because this fixture does not prove a startup session selector for Phase 9.
- Session state evidence is accepted only when `get_session_stats.sessionFile` resolves under `/agent-home/.pi/agent/sessions` and `sessionId` is non-empty.
- Startup gates are `PI_CODING_AGENT_DIR=/agent-home/.pi/agent`, `PI_CODING_AGENT_SESSION_DIR=/agent-home/.pi/agent/sessions`, `PI_OFFLINE=1`, `PI_SKIP_VERSION_CHECK=1`, and `PI_TELEMETRY=0`.
- Normalizer tests consume `event-normalizer-corpus.jsonl` and fail closed for event types outside `event-schema.json`.
