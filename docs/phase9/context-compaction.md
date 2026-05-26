# Phase 9b: Proactive Context Compaction

> Status: planned. Part of [Phase 9](./README.md).

## Goal

Trigger Claude Code's context compaction earlier than its built-in threshold, based on the deployed proxy backend's real context window rather than the official Claude model's.

Today: Claude Code self-triggers compaction based on its own assumptions about the model's context size. When the harness runs against `claude-code-proxy` translating to an OpenAI-compatible upstream with a smaller effective context, sessions silently approach the real limit before Claude Code thinks compaction is necessary. The session then fails mid-conversation with no warning.

The fix is in two parts: (1) get accurate per-turn token usage into the orchestrator, (2) have the orchestrator instruct compaction before the running total crosses a configurable budget.

## Background: Where Token Usage Lives Today

The data flow today (from the proxy/orchestrator exploration):

- Upstream OpenAI response contains `usage.prompt_tokens` and `usage.completion_tokens`. The proxy explicitly requests `stream_options.include_usage = true` so streaming responses also include a final usage chunk. See `/root/claude-code-proxy/src/core/client.py:152, 191`.
- `convert_openai_to_claude_response` and `convert_openai_streaming_to_claude_with_cancellation` in `/root/claude-code-proxy/src/conversion/response_converter.py` already convert this into Anthropic-shaped `usage.input_tokens` / `usage.output_tokens` and emit it back to Claude Code (non-streaming at line 72; streaming `message_delta` at line 394).
- The proxy already posts `POST /internal/proxy/requests/finish` to the orchestrator at the end of every request via `HarnessProxyObservation.finish_success` in `/root/claude-code-proxy/src/core/harness_observability.py:95`. Today the payload only carries timing fields.
- The orchestrator stream parser at `orchestrator/internal/server/stream_parser.go:146` does not parse `message_delta` events from Claude Code's stream-json output, so usage is currently dropped on that path.

Conclusion: usage is already correctly accumulated inside the proxy. The cheapest integration is to extend the existing `finish` observation rather than parsing it out of the in-sandbox Claude Code stdout.

Session/turn correlation already works: the proxy reports `sandbox_source_ip` on `start`, the orchestrator's `StartProxyRequest` at `orchestrator/internal/store/proxy.go:86` resolves it to `session_id` / `turn_id` / `generation_id` via the active model-request context, and the resulting `proxy_request_id` ties the `finish` event back to the right turn. No new headers, no plumbing through Claude Code env vars.

## Part 1: Token Reporting Plumbing

### Proxy changes

`/root/claude-code-proxy/src/core/harness_observability.py`:

- Extend the `HarnessProxyObservation` dataclass (around line 15) with three optional int fields: `input_tokens`, `output_tokens`, `cache_read_input_tokens`.
- Extend `finish_success()` (around line 95) to accept usage and include it in the JSON payload posted to `/internal/proxy/requests/finish`.

`/root/claude-code-proxy/src/api/endpoints.py`:

- After the non-streaming OpenAI response is decoded (around line 115), read `openai_response["usage"]` and pass into `finish_success()`.
- For the streaming path, the inner generator in `convert_openai_streaming_to_claude_with_cancellation` already maintains `usage_data` (line 242). Surface it back to the caller (callback or shared mutable container) so `finish_success()` can include the final accumulated values after the stream completes.

### Orchestrator changes

- `orchestrator/internal/server/server.go:1074` (`internalProxyRequestFinish`): add `InputTokens`, `OutputTokens`, `CacheReadInputTokens` to the request struct, decode them, pass to `FinishProxyRequestParams`.
- `orchestrator/internal/store/proxy.go:210`: `FinishProxyRequest` already builds a `payload` map for the `proxy.request.completed` event. Add the three token fields to that map.

No schema migration is required: the values land inside the existing JSON payload column on the `events` table.

### Pinned proxy contract update

`/root/claude-code-proxy/tests/test_harness_probe_contract.py` currently asserts that finish payloads include `upstream_total_latency_ms` (around line 112) but does not forbid extra fields. Adding token fields is non-breaking. To formalize the new behavior, append assertions that the finish payload includes `input_tokens` and `output_tokens` whenever the upstream returned usage. Then re-pin per the standard process described in `docs/phase7/release-qualification.md`:

1. Update `tests/test_harness_probe_contract.py`.
2. Re-run the proxy contract gate (`.venv/bin/python -m pytest -q tests/test_harness_probe_contract.py`).
3. Record the new commit in `tools/phase7/release-gates.py`'s pinned-proxy reference.

## Part 2: Compaction Trigger

### Config

```yaml
harness:
  compaction:
    enabled: true
    model_context_tokens: 128000     # the deployed model's real context window
    soft_threshold: 0.65             # instruct compaction when running total crosses this fraction
    hard_threshold: 0.80             # if a turn would push past this, refuse the turn and require compaction
```

`model_context_tokens` is operator-configured per deployment. There is no autodiscovery; the proxy backend's true context window depends on the OpenAI-compatible model behind it.

### Aggregation

The orchestrator already has per-turn correlation via `proxy.request.completed` events. Add a query that, given a session, sums `input_tokens + output_tokens` over the events of the current Claude Code session-process lifetime (i.e., since the last cold-start or restore; not since session creation, because compaction resets the running total).

Add a per-generation counter in memory that the bridge dispatcher updates as `finish` events come in, with the durable event log as the recovery source on restart. The in-memory counter is the hot-path source; the event log is the cold reload source.

### Trigger mechanism

When the running total crosses `soft_threshold * model_context_tokens`, the orchestrator instructs Claude Code to compact before the next turn. Options for the mechanism, in order of preference:

1. **System-prompt injection on next turn (preferred).** Append to the next user message: "[Harness: context budget at X%. Please compact your conversation before continuing.]" Claude Code already supports a `/compact` slash command and will run it when prompted. This requires no new protocol; the message is passed via the existing bridge `emit_input` path.
2. **Bridge protocol extension.** Add a `compact_now` envelope to the Agent Bridge protocol (see `docs/phase7/bridge-protocol.md`). Heavier change, requires sandbox-side client update and a contract version bump.

Recommend (1) for the first cut.

When the running total crosses `hard_threshold * model_context_tokens` and the user posts another message, the orchestrator returns HTTP 409 with `error_class: context_budget_exceeded` and a body explaining that compaction must run first. The frontend surfaces this as a clear UI state, not a generic failure.

### Reset

After a successful compaction (the next `result` frame from Claude Code reports a low `usage.input_tokens` relative to the prior turn), reset the running total to the post-compaction value. Detect compaction by the order-of-magnitude drop in `input_tokens`, or by parsing the slash-command echo in stream-json output. The simpler rule (drop by more than 50% turn-over-turn) is sufficient for the first cut.

## Implementation Steps

1. Proxy: extend `HarnessProxyObservation` and `finish_success` with token fields.
2. Proxy: surface streaming `usage_data` back to the request handler.
3. Proxy: add token assertions to `test_harness_probe_contract.py`; re-pin.
4. Orchestrator: extend `internalProxyRequestFinish` request struct and `FinishProxyRequestParams`.
5. Orchestrator: include token fields in the `proxy.request.completed` event payload.
6. Orchestrator: per-generation in-memory token counter, fed by finish events, recoverable from event log.
7. Orchestrator: config block `harness.compaction.*` with validation.
8. Orchestrator: trigger logic — soft threshold appends compaction directive to next user message; hard threshold rejects new turns with 409.
9. Orchestrator: reset rule on detected compaction.
10. Tests:
    - Proxy contract: token fields present in finish payload.
    - Orchestrator: finish-handler stores tokens in event payload.
    - Orchestrator: counter sums across multiple finish events; survives orchestrator restart by replaying the event log.
    - Orchestrator: soft threshold injects directive; hard threshold returns 409; reset clears counter on detected compaction.

## Risks

- The deployed model's effective context window depends on the OpenAI-compatible backend. `model_context_tokens` must be configured per deployment; a wrong value either compacts too eagerly (wasting context) or too late (defeating the purpose). Document this clearly in `config/harness.yaml` comments.
- Detecting compaction by token-count drop is heuristic. If a user's next turn happens to be very small, the counter could falsely reset. Acceptable for the first cut; revisit if false resets occur.
- Compaction requires Claude Code to actually accept the directive. If the model ignores it, the hard threshold catches the case and forces user action. Phase 9a's system prompt should also reinforce that the agent must obey harness compaction directives.
