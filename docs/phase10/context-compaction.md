# Phase 10b: Proactive Context Compaction

> Status: planned on top of the completed [Phase 8 runtime isolation hardening](../phase8/README.md).
> Part of [Phase 10](./README.md).

## Goal

Trigger agent context compaction before the deployed proxy backend's real
context window is exhausted.

The first implementation targets Claude Code because that is the current
model-backed driver. Claude Code self-triggers compaction based on its own
assumptions about the model's context size. When the platform runs through
`claude-code-proxy` to an OpenAI-compatible upstream with a smaller effective
context, sessions silently approach the real limit before Claude Code thinks
compaction is necessary. The session then fails mid-conversation with no
warning.

The fix is in two parts: (1) get accurate per-turn token usage into the
orchestrator, (2) have the orchestrator call the selected driver's compaction
adapter before the running total crosses a configurable budget.

Phase 8 is the completed baseline. Phase 10b depends on proxy request
correlation from the verified contract/resource sandbox identity, not the
broader pre-Phase-8 source-IP assumptions.

## Background: Where Token Usage Lives Today

The data flow today (from the proxy/orchestrator exploration):

- Upstream OpenAI responses contain `usage.prompt_tokens` and `usage.completion_tokens`. The proxy explicitly requests `stream_options.include_usage = true` so streaming responses also include a final usage chunk.
- `convert_openai_to_claude_response` and `convert_openai_streaming_to_claude_with_cancellation` in `/root/claude-code-proxy/src/conversion/response_converter.py` already convert this into Anthropic-shaped `usage.input_tokens` / `usage.output_tokens` and emit it back to Claude Code.
- The proxy already posts `POST /internal/proxy/requests/finish` to the orchestrator at the end of every request via `HarnessProxyObservation.finish_success` in `/root/claude-code-proxy/src/core/harness_observability.py`. Today the payload only carries timing fields.
- The orchestrator stream parser in `orchestrator/internal/server/stream_parser.go` does not parse `message_delta` usage events from Claude Code's stream-json output, so usage is currently dropped on that path.

Conclusion: usage is already correctly accumulated inside the proxy for the
Claude path. The cheapest first integration is to extend the existing `finish`
observation rather than parsing it out of in-sandbox Claude Code stdout. Other
drivers should either report usage through the same proxy observation path or
declare usage reporting unsupported until their adapter can provide it.

After Phase 8, session/turn correlation uses the observed proxy peer IP matched
to the verified contract/resource sandbox identity and active model-request
context. The resulting `proxy_request_id` ties the `finish` event back to the
right turn. No new headers or Claude Code env vars are part of the trusted
path.

## Part 1: Token Reporting Plumbing

### Proxy changes

`/root/claude-code-proxy/src/core/harness_observability.py`:

- Extend the `HarnessProxyObservation` dataclass with three optional int fields: `input_tokens`, `output_tokens`, `cache_read_input_tokens`.
- Extend `finish_success()` to accept usage and include it in the JSON payload posted to `/internal/proxy/requests/finish`.

`/root/claude-code-proxy/src/api/endpoints.py`:

- After the non-streaming OpenAI response is decoded, read `openai_response["usage"]` and pass into `finish_success()`.
- For the streaming path, the inner generator in `convert_openai_streaming_to_claude_with_cancellation` already maintains `usage_data`. Surface it back to the caller (callback or shared mutable container) so `finish_success()` can include the final accumulated values after the stream completes.

### Orchestrator changes

- `orchestrator/internal/server/server.go` (`internalProxyRequestFinish`): add `InputTokens`, `OutputTokens`, `CacheReadInputTokens` to the request struct, decode them, pass to `FinishProxyRequestParams`.
- `orchestrator/internal/store/proxy.go` (`FinishProxyRequest`): add the three token fields to `FinishProxyRequestParams` and the `proxy.request.completed` payload map.

No schema migration is required: the values land inside the existing JSON payload column on the `events` table.

### Pinned proxy contract update

`/root/claude-code-proxy/tests/test_harness_probe_contract.py` currently asserts that finish payloads include `upstream_total_latency_ms` but does not forbid extra fields. Adding token fields is non-breaking. To formalize the new behavior, append assertions that the finish payload includes `input_tokens` and `output_tokens` whenever the upstream returned usage. Then re-pin per the standard process described in `docs/phase7/release-qualification.md`:

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

The orchestrator already has per-turn correlation via `proxy.request.completed`
events. Add a query that, given a session, sums
`input_tokens + output_tokens` over the current driver conversation lifetime
(for Claude Code, since the last cold-start or restore; not since session
creation, because compaction resets the running total).

Add a per-generation counter in memory that the bridge dispatcher updates as `finish` events come in, with the durable event log as the recovery source on restart. The in-memory counter is the hot-path source; the event log is the cold reload source.

### Trigger mechanism

When the running total crosses `soft_threshold * model_context_tokens`, the
orchestrator instructs the selected driver to compact before the next turn
through `DriverCompactionAdapter`.

Initial renderer behavior:

- Claude Code: append a control-plane directive to the next user message, e.g.
  "[Control plane: context budget at X%. Please compact your conversation before
  continuing.]" Claude Code already supports a `/compact` slash command and
  will run it when prompted. This requires no new bridge protocol.
- Pi: use native RPC `compact` with optional custom instructions once the Phase
  9 Pi driver lands; otherwise declare compaction unsupported.

If prompt-based compaction proves unreliable, add a `compact_now` envelope to
the Agent Bridge protocol (see `docs/phase7/bridge-protocol.md`). That is a
heavier change because it requires sandbox-side client updates and a contract
version bump.

When the running total crosses `hard_threshold * model_context_tokens` and the user posts another message, the orchestrator returns HTTP 409 with `error_class: context_budget_exceeded` and a body explaining that compaction must run first. The frontend surfaces this as a clear UI state, not a generic failure.

### Reset

After a successful compaction, reset the running total to the post-compaction
value. For Claude Code, the first-cut signal is the next `result` frame
reporting a low `usage.input_tokens` relative to the prior turn, or a parsed
slash-command echo in stream-json output. The simpler rule (drop by more than
50% turn-over-turn) is sufficient for the first cut. Native drivers such as Pi
should prefer an explicit adapter result when available.

## Implementation Steps

1. Proxy: extend `HarnessProxyObservation` and `finish_success` with token fields.
2. Proxy: surface streaming `usage_data` back to the request handler.
3. Proxy: add token assertions to `test_harness_probe_contract.py`; re-pin.
4. Orchestrator: extend `internalProxyRequestFinish` request struct and `FinishProxyRequestParams`.
5. Orchestrator: include token fields in the `proxy.request.completed` event payload.
6. Orchestrator: per-generation in-memory token counter, fed by finish events, recoverable from event log.
7. Orchestrator: config block `harness.compaction.*` with validation.
8. Orchestrator: trigger logic — soft threshold calls the driver compaction
   adapter; hard threshold rejects new turns with 409.
9. Orchestrator: reset rule on detected compaction.
10. Tests:
    - Proxy contract: token fields present in finish payload.
    - Orchestrator: finish-handler stores tokens in event payload.
    - Orchestrator: counter sums across multiple finish events; survives orchestrator restart by replaying the event log.
    - Orchestrator: soft threshold invokes the selected driver's compaction
      adapter; hard threshold returns 409; reset clears counter on detected
      compaction.

## Risks

- The deployed model's effective context window depends on the OpenAI-compatible backend. `model_context_tokens` must be configured per deployment; a wrong value either compacts too eagerly (wasting context) or too late (defeating the purpose). Document this clearly in `config/harness.yaml` comments.
- Detecting Claude Code compaction by token-count drop is heuristic. If a
  user's next turn happens to be very small, the counter could falsely reset.
  Acceptable for the first cut; revisit if false resets occur.
- Prompt-based compaction requires the driver/model to obey the directive. If it
  ignores the directive, the hard threshold catches the case and forces user
  action. Phase 10a's system prompt should also reinforce that the agent must
  obey control-plane compaction directives. Native adapter compaction is
  preferred where available.
