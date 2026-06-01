# Phase 10b: Proactive Context Compaction

> Status: planned on top of the completed [Phase 8 runtime isolation hardening](../phase8/README.md).
> Part of [Phase 10](./README.md).

## Goal

Compact agent context before the deployed proxy backend's real context window
is exhausted.

Claude Code currently decides when to compact using its own model assumptions.
When routed through `claude-code-proxy` to an OpenAI-compatible model with a
smaller effective context window, the session can hit the backend limit before
Claude Code self-compacts. Phase 10b moves compaction budgeting into the
orchestrator.

## Usage Authority

The first implementation uses the host-side proxy finish event as the
authoritative token source.

Why proxy first:

- OpenAI upstream responses already include usage, and streaming requests can
  include a final usage chunk.
- `claude-code-proxy` already converts OpenAI usage to Anthropic-shaped usage
  for Claude Code.
- The proxy already posts `POST /internal/proxy/requests/finish` for every
  model request.
- Phase 8 proxy correlation ties the request to the verified sandbox identity
  and active turn without adding trusted in-sandbox headers.

Agent-side usage is not the first authority in Phase 10b. It depends on
driver-specific stdout/RPC formats, can be lost if the sandbox dies before
`ack_turn_completed`, and would need durable replay semantics after restart.
Future drivers may expose native usage, but they should either feed the same
host-side observation path or declare usage reporting unsupported.

## Token Reporting

Proxy changes:

- Add optional `input_tokens`, `output_tokens`, and
  `cache_read_input_tokens` fields to the finish observation payload.
- Populate them for non-streaming responses from OpenAI `usage`.
- Surface accumulated streaming `usage_data` back to the request handler before
  posting finish.
- Extend the pinned proxy contract test to assert token fields when upstream
  usage exists, then re-pin the proxy evidence.

Orchestrator changes:

- Decode the new token fields in `internalProxyRequestFinish`.
- Store them in the existing `proxy.request.completed` event payload.
- No new table or column is required; the event JSON payload is enough.

## Config

```yaml
harness:
  compaction:
    enabled: true
    model_context_tokens: 128000
    soft_threshold: 0.65
    hard_threshold: 0.80
```

`model_context_tokens` is operator-configured per deployment. There is no
autodiscovery because the effective window depends on the OpenAI-compatible
backend behind the proxy.

## Aggregation And Trigger

Maintain a per-generation hot counter fed by proxy finish events. Rebuild it
from the durable event log after orchestrator restart.

For a session, sum `input_tokens + output_tokens` across the current driver
conversation lifetime. For Claude Code, that means since the last cold start,
restore, or successful compaction; not since session creation.

Trigger behavior:

- Soft threshold: when usage crosses
  `soft_threshold * model_context_tokens`, call the selected driver's
  `DriverCompactionAdapter` before the next turn.
- Hard threshold: when usage crosses
  `hard_threshold * model_context_tokens`, reject new turns with HTTP 409 and
  `error_class: context_budget_exceeded` until compaction succeeds.

Initial adapters:

- Claude Code: append a control-plane compaction directive to the next user
  message. If prompt-based compaction proves unreliable, add a bridge
  `compact_now` envelope in a later protocol bump.
- Pi: use native RPC compaction when available; otherwise declare compaction
  unsupported.

After successful compaction, reset the counter to the post-compaction usage.
For Claude Code first cut, detect success by an explicit stream signal when
available, otherwise by a large input-token drop. Native drivers should report
an explicit adapter result.

## Implementation Checklist

1. Add token fields to proxy finish observations and contract tests.
2. Surface streaming usage into proxy finish.
3. Decode/store token fields in orchestrator finish handling.
4. Add `harness.compaction` config and validation.
5. Add per-generation counters recoverable from the event log.
6. Add soft/hard threshold handling through driver compaction adapters.
7. Add reset detection after compaction.
8. Add frontend handling for `context_budget_exceeded`.

## Acceptance Tests

- Proxy finish payload includes token fields when upstream usage exists.
- Orchestrator stores token fields in `proxy.request.completed`.
- Counter sums multiple finish events and recovers after restart.
- Soft threshold invokes the selected driver's compaction adapter.
- Hard threshold returns HTTP 409 with `context_budget_exceeded`.
- Successful compaction resets the counter.

## Risks

- Wrong `model_context_tokens` causes early or late compaction; document this
  clearly in deployment config comments.
- Claude Code compaction detection by token drop is heuristic; prefer explicit
  driver signals when available.
- Prompt-based compaction can be ignored by the agent/model; the hard threshold
  remains the enforcement backstop.
