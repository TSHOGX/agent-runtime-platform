# Network And Probes

Per-generation network resources, egress policy, host-side and in-sandbox probes, and proxy/upstream observability. The DB row that backs these resources is `network_profiles` (see [schema.md](./schema.md#network_profiles)); this file owns the *semantics* of those fields and the network rules they encode.

## Network Profile Semantics

Network config must be explicit and persisted, not inferred from ambient host settings. The schema separates **network** (the host-resource shape: netns, veth, egress, gateway) from **agent runtime** (model, output format, traffic shaping policy). Both are durable; both are fenced by generation; but they have independent lifecycles, and a change to one should not force re-allocation of the other.

The full field list lives in [schema.md](./schema.md#network_profiles). Each `runtime_generations` row references exactly one `network_profile_id` and one `agent_runtime_profile_id`, and **both references are immutable for the lifetime of the generation row**. Any change that requires a new generation also requires a new `network_profile` row drawn from a fresh allocation; the predecessor's allocation moves through the standard `live -> reclaimable -> destroyed` path and its identity is not reused until the row is `destroyed`.

The single exception that reuses host resources is **physical restore of the same `generation_id`** (see [invariants.md](./invariants.md#hard-invariants) and [runtime-resources.md](./runtime-resources.md#resource-allocation-lifecycle)): the same generation row's allocation transitions `reserved_checkpointed -> recreating -> ready -> live` while keeping its `network_profile_id` and `agent_runtime_profile_id` bindings; it is not "another generation" and therefore not subject to the no-reuse rule.

## CIDR Pool And `/30` Allocation

The host carves IPs from a configured CIDR pool (`harness.network.cidr_pool`, default `10.200.0.0/16`) into per-generation `/30` subnets — gateway IP is `.1`, sandbox IP is `.2`. The allocator hands out the lowest free `/30`. Identity (the `/30`, the netns name, the veth pair, the control/bundle dirs) is released back to the pool **only when the allocation reaches `destroyed`** — never on `reclaimable`.

This is consistent with the failed-retention window ([runtime-resources.md](./runtime-resources.md#failed-retention-window)): a `failed` generation moves to `reclaimable` after retention so an operator can inspect netns/control/spec/log artifacts; if the `/30` were reused while in `reclaimable`, those artifacts would either disappear (defeating retention) or collide with the new generation. Pool capacity is therefore the count of allocations *not in `destroyed`* (i.e. `allocating + ready + live + reserved_checkpointed + recreating + reclaimable`); `harness.max_sessions` is the soft policy ceiling and must be set below the pool ceiling. Both ceilings are reported by `/api/quota`.

The allocator enforces uniqueness over every non-destroyed row for: `netns_name`, `netns_path`, `host_veth`, `sandbox_veth`, `host_gateway_ip`, `host_side_cidr`, `sandbox_ip_cidr`, `control_dir`, `control_manifest_path`, `runtime_bundle_dir`. `reserved_checkpointed` allocations are not live processes, but their identity stays reserved for physical restore and must not be reused.

**Pool exhaustion.** The allocator returns a typed error before any generation row is created. The session-create / first-turn POST surfaces this as `503` with `error_class = pool_exhausted`, and the queued turn is rejected rather than left waiting. Reaper urgency does not change — a stuck `recreating` allocation is still recovered only by the startup sweep, not by allocator pressure. The `harness.max_sessions` soft ceiling produces the same error class but is hit first under the lab's defaults (`harness.max_sessions=30` vs ~16K `/30` slots in `10.200.0.0/16`).

## Egress Policy

7a populates every network-profile field needed to run the data-analysis path end-to-end without `phase1-demo`: netns / veth / IP / gateway / CIDR, plus the static egress allow-list covering the local LLM proxy at `host_gateway_ip:8082`, the configured Doris FE/BE hosts and ports (`doris_fe_hosts`, `doris_be_hosts`, `doris_ports`), and DNS (`dns_policy`) when any of those targets are expressed as hostnames. These are *static, lab-wide* values read from `harness.network.egress` config — every 7a-allocated network profile gets the same Doris/DNS allow-list. This is what 7a's "production-like mode" actually requires; without Doris egress, the local LLM proxy works but `vhr_data` queries break, which is the project's primary product path.

Phase 8 turns the same fields into per-tenant policy: tenant-scoped Doris ACLs, per-session egress quotas, and rotation of the host-firewall enforcement layer. The schema is identical between 7a and 8; what changes in 8 is the *source* of the values (per-tenant policy rows feeding `egress_policies` instead of a single host-config block) and the enforcement strength (host firewall + nft chains rather than just per-netns nft rules).

Schema-only fields not yet wired are not silently optional — the test matrix asserts that on 7a, every `network_profiles` row is populated with the lab-wide Doris/DNS values exactly as configured (not NULL), and that `egress_policies` rows materialize the corresponding allow-rules. An accidental partial implementation that leaves Doris fields empty fails loudly.

## Network Path

Target network path:

```text
Claude Code inside sandbox
  -> http://{allocated_host_gateway_ip}:8082/v1/messages?beta=true
  -> host namespace listener at http://0.0.0.0:8082
  -> claude-code-proxy
  -> upstream model provider
```

In the lab today `{allocated_host_gateway_ip}` resolves to `10.200.1.1` because the netns is fixed; under the target architecture it is generation-scoped allocation data per the network profile fields above.

## Probes

Two probe phases:

- **Pre-start / pre-restore host-side netns probe.** Host runs it from the recreated netns once netns/veth/IP/egress resources exist. Validates route, gateway, egress policy, and proxy reachability without requiring a sandbox process.
- **Post-start / post-restore in-sandbox probe.** Run by the Agent Bridge after `hello` and before `claim_next_turn`. Validates the agent-visible Anthropic base URL and auth config.

Both probes target the local proxy at `http://{host_gateway_ip}:8082`, never the upstream Anthropic API: the proxy is the actual boundary, probing upstream would consume real quota and reveal nothing about netns/egress policy, and the proxy short-circuits auth so a 401 is a deterministic local failure.

### Probe contract with `claude-code-proxy`

Phase 7a pins a dedicated reachability route, `GET /healthz` returning `200` with body `{"status":"ok"}`, on the proxy itself; the host-side netns probe and the in-sandbox probe both call it. This replaces the previous reliance on `HEAD /` returning `405`, which depended on undocumented behavior of the current proxy version and would silently start "passing" if a future proxy build returned `200` for `HEAD /` — a probe that passes when routing is broken is worse than no probe.

Auth-path reachability is verified by the second probe call: `POST /v1/messages` with the configured key, which the proxy must short-circuit and reject locally. The accept set is configurable per profile (`harness.probe.accept_status`, default `{401}` for `POST /v1/messages` and `{200}` for `GET /healthz`); any other status, refused connection, timeout, or socket failure fails the probe. The proxy must reject locally — a probe must never reach upstream. The current proxy key is `123`; any other key is misconfigured.

A contract test in the harness CI build runs against the pinned `claude-code-proxy` version on every release: it asserts `GET /healthz → 200`, `POST /v1/messages` (no key) `→ 401`, and `POST /v1/messages` (configured key with empty body) `→ 401` (because the proxy rejects malformed bodies before forwarding). Any divergence in proxy behavior fails the build, forcing an explicit re-pin of the proxy version or a probe-config update before deployment.

### Probe failure handling

Probe failure must not leave the session silently queued. Each probe gets a bounded retry budget (default: 3 attempts, 500 ms apart for the host-side netns probe; 5 attempts, 1 s apart for the in-sandbox probe). On exhaustion the generation is marked `failed` with `error_class = probe_failed_pre_start` or `probe_failed_post_start`; the session surfaces the error through the standard generation-failure path so the UI sees a concrete failure instead of a stuck queue. Cold fallback applies as it does for any generation failure (see [checkpoint-restore.md](./checkpoint-restore.md#cold-fallback-path)).

Egress policy is part of the network profile, not later hardening: allow the local proxy at `host_gateway_ip:8082`, allow Doris FE/BE hosts and ports explicitly, allow DNS only if the profile uses hostnames, deny everything else by default.

## Proxy And Upstream Observability

Sandbox-to-proxy reachability is only one part of the model path. Phase 7 also makes the proxy/upstream hop observable enough to debug slow or stuck turns:

```text
proxy_request_id
session_id
turn_id
generation_id
upstream_model
upstream_base_url
proxy_connect_latency_ms
upstream_first_byte_latency_ms
upstream_total_latency_ms
retry_count
timeout_kind = connect | first_byte | total | idle_stream
                                              -- non-NULL iff error_class = timeout, NULL otherwise
http_status
error_class = auth | network | upstream_5xx | rate_limit | timeout
            | malformed_stream | canceled
```

Correlation must not depend on Claude Code exposing response headers. When the bridge starts a turn, the control plane registers `active_model_request_context` (`session_id`, `turn_id`, `generation_id`, `sandbox_source_ip`, `lease_owner`, `expires_at`); claude-code-proxy matches inbound requests by `sandbox_source_ip` against the active context, assigns `proxy_request_id` and a per-turn `request_sequence`, and emits `proxy.request.started` / `proxy.request.completed` events that the orchestrator joins to `turn_id` / `generation_id`.

### Source-IP correlation requires single-in-flight-turn-per-generation

`sandbox_source_ip` uniquely identifies a generation, not a turn — every turn that runs against generation G shares the same per-generation sandbox IP. The IP-based join is therefore only sound while there is **at most one turn in `leased`/`running` state per generation at a time**. Phase 7 enforces this with the `claim_next_turn` CAS defined in the [Single-Helper Contract](./schema.md#single-helper-contract): queued turns are unbound (no `generation_id`), the lowest-sequence queued turn for the session is selected by `MIN(sequence)`, the helper's update binds `generation_id = :generation_id` at claim time, and the same statement carries a `NOT EXISTS (… status in ('leased','running') for this generation_id)` predicate that fails the claim if another turn is already in-flight on the same generation. The proxy-correlation guarantee follows directly from that predicate; the canonical SQL lives in the Single-Helper Contract / `claim_next_turn` block and is not restated here, so future implementers cannot copy a divergent variant from this section by mistake.

If at any future point we want to support concurrent turns per generation (the protocol is structurally close — see the `NOT EXISTS` predicate in completion CAS case 3), the source-IP join is no longer unique and the **fallback bridge-managed local gateway becomes the default** rather than an alternative: the bridge intercepts the agent's outbound request to the proxy, attaches a turn-correlation header (e.g. `X-Harness-Turn-Id` signed with a per-generation secret to prevent in-sandbox spoofing), and the proxy uses the header-derived turn id rather than `sandbox_source_ip` for the join. The header path is the right answer because the bridge is inside the sandbox and is the only component that knows which turn each agent process invocation is servicing; the source-IP path is only correct when "the turn currently running on this generation" is unambiguous.

For Phase 7 the single-in-flight invariant holds and the source-IP join is the documented mechanism. The header-based fallback is described here so that the schema and proxy-event payload reserve a `correlation_mode` enum (`source_ip` | `header`) on the `proxy.request.started` event from day one, even though the value is always `source_ip` until concurrent turns ship.
