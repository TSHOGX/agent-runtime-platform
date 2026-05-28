# Network And Probes

Per-generation network resources, egress policy, host-side and in-sandbox probes, and proxy/upstream observability. The DB row that backs these resources is `network_profiles` (see [schema.md](./schema.md#network_profiles)); this file owns the *semantics* of those fields and the network rules they encode.

## Network Profile Semantics

Network config must be explicit and persisted, not inferred from ambient host settings. The schema separates **network** (the host-resource shape: netns, veth, egress, gateway) from **agent runtime** (model, output format, traffic shaping policy), but Phase 7 still treats the pair as one generation contract: any change that alters either side requires a fresh generation and a fresh network profile. The only reuse path is physical restore of the same `generation_id`.

The full field list and the immutability rule for `network_profile_id` / `agent_runtime_profile_id` (and the single-exception physical-restore path) live in [schema.md](./schema.md#network_profiles) and [schema.md](./schema.md#agent_runtime_profiles); this file does not restate them.

## CIDR Pool And `/30` Allocation

The host carves IPs from a configured CIDR pool (`harness.network.cidr_pool`, default `10.200.0.0/16`) into per-generation `/30` subnets — gateway IP is `.1`, sandbox IP is `.2`. The allocator hands out the lowest free `/30`. Identity (the `/30`, the netns name, the veth pair, the control/bundle dirs) is released back to the pool **only when the allocation reaches `destroyed`** — never on `reclaimable`.

This is consistent with the failed-retention window ([runtime-resources.md](./runtime-resources.md#failed-retention-window)): a `failed` generation moves to `reclaimable` after retention so an operator can inspect netns/control/spec/log artifacts; if the `/30` were reused while in `reclaimable`, those artifacts would either disappear (defeating retention) or collide with the new generation. Pool capacity is therefore the count of allocations *not in `destroyed`* (i.e. `allocating + ready + live + reserved_checkpointed + recreating + reclaimable`). `harness.max_sessions` is a separate non-terminal session ceiling for retained history, and it may be higher than the live /30 pool. `/api/quota` reports `soft_session_ceiling` and `live_pool_ceiling` separately without clamping them together.

The allocator's uniqueness constraint over every non-destroyed network row (`netns_name`, `netns_path`, `host_veth`, `sandbox_veth`, `host_gateway_ip`, `host_side_cidr`, `sandbox_ip_cidr`) is defined alongside the table in [schema.md](./schema.md#network_profiles); path uniqueness lives on `runtime_generation_resources`. `reserved_checkpointed` identity stays reserved for physical restore and is not reused.

**Pool exhaustion.** The allocator returns a typed error before any generation row is created. The session-create / first-turn POST surfaces this as `503` with `error_class = pool_exhausted`, and the queued turn is rejected rather than left waiting. Reaper urgency does not change — a stuck `recreating` allocation is still recovered only by the startup sweep, not by allocator pressure. The `harness.max_sessions` soft ceiling can return the same error class when too many non-terminal sessions are retained, but operators recover that quota by closing sessions rather than by shrinking live runtime usage.

## Egress Policy

7a populates every network-profile field needed to run the data-analysis path end-to-end without `phase1-demo`: netns / veth / IP / gateway / CIDR, plus the static egress allow-list covering the local LLM proxy at `host_gateway_ip:8082`, the configured Doris FE/BE hosts and ports (`doris_fe_hosts`, `doris_be_hosts`, `doris_ports`), and DNS (`dns_policy`) when any of those targets are expressed as hostnames. These are *static, lab-wide* values read from `harness.network.egress` config — every 7a-allocated network profile gets the same Doris/DNS allow-list. This is what 7a's "production-like mode" actually requires; without Doris egress, the local LLM proxy works but `vhr_data` queries break, which is the project's primary product path.

Phase 11 turns the same fields into per-tenant policy: tenant-scoped Doris ACLs, per-session egress quotas, and rotation of the host-firewall enforcement layer. The schema is identical between Phase 7 and Phase 11; what changes in Phase 11 is the *source* of the values (per-tenant policy rows feeding `egress_policies` instead of a single host-config block) and the enforcement strength (host firewall + nft chains rather than just per-netns nft rules).

The allocator persists those fields into every `network_profiles` row and materializes the matching `egress_policies` row. Tests assert the Doris/DNS values are populated from `harness.network.egress` and that the generated nft rules allow only the proxy, configured Doris targets, DNS when needed, and established return traffic.

## Network Path

Target network path:

```text
Claude Code inside sandbox
  -> http://{allocated_host_gateway_ip}:8082/v1/messages?beta=true
  -> host namespace listener at http://0.0.0.0:8082
  -> claude-code-proxy
  -> upstream model provider
```

`{allocated_host_gateway_ip}` is generation-scoped allocation data from the network profile. It is not derived from a shared `phase1-demo` netns or from a process-wide constant.

## Probes

Two probe phases:

- **Pre-start / pre-restore host-side netns probe.** Host runs it from the recreated netns once netns/veth/IP/egress resources exist. Validates route, gateway, egress policy, and proxy reachability without requiring a sandbox process.
- **Post-start / post-restore in-sandbox probe.** Run by the Agent Bridge after `hello` and before `claim_next_turn`. Validates the agent-visible Anthropic base URL and auth config.

Both probes target the local proxy at `http://{host_gateway_ip}:8082`, never the upstream Anthropic API: the proxy is the actual boundary, probing upstream would consume real quota and reveal nothing about netns/egress policy, and the proxy short-circuits auth so a 401 is a deterministic local failure.

### Probe contract with `claude-code-proxy`

Phase 7a pins a dedicated reachability route, `GET /healthz` returning `200` with body `{"status":"ok"}`, on the proxy itself; the host-side netns probe and the in-sandbox probe both call it. This replaces the previous reliance on `HEAD /` returning `405`, which depended on undocumented behavior of the current proxy version and would silently start "passing" if a future proxy build returned `200` for `HEAD /` — a probe that passes when routing is broken is worse than no probe.

Auth-path reachability is verified by the second probe call: `POST /v1/messages` with the configured key and a deliberately malformed body, which the proxy must short-circuit locally. The accept set is configurable per profile (`harness.probe.accept_status.get_healthz`, `harness.probe.accept_status.post_v1_messages.unauthorized`, and `harness.probe.accept_status.post_v1_messages.malformed_authenticated`), defaulting to `{200}`, `{401}`, and `{400}` respectively; any other status, refused connection, timeout, or socket failure fails the probe. The proxy must reject locally — a probe must never reach upstream. The current proxy key is `123`; any other key is misconfigured.

Release qualification runs a contract test against the pinned `claude-code-proxy` version: it asserts `GET /healthz -> 200`, `POST /v1/messages` (no key or wrong key) `-> 401`, and `POST /v1/messages` (configured key with empty body) `-> 400`. Any divergence in proxy behavior blocks deployment, forcing an explicit re-pin of the proxy version or a probe-config update. The in-repo unit tests cover the orchestrator and bridge probe call shapes and configurable accept sets; the external proxy contract is satisfied by the pinned-proxy release gate, not by the default Go/Python unit test suite.

### Probe failure handling

Probe failure must not leave the session silently queued. Each probe gets a bounded retry budget configured by `harness.probe.pre_start_attempts` × `pre_start_interval` for the host-side netns probe (defaults: 3 attempts, 500 ms apart) and `harness.probe.post_start_attempts` × `post_start_interval` for the in-sandbox probe (defaults: 5 attempts, 1 s apart) — see [implementation-plan.md](./implementation-plan.md#phase-7-configuration-schema). On exhaustion the generation is marked `failed` with `error_class = probe_failed_pre_start` or `probe_failed_post_start`; the session surfaces the error through the standard generation-failure path so the UI sees a concrete failure instead of a stuck queue. Cold fallback applies as it does for any generation failure (see [checkpoint-restore.md](./checkpoint-restore.md#cold-fallback-path)).

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

Correlation must not depend on Claude Code exposing response headers. The control plane owns the active-context table (`active_model_request_contexts` in [schema.md](./schema.md#active_model_request_contexts)); the proxy accesses it only through localhost-only control-plane endpoints:

```text
POST /internal/proxy/requests/start
  input:  sandbox_source_ip, proxy_request_id, upstream_model, upstream_base_url
  output: session_id, turn_id, generation_id, request_sequence

POST /internal/proxy/requests/finish
  input:  proxy_request_id, timing/status/error payload
  output: accepted | stale_unknown_request
```

`ack_turn_started` creates the active context before the bridge launches agent work. Bridge heartbeat renews the context TTL with the turn lease; terminal turn CAS deletes it. Startup recovery deletes contexts from prior orchestrator owners, and proxy lookup ignores expired rows. On `start`, the control plane atomically resolves `sandbox_source_ip`, increments the per-turn `request_sequence`, and appends `proxy.request.started`; on `finish`, it appends `proxy.request.completed` or `proxy.request.failed` with the same `proxy_request_id`.

### Source-IP correlation requires single-in-flight-turn-per-generation

`sandbox_source_ip` uniquely identifies a generation, not a turn — every turn that runs against generation G shares the same per-generation sandbox IP. The IP-based join is therefore only sound while there is **at most one turn in `leased`/`running` state per generation at a time**. Phase 7 enforces this with the `claim_next_turn` CAS defined in the [Single-Helper Contract](./schema.md#single-helper-contract): queued turns are unbound (no `generation_id`), the lowest-sequence queued turn for the session is selected by `MIN(sequence)`, the helper's update binds `generation_id = :generation_id` at claim time, and the same statement carries a `NOT EXISTS (… status in ('leased','running') for this generation_id)` predicate that fails the claim if another turn is already in-flight on the same generation. The proxy-correlation guarantee follows directly from that predicate; the canonical SQL lives in the Single-Helper Contract / `claim_next_turn` block and is not restated here, so future implementers cannot copy a divergent variant from this section by mistake.

If a future protocol ever supports concurrent turns per generation, the source-IP join is no longer unique and the bridge-managed header path becomes mandatory: the bridge intercepts the agent's outbound request to the proxy, attaches a turn-correlation header (e.g. `X-Harness-Turn-Id` signed with a per-generation secret to prevent in-sandbox spoofing), and the proxy uses the header-derived turn id rather than `sandbox_source_ip` for the join. The header path is the right answer because the bridge is inside the sandbox and is the only component that knows which turn each agent process invocation is servicing; the source-IP path is only correct when "the turn currently running on this generation" is unambiguous.

For Phase 7 the single-in-flight invariant holds and the source-IP join is the documented mechanism. The header-based fallback is described here so that the schema and proxy-event payload reserve a `correlation_mode` enum (`source_ip` | `header`) on the `proxy.request.started` event from day one, even though the value is always `source_ip` until concurrent turns ship.
