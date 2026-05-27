# Phase 8 Model Proxy Boundary

Phase 8 keeps upstream model provider credentials entirely host-side. Sandbox
model access is authorized by the local proxy from trusted network identity,
active turn context, driver entitlement, and the verified
`sandbox-isolation-v1` contract.

No upstream provider secret is mounted, rendered, exported, logged, written to a
bridge queue, stored in a sandbox-visible DB row, included in release evidence,
or passed to helper subprocesses as a command-line argument.

## Trust Domains

Host-only provider credentials:

- readable only by the orchestrator/proxy trust domain;
- stored in a secret manager or a configured file-backed provider credential
  root that passes Phase 8 canonical root disjointness validation;
- never materialized into generation resources;
- redacted from logs, events, DB rows, command lines, stdout/stderr, checkpoint
  metadata, and release evidence.

Sandbox-scoped secrets:

- disabled by default in Phase 8;
- require distinct config, schema, paths, and tests if introduced later;
- are not provider credentials.

The legacy `harness.secrets.root` sandbox secret mount contract is removed from
Phase 8. Old secret roots are migration inventory only and must be deleted or
quarantined outside all active Phase 8 roots.

Local proxy tokens:

- out of scope for Phase 8;
- require a separate broker design before any token can be hidden from Claude,
  tools, `/proc`, bridge queues, or logs.

## Authorization Inputs

The authorizer uses only trusted inputs:

- actual TCP peer source IP observed by the proxy;
- authenticated proxy-to-orchestrator correlation over a Unix domain socket;
- active turn context written by the orchestrator;
- durable `agent_runtime_profiles.model_access_allowed`;
- copied `active_model_request_contexts.model_access_allowed`;
- live `runtime_resource_instances` row;
- verified canonical `SandboxContract` payload.

Sandbox-controlled headers, request bodies, query params, env vars, bridge
payloads, and `ack_turn_started.sandbox_source_ip` are diagnostics only.

At turn start, the orchestrator writes
`active_model_request_contexts.sandbox_source_ip` from the verified contract /
resource instance sandbox IP. It does not trust the sandbox ack payload as an
authorization fact.

## Network Identity Anti-Spoof Requirements

The proxy may trust the observed TCP peer source IP only because the runtime
also proves the sandbox cannot mutate or spoof its network identity.
`RuntimeAdapter` must render and release-gate the OCI spec so the sandbox
process and its descendants have:

- no `CAP_NET_ADMIN`, `CAP_NET_RAW`, or `CAP_SYS_ADMIN` in effective,
  permitted, inheritable, bounding, or ambient capability sets;
- no ambient capabilities at all;
- `noNewPrivileges` enabled;
- no `/dev/net/tun`, tun/tap device, or equivalent host device mount;
- no writable host network control surfaces mounted into the sandbox.

Post-launch adversarial probes must fail when the sandbox attempts to add or
move addresses, change routes, create raw sockets, create tun/tap devices, bind
the host gateway IP, bind a non-sandbox source IP, or spoof headers/source
claims. A failed probe must leave model endpoints denied unless the observed
peer IP still exactly matches the verified contract/resource sandbox IP and all
other authorization checks pass.

## Required Checks

A model request is authorized only when one store lookup and payload validation
prove all of the following:

- trusted TCP peer source IP exactly equals `runtime_resource_instances.sandbox_ip`;
- the contract payload exact sandbox IP equals that same address;
- `runsc_network = sandbox`;
- the resource instance state is `live`, which is reachable only after
  post-start runsc proof and bridge startup readiness proof;
- generation and session state are live and current;
- the turn lease is current and owned by the sandbox runner;
- the authorizing turn is `running`;
- exactly one turn is `leased` or `running` for the generation, as a
  single-in-flight invariant only;
- a committed active model request context exists for the same generation,
  session, turn, runtime profile, model entitlement, and sandbox source IP;
- `runtime_generations.sandbox_contract_version = sandbox-isolation-v1`;
- `sandbox_contracts.sandbox_contract_version = sandbox-isolation-v1`;
- the loaded payload digest equals `sandbox_contract_digest`;
- durable and active-context runtime profile IDs match the generation;
- durable profile entitlement, active-context entitlement, and contract
  entitlement are all true;
- credential policy is `provider_credentials = host-only` and
  `proxy_token = absent`;
- mirror rows match the verified payload.

CIDR membership is not sufficient. A per-generation `/30` also contains the host
gateway address; authorization requires exact sandbox IP equality.

Leased-only turns are not authorized. Active contexts are created only after
`claim_next_turn` plus `ack_turn_started` commits the turn to `running`; they are
cleared on finish, cancellation, entitlement changes, generation retirement, and
cleanup. If the runtime cannot prove unique trusted sandbox identity, a running
turn with committed active context, and matching driver entitlement, sandbox
model endpoints are disabled instead of falling back to a token path.

## Entitlement Changes

`model_access_allowed` is part of the runtime profile identity and the immutable
contract. Granting or revoking model access is an allocation fence:

- stop new turns for affected sessions;
- clear active model request contexts;
- retire live generations that carry the old entitlement;
- reconcile generation-owned resources;
- allocate fresh contracts under the new entitlement.

Phase 8 does not silently mutate existing contracts or implement per-process
model revocation. Emergency global model disablement is a host/proxy stop switch
plus generation retirement.

## Internal Correlation Transport

Proxy request start/finish correlation APIs are reachable only through:

```text
<run_dir>/proxy-internal/proxy-correlation.sock
```

Required properties:

- the proxy-internal root is canonical, symlink-safe, host-only, and disjoint
  from every sandbox content bind source and provider credential root;
- parent directory root-owned `0750`, group-owned by the proxy service group;
- socket root-owned, group-owned by the proxy service group, mode `0660`;
- proxy runs as the configured proxy service UID/GID;
- orchestrator verifies `SO_PEERCRED` or equivalent before reading the body;
- public HTTP and unauthenticated loopback do not serve internal correlation
  APIs;
- stale socket handling is explicit and release-gated.

## Startup and Probes

Pre-turn sandbox startup may call:

- bridge `hello`;
- bridge network probes;
- proxy `/healthz`.

Pre-turn sandbox startup must not call `/v1/messages` or any upstream-dispatching
model endpoint.

Sandbox-visible proxy calls use the allow-listed
`sandbox_model_proxy_base_url`, whose value is a stable alias such as
`http://harness-model-proxy.internal:8082`. The manifest must not render
`http://<host_gateway_ip>:<port>` or any other per-generation network identity
as the model base URL. RuntimeAdapter/network setup must resolve the alias to
the local proxy for the generation, but the alias is ignored by authorization
and release gates prove it reaches only the local proxy surface.

Sandbox egress policy treats the alias as the supported model API surface, but
the alias is not a hard security boundary for plain HTTP. IP egress rules cannot
distinguish a request made to the stable alias from a request made to the same
local proxy IP with a spoofed alias `Host` header. Enforcement is layered:

- nft/egress policy allows only the local proxy IP and port needed for the
  generation and denies provider upstreams, loopback, host-local addresses
  other than that proxy endpoint, and proxy-internal sockets;
- the local proxy rejects sandbox-originated HTTP requests whose `Host` header
  is not the configured stable alias host and port;
- release gates prove direct `http://<host_gateway_ip>:<port>` with its default
  literal `Host` header, wrong `Host`, and non-alias names are rejected before
  upstream dispatch;
- a request to the proxy IP with a manually spoofed alias `Host` header is not
  distinguishable from an alias request at the HTTP proxy layer. It must not be
  used as evidence for alias enforcement, and it is authorized only if the exact
  source IP, active context, entitlement, and verified contract checks pass.

The alias and `Host` value are routing/surface constraints only. They are not
authorization inputs; model authorization still comes from the observed TCP peer
source IP, committed active turn context, entitlement, and verified contract.

Model reachability checks run host-side or after `claim_next_turn` plus
`ack_turn_started`, when the turn is `running` and an active proxy context has
committed. Host-side self-checks must not put provider credentials or
compatibility keys in process arguments.

Claude CLI compatibility must be pinned as either:

- no API key; or
- a fixed non-secret dummy key ignored by the proxy authorizer.

The selected mode must be proven against the pinned Claude Code CLI and
re-pinned proxy checkout.

## Phase 7 Gate Replacement

Phase 8 deliberately retires the Phase 7 proxy probe that authenticated
`POST /v1/messages` with the configured key and malformed JSON. Pre-turn
sandbox startup must not exercise an upstream-dispatching model endpoint, and
Phase 8 has no sandbox-readable provider key. The replacement pinned proxy
contract is:

- `GET /healthz` through `sandbox_model_proxy_base_url` returns `200`;
- pre-turn `POST /v1/messages` from the sandbox rejects without an active
  context, regardless of the no-key or dummy-key CLI compatibility mode;
- `POST /v1/messages` with malformed JSON is tested only after
  `claim_next_turn` plus `ack_turn_started` commits a running turn and an
  active context exists;
- wrong `Host`, gateway-literal URL with default literal `Host`, non-alias
  names, spoofed source headers, leased-only turn, stale active context, wrong
  entitlement, and wrong source IP all deny before any upstream dispatch. A
  gateway-literal connection with a manually spoofed alias `Host` is not
  distinguishable from an alias connection at this layer and must still pass the
  normal source-IP, active-context, entitlement, and contract authorization
  checks.

The Phase 7 secret permission lab is also retired for Phase 8 because
`harness.secrets.*` and `/harness-secrets` are removed. Its replacement evidence
is the host-only provider credential, root disjointness, no sandbox secret
mount, and credential redaction gates in [Release gates](./release-gates.md).

## Proxy Deliverable

Updating and re-pinning `claude-code-proxy` is part of Phase 8. The pinned proxy
must support:

- TCP peer source-IP derivation;
- exact sandbox IP comparison;
- sandbox-facing stable-alias `Host` validation that rejects gateway literals
  when the HTTP `Host` value is literal or otherwise non-alias, without treating
  `Host` as an authorization input;
- UDS internal correlation;
- entitlement-aware model endpoint rejection;
- pre-turn model endpoint denial;
- header/source spoof rejection;
- credential redaction.

Release evidence records the proxy commit.
