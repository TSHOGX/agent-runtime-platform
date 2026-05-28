# Pre-Phase 9 Runtime Cleanup

> Date: 2026-05-28
> Status: pre-Phase 9 gate

This cleanup closes the local publish/runtime isolation blockers before Phase 9
Agent Driver and Pi integration work continues. It keeps compatibility names in place and
moves the model proxy listener and sandbox alias into the primary
`harness:` schema.

## Decisions

- Product-facing language uses Agent Runtime Platform, control plane, sandbox
  runtime, workbench, and orchestrator.
- Internal compatibility names stay in place for now: `harness:`, `HARNESS_*`,
  `/var/lib/harness*`, `harness-bridge-client`, and `harness.*` event names.
- The model proxy is configured under `harness.model_proxy`.
- Legacy-only `claude.proxy_bind_url` and `claude.sandbox_base_url` still load
  and map to `harness.model_proxy`.
- Mixed `harness:` and legacy top-level `runtime:` / `claude:` config remains
  invalid.
- No new temporary env interface is added for model proxy port selection.

## Model Proxy Config

```yaml
harness:
  model_proxy:
    bind_url: http://0.0.0.0:8082
    sandbox_base_url: http://harness-model-proxy.internal:8082
```

`bind_url` is the host listener URL. It must be an `http` URL with an explicit
valid port. The orchestrator parses that port and uses it for generated
network profiles, pre-start proxy probes, and the sandbox egress allow-list.

`sandbox_base_url` is the sandbox-visible model proxy alias written into the
control manifest when model access is enabled.

## Local Runtime Split

| Environment | Frontend | Orchestrator | Model proxy | Sandbox CIDR | Run dir |
| --- | ---: | ---: | ---: | --- | --- |
| `main/` | `8000` | `8090` | `8082` | `10.200.0.0/16` | `/var/lib/harness/run` |
| `publish/` | `9000` | `9090` | `8083` | `10.201.0.0/16` | `/var/lib/harness-publish/run` |

Publish also uses isolated DB, session, agent-home, checkpoint, and runsc
roots under `/var/lib/harness-publish/...`.

## Publish Startup Contract

Publish configuration should set:

```yaml
harness:
  run_dir: /var/lib/harness-publish/run
  network:
    cidr_pool: 10.201.0.0/16
  model_proxy:
    bind_url: http://0.0.0.0:8083
    sandbox_base_url: http://harness-model-proxy.internal:8083
```

Related environment/path differences remain publish-local:

```text
Frontend: 9000
Orchestrator: 9090
DB: /var/lib/harness-publish/state/orchestrator.db
Sessions: /var/lib/harness-publish/sessions
Agent homes: /var/lib/harness-publish/agent-homes
Checkpoints: /var/lib/harness-publish/checkpoints
runsc root: /var/lib/harness-publish/runsc
Correlation socket: /var/lib/harness-publish/run/proxy-internal/proxy-correlation.sock
```

## Publish Smoke Requirements

A publish smoke should prove:

- sandbox allocation uses `10.201.*`;
- the pre-start probe targets `:8083`;
- egress allow-list includes the model proxy at `:8083`;
- the control manifest uses `http://harness-model-proxy.internal:8083`;
- proxy correlation uses the publish socket path;
- after a Claude response returns, the session reaches `running_idle`.

## Naming Boundary

This gate intentionally avoids a full internal rename. Public copy and frontend
metadata should not present the product as "Harness". Existing config keys,
environment variables, event names, binary names, paths, and historical phase
docs remain compatibility interfaces until a dedicated migration window.
