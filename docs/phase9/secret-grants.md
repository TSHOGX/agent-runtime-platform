# Secret Grants

Phase 8 intentionally keeps provider credentials on the host side and exposes
only a non-secret model proxy alias to sandboxes. Phase 9 keeps that boundary
and adds a structured grant model so future Git, package registry, MCP, and
webhook credentials cannot be introduced as one-off exceptions.

## Phase Split

9a adds the schema slot:

```json
{
  "credential_policy": {
    "provider_credentials": "host-only",
    "sandbox_secret_mount": "absent",
    "proxy_token": "absent",
    "secret_grants": [
      {
        "grant_id": null,
        "domain": "model_provider",
        "scope": "anthropic_messages",
        "exposure_mode": "proxy_only",
        "ttl_seconds": null,
        "allowed_drivers": [],
        "allowed_runtime_providers": []
      }
    ]
  }
}
```

9a validation allows only:

```text
domain = model_provider
exposure_mode = proxy_only
```

9f makes the fields semantically strict but still does not enable new exposure
modes.

## Fields

| Field | 9a | 9f |
| --- | --- | --- |
| `grant_id` | May be `null` | Required; included in digest |
| `domain` | Must be `model_provider` | Legal schema values may include `model_provider`, `git`, `package_registry`, `mcp_remote`, `webhook`; non-model domains still require `proxy_only` |
| `scope` | String, minimally validated | Normalized and validated per domain |
| `exposure_mode` | Must be `proxy_only` | Still must be `proxy_only` in Phase 9 |
| `ttl_seconds` | May be `null` | Optional, bounded when present |
| `allowed_drivers` | May be empty | Required and validated against the driver registry |
| `allowed_runtime_providers` | May be empty | Required and validated against the provider registry |

## Exposure Modes

Allowed in Phase 9:

- `proxy_only`: the sandbox can reach a host-authorized proxy path, but no
  credential is mounted, written, passed in env, or passed in argv.

Reserved for later phases:

- `gateway_url`: a broker URL that returns scoped access after a separate
  broker contract exists.
- `brokered_token_env`: short-lived token injected into env.
- `brokered_token_file`: short-lived token written to a file.

Forbidden:

- Any `os_visible_*` exposure mode.
- Any direct provider secret mount under `/harness-secrets`.
- Any long-lived provider credential in `/agent-home`, `/workspace`,
  `/harness-control`, bridge queues, logs, artifacts, env, argv, or process
  listings.

## Digest Rules

By 9f, the credential-policy digest must cover:

- grant order after canonical sorting
- `grant_id`
- `domain`
- normalized `scope`
- `exposure_mode`
- normalized `ttl_seconds`
- sorted `allowed_drivers`
- sorted `allowed_runtime_providers`

Any effective grant change must change the digest.

## Relationship to Model Proxy

The current model access path is still authorized by host/proxy facts:

- active turn context
- sandbox source IP
- live runtime resource
- driver entitlement
- verified sandbox contract
- model profile access policy

`secret_grants[]` does not replace those checks. It records the credential
policy that made the proxy path legal for the selected driver/provider pair.

## Phase 10d Dependency

Credential-bearing MCP, Git, package registry, and webhook access require a
separate broker/gateway design. Phase 10d may introduce `gateway_url` only when
it also defines:

- broker authentication
- broker authorization against active session/generation/turn
- TTL and revocation behavior
- audit events
- sandbox-visible URL format
- failure behavior during restore or reconnect

Until that contract exists, new domains can be reserved in schema but cannot
deliver credentials into the sandbox.
