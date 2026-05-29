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
    "digest": "sha256:...",
    "secret_grants": [
      {
        "grant_id": "model_provider:anthropic_proxy",
        "domain": "model_provider",
        "scope": "anthropic_messages",
        "exposure_mode": "proxy_only",
        "ttl_seconds": null,
        "allowed_drivers": ["claude_code"],
        "allowed_runtime_providers": ["local_runsc"]
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

Grant allowlist validation is scoped to model-access-enabled v2 contracts.
Those contracts must carry a `model_provider` grant; 9a rejects missing or
empty `allowed_drivers` and `allowed_runtime_providers`, and v2 proxy
authorization requires the selected `driver.driver_id` and
`runtime_provider.provider_id` to be present in those allowlists. Claude Code v2
contracts synthesize explicit `claude_code` / `local_runsc` allowlists. Shell
contracts do not carry a model grant and are not required to appear in
model-provider allowlists; they fail closed if they attempt model-proxy
authorization. Persisted v1 contracts are not authorized after the 9a cutover;
delete or fail closed on those rows before proxy authorization.

9a fixes the v2 digest algorithm and normalized field set below. 9e makes the
same model-provider fields semantically strict through registry-backed
validation, but it must not change digest bytes unless Phase 9 intentionally
introduces a new digest version. 9e still does not enable new domains or new
exposure modes.

## Fields

| Field | 9a | 9e |
| --- | --- | --- |
| `grant_id` | Deterministic for model-provider grants; included in digest | Registry-backed validation; same normalized bytes |
| `domain` | Must be `model_provider` | Still must be `model_provider`; `git`, `package_registry`, `mcp_remote`, and `webhook` remain reserved names only |
| `scope` | String, minimally validated | Normalized and validated for the `model_provider` domain |
| `exposure_mode` | Must be `proxy_only` | Still must be `proxy_only` in Phase 9 |
| `ttl_seconds` | May be `null` | Optional, bounded when present |
| `allowed_drivers` | Required for v2 model-provider grants; validated against the 9a vocabulary | Validated against the driver registry |
| `allowed_runtime_providers` | Required for v2 model-provider grants; validated against the 9a runtime-provider vocabulary | Validated against the runtime-provider registry |

Phase 9 validators must reject any grant whose `domain` is not
`model_provider`, even when `exposure_mode` is `proxy_only`. Accepting
non-model grants before the broker/gateway contract exists would weaken the
credential boundary by implying that the proxy path is already defined.

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

`credential_policy.digest` is required in v2 sandbox contracts starting in 9a.
It is computed over the canonical effective credential policy with the
`digest` field omitted.

The 9a v2 validator must recompute this digest and reject mismatches before the
contract row is written. 9e may re-enforce grant membership through registries,
but it is not the first gate that detects a bad `credential_policy.digest`.

The v2 digest algorithm is:

1. Normalize the top-level posture fields and every grant field below.
2. Sort grants by canonical tuple
   `(domain, grant_id, scope, exposure_mode, ttl_seconds, allowed_drivers,
   allowed_runtime_providers)`.
3. Emit canonical JSON with deterministic object keys, no insignificant
   whitespace, and the `digest` field omitted.
4. Prefix the bytes with `credential_policy_digest_v1\n` and compute
   `sha256:<hex>`.

For model-access-enabled contracts, 9a rejects empty v2 model-provider
allowlists and rejects v2 proxy authorization when the selected
`driver.driver_id` or `runtime_provider.provider_id` is absent from the active
grant allowlists, using the hard-coded 9a vocabulary. Shell/no-model contracts
carry no model grant and skip this membership requirement because no proxy
model request is legal for them. This is a semantic authorization check over
already-normalized fields and does not add, remove, or reorder digest-covered
bytes. Before Pi can be enabled, 9e proves the same membership checks against
the driver and runtime-provider registries.

The 9a `credential_policy_digest_v1` field coverage is final for v2 contracts:

- `provider_credentials`
- `sandbox_secret_mount`
- `proxy_token`
- grant order after canonical sorting
- `grant_id`
- `domain`
- normalized `scope`
- `exposure_mode`
- normalized `ttl_seconds`
- sorted `allowed_drivers`
- sorted `allowed_runtime_providers`

Normalization rules:

- `provider_credentials`, `sandbox_secret_mount`, `proxy_token`, `domain`, and
  `exposure_mode` are lower-case enum strings.
- `grant_id` is a deterministic non-empty string for model-provider grants.
- `scope` is the current proxy scope string in 9a and the registry-normalized
  model-provider scope by 9e; those normalized bytes must match for current
  9a scopes.
- `ttl_seconds` is either `null` or a positive integer.
- `allowed_drivers` and `allowed_runtime_providers` are de-duplicated and
  sorted by canonical ID before hashing.

Any effective grant change must change the digest. 9e can reject values that
are not registry-valid, but it cannot silently reinterpret already-hashed v2
bytes.

## Relationship to Model Proxy

The current model access path is still authorized by host/proxy facts:

- active turn context
- sandbox source IP
- live runtime resource
- driver entitlement
- verified sandbox contract
- model profile access policy

`secret_grants[]` does not replace those checks. It records the credential
policy that made the proxy path legal for the selected driver and runtime
provider pair. From the 9a selector gate onward, those checks must use
`sessions.driver_id`, the selected driver spec, model profile, runtime
provider, and grant allowlists; they must not infer entitlement from legacy
strings such as `agent == "claude"`.
Model-provider IDs consumed by driver-native config, such as Pi's generated
`harness_anthropic_proxy`, are bound through the selected agent config or model
profile; they are not values for `allowed_runtime_providers`.

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
pass Phase 9 validation and cannot deliver credentials into the sandbox.
