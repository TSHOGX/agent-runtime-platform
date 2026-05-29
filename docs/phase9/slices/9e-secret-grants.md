# 9e: Strict Secret Grants

> Parent: [Phase 9 implementation slices](../implementation-slices.md).

Goal: make `secret_grants[]` semantically strict through registries without
changing the 9a credential-policy digest bytes or enabling new exposure modes.

Deliverables:

1. Enforce the field rules in [secret-grants.md](../secret-grants.md) through
   driver and runtime-provider registries.
2. Re-enforce the 9a membership rule through registries for
   model-access-enabled contracts: the selected `driver.driver_id` and
   `runtime_provider.provider_id` must be present in the active model-provider
   grant allowlists. Shell/no-model contracts still carry no model grant and
   have no authorized model-proxy path.
3. Keep validating `credential_policy.digest` against the 9a
   `credential_policy_digest_v1` bytes. 9a already rejects missing or
   mismatched digest values before writing v2 rows; 9e must not change those
   bytes or defer mismatch detection to registry rollout.
4. Keep non-model domains reserved only; Phase 9 still rejects them.

Gates:

- The gates in [secret-grants.md](../secret-grants.md) pass.
- Registry validation does not reinterpret already-hashed v2 bytes.
- Phase 10d must introduce a broker/gateway contract before credential-bearing
  MCP, Git, package registry, or webhook paths can pass.
