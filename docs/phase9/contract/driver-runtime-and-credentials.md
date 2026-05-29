# Driver Runtime and Credentials

> Parent: [Sandbox contract schema v2](../sandbox-contract-v2.md).

## Driver Runtime and State

`driver_runtime.initial_driver_state_digest` is a generation-scoped snapshot
taken at allocation time. It records the driver-private state that the
generation starts from; it is an allocation fence for that generation and does
not change after the contract is written.

This digest is not a promise that the mutable sidecar still has the same value
when the contract is later read. Allocation and contract persistence have two
CAS boundaries: `AllocateGeneration` claims the lease and returns a typed
sidecar start-state token, then `StoreSandboxContract` revalidates that token
against the still-owned lease and current sidecar before persisting the
contract. The documented first-allocation bootstrap path inserts the canonical
sidecar in the allocation transaction after the generation row exists; the
later contract write still revalidates the returned digest/version before it
writes `driver_runtime.initial_driver_state_digest`. Ordinary v2 contract
loading and proxy authorization must not fail only because a later turn
advanced the sidecar. Physical checkpoint restore uses the separate
`checkpoint_driver_states_digest` fence.

Driver state that changes during or after a turn is mutable sidecar evidence,
not an in-place contract update. The durable current state belongs in
`session_driver_states`. CAS writes, digest algorithms, checkpoint fencing, and
transition rules are defined in [driver-state.md](../driver-state.md). Pi-owned
payloads and session-file validation are defined in
[pi-driver.md](../pi-driver.md).

## Credential Policy

Top-level fields keep the sandbox-visible posture:

- `provider_credentials: host-only`
- `sandbox_secret_mount: absent`
- `proxy_token: absent`
- `digest: sha256:...`

`secret_grants[]` enumerates allowed secret domains and exposure mode. During
Phase 9, only `domain: model_provider` and `exposure_mode: proxy_only` may pass
validation. See [secret-grants.md](../secret-grants.md).

`credential_policy.digest` is an allocation fence. The v2 digest algorithm,
field coverage, grant allowlist rules, and reserved future domains are defined
in [secret-grants.md](../secret-grants.md).
