# Runtime Provider Object

> Parent: [Sandbox contract schema v2](../sandbox-contract-v2.md).

`runtime_provider` replaces top-level `runtime_adapter`. Runsc facts move under
`runtime_provider.provider_specific` so future providers can keep their own
provider-specific facts without polluting the contract root.

For `local_runsc`, v2 contracts must keep the Phase 8 immutable runsc identity
facts in the contract payload:

- `runsc_container_id`
- `runsc_platform`
- `runsc_version`
- `runsc_binary_path`
- `runsc_binary_digest`
- `runsc_overlay2`
- `no_new_privileges`
- `ambient_capabilities`
- `required_annotations`, including the pinned gVisor bridge bind annotation

`network_identity.runsc_network` is the canonical runsc network mode and must
be `sandbox` for `local_runsc`. The model proxy authorization path still depends
on exact sandbox peer IP proof and `runsc_network = sandbox`; `harness` is not a
valid v2 value.

`runtime_resource_instances.resource_identity_payload` remains the authoritative
resource identity proof while a row exists. The v2 contract carries
`resource_identity.resource_identity_digest`, and launch/restore/cleanup
validation must prove the persisted resource identity payload hashes to that
digest and matches the contract's runsc, network, and generation-owned path
facts. During the 9a destructive cutover, cleanup may use the verified resource
identity payload for automatic delete decisions, especially when the contract
payload is corrupt. Discoverable live resources must be cleaned, proven absent,
or durably quarantined before their ownership rows are removed, but old rows
and old `sessions`, `messages`, `artifacts`, `turns`, and `events` rows may
still be deleted directly after that safety gate as defined in
[sandbox-contract-v2.md](../sandbox-contract-v2.md).

The current Linux capability deny list belongs to OCI/runtime construction. It
is not the Phase 9 product capability contract. Product capabilities and their
digest come from [runtime-capabilities.md](../runtime-capabilities.md).

`runtime_provider.template_digest` is required for new v2 contracts from 9a.
It is a runtime-provider template allocation fence, not the full rootfs content
fence. In 9a/9b, `input_digests.rootfs_image_digest` may still be null, so the
template digest must have its own versioned preimage and fixtures.

For `local_runsc`, the required algorithm is `runtime_template_digest_v1`:

1. Build a canonical JSON object with sorted keys and no insignificant
   whitespace.
2. Include `provider_id`, `provider_profile_id`, `isolation_kind`,
   `template_ref`, `template_schema_version`, normalized `rootfs_path`
   identity, `runsc_network`, `runsc_overlay2`, `runsc_platform`,
   `runsc_version`, `runsc_binary_path`, `runsc_binary_digest`,
   `no_new_privileges`,
   `ambient_capabilities`, and the required gVisor mount annotation set.
3. Do not include per-generation rendered artifact digests such as OCI spec,
   control manifest, bundle, checkpoint, or network hosts digests.
4. Prefix the canonical bytes with `runtime_template_digest_v1\n` and compute
   `sha256:<hex>`.

The allocator/runtime provider computes the preimage from
`harness.runtime_providers.local_runsc`, the selected template ref, normalized
runtime defaults, and the verified runsc pin before contract construction. 9a
fixtures must cover the canonical preimage and expected hash. When 9b promotes
these facts into `RuntimeProviderSpec(local_runsc)`, it must preserve the digest
bytes for identical template facts.
