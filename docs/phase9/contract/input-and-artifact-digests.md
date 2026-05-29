# Input and Artifact Digests

> Parent: [Sandbox contract schema v2](../sandbox-contract-v2.md).

For contracts with `contract_gate_version: "phase9a"` (9a/9b), the pre-9c
allocation evidence is deliberately incomplete: `runtime_config_digest`,
`agent_manifest_digest`, and `rootfs_image_digest` must all be `null`. A
`phase9a` row is therefore never image-manifest-backed, even if the host could
discover an image manifest later. Starting with `contract_gate_version:
"phase9c"` at the 9c generic-config gate, `runtime_config_digest` is required
and uses `runtime_config_digest_v1`:

The shared loader must key this rule from the persisted `contract_gate_version`.
The schema major remains `contract_schema_version: 2`, so current release
version, wall-clock time, or nullable digest presence is not a durable
discriminator.

The digest is allocation-time evidence, not a live comparison to current
deployment config. New allocations compute it from the source config that
selected the driver/runtime and persist both:

- the digest in the v2 contract; and
- the canonical preimage or a typed config-generation artifact in host-only
  allocation evidence.

The stored evidence should be a typed `sandbox_contract_artifacts` row,
first-class store column, or equivalent audited record; validators must not
recover it from rendered control files or ad hoc payload parsing.

Later contract reads, proxy authorization, restore planning, and physical
checkpoint restore recompute only from that persisted allocation-time evidence.
They must not recompute from current `harness.*` config, because operator config
changes affect new allocations only. An older generation remains restorable when
its stored config/image evidence and checkpoint artifacts are available and pass
their own gates; current defaults changing does not by itself invalidate it.

1. Build a canonical JSON object with sorted keys and no insignificant
   whitespace:

   ```json
   {
     "schema_version": 1,
     "product_mode": "<agent-or-shell>",
     "selected_driver_id": "<canonical-driver-id>",
     "selected_agent_config_id": "<harness.agents id or null>",
     "selected_model_profile_id": "<harness.model_profiles id or null>",
     "selected_runtime_provider_id": "<harness.runtime_providers id>",
     "deployment_defaults": {
       "default_agent": "<canonical-driver-id>"
     },
     "agent_config": {},
     "model_profile": {},
     "runtime_provider_config": {}
   }
   ```

2. For new allocations, populate the object from the current source deployment
   config in
   `harness.agents`, `harness.model_profiles`, and
   `harness.runtime_providers`, plus the product-mode selection that chose the
   driver. Product mode selection follows the 9c table: `shell` selects
   `driver_id: "sh"` and `agent` selects `harness.default_agent`, which must be
   an enabled non-shell Agent-capable driver. IDs and enum values are canonical
   strings. Fields that are not applicable to shell are explicit `null` where
   the schema calls for an ID.
3. Include operator-selected source inputs that can change runtime behavior,
   such as selected driver config, selected model profile, runtime provider
   profile/config, and deployment default selection.
   `deployment_defaults.default_agent` is intentionally present even when
   `product_mode: "shell"` selects `driver_id: "sh"`. The digest is a source
   deployment selection snapshot for the allocation, not only the minimal set
   of bytes needed by the selected process. Changing the deployment's Agent
   default therefore changes new shell allocation digest bytes even though shell
   runtime behavior is unchanged. Existing shell generations continue to
   validate against their persisted allocation-time preimage.
4. Exclude rendered `/harness-control` files, OCI specs, bundle paths, host
   paths, DataVolume/resource identity evidence, runsc-generated IDs, proxy
   tokens, provider credentials, mutable driver state, and any artifact digest
   produced after contract construction.
5. Prefix the canonical bytes with `runtime_config_digest_v1\n` and compute
   `sha256:<hex>`.

9c fixtures must cover the canonical source-config preimage for Claude Code,
shell, and the default-agent selection path. 9f adds Pi fixture coverage before
Pi is selectable.

`input_digests.rootfs_image_digest` is the full rootfs/template content fence.
It must not be redefined as the digest of `/etc/harness-image/agents.json`.
When present, it is checked against the rootfs/template evidence captured for
that allocation or checkpoint, not against whatever image is current when an
older generation is later read.

`input_digests.agent_manifest_digest` is the digest of
`/etc/harness-image/agents.json` or the host-visible equivalent used by the
allocator. It records selectable drivers, their pinned execution facts,
`bridge_protocol_version`, and `turn_input_schema` supported by the installed
sandbox client. Allocation persists the manifest evidence that produced the
digest; later validation compares to that stored evidence, while new allocations
compare the selected driver against the current image manifest.

Phase 9 v2 does not define `input_digests.schema_pack_digest`. Native driver
schema packs are Phase 10 adapter evidence unless a later Phase 9 update adds a
specific canonical preimage, nullability rule, mount evidence, and validation
gate. Until then, v2 contracts must omit this field and v2 launches must not
mount the repo-root `schema-pack` directory at `/schema-pack`, even when that
directory exists on the host.

Both image fields are required to be `null` for `contract_gate_version:
"phase9a"` because the image-manifest gate lands in 9c. During those slices,
`runtime_provider.template_digest` is the `runtime_template_digest_v1`
allocation fence for the selected provider template, and the absence of image
digests means only that the selected image contents and manifest have not yet
been verified through the Phase 9 gate.

Starting with the 9c image-manifest gate, new `phase9c` v2 contracts must set
`runtime_config_digest`, must set `agent_manifest_digest`, and must set
`rootfs_image_digest` when the selected runtime provider exposes a trustworthy
full rootfs/template digest. Image manifest content requirements and rollout
gates live in [9c: Generic Config and Frontend Product Surface](../slices/9c-generic-config-frontend.md).

## Rendered Artifact Digests

Contract v2 keeps the Phase 8 digest direction:

```text
config/content/runtime inputs
  -> canonical SandboxContract payload
  -> sandbox_contract_digest
  -> rendered control manifest / OCI spec / bundle / network hosts digests
```

Rendered artifact digests are sidecar outputs. They must not appear under
`input_digests`.

Sidecar evidence includes:

- `control_manifest_digest`
- `oci_spec_digest`
- `bundle_digest`
- `network_hosts_digest`
- `checkpoint_metadata_digest`, when present
