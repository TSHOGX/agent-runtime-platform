# Validation Gates

> Parent: [Sandbox contract schema v2](../sandbox-contract-v2.md).

Contract v2 payload and operation validation should reject:

- payloads containing `sandbox_contract_digest`
- wrong `contract_id`, `session_id`, or `generation_id`
- `contract_schema_version` other than `2` for new writes
- missing or unknown `contract_gate_version`
- persisted `sandbox_contracts.contract_schema_version` or
  `sandbox_contracts.contract_gate_version` values that do not match the
  payload
- new writes whose `contract_gate_version` is not the current writer gate
  (`phase9a` for 9a/9b, `phase9c` for 9c+)
- persisted v1 rows after cutover; they must be deleted or fail closed before
  restore/proxy authorization
- restore or proxy authorization paths that parse contract payloads directly
  instead of using the shared audited v2 loader
- string `driver` payloads
- missing or unknown `driver.driver_id`
- missing `runtime_provider.provider_id`
- missing `capability_vocab_version`
- missing capability/config/template digests
- missing or mismatched `driver.command_argv_digest` against
  `driver_command_argv_digest_v1` for the selected canonical driver and
  sidecar-backed command-plan selector
- missing or mismatched `driver.driver_config_digest` against
  `driver_config_digest_v1` for the selected canonical driver config input
- missing or mismatched `driver.required_runtime_capabilities_digest` against
  `runtime_capabilities_digest_v1` for the selected canonical driver
- missing local-runsc immutable identity fields in
  `runtime_provider.provider_specific`: `runsc_container_id`,
  `runsc_platform`, `runsc_version`, `runsc_binary_path`,
  `runsc_binary_digest`, `runsc_overlay2`, `no_new_privileges`,
  `ambient_capabilities`, and `required_annotations`
- `network_identity.runsc_network` other than `sandbox` for `local_runsc`
- `runtime_provider.template_digest` that does not match
  `runtime_template_digest_v1` for the selected provider template descriptor
- `runtime_provider.capability_digest` that does not match the selected
  `RuntimeProviderSpec`, or a `snapshot_policy` projection that does not match
  that same provider spec and capability digest
- snapshot policy contradictions, including branch count without branch
  support, `base_branch_fanout` without `branch: true`, or
  `generation_checkpoint_restore` without `snapshot_disk: true`
- `resource_identity.resource_identity_digest` that does not match the
  persisted `runtime_resource_instances.resource_identity_payload`, or resource
  identity payload facts that do not match the v2 contract's runsc/network
  identity
- missing `data_volumes.workspace` or `data_volumes.agent_home` evidence
- DataVolume evidence that does not match the verified DB row, root-owned
  provisioning marker, runtime identity digest, or `mount_plan` source
- missing or mismatched `mount_plan.workspace`, `mount_plan.agent_home`,
  `mount_plan.control`, `mount_plan.bridge`, `mount_plan.bridge_inbox`, or
  `mount_plan.bridge_host_tmp`; each entry must match the allocated
  generation/DataVolume/resource facts, exact destination, bind mode, and
  required mount options, and the bridge mount must carry the pinned gVisor
  exclusive/share annotation
- `mount_plan.bridge_inbox` or `mount_plan.bridge_host_tmp` sources that are
  not the host-owned inbox/temp subpaths for the same generation-owned bridge
  directory, or that are writable inside the sandbox
- missing `mount_plan.network_hosts` when the generation has a persisted
  network hosts projection path, present `mount_plan.network_hosts` when no
  projection was allocated, a source that differs from
  `runtime_resource_instances.network_hosts_path`, a destination other than
  `/etc/hosts`, a non-read-only bind mode, or rendered bytes whose sidecar
  `network_hosts_digest` does not match the generated alias projection
- a driver-native config pathname under `/agent-home` that is required by the
  selected driver but missing from `mount_plan.driver_config_materializations`
  or `driver_runtime.materialized_driver_config`
- Pi contracts whose `/agent-home/.pi/agent/models.json` or
  `/agent-home/.pi/agent/settings.json` materialization is absent, not an
  exact read-only file bind, not backed by the generated
  `/harness-control/driver/pi/` projection source and digest, or mutable by the
  sandbox UID/GID
- Pi launches that rely on symlinks or copied config files in a
  sandbox-writable `/agent-home/.pi/agent` tree instead of declared read-only
  file-bind materialization evidence
- `data_volumes.agent_home.driver_home_key` that was not produced by the
  allocation resolver for the selected driver
- new allocations that would create a canonical driver home beside a legacy
  `session_driver_homes(driver='claude')` row instead of deleting that old row
  during the destructive cutover
- `driver_runtime.initial_driver_state_digest` missing, malformed, or not tied
  to the selected canonical driver for the generation
- allocation attempts that cannot claim the session lease and return a typed
  sidecar start-state token, except for the first-allocation bootstrap path
  that creates the generation row and canonical sidecar in the same allocation
  transaction
- contract writes whose generation no longer owns the session lease, whose
  selected driver differs from the sidecar, or whose current
  `session_driver_states.state_digest` / version no longer matches the
  allocation start-state token and
  `driver_runtime.initial_driver_state_digest`
- allocation/start, reconnect, restore, proxy authorization, or sidecar
  bootstrap attempts for any v1-derived row that survived the 9a destructive
  cutover without being recreated as a valid post-cutover v2 session
- ordinary contract reads, restore-planning reads, or proxy authorization paths
  that require current sidecar equality with
  `driver_runtime.initial_driver_state_digest`; those paths validate contract
  shape and use checkpoint fences when restore state must be current
- `session_driver_states` CAS conflicts, missing rows after the first
  allocation bootstrap window, malformed payloads, non-monotonic
  `state_version`, writes from a generation that does not own the session
  lease, or sidecar updates whose `updated_turn_id` belongs to another session
  or generation
- Pi `driver_state_update` payloads that have not passed host-side
  DataVolume-backed path validation and host recomputation of `state_digest`
- missing or mismatched `credential_policy.digest` against
  `credential_policy_digest_v1`, computed from the canonical effective policy
  with the `digest` field omitted
- `credential_policy.secret_grants[]` with non-`proxy_only` exposure mode
- `credential_policy.secret_grants[]` with a domain other than `model_provider`
- model-provider grants with empty driver/runtime-provider allowlists from the
  9a v2 cutover onward
- `contract_gate_version: "phase9a"` contracts with a non-null
  `input_digests.runtime_config_digest`,
  `input_digests.agent_manifest_digest`, or
  `input_digests.rootfs_image_digest`; pre-9c contracts are explicitly not
  image-manifest-backed
- `contract_gate_version: "phase9c"` contracts with a missing or malformed
  `input_digests.runtime_config_digest`, or one that does not match the
  host-persisted canonical runtime-config preimage/config generation captured
  at allocation time
- validation of an existing `phase9c` contract that recomputes
  `input_digests.runtime_config_digest` from current deployment config instead
  of the persisted allocation-time preimage
- new `phase9c` allocations whose selected driver is absent from the current
  image manifest evidence
- `phase9c` contracts with a missing or mismatched `agent_manifest_digest`
  against the host-persisted allocation-time image manifest evidence, or a
  missing/mismatched `rootfs_image_digest` against persisted allocation-time
  rootfs/template evidence when the selected runtime provider exposes a full
  rootfs/template content digest
- `input_digests.schema_pack_digest` in Phase 9 v2 contracts; the field is not
  defined until a later adapter/schema-pack gate owns its algorithm and
  validation rules
- `mount_plan` entries that bind the repo-root `schema-pack` directory, or any
  other schema-pack content, into `/schema-pack` for a Phase 9 v2 launch
- rendered artifact digests under `input_digests`
- host-only fields in sandbox-visible projection
- `snapshot_semantic` values outside the allowed enum
