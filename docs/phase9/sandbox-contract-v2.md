# Sandbox Contract Schema v2

Phase 9 keeps the Phase 8 outer contract label:

```json
"sandbox_contract_version": "sandbox-isolation-v1"
```

The JSON payload schema changes from `contract_schema_version: 1` to
`contract_schema_version: 2`. New writes should use v2 only. Historical v1 data
migration and backup plans are not part of Phase 9.

## Target Shape

```json
{
  "sandbox_contract_version": "sandbox-isolation-v1",
  "contract_schema_version": 2,
  "contract_id": "contract_gen_123",
  "session_id": "sess_123",
  "generation_id": "gen_123",
  "driver": {
    "driver_id": "claude_code",
    "driver_version": "pinned-or-discovered-version",
    "bridge_protocol": "claude_stream_json_per_turn",
    "output_schema": "claude_stream_json_v1",
    "command_argv_digest": "sha256:...",
    "driver_config_digest": "sha256:...",
    "required_runtime_capabilities_digest": "sha256:...",
    "supports_interrupt": true,
    "supports_compaction": true
  },
  "runtime_provider": {
    "provider_id": "local_runsc",
    "provider_profile_id": "local_runsc_default",
    "isolation_kind": "gvisor",
    "template_ref": "default",
    "template_digest": "sha256:...",
    "capability_vocab_version": "1",
    "capability_digest": "sha256:...",
    "provider_specific": {
      "runsc_platform": "systrap",
      "runsc_version": "runsc release-...",
      "runsc_binary_digest": "sha256:..."
    }
  },
  "identity": {
    "sandbox_uid": 65534,
    "sandbox_gid": 65534,
    "sandbox_supplemental_gids": [],
    "model_access_allowed": true
  },
  "mount_plan": {
    "workspace": {
      "source": "<host path>",
      "destination": "/workspace",
      "mode": "rw"
    },
    "agent_home": {
      "source": "<host path>",
      "destination": "/agent-home",
      "mode": "rw"
    },
    "control": {
      "source": "<host path>",
      "destination": "/harness-control",
      "mode": "ro"
    },
    "bridge": {
      "source": "<host path>",
      "destination": "/harness-control/bridge",
      "mode": "rw"
    }
  },
  "network_identity": {
    "runsc_network": "harness",
    "sandbox_ip": "10.200.1.2",
    "sandbox_ip_cidr": "10.200.1.2/30",
    "host_gateway_ip": "10.200.1.1",
    "netns_name": "harness-...",
    "netns_path": "<host path>",
    "host_veth": "veth...",
    "sandbox_veth": "eth0",
    "host_side_cidr": "10.200.1.1/30",
    "nft_table_name": "harness_...",
    "egress_policy_id": "egress_..."
  },
  "snapshot_policy": {
    "provider_supports_snapshot_disk": true,
    "provider_supports_snapshot_memory": false,
    "provider_supports_branch": false,
    "branch_count_limit": 0,
    "must_quiesce_processes": true,
    "stream_disconnects_on_snapshot": true,
    "snapshot_semantic": "generation_checkpoint_restore"
  },
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
  },
  "model_access": {
    "model_access_allowed": true,
    "active_turn_required": true,
    "provider_protocol": "anthropic_messages",
    "sandbox_model_proxy_base_url": "http://harness-model-proxy.internal:8082"
  },
  "driver_runtime": {
    "driver_home_mount": "/agent-home",
    "generated_driver_config_mount": "/harness-control/driver/claude_code",
    "driver_state_digest": "sha256:..."
  },
  "resource_identity": {
    "resource_identity_digest": "sha256:..."
  },
  "input_digests": {
    "bundle_digest": "sha256:...",
    "runtime_config_digest": "sha256:...",
    "oci_spec_digest": "sha256:...",
    "control_manifest_digest": "sha256:..."
  }
}
```

## Driver Object

The `driver` object replaces the v1 string `driver` field. It is the immutable
driver identity for the generation, not a UI label.

Required Phase 9a fields:

- `driver_id`
- `driver_version`
- `bridge_protocol`
- `output_schema`
- `command_argv_digest`
- `driver_config_digest`
- `required_runtime_capabilities_digest`
- `supports_interrupt`
- `supports_compaction`

`claude` remains a legacy alias for current public and persisted surfaces, but
new v2 contracts should record the canonical driver spec ID, `claude_code`.

## Runtime Provider Object

`runtime_provider` replaces top-level `runtime_adapter`. Runsc facts move under
`runtime_provider.provider_specific` so future providers can keep their own
provider-specific facts without polluting the contract root.

The current Linux capability deny list belongs to OCI/runtime construction. It
is not the Phase 9 product capability contract. Product capabilities and their
digest come from [runtime-capabilities.md](./runtime-capabilities.md).

## Snapshot Policy

`snapshot_policy` distinguishes current checkpoint/restore behavior from
future fanout:

- `generation_checkpoint_restore`: current local runsc behavior.
- `base_branch_fanout`: future base-to-N child sandbox behavior.
- `pause_resume_only`: no checkpoint/branch guarantee.

Phase 9 must not infer fanout readiness from `snapshot_disk: true`. Fanout
requires `provider_supports_branch: true` and
`snapshot_semantic: base_branch_fanout`.

## Credential Policy

Top-level fields keep the sandbox-visible posture:

- `provider_credentials: host-only`
- `sandbox_secret_mount: absent`
- `proxy_token: absent`

`secret_grants[]` enumerates allowed secret domains and exposure mode. During
Phase 9, only `domain: model_provider` and `exposure_mode: proxy_only` may pass
validation. See [secret-grants.md](./secret-grants.md).

## Control Manifest Projection

The sandbox-visible control manifest may contain:

- `driver_id`
- `bridge_protocol`
- generated driver config paths and digests
- model proxy alias when model access is allowed
- Phase 10 adapter metadata after Phase 10 lands

It must not expose:

- host roots
- host gateway internals beyond sandbox-required network projection
- netns paths when not required by the sandbox
- veth names when not required by the sandbox
- DB paths
- bundle/spec/checkpoint host paths
- proxy-internal paths
- provider credential paths

During 9a, existing top-level manifest fields such as `agent`,
`claude_session_uuid`, `resume_claude`, and
`claude_code_disable_nonessential_traffic` may remain as compatibility mirrors
while the bridge and runner code still consume them. New code should read the
driver object and generated driver config projection.

## Driver Config Projection

`/harness-control/driver/<driver_id>/` is generated inside the existing
read-only `/harness-control` projection. It is not a new bind mount and does
not change the Phase 8 MountPlan allow-list.

Examples:

```text
/harness-control/driver/claude_code/settings.json
/harness-control/driver/pi/models.json
/harness-control/driver/pi/settings.json
```

Any driver that writes config, sessions, caches, sockets, or package data must
declare exact writable paths under `/agent-home` or explicit scratch mounts. No
driver may rely on writable rootfs paths.

## Validation Gates

Contract v2 validation should reject:

- payloads containing `sandbox_contract_digest`
- wrong `contract_id`, `session_id`, or `generation_id`
- `contract_schema_version` other than `2` for new writes
- string `driver` payloads
- missing or unknown `driver.driver_id`
- missing `runtime_provider.provider_id`
- missing `capability_vocab_version`
- missing capability/config/template digests
- `credential_policy.secret_grants[]` with non-`proxy_only` exposure mode
- host-only fields in sandbox-visible projection
- `snapshot_semantic` values outside the allowed enum
