# Contract Target Shape

> Parent: [Sandbox contract schema v2](../sandbox-contract-v2.md).

This example shows the 9a/9b validation profile. 9c+ writes keep
`contract_schema_version: 2`, set `contract_gate_version: "phase9c"`, and fill
the runtime/image digest fields required by the 9c gates.

```json
{
  "sandbox_contract_version": "sandbox-isolation-v1",
  "contract_schema_version": 2,
  "contract_gate_version": "phase9a",
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
      "runsc_container_id": "harness-gen-gen_123",
      "runsc_platform": "systrap",
      "runsc_version": "runsc release-...",
      "runsc_binary_path": "<canonical host path to runsc>",
      "runsc_binary_digest": "sha256:...",
      "runsc_overlay2": "none",
      "no_new_privileges": true,
      "ambient_capabilities": [],
      "required_annotations": {
        "/harness-control/bridge": {
          "dev.gvisor.spec.mount./harness-control/bridge.type": "bind",
          "dev.gvisor.spec.mount./harness-control/bridge.share": "exclusive"
        }
      }
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
    },
    "bridge_inbox": {
      "source": "<host-owned bridge inbox path>",
      "destination": "/harness-control/bridge/inbox",
      "mode": "ro"
    },
    "bridge_host_tmp": {
      "source": "<host-owned bridge host-tmp path>",
      "destination": "/harness-control/bridge/host-tmp",
      "mode": "ro"
    },
    "network_hosts": {
      "source": "<host path, when alias projection is enabled>",
      "destination": "/etc/hosts",
      "mode": "ro"
    },
    "driver_config_materializations": {}
  },
  "data_volumes": {
    "workspace": {
      "table": "session_workspaces",
      "session_id": "sess_123",
      "host_path": "<host path>",
      "layout_version": 1,
      "runtime_identity_digest": "sha256:...",
      "provisioning_marker_path": "<host-only evidence path>",
      "provisioning_marker_digest": "sha256:...",
      "sandbox_destination": "/workspace",
      "provisioning_evidence_root": "<host-only evidence root>"
    },
    "agent_home": {
      "table": "session_driver_homes",
      "session_id": "sess_123",
      "driver_home_key": "claude_code",
      "host_path": "<host path>",
      "layout_version": 1,
      "runtime_identity_digest": "sha256:...",
      "provisioning_marker_path": "<host-only evidence path>",
      "provisioning_marker_digest": "sha256:...",
      "sandbox_destination": "/agent-home",
      "provisioning_evidence_root": "<host-only evidence root>"
    }
  },
  "network_identity": {
    "runsc_network": "sandbox",
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
    "materialized_driver_config": {},
    "initial_driver_state_digest": "sha256:..."
  },
  "resource_identity": {
    "resource_identity_digest": "sha256:..."
  },
  "input_digests": {
    "runtime_config_digest": null,
    "rootfs_image_digest": null,
    "agent_manifest_digest": null
  }
}
```

Drivers whose native config loader cannot read directly from
`/harness-control/driver/<driver_id>/` must make every extra materialization
explicit in both `mount_plan.driver_config_materializations` and
`driver_runtime.materialized_driver_config`. Pi uses this path because its
loader reads from `PI_CODING_AGENT_DIR=/agent-home/.pi/agent`:

```json
{
  "mount_plan": {
    "driver_config_materializations": {
      "pi.models": {
        "logical_name": "pi.models",
        "source": "<host path for generated /harness-control/driver/pi/models.json>",
        "destination": "/agent-home/.pi/agent/models.json",
        "mode": "ro",
        "materialization_kind": "file_bind"
      },
      "pi.settings": {
        "logical_name": "pi.settings",
        "source": "<host path for generated /harness-control/driver/pi/settings.json>",
        "destination": "/agent-home/.pi/agent/settings.json",
        "mode": "ro",
        "materialization_kind": "file_bind"
      }
    }
  },
  "driver_runtime": {
    "generated_driver_config_mount": "/harness-control/driver/pi",
    "materialized_driver_config": {
      "pi.models": {
        "logical_name": "pi.models",
        "source_projection": "/harness-control/driver/pi/models.json",
        "destination": "/agent-home/.pi/agent/models.json",
        "source_digest": "sha256:...",
        "destination_mutable_by_sandbox": false
      },
      "pi.settings": {
        "logical_name": "pi.settings",
        "source_projection": "/harness-control/driver/pi/settings.json",
        "destination": "/agent-home/.pi/agent/settings.json",
        "source_digest": "sha256:...",
        "destination_mutable_by_sandbox": false
      }
    }
  }
}
```

These entries are host-side contract evidence. The sandbox-visible projection
may expose the sandbox paths and digests needed by the runner, but it must not
expose host source paths.

`bridge_inbox` and `bridge_host_tmp` are explicit v2 mount-plan entries, not
implicit subdirectories of the writable bridge mount. They preserve the Phase 8
directional bridge ACL: host-published inbox and host temp paths are read-only
inside the sandbox, while sandbox output remains under the writable bridge
surface.

`network_hosts` is present when the generation requires the model-proxy alias
projection and absent otherwise. Its source must match the persisted
generation-owned network hosts path, and the rendered file's digest remains
sidecar artifact evidence (`network_hosts_digest`) rather than an
`input_digests` field.

Phase 9 v2 intentionally omits `/schema-pack` from `mount_plan`. The current
runtime's repo-root `schema-pack` auto-mount is a pre-v2 compatibility path and
must be disabled or gated off for v2 launches before `contract_gate_version:
"phase9a"` writes are accepted. A v2 contract must not expose a read-only
content mount that lacks a contract field, digest preimage, and validation gate.
Native driver schema packs remain Phase 10 adapter evidence unless a later Phase
9 update defines those rules explicitly.
