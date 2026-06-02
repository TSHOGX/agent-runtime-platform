package generationplan

import (
	"strings"
	"testing"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/store"
)

func TestValidateAcceptsCurrentShadowPlanShape(t *testing.T) {
	payload := validPlanPayload()
	if err := Validate(ValidateParams{Payload: payload}); err != nil {
		t.Fatalf("validate current shadow plan shape: %v", err)
	}
}

func TestValidateRejectsUnsupportedRequiredFeature(t *testing.T) {
	payload := validPlanPayload()
	featurePolicy := payload["feature_policy"].(map[string]any)
	featurePolicy["interrupt"] = "required"

	err := Validate(ValidateParams{Payload: payload})
	if err == nil || !strings.Contains(err.Error(), "feature interrupt requires driver claude_code support") {
		t.Fatalf("expected unsupported required feature error, got %v", err)
	}
}

func TestValidateRejectsMutableContentSnapshotReference(t *testing.T) {
	payload := validPlanPayload()
	contentSnapshots := payload["content_snapshots"].(map[string]any)
	contentSnapshots["skills"] = map[string]any{
		"kind":                   "skills",
		"digest":                 "sha256:skills",
		"immutable_host_path":    "relative/path",
		"mount_destination":      "/harness-skills",
		"source_evidence_digest": "sha256:source",
		"retention_class":        "active",
	}

	err := Validate(ValidateParams{Payload: payload})
	if err == nil || !strings.Contains(err.Error(), "content_snapshots.skills.immutable_host_path must be absolute") {
		t.Fatalf("expected content snapshot path error, got %v", err)
	}
}

func TestValidateRejectsProjectionDigestShape(t *testing.T) {
	payload := validPlanPayload()
	projections := payload["projection_digests"].(map[string]any)
	projections["oci_spec"].(map[string]any)["payload_digest"] = "spec_digest"

	err := Validate(ValidateParams{Payload: payload})
	if err == nil || !strings.Contains(err.Error(), "projection_digests.oci_spec.payload_digest is required") {
		t.Fatalf("expected projection digest error, got %v", err)
	}
}

func TestVerifyFrozenEvidenceChecksRunscAndProjections(t *testing.T) {
	payload := validPlanPayload()
	params := VerifyFrozenEvidenceParams{
		Payload:           payload,
		RunscPlatform:     "systrap",
		RunscVersion:      "runsc test",
		RunscBinaryPath:   "/usr/local/bin/runsc-test",
		RunscBinaryDigest: "sha256:runsc",
		ProjectionDigests: map[string]string{
			"bundle":                     "sha256:bundle",
			"runtime_config":             "sha256:runtime-config",
			"control_manifest_projected": "sha256:control-manifest-projected",
		},
		CheckpointBundleDigest:          "sha256:bundle",
		CheckpointRuntimeConfigDigest:   "sha256:runtime-config",
		CheckpointControlManifestDigest: "sha256:control-manifest-projected",
	}
	if err := VerifyFrozenEvidence(params); err != nil {
		t.Fatalf("verify frozen evidence: %v", err)
	}

	mismatch := params
	mismatch.RunscBinaryDigest = "sha256:changed"
	if err := VerifyFrozenEvidence(mismatch); err == nil || !strings.Contains(err.Error(), "runsc pin mismatch") {
		t.Fatalf("expected runsc mismatch, got %v", err)
	}

	mismatch = params
	mismatch.ProjectionDigests = map[string]string{"bundle": "sha256:changed"}
	if err := VerifyFrozenEvidence(mismatch); err == nil || !strings.Contains(err.Error(), "projection bundle digest mismatch") {
		t.Fatalf("expected projection mismatch, got %v", err)
	}
}

func validPlanPayload() map[string]any {
	driver, _ := agents.DriverSpecFor("claude_code")
	provider, _ := agents.RuntimeProviderSpecFor("local_runsc")
	featurePolicy, _ := agents.FeaturePolicyPayload(agents.DefaultFeaturePolicyForDriver(driver))
	featurePolicyPayload := map[string]any{}
	for key, value := range featurePolicy {
		featurePolicyPayload[key] = value
	}
	featurePolicyPayload["capability_schema_version"] = agents.DriverCapabilitySchemaVersion
	featurePolicyPayload["capability_vocab_version"] = provider.CapabilityVocabulary
	featurePolicyPayload["driver_capabilities"] = agents.DriverCapabilityPayload(driver)
	featurePolicyPayload["runtime_provider_capabilities"] = agents.RuntimeProviderCapabilityPayload(provider)
	featurePolicyPayload["legacy_supports_interrupt"] = driver.SupportsInterrupt
	featurePolicyPayload["legacy_supports_compaction"] = driver.SupportsCompaction
	featurePolicyPayload["unsupported_features_fail"] = true
	featurePolicyPayload["credential_bearing_mcp_scope"] = "out_of_scope"

	return map[string]any{
		"plan_version": store.GenerationPlanVersion,
		"identity": map[string]any{
			"session_id":    "sess_plan",
			"generation_id": "gen_plan",
			"product_mode":  "agent",
		},
		"driver": map[string]any{
			"driver_id":               "claude_code",
			"driver_kind":             string(driver.Kind),
			"bridge_protocol":         driver.BridgeProtocol,
			"bridge_protocol_version": driver.BridgeProtocolVersion,
			"turn_input_schema":       driver.TurnInputSchema,
			"output_schema":           driver.OutputSchema,
			"output_format":           driver.OutputFormat,
			"model":                   "claude-test",
			"initial_state_digest":    "sha256:driver-state",
			"initial_state_version":   1,
			"capability_snapshot":     agents.DriverCapabilityPayload(driver),
		},
		"runtime_provider": map[string]any{
			"provider_id":                  provider.ID,
			"provider_config_id":           "local_runsc",
			"provider_profile_id":          provider.ProviderProfileID,
			"isolation_kind":               provider.IsolationKind,
			"template_ref":                 provider.TemplateRef,
			"capability_vocab_version":     provider.CapabilityVocabulary,
			"capability_digest":            agents.CapabilityDigest(provider),
			"capability_snapshot":          agents.RuntimeProviderCapabilityPayload(provider),
			"snapshot_policy":              provider.SnapshotPolicy,
			"agent_runtime_profile_id":     "arp_gen_plan",
			"runtime_profile_provider_ref": "systrap",
		},
		"runsc_pin": map[string]any{
			"platform":      "systrap",
			"version":       "runsc test",
			"binary_path":   "/usr/local/bin/runsc-test",
			"binary_digest": "sha256:runsc",
		},
		"image": map[string]any{
			"agent_manifest_digest": "sha256:agent-manifest",
			"rootfs_path":           "/var/lib/harness/rootfs",
			"rootfs_image_digest":   nil,
		},
		"bridge_probe": map[string]any{
			"bridge_mode":               "claim-loop",
			"bridge_heartbeat_seconds":  "1.000000000",
			"bridge_poll_seconds":       "1.000000000",
			"lease_ttl_seconds":         "10.000000000",
			"ack_started_grace_seconds": "1.000000000",
			"reconnect_grace_seconds":   "1.000000000",
			"probe_url":                 "http://127.0.0.1:1/healthz",
			"probe_healthz_statuses":    []int{200},
			"pre_start_attempts":        1,
			"pre_start_interval_secs":   "1.000000000",
			"post_start_attempts":       1,
			"post_start_interval_secs":  "1.000000000",
		},
		"network": map[string]any{
			"network_profile_id":   "net_gen_plan",
			"runsc_network":        "sandbox",
			"runsc_overlay2":       "none",
			"sandbox_ip":           "10.240.0.2",
			"sandbox_ip_cidr":      "10.240.0.2/30",
			"host_gateway_ip":      "10.240.0.1",
			"sandbox_base_url":     "http://10.240.0.1:8080",
			"host_proxy_bind_url":  "http://127.0.0.1:8080",
			"proxy_port":           8080,
			"netns_name":           "harness-gen-plan",
			"netns_path":           "/var/run/netns/harness-gen-plan",
			"host_veth":            "vh-gen-plan",
			"sandbox_veth":         "vs-gen-plan",
			"host_side_cidr":       "10.240.0.1/30",
			"nft_table_name":       "harness-gen-plan",
			"egress_policy_id":     "egress_gen_plan",
			"egress_policy_digest": "sha256:egress",
			"dns_policy":           "off",
		},
		"data_volumes": map[string]any{
			"workspace": validVolumePayload("/var/lib/harness/sessions/sess_plan", "/var/lib/harness/evidence/workspaces/sess_plan.json", "/workspace"),
			"agent_home": map[string]any{
				"session_id":                 "sess_plan",
				"driver":                     "claude_code",
				"host_path":                  "/var/lib/harness/agent-homes/sess_plan/claude_code",
				"layout_version":             1,
				"runtime_identity_digest":    "sha256:agent-home-identity",
				"provisioning_marker_path":   "/var/lib/harness/evidence/driver-homes/sess_plan/claude_code.json",
				"provisioning_marker_digest": "sha256:agent-home-marker",
				"sandbox_destination":        "/agent-home",
				"sandbox_uid":                65534,
				"sandbox_gid":                65534,
				"sandbox_supplemental_gids":  []int{},
			},
		},
		"mounts": map[string]any{
			"workspace":                      map[string]any{"source": "/var/lib/harness/sessions/sess_plan", "destination": "/workspace", "mode": "rw"},
			"agent_home":                     map[string]any{"source": "/var/lib/harness/agent-homes/sess_plan/claude_code", "destination": "/agent-home", "mode": "rw"},
			"control":                        map[string]any{"source": "/var/lib/harness/run/control/gen_plan", "destination": "/harness-control", "mode": "ro"},
			"bridge":                         map[string]any{"source": "/var/lib/harness/run/bridge/gen_plan", "destination": "/harness-control/bridge", "mode": "rw"},
			"network_hosts_path":             nil,
			"driver_config_materializations": nil,
		},
		"runtime_artifacts": map[string]any{
			"control_dir_path":                     "/var/lib/harness/run/control/gen_plan",
			"control_manifest_path":                "/var/lib/harness/run/control/gen_plan/session.json",
			"control_manifest_digest":              "manifest_digest",
			"projected_control_manifest_digest":    "projected_manifest_digest",
			"bundle_dir_path":                      "/var/lib/harness/run/runtime/gen_plan",
			"bundle_digest":                        "bundle_digest",
			"runtime_config_digest":                "runtime_config_digest",
			"spec_path":                            "/var/lib/harness/run/runtime/gen_plan/config.json",
			"spec_digest":                          "spec_digest",
			"bridge_dir_path":                      "/var/lib/harness/run/bridge/gen_plan",
			"log_dir_path":                         "/var/lib/harness/logs/gen_plan",
			"network_hosts_path":                   nil,
			"materialized_driver_config":           []map[string]any{},
			"resource_identity_digest":             "sha256:resource",
			"sandbox_contract_id":                  "contract_gen_plan",
			"sandbox_contract_payload_digest":      "sha256:sandbox-contract",
			"sandbox_contract_compatibility_shape": store.SandboxContractVersion,
		},
		"feature_policy":      featurePolicyPayload,
		"content_snapshots":   map[string]any{"skills": nil, "managed_settings": nil},
		"source_digests":      map[string]any{"runtime_config_digest": "sha256:runtime-config", "agent_manifest_digest": "sha256:agent-manifest"},
		"projection_digests":  validProjectionPayload(),
		"mutable_state_scope": map[string]any{"leases": "runtime_generations", "events": "events", "checkpoint_state": "runtime_generations"},
	}
}

func validVolumePayload(hostPath, markerPath, destination string) map[string]any {
	return map[string]any{
		"session_id":                   "sess_plan",
		"host_path":                    hostPath,
		"layout_version":               1,
		"runtime_identity_digest":      "sha256:workspace-identity",
		"provisioning_marker_path":     markerPath,
		"provisioning_marker_digest":   "sha256:workspace-marker",
		"sandbox_destination":          destination,
		"sandbox_uid":                  65534,
		"sandbox_gid":                  65534,
		"sandbox_supplemental_gids":    []int{},
		"artifact_watcher_scope":       "workspace_only",
		"platform_content_mount_scope": "none",
	}
}

func validProjectionPayload() map[string]any {
	return map[string]any{
		"sandbox_contract":           map[string]any{"projection_version": 1, "payload_digest": "sha256:sandbox-contract", "materialized_path": nil},
		"control_manifest":           map[string]any{"projection_version": 1, "payload_digest": "sha256:control-manifest", "materialized_path": "/var/lib/harness/run/control/gen_plan/session.json"},
		"control_manifest_projected": map[string]any{"projection_version": 1, "payload_digest": "sha256:control-manifest-projected", "materialized_path": "/var/lib/harness/run/control/gen_plan/session.json"},
		"oci_spec":                   map[string]any{"projection_version": 1, "payload_digest": "sha256:oci-spec", "materialized_path": "/var/lib/harness/run/runtime/gen_plan/config.json"},
		"bundle":                     map[string]any{"projection_version": 1, "payload_digest": "sha256:bundle", "materialized_path": "/var/lib/harness/run/runtime/gen_plan"},
		"runtime_config":             map[string]any{"projection_version": 1, "payload_digest": "sha256:runtime-config", "materialized_path": nil},
	}
}
