package server

import (
	"context"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/generationplan"
	"harness-platform/orchestrator/internal/sessionstate"
	"harness-platform/orchestrator/internal/store"
)

func validServerGenerationPlanPayload() map[string]any {
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
	adapterInputDigests := serverAdapterInputDigestPayloadForTest(serverFrozenEvidenceSandboxContractPayloadForTest(
		"sess_frozen_evidence",
		"gen_frozen_evidence",
		"contract_gen_frozen_evidence",
		"claude_code",
		"sha256:driver-state",
	))
	return map[string]any{
		"plan_version": store.GenerationPlanVersion,
		"identity":     map[string]any{"session_id": "sess_frozen_evidence", "generation_id": "gen_frozen_evidence", "product_mode": "agent"},
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
			"agent_runtime_profile_id":     "arp_gen_frozen_evidence",
			"runtime_profile_provider_ref": "systrap",
		},
		"runsc_pin":    map[string]any{"platform": "systrap", "version": "runsc test", "binary_path": "/usr/local/bin/runsc-test", "binary_digest": "sha256:runsc"},
		"image":        map[string]any{"agent_manifest_digest": "sha256:agent-manifest", "rootfs_path": "/var/lib/harness/rootfs", "rootfs_image_digest": nil},
		"bridge_probe": map[string]any{"bridge_mode": "claim-loop"},
		"network": map[string]any{
			"network_profile_id": "net_gen_frozen_evidence", "runsc_network": "sandbox", "runsc_overlay2": "none",
			"sandbox_ip": "10.240.0.2", "sandbox_ip_cidr": "10.240.0.2/30", "host_gateway_ip": "10.240.0.1",
			"sandbox_base_url": "http://10.240.0.1:8080", "host_proxy_bind_url": "http://127.0.0.1:8080",
			"netns_name": "harness-gen-frozen", "netns_path": "/var/run/netns/harness-gen-frozen",
			"host_veth": "vh-frozen", "sandbox_veth": "vs-frozen", "host_side_cidr": "10.240.0.1/30",
			"nft_table_name": "harness-gen-frozen", "egress_policy_id": "egress_frozen",
			"egress_policy_digest": "egress_digest", "dns_policy": "off",
		},
		"data_volumes": map[string]any{
			"workspace":  serverPlanVolumePayload("/var/lib/harness/sessions/sess_frozen_evidence", "/var/lib/harness/evidence/workspaces/sess_frozen_evidence.json", "/workspace"),
			"agent_home": serverPlanVolumePayload("/var/lib/harness/agent-homes/sess_frozen_evidence/claude_code", "/var/lib/harness/evidence/driver-homes/sess_frozen_evidence/claude_code.json", "/agent-home"),
		},
		"mounts": map[string]any{
			"workspace":                      map[string]any{"source": "/var/lib/harness/sessions/sess_frozen_evidence", "destination": "/workspace", "mode": "rw"},
			"agent_home":                     map[string]any{"source": "/var/lib/harness/agent-homes/sess_frozen_evidence/claude_code", "destination": "/agent-home", "mode": "rw"},
			"control":                        map[string]any{"source": "/var/lib/harness/run/control/gen_frozen_evidence", "destination": "/harness-control", "mode": "ro"},
			"bridge":                         map[string]any{"source": "/var/lib/harness/run/bridge/gen_frozen_evidence", "destination": "/harness-control/bridge", "mode": "rw"},
			"network_hosts_path":             nil,
			"driver_config_materializations": nil,
		},
		"runtime_artifacts": map[string]any{
			"control_dir_path": "/var/lib/harness/run/control/gen_frozen_evidence", "control_manifest_path": "/var/lib/harness/run/control/gen_frozen_evidence/session.json",
			"control_manifest_digest": "manifest_digest", "projected_control_manifest_digest": "projected_manifest_digest",
			"bundle_dir_path": "/var/lib/harness/run/runtime/gen_frozen_evidence", "bundle_digest": "bundle_digest",
			"runtime_config_digest": "runtime_config_digest", "spec_path": "/var/lib/harness/run/runtime/gen_frozen_evidence/config.json",
			"spec_digest": "spec_digest", "bridge_dir_path": "/var/lib/harness/run/bridge/gen_frozen_evidence",
			"log_dir_path": "/var/lib/harness/logs/gen_frozen_evidence", "network_hosts_path": nil,
			"materialized_driver_config": []map[string]any{}, "resource_identity_digest": "sha256:resource",
			"sandbox_contract_id": "contract_gen_frozen_evidence", "sandbox_contract_payload_digest": "sha256:sandbox-contract",
			"sandbox_contract_compatibility_shape": store.SandboxContractVersion,
		},
		"feature_policy":    featurePolicyPayload,
		"content_snapshots": map[string]any{"skills": nil, "managed_settings": nil},
		"source_digests": map[string]any{
			"runtime_config_digest": "sha256:runtime-config-source",
			"agent_manifest_digest": "sha256:agent-manifest",
			"adapter_input_digests": adapterInputDigests,
		},
		"mutable_state_scope": map[string]any{"leases": "runtime_generations", "events": "events", "checkpoint_state": "runtime_generations"},
	}
}

func serverFrozenEvidenceSandboxContractPayloadForTest(sessionID, generationID, contractID, driverID, driverStateDigest string) map[string]any {
	modelAccessAllowed := driverID == "claude_code"
	return map[string]any{
		"sandbox_contract_version": store.SandboxContractVersion,
		"contract_schema_version":  store.SandboxContractSchemaVersion,
		"contract_gate_version":    store.SandboxContractGateDriverManifest,
		"contract_id":              contractID,
		"session_id":               sessionID,
		"generation_id":            generationID,
		"runtime_profile_id":       "arp_gen_frozen_evidence",
		"network_profile_id":       "net_gen_frozen_evidence",
		"driver": map[string]any{
			"driver_id":                            driverID,
			"driver_version":                       "test",
			"bridge_protocol":                      "harness_bridge_v2",
			"bridge_protocol_version":              2,
			"turn_input_schema":                    "RunTurn",
			"output_schema":                        "claude_stream_json_v1",
			"command_argv_digest":                  "sha256:command",
			"driver_config_digest":                 "sha256:driver-config",
			"required_runtime_capabilities_digest": "sha256:driver-capabilities",
			"supports_interrupt":                   false,
			"supports_compaction":                  true,
		},
		"runtime_provider": map[string]any{
			"provider_id":              "local_runsc",
			"provider_profile_id":      "local_runsc_default",
			"isolation_kind":           "gvisor",
			"template_ref":             "default",
			"template_digest":          "sha256:template",
			"capability_vocab_version": "1",
			"capability_digest":        "sha256:provider-capabilities",
		},
		"identity": map[string]any{
			"model_access_allowed": modelAccessAllowed,
		},
		"network_identity": map[string]any{
			"runsc_network": "sandbox",
			"sandbox_ip":    "10.240.0.2",
		},
		"credential_policy": serverCredentialPolicyPayloadForTest(driverID),
		"model_access": map[string]any{
			"model_access_allowed":         modelAccessAllowed,
			"sandbox_model_proxy_base_url": "http://harness-model-proxy.internal:8082",
		},
		"driver_runtime": map[string]any{
			"driver_home_mount":             "/agent-home",
			"generated_driver_config_mount": "/harness-control/driver/" + driverID,
			"materialized_driver_config":    map[string]any{},
			"initial_driver_state_digest":   driverStateDigest,
		},
		"runtime_adapter": map[string]any{
			"kind":                "runsc",
			"runsc_platform":      "systrap",
			"runsc_version":       "runsc test",
			"runsc_binary_path":   "/usr/local/bin/runsc-test",
			"runsc_binary_digest": "sha256:runsc",
			"runsc_container_id":  "runsc-gen-frozen",
			"runsc_network":       "sandbox",
			"runsc_overlay2":      "none",
		},
		"input_digests": map[string]any{
			"runtime_config_digest": "sha256:runtime-config",
			"rootfs_image_digest":   nil,
			"agent_manifest_digest": "sha256:agent-manifest",
		},
	}
}

func serverAdapterInputDigestPayloadForTest(contractPayload map[string]any) map[string]any {
	digests, err := generationplan.AdapterInputDigestsFromSandboxContract(contractPayload)
	if err != nil {
		panic(err)
	}
	return map[string]any{
		"driver_adapter":  digests["driver_adapter"],
		"runtime_adapter": digests["runtime_adapter"],
	}
}

func storeServerFrozenEvidenceCanonicalPayload(t *testing.T) []byte {
	t.Helper()
	canonical, err := serverFrozenEvidenceCanonicalPayload()
	if err != nil {
		t.Fatalf("canonical frozen evidence payload: %v", err)
	}
	return canonical
}

func serverFrozenEvidenceCanonicalPayload() ([]byte, error) {
	return store.CanonicalGenerationPlanPayload(validServerGenerationPlanPayload())
}

func mustServerFrozenEvidenceCanonicalPayload() []byte {
	canonical, err := serverFrozenEvidenceCanonicalPayload()
	if err != nil {
		panic(err)
	}
	return canonical
}

func storeServerFrozenEvidencePlan(t *testing.T, ctx context.Context, st *store.Store, dir string, payload map[string]any) store.GenerationPlanRecord {
	t.Helper()
	session := createServerTestSession(t, ctx, st, dir, "sess_frozen_evidence", string(sessionstate.Created), time.Now().UTC(), nil)
	if _, err := st.DBForTest().ExecContext(ctx, `
INSERT INTO runtime_generations (generation_id, session_id, status, lease_owner, lease_expires_at)
VALUES (?, ?, 'allocating', 'owner', ?)`, "gen_frozen_evidence", session.ID, time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("insert runtime generation: %v", err)
	}
	plan, err := st.StoreGenerationPlan(ctx, store.StoreGenerationPlanParams{
		GenerationID: "gen_frozen_evidence",
		Payload:      payload,
	})
	if err != nil {
		t.Fatalf("store generation plan: %v", err)
	}
	for kind, digest := range map[string]string{
		"sandbox_contract":           "sha256:sandbox-contract",
		"control_manifest":           "sha256:control-manifest",
		"control_manifest_projected": "sha256:control-manifest-projected",
		"oci_spec":                   "sha256:oci-spec",
		"bundle":                     "sha256:bundle",
		"runtime_config":             "sha256:runtime-config",
	} {
		if _, err := st.StoreGenerationPlanProjection(ctx, store.StoreGenerationPlanProjectionParams{
			GenerationID:      "gen_frozen_evidence",
			PlanDigest:        plan.PlanDigest,
			ProjectionKind:    kind,
			ProjectionVersion: 1,
			PayloadDigest:     digest,
		}); err != nil {
			t.Fatalf("store projection %s: %v", kind, err)
		}
	}
	return plan
}
