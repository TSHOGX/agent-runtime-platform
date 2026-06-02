package server

import (
	"testing"

	"harness-platform/orchestrator/internal/store"
)

func serverRuntimeResourceSandboxContractPayloadForTest(t *testing.T, details store.RuntimeGenerationDetails, allocation store.GenerationAllocation, contractID string) map[string]any {
	t.Helper()
	sandboxIP, err := runtimeResourceSandboxIP(details.SandboxIPCIDR)
	if err != nil {
		t.Fatalf("sandbox contract sandbox ip for %s: %v", allocation.GenerationID, err)
	}
	driverID := allocation.DriverState.DriverID
	credentialPolicy := serverCredentialPolicyForTest(t, driverID)
	modelAccessAllowed := driverID == "claude_code"
	return map[string]any{
		"sandbox_contract_version": store.SandboxContractVersion,
		"contract_schema_version":  store.SandboxContractSchemaVersion,
		"contract_gate_version":    store.SandboxContractGateDriverManifest,
		"contract_id":              contractID,
		"session_id":               details.SessionID,
		"generation_id":            allocation.GenerationID,
		"runtime_profile_id":       allocation.AgentRuntimeProfileID,
		"network_profile_id":       allocation.NetworkProfileID,
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
			"runsc_network": details.RunscNetwork,
			"sandbox_ip":    sandboxIP,
		},
		"credential_policy": credentialPolicy,
		"model_access": map[string]any{
			"model_access_allowed": modelAccessAllowed,
		},
		"driver_runtime": map[string]any{
			"driver_home_mount":             "/agent-home",
			"generated_driver_config_mount": "/harness-control/driver/" + driverID,
			"materialized_driver_config":    map[string]any{},
			"initial_driver_state_digest":   allocation.DriverState.StateDigest,
		},
		"runtime_adapter": map[string]any{
			"kind":                "runsc",
			"runsc_platform":      details.RunscPlatform,
			"runsc_version":       details.RunscVersion,
			"runsc_binary_path":   details.RunscBinaryPath,
			"runsc_binary_digest": details.RunscBinaryDigest,
			"runsc_container_id":  details.RunscContainerID,
			"runsc_network":       details.RunscNetwork,
			"runsc_overlay2":      details.RunscOverlay2,
		},
		"input_digests": map[string]any{
			"runtime_config_digest": "sha256:runtime-config",
			"rootfs_image_digest":   nil,
			"agent_manifest_digest": "sha256:agent-manifest",
		},
	}
}

func serverSandboxContractPayloadDigestForTest(t *testing.T, payload map[string]any) string {
	t.Helper()
	canonical, err := store.CanonicalSandboxContractPayload(payload)
	if err != nil {
		t.Fatalf("canonical sandbox contract payload: %v", err)
	}
	return store.SandboxContractDigest(canonical)
}
