package generationplan

import (
	"strings"
	"testing"

	"harness-platform/orchestrator/internal/store"
)

func TestVerifyFrozenEvidenceChecksRunscAndProjections(t *testing.T) {
	payload := validPlanPayload()
	params := validFrozenEvidenceParams(payload)
	if err := VerifyFrozenEvidence(params); err != nil {
		t.Fatalf("verify frozen evidence: %v", err)
	}

	mismatch := params
	mismatch.SessionID = "sess_changed"
	if err := VerifyFrozenEvidence(mismatch); err == nil || !strings.Contains(err.Error(), "identity.session_id mismatch") {
		t.Fatalf("expected identity mismatch, got %v", err)
	}

	mismatch = params
	mismatch.DriverStateDigest = "sha256:changed"
	if err := VerifyFrozenEvidence(mismatch); err == nil || !strings.Contains(err.Error(), "driver.initial_state_digest mismatch") {
		t.Fatalf("expected driver-state digest mismatch, got %v", err)
	}

	mismatch = params
	mismatch.DriverStateVersion = 2
	if err := VerifyFrozenEvidence(mismatch); err == nil || !strings.Contains(err.Error(), "driver.initial_state_version mismatch") {
		t.Fatalf("expected driver-state version mismatch, got %v", err)
	}

	mismatch = params
	mismatch.NetworkProfileID = "net_changed"
	if err := VerifyFrozenEvidence(mismatch); err == nil || !strings.Contains(err.Error(), "network.network_profile_id mismatch") {
		t.Fatalf("expected network profile mismatch, got %v", err)
	}

	mismatch = params
	mismatch.RunscBinaryDigest = "sha256:changed"
	if err := VerifyFrozenEvidence(mismatch); err == nil || !strings.Contains(err.Error(), "runsc pin mismatch") {
		t.Fatalf("expected runsc mismatch, got %v", err)
	}

	mismatch = params
	mismatch.ProjectionDigests = cloneStringMap(params.ProjectionDigests)
	delete(mismatch.ProjectionDigests, "oci_spec")
	if err := VerifyFrozenEvidence(mismatch); err == nil || !strings.Contains(err.Error(), "projection oci_spec digest is required") {
		t.Fatalf("expected missing projection digest, got %v", err)
	}

	mismatch = params
	mismatch.ProjectionVersions = map[string]int{"bundle": 2}
	if err := VerifyFrozenEvidence(mismatch); err == nil || !strings.Contains(err.Error(), "projection bundle version mismatch") {
		t.Fatalf("expected projection version mismatch, got %v", err)
	}
}

func TestVerifyFrozenEvidenceRequiresCheckpointDriverStateFence(t *testing.T) {
	payload := validPlanPayload()
	params := validFrozenEvidenceParams(payload)
	params.CheckpointDriverStatesDigest = ""
	if err := VerifyFrozenEvidence(params); err == nil || !strings.Contains(err.Error(), "checkpoint driver-state digest is required") {
		t.Fatalf("expected missing checkpoint driver-state fence, got %v", err)
	}

	params = validFrozenEvidenceParams(payload)
	params.CheckpointDriverStatesDigest = "driver-state-fence"
	if err := VerifyFrozenEvidence(params); err == nil || !strings.Contains(err.Error(), "checkpoint driver-state digest is required") {
		t.Fatalf("expected malformed checkpoint driver-state fence, got %v", err)
	}

	params = validFrozenEvidenceParams(payload)
	params.CheckpointPlanDigest = "sha256:changed"
	if err := VerifyFrozenEvidence(params); err == nil || !strings.Contains(err.Error(), "checkpoint plan digest mismatch") {
		t.Fatalf("expected checkpoint plan digest mismatch, got %v", err)
	}

	params = validFrozenEvidenceParams(payload)
	params.CheckpointBundleDigest = ""
	params.CheckpointRuntimeConfigDigest = ""
	params.CheckpointControlManifestDigest = ""
	params.CheckpointDriverStatesDigest = ""
	params.CheckpointPlanDigest = ""
	if err := VerifyFrozenEvidence(params); err != nil {
		t.Fatalf("non-checkpoint evidence should not require driver-state fence: %v", err)
	}
}

func TestVerifyFrozenEvidenceChecksContentSnapshotDigests(t *testing.T) {
	payload := validPlanPayload()
	contentSnapshots := payload["content_snapshots"].(map[string]any)
	contentSnapshots["skills"] = map[string]any{
		"kind":                   "skills",
		"digest":                 "sha256:skills",
		"immutable_host_path":    "/var/lib/harness/content/skills/sha256-skills",
		"mount_destination":      "/harness-skills",
		"source_evidence_digest": "sha256:source",
		"retention_class":        "active",
	}
	params := validFrozenEvidenceParams(payload)
	params.ContentSnapshotDigests = map[string]string{"skills": "sha256:skills"}
	if err := VerifyFrozenEvidence(params); err != nil {
		t.Fatalf("verify content snapshot evidence: %v", err)
	}

	missing := params
	missing.ContentSnapshotDigests = nil
	if err := VerifyFrozenEvidence(missing); err == nil || !strings.Contains(err.Error(), "content snapshot skills digest is required") {
		t.Fatalf("expected missing content snapshot digest, got %v", err)
	}

	mismatch := params
	mismatch.ContentSnapshotDigests = map[string]string{"skills": "sha256:changed"}
	if err := VerifyFrozenEvidence(mismatch); err == nil || !strings.Contains(err.Error(), "content snapshot skills digest mismatch") {
		t.Fatalf("expected content snapshot digest mismatch, got %v", err)
	}
}

func TestVerifyDataVolumeEvidenceChecksFrozenVolumeRefs(t *testing.T) {
	payload := validPlanPayload()
	params := VerifyDataVolumeEvidenceParams{
		Payload:                         payload,
		WorkspaceHostPath:               "/var/lib/harness/sessions/sess_plan",
		WorkspaceRuntimeIdentityDigest:  "sha256:workspace-identity",
		DriverHomeHostPath:              "/var/lib/harness/agent-homes/sess_plan/claude_code",
		DriverHomeRuntimeIdentityDigest: "sha256:agent-home-identity",
	}
	if err := VerifyDataVolumeEvidence(params); err != nil {
		t.Fatalf("verify data volume evidence: %v", err)
	}

	mismatch := params
	mismatch.WorkspaceHostPath = "/var/lib/harness/sessions/changed"
	if err := VerifyDataVolumeEvidence(mismatch); err == nil || !strings.Contains(err.Error(), "data_volumes.workspace.host_path mismatch") {
		t.Fatalf("expected workspace host path mismatch, got %v", err)
	}

	mismatch = params
	mismatch.DriverHomeRuntimeIdentityDigest = "sha256:changed"
	if err := VerifyDataVolumeEvidence(mismatch); err == nil || !strings.Contains(err.Error(), "data_volumes.agent_home.runtime_identity_digest mismatch") {
		t.Fatalf("expected driver-home identity mismatch, got %v", err)
	}
}

func TestVerifyNetworkEvidenceChecksFrozenNetworkIdentity(t *testing.T) {
	payload := validPlanPayload()
	params := VerifyNetworkEvidenceParams{
		Payload:            payload,
		NetworkProfileID:   "net_gen_plan",
		RunscNetwork:       "sandbox",
		RunscOverlay2:      "none",
		SandboxIP:          "10.240.0.2",
		SandboxIPCIDR:      "10.240.0.2/30",
		HostGatewayIP:      "10.240.0.1",
		SandboxBaseURL:     "http://10.240.0.1:8080",
		HostProxyBindURL:   "http://127.0.0.1:8080",
		ProxyPort:          8080,
		NetnsName:          "harness-gen-plan",
		NetnsPath:          "/var/run/netns/harness-gen-plan",
		HostVeth:           "vh-gen-plan",
		SandboxVeth:        "vs-gen-plan",
		HostSideCIDR:       "10.240.0.1/30",
		NftTableName:       "harness-gen-plan",
		EgressPolicyID:     "egress_gen_plan",
		EgressPolicyDigest: "sha256:egress",
		DNSPolicy:          "off",
	}
	if err := VerifyNetworkEvidence(params); err != nil {
		t.Fatalf("verify network evidence: %v", err)
	}

	mismatch := params
	mismatch.NftTableName = "changed-table"
	if err := VerifyNetworkEvidence(mismatch); err == nil || !strings.Contains(err.Error(), "network.nft_table_name mismatch") {
		t.Fatalf("expected nft table mismatch, got %v", err)
	}

	mismatch = params
	mismatch.ProxyPort = 8081
	if err := VerifyNetworkEvidence(mismatch); err == nil || !strings.Contains(err.Error(), "network.proxy_port mismatch") {
		t.Fatalf("expected proxy port mismatch, got %v", err)
	}
}

func TestVerifyRuntimeArtifactPathEvidenceChecksFrozenPaths(t *testing.T) {
	payload := validPlanPayload()
	params := VerifyRuntimeArtifactPathEvidenceParams{
		Payload:             payload,
		ControlDirPath:      "/var/lib/harness/run/control/gen_plan",
		ControlManifestPath: "/var/lib/harness/run/control/gen_plan/session.json",
		BundleDirPath:       "/var/lib/harness/run/runtime/gen_plan",
		SpecPath:            "/var/lib/harness/run/runtime/gen_plan/config.json",
		BridgeDirPath:       "/var/lib/harness/run/bridge/gen_plan",
		LogDirPath:          "/var/lib/harness/logs/gen_plan",
	}
	if err := VerifyRuntimeArtifactPathEvidence(params); err != nil {
		t.Fatalf("verify runtime artifact paths: %v", err)
	}

	mismatch := params
	mismatch.BundleDirPath = "/var/lib/harness/run/runtime/changed"
	if err := VerifyRuntimeArtifactPathEvidence(mismatch); err == nil || !strings.Contains(err.Error(), "runtime_artifacts.bundle_dir_path mismatch") {
		t.Fatalf("expected bundle dir mismatch, got %v", err)
	}

	payloadWithHosts := validPlanPayload()
	runtimeArtifacts := payloadWithHosts["runtime_artifacts"].(map[string]any)
	runtimeArtifacts["network_hosts_path"] = "/var/lib/harness/run/network/gen_plan/hosts"
	params.Payload = payloadWithHosts
	params.NetworkHostsPath = "/var/lib/harness/run/network/gen_plan/hosts.changed"
	if err := VerifyRuntimeArtifactPathEvidence(params); err == nil || !strings.Contains(err.Error(), "runtime_artifacts.network_hosts_path mismatch") {
		t.Fatalf("expected network hosts path mismatch, got %v", err)
	}
}

func TestVerifyMountPlanEvidenceChecksRuntimeMountPlan(t *testing.T) {
	payload := validPlanPayload()
	mountPlan := mountPlanForPayload(t, payload, nil)
	if err := VerifyMountPlanEvidence(VerifyMountPlanEvidenceParams{Payload: payload, MountPlan: mountPlan}); err != nil {
		t.Fatalf("verify mount plan evidence: %v", err)
	}

	mutated := mountPlanForPayload(t, payload, nil)
	mutateMountSource(t, &mutated, "workspace", "/var/lib/harness/sessions/changed")
	if err := VerifyMountPlanEvidence(VerifyMountPlanEvidenceParams{Payload: payload, MountPlan: mutated}); err == nil ||
		!strings.Contains(err.Error(), "mounts.workspace.source mismatch") {
		t.Fatalf("expected workspace mount mismatch, got %v", err)
	}
}

func TestVerifyMountPlanEvidenceChecksContentSnapshotMounts(t *testing.T) {
	payload := validPlanPayload()
	skills := validSkillsSnapshotPayload()
	payload["content_snapshots"].(map[string]any)["skills"] = skills
	addSkillsSnapshotMount(payload, skills)
	snapshots := []store.ContentSnapshotRecord{{
		Kind:                 store.ContentSnapshotKindSkills,
		Digest:               "sha256:skills",
		ImmutableHostPath:    "/var/lib/harness/content/skills/sha256-skills",
		MountDestination:     store.ContentSnapshotSkillsMount,
		SourceEvidenceDigest: "sha256:source",
		RetentionClass:       "active",
	}}
	mountPlan := mountPlanForPayload(t, payload, snapshots)
	if err := VerifyMountPlanEvidence(VerifyMountPlanEvidenceParams{Payload: payload, MountPlan: mountPlan}); err != nil {
		t.Fatalf("verify content snapshot mount plan evidence: %v", err)
	}

	mutated := mountPlanForPayload(t, payload, snapshots)
	mutateMountSource(t, &mutated, "skills_snapshot", "/var/lib/harness/content/skills/changed")
	if err := VerifyMountPlanEvidence(VerifyMountPlanEvidenceParams{Payload: payload, MountPlan: mutated}); err == nil ||
		!strings.Contains(err.Error(), "mounts.content_snapshots.skills.source mismatch") {
		t.Fatalf("expected skills snapshot mount mismatch, got %v", err)
	}
}

func TestVerifyMountPlanEvidenceChecksDriverConfigMounts(t *testing.T) {
	payload := validPiPlanPayload(t)
	mountPlan := mountPlanForPayload(t, payload, nil)
	if err := VerifyMountPlanEvidence(VerifyMountPlanEvidenceParams{Payload: payload, MountPlan: mountPlan}); err != nil {
		t.Fatalf("verify driver config mount plan evidence: %v", err)
	}

	mutated := mountPlanForPayload(t, payload, nil)
	mutateMountSource(t, &mutated, "pi_settings_config", "/var/lib/harness/run/control/gen_plan/driver/pi/changed.json")
	if err := VerifyMountPlanEvidence(VerifyMountPlanEvidenceParams{Payload: payload, MountPlan: mutated}); err == nil ||
		!strings.Contains(err.Error(), "mounts.driver_config_materializations.settings.source mismatch") {
		t.Fatalf("expected driver config mount mismatch, got %v", err)
	}
}

func TestVerifySandboxContractEvidenceChecksStoredProjectionDigest(t *testing.T) {
	payload := validPlanPayload()
	params := VerifySandboxContractEvidenceParams{
		Payload:          payload,
		ContractID:       "contract_gen_plan",
		ContractDigest:   "sha256:sandbox-contract",
		ProjectionDigest: "sha256:sandbox-contract",
	}
	if err := VerifySandboxContractEvidence(params); err != nil {
		t.Fatalf("verify sandbox contract evidence: %v", err)
	}

	mismatch := params
	mismatch.ProjectionDigest = "sha256:changed"
	if err := VerifySandboxContractEvidence(mismatch); err == nil ||
		!strings.Contains(err.Error(), "sandbox_contract projection digest mismatch") {
		t.Fatalf("expected sandbox contract projection mismatch, got %v", err)
	}

	mismatch = params
	mismatch.ContractDigest = "sha256:changed"
	mismatch.ProjectionDigest = "sha256:changed"
	if err := VerifySandboxContractEvidence(mismatch); err == nil ||
		!strings.Contains(err.Error(), "runtime_artifacts.sandbox_contract_payload_digest mismatch") {
		t.Fatalf("expected sandbox contract payload digest mismatch, got %v", err)
	}
}

func TestVerifyRuntimeResourceEvidenceChecksFrozenIdentityDigest(t *testing.T) {
	payload := validPlanPayload()
	params := VerifyRuntimeResourceEvidenceParams{
		Payload:                payload,
		ResourceIdentityDigest: "sha256:resource",
	}
	if err := VerifyRuntimeResourceEvidence(params); err != nil {
		t.Fatalf("verify runtime resource evidence: %v", err)
	}

	params.ResourceIdentityDigest = "sha256:changed"
	if err := VerifyRuntimeResourceEvidence(params); err == nil ||
		!strings.Contains(err.Error(), "runtime_artifacts.resource_identity_digest mismatch") {
		t.Fatalf("expected resource identity digest mismatch, got %v", err)
	}
}

func TestVerifySourceDigestEvidenceChecksStoredInputDigests(t *testing.T) {
	payload := validPlanPayload()
	params := VerifySourceDigestEvidenceParams{
		Payload:             payload,
		RuntimeConfigDigest: "sha256:runtime-config",
		AgentManifestDigest: "sha256:agent-manifest",
	}
	if err := VerifySourceDigestEvidence(params); err != nil {
		t.Fatalf("verify source digest evidence: %v", err)
	}

	missing := params
	missing.RuntimeConfigDigest = ""
	if err := VerifySourceDigestEvidence(missing); err == nil ||
		!strings.Contains(err.Error(), "source_digests.runtime_config_digest evidence is required") {
		t.Fatalf("expected missing runtime config evidence, got %v", err)
	}

	mismatch := params
	mismatch.AgentManifestDigest = "sha256:changed"
	if err := VerifySourceDigestEvidence(mismatch); err == nil ||
		!strings.Contains(err.Error(), "source_digests.agent_manifest_digest mismatch") {
		t.Fatalf("expected agent manifest evidence mismatch, got %v", err)
	}
}

func TestVerifySourceDigestEvidenceChecksAdapterInputDigests(t *testing.T) {
	contractPayload := validSandboxContractPayloadForAdapterInputDigests()
	adapterInputDigests, err := AdapterInputDigestsFromSandboxContract(contractPayload)
	if err != nil {
		t.Fatalf("adapter input digests: %v", err)
	}
	payload := validPlanPayload()
	sourceDigests := payload["source_digests"].(map[string]any)
	sourceDigests["adapter_input_digests"] = map[string]any{
		"driver_adapter":  adapterInputDigests["driver_adapter"],
		"runtime_adapter": adapterInputDigests["runtime_adapter"],
	}
	params := VerifySourceDigestEvidenceParams{
		Payload:             payload,
		RuntimeConfigDigest: "sha256:runtime-config",
		AgentManifestDigest: "sha256:agent-manifest",
		AdapterInputDigests: adapterInputDigests,
	}
	if err := VerifySourceDigestEvidence(params); err != nil {
		t.Fatalf("verify adapter input digests: %v", err)
	}

	mismatch := cloneStringMap(adapterInputDigests)
	mismatch["runtime_adapter"] = "sha256:changed"
	params.AdapterInputDigests = mismatch
	if err := VerifySourceDigestEvidence(params); err == nil ||
		!strings.Contains(err.Error(), "source_digests.adapter_input_digests.runtime_adapter mismatch") {
		t.Fatalf("expected adapter input digest mismatch, got %v", err)
	}
}
