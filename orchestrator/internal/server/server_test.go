package server

import (
	"context"
	"net/netip"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/planprojection"
	"harness-platform/orchestrator/internal/runtime"
	"harness-platform/orchestrator/internal/store"
)

func storeServerGenerationPlanForArtifacts(t *testing.T, ctx context.Context, st *store.Store, sessionID, generationID string, artifacts runtime.GenerationArtifacts) store.GenerationPlanRecord {
	t.Helper()
	details, err := st.GetRuntimeGenerationDetails(ctx, sessionID, generationID)
	if err != nil {
		t.Fatalf("get generation details for plan %s: %v", generationID, err)
	}
	mode := "agent"
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COALESCE(mode, '')
FROM sessions
WHERE id = ?`, sessionID).Scan(&mode); err != nil {
		t.Fatalf("get session mode for plan %s: %v", generationID, err)
	}
	if strings.TrimSpace(mode) == "" {
		mode = "agent"
	}
	driverSpec, ok := agents.DriverSpecFor(details.DriverID)
	if !ok {
		t.Fatalf("driver spec missing for %s", details.DriverID)
	}
	providerSpec, ok := agents.RuntimeProviderSpecFor("local_runsc")
	if !ok {
		t.Fatalf("provider spec missing")
	}
	featurePolicy, err := agents.FeaturePolicyPayload(agents.DefaultFeaturePolicyForDriver(driverSpec))
	if err != nil {
		t.Fatalf("feature policy for plan %s: %v", generationID, err)
	}
	featurePolicyPayload := map[string]any{}
	for key, value := range featurePolicy {
		featurePolicyPayload[key] = value
	}
	featurePolicyPayload["capability_schema_version"] = agents.DriverCapabilitySchemaVersion
	featurePolicyPayload["capability_vocab_version"] = providerSpec.CapabilityVocabulary
	featurePolicyPayload["driver_capabilities"] = agents.DriverCapabilityPayload(driverSpec)
	featurePolicyPayload["runtime_provider_capabilities"] = agents.RuntimeProviderCapabilityPayload(providerSpec)
	featurePolicyPayload["legacy_supports_interrupt"] = driverSpec.SupportsInterrupt
	featurePolicyPayload["legacy_supports_compaction"] = driverSpec.SupportsCompaction
	featurePolicyPayload["unsupported_features_fail"] = true
	featurePolicyPayload["credential_bearing_mcp_scope"] = "out_of_scope"
	payload := validServerGenerationPlanPayload()
	payload["identity"] = map[string]any{"session_id": sessionID, "generation_id": generationID, "product_mode": mode}
	workspaceVolume, driverHomeVolume := provisionServerGenerationPlanFixtureVolumes(t, ctx, st, sessionID, details)
	driverPlan := payload["driver"].(map[string]any)
	driverPlan["driver_id"] = string(driverSpec.ID)
	driverPlan["driver_kind"] = string(driverSpec.Kind)
	driverPlan["bridge_protocol"] = driverSpec.BridgeProtocol
	driverPlan["bridge_protocol_version"] = driverSpec.BridgeProtocolVersion
	driverPlan["turn_input_schema"] = driverSpec.TurnInputSchema
	driverPlan["output_schema"] = driverSpec.OutputSchema
	driverPlan["output_format"] = details.OutputFormat
	if strings.TrimSpace(details.Model) == "" {
		driverPlan["model"] = nil
	} else {
		driverPlan["model"] = details.Model
	}
	driverPlan["initial_state_digest"] = details.DriverStateDigest
	driverPlan["initial_state_version"] = details.DriverStateVersion
	driverPlan["capability_snapshot"] = agents.DriverCapabilityPayload(driverSpec)
	runtimeProvider := payload["runtime_provider"].(map[string]any)
	runtimeProvider["agent_runtime_profile_id"] = details.AgentRuntimeProfileID
	runtimeProvider["runtime_profile_provider_ref"] = details.RunscPlatform
	networkPlan := payload["network"].(map[string]any)
	networkPlan["network_profile_id"] = details.NetworkProfileID
	networkPlan["runsc_network"] = details.RunscNetwork
	networkPlan["runsc_overlay2"] = details.RunscOverlay2
	sandboxIP, err := runtimeResourceSandboxIP(details.SandboxIPCIDR)
	if err != nil {
		t.Fatalf("render sandbox ip for plan %s: %v", generationID, err)
	}
	networkPlan["sandbox_ip"] = sandboxIP
	networkPlan["sandbox_ip_cidr"] = details.SandboxIPCIDR
	networkPlan["host_gateway_ip"] = details.HostGatewayIP
	networkPlan["sandbox_base_url"] = details.SandboxBaseURL
	networkPlan["host_proxy_bind_url"] = details.HostProxyBindURL
	networkPlan["proxy_port"] = details.ProxyPort
	networkPlan["netns_name"] = details.NetnsName
	networkPlan["netns_path"] = details.NetnsPath
	networkPlan["host_veth"] = details.HostVeth
	networkPlan["sandbox_veth"] = details.SandboxVeth
	networkPlan["host_side_cidr"] = details.HostSideCIDR
	nftTableName, err := runtimeResourceNftTableName(details.GenerationID)
	if err != nil {
		t.Fatalf("render nft table for plan %s: %v", generationID, err)
	}
	networkPlan["nft_table_name"] = nftTableName
	networkPlan["egress_policy_id"] = details.EgressPolicyID
	networkPlan["egress_policy_digest"] = details.EgressPolicyDigest
	networkPlan["dns_policy"] = details.DNSPolicy
	runscPin := payload["runsc_pin"].(map[string]any)
	runscPin["version"] = artifacts.RunscVersion
	runscPin["binary_path"] = artifacts.RunscBinaryPath
	runscPin["binary_digest"] = artifacts.RunscBinaryDigest
	runtimeArtifacts := payload["runtime_artifacts"].(map[string]any)
	runtimeArtifacts["control_manifest_digest"] = artifacts.ManifestDigest
	runtimeArtifacts["projected_control_manifest_digest"] = artifacts.ProjectedManifestDigest
	runtimeArtifacts["control_dir_path"] = details.ControlDirPath
	runtimeArtifacts["control_manifest_path"] = details.ControlManifestPath
	runtimeArtifacts["bundle_dir_path"] = details.BundleDirPath
	runtimeArtifacts["bundle_digest"] = artifacts.BundleDigest
	runtimeArtifacts["runtime_config_digest"] = artifacts.RuntimeConfigDigest
	runtimeArtifacts["spec_path"] = details.SpecPath
	runtimeArtifacts["spec_digest"] = artifacts.SpecDigest
	runtimeArtifacts["bridge_dir_path"] = details.BridgeDirPath
	runtimeArtifacts["log_dir_path"] = details.LogDirPath
	if strings.TrimSpace(details.NetworkHostsPath) == "" {
		runtimeArtifacts["network_hosts_path"] = nil
	} else {
		runtimeArtifacts["network_hosts_path"] = details.NetworkHostsPath
	}
	allocation := serverGenerationAllocationForTest(t, ctx, st, sessionID, generationID)
	sandboxContractPayload := serverRuntimeResourceSandboxContractPayloadForTest(t, details, allocation, sandboxContractID(generationID))
	sandboxContractDigest := serverSandboxContractPayloadDigestForTest(t, sandboxContractPayload)
	runtimeArtifacts["sandbox_contract_id"] = sandboxContractID(generationID)
	runtimeArtifacts["sandbox_contract_payload_digest"] = sandboxContractDigest
	runtimeArtifacts["resource_identity_digest"] = serverRuntimeResourceIdentityDigestForPlanFixture(t, details, artifacts)
	sourceDigests := payload["source_digests"].(map[string]any)
	sourceDigests["adapter_input_digests"] = serverAdapterInputDigestPayloadForTest(sandboxContractPayload)
	dataVolumes := payload["data_volumes"].(map[string]any)
	serverApplyWorkspaceVolumePayload(dataVolumes["workspace"].(map[string]any), workspaceVolume)
	serverApplyDriverHomeVolumePayload(dataVolumes["agent_home"].(map[string]any), driverHomeVolume)
	mounts := payload["mounts"].(map[string]any)
	mounts["workspace"].(map[string]any)["source"] = workspaceVolume.HostPath
	mounts["agent_home"].(map[string]any)["source"] = driverHomeVolume.HostPath
	mounts["control"].(map[string]any)["source"] = details.ControlDirPath
	mounts["bridge"].(map[string]any)["source"] = details.BridgeDirPath
	if strings.TrimSpace(details.NetworkHostsPath) == "" {
		mounts["network_hosts_path"] = nil
	} else {
		mounts["network_hosts_path"] = details.NetworkHostsPath
	}
	payload["feature_policy"] = featurePolicyPayload
	plan, err := st.StoreGenerationPlan(ctx, store.StoreGenerationPlanParams{
		GenerationID: generationID,
		Payload:      payload,
	})
	if err != nil {
		t.Fatalf("store generation plan for %s: %v", generationID, err)
	}
	projections := append([]store.GenerationPlanProjectionExpectation{{
		ProjectionKind:    store.GenerationPlanProjectionSandboxContract,
		ProjectionVersion: store.GenerationPlanProjectionVersion,
		PayloadDigest:     sandboxContractDigest,
	}}, planprojection.ExpectationsForDetails(details, artifacts)...)
	for _, projection := range projections {
		if _, err := st.StoreGenerationPlanProjection(ctx, store.StoreGenerationPlanProjectionParams{
			GenerationID:      generationID,
			PlanDigest:        plan.PlanDigest,
			ProjectionKind:    projection.ProjectionKind,
			ProjectionVersion: projection.ProjectionVersion,
			PayloadDigest:     projection.PayloadDigest,
			MaterializedPath:  projection.MaterializedPath,
		}); err != nil {
			t.Fatalf("store generation plan projection %s for %s: %v", projection.ProjectionKind, generationID, err)
		}
	}
	return plan
}

func serverRuntimeResourceIdentityDigestForPlanFixture(t *testing.T, details store.RuntimeGenerationDetails, artifacts runtime.GenerationArtifacts) string {
	t.Helper()
	sandboxIP, err := runtimeResourceSandboxIP(details.SandboxIPCIDR)
	if err != nil {
		t.Fatalf("runtime resource sandbox ip for plan %s: %v", details.GenerationID, err)
	}
	nftTableName, err := runtimeResourceNftTableName(details.GenerationID)
	if err != nil {
		t.Fatalf("runtime resource nft table for plan %s: %v", details.GenerationID, err)
	}
	params := store.RuntimeResourceInstanceParams{
		GenerationID:           details.GenerationID,
		SessionID:              details.SessionID,
		ContractID:             sandboxContractID(details.GenerationID),
		SandboxContractVersion: store.SandboxContractVersion,
		HostID:                 mustRuntimeResourceHostID(t),
		RunscContainerID:       details.RunscContainerID,
		RunscPlatform:          details.RunscPlatform,
		RunscVersion:           artifacts.RunscVersion,
		RunscBinaryPath:        artifacts.RunscBinaryPath,
		RunscBinaryDigest:      artifacts.RunscBinaryDigest,
		NetworkProfileID:       details.NetworkProfileID,
		NetnsName:              details.NetnsName,
		NetnsPath:              details.NetnsPath,
		HostVeth:               details.HostVeth,
		SandboxVeth:            details.SandboxVeth,
		HostGatewayIP:          details.HostGatewayIP,
		SandboxIP:              sandboxIP,
		SandboxIPCIDR:          details.SandboxIPCIDR,
		HostSideCIDR:           details.HostSideCIDR,
		NftTableName:           nftTableName,
		ControlDirPath:         details.ControlDirPath,
		ControlManifestPath:    details.ControlManifestPath,
		BundleDirPath:          details.BundleDirPath,
		SpecPath:               details.SpecPath,
		CheckpointPath:         details.CheckpointPath,
		BridgeDirPath:          details.BridgeDirPath,
		NetworkHostsPath:       details.NetworkHostsPath,
		LogDirPath:             details.LogDirPath,
		RootPrefixes: map[string]string{
			"run_dir":      filepath.Dir(filepath.Dir(details.ControlDirPath)),
			"control_root": filepath.Dir(details.ControlDirPath),
			"bridge_root":  filepath.Dir(details.BridgeDirPath),
		},
	}
	_, digest, err := store.RuntimeResourceIdentityForParams(params)
	if err != nil {
		t.Fatalf("runtime resource identity for plan %s: %v", details.GenerationID, err)
	}
	return digest
}

func provisionServerGenerationPlanFixtureVolumes(t *testing.T, ctx context.Context, st *store.Store, sessionID string, details store.RuntimeGenerationDetails) (store.SessionWorkspaceVolume, store.SessionDriverHomeVolume) {
	t.Helper()
	cfg := serverGenerationPlanFixtureVolumeConfig(t, details)
	now := time.Now().UTC()
	workspace, err := st.ProvisionSessionWorkspace(ctx, store.ProvisionSessionWorkspaceParams{
		SessionID: sessionID,
		Config:    cfg,
		Now:       now,
	})
	if err != nil {
		t.Fatalf("provision workspace volume for plan %s: %v", details.GenerationID, err)
	}
	driverHome, err := st.ProvisionSessionDriverHome(ctx, store.ProvisionSessionDriverHomeParams{
		SessionID: sessionID,
		Driver:    details.DriverID,
		Config:    cfg,
		Now:       now,
	})
	if err != nil {
		t.Fatalf("provision driver-home volume for plan %s: %v", details.GenerationID, err)
	}
	return workspace, driverHome
}

func serverGenerationPlanFixtureVolumeConfig(t *testing.T, details store.RuntimeGenerationDetails) store.DataVolumeProvisionerConfig {
	t.Helper()
	controlDir := strings.TrimSpace(details.ControlDirPath)
	if controlDir == "" {
		t.Fatalf("generation %s control dir path is required for plan fixture volumes", details.GenerationID)
	}
	runDir := filepath.Dir(filepath.Dir(controlDir))
	fixtureRoot := filepath.Dir(runDir)
	if runDir == "." || fixtureRoot == "." {
		t.Fatalf("generation %s control dir path %q cannot derive plan fixture roots", details.GenerationID, controlDir)
	}
	return store.DataVolumeProvisionerConfig{
		SessionsRoot:   filepath.Join(fixtureRoot, "sessions"),
		AgentHomesRoot: filepath.Join(fixtureRoot, "agent-homes"),
		EvidenceRoot:   filepath.Join(fixtureRoot, "state", "volume-evidence"),
		LayoutVersion:  store.DataVolumeLayoutVersion,
		RuntimeIdentity: store.RuntimeIdentity{
			UID: serverTestSandboxUID(),
			GID: serverTestSandboxGID(),
		},
	}
}

func serverApplyWorkspaceVolumePayload(payload map[string]any, volume store.SessionWorkspaceVolume) {
	payload["session_id"] = volume.SessionID
	payload["host_path"] = volume.HostPath
	payload["layout_version"] = volume.LayoutVersion
	payload["runtime_identity_digest"] = volume.RuntimeIdentityDigest
	payload["provisioning_marker_path"] = volume.ProvisioningMarkerPath
	payload["provisioning_marker_digest"] = volume.ProvisioningMarkerDigest
	payload["sandbox_uid"] = volume.SandboxUID
	payload["sandbox_gid"] = volume.SandboxGID
	payload["sandbox_supplemental_gids"] = append([]int(nil), volume.SandboxSupplementalGIDs...)
}

func serverApplyDriverHomeVolumePayload(payload map[string]any, volume store.SessionDriverHomeVolume) {
	payload["session_id"] = volume.SessionID
	payload["driver"] = volume.Driver
	payload["host_path"] = volume.HostPath
	payload["layout_version"] = volume.LayoutVersion
	payload["runtime_identity_digest"] = volume.RuntimeIdentityDigest
	payload["provisioning_marker_path"] = volume.ProvisioningMarkerPath
	payload["provisioning_marker_digest"] = volume.ProvisioningMarkerDigest
	payload["sandbox_uid"] = volume.SandboxUID
	payload["sandbox_gid"] = volume.SandboxGID
	payload["sandbox_supplemental_gids"] = append([]int(nil), volume.SandboxSupplementalGIDs...)
}

func serverPlanVolumePayload(hostPath, markerPath, destination string) map[string]any {
	return map[string]any{
		"session_id": "sess_frozen_evidence", "host_path": hostPath, "layout_version": 1,
		"runtime_identity_digest": "sha256:identity", "provisioning_marker_path": markerPath,
		"provisioning_marker_digest": "sha256:marker", "sandbox_destination": destination,
		"sandbox_uid": 65534, "sandbox_gid": 65534, "sandbox_supplemental_gids": []int{},
	}
}

func serverGenerationPlanFrozenEvidenceDetails() store.RuntimeGenerationDetails {
	return store.RuntimeGenerationDetails{
		GenerationID:                    "gen_frozen_evidence",
		SessionID:                       "sess_frozen_evidence",
		RunscPlatform:                   "systrap",
		CheckpointBundleDigest:          "sha256:bundle",
		CheckpointRuntimeConfigDigest:   "sha256:runtime-config",
		CheckpointControlManifestDigest: "sha256:control-manifest-projected",
		CheckpointDriverStatesDigest:    "sha256:driver-state-fence",
		CheckpointPlanDigest:            store.GenerationPlanDigest(mustServerFrozenEvidenceCanonicalPayload()),
	}
}

func serverGenerationPlanFrozenEvidenceArtifacts() runtime.GenerationArtifacts {
	return runtime.GenerationArtifacts{
		ManifestDigest:          "sha256:control-manifest",
		ProjectedManifestDigest: "sha256:control-manifest-projected",
		BundleDigest:            "sha256:bundle",
		RuntimeConfigDigest:     "sha256:runtime-config",
		SpecDigest:              "sha256:oci-spec",
		RunscVersion:            "runsc test",
		RunscBinaryPath:         "/usr/local/bin/runsc-test",
		RunscBinaryDigest:       "sha256:runsc",
	}
}

func createServerRuntimeResourceLive(t *testing.T, ctx context.Context, st *store.Store, sessionID string, allocation store.GenerationAllocation, ownerUUID, hostID string, now time.Time) store.RuntimeResourceInstance {
	t.Helper()
	contractID := sandboxContractID(allocation.GenerationID)
	details, err := st.GetRuntimeGenerationDetails(ctx, sessionID, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get generation details: %v", err)
	}
	prefix, err := netip.ParsePrefix(details.SandboxIPCIDR)
	if err != nil {
		t.Fatalf("parse sandbox cidr: %v", err)
	}
	if _, err := st.StoreSandboxContract(ctx, store.StoreSandboxContractParams{
		ContractID:             contractID,
		SessionID:              sessionID,
		GenerationID:           allocation.GenerationID,
		SandboxContractVersion: store.SandboxContractVersion,
		ContractSchemaVersion:  store.SandboxContractSchemaVersion,
		ContractGateVersion:    store.SandboxContractGateDriverManifest,
		Payload:                serverRuntimeResourceSandboxContractPayloadForTest(t, details, allocation, contractID),
		Now:                    now,
	}); err != nil {
		t.Fatalf("store sandbox contract: %v", err)
	}
	storeServerSandboxContractInputEvidenceFromGenerationPlanIfPresent(t, ctx, st, allocation.GenerationID)
	artifacts := testGenerationArtifacts()
	if strings.TrimSpace(details.RunscVersion) != "" {
		artifacts.RunscVersion = details.RunscVersion
	}
	if strings.TrimSpace(details.RunscBinaryPath) != "" {
		artifacts.RunscBinaryPath = details.RunscBinaryPath
	}
	if strings.TrimSpace(details.RunscBinaryDigest) != "" {
		artifacts.RunscBinaryDigest = details.RunscBinaryDigest
	}
	instance, err := st.CreateRuntimeResourceInstance(ctx, store.RuntimeResourceInstanceParams{
		GenerationID:           allocation.GenerationID,
		SessionID:              sessionID,
		ContractID:             contractID,
		SandboxContractVersion: store.SandboxContractVersion,
		HostID:                 hostID,
		RunscContainerID:       details.RunscContainerID,
		RunscPlatform:          "systrap",
		RunscVersion:           artifacts.RunscVersion,
		RunscBinaryPath:        artifacts.RunscBinaryPath,
		RunscBinaryDigest:      artifacts.RunscBinaryDigest,
		NetworkProfileID:       allocation.NetworkProfileID,
		NetnsName:              details.NetnsName,
		NetnsPath:              details.NetnsPath,
		HostVeth:               details.HostVeth,
		SandboxVeth:            details.SandboxVeth,
		HostGatewayIP:          details.HostGatewayIP,
		SandboxIP:              prefix.Addr().String(),
		SandboxIPCIDR:          details.SandboxIPCIDR,
		HostSideCIDR:           details.HostSideCIDR,
		NftTableName:           mustRuntimeResourceNftTableName(t, allocation.GenerationID),
		ControlDirPath:         details.ControlDirPath,
		ControlManifestPath:    details.ControlManifestPath,
		BundleDirPath:          details.BundleDirPath,
		SpecPath:               details.SpecPath,
		CheckpointPath:         details.CheckpointPath,
		BridgeDirPath:          details.BridgeDirPath,
		NetworkHostsPath:       details.NetworkHostsPath,
		LogDirPath:             details.LogDirPath,
		RootPrefixes: map[string]string{
			"run_dir":      filepath.Dir(filepath.Dir(details.ControlDirPath)),
			"control_root": filepath.Dir(details.ControlDirPath),
			"bridge_root":  filepath.Dir(details.BridgeDirPath),
		},
		Now: now,
	})
	if err != nil {
		t.Fatalf("create runtime resource instance: %v", err)
	}
	workerID := strings.TrimSpace(ownerUUID)
	if workerID == "" {
		workerID = strings.TrimSuffix(strings.TrimSpace(allocation.Owner), ":"+store.RuntimeManagerRoleTag)
	}
	if err := st.ClaimRuntimeResourceMaterialization(ctx, store.RuntimeResourceMaterializationClaimParams{
		GenerationID:     allocation.GenerationID,
		WorkerID:         workerID,
		HostID:           hostID,
		LeaseExpiresAt:   now.Add(time.Minute),
		IdempotencyToken: "test:" + allocation.GenerationID,
		Now:              now.Add(time.Millisecond),
	}); err != nil {
		t.Fatalf("claim runtime resource materialization: %v", err)
	}
	if err := st.MarkRuntimeResourceReady(ctx, store.RuntimeResourceWorkerTransitionParams{
		GenerationID: allocation.GenerationID,
		WorkerID:     workerID,
		HostID:       hostID,
		Now:          now.Add(2 * time.Millisecond),
	}); err != nil {
		t.Fatalf("mark runtime resource ready: %v", err)
	}
	if err := st.MarkRuntimeResourceLive(ctx, store.RuntimeResourceWorkerTransitionParams{
		GenerationID: allocation.GenerationID,
		WorkerID:     workerID,
		HostID:       hostID,
		PostStart:    serverPostStartProofForTest(instance),
		Now:          now.Add(3 * time.Millisecond),
	}); err != nil {
		t.Fatalf("mark runtime resource live: %v", err)
	}
	return instance
}
