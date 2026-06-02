package planprojection

import (
	"fmt"
	"net/netip"
	"path/filepath"
	"strings"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/bridge"
	"harness-platform/orchestrator/internal/runtime"
	"harness-platform/orchestrator/internal/store"
)

type DataVolumes struct {
	Workspace  store.SessionWorkspaceVolume
	DriverHome store.SessionDriverHomeVolume
}

func DriverConfigMaterializationPayload(driverID string, entries []runtime.DriverConfigMaterialization) (map[string]any, map[string]any, error) {
	driverID = strings.TrimSpace(driverID)
	specs := agents.DriverConfigMaterializationSpecsFor(agents.ID(driverID))
	if len(specs) == 0 {
		if len(entries) != 0 {
			return nil, nil, fmt.Errorf("driver %s does not support driver config materialization", driverID)
		}
		return map[string]any{}, nil, nil
	}
	if len(entries) != len(specs) {
		return nil, nil, fmt.Errorf("driver %s config materialization requires %d projections", driverID, len(specs))
	}
	runtimePayload := map[string]any{}
	mountPayload := map[string]any{}
	expected := map[string]agents.DriverConfigMaterializationSpec{}
	for _, spec := range specs {
		expected[spec.Name] = spec
	}
	seen := map[string]struct{}{}
	for _, entry := range entries {
		name := strings.TrimSpace(entry.Name)
		want, ok := expected[name]
		if !ok {
			return nil, nil, fmt.Errorf("unsupported %s driver config materialization %q", driverID, entry.Name)
		}
		if _, ok := seen[name]; ok {
			return nil, nil, fmt.Errorf("duplicate %s driver config materialization %q", driverID, name)
		}
		seen[name] = struct{}{}
		if entry.SourceProjectionPath != want.SourceProjectionPath || entry.SandboxDestination != want.SandboxDestination {
			return nil, nil, fmt.Errorf("%s driver config materialization %s path mismatch", driverID, name)
		}
		if !strings.HasPrefix(strings.TrimSpace(entry.SourceDigest), "sha256:") {
			return nil, nil, fmt.Errorf("%s driver config materialization %s digest is required", driverID, name)
		}
		if entry.DestinationMutableBySandbox != want.DestinationMutableBySandbox {
			return nil, nil, fmt.Errorf("%s driver config materialization %s mutability mismatch", driverID, name)
		}
		runtimePayload[name] = map[string]any{
			"source_projection_path":         entry.SourceProjectionPath,
			"source_digest":                  entry.SourceDigest,
			"sandbox_destination":            entry.SandboxDestination,
			"destination_mutable_by_sandbox": entry.DestinationMutableBySandbox,
		}
		mountPayload[name] = map[string]any{
			"type":                           want.MountType,
			"mode":                           want.MountMode,
			"exact":                          want.MountExact,
			"source_projection_path":         entry.SourceProjectionPath,
			"sandbox_destination":            entry.SandboxDestination,
			"destination_mutable_by_sandbox": entry.DestinationMutableBySandbox,
		}
	}
	if len(seen) != len(expected) {
		return nil, nil, fmt.Errorf("%s driver config materialization missing required projections", driverID)
	}
	return runtimePayload, mountPayload, nil
}

func SandboxContractDigestForPayload(value any) (string, error) {
	payload, err := store.CanonicalSandboxContractPayload(value)
	if err != nil {
		return "", err
	}
	return store.SandboxContractDigest(payload), nil
}

func runtimeResourceSandboxIP(cidr string) (string, error) {
	prefix, err := netip.ParsePrefix(strings.TrimSpace(cidr))
	if err != nil {
		return "", fmt.Errorf("sandbox ip cidr is invalid: %w", err)
	}
	return prefix.Addr().String(), nil
}

func evidenceRoot(markerPath string, parents int) string {
	path := strings.TrimSpace(markerPath)
	for range parents {
		path = filepath.Dir(path)
	}
	return path
}

type SandboxContractInputDigests struct {
	RuntimeConfigDigest string
	AgentManifestDigest string
}

type SandboxContractParams struct {
	Session                     store.Session
	Details                     store.RuntimeGenerationDetails
	Artifacts                   runtime.GenerationArtifacts
	ResourceIdentityDigest      string
	NetworkIdentityNftTableName string
	Volumes                     DataVolumes
	DriverSpec                  agents.DriverSpec
	ProviderSpec                agents.RuntimeProviderSpec
	InputDigests                SandboxContractInputDigests
	ContentSnapshots            []store.ContentSnapshotRecord
}

func RenderSandboxContract(p SandboxContractParams) (map[string]any, error) {
	details := p.Details
	runscPlatform := strings.TrimSpace(details.RunscPlatform)
	if runscPlatform == "" {
		return nil, fmt.Errorf("runsc platform is required")
	}
	driverID := strings.TrimSpace(details.DriverID)
	if driverID == "" || p.DriverSpec.ID != agents.ID(driverID) {
		return nil, fmt.Errorf("unsupported driver %q", driverID)
	}
	mode := strings.TrimSpace(p.Session.Mode)
	if mode == "" {
		return nil, fmt.Errorf("session mode is required")
	}
	initialDriverStateDigest := strings.TrimSpace(details.DriverStateDigest)
	if initialDriverStateDigest == "" {
		return nil, fmt.Errorf("initial driver state digest is required")
	}
	sandboxIP, err := runtimeResourceSandboxIP(details.SandboxIPCIDR)
	if err != nil {
		return nil, err
	}
	nftTableName := strings.TrimSpace(p.NetworkIdentityNftTableName)
	if nftTableName == "" {
		return nil, fmt.Errorf("network identity nft table name is required")
	}
	var sandboxModelProxyBaseURL any
	if value := strings.TrimSpace(details.ManifestAnthropicBaseURL); value != "" {
		sandboxModelProxyBaseURL = value
	}
	mountPlan := map[string]any{
		"workspace":  map[string]any{"source": p.Volumes.Workspace.HostPath, "destination": "/workspace", "mode": "rw"},
		"agent_home": map[string]any{"source": p.Volumes.DriverHome.HostPath, "destination": "/agent-home", "mode": "rw"},
		"control":    map[string]any{"source": details.ControlDirPath, "destination": "/harness-control", "mode": "ro"},
		"bridge":     map[string]any{"source": details.BridgeDirPath, "destination": "/harness-control/bridge", "mode": "rw"},
	}
	if strings.TrimSpace(details.NetworkHostsPath) != "" {
		mountPlan["network_hosts"] = map[string]any{"source": details.NetworkHostsPath, "destination": "/etc/hosts", "mode": "ro"}
	}
	contentSnapshotMounts, err := ContentSnapshotMountPayload(p.ContentSnapshots)
	if err != nil {
		return nil, err
	}
	if len(contentSnapshotMounts) > 0 {
		mountPlan["content_snapshots"] = contentSnapshotMounts
	}
	materializedDriverConfig, mountMaterializations, err := DriverConfigMaterializationPayload(driverID, p.Artifacts.MaterializedDriverConfig)
	if err != nil {
		return nil, err
	}
	if len(mountMaterializations) > 0 {
		mountPlan["driver_config_materializations"] = mountMaterializations
	}
	driverConfigPreimage := map[string]any{
		"driver_id":     driverID,
		"model":         details.Model,
		"output_format": details.OutputFormat,
	}
	if len(materializedDriverConfig) > 0 {
		driverConfigPreimage["materialized_driver_config"] = materializedDriverConfig
	}
	driverConfigDigest, err := SandboxContractDigestForPayload(driverConfigPreimage)
	if err != nil {
		return nil, fmt.Errorf("driver config digest: %w", err)
	}
	commandDigest, err := SandboxContractDigestForPayload(map[string]any{
		"driver_id":    driverID,
		"protocol":     details.OutputFormat,
		"resume_field": "driver_state",
	})
	if err != nil {
		return nil, fmt.Errorf("command digest: %w", err)
	}
	driverCapabilitiesDigest, err := SandboxContractDigestForPayload(map[string]any{
		"driver_id":     driverID,
		"capabilities":  p.DriverSpec.RequiredRuntimeCapabilities,
		"registry_kind": string(p.DriverSpec.Kind),
	})
	if err != nil {
		return nil, fmt.Errorf("driver capabilities digest: %w", err)
	}
	providerCapabilitiesDigest := agents.CapabilityDigest(p.ProviderSpec)
	runtimeTemplateDigest, err := SandboxContractDigestForPayload(map[string]any{
		"provider_id":          p.ProviderSpec.ID,
		"runsc_platform":       runscPlatform,
		"runsc_overlay2":       details.RunscOverlay2,
		"no_new_privileges":    true,
		"ambient_capabilities": []string{},
	})
	if err != nil {
		return nil, fmt.Errorf("runtime template digest: %w", err)
	}
	secretGrants := []map[string]any{}
	if details.ModelAccessAllowed {
		secretGrants = append(secretGrants, map[string]any{
			"grant_id":                  "model_provider:anthropic_proxy",
			"domain":                    "model_provider",
			"scope":                     "anthropic_messages",
			"exposure_mode":             "proxy_only",
			"ttl_seconds":               nil,
			"allowed_drivers":           []string{driverID},
			"allowed_runtime_providers": []string{p.ProviderSpec.ID},
		})
	}
	credentialPreimage := map[string]any{
		"provider_credentials": "host-only",
		"sandbox_secret_mount": "absent",
		"proxy_token":          "absent",
		"secret_grants":        secretGrants,
	}
	credentialDigest, err := store.CredentialPolicyDigest(credentialPreimage)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"sandbox_contract_version": store.SandboxContractVersion,
		"contract_schema_version":  store.SandboxContractSchemaVersion,
		"contract_gate_version":    store.SandboxContractGateDriverManifest,
		"contract_id":              "contract_" + strings.TrimSpace(details.GenerationID),
		"session_id":               details.SessionID,
		"generation_id":            details.GenerationID,
		"driver": map[string]any{
			"driver_id":                            driverID,
			"driver_version":                       "bundled",
			"bridge_protocol":                      p.DriverSpec.BridgeProtocol,
			"bridge_protocol_version":              p.DriverSpec.BridgeProtocolVersion,
			"turn_input_schema":                    p.DriverSpec.TurnInputSchema,
			"output_schema":                        p.DriverSpec.OutputSchema,
			"command_argv_digest":                  commandDigest,
			"driver_config_digest":                 driverConfigDigest,
			"required_runtime_capabilities_digest": driverCapabilitiesDigest,
			"supports_interrupt":                   p.DriverSpec.SupportsInterrupt,
			"supports_compaction":                  p.DriverSpec.SupportsCompaction,
		},
		"runtime_provider": map[string]any{
			"provider_id":              p.ProviderSpec.ID,
			"provider_profile_id":      p.ProviderSpec.ProviderProfileID,
			"isolation_kind":           p.ProviderSpec.IsolationKind,
			"template_ref":             p.ProviderSpec.TemplateRef,
			"template_digest":          runtimeTemplateDigest,
			"capability_vocab_version": p.ProviderSpec.CapabilityVocabulary,
			"capability_digest":        providerCapabilitiesDigest,
			"provider_specific": map[string]any{
				"runsc_container_id":   details.RunscContainerID,
				"runsc_platform":       runscPlatform,
				"runsc_version":        p.Artifacts.RunscVersion,
				"runsc_binary_path":    p.Artifacts.RunscBinaryPath,
				"runsc_binary_digest":  p.Artifacts.RunscBinaryDigest,
				"runsc_overlay2":       details.RunscOverlay2,
				"no_new_privileges":    true,
				"ambient_capabilities": []string{},
				"required_annotations": map[string]any{
					bridge.BridgeMountDestination: map[string]string{
						"dev.gvisor.spec.mount./harness-control/bridge.type":  "bind",
						"dev.gvisor.spec.mount./harness-control/bridge.share": "exclusive",
					},
				},
			},
		},
		"runtime_profile_id": details.AgentRuntimeProfileID,
		"network_profile_id": details.NetworkProfileID,
		"identity": map[string]any{
			"sandbox_uid":               details.SandboxUID,
			"sandbox_gid":               details.SandboxGID,
			"sandbox_supplemental_gids": append([]int(nil), details.SandboxSupplementalGIDs...),
			"model_access_allowed":      details.ModelAccessAllowed,
		},
		"mount_plan": mountPlan,
		"network_identity": map[string]any{
			"runsc_network":    details.RunscNetwork,
			"sandbox_ip":       sandboxIP,
			"sandbox_ip_cidr":  details.SandboxIPCIDR,
			"host_gateway_ip":  details.HostGatewayIP,
			"netns_name":       details.NetnsName,
			"netns_path":       details.NetnsPath,
			"host_veth":        details.HostVeth,
			"sandbox_veth":     details.SandboxVeth,
			"host_side_cidr":   details.HostSideCIDR,
			"nft_table_name":   nftTableName,
			"egress_policy_id": details.EgressPolicyID,
		},
		"runtime_adapter": map[string]any{
			"kind":                 "runsc",
			"runsc_platform":       runscPlatform,
			"runsc_version":        p.Artifacts.RunscVersion,
			"runsc_binary_path":    p.Artifacts.RunscBinaryPath,
			"runsc_binary_digest":  p.Artifacts.RunscBinaryDigest,
			"runsc_container_id":   details.RunscContainerID,
			"runsc_network":        details.RunscNetwork,
			"runsc_overlay2":       details.RunscOverlay2,
			"no_new_privileges":    true,
			"ambient_capabilities": []string{},
			"forbidden_capabilities": []string{
				"CAP_NET_ADMIN",
				"CAP_NET_RAW",
				"CAP_SYS_ADMIN",
			},
			"required_annotations": map[string]any{
				bridge.BridgeMountDestination: map[string]string{
					"dev.gvisor.spec.mount./harness-control/bridge.type":  "bind",
					"dev.gvisor.spec.mount./harness-control/bridge.share": "exclusive",
				},
			},
		},
		"resource_identity": map[string]any{
			"resource_identity_digest": p.ResourceIdentityDigest,
		},
		"data_volumes": map[string]any{
			"workspace": map[string]any{
				"table":                      "session_workspaces",
				"session_id":                 p.Volumes.Workspace.SessionID,
				"host_path":                  p.Volumes.Workspace.HostPath,
				"layout_version":             p.Volumes.Workspace.LayoutVersion,
				"runtime_identity_digest":    p.Volumes.Workspace.RuntimeIdentityDigest,
				"provisioning_marker_path":   p.Volumes.Workspace.ProvisioningMarkerPath,
				"provisioning_marker_digest": p.Volumes.Workspace.ProvisioningMarkerDigest,
				"sandbox_destination":        "/workspace",
				"provisioning_evidence_root": evidenceRoot(p.Volumes.Workspace.ProvisioningMarkerPath, 2),
			},
			"agent_home": map[string]any{
				"table":                      "session_driver_homes",
				"session_id":                 p.Volumes.DriverHome.SessionID,
				"driver":                     p.Volumes.DriverHome.Driver,
				"driver_home_key":            p.Volumes.DriverHome.Driver,
				"host_path":                  p.Volumes.DriverHome.HostPath,
				"layout_version":             p.Volumes.DriverHome.LayoutVersion,
				"runtime_identity_digest":    p.Volumes.DriverHome.RuntimeIdentityDigest,
				"provisioning_marker_path":   p.Volumes.DriverHome.ProvisioningMarkerPath,
				"provisioning_marker_digest": p.Volumes.DriverHome.ProvisioningMarkerDigest,
				"sandbox_destination":        "/agent-home",
				"provisioning_evidence_root": evidenceRoot(p.Volumes.DriverHome.ProvisioningMarkerPath, 3),
			},
		},
		"credential_policy": map[string]any{
			"provider_credentials": "host-only",
			"sandbox_secret_mount": "absent",
			"proxy_token":          "absent",
			"digest":               credentialDigest,
			"secret_grants":        secretGrants,
		},
		"model_access": map[string]any{
			"model_access_allowed":         details.ModelAccessAllowed,
			"active_turn_required":         true,
			"provider_protocol":            "anthropic_messages",
			"sandbox_model_proxy_base_url": sandboxModelProxyBaseURL,
		},
		"snapshot_policy": map[string]any{
			"provider_supports_snapshot_disk":   p.ProviderSpec.SnapshotPolicy.ProviderSupportsSnapshotDisk,
			"provider_supports_snapshot_memory": p.ProviderSpec.SnapshotPolicy.ProviderSupportsSnapshotMemory,
			"provider_supports_branch":          p.ProviderSpec.SnapshotPolicy.ProviderSupportsBranch,
			"branch_count_limit":                p.ProviderSpec.SnapshotPolicy.BranchCountLimit,
			"must_quiesce_processes":            p.ProviderSpec.SnapshotPolicy.MustQuiesceProcesses,
			"stream_disconnects_on_snapshot":    p.ProviderSpec.SnapshotPolicy.StreamDisconnectsOnSnapshot,
			"snapshot_semantic":                 p.ProviderSpec.SnapshotPolicy.SnapshotSemantic,
		},
		"driver_runtime": map[string]any{
			"driver_home_mount":             "/agent-home",
			"generated_driver_config_mount": "/harness-control/driver/" + driverID,
			"materialized_driver_config":    materializedDriverConfig,
			"initial_driver_state_digest":   initialDriverStateDigest,
		},
		"input_digests": map[string]any{
			"runtime_config_digest": p.InputDigests.RuntimeConfigDigest,
			"rootfs_image_digest":   nil,
			"agent_manifest_digest": p.InputDigests.AgentManifestDigest,
		},
	}, nil
}

func ContentSnapshotMountPayload(records []store.ContentSnapshotRecord) (map[string]any, error) {
	out := map[string]any{}
	seen := map[string]struct{}{}
	for _, record := range records {
		kind := strings.TrimSpace(record.Kind)
		switch kind {
		case store.ContentSnapshotKindSkills:
		case "":
			return nil, fmt.Errorf("content snapshot kind is required")
		default:
			return nil, fmt.Errorf("unsupported content snapshot kind %q for sandbox contract mount plan", record.Kind)
		}
		if _, ok := seen[kind]; ok {
			return nil, fmt.Errorf("duplicate content snapshot kind %q for sandbox contract mount plan", kind)
		}
		seen[kind] = struct{}{}
		if strings.TrimSpace(record.Digest) == "" || !strings.HasPrefix(strings.TrimSpace(record.Digest), "sha256:") {
			return nil, fmt.Errorf("content snapshot %s digest is required", kind)
		}
		source := strings.TrimSpace(record.ImmutableHostPath)
		if source == "" || !filepath.IsAbs(source) {
			return nil, fmt.Errorf("content snapshot %s immutable host path must be absolute", kind)
		}
		destination := strings.TrimSpace(record.MountDestination)
		if destination != store.ContentSnapshotSkillsMount {
			return nil, fmt.Errorf("content snapshot %s mount destination must be %s", kind, store.ContentSnapshotSkillsMount)
		}
		if strings.TrimSpace(record.SourceEvidenceDigest) == "" || !strings.HasPrefix(strings.TrimSpace(record.SourceEvidenceDigest), "sha256:") {
			return nil, fmt.Errorf("content snapshot %s source evidence digest is required", kind)
		}
		if strings.TrimSpace(record.RetentionClass) == "" {
			return nil, fmt.Errorf("content snapshot %s retention class is required", kind)
		}
		out[kind] = map[string]any{
			"mount_name":             "skills_snapshot",
			"type":                   "bind",
			"mode":                   "ro",
			"exact":                  true,
			"source":                 filepath.Clean(source),
			"destination":            destination,
			"digest":                 strings.TrimSpace(record.Digest),
			"source_evidence_digest": strings.TrimSpace(record.SourceEvidenceDigest),
			"retention_class":        strings.TrimSpace(record.RetentionClass),
		}
	}
	return out, nil
}
