package generationplan

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/runtime"
	"harness-platform/orchestrator/internal/store"
)

type BridgeProbePayload struct {
	BridgeHeartbeatInterval time.Duration
	BridgePollInterval      time.Duration
	LeaseTTL                time.Duration
	AckStartedGrace         time.Duration
	ReconnectGrace          time.Duration
	ProbeHealthzStatuses    []int
	PreStartAttempts        int
	PreStartInterval        time.Duration
	PostStartAttempts       int
	PostStartInterval       time.Duration
}

type DataVolumes struct {
	Workspace  store.SessionWorkspaceVolume
	DriverHome store.SessionDriverHomeVolume
}

type SourceDigests struct {
	RuntimeConfigDigest string
	AgentManifestDigest string
}

type RenderPayloadParams struct {
	Session                      store.Session
	Details                      store.RuntimeGenerationDetails
	Artifacts                    runtime.GenerationArtifacts
	SandboxContractPayload       map[string]any
	SandboxContractPayloadDigest string
	ResourceIdentityDigest       string
	Volumes                      DataVolumes
	DriverSpec                   agents.DriverSpec
	ProviderSpec                 agents.RuntimeProviderSpec
	RuntimeProviderConfigID      string
	RootFSPath                   string
	SandboxIP                    string
	NetworkIdentityNftTableName  string
	BridgeProbe                  BridgeProbePayload
	FeaturePolicy                map[string]any
	ContentSnapshots             []store.ContentSnapshotRecord
	SourceDigests                SourceDigests
	SandboxContractCompatibility string
	SandboxContractID            string
	WorkspaceDestination         string
	DriverHomeDestination        string
	ArtifactWatcherScope         string
	PlatformContentMountScope    string
	MutableLeasesScope           string
	MutableEventsScope           string
	MutableCheckpointStateScope  string
}

func RenderPayload(p RenderPayloadParams) (map[string]any, error) {
	details := p.Details
	driverID := strings.TrimSpace(details.DriverID)
	if driverID == "" {
		return nil, fmt.Errorf("generation plan driver id is required")
	}
	if p.DriverSpec.ID != agents.ID(driverID) {
		return nil, fmt.Errorf("unsupported driver %q", driverID)
	}
	mode := strings.TrimSpace(p.Session.Mode)
	if mode == "" {
		return nil, fmt.Errorf("session mode is required")
	}
	mountPlan, ok := p.SandboxContractPayload["mount_plan"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("sandbox contract mount_plan is required")
	}
	featurePolicy := p.FeaturePolicy
	if featurePolicy == nil {
		var err error
		featurePolicy, err = RenderFeaturePolicyPayload(p.DriverSpec, p.ProviderSpec)
		if err != nil {
			return nil, err
		}
	}
	sandboxContractPayloadDigest := strings.TrimSpace(p.SandboxContractPayloadDigest)
	if sandboxContractPayloadDigest == "" {
		sandboxContractPayloadDigest = strings.TrimSpace(stringValue(p.SandboxContractPayload["sandbox_contract_digest"]))
	}
	if sandboxContractPayloadDigest == "" {
		return nil, fmt.Errorf("sandbox contract payload digest is required")
	}
	sandboxContractCompatibility := strings.TrimSpace(p.SandboxContractCompatibility)
	if sandboxContractCompatibility == "" {
		sandboxContractCompatibility = store.SandboxContractVersion
	}
	sandboxContractID := strings.TrimSpace(p.SandboxContractID)
	if sandboxContractID == "" {
		sandboxContractID = "contract_" + strings.TrimSpace(details.GenerationID)
	}
	workspaceDestination := strings.TrimSpace(p.WorkspaceDestination)
	if workspaceDestination == "" {
		workspaceDestination = "/workspace"
	}
	driverHomeDestination := strings.TrimSpace(p.DriverHomeDestination)
	if driverHomeDestination == "" {
		driverHomeDestination = "/agent-home"
	}
	artifactWatcherScope := strings.TrimSpace(p.ArtifactWatcherScope)
	if artifactWatcherScope == "" {
		artifactWatcherScope = "workspace_only"
	}
	platformContentMountScope := strings.TrimSpace(p.PlatformContentMountScope)
	if platformContentMountScope == "" {
		if len(p.ContentSnapshots) > 0 {
			platformContentMountScope = "immutable_content_snapshots"
		} else {
			platformContentMountScope = "none"
		}
	}
	mutableLeasesScope := strings.TrimSpace(p.MutableLeasesScope)
	if mutableLeasesScope == "" {
		mutableLeasesScope = "runtime_generations"
	}
	mutableEventsScope := strings.TrimSpace(p.MutableEventsScope)
	if mutableEventsScope == "" {
		mutableEventsScope = "events"
	}
	mutableCheckpointStateScope := strings.TrimSpace(p.MutableCheckpointStateScope)
	if mutableCheckpointStateScope == "" {
		mutableCheckpointStateScope = "runtime_generations"
	}
	mountsPayload := map[string]any{
		"workspace":                      mountPlan["workspace"],
		"agent_home":                     mountPlan["agent_home"],
		"control":                        mountPlan["control"],
		"bridge":                         mountPlan["bridge"],
		"network_hosts_path":             nullablePath(details.NetworkHostsPath),
		"driver_config_materializations": mountPlan["driver_config_materializations"],
	}
	if contentSnapshotMounts, ok := mountPlan["content_snapshots"]; ok {
		mountsPayload["content_snapshots"] = contentSnapshotMounts
	}
	contentSnapshots, err := RenderContentSnapshotsPayload(p.ContentSnapshots)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"plan_version": store.GenerationPlanVersion,
		"identity": map[string]any{
			"session_id":    p.Session.ID,
			"generation_id": details.GenerationID,
			"product_mode":  mode,
		},
		"driver": map[string]any{
			"driver_id":               driverID,
			"driver_kind":             string(p.DriverSpec.Kind),
			"bridge_protocol":         p.DriverSpec.BridgeProtocol,
			"bridge_protocol_version": p.DriverSpec.BridgeProtocolVersion,
			"turn_input_schema":       p.DriverSpec.TurnInputSchema,
			"output_schema":           p.DriverSpec.OutputSchema,
			"output_format":           details.OutputFormat,
			"model":                   nullablePath(details.Model),
			"initial_state_digest":    details.DriverStateDigest,
			"initial_state_version":   details.DriverStateVersion,
			"capability_snapshot":     agents.DriverCapabilityPayload(p.DriverSpec),
		},
		"runtime_provider": map[string]any{
			"provider_id":                  p.ProviderSpec.ID,
			"provider_config_id":           p.RuntimeProviderConfigID,
			"provider_profile_id":          p.ProviderSpec.ProviderProfileID,
			"isolation_kind":               p.ProviderSpec.IsolationKind,
			"template_ref":                 p.ProviderSpec.TemplateRef,
			"capability_vocab_version":     p.ProviderSpec.CapabilityVocabulary,
			"capability_digest":            agents.CapabilityDigest(p.ProviderSpec),
			"capability_snapshot":          agents.RuntimeProviderCapabilityPayload(p.ProviderSpec),
			"snapshot_policy":              p.ProviderSpec.SnapshotPolicy,
			"agent_runtime_profile_id":     details.AgentRuntimeProfileID,
			"runtime_profile_provider_ref": details.RunscPlatform,
		},
		"runsc_pin": map[string]any{
			"platform":      details.RunscPlatform,
			"version":       p.Artifacts.RunscVersion,
			"binary_path":   p.Artifacts.RunscBinaryPath,
			"binary_digest": p.Artifacts.RunscBinaryDigest,
		},
		"image": map[string]any{
			"agent_manifest_digest": p.SourceDigests.AgentManifestDigest,
			"rootfs_path":           filepath.Clean(p.RootFSPath),
			"rootfs_image_digest":   nil,
		},
		"bridge_probe": map[string]any{
			"bridge_mode":               "claim-loop",
			"bridge_heartbeat_seconds":  durationSeconds(p.BridgeProbe.BridgeHeartbeatInterval),
			"bridge_poll_seconds":       durationSeconds(p.BridgeProbe.BridgePollInterval),
			"lease_ttl_seconds":         durationSeconds(p.BridgeProbe.LeaseTTL),
			"ack_started_grace_seconds": durationSeconds(p.BridgeProbe.AckStartedGrace),
			"reconnect_grace_seconds":   durationSeconds(p.BridgeProbe.ReconnectGrace),
			"probe_url":                 details.ProbeURL,
			"probe_healthz_statuses":    append([]int(nil), p.BridgeProbe.ProbeHealthzStatuses...),
			"pre_start_attempts":        p.BridgeProbe.PreStartAttempts,
			"pre_start_interval_secs":   durationSeconds(p.BridgeProbe.PreStartInterval),
			"post_start_attempts":       p.BridgeProbe.PostStartAttempts,
			"post_start_interval_secs":  durationSeconds(p.BridgeProbe.PostStartInterval),
		},
		"network": map[string]any{
			"network_profile_id":   details.NetworkProfileID,
			"runsc_network":        details.RunscNetwork,
			"runsc_overlay2":       details.RunscOverlay2,
			"sandbox_ip":           p.SandboxIP,
			"sandbox_ip_cidr":      details.SandboxIPCIDR,
			"host_gateway_ip":      details.HostGatewayIP,
			"sandbox_base_url":     details.SandboxBaseURL,
			"host_proxy_bind_url":  details.HostProxyBindURL,
			"proxy_port":           details.ProxyPort,
			"netns_name":           details.NetnsName,
			"netns_path":           details.NetnsPath,
			"host_veth":            details.HostVeth,
			"sandbox_veth":         details.SandboxVeth,
			"host_side_cidr":       details.HostSideCIDR,
			"nft_table_name":       p.NetworkIdentityNftTableName,
			"egress_policy_id":     details.EgressPolicyID,
			"egress_policy_digest": details.EgressPolicyDigest,
			"dns_policy":           details.DNSPolicy,
		},
		"data_volumes": map[string]any{
			"workspace": map[string]any{
				"session_id":                   p.Volumes.Workspace.SessionID,
				"host_path":                    p.Volumes.Workspace.HostPath,
				"layout_version":               p.Volumes.Workspace.LayoutVersion,
				"runtime_identity_digest":      p.Volumes.Workspace.RuntimeIdentityDigest,
				"provisioning_marker_path":     p.Volumes.Workspace.ProvisioningMarkerPath,
				"provisioning_marker_digest":   p.Volumes.Workspace.ProvisioningMarkerDigest,
				"sandbox_destination":          workspaceDestination,
				"sandbox_uid":                  p.Volumes.Workspace.SandboxUID,
				"sandbox_gid":                  p.Volumes.Workspace.SandboxGID,
				"sandbox_supplemental_gids":    append([]int(nil), p.Volumes.Workspace.SandboxSupplementalGIDs...),
				"artifact_watcher_scope":       artifactWatcherScope,
				"platform_content_mount_scope": platformContentMountScope,
			},
			"agent_home": map[string]any{
				"session_id":                 p.Volumes.DriverHome.SessionID,
				"driver":                     p.Volumes.DriverHome.Driver,
				"host_path":                  p.Volumes.DriverHome.HostPath,
				"layout_version":             p.Volumes.DriverHome.LayoutVersion,
				"runtime_identity_digest":    p.Volumes.DriverHome.RuntimeIdentityDigest,
				"provisioning_marker_path":   p.Volumes.DriverHome.ProvisioningMarkerPath,
				"provisioning_marker_digest": p.Volumes.DriverHome.ProvisioningMarkerDigest,
				"sandbox_destination":        driverHomeDestination,
				"sandbox_uid":                p.Volumes.DriverHome.SandboxUID,
				"sandbox_gid":                p.Volumes.DriverHome.SandboxGID,
				"sandbox_supplemental_gids":  append([]int(nil), p.Volumes.DriverHome.SandboxSupplementalGIDs...),
			},
		},
		"mounts": mountsPayload,
		"runtime_artifacts": map[string]any{
			"control_dir_path":                     details.ControlDirPath,
			"control_manifest_path":                details.ControlManifestPath,
			"control_manifest_digest":              p.Artifacts.ManifestDigest,
			"projected_control_manifest_digest":    p.Artifacts.ProjectedManifestDigest,
			"bundle_dir_path":                      details.BundleDirPath,
			"bundle_digest":                        p.Artifacts.BundleDigest,
			"runtime_config_digest":                p.Artifacts.RuntimeConfigDigest,
			"spec_path":                            details.SpecPath,
			"spec_digest":                          p.Artifacts.SpecDigest,
			"bridge_dir_path":                      details.BridgeDirPath,
			"log_dir_path":                         details.LogDirPath,
			"network_hosts_path":                   nullablePath(details.NetworkHostsPath),
			"materialized_driver_config":           MaterializedDriverConfigPayload(p.Artifacts.MaterializedDriverConfig),
			"resource_identity_digest":             p.ResourceIdentityDigest,
			"sandbox_contract_id":                  sandboxContractID,
			"sandbox_contract_payload_digest":      sandboxContractPayloadDigest,
			"sandbox_contract_compatibility_shape": sandboxContractCompatibility,
		},
		"feature_policy":      featurePolicy,
		"content_snapshots":   contentSnapshots,
		"source_digests":      map[string]any{"runtime_config_digest": p.SourceDigests.RuntimeConfigDigest, "agent_manifest_digest": p.SourceDigests.AgentManifestDigest},
		"mutable_state_scope": map[string]any{"leases": mutableLeasesScope, "events": mutableEventsScope, "checkpoint_state": mutableCheckpointStateScope},
	}, nil
}

func RenderContentSnapshotsPayload(records []store.ContentSnapshotRecord) (map[string]any, error) {
	out := map[string]any{
		store.ContentSnapshotKindSkills:          nil,
		store.ContentSnapshotKindManagedSettings: nil,
	}
	seen := map[string]struct{}{}
	for _, record := range records {
		kind := strings.TrimSpace(record.Kind)
		switch kind {
		case store.ContentSnapshotKindSkills, store.ContentSnapshotKindManagedSettings:
		default:
			return nil, fmt.Errorf("unsupported content snapshot kind %q", record.Kind)
		}
		if _, ok := seen[kind]; ok {
			return nil, fmt.Errorf("duplicate content snapshot kind %q", kind)
		}
		seen[kind] = struct{}{}
		payload, err := contentSnapshotRecordPayload(record)
		if err != nil {
			return nil, err
		}
		out[kind] = payload
	}
	return out, nil
}

func contentSnapshotRecordPayload(record store.ContentSnapshotRecord) (map[string]any, error) {
	required := []struct {
		field string
		value string
	}{
		{"kind", record.Kind},
		{"digest", record.Digest},
		{"immutable_host_path", record.ImmutableHostPath},
		{"mount_destination", record.MountDestination},
		{"source_evidence_digest", record.SourceEvidenceDigest},
		{"retention_class", record.RetentionClass},
	}
	for _, check := range required {
		if strings.TrimSpace(check.value) == "" {
			return nil, fmt.Errorf("content snapshot %s is required", check.field)
		}
	}
	if err := validateContentSnapshotRecordPath("immutable_host_path", record.ImmutableHostPath); err != nil {
		return nil, err
	}
	if err := validateContentSnapshotRecordPath("mount_destination", record.MountDestination); err != nil {
		return nil, err
	}
	return map[string]any{
		"kind":                   strings.TrimSpace(record.Kind),
		"digest":                 strings.TrimSpace(record.Digest),
		"immutable_host_path":    record.ImmutableHostPath,
		"mount_destination":      record.MountDestination,
		"source_evidence_digest": strings.TrimSpace(record.SourceEvidenceDigest),
		"retention_class":        strings.TrimSpace(record.RetentionClass),
	}, nil
}

func validateContentSnapshotRecordPath(field, path string) error {
	if strings.TrimSpace(path) != path || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return fmt.Errorf("content snapshot %s must be canonical absolute", field)
	}
	return nil
}

func durationSeconds(duration time.Duration) string {
	return fmt.Sprintf("%.9f", duration.Seconds())
}

func nullablePath(value string) any {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return value
}

func stringValue(value any) string {
	switch v := value.(type) {
	case string:
		return v
	default:
		return ""
	}
}
