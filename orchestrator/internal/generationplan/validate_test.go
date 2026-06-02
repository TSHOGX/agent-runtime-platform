package generationplan

import (
	"strings"
	"testing"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/planprojection"
	"harness-platform/orchestrator/internal/runtime"
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

func TestRenderFeaturePolicyPayloadUsesTypedCapabilities(t *testing.T) {
	driver, ok := agents.DriverSpecFor("claude_code")
	if !ok {
		t.Fatalf("driver spec missing")
	}
	provider, ok := agents.RuntimeProviderSpecFor("local_runsc")
	if !ok {
		t.Fatalf("provider spec missing")
	}
	payload, err := RenderFeaturePolicyPayload(driver, provider)
	if err != nil {
		t.Fatalf("render feature policy payload: %v", err)
	}
	if payload[string(agents.FeatureCompaction)] != string(agents.FeaturePolicyRequired) ||
		payload[string(agents.FeatureInterrupt)] != string(agents.FeaturePolicyUnsupported) ||
		payload["capability_schema_version"] != agents.DriverCapabilitySchemaVersion ||
		payload["capability_vocab_version"] != provider.CapabilityVocabulary ||
		payload["legacy_supports_compaction"] != driver.SupportsCompaction ||
		payload["legacy_supports_interrupt"] != driver.SupportsInterrupt ||
		payload["unsupported_features_fail"] != true ||
		payload["credential_bearing_mcp_scope"] != "out_of_scope" {
		t.Fatalf("unexpected rendered feature policy payload: %+v", payload)
	}
	plan := validPlanPayload()
	plan["feature_policy"] = payload
	if err := Validate(ValidateParams{Payload: plan}); err != nil {
		t.Fatalf("rendered feature policy should validate: %v", err)
	}
}

func TestMaterializedDriverConfigPayloadIsPlanEvidence(t *testing.T) {
	payload := MaterializedDriverConfigPayload([]runtime.DriverConfigMaterialization{
		{
			Name:                        "settings",
			SourceProjectionPath:        "/harness-control/driver/pi/settings.json",
			SourceDigest:                "sha256:settings",
			SandboxDestination:          "/home/pi/.config/settings.json",
			DestinationMutableBySandbox: false,
		},
	})
	if len(payload) != 1 ||
		payload[0]["name"] != "settings" ||
		payload[0]["source_projection_path"] != "/harness-control/driver/pi/settings.json" ||
		payload[0]["source_digest"] != "sha256:settings" ||
		payload[0]["sandbox_destination"] != "/home/pi/.config/settings.json" ||
		payload[0]["destination_mutable_by_sandbox"] != false ||
		payload[0]["projection_materialization_kind"] != "driver_config" {
		t.Fatalf("unexpected materialized driver config payload: %+v", payload)
	}
}

func TestValidateRejectsUnsupportedDriverConfigMaterialization(t *testing.T) {
	payload := validPlanPayload()
	runtimeArtifacts := payload["runtime_artifacts"].(map[string]any)
	runtimeArtifacts["materialized_driver_config"] = MaterializedDriverConfigPayload([]runtime.DriverConfigMaterialization{
		{
			Name:                        "settings",
			SourceProjectionPath:        "/harness-control/driver/claude_code/settings.json",
			SourceDigest:                "sha256:settings",
			SandboxDestination:          "/agent-home/settings.json",
			DestinationMutableBySandbox: false,
		},
	})

	err := Validate(ValidateParams{Payload: payload})
	if err == nil || !strings.Contains(err.Error(), "driver claude_code does not support driver config materialization") {
		t.Fatalf("expected unsupported driver config materialization error, got %v", err)
	}
}

func TestValidateDriverConfigMaterializationEvidence(t *testing.T) {
	payload := validPiPlanPayload(t)
	if err := Validate(ValidateParams{Payload: payload}); err != nil {
		t.Fatalf("valid pi driver config materialization should validate: %v", err)
	}

	runtimeArtifacts := payload["runtime_artifacts"].(map[string]any)
	entries := runtimeArtifacts["materialized_driver_config"].([]map[string]any)
	entries[0]["source_digest"] = "settings"
	err := Validate(ValidateParams{Payload: payload})
	if err == nil || !strings.Contains(err.Error(), "source_digest is required") {
		t.Fatalf("expected driver config source digest error, got %v", err)
	}
}

func TestRuntimeArtifactsHydratesFromPlanPayload(t *testing.T) {
	canonical, err := store.CanonicalGenerationPlanPayload(validPlanPayload())
	if err != nil {
		t.Fatalf("canonical plan payload: %v", err)
	}
	artifacts, err := RuntimeArtifacts(canonical)
	if err != nil {
		t.Fatalf("runtime artifacts: %v", err)
	}
	if artifacts.BundleDir != "/var/lib/harness/run/runtime/gen_plan" ||
		artifacts.SpecPath != "/var/lib/harness/run/runtime/gen_plan/config.json" ||
		artifacts.ManifestPath != "/var/lib/harness/run/control/gen_plan/session.json" ||
		artifacts.ManifestDigest != "manifest_digest" ||
		artifacts.ProjectedManifestDigest != "projected_manifest_digest" ||
		artifacts.BundleDigest != "bundle_digest" ||
		artifacts.RuntimeConfigDigest != "runtime_config_digest" ||
		artifacts.SpecDigest != "spec_digest" ||
		artifacts.RunscVersion != "runsc test" ||
		artifacts.RunscBinaryPath != "/usr/local/bin/runsc-test" ||
		artifacts.RunscBinaryDigest != "sha256:runsc" {
		t.Fatalf("unexpected runtime artifacts: %+v", artifacts)
	}
	if len(artifacts.MaterializedDriverConfig) != 0 {
		t.Fatalf("unexpected driver config materialization: %+v", artifacts.MaterializedDriverConfig)
	}
}

func TestRuntimeArtifactsHydratesDriverConfigMaterializationFromPlan(t *testing.T) {
	canonical, err := store.CanonicalGenerationPlanPayload(validPiPlanPayload(t))
	if err != nil {
		t.Fatalf("canonical pi plan payload: %v", err)
	}
	artifacts, err := RuntimeArtifacts(canonical)
	if err != nil {
		t.Fatalf("runtime artifacts: %v", err)
	}
	entries := map[string]runtime.DriverConfigMaterialization{}
	for _, entry := range artifacts.MaterializedDriverConfig {
		entries[entry.Name] = entry
	}
	models := entries["models"]
	if len(entries) != 2 ||
		models.SourceProjectionPath != agents.PiModelsConfigPath ||
		models.HostSourcePath != "/var/lib/harness/run/control/gen_plan/driver/pi/models.json" ||
		models.SourceDigest != "sha256:models" ||
		models.SandboxDestination != agents.PiModelsSandboxPath ||
		models.DestinationMutableBySandbox {
		t.Fatalf("unexpected hydrated driver config materialization: %+v", artifacts.MaterializedDriverConfig)
	}
}

func TestRenderContentSnapshotsPayloadFreezesImmutableRefs(t *testing.T) {
	payload, err := RenderContentSnapshotsPayload([]store.ContentSnapshotRecord{
		{
			Kind:                 store.ContentSnapshotKindSkills,
			Digest:               "sha256:skills",
			ImmutableHostPath:    "/var/lib/harness/content/skills/sha256-skills",
			MountDestination:     "/harness-skills",
			SourceEvidenceDigest: "sha256:skills-source",
			RetentionClass:       "generation_plan",
		},
	})
	if err != nil {
		t.Fatalf("render content snapshots: %v", err)
	}
	if payload[store.ContentSnapshotKindManagedSettings] != nil {
		t.Fatalf("managed settings snapshot should remain nil: %+v", payload)
	}
	skills := payload[store.ContentSnapshotKindSkills].(map[string]any)
	if skills["kind"] != store.ContentSnapshotKindSkills ||
		skills["digest"] != "sha256:skills" ||
		skills["immutable_host_path"] != "/var/lib/harness/content/skills/sha256-skills" ||
		skills["mount_destination"] != "/harness-skills" ||
		skills["source_evidence_digest"] != "sha256:skills-source" ||
		skills["retention_class"] != "generation_plan" {
		t.Fatalf("unexpected skills snapshot payload: %+v", skills)
	}
	plan := validPlanPayload()
	plan["content_snapshots"] = payload
	addSkillsSnapshotMount(plan, skills)
	if err := Validate(ValidateParams{Payload: plan}); err != nil {
		t.Fatalf("rendered content snapshots should validate: %v", err)
	}
}

func TestRenderContentSnapshotsPayloadRejectsInvalidSelection(t *testing.T) {
	if _, err := RenderContentSnapshotsPayload([]store.ContentSnapshotRecord{
		{Kind: store.ContentSnapshotKindSkills, Digest: "sha256:skills", ImmutableHostPath: "/var/lib/harness/content/skills/sha256-skills", MountDestination: "/harness-skills", SourceEvidenceDigest: "sha256:skills-source", RetentionClass: "generation_plan"},
		{Kind: store.ContentSnapshotKindSkills, Digest: "sha256:skills2", ImmutableHostPath: "/var/lib/harness/content/skills/sha256-skills2", MountDestination: "/harness-skills", SourceEvidenceDigest: "sha256:skills-source2", RetentionClass: "generation_plan"},
	}); err == nil || !strings.Contains(err.Error(), "duplicate content snapshot kind") {
		t.Fatalf("expected duplicate snapshot rejection, got %v", err)
	}
	if _, err := RenderContentSnapshotsPayload([]store.ContentSnapshotRecord{
		{Kind: "workspace", Digest: "sha256:workspace", ImmutableHostPath: "/var/lib/harness/content/workspace/sha256-workspace", MountDestination: "/workspace-content", SourceEvidenceDigest: "sha256:workspace-source", RetentionClass: "generation_plan"},
	}); err == nil || !strings.Contains(err.Error(), "unsupported content snapshot kind") {
		t.Fatalf("expected unsupported snapshot rejection, got %v", err)
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

func TestValidateRejectsContentSnapshotKindMismatch(t *testing.T) {
	payload := validPlanPayload()
	contentSnapshots := payload["content_snapshots"].(map[string]any)
	contentSnapshots["managed_settings"] = map[string]any{
		"kind":                   "skills",
		"digest":                 "sha256:settings",
		"immutable_host_path":    "/var/lib/harness/content/managed-settings/sha256-settings",
		"mount_destination":      "/harness-managed-settings",
		"source_evidence_digest": "sha256:source",
		"retention_class":        "active",
	}

	err := Validate(ValidateParams{Payload: payload})
	if err == nil || !strings.Contains(err.Error(), "content_snapshots.managed_settings.kind must be managed_settings") {
		t.Fatalf("expected content snapshot kind error, got %v", err)
	}
}

func TestValidateRejectsSkillsSnapshotMountDrift(t *testing.T) {
	payload := validPlanPayload()
	contentSnapshots := payload["content_snapshots"].(map[string]any)
	contentSnapshots["skills"] = map[string]any{
		"kind":                   "skills",
		"digest":                 "sha256:skills",
		"immutable_host_path":    "/var/lib/harness/content/skills/sha256-skills",
		"mount_destination":      "/other-skills",
		"source_evidence_digest": "sha256:source",
		"retention_class":        "active",
	}

	err := Validate(ValidateParams{Payload: payload})
	if err == nil || !strings.Contains(err.Error(), "content_snapshots.skills.mount_destination must be /harness-skills") {
		t.Fatalf("expected skills mount destination error, got %v", err)
	}
}

func TestValidateRejectsManagedSettingsSnapshotMountDrift(t *testing.T) {
	payload := validPlanPayload()
	contentSnapshots := payload["content_snapshots"].(map[string]any)
	contentSnapshots["managed_settings"] = map[string]any{
		"kind":                   "managed_settings",
		"digest":                 "sha256:settings",
		"immutable_host_path":    "/var/lib/harness/content/managed-settings/sha256-settings",
		"mount_destination":      "/other-managed-settings",
		"source_evidence_digest": "sha256:source",
		"retention_class":        "active",
	}

	err := Validate(ValidateParams{Payload: payload})
	if err == nil || !strings.Contains(err.Error(), "content_snapshots.managed_settings.mount_destination must be /harness-managed-settings") {
		t.Fatalf("expected managed settings mount destination error, got %v", err)
	}
}

func TestValidateRejectsUnsupportedContentSnapshotKey(t *testing.T) {
	payload := validPlanPayload()
	contentSnapshots := payload["content_snapshots"].(map[string]any)
	contentSnapshots["workspace"] = nil

	err := Validate(ValidateParams{Payload: payload})
	if err == nil || !strings.Contains(err.Error(), "content_snapshots.workspace is unsupported") {
		t.Fatalf("expected content snapshot key error, got %v", err)
	}
}

func TestValidateRequiresSkillsSnapshotMountEvidence(t *testing.T) {
	payload := validPlanPayload()
	contentSnapshots := payload["content_snapshots"].(map[string]any)
	skills := validSkillsSnapshotPayload()
	contentSnapshots["skills"] = skills

	err := Validate(ValidateParams{Payload: payload})
	if err == nil || !strings.Contains(err.Error(), "mounts.content_snapshots is required for skills content snapshot") {
		t.Fatalf("expected missing skills mount evidence error, got %v", err)
	}

	addSkillsSnapshotMount(payload, skills)
	mounts := payload["mounts"].(map[string]any)
	snapshotMounts := mounts["content_snapshots"].(map[string]any)
	snapshotMounts["skills"].(map[string]any)["digest"] = "sha256:changed"
	err = Validate(ValidateParams{Payload: payload})
	if err == nil || !strings.Contains(err.Error(), "mounts.content_snapshots.skills.digest mismatch") {
		t.Fatalf("expected skills mount digest mismatch, got %v", err)
	}
}

func TestValidateRequiresContentSnapshotMountScope(t *testing.T) {
	payload := validPlanPayload()
	contentSnapshots := payload["content_snapshots"].(map[string]any)
	skills := validSkillsSnapshotPayload()
	contentSnapshots["skills"] = skills
	addSkillsSnapshotMount(payload, skills)
	workspace := payload["data_volumes"].(map[string]any)["workspace"].(map[string]any)
	workspace["platform_content_mount_scope"] = "none"

	err := Validate(ValidateParams{Payload: payload})
	if err == nil || !strings.Contains(err.Error(), "platform_content_mount_scope must be immutable_content_snapshots") {
		t.Fatalf("expected content snapshot mount scope error, got %v", err)
	}
}

func TestValidateRequiresManagedSettingsSnapshotMountEvidence(t *testing.T) {
	payload := validPlanPayload()
	contentSnapshots := payload["content_snapshots"].(map[string]any)
	settings := validManagedSettingsSnapshotPayload()
	contentSnapshots["managed_settings"] = settings

	err := Validate(ValidateParams{Payload: payload})
	if err == nil || !strings.Contains(err.Error(), "mounts.content_snapshots is required for managed_settings content snapshot") {
		t.Fatalf("expected missing managed settings mount evidence error, got %v", err)
	}

	addManagedSettingsSnapshotMount(payload, settings)
	mounts := payload["mounts"].(map[string]any)
	snapshotMounts := mounts["content_snapshots"].(map[string]any)
	snapshotMounts["managed_settings"].(map[string]any)["mount_name"] = "settings_snapshot"
	err = Validate(ValidateParams{Payload: payload})
	if err == nil || !strings.Contains(err.Error(), "mounts.content_snapshots.managed_settings.mount_name mismatch") {
		t.Fatalf("expected managed settings mount name mismatch, got %v", err)
	}
}

func TestValidateRejectsBaseMountEvidenceDrift(t *testing.T) {
	payload := validPlanPayload()
	mounts := payload["mounts"].(map[string]any)
	mounts["workspace"].(map[string]any)["source"] = "/var/lib/harness/sessions/other"

	err := Validate(ValidateParams{Payload: payload})
	if err == nil || !strings.Contains(err.Error(), "mounts.workspace.source mismatch") {
		t.Fatalf("expected workspace mount source mismatch, got %v", err)
	}
}

func TestValidateRejectsNetworkHostsMountEvidenceDrift(t *testing.T) {
	payload := validPlanPayload()
	mounts := payload["mounts"].(map[string]any)
	runtimeArtifacts := payload["runtime_artifacts"].(map[string]any)
	mounts["network_hosts_path"] = "/var/lib/harness/run/network/gen_plan/hosts"
	runtimeArtifacts["network_hosts_path"] = "/var/lib/harness/run/network/gen_plan/hosts.changed"

	err := Validate(ValidateParams{Payload: payload})
	if err == nil || !strings.Contains(err.Error(), "mounts.network_hosts_path mismatch") {
		t.Fatalf("expected network hosts mount mismatch, got %v", err)
	}
}

func TestValidateRequiredContentSnapshotSelections(t *testing.T) {
	policy := map[string]any{
		string(agents.FeatureSkillsSnapshot):  string(agents.FeaturePolicyRequired),
		string(agents.FeatureManagedSettings): string(agents.FeaturePolicyDisabled),
	}
	snapshots := map[string]any{
		store.ContentSnapshotKindSkills:          nil,
		store.ContentSnapshotKindManagedSettings: nil,
	}

	err := validateRequiredContentSnapshotSelections(policy, snapshots)
	if err == nil || !strings.Contains(err.Error(), "content_snapshots.skills is required by feature_policy.skills_snapshot") {
		t.Fatalf("expected required skills snapshot error, got %v", err)
	}

	snapshots[store.ContentSnapshotKindSkills] = validSkillsSnapshotPayload()
	if err := validateRequiredContentSnapshotSelections(policy, snapshots); err != nil {
		t.Fatalf("required skills snapshot should validate: %v", err)
	}

	policy[string(agents.FeatureManagedSettings)] = string(agents.FeaturePolicyRequired)
	err = validateRequiredContentSnapshotSelections(policy, snapshots)
	if err == nil || !strings.Contains(err.Error(), "content_snapshots.managed_settings is required by feature_policy.managed_settings") {
		t.Fatalf("expected required managed settings snapshot error, got %v", err)
	}
}

func TestValidateRejectsEmbeddedProjectionDigests(t *testing.T) {
	payload := validPlanPayload()
	payload["projection_digests"] = map[string]any{}

	err := Validate(ValidateParams{Payload: payload})
	if err == nil || !strings.Contains(err.Error(), "projection_digests must be stored outside the plan") {
		t.Fatalf("expected embedded projection digest error, got %v", err)
	}
}

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

func TestContentSnapshotRefsExtractsPlanSnapshotDigests(t *testing.T) {
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
	canonical, err := store.CanonicalGenerationPlanPayload(payload)
	if err != nil {
		t.Fatalf("canonical plan payload: %v", err)
	}
	refs := ContentSnapshotRefs(canonical)
	if len(refs) != 1 || refs["skills"] != "sha256:skills" {
		t.Fatalf("content snapshot refs = %+v", refs)
	}
	fullRefs := ContentSnapshotReferences(canonical)
	if len(fullRefs) != 1 ||
		fullRefs["skills"].Kind != "skills" ||
		fullRefs["skills"].Digest != "sha256:skills" ||
		fullRefs["skills"].ImmutableHostPath != "/var/lib/harness/content/skills/sha256-skills" ||
		fullRefs["skills"].MountDestination != "/harness-skills" ||
		fullRefs["skills"].SourceEvidenceDigest != "sha256:source" ||
		fullRefs["skills"].RetentionClass != "active" {
		t.Fatalf("content snapshot full refs = %+v", fullRefs)
	}
}

func TestOptionalProjectionPayloadDigest(t *testing.T) {
	if got := OptionalProjectionPayloadDigest(store.GenerationPlanProjectionBundle, ""); got != "" {
		t.Fatalf("empty optional projection digest = %q", got)
	}
	if got := OptionalProjectionPayloadDigest(store.GenerationPlanProjectionBundle, "sha256:bundle"); got != "sha256:bundle" {
		t.Fatalf("prefixed optional projection digest = %q", got)
	}
	if got := OptionalProjectionPayloadDigest(store.GenerationPlanProjectionBundle, "bundle_digest"); got == "" || !strings.HasPrefix(got, "sha256:") {
		t.Fatalf("optional projection digest = %q", got)
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
		"mutable_state_scope": map[string]any{"leases": "runtime_generations", "events": "events", "checkpoint_state": "runtime_generations"},
	}
}

func validPiPlanPayload(t *testing.T) map[string]any {
	t.Helper()
	payload := validPlanPayload()
	driver, ok := agents.DriverSpecFor("pi")
	if !ok {
		t.Fatalf("pi driver spec missing")
	}
	provider, ok := agents.RuntimeProviderSpecFor("local_runsc")
	if !ok {
		t.Fatalf("provider spec missing")
	}
	featurePolicy, err := agents.FeaturePolicyPayload(agents.DefaultFeaturePolicyForDriver(driver))
	if err != nil {
		t.Fatalf("pi feature policy payload: %v", err)
	}
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

	driverPayload := payload["driver"].(map[string]any)
	driverPayload["driver_id"] = string(agents.Pi)
	driverPayload["driver_kind"] = string(driver.Kind)
	driverPayload["bridge_protocol"] = driver.BridgeProtocol
	driverPayload["bridge_protocol_version"] = driver.BridgeProtocolVersion
	driverPayload["turn_input_schema"] = driver.TurnInputSchema
	driverPayload["output_schema"] = driver.OutputSchema
	driverPayload["output_format"] = driver.OutputFormat
	driverPayload["capability_snapshot"] = agents.DriverCapabilityPayload(driver)
	payload["feature_policy"] = featurePolicyPayload

	agentHome := payload["data_volumes"].(map[string]any)["agent_home"].(map[string]any)
	agentHome["driver"] = string(agents.Pi)

	entries := []runtime.DriverConfigMaterialization{}
	for _, spec := range agents.DriverConfigMaterializationSpecsFor(agents.Pi) {
		entries = append(entries, runtime.DriverConfigMaterialization{
			Name:                        spec.Name,
			SourceProjectionPath:        spec.SourceProjectionPath,
			SourceDigest:                "sha256:" + spec.Name,
			SandboxDestination:          spec.SandboxDestination,
			DestinationMutableBySandbox: spec.DestinationMutableBySandbox,
		})
	}
	_, mountPayload, err := planprojection.DriverConfigMaterializationPayload(string(agents.Pi), entries)
	if err != nil {
		t.Fatalf("pi driver config mount payload: %v", err)
	}
	runtimeArtifacts := payload["runtime_artifacts"].(map[string]any)
	runtimeArtifacts["materialized_driver_config"] = MaterializedDriverConfigPayload(entries)
	mounts := payload["mounts"].(map[string]any)
	mounts["driver_config_materializations"] = mountPayload
	return payload
}

func validFrozenEvidenceParams(payload map[string]any) VerifyFrozenEvidenceParams {
	return VerifyFrozenEvidenceParams{
		Payload:               payload,
		SessionID:             "sess_plan",
		GenerationID:          "gen_plan",
		DriverID:              "claude_code",
		OutputFormat:          "stream-json",
		DriverStateDigest:     "sha256:driver-state",
		DriverStateVersion:    1,
		NetworkProfileID:      "net_gen_plan",
		AgentRuntimeProfileID: "arp_gen_plan",
		RunscPlatform:         "systrap",
		RunscVersion:          "runsc test",
		RunscBinaryPath:       "/usr/local/bin/runsc-test",
		RunscBinaryDigest:     "sha256:runsc",
		ProjectionDigests: map[string]string{
			"sandbox_contract":           "sha256:sandbox-contract",
			"control_manifest":           "sha256:control-manifest",
			"control_manifest_projected": "sha256:control-manifest-projected",
			"oci_spec":                   "sha256:oci-spec",
			"bundle":                     "sha256:bundle",
			"runtime_config":             "sha256:runtime-config",
		},
		ProjectionVersions: map[string]int{
			"sandbox_contract":           store.GenerationPlanProjectionVersion,
			"control_manifest":           store.GenerationPlanProjectionVersion,
			"control_manifest_projected": store.GenerationPlanProjectionVersion,
			"oci_spec":                   store.GenerationPlanProjectionVersion,
			"bundle":                     store.GenerationPlanProjectionVersion,
			"runtime_config":             store.GenerationPlanProjectionVersion,
		},
		CheckpointBundleDigest:          "sha256:bundle",
		CheckpointRuntimeConfigDigest:   "sha256:runtime-config",
		CheckpointControlManifestDigest: "sha256:control-manifest-projected",
		CheckpointDriverStatesDigest:    "sha256:driver-state-fence",
		CheckpointPlanDigest:            store.GenerationPlanDigest(mustCanonicalPlanPayloadForTest(payload)),
	}
}

func cloneStringMap(values map[string]string) map[string]string {
	out := map[string]string{}
	for key, value := range values {
		out[key] = value
	}
	return out
}

func mustCanonicalPlanPayloadForTest(payload map[string]any) []byte {
	canonical, err := store.CanonicalGenerationPlanPayload(payload)
	if err != nil {
		panic(err)
	}
	return canonical
}

func validSkillsSnapshotPayload() map[string]any {
	return map[string]any{
		"kind":                   "skills",
		"digest":                 "sha256:skills",
		"immutable_host_path":    "/var/lib/harness/content/skills/sha256-skills",
		"mount_destination":      "/harness-skills",
		"source_evidence_digest": "sha256:source",
		"retention_class":        "active",
	}
}

func validManagedSettingsSnapshotPayload() map[string]any {
	return map[string]any{
		"kind":                   "managed_settings",
		"digest":                 "sha256:settings",
		"immutable_host_path":    "/var/lib/harness/content/managed-settings/sha256-settings",
		"mount_destination":      "/harness-managed-settings",
		"source_evidence_digest": "sha256:source",
		"retention_class":        "active",
	}
}

func addSkillsSnapshotMount(payload map[string]any, snapshot map[string]any) {
	addContentSnapshotMount(payload, "skills", "skills_snapshot", snapshot)
}

func addManagedSettingsSnapshotMount(payload map[string]any, snapshot map[string]any) {
	addContentSnapshotMount(payload, "managed_settings", "managed_settings_snapshot", snapshot)
}

func addContentSnapshotMount(payload map[string]any, kind, mountName string, snapshot map[string]any) {
	mounts := payload["mounts"].(map[string]any)
	snapshotMounts, ok := mounts["content_snapshots"].(map[string]any)
	if !ok {
		snapshotMounts = map[string]any{}
		mounts["content_snapshots"] = snapshotMounts
	}
	snapshotMounts[kind] = map[string]any{
		"mount_name":  mountName,
		"type":        "bind",
		"mode":        "ro",
		"exact":       true,
		"source":      snapshot["immutable_host_path"],
		"destination": snapshot["mount_destination"],
		"digest":      snapshot["digest"],
	}
	workspace := payload["data_volumes"].(map[string]any)["workspace"].(map[string]any)
	workspace["platform_content_mount_scope"] = "immutable_content_snapshots"
}

func mountPlanForPayload(t *testing.T, payload map[string]any, snapshots []store.ContentSnapshotRecord) runtime.MountPlan {
	t.Helper()
	driver := payload["driver"].(map[string]any)
	artifacts := payload["runtime_artifacts"].(map[string]any)
	volumes := payload["data_volumes"].(map[string]any)
	workspace := volumes["workspace"].(map[string]any)
	agentHome := volumes["agent_home"].(map[string]any)
	details := store.RuntimeGenerationDetails{
		DriverID:         driver["driver_id"].(string),
		ControlDirPath:   artifacts["control_dir_path"].(string),
		BridgeDirPath:    artifacts["bridge_dir_path"].(string),
		NetworkHostsPath: stringValue(artifacts["network_hosts_path"]),
	}
	mountPlan, err := runtime.BuildSandboxMountPlan(runtime.SandboxMountPlanInputs{
		Generation:        details,
		WorkspaceHostPath: workspace["host_path"].(string),
		AgentHomeHostPath: agentHome["host_path"].(string),
		NetworkHostsPath:  details.NetworkHostsPath,
		ContentSnapshots:  snapshots,
	})
	if err != nil {
		t.Fatalf("build runtime mount plan: %v", err)
	}
	return mountPlan
}

func mutateMountSource(t *testing.T, mountPlan *runtime.MountPlan, name, source string) {
	t.Helper()
	for i := range mountPlan.Content {
		if mountPlan.Content[i].Name == name {
			mountPlan.Content[i].Source = source
			return
		}
	}
	t.Fatalf("runtime mount %s not found in %+v", name, mountPlan.Content)
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
