package planprojection

import (
	"path/filepath"
	"strings"
	"testing"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/runtime"
	"harness-platform/orchestrator/internal/store"
)

func TestRenderSandboxContractIncludesSkillsSnapshotMountFacts(t *testing.T) {
	dir := t.TempDir()
	params := validSandboxContractParams(t, dir)
	params.ContentSnapshots = []store.ContentSnapshotRecord{
		{
			Kind:                 store.ContentSnapshotKindSkills,
			Digest:               "sha256:skills",
			ImmutableHostPath:    filepath.Join(dir, "content", "skills", "sha256-skills"),
			MountDestination:     store.ContentSnapshotSkillsMount,
			SourceEvidenceDigest: "sha256:skills-source",
			RetentionClass:       "generation_plan",
		},
		{
			Kind:                 store.ContentSnapshotKindManagedSettings,
			Digest:               "sha256:settings",
			ImmutableHostPath:    filepath.Join(dir, "content", "managed-settings", "sha256-settings"),
			MountDestination:     store.ContentSnapshotManagedSettingsMount,
			SourceEvidenceDigest: "sha256:settings-source",
			RetentionClass:       "generation_plan",
		},
	}

	payload, err := RenderSandboxContract(params)
	if err != nil {
		t.Fatalf("render sandbox contract: %v", err)
	}
	mountPlan := payload["mount_plan"].(map[string]any)
	contentSnapshots := mountPlan["content_snapshots"].(map[string]any)
	skills := contentSnapshots[store.ContentSnapshotKindSkills].(map[string]any)
	if skills["mount_name"] != "skills_snapshot" ||
		skills["type"] != "bind" ||
		skills["mode"] != "ro" ||
		skills["exact"] != true ||
		skills["source"] != filepath.Join(dir, "content", "skills", "sha256-skills") ||
		skills["destination"] != store.ContentSnapshotSkillsMount ||
		skills["digest"] != "sha256:skills" ||
		skills["source_evidence_digest"] != "sha256:skills-source" ||
		skills["retention_class"] != "generation_plan" {
		t.Fatalf("unexpected skills snapshot mount facts: %+v", skills)
	}
	settings := contentSnapshots[store.ContentSnapshotKindManagedSettings].(map[string]any)
	if settings["mount_name"] != "managed_settings_snapshot" ||
		settings["type"] != "bind" ||
		settings["mode"] != "ro" ||
		settings["exact"] != true ||
		settings["source"] != filepath.Join(dir, "content", "managed-settings", "sha256-settings") ||
		settings["destination"] != store.ContentSnapshotManagedSettingsMount ||
		settings["digest"] != "sha256:settings" ||
		settings["source_evidence_digest"] != "sha256:settings-source" ||
		settings["retention_class"] != "generation_plan" {
		t.Fatalf("unexpected managed settings snapshot mount facts: %+v", settings)
	}
}

func TestRenderSandboxContractRejectsUnsupportedContentSnapshotMount(t *testing.T) {
	dir := t.TempDir()
	params := validSandboxContractParams(t, dir)
	params.ContentSnapshots = []store.ContentSnapshotRecord{
		{
			Kind:                 "workspace",
			Digest:               "sha256:workspace",
			ImmutableHostPath:    filepath.Join(dir, "content", "workspace", "sha256-workspace"),
			MountDestination:     "/workspace-content",
			SourceEvidenceDigest: "sha256:workspace-source",
			RetentionClass:       "generation_plan",
		},
	}

	_, err := RenderSandboxContract(params)
	if err == nil || !strings.Contains(err.Error(), "unsupported content snapshot kind") {
		t.Fatalf("expected unsupported content snapshot error, got %v", err)
	}
}

func TestRenderSandboxContractRejectsContentSnapshotMountDestinationDrift(t *testing.T) {
	dir := t.TempDir()
	params := validSandboxContractParams(t, dir)
	params.ContentSnapshots = []store.ContentSnapshotRecord{
		{
			Kind:                 store.ContentSnapshotKindManagedSettings,
			Digest:               "sha256:settings",
			ImmutableHostPath:    filepath.Join(dir, "content", "managed-settings", "sha256-settings"),
			MountDestination:     "/other-managed-settings",
			SourceEvidenceDigest: "sha256:settings-source",
			RetentionClass:       "generation_plan",
		},
	}

	_, err := RenderSandboxContract(params)
	if err == nil || !strings.Contains(err.Error(), "content snapshot managed_settings mount destination must be /harness-managed-settings") {
		t.Fatalf("expected managed settings mount destination error, got %v", err)
	}
}

func validSandboxContractParams(t *testing.T, dir string) SandboxContractParams {
	t.Helper()
	driver, ok := agents.DriverSpecFor("claude_code")
	if !ok {
		t.Fatalf("claude driver spec missing")
	}
	provider, ok := agents.RuntimeProviderSpecFor("local_runsc")
	if !ok {
		t.Fatalf("provider spec missing")
	}
	return SandboxContractParams{
		Session: store.Session{
			ID:       "sess_contract",
			DriverID: "claude_code",
			Mode:     "agent",
		},
		Details: store.RuntimeGenerationDetails{
			SessionID:                "sess_contract",
			GenerationID:             "gen_contract",
			NetworkProfileID:         "net_contract",
			AgentRuntimeProfileID:    "arp_contract",
			RunscPlatform:            "systrap",
			RunscNetwork:             "sandbox",
			RunscOverlay2:            "none",
			ControlDirPath:           filepath.Join(dir, "run", "control", "gen_contract"),
			BundleDirPath:            filepath.Join(dir, "run", "runtime", "gen_contract"),
			SpecPath:                 filepath.Join(dir, "run", "runtime", "gen_contract", "config.json"),
			BridgeDirPath:            filepath.Join(dir, "run", "bridge", "gen_contract"),
			LogDirPath:               filepath.Join(dir, "logs", "gen_contract"),
			RunscContainerID:         "harness-gen-contract",
			HostGatewayIP:            "10.240.0.1",
			SandboxIPCIDR:            "10.240.0.2/30",
			HostSideCIDR:             "10.240.0.1/30",
			NetnsName:                "harness-gen-contract",
			NetnsPath:                filepath.Join(dir, "netns", "harness-gen-contract"),
			HostVeth:                 "vh-contract",
			SandboxVeth:              "vs-contract",
			EgressPolicyID:           "egress_contract",
			DriverID:                 "claude_code",
			Model:                    "sonnet",
			OutputFormat:             "stream-json",
			SandboxUID:               65534,
			SandboxGID:               65534,
			ModelAccessAllowed:       true,
			DriverStateDigest:        "sha256:driver-state",
			DriverStateVersion:       1,
			ManifestAnthropicBaseURL: "http://harness-model-proxy.internal:8082",
		},
		Artifacts: runtime.GenerationArtifacts{
			RunscVersion:      "runsc test",
			RunscBinaryPath:   "/usr/local/bin/runsc-test",
			RunscBinaryDigest: "sha256:runsc",
		},
		ResourceIdentityDigest:      "sha256:resource",
		NetworkIdentityNftTableName: "harness-gen-contract",
		Volumes: DataVolumes{
			Workspace: store.SessionWorkspaceVolume{
				SessionID: "sess_contract",
				HostPath:  filepath.Join(dir, "sessions", "sess_contract"),
			},
			DriverHome: store.SessionDriverHomeVolume{
				SessionID: "sess_contract",
				Driver:    "claude_code",
				HostPath:  filepath.Join(dir, "agent-homes", "sess_contract", "claude_code"),
			},
		},
		DriverSpec:   driver,
		ProviderSpec: provider,
		InputDigests: SandboxContractInputDigests{
			RuntimeConfigDigest: "sha256:runtime-config",
			AgentManifestDigest: "sha256:agent-manifest",
		},
	}
}
