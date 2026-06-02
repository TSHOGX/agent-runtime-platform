package server

import (
	"context"
	"net/netip"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/runtime"
	"harness-platform/orchestrator/internal/sessionstate"
	"harness-platform/orchestrator/internal/store"
)

func TestSandboxContractDigestForPayloadFailsClosedOnCanonicalizationError(t *testing.T) {
	if got, err := sandboxContractDigestForPayload(map[string]any{"invalid": func() {}}); err == nil {
		t.Fatalf("expected canonicalization error, got digest %q", got)
	}
}

func TestSandboxContractInputEvidenceRequiresExplicitDefaultAgentAndSessionMode(t *testing.T) {
	dir := t.TempDir()
	cfg := testServerConfig(dir)
	srv := &Server{cfg: cfg}
	session := store.Session{
		ID:       "sess_contract_evidence",
		DriverID: "claude_code",
		Mode:     "agent",
	}

	cfg.DefaultAgent = ""
	srv = &Server{cfg: cfg}
	if _, err := srv.sandboxContractInputEvidenceFor(session, "claude_code"); err == nil || !strings.Contains(err.Error(), "default agent is required") {
		t.Fatalf("expected missing default agent error, got %v", err)
	}

	cfg.DefaultAgent = "claude_code"
	srv = &Server{cfg: cfg}
	session.Mode = ""
	if _, err := srv.sandboxContractInputEvidenceFor(session, "claude_code"); err == nil || !strings.Contains(err.Error(), "session mode is required") {
		t.Fatalf("expected missing session mode error, got %v", err)
	}
}

func TestSandboxContractPayloadRequiresRunscPlatformAndSessionMode(t *testing.T) {
	dir := t.TempDir()
	cfg := testServerConfig(dir)
	srv := &Server{cfg: cfg}
	session := store.Session{
		ID:       "sess_contract_payload",
		DriverID: "claude_code",
		Mode:     "agent",
	}
	details := store.RuntimeGenerationDetails{
		SessionID:      session.ID,
		GenerationID:   "gen_contract_payload",
		DriverID:       "claude_code",
		RunscPlatform:  "systrap",
		SandboxIPCIDR:  "10.241.0.2/29",
		RunscOverlay2:  "true",
		SandboxUID:     serverTestSandboxUID(),
		SandboxGID:     serverTestSandboxGID(),
		ControlDirPath: filepath.Join(dir, "control"),
		BridgeDirPath:  filepath.Join(dir, "bridge"),
	}

	missingPlatform := details
	missingPlatform.RunscPlatform = ""
	if _, err := srv.sandboxContractPayload(session, missingPlatform, runtime.GenerationArtifacts{}, "sha256:resource-identity", sessionRuntimeDataVolumes{}, nil); err == nil || !strings.Contains(err.Error(), "runsc platform is required") {
		t.Fatalf("expected missing runsc platform error, got %v", err)
	}
	if _, err := srv.runtimeResourceInstanceParams(missingPlatform, runtime.GenerationArtifacts{}, "host-a"); err == nil || !strings.Contains(err.Error(), "runsc platform is required") {
		t.Fatalf("expected missing runtime resource runsc platform error, got %v", err)
	}

	missingDriverStateDigest := details
	missingDriverStateDigest.DriverStateDigest = ""
	if _, err := srv.sandboxContractPayload(session, missingDriverStateDigest, runtime.GenerationArtifacts{}, "sha256:resource-identity", sessionRuntimeDataVolumes{}, nil); err == nil || !strings.Contains(err.Error(), "initial driver state digest is required") {
		t.Fatalf("expected missing driver state digest error, got %v", err)
	}

	details.DriverStateDigest = "sha256:driver-state"
	session.Mode = ""
	if _, err := srv.sandboxContractPayload(session, details, runtime.GenerationArtifacts{}, "sha256:resource-identity", sessionRuntimeDataVolumes{}, nil); err == nil || !strings.Contains(err.Error(), "session mode is required") {
		t.Fatalf("expected missing session mode error, got %v", err)
	}
}

func TestSandboxContractPayloadRecordsPiMaterializedConfig(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	now := time.Now().UTC()
	session := store.Session{
		ID:        "sess_pi_contract",
		UserID:    labUserID,
		Status:    string(sessionstate.Created),
		DriverID:  "pi",
		Mode:      "agent",
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := st.CreateSession(ctx, session); err != nil {
		t.Fatalf("create pi session: %v", err)
	}
	modelAccessAllowed := true
	allocatorConfig := store.ResourceAllocatorConfig{
		RunDir:                      filepath.Join(dir, "run"),
		CIDRPool:                    netip.MustParsePrefix("10.240.0.0/28"),
		EgressDNSPolicy:             "hostnames_only",
		HostProxyBindURL:            "http://0.0.0.0:8082",
		ProxyPort:                   8082,
		DriverID:                    "pi",
		Model:                       "sonnet",
		OutputFormat:                "pi_rpc_events_v1.0",
		DisableNonessentialTraffic:  true,
		SandboxUID:                  65534,
		SandboxGID:                  65534,
		ModelAccessAllowed:          &modelAccessAllowed,
		ProviderCredentialsHostOnly: true,
		SandboxModelProxyBaseURL:    "http://harness-model-proxy.internal:8082",
	}
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    allocatorConfig,
	})
	if err != nil {
		t.Fatalf("allocate pi generation: %v", err)
	}
	details, err := st.GetRuntimeGenerationDetails(ctx, session.ID, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get generation details: %v", err)
	}
	rt := runtime.New(runtime.Config{
		BundleRoot:           filepath.Join(dir, "bundle", "out"),
		RootFSPath:           filepath.Join(dir, "rootfs"),
		BridgeMode:           "claim-loop",
		BridgeHeartbeat:      20 * time.Second,
		BridgePollInterval:   5 * time.Millisecond,
		ProbeHealthzStatuses: []int{200},
		CommandRunner: serverCommandRunner{outputs: map[string][]byte{
			"runsc --version": []byte("runsc test"),
			"ip netns exec " + details.NetnsName + " curl -sS --max-time 2 -o /dev/null -w %{http_code} " + strings.TrimRight(details.ProbeURL, "/") + "/healthz": []byte("200"),
		}},
	})
	workspaceHostPath := filepath.Join(dir, "volumes", "workspaces", session.ID)
	agentHomeHostPath := filepath.Join(dir, "volumes", "driver-homes", session.ID, "pi")
	artifacts, err := rt.PrepareGeneration(ctx, runtime.StartRequest{
		SessionID:         session.ID,
		GenerationID:      allocation.GenerationID,
		DriverID:          "pi",
		Generation:        details,
		WorkspaceHostPath: workspaceHostPath,
		AgentHomeHostPath: agentHomeHostPath,
	})
	if err != nil {
		t.Fatalf("prepare pi runtime artifacts: %v", err)
	}
	cfg := testServerConfig(dir)
	cfg.DefaultAgent = "pi"
	srv := &Server{cfg: cfg, store: st, runtime: rt}
	payload, err := srv.sandboxContractPayload(session, details, artifacts, "sha256:resource-identity", sessionRuntimeDataVolumes{
		Workspace: store.SessionWorkspaceVolume{
			SessionID:                session.ID,
			HostPath:                 workspaceHostPath,
			LayoutVersion:            store.DataVolumeLayoutVersion,
			RuntimeIdentityDigest:    "sha256:workspace-identity",
			ProvisioningMarkerPath:   filepath.Join(dir, "evidence", "workspaces", session.ID+".json"),
			ProvisioningMarkerDigest: "sha256:workspace-marker",
		},
		DriverHome: store.SessionDriverHomeVolume{
			SessionID:                session.ID,
			Driver:                   "pi",
			HostPath:                 agentHomeHostPath,
			LayoutVersion:            store.DataVolumeLayoutVersion,
			RuntimeIdentityDigest:    "sha256:driver-home-identity",
			ProvisioningMarkerPath:   filepath.Join(dir, "evidence", "driver-homes", session.ID, "pi.json"),
			ProvisioningMarkerDigest: "sha256:driver-home-marker",
		},
	}, nil)
	if err != nil {
		t.Fatalf("build pi sandbox contract payload: %v", err)
	}
	mountPlan := payload["mount_plan"].(map[string]any)
	materializedMounts := mountPlan["driver_config_materializations"].(map[string]any)
	driverRuntime := payload["driver_runtime"].(map[string]any)
	materializedRuntime := driverRuntime["materialized_driver_config"].(map[string]any)
	for _, name := range []string{"models", "settings"} {
		runtimeEntry := materializedRuntime[name].(map[string]any)
		mountEntry := materializedMounts[name].(map[string]any)
		if runtimeEntry["destination_mutable_by_sandbox"] != false ||
			mountEntry["destination_mutable_by_sandbox"] != false ||
			mountEntry["mode"] != "ro" ||
			mountEntry["type"] != "bind" ||
			mountEntry["exact"] != true {
			t.Fatalf("pi materialization %s is not read-only/exact: runtime=%+v mount=%+v", name, runtimeEntry, mountEntry)
		}
		if digest, _ := runtimeEntry["source_digest"].(string); !strings.HasPrefix(digest, "sha256:") {
			t.Fatalf("pi materialization %s missing digest: %+v", name, runtimeEntry)
		}
	}
	if materializedRuntime["models"].(map[string]any)["source_projection_path"] != agents.PiModelsConfigPath ||
		materializedRuntime["settings"].(map[string]any)["source_projection_path"] != agents.PiSettingsConfigPath {
		t.Fatalf("pi materialization source projections wrong: %+v", materializedRuntime)
	}
	if _, err := st.StoreSandboxContract(ctx, store.StoreSandboxContractParams{
		ContractID:             sandboxContractID(allocation.GenerationID),
		SessionID:              session.ID,
		GenerationID:           allocation.GenerationID,
		Owner:                  store.GenerationLeaseOwner(owner.UUID),
		SandboxContractVersion: store.SandboxContractVersion,
		ContractSchemaVersion:  store.SandboxContractSchemaVersion,
		ContractGateVersion:    store.SandboxContractGateDriverManifest,
		DriverState:            allocation.DriverState,
		Payload:                payload,
		Now:                    now,
	}); err != nil {
		t.Fatalf("store pi sandbox contract: %v", err)
	}
}

func TestRuntimeConfigDigestFailsClosedOnCanonicalizationError(t *testing.T) {
	if got, err := runtimeConfigDigest(map[string]any{"invalid": func() {}}); err == nil {
		t.Fatalf("expected canonicalization error, got digest %q", got)
	}
}
