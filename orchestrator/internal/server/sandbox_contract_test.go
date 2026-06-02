package server

import (
	"path/filepath"
	"strings"
	"testing"

	"harness-platform/orchestrator/internal/runtime"
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

func TestRuntimeConfigDigestFailsClosedOnCanonicalizationError(t *testing.T) {
	if got, err := runtimeConfigDigest(map[string]any{"invalid": func() {}}); err == nil {
		t.Fatalf("expected canonicalization error, got digest %q", got)
	}
}
