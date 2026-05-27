package store

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestProvisionSessionWorkspaceCreatesRowAndMarker(t *testing.T) {
	ctx := context.Background()
	st, _ := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_workspace")
	cfg := testDataVolumeConfig(t)
	now := time.Now().UTC()

	volume, err := st.ProvisionSessionWorkspace(ctx, ProvisionSessionWorkspaceParams{
		SessionID: "sess_workspace",
		Config:    cfg,
		Now:       now,
	})
	if err != nil {
		t.Fatalf("provision workspace: %v", err)
	}
	if volume.HostPath != filepath.Join(cfg.SessionsRoot, "sess_workspace") ||
		volume.ProvisioningMarkerPath != filepath.Join(cfg.EvidenceRoot, "workspaces", "sess_workspace.json") ||
		volume.LayoutVersion != DataVolumeLayoutVersion ||
		volume.SandboxUID != 7000 ||
		volume.SandboxGID != 7001 ||
		!equalIntSlices(volume.SandboxSupplementalGIDs, []int{42, 43}) {
		t.Fatalf("unexpected workspace volume: %+v", volume)
	}
	if _, err := os.Stat(volume.HostPath); err != nil {
		t.Fatalf("workspace dir missing: %v", err)
	}
	assertMarkerDigest(t, volume.ProvisioningMarkerPath, volume.ProvisioningMarkerDigest)
	marker := readMarker(t, volume.ProvisioningMarkerPath)
	if marker["volume_type"] != string(dataVolumeWorkspace) ||
		marker["session_id"] != "sess_workspace" ||
		marker["host_path"] != volume.HostPath ||
		marker["runtime_identity_digest"] != volume.RuntimeIdentityDigest {
		t.Fatalf("unexpected marker payload: %+v", marker)
	}
}

func TestProvisionSessionDriverHomeCreatesDriverScopedRows(t *testing.T) {
	ctx := context.Background()
	st, _ := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_driver_home")
	cfg := testDataVolumeConfig(t)

	claude, err := st.ProvisionSessionDriverHome(ctx, ProvisionSessionDriverHomeParams{
		SessionID: "sess_driver_home",
		Driver:    "claude",
		Config:    cfg,
		Now:       time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("provision claude driver home: %v", err)
	}
	shell, err := st.ProvisionSessionDriverHome(ctx, ProvisionSessionDriverHomeParams{
		SessionID: "sess_driver_home",
		Driver:    "sh",
		Config:    cfg,
		Now:       time.Now().UTC().Add(time.Second),
	})
	if err != nil {
		t.Fatalf("provision shell driver home: %v", err)
	}
	if claude.HostPath != filepath.Join(cfg.AgentHomesRoot, "sess_driver_home", "claude") ||
		shell.HostPath != filepath.Join(cfg.AgentHomesRoot, "sess_driver_home", "sh") ||
		claude.HostPath == shell.HostPath {
		t.Fatalf("driver home paths not scoped by driver: claude=%+v shell=%+v", claude, shell)
	}
	assertMarkerDigest(t, claude.ProvisioningMarkerPath, claude.ProvisioningMarkerDigest)
	assertMarkerDigest(t, shell.ProvisioningMarkerPath, shell.ProvisioningMarkerDigest)
}

func TestProvisionSessionWorkspaceRejectsNonEmptyFreshDirectory(t *testing.T) {
	ctx := context.Background()
	st, _ := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_non_empty")
	cfg := testDataVolumeConfig(t)
	workspacePath := filepath.Join(cfg.SessionsRoot, "sess_non_empty")
	if err := os.MkdirAll(workspacePath, 0o755); err != nil {
		t.Fatalf("mkdir workspace: %v", err)
	}
	if err := os.WriteFile(filepath.Join(workspacePath, "legacy.txt"), []byte("legacy"), 0o644); err != nil {
		t.Fatalf("write legacy content: %v", err)
	}

	_, err := st.ProvisionSessionWorkspace(ctx, ProvisionSessionWorkspaceParams{
		SessionID: "sess_non_empty",
		Config:    cfg,
		Now:       time.Now().UTC(),
	})
	if err == nil || !strings.Contains(err.Error(), "must be empty") {
		t.Fatalf("expected non-empty fresh directory rejection, got %v", err)
	}
}

func TestProvisionSessionWorkspaceIsIdempotentWithUserContent(t *testing.T) {
	ctx := context.Background()
	st, _ := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_idempotent")
	cfg := testDataVolumeConfig(t)
	now := time.Now().UTC()

	first, err := st.ProvisionSessionWorkspace(ctx, ProvisionSessionWorkspaceParams{
		SessionID: "sess_idempotent",
		Config:    cfg,
		Now:       now,
	})
	if err != nil {
		t.Fatalf("provision first workspace: %v", err)
	}
	if err := os.WriteFile(filepath.Join(first.HostPath, "report.txt"), []byte("user content"), 0o644); err != nil {
		t.Fatalf("write user content: %v", err)
	}
	second, err := st.ProvisionSessionWorkspace(ctx, ProvisionSessionWorkspaceParams{
		SessionID: "sess_idempotent",
		Config:    cfg,
		Now:       now.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("provision idempotent workspace: %v", err)
	}
	if second.ProvisioningMarkerDigest != first.ProvisioningMarkerDigest ||
		!second.ProvisionedAt.Equal(first.ProvisionedAt) {
		t.Fatalf("idempotent provision changed evidence: first=%+v second=%+v", first, second)
	}
}

func TestProvisionSessionWorkspaceRejectsMarkerTampering(t *testing.T) {
	ctx := context.Background()
	st, _ := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_tamper")
	cfg := testDataVolumeConfig(t)

	volume, err := st.ProvisionSessionWorkspace(ctx, ProvisionSessionWorkspaceParams{
		SessionID: "sess_tamper",
		Config:    cfg,
		Now:       time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("provision workspace: %v", err)
	}
	if err := os.WriteFile(volume.ProvisioningMarkerPath, []byte(`{"tampered":true}`), 0o644); err != nil {
		t.Fatalf("tamper marker: %v", err)
	}

	_, err = st.ProvisionSessionWorkspace(ctx, ProvisionSessionWorkspaceParams{
		SessionID: "sess_tamper",
		Config:    cfg,
		Now:       time.Now().UTC().Add(time.Second),
	})
	if err == nil || !strings.Contains(err.Error(), "marker digest mismatch") {
		t.Fatalf("expected marker digest mismatch, got %v", err)
	}
}

func testDataVolumeConfig(t *testing.T) DataVolumeProvisionerConfig {
	t.Helper()
	dir := t.TempDir()
	return DataVolumeProvisionerConfig{
		SessionsRoot:   filepath.Join(dir, "sessions"),
		AgentHomesRoot: filepath.Join(dir, "agent-homes"),
		EvidenceRoot:   filepath.Join(dir, "evidence"),
		RuntimeIdentity: RuntimeIdentity{
			UID:              7000,
			GID:              7001,
			SupplementalGIDs: []int{43, 42},
		},
	}
}

func assertMarkerDigest(t *testing.T, path, digest string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read marker %s: %v", path, err)
	}
	if got := SandboxContractDigest(data); got != digest {
		t.Fatalf("marker digest = %s, want %s", got, digest)
	}
}

func readMarker(t *testing.T, path string) map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read marker %s: %v", path, err)
	}
	var marker map[string]any
	if err := json.Unmarshal(data, &marker); err != nil {
		t.Fatalf("decode marker %s: %v", path, err)
	}
	return marker
}
