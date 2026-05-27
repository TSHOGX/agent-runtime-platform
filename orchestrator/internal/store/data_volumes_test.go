package store

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"syscall"
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
	assertRootOwnedProtectedDir(t, cfg.EvidenceRoot)
	assertRootOwnedProtectedDir(t, filepath.Join(cfg.EvidenceRoot, "workspaces"))
	assertRootOwnedProtectedFile(t, volume.ProvisioningMarkerPath)
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

func TestProvisionSessionWorkspaceRejectsWritableEvidenceRoot(t *testing.T) {
	ctx := context.Background()
	st, _ := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_writable_evidence")
	cfg := testDataVolumeConfig(t)
	if err := os.MkdirAll(cfg.EvidenceRoot, 0o755); err != nil {
		t.Fatalf("mkdir evidence root: %v", err)
	}
	if err := os.Chmod(cfg.EvidenceRoot, 0o777); err != nil {
		t.Fatalf("chmod evidence root: %v", err)
	}

	_, err := st.ProvisionSessionWorkspace(ctx, ProvisionSessionWorkspaceParams{
		SessionID: "sess_writable_evidence",
		Config:    cfg,
		Now:       time.Now().UTC(),
	})
	if err == nil || !strings.Contains(err.Error(), "must not be group/world writable") {
		t.Fatalf("expected writable evidence root rejection, got %v", err)
	}
}

func TestProvisionSessionWorkspaceRejectsNonRootEvidenceRoot(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root to create a non-root-owned evidence root")
	}
	ctx := context.Background()
	st, _ := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_nonroot_evidence")
	cfg := testDataVolumeConfig(t)
	if err := os.MkdirAll(cfg.EvidenceRoot, 0o755); err != nil {
		t.Fatalf("mkdir evidence root: %v", err)
	}
	if err := os.Chown(cfg.EvidenceRoot, 12345, 12345); err != nil {
		t.Fatalf("chown evidence root: %v", err)
	}

	_, err := st.ProvisionSessionWorkspace(ctx, ProvisionSessionWorkspaceParams{
		SessionID: "sess_nonroot_evidence",
		Config:    cfg,
		Now:       time.Now().UTC(),
	})
	if err == nil || !strings.Contains(err.Error(), "must be root-owned") {
		t.Fatalf("expected non-root evidence root rejection, got %v", err)
	}
}

func TestProvisionSessionWorkspaceRejectsSymlinkedMarkerDir(t *testing.T) {
	ctx := context.Background()
	st, _ := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_symlink_marker")
	cfg := testDataVolumeConfig(t)
	if err := os.MkdirAll(cfg.EvidenceRoot, 0o755); err != nil {
		t.Fatalf("mkdir evidence root: %v", err)
	}
	target := filepath.Join(t.TempDir(), "marker-target")
	if err := os.MkdirAll(target, 0o755); err != nil {
		t.Fatalf("mkdir marker target: %v", err)
	}
	if err := os.Symlink(target, filepath.Join(cfg.EvidenceRoot, "workspaces")); err != nil {
		t.Fatalf("symlink marker dir: %v", err)
	}

	_, err := st.ProvisionSessionWorkspace(ctx, ProvisionSessionWorkspaceParams{
		SessionID: "sess_symlink_marker",
		Config:    cfg,
		Now:       time.Now().UTC(),
	})
	if err == nil || !strings.Contains(err.Error(), "must not be a symlink") {
		t.Fatalf("expected symlink marker dir rejection, got %v", err)
	}
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

func TestVerifySessionWorkspaceVolumeRejectsConfigMismatch(t *testing.T) {
	ctx := context.Background()
	st, _ := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_config_mismatch")
	cfg := testDataVolumeConfig(t)
	if _, err := st.ProvisionSessionWorkspace(ctx, ProvisionSessionWorkspaceParams{
		SessionID: "sess_config_mismatch",
		Config:    cfg,
		Now:       time.Now().UTC(),
	}); err != nil {
		t.Fatalf("provision workspace: %v", err)
	}

	other := cfg
	other.SessionsRoot = filepath.Join(t.TempDir(), "sessions")
	_, err := st.VerifySessionWorkspaceVolume(ctx, VerifySessionWorkspaceVolumeParams{
		SessionID: "sess_config_mismatch",
		Config:    other,
	})
	if err == nil || !strings.Contains(err.Error(), "expected provisioning config") {
		t.Fatalf("expected config mismatch, got %v", err)
	}
}

func TestVerifySessionWorkspaceVolumeRejectsMarkerPayloadMismatch(t *testing.T) {
	ctx := context.Background()
	st, _ := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_marker_mismatch")
	cfg := testDataVolumeConfig(t)
	volume, err := st.ProvisionSessionWorkspace(ctx, ProvisionSessionWorkspaceParams{
		SessionID: "sess_marker_mismatch",
		Config:    cfg,
		Now:       time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("provision workspace: %v", err)
	}

	marker := dataVolumeMarker{
		MarkerVersion: 1,
		VolumeType:    string(dataVolumeWorkspace),
		SessionID:     "sess_marker_mismatch",
		HostPath:      filepath.Join(cfg.SessionsRoot, "other_session"),
		LayoutVersion: DataVolumeLayoutVersion,
		RuntimeIdentity: dataVolumeIdentityJSON{
			SandboxUID:              volume.SandboxUID,
			SandboxGID:              volume.SandboxGID,
			SandboxSupplementalGIDs: volume.SandboxSupplementalGIDs,
		},
		RuntimeIdentityDigest: volume.RuntimeIdentityDigest,
		ProvisionedAt:         formatTime(volume.ProvisionedAt),
	}
	payload, err := canonicalDataVolumeJSON(marker)
	if err != nil {
		t.Fatalf("canonical marker: %v", err)
	}
	digest := SandboxContractDigest(payload)
	if err := os.WriteFile(volume.ProvisioningMarkerPath, payload, 0o644); err != nil {
		t.Fatalf("write mismatched marker: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `
UPDATE session_workspaces
SET provisioning_marker_digest = ?
WHERE session_id = ?`, digest, volume.SessionID); err != nil {
		t.Fatalf("update marker digest: %v", err)
	}

	_, err = st.VerifySessionWorkspaceVolume(ctx, VerifySessionWorkspaceVolumeParams{
		SessionID: volume.SessionID,
		Config:    cfg,
	})
	if err == nil || !strings.Contains(err.Error(), "does not match expected provisioning evidence") {
		t.Fatalf("expected marker payload mismatch, got %v", err)
	}
}

func TestVerifySessionDriverHomeVolumeRejectsConfigMismatch(t *testing.T) {
	ctx := context.Background()
	st, _ := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_driver_verify")
	cfg := testDataVolumeConfig(t)
	if _, err := st.ProvisionSessionDriverHome(ctx, ProvisionSessionDriverHomeParams{
		SessionID: "sess_driver_verify",
		Driver:    "claude",
		Config:    cfg,
		Now:       time.Now().UTC(),
	}); err != nil {
		t.Fatalf("provision driver home: %v", err)
	}

	other := cfg
	other.AgentHomesRoot = filepath.Join(t.TempDir(), "agent-homes")
	_, err := st.VerifySessionDriverHomeVolume(ctx, VerifySessionDriverHomeVolumeParams{
		SessionID: "sess_driver_verify",
		Driver:    "claude",
		Config:    other,
	})
	if err == nil || !strings.Contains(err.Error(), "expected provisioning config") {
		t.Fatalf("expected driver home config mismatch, got %v", err)
	}
}

func TestNormalizeDataVolumeConfigRejectsOverlappingRoots(t *testing.T) {
	cfg := testDataVolumeConfig(t)
	tests := []struct {
		name   string
		mutate func(*DataVolumeProvisionerConfig)
		want   string
	}{
		{
			name: "evidence under sessions",
			mutate: func(cfg *DataVolumeProvisionerConfig) {
				cfg.EvidenceRoot = filepath.Join(cfg.SessionsRoot, ".evidence")
			},
			want: "overlaps sessions root",
		},
		{
			name: "evidence under agent homes",
			mutate: func(cfg *DataVolumeProvisionerConfig) {
				cfg.EvidenceRoot = filepath.Join(cfg.AgentHomesRoot, ".evidence")
			},
			want: "overlaps agent homes root",
		},
		{
			name: "sessions under agent homes",
			mutate: func(cfg *DataVolumeProvisionerConfig) {
				cfg.SessionsRoot = filepath.Join(cfg.AgentHomesRoot, "sessions")
			},
			want: "overlaps agent homes root",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := cfg
			tc.mutate(&cfg)
			_, err := normalizeDataVolumeConfig(cfg)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected %q rejection, got %v", tc.want, err)
			}
		})
	}
}

func TestNormalizeDataVolumeConfigResolvesSymlinkedRoots(t *testing.T) {
	dir := t.TempDir()
	realSessions := filepath.Join(dir, "real-sessions")
	if err := os.MkdirAll(realSessions, 0o755); err != nil {
		t.Fatalf("mkdir real sessions: %v", err)
	}
	linkSessions := filepath.Join(dir, "sessions-link")
	if err := os.Symlink(realSessions, linkSessions); err != nil {
		t.Fatalf("symlink sessions root: %v", err)
	}
	cfg := DataVolumeProvisionerConfig{
		SessionsRoot:    linkSessions,
		AgentHomesRoot:  filepath.Join(dir, "agent-homes"),
		EvidenceRoot:    filepath.Join(dir, "evidence"),
		RuntimeIdentity: RuntimeIdentity{UID: 7000, GID: 7001},
	}
	normalized, err := normalizeDataVolumeConfig(cfg)
	if err != nil {
		t.Fatalf("normalize data volume config: %v", err)
	}
	if normalized.SessionsRoot != realSessions {
		t.Fatalf("sessions root was not canonicalized through symlink: got %q want %q", normalized.SessionsRoot, realSessions)
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

func assertRootOwnedProtectedDir(t *testing.T, path string) {
	t.Helper()
	assertRootOwnedProtectedPath(t, path, true)
}

func assertRootOwnedProtectedFile(t *testing.T, path string) {
	t.Helper()
	assertRootOwnedProtectedPath(t, path, false)
}

func assertRootOwnedProtectedPath(t *testing.T, path string, wantDir bool) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("stat protected path %s: %v", path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("protected path %s is a symlink", path)
	}
	if wantDir && !info.IsDir() {
		t.Fatalf("protected path %s is not a directory", path)
	}
	if !wantDir && !info.Mode().IsRegular() {
		t.Fatalf("protected path %s is not a regular file", path)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("stat ownership unavailable for %s", path)
	}
	if stat.Uid != 0 {
		t.Fatalf("protected path %s uid=%d want 0", path, stat.Uid)
	}
	if info.Mode().Perm()&0o022 != 0 {
		t.Fatalf("protected path %s mode=%#o is group/world writable", path, info.Mode().Perm())
	}
}
