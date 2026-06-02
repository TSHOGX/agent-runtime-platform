package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/config"
	"harness-platform/orchestrator/internal/sessionstate"
	"harness-platform/orchestrator/internal/store"
)

func TestDownloadArtifactAllowsNestedRegularFile(t *testing.T) {
	srv, _, workspace := newArtifactDownloadServer(t, "sess_1")
	sessionDir := filepath.Join(workspace, "reports")
	if err := os.MkdirAll(sessionDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sessionDir, "summary.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("write artifact: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/artifacts/sess_1/reports/summary.txt", nil)
	rec := httptest.NewRecorder()

	srv.downloadArtifact(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d body %s", rec.Code, rec.Body.String())
	}
	if rec.Body.String() != "hello" {
		t.Fatalf("unexpected body %q", rec.Body.String())
	}
}

func TestDownloadArtifactRejectsMissingWorkspaceEvidence(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := testArtifactDownloadConfig(t, dir)
	if err := os.MkdirAll(filepath.Dir(cfg.DBPath), 0o755); err != nil {
		t.Fatalf("mkdir db state: %v", err)
	}
	st, err := store.Open(ctx, cfg.DBPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	sessionDir := filepath.Join(cfg.SessionsRoot, "sess_1")
	if err := os.MkdirAll(sessionDir, 0o755); err != nil {
		t.Fatalf("mkdir session dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sessionDir, "orphan.txt"), []byte("orphan"), 0o644); err != nil {
		t.Fatalf("write orphan artifact: %v", err)
	}

	srv := &Server{cfg: cfg, store: st}
	req := httptest.NewRequest(http.MethodGet, "/artifacts/sess_1/orphan.txt", nil)
	rec := httptest.NewRecorder()

	srv.downloadArtifact(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d body %s", rec.Code, rec.Body.String())
	}
}

func TestDownloadArtifactRejectsInvalidWorkspaceEvidence(t *testing.T) {
	srv, st, workspace := newArtifactDownloadServer(t, "sess_1")
	if err := os.WriteFile(filepath.Join(workspace, "report.txt"), []byte("report"), 0o644); err != nil {
		t.Fatalf("write artifact: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(context.Background(), `
UPDATE session_workspaces
SET host_path = ?
WHERE session_id = ?`, filepath.Join(filepath.Dir(workspace), "other_session"), "sess_1"); err != nil {
		t.Fatalf("corrupt workspace evidence: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/artifacts/sess_1/report.txt", nil)
	rec := httptest.NewRecorder()

	srv.downloadArtifact(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected status 403, got %d body %s", rec.Code, rec.Body.String())
	}
}

func TestDownloadArtifactRejectsSymlinkEscape(t *testing.T) {
	dir := t.TempDir()
	srv, _, workspace := newArtifactDownloadServer(t, "sess_1")
	outside := filepath.Join(dir, "outside.txt")
	if err := os.WriteFile(outside, []byte("secret"), 0o644); err != nil {
		t.Fatalf("write outside file: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(workspace, "outside.txt")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/artifacts/sess_1/outside.txt", nil)
	rec := httptest.NewRecorder()

	srv.downloadArtifact(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected status 403, got %d body %s", rec.Code, rec.Body.String())
	}
}

func TestDownloadArtifactRejectsSymlinkDirectory(t *testing.T) {
	dir := t.TempDir()
	srv, _, workspace := newArtifactDownloadServer(t, "sess_1")
	outsideDir := filepath.Join(dir, "outside")
	if err := os.MkdirAll(outsideDir, 0o755); err != nil {
		t.Fatalf("mkdir outside: %v", err)
	}
	if err := os.WriteFile(filepath.Join(outsideDir, "secret.txt"), []byte("secret"), 0o644); err != nil {
		t.Fatalf("write outside file: %v", err)
	}
	if err := os.Symlink(outsideDir, filepath.Join(workspace, "linked")); err != nil {
		t.Fatalf("symlink dir: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/artifacts/sess_1/linked/secret.txt", nil)
	rec := httptest.NewRecorder()

	srv.downloadArtifact(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected status 403, got %d body %s", rec.Code, rec.Body.String())
	}
}

func TestDownloadArtifactRejectsTraversal(t *testing.T) {
	dir := t.TempDir()
	srv := &Server{cfg: config.Config{SessionsRoot: dir}}
	req := httptest.NewRequest(http.MethodGet, "/artifacts/sess_1/../outside.txt", nil)
	rec := httptest.NewRecorder()

	srv.downloadArtifact(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d body %s", rec.Code, rec.Body.String())
	}
}

func newArtifactDownloadServer(t *testing.T, sessionID string) (*Server, *store.Store, string) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	cfg := testArtifactDownloadConfig(t, dir)
	if err := os.MkdirAll(filepath.Dir(cfg.DBPath), 0o755); err != nil {
		t.Fatalf("mkdir db state: %v", err)
	}
	st, err := store.Open(ctx, cfg.DBPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	createServerTestSession(t, ctx, st, dir, sessionID, string(sessionstate.Created), time.Now().UTC(), nil)
	srv := &Server{cfg: cfg, store: st}
	volumeConfig, err := srv.dataVolumeProvisionerConfig()
	if err != nil {
		t.Fatalf("data volume config: %v", err)
	}
	workspace, err := st.ProvisionSessionWorkspace(ctx, store.ProvisionSessionWorkspaceParams{
		SessionID: sessionID,
		Config:    volumeConfig,
		Now:       time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("provision workspace: %v", err)
	}
	return srv, st, workspace.HostPath
}

func testArtifactDownloadConfig(t *testing.T, dir string) config.Config {
	t.Helper()
	cfg := testServerConfig(dir)
	cfg.AgentHomesRoot = filepath.Join(dir, "agent-homes")
	cfg.BundleRoot = filepath.Join(dir, "bundle")
	cfg.RootFSPath = filepath.Join(dir, "rootfs")
	cfg.DBPath = filepath.Join(dir, "state", "orchestrator.db")
	cfg.RepoRoot = dir
	return cfg
}
