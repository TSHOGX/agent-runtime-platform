package server

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/artifacts"
	"harness-platform/orchestrator/internal/config"
	"harness-platform/orchestrator/internal/events"
	"harness-platform/orchestrator/internal/runtime"
	"harness-platform/orchestrator/internal/sessionstate"
	"harness-platform/orchestrator/internal/store"
)

func TestCreateSessionRejectsUnsupportedAgent(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	srv := &Server{
		cfg: config.Config{
			SessionsRoot: dir,
			SessionTTL:   time.Hour,
			MaxSessions:  10,
			DefaultAgent: "claude",
		},
		store:   st,
		runtime: runtime.New(runtime.Config{}),
		watcher: artifacts.New(dir, st, events.NewHub(), slog.Default()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}

	req := httptest.NewRequest(http.MethodPost, "/api/sessions", strings.NewReader(`{"agent":"opencode"}`))
	rec := httptest.NewRecorder()

	srv.createSession(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "unsupported agent") {
		t.Fatalf("expected unsupported agent error, got %s", rec.Body.String())
	}
}

func TestMonitorIdleSessionsSkipsHostNetwork(t *testing.T) {
	srv := &Server{
		cfg: config.Config{
			RunscNetwork: "host",
		},
		log: slog.Default(),
	}

	if err := srv.MonitorIdleSessions(context.Background()); err != nil {
		t.Fatalf("expected idle monitor to exit cleanly in host mode, got %v", err)
	}
}

func TestMonitorIdleSessionsReEnablesCheckpointedSessionsWhenCheckpointDisabled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	now := time.Now().UTC()
	session := store.Session{
		ID:        "sess_checkpointed",
		UserID:    "lab",
		Status:    string(sessionstate.Checkpointed),
		Agent:     "claude",
		Workspace: filepath.Join(dir, "sessions", "sess_checkpointed"),
		RestoreID: "phase3-sess_checkpointed",
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := st.CreateSession(ctx, session); err != nil {
		t.Fatalf("create session: %v", err)
	}

	srv := &Server{
		cfg: config.Config{
			RunscNetwork:    "sandbox",
			CheckpointsRoot: filepath.Join(dir, "checkpoints"),
		},
		store: st,
		hub:   events.NewHub(),
		log:   slog.Default(),
	}
	if err := srv.MonitorIdleSessions(ctx); err != nil {
		t.Fatalf("monitor idle sessions: %v", err)
	}
	got, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if got.Status != string(sessionstate.RunningIdle) {
		t.Fatalf("want running_idle, got %s", got.Status)
	}
}

func TestReconcileCheckpointingSessionsMarksCompleteCheckpoint(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	now := time.Now().UTC()
	session := store.Session{
		ID:        "sess_complete",
		UserID:    "lab",
		Status:    string(sessionstate.Checkpointing),
		Agent:     "claude",
		Workspace: filepath.Join(dir, "sessions", "sess_complete"),
		RestoreID: "phase3-sess_complete",
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := st.CreateSession(ctx, session); err != nil {
		t.Fatalf("create session: %v", err)
	}
	checkpointPath := filepath.Join(dir, "checkpoints", session.ID)
	if err := os.MkdirAll(checkpointPath, 0o755); err != nil {
		t.Fatalf("create checkpoint dir: %v", err)
	}
	for _, name := range []string{"checkpoint.img", "pages.img", "pages_meta.img"} {
		if err := os.WriteFile(filepath.Join(checkpointPath, name), []byte("x"), 0o644); err != nil {
			t.Fatalf("write checkpoint file %s: %v", name, err)
		}
	}

	srv := &Server{
		cfg: config.Config{
			CheckpointsRoot: filepath.Join(dir, "checkpoints"),
		},
		store: st,
		hub:   events.NewHub(),
		log:   slog.Default(),
	}

	if err := srv.reconcileCheckpointingSessions(ctx); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if got.Status != string(sessionstate.Checkpointed) {
		t.Fatalf("want checkpointed, got %s", got.Status)
	}
}

func TestReconcileCheckpointingSessionsRevertsIncompleteCheckpoint(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	now := time.Now().UTC()
	freshSession := store.Session{
		ID:        "sess_fresh",
		UserID:    "lab",
		Status:    string(sessionstate.Checkpointing),
		Agent:     "claude",
		Workspace: filepath.Join(dir, "sessions", "sess_fresh"),
		RestoreID: "phase3-sess_fresh",
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := st.CreateSession(ctx, freshSession); err != nil {
		t.Fatalf("create fresh session: %v", err)
	}
	if err := st.UpdateSessionStatus(ctx, freshSession.ID, string(sessionstate.Checkpointing), nil); err != nil {
		t.Fatalf("refresh checkpointing status: %v", err)
	}

	staleSession := store.Session{
		ID:        "sess_incomplete",
		UserID:    "lab",
		Status:    string(sessionstate.Checkpointing),
		Agent:     "claude",
		Workspace: filepath.Join(dir, "sessions", "sess_incomplete"),
		RestoreID: "phase3-sess_incomplete",
		CreatedAt: now.Add(-(checkpointTimeout + time.Minute)),
		UpdatedAt: now.Add(-(checkpointTimeout + time.Minute)),
	}
	if err := st.CreateSession(ctx, staleSession); err != nil {
		t.Fatalf("create stale session: %v", err)
	}

	srv := &Server{
		cfg: config.Config{
			CheckpointsRoot: filepath.Join(dir, "checkpoints"),
		},
		store: st,
		hub:   events.NewHub(),
		log:   slog.Default(),
	}

	if err := srv.reconcileCheckpointingSessions(ctx); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got, err := st.GetSession(ctx, freshSession.ID)
	if err != nil {
		t.Fatalf("get fresh session: %v", err)
	}
	if got.Status != string(sessionstate.Checkpointing) {
		t.Fatalf("fresh checkpointing session should be left alone, got %s", got.Status)
	}
	got, err = st.GetSession(ctx, staleSession.ID)
	if err != nil {
		t.Fatalf("get stale session: %v", err)
	}
	if got.Status != string(sessionstate.RunningIdle) {
		t.Fatalf("want running_idle, got %s", got.Status)
	}
}

func TestDownloadArtifactAllowsNestedRegularFile(t *testing.T) {
	dir := t.TempDir()
	sessionDir := filepath.Join(dir, "sess_1", "reports")
	if err := os.MkdirAll(sessionDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sessionDir, "summary.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("write artifact: %v", err)
	}

	srv := &Server{cfg: config.Config{SessionsRoot: dir}}
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

func TestDownloadArtifactRejectsSymlinkEscape(t *testing.T) {
	dir := t.TempDir()
	sessionDir := filepath.Join(dir, "sess_1")
	if err := os.MkdirAll(sessionDir, 0o755); err != nil {
		t.Fatalf("mkdir session: %v", err)
	}
	outside := filepath.Join(dir, "outside.txt")
	if err := os.WriteFile(outside, []byte("secret"), 0o644); err != nil {
		t.Fatalf("write outside file: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(sessionDir, "outside.txt")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	srv := &Server{cfg: config.Config{SessionsRoot: dir}}
	req := httptest.NewRequest(http.MethodGet, "/artifacts/sess_1/outside.txt", nil)
	rec := httptest.NewRecorder()

	srv.downloadArtifact(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected status 403, got %d body %s", rec.Code, rec.Body.String())
	}
}

func TestDownloadArtifactRejectsSymlinkDirectory(t *testing.T) {
	dir := t.TempDir()
	sessionDir := filepath.Join(dir, "sess_1")
	outsideDir := filepath.Join(dir, "outside")
	if err := os.MkdirAll(sessionDir, 0o755); err != nil {
		t.Fatalf("mkdir session: %v", err)
	}
	if err := os.MkdirAll(outsideDir, 0o755); err != nil {
		t.Fatalf("mkdir outside: %v", err)
	}
	if err := os.WriteFile(filepath.Join(outsideDir, "secret.txt"), []byte("secret"), 0o644); err != nil {
		t.Fatalf("write outside file: %v", err)
	}
	if err := os.Symlink(outsideDir, filepath.Join(sessionDir, "linked")); err != nil {
		t.Fatalf("symlink dir: %v", err)
	}

	srv := &Server{cfg: config.Config{SessionsRoot: dir}}
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
