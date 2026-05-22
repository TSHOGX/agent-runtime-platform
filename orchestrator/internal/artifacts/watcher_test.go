package artifacts

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/events"
	"harness-platform/orchestrator/internal/sessionstate"
	"harness-platform/orchestrator/internal/store"
)

func TestScanSessionSkipsSymlinkArtifacts(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	st := newArtifactTestStore(t, ctx, root, "sess_1")
	w := New(root, st, events.NewHub(), slog.Default())

	sessionDir := filepath.Join(root, "sess_1")
	if err := os.MkdirAll(filepath.Join(sessionDir, "reports"), 0o755); err != nil {
		t.Fatalf("mkdir reports: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sessionDir, "reports", "summary.txt"), []byte("ok"), 0o644); err != nil {
		t.Fatalf("write artifact: %v", err)
	}
	outside := filepath.Join(root, "outside.txt")
	if err := os.WriteFile(outside, []byte("secret"), 0o644); err != nil {
		t.Fatalf("write outside: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(sessionDir, "linked.txt")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	if err := w.ScanSession(ctx, "sess_1"); err != nil {
		t.Fatalf("scan session: %v", err)
	}
	artifacts, err := st.ListArtifacts(ctx, "sess_1")
	if err != nil {
		t.Fatalf("list artifacts: %v", err)
	}
	if len(artifacts) != 1 || artifacts[0].Path != "reports/summary.txt" {
		t.Fatalf("unexpected artifacts: %+v", artifacts)
	}
}

func TestDeletePathRemovesFileAndDescendants(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	st := newArtifactTestStore(t, ctx, root, "sess_1")
	w := New(root, st, events.NewHub(), slog.Default())

	now := time.Now().UTC()
	for _, path := range []string{"keep.txt", "reports/a.txt", "reports/nested/b.txt"} {
		if err := st.UpsertArtifact(ctx, store.Artifact{
			SessionID: "sess_1",
			Path:      path,
			Size:      int64(len(path)),
			ModTime:   now,
		}); err != nil {
			t.Fatalf("upsert %s: %v", path, err)
		}
	}

	w.deletePath(ctx, filepath.Join(root, "sess_1", "reports"))

	artifacts, err := st.ListArtifacts(ctx, "sess_1")
	if err != nil {
		t.Fatalf("list artifacts: %v", err)
	}
	if len(artifacts) != 1 || artifacts[0].Path != "keep.txt" {
		t.Fatalf("unexpected artifacts after delete: %+v", artifacts)
	}
}

func newArtifactTestStore(t *testing.T, ctx context.Context, workspaceRoot, sessionID string) *store.Store {
	t.Helper()

	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	now := time.Now().UTC()
	if err := st.CreateSession(ctx, store.Session{
		ID:        sessionID,
		UserID:    "lab",
		Status:    string(sessionstate.Created),
		Agent:     "claude",
		Workspace: filepath.Join(workspaceRoot, sessionID),
		RestoreID: "phase3-" + sessionID,
		CreatedAt: now,
		UpdatedAt: now,
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	return st
}
