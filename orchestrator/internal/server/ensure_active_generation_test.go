package server

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/events"
	"harness-platform/orchestrator/internal/sessionstate"
	"harness-platform/orchestrator/internal/store"
)

func TestEnsureActiveGenerationRequiresPersistedSessionMode(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	cfg := testServerConfig(dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_missing_mode_start", string(sessionstate.Created), time.Now().UTC(), nil)
	session.Mode = ""
	srv := &Server{
		cfg:   cfg,
		store: st,
		hub:   events.NewHub(),
		log:   slog.Default(),
	}

	if _, err := srv.ensureActiveGeneration(ctx, session, store.GenerationLeaseOwner(owner.UUID)); err == nil || !strings.Contains(err.Error(), "session mode is required") {
		t.Fatalf("expected missing session mode error, got %v", err)
	}
	var count int
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_generations WHERE session_id = ?`, session.ID).Scan(&count); err != nil {
		t.Fatalf("count runtime generations: %v", err)
	}
	if count != 0 {
		t.Fatalf("missing mode session should not allocate generation, got %d", count)
	}
}
