package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"harness-platform/orchestrator/internal/agents"
)

func TestInterruptSessionUsesFrozenPlanFeaturePolicy(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	cfg := testServerConfig(dir)
	session, _ := createServerPlannedActiveGeneration(t, ctx, st, cfg, owner.UUID, dir, "sess_interrupt_shell", agents.Shell)
	rt := &recordingRuntime{}
	srv := &Server{store: st, runtime: rt}

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/interrupt", nil)
	rec := httptest.NewRecorder()
	srv.interruptSession(rec, req, session.ID)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected interrupt status 202, got %d body %s", rec.Code, rec.Body.String())
	}
	if len(rt.interruptSessionIDs) != 1 || rt.interruptSessionIDs[0] != session.ID {
		t.Fatalf("runtime interrupt calls = %+v want %s", rt.interruptSessionIDs, session.ID)
	}
}

func TestInterruptSessionRejectsFrozenUnsupportedFeature(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	cfg := testServerConfig(dir)
	session, _ := createServerPlannedActiveGeneration(t, ctx, st, cfg, owner.UUID, dir, "sess_interrupt_agent", agents.ClaudeCode)
	rt := &recordingRuntime{}
	srv := &Server{store: st, runtime: rt}

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/interrupt", nil)
	rec := httptest.NewRecorder()
	srv.interruptSession(rec, req, session.ID)
	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "interrupt is not supported") {
		t.Fatalf("expected unsupported interrupt conflict, got %d body %s", rec.Code, rec.Body.String())
	}
	if len(rt.interruptSessionIDs) != 0 {
		t.Fatalf("runtime interrupt should not be called: %+v", rt.interruptSessionIDs)
	}
}
