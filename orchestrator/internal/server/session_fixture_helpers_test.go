package server

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/config"
	"harness-platform/orchestrator/internal/sessionstate"
	"harness-platform/orchestrator/internal/store"
)

func openServerOwnedStore(t *testing.T, ctx context.Context, dir string) (*store.Store, *store.OwnerLock) {
	t.Helper()
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	owner, err := store.AcquireOwnerLock(filepath.Join(dir, "run"))
	if err != nil {
		t.Fatalf("acquire owner: %v", err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	if err := st.WriteOwner(ctx, owner); err != nil {
		t.Fatalf("write owner: %v", err)
	}
	return st, owner
}

func createServerTestSession(t *testing.T, ctx context.Context, st *store.Store, dir, id, status string, now time.Time, expiresAt *time.Time) store.Session {
	t.Helper()
	session := store.Session{
		ID:        id,
		UserID:    labUserID,
		Status:    status,
		DriverID:  "claude_code",
		Mode:      store.ModeForDriver("claude_code"),
		CreatedAt: now,
		UpdatedAt: now,
		ExpiresAt: expiresAt,
	}
	if err := os.MkdirAll(filepath.Join(dir, "sessions", id), 0o755); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := st.CreateSession(ctx, session); err != nil {
		t.Fatalf("create session: %v", err)
	}
	return session
}

func createServerPlannedActiveGeneration(t *testing.T, ctx context.Context, st *store.Store, cfg config.Config, ownerUUID, dir, sessionID string, driver agents.ID) (store.Session, store.GenerationAllocation) {
	t.Helper()
	now := time.Now().UTC()
	driverID := string(driver)
	session := store.Session{
		ID:        sessionID,
		UserID:    labUserID,
		Status:    string(sessionstate.Created),
		DriverID:  driverID,
		Mode:      store.ModeForDriver(driverID),
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := os.MkdirAll(filepath.Join(dir, "sessions", sessionID), 0o755); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := st.CreateSession(ctx, session); err != nil {
		t.Fatalf("create planned active session: %v", err)
	}
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: sessionID,
		Owner:     store.GenerationLeaseOwner(ownerUUID),
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    serverTestAllocatorConfig(cfg, driverID),
	})
	if err != nil {
		t.Fatalf("allocate planned active generation: %v", err)
	}
	artifacts := testGenerationArtifacts()
	recordServerRuntimeArtifacts(t, ctx, st, allocation.GenerationID, artifacts.ManifestDigest, artifacts.RunscVersion)
	if err := st.MarkGenerationResourcesLive(ctx, sessionID, allocation.GenerationID, allocation.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark planned active generation live: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE sessions
SET status = ?,
    updated_at = ?
WHERE id = ?`, string(sessionstate.RunningActive), now.Add(2*time.Second).Format(time.RFC3339Nano), sessionID); err != nil {
		t.Fatalf("mark planned active session running: %v", err)
	}
	session.Status = string(sessionstate.RunningActive)
	session.ActiveGenerationID = allocation.GenerationID
	session.UpdatedAt = now.Add(2 * time.Second)
	return session, allocation
}
