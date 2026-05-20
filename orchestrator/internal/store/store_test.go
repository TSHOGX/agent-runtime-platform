package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestListMessagesAndUUID(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	ctx := context.Background()
	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	now := time.Now().UTC()
	want := Session{
		ID:                "sess_1",
		UserID:            "lab",
		Status:            "created",
		Agent:             "claude",
		Workspace:         dir,
		RestoreID:         "phase3-sess_1",
		ClaudeSessionUUID: "11111111-2222-3333-4444-555555555555",
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	if err := st.CreateSession(ctx, want); err != nil {
		t.Fatalf("create session: %v", err)
	}

	got, err := st.GetSession(ctx, want.ID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if got.ClaudeSessionUUID != want.ClaudeSessionUUID {
		t.Fatalf("uuid: want %q, got %q", want.ClaudeSessionUUID, got.ClaudeSessionUUID)
	}

	if _, err := st.AddMessage(ctx, want.ID, "user", "hello"); err != nil {
		t.Fatalf("add user: %v", err)
	}
	if _, err := st.AddMessage(ctx, want.ID, "assistant", "world"); err != nil {
		t.Fatalf("add assistant: %v", err)
	}

	msgs, err := st.ListMessages(ctx, want.ID)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("want 2 messages, got %d", len(msgs))
	}
	if msgs[0].Role != "user" || msgs[0].Content != "hello" {
		t.Fatalf("msg[0] mismatch: %+v", msgs[0])
	}
	if msgs[1].Role != "assistant" || msgs[1].Content != "world" {
		t.Fatalf("msg[1] mismatch: %+v", msgs[1])
	}
	if msgs[0].ID >= msgs[1].ID {
		t.Fatalf("messages should be ordered by id ascending")
	}
}

func TestEnsureColumnIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	ctx := context.Background()
	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()

	if err := st.ensureColumn(ctx, "sessions", "claude_session_uuid", "TEXT"); err != nil {
		t.Fatalf("second ensureColumn call should be a no-op, got %v", err)
	}
}

func TestUpdateSessionStatusAndActivity(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	ctx := context.Background()
	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	now := time.Now().UTC()
	session := Session{
		ID:        "sess_test",
		UserID:    "lab",
		Status:    "created",
		Agent:     "claude",
		Workspace: dir,
		RestoreID: "phase3-sess_test",
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := st.CreateSession(ctx, session); err != nil {
		t.Fatalf("create session: %v", err)
	}

	// Update status and activity
	activityTime := now.Add(5 * time.Minute)
	restoreMS := int64(250)
	if err := st.UpdateSessionStatusAndActivity(ctx, session.ID, "running_idle", &restoreMS, activityTime); err != nil {
		t.Fatalf("update status and activity: %v", err)
	}

	// Verify update
	got, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if got.Status != "running_idle" {
		t.Errorf("status: want running_idle, got %s", got.Status)
	}
	if got.RestoreMS == nil || *got.RestoreMS != 250 {
		t.Errorf("restore_ms: want 250, got %v", got.RestoreMS)
	}
	if got.LastActivityAt == nil {
		t.Fatalf("last_activity_at should not be nil")
	}
	if got.LastActivityAt.Sub(activityTime).Abs() > time.Second {
		t.Errorf("last_activity_at: want %v, got %v", activityTime, *got.LastActivityAt)
	}
}

func TestListSessionsByStatus(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	ctx := context.Background()
	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	now := time.Now().UTC()

	// Create sessions with different statuses
	sessions := []Session{
		{
			ID:        "sess_1",
			UserID:    "lab",
			Status:    "running_idle",
			Agent:     "claude",
			Workspace: dir,
			RestoreID: "phase3-sess_1",
			CreatedAt: now,
			UpdatedAt: now,
		},
		{
			ID:        "sess_2",
			UserID:    "lab",
			Status:    "running_active",
			Agent:     "claude",
			Workspace: dir,
			RestoreID: "phase3-sess_2",
			CreatedAt: now,
			UpdatedAt: now,
		},
		{
			ID:        "sess_3",
			UserID:    "lab",
			Status:    "running_idle",
			Agent:     "claude",
			Workspace: dir,
			RestoreID: "phase3-sess_3",
			CreatedAt: now,
			UpdatedAt: now,
		},
	}

	for _, s := range sessions {
		if err := st.CreateSession(ctx, s); err != nil {
			t.Fatalf("create session %s: %v", s.ID, err)
		}
	}

	// Update activity times
	activity1 := now.Add(-10 * time.Minute)
	activity3 := now.Add(-35 * time.Minute)
	if err := st.UpdateSessionStatusAndActivity(ctx, "sess_1", "running_idle", nil, activity1); err != nil {
		t.Fatalf("update sess_1: %v", err)
	}
	if err := st.UpdateSessionStatusAndActivity(ctx, "sess_3", "running_idle", nil, activity3); err != nil {
		t.Fatalf("update sess_3: %v", err)
	}

	// List idle sessions
	idleSessions, err := st.ListSessionsByStatus(ctx, "running_idle")
	if err != nil {
		t.Fatalf("list sessions by status: %v", err)
	}

	if len(idleSessions) != 2 {
		t.Fatalf("want 2 idle sessions, got %d", len(idleSessions))
	}

	// Should be ordered by last_activity_at ASC (oldest first)
	if idleSessions[0].ID != "sess_3" {
		t.Errorf("first idle session should be sess_3, got %s", idleSessions[0].ID)
	}
	if idleSessions[1].ID != "sess_1" {
		t.Errorf("second idle session should be sess_1, got %s", idleSessions[1].ID)
	}
}
