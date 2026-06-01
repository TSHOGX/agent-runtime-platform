package store

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/sessionstate"
)

func TestModeForDriverUsesDriverRegistryKind(t *testing.T) {
	tests := []struct {
		driverID string
		want     string
	}{
		{driverID: "claude_code", want: "agent"},
		{driverID: "pi", want: "agent"},
		{driverID: "sh", want: "shell"},
		{driverID: "unknown", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.driverID, func(t *testing.T) {
			if got := ModeForDriver(tt.driverID); got != tt.want {
				t.Fatalf("ModeForDriver(%q)=%q want %q", tt.driverID, got, tt.want)
			}
		})
	}
}

func TestListMessages(t *testing.T) {
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
		ID:        "sess_1",
		UserID:    "lab",
		Status:    string(sessionstate.Created),
		Agent:     "claude_code",
		RestoreID: "phase3-sess_1",
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := st.CreateSession(ctx, want); err != nil {
		t.Fatalf("create session: %v", err)
	}

	got, err := st.GetSession(ctx, want.ID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if got.ID != want.ID || got.Agent != want.Agent || got.RestoreID != want.RestoreID {
		t.Fatalf("session mismatch: got=%+v want=%+v", got, want)
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

func TestCreateSessionDoesNotWriteLegacyAgentHomePathColumn(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	ctx := context.Background()
	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if _, err := st.db.ExecContext(ctx, `ALTER TABLE sessions ADD COLUMN agent_home_path TEXT`); err != nil {
		t.Fatalf("add legacy agent_home_path column: %v", err)
	}

	now := time.Now().UTC()
	if err := st.CreateSession(ctx, Session{
		ID:        "sess_legacy_home",
		UserID:    "lab",
		Status:    string(sessionstate.Created),
		Agent:     "claude_code",
		RestoreID: "phase3-sess_legacy_home",
		CreatedAt: now,
		UpdatedAt: now,
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	var nonNullCount int
	if err := st.db.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM sessions
WHERE id = ?
  AND agent_home_path IS NOT NULL`, "sess_legacy_home").Scan(&nonNullCount); err != nil {
		t.Fatalf("query legacy agent_home_path: %v", err)
	}
	if nonNullCount != 0 {
		t.Fatalf("new session wrote legacy agent_home_path")
	}
}

func TestFreshSchemaDoesNotCreateLegacySessionWorkspaceColumn(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	ctx := context.Background()
	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	exists, err := tableColumnExists(ctx, st.db, "sessions", "workspace")
	if err != nil {
		t.Fatalf("check sessions.workspace: %v", err)
	}
	if exists {
		t.Fatalf("fresh schema should not create sessions.workspace")
	}

	now := time.Now().UTC()
	if err := st.CreateSession(ctx, Session{
		ID:        "sess_no_workspace",
		UserID:    "lab",
		Status:    string(sessionstate.Created),
		Agent:     "claude_code",
		RestoreID: "unused-sess_no_workspace",
		CreatedAt: now,
		UpdatedAt: now,
	}); err != nil {
		t.Fatalf("create session without legacy workspace: %v", err)
	}
}

func TestMigrateDropsLegacySessionWorkspaceColumn(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	ctx := context.Background()
	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `ALTER TABLE sessions ADD COLUMN workspace TEXT`); err != nil {
		t.Fatalf("add legacy workspace column: %v", err)
	}
	if exists, err := tableColumnExists(ctx, st.db, "sessions", "workspace"); err != nil {
		t.Fatalf("check added workspace column: %v", err)
	} else if !exists {
		t.Fatalf("legacy workspace column was not added")
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	st, err = Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	exists, err := tableColumnExists(ctx, st.db, "sessions", "workspace")
	if err != nil {
		t.Fatalf("check migrated workspace column: %v", err)
	}
	if exists {
		t.Fatalf("migration should drop legacy sessions.workspace")
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
		Status:    string(sessionstate.Created),
		Agent:     "claude_code",
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
	if err := st.UpdateSessionStatusAndActivity(ctx, session.ID, string(sessionstate.RunningIdle), &restoreMS, activityTime); err != nil {
		t.Fatalf("update status and activity: %v", err)
	}

	// Verify update
	got, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if got.Status != string(sessionstate.RunningIdle) {
		t.Errorf("status: want %s, got %s", sessionstate.RunningIdle, got.Status)
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

func TestFailSessionStoresTypedFailure(t *testing.T) {
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
		ID:        "sess_fail",
		UserID:    "lab",
		Status:    string(sessionstate.Created),
		Agent:     "claude_code",
		RestoreID: "phase3-sess_fail",
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := st.CreateSession(ctx, session); err != nil {
		t.Fatalf("create session: %v", err)
	}

	failedAt := now.Add(10 * time.Second)
	if err := st.FailSession(ctx, FailSessionParams{
		SessionID:    session.ID,
		ErrorClass:   "probe_failed_pre_start",
		Reason:       "pre-start sandbox network probe failed",
		LastActivity: failedAt,
		Now:          failedAt,
	}); err != nil {
		t.Fatalf("fail session: %v", err)
	}

	got, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if got.Status != string(sessionstate.Failed) ||
		got.ErrorClass != "probe_failed_pre_start" ||
		got.FailureReason != "pre-start sandbox network probe failed" {
		t.Fatalf("unexpected failed session: %+v", got)
	}
	if got.EndedAt == nil {
		t.Fatalf("ended_at should be set")
	}
}

func TestEnqueueTurnMessageCreatesQueuedTurnMessageAndActivatesSession(t *testing.T) {
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
		ID:        "sess_enqueue",
		UserID:    "lab",
		Status:    string(sessionstate.Created),
		Agent:     "claude_code",
		RestoreID: "phase3-sess_enqueue",
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := st.CreateSession(ctx, session); err != nil {
		t.Fatalf("create session: %v", err)
	}

	result, err := st.EnqueueTurnMessage(ctx, EnqueueTurnMessageParams{
		SessionID: session.ID,
		Content:   "hello",
		Now:       now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("enqueue turn message: %v", err)
	}
	if result.TurnID == 0 || result.Message.ID == 0 {
		t.Fatalf("expected ids to be assigned: %+v", result)
	}
	if result.Message.Role != "user" || result.Message.Content != "hello" {
		t.Fatalf("unexpected message: %+v", result.Message)
	}

	var turnStatus, turnContent string
	var turnSequence int64
	if err := st.db.QueryRowContext(ctx, `
SELECT sequence, content, status
FROM turns
WHERE id = ?`, result.TurnID).Scan(&turnSequence, &turnContent, &turnStatus); err != nil {
		t.Fatalf("query turn: %v", err)
	}
	if turnSequence != 1 || turnContent != "hello" || turnStatus != "queued" {
		t.Fatalf("unexpected queued turn: sequence=%d content=%q status=%q", turnSequence, turnContent, turnStatus)
	}
	got, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if got.Status != string(sessionstate.RunningActive) || got.LastActivityAt == nil {
		t.Fatalf("session not marked active: %+v", got)
	}
}

func TestEnqueueTurnMessageRejectsBusySessionWithoutWrites(t *testing.T) {
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
		ID:        "sess_busy_enqueue",
		UserID:    "lab",
		Status:    string(sessionstate.RunningActive),
		Agent:     "claude_code",
		RestoreID: "phase3-sess_busy_enqueue",
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := st.CreateSession(ctx, session); err != nil {
		t.Fatalf("create session: %v", err)
	}

	_, err = st.EnqueueTurnMessage(ctx, EnqueueTurnMessageParams{
		SessionID: session.ID,
		Content:   "hello",
		Now:       now.Add(time.Second),
	})
	if err == nil || !strings.Contains(err.Error(), "session cannot accept input") {
		t.Fatalf("expected session cannot accept input, got %v", err)
	}

	var turns, messages int
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM turns WHERE session_id = ?`, session.ID).Scan(&turns); err != nil {
		t.Fatalf("count turns: %v", err)
	}
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages WHERE session_id = ?`, session.ID).Scan(&messages); err != nil {
		t.Fatalf("count messages: %v", err)
	}
	if turns != 0 || messages != 0 {
		t.Fatalf("busy enqueue should roll back writes: turns=%d messages=%d", turns, messages)
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
			Status:    string(sessionstate.RunningIdle),
			Agent:     "claude_code",
			RestoreID: "phase3-sess_1",
			CreatedAt: now,
			UpdatedAt: now,
		},
		{
			ID:        "sess_2",
			UserID:    "lab",
			Status:    string(sessionstate.RunningActive),
			Agent:     "claude_code",
			RestoreID: "phase3-sess_2",
			CreatedAt: now,
			UpdatedAt: now,
		},
		{
			ID:        "sess_3",
			UserID:    "lab",
			Status:    string(sessionstate.RunningIdle),
			Agent:     "claude_code",
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
	if err := st.UpdateSessionStatusAndActivity(ctx, "sess_1", string(sessionstate.RunningIdle), nil, activity1); err != nil {
		t.Fatalf("update sess_1: %v", err)
	}
	if err := st.UpdateSessionStatusAndActivity(ctx, "sess_3", string(sessionstate.RunningIdle), nil, activity3); err != nil {
		t.Fatalf("update sess_3: %v", err)
	}

	// List idle sessions
	idleSessions, err := st.ListSessionsByStatus(ctx, string(sessionstate.RunningIdle))
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

func TestRejectsLegacyStatuses(t *testing.T) {
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
		ID:        "sess_legacy",
		UserID:    "lab",
		Status:    "idle",
		Agent:     "claude_code",
		RestoreID: "phase3-sess_legacy",
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := st.CreateSession(ctx, session); err == nil {
		t.Fatalf("create session with legacy status should fail")
	}
	if err := st.UpdateSessionStatus(ctx, "sess_legacy", "completed", nil); err == nil {
		t.Fatalf("update to legacy status should fail")
	}
	if _, err := st.ListSessionsByStatus(ctx, "running"); err == nil {
		t.Fatalf("listing legacy status should fail")
	}
}

func TestCountActiveSessionsUsesCanonicalStatuses(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	ctx := context.Background()
	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	now := time.Now().UTC()
	statuses := []sessionstate.Status{
		sessionstate.Created,
		sessionstate.RunningActive,
		sessionstate.RunningIdle,
		sessionstate.Checkpointing,
		sessionstate.Checkpointed,
		sessionstate.Failed,
		sessionstate.Destroyed,
	}
	for i, status := range statuses {
		id := "sess_count_" + string(rune('a'+i))
		session := Session{
			ID:        id,
			UserID:    "lab",
			Status:    string(status),
			Agent:     "claude_code",
			RestoreID: "phase3-" + id,
			CreatedAt: now,
			UpdatedAt: now,
		}
		if err := st.CreateSession(ctx, session); err != nil {
			t.Fatalf("create session %s: %v", status, err)
		}
	}

	count, err := st.CountActiveSessions(ctx)
	if err != nil {
		t.Fatalf("count active sessions: %v", err)
	}
	if count != 5 {
		t.Fatalf("want 5 active sessions, got %d", count)
	}
}

func TestDeleteArtifactPathDeletesFileAndDescendants(t *testing.T) {
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
		ID:        "sess_artifacts",
		UserID:    "lab",
		Status:    string(sessionstate.Created),
		Agent:     "claude_code",
		RestoreID: "phase3-sess_artifacts",
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := st.CreateSession(ctx, session); err != nil {
		t.Fatalf("create session: %v", err)
	}

	for _, path := range []string{"keep.txt", "dir/a.txt", "dir/nested/b.txt", "dir2/a.txt"} {
		if err := st.UpsertArtifact(ctx, Artifact{
			SessionID: session.ID,
			Path:      path,
			Size:      int64(len(path)),
			ModTime:   now,
		}); err != nil {
			t.Fatalf("upsert %s: %v", path, err)
		}
	}

	if err := st.DeleteArtifactPath(ctx, session.ID, "dir"); err != nil {
		t.Fatalf("delete dir: %v", err)
	}

	got, err := st.ListArtifacts(ctx, session.ID)
	if err != nil {
		t.Fatalf("list artifacts: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 artifacts after prefix delete, got %+v", got)
	}
	if got[0].Path != "dir2/a.txt" || got[1].Path != "keep.txt" {
		t.Fatalf("unexpected artifacts after prefix delete: %+v", got)
	}

	if err := st.DeleteArtifactPath(ctx, session.ID, "keep.txt"); err != nil {
		t.Fatalf("delete file: %v", err)
	}
	got, err = st.ListArtifacts(ctx, session.ID)
	if err != nil {
		t.Fatalf("list artifacts after file delete: %v", err)
	}
	if len(got) != 1 || got[0].Path != "dir2/a.txt" {
		t.Fatalf("unexpected artifacts after file delete: %+v", got)
	}
}
