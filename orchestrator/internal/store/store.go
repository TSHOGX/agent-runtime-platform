package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/sessionstate"

	_ "modernc.org/sqlite"
)

type Session struct {
	ID                    string     `json:"id"`
	UserID                string     `json:"user_id"`
	Status                string     `json:"status"`
	Agent                 string     `json:"agent"`
	Mode                  string     `json:"mode"`
	Workspace             string     `json:"workspace"`
	ActiveGenerationID    string     `json:"active_generation_id,omitempty"`
	RestoreID             string     `json:"restore_id"`
	RestoreMS             *int64     `json:"restore_ms,omitempty"`
	CreatedAt             time.Time  `json:"created_at"`
	UpdatedAt             time.Time  `json:"updated_at"`
	ExpiresAt             *time.Time `json:"expires_at,omitempty"`
	EndedAt               *time.Time `json:"ended_at,omitempty"`
	LastActivityAt        *time.Time `json:"last_activity_at,omitempty"`
	CheckpointPath        string     `json:"checkpoint_path,omitempty"`
	AutoCheckpointEnabled bool       `json:"auto_checkpoint_enabled"`
	FailureReason         string     `json:"failure_reason,omitempty"`
	ErrorClass            string     `json:"error_class,omitempty"`
}

type DestroySessionResult struct {
	GenerationIDs []string
	EventID       int64
}

type Message struct {
	ID        int64     `json:"id"`
	SessionID string    `json:"session_id"`
	Role      string    `json:"role"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"created_at"`
}

type EnqueueTurnMessageParams struct {
	SessionID string
	Content   string
	Now       time.Time
}

type EnqueueTurnMessageResult struct {
	TurnID  int64
	Message Message
}

type Artifact struct {
	SessionID string    `json:"session_id"`
	Path      string    `json:"path"`
	Size      int64     `json:"size"`
	ModTime   time.Time `json:"mod_time"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Store struct {
	db *sql.DB
}

type SessionActiveGenerationCASParams struct {
	SessionID            string
	ExpectedGenerationID sql.NullString
	NextGenerationID     string
}

func Open(ctx context.Context, path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	store := &Store{db: db}
	if err := store.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) DBForTest() *sql.DB {
	return s.db
}

func (s *Store) migrate(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `
PRAGMA busy_timeout=5000;
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
`); err != nil {
		return err
	}
	return s.ensureSchema(ctx)
}

func (s *Store) EnsureUser(ctx context.Context, id, name string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx, `
INSERT INTO users (id, display_name, created_at)
VALUES (?, ?, ?)
ON CONFLICT(id) DO NOTHING`, id, name, now)
	return err
}

func (s *Store) CreateSession(ctx context.Context, session Session) error {
	if err := sessionstate.Validate(session.Status); err != nil {
		return err
	}
	if _, ok := agents.Lookup(session.Agent); !ok {
		return fmt.Errorf("unsupported driver %q", session.Agent)
	}
	session.Mode = strings.TrimSpace(session.Mode)
	if session.Mode == "" {
		session.Mode = ModeForDriver(session.Agent)
	}
	if session.Mode == "" {
		return fmt.Errorf("unsupported session mode for driver %q", session.Agent)
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO sessions (
  id, user_id, status, driver_id, mode, workspace, restore_id,
  created_at, updated_at, expires_at, auto_checkpoint_enabled
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		session.ID, session.UserID, session.Status, session.Agent, session.Mode, session.Workspace,
		session.RestoreID, formatTime(session.CreatedAt), formatTime(session.UpdatedAt), formatOptionalTime(session.ExpiresAt),
		boolInt(session.AutoCheckpointEnabled),
	)
	return err
}

func (s *Store) GetSession(ctx context.Context, id string) (Session, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT id, user_id, status, driver_id, mode, workspace, active_generation_id, restore_id, restore_ms, created_at, updated_at, expires_at, ended_at, last_activity_at, checkpoint_path, auto_checkpoint_enabled, failure_reason, error_class
FROM sessions WHERE id = ?`, id)
	return scanSession(row)
}

func (s *Store) ListSessions(ctx context.Context) ([]Session, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, user_id, status, driver_id, mode, workspace, active_generation_id, restore_id, restore_ms, created_at, updated_at, expires_at, ended_at, last_activity_at, checkpoint_path, auto_checkpoint_enabled, failure_reason, error_class
FROM sessions ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	sessions := []Session{}
	for rows.Next() {
		session, err := scanSession(rows)
		if err != nil {
			return nil, err
		}
		sessions = append(sessions, session)
	}
	return sessions, rows.Err()
}

func (s *Store) CountActiveSessions(ctx context.Context) (int, error) {
	var count int
	statuses := sessionstate.ActiveStatuses()
	placeholders := sqlPlaceholders(len(statuses))
	args := make([]any, len(statuses))
	for i, status := range statuses {
		args[i] = status
	}
	err := s.db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM sessions
WHERE status IN (`+placeholders+`)`, args...).Scan(&count)
	return count, err
}

func (s *Store) UpdateSessionStatus(ctx context.Context, id, status string, restoreMS *int64) error {
	if err := sessionstate.Validate(status); err != nil {
		return err
	}
	now := time.Now().UTC()
	var terminalAt any
	if sessionstate.IsTerminal(status) {
		terminalAt = formatTime(now)
	}
	query := `
UPDATE sessions
SET status = ?, restore_ms = COALESCE(?, restore_ms), updated_at = ?, ended_at = COALESCE(?, ended_at)
WHERE id = ?`
	args := []any{status, restoreMS, formatTime(now), terminalAt, id}
	if !sessionstate.IsTerminal(status) {
		query += ` AND status NOT IN ('failed', 'destroyed')`
	}
	_, err := s.db.ExecContext(ctx, query, args...)
	return err
}

func (s *Store) DestroySession(ctx context.Context, id string, now time.Time) (DestroySessionResult, error) {
	if strings.TrimSpace(id) == "" {
		return DestroySessionResult{}, fmt.Errorf("session id is required")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return DestroySessionResult{}, err
	}
	defer func() { _ = tx.Rollback() }()

	generationIDs, err := queryStringColumnTx(ctx, tx, `
SELECT generation_id
FROM runtime_generations
WHERE session_id = ?`, id)
	if err != nil {
		return DestroySessionResult{}, err
	}

	nowString := formatTime(now)
	if _, err := tx.ExecContext(ctx, `
UPDATE sessions
SET status = 'destroyed',
    checkpoint_path = NULL,
    restore_ms = NULL,
    error_class = 'session_destroyed',
    failure_reason = 'session_destroyed',
    updated_at = ?,
    ended_at = COALESCE(ended_at, ?)
WHERE id = ?`, nowString, nowString, id); err != nil {
		return DestroySessionResult{}, err
	}

	if _, err := tx.ExecContext(ctx, `
UPDATE turns
SET status = 'canceled',
    completed_at = ?,
    completed_by_generation = COALESCE(completed_by_generation, generation_id),
    error_class = 'session_destroyed',
    error = 'session_destroyed',
    lease_owner = NULL,
    lease_expires_at = NULL
WHERE session_id = ?
  AND status IN ('queued', 'leased', 'running')`, nowString, id); err != nil {
		return DestroySessionResult{}, err
	}

	if len(generationIDs) > 0 {
		args := []any{nowString}
		args = appendStringIDs(args, generationIDs)
		if _, err := tx.ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'failed',
    error_class = COALESCE(error_class, 'session_destroyed'),
    failure_reason = COALESCE(failure_reason, 'session_destroyed'),
    ended_at = COALESCE(ended_at, ?),
    lease_owner = NULL
WHERE generation_id IN (`+sqlPlaceholders(len(generationIDs))+`)
  AND status NOT IN ('failed', 'destroyed')`, args...); err != nil {
			return DestroySessionResult{}, err
		}
		if err := markAllocationsReclaimableTx(ctx, tx, generationIDs); err != nil {
			return DestroySessionResult{}, err
		}
		if err := deleteActiveContextsForGenerationsTx(ctx, tx, generationIDs); err != nil {
			return DestroySessionResult{}, err
		}
	}

	eventID, err := appendEventTx(ctx, tx, AppendEventParams{
		SessionID: id,
		DedupeKey: "session_destroyed:" + id,
		Type:      "session.destroyed",
		Payload: map[string]any{
			"terminal":           true,
			"status":             "destroyed",
			"session_status":     "destroyed",
			"session_updated_at": nowString,
			"updated_at":         nowString,
		},
		Now: now,
	})
	if err != nil {
		return DestroySessionResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return DestroySessionResult{}, err
	}
	return DestroySessionResult{GenerationIDs: generationIDs, EventID: eventID}, nil
}

func (s *Store) CancelTerminalSessionPendingTurns(ctx context.Context, now time.Time) (int64, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	res, err := s.db.ExecContext(ctx, `
UPDATE turns
SET status = 'canceled',
    completed_at = ?,
    completed_by_generation = COALESCE(completed_by_generation, generation_id),
    error_class = COALESCE(
      (SELECT NULLIF(sessions.error_class, '') FROM sessions WHERE sessions.id = turns.session_id),
      CASE (SELECT sessions.status FROM sessions WHERE sessions.id = turns.session_id)
        WHEN 'destroyed' THEN 'session_destroyed'
        ELSE 'session_failed'
      END
    ),
    error = COALESCE(
      (SELECT NULLIF(sessions.failure_reason, '') FROM sessions WHERE sessions.id = turns.session_id),
      CASE (SELECT sessions.status FROM sessions WHERE sessions.id = turns.session_id)
        WHEN 'destroyed' THEN 'session_destroyed'
        ELSE 'session_failed'
      END
    ),
    lease_owner = NULL,
    lease_expires_at = NULL
WHERE status IN ('queued', 'leased', 'running')
  AND EXISTS (
    SELECT 1
    FROM sessions
    WHERE sessions.id = turns.session_id
      AND sessions.status IN ('failed', 'destroyed')
  )`, formatTime(now))
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *Store) UpdateSessionActiveGeneration(ctx context.Context, p SessionActiveGenerationCASParams) error {
	if strings.TrimSpace(p.SessionID) == "" {
		return fmt.Errorf("session id is required")
	}
	if strings.TrimSpace(p.NextGenerationID) == "" {
		return fmt.Errorf("next generation id is required")
	}

	args := []any{p.NextGenerationID, p.SessionID}
	where := "active_generation_id IS NULL"
	if p.ExpectedGenerationID.Valid {
		where = "active_generation_id = ?"
		args = append(args, p.ExpectedGenerationID.String)
	}
	res, err := s.db.ExecContext(ctx, `
UPDATE sessions
SET active_generation_id = ?
WHERE id = ?
  AND `+where, args...)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return fmt.Errorf("session active generation CAS failed")
	}
	return nil
}

func (s *Store) AddMessage(ctx context.Context, sessionID, role, content string) (Message, error) {
	now := time.Now().UTC()
	res, err := s.db.ExecContext(ctx, `
INSERT INTO messages (session_id, role, content, created_at)
VALUES (?, ?, ?, ?)`, sessionID, role, content, formatTime(now))
	if err != nil {
		return Message{}, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return Message{}, err
	}
	return Message{ID: id, SessionID: sessionID, Role: role, Content: content, CreatedAt: now}, nil
}

func (s *Store) EnqueueTurnMessage(ctx context.Context, p EnqueueTurnMessageParams) (EnqueueTurnMessageResult, error) {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return EnqueueTurnMessageResult{}, err
	}
	defer func() { _ = tx.Rollback() }()
	next, err := nextTurnSequence(ctx, tx, p.SessionID)
	if err != nil {
		return EnqueueTurnMessageResult{}, err
	}
	turnRes, err := tx.ExecContext(ctx, `
INSERT INTO turns (session_id, sequence, role, content, status, attempt, created_at)
VALUES (?, ?, 'user', ?, 'queued', 0, ?)`, p.SessionID, next, p.Content, formatTime(p.Now))
	if err != nil {
		return EnqueueTurnMessageResult{}, err
	}
	turnID, err := turnRes.LastInsertId()
	if err != nil {
		return EnqueueTurnMessageResult{}, err
	}
	msgRes, err := tx.ExecContext(ctx, `
INSERT INTO messages (session_id, role, content, created_at)
VALUES (?, 'user', ?, ?)`, p.SessionID, p.Content, formatTime(p.Now))
	if err != nil {
		return EnqueueTurnMessageResult{}, err
	}
	messageID, err := msgRes.LastInsertId()
	if err != nil {
		return EnqueueTurnMessageResult{}, err
	}
	res, err := tx.ExecContext(ctx, `
UPDATE sessions
SET status = ?,
    updated_at = ?,
    last_activity_at = ?
WHERE id = ?
  AND status IN (?, ?, ?)`,
		string(sessionstate.RunningActive), formatTime(p.Now), formatTime(p.Now), p.SessionID,
		string(sessionstate.Created), string(sessionstate.RunningIdle), string(sessionstate.Checkpointed))
	if err != nil {
		return EnqueueTurnMessageResult{}, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return EnqueueTurnMessageResult{}, err
	}
	if affected != 1 {
		return EnqueueTurnMessageResult{}, fmt.Errorf("session cannot accept input")
	}
	if err := tx.Commit(); err != nil {
		return EnqueueTurnMessageResult{}, err
	}
	return EnqueueTurnMessageResult{
		TurnID: turnID,
		Message: Message{
			ID:        messageID,
			SessionID: p.SessionID,
			Role:      "user",
			Content:   p.Content,
			CreatedAt: p.Now,
		},
	}, nil
}

func (s *Store) ListMessages(ctx context.Context, sessionID string) ([]Message, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, session_id, role, content, created_at
FROM messages WHERE session_id = ? ORDER BY id ASC`, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	messages := []Message{}
	for rows.Next() {
		var m Message
		var createdAt string
		if err := rows.Scan(&m.ID, &m.SessionID, &m.Role, &m.Content, &createdAt); err != nil {
			return nil, err
		}
		m.CreatedAt = parseTime(createdAt)
		messages = append(messages, m)
	}
	return messages, rows.Err()
}

func (s *Store) UpsertArtifact(ctx context.Context, artifact Artifact) error {
	now := time.Now().UTC()
	if artifact.CreatedAt.IsZero() {
		artifact.CreatedAt = now
	}
	if artifact.UpdatedAt.IsZero() {
		artifact.UpdatedAt = now
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO artifacts (session_id, path, size, mod_time, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(session_id, path) DO UPDATE SET
  size = excluded.size,
  mod_time = excluded.mod_time,
  updated_at = excluded.updated_at`,
		artifact.SessionID, artifact.Path, artifact.Size, formatTime(artifact.ModTime),
		formatTime(artifact.CreatedAt), formatTime(artifact.UpdatedAt),
	)
	return err
}

func (s *Store) ListArtifacts(ctx context.Context, sessionID string) ([]Artifact, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT session_id, path, size, mod_time, created_at, updated_at
FROM artifacts WHERE session_id = ? ORDER BY path`, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	artifacts := []Artifact{}
	for rows.Next() {
		var artifact Artifact
		var modTime, createdAt, updatedAt string
		if err := rows.Scan(&artifact.SessionID, &artifact.Path, &artifact.Size, &modTime, &createdAt, &updatedAt); err != nil {
			return nil, err
		}
		artifact.ModTime = parseTime(modTime)
		artifact.CreatedAt = parseTime(createdAt)
		artifact.UpdatedAt = parseTime(updatedAt)
		artifacts = append(artifacts, artifact)
	}
	return artifacts, rows.Err()
}

func (s *Store) DeleteArtifactPath(ctx context.Context, sessionID, artifactPath string) error {
	_, err := s.db.ExecContext(ctx, `
DELETE FROM artifacts
WHERE session_id = ?
  AND (path = ? OR path LIKE ? ESCAPE '\')`,
		sessionID, artifactPath, escapeLike(artifactPath)+"/%",
	)
	return err
}

type scanner interface {
	Scan(dest ...any) error
}

func scanSession(row scanner) (Session, error) {
	var session Session
	var restoreMS sql.NullInt64
	var activeGenerationID sql.NullString
	var createdAt, updatedAt string
	var expiresAt, endedAt, lastActivityAt sql.NullString
	var checkpointPath, failureReason, errorClass sql.NullString
	var autoCheckpointEnabled int
	err := row.Scan(
		&session.ID, &session.UserID, &session.Status, &session.Agent, &session.Mode, &session.Workspace,
		&activeGenerationID, &session.RestoreID, &restoreMS, &createdAt, &updatedAt,
		&expiresAt, &endedAt, &lastActivityAt, &checkpointPath, &autoCheckpointEnabled, &failureReason, &errorClass,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return Session{}, err
	}
	if err != nil {
		return Session{}, err
	}
	if restoreMS.Valid {
		session.RestoreMS = &restoreMS.Int64
	}
	if activeGenerationID.Valid {
		session.ActiveGenerationID = activeGenerationID.String
	}
	session.CreatedAt = parseTime(createdAt)
	session.UpdatedAt = parseTime(updatedAt)
	if expiresAt.Valid {
		t := parseTime(expiresAt.String)
		session.ExpiresAt = &t
	}
	if endedAt.Valid {
		t := parseTime(endedAt.String)
		session.EndedAt = &t
	}
	if lastActivityAt.Valid {
		t := parseTime(lastActivityAt.String)
		session.LastActivityAt = &t
	}
	if checkpointPath.Valid {
		session.CheckpointPath = checkpointPath.String
	}
	session.AutoCheckpointEnabled = autoCheckpointEnabled != 0
	if failureReason.Valid {
		session.FailureReason = failureReason.String
	}
	if errorClass.Valid {
		session.ErrorClass = errorClass.String
	}
	if strings.TrimSpace(session.Mode) == "" {
		session.Mode = ModeForDriver(session.Agent)
	}
	return session, nil
}

func ModeForDriver(driverID string) string {
	spec, ok := agents.DriverSpecFor(driverID)
	if !ok {
		return ""
	}
	switch spec.Kind {
	case agents.DriverKindAgent, agents.DriverKindShell:
		return string(spec.Kind)
	default:
		return ""
	}
}

func nullableString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func formatOptionalTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return formatTime(*t)
}

func formatTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

func formatEventTime(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000000000Z07:00")
}

func parseTime(value string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, value)
	return t
}

func (s *Store) UpdateSessionStatusAndActivity(ctx context.Context, id, status string, restoreMS *int64, lastActivity time.Time) error {
	if err := sessionstate.Validate(status); err != nil {
		return err
	}
	now := time.Now().UTC()
	var terminalAt any
	if sessionstate.IsTerminal(status) {
		terminalAt = formatTime(now)
	}
	query := `
UPDATE sessions
SET status = ?, restore_ms = COALESCE(?, restore_ms), updated_at = ?, ended_at = COALESCE(?, ended_at), last_activity_at = ?
WHERE id = ?`
	args := []any{status, restoreMS, formatTime(now), terminalAt, formatTime(lastActivity), id}
	if !sessionstate.IsTerminal(status) {
		query += ` AND status NOT IN ('failed', 'destroyed')`
	}
	_, err := s.db.ExecContext(ctx, query, args...)
	return err
}

type FailSessionParams struct {
	SessionID    string
	ErrorClass   string
	Reason       string
	LastActivity time.Time
	Now          time.Time
}

func (s *Store) FailSession(ctx context.Context, p FailSessionParams) error {
	if strings.TrimSpace(p.SessionID) == "" {
		return fmt.Errorf("session id is required")
	}
	now := p.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	lastActivity := p.LastActivity
	if lastActivity.IsZero() {
		lastActivity = now
	}
	nowString := formatTime(now)
	_, err := s.db.ExecContext(ctx, `
UPDATE sessions
SET status = 'failed',
    updated_at = ?,
    ended_at = COALESCE(ended_at, ?),
    last_activity_at = ?,
    error_class = ?,
    failure_reason = ?
WHERE id = ?
  AND status != 'destroyed'`,
		nowString,
		nowString,
		formatTime(lastActivity),
		nullableString(p.ErrorClass),
		nullableString(p.Reason),
		p.SessionID,
	)
	return err
}

func (s *Store) ListSessionsByStatus(ctx context.Context, status string) ([]Session, error) {
	if err := sessionstate.Validate(status); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id, user_id, status, driver_id, mode, workspace, active_generation_id, restore_id, restore_ms, created_at, updated_at, expires_at, ended_at, last_activity_at, checkpoint_path, auto_checkpoint_enabled, failure_reason, error_class
FROM sessions WHERE status = ? ORDER BY last_activity_at ASC`, status)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	sessions := []Session{}
	for rows.Next() {
		session, err := scanSession(rows)
		if err != nil {
			return nil, err
		}
		sessions = append(sessions, session)
	}
	return sessions, rows.Err()
}

func sqlPlaceholders(count int) string {
	return strings.TrimSuffix(strings.Repeat("?,", count), ",")
}

func sqlStringList(values []string) string {
	quoted := make([]string, len(values))
	for i, value := range values {
		quoted[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(value, "'", "''"))
	}
	return strings.Join(quoted, ",")
}

func escapeLike(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `%`, `\%`)
	value = strings.ReplaceAll(value, `_`, `\_`)
	return value
}
