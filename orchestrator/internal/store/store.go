package store

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

type Session struct {
	ID                string     `json:"id"`
	UserID            string     `json:"user_id"`
	Status            string     `json:"status"`
	Agent             string     `json:"agent"`
	Workspace         string     `json:"workspace"`
	RestoreID         string     `json:"restore_id"`
	RestoreMS         *int64     `json:"restore_ms,omitempty"`
	ClaudeSessionUUID string     `json:"claude_session_uuid,omitempty"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
	ExpiresAt         *time.Time `json:"expires_at,omitempty"`
	CompletedAt       *time.Time `json:"completed_at,omitempty"`
	LastActivityAt    *time.Time `json:"last_activity_at,omitempty"`
	CheckpointPath    string     `json:"checkpoint_path,omitempty"`
}

type Message struct {
	ID        int64     `json:"id"`
	SessionID string    `json:"session_id"`
	Role      string    `json:"role"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"created_at"`
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

func (s *Store) migrate(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `
PRAGMA busy_timeout=5000;
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS users (
  id TEXT PRIMARY KEY,
  display_name TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
  id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL,
  status TEXT NOT NULL,
  agent TEXT NOT NULL,
  workspace TEXT NOT NULL,
  restore_id TEXT NOT NULL,
  restore_ms INTEGER,
  claude_session_uuid TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  expires_at TEXT,
  completed_at TEXT
);

CREATE TABLE IF NOT EXISTS messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT NOT NULL,
  role TEXT NOT NULL,
  content TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS artifacts (
  session_id TEXT NOT NULL,
  path TEXT NOT NULL,
  size INTEGER NOT NULL,
  mod_time TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY(session_id, path),
  FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
);
`); err != nil {
		return err
	}
	if err := s.ensureColumn(ctx, "sessions", "claude_session_uuid", "TEXT"); err != nil {
		return err
	}
	if err := s.ensureColumn(ctx, "sessions", "last_activity_at", "TEXT"); err != nil {
		return err
	}
	if err := s.ensureColumn(ctx, "sessions", "checkpoint_path", "TEXT"); err != nil {
		return err
	}
	return nil
}

func (s *Store) ensureColumn(ctx context.Context, table, column, decl string) error {
	rows, err := s.db.QueryContext(ctx, `SELECT name FROM pragma_table_info(?)`, table)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		if name == column {
			return rows.Err()
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, "ALTER TABLE "+table+" ADD COLUMN "+column+" "+decl)
	return err
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
	_, err := s.db.ExecContext(ctx, `
INSERT INTO sessions (
  id, user_id, status, agent, workspace, restore_id, claude_session_uuid, created_at, updated_at, expires_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		session.ID, session.UserID, session.Status, session.Agent, session.Workspace, session.RestoreID,
		nullableString(session.ClaudeSessionUUID),
		formatTime(session.CreatedAt), formatTime(session.UpdatedAt), formatOptionalTime(session.ExpiresAt),
	)
	return err
}

func (s *Store) GetSession(ctx context.Context, id string) (Session, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT id, user_id, status, agent, workspace, restore_id, restore_ms, claude_session_uuid, created_at, updated_at, expires_at, completed_at, last_activity_at, checkpoint_path
FROM sessions WHERE id = ?`, id)
	return scanSession(row)
}

func (s *Store) ListSessions(ctx context.Context) ([]Session, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, user_id, status, agent, workspace, restore_id, restore_ms, claude_session_uuid, created_at, updated_at, expires_at, completed_at, last_activity_at, checkpoint_path
FROM sessions ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sessions []Session
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
	err := s.db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM sessions WHERE status IN ('created', 'running', 'idle')`).Scan(&count)
	return count, err
}

func (s *Store) UpdateSessionStatus(ctx context.Context, id, status string, restoreMS *int64) error {
	now := time.Now().UTC()
	var completed any
	if status == "completed" || status == "failed" || status == "destroyed" {
		completed = formatTime(now)
	}
	_, err := s.db.ExecContext(ctx, `
UPDATE sessions
SET status = ?, restore_ms = COALESCE(?, restore_ms), updated_at = ?, completed_at = COALESCE(?, completed_at)
WHERE id = ?`, status, restoreMS, formatTime(now), completed, id)
	return err
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

func (s *Store) ListMessages(ctx context.Context, sessionID string) ([]Message, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, session_id, role, content, created_at
FROM messages WHERE session_id = ? ORDER BY id ASC`, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []Message
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

	var artifacts []Artifact
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

type scanner interface {
	Scan(dest ...any) error
}

func scanSession(row scanner) (Session, error) {
	var session Session
	var restoreMS sql.NullInt64
	var claudeUUID sql.NullString
	var createdAt, updatedAt string
	var expiresAt, completedAt, lastActivityAt sql.NullString
	var checkpointPath sql.NullString
	err := row.Scan(
		&session.ID, &session.UserID, &session.Status, &session.Agent, &session.Workspace, &session.RestoreID,
		&restoreMS, &claudeUUID, &createdAt, &updatedAt, &expiresAt, &completedAt, &lastActivityAt, &checkpointPath,
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
	if claudeUUID.Valid {
		session.ClaudeSessionUUID = claudeUUID.String
	}
	session.CreatedAt = parseTime(createdAt)
	session.UpdatedAt = parseTime(updatedAt)
	if expiresAt.Valid {
		t := parseTime(expiresAt.String)
		session.ExpiresAt = &t
	}
	if completedAt.Valid {
		t := parseTime(completedAt.String)
		session.CompletedAt = &t
	}
	if lastActivityAt.Valid {
		t := parseTime(lastActivityAt.String)
		session.LastActivityAt = &t
	}
	if checkpointPath.Valid {
		session.CheckpointPath = checkpointPath.String
	}
	return session, nil
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

func parseTime(value string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, value)
	return t
}

func (s *Store) UpdateSessionStatusAndActivity(ctx context.Context, id, status string, restoreMS *int64, lastActivity time.Time) error {
	now := time.Now().UTC()
	var completed any
	if status == "completed" || status == "failed" || status == "destroyed" {
		completed = formatTime(now)
	}
	_, err := s.db.ExecContext(ctx, `
UPDATE sessions
SET status = ?, restore_ms = COALESCE(?, restore_ms), updated_at = ?, completed_at = COALESCE(?, completed_at), last_activity_at = ?
WHERE id = ?`, status, restoreMS, formatTime(now), completed, formatTime(lastActivity), id)
	return err
}

func (s *Store) ListSessionsByStatus(ctx context.Context, status string) ([]Session, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, user_id, status, agent, workspace, restore_id, restore_ms, claude_session_uuid, created_at, updated_at, expires_at, completed_at, last_activity_at, checkpoint_path
FROM sessions WHERE status = ? ORDER BY last_activity_at ASC`, status)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sessions []Session
	for rows.Next() {
		session, err := scanSession(rows)
		if err != nil {
			return nil, err
		}
		sessions = append(sessions, session)
	}
	return sessions, rows.Err()
}
