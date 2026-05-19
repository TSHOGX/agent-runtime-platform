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
	ID          string     `json:"id"`
	UserID      string     `json:"user_id"`
	Status      string     `json:"status"`
	Agent       string     `json:"agent"`
	Workspace   string     `json:"workspace"`
	RestoreID   string     `json:"restore_id"`
	RestoreMS   *int64     `json:"restore_ms,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	ExpiresAt   *time.Time `json:"expires_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
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
	_, err := s.db.ExecContext(ctx, `
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
`)
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
  id, user_id, status, agent, workspace, restore_id, created_at, updated_at, expires_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		session.ID, session.UserID, session.Status, session.Agent, session.Workspace, session.RestoreID,
		formatTime(session.CreatedAt), formatTime(session.UpdatedAt), formatOptionalTime(session.ExpiresAt),
	)
	return err
}

func (s *Store) GetSession(ctx context.Context, id string) (Session, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT id, user_id, status, agent, workspace, restore_id, restore_ms, created_at, updated_at, expires_at, completed_at
FROM sessions WHERE id = ?`, id)
	return scanSession(row)
}

func (s *Store) ListSessions(ctx context.Context) ([]Session, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, user_id, status, agent, workspace, restore_id, restore_ms, created_at, updated_at, expires_at, completed_at
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
SELECT COUNT(*) FROM sessions WHERE status IN ('created', 'running')`).Scan(&count)
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

func (s *Store) AddMessage(ctx context.Context, sessionID, role, content string) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO messages (session_id, role, content, created_at)
VALUES (?, ?, ?, ?)`, sessionID, role, content, formatTime(time.Now().UTC()))
	return err
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
	var createdAt, updatedAt string
	var expiresAt, completedAt sql.NullString
	err := row.Scan(
		&session.ID, &session.UserID, &session.Status, &session.Agent, &session.Workspace, &session.RestoreID,
		&restoreMS, &createdAt, &updatedAt, &expiresAt, &completedAt,
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
	return session, nil
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
