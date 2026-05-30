package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/sessionstate"
)

const phase9CutoverMarker = "phase9a_clean_schema"
const phase9CutoverInProgressMarker = "phase9_cutover_in_progress"

func (s *Store) runPhase9Cutover(ctx context.Context) error {
	if err := s.ensurePhase9CutoverState(ctx, s.db); err != nil {
		return err
	}
	done, err := s.phase9CutoverDone(ctx, s.db)
	if err != nil {
		return err
	}
	if done {
		return nil
	}

	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, `PRAGMA foreign_keys=OFF`); err != nil {
		return fmt.Errorf("phase9 cutover disable foreign keys: %w", err)
	}
	defer func() { _, _ = conn.ExecContext(context.Background(), `PRAGMA foreign_keys=ON`) }()

	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return fmt.Errorf("phase9 cutover begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()

	if err := s.ensurePhase9CutoverState(ctx, conn); err != nil {
		return err
	}
	done, err = s.phase9CutoverDone(ctx, conn)
	if err != nil {
		return err
	}
	if done {
		if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
			return fmt.Errorf("phase9 cutover commit: %w", err)
		}
		committed = true
		return nil
	}

	now := formatTime(time.Now().UTC())
	if _, err := conn.ExecContext(ctx, `
INSERT INTO phase9_cutover_state (key, payload, updated_at)
VALUES (?, ?, ?)
ON CONFLICT(key) DO UPDATE SET
  payload = excluded.payload,
  updated_at = excluded.updated_at`,
		phase9CutoverInProgressMarker,
		`{"cutover":"phase9a","cleanup":"destructive_schema_rebuild"}`,
		now,
	); err != nil {
		return fmt.Errorf("phase9 cutover marker: %w", err)
	}

	if err := rebuildPhase9Schema(ctx, conn); err != nil {
		return err
	}

	if _, err := conn.ExecContext(ctx, `
INSERT INTO phase9_cutover_state (key, payload, updated_at)
VALUES (?, ?, ?)
ON CONFLICT(key) DO UPDATE SET
  payload = excluded.payload,
  updated_at = excluded.updated_at`,
		phase9CutoverMarker,
		`{"contract_schema_version":2,"contract_gate_version":"phase9a"}`,
		now,
	); err != nil {
		return fmt.Errorf("phase9 cutover final marker: %w", err)
	}
	if _, err := conn.ExecContext(ctx, `DELETE FROM phase9_cutover_state WHERE key = ?`, phase9CutoverInProgressMarker); err != nil {
		return fmt.Errorf("phase9 cutover clear in-progress marker: %w", err)
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return fmt.Errorf("phase9 cutover commit: %w", err)
	}
	committed = true
	if _, err := conn.ExecContext(ctx, `PRAGMA foreign_keys=ON`); err != nil {
		return fmt.Errorf("phase9 cutover enable foreign keys: %w", err)
	}
	if rows, err := conn.QueryContext(ctx, `PRAGMA foreign_key_check`); err != nil {
		return fmt.Errorf("phase9 cutover foreign key check: %w", err)
	} else {
		defer rows.Close()
		if rows.Next() {
			return fmt.Errorf("phase9 cutover foreign key check failed")
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("phase9 cutover foreign key check: %w", err)
		}
	}
	return nil
}

func (s *Store) ensurePhase9CutoverState(ctx context.Context, db dbRunner) error {
	_, err := db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS phase9_cutover_state (
  key TEXT PRIMARY KEY,
  payload TEXT NOT NULL,
  updated_at TEXT NOT NULL
)`)
	return err
}

func (s *Store) phase9CutoverDone(ctx context.Context, db dbRunner) (bool, error) {
	return phase9StateMarkerDone(ctx, db, phase9CutoverMarker)
}

func phase9StateMarkerDone(ctx context.Context, db dbRunner, markerKey string) (bool, error) {
	var key string
	err := db.QueryRowContext(ctx, `SELECT key FROM phase9_cutover_state WHERE key = ?`, markerKey).Scan(&key)
	if err == nil {
		return true, nil
	}
	if err == sql.ErrNoRows {
		return false, nil
	}
	return false, err
}

func rebuildPhase9Schema(ctx context.Context, db dbRunner) error {
	statusCheck := sqlStringList(sessionstate.AllStatuses())
	driverCheck := sqlStringList([]string{string(agents.ClaudeCode), string(agents.Pi), string(agents.Shell)})
	statements := []string{
		`DELETE FROM active_model_request_contexts`,
		`DELETE FROM events`,
		`DELETE FROM turns`,
		`DELETE FROM messages`,
		`DELETE FROM artifacts`,
		`DELETE FROM sandbox_contract_artifacts`,
		`DELETE FROM runtime_resource_instances`,
		`DELETE FROM runtime_generation_resources`,
		`DELETE FROM sandbox_contracts`,
		`DELETE FROM network_profiles`,
		`DELETE FROM runtime_generations`,
		`DELETE FROM session_driver_homes`,
		`DELETE FROM session_workspaces`,
		`DELETE FROM sessions`,
		`DELETE FROM agent_runtime_profiles`,
		`DROP TABLE IF EXISTS sessions_phase9`,
		`CREATE TABLE sessions_phase9 (
  id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN (` + statusCheck + `)),
  driver_id TEXT NOT NULL CHECK(driver_id IN (` + driverCheck + `)),
  mode TEXT NOT NULL CHECK(mode IN ('agent','shell')),
  workspace TEXT NOT NULL,
  restore_id TEXT NOT NULL,
  restore_ms INTEGER,
  claude_session_uuid TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  expires_at TEXT,
  ended_at TEXT,
  last_activity_at TEXT,
  checkpoint_path TEXT,
  auto_checkpoint_enabled INTEGER NOT NULL DEFAULT 0 CHECK(auto_checkpoint_enabled IN (0,1)),
  active_generation_id TEXT REFERENCES runtime_generations(generation_id),
  agent_home_path TEXT,
  failure_reason TEXT,
  error_class TEXT
)`,
		`DROP TABLE sessions`,
		`ALTER TABLE sessions_phase9 RENAME TO sessions`,
		`DROP INDEX IF EXISTS agent_runtime_profiles_tuple_uq`,
		`DROP TABLE IF EXISTS agent_runtime_profiles_phase9`,
		`CREATE TABLE agent_runtime_profiles_phase9 (
  agent_runtime_profile_id TEXT PRIMARY KEY,
  driver_id TEXT NOT NULL CHECK(driver_id IN (` + driverCheck + `)),
  model TEXT,
  output_format TEXT NOT NULL,
  disable_nonessential_traffic INTEGER NOT NULL CHECK(disable_nonessential_traffic IN (0,1)),
  sandbox_uid INTEGER NOT NULL CHECK(sandbox_uid > 0),
  sandbox_gid INTEGER NOT NULL CHECK(sandbox_gid > 0),
  sandbox_supplemental_gids TEXT NOT NULL,
  requires_secret_drop INTEGER NOT NULL CHECK(requires_secret_drop IN (0,1)),
  model_access_allowed INTEGER NOT NULL CHECK(model_access_allowed IN (0,1)),
  manifest_model_proxy_base_url TEXT,
  model_proxy_api_key_secret_id TEXT,
  model_proxy_auth_token_secret_id TEXT,
  secret_version TEXT,
  created_at TEXT NOT NULL
)`,
		`DROP TABLE agent_runtime_profiles`,
		`ALTER TABLE agent_runtime_profiles_phase9 RENAME TO agent_runtime_profiles`,
		`CREATE UNIQUE INDEX agent_runtime_profiles_tuple_uq
  ON agent_runtime_profiles (
    driver_id,
    COALESCE(model, ''),
    output_format,
    disable_nonessential_traffic,
    sandbox_uid,
    sandbox_gid,
    sandbox_supplemental_gids,
    requires_secret_drop,
    model_access_allowed,
    COALESCE(manifest_model_proxy_base_url, ''),
    COALESCE(model_proxy_api_key_secret_id, ''),
    COALESCE(model_proxy_auth_token_secret_id, ''),
    COALESCE(secret_version, '')
  )`,
		`DROP TABLE IF EXISTS sandbox_contracts_phase9`,
		`CREATE TABLE sandbox_contracts_phase9 (
  contract_id TEXT PRIMARY KEY,
  generation_id TEXT NOT NULL UNIQUE,
  session_id TEXT NOT NULL,
  sandbox_contract_version TEXT NOT NULL CHECK(sandbox_contract_version = 'sandbox-isolation-v1'),
  contract_schema_version INTEGER NOT NULL CHECK(contract_schema_version = 2),
  contract_gate_version TEXT NOT NULL CHECK(contract_gate_version IN ('phase9a','phase9c')),
  canonical_payload TEXT NOT NULL,
  sandbox_contract_digest TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE,
  FOREIGN KEY(generation_id) REFERENCES runtime_generations(generation_id) ON DELETE CASCADE
)`,
		`DROP TABLE sandbox_contracts`,
		`ALTER TABLE sandbox_contracts_phase9 RENAME TO sandbox_contracts`,
		`CREATE TABLE IF NOT EXISTS session_driver_states (
  session_id TEXT NOT NULL,
  driver_id TEXT NOT NULL CHECK(driver_id <> '' AND driver_id IN (` + driverCheck + `)),
  state_payload TEXT NOT NULL CHECK(state_payload <> ''),
  state_digest TEXT NOT NULL CHECK(state_digest LIKE 'sha256:%'),
  state_version INTEGER NOT NULL CHECK(state_version > 0),
  updated_generation_id TEXT NOT NULL,
  updated_turn_id INTEGER,
  updated_at TEXT NOT NULL,
  PRIMARY KEY(session_id, driver_id),
  FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE,
  FOREIGN KEY(updated_generation_id) REFERENCES runtime_generations(generation_id) ON DELETE RESTRICT,
  FOREIGN KEY(updated_turn_id) REFERENCES turns(id) ON DELETE SET NULL
)`,
		`CREATE TABLE IF NOT EXISTS runtime_resource_quarantine_tombstones (
  tombstone_id TEXT PRIMARY KEY,
  provider_id TEXT NOT NULL,
  host_id TEXT NOT NULL,
  resource_kind TEXT NOT NULL,
  identity_key TEXT NOT NULL,
  identity_digest TEXT NOT NULL,
  source_table TEXT NOT NULL,
  source_session_id TEXT,
  source_generation_id TEXT,
  source_contract_id TEXT,
  source_identity_digest TEXT,
  source_identity_payload TEXT,
  cleanup_attempt_id TEXT NOT NULL,
  quarantine_reason TEXT NOT NULL,
  quarantine_evidence_payload TEXT NOT NULL,
  quarantined_at TEXT NOT NULL,
  last_checked_at TEXT,
  released_at TEXT,
  release_evidence_payload TEXT,
  expires_at TEXT
)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS runtime_resource_quarantine_active_uq
  ON runtime_resource_quarantine_tombstones (
    provider_id,
    host_id,
    resource_kind,
    identity_digest
  )
  WHERE released_at IS NULL`,
	}
	for _, stmt := range statements {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("phase9 schema rebuild statement %q: %w", stmt, err)
		}
	}
	if exists, err := columnExists(ctx, db, "runtime_generations", "checkpoint_driver_states_digest"); err != nil {
		return err
	} else if !exists {
		if _, err := db.ExecContext(ctx, `ALTER TABLE runtime_generations ADD COLUMN checkpoint_driver_states_digest TEXT`); err != nil {
			return fmt.Errorf("phase9 runtime generation checkpoint fence column: %w", err)
		}
	}
	return nil
}

func (s *Store) ensurePhase9ModeSchema(ctx context.Context) error {
	exists, err := columnExists(ctx, s.db, "sessions", "mode")
	if err != nil {
		return err
	}
	if !exists {
		if _, err := s.db.ExecContext(ctx, `ALTER TABLE sessions ADD COLUMN mode TEXT NOT NULL DEFAULT 'agent' CHECK(mode IN ('agent','shell'))`); err != nil {
			return fmt.Errorf("add sessions mode column: %w", err)
		}
	}
	if _, err := s.db.ExecContext(ctx, `
UPDATE sessions
SET mode = CASE driver_id
  WHEN 'sh' THEN 'shell'
  ELSE 'agent'
END
WHERE mode <> CASE driver_id
  WHEN 'sh' THEN 'shell'
  ELSE 'agent'
END`); err != nil {
		return fmt.Errorf("backfill sessions mode: %w", err)
	}
	return nil
}
