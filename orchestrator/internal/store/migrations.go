package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"harness-platform/orchestrator/internal/sessionstate"
)

type migration struct {
	version int
	name    string
	fn      func(context.Context, dbRunner) error
}

type dbRunner interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *Store) runMigrations(ctx context.Context, migrations []migration) error {
	for _, m := range migrations {
		if err := s.runMigration(ctx, m); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) runMigration(ctx context.Context, m migration) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return fmt.Errorf("migration %d %s: begin immediate: %w", m.version, m.name, err)
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()

	if _, err := conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
  version INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  applied_at TEXT NOT NULL
)`); err != nil {
		return fmt.Errorf("migration %d %s: %w", m.version, m.name, err)
	}

	applied, err := migrationApplied(ctx, conn, m.version)
	if err != nil {
		return fmt.Errorf("migration %d %s: %w", m.version, m.name, err)
	}
	if applied {
		if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
			return fmt.Errorf("migration %d %s: commit: %w", m.version, m.name, err)
		}
		committed = true
		return nil
	}

	if err := m.fn(ctx, conn); err != nil {
		return fmt.Errorf("migration %d %s: %w", m.version, m.name, err)
	}
	if _, err := conn.ExecContext(ctx, `
INSERT INTO schema_migrations (version, name, applied_at)
VALUES (?, ?, ?)`, m.version, m.name, formatTime(time.Now().UTC())); err != nil {
		return fmt.Errorf("migration %d %s: %w", m.version, m.name, err)
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return fmt.Errorf("migration %d %s: commit: %w", m.version, m.name, err)
	}
	committed = true
	return nil
}

func migrationApplied(ctx context.Context, tx dbRunner, version int) (bool, error) {
	var existing int
	err := tx.QueryRowContext(ctx, `SELECT version FROM schema_migrations WHERE version = ?`, version).Scan(&existing)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func defaultMigrations(options Options) []migration {
	return []migration{
		{version: 1, name: "baseline_schema", fn: migrateV1BaselineSchema},
		{version: 2, name: "phase7_baseline_tables", fn: migrateV2Phase7BaselineTables},
		{version: 3, name: "phase7_turn_event_and_proxy_context", fn: migrateV3Phase7TurnEventAndProxyContext},
		{version: 4, name: "phase7_session_columns", fn: func(ctx context.Context, tx dbRunner) error {
			return migrateV4Phase7SessionColumns(ctx, tx, options.AgentHomesRoot)
		}},
		{version: 5, name: "phase7_indexes", fn: migrateV5Phase7Indexes},
		{version: 6, name: "phase7_legacy_session_backfill", fn: migrateV6Phase7LegacySessionBackfill},
		{version: 7, name: "phase7_proxy_event_uniqueness", fn: migrateV7Phase7ProxyEventUniqueness},
		{version: 8, name: "phase7_event_retention_index", fn: migrateV8Phase7EventRetentionIndex},
	}
}

func migrateV1BaselineSchema(ctx context.Context, tx dbRunner) error {
	statusCheck := sqlStringList(sessionstate.AllStatuses())
	_, err := tx.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS users (
  id TEXT PRIMARY KEY,
  display_name TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
  id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN (`+statusCheck+`)),
  agent TEXT NOT NULL,
  workspace TEXT NOT NULL,
  restore_id TEXT NOT NULL,
  restore_ms INTEGER,
  claude_session_uuid TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  expires_at TEXT,
  ended_at TEXT,
  last_activity_at TEXT,
  checkpoint_path TEXT
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

func migrateV2Phase7BaselineTables(ctx context.Context, tx dbRunner) error {
	_, err := tx.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS orchestrator_owner (
  singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
  uuid TEXT NOT NULL,
  boot_id TEXT NOT NULL,
  host_run_dir TEXT NOT NULL,
  acquired_at TEXT NOT NULL,
  heartbeat_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS runtime_generations (
  generation_id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('allocating','starting','probing','active','idle','checkpointing','checkpointed','restoring','failed','destroyed')),
  checkpoint_created_at TEXT,
  checkpoint_network_profile_id TEXT,
  checkpoint_agent_runtime_profile_id TEXT,
  checkpoint_runsc_version TEXT,
  checkpoint_runsc_platform TEXT,
  checkpoint_bundle_digest TEXT,
  checkpoint_runtime_config_digest TEXT,
  checkpoint_control_manifest_digest TEXT,
  network_profile_id TEXT,
  agent_runtime_profile_id TEXT,
  runsc_platform TEXT DEFAULT 'systrap',
  runsc_version TEXT,
  lease_owner TEXT,
  lease_expires_at TEXT,
  started_at TEXT,
  last_seen_at TEXT,
  ended_at TEXT,
  failure_reason TEXT,
  error_class TEXT,
  FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE,
  FOREIGN KEY(checkpoint_network_profile_id) REFERENCES network_profiles(network_profile_id) DEFERRABLE INITIALLY DEFERRED,
  FOREIGN KEY(checkpoint_agent_runtime_profile_id) REFERENCES agent_runtime_profiles(agent_runtime_profile_id) DEFERRABLE INITIALLY DEFERRED,
  FOREIGN KEY(network_profile_id) REFERENCES network_profiles(network_profile_id) DEFERRABLE INITIALLY DEFERRED,
  FOREIGN KEY(agent_runtime_profile_id) REFERENCES agent_runtime_profiles(agent_runtime_profile_id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE IF NOT EXISTS egress_policies (
  egress_policy_id TEXT PRIMARY KEY,
  policy_digest TEXT NOT NULL,
  allowed_egress_rules TEXT NOT NULL,
  doris_fe_hosts TEXT NOT NULL,
  doris_be_hosts TEXT NOT NULL,
  doris_ports TEXT NOT NULL,
  dns_policy TEXT NOT NULL CHECK(dns_policy IN ('off','hostnames_only','always')),
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS network_profiles (
  network_profile_id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  generation_id TEXT NOT NULL,
  runsc_network TEXT NOT NULL DEFAULT 'sandbox',
  runsc_overlay2 TEXT NOT NULL DEFAULT 'none',
  host_proxy_bind_url TEXT NOT NULL,
  proxy_port INTEGER NOT NULL,
  host_gateway_ip TEXT NOT NULL,
  sandbox_base_url TEXT NOT NULL,
  probe_url TEXT NOT NULL,
  netns_name TEXT NOT NULL,
  netns_path TEXT NOT NULL,
  host_veth TEXT NOT NULL,
  sandbox_veth TEXT NOT NULL,
  sandbox_ip_cidr TEXT NOT NULL,
  egress_policy_id TEXT NOT NULL,
  allowed_egress_rules TEXT NOT NULL,
  doris_fe_hosts TEXT NOT NULL,
  doris_be_hosts TEXT NOT NULL,
  doris_ports TEXT NOT NULL,
  dns_policy TEXT NOT NULL CHECK(dns_policy IN ('off','hostnames_only','always')),
  host_side_cidr TEXT NOT NULL,
  allocation_state TEXT NOT NULL CHECK(allocation_state IN ('allocating','ready','live','reserved_checkpointed','recreating','reclaimable','destroyed')),
  created_at TEXT NOT NULL,
  destroyed_at TEXT,
  FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE,
  FOREIGN KEY(generation_id) REFERENCES runtime_generations(generation_id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
  FOREIGN KEY(egress_policy_id) REFERENCES egress_policies(egress_policy_id)
);

CREATE TABLE IF NOT EXISTS agent_runtime_profiles (
  agent_runtime_profile_id TEXT PRIMARY KEY,
  agent TEXT NOT NULL CHECK(agent IN ('claude','sh')),
  model TEXT,
  output_format TEXT NOT NULL,
  disable_nonessential_traffic INTEGER NOT NULL CHECK(disable_nonessential_traffic IN (0,1)),
  requires_secret_drop INTEGER NOT NULL CHECK(requires_secret_drop IN (0,1)),
  manifest_anthropic_base_url TEXT,
  anthropic_api_key_secret_id TEXT,
  anthropic_auth_token_secret_id TEXT,
  secret_version TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS runtime_generation_resources (
  generation_id TEXT PRIMARY KEY,
  network_profile_id TEXT NOT NULL,
  agent_runtime_profile_id TEXT NOT NULL,
  control_dir_path TEXT NOT NULL,
  control_manifest_path TEXT NOT NULL,
  control_manifest_digest TEXT,
  bundle_dir_path TEXT NOT NULL,
  spec_path TEXT NOT NULL,
  checkpoint_path TEXT,
  secrets_dir_path TEXT,
  bridge_dir_path TEXT NOT NULL,
  log_dir_path TEXT NOT NULL,
  runsc_pid INTEGER,
  runsc_version TEXT,
  resource_state TEXT NOT NULL CHECK(resource_state IN ('allocating','ready','live','reserved_checkpointed','recreating','reclaimable','destroyed')),
  created_at TEXT NOT NULL,
  destroyed_at TEXT,
  FOREIGN KEY(generation_id) REFERENCES runtime_generations(generation_id) ON DELETE CASCADE,
  FOREIGN KEY(network_profile_id) REFERENCES network_profiles(network_profile_id) DEFERRABLE INITIALLY DEFERRED,
  FOREIGN KEY(agent_runtime_profile_id) REFERENCES agent_runtime_profiles(agent_runtime_profile_id) DEFERRABLE INITIALLY DEFERRED
);
`)
	return err
}

func migrateV3Phase7TurnEventAndProxyContext(ctx context.Context, tx dbRunner) error {
	_, err := tx.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS turns (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT NOT NULL,
  sequence INTEGER NOT NULL,
  role TEXT NOT NULL CHECK(role = 'user'),
  content TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('queued','leased','running','completed','failed','canceled')),
  generation_id TEXT,
  lease_owner TEXT,
  lease_expires_at TEXT,
  claim_request_id TEXT,
  claim_granted_at TEXT,
  attempt INTEGER NOT NULL DEFAULT 0,
  ack_started_at TEXT,
  completed_by_generation TEXT,
  retry_policy TEXT,
  created_at TEXT NOT NULL,
  started_at TEXT,
  completed_at TEXT,
  error_class TEXT,
  error TEXT,
  FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE,
  FOREIGN KEY(generation_id) REFERENCES runtime_generations(generation_id),
  FOREIGN KEY(completed_by_generation) REFERENCES runtime_generations(generation_id)
);

CREATE TABLE IF NOT EXISTS events (
  event_id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT,
  turn_id INTEGER,
  generation_id TEXT,
  output_sequence INTEGER,
  dedupe_key TEXT,
  proxy_request_id TEXT,
  stream TEXT,
  severity TEXT,
  type TEXT NOT NULL,
  payload TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE,
  FOREIGN KEY(turn_id) REFERENCES turns(id),
  FOREIGN KEY(generation_id) REFERENCES runtime_generations(generation_id)
);

CREATE TABLE IF NOT EXISTS active_model_request_contexts (
  sandbox_source_ip TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  generation_id TEXT NOT NULL,
  turn_id INTEGER NOT NULL,
  lease_owner TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  next_request_sequence INTEGER NOT NULL,
  registered_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE,
  FOREIGN KEY(generation_id) REFERENCES runtime_generations(generation_id),
  FOREIGN KEY(turn_id) REFERENCES turns(id)
);
`)
	return err
}

func migrateV4Phase7SessionColumns(ctx context.Context, tx dbRunner, agentHomesRoot string) error {
	columns := []struct {
		name string
		ddl  string
	}{
		{name: "active_generation_id", ddl: "ALTER TABLE sessions ADD COLUMN active_generation_id TEXT REFERENCES runtime_generations(generation_id)"},
		{name: "agent_home_path", ddl: "ALTER TABLE sessions ADD COLUMN agent_home_path TEXT"},
		{name: "failure_reason", ddl: "ALTER TABLE sessions ADD COLUMN failure_reason TEXT"},
		{name: "error_class", ddl: "ALTER TABLE sessions ADD COLUMN error_class TEXT"},
	}
	for _, column := range columns {
		exists, err := columnExists(ctx, tx, "sessions", column.name)
		if err != nil {
			return err
		}
		if exists {
			continue
		}
		if _, err := tx.ExecContext(ctx, column.ddl); err != nil {
			return err
		}
	}
	rows, err := tx.QueryContext(ctx, `SELECT id FROM sessions WHERE agent_home_path IS NULL OR agent_home_path = ''`)
	if err != nil {
		return err
	}
	defer rows.Close()
	type update struct {
		id   string
		path string
	}
	var updates []update
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return err
		}
		updates = append(updates, update{id: id, path: filepath.Join(agentHomesRoot, id)})
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, update := range updates {
		if _, err := tx.ExecContext(ctx, `UPDATE sessions SET agent_home_path = ? WHERE id = ?`, update.path, update.id); err != nil {
			return err
		}
	}
	return nil
}

func migrateV5Phase7Indexes(ctx context.Context, tx dbRunner) error {
	_, err := tx.ExecContext(ctx, `
CREATE UNIQUE INDEX IF NOT EXISTS runtime_generations_one_nonterminal_per_session
  ON runtime_generations (session_id)
  WHERE status NOT IN ('failed', 'destroyed');

CREATE INDEX IF NOT EXISTS runtime_generations_session_status_idx
  ON runtime_generations (session_id, status);

CREATE UNIQUE INDEX IF NOT EXISTS turns_claim_request_id_uq
  ON turns (session_id, claim_request_id)
  WHERE claim_request_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS turns_session_sequence_uq
  ON turns (session_id, sequence);

CREATE INDEX IF NOT EXISTS turns_session_status_sequence_idx
  ON turns (session_id, status, sequence);

CREATE UNIQUE INDEX IF NOT EXISTS events_output_dedupe_uq
  ON events (turn_id, generation_id, output_sequence)
  WHERE output_sequence IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS events_dedupe_key_uq
  ON events (session_id, dedupe_key)
  WHERE dedupe_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS events_session_event_id_idx
  ON events (session_id, event_id);

CREATE INDEX IF NOT EXISTS events_proxy_request_id_idx
  ON events (proxy_request_id);

CREATE INDEX IF NOT EXISTS active_model_request_contexts_session_generation_idx
  ON active_model_request_contexts (session_id, generation_id);

CREATE UNIQUE INDEX IF NOT EXISTS network_profiles_netns_name_non_destroyed_uq
  ON network_profiles (netns_name)
  WHERE allocation_state != 'destroyed';

CREATE UNIQUE INDEX IF NOT EXISTS network_profiles_netns_path_non_destroyed_uq
  ON network_profiles (netns_path)
  WHERE allocation_state != 'destroyed';

CREATE UNIQUE INDEX IF NOT EXISTS network_profiles_host_veth_non_destroyed_uq
  ON network_profiles (host_veth)
  WHERE allocation_state != 'destroyed';

CREATE UNIQUE INDEX IF NOT EXISTS network_profiles_sandbox_veth_non_destroyed_uq
  ON network_profiles (sandbox_veth)
  WHERE allocation_state != 'destroyed';

CREATE UNIQUE INDEX IF NOT EXISTS network_profiles_host_gateway_ip_non_destroyed_uq
  ON network_profiles (host_gateway_ip)
  WHERE allocation_state != 'destroyed';

CREATE UNIQUE INDEX IF NOT EXISTS network_profiles_host_side_cidr_non_destroyed_uq
  ON network_profiles (host_side_cidr)
  WHERE allocation_state != 'destroyed';

CREATE UNIQUE INDEX IF NOT EXISTS network_profiles_sandbox_ip_cidr_non_destroyed_uq
  ON network_profiles (sandbox_ip_cidr)
  WHERE allocation_state != 'destroyed';

CREATE UNIQUE INDEX IF NOT EXISTS runtime_generation_resources_control_dir_path_non_destroyed_uq
  ON runtime_generation_resources (control_dir_path)
  WHERE resource_state != 'destroyed';

CREATE UNIQUE INDEX IF NOT EXISTS runtime_generation_resources_control_manifest_path_non_destroyed_uq
  ON runtime_generation_resources (control_manifest_path)
  WHERE resource_state != 'destroyed';

CREATE UNIQUE INDEX IF NOT EXISTS runtime_generation_resources_bundle_dir_path_non_destroyed_uq
  ON runtime_generation_resources (bundle_dir_path)
  WHERE resource_state != 'destroyed';

CREATE UNIQUE INDEX IF NOT EXISTS runtime_generation_resources_spec_path_non_destroyed_uq
  ON runtime_generation_resources (spec_path)
  WHERE resource_state != 'destroyed';

CREATE UNIQUE INDEX IF NOT EXISTS runtime_generation_resources_checkpoint_path_non_destroyed_uq
  ON runtime_generation_resources (checkpoint_path)
  WHERE checkpoint_path IS NOT NULL AND resource_state != 'destroyed';

CREATE UNIQUE INDEX IF NOT EXISTS runtime_generation_resources_bridge_dir_path_non_destroyed_uq
  ON runtime_generation_resources (bridge_dir_path)
  WHERE resource_state != 'destroyed';

CREATE UNIQUE INDEX IF NOT EXISTS runtime_generation_resources_log_dir_path_non_destroyed_uq
  ON runtime_generation_resources (log_dir_path)
  WHERE resource_state != 'destroyed';

CREATE UNIQUE INDEX IF NOT EXISTS runtime_generation_resources_secrets_dir_path_non_destroyed_uq
  ON runtime_generation_resources (secrets_dir_path)
  WHERE secrets_dir_path IS NOT NULL AND resource_state != 'destroyed';

CREATE UNIQUE INDEX IF NOT EXISTS agent_runtime_profiles_tuple_uq
  ON agent_runtime_profiles (
    agent, model, output_format, disable_nonessential_traffic,
    requires_secret_drop, manifest_anthropic_base_url,
    anthropic_api_key_secret_id, anthropic_auth_token_secret_id, secret_version
  );
`)
	return err
}

func migrateV6Phase7LegacySessionBackfill(ctx context.Context, tx dbRunner) error {
	now := formatTime(time.Now().UTC())
	if _, err := tx.ExecContext(ctx, `
UPDATE sessions
SET status = ?,
    error_class = 'legacy_checkpoint_unrestorable',
    failure_reason = 'legacy_checkpoint_unrestorable',
    ended_at = COALESCE(ended_at, ?),
    updated_at = ?
WHERE status IN (?, ?)
  AND ended_at IS NULL`,
		string(sessionstate.Failed), now, now, string(sessionstate.Checkpointing), string(sessionstate.Checkpointed)); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `
UPDATE sessions
SET status = ?,
    error_class = 'legacy_pre_phase7_no_generation',
    failure_reason = 'legacy_pre_phase7_no_generation',
    ended_at = COALESCE(ended_at, ?),
    updated_at = ?
WHERE status IN (?, ?)
  AND ended_at IS NULL`,
		string(sessionstate.Failed), now, now, string(sessionstate.RunningActive), string(sessionstate.RunningIdle))
	return err
}

func migrateV7Phase7ProxyEventUniqueness(ctx context.Context, tx dbRunner) error {
	_, err := tx.ExecContext(ctx, `
CREATE UNIQUE INDEX IF NOT EXISTS events_proxy_started_request_uq
  ON events (proxy_request_id)
  WHERE proxy_request_id IS NOT NULL
    AND type = 'proxy.request.started';

CREATE UNIQUE INDEX IF NOT EXISTS events_proxy_finished_request_uq
  ON events (proxy_request_id)
  WHERE proxy_request_id IS NOT NULL
    AND type IN ('proxy.request.completed', 'proxy.request.failed');
`)
	return err
}

func migrateV8Phase7EventRetentionIndex(ctx context.Context, tx dbRunner) error {
	rows, err := tx.QueryContext(ctx, `SELECT event_id, created_at FROM events`)
	if err != nil {
		return err
	}
	type eventTimeUpdate struct {
		eventID   int64
		createdAt string
	}
	var updates []eventTimeUpdate
	for rows.Next() {
		var eventID int64
		var createdAt string
		if err := rows.Scan(&eventID, &createdAt); err != nil {
			_ = rows.Close()
			return err
		}
		parsed, err := time.Parse(time.RFC3339Nano, createdAt)
		if err != nil {
			_ = rows.Close()
			return fmt.Errorf("parse event %d created_at: %w", eventID, err)
		}
		formatted := formatEventTime(parsed)
		if formatted != createdAt {
			updates = append(updates, eventTimeUpdate{eventID: eventID, createdAt: formatted})
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, update := range updates {
		if _, err := tx.ExecContext(ctx, `
UPDATE events
SET created_at = ?
WHERE event_id = ?`, update.createdAt, update.eventID); err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(ctx, `
CREATE INDEX IF NOT EXISTS events_created_at_idx
  ON events (created_at);
`)
	return err
}

func columnExists(ctx context.Context, tx dbRunner, table, column string) (bool, error) {
	rows, err := tx.QueryContext(ctx, `PRAGMA table_info(`+quoteSQLiteIdent(table)+`)`)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var defaultValue any
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	return false, rows.Err()
}

func quoteSQLiteIdent(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}
