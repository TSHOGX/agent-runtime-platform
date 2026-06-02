package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/sessionstate"
)

var removedSessionColumns = []string{
	"workspace",
	"restore_id",
	"claude_session_uuid",
	"agent_home_path",
}

type dbRunner interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *Store) ensureSchema(ctx context.Context) error {
	statusCheck := sqlStringList(sessionstate.AllStatuses())
	driverCheck := sqlStringList([]string{string(agents.ClaudeCode), string(agents.Pi), string(agents.Shell)})
	_, err := s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS users (
  id TEXT PRIMARY KEY,
  display_name TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
  id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN (`+statusCheck+`)),
  driver_id TEXT NOT NULL CHECK(driver_id IN (`+driverCheck+`)),
  mode TEXT NOT NULL CHECK(mode IN ('agent','shell')),
  restore_ms INTEGER,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  expires_at TEXT,
  ended_at TEXT,
  last_activity_at TEXT,
  checkpoint_path TEXT,
  auto_checkpoint_enabled INTEGER NOT NULL DEFAULT 0 CHECK(auto_checkpoint_enabled IN (0,1)),
  active_generation_id TEXT REFERENCES runtime_generations(generation_id),
  failure_reason TEXT,
  error_class TEXT
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

CREATE TABLE IF NOT EXISTS orchestrator_owner (
  singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
  uuid TEXT NOT NULL,
  boot_id TEXT NOT NULL,
  host_run_dir TEXT NOT NULL,
  acquired_at TEXT NOT NULL,
  heartbeat_at TEXT NOT NULL
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

CREATE TABLE IF NOT EXISTS agent_runtime_profiles (
  agent_runtime_profile_id TEXT PRIMARY KEY,
  driver_id TEXT NOT NULL CHECK(driver_id IN (`+driverCheck+`)),
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
);

CREATE UNIQUE INDEX IF NOT EXISTS agent_runtime_profiles_tuple_uq
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
  checkpoint_runsc_binary_path TEXT,
  checkpoint_runsc_binary_digest TEXT,
  checkpoint_bundle_digest TEXT,
  checkpoint_runtime_config_digest TEXT,
  checkpoint_control_manifest_digest TEXT,
  checkpoint_driver_states_digest TEXT,
  checkpoint_plan_digest TEXT,
  checkpoint_image_manifest_digest TEXT,
  network_profile_id TEXT,
  agent_runtime_profile_id TEXT,
  runsc_platform TEXT DEFAULT 'systrap',
  runsc_version TEXT,
  sandbox_contract_id TEXT REFERENCES sandbox_contracts(contract_id),
  sandbox_contract_version TEXT,
  auto_checkpoint_enabled INTEGER NOT NULL DEFAULT 0 CHECK(auto_checkpoint_enabled IN (0,1)),
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

CREATE UNIQUE INDEX IF NOT EXISTS runtime_generations_one_nonterminal_per_session
  ON runtime_generations (session_id)
  WHERE status NOT IN ('failed', 'destroyed');

CREATE INDEX IF NOT EXISTS runtime_generations_session_status_idx
  ON runtime_generations (session_id, status);

CREATE UNIQUE INDEX IF NOT EXISTS runtime_generations_sandbox_contract_id_uq
  ON runtime_generations (sandbox_contract_id)
  WHERE sandbox_contract_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS generation_plans (
  generation_id TEXT PRIMARY KEY
    REFERENCES runtime_generations(generation_id) ON DELETE CASCADE,
  plan_version INTEGER NOT NULL CHECK(plan_version = 1),
  canonical_payload TEXT NOT NULL,
  plan_digest TEXT NOT NULL CHECK(plan_digest LIKE 'sha256:%'),
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS generation_plan_projections (
  generation_id TEXT NOT NULL
    REFERENCES generation_plans(generation_id) ON DELETE CASCADE,
  plan_digest TEXT NOT NULL CHECK(plan_digest LIKE 'sha256:%'),
  projection_kind TEXT NOT NULL CHECK(projection_kind <> ''),
  projection_version INTEGER NOT NULL CHECK(projection_version > 0),
  payload_digest TEXT NOT NULL CHECK(payload_digest LIKE 'sha256:%'),
  materialized_path TEXT,
  created_at TEXT NOT NULL,
  PRIMARY KEY (generation_id, projection_kind)
);

CREATE TABLE IF NOT EXISTS content_snapshots (
  snapshot_kind TEXT NOT NULL CHECK(snapshot_kind IN ('skills','managed_settings')),
  snapshot_digest TEXT NOT NULL CHECK(snapshot_digest LIKE 'sha256:%'),
  immutable_host_path TEXT NOT NULL CHECK(immutable_host_path <> ''),
  mount_destination TEXT NOT NULL CHECK(mount_destination <> ''),
  source_evidence_digest TEXT NOT NULL CHECK(source_evidence_digest LIKE 'sha256:%'),
  retention_class TEXT NOT NULL CHECK(retention_class <> ''),
  created_at TEXT NOT NULL,
  PRIMARY KEY (snapshot_kind, snapshot_digest)
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

CREATE TABLE IF NOT EXISTS runtime_generation_resources (
  generation_id TEXT PRIMARY KEY,
  network_profile_id TEXT NOT NULL,
  agent_runtime_profile_id TEXT NOT NULL,
  control_dir_path TEXT NOT NULL,
  control_manifest_path TEXT NOT NULL,
  control_manifest_digest TEXT,
  projected_control_manifest_digest TEXT,
  bundle_digest TEXT,
  runtime_config_digest TEXT,
  spec_digest TEXT,
  bundle_dir_path TEXT NOT NULL,
  spec_path TEXT NOT NULL,
  checkpoint_path TEXT,
  secrets_dir_path TEXT,
  bridge_dir_path TEXT NOT NULL,
  log_dir_path TEXT NOT NULL,
  contract_id TEXT REFERENCES sandbox_contracts(contract_id),
  sandbox_contract_version TEXT,
  runsc_container_id TEXT,
  runsc_pid INTEGER,
  runsc_platform TEXT,
  runsc_version TEXT,
  runsc_binary_path TEXT,
  runsc_binary_digest TEXT,
  sandbox_ip TEXT,
  network_hosts_path TEXT,
  resource_identity_payload TEXT,
  resource_identity_digest TEXT,
  resource_state TEXT NOT NULL CHECK(resource_state IN ('allocating','ready','live','reserved_checkpointed','recreating','reclaimable','destroyed')),
  created_at TEXT NOT NULL,
  destroyed_at TEXT,
  FOREIGN KEY(generation_id) REFERENCES runtime_generations(generation_id) ON DELETE CASCADE,
  FOREIGN KEY(network_profile_id) REFERENCES network_profiles(network_profile_id) DEFERRABLE INITIALLY DEFERRED,
  FOREIGN KEY(agent_runtime_profile_id) REFERENCES agent_runtime_profiles(agent_runtime_profile_id) DEFERRABLE INITIALLY DEFERRED
);

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

CREATE UNIQUE INDEX IF NOT EXISTS runtime_generation_resources_contract_id_uq
  ON runtime_generation_resources (contract_id)
  WHERE contract_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS runtime_generation_resources_runsc_container_id_non_destroyed_uq
  ON runtime_generation_resources (runsc_container_id)
  WHERE runsc_container_id IS NOT NULL
    AND resource_state != 'destroyed';

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

CREATE UNIQUE INDEX IF NOT EXISTS turns_claim_request_id_uq
  ON turns (session_id, claim_request_id)
  WHERE claim_request_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS turns_session_sequence_uq
  ON turns (session_id, sequence);

CREATE INDEX IF NOT EXISTS turns_session_status_sequence_idx
  ON turns (session_id, status, sequence);

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

CREATE UNIQUE INDEX IF NOT EXISTS events_proxy_started_request_uq
  ON events (proxy_request_id)
  WHERE proxy_request_id IS NOT NULL
    AND type = 'proxy.request.started';

CREATE UNIQUE INDEX IF NOT EXISTS events_proxy_finished_request_uq
  ON events (proxy_request_id)
  WHERE proxy_request_id IS NOT NULL
    AND type IN ('proxy.request.completed', 'proxy.request.failed');

CREATE INDEX IF NOT EXISTS events_created_at_idx
  ON events (created_at);

CREATE TABLE IF NOT EXISTS active_model_request_contexts (
  sandbox_source_ip TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  generation_id TEXT NOT NULL,
  turn_id INTEGER NOT NULL,
  lease_owner TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  next_request_sequence INTEGER NOT NULL,
  model_access_allowed INTEGER NOT NULL DEFAULT 0 CHECK(model_access_allowed IN (0,1)),
  registered_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE,
  FOREIGN KEY(generation_id) REFERENCES runtime_generations(generation_id),
  FOREIGN KEY(turn_id) REFERENCES turns(id)
);

CREATE INDEX IF NOT EXISTS active_model_request_contexts_session_generation_idx
  ON active_model_request_contexts (session_id, generation_id);

CREATE TABLE IF NOT EXISTS sandbox_contracts (
  contract_id TEXT PRIMARY KEY,
  generation_id TEXT NOT NULL UNIQUE,
  session_id TEXT NOT NULL,
  sandbox_contract_version TEXT NOT NULL CHECK(sandbox_contract_version = 'sandbox-isolation-v1'),
  contract_schema_version INTEGER NOT NULL CHECK(contract_schema_version = 2),
  contract_gate_version TEXT NOT NULL CHECK(contract_gate_version = 'driver_manifest_v1'),
  canonical_payload TEXT NOT NULL,
  sandbox_contract_digest TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE,
  FOREIGN KEY(generation_id) REFERENCES runtime_generations(generation_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS sandbox_contract_artifacts (
  contract_id TEXT PRIMARY KEY,
  sandbox_contract_digest TEXT NOT NULL,
  network_hosts_digest TEXT,
  control_manifest_digest TEXT NOT NULL,
  oci_spec_digest TEXT NOT NULL,
  bundle_digest TEXT NOT NULL,
  checkpoint_metadata_digest TEXT,
  created_at TEXT NOT NULL,
  FOREIGN KEY(contract_id) REFERENCES sandbox_contracts(contract_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS sandbox_contract_input_evidence (
  contract_id TEXT PRIMARY KEY,
  runtime_config_digest TEXT NOT NULL,
  runtime_config_preimage TEXT NOT NULL,
  agent_manifest_digest TEXT NOT NULL,
  agent_manifest_payload TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY(contract_id) REFERENCES sandbox_contracts(contract_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS session_workspaces (
  session_id TEXT PRIMARY KEY,
  host_path TEXT NOT NULL UNIQUE,
  layout_version INTEGER NOT NULL,
  sandbox_uid INTEGER NOT NULL,
  sandbox_gid INTEGER NOT NULL,
  sandbox_supplemental_gids TEXT NOT NULL,
  runtime_identity_digest TEXT NOT NULL,
  provisioned_at TEXT NOT NULL,
  provisioning_marker_path TEXT NOT NULL UNIQUE,
  provisioning_marker_digest TEXT NOT NULL,
  FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS session_driver_homes (
  session_id TEXT NOT NULL,
  driver TEXT NOT NULL,
  host_path TEXT NOT NULL UNIQUE,
  layout_version INTEGER NOT NULL,
  sandbox_uid INTEGER NOT NULL,
  sandbox_gid INTEGER NOT NULL,
  sandbox_supplemental_gids TEXT NOT NULL,
  runtime_identity_digest TEXT NOT NULL,
  provisioned_at TEXT NOT NULL,
  provisioning_marker_path TEXT NOT NULL UNIQUE,
  provisioning_marker_digest TEXT NOT NULL,
  PRIMARY KEY(session_id, driver),
  FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS session_driver_homes_session_idx
  ON session_driver_homes (session_id);

CREATE TABLE IF NOT EXISTS session_driver_states (
  session_id TEXT NOT NULL,
  driver_id TEXT NOT NULL CHECK(driver_id <> '' AND driver_id IN (`+driverCheck+`)),
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
);

CREATE TABLE IF NOT EXISTS runtime_resource_instances (
  generation_id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  contract_id TEXT NOT NULL,
  sandbox_contract_version TEXT NOT NULL CHECK(sandbox_contract_version = 'sandbox-isolation-v1'),
  worker_id TEXT,
  host_id TEXT NOT NULL,
  state TEXT NOT NULL CHECK(state IN ('allocated','materializing','ready','live','checkpoint_reserved','retiring','reconciling','absent_verified','destroyed')),
  lease_expires_at TEXT,
  idempotency_token TEXT,
  runsc_container_id TEXT NOT NULL,
  runsc_platform TEXT NOT NULL,
  runsc_version TEXT NOT NULL,
  runsc_binary_path TEXT NOT NULL,
  runsc_binary_digest TEXT NOT NULL,
  network_profile_id TEXT NOT NULL,
  netns_name TEXT NOT NULL,
  netns_path TEXT NOT NULL,
  host_veth TEXT NOT NULL,
  sandbox_veth TEXT NOT NULL,
  host_gateway_ip TEXT NOT NULL,
  sandbox_ip TEXT NOT NULL,
  sandbox_ip_cidr TEXT NOT NULL,
  host_side_cidr TEXT NOT NULL,
  nft_table_name TEXT NOT NULL,
  control_dir_path TEXT NOT NULL,
  control_manifest_path TEXT NOT NULL,
  bundle_dir_path TEXT NOT NULL,
  spec_path TEXT NOT NULL,
  checkpoint_path TEXT,
  bridge_dir_path TEXT NOT NULL,
  network_hosts_path TEXT,
  log_dir_path TEXT NOT NULL,
  resource_identity_payload TEXT NOT NULL,
  resource_identity_digest TEXT NOT NULL,
  evidence_json TEXT,
  evidence_digest TEXT,
  verified_at TEXT,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE,
  FOREIGN KEY(generation_id) REFERENCES runtime_generations(generation_id) ON DELETE CASCADE,
  FOREIGN KEY(contract_id) REFERENCES sandbox_contracts(contract_id),
  FOREIGN KEY(network_profile_id) REFERENCES network_profiles(network_profile_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS runtime_resource_instances_runsc_container_id_active_uq
  ON runtime_resource_instances (runsc_container_id)
  WHERE runsc_container_id IS NOT NULL
    AND state NOT IN ('absent_verified','destroyed');

CREATE UNIQUE INDEX IF NOT EXISTS runtime_resource_instances_netns_name_active_uq
  ON runtime_resource_instances (netns_name)
  WHERE netns_name IS NOT NULL
    AND state NOT IN ('absent_verified','destroyed');

CREATE UNIQUE INDEX IF NOT EXISTS runtime_resource_instances_netns_path_active_uq
  ON runtime_resource_instances (netns_path)
  WHERE netns_path IS NOT NULL
    AND state NOT IN ('absent_verified','destroyed');

CREATE UNIQUE INDEX IF NOT EXISTS runtime_resource_instances_host_veth_active_uq
  ON runtime_resource_instances (host_veth)
  WHERE host_veth IS NOT NULL
    AND state NOT IN ('absent_verified','destroyed');

CREATE UNIQUE INDEX IF NOT EXISTS runtime_resource_instances_sandbox_veth_active_uq
  ON runtime_resource_instances (sandbox_veth)
  WHERE sandbox_veth IS NOT NULL
    AND state NOT IN ('absent_verified','destroyed');

CREATE UNIQUE INDEX IF NOT EXISTS runtime_resource_instances_host_gateway_ip_active_uq
  ON runtime_resource_instances (host_gateway_ip)
  WHERE host_gateway_ip IS NOT NULL
    AND state NOT IN ('absent_verified','destroyed');

CREATE UNIQUE INDEX IF NOT EXISTS runtime_resource_instances_sandbox_ip_active_uq
  ON runtime_resource_instances (sandbox_ip)
  WHERE sandbox_ip IS NOT NULL
    AND state NOT IN ('absent_verified','destroyed');

CREATE UNIQUE INDEX IF NOT EXISTS runtime_resource_instances_sandbox_ip_cidr_active_uq
  ON runtime_resource_instances (sandbox_ip_cidr)
  WHERE sandbox_ip_cidr IS NOT NULL
    AND state NOT IN ('absent_verified','destroyed');

CREATE UNIQUE INDEX IF NOT EXISTS runtime_resource_instances_host_side_cidr_active_uq
  ON runtime_resource_instances (host_side_cidr)
  WHERE host_side_cidr IS NOT NULL
    AND state NOT IN ('absent_verified','destroyed');

CREATE UNIQUE INDEX IF NOT EXISTS runtime_resource_instances_nft_table_name_active_uq
  ON runtime_resource_instances (nft_table_name)
  WHERE nft_table_name IS NOT NULL
    AND state NOT IN ('absent_verified','destroyed');

CREATE UNIQUE INDEX IF NOT EXISTS runtime_resource_instances_control_dir_path_active_uq
  ON runtime_resource_instances (control_dir_path)
  WHERE control_dir_path IS NOT NULL
    AND state NOT IN ('absent_verified','destroyed');

CREATE UNIQUE INDEX IF NOT EXISTS runtime_resource_instances_control_manifest_path_active_uq
  ON runtime_resource_instances (control_manifest_path)
  WHERE control_manifest_path IS NOT NULL
    AND state NOT IN ('absent_verified','destroyed');

CREATE UNIQUE INDEX IF NOT EXISTS runtime_resource_instances_bundle_dir_path_active_uq
  ON runtime_resource_instances (bundle_dir_path)
  WHERE bundle_dir_path IS NOT NULL
    AND state NOT IN ('absent_verified','destroyed');

CREATE UNIQUE INDEX IF NOT EXISTS runtime_resource_instances_spec_path_active_uq
  ON runtime_resource_instances (spec_path)
  WHERE spec_path IS NOT NULL
    AND state NOT IN ('absent_verified','destroyed');

CREATE UNIQUE INDEX IF NOT EXISTS runtime_resource_instances_checkpoint_path_active_uq
  ON runtime_resource_instances (checkpoint_path)
  WHERE checkpoint_path IS NOT NULL
    AND state NOT IN ('absent_verified','destroyed');

CREATE UNIQUE INDEX IF NOT EXISTS runtime_resource_instances_bridge_dir_path_active_uq
  ON runtime_resource_instances (bridge_dir_path)
  WHERE bridge_dir_path IS NOT NULL
    AND state NOT IN ('absent_verified','destroyed');

CREATE UNIQUE INDEX IF NOT EXISTS runtime_resource_instances_network_hosts_path_active_uq
  ON runtime_resource_instances (network_hosts_path)
  WHERE network_hosts_path IS NOT NULL
    AND state NOT IN ('absent_verified','destroyed');

CREATE UNIQUE INDEX IF NOT EXISTS runtime_resource_instances_log_dir_path_active_uq
  ON runtime_resource_instances (log_dir_path)
  WHERE log_dir_path IS NOT NULL
    AND state NOT IN ('absent_verified','destroyed');
`)
	if err != nil {
		return err
	}
	return s.rejectRemovedSessionColumns(ctx)
}

func (s *Store) rejectRemovedSessionColumns(ctx context.Context) error {
	for _, column := range removedSessionColumns {
		exists, err := tableColumnExists(ctx, s.db, "sessions", column)
		if err != nil {
			return fmt.Errorf("check removed sessions.%s: %w", column, err)
		}
		if exists {
			return fmt.Errorf("removed sessions.%s column is present; run the destructive cutover cleanup before starting this build", column)
		}
	}
	return nil
}

func tableColumnExists(ctx context.Context, db *sql.DB, table, column string) (bool, error) {
	rows, err := db.QueryContext(ctx, `PRAGMA table_info(`+quoteSQLiteIdent(table)+`)`)
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
