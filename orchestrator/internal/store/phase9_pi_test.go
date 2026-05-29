package store

import (
	"context"
	"database/sql"
	"net/netip"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/sessionstate"

	_ "modernc.org/sqlite"
)

func TestPhase9PiSchemaWideningPreservesExistingRows(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(dir, "phase9f.db"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = db.Close() })

	st := &Store{db: db, options: Options{AgentHomesRoot: filepath.Join(dir, "agent-homes")}}
	if _, err := st.db.ExecContext(ctx, `PRAGMA foreign_keys=ON`); err != nil {
		t.Fatalf("enable foreign keys: %v", err)
	}
	if err := st.runMigrations(ctx, defaultMigrations(st.options)); err != nil {
		t.Fatalf("run base migrations: %v", err)
	}
	if err := st.runPhase9Cutover(ctx); err != nil {
		t.Fatalf("run phase9 cutover: %v", err)
	}
	if err := st.ensurePhase9ModeSchema(ctx); err != nil {
		t.Fatalf("ensure mode schema: %v", err)
	}

	if err := st.EnsureUser(ctx, "lab", "Lab"); err != nil {
		t.Fatalf("ensure user: %v", err)
	}
	owner, err := AcquireOwnerLock(filepath.Join(dir, "run"))
	if err != nil {
		t.Fatalf("acquire owner: %v", err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	if err := st.WriteOwner(ctx, owner); err != nil {
		t.Fatalf("write owner: %v", err)
	}

	baseCfg := testAllocatorConfig(t)
	baseCfg.CIDRPool = netip.MustParsePrefix("10.240.0.0/28")
	claude := createAllocatedPhase9Session(t, ctx, st, owner.UUID, baseCfg, "sess_pi_widen_claude", "claude_code", "stream-json")
	createAllocatedPhase9Session(t, ctx, st, owner.UUID, baseCfg, "sess_pi_widen_shell", "sh", "shell_pty")
	addPhase9UserRows(t, ctx, st, "sess_pi_widen_claude")
	addPhase9UserRows(t, ctx, st, "sess_pi_widen_shell")

	assertPiRejectedByRestrictedPhase9Schema(t, ctx, st, claude.GenerationID)

	if err := st.ensurePhase9PiSchema(ctx); err != nil {
		t.Fatalf("ensure pi schema: %v", err)
	}
	assertPhase9PiMarker(t, ctx, st)
	assertForeignKeyCheckClean(t, ctx, st.db)

	for _, sessionID := range []string{"sess_pi_widen_claude", "sess_pi_widen_shell"} {
		if messages, err := st.ListMessages(ctx, sessionID); err != nil || len(messages) != 1 {
			t.Fatalf("messages for %s len=%d err=%v", sessionID, len(messages), err)
		}
		if artifacts, err := st.ListArtifacts(ctx, sessionID); err != nil || len(artifacts) != 1 {
			t.Fatalf("artifacts for %s len=%d err=%v", sessionID, len(artifacts), err)
		}
	}
	if state := driverStatePayloadForTest(t, ctx, st, "sess_pi_widen_claude", "claude_code"); !strings.Contains(state, `"driver_id":"claude_code"`) {
		t.Fatalf("claude driver state was not preserved: %s", state)
	}
	if state := driverStatePayloadForTest(t, ctx, st, "sess_pi_widen_shell", "sh"); !strings.Contains(state, `"driver_id":"sh"`) {
		t.Fatalf("shell driver state was not preserved: %s", state)
	}

	pi := createAllocatedPhase9Session(t, ctx, st, owner.UUID, baseCfg, "sess_pi_widen_pi", "pi", "pi_rpc_events_v1.0")
	state := driverStatePayloadForTest(t, ctx, st, "sess_pi_widen_pi", "pi")
	if !strings.Contains(state, `"state_kind":"pi_uninitialized"`) ||
		!strings.Contains(state, `"session_dir":"/agent-home/.pi/agent/sessions"`) ||
		pi.DriverState.DriverID != "pi" ||
		pi.DriverState.StateVersion != 1 {
		t.Fatalf("unexpected pi bootstrap state allocation=%+v payload=%s", pi.DriverState, state)
	}
	if got := ModeForDriver("pi"); got != "agent" {
		t.Fatalf("ModeForDriver(pi)=%q want agent", got)
	}
	if key, err := DriverHomeKeyFor("pi"); err != nil || key != "pi" {
		t.Fatalf("DriverHomeKeyFor(pi)=%q err=%v", key, err)
	}
}

func TestPiDriverStateValidation(t *testing.T) {
	payload, digest, err := canonicalBootstrapDriverState("pi", "")
	if err != nil {
		t.Fatalf("bootstrap pi state: %v", err)
	}
	if digest != DriverStateDigest(payload) {
		t.Fatalf("bootstrap digest mismatch")
	}
	initialized := map[string]any{
		"schema_version":           1,
		"driver_id":                "pi",
		"state_kind":               "pi_session",
		"session_dir":              "/agent-home/.pi/agent/sessions",
		"selected_session_relpath": "session-1.jsonl",
		"selected_session_file":    "/agent-home/.pi/agent/sessions/session-1.jsonl",
		"selected_session_id":      "pi-session-1",
		"last_completed_turn_id":   "42",
	}
	if _, _, err := canonicalDriverStatePayload(initialized, "pi"); err != nil {
		t.Fatalf("initialized pi state rejected: %v", err)
	}

	for _, rel := range []string{"../escape.jsonl", "/tmp/session.jsonl", "nested/../escape.jsonl", ""} {
		initialized["selected_session_relpath"] = rel
		initialized["selected_session_file"] = "/agent-home/.pi/agent/sessions/" + rel
		if _, _, err := canonicalDriverStatePayload(initialized, "pi"); err == nil {
			t.Fatalf("pi state relpath %q should be rejected", rel)
		}
	}
}

func createAllocatedPhase9Session(t *testing.T, ctx context.Context, st *Store, ownerUUID string, cfg ResourceAllocatorConfig, sessionID, driverID, outputFormat string) GenerationAllocation {
	t.Helper()
	createStoreSessionWithAgent(t, ctx, st, sessionID, driverID)
	cfg.Agent = driverID
	cfg.AgentOutputFormat = outputFormat
	if driverID == "sh" {
		cfg.AgentModel = ""
	}
	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: sessionID,
		Owner:     GenerationLeaseOwner(ownerUUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate %s generation: %v", driverID, err)
	}
	return allocation
}

func addPhase9UserRows(t *testing.T, ctx context.Context, st *Store, sessionID string) {
	t.Helper()
	if _, err := st.AddMessage(ctx, sessionID, "user", "hello "+sessionID); err != nil {
		t.Fatalf("add message %s: %v", sessionID, err)
	}
	if err := st.UpsertArtifact(ctx, Artifact{
		SessionID: sessionID,
		Path:      "artifact.txt",
		Size:      12,
		ModTime:   time.Now().UTC(),
	}); err != nil {
		t.Fatalf("upsert artifact %s: %v", sessionID, err)
	}
}

func assertPiRejectedByRestrictedPhase9Schema(t *testing.T, ctx context.Context, st *Store, generationID string) {
	t.Helper()
	now := formatTime(time.Now().UTC())
	if _, err := st.db.ExecContext(ctx, `
INSERT INTO sessions (
  id, user_id, status, driver_id, mode, workspace, restore_id, created_at, updated_at
) VALUES ('sess_pi_before_9f', 'lab', ?, 'pi', 'agent', '/tmp/pi', 'restore-pi', ?, ?)`,
		string(sessionstate.Created), now, now); err == nil {
		t.Fatalf("restricted sessions.driver_id accepted pi before 9f")
	}
	if _, err := st.db.ExecContext(ctx, `
INSERT INTO agent_runtime_profiles (
  agent_runtime_profile_id, driver_id, model, output_format,
  disable_nonessential_traffic, sandbox_uid, sandbox_gid,
  sandbox_supplemental_gids, requires_secret_drop, model_access_allowed,
  created_at
) VALUES ('arp_pi_before_9f', 'pi', 'sonnet', 'pi_rpc_events_v1.0', 1, 7000, 7001, '[]', 0, 1, ?)`, now); err == nil {
		t.Fatalf("restricted agent_runtime_profiles.driver_id accepted pi before 9f")
	}
	if _, err := st.db.ExecContext(ctx, `
INSERT INTO session_driver_states (
  session_id, driver_id, state_payload, state_digest, state_version,
  updated_generation_id, updated_at
) VALUES ('sess_pi_widen_claude', 'pi', '{}', 'sha256:bad', 1, ?, ?)`, generationID, now); err == nil {
		t.Fatalf("restricted session_driver_states.driver_id accepted pi before 9f")
	}
}

func assertPhase9PiMarker(t *testing.T, ctx context.Context, st *Store) {
	t.Helper()
	var payload string
	if err := st.db.QueryRowContext(ctx, `SELECT payload FROM phase9_cutover_state WHERE key = ?`, phase9PiSchemaMarker).Scan(&payload); err != nil {
		t.Fatalf("phase9f marker missing: %v", err)
	}
	for _, field := range []string{"sessions.driver_id", "agent_runtime_profiles.driver_id", "session_driver_states.driver_id"} {
		if !strings.Contains(payload, field) {
			t.Fatalf("phase9f marker payload %q missing %s", payload, field)
		}
	}
}

func assertForeignKeyCheckClean(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	rows, err := db.QueryContext(ctx, `PRAGMA foreign_key_check`)
	if err != nil {
		t.Fatalf("foreign key check: %v", err)
	}
	defer rows.Close()
	if rows.Next() {
		t.Fatalf("foreign key check returned violation")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("foreign key check rows: %v", err)
	}
}

func driverStatePayloadForTest(t *testing.T, ctx context.Context, st *Store, sessionID, driverID string) string {
	t.Helper()
	var payload string
	if err := st.db.QueryRowContext(ctx, `
SELECT state_payload
FROM session_driver_states
WHERE session_id = ?
  AND driver_id = ?`, sessionID, driverID).Scan(&payload); err != nil {
		t.Fatalf("query driver state payload: %v", err)
	}
	return payload
}
