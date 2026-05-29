package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/agents"
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
	if err := ValidatePiDriverStatePayloadForHost(payload, "", ""); err == nil || !strings.Contains(err.Error(), "host path is required") {
		t.Fatalf("expected missing pi host path rejection, got %v", err)
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

	agentHome := t.TempDir()
	sessionRoot := filepath.Join(agentHome, ".pi", "agent", "sessions")
	if err := os.MkdirAll(sessionRoot, 0o755); err != nil {
		t.Fatalf("create pi session root: %v", err)
	}
	sessionFile := filepath.Join(sessionRoot, "session-1.jsonl")
	if err := os.WriteFile(sessionFile, []byte("{}\n"), 0o644); err != nil {
		t.Fatalf("write pi session file: %v", err)
	}
	initialized["selected_session_relpath"] = "session-1.jsonl"
	initialized["selected_session_file"] = agents.PiSessionDir + "/session-1.jsonl"
	canonical, _, err := canonicalDriverStatePayload(initialized, "pi")
	if err != nil {
		t.Fatalf("canonical pi host state: %v", err)
	}
	if err := ValidatePiDriverStatePayloadForHost(canonical, agentHome, "42"); err != nil {
		t.Fatalf("host pi state rejected valid session file: %v", err)
	}
	if err := os.Remove(sessionFile); err != nil {
		t.Fatalf("remove pi session file: %v", err)
	}
	if err := os.Symlink(filepath.Join(t.TempDir(), "escape.jsonl"), sessionFile); err != nil {
		t.Fatalf("symlink pi session file: %v", err)
	}
	if err := ValidatePiDriverStatePayloadForHost(canonical, agentHome, "42"); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("expected symlink session file rejection, got %v", err)
	}

	symlinkHome := t.TempDir()
	outsideRoot := filepath.Join(t.TempDir(), "sessions")
	if err := os.MkdirAll(outsideRoot, 0o755); err != nil {
		t.Fatalf("create external pi session root: %v", err)
	}
	if err := os.WriteFile(filepath.Join(outsideRoot, "session-1.jsonl"), []byte("{}\n"), 0o644); err != nil {
		t.Fatalf("write external pi session file: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(symlinkHome, ".pi", "agent"), 0o755); err != nil {
		t.Fatalf("create pi agent dir: %v", err)
	}
	if err := os.Symlink(outsideRoot, filepath.Join(symlinkHome, ".pi", "agent", "sessions")); err != nil {
		t.Fatalf("symlink pi session root: %v", err)
	}
	if err := ValidatePiDriverStatePayloadForHost(canonical, symlinkHome, "42"); err == nil || !strings.Contains(err.Error(), "session root realpath escapes") {
		t.Fatalf("expected symlink session root rejection, got %v", err)
	}
}

func TestPiCompletedTurnAdvancesSidecarOnlyAfterHostSessionValidation(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	now := time.Now().UTC()
	sessionID := "sess_pi_sidecar"
	createStoreSessionWithAgent(t, ctx, st, sessionID, "pi")
	cfg := testAllocatorConfig(t)
	cfg.Agent = "pi"
	cfg.AgentModel = "sonnet"
	cfg.AgentOutputFormat = agents.PiEventSchemaVersion
	modelAccessAllowed := true
	cfg.ModelAccessAllowed = &modelAccessAllowed
	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: sessionID,
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate pi generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, sessionID, allocation.GenerationID, allocation.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark resources live: %v", err)
	}
	agentHome := filepath.Join(t.TempDir(), "agent-home")
	createPiRuntimeResourceLiveForTest(t, ctx, st, sessionID, allocation, owner.UUID, agentHome, now.Add(2*time.Second))
	turnID, err := st.EnqueueTurn(ctx, sessionID, "pi turn", now.Add(3*time.Second))
	if err != nil {
		t.Fatalf("enqueue pi turn: %v", err)
	}
	grant, ok, err := st.ClaimNextTurn(ctx, ClaimNextTurnParams{
		SessionID:    sessionID,
		GenerationID: allocation.GenerationID,
		Owner:        allocation.Owner,
		RequestID:    "claim_pi",
		LeaseTTL:     time.Minute,
		Now:          now.Add(4 * time.Second),
	})
	if err != nil || !ok || grant.TurnID != turnID {
		t.Fatalf("claim pi turn: ok=%v grant=%+v err=%v", ok, grant, err)
	}
	if grant.DriverState.DriverID != "pi" || len(grant.DriverStatePayload) == 0 {
		t.Fatalf("pi grant missing sidecar payload: %+v payload=%s", grant.DriverState, grant.DriverStatePayload)
	}
	if _, err := st.AckTurnStarted(ctx, AckStartedParams{
		SessionID:       sessionID,
		GenerationID:    allocation.GenerationID,
		TurnID:          turnID,
		Owner:           allocation.Owner,
		SandboxSourceIP: sandboxSourceIPForGeneration(t, ctx, st, allocation.GenerationID),
		LeaseTTL:        time.Minute,
		Now:             now.Add(5 * time.Second),
	}); err != nil {
		t.Fatalf("ack pi turn started: %v", err)
	}
	update := piDriverStateUpdateForTest(t, allocation.DriverState, turnID, "session-1.jsonl", "pi-session-1")
	if _, err := st.CompleteTurn(ctx, CompleteTurnParams{
		SessionID:         sessionID,
		GenerationID:      allocation.GenerationID,
		TurnID:            turnID,
		Owner:             allocation.Owner,
		TerminalStatus:    "completed",
		DriverStateUpdate: &update,
		Now:               now.Add(6 * time.Second),
	}); err == nil || !strings.Contains(err.Error(), "host file missing") {
		t.Fatalf("expected missing pi host session file rejection, got %v", err)
	}
	sessionRoot := filepath.Join(agentHome, ".pi", "agent", "sessions")
	if err := os.MkdirAll(sessionRoot, 0o755); err != nil {
		t.Fatalf("create pi session root: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sessionRoot, "session-1.jsonl"), []byte("{}\n"), 0o644); err != nil {
		t.Fatalf("write pi host session: %v", err)
	}
	if _, err := st.CompleteTurn(ctx, CompleteTurnParams{
		SessionID:         sessionID,
		GenerationID:      allocation.GenerationID,
		TurnID:            turnID,
		Owner:             allocation.Owner,
		TerminalStatus:    "completed",
		DriverStateUpdate: &update,
		Now:               now.Add(7 * time.Second),
	}); err != nil {
		t.Fatalf("complete pi turn after host validation: %v", err)
	}
	var version int
	var payload string
	if err := st.db.QueryRowContext(ctx, `
SELECT state_version, state_payload
FROM session_driver_states
WHERE session_id = ?
  AND driver_id = 'pi'`, sessionID).Scan(&version, &payload); err != nil {
		t.Fatalf("query pi sidecar: %v", err)
	}
	if version != 2 || !strings.Contains(payload, `"selected_session_id":"pi-session-1"`) {
		t.Fatalf("unexpected pi sidecar after complete: version=%d payload=%s", version, payload)
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

func piDriverStateUpdateForTest(t *testing.T, previous DriverStateToken, turnID int64, relpath, sessionID string) DriverStateUpdate {
	t.Helper()
	payload := map[string]any{
		"schema_version":           1,
		"driver_id":                "pi",
		"state_kind":               "pi_session",
		"session_dir":              agents.PiSessionDir,
		"selected_session_relpath": relpath,
		"selected_session_file":    agents.PiSessionDir + "/" + relpath,
		"selected_session_id":      sessionID,
		"last_completed_turn_id":   strconv.FormatInt(turnID, 10),
	}
	canonical, digest, err := canonicalDriverStatePayload(payload, "pi")
	if err != nil {
		t.Fatalf("canonical pi update: %v", err)
	}
	return DriverStateUpdate{
		DriverID:            "pi",
		PreviousStateDigest: previous.StateDigest,
		StatePayload:        json.RawMessage(canonical),
		StateDigest:         digest,
		StateVersion:        previous.StateVersion + 1,
	}
}

func createPiRuntimeResourceLiveForTest(t *testing.T, ctx context.Context, st *Store, sessionID string, allocation GenerationAllocation, ownerUUID, agentHomeHostPath string, now time.Time) RuntimeResourceInstance {
	t.Helper()
	contractID := "contract_" + allocation.GenerationID
	payload := testSandboxContractPayload(t, sessionID, allocation)
	payload["contract_gate_version"] = SandboxContractGatePhase9C
	driver := payload["driver"].(map[string]any)
	driver["bridge_protocol"] = "harness_bridge_v2"
	driver["bridge_protocol_version"] = 2
	driver["turn_input_schema"] = "RunTurn"
	driver["output_schema"] = agents.PiEventSchemaVersion
	payload["input_digests"] = map[string]any{
		"runtime_config_digest": "sha256:runtime-config",
		"rootfs_image_digest":   nil,
		"agent_manifest_digest": "sha256:agent-manifest",
	}
	mountPlan := payload["mount_plan"].(map[string]any)
	mountPlan["agent_home"].(map[string]any)["source"] = agentHomeHostPath
	payload["data_volumes"] = map[string]any{
		"agent_home": map[string]any{
			"table":                      "session_driver_homes",
			"session_id":                 sessionID,
			"driver":                     "pi",
			"driver_home_key":            "pi",
			"host_path":                  agentHomeHostPath,
			"layout_version":             DataVolumeLayoutVersion,
			"runtime_identity_digest":    "sha256:driver-home",
			"provisioning_marker_path":   filepath.Join(t.TempDir(), "pi-driver-home.json"),
			"provisioning_marker_digest": "sha256:driver-home-marker",
			"sandbox_destination":        "/agent-home",
		},
	}
	addPiMaterializedConfigForTest(payload)
	if _, err := st.StoreSandboxContract(ctx, StoreSandboxContractParams{
		ContractID:             contractID,
		SessionID:              sessionID,
		GenerationID:           allocation.GenerationID,
		Owner:                  allocation.Owner,
		SandboxContractVersion: SandboxContractVersion,
		ContractGateVersion:    SandboxContractGatePhase9C,
		DriverState:            allocation.DriverState,
		Payload:                payload,
		Now:                    now,
	}); err != nil {
		t.Fatalf("store pi sandbox contract: %v", err)
	}
	details, err := st.GetRuntimeGenerationDetails(ctx, sessionID, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get pi generation details: %v", err)
	}
	sandboxIP := sandboxIPFromCIDRForTest(t, details.SandboxIPCIDR)
	instance, err := st.CreateRuntimeResourceInstance(ctx, RuntimeResourceInstanceParams{
		GenerationID:           allocation.GenerationID,
		SessionID:              sessionID,
		ContractID:             contractID,
		SandboxContractVersion: SandboxContractVersion,
		HostID:                 "host-pi",
		RunscContainerID:       details.RunscContainerID,
		RunscPlatform:          "systrap",
		RunscVersion:           "runsc test",
		RunscBinaryPath:        filepath.Join(t.TempDir(), "runsc"),
		RunscBinaryDigest:      "sha256:runsc",
		NetworkProfileID:       allocation.NetworkProfileID,
		NetnsName:              details.NetnsName,
		NetnsPath:              details.NetnsPath,
		HostVeth:               details.HostVeth,
		SandboxVeth:            details.SandboxVeth,
		HostGatewayIP:          details.HostGatewayIP,
		SandboxIP:              sandboxIP,
		SandboxIPCIDR:          details.SandboxIPCIDR,
		HostSideCIDR:           details.HostSideCIDR,
		NftTableName:           "harness_gen_" + strings.TrimPrefix(allocation.GenerationID, "gen_"),
		ControlDirPath:         details.ControlDirPath,
		ControlManifestPath:    details.ControlManifestPath,
		BundleDirPath:          details.BundleDirPath,
		SpecPath:               details.SpecPath,
		CheckpointPath:         details.CheckpointPath,
		BridgeDirPath:          details.BridgeDirPath,
		LogDirPath:             details.LogDirPath,
		RootPrefixes: map[string]string{
			"run_dir":      filepath.Dir(filepath.Dir(details.ControlDirPath)),
			"control_root": filepath.Dir(details.ControlDirPath),
			"bridge_root":  filepath.Dir(details.BridgeDirPath),
		},
		Now: now,
	})
	if err != nil {
		t.Fatalf("create pi runtime resource instance: %v", err)
	}
	if err := st.ClaimRuntimeResourceMaterialization(ctx, RuntimeResourceMaterializationClaimParams{
		GenerationID:     allocation.GenerationID,
		WorkerID:         ownerUUID,
		HostID:           "host-pi",
		LeaseExpiresAt:   now.Add(time.Minute),
		IdempotencyToken: "test:" + allocation.GenerationID,
		Now:              now.Add(time.Millisecond),
	}); err != nil {
		t.Fatalf("claim pi resource materialization: %v", err)
	}
	if err := st.MarkRuntimeResourceReady(ctx, RuntimeResourceWorkerTransitionParams{
		GenerationID: allocation.GenerationID,
		WorkerID:     ownerUUID,
		HostID:       "host-pi",
		Now:          now.Add(2 * time.Millisecond),
	}); err != nil {
		t.Fatalf("mark pi resource ready: %v", err)
	}
	if err := st.MarkRuntimeResourceLive(ctx, RuntimeResourceWorkerTransitionParams{
		GenerationID: allocation.GenerationID,
		WorkerID:     ownerUUID,
		HostID:       "host-pi",
		PostStart:    runtimeResourcePostStartProofForTest(instance),
		Now:          now.Add(3 * time.Millisecond),
	}); err != nil {
		t.Fatalf("mark pi resource live: %v", err)
	}
	return instance
}
