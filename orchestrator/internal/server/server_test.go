package server

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/artifacts"
	"harness-platform/orchestrator/internal/bridge"
	"harness-platform/orchestrator/internal/config"
	"harness-platform/orchestrator/internal/events"
	"harness-platform/orchestrator/internal/runtime"
	"harness-platform/orchestrator/internal/sessionstate"
	"harness-platform/orchestrator/internal/store"
)

func TestCreateSessionRejectsUnsupportedAgent(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	srv := &Server{
		cfg: config.Config{
			SessionsRoot:     dir,
			SessionRetention: time.Hour,
			MaxSessions:      10,
			DefaultAgent:     "claude",
		},
		store:   st,
		runtime: runtime.New(runtime.Config{}),
		watcher: newServerTestWatcher(t, dir, st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}

	req := httptest.NewRequest(http.MethodPost, "/api/sessions", strings.NewReader(`{"agent":"opencode"}`))
	rec := httptest.NewRecorder()

	srv.createSession(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "unsupported agent") {
		t.Fatalf("expected unsupported agent error, got %s", rec.Body.String())
	}
}

func TestCreateSessionSoftLimitUsesPoolExhaustedEnvelope(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	createServerTestSession(t, ctx, st, dir, "sess_existing", string(sessionstate.Created), time.Now().UTC(), nil)

	srv := &Server{
		cfg: config.Config{
			SessionsRoot:     dir,
			SessionRetention: time.Hour,
			MaxSessions:      1,
			DefaultAgent:     "claude",
		},
		store:   st,
		runtime: runtime.New(runtime.Config{}),
		watcher: newServerTestWatcher(t, dir, st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}

	req := httptest.NewRequest(http.MethodPost, "/api/sessions", strings.NewReader(`{"agent":"claude"}`))
	rec := httptest.NewRecorder()
	srv.createSession(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d body %s", rec.Code, rec.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["error_class"] != "pool_exhausted" {
		t.Fatalf("expected pool_exhausted, got %v", body)
	}
}

func TestCloseSessionReleasesSoftLimitWithoutDeletingHistory(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	cfg := testServerConfig(dir)
	cfg.SessionRetention = 0
	cfg.MaxSessions = 1
	cfg.Phase7.MaxSessions = 1
	now := time.Now().UTC()
	oldSession := store.Session{
		ID:                "sess_retained",
		UserID:            labUserID,
		Status:            string(sessionstate.Created),
		Agent:             "claude",
		Workspace:         filepath.Join(cfg.SessionsRoot, "sess_retained"),
		AgentHomePath:     filepath.Join(dir, "agent-homes", "sess_retained"),
		RestoreID:         "phase3-sess_retained",
		ClaudeSessionUUID: "11111111-2222-3333-4444-555555555555",
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	if err := os.MkdirAll(oldSession.Workspace, 0o755); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := os.MkdirAll(oldSession.AgentHomePath, 0o755); err != nil {
		t.Fatalf("create agent home: %v", err)
	}
	if err := st.CreateSession(ctx, oldSession); err != nil {
		t.Fatalf("create retained session: %v", err)
	}
	if _, err := st.AddMessage(ctx, oldSession.ID, "user", "keep this"); err != nil {
		t.Fatalf("add message: %v", err)
	}
	if err := st.UpsertArtifact(ctx, store.Artifact{
		SessionID: oldSession.ID,
		Path:      "report.txt",
		Size:      12,
		ModTime:   now,
	}); err != nil {
		t.Fatalf("upsert artifact: %v", err)
	}

	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: runtime.New(runtime.Config{}),
		watcher: newServerTestWatcher(t, cfg.SessionsRoot, st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}

	req := httptest.NewRequest(http.MethodPost, "/api/sessions", strings.NewReader(`{"agent":"claude"}`))
	rec := httptest.NewRecorder()
	srv.createSession(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected quota rejection before close, got %d body %s", rec.Code, rec.Body.String())
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/api/sessions/"+oldSession.ID, nil)
	deleteRec := httptest.NewRecorder()
	srv.destroySession(deleteRec, deleteReq, oldSession.ID)
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("expected close status 200, got %d body %s", deleteRec.Code, deleteRec.Body.String())
	}

	createReq := httptest.NewRequest(http.MethodPost, "/api/sessions", strings.NewReader(`{"agent":"claude"}`))
	createRec := httptest.NewRecorder()
	srv.createSession(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("expected create after close, got %d body %s", createRec.Code, createRec.Body.String())
	}

	closed, err := st.GetSession(ctx, oldSession.ID)
	if err != nil {
		t.Fatalf("get closed session: %v", err)
	}
	if closed.Status != string(sessionstate.Destroyed) ||
		closed.Workspace != oldSession.Workspace ||
		closed.AgentHomePath != oldSession.AgentHomePath {
		t.Fatalf("closed session should preserve terminal state and paths: %+v", closed)
	}
	if _, err := os.Stat(oldSession.Workspace); err != nil {
		t.Fatalf("workspace should remain after close: %v", err)
	}
	if _, err := os.Stat(oldSession.AgentHomePath); err != nil {
		t.Fatalf("agent home should remain after close: %v", err)
	}
	messages, err := st.ListMessages(ctx, oldSession.ID)
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(messages) != 1 || messages[0].Content != "keep this" {
		t.Fatalf("expected retained message, got %+v", messages)
	}
	artifacts, err := st.ListArtifacts(ctx, oldSession.ID)
	if err != nil {
		t.Fatalf("list artifacts: %v", err)
	}
	if len(artifacts) != 1 || artifacts[0].Path != "report.txt" {
		t.Fatalf("expected retained artifact, got %+v", artifacts)
	}
}

func TestCreateSessionUsesNullExpiryWhenSessionRetentionDisabled(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	srv := &Server{
		cfg: config.Config{
			SessionsRoot:     dir,
			SessionRetention: 0,
			MaxSessions:      10,
			DefaultAgent:     "claude",
		},
		store:   st,
		runtime: runtime.New(runtime.Config{}),
		watcher: newServerTestWatcher(t, dir, st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}

	req := httptest.NewRequest(http.MethodPost, "/api/sessions", strings.NewReader(`{"agent":"claude"}`))
	rec := httptest.NewRecorder()
	srv.createSession(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d body %s", rec.Code, rec.Body.String())
	}
	var created store.Session
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode session: %v", err)
	}
	if created.ExpiresAt != nil {
		t.Fatalf("expected nil expires_at in response, got %v", created.ExpiresAt)
	}
	got, err := st.GetSession(ctx, created.ID)
	if err != nil {
		t.Fatalf("get created session: %v", err)
	}
	if got.ExpiresAt != nil {
		t.Fatalf("expected nil stored expires_at, got %v", got.ExpiresAt)
	}
	changed, err := st.SweepExpiredSessions(ctx, time.Now().Add(24*time.Hour))
	if err != nil {
		t.Fatalf("sweep sessions: %v", err)
	}
	if changed != 0 {
		t.Fatalf("expected NULL expires_at session to be preserved, swept %d", changed)
	}
	got, err = st.GetSession(ctx, created.ID)
	if err != nil {
		t.Fatalf("get session after sweep: %v", err)
	}
	if got.Status != string(sessionstate.Created) {
		t.Fatalf("expected session to remain created, got %s", got.Status)
	}
}

func TestCreateSessionUsesPublicSessionDTO(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	hub := events.NewHub()
	eventsCh, cancelEvents := hub.Subscribe("")
	defer cancelEvents()
	srv := &Server{
		cfg: config.Config{
			SessionsRoot:     dir,
			SessionRetention: time.Hour,
			MaxSessions:      10,
			DefaultAgent:     "claude",
		},
		store:   st,
		runtime: runtime.New(runtime.Config{}),
		watcher: newServerTestWatcher(t, dir, st, events.NewHub()),
		hub:     hub,
		log:     slog.Default(),
	}

	req := httptest.NewRequest(http.MethodPost, "/api/sessions", strings.NewReader(`{"agent":"claude"}`))
	rec := httptest.NewRecorder()
	srv.createSession(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d body %s", rec.Code, rec.Body.String())
	}
	assertPublicSessionJSONOmitsHostFields(t, rec.Body.Bytes())
	var created map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	if created["id"] == "" || created["agent"] != "claude" {
		t.Fatalf("unexpected create response: %v", created)
	}

	select {
	case event := <-eventsCh:
		if event.Type != "session.created" {
			t.Fatalf("event type=%s want session.created", event.Type)
		}
		payload, err := json.Marshal(event.Payload)
		if err != nil {
			t.Fatalf("marshal event payload: %v", err)
		}
		assertPublicSessionJSONOmitsHostFields(t, payload)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for session.created event")
	}
}

func TestSessionReadResponsesUsePublicSessionDTO(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	now := time.Now().UTC()
	session := createServerTestSession(t, ctx, st, dir, "sess_public", string(sessionstate.RunningIdle), now, nil)
	restoreMS := int64(123)
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE sessions
SET agent_home_path = ?,
    checkpoint_path = ?,
    restore_ms = ?
WHERE id = ?`, filepath.Join(dir, "agent-homes", session.ID), filepath.Join(dir, "checkpoints", session.ID), restoreMS, session.ID); err != nil {
		t.Fatalf("seed host-only fields: %v", err)
	}

	srv := &Server{
		store: st,
		hub:   events.NewHub(),
		log:   slog.Default(),
	}

	getReq := httptest.NewRequest(http.MethodGet, "/api/sessions/"+session.ID, nil)
	getRec := httptest.NewRecorder()
	srv.getSession(getRec, getReq, session.ID)
	if getRec.Code != http.StatusOK {
		t.Fatalf("expected get status 200, got %d body %s", getRec.Code, getRec.Body.String())
	}
	assertPublicSessionJSONOmitsHostFields(t, getRec.Body.Bytes())
	assertContains(t, getRec.Body.String(), `"restore_ms":123`)

	listReq := httptest.NewRequest(http.MethodGet, "/api/sessions", nil)
	listRec := httptest.NewRecorder()
	srv.listSessions(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("expected list status 200, got %d body %s", listRec.Code, listRec.Body.String())
	}
	assertPublicSessionJSONOmitsHostFields(t, listRec.Body.Bytes())
	assertContains(t, listRec.Body.String(), `"id":"sess_public"`)
}

func TestMonitorIdleSessionsSkipsHostNetwork(t *testing.T) {
	srv := &Server{
		cfg: config.Config{
			RunscNetwork: "host",
		},
		log: slog.Default(),
	}

	if err := srv.MonitorIdleSessions(context.Background()); err != nil {
		t.Fatalf("expected idle monitor to exit cleanly in host mode, got %v", err)
	}
}

func TestMonitorIdleSessionsReEnablesCheckpointedSessionsWhenCheckpointDisabled(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	now := time.Now().UTC()
	session := store.Session{
		ID:        "sess_checkpointed",
		UserID:    "lab",
		Status:    string(sessionstate.Checkpointed),
		Agent:     "claude",
		Workspace: filepath.Join(dir, "sessions", "sess_checkpointed"),
		RestoreID: "phase3-sess_checkpointed",
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := st.CreateSession(ctx, session); err != nil {
		t.Fatalf("create session: %v", err)
	}

	srv := &Server{
		cfg: config.Config{
			RunscNetwork: "sandbox",
		},
		store: st,
		hub:   events.NewHub(),
		log:   slog.Default(),
	}
	if err := srv.MonitorIdleSessions(context.Background()); err != nil {
		t.Fatalf("monitor idle sessions: %v", err)
	}
	got, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if got.Status != string(sessionstate.Checkpointed) {
		t.Fatalf("disabled monitor should leave checkpointed session alone, got %s", got.Status)
	}
}

func TestBridgeCheckpointReadyRequiresFreshHeartbeatAndMarker(t *testing.T) {
	dir := t.TempDir()
	now := time.Now().UTC()

	if bridgeCheckpointReady(dir, now, time.Second) {
		t.Fatal("empty bridge dir should not be checkpoint-ready")
	}
	if err := bridge.TouchHeartbeat(dir, bridge.BridgeHeartbeatFile, now); err != nil {
		t.Fatalf("touch heartbeat: %v", err)
	}
	if bridgeCheckpointReady(dir, now, time.Second) {
		t.Fatal("missing ready marker should not be checkpoint-ready")
	}
	if err := bridge.TouchCheckpointReady(dir, now); err != nil {
		t.Fatalf("touch ready: %v", err)
	}
	if !bridgeCheckpointReady(dir, now, time.Second) {
		t.Fatal("fresh heartbeat and ready marker should be checkpoint-ready")
	}
	if bridgeCheckpointReady(dir, now.Add(10*time.Second), time.Second) {
		t.Fatal("stale bridge control files should not be checkpoint-ready")
	}
}

func TestMonitorIdleSessionsCheckpointsEligibleGeneration(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_auto_checkpoint", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	enableSessionAutoCheckpoint(t, ctx, st, session.ID)
	cfg := testServerConfig(dir)
	cfg.Phase7.Checkpoint.AutoEnabled = true
	cfg.Phase7.Checkpoint.IdleThreshold = config.Duration{Duration: time.Nanosecond}
	cfg.Phase7.Checkpoint.MonitorInterval = config.Duration{Duration: time.Hour}
	allocation := prepareServerIdleGeneration(t, ctx, st, cfg, owner.UUID, session.ID)
	details, err := st.GetRuntimeGenerationDetails(ctx, session.ID, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get generation details: %v", err)
	}
	if err := bridge.TouchHeartbeat(details.BridgeDirPath, bridge.BridgeHeartbeatFile, time.Now().UTC()); err != nil {
		t.Fatalf("touch heartbeat: %v", err)
	}
	if err := bridge.TouchCheckpointReady(details.BridgeDirPath, time.Now().UTC()); err != nil {
		t.Fatalf("touch ready: %v", err)
	}
	rt := &recordingRuntime{}
	runCtx, cancel := context.WithCancel(ctx)
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.MonitorIdleSessions(runCtx)
	}()
	waitForSessionStatus(t, ctx, st, session.ID, string(sessionstate.Checkpointed))
	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("monitor exit err=%v, want context canceled", err)
	}

	checkpoints := rt.checkpointRequests()
	if len(checkpoints) != 1 {
		t.Fatalf("checkpoint requests=%d want 1: %+v", len(checkpoints), checkpoints)
	}
	if checkpoints[0].SessionID != session.ID ||
		checkpoints[0].GenerationID != allocation.GenerationID ||
		checkpoints[0].CheckpointPath != details.CheckpointPath {
		t.Fatalf("unexpected checkpoint request: %+v details=%+v", checkpoints[0], details)
	}
	var generationStatus, networkState, resourceState, checkpointPath, checkpointBundle, checkpointRuntimeConfig, checkpointManifest string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state, COALESCE(r.checkpoint_path, ''),
       COALESCE(g.checkpoint_bundle_digest, ''), COALESCE(g.checkpoint_runtime_config_digest, ''), COALESCE(g.checkpoint_control_manifest_digest, '')
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(
		&generationStatus, &networkState, &resourceState, &checkpointPath,
		&checkpointBundle, &checkpointRuntimeConfig, &checkpointManifest,
	); err != nil {
		t.Fatalf("query checkpointed generation: %v", err)
	}
	if generationStatus != "checkpointed" ||
		networkState != "reserved_checkpointed" ||
		resourceState != "reserved_checkpointed" ||
		checkpointPath != details.CheckpointPath ||
		checkpointBundle != "bundle_digest" ||
		checkpointRuntimeConfig != "runtime_config_digest" ||
		checkpointManifest != "projected_manifest_digest" {
		t.Fatalf("unexpected checkpoint metadata: generation=%s network=%s resource=%s path=%s bundle=%s runtime=%s manifest=%s",
			generationStatus, networkState, resourceState, checkpointPath, checkpointBundle, checkpointRuntimeConfig, checkpointManifest)
	}
}

func TestMonitorIdleSessionsAbortsFailedCheckpoint(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_auto_checkpoint_fail", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	enableSessionAutoCheckpoint(t, ctx, st, session.ID)
	cfg := testServerConfig(dir)
	cfg.Phase7.Checkpoint.AutoEnabled = true
	cfg.Phase7.Checkpoint.IdleThreshold = config.Duration{Duration: time.Nanosecond}
	cfg.Phase7.Checkpoint.MonitorInterval = config.Duration{Duration: time.Hour}
	allocation := prepareServerIdleGeneration(t, ctx, st, cfg, owner.UUID, session.ID)
	details, err := st.GetRuntimeGenerationDetails(ctx, session.ID, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get generation details: %v", err)
	}
	if err := bridge.TouchHeartbeat(details.BridgeDirPath, bridge.BridgeHeartbeatFile, time.Now().UTC()); err != nil {
		t.Fatalf("touch heartbeat: %v", err)
	}
	if err := bridge.TouchCheckpointReady(details.BridgeDirPath, time.Now().UTC()); err != nil {
		t.Fatalf("touch ready: %v", err)
	}
	rt := &recordingRuntime{checkpointErr: errors.New("checkpoint boom")}
	runCtx, cancel := context.WithCancel(ctx)
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.MonitorIdleSessions(runCtx)
	}()
	waitForCheckpointRequests(t, ctx, rt, 1)
	waitForGenerationStatus(t, ctx, st, allocation.GenerationID, "idle")
	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("monitor exit err=%v, want context canceled", err)
	}
	var generationStatus, networkState, resourceState string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&generationStatus, &networkState, &resourceState); err != nil {
		t.Fatalf("query aborted generation: %v", err)
	}
	if generationStatus != "idle" || networkState != "live" || resourceState != "live" {
		t.Fatalf("checkpoint failure should return generation live idle, got generation=%s network=%s resource=%s", generationStatus, networkState, resourceState)
	}
}

func TestSendMessageAllocatesGenerationAndQueuesBridgeTurn(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_turn", string(sessionstate.Created), time.Now().UTC(), nil)

	srv := &Server{
		cfg:     testServerConfig(dir),
		store:   st,
		runtime: instantRuntime{},
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"hello"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d body %s", rec.Code, rec.Body.String())
	}
	waitForSessionStatus(t, ctx, st, session.ID, string(sessionstate.RunningActive))

	var generations, networkRows, resourceRows, queuedTurns, userMessages int
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_generations WHERE session_id = ?`, session.ID).Scan(&generations); err != nil {
		t.Fatalf("count generations: %v", err)
	}
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM network_profiles WHERE session_id = ? AND allocation_state = 'live'`, session.ID).Scan(&networkRows); err != nil {
		t.Fatalf("count network rows: %v", err)
	}
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM runtime_generation_resources r
JOIN runtime_generations g ON g.generation_id = r.generation_id
WHERE g.session_id = ? AND r.resource_state = 'live'`, session.ID).Scan(&resourceRows); err != nil {
		t.Fatalf("count resource rows: %v", err)
	}
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM turns WHERE session_id = ? AND status = 'queued' AND generation_id IS NULL`, session.ID).Scan(&queuedTurns); err != nil {
		t.Fatalf("count turns: %v", err)
	}
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM messages WHERE session_id = ? AND role = 'user' AND content = 'hello'`, session.ID).Scan(&userMessages); err != nil {
		t.Fatalf("count user messages: %v", err)
	}
	if generations != 1 || networkRows != 1 || resourceRows != 1 || queuedTurns != 1 || userMessages != 1 {
		t.Fatalf("unexpected bridge enqueue rows: generations=%d network=%d resources=%d queued_turns=%d user_messages=%d", generations, networkRows, resourceRows, queuedTurns, userMessages)
	}
}

func TestSendMessageReusesActiveGenerationArtifacts(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_reuse", string(sessionstate.Created), time.Now().UTC(), nil)
	srv := &Server{
		cfg:     testServerConfig(dir),
		store:   st,
		runtime: instantRuntime{},
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)
	atomic.StoreInt64(&instantRuntimePrepareCalls, 0)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"first"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected first status 202, got %d body %s", rec.Code, rec.Body.String())
	}
	waitForSessionStatus(t, ctx, st, session.ID, string(sessionstate.RunningActive))
	if got := atomic.LoadInt64(&instantRuntimePrepareCalls); got != 1 {
		t.Fatalf("first turn prepare calls=%d want 1", got)
	}
	var generationID string
	var firstTurnID int64
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.generation_id, t.id
FROM runtime_generations g
JOIN turns t ON t.session_id = g.session_id
WHERE g.session_id = ?
  AND t.status = 'queued'
  AND t.content = 'first'`, session.ID).Scan(&generationID, &firstTurnID); err != nil {
		t.Fatalf("query first queued turn: %v", err)
	}
	leaseOwner := store.GenerationLeaseOwner(owner.UUID)
	if grant, ok, err := st.ClaimNextTurn(ctx, store.ClaimNextTurnParams{
		SessionID:    session.ID,
		GenerationID: generationID,
		Owner:        leaseOwner,
		RequestID:    "req_first",
		LeaseTTL:     time.Minute,
		Now:          time.Now().UTC(),
	}); err != nil || !ok || grant.TurnID != firstTurnID {
		t.Fatalf("claim first turn: ok=%v grant=%+v err=%v", ok, grant, err)
	}
	if _, err := st.AckTurnStarted(ctx, store.AckStartedParams{
		SessionID:       session.ID,
		GenerationID:    generationID,
		TurnID:          firstTurnID,
		Owner:           leaseOwner,
		SandboxSourceIP: serverSandboxSourceIPForGeneration(t, ctx, st, generationID),
		LeaseTTL:        time.Minute,
		Now:             time.Now().UTC(),
	}); err != nil {
		t.Fatalf("ack first turn started: %v", err)
	}
	if _, err := st.CompleteTurn(ctx, store.CompleteTurnParams{
		SessionID:      session.ID,
		GenerationID:   generationID,
		TurnID:         firstTurnID,
		Owner:          leaseOwner,
		TerminalStatus: "completed",
		Now:            time.Now().UTC(),
	}); err != nil {
		t.Fatalf("complete first turn: %v", err)
	}
	if err := st.UpdateSessionStatusAndActivity(ctx, session.ID, string(sessionstate.RunningIdle), nil, time.Now().UTC()); err != nil {
		t.Fatalf("mark session idle: %v", err)
	}

	req = httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"second"}`))
	rec = httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected second status 202, got %d body %s", rec.Code, rec.Body.String())
	}
	waitForSessionStatus(t, ctx, st, session.ID, string(sessionstate.RunningActive))
	if got := atomic.LoadInt64(&instantRuntimePrepareCalls); got != 1 {
		t.Fatalf("active generation should reuse prepared artifacts, prepare calls=%d", got)
	}
	var completedTurns, queuedTurns int
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM turns WHERE session_id = ? AND status = 'completed'`, session.ID).Scan(&completedTurns); err != nil {
		t.Fatalf("count turns: %v", err)
	}
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM turns WHERE session_id = ? AND status = 'queued' AND content = 'second'`, session.ID).Scan(&queuedTurns); err != nil {
		t.Fatalf("count queued turns: %v", err)
	}
	if completedTurns != 1 || queuedTurns != 1 {
		t.Fatalf("unexpected turn statuses after reuse: completed=%d queued=%d", completedTurns, queuedTurns)
	}
}

func TestSendMessageColdFallbackAllocatesReplacementGeneration(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_send_fallback", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	cfg.Phase7.Network.CIDRPool = config.CIDRPrefix{Prefix: netip.MustParsePrefix("10.241.0.0/28")}
	leaseOwner := store.GenerationLeaseOwner(owner.UUID)
	old, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, "claude"),
	})
	if err != nil {
		t.Fatalf("allocate old generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, old.GenerationID, old.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("mark old generation live: %v", err)
	}
	if err := st.FailGeneration(ctx, store.FailGenerationParams{
		SessionID:    session.ID,
		GenerationID: old.GenerationID,
		Owner:        old.Owner,
		ErrorClass:   "orchestrator_restart_reconnect_grace_expired",
		Reason:       "orchestrator_restart_reconnect_grace_expired",
		Now:          time.Now().UTC(),
	}); err != nil {
		t.Fatalf("fail old generation: %v", err)
	}

	rt := &recordingRuntime{}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"after fallback"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d body %s", rec.Code, rec.Body.String())
	}

	gotSession, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if gotSession.ActiveGenerationID == "" || gotSession.ActiveGenerationID == old.GenerationID {
		t.Fatalf("active generation was not replaced: %q old=%q", gotSession.ActiveGenerationID, old.GenerationID)
	}
	if gotSession.ClaudeSessionUUID != session.ClaudeSessionUUID ||
		gotSession.Workspace != session.Workspace ||
		gotSession.AgentHomePath == "" {
		t.Fatalf("session identity not preserved: before=%+v after=%+v", session, gotSession)
	}
	var oldStatus, oldNetwork, oldResources, newStatus, newNetwork, newResources string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, old.GenerationID).Scan(&oldStatus, &oldNetwork, &oldResources); err != nil {
		t.Fatalf("query old generation: %v", err)
	}
	if oldStatus != "failed" || oldNetwork != "reclaimable" || oldResources != "reclaimable" {
		t.Fatalf("old generation not fenced/reclaimable: status=%s network=%s resources=%s", oldStatus, oldNetwork, oldResources)
	}
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, gotSession.ActiveGenerationID).Scan(&newStatus, &newNetwork, &newResources); err != nil {
		t.Fatalf("query replacement generation: %v", err)
	}
	if newStatus != "idle" || newNetwork != "live" || newResources != "live" {
		t.Fatalf("replacement generation not live idle: status=%s network=%s resources=%s", newStatus, newNetwork, newResources)
	}
	var queuedTurns int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM turns
WHERE session_id = ?
  AND status = 'queued'
  AND generation_id IS NULL
  AND content = 'after fallback'`, session.ID).Scan(&queuedTurns); err != nil {
		t.Fatalf("count queued turns: %v", err)
	}
	if queuedTurns != 1 {
		t.Fatalf("queued fallback turn count=%d want 1", queuedTurns)
	}
	prepareRequests, startRequests := rt.requests()
	if len(prepareRequests) != 1 || len(startRequests) != 1 {
		t.Fatalf("runtime calls prepare=%d start=%d", len(prepareRequests), len(startRequests))
	}
	if startRequests[0].GenerationID != gotSession.ActiveGenerationID ||
		startRequests[0].ClaudeSessionUUID != session.ClaudeSessionUUID ||
		!startRequests[0].ResumeClaude ||
		startRequests[0].WaitForTurn {
		t.Fatalf("unexpected replacement start request: %+v", startRequests[0])
	}
}

func TestSendMessageRestoresCheckpointedGeneration(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_restore", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	leaseOwner := store.GenerationLeaseOwner(owner.UUID)
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, "claude"),
	})
	if err != nil {
		t.Fatalf("allocate checkpointed generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, allocation.GenerationID, allocation.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("mark checkpointed generation live: %v", err)
	}
	if err := st.RecordGenerationRuntimeArtifacts(ctx, allocation.GenerationID, "restore_manifest_digest", "runsc restore-test"); err != nil {
		t.Fatalf("record checkpointed artifacts: %v", err)
	}
	markServerGenerationCheckpointed(t, ctx, st, session.ID, allocation.GenerationID, time.Now().UTC())

	rt := &recordingRuntime{}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"after restore"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d body %s", rec.Code, rec.Body.String())
	}

	gotSession, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get restored session: %v", err)
	}
	if gotSession.ActiveGenerationID != allocation.GenerationID || gotSession.Status != string(sessionstate.RunningActive) {
		t.Fatalf("unexpected restored session: %+v allocation=%+v", gotSession, allocation)
	}
	var generationStatus, networkState, resourceState string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&generationStatus, &networkState, &resourceState); err != nil {
		t.Fatalf("query restored generation: %v", err)
	}
	if generationStatus != "idle" || networkState != "live" || resourceState != "live" {
		t.Fatalf("restored generation not live idle: status=%s network=%s resources=%s", generationStatus, networkState, resourceState)
	}
	var queuedTurns int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM turns
WHERE session_id = ?
  AND status = 'queued'
  AND generation_id IS NULL
  AND content = 'after restore'`, session.ID).Scan(&queuedTurns); err != nil {
		t.Fatalf("count restored queued turns: %v", err)
	}
	if queuedTurns != 1 {
		t.Fatalf("queued restored turn count=%d want 1", queuedTurns)
	}
	prepareRequests, startRequests := rt.requests()
	if len(prepareRequests) != 0 || len(startRequests) != 1 {
		t.Fatalf("restore should skip prepare and start once: prepare=%d start=%d", len(prepareRequests), len(startRequests))
	}
	start := startRequests[0]
	if start.GenerationID != allocation.GenerationID ||
		!start.RestoreFromCheckpoint ||
		!start.ResumeClaude ||
		start.WaitForTurn ||
		start.PreparedArtifacts.ManifestDigest != "restore_manifest_digest" ||
		start.PreparedArtifacts.RunscVersion != "runsc restore-test" ||
		start.Generation.NetworkAllocationState != "recreating" {
		t.Fatalf("unexpected restore start request: %+v", start)
	}
}

func TestSendMessageFallsBackWhenCheckpointRestoreFails(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_restore_fallback", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	cfg.Phase7.Network.CIDRPool = config.CIDRPrefix{Prefix: netip.MustParsePrefix("10.241.0.0/28")}
	leaseOwner := store.GenerationLeaseOwner(owner.UUID)
	old, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, "claude"),
	})
	if err != nil {
		t.Fatalf("allocate checkpointed generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, old.GenerationID, old.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("mark checkpointed generation live: %v", err)
	}
	if err := st.RecordGenerationRuntimeArtifacts(ctx, old.GenerationID, "restore_manifest_digest", "runsc restore-test"); err != nil {
		t.Fatalf("record checkpointed artifacts: %v", err)
	}
	markServerGenerationCheckpointed(t, ctx, st, session.ID, old.GenerationID, time.Now().UTC())
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE sessions
SET checkpoint_path = ?,
    restore_ms = 123
WHERE id = ?`, filepath.Join(dir, "checkpoints", session.ID), session.ID); err != nil {
		t.Fatalf("seed checkpoint metadata: %v", err)
	}

	rt := &restoreFailoverRuntime{err: errors.New("checkpoint_runsc_version mismatch")}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"after restore fallback"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d body %s", rec.Code, rec.Body.String())
	}
	gotSession, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get fallback session: %v", err)
	}
	if gotSession.ActiveGenerationID == "" || gotSession.ActiveGenerationID == old.GenerationID || gotSession.Status != string(sessionstate.RunningActive) {
		t.Fatalf("fallback did not replace active generation: %+v old=%s", gotSession, old.GenerationID)
	}
	if gotSession.CheckpointPath != "" || gotSession.RestoreMS != nil {
		t.Fatalf("fallback should clear checkpoint metadata: checkpoint=%q restore=%v", gotSession.CheckpointPath, gotSession.RestoreMS)
	}
	var oldStatus, oldNetwork, oldResources, oldErrorClass, newStatus, newNetwork, newResources string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state, COALESCE(g.error_class, '')
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, old.GenerationID).Scan(&oldStatus, &oldNetwork, &oldResources, &oldErrorClass); err != nil {
		t.Fatalf("query old generation: %v", err)
	}
	if oldStatus != "failed" || oldNetwork != "reclaimable" || oldResources != "reclaimable" || oldErrorClass != "runtime_failed" {
		t.Fatalf("old generation not fenced after restore failure: status=%s network=%s resources=%s class=%s", oldStatus, oldNetwork, oldResources, oldErrorClass)
	}
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, gotSession.ActiveGenerationID).Scan(&newStatus, &newNetwork, &newResources); err != nil {
		t.Fatalf("query fallback generation: %v", err)
	}
	if newStatus != "idle" || newNetwork != "live" || newResources != "live" {
		t.Fatalf("fallback generation not live idle: status=%s network=%s resources=%s", newStatus, newNetwork, newResources)
	}
	var queuedTurns int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM turns
WHERE session_id = ?
  AND status = 'queued'
  AND generation_id IS NULL
  AND content = 'after restore fallback'`, session.ID).Scan(&queuedTurns); err != nil {
		t.Fatalf("count fallback queued turns: %v", err)
	}
	if queuedTurns != 1 {
		t.Fatalf("queued fallback turn count=%d want 1", queuedTurns)
	}
	prepareRequests, startRequests := rt.requests()
	if len(prepareRequests) != 1 || len(startRequests) != 2 {
		t.Fatalf("runtime calls prepare=%d start=%d", len(prepareRequests), len(startRequests))
	}
	if !startRequests[0].RestoreFromCheckpoint || startRequests[0].GenerationID != old.GenerationID {
		t.Fatalf("first start was not restore: %+v", startRequests[0])
	}
	if startRequests[1].RestoreFromCheckpoint || startRequests[1].GenerationID != gotSession.ActiveGenerationID {
		t.Fatalf("second start was not cold fallback: %+v", startRequests[1])
	}
	var restoreRetiredEvents, runtimeEvents, terminalEvents int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT
  SUM(CASE WHEN type = 'session.restore_fallback_retired' THEN 1 ELSE 0 END),
  SUM(CASE WHEN type = 'generation.error' THEN 1 ELSE 0 END),
  SUM(CASE WHEN type = 'session.error' THEN 1 ELSE 0 END)
FROM events
WHERE session_id = ?`, session.ID).Scan(&restoreRetiredEvents, &runtimeEvents, &terminalEvents); err != nil {
		t.Fatalf("count restore fallback events: %v", err)
	}
	if restoreRetiredEvents != 1 || runtimeEvents != 1 || terminalEvents != 0 {
		t.Fatalf("unexpected restore fallback events: retired=%d runtime=%d terminal=%d", restoreRetiredEvents, runtimeEvents, terminalEvents)
	}
}

func TestSendMessageRestoreFallbackColdStartFailureLeavesSessionRetryable(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_restore_fallback_start_fail", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	cfg.Phase7.Network.CIDRPool = config.CIDRPrefix{Prefix: netip.MustParsePrefix("10.241.0.0/28")}
	leaseOwner := store.GenerationLeaseOwner(owner.UUID)
	old, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, "claude"),
	})
	if err != nil {
		t.Fatalf("allocate checkpointed generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, old.GenerationID, old.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("mark checkpointed generation live: %v", err)
	}
	if err := st.RecordGenerationRuntimeArtifacts(ctx, old.GenerationID, "restore_manifest_digest", "runsc restore-test"); err != nil {
		t.Fatalf("record checkpointed artifacts: %v", err)
	}
	markServerGenerationCheckpointed(t, ctx, st, session.ID, old.GenerationID, time.Now().UTC())
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE sessions
SET checkpoint_path = ?,
    restore_ms = 456
WHERE id = ?`, filepath.Join(dir, "checkpoints", session.ID), session.ID); err != nil {
		t.Fatalf("seed checkpoint metadata: %v", err)
	}

	rt := &restoreFailoverRuntime{
		err:     errors.New("checkpoint_runsc_version mismatch"),
		coldErr: errors.New("pre-start sandbox network probe failed"),
	}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"after restore fallback start fail"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d body %s", rec.Code, rec.Body.String())
	}
	gotSession, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get fallback session: %v", err)
	}
	if gotSession.Status != string(sessionstate.RunningIdle) || gotSession.CheckpointPath != "" || gotSession.RestoreMS != nil {
		t.Fatalf("session should stay non-checkpointed and retryable after fallback start failure: %+v", gotSession)
	}
	if gotSession.ActiveGenerationID == "" || gotSession.ActiveGenerationID == old.GenerationID {
		t.Fatalf("expected replacement generation to remain active after failed cold fallback: %+v old=%s", gotSession, old.GenerationID)
	}
	var oldStatus, oldNetwork, oldResources, newStatus, newNetwork, newResources string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, old.GenerationID).Scan(&oldStatus, &oldNetwork, &oldResources); err != nil {
		t.Fatalf("query old generation: %v", err)
	}
	if oldStatus != "failed" || oldNetwork != "reclaimable" || oldResources != "reclaimable" {
		t.Fatalf("old generation not reclaimable after restore fallback: status=%s network=%s resources=%s", oldStatus, oldNetwork, oldResources)
	}
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, gotSession.ActiveGenerationID).Scan(&newStatus, &newNetwork, &newResources); err != nil {
		t.Fatalf("query replacement generation: %v", err)
	}
	if newStatus != "failed" || newNetwork != "reclaimable" || newResources != "reclaimable" {
		t.Fatalf("replacement generation should be failed/reclaimable after cold fallback failure: status=%s network=%s resources=%s", newStatus, newNetwork, newResources)
	}
	var queuedTurns int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM turns
WHERE session_id = ?`, session.ID).Scan(&queuedTurns); err != nil {
		t.Fatalf("count turns: %v", err)
	}
	if queuedTurns != 0 {
		t.Fatalf("turn should not be enqueued when cold fallback start fails, got %d", queuedTurns)
	}
}

func TestSendMessageRestoreLiveCASFailureDestroysRestoreIDBeforeFallback(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_restore_live_cas", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	cfg.Phase7.Network.CIDRPool = config.CIDRPrefix{Prefix: netip.MustParsePrefix("10.241.0.0/28")}
	leaseOwner := store.GenerationLeaseOwner(owner.UUID)
	old, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, "claude"),
	})
	if err != nil {
		t.Fatalf("allocate checkpointed generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, old.GenerationID, old.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("mark checkpointed generation live: %v", err)
	}
	if err := st.RecordGenerationRuntimeArtifacts(ctx, old.GenerationID, "restore_manifest_digest", "runsc restore-test"); err != nil {
		t.Fatalf("record checkpointed artifacts: %v", err)
	}
	markServerGenerationCheckpointed(t, ctx, st, session.ID, old.GenerationID, time.Now().UTC())

	rt := &restoreStartHookRuntime{
		onRestoreStart: func() {
			if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE network_profiles
SET allocation_state = 'reserved_checkpointed'
WHERE generation_id = ?`, old.GenerationID); err != nil {
				t.Fatalf("force restore live CAS failure: %v", err)
			}
		},
	}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"after restore live cas"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d body %s", rec.Code, rec.Body.String())
	}
	destroyIDs := rt.runtimeDestroyRequests()
	if len(destroyIDs) != 1 || destroyIDs[0] != session.RestoreID {
		t.Fatalf("restore live CAS cleanup should destroy restore id %q, got %+v", session.RestoreID, destroyIDs)
	}
	if destroyIDs[0] == session.ID {
		t.Fatalf("restore live CAS cleanup used bare session id %q", session.ID)
	}
	gotSession, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get fallback session: %v", err)
	}
	if gotSession.ActiveGenerationID == old.GenerationID || gotSession.Status != string(sessionstate.RunningActive) {
		t.Fatalf("fallback did not allocate replacement after restore live CAS failure: %+v old=%s", gotSession, old.GenerationID)
	}
	var oldStatus, oldNetwork, oldResources string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, old.GenerationID).Scan(&oldStatus, &oldNetwork, &oldResources); err != nil {
		t.Fatalf("query old generation: %v", err)
	}
	if oldStatus != "failed" || oldNetwork != "reclaimable" || oldResources != "reclaimable" {
		t.Fatalf("old generation not reclaimable after restore live CAS fallback: status=%s network=%s resources=%s", oldStatus, oldNetwork, oldResources)
	}
}

func TestSendMessageRestoreLiveCASFailureDoesNotRetireWhenDestroyFails(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_restore_destroy_fail", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	cfg.Phase7.Network.CIDRPool = config.CIDRPrefix{Prefix: netip.MustParsePrefix("10.241.0.0/28")}
	leaseOwner := store.GenerationLeaseOwner(owner.UUID)
	old, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, "claude"),
	})
	if err != nil {
		t.Fatalf("allocate checkpointed generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, old.GenerationID, old.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("mark checkpointed generation live: %v", err)
	}
	if err := st.RecordGenerationRuntimeArtifacts(ctx, old.GenerationID, "restore_manifest_digest", "runsc restore-test"); err != nil {
		t.Fatalf("record checkpointed artifacts: %v", err)
	}
	markServerGenerationCheckpointed(t, ctx, st, session.ID, old.GenerationID, time.Now().UTC())

	rt := &restoreStartHookRuntime{
		recordingRuntime: recordingRuntime{destroyRuntimeErr: errors.New("destroy failed")},
		onRestoreStart: func() {
			if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE network_profiles
SET allocation_state = 'reserved_checkpointed'
WHERE generation_id = ?`, old.GenerationID); err != nil {
				t.Fatalf("force restore live CAS failure: %v", err)
			}
		},
	}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"after restore destroy fail"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d body %s", rec.Code, rec.Body.String())
	}
	destroyIDs := rt.runtimeDestroyRequests()
	if len(destroyIDs) != 1 || destroyIDs[0] != session.RestoreID {
		t.Fatalf("restore cleanup should target restore id %q before failing, got %+v", session.RestoreID, destroyIDs)
	}
	var oldStatus, oldNetwork, oldResources string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, old.GenerationID).Scan(&oldStatus, &oldNetwork, &oldResources); err != nil {
		t.Fatalf("query old generation: %v", err)
	}
	if oldStatus == "failed" || oldNetwork == "reclaimable" || oldResources == "reclaimable" {
		t.Fatalf("restore generation should not be retired when runtime destroy fails: status=%s network=%s resources=%s", oldStatus, oldNetwork, oldResources)
	}
	var retirementEvents int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM events
WHERE session_id = ?
  AND type IN ('session.restore_fallback_retired', 'generation.error')`, session.ID).Scan(&retirementEvents); err != nil {
		t.Fatalf("count restore fallback events: %v", err)
	}
	if retirementEvents != 0 {
		t.Fatalf("restore fallback events should not be committed when destroy fails, got %d", retirementEvents)
	}
}

func TestSendMessageFallsBackWhenCheckpointImageManifestInvalid(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_restore_manifest_fallback", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	if _, err := st.DBForTest().ExecContext(ctx, `UPDATE sessions SET agent = 'sh' WHERE id = ?`, session.ID); err != nil {
		t.Fatalf("set shell session agent: %v", err)
	}
	session.Agent = "sh"
	cfg := testServerConfig(dir)
	cfg.Phase7.Network.CIDRPool = config.CIDRPrefix{Prefix: netip.MustParsePrefix("10.241.0.0/28")}
	leaseOwner := store.GenerationLeaseOwner(owner.UUID)
	old, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, "sh"),
	})
	if err != nil {
		t.Fatalf("allocate checkpointed generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, old.GenerationID, old.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("mark checkpointed generation live: %v", err)
	}

	checkpointPath := filepath.Join(dir, "checkpoints", session.ID)
	writeServerCheckpointFilesWithoutManifest(t, checkpointPath)
	manifest, err := buildServerCheckpointImageManifest(checkpointPath)
	if err != nil {
		t.Fatalf("build checkpoint image manifest: %v", err)
	}
	if err := writeServerJSONFile(filepath.Join(checkpointPath, "harness-checkpoint-manifest.json"), manifest); err != nil {
		t.Fatalf("write checkpoint image manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkpointPath, "pages.img"), []byte("corrupt"), 0o644); err != nil {
		t.Fatalf("corrupt checkpoint image file: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generation_resources
SET checkpoint_path = ?
WHERE generation_id = ?`, checkpointPath, old.GenerationID); err != nil {
		t.Fatalf("record checkpoint path: %v", err)
	}
	markServerGenerationCheckpointed(t, ctx, st, session.ID, old.GenerationID, time.Now().UTC())

	realRuntime := runtime.New(runtime.Config{
		DefaultAgent:    "sh",
		SessionsRoot:    cfg.SessionsRoot,
		AgentHomesRoot:  filepath.Join(dir, "agent-homes"),
		CheckpointsRoot: filepath.Join(dir, "checkpoints"),
		RootFSPath:      filepath.Join(dir, "rootfs"),
		BundleRoot:      filepath.Join(dir, "run", "runtime"),
		RunscNetwork:    "host",
		RunscOverlay2:   "none",
		Phase7RunDir:    cfg.Phase7.RunDir,
		CommandRunner:   serverCommandRunner{outputs: map[string][]byte{"runsc --version": []byte("runsc test")}},
		BridgeMode:      "claim-loop",
		BridgeHeartbeat: time.Second,
		SandboxUID:      cfg.Phase7.SandboxIdentity.UID,
		SandboxGID:      cfg.Phase7.SandboxIdentity.GID,
	})
	rt := &restoreValidationRuntime{restore: realRuntime}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"after corrupt checkpoint"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d body %s", rec.Code, rec.Body.String())
	}
	gotSession, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get fallback session: %v", err)
	}
	if gotSession.ActiveGenerationID == "" || gotSession.ActiveGenerationID == old.GenerationID || gotSession.Status != string(sessionstate.RunningActive) {
		t.Fatalf("fallback did not replace active generation: %+v old=%s", gotSession, old.GenerationID)
	}
	var oldStatus, oldNetwork, oldResources, oldReason string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state, COALESCE(g.failure_reason, '')
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, old.GenerationID).Scan(&oldStatus, &oldNetwork, &oldResources, &oldReason); err != nil {
		t.Fatalf("query old generation: %v", err)
	}
	if oldStatus != "failed" || oldNetwork != "reclaimable" || oldResources != "reclaimable" {
		t.Fatalf("old generation not fenced after invalid checkpoint manifest: status=%s network=%s resources=%s reason=%s", oldStatus, oldNetwork, oldResources, oldReason)
	}
	if !strings.Contains(oldReason, "checkpoint image manifest") || !strings.Contains(oldReason, "pages.img") {
		t.Fatalf("old generation failure reason did not include checkpoint manifest mismatch: %q", oldReason)
	}
	var newStatus, newNetwork, newResources string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, gotSession.ActiveGenerationID).Scan(&newStatus, &newNetwork, &newResources); err != nil {
		t.Fatalf("query fallback generation: %v", err)
	}
	if newStatus != "idle" || newNetwork != "live" || newResources != "live" {
		t.Fatalf("fallback generation not live idle: status=%s network=%s resources=%s", newStatus, newNetwork, newResources)
	}
	var queuedTurns int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM turns
WHERE session_id = ?
  AND status = 'queued'
  AND generation_id IS NULL
  AND content = 'after corrupt checkpoint'`, session.ID).Scan(&queuedTurns); err != nil {
		t.Fatalf("count fallback queued turns: %v", err)
	}
	if queuedTurns != 1 {
		t.Fatalf("queued fallback turn count=%d want 1", queuedTurns)
	}
	if got := len(rt.startRequests); got != 2 {
		t.Fatalf("runtime calls start=%d want 2", got)
	}
	if !rt.startRequests[0].RestoreFromCheckpoint || rt.startRequests[0].GenerationID != old.GenerationID {
		t.Fatalf("first start was not restore: %+v", rt.startRequests[0])
	}
	if rt.startRequests[1].RestoreFromCheckpoint || rt.startRequests[1].GenerationID != gotSession.ActiveGenerationID {
		t.Fatalf("second start was not cold fallback: %+v", rt.startRequests[1])
	}
}

func TestColdFallbackMaintenanceStartsReplacementForQueuedTurn(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_maintenance_fallback", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	cfg.Phase7.Network.CIDRPool = config.CIDRPrefix{Prefix: netip.MustParsePrefix("10.241.0.0/28")}
	leaseOwner := store.GenerationLeaseOwner(owner.UUID)
	old, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, "claude"),
	})
	if err != nil {
		t.Fatalf("allocate old generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, old.GenerationID, old.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("mark old generation live: %v", err)
	}
	if err := st.FailGeneration(ctx, store.FailGenerationParams{
		SessionID:    session.ID,
		GenerationID: old.GenerationID,
		Owner:        old.Owner,
		ErrorClass:   "orchestrator_restart_reconnect_grace_expired",
		Reason:       "orchestrator_restart_reconnect_grace_expired",
		Now:          time.Now().UTC(),
	}); err != nil {
		t.Fatalf("fail old generation: %v", err)
	}
	turnID, err := st.EnqueueTurn(ctx, session.ID, "retry old queued turn", time.Now().UTC())
	if err != nil {
		t.Fatalf("enqueue queued turn: %v", err)
	}

	rt := &recordingRuntime{}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)
	srv.startColdFallbackSessions(ctx, leaseOwner)

	gotSession, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if gotSession.Status != string(sessionstate.RunningActive) ||
		gotSession.ActiveGenerationID == "" ||
		gotSession.ActiveGenerationID == old.GenerationID {
		t.Fatalf("unexpected fallback session state: %+v old=%s", gotSession, old.GenerationID)
	}
	_, startRequests := rt.requests()
	if len(startRequests) != 1 ||
		startRequests[0].GenerationID != gotSession.ActiveGenerationID ||
		startRequests[0].ClaudeSessionUUID != session.ClaudeSessionUUID ||
		!startRequests[0].ResumeClaude {
		t.Fatalf("unexpected maintenance start requests: %+v", startRequests)
	}
	grant, ok, err := st.ClaimNextTurn(ctx, store.ClaimNextTurnParams{
		SessionID:    session.ID,
		GenerationID: gotSession.ActiveGenerationID,
		Owner:        leaseOwner,
		RequestID:    "req_fallback_claim",
		LeaseTTL:     time.Minute,
		Now:          time.Now().UTC(),
	})
	if err != nil || !ok {
		t.Fatalf("claim queued fallback turn: ok=%v grant=%+v err=%v", ok, grant, err)
	}
	if grant.TurnID != turnID || grant.Content != "retry old queued turn" {
		t.Fatalf("claimed wrong fallback turn: %+v want turn %d", grant, turnID)
	}
}

func TestColdFallbackMaintenanceStartFailureKeepsSessionInputBlocking(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_maintenance_fallback_fail", string(sessionstate.RunningActive), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	cfg.Phase7.Network.CIDRPool = config.CIDRPrefix{Prefix: netip.MustParsePrefix("10.241.0.0/28")}
	leaseOwner := store.GenerationLeaseOwner(owner.UUID)
	old, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, "claude"),
	})
	if err != nil {
		t.Fatalf("allocate old generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, old.GenerationID, old.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("mark old generation live: %v", err)
	}
	if err := st.FailGeneration(ctx, store.FailGenerationParams{
		SessionID:    session.ID,
		GenerationID: old.GenerationID,
		Owner:        old.Owner,
		ErrorClass:   "orchestrator_restart_reconnect_grace_expired",
		Reason:       "orchestrator_restart_reconnect_grace_expired",
		Now:          time.Now().UTC(),
	}); err != nil {
		t.Fatalf("fail old generation: %v", err)
	}
	turnID, err := st.EnqueueTurn(ctx, session.ID, "protected queued turn", time.Now().UTC())
	if err != nil {
		t.Fatalf("enqueue queued turn: %v", err)
	}

	hub := events.NewHub()
	eventsCh, cancelEvents := hub.Subscribe(session.ID)
	defer cancelEvents()
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: failingRuntime{err: errors.New("pre-start sandbox network probe failed")},
		hub:     hub,
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)
	srv.startColdFallbackSessions(ctx, leaseOwner)

	gotSession, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if gotSession.Status != string(sessionstate.RunningActive) ||
		sessionstate.CanAcceptInput(gotSession.Status) ||
		gotSession.ActiveGenerationID == "" ||
		gotSession.ActiveGenerationID == old.GenerationID {
		t.Fatalf("unexpected fallback-failure session state: %+v old=%s", gotSession, old.GenerationID)
	}
	var generationStatus, errorClass, networkState, resourceState string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, COALESCE(g.error_class, ''), n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, gotSession.ActiveGenerationID).Scan(&generationStatus, &errorClass, &networkState, &resourceState); err != nil {
		t.Fatalf("query failed fallback generation: %v", err)
	}
	if generationStatus != "failed" ||
		errorClass != "probe_failed_pre_start" ||
		networkState != "reclaimable" ||
		resourceState != "reclaimable" {
		t.Fatalf("unexpected failed fallback generation: status=%s class=%s network=%s resource=%s", generationStatus, errorClass, networkState, resourceState)
	}
	var queuedStatus string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT status
FROM turns
WHERE id = ?`, turnID).Scan(&queuedStatus); err != nil {
		t.Fatalf("query queued turn: %v", err)
	}
	if queuedStatus != "queued" {
		t.Fatalf("queued turn status=%s want queued", queuedStatus)
	}
	seenGenerationError := false
	for {
		select {
		case event := <-eventsCh:
			switch event.Type {
			case "generation.error":
				seenGenerationError = true
			case "session." + string(sessionstate.RunningIdle), "session." + string(sessionstate.Failed), "session.error":
				t.Fatalf("unexpected terminal/input-acceptable event after fallback failure: %+v", event)
			}
		default:
			if !seenGenerationError {
				t.Fatalf("missing generation.error event")
			}
			return
		}
	}
}

func TestRunPhase7MaintenanceRetiresExpiredCheckpoint(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_retire_checkpoint", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	cfg.Phase7.Reaper.CheckpointImageRetention = config.Duration{Duration: 0}
	cfg.Phase7.Reaper.FailedRetention = config.Duration{Duration: time.Hour}
	allocation := prepareServerIdleGeneration(t, ctx, st, cfg, owner.UUID, session.ID)
	checkpointedAt := time.Now().UTC().Add(-2 * time.Hour)
	markServerGenerationCheckpointed(t, ctx, st, session.ID, allocation.GenerationID, checkpointedAt)
	checkpointPath := filepath.Join(dir, "checkpoint", session.ID)
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE sessions
SET checkpoint_path = ?,
    restore_ms = 99,
    last_activity_at = ?
WHERE id = ?`, checkpointPath, checkpointedAt.Format(time.RFC3339Nano), session.ID); err != nil {
		t.Fatalf("seed checkpoint session metadata: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generation_resources
SET checkpoint_path = ?
WHERE generation_id = ?`, checkpointPath, allocation.GenerationID); err != nil {
		t.Fatalf("seed checkpoint resource path: %v", err)
	}

	hub := events.NewHub()
	eventsCh, cancelEvents := hub.Subscribe(session.ID)
	defer cancelEvents()
	rt := &recordingRuntime{}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, hub),
		hub:     hub,
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- srv.RunPhase7Maintenance(runCtx)
	}()
	event := waitForHubEvent(t, eventsCh, "session.checkpoint_retired")
	waitForGenerationResourceStates(t, runCtx, st, allocation.GenerationID, "destroyed", "destroyed")
	cancel()
	err := <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("maintenance exit err=%v, want context canceled", err)
	}
	payload, ok := event.Payload.(json.RawMessage)
	if !ok || strings.Contains(string(payload), `"checkpoint_path"`) || !strings.Contains(string(payload), `"restore_ms":null`) {
		t.Fatalf("unexpected checkpoint retirement event payload: %#v", event.Payload)
	}

	gotSession, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get retired session: %v", err)
	}
	if gotSession.Status != string(sessionstate.RunningIdle) || gotSession.CheckpointPath != "" || gotSession.RestoreMS != nil {
		t.Fatalf("unexpected retired session: %+v", gotSession)
	}
	var generationStatus, generationError, networkState, resourceState string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, COALESCE(g.error_class, ''), n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&generationStatus, &generationError, &networkState, &resourceState); err != nil {
		t.Fatalf("query retired generation: %v", err)
	}
	if generationStatus != "failed" || generationError != "checkpoint_retired" || networkState != "destroyed" || resourceState != "destroyed" {
		t.Fatalf("unexpected retired generation: status=%s error=%s network=%s resource=%s", generationStatus, generationError, networkState, resourceState)
	}
	destroyRequests := rt.destroyGenerationRequests()
	if len(destroyRequests) != 1 || destroyRequests[0].GenerationID != allocation.GenerationID {
		t.Fatalf("unexpected destroy generation requests: %+v", destroyRequests)
	}
}

func TestEnsureActiveGenerationColdStartsAfterCheckpointRetirement(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_retire_then_send", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	cfg.Phase7.Network.CIDRPool = config.CIDRPrefix{Prefix: netip.MustParsePrefix("10.241.0.0/28")}
	allocation := prepareServerIdleGeneration(t, ctx, st, cfg, owner.UUID, session.ID)
	checkpointedAt := time.Now().UTC().Add(-2 * time.Hour)
	markServerGenerationCheckpointed(t, ctx, st, session.ID, allocation.GenerationID, checkpointedAt)
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE sessions
SET checkpoint_path = ?,
    last_activity_at = ?
WHERE id = ?`, filepath.Join(dir, "checkpoint", session.ID), checkpointedAt.Format(time.RFC3339Nano), session.ID); err != nil {
		t.Fatalf("seed checkpoint session metadata: %v", err)
	}
	staleCheckpointedSession, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get checkpointed session: %v", err)
	}
	if _, err := st.RetireExpiredCheckpoints(ctx, store.RetireExpiredCheckpointsParams{
		OwnerUUID:                owner.UUID,
		Now:                      time.Now().UTC(),
		CheckpointImageRetention: time.Hour,
	}); err != nil {
		t.Fatalf("retire checkpoint: %v", err)
	}

	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: &recordingRuntime{},
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	ensured, err := srv.ensureActiveGeneration(ctx, staleCheckpointedSession, store.GenerationLeaseOwner(owner.UUID))
	if err != nil {
		t.Fatalf("ensure active generation after retirement: %v", err)
	}
	if !ensured.IsNew || ensured.RestoreFromCheckpoint || ensured.Allocation.GenerationID == allocation.GenerationID {
		t.Fatalf("ensure should cold-start replacement after checkpoint retirement: %+v old=%s", ensured, allocation.GenerationID)
	}
	gotSession, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get replacement session: %v", err)
	}
	if gotSession.ActiveGenerationID != ensured.Allocation.GenerationID {
		t.Fatalf("session active generation=%s want replacement %s", gotSession.ActiveGenerationID, ensured.Allocation.GenerationID)
	}
}

func TestGetQuotaReportsSessionAndPoolCeilings(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	createServerTestSession(t, ctx, st, dir, "sess_quota", string(sessionstate.Created), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	cfg.MaxSessions = 3
	cfg.Phase7.MaxSessions = 3
	cfg.Phase7.Network.CIDRPool = config.CIDRPrefix{Prefix: netip.MustParsePrefix("10.242.0.0/29")}
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: "sess_quota",
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config: store.ResourceAllocatorConfig{
			RunDir:             cfg.Phase7.RunDir,
			CIDRPool:           cfg.Phase7.Network.CIDRPool.Prefix,
			EgressDorisFEHosts: cfg.Phase7.Network.Egress.DorisFEHosts,
			EgressDorisBEHosts: cfg.Phase7.Network.Egress.DorisBEHosts,
			EgressDorisPorts:   cfg.Phase7.Network.Egress.DorisPorts,
			EgressDNSPolicy:    string(cfg.Phase7.Network.Egress.DNSPolicy),
			HostProxyBindURL:   cfg.Claude.ProxyBindURL,
			ProxyPort:          8082,
			Agent:              "claude",
			AgentModel:         cfg.Claude.Model,
			AgentOutputFormat:  cfg.Claude.OutputFormat,
		},
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}

	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: instantRuntime{},
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	req := httptest.NewRequest(http.MethodGet, "/api/quota", nil)
	rec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d body %s", rec.Code, rec.Body.String())
	}
	var body map[string]int
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode quota: %v", err)
	}
	if body["soft_session_ceiling"] != 3 ||
		body["active_sessions"] != 1 ||
		body["live_pool_ceiling"] != 2 ||
		body["allocated_pool_slots"] != 1 ||
		body["remaining_pool_slots"] != 1 {
		t.Fatalf("unexpected quota body for allocation %s: %+v", allocation.GenerationID, body)
	}
	if _, ok := body["effective_ceiling"]; ok {
		t.Fatalf("quota should report session and pool ceilings separately without effective_ceiling: %+v", body)
	}
}

func TestSendMessagePoolExhaustionDoesNotQueueTurn(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	cfg := testServerConfig(dir)
	cfg.Phase7.Network.CIDRPool = config.CIDRPrefix{Prefix: netip.MustParsePrefix("10.242.0.0/30")}
	createServerTestSession(t, ctx, st, dir, "sess_pool_used", string(sessionstate.Created), time.Now().UTC(), nil)
	target := createServerTestSession(t, ctx, st, dir, "sess_pool_target", string(sessionstate.Created), time.Now().UTC(), nil)
	if _, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: "sess_pool_used",
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, "claude"),
	}); err != nil {
		t.Fatalf("allocate pool slot: %v", err)
	}

	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: instantRuntime{},
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+target.ID+"/messages", strings.NewReader(`{"content":"hello"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, target.ID)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d body %s", rec.Code, rec.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["error_class"] != "pool_exhausted" {
		t.Fatalf("expected pool_exhausted, got %v", body)
	}
	var targetGenerations, targetTurns int
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_generations WHERE session_id = ?`, target.ID).Scan(&targetGenerations); err != nil {
		t.Fatalf("count target generations: %v", err)
	}
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM turns WHERE session_id = ?`, target.ID).Scan(&targetTurns); err != nil {
		t.Fatalf("count target turns: %v", err)
	}
	if targetGenerations != 0 || targetTurns != 0 {
		t.Fatalf("pool exhaustion leaked target state: generations=%d turns=%d", targetGenerations, targetTurns)
	}
}

func TestSendMessageRuntimeStartFailureMarksGenerationFailedAndReclaimable(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_runtime_fail", string(sessionstate.Created), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	srv := &Server{
		cfg:   cfg,
		store: st,
		runtime: failingRuntime{
			err: errors.New("pre-start sandbox network probe failed"),
		},
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"hello"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d body %s", rec.Code, rec.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["error_class"] != "probe_failed_pre_start" ||
		body["error"] != "sandbox network probe failed before start" {
		t.Fatalf("unexpected response body: %v", body)
	}
	var generationStatus, errorClass, networkState, resourceState, sessionStatus, sessionErrorClass, sessionFailureReason string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, COALESCE(g.error_class, ''), n.allocation_state, r.resource_state, s.status,
       COALESCE(s.error_class, ''), COALESCE(s.failure_reason, '')
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
JOIN sessions s ON s.active_generation_id = g.generation_id
WHERE g.session_id = ?`, session.ID).Scan(
		&generationStatus,
		&errorClass,
		&networkState,
		&resourceState,
		&sessionStatus,
		&sessionErrorClass,
		&sessionFailureReason,
	); err != nil {
		t.Fatalf("query generation state: %v", err)
	}
	if generationStatus != "failed" ||
		errorClass != "probe_failed_pre_start" ||
		networkState != "reclaimable" ||
		resourceState != "reclaimable" ||
		sessionStatus != string(sessionstate.Created) ||
		sessionErrorClass != "" ||
		sessionFailureReason != "" {
		t.Fatalf("unexpected failed generation state: generation=%s class=%s network=%s resource=%s session=%s session_class=%s session_reason=%s", generationStatus, errorClass, networkState, resourceState, sessionStatus, sessionErrorClass, sessionFailureReason)
	}
	if !sessionstate.CanAcceptInput(sessionStatus) {
		t.Fatalf("session should remain input-acceptable after start failure, got %s", sessionStatus)
	}
	var runtimeEvents int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM events
WHERE session_id = ?
  AND type = 'generation.error'`, session.ID).Scan(&runtimeEvents); err != nil {
		t.Fatalf("count generation error events: %v", err)
	}
	if runtimeEvents != 1 {
		t.Fatalf("expected one generation.error event, got %d", runtimeEvents)
	}
	var turns int
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM turns WHERE session_id = ?`, session.ID).Scan(&turns); err != nil {
		t.Fatalf("count turns: %v", err)
	}
	if turns != 0 {
		t.Fatalf("runtime start failure should happen before turn creation, got %d turns", turns)
	}

	srv.runtime = instantRuntime{}
	retryReq := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"retry"}`))
	retryRec := httptest.NewRecorder()
	srv.sendMessage(retryRec, retryReq, session.ID)
	if retryRec.Code != http.StatusAccepted {
		t.Fatalf("expected retry status 202, got %d body %s", retryRec.Code, retryRec.Body.String())
	}
	var generationCount int
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_generations WHERE session_id = ?`, session.ID).Scan(&generationCount); err != nil {
		t.Fatalf("count generations after retry: %v", err)
	}
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM turns WHERE session_id = ?`, session.ID).Scan(&turns); err != nil {
		t.Fatalf("count turns after retry: %v", err)
	}
	if generationCount != 2 || turns != 1 {
		t.Fatalf("retry should allocate generation N+1 and enqueue one turn, generations=%d turns=%d", generationCount, turns)
	}
}

func TestStartEnsuredGenerationRenewsLeaseDuringSlowPrepare(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_slow_start", string(sessionstate.Created), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	cfg.Phase7.Bridge.LeaseTTL = config.Duration{Duration: 40 * time.Millisecond}
	now := time.Now().UTC()
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  cfg.Phase7.Bridge.LeaseTTL.Duration,
		Now:       now,
		Config:    serverTestAllocatorConfig(cfg, session.Agent),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	rt := newBlockingPrepareRuntime()
	t.Cleanup(rt.release)
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.startEnsuredGeneration(ctx, session, ensuredGeneration{
			Allocation: allocation,
			IsNew:      true,
		}, startFailureInputAcceptable)
	}()

	select {
	case <-rt.prepareStarted:
	case <-time.After(time.Second):
		t.Fatalf("prepare did not start")
	}
	waitForGenerationLeaseAfter(t, ctx, st, allocation.GenerationID, allocation.LeaseExpiresAt)
	rt.release()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("start ensured generation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("start ensured generation did not finish")
	}
	waitForGenerationStatus(t, ctx, st, allocation.GenerationID, "idle")
}

func TestStartEnsuredGenerationDestroysRuntimeAfterOwnerLoss(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_start_owner_loss", string(sessionstate.Created), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, session.Agent),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	rt := &startHookRuntime{
		onStart: func(req runtime.StartRequest) {
			if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generations
SET lease_owner = 'other_owner',
    lease_expires_at = ?
WHERE generation_id = ?`, time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano), req.GenerationID); err != nil {
				t.Fatalf("steal generation lease: %v", err)
			}
		},
	}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}

	err = srv.startEnsuredGeneration(ctx, session, ensuredGeneration{
		Allocation: allocation,
		IsNew:      true,
	}, startFailureInputAcceptable)
	if !errors.Is(err, errGenerationStartLeaseLost) {
		t.Fatalf("expected start lease loss, got %v", err)
	}
	if got := rt.runtimeDestroyRequests(); len(got) != 1 || got[0] != session.RestoreID {
		t.Fatalf("owner loss should destroy started runtime %q, got %+v", session.RestoreID, got)
	}
	var status, ownerValue, errorClass, networkState, resourceState string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, COALESCE(g.lease_owner, ''), COALESCE(g.error_class, ''), n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&status, &ownerValue, &errorClass, &networkState, &resourceState); err != nil {
		t.Fatalf("query generation after owner loss: %v", err)
	}
	if status != "idle" ||
		ownerValue != "other_owner" ||
		errorClass != "" ||
		networkState != "live" ||
		resourceState != "live" {
		t.Fatalf("owner loss should not fail or reclaim the stolen generation: status=%s owner=%q class=%q network=%s resource=%s", status, ownerValue, errorClass, networkState, resourceState)
	}
	var runtimeEvents int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM events
WHERE session_id = ?
  AND type = 'generation.error'`, session.ID).Scan(&runtimeEvents); err != nil {
		t.Fatalf("count generation events: %v", err)
	}
	if runtimeEvents != 0 {
		t.Fatalf("owner loss should not publish generation error events, got %d", runtimeEvents)
	}
}

func TestRuntimeFailureClassDetectsPostStartProbeFailure(t *testing.T) {
	cases := []string{
		"harness-bridge-client probe exited with status 1",
		"bridge probe starting failed",
		"probe GET /healthz returned 503, want one of [200]",
		"probe POST /v1/messages returned 502, want one of [400]",
	}
	for _, message := range cases {
		if got := runtimeFailureClass(message); got != "probe_failed_post_start" {
			t.Fatalf("runtimeFailureClass(%q)=%s want probe_failed_post_start", message, got)
		}
	}
}

func TestRuntimeFailureClassDetectsManifestFailures(t *testing.T) {
	cases := []struct {
		message string
		want    string
	}{
		{"shell_secret_disallowed", "shell_secret_disallowed"},
		{"runsc run: exit status 1: control manifest digest mismatch", "manifest_digest_mismatch"},
		{"runsc run: exit status 1: expected session_id=sess_a got sess_b", "manifest_digest_mismatch"},
		{"runsc run: exit status 1: expected generation_id=gen_a got gen_b", "manifest_digest_mismatch"},
		{"runsc run: exit status 1: expected network_profile_id=net_a got net_b", "manifest_digest_mismatch"},
		{"runsc run: exit status 1: expected agent_runtime_profile_id=arp_a got arp_b", "manifest_digest_mismatch"},
		{"runsc run: exit status 1: expected anthropic_api_key_secret_id=anthropic_api_key got other", "manifest_digest_mismatch"},
		{"runsc run: exit status 1: expected anthropic_auth_token_secret_id=anthropic_auth_token got other", "manifest_digest_mismatch"},
		{"runsc run: exit status 1: expected manifest_version=1 got 2", "manifest_digest_mismatch"},
		{"runsc run: exit status 1: expected secret_version=local got rotated", "manifest_digest_mismatch"},
		{"runsc run: exit status 1: secret mount /harness-secrets missing", "manifest_digest_mismatch"},
	}
	for _, tc := range cases {
		if got := runtimeFailureClass(tc.message); got != tc.want {
			t.Fatalf("runtimeFailureClass(%q)=%s want %s", tc.message, got, tc.want)
		}
	}
}

func TestSendMessagePrepareFailureMarksGenerationFailedAndReclaimable(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_prepare_fail", string(sessionstate.Created), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	srv := &Server{
		cfg:   cfg,
		store: st,
		runtime: failingRuntime{
			prepareErr: errors.New("pre-start sandbox network probe failed"),
		},
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"hello"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d body %s", rec.Code, rec.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["error_class"] != "probe_failed_pre_start" ||
		body["error"] != "sandbox network probe failed before start" {
		t.Fatalf("unexpected response body: %v", body)
	}
	var generationStatus, errorClass, networkState, resourceState, sessionStatus, sessionErrorClass, sessionFailureReason string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, COALESCE(g.error_class, ''), n.allocation_state, r.resource_state, s.status,
       COALESCE(s.error_class, ''), COALESCE(s.failure_reason, '')
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
JOIN sessions s ON s.active_generation_id = g.generation_id
WHERE g.session_id = ?`, session.ID).Scan(
		&generationStatus,
		&errorClass,
		&networkState,
		&resourceState,
		&sessionStatus,
		&sessionErrorClass,
		&sessionFailureReason,
	); err != nil {
		t.Fatalf("query generation state: %v", err)
	}
	if generationStatus != "failed" ||
		errorClass != "probe_failed_pre_start" ||
		networkState != "reclaimable" ||
		resourceState != "reclaimable" ||
		sessionStatus != string(sessionstate.Created) ||
		sessionErrorClass != "" ||
		sessionFailureReason != "" {
		t.Fatalf("unexpected failed generation state: generation=%s class=%s network=%s resource=%s session=%s session_class=%s session_reason=%s", generationStatus, errorClass, networkState, resourceState, sessionStatus, sessionErrorClass, sessionFailureReason)
	}
	if !sessionstate.CanAcceptInput(sessionStatus) {
		t.Fatalf("session should remain input-acceptable after prepare failure, got %s", sessionStatus)
	}
	var runtimeEvents int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM events
WHERE session_id = ?
  AND type = 'generation.error'`, session.ID).Scan(&runtimeEvents); err != nil {
		t.Fatalf("count generation error events: %v", err)
	}
	if runtimeEvents != 1 {
		t.Fatalf("expected one generation.error event, got %d", runtimeEvents)
	}
	var turns int
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM turns WHERE session_id = ?`, session.ID).Scan(&turns); err != nil {
		t.Fatalf("count turns: %v", err)
	}
	if turns != 0 {
		t.Fatalf("prepare failure should happen before turn creation, got %d turns", turns)
	}
}

func TestDestroySessionCancelsPendingTurnAndReclaimsGeneration(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_destroy_pending", string(sessionstate.Created), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, session.Agent),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, allocation.GenerationID, allocation.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("mark generation live: %v", err)
	}
	enqueued, err := st.EnqueueTurnMessage(ctx, store.EnqueueTurnMessageParams{
		SessionID: session.ID,
		Content:   "hello",
		Now:       time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("enqueue turn: %v", err)
	}
	rt := &recordingRuntime{}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}

	req := httptest.NewRequest(http.MethodDelete, "/api/sessions/"+session.ID, nil)
	rec := httptest.NewRecorder()
	srv.destroySession(rec, req, session.ID)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d body %s", rec.Code, rec.Body.String())
	}
	destroyIDs := rt.runtimeDestroyRequests()
	if len(destroyIDs) != 1 || destroyIDs[0] != session.RestoreID {
		t.Fatalf("destroy session should tear down restore id %q, got %+v", session.RestoreID, destroyIDs)
	}
	destroyGenerationRequests := rt.destroyGenerationRequests()
	if len(destroyGenerationRequests) != 1 || destroyGenerationRequests[0].GenerationID != allocation.GenerationID {
		t.Fatalf("destroy session should clean generation resources, got %+v", destroyGenerationRequests)
	}
	var sessionStatus, turnStatus, turnErrorClass, generationStatus, generationErrorClass, networkState, resourceState string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT s.status, t.status, COALESCE(t.error_class, ''), g.status, COALESCE(g.error_class, ''),
       n.allocation_state, r.resource_state
FROM sessions s
JOIN turns t ON t.session_id = s.id
JOIN runtime_generations g ON g.session_id = s.id
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE s.id = ?
  AND t.id = ?`, session.ID, enqueued.TurnID).Scan(
		&sessionStatus,
		&turnStatus,
		&turnErrorClass,
		&generationStatus,
		&generationErrorClass,
		&networkState,
		&resourceState,
	); err != nil {
		t.Fatalf("query destroyed state: %v", err)
	}
	if sessionStatus != string(sessionstate.Destroyed) ||
		turnStatus != "canceled" ||
		turnErrorClass != "session_destroyed" ||
		generationStatus != "failed" ||
		generationErrorClass != "session_destroyed" ||
		networkState != "destroyed" ||
		resourceState != "destroyed" {
		t.Fatalf("unexpected destroyed state: session=%s turn=%s turn_error=%s generation=%s generation_error=%s network=%s resource=%s",
			sessionStatus, turnStatus, turnErrorClass, generationStatus, generationErrorClass, networkState, resourceState)
	}
	var destroyedEvents int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM events
WHERE session_id = ?
  AND type = 'session.destroyed'`, session.ID).Scan(&destroyedEvents); err != nil {
		t.Fatalf("count destroyed events: %v", err)
	}
	if destroyedEvents != 1 {
		t.Fatalf("expected one durable destroyed event, got %d", destroyedEvents)
	}
}

func TestRunPhase7MaintenancePollsBridgeOutbox(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_bridge_poll", string(sessionstate.Created), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	cfg.Phase7.Bridge.PollInterval = config.Duration{Duration: 10 * time.Millisecond}
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config: store.ResourceAllocatorConfig{
			RunDir:             cfg.Phase7.RunDir,
			CIDRPool:           cfg.Phase7.Network.CIDRPool.Prefix,
			EgressDorisFEHosts: cfg.Phase7.Network.Egress.DorisFEHosts,
			EgressDorisBEHosts: cfg.Phase7.Network.Egress.DorisBEHosts,
			EgressDorisPorts:   cfg.Phase7.Network.Egress.DorisPorts,
			EgressDNSPolicy:    string(cfg.Phase7.Network.Egress.DNSPolicy),
			HostProxyBindURL:   cfg.Claude.ProxyBindURL,
			ProxyPort:          8082,
			Agent:              "claude",
			AgentModel:         cfg.Claude.Model,
			AgentOutputFormat:  cfg.Claude.OutputFormat,
		},
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, allocation.GenerationID, allocation.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("mark generation live: %v", err)
	}
	details, err := st.GetRuntimeGenerationDetails(ctx, session.ID, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get generation details: %v", err)
	}
	outbox, err := bridge.OpenQueue(details.BridgeDirPath, bridge.OutboxDir)
	if err != nil {
		t.Fatalf("open outbox: %v", err)
	}
	if _, err := outbox.Write(ctx, bridge.Envelope{
		MessageID:    "msg_hello",
		RequestID:    "req_hello",
		Type:         bridge.TypeHello,
		SessionID:    session.ID,
		GenerationID: allocation.GenerationID,
	}); err != nil {
		t.Fatalf("write hello: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: instantRuntime{},
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)
	done := make(chan error, 1)
	go func() {
		done <- srv.RunPhase7Maintenance(runCtx)
	}()

	response := waitForBridgeInboxResponse(t, runCtx, details.BridgeDirPath, bridge.TypeHelloAck, "req_hello")
	if response.GenerationID != allocation.GenerationID || response.SessionID != session.ID {
		t.Fatalf("unexpected bridge response identity: %+v", response)
	}
	if _, err := os.Stat(filepath.Join(details.BridgeDirPath, bridge.HeartbeatDir, bridge.HostHeartbeatFile)); err != nil {
		t.Fatalf("host heartbeat file missing after bridge poll: %v", err)
	}
	cancel()
	err = <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("maintenance exit err=%v, want context canceled", err)
	}
}

func TestRunPhase7MaintenanceRecoversGenerationThatExpiresAfterStartup(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_expiring_generation", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	cfg.Phase7.Bridge.HeartbeatInterval = config.Duration{Duration: 10 * time.Millisecond}
	cfg.Phase7.Bridge.ReconnectGrace = config.Duration{Duration: 20 * time.Millisecond}
	leaseOwner := store.GenerationLeaseOwner(owner.UUID)
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, "claude"),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, allocation.GenerationID, allocation.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("mark generation live: %v", err)
	}
	expiresAt := time.Now().UTC().Add(25 * time.Millisecond)
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generations
SET lease_owner = ?,
    lease_expires_at = ?,
    last_seen_at = ?
WHERE generation_id = ?`,
		store.GenerationLeaseOwner("previous-owner"),
		expiresAt.Format(time.RFC3339Nano),
		expiresAt.Add(-time.Minute).Format(time.RFC3339Nano),
		allocation.GenerationID,
	); err != nil {
		t.Fatalf("move generation to previous owner: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	rt := &recordingRuntime{}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)
	done := make(chan error, 1)
	go func() {
		done <- srv.RunPhase7Maintenance(runCtx)
	}()

	waitForGenerationStatus(t, runCtx, st, allocation.GenerationID, "failed")
	cancel()
	err = <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("maintenance exit err=%v, want context canceled", err)
	}

	var errorClass, leaseOwnerAfter string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COALESCE(error_class, ''), COALESCE(lease_owner, '')
FROM runtime_generations
WHERE generation_id = ?`, allocation.GenerationID).Scan(&errorClass, &leaseOwnerAfter); err != nil {
		t.Fatalf("query recovered generation: %v", err)
	}
	if errorClass != "orchestrator_restart_reconnect_grace_expired" || leaseOwnerAfter != "" {
		t.Fatalf("unexpected recovered generation: error_class=%s lease_owner=%s", errorClass, leaseOwnerAfter)
	}
	if got := rt.runtimeDestroyRequests(); len(got) != 1 || got[0] != session.RestoreID {
		t.Fatalf("maintenance should destroy runtime before repair using restore id %q, got %+v", session.RestoreID, got)
	}
	if _, starts := rt.requests(); len(starts) != 0 {
		t.Fatalf("maintenance should not cold-start without a queued turn: %+v", starts)
	}
}

func TestExpiredRuntimeRecoverySkipsRepairWhenRuntimeCleanupFails(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	now := time.Now().UTC()
	session := createServerTestSession(t, ctx, st, dir, "sess_recovery_cleanup_fail", string(sessionstate.RunningIdle), now.Add(-2*time.Minute), nil)
	cfg := testServerConfig(dir)
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now.Add(-3 * time.Minute),
		Config:    serverTestAllocatorConfig(cfg, session.Agent),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, allocation.GenerationID, allocation.Owner, now.Add(-3*time.Minute+time.Second)); err != nil {
		t.Fatalf("mark generation live: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'idle',
    lease_owner = ?,
    lease_expires_at = ?
WHERE generation_id = ?`, store.GenerationLeaseOwner("previous-owner"), now.Add(-time.Minute).Format(time.RFC3339Nano), allocation.GenerationID); err != nil {
		t.Fatalf("expire generation: %v", err)
	}
	rt := &recordingRuntime{destroyRuntimeErr: errors.New("runsc delete failed")}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	recovered, err := srv.RecoverExpiredRuntimeResources(ctx, now)
	if err != nil {
		t.Fatalf("recover expired runtime resources: %v", err)
	}
	if recovered.RuntimeCleanupSkipped != 1 ||
		recovered.ReconnectGraceFailed != 0 ||
		recovered.ExpiredLifecycleFailed != 0 ||
		recovered.UnknownAfterAckStarted != 0 {
		t.Fatalf("cleanup failure should skip repair, got %+v", recovered)
	}
	if got := rt.runtimeDestroyRequests(); len(got) != 1 || got[0] != session.RestoreID {
		t.Fatalf("expected runtime cleanup attempt for %q, got %+v", session.RestoreID, got)
	}
	var generationStatus, ownerAfter, networkState, resourceState string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, COALESCE(g.lease_owner, ''), n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&generationStatus, &ownerAfter, &networkState, &resourceState); err != nil {
		t.Fatalf("query skipped recovery state: %v", err)
	}
	if generationStatus != "idle" ||
		ownerAfter != string(store.GenerationLeaseOwner("previous-owner")) ||
		networkState != "live" ||
		resourceState != "live" {
		t.Fatalf("cleanup failure should leave DB non-reclaimable: generation=%s owner=%s network=%s resource=%s", generationStatus, ownerAfter, networkState, resourceState)
	}
}

func TestDestroyReclaimableGenerationResourcesMarksDestroyedOnlyAfterRuntimeCleanup(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()

	for _, tc := range []struct {
		name       string
		destroyErr error
		wantState  string
	}{
		{name: "cleanup succeeds", wantState: "destroyed"},
		{name: "cleanup fails", destroyErr: errors.New("netns busy"), wantState: "reclaimable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			st, owner := openServerOwnedStore(t, ctx, dir)
			cfg := testServerConfig(dir)
			createServerTestSession(t, ctx, st, dir, "sess_cleanup", string(sessionstate.Created), now.Add(-time.Minute), nil)
			allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
				SessionID: "sess_cleanup",
				Owner:     store.GenerationLeaseOwner(owner.UUID),
				LeaseTTL:  time.Minute,
				Now:       now.Add(-time.Minute),
				Config:    serverTestAllocatorConfig(cfg, "claude"),
			})
			if err != nil {
				t.Fatalf("allocate generation: %v", err)
			}
			if err := st.MarkGenerationResourcesLive(ctx, "sess_cleanup", allocation.GenerationID, allocation.Owner, now.Add(-59*time.Second)); err != nil {
				t.Fatalf("mark resources live: %v", err)
			}
			if err := st.FailGeneration(ctx, store.FailGenerationParams{
				SessionID:    "sess_cleanup",
				GenerationID: allocation.GenerationID,
				Owner:        allocation.Owner,
				ErrorClass:   "probe_failed_pre_start",
				Reason:       "probe failed",
				Now:          now.Add(-58 * time.Second),
			}); err != nil {
				t.Fatalf("fail generation: %v", err)
			}

			rt := &recordingRuntime{destroyErr: tc.destroyErr}
			srv := &Server{
				cfg:     cfg,
				store:   st,
				runtime: rt,
				watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
				hub:     events.NewHub(),
				log:     slog.Default(),
			}
			srv.destroyReclaimableGenerationResources(ctx, now)

			calls := rt.destroyGenerationRequests()
			if len(calls) != 1 || calls[0].GenerationID != allocation.GenerationID {
				t.Fatalf("destroy generation calls=%+v", calls)
			}
			var networkState, resourceState string
			if err := st.DBForTest().QueryRowContext(ctx, `
SELECT n.allocation_state, r.resource_state
FROM network_profiles n
JOIN runtime_generation_resources r ON r.generation_id = n.generation_id
WHERE n.generation_id = ?`, allocation.GenerationID).Scan(&networkState, &resourceState); err != nil {
				t.Fatalf("query resource states: %v", err)
			}
			if networkState != tc.wantState || resourceState != tc.wantState {
				t.Fatalf("unexpected states after cleanup: network=%s resource=%s want %s", networkState, resourceState, tc.wantState)
			}
		})
	}
}

func TestDestroyReclaimableGenerationResourcesRemovesFilesystemWithRealRuntime(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	cfg := testServerConfig(dir)
	createServerTestSession(t, ctx, st, dir, "sess_cleanup_real", string(sessionstate.Created), now.Add(-time.Minute), nil)
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: "sess_cleanup_real",
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now.Add(-time.Minute),
		Config:    serverTestAllocatorConfig(cfg, "claude"),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, "sess_cleanup_real", allocation.GenerationID, allocation.Owner, now.Add(-59*time.Second)); err != nil {
		t.Fatalf("mark resources live: %v", err)
	}
	if err := st.FailGeneration(ctx, store.FailGenerationParams{
		SessionID:    "sess_cleanup_real",
		GenerationID: allocation.GenerationID,
		Owner:        allocation.Owner,
		ErrorClass:   "probe_failed_pre_start",
		Reason:       "probe failed",
		Now:          now.Add(-58 * time.Second),
	}); err != nil {
		t.Fatalf("fail generation: %v", err)
	}
	details, err := st.GetRuntimeGenerationDetails(ctx, "sess_cleanup_real", allocation.GenerationID)
	if err != nil {
		t.Fatalf("get runtime generation details: %v", err)
	}
	createServerGenerationFilesystem(t, details)

	realRuntime := runtime.New(runtime.Config{
		RunscNetwork:    "sandbox",
		RunscOverlay2:   "none",
		Phase7RunDir:    cfg.Phase7.RunDir,
		CheckpointsRoot: filepath.Join(dir, "checkpoints"),
		CommandRunner:   serverCommandRunner{},
	})
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: realRuntime,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.destroyReclaimableGenerationResources(ctx, now)

	for _, path := range []string{details.CheckpointPath, details.ControlDirPath, details.BundleDirPath, details.BridgeDirPath, details.LogDirPath} {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatalf("expected cleanup path %s to be removed, stat err=%v", path, err)
		}
	}
	var networkState, resourceState string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT n.allocation_state, r.resource_state
FROM network_profiles n
JOIN runtime_generation_resources r ON r.generation_id = n.generation_id
WHERE n.generation_id = ?`, allocation.GenerationID).Scan(&networkState, &resourceState); err != nil {
		t.Fatalf("query resource states: %v", err)
	}
	if networkState != "destroyed" || resourceState != "destroyed" {
		t.Fatalf("unexpected states after real runtime cleanup: network=%s resource=%s", networkState, resourceState)
	}
}

func TestRunPhase7MaintenancePublishesBridgeOutputAndCompletion(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_bridge_events", string(sessionstate.RunningActive), time.Now().UTC(), nil)
	if _, err := st.DBForTest().ExecContext(ctx, `UPDATE sessions SET agent = 'sh' WHERE id = ?`, session.ID); err != nil {
		t.Fatalf("set shell agent: %v", err)
	}
	cfg := testServerConfig(dir)
	cfg.Phase7.Bridge.PollInterval = config.Duration{Duration: 10 * time.Millisecond}
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, "sh"),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, allocation.GenerationID, allocation.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("mark generation live: %v", err)
	}
	details, err := st.GetRuntimeGenerationDetails(ctx, session.ID, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get generation details: %v", err)
	}
	turnID, err := st.EnqueueTurn(ctx, session.ID, "run", time.Now().UTC())
	if err != nil {
		t.Fatalf("enqueue turn: %v", err)
	}
	grant, ok, err := st.ClaimNextTurn(ctx, store.ClaimNextTurnParams{
		SessionID:    session.ID,
		GenerationID: allocation.GenerationID,
		Owner:        allocation.Owner,
		RequestID:    "req_claim",
		LeaseTTL:     time.Minute,
		Now:          time.Now().UTC(),
	})
	if err != nil || !ok || grant.TurnID != turnID {
		t.Fatalf("claim setup: ok=%v grant=%+v err=%v", ok, grant, err)
	}
	sandboxSourceIP := serverSandboxSourceIPForGeneration(t, ctx, st, allocation.GenerationID)
	outbox, err := bridge.OpenQueue(details.BridgeDirPath, bridge.OutboxDir)
	if err != nil {
		t.Fatalf("open outbox: %v", err)
	}
	if _, err := outbox.Write(ctx, bridge.Envelope{
		MessageID:    "msg_started",
		Type:         bridge.TypeAckTurnStarted,
		SessionID:    session.ID,
		GenerationID: allocation.GenerationID,
		TurnID:       &turnID,
		Payload:      json.RawMessage(fmt.Sprintf(`{"sandbox_source_ip":%q}`, sandboxSourceIP)),
	}); err != nil {
		t.Fatalf("write started: %v", err)
	}
	if _, err := outbox.Write(ctx, bridge.Envelope{
		MessageID:    "msg_output",
		Type:         bridge.TypeEmitOutput,
		SessionID:    session.ID,
		GenerationID: allocation.GenerationID,
		TurnID:       &turnID,
		Payload:      json.RawMessage(`{"output_sequence":1,"stream":"stdout","payload":{"line":"{\"type\":\"harness.shell_output\",\"stream\":\"stdout\",\"text\":\"ok\\n\"}"}}`),
	}); err != nil {
		t.Fatalf("write output: %v", err)
	}
	if _, err := outbox.Write(ctx, bridge.Envelope{
		MessageID:    "msg_done",
		Type:         bridge.TypeAckTurnCompleted,
		SessionID:    session.ID,
		GenerationID: allocation.GenerationID,
		TurnID:       &turnID,
		Payload:      json.RawMessage(`{"status":"completed"}`),
	}); err != nil {
		t.Fatalf("write done: %v", err)
	}

	hub := events.NewHub()
	eventsCh, cancelEvents := hub.Subscribe(session.ID)
	defer cancelEvents()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: instantRuntime{},
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, hub),
		hub:     hub,
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)
	done := make(chan error, 1)
	go func() {
		done <- srv.RunPhase7Maintenance(runCtx)
	}()
	waitForSessionStatus(t, runCtx, st, session.ID, string(sessionstate.RunningIdle))
	waitForHubEvent(t, eventsCh, bridge.TypeAckTurnCompleted)
	cancel()
	err = <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("maintenance exit err=%v, want context canceled", err)
	}

	var assistantMessages int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM messages
WHERE session_id = ?
  AND role = 'assistant'
  AND content = 'ok
'`, session.ID).Scan(&assistantMessages); err != nil {
		t.Fatalf("assistant messages: %v", err)
	}
	if assistantMessages != 1 {
		t.Fatalf("assistant messages=%d want 1", assistantMessages)
	}
}

func TestBridgeFailedCompletionDoesNotFailSession(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_bridge_failed", string(sessionstate.RunningActive), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, "claude"),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, allocation.GenerationID, allocation.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("mark generation live: %v", err)
	}
	turnID, err := st.EnqueueTurn(ctx, session.ID, "run", time.Now().UTC())
	if err != nil {
		t.Fatalf("enqueue turn: %v", err)
	}
	grant, ok, err := st.ClaimNextTurn(ctx, store.ClaimNextTurnParams{
		SessionID:    session.ID,
		GenerationID: allocation.GenerationID,
		Owner:        allocation.Owner,
		RequestID:    "req_failed",
		LeaseTTL:     time.Minute,
		Now:          time.Now().UTC(),
	})
	if err != nil || !ok || grant.TurnID != turnID {
		t.Fatalf("claim setup: ok=%v grant=%+v err=%v", ok, grant, err)
	}
	if _, err := st.AckTurnStarted(ctx, store.AckStartedParams{
		SessionID:       session.ID,
		GenerationID:    allocation.GenerationID,
		TurnID:          turnID,
		Owner:           allocation.Owner,
		SandboxSourceIP: serverSandboxSourceIPForGeneration(t, ctx, st, allocation.GenerationID),
		LeaseTTL:        time.Minute,
		Now:             time.Now().UTC(),
	}); err != nil {
		t.Fatalf("ack started: %v", err)
	}
	completionPayload := map[string]string{
		"status":      "failed",
		"error_class": "agent_error",
		"error":       "agent exited 1",
	}
	eventID, err := st.CompleteTurn(ctx, store.CompleteTurnParams{
		SessionID:      session.ID,
		GenerationID:   allocation.GenerationID,
		TurnID:         turnID,
		Owner:          allocation.Owner,
		TerminalStatus: "failed",
		ErrorClass:     "agent_error",
		Error:          "agent exited 1",
		EventType:      bridge.TypeAckTurnCompleted,
		EventDedupeKey: "ack_completed:" + allocation.GenerationID,
		EventPayload:   completionPayload,
		Now:            time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("complete failed turn: %v", err)
	}

	hub := events.NewHub()
	eventsCh, cancelEvents := hub.Subscribe(session.ID)
	defer cancelEvents()
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: instantRuntime{},
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, hub),
		hub:     hub,
		log:     slog.Default(),
	}
	envelopePayload, err := json.Marshal(completionPayload)
	if err != nil {
		t.Fatalf("marshal completion payload: %v", err)
	}
	srv.handleBridgeCommittedEnvelope(ctx, bridge.Envelope{
		Type:         bridge.TypeAckTurnCompleted,
		SessionID:    session.ID,
		GenerationID: allocation.GenerationID,
		TurnID:       &turnID,
		Payload:      envelopePayload,
	}, eventID)

	got, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if got.Status != string(sessionstate.RunningIdle) || !sessionstate.CanAcceptInput(got.Status) {
		t.Fatalf("failed completion should leave session retryable, got %s", got.Status)
	}
	seenCompletion := false
	for {
		select {
		case event := <-eventsCh:
			switch event.Type {
			case bridge.TypeAckTurnCompleted:
				seenCompletion = true
			case "session." + string(sessionstate.Failed), "session.error":
				t.Fatalf("unexpected terminal event after failed completion: %+v", event)
			}
		default:
			if !seenCompletion {
				t.Fatalf("missing durable completion event")
			}
			return
		}
	}
}

func TestRunPhase7MaintenancePrunesRetainedEvents(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	now := time.Now().UTC()
	createServerTestSession(t, ctx, st, dir, "sess_events_a", string(sessionstate.Created), now, nil)
	createServerTestSession(t, ctx, st, dir, "sess_events_b", string(sessionstate.Created), now, nil)
	firstID, err := st.AppendEvent(ctx, store.AppendEventParams{
		SessionID: "sess_events_a",
		Type:      "test.event",
		Payload:   map[string]string{"name": "first"},
		Now:       now.Add(-3 * time.Second),
	})
	if err != nil {
		t.Fatalf("append first event: %v", err)
	}
	secondID, err := st.AppendEvent(ctx, store.AppendEventParams{
		SessionID: "sess_events_b",
		Type:      "test.event",
		Payload:   map[string]string{"name": "second"},
		Now:       now.Add(-2 * time.Second),
	})
	if err != nil {
		t.Fatalf("append second event: %v", err)
	}
	thirdID, err := st.AppendEvent(ctx, store.AppendEventParams{
		SessionID: "sess_events_a",
		Type:      "test.event",
		Payload:   map[string]string{"name": "third"},
		Now:       now.Add(-time.Second),
	})
	if err != nil {
		t.Fatalf("append third event: %v", err)
	}

	cfg := testServerConfig(dir)
	cfg.Phase7.Events.RetentionWindow = config.Duration{Duration: time.Hour}
	cfg.Phase7.Events.RetentionRows = 2
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: instantRuntime{},
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)
	done := make(chan error, 1)
	go func() {
		done <- srv.RunPhase7Maintenance(runCtx)
	}()
	waitForEventIDs(t, runCtx, st, []int64{secondID, thirdID})
	cancel()
	err = <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("maintenance exit err=%v, want context canceled", err)
	}
	if _, ok, err := st.GetEvent(ctx, firstID); err != nil || ok {
		t.Fatalf("first event retained ok=%v err=%v", ok, err)
	}
}

func TestSendMessageRejectsExpiredSessionBeforeAllocation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	expired := time.Now().UTC().Add(-time.Second)
	session := createServerTestSession(t, ctx, st, dir, "sess_expired", string(sessionstate.Created), time.Now().UTC(), &expired)
	srv := &Server{
		cfg:     testServerConfig(dir),
		store:   st,
		runtime: instantRuntime{},
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"hello"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected status 409, got %d body %s", rec.Code, rec.Body.String())
	}
	var generations int
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_generations`).Scan(&generations); err != nil {
		t.Fatalf("count generations: %v", err)
	}
	if generations != 0 {
		t.Fatalf("expired session should not allocate generation, got %d", generations)
	}
}

func TestInternalProxyRequestEndpointsPublishDurableEvents(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	cfg := testServerConfig(dir)
	now := time.Now().UTC()
	allocation, turnID, sandboxSourceIP := createServerRunningProxyTurn(t, ctx, st, cfg, owner.UUID, dir, "sess_proxy_http", now)

	hub := events.NewHub()
	eventsCh, cancelEvents := hub.Subscribe("sess_proxy_http")
	defer cancelEvents()
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: instantRuntime{},
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, hub),
		hub:     hub,
		log:     slog.Default(),
	}

	blocked := httptest.NewRequest(http.MethodPost, "/internal/proxy/requests/start", strings.NewReader(fmt.Sprintf(`{"sandbox_source_ip":%q,"proxy_request_id":"proxy_blocked"}`, sandboxSourceIP)))
	blocked.RemoteAddr = "203.0.113.7:5000"
	blockedRec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(blockedRec, blocked)
	if blockedRec.Code != http.StatusForbidden {
		t.Fatalf("non-loopback proxy request status=%d body=%s", blockedRec.Code, blockedRec.Body.String())
	}

	startReq := httptest.NewRequest(http.MethodPost, "/internal/proxy/requests/start", strings.NewReader(fmt.Sprintf(`{
		"sandbox_source_ip":%q,
		"proxy_request_id":"proxy_http_1",
		"upstream_model":"claude-sonnet",
		"upstream_base_url":"https://api.anthropic.test"
	}`, sandboxSourceIP)))
	startReq.RemoteAddr = "127.0.0.1:5001"
	startRec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(startRec, startReq)
	if startRec.Code != http.StatusOK {
		t.Fatalf("start status=%d body=%s", startRec.Code, startRec.Body.String())
	}
	var startResp struct {
		SessionID       string `json:"session_id"`
		TurnID          int64  `json:"turn_id"`
		GenerationID    string `json:"generation_id"`
		RequestSequence int64  `json:"request_sequence"`
		EventID         int64  `json:"event_id"`
		Replayed        bool   `json:"replayed"`
	}
	if err := json.Unmarshal(startRec.Body.Bytes(), &startResp); err != nil {
		t.Fatalf("decode start response: %v", err)
	}
	if startResp.SessionID != "sess_proxy_http" || startResp.GenerationID != allocation.GenerationID ||
		startResp.TurnID != turnID || startResp.RequestSequence != 1 || startResp.EventID == 0 || startResp.Replayed {
		t.Fatalf("unexpected start response: %+v allocation=%+v turn=%d", startResp, allocation, turnID)
	}
	startEvent := waitForHubEvent(t, eventsCh, "proxy.request.started")
	if startEvent.EventID != startResp.EventID || startEvent.ProxyRequestID != "proxy_http_1" ||
		startEvent.SessionID != "sess_proxy_http" {
		t.Fatalf("unexpected start hub event: %+v response=%+v", startEvent, startResp)
	}

	finishReq := httptest.NewRequest(http.MethodPost, "/internal/proxy/requests/finish", strings.NewReader(`{
		"proxy_request_id":"proxy_http_1",
		"http_status":200,
		"upstream_total_latency_ms":321,
		"retry_count":0
	}`))
	finishReq.RemoteAddr = "[::1]:5002"
	finishRec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(finishRec, finishReq)
	if finishRec.Code != http.StatusOK {
		t.Fatalf("finish status=%d body=%s", finishRec.Code, finishRec.Body.String())
	}
	var finishResp struct {
		Status       string `json:"status"`
		EventID      int64  `json:"event_id"`
		EventType    string `json:"event_type"`
		SessionID    string `json:"session_id"`
		TurnID       int64  `json:"turn_id"`
		GenerationID string `json:"generation_id"`
		Replayed     bool   `json:"replayed"`
	}
	if err := json.Unmarshal(finishRec.Body.Bytes(), &finishResp); err != nil {
		t.Fatalf("decode finish response: %v", err)
	}
	if finishResp.Status != "accepted" || finishResp.EventType != "proxy.request.completed" ||
		finishResp.SessionID != "sess_proxy_http" || finishResp.GenerationID != allocation.GenerationID ||
		finishResp.TurnID != turnID || finishResp.EventID <= startResp.EventID || finishResp.Replayed {
		t.Fatalf("unexpected finish response: %+v start=%+v", finishResp, startResp)
	}
	finishEvent := waitForHubEvent(t, eventsCh, "proxy.request.completed")
	if finishEvent.EventID != finishResp.EventID || finishEvent.ProxyRequestID != "proxy_http_1" {
		t.Fatalf("unexpected finish hub event: %+v response=%+v", finishEvent, finishResp)
	}

	unknownReq := httptest.NewRequest(http.MethodPost, "/internal/proxy/requests/finish", strings.NewReader(`{"proxy_request_id":"proxy_missing"}`))
	unknownReq.RemoteAddr = "127.0.0.1:5003"
	unknownRec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(unknownRec, unknownReq)
	if unknownRec.Code != http.StatusOK {
		t.Fatalf("unknown finish status=%d body=%s", unknownRec.Code, unknownRec.Body.String())
	}
	var unknownResp map[string]string
	if err := json.Unmarshal(unknownRec.Body.Bytes(), &unknownResp); err != nil {
		t.Fatalf("decode unknown finish response: %v", err)
	}
	if unknownResp["status"] != "stale_unknown_request" {
		t.Fatalf("unexpected unknown finish response: %v", unknownResp)
	}
}

func TestEventsStreamReplaysDurableEventsAfterLastEventID(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	now := time.Now().UTC()
	createServerTestSession(t, ctx, st, dir, "sess_a", string(sessionstate.RunningActive), now, nil)
	createServerTestSession(t, ctx, st, dir, "sess_b", string(sessionstate.RunningActive), now, nil)

	firstID, err := st.AppendEvent(ctx, store.AppendEventParams{
		SessionID: "sess_a",
		Type:      bridge.TypeAckTurnStarted,
		Payload:   map[string]string{"phase": "first"},
		Now:       now,
	})
	if err != nil {
		t.Fatalf("append first event: %v", err)
	}
	secondID, err := st.AppendEvent(ctx, store.AppendEventParams{
		SessionID: "sess_b",
		Type:      bridge.TypeEmitOutput,
		Payload:   map[string]string{"line": "second"},
		Now:       now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("append second event: %v", err)
	}
	thirdID, err := st.AppendEvent(ctx, store.AppendEventParams{
		SessionID: "sess_a",
		Type:      bridge.TypeAckTurnCompleted,
		Payload:   map[string]string{"status": "completed"},
		Now:       now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("append third event: %v", err)
	}

	srv := &Server{store: st, hub: events.NewHub(), log: slog.Default()}
	req := httptest.NewRequest(http.MethodGet, "/api/events/stream?last_event_id="+strconv.FormatInt(firstID, 10), nil)
	lastEventID, ok, err := parseLastEventID(req)
	if err != nil || !ok || lastEventID != firstID {
		t.Fatalf("parse last_event_id: id=%d ok=%v err=%v", lastEventID, ok, err)
	}
	rec := httptest.NewRecorder()
	replayedThrough, err := srv.writeSSEReplay(req.Context(), rec, rec, "", lastEventID)
	if err != nil {
		t.Fatalf("write replay: %v", err)
	}
	if replayedThrough != thirdID {
		t.Fatalf("replayed through=%d want %d", replayedThrough, thirdID)
	}
	body := rec.Body.String()
	if strings.Contains(body, "id: "+strconv.FormatInt(firstID, 10)+"\n") {
		t.Fatalf("replay included already-seen event: %s", body)
	}
	assertContains(t, body, "id: "+strconv.FormatInt(secondID, 10)+"\n")
	assertContains(t, body, "event: "+bridge.TypeEmitOutput+"\n")
	assertContains(t, body, `"event_id":`+strconv.FormatInt(secondID, 10))
	assertContains(t, body, `"session_id":"sess_b"`)
	assertContains(t, body, `"payload":{"line":"second"}`)
	assertContains(t, body, "id: "+strconv.FormatInt(thirdID, 10)+"\n")
	assertContains(t, body, "event: "+bridge.TypeAckTurnCompleted+"\n")
	if strings.Index(body, "id: "+strconv.FormatInt(secondID, 10)+"\n") >
		strings.Index(body, "id: "+strconv.FormatInt(thirdID, 10)+"\n") {
		t.Fatalf("replayed events out of order: %s", body)
	}

	filtered := httptest.NewRecorder()
	replayedThrough, err = srv.writeSSEReplay(req.Context(), filtered, filtered, "sess_a", firstID)
	if err != nil {
		t.Fatalf("write filtered replay: %v", err)
	}
	if replayedThrough != thirdID {
		t.Fatalf("filtered replayed through=%d want %d", replayedThrough, thirdID)
	}
	filteredBody := filtered.Body.String()
	if strings.Contains(filteredBody, "id: "+strconv.FormatInt(secondID, 10)+"\n") {
		t.Fatalf("filtered replay included another session: %s", filteredBody)
	}
	assertContains(t, filteredBody, "id: "+strconv.FormatInt(thirdID, 10)+"\n")

	headerReq := httptest.NewRequest(http.MethodGet, "/api/events/stream?last_event_id=1", nil)
	headerReq.Header.Set("Last-Event-ID", strconv.FormatInt(thirdID, 10))
	lastEventID, ok, err = parseLastEventID(headerReq)
	if err != nil || !ok || lastEventID != thirdID {
		t.Fatalf("header Last-Event-ID should win: id=%d ok=%v err=%v", lastEventID, ok, err)
	}

	deleted, err := st.PruneEvents(ctx, store.PruneEventsParams{
		RetentionRows: 2,
		Now:           now.Add(3 * time.Second),
	})
	if err != nil {
		t.Fatalf("prune replay events: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted=%d want 1", deleted)
	}

	gap := httptest.NewRecorder()
	replayedThrough, err = srv.writeSSEReplay(req.Context(), gap, gap, "", 0)
	if err != nil {
		t.Fatalf("write gap replay: %v", err)
	}
	if replayedThrough != thirdID {
		t.Fatalf("gap replayed through=%d want %d", replayedThrough, thirdID)
	}
	gapBody := gap.Body.String()
	assertContains(t, gapBody, "id: "+strconv.FormatInt(secondID-1, 10)+"\n")
	assertContains(t, gapBody, "event: replay_gap\n")
	assertContains(t, gapBody, `"requested_last_event_id":0`)
	assertContains(t, gapBody, `"oldest_available":`+strconv.FormatInt(secondID, 10))
	assertContains(t, gapBody, `"session_id_filter":null`)
	assertContains(t, gapBody, `"reason":"retention_window_exceeded"`)
	assertContains(t, gapBody, "id: "+strconv.FormatInt(secondID, 10)+"\n")
	assertContains(t, gapBody, "id: "+strconv.FormatInt(thirdID, 10)+"\n")
	if strings.Contains(gapBody, `"payload":{"phase":"first"}`) {
		t.Fatalf("gap replay included pruned event: %s", gapBody)
	}

	filteredGap := httptest.NewRecorder()
	replayedThrough, err = srv.writeSSEReplay(req.Context(), filteredGap, filteredGap, "sess_a", 0)
	if err != nil {
		t.Fatalf("write filtered gap replay: %v", err)
	}
	if replayedThrough != thirdID {
		t.Fatalf("filtered gap replayed through=%d want %d", replayedThrough, thirdID)
	}
	filteredGapBody := filteredGap.Body.String()
	assertContains(t, filteredGapBody, "id: "+strconv.FormatInt(thirdID-1, 10)+"\n")
	assertContains(t, filteredGapBody, "event: replay_gap\n")
	assertContains(t, filteredGapBody, `"oldest_available":`+strconv.FormatInt(thirdID, 10))
	assertContains(t, filteredGapBody, `"session_id_filter":"sess_a"`)
	assertContains(t, filteredGapBody, "id: "+strconv.FormatInt(thirdID, 10)+"\n")
	if strings.Contains(filteredGapBody, `"payload":{"line":"second"}`) {
		t.Fatalf("filtered gap replay included another session: %s", filteredGapBody)
	}
}

func waitForBridgeInboxResponse(t *testing.T, ctx context.Context, root, responseType, requestID string) bridge.Envelope {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		queue, err := bridge.OpenQueue(root, bridge.InboxDir)
		if err != nil {
			t.Fatalf("open inbox: %v", err)
		}
		files, err := queue.ReadAll()
		if err != nil {
			t.Fatalf("read inbox: %v", err)
		}
		for _, file := range files {
			if file.Envelope.Type == responseType && file.Envelope.RequestID == requestID {
				if err := file.Unlink(); err != nil {
					t.Fatalf("unlink response: %v", err)
				}
				return file.Envelope
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context canceled before bridge response")
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Fatalf("timed out waiting for bridge response %s/%s", responseType, requestID)
	return bridge.Envelope{}
}

func TestDownloadArtifactAllowsNestedRegularFile(t *testing.T) {
	srv, _, workspace := newArtifactDownloadServer(t, "sess_1")
	sessionDir := filepath.Join(workspace, "reports")
	if err := os.MkdirAll(sessionDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sessionDir, "summary.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("write artifact: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/artifacts/sess_1/reports/summary.txt", nil)
	rec := httptest.NewRecorder()

	srv.downloadArtifact(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d body %s", rec.Code, rec.Body.String())
	}
	if rec.Body.String() != "hello" {
		t.Fatalf("unexpected body %q", rec.Body.String())
	}
}

func TestDownloadArtifactRejectsMissingWorkspaceEvidence(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := testArtifactDownloadConfig(t, dir)
	if err := os.MkdirAll(filepath.Dir(cfg.DBPath), 0o755); err != nil {
		t.Fatalf("mkdir db state: %v", err)
	}
	st, err := store.Open(ctx, cfg.DBPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	sessionDir := filepath.Join(cfg.SessionsRoot, "sess_1")
	if err := os.MkdirAll(sessionDir, 0o755); err != nil {
		t.Fatalf("mkdir session dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sessionDir, "orphan.txt"), []byte("orphan"), 0o644); err != nil {
		t.Fatalf("write orphan artifact: %v", err)
	}

	srv := &Server{cfg: cfg, store: st}
	req := httptest.NewRequest(http.MethodGet, "/artifacts/sess_1/orphan.txt", nil)
	rec := httptest.NewRecorder()

	srv.downloadArtifact(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d body %s", rec.Code, rec.Body.String())
	}
}

func TestDownloadArtifactRejectsInvalidWorkspaceEvidence(t *testing.T) {
	srv, st, workspace := newArtifactDownloadServer(t, "sess_1")
	if err := os.WriteFile(filepath.Join(workspace, "report.txt"), []byte("report"), 0o644); err != nil {
		t.Fatalf("write artifact: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(context.Background(), `
UPDATE session_workspaces
SET host_path = ?
WHERE session_id = ?`, filepath.Join(filepath.Dir(workspace), "other_session"), "sess_1"); err != nil {
		t.Fatalf("corrupt workspace evidence: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/artifacts/sess_1/report.txt", nil)
	rec := httptest.NewRecorder()

	srv.downloadArtifact(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected status 403, got %d body %s", rec.Code, rec.Body.String())
	}
}

func newArtifactDownloadServer(t *testing.T, sessionID string) (*Server, *store.Store, string) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	cfg := testArtifactDownloadConfig(t, dir)
	if err := os.MkdirAll(filepath.Dir(cfg.DBPath), 0o755); err != nil {
		t.Fatalf("mkdir db state: %v", err)
	}
	st, err := store.Open(ctx, cfg.DBPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	createServerTestSession(t, ctx, st, dir, sessionID, string(sessionstate.Created), time.Now().UTC(), nil)
	srv := &Server{cfg: cfg, store: st}
	volumeConfig, err := srv.dataVolumeProvisionerConfig()
	if err != nil {
		t.Fatalf("data volume config: %v", err)
	}
	workspace, err := st.ProvisionSessionWorkspace(ctx, store.ProvisionSessionWorkspaceParams{
		SessionID: sessionID,
		Config:    volumeConfig,
		Now:       time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("provision workspace: %v", err)
	}
	return srv, st, workspace.HostPath
}

func testArtifactDownloadConfig(t *testing.T, dir string) config.Config {
	t.Helper()
	cfg := testServerConfig(dir)
	cfg.AgentHomesRoot = filepath.Join(dir, "agent-homes")
	cfg.CheckpointsRoot = filepath.Join(dir, "checkpoints")
	cfg.BundleRoot = filepath.Join(dir, "bundle")
	cfg.RootFSPath = filepath.Join(dir, "rootfs")
	cfg.DBPath = filepath.Join(dir, "state", "orchestrator.db")
	cfg.RepoRoot = dir
	return cfg
}

type instantRuntime struct{}

var instantRuntimePrepareCalls int64

func (instantRuntime) PrepareGeneration(context.Context, runtime.StartRequest) (runtime.GenerationArtifacts, error) {
	atomic.AddInt64(&instantRuntimePrepareCalls, 1)
	return testGenerationArtifacts(), nil
}

func (instantRuntime) Start(ctx context.Context, req runtime.StartRequest, output func(runtime.Output)) runtime.Result {
	if output != nil {
		output(runtime.Output{Stream: "stdout", Line: `{"type":"result","subtype":"success","result":"ok"}`})
	}
	return runtime.Result{ControlManifestDigest: req.PreparedArtifacts.ManifestDigest, RunscVersion: req.PreparedArtifacts.RunscVersion}
}

func (instantRuntime) Destroy(context.Context, string) error {
	return nil
}

func (instantRuntime) DestroyGenerationResources(context.Context, store.RuntimeGenerationDetails) (runtime.GenerationResourceCleanup, error) {
	return runtime.GenerationResourceCleanup{}, nil
}

func (instantRuntime) Interrupt(string) error {
	return nil
}

func (instantRuntime) Checkpoint(context.Context, runtime.CheckpointRequest) error {
	return nil
}

type recordingRuntime struct {
	mu                sync.Mutex
	prepareRequests   []runtime.StartRequest
	startRequests     []runtime.StartRequest
	destroyRuntimeIDs []string
	destroyRuntimeErr error
	destroyRequests   []store.RuntimeGenerationDetails
	destroyErr        error
	checkpointReqs    []runtime.CheckpointRequest
	checkpointErr     error
}

type blockingPrepareRuntime struct {
	recordingRuntime
	prepareStarted chan struct{}
	releasePrepare chan struct{}
	startedOnce    sync.Once
	releaseOnce    sync.Once
}

func newBlockingPrepareRuntime() *blockingPrepareRuntime {
	return &blockingPrepareRuntime{
		prepareStarted: make(chan struct{}),
		releasePrepare: make(chan struct{}),
	}
}

func (r *blockingPrepareRuntime) PrepareGeneration(ctx context.Context, req runtime.StartRequest) (runtime.GenerationArtifacts, error) {
	r.mu.Lock()
	r.prepareRequests = append(r.prepareRequests, req)
	r.mu.Unlock()
	r.startedOnce.Do(func() { close(r.prepareStarted) })
	select {
	case <-r.releasePrepare:
		return testGenerationArtifacts(), nil
	case <-ctx.Done():
		return runtime.GenerationArtifacts{}, ctx.Err()
	}
}

func (r *blockingPrepareRuntime) release() {
	r.releaseOnce.Do(func() { close(r.releasePrepare) })
}

type startHookRuntime struct {
	recordingRuntime
	onStart func(runtime.StartRequest)
}

func (r *startHookRuntime) Start(_ context.Context, req runtime.StartRequest, _ func(runtime.Output)) runtime.Result {
	r.mu.Lock()
	r.startRequests = append(r.startRequests, req)
	r.mu.Unlock()
	if r.onStart != nil {
		r.onStart(req)
	}
	return runtime.Result{ControlManifestDigest: req.PreparedArtifacts.ManifestDigest, RunscVersion: req.PreparedArtifacts.RunscVersion}
}

func (r *recordingRuntime) PrepareGeneration(_ context.Context, req runtime.StartRequest) (runtime.GenerationArtifacts, error) {
	r.mu.Lock()
	r.prepareRequests = append(r.prepareRequests, req)
	r.mu.Unlock()
	return testGenerationArtifacts(), nil
}

func (r *recordingRuntime) Start(_ context.Context, req runtime.StartRequest, _ func(runtime.Output)) runtime.Result {
	r.mu.Lock()
	r.startRequests = append(r.startRequests, req)
	r.mu.Unlock()
	return runtime.Result{ControlManifestDigest: req.PreparedArtifacts.ManifestDigest, RunscVersion: req.PreparedArtifacts.RunscVersion}
}

func (r *recordingRuntime) DestroyGenerationResources(_ context.Context, details store.RuntimeGenerationDetails) (runtime.GenerationResourceCleanup, error) {
	r.mu.Lock()
	r.destroyRequests = append(r.destroyRequests, details)
	err := r.destroyErr
	r.mu.Unlock()
	if err != nil {
		return runtime.GenerationResourceCleanup{}, err
	}
	return runtime.GenerationResourceCleanup{
		NetnsDeleted:    true,
		HostVethDeleted: true,
		NftTableDeleted: true,
	}, nil
}

func (r *recordingRuntime) Destroy(_ context.Context, restoreID string) error {
	r.mu.Lock()
	r.destroyRuntimeIDs = append(r.destroyRuntimeIDs, restoreID)
	err := r.destroyRuntimeErr
	r.mu.Unlock()
	return err
}

func (r *recordingRuntime) Interrupt(string) error {
	return nil
}

func (r *recordingRuntime) Checkpoint(_ context.Context, req runtime.CheckpointRequest) error {
	r.mu.Lock()
	r.checkpointReqs = append(r.checkpointReqs, req)
	err := r.checkpointErr
	r.mu.Unlock()
	return err
}

func (r *recordingRuntime) requests() ([]runtime.StartRequest, []runtime.StartRequest) {
	r.mu.Lock()
	defer r.mu.Unlock()
	prepares := append([]runtime.StartRequest(nil), r.prepareRequests...)
	starts := append([]runtime.StartRequest(nil), r.startRequests...)
	return prepares, starts
}

func (r *recordingRuntime) checkpointRequests() []runtime.CheckpointRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]runtime.CheckpointRequest(nil), r.checkpointReqs...)
}

func (r *recordingRuntime) destroyGenerationRequests() []store.RuntimeGenerationDetails {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]store.RuntimeGenerationDetails(nil), r.destroyRequests...)
}

func (r *recordingRuntime) runtimeDestroyRequests() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.destroyRuntimeIDs...)
}

type restoreFailoverRuntime struct {
	recordingRuntime
	err     error
	coldErr error
}

func (r *restoreFailoverRuntime) Start(_ context.Context, req runtime.StartRequest, _ func(runtime.Output)) runtime.Result {
	r.mu.Lock()
	r.startRequests = append(r.startRequests, req)
	r.mu.Unlock()
	if req.RestoreFromCheckpoint {
		return runtime.Result{Err: r.err}
	}
	if r.coldErr != nil {
		return runtime.Result{Err: r.coldErr}
	}
	return runtime.Result{ControlManifestDigest: req.PreparedArtifacts.ManifestDigest, RunscVersion: req.PreparedArtifacts.RunscVersion}
}

type restoreStartHookRuntime struct {
	recordingRuntime
	onRestoreStart func()
}

func (r *restoreStartHookRuntime) Start(_ context.Context, req runtime.StartRequest, _ func(runtime.Output)) runtime.Result {
	r.mu.Lock()
	r.startRequests = append(r.startRequests, req)
	r.mu.Unlock()
	if req.RestoreFromCheckpoint && r.onRestoreStart != nil {
		r.onRestoreStart()
	}
	return runtime.Result{ControlManifestDigest: req.PreparedArtifacts.ManifestDigest, RunscVersion: req.PreparedArtifacts.RunscVersion}
}

type restoreValidationRuntime struct {
	restore       *runtime.Runtime
	startRequests []runtime.StartRequest
}

func (r *restoreValidationRuntime) PrepareGeneration(context.Context, runtime.StartRequest) (runtime.GenerationArtifacts, error) {
	return testGenerationArtifacts(), nil
}

func (r *restoreValidationRuntime) Start(ctx context.Context, req runtime.StartRequest, output func(runtime.Output)) runtime.Result {
	r.startRequests = append(r.startRequests, req)
	if req.RestoreFromCheckpoint {
		return r.restore.Start(ctx, req, output)
	}
	return runtime.Result{ControlManifestDigest: req.PreparedArtifacts.ManifestDigest, RunscVersion: req.PreparedArtifacts.RunscVersion}
}

func (r *restoreValidationRuntime) Destroy(context.Context, string) error {
	return nil
}

func (r *restoreValidationRuntime) DestroyGenerationResources(context.Context, store.RuntimeGenerationDetails) (runtime.GenerationResourceCleanup, error) {
	return runtime.GenerationResourceCleanup{}, nil
}

func (r *restoreValidationRuntime) Interrupt(string) error {
	return nil
}

func (r *restoreValidationRuntime) Checkpoint(context.Context, runtime.CheckpointRequest) error {
	return nil
}

type serverCommandRunner struct {
	outputs map[string][]byte
}

func (r serverCommandRunner) CombinedOutput(_ context.Context, name string, args ...string) ([]byte, error) {
	key := strings.Join(append([]string{name}, args...), " ")
	if out, ok := r.outputs[key]; ok {
		return out, nil
	}
	return nil, nil
}

type failingRuntime struct {
	prepareErr    error
	err           error
	checkpointErr error
}

func (f failingRuntime) PrepareGeneration(context.Context, runtime.StartRequest) (runtime.GenerationArtifacts, error) {
	if f.prepareErr != nil {
		return runtime.GenerationArtifacts{}, f.prepareErr
	}
	return testGenerationArtifacts(), nil
}

func (f failingRuntime) Start(context.Context, runtime.StartRequest, func(runtime.Output)) runtime.Result {
	return runtime.Result{Err: f.err}
}

func (f failingRuntime) Destroy(context.Context, string) error {
	return nil
}

func (f failingRuntime) DestroyGenerationResources(context.Context, store.RuntimeGenerationDetails) (runtime.GenerationResourceCleanup, error) {
	return runtime.GenerationResourceCleanup{}, nil
}

func (f failingRuntime) Interrupt(string) error {
	return nil
}

func (f failingRuntime) Checkpoint(context.Context, runtime.CheckpointRequest) error {
	return f.checkpointErr
}

func testGenerationArtifacts() runtime.GenerationArtifacts {
	return runtime.GenerationArtifacts{
		BundleDir:               "/tmp/bundle",
		SpecPath:                "/tmp/bundle/config.json",
		ManifestPath:            "/tmp/control/session.json",
		ManifestDigest:          "manifest_digest",
		ProjectedManifestDigest: "projected_manifest_digest",
		BundleDigest:            "bundle_digest",
		RuntimeConfigDigest:     "runtime_config_digest",
		SpecDigest:              "spec_digest",
		RunscVersion:            "runsc test",
	}
}

type serverCheckpointImageManifest struct {
	Version int                                 `json:"version"`
	Files   []serverCheckpointImageManifestFile `json:"files"`
}

type serverCheckpointImageManifestFile struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

func writeServerCheckpointFilesWithoutManifest(t *testing.T, checkpointPath string) {
	t.Helper()
	if err := os.MkdirAll(checkpointPath, 0o755); err != nil {
		t.Fatalf("create checkpoint path: %v", err)
	}
	for _, name := range []string{"checkpoint.img", "pages.img", "pages_meta.img"} {
		if err := os.WriteFile(filepath.Join(checkpointPath, name), []byte("x"), 0o644); err != nil {
			t.Fatalf("write checkpoint file %s: %v", name, err)
		}
	}
}

func buildServerCheckpointImageManifest(checkpointPath string) (serverCheckpointImageManifest, error) {
	manifest := serverCheckpointImageManifest{Version: 1}
	for _, name := range []string{"checkpoint.img", "pages.img", "pages_meta.img"} {
		path := filepath.Join(checkpointPath, name)
		data, err := os.ReadFile(path)
		if err != nil {
			return serverCheckpointImageManifest{}, err
		}
		sum := sha256.Sum256(data)
		manifest.Files = append(manifest.Files, serverCheckpointImageManifestFile{
			Path:   name,
			Size:   int64(len(data)),
			SHA256: fmt.Sprintf("%x", sum),
		})
	}
	return manifest, nil
}

func writeServerJSONFile(path string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func openServerOwnedStore(t *testing.T, ctx context.Context, dir string) (*store.Store, *store.OwnerLock) {
	t.Helper()
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	owner, err := store.AcquireOwnerLock(filepath.Join(dir, "run"))
	if err != nil {
		t.Fatalf("acquire owner: %v", err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	if err := st.WriteOwner(ctx, owner); err != nil {
		t.Fatalf("write owner: %v", err)
	}
	return st, owner
}

func createServerTestSession(t *testing.T, ctx context.Context, st *store.Store, dir, id, status string, now time.Time, expiresAt *time.Time) store.Session {
	t.Helper()
	session := store.Session{
		ID:                id,
		UserID:            labUserID,
		Status:            status,
		Agent:             "claude",
		Workspace:         filepath.Join(dir, "sessions", id),
		RestoreID:         "phase3-" + id,
		ClaudeSessionUUID: "11111111-2222-3333-4444-555555555555",
		CreatedAt:         now,
		UpdatedAt:         now,
		ExpiresAt:         expiresAt,
	}
	if err := os.MkdirAll(session.Workspace, 0o755); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := st.CreateSession(ctx, session); err != nil {
		t.Fatalf("create session: %v", err)
	}
	return session
}

func enableSessionAutoCheckpoint(t *testing.T, ctx context.Context, st *store.Store, sessionID string) {
	t.Helper()
	if _, err := st.DBForTest().ExecContext(ctx, `UPDATE sessions SET auto_checkpoint_enabled = 1 WHERE id = ?`, sessionID); err != nil {
		t.Fatalf("enable auto checkpoint: %v", err)
	}
}

func prepareServerIdleGeneration(t *testing.T, ctx context.Context, st *store.Store, cfg config.Config, ownerUUID, sessionID string) store.GenerationAllocation {
	t.Helper()
	now := time.Now().UTC()
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: sessionID,
		Owner:     store.GenerationLeaseOwner(ownerUUID),
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    serverTestAllocatorConfig(cfg, "claude"),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	artifacts := testGenerationArtifacts()
	if err := st.RecordGenerationRuntimeArtifactDigests(ctx, allocation.GenerationID, store.GenerationRuntimeArtifactDigests{
		ControlManifestDigest:          artifacts.ManifestDigest,
		ProjectedControlManifestDigest: artifacts.ProjectedManifestDigest,
		BundleDigest:                   artifacts.BundleDigest,
		RuntimeConfigDigest:            artifacts.RuntimeConfigDigest,
		SpecDigest:                     artifacts.SpecDigest,
		RunscVersion:                   artifacts.RunscVersion,
	}); err != nil {
		t.Fatalf("record runtime artifacts: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, sessionID, allocation.GenerationID, allocation.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark generation live: %v", err)
	}
	if err := st.UpdateSessionStatusAndActivity(ctx, sessionID, string(sessionstate.RunningIdle), nil, now.Add(-time.Minute)); err != nil {
		t.Fatalf("mark session idle: %v", err)
	}
	return allocation
}

func markServerGenerationCheckpointed(t *testing.T, ctx context.Context, st *store.Store, sessionID, generationID string, now time.Time) {
	t.Helper()
	formattedNow := now.UTC().Format(time.RFC3339Nano)
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'checkpointed',
    checkpoint_created_at = ?,
    checkpoint_network_profile_id = network_profile_id,
    checkpoint_agent_runtime_profile_id = agent_runtime_profile_id,
    checkpoint_runsc_version = COALESCE(runsc_version, 'runsc test'),
    checkpoint_runsc_platform = COALESCE(runsc_platform, 'systrap'),
    checkpoint_bundle_digest = 'bundle_digest',
    checkpoint_runtime_config_digest = 'runtime_config_digest',
    checkpoint_control_manifest_digest = COALESCE((
      SELECT control_manifest_digest
      FROM runtime_generation_resources
      WHERE runtime_generation_resources.generation_id = runtime_generations.generation_id
    ), 'manifest_digest'),
    lease_owner = NULL,
    lease_expires_at = NULL,
    last_seen_at = ?
WHERE generation_id = ?
  AND session_id = ?`, formattedNow, formattedNow, generationID, sessionID); err != nil {
		t.Fatalf("set checkpointed generation: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE network_profiles
SET allocation_state = 'reserved_checkpointed'
WHERE generation_id = ?
  AND session_id = ?`, generationID, sessionID); err != nil {
		t.Fatalf("reserve checkpointed network: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generation_resources
SET resource_state = 'reserved_checkpointed'
WHERE generation_id = ?`, generationID); err != nil {
		t.Fatalf("reserve checkpointed resources: %v", err)
	}
	if err := st.UpdateSessionStatus(ctx, sessionID, string(sessionstate.Checkpointed), nil); err != nil {
		t.Fatalf("set checkpointed session: %v", err)
	}
}

func newServerTestWatcher(t *testing.T, sessionsRoot string, st *store.Store, hub *events.Hub) *artifacts.Watcher {
	t.Helper()
	return artifacts.New(store.DataVolumeProvisionerConfig{
		SessionsRoot:   sessionsRoot,
		AgentHomesRoot: filepath.Join(t.TempDir(), "agent-homes"),
		EvidenceRoot:   filepath.Join(t.TempDir(), "volume-evidence"),
		RuntimeIdentity: store.RuntimeIdentity{
			UID: serverTestSandboxUID(),
			GID: serverTestSandboxGID(),
		},
	}, st, hub, slog.Default())
}

func testServerConfig(dir string) config.Config {
	return config.Config{
		SessionsRoot:     filepath.Join(dir, "sessions"),
		SessionRetention: time.Hour,
		MaxSessions:      10,
		DefaultAgent:     "claude",
		Claude: config.ClaudeConfig{
			ProxyBindURL:               "http://0.0.0.0:8082",
			SandboxBaseURL:             "http://harness-model-proxy.internal:8082",
			Model:                      "sonnet",
			OutputFormat:               "stream-json",
			DisableNonessentialTraffic: true,
		},
		Phase7: config.Phase7Config{
			RunDir: filepath.Join(dir, "run"),
			Network: config.NetworkConfig{
				CIDRPool: config.CIDRPrefix{Prefix: netip.MustParsePrefix("10.241.0.0/29")},
				Egress: config.EgressConfig{
					DorisFEHosts: []string{"172.16.0.138"},
					DorisBEHosts: []string{"172.16.0.139"},
					DorisPorts:   []int{9030},
					DNSPolicy:    config.DNSPolicyHostnamesOnly,
				},
			},
			Bridge: config.BridgeConfig{
				LeaseTTL:          config.Duration{Duration: time.Minute},
				HeartbeatInterval: config.Duration{Duration: 10 * time.Millisecond},
				PollInterval:      config.Duration{Duration: 10 * time.Millisecond},
				AckStartedGrace:   config.Duration{Duration: 90 * time.Second},
				ReconnectGrace:    config.Duration{Duration: 30 * time.Second},
			},
			Events: config.EventsConfig{
				RetentionWindow:        config.Duration{Duration: time.Hour},
				RetentionRows:          1_000,
				EmitOutputBatchMaxRows: 64,
				EmitOutputBatchMaxAge:  config.Duration{Duration: 100 * time.Millisecond},
			},
			Reaper: config.ReaperConfig{
				FailedRetention: config.Duration{Duration: 0},
			},
			SandboxIdentity: config.SandboxIdentity{
				UID: serverTestSandboxUID(),
				GID: serverTestSandboxGID(),
			},
		},
	}
}

func serverTestSandboxUID() int {
	uid := os.Getuid()
	if uid > 0 {
		return uid
	}
	return 65534
}

func serverTestSandboxGID() int {
	gid := os.Getgid()
	if gid > 0 {
		return gid
	}
	return 65534
}

func TestResourceAllocatorConfigUsesHostOnlyClaudeCredentials(t *testing.T) {
	cfg := testServerConfig(t.TempDir())
	cfg.Claude.SandboxBaseURL = "http://harness-model-proxy.internal:8082"
	srv := &Server{cfg: cfg}

	claudeConfig := srv.resourceAllocatorConfig("claude")
	if !claudeConfig.ProviderCredentialsHostOnly {
		t.Fatalf("claude allocator should keep provider credentials host-only: %+v", claudeConfig)
	}
	if claudeConfig.SandboxModelProxyBaseURL != "http://harness-model-proxy.internal:8082" {
		t.Fatalf("claude sandbox model proxy base url = %q", claudeConfig.SandboxModelProxyBaseURL)
	}
	if claudeConfig.SandboxUID != cfg.Phase7.SandboxIdentity.UID ||
		claudeConfig.SandboxGID != cfg.Phase7.SandboxIdentity.GID {
		t.Fatalf("claude allocator sandbox identity = %+v", claudeConfig)
	}

	shellConfig := srv.resourceAllocatorConfig("sh")
	if shellConfig.ProviderCredentialsHostOnly || shellConfig.SandboxModelProxyBaseURL != "http://harness-model-proxy.internal:8082" {
		t.Fatalf("shell allocator should not request host-only provider credentials: %+v", shellConfig)
	}
}

func serverTestAllocatorConfig(cfg config.Config, agent string) store.ResourceAllocatorConfig {
	outputFormat := cfg.Claude.OutputFormat
	if agent == "sh" {
		outputFormat = "shell_pty"
	}
	return store.ResourceAllocatorConfig{
		RunDir:                      cfg.Phase7.RunDir,
		CIDRPool:                    cfg.Phase7.Network.CIDRPool.Prefix,
		EgressDorisFEHosts:          cfg.Phase7.Network.Egress.DorisFEHosts,
		EgressDorisBEHosts:          cfg.Phase7.Network.Egress.DorisBEHosts,
		EgressDorisPorts:            cfg.Phase7.Network.Egress.DorisPorts,
		EgressDNSPolicy:             string(cfg.Phase7.Network.Egress.DNSPolicy),
		HostProxyBindURL:            cfg.Claude.ProxyBindURL,
		ProxyPort:                   8082,
		Agent:                       agent,
		AgentModel:                  cfg.Claude.Model,
		AgentOutputFormat:           outputFormat,
		DisableNonessentialTraffic:  cfg.Claude.DisableNonessentialTraffic,
		SandboxUID:                  cfg.Phase7.SandboxIdentity.UID,
		SandboxGID:                  cfg.Phase7.SandboxIdentity.GID,
		SandboxSupplementalGIDs:     cfg.Phase7.SandboxIdentity.SupplementalGIDs,
		ProviderCredentialsHostOnly: agent == "claude",
		SandboxModelProxyBaseURL:    cfg.Claude.SandboxBaseURL,
	}
}

func createServerGenerationFilesystem(t *testing.T, details store.RuntimeGenerationDetails) {
	t.Helper()
	for _, path := range []string{details.CheckpointPath, details.ControlDirPath, details.BundleDirPath, details.BridgeDirPath, details.LogDirPath} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatalf("create generation filesystem path %s: %v", path, err)
		}
		if err := os.WriteFile(filepath.Join(path, ".keep"), []byte("keep"), 0o644); err != nil {
			t.Fatalf("write generation filesystem marker %s: %v", path, err)
		}
	}
}

func waitForSessionStatus(t *testing.T, ctx context.Context, st *store.Store, sessionID, want string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		got, err := st.GetSession(ctx, sessionID)
		if err != nil {
			t.Fatalf("get session: %v", err)
		}
		if got.Status == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	got, err := st.GetSession(ctx, sessionID)
	if err != nil {
		t.Fatalf("get final session: %v", err)
	}
	data, _ := json.Marshal(got)
	t.Fatalf("session did not reach %s: %s", want, data)
}

func waitForGenerationResourceStates(t *testing.T, ctx context.Context, st *store.Store, generationID, wantNetwork, wantResource string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		var networkState, resourceState string
		if err := st.DBForTest().QueryRowContext(ctx, `
SELECT n.allocation_state, r.resource_state
FROM network_profiles n
JOIN runtime_generation_resources r ON r.generation_id = n.generation_id
WHERE n.generation_id = ?`, generationID).Scan(&networkState, &resourceState); err != nil {
			t.Fatalf("query generation resource states: %v", err)
		}
		if networkState == wantNetwork && resourceState == wantResource {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	var networkState, resourceState string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT n.allocation_state, r.resource_state
FROM network_profiles n
JOIN runtime_generation_resources r ON r.generation_id = n.generation_id
WHERE n.generation_id = ?`, generationID).Scan(&networkState, &resourceState); err != nil {
		t.Fatalf("query final generation resource states: %v", err)
	}
	t.Fatalf("generation %s resource states did not reach %s/%s: network=%s resource=%s", generationID, wantNetwork, wantResource, networkState, resourceState)
}

func waitForCheckpointRequests(t *testing.T, ctx context.Context, rt *recordingRuntime, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if got := len(rt.checkpointRequests()); got >= want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context canceled before checkpoint requests reached %d", want)
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Fatalf("checkpoint requests=%d want at least %d", len(rt.checkpointRequests()), want)
}

func waitForGenerationStatus(t *testing.T, ctx context.Context, st *store.Store, generationID, want string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		var got string
		if err := st.DBForTest().QueryRowContext(ctx, `SELECT status FROM runtime_generations WHERE generation_id = ?`, generationID).Scan(&got); err != nil {
			t.Fatalf("query generation status: %v", err)
		}
		if got == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context canceled before generation reached %s", want)
		case <-time.After(10 * time.Millisecond):
		}
	}
	var got string
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT status FROM runtime_generations WHERE generation_id = ?`, generationID).Scan(&got); err != nil {
		t.Fatalf("query final generation status: %v", err)
	}
	t.Fatalf("generation did not reach %s: got %s", want, got)
}

func waitForGenerationLeaseAfter(t *testing.T, ctx context.Context, st *store.Store, generationID string, after time.Time) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		var raw string
		if err := st.DBForTest().QueryRowContext(ctx, `SELECT lease_expires_at FROM runtime_generations WHERE generation_id = ?`, generationID).Scan(&raw); err != nil {
			t.Fatalf("query generation lease: %v", err)
		}
		if got, err := time.Parse(time.RFC3339Nano, raw); err == nil && got.After(after) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context canceled before generation lease renewed")
		case <-time.After(5 * time.Millisecond):
		}
	}
	var raw string
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT lease_expires_at FROM runtime_generations WHERE generation_id = ?`, generationID).Scan(&raw); err != nil {
		t.Fatalf("query final generation lease: %v", err)
	}
	t.Fatalf("generation %s lease was not renewed after %s: got %s", generationID, after, raw)
}

func waitForEventIDs(t *testing.T, ctx context.Context, st *store.Store, want []int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		records, err := st.ListEvents(ctx, store.ListEventsParams{})
		if err != nil {
			t.Fatalf("list events: %v", err)
		}
		got := make([]int64, 0, len(records))
		for _, record := range records {
			got = append(got, record.EventID)
		}
		if int64sEqual(got, want) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context canceled before retained events reached %v", want)
		case <-time.After(10 * time.Millisecond):
		}
	}
	records, err := st.ListEvents(context.Background(), store.ListEventsParams{})
	if err != nil {
		t.Fatalf("list final events: %v", err)
	}
	got := make([]int64, 0, len(records))
	for _, record := range records {
		got = append(got, record.EventID)
	}
	t.Fatalf("event ids=%v want %v", got, want)
}

func int64sEqual(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func createServerRunningProxyTurn(t *testing.T, ctx context.Context, st *store.Store, cfg config.Config, ownerUUID, dir, sessionID string, now time.Time) (store.GenerationAllocation, int64, string) {
	t.Helper()
	createServerTestSession(t, ctx, st, dir, sessionID, string(sessionstate.RunningActive), now, nil)
	owner := store.GenerationLeaseOwner(ownerUUID)
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: sessionID,
		Owner:     owner,
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    serverTestAllocatorConfig(cfg, "claude"),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, sessionID, allocation.GenerationID, allocation.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark resources live: %v", err)
	}
	turnID, err := st.EnqueueTurn(ctx, sessionID, "proxy observed turn", now.Add(2*time.Second))
	if err != nil {
		t.Fatalf("enqueue turn: %v", err)
	}
	grant, ok, err := st.ClaimNextTurn(ctx, store.ClaimNextTurnParams{
		SessionID:    sessionID,
		GenerationID: allocation.GenerationID,
		Owner:        allocation.Owner,
		RequestID:    "claim_" + sessionID,
		LeaseTTL:     time.Minute,
		Now:          now.Add(3 * time.Second),
	})
	if err != nil || !ok || grant.TurnID != turnID {
		t.Fatalf("claim setup: ok=%v grant=%+v err=%v", ok, grant, err)
	}
	sandboxSourceIP := serverSandboxSourceIPForGeneration(t, ctx, st, allocation.GenerationID)
	if _, err := st.AckTurnStarted(ctx, store.AckStartedParams{
		SessionID:       sessionID,
		GenerationID:    allocation.GenerationID,
		TurnID:          turnID,
		Owner:           allocation.Owner,
		SandboxSourceIP: sandboxSourceIP,
		LeaseTTL:        time.Minute,
		Now:             now.Add(4 * time.Second),
	}); err != nil {
		t.Fatalf("ack turn started: %v", err)
	}
	return allocation, turnID, sandboxSourceIP
}

func serverSandboxSourceIPForGeneration(t *testing.T, ctx context.Context, st *store.Store, generationID string) string {
	t.Helper()
	var sandboxCIDR string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT sandbox_ip_cidr
FROM network_profiles
WHERE generation_id = ?`, generationID).Scan(&sandboxCIDR); err != nil {
		t.Fatalf("query sandbox ip cidr: %v", err)
	}
	parts := strings.SplitN(sandboxCIDR, "/", 2)
	if len(parts) != 2 || parts[0] == "" {
		t.Fatalf("unexpected sandbox ip cidr: %q", sandboxCIDR)
	}
	return parts[0]
}

func waitForHubEvent(t *testing.T, ch <-chan events.Event, eventType string) events.Event {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		select {
		case event := <-ch:
			if event.Type == eventType {
				return event
			}
		case <-deadline:
			t.Fatalf("timeout waiting for hub event %s", eventType)
		}
	}
}

func assertPublicSessionJSONOmitsHostFields(t *testing.T, payload []byte) {
	t.Helper()
	body := string(payload)
	for _, field := range []string{
		`"workspace"`,
		`"agent_home_path"`,
		`"restore_id"`,
		`"checkpoint_path"`,
		`"claude_session_uuid"`,
	} {
		if strings.Contains(body, field) {
			t.Fatalf("public session payload exposed host-only field %s: %s", field, body)
		}
	}
}

func assertContains(t *testing.T, value, want string) {
	t.Helper()
	if !strings.Contains(value, want) {
		t.Fatalf("expected %q to contain %q", value, want)
	}
}

func drainHasEvent(ch <-chan events.Event, eventType string) bool {
	for {
		select {
		case event := <-ch:
			if event.Type == eventType {
				return true
			}
		default:
			return false
		}
	}
}

func TestDownloadArtifactRejectsSymlinkEscape(t *testing.T) {
	dir := t.TempDir()
	srv, _, workspace := newArtifactDownloadServer(t, "sess_1")
	outside := filepath.Join(dir, "outside.txt")
	if err := os.WriteFile(outside, []byte("secret"), 0o644); err != nil {
		t.Fatalf("write outside file: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(workspace, "outside.txt")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/artifacts/sess_1/outside.txt", nil)
	rec := httptest.NewRecorder()

	srv.downloadArtifact(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected status 403, got %d body %s", rec.Code, rec.Body.String())
	}
}

func TestDownloadArtifactRejectsSymlinkDirectory(t *testing.T) {
	dir := t.TempDir()
	srv, _, workspace := newArtifactDownloadServer(t, "sess_1")
	outsideDir := filepath.Join(dir, "outside")
	if err := os.MkdirAll(outsideDir, 0o755); err != nil {
		t.Fatalf("mkdir outside: %v", err)
	}
	if err := os.WriteFile(filepath.Join(outsideDir, "secret.txt"), []byte("secret"), 0o644); err != nil {
		t.Fatalf("write outside file: %v", err)
	}
	if err := os.Symlink(outsideDir, filepath.Join(workspace, "linked")); err != nil {
		t.Fatalf("symlink dir: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/artifacts/sess_1/linked/secret.txt", nil)
	rec := httptest.NewRecorder()

	srv.downloadArtifact(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected status 403, got %d body %s", rec.Code, rec.Body.String())
	}
}

func TestDownloadArtifactRejectsTraversal(t *testing.T) {
	dir := t.TempDir()
	srv := &Server{cfg: config.Config{SessionsRoot: dir}}
	req := httptest.NewRequest(http.MethodGet, "/artifacts/sess_1/../outside.txt", nil)
	rec := httptest.NewRecorder()

	srv.downloadArtifact(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d body %s", rec.Code, rec.Body.String())
	}
}
