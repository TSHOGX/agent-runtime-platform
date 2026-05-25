package server

import (
	"context"
	"encoding/json"
	"errors"
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
			SessionsRoot: dir,
			SessionTTL:   time.Hour,
			MaxSessions:  10,
			DefaultAgent: "claude",
		},
		store:   st,
		runtime: runtime.New(runtime.Config{}),
		watcher: artifacts.New(dir, st, events.NewHub(), slog.Default()),
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
			SessionsRoot: dir,
			SessionTTL:   time.Hour,
			MaxSessions:  1,
			DefaultAgent: "claude",
		},
		store:   st,
		runtime: runtime.New(runtime.Config{}),
		watcher: artifacts.New(dir, st, events.NewHub(), slog.Default()),
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
		watcher: artifacts.New(filepath.Join(dir, "sessions"), st, events.NewHub(), slog.Default()),
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
		watcher: artifacts.New(filepath.Join(dir, "sessions"), st, events.NewHub(), slog.Default()),
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
		SandboxSourceIP: "10.241.0.2",
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
		watcher: artifacts.New(filepath.Join(dir, "sessions"), st, events.NewHub(), slog.Default()),
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
		watcher: artifacts.New(filepath.Join(dir, "sessions"), st, events.NewHub(), slog.Default()),
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

	rt := &restoreFailoverRuntime{err: errors.New("checkpoint_runsc_version mismatch")}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: artifacts.New(filepath.Join(dir, "sessions"), st, events.NewHub(), slog.Default()),
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
		watcher: artifacts.New(filepath.Join(dir, "sessions"), st, events.NewHub(), slog.Default()),
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
		body["remaining_pool_slots"] != 1 ||
		body["effective_ceiling"] != 2 {
		t.Fatalf("unexpected quota body for allocation %s: %+v", allocation.GenerationID, body)
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
		watcher: artifacts.New(filepath.Join(dir, "sessions"), st, events.NewHub(), slog.Default()),
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
	var generationStatus, errorClass, networkState, resourceState, sessionStatus string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, COALESCE(g.error_class, ''), n.allocation_state, r.resource_state, s.status
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
JOIN sessions s ON s.active_generation_id = g.generation_id
WHERE g.session_id = ?`, session.ID).Scan(&generationStatus, &errorClass, &networkState, &resourceState, &sessionStatus); err != nil {
		t.Fatalf("query generation state: %v", err)
	}
	if generationStatus != "failed" ||
		errorClass != "probe_failed_pre_start" ||
		networkState != "reclaimable" ||
		resourceState != "reclaimable" ||
		sessionStatus != string(sessionstate.Failed) {
		t.Fatalf("unexpected failed generation state: generation=%s class=%s network=%s resource=%s session=%s", generationStatus, errorClass, networkState, resourceState, sessionStatus)
	}
	var turns int
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM turns WHERE session_id = ?`, session.ID).Scan(&turns); err != nil {
		t.Fatalf("count turns: %v", err)
	}
	if turns != 0 {
		t.Fatalf("runtime start failure should happen before turn creation, got %d turns", turns)
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
		watcher: artifacts.New(filepath.Join(dir, "sessions"), st, events.NewHub(), slog.Default()),
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
	var generationStatus, errorClass, networkState, resourceState, sessionStatus string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, COALESCE(g.error_class, ''), n.allocation_state, r.resource_state, s.status
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
JOIN sessions s ON s.active_generation_id = g.generation_id
WHERE g.session_id = ?`, session.ID).Scan(&generationStatus, &errorClass, &networkState, &resourceState, &sessionStatus); err != nil {
		t.Fatalf("query generation state: %v", err)
	}
	if generationStatus != "failed" ||
		errorClass != "probe_failed_pre_start" ||
		networkState != "reclaimable" ||
		resourceState != "reclaimable" ||
		sessionStatus != string(sessionstate.Failed) {
		t.Fatalf("unexpected failed generation state: generation=%s class=%s network=%s resource=%s session=%s", generationStatus, errorClass, networkState, resourceState, sessionStatus)
	}
	var turns int
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM turns WHERE session_id = ?`, session.ID).Scan(&turns); err != nil {
		t.Fatalf("count turns: %v", err)
	}
	if turns != 0 {
		t.Fatalf("prepare failure should happen before turn creation, got %d turns", turns)
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
		watcher: artifacts.New(filepath.Join(dir, "sessions"), st, events.NewHub(), slog.Default()),
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
		Payload:      json.RawMessage(`{"sandbox_source_ip":"10.240.0.2"}`),
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
		watcher: artifacts.New(filepath.Join(dir, "sessions"), st, hub, slog.Default()),
		hub:     hub,
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)
	done := make(chan error, 1)
	go func() {
		done <- srv.RunPhase7Maintenance(runCtx)
	}()
	waitForSessionStatus(t, runCtx, st, session.ID, string(sessionstate.RunningIdle))
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
	if !drainHasEvent(eventsCh, "session."+string(sessionstate.RunningIdle)) {
		t.Fatalf("missing running_idle hub event")
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
		watcher: artifacts.New(filepath.Join(dir, "sessions"), st, events.NewHub(), slog.Default()),
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
		watcher: artifacts.New(filepath.Join(dir, "sessions"), st, events.NewHub(), slog.Default()),
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
	allocation, turnID := createServerRunningProxyTurn(t, ctx, st, cfg, owner.UUID, dir, "sess_proxy_http", "10.240.0.2", now)

	hub := events.NewHub()
	eventsCh, cancelEvents := hub.Subscribe("sess_proxy_http")
	defer cancelEvents()
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: instantRuntime{},
		watcher: artifacts.New(filepath.Join(dir, "sessions"), st, hub, slog.Default()),
		hub:     hub,
		log:     slog.Default(),
	}

	blocked := httptest.NewRequest(http.MethodPost, "/internal/proxy/requests/start", strings.NewReader(`{"sandbox_source_ip":"10.240.0.2","proxy_request_id":"proxy_blocked"}`))
	blocked.RemoteAddr = "203.0.113.7:5000"
	blockedRec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(blockedRec, blocked)
	if blockedRec.Code != http.StatusForbidden {
		t.Fatalf("non-loopback proxy request status=%d body=%s", blockedRec.Code, blockedRec.Body.String())
	}

	startReq := httptest.NewRequest(http.MethodPost, "/internal/proxy/requests/start", strings.NewReader(`{
		"sandbox_source_ip":"10.240.0.2",
		"proxy_request_id":"proxy_http_1",
		"upstream_model":"claude-sonnet",
		"upstream_base_url":"https://api.anthropic.test"
	}`))
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
	dir := t.TempDir()
	sessionDir := filepath.Join(dir, "sess_1", "reports")
	if err := os.MkdirAll(sessionDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sessionDir, "summary.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("write artifact: %v", err)
	}

	srv := &Server{cfg: config.Config{SessionsRoot: dir}}
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

func (instantRuntime) Interrupt(string) error {
	return nil
}

func (instantRuntime) Checkpoint(context.Context, runtime.CheckpointRequest) error {
	return nil
}

type recordingRuntime struct {
	mu              sync.Mutex
	prepareRequests []runtime.StartRequest
	startRequests   []runtime.StartRequest
	checkpointReqs  []runtime.CheckpointRequest
	checkpointErr   error
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

func (r *recordingRuntime) Destroy(context.Context, string) error {
	return nil
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

type restoreFailoverRuntime struct {
	recordingRuntime
	err error
}

func (r *restoreFailoverRuntime) Start(_ context.Context, req runtime.StartRequest, _ func(runtime.Output)) runtime.Result {
	r.mu.Lock()
	r.startRequests = append(r.startRequests, req)
	r.mu.Unlock()
	if req.RestoreFromCheckpoint {
		return runtime.Result{Err: r.err}
	}
	return runtime.Result{ControlManifestDigest: req.PreparedArtifacts.ManifestDigest, RunscVersion: req.PreparedArtifacts.RunscVersion}
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

func testServerConfig(dir string) config.Config {
	return config.Config{
		SessionsRoot: filepath.Join(dir, "sessions"),
		SessionTTL:   time.Hour,
		MaxSessions:  10,
		DefaultAgent: "claude",
		Claude: config.ClaudeConfig{
			ProxyBindURL:               "http://0.0.0.0:8082",
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
		},
	}
}

func serverTestAllocatorConfig(cfg config.Config, agent string) store.ResourceAllocatorConfig {
	outputFormat := cfg.Claude.OutputFormat
	if agent == "sh" {
		outputFormat = "shell_pty"
	}
	return store.ResourceAllocatorConfig{
		RunDir:                     cfg.Phase7.RunDir,
		CIDRPool:                   cfg.Phase7.Network.CIDRPool.Prefix,
		EgressDorisFEHosts:         cfg.Phase7.Network.Egress.DorisFEHosts,
		EgressDorisBEHosts:         cfg.Phase7.Network.Egress.DorisBEHosts,
		EgressDorisPorts:           cfg.Phase7.Network.Egress.DorisPorts,
		EgressDNSPolicy:            string(cfg.Phase7.Network.Egress.DNSPolicy),
		HostProxyBindURL:           cfg.Claude.ProxyBindURL,
		ProxyPort:                  8082,
		Agent:                      agent,
		AgentModel:                 cfg.Claude.Model,
		AgentOutputFormat:          outputFormat,
		DisableNonessentialTraffic: cfg.Claude.DisableNonessentialTraffic,
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

func createServerRunningProxyTurn(t *testing.T, ctx context.Context, st *store.Store, cfg config.Config, ownerUUID, dir, sessionID, sandboxSourceIP string, now time.Time) (store.GenerationAllocation, int64) {
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
	return allocation, turnID
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
	sessionDir := filepath.Join(dir, "sess_1")
	if err := os.MkdirAll(sessionDir, 0o755); err != nil {
		t.Fatalf("mkdir session: %v", err)
	}
	outside := filepath.Join(dir, "outside.txt")
	if err := os.WriteFile(outside, []byte("secret"), 0o644); err != nil {
		t.Fatalf("write outside file: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(sessionDir, "outside.txt")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	srv := &Server{cfg: config.Config{SessionsRoot: dir}}
	req := httptest.NewRequest(http.MethodGet, "/artifacts/sess_1/outside.txt", nil)
	rec := httptest.NewRecorder()

	srv.downloadArtifact(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected status 403, got %d body %s", rec.Code, rec.Body.String())
	}
}

func TestDownloadArtifactRejectsSymlinkDirectory(t *testing.T) {
	dir := t.TempDir()
	sessionDir := filepath.Join(dir, "sess_1")
	outsideDir := filepath.Join(dir, "outside")
	if err := os.MkdirAll(sessionDir, 0o755); err != nil {
		t.Fatalf("mkdir session: %v", err)
	}
	if err := os.MkdirAll(outsideDir, 0o755); err != nil {
		t.Fatalf("mkdir outside: %v", err)
	}
	if err := os.WriteFile(filepath.Join(outsideDir, "secret.txt"), []byte("secret"), 0o644); err != nil {
		t.Fatalf("write outside file: %v", err)
	}
	if err := os.Symlink(outsideDir, filepath.Join(sessionDir, "linked")); err != nil {
		t.Fatalf("symlink dir: %v", err)
	}

	srv := &Server{cfg: config.Config{SessionsRoot: dir}}
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
