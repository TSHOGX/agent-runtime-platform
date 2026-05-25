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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
			RunscNetwork:    "sandbox",
			CheckpointsRoot: filepath.Join(dir, "checkpoints"),
		},
		store: st,
		hub:   events.NewHub(),
		log:   slog.Default(),
	}
	if err := srv.MonitorIdleSessions(ctx); err != nil {
		t.Fatalf("monitor idle sessions: %v", err)
	}
	got, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if got.Status != string(sessionstate.RunningIdle) {
		t.Fatalf("want running_idle, got %s", got.Status)
	}
}

func TestReconcileCheckpointingSessionsMarksCompleteCheckpoint(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	now := time.Now().UTC()
	session := store.Session{
		ID:        "sess_complete",
		UserID:    "lab",
		Status:    string(sessionstate.Checkpointing),
		Agent:     "claude",
		Workspace: filepath.Join(dir, "sessions", "sess_complete"),
		RestoreID: "phase3-sess_complete",
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := st.CreateSession(ctx, session); err != nil {
		t.Fatalf("create session: %v", err)
	}
	checkpointPath := filepath.Join(dir, "checkpoints", session.ID)
	if err := os.MkdirAll(checkpointPath, 0o755); err != nil {
		t.Fatalf("create checkpoint dir: %v", err)
	}
	for _, name := range []string{"checkpoint.img", "pages.img", "pages_meta.img"} {
		if err := os.WriteFile(filepath.Join(checkpointPath, name), []byte("x"), 0o644); err != nil {
			t.Fatalf("write checkpoint file %s: %v", name, err)
		}
	}

	srv := &Server{
		cfg: config.Config{
			CheckpointsRoot: filepath.Join(dir, "checkpoints"),
		},
		store: st,
		hub:   events.NewHub(),
		log:   slog.Default(),
	}

	if err := srv.reconcileCheckpointingSessions(ctx); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if got.Status != string(sessionstate.Checkpointed) {
		t.Fatalf("want checkpointed, got %s", got.Status)
	}
}

func TestReconcileCheckpointingSessionsRevertsIncompleteCheckpoint(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	now := time.Now().UTC()
	freshSession := store.Session{
		ID:        "sess_fresh",
		UserID:    "lab",
		Status:    string(sessionstate.Checkpointing),
		Agent:     "claude",
		Workspace: filepath.Join(dir, "sessions", "sess_fresh"),
		RestoreID: "phase3-sess_fresh",
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := st.CreateSession(ctx, freshSession); err != nil {
		t.Fatalf("create fresh session: %v", err)
	}
	if err := st.UpdateSessionStatus(ctx, freshSession.ID, string(sessionstate.Checkpointing), nil); err != nil {
		t.Fatalf("refresh checkpointing status: %v", err)
	}

	staleSession := store.Session{
		ID:        "sess_incomplete",
		UserID:    "lab",
		Status:    string(sessionstate.Checkpointing),
		Agent:     "claude",
		Workspace: filepath.Join(dir, "sessions", "sess_incomplete"),
		RestoreID: "phase3-sess_incomplete",
		CreatedAt: now.Add(-(checkpointTimeout + time.Minute)),
		UpdatedAt: now.Add(-(checkpointTimeout + time.Minute)),
	}
	if err := st.CreateSession(ctx, staleSession); err != nil {
		t.Fatalf("create stale session: %v", err)
	}

	srv := &Server{
		cfg: config.Config{
			CheckpointsRoot: filepath.Join(dir, "checkpoints"),
		},
		store: st,
		hub:   events.NewHub(),
		log:   slog.Default(),
	}

	if err := srv.reconcileCheckpointingSessions(ctx); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got, err := st.GetSession(ctx, freshSession.ID)
	if err != nil {
		t.Fatalf("get fresh session: %v", err)
	}
	if got.Status != string(sessionstate.Checkpointing) {
		t.Fatalf("fresh checkpointing session should be left alone, got %s", got.Status)
	}
	got, err = st.GetSession(ctx, staleSession.ID)
	if err != nil {
		t.Fatalf("get stale session: %v", err)
	}
	if got.Status != string(sessionstate.RunningIdle) {
		t.Fatalf("want running_idle, got %s", got.Status)
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
	return runtime.GenerationArtifacts{
		BundleDir:      "/tmp/bundle",
		SpecPath:       "/tmp/bundle/config.json",
		ManifestPath:   "/tmp/control/session.json",
		ManifestDigest: "digest",
		RunscVersion:   "runsc test",
	}, nil
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

func (instantRuntime) Checkpoint(context.Context, string) error {
	return nil
}

type recordingRuntime struct {
	mu              sync.Mutex
	prepareRequests []runtime.StartRequest
	startRequests   []runtime.StartRequest
}

func (r *recordingRuntime) PrepareGeneration(_ context.Context, req runtime.StartRequest) (runtime.GenerationArtifacts, error) {
	r.mu.Lock()
	r.prepareRequests = append(r.prepareRequests, req)
	r.mu.Unlock()
	return runtime.GenerationArtifacts{
		BundleDir:      "/tmp/bundle",
		SpecPath:       "/tmp/bundle/config.json",
		ManifestPath:   "/tmp/control/session.json",
		ManifestDigest: "digest",
		RunscVersion:   "runsc test",
	}, nil
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

func (r *recordingRuntime) Checkpoint(context.Context, string) error {
	return nil
}

func (r *recordingRuntime) requests() ([]runtime.StartRequest, []runtime.StartRequest) {
	r.mu.Lock()
	defer r.mu.Unlock()
	prepares := append([]runtime.StartRequest(nil), r.prepareRequests...)
	starts := append([]runtime.StartRequest(nil), r.startRequests...)
	return prepares, starts
}

type failingRuntime struct {
	prepareErr error
	err        error
}

func (f failingRuntime) PrepareGeneration(context.Context, runtime.StartRequest) (runtime.GenerationArtifacts, error) {
	if f.prepareErr != nil {
		return runtime.GenerationArtifacts{}, f.prepareErr
	}
	return runtime.GenerationArtifacts{}, nil
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

func (f failingRuntime) Checkpoint(context.Context, string) error {
	return nil
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
