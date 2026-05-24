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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/artifacts"
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

func TestSendMessageAllocatesGenerationAndWrites7ALedger(t *testing.T) {
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
	waitForSessionStatus(t, ctx, st, session.ID, string(sessionstate.RunningIdle))

	var generations, networkRows, resourceRows, completedTurns int
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
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM turns WHERE session_id = ? AND status = 'completed'`, session.ID).Scan(&completedTurns); err != nil {
		t.Fatalf("count turns: %v", err)
	}
	if generations != 1 || networkRows != 1 || resourceRows != 1 || completedTurns != 1 {
		t.Fatalf("unexpected ledger rows: generations=%d network=%d resources=%d turns=%d", generations, networkRows, resourceRows, completedTurns)
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
	waitForSessionStatus(t, ctx, st, session.ID, string(sessionstate.RunningIdle))
	if got := atomic.LoadInt64(&instantRuntimePrepareCalls); got != 1 {
		t.Fatalf("first turn prepare calls=%d want 1", got)
	}

	req = httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"second"}`))
	rec = httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected second status 202, got %d body %s", rec.Code, rec.Body.String())
	}
	waitForSessionStatus(t, ctx, st, session.ID, string(sessionstate.RunningIdle))
	if got := atomic.LoadInt64(&instantRuntimePrepareCalls); got != 1 {
		t.Fatalf("active generation should reuse prepared artifacts, prepare calls=%d", got)
	}
	var turns int
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM turns WHERE session_id = ? AND status = 'completed'`, session.ID).Scan(&turns); err != nil {
		t.Fatalf("count turns: %v", err)
	}
	if turns != 2 {
		t.Fatalf("completed turns=%d want 2", turns)
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

func TestRunSessionFailureMarksGenerationFailedAndReclaimable(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_runtime_fail", string(sessionstate.Created), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
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
	turnID, err := st.Start7ATurn(ctx, session.ID, allocation.GenerationID, allocation.Owner, "hello", time.Now().UTC())
	if err != nil {
		t.Fatalf("start turn: %v", err)
	}
	details, err := st.GetRuntimeGenerationDetails(ctx, session.ID, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get generation details: %v", err)
	}
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

	srv.runSession(ctx, session, "hello", allocation.GenerationID, allocation.Owner, turnID, details, runtime.GenerationArtifacts{}, false)

	var generationStatus, errorClass, networkState, resourceState, turnStatus string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, COALESCE(g.error_class, ''), n.allocation_state, r.resource_state, t.status
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
JOIN turns t ON t.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&generationStatus, &errorClass, &networkState, &resourceState, &turnStatus); err != nil {
		t.Fatalf("query generation state: %v", err)
	}
	if generationStatus != "failed" ||
		errorClass != "probe_failed_pre_start" ||
		networkState != "reclaimable" ||
		resourceState != "reclaimable" ||
		turnStatus != "failed" {
		t.Fatalf("unexpected failed generation state: generation=%s class=%s network=%s resource=%s turn=%s", generationStatus, errorClass, networkState, resourceState, turnStatus)
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
