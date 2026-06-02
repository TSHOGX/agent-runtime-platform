package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/config"
	"harness-platform/orchestrator/internal/events"
	"harness-platform/orchestrator/internal/runtime"
	"harness-platform/orchestrator/internal/sessionstate"
	"harness-platform/orchestrator/internal/store"
)

func TestCreateSessionRejectsRemovedAgentInput(t *testing.T) {
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
			DefaultAgent:     "claude_code",
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
	if !strings.Contains(rec.Body.String(), "agent input is no longer supported") {
		t.Fatalf("expected removed agent input error, got %s", rec.Body.String())
	}
}

func TestCreateSessionModeMapping(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		wantStatus int
		wantMode   string
		wantDriver string
		wantError  string
	}{
		{name: "omitted body", wantStatus: http.StatusBadRequest, wantError: "mode is required"},
		{name: "empty object", body: `{}`, wantStatus: http.StatusBadRequest, wantError: "mode is required"},
		{name: "empty mode", body: `{"mode":" "}`, wantStatus: http.StatusBadRequest, wantError: "mode is required"},
		{name: "agent mode", body: `{"mode":"agent"}`, wantStatus: http.StatusCreated, wantMode: "agent", wantDriver: "claude_code"},
		{name: "shell mode", body: `{"mode":"shell"}`, wantStatus: http.StatusCreated, wantMode: "shell", wantDriver: "sh"},
		{name: "unknown mode", body: `{"mode":"pi"}`, wantStatus: http.StatusBadRequest, wantError: "unsupported mode"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
			if err != nil {
				t.Fatalf("open store: %v", err)
			}
			t.Cleanup(func() { _ = st.Close() })
			cfg := testServerConfig(dir)
			srv := &Server{
				cfg:     cfg,
				store:   st,
				runtime: runtime.New(runtime.Config{}),
				watcher: newServerTestWatcher(t, cfg.SessionsRoot, st, events.NewHub()),
				hub:     events.NewHub(),
				log:     slog.Default(),
			}
			var body io.Reader
			if tc.body != "" {
				body = strings.NewReader(tc.body)
			}
			req := httptest.NewRequest(http.MethodPost, "/api/sessions", body)
			rec := httptest.NewRecorder()
			srv.createSession(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status=%d want %d body=%s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if tc.wantStatus != http.StatusCreated {
				if !strings.Contains(rec.Body.String(), tc.wantError) {
					t.Fatalf("expected error %q, got %s", tc.wantError, rec.Body.String())
				}
				var count int
				if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM sessions`).Scan(&count); err != nil {
					t.Fatalf("count sessions: %v", err)
				}
				if count != 0 {
					t.Fatalf("failed create should not persist sessions, got %d", count)
				}
				return
			}
			var created struct {
				ID        string `json:"id"`
				Mode      string `json:"mode"`
				ModeLabel string `json:"mode_label"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if created.Mode != tc.wantMode || created.ModeLabel != modeLabel(tc.wantMode) {
				t.Fatalf("public mode=%s label=%s want %s/%s", created.Mode, created.ModeLabel, tc.wantMode, modeLabel(tc.wantMode))
			}
			var driverID, storedMode string
			if err := st.DBForTest().QueryRowContext(ctx, `SELECT driver_id, mode FROM sessions WHERE id = ?`, created.ID).Scan(&driverID, &storedMode); err != nil {
				t.Fatalf("read stored session selector: %v", err)
			}
			if driverID != tc.wantDriver || storedMode != tc.wantMode {
				t.Fatalf("stored driver/mode=%s/%s want %s/%s", driverID, storedMode, tc.wantDriver, tc.wantMode)
			}
		})
	}
}

func TestCreateSessionAgentModeRejectsShellDefault(t *testing.T) {
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
			DefaultAgent:     "sh",
		},
		store:   st,
		runtime: runtime.New(runtime.Config{}),
		watcher: newServerTestWatcher(t, dir, st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	req := httptest.NewRequest(http.MethodPost, "/api/sessions", strings.NewReader(`{"mode":"agent"}`))
	rec := httptest.NewRecorder()
	srv.createSession(rec, req)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "agent mode unavailable") {
		t.Fatalf("expected unavailable agent mode, got status=%d body=%s", rec.Code, rec.Body.String())
	}
	var count int
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM sessions`).Scan(&count); err != nil {
		t.Fatalf("count sessions: %v", err)
	}
	if count != 0 {
		t.Fatalf("failed create should not persist sessions, got %d", count)
	}
}

func TestResolveAgentModeRequiresExplicitDefaultAgent(t *testing.T) {
	dir := t.TempDir()
	cfg := testServerConfig(dir)
	cfg.DefaultAgent = ""
	srv := &Server{cfg: cfg}

	if _, err := srv.resolveModeDeployment("agent"); err == nil || err.code != "default_unavailable" {
		t.Fatalf("expected missing default agent to make agent mode unavailable, got %v", err)
	}
}

func TestResolveModeDeploymentRejectsEmptyMode(t *testing.T) {
	srv := &Server{cfg: testServerConfig(t.TempDir())}

	if _, err := srv.resolveModeDeployment(""); err == nil || err.code != "unsupported_mode" || err.message != "unsupported mode" {
		t.Fatalf("expected empty mode to be unsupported, got %v", err)
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
	cfg := testServerConfig(dir)
	cfg.MaxSessions = 1
	cfg.Harness.MaxSessions = 1

	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: runtime.New(runtime.Config{}),
		watcher: newServerTestWatcher(t, cfg.SessionsRoot, st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}

	req := httptest.NewRequest(http.MethodPost, "/api/sessions", strings.NewReader(`{"mode":"agent"}`))
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
	cfg.Harness.MaxSessions = 1
	volumeConfig, err := serverDataVolumeConfigForTest(cfg)
	if err != nil {
		t.Fatalf("data volume config: %v", err)
	}
	now := time.Now().UTC()
	oldSession := store.Session{
		ID:        "sess_retained",
		UserID:    labUserID,
		Status:    string(sessionstate.Created),
		DriverID:  "claude_code",
		Mode:      store.ModeForDriver("claude_code"),
		CreatedAt: now,
		UpdatedAt: now,
	}
	oldWorkspacePath := filepath.Join(cfg.SessionsRoot, oldSession.ID)
	if err := os.MkdirAll(oldWorkspacePath, 0o755); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := st.CreateSession(ctx, oldSession); err != nil {
		t.Fatalf("create retained session: %v", err)
	}
	oldDriverHome, err := st.ProvisionSessionDriverHome(ctx, store.ProvisionSessionDriverHomeParams{
		SessionID: oldSession.ID,
		Driver:    oldSession.DriverID,
		Config:    volumeConfig,
		Now:       now,
	})
	if err != nil {
		t.Fatalf("provision driver home: %v", err)
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

	req := httptest.NewRequest(http.MethodPost, "/api/sessions", strings.NewReader(`{"mode":"agent"}`))
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

	createReq := httptest.NewRequest(http.MethodPost, "/api/sessions", strings.NewReader(`{"mode":"agent"}`))
	createRec := httptest.NewRecorder()
	srv.createSession(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("expected create after close, got %d body %s", createRec.Code, createRec.Body.String())
	}

	closed, err := st.GetSession(ctx, oldSession.ID)
	if err != nil {
		t.Fatalf("get closed session: %v", err)
	}
	if closed.Status != string(sessionstate.Destroyed) {
		t.Fatalf("closed session should preserve terminal state: %+v", closed)
	}
	if _, err := os.Stat(oldWorkspacePath); err != nil {
		t.Fatalf("workspace should remain after close: %v", err)
	}
	if _, err := os.Stat(oldDriverHome.HostPath); err != nil {
		t.Fatalf("agent home should remain after close: %v", err)
	}
	retainedDriverHome, err := st.GetSessionDriverHomeVolume(ctx, oldSession.ID, oldSession.DriverID)
	if err != nil {
		t.Fatalf("get retained driver home: %v", err)
	}
	if retainedDriverHome.HostPath != oldDriverHome.HostPath {
		t.Fatalf("driver home row should be retained: got=%+v want=%+v", retainedDriverHome, oldDriverHome)
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
		Config:    serverTestAllocatorConfig(cfg, session.DriverID),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, allocation.GenerationID, allocation.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("mark generation live: %v", err)
	}
	createServerRuntimeResourceLive(t, ctx, st, session.ID, allocation, owner.UUID, mustRuntimeResourceHostID(t), time.Now().UTC())
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
	runscID := serverRunscContainerID(t, ctx, st, session.ID, allocation.GenerationID)
	if len(destroyIDs) != 1 || destroyIDs[0] != runscID {
		t.Fatalf("destroy session should tear down runsc container id %q, got %+v", runscID, destroyIDs)
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

func TestCreateSessionUsesNullExpiryWhenSessionRetentionDisabled(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	cfg := testServerConfig(dir)
	cfg.SessionRetention = 0
	cfg.Harness.SessionRetention = config.Duration{Duration: 0}

	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: runtime.New(runtime.Config{}),
		watcher: newServerTestWatcher(t, cfg.SessionsRoot, st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}

	req := httptest.NewRequest(http.MethodPost, "/api/sessions", strings.NewReader(`{"mode":"agent"}`))
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

func TestCreateSessionDefersWorkspaceProvisioning(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	cfg := testServerConfig(dir)
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: runtime.New(runtime.Config{}),
		watcher: newServerTestWatcher(t, cfg.SessionsRoot, st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}

	req := httptest.NewRequest(http.MethodPost, "/api/sessions", strings.NewReader(`{"mode":"agent"}`))
	rec := httptest.NewRecorder()
	srv.createSession(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d body %s", rec.Code, rec.Body.String())
	}

	var created struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	session, err := st.GetSession(ctx, created.ID)
	if err != nil {
		t.Fatalf("get created session: %v", err)
	}
	workspacePath := filepath.Join(cfg.SessionsRoot, session.ID)
	if _, err := os.Stat(workspacePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("session create should defer workspace provisioning, stat err=%v path=%s", err, workspacePath)
	}
	var workspaceRows int
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM session_workspaces WHERE session_id = ?`, session.ID).Scan(&workspaceRows); err != nil {
		t.Fatalf("count workspace evidence rows: %v", err)
	}
	if workspaceRows != 0 {
		t.Fatalf("session create should not synthesize workspace evidence rows, got %d", workspaceRows)
	}
}
