package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/config"
	"harness-platform/orchestrator/internal/events"
	"harness-platform/orchestrator/internal/sessionstate"
	"harness-platform/orchestrator/internal/store"
)

func TestGetQuotaReportsSessionAndPoolCeilings(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	createServerTestSession(t, ctx, st, dir, "sess_quota", string(sessionstate.Created), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	cfg.MaxSessions = 3
	cfg.Harness.MaxSessions = 3
	cfg.Harness.Network.CIDRPool = config.CIDRPrefix{Prefix: netip.MustParsePrefix("10.242.0.0/29")}
	modelAccessAllowed := true
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: "sess_quota",
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config: store.ResourceAllocatorConfig{
			RunDir:                      cfg.Harness.RunDir,
			CIDRPool:                    cfg.Harness.Network.CIDRPool.Prefix,
			EgressDorisFEHosts:          cfg.Harness.Network.Egress.DorisFEHosts,
			EgressDorisBEHosts:          cfg.Harness.Network.Egress.DorisBEHosts,
			EgressDorisPorts:            cfg.Harness.Network.Egress.DorisPorts,
			EgressDNSPolicy:             string(cfg.Harness.Network.Egress.DNSPolicy),
			HostProxyBindURL:            cfg.ModelProxy.BindURL,
			ProxyPort:                   cfg.ModelProxy.BindPort,
			DriverID:                    "claude_code",
			Model:                       "sonnet",
			OutputFormat:                "stream-json",
			SandboxUID:                  cfg.Harness.SandboxIdentity.UID,
			SandboxGID:                  cfg.Harness.SandboxIdentity.GID,
			ModelAccessAllowed:          &modelAccessAllowed,
			ProviderCredentialsHostOnly: true,
			SandboxModelProxyBaseURL:    cfg.ModelProxy.SandboxBaseURL,
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
	cfg.Harness.Network.CIDRPool = config.CIDRPrefix{Prefix: netip.MustParsePrefix("10.242.0.0/30")}
	createServerTestSession(t, ctx, st, dir, "sess_pool_used", string(sessionstate.Created), time.Now().UTC(), nil)
	target := createServerTestSession(t, ctx, st, dir, "sess_pool_target", string(sessionstate.Created), time.Now().UTC(), nil)
	if _, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: "sess_pool_used",
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, "claude_code"),
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
