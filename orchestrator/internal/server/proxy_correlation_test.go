package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/config"
	"harness-platform/orchestrator/internal/events"
	"harness-platform/orchestrator/internal/sessionstate"
	"harness-platform/orchestrator/internal/store"
)

func TestProxyCorrelationUnixSocketPublishesDurableEvents(t *testing.T) {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "hp-proxy-")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
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

	publicReq := httptest.NewRequest(http.MethodPost, "/internal/proxy/requests/start", strings.NewReader(fmt.Sprintf(`{"sandbox_source_ip":%q,"proxy_request_id":"proxy_public"}`, sandboxSourceIP)))
	publicRec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(publicRec, publicReq)
	if publicRec.Code != http.StatusNotFound {
		t.Fatalf("public proxy route status=%d body=%s", publicRec.Code, publicRec.Body.String())
	}

	directReq := httptest.NewRequest(http.MethodPost, "/internal/proxy/requests/start", strings.NewReader(fmt.Sprintf(`{"sandbox_source_ip":%q,"proxy_request_id":"proxy_direct"}`, sandboxSourceIP)))
	directRec := httptest.NewRecorder()
	srv.ProxyCorrelationRoutes().ServeHTTP(directRec, directReq)
	if directRec.Code != http.StatusForbidden {
		t.Fatalf("proxy route without peer credentials status=%d body=%s", directRec.Code, directRec.Body.String())
	}

	listener, socketPath, err := srv.ListenProxyCorrelation()
	if err != nil {
		t.Fatalf("listen proxy correlation: %v", err)
	}
	assertProxyCorrelationSocketPermissions(t, socketPath, cfg.Harness.ProxyServiceIdentity.GID)
	proxyServer := srv.ProxyCorrelationServer()
	errCh := make(chan error, 1)
	go func() { errCh <- proxyServer.Serve(listener) }()
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := proxyServer.Shutdown(shutdownCtx); err != nil {
			t.Errorf("shutdown proxy server: %v", err)
		}
		_ = os.Remove(socketPath)
		if err := <-errCh; err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("proxy server stopped: %v", err)
		}
	})

	client := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
		},
	}}
	clientPost := func(path, body string) (int, []byte) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://proxy.internal"+path, strings.NewReader(body))
		if err != nil {
			t.Fatalf("build proxy request: %v", err)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("proxy request %s: %v", path, err)
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read proxy response: %v", err)
		}
		return resp.StatusCode, data
	}

	startStatus, startBody := clientPost("/internal/proxy/requests/start", fmt.Sprintf(`{
		"sandbox_source_ip":%q,
		"proxy_request_id":"proxy_http_1",
		"upstream_model":"claude-sonnet",
		"upstream_base_url":"https://api.anthropic.test"
	}`, sandboxSourceIP))
	if startStatus != http.StatusOK {
		t.Fatalf("start status=%d body=%s", startStatus, string(startBody))
	}
	var startResp struct {
		SessionID       string `json:"session_id"`
		TurnID          int64  `json:"turn_id"`
		GenerationID    string `json:"generation_id"`
		RequestSequence int64  `json:"request_sequence"`
		EventID         int64  `json:"event_id"`
		Replayed        bool   `json:"replayed"`
	}
	if err := json.Unmarshal(startBody, &startResp); err != nil {
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

	finishStatus, finishBody := clientPost("/internal/proxy/requests/finish", `{
		"proxy_request_id":"proxy_http_1",
		"http_status":200,
		"upstream_total_latency_ms":321,
		"retry_count":0
	}`)
	if finishStatus != http.StatusOK {
		t.Fatalf("finish status=%d body=%s", finishStatus, string(finishBody))
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
	if err := json.Unmarshal(finishBody, &finishResp); err != nil {
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

	unknownStatus, unknownBody := clientPost("/internal/proxy/requests/finish", `{"proxy_request_id":"proxy_missing"}`)
	if unknownStatus != http.StatusOK {
		t.Fatalf("unknown finish status=%d body=%s", unknownStatus, string(unknownBody))
	}
	var unknownResp map[string]string
	if err := json.Unmarshal(unknownBody, &unknownResp); err != nil {
		t.Fatalf("decode unknown finish response: %v", err)
	}
	if unknownResp["status"] != "stale_unknown_request" {
		t.Fatalf("unexpected unknown finish response: %v", unknownResp)
	}
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
		Config:    serverTestAllocatorConfig(cfg, "claude_code"),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, sessionID, allocation.GenerationID, allocation.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark resources live: %v", err)
	}
	createServerRuntimeResourceLive(t, ctx, st, sessionID, allocation, ownerUUID, "host-proxy", now.Add(2*time.Second))
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

func assertProxyCorrelationSocketPermissions(t *testing.T, socketPath string, proxyServiceGID int) {
	t.Helper()
	for _, check := range []struct {
		name string
		path string
		mode os.FileMode
	}{
		{name: "socket root", path: filepath.Dir(socketPath), mode: 0o750},
		{name: "socket", path: socketPath, mode: 0o660},
	} {
		info, err := os.Stat(check.path)
		if err != nil {
			t.Fatalf("stat proxy correlation %s: %v", check.name, err)
		}
		if info.Mode().Perm() != check.mode {
			t.Fatalf("proxy correlation %s mode=%#o want %#o", check.name, info.Mode().Perm(), check.mode)
		}
		if os.Geteuid() != 0 {
			continue
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			t.Fatalf("proxy correlation %s stat type = %T", check.name, info.Sys())
		}
		if stat.Uid != 0 || stat.Gid != uint32(proxyServiceGID) {
			t.Fatalf("proxy correlation %s ownership=%d:%d want 0:%d", check.name, stat.Uid, stat.Gid, proxyServiceGID)
		}
	}
}
