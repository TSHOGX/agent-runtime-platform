package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestProxyRequestStartResolvesActiveContextAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	now := time.Now().UTC()
	allocation, turnID := createRunningProxyTurn(t, ctx, st, owner.UUID, "sess_proxy_start", "10.240.0.2", now)

	started, err := st.StartProxyRequest(ctx, StartProxyRequestParams{
		SandboxSourceIP: "10.240.0.2",
		ProxyRequestID:  "proxy_start_1",
		UpstreamModel:   "claude-sonnet",
		UpstreamBaseURL: "https://api.anthropic.test",
		Now:             now.Add(5 * time.Second),
	})
	if err != nil {
		t.Fatalf("start proxy request: %v", err)
	}
	if started.EventID == 0 || started.Replayed || started.SessionID != "sess_proxy_start" ||
		started.GenerationID != allocation.GenerationID || started.TurnID != turnID || started.RequestSequence != 1 {
		t.Fatalf("unexpected start result: %+v allocation=%+v turn=%d", started, allocation, turnID)
	}

	replayed, err := st.StartProxyRequest(ctx, StartProxyRequestParams{
		SandboxSourceIP: "10.240.0.2",
		ProxyRequestID:  "proxy_start_1",
		UpstreamModel:   "ignored-on-replay",
		Now:             now.Add(6 * time.Second),
	})
	if err != nil {
		t.Fatalf("replay proxy request start: %v", err)
	}
	if !replayed.Replayed || replayed.EventID != started.EventID || replayed.RequestSequence != 1 {
		t.Fatalf("unexpected replay result: %+v want event %d sequence 1", replayed, started.EventID)
	}

	second, err := st.StartProxyRequest(ctx, StartProxyRequestParams{
		SandboxSourceIP: "10.240.0.2",
		ProxyRequestID:  "proxy_start_2",
		Now:             now.Add(7 * time.Second),
	})
	if err != nil {
		t.Fatalf("start second proxy request: %v", err)
	}
	if second.RequestSequence != 2 || second.EventID <= started.EventID {
		t.Fatalf("unexpected second start result: %+v first=%+v", second, started)
	}

	var nextSequence int64
	if err := st.db.QueryRowContext(ctx, `
SELECT next_request_sequence
FROM active_model_request_contexts
WHERE sandbox_source_ip = '10.240.0.2'`).Scan(&nextSequence); err != nil {
		t.Fatalf("query next request sequence: %v", err)
	}
	if nextSequence != 3 {
		t.Fatalf("next_request_sequence=%d want 3", nextSequence)
	}

	var startEvents int
	if err := st.db.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM events
WHERE proxy_request_id = 'proxy_start_1'
  AND type = 'proxy.request.started'`).Scan(&startEvents); err != nil {
		t.Fatalf("count start events: %v", err)
	}
	if startEvents != 1 {
		t.Fatalf("duplicate start wrote %d events, want 1", startEvents)
	}

	record, ok, err := st.GetEvent(ctx, started.EventID)
	if err != nil || !ok {
		t.Fatalf("get start event: ok=%v err=%v", ok, err)
	}
	if record.Type != "proxy.request.started" || record.ProxyRequestID != "proxy_start_1" ||
		record.SessionID != "sess_proxy_start" || record.GenerationID != allocation.GenerationID ||
		record.TurnID == nil || *record.TurnID != turnID {
		t.Fatalf("unexpected start event record: %+v", record)
	}
	var payload struct {
		ProxyRequestID  string `json:"proxy_request_id"`
		RequestSequence int64  `json:"request_sequence"`
		CorrelationMode string `json:"correlation_mode"`
		UpstreamModel   string `json:"upstream_model"`
		UpstreamBaseURL string `json:"upstream_base_url"`
	}
	if err := json.Unmarshal(record.Payload, &payload); err != nil {
		t.Fatalf("decode start payload: %v", err)
	}
	if payload.ProxyRequestID != "proxy_start_1" || payload.RequestSequence != 1 ||
		payload.CorrelationMode != "source_ip" || payload.UpstreamModel != "claude-sonnet" ||
		payload.UpstreamBaseURL != "https://api.anthropic.test" {
		t.Fatalf("unexpected start payload: %+v", payload)
	}
}

func TestProxyRequestStartRejectsExpiredContext(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	now := time.Now().UTC()
	createRunningProxyTurn(t, ctx, st, owner.UUID, "sess_proxy_expired", "10.240.0.3", now)

	_, err := st.StartProxyRequest(ctx, StartProxyRequestParams{
		SandboxSourceIP: "10.240.0.3",
		ProxyRequestID:  "proxy_expired",
		Now:             now.Add(2 * time.Minute),
	})
	if !errors.Is(err, ErrProxyContextUnavailable) {
		t.Fatalf("expired context err=%v want ErrProxyContextUnavailable", err)
	}
}

func TestProxyRequestFinishUsesDurableStartEvent(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	now := time.Now().UTC()
	allocation, turnID := createRunningProxyTurn(t, ctx, st, owner.UUID, "sess_proxy_finish", "10.240.0.4", now)

	if _, err := st.StartProxyRequest(ctx, StartProxyRequestParams{
		SandboxSourceIP: "10.240.0.4",
		ProxyRequestID:  "proxy_finish_ok",
		Now:             now.Add(5 * time.Second),
	}); err != nil {
		t.Fatalf("start ok request: %v", err)
	}
	if _, err := st.StartProxyRequest(ctx, StartProxyRequestParams{
		SandboxSourceIP: "10.240.0.4",
		ProxyRequestID:  "proxy_finish_timeout",
		Now:             now.Add(6 * time.Second),
	}); err != nil {
		t.Fatalf("start timeout request: %v", err)
	}
	if _, err := st.StartProxyRequest(ctx, StartProxyRequestParams{
		SandboxSourceIP: "10.240.0.4",
		ProxyRequestID:  "proxy_finish_invalid",
		Now:             now.Add(7 * time.Second),
	}); err != nil {
		t.Fatalf("start invalid request: %v", err)
	}
	if _, err := st.CompleteTurn(ctx, CompleteTurnParams{
		SessionID:      "sess_proxy_finish",
		GenerationID:   allocation.GenerationID,
		TurnID:         turnID,
		Owner:          allocation.Owner,
		TerminalStatus: "completed",
		Now:            now.Add(8 * time.Second),
	}); err != nil {
		t.Fatalf("complete turn: %v", err)
	}

	var activeContexts int
	if err := st.db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM active_model_request_contexts WHERE generation_id = ?`, allocation.GenerationID).Scan(&activeContexts); err != nil {
		t.Fatalf("count active contexts: %v", err)
	}
	if activeContexts != 0 {
		t.Fatalf("context should be deleted after completion, got %d", activeContexts)
	}

	status := int64(200)
	totalLatency := int64(345)
	completed, err := st.FinishProxyRequest(ctx, FinishProxyRequestParams{
		ProxyRequestID:         "proxy_finish_ok",
		HTTPStatus:             &status,
		UpstreamTotalLatencyMS: &totalLatency,
		Now:                    now.Add(9 * time.Second),
	})
	if err != nil {
		t.Fatalf("finish completed request: %v", err)
	}
	if completed.EventType != "proxy.request.completed" || completed.SessionID != "sess_proxy_finish" ||
		completed.GenerationID != allocation.GenerationID || completed.TurnID != turnID || completed.EventID == 0 {
		t.Fatalf("unexpected completed result: %+v", completed)
	}

	replayed, err := st.FinishProxyRequest(ctx, FinishProxyRequestParams{
		ProxyRequestID: "proxy_finish_ok",
		HTTPStatus:     &status,
		Now:            now.Add(10 * time.Second),
	})
	if err != nil {
		t.Fatalf("replay finish request: %v", err)
	}
	if !replayed.Replayed || replayed.EventID != completed.EventID || replayed.EventType != completed.EventType {
		t.Fatalf("unexpected replay finish result: %+v completed=%+v", replayed, completed)
	}

	failed, err := st.FinishProxyRequest(ctx, FinishProxyRequestParams{
		ProxyRequestID: "proxy_finish_timeout",
		ErrorClass:     "timeout",
		TimeoutKind:    "first_byte",
		Error:          "upstream first byte deadline exceeded",
		Now:            now.Add(11 * time.Second),
	})
	if err != nil {
		t.Fatalf("finish failed request: %v", err)
	}
	if failed.EventType != "proxy.request.failed" {
		t.Fatalf("failed event type=%s want proxy.request.failed", failed.EventType)
	}

	record, ok, err := st.GetEvent(ctx, failed.EventID)
	if err != nil || !ok {
		t.Fatalf("get failed event: ok=%v err=%v", ok, err)
	}
	if record.ProxyRequestID != "proxy_finish_timeout" || !strings.Contains(string(record.Payload), `"timeout_kind":"first_byte"`) {
		t.Fatalf("unexpected failed event: %+v payload=%s", record, string(record.Payload))
	}

	_, err = st.FinishProxyRequest(ctx, FinishProxyRequestParams{
		ProxyRequestID: "proxy_missing",
		Now:            now.Add(12 * time.Second),
	})
	if !errors.Is(err, ErrProxyRequestUnknown) {
		t.Fatalf("missing finish err=%v want ErrProxyRequestUnknown", err)
	}

	_, err = st.FinishProxyRequest(ctx, FinishProxyRequestParams{
		ProxyRequestID: "proxy_finish_invalid",
		ErrorClass:     "network",
		TimeoutKind:    "first_byte",
		Now:            now.Add(13 * time.Second),
	})
	if err == nil || !strings.Contains(err.Error(), "timeout_kind requires error_class timeout") {
		t.Fatalf("invalid timeout kind err=%v", err)
	}
}

func createRunningProxyTurn(t *testing.T, ctx context.Context, st *Store, ownerUUID, sessionID, sandboxSourceIP string, now time.Time) (GenerationAllocation, int64) {
	t.Helper()
	cfg := testAllocatorConfig(t)
	createStoreSession(t, ctx, st, sessionID)
	owner := GenerationLeaseOwner(ownerUUID)
	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: sessionID,
		Owner:     owner,
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    cfg,
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
	grant, ok, err := st.ClaimNextTurn(ctx, ClaimNextTurnParams{
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
	if _, err := st.AckTurnStarted(ctx, AckStartedParams{
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
