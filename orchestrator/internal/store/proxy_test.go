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
	allocation, turnID, sandboxSourceIP := createRunningProxyTurn(t, ctx, st, owner.UUID, "sess_proxy_start", now)

	started, err := st.StartProxyRequest(ctx, StartProxyRequestParams{
		SandboxSourceIP: sandboxSourceIP,
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
		SandboxSourceIP: sandboxSourceIP,
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
		SandboxSourceIP: sandboxSourceIP,
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
WHERE sandbox_source_ip = ?`, sandboxSourceIP).Scan(&nextSequence); err != nil {
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
	_, _, sandboxSourceIP := createRunningProxyTurn(t, ctx, st, owner.UUID, "sess_proxy_expired", now)

	_, err := st.StartProxyRequest(ctx, StartProxyRequestParams{
		SandboxSourceIP: sandboxSourceIP,
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
	allocation, turnID, sandboxSourceIP := createRunningProxyTurn(t, ctx, st, owner.UUID, "sess_proxy_finish", now)

	if _, err := st.StartProxyRequest(ctx, StartProxyRequestParams{
		SandboxSourceIP: sandboxSourceIP,
		ProxyRequestID:  "proxy_finish_ok",
		Now:             now.Add(5 * time.Second),
	}); err != nil {
		t.Fatalf("start ok request: %v", err)
	}
	if _, err := st.StartProxyRequest(ctx, StartProxyRequestParams{
		SandboxSourceIP: sandboxSourceIP,
		ProxyRequestID:  "proxy_finish_timeout",
		Now:             now.Add(6 * time.Second),
	}); err != nil {
		t.Fatalf("start timeout request: %v", err)
	}
	if _, err := st.StartProxyRequest(ctx, StartProxyRequestParams{
		SandboxSourceIP: sandboxSourceIP,
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

func TestProxyRequestFinishRecordsTimeoutObservabilityFields(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	now := time.Now().UTC()
	allocation, turnID, sandboxSourceIP := createRunningProxyTurn(t, ctx, st, owner.UUID, "sess_proxy_timeout_fields", now)

	timeoutKinds := []string{"connect", "first_byte", "total", "idle_stream"}
	for i, timeoutKind := range timeoutKinds {
		proxyRequestID := "proxy_timeout_" + timeoutKind
		if _, err := st.StartProxyRequest(ctx, StartProxyRequestParams{
			SandboxSourceIP: sandboxSourceIP,
			ProxyRequestID:  proxyRequestID,
			Now:             now.Add(time.Duration(5+i) * time.Second),
		}); err != nil {
			t.Fatalf("start %s request: %v", timeoutKind, err)
		}

		connectLatency := int64(10 + i)
		firstByteLatency := int64(20 + i)
		totalLatency := int64(30 + i)
		retryCount := int64(i)
		finished, err := st.FinishProxyRequest(ctx, FinishProxyRequestParams{
			ProxyRequestID:             proxyRequestID,
			ProxyConnectLatencyMS:      &connectLatency,
			UpstreamFirstByteLatencyMS: &firstByteLatency,
			UpstreamTotalLatencyMS:     &totalLatency,
			RetryCount:                 &retryCount,
			ErrorClass:                 "timeout",
			TimeoutKind:                timeoutKind,
			Error:                      "deadline exceeded",
			Now:                        now.Add(time.Duration(20+i) * time.Second),
		})
		if err != nil {
			t.Fatalf("finish %s request: %v", timeoutKind, err)
		}
		if finished.EventType != "proxy.request.failed" || finished.SessionID != "sess_proxy_timeout_fields" ||
			finished.GenerationID != allocation.GenerationID || finished.TurnID != turnID {
			t.Fatalf("unexpected finish result for %s: %+v", timeoutKind, finished)
		}

		record, ok, err := st.GetEvent(ctx, finished.EventID)
		if err != nil || !ok {
			t.Fatalf("get %s failed event: ok=%v err=%v", timeoutKind, ok, err)
		}
		var payload struct {
			ProxyRequestID             string `json:"proxy_request_id"`
			RequestSequence            int64  `json:"request_sequence"`
			ProxyConnectLatencyMS      int64  `json:"proxy_connect_latency_ms"`
			UpstreamFirstByteLatencyMS int64  `json:"upstream_first_byte_latency_ms"`
			UpstreamTotalLatencyMS     int64  `json:"upstream_total_latency_ms"`
			RetryCount                 int64  `json:"retry_count"`
			TimeoutKind                string `json:"timeout_kind"`
			ErrorClass                 string `json:"error_class"`
			Error                      string `json:"error"`
		}
		if err := json.Unmarshal(record.Payload, &payload); err != nil {
			t.Fatalf("decode %s failed payload: %v", timeoutKind, err)
		}
		if payload.ProxyRequestID != proxyRequestID ||
			payload.RequestSequence != int64(i+1) ||
			payload.ProxyConnectLatencyMS != connectLatency ||
			payload.UpstreamFirstByteLatencyMS != firstByteLatency ||
			payload.UpstreamTotalLatencyMS != totalLatency ||
			payload.RetryCount != retryCount ||
			payload.TimeoutKind != timeoutKind ||
			payload.ErrorClass != "timeout" ||
			payload.Error != "deadline exceeded" {
			t.Fatalf("unexpected %s failed payload: %+v", timeoutKind, payload)
		}
	}
}

func TestAckTurnStartedRejectsMismatchedSandboxSourceIP(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	now := time.Now().UTC()
	cfg := testAllocatorConfig(t)
	createStoreSession(t, ctx, st, "sess_proxy_ip_mismatch")
	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_proxy_ip_mismatch",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, "sess_proxy_ip_mismatch", allocation.GenerationID, allocation.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark resources live: %v", err)
	}
	turnID, err := st.EnqueueTurn(ctx, "sess_proxy_ip_mismatch", "bad source ip", now.Add(2*time.Second))
	if err != nil {
		t.Fatalf("enqueue turn: %v", err)
	}
	if grant, ok, err := st.ClaimNextTurn(ctx, ClaimNextTurnParams{
		SessionID:    "sess_proxy_ip_mismatch",
		GenerationID: allocation.GenerationID,
		Owner:        allocation.Owner,
		RequestID:    "claim_proxy_ip_mismatch",
		LeaseTTL:     time.Minute,
		Now:          now.Add(3 * time.Second),
	}); err != nil || !ok || grant.TurnID != turnID {
		t.Fatalf("claim setup: ok=%v grant=%+v err=%v", ok, grant, err)
	}

	_, err = st.AckTurnStarted(ctx, AckStartedParams{
		SessionID:       "sess_proxy_ip_mismatch",
		GenerationID:    allocation.GenerationID,
		TurnID:          turnID,
		Owner:           allocation.Owner,
		SandboxSourceIP: "10.240.0.99",
		LeaseTTL:        time.Minute,
		Now:             now.Add(4 * time.Second),
	})
	if err == nil || !strings.Contains(err.Error(), "sandbox_source_ip mismatch") {
		t.Fatalf("mismatched sandbox source ip err=%v, want mismatch", err)
	}

	var turnStatus string
	var contexts int
	if err := st.db.QueryRowContext(ctx, `SELECT status FROM turns WHERE id = ?`, turnID).Scan(&turnStatus); err != nil {
		t.Fatalf("query turn status: %v", err)
	}
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM active_model_request_contexts`).Scan(&contexts); err != nil {
		t.Fatalf("query active contexts: %v", err)
	}
	if turnStatus != "leased" || contexts != 0 {
		t.Fatalf("mismatched ack mutated state: turn=%s contexts=%d", turnStatus, contexts)
	}
}

func createRunningProxyTurn(t *testing.T, ctx context.Context, st *Store, ownerUUID, sessionID string, now time.Time) (GenerationAllocation, int64, string) {
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
	sandboxSourceIP := sandboxSourceIPForGeneration(t, ctx, st, allocation.GenerationID)
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
	return allocation, turnID, sandboxSourceIP
}

func sandboxSourceIPForGeneration(t *testing.T, ctx context.Context, st *Store, generationID string) string {
	t.Helper()
	var sandboxCIDR string
	if err := st.db.QueryRowContext(ctx, `
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
