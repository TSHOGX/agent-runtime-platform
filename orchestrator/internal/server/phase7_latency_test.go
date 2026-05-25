//go:build phase7bench

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/artifacts"
	"harness-platform/orchestrator/internal/bridge"
	"harness-platform/orchestrator/internal/events"
	"harness-platform/orchestrator/internal/sessionstate"
)

func TestPhase7TurnStartLatencyGate(t *testing.T) {
	const budget = 50 * time.Millisecond

	ctx := context.Background()
	dir := t.TempDir()
	cfg := testServerConfig(dir)
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_phase7_latency", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	allocation := prepareServerIdleGeneration(t, ctx, st, cfg, owner.UUID, session.ID)
	details, err := st.GetRuntimeGenerationDetails(ctx, session.ID, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get generation details: %v", err)
	}
	if err := bridge.EnsureLayout(details.BridgeDirPath); err != nil {
		t.Fatalf("ensure bridge layout: %v", err)
	}

	srv := &Server{
		cfg:       cfg,
		store:     st,
		runtime:   instantRuntime{},
		watcher:   artifacts.New(filepath.Join(dir, "sessions"), st, events.NewHub(), slog.Default()),
		hub:       events.NewHub(),
		log:       slog.Default(),
		ownerUUID: owner.UUID,
	}
	processor := &bridge.Processor{
		Store:           st,
		Owner:           allocation.Owner,
		LeaseTTL:        cfg.Phase7.Bridge.LeaseTTL.Duration,
		AckStartedGrace: cfg.Phase7.Bridge.AckStartedGrace.Duration,
		AfterCommit:     srv.handleBridgeCommittedEnvelope,
	}
	outbox, err := bridge.OpenQueue(details.BridgeDirPath, bridge.OutboxDir)
	if err != nil {
		t.Fatalf("open outbox: %v", err)
	}
	process := func(envelope bridge.Envelope) {
		t.Helper()
		if _, err := outbox.Write(ctx, envelope); err != nil {
			t.Fatalf("write outbox %s: %v", envelope.Type, err)
		}
		if err := processor.ProcessOnce(ctx, details.BridgeDirPath); err != nil {
			t.Fatalf("process %s: %v", envelope.Type, err)
		}
	}

	process(bridge.Envelope{
		MessageID:    "msg_latency_hello",
		RequestID:    "req_latency_hello",
		Type:         bridge.TypeHello,
		SessionID:    session.ID,
		GenerationID: allocation.GenerationID,
	})
	process(bridge.Envelope{
		MessageID:    "msg_latency_probe",
		RequestID:    "req_latency_probe",
		Type:         bridge.TypeProbeNetwork,
		SessionID:    session.ID,
		GenerationID: allocation.GenerationID,
	})

	content := "phase7 latency gate " + time.Now().UTC().Format(time.RFC3339Nano)
	start := time.Now()
	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(fmt.Sprintf(`{"content":%q}`, content)))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("send message status=%d body=%s", rec.Code, rec.Body.String())
	}
	process(bridge.Envelope{
		MessageID:    "msg_latency_claim",
		RequestID:    "req_latency_claim",
		Type:         bridge.TypeClaimNextTurn,
		SessionID:    session.ID,
		GenerationID: allocation.GenerationID,
	})

	var turnID int64
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT id
FROM turns
WHERE session_id = ?
  AND content = ?
  AND status = 'leased'`, session.ID, content).Scan(&turnID); err != nil {
		t.Fatalf("query leased latency turn: %v", err)
	}
	ackPayload, err := json.Marshal(map[string]string{
		"sandbox_source_ip": serverSandboxSourceIPForGeneration(t, ctx, st, allocation.GenerationID),
	})
	if err != nil {
		t.Fatalf("marshal ack payload: %v", err)
	}
	process(bridge.Envelope{
		MessageID:    "msg_latency_ack_started",
		RequestID:    "req_latency_ack_started",
		Type:         bridge.TypeAckTurnStarted,
		SessionID:    session.ID,
		GenerationID: allocation.GenerationID,
		TurnID:       &turnID,
		Payload:      ackPayload,
	})
	elapsed := time.Since(start)

	var status string
	var ackStartedAt string
	var ackEvents int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT status, COALESCE(ack_started_at, '')
FROM turns
WHERE id = ?`, turnID).Scan(&status, &ackStartedAt); err != nil {
		t.Fatalf("query acked turn: %v", err)
	}
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM events
WHERE turn_id = ?
  AND generation_id = ?
  AND type = ?`, turnID, allocation.GenerationID, bridge.TypeAckTurnStarted).Scan(&ackEvents); err != nil {
		t.Fatalf("query ack event: %v", err)
	}
	if status != "running" || ackStartedAt == "" || ackEvents != 1 {
		t.Fatalf("turn start did not commit: status=%s ack_started_at=%q ack_events=%d", status, ackStartedAt, ackEvents)
	}
	if elapsed > budget {
		t.Fatalf("turn-start latency %s exceeded phase7 budget %s", elapsed, budget)
	}
}
