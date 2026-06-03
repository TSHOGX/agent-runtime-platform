package bridge

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/store"
)

func TestProcessorRejectsUnknownNativeEventPayload(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	if err := EnsureLayout(root); err != nil {
		t.Fatalf("ensure layout: %v", err)
	}
	turnID := int64(9)
	processor := &Processor{
		Store:                   storeFailure{},
		Owner:                   "owner",
		LeaseTTL:                time.Minute,
		RequiredProtocolVersion: RequiredProtocolVersionV2,
		RequiredTurnInputSchema: RequiredTurnInputRunTurn,
	}
	writeOutbox(t, ctx, root, Envelope{
		MessageID:    "msg_native_unknown",
		RequestID:    "req_native_unknown",
		Type:         TypeEmitOutput,
		SessionID:    "sess_native",
		GenerationID: "gen_native",
		TurnID:       &turnID,
		Payload: json.RawMessage(`{
			"output_sequence":1,
			"stream":"stdout",
			"payload":{"schema":"harness_native_events_v1","event":{"type":"agent.future","payload":{}}}
		}`),
	})
	if err := processor.ProcessOnce(ctx, root); err != nil {
		t.Fatalf("process unknown native event: %v", err)
	}
	response := assertSingleInboxResponse(t, root, TypeError, "req_native_unknown")
	var payload errorPayload
	if err := json.Unmarshal(response.Payload, &payload); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if payload.ErrorClass != "bridge_protocol_error" || !strings.Contains(payload.Error, `unsupported native event type "agent.future"`) {
		t.Fatalf("unexpected native event error: %+v", payload)
	}
}

func TestProcessorFailsGenerationOnOutputSequenceMismatch(t *testing.T) {
	ctx := context.Background()
	st, owner := openBridgeStore(t, ctx)
	sessionID := "sess_output_mismatch"
	createBridgeSession(t, ctx, st, sessionID)
	allocation, details := allocateBridgeGeneration(t, ctx, st, owner, sessionID)
	turnID, err := st.EnqueueTurn(ctx, sessionID, "stream mismatch", time.Now().UTC())
	if err != nil {
		t.Fatalf("enqueue turn: %v", err)
	}
	if _, ok, err := st.ClaimNextTurn(ctx, store.ClaimNextTurnParams{
		SessionID:    sessionID,
		GenerationID: allocation.GenerationID,
		Owner:        allocation.Owner,
		RequestID:    "direct_claim",
		LeaseTTL:     time.Minute,
		Now:          time.Now().UTC(),
	}); err != nil || !ok {
		t.Fatalf("claim setup: ok=%v err=%v", ok, err)
	}
	processor := &Processor{
		Store:                   st,
		Owner:                   allocation.Owner,
		LeaseTTL:                time.Minute,
		RequiredProtocolVersion: RequiredProtocolVersionV2,
		RequiredTurnInputSchema: RequiredTurnInputRunTurn,
	}
	writeOutbox(t, ctx, details.BridgeDirPath, Envelope{
		MessageID:    "msg_started",
		RequestID:    "req_started",
		Type:         TypeAckTurnStarted,
		SessionID:    sessionID,
		GenerationID: allocation.GenerationID,
		TurnID:       &turnID,
		Payload:      json.RawMessage(`{"sandbox_source_ip":"10.240.0.2"}`),
	})
	writeOutbox(t, ctx, details.BridgeDirPath, emitOutputEnvelope(sessionID, allocation.GenerationID, turnID, 1, "first"))
	if err := processor.ProcessOnce(ctx, details.BridgeDirPath); err != nil {
		t.Fatalf("process first output: %v", err)
	}

	writeOutbox(t, ctx, details.BridgeDirPath, emitOutputEnvelope(sessionID, allocation.GenerationID, turnID, 1, "changed"))
	if err := processor.ProcessOnce(ctx, details.BridgeDirPath); err != nil {
		t.Fatalf("process mismatched duplicate: %v", err)
	}
	response := assertSingleInboxResponse(t, details.BridgeDirPath, TypeError, "req_output_changed_001")
	var errorResponse errorPayload
	if err := json.Unmarshal(response.Payload, &errorResponse); err != nil {
		t.Fatalf("decode duplicate error: %v", err)
	}
	if errorResponse.ErrorClass != "bridge_protocol_error" || !strings.Contains(errorResponse.Error, "duplicate output_sequence mismatch") {
		t.Fatalf("unexpected duplicate error response: %+v", errorResponse)
	}

	var generationStatus, generationErrorClass, turnStatus, turnErrorClass string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, COALESCE(g.error_class, ''), t.status, COALESCE(t.error_class, '')
FROM runtime_generations g
JOIN turns t ON t.generation_id = g.generation_id
WHERE g.generation_id = ?
  AND t.id = ?`, allocation.GenerationID, turnID).Scan(&generationStatus, &generationErrorClass, &turnStatus, &turnErrorClass); err != nil {
		t.Fatalf("query failed generation: %v", err)
	}
	if generationStatus != "failed" || generationErrorClass != "bridge_output_sequence_mismatch" || turnStatus != "failed" || turnErrorClass != "bridge_output_sequence_mismatch" {
		t.Fatalf("unexpected failure state: generation=%s/%s turn=%s/%s", generationStatus, generationErrorClass, turnStatus, turnErrorClass)
	}
	var outputCount int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM events
WHERE generation_id = ?
  AND turn_id = ?
  AND type = ?`, allocation.GenerationID, turnID, TypeEmitOutput).Scan(&outputCount); err != nil {
		t.Fatalf("query output count: %v", err)
	}
	if outputCount != 1 {
		t.Fatalf("mismatched duplicate should not append another output event, got %d", outputCount)
	}
}

func TestProcessorFailsGenerationWhenCompletionCommitFailsAfterOutput(t *testing.T) {
	ctx := context.Background()
	st, owner := openBridgeStore(t, ctx)
	sessionID := "sess_completion_failure"
	createBridgeSession(t, ctx, st, sessionID)
	allocation, details := allocateBridgeGeneration(t, ctx, st, owner, sessionID)
	turnID, err := st.EnqueueTurn(ctx, sessionID, "completion fails", time.Now().UTC())
	if err != nil {
		t.Fatalf("enqueue turn: %v", err)
	}
	if _, ok, err := st.ClaimNextTurn(ctx, store.ClaimNextTurnParams{
		SessionID:    sessionID,
		GenerationID: allocation.GenerationID,
		Owner:        allocation.Owner,
		RequestID:    "direct_claim",
		LeaseTTL:     time.Minute,
		Now:          time.Now().UTC(),
	}); err != nil || !ok {
		t.Fatalf("claim setup: ok=%v err=%v", ok, err)
	}
	processor := &Processor{
		Store:                   st,
		Owner:                   allocation.Owner,
		LeaseTTL:                time.Minute,
		RequiredProtocolVersion: RequiredProtocolVersionV2,
		RequiredTurnInputSchema: RequiredTurnInputRunTurn,
	}
	writeOutbox(t, ctx, details.BridgeDirPath, Envelope{
		MessageID:    "msg_started",
		RequestID:    "req_started",
		Type:         TypeAckTurnStarted,
		SessionID:    sessionID,
		GenerationID: allocation.GenerationID,
		TurnID:       &turnID,
		Payload:      json.RawMessage(`{"sandbox_source_ip":"10.240.0.2"}`),
	})
	writeOutbox(t, ctx, details.BridgeDirPath, emitOutputEnvelope(sessionID, allocation.GenerationID, turnID, 1, "prefix"))
	if err := processor.ProcessOnce(ctx, details.BridgeDirPath); err != nil {
		t.Fatalf("process output prefix: %v", err)
	}

	writeOutbox(t, ctx, details.BridgeDirPath, Envelope{
		MessageID:    "msg_bad_completion",
		RequestID:    "req_bad_completion",
		Type:         TypeAckTurnCompleted,
		SessionID:    sessionID,
		GenerationID: allocation.GenerationID,
		TurnID:       &turnID,
		Payload: json.RawMessage(`{
			"status":"completed",
			"driver_state_update":{
				"driver_id":"claude_code",
				"previous_state_digest":"sha256:stale",
				"state_payload":{
					"schema_version":1,
					"driver_id":"claude_code",
					"state_kind":"claude_session",
					"claude_session_uuid":"driver-state-test-session",
					"initialized":true,
					"last_completed_turn_id":"1"
				},
				"state_digest":"sha256:not-the-canonical-digest",
				"state_version":2
			}
		}`),
	})
	if err := processor.ProcessOnce(ctx, details.BridgeDirPath); err != nil {
		t.Fatalf("process bad completion: %v", err)
	}
	assertInboxEmpty(t, details.BridgeDirPath)

	var generationStatus, generationErrorClass, turnStatus, turnErrorClass string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, COALESCE(g.error_class, ''), t.status, COALESCE(t.error_class, '')
FROM runtime_generations g
JOIN turns t ON t.generation_id = g.generation_id
WHERE g.generation_id = ?
  AND t.id = ?`, allocation.GenerationID, turnID).Scan(&generationStatus, &generationErrorClass, &turnStatus, &turnErrorClass); err != nil {
		t.Fatalf("query failed completion state: %v", err)
	}
	if generationStatus != "failed" || generationErrorClass != "driver_state_validation_failed" || turnStatus != "failed" || turnErrorClass != "driver_state_validation_failed" {
		t.Fatalf("unexpected completion failure state: generation=%s/%s turn=%s/%s", generationStatus, generationErrorClass, turnStatus, turnErrorClass)
	}
	var outputCount, completionCount int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT
  SUM(CASE WHEN type = ? THEN 1 ELSE 0 END),
  SUM(CASE WHEN type = ? THEN 1 ELSE 0 END)
FROM events
WHERE generation_id = ?
  AND turn_id = ?`, TypeEmitOutput, TypeAckTurnCompleted, allocation.GenerationID, turnID).Scan(&outputCount, &completionCount); err != nil {
		t.Fatalf("query event counts: %v", err)
	}
	if outputCount != 1 || completionCount != 0 {
		t.Fatalf("event counts after completion failure: output=%d completion=%d", outputCount, completionCount)
	}
}
