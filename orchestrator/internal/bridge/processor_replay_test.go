package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/store"
)

func TestProcessorReplayAfterCommitBeforeUnlinkIsIdempotent(t *testing.T) {
	ctx := context.Background()
	st, owner := openBridgeStore(t, ctx)
	sessionID := "sess_unlink_replay"
	createBridgeSession(t, ctx, st, sessionID)
	allocation, details := allocateBridgeGeneration(t, ctx, st, owner, sessionID)
	turnID, err := st.EnqueueTurn(ctx, sessionID, "run", time.Now().UTC())
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
	started := Envelope{
		MessageID:    "msg_started",
		RequestID:    "req_started",
		Type:         TypeAckTurnStarted,
		SessionID:    sessionID,
		GenerationID: allocation.GenerationID,
		TurnID:       &turnID,
		Payload:      json.RawMessage(`{"sandbox_source_ip":"10.240.0.2"}`),
	}
	output := Envelope{
		MessageID:    "msg_output",
		RequestID:    "req_output",
		Type:         TypeEmitOutput,
		SessionID:    sessionID,
		GenerationID: allocation.GenerationID,
		TurnID:       &turnID,
		Payload:      json.RawMessage(`{"output_sequence":1,"stream":"stdout","payload":{"line":"ok"}}`),
	}
	writeOutbox(t, ctx, details.BridgeDirPath, started)
	writeOutbox(t, ctx, details.BridgeDirPath, output)
	if err := processor.ProcessOnce(ctx, details.BridgeDirPath); err != nil {
		t.Fatalf("process first delivery: %v", err)
	}
	writeOutbox(t, ctx, details.BridgeDirPath, started)
	writeOutbox(t, ctx, details.BridgeDirPath, output)
	if err := processor.ProcessOnce(ctx, details.BridgeDirPath); err != nil {
		t.Fatalf("process replay delivery: %v", err)
	}

	var eventCount int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM events
WHERE session_id = ?
  AND generation_id = ?
  AND turn_id = ?`, sessionID, allocation.GenerationID, turnID).Scan(&eventCount); err != nil {
		t.Fatalf("event count: %v", err)
	}
	if eventCount != 2 {
		t.Fatalf("event count after replay=%d want 2", eventCount)
	}
	outbox, err := OpenQueue(details.BridgeDirPath, OutboxDir)
	if err != nil {
		t.Fatalf("open outbox: %v", err)
	}
	files, err := outbox.ReadAll()
	if err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("outbox files after replay=%d want 0", len(files))
	}
}

func TestProcessorDedupesMidStreamOutputReplayUnderLoad(t *testing.T) {
	ctx := context.Background()
	st, owner := openBridgeStore(t, ctx)
	sessionID := "sess_stream_replay"
	createBridgeSession(t, ctx, st, sessionID)
	allocation, details := allocateBridgeGeneration(t, ctx, st, owner, sessionID)
	turnID, err := st.EnqueueTurn(ctx, sessionID, "stream lots", time.Now().UTC())
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
	if err := processor.ProcessOnce(ctx, details.BridgeDirPath); err != nil {
		t.Fatalf("process ack started: %v", err)
	}

	for seq := 1; seq <= 100; seq++ {
		writeOutbox(t, ctx, details.BridgeDirPath, emitOutputEnvelope(sessionID, allocation.GenerationID, turnID, seq, "first"))
	}
	if err := processor.ProcessOnce(ctx, details.BridgeDirPath); err != nil {
		t.Fatalf("process first output burst: %v", err)
	}

	writeOutbox(t, ctx, details.BridgeDirPath, Envelope{
		MessageID:    "msg_hello_reconnect",
		RequestID:    "req_hello_reconnect",
		Type:         TypeHello,
		SessionID:    sessionID,
		GenerationID: allocation.GenerationID,
		Payload:      bridgeHelloPayloadForTest("claude_code"),
	})
	if err := processor.ProcessOnce(ctx, details.BridgeDirPath); err != nil {
		t.Fatalf("process reconnect hello: %v", err)
	}
	response := assertSingleInboxResponse(t, details.BridgeDirPath, TypeHelloAck, "req_hello_reconnect")
	var ack helloAckPayload
	if err := json.Unmarshal(response.Payload, &ack); err != nil {
		t.Fatalf("decode hello ack: %v", err)
	}
	if got := ack.LastOutputSequenceByTurn[fmt.Sprint(turnID)]; got != 100 {
		t.Fatalf("hello ack last output sequence=%d want 100: %+v", got, ack.LastOutputSequenceByTurn)
	}

	for seq := 41; seq <= 100; seq++ {
		writeOutbox(t, ctx, details.BridgeDirPath, emitOutputEnvelope(sessionID, allocation.GenerationID, turnID, seq, "first"))
	}
	for seq := 101; seq <= 120; seq++ {
		writeOutbox(t, ctx, details.BridgeDirPath, emitOutputEnvelope(sessionID, allocation.GenerationID, turnID, seq, "replay"))
	}
	if err := processor.ProcessOnce(ctx, details.BridgeDirPath); err != nil {
		t.Fatalf("process replay output burst: %v", err)
	}
	assertInboxEmpty(t, details.BridgeDirPath)

	var outputCount, distinctSequences, minSequence, maxSequence int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*), COUNT(DISTINCT output_sequence), COALESCE(MIN(output_sequence), 0), COALESCE(MAX(output_sequence), 0)
FROM events
WHERE session_id = ?
  AND generation_id = ?
  AND turn_id = ?
  AND type = ?`, sessionID, allocation.GenerationID, turnID, TypeEmitOutput).Scan(&outputCount, &distinctSequences, &minSequence, &maxSequence); err != nil {
		t.Fatalf("query output events: %v", err)
	}
	if outputCount != 120 || distinctSequences != 120 || minSequence != 1 || maxSequence != 120 {
		t.Fatalf("unexpected output sequence set: count=%d distinct=%d min=%d max=%d", outputCount, distinctSequences, minSequence, maxSequence)
	}
}
