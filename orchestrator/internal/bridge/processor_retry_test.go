package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

// TestProcessorRetainsOutboxMessageWhenCompletionIsTransient asserts that a
// transient CompleteTurn error (e.g. "database is locked") is propagated so the
// outbox envelope is retained for retry, and that the generation is NOT failed.
func TestProcessorRetainsOutboxMessageWhenCompletionIsTransient(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	if err := EnsureLayout(root); err != nil {
		t.Fatalf("ensure layout: %v", err)
	}
	failGenCalled := false
	processor := &Processor{
		Store: storeFailure{
			completeErr:   errors.New("database is locked"),
			failGenCalled: &failGenCalled,
		},
		Owner:                   "owner",
		LeaseTTL:                time.Minute,
		RequiredProtocolVersion: RequiredProtocolVersionV2,
		RequiredTurnInputSchema: RequiredTurnInputRunTurn,
	}
	turnID := int64(1)
	writeOutbox(t, ctx, root, Envelope{
		MessageID:    "msg_complete",
		RequestID:    "req_complete",
		Type:         TypeAckTurnCompleted,
		SessionID:    "sess_retry",
		GenerationID: "gen_retry",
		TurnID:       &turnID,
		Payload:      json.RawMessage(`{"status":"completed"}`),
	})
	if err := processor.ProcessOnce(ctx, root); err == nil || !strings.Contains(err.Error(), "database is locked") {
		t.Fatalf("process transient completion err=%v, want database is locked", err)
	}
	if failGenCalled {
		t.Fatalf("failGeneration was called for a transient completion error; want retry without retiring the generation")
	}
	assertInboxEmpty(t, root)
	outbox, err := OpenQueue(root, OutboxDir)
	if err != nil {
		t.Fatalf("open outbox: %v", err)
	}
	files, err := outbox.ReadAll()
	if err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	if len(files) != 1 || files[0].Envelope.RequestID != "req_complete" {
		t.Fatalf("outbox after transient completion=%+v, want original message retained", files)
	}
}

func TestProcessorRetainsOutboxMessageWhenStoreApplyFails(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	if err := EnsureLayout(root); err != nil {
		t.Fatalf("ensure layout: %v", err)
	}
	processor := &Processor{
		Store: storeFailure{
			claimErr: errors.New("database is locked"),
		},
		Owner:                   "owner",
		LeaseTTL:                time.Minute,
		RequiredProtocolVersion: RequiredProtocolVersionV2,
		RequiredTurnInputSchema: RequiredTurnInputRunTurn,
	}
	processor.setState(stateKey("sess_retry", "gen_retry"), func(state bridgeState) bridgeState {
		state.helloSeen = true
		state.probed = true
		return state
	})
	writeOutbox(t, ctx, root, Envelope{
		MessageID:    "msg_claim",
		RequestID:    "req_claim",
		Type:         TypeClaimNextTurn,
		SessionID:    "sess_retry",
		GenerationID: "gen_retry",
	})
	if err := processor.ProcessOnce(ctx, root); err == nil || !strings.Contains(err.Error(), "database is locked") {
		t.Fatalf("process with store failure err=%v, want database is locked", err)
	}
	assertInboxEmpty(t, root)
	outbox, err := OpenQueue(root, OutboxDir)
	if err != nil {
		t.Fatalf("open outbox: %v", err)
	}
	files, err := outbox.ReadAll()
	if err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	if len(files) != 1 || files[0].Envelope.RequestID != "req_claim" {
		t.Fatalf("outbox after store failure=%+v, want original message retained", files)
	}
}

func TestProcessorProtocolErrorWritesErrorAndUnlinks(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	if err := EnsureLayout(root); err != nil {
		t.Fatalf("ensure layout: %v", err)
	}
	processor := &Processor{
		Store:                   storeFailure{},
		Owner:                   "owner",
		LeaseTTL:                time.Minute,
		RequiredProtocolVersion: RequiredProtocolVersionV2,
		RequiredTurnInputSchema: RequiredTurnInputRunTurn,
	}
	writeOutbox(t, ctx, root, Envelope{
		MessageID:    "msg_bad",
		RequestID:    "req_bad",
		Type:         TypeEmitOutput,
		SessionID:    "sess_bad",
		GenerationID: "gen_bad",
	})
	if err := processor.ProcessOnce(ctx, root); err != nil {
		t.Fatalf("process protocol error: %v", err)
	}
	response := assertSingleInboxResponse(t, root, TypeError, "req_bad")
	var payload errorPayload
	if err := json.Unmarshal(response.Payload, &payload); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if payload.ErrorClass != "bridge_protocol_error" || !strings.Contains(payload.Error, "emit_output requires turn_id") {
		t.Fatalf("unexpected protocol error payload: %+v", payload)
	}
	outbox, err := OpenQueue(root, OutboxDir)
	if err != nil {
		t.Fatalf("open outbox: %v", err)
	}
	files, err := outbox.ReadAll()
	if err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("outbox files after protocol error=%d want 0", len(files))
	}
}
