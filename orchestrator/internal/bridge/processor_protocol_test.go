package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestProcessorRequiresHelloAndProbeBeforeClaimGrant(t *testing.T) {
	ctx := context.Background()
	st, owner := openBridgeStore(t, ctx)
	sessionID := "sess_bridge"
	createBridgeSession(t, ctx, st, sessionID)
	allocation, details := allocateBridgeGeneration(t, ctx, st, owner, sessionID)
	if _, err := st.EnqueueTurn(ctx, sessionID, "hello bridge", time.Now().UTC()); err != nil {
		t.Fatalf("enqueue turn: %v", err)
	}

	now := time.Now().UTC()
	processor := &Processor{
		Store:                   st,
		Owner:                   allocation.Owner,
		LeaseTTL:                time.Minute,
		RequiredProtocolVersion: RequiredProtocolVersionV2,
		RequiredTurnInputSchema: RequiredTurnInputRunTurn,
		Now: func() time.Time {
			return now
		},
	}

	writeOutbox(t, ctx, details.BridgeDirPath, Envelope{
		MessageID:    "msg_claim_early",
		RequestID:    "req_claim",
		Type:         TypeClaimNextTurn,
		SessionID:    sessionID,
		GenerationID: allocation.GenerationID,
	})
	if err := processor.ProcessOnce(ctx, details.BridgeDirPath); err != nil {
		t.Fatalf("process early claim: %v", err)
	}
	assertSingleInboxResponse(t, details.BridgeDirPath, TypeNoWork, "req_claim")

	writeOutbox(t, ctx, details.BridgeDirPath, Envelope{
		MessageID:    "msg_hello",
		RequestID:    "req_hello",
		Type:         TypeHello,
		SessionID:    sessionID,
		GenerationID: allocation.GenerationID,
		Payload:      bridgeHelloPayloadForTest("claude_code"),
	})
	if err := processor.ProcessOnce(ctx, details.BridgeDirPath); err != nil {
		t.Fatalf("process hello: %v", err)
	}
	assertSingleInboxResponse(t, details.BridgeDirPath, TypeHelloAck, "req_hello")

	writeOutbox(t, ctx, details.BridgeDirPath, Envelope{
		MessageID:    "msg_probe",
		RequestID:    "req_probe",
		Type:         TypeProbeNetwork,
		SessionID:    sessionID,
		GenerationID: allocation.GenerationID,
	})
	if err := processor.ProcessOnce(ctx, details.BridgeDirPath); err != nil {
		t.Fatalf("process probe: %v", err)
	}
	assertSingleInboxResponse(t, details.BridgeDirPath, TypeNoWork, "req_probe")

	writeOutbox(t, ctx, details.BridgeDirPath, Envelope{
		MessageID:    "msg_claim_ready",
		RequestID:    "req_claim_ready",
		Type:         TypeClaimNextTurn,
		SessionID:    sessionID,
		GenerationID: allocation.GenerationID,
	})
	if err := processor.ProcessOnce(ctx, details.BridgeDirPath); err != nil {
		t.Fatalf("process ready claim: %v", err)
	}
	response := assertSingleInboxResponse(t, details.BridgeDirPath, TypeGrant, "req_claim_ready")
	var grant grantPayload
	if err := json.Unmarshal(response.Payload, &grant); err != nil {
		t.Fatalf("decode grant: %v", err)
	}
	if grant.Input.Content != "hello bridge" || grant.TurnID == 0 || grant.Sequence != 1 || grant.TurnInputSchema != "RunTurn" {
		t.Fatalf("unexpected grant: %+v", grant)
	}
	if grant.DriverState == nil || grant.DriverState.DriverID != "claude_code" || grant.DriverState.StateVersion != 1 || !strings.HasPrefix(grant.DriverState.StateDigest, "sha256:") {
		t.Fatalf("grant missing driver state token: %+v", grant.DriverState)
	}
}

func TestProcessorRejectsInvalidV2Hello(t *testing.T) {
	tests := []struct {
		name    string
		payload json.RawMessage
		want    string
	}{
		{name: "missing payload", want: "hello requires protocol_version"},
		{name: "protocol v1", payload: json.RawMessage(`{"protocol_version":1,"driver_id":"claude_code","turn_input_schema":"RunTurn"}`), want: "unsupported bridge protocol_version 1"},
		{name: "driver mismatch", payload: json.RawMessage(`{"protocol_version":2,"driver_id":"sh","turn_input_schema":"RunTurn"}`), want: "does not match generation driver"},
		{name: "schema mismatch", payload: json.RawMessage(`{"protocol_version":2,"driver_id":"claude_code","turn_input_schema":"removed"}`), want: "unsupported turn_input_schema"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st, owner := openBridgeStore(t, ctx)
			sessionID := "sess_bridge_hello_" + strings.ReplaceAll(tc.name, " ", "_")
			createBridgeSession(t, ctx, st, sessionID)
			allocation, details := allocateBridgeGeneration(t, ctx, st, owner, sessionID)
			if _, err := st.EnqueueTurn(ctx, sessionID, "must not lease", time.Now().UTC()); err != nil {
				t.Fatalf("enqueue turn: %v", err)
			}
			processor := &Processor{
				Store:                   st,
				Owner:                   allocation.Owner,
				LeaseTTL:                time.Minute,
				RequiredProtocolVersion: RequiredProtocolVersionV2,
				RequiredTurnInputSchema: RequiredTurnInputRunTurn,
			}
			writeOutbox(t, ctx, details.BridgeDirPath, Envelope{
				MessageID:    "msg_bad_hello",
				RequestID:    "req_bad_hello",
				Type:         TypeHello,
				SessionID:    sessionID,
				GenerationID: allocation.GenerationID,
				Payload:      tc.payload,
			})
			if err := processor.ProcessOnce(ctx, details.BridgeDirPath); err != nil {
				t.Fatalf("process invalid hello: %v", err)
			}
			response := assertSingleInboxResponse(t, details.BridgeDirPath, TypeError, "req_bad_hello")
			var payload errorPayload
			if err := json.Unmarshal(response.Payload, &payload); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if payload.ErrorClass != "bridge_protocol_error" || !strings.Contains(payload.Error, tc.want) {
				t.Fatalf("unexpected hello error payload: %+v want %q", payload, tc.want)
			}
			var leasedTurns int
			if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM turns WHERE session_id = ? AND status <> 'queued'`, sessionID).Scan(&leasedTurns); err != nil {
				t.Fatalf("count leased turns: %v", err)
			}
			if leasedTurns != 0 {
				t.Fatalf("invalid hello should not lease turns, got %d", leasedTurns)
			}
		})
	}
}

func TestValidateHelloPayloadRequiresRequiredContract(t *testing.T) {
	ctx := context.Background()
	envelope := Envelope{
		MessageID:    "msg_hello",
		Type:         TypeHello,
		SessionID:    "sess_hello_config",
		GenerationID: "gen_hello_config",
		Payload:      bridgeHelloPayloadForTest("claude_code"),
	}
	tests := []struct {
		name                    string
		requiredProtocolVersion int
		requiredTurnInputSchema string
		want                    string
	}{
		{name: "missing protocol version", requiredTurnInputSchema: RequiredTurnInputRunTurn, want: "required bridge protocol_version must be positive"},
		{name: "missing turn input schema", requiredProtocolVersion: RequiredProtocolVersionV2, want: "required turn_input_schema is required"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := ValidateHelloPayload(ctx, storeFailure{}, envelope, tc.requiredProtocolVersion, tc.requiredTurnInputSchema)
			if err == nil || !errors.Is(err, errProtocol) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ValidateHelloPayload err=%v, want protocol error containing %q", err, tc.want)
			}
		})
	}
}

func TestProcessorRejectsMissingRequiredProtocolConfig(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	if err := EnsureLayout(root); err != nil {
		t.Fatalf("ensure layout: %v", err)
	}
	processor := &Processor{
		Store:    storeFailure{},
		Owner:    "owner",
		LeaseTTL: time.Minute,
	}
	writeOutbox(t, ctx, root, Envelope{
		MessageID:    "msg_hello",
		RequestID:    "req_hello",
		Type:         TypeHello,
		SessionID:    "sess_missing_config",
		GenerationID: "gen_missing_config",
		Payload:      bridgeHelloPayloadForTest("claude_code"),
	})
	if err := processor.ProcessOnce(ctx, root); err != nil {
		t.Fatalf("process missing config hello: %v", err)
	}
	response := assertSingleInboxResponse(t, root, TypeError, "req_hello")
	var payload errorPayload
	if err := json.Unmarshal(response.Payload, &payload); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if payload.ErrorClass != "bridge_protocol_error" || !strings.Contains(payload.Error, "required bridge protocol_version must be positive") {
		t.Fatalf("unexpected missing config error: %+v", payload)
	}
}

func TestProcessorRejectsMissingLeaseTTLBeforeClaim(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	if err := EnsureLayout(root); err != nil {
		t.Fatalf("ensure layout: %v", err)
	}
	processor := &Processor{
		Store:                   storeFailure{},
		Owner:                   "owner",
		RequiredProtocolVersion: RequiredProtocolVersionV2,
		RequiredTurnInputSchema: RequiredTurnInputRunTurn,
	}
	processor.MarkReady("sess_missing_ttl", "gen_missing_ttl")
	writeOutbox(t, ctx, root, Envelope{
		MessageID:    "msg_claim",
		RequestID:    "req_claim",
		Type:         TypeClaimNextTurn,
		SessionID:    "sess_missing_ttl",
		GenerationID: "gen_missing_ttl",
	})
	if err := processor.ProcessOnce(ctx, root); err != nil {
		t.Fatalf("process missing ttl claim: %v", err)
	}
	response := assertSingleInboxResponse(t, root, TypeError, "req_claim")
	var payload errorPayload
	if err := json.Unmarshal(response.Payload, &payload); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if payload.ErrorClass != "bridge_protocol_error" || !strings.Contains(payload.Error, "bridge processor lease_ttl must be positive") {
		t.Fatalf("unexpected missing ttl error: %+v", payload)
	}
}

func TestProcessorRejectsAckCompletedMissingStatus(t *testing.T) {
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
	turnID := int64(1)
	writeOutbox(t, ctx, root, Envelope{
		MessageID:    "msg_done",
		RequestID:    "req_done",
		Type:         TypeAckTurnCompleted,
		SessionID:    "sess_done",
		GenerationID: "gen_done",
		TurnID:       &turnID,
		Payload:      json.RawMessage(`{}`),
	})
	if err := processor.ProcessOnce(ctx, root); err != nil {
		t.Fatalf("process missing status completion: %v", err)
	}
	response := assertSingleInboxResponse(t, root, TypeError, "req_done")
	var payload errorPayload
	if err := json.Unmarshal(response.Payload, &payload); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if payload.ErrorClass != "bridge_protocol_error" || !strings.Contains(payload.Error, "ack_turn_completed requires status") {
		t.Fatalf("unexpected missing completion status error: %+v", payload)
	}
}
