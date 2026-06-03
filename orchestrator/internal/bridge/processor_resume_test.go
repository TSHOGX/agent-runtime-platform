package bridge

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/store"
)

func TestProcessorResumeTurnRequiresReadyBridgeAndExistingLease(t *testing.T) {
	ctx := context.Background()
	st, owner := openBridgeStore(t, ctx)
	sessionID := "sess_resume"
	createBridgeSession(t, ctx, st, sessionID)
	allocation, details := allocateBridgeGeneration(t, ctx, st, owner, sessionID)
	turnID, err := st.EnqueueTurn(ctx, sessionID, "resume me", time.Now().UTC())
	if err != nil {
		t.Fatalf("enqueue turn: %v", err)
	}
	grant, ok, err := st.ClaimNextTurn(ctx, store.ClaimNextTurnParams{
		SessionID:    sessionID,
		GenerationID: allocation.GenerationID,
		Owner:        allocation.Owner,
		RequestID:    "direct_claim",
		LeaseTTL:     time.Minute,
		Now:          time.Now().UTC(),
	})
	if err != nil || !ok || grant.TurnID != turnID {
		t.Fatalf("claim setup: ok=%v grant=%+v err=%v", ok, grant, err)
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
	resume := Envelope{
		MessageID:    "msg_resume_early",
		RequestID:    "req_resume_early",
		Type:         TypeResumeTurn,
		SessionID:    sessionID,
		GenerationID: allocation.GenerationID,
		TurnID:       &turnID,
	}
	writeOutbox(t, ctx, details.BridgeDirPath, resume)
	if err := processor.ProcessOnce(ctx, details.BridgeDirPath); err != nil {
		t.Fatalf("process early resume: %v", err)
	}
	assertSingleInboxResponse(t, details.BridgeDirPath, TypeNoWork, "req_resume_early")

	processor.setState(stateKey(sessionID, allocation.GenerationID), func(state bridgeState) bridgeState {
		state.helloSeen = true
		state.probed = true
		return state
	})
	resume.MessageID = "msg_resume_ready"
	resume.RequestID = "req_resume_ready"
	writeOutbox(t, ctx, details.BridgeDirPath, resume)
	if err := processor.ProcessOnce(ctx, details.BridgeDirPath); err != nil {
		t.Fatalf("process ready resume: %v", err)
	}
	response := assertSingleInboxResponse(t, details.BridgeDirPath, TypeGrant, "req_resume_ready")
	var resumed grantPayload
	if err := json.Unmarshal(response.Payload, &resumed); err != nil {
		t.Fatalf("decode resume grant: %v", err)
	}
	if resumed.TurnID != turnID || resumed.Input.Content != "resume me" || !resumed.Replayed || resumed.TurnInputSchema != "RunTurn" {
		t.Fatalf("unexpected resume grant: %+v", resumed)
	}

	var turnStatus string
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT status FROM turns WHERE id = ?`, turnID).Scan(&turnStatus); err != nil {
		t.Fatalf("turn status: %v", err)
	}
	if turnStatus != "leased" {
		t.Fatalf("resume changed turn status=%s want leased", turnStatus)
	}
}

func TestProcessorResumesAckStartedTurnDuringGrace(t *testing.T) {
	ctx := context.Background()
	st, owner := openBridgeStore(t, ctx)
	sessionID := "sess_ack_resume"
	createBridgeSession(t, ctx, st, sessionID)
	allocation, details := allocateBridgeGeneration(t, ctx, st, owner, sessionID)
	turnID, err := st.EnqueueTurn(ctx, sessionID, "resume after restart", time.Now().UTC())
	if err != nil {
		t.Fatalf("enqueue turn: %v", err)
	}
	now := time.Now().UTC()
	claimAt := now.Add(-2 * time.Minute)
	grant, ok, err := st.ClaimNextTurn(ctx, store.ClaimNextTurnParams{
		SessionID:    sessionID,
		GenerationID: allocation.GenerationID,
		Owner:        allocation.Owner,
		RequestID:    "direct_claim",
		LeaseTTL:     time.Minute,
		Now:          claimAt,
	})
	if err != nil || !ok || grant.TurnID != turnID {
		t.Fatalf("claim setup: ok=%v grant=%+v err=%v", ok, grant, err)
	}
	if _, err := st.AckTurnStarted(ctx, store.AckStartedParams{
		SessionID:       sessionID,
		GenerationID:    allocation.GenerationID,
		TurnID:          turnID,
		Owner:           allocation.Owner,
		SandboxSourceIP: "10.240.0.2",
		LeaseTTL:        time.Minute,
		Now:             claimAt.Add(time.Second),
	}); err != nil {
		t.Fatalf("ack started setup: %v", err)
	}
	expiredAt := now.Add(-30 * time.Second)
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generations
SET lease_owner = ?, lease_expires_at = ?
WHERE generation_id = ?`, store.GenerationLeaseOwner("previous-owner"), formatStoreTimeForBridgeTest(expiredAt), allocation.GenerationID); err != nil {
		t.Fatalf("expire generation lease: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE turns
SET lease_owner = ?, lease_expires_at = ?
WHERE id = ?`, store.GenerationLeaseOwner("previous-owner"), formatStoreTimeForBridgeTest(expiredAt), turnID); err != nil {
		t.Fatalf("expire turn lease: %v", err)
	}

	processor := &Processor{
		Store:                   st,
		Owner:                   allocation.Owner,
		LeaseTTL:                time.Minute,
		AckStartedGrace:         90 * time.Second,
		RequiredProtocolVersion: RequiredProtocolVersionV2,
		RequiredTurnInputSchema: RequiredTurnInputRunTurn,
		Now: func() time.Time {
			return now
		},
	}
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
	response := assertSingleInboxResponse(t, details.BridgeDirPath, TypeHelloAck, "req_hello")
	var ack helloAckPayload
	if err := json.Unmarshal(response.Payload, &ack); err != nil {
		t.Fatalf("decode hello ack: %v", err)
	}
	if ack.LeasedTurnID == nil || *ack.LeasedTurnID != turnID {
		t.Fatalf("hello ack leased_turn_id=%v want %d", ack.LeasedTurnID, turnID)
	}

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
		MessageID:    "msg_resume",
		RequestID:    "req_resume",
		Type:         TypeResumeTurn,
		SessionID:    sessionID,
		GenerationID: allocation.GenerationID,
		TurnID:       &turnID,
	})
	if err := processor.ProcessOnce(ctx, details.BridgeDirPath); err != nil {
		t.Fatalf("process resume: %v", err)
	}
	response = assertSingleInboxResponse(t, details.BridgeDirPath, TypeGrant, "req_resume")
	var resumed grantPayload
	if err := json.Unmarshal(response.Payload, &resumed); err != nil {
		t.Fatalf("decode resume grant: %v", err)
	}
	if resumed.TurnID != turnID || resumed.Input.Content != "resume after restart" || !resumed.Replayed || resumed.TurnInputSchema != "RunTurn" {
		t.Fatalf("unexpected resume grant: %+v", resumed)
	}

	var turnOwner, generationOwner string
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT lease_owner FROM turns WHERE id = ?`, turnID).Scan(&turnOwner); err != nil {
		t.Fatalf("query turn owner: %v", err)
	}
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT lease_owner FROM runtime_generations WHERE generation_id = ?`, allocation.GenerationID).Scan(&generationOwner); err != nil {
		t.Fatalf("query generation owner: %v", err)
	}
	if turnOwner != allocation.Owner || generationOwner != allocation.Owner {
		t.Fatalf("lease owners not transferred: turn=%s generation=%s want %s", turnOwner, generationOwner, allocation.Owner)
	}
}
