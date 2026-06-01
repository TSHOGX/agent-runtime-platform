package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/sessionstate"
	"harness-platform/orchestrator/internal/store"
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
		Store:    st,
		Owner:    allocation.Owner,
		LeaseTTL: time.Minute,
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
				Store:    st,
				Owner:    allocation.Owner,
				LeaseTTL: time.Minute,
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

func TestProcessorMarkReadyAllowsClaimAfterExternalStartupProbe(t *testing.T) {
	ctx := context.Background()
	st, owner := openBridgeStore(t, ctx)
	sessionID := "sess_bridge_mark_ready"
	createBridgeSession(t, ctx, st, sessionID)
	allocation, details := allocateBridgeGeneration(t, ctx, st, owner, sessionID)
	if _, err := st.EnqueueTurn(ctx, sessionID, "after startup probe", time.Now().UTC()); err != nil {
		t.Fatalf("enqueue turn: %v", err)
	}

	now := time.Now().UTC()
	processor := &Processor{
		Store:    st,
		Owner:    allocation.Owner,
		LeaseTTL: time.Minute,
		Now: func() time.Time {
			return now
		},
	}
	processor.MarkReady(sessionID, allocation.GenerationID)

	writeOutbox(t, ctx, details.BridgeDirPath, Envelope{
		MessageID:    "msg_mark_ready_claim",
		RequestID:    "req_mark_ready_claim",
		Type:         TypeClaimNextTurn,
		SessionID:    sessionID,
		GenerationID: allocation.GenerationID,
	})
	if err := processor.ProcessOnce(ctx, details.BridgeDirPath); err != nil {
		t.Fatalf("process marked-ready claim: %v", err)
	}
	response := assertSingleInboxResponse(t, details.BridgeDirPath, TypeGrant, "req_mark_ready_claim")
	var grant grantPayload
	if err := json.Unmarshal(response.Payload, &grant); err != nil {
		t.Fatalf("decode grant: %v", err)
	}
	if grant.Input.Content != "after startup probe" || grant.TurnID == 0 || grant.TurnInputSchema != "RunTurn" {
		t.Fatalf("unexpected grant: %+v", grant)
	}
}

func TestProcessorRequiresProbeBeforeRestoredGenerationClaimsTurn(t *testing.T) {
	ctx := context.Background()
	st, owner := openBridgeStore(t, ctx)
	sessionID := "sess_restore_probe"
	createBridgeSession(t, ctx, st, sessionID)
	allocation, details := allocateBridgeGeneration(t, ctx, st, owner, sessionID)
	if err := st.RecordGenerationRuntimeArtifactDigests(ctx, allocation.GenerationID, store.GenerationRuntimeArtifactDigests{
		ControlManifestDigest:          "restore_manifest_digest",
		ProjectedControlManifestDigest: "restore_manifest_digest",
		BundleDigest:                   "restore_bundle_digest",
		RuntimeConfigDigest:            "restore_runtime_config_digest",
		SpecDigest:                     "restore_spec_digest",
		RunscVersion:                   "runsc restore-test",
		RunscBinaryPath:                "/usr/local/bin/runsc-restore-test",
		RunscBinaryDigest:              "sha256:runsc-restore-test",
	}); err != nil {
		t.Fatalf("record generation artifacts: %v", err)
	}
	now := time.Now().UTC()
	markBridgeGenerationCheckpointed(t, ctx, st, sessionID, allocation.GenerationID, now)
	restoring, err := st.ClaimCheckpointedGenerationForRestore(ctx, store.ClaimCheckpointedGenerationParams{
		SessionID:    sessionID,
		GenerationID: allocation.GenerationID,
		Owner:        allocation.Owner,
		LeaseTTL:     time.Minute,
		Now:          now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("claim checkpointed generation for restore: %v", err)
	}
	turnID, err := st.EnqueueTurn(ctx, sessionID, "after restore", now.Add(2*time.Second))
	if err != nil {
		t.Fatalf("enqueue turn: %v", err)
	}

	processorNow := now.Add(3 * time.Second)
	processor := &Processor{
		Store:    st,
		Owner:    allocation.Owner,
		LeaseTTL: time.Minute,
		Now: func() time.Time {
			return processorNow
		},
	}
	writeOutbox(t, ctx, details.BridgeDirPath, Envelope{
		MessageID:    "msg_restore_claim_early",
		RequestID:    "req_restore_claim_early",
		Type:         TypeClaimNextTurn,
		SessionID:    sessionID,
		GenerationID: allocation.GenerationID,
	})
	if err := processor.ProcessOnce(ctx, details.BridgeDirPath); err != nil {
		t.Fatalf("process restore claim before probe: %v", err)
	}
	response := assertSingleInboxResponse(t, details.BridgeDirPath, TypeNoWork, "req_restore_claim_early")
	var noWork map[string]string
	if err := json.Unmarshal(response.Payload, &noWork); err != nil {
		t.Fatalf("decode no_work response: %v", err)
	}
	if noWork["reason"] != "bridge_not_ready" {
		t.Fatalf("early restore claim reason=%q want bridge_not_ready", noWork["reason"])
	}
	var turnStatus, turnGeneration, turnOwner string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT status, COALESCE(generation_id, ''), COALESCE(lease_owner, '')
FROM turns
WHERE id = ?`, turnID).Scan(&turnStatus, &turnGeneration, &turnOwner); err != nil {
		t.Fatalf("query turn after early claim: %v", err)
	}
	if turnStatus != "queued" || turnGeneration != "" || turnOwner != "" {
		t.Fatalf("turn leased before bridge probe: status=%s generation=%s owner=%s", turnStatus, turnGeneration, turnOwner)
	}

	writeOutbox(t, ctx, details.BridgeDirPath, Envelope{
		MessageID:    "msg_restore_hello",
		RequestID:    "req_restore_hello",
		Type:         TypeHello,
		SessionID:    sessionID,
		GenerationID: allocation.GenerationID,
		Payload:      bridgeHelloPayloadForTest("claude_code"),
	})
	if err := processor.ProcessOnce(ctx, details.BridgeDirPath); err != nil {
		t.Fatalf("process restore hello: %v", err)
	}
	assertSingleInboxResponse(t, details.BridgeDirPath, TypeHelloAck, "req_restore_hello")
	writeOutbox(t, ctx, details.BridgeDirPath, Envelope{
		MessageID:    "msg_restore_probe",
		RequestID:    "req_restore_probe",
		Type:         TypeProbeNetwork,
		SessionID:    sessionID,
		GenerationID: allocation.GenerationID,
	})
	if err := processor.ProcessOnce(ctx, details.BridgeDirPath); err != nil {
		t.Fatalf("process restore probe: %v", err)
	}
	assertSingleInboxResponse(t, details.BridgeDirPath, TypeNoWork, "req_restore_probe")
	if err := st.MarkGenerationResourcesLive(ctx, sessionID, allocation.GenerationID, restoring.Owner, now.Add(4*time.Second)); err != nil {
		t.Fatalf("mark restored resources live: %v", err)
	}
	processorNow = now.Add(5 * time.Second)

	writeOutbox(t, ctx, details.BridgeDirPath, Envelope{
		MessageID:    "msg_restore_claim_ready",
		RequestID:    "req_restore_claim_ready",
		Type:         TypeClaimNextTurn,
		SessionID:    sessionID,
		GenerationID: allocation.GenerationID,
	})
	if err := processor.ProcessOnce(ctx, details.BridgeDirPath); err != nil {
		t.Fatalf("process restore claim after probe: %v", err)
	}
	response = assertSingleInboxResponse(t, details.BridgeDirPath, TypeGrant, "req_restore_claim_ready")
	var grant grantPayload
	if err := json.Unmarshal(response.Payload, &grant); err != nil {
		t.Fatalf("decode restore grant: %v", err)
	}
	if grant.TurnID != turnID || grant.Input.Content != "after restore" || grant.Sequence != 1 || grant.TurnInputSchema != "RunTurn" {
		t.Fatalf("unexpected restore grant: %+v", grant)
	}
}

func TestProcessorLifecycleMessagesUpdateTurnAndEvents(t *testing.T) {
	ctx := context.Background()
	st, owner := openBridgeStore(t, ctx)
	sessionID := "sess_lifecycle"
	createBridgeSession(t, ctx, st, sessionID)
	allocation, details := allocateBridgeGeneration(t, ctx, st, owner, sessionID)
	turnID, err := st.EnqueueTurn(ctx, sessionID, "run", time.Now().UTC())
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

	processor := &Processor{
		Store:    st,
		Owner:    allocation.Owner,
		LeaseTTL: time.Minute,
	}
	started := Envelope{
		MessageID:    "msg_started",
		RequestID:    "req_started",
		Type:         TypeAckTurnStarted,
		SessionID:    sessionID,
		GenerationID: allocation.GenerationID,
		TurnID:       &turnID,
		Payload:      json.RawMessage(`{}`),
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
	done := Envelope{
		MessageID:    "msg_done",
		RequestID:    "req_done",
		Type:         TypeAckTurnCompleted,
		SessionID:    sessionID,
		GenerationID: allocation.GenerationID,
		TurnID:       &turnID,
		Payload:      json.RawMessage(`{"status":"completed"}`),
	}
	writeOutbox(t, ctx, details.BridgeDirPath, started)
	writeOutbox(t, ctx, details.BridgeDirPath, output)
	if err := processor.ProcessOnce(ctx, details.BridgeDirPath); err != nil {
		t.Fatalf("process start/output: %v", err)
	}
	assertInboxEmpty(t, details.BridgeDirPath)
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE active_model_request_contexts
SET next_request_sequence = 7
WHERE sandbox_source_ip = '10.240.0.2'`); err != nil {
		t.Fatalf("advance active request context sequence: %v", err)
	}

	started.MessageID = "msg_started_replay"
	output.MessageID = "msg_output_replay"
	writeOutbox(t, ctx, details.BridgeDirPath, started)
	writeOutbox(t, ctx, details.BridgeDirPath, output)
	if err := processor.ProcessOnce(ctx, details.BridgeDirPath); err != nil {
		t.Fatalf("process start/output replay: %v", err)
	}
	assertInboxEmpty(t, details.BridgeDirPath)
	var nextRequestSequence int64
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT next_request_sequence
FROM active_model_request_contexts
WHERE sandbox_source_ip = '10.240.0.2'`).Scan(&nextRequestSequence); err != nil {
		t.Fatalf("active request context sequence: %v", err)
	}
	if nextRequestSequence != 7 {
		t.Fatalf("next request sequence after ack replay=%d want 7", nextRequestSequence)
	}

	writeOutbox(t, ctx, details.BridgeDirPath, done)
	if err := processor.ProcessOnce(ctx, details.BridgeDirPath); err != nil {
		t.Fatalf("process completion: %v", err)
	}
	assertInboxEmpty(t, details.BridgeDirPath)

	var turnStatus, generationStatus string
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT status FROM turns WHERE id = ?`, turnID).Scan(&turnStatus); err != nil {
		t.Fatalf("turn status: %v", err)
	}
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT status FROM runtime_generations WHERE generation_id = ?`, allocation.GenerationID).Scan(&generationStatus); err != nil {
		t.Fatalf("generation status: %v", err)
	}
	if turnStatus != "completed" || generationStatus != "idle" {
		t.Fatalf("unexpected statuses: turn=%s generation=%s", turnStatus, generationStatus)
	}
	var eventCount int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*) FROM events
WHERE session_id = ?
  AND generation_id = ?
  AND turn_id = ?`, sessionID, allocation.GenerationID, turnID).Scan(&eventCount); err != nil {
		t.Fatalf("event count: %v", err)
	}
	if eventCount != 3 {
		t.Fatalf("event count=%d want 3", eventCount)
	}

	done.MessageID = "msg_done_replay"
	writeOutbox(t, ctx, details.BridgeDirPath, done)
	if err := processor.ProcessOnce(ctx, details.BridgeDirPath); err != nil {
		t.Fatalf("process completion replay: %v", err)
	}
	assertInboxEmpty(t, details.BridgeDirPath)
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*) FROM events
WHERE session_id = ?
  AND generation_id = ?
  AND turn_id = ?`, sessionID, allocation.GenerationID, turnID).Scan(&eventCount); err != nil {
		t.Fatalf("event count after replay: %v", err)
	}
	if eventCount != 3 {
		t.Fatalf("event count after replay=%d want 3", eventCount)
	}
}

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
		Store:    st,
		Owner:    allocation.Owner,
		LeaseTTL: time.Minute,
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
		Store:           st,
		Owner:           allocation.Owner,
		LeaseTTL:        time.Minute,
		AckStartedGrace: 90 * time.Second,
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
		Store:    st,
		Owner:    allocation.Owner,
		LeaseTTL: time.Minute,
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
		Store:    st,
		Owner:    allocation.Owner,
		LeaseTTL: time.Minute,
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

func TestProcessorRejectsUnknownNativeEventPayload(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	if err := EnsureLayout(root); err != nil {
		t.Fatalf("ensure layout: %v", err)
	}
	turnID := int64(9)
	processor := &Processor{
		Store:    storeFailure{},
		Owner:    "owner",
		LeaseTTL: time.Minute,
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
	processor := &Processor{Store: st, Owner: allocation.Owner, LeaseTTL: time.Minute}
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
	processor := &Processor{Store: st, Owner: allocation.Owner, LeaseTTL: time.Minute}
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
		Owner:    "owner",
		LeaseTTL: time.Minute,
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
		Owner:    "owner",
		LeaseTTL: time.Minute,
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
		Store:    storeFailure{},
		Owner:    "owner",
		LeaseTTL: time.Minute,
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

func emitOutputEnvelope(sessionID, generationID string, turnID int64, sequence int, suffix string) Envelope {
	return Envelope{
		MessageID:    fmt.Sprintf("msg_output_%s_%03d", suffix, sequence),
		RequestID:    fmt.Sprintf("req_output_%s_%03d", suffix, sequence),
		Type:         TypeEmitOutput,
		SessionID:    sessionID,
		GenerationID: generationID,
		TurnID:       &turnID,
		Payload:      json.RawMessage(fmt.Sprintf(`{"output_sequence":%d,"stream":"stdout","payload":{"line":"%s-%03d"}}`, sequence, suffix, sequence)),
	}
}

func bridgeHelloPayloadForTest(driverID string) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{"protocol_version":2,"driver_id":%q,"turn_input_schema":"RunTurn"}`, driverID))
}

func assertInboxEmpty(t *testing.T, root string) {
	t.Helper()
	queue, err := OpenQueue(root, InboxDir)
	if err != nil {
		t.Fatalf("open inbox: %v", err)
	}
	files, err := queue.ReadAll()
	if err != nil {
		t.Fatalf("read inbox: %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("inbox files=%d want 0: %+v", len(files), files)
	}
}

func writeOutbox(t *testing.T, ctx context.Context, root string, envelope Envelope) {
	t.Helper()
	queue, err := OpenQueue(root, OutboxDir)
	if err != nil {
		t.Fatalf("open outbox: %v", err)
	}
	if _, err := queue.Write(ctx, envelope); err != nil {
		t.Fatalf("write outbox: %v", err)
	}
}

func assertSingleInboxResponse(t *testing.T, root, responseType, requestID string) Envelope {
	t.Helper()
	queue, err := OpenQueue(root, InboxDir)
	if err != nil {
		t.Fatalf("open inbox: %v", err)
	}
	files, err := queue.ReadAll()
	if err != nil {
		t.Fatalf("read inbox: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("inbox files=%d want 1: %+v", len(files), files)
	}
	response := files[0].Envelope
	if response.Type != responseType || response.RequestID != requestID {
		t.Fatalf("unexpected response: %+v", response)
	}
	if err := files[0].Unlink(); err != nil {
		t.Fatalf("unlink inbox response: %v", err)
	}
	return response
}

func formatStoreTimeForBridgeTest(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

func openBridgeStore(t *testing.T, ctx context.Context) (*store.Store, *store.OwnerLock) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	owner, err := store.AcquireOwnerLock(filepath.Join(dir, "run"))
	if err != nil {
		t.Fatalf("acquire owner: %v", err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	if err := st.WriteOwner(ctx, owner); err != nil {
		t.Fatalf("write owner: %v", err)
	}
	return st, owner
}

func createBridgeSession(t *testing.T, ctx context.Context, st *store.Store, id string) {
	t.Helper()
	now := time.Now().UTC()
	if err := st.CreateSession(ctx, store.Session{
		ID:        id,
		UserID:    "lab",
		Status:    string(sessionstate.Created),
		DriverID:  "claude_code",
		CreatedAt: now,
		UpdatedAt: now,
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
}

func allocateBridgeGeneration(t *testing.T, ctx context.Context, st *store.Store, owner *store.OwnerLock, sessionID string) (store.GenerationAllocation, store.RuntimeGenerationDetails) {
	t.Helper()
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: sessionID,
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config: store.ResourceAllocatorConfig{
			RunDir:                     filepath.Join(t.TempDir(), "run"),
			CIDRPool:                   netip.MustParsePrefix("10.240.0.0/29"),
			EgressDorisFEHosts:         []string{"172.16.0.138"},
			EgressDorisBEHosts:         []string{"172.16.0.139"},
			EgressDorisPorts:           []int{9030},
			EgressDNSPolicy:            "hostnames_only",
			HostProxyBindURL:           "http://0.0.0.0:8082",
			ProxyPort:                  8082,
			DriverID:                   "claude_code",
			Model:                      "sonnet",
			OutputFormat:               "stream-json",
			DisableNonessentialTraffic: true,
		},
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, sessionID, allocation.GenerationID, allocation.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("mark live: %v", err)
	}
	details, err := st.GetRuntimeGenerationDetails(ctx, sessionID, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get generation details: %v", err)
	}
	createBridgeRuntimeResourceLive(t, ctx, st, sessionID, allocation, details, owner.UUID)
	if err := EnsureLayout(details.BridgeDirPath); err != nil {
		t.Fatalf("ensure bridge layout: %v", err)
	}
	return allocation, details
}

func createBridgeRuntimeResourceLive(t *testing.T, ctx context.Context, st *store.Store, sessionID string, allocation store.GenerationAllocation, details store.RuntimeGenerationDetails, ownerUUID string) {
	t.Helper()
	contractID := "contract_" + allocation.GenerationID
	credentialPolicy := bridgeCredentialPolicyForTest(t, allocation.DriverState.DriverID)
	if _, err := st.StoreSandboxContract(ctx, store.StoreSandboxContractParams{
		ContractID:   contractID,
		SessionID:    sessionID,
		GenerationID: allocation.GenerationID,
		Payload: map[string]any{
			"contract_id":              contractID,
			"contract_schema_version":  store.SandboxContractSchemaVersion,
			"contract_gate_version":    store.SandboxContractGateDriverManifest,
			"generation_id":            allocation.GenerationID,
			"session_id":               sessionID,
			"sandbox_contract_version": store.SandboxContractVersion,
			"runtime_profile_id":       allocation.AgentRuntimeProfileID,
			"network_profile_id":       allocation.NetworkProfileID,
			"driver": map[string]any{
				"driver_id":                            allocation.DriverState.DriverID,
				"driver_version":                       "test",
				"bridge_protocol":                      "harness_bridge_v2",
				"bridge_protocol_version":              2,
				"turn_input_schema":                    "RunTurn",
				"output_schema":                        "claude_stream_json_v1",
				"command_argv_digest":                  "sha256:command",
				"driver_config_digest":                 "sha256:driver-config",
				"required_runtime_capabilities_digest": "sha256:driver-capabilities",
				"supports_interrupt":                   false,
				"supports_compaction":                  true,
			},
			"runtime_provider": map[string]any{
				"provider_id":              "local_runsc",
				"provider_profile_id":      "local_runsc_default",
				"isolation_kind":           "gvisor",
				"template_ref":             "default",
				"template_digest":          "sha256:template",
				"capability_vocab_version": "1",
				"capability_digest":        "sha256:provider-capabilities",
			},
			"identity": map[string]any{
				"model_access_allowed": true,
			},
			"network_identity": map[string]any{
				"runsc_network": details.RunscNetwork,
				"sandbox_ip":    "10.240.0.2",
			},
			"credential_policy": credentialPolicy,
			"model_access": map[string]any{
				"model_access_allowed": true,
			},
			"driver_runtime": map[string]any{
				"driver_home_mount":             "/agent-home",
				"generated_driver_config_mount": "/harness-control/driver/" + allocation.DriverState.DriverID,
				"materialized_driver_config":    map[string]any{},
				"initial_driver_state_digest":   allocation.DriverState.StateDigest,
			},
			"input_digests": map[string]any{
				"runtime_config_digest": "sha256:runtime-config",
				"rootfs_image_digest":   nil,
				"agent_manifest_digest": "sha256:agent-manifest",
			},
		},
		ContractGateVersion: store.SandboxContractGateDriverManifest,
		Now:                 time.Now().UTC(),
	}); err != nil {
		t.Fatalf("store sandbox contract: %v", err)
	}
	prefix, err := netip.ParsePrefix(details.SandboxIPCIDR)
	if err != nil {
		t.Fatalf("parse sandbox cidr: %v", err)
	}
	runscPath := filepath.Join(t.TempDir(), "runsc")
	instance, err := st.CreateRuntimeResourceInstance(ctx, store.RuntimeResourceInstanceParams{
		GenerationID:           allocation.GenerationID,
		SessionID:              sessionID,
		ContractID:             contractID,
		SandboxContractVersion: store.SandboxContractVersion,
		HostID:                 "host-" + allocation.GenerationID,
		RunscContainerID:       details.RunscContainerID,
		RunscPlatform:          "systrap",
		RunscVersion:           "runsc test",
		RunscBinaryPath:        runscPath,
		RunscBinaryDigest:      "sha256:runsc",
		NetworkProfileID:       allocation.NetworkProfileID,
		NetnsName:              details.NetnsName,
		NetnsPath:              details.NetnsPath,
		HostVeth:               details.HostVeth,
		SandboxVeth:            details.SandboxVeth,
		HostGatewayIP:          details.HostGatewayIP,
		SandboxIP:              prefix.Addr().String(),
		SandboxIPCIDR:          details.SandboxIPCIDR,
		HostSideCIDR:           details.HostSideCIDR,
		NftTableName:           "harness_gen_" + strings.TrimPrefix(allocation.GenerationID, "gen_"),
		ControlDirPath:         details.ControlDirPath,
		ControlManifestPath:    details.ControlManifestPath,
		BundleDirPath:          details.BundleDirPath,
		SpecPath:               details.SpecPath,
		CheckpointPath:         details.CheckpointPath,
		BridgeDirPath:          details.BridgeDirPath,
		LogDirPath:             details.LogDirPath,
		RootPrefixes: map[string]string{
			"run_dir":      filepath.Dir(filepath.Dir(details.ControlDirPath)),
			"control_root": filepath.Dir(details.ControlDirPath),
			"bridge_root":  filepath.Dir(details.BridgeDirPath),
		},
		Now: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("create runtime resource instance: %v", err)
	}
	if err := st.ClaimRuntimeResourceMaterialization(ctx, store.RuntimeResourceMaterializationClaimParams{
		GenerationID:     allocation.GenerationID,
		WorkerID:         ownerUUID,
		HostID:           instance.HostID,
		LeaseExpiresAt:   time.Now().Add(time.Minute),
		IdempotencyToken: "test:" + allocation.GenerationID,
		Now:              time.Now().UTC(),
	}); err != nil {
		t.Fatalf("claim runtime resource materialization: %v", err)
	}
	if err := st.MarkRuntimeResourceReady(ctx, store.RuntimeResourceWorkerTransitionParams{
		GenerationID: allocation.GenerationID,
		WorkerID:     ownerUUID,
		HostID:       instance.HostID,
		Now:          time.Now().UTC(),
	}); err != nil {
		t.Fatalf("mark runtime resource ready: %v", err)
	}
	if err := st.MarkRuntimeResourceLive(ctx, store.RuntimeResourceWorkerTransitionParams{
		GenerationID: allocation.GenerationID,
		WorkerID:     ownerUUID,
		HostID:       instance.HostID,
		PostStart:    bridgePostStartProofForTest(instance),
		Now:          time.Now().UTC(),
	}); err != nil {
		t.Fatalf("mark runtime resource live: %v", err)
	}
}

func bridgeCredentialPolicyForTest(t *testing.T, driverID string) map[string]any {
	t.Helper()
	policy := map[string]any{
		"provider_credentials": "host-only",
		"sandbox_secret_mount": "absent",
		"proxy_token":          "absent",
		"secret_grants": []map[string]any{{
			"grant_id":                  "model_provider:anthropic_proxy",
			"domain":                    "model_provider",
			"scope":                     "anthropic_messages",
			"exposure_mode":             "proxy_only",
			"ttl_seconds":               nil,
			"allowed_drivers":           []string{driverID},
			"allowed_runtime_providers": []string{"local_runsc"},
		}},
	}
	digest, err := store.CredentialPolicyDigest(policy)
	if err != nil {
		t.Fatalf("credential digest: %v", err)
	}
	policy["digest"] = digest
	return policy
}

func bridgePostStartProofForTest(instance store.RuntimeResourceInstance) *store.RuntimeResourcePostStartProof {
	return &store.RuntimeResourcePostStartProof{
		HostID:                 instance.HostID,
		GenerationID:           instance.GenerationID,
		ContractID:             instance.ContractID,
		SandboxContractVersion: instance.SandboxContractVersion,
		RunscContainerID:       instance.RunscContainerID,
		RunscState:             "runsc_container:" + instance.RunscContainerID + ":running; check=test",
		RunscPlatform:          instance.RunscPlatform,
		RunscVersion:           instance.RunscVersion,
		RunscBinaryPath:        instance.RunscBinaryPath,
		RunscBinaryDigest:      instance.RunscBinaryDigest,
		IPNetns:                "netns:present; check=test",
		IPLink:                 "host_veth:present; check=test",
		NFT:                    "nft_table:present; check=test",
		BridgeStartup:          "bridge_startup_probe:passed; check=test",
	}
}

func markBridgeGenerationCheckpointed(t *testing.T, ctx context.Context, st *store.Store, sessionID, generationID string, now time.Time) {
	t.Helper()
	fence := bridgeCheckpointDriverStateFenceForTest(t, ctx, st, sessionID, generationID)
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'checkpointed',
    checkpoint_created_at = ?,
    checkpoint_network_profile_id = network_profile_id,
    checkpoint_agent_runtime_profile_id = agent_runtime_profile_id,
    checkpoint_runsc_version = runsc_version,
    checkpoint_runsc_platform = runsc_platform,
    checkpoint_bundle_digest = 'bundle_digest',
    checkpoint_runtime_config_digest = 'runtime_config_digest',
    checkpoint_control_manifest_digest = (
      SELECT control_manifest_digest
      FROM runtime_generation_resources
      WHERE runtime_generation_resources.generation_id = runtime_generations.generation_id
    ),
    checkpoint_driver_states_digest = ?,
    lease_owner = NULL,
    lease_expires_at = NULL,
    last_seen_at = ?
WHERE generation_id = ?
  AND session_id = ?`, formatStoreTimeForBridgeTest(now), fence, formatStoreTimeForBridgeTest(now), generationID, sessionID); err != nil {
		t.Fatalf("set checkpointed generation: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE network_profiles
SET allocation_state = 'reserved_checkpointed'
WHERE generation_id = ?
  AND session_id = ?`, generationID, sessionID); err != nil {
		t.Fatalf("reserve checkpointed network: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generation_resources
SET resource_state = 'reserved_checkpointed'
WHERE generation_id = ?`, generationID); err != nil {
		t.Fatalf("reserve checkpointed resources: %v", err)
	}
	if err := st.UpdateSessionStatus(ctx, sessionID, string(sessionstate.Checkpointed), nil); err != nil {
		t.Fatalf("set checkpointed session: %v", err)
	}
}

func bridgeCheckpointDriverStateFenceForTest(t *testing.T, ctx context.Context, st *store.Store, sessionID, generationID string) string {
	t.Helper()
	var driverID, stateDigest string
	var stateVersion int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT ds.driver_id, ds.state_digest, ds.state_version
FROM session_driver_states ds
JOIN runtime_generations g ON g.session_id = ds.session_id
JOIN agent_runtime_profiles a ON a.agent_runtime_profile_id = g.agent_runtime_profile_id
WHERE g.session_id = ?
  AND g.generation_id = ?
  AND ds.driver_id = a.driver_id`, sessionID, generationID).Scan(&driverID, &stateDigest, &stateVersion); err != nil {
		t.Fatalf("query driver state fence input: %v", err)
	}
	fence, err := store.CheckpointDriverStatesDigest(generationID, []store.DriverStateToken{{
		DriverID:     driverID,
		StateDigest:  stateDigest,
		StateVersion: stateVersion,
	}})
	if err != nil {
		t.Fatalf("compute driver state fence: %v", err)
	}
	return fence
}

type storeFailure struct {
	claimErr      error
	completeErr   error
	failGenCalled *bool
}

func (s storeFailure) BridgeHelloAck(context.Context, string, string, string, time.Time, time.Duration) (store.BridgeHelloAck, error) {
	return store.BridgeHelloAck{}, errors.New("unexpected BridgeHelloAck")
}

func (s storeFailure) BridgeProtocolEvidence(context.Context, string, string) (store.BridgeProtocolEvidence, error) {
	return store.BridgeProtocolEvidence{}, errors.New("unexpected BridgeProtocolEvidence")
}

func (s storeFailure) RenewGenerationHeartbeat(context.Context, store.RenewHeartbeatParams) error {
	return errors.New("unexpected RenewGenerationHeartbeat")
}

func (s storeFailure) ClaimNextTurn(context.Context, store.ClaimNextTurnParams) (store.TurnGrant, bool, error) {
	return store.TurnGrant{}, false, s.claimErr
}

func (s storeFailure) ResumeTurn(context.Context, store.ResumeTurnParams) (store.TurnGrant, bool, error) {
	return store.TurnGrant{}, false, errors.New("unexpected ResumeTurn")
}

func (s storeFailure) AckTurnStarted(context.Context, store.AckStartedParams) (int64, error) {
	return 0, errors.New("unexpected AckTurnStarted")
}

func (s storeFailure) CompleteTurn(context.Context, store.CompleteTurnParams) (int64, error) {
	if s.completeErr != nil {
		return 0, s.completeErr
	}
	return 0, errors.New("unexpected CompleteTurn")
}

func (s storeFailure) FailGeneration(_ context.Context, _ store.FailGenerationParams) error {
	if s.failGenCalled != nil {
		*s.failGenCalled = true
		return nil
	}
	return errors.New("unexpected FailGeneration")
}

func (s storeFailure) AppendEvent(context.Context, store.AppendEventParams) (int64, error) {
	return 0, errors.New("unexpected AppendEvent")
}
