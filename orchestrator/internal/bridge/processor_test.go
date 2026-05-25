package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"os"
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
	if grant.Content != "hello bridge" || grant.TurnID == 0 || grant.Sequence != 1 {
		t.Fatalf("unexpected grant: %+v", grant)
	}
}

func TestProcessorRequiresProbeBeforeRestoredGenerationClaimsTurn(t *testing.T) {
	ctx := context.Background()
	st, owner := openBridgeStore(t, ctx)
	sessionID := "sess_restore_probe"
	createBridgeSession(t, ctx, st, sessionID)
	allocation, details := allocateBridgeGeneration(t, ctx, st, owner, sessionID)
	if err := st.RecordGenerationRuntimeArtifacts(ctx, allocation.GenerationID, "restore_manifest_digest", "runsc restore-test"); err != nil {
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
	if grant.TurnID != turnID || grant.Content != "after restore" || grant.Sequence != 1 {
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
	if resumed.TurnID != turnID || resumed.Content != "resume me" || !resumed.Replayed {
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
	if resumed.TurnID != turnID || resumed.Content != "resume after restart" || !resumed.Replayed {
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

	for seq := 41; seq <= 120; seq++ {
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
	workspace := filepath.Join(t.TempDir(), "sessions", id)
	if err := os.MkdirAll(workspace, 0o755); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := st.CreateSession(ctx, store.Session{
		ID:        id,
		UserID:    "lab",
		Status:    string(sessionstate.Created),
		Agent:     "claude",
		Workspace: workspace,
		RestoreID: "phase3-" + id,
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
			Agent:                      "claude",
			AgentModel:                 "sonnet",
			AgentOutputFormat:          "stream-json",
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
	if err := EnsureLayout(details.BridgeDirPath); err != nil {
		t.Fatalf("ensure bridge layout: %v", err)
	}
	return allocation, details
}

func markBridgeGenerationCheckpointed(t *testing.T, ctx context.Context, st *store.Store, sessionID, generationID string, now time.Time) {
	t.Helper()
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'checkpointed',
    checkpoint_created_at = ?,
    checkpoint_network_profile_id = network_profile_id,
    checkpoint_agent_runtime_profile_id = agent_runtime_profile_id,
    checkpoint_runsc_version = COALESCE(runsc_version, 'runsc test'),
    checkpoint_runsc_platform = COALESCE(runsc_platform, 'systrap'),
    checkpoint_bundle_digest = 'bundle_digest',
    checkpoint_runtime_config_digest = 'runtime_config_digest',
    checkpoint_control_manifest_digest = COALESCE((
      SELECT control_manifest_digest
      FROM runtime_generation_resources
      WHERE runtime_generation_resources.generation_id = runtime_generations.generation_id
    ), 'manifest_digest'),
    lease_owner = NULL,
    lease_expires_at = NULL,
    last_seen_at = ?
WHERE generation_id = ?
  AND session_id = ?`, formatStoreTimeForBridgeTest(now), formatStoreTimeForBridgeTest(now), generationID, sessionID); err != nil {
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

type storeFailure struct {
	claimErr error
}

func (s storeFailure) BridgeHelloAck(context.Context, string, string, string, time.Time, time.Duration) (store.BridgeHelloAck, error) {
	return store.BridgeHelloAck{}, errors.New("unexpected BridgeHelloAck")
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
	return 0, errors.New("unexpected CompleteTurn")
}

func (s storeFailure) AppendEvent(context.Context, store.AppendEventParams) (int64, error) {
	return 0, errors.New("unexpected AppendEvent")
}
