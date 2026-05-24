package bridge

import (
	"context"
	"encoding/json"
	"errors"
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

type storeFailure struct {
	claimErr error
}

func (s storeFailure) BridgeHelloAck(context.Context, string, string, string, time.Time) (store.BridgeHelloAck, error) {
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

func (s storeFailure) AckTurnStarted(context.Context, store.AckStartedParams) error {
	return errors.New("unexpected AckTurnStarted")
}

func (s storeFailure) CompleteTurn(context.Context, store.CompleteTurnParams) error {
	return errors.New("unexpected CompleteTurn")
}

func (s storeFailure) AppendEvent(context.Context, store.AppendEventParams) (int64, error) {
	return 0, errors.New("unexpected AppendEvent")
}
