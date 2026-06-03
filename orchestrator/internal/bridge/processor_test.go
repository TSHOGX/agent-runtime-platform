package bridge

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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

const checkpointImageManifestDigestForBridgeTest = "sha256:checkpoint-image-manifest"

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
		Store:                   st,
		Owner:                   allocation.Owner,
		LeaseTTL:                time.Minute,
		RequiredProtocolVersion: RequiredProtocolVersionV2,
		RequiredTurnInputSchema: RequiredTurnInputRunTurn,
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
	storeBridgeCheckpointTestGenerationPlan(t, ctx, st, allocation.GenerationID)
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
		Store:                   st,
		Owner:                   allocation.Owner,
		LeaseTTL:                time.Minute,
		RequiredProtocolVersion: RequiredProtocolVersionV2,
		RequiredTurnInputSchema: RequiredTurnInputRunTurn,
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
		Mode:      store.ModeForDriver("claude_code"),
		CreatedAt: now,
		UpdatedAt: now,
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
}

func allocateBridgeGeneration(t *testing.T, ctx context.Context, st *store.Store, owner *store.OwnerLock, sessionID string) (store.GenerationAllocation, store.RuntimeGenerationDetails) {
	t.Helper()
	modelAccessAllowed := true
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: sessionID,
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config: store.ResourceAllocatorConfig{
			RunDir:                      filepath.Join(t.TempDir(), "run"),
			CIDRPool:                    netip.MustParsePrefix("10.240.0.0/29"),
			EgressDorisFEHosts:          []string{"172.16.0.138"},
			EgressDorisBEHosts:          []string{"172.16.0.139"},
			EgressDorisPorts:            []int{9030},
			EgressDNSPolicy:             "hostnames_only",
			HostProxyBindURL:            "http://0.0.0.0:8082",
			ProxyPort:                   8082,
			DriverID:                    "claude_code",
			Model:                       "sonnet",
			OutputFormat:                "stream-json",
			DisableNonessentialTraffic:  true,
			SandboxUID:                  7000,
			SandboxGID:                  7001,
			ModelAccessAllowed:          &modelAccessAllowed,
			ProviderCredentialsHostOnly: true,
			SandboxModelProxyBaseURL:    "http://harness-model-proxy.internal:8082",
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
		ContractID:             contractID,
		SessionID:              sessionID,
		GenerationID:           allocation.GenerationID,
		SandboxContractVersion: store.SandboxContractVersion,
		ContractSchemaVersion:  store.SandboxContractSchemaVersion,
		ContractGateVersion:    store.SandboxContractGateDriverManifest,
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
		Now: time.Now().UTC(),
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
	plan, err := st.GetGenerationPlan(ctx, generationID)
	if err != nil {
		t.Fatalf("get checkpoint generation plan: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'checkpointed',
    checkpoint_created_at = ?,
    checkpoint_network_profile_id = network_profile_id,
    checkpoint_agent_runtime_profile_id = agent_runtime_profile_id,
    checkpoint_runsc_version = runsc_version,
    checkpoint_runsc_platform = runsc_platform,
    checkpoint_runsc_binary_path = (
      SELECT runsc_binary_path
      FROM runtime_generation_resources
      WHERE runtime_generation_resources.generation_id = runtime_generations.generation_id
    ),
    checkpoint_runsc_binary_digest = (
      SELECT runsc_binary_digest
      FROM runtime_generation_resources
      WHERE runtime_generation_resources.generation_id = runtime_generations.generation_id
    ),
    checkpoint_bundle_digest = (
      SELECT bundle_digest
      FROM runtime_generation_resources
      WHERE runtime_generation_resources.generation_id = runtime_generations.generation_id
    ),
    checkpoint_runtime_config_digest = (
      SELECT runtime_config_digest
      FROM runtime_generation_resources
      WHERE runtime_generation_resources.generation_id = runtime_generations.generation_id
    ),
    checkpoint_control_manifest_digest = (
      SELECT projected_control_manifest_digest
      FROM runtime_generation_resources
      WHERE runtime_generation_resources.generation_id = runtime_generations.generation_id
    ),
    checkpoint_driver_states_digest = ?,
    checkpoint_plan_digest = ?,
    checkpoint_image_manifest_digest = ?,
    lease_owner = NULL,
    lease_expires_at = NULL,
    last_seen_at = ?
WHERE generation_id = ?
  AND session_id = ?`, formatStoreTimeForBridgeTest(now), fence, plan.PlanDigest, checkpointImageManifestDigestForBridgeTest, formatStoreTimeForBridgeTest(now), generationID, sessionID); err != nil {
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

func storeBridgeCheckpointTestGenerationPlan(t *testing.T, ctx context.Context, st *store.Store, generationID string) store.GenerationPlanRecord {
	t.Helper()
	plan, err := st.StoreGenerationPlan(ctx, store.StoreGenerationPlanParams{
		GenerationID: generationID,
		Payload:      map[string]any{"generation_id": generationID, "plan_version": store.GenerationPlanVersion},
	})
	if err != nil {
		t.Fatalf("store generation plan for %s: %v", generationID, err)
	}

	var controlManifest, projectedControlManifest, spec, bundle, runtimeConfig string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COALESCE(control_manifest_digest, ''),
       COALESCE(projected_control_manifest_digest, ''),
       COALESCE(spec_digest, ''),
       COALESCE(bundle_digest, ''),
       COALESCE(runtime_config_digest, '')
FROM runtime_generation_resources
WHERE generation_id = ?`, generationID).Scan(
		&controlManifest,
		&projectedControlManifest,
		&spec,
		&bundle,
		&runtimeConfig,
	); err != nil {
		t.Fatalf("query generation artifact digests for %s: %v", generationID, err)
	}

	for _, projection := range []store.StoreGenerationPlanProjectionParams{
		{
			GenerationID:      generationID,
			PlanDigest:        plan.PlanDigest,
			ProjectionKind:    store.GenerationPlanProjectionSandboxContract,
			ProjectionVersion: store.GenerationPlanProjectionVersion,
			PayloadDigest:     "sha256:sandbox-contract",
		},
		{
			GenerationID:      generationID,
			PlanDigest:        plan.PlanDigest,
			ProjectionKind:    store.GenerationPlanProjectionControlManifest,
			ProjectionVersion: store.GenerationPlanProjectionVersion,
			PayloadDigest:     bridgeProjectionPayloadDigest(store.GenerationPlanProjectionControlManifest, controlManifest),
		},
		{
			GenerationID:      generationID,
			PlanDigest:        plan.PlanDigest,
			ProjectionKind:    store.GenerationPlanProjectionControlManifestProjected,
			ProjectionVersion: store.GenerationPlanProjectionVersion,
			PayloadDigest:     bridgeProjectionPayloadDigest(store.GenerationPlanProjectionControlManifestProjected, projectedControlManifest),
		},
		{
			GenerationID:      generationID,
			PlanDigest:        plan.PlanDigest,
			ProjectionKind:    store.GenerationPlanProjectionOCISpec,
			ProjectionVersion: store.GenerationPlanProjectionVersion,
			PayloadDigest:     bridgeProjectionPayloadDigest(store.GenerationPlanProjectionOCISpec, spec),
		},
		{
			GenerationID:      generationID,
			PlanDigest:        plan.PlanDigest,
			ProjectionKind:    store.GenerationPlanProjectionBundle,
			ProjectionVersion: store.GenerationPlanProjectionVersion,
			PayloadDigest:     bridgeProjectionPayloadDigest(store.GenerationPlanProjectionBundle, bundle),
		},
		{
			GenerationID:      generationID,
			PlanDigest:        plan.PlanDigest,
			ProjectionKind:    store.GenerationPlanProjectionRuntimeConfig,
			ProjectionVersion: store.GenerationPlanProjectionVersion,
			PayloadDigest:     bridgeProjectionPayloadDigest(store.GenerationPlanProjectionRuntimeConfig, runtimeConfig),
		},
	} {
		if _, err := st.StoreGenerationPlanProjection(ctx, projection); err != nil {
			t.Fatalf("store generation plan projection %s for %s: %v", projection.ProjectionKind, generationID, err)
		}
	}
	return plan
}

func bridgeProjectionPayloadDigest(kind, digest string) string {
	value := strings.TrimSpace(digest)
	if strings.HasPrefix(value, "sha256:") {
		return value
	}
	sum := sha256.Sum256([]byte(strings.TrimSpace(kind) + "\n" + value))
	return "sha256:" + hex.EncodeToString(sum[:])
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
