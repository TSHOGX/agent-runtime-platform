package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/events"
	"harness-platform/orchestrator/internal/generationplan"
	"harness-platform/orchestrator/internal/sessionstate"
	"harness-platform/orchestrator/internal/store"
)

func TestSendMessageAllocatesGenerationAndQueuesBridgeTurn(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_turn", string(sessionstate.Created), time.Now().UTC(), nil)

	srv := &Server{
		cfg:     testServerConfig(dir),
		store:   st,
		runtime: instantRuntime{},
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"hello"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d body %s", rec.Code, rec.Body.String())
	}
	waitForSessionStatus(t, ctx, st, session.ID, string(sessionstate.RunningActive))

	var generations, networkRows, resourceRows, queuedTurns, userMessages int
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_generations WHERE session_id = ?`, session.ID).Scan(&generations); err != nil {
		t.Fatalf("count generations: %v", err)
	}
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM network_profiles WHERE session_id = ? AND allocation_state = 'live'`, session.ID).Scan(&networkRows); err != nil {
		t.Fatalf("count network rows: %v", err)
	}
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM runtime_generation_resources r
JOIN runtime_generations g ON g.generation_id = r.generation_id
WHERE g.session_id = ? AND r.resource_state = 'live'`, session.ID).Scan(&resourceRows); err != nil {
		t.Fatalf("count resource rows: %v", err)
	}
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM turns WHERE session_id = ? AND status = 'queued' AND generation_id IS NULL`, session.ID).Scan(&queuedTurns); err != nil {
		t.Fatalf("count turns: %v", err)
	}
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM messages WHERE session_id = ? AND role = 'user' AND content = 'hello'`, session.ID).Scan(&userMessages); err != nil {
		t.Fatalf("count user messages: %v", err)
	}
	if generations != 1 || networkRows != 1 || resourceRows != 1 || queuedTurns != 1 || userMessages != 1 {
		t.Fatalf("unexpected bridge enqueue rows: generations=%d network=%d resources=%d queued_turns=%d user_messages=%d", generations, networkRows, resourceRows, queuedTurns, userMessages)
	}
	var generationID string
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT generation_id FROM runtime_generations WHERE session_id = ?`, session.ID).Scan(&generationID); err != nil {
		t.Fatalf("query generation id: %v", err)
	}
	plan, err := st.GetGenerationPlan(ctx, generationID)
	if err != nil {
		t.Fatalf("fresh start should persist generation plan: %v", err)
	}
	if err := generationplan.Validate(generationplan.ValidateParams{Payload: plan.CanonicalPayload}); err != nil {
		t.Fatalf("fresh start persisted invalid generation plan: %v\n%s", err, plan.CanonicalPayload)
	}
	var planPayload map[string]any
	if err := json.Unmarshal(plan.CanonicalPayload, &planPayload); err != nil {
		t.Fatalf("decode generation plan: %v", err)
	}
	identity := planPayload["identity"].(map[string]any)
	runscPin := planPayload["runsc_pin"].(map[string]any)
	if identity["session_id"] != session.ID || identity["generation_id"] != generationID || runscPin["binary_digest"] != "sha256:runsc-test" {
		t.Fatalf("generation plan did not capture launch identity/runsc pin: %s", plan.CanonicalPayload)
	}
	if _, ok := planPayload["projection_digests"]; ok {
		t.Fatalf("generation plan must not embed projection digests: %s", plan.CanonicalPayload)
	}
	driverPlan := planPayload["driver"].(map[string]any)
	driverCapabilities := driverPlan["capability_snapshot"].(map[string]any)
	driverFeatures := driverCapabilities["features"].(map[string]any)
	featurePolicy := planPayload["feature_policy"].(map[string]any)
	providerPlan := planPayload["runtime_provider"].(map[string]any)
	providerCapabilities := providerPlan["capability_snapshot"].(map[string]any)
	if driverFeatures["compaction"] != "supported" ||
		driverFeatures["interrupt"] != "unsupported" ||
		featurePolicy["compaction"] != "required" ||
		featurePolicy["interrupt"] != "unsupported" ||
		featurePolicy["legacy_supports_compaction"] != true ||
		featurePolicy["legacy_supports_interrupt"] != false ||
		providerCapabilities["vocabulary_version"] != "1" {
		t.Fatalf("generation plan did not freeze typed capability policy: %s", plan.CanonicalPayload)
	}
	projections, err := st.ListGenerationPlanProjections(ctx, generationID)
	if err != nil {
		t.Fatalf("list generation plan projections: %v", err)
	}
	if len(projections) != 6 {
		t.Fatalf("projection count=%d want 6: %+v", len(projections), projections)
	}
	projectionKinds := map[string]string{}
	for _, projection := range projections {
		if projection.PlanDigest != plan.PlanDigest {
			t.Fatalf("projection %s plan digest=%s want %s", projection.ProjectionKind, projection.PlanDigest, plan.PlanDigest)
		}
		if !strings.HasPrefix(projection.PayloadDigest, "sha256:") {
			t.Fatalf("projection %s payload digest is not sha256: %s", projection.ProjectionKind, projection.PayloadDigest)
		}
		projectionKinds[projection.ProjectionKind] = projection.PayloadDigest
	}
	contract, err := st.GetSandboxContractForGeneration(ctx, session.ID, generationID)
	if err != nil {
		t.Fatalf("load sandbox contract: %v", err)
	}
	if projectionKinds["sandbox_contract"] != contract.SandboxContractDigest ||
		projectionKinds["control_manifest"] == "" ||
		projectionKinds["oci_spec"] == "" {
		t.Fatalf("unexpected projection digests: %+v", projectionKinds)
	}
}

func TestSendMessageReusesActiveGenerationArtifacts(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_reuse", string(sessionstate.Created), time.Now().UTC(), nil)
	srv := &Server{
		cfg:     testServerConfig(dir),
		store:   st,
		runtime: instantRuntime{},
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)
	atomic.StoreInt64(&instantRuntimePrepareCalls, 0)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"first"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected first status 202, got %d body %s", rec.Code, rec.Body.String())
	}
	waitForSessionStatus(t, ctx, st, session.ID, string(sessionstate.RunningActive))
	if got := atomic.LoadInt64(&instantRuntimePrepareCalls); got != 2 {
		t.Fatalf("first turn prepare calls=%d want 2", got)
	}
	var generationID string
	var firstTurnID int64
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.generation_id, t.id
FROM runtime_generations g
JOIN turns t ON t.session_id = g.session_id
WHERE g.session_id = ?
  AND t.status = 'queued'
  AND t.content = 'first'`, session.ID).Scan(&generationID, &firstTurnID); err != nil {
		t.Fatalf("query first queued turn: %v", err)
	}
	leaseOwner := store.GenerationLeaseOwner(owner.UUID)
	if grant, ok, err := st.ClaimNextTurn(ctx, store.ClaimNextTurnParams{
		SessionID:    session.ID,
		GenerationID: generationID,
		Owner:        leaseOwner,
		RequestID:    "req_first",
		LeaseTTL:     time.Minute,
		Now:          time.Now().UTC(),
	}); err != nil || !ok || grant.TurnID != firstTurnID {
		t.Fatalf("claim first turn: ok=%v grant=%+v err=%v", ok, grant, err)
	}
	if _, err := st.AckTurnStarted(ctx, store.AckStartedParams{
		SessionID:       session.ID,
		GenerationID:    generationID,
		TurnID:          firstTurnID,
		Owner:           leaseOwner,
		SandboxSourceIP: serverSandboxSourceIPForGeneration(t, ctx, st, generationID),
		LeaseTTL:        time.Minute,
		Now:             time.Now().UTC(),
	}); err != nil {
		t.Fatalf("ack first turn started: %v", err)
	}
	if _, err := st.CompleteTurn(ctx, store.CompleteTurnParams{
		SessionID:      session.ID,
		GenerationID:   generationID,
		TurnID:         firstTurnID,
		Owner:          leaseOwner,
		TerminalStatus: "completed",
		Now:            time.Now().UTC(),
	}); err != nil {
		t.Fatalf("complete first turn: %v", err)
	}
	if err := st.UpdateSessionStatusAndActivity(ctx, session.ID, string(sessionstate.RunningIdle), nil, time.Now().UTC()); err != nil {
		t.Fatalf("mark session idle: %v", err)
	}

	req = httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"second"}`))
	rec = httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected second status 202, got %d body %s", rec.Code, rec.Body.String())
	}
	waitForSessionStatus(t, ctx, st, session.ID, string(sessionstate.RunningActive))
	if got := atomic.LoadInt64(&instantRuntimePrepareCalls); got != 2 {
		t.Fatalf("active generation should reuse prepared artifacts, prepare calls=%d", got)
	}
	var completedTurns, queuedTurns int
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM turns WHERE session_id = ? AND status = 'completed'`, session.ID).Scan(&completedTurns); err != nil {
		t.Fatalf("count turns: %v", err)
	}
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM turns WHERE session_id = ? AND status = 'queued' AND content = 'second'`, session.ID).Scan(&queuedTurns); err != nil {
		t.Fatalf("count queued turns: %v", err)
	}
	if completedTurns != 1 || queuedTurns != 1 {
		t.Fatalf("unexpected turn statuses after reuse: completed=%d queued=%d", completedTurns, queuedTurns)
	}
}
