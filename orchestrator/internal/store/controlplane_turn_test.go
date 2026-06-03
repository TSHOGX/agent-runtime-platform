package store

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/sessionstate"
)

func TestTurnHelperClaimAckComplete(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	createStoreSession(t, ctx, st, "sess_turn")
	createActiveGeneration(t, ctx, st, "sess_turn", "gen_turn", "owner")
	if _, err := st.EnqueueTurn(ctx, "sess_turn", "first", time.Now().UTC()); err != nil {
		t.Fatalf("enqueue first: %v", err)
	}
	if _, err := st.EnqueueTurn(ctx, "sess_turn", "second", time.Now().UTC()); err != nil {
		t.Fatalf("enqueue second: %v", err)
	}

	now := time.Now().UTC()
	claim := ClaimNextTurnParams{
		SessionID:    "sess_turn",
		GenerationID: "gen_turn",
		Owner:        "owner",
		RequestID:    "req-1",
		LeaseTTL:     time.Minute,
		Now:          now,
	}
	grant, ok, err := st.ClaimNextTurn(ctx, claim)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if !ok || grant.Sequence != 1 || grant.Content != "first" || grant.Replayed {
		t.Fatalf("unexpected grant: ok=%v grant=%+v", ok, grant)
	}
	replay, ok, err := st.ClaimNextTurn(ctx, claim)
	if err != nil {
		t.Fatalf("replay claim: %v", err)
	}
	if !ok || !replay.Replayed || replay.TurnID != grant.TurnID {
		t.Fatalf("unexpected replay grant: ok=%v replay=%+v", ok, replay)
	}
	if _, err := st.AckTurnStarted(ctx, AckStartedParams{
		SessionID:       "sess_turn",
		GenerationID:    "gen_turn",
		TurnID:          grant.TurnID,
		Owner:           "owner",
		SandboxSourceIP: "10.240.0.2",
		LeaseTTL:        time.Minute,
		Now:             now.Add(time.Second),
	}); err != nil {
		t.Fatalf("ack started: %v", err)
	}
	if _, err := st.CompleteTurn(ctx, CompleteTurnParams{
		SessionID:      "sess_turn",
		GenerationID:   "gen_turn",
		TurnID:         grant.TurnID,
		Owner:          "owner",
		TerminalStatus: "completed",
		Now:            now.Add(2 * time.Second),
	}); err != nil {
		t.Fatalf("complete: %v", err)
	}

	var turnStatus, generationStatus, sessionStatus string
	if err := st.db.QueryRowContext(ctx, `
SELECT t.status, g.status, s.status
FROM turns t
JOIN runtime_generations g ON g.generation_id = t.generation_id
JOIN sessions s ON s.id = t.session_id
WHERE t.id = ?`, grant.TurnID).Scan(&turnStatus, &generationStatus, &sessionStatus); err != nil {
		t.Fatalf("query completion state: %v", err)
	}
	if turnStatus != "completed" || generationStatus != "active" || sessionStatus == string(sessionstate.RunningIdle) {
		t.Fatalf("unexpected statuses: turn=%s generation=%s session=%s", turnStatus, generationStatus, sessionStatus)
	}

	secondClaim := claim
	secondClaim.RequestID = "req-2"
	secondClaim.Now = now.Add(3 * time.Second)
	secondGrant, ok, err := st.ClaimNextTurn(ctx, secondClaim)
	if err != nil {
		t.Fatalf("claim second: %v", err)
	}
	if !ok || secondGrant.Sequence != 2 || secondGrant.Content != "second" || secondGrant.Replayed {
		t.Fatalf("unexpected second grant: ok=%v grant=%+v", ok, secondGrant)
	}
	if _, err := st.AckTurnStarted(ctx, AckStartedParams{
		SessionID:       "sess_turn",
		GenerationID:    "gen_turn",
		TurnID:          secondGrant.TurnID,
		Owner:           "owner",
		SandboxSourceIP: "10.240.0.2",
		LeaseTTL:        time.Minute,
		Now:             now.Add(4 * time.Second),
	}); err != nil {
		t.Fatalf("ack second started: %v", err)
	}
	if _, err := st.CompleteTurn(ctx, CompleteTurnParams{
		SessionID:      "sess_turn",
		GenerationID:   "gen_turn",
		TurnID:         secondGrant.TurnID,
		Owner:          "owner",
		TerminalStatus: "completed",
		Now:            now.Add(5 * time.Second),
	}); err != nil {
		t.Fatalf("complete second: %v", err)
	}

	var lastActivityAt string
	if err := st.db.QueryRowContext(ctx, `
SELECT g.status, s.status, COALESCE(s.last_activity_at, '')
FROM runtime_generations g
JOIN sessions s ON s.active_generation_id = g.generation_id
WHERE g.generation_id = ?`, "gen_turn").Scan(&generationStatus, &sessionStatus, &lastActivityAt); err != nil {
		t.Fatalf("query final completion state: %v", err)
	}
	if generationStatus != "idle" || sessionStatus != string(sessionstate.RunningIdle) {
		t.Fatalf("unexpected final statuses: generation=%s session=%s", generationStatus, sessionStatus)
	}
	if lastActivityAt != formatTime(now.Add(5*time.Second)) {
		t.Fatalf("last_activity_at=%s want %s", lastActivityAt, formatTime(now.Add(5*time.Second)))
	}
	var contexts int
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM active_model_request_contexts`).Scan(&contexts); err != nil {
		t.Fatalf("context count: %v", err)
	}
	if contexts != 0 {
		t.Fatalf("expected context cleanup, got %d", contexts)
	}
}

func TestClaimNextTurnRequiresLiveRuntimeResourceInstance(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	createStoreSession(t, ctx, st, "sess_turn_resource")
	createActiveGeneration(t, ctx, st, "sess_turn_resource", "gen_turn_resource", "owner")
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_resource_instances
SET state = 'ready'
WHERE generation_id = 'gen_turn_resource'`); err != nil {
		t.Fatalf("downgrade runtime resource state: %v", err)
	}
	if _, err := st.EnqueueTurn(ctx, "sess_turn_resource", "blocked", time.Now().UTC()); err != nil {
		t.Fatalf("enqueue turn: %v", err)
	}
	grant, ok, err := st.ClaimNextTurn(ctx, ClaimNextTurnParams{
		SessionID:    "sess_turn_resource",
		GenerationID: "gen_turn_resource",
		Owner:        "owner",
		RequestID:    "req-resource-not-live",
		LeaseTTL:     time.Minute,
		Now:          time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("claim without live resource should return no work, got err=%v", err)
	}
	if ok {
		t.Fatalf("claim should require live runtime resource, got grant=%+v", grant)
	}
}

func TestAckTurnStartedRequiresLiveRuntimeResourceInstance(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	createStoreSession(t, ctx, st, "sess_ack_resource")
	createActiveGeneration(t, ctx, st, "sess_ack_resource", "gen_ack_resource", "owner")
	turnID, err := st.EnqueueTurn(ctx, "sess_ack_resource", "blocked ack", time.Now().UTC())
	if err != nil {
		t.Fatalf("enqueue turn: %v", err)
	}
	now := time.Now().UTC()
	grant, ok, err := st.ClaimNextTurn(ctx, ClaimNextTurnParams{
		SessionID:    "sess_ack_resource",
		GenerationID: "gen_ack_resource",
		Owner:        "owner",
		RequestID:    "req-ack-resource",
		LeaseTTL:     time.Minute,
		Now:          now,
	})
	if err != nil || !ok || grant.TurnID != turnID {
		t.Fatalf("claim setup: ok=%v grant=%+v err=%v", ok, grant, err)
	}
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_resource_instances
SET state = 'ready'
WHERE generation_id = 'gen_ack_resource'`); err != nil {
		t.Fatalf("downgrade runtime resource state: %v", err)
	}
	if _, err := st.AckTurnStarted(ctx, AckStartedParams{
		SessionID:    "sess_ack_resource",
		GenerationID: "gen_ack_resource",
		TurnID:       turnID,
		Owner:        "owner",
		LeaseTTL:     time.Minute,
		Now:          now.Add(time.Second),
	}); err == nil || !strings.Contains(err.Error(), "generation ack_started CAS failed") {
		t.Fatalf("expected ack failure without live resource, got %v", err)
	}
	var turnStatus string
	var contexts int
	if err := st.db.QueryRowContext(ctx, `SELECT status FROM turns WHERE id = ?`, turnID).Scan(&turnStatus); err != nil {
		t.Fatalf("query turn: %v", err)
	}
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM active_model_request_contexts`).Scan(&contexts); err != nil {
		t.Fatalf("query active contexts: %v", err)
	}
	if turnStatus != "leased" || contexts != 0 {
		t.Fatalf("ack should not commit turn/context without live resource: turn=%s contexts=%d", turnStatus, contexts)
	}
}
