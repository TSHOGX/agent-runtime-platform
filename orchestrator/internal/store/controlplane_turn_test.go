package store

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
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

func TestTurnHelperTerminalFailureAndCancelKeepGenerationCacheConsistent(t *testing.T) {
	for _, terminalStatus := range []string{"failed", "canceled"} {
		t.Run(terminalStatus, func(t *testing.T) {
			ctx := context.Background()
			st, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			t.Cleanup(func() { _ = st.Close() })

			sessionID := "sess_terminal_" + terminalStatus
			generationID := "gen_terminal_" + terminalStatus
			createStoreSession(t, ctx, st, sessionID)
			createActiveGeneration(t, ctx, st, sessionID, generationID, "owner")
			turnID, err := st.EnqueueTurn(ctx, sessionID, terminalStatus+" turn", time.Now().UTC())
			if err != nil {
				t.Fatalf("enqueue: %v", err)
			}

			now := time.Now().UTC()
			grant, ok, err := st.ClaimNextTurn(ctx, ClaimNextTurnParams{
				SessionID:    sessionID,
				GenerationID: generationID,
				Owner:        "owner",
				RequestID:    "req-" + terminalStatus,
				LeaseTTL:     time.Minute,
				Now:          now,
			})
			if err != nil || !ok || grant.TurnID != turnID {
				t.Fatalf("claim: ok=%v grant=%+v err=%v", ok, grant, err)
			}
			if _, err := st.AckTurnStarted(ctx, AckStartedParams{
				SessionID:       sessionID,
				GenerationID:    generationID,
				TurnID:          turnID,
				Owner:           "owner",
				SandboxSourceIP: "10.240.0.2",
				LeaseTTL:        time.Minute,
				Now:             now.Add(time.Second),
			}); err != nil {
				t.Fatalf("ack started: %v", err)
			}

			eventID, err := st.CompleteTurn(ctx, CompleteTurnParams{
				SessionID:      sessionID,
				GenerationID:   generationID,
				TurnID:         turnID,
				Owner:          "owner",
				TerminalStatus: terminalStatus,
				ErrorClass:     "test_" + terminalStatus,
				Error:          "terminal " + terminalStatus,
				EventType:      "ack_turn_completed",
				EventDedupeKey: "ack_completed:" + generationID,
				EventPayload: map[string]string{
					"status":      terminalStatus,
					"error_class": "test_" + terminalStatus,
					"error":       "terminal " + terminalStatus,
				},
				Now: now.Add(2 * time.Second),
			})
			if err != nil {
				t.Fatalf("complete %s: %v", terminalStatus, err)
			}
			if eventID == 0 {
				t.Fatalf("expected completion event id")
			}

			var turnStatus, turnErrorClass, generationStatus, sessionStatus, eventPayload string
			if err := st.db.QueryRowContext(ctx, `
SELECT t.status, COALESCE(t.error_class, ''), g.status, s.status
FROM turns t
JOIN runtime_generations g ON g.generation_id = t.generation_id
JOIN sessions s ON s.id = t.session_id
WHERE t.id = ?`, turnID).Scan(&turnStatus, &turnErrorClass, &generationStatus, &sessionStatus); err != nil {
				t.Fatalf("query terminal state: %v", err)
			}
			if turnStatus != terminalStatus ||
				turnErrorClass != "test_"+terminalStatus ||
				generationStatus != "idle" ||
				sessionStatus != string(sessionstate.RunningIdle) {
				t.Fatalf("unexpected terminal state: turn=%s error=%s generation=%s session=%s", turnStatus, turnErrorClass, generationStatus, sessionStatus)
			}
			if err := st.db.QueryRowContext(ctx, `SELECT payload FROM events WHERE event_id = ?`, eventID).Scan(&eventPayload); err != nil {
				t.Fatalf("query completion event payload: %v", err)
			}
			if !strings.Contains(eventPayload, `"status":"`+terminalStatus+`"`) ||
				!strings.Contains(eventPayload, `"session_marked_idle":true`) ||
				!strings.Contains(eventPayload, `"session_status":"running_idle"`) ||
				!strings.Contains(eventPayload, `"session_terminal":false`) {
				t.Fatalf("completion event payload missing session effect: %s", eventPayload)
			}
			var contexts int
			if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM active_model_request_contexts`).Scan(&contexts); err != nil {
				t.Fatalf("context count: %v", err)
			}
			if contexts != 0 {
				t.Fatalf("expected active proxy context cleanup, got %d", contexts)
			}
		})
	}
}

func TestClaimNextTurnConcurrentAttemptsOnlyOneWins(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	createStoreSession(t, ctx, st, "sess_claim_race")
	createActiveGeneration(t, ctx, st, "sess_claim_race", "gen_claim_race", "owner")
	turnID, err := st.EnqueueTurn(ctx, "sess_claim_race", "race", time.Now().UTC())
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	requestIDs := []string{"req-a", "req-b", "req-c", "req-d", "req-e", "req-f", "req-g", "req-h"}
	stores := make([]*Store, len(requestIDs))
	for i := range stores {
		storeConn, err := Open(ctx, dbPath)
		if err != nil {
			t.Fatalf("open contender %d: %v", i, err)
		}
		stores[i] = storeConn
		t.Cleanup(func() { _ = storeConn.Close() })
	}

	type claimResult struct {
		requestID string
		grant     TurnGrant
		ok        bool
		err       error
	}
	results := make(chan claimResult, len(requestIDs))
	start := make(chan struct{})
	var wg sync.WaitGroup
	claimAt := time.Now().UTC()
	for i, requestID := range requestIDs {
		wg.Add(1)
		go func(storeConn *Store, requestID string) {
			defer wg.Done()
			<-start
			grant, ok, err := storeConn.ClaimNextTurn(ctx, ClaimNextTurnParams{
				SessionID:    "sess_claim_race",
				GenerationID: "gen_claim_race",
				Owner:        "owner",
				RequestID:    requestID,
				LeaseTTL:     time.Minute,
				Now:          claimAt,
			})
			results <- claimResult{requestID: requestID, grant: grant, ok: ok, err: err}
		}(stores[i], requestID)
	}
	close(start)
	wg.Wait()
	close(results)

	var winner *claimResult
	for result := range results {
		if result.err != nil {
			if !strings.Contains(result.err.Error(), "database is locked") && !strings.Contains(result.err.Error(), "SQLITE_BUSY") {
				t.Fatalf("unexpected claim error for %s: %v", result.requestID, result.err)
			}
			continue
		}
		if !result.ok {
			continue
		}
		if winner != nil {
			t.Fatalf("multiple concurrent claims won: first=%+v second=%+v", *winner, result)
		}
		resultCopy := result
		winner = &resultCopy
	}
	if winner == nil {
		t.Fatalf("no concurrent claim won")
	}
	if winner.grant.TurnID != turnID || winner.grant.Sequence != 1 || winner.grant.Content != "race" {
		t.Fatalf("unexpected winning grant: %+v turnID=%d", winner.grant, turnID)
	}

	var status, generationID, owner, claimRequestID string
	if err := st.db.QueryRowContext(ctx, `
SELECT status, generation_id, lease_owner, claim_request_id
FROM turns
WHERE id = ?`, turnID).Scan(&status, &generationID, &owner, &claimRequestID); err != nil {
		t.Fatalf("query raced turn: %v", err)
	}
	if status != "leased" || generationID != "gen_claim_race" || owner != "owner" || claimRequestID != winner.requestID {
		t.Fatalf("turn lease was stolen or not written atomically: status=%s generation=%s owner=%s request=%s winner=%s",
			status, generationID, owner, claimRequestID, winner.requestID)
	}
	for _, requestID := range requestIDs {
		if requestID == winner.requestID {
			continue
		}
		replay, ok, err := st.ClaimNextTurn(ctx, ClaimNextTurnParams{
			SessionID:    "sess_claim_race",
			GenerationID: "gen_claim_race",
			Owner:        "owner",
			RequestID:    requestID,
			LeaseTTL:     time.Minute,
			Now:          claimAt.Add(time.Second),
		})
		if err != nil {
			t.Fatalf("loser replay %s: %v", requestID, err)
		}
		if ok {
			t.Fatalf("loser request %s replayed or stole winner grant: %+v", requestID, replay)
		}
	}
}

func TestTurnHelperRejectsWrongSessionGenerationBinding(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	createStoreSession(t, ctx, st, "sess_a")
	createStoreSession(t, ctx, st, "sess_b")
	createActiveGeneration(t, ctx, st, "sess_b", "gen_b", "owner")
	if _, err := st.EnqueueTurn(ctx, "sess_a", "work", time.Now().UTC()); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	_, ok, err := st.ClaimNextTurn(ctx, ClaimNextTurnParams{
		SessionID:    "sess_a",
		GenerationID: "gen_b",
		Owner:        "owner",
		RequestID:    "req",
		LeaseTTL:     time.Minute,
		Now:          time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("claim wrong binding: %v", err)
	}
	if ok {
		t.Fatalf("generation from another session must not claim turn")
	}
}
