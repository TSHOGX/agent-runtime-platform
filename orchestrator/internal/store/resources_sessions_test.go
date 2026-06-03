package store

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/sessionstate"
)

func TestSweepExpiredSessionsDestroysAndRejectsInputState(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	now := time.Now().UTC()
	expiredAt := now.Add(-time.Second)
	if err := st.CreateSession(ctx, Session{
		ID:        "sess_expired",
		UserID:    "lab",
		Status:    string(sessionstate.Created),
		DriverID:  "claude_code",
		Mode:      ModeForDriver("claude_code"),
		CreatedAt: now.Add(-time.Hour),
		UpdatedAt: now.Add(-time.Hour),
		ExpiresAt: &expiredAt,
	}); err != nil {
		t.Fatalf("create expired session: %v", err)
	}
	changed, err := st.SweepExpiredSessions(ctx, now)
	if err != nil {
		t.Fatalf("sweep expired sessions: %v", err)
	}
	if changed != 1 {
		t.Fatalf("expected one expired session swept, got %d", changed)
	}
	got, err := st.GetSession(ctx, "sess_expired")
	if err != nil {
		t.Fatalf("get expired session: %v", err)
	}
	if got.Status != string(sessionstate.Destroyed) || got.ErrorClass != "session_expired" {
		t.Fatalf("unexpected expired session: %+v", got)
	}

	if err := st.CreateSession(ctx, Session{
		ID:        "sess_expired_allocated",
		UserID:    "lab",
		Status:    string(sessionstate.RunningIdle),
		DriverID:  "claude_code",
		Mode:      ModeForDriver("claude_code"),
		CreatedAt: now.Add(-time.Hour),
		UpdatedAt: now.Add(-time.Hour),
		ExpiresAt: &expiredAt,
	}); err != nil {
		t.Fatalf("create expired allocated session: %v", err)
	}
	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_expired_allocated",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now.Add(-time.Minute),
		Config:    testAllocatorConfig(t),
	})
	if err != nil {
		t.Fatalf("allocate expired generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, "sess_expired_allocated", allocation.GenerationID, allocation.Owner, now.Add(-30*time.Second)); err != nil {
		t.Fatalf("mark expired resources live: %v", err)
	}
	changed, err = st.SweepExpiredSessions(ctx, now)
	if err != nil {
		t.Fatalf("sweep expired allocated session: %v", err)
	}
	if changed != 1 {
		t.Fatalf("expected one expired allocated session swept, got %d", changed)
	}
	var generationStatus, networkState, resourceState string
	if err := st.db.QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&generationStatus, &networkState, &resourceState); err != nil {
		t.Fatalf("query expired allocation state: %v", err)
	}
	if generationStatus != "failed" || networkState != "reclaimable" || resourceState != "reclaimable" {
		t.Fatalf("unexpected expired allocation state: generation=%s network=%s resource=%s", generationStatus, networkState, resourceState)
	}
}

func TestSweepExpiredSessionsIgnoresNullExpiry(t *testing.T) {
	ctx := context.Background()
	st, _ := openOwnedStore(t, ctx)
	now := time.Now().UTC()
	if err := st.CreateSession(ctx, Session{
		ID:        "sess_no_expiry",
		UserID:    "lab",
		Status:    string(sessionstate.Created),
		DriverID:  "claude_code",
		Mode:      ModeForDriver("claude_code"),
		CreatedAt: now.Add(-time.Hour),
		UpdatedAt: now.Add(-time.Hour),
		ExpiresAt: nil,
	}); err != nil {
		t.Fatalf("create no-expiry session: %v", err)
	}

	changed, err := st.SweepExpiredSessions(ctx, now.Add(24*time.Hour))
	if err != nil {
		t.Fatalf("sweep expired sessions: %v", err)
	}
	if changed != 0 {
		t.Fatalf("expected no sessions swept, got %d", changed)
	}
	got, err := st.GetSession(ctx, "sess_no_expiry")
	if err != nil {
		t.Fatalf("get no-expiry session: %v", err)
	}
	if got.Status != string(sessionstate.Created) {
		t.Fatalf("expected session to remain created, got %s", got.Status)
	}
}

func TestClearActiveSessionExpiryClearsOnlyActiveSessions(t *testing.T) {
	ctx := context.Background()
	st, _ := openOwnedStore(t, ctx)
	now := time.Now().UTC()
	expiredAt := now.Add(-time.Hour)
	sessions := []Session{
		{
			ID:        "sess_active_retained_expiry",
			UserID:    "lab",
			Status:    string(sessionstate.RunningIdle),
			DriverID:  "claude_code",
			Mode:      ModeForDriver("claude_code"),
			CreatedAt: now.Add(-2 * time.Hour),
			UpdatedAt: now.Add(-2 * time.Hour),
			ExpiresAt: &expiredAt,
		},
		{
			ID:        "sess_failed_retained_expiry",
			UserID:    "lab",
			Status:    string(sessionstate.Failed),
			DriverID:  "claude_code",
			Mode:      ModeForDriver("claude_code"),
			CreatedAt: now.Add(-2 * time.Hour),
			UpdatedAt: now.Add(-2 * time.Hour),
			ExpiresAt: &expiredAt,
		},
		{
			ID:        "sess_destroyed_retained_expiry",
			UserID:    "lab",
			Status:    string(sessionstate.Destroyed),
			DriverID:  "claude_code",
			Mode:      ModeForDriver("claude_code"),
			CreatedAt: now.Add(-2 * time.Hour),
			UpdatedAt: now.Add(-2 * time.Hour),
			ExpiresAt: &expiredAt,
		},
	}
	for _, session := range sessions {
		if err := st.CreateSession(ctx, session); err != nil {
			t.Fatalf("create session %s: %v", session.ID, err)
		}
	}

	changed, err := st.ClearActiveSessionExpiry(ctx, now)
	if err != nil {
		t.Fatalf("clear active session expiry: %v", err)
	}
	if changed != 1 {
		t.Fatalf("expected one active session cleared, got %d", changed)
	}
	changed, err = st.SweepExpiredSessions(ctx, now.Add(24*time.Hour))
	if err != nil {
		t.Fatalf("sweep after clear: %v", err)
	}
	if changed != 0 {
		t.Fatalf("expected cleared active session to survive sweep, swept %d", changed)
	}

	var activeExpiry, failedExpiry, destroyedExpiry sql.NullString
	if err := st.db.QueryRowContext(ctx, `SELECT expires_at FROM sessions WHERE id = 'sess_active_retained_expiry'`).Scan(&activeExpiry); err != nil {
		t.Fatalf("query active expiry: %v", err)
	}
	if err := st.db.QueryRowContext(ctx, `SELECT expires_at FROM sessions WHERE id = 'sess_failed_retained_expiry'`).Scan(&failedExpiry); err != nil {
		t.Fatalf("query failed expiry: %v", err)
	}
	if err := st.db.QueryRowContext(ctx, `SELECT expires_at FROM sessions WHERE id = 'sess_destroyed_retained_expiry'`).Scan(&destroyedExpiry); err != nil {
		t.Fatalf("query destroyed expiry: %v", err)
	}
	if activeExpiry.Valid {
		t.Fatalf("expected active expiry cleared, got %s", activeExpiry.String)
	}
	if !failedExpiry.Valid || !destroyedExpiry.Valid {
		t.Fatalf("expected terminal expiries preserved, failed=%v destroyed=%v", failedExpiry.Valid, destroyedExpiry.Valid)
	}
}

func TestSweepExpiredSessionsCancelsUnstartedTurnsButPreservesAckStartedLease(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	now := time.Now().UTC()
	expiredAt := now.Add(-time.Second)

	if err := st.CreateSession(ctx, Session{
		ID:        "sess_expired_queued",
		UserID:    "lab",
		Status:    string(sessionstate.RunningIdle),
		DriverID:  "claude_code",
		Mode:      ModeForDriver("claude_code"),
		CreatedAt: now.Add(-time.Hour),
		UpdatedAt: now.Add(-time.Hour),
		ExpiresAt: &expiredAt,
	}); err != nil {
		t.Fatalf("create expired queued session: %v", err)
	}
	queuedTurnID, err := st.EnqueueTurn(ctx, "sess_expired_queued", "queued", now.Add(-time.Minute))
	if err != nil {
		t.Fatalf("enqueue queued turn: %v", err)
	}

	if err := st.CreateSession(ctx, Session{
		ID:        "sess_expired_ack",
		UserID:    "lab",
		Status:    string(sessionstate.RunningActive),
		DriverID:  "claude_code",
		Mode:      ModeForDriver("claude_code"),
		CreatedAt: now.Add(-time.Hour),
		UpdatedAt: now.Add(-time.Hour),
		ExpiresAt: &expiredAt,
	}); err != nil {
		t.Fatalf("create expired ack session: %v", err)
	}
	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_expired_ack",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now.Add(-30 * time.Second),
		Config:    testAllocatorConfig(t),
	})
	if err != nil {
		t.Fatalf("allocate ack generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, "sess_expired_ack", allocation.GenerationID, allocation.Owner, now.Add(-29*time.Second)); err != nil {
		t.Fatalf("mark ack resources live: %v", err)
	}
	createLiveRuntimeResourceInstanceForAllocation(t, ctx, st, "sess_expired_ack", allocation, owner.UUID, "host-expired-ack", now.Add(-28*time.Second))
	ackTurnID, err := st.EnqueueTurn(ctx, "sess_expired_ack", "started", now.Add(-28*time.Second))
	if err != nil {
		t.Fatalf("enqueue ack turn: %v", err)
	}
	if grant, ok, err := st.ClaimNextTurn(ctx, ClaimNextTurnParams{
		SessionID:    "sess_expired_ack",
		GenerationID: allocation.GenerationID,
		Owner:        allocation.Owner,
		RequestID:    "claim_expired_ack",
		LeaseTTL:     time.Minute,
		Now:          now.Add(-27 * time.Second),
	}); err != nil || !ok || grant.TurnID != ackTurnID {
		t.Fatalf("claim ack turn: ok=%v grant=%+v err=%v", ok, grant, err)
	}
	if _, err := st.AckTurnStarted(ctx, AckStartedParams{
		SessionID:       "sess_expired_ack",
		GenerationID:    allocation.GenerationID,
		TurnID:          ackTurnID,
		Owner:           allocation.Owner,
		SandboxSourceIP: sandboxSourceIPForGeneration(t, ctx, st, allocation.GenerationID),
		LeaseTTL:        time.Minute,
		Now:             now.Add(-26 * time.Second),
	}); err != nil {
		t.Fatalf("ack turn started: %v", err)
	}

	changed, err := st.SweepExpiredSessions(ctx, now)
	if err != nil {
		t.Fatalf("sweep expired sessions: %v", err)
	}
	if changed != 2 {
		t.Fatalf("expired sessions changed=%d want 2", changed)
	}

	var queuedStatus, queuedError, ackStatus, generationStatus, networkState, resourceState string
	if err := st.db.QueryRowContext(ctx, `
SELECT status, COALESCE(error_class, '')
FROM turns
WHERE id = ?`, queuedTurnID).Scan(&queuedStatus, &queuedError); err != nil {
		t.Fatalf("query queued turn: %v", err)
	}
	if err := st.db.QueryRowContext(ctx, `
SELECT t.status, g.status, n.allocation_state, r.resource_state
FROM turns t
JOIN runtime_generations g ON g.generation_id = t.generation_id
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE t.id = ?`, ackTurnID).Scan(&ackStatus, &generationStatus, &networkState, &resourceState); err != nil {
		t.Fatalf("query ack-started state: %v", err)
	}
	if queuedStatus != "canceled" || queuedError != "session_expired" {
		t.Fatalf("queued turn not canceled by TTL: status=%s error=%s", queuedStatus, queuedError)
	}
	if ackStatus != "running" || generationStatus != "active" || networkState != "live" || resourceState != "live" {
		t.Fatalf("ack-started lease should be preserved: turn=%s generation=%s network=%s resource=%s", ackStatus, generationStatus, networkState, resourceState)
	}
}

func TestUpdateSessionStatusDoesNotResurrectDestroyedSession(t *testing.T) {
	ctx := context.Background()
	st, _ := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_terminal")
	if err := st.UpdateSessionStatus(ctx, "sess_terminal", string(sessionstate.Destroyed), nil); err != nil {
		t.Fatalf("destroy session: %v", err)
	}
	if err := st.UpdateSessionStatusAndActivity(ctx, "sess_terminal", string(sessionstate.RunningIdle), nil, time.Now().UTC()); err != nil {
		t.Fatalf("attempt resurrect destroyed session: %v", err)
	}
	got, err := st.GetSession(ctx, "sess_terminal")
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if got.Status != string(sessionstate.Destroyed) {
		t.Fatalf("destroyed session was resurrected as %s", got.Status)
	}
}

func TestDestroySessionCancelsPendingTurnsAndReclaimsGeneration(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	sessionID := "sess_destroy_pending"
	createStoreSession(t, ctx, st, sessionID)
	now := time.Now().UTC()
	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: sessionID,
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    testAllocatorConfig(t),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, sessionID, allocation.GenerationID, allocation.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark generation live: %v", err)
	}
	enqueued, err := st.EnqueueTurnMessage(ctx, EnqueueTurnMessageParams{
		SessionID: sessionID,
		Content:   "hello",
		Now:       now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("enqueue turn: %v", err)
	}

	destroyedAt := now.Add(3 * time.Second)
	result, err := st.DestroySession(ctx, sessionID, destroyedAt)
	if err != nil {
		t.Fatalf("destroy session: %v", err)
	}
	if len(result.GenerationIDs) != 1 || result.GenerationIDs[0] != allocation.GenerationID || result.EventID == 0 {
		t.Fatalf("unexpected destroy session result: %+v", result)
	}

	var sessionStatus, turnStatus, turnErrorClass, generationStatus, generationErrorClass, networkState, resourceState string
	if err := st.db.QueryRowContext(ctx, `
SELECT s.status, t.status, COALESCE(t.error_class, ''), g.status, COALESCE(g.error_class, ''),
       n.allocation_state, r.resource_state
FROM sessions s
JOIN turns t ON t.session_id = s.id
JOIN runtime_generations g ON g.session_id = s.id
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE s.id = ?
  AND t.id = ?`, sessionID, enqueued.TurnID).Scan(
		&sessionStatus,
		&turnStatus,
		&turnErrorClass,
		&generationStatus,
		&generationErrorClass,
		&networkState,
		&resourceState,
	); err != nil {
		t.Fatalf("query destroyed state: %v", err)
	}
	if sessionStatus != string(sessionstate.Destroyed) ||
		turnStatus != "canceled" ||
		turnErrorClass != "session_destroyed" ||
		generationStatus != "failed" ||
		generationErrorClass != "session_destroyed" ||
		networkState != "reclaimable" ||
		resourceState != "reclaimable" {
		t.Fatalf("unexpected destroyed state: session=%s turn=%s turn_error=%s generation=%s generation_error=%s network=%s resource=%s",
			sessionStatus, turnStatus, turnErrorClass, generationStatus, generationErrorClass, networkState, resourceState)
	}
	var eventType, eventPayload string
	if err := st.db.QueryRowContext(ctx, `SELECT type, payload FROM events WHERE event_id = ?`, result.EventID).Scan(&eventType, &eventPayload); err != nil {
		t.Fatalf("query destroyed event: %v", err)
	}
	if eventType != "session.destroyed" || !strings.Contains(eventPayload, `"terminal":true`) {
		t.Fatalf("unexpected destroyed event: type=%s payload=%s", eventType, eventPayload)
	}
}

func TestCancelTerminalSessionPendingTurnsRepairsTerminalQueue(t *testing.T) {
	ctx := context.Background()
	st, _ := openOwnedStore(t, ctx)
	sessionID := "sess_terminal_queue"
	createStoreSession(t, ctx, st, sessionID)
	now := time.Now().UTC()
	enqueued, err := st.EnqueueTurnMessage(ctx, EnqueueTurnMessageParams{
		SessionID: sessionID,
		Content:   "hello",
		Now:       now,
	})
	if err != nil {
		t.Fatalf("enqueue turn: %v", err)
	}
	if err := st.UpdateSessionStatus(ctx, sessionID, string(sessionstate.Destroyed), nil); err != nil {
		t.Fatalf("destroy session: %v", err)
	}

	canceled, err := st.CancelTerminalSessionPendingTurns(ctx, now.Add(time.Second))
	if err != nil {
		t.Fatalf("cancel terminal pending turns: %v", err)
	}
	if canceled != 1 {
		t.Fatalf("canceled=%d want 1", canceled)
	}

	var status, errorClass, errText string
	if err := st.db.QueryRowContext(ctx, `
SELECT status, COALESCE(error_class, ''), COALESCE(error, '')
FROM turns
WHERE id = ?`, enqueued.TurnID).Scan(&status, &errorClass, &errText); err != nil {
		t.Fatalf("query turn: %v", err)
	}
	if status != "canceled" || errorClass != "session_destroyed" || errText != "session_destroyed" {
		t.Fatalf("unexpected repaired turn: status=%s error_class=%s error=%s", status, errorClass, errText)
	}
}
