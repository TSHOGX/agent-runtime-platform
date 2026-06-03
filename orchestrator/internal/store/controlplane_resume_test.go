package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestResumeTurnRenewsOnlyActiveGenerationLease(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	createStoreSession(t, ctx, st, "sess_resume")
	createActiveGeneration(t, ctx, st, "sess_resume", "gen_resume", "owner")
	turnID, err := st.EnqueueTurn(ctx, "sess_resume", "resume", time.Now().UTC())
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	now := time.Now().UTC()
	grant, ok, err := st.ClaimNextTurn(ctx, ClaimNextTurnParams{
		SessionID:    "sess_resume",
		GenerationID: "gen_resume",
		Owner:        "owner",
		RequestID:    "req-1",
		LeaseTTL:     time.Minute,
		Now:          now,
	})
	if err != nil || !ok || grant.TurnID != turnID {
		t.Fatalf("claim setup: ok=%v grant=%+v err=%v", ok, grant, err)
	}
	resumed, ok, err := st.ResumeTurn(ctx, ResumeTurnParams{
		SessionID:    "sess_resume",
		GenerationID: "gen_resume",
		TurnID:       turnID,
		Owner:        "owner",
		LeaseTTL:     2 * time.Minute,
		Now:          now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if !ok || resumed.TurnID != turnID || resumed.Content != "resume" {
		t.Fatalf("unexpected resume grant: ok=%v grant=%+v", ok, resumed)
	}
	if !resumed.ExpiresAt.Equal(now.Add(time.Second).Add(2 * time.Minute)) {
		t.Fatalf("resume expires_at=%s want %s", resumed.ExpiresAt, now.Add(time.Second).Add(2*time.Minute))
	}
	_, ok, err = st.ResumeTurn(ctx, ResumeTurnParams{
		SessionID:    "sess_resume",
		GenerationID: "gen_wrong",
		TurnID:       turnID,
		Owner:        "owner",
		LeaseTTL:     time.Minute,
		Now:          now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("resume wrong generation: %v", err)
	}
	if ok {
		t.Fatalf("resume with wrong generation must return no work")
	}
}

func TestResumeTurnRecoversAckStartedExpiredLeaseDuringGrace(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	createStoreSession(t, ctx, st, "sess_resume_ack")
	now := time.Now().UTC()
	allocation, turnID := createExpiredAckStartedTurn(t, ctx, st, owner.UUID, cfg, "sess_resume_ack", now, 30*time.Second)
	previousOwner := GenerationLeaseOwner("previous-owner")
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generations
SET lease_owner = ?
WHERE generation_id = ?`, previousOwner, allocation.GenerationID); err != nil {
		t.Fatalf("move generation to previous owner: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `
UPDATE turns
SET lease_owner = ?
WHERE id = ?`, previousOwner, turnID); err != nil {
		t.Fatalf("move turn to previous owner: %v", err)
	}

	resumed, ok, err := st.ResumeTurn(ctx, ResumeTurnParams{
		SessionID:       "sess_resume_ack",
		GenerationID:    allocation.GenerationID,
		TurnID:          turnID,
		Owner:           allocation.Owner,
		LeaseTTL:        2 * time.Minute,
		AckStartedGrace: time.Minute,
		Now:             now,
	})
	if err != nil {
		t.Fatalf("resume expired ack-started turn: %v", err)
	}
	if !ok || resumed.TurnID != turnID || resumed.Content != "maybe already ran" || resumed.Attempt != 0 {
		t.Fatalf("unexpected resumed grant: ok=%v grant=%+v", ok, resumed)
	}
	if !resumed.ExpiresAt.Equal(now.Add(2 * time.Minute)) {
		t.Fatalf("resume expires_at=%s want %s", resumed.ExpiresAt, now.Add(2*time.Minute))
	}

	var turnStatus, turnOwner, turnExpires, generationStatus, generationOwner, generationExpires string
	if err := st.db.QueryRowContext(ctx, `
SELECT t.status, t.lease_owner, t.lease_expires_at, g.status, g.lease_owner, g.lease_expires_at
FROM turns t
JOIN runtime_generations g ON g.generation_id = t.generation_id
WHERE t.id = ?`, turnID).Scan(&turnStatus, &turnOwner, &turnExpires, &generationStatus, &generationOwner, &generationExpires); err != nil {
		t.Fatalf("query recovered leases: %v", err)
	}
	if turnStatus != "running" || generationStatus != "active" || turnOwner != allocation.Owner || generationOwner != allocation.Owner {
		t.Fatalf("unexpected recovered state: turn=%s/%s generation=%s/%s want owner %s", turnStatus, turnOwner, generationStatus, generationOwner, allocation.Owner)
	}
	if !parseTime(turnExpires).Equal(now.Add(2*time.Minute)) || !parseTime(generationExpires).Equal(now.Add(2*time.Minute)) {
		t.Fatalf("leases not renewed: turn=%s generation=%s", turnExpires, generationExpires)
	}
}

func TestResumeTurnRejectsAckStartedExpiredLeaseAfterGrace(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	createStoreSession(t, ctx, st, "sess_resume_ack_expired")
	now := time.Now().UTC()
	allocation, turnID := createExpiredAckStartedTurn(t, ctx, st, owner.UUID, cfg, "sess_resume_ack_expired", now, 2*time.Minute)

	_, ok, err := st.ResumeTurn(ctx, ResumeTurnParams{
		SessionID:       "sess_resume_ack_expired",
		GenerationID:    allocation.GenerationID,
		TurnID:          turnID,
		Owner:           allocation.Owner,
		LeaseTTL:        time.Minute,
		AckStartedGrace: time.Minute,
		Now:             now,
	})
	if err != nil {
		t.Fatalf("resume expired ack-started turn after grace: %v", err)
	}
	if ok {
		t.Fatalf("resume after ack-started grace must return no work")
	}
}
