package store

import (
	"context"
	"errors"
	"net/netip"
	"testing"
	"time"
)

func TestAllocatorReturnsPoolExhaustedBeforeRows(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	cfg.CIDRPool = netip.MustParsePrefix("10.250.0.0/30")

	createStoreSession(t, ctx, st, "sess_one")
	if _, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_one",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    cfg,
	}); err != nil {
		t.Fatalf("allocate first generation: %v", err)
	}
	createStoreSession(t, ctx, st, "sess_two")
	_, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_two",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    cfg,
	})
	if !errors.Is(err, ErrPoolExhausted) {
		t.Fatalf("expected pool exhausted, got %v", err)
	}
	var generations int
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_generations`).Scan(&generations); err != nil {
		t.Fatalf("count generations: %v", err)
	}
	if generations != 1 {
		t.Fatalf("pool exhaustion should not create a generation row, got %d", generations)
	}
}
