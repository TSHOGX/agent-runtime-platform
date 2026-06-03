package store

import (
	"context"
	"net/netip"
	"path/filepath"
	"testing"
	"time"
)

func TestRenewLiveGenerationLeasesKeepsIdle7AGenerationAlive(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	createStoreSession(t, ctx, st, "sess_idle")
	now := time.Now().UTC()
	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_idle",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, "sess_idle", allocation.GenerationID, allocation.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark resources live: %v", err)
	}
	renewed, err := st.RenewLiveGenerationLeases(ctx, RenewLiveGenerationsParams{
		Owner:    allocation.Owner,
		LeaseTTL: time.Minute,
		Now:      now.Add(30 * time.Second),
	})
	if err != nil {
		t.Fatalf("renew live generation leases: %v", err)
	}
	if renewed != 1 {
		t.Fatalf("expected one renewed generation, got %d", renewed)
	}
	var leaseExpires string
	if err := st.db.QueryRowContext(ctx, `SELECT lease_expires_at FROM runtime_generations WHERE generation_id = ?`, allocation.GenerationID).Scan(&leaseExpires); err != nil {
		t.Fatalf("query lease expiry: %v", err)
	}
	if got := parseTime(leaseExpires); !got.After(now.Add(time.Minute)) {
		t.Fatalf("lease expiry was not extended enough: %s", got)
	}
}

func TestListBridgePollGenerationsFiltersCurrentOwnerLiveResources(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	cfg.CIDRPool = netip.MustParsePrefix("10.240.0.0/27")
	now := time.Now().UTC()

	createStoreSession(t, ctx, st, "sess_poll")
	pollAllocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_poll",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate poll generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, "sess_poll", pollAllocation.GenerationID, pollAllocation.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark poll generation live: %v", err)
	}
	pollResource := createLiveRuntimeResourceInstanceForAllocation(t, ctx, st, "sess_poll", pollAllocation, owner.UUID, "host-1", now.Add(2*time.Second))
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generation_resources
SET resource_state = 'ready',
    bridge_dir_path = ?
WHERE generation_id = ?`, filepath.Join(t.TempDir(), "stale-bridge"), pollAllocation.GenerationID); err != nil {
		t.Fatalf("make poll resource state stale: %v", err)
	}

	createStoreSession(t, ctx, st, "sess_other")
	otherAllocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_other",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate other generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, "sess_other", otherAllocation.GenerationID, otherAllocation.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark other generation live: %v", err)
	}
	createLiveRuntimeResourceInstanceForAllocation(t, ctx, st, "sess_other", otherAllocation, owner.UUID, "host-1", now.Add(2*time.Second))
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generations
SET lease_owner = ?
WHERE generation_id = ?`, GenerationLeaseOwner("other-owner"), otherAllocation.GenerationID); err != nil {
		t.Fatalf("move other generation to another owner: %v", err)
	}

	createStoreSession(t, ctx, st, "sess_ready_only")
	readyOnly, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_ready_only",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate ready-only generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, "sess_ready_only", readyOnly.GenerationID, readyOnly.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark ready-only generation live: %v", err)
	}
	createLiveRuntimeResourceInstanceForAllocation(t, ctx, st, "sess_ready_only", readyOnly, owner.UUID, "host-1", now.Add(2*time.Second))
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_resource_instances
SET state = 'ready'
WHERE generation_id = ?`, readyOnly.GenerationID); err != nil {
		t.Fatalf("move ready-only runtime resource to ready: %v", err)
	}

	generations, err := st.ListBridgePollGenerations(ctx, pollAllocation.Owner, now.Add(2*time.Second), 0)
	if err != nil {
		t.Fatalf("list bridge poll generations: %v", err)
	}
	if len(generations) != 1 {
		t.Fatalf("generations=%+v, want one current-owner live generation", generations)
	}
	if generations[0].SessionID != "sess_poll" ||
		generations[0].GenerationID != pollAllocation.GenerationID ||
		generations[0].BridgeDirPath != pollResource.BridgeDirPath {
		t.Fatalf("unexpected poll generation: %+v", generations[0])
	}
}

func TestListBridgePollGenerationsIncludesAckStartedExpiredLeaseDuringGrace(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	now := time.Now().UTC()

	createStoreSession(t, ctx, st, "sess_poll_recover")
	recoverable, recoverableTurnID := createExpiredAckStartedTurn(t, ctx, st, owner.UUID, cfg, "sess_poll_recover", now, 30*time.Second)
	previousOwner := GenerationLeaseOwner("previous-owner")
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generations
SET lease_owner = ?
WHERE generation_id = ?`, previousOwner, recoverable.GenerationID); err != nil {
		t.Fatalf("move recoverable generation owner: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `
UPDATE turns
SET lease_owner = ?
WHERE id = ?`, previousOwner, recoverableTurnID); err != nil {
		t.Fatalf("move recoverable turn owner: %v", err)
	}

	createStoreSession(t, ctx, st, "sess_poll_expired")
	expired, _ := createExpiredAckStartedTurn(t, ctx, st, owner.UUID, cfg, "sess_poll_expired", now, 2*time.Minute)

	generations, err := st.ListBridgePollGenerations(ctx, recoverable.Owner, now, time.Minute)
	if err != nil {
		t.Fatalf("list bridge poll generations: %v", err)
	}
	if len(generations) != 1 {
		t.Fatalf("generations=%+v, want only recoverable ack-started generation", generations)
	}
	if generations[0].SessionID != "sess_poll_recover" ||
		generations[0].GenerationID != recoverable.GenerationID ||
		generations[0].BridgeDirPath == "" {
		t.Fatalf("unexpected recoverable generation: %+v", generations[0])
	}

	generations, err = st.ListBridgePollGenerations(ctx, recoverable.Owner, now, 0)
	if err != nil {
		t.Fatalf("list bridge poll generations without grace: %v", err)
	}
	for _, generation := range generations {
		if generation.GenerationID == recoverable.GenerationID || generation.GenerationID == expired.GenerationID {
			t.Fatalf("expired ack-started generation listed without grace: %+v", generations)
		}
	}
}
