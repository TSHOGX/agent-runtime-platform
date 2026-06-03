package store

import (
	"context"
	"net/netip"
	"strings"
	"testing"
	"time"
)

func TestExpiredRuntimeRecoveryAndReaperTransitions(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	createStoreSession(t, ctx, st, "sess_recover")
	now := time.Now().UTC()
	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_recover",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Second,
		Now:       now.Add(-time.Minute),
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generations SET status = 'restoring', lease_expires_at = ? WHERE generation_id = ?`, formatTime(now.Add(-time.Second)), allocation.GenerationID); err != nil {
		t.Fatalf("set restoring: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `
UPDATE network_profiles SET allocation_state = 'recreating' WHERE generation_id = ?`, allocation.GenerationID); err != nil {
		t.Fatalf("set recreating network: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generation_resources SET resource_state = 'recreating' WHERE generation_id = ?`, allocation.GenerationID); err != nil {
		t.Fatalf("set recreating resource: %v", err)
	}
	createLiveRuntimeResourceInstanceForAllocation(t, ctx, st, "sess_recover", allocation, owner.UUID, "host-recover", now.Add(-30*time.Second))

	recovered, err := recoverCleanedAllocations(t, ctx, st, StartupRecoveryParams{
		OwnerUUID:       owner.UUID,
		Now:             now,
		LeaseTTL:        time.Minute,
		ReconnectGrace:  30 * time.Second,
		AckStartedGrace: time.Minute,
	})
	if err != nil {
		t.Fatalf("recover allocations: %v", err)
	}
	if recovered.ExpiredLifecycleFailed != 1 {
		t.Fatalf("expected one lifecycle failure, got %+v", recovered)
	}
	var generationStatus, networkState, resourceState string
	if err := st.db.QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&generationStatus, &networkState, &resourceState); err != nil {
		t.Fatalf("query recovered state: %v", err)
	}
	if generationStatus != "failed" || networkState != "reclaimable" || resourceState != "reclaimable" {
		t.Fatalf("unexpected recovered state: generation=%s network=%s resource=%s", generationStatus, networkState, resourceState)
	}

	reaped, err := st.ReapResources(ctx, ReaperParams{OwnerUUID: owner.UUID, FailedRetention: 0, Now: now.Add(time.Second)})
	if err != nil {
		t.Fatalf("reap resources: %v", err)
	}
	if reaped.DestroyedAllocations != 0 {
		t.Fatalf("store reaper must not mark physical allocations destroyed, got %+v", reaped)
	}
	destroyable, err := st.ListDestroyableReclaimableGenerations(ctx, now.Add(time.Second), 0)
	if err != nil {
		t.Fatalf("list destroyable resources: %v", err)
	}
	if len(destroyable) != 1 || destroyable[0].GenerationID != allocation.GenerationID {
		t.Fatalf("unexpected destroyable resources: %+v", destroyable)
	}
	if err := st.MarkGenerationResourcesDestroyed(ctx, DestroyGenerationResourcesParams{
		SessionID:    "sess_recover",
		GenerationID: allocation.GenerationID,
		Now:          now.Add(2 * time.Second),
	}); err != nil {
		t.Fatalf("mark generation resources destroyed: %v", err)
	}
	if err := st.MarkGenerationResourcesDestroyed(ctx, DestroyGenerationResourcesParams{
		SessionID:    "sess_recover",
		GenerationID: allocation.GenerationID,
		Now:          now.Add(3 * time.Second),
	}); err != nil {
		t.Fatalf("mark already destroyed generation resources: %v", err)
	}
	if destroyable, err = st.ListDestroyableReclaimableGenerations(ctx, now.Add(3*time.Second), 0); err != nil {
		t.Fatalf("list destroyable after mark: %v", err)
	} else if len(destroyable) != 0 {
		t.Fatalf("destroyed generation must not remain destroyable: %+v", destroyable)
	}
}

func TestExpiredRuntimeRecoveryDoesNotReclaimUnrelatedFailedGeneration(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	now := time.Now().UTC()

	createStoreSession(t, ctx, st, "sess_crashed")
	crashed, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_crashed",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Second,
		Now:       now.Add(-time.Minute),
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate crashed generation: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generations SET status = 'starting', lease_expires_at = ? WHERE generation_id = ?`, formatTime(now.Add(-time.Second)), crashed.GenerationID); err != nil {
		t.Fatalf("set crashed generation starting: %v", err)
	}
	createLiveRuntimeResourceInstanceForAllocation(t, ctx, st, "sess_crashed", crashed, owner.UUID, "host-crashed", now.Add(-30*time.Second))

	createStoreSession(t, ctx, st, "sess_recent_failed")
	recentFailed, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_recent_failed",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now.Add(-30 * time.Second),
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate recent failed generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, "sess_recent_failed", recentFailed.GenerationID, recentFailed.Owner, now.Add(-20*time.Second)); err != nil {
		t.Fatalf("mark recent failed resources live: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'failed', ended_at = ?, lease_owner = NULL
WHERE generation_id = ?`, formatTime(now.Add(-5*time.Second)), recentFailed.GenerationID); err != nil {
		t.Fatalf("set recent failed generation: %v", err)
	}

	recovered, err := recoverCleanedAllocations(t, ctx, st, StartupRecoveryParams{
		OwnerUUID:       owner.UUID,
		Now:             now,
		LeaseTTL:        time.Minute,
		ReconnectGrace:  30 * time.Second,
		AckStartedGrace: time.Minute,
	})
	if err != nil {
		t.Fatalf("recover allocations: %v", err)
	}
	if recovered.ExpiredLifecycleFailed != 1 {
		t.Fatalf("expected one lifecycle failure, got %+v", recovered)
	}
	var crashedState, recentState string
	if err := st.db.QueryRowContext(ctx, `SELECT allocation_state FROM network_profiles WHERE generation_id = ?`, crashed.GenerationID).Scan(&crashedState); err != nil {
		t.Fatalf("query crashed allocation: %v", err)
	}
	if err := st.db.QueryRowContext(ctx, `SELECT allocation_state FROM network_profiles WHERE generation_id = ?`, recentFailed.GenerationID).Scan(&recentState); err != nil {
		t.Fatalf("query recent allocation: %v", err)
	}
	if crashedState != "reclaimable" || recentState != "live" {
		t.Fatalf("unexpected allocation states: crashed=%s recent_failed=%s", crashedState, recentState)
	}
}

func TestListExpiredRuntimeRecoveryCandidatesRequiresMatchingRuntimeResourceInstance(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	cfg.CIDRPool = netip.MustParsePrefix("10.240.0.0/27")
	now := time.Now().UTC()

	createExpiredIdle := func(sessionID string) GenerationAllocation {
		t.Helper()
		createStoreSession(t, ctx, st, sessionID)
		allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
			SessionID: sessionID,
			Owner:     GenerationLeaseOwner(owner.UUID),
			LeaseTTL:  time.Minute,
			Now:       now.Add(-3 * time.Minute),
			Config:    cfg,
		})
		if err != nil {
			t.Fatalf("allocate %s: %v", sessionID, err)
		}
		if err := st.MarkGenerationResourcesLive(ctx, sessionID, allocation.GenerationID, allocation.Owner, now.Add(-3*time.Minute+time.Second)); err != nil {
			t.Fatalf("mark %s live: %v", sessionID, err)
		}
		if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generations
SET lease_expires_at = ?
WHERE generation_id = ?`, formatTime(now.Add(-2*time.Minute)), allocation.GenerationID); err != nil {
			t.Fatalf("expire %s lease: %v", sessionID, err)
		}
		return allocation
	}

	valid := createExpiredIdle("sess_recovery_instance_valid")
	validInstance := createLiveRuntimeResourceInstanceForAllocation(t, ctx, st, "sess_recovery_instance_valid", valid, owner.UUID, "host-valid", now.Add(-2*time.Minute+time.Second))
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generation_resources
SET runsc_container_id = ?
WHERE generation_id = ?`, "legacy-"+valid.GenerationID, valid.GenerationID); err != nil {
		t.Fatalf("set stale legacy runtime id: %v", err)
	}

	createExpiredIdle("sess_recovery_instance_missing")

	mismatch := createExpiredIdle("sess_recovery_instance_mismatch")
	createLiveRuntimeResourceInstanceForAllocation(t, ctx, st, "sess_recovery_instance_mismatch", mismatch, owner.UUID, "host-mismatch", now.Add(-2*time.Minute+time.Second))
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generations
SET sandbox_contract_id = NULL
WHERE generation_id = ?`, mismatch.GenerationID); err != nil {
		t.Fatalf("break generation contract mirror: %v", err)
	}

	candidates, err := st.ListExpiredRuntimeRecoveryCandidates(ctx, StartupRecoveryParams{
		OwnerUUID:       owner.UUID,
		Now:             now,
		ReconnectGrace:  time.Minute,
		AckStartedGrace: time.Minute,
	})
	if err != nil {
		t.Fatalf("list recovery candidates: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("candidates=%+v, want only generation with matching runtime resource instance", candidates)
	}
	if candidates[0].GenerationID != valid.GenerationID ||
		candidates[0].RuntimeID != validInstance.RunscContainerID ||
		candidates[0].RuntimeID == "legacy-"+valid.GenerationID {
		t.Fatalf("unexpected recovery candidate: %+v want runtime id %q", candidates[0], validInstance.RunscContainerID)
	}
}

func TestExpiredRuntimeRecoveryRequiresPositiveGraceWindows(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	now := time.Now().UTC()

	tests := []struct {
		name string
		p    StartupRecoveryParams
		want string
	}{
		{
			name: "list missing reconnect grace",
			p: StartupRecoveryParams{
				OwnerUUID:       owner.UUID,
				Now:             now,
				AckStartedGrace: time.Minute,
			},
			want: "reconnect grace must be > 0",
		},
		{
			name: "list missing ack-started grace",
			p: StartupRecoveryParams{
				OwnerUUID:      owner.UUID,
				Now:            now,
				ReconnectGrace: time.Minute,
			},
			want: "ack-started grace must be > 0",
		},
		{
			name: "list negative reconnect grace",
			p: StartupRecoveryParams{
				OwnerUUID:       owner.UUID,
				Now:             now,
				ReconnectGrace:  -time.Second,
				AckStartedGrace: time.Minute,
			},
			want: "reconnect grace must be > 0",
		},
		{
			name: "list negative ack-started grace",
			p: StartupRecoveryParams{
				OwnerUUID:       owner.UUID,
				Now:             now,
				ReconnectGrace:  time.Minute,
				AckStartedGrace: -time.Second,
			},
			want: "ack-started grace must be > 0",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := st.ListExpiredRuntimeRecoveryCandidates(ctx, tc.p)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("list err=%v, want %q", err, tc.want)
			}
		})
	}

	repairTests := []struct {
		name string
		p    StartupRecoveryParams
		want string
	}{
		{
			name: "repair missing reconnect grace",
			p: StartupRecoveryParams{
				OwnerUUID:       owner.UUID,
				Now:             now,
				AckStartedGrace: time.Minute,
			},
			want: "reconnect grace must be > 0",
		},
		{
			name: "repair missing ack-started grace",
			p: StartupRecoveryParams{
				OwnerUUID:      owner.UUID,
				Now:            now,
				ReconnectGrace: time.Minute,
			},
			want: "ack-started grace must be > 0",
		},
		{
			name: "repair negative reconnect grace",
			p: StartupRecoveryParams{
				OwnerUUID:       owner.UUID,
				Now:             now,
				ReconnectGrace:  -time.Second,
				AckStartedGrace: time.Minute,
			},
			want: "reconnect grace must be > 0",
		},
		{
			name: "repair negative ack-started grace",
			p: StartupRecoveryParams{
				OwnerUUID:       owner.UUID,
				Now:             now,
				ReconnectGrace:  time.Minute,
				AckStartedGrace: -time.Second,
			},
			want: "ack-started grace must be > 0",
		},
	}
	for _, tc := range repairTests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := st.RepairExpiredRuntimeRecovery(ctx, tc.p, nil)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("repair err=%v, want %q", err, tc.want)
			}
		})
	}
}
