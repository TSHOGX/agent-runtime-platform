package store

import (
	"context"
	"database/sql"
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

func TestExpiredRuntimeRecoveryRequeuesExpiredLeasedTurn(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	createStoreSession(t, ctx, st, "sess_requeue")
	now := time.Now().UTC()
	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_requeue",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now.Add(-3 * time.Minute),
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, "sess_requeue", allocation.GenerationID, allocation.Owner, now.Add(-3*time.Minute+time.Second)); err != nil {
		t.Fatalf("mark resources live: %v", err)
	}
	createLiveRuntimeResourceInstanceForAllocation(t, ctx, st, "sess_requeue", allocation, owner.UUID, "host-requeue", now.Add(-3*time.Minute+2*time.Second))
	turnID, err := st.EnqueueTurn(ctx, "sess_requeue", "retry me", now.Add(-3*time.Minute+2*time.Second))
	if err != nil {
		t.Fatalf("enqueue turn: %v", err)
	}
	if grant, ok, err := st.ClaimNextTurn(ctx, ClaimNextTurnParams{
		SessionID:    "sess_requeue",
		GenerationID: allocation.GenerationID,
		Owner:        allocation.Owner,
		RequestID:    "req_requeue",
		LeaseTTL:     30 * time.Second,
		Now:          now.Add(-3*time.Minute + 3*time.Second),
	}); err != nil || !ok || grant.TurnID != turnID {
		t.Fatalf("claim setup: ok=%v grant=%+v err=%v", ok, grant, err)
	}

	recovered, err := recoverCleanedAllocations(t, ctx, st, StartupRecoveryParams{
		OwnerUUID:       owner.UUID,
		Now:             now,
		LeaseTTL:        time.Minute,
		ReconnectGrace:  time.Minute,
		AckStartedGrace: 2 * time.Minute,
	})
	if err != nil {
		t.Fatalf("recover allocations: %v", err)
	}
	if recovered.ReconnectGraceFailed != 1 || recovered.ExpiredLeasedRequeued != 1 || recovered.UnknownAfterAckStarted != 0 {
		t.Fatalf("unexpected recovery result: %+v", recovered)
	}

	var status string
	var generationID, leaseOwner, leaseExpires, claimRequest sql.NullString
	var attempt int
	if err := st.db.QueryRowContext(ctx, `
SELECT status, generation_id, lease_owner, lease_expires_at, claim_request_id, attempt
FROM turns
WHERE id = ?`, turnID).Scan(&status, &generationID, &leaseOwner, &leaseExpires, &claimRequest, &attempt); err != nil {
		t.Fatalf("query turn: %v", err)
	}
	if status != "queued" || generationID.Valid || leaseOwner.Valid || leaseExpires.Valid || claimRequest.Valid || attempt != 1 {
		t.Fatalf("leased turn was not reset for retry: status=%s gen=%v owner=%v expires=%v claim=%v attempt=%d", status, generationID, leaseOwner, leaseExpires, claimRequest, attempt)
	}
	var sessionStatus string
	if err := st.db.QueryRowContext(ctx, `
SELECT status
FROM sessions
WHERE id = 'sess_requeue'`).Scan(&sessionStatus); err != nil {
		t.Fatalf("query session status: %v", err)
	}
	if sessionStatus != "running_idle" {
		t.Fatalf("session status after requeue recovery=%s want running_idle", sessionStatus)
	}
	var generationStatus, networkState, resourceState string
	if err := st.db.QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&generationStatus, &networkState, &resourceState); err != nil {
		t.Fatalf("query generation: %v", err)
	}
	if generationStatus != "failed" || networkState != "reclaimable" || resourceState != "reclaimable" {
		t.Fatalf("unexpected generation state after requeue recovery: generation=%s network=%s resource=%s", generationStatus, networkState, resourceState)
	}
}

func TestClaimNextTurnPreservesSequenceOrderingAfterRecoveryRequeue(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	cfg.CIDRPool = netip.MustParsePrefix("10.240.0.0/27")
	now := time.Now().UTC()
	ownerLease := GenerationLeaseOwner(owner.UUID)

	for _, tc := range []struct {
		name              string
		requeuedSequence  int64
		freshSequence     int64
		wantContent       string
		wantAttempt       int
		wantRequeuedClaim bool
	}{
		{
			name:              "requeued lower sequence wins",
			requeuedSequence:  10,
			freshSequence:     20,
			wantContent:       "retry me",
			wantAttempt:       1,
			wantRequeuedClaim: true,
		},
		{
			name:              "fresh lower sequence wins",
			requeuedSequence:  20,
			freshSequence:     10,
			wantContent:       "fresh work",
			wantAttempt:       0,
			wantRequeuedClaim: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sessionID := "sess_order_" + strings.NewReplacer(" ", "_").Replace(tc.name)
			createStoreSession(t, ctx, st, sessionID)
			oldAllocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
				SessionID: sessionID,
				Owner:     ownerLease,
				LeaseTTL:  time.Minute,
				Now:       now.Add(-3 * time.Minute),
				Config:    cfg,
			})
			if err != nil {
				t.Fatalf("allocate old generation: %v", err)
			}
			if err := st.MarkGenerationResourcesLive(ctx, sessionID, oldAllocation.GenerationID, oldAllocation.Owner, now.Add(-3*time.Minute+time.Second)); err != nil {
				t.Fatalf("mark old generation live: %v", err)
			}
			createLiveRuntimeResourceInstanceForAllocation(t, ctx, st, sessionID, oldAllocation, owner.UUID, "host-old-"+sessionID, now.Add(-3*time.Minute+2*time.Second))
			requeuedTurnID, err := st.EnqueueTurn(ctx, sessionID, "retry me", now.Add(-3*time.Minute+2*time.Second))
			if err != nil {
				t.Fatalf("enqueue requeued turn: %v", err)
			}
			if grant, ok, err := st.ClaimNextTurn(ctx, ClaimNextTurnParams{
				SessionID:    sessionID,
				GenerationID: oldAllocation.GenerationID,
				Owner:        oldAllocation.Owner,
				RequestID:    "req_old_" + sessionID,
				LeaseTTL:     30 * time.Second,
				Now:          now.Add(-3*time.Minute + 3*time.Second),
			}); err != nil || !ok || grant.TurnID != requeuedTurnID {
				t.Fatalf("claim old turn setup: ok=%v grant=%+v err=%v", ok, grant, err)
			}

			recovered, err := recoverCleanedAllocations(t, ctx, st, StartupRecoveryParams{
				OwnerUUID:       owner.UUID,
				Now:             now,
				LeaseTTL:        time.Minute,
				ReconnectGrace:  time.Minute,
				AckStartedGrace: 2 * time.Minute,
			})
			if err != nil {
				t.Fatalf("recover allocations: %v", err)
			}
			if recovered.ExpiredLeasedRequeued != 1 {
				t.Fatalf("unexpected recovery result: %+v", recovered)
			}

			freshTurnID, err := st.EnqueueTurn(ctx, sessionID, "fresh work", now.Add(time.Second))
			if err != nil {
				t.Fatalf("enqueue fresh turn: %v", err)
			}
			if _, err := st.db.ExecContext(ctx, `UPDATE turns SET sequence = ? WHERE id = ?`, tc.requeuedSequence, requeuedTurnID); err != nil {
				t.Fatalf("set requeued sequence: %v", err)
			}
			if _, err := st.db.ExecContext(ctx, `UPDATE turns SET sequence = ? WHERE id = ?`, tc.freshSequence, freshTurnID); err != nil {
				t.Fatalf("set fresh sequence: %v", err)
			}

			newAllocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
				SessionID: sessionID,
				ExpectedGenerationID: sql.NullString{
					String: oldAllocation.GenerationID,
					Valid:  true,
				},
				Owner:    ownerLease,
				LeaseTTL: time.Minute,
				Now:      now.Add(2 * time.Second),
				Config:   cfg,
			})
			if err != nil {
				t.Fatalf("allocate new generation: %v", err)
			}
			if err := st.MarkGenerationResourcesLive(ctx, sessionID, newAllocation.GenerationID, newAllocation.Owner, now.Add(3*time.Second)); err != nil {
				t.Fatalf("mark new generation live: %v", err)
			}
			createLiveRuntimeResourceInstanceForAllocation(t, ctx, st, sessionID, newAllocation, owner.UUID, "host-new-"+sessionID, now.Add(3*time.Second+time.Millisecond))

			grant, ok, err := st.ClaimNextTurn(ctx, ClaimNextTurnParams{
				SessionID:    sessionID,
				GenerationID: newAllocation.GenerationID,
				Owner:        newAllocation.Owner,
				RequestID:    "req_new_" + sessionID,
				LeaseTTL:     time.Minute,
				Now:          now.Add(4 * time.Second),
			})
			if err != nil {
				t.Fatalf("claim next turn: %v", err)
			}
			if !ok {
				t.Fatal("expected claim grant")
			}
			if grant.Content != tc.wantContent || grant.Attempt != tc.wantAttempt {
				t.Fatalf("unexpected grant: %+v want content=%q attempt=%d", grant, tc.wantContent, tc.wantAttempt)
			}
			if gotRequeued := grant.TurnID == requeuedTurnID; gotRequeued != tc.wantRequeuedClaim {
				t.Fatalf("claimed requeued=%v want %v grant=%+v requeued=%d fresh=%d", gotRequeued, tc.wantRequeuedClaim, grant, requeuedTurnID, freshTurnID)
			}
		})
	}
}

func TestExpiredRuntimeRecoveryLeavesAckStartedTurnDuringGrace(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	createStoreSession(t, ctx, st, "sess_ack_grace")
	now := time.Now().UTC()
	allocation, turnID := createExpiredAckStartedTurn(t, ctx, st, owner.UUID, cfg, "sess_ack_grace", now, 80*time.Second)

	recovered, err := recoverCleanedAllocations(t, ctx, st, StartupRecoveryParams{
		OwnerUUID:       owner.UUID,
		Now:             now,
		LeaseTTL:        time.Minute,
		ReconnectGrace:  10 * time.Second,
		AckStartedGrace: 90 * time.Second,
	})
	if err != nil {
		t.Fatalf("recover allocations: %v", err)
	}
	if recovered.ReconnectGraceFailed != 0 || recovered.UnknownAfterAckStarted != 0 || recovered.ExpiredLeasedRequeued != 0 {
		t.Fatalf("ack-started turn inside grace should not be fenced: %+v", recovered)
	}
	var turnStatus, generationStatus string
	if err := st.db.QueryRowContext(ctx, `SELECT status FROM turns WHERE id = ?`, turnID).Scan(&turnStatus); err != nil {
		t.Fatalf("query turn status: %v", err)
	}
	if err := st.db.QueryRowContext(ctx, `SELECT status FROM runtime_generations WHERE generation_id = ?`, allocation.GenerationID).Scan(&generationStatus); err != nil {
		t.Fatalf("query generation status: %v", err)
	}
	if turnStatus != "running" || generationStatus != "active" {
		t.Fatalf("ack-started turn should remain recoverable inside grace: turn=%s generation=%s", turnStatus, generationStatus)
	}
}

func TestExpiredRuntimeRecoveryMarksExpiredAckStartedTurnUnknown(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	createStoreSession(t, ctx, st, "sess_ack_unknown")
	now := time.Now().UTC()
	allocation, turnID := createExpiredAckStartedTurn(t, ctx, st, owner.UUID, cfg, "sess_ack_unknown", now, 3*time.Minute)

	recovered, err := recoverCleanedAllocations(t, ctx, st, StartupRecoveryParams{
		OwnerUUID:       owner.UUID,
		Now:             now,
		LeaseTTL:        time.Minute,
		ReconnectGrace:  time.Minute,
		AckStartedGrace: 90 * time.Second,
	})
	if err != nil {
		t.Fatalf("recover allocations: %v", err)
	}
	if recovered.UnknownAfterAckStarted != 1 || recovered.ReconnectGraceFailed != 0 || recovered.ExpiredLeasedRequeued != 0 {
		t.Fatalf("unexpected recovery result: %+v", recovered)
	}
	var turnStatus, turnError, generationStatus, generationError, networkState, resourceState string
	if err := st.db.QueryRowContext(ctx, `
SELECT t.status, COALESCE(t.error_class, ''), g.status, COALESCE(g.error_class, ''), n.allocation_state, r.resource_state
FROM turns t
JOIN runtime_generations g ON g.generation_id = t.generation_id
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE t.id = ?`, turnID).Scan(&turnStatus, &turnError, &generationStatus, &generationError, &networkState, &resourceState); err != nil {
		t.Fatalf("query recovered state: %v", err)
	}
	if turnStatus != "failed" ||
		turnError != "unknown_after_ack_started" ||
		generationStatus != "failed" ||
		generationError != "unknown_after_ack_started" ||
		networkState != "reclaimable" ||
		resourceState != "reclaimable" {
		t.Fatalf("unexpected unknown-after-ack state: turn=%s/%s generation=%s/%s network=%s resource=%s", turnStatus, turnError, generationStatus, generationError, networkState, resourceState)
	}
	var contexts int
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM active_model_request_contexts WHERE generation_id = ?`, allocation.GenerationID).Scan(&contexts); err != nil {
		t.Fatalf("count active contexts: %v", err)
	}
	if contexts != 0 {
		t.Fatalf("active model contexts should be cleared, got %d", contexts)
	}
}

func TestExpiredRuntimeRecoveryDeletesStaleProxyContextsFromPreviousOwner(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	now := time.Now().UTC()

	createStoreSession(t, ctx, st, "sess_proxy_context_current")
	current, currentTurnID := createExpiredAckStartedTurn(t, ctx, st, owner.UUID, cfg, "sess_proxy_context_current", now, 30*time.Second)
	createStoreSession(t, ctx, st, "sess_proxy_context_stale")
	stale, staleTurnID := createExpiredAckStartedTurn(t, ctx, st, owner.UUID, cfg, "sess_proxy_context_stale", now, 30*time.Second)
	staleOwner := GenerationLeaseOwner("previous-owner")
	if _, err := st.db.ExecContext(ctx, `
UPDATE active_model_request_contexts
SET lease_owner = ?
WHERE generation_id = ?`, staleOwner, stale.GenerationID); err != nil {
		t.Fatalf("move stale proxy context to previous owner: %v", err)
	}

	recovered, err := recoverCleanedAllocations(t, ctx, st, StartupRecoveryParams{
		OwnerUUID:       owner.UUID,
		Now:             now,
		LeaseTTL:        time.Minute,
		ReconnectGrace:  10 * time.Second,
		AckStartedGrace: 90 * time.Second,
	})
	if err != nil {
		t.Fatalf("recover allocations: %v", err)
	}
	if recovered.UnknownAfterAckStarted != 0 || recovered.ReconnectGraceFailed != 0 {
		t.Fatalf("proxy context cleanup should not fence recoverable turns: %+v", recovered)
	}

	var currentContexts, staleContexts int
	if err := st.db.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM active_model_request_contexts
WHERE generation_id = ?
  AND turn_id = ?
  AND lease_owner = ?`, current.GenerationID, currentTurnID, current.Owner).Scan(&currentContexts); err != nil {
		t.Fatalf("count current proxy contexts: %v", err)
	}
	if err := st.db.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM active_model_request_contexts
WHERE generation_id = ?
  AND turn_id = ?`, stale.GenerationID, staleTurnID).Scan(&staleContexts); err != nil {
		t.Fatalf("count stale proxy contexts: %v", err)
	}
	if currentContexts != 1 || staleContexts != 0 {
		t.Fatalf("unexpected proxy context cleanup: current=%d stale=%d", currentContexts, staleContexts)
	}
}

func createExpiredAckStartedTurn(t *testing.T, ctx context.Context, st *Store, ownerUUID string, cfg ResourceAllocatorConfig, sessionID string, now time.Time, expiredFor time.Duration) (GenerationAllocation, int64) {
	t.Helper()
	owner := GenerationLeaseOwner(ownerUUID)
	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: sessionID,
		Owner:     owner,
		LeaseTTL:  time.Minute,
		Now:       now.Add(-expiredFor - time.Minute),
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, sessionID, allocation.GenerationID, allocation.Owner, now.Add(-expiredFor-time.Minute+time.Second)); err != nil {
		t.Fatalf("mark resources live: %v", err)
	}
	createLiveRuntimeResourceInstanceForAllocation(t, ctx, st, sessionID, allocation, ownerUUID, "host-expired-"+sessionID, now.Add(-expiredFor-time.Minute+2*time.Second))
	turnID, err := st.EnqueueTurn(ctx, sessionID, "maybe already ran", now.Add(-expiredFor-time.Minute+2*time.Second))
	if err != nil {
		t.Fatalf("enqueue turn: %v", err)
	}
	claimAt := now.Add(-expiredFor - time.Minute + 3*time.Second)
	if grant, ok, err := st.ClaimNextTurn(ctx, ClaimNextTurnParams{
		SessionID:    sessionID,
		GenerationID: allocation.GenerationID,
		Owner:        allocation.Owner,
		RequestID:    "req_" + sessionID,
		LeaseTTL:     30 * time.Second,
		Now:          claimAt,
	}); err != nil || !ok || grant.TurnID != turnID {
		t.Fatalf("claim setup: ok=%v grant=%+v err=%v", ok, grant, err)
	}
	sandboxSourceIP := sandboxSourceIPForGeneration(t, ctx, st, allocation.GenerationID)
	if _, err := st.AckTurnStarted(ctx, AckStartedParams{
		SessionID:       sessionID,
		GenerationID:    allocation.GenerationID,
		TurnID:          turnID,
		Owner:           allocation.Owner,
		SandboxSourceIP: sandboxSourceIP,
		LeaseTTL:        30 * time.Second,
		Now:             claimAt.Add(time.Second),
	}); err != nil {
		t.Fatalf("ack started setup: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generations
SET lease_expires_at = ?
WHERE generation_id = ?`, formatTime(now.Add(-expiredFor)), allocation.GenerationID); err != nil {
		t.Fatalf("expire generation lease: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `
UPDATE turns
SET lease_expires_at = ?
WHERE id = ?`, formatTime(now.Add(-expiredFor)), turnID); err != nil {
		t.Fatalf("expire turn lease: %v", err)
	}
	return allocation, turnID
}
