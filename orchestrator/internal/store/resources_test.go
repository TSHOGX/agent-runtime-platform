package store

import (
	"context"
	"database/sql"
	"errors"
	"net/netip"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/sessionstate"
)

func TestAllocateGenerationCreatesRowsAndReservesNonDestroyedSlots(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_alloc")
	cfg := testAllocatorConfig(t)
	now := time.Now().UTC()

	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_alloc",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}

	var activeGeneration string
	if err := st.db.QueryRowContext(ctx, `SELECT active_generation_id FROM sessions WHERE id = 'sess_alloc'`).Scan(&activeGeneration); err != nil {
		t.Fatalf("query active generation: %v", err)
	}
	if activeGeneration != allocation.GenerationID {
		t.Fatalf("active generation = %q, want %q", activeGeneration, allocation.GenerationID)
	}
	var generationStatus, networkState, resourceState, hostCIDR string
	if err := st.db.QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state, n.host_side_cidr
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&generationStatus, &networkState, &resourceState, &hostCIDR); err != nil {
		t.Fatalf("query allocation rows: %v", err)
	}
	if generationStatus != "allocating" || networkState != "allocating" || resourceState != "allocating" || hostCIDR != "10.240.0.0/30" {
		t.Fatalf("unexpected allocation row state: generation=%s network=%s resource=%s cidr=%s", generationStatus, networkState, resourceState, hostCIDR)
	}
	details, err := st.GetRuntimeGenerationDetails(ctx, "sess_alloc", allocation.GenerationID)
	if err != nil {
		t.Fatalf("get runtime generation details: %v", err)
	}
	if details.AnthropicAPIKeySecretID != "anthropic_api_key" ||
		details.AnthropicAuthTokenSecretID != "anthropic_auth_token" ||
		details.SecretVersion != "local" ||
		!details.RequiresSecretDrop ||
		details.SecretsDirPath == "" {
		t.Fatalf("unexpected claude generation details: %+v", details)
	}
	if details.RunscNetwork != "sandbox" ||
		details.RunscOverlay2 != "none" ||
		details.HostProxyBindURL != cfg.HostProxyBindURL ||
		details.ProxyPort != 8082 ||
		details.HostGatewayIP != "10.240.0.1" ||
		details.SandboxBaseURL != "http://10.240.0.1:8082" ||
		details.ProbeURL != "http://10.240.0.1:8082" ||
		details.NetnsName == "" ||
		details.NetnsPath == "" ||
		details.HostVeth == "" ||
		details.SandboxVeth == "" ||
		details.SandboxIPCIDR != "10.240.0.2/30" ||
		details.HostSideCIDR != "10.240.0.0/30" ||
		details.EgressPolicyID == "" ||
		details.EgressPolicyDigest == "" ||
		details.AllowedEgressRules == "" ||
		details.DorisFEHosts == "" ||
		details.DorisBEHosts == "" ||
		details.DorisPorts == "" ||
		details.DNSPolicy != "hostnames_only" ||
		details.NetworkAllocationState != "allocating" {
		t.Fatalf("generation details missing network allocation fields: %+v", details)
	}

	if err := st.MarkGenerationResourcesLive(ctx, "sess_alloc", allocation.GenerationID, allocation.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark resources live: %v", err)
	}
	if err := st.RecordGenerationRuntimeArtifacts(ctx, allocation.GenerationID, "digest_a", "runsc test"); err != nil {
		t.Fatalf("record runtime artifacts: %v", err)
	}
	details, err = st.GetRuntimeGenerationDetails(ctx, "sess_alloc", allocation.GenerationID)
	if err != nil {
		t.Fatalf("get runtime generation details after artifact record: %v", err)
	}
	if details.ControlManifestDigest != "digest_a" || details.RunscVersion != "runsc test" {
		t.Fatalf("runtime artifact details not persisted: %+v", details)
	}
	if err := st.FailGeneration(ctx, FailGenerationParams{
		SessionID:    "sess_alloc",
		GenerationID: allocation.GenerationID,
		Owner:        allocation.Owner,
		ErrorClass:   "test_failure",
		Reason:       "test failure",
		Now:          now.Add(2 * time.Second),
	}); err != nil {
		t.Fatalf("fail generation: %v", err)
	}

	createStoreSession(t, ctx, st, "sess_next")
	next, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_next",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now.Add(3 * time.Second),
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate second generation: %v", err)
	}
	var firstNetns, nextCIDR, nextNetns string
	if err := st.db.QueryRowContext(ctx, `SELECT netns_name FROM network_profiles WHERE generation_id = ?`, allocation.GenerationID).Scan(&firstNetns); err != nil {
		t.Fatalf("query first netns: %v", err)
	}
	if err := st.db.QueryRowContext(ctx, `SELECT host_side_cidr, netns_name FROM network_profiles WHERE generation_id = ?`, next.GenerationID).Scan(&nextCIDR, &nextNetns); err != nil {
		t.Fatalf("query next network identity: %v", err)
	}
	if nextCIDR != "10.240.0.4/30" {
		t.Fatalf("expected reclaimable first slot to remain reserved, got next cidr %s", nextCIDR)
	}
	if nextNetns == firstNetns {
		t.Fatalf("expected reclaimable first netns to remain reserved, got %s", nextNetns)
	}
}

func TestAllocateGenerationSnapshotsSessionAutoCheckpointPolicy(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	now := time.Now().UTC()
	if err := st.CreateSession(ctx, Session{
		ID:                    "sess_policy",
		UserID:                "lab",
		Status:                string(sessionstate.Created),
		Agent:                 "claude",
		Workspace:             filepath.Join(t.TempDir(), "sess_policy"),
		RestoreID:             "phase3-sess_policy",
		AutoCheckpointEnabled: true,
		CreatedAt:             now,
		UpdatedAt:             now,
	}); err != nil {
		t.Fatalf("create policy session: %v", err)
	}
	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_policy",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    testAllocatorConfig(t),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `UPDATE sessions SET auto_checkpoint_enabled = 0 WHERE id = 'sess_policy'`); err != nil {
		t.Fatalf("disable session policy after allocation: %v", err)
	}
	details, err := st.GetRuntimeGenerationDetails(ctx, "sess_policy", allocation.GenerationID)
	if err != nil {
		t.Fatalf("get generation details: %v", err)
	}
	if !details.AutoCheckpointEnabled {
		t.Fatalf("generation policy should snapshot enabled session policy: %+v", details)
	}
}

func TestAllocateGenerationCanCASFromFailedActiveGeneration(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_fallback")
	cfg := testAllocatorConfig(t)
	cfg.CIDRPool = netip.MustParsePrefix("10.240.0.0/28")
	now := time.Now().UTC()
	leaseOwner := GenerationLeaseOwner(owner.UUID)

	first, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_fallback",
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate first generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, "sess_fallback", first.GenerationID, first.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark first generation live: %v", err)
	}
	if err := st.FailGeneration(ctx, FailGenerationParams{
		SessionID:    "sess_fallback",
		GenerationID: first.GenerationID,
		Owner:        first.Owner,
		ErrorClass:   "orchestrator_restart_reconnect_grace_expired",
		Reason:       "orchestrator_restart_reconnect_grace_expired",
		Now:          now.Add(2 * time.Second),
	}); err != nil {
		t.Fatalf("fail first generation: %v", err)
	}

	_, err = st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID:            "sess_fallback",
		ExpectedGenerationID: sql.NullString{String: "gen_stale", Valid: true},
		Owner:                leaseOwner,
		LeaseTTL:             time.Minute,
		Now:                  now.Add(3 * time.Second),
		Config:               cfg,
	})
	if err == nil || !strings.Contains(err.Error(), "session active generation CAS failed") {
		t.Fatalf("expected stale active-generation CAS failure, got %v", err)
	}
	var generations int
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_generations WHERE session_id = 'sess_fallback'`).Scan(&generations); err != nil {
		t.Fatalf("count generations after stale CAS: %v", err)
	}
	if generations != 1 {
		t.Fatalf("stale CAS should roll back inserted rows, generations=%d", generations)
	}

	next, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID:            "sess_fallback",
		ExpectedGenerationID: sql.NullString{String: first.GenerationID, Valid: true},
		Owner:                leaseOwner,
		LeaseTTL:             time.Minute,
		Now:                  now.Add(4 * time.Second),
		Config:               cfg,
	})
	if err != nil {
		t.Fatalf("allocate fallback generation: %v", err)
	}
	if next.GenerationID == first.GenerationID {
		t.Fatalf("fallback reused failed generation id %s", next.GenerationID)
	}

	var activeGeneration, firstStatus, firstNetworkState, nextStatus, nextCIDR string
	if err := st.db.QueryRowContext(ctx, `SELECT active_generation_id FROM sessions WHERE id = 'sess_fallback'`).Scan(&activeGeneration); err != nil {
		t.Fatalf("query active generation: %v", err)
	}
	if activeGeneration != next.GenerationID {
		t.Fatalf("active generation = %q, want %q", activeGeneration, next.GenerationID)
	}
	if err := st.db.QueryRowContext(ctx, `
SELECT g.status, n.allocation_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
WHERE g.generation_id = ?`, first.GenerationID).Scan(&firstStatus, &firstNetworkState); err != nil {
		t.Fatalf("query first generation: %v", err)
	}
	if firstStatus != "failed" || firstNetworkState != "reclaimable" {
		t.Fatalf("first generation not fenced/reclaimable: status=%s network=%s", firstStatus, firstNetworkState)
	}
	if err := st.db.QueryRowContext(ctx, `
SELECT g.status, n.host_side_cidr
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
WHERE g.generation_id = ?`, next.GenerationID).Scan(&nextStatus, &nextCIDR); err != nil {
		t.Fatalf("query fallback generation: %v", err)
	}
	if nextStatus != "allocating" || nextCIDR != "10.240.0.4/30" {
		t.Fatalf("unexpected fallback generation state: status=%s cidr=%s", nextStatus, nextCIDR)
	}
}

func TestClaimCheckpointedGenerationForRestoreMovesReservedResources(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_restore_claim")
	cfg := testAllocatorConfig(t)
	now := time.Now().UTC()
	leaseOwner := GenerationLeaseOwner(owner.UUID)

	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_restore_claim",
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, "sess_restore_claim", allocation.GenerationID, allocation.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark generation live: %v", err)
	}
	if err := st.RecordGenerationRuntimeArtifacts(ctx, allocation.GenerationID, "manifest_digest", "runsc test"); err != nil {
		t.Fatalf("record artifacts: %v", err)
	}
	checkpointedGeneration(t, ctx, st, "sess_restore_claim", allocation.GenerationID, now.Add(2*time.Second))

	claimed, err := st.ClaimCheckpointedGenerationForRestore(ctx, ClaimCheckpointedGenerationParams{
		SessionID:    "sess_restore_claim",
		GenerationID: allocation.GenerationID,
		Owner:        leaseOwner,
		LeaseTTL:     2 * time.Minute,
		Now:          now.Add(3 * time.Second),
	})
	if err != nil {
		t.Fatalf("claim checkpointed generation: %v", err)
	}
	if claimed.GenerationID != allocation.GenerationID ||
		claimed.NetworkProfileID != allocation.NetworkProfileID ||
		claimed.AgentRuntimeProfileID != allocation.AgentRuntimeProfileID ||
		claimed.Owner != leaseOwner ||
		!claimed.LeaseExpiresAt.Equal(now.Add(3*time.Second).Add(2*time.Minute)) {
		t.Fatalf("unexpected claimed allocation: %+v want base %+v", claimed, allocation)
	}

	var generationStatus, generationOwner, leaseExpires, lastSeen, networkState, resourceState string
	if err := st.db.QueryRowContext(ctx, `
SELECT g.status, COALESCE(g.lease_owner, ''), COALESCE(g.lease_expires_at, ''), COALESCE(g.last_seen_at, ''),
       n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&generationStatus, &generationOwner, &leaseExpires, &lastSeen, &networkState, &resourceState); err != nil {
		t.Fatalf("query restore claim state: %v", err)
	}
	if generationStatus != "restoring" ||
		generationOwner != leaseOwner ||
		!parseTime(leaseExpires).Equal(claimed.LeaseExpiresAt) ||
		!parseTime(lastSeen).Equal(now.Add(3*time.Second)) ||
		networkState != "recreating" ||
		resourceState != "recreating" {
		t.Fatalf("unexpected restore claim state: generation=%s owner=%s expires=%s last_seen=%s network=%s resource=%s",
			generationStatus, generationOwner, leaseExpires, lastSeen, networkState, resourceState)
	}
	details, err := st.GetRuntimeGenerationDetails(ctx, "sess_restore_claim", allocation.GenerationID)
	if err != nil {
		t.Fatalf("get restore details: %v", err)
	}
	if details.NetworkAllocationState != "recreating" ||
		details.ControlManifestDigest != "manifest_digest" ||
		details.RunscVersion != "runsc test" {
		t.Fatalf("restore details not preserved: %+v", details)
	}
	if details.CheckpointNetworkProfileID != allocation.NetworkProfileID ||
		details.CheckpointAgentRuntimeProfileID != allocation.AgentRuntimeProfileID ||
		details.CheckpointRunscVersion != "runsc test" ||
		details.CheckpointRunscPlatform != "systrap" ||
		details.CheckpointBundleDigest != "bundle_digest" ||
		details.CheckpointRuntimeConfigDigest != "runtime_config_digest" ||
		details.CheckpointControlManifestDigest != "manifest_digest" {
		t.Fatalf("checkpoint metadata not loaded into restore details: %+v", details)
	}

	if err := st.MarkGenerationResourcesLive(ctx, "sess_restore_claim", allocation.GenerationID, claimed.Owner, now.Add(4*time.Second)); err != nil {
		t.Fatalf("mark restored generation live: %v", err)
	}
	if err := st.db.QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&generationStatus, &networkState, &resourceState); err != nil {
		t.Fatalf("query restored live state: %v", err)
	}
	if generationStatus != "idle" || networkState != "live" || resourceState != "live" {
		t.Fatalf("restored generation not live idle: generation=%s network=%s resource=%s", generationStatus, networkState, resourceState)
	}
}

func TestClaimCheckpointedGenerationForRestoreRollsBackOnResourceMismatch(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_restore_mismatch")
	cfg := testAllocatorConfig(t)
	now := time.Now().UTC()
	leaseOwner := GenerationLeaseOwner(owner.UUID)

	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_restore_mismatch",
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, "sess_restore_mismatch", allocation.GenerationID, allocation.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark generation live: %v", err)
	}
	checkpointedGeneration(t, ctx, st, "sess_restore_mismatch", allocation.GenerationID, now.Add(2*time.Second))
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generation_resources
SET resource_state = 'live'
WHERE generation_id = ?`, allocation.GenerationID); err != nil {
		t.Fatalf("break resource state: %v", err)
	}

	_, err = st.ClaimCheckpointedGenerationForRestore(ctx, ClaimCheckpointedGenerationParams{
		SessionID:    "sess_restore_mismatch",
		GenerationID: allocation.GenerationID,
		Owner:        leaseOwner,
		LeaseTTL:     time.Minute,
		Now:          now.Add(3 * time.Second),
	})
	if err == nil || !strings.Contains(err.Error(), "checkpointed resource restore CAS failed") {
		t.Fatalf("expected resource CAS failure, got %v", err)
	}
	var generationStatus, leaseOwnerAfter, networkState, resourceState string
	if err := st.db.QueryRowContext(ctx, `
SELECT g.status, COALESCE(g.lease_owner, ''), n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&generationStatus, &leaseOwnerAfter, &networkState, &resourceState); err != nil {
		t.Fatalf("query rolled back state: %v", err)
	}
	if generationStatus != "checkpointed" || leaseOwnerAfter != "" || networkState != "reserved_checkpointed" || resourceState != "live" {
		t.Fatalf("restore claim did not roll back cleanly: generation=%s owner=%q network=%s resource=%s",
			generationStatus, leaseOwnerAfter, networkState, resourceState)
	}
}

func TestClaimCheckpointedGenerationForRestoreRequiresCheckpointedSession(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_restore_session_state")
	cfg := testAllocatorConfig(t)
	now := time.Now().UTC()
	leaseOwner := GenerationLeaseOwner(owner.UUID)

	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_restore_session_state",
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, "sess_restore_session_state", allocation.GenerationID, allocation.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark generation live: %v", err)
	}
	checkpointedGeneration(t, ctx, st, "sess_restore_session_state", allocation.GenerationID, now.Add(2*time.Second))
	if err := st.UpdateSessionStatus(ctx, "sess_restore_session_state", string(sessionstate.RunningIdle), nil); err != nil {
		t.Fatalf("set non-checkpointed session state: %v", err)
	}

	_, err = st.ClaimCheckpointedGenerationForRestore(ctx, ClaimCheckpointedGenerationParams{
		SessionID:    "sess_restore_session_state",
		GenerationID: allocation.GenerationID,
		Owner:        leaseOwner,
		LeaseTTL:     time.Minute,
		Now:          now.Add(3 * time.Second),
	})
	if err == nil || !strings.Contains(err.Error(), "checkpointed generation restore CAS failed") {
		t.Fatalf("expected generation CAS failure, got %v", err)
	}
	var generationStatus, leaseOwnerAfter, networkState, resourceState string
	if err := st.db.QueryRowContext(ctx, `
SELECT g.status, COALESCE(g.lease_owner, ''), n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&generationStatus, &leaseOwnerAfter, &networkState, &resourceState); err != nil {
		t.Fatalf("query rejected restore state: %v", err)
	}
	if generationStatus != "checkpointed" || leaseOwnerAfter != "" || networkState != "reserved_checkpointed" || resourceState != "reserved_checkpointed" {
		t.Fatalf("restore claim mutated rejected session state: generation=%s owner=%q network=%s resource=%s",
			generationStatus, leaseOwnerAfter, networkState, resourceState)
	}
}

func TestListColdFallbackSessionsReturnsFailedActiveWithQueuedTurns(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	cfg.CIDRPool = netip.MustParsePrefix("10.240.0.0/28")
	now := time.Now().UTC()
	leaseOwner := GenerationLeaseOwner(owner.UUID)

	createStoreSession(t, ctx, st, "sess_fallback_queue")
	failed, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_fallback_queue",
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate failed generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, "sess_fallback_queue", failed.GenerationID, failed.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark failed generation live: %v", err)
	}
	if err := st.FailGeneration(ctx, FailGenerationParams{
		SessionID:    "sess_fallback_queue",
		GenerationID: failed.GenerationID,
		Owner:        failed.Owner,
		ErrorClass:   "orchestrator_restart_reconnect_grace_expired",
		Reason:       "orchestrator_restart_reconnect_grace_expired",
		Now:          now.Add(2 * time.Second),
	}); err != nil {
		t.Fatalf("fail generation: %v", err)
	}
	if _, err := st.EnqueueTurn(ctx, "sess_fallback_queue", "retry me", now.Add(3*time.Second)); err != nil {
		t.Fatalf("enqueue fallback turn: %v", err)
	}

	createStoreSession(t, ctx, st, "sess_failed_no_queue")
	noQueue, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_failed_no_queue",
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate no-queue generation: %v", err)
	}
	if err := st.FailGeneration(ctx, FailGenerationParams{
		SessionID:    "sess_failed_no_queue",
		GenerationID: noQueue.GenerationID,
		Owner:        noQueue.Owner,
		ErrorClass:   "test_failure",
		Reason:       "test_failure",
		Now:          now.Add(time.Second),
	}); err != nil {
		t.Fatalf("fail no-queue generation: %v", err)
	}

	createStoreSession(t, ctx, st, "sess_active_queue")
	active, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_active_queue",
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate active generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, "sess_active_queue", active.GenerationID, active.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark active generation live: %v", err)
	}
	if _, err := st.EnqueueTurn(ctx, "sess_active_queue", "not fallback", now.Add(2*time.Second)); err != nil {
		t.Fatalf("enqueue active turn: %v", err)
	}

	fallbacks, err := st.ListColdFallbackSessions(ctx)
	if err != nil {
		t.Fatalf("list cold fallback sessions: %v", err)
	}
	if len(fallbacks) != 1 {
		t.Fatalf("fallback sessions=%d want 1: %+v", len(fallbacks), fallbacks)
	}
	if fallbacks[0].Session.ID != "sess_fallback_queue" ||
		fallbacks[0].OldGeneration != failed.GenerationID ||
		fallbacks[0].QueuedTurns != 1 {
		t.Fatalf("unexpected fallback session: %+v", fallbacks[0])
	}
}

func TestAllocateShellGenerationHasNoSecretReferences(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_shell")
	cfg := testAllocatorConfig(t)
	cfg.Agent = "sh"
	cfg.AgentModel = ""
	cfg.AgentOutputFormat = "shell_pty"

	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_shell",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate shell generation: %v", err)
	}
	details, err := st.GetRuntimeGenerationDetails(ctx, "sess_shell", allocation.GenerationID)
	if err != nil {
		t.Fatalf("get shell generation details: %v", err)
	}
	if details.Agent != "sh" ||
		details.RequiresSecretDrop ||
		details.SecretsDirPath != "" ||
		details.AnthropicAPIKeySecretID != "" ||
		details.AnthropicAuthTokenSecretID != "" ||
		details.SecretVersion != "" {
		t.Fatalf("shell generation should not carry secrets: %+v", details)
	}
}

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

func TestRecoverAllocationsAndReaperTransitions(t *testing.T) {
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

	recovered, err := st.RecoverAllocations(ctx, StartupRecoveryParams{
		OwnerUUID:      owner.UUID,
		Now:            now,
		LeaseTTL:       time.Minute,
		ReconnectGrace: 30 * time.Second,
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
	if destroyable, err = st.ListDestroyableReclaimableGenerations(ctx, now.Add(3*time.Second), 0); err != nil {
		t.Fatalf("list destroyable after mark: %v", err)
	} else if len(destroyable) != 0 {
		t.Fatalf("destroyed generation must not remain destroyable: %+v", destroyable)
	}
}

func TestRecoverAllocationsDoesNotReclaimUnrelatedFailedGeneration(t *testing.T) {
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

	recovered, err := st.RecoverAllocations(ctx, StartupRecoveryParams{
		OwnerUUID:      owner.UUID,
		Now:            now,
		LeaseTTL:       time.Minute,
		ReconnectGrace: 30 * time.Second,
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

func TestRecoverAllocationsRequeuesExpiredLeasedTurn(t *testing.T) {
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

	recovered, err := st.RecoverAllocations(ctx, StartupRecoveryParams{
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

			recovered, err := st.RecoverAllocations(ctx, StartupRecoveryParams{
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

func TestRecoverAllocationsLeavesAckStartedTurnDuringGrace(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	createStoreSession(t, ctx, st, "sess_ack_grace")
	now := time.Now().UTC()
	allocation, turnID := createExpiredAckStartedTurn(t, ctx, st, owner.UUID, cfg, "sess_ack_grace", now, 80*time.Second)

	recovered, err := st.RecoverAllocations(ctx, StartupRecoveryParams{
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

func TestRecoverAllocationsMarksExpiredAckStartedTurnUnknown(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	createStoreSession(t, ctx, st, "sess_ack_unknown")
	now := time.Now().UTC()
	allocation, turnID := createExpiredAckStartedTurn(t, ctx, st, owner.UUID, cfg, "sess_ack_unknown", now, 3*time.Minute)

	recovered, err := st.RecoverAllocations(ctx, StartupRecoveryParams{
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

func TestRecoverAllocationsDeletesStaleProxyContextsFromPreviousOwner(t *testing.T) {
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

	recovered, err := st.RecoverAllocations(ctx, StartupRecoveryParams{
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
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generations
SET lease_owner = ?
WHERE generation_id = ?`, GenerationLeaseOwner("other-owner"), otherAllocation.GenerationID); err != nil {
		t.Fatalf("move other generation to another owner: %v", err)
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
		generations[0].BridgeDirPath == "" {
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

func TestAutoCheckpointCandidatesRequirePolicyArtifactsAndNoActiveTurns(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	cfg.CIDRPool = netip.MustParsePrefix("10.240.0.0/27")
	now := time.Now().UTC()
	ownerLease := GenerationLeaseOwner(owner.UUID)

	eligible := createAutoCheckpointGeneration(t, ctx, st, cfg, "sess_auto_eligible", ownerLease, now)
	disabled := createAutoCheckpointGeneration(t, ctx, st, cfg, "sess_auto_disabled", ownerLease, now)
	if _, err := st.db.ExecContext(ctx, `UPDATE sessions SET auto_checkpoint_enabled = 0 WHERE id = ?`, "sess_auto_disabled"); err != nil {
		t.Fatalf("disable session policy: %v", err)
	}
	busy := createAutoCheckpointGeneration(t, ctx, st, cfg, "sess_auto_busy", ownerLease, now)
	if _, err := st.EnqueueTurn(ctx, "sess_auto_busy", "queued", now.Add(time.Second)); err != nil {
		t.Fatalf("enqueue busy turn: %v", err)
	}
	missingArtifacts := createAutoCheckpointGeneration(t, ctx, st, cfg, "sess_auto_missing_artifacts", ownerLease, now)
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generation_resources
SET bundle_digest = NULL
WHERE generation_id = ?`, missingArtifacts.GenerationID); err != nil {
		t.Fatalf("clear artifact digest: %v", err)
	}
	otherOwner := createAutoCheckpointGeneration(t, ctx, st, cfg, "sess_auto_other_owner", ownerLease, now)
	if _, err := st.db.ExecContext(ctx, `UPDATE runtime_generations SET lease_owner = ? WHERE generation_id = ?`, GenerationLeaseOwner("other"), otherOwner.GenerationID); err != nil {
		t.Fatalf("move owner: %v", err)
	}

	candidates, err := st.ListAutoCheckpointCandidates(ctx, ownerLease, now.Add(2*time.Minute), time.Minute)
	if err != nil {
		t.Fatalf("list candidates: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("candidates=%+v want one eligible generation", candidates)
	}
	if candidates[0].SessionID != "sess_auto_eligible" ||
		candidates[0].GenerationID != eligible.GenerationID ||
		candidates[0].BridgeDirPath == "" {
		t.Fatalf("unexpected candidate: %+v eligible=%+v disabled=%s busy=%s",
			candidates[0], eligible, disabled.GenerationID, busy.GenerationID)
	}
}

func TestGenerationCheckpointTransitionsAndMetadata(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	now := time.Now().UTC()
	allocation := createAutoCheckpointGeneration(t, ctx, st, cfg, "sess_auto_complete", GenerationLeaseOwner(owner.UUID), now)

	if err := st.BeginGenerationCheckpoint(ctx, "sess_auto_complete", allocation.GenerationID, allocation.Owner, now.Add(2*time.Minute)); err != nil {
		t.Fatalf("begin checkpoint: %v", err)
	}
	var generationStatus, sessionStatus string
	if err := st.db.QueryRowContext(ctx, `
SELECT g.status, s.status
FROM runtime_generations g
JOIN sessions s ON s.active_generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&generationStatus, &sessionStatus); err != nil {
		t.Fatalf("query checkpointing state: %v", err)
	}
	if generationStatus != "checkpointing" || sessionStatus != string(sessionstate.Checkpointing) {
		t.Fatalf("unexpected checkpointing state: generation=%s session=%s", generationStatus, sessionStatus)
	}
	if err := st.CompleteGenerationCheckpoint(ctx, CompleteCheckpointParams{
		SessionID:                       "sess_auto_complete",
		GenerationID:                    allocation.GenerationID,
		Owner:                           allocation.Owner,
		CheckpointPath:                  filepath.Join(cfg.RunDir, "checkpoint"),
		RunscPlatform:                   "systrap",
		RunscVersion:                    "runsc auto",
		CheckpointBundleDigest:          "bundle_digest",
		CheckpointRuntimeConfigDigest:   "runtime_config_digest",
		CheckpointControlManifestDigest: "projected_manifest_digest",
		Now:                             now.Add(3 * time.Minute),
	}); err != nil {
		t.Fatalf("complete checkpoint: %v", err)
	}
	var networkState, resourceState, checkpointPath, checkpointBundle, checkpointManifest string
	if err := st.db.QueryRowContext(ctx, `
SELECT g.status, s.status, n.allocation_state, r.resource_state, COALESCE(r.checkpoint_path, ''),
       COALESCE(g.checkpoint_bundle_digest, ''), COALESCE(g.checkpoint_control_manifest_digest, '')
FROM runtime_generations g
JOIN sessions s ON s.active_generation_id = g.generation_id
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(
		&generationStatus, &sessionStatus, &networkState, &resourceState, &checkpointPath, &checkpointBundle, &checkpointManifest,
	); err != nil {
		t.Fatalf("query checkpoint complete state: %v", err)
	}
	if generationStatus != "checkpointed" ||
		sessionStatus != string(sessionstate.Checkpointed) ||
		networkState != "reserved_checkpointed" ||
		resourceState != "reserved_checkpointed" ||
		checkpointPath == "" ||
		checkpointBundle != "bundle_digest" ||
		checkpointManifest != "projected_manifest_digest" {
		t.Fatalf("unexpected completed checkpoint state: generation=%s session=%s network=%s resource=%s path=%s bundle=%s manifest=%s",
			generationStatus, sessionStatus, networkState, resourceState, checkpointPath, checkpointBundle, checkpointManifest)
	}
}

func TestGenerationCheckpointAbortRestoresIdleState(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	now := time.Now().UTC()
	allocation := createAutoCheckpointGeneration(t, ctx, st, cfg, "sess_auto_abort", GenerationLeaseOwner(owner.UUID), now)

	if err := st.BeginGenerationCheckpoint(ctx, "sess_auto_abort", allocation.GenerationID, allocation.Owner, now.Add(2*time.Minute)); err != nil {
		t.Fatalf("begin checkpoint: %v", err)
	}
	if err := st.AbortGenerationCheckpoint(ctx, "sess_auto_abort", allocation.GenerationID, allocation.Owner, now.Add(3*time.Minute)); err != nil {
		t.Fatalf("abort checkpoint: %v", err)
	}
	var generationStatus, sessionStatus, networkState, resourceState string
	if err := st.db.QueryRowContext(ctx, `
SELECT g.status, s.status, n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN sessions s ON s.active_generation_id = g.generation_id
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&generationStatus, &sessionStatus, &networkState, &resourceState); err != nil {
		t.Fatalf("query aborted checkpoint state: %v", err)
	}
	if generationStatus != "idle" ||
		sessionStatus != string(sessionstate.RunningIdle) ||
		networkState != "live" ||
		resourceState != "live" {
		t.Fatalf("unexpected aborted checkpoint state: generation=%s session=%s network=%s resource=%s", generationStatus, sessionStatus, networkState, resourceState)
	}
}

func TestSweepExpiredSessionsDestroysAndRejectsInputState(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	now := time.Now().UTC()
	expiredAt := now.Add(-time.Second)
	if err := st.CreateSession(ctx, Session{
		ID:        "sess_expired",
		UserID:    "lab",
		Status:    string(sessionstate.Created),
		Agent:     "claude",
		Workspace: filepath.Join(t.TempDir(), "sess_expired"),
		RestoreID: "phase3-sess_expired",
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
		Agent:     "claude",
		Workspace: filepath.Join(t.TempDir(), "sess_expired_allocated"),
		RestoreID: "phase3-sess_expired_allocated",
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

func TestSweepExpiredSessionsCancelsUnstartedTurnsButPreservesAckStartedLease(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	now := time.Now().UTC()
	expiredAt := now.Add(-time.Second)

	if err := st.CreateSession(ctx, Session{
		ID:        "sess_expired_queued",
		UserID:    "lab",
		Status:    string(sessionstate.RunningIdle),
		Agent:     "claude",
		Workspace: filepath.Join(t.TempDir(), "sess_expired_queued"),
		RestoreID: "phase3-sess_expired_queued",
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
		Agent:     "claude",
		Workspace: filepath.Join(t.TempDir(), "sess_expired_ack"),
		RestoreID: "phase3-sess_expired_ack",
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

func checkpointedGeneration(t *testing.T, ctx context.Context, st *Store, sessionID, generationID string, now time.Time) {
	t.Helper()
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'checkpointed',
    checkpoint_created_at = ?,
    checkpoint_network_profile_id = network_profile_id,
    checkpoint_agent_runtime_profile_id = agent_runtime_profile_id,
    checkpoint_runsc_version = COALESCE(runsc_version, 'runsc test'),
    checkpoint_runsc_platform = COALESCE(runsc_platform, 'systrap'),
    checkpoint_bundle_digest = 'bundle_digest',
    checkpoint_runtime_config_digest = 'runtime_config_digest',
    checkpoint_control_manifest_digest = COALESCE((
      SELECT control_manifest_digest
      FROM runtime_generation_resources
      WHERE runtime_generation_resources.generation_id = runtime_generations.generation_id
    ), 'manifest_digest'),
    lease_owner = NULL,
    lease_expires_at = NULL,
    last_seen_at = ?
WHERE generation_id = ?
  AND session_id = ?`, formatTime(now), formatTime(now), generationID, sessionID); err != nil {
		t.Fatalf("set checkpointed generation: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `
UPDATE network_profiles
SET allocation_state = 'reserved_checkpointed'
WHERE generation_id = ?
  AND session_id = ?`, generationID, sessionID); err != nil {
		t.Fatalf("reserve checkpointed network: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generation_resources
SET resource_state = 'reserved_checkpointed'
WHERE generation_id = ?`, generationID); err != nil {
		t.Fatalf("reserve checkpointed resources: %v", err)
	}
	if err := st.UpdateSessionStatus(ctx, sessionID, string(sessionstate.Checkpointed), nil); err != nil {
		t.Fatalf("set checkpointed session: %v", err)
	}
}

func openOwnedStore(t *testing.T, ctx context.Context) (*Store, *OwnerLock) {
	t.Helper()
	dir := t.TempDir()
	st, err := Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	owner, err := AcquireOwnerLock(filepath.Join(dir, "run"))
	if err != nil {
		t.Fatalf("acquire owner: %v", err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	if err := st.WriteOwner(ctx, owner); err != nil {
		t.Fatalf("write owner: %v", err)
	}
	return st, owner
}

func createAutoCheckpointGeneration(t *testing.T, ctx context.Context, st *Store, cfg ResourceAllocatorConfig, sessionID, owner string, now time.Time) GenerationAllocation {
	t.Helper()
	if err := st.CreateSession(ctx, Session{
		ID:                    sessionID,
		UserID:                "lab",
		Status:                string(sessionstate.Created),
		Agent:                 "claude",
		Workspace:             filepath.Join(t.TempDir(), sessionID),
		RestoreID:             "phase3-" + sessionID,
		AutoCheckpointEnabled: true,
		CreatedAt:             now.Add(-2 * time.Minute),
		UpdatedAt:             now.Add(-2 * time.Minute),
	}); err != nil {
		t.Fatalf("create session %s: %v", sessionID, err)
	}
	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: sessionID,
		Owner:     owner,
		LeaseTTL:  time.Hour,
		Now:       now.Add(-2 * time.Minute),
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate generation for %s: %v", sessionID, err)
	}
	if err := st.RecordGenerationRuntimeArtifactDigests(ctx, allocation.GenerationID, GenerationRuntimeArtifactDigests{
		ControlManifestDigest:          "manifest_digest",
		ProjectedControlManifestDigest: "projected_manifest_digest",
		BundleDigest:                   "bundle_digest",
		RuntimeConfigDigest:            "runtime_config_digest",
		SpecDigest:                     "spec_digest",
		RunscVersion:                   "runsc auto",
	}); err != nil {
		t.Fatalf("record artifacts for %s: %v", sessionID, err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, sessionID, allocation.GenerationID, allocation.Owner, now.Add(-time.Minute)); err != nil {
		t.Fatalf("mark generation live for %s: %v", sessionID, err)
	}
	if err := st.UpdateSessionStatusAndActivity(ctx, sessionID, string(sessionstate.RunningIdle), nil, now.Add(-2*time.Minute)); err != nil {
		t.Fatalf("mark session idle for %s: %v", sessionID, err)
	}
	return allocation
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

func testAllocatorConfig(t *testing.T) ResourceAllocatorConfig {
	t.Helper()
	return ResourceAllocatorConfig{
		RunDir:                     filepath.Join(t.TempDir(), "run"),
		CIDRPool:                   netip.MustParsePrefix("10.240.0.0/29"),
		EgressDorisFEHosts:         []string{"172.16.0.138"},
		EgressDorisBEHosts:         []string{"172.16.0.139"},
		EgressDorisPorts:           []int{9030, 8040},
		EgressDNSPolicy:            "hostnames_only",
		HostProxyBindURL:           "http://0.0.0.0:8082",
		ProxyPort:                  8082,
		Agent:                      "claude",
		AgentModel:                 "sonnet",
		AgentOutputFormat:          "stream-json",
		DisableNonessentialTraffic: true,
	}
}
