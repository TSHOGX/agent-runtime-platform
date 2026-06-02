package store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/sessionstate"
)

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
	if err := st.RecordGenerationRuntimeArtifactDigests(ctx, allocation.GenerationID, GenerationRuntimeArtifactDigests{
		ControlManifestDigest:          "manifest_digest",
		ProjectedControlManifestDigest: "projected_manifest_digest",
		BundleDigest:                   "bundle_digest",
		RuntimeConfigDigest:            "runtime_config_digest",
		SpecDigest:                     "spec_digest",
		RunscVersion:                   "runsc test",
		RunscBinaryPath:                "/usr/local/bin/runsc-test",
		RunscBinaryDigest:              "sha256:runsc-test",
	}); err != nil {
		t.Fatalf("record artifacts: %v", err)
	}
	plan := storeCheckpointTestGenerationPlan(t, ctx, st, allocation.GenerationID)
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
		details.RunscVersion != "runsc test" ||
		details.RunscBinaryPath != "/usr/local/bin/runsc-test" ||
		details.RunscBinaryDigest != "sha256:runsc-test" {
		t.Fatalf("restore details not preserved: %+v", details)
	}
	if details.CheckpointNetworkProfileID != allocation.NetworkProfileID ||
		details.CheckpointAgentRuntimeProfileID != allocation.AgentRuntimeProfileID ||
		details.CheckpointRunscVersion != "runsc test" ||
		details.CheckpointRunscPlatform != "systrap" ||
		details.CheckpointRunscBinaryPath != "/usr/local/bin/runsc-test" ||
		details.CheckpointRunscBinaryDigest != "sha256:runsc-test" ||
		details.CheckpointBundleDigest != "bundle_digest" ||
		details.CheckpointRuntimeConfigDigest != "runtime_config_digest" ||
		details.CheckpointControlManifestDigest != "projected_manifest_digest" ||
		details.CheckpointPlanDigest != plan.PlanDigest ||
		details.CheckpointImageManifestDigest != checkpointImageManifestDigestForTest {
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

func TestClaimCheckpointedGenerationForRestoreRejectsPlanDigestMismatch(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_restore_plan_mismatch")
	cfg := testAllocatorConfig(t)
	now := time.Now().UTC()
	leaseOwner := GenerationLeaseOwner(owner.UUID)
	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_restore_plan_mismatch",
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.RecordGenerationRuntimeArtifactDigests(ctx, allocation.GenerationID, GenerationRuntimeArtifactDigests{
		ControlManifestDigest:          "manifest_digest",
		ProjectedControlManifestDigest: "projected_manifest_digest",
		BundleDigest:                   "bundle_digest",
		RuntimeConfigDigest:            "runtime_config_digest",
		SpecDigest:                     "spec_digest",
		RunscVersion:                   "runsc test",
		RunscBinaryPath:                "/usr/local/bin/runsc-test",
		RunscBinaryDigest:              "sha256:runsc-test",
	}); err != nil {
		t.Fatalf("record artifacts: %v", err)
	}
	storeCheckpointTestGenerationPlan(t, ctx, st, allocation.GenerationID)
	if err := st.MarkGenerationResourcesLive(ctx, "sess_restore_plan_mismatch", allocation.GenerationID, allocation.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark generation live: %v", err)
	}
	checkpointedGeneration(t, ctx, st, "sess_restore_plan_mismatch", allocation.GenerationID, now.Add(2*time.Second))
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generations
SET checkpoint_plan_digest = 'sha256:changed'
WHERE generation_id = ?`, allocation.GenerationID); err != nil {
		t.Fatalf("corrupt checkpoint plan digest: %v", err)
	}

	_, err = st.ClaimCheckpointedGenerationForRestore(ctx, ClaimCheckpointedGenerationParams{
		SessionID:    "sess_restore_plan_mismatch",
		GenerationID: allocation.GenerationID,
		Owner:        leaseOwner,
		LeaseTTL:     2 * time.Minute,
		Now:          now.Add(3 * time.Second),
	})
	if !errors.Is(err, ErrStaleCheckpointRestore) || !strings.Contains(err.Error(), "checkpoint plan digest mismatch") {
		t.Fatalf("expected checkpoint plan digest stale restore error, got %v", err)
	}
}

func TestClaimCheckpointedGenerationForRestoreRejectsProjectionDigestMismatch(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	now := time.Now().UTC()
	leaseOwner := GenerationLeaseOwner(owner.UUID)
	allocation := createAutoCheckpointGeneration(t, ctx, st, cfg, "sess_restore_projection_mismatch", leaseOwner, now)
	checkpointedGeneration(t, ctx, st, "sess_restore_projection_mismatch", allocation.GenerationID, now.Add(2*time.Second))
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generations
SET checkpoint_bundle_digest = 'changed_bundle_digest'
WHERE generation_id = ?`, allocation.GenerationID); err != nil {
		t.Fatalf("corrupt checkpoint bundle digest: %v", err)
	}

	_, err := st.ClaimCheckpointedGenerationForRestore(ctx, ClaimCheckpointedGenerationParams{
		SessionID:    "sess_restore_projection_mismatch",
		GenerationID: allocation.GenerationID,
		Owner:        leaseOwner,
		LeaseTTL:     2 * time.Minute,
		Now:          now.Add(3 * time.Second),
	})
	if !errors.Is(err, ErrStaleCheckpointRestore) || !strings.Contains(err.Error(), "checkpoint projection bundle digest mismatch") {
		t.Fatalf("expected checkpoint projection stale restore error, got %v", err)
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
		t.Fatalf("restore projection mismatch mutated state: generation=%s owner=%q network=%s resource=%s",
			generationStatus, leaseOwnerAfter, networkState, resourceState)
	}
}

func TestClaimCheckpointedGenerationForRestoreRejectsMissingImageManifestDigest(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	now := time.Now().UTC()
	leaseOwner := GenerationLeaseOwner(owner.UUID)
	allocation := createAutoCheckpointGeneration(t, ctx, st, cfg, "sess_restore_image_manifest_missing", leaseOwner, now)
	checkpointedGeneration(t, ctx, st, "sess_restore_image_manifest_missing", allocation.GenerationID, now.Add(2*time.Second))
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generations
SET checkpoint_image_manifest_digest = NULL
WHERE generation_id = ?`, allocation.GenerationID); err != nil {
		t.Fatalf("clear checkpoint image manifest digest: %v", err)
	}

	_, err := st.ClaimCheckpointedGenerationForRestore(ctx, ClaimCheckpointedGenerationParams{
		SessionID:    "sess_restore_image_manifest_missing",
		GenerationID: allocation.GenerationID,
		Owner:        leaseOwner,
		LeaseTTL:     2 * time.Minute,
		Now:          now.Add(3 * time.Second),
	})
	if !errors.Is(err, ErrStaleCheckpointRestore) || !strings.Contains(err.Error(), "checkpoint image manifest digest is missing") {
		t.Fatalf("expected checkpoint image manifest stale restore error, got %v", err)
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
		t.Fatalf("restore image manifest missing mutated state: generation=%s owner=%q network=%s resource=%s",
			generationStatus, leaseOwnerAfter, networkState, resourceState)
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
	storeCheckpointTestGenerationPlan(t, ctx, st, allocation.GenerationID)
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
	storeCheckpointTestGenerationPlan(t, ctx, st, allocation.GenerationID)
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
	if !errors.Is(err, ErrStaleCheckpointRestore) {
		t.Fatalf("expected stale checkpoint restore error, got %v", err)
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
