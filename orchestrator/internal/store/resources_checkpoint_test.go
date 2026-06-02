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

func TestAutoCheckpointCandidatesRequirePolicyArtifactsAndNoActiveTurns(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	cfg.CIDRPool = netip.MustParsePrefix("10.240.0.0/27")
	now := time.Now().UTC()
	ownerLease := GenerationLeaseOwner(owner.UUID)

	eligible := createAutoCheckpointGeneration(t, ctx, st, cfg, "sess_auto_eligible", ownerLease, now)
	eligibleResource, err := st.GetRuntimeResourceInstance(ctx, eligible.GenerationID)
	if err != nil {
		t.Fatalf("get eligible runtime resource: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generation_resources
SET resource_state = 'ready',
    bridge_dir_path = ?
WHERE generation_id = ?`, filepath.Join(t.TempDir(), "stale-auto-bridge"), eligible.GenerationID); err != nil {
		t.Fatalf("make auto checkpoint resource state stale: %v", err)
	}
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
	readyOnly := createAutoCheckpointGeneration(t, ctx, st, cfg, "sess_auto_ready_resource", ownerLease, now)
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_resource_instances
SET state = 'ready'
WHERE generation_id = ?`, readyOnly.GenerationID); err != nil {
		t.Fatalf("move runtime resource out of live: %v", err)
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
		candidates[0].BridgeDirPath != eligibleResource.BridgeDirPath {
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
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generation_resources
SET resource_state = 'ready'
WHERE generation_id = ?`, allocation.GenerationID); err != nil {
		t.Fatalf("make checkpoint resource state stale: %v", err)
	}

	if err := st.BeginGenerationCheckpoint(ctx, "sess_auto_complete", allocation.GenerationID, allocation.Owner, now.Add(2*time.Minute)); err != nil {
		t.Fatalf("begin checkpoint: %v", err)
	}
	plan, err := st.GetGenerationPlan(ctx, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get checkpoint generation plan: %v", err)
	}
	var generationStatus, sessionStatus, checkpointPlan string
	if err := st.db.QueryRowContext(ctx, `
SELECT g.status, s.status, COALESCE(g.checkpoint_plan_digest, '')
FROM runtime_generations g
JOIN sessions s ON s.active_generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&generationStatus, &sessionStatus, &checkpointPlan); err != nil {
		t.Fatalf("query checkpointing state: %v", err)
	}
	if generationStatus != "checkpointing" || sessionStatus != string(sessionstate.Checkpointing) || checkpointPlan != plan.PlanDigest {
		t.Fatalf("unexpected checkpointing state: generation=%s session=%s plan=%s want_plan=%s", generationStatus, sessionStatus, checkpointPlan, plan.PlanDigest)
	}
	if err := st.CompleteGenerationCheckpoint(ctx, CompleteCheckpointParams{
		SessionID:                       "sess_auto_complete",
		GenerationID:                    allocation.GenerationID,
		Owner:                           allocation.Owner,
		CheckpointPath:                  filepath.Join(cfg.RunDir, "checkpoint"),
		RunscPlatform:                   "systrap",
		RunscVersion:                    "runsc auto",
		RunscBinaryPath:                 "/usr/local/bin/runsc-auto",
		RunscBinaryDigest:               "sha256:runsc-auto",
		CheckpointBundleDigest:          "bundle_digest",
		CheckpointRuntimeConfigDigest:   "runtime_config_digest",
		CheckpointControlManifestDigest: "projected_manifest_digest",
		CheckpointPlanDigest:            plan.PlanDigest,
		CheckpointImageManifestDigest:   checkpointImageManifestDigestForTest,
		Now:                             now.Add(3 * time.Minute),
	}); err != nil {
		t.Fatalf("complete checkpoint: %v", err)
	}
	var networkState, resourceState, checkpointPath, checkpointBundle, checkpointManifest, checkpointImageManifest string
	if err := st.db.QueryRowContext(ctx, `
SELECT g.status, s.status, n.allocation_state, r.resource_state, COALESCE(r.checkpoint_path, ''),
       COALESCE(g.checkpoint_bundle_digest, ''), COALESCE(g.checkpoint_control_manifest_digest, ''),
       COALESCE(g.checkpoint_plan_digest, ''), COALESCE(g.checkpoint_image_manifest_digest, '')
FROM runtime_generations g
JOIN sessions s ON s.active_generation_id = g.generation_id
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(
		&generationStatus, &sessionStatus, &networkState, &resourceState, &checkpointPath, &checkpointBundle, &checkpointManifest, &checkpointPlan, &checkpointImageManifest,
	); err != nil {
		t.Fatalf("query checkpoint complete state: %v", err)
	}
	if generationStatus != "checkpointed" ||
		sessionStatus != string(sessionstate.Checkpointed) ||
		networkState != "reserved_checkpointed" ||
		resourceState != "reserved_checkpointed" ||
		checkpointPath == "" ||
		checkpointBundle != "bundle_digest" ||
		checkpointManifest != "projected_manifest_digest" ||
		checkpointPlan != plan.PlanDigest ||
		checkpointImageManifest != checkpointImageManifestDigestForTest {
		t.Fatalf("unexpected completed checkpoint state: generation=%s session=%s network=%s resource=%s path=%s bundle=%s manifest=%s plan=%s image_manifest=%s want_plan=%s",
			generationStatus, sessionStatus, networkState, resourceState, checkpointPath, checkpointBundle, checkpointManifest, checkpointPlan, checkpointImageManifest, plan.PlanDigest)
	}
}

func TestCompleteGenerationCheckpointChecksProjectionDigestFence(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	now := time.Now().UTC()
	allocation := createAutoCheckpointGeneration(t, ctx, st, cfg, "sess_auto_projection_fence", GenerationLeaseOwner(owner.UUID), now)

	if err := st.BeginGenerationCheckpoint(ctx, "sess_auto_projection_fence", allocation.GenerationID, allocation.Owner, now.Add(2*time.Minute)); err != nil {
		t.Fatalf("begin checkpoint: %v", err)
	}
	plan, err := st.GetGenerationPlan(ctx, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get checkpoint generation plan: %v", err)
	}
	err = st.CompleteGenerationCheckpoint(ctx, CompleteCheckpointParams{
		SessionID:                       "sess_auto_projection_fence",
		GenerationID:                    allocation.GenerationID,
		Owner:                           allocation.Owner,
		CheckpointPath:                  filepath.Join(cfg.RunDir, "checkpoint"),
		RunscPlatform:                   "systrap",
		RunscVersion:                    "runsc auto",
		RunscBinaryPath:                 "/usr/local/bin/runsc-auto",
		RunscBinaryDigest:               "sha256:runsc-auto",
		CheckpointBundleDigest:          "changed_bundle_digest",
		CheckpointRuntimeConfigDigest:   "runtime_config_digest",
		CheckpointControlManifestDigest: "projected_manifest_digest",
		CheckpointPlanDigest:            plan.PlanDigest,
		CheckpointImageManifestDigest:   checkpointImageManifestDigestForTest,
		Now:                             now.Add(3 * time.Minute),
	})
	if err == nil || !strings.Contains(err.Error(), "checkpoint projection bundle digest mismatch") {
		t.Fatalf("expected checkpoint projection digest mismatch, got %v", err)
	}

	var generationStatus, sessionStatus string
	if err := st.db.QueryRowContext(ctx, `
SELECT g.status, s.status
FROM runtime_generations g
JOIN sessions s ON s.active_generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&generationStatus, &sessionStatus); err != nil {
		t.Fatalf("query checkpoint state after projection mismatch: %v", err)
	}
	if generationStatus != "checkpointing" || sessionStatus != string(sessionstate.Checkpointing) {
		t.Fatalf("checkpoint projection mismatch should not complete: generation=%s session=%s", generationStatus, sessionStatus)
	}
}

func TestCompleteGenerationCheckpointRequiresRunscMetadata(t *testing.T) {
	ctx := context.Background()
	st, _ := openOwnedStore(t, ctx)
	params := CompleteCheckpointParams{
		SessionID:                       "sess_checkpoint_metadata",
		GenerationID:                    "gen_checkpoint_metadata",
		Owner:                           "owner",
		CheckpointPath:                  "/tmp/checkpoint",
		RunscPlatform:                   "systrap",
		RunscVersion:                    "runsc test",
		RunscBinaryPath:                 "/usr/local/bin/runsc-test",
		RunscBinaryDigest:               "sha256:runsc-test",
		CheckpointBundleDigest:          "bundle_digest",
		CheckpointRuntimeConfigDigest:   "runtime_config_digest",
		CheckpointControlManifestDigest: "manifest_digest",
		CheckpointPlanDigest:            "sha256:plan",
		CheckpointImageManifestDigest:   checkpointImageManifestDigestForTest,
		Now:                             time.Now().UTC(),
	}
	tests := []struct {
		name string
		want string
		edit func(*CompleteCheckpointParams)
	}{
		{name: "checkpoint path", want: "checkpoint path is required", edit: func(p *CompleteCheckpointParams) { p.CheckpointPath = "" }},
		{name: "relative checkpoint path", want: "checkpoint path must be canonical absolute", edit: func(p *CompleteCheckpointParams) { p.CheckpointPath = "checkpoint" }},
		{name: "runsc version", want: "checkpoint runsc version is required", edit: func(p *CompleteCheckpointParams) { p.RunscVersion = "" }},
		{name: "runsc platform", want: "checkpoint runsc platform is required", edit: func(p *CompleteCheckpointParams) { p.RunscPlatform = "" }},
		{name: "relative runsc path", want: "checkpoint runsc binary path must be canonical absolute", edit: func(p *CompleteCheckpointParams) { p.RunscBinaryPath = "runsc" }},
		{name: "unclean runsc path", want: "checkpoint runsc binary path must be canonical absolute", edit: func(p *CompleteCheckpointParams) { p.RunscBinaryPath = "/usr/local/bin/../bin/runsc-test" }},
		{name: "plan digest", want: "checkpoint plan digest is required", edit: func(p *CompleteCheckpointParams) { p.CheckpointPlanDigest = "" }},
		{name: "image manifest digest", want: "checkpoint image manifest digest is required", edit: func(p *CompleteCheckpointParams) { p.CheckpointImageManifestDigest = "" }},
		{name: "invalid image manifest digest", want: "checkpoint image manifest digest is invalid", edit: func(p *CompleteCheckpointParams) { p.CheckpointImageManifestDigest = "checkpoint-image-manifest" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := params
			tt.edit(&p)
			err := st.CompleteGenerationCheckpoint(ctx, p)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("CompleteGenerationCheckpoint error=%v want %q", err, tt.want)
			}
		})
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
	var generationStatus, sessionStatus, networkState, resourceState, checkpointDriverFence, checkpointPlan string
	if err := st.db.QueryRowContext(ctx, `
SELECT g.status, s.status, n.allocation_state, r.resource_state,
       COALESCE(g.checkpoint_driver_states_digest, ''), COALESCE(g.checkpoint_plan_digest, '')
FROM runtime_generations g
JOIN sessions s ON s.active_generation_id = g.generation_id
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&generationStatus, &sessionStatus, &networkState, &resourceState, &checkpointDriverFence, &checkpointPlan); err != nil {
		t.Fatalf("query aborted checkpoint state: %v", err)
	}
	if generationStatus != "idle" ||
		sessionStatus != string(sessionstate.RunningIdle) ||
		networkState != "live" ||
		resourceState != "live" ||
		checkpointDriverFence != "" ||
		checkpointPlan != "" {
		t.Fatalf("unexpected aborted checkpoint state: generation=%s session=%s network=%s resource=%s driver_fence=%s plan=%s",
			generationStatus, sessionStatus, networkState, resourceState, checkpointDriverFence, checkpointPlan)
	}
}

func TestRetireExpiredCheckpointsClearsSessionAndMakesGenerationDestroyable(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	now := time.Now().UTC()
	allocation := createAutoCheckpointGeneration(t, ctx, st, cfg, "sess_retire_checkpoint", GenerationLeaseOwner(owner.UUID), now.Add(-48*time.Hour))
	checkpointedGeneration(t, ctx, st, "sess_retire_checkpoint", allocation.GenerationID, now.Add(-36*time.Hour))
	checkpointPath := filepath.Join(t.TempDir(), "checkpoint")
	if _, err := st.db.ExecContext(ctx, `
UPDATE sessions
SET checkpoint_path = ?,
    restore_ms = 123,
    last_activity_at = ?
WHERE id = ?`, checkpointPath, formatTime(now.Add(-30*time.Hour)), "sess_retire_checkpoint"); err != nil {
		t.Fatalf("seed session checkpoint metadata: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generation_resources
SET checkpoint_path = ?
WHERE generation_id = ?`, checkpointPath, allocation.GenerationID); err != nil {
		t.Fatalf("seed resource checkpoint path: %v", err)
	}

	normal := createAutoCheckpointGeneration(t, ctx, st, cfg, "sess_recent_failed", GenerationLeaseOwner(owner.UUID), now.Add(-30*time.Minute))
	if err := st.FailGeneration(ctx, FailGenerationParams{
		SessionID:    "sess_recent_failed",
		GenerationID: normal.GenerationID,
		Owner:        normal.Owner,
		ErrorClass:   "recent_failure",
		Reason:       "recent failure",
		Now:          now,
	}); err != nil {
		t.Fatalf("fail recent generation: %v", err)
	}

	retired, err := st.RetireExpiredCheckpoints(ctx, RetireExpiredCheckpointsParams{
		OwnerUUID:                owner.UUID,
		Now:                      now,
		CheckpointImageRetention: 24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("retire expired checkpoints: %v", err)
	}
	if len(retired) != 1 || retired[0].SessionID != "sess_retire_checkpoint" || retired[0].GenerationID != allocation.GenerationID || retired[0].EventID == 0 {
		t.Fatalf("unexpected retired checkpoints: %+v", retired)
	}

	var generationStatus, generationError, sessionStatus, networkState, resourceState string
	var sessionCheckpointPath, sessionRestoreMS sql.NullString
	if err := st.db.QueryRowContext(ctx, `
SELECT g.status, COALESCE(g.error_class, ''), s.status, s.checkpoint_path, s.restore_ms,
       n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN sessions s ON s.active_generation_id = g.generation_id
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(
		&generationStatus,
		&generationError,
		&sessionStatus,
		&sessionCheckpointPath,
		&sessionRestoreMS,
		&networkState,
		&resourceState,
	); err != nil {
		t.Fatalf("query retired checkpoint state: %v", err)
	}
	if generationStatus != "failed" ||
		generationError != "checkpoint_retired" ||
		sessionStatus != string(sessionstate.RunningIdle) ||
		sessionCheckpointPath.Valid ||
		sessionRestoreMS.Valid ||
		networkState != "reclaimable" ||
		resourceState != "reclaimable" {
		t.Fatalf("unexpected retired checkpoint state: generation=%s error=%s session=%s checkpoint_valid=%v restore_valid=%v network=%s resource=%s",
			generationStatus, generationError, sessionStatus, sessionCheckpointPath.Valid, sessionRestoreMS.Valid, networkState, resourceState)
	}
	var eventType, eventPayload string
	if err := st.db.QueryRowContext(ctx, `SELECT type, payload FROM events WHERE event_id = ?`, retired[0].EventID).Scan(&eventType, &eventPayload); err != nil {
		t.Fatalf("query retirement event: %v", err)
	}
	if eventType != "session.checkpoint_retired" ||
		!strings.Contains(eventPayload, `"restore_ms":null`) ||
		!strings.Contains(eventPayload, `"session_status":"running_idle"`) ||
		strings.Contains(eventPayload, `"checkpoint_path"`) {
		t.Fatalf("unexpected retirement event: type=%s payload=%s", eventType, eventPayload)
	}

	destroyable, err := st.ListDestroyableReclaimableGenerations(ctx, now, time.Hour)
	if err != nil {
		t.Fatalf("list destroyable generations: %v", err)
	}
	if !hasReclaimableGeneration(destroyable, "sess_retire_checkpoint", allocation.GenerationID) {
		t.Fatalf("checkpoint-retired generation should be immediately destroyable: %+v", destroyable)
	}
	if hasReclaimableGeneration(destroyable, "sess_recent_failed", normal.GenerationID) {
		t.Fatalf("recent ordinary failed generation should still honor failed_retention: %+v", destroyable)
	}
}

func TestRetireExpiredCheckpointsUsesCheckpointCreatedAtWhenSessionActivityIsMissing(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	now := time.Now().UTC()
	old := createAutoCheckpointGeneration(t, ctx, st, cfg, "sess_retire_null_activity", GenerationLeaseOwner(owner.UUID), now.Add(-3*time.Hour))
	checkpointedGeneration(t, ctx, st, "sess_retire_null_activity", old.GenerationID, now.Add(-2*time.Hour))
	young := createAutoCheckpointGeneration(t, ctx, st, cfg, "sess_keep_null_activity", GenerationLeaseOwner(owner.UUID), now.Add(-30*time.Minute))
	checkpointedGeneration(t, ctx, st, "sess_keep_null_activity", young.GenerationID, now.Add(-30*time.Minute))
	for _, sessionID := range []string{"sess_retire_null_activity", "sess_keep_null_activity"} {
		if _, err := st.db.ExecContext(ctx, `
UPDATE sessions
SET last_activity_at = NULL,
    checkpoint_path = ?
WHERE id = ?`, filepath.Join(t.TempDir(), sessionID, "checkpoint"), sessionID); err != nil {
			t.Fatalf("clear last activity for %s: %v", sessionID, err)
		}
	}
	if _, err := st.RetireExpiredCheckpoints(ctx, RetireExpiredCheckpointsParams{
		OwnerUUID:                "wrong-owner",
		Now:                      now,
		CheckpointImageRetention: time.Hour,
	}); err == nil {
		t.Fatalf("owner mismatch should reject checkpoint retirement")
	}
	retired, err := st.RetireExpiredCheckpoints(ctx, RetireExpiredCheckpointsParams{
		OwnerUUID:                owner.UUID,
		Now:                      now,
		CheckpointImageRetention: time.Hour,
	})
	if err != nil {
		t.Fatalf("retire expired checkpoints: %v", err)
	}
	if len(retired) != 1 || retired[0].SessionID != "sess_retire_null_activity" {
		t.Fatalf("unexpected checkpoint-created-at retirements: %+v", retired)
	}
	oldSession, err := st.GetSession(ctx, "sess_retire_null_activity")
	if err != nil {
		t.Fatalf("get old session: %v", err)
	}
	youngSession, err := st.GetSession(ctx, "sess_keep_null_activity")
	if err != nil {
		t.Fatalf("get young session: %v", err)
	}
	if oldSession.Status != string(sessionstate.RunningIdle) || youngSession.Status != string(sessionstate.Checkpointed) {
		t.Fatalf("unexpected checkpoint-created-at statuses: old=%s young=%s", oldSession.Status, youngSession.Status)
	}
}

func TestClaimCheckpointedGenerationForRestoreReturnsStaleAfterCheckpointRetirement(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	now := time.Now().UTC()
	leaseOwner := GenerationLeaseOwner(owner.UUID)
	allocation := createAutoCheckpointGeneration(t, ctx, st, cfg, "sess_restore_stale", leaseOwner, now.Add(-3*time.Hour))
	checkpointedGeneration(t, ctx, st, "sess_restore_stale", allocation.GenerationID, now.Add(-2*time.Hour))
	if _, err := st.db.ExecContext(ctx, `
UPDATE sessions
SET checkpoint_path = ?,
    last_activity_at = ?
WHERE id = ?`, filepath.Join(t.TempDir(), "checkpoint"), formatTime(now.Add(-2*time.Hour)), "sess_restore_stale"); err != nil {
		t.Fatalf("seed stale checkpoint metadata: %v", err)
	}
	retired, err := st.RetireExpiredCheckpoints(ctx, RetireExpiredCheckpointsParams{
		OwnerUUID:                owner.UUID,
		Now:                      now,
		CheckpointImageRetention: time.Hour,
	})
	if err != nil {
		t.Fatalf("retire checkpoint: %v", err)
	}
	if len(retired) != 1 {
		t.Fatalf("expected one retired checkpoint, got %+v", retired)
	}

	_, err = st.ClaimCheckpointedGenerationForRestore(ctx, ClaimCheckpointedGenerationParams{
		SessionID:    "sess_restore_stale",
		GenerationID: allocation.GenerationID,
		Owner:        leaseOwner,
		LeaseTTL:     time.Minute,
		Now:          now.Add(time.Second),
	})
	if !errors.Is(err, ErrStaleCheckpointRestore) {
		t.Fatalf("expected stale checkpoint restore error, got %v", err)
	}
}
