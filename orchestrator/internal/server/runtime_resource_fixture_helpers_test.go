package server

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/config"
	"harness-platform/orchestrator/internal/sessionstate"
	"harness-platform/orchestrator/internal/store"
)

func serverPostStartProofForTest(instance store.RuntimeResourceInstance) *store.RuntimeResourcePostStartProof {
	return &store.RuntimeResourcePostStartProof{
		HostID:                 instance.HostID,
		GenerationID:           instance.GenerationID,
		ContractID:             instance.ContractID,
		SandboxContractVersion: instance.SandboxContractVersion,
		RunscContainerID:       instance.RunscContainerID,
		RunscState:             "runsc_container:" + instance.RunscContainerID + ":running; check=test",
		RunscPlatform:          instance.RunscPlatform,
		RunscVersion:           instance.RunscVersion,
		RunscBinaryPath:        instance.RunscBinaryPath,
		RunscBinaryDigest:      instance.RunscBinaryDigest,
		IPNetns:                "netns:present; check=test",
		IPLink:                 "host_veth:present; check=test",
		NFT:                    "nft_table:present; check=test",
		BridgeStartup:          "bridge_startup_probe:passed; check=test",
	}
}

func serverRunscContainerID(t *testing.T, ctx context.Context, st *store.Store, sessionID, generationID string) string {
	t.Helper()
	details, err := st.GetRuntimeGenerationDetails(ctx, sessionID, generationID)
	if err != nil {
		t.Fatalf("get generation details: %v", err)
	}
	if strings.TrimSpace(details.RunscContainerID) == "" {
		t.Fatalf("generation %s has no runsc container id", generationID)
	}
	return details.RunscContainerID
}

func enableSessionAutoCheckpoint(t *testing.T, ctx context.Context, st *store.Store, sessionID string) {
	t.Helper()
	if _, err := st.DBForTest().ExecContext(ctx, `UPDATE sessions SET auto_checkpoint_enabled = 1 WHERE id = ?`, sessionID); err != nil {
		t.Fatalf("enable auto checkpoint: %v", err)
	}
}

func mustRuntimeResourceHostID(t *testing.T) string {
	t.Helper()
	hostID, err := runtimeResourceHostID()
	if err != nil {
		t.Fatalf("runtime resource host id: %v", err)
	}
	return hostID
}

func mustRuntimeResourceNftTableName(t *testing.T, generationID string) string {
	t.Helper()
	tableName, err := runtimeResourceNftTableName(generationID)
	if err != nil {
		t.Fatalf("runtime resource nft table name: %v", err)
	}
	return tableName
}

func prepareServerIdleGeneration(t *testing.T, ctx context.Context, st *store.Store, cfg config.Config, ownerUUID, sessionID string) store.GenerationAllocation {
	t.Helper()
	now := time.Now().UTC()
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: sessionID,
		Owner:     store.GenerationLeaseOwner(ownerUUID),
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    serverTestAllocatorConfig(cfg, "claude_code"),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	artifacts := testGenerationArtifacts()
	if err := st.RecordGenerationRuntimeArtifactDigests(ctx, allocation.GenerationID, store.GenerationRuntimeArtifactDigests{
		ControlManifestDigest:          artifacts.ManifestDigest,
		ProjectedControlManifestDigest: artifacts.ProjectedManifestDigest,
		BundleDigest:                   artifacts.BundleDigest,
		RuntimeConfigDigest:            artifacts.RuntimeConfigDigest,
		SpecDigest:                     artifacts.SpecDigest,
		RunscVersion:                   artifacts.RunscVersion,
		RunscBinaryPath:                artifacts.RunscBinaryPath,
		RunscBinaryDigest:              artifacts.RunscBinaryDigest,
	}); err != nil {
		t.Fatalf("record runtime artifacts: %v", err)
	}
	storeServerGenerationPlanForArtifacts(t, ctx, st, sessionID, allocation.GenerationID, artifacts)
	if err := st.MarkGenerationResourcesLive(ctx, sessionID, allocation.GenerationID, allocation.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark generation live: %v", err)
	}
	createServerRuntimeResourceLive(t, ctx, st, sessionID, allocation, ownerUUID, mustRuntimeResourceHostID(t), now.Add(2*time.Second))
	if err := st.UpdateSessionStatusAndActivity(ctx, sessionID, string(sessionstate.RunningIdle), nil, now.Add(-time.Minute)); err != nil {
		t.Fatalf("mark session idle: %v", err)
	}
	return allocation
}

func markServerGenerationCheckpointed(t *testing.T, ctx context.Context, st *store.Store, sessionID, generationID string, now time.Time) {
	t.Helper()
	ensureServerRuntimeResourceLiveForCheckpoint(t, ctx, st, sessionID, generationID, now.Add(-time.Millisecond))
	formattedNow := now.UTC().Format(time.RFC3339Nano)
	fence := serverCheckpointDriverStateFenceForTest(t, ctx, st, sessionID, generationID)
	checkpointPlanDigest := "sha256:plan"
	if plan, err := st.GetGenerationPlan(ctx, generationID); err == nil {
		checkpointPlanDigest = plan.PlanDigest
	} else if err != sql.ErrNoRows {
		t.Fatalf("get checkpoint generation plan: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'checkpointed',
    checkpoint_created_at = ?,
    checkpoint_network_profile_id = network_profile_id,
    checkpoint_agent_runtime_profile_id = agent_runtime_profile_id,
    checkpoint_runsc_version = runsc_version,
    checkpoint_runsc_platform = runsc_platform,
    checkpoint_runsc_binary_path = (
      SELECT runsc_binary_path
      FROM runtime_generation_resources
      WHERE runtime_generation_resources.generation_id = runtime_generations.generation_id
    ),
    checkpoint_runsc_binary_digest = (
      SELECT runsc_binary_digest
      FROM runtime_generation_resources
      WHERE runtime_generation_resources.generation_id = runtime_generations.generation_id
    ),
    checkpoint_bundle_digest = 'bundle_digest',
    checkpoint_runtime_config_digest = 'runtime_config_digest',
    checkpoint_control_manifest_digest = (
      SELECT control_manifest_digest
      FROM runtime_generation_resources
      WHERE runtime_generation_resources.generation_id = runtime_generations.generation_id
    ),
    checkpoint_driver_states_digest = ?,
    checkpoint_plan_digest = ?,
    checkpoint_image_manifest_digest = ?,
    lease_owner = NULL,
    lease_expires_at = NULL,
    last_seen_at = ?
WHERE generation_id = ?
  AND session_id = ?`, formattedNow, fence, checkpointPlanDigest, checkpointImageManifestDigestForTest, formattedNow, generationID, sessionID); err != nil {
		t.Fatalf("set checkpointed generation: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE network_profiles
SET allocation_state = 'reserved_checkpointed'
WHERE generation_id = ?
  AND session_id = ?`, generationID, sessionID); err != nil {
		t.Fatalf("reserve checkpointed network: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generation_resources
SET resource_state = 'reserved_checkpointed'
WHERE generation_id = ?`, generationID); err != nil {
		t.Fatalf("reserve checkpointed resources: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_resource_instances
SET state = 'checkpoint_reserved',
    lease_expires_at = NULL,
    idempotency_token = NULL,
    updated_at = ?
WHERE generation_id = ?
  AND state IN ('live', 'checkpoint_reserved')`, formattedNow, generationID); err != nil {
		t.Fatalf("reserve checkpointed runtime resource: %v", err)
	}
	if err := st.UpdateSessionStatus(ctx, sessionID, string(sessionstate.Checkpointed), nil); err != nil {
		t.Fatalf("set checkpointed session: %v", err)
	}
}

func ensureServerRuntimeResourceLiveForCheckpoint(t *testing.T, ctx context.Context, st *store.Store, sessionID, generationID string, now time.Time) {
	t.Helper()
	if _, err := st.GetRuntimeResourceInstance(ctx, generationID); err == nil {
		return
	} else if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("get checkpoint runtime resource instance: %v", err)
	}
	allocation := serverGenerationAllocationForTest(t, ctx, st, sessionID, generationID)
	createServerRuntimeResourceLive(t, ctx, st, sessionID, allocation, "checkpoint-test-owner", mustRuntimeResourceHostID(t), now)
}

func serverGenerationAllocationForTest(t *testing.T, ctx context.Context, st *store.Store, sessionID, generationID string) store.GenerationAllocation {
	t.Helper()
	allocation := store.GenerationAllocation{GenerationID: generationID}
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT network_profile_id, agent_runtime_profile_id, COALESCE(lease_owner, '')
FROM runtime_generations
WHERE session_id = ?
  AND generation_id = ?`, sessionID, generationID).Scan(
		&allocation.NetworkProfileID,
		&allocation.AgentRuntimeProfileID,
		&allocation.Owner,
	); err != nil {
		t.Fatalf("query generation allocation for checkpoint: %v", err)
	}
	if strings.TrimSpace(allocation.Owner) == "" {
		allocation.Owner = store.GenerationLeaseOwner("checkpoint-test-owner")
	}
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT driver_id, state_digest, state_version
FROM session_driver_states
WHERE session_id = ?`, sessionID).Scan(
		&allocation.DriverState.DriverID,
		&allocation.DriverState.StateDigest,
		&allocation.DriverState.StateVersion,
	); err != nil {
		t.Fatalf("query driver state for checkpoint: %v", err)
	}
	return allocation
}

func serverCheckpointDriverStateFenceForTest(t *testing.T, ctx context.Context, st *store.Store, sessionID, generationID string) string {
	t.Helper()
	var driverID, stateDigest string
	var stateVersion int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT ds.driver_id, ds.state_digest, ds.state_version
FROM session_driver_states ds
JOIN runtime_generations g ON g.session_id = ds.session_id
JOIN agent_runtime_profiles a ON a.agent_runtime_profile_id = g.agent_runtime_profile_id
WHERE g.session_id = ?
  AND g.generation_id = ?
  AND ds.driver_id = a.driver_id`, sessionID, generationID).Scan(&driverID, &stateDigest, &stateVersion); err != nil {
		t.Fatalf("query driver state fence input: %v", err)
	}
	fence, err := store.CheckpointDriverStatesDigest(generationID, []store.DriverStateToken{{
		DriverID:     driverID,
		StateDigest:  stateDigest,
		StateVersion: stateVersion,
	}})
	if err != nil {
		t.Fatalf("compute driver state fence: %v", err)
	}
	return fence
}

func createServerGenerationFilesystem(t *testing.T, details store.RuntimeGenerationDetails) {
	t.Helper()
	for _, path := range []string{details.CheckpointPath, details.ControlDirPath, details.BundleDirPath, details.BridgeDirPath, details.LogDirPath} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatalf("create generation filesystem path %s: %v", path, err)
		}
		if err := os.WriteFile(filepath.Join(path, ".keep"), []byte("keep"), 0o644); err != nil {
			t.Fatalf("write generation filesystem marker %s: %v", path, err)
		}
	}
}

func serverSandboxSourceIPForGeneration(t *testing.T, ctx context.Context, st *store.Store, generationID string) string {
	t.Helper()
	var sandboxCIDR string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT sandbox_ip_cidr
FROM network_profiles
WHERE generation_id = ?`, generationID).Scan(&sandboxCIDR); err != nil {
		t.Fatalf("query sandbox ip cidr: %v", err)
	}
	parts := strings.SplitN(sandboxCIDR, "/", 2)
	if len(parts) != 2 || parts[0] == "" {
		t.Fatalf("unexpected sandbox ip cidr: %q", sandboxCIDR)
	}
	return parts[0]
}
