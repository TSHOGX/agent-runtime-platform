package store

import (
	"context"
	"net/netip"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRuntimeResourceInstanceStateMachine(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	now := time.Now().UTC()
	instance := createRuntimeResourceInstanceForTest(t, ctx, st, owner.UUID, "sess_resource_state", "host-1", now)
	workerID := "worker-1"

	if err := st.ClaimRuntimeResourceMaterialization(ctx, RuntimeResourceMaterializationClaimParams{
		GenerationID:     instance.GenerationID,
		WorkerID:         workerID,
		HostID:           instance.HostID,
		LeaseExpiresAt:   now.Add(time.Minute),
		IdempotencyToken: "idem-1",
		Now:              now.Add(time.Second),
	}); err != nil {
		t.Fatalf("claim materialization: %v", err)
	}
	if err := st.MarkRuntimeResourceReady(ctx, RuntimeResourceWorkerTransitionParams{
		GenerationID: instance.GenerationID,
		WorkerID:     workerID,
		HostID:       instance.HostID,
		Now:          now.Add(2 * time.Second),
	}); err != nil {
		t.Fatalf("mark ready: %v", err)
	}
	if err := st.ClaimRuntimeResourceCheckpointRestore(ctx, RuntimeResourceMaterializationClaimParams{
		GenerationID:     instance.GenerationID,
		WorkerID:         workerID,
		HostID:           instance.HostID,
		LeaseExpiresAt:   now.Add(time.Minute),
		IdempotencyToken: "idem-forbidden",
		Now:              now.Add(3 * time.Second),
	}); err == nil || !strings.Contains(err.Error(), "materialization CAS failed") {
		t.Fatalf("expected ready -> materializing rejection, got %v", err)
	}
	if err := st.MarkRuntimeResourceLive(ctx, RuntimeResourceWorkerTransitionParams{
		GenerationID: instance.GenerationID,
		WorkerID:     workerID,
		HostID:       instance.HostID,
		Now:          now.Add(4 * time.Second),
	}); err != nil {
		t.Fatalf("mark live: %v", err)
	}
	if err := st.ReserveRuntimeResourceCheckpoint(ctx, RuntimeResourceWorkerTransitionParams{
		GenerationID: instance.GenerationID,
		WorkerID:     workerID,
		HostID:       instance.HostID,
		Now:          now.Add(5 * time.Second),
	}); err != nil {
		t.Fatalf("reserve checkpoint: %v", err)
	}
	evidence := runtimeResourceEvidenceForTest(instance.HostID)
	if err := st.MarkRuntimeResourceAbsentVerified(ctx, RuntimeResourceEvidenceParams{
		GenerationID: instance.GenerationID,
		WorkerID:     workerID,
		HostID:       instance.HostID,
		Evidence:     evidence,
		Now:          now.Add(5500 * time.Millisecond),
	}); err == nil || !strings.Contains(err.Error(), "requires reconciling state") {
		t.Fatalf("expected checkpoint_reserved -> absent_verified rejection, got %v", err)
	}
	if err := st.MarkRuntimeResourceDestroyed(ctx, RuntimeResourceRetireParams{
		GenerationID: instance.GenerationID,
		WorkerID:     workerID,
		HostID:       instance.HostID,
		Now:          now.Add(6 * time.Second),
	}); err == nil || !strings.Contains(err.Error(), "destroyed CAS failed") {
		t.Fatalf("expected checkpoint_reserved -> destroyed rejection, got %v", err)
	}
	if err := st.ClaimRuntimeResourceCheckpointRestore(ctx, RuntimeResourceMaterializationClaimParams{
		GenerationID:     instance.GenerationID,
		WorkerID:         workerID,
		HostID:           instance.HostID,
		LeaseExpiresAt:   now.Add(2 * time.Minute),
		IdempotencyToken: "idem-restore",
		Now:              now.Add(7 * time.Second),
	}); err != nil {
		t.Fatalf("claim checkpoint restore: %v", err)
	}
	if err := st.MarkRuntimeResourceReady(ctx, RuntimeResourceWorkerTransitionParams{
		GenerationID: instance.GenerationID,
		WorkerID:     workerID,
		HostID:       instance.HostID,
		Now:          now.Add(8 * time.Second),
	}); err != nil {
		t.Fatalf("mark restored ready: %v", err)
	}
	if err := st.MarkRuntimeResourceLive(ctx, RuntimeResourceWorkerTransitionParams{
		GenerationID: instance.GenerationID,
		WorkerID:     workerID,
		HostID:       instance.HostID,
		Now:          now.Add(9 * time.Second),
	}); err != nil {
		t.Fatalf("mark restored live: %v", err)
	}
	if err := st.ClaimRuntimeResourceRetiring(ctx, RuntimeResourceRetireParams{
		GenerationID: instance.GenerationID,
		WorkerID:     workerID,
		HostID:       instance.HostID,
		Now:          now.Add(10 * time.Second),
	}); err != nil {
		t.Fatalf("claim retiring: %v", err)
	}
	if err := st.MarkRuntimeResourceReconciling(ctx, RuntimeResourceEvidenceParams{
		GenerationID: instance.GenerationID,
		WorkerID:     workerID,
		HostID:       instance.HostID,
		Evidence:     evidence,
		Now:          now.Add(11 * time.Second),
	}); err != nil {
		t.Fatalf("mark reconciling: %v", err)
	}
	if err := st.MarkRuntimeResourceAbsentVerified(ctx, RuntimeResourceEvidenceParams{
		GenerationID: instance.GenerationID,
		WorkerID:     workerID,
		HostID:       instance.HostID,
		Evidence:     evidence,
		Now:          now.Add(12 * time.Second),
	}); err != nil {
		t.Fatalf("mark absent verified: %v", err)
	}
	if err := st.MarkRuntimeResourceDestroyed(ctx, RuntimeResourceRetireParams{
		GenerationID: instance.GenerationID,
		WorkerID:     workerID,
		HostID:       instance.HostID,
		Now:          now.Add(13 * time.Second),
	}); err != nil {
		t.Fatalf("mark destroyed: %v", err)
	}
	got, err := st.GetRuntimeResourceInstance(ctx, instance.GenerationID)
	if err != nil {
		t.Fatalf("get final instance: %v", err)
	}
	if got.State != RuntimeResourceDestroyed || got.VerifiedAt == nil || got.EvidenceDigest == "" {
		t.Fatalf("unexpected final resource state: %+v", got)
	}
}

func TestRuntimeResourceIdentityReuseRequiresAbsentVerified(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	now := time.Now().UTC()
	first := createRuntimeResourceInstanceForTest(t, ctx, st, owner.UUID, "sess_resource_first", "host-1", now)
	secondParams := runtimeResourceInstanceParamsForTest(t, ctx, st, owner.UUID, "sess_resource_second", "host-1", now.Add(time.Second))
	secondParams.RunscContainerID = first.RunscContainerID

	if _, err := st.CreateRuntimeResourceInstance(ctx, secondParams); err == nil {
		t.Fatalf("expected active runsc_container_id uniqueness rejection")
	}
	if err := st.ClaimRuntimeResourceRetiring(ctx, RuntimeResourceRetireParams{
		GenerationID: first.GenerationID,
		WorkerID:     "worker-cleanup",
		HostID:       first.HostID,
		Now:          now.Add(2 * time.Second),
	}); err != nil {
		t.Fatalf("retire first: %v", err)
	}
	evidence := runtimeResourceEvidenceForTest(first.HostID)
	if err := st.MarkRuntimeResourceReconciling(ctx, RuntimeResourceEvidenceParams{
		GenerationID: first.GenerationID,
		WorkerID:     "worker-cleanup",
		HostID:       first.HostID,
		Evidence:     evidence,
		Now:          now.Add(3 * time.Second),
	}); err != nil {
		t.Fatalf("reconcile first: %v", err)
	}
	if _, err := st.CreateRuntimeResourceInstance(ctx, secondParams); err == nil {
		t.Fatalf("expected reconciling runsc_container_id to keep blocking reuse")
	}
	if err := st.MarkRuntimeResourceAbsentVerified(ctx, RuntimeResourceEvidenceParams{
		GenerationID: first.GenerationID,
		WorkerID:     "worker-cleanup",
		HostID:       first.HostID,
		Evidence:     evidence,
		Now:          now.Add(4 * time.Second),
	}); err != nil {
		t.Fatalf("absent verify first: %v", err)
	}
	second, err := st.CreateRuntimeResourceInstance(ctx, secondParams)
	if err != nil {
		t.Fatalf("create second after absent_verified: %v", err)
	}
	if second.RunscContainerID != first.RunscContainerID || second.State != RuntimeResourceAllocated {
		t.Fatalf("unexpected second resource: %+v", second)
	}
}

func TestRuntimeResourceAbsentVerifiedRejectsCorruptIdentityPayload(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	now := time.Now().UTC()
	instance := createRuntimeResourceInstanceForTest(t, ctx, st, owner.UUID, "sess_resource_corrupt", "host-1", now)
	if err := st.ClaimRuntimeResourceRetiring(ctx, RuntimeResourceRetireParams{
		GenerationID: instance.GenerationID,
		WorkerID:     "worker-cleanup",
		HostID:       instance.HostID,
		Now:          now.Add(time.Second),
	}); err != nil {
		t.Fatalf("retire resource: %v", err)
	}
	evidence := runtimeResourceEvidenceForTest(instance.HostID)
	if err := st.MarkRuntimeResourceReconciling(ctx, RuntimeResourceEvidenceParams{
		GenerationID: instance.GenerationID,
		WorkerID:     "worker-cleanup",
		HostID:       instance.HostID,
		Evidence:     evidence,
		Now:          now.Add(2 * time.Second),
	}); err != nil {
		t.Fatalf("mark reconciling: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_resource_instances
SET resource_identity_payload = '{}'
WHERE generation_id = ?`, instance.GenerationID); err != nil {
		t.Fatalf("corrupt identity payload: %v", err)
	}
	err := st.MarkRuntimeResourceAbsentVerified(ctx, RuntimeResourceEvidenceParams{
		GenerationID: instance.GenerationID,
		WorkerID:     "worker-cleanup",
		HostID:       instance.HostID,
		Evidence:     evidence,
		Now:          now.Add(3 * time.Second),
	})
	if err == nil || !strings.Contains(err.Error(), "identity digest mismatch") {
		t.Fatalf("expected identity digest mismatch, got %v", err)
	}
}

func TestRuntimeResourceAbsentVerifiedRequiresHostEvidence(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	now := time.Now().UTC()
	instance := createRuntimeResourceInstanceForTest(t, ctx, st, owner.UUID, "sess_resource_evidence", "host-1", now)
	if err := st.ClaimRuntimeResourceRetiring(ctx, RuntimeResourceRetireParams{
		GenerationID: instance.GenerationID,
		WorkerID:     "worker-cleanup",
		HostID:       instance.HostID,
		Now:          now.Add(time.Second),
	}); err != nil {
		t.Fatalf("retire resource: %v", err)
	}
	evidence := runtimeResourceEvidenceForTest(instance.HostID)
	if err := st.MarkRuntimeResourceReconciling(ctx, RuntimeResourceEvidenceParams{
		GenerationID: instance.GenerationID,
		WorkerID:     "worker-cleanup",
		HostID:       instance.HostID,
		Evidence:     evidence,
		Now:          now.Add(2 * time.Second),
	}); err != nil {
		t.Fatalf("mark reconciling: %v", err)
	}
	evidence.HostID = "wrong-host"
	err := st.MarkRuntimeResourceAbsentVerified(ctx, RuntimeResourceEvidenceParams{
		GenerationID: instance.GenerationID,
		WorkerID:     "worker-cleanup",
		HostID:       instance.HostID,
		Evidence:     evidence,
		Now:          now.Add(3 * time.Second),
	})
	if err == nil || !strings.Contains(err.Error(), "host_id") {
		t.Fatalf("expected host evidence rejection, got %v", err)
	}
}

func createRuntimeResourceInstanceForTest(t *testing.T, ctx context.Context, st *Store, ownerUUID, sessionID, hostID string, now time.Time) RuntimeResourceInstance {
	t.Helper()
	params := runtimeResourceInstanceParamsForTest(t, ctx, st, ownerUUID, sessionID, hostID, now)
	instance, err := st.CreateRuntimeResourceInstance(ctx, params)
	if err != nil {
		t.Fatalf("create runtime resource instance: %v", err)
	}
	return instance
}

func runtimeResourceInstanceParamsForTest(t *testing.T, ctx context.Context, st *Store, ownerUUID, sessionID, hostID string, now time.Time) RuntimeResourceInstanceParams {
	t.Helper()
	createStoreSession(t, ctx, st, sessionID)
	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: sessionID,
		Owner:     GenerationLeaseOwner(ownerUUID),
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    testAllocatorConfig(t),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	contractID := "contract_" + allocation.GenerationID
	if _, err := st.StoreSandboxContract(ctx, StoreSandboxContractParams{
		ContractID:   contractID,
		SessionID:    sessionID,
		GenerationID: allocation.GenerationID,
		Payload:      testSandboxContractPayload(t, sessionID, allocation),
		Now:          now.Add(time.Millisecond),
	}); err != nil {
		t.Fatalf("store sandbox contract: %v", err)
	}
	details, err := st.GetRuntimeGenerationDetails(ctx, sessionID, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get generation details: %v", err)
	}
	sandboxIP := sandboxIPFromCIDRForTest(t, details.SandboxIPCIDR)
	runscPath := filepath.Join(t.TempDir(), "runsc")
	return RuntimeResourceInstanceParams{
		GenerationID:           allocation.GenerationID,
		SessionID:              sessionID,
		ContractID:             contractID,
		SandboxContractVersion: SandboxContractVersion,
		HostID:                 hostID,
		RunscContainerID:       "harness-gen-" + allocation.GenerationID,
		RunscPlatform:          "systrap",
		RunscVersion:           "runsc test",
		RunscBinaryPath:        runscPath,
		RunscBinaryDigest:      "sha256:runsc",
		NetworkProfileID:       allocation.NetworkProfileID,
		NetnsName:              details.NetnsName,
		NetnsPath:              details.NetnsPath,
		HostVeth:               details.HostVeth,
		SandboxVeth:            details.SandboxVeth,
		HostGatewayIP:          details.HostGatewayIP,
		SandboxIP:              sandboxIP,
		SandboxIPCIDR:          details.SandboxIPCIDR,
		HostSideCIDR:           details.HostSideCIDR,
		NftTableName:           "harness_gen_" + strings.TrimPrefix(allocation.GenerationID, "gen_"),
		ControlDirPath:         details.ControlDirPath,
		ControlManifestPath:    details.ControlManifestPath,
		BundleDirPath:          details.BundleDirPath,
		SpecPath:               details.SpecPath,
		CheckpointPath:         details.CheckpointPath,
		BridgeDirPath:          details.BridgeDirPath,
		LogDirPath:             details.LogDirPath,
		RootPrefixes: map[string]string{
			"run_dir":      filepath.Dir(filepath.Dir(details.ControlDirPath)),
			"control_root": filepath.Dir(details.ControlDirPath),
			"bridge_root":  filepath.Dir(details.BridgeDirPath),
		},
		Now: now.Add(2 * time.Millisecond),
	}
}

func sandboxIPFromCIDRForTest(t *testing.T, cidr string) string {
	t.Helper()
	prefix, err := netip.ParsePrefix(cidr)
	if err != nil {
		t.Fatalf("parse sandbox cidr: %v", err)
	}
	return prefix.Addr().String()
}

func runtimeResourceEvidenceForTest(hostID string) ResourceReconciliationEvidence {
	return ResourceReconciliationEvidence{
		HostID:     hostID,
		RunscState: "absent",
		IPNetns:    "absent",
		IPLink:     "absent",
		NFT:        "absent",
		FilesystemLstat: map[string]string{
			"control":    "absent",
			"bundle":     "absent",
			"checkpoint": "absent",
			"bridge":     "absent",
			"log":        "absent",
		},
	}
}
