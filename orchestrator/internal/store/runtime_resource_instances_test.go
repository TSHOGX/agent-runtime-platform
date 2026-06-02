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
		Now:          now.Add(3500 * time.Millisecond),
	}); err == nil || !strings.Contains(err.Error(), "post-start proof") {
		t.Fatalf("expected ready -> live proof rejection, got %v", err)
	}
	if err := st.MarkRuntimeResourceLive(ctx, RuntimeResourceWorkerTransitionParams{
		GenerationID: instance.GenerationID,
		WorkerID:     workerID,
		HostID:       instance.HostID,
		PostStart:    runtimeResourcePostStartProofForTest(instance),
		Now:          now.Add(4 * time.Second),
	}); err != nil {
		t.Fatalf("mark live: %v", err)
	}
	live, err := st.GetRuntimeResourceInstance(ctx, instance.GenerationID)
	if err != nil {
		t.Fatalf("get live runtime resource: %v", err)
	}
	if live.LeaseExpiresAt != nil || live.IdempotencyToken != "" {
		t.Fatalf("live runtime resource should clear materialization lease, got expires=%v token=%q", live.LeaseExpiresAt, live.IdempotencyToken)
	}
	if err := st.ReserveRuntimeResourceCheckpoint(ctx, RuntimeResourceWorkerTransitionParams{
		GenerationID: instance.GenerationID,
		WorkerID:     workerID,
		HostID:       instance.HostID,
		Now:          now.Add(5 * time.Second),
	}); err != nil {
		t.Fatalf("reserve checkpoint: %v", err)
	}
	evidence := runtimeResourceEvidenceForTest(instance)
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
		PostStart:    runtimeResourcePostStartProofForTest(instance),
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
	evidence := runtimeResourceEvidenceForTest(first)
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

func TestRuntimeResourceCleanupTransitionsRequireWorkerIDBeforeMutation(t *testing.T) {
	ctx := context.Background()
	const workerID = "worker-cleanup"

	t.Run("retiring", func(t *testing.T) {
		st, owner := openOwnedStore(t, ctx)
		now := time.Now().UTC()
		before, _ := runtimeResourceInCleanupStateForTest(t, ctx, st, owner.UUID, "sess_resource_retire_worker_required", now, RuntimeResourceMaterializing, workerID)

		err := st.ClaimRuntimeResourceRetiring(ctx, RuntimeResourceRetireParams{
			GenerationID: before.GenerationID,
			WorkerID:     " ",
			HostID:       before.HostID,
			Now:          now.Add(10 * time.Second),
		})
		requireRuntimeResourceWorkerIDErrorForTest(t, err)
		assertRuntimeResourceUnchangedForTest(t, ctx, st, before)
	})

	t.Run("reconciling", func(t *testing.T) {
		st, owner := openOwnedStore(t, ctx)
		now := time.Now().UTC()
		before, evidence := runtimeResourceInCleanupStateForTest(t, ctx, st, owner.UUID, "sess_resource_reconcile_worker_required", now, RuntimeResourceRetiring, workerID)

		err := st.MarkRuntimeResourceReconciling(ctx, RuntimeResourceEvidenceParams{
			GenerationID: before.GenerationID,
			WorkerID:     " ",
			HostID:       before.HostID,
			Evidence:     evidence,
			Now:          now.Add(10 * time.Second),
		})
		requireRuntimeResourceWorkerIDErrorForTest(t, err)
		assertRuntimeResourceUnchangedForTest(t, ctx, st, before)
	})

	t.Run("absent verified", func(t *testing.T) {
		st, owner := openOwnedStore(t, ctx)
		now := time.Now().UTC()
		before, evidence := runtimeResourceInCleanupStateForTest(t, ctx, st, owner.UUID, "sess_resource_absent_worker_required", now, RuntimeResourceReconciling, workerID)

		err := st.MarkRuntimeResourceAbsentVerified(ctx, RuntimeResourceEvidenceParams{
			GenerationID: before.GenerationID,
			WorkerID:     " ",
			HostID:       before.HostID,
			Evidence:     evidence,
			Now:          now.Add(10 * time.Second),
		})
		requireRuntimeResourceWorkerIDErrorForTest(t, err)
		assertRuntimeResourceUnchangedForTest(t, ctx, st, before)
	})

	t.Run("destroyed", func(t *testing.T) {
		st, owner := openOwnedStore(t, ctx)
		now := time.Now().UTC()
		before, _ := runtimeResourceInCleanupStateForTest(t, ctx, st, owner.UUID, "sess_resource_destroy_worker_required", now, RuntimeResourceAbsentVerified, workerID)

		err := st.MarkRuntimeResourceDestroyed(ctx, RuntimeResourceRetireParams{
			GenerationID: before.GenerationID,
			WorkerID:     " ",
			HostID:       before.HostID,
			Now:          now.Add(10 * time.Second),
		})
		requireRuntimeResourceWorkerIDErrorForTest(t, err)
		assertRuntimeResourceUnchangedForTest(t, ctx, st, before)
	})
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
	evidence := runtimeResourceEvidenceForTest(instance)
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

func TestRuntimeResourceCleanupIdentityUsesVerifiedPayload(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	now := time.Now().UTC()
	instance := createRuntimeResourceInstanceForTest(t, ctx, st, owner.UUID, "sess_resource_cleanup_identity", "host-1", now)
	originalBridgeDir := instance.BridgeDirPath
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_resource_instances
SET bridge_dir_path = ?
WHERE generation_id = ?`, filepath.Join(t.TempDir(), "corrupt-bridge"), instance.GenerationID); err != nil {
		t.Fatalf("corrupt row mirror: %v", err)
	}
	if _, err := st.GetRuntimeResourceInstance(ctx, instance.GenerationID); err == nil || !strings.Contains(err.Error(), "payload does not match row mirrors") {
		t.Fatalf("expected strict getter to reject row mirror corruption, got %v", err)
	}
	cleanupIdentity, err := st.GetRuntimeResourceCleanupIdentity(ctx, instance.GenerationID)
	if err != nil {
		t.Fatalf("get cleanup identity: %v", err)
	}
	if cleanupIdentity.BridgeDirPath != originalBridgeDir {
		t.Fatalf("cleanup identity used row mirror bridge path %q, want payload path %q", cleanupIdentity.BridgeDirPath, originalBridgeDir)
	}
	if err := st.ClaimRuntimeResourceRetiring(ctx, RuntimeResourceRetireParams{
		GenerationID: instance.GenerationID,
		WorkerID:     "worker-cleanup",
		HostID:       cleanupIdentity.HostID,
		Now:          now.Add(time.Second),
	}); err != nil {
		t.Fatalf("retire resource: %v", err)
	}
	evidence := runtimeResourceEvidenceForTest(cleanupIdentity)
	if err := st.MarkRuntimeResourceReconciling(ctx, RuntimeResourceEvidenceParams{
		GenerationID: instance.GenerationID,
		WorkerID:     "worker-cleanup",
		HostID:       cleanupIdentity.HostID,
		Evidence:     evidence,
		Now:          now.Add(2 * time.Second),
	}); err != nil {
		t.Fatalf("mark reconciling: %v", err)
	}
	if err := st.MarkRuntimeResourceAbsentVerified(ctx, RuntimeResourceEvidenceParams{
		GenerationID: instance.GenerationID,
		WorkerID:     "worker-cleanup",
		HostID:       cleanupIdentity.HostID,
		Evidence:     evidence,
		Now:          now.Add(3 * time.Second),
	}); err != nil {
		t.Fatalf("absent verify with row mirror corruption: %v", err)
	}
}

func TestRuntimeResourceCleanupIdentityRejectsCorruptPayload(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	now := time.Now().UTC()
	instance := createRuntimeResourceInstanceForTest(t, ctx, st, owner.UUID, "sess_resource_cleanup_corrupt_payload", "host-1", now)
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_resource_instances
SET resource_identity_payload = '{}'
WHERE generation_id = ?`, instance.GenerationID); err != nil {
		t.Fatalf("corrupt identity payload: %v", err)
	}
	_, err := st.GetRuntimeResourceCleanupIdentity(ctx, instance.GenerationID)
	if err == nil || !strings.Contains(err.Error(), "identity digest mismatch") {
		t.Fatalf("expected identity digest mismatch, got %v", err)
	}
}

func TestRuntimeResourceCleanupIdentityRejectsNonCanonicalPayloadPaths(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	now := time.Now().UTC()
	instance := createRuntimeResourceInstanceForTest(t, ctx, st, owner.UUID, "sess_resource_cleanup_corrupt_payload_path", "host-1", now)
	payload, err := verifyRuntimeResourceIdentityPayload(instance)
	if err != nil {
		t.Fatalf("load identity payload: %v", err)
	}
	payload.BridgeDirPath = "bridge/gen-1"
	corruptPayload, err := canonicalDataVolumeJSON(payload)
	if err != nil {
		t.Fatalf("canonical corrupt payload: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_resource_instances
SET resource_identity_payload = ?,
    resource_identity_digest = ?
WHERE generation_id = ?`, string(corruptPayload), SandboxContractDigest(corruptPayload), instance.GenerationID); err != nil {
		t.Fatalf("store corrupt identity payload path: %v", err)
	}
	_, err = st.GetRuntimeResourceCleanupIdentity(ctx, instance.GenerationID)
	if err == nil || !strings.Contains(err.Error(), "runtime resource identity bridge dir path must be canonical absolute") {
		t.Fatalf("expected non-canonical identity path rejection, got %v", err)
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
	evidence := runtimeResourceEvidenceForTest(instance)
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

func TestRuntimeResourceAbsentVerifiedRejectsSyntheticAbsenceEvidence(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	now := time.Now().UTC()
	instance := createRuntimeResourceInstanceForTest(t, ctx, st, owner.UUID, "sess_resource_synthetic_evidence", "host-1", now)
	if err := st.ClaimRuntimeResourceRetiring(ctx, RuntimeResourceRetireParams{
		GenerationID: instance.GenerationID,
		WorkerID:     "worker-cleanup",
		HostID:       instance.HostID,
		Now:          now.Add(time.Second),
	}); err != nil {
		t.Fatalf("retire resource: %v", err)
	}
	evidence := runtimeResourceEvidenceForTest(instance)
	if err := st.MarkRuntimeResourceReconciling(ctx, RuntimeResourceEvidenceParams{
		GenerationID: instance.GenerationID,
		WorkerID:     "worker-cleanup",
		HostID:       instance.HostID,
		Evidence:     evidence,
		Now:          now.Add(2 * time.Second),
	}); err != nil {
		t.Fatalf("mark reconciling: %v", err)
	}
	evidence.RunscState = "runsc_container:absent_or_previously_removed"
	err := st.MarkRuntimeResourceAbsentVerified(ctx, RuntimeResourceEvidenceParams{
		GenerationID: instance.GenerationID,
		WorkerID:     "worker-cleanup",
		HostID:       instance.HostID,
		Evidence:     evidence,
		Now:          now.Add(3 * time.Second),
	})
	if err == nil || !strings.Contains(err.Error(), "independently verified") {
		t.Fatalf("expected synthetic evidence rejection, got %v", err)
	}
}

func TestRuntimeResourceAbsentVerifiedRequiresIdentityFilesystemEvidence(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	now := time.Now().UTC()
	params := runtimeResourceInstanceParamsForTest(t, ctx, st, owner.UUID, "sess_resource_network_hosts_evidence", "host-1", now)
	params.NetworkHostsPath = filepath.Join(filepath.Dir(filepath.Dir(params.ControlDirPath)), "network", "gen-"+params.GenerationID, "hosts")
	instance, err := st.CreateRuntimeResourceInstance(ctx, params)
	if err != nil {
		t.Fatalf("create runtime resource instance: %v", err)
	}
	if err := st.ClaimRuntimeResourceRetiring(ctx, RuntimeResourceRetireParams{
		GenerationID: instance.GenerationID,
		WorkerID:     "worker-cleanup",
		HostID:       instance.HostID,
		Now:          now.Add(time.Second),
	}); err != nil {
		t.Fatalf("retire resource: %v", err)
	}
	evidence := runtimeResourceEvidenceForTest(instance)
	if err := st.MarkRuntimeResourceReconciling(ctx, RuntimeResourceEvidenceParams{
		GenerationID: instance.GenerationID,
		WorkerID:     "worker-cleanup",
		HostID:       instance.HostID,
		Evidence:     evidence,
		Now:          now.Add(2 * time.Second),
	}); err != nil {
		t.Fatalf("mark reconciling: %v", err)
	}
	delete(evidence.FilesystemLstat, "network_hosts:"+instance.NetworkHostsPath)
	err = st.MarkRuntimeResourceAbsentVerified(ctx, RuntimeResourceEvidenceParams{
		GenerationID: instance.GenerationID,
		WorkerID:     "worker-cleanup",
		HostID:       instance.HostID,
		Evidence:     evidence,
		Now:          now.Add(3 * time.Second),
	})
	if err == nil || !strings.Contains(err.Error(), "network_hosts") {
		t.Fatalf("expected network_hosts evidence rejection, got %v", err)
	}
}

func TestCreateRuntimeResourceInstanceRequiresExplicitSandboxContractVersion(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	now := time.Now().UTC()
	params := runtimeResourceInstanceParamsForTest(t, ctx, st, owner.UUID, "sess_resource_required_contract_version", "host-1", now)
	params.SandboxContractVersion = " "

	_, err := st.CreateRuntimeResourceInstance(ctx, params)
	if err == nil || !strings.Contains(err.Error(), "runtime resource sandbox contract version is required") {
		t.Fatalf("CreateRuntimeResourceInstance err=%v, want sandbox contract version required", err)
	}
}

func TestRuntimeResourceIdentityForParamsRequiresExplicitSandboxContractVersion(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	now := time.Now().UTC()
	params := runtimeResourceInstanceParamsForTest(t, ctx, st, owner.UUID, "sess_resource_identity_required_contract_version", "host-1", now)
	params.SandboxContractVersion = " "

	_, _, err := RuntimeResourceIdentityForParams(params)
	if err == nil || !strings.Contains(err.Error(), "runtime resource sandbox contract version is required") {
		t.Fatalf("RuntimeResourceIdentityForParams err=%v, want sandbox contract version required", err)
	}
}

func TestRuntimeResourceIdentityForParamsRejectsNonCanonicalPaths(t *testing.T) {
	tests := []struct {
		name string
		edit func(*RuntimeResourceInstanceParams)
		want string
	}{
		{
			name: "relative runsc binary path",
			edit: func(p *RuntimeResourceInstanceParams) { p.RunscBinaryPath = "runsc" },
			want: "runtime resource runsc binary path must be canonical absolute",
		},
		{
			name: "unclean netns path",
			edit: func(p *RuntimeResourceInstanceParams) { p.NetnsPath = "/var/run/netns/../netns/harness-gen-1" },
			want: "runtime resource netns path must be canonical absolute",
		},
		{
			name: "relative control dir",
			edit: func(p *RuntimeResourceInstanceParams) { p.ControlDirPath = "control/gen-1" },
			want: "runtime resource control dir path must be canonical absolute",
		},
		{
			name: "unclean control manifest",
			edit: func(p *RuntimeResourceInstanceParams) {
				p.ControlManifestPath = "/var/lib/harness/run/control/gen-1/../gen-1/session.json"
			},
			want: "runtime resource control manifest path must be canonical absolute",
		},
		{
			name: "relative bundle dir",
			edit: func(p *RuntimeResourceInstanceParams) { p.BundleDirPath = "runtime/gen-1" },
			want: "runtime resource bundle dir path must be canonical absolute",
		},
		{
			name: "unclean spec path",
			edit: func(p *RuntimeResourceInstanceParams) {
				p.SpecPath = "/var/lib/harness/run/runtime/gen-1/../gen-1/config.json"
			},
			want: "runtime resource spec path must be canonical absolute",
		},
		{
			name: "relative checkpoint path",
			edit: func(p *RuntimeResourceInstanceParams) { p.CheckpointPath = "checkpoint" },
			want: "runtime resource checkpoint path must be canonical absolute",
		},
		{
			name: "relative bridge dir",
			edit: func(p *RuntimeResourceInstanceParams) { p.BridgeDirPath = "bridge/gen-1" },
			want: "runtime resource bridge dir path must be canonical absolute",
		},
		{
			name: "whitespace network hosts path",
			edit: func(p *RuntimeResourceInstanceParams) {
				p.NetworkHostsPath = " /var/lib/harness/run/network/gen-1/hosts"
			},
			want: "runtime resource network hosts path must be canonical absolute",
		},
		{
			name: "unclean log dir",
			edit: func(p *RuntimeResourceInstanceParams) { p.LogDirPath = "/var/lib/harness/run/logs/../logs/gen-1" },
			want: "runtime resource log dir path must be canonical absolute",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			st, owner := openOwnedStore(t, ctx)
			now := time.Now().UTC()
			sessionID := "sess_resource_paths_" + strings.ReplaceAll(tt.name, " ", "_")
			params := runtimeResourceInstanceParamsForTest(t, ctx, st, owner.UUID, sessionID, "host-1", now)
			tt.edit(&params)

			_, _, err := RuntimeResourceIdentityForParams(params)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("RuntimeResourceIdentityForParams err=%v, want %q", err, tt.want)
			}
		})
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

func runtimeResourceInCleanupStateForTest(t *testing.T, ctx context.Context, st *Store, ownerUUID, sessionID string, now time.Time, state RuntimeResourceState, workerID string) (RuntimeResourceInstance, ResourceReconciliationEvidence) {
	t.Helper()
	instance := createRuntimeResourceInstanceForTest(t, ctx, st, ownerUUID, sessionID, "host-1", now)
	evidence := runtimeResourceEvidenceForTest(instance)
	if err := st.ClaimRuntimeResourceMaterialization(ctx, RuntimeResourceMaterializationClaimParams{
		GenerationID:     instance.GenerationID,
		WorkerID:         workerID,
		HostID:           instance.HostID,
		LeaseExpiresAt:   now.Add(time.Minute),
		IdempotencyToken: "idem-cleanup-worker-test",
		Now:              now.Add(time.Second),
	}); err != nil {
		t.Fatalf("claim materialization: %v", err)
	}
	if state == RuntimeResourceMaterializing {
		return getRuntimeResourceInstanceForTest(t, ctx, st, instance.GenerationID), evidence
	}
	if err := st.ClaimRuntimeResourceRetiring(ctx, RuntimeResourceRetireParams{
		GenerationID: instance.GenerationID,
		WorkerID:     workerID,
		HostID:       instance.HostID,
		Now:          now.Add(2 * time.Second),
	}); err != nil {
		t.Fatalf("claim retiring: %v", err)
	}
	if state == RuntimeResourceRetiring {
		return getRuntimeResourceInstanceForTest(t, ctx, st, instance.GenerationID), evidence
	}
	if err := st.MarkRuntimeResourceReconciling(ctx, RuntimeResourceEvidenceParams{
		GenerationID: instance.GenerationID,
		WorkerID:     workerID,
		HostID:       instance.HostID,
		Evidence:     evidence,
		Now:          now.Add(3 * time.Second),
	}); err != nil {
		t.Fatalf("mark reconciling: %v", err)
	}
	if state == RuntimeResourceReconciling {
		return getRuntimeResourceInstanceForTest(t, ctx, st, instance.GenerationID), evidence
	}
	if err := st.MarkRuntimeResourceAbsentVerified(ctx, RuntimeResourceEvidenceParams{
		GenerationID: instance.GenerationID,
		WorkerID:     workerID,
		HostID:       instance.HostID,
		Evidence:     evidence,
		Now:          now.Add(4 * time.Second),
	}); err != nil {
		t.Fatalf("mark absent verified: %v", err)
	}
	if state == RuntimeResourceAbsentVerified {
		return getRuntimeResourceInstanceForTest(t, ctx, st, instance.GenerationID), evidence
	}
	t.Fatalf("unsupported cleanup test state %s", state)
	return RuntimeResourceInstance{}, ResourceReconciliationEvidence{}
}

func getRuntimeResourceInstanceForTest(t *testing.T, ctx context.Context, st *Store, generationID string) RuntimeResourceInstance {
	t.Helper()
	instance, err := st.GetRuntimeResourceInstance(ctx, generationID)
	if err != nil {
		t.Fatalf("get runtime resource instance: %v", err)
	}
	return instance
}

func requireRuntimeResourceWorkerIDErrorForTest(t *testing.T, err error) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), "worker id") {
		t.Fatalf("expected worker id validation error, got %v", err)
	}
}

func assertRuntimeResourceUnchangedForTest(t *testing.T, ctx context.Context, st *Store, before RuntimeResourceInstance) {
	t.Helper()
	after := getRuntimeResourceInstanceForTest(t, ctx, st, before.GenerationID)
	if after.State != before.State || after.WorkerID != before.WorkerID || !after.UpdatedAt.Equal(before.UpdatedAt) {
		t.Fatalf("runtime resource mutated: before state=%s worker=%q updated_at=%s; after state=%s worker=%q updated_at=%s",
			before.State, before.WorkerID, before.UpdatedAt.Format(time.RFC3339Nano),
			after.State, after.WorkerID, after.UpdatedAt.Format(time.RFC3339Nano))
	}
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
		ContractID:             contractID,
		SessionID:              sessionID,
		GenerationID:           allocation.GenerationID,
		SandboxContractVersion: SandboxContractVersion,
		ContractSchemaVersion:  SandboxContractSchemaVersion,
		ContractGateVersion:    SandboxContractGateDriverManifest,
		Payload:                testSandboxContractPayload(t, sessionID, allocation),
		Now:                    now.Add(time.Millisecond),
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

func runtimeResourceEvidenceForTest(instance RuntimeResourceInstance) ResourceReconciliationEvidence {
	evidence := ResourceReconciliationEvidence{
		HostID:     instance.HostID,
		RunscState: "absent",
		IPNetns:    "absent",
		IPLink:     "absent",
		NFT:        "absent",
		FilesystemLstat: map[string]string{
			"checkpoint:" + instance.CheckpointPath:            "lstat:absent",
			"control:" + instance.ControlDirPath:               "lstat:absent",
			"control_manifest:" + instance.ControlManifestPath: "lstat:absent",
			"bundle:" + instance.BundleDirPath:                 "lstat:absent",
			"spec:" + instance.SpecPath:                        "lstat:absent",
			"bridge:" + instance.BridgeDirPath:                 "lstat:absent",
			"log:" + instance.LogDirPath:                       "lstat:absent",
		},
	}
	if strings.TrimSpace(instance.NetworkHostsPath) != "" {
		evidence.FilesystemLstat["network:"+filepath.Dir(instance.NetworkHostsPath)] = "lstat:absent"
		evidence.FilesystemLstat["network_hosts:"+instance.NetworkHostsPath] = "lstat:absent"
	}
	return evidence
}

func runtimeResourcePostStartProofForTest(instance RuntimeResourceInstance) *RuntimeResourcePostStartProof {
	return &RuntimeResourcePostStartProof{
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
