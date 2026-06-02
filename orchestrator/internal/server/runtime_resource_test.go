package server

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/events"
	"harness-platform/orchestrator/internal/runtime"
	"harness-platform/orchestrator/internal/sessionstate"
	"harness-platform/orchestrator/internal/store"
)

func TestRuntimeResourceHostIDFailsClosed(t *testing.T) {
	if _, err := runtimeResourceHostIDFrom(func() (string, error) { return " ", nil }); err == nil || !strings.Contains(err.Error(), "host id is required") {
		t.Fatalf("expected empty hostname error, got %v", err)
	}

	boom := errors.New("hostname failed")
	if _, err := runtimeResourceHostIDFrom(func() (string, error) { return "", boom }); !errors.Is(err, boom) {
		t.Fatalf("expected hostname error, got %v", err)
	}
}

func TestRuntimeResourceNftTableNameRequiresIdentifier(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "empty", value: ""},
		{name: "whitespace only", value: " \t\n"},
		{name: "all invalid", value: "!!!"},
		{name: "underscore only", value: "___"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := runtimeResourceNftTableName(tc.value); err == nil || !strings.Contains(err.Error(), "identifier is required") {
				t.Fatalf("expected generation id error, got %v", err)
			}
		})
	}
}

func TestRuntimeResourcePostStartProofValidatesRuntimeIdentity(t *testing.T) {
	instance := store.RuntimeResourceInstance{
		GenerationID:           "gen_post_start",
		HostID:                 "host-post-start",
		ContractID:             "contract_gen_post_start",
		SandboxContractVersion: store.SandboxContractVersion,
		RunscContainerID:       "harness-gen-post-start",
		RunscPlatform:          "systrap",
		RunscVersion:           "runsc test",
		RunscBinaryPath:        "/usr/local/bin/runsc-test",
		RunscBinaryDigest:      "sha256:runsc-test",
	}
	proof := serverPostStartProofForTest(instance)
	proof.HostID = ""
	proof.ContractID = ""
	proof.SandboxContractVersion = ""

	verified, err := runtimeResourcePostStartProof(instance, runtime.Result{PostStartProof: proof}, "bridge_startup_probe:passed; check=test")
	if err != nil {
		t.Fatalf("validate post-start proof: %v", err)
	}
	if verified.HostID != instance.HostID ||
		verified.ContractID != instance.ContractID ||
		verified.SandboxContractVersion != instance.SandboxContractVersion {
		t.Fatalf("server-owned proof fields were not filled from instance: %+v", verified)
	}

	mismatch := *serverPostStartProofForTest(instance)
	mismatch.RunscBinaryDigest = "sha256:changed"
	if _, err := runtimeResourcePostStartProof(instance, runtime.Result{PostStartProof: &mismatch}, "bridge_startup_probe:passed; check=test"); err == nil ||
		!strings.Contains(err.Error(), "runtime post-start proof runsc_binary_digest") {
		t.Fatalf("expected runsc digest mismatch, got %v", err)
	}
}

func TestRuntimeResourceInstanceCheckpointRestoreTransitions(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_resource_checkpoint_restore", string(sessionstate.Created), time.Now().UTC(), nil)
	enableSessionAutoCheckpoint(t, ctx, st, session.ID)
	cfg := testServerConfig(dir)
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, session.DriverID),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	rt := &recordingRuntime{}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	if err := srv.startEnsuredGeneration(ctx, session, ensuredGeneration{Allocation: allocation, IsNew: true}, startFailureInputAcceptable); err != nil {
		t.Fatalf("start ensured generation: %v", err)
	}
	if err := st.UpdateSessionStatusAndActivity(ctx, session.ID, string(sessionstate.RunningIdle), nil, time.Now().UTC()); err != nil {
		t.Fatalf("mark session idle: %v", err)
	}
	if err := srv.checkpointGeneration(ctx, store.CheckpointCandidate{
		SessionID:    session.ID,
		GenerationID: allocation.GenerationID,
	}, allocation.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("checkpoint generation: %v", err)
	}
	instance, err := st.GetRuntimeResourceInstance(ctx, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get checkpointed runtime resource: %v", err)
	}
	if instance.State != store.RuntimeResourceCheckpointReserved {
		t.Fatalf("runtime resource after checkpoint=%s want %s", instance.State, store.RuntimeResourceCheckpointReserved)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"after checkpoint"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected restore status 202, got %d body %s", rec.Code, rec.Body.String())
	}
	instance, err = st.GetRuntimeResourceInstance(ctx, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get restored runtime resource: %v", err)
	}
	if instance.State != store.RuntimeResourceLive ||
		instance.WorkerID != owner.UUID ||
		instance.IdempotencyToken != "" ||
		instance.LeaseExpiresAt != nil {
		t.Fatalf("unexpected runtime resource after restore: %+v", instance)
	}
	_, starts := rt.requests()
	if len(starts) != 2 || !starts[1].RestoreFromCheckpoint {
		t.Fatalf("expected second start to restore checkpoint, got %+v", starts)
	}
	if starts[1].Generation.RunscContainerID != instance.RunscContainerID ||
		starts[1].Generation.RunscBinaryDigest != instance.RunscBinaryDigest ||
		starts[1].Generation.NetnsName != instance.NetnsName {
		t.Fatalf("restore start did not use runtime resource identity: start=%+v instance=%+v", starts[1].Generation, instance)
	}
	volumeConfig, err := srv.dataVolumeProvisionerConfig()
	if err != nil {
		t.Fatalf("data volume config: %v", err)
	}
	workspaceVolume, err := st.VerifySessionWorkspaceVolume(ctx, store.VerifySessionWorkspaceVolumeParams{
		SessionID: session.ID,
		Config:    volumeConfig,
	})
	if err != nil {
		t.Fatalf("verify workspace volume: %v", err)
	}
	driverHomeVolume, err := st.VerifySessionDriverHomeVolume(ctx, store.VerifySessionDriverHomeVolumeParams{
		SessionID: session.ID,
		Driver:    session.DriverID,
		Config:    volumeConfig,
	})
	if err != nil {
		t.Fatalf("verify driver home volume: %v", err)
	}
	if starts[1].WorkspaceHostPath != workspaceVolume.HostPath ||
		starts[1].AgentHomeHostPath != driverHomeVolume.HostPath {
		t.Fatalf("restore start did not use data volume paths: start=%+v workspace=%+v home=%+v", starts[1], workspaceVolume, driverHomeVolume)
	}
}

func TestDestroyReclaimableGenerationResourcesMarksDestroyedOnlyAfterRuntimeCleanup(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()

	for _, tc := range []struct {
		name       string
		destroyErr error
		wantState  string
	}{
		{name: "cleanup succeeds", wantState: "destroyed"},
		{name: "cleanup fails", destroyErr: errors.New("netns busy"), wantState: "reclaimable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			st, owner := openServerOwnedStore(t, ctx, dir)
			cfg := testServerConfig(dir)
			createServerTestSession(t, ctx, st, dir, "sess_cleanup", string(sessionstate.Created), now.Add(-time.Minute), nil)
			allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
				SessionID: "sess_cleanup",
				Owner:     store.GenerationLeaseOwner(owner.UUID),
				LeaseTTL:  time.Minute,
				Now:       now.Add(-time.Minute),
				Config:    serverTestAllocatorConfig(cfg, "claude_code"),
			})
			if err != nil {
				t.Fatalf("allocate generation: %v", err)
			}
			if err := st.MarkGenerationResourcesLive(ctx, "sess_cleanup", allocation.GenerationID, allocation.Owner, now.Add(-59*time.Second)); err != nil {
				t.Fatalf("mark resources live: %v", err)
			}
			createServerRuntimeResourceLive(t, ctx, st, "sess_cleanup", allocation, owner.UUID, mustRuntimeResourceHostID(t), now.Add(-59*time.Second+time.Millisecond))
			if err := st.FailGeneration(ctx, store.FailGenerationParams{
				SessionID:    "sess_cleanup",
				GenerationID: allocation.GenerationID,
				Owner:        allocation.Owner,
				ErrorClass:   "probe_failed_pre_start",
				Reason:       "probe failed",
				Now:          now.Add(-58 * time.Second),
			}); err != nil {
				t.Fatalf("fail generation: %v", err)
			}

			rt := &recordingRuntime{destroyErr: tc.destroyErr}
			srv := &Server{
				cfg:     cfg,
				store:   st,
				runtime: rt,
				watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
				hub:     events.NewHub(),
				log:     slog.Default(),
			}
			srv.destroyReclaimableGenerationResources(ctx, now)

			calls := rt.destroyGenerationRequests()
			if len(calls) != 1 || calls[0].GenerationID != allocation.GenerationID {
				t.Fatalf("destroy generation calls=%+v", calls)
			}
			var networkState, resourceState string
			if err := st.DBForTest().QueryRowContext(ctx, `
SELECT n.allocation_state, r.resource_state
FROM network_profiles n
JOIN runtime_generation_resources r ON r.generation_id = n.generation_id
WHERE n.generation_id = ?`, allocation.GenerationID).Scan(&networkState, &resourceState); err != nil {
				t.Fatalf("query resource states: %v", err)
			}
			if networkState != tc.wantState || resourceState != tc.wantState {
				t.Fatalf("unexpected states after cleanup: network=%s resource=%s want %s", networkState, resourceState, tc.wantState)
			}
			if tc.destroyErr == nil {
				instance, err := st.GetRuntimeResourceInstance(ctx, allocation.GenerationID)
				if err != nil {
					t.Fatalf("get cleaned runtime resource instance: %v", err)
				}
				if instance.State != store.RuntimeResourceDestroyed || len(instance.EvidenceJSON) == 0 || instance.EvidenceDigest == "" || instance.VerifiedAt == nil {
					t.Fatalf("runtime resource cleanup evidence not completed: %+v", instance)
				}
			}
		})
	}
}

func TestCleanupGenerationResourcesRequiresRuntimeResourceInstance(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	cfg := testServerConfig(dir)
	createServerTestSession(t, ctx, st, dir, "sess_cleanup_missing_instance", string(sessionstate.Created), now.Add(-time.Minute), nil)
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: "sess_cleanup_missing_instance",
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now.Add(-time.Minute),
		Config:    serverTestAllocatorConfig(cfg, "claude_code"),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, "sess_cleanup_missing_instance", allocation.GenerationID, allocation.Owner, now.Add(-59*time.Second)); err != nil {
		t.Fatalf("mark resources live: %v", err)
	}
	if err := st.FailGeneration(ctx, store.FailGenerationParams{
		SessionID:    "sess_cleanup_missing_instance",
		GenerationID: allocation.GenerationID,
		Owner:        allocation.Owner,
		ErrorClass:   "probe_failed_pre_start",
		Reason:       "probe failed",
		Now:          now.Add(-58 * time.Second),
	}); err != nil {
		t.Fatalf("fail generation: %v", err)
	}
	rt := &recordingRuntime{}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}

	err = srv.cleanupGenerationResources(ctx, "sess_cleanup_missing_instance", allocation.GenerationID, now)
	if err == nil || !strings.Contains(err.Error(), "runtime resource instance is required for generation cleanup") {
		t.Fatalf("expected missing runtime resource invariant failure, got %v", err)
	}
	if calls := rt.destroyGenerationRequests(); len(calls) != 0 {
		t.Fatalf("cleanup should not fall back to legacy resource details, calls=%+v", calls)
	}
}

func TestDestroyReclaimableGenerationResourcesRemovesFilesystemWithRealRuntime(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	cfg := testServerConfig(dir)
	createServerTestSession(t, ctx, st, dir, "sess_cleanup_real", string(sessionstate.Created), now.Add(-time.Minute), nil)
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: "sess_cleanup_real",
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now.Add(-time.Minute),
		Config:    serverTestAllocatorConfig(cfg, "claude_code"),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, "sess_cleanup_real", allocation.GenerationID, allocation.Owner, now.Add(-59*time.Second)); err != nil {
		t.Fatalf("mark resources live: %v", err)
	}
	createServerRuntimeResourceLive(t, ctx, st, "sess_cleanup_real", allocation, owner.UUID, mustRuntimeResourceHostID(t), now.Add(-59*time.Second+time.Millisecond))
	if err := st.FailGeneration(ctx, store.FailGenerationParams{
		SessionID:    "sess_cleanup_real",
		GenerationID: allocation.GenerationID,
		Owner:        allocation.Owner,
		ErrorClass:   "probe_failed_pre_start",
		Reason:       "probe failed",
		Now:          now.Add(-58 * time.Second),
	}); err != nil {
		t.Fatalf("fail generation: %v", err)
	}
	details, err := st.GetRuntimeGenerationDetails(ctx, "sess_cleanup_real", allocation.GenerationID)
	if err != nil {
		t.Fatalf("get runtime generation details: %v", err)
	}
	createServerGenerationFilesystem(t, details)
	currentRunscBinary, _ := currentRunscBinaryMetadataForServerTest(t)

	realRuntime := runtime.New(runtime.Config{
		RunscNetwork:  "sandbox",
		RunscOverlay2: "none",
		RunscRoot:     filepath.Join(dir, "runsc-root"),
		RunDir:        cfg.Harness.RunDir,
		CommandRunner: serverCommandRunner{
			outputs: map[string][]byte{
				"runsc --version": []byte("runsc test"),
			},
			fail: map[string]error{
				currentRunscBinary + " -root " + filepath.Join(dir, "runsc-root") + " state " + details.RunscContainerID: errors.New("not found"),
				"ip link show " + details.HostVeth:                                                errors.New("does not exist"),
				"nft list table inet " + mustRuntimeResourceNftTableName(t, details.GenerationID): errors.New("No such table"),
			},
		},
	})
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: realRuntime,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.destroyReclaimableGenerationResources(ctx, now)

	for _, path := range []string{details.CheckpointPath, details.ControlDirPath, details.BundleDirPath, details.BridgeDirPath, details.LogDirPath} {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatalf("expected cleanup path %s to be removed, stat err=%v", path, err)
		}
	}
	var networkState, resourceState string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT n.allocation_state, r.resource_state
FROM network_profiles n
JOIN runtime_generation_resources r ON r.generation_id = n.generation_id
WHERE n.generation_id = ?`, allocation.GenerationID).Scan(&networkState, &resourceState); err != nil {
		t.Fatalf("query resource states: %v", err)
	}
	if networkState != "destroyed" || resourceState != "destroyed" {
		t.Fatalf("unexpected states after real runtime cleanup: network=%s resource=%s", networkState, resourceState)
	}
}

func TestReserveRuntimeResourceCheckpointRequiresInstance(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_checkpoint_missing_instance", string(sessionstate.Created), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	now := time.Now().UTC()
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    serverTestAllocatorConfig(cfg, "claude_code"),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, allocation.GenerationID, allocation.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark resources live: %v", err)
	}
	srv := &Server{store: st}

	err = srv.reserveRuntimeResourceCheckpoint(ctx, allocation.GenerationID)
	if err == nil || !strings.Contains(err.Error(), "runtime resource instance is required for checkpoint reserve") {
		t.Fatalf("expected missing runtime resource invariant failure, got %v", err)
	}
}

func TestStartEnsuredGenerationDestroysRuntimeAfterOwnerLoss(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_start_owner_loss", string(sessionstate.Created), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, session.DriverID),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	rt := &startHookRuntime{
		onStart: func(req runtime.StartRequest) {
			if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generations
SET lease_owner = 'other_owner',
    lease_expires_at = ?
WHERE generation_id = ?`, time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano), req.GenerationID); err != nil {
				t.Fatalf("steal generation lease: %v", err)
			}
		},
	}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}

	err = srv.startEnsuredGeneration(ctx, session, ensuredGeneration{
		Allocation: allocation,
		IsNew:      true,
	}, startFailureInputAcceptable)
	if !errors.Is(err, errGenerationStartLeaseLost) {
		t.Fatalf("expected start lease loss, got %v", err)
	}
	runscID := serverRunscContainerID(t, ctx, st, session.ID, allocation.GenerationID)
	if got := rt.runtimeDestroyRequests(); len(got) != 1 || got[0] != runscID {
		t.Fatalf("owner loss should destroy started runtime %q, got %+v", runscID, got)
	}
	var status, ownerValue, errorClass, networkState, resourceState string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, COALESCE(g.lease_owner, ''), COALESCE(g.error_class, ''), n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&status, &ownerValue, &errorClass, &networkState, &resourceState); err != nil {
		t.Fatalf("query generation after owner loss: %v", err)
	}
	if status != "starting" ||
		ownerValue != "other_owner" ||
		errorClass != "" ||
		networkState != "allocating" ||
		resourceState != "allocating" {
		t.Fatalf("owner loss should not fail or reclaim the stolen generation: status=%s owner=%q class=%q network=%s resource=%s", status, ownerValue, errorClass, networkState, resourceState)
	}
	instance, err := st.GetRuntimeResourceInstance(ctx, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get runtime resource instance after owner loss: %v", err)
	}
	if instance.State != store.RuntimeResourceRetiring {
		t.Fatalf("runtime resource after owner loss=%s want %s", instance.State, store.RuntimeResourceRetiring)
	}
	var runtimeEvents int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM events
WHERE session_id = ?
  AND type = 'generation.error'`, session.ID).Scan(&runtimeEvents); err != nil {
		t.Fatalf("count generation events: %v", err)
	}
	if runtimeEvents != 0 {
		t.Fatalf("owner loss should not publish generation error events, got %d", runtimeEvents)
	}
}
