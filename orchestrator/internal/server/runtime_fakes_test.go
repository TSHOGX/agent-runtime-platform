package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"harness-platform/orchestrator/internal/bridge"
	"harness-platform/orchestrator/internal/generationplan"
	"harness-platform/orchestrator/internal/runtime"
	"harness-platform/orchestrator/internal/store"
)

const checkpointImageManifestDigestForTest = "sha256:checkpoint-image-manifest"

type instantRuntime struct{}

var instantRuntimePrepareCalls int64

func (instantRuntime) PrepareGeneration(context.Context, runtime.StartRequest) (runtime.GenerationArtifacts, error) {
	atomic.AddInt64(&instantRuntimePrepareCalls, 1)
	return testGenerationArtifacts(), nil
}

func (instantRuntime) RenderGenerationArtifacts(context.Context, runtime.StartRequest) (runtime.GenerationArtifactProjection, error) {
	atomic.AddInt64(&instantRuntimePrepareCalls, 1)
	return runtime.GenerationArtifactProjection{Artifacts: testGenerationArtifacts()}, nil
}

func (instantRuntime) MaterializeGenerationArtifacts(runtime.StartRequest, runtime.GenerationArtifactProjection) error {
	return nil
}

func (instantRuntime) PrepareGenerationNetwork(context.Context, runtime.StartRequest) error {
	return nil
}

func (instantRuntime) Start(ctx context.Context, req runtime.StartRequest, output func(runtime.Output)) runtime.Result {
	if output != nil {
		output(runtime.Output{Stream: "stdout", Line: `{"type":"result","subtype":"success","result":"ok"}`})
	}
	return serverRuntimeStartResult(req)
}

func (instantRuntime) Destroy(context.Context, string) error {
	return nil
}

func (instantRuntime) DestroyGenerationResources(_ context.Context, details store.RuntimeGenerationDetails) (runtime.GenerationResourceCleanup, error) {
	return runtimeCleanupEvidenceForDetails(details), nil
}

func (instantRuntime) Interrupt(string) error {
	return nil
}

func (instantRuntime) Checkpoint(context.Context, runtime.CheckpointRequest) (runtime.CheckpointResult, error) {
	return runtime.CheckpointResult{ImageManifestDigest: checkpointImageManifestDigestForTest}, nil
}

type recordingRuntime struct {
	mu                  sync.Mutex
	prepareRequests     []runtime.StartRequest
	materializeRequests []runtime.StartRequest
	networkRequests     []runtime.StartRequest
	startRequests       []runtime.StartRequest
	destroyRuntimeIDs   []string
	destroyRuntimeErr   error
	destroyRequests     []store.RuntimeGenerationDetails
	destroyErr          error
	checkpointReqs      []runtime.CheckpointRequest
	checkpointErr       error
	interruptSessionIDs []string
}

type planOrderRuntime struct {
	recordingRuntime
	store                                   *store.Store
	t                                       *testing.T
	planSeenBeforeNetwork                   bool
	planSeenBeforeMaterializeRender         bool
	planSeenBeforeMaterialize               bool
	projectionVerificationObserved          bool
	projectionVerificationBeforeMaterialize bool
	runtimeResourceClaimedBeforeNetwork     bool
	runtimeResourceClaimedBeforeMaterialize bool
}

type corruptProjectionBeforeMaterializeRuntime struct {
	recordingRuntime
	store        *store.Store
	t            *testing.T
	corrupted    bool
	materialized bool
}

func (r *planOrderRuntime) RenderGenerationArtifacts(ctx context.Context, req runtime.StartRequest) (runtime.GenerationArtifactProjection, error) {
	r.t.Helper()
	projection, err := r.recordingRuntime.RenderGenerationArtifacts(ctx, req)
	if err != nil {
		return runtime.GenerationArtifactProjection{}, err
	}
	plan, err := r.store.GetGenerationPlan(ctx, req.GenerationID)
	if errors.Is(err, sql.ErrNoRows) {
		return projection, nil
	}
	if err != nil {
		return runtime.GenerationArtifactProjection{}, fmt.Errorf("get generation plan before materialize render: %w", err)
	}
	planArtifacts, err := generationplan.RuntimeArtifacts(plan.CanonicalPayload)
	if err != nil {
		return runtime.GenerationArtifactProjection{}, fmt.Errorf("read generation plan runtime artifacts before materialize render: %w", err)
	}
	if reflect.DeepEqual(req.PreparedArtifacts, planArtifacts) {
		r.planSeenBeforeMaterializeRender = true
	}
	return projection, nil
}

func (r *corruptProjectionBeforeMaterializeRuntime) RenderGenerationArtifacts(ctx context.Context, req runtime.StartRequest) (runtime.GenerationArtifactProjection, error) {
	r.t.Helper()
	projection, err := r.recordingRuntime.RenderGenerationArtifacts(ctx, req)
	if err != nil {
		return runtime.GenerationArtifactProjection{}, err
	}
	plan, err := r.store.GetGenerationPlan(ctx, req.GenerationID)
	if errors.Is(err, sql.ErrNoRows) {
		return projection, nil
	}
	if err != nil {
		return runtime.GenerationArtifactProjection{}, fmt.Errorf("get generation plan before corrupting projection: %w", err)
	}
	planArtifacts, err := generationplan.RuntimeArtifacts(plan.CanonicalPayload)
	if err != nil {
		return runtime.GenerationArtifactProjection{}, fmt.Errorf("read generation plan runtime artifacts before corrupting projection: %w", err)
	}
	if r.corrupted || !reflect.DeepEqual(req.PreparedArtifacts, planArtifacts) {
		return projection, nil
	}
	if _, err := r.store.DBForTest().ExecContext(ctx, `
UPDATE generation_plan_projections
SET payload_digest = 'sha256:changed-before-materialize'
WHERE generation_id = ?
  AND projection_kind = ?`, req.GenerationID, store.GenerationPlanProjectionBundle); err != nil {
		return runtime.GenerationArtifactProjection{}, fmt.Errorf("corrupt generation plan projection before materialize: %w", err)
	}
	r.corrupted = true
	return projection, nil
}

func (r *corruptProjectionBeforeMaterializeRuntime) MaterializeGenerationArtifacts(req runtime.StartRequest, projection runtime.GenerationArtifactProjection) error {
	r.materialized = true
	return r.recordingRuntime.MaterializeGenerationArtifacts(req, projection)
}

type blockingPrepareRuntime struct {
	recordingRuntime
	prepareStarted chan struct{}
	releasePrepare chan struct{}
	startedOnce    sync.Once
	releaseOnce    sync.Once
}

func newBlockingPrepareRuntime() *blockingPrepareRuntime {
	return &blockingPrepareRuntime{
		prepareStarted: make(chan struct{}),
		releasePrepare: make(chan struct{}),
	}
}

func (r *blockingPrepareRuntime) RenderGenerationArtifacts(ctx context.Context, req runtime.StartRequest) (runtime.GenerationArtifactProjection, error) {
	r.mu.Lock()
	r.prepareRequests = append(r.prepareRequests, req)
	r.mu.Unlock()
	r.startedOnce.Do(func() { close(r.prepareStarted) })
	select {
	case <-r.releasePrepare:
		return runtime.GenerationArtifactProjection{Artifacts: testGenerationArtifacts()}, nil
	case <-ctx.Done():
		return runtime.GenerationArtifactProjection{}, ctx.Err()
	}
}

func (r *blockingPrepareRuntime) PrepareGeneration(ctx context.Context, req runtime.StartRequest) (runtime.GenerationArtifacts, error) {
	projection, err := r.RenderGenerationArtifacts(ctx, req)
	return projection.Artifacts, err
}

func (r *blockingPrepareRuntime) release() {
	r.releaseOnce.Do(func() { close(r.releasePrepare) })
}

type startHookRuntime struct {
	recordingRuntime
	onStart func(runtime.StartRequest)
}

func (r *startHookRuntime) Start(_ context.Context, req runtime.StartRequest, _ func(runtime.Output)) runtime.Result {
	r.mu.Lock()
	r.startRequests = append(r.startRequests, req)
	r.mu.Unlock()
	if r.onStart != nil {
		r.onStart(req)
	}
	return serverRuntimeStartResult(req)
}

type claimAfterProbeRuntime struct {
	recordingRuntime
}

func (r *claimAfterProbeRuntime) Start(_ context.Context, req runtime.StartRequest, _ func(runtime.Output)) runtime.Result {
	r.mu.Lock()
	r.startRequests = append(r.startRequests, req)
	r.mu.Unlock()
	result := serverRuntimeStartResult(req)
	if result.Err != nil {
		return result
	}
	outbox, err := bridge.OpenQueue(req.Generation.BridgeDirPath, bridge.OutboxDir)
	if err != nil {
		return runtime.Result{Err: err}
	}
	if _, err := outbox.Write(context.Background(), bridge.Envelope{
		RequestID:    "test_claim_after_probe",
		Type:         bridge.TypeClaimNextTurn,
		SessionID:    req.SessionID,
		GenerationID: req.GenerationID,
	}); err != nil {
		return runtime.Result{Err: err}
	}
	return result
}

func (r *recordingRuntime) PrepareGeneration(_ context.Context, req runtime.StartRequest) (runtime.GenerationArtifacts, error) {
	r.mu.Lock()
	r.prepareRequests = append(r.prepareRequests, req)
	r.mu.Unlock()
	return testGenerationArtifacts(), nil
}

func (r *recordingRuntime) RenderGenerationArtifacts(_ context.Context, req runtime.StartRequest) (runtime.GenerationArtifactProjection, error) {
	r.mu.Lock()
	r.prepareRequests = append(r.prepareRequests, req)
	r.mu.Unlock()
	return runtime.GenerationArtifactProjection{Artifacts: testGenerationArtifacts()}, nil
}

func (r *recordingRuntime) MaterializeGenerationArtifacts(req runtime.StartRequest, _ runtime.GenerationArtifactProjection) error {
	r.mu.Lock()
	r.materializeRequests = append(r.materializeRequests, req)
	r.mu.Unlock()
	return nil
}

func (r *recordingRuntime) PrepareGenerationNetwork(_ context.Context, req runtime.StartRequest) error {
	r.mu.Lock()
	r.networkRequests = append(r.networkRequests, req)
	r.mu.Unlock()
	return nil
}

func (r *planOrderRuntime) MaterializeGenerationArtifacts(req runtime.StartRequest, projection runtime.GenerationArtifactProjection) error {
	r.t.Helper()
	if err := r.recordingRuntime.MaterializeGenerationArtifacts(req, projection); err != nil {
		return err
	}
	if err := r.requireStoredPlanAndMaterializationClaim(context.Background(), req, "materialize"); err != nil {
		return err
	}
	r.planSeenBeforeMaterialize = true
	r.projectionVerificationBeforeMaterialize = true
	r.runtimeResourceClaimedBeforeMaterialize = true
	return nil
}

func (r *planOrderRuntime) PrepareGenerationNetwork(ctx context.Context, req runtime.StartRequest) error {
	r.t.Helper()
	if err := r.recordingRuntime.PrepareGenerationNetwork(ctx, req); err != nil {
		return err
	}
	if err := r.requireStoredPlanAndMaterializationClaim(ctx, req, "network prepare"); err != nil {
		return err
	}
	r.planSeenBeforeNetwork = true
	r.projectionVerificationObserved = true
	r.runtimeResourceClaimedBeforeNetwork = true
	return nil
}

func (r *planOrderRuntime) requireStoredPlanAndMaterializationClaim(ctx context.Context, req runtime.StartRequest, phase string) error {
	plan, err := r.store.GetGenerationPlan(ctx, req.GenerationID)
	if err != nil {
		return fmt.Errorf("get generation plan before %s: %w", phase, err)
	}
	planArtifacts, err := generationplan.RuntimeArtifacts(plan.CanonicalPayload)
	if err != nil {
		return fmt.Errorf("read generation plan runtime artifacts before %s: %w", phase, err)
	}
	if !reflect.DeepEqual(req.PreparedArtifacts, planArtifacts) {
		return fmt.Errorf("prepared artifacts before %s did not come from stored plan: got %+v want %+v", phase, req.PreparedArtifacts, planArtifacts)
	}
	projections, err := r.store.ListGenerationPlanProjections(ctx, req.GenerationID)
	if err != nil {
		return fmt.Errorf("list generation plan projections before %s: %w", phase, err)
	}
	if len(projections) != len(store.GenerationPlanProjectionKinds()) {
		return fmt.Errorf("generation plan projection count before %s = %d want %d", phase, len(projections), len(store.GenerationPlanProjectionKinds()))
	}
	verified, err := r.store.VerifyGenerationPlanProjections(ctx, store.VerifyGenerationPlanProjectionsParams{
		GenerationID: req.GenerationID,
		Expected:     generationPlanProjectionExpectationsForDetails(req.Generation, req.PreparedArtifacts, ""),
	})
	if err != nil {
		return fmt.Errorf("verify generation plan projections before %s: %w", phase, err)
	}
	if !verified {
		return fmt.Errorf("generation plan projections were missing before %s", phase)
	}
	for _, projection := range projections {
		if projection.PlanDigest != plan.PlanDigest {
			return fmt.Errorf("generation plan projection %s digest before %s = %s want %s", projection.ProjectionKind, phase, projection.PlanDigest, plan.PlanDigest)
		}
	}
	instance, err := r.store.GetRuntimeResourceInstance(ctx, req.GenerationID)
	if err != nil {
		return fmt.Errorf("get runtime resource instance before %s: %w", phase, err)
	}
	if instance.State != store.RuntimeResourceMaterializing {
		return fmt.Errorf("runtime resource state before %s = %s want %s", phase, instance.State, store.RuntimeResourceMaterializing)
	}
	if strings.TrimSpace(instance.WorkerID) == "" || strings.TrimSpace(instance.HostID) == "" {
		return fmt.Errorf("runtime resource worker lease was not claimed before %s", phase)
	}
	if instance.LeaseExpiresAt == nil {
		return fmt.Errorf("runtime resource materialization lease is missing before %s", phase)
	}
	if instance.IdempotencyToken != "start:"+req.GenerationID {
		return fmt.Errorf("runtime resource idempotency token before %s = %q want %q", phase, instance.IdempotencyToken, "start:"+req.GenerationID)
	}
	return nil
}

func (r *recordingRuntime) Start(_ context.Context, req runtime.StartRequest, _ func(runtime.Output)) runtime.Result {
	r.mu.Lock()
	r.startRequests = append(r.startRequests, req)
	r.mu.Unlock()
	return serverRuntimeStartResult(req)
}

func (r *recordingRuntime) DestroyGenerationResources(_ context.Context, details store.RuntimeGenerationDetails) (runtime.GenerationResourceCleanup, error) {
	r.mu.Lock()
	r.destroyRequests = append(r.destroyRequests, details)
	err := r.destroyErr
	r.mu.Unlock()
	if err != nil {
		return runtime.GenerationResourceCleanup{}, err
	}
	return runtimeCleanupEvidenceForDetails(details), nil
}

func runtimeCleanupEvidenceForDetails(details store.RuntimeGenerationDetails) runtime.GenerationResourceCleanup {
	filesystem := map[string]string{}
	addFilesystem := func(label, path string) {
		if strings.TrimSpace(path) != "" {
			filesystem[label+":"+path] = "lstat:absent"
		}
	}
	addFilesystem("checkpoint", details.CheckpointPath)
	addFilesystem("control", details.ControlDirPath)
	addFilesystem("control_manifest", details.ControlManifestPath)
	addFilesystem("bundle", details.BundleDirPath)
	addFilesystem("spec", details.SpecPath)
	addFilesystem("bridge", details.BridgeDirPath)
	if strings.TrimSpace(details.NetworkHostsPath) != "" {
		addFilesystem("network", filepath.Dir(details.NetworkHostsPath))
	}
	addFilesystem("network_hosts", details.NetworkHostsPath)
	addFilesystem("log", details.LogDirPath)
	if len(filesystem) == 0 {
		filesystem["test:runtime_resource"] = "lstat:absent"
	}
	return runtime.GenerationResourceCleanup{
		RunscDeleted:      true,
		CheckpointDeleted: true,
		ControlDirDeleted: true,
		BundleDirDeleted:  true,
		BridgeDirDeleted:  true,
		NetworkDirDeleted: true,
		LogDirDeleted:     true,
		NetnsDeleted:      true,
		HostVethDeleted:   true,
		NftTableDeleted:   true,
		RunscState:        "runsc_container:absent; check=test",
		IPNetns:           "netns:absent; check=test",
		IPLink:            "host_veth:absent; check=test",
		NFT:               "nft_table:absent; check=test",
		FilesystemLstat:   filesystem,
	}
}

func (r *recordingRuntime) Destroy(_ context.Context, runtimeID string) error {
	r.mu.Lock()
	r.destroyRuntimeIDs = append(r.destroyRuntimeIDs, runtimeID)
	err := r.destroyRuntimeErr
	r.mu.Unlock()
	return err
}

func (r *recordingRuntime) Interrupt(sessionID string) error {
	r.mu.Lock()
	r.interruptSessionIDs = append(r.interruptSessionIDs, sessionID)
	r.mu.Unlock()
	return nil
}

func (r *recordingRuntime) Checkpoint(_ context.Context, req runtime.CheckpointRequest) (runtime.CheckpointResult, error) {
	r.mu.Lock()
	r.checkpointReqs = append(r.checkpointReqs, req)
	err := r.checkpointErr
	r.mu.Unlock()
	if err != nil {
		return runtime.CheckpointResult{}, err
	}
	return runtime.CheckpointResult{ImageManifestDigest: checkpointImageManifestDigestForTest}, nil
}

func (r *recordingRuntime) requests() ([]runtime.StartRequest, []runtime.StartRequest) {
	r.mu.Lock()
	defer r.mu.Unlock()
	prepares := append([]runtime.StartRequest(nil), r.prepareRequests...)
	starts := append([]runtime.StartRequest(nil), r.startRequests...)
	return prepares, starts
}

func (r *recordingRuntime) networkPrepareRequests() []runtime.StartRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]runtime.StartRequest(nil), r.networkRequests...)
}

func (r *recordingRuntime) checkpointRequests() []runtime.CheckpointRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]runtime.CheckpointRequest(nil), r.checkpointReqs...)
}

func (r *recordingRuntime) destroyGenerationRequests() []store.RuntimeGenerationDetails {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]store.RuntimeGenerationDetails(nil), r.destroyRequests...)
}

func (r *recordingRuntime) runtimeDestroyRequests() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.destroyRuntimeIDs...)
}

type restoreFailingRuntime struct {
	recordingRuntime
	err error
}

func (r *restoreFailingRuntime) Start(_ context.Context, req runtime.StartRequest, _ func(runtime.Output)) runtime.Result {
	r.mu.Lock()
	r.startRequests = append(r.startRequests, req)
	r.mu.Unlock()
	if req.RestoreFromCheckpoint {
		return runtime.Result{Err: r.err}
	}
	return serverRuntimeStartResult(req)
}

type restoreStartHookRuntime struct {
	recordingRuntime
	onRestoreStart func()
}

func (r *restoreStartHookRuntime) Start(_ context.Context, req runtime.StartRequest, _ func(runtime.Output)) runtime.Result {
	r.mu.Lock()
	r.startRequests = append(r.startRequests, req)
	r.mu.Unlock()
	if req.RestoreFromCheckpoint && r.onRestoreStart != nil {
		r.onRestoreStart()
	}
	return serverRuntimeStartResult(req)
}

type restoreValidationRuntime struct {
	restore       *runtime.Runtime
	startRequests []runtime.StartRequest
}

func (r *restoreValidationRuntime) PrepareGeneration(context.Context, runtime.StartRequest) (runtime.GenerationArtifacts, error) {
	return testGenerationArtifacts(), nil
}

func (r *restoreValidationRuntime) RenderGenerationArtifacts(context.Context, runtime.StartRequest) (runtime.GenerationArtifactProjection, error) {
	return runtime.GenerationArtifactProjection{Artifacts: testGenerationArtifacts()}, nil
}

func (r *restoreValidationRuntime) MaterializeGenerationArtifacts(runtime.StartRequest, runtime.GenerationArtifactProjection) error {
	return nil
}

func (r *restoreValidationRuntime) PrepareGenerationNetwork(context.Context, runtime.StartRequest) error {
	return nil
}

func (r *restoreValidationRuntime) Start(ctx context.Context, req runtime.StartRequest, output func(runtime.Output)) runtime.Result {
	r.startRequests = append(r.startRequests, req)
	if req.RestoreFromCheckpoint {
		return r.restore.Start(ctx, req, output)
	}
	return serverRuntimeStartResult(req)
}

func (r *restoreValidationRuntime) Destroy(context.Context, string) error {
	return nil
}

func (r *restoreValidationRuntime) DestroyGenerationResources(context.Context, store.RuntimeGenerationDetails) (runtime.GenerationResourceCleanup, error) {
	return runtime.GenerationResourceCleanup{}, nil
}

func (r *restoreValidationRuntime) Interrupt(string) error {
	return nil
}

func (r *restoreValidationRuntime) Checkpoint(context.Context, runtime.CheckpointRequest) (runtime.CheckpointResult, error) {
	return runtime.CheckpointResult{ImageManifestDigest: checkpointImageManifestDigestForTest}, nil
}

type serverCommandRunner struct {
	outputs map[string][]byte
	fail    map[string]error
}

func (r serverCommandRunner) CombinedOutput(_ context.Context, name string, args ...string) ([]byte, error) {
	key := strings.Join(append([]string{name}, args...), " ")
	if err := r.fail[key]; err != nil {
		return nil, err
	}
	if out, ok := r.outputs[key]; ok {
		return out, nil
	}
	return nil, nil
}

type failingRuntime struct {
	prepareErr    error
	err           error
	checkpointErr error
}

func (f failingRuntime) PrepareGeneration(context.Context, runtime.StartRequest) (runtime.GenerationArtifacts, error) {
	if f.prepareErr != nil {
		return runtime.GenerationArtifacts{}, f.prepareErr
	}
	return testGenerationArtifacts(), nil
}

func (f failingRuntime) RenderGenerationArtifacts(context.Context, runtime.StartRequest) (runtime.GenerationArtifactProjection, error) {
	if f.prepareErr != nil {
		return runtime.GenerationArtifactProjection{}, f.prepareErr
	}
	return runtime.GenerationArtifactProjection{Artifacts: testGenerationArtifacts()}, nil
}

func (f failingRuntime) MaterializeGenerationArtifacts(runtime.StartRequest, runtime.GenerationArtifactProjection) error {
	return nil
}

func (f failingRuntime) PrepareGenerationNetwork(context.Context, runtime.StartRequest) error {
	return nil
}

func (f failingRuntime) Start(context.Context, runtime.StartRequest, func(runtime.Output)) runtime.Result {
	return runtime.Result{Err: f.err}
}

func (f failingRuntime) Destroy(context.Context, string) error {
	return nil
}

func (f failingRuntime) DestroyGenerationResources(_ context.Context, details store.RuntimeGenerationDetails) (runtime.GenerationResourceCleanup, error) {
	return runtimeCleanupEvidenceForDetails(details), nil
}

func (f failingRuntime) Interrupt(string) error {
	return nil
}

func (f failingRuntime) Checkpoint(context.Context, runtime.CheckpointRequest) (runtime.CheckpointResult, error) {
	if f.checkpointErr != nil {
		return runtime.CheckpointResult{}, f.checkpointErr
	}
	return runtime.CheckpointResult{ImageManifestDigest: checkpointImageManifestDigestForTest}, nil
}
