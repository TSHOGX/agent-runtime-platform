package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/bridge"
	"harness-platform/orchestrator/internal/generationplan"
	"harness-platform/orchestrator/internal/planprojection"
	"harness-platform/orchestrator/internal/runtime"
	"harness-platform/orchestrator/internal/sessionstate"
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

func validServerGenerationPlanPayload() map[string]any {
	driver, _ := agents.DriverSpecFor("claude_code")
	provider, _ := agents.RuntimeProviderSpecFor("local_runsc")
	featurePolicy, _ := agents.FeaturePolicyPayload(agents.DefaultFeaturePolicyForDriver(driver))
	featurePolicyPayload := map[string]any{}
	for key, value := range featurePolicy {
		featurePolicyPayload[key] = value
	}
	featurePolicyPayload["capability_schema_version"] = agents.DriverCapabilitySchemaVersion
	featurePolicyPayload["capability_vocab_version"] = provider.CapabilityVocabulary
	featurePolicyPayload["driver_capabilities"] = agents.DriverCapabilityPayload(driver)
	featurePolicyPayload["runtime_provider_capabilities"] = agents.RuntimeProviderCapabilityPayload(provider)
	featurePolicyPayload["legacy_supports_interrupt"] = driver.SupportsInterrupt
	featurePolicyPayload["legacy_supports_compaction"] = driver.SupportsCompaction
	featurePolicyPayload["unsupported_features_fail"] = true
	featurePolicyPayload["credential_bearing_mcp_scope"] = "out_of_scope"
	adapterInputDigests := serverAdapterInputDigestPayloadForTest(serverFrozenEvidenceSandboxContractPayloadForTest(
		"sess_frozen_evidence",
		"gen_frozen_evidence",
		"contract_gen_frozen_evidence",
		"claude_code",
		"sha256:driver-state",
	))
	return map[string]any{
		"plan_version": store.GenerationPlanVersion,
		"identity":     map[string]any{"session_id": "sess_frozen_evidence", "generation_id": "gen_frozen_evidence", "product_mode": "agent"},
		"driver": map[string]any{
			"driver_id":               "claude_code",
			"driver_kind":             string(driver.Kind),
			"bridge_protocol":         driver.BridgeProtocol,
			"bridge_protocol_version": driver.BridgeProtocolVersion,
			"turn_input_schema":       driver.TurnInputSchema,
			"output_schema":           driver.OutputSchema,
			"output_format":           driver.OutputFormat,
			"model":                   "claude-test",
			"initial_state_digest":    "sha256:driver-state",
			"initial_state_version":   1,
			"capability_snapshot":     agents.DriverCapabilityPayload(driver),
		},
		"runtime_provider": map[string]any{
			"provider_id":                  provider.ID,
			"provider_config_id":           "local_runsc",
			"provider_profile_id":          provider.ProviderProfileID,
			"isolation_kind":               provider.IsolationKind,
			"template_ref":                 provider.TemplateRef,
			"capability_vocab_version":     provider.CapabilityVocabulary,
			"capability_digest":            agents.CapabilityDigest(provider),
			"capability_snapshot":          agents.RuntimeProviderCapabilityPayload(provider),
			"snapshot_policy":              provider.SnapshotPolicy,
			"agent_runtime_profile_id":     "arp_gen_frozen_evidence",
			"runtime_profile_provider_ref": "systrap",
		},
		"runsc_pin":    map[string]any{"platform": "systrap", "version": "runsc test", "binary_path": "/usr/local/bin/runsc-test", "binary_digest": "sha256:runsc"},
		"image":        map[string]any{"agent_manifest_digest": "sha256:agent-manifest", "rootfs_path": "/var/lib/harness/rootfs", "rootfs_image_digest": nil},
		"bridge_probe": map[string]any{"bridge_mode": "claim-loop"},
		"network": map[string]any{
			"network_profile_id": "net_gen_frozen_evidence", "runsc_network": "sandbox", "runsc_overlay2": "none",
			"sandbox_ip": "10.240.0.2", "sandbox_ip_cidr": "10.240.0.2/30", "host_gateway_ip": "10.240.0.1",
			"sandbox_base_url": "http://10.240.0.1:8080", "host_proxy_bind_url": "http://127.0.0.1:8080",
			"netns_name": "harness-gen-frozen", "netns_path": "/var/run/netns/harness-gen-frozen",
			"host_veth": "vh-frozen", "sandbox_veth": "vs-frozen", "host_side_cidr": "10.240.0.1/30",
			"nft_table_name": "harness-gen-frozen", "egress_policy_id": "egress_frozen",
			"egress_policy_digest": "egress_digest", "dns_policy": "off",
		},
		"data_volumes": map[string]any{
			"workspace":  serverPlanVolumePayload("/var/lib/harness/sessions/sess_frozen_evidence", "/var/lib/harness/evidence/workspaces/sess_frozen_evidence.json", "/workspace"),
			"agent_home": serverPlanVolumePayload("/var/lib/harness/agent-homes/sess_frozen_evidence/claude_code", "/var/lib/harness/evidence/driver-homes/sess_frozen_evidence/claude_code.json", "/agent-home"),
		},
		"mounts": map[string]any{
			"workspace":                      map[string]any{"source": "/var/lib/harness/sessions/sess_frozen_evidence", "destination": "/workspace", "mode": "rw"},
			"agent_home":                     map[string]any{"source": "/var/lib/harness/agent-homes/sess_frozen_evidence/claude_code", "destination": "/agent-home", "mode": "rw"},
			"control":                        map[string]any{"source": "/var/lib/harness/run/control/gen_frozen_evidence", "destination": "/harness-control", "mode": "ro"},
			"bridge":                         map[string]any{"source": "/var/lib/harness/run/bridge/gen_frozen_evidence", "destination": "/harness-control/bridge", "mode": "rw"},
			"network_hosts_path":             nil,
			"driver_config_materializations": nil,
		},
		"runtime_artifacts": map[string]any{
			"control_dir_path": "/var/lib/harness/run/control/gen_frozen_evidence", "control_manifest_path": "/var/lib/harness/run/control/gen_frozen_evidence/session.json",
			"control_manifest_digest": "manifest_digest", "projected_control_manifest_digest": "projected_manifest_digest",
			"bundle_dir_path": "/var/lib/harness/run/runtime/gen_frozen_evidence", "bundle_digest": "bundle_digest",
			"runtime_config_digest": "runtime_config_digest", "spec_path": "/var/lib/harness/run/runtime/gen_frozen_evidence/config.json",
			"spec_digest": "spec_digest", "bridge_dir_path": "/var/lib/harness/run/bridge/gen_frozen_evidence",
			"log_dir_path": "/var/lib/harness/logs/gen_frozen_evidence", "network_hosts_path": nil,
			"materialized_driver_config": []map[string]any{}, "resource_identity_digest": "sha256:resource",
			"sandbox_contract_id": "contract_gen_frozen_evidence", "sandbox_contract_payload_digest": "sha256:sandbox-contract",
			"sandbox_contract_compatibility_shape": store.SandboxContractVersion,
		},
		"feature_policy":    featurePolicyPayload,
		"content_snapshots": map[string]any{"skills": nil, "managed_settings": nil},
		"source_digests": map[string]any{
			"runtime_config_digest": "sha256:runtime-config-source",
			"agent_manifest_digest": "sha256:agent-manifest",
			"adapter_input_digests": adapterInputDigests,
		},
		"mutable_state_scope": map[string]any{"leases": "runtime_generations", "events": "events", "checkpoint_state": "runtime_generations"},
	}
}

func serverFrozenEvidenceSandboxContractPayloadForTest(sessionID, generationID, contractID, driverID, driverStateDigest string) map[string]any {
	modelAccessAllowed := driverID == "claude_code"
	return map[string]any{
		"sandbox_contract_version": store.SandboxContractVersion,
		"contract_schema_version":  store.SandboxContractSchemaVersion,
		"contract_gate_version":    store.SandboxContractGateDriverManifest,
		"contract_id":              contractID,
		"session_id":               sessionID,
		"generation_id":            generationID,
		"runtime_profile_id":       "arp_gen_frozen_evidence",
		"network_profile_id":       "net_gen_frozen_evidence",
		"driver": map[string]any{
			"driver_id":                            driverID,
			"driver_version":                       "test",
			"bridge_protocol":                      "harness_bridge_v2",
			"bridge_protocol_version":              2,
			"turn_input_schema":                    "RunTurn",
			"output_schema":                        "claude_stream_json_v1",
			"command_argv_digest":                  "sha256:command",
			"driver_config_digest":                 "sha256:driver-config",
			"required_runtime_capabilities_digest": "sha256:driver-capabilities",
			"supports_interrupt":                   false,
			"supports_compaction":                  true,
		},
		"runtime_provider": map[string]any{
			"provider_id":              "local_runsc",
			"provider_profile_id":      "local_runsc_default",
			"isolation_kind":           "gvisor",
			"template_ref":             "default",
			"template_digest":          "sha256:template",
			"capability_vocab_version": "1",
			"capability_digest":        "sha256:provider-capabilities",
		},
		"identity": map[string]any{
			"model_access_allowed": modelAccessAllowed,
		},
		"network_identity": map[string]any{
			"runsc_network": "sandbox",
			"sandbox_ip":    "10.240.0.2",
		},
		"credential_policy": serverCredentialPolicyPayloadForTest(driverID),
		"model_access": map[string]any{
			"model_access_allowed":         modelAccessAllowed,
			"sandbox_model_proxy_base_url": "http://harness-model-proxy.internal:8082",
		},
		"driver_runtime": map[string]any{
			"driver_home_mount":             "/agent-home",
			"generated_driver_config_mount": "/harness-control/driver/" + driverID,
			"materialized_driver_config":    map[string]any{},
			"initial_driver_state_digest":   driverStateDigest,
		},
		"runtime_adapter": map[string]any{
			"kind":                "runsc",
			"runsc_platform":      "systrap",
			"runsc_version":       "runsc test",
			"runsc_binary_path":   "/usr/local/bin/runsc-test",
			"runsc_binary_digest": "sha256:runsc",
			"runsc_container_id":  "runsc-gen-frozen",
			"runsc_network":       "sandbox",
			"runsc_overlay2":      "none",
		},
		"input_digests": map[string]any{
			"runtime_config_digest": "sha256:runtime-config",
			"rootfs_image_digest":   nil,
			"agent_manifest_digest": "sha256:agent-manifest",
		},
	}
}

func serverAdapterInputDigestPayloadForTest(contractPayload map[string]any) map[string]any {
	digests, err := generationplan.AdapterInputDigestsFromSandboxContract(contractPayload)
	if err != nil {
		panic(err)
	}
	return map[string]any{
		"driver_adapter":  digests["driver_adapter"],
		"runtime_adapter": digests["runtime_adapter"],
	}
}

func storeServerFrozenEvidenceCanonicalPayload(t *testing.T) []byte {
	t.Helper()
	canonical, err := serverFrozenEvidenceCanonicalPayload()
	if err != nil {
		t.Fatalf("canonical frozen evidence payload: %v", err)
	}
	return canonical
}

func serverFrozenEvidenceCanonicalPayload() ([]byte, error) {
	return store.CanonicalGenerationPlanPayload(validServerGenerationPlanPayload())
}

func mustServerFrozenEvidenceCanonicalPayload() []byte {
	canonical, err := serverFrozenEvidenceCanonicalPayload()
	if err != nil {
		panic(err)
	}
	return canonical
}

func storeServerFrozenEvidencePlan(t *testing.T, ctx context.Context, st *store.Store, dir string, payload map[string]any) store.GenerationPlanRecord {
	t.Helper()
	session := createServerTestSession(t, ctx, st, dir, "sess_frozen_evidence", string(sessionstate.Created), time.Now().UTC(), nil)
	if _, err := st.DBForTest().ExecContext(ctx, `
INSERT INTO runtime_generations (generation_id, session_id, status, lease_owner, lease_expires_at)
VALUES (?, ?, 'allocating', 'owner', ?)`, "gen_frozen_evidence", session.ID, time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("insert runtime generation: %v", err)
	}
	plan, err := st.StoreGenerationPlan(ctx, store.StoreGenerationPlanParams{
		GenerationID: "gen_frozen_evidence",
		Payload:      payload,
	})
	if err != nil {
		t.Fatalf("store generation plan: %v", err)
	}
	for kind, digest := range map[string]string{
		"sandbox_contract":           "sha256:sandbox-contract",
		"control_manifest":           "sha256:control-manifest",
		"control_manifest_projected": "sha256:control-manifest-projected",
		"oci_spec":                   "sha256:oci-spec",
		"bundle":                     "sha256:bundle",
		"runtime_config":             "sha256:runtime-config",
	} {
		if _, err := st.StoreGenerationPlanProjection(ctx, store.StoreGenerationPlanProjectionParams{
			GenerationID:      "gen_frozen_evidence",
			PlanDigest:        plan.PlanDigest,
			ProjectionKind:    kind,
			ProjectionVersion: 1,
			PayloadDigest:     digest,
		}); err != nil {
			t.Fatalf("store projection %s: %v", kind, err)
		}
	}
	return plan
}

type serverGenerationPlanSourceDigestsForTest struct {
	RuntimeConfigDigest string
	AgentManifestDigest string
}

func storeServerSyntheticSandboxContractParentForPlan(t *testing.T, ctx context.Context, st *store.Store, plan store.GenerationPlanRecord) {
	t.Helper()
	sessionID := serverGenerationPlanSessionID(t, plan.CanonicalPayload)
	contractID := sandboxContractID(plan.GenerationID)
	canonicalPayload, err := store.CanonicalSandboxContractPayload(serverFrozenEvidenceSandboxContractPayloadForTest(
		sessionID,
		plan.GenerationID,
		contractID,
		"claude_code",
		"sha256:driver-state",
	))
	if err != nil {
		t.Fatalf("canonical synthetic sandbox contract parent: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
INSERT INTO sandbox_contracts (
  contract_id, generation_id, session_id, sandbox_contract_version,
  contract_schema_version, contract_gate_version, canonical_payload,
  sandbox_contract_digest, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(contract_id) DO NOTHING`,
		contractID, plan.GenerationID, sessionID, store.SandboxContractVersion,
		store.SandboxContractSchemaVersion, store.SandboxContractGateDriverManifest,
		string(canonicalPayload), store.SandboxContractDigest(canonicalPayload),
		time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("store synthetic sandbox contract parent: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generations
SET sandbox_contract_id = ?,
    sandbox_contract_version = ?
WHERE generation_id = ?
  AND session_id = ?`, contractID, store.SandboxContractVersion, plan.GenerationID, sessionID); err != nil {
		t.Fatalf("store synthetic sandbox contract generation mirror: %v", err)
	}
}

func storeServerSandboxContractInputEvidenceFromGenerationPlanIfPresent(t *testing.T, ctx context.Context, st *store.Store, generationID string) {
	t.Helper()
	plan, err := st.GetGenerationPlan(ctx, generationID)
	if errors.Is(err, sql.ErrNoRows) {
		return
	}
	if err != nil {
		t.Fatalf("get generation plan for input evidence: %v", err)
	}
	storeServerSandboxContractInputEvidenceFromPlan(t, ctx, st, plan)
}

func storeServerSandboxContractInputEvidenceFromPlan(t *testing.T, ctx context.Context, st *store.Store, plan store.GenerationPlanRecord) {
	t.Helper()
	digests := serverGenerationPlanSourceDigests(t, plan.CanonicalPayload)
	contractID := sandboxContractID(plan.GenerationID)
	if _, err := st.DBForTest().ExecContext(ctx, `
INSERT INTO sandbox_contract_input_evidence (
  contract_id, runtime_config_digest, runtime_config_preimage,
  agent_manifest_digest, agent_manifest_payload, created_at
) VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(contract_id) DO NOTHING`,
		contractID, digests.RuntimeConfigDigest, "{}",
		digests.AgentManifestDigest, "{}", time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("store sandbox contract input evidence: %v", err)
	}
	evidence, err := st.GetSandboxContractInputEvidence(ctx, contractID)
	if err != nil {
		t.Fatalf("get sandbox contract input evidence: %v", err)
	}
	if evidence.RuntimeConfigDigest != digests.RuntimeConfigDigest ||
		evidence.AgentManifestDigest != digests.AgentManifestDigest {
		t.Fatalf("sandbox contract input evidence mismatch: evidence=%+v want=%+v", evidence, digests)
	}
}

func serverGenerationPlanSessionID(t *testing.T, canonicalPayload []byte) string {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(canonicalPayload, &payload); err != nil {
		t.Fatalf("decode generation plan payload: %v", err)
	}
	identity, ok := payload["identity"].(map[string]any)
	if !ok {
		t.Fatalf("generation plan missing identity: %s", canonicalPayload)
	}
	sessionID, _ := identity["session_id"].(string)
	if strings.TrimSpace(sessionID) == "" {
		t.Fatalf("generation plan missing identity.session_id: %s", canonicalPayload)
	}
	return sessionID
}

func serverGenerationPlanSourceDigests(t *testing.T, canonicalPayload []byte) serverGenerationPlanSourceDigestsForTest {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(canonicalPayload, &payload); err != nil {
		t.Fatalf("decode generation plan payload: %v", err)
	}
	sourceDigests, ok := payload["source_digests"].(map[string]any)
	if !ok {
		t.Fatalf("generation plan missing source_digests: %s", canonicalPayload)
	}
	digests := serverGenerationPlanSourceDigestsForTest{}
	digests.RuntimeConfigDigest, _ = sourceDigests["runtime_config_digest"].(string)
	digests.AgentManifestDigest, _ = sourceDigests["agent_manifest_digest"].(string)
	if strings.TrimSpace(digests.RuntimeConfigDigest) == "" ||
		strings.TrimSpace(digests.AgentManifestDigest) == "" {
		t.Fatalf("generation plan missing source digests: %s", canonicalPayload)
	}
	return digests
}

func storeServerGenerationPlanForArtifacts(t *testing.T, ctx context.Context, st *store.Store, sessionID, generationID string, artifacts runtime.GenerationArtifacts) store.GenerationPlanRecord {
	t.Helper()
	details, err := st.GetRuntimeGenerationDetails(ctx, sessionID, generationID)
	if err != nil {
		t.Fatalf("get generation details for plan %s: %v", generationID, err)
	}
	mode := "agent"
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COALESCE(mode, '')
FROM sessions
WHERE id = ?`, sessionID).Scan(&mode); err != nil {
		t.Fatalf("get session mode for plan %s: %v", generationID, err)
	}
	if strings.TrimSpace(mode) == "" {
		mode = "agent"
	}
	driverSpec, ok := agents.DriverSpecFor(details.DriverID)
	if !ok {
		t.Fatalf("driver spec missing for %s", details.DriverID)
	}
	providerSpec, ok := agents.RuntimeProviderSpecFor("local_runsc")
	if !ok {
		t.Fatalf("provider spec missing")
	}
	featurePolicy, err := agents.FeaturePolicyPayload(agents.DefaultFeaturePolicyForDriver(driverSpec))
	if err != nil {
		t.Fatalf("feature policy for plan %s: %v", generationID, err)
	}
	featurePolicyPayload := map[string]any{}
	for key, value := range featurePolicy {
		featurePolicyPayload[key] = value
	}
	featurePolicyPayload["capability_schema_version"] = agents.DriverCapabilitySchemaVersion
	featurePolicyPayload["capability_vocab_version"] = providerSpec.CapabilityVocabulary
	featurePolicyPayload["driver_capabilities"] = agents.DriverCapabilityPayload(driverSpec)
	featurePolicyPayload["runtime_provider_capabilities"] = agents.RuntimeProviderCapabilityPayload(providerSpec)
	featurePolicyPayload["legacy_supports_interrupt"] = driverSpec.SupportsInterrupt
	featurePolicyPayload["legacy_supports_compaction"] = driverSpec.SupportsCompaction
	featurePolicyPayload["unsupported_features_fail"] = true
	featurePolicyPayload["credential_bearing_mcp_scope"] = "out_of_scope"
	payload := validServerGenerationPlanPayload()
	payload["identity"] = map[string]any{"session_id": sessionID, "generation_id": generationID, "product_mode": mode}
	workspaceVolume, driverHomeVolume := provisionServerGenerationPlanFixtureVolumes(t, ctx, st, sessionID, details)
	driverPlan := payload["driver"].(map[string]any)
	driverPlan["driver_id"] = string(driverSpec.ID)
	driverPlan["driver_kind"] = string(driverSpec.Kind)
	driverPlan["bridge_protocol"] = driverSpec.BridgeProtocol
	driverPlan["bridge_protocol_version"] = driverSpec.BridgeProtocolVersion
	driverPlan["turn_input_schema"] = driverSpec.TurnInputSchema
	driverPlan["output_schema"] = driverSpec.OutputSchema
	driverPlan["output_format"] = details.OutputFormat
	if strings.TrimSpace(details.Model) == "" {
		driverPlan["model"] = nil
	} else {
		driverPlan["model"] = details.Model
	}
	driverPlan["initial_state_digest"] = details.DriverStateDigest
	driverPlan["initial_state_version"] = details.DriverStateVersion
	driverPlan["capability_snapshot"] = agents.DriverCapabilityPayload(driverSpec)
	runtimeProvider := payload["runtime_provider"].(map[string]any)
	runtimeProvider["agent_runtime_profile_id"] = details.AgentRuntimeProfileID
	runtimeProvider["runtime_profile_provider_ref"] = details.RunscPlatform
	networkPlan := payload["network"].(map[string]any)
	networkPlan["network_profile_id"] = details.NetworkProfileID
	networkPlan["runsc_network"] = details.RunscNetwork
	networkPlan["runsc_overlay2"] = details.RunscOverlay2
	sandboxIP, err := runtimeResourceSandboxIP(details.SandboxIPCIDR)
	if err != nil {
		t.Fatalf("render sandbox ip for plan %s: %v", generationID, err)
	}
	networkPlan["sandbox_ip"] = sandboxIP
	networkPlan["sandbox_ip_cidr"] = details.SandboxIPCIDR
	networkPlan["host_gateway_ip"] = details.HostGatewayIP
	networkPlan["sandbox_base_url"] = details.SandboxBaseURL
	networkPlan["host_proxy_bind_url"] = details.HostProxyBindURL
	networkPlan["proxy_port"] = details.ProxyPort
	networkPlan["netns_name"] = details.NetnsName
	networkPlan["netns_path"] = details.NetnsPath
	networkPlan["host_veth"] = details.HostVeth
	networkPlan["sandbox_veth"] = details.SandboxVeth
	networkPlan["host_side_cidr"] = details.HostSideCIDR
	nftTableName, err := runtimeResourceNftTableName(details.GenerationID)
	if err != nil {
		t.Fatalf("render nft table for plan %s: %v", generationID, err)
	}
	networkPlan["nft_table_name"] = nftTableName
	networkPlan["egress_policy_id"] = details.EgressPolicyID
	networkPlan["egress_policy_digest"] = details.EgressPolicyDigest
	networkPlan["dns_policy"] = details.DNSPolicy
	runscPin := payload["runsc_pin"].(map[string]any)
	runscPin["version"] = artifacts.RunscVersion
	runscPin["binary_path"] = artifacts.RunscBinaryPath
	runscPin["binary_digest"] = artifacts.RunscBinaryDigest
	runtimeArtifacts := payload["runtime_artifacts"].(map[string]any)
	runtimeArtifacts["control_manifest_digest"] = artifacts.ManifestDigest
	runtimeArtifacts["projected_control_manifest_digest"] = artifacts.ProjectedManifestDigest
	runtimeArtifacts["control_dir_path"] = details.ControlDirPath
	runtimeArtifacts["control_manifest_path"] = details.ControlManifestPath
	runtimeArtifacts["bundle_dir_path"] = details.BundleDirPath
	runtimeArtifacts["bundle_digest"] = artifacts.BundleDigest
	runtimeArtifacts["runtime_config_digest"] = artifacts.RuntimeConfigDigest
	runtimeArtifacts["spec_path"] = details.SpecPath
	runtimeArtifacts["spec_digest"] = artifacts.SpecDigest
	runtimeArtifacts["bridge_dir_path"] = details.BridgeDirPath
	runtimeArtifacts["log_dir_path"] = details.LogDirPath
	if strings.TrimSpace(details.NetworkHostsPath) == "" {
		runtimeArtifacts["network_hosts_path"] = nil
	} else {
		runtimeArtifacts["network_hosts_path"] = details.NetworkHostsPath
	}
	allocation := serverGenerationAllocationForTest(t, ctx, st, sessionID, generationID)
	sandboxContractPayload := serverRuntimeResourceSandboxContractPayloadForTest(t, details, allocation, sandboxContractID(generationID))
	sandboxContractDigest := serverSandboxContractPayloadDigestForTest(t, sandboxContractPayload)
	runtimeArtifacts["sandbox_contract_id"] = sandboxContractID(generationID)
	runtimeArtifacts["sandbox_contract_payload_digest"] = sandboxContractDigest
	runtimeArtifacts["resource_identity_digest"] = serverRuntimeResourceIdentityDigestForPlanFixture(t, details, artifacts)
	sourceDigests := payload["source_digests"].(map[string]any)
	sourceDigests["adapter_input_digests"] = serverAdapterInputDigestPayloadForTest(sandboxContractPayload)
	dataVolumes := payload["data_volumes"].(map[string]any)
	serverApplyWorkspaceVolumePayload(dataVolumes["workspace"].(map[string]any), workspaceVolume)
	serverApplyDriverHomeVolumePayload(dataVolumes["agent_home"].(map[string]any), driverHomeVolume)
	mounts := payload["mounts"].(map[string]any)
	mounts["workspace"].(map[string]any)["source"] = workspaceVolume.HostPath
	mounts["agent_home"].(map[string]any)["source"] = driverHomeVolume.HostPath
	mounts["control"].(map[string]any)["source"] = details.ControlDirPath
	mounts["bridge"].(map[string]any)["source"] = details.BridgeDirPath
	if strings.TrimSpace(details.NetworkHostsPath) == "" {
		mounts["network_hosts_path"] = nil
	} else {
		mounts["network_hosts_path"] = details.NetworkHostsPath
	}
	payload["feature_policy"] = featurePolicyPayload
	plan, err := st.StoreGenerationPlan(ctx, store.StoreGenerationPlanParams{
		GenerationID: generationID,
		Payload:      payload,
	})
	if err != nil {
		t.Fatalf("store generation plan for %s: %v", generationID, err)
	}
	projections := append([]store.GenerationPlanProjectionExpectation{{
		ProjectionKind:    store.GenerationPlanProjectionSandboxContract,
		ProjectionVersion: store.GenerationPlanProjectionVersion,
		PayloadDigest:     sandboxContractDigest,
	}}, planprojection.ExpectationsForDetails(details, artifacts)...)
	for _, projection := range projections {
		if _, err := st.StoreGenerationPlanProjection(ctx, store.StoreGenerationPlanProjectionParams{
			GenerationID:      generationID,
			PlanDigest:        plan.PlanDigest,
			ProjectionKind:    projection.ProjectionKind,
			ProjectionVersion: projection.ProjectionVersion,
			PayloadDigest:     projection.PayloadDigest,
			MaterializedPath:  projection.MaterializedPath,
		}); err != nil {
			t.Fatalf("store generation plan projection %s for %s: %v", projection.ProjectionKind, generationID, err)
		}
	}
	return plan
}

func serverRuntimeResourceIdentityDigestForPlanFixture(t *testing.T, details store.RuntimeGenerationDetails, artifacts runtime.GenerationArtifacts) string {
	t.Helper()
	sandboxIP, err := runtimeResourceSandboxIP(details.SandboxIPCIDR)
	if err != nil {
		t.Fatalf("runtime resource sandbox ip for plan %s: %v", details.GenerationID, err)
	}
	nftTableName, err := runtimeResourceNftTableName(details.GenerationID)
	if err != nil {
		t.Fatalf("runtime resource nft table for plan %s: %v", details.GenerationID, err)
	}
	params := store.RuntimeResourceInstanceParams{
		GenerationID:           details.GenerationID,
		SessionID:              details.SessionID,
		ContractID:             sandboxContractID(details.GenerationID),
		SandboxContractVersion: store.SandboxContractVersion,
		HostID:                 mustRuntimeResourceHostID(t),
		RunscContainerID:       details.RunscContainerID,
		RunscPlatform:          details.RunscPlatform,
		RunscVersion:           artifacts.RunscVersion,
		RunscBinaryPath:        artifacts.RunscBinaryPath,
		RunscBinaryDigest:      artifacts.RunscBinaryDigest,
		NetworkProfileID:       details.NetworkProfileID,
		NetnsName:              details.NetnsName,
		NetnsPath:              details.NetnsPath,
		HostVeth:               details.HostVeth,
		SandboxVeth:            details.SandboxVeth,
		HostGatewayIP:          details.HostGatewayIP,
		SandboxIP:              sandboxIP,
		SandboxIPCIDR:          details.SandboxIPCIDR,
		HostSideCIDR:           details.HostSideCIDR,
		NftTableName:           nftTableName,
		ControlDirPath:         details.ControlDirPath,
		ControlManifestPath:    details.ControlManifestPath,
		BundleDirPath:          details.BundleDirPath,
		SpecPath:               details.SpecPath,
		CheckpointPath:         details.CheckpointPath,
		BridgeDirPath:          details.BridgeDirPath,
		NetworkHostsPath:       details.NetworkHostsPath,
		LogDirPath:             details.LogDirPath,
		RootPrefixes: map[string]string{
			"run_dir":      filepath.Dir(filepath.Dir(details.ControlDirPath)),
			"control_root": filepath.Dir(details.ControlDirPath),
			"bridge_root":  filepath.Dir(details.BridgeDirPath),
		},
	}
	_, digest, err := store.RuntimeResourceIdentityForParams(params)
	if err != nil {
		t.Fatalf("runtime resource identity for plan %s: %v", details.GenerationID, err)
	}
	return digest
}

func provisionServerGenerationPlanFixtureVolumes(t *testing.T, ctx context.Context, st *store.Store, sessionID string, details store.RuntimeGenerationDetails) (store.SessionWorkspaceVolume, store.SessionDriverHomeVolume) {
	t.Helper()
	cfg := serverGenerationPlanFixtureVolumeConfig(t, details)
	now := time.Now().UTC()
	workspace, err := st.ProvisionSessionWorkspace(ctx, store.ProvisionSessionWorkspaceParams{
		SessionID: sessionID,
		Config:    cfg,
		Now:       now,
	})
	if err != nil {
		t.Fatalf("provision workspace volume for plan %s: %v", details.GenerationID, err)
	}
	driverHome, err := st.ProvisionSessionDriverHome(ctx, store.ProvisionSessionDriverHomeParams{
		SessionID: sessionID,
		Driver:    details.DriverID,
		Config:    cfg,
		Now:       now,
	})
	if err != nil {
		t.Fatalf("provision driver-home volume for plan %s: %v", details.GenerationID, err)
	}
	return workspace, driverHome
}

func serverGenerationPlanFixtureVolumeConfig(t *testing.T, details store.RuntimeGenerationDetails) store.DataVolumeProvisionerConfig {
	t.Helper()
	controlDir := strings.TrimSpace(details.ControlDirPath)
	if controlDir == "" {
		t.Fatalf("generation %s control dir path is required for plan fixture volumes", details.GenerationID)
	}
	runDir := filepath.Dir(filepath.Dir(controlDir))
	fixtureRoot := filepath.Dir(runDir)
	if runDir == "." || fixtureRoot == "." {
		t.Fatalf("generation %s control dir path %q cannot derive plan fixture roots", details.GenerationID, controlDir)
	}
	return store.DataVolumeProvisionerConfig{
		SessionsRoot:   filepath.Join(fixtureRoot, "sessions"),
		AgentHomesRoot: filepath.Join(fixtureRoot, "agent-homes"),
		EvidenceRoot:   filepath.Join(fixtureRoot, "state", "volume-evidence"),
		LayoutVersion:  store.DataVolumeLayoutVersion,
		RuntimeIdentity: store.RuntimeIdentity{
			UID: serverTestSandboxUID(),
			GID: serverTestSandboxGID(),
		},
	}
}

func serverApplyWorkspaceVolumePayload(payload map[string]any, volume store.SessionWorkspaceVolume) {
	payload["session_id"] = volume.SessionID
	payload["host_path"] = volume.HostPath
	payload["layout_version"] = volume.LayoutVersion
	payload["runtime_identity_digest"] = volume.RuntimeIdentityDigest
	payload["provisioning_marker_path"] = volume.ProvisioningMarkerPath
	payload["provisioning_marker_digest"] = volume.ProvisioningMarkerDigest
	payload["sandbox_uid"] = volume.SandboxUID
	payload["sandbox_gid"] = volume.SandboxGID
	payload["sandbox_supplemental_gids"] = append([]int(nil), volume.SandboxSupplementalGIDs...)
}

func serverApplyDriverHomeVolumePayload(payload map[string]any, volume store.SessionDriverHomeVolume) {
	payload["session_id"] = volume.SessionID
	payload["driver"] = volume.Driver
	payload["host_path"] = volume.HostPath
	payload["layout_version"] = volume.LayoutVersion
	payload["runtime_identity_digest"] = volume.RuntimeIdentityDigest
	payload["provisioning_marker_path"] = volume.ProvisioningMarkerPath
	payload["provisioning_marker_digest"] = volume.ProvisioningMarkerDigest
	payload["sandbox_uid"] = volume.SandboxUID
	payload["sandbox_gid"] = volume.SandboxGID
	payload["sandbox_supplemental_gids"] = append([]int(nil), volume.SandboxSupplementalGIDs...)
}

func serverPlanVolumePayload(hostPath, markerPath, destination string) map[string]any {
	return map[string]any{
		"session_id": "sess_frozen_evidence", "host_path": hostPath, "layout_version": 1,
		"runtime_identity_digest": "sha256:identity", "provisioning_marker_path": markerPath,
		"provisioning_marker_digest": "sha256:marker", "sandbox_destination": destination,
		"sandbox_uid": 65534, "sandbox_gid": 65534, "sandbox_supplemental_gids": []int{},
	}
}

func serverGenerationPlanFrozenEvidenceDetails() store.RuntimeGenerationDetails {
	return store.RuntimeGenerationDetails{
		GenerationID:                    "gen_frozen_evidence",
		SessionID:                       "sess_frozen_evidence",
		RunscPlatform:                   "systrap",
		CheckpointBundleDigest:          "sha256:bundle",
		CheckpointRuntimeConfigDigest:   "sha256:runtime-config",
		CheckpointControlManifestDigest: "sha256:control-manifest-projected",
		CheckpointDriverStatesDigest:    "sha256:driver-state-fence",
		CheckpointPlanDigest:            store.GenerationPlanDigest(mustServerFrozenEvidenceCanonicalPayload()),
	}
}

func serverGenerationPlanFrozenEvidenceArtifacts() runtime.GenerationArtifacts {
	return runtime.GenerationArtifacts{
		ManifestDigest:          "sha256:control-manifest",
		ProjectedManifestDigest: "sha256:control-manifest-projected",
		BundleDigest:            "sha256:bundle",
		RuntimeConfigDigest:     "sha256:runtime-config",
		SpecDigest:              "sha256:oci-spec",
		RunscVersion:            "runsc test",
		RunscBinaryPath:         "/usr/local/bin/runsc-test",
		RunscBinaryDigest:       "sha256:runsc",
	}
}

func createServerRuntimeResourceLive(t *testing.T, ctx context.Context, st *store.Store, sessionID string, allocation store.GenerationAllocation, ownerUUID, hostID string, now time.Time) store.RuntimeResourceInstance {
	t.Helper()
	contractID := sandboxContractID(allocation.GenerationID)
	details, err := st.GetRuntimeGenerationDetails(ctx, sessionID, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get generation details: %v", err)
	}
	prefix, err := netip.ParsePrefix(details.SandboxIPCIDR)
	if err != nil {
		t.Fatalf("parse sandbox cidr: %v", err)
	}
	if _, err := st.StoreSandboxContract(ctx, store.StoreSandboxContractParams{
		ContractID:             contractID,
		SessionID:              sessionID,
		GenerationID:           allocation.GenerationID,
		SandboxContractVersion: store.SandboxContractVersion,
		ContractSchemaVersion:  store.SandboxContractSchemaVersion,
		ContractGateVersion:    store.SandboxContractGateDriverManifest,
		Payload:                serverRuntimeResourceSandboxContractPayloadForTest(t, details, allocation, contractID),
		Now:                    now,
	}); err != nil {
		t.Fatalf("store sandbox contract: %v", err)
	}
	storeServerSandboxContractInputEvidenceFromGenerationPlanIfPresent(t, ctx, st, allocation.GenerationID)
	artifacts := testGenerationArtifacts()
	if strings.TrimSpace(details.RunscVersion) != "" {
		artifacts.RunscVersion = details.RunscVersion
	}
	if strings.TrimSpace(details.RunscBinaryPath) != "" {
		artifacts.RunscBinaryPath = details.RunscBinaryPath
	}
	if strings.TrimSpace(details.RunscBinaryDigest) != "" {
		artifacts.RunscBinaryDigest = details.RunscBinaryDigest
	}
	instance, err := st.CreateRuntimeResourceInstance(ctx, store.RuntimeResourceInstanceParams{
		GenerationID:           allocation.GenerationID,
		SessionID:              sessionID,
		ContractID:             contractID,
		SandboxContractVersion: store.SandboxContractVersion,
		HostID:                 hostID,
		RunscContainerID:       details.RunscContainerID,
		RunscPlatform:          "systrap",
		RunscVersion:           artifacts.RunscVersion,
		RunscBinaryPath:        artifacts.RunscBinaryPath,
		RunscBinaryDigest:      artifacts.RunscBinaryDigest,
		NetworkProfileID:       allocation.NetworkProfileID,
		NetnsName:              details.NetnsName,
		NetnsPath:              details.NetnsPath,
		HostVeth:               details.HostVeth,
		SandboxVeth:            details.SandboxVeth,
		HostGatewayIP:          details.HostGatewayIP,
		SandboxIP:              prefix.Addr().String(),
		SandboxIPCIDR:          details.SandboxIPCIDR,
		HostSideCIDR:           details.HostSideCIDR,
		NftTableName:           mustRuntimeResourceNftTableName(t, allocation.GenerationID),
		ControlDirPath:         details.ControlDirPath,
		ControlManifestPath:    details.ControlManifestPath,
		BundleDirPath:          details.BundleDirPath,
		SpecPath:               details.SpecPath,
		CheckpointPath:         details.CheckpointPath,
		BridgeDirPath:          details.BridgeDirPath,
		NetworkHostsPath:       details.NetworkHostsPath,
		LogDirPath:             details.LogDirPath,
		RootPrefixes: map[string]string{
			"run_dir":      filepath.Dir(filepath.Dir(details.ControlDirPath)),
			"control_root": filepath.Dir(details.ControlDirPath),
			"bridge_root":  filepath.Dir(details.BridgeDirPath),
		},
		Now: now,
	})
	if err != nil {
		t.Fatalf("create runtime resource instance: %v", err)
	}
	workerID := strings.TrimSpace(ownerUUID)
	if workerID == "" {
		workerID = strings.TrimSuffix(strings.TrimSpace(allocation.Owner), ":"+store.RuntimeManagerRoleTag)
	}
	if err := st.ClaimRuntimeResourceMaterialization(ctx, store.RuntimeResourceMaterializationClaimParams{
		GenerationID:     allocation.GenerationID,
		WorkerID:         workerID,
		HostID:           hostID,
		LeaseExpiresAt:   now.Add(time.Minute),
		IdempotencyToken: "test:" + allocation.GenerationID,
		Now:              now.Add(time.Millisecond),
	}); err != nil {
		t.Fatalf("claim runtime resource materialization: %v", err)
	}
	if err := st.MarkRuntimeResourceReady(ctx, store.RuntimeResourceWorkerTransitionParams{
		GenerationID: allocation.GenerationID,
		WorkerID:     workerID,
		HostID:       hostID,
		Now:          now.Add(2 * time.Millisecond),
	}); err != nil {
		t.Fatalf("mark runtime resource ready: %v", err)
	}
	if err := st.MarkRuntimeResourceLive(ctx, store.RuntimeResourceWorkerTransitionParams{
		GenerationID: allocation.GenerationID,
		WorkerID:     workerID,
		HostID:       hostID,
		PostStart:    serverPostStartProofForTest(instance),
		Now:          now.Add(3 * time.Millisecond),
	}); err != nil {
		t.Fatalf("mark runtime resource live: %v", err)
	}
	return instance
}
