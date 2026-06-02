package server

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"harness-platform/orchestrator/internal/artifacts"
	"harness-platform/orchestrator/internal/bridge"
	"harness-platform/orchestrator/internal/config"
	"harness-platform/orchestrator/internal/events"
	"harness-platform/orchestrator/internal/planprojection"
	"harness-platform/orchestrator/internal/runtime"
	"harness-platform/orchestrator/internal/store"
)

const labUserID = "lab"

type generationStartFailureMode int

const (
	startFailureInputAcceptable generationStartFailureMode = iota
	startFailureInputBlocking
)

type Server struct {
	cfg       config.Config
	store     *store.Store
	runtime   runtimeDriver
	watcher   *artifacts.Watcher
	hub       *events.Hub
	log       *slog.Logger
	ownerUUID string

	bridgeParserMu sync.Mutex
	bridgeParsers  map[bridgeStreamParserKey]*streamParser
}

type runtimeDriver interface {
	Start(context.Context, runtime.StartRequest, func(runtime.Output)) runtime.Result
	RenderGenerationArtifacts(context.Context, runtime.StartRequest) (runtime.GenerationArtifactProjection, error)
	MaterializeGenerationArtifacts(runtime.StartRequest, runtime.GenerationArtifactProjection) error
	PrepareGenerationNetwork(context.Context, runtime.StartRequest) error
	Destroy(context.Context, string) error
	DestroyGenerationResources(context.Context, store.RuntimeGenerationDetails) (runtime.GenerationResourceCleanup, error)
	Interrupt(string) error
	Checkpoint(context.Context, runtime.CheckpointRequest) (runtime.CheckpointResult, error)
}

type bridgeStore interface {
	bridge.Store
	ListBridgePollGenerations(context.Context, string, time.Time, time.Duration) ([]store.BridgePollGeneration, error)
}

func New(
	cfg config.Config,
	store *store.Store,
	runtime runtimeDriver,
	watcher *artifacts.Watcher,
	hub *events.Hub,
	log *slog.Logger,
) *Server {
	return &Server{
		cfg:     cfg,
		store:   store,
		runtime: runtime,
		watcher: watcher,
		hub:     hub,
		log:     log,
	}
}

func (s *Server) SetOwnerUUID(ownerUUID string) {
	s.ownerUUID = strings.TrimSpace(ownerUUID)
}

func (s *Server) startEnsuredGeneration(ctx context.Context, session store.Session, ensured ensuredGeneration, failureMode generationStartFailureMode) error {
	allocation := ensured.Allocation
	startCtx := ctx
	leaseKeeper := noopStartLeaseKeeper()
	if ensured.IsNew || ensured.RestoreFromCheckpoint {
		var err error
		startCtx, leaseKeeper, err = s.beginGenerationStartLease(ctx, session.ID, allocation.GenerationID, allocation.Owner)
		if err != nil {
			return err
		}
	}
	defer leaseKeeper.stop()
	generationDetails, err := s.runtimeGenerationDetails(ctx, session.ID, allocation.GenerationID)
	if err != nil {
		return err
	}
	dataVolumes, err := s.ensureSessionRuntimeDataVolumes(ctx, session)
	if err != nil {
		if leaseErr := leaseKeeper.err(); leaseErr != nil {
			return leaseErr
		}
		if ensured.RestoreFromCheckpoint {
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		if ensured.IsNew {
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
		}
		return err
	}
	contentSnapshots, err := s.generationContentSnapshotsForStart(ctx, session, generationDetails, ensured.IsNew)
	if err != nil {
		if leaseErr := leaseKeeper.err(); leaseErr != nil {
			return leaseErr
		}
		if ensured.RestoreFromCheckpoint || ensured.IsNew {
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
		}
		return err
	}
	preparedArtifacts := runtimeArtifactsFromDetails(generationDetails)
	if !ensured.IsNew {
		preparedArtifacts, err = s.generationPlanRuntimeArtifacts(ctx, allocation.GenerationID)
		if err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				return leaseErr
			}
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
	}
	resourceWorkerID := runtimeResourceWorkerID(s.ownerUUID, allocation.Owner)
	resourceHostID, err := runtimeResourceHostID()
	if err != nil {
		if leaseErr := leaseKeeper.err(); leaseErr != nil {
			return leaseErr
		}
		if ensured.RestoreFromCheckpoint || ensured.IsNew {
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
		}
		return err
	}
	var runtimeResourceCreated bool
	var runtimeResourceInstance store.RuntimeResourceInstance
	resourceIdentityDigest := ""
	sandboxContractDigest := ""
	var artifactProjection runtime.GenerationArtifactProjection
	retireRuntimeResource := func() {
		if !runtimeResourceCreated {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.store.ClaimRuntimeResourceRetiring(cleanupCtx, store.RuntimeResourceRetireParams{
			GenerationID: allocation.GenerationID,
			WorkerID:     resourceWorkerID,
			HostID:       resourceHostID,
			Now:          time.Now().UTC(),
		}); err != nil && !errors.Is(err, context.Canceled) {
			s.log.Warn("failed to retire runtime resource after start failure", "session_id", session.ID, "generation_id", allocation.GenerationID, "error", err)
		}
	}
	if ensured.IsNew {
		renderRequest := s.runtimeStartRequest(session, allocation.GenerationID, generationDetails, runtime.GenerationArtifacts{}, dataVolumes, contentSnapshots)
		artifactProjection, err = s.runtime.RenderGenerationArtifacts(startCtx, renderRequest)
		if err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				return leaseErr
			}
			if leaseErr := leaseKeeper.ensureOwned(); leaseErr != nil {
				return leaseErr
			}
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		preparedArtifacts = artifactProjection.Artifacts
		if err := leaseKeeper.ensureOwned(); err != nil {
			return err
		}
		runtimeResourceParams, err := s.runtimeResourceInstanceParams(generationDetails, preparedArtifacts, resourceHostID)
		if err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				return leaseErr
			}
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		_, resourceIdentityDigest, err = store.RuntimeResourceIdentityForParams(runtimeResourceParams)
		if err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				return leaseErr
			}
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		contractPayload, err := s.sandboxContractPayload(session, generationDetails, preparedArtifacts, resourceIdentityDigest, dataVolumes, contentSnapshots)
		if err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				return leaseErr
			}
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		sandboxContractDigest = planprojection.SandboxContractPayloadDigest(contractPayload)
		inputEvidence, err := s.sandboxContractInputEvidenceFor(session, generationDetails.DriverID)
		if err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				return leaseErr
			}
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		if _, err := s.store.StoreSandboxContract(ctx, store.StoreSandboxContractParams{
			ContractID:             sandboxContractID(allocation.GenerationID),
			SessionID:              session.ID,
			GenerationID:           allocation.GenerationID,
			Owner:                  allocation.Owner,
			SandboxContractVersion: store.SandboxContractVersion,
			ContractSchemaVersion:  store.SandboxContractSchemaVersion,
			ContractGateVersion:    store.SandboxContractGateDriverManifest,
			DriverState:            allocation.DriverState,
			Payload:                contractPayload,
			RuntimeConfigDigest:    inputEvidence.RuntimeConfigDigest,
			RuntimeConfigPreimage:  inputEvidence.RuntimeConfigPreimage,
			AgentManifestDigest:    inputEvidence.AgentManifestDigest,
			AgentManifestPayload:   inputEvidence.AgentManifestPayload,
			Now:                    time.Now().UTC(),
		}); err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				return leaseErr
			}
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		if err := leaseKeeper.ensureOwned(); err != nil {
			return err
		}
		if err := s.storeShadowGenerationPlan(ctx, session, generationDetails, preparedArtifacts, contractPayload, resourceIdentityDigest, dataVolumes, contentSnapshots, inputEvidence); err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				return leaseErr
			}
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		if err := leaseKeeper.ensureOwned(); err != nil {
			return err
		}
		if _, err := s.verifyStoredGenerationPlanProjections(ctx, generationDetails, preparedArtifacts, sandboxContractDigest); err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				return leaseErr
			}
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		preparedArtifacts, err = s.generationPlanRuntimeArtifacts(ctx, allocation.GenerationID)
		if err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				return leaseErr
			}
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		artifactProjection.Artifacts = preparedArtifacts
		if err := leaseKeeper.ensureOwned(); err != nil {
			return err
		}
		instance, err := s.createRuntimeResourceInstance(ctx, runtimeResourceParams)
		if err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				return leaseErr
			}
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		runtimeResourceInstance = instance
		runtimeResourceCreated = true
		materializeNow := time.Now().UTC()
		if err := s.store.ClaimRuntimeResourceMaterialization(ctx, store.RuntimeResourceMaterializationClaimParams{
			GenerationID:     allocation.GenerationID,
			WorkerID:         resourceWorkerID,
			HostID:           resourceHostID,
			LeaseExpiresAt:   materializeNow.Add(s.cfg.Harness.Bridge.LeaseTTL.Duration),
			IdempotencyToken: "start:" + allocation.GenerationID,
			Now:              materializeNow,
		}); err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				return leaseErr
			}
			retireRuntimeResource()
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		if err := leaseKeeper.ensureOwned(); err != nil {
			retireRuntimeResource()
			return err
		}
		networkDetails := runtimeDetailsWithResourceInstance(generationDetails, runtimeResourceInstance)
		if err := s.verifyGenerationPlanMountPlanEvidence(ctx, allocation.GenerationID, networkDetails, dataVolumes, contentSnapshots); err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				return leaseErr
			}
			retireRuntimeResource()
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		if err := s.verifyGenerationPlanSandboxContractEvidence(ctx, allocation.GenerationID, session.ID); err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				return leaseErr
			}
			retireRuntimeResource()
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		materializeRequest := s.runtimeStartRequest(session, allocation.GenerationID, networkDetails, preparedArtifacts, dataVolumes, contentSnapshots)
		artifactProjection, err = s.runtime.RenderGenerationArtifacts(startCtx, materializeRequest)
		if err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				return leaseErr
			}
			retireRuntimeResource()
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		if _, err := s.verifyStoredGenerationPlanProjections(ctx, networkDetails, artifactProjection.Artifacts, sandboxContractDigest); err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				return leaseErr
			}
			retireRuntimeResource()
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		if err := leaseKeeper.ensureOwned(); err != nil {
			retireRuntimeResource()
			return err
		}
		if err := s.runtime.MaterializeGenerationArtifacts(materializeRequest, artifactProjection); err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				return leaseErr
			}
			retireRuntimeResource()
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		if err := leaseKeeper.ensureOwned(); err != nil {
			retireRuntimeResource()
			return err
		}
		if err := s.runtime.PrepareGenerationNetwork(startCtx, materializeRequest); err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				return leaseErr
			}
			retireRuntimeResource()
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		preparedArtifacts.NetworkPrepared = true
		if err := leaseKeeper.ensureOwned(); err != nil {
			retireRuntimeResource()
			return err
		}
		if err := s.store.RecordGenerationRuntimeArtifactDigests(ctx, allocation.GenerationID, runtimeArtifactDigests(preparedArtifacts)); err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				return leaseErr
			}
			retireRuntimeResource()
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		if err := leaseKeeper.ensureOwned(); err != nil {
			retireRuntimeResource()
			return err
		}
		if _, err := s.store.RecordSandboxContractArtifacts(ctx, store.RecordSandboxContractArtifactsParams{
			ContractID:            sandboxContractID(allocation.GenerationID),
			ControlManifestDigest: preparedArtifacts.ManifestDigest,
			OCISpecDigest:         preparedArtifacts.SpecDigest,
			BundleDigest:          preparedArtifacts.BundleDigest,
			Now:                   time.Now().UTC(),
		}); err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				return leaseErr
			}
			retireRuntimeResource()
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		if err := leaseKeeper.ensureOwned(); err != nil {
			retireRuntimeResource()
			return err
		}
		if err := s.store.MarkRuntimeResourceReady(ctx, store.RuntimeResourceWorkerTransitionParams{
			GenerationID: allocation.GenerationID,
			WorkerID:     resourceWorkerID,
			HostID:       resourceHostID,
			Now:          time.Now().UTC(),
		}); err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				return leaseErr
			}
			retireRuntimeResource()
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		if err := leaseKeeper.ensureOwned(); err != nil {
			retireRuntimeResource()
			return err
		}
		if err := s.store.MarkGenerationStarting(ctx, session.ID, allocation.GenerationID, allocation.Owner, time.Now().UTC()); err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				return leaseErr
			}
			retireRuntimeResource()
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		if err := leaseKeeper.ensureOwned(); err != nil {
			retireRuntimeResource()
			return err
		}
	}
	if ensured.RestoreFromCheckpoint {
		instance, resourceTracked, err := s.prepareRuntimeResourceRestore(ctx, allocation.GenerationID, resourceWorkerID, resourceHostID, s.cfg.Harness.Bridge.LeaseTTL.Duration)
		if err != nil {
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		runtimeResourceCreated = resourceTracked
		runtimeResourceInstance = instance
		resourceIdentityDigest = instance.ResourceIdentityDigest
	}
	if runtimeResourceCreated {
		generationDetails = runtimeDetailsWithResourceInstance(generationDetails, runtimeResourceInstance)
	}
	if err := validateDriverStateForRuntimeLaunch(generationDetails, dataVolumes); err != nil {
		if ensured.RestoreFromCheckpoint {
			retireRuntimeResource()
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		retireRuntimeResource()
		s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
		return err
	}
	if !ensured.IsNew {
		if _, err := s.verifyStoredGenerationPlanProjections(ctx, generationDetails, preparedArtifacts, sandboxContractDigest); err != nil {
			if ensured.RestoreFromCheckpoint {
				retireRuntimeResource()
				s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
				return err
			}
			retireRuntimeResource()
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
	}
	if err := s.verifyGenerationPlanNetworkEvidence(ctx, allocation.GenerationID, generationDetails); err != nil {
		retireRuntimeResource()
		s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
		return err
	}
	if err := s.verifyGenerationPlanRuntimeArtifactPaths(ctx, allocation.GenerationID, generationDetails); err != nil {
		retireRuntimeResource()
		s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
		return err
	}
	if err := s.verifyGenerationPlanRuntimeResourceEvidence(ctx, allocation.GenerationID, resourceIdentityDigest); err != nil {
		retireRuntimeResource()
		s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
		return err
	}
	if err := s.verifyGenerationPlanDataVolumes(ctx, allocation.GenerationID, dataVolumes); err != nil {
		retireRuntimeResource()
		s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
		return err
	}
	if !ensured.IsNew {
		if err := s.verifyGenerationPlanMountPlanEvidence(ctx, allocation.GenerationID, generationDetails, dataVolumes, contentSnapshots); err != nil {
			retireRuntimeResource()
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		if err := s.verifyGenerationPlanSandboxContractEvidence(ctx, allocation.GenerationID, session.ID); err != nil {
			retireRuntimeResource()
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
	}
	if err := s.verifyGenerationPlanSourceDigestEvidence(ctx, session.ID, allocation.GenerationID); err != nil {
		retireRuntimeResource()
		s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
		return err
	}
	if err := s.verifyGenerationPlanFrozenEvidenceForLaunch(ctx, allocation.GenerationID, generationDetails, preparedArtifacts, ensured.IsNew); err != nil {
		retireRuntimeResource()
		s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
		return err
	}
	startReq := s.runtimeStartRequest(session, allocation.GenerationID, generationDetails, preparedArtifacts, dataVolumes, contentSnapshots)
	startReq.RestoreFromCheckpoint = ensured.RestoreFromCheckpoint
	result := s.runtime.Start(startCtx, startReq, nil)
	if result.Err != nil {
		if leaseErr := leaseKeeper.err(); leaseErr != nil {
			return leaseErr
		}
		if leaseErr := leaseKeeper.ensureOwned(); leaseErr != nil {
			return leaseErr
		}
		if ensured.RestoreFromCheckpoint {
			retireRuntimeResource()
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, result.Err, failureMode)
			return result.Err
		}
		retireRuntimeResource()
		s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, result.Err, failureMode)
		return result.Err
	}
	if err := leaseKeeper.ensureOwned(); err != nil {
		if destroyErr := s.destroyGenerationRuntime(context.Background(), generationDetails); destroyErr != nil && !errors.Is(destroyErr, context.Canceled) {
			s.log.Warn("failed to destroy runtime after start lease loss", "session_id", session.ID, "generation_id", allocation.GenerationID, "error", destroyErr)
			return destroyErr
		}
		retireRuntimeResource()
		return err
	}
	bridgeStartupEvidence := ""
	if runtimeResourceCreated {
		renewRuntimeResourceWorkerLease := func() error {
			now := time.Now().UTC()
			return s.store.RenewRuntimeResourceWorkerLease(ctx, store.RuntimeResourceWorkerLeaseRenewalParams{
				GenerationID:   allocation.GenerationID,
				WorkerID:       resourceWorkerID,
				HostID:         resourceHostID,
				LeaseExpiresAt: now.Add(s.cfg.Harness.Bridge.LeaseTTL.Duration),
				Now:            now,
			})
		}
		if err := renewRuntimeResourceWorkerLease(); err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				return leaseErr
			}
			if destroyErr := s.destroyGenerationRuntime(ctx, generationDetails); destroyErr != nil && !errors.Is(destroyErr, context.Canceled) {
				s.log.Warn("failed to destroy runtime after resource lease renewal failure", "session_id", session.ID, "generation_id", allocation.GenerationID, "error", destroyErr)
				return destroyErr
			}
			retireRuntimeResource()
			if ensured.RestoreFromCheckpoint {
				s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			} else {
				s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			}
			return err
		}
		bridgeStartupEvidence, err = s.waitForBridgeStartupReadiness(startCtx, allocation, runtimeResourceInstance)
		if err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				if destroyErr := s.destroyGenerationRuntime(context.Background(), generationDetails); destroyErr != nil && !errors.Is(destroyErr, context.Canceled) {
					s.log.Warn("failed to destroy runtime after bridge startup lease loss", "session_id", session.ID, "generation_id", allocation.GenerationID, "error", destroyErr)
					return destroyErr
				}
				retireRuntimeResource()
				return leaseErr
			}
			if leaseErr := leaseKeeper.ensureOwned(); leaseErr != nil {
				if destroyErr := s.destroyGenerationRuntime(context.Background(), generationDetails); destroyErr != nil && !errors.Is(destroyErr, context.Canceled) {
					s.log.Warn("failed to destroy runtime after bridge startup lease loss", "session_id", session.ID, "generation_id", allocation.GenerationID, "error", destroyErr)
					return destroyErr
				}
				retireRuntimeResource()
				return leaseErr
			}
			if destroyErr := s.destroyGenerationRuntime(ctx, generationDetails); destroyErr != nil && !errors.Is(destroyErr, context.Canceled) {
				s.log.Warn("failed to destroy runtime after bridge startup probe failure", "session_id", session.ID, "generation_id", allocation.GenerationID, "error", destroyErr)
				return destroyErr
			}
			retireRuntimeResource()
			if ensured.RestoreFromCheckpoint {
				s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			} else {
				s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			}
			return err
		}
		if err := renewRuntimeResourceWorkerLease(); err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				return leaseErr
			}
			if destroyErr := s.destroyGenerationRuntime(ctx, generationDetails); destroyErr != nil && !errors.Is(destroyErr, context.Canceled) {
				s.log.Warn("failed to destroy runtime after resource lease renewal failure", "session_id", session.ID, "generation_id", allocation.GenerationID, "error", destroyErr)
				return destroyErr
			}
			retireRuntimeResource()
			if ensured.RestoreFromCheckpoint {
				s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			} else {
				s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			}
			return err
		}
		if err := leaseKeeper.ensureOwned(); err != nil {
			if destroyErr := s.destroyGenerationRuntime(context.Background(), generationDetails); destroyErr != nil && !errors.Is(destroyErr, context.Canceled) {
				s.log.Warn("failed to destroy runtime after bridge startup lease loss", "session_id", session.ID, "generation_id", allocation.GenerationID, "error", destroyErr)
				return destroyErr
			}
			retireRuntimeResource()
			return err
		}
	}
	if ensured.IsNew {
		postStartProof, err := runtimeResourcePostStartProof(runtimeResourceInstance, result, bridgeStartupEvidence)
		if err != nil {
			if destroyErr := s.destroyGenerationRuntime(ctx, generationDetails); destroyErr != nil && !errors.Is(destroyErr, context.Canceled) {
				s.log.Warn("failed to destroy runtime after missing post-start proof", "session_id", session.ID, "generation_id", allocation.GenerationID, "error", destroyErr)
				return destroyErr
			}
			retireRuntimeResource()
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		if err := s.store.MarkRuntimeResourceLive(ctx, store.RuntimeResourceWorkerTransitionParams{
			GenerationID: allocation.GenerationID,
			WorkerID:     resourceWorkerID,
			HostID:       resourceHostID,
			PostStart:    &postStartProof,
			Now:          time.Now().UTC(),
		}); err != nil {
			if destroyErr := s.destroyGenerationRuntime(ctx, generationDetails); destroyErr != nil && !errors.Is(destroyErr, context.Canceled) {
				s.log.Warn("failed to destroy runtime after resource live CAS failure", "session_id", session.ID, "generation_id", allocation.GenerationID, "error", destroyErr)
				return destroyErr
			}
			retireRuntimeResource()
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		if err := leaseKeeper.ensureOwned(); err != nil {
			if destroyErr := s.destroyGenerationRuntime(context.Background(), generationDetails); destroyErr != nil && !errors.Is(destroyErr, context.Canceled) {
				s.log.Warn("failed to destroy runtime after resource live lease loss", "session_id", session.ID, "generation_id", allocation.GenerationID, "error", destroyErr)
				return destroyErr
			}
			retireRuntimeResource()
			return err
		}
		if err := s.store.MarkGenerationResourcesLive(ctx, session.ID, allocation.GenerationID, allocation.Owner, time.Now().UTC()); err != nil {
			if destroyErr := s.destroyGenerationRuntime(ctx, generationDetails); destroyErr != nil && !errors.Is(destroyErr, context.Canceled) {
				s.log.Warn("failed to destroy runtime after generation live CAS failure", "session_id", session.ID, "generation_id", allocation.GenerationID, "error", destroyErr)
				return destroyErr
			}
			retireRuntimeResource()
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		if err := leaseKeeper.ensureOwned(); err != nil {
			if destroyErr := s.destroyGenerationRuntime(context.Background(), generationDetails); destroyErr != nil && !errors.Is(destroyErr, context.Canceled) {
				s.log.Warn("failed to destroy runtime after generation live lease loss", "session_id", session.ID, "generation_id", allocation.GenerationID, "error", destroyErr)
				return destroyErr
			}
			retireRuntimeResource()
			return err
		}
	}
	if ensured.RestoreFromCheckpoint {
		if runtimeResourceCreated {
			postStartProof, err := runtimeResourcePostStartProof(runtimeResourceInstance, result, bridgeStartupEvidence)
			if err != nil {
				if destroyErr := s.destroyGenerationRuntime(ctx, generationDetails); destroyErr != nil && !errors.Is(destroyErr, context.Canceled) {
					s.log.Warn("failed to destroy runtime after restore missing post-start proof", "session_id", session.ID, "generation_id", allocation.GenerationID, "error", destroyErr)
					return destroyErr
				}
				retireRuntimeResource()
				if leaseErr := leaseKeeper.ensureOwned(); leaseErr != nil {
					return leaseErr
				}
				s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
				return err
			}
			if err := s.store.MarkRuntimeResourceLive(ctx, store.RuntimeResourceWorkerTransitionParams{
				GenerationID: allocation.GenerationID,
				WorkerID:     resourceWorkerID,
				HostID:       resourceHostID,
				PostStart:    &postStartProof,
				Now:          time.Now().UTC(),
			}); err != nil {
				if destroyErr := s.destroyGenerationRuntime(ctx, generationDetails); destroyErr != nil && !errors.Is(destroyErr, context.Canceled) {
					s.log.Warn("failed to destroy runtime after restore resource live CAS failure", "session_id", session.ID, "generation_id", allocation.GenerationID, "error", destroyErr)
					return destroyErr
				}
				retireRuntimeResource()
				if leaseErr := leaseKeeper.ensureOwned(); leaseErr != nil {
					return leaseErr
				}
				s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
				return err
			}
		}
		if err := s.store.MarkGenerationResourcesLive(ctx, session.ID, allocation.GenerationID, allocation.Owner, time.Now().UTC()); err != nil {
			if destroyErr := s.destroyGenerationRuntime(ctx, generationDetails); destroyErr != nil && !errors.Is(destroyErr, context.Canceled) {
				s.log.Warn("failed to destroy runtime after restore live CAS failure", "session_id", session.ID, "generation_id", allocation.GenerationID, "error", destroyErr)
				return destroyErr
			}
			retireRuntimeResource()
			if leaseErr := leaseKeeper.ensureOwned(); leaseErr != nil {
				return leaseErr
			}
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		if err := leaseKeeper.ensureOwned(); err != nil {
			if destroyErr := s.destroyGenerationRuntime(context.Background(), generationDetails); destroyErr != nil && !errors.Is(destroyErr, context.Canceled) {
				s.log.Warn("failed to destroy runtime after restore start lease loss", "session_id", session.ID, "generation_id", allocation.GenerationID, "error", destroyErr)
				return destroyErr
			}
			return err
		}
	}
	return nil
}
