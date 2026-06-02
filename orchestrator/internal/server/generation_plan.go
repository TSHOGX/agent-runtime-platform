package server

import (
	"context"
	"fmt"
	"strings"
	"time"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/generationplan"
	"harness-platform/orchestrator/internal/planprojection"
	"harness-platform/orchestrator/internal/runtime"
	"harness-platform/orchestrator/internal/store"
)

func (s *Server) storeShadowGenerationPlan(ctx context.Context, session store.Session, details store.RuntimeGenerationDetails, artifacts runtime.GenerationArtifacts, sandboxContractPayload map[string]any, resourceIdentityDigest string, volumes sessionRuntimeDataVolumes, contentSnapshots []store.ContentSnapshotRecord, inputEvidence sandboxContractInputEvidence) error {
	payload, err := s.shadowGenerationPlanPayload(session, details, artifacts, sandboxContractPayload, resourceIdentityDigest, volumes, contentSnapshots, inputEvidence)
	if err != nil {
		return err
	}
	if err := generationplan.Validate(generationplan.ValidateParams{Payload: payload}); err != nil {
		return err
	}
	plan, err := s.store.StoreGenerationPlan(ctx, store.StoreGenerationPlanParams{
		GenerationID: details.GenerationID,
		PlanVersion:  store.GenerationPlanVersion,
		Payload:      payload,
		Now:          time.Now().UTC(),
	})
	if err != nil {
		return err
	}
	for _, projection := range planprojection.Rows(details, artifacts, sandboxContractPayload, plan.PlanDigest) {
		if _, err := s.store.StoreGenerationPlanProjection(ctx, projection); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) shadowGenerationPlanPayload(session store.Session, details store.RuntimeGenerationDetails, artifacts runtime.GenerationArtifacts, sandboxContractPayload map[string]any, resourceIdentityDigest string, volumes sessionRuntimeDataVolumes, contentSnapshots []store.ContentSnapshotRecord, inputEvidence sandboxContractInputEvidence) (map[string]any, error) {
	driverID := strings.TrimSpace(details.DriverID)
	if driverID == "" {
		return nil, fmt.Errorf("generation plan driver id is required")
	}
	driverSpec, ok := agents.DriverSpecFor(driverID)
	if !ok {
		return nil, fmt.Errorf("unsupported driver %q", driverID)
	}
	mode := strings.TrimSpace(session.Mode)
	if mode == "" {
		return nil, fmt.Errorf("session mode is required")
	}
	deployment, capabilityErr := s.resolveDriverDeployment(mode, agents.ID(driverID))
	if capabilityErr != nil {
		return nil, capabilityErr
	}
	providerSpec := deployment.ProviderSpec
	sandboxIP, err := runtimeResourceSandboxIP(details.SandboxIPCIDR)
	if err != nil {
		return nil, err
	}
	nftTableName, err := runtimeResourceNftTableName(details.GenerationID)
	if err != nil {
		return nil, err
	}
	return generationplan.RenderPayload(generationplan.RenderPayloadParams{
		Session:                      session,
		Details:                      details,
		Artifacts:                    artifacts,
		SandboxContractPayload:       sandboxContractPayload,
		SandboxContractPayloadDigest: planprojection.SandboxContractPayloadDigest(sandboxContractPayload),
		ResourceIdentityDigest:       resourceIdentityDigest,
		Volumes: generationplan.DataVolumes{
			Workspace:  volumes.Workspace,
			DriverHome: volumes.DriverHome,
		},
		DriverSpec:                  driverSpec,
		ProviderSpec:                providerSpec,
		RuntimeProviderConfigID:     deployment.RuntimeProviderConfigID,
		RootFSPath:                  s.cfg.RootFSPath,
		SandboxIP:                   sandboxIP,
		NetworkIdentityNftTableName: nftTableName,
		BridgeProbe: generationplan.BridgeProbePayload{
			BridgeHeartbeatInterval: s.cfg.Harness.Bridge.HeartbeatInterval.Duration,
			BridgePollInterval:      s.cfg.Harness.Bridge.PollInterval.Duration,
			LeaseTTL:                s.cfg.Harness.Bridge.LeaseTTL.Duration,
			AckStartedGrace:         s.cfg.Harness.Bridge.AckStartedGrace.Duration,
			ReconnectGrace:          s.cfg.Harness.Bridge.ReconnectGrace.Duration,
			ProbeHealthzStatuses:    s.cfg.Harness.Probe.AcceptStatus.GetHealthz,
			PreStartAttempts:        s.cfg.Harness.Probe.PreStartAttempts,
			PreStartInterval:        s.cfg.Harness.Probe.PreStartInterval.Duration,
			PostStartAttempts:       s.cfg.Harness.Probe.PostStartAttempts,
			PostStartInterval:       s.cfg.Harness.Probe.PostStartInterval.Duration,
		},
		ContentSnapshots: contentSnapshots,
		SourceDigests: generationplan.SourceDigests{
			RuntimeConfigDigest: inputEvidence.RuntimeConfigDigest,
			AgentManifestDigest: inputEvidence.AgentManifestDigest,
		},
		SandboxContractCompatibility: store.SandboxContractVersion,
		SandboxContractID:            sandboxContractID(details.GenerationID),
	})
}

func runtimeArtifactsFromDetails(details store.RuntimeGenerationDetails) runtime.GenerationArtifacts {
	return runtime.GenerationArtifacts{
		BundleDir:               details.BundleDirPath,
		SpecPath:                details.SpecPath,
		ManifestPath:            details.ControlManifestPath,
		ManifestDigest:          details.ControlManifestDigest,
		ProjectedManifestDigest: details.ProjectedControlManifestDigest,
		BundleDigest:            details.BundleDigest,
		RuntimeConfigDigest:     details.RuntimeConfigDigest,
		SpecDigest:              details.SpecDigest,
		RunscVersion:            details.RunscVersion,
		RunscBinaryPath:         details.RunscBinaryPath,
		RunscBinaryDigest:       details.RunscBinaryDigest,
	}
}

func (s *Server) generationPlanRuntimeArtifacts(ctx context.Context, generationID string) (runtime.GenerationArtifacts, error) {
	plan, err := s.store.RequireGenerationPlanForLaunch(ctx, strings.TrimSpace(generationID))
	if err != nil {
		return runtime.GenerationArtifacts{}, err
	}
	if err := generationplan.Validate(generationplan.ValidateParams{Payload: plan.CanonicalPayload}); err != nil {
		return runtime.GenerationArtifacts{}, err
	}
	return generationplan.RuntimeArtifacts(plan.CanonicalPayload)
}

func runtimeArtifactDigests(artifacts runtime.GenerationArtifacts) store.GenerationRuntimeArtifactDigests {
	return store.GenerationRuntimeArtifactDigests{
		ControlManifestDigest:          artifacts.ManifestDigest,
		ProjectedControlManifestDigest: artifacts.ProjectedManifestDigest,
		BundleDigest:                   artifacts.BundleDigest,
		RuntimeConfigDigest:            artifacts.RuntimeConfigDigest,
		SpecDigest:                     artifacts.SpecDigest,
		RunscVersion:                   artifacts.RunscVersion,
		RunscBinaryPath:                artifacts.RunscBinaryPath,
		RunscBinaryDigest:              artifacts.RunscBinaryDigest,
	}
}
