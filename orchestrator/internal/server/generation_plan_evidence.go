package server

import (
	"context"
	"strings"

	"harness-platform/orchestrator/internal/generationplan"
	"harness-platform/orchestrator/internal/runtime"
	"harness-platform/orchestrator/internal/store"
)

func (s *Server) verifyGenerationPlanNetworkEvidence(ctx context.Context, generationID string, details store.RuntimeGenerationDetails) error {
	plan, err := s.store.RequireGenerationPlanForLaunch(ctx, strings.TrimSpace(generationID))
	if err != nil {
		return err
	}
	if err := generationplan.Validate(generationplan.ValidateParams{Payload: plan.CanonicalPayload}); err != nil {
		return err
	}
	sandboxIP, err := runtimeResourceSandboxIP(details.SandboxIPCIDR)
	if err != nil {
		return err
	}
	nftTableName, err := runtimeResourceNftTableName(details.GenerationID)
	if err != nil {
		return err
	}
	return generationplan.VerifyNetworkEvidence(generationplan.VerifyNetworkEvidenceParams{
		Payload:            plan.CanonicalPayload,
		NetworkProfileID:   details.NetworkProfileID,
		RunscNetwork:       details.RunscNetwork,
		RunscOverlay2:      details.RunscOverlay2,
		SandboxIP:          sandboxIP,
		SandboxIPCIDR:      details.SandboxIPCIDR,
		HostGatewayIP:      details.HostGatewayIP,
		SandboxBaseURL:     details.SandboxBaseURL,
		HostProxyBindURL:   details.HostProxyBindURL,
		ProxyPort:          details.ProxyPort,
		NetnsName:          details.NetnsName,
		NetnsPath:          details.NetnsPath,
		HostVeth:           details.HostVeth,
		SandboxVeth:        details.SandboxVeth,
		HostSideCIDR:       details.HostSideCIDR,
		NftTableName:       nftTableName,
		EgressPolicyID:     details.EgressPolicyID,
		EgressPolicyDigest: details.EgressPolicyDigest,
		DNSPolicy:          details.DNSPolicy,
	})
}

func (s *Server) verifyGenerationPlanRuntimeArtifactPaths(ctx context.Context, generationID string, details store.RuntimeGenerationDetails) error {
	plan, err := s.store.RequireGenerationPlanForLaunch(ctx, strings.TrimSpace(generationID))
	if err != nil {
		return err
	}
	if err := generationplan.Validate(generationplan.ValidateParams{Payload: plan.CanonicalPayload}); err != nil {
		return err
	}
	return generationplan.VerifyRuntimeArtifactPathEvidence(generationplan.VerifyRuntimeArtifactPathEvidenceParams{
		Payload:             plan.CanonicalPayload,
		ControlDirPath:      details.ControlDirPath,
		ControlManifestPath: details.ControlManifestPath,
		BundleDirPath:       details.BundleDirPath,
		SpecPath:            details.SpecPath,
		BridgeDirPath:       details.BridgeDirPath,
		LogDirPath:          details.LogDirPath,
		NetworkHostsPath:    details.NetworkHostsPath,
	})
}

func (s *Server) verifyGenerationPlanRuntimeResourceEvidence(ctx context.Context, generationID, resourceIdentityDigest string) error {
	resourceIdentityDigest = strings.TrimSpace(resourceIdentityDigest)
	if resourceIdentityDigest == "" {
		return nil
	}
	plan, err := s.store.RequireGenerationPlanForLaunch(ctx, strings.TrimSpace(generationID))
	if err != nil {
		return err
	}
	if err := generationplan.Validate(generationplan.ValidateParams{Payload: plan.CanonicalPayload}); err != nil {
		return err
	}
	return generationplan.VerifyRuntimeResourceEvidence(generationplan.VerifyRuntimeResourceEvidenceParams{
		Payload:                plan.CanonicalPayload,
		ResourceIdentityDigest: resourceIdentityDigest,
	})
}

func (s *Server) verifyGenerationPlanDataVolumes(ctx context.Context, generationID string, volumes sessionRuntimeDataVolumes) error {
	plan, err := s.store.RequireGenerationPlanForLaunch(ctx, strings.TrimSpace(generationID))
	if err != nil {
		return err
	}
	if err := generationplan.Validate(generationplan.ValidateParams{Payload: plan.CanonicalPayload}); err != nil {
		return err
	}
	return generationplan.VerifyDataVolumeEvidence(generationplan.VerifyDataVolumeEvidenceParams{
		Payload:                         plan.CanonicalPayload,
		WorkspaceHostPath:               volumes.Workspace.HostPath,
		WorkspaceRuntimeIdentityDigest:  volumes.Workspace.RuntimeIdentityDigest,
		DriverHomeHostPath:              volumes.DriverHome.HostPath,
		DriverHomeRuntimeIdentityDigest: volumes.DriverHome.RuntimeIdentityDigest,
	})
}

func (s *Server) verifyGenerationPlanMountPlanEvidence(ctx context.Context, generationID string, details store.RuntimeGenerationDetails, volumes sessionRuntimeDataVolumes, contentSnapshots []store.ContentSnapshotRecord) error {
	plan, err := s.store.RequireGenerationPlanForLaunch(ctx, strings.TrimSpace(generationID))
	if err != nil {
		return err
	}
	if err := generationplan.Validate(generationplan.ValidateParams{Payload: plan.CanonicalPayload}); err != nil {
		return err
	}
	mountPlan, err := runtime.BuildSandboxMountPlan(runtime.SandboxMountPlanInputs{
		Generation:        details,
		WorkspaceHostPath: volumes.Workspace.HostPath,
		AgentHomeHostPath: volumes.DriverHome.HostPath,
		NetworkHostsPath:  details.NetworkHostsPath,
		ContentSnapshots:  contentSnapshots,
	})
	if err != nil {
		return err
	}
	return generationplan.VerifyMountPlanEvidence(generationplan.VerifyMountPlanEvidenceParams{
		Payload:   plan.CanonicalPayload,
		MountPlan: mountPlan,
	})
}

func (s *Server) verifyGenerationPlanSandboxContractEvidence(ctx context.Context, generationID, sessionID string) error {
	generationID = strings.TrimSpace(generationID)
	plan, err := s.store.RequireGenerationPlanForLaunch(ctx, generationID)
	if err != nil {
		return err
	}
	if err := generationplan.Validate(generationplan.ValidateParams{Payload: plan.CanonicalPayload}); err != nil {
		return err
	}
	contract, err := s.store.GetSandboxContractForGeneration(ctx, strings.TrimSpace(sessionID), generationID)
	if err != nil {
		return err
	}
	projectionDigests, _, err := s.storedGenerationPlanProjectionEvidence(ctx, generationID, plan.PlanDigest)
	if err != nil {
		return err
	}
	return generationplan.VerifySandboxContractEvidence(generationplan.VerifySandboxContractEvidenceParams{
		Payload:          plan.CanonicalPayload,
		ContractID:       contract.ContractID,
		ContractDigest:   contract.SandboxContractDigest,
		ProjectionDigest: projectionDigests[store.GenerationPlanProjectionSandboxContract],
	})
}

func (s *Server) verifyGenerationPlanSourceDigestEvidence(ctx context.Context, sessionID, generationID string) error {
	generationID = strings.TrimSpace(generationID)
	plan, err := s.store.RequireGenerationPlanForLaunch(ctx, generationID)
	if err != nil {
		return err
	}
	if err := generationplan.Validate(generationplan.ValidateParams{Payload: plan.CanonicalPayload}); err != nil {
		return err
	}
	inputEvidence, err := s.store.GetSandboxContractInputEvidence(ctx, sandboxContractID(generationID))
	if err != nil {
		return err
	}
	contract, err := s.store.GetSandboxContractForGeneration(ctx, strings.TrimSpace(sessionID), generationID)
	if err != nil {
		return err
	}
	adapterInputDigests, err := generationplan.AdapterInputDigestsFromSandboxContract(contract.CanonicalPayload)
	if err != nil {
		return err
	}
	return generationplan.VerifySourceDigestEvidence(generationplan.VerifySourceDigestEvidenceParams{
		Payload:             plan.CanonicalPayload,
		RuntimeConfigDigest: inputEvidence.RuntimeConfigDigest,
		AgentManifestDigest: inputEvidence.AgentManifestDigest,
		AdapterInputDigests: adapterInputDigests,
	})
}

func (s *Server) verifyGenerationPlanFrozenEvidence(ctx context.Context, generationID string, details store.RuntimeGenerationDetails, artifacts runtime.GenerationArtifacts) error {
	return s.verifyGenerationPlanFrozenEvidenceForLaunch(ctx, generationID, details, artifacts, false)
}

func (s *Server) verifyGenerationPlanFrozenEvidenceForLaunch(ctx context.Context, generationID string, details store.RuntimeGenerationDetails, artifacts runtime.GenerationArtifacts, verifyBootstrapDriverState bool) error {
	plan, err := s.store.RequireGenerationPlanForLaunch(ctx, strings.TrimSpace(generationID))
	if err != nil {
		return err
	}
	if err := generationplan.Validate(generationplan.ValidateParams{Payload: plan.CanonicalPayload}); err != nil {
		return err
	}
	projectionDigests, projectionVersions, err := s.storedGenerationPlanProjectionEvidence(ctx, generationID, plan.PlanDigest)
	if err != nil {
		return err
	}
	contentSnapshotDigests, err := s.generationPlanContentSnapshotDigests(ctx, plan.CanonicalPayload)
	if err != nil {
		return err
	}
	runscVersion, runscBinaryPath, runscBinaryDigest := generationPlanRunscEvidence(details, artifacts)
	params := generationplan.VerifyFrozenEvidenceParams{
		Payload:                         plan.CanonicalPayload,
		SessionID:                       details.SessionID,
		GenerationID:                    details.GenerationID,
		DriverID:                        details.DriverID,
		OutputFormat:                    details.OutputFormat,
		NetworkProfileID:                details.NetworkProfileID,
		AgentRuntimeProfileID:           details.AgentRuntimeProfileID,
		RunscPlatform:                   details.RunscPlatform,
		RunscVersion:                    runscVersion,
		RunscBinaryPath:                 runscBinaryPath,
		RunscBinaryDigest:               runscBinaryDigest,
		ProjectionDigests:               projectionDigests,
		ProjectionVersions:              projectionVersions,
		ContentSnapshotDigests:          contentSnapshotDigests,
		CheckpointBundleDigest:          generationplan.OptionalProjectionPayloadDigest(store.GenerationPlanProjectionBundle, details.CheckpointBundleDigest),
		CheckpointRuntimeConfigDigest:   generationplan.OptionalProjectionPayloadDigest(store.GenerationPlanProjectionRuntimeConfig, details.CheckpointRuntimeConfigDigest),
		CheckpointControlManifestDigest: generationplan.OptionalProjectionPayloadDigest(store.GenerationPlanProjectionControlManifestProjected, details.CheckpointControlManifestDigest),
		CheckpointDriverStatesDigest:    details.CheckpointDriverStatesDigest,
		CheckpointPlanDigest:            details.CheckpointPlanDigest,
	}
	if verifyBootstrapDriverState {
		params.DriverStateDigest = details.DriverStateDigest
		params.DriverStateVersion = details.DriverStateVersion
	}
	return generationplan.VerifyFrozenEvidence(params)
}

func generationPlanRunscEvidence(details store.RuntimeGenerationDetails, artifacts runtime.GenerationArtifacts) (string, string, string) {
	if strings.TrimSpace(details.RunscVersion) == "" &&
		strings.TrimSpace(details.RunscBinaryPath) == "" &&
		strings.TrimSpace(details.RunscBinaryDigest) == "" {
		return artifacts.RunscVersion, artifacts.RunscBinaryPath, artifacts.RunscBinaryDigest
	}
	return details.RunscVersion, details.RunscBinaryPath, details.RunscBinaryDigest
}
