package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"harness-platform/orchestrator/internal/runtime"
	"harness-platform/orchestrator/internal/store"
)

func (s *Server) runtimeResourceInstanceParams(details store.RuntimeGenerationDetails, artifacts runtime.GenerationArtifacts, hostID string) (store.RuntimeResourceInstanceParams, error) {
	runscPlatform := strings.TrimSpace(details.RunscPlatform)
	if runscPlatform == "" {
		return store.RuntimeResourceInstanceParams{}, fmt.Errorf("runsc platform is required")
	}
	sandboxIP, err := runtimeResourceSandboxIP(details.SandboxIPCIDR)
	if err != nil {
		return store.RuntimeResourceInstanceParams{}, err
	}
	nftTableName, err := runtimeResourceNftTableName(details.GenerationID)
	if err != nil {
		return store.RuntimeResourceInstanceParams{}, err
	}
	return store.RuntimeResourceInstanceParams{
		GenerationID:           details.GenerationID,
		SessionID:              details.SessionID,
		ContractID:             sandboxContractID(details.GenerationID),
		SandboxContractVersion: store.SandboxContractVersion,
		HostID:                 hostID,
		RunscContainerID:       details.RunscContainerID,
		RunscPlatform:          runscPlatform,
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
		RootPrefixes:           s.runtimeResourceRootPrefixes(),
		Now:                    time.Now().UTC(),
	}, nil
}

func (s *Server) createRuntimeResourceInstance(ctx context.Context, params store.RuntimeResourceInstanceParams) (store.RuntimeResourceInstance, error) {
	return s.store.CreateRuntimeResourceInstance(ctx, params)
}

func (s *Server) prepareRuntimeResourceRestore(ctx context.Context, generationID, workerID, hostID string, leaseTTL time.Duration) (store.RuntimeResourceInstance, bool, error) {
	if _, err := s.store.GetRuntimeResourceInstance(ctx, generationID); errors.Is(err, sql.ErrNoRows) {
		return store.RuntimeResourceInstance{}, false, fmt.Errorf("runtime resource instance is required for checkpoint restore")
	} else if err != nil {
		return store.RuntimeResourceInstance{}, false, err
	}
	now := time.Now().UTC()
	if err := s.store.ClaimRuntimeResourceCheckpointRestore(ctx, store.RuntimeResourceMaterializationClaimParams{
		GenerationID:     generationID,
		WorkerID:         workerID,
		HostID:           hostID,
		LeaseExpiresAt:   now.Add(leaseTTL),
		IdempotencyToken: "restore:" + generationID,
		Now:              now,
	}); err != nil {
		return store.RuntimeResourceInstance{}, true, err
	}
	if err := s.store.MarkRuntimeResourceReady(ctx, store.RuntimeResourceWorkerTransitionParams{
		GenerationID: generationID,
		WorkerID:     workerID,
		HostID:       hostID,
		Now:          time.Now().UTC(),
	}); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.store.ClaimRuntimeResourceRetiring(cleanupCtx, store.RuntimeResourceRetireParams{
			GenerationID: generationID,
			WorkerID:     workerID,
			HostID:       hostID,
			Now:          time.Now().UTC(),
		})
		return store.RuntimeResourceInstance{}, true, err
	}
	instance, err := s.store.GetRuntimeResourceInstance(ctx, generationID)
	if err != nil {
		return store.RuntimeResourceInstance{}, true, err
	}
	return instance, true, nil
}

func (s *Server) reserveRuntimeResourceCheckpoint(ctx context.Context, generationID string) error {
	instance, err := s.store.GetRuntimeResourceInstance(ctx, generationID)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("runtime resource instance is required for checkpoint reserve")
	}
	if err != nil {
		return err
	}
	return s.store.ReserveRuntimeResourceCheckpoint(ctx, store.RuntimeResourceWorkerTransitionParams{
		GenerationID: generationID,
		WorkerID:     instance.WorkerID,
		HostID:       instance.HostID,
		Now:          time.Now().UTC(),
	})
}

func (s *Server) claimRuntimeResourceCleanup(ctx context.Context, instance store.RuntimeResourceInstance, now time.Time) error {
	switch instance.State {
	case store.RuntimeResourceRetiring, store.RuntimeResourceReconciling, store.RuntimeResourceAbsentVerified, store.RuntimeResourceDestroyed:
		return nil
	}
	if err := s.store.ClaimRuntimeResourceRetiring(ctx, store.RuntimeResourceRetireParams{
		GenerationID: instance.GenerationID,
		WorkerID:     instance.WorkerID,
		HostID:       instance.HostID,
		Now:          now,
	}); err != nil {
		return fmt.Errorf("claim runtime resource retiring: %w", err)
	}
	return nil
}

func (s *Server) completeRuntimeResourceCleanup(ctx context.Context, instance store.RuntimeResourceInstance, cleanup runtime.GenerationResourceCleanup, now time.Time) error {
	evidence := runtimeResourceCleanupEvidence(instance, cleanup)
	current, err := s.store.GetRuntimeResourceCleanupIdentity(ctx, instance.GenerationID)
	if err != nil {
		return err
	}
	switch current.State {
	case store.RuntimeResourceDestroyed:
		return nil
	case store.RuntimeResourceAbsentVerified:
	case store.RuntimeResourceReconciling:
	default:
		if err := s.store.MarkRuntimeResourceReconciling(ctx, store.RuntimeResourceEvidenceParams{
			GenerationID: instance.GenerationID,
			WorkerID:     current.WorkerID,
			HostID:       instance.HostID,
			Evidence:     evidence,
			Now:          now,
		}); err != nil {
			return err
		}
	}
	current, err = s.store.GetRuntimeResourceCleanupIdentity(ctx, instance.GenerationID)
	if err != nil {
		return err
	}
	if current.State == store.RuntimeResourceReconciling {
		if err := s.store.MarkRuntimeResourceAbsentVerified(ctx, store.RuntimeResourceEvidenceParams{
			GenerationID: instance.GenerationID,
			WorkerID:     current.WorkerID,
			HostID:       instance.HostID,
			Evidence:     evidence,
			Now:          now,
		}); err != nil {
			return err
		}
	}
	if err := s.store.MarkRuntimeResourceDestroyed(ctx, store.RuntimeResourceRetireParams{
		GenerationID: instance.GenerationID,
		WorkerID:     current.WorkerID,
		HostID:       instance.HostID,
		Now:          now,
	}); err != nil {
		return err
	}
	return nil
}

func runtimeDetailsWithResourceInstance(details store.RuntimeGenerationDetails, instance store.RuntimeResourceInstance) store.RuntimeGenerationDetails {
	details.RunscContainerID = instance.RunscContainerID
	details.RunscPlatform = instance.RunscPlatform
	details.RunscVersion = instance.RunscVersion
	details.RunscBinaryPath = instance.RunscBinaryPath
	details.RunscBinaryDigest = instance.RunscBinaryDigest
	details.NetworkProfileID = instance.NetworkProfileID
	details.NetnsName = instance.NetnsName
	details.NetnsPath = instance.NetnsPath
	details.HostVeth = instance.HostVeth
	details.SandboxVeth = instance.SandboxVeth
	details.HostGatewayIP = instance.HostGatewayIP
	details.SandboxIPCIDR = instance.SandboxIPCIDR
	details.HostSideCIDR = instance.HostSideCIDR
	details.NftTableName = instance.NftTableName
	details.ControlDirPath = instance.ControlDirPath
	details.ControlManifestPath = instance.ControlManifestPath
	details.BundleDirPath = instance.BundleDirPath
	details.SpecPath = instance.SpecPath
	details.CheckpointPath = instance.CheckpointPath
	details.BridgeDirPath = instance.BridgeDirPath
	details.NetworkHostsPath = instance.NetworkHostsPath
	details.LogDirPath = instance.LogDirPath
	return details
}
