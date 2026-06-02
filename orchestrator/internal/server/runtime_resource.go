package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
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

func runtimeResourcePostStartProof(instance store.RuntimeResourceInstance, result runtime.Result, bridgeStartupEvidence string) (store.RuntimeResourcePostStartProof, error) {
	if result.PostStartProof == nil {
		return store.RuntimeResourcePostStartProof{}, fmt.Errorf("runtime start did not return post-start proof for generation %s", instance.GenerationID)
	}
	proof := *result.PostStartProof
	if err := validateRuntimeResourcePostStartProof(instance, proof); err != nil {
		return store.RuntimeResourcePostStartProof{}, err
	}
	proof.HostID = instance.HostID
	proof.ContractID = instance.ContractID
	proof.SandboxContractVersion = instance.SandboxContractVersion
	proof.BridgeStartup = strings.TrimSpace(bridgeStartupEvidence)
	if strings.TrimSpace(proof.GenerationID) == "" {
		proof.GenerationID = instance.GenerationID
	}
	if strings.TrimSpace(proof.RunscContainerID) == "" {
		proof.RunscContainerID = instance.RunscContainerID
	}
	return proof, nil
}

func validateRuntimeResourcePostStartProof(instance store.RuntimeResourceInstance, proof store.RuntimeResourcePostStartProof) error {
	checks := []struct {
		label    string
		got      string
		want     string
		required bool
	}{
		{"host_id", proof.HostID, instance.HostID, false},
		{"generation_id", proof.GenerationID, instance.GenerationID, true},
		{"contract_id", proof.ContractID, instance.ContractID, false},
		{"sandbox_contract_version", proof.SandboxContractVersion, instance.SandboxContractVersion, false},
		{"runsc_container_id", proof.RunscContainerID, instance.RunscContainerID, true},
		{"runsc_platform", proof.RunscPlatform, instance.RunscPlatform, true},
		{"runsc_version", proof.RunscVersion, instance.RunscVersion, true},
		{"runsc_binary_path", proof.RunscBinaryPath, instance.RunscBinaryPath, true},
		{"runsc_binary_digest", proof.RunscBinaryDigest, instance.RunscBinaryDigest, true},
	}
	for _, check := range checks {
		got := strings.TrimSpace(check.got)
		want := strings.TrimSpace(check.want)
		if got == "" {
			if check.required {
				return fmt.Errorf("runtime post-start proof %s is required", check.label)
			}
			continue
		}
		if got != want {
			return fmt.Errorf("runtime post-start proof %s = %q, want %q", check.label, got, want)
		}
	}
	return nil
}

func runtimeResourceCleanupEvidence(instance store.RuntimeResourceInstance, cleanup runtime.GenerationResourceCleanup) store.ResourceReconciliationEvidence {
	filesystem := make(map[string]string, len(cleanup.FilesystemLstat))
	for path, value := range cleanup.FilesystemLstat {
		filesystem[path] = value
	}
	return store.ResourceReconciliationEvidence{
		HostID:          instance.HostID,
		RunscState:      cleanup.RunscState,
		IPNetns:         cleanup.IPNetns,
		IPLink:          cleanup.IPLink,
		NFT:             cleanup.NFT,
		FilesystemLstat: filesystem,
	}
}

func runtimeResourceWorkerID(ownerUUID, leaseOwner string) string {
	workerID := strings.TrimSpace(ownerUUID)
	if workerID != "" {
		return workerID
	}
	leaseOwner = strings.TrimSpace(leaseOwner)
	suffix := ":" + store.RuntimeManagerRoleTag
	if strings.HasSuffix(leaseOwner, suffix) {
		workerID = strings.TrimSpace(strings.TrimSuffix(leaseOwner, suffix))
	}
	if workerID == "" {
		workerID = leaseOwner
	}
	return workerID
}

func runtimeResourceHostID() (string, error) {
	return runtimeResourceHostIDFrom(os.Hostname)
}

func runtimeResourceHostIDFrom(hostname func() (string, error)) (string, error) {
	host, err := hostname()
	if err == nil && strings.TrimSpace(host) != "" {
		return strings.TrimSpace(host), nil
	}
	if err != nil {
		return "", fmt.Errorf("runtime resource host id: %w", err)
	}
	return "", fmt.Errorf("runtime resource host id is required")
}

func runtimeResourceSandboxIP(cidr string) (string, error) {
	cidr = strings.TrimSpace(cidr)
	prefix, err := netip.ParsePrefix(cidr)
	if err == nil {
		return prefix.Addr().String(), nil
	}
	if before, _, ok := strings.Cut(cidr, "/"); ok && strings.TrimSpace(before) != "" {
		return strings.TrimSpace(before), nil
	}
	return "", fmt.Errorf("runtime resource sandbox ip cidr %q is invalid: %w", cidr, err)
}

func runtimeResourceNftTableName(generationID string) (string, error) {
	identifier, err := runtimeResourceIdentifier(generationID)
	if err != nil {
		return "", err
	}
	return "harness_gen_" + identifier, nil
}

func runtimeResourceIdentifier(value string) (string, error) {
	value = strings.TrimSpace(value)
	var b strings.Builder
	hasToken := false
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			b.WriteRune(r)
			hasToken = true
		case r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	out := b.String()
	if out == "" || !hasToken {
		return "", fmt.Errorf("runtime resource identifier is required")
	}
	return out, nil
}

func (s *Server) runtimeResourceRootPrefixes() map[string]string {
	roots := s.cfg.IsolationRoots()
	if strings.TrimSpace(s.cfg.DBPath) == "" {
		roots.DataVolumeEvidenceRoot = ""
	}
	if strings.TrimSpace(s.cfg.Harness.RunDir) == "" {
		roots.ProxyInternalRoot = ""
	}
	values := map[string]string{
		"sessions_root":             roots.SessionsRoot,
		"agent_homes_root":          roots.AgentHomesRoot,
		"run_dir":                   roots.RunDir,
		"prepared_bundle_root":      roots.PreparedBundleRoot,
		"rootfs_path":               roots.RootFSPath,
		"db_path":                   roots.DBPath,
		"schema_pack_root":          roots.SchemaPackRoot,
		"data_volume_evidence_root": roots.DataVolumeEvidenceRoot,
		"proxy_internal_root":       roots.ProxyInternalRoot,
		"provider_credential_root":  roots.ProviderCredentialRoot,
	}
	out := make(map[string]string, len(values))
	for key, value := range values {
		if strings.TrimSpace(value) == "" {
			continue
		}
		out[key] = cleanRuntimeResourceRoot(value)
	}
	return out
}

func cleanRuntimeResourceRoot(path string) string {
	path = strings.TrimSpace(path)
	absolute, err := filepath.Abs(path)
	if err != nil {
		return filepath.Clean(path)
	}
	return filepath.Clean(absolute)
}
