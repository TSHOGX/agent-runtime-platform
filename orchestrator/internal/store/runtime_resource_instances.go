package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type RuntimeResourceState string

const (
	RuntimeResourceAllocated          RuntimeResourceState = "allocated"
	RuntimeResourceMaterializing      RuntimeResourceState = "materializing"
	RuntimeResourceReady              RuntimeResourceState = "ready"
	RuntimeResourceLive               RuntimeResourceState = "live"
	RuntimeResourceCheckpointReserved RuntimeResourceState = "checkpoint_reserved"
	RuntimeResourceRetiring           RuntimeResourceState = "retiring"
	RuntimeResourceReconciling        RuntimeResourceState = "reconciling"
	RuntimeResourceAbsentVerified     RuntimeResourceState = "absent_verified"
	RuntimeResourceDestroyed          RuntimeResourceState = "destroyed"
)

type RuntimeResourceInstance struct {
	GenerationID            string
	SessionID               string
	ContractID              string
	SandboxContractVersion  string
	WorkerID                string
	HostID                  string
	State                   RuntimeResourceState
	LeaseExpiresAt          *time.Time
	IdempotencyToken        string
	RunscContainerID        string
	RunscPlatform           string
	RunscVersion            string
	RunscBinaryPath         string
	RunscBinaryDigest       string
	NetworkProfileID        string
	NetnsName               string
	NetnsPath               string
	HostVeth                string
	SandboxVeth             string
	HostGatewayIP           string
	SandboxIP               string
	SandboxIPCIDR           string
	HostSideCIDR            string
	NftTableName            string
	ControlDirPath          string
	ControlManifestPath     string
	BundleDirPath           string
	SpecPath                string
	CheckpointPath          string
	BridgeDirPath           string
	NetworkHostsPath        string
	LogDirPath              string
	ResourceIdentityPayload []byte
	ResourceIdentityDigest  string
	EvidenceJSON            []byte
	EvidenceDigest          string
	VerifiedAt              *time.Time
	UpdatedAt               time.Time
}

type RuntimeResourceInstanceParams struct {
	GenerationID           string
	SessionID              string
	ContractID             string
	SandboxContractVersion string
	HostID                 string
	RunscContainerID       string
	RunscPlatform          string
	RunscVersion           string
	RunscBinaryPath        string
	RunscBinaryDigest      string
	NetworkProfileID       string
	NetnsName              string
	NetnsPath              string
	HostVeth               string
	SandboxVeth            string
	HostGatewayIP          string
	SandboxIP              string
	SandboxIPCIDR          string
	HostSideCIDR           string
	NftTableName           string
	ControlDirPath         string
	ControlManifestPath    string
	BundleDirPath          string
	SpecPath               string
	CheckpointPath         string
	BridgeDirPath          string
	NetworkHostsPath       string
	LogDirPath             string
	RootPrefixes           map[string]string
	Now                    time.Time
}

type RuntimeResourceMaterializationClaimParams struct {
	GenerationID     string
	WorkerID         string
	HostID           string
	LeaseExpiresAt   time.Time
	IdempotencyToken string
	Now              time.Time
}

type RuntimeResourceWorkerTransitionParams struct {
	GenerationID string
	WorkerID     string
	HostID       string
	Now          time.Time
}

type RuntimeResourceRetireParams struct {
	GenerationID string
	WorkerID     string
	HostID       string
	Now          time.Time
}

type RuntimeResourceEvidenceParams struct {
	GenerationID string
	WorkerID     string
	HostID       string
	Evidence     ResourceReconciliationEvidence
	Now          time.Time
}

type ResourceReconciliationEvidence struct {
	HostID          string            `json:"host_id"`
	RunscState      string            `json:"runsc_state"`
	IPNetns         string            `json:"ip_netns"`
	IPLink          string            `json:"ip_link"`
	NFT             string            `json:"nft"`
	FilesystemLstat map[string]string `json:"filesystem_lstat"`
}

type runtimeResourceIdentityPayload struct {
	HostID                 string            `json:"host_id"`
	SessionID              string            `json:"session_id"`
	GenerationID           string            `json:"generation_id"`
	ContractID             string            `json:"contract_id"`
	SandboxContractVersion string            `json:"sandbox_contract_version"`
	RunscContainerID       string            `json:"runsc_container_id"`
	RunscPlatform          string            `json:"runsc_platform"`
	RunscVersion           string            `json:"runsc_version"`
	RunscBinaryPath        string            `json:"runsc_binary_path"`
	RunscBinaryDigest      string            `json:"runsc_binary_digest"`
	NetworkProfileID       string            `json:"network_profile_id"`
	NetnsName              string            `json:"netns_name"`
	NetnsPath              string            `json:"netns_path"`
	HostVeth               string            `json:"host_veth"`
	SandboxVeth            string            `json:"sandbox_veth"`
	HostGatewayIP          string            `json:"host_gateway_ip"`
	SandboxIP              string            `json:"sandbox_ip"`
	SandboxIPCIDR          string            `json:"sandbox_ip_cidr"`
	HostSideCIDR           string            `json:"host_side_cidr"`
	NftTableName           string            `json:"nft_table_name"`
	ControlDirPath         string            `json:"control_dir_path"`
	ControlManifestPath    string            `json:"control_manifest_path"`
	BundleDirPath          string            `json:"bundle_dir_path"`
	SpecPath               string            `json:"spec_path"`
	CheckpointPath         string            `json:"checkpoint_path,omitempty"`
	BridgeDirPath          string            `json:"bridge_dir_path"`
	NetworkHostsPath       string            `json:"network_hosts_path,omitempty"`
	LogDirPath             string            `json:"log_dir_path"`
	RootPrefixes           map[string]string `json:"root_prefixes"`
}

func (s *Store) CreateRuntimeResourceInstance(ctx context.Context, p RuntimeResourceInstanceParams) (RuntimeResourceInstance, error) {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	if strings.TrimSpace(p.SandboxContractVersion) == "" {
		p.SandboxContractVersion = SandboxContractVersion
	} else {
		p.SandboxContractVersion = strings.TrimSpace(p.SandboxContractVersion)
	}
	if err := validateRuntimeResourceInstanceParams(p); err != nil {
		return RuntimeResourceInstance{}, err
	}
	contract, err := s.GetSandboxContractForGeneration(ctx, p.SessionID, p.GenerationID)
	if err != nil {
		return RuntimeResourceInstance{}, fmt.Errorf("load sandbox contract: %w", err)
	}
	if contract.ContractID != p.ContractID || contract.SandboxContractVersion != p.SandboxContractVersion {
		return RuntimeResourceInstance{}, fmt.Errorf("runtime resource contract mismatch")
	}
	identityPayload, identityDigest, err := runtimeResourceIdentity(p)
	if err != nil {
		return RuntimeResourceInstance{}, err
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO runtime_resource_instances (
  generation_id, session_id, contract_id, sandbox_contract_version,
  host_id, state, runsc_container_id, runsc_platform, runsc_version,
  runsc_binary_path, runsc_binary_digest, network_profile_id, netns_name,
  netns_path, host_veth, sandbox_veth, host_gateway_ip, sandbox_ip,
  sandbox_ip_cidr, host_side_cidr, nft_table_name, control_dir_path,
  control_manifest_path, bundle_dir_path, spec_path, checkpoint_path,
  bridge_dir_path, network_hosts_path, log_dir_path, resource_identity_payload,
  resource_identity_digest, updated_at
) VALUES (?, ?, ?, ?, ?, 'allocated', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		p.GenerationID, p.SessionID, p.ContractID, p.SandboxContractVersion,
		p.HostID, p.RunscContainerID, p.RunscPlatform, p.RunscVersion,
		p.RunscBinaryPath, p.RunscBinaryDigest, p.NetworkProfileID, p.NetnsName,
		p.NetnsPath, p.HostVeth, p.SandboxVeth, p.HostGatewayIP, p.SandboxIP,
		p.SandboxIPCIDR, p.HostSideCIDR, p.NftTableName, p.ControlDirPath,
		p.ControlManifestPath, p.BundleDirPath, p.SpecPath, nullableString(p.CheckpointPath),
		p.BridgeDirPath, nullableString(p.NetworkHostsPath), p.LogDirPath, string(identityPayload),
		identityDigest, formatTime(p.Now))
	if err != nil {
		return RuntimeResourceInstance{}, err
	}
	return s.GetRuntimeResourceInstance(ctx, p.GenerationID)
}

func (s *Store) GetRuntimeResourceInstance(ctx context.Context, generationID string) (RuntimeResourceInstance, error) {
	row := s.db.QueryRowContext(ctx, runtimeResourceInstanceSelectSQL()+`
WHERE generation_id = ?`, strings.TrimSpace(generationID))
	instance, err := scanRuntimeResourceInstance(row)
	if err != nil {
		return RuntimeResourceInstance{}, err
	}
	if err := verifyRuntimeResourceIdentity(instance); err != nil {
		return RuntimeResourceInstance{}, err
	}
	return instance, nil
}

func (s *Store) ClaimRuntimeResourceMaterialization(ctx context.Context, p RuntimeResourceMaterializationClaimParams) error {
	return s.claimRuntimeResourceMaterialization(ctx, p, RuntimeResourceAllocated)
}

func (s *Store) ClaimRuntimeResourceCheckpointRestore(ctx context.Context, p RuntimeResourceMaterializationClaimParams) error {
	return s.claimRuntimeResourceMaterialization(ctx, p, RuntimeResourceCheckpointReserved)
}

func (s *Store) MarkRuntimeResourceReady(ctx context.Context, p RuntimeResourceWorkerTransitionParams) error {
	return s.workerStateTransition(ctx, p, RuntimeResourceMaterializing, RuntimeResourceReady)
}

func (s *Store) MarkRuntimeResourceLive(ctx context.Context, p RuntimeResourceWorkerTransitionParams) error {
	return s.workerStateTransition(ctx, p, RuntimeResourceReady, RuntimeResourceLive)
}

func (s *Store) ReserveRuntimeResourceCheckpoint(ctx context.Context, p RuntimeResourceWorkerTransitionParams) error {
	return s.workerStateTransition(ctx, p, RuntimeResourceLive, RuntimeResourceCheckpointReserved)
}

func (s *Store) ClaimRuntimeResourceRetiring(ctx context.Context, p RuntimeResourceRetireParams) error {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	if strings.TrimSpace(p.GenerationID) == "" || strings.TrimSpace(p.HostID) == "" {
		return fmt.Errorf("generation id and host id are required")
	}
	res, err := s.db.ExecContext(ctx, `
UPDATE runtime_resource_instances
SET state = 'retiring',
    worker_id = COALESCE(NULLIF(?, ''), worker_id),
    host_id = ?,
    lease_expires_at = NULL,
    idempotency_token = NULL,
    updated_at = ?
WHERE generation_id = ?
  AND host_id = ?
  AND state IN ('allocated','materializing','ready','live','checkpoint_reserved')`,
		strings.TrimSpace(p.WorkerID), strings.TrimSpace(p.HostID), formatTime(p.Now),
		strings.TrimSpace(p.GenerationID), strings.TrimSpace(p.HostID))
	if err != nil {
		return err
	}
	return requireOneRow(res, "runtime resource retiring CAS failed")
}

func (s *Store) MarkRuntimeResourceReconciling(ctx context.Context, p RuntimeResourceEvidenceParams) error {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	evidenceJSON, evidenceDigest, err := runtimeResourceEvidenceDigest(p.Evidence)
	if err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx, `
UPDATE runtime_resource_instances
SET state = 'reconciling',
    worker_id = COALESCE(NULLIF(?, ''), worker_id),
    evidence_json = ?,
    evidence_digest = ?,
    updated_at = ?
WHERE generation_id = ?
  AND host_id = ?
  AND state = 'retiring'`,
		strings.TrimSpace(p.WorkerID), string(evidenceJSON), evidenceDigest, formatTime(p.Now),
		strings.TrimSpace(p.GenerationID), strings.TrimSpace(p.HostID))
	if err != nil {
		return err
	}
	return requireOneRow(res, "runtime resource reconciling CAS failed")
}

func (s *Store) MarkRuntimeResourceAbsentVerified(ctx context.Context, p RuntimeResourceEvidenceParams) error {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	instance, err := s.GetRuntimeResourceInstance(ctx, p.GenerationID)
	if err != nil {
		return err
	}
	if instance.State != RuntimeResourceReconciling {
		return fmt.Errorf("runtime resource absent verification requires reconciling state, got %s", instance.State)
	}
	if instance.HostID != strings.TrimSpace(p.HostID) {
		return fmt.Errorf("runtime resource host mismatch: row=%s evidence=%s", instance.HostID, strings.TrimSpace(p.HostID))
	}
	if err := validateAbsenceEvidence(p.Evidence, instance.HostID); err != nil {
		return err
	}
	evidenceJSON, evidenceDigest, err := runtimeResourceEvidenceDigest(p.Evidence)
	if err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx, `
UPDATE runtime_resource_instances
SET state = 'absent_verified',
    worker_id = COALESCE(NULLIF(?, ''), worker_id),
    evidence_json = ?,
    evidence_digest = ?,
    verified_at = ?,
    updated_at = ?
WHERE generation_id = ?
  AND host_id = ?
  AND state = 'reconciling'
  AND resource_identity_digest = ?`,
		strings.TrimSpace(p.WorkerID), string(evidenceJSON), evidenceDigest, formatTime(p.Now), formatTime(p.Now),
		strings.TrimSpace(p.GenerationID), instance.HostID, instance.ResourceIdentityDigest)
	if err != nil {
		return err
	}
	return requireOneRow(res, "runtime resource absent verification CAS failed")
}

func (s *Store) MarkRuntimeResourceDestroyed(ctx context.Context, p RuntimeResourceRetireParams) error {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	res, err := s.db.ExecContext(ctx, `
UPDATE runtime_resource_instances
SET state = 'destroyed',
    worker_id = COALESCE(NULLIF(?, ''), worker_id),
    updated_at = ?
WHERE generation_id = ?
  AND host_id = ?
  AND state = 'absent_verified'`,
		strings.TrimSpace(p.WorkerID), formatTime(p.Now), strings.TrimSpace(p.GenerationID), strings.TrimSpace(p.HostID))
	if err != nil {
		return err
	}
	return requireOneRow(res, "runtime resource destroyed CAS failed")
}

func (s *Store) claimRuntimeResourceMaterialization(ctx context.Context, p RuntimeResourceMaterializationClaimParams, from RuntimeResourceState) error {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	if strings.TrimSpace(p.GenerationID) == "" ||
		strings.TrimSpace(p.WorkerID) == "" ||
		strings.TrimSpace(p.HostID) == "" ||
		strings.TrimSpace(p.IdempotencyToken) == "" {
		return fmt.Errorf("generation id, worker id, host id, and idempotency token are required")
	}
	if p.LeaseExpiresAt.IsZero() || !p.LeaseExpiresAt.After(p.Now) {
		return fmt.Errorf("runtime resource materialization lease must be in the future")
	}
	res, err := s.db.ExecContext(ctx, `
UPDATE runtime_resource_instances
SET state = 'materializing',
    worker_id = ?,
    host_id = ?,
    lease_expires_at = ?,
    idempotency_token = ?,
    updated_at = ?
WHERE generation_id = ?
  AND host_id = ?
  AND state = ?`,
		strings.TrimSpace(p.WorkerID), strings.TrimSpace(p.HostID), formatTime(p.LeaseExpiresAt),
		strings.TrimSpace(p.IdempotencyToken), formatTime(p.Now), strings.TrimSpace(p.GenerationID),
		strings.TrimSpace(p.HostID), string(from))
	if err != nil {
		return err
	}
	return requireOneRow(res, "runtime resource materialization CAS failed")
}

func (s *Store) workerStateTransition(ctx context.Context, p RuntimeResourceWorkerTransitionParams, from, to RuntimeResourceState) error {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	if strings.TrimSpace(p.GenerationID) == "" || strings.TrimSpace(p.WorkerID) == "" || strings.TrimSpace(p.HostID) == "" {
		return fmt.Errorf("generation id, worker id, and host id are required")
	}
	clearMaterializationLease := to == RuntimeResourceLive || to == RuntimeResourceCheckpointReserved
	res, err := s.db.ExecContext(ctx, `
UPDATE runtime_resource_instances
SET state = ?,
    lease_expires_at = CASE WHEN ? = 1 THEN NULL ELSE lease_expires_at END,
    idempotency_token = CASE WHEN ? = 1 THEN NULL ELSE idempotency_token END,
    updated_at = ?
WHERE generation_id = ?
  AND worker_id = ?
  AND host_id = ?
  AND state = ?
  AND (lease_expires_at IS NULL OR lease_expires_at > ?)`,
		string(to), boolInt(clearMaterializationLease), boolInt(clearMaterializationLease),
		formatTime(p.Now), strings.TrimSpace(p.GenerationID), strings.TrimSpace(p.WorkerID),
		strings.TrimSpace(p.HostID), string(from), formatTime(p.Now))
	if err != nil {
		return err
	}
	return requireOneRow(res, fmt.Sprintf("runtime resource %s -> %s CAS failed", from, to))
}

func validateRuntimeResourceInstanceParams(p RuntimeResourceInstanceParams) error {
	required := map[string]string{
		"generation id":            p.GenerationID,
		"session id":               p.SessionID,
		"contract id":              p.ContractID,
		"sandbox contract version": p.SandboxContractVersion,
		"host id":                  p.HostID,
		"runsc container id":       p.RunscContainerID,
		"runsc platform":           p.RunscPlatform,
		"runsc version":            p.RunscVersion,
		"runsc binary path":        p.RunscBinaryPath,
		"runsc binary digest":      p.RunscBinaryDigest,
		"network profile id":       p.NetworkProfileID,
		"netns name":               p.NetnsName,
		"netns path":               p.NetnsPath,
		"host veth":                p.HostVeth,
		"sandbox veth":             p.SandboxVeth,
		"host gateway ip":          p.HostGatewayIP,
		"sandbox ip":               p.SandboxIP,
		"sandbox ip cidr":          p.SandboxIPCIDR,
		"host side cidr":           p.HostSideCIDR,
		"nft table name":           p.NftTableName,
		"control dir path":         p.ControlDirPath,
		"control manifest path":    p.ControlManifestPath,
		"bundle dir path":          p.BundleDirPath,
		"spec path":                p.SpecPath,
		"bridge dir path":          p.BridgeDirPath,
		"log dir path":             p.LogDirPath,
	}
	for label, value := range required {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("runtime resource %s is required", label)
		}
	}
	if p.SandboxContractVersion != SandboxContractVersion {
		return fmt.Errorf("unsupported runtime resource sandbox contract version %q", p.SandboxContractVersion)
	}
	return nil
}

func runtimeResourceIdentity(p RuntimeResourceInstanceParams) ([]byte, string, error) {
	payload := runtimeResourceIdentityPayload{
		HostID:                 strings.TrimSpace(p.HostID),
		SessionID:              strings.TrimSpace(p.SessionID),
		GenerationID:           strings.TrimSpace(p.GenerationID),
		ContractID:             strings.TrimSpace(p.ContractID),
		SandboxContractVersion: strings.TrimSpace(p.SandboxContractVersion),
		RunscContainerID:       strings.TrimSpace(p.RunscContainerID),
		RunscPlatform:          strings.TrimSpace(p.RunscPlatform),
		RunscVersion:           strings.TrimSpace(p.RunscVersion),
		RunscBinaryPath:        strings.TrimSpace(p.RunscBinaryPath),
		RunscBinaryDigest:      strings.TrimSpace(p.RunscBinaryDigest),
		NetworkProfileID:       strings.TrimSpace(p.NetworkProfileID),
		NetnsName:              strings.TrimSpace(p.NetnsName),
		NetnsPath:              strings.TrimSpace(p.NetnsPath),
		HostVeth:               strings.TrimSpace(p.HostVeth),
		SandboxVeth:            strings.TrimSpace(p.SandboxVeth),
		HostGatewayIP:          strings.TrimSpace(p.HostGatewayIP),
		SandboxIP:              strings.TrimSpace(p.SandboxIP),
		SandboxIPCIDR:          strings.TrimSpace(p.SandboxIPCIDR),
		HostSideCIDR:           strings.TrimSpace(p.HostSideCIDR),
		NftTableName:           strings.TrimSpace(p.NftTableName),
		ControlDirPath:         strings.TrimSpace(p.ControlDirPath),
		ControlManifestPath:    strings.TrimSpace(p.ControlManifestPath),
		BundleDirPath:          strings.TrimSpace(p.BundleDirPath),
		SpecPath:               strings.TrimSpace(p.SpecPath),
		CheckpointPath:         strings.TrimSpace(p.CheckpointPath),
		BridgeDirPath:          strings.TrimSpace(p.BridgeDirPath),
		NetworkHostsPath:       strings.TrimSpace(p.NetworkHostsPath),
		LogDirPath:             strings.TrimSpace(p.LogDirPath),
		RootPrefixes:           sortedStringMap(p.RootPrefixes),
	}
	data, err := canonicalDataVolumeJSON(payload)
	if err != nil {
		return nil, "", err
	}
	return data, SandboxContractDigest(data), nil
}

func verifyRuntimeResourceIdentity(instance RuntimeResourceInstance) error {
	canonical, err := canonicalDataVolumeJSONBytes(instance.ResourceIdentityPayload)
	if err != nil {
		return err
	}
	if !bytes.Equal(canonical, instance.ResourceIdentityPayload) {
		return fmt.Errorf("runtime resource identity payload is not canonical")
	}
	if got := SandboxContractDigest(instance.ResourceIdentityPayload); got != instance.ResourceIdentityDigest {
		return fmt.Errorf("runtime resource identity digest mismatch: got %s want %s", got, instance.ResourceIdentityDigest)
	}
	var payload runtimeResourceIdentityPayload
	if err := json.Unmarshal(instance.ResourceIdentityPayload, &payload); err != nil {
		return err
	}
	if payload.HostID != instance.HostID ||
		payload.SessionID != instance.SessionID ||
		payload.GenerationID != instance.GenerationID ||
		payload.ContractID != instance.ContractID ||
		payload.SandboxContractVersion != instance.SandboxContractVersion ||
		payload.RunscContainerID != instance.RunscContainerID ||
		payload.RunscPlatform != instance.RunscPlatform ||
		payload.RunscVersion != instance.RunscVersion ||
		payload.RunscBinaryPath != instance.RunscBinaryPath ||
		payload.RunscBinaryDigest != instance.RunscBinaryDigest ||
		payload.NetworkProfileID != instance.NetworkProfileID ||
		payload.NetnsName != instance.NetnsName ||
		payload.NetnsPath != instance.NetnsPath ||
		payload.HostVeth != instance.HostVeth ||
		payload.SandboxVeth != instance.SandboxVeth ||
		payload.HostGatewayIP != instance.HostGatewayIP ||
		payload.SandboxIP != instance.SandboxIP ||
		payload.SandboxIPCIDR != instance.SandboxIPCIDR ||
		payload.HostSideCIDR != instance.HostSideCIDR ||
		payload.NftTableName != instance.NftTableName ||
		payload.ControlDirPath != instance.ControlDirPath ||
		payload.ControlManifestPath != instance.ControlManifestPath ||
		payload.BundleDirPath != instance.BundleDirPath ||
		payload.SpecPath != instance.SpecPath ||
		payload.CheckpointPath != instance.CheckpointPath ||
		payload.BridgeDirPath != instance.BridgeDirPath ||
		payload.NetworkHostsPath != instance.NetworkHostsPath ||
		payload.LogDirPath != instance.LogDirPath {
		return fmt.Errorf("runtime resource identity payload does not match row mirrors")
	}
	return nil
}

func runtimeResourceEvidenceDigest(evidence ResourceReconciliationEvidence) ([]byte, string, error) {
	data, err := canonicalDataVolumeJSON(evidence)
	if err != nil {
		return nil, "", err
	}
	return data, SandboxContractDigest(data), nil
}

func validateAbsenceEvidence(evidence ResourceReconciliationEvidence, hostID string) error {
	if strings.TrimSpace(evidence.HostID) != hostID {
		return fmt.Errorf("reconciliation evidence host_id = %q, want %q", strings.TrimSpace(evidence.HostID), hostID)
	}
	required := map[string]string{
		"runsc state": evidence.RunscState,
		"ip netns":    evidence.IPNetns,
		"ip link":     evidence.IPLink,
		"nft":         evidence.NFT,
	}
	for label, value := range required {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("reconciliation evidence %s is required before absent_verified", label)
		}
	}
	if len(evidence.FilesystemLstat) == 0 {
		return fmt.Errorf("reconciliation evidence filesystem lstat is required before absent_verified")
	}
	for path, value := range evidence.FilesystemLstat {
		if strings.TrimSpace(path) == "" || strings.TrimSpace(value) == "" {
			return fmt.Errorf("reconciliation evidence filesystem lstat entries must be non-empty")
		}
	}
	return nil
}

func runtimeResourceInstanceSelectSQL() string {
	return `
SELECT generation_id, session_id, contract_id, sandbox_contract_version,
       COALESCE(worker_id, ''), host_id, state, COALESCE(lease_expires_at, ''),
       COALESCE(idempotency_token, ''), runsc_container_id, runsc_platform,
       runsc_version, runsc_binary_path, runsc_binary_digest, network_profile_id,
       netns_name, netns_path, host_veth, sandbox_veth, host_gateway_ip,
       sandbox_ip, sandbox_ip_cidr, host_side_cidr, nft_table_name,
       control_dir_path, control_manifest_path, bundle_dir_path, spec_path,
       COALESCE(checkpoint_path, ''), bridge_dir_path, COALESCE(network_hosts_path, ''),
       log_dir_path, resource_identity_payload, resource_identity_digest,
       COALESCE(evidence_json, ''), COALESCE(evidence_digest, ''),
       COALESCE(verified_at, ''), updated_at
FROM runtime_resource_instances
`
}

type runtimeResourceScanner interface {
	Scan(dest ...any) error
}

func scanRuntimeResourceInstance(row runtimeResourceScanner) (RuntimeResourceInstance, error) {
	var instance RuntimeResourceInstance
	var state, leaseExpiresAt, verifiedAt, updatedAt, payload, evidence string
	if err := row.Scan(
		&instance.GenerationID,
		&instance.SessionID,
		&instance.ContractID,
		&instance.SandboxContractVersion,
		&instance.WorkerID,
		&instance.HostID,
		&state,
		&leaseExpiresAt,
		&instance.IdempotencyToken,
		&instance.RunscContainerID,
		&instance.RunscPlatform,
		&instance.RunscVersion,
		&instance.RunscBinaryPath,
		&instance.RunscBinaryDigest,
		&instance.NetworkProfileID,
		&instance.NetnsName,
		&instance.NetnsPath,
		&instance.HostVeth,
		&instance.SandboxVeth,
		&instance.HostGatewayIP,
		&instance.SandboxIP,
		&instance.SandboxIPCIDR,
		&instance.HostSideCIDR,
		&instance.NftTableName,
		&instance.ControlDirPath,
		&instance.ControlManifestPath,
		&instance.BundleDirPath,
		&instance.SpecPath,
		&instance.CheckpointPath,
		&instance.BridgeDirPath,
		&instance.NetworkHostsPath,
		&instance.LogDirPath,
		&payload,
		&instance.ResourceIdentityDigest,
		&evidence,
		&instance.EvidenceDigest,
		&verifiedAt,
		&updatedAt,
	); err != nil {
		return RuntimeResourceInstance{}, err
	}
	instance.State = RuntimeResourceState(state)
	instance.ResourceIdentityPayload = []byte(payload)
	if evidence != "" {
		instance.EvidenceJSON = []byte(evidence)
	}
	if leaseExpiresAt != "" {
		parsed := parseTime(leaseExpiresAt)
		instance.LeaseExpiresAt = &parsed
	}
	if verifiedAt != "" {
		parsed := parseTime(verifiedAt)
		instance.VerifiedAt = &parsed
	}
	instance.UpdatedAt = parseTime(updatedAt)
	return instance, nil
}

func requireOneRow(result sql.Result, message string) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return fmt.Errorf(message)
	}
	return nil
}

func sortedStringMap(input map[string]string) map[string]string {
	if len(input) == 0 {
		return nil
	}
	out := make(map[string]string, len(input))
	for key, value := range input {
		out[strings.TrimSpace(key)] = strings.TrimSpace(value)
	}
	return out
}
