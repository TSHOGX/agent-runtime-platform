package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"net/url"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/sessionstate"

	"github.com/google/uuid"
)

const RuntimeManagerRoleTag = "runtime_manager"

var ErrPoolExhausted = errors.New("pool exhausted")
var ErrStaleCheckpointRestore = errors.New("stale checkpoint restore")

type ResourceAllocatorConfig struct {
	RunDir                      string
	CIDRPool                    netip.Prefix
	EgressDorisFEHosts          []string
	EgressDorisBEHosts          []string
	EgressDorisPorts            []int
	EgressDNSPolicy             string
	HostProxyBindURL            string
	ProxyPort                   int
	DriverID                    string
	Model                       string
	OutputFormat                string
	DisableNonessentialTraffic  bool
	SandboxUID                  int
	SandboxGID                  int
	SandboxSupplementalGIDs     []int
	ModelAccessAllowed          *bool
	ProviderCredentialsHostOnly bool
	SandboxModelProxyBaseURL    string
}

type AllocateGenerationParams struct {
	SessionID            string
	ExpectedGenerationID sql.NullString
	Owner                string
	LeaseTTL             time.Duration
	Now                  time.Time
	Config               ResourceAllocatorConfig
}

type ClaimCheckpointedGenerationParams struct {
	SessionID    string
	GenerationID string
	Owner        string
	LeaseTTL     time.Duration
	Now          time.Time
}

type GenerationAllocation struct {
	GenerationID          string
	NetworkProfileID      string
	AgentRuntimeProfileID string
	Owner                 string
	LeaseExpiresAt        time.Time
	DriverState           DriverStateToken
}

type RuntimeGenerationDetails struct {
	SessionID                       string
	GenerationID                    string
	NetworkProfileID                string
	AgentRuntimeProfileID           string
	RunscPlatform                   string
	SandboxContractVersion          string
	ControlDirPath                  string
	ControlManifestPath             string
	BundleDirPath                   string
	SpecPath                        string
	CheckpointPath                  string
	SecretsDirPath                  string
	BridgeDirPath                   string
	NetworkHostsPath                string
	LogDirPath                      string
	ControlManifestDigest           string
	ProjectedControlManifestDigest  string
	BundleDigest                    string
	RuntimeConfigDigest             string
	SpecDigest                      string
	RunscContainerID                string
	RunscVersion                    string
	RunscBinaryPath                 string
	RunscBinaryDigest               string
	CheckpointNetworkProfileID      string
	CheckpointAgentRuntimeProfileID string
	CheckpointRunscVersion          string
	CheckpointRunscPlatform         string
	CheckpointRunscBinaryPath       string
	CheckpointRunscBinaryDigest     string
	CheckpointBundleDigest          string
	CheckpointRuntimeConfigDigest   string
	CheckpointControlManifestDigest string
	CheckpointDriverStatesDigest    string
	CheckpointPlanDigest            string
	RunscNetwork                    string
	RunscOverlay2                   string
	HostProxyBindURL                string
	ProxyPort                       int
	HostGatewayIP                   string
	SandboxBaseURL                  string
	ProbeURL                        string
	NetnsName                       string
	NetnsPath                       string
	HostVeth                        string
	SandboxVeth                     string
	SandboxIPCIDR                   string
	HostSideCIDR                    string
	NftTableName                    string
	EgressPolicyID                  string
	EgressPolicyDigest              string
	AllowedEgressRules              string
	DorisFEHosts                    string
	DorisBEHosts                    string
	DorisPorts                      string
	DNSPolicy                       string
	NetworkAllocationState          string
	AutoCheckpointEnabled           bool
	DriverID                        string
	Model                           string
	OutputFormat                    string
	DisableNonessentialTraffic      bool
	SandboxUID                      int
	SandboxGID                      int
	SandboxSupplementalGIDs         []int
	ModelAccessAllowed              bool
	RequiresSecretDrop              bool
	ManifestAnthropicBaseURL        string
	AnthropicAPIKeySecretID         string
	AnthropicAuthTokenSecretID      string
	SecretVersion                   string
	DriverStateDigest               string
	DriverStateVersion              int
	DriverStatePayload              []byte
}

type BridgePollGeneration struct {
	SessionID     string
	GenerationID  string
	BridgeDirPath string
}

type CheckpointCandidate struct {
	SessionID     string
	GenerationID  string
	BridgeDirPath string
}

type CompleteCheckpointParams struct {
	SessionID                       string
	GenerationID                    string
	Owner                           string
	CheckpointPath                  string
	RunscPlatform                   string
	RunscVersion                    string
	RunscBinaryPath                 string
	RunscBinaryDigest               string
	CheckpointBundleDigest          string
	CheckpointRuntimeConfigDigest   string
	CheckpointControlManifestDigest string
	CheckpointPlanDigest            string
	Now                             time.Time
}

type ReaperParams struct {
	OwnerUUID       string
	FailedRetention time.Duration
	Now             time.Time
}

type ReaperResult struct {
	FailedMarkedReclaimable int64
	DestroyedAllocations    int64
}

type RetireExpiredCheckpointsParams struct {
	OwnerUUID                string
	Now                      time.Time
	CheckpointImageRetention time.Duration
}

type RetiredCheckpoint struct {
	SessionID    string
	GenerationID string
	EventID      int64
}

type ReclaimableGeneration struct {
	SessionID    string
	GenerationID string
}

type DestroyGenerationResourcesParams struct {
	SessionID    string
	GenerationID string
	Now          time.Time
}

type StartupRecoveryParams struct {
	OwnerUUID       string
	Now             time.Time
	LeaseTTL        time.Duration
	ReconnectGrace  time.Duration
	AckStartedGrace time.Duration
}

type StartupRecoveryResult struct {
	ExpiredLifecycleFailed int64
	ReconnectGraceFailed   int64
	ExpiredLeasedRequeued  int64
	UnknownAfterAckStarted int64
	RuntimeCleanupSkipped  int64
	EventIDs               []int64
}

type RenewLiveGenerationsParams struct {
	Owner    string
	LeaseTTL time.Duration
	Now      time.Time
}

type ExpiredRuntimeRecoveryCandidate struct {
	SessionID    string
	GenerationID string
	RuntimeID    string
	Status       string
	ErrorClass   string
}

type GenerationRuntimeArtifactDigests struct {
	ControlManifestDigest          string
	ProjectedControlManifestDigest string
	BundleDigest                   string
	RuntimeConfigDigest            string
	SpecDigest                     string
	RunscVersion                   string
	RunscBinaryPath                string
	RunscBinaryDigest              string
}

type ResourceQuota struct {
	AllocatedPoolSlots int
}

func GenerationLeaseOwner(ownerUUID string) string {
	return strings.TrimSpace(ownerUUID) + ":" + RuntimeManagerRoleTag
}

func (s *Store) AllocateGeneration(ctx context.Context, p AllocateGenerationParams) (GenerationAllocation, error) {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	if strings.TrimSpace(p.SessionID) == "" {
		return GenerationAllocation{}, fmt.Errorf("session id is required")
	}
	if strings.TrimSpace(p.Owner) == "" {
		return GenerationAllocation{}, fmt.Errorf("owner is required")
	}
	if p.LeaseTTL <= 0 {
		return GenerationAllocation{}, fmt.Errorf("lease ttl must be > 0")
	}
	if !p.Config.CIDRPool.IsValid() || !p.Config.CIDRPool.Addr().Is4() || p.Config.CIDRPool.Bits() > 30 {
		return GenerationAllocation{}, fmt.Errorf("valid IPv4 /30-capable CIDR pool is required")
	}
	if strings.TrimSpace(p.Config.RunDir) == "" {
		return GenerationAllocation{}, fmt.Errorf("run dir is required")
	}
	if err := p.Config.validateAllocationBoundary(); err != nil {
		return GenerationAllocation{}, err
	}
	if _, ok := agents.Lookup(p.Config.driverID()); !ok {
		return GenerationAllocation{}, fmt.Errorf("unsupported driver %q", p.Config.driverID())
	}
	if err := agents.EnsureDriverSupportedByProvider(p.Config.driverID(), "local_runsc"); err != nil {
		return GenerationAllocation{}, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return GenerationAllocation{}, err
	}
	defer func() { _ = tx.Rollback() }()

	if err := assertOwnerTx(ctx, tx, ownerUUIDFromLeaseOwner(p.Owner)); err != nil {
		return GenerationAllocation{}, err
	}
	slot, err := nextFreeSlot(ctx, tx, p.Config.CIDRPool)
	if err != nil {
		return GenerationAllocation{}, err
	}
	generationID := "gen_" + uuid.NewString()
	network, err := buildNetworkAllocation(p.Config, slot, generationID)
	if err != nil {
		return GenerationAllocation{}, err
	}
	networkProfileID := "net_" + generationID
	agentRuntimeProfileID := agentRuntimeProfileID(generationID)
	resources := buildResourcePaths(p.Config.RunDir, generationID)
	runscContainerID := generationRunscContainerID(generationID)
	resources.SecretsDirPath = ""
	if !p.Config.requiresNetworkHostsProjection() {
		resources.NetworkHostsPath = ""
	}
	egressPolicyID := egressPolicyID(p.Config)
	allowedRules := allowedEgressRules(network.HostGatewayIP, p.Config)
	allowedRulesJSON, err := json.Marshal(allowedRules)
	if err != nil {
		return GenerationAllocation{}, err
	}
	feHostsJSON, err := json.Marshal(p.Config.EgressDorisFEHosts)
	if err != nil {
		return GenerationAllocation{}, err
	}
	beHostsJSON, err := json.Marshal(p.Config.EgressDorisBEHosts)
	if err != nil {
		return GenerationAllocation{}, err
	}
	portsJSON, err := json.Marshal(p.Config.EgressDorisPorts)
	if err != nil {
		return GenerationAllocation{}, err
	}
	supplementalGIDsJSON, err := json.Marshal(p.Config.sandboxSupplementalGIDs())
	if err != nil {
		return GenerationAllocation{}, err
	}
	var sessionDriverID string
	var autoCheckpointEnabled int
	if err := tx.QueryRowContext(ctx, `
SELECT driver_id, auto_checkpoint_enabled
FROM sessions
WHERE id = ?`, p.SessionID).Scan(&sessionDriverID, &autoCheckpointEnabled); err != nil {
		return GenerationAllocation{}, err
	}
	if sessionDriverID != p.Config.driverID() {
		return GenerationAllocation{}, fmt.Errorf("session driver %q does not match allocation driver %q", sessionDriverID, p.Config.driverID())
	}
	now := formatTime(p.Now)
	leaseExpires := p.Now.Add(p.LeaseTTL)

	if _, err := tx.ExecContext(ctx, `
INSERT INTO agent_runtime_profiles (
  agent_runtime_profile_id, driver_id, model, output_format,
  disable_nonessential_traffic, sandbox_uid, sandbox_gid, sandbox_supplemental_gids,
  requires_secret_drop,
  model_access_allowed, manifest_model_proxy_base_url, model_proxy_api_key_secret_id,
  model_proxy_auth_token_secret_id, secret_version, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT DO NOTHING`,
		agentRuntimeProfileID, p.Config.driverID(), nullableString(p.Config.Model), p.Config.outputFormat(),
		boolInt(p.Config.DisableNonessentialTraffic), p.Config.sandboxUID(), p.Config.sandboxGID(), string(supplementalGIDsJSON),
		0,
		boolInt(p.Config.modelAccessAllowed()),
		nullableString(p.Config.manifestAnthropicBaseURL(network.SandboxBaseURL)),
		nil, nil, nil, now); err != nil {
		return GenerationAllocation{}, err
	}
	if err := tx.QueryRowContext(ctx, `
SELECT agent_runtime_profile_id
FROM agent_runtime_profiles
WHERE driver_id = ?
  AND model IS ?
  AND output_format = ?
  AND disable_nonessential_traffic = ?
  AND sandbox_uid = ?
  AND sandbox_gid = ?
  AND sandbox_supplemental_gids = ?
  AND requires_secret_drop = ?
  AND model_access_allowed = ?
  AND manifest_model_proxy_base_url IS ?
  AND model_proxy_api_key_secret_id IS ?
  AND model_proxy_auth_token_secret_id IS ?
  AND secret_version IS ?`,
		p.Config.driverID(), nullableString(p.Config.Model), p.Config.outputFormat(),
		boolInt(p.Config.DisableNonessentialTraffic), p.Config.sandboxUID(), p.Config.sandboxGID(), string(supplementalGIDsJSON),
		0,
		boolInt(p.Config.modelAccessAllowed()),
		nullableString(p.Config.manifestAnthropicBaseURL(network.SandboxBaseURL)),
		nil, nil, nil).Scan(&agentRuntimeProfileID); err != nil {
		return GenerationAllocation{}, fmt.Errorf("lookup explicit agent runtime profile: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO egress_policies (
  egress_policy_id, policy_digest, allowed_egress_rules,
  doris_fe_hosts, doris_be_hosts, doris_ports, dns_policy, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(egress_policy_id) DO NOTHING`,
		egressPolicyID, egressPolicyID, string(allowedRulesJSON), string(feHostsJSON),
		string(beHostsJSON), string(portsJSON), p.Config.EgressDNSPolicy, now); err != nil {
		return GenerationAllocation{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO runtime_generations (
  generation_id, session_id, status, network_profile_id,
  agent_runtime_profile_id, runsc_platform, sandbox_contract_version, lease_owner,
  lease_expires_at, last_seen_at, auto_checkpoint_enabled
) VALUES (?, ?, 'allocating', ?, ?, 'systrap', ?, ?, ?, ?, ?)`,
		generationID, p.SessionID, networkProfileID, agentRuntimeProfileID, SandboxContractVersion,
		p.Owner, formatTime(leaseExpires), now, autoCheckpointEnabled); err != nil {
		return GenerationAllocation{}, err
	}
	driverState, err := ensureAllocationDriverStateTx(ctx, tx, p.SessionID, generationID, sessionDriverID, p.Now)
	if err != nil {
		return GenerationAllocation{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO network_profiles (
  network_profile_id, session_id, generation_id,
  runsc_network, runsc_overlay2, host_proxy_bind_url, proxy_port,
  host_gateway_ip, sandbox_base_url, probe_url, netns_name, netns_path,
  host_veth, sandbox_veth, sandbox_ip_cidr, egress_policy_id,
  allowed_egress_rules, doris_fe_hosts, doris_be_hosts, doris_ports,
  dns_policy, host_side_cidr, allocation_state, created_at
) VALUES (?, ?, ?, 'sandbox', 'none', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'allocating', ?)`,
		networkProfileID, p.SessionID, generationID, p.Config.hostProxyBindURL(), p.Config.proxyPort(),
		network.HostGatewayIP, network.SandboxBaseURL, network.ProbeURL, network.NetnsName, network.NetnsPath,
		network.HostVeth, network.SandboxVeth, network.SandboxIPCIDR, egressPolicyID,
		string(allowedRulesJSON), string(feHostsJSON), string(beHostsJSON), string(portsJSON),
		p.Config.EgressDNSPolicy, network.HostSideCIDR, now); err != nil {
		return GenerationAllocation{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO runtime_generation_resources (
  generation_id, network_profile_id, agent_runtime_profile_id,
  control_dir_path, control_manifest_path, bundle_dir_path, spec_path,
  checkpoint_path, secrets_dir_path, bridge_dir_path, network_hosts_path, log_dir_path,
  sandbox_contract_version, runsc_container_id, resource_state, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'allocating', ?)`,
		generationID, networkProfileID, agentRuntimeProfileID,
		resources.ControlDirPath, resources.ControlManifestPath, resources.BundleDirPath, resources.SpecPath,
		nullableString(resources.CheckpointPath), nullableString(resources.SecretsDirPath), resources.BridgeDirPath,
		nullableString(resources.NetworkHostsPath), resources.LogDirPath, SandboxContractVersion, runscContainerID, now); err != nil {
		return GenerationAllocation{}, err
	}
	if err := updateSessionActiveGenerationTx(ctx, tx, SessionActiveGenerationCASParams{
		SessionID:            p.SessionID,
		ExpectedGenerationID: p.ExpectedGenerationID,
		NextGenerationID:     generationID,
	}); err != nil {
		return GenerationAllocation{}, err
	}

	if err := tx.Commit(); err != nil {
		return GenerationAllocation{}, err
	}
	return GenerationAllocation{
		GenerationID:          generationID,
		NetworkProfileID:      networkProfileID,
		AgentRuntimeProfileID: agentRuntimeProfileID,
		Owner:                 p.Owner,
		LeaseExpiresAt:        leaseExpires,
		DriverState:           driverState,
	}, nil
}

func (s *Store) MarkGenerationStarting(ctx context.Context, sessionID, generationID, owner string, now time.Time) error {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	res, err := s.db.ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'starting',
    last_seen_at = ?
WHERE generation_id = ?
  AND session_id = ?
  AND status = 'allocating'
  AND lease_owner = ?
  AND lease_expires_at > ?
  AND EXISTS (
    SELECT 1 FROM sessions
    WHERE id = ?
      AND active_generation_id = ?
      AND status NOT IN ('failed', 'destroyed')
  )`, formatTime(now), generationID, sessionID, owner, formatTime(now), sessionID, generationID)
	if err != nil {
		return err
	}
	return requireOneRow(res, "generation starting CAS failed")
}

func (s *Store) MarkGenerationResourcesLive(ctx context.Context, sessionID, generationID, owner string, now time.Time) error {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	res, err := tx.ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'idle',
    last_seen_at = ?
WHERE generation_id = ?
  AND session_id = ?
  AND status IN ('allocating','starting','probing','restoring')
  AND lease_owner = ?
  AND lease_expires_at > ?
  AND EXISTS (
    SELECT 1 FROM sessions
    WHERE id = ?
      AND active_generation_id = ?
      AND status NOT IN ('failed', 'destroyed')
  )`, formatTime(now), generationID, sessionID, owner, formatTime(now), sessionID, generationID)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return fmt.Errorf("generation live CAS failed")
	}
	res, err = tx.ExecContext(ctx, `
UPDATE network_profiles
SET allocation_state = 'live'
WHERE generation_id = ?
  AND session_id = ?
  AND allocation_state IN ('allocating','ready','recreating')`, generationID, sessionID)
	if err != nil {
		return err
	}
	affected, err = res.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return fmt.Errorf("network allocation live CAS failed")
	}
	res, err = tx.ExecContext(ctx, `
UPDATE runtime_generation_resources
SET resource_state = 'live'
WHERE generation_id = ?
  AND resource_state IN ('allocating','ready','recreating')`, generationID)
	if err != nil {
		return err
	}
	affected, err = res.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return fmt.Errorf("generation resource live CAS failed")
	}
	return tx.Commit()
}

func (s *Store) ClaimCheckpointedGenerationForRestore(ctx context.Context, p ClaimCheckpointedGenerationParams) (GenerationAllocation, error) {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	if strings.TrimSpace(p.SessionID) == "" {
		return GenerationAllocation{}, fmt.Errorf("session id is required")
	}
	if strings.TrimSpace(p.GenerationID) == "" {
		return GenerationAllocation{}, fmt.Errorf("generation id is required")
	}
	if strings.TrimSpace(p.Owner) == "" {
		return GenerationAllocation{}, fmt.Errorf("owner is required")
	}
	if p.LeaseTTL <= 0 {
		return GenerationAllocation{}, fmt.Errorf("lease ttl must be > 0")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return GenerationAllocation{}, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := assertOwnerTx(ctx, tx, ownerUUIDFromLeaseOwner(p.Owner)); err != nil {
		return GenerationAllocation{}, err
	}
	currentFence, err := checkpointDriverStatesDigestTx(ctx, tx, p.SessionID, p.GenerationID)
	if err != nil {
		return GenerationAllocation{}, err
	}
	var storedFence string
	if err := tx.QueryRowContext(ctx, `
SELECT COALESCE(checkpoint_driver_states_digest, '')
FROM runtime_generations
WHERE generation_id = ?
  AND session_id = ?`, p.GenerationID, p.SessionID).Scan(&storedFence); err != nil {
		return GenerationAllocation{}, err
	}
	if storedFence == "" {
		return GenerationAllocation{}, fmt.Errorf("%w: checkpoint driver state fence is missing", ErrStaleCheckpointRestore)
	}
	if storedFence != currentFence {
		return GenerationAllocation{}, fmt.Errorf("%w: checkpoint driver state fence mismatch", ErrStaleCheckpointRestore)
	}
	plan, err := getGenerationPlanTx(ctx, tx, p.GenerationID)
	if err != nil {
		return GenerationAllocation{}, err
	}
	var checkpointPlanDigest string
	if err := tx.QueryRowContext(ctx, `
SELECT COALESCE(checkpoint_plan_digest, '')
FROM runtime_generations
WHERE generation_id = ?
  AND session_id = ?`, p.GenerationID, p.SessionID).Scan(&checkpointPlanDigest); err != nil {
		return GenerationAllocation{}, err
	}
	if checkpointPlanDigest == "" {
		return GenerationAllocation{}, fmt.Errorf("%w: checkpoint plan digest is missing", ErrStaleCheckpointRestore)
	}
	if checkpointPlanDigest != plan.PlanDigest {
		return GenerationAllocation{}, fmt.Errorf("%w: checkpoint plan digest mismatch", ErrStaleCheckpointRestore)
	}
	var checkpointBundleDigest, checkpointRuntimeConfigDigest, checkpointControlManifestDigest string
	if err := tx.QueryRowContext(ctx, `
SELECT COALESCE(checkpoint_bundle_digest, ''),
       COALESCE(checkpoint_runtime_config_digest, ''),
       COALESCE(checkpoint_control_manifest_digest, '')
FROM runtime_generations
WHERE generation_id = ?
  AND session_id = ?`, p.GenerationID, p.SessionID).Scan(
		&checkpointBundleDigest, &checkpointRuntimeConfigDigest, &checkpointControlManifestDigest,
	); err != nil {
		return GenerationAllocation{}, err
	}
	if err := verifyCheckpointProjectionDigestChecksTx(ctx, tx, p.GenerationID, checkpointPlanDigest, checkpointProjectionDigestChecks(
		checkpointBundleDigest,
		checkpointRuntimeConfigDigest,
		checkpointControlManifestDigest,
	)); err != nil {
		return GenerationAllocation{}, fmt.Errorf("%w: %w", ErrStaleCheckpointRestore, err)
	}

	expiresAt := p.Now.Add(p.LeaseTTL)
	res, err := tx.ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'restoring',
    lease_owner = ?,
    lease_expires_at = ?,
    last_seen_at = ?
WHERE generation_id = ?
  AND session_id = ?
  AND status = 'checkpointed'
  AND EXISTS (
    SELECT 1 FROM sessions
    WHERE id = ?
      AND active_generation_id = ?
      AND status = 'checkpointed'
  )`, p.Owner, formatTime(expiresAt), formatTime(p.Now), p.GenerationID, p.SessionID, p.SessionID, p.GenerationID)
	if err != nil {
		return GenerationAllocation{}, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return GenerationAllocation{}, err
	}
	if affected != 1 {
		stale, err := staleCheckpointRestoreTx(ctx, tx, p.SessionID, p.GenerationID)
		if err != nil {
			return GenerationAllocation{}, err
		}
		if stale {
			return GenerationAllocation{}, fmt.Errorf("%w: checkpointed generation restore CAS failed", ErrStaleCheckpointRestore)
		}
		return GenerationAllocation{}, fmt.Errorf("checkpointed generation restore CAS failed")
	}

	res, err = tx.ExecContext(ctx, `
UPDATE network_profiles
SET allocation_state = 'recreating'
WHERE generation_id = ?
  AND session_id = ?
  AND allocation_state = 'reserved_checkpointed'`, p.GenerationID, p.SessionID)
	if err != nil {
		return GenerationAllocation{}, err
	}
	affected, err = res.RowsAffected()
	if err != nil {
		return GenerationAllocation{}, err
	}
	if affected != 1 {
		return GenerationAllocation{}, fmt.Errorf("checkpointed network restore CAS failed")
	}

	res, err = tx.ExecContext(ctx, `
UPDATE runtime_generation_resources
SET resource_state = 'recreating'
WHERE generation_id = ?
  AND resource_state = 'reserved_checkpointed'`, p.GenerationID)
	if err != nil {
		return GenerationAllocation{}, err
	}
	affected, err = res.RowsAffected()
	if err != nil {
		return GenerationAllocation{}, err
	}
	if affected != 1 {
		return GenerationAllocation{}, fmt.Errorf("checkpointed resource restore CAS failed")
	}

	allocation := GenerationAllocation{
		GenerationID:   p.GenerationID,
		Owner:          p.Owner,
		LeaseExpiresAt: expiresAt,
	}
	if err := tx.QueryRowContext(ctx, `
SELECT network_profile_id, agent_runtime_profile_id
FROM runtime_generations
WHERE generation_id = ?
  AND session_id = ?`, p.GenerationID, p.SessionID).Scan(&allocation.NetworkProfileID, &allocation.AgentRuntimeProfileID); err != nil {
		return GenerationAllocation{}, err
	}
	driverState, err := currentDriverStateTokenTx(ctx, tx, p.SessionID, p.GenerationID)
	if err != nil {
		return GenerationAllocation{}, err
	}
	allocation.DriverState = driverState
	if err := tx.Commit(); err != nil {
		return GenerationAllocation{}, err
	}
	return allocation, nil
}

func staleCheckpointRestoreTx(ctx context.Context, tx *sql.Tx, sessionID, generationID string) (bool, error) {
	var sessionStatus, activeGenerationID, generationStatus string
	if err := tx.QueryRowContext(ctx, `
SELECT s.status, COALESCE(s.active_generation_id, ''), COALESCE(g.status, '')
FROM sessions s
LEFT JOIN runtime_generations g ON g.session_id = s.id
  AND g.generation_id = ?
WHERE s.id = ?`, generationID, sessionID).Scan(&sessionStatus, &activeGenerationID, &generationStatus); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return activeGenerationID != generationID ||
		sessionStatus != "checkpointed" ||
		generationStatus != "checkpointed", nil
}

func (s *Store) SweepExpiredSessions(ctx context.Context, now time.Time) (int64, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	expiredSessionIDs, err := queryStringColumnTx(ctx, tx, `
SELECT id
FROM sessions
WHERE expires_at IS NOT NULL
  AND expires_at <= ?
  AND status NOT IN ('failed', 'destroyed')`, formatTime(now))
	if err != nil {
		return 0, err
	}
	if len(expiredSessionIDs) == 0 {
		return 0, tx.Commit()
	}

	nowString := formatTime(now)
	args := []any{nowString, nowString}
	args = appendStringIDs(args, expiredSessionIDs)
	res, err := tx.ExecContext(ctx, `
UPDATE sessions
SET status = 'destroyed',
    updated_at = ?,
    ended_at = COALESCE(ended_at, ?),
    error_class = COALESCE(error_class, 'session_expired'),
    failure_reason = COALESCE(failure_reason, 'session_expired')
WHERE id IN (`+sqlPlaceholders(len(expiredSessionIDs))+`)`, args...)
	if err != nil {
		return 0, err
	}
	changed, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	args = []any{nowString}
	args = appendStringIDs(args, expiredSessionIDs)
	if _, err := tx.ExecContext(ctx, `
UPDATE turns
SET status = 'canceled',
    completed_at = ?,
    error_class = 'session_expired',
    error = 'session_expired'
WHERE session_id IN (`+sqlPlaceholders(len(expiredSessionIDs))+`)
  AND status IN ('queued', 'leased')
  AND ack_started_at IS NULL`, args...); err != nil {
		return 0, err
	}

	args = appendStringIDs(nil, expiredSessionIDs)
	args = append(args, nowString)
	expiredGenerationIDs, err := queryStringColumnTx(ctx, tx, `
SELECT generation_id
FROM runtime_generations
WHERE session_id IN (`+sqlPlaceholders(len(expiredSessionIDs))+`)
  AND status NOT IN ('failed', 'destroyed')
  AND NOT EXISTS (
    SELECT 1 FROM turns
    WHERE turns.generation_id = runtime_generations.generation_id
      AND turns.status IN ('leased', 'running')
      AND turns.ack_started_at IS NOT NULL
      AND turns.lease_expires_at > ?
  )`, args...)
	if err != nil {
		return 0, err
	}
	if len(expiredGenerationIDs) > 0 {
		args = []any{nowString}
		args = appendStringIDs(args, expiredGenerationIDs)
		if _, err := tx.ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'failed',
    error_class = 'session_expired',
    failure_reason = 'session_expired',
    ended_at = ?,
    lease_owner = NULL
WHERE generation_id IN (`+sqlPlaceholders(len(expiredGenerationIDs))+`)`, args...); err != nil {
			return 0, err
		}
		if err := markAllocationsReclaimableTx(ctx, tx, expiredGenerationIDs); err != nil {
			return 0, err
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return changed, nil
}

func (s *Store) ClearActiveSessionExpiry(ctx context.Context, now time.Time) (int64, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	activeStatuses := sessionstate.ActiveStatuses()
	args := []any{formatTime(now)}
	args = appendStringIDs(args, activeStatuses)
	res, err := s.db.ExecContext(ctx, `
UPDATE sessions
SET expires_at = NULL,
    updated_at = ?
WHERE expires_at IS NOT NULL
  AND status IN (`+sqlPlaceholders(len(activeStatuses))+`)`, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *Store) ReapResources(ctx context.Context, p ReaperParams) (ReaperResult, error) {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ReaperResult{}, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := assertOwnerTx(ctx, tx, p.OwnerUUID); err != nil {
		return ReaperResult{}, err
	}

	cutoff := p.Now.Add(-p.FailedRetention)
	res, err := tx.ExecContext(ctx, `
UPDATE network_profiles
SET allocation_state = 'reclaimable'
WHERE allocation_state IN ('allocating','ready','live','reserved_checkpointed','recreating')
  AND generation_id IN (
    SELECT generation_id FROM runtime_generations
    WHERE status = 'failed'
      AND ended_at IS NOT NULL
      AND ended_at <= ?
  )`, formatTime(cutoff))
	if err != nil {
		return ReaperResult{}, err
	}
	failedMarked, err := res.RowsAffected()
	if err != nil {
		return ReaperResult{}, err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE runtime_generation_resources
SET resource_state = 'reclaimable'
WHERE resource_state IN ('allocating','ready','live','reserved_checkpointed','recreating')
  AND generation_id IN (
    SELECT generation_id FROM runtime_generations
    WHERE status = 'failed'
      AND ended_at IS NOT NULL
      AND ended_at <= ?
  )`, formatTime(cutoff)); err != nil {
		return ReaperResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return ReaperResult{}, err
	}
	return ReaperResult{FailedMarkedReclaimable: failedMarked}, nil
}

func (s *Store) ListDestroyableReclaimableGenerations(ctx context.Context, now time.Time, failedRetention time.Duration) ([]ReclaimableGeneration, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	cutoff := now.Add(-failedRetention)
	rows, err := s.db.QueryContext(ctx, `
SELECT n.session_id, n.generation_id
FROM network_profiles n
JOIN runtime_generation_resources r ON r.generation_id = n.generation_id
JOIN runtime_generations g ON g.generation_id = n.generation_id
WHERE n.allocation_state = 'reclaimable'
  AND r.resource_state = 'reclaimable'
  AND (
    g.status != 'failed'
    OR COALESCE(g.error_class, '') = 'checkpoint_retired'
    OR (g.ended_at IS NOT NULL AND g.ended_at <= ?)
  )
ORDER BY n.created_at, n.generation_id`, formatTime(cutoff))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var generations []ReclaimableGeneration
	for rows.Next() {
		var generation ReclaimableGeneration
		if err := rows.Scan(&generation.SessionID, &generation.GenerationID); err != nil {
			return nil, err
		}
		generations = append(generations, generation)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return generations, nil
}

func (s *Store) RetireExpiredCheckpoints(ctx context.Context, p RetireExpiredCheckpointsParams) ([]RetiredCheckpoint, error) {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	if p.CheckpointImageRetention < 0 {
		return nil, fmt.Errorf("checkpoint image retention must be >= 0")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := assertOwnerTx(ctx, tx, p.OwnerUUID); err != nil {
		return nil, err
	}

	cutoff := p.Now.Add(-p.CheckpointImageRetention)
	rows, err := tx.QueryContext(ctx, `
SELECT s.id, g.generation_id, s.last_activity_at
FROM sessions s
JOIN runtime_generations g ON g.generation_id = s.active_generation_id
  AND g.session_id = s.id
JOIN network_profiles n ON n.generation_id = g.generation_id
  AND n.session_id = s.id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE s.status = 'checkpointed'
  AND g.status = 'checkpointed'
  AND n.allocation_state = 'reserved_checkpointed'
  AND r.resource_state = 'reserved_checkpointed'
  AND COALESCE(s.last_activity_at, g.checkpoint_created_at, s.updated_at, s.created_at) < ?
ORDER BY COALESCE(s.last_activity_at, g.checkpoint_created_at, s.updated_at, s.created_at), s.id`, formatTime(cutoff))
	if err != nil {
		return nil, err
	}
	type candidate struct {
		sessionID      string
		generationID   string
		lastActivityAt sql.NullString
	}
	var candidates []candidate
	for rows.Next() {
		var candidate candidate
		if err := rows.Scan(&candidate.sessionID, &candidate.generationID, &candidate.lastActivityAt); err != nil {
			_ = rows.Close()
			return nil, err
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	retired := make([]RetiredCheckpoint, 0, len(candidates))
	nowString := formatTime(p.Now)
	for _, candidate := range candidates {
		res, err := tx.ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'failed',
    error_class = 'checkpoint_retired',
    failure_reason = 'checkpoint image retired after retention window',
    ended_at = ?,
    lease_owner = NULL,
    lease_expires_at = NULL
WHERE generation_id = ?
  AND session_id = ?
  AND status = 'checkpointed'
  AND EXISTS (
    SELECT 1 FROM sessions
    WHERE id = ?
      AND active_generation_id = ?
      AND status = 'checkpointed'
  )`, nowString, candidate.generationID, candidate.sessionID, candidate.sessionID, candidate.generationID)
		if err != nil {
			return nil, err
		}
		if affected, err := res.RowsAffected(); err != nil {
			return nil, err
		} else if affected != 1 {
			return nil, fmt.Errorf("checkpoint retirement generation CAS failed")
		}
		res, err = tx.ExecContext(ctx, `
UPDATE network_profiles
SET allocation_state = 'reclaimable'
WHERE generation_id = ?
  AND session_id = ?
  AND allocation_state = 'reserved_checkpointed'`, candidate.generationID, candidate.sessionID)
		if err != nil {
			return nil, err
		}
		if affected, err := res.RowsAffected(); err != nil {
			return nil, err
		} else if affected != 1 {
			return nil, fmt.Errorf("checkpoint retirement network CAS failed")
		}
		res, err = tx.ExecContext(ctx, `
UPDATE runtime_generation_resources
SET resource_state = 'reclaimable'
WHERE generation_id = ?
  AND resource_state = 'reserved_checkpointed'`, candidate.generationID)
		if err != nil {
			return nil, err
		}
		if affected, err := res.RowsAffected(); err != nil {
			return nil, err
		} else if affected != 1 {
			return nil, fmt.Errorf("checkpoint retirement resource CAS failed")
		}
		res, err = tx.ExecContext(ctx, `
UPDATE sessions
SET status = 'running_idle',
    checkpoint_path = NULL,
    restore_ms = NULL,
    updated_at = ?
WHERE id = ?
  AND status = 'checkpointed'
  AND active_generation_id = ?`, nowString, candidate.sessionID, candidate.generationID)
		if err != nil {
			return nil, err
		}
		if affected, err := res.RowsAffected(); err != nil {
			return nil, err
		} else if affected != 1 {
			return nil, fmt.Errorf("checkpoint retirement session CAS failed")
		}
		var lastActivity any
		if candidate.lastActivityAt.Valid {
			lastActivity = candidate.lastActivityAt.String
		}
		eventID, err := appendEventTx(ctx, tx, AppendEventParams{
			SessionID:    candidate.sessionID,
			GenerationID: candidate.generationID,
			DedupeKey:    "checkpoint_retired:" + candidate.generationID,
			Type:         "session.checkpoint_retired",
			Payload: map[string]any{
				"terminal":                 false,
				"generation_id":            candidate.generationID,
				"session_status":           "running_idle",
				"status":                   "running_idle",
				"session_updated_at":       nowString,
				"updated_at":               nowString,
				"session_last_activity_at": lastActivity,
				"active_generation_id":     candidate.generationID,
				"restore_ms":               nil,
			},
			Now: p.Now,
		})
		if err != nil {
			return nil, err
		}
		retired = append(retired, RetiredCheckpoint{SessionID: candidate.sessionID, GenerationID: candidate.generationID, EventID: eventID})
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return retired, nil
}

func (s *Store) MarkGenerationResourcesDestroyed(ctx context.Context, p DestroyGenerationResourcesParams) error {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	if strings.TrimSpace(p.SessionID) == "" {
		return fmt.Errorf("session id is required")
	}
	if strings.TrimSpace(p.GenerationID) == "" {
		return fmt.Errorf("generation id is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var networkState, resourceState string
	if err := tx.QueryRowContext(ctx, `
SELECT n.allocation_state, r.resource_state
FROM network_profiles n
JOIN runtime_generation_resources r ON r.generation_id = n.generation_id
WHERE n.session_id = ?
  AND n.generation_id = ?`, p.SessionID, p.GenerationID).Scan(&networkState, &resourceState); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("generation resources not found for session %q generation %q", p.SessionID, p.GenerationID)
		}
		return err
	}
	if networkState == "destroyed" && resourceState == "destroyed" {
		return tx.Commit()
	}
	if networkState != "reclaimable" {
		return fmt.Errorf("network allocation destroyed CAS failed: state=%q", networkState)
	}
	if resourceState != "reclaimable" {
		return fmt.Errorf("generation resource destroyed CAS failed: state=%q", resourceState)
	}
	res, err := tx.ExecContext(ctx, `
UPDATE network_profiles
SET allocation_state = 'destroyed',
    destroyed_at = COALESCE(destroyed_at, ?)
WHERE session_id = ?
  AND generation_id = ?
  AND allocation_state = 'reclaimable'`,
		formatTime(p.Now), p.SessionID, p.GenerationID)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return fmt.Errorf("network allocation destroyed CAS failed")
	}
	res, err = tx.ExecContext(ctx, `
UPDATE runtime_generation_resources
SET resource_state = 'destroyed',
    destroyed_at = COALESCE(destroyed_at, ?)
WHERE generation_id = ?
  AND resource_state = 'reclaimable'`, formatTime(p.Now), p.GenerationID)
	if err != nil {
		return err
	}
	affected, err = res.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return fmt.Errorf("generation resource destroyed CAS failed")
	}
	return tx.Commit()
}

func (s *Store) ListExpiredRuntimeRecoveryCandidates(ctx context.Context, p StartupRecoveryParams) ([]ExpiredRuntimeRecoveryCandidate, error) {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	if p.ReconnectGrace <= 0 {
		return nil, fmt.Errorf("reconnect grace must be > 0")
	}
	if p.AckStartedGrace <= 0 {
		return nil, fmt.Errorf("ack-started grace must be > 0")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := assertOwnerTx(ctx, tx, p.OwnerUUID); err != nil {
		return nil, err
	}

	var candidates []ExpiredRuntimeRecoveryCandidate
	rows, err := tx.QueryContext(ctx, `
SELECT s.id, g.generation_id, ri.runsc_container_id, g.status
FROM runtime_generations g
JOIN sessions s ON s.id = g.session_id
JOIN runtime_resource_instances ri ON ri.generation_id = g.generation_id
  AND ri.session_id = s.id
  AND ri.contract_id = g.sandbox_contract_id
  AND ri.sandbox_contract_version = g.sandbox_contract_version
  AND ri.sandbox_contract_version = 'sandbox-isolation-v1'
WHERE g.status IN ('allocating','starting','probing','restoring','checkpointing')
  AND g.lease_expires_at IS NOT NULL
  AND g.lease_expires_at <= ?
  AND s.active_generation_id = g.generation_id
  AND s.status NOT IN ('failed', 'destroyed')
ORDER BY s.id, g.generation_id`, formatTime(p.Now))
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var c ExpiredRuntimeRecoveryCandidate
		if err := rows.Scan(&c.SessionID, &c.GenerationID, &c.RuntimeID, &c.Status); err != nil {
			_ = rows.Close()
			return nil, err
		}
		c.ErrorClass = "orchestrator_restart_during_" + c.Status
		candidates = append(candidates, c)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	ackStartedCutoff := p.Now.Add(-p.AckStartedGrace)
	rows, err = tx.QueryContext(ctx, `
SELECT s.id, g.generation_id, ri.runsc_container_id, g.status
FROM runtime_generations g
JOIN sessions s ON s.id = g.session_id
JOIN runtime_resource_instances ri ON ri.generation_id = g.generation_id
  AND ri.session_id = s.id
  AND ri.contract_id = g.sandbox_contract_id
  AND ri.sandbox_contract_version = g.sandbox_contract_version
  AND ri.sandbox_contract_version = 'sandbox-isolation-v1'
WHERE g.status IN ('active','idle')
  AND g.lease_expires_at IS NOT NULL
  AND g.lease_expires_at <= ?
  AND s.active_generation_id = g.generation_id
  AND s.status NOT IN ('failed', 'destroyed')
  AND EXISTS (
    SELECT 1 FROM turns
    WHERE turns.generation_id = g.generation_id
      AND turns.status = 'running'
      AND turns.ack_started_at IS NOT NULL
      AND turns.lease_expires_at IS NOT NULL
      AND turns.lease_expires_at <= ?
  )
ORDER BY s.id, g.generation_id`, formatTime(ackStartedCutoff), formatTime(ackStartedCutoff))
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var c ExpiredRuntimeRecoveryCandidate
		if err := rows.Scan(&c.SessionID, &c.GenerationID, &c.RuntimeID, &c.Status); err != nil {
			_ = rows.Close()
			return nil, err
		}
		c.ErrorClass = "unknown_after_ack_started"
		candidates = append(candidates, c)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	cutoff := p.Now.Add(-p.ReconnectGrace)
	rows, err = tx.QueryContext(ctx, `
SELECT s.id, g.generation_id, ri.runsc_container_id, g.status
FROM runtime_generations g
JOIN sessions s ON s.id = g.session_id
JOIN runtime_resource_instances ri ON ri.generation_id = g.generation_id
  AND ri.session_id = s.id
  AND ri.contract_id = g.sandbox_contract_id
  AND ri.sandbox_contract_version = g.sandbox_contract_version
  AND ri.sandbox_contract_version = 'sandbox-isolation-v1'
WHERE g.status IN ('active','idle')
  AND g.lease_expires_at IS NOT NULL
  AND g.lease_expires_at <= ?
  AND s.active_generation_id = g.generation_id
  AND s.status NOT IN ('failed', 'destroyed')
  AND NOT EXISTS (
    SELECT 1 FROM turns
    WHERE turns.generation_id = g.generation_id
      AND turns.status = 'running'
      AND turns.ack_started_at IS NOT NULL
  )
ORDER BY s.id, g.generation_id`, formatTime(cutoff))
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var c ExpiredRuntimeRecoveryCandidate
		if err := rows.Scan(&c.SessionID, &c.GenerationID, &c.RuntimeID, &c.Status); err != nil {
			_ = rows.Close()
			return nil, err
		}
		c.ErrorClass = "orchestrator_restart_reconnect_grace_expired"
		candidates = append(candidates, c)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return candidates, nil
}

func (s *Store) RepairExpiredRuntimeRecovery(ctx context.Context, p StartupRecoveryParams, candidates []ExpiredRuntimeRecoveryCandidate) (StartupRecoveryResult, error) {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	if p.ReconnectGrace <= 0 {
		return StartupRecoveryResult{}, fmt.Errorf("reconnect grace must be > 0")
	}
	if p.AckStartedGrace <= 0 {
		return StartupRecoveryResult{}, fmt.Errorf("ack-started grace must be > 0")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return StartupRecoveryResult{}, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := assertOwnerTx(ctx, tx, p.OwnerUUID); err != nil {
		return StartupRecoveryResult{}, err
	}
	result := StartupRecoveryResult{}
	now := formatTime(p.Now)
	lifecycleIDs, unknownIDs, reconnectIDs := recoveryCandidateIDs(candidates)

	lifecycleIDs, err = filterLifecycleRecoveryIDsTx(ctx, tx, lifecycleIDs, p.Now)
	if err != nil {
		return StartupRecoveryResult{}, err
	}
	if len(lifecycleIDs) > 0 {
		requeued, err := requeueExpiredLeasedTurnsTx(ctx, tx, lifecycleIDs, p.Now)
		if err != nil {
			return StartupRecoveryResult{}, err
		}
		result.ExpiredLeasedRequeued += requeued
		args := []any{now}
		args = appendStringIDs(args, lifecycleIDs)
		if _, err := tx.ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'failed',
    error_class = 'orchestrator_restart_during_' || status,
    failure_reason = 'orchestrator_restart_during_' || status,
    ended_at = ?,
    lease_owner = NULL
WHERE generation_id IN (`+sqlPlaceholders(len(lifecycleIDs))+`)`, args...); err != nil {
			return StartupRecoveryResult{}, err
		}
		if err := markAllocationsReclaimableTx(ctx, tx, lifecycleIDs); err != nil {
			return StartupRecoveryResult{}, err
		}
		if err := deleteActiveContextsForGenerationsTx(ctx, tx, lifecycleIDs); err != nil {
			return StartupRecoveryResult{}, err
		}
		result.ExpiredLifecycleFailed = int64(len(lifecycleIDs))
	}

	unknownIDs, err = filterUnknownRecoveryIDsTx(ctx, tx, unknownIDs, p.Now.Add(-p.AckStartedGrace))
	if err != nil {
		return StartupRecoveryResult{}, err
	}
	if len(unknownIDs) > 0 {
		args := []any{now}
		args = appendStringIDs(args, unknownIDs)
		res, err := tx.ExecContext(ctx, `
UPDATE turns
SET status = 'failed',
    completed_at = ?,
    completed_by_generation = generation_id,
    error_class = 'unknown_after_ack_started',
    error = 'unknown_after_ack_started',
    lease_owner = NULL,
    lease_expires_at = NULL
WHERE generation_id IN (`+sqlPlaceholders(len(unknownIDs))+`)
  AND status = 'running'
  AND ack_started_at IS NOT NULL`, args...)
		if err != nil {
			return StartupRecoveryResult{}, err
		}
		unknownTurns, err := res.RowsAffected()
		if err != nil {
			return StartupRecoveryResult{}, err
		}
		args = []any{now}
		args = appendStringIDs(args, unknownIDs)
		if _, err := tx.ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'failed',
    error_class = 'unknown_after_ack_started',
    failure_reason = 'unknown_after_ack_started',
    ended_at = ?,
    lease_owner = NULL
WHERE generation_id IN (`+sqlPlaceholders(len(unknownIDs))+`)`, args...); err != nil {
			return StartupRecoveryResult{}, err
		}
		if err := markAllocationsReclaimableTx(ctx, tx, unknownIDs); err != nil {
			return StartupRecoveryResult{}, err
		}
		if err := deleteActiveContextsForGenerationsTx(ctx, tx, unknownIDs); err != nil {
			return StartupRecoveryResult{}, err
		}
		result.UnknownAfterAckStarted += unknownTurns
	}

	reconnectIDs, err = filterReconnectRecoveryIDsTx(ctx, tx, reconnectIDs, p.Now.Add(-p.ReconnectGrace))
	if err != nil {
		return StartupRecoveryResult{}, err
	}
	if len(reconnectIDs) > 0 {
		requeued, err := requeueExpiredLeasedTurnsTx(ctx, tx, reconnectIDs, p.Now)
		if err != nil {
			return StartupRecoveryResult{}, err
		}
		result.ExpiredLeasedRequeued += requeued
		args := []any{now}
		args = appendStringIDs(args, reconnectIDs)
		if _, err := tx.ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'failed',
    error_class = 'orchestrator_restart_reconnect_grace_expired',
    failure_reason = 'orchestrator_restart_reconnect_grace_expired',
    ended_at = ?,
    lease_owner = NULL
WHERE generation_id IN (`+sqlPlaceholders(len(reconnectIDs))+`)`, args...); err != nil {
			return StartupRecoveryResult{}, err
		}
		if err := markAllocationsReclaimableTx(ctx, tx, reconnectIDs); err != nil {
			return StartupRecoveryResult{}, err
		}
		if err := deleteActiveContextsForGenerationsTx(ctx, tx, reconnectIDs); err != nil {
			return StartupRecoveryResult{}, err
		}
		result.ReconnectGraceFailed = int64(len(reconnectIDs))
	}
	repairedIDs := append(append([]string{}, lifecycleIDs...), unknownIDs...)
	repairedIDs = append(repairedIDs, reconnectIDs...)
	for _, generationID := range repairedIDs {
		eventID, err := repairRecoveredSessionTx(ctx, tx, generationID, p.Now)
		if err != nil {
			return StartupRecoveryResult{}, err
		}
		if eventID != 0 {
			result.EventIDs = append(result.EventIDs, eventID)
		}
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM active_model_request_contexts
WHERE lease_owner NOT LIKE ?`, p.OwnerUUID+":%"); err != nil {
		return StartupRecoveryResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return StartupRecoveryResult{}, err
	}
	return result, nil
}

func recoveryCandidateIDs(candidates []ExpiredRuntimeRecoveryCandidate) (lifecycleIDs, unknownIDs, reconnectIDs []string) {
	for _, candidate := range candidates {
		switch candidate.ErrorClass {
		case "unknown_after_ack_started":
			unknownIDs = append(unknownIDs, candidate.GenerationID)
		case "orchestrator_restart_reconnect_grace_expired":
			reconnectIDs = append(reconnectIDs, candidate.GenerationID)
		default:
			if strings.HasPrefix(candidate.ErrorClass, "orchestrator_restart_during_") {
				lifecycleIDs = append(lifecycleIDs, candidate.GenerationID)
			}
		}
	}
	return lifecycleIDs, unknownIDs, reconnectIDs
}

func filterLifecycleRecoveryIDsTx(ctx context.Context, tx *sql.Tx, ids []string, now time.Time) ([]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	args := appendStringIDs([]any{formatTime(now)}, ids)
	return queryStringColumnTx(ctx, tx, `
SELECT generation_id
FROM runtime_generations
WHERE status IN ('allocating','starting','probing','restoring','checkpointing')
  AND lease_expires_at IS NOT NULL
  AND lease_expires_at <= ?
  AND EXISTS (
    SELECT 1 FROM sessions
    WHERE sessions.id = runtime_generations.session_id
      AND sessions.active_generation_id = runtime_generations.generation_id
      AND sessions.status NOT IN ('failed', 'destroyed')
  )
  AND generation_id IN (`+sqlPlaceholders(len(ids))+`)`, args...)
}

func filterUnknownRecoveryIDsTx(ctx context.Context, tx *sql.Tx, ids []string, cutoff time.Time) ([]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	args := appendStringIDs([]any{formatTime(cutoff), formatTime(cutoff)}, ids)
	return queryStringColumnTx(ctx, tx, `
SELECT generation_id
FROM runtime_generations
WHERE status IN ('active','idle')
  AND lease_expires_at IS NOT NULL
  AND lease_expires_at <= ?
  AND EXISTS (
    SELECT 1 FROM sessions
    WHERE sessions.id = runtime_generations.session_id
      AND sessions.active_generation_id = runtime_generations.generation_id
      AND sessions.status NOT IN ('failed', 'destroyed')
  )
  AND EXISTS (
    SELECT 1 FROM turns
    WHERE turns.generation_id = runtime_generations.generation_id
      AND turns.status = 'running'
      AND turns.ack_started_at IS NOT NULL
      AND turns.lease_expires_at IS NOT NULL
      AND turns.lease_expires_at <= ?
  )
  AND generation_id IN (`+sqlPlaceholders(len(ids))+`)`, args...)
}

func filterReconnectRecoveryIDsTx(ctx context.Context, tx *sql.Tx, ids []string, cutoff time.Time) ([]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	args := appendStringIDs([]any{formatTime(cutoff)}, ids)
	return queryStringColumnTx(ctx, tx, `
SELECT generation_id
FROM runtime_generations
WHERE status IN ('active','idle')
  AND lease_expires_at IS NOT NULL
  AND lease_expires_at <= ?
  AND EXISTS (
    SELECT 1 FROM sessions
    WHERE sessions.id = runtime_generations.session_id
      AND sessions.active_generation_id = runtime_generations.generation_id
      AND sessions.status NOT IN ('failed', 'destroyed')
  )
  AND NOT EXISTS (
    SELECT 1 FROM turns
    WHERE turns.generation_id = runtime_generations.generation_id
      AND turns.status = 'running'
      AND turns.ack_started_at IS NOT NULL
  )
  AND generation_id IN (`+sqlPlaceholders(len(ids))+`)`, args...)
}

func repairRecoveredSessionTx(ctx context.Context, tx *sql.Tx, generationID string, now time.Time) (int64, error) {
	nowString := formatTime(now)
	res, err := tx.ExecContext(ctx, `
UPDATE sessions
SET status = CASE
      WHEN EXISTS (
        SELECT 1 FROM turns
        WHERE turns.session_id = sessions.id
          AND turns.status IN ('leased','running')
      ) THEN 'running_active'
      ELSE 'running_idle'
    END,
    checkpoint_path = NULL,
    restore_ms = NULL,
    error_class = NULL,
    failure_reason = NULL,
    updated_at = ?
WHERE active_generation_id = ?
  AND status NOT IN ('failed', 'destroyed')`, nowString, generationID)
	if err != nil {
		return 0, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	if affected == 0 {
		return 0, nil
	}
	var sessionID, sessionStatus, errorClass, reason string
	if err := tx.QueryRowContext(ctx, `
SELECT s.id, s.status, COALESCE(g.error_class, ''), COALESCE(g.failure_reason, '')
FROM sessions s
JOIN runtime_generations g ON g.generation_id = s.active_generation_id
WHERE s.active_generation_id = ?`, generationID).Scan(&sessionID, &sessionStatus, &errorClass, &reason); err != nil {
		return 0, err
	}
	return appendEventTx(ctx, tx, AppendEventParams{
		SessionID:    sessionID,
		GenerationID: generationID,
		DedupeKey:    "runtime_recovery:" + generationID + ":" + errorClass,
		Type:         "generation.error",
		Payload: map[string]any{
			"terminal":             false,
			"error_class":          errorClass,
			"error":                reason,
			"generation_id":        generationID,
			"session_status":       sessionStatus,
			"session_updated_at":   nowString,
			"active_generation_id": generationID,
			"restore_ms":           nil,
		},
		Now: now,
	})
}

func (s *Store) RenewLiveGenerationLeases(ctx context.Context, p RenewLiveGenerationsParams) (int64, error) {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	if strings.TrimSpace(p.Owner) == "" {
		return 0, fmt.Errorf("owner is required")
	}
	if p.LeaseTTL <= 0 {
		return 0, fmt.Errorf("lease ttl must be > 0")
	}
	expiresAt := p.Now.Add(p.LeaseTTL)
	res, err := s.db.ExecContext(ctx, `
UPDATE runtime_generations
SET lease_expires_at = ?,
    last_seen_at = ?
WHERE status IN ('starting','probing','active','idle','checkpointing','restoring')
  AND lease_owner = ?
  AND lease_expires_at > ?
  AND EXISTS (
    SELECT 1 FROM sessions
    WHERE id = runtime_generations.session_id
      AND active_generation_id = runtime_generations.generation_id
      AND status NOT IN ('failed', 'destroyed')
  )`, formatTime(expiresAt), formatTime(p.Now), p.Owner, formatTime(p.Now))
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *Store) ListBridgePollGenerations(ctx context.Context, owner string, now time.Time, ackStartedGrace time.Duration) ([]BridgePollGeneration, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if strings.TrimSpace(owner) == "" {
		return nil, fmt.Errorf("owner is required")
	}
	args := []any{owner, formatTime(now)}
	recoverableWhere := ""
	if ackStartedGrace > 0 {
		cutoff := now.Add(-ackStartedGrace)
		recoverableWhere = `
  OR (
    g.status IN ('active','idle')
    AND g.lease_expires_at IS NOT NULL
    AND g.lease_expires_at <= ?
    AND g.lease_expires_at > ?
    AND EXISTS (
      SELECT 1 FROM turns t
      WHERE t.session_id = g.session_id
        AND t.generation_id = g.generation_id
        AND t.status = 'running'
        AND t.ack_started_at IS NOT NULL
        AND t.lease_expires_at IS NOT NULL
        AND t.lease_expires_at <= ?
        AND t.lease_expires_at > ?
    )
  )`
		args = append(args, formatTime(now), formatTime(cutoff), formatTime(now), formatTime(cutoff))
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT g.session_id, g.generation_id, ri.bridge_dir_path
FROM runtime_generations g
JOIN runtime_resource_instances ri ON ri.generation_id = g.generation_id
  AND ri.session_id = g.session_id
JOIN sessions s ON s.id = g.session_id
WHERE g.status IN ('active','idle','probing','restoring','starting')
  AND (
    (g.lease_owner = ? AND g.lease_expires_at > ?)
`+recoverableWhere+`
  )
  AND s.active_generation_id = g.generation_id
  AND s.status NOT IN ('failed', 'destroyed')
  AND ri.state = 'live'
ORDER BY g.session_id, g.generation_id`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var generations []BridgePollGeneration
	for rows.Next() {
		var generation BridgePollGeneration
		if err := rows.Scan(&generation.SessionID, &generation.GenerationID, &generation.BridgeDirPath); err != nil {
			return nil, err
		}
		generations = append(generations, generation)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, generation := range generations {
		if _, err := s.GetSandboxContractForGeneration(ctx, generation.SessionID, generation.GenerationID); err != nil {
			return nil, err
		}
	}
	return generations, nil
}

func (s *Store) GetRuntimeGenerationStatus(ctx context.Context, sessionID, generationID string) (string, error) {
	var status string
	err := s.db.QueryRowContext(ctx, `
SELECT status
FROM runtime_generations
WHERE session_id = ?
  AND generation_id = ?`, sessionID, generationID).Scan(&status)
	return status, err
}

func (s *Store) GetResourceQuota(ctx context.Context) (ResourceQuota, error) {
	var quota ResourceQuota
	err := s.db.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM network_profiles
WHERE allocation_state != 'destroyed'`).Scan(&quota.AllocatedPoolSlots)
	return quota, err
}

func (s *Store) ListAutoCheckpointCandidates(ctx context.Context, owner string, now time.Time, idleThreshold time.Duration) ([]CheckpointCandidate, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if strings.TrimSpace(owner) == "" {
		return nil, fmt.Errorf("owner is required")
	}
	if idleThreshold < 0 {
		return nil, fmt.Errorf("idle threshold must be >= 0")
	}
	cutoff := now.Add(-idleThreshold)
	rows, err := s.db.QueryContext(ctx, `
SELECT s.id, g.generation_id, ri.bridge_dir_path
FROM sessions s
JOIN runtime_generations g ON g.generation_id = s.active_generation_id
  AND g.session_id = s.id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
JOIN runtime_resource_instances ri ON ri.generation_id = g.generation_id
  AND ri.session_id = g.session_id
WHERE s.status = 'running_idle'
  AND s.auto_checkpoint_enabled = 1
  AND s.last_activity_at IS NOT NULL
  AND s.last_activity_at <= ?
  AND g.status = 'idle'
  AND g.auto_checkpoint_enabled = 1
  AND g.lease_owner = ?
  AND g.lease_expires_at > ?
  AND g.runsc_version IS NOT NULL
  AND g.runsc_platform IS NOT NULL
  AND ri.state = 'live'
  AND r.checkpoint_path IS NOT NULL
  AND r.control_manifest_digest IS NOT NULL
  AND r.projected_control_manifest_digest IS NOT NULL
  AND r.bundle_digest IS NOT NULL
  AND r.runtime_config_digest IS NOT NULL
  AND r.spec_digest IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM turns t
    WHERE t.session_id = s.id
      AND t.status IN ('queued', 'leased', 'running')
  )
ORDER BY s.last_activity_at ASC`, formatTime(cutoff), owner, formatTime(now))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var candidates []CheckpointCandidate
	for rows.Next() {
		var candidate CheckpointCandidate
		if err := rows.Scan(&candidate.SessionID, &candidate.GenerationID, &candidate.BridgeDirPath); err != nil {
			return nil, err
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, candidate := range candidates {
		if _, err := s.GetSandboxContractForGeneration(ctx, candidate.SessionID, candidate.GenerationID); err != nil {
			return nil, err
		}
	}
	return candidates, nil
}

func (s *Store) BeginGenerationCheckpoint(ctx context.Context, sessionID, generationID, owner string, now time.Time) error {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := getSandboxContractForGenerationWithGenerationMirror(ctx, tx, sessionID, generationID); err != nil {
		return err
	}
	checkpointDriverStatesDigest, err := checkpointDriverStatesDigestTx(ctx, tx, sessionID, generationID)
	if err != nil {
		return err
	}
	plan, err := getGenerationPlanTx(ctx, tx, generationID)
	if err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'checkpointing',
    last_seen_at = ?,
    checkpoint_driver_states_digest = ?,
    checkpoint_plan_digest = ?
WHERE generation_id = ?
  AND session_id = ?
  AND status = 'idle'
  AND auto_checkpoint_enabled = 1
  AND lease_owner = ?
  AND lease_expires_at > ?
  AND runsc_version IS NOT NULL
  AND runsc_platform IS NOT NULL
  AND EXISTS (
    SELECT 1 FROM sessions
    WHERE id = ?
      AND active_generation_id = ?
      AND status = 'running_idle'
      AND auto_checkpoint_enabled = 1
  )
  AND EXISTS (
    SELECT 1 FROM runtime_generation_resources r
    JOIN runtime_resource_instances ri ON ri.generation_id = r.generation_id
    WHERE r.generation_id = runtime_generations.generation_id
      AND ri.session_id = runtime_generations.session_id
      AND ri.state = 'live'
      AND r.checkpoint_path IS NOT NULL
      AND r.control_manifest_digest IS NOT NULL
      AND r.projected_control_manifest_digest IS NOT NULL
      AND r.bundle_digest IS NOT NULL
      AND r.runtime_config_digest IS NOT NULL
      AND r.spec_digest IS NOT NULL
  )
  AND NOT EXISTS (
    SELECT 1 FROM turns
    WHERE session_id = ?
      AND status IN ('queued', 'leased', 'running')
  )`, formatTime(now), checkpointDriverStatesDigest, plan.PlanDigest, generationID, sessionID, owner, formatTime(now), sessionID, generationID, sessionID)
	if err != nil {
		return err
	}
	if affected, err := res.RowsAffected(); err != nil {
		return err
	} else if affected != 1 {
		return fmt.Errorf("generation checkpoint begin CAS failed")
	}
	res, err = tx.ExecContext(ctx, `
UPDATE sessions
SET status = 'checkpointing',
    updated_at = ?
WHERE id = ?
  AND status = 'running_idle'
  AND active_generation_id = ?
  AND auto_checkpoint_enabled = 1`, formatTime(now), sessionID, generationID)
	if err != nil {
		return err
	}
	if affected, err := res.RowsAffected(); err != nil {
		return err
	} else if affected != 1 {
		return fmt.Errorf("session checkpoint begin CAS failed")
	}
	return tx.Commit()
}

func (s *Store) AbortGenerationCheckpoint(ctx context.Context, sessionID, generationID, owner string, now time.Time) error {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	res, err := tx.ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'idle',
    last_seen_at = ?,
    checkpoint_driver_states_digest = NULL,
    checkpoint_plan_digest = NULL
WHERE generation_id = ?
  AND session_id = ?
  AND status = 'checkpointing'
  AND lease_owner = ?
  AND lease_expires_at > ?`, formatTime(now), generationID, sessionID, owner, formatTime(now))
	if err != nil {
		return err
	}
	if affected, err := res.RowsAffected(); err != nil {
		return err
	} else if affected != 1 {
		return fmt.Errorf("generation checkpoint abort CAS failed")
	}
	res, err = tx.ExecContext(ctx, `
UPDATE sessions
SET status = 'running_idle',
    updated_at = ?
WHERE id = ?
  AND status = 'checkpointing'
  AND active_generation_id = ?`, formatTime(now), sessionID, generationID)
	if err != nil {
		return err
	}
	if affected, err := res.RowsAffected(); err != nil {
		return err
	} else if affected != 1 {
		return fmt.Errorf("session checkpoint abort CAS failed")
	}
	return tx.Commit()
}

func (s *Store) CompleteGenerationCheckpoint(ctx context.Context, p CompleteCheckpointParams) error {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	if strings.TrimSpace(p.CheckpointPath) == "" {
		return fmt.Errorf("checkpoint path is required")
	}
	if strings.TrimSpace(p.CheckpointPath) != p.CheckpointPath ||
		!filepath.IsAbs(p.CheckpointPath) ||
		filepath.Clean(p.CheckpointPath) != p.CheckpointPath {
		return fmt.Errorf("checkpoint path must be canonical absolute")
	}
	if strings.TrimSpace(p.CheckpointBundleDigest) == "" {
		return fmt.Errorf("checkpoint bundle digest is required")
	}
	if strings.TrimSpace(p.CheckpointRuntimeConfigDigest) == "" {
		return fmt.Errorf("checkpoint runtime config digest is required")
	}
	if strings.TrimSpace(p.CheckpointControlManifestDigest) == "" {
		return fmt.Errorf("checkpoint control manifest digest is required")
	}
	if strings.TrimSpace(p.CheckpointPlanDigest) == "" {
		return fmt.Errorf("checkpoint plan digest is required")
	}
	if strings.TrimSpace(p.RunscVersion) == "" {
		return fmt.Errorf("checkpoint runsc version is required")
	}
	if strings.TrimSpace(p.RunscPlatform) == "" {
		return fmt.Errorf("checkpoint runsc platform is required")
	}
	if strings.TrimSpace(p.RunscBinaryPath) == "" {
		return fmt.Errorf("checkpoint runsc binary path is required")
	}
	if strings.TrimSpace(p.RunscBinaryPath) != p.RunscBinaryPath ||
		!filepath.IsAbs(p.RunscBinaryPath) ||
		filepath.Clean(p.RunscBinaryPath) != p.RunscBinaryPath {
		return fmt.Errorf("checkpoint runsc binary path must be canonical absolute")
	}
	if strings.TrimSpace(p.RunscBinaryDigest) == "" {
		return fmt.Errorf("checkpoint runsc binary digest is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	currentFence, err := checkpointDriverStatesDigestTx(ctx, tx, p.SessionID, p.GenerationID)
	if err != nil {
		return err
	}
	var storedFence string
	if err := tx.QueryRowContext(ctx, `
SELECT COALESCE(checkpoint_driver_states_digest, '')
FROM runtime_generations
WHERE generation_id = ?
  AND session_id = ?`, p.GenerationID, p.SessionID).Scan(&storedFence); err != nil {
		return err
	}
	if storedFence == "" {
		return fmt.Errorf("checkpoint driver state fence is missing")
	}
	if storedFence != currentFence {
		return fmt.Errorf("checkpoint driver state fence mismatch")
	}
	var storedPlanDigest string
	if err := tx.QueryRowContext(ctx, `
SELECT COALESCE(checkpoint_plan_digest, '')
FROM runtime_generations
WHERE generation_id = ?
  AND session_id = ?`, p.GenerationID, p.SessionID).Scan(&storedPlanDigest); err != nil {
		return err
	}
	if storedPlanDigest == "" {
		return fmt.Errorf("checkpoint plan digest is missing")
	}
	if storedPlanDigest != strings.TrimSpace(p.CheckpointPlanDigest) {
		return fmt.Errorf("checkpoint plan digest mismatch")
	}
	plan, err := getGenerationPlanTx(ctx, tx, p.GenerationID)
	if err != nil {
		return err
	}
	if plan.PlanDigest != storedPlanDigest {
		return fmt.Errorf("checkpoint stored plan digest mismatch")
	}
	if err := verifyCheckpointProjectionDigestsTx(ctx, tx, p); err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'checkpointed',
    checkpoint_created_at = ?,
    checkpoint_network_profile_id = network_profile_id,
    checkpoint_agent_runtime_profile_id = agent_runtime_profile_id,
    checkpoint_runsc_version = ?,
    checkpoint_runsc_platform = ?,
    checkpoint_runsc_binary_path = ?,
    checkpoint_runsc_binary_digest = ?,
    checkpoint_bundle_digest = ?,
    checkpoint_runtime_config_digest = ?,
    checkpoint_control_manifest_digest = ?,
    checkpoint_plan_digest = ?,
    lease_owner = NULL,
    lease_expires_at = NULL,
    last_seen_at = ?
WHERE generation_id = ?
  AND session_id = ?
  AND status = 'checkpointing'
  AND lease_owner = ?
  AND lease_expires_at > ?
  AND EXISTS (
    SELECT 1 FROM sessions
    WHERE id = ?
      AND active_generation_id = ?
      AND status = 'checkpointing'
  )`, formatTime(p.Now), p.RunscVersion, p.RunscPlatform, p.RunscBinaryPath, p.RunscBinaryDigest,
		p.CheckpointBundleDigest, p.CheckpointRuntimeConfigDigest, p.CheckpointControlManifestDigest,
		p.CheckpointPlanDigest, formatTime(p.Now), p.GenerationID, p.SessionID, p.Owner, formatTime(p.Now), p.SessionID, p.GenerationID)
	if err != nil {
		return err
	}
	if affected, err := res.RowsAffected(); err != nil {
		return err
	} else if affected != 1 {
		return fmt.Errorf("generation checkpoint complete CAS failed")
	}
	res, err = tx.ExecContext(ctx, `
UPDATE network_profiles
SET allocation_state = 'reserved_checkpointed'
WHERE generation_id = ?
  AND session_id = ?
  AND allocation_state = 'live'`, p.GenerationID, p.SessionID)
	if err != nil {
		return err
	}
	if affected, err := res.RowsAffected(); err != nil {
		return err
	} else if affected != 1 {
		return fmt.Errorf("network checkpoint complete CAS failed")
	}
	res, err = tx.ExecContext(ctx, `
UPDATE runtime_generation_resources
SET resource_state = 'reserved_checkpointed',
    checkpoint_path = ?
WHERE generation_id = ?
  AND resource_state IN ('allocating','ready','live','recreating')
  AND EXISTS (
    SELECT 1 FROM runtime_resource_instances ri
    WHERE ri.generation_id = runtime_generation_resources.generation_id
      AND ri.session_id = ?
      AND ri.state = 'live'
  )`, p.CheckpointPath, p.GenerationID, p.SessionID)
	if err != nil {
		return err
	}
	if affected, err := res.RowsAffected(); err != nil {
		return err
	} else if affected != 1 {
		return fmt.Errorf("resource checkpoint complete CAS failed")
	}
	res, err = tx.ExecContext(ctx, `
UPDATE sessions
SET status = 'checkpointed',
    checkpoint_path = ?,
    updated_at = ?
WHERE id = ?
  AND status = 'checkpointing'
  AND active_generation_id = ?`, p.CheckpointPath, formatTime(p.Now), p.SessionID, p.GenerationID)
	if err != nil {
		return err
	}
	if affected, err := res.RowsAffected(); err != nil {
		return err
	} else if affected != 1 {
		return fmt.Errorf("session checkpoint complete CAS failed")
	}
	return tx.Commit()
}

type checkpointProjectionDigestCheck struct {
	kind   string
	digest string
}

func checkpointProjectionDigestChecks(bundleDigest, runtimeConfigDigest, controlManifestDigest string) []checkpointProjectionDigestCheck {
	return []checkpointProjectionDigestCheck{
		{kind: GenerationPlanProjectionBundle, digest: bundleDigest},
		{kind: GenerationPlanProjectionRuntimeConfig, digest: runtimeConfigDigest},
		{kind: GenerationPlanProjectionControlManifestProjected, digest: controlManifestDigest},
	}
}

func verifyCheckpointProjectionDigestsTx(ctx context.Context, tx *sql.Tx, p CompleteCheckpointParams) error {
	return verifyCheckpointProjectionDigestChecksTx(ctx, tx, p.GenerationID, p.CheckpointPlanDigest, checkpointProjectionDigestChecks(
		p.CheckpointBundleDigest,
		p.CheckpointRuntimeConfigDigest,
		p.CheckpointControlManifestDigest,
	))
}

func verifyCheckpointProjectionDigestChecksTx(ctx context.Context, tx *sql.Tx, generationID, planDigest string, checks []checkpointProjectionDigestCheck) error {
	for _, check := range checks {
		if strings.TrimSpace(check.digest) == "" {
			return fmt.Errorf("checkpoint projection %s digest is missing", check.kind)
		}
		projection, err := getGenerationPlanProjectionTx(ctx, tx, generationID, check.kind)
		if err != nil {
			return fmt.Errorf("checkpoint projection %s: %w", check.kind, err)
		}
		if projection.PlanDigest != strings.TrimSpace(planDigest) {
			return fmt.Errorf("checkpoint projection %s plan digest mismatch", check.kind)
		}
		expected := generationPlanProjectionPayloadDigest(check.kind, check.digest)
		if projection.PayloadDigest != expected {
			return fmt.Errorf("checkpoint projection %s digest mismatch", check.kind)
		}
	}
	return nil
}

func (s *Store) GetRuntimeGenerationDetails(ctx context.Context, sessionID, generationID string) (RuntimeGenerationDetails, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT
  g.session_id,
  g.generation_id,
  g.network_profile_id,
  g.agent_runtime_profile_id,
  COALESCE(g.runsc_platform, ''),
  COALESCE(r.sandbox_contract_version, ''),
  r.control_dir_path,
  r.control_manifest_path,
  r.bundle_dir_path,
  r.spec_path,
  COALESCE(r.checkpoint_path, ''),
  COALESCE(r.secrets_dir_path, ''),
  r.bridge_dir_path,
  COALESCE(r.network_hosts_path, ''),
  r.log_dir_path,
  COALESCE(r.control_manifest_digest, ''),
  COALESCE(r.projected_control_manifest_digest, ''),
  COALESCE(r.bundle_digest, ''),
  COALESCE(r.runtime_config_digest, ''),
  COALESCE(r.spec_digest, ''),
  COALESCE(r.runsc_container_id, ''),
  COALESCE(r.runsc_version, ''),
  COALESCE(r.runsc_binary_path, ''),
  COALESCE(r.runsc_binary_digest, ''),
  COALESCE(g.checkpoint_network_profile_id, ''),
  COALESCE(g.checkpoint_agent_runtime_profile_id, ''),
  COALESCE(g.checkpoint_runsc_version, ''),
  COALESCE(g.checkpoint_runsc_platform, ''),
  COALESCE(g.checkpoint_runsc_binary_path, ''),
  COALESCE(g.checkpoint_runsc_binary_digest, ''),
  COALESCE(g.checkpoint_bundle_digest, ''),
  COALESCE(g.checkpoint_runtime_config_digest, ''),
  COALESCE(g.checkpoint_control_manifest_digest, ''),
  COALESCE(g.checkpoint_driver_states_digest, ''),
  COALESCE(g.checkpoint_plan_digest, ''),
  n.runsc_network,
  n.runsc_overlay2,
  n.host_proxy_bind_url,
  n.proxy_port,
  n.host_gateway_ip,
  n.sandbox_base_url,
  n.probe_url,
  n.netns_name,
  n.netns_path,
  n.host_veth,
  n.sandbox_veth,
  n.sandbox_ip_cidr,
  n.host_side_cidr,
  n.egress_policy_id,
  e.policy_digest,
  n.allowed_egress_rules,
  n.doris_fe_hosts,
  n.doris_be_hosts,
  n.doris_ports,
  n.dns_policy,
  n.allocation_state,
  g.auto_checkpoint_enabled,
  a.driver_id,
  COALESCE(a.model, ''),
  a.output_format,
  a.disable_nonessential_traffic,
  a.sandbox_uid,
  a.sandbox_gid,
  a.sandbox_supplemental_gids,
  a.model_access_allowed,
  a.requires_secret_drop,
  COALESCE(a.manifest_model_proxy_base_url, ''),
  COALESCE(a.model_proxy_api_key_secret_id, ''),
  COALESCE(a.model_proxy_auth_token_secret_id, ''),
  COALESCE(a.secret_version, ''),
  COALESCE(ds.state_digest, ''),
  COALESCE(ds.state_version, 0),
  COALESCE(ds.state_payload, '')
FROM runtime_generations g
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
JOIN network_profiles n ON n.network_profile_id = g.network_profile_id
JOIN egress_policies e ON e.egress_policy_id = n.egress_policy_id
JOIN agent_runtime_profiles a ON a.agent_runtime_profile_id = g.agent_runtime_profile_id
LEFT JOIN session_driver_states ds ON ds.session_id = g.session_id
  AND ds.driver_id = a.driver_id
WHERE g.session_id = ?
  AND g.generation_id = ?`, sessionID, generationID)
	var details RuntimeGenerationDetails
	var disableNonessentialTraffic, modelAccessAllowed, requiresSecretDrop, autoCheckpointEnabled int
	var sandboxSupplementalGIDs string
	if err := row.Scan(
		&details.SessionID,
		&details.GenerationID,
		&details.NetworkProfileID,
		&details.AgentRuntimeProfileID,
		&details.RunscPlatform,
		&details.SandboxContractVersion,
		&details.ControlDirPath,
		&details.ControlManifestPath,
		&details.BundleDirPath,
		&details.SpecPath,
		&details.CheckpointPath,
		&details.SecretsDirPath,
		&details.BridgeDirPath,
		&details.NetworkHostsPath,
		&details.LogDirPath,
		&details.ControlManifestDigest,
		&details.ProjectedControlManifestDigest,
		&details.BundleDigest,
		&details.RuntimeConfigDigest,
		&details.SpecDigest,
		&details.RunscContainerID,
		&details.RunscVersion,
		&details.RunscBinaryPath,
		&details.RunscBinaryDigest,
		&details.CheckpointNetworkProfileID,
		&details.CheckpointAgentRuntimeProfileID,
		&details.CheckpointRunscVersion,
		&details.CheckpointRunscPlatform,
		&details.CheckpointRunscBinaryPath,
		&details.CheckpointRunscBinaryDigest,
		&details.CheckpointBundleDigest,
		&details.CheckpointRuntimeConfigDigest,
		&details.CheckpointControlManifestDigest,
		&details.CheckpointDriverStatesDigest,
		&details.CheckpointPlanDigest,
		&details.RunscNetwork,
		&details.RunscOverlay2,
		&details.HostProxyBindURL,
		&details.ProxyPort,
		&details.HostGatewayIP,
		&details.SandboxBaseURL,
		&details.ProbeURL,
		&details.NetnsName,
		&details.NetnsPath,
		&details.HostVeth,
		&details.SandboxVeth,
		&details.SandboxIPCIDR,
		&details.HostSideCIDR,
		&details.EgressPolicyID,
		&details.EgressPolicyDigest,
		&details.AllowedEgressRules,
		&details.DorisFEHosts,
		&details.DorisBEHosts,
		&details.DorisPorts,
		&details.DNSPolicy,
		&details.NetworkAllocationState,
		&autoCheckpointEnabled,
		&details.DriverID,
		&details.Model,
		&details.OutputFormat,
		&disableNonessentialTraffic,
		&details.SandboxUID,
		&details.SandboxGID,
		&sandboxSupplementalGIDs,
		&modelAccessAllowed,
		&requiresSecretDrop,
		&details.ManifestAnthropicBaseURL,
		&details.AnthropicAPIKeySecretID,
		&details.AnthropicAuthTokenSecretID,
		&details.SecretVersion,
		&details.DriverStateDigest,
		&details.DriverStateVersion,
		&details.DriverStatePayload,
	); err != nil {
		return RuntimeGenerationDetails{}, err
	}
	details.DisableNonessentialTraffic = disableNonessentialTraffic != 0
	if strings.TrimSpace(sandboxSupplementalGIDs) != "" {
		if err := json.Unmarshal([]byte(sandboxSupplementalGIDs), &details.SandboxSupplementalGIDs); err != nil {
			return RuntimeGenerationDetails{}, fmt.Errorf("parse sandbox supplemental gids: %w", err)
		}
	}
	details.ModelAccessAllowed = modelAccessAllowed != 0
	details.RequiresSecretDrop = requiresSecretDrop != 0
	details.AutoCheckpointEnabled = autoCheckpointEnabled != 0
	if err := validateRuntimeGenerationDetailsPaths(details); err != nil {
		return RuntimeGenerationDetails{}, err
	}
	return details, nil
}

func validateRuntimeGenerationDetailsPaths(details RuntimeGenerationDetails) error {
	required := []struct {
		label string
		path  string
	}{
		{"control dir path", details.ControlDirPath},
		{"control manifest path", details.ControlManifestPath},
		{"bundle dir path", details.BundleDirPath},
		{"spec path", details.SpecPath},
		{"checkpoint path", details.CheckpointPath},
		{"bridge dir path", details.BridgeDirPath},
		{"log dir path", details.LogDirPath},
	}
	for _, field := range required {
		if strings.TrimSpace(field.path) == "" {
			return fmt.Errorf("runtime generation %s is required", field.label)
		}
		if !runtimeGenerationDetailsPathIsCanonical(field.path) {
			return fmt.Errorf("runtime generation %s must be canonical absolute", field.label)
		}
	}
	optional := []struct {
		label string
		path  string
	}{
		{"secrets dir path", details.SecretsDirPath},
		{"network hosts path", details.NetworkHostsPath},
		{"runsc binary path", details.RunscBinaryPath},
		{"checkpoint runsc binary path", details.CheckpointRunscBinaryPath},
	}
	for _, field := range optional {
		if strings.TrimSpace(field.path) == "" {
			if field.path != "" {
				return fmt.Errorf("runtime generation %s must be canonical absolute", field.label)
			}
			continue
		}
		if !runtimeGenerationDetailsPathIsCanonical(field.path) {
			return fmt.Errorf("runtime generation %s must be canonical absolute", field.label)
		}
	}
	return nil
}

func runtimeGenerationDetailsPathIsCanonical(path string) bool {
	return strings.TrimSpace(path) == path && filepath.IsAbs(path) && filepath.Clean(path) == path
}

func (s *Store) RecordGenerationRuntimeArtifactDigests(ctx context.Context, generationID string, digests GenerationRuntimeArtifactDigests) error {
	if err := validateGenerationRuntimeArtifactDigests(digests); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `
UPDATE runtime_generation_resources
SET control_manifest_digest = ?,
    projected_control_manifest_digest = ?,
    bundle_digest = ?,
    runtime_config_digest = ?,
    spec_digest = ?,
    runsc_version = ?,
    runsc_binary_path = ?,
    runsc_binary_digest = ?
WHERE generation_id = ?`,
		digests.ControlManifestDigest,
		digests.ProjectedControlManifestDigest,
		digests.BundleDigest,
		digests.RuntimeConfigDigest,
		digests.SpecDigest,
		digests.RunscVersion,
		digests.RunscBinaryPath,
		digests.RunscBinaryDigest,
		generationID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE runtime_generations
SET runsc_version = ?
WHERE generation_id = ?`, digests.RunscVersion, generationID); err != nil {
		return err
	}
	return tx.Commit()
}

func validateGenerationRuntimeArtifactDigests(digests GenerationRuntimeArtifactDigests) error {
	required := []struct {
		name  string
		value string
	}{
		{"control manifest digest", digests.ControlManifestDigest},
		{"projected control manifest digest", digests.ProjectedControlManifestDigest},
		{"bundle digest", digests.BundleDigest},
		{"runtime config digest", digests.RuntimeConfigDigest},
		{"spec digest", digests.SpecDigest},
		{"runsc version", digests.RunscVersion},
		{"runsc binary path", digests.RunscBinaryPath},
		{"runsc binary digest", digests.RunscBinaryDigest},
	}
	for _, field := range required {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("runtime artifact %s is required", field.name)
		}
	}
	if strings.TrimSpace(digests.RunscBinaryPath) != digests.RunscBinaryPath ||
		!filepath.IsAbs(digests.RunscBinaryPath) ||
		filepath.Clean(digests.RunscBinaryPath) != digests.RunscBinaryPath {
		return fmt.Errorf("runtime artifact runsc binary path must be canonical absolute")
	}
	return nil
}

type networkAllocation struct {
	HostGatewayIP  string
	SandboxBaseURL string
	ProbeURL       string
	NetnsName      string
	NetnsPath      string
	HostVeth       string
	SandboxVeth    string
	SandboxIPCIDR  string
	HostSideCIDR   string
}

type resourcePaths struct {
	ControlDirPath      string
	ControlManifestPath string
	BundleDirPath       string
	SpecPath            string
	CheckpointPath      string
	SecretsDirPath      string
	BridgeDirPath       string
	NetworkHostsPath    string
	LogDirPath          string
}

func assertOwnerTx(ctx context.Context, tx *sql.Tx, ownerUUID string) error {
	if strings.TrimSpace(ownerUUID) == "" {
		return fmt.Errorf("owner uuid is required")
	}
	var got string
	if err := tx.QueryRowContext(ctx, `SELECT uuid FROM orchestrator_owner WHERE singleton = 1`).Scan(&got); err != nil {
		return err
	}
	if got != ownerUUID {
		return fmt.Errorf("orchestrator owner uuid mismatch: db=%s process=%s", got, ownerUUID)
	}
	return nil
}

func queryStringColumnTx(ctx context.Context, tx *sql.Tx, query string, args ...any) ([]string, error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var values []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func appendStringIDs(args []any, ids []string) []any {
	for _, id := range ids {
		args = append(args, id)
	}
	return args
}

func markAllocationsReclaimableTx(ctx context.Context, tx *sql.Tx, generationIDs []string) error {
	if len(generationIDs) == 0 {
		return nil
	}
	args := appendStringIDs(nil, generationIDs)
	if _, err := tx.ExecContext(ctx, `
UPDATE network_profiles
SET allocation_state = 'reclaimable'
WHERE allocation_state IN ('allocating','ready','live','reserved_checkpointed','recreating')
  AND generation_id IN (`+sqlPlaceholders(len(generationIDs))+`)`, args...); err != nil {
		return err
	}
	args = appendStringIDs(nil, generationIDs)
	if _, err := tx.ExecContext(ctx, `
UPDATE runtime_generation_resources
SET resource_state = 'reclaimable'
WHERE resource_state IN ('allocating','ready','live','reserved_checkpointed','recreating')
  AND generation_id IN (`+sqlPlaceholders(len(generationIDs))+`)`, args...); err != nil {
		return err
	}
	return nil
}

func requeueExpiredLeasedTurnsTx(ctx context.Context, tx *sql.Tx, generationIDs []string, now time.Time) (int64, error) {
	if len(generationIDs) == 0 {
		return 0, nil
	}
	args := appendStringIDs([]any{formatTime(now)}, generationIDs)
	res, err := tx.ExecContext(ctx, `
UPDATE turns
SET status = 'queued',
    generation_id = NULL,
    lease_owner = NULL,
    lease_expires_at = NULL,
    claim_request_id = NULL,
    claim_granted_at = NULL,
    started_at = NULL,
    ack_started_at = NULL,
    completed_by_generation = NULL,
    completed_at = NULL,
    error_class = NULL,
    error = NULL,
    attempt = attempt + 1
WHERE lease_expires_at IS NOT NULL
  AND lease_expires_at <= ?
  AND generation_id IN (`+sqlPlaceholders(len(generationIDs))+`)
  AND status = 'leased'
  AND ack_started_at IS NULL`, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func deleteActiveContextsForGenerationsTx(ctx context.Context, tx *sql.Tx, generationIDs []string) error {
	if len(generationIDs) == 0 {
		return nil
	}
	args := appendStringIDs(nil, generationIDs)
	_, err := tx.ExecContext(ctx, `
DELETE FROM active_model_request_contexts
WHERE generation_id IN (`+sqlPlaceholders(len(generationIDs))+`)`, args...)
	return err
}

func updateSessionActiveGenerationTx(ctx context.Context, tx *sql.Tx, p SessionActiveGenerationCASParams) error {
	args := []any{p.NextGenerationID, p.SessionID}
	where := "active_generation_id IS NULL"
	if p.ExpectedGenerationID.Valid {
		where = "active_generation_id = ?"
		args = append(args, p.ExpectedGenerationID.String)
	}
	res, err := tx.ExecContext(ctx, `
UPDATE sessions
SET active_generation_id = ?
WHERE id = ?
  AND `+where, args...)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return fmt.Errorf("session active generation CAS failed")
	}
	return nil
}

func nextFreeSlot(ctx context.Context, tx *sql.Tx, pool netip.Prefix) (uint64, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT host_side_cidr
FROM network_profiles
WHERE allocation_state != 'destroyed'`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	used := map[uint64]struct{}{}
	for rows.Next() {
		var cidr string
		if err := rows.Scan(&cidr); err != nil {
			return 0, err
		}
		prefix, err := netip.ParsePrefix(cidr)
		if err != nil {
			return 0, fmt.Errorf("invalid occupied network CIDR %q: %w", cidr, err)
		}
		if prefix.Bits() != 30 {
			return 0, fmt.Errorf("invalid occupied network CIDR %q: expected /30, got /%d", cidr, prefix.Bits())
		}
		if slot, ok := slotForPrefix(pool, prefix); ok {
			used[slot] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	capacity := uint64(1) << uint(30-pool.Bits())
	for slot := uint64(0); slot < capacity; slot++ {
		if _, ok := used[slot]; !ok {
			return slot, nil
		}
	}
	return 0, ErrPoolExhausted
}

func slotForPrefix(pool, prefix netip.Prefix) (uint64, bool) {
	if !pool.Contains(prefix.Addr()) {
		return 0, false
	}
	base := ip4ToUint32(pool.Addr())
	addr := ip4ToUint32(prefix.Addr())
	if addr < base {
		return 0, false
	}
	return uint64(addr-base) / 4, true
}

func buildNetworkAllocation(cfg ResourceAllocatorConfig, slot uint64, generationID string) (networkAllocation, error) {
	base := ip4ToUint32(cfg.CIDRPool.Addr())
	networkIP := base + uint32(slot*4)
	gatewayIP := uint32ToIP4(networkIP + 1)
	sandboxIP := uint32ToIP4(networkIP + 2)
	generationToken := shortGenerationToken(generationID)
	proxyPort := cfg.proxyPort()
	sandboxBaseURL := fmt.Sprintf("http://%s:%d", gatewayIP, proxyPort)
	return networkAllocation{
		HostGatewayIP:  gatewayIP.String(),
		SandboxBaseURL: sandboxBaseURL,
		ProbeURL:       sandboxBaseURL,
		NetnsName:      "harness-gen-" + generationToken,
		NetnsPath:      filepath.Join("/var/run/netns", "harness-gen-"+generationToken),
		HostVeth:       "hgen" + generationToken[:6] + "h",
		SandboxVeth:    "hgen" + generationToken[:6] + "s",
		SandboxIPCIDR:  sandboxIP.String() + "/30",
		HostSideCIDR:   netip.PrefixFrom(uint32ToIP4(networkIP), 30).String(),
	}, nil
}

func shortGenerationToken(generationID string) string {
	token := strings.NewReplacer("gen_", "", "-", "").Replace(generationID)
	if len(token) < 8 {
		return fmt.Sprintf("%08s", token)
	}
	return token[:8]
}

func buildResourcePaths(runDir, generationID string) resourcePaths {
	base := filepath.Join(runDir, "gen-"+generationID)
	controlDir := filepath.Join(runDir, "control", "gen-"+generationID)
	bundleDir := filepath.Join(runDir, "runtime", "gen-"+generationID)
	bridgeDir := filepath.Join(runDir, "bridge", "gen-"+generationID)
	return resourcePaths{
		ControlDirPath:      controlDir,
		ControlManifestPath: filepath.Join(controlDir, "session.json"),
		BundleDirPath:       bundleDir,
		SpecPath:            filepath.Join(bundleDir, "config.json"),
		CheckpointPath:      filepath.Join(base, "checkpoint"),
		SecretsDirPath:      filepath.Join(controlDir, "secrets"),
		BridgeDirPath:       bridgeDir,
		NetworkHostsPath:    filepath.Join(runDir, "network", "gen-"+generationID, "hosts"),
		LogDirPath:          filepath.Join(runDir, "logs", "gen-"+generationID),
	}
}

func generationRunscContainerID(generationID string) string {
	return "harness-gen-" + strings.TrimSpace(generationID)
}

func allowedEgressRules(hostGatewayIP string, cfg ResourceAllocatorConfig) []string {
	rules := []string{fmt.Sprintf("tcp:%s:%d", hostGatewayIP, cfg.proxyPort())}
	for _, host := range cfg.EgressDorisFEHosts {
		for _, port := range cfg.EgressDorisPorts {
			rules = append(rules, fmt.Sprintf("tcp:%s:%d", host, port))
		}
	}
	for _, host := range cfg.EgressDorisBEHosts {
		for _, port := range cfg.EgressDorisPorts {
			rules = append(rules, fmt.Sprintf("tcp:%s:%d", host, port))
		}
	}
	if egressAllowsDNS(cfg) {
		rules = append(rules, "udp:53", "tcp:53")
	}
	return rules
}

func egressAllowsDNS(cfg ResourceAllocatorConfig) bool {
	switch cfg.EgressDNSPolicy {
	case "always":
		return true
	case "hostnames_only":
		return egressUsesHostnames(cfg)
	default:
		return false
	}
}

func egressUsesHostnames(cfg ResourceAllocatorConfig) bool {
	return containsHostname(cfg.EgressDorisFEHosts) || containsHostname(cfg.EgressDorisBEHosts)
}

func containsHostname(hosts []string) bool {
	for _, host := range hosts {
		trimmed := strings.TrimSpace(host)
		if trimmed == "" {
			return true
		}
		if _, err := netip.ParseAddr(trimmed); err != nil {
			return true
		}
	}
	return false
}

func egressPolicyID(cfg ResourceAllocatorConfig) string {
	payload := fmt.Sprintf("proxy_port=%d", cfg.proxyPort()) + "|" +
		strings.Join(cfg.EgressDorisFEHosts, ",") + "|" +
		strings.Join(cfg.EgressDorisBEHosts, ",") + "|" +
		fmt.Sprint(cfg.EgressDorisPorts) + "|" + cfg.EgressDNSPolicy + "|" +
		fmt.Sprintf("dns_allowed=%t", egressAllowsDNS(cfg))
	return "egress_" + strings.ReplaceAll(payload, " ", "_")
}

func agentRuntimeProfileID(generationID string) string {
	return "arp_" + generationID
}

func (c ResourceAllocatorConfig) driverID() string {
	return strings.TrimSpace(c.DriverID)
}

func (c ResourceAllocatorConfig) outputFormat() string {
	return strings.TrimSpace(c.OutputFormat)
}

func (c ResourceAllocatorConfig) sandboxUID() int {
	return c.SandboxUID
}

func (c ResourceAllocatorConfig) sandboxGID() int {
	return c.SandboxGID
}

func (c ResourceAllocatorConfig) sandboxSupplementalGIDs() []int {
	if len(c.SandboxSupplementalGIDs) == 0 {
		return []int{}
	}
	out := append([]int(nil), c.SandboxSupplementalGIDs...)
	sort.Ints(out)
	return out
}

func (c ResourceAllocatorConfig) modelAccessAllowed() bool {
	return c.ModelAccessAllowed != nil && *c.ModelAccessAllowed
}

func (c ResourceAllocatorConfig) providerCredentialsHostOnly() bool {
	return c.ProviderCredentialsHostOnly
}

func (c ResourceAllocatorConfig) requiresNetworkHostsProjection() bool {
	if !c.providerCredentialsHostOnly() || !c.modelAccessAllowed() {
		return false
	}
	host := modelProxyBaseURLHost(c.sandboxModelProxyBaseURL())
	if host == "" {
		return false
	}
	if _, err := netip.ParseAddr(host); err == nil {
		return false
	}
	return true
}

func (c ResourceAllocatorConfig) manifestAnthropicBaseURL(baseURL string) string {
	if !c.modelAccessAllowed() {
		return ""
	}
	if c.providerCredentialsHostOnly() {
		return c.sandboxModelProxyBaseURL()
	}
	return strings.TrimSpace(baseURL)
}

func (c ResourceAllocatorConfig) sandboxModelProxyBaseURL() string {
	return strings.TrimSpace(c.SandboxModelProxyBaseURL)
}

func (c ResourceAllocatorConfig) validateAllocationBoundary() error {
	if c.driverID() == "" {
		return fmt.Errorf("driver id is required")
	}
	if c.outputFormat() == "" {
		return fmt.Errorf("output format is required")
	}
	if c.sandboxUID() <= 0 {
		return fmt.Errorf("sandbox uid must be > 0")
	}
	if c.sandboxGID() <= 0 {
		return fmt.Errorf("sandbox gid must be > 0")
	}
	if err := c.validateSandboxSupplementalGIDs(); err != nil {
		return err
	}
	if c.hostProxyBindURL() == "" {
		return fmt.Errorf("host proxy bind url is required")
	}
	if c.proxyPort() <= 0 {
		return fmt.Errorf("proxy port must be > 0")
	}
	if c.ModelAccessAllowed == nil {
		return fmt.Errorf("model access allowed must be explicitly set")
	}
	if c.modelAccessAllowed() && strings.TrimSpace(c.Model) == "" {
		return fmt.Errorf("model is required when model access is enabled")
	}
	if err := c.validateSandboxModelProxyBaseURL(); err != nil {
		return err
	}
	return nil
}

func (c ResourceAllocatorConfig) validateSandboxSupplementalGIDs() error {
	seen := map[int]struct{}{}
	for _, gid := range c.SandboxSupplementalGIDs {
		if gid <= 0 {
			return fmt.Errorf("sandbox supplemental gids must contain only positive gids")
		}
		if _, ok := seen[gid]; ok {
			return fmt.Errorf("sandbox supplemental gids contains duplicate gid %d", gid)
		}
		seen[gid] = struct{}{}
	}
	return nil
}

func (c ResourceAllocatorConfig) validateSandboxModelProxyBaseURL() error {
	if !c.providerCredentialsHostOnly() || !c.modelAccessAllowed() {
		return nil
	}
	raw := c.sandboxModelProxyBaseURL()
	if raw == "" {
		return fmt.Errorf("sandbox model proxy base url is required when host-only model access is enabled")
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("sandbox model proxy base url %q is invalid: %w", raw, err)
	}
	if parsed.Scheme != "http" {
		return fmt.Errorf("sandbox model proxy base url %q must use the local http proxy scheme", raw)
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("sandbox model proxy base url %q must not include userinfo, query, or fragment", raw)
	}
	if parsed.Path != "" && parsed.Path != "/" {
		return fmt.Errorf("sandbox model proxy base url %q must not include a path", raw)
	}
	host := strings.ToLower(strings.TrimSpace(parsed.Hostname()))
	if host == "" {
		return fmt.Errorf("sandbox model proxy base url %q must include a hostname alias", raw)
	}
	if _, err := netip.ParseAddr(host); err == nil {
		return fmt.Errorf("sandbox model proxy base url %q must use a stable hostname alias, not an IP literal", raw)
	}
	if modelProxyHostIsHostLocal(host) {
		return fmt.Errorf("sandbox model proxy base url %q must not use a host-local name", raw)
	}
	if modelProxyHostIsProviderUpstream(host) {
		return fmt.Errorf("sandbox model proxy base url %q must not point at a provider upstream", raw)
	}
	portRaw := parsed.Port()
	if portRaw == "" {
		return fmt.Errorf("sandbox model proxy base url %q must include an explicit port matching proxy port %d", raw, c.proxyPort())
	}
	port, err := strconv.Atoi(portRaw)
	if err != nil || port <= 0 || port > 65535 {
		return fmt.Errorf("sandbox model proxy base url %q contains invalid port %q", raw, portRaw)
	}
	if port != c.proxyPort() {
		return fmt.Errorf("sandbox model proxy base url %q port must match proxy port %d", raw, c.proxyPort())
	}
	return nil
}

func modelProxyBaseURLHost(raw string) string {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(parsed.Hostname()))
}

func modelProxyHostIsHostLocal(host string) bool {
	return host == "localhost" ||
		host == "host.docker.internal" ||
		strings.HasSuffix(host, ".localhost") ||
		strings.HasSuffix(host, ".local")
}

func modelProxyHostIsProviderUpstream(host string) bool {
	switch host {
	case "api.anthropic.com", "anthropic.com", "api.openai.com", "openai.com":
		return true
	default:
		return strings.HasSuffix(host, ".anthropic.com") || strings.HasSuffix(host, ".openai.com")
	}
}

func (c ResourceAllocatorConfig) hostProxyBindURL() string {
	return strings.TrimSpace(c.HostProxyBindURL)
}

func (c ResourceAllocatorConfig) proxyPort() int {
	return c.ProxyPort
}

func ip4ToUint32(addr netip.Addr) uint32 {
	a := addr.As4()
	return uint32(a[0])<<24 | uint32(a[1])<<16 | uint32(a[2])<<8 | uint32(a[3])
}

func uint32ToIP4(value uint32) netip.Addr {
	return netip.AddrFrom4([4]byte{byte(value >> 24), byte(value >> 16), byte(value >> 8), byte(value)})
}

func ownerUUIDFromLeaseOwner(owner string) string {
	before, _, ok := strings.Cut(owner, ":")
	if !ok {
		return owner
	}
	return before
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
