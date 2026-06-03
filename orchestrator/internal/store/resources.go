package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"path/filepath"
	"strings"
	"time"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/sessionstate"

	"github.com/google/uuid"
)

const RuntimeManagerRoleTag = "runtime_manager"

var ErrPoolExhausted = errors.New("pool exhausted")

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
	CheckpointImageManifestDigest   string
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

func (s *Store) GetResourceQuota(ctx context.Context) (ResourceQuota, error) {
	var quota ResourceQuota
	err := s.db.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM network_profiles
WHERE allocation_state != 'destroyed'`).Scan(&quota.AllocatedPoolSlots)
	return quota, err
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
  COALESCE(g.checkpoint_image_manifest_digest, ''),
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
		&details.CheckpointImageManifestDigest,
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
	if err := validateRuntimeGenerationDetailsDigests(details); err != nil {
		return RuntimeGenerationDetails{}, err
	}
	return details, nil
}

func validateRuntimeGenerationDetailsDigests(details RuntimeGenerationDetails) error {
	if details.CheckpointImageManifestDigest != "" &&
		(strings.TrimSpace(details.CheckpointImageManifestDigest) != details.CheckpointImageManifestDigest ||
			!strings.HasPrefix(details.CheckpointImageManifestDigest, "sha256:")) {
		return fmt.Errorf("runtime generation checkpoint image manifest digest is invalid")
	}
	return nil
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
		{"netns path", details.NetnsPath},
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
