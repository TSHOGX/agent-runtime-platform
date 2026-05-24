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

	"github.com/google/uuid"
)

const RuntimeManagerRoleTag = "runtime_manager"

var ErrPoolExhausted = errors.New("pool exhausted")

type ResourceAllocatorConfig struct {
	RunDir                     string
	CIDRPool                   netip.Prefix
	EgressDorisFEHosts         []string
	EgressDorisBEHosts         []string
	EgressDorisPorts           []int
	EgressDNSPolicy            string
	HostProxyBindURL           string
	ProxyPort                  int
	Agent                      string
	AgentModel                 string
	AgentOutputFormat          string
	DisableNonessentialTraffic bool
	AnthropicAPIKeySecretID    string
	AnthropicAuthTokenSecretID string
	SecretVersion              string
}

type AllocateGenerationParams struct {
	SessionID string
	Owner     string
	LeaseTTL  time.Duration
	Now       time.Time
	Config    ResourceAllocatorConfig
}

type GenerationAllocation struct {
	GenerationID          string
	NetworkProfileID      string
	AgentRuntimeProfileID string
	Owner                 string
	LeaseExpiresAt        time.Time
}

type RuntimeGenerationDetails struct {
	SessionID                  string
	GenerationID               string
	NetworkProfileID           string
	AgentRuntimeProfileID      string
	RunscPlatform              string
	ControlDirPath             string
	ControlManifestPath        string
	BundleDirPath              string
	SpecPath                   string
	CheckpointPath             string
	SecretsDirPath             string
	BridgeDirPath              string
	LogDirPath                 string
	ControlManifestDigest      string
	RunscVersion               string
	RunscNetwork               string
	RunscOverlay2              string
	HostProxyBindURL           string
	ProxyPort                  int
	HostGatewayIP              string
	SandboxBaseURL             string
	ProbeURL                   string
	NetnsName                  string
	NetnsPath                  string
	HostVeth                   string
	SandboxVeth                string
	SandboxIPCIDR              string
	HostSideCIDR               string
	EgressPolicyID             string
	EgressPolicyDigest         string
	AllowedEgressRules         string
	DorisFEHosts               string
	DorisBEHosts               string
	DorisPorts                 string
	DNSPolicy                  string
	NetworkAllocationState     string
	Agent                      string
	Model                      string
	OutputFormat               string
	DisableNonessentialTraffic bool
	RequiresSecretDrop         bool
	ManifestAnthropicBaseURL   string
	AnthropicAPIKeySecretID    string
	AnthropicAuthTokenSecretID string
	SecretVersion              string
}

type BridgePollGeneration struct {
	SessionID     string
	GenerationID  string
	BridgeDirPath string
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
}

type RenewLiveGenerationsParams struct {
	Owner    string
	LeaseTTL time.Duration
	Now      time.Time
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
	agentRuntimeProfileID := agentRuntimeProfileID(p.SessionID, p.Config)
	resources := buildResourcePaths(p.Config.RunDir, generationID)
	if p.Config.agent() == "sh" {
		resources.SecretsDirPath = ""
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
	now := formatTime(p.Now)
	leaseExpires := p.Now.Add(p.LeaseTTL)

	if _, err := tx.ExecContext(ctx, `
INSERT INTO agent_runtime_profiles (
  agent_runtime_profile_id, agent, model, output_format,
  disable_nonessential_traffic, requires_secret_drop,
  manifest_anthropic_base_url, anthropic_api_key_secret_id,
  anthropic_auth_token_secret_id, secret_version, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(agent, model, output_format, disable_nonessential_traffic,
  requires_secret_drop, manifest_anthropic_base_url,
  anthropic_api_key_secret_id, anthropic_auth_token_secret_id, secret_version
) DO NOTHING`,
		agentRuntimeProfileID, p.Config.agent(), nullableString(p.Config.AgentModel), p.Config.outputFormat(),
		boolInt(p.Config.DisableNonessentialTraffic), boolInt(p.Config.requiresSecretDrop()),
		nullableString(p.Config.manifestAnthropicBaseURL(network.SandboxBaseURL)),
		nullableString(p.Config.apiKeySecretID()), nullableString(p.Config.authTokenSecretID()),
		nullableString(p.Config.secretVersion()), now); err != nil {
		return GenerationAllocation{}, err
	}
	if err := tx.QueryRowContext(ctx, `
SELECT agent_runtime_profile_id
FROM agent_runtime_profiles
WHERE agent = ?
  AND COALESCE(model, '') = COALESCE(?, '')
  AND output_format = ?
  AND disable_nonessential_traffic = ?
  AND requires_secret_drop = ?
  AND COALESCE(manifest_anthropic_base_url, '') = COALESCE(?, '')
  AND COALESCE(anthropic_api_key_secret_id, '') = COALESCE(?, '')
  AND COALESCE(anthropic_auth_token_secret_id, '') = COALESCE(?, '')
  AND COALESCE(secret_version, '') = COALESCE(?, '')`,
		p.Config.agent(), nullableString(p.Config.AgentModel), p.Config.outputFormat(),
		boolInt(p.Config.DisableNonessentialTraffic), boolInt(p.Config.requiresSecretDrop()),
		nullableString(p.Config.manifestAnthropicBaseURL(network.SandboxBaseURL)),
		nullableString(p.Config.apiKeySecretID()), nullableString(p.Config.authTokenSecretID()),
		nullableString(p.Config.secretVersion())).Scan(&agentRuntimeProfileID); err != nil {
		return GenerationAllocation{}, err
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
  agent_runtime_profile_id, runsc_platform, lease_owner,
  lease_expires_at, last_seen_at
) VALUES (?, ?, 'allocating', ?, ?, 'systrap', ?, ?, ?)`,
		generationID, p.SessionID, networkProfileID, agentRuntimeProfileID, p.Owner, formatTime(leaseExpires), now); err != nil {
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
  checkpoint_path, secrets_dir_path, bridge_dir_path, log_dir_path,
  resource_state, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'allocating', ?)`,
		generationID, networkProfileID, agentRuntimeProfileID,
		resources.ControlDirPath, resources.ControlManifestPath, resources.BundleDirPath, resources.SpecPath,
		nullableString(resources.CheckpointPath), nullableString(resources.SecretsDirPath), resources.BridgeDirPath, resources.LogDirPath, now); err != nil {
		return GenerationAllocation{}, err
	}
	if err := updateSessionActiveGenerationTx(ctx, tx, SessionActiveGenerationCASParams{
		SessionID:        p.SessionID,
		NextGenerationID: generationID,
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
	}, nil
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
  AND allocation_state IN ('allocating','ready')`, generationID, sessionID)
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
  AND resource_state IN ('allocating','ready')`, generationID)
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
SET allocation_state = 'destroyed',
    destroyed_at = COALESCE(destroyed_at, ?)
WHERE allocation_state = 'reclaimable'
  AND netns_name LIKE 'harness-gen-%'
  AND EXISTS (
    SELECT 1 FROM runtime_generation_resources r
    WHERE r.generation_id = network_profiles.generation_id
      AND r.resource_state IN ('reclaimable', 'destroyed')
  )
  AND EXISTS (
    SELECT 1 FROM runtime_generations g
    WHERE g.generation_id = network_profiles.generation_id
      AND (
        g.status != 'failed'
        OR (g.ended_at IS NOT NULL AND g.ended_at <= ?)
      )
  )`, formatTime(p.Now), formatTime(cutoff))
	if err != nil {
		return ReaperResult{}, err
	}
	destroyed, err := res.RowsAffected()
	if err != nil {
		return ReaperResult{}, err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE runtime_generation_resources
SET resource_state = 'destroyed',
    destroyed_at = COALESCE(destroyed_at, ?)
WHERE resource_state = 'reclaimable'
  AND generation_id IN (
    SELECT generation_id FROM network_profiles
    WHERE allocation_state = 'destroyed'
  )`, formatTime(p.Now)); err != nil {
		return ReaperResult{}, err
	}

	res, err = tx.ExecContext(ctx, `
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
	return ReaperResult{FailedMarkedReclaimable: failedMarked, DestroyedAllocations: destroyed}, nil
}

func (s *Store) RecoverAllocations(ctx context.Context, p StartupRecoveryParams) (StartupRecoveryResult, error) {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	if p.AckStartedGrace <= 0 {
		p.AckStartedGrace = p.ReconnectGrace
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
	lifecycleIDs, err := queryStringColumnTx(ctx, tx, `
SELECT generation_id
FROM runtime_generations
WHERE status IN ('allocating','starting','probing','restoring','checkpointing')
  AND lease_expires_at IS NOT NULL
  AND lease_expires_at <= ?`, now)
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
	}

	ackStartedCutoff := p.Now.Add(-p.AckStartedGrace)
	unknownIDs, err := queryStringColumnTx(ctx, tx, `
SELECT generation_id
FROM runtime_generations
WHERE status IN ('active','idle')
  AND lease_expires_at IS NOT NULL
  AND lease_expires_at <= ?
  AND EXISTS (
    SELECT 1 FROM turns
    WHERE turns.generation_id = runtime_generations.generation_id
      AND turns.status = 'running'
      AND turns.ack_started_at IS NOT NULL
      AND turns.lease_expires_at IS NOT NULL
      AND turns.lease_expires_at <= ?
  )`, formatTime(ackStartedCutoff), formatTime(ackStartedCutoff))
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

	cutoff := p.Now.Add(-p.ReconnectGrace)
	reconnectIDs, err := queryStringColumnTx(ctx, tx, `
SELECT generation_id
FROM runtime_generations
WHERE status IN ('active','idle')
  AND lease_expires_at IS NOT NULL
  AND lease_expires_at <= ?
  AND NOT EXISTS (
    SELECT 1 FROM turns
    WHERE turns.generation_id = runtime_generations.generation_id
      AND turns.status = 'running'
      AND turns.ack_started_at IS NOT NULL
  )`, formatTime(cutoff))
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
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM active_model_request_contexts
WHERE lease_owner NOT LIKE ?`, p.OwnerUUID+":%"); err != nil {
		return StartupRecoveryResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return StartupRecoveryResult{}, err
	}
	result.ExpiredLifecycleFailed = int64(len(lifecycleIDs))
	result.ReconnectGraceFailed = int64(len(reconnectIDs))
	return result, nil
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

func (s *Store) ListBridgePollGenerations(ctx context.Context, owner string, now time.Time) ([]BridgePollGeneration, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if strings.TrimSpace(owner) == "" {
		return nil, fmt.Errorf("owner is required")
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT g.session_id, g.generation_id, r.bridge_dir_path
FROM runtime_generations g
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
JOIN sessions s ON s.id = g.session_id
WHERE g.status IN ('active','idle','probing','restoring','starting')
  AND g.lease_owner = ?
  AND g.lease_expires_at > ?
  AND s.active_generation_id = g.generation_id
  AND s.status NOT IN ('failed', 'destroyed')
  AND r.resource_state IN ('ready','live','recreating')
ORDER BY g.session_id, g.generation_id`, owner, formatTime(now))
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
	return generations, rows.Err()
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
  r.control_dir_path,
  r.control_manifest_path,
  r.bundle_dir_path,
  r.spec_path,
  COALESCE(r.checkpoint_path, ''),
  COALESCE(r.secrets_dir_path, ''),
  r.bridge_dir_path,
  r.log_dir_path,
  COALESCE(r.control_manifest_digest, ''),
  COALESCE(r.runsc_version, ''),
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
  a.agent,
  COALESCE(a.model, ''),
  a.output_format,
  a.disable_nonessential_traffic,
  a.requires_secret_drop,
  COALESCE(a.manifest_anthropic_base_url, ''),
  COALESCE(a.anthropic_api_key_secret_id, ''),
  COALESCE(a.anthropic_auth_token_secret_id, ''),
  COALESCE(a.secret_version, '')
FROM runtime_generations g
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
JOIN network_profiles n ON n.network_profile_id = g.network_profile_id
JOIN egress_policies e ON e.egress_policy_id = n.egress_policy_id
JOIN agent_runtime_profiles a ON a.agent_runtime_profile_id = g.agent_runtime_profile_id
WHERE g.session_id = ?
  AND g.generation_id = ?`, sessionID, generationID)
	var details RuntimeGenerationDetails
	var disableNonessentialTraffic, requiresSecretDrop int
	if err := row.Scan(
		&details.SessionID,
		&details.GenerationID,
		&details.NetworkProfileID,
		&details.AgentRuntimeProfileID,
		&details.RunscPlatform,
		&details.ControlDirPath,
		&details.ControlManifestPath,
		&details.BundleDirPath,
		&details.SpecPath,
		&details.CheckpointPath,
		&details.SecretsDirPath,
		&details.BridgeDirPath,
		&details.LogDirPath,
		&details.ControlManifestDigest,
		&details.RunscVersion,
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
		&details.Agent,
		&details.Model,
		&details.OutputFormat,
		&disableNonessentialTraffic,
		&requiresSecretDrop,
		&details.ManifestAnthropicBaseURL,
		&details.AnthropicAPIKeySecretID,
		&details.AnthropicAuthTokenSecretID,
		&details.SecretVersion,
	); err != nil {
		return RuntimeGenerationDetails{}, err
	}
	details.DisableNonessentialTraffic = disableNonessentialTraffic != 0
	details.RequiresSecretDrop = requiresSecretDrop != 0
	return details, nil
}

func (s *Store) RecordGenerationRuntimeArtifacts(ctx context.Context, generationID, controlManifestDigest, runscVersion string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `
UPDATE runtime_generation_resources
SET control_manifest_digest = ?,
    runsc_version = COALESCE(?, runsc_version)
WHERE generation_id = ?`, controlManifestDigest, nullableString(runscVersion), generationID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE runtime_generations
SET runsc_version = COALESCE(?, runsc_version)
WHERE generation_id = ?`, nullableString(runscVersion), generationID); err != nil {
		return err
	}
	return tx.Commit()
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
		if err != nil || prefix.Bits() != 30 {
			continue
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
		LogDirPath:          filepath.Join(runDir, "logs", "gen-"+generationID),
	}
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
	if cfg.EgressDNSPolicy != "" && cfg.EgressDNSPolicy != "off" {
		rules = append(rules, "udp:53", "tcp:53")
	}
	return rules
}

func egressPolicyID(cfg ResourceAllocatorConfig) string {
	payload := strings.Join(cfg.EgressDorisFEHosts, ",") + "|" +
		strings.Join(cfg.EgressDorisBEHosts, ",") + "|" +
		fmt.Sprint(cfg.EgressDorisPorts) + "|" + cfg.EgressDNSPolicy
	return "egress_" + strings.ReplaceAll(payload, " ", "_")
}

func agentRuntimeProfileID(sessionID string, cfg ResourceAllocatorConfig) string {
	return "arp_" + sessionID
}

func (c ResourceAllocatorConfig) agent() string {
	if strings.TrimSpace(c.Agent) != "" {
		return strings.TrimSpace(c.Agent)
	}
	if strings.TrimSpace(c.AgentOutputFormat) == "shell_pty" {
		return "sh"
	}
	return "claude"
}

func (c ResourceAllocatorConfig) outputFormat() string {
	if strings.TrimSpace(c.AgentOutputFormat) == "" {
		return "stream-json"
	}
	return strings.TrimSpace(c.AgentOutputFormat)
}

func (c ResourceAllocatorConfig) requiresSecretDrop() bool {
	return c.agent() == "claude"
}

func (c ResourceAllocatorConfig) manifestAnthropicBaseURL(baseURL string) string {
	if c.agent() == "sh" {
		return ""
	}
	return strings.TrimSpace(baseURL)
}

func (c ResourceAllocatorConfig) apiKeySecretID() string {
	if c.agent() == "sh" {
		return ""
	}
	if strings.TrimSpace(c.AnthropicAPIKeySecretID) == "" {
		return "anthropic_api_key"
	}
	return strings.TrimSpace(c.AnthropicAPIKeySecretID)
}

func (c ResourceAllocatorConfig) authTokenSecretID() string {
	if c.agent() == "sh" {
		return ""
	}
	if strings.TrimSpace(c.AnthropicAuthTokenSecretID) == "" {
		return "anthropic_auth_token"
	}
	return strings.TrimSpace(c.AnthropicAuthTokenSecretID)
}

func (c ResourceAllocatorConfig) secretVersion() string {
	if c.agent() == "sh" {
		return ""
	}
	if strings.TrimSpace(c.SecretVersion) == "" {
		return "local"
	}
	return strings.TrimSpace(c.SecretVersion)
}

func (c ResourceAllocatorConfig) hostProxyBindURL() string {
	if strings.TrimSpace(c.HostProxyBindURL) == "" {
		return "http://0.0.0.0:8082"
	}
	return strings.TrimSpace(c.HostProxyBindURL)
}

func (c ResourceAllocatorConfig) proxyPort() int {
	if c.ProxyPort <= 0 {
		return 8082
	}
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
