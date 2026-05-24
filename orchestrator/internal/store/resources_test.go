package store

import (
	"context"
	"errors"
	"net/netip"
	"path/filepath"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/sessionstate"
)

func TestAllocateGenerationCreatesRowsAndReservesNonDestroyedSlots(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_alloc")
	cfg := testAllocatorConfig(t)
	now := time.Now().UTC()

	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_alloc",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}

	var activeGeneration string
	if err := st.db.QueryRowContext(ctx, `SELECT active_generation_id FROM sessions WHERE id = 'sess_alloc'`).Scan(&activeGeneration); err != nil {
		t.Fatalf("query active generation: %v", err)
	}
	if activeGeneration != allocation.GenerationID {
		t.Fatalf("active generation = %q, want %q", activeGeneration, allocation.GenerationID)
	}
	var generationStatus, networkState, resourceState, hostCIDR string
	if err := st.db.QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state, n.host_side_cidr
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&generationStatus, &networkState, &resourceState, &hostCIDR); err != nil {
		t.Fatalf("query allocation rows: %v", err)
	}
	if generationStatus != "allocating" || networkState != "allocating" || resourceState != "allocating" || hostCIDR != "10.240.0.0/30" {
		t.Fatalf("unexpected allocation row state: generation=%s network=%s resource=%s cidr=%s", generationStatus, networkState, resourceState, hostCIDR)
	}
	details, err := st.GetRuntimeGenerationDetails(ctx, "sess_alloc", allocation.GenerationID)
	if err != nil {
		t.Fatalf("get runtime generation details: %v", err)
	}
	if details.AnthropicAPIKeySecretID != "anthropic_api_key" ||
		details.AnthropicAuthTokenSecretID != "anthropic_auth_token" ||
		details.SecretVersion != "local" ||
		!details.RequiresSecretDrop ||
		details.SecretsDirPath == "" {
		t.Fatalf("unexpected claude generation details: %+v", details)
	}
	if details.RunscNetwork != "sandbox" ||
		details.RunscOverlay2 != "none" ||
		details.HostProxyBindURL != cfg.HostProxyBindURL ||
		details.ProxyPort != 8082 ||
		details.HostGatewayIP != "10.240.0.1" ||
		details.SandboxBaseURL != "http://10.240.0.1:8082" ||
		details.ProbeURL != "http://10.240.0.1:8082" ||
		details.NetnsName == "" ||
		details.NetnsPath == "" ||
		details.HostVeth == "" ||
		details.SandboxVeth == "" ||
		details.SandboxIPCIDR != "10.240.0.2/30" ||
		details.HostSideCIDR != "10.240.0.0/30" ||
		details.EgressPolicyID == "" ||
		details.EgressPolicyDigest == "" ||
		details.AllowedEgressRules == "" ||
		details.DorisFEHosts == "" ||
		details.DorisBEHosts == "" ||
		details.DorisPorts == "" ||
		details.DNSPolicy != "hostnames_only" ||
		details.NetworkAllocationState != "allocating" {
		t.Fatalf("generation details missing network allocation fields: %+v", details)
	}

	if err := st.MarkGenerationResourcesLive(ctx, "sess_alloc", allocation.GenerationID, allocation.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark resources live: %v", err)
	}
	if err := st.RecordGenerationRuntimeArtifacts(ctx, allocation.GenerationID, "digest_a", "runsc test"); err != nil {
		t.Fatalf("record runtime artifacts: %v", err)
	}
	details, err = st.GetRuntimeGenerationDetails(ctx, "sess_alloc", allocation.GenerationID)
	if err != nil {
		t.Fatalf("get runtime generation details after artifact record: %v", err)
	}
	if details.ControlManifestDigest != "digest_a" || details.RunscVersion != "runsc test" {
		t.Fatalf("runtime artifact details not persisted: %+v", details)
	}
	if err := st.FailGeneration(ctx, FailGenerationParams{
		SessionID:    "sess_alloc",
		GenerationID: allocation.GenerationID,
		Owner:        allocation.Owner,
		ErrorClass:   "test_failure",
		Reason:       "test failure",
		Now:          now.Add(2 * time.Second),
	}); err != nil {
		t.Fatalf("fail generation: %v", err)
	}

	createStoreSession(t, ctx, st, "sess_next")
	next, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_next",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now.Add(3 * time.Second),
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate second generation: %v", err)
	}
	var nextCIDR string
	if err := st.db.QueryRowContext(ctx, `SELECT host_side_cidr FROM network_profiles WHERE generation_id = ?`, next.GenerationID).Scan(&nextCIDR); err != nil {
		t.Fatalf("query next cidr: %v", err)
	}
	if nextCIDR != "10.240.0.4/30" {
		t.Fatalf("expected reclaimable first slot to remain reserved, got next cidr %s", nextCIDR)
	}
}

func TestAllocateShellGenerationHasNoSecretReferences(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_shell")
	cfg := testAllocatorConfig(t)
	cfg.Agent = "sh"
	cfg.AgentModel = ""
	cfg.AgentOutputFormat = "shell_pty"

	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_shell",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate shell generation: %v", err)
	}
	details, err := st.GetRuntimeGenerationDetails(ctx, "sess_shell", allocation.GenerationID)
	if err != nil {
		t.Fatalf("get shell generation details: %v", err)
	}
	if details.Agent != "sh" ||
		details.RequiresSecretDrop ||
		details.SecretsDirPath != "" ||
		details.AnthropicAPIKeySecretID != "" ||
		details.AnthropicAuthTokenSecretID != "" ||
		details.SecretVersion != "" {
		t.Fatalf("shell generation should not carry secrets: %+v", details)
	}
}

func TestAllocatorReturnsPoolExhaustedBeforeRows(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	cfg.CIDRPool = netip.MustParsePrefix("10.250.0.0/30")

	createStoreSession(t, ctx, st, "sess_one")
	if _, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_one",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    cfg,
	}); err != nil {
		t.Fatalf("allocate first generation: %v", err)
	}
	createStoreSession(t, ctx, st, "sess_two")
	_, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_two",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    cfg,
	})
	if !errors.Is(err, ErrPoolExhausted) {
		t.Fatalf("expected pool exhausted, got %v", err)
	}
	var generations int
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_generations`).Scan(&generations); err != nil {
		t.Fatalf("count generations: %v", err)
	}
	if generations != 1 {
		t.Fatalf("pool exhaustion should not create a generation row, got %d", generations)
	}
}

func TestRecoverAllocationsAndReaperTransitions(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	createStoreSession(t, ctx, st, "sess_recover")
	now := time.Now().UTC()
	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_recover",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Second,
		Now:       now.Add(-time.Minute),
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generations SET status = 'restoring', lease_expires_at = ? WHERE generation_id = ?`, formatTime(now.Add(-time.Second)), allocation.GenerationID); err != nil {
		t.Fatalf("set restoring: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `
UPDATE network_profiles SET allocation_state = 'recreating' WHERE generation_id = ?`, allocation.GenerationID); err != nil {
		t.Fatalf("set recreating network: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generation_resources SET resource_state = 'recreating' WHERE generation_id = ?`, allocation.GenerationID); err != nil {
		t.Fatalf("set recreating resource: %v", err)
	}

	recovered, err := st.RecoverAllocations(ctx, StartupRecoveryParams{
		OwnerUUID:      owner.UUID,
		Now:            now,
		LeaseTTL:       time.Minute,
		ReconnectGrace: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("recover allocations: %v", err)
	}
	if recovered.ExpiredLifecycleFailed != 1 {
		t.Fatalf("expected one lifecycle failure, got %+v", recovered)
	}
	var generationStatus, networkState, resourceState string
	if err := st.db.QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&generationStatus, &networkState, &resourceState); err != nil {
		t.Fatalf("query recovered state: %v", err)
	}
	if generationStatus != "failed" || networkState != "reclaimable" || resourceState != "reclaimable" {
		t.Fatalf("unexpected recovered state: generation=%s network=%s resource=%s", generationStatus, networkState, resourceState)
	}

	reaped, err := st.ReapResources(ctx, ReaperParams{OwnerUUID: owner.UUID, FailedRetention: 0, Now: now.Add(time.Second)})
	if err != nil {
		t.Fatalf("reap resources: %v", err)
	}
	if reaped.DestroyedAllocations != 1 {
		t.Fatalf("expected one destroyed allocation, got %+v", reaped)
	}
	reaped, err = st.ReapResources(ctx, ReaperParams{OwnerUUID: owner.UUID, FailedRetention: 0, Now: now.Add(2 * time.Second)})
	if err != nil {
		t.Fatalf("second reap resources: %v", err)
	}
	if reaped.DestroyedAllocations != 0 {
		t.Fatalf("second reap should be idempotent, got %+v", reaped)
	}
}

func TestRecoverAllocationsDoesNotReclaimUnrelatedFailedGeneration(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	now := time.Now().UTC()

	createStoreSession(t, ctx, st, "sess_crashed")
	crashed, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_crashed",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Second,
		Now:       now.Add(-time.Minute),
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate crashed generation: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generations SET status = 'starting', lease_expires_at = ? WHERE generation_id = ?`, formatTime(now.Add(-time.Second)), crashed.GenerationID); err != nil {
		t.Fatalf("set crashed generation starting: %v", err)
	}

	createStoreSession(t, ctx, st, "sess_recent_failed")
	recentFailed, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_recent_failed",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now.Add(-30 * time.Second),
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate recent failed generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, "sess_recent_failed", recentFailed.GenerationID, recentFailed.Owner, now.Add(-20*time.Second)); err != nil {
		t.Fatalf("mark recent failed resources live: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'failed', ended_at = ?, lease_owner = NULL
WHERE generation_id = ?`, formatTime(now.Add(-5*time.Second)), recentFailed.GenerationID); err != nil {
		t.Fatalf("set recent failed generation: %v", err)
	}

	recovered, err := st.RecoverAllocations(ctx, StartupRecoveryParams{
		OwnerUUID:      owner.UUID,
		Now:            now,
		LeaseTTL:       time.Minute,
		ReconnectGrace: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("recover allocations: %v", err)
	}
	if recovered.ExpiredLifecycleFailed != 1 {
		t.Fatalf("expected one lifecycle failure, got %+v", recovered)
	}
	var crashedState, recentState string
	if err := st.db.QueryRowContext(ctx, `SELECT allocation_state FROM network_profiles WHERE generation_id = ?`, crashed.GenerationID).Scan(&crashedState); err != nil {
		t.Fatalf("query crashed allocation: %v", err)
	}
	if err := st.db.QueryRowContext(ctx, `SELECT allocation_state FROM network_profiles WHERE generation_id = ?`, recentFailed.GenerationID).Scan(&recentState); err != nil {
		t.Fatalf("query recent allocation: %v", err)
	}
	if crashedState != "reclaimable" || recentState != "live" {
		t.Fatalf("unexpected allocation states: crashed=%s recent_failed=%s", crashedState, recentState)
	}
}

func TestRenewLiveGenerationLeasesKeepsIdle7AGenerationAlive(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	createStoreSession(t, ctx, st, "sess_idle")
	now := time.Now().UTC()
	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_idle",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, "sess_idle", allocation.GenerationID, allocation.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark resources live: %v", err)
	}
	renewed, err := st.RenewLiveGenerationLeases(ctx, RenewLiveGenerationsParams{
		Owner:    allocation.Owner,
		LeaseTTL: time.Minute,
		Now:      now.Add(30 * time.Second),
	})
	if err != nil {
		t.Fatalf("renew live generation leases: %v", err)
	}
	if renewed != 1 {
		t.Fatalf("expected one renewed generation, got %d", renewed)
	}
	var leaseExpires string
	if err := st.db.QueryRowContext(ctx, `SELECT lease_expires_at FROM runtime_generations WHERE generation_id = ?`, allocation.GenerationID).Scan(&leaseExpires); err != nil {
		t.Fatalf("query lease expiry: %v", err)
	}
	if got := parseTime(leaseExpires); !got.After(now.Add(time.Minute)) {
		t.Fatalf("lease expiry was not extended enough: %s", got)
	}
}

func TestSweepExpiredSessionsDestroysAndRejectsInputState(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	now := time.Now().UTC()
	expiredAt := now.Add(-time.Second)
	if err := st.CreateSession(ctx, Session{
		ID:        "sess_expired",
		UserID:    "lab",
		Status:    string(sessionstate.Created),
		Agent:     "claude",
		Workspace: filepath.Join(t.TempDir(), "sess_expired"),
		RestoreID: "phase3-sess_expired",
		CreatedAt: now.Add(-time.Hour),
		UpdatedAt: now.Add(-time.Hour),
		ExpiresAt: &expiredAt,
	}); err != nil {
		t.Fatalf("create expired session: %v", err)
	}
	changed, err := st.SweepExpiredSessions(ctx, now)
	if err != nil {
		t.Fatalf("sweep expired sessions: %v", err)
	}
	if changed != 1 {
		t.Fatalf("expected one expired session swept, got %d", changed)
	}
	got, err := st.GetSession(ctx, "sess_expired")
	if err != nil {
		t.Fatalf("get expired session: %v", err)
	}
	if got.Status != string(sessionstate.Destroyed) || got.ErrorClass != "session_expired" {
		t.Fatalf("unexpected expired session: %+v", got)
	}

	if err := st.CreateSession(ctx, Session{
		ID:        "sess_expired_allocated",
		UserID:    "lab",
		Status:    string(sessionstate.RunningIdle),
		Agent:     "claude",
		Workspace: filepath.Join(t.TempDir(), "sess_expired_allocated"),
		RestoreID: "phase3-sess_expired_allocated",
		CreatedAt: now.Add(-time.Hour),
		UpdatedAt: now.Add(-time.Hour),
		ExpiresAt: &expiredAt,
	}); err != nil {
		t.Fatalf("create expired allocated session: %v", err)
	}
	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_expired_allocated",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now.Add(-time.Minute),
		Config:    testAllocatorConfig(t),
	})
	if err != nil {
		t.Fatalf("allocate expired generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, "sess_expired_allocated", allocation.GenerationID, allocation.Owner, now.Add(-30*time.Second)); err != nil {
		t.Fatalf("mark expired resources live: %v", err)
	}
	changed, err = st.SweepExpiredSessions(ctx, now)
	if err != nil {
		t.Fatalf("sweep expired allocated session: %v", err)
	}
	if changed != 1 {
		t.Fatalf("expected one expired allocated session swept, got %d", changed)
	}
	var generationStatus, networkState, resourceState string
	if err := st.db.QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&generationStatus, &networkState, &resourceState); err != nil {
		t.Fatalf("query expired allocation state: %v", err)
	}
	if generationStatus != "failed" || networkState != "reclaimable" || resourceState != "reclaimable" {
		t.Fatalf("unexpected expired allocation state: generation=%s network=%s resource=%s", generationStatus, networkState, resourceState)
	}
}

func TestUpdateSessionStatusDoesNotResurrectDestroyedSession(t *testing.T) {
	ctx := context.Background()
	st, _ := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_terminal")
	if err := st.UpdateSessionStatus(ctx, "sess_terminal", string(sessionstate.Destroyed), nil); err != nil {
		t.Fatalf("destroy session: %v", err)
	}
	if err := st.UpdateSessionStatusAndActivity(ctx, "sess_terminal", string(sessionstate.RunningIdle), nil, time.Now().UTC()); err != nil {
		t.Fatalf("attempt resurrect destroyed session: %v", err)
	}
	got, err := st.GetSession(ctx, "sess_terminal")
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if got.Status != string(sessionstate.Destroyed) {
		t.Fatalf("destroyed session was resurrected as %s", got.Status)
	}
}

func openOwnedStore(t *testing.T, ctx context.Context) (*Store, *OwnerLock) {
	t.Helper()
	dir := t.TempDir()
	st, err := Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	owner, err := AcquireOwnerLock(filepath.Join(dir, "run"))
	if err != nil {
		t.Fatalf("acquire owner: %v", err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	if err := st.WriteOwner(ctx, owner); err != nil {
		t.Fatalf("write owner: %v", err)
	}
	return st, owner
}

func testAllocatorConfig(t *testing.T) ResourceAllocatorConfig {
	t.Helper()
	return ResourceAllocatorConfig{
		RunDir:                     filepath.Join(t.TempDir(), "run"),
		CIDRPool:                   netip.MustParsePrefix("10.240.0.0/29"),
		EgressDorisFEHosts:         []string{"172.16.0.138"},
		EgressDorisBEHosts:         []string{"172.16.0.139"},
		EgressDorisPorts:           []int{9030, 8040},
		EgressDNSPolicy:            "hostnames_only",
		HostProxyBindURL:           "http://0.0.0.0:8082",
		ProxyPort:                  8082,
		Agent:                      "claude",
		AgentModel:                 "sonnet",
		AgentOutputFormat:          "stream-json",
		DisableNonessentialTraffic: true,
	}
}
