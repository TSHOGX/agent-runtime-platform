package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/netip"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/sessionstate"
)

func TestAllocateGenerationRequiresExplicitAllocatorConfigFields(t *testing.T) {
	tests := []struct {
		name string
		edit func(*ResourceAllocatorConfig)
		want string
	}{
		{
			name: "missing driver",
			edit: func(cfg *ResourceAllocatorConfig) { cfg.DriverID = "" },
			want: "driver id is required",
		},
		{
			name: "missing output format",
			edit: func(cfg *ResourceAllocatorConfig) { cfg.OutputFormat = "" },
			want: "output format is required",
		},
		{
			name: "missing sandbox uid",
			edit: func(cfg *ResourceAllocatorConfig) { cfg.SandboxUID = 0 },
			want: "sandbox uid must be > 0",
		},
		{
			name: "missing sandbox gid",
			edit: func(cfg *ResourceAllocatorConfig) { cfg.SandboxGID = 0 },
			want: "sandbox gid must be > 0",
		},
		{
			name: "invalid supplemental gid",
			edit: func(cfg *ResourceAllocatorConfig) { cfg.SandboxSupplementalGIDs = []int{44, 0} },
			want: "sandbox supplemental gids must contain only positive gids",
		},
		{
			name: "duplicate supplemental gid",
			edit: func(cfg *ResourceAllocatorConfig) { cfg.SandboxSupplementalGIDs = []int{44, 44} },
			want: "sandbox supplemental gids contains duplicate gid 44",
		},
		{
			name: "missing host proxy bind url",
			edit: func(cfg *ResourceAllocatorConfig) { cfg.HostProxyBindURL = "" },
			want: "host proxy bind url is required",
		},
		{
			name: "missing proxy port",
			edit: func(cfg *ResourceAllocatorConfig) { cfg.ProxyPort = 0 },
			want: "proxy port must be > 0",
		},
		{
			name: "missing model access allowed",
			edit: func(cfg *ResourceAllocatorConfig) { cfg.ModelAccessAllowed = nil },
			want: "model access allowed must be explicitly set",
		},
		{
			name: "missing model for model access",
			edit: func(cfg *ResourceAllocatorConfig) { cfg.Model = "" },
			want: "model is required when model access is enabled",
		},
		{
			name: "missing sandbox model proxy url for host-only model access",
			edit: func(cfg *ResourceAllocatorConfig) { cfg.SandboxModelProxyBaseURL = "" },
			want: "sandbox model proxy base url is required when host-only model access is enabled",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			st, owner := openOwnedStore(t, ctx)
			createStoreSession(t, ctx, st, "sess_required_"+strings.ReplaceAll(tt.name, " ", "_"))
			cfg := testAllocatorConfig(t)
			tt.edit(&cfg)

			_, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
				SessionID: "sess_required_" + strings.ReplaceAll(tt.name, " ", "_"),
				Owner:     GenerationLeaseOwner(owner.UUID),
				LeaseTTL:  time.Minute,
				Now:       time.Now().UTC(),
				Config:    cfg,
			})
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("AllocateGeneration error=%v want %q", err, tt.want)
			}
		})
	}
}

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
	if details.RequiresSecretDrop ||
		details.SecretsDirPath != "" ||
		details.AnthropicAPIKeySecretID != "" ||
		details.AnthropicAuthTokenSecretID != "" ||
		details.SecretVersion != "" {
		t.Fatalf("claude generation should not carry sandbox secrets: %+v", details)
	}
	if details.RunscContainerID != "harness-gen-"+allocation.GenerationID {
		t.Fatalf("runsc container id = %q, want generation-scoped id", details.RunscContainerID)
	}
	if details.SandboxContractVersion != SandboxContractVersion {
		t.Fatalf("sandbox contract version = %q, want %q", details.SandboxContractVersion, SandboxContractVersion)
	}
	if details.SandboxUID != 7000 ||
		details.SandboxGID != 7001 ||
		!slices.Equal(details.SandboxSupplementalGIDs, []int{43, 44}) {
		t.Fatalf("unexpected sandbox runtime identity: %+v", details)
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
	if details.ManifestAnthropicBaseURL != "http://harness-model-proxy.internal:8082" {
		t.Fatalf("manifest model proxy base URL = %q, want default alias", details.ManifestAnthropicBaseURL)
	}
	if details.NetworkHostsPath == "" {
		t.Fatalf("claude generation should allocate network hosts alias projection: %+v", details)
	}
	var claudeStatePayload string
	if err := st.db.QueryRowContext(ctx, `
SELECT state_payload
FROM session_driver_states
WHERE session_id = ?
  AND driver_id = 'claude_code'`, "sess_alloc").Scan(&claudeStatePayload); err != nil {
		t.Fatalf("query claude driver state: %v", err)
	}
	if !strings.Contains(claudeStatePayload, `"claude_session_uuid":"bootstrap-sess_alloc"`) {
		t.Fatalf("claude driver state did not initialize deterministic uuid: %s", claudeStatePayload)
	}

	if err := st.MarkGenerationResourcesLive(ctx, "sess_alloc", allocation.GenerationID, allocation.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark resources live: %v", err)
	}
	if err := st.RecordGenerationRuntimeArtifactDigests(ctx, allocation.GenerationID, GenerationRuntimeArtifactDigests{
		ControlManifestDigest:          "digest_a",
		ProjectedControlManifestDigest: "projected_digest_a",
		BundleDigest:                   "bundle_digest_a",
		RuntimeConfigDigest:            "runtime_config_digest_a",
		SpecDigest:                     "spec_digest_a",
		RunscVersion:                   "runsc test",
		RunscBinaryPath:                "/usr/local/bin/runsc-test",
		RunscBinaryDigest:              "sha256:runsc-test",
	}); err != nil {
		t.Fatalf("record runtime artifacts: %v", err)
	}
	details, err = st.GetRuntimeGenerationDetails(ctx, "sess_alloc", allocation.GenerationID)
	if err != nil {
		t.Fatalf("get runtime generation details after artifact record: %v", err)
	}
	if details.ControlManifestDigest != "digest_a" ||
		details.ProjectedControlManifestDigest != "projected_digest_a" ||
		details.BundleDigest != "bundle_digest_a" ||
		details.RuntimeConfigDigest != "runtime_config_digest_a" ||
		details.SpecDigest != "spec_digest_a" ||
		details.RunscVersion != "runsc test" ||
		details.RunscBinaryPath != "/usr/local/bin/runsc-test" ||
		details.RunscBinaryDigest != "sha256:runsc-test" {
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
	var firstNetns, nextCIDR, nextNetns string
	if err := st.db.QueryRowContext(ctx, `SELECT netns_name FROM network_profiles WHERE generation_id = ?`, allocation.GenerationID).Scan(&firstNetns); err != nil {
		t.Fatalf("query first netns: %v", err)
	}
	if err := st.db.QueryRowContext(ctx, `SELECT host_side_cidr, netns_name FROM network_profiles WHERE generation_id = ?`, next.GenerationID).Scan(&nextCIDR, &nextNetns); err != nil {
		t.Fatalf("query next network identity: %v", err)
	}
	if nextCIDR != "10.240.0.4/30" {
		t.Fatalf("expected reclaimable first slot to remain reserved, got next cidr %s", nextCIDR)
	}
	if nextNetns == firstNetns {
		t.Fatalf("expected reclaimable first netns to remain reserved, got %s", nextNetns)
	}
}

func TestAllocateGenerationFailsOnMalformedOccupiedNetworkCIDR(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_bad_cidr")
	cfg := testAllocatorConfig(t)
	now := time.Now().UTC()

	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_bad_cidr",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `
UPDATE network_profiles
SET host_side_cidr = 'not-a-cidr',
    allocation_state = 'live'
WHERE generation_id = ?`, allocation.GenerationID); err != nil {
		t.Fatalf("corrupt occupied network CIDR: %v", err)
	}

	createStoreSession(t, ctx, st, "sess_bad_cidr_next")
	_, err = st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_bad_cidr_next",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now.Add(time.Second),
		Config:    cfg,
	})
	if err == nil || !strings.Contains(err.Error(), `invalid occupied network CIDR "not-a-cidr"`) {
		t.Fatalf("AllocateGeneration error=%v want malformed occupied CIDR failure", err)
	}
}

func TestAllocateGenerationFailsOnNonThirtyOccupiedCheckpointNetworkCIDR(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_bad_checkpoint_cidr")
	cfg := testAllocatorConfig(t)
	now := time.Now().UTC()
	leaseOwner := GenerationLeaseOwner(owner.UUID)

	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_bad_checkpoint_cidr",
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, "sess_bad_checkpoint_cidr", allocation.GenerationID, allocation.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark generation live: %v", err)
	}
	checkpointedGeneration(t, ctx, st, "sess_bad_checkpoint_cidr", allocation.GenerationID, now.Add(2*time.Second))
	if _, err := st.db.ExecContext(ctx, `
UPDATE network_profiles
SET host_side_cidr = '10.240.0.0/29'
WHERE generation_id = ?`, allocation.GenerationID); err != nil {
		t.Fatalf("corrupt checkpoint network CIDR prefix length: %v", err)
	}

	createStoreSession(t, ctx, st, "sess_bad_checkpoint_cidr_next")
	_, err = st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_bad_checkpoint_cidr_next",
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       now.Add(3 * time.Second),
		Config:    cfg,
	})
	if err == nil || !strings.Contains(err.Error(), `invalid occupied network CIDR "10.240.0.0/29": expected /30, got /29`) {
		t.Fatalf("AllocateGeneration error=%v want non-/30 checkpoint CIDR failure", err)
	}
}

func TestRecordGenerationRuntimeArtifactDigestsRequiresCompleteMetadata(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_artifact_metadata")
	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_artifact_metadata",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    testAllocatorConfig(t),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	valid := GenerationRuntimeArtifactDigests{
		ControlManifestDigest:          "manifest_digest",
		ProjectedControlManifestDigest: "projected_manifest_digest",
		BundleDigest:                   "bundle_digest",
		RuntimeConfigDigest:            "runtime_config_digest",
		SpecDigest:                     "spec_digest",
		RunscVersion:                   "runsc test",
		RunscBinaryPath:                "/usr/local/bin/runsc-test",
		RunscBinaryDigest:              "sha256:runsc-test",
	}
	tests := []struct {
		name string
		want string
		edit func(*GenerationRuntimeArtifactDigests)
	}{
		{name: "control manifest", want: "control manifest digest", edit: func(d *GenerationRuntimeArtifactDigests) { d.ControlManifestDigest = "" }},
		{name: "projected manifest", want: "projected control manifest digest", edit: func(d *GenerationRuntimeArtifactDigests) { d.ProjectedControlManifestDigest = "" }},
		{name: "bundle", want: "bundle digest", edit: func(d *GenerationRuntimeArtifactDigests) { d.BundleDigest = "" }},
		{name: "runtime config", want: "runtime config digest", edit: func(d *GenerationRuntimeArtifactDigests) { d.RuntimeConfigDigest = "" }},
		{name: "spec", want: "spec digest", edit: func(d *GenerationRuntimeArtifactDigests) { d.SpecDigest = "" }},
		{name: "runsc version", want: "runsc version", edit: func(d *GenerationRuntimeArtifactDigests) { d.RunscVersion = "" }},
		{name: "runsc binary path", want: "runsc binary path", edit: func(d *GenerationRuntimeArtifactDigests) { d.RunscBinaryPath = "" }},
		{name: "runsc binary digest", want: "runsc binary digest", edit: func(d *GenerationRuntimeArtifactDigests) { d.RunscBinaryDigest = "" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			digests := valid
			tt.edit(&digests)
			err := st.RecordGenerationRuntimeArtifactDigests(ctx, allocation.GenerationID, digests)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("RecordGenerationRuntimeArtifactDigests error=%v want field %q", err, tt.want)
			}
		})
	}

	if err := st.RecordGenerationRuntimeArtifactDigests(ctx, allocation.GenerationID, valid); err != nil {
		t.Fatalf("record complete artifacts: %v", err)
	}
	partial := valid
	partial.ControlManifestDigest = "new_manifest_digest"
	partial.BundleDigest = ""
	if err := st.RecordGenerationRuntimeArtifactDigests(ctx, allocation.GenerationID, partial); err == nil {
		t.Fatalf("partial artifact update succeeded")
	}
	details, err := st.GetRuntimeGenerationDetails(ctx, "sess_artifact_metadata", allocation.GenerationID)
	if err != nil {
		t.Fatalf("get runtime generation details: %v", err)
	}
	if details.ControlManifestDigest != valid.ControlManifestDigest ||
		details.BundleDigest != valid.BundleDigest ||
		details.RunscVersion != valid.RunscVersion {
		t.Fatalf("partial artifact update changed stored metadata: %+v", details)
	}
}

func TestAllocateGenerationUsesConfiguredModelProxyPort(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_proxy_port")
	cfg := testAllocatorConfig(t)
	cfg.HostProxyBindURL = "http://0.0.0.0:8083"
	cfg.ProxyPort = 8083
	cfg.SandboxModelProxyBaseURL = "http://harness-model-proxy.internal:8083"

	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_proxy_port",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	details, err := st.GetRuntimeGenerationDetails(ctx, "sess_proxy_port", allocation.GenerationID)
	if err != nil {
		t.Fatalf("get runtime generation details: %v", err)
	}
	if details.ProxyPort != 8083 ||
		details.HostProxyBindURL != "http://0.0.0.0:8083" ||
		details.SandboxBaseURL != "http://10.240.0.1:8083" ||
		details.ProbeURL != "http://10.240.0.1:8083" ||
		details.ManifestAnthropicBaseURL != "http://harness-model-proxy.internal:8083" {
		t.Fatalf("generation did not use configured model proxy port: %+v", details)
	}
	assertJSONStrings(t, details.AllowedEgressRules, []string{
		"tcp:10.240.0.1:8083",
		"tcp:172.16.0.138:9030",
		"tcp:172.16.0.138:8040",
		"tcp:172.16.0.139:9030",
		"tcp:172.16.0.139:8040",
	})
	if !strings.Contains(details.EgressPolicyID, "proxy_port=8083") ||
		!strings.Contains(details.EgressPolicyDigest, "proxy_port=8083") {
		t.Fatalf("egress policy identity does not include proxy port: id=%q digest=%q", details.EgressPolicyID, details.EgressPolicyDigest)
	}
}

func TestAllocateGenerationEgressPolicyIdentityIncludesProxyPort(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_proxy_policy_8082")
	createStoreSession(t, ctx, st, "sess_proxy_policy_8083")
	cfg := testAllocatorConfig(t)

	first, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_proxy_policy_8082",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate first generation: %v", err)
	}
	firstDetails, err := st.GetRuntimeGenerationDetails(ctx, "sess_proxy_policy_8082", first.GenerationID)
	if err != nil {
		t.Fatalf("get first generation details: %v", err)
	}

	cfg.HostProxyBindURL = "http://0.0.0.0:8083"
	cfg.ProxyPort = 8083
	cfg.SandboxModelProxyBaseURL = "http://harness-model-proxy.internal:8083"
	second, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_proxy_policy_8083",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC().Add(time.Second),
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate second generation: %v", err)
	}
	secondDetails, err := st.GetRuntimeGenerationDetails(ctx, "sess_proxy_policy_8083", second.GenerationID)
	if err != nil {
		t.Fatalf("get second generation details: %v", err)
	}
	if firstDetails.EgressPolicyID == secondDetails.EgressPolicyID {
		t.Fatalf("proxy port change reused egress policy id %q", secondDetails.EgressPolicyID)
	}
	if secondDetails.ManifestAnthropicBaseURL != "http://harness-model-proxy.internal:8083" {
		t.Fatalf("manifest proxy alias was not persisted from explicit config: %+v", secondDetails)
	}
	wantSecondRules := []string{
		"tcp:10.240.0.5:8083",
		"tcp:172.16.0.138:9030",
		"tcp:172.16.0.138:8040",
		"tcp:172.16.0.139:9030",
		"tcp:172.16.0.139:8040",
	}
	assertJSONStrings(t, secondDetails.AllowedEgressRules, wantSecondRules)

	var policyRules string
	if err := st.db.QueryRowContext(ctx, `
SELECT allowed_egress_rules
FROM egress_policies
WHERE egress_policy_id = ?`, secondDetails.EgressPolicyID).Scan(&policyRules); err != nil {
		t.Fatalf("query egress policy rules: %v", err)
	}
	assertJSONStrings(t, policyRules, wantSecondRules)
}

func TestAllowedEgressRulesHonorsDNSPolicy(t *testing.T) {
	base := testAllocatorConfig(t)
	base.EgressDorisPorts = []int{9030}

	tests := []struct {
		name   string
		policy string
		fe     []string
		be     []string
		want   []string
	}{
		{
			name:   "hostnames_only skips DNS for ip only Doris hosts",
			policy: "hostnames_only",
			fe:     []string{"172.16.0.138"},
			be:     []string{"172.16.0.139"},
			want: []string{
				"tcp:10.240.0.1:8082",
				"tcp:172.16.0.138:9030",
				"tcp:172.16.0.139:9030",
			},
		},
		{
			name:   "hostnames_only allows DNS for Doris hostnames",
			policy: "hostnames_only",
			fe:     []string{"doris-fe.local"},
			be:     []string{"172.16.0.139"},
			want: []string{
				"tcp:10.240.0.1:8082",
				"tcp:doris-fe.local:9030",
				"tcp:172.16.0.139:9030",
				"udp:53",
				"tcp:53",
			},
		},
		{
			name:   "always allows DNS for ip only Doris hosts",
			policy: "always",
			fe:     []string{"172.16.0.138"},
			be:     []string{"172.16.0.139"},
			want: []string{
				"tcp:10.240.0.1:8082",
				"tcp:172.16.0.138:9030",
				"tcp:172.16.0.139:9030",
				"udp:53",
				"tcp:53",
			},
		},
		{
			name:   "off skips DNS even when given a hostname",
			policy: "off",
			fe:     []string{"doris-fe.local"},
			be:     []string{"172.16.0.139"},
			want: []string{
				"tcp:10.240.0.1:8082",
				"tcp:doris-fe.local:9030",
				"tcp:172.16.0.139:9030",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base
			cfg.EgressDNSPolicy = tt.policy
			cfg.EgressDorisFEHosts = tt.fe
			cfg.EgressDorisBEHosts = tt.be

			got := allowedEgressRules("10.240.0.1", cfg)
			if !slices.Equal(got, tt.want) {
				t.Fatalf("allowed egress rules = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestAllocateGenerationPersistsHostnameOnlyDNSPolicy(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_dns_policy")
	cfg := testAllocatorConfig(t)
	cfg.EgressDorisPorts = []int{9030}
	now := time.Now().UTC()

	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_dns_policy",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}

	want := []string{
		"tcp:10.240.0.1:8082",
		"tcp:172.16.0.138:9030",
		"tcp:172.16.0.139:9030",
	}
	details, err := st.GetRuntimeGenerationDetails(ctx, "sess_dns_policy", allocation.GenerationID)
	if err != nil {
		t.Fatalf("get runtime generation details: %v", err)
	}
	assertJSONStrings(t, details.AllowedEgressRules, want)
	if !strings.Contains(details.EgressPolicyID, "dns_allowed=false") {
		t.Fatalf("egress policy id %q should include derived DNS allowance", details.EgressPolicyID)
	}

	var policyRules string
	if err := st.db.QueryRowContext(ctx, `
SELECT allowed_egress_rules
FROM egress_policies
WHERE egress_policy_id = ?`, details.EgressPolicyID).Scan(&policyRules); err != nil {
		t.Fatalf("query egress policy rules: %v", err)
	}
	assertJSONStrings(t, policyRules, want)
}

func TestAllocateGenerationSnapshotsSessionAutoCheckpointPolicy(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	now := time.Now().UTC()
	if err := st.CreateSession(ctx, Session{
		ID:                    "sess_policy",
		UserID:                "lab",
		Status:                string(sessionstate.Created),
		DriverID:              "claude_code",
		Mode:                  ModeForDriver("claude_code"),
		AutoCheckpointEnabled: true,
		CreatedAt:             now,
		UpdatedAt:             now,
	}); err != nil {
		t.Fatalf("create policy session: %v", err)
	}
	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_policy",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    testAllocatorConfig(t),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `UPDATE sessions SET auto_checkpoint_enabled = 0 WHERE id = 'sess_policy'`); err != nil {
		t.Fatalf("disable session policy after allocation: %v", err)
	}
	details, err := st.GetRuntimeGenerationDetails(ctx, "sess_policy", allocation.GenerationID)
	if err != nil {
		t.Fatalf("get generation details: %v", err)
	}
	if !details.AutoCheckpointEnabled {
		t.Fatalf("generation policy should snapshot enabled session policy: %+v", details)
	}
}

func TestAllocateGenerationCanCASFromFailedActiveGeneration(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_new_generation")
	cfg := testAllocatorConfig(t)
	cfg.CIDRPool = netip.MustParsePrefix("10.240.0.0/28")
	now := time.Now().UTC()
	leaseOwner := GenerationLeaseOwner(owner.UUID)

	first, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_new_generation",
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate first generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, "sess_new_generation", first.GenerationID, first.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark first generation live: %v", err)
	}
	if err := st.FailGeneration(ctx, FailGenerationParams{
		SessionID:    "sess_new_generation",
		GenerationID: first.GenerationID,
		Owner:        first.Owner,
		ErrorClass:   "orchestrator_restart_reconnect_grace_expired",
		Reason:       "orchestrator_restart_reconnect_grace_expired",
		Now:          now.Add(2 * time.Second),
	}); err != nil {
		t.Fatalf("fail first generation: %v", err)
	}

	_, err = st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID:            "sess_new_generation",
		ExpectedGenerationID: sql.NullString{String: "gen_stale", Valid: true},
		Owner:                leaseOwner,
		LeaseTTL:             time.Minute,
		Now:                  now.Add(3 * time.Second),
		Config:               cfg,
	})
	if err == nil || !strings.Contains(err.Error(), "session active generation CAS failed") {
		t.Fatalf("expected stale active-generation CAS failure, got %v", err)
	}
	var generations int
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_generations WHERE session_id = 'sess_new_generation'`).Scan(&generations); err != nil {
		t.Fatalf("count generations after stale CAS: %v", err)
	}
	if generations != 1 {
		t.Fatalf("stale CAS should roll back inserted rows, generations=%d", generations)
	}

	next, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID:            "sess_new_generation",
		ExpectedGenerationID: sql.NullString{String: first.GenerationID, Valid: true},
		Owner:                leaseOwner,
		LeaseTTL:             time.Minute,
		Now:                  now.Add(4 * time.Second),
		Config:               cfg,
	})
	if err != nil {
		t.Fatalf("allocate replacement generation: %v", err)
	}
	if next.GenerationID == first.GenerationID {
		t.Fatalf("replacement reused failed generation id %s", next.GenerationID)
	}

	var activeGeneration, firstStatus, firstNetworkState, nextStatus, nextCIDR string
	if err := st.db.QueryRowContext(ctx, `SELECT active_generation_id FROM sessions WHERE id = 'sess_new_generation'`).Scan(&activeGeneration); err != nil {
		t.Fatalf("query active generation: %v", err)
	}
	if activeGeneration != next.GenerationID {
		t.Fatalf("active generation = %q, want %q", activeGeneration, next.GenerationID)
	}
	if err := st.db.QueryRowContext(ctx, `
SELECT g.status, n.allocation_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
WHERE g.generation_id = ?`, first.GenerationID).Scan(&firstStatus, &firstNetworkState); err != nil {
		t.Fatalf("query first generation: %v", err)
	}
	if firstStatus != "failed" || firstNetworkState != "reclaimable" {
		t.Fatalf("first generation not fenced/reclaimable: status=%s network=%s", firstStatus, firstNetworkState)
	}
	if err := st.db.QueryRowContext(ctx, `
SELECT g.status, n.host_side_cidr
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
WHERE g.generation_id = ?`, next.GenerationID).Scan(&nextStatus, &nextCIDR); err != nil {
		t.Fatalf("query replacement generation: %v", err)
	}
	if nextStatus != "allocating" || nextCIDR != "10.240.0.4/30" {
		t.Fatalf("unexpected replacement generation state: status=%s cidr=%s", nextStatus, nextCIDR)
	}
}

func TestClaimCheckpointedGenerationForRestoreMovesReservedResources(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_restore_claim")
	cfg := testAllocatorConfig(t)
	now := time.Now().UTC()
	leaseOwner := GenerationLeaseOwner(owner.UUID)

	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_restore_claim",
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, "sess_restore_claim", allocation.GenerationID, allocation.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark generation live: %v", err)
	}
	if err := st.RecordGenerationRuntimeArtifactDigests(ctx, allocation.GenerationID, GenerationRuntimeArtifactDigests{
		ControlManifestDigest:          "manifest_digest",
		ProjectedControlManifestDigest: "manifest_digest",
		BundleDigest:                   "bundle_digest",
		RuntimeConfigDigest:            "runtime_config_digest",
		SpecDigest:                     "spec_digest",
		RunscVersion:                   "runsc test",
		RunscBinaryPath:                "/usr/local/bin/runsc-test",
		RunscBinaryDigest:              "sha256:runsc-test",
	}); err != nil {
		t.Fatalf("record artifacts: %v", err)
	}
	checkpointedGeneration(t, ctx, st, "sess_restore_claim", allocation.GenerationID, now.Add(2*time.Second))

	claimed, err := st.ClaimCheckpointedGenerationForRestore(ctx, ClaimCheckpointedGenerationParams{
		SessionID:    "sess_restore_claim",
		GenerationID: allocation.GenerationID,
		Owner:        leaseOwner,
		LeaseTTL:     2 * time.Minute,
		Now:          now.Add(3 * time.Second),
	})
	if err != nil {
		t.Fatalf("claim checkpointed generation: %v", err)
	}
	if claimed.GenerationID != allocation.GenerationID ||
		claimed.NetworkProfileID != allocation.NetworkProfileID ||
		claimed.AgentRuntimeProfileID != allocation.AgentRuntimeProfileID ||
		claimed.Owner != leaseOwner ||
		!claimed.LeaseExpiresAt.Equal(now.Add(3*time.Second).Add(2*time.Minute)) {
		t.Fatalf("unexpected claimed allocation: %+v want base %+v", claimed, allocation)
	}

	var generationStatus, generationOwner, leaseExpires, lastSeen, networkState, resourceState string
	if err := st.db.QueryRowContext(ctx, `
SELECT g.status, COALESCE(g.lease_owner, ''), COALESCE(g.lease_expires_at, ''), COALESCE(g.last_seen_at, ''),
       n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&generationStatus, &generationOwner, &leaseExpires, &lastSeen, &networkState, &resourceState); err != nil {
		t.Fatalf("query restore claim state: %v", err)
	}
	if generationStatus != "restoring" ||
		generationOwner != leaseOwner ||
		!parseTime(leaseExpires).Equal(claimed.LeaseExpiresAt) ||
		!parseTime(lastSeen).Equal(now.Add(3*time.Second)) ||
		networkState != "recreating" ||
		resourceState != "recreating" {
		t.Fatalf("unexpected restore claim state: generation=%s owner=%s expires=%s last_seen=%s network=%s resource=%s",
			generationStatus, generationOwner, leaseExpires, lastSeen, networkState, resourceState)
	}
	details, err := st.GetRuntimeGenerationDetails(ctx, "sess_restore_claim", allocation.GenerationID)
	if err != nil {
		t.Fatalf("get restore details: %v", err)
	}
	if details.NetworkAllocationState != "recreating" ||
		details.ControlManifestDigest != "manifest_digest" ||
		details.RunscVersion != "runsc test" ||
		details.RunscBinaryPath != "/usr/local/bin/runsc-test" ||
		details.RunscBinaryDigest != "sha256:runsc-test" {
		t.Fatalf("restore details not preserved: %+v", details)
	}
	if details.CheckpointNetworkProfileID != allocation.NetworkProfileID ||
		details.CheckpointAgentRuntimeProfileID != allocation.AgentRuntimeProfileID ||
		details.CheckpointRunscVersion != "runsc test" ||
		details.CheckpointRunscPlatform != "systrap" ||
		details.CheckpointRunscBinaryPath != "/usr/local/bin/runsc-test" ||
		details.CheckpointRunscBinaryDigest != "sha256:runsc-test" ||
		details.CheckpointBundleDigest != "bundle_digest" ||
		details.CheckpointRuntimeConfigDigest != "runtime_config_digest" ||
		details.CheckpointControlManifestDigest != "manifest_digest" {
		t.Fatalf("checkpoint metadata not loaded into restore details: %+v", details)
	}

	if err := st.MarkGenerationResourcesLive(ctx, "sess_restore_claim", allocation.GenerationID, claimed.Owner, now.Add(4*time.Second)); err != nil {
		t.Fatalf("mark restored generation live: %v", err)
	}
	if err := st.db.QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&generationStatus, &networkState, &resourceState); err != nil {
		t.Fatalf("query restored live state: %v", err)
	}
	if generationStatus != "idle" || networkState != "live" || resourceState != "live" {
		t.Fatalf("restored generation not live idle: generation=%s network=%s resource=%s", generationStatus, networkState, resourceState)
	}
}

func TestClaimCheckpointedGenerationForRestoreRollsBackOnResourceMismatch(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_restore_mismatch")
	cfg := testAllocatorConfig(t)
	now := time.Now().UTC()
	leaseOwner := GenerationLeaseOwner(owner.UUID)

	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_restore_mismatch",
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, "sess_restore_mismatch", allocation.GenerationID, allocation.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark generation live: %v", err)
	}
	checkpointedGeneration(t, ctx, st, "sess_restore_mismatch", allocation.GenerationID, now.Add(2*time.Second))
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generation_resources
SET resource_state = 'live'
WHERE generation_id = ?`, allocation.GenerationID); err != nil {
		t.Fatalf("break resource state: %v", err)
	}

	_, err = st.ClaimCheckpointedGenerationForRestore(ctx, ClaimCheckpointedGenerationParams{
		SessionID:    "sess_restore_mismatch",
		GenerationID: allocation.GenerationID,
		Owner:        leaseOwner,
		LeaseTTL:     time.Minute,
		Now:          now.Add(3 * time.Second),
	})
	if err == nil || !strings.Contains(err.Error(), "checkpointed resource restore CAS failed") {
		t.Fatalf("expected resource CAS failure, got %v", err)
	}
	var generationStatus, leaseOwnerAfter, networkState, resourceState string
	if err := st.db.QueryRowContext(ctx, `
SELECT g.status, COALESCE(g.lease_owner, ''), n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&generationStatus, &leaseOwnerAfter, &networkState, &resourceState); err != nil {
		t.Fatalf("query rolled back state: %v", err)
	}
	if generationStatus != "checkpointed" || leaseOwnerAfter != "" || networkState != "reserved_checkpointed" || resourceState != "live" {
		t.Fatalf("restore claim did not roll back cleanly: generation=%s owner=%q network=%s resource=%s",
			generationStatus, leaseOwnerAfter, networkState, resourceState)
	}
}

func TestClaimCheckpointedGenerationForRestoreRequiresCheckpointedSession(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_restore_session_state")
	cfg := testAllocatorConfig(t)
	now := time.Now().UTC()
	leaseOwner := GenerationLeaseOwner(owner.UUID)

	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_restore_session_state",
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, "sess_restore_session_state", allocation.GenerationID, allocation.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark generation live: %v", err)
	}
	checkpointedGeneration(t, ctx, st, "sess_restore_session_state", allocation.GenerationID, now.Add(2*time.Second))
	if err := st.UpdateSessionStatus(ctx, "sess_restore_session_state", string(sessionstate.RunningIdle), nil); err != nil {
		t.Fatalf("set non-checkpointed session state: %v", err)
	}

	_, err = st.ClaimCheckpointedGenerationForRestore(ctx, ClaimCheckpointedGenerationParams{
		SessionID:    "sess_restore_session_state",
		GenerationID: allocation.GenerationID,
		Owner:        leaseOwner,
		LeaseTTL:     time.Minute,
		Now:          now.Add(3 * time.Second),
	})
	if !errors.Is(err, ErrStaleCheckpointRestore) {
		t.Fatalf("expected stale checkpoint restore error, got %v", err)
	}
	var generationStatus, leaseOwnerAfter, networkState, resourceState string
	if err := st.db.QueryRowContext(ctx, `
SELECT g.status, COALESCE(g.lease_owner, ''), n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&generationStatus, &leaseOwnerAfter, &networkState, &resourceState); err != nil {
		t.Fatalf("query rejected restore state: %v", err)
	}
	if generationStatus != "checkpointed" || leaseOwnerAfter != "" || networkState != "reserved_checkpointed" || resourceState != "reserved_checkpointed" {
		t.Fatalf("restore claim mutated rejected session state: generation=%s owner=%q network=%s resource=%s",
			generationStatus, leaseOwnerAfter, networkState, resourceState)
	}
}

func TestAllocateShellGenerationHasNoSecretReferences(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	createStoreSessionWithAgent(t, ctx, st, "sess_shell", "sh")
	cfg := testAllocatorConfig(t)
	cfg.DriverID = "sh"
	cfg.Model = ""
	cfg.OutputFormat = "shell_pty"
	modelAccessAllowed := false
	cfg.ModelAccessAllowed = &modelAccessAllowed
	cfg.ProviderCredentialsHostOnly = false
	cfg.SandboxModelProxyBaseURL = ""

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
	if details.DriverID != "sh" ||
		details.RequiresSecretDrop ||
		details.SecretsDirPath != "" ||
		details.AnthropicAPIKeySecretID != "" ||
		details.AnthropicAuthTokenSecretID != "" ||
		details.SecretVersion != "" {
		t.Fatalf("shell generation should not carry secrets: %+v", details)
	}
}

func TestAllocateClaudeHostOnlyGenerationHasNoSecretReferences(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_claude_host_only")
	cfg := testAllocatorConfig(t)
	cfg.ProviderCredentialsHostOnly = true
	cfg.SandboxModelProxyBaseURL = "http://harness-model-proxy.internal:8082"

	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_claude_host_only",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate host-only claude generation: %v", err)
	}
	details, err := st.GetRuntimeGenerationDetails(ctx, "sess_claude_host_only", allocation.GenerationID)
	if err != nil {
		t.Fatalf("get host-only claude generation details: %v", err)
	}
	if details.DriverID != "claude_code" ||
		details.RequiresSecretDrop ||
		details.SecretsDirPath != "" ||
		details.AnthropicAPIKeySecretID != "" ||
		details.AnthropicAuthTokenSecretID != "" ||
		details.SecretVersion != "" {
		t.Fatalf("host-only claude generation should not carry secrets: %+v", details)
	}
	wantHostsSuffix := filepath.Join("network", "gen-"+allocation.GenerationID, "hosts")
	if details.NetworkHostsPath == "" || !strings.HasSuffix(details.NetworkHostsPath, wantHostsSuffix) {
		t.Fatalf("host-only claude generation should carry network hosts projection path ending %q: %+v", wantHostsSuffix, details)
	}
	if !details.ModelAccessAllowed {
		t.Fatalf("host-only claude generation should allow model access: %+v", details)
	}
	if details.ManifestAnthropicBaseURL != "http://harness-model-proxy.internal:8082" {
		t.Fatalf("manifest base url = %q", details.ManifestAnthropicBaseURL)
	}
}

func TestAllocateClaudeRejectsInvalidSandboxModelProxyBaseURL(t *testing.T) {
	tests := []struct {
		name    string
		baseURL string
		want    string
	}{
		{
			name:    "gateway literal",
			baseURL: "http://10.240.0.1:8082",
			want:    "IP literal",
		},
		{
			name:    "localhost",
			baseURL: "http://localhost:8082",
			want:    "host-local",
		},
		{
			name:    "provider upstream",
			baseURL: "http://api.anthropic.com",
			want:    "provider upstream",
		},
		{
			name:    "path",
			baseURL: "http://harness-model-proxy.internal:8082/v1",
			want:    "must not include a path",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			st, owner := openOwnedStore(t, ctx)
			createStoreSession(t, ctx, st, "sess_invalid_proxy")
			cfg := testAllocatorConfig(t)
			cfg.SandboxModelProxyBaseURL = tt.baseURL

			_, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
				SessionID: "sess_invalid_proxy",
				Owner:     GenerationLeaseOwner(owner.UUID),
				LeaseTTL:  time.Minute,
				Now:       time.Now().UTC(),
				Config:    cfg,
			})
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected %q rejection, got %v", tt.want, err)
			}
		})
	}
}

func TestAllocateClaudeRejectsMismatchedSandboxModelProxyPort(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_invalid_proxy_port")
	cfg := testAllocatorConfig(t)
	cfg.ProxyPort = 8083
	cfg.SandboxModelProxyBaseURL = "http://harness-model-proxy.internal:8082"

	_, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_invalid_proxy_port",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    cfg,
	})
	if err == nil || !strings.Contains(err.Error(), "must match proxy port 8083") {
		t.Fatalf("expected proxy port mismatch rejection, got %v", err)
	}
}

func TestAllocateClaudeModelAccessDisabledOmitsProxyAliasProjection(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_model_disabled")
	modelAccessAllowed := false
	cfg := testAllocatorConfig(t)
	cfg.ModelAccessAllowed = &modelAccessAllowed

	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_model_disabled",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate model-disabled claude generation: %v", err)
	}
	details, err := st.GetRuntimeGenerationDetails(ctx, "sess_model_disabled", allocation.GenerationID)
	if err != nil {
		t.Fatalf("get model-disabled generation details: %v", err)
	}
	if details.ModelAccessAllowed ||
		details.ManifestAnthropicBaseURL != "" ||
		details.NetworkHostsPath != "" {
		t.Fatalf("model-disabled generation should not expose proxy alias: %+v", details)
	}
}

func TestAllocateGenerationRuntimeProfileIncludesSandboxIdentity(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_identity_a")
	createStoreSession(t, ctx, st, "sess_identity_b")
	cfg := testAllocatorConfig(t)

	first, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_identity_a",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate first identity generation: %v", err)
	}
	cfg.SandboxGID = 8001
	second, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_identity_b",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate second identity generation: %v", err)
	}

	firstDetails, err := st.GetRuntimeGenerationDetails(ctx, "sess_identity_a", first.GenerationID)
	if err != nil {
		t.Fatalf("get first identity generation: %v", err)
	}
	secondDetails, err := st.GetRuntimeGenerationDetails(ctx, "sess_identity_b", second.GenerationID)
	if err != nil {
		t.Fatalf("get second identity generation: %v", err)
	}
	if firstDetails.AgentRuntimeProfileID == secondDetails.AgentRuntimeProfileID {
		t.Fatalf("runtime profile should differ when sandbox identity changes: first=%+v second=%+v", firstDetails, secondDetails)
	}
	if secondDetails.SandboxGID != 8001 {
		t.Fatalf("second sandbox gid = %d", secondDetails.SandboxGID)
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

func TestExpiredRuntimeRecoveryAndReaperTransitions(t *testing.T) {
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
	createLiveRuntimeResourceInstanceForAllocation(t, ctx, st, "sess_recover", allocation, owner.UUID, "host-recover", now.Add(-30*time.Second))

	recovered, err := recoverCleanedAllocations(t, ctx, st, StartupRecoveryParams{
		OwnerUUID:       owner.UUID,
		Now:             now,
		LeaseTTL:        time.Minute,
		ReconnectGrace:  30 * time.Second,
		AckStartedGrace: time.Minute,
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
	if reaped.DestroyedAllocations != 0 {
		t.Fatalf("store reaper must not mark physical allocations destroyed, got %+v", reaped)
	}
	destroyable, err := st.ListDestroyableReclaimableGenerations(ctx, now.Add(time.Second), 0)
	if err != nil {
		t.Fatalf("list destroyable resources: %v", err)
	}
	if len(destroyable) != 1 || destroyable[0].GenerationID != allocation.GenerationID {
		t.Fatalf("unexpected destroyable resources: %+v", destroyable)
	}
	if err := st.MarkGenerationResourcesDestroyed(ctx, DestroyGenerationResourcesParams{
		SessionID:    "sess_recover",
		GenerationID: allocation.GenerationID,
		Now:          now.Add(2 * time.Second),
	}); err != nil {
		t.Fatalf("mark generation resources destroyed: %v", err)
	}
	if err := st.MarkGenerationResourcesDestroyed(ctx, DestroyGenerationResourcesParams{
		SessionID:    "sess_recover",
		GenerationID: allocation.GenerationID,
		Now:          now.Add(3 * time.Second),
	}); err != nil {
		t.Fatalf("mark already destroyed generation resources: %v", err)
	}
	if destroyable, err = st.ListDestroyableReclaimableGenerations(ctx, now.Add(3*time.Second), 0); err != nil {
		t.Fatalf("list destroyable after mark: %v", err)
	} else if len(destroyable) != 0 {
		t.Fatalf("destroyed generation must not remain destroyable: %+v", destroyable)
	}
}

func TestExpiredRuntimeRecoveryDoesNotReclaimUnrelatedFailedGeneration(t *testing.T) {
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
	createLiveRuntimeResourceInstanceForAllocation(t, ctx, st, "sess_crashed", crashed, owner.UUID, "host-crashed", now.Add(-30*time.Second))

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

	recovered, err := recoverCleanedAllocations(t, ctx, st, StartupRecoveryParams{
		OwnerUUID:       owner.UUID,
		Now:             now,
		LeaseTTL:        time.Minute,
		ReconnectGrace:  30 * time.Second,
		AckStartedGrace: time.Minute,
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

func TestListExpiredRuntimeRecoveryCandidatesRequiresMatchingRuntimeResourceInstance(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	cfg.CIDRPool = netip.MustParsePrefix("10.240.0.0/27")
	now := time.Now().UTC()

	createExpiredIdle := func(sessionID string) GenerationAllocation {
		t.Helper()
		createStoreSession(t, ctx, st, sessionID)
		allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
			SessionID: sessionID,
			Owner:     GenerationLeaseOwner(owner.UUID),
			LeaseTTL:  time.Minute,
			Now:       now.Add(-3 * time.Minute),
			Config:    cfg,
		})
		if err != nil {
			t.Fatalf("allocate %s: %v", sessionID, err)
		}
		if err := st.MarkGenerationResourcesLive(ctx, sessionID, allocation.GenerationID, allocation.Owner, now.Add(-3*time.Minute+time.Second)); err != nil {
			t.Fatalf("mark %s live: %v", sessionID, err)
		}
		if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generations
SET lease_expires_at = ?
WHERE generation_id = ?`, formatTime(now.Add(-2*time.Minute)), allocation.GenerationID); err != nil {
			t.Fatalf("expire %s lease: %v", sessionID, err)
		}
		return allocation
	}

	valid := createExpiredIdle("sess_recovery_instance_valid")
	validInstance := createLiveRuntimeResourceInstanceForAllocation(t, ctx, st, "sess_recovery_instance_valid", valid, owner.UUID, "host-valid", now.Add(-2*time.Minute+time.Second))
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generation_resources
SET runsc_container_id = ?
WHERE generation_id = ?`, "legacy-"+valid.GenerationID, valid.GenerationID); err != nil {
		t.Fatalf("set stale legacy runtime id: %v", err)
	}

	createExpiredIdle("sess_recovery_instance_missing")

	mismatch := createExpiredIdle("sess_recovery_instance_mismatch")
	createLiveRuntimeResourceInstanceForAllocation(t, ctx, st, "sess_recovery_instance_mismatch", mismatch, owner.UUID, "host-mismatch", now.Add(-2*time.Minute+time.Second))
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generations
SET sandbox_contract_id = NULL
WHERE generation_id = ?`, mismatch.GenerationID); err != nil {
		t.Fatalf("break generation contract mirror: %v", err)
	}

	candidates, err := st.ListExpiredRuntimeRecoveryCandidates(ctx, StartupRecoveryParams{
		OwnerUUID:       owner.UUID,
		Now:             now,
		ReconnectGrace:  time.Minute,
		AckStartedGrace: time.Minute,
	})
	if err != nil {
		t.Fatalf("list recovery candidates: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("candidates=%+v, want only generation with matching runtime resource instance", candidates)
	}
	if candidates[0].GenerationID != valid.GenerationID ||
		candidates[0].RuntimeID != validInstance.RunscContainerID ||
		candidates[0].RuntimeID == "legacy-"+valid.GenerationID {
		t.Fatalf("unexpected recovery candidate: %+v want runtime id %q", candidates[0], validInstance.RunscContainerID)
	}
}

func TestExpiredRuntimeRecoveryRequiresPositiveGraceWindows(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	now := time.Now().UTC()

	tests := []struct {
		name string
		p    StartupRecoveryParams
		want string
	}{
		{
			name: "list missing reconnect grace",
			p: StartupRecoveryParams{
				OwnerUUID:       owner.UUID,
				Now:             now,
				AckStartedGrace: time.Minute,
			},
			want: "reconnect grace must be > 0",
		},
		{
			name: "list missing ack-started grace",
			p: StartupRecoveryParams{
				OwnerUUID:      owner.UUID,
				Now:            now,
				ReconnectGrace: time.Minute,
			},
			want: "ack-started grace must be > 0",
		},
		{
			name: "list negative reconnect grace",
			p: StartupRecoveryParams{
				OwnerUUID:       owner.UUID,
				Now:             now,
				ReconnectGrace:  -time.Second,
				AckStartedGrace: time.Minute,
			},
			want: "reconnect grace must be > 0",
		},
		{
			name: "list negative ack-started grace",
			p: StartupRecoveryParams{
				OwnerUUID:       owner.UUID,
				Now:             now,
				ReconnectGrace:  time.Minute,
				AckStartedGrace: -time.Second,
			},
			want: "ack-started grace must be > 0",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := st.ListExpiredRuntimeRecoveryCandidates(ctx, tc.p)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("list err=%v, want %q", err, tc.want)
			}
		})
	}

	repairTests := []struct {
		name string
		p    StartupRecoveryParams
		want string
	}{
		{
			name: "repair missing reconnect grace",
			p: StartupRecoveryParams{
				OwnerUUID:       owner.UUID,
				Now:             now,
				AckStartedGrace: time.Minute,
			},
			want: "reconnect grace must be > 0",
		},
		{
			name: "repair missing ack-started grace",
			p: StartupRecoveryParams{
				OwnerUUID:      owner.UUID,
				Now:            now,
				ReconnectGrace: time.Minute,
			},
			want: "ack-started grace must be > 0",
		},
		{
			name: "repair negative reconnect grace",
			p: StartupRecoveryParams{
				OwnerUUID:       owner.UUID,
				Now:             now,
				ReconnectGrace:  -time.Second,
				AckStartedGrace: time.Minute,
			},
			want: "reconnect grace must be > 0",
		},
		{
			name: "repair negative ack-started grace",
			p: StartupRecoveryParams{
				OwnerUUID:       owner.UUID,
				Now:             now,
				ReconnectGrace:  time.Minute,
				AckStartedGrace: -time.Second,
			},
			want: "ack-started grace must be > 0",
		},
	}
	for _, tc := range repairTests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := st.RepairExpiredRuntimeRecovery(ctx, tc.p, nil)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("repair err=%v, want %q", err, tc.want)
			}
		})
	}
}

func TestExpiredRuntimeRecoveryRequeuesExpiredLeasedTurn(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	createStoreSession(t, ctx, st, "sess_requeue")
	now := time.Now().UTC()
	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_requeue",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now.Add(-3 * time.Minute),
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, "sess_requeue", allocation.GenerationID, allocation.Owner, now.Add(-3*time.Minute+time.Second)); err != nil {
		t.Fatalf("mark resources live: %v", err)
	}
	createLiveRuntimeResourceInstanceForAllocation(t, ctx, st, "sess_requeue", allocation, owner.UUID, "host-requeue", now.Add(-3*time.Minute+2*time.Second))
	turnID, err := st.EnqueueTurn(ctx, "sess_requeue", "retry me", now.Add(-3*time.Minute+2*time.Second))
	if err != nil {
		t.Fatalf("enqueue turn: %v", err)
	}
	if grant, ok, err := st.ClaimNextTurn(ctx, ClaimNextTurnParams{
		SessionID:    "sess_requeue",
		GenerationID: allocation.GenerationID,
		Owner:        allocation.Owner,
		RequestID:    "req_requeue",
		LeaseTTL:     30 * time.Second,
		Now:          now.Add(-3*time.Minute + 3*time.Second),
	}); err != nil || !ok || grant.TurnID != turnID {
		t.Fatalf("claim setup: ok=%v grant=%+v err=%v", ok, grant, err)
	}

	recovered, err := recoverCleanedAllocations(t, ctx, st, StartupRecoveryParams{
		OwnerUUID:       owner.UUID,
		Now:             now,
		LeaseTTL:        time.Minute,
		ReconnectGrace:  time.Minute,
		AckStartedGrace: 2 * time.Minute,
	})
	if err != nil {
		t.Fatalf("recover allocations: %v", err)
	}
	if recovered.ReconnectGraceFailed != 1 || recovered.ExpiredLeasedRequeued != 1 || recovered.UnknownAfterAckStarted != 0 {
		t.Fatalf("unexpected recovery result: %+v", recovered)
	}

	var status string
	var generationID, leaseOwner, leaseExpires, claimRequest sql.NullString
	var attempt int
	if err := st.db.QueryRowContext(ctx, `
SELECT status, generation_id, lease_owner, lease_expires_at, claim_request_id, attempt
FROM turns
WHERE id = ?`, turnID).Scan(&status, &generationID, &leaseOwner, &leaseExpires, &claimRequest, &attempt); err != nil {
		t.Fatalf("query turn: %v", err)
	}
	if status != "queued" || generationID.Valid || leaseOwner.Valid || leaseExpires.Valid || claimRequest.Valid || attempt != 1 {
		t.Fatalf("leased turn was not reset for retry: status=%s gen=%v owner=%v expires=%v claim=%v attempt=%d", status, generationID, leaseOwner, leaseExpires, claimRequest, attempt)
	}
	var generationStatus, networkState, resourceState string
	if err := st.db.QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&generationStatus, &networkState, &resourceState); err != nil {
		t.Fatalf("query generation: %v", err)
	}
	if generationStatus != "failed" || networkState != "reclaimable" || resourceState != "reclaimable" {
		t.Fatalf("unexpected generation state after requeue recovery: generation=%s network=%s resource=%s", generationStatus, networkState, resourceState)
	}
}

func TestClaimNextTurnPreservesSequenceOrderingAfterRecoveryRequeue(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	cfg.CIDRPool = netip.MustParsePrefix("10.240.0.0/27")
	now := time.Now().UTC()
	ownerLease := GenerationLeaseOwner(owner.UUID)

	for _, tc := range []struct {
		name              string
		requeuedSequence  int64
		freshSequence     int64
		wantContent       string
		wantAttempt       int
		wantRequeuedClaim bool
	}{
		{
			name:              "requeued lower sequence wins",
			requeuedSequence:  10,
			freshSequence:     20,
			wantContent:       "retry me",
			wantAttempt:       1,
			wantRequeuedClaim: true,
		},
		{
			name:              "fresh lower sequence wins",
			requeuedSequence:  20,
			freshSequence:     10,
			wantContent:       "fresh work",
			wantAttempt:       0,
			wantRequeuedClaim: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sessionID := "sess_order_" + strings.NewReplacer(" ", "_").Replace(tc.name)
			createStoreSession(t, ctx, st, sessionID)
			oldAllocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
				SessionID: sessionID,
				Owner:     ownerLease,
				LeaseTTL:  time.Minute,
				Now:       now.Add(-3 * time.Minute),
				Config:    cfg,
			})
			if err != nil {
				t.Fatalf("allocate old generation: %v", err)
			}
			if err := st.MarkGenerationResourcesLive(ctx, sessionID, oldAllocation.GenerationID, oldAllocation.Owner, now.Add(-3*time.Minute+time.Second)); err != nil {
				t.Fatalf("mark old generation live: %v", err)
			}
			createLiveRuntimeResourceInstanceForAllocation(t, ctx, st, sessionID, oldAllocation, owner.UUID, "host-old-"+sessionID, now.Add(-3*time.Minute+2*time.Second))
			requeuedTurnID, err := st.EnqueueTurn(ctx, sessionID, "retry me", now.Add(-3*time.Minute+2*time.Second))
			if err != nil {
				t.Fatalf("enqueue requeued turn: %v", err)
			}
			if grant, ok, err := st.ClaimNextTurn(ctx, ClaimNextTurnParams{
				SessionID:    sessionID,
				GenerationID: oldAllocation.GenerationID,
				Owner:        oldAllocation.Owner,
				RequestID:    "req_old_" + sessionID,
				LeaseTTL:     30 * time.Second,
				Now:          now.Add(-3*time.Minute + 3*time.Second),
			}); err != nil || !ok || grant.TurnID != requeuedTurnID {
				t.Fatalf("claim old turn setup: ok=%v grant=%+v err=%v", ok, grant, err)
			}

			recovered, err := recoverCleanedAllocations(t, ctx, st, StartupRecoveryParams{
				OwnerUUID:       owner.UUID,
				Now:             now,
				LeaseTTL:        time.Minute,
				ReconnectGrace:  time.Minute,
				AckStartedGrace: 2 * time.Minute,
			})
			if err != nil {
				t.Fatalf("recover allocations: %v", err)
			}
			if recovered.ExpiredLeasedRequeued != 1 {
				t.Fatalf("unexpected recovery result: %+v", recovered)
			}

			freshTurnID, err := st.EnqueueTurn(ctx, sessionID, "fresh work", now.Add(time.Second))
			if err != nil {
				t.Fatalf("enqueue fresh turn: %v", err)
			}
			if _, err := st.db.ExecContext(ctx, `UPDATE turns SET sequence = ? WHERE id = ?`, tc.requeuedSequence, requeuedTurnID); err != nil {
				t.Fatalf("set requeued sequence: %v", err)
			}
			if _, err := st.db.ExecContext(ctx, `UPDATE turns SET sequence = ? WHERE id = ?`, tc.freshSequence, freshTurnID); err != nil {
				t.Fatalf("set fresh sequence: %v", err)
			}

			newAllocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
				SessionID: sessionID,
				ExpectedGenerationID: sql.NullString{
					String: oldAllocation.GenerationID,
					Valid:  true,
				},
				Owner:    ownerLease,
				LeaseTTL: time.Minute,
				Now:      now.Add(2 * time.Second),
				Config:   cfg,
			})
			if err != nil {
				t.Fatalf("allocate new generation: %v", err)
			}
			if err := st.MarkGenerationResourcesLive(ctx, sessionID, newAllocation.GenerationID, newAllocation.Owner, now.Add(3*time.Second)); err != nil {
				t.Fatalf("mark new generation live: %v", err)
			}
			createLiveRuntimeResourceInstanceForAllocation(t, ctx, st, sessionID, newAllocation, owner.UUID, "host-new-"+sessionID, now.Add(3*time.Second+time.Millisecond))

			grant, ok, err := st.ClaimNextTurn(ctx, ClaimNextTurnParams{
				SessionID:    sessionID,
				GenerationID: newAllocation.GenerationID,
				Owner:        newAllocation.Owner,
				RequestID:    "req_new_" + sessionID,
				LeaseTTL:     time.Minute,
				Now:          now.Add(4 * time.Second),
			})
			if err != nil {
				t.Fatalf("claim next turn: %v", err)
			}
			if !ok {
				t.Fatal("expected claim grant")
			}
			if grant.Content != tc.wantContent || grant.Attempt != tc.wantAttempt {
				t.Fatalf("unexpected grant: %+v want content=%q attempt=%d", grant, tc.wantContent, tc.wantAttempt)
			}
			if gotRequeued := grant.TurnID == requeuedTurnID; gotRequeued != tc.wantRequeuedClaim {
				t.Fatalf("claimed requeued=%v want %v grant=%+v requeued=%d fresh=%d", gotRequeued, tc.wantRequeuedClaim, grant, requeuedTurnID, freshTurnID)
			}
		})
	}
}

func TestExpiredRuntimeRecoveryLeavesAckStartedTurnDuringGrace(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	createStoreSession(t, ctx, st, "sess_ack_grace")
	now := time.Now().UTC()
	allocation, turnID := createExpiredAckStartedTurn(t, ctx, st, owner.UUID, cfg, "sess_ack_grace", now, 80*time.Second)

	recovered, err := recoverCleanedAllocations(t, ctx, st, StartupRecoveryParams{
		OwnerUUID:       owner.UUID,
		Now:             now,
		LeaseTTL:        time.Minute,
		ReconnectGrace:  10 * time.Second,
		AckStartedGrace: 90 * time.Second,
	})
	if err != nil {
		t.Fatalf("recover allocations: %v", err)
	}
	if recovered.ReconnectGraceFailed != 0 || recovered.UnknownAfterAckStarted != 0 || recovered.ExpiredLeasedRequeued != 0 {
		t.Fatalf("ack-started turn inside grace should not be fenced: %+v", recovered)
	}
	var turnStatus, generationStatus string
	if err := st.db.QueryRowContext(ctx, `SELECT status FROM turns WHERE id = ?`, turnID).Scan(&turnStatus); err != nil {
		t.Fatalf("query turn status: %v", err)
	}
	if err := st.db.QueryRowContext(ctx, `SELECT status FROM runtime_generations WHERE generation_id = ?`, allocation.GenerationID).Scan(&generationStatus); err != nil {
		t.Fatalf("query generation status: %v", err)
	}
	if turnStatus != "running" || generationStatus != "active" {
		t.Fatalf("ack-started turn should remain recoverable inside grace: turn=%s generation=%s", turnStatus, generationStatus)
	}
}

func TestExpiredRuntimeRecoveryMarksExpiredAckStartedTurnUnknown(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	createStoreSession(t, ctx, st, "sess_ack_unknown")
	now := time.Now().UTC()
	allocation, turnID := createExpiredAckStartedTurn(t, ctx, st, owner.UUID, cfg, "sess_ack_unknown", now, 3*time.Minute)

	recovered, err := recoverCleanedAllocations(t, ctx, st, StartupRecoveryParams{
		OwnerUUID:       owner.UUID,
		Now:             now,
		LeaseTTL:        time.Minute,
		ReconnectGrace:  time.Minute,
		AckStartedGrace: 90 * time.Second,
	})
	if err != nil {
		t.Fatalf("recover allocations: %v", err)
	}
	if recovered.UnknownAfterAckStarted != 1 || recovered.ReconnectGraceFailed != 0 || recovered.ExpiredLeasedRequeued != 0 {
		t.Fatalf("unexpected recovery result: %+v", recovered)
	}
	var turnStatus, turnError, generationStatus, generationError, networkState, resourceState string
	if err := st.db.QueryRowContext(ctx, `
SELECT t.status, COALESCE(t.error_class, ''), g.status, COALESCE(g.error_class, ''), n.allocation_state, r.resource_state
FROM turns t
JOIN runtime_generations g ON g.generation_id = t.generation_id
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE t.id = ?`, turnID).Scan(&turnStatus, &turnError, &generationStatus, &generationError, &networkState, &resourceState); err != nil {
		t.Fatalf("query recovered state: %v", err)
	}
	if turnStatus != "failed" ||
		turnError != "unknown_after_ack_started" ||
		generationStatus != "failed" ||
		generationError != "unknown_after_ack_started" ||
		networkState != "reclaimable" ||
		resourceState != "reclaimable" {
		t.Fatalf("unexpected unknown-after-ack state: turn=%s/%s generation=%s/%s network=%s resource=%s", turnStatus, turnError, generationStatus, generationError, networkState, resourceState)
	}
	var contexts int
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM active_model_request_contexts WHERE generation_id = ?`, allocation.GenerationID).Scan(&contexts); err != nil {
		t.Fatalf("count active contexts: %v", err)
	}
	if contexts != 0 {
		t.Fatalf("active model contexts should be cleared, got %d", contexts)
	}
}

func TestExpiredRuntimeRecoveryDeletesStaleProxyContextsFromPreviousOwner(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	now := time.Now().UTC()

	createStoreSession(t, ctx, st, "sess_proxy_context_current")
	current, currentTurnID := createExpiredAckStartedTurn(t, ctx, st, owner.UUID, cfg, "sess_proxy_context_current", now, 30*time.Second)
	createStoreSession(t, ctx, st, "sess_proxy_context_stale")
	stale, staleTurnID := createExpiredAckStartedTurn(t, ctx, st, owner.UUID, cfg, "sess_proxy_context_stale", now, 30*time.Second)
	staleOwner := GenerationLeaseOwner("previous-owner")
	if _, err := st.db.ExecContext(ctx, `
UPDATE active_model_request_contexts
SET lease_owner = ?
WHERE generation_id = ?`, staleOwner, stale.GenerationID); err != nil {
		t.Fatalf("move stale proxy context to previous owner: %v", err)
	}

	recovered, err := recoverCleanedAllocations(t, ctx, st, StartupRecoveryParams{
		OwnerUUID:       owner.UUID,
		Now:             now,
		LeaseTTL:        time.Minute,
		ReconnectGrace:  10 * time.Second,
		AckStartedGrace: 90 * time.Second,
	})
	if err != nil {
		t.Fatalf("recover allocations: %v", err)
	}
	if recovered.UnknownAfterAckStarted != 0 || recovered.ReconnectGraceFailed != 0 {
		t.Fatalf("proxy context cleanup should not fence recoverable turns: %+v", recovered)
	}

	var currentContexts, staleContexts int
	if err := st.db.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM active_model_request_contexts
WHERE generation_id = ?
  AND turn_id = ?
  AND lease_owner = ?`, current.GenerationID, currentTurnID, current.Owner).Scan(&currentContexts); err != nil {
		t.Fatalf("count current proxy contexts: %v", err)
	}
	if err := st.db.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM active_model_request_contexts
WHERE generation_id = ?
  AND turn_id = ?`, stale.GenerationID, staleTurnID).Scan(&staleContexts); err != nil {
		t.Fatalf("count stale proxy contexts: %v", err)
	}
	if currentContexts != 1 || staleContexts != 0 {
		t.Fatalf("unexpected proxy context cleanup: current=%d stale=%d", currentContexts, staleContexts)
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

func TestListBridgePollGenerationsFiltersCurrentOwnerLiveResources(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	cfg.CIDRPool = netip.MustParsePrefix("10.240.0.0/27")
	now := time.Now().UTC()

	createStoreSession(t, ctx, st, "sess_poll")
	pollAllocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_poll",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate poll generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, "sess_poll", pollAllocation.GenerationID, pollAllocation.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark poll generation live: %v", err)
	}
	pollResource := createLiveRuntimeResourceInstanceForAllocation(t, ctx, st, "sess_poll", pollAllocation, owner.UUID, "host-1", now.Add(2*time.Second))
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generation_resources
SET resource_state = 'ready',
    bridge_dir_path = ?
WHERE generation_id = ?`, filepath.Join(t.TempDir(), "stale-bridge"), pollAllocation.GenerationID); err != nil {
		t.Fatalf("make poll resource state stale: %v", err)
	}

	createStoreSession(t, ctx, st, "sess_other")
	otherAllocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_other",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate other generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, "sess_other", otherAllocation.GenerationID, otherAllocation.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark other generation live: %v", err)
	}
	createLiveRuntimeResourceInstanceForAllocation(t, ctx, st, "sess_other", otherAllocation, owner.UUID, "host-1", now.Add(2*time.Second))
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generations
SET lease_owner = ?
WHERE generation_id = ?`, GenerationLeaseOwner("other-owner"), otherAllocation.GenerationID); err != nil {
		t.Fatalf("move other generation to another owner: %v", err)
	}

	createStoreSession(t, ctx, st, "sess_ready_only")
	readyOnly, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_ready_only",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate ready-only generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, "sess_ready_only", readyOnly.GenerationID, readyOnly.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark ready-only generation live: %v", err)
	}
	createLiveRuntimeResourceInstanceForAllocation(t, ctx, st, "sess_ready_only", readyOnly, owner.UUID, "host-1", now.Add(2*time.Second))
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_resource_instances
SET state = 'ready'
WHERE generation_id = ?`, readyOnly.GenerationID); err != nil {
		t.Fatalf("move ready-only runtime resource to ready: %v", err)
	}

	generations, err := st.ListBridgePollGenerations(ctx, pollAllocation.Owner, now.Add(2*time.Second), 0)
	if err != nil {
		t.Fatalf("list bridge poll generations: %v", err)
	}
	if len(generations) != 1 {
		t.Fatalf("generations=%+v, want one current-owner live generation", generations)
	}
	if generations[0].SessionID != "sess_poll" ||
		generations[0].GenerationID != pollAllocation.GenerationID ||
		generations[0].BridgeDirPath != pollResource.BridgeDirPath {
		t.Fatalf("unexpected poll generation: %+v", generations[0])
	}
}

func TestListBridgePollGenerationsIncludesAckStartedExpiredLeaseDuringGrace(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	now := time.Now().UTC()

	createStoreSession(t, ctx, st, "sess_poll_recover")
	recoverable, recoverableTurnID := createExpiredAckStartedTurn(t, ctx, st, owner.UUID, cfg, "sess_poll_recover", now, 30*time.Second)
	previousOwner := GenerationLeaseOwner("previous-owner")
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generations
SET lease_owner = ?
WHERE generation_id = ?`, previousOwner, recoverable.GenerationID); err != nil {
		t.Fatalf("move recoverable generation owner: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `
UPDATE turns
SET lease_owner = ?
WHERE id = ?`, previousOwner, recoverableTurnID); err != nil {
		t.Fatalf("move recoverable turn owner: %v", err)
	}

	createStoreSession(t, ctx, st, "sess_poll_expired")
	expired, _ := createExpiredAckStartedTurn(t, ctx, st, owner.UUID, cfg, "sess_poll_expired", now, 2*time.Minute)

	generations, err := st.ListBridgePollGenerations(ctx, recoverable.Owner, now, time.Minute)
	if err != nil {
		t.Fatalf("list bridge poll generations: %v", err)
	}
	if len(generations) != 1 {
		t.Fatalf("generations=%+v, want only recoverable ack-started generation", generations)
	}
	if generations[0].SessionID != "sess_poll_recover" ||
		generations[0].GenerationID != recoverable.GenerationID ||
		generations[0].BridgeDirPath == "" {
		t.Fatalf("unexpected recoverable generation: %+v", generations[0])
	}

	generations, err = st.ListBridgePollGenerations(ctx, recoverable.Owner, now, 0)
	if err != nil {
		t.Fatalf("list bridge poll generations without grace: %v", err)
	}
	for _, generation := range generations {
		if generation.GenerationID == recoverable.GenerationID || generation.GenerationID == expired.GenerationID {
			t.Fatalf("expired ack-started generation listed without grace: %+v", generations)
		}
	}
}

func TestAutoCheckpointCandidatesRequirePolicyArtifactsAndNoActiveTurns(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	cfg.CIDRPool = netip.MustParsePrefix("10.240.0.0/27")
	now := time.Now().UTC()
	ownerLease := GenerationLeaseOwner(owner.UUID)

	eligible := createAutoCheckpointGeneration(t, ctx, st, cfg, "sess_auto_eligible", ownerLease, now)
	eligibleResource, err := st.GetRuntimeResourceInstance(ctx, eligible.GenerationID)
	if err != nil {
		t.Fatalf("get eligible runtime resource: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generation_resources
SET resource_state = 'ready',
    bridge_dir_path = ?
WHERE generation_id = ?`, filepath.Join(t.TempDir(), "stale-auto-bridge"), eligible.GenerationID); err != nil {
		t.Fatalf("make auto checkpoint resource state stale: %v", err)
	}
	disabled := createAutoCheckpointGeneration(t, ctx, st, cfg, "sess_auto_disabled", ownerLease, now)
	if _, err := st.db.ExecContext(ctx, `UPDATE sessions SET auto_checkpoint_enabled = 0 WHERE id = ?`, "sess_auto_disabled"); err != nil {
		t.Fatalf("disable session policy: %v", err)
	}
	busy := createAutoCheckpointGeneration(t, ctx, st, cfg, "sess_auto_busy", ownerLease, now)
	if _, err := st.EnqueueTurn(ctx, "sess_auto_busy", "queued", now.Add(time.Second)); err != nil {
		t.Fatalf("enqueue busy turn: %v", err)
	}
	missingArtifacts := createAutoCheckpointGeneration(t, ctx, st, cfg, "sess_auto_missing_artifacts", ownerLease, now)
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generation_resources
SET bundle_digest = NULL
WHERE generation_id = ?`, missingArtifacts.GenerationID); err != nil {
		t.Fatalf("clear artifact digest: %v", err)
	}
	otherOwner := createAutoCheckpointGeneration(t, ctx, st, cfg, "sess_auto_other_owner", ownerLease, now)
	if _, err := st.db.ExecContext(ctx, `UPDATE runtime_generations SET lease_owner = ? WHERE generation_id = ?`, GenerationLeaseOwner("other"), otherOwner.GenerationID); err != nil {
		t.Fatalf("move owner: %v", err)
	}
	readyOnly := createAutoCheckpointGeneration(t, ctx, st, cfg, "sess_auto_ready_resource", ownerLease, now)
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_resource_instances
SET state = 'ready'
WHERE generation_id = ?`, readyOnly.GenerationID); err != nil {
		t.Fatalf("move runtime resource out of live: %v", err)
	}

	candidates, err := st.ListAutoCheckpointCandidates(ctx, ownerLease, now.Add(2*time.Minute), time.Minute)
	if err != nil {
		t.Fatalf("list candidates: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("candidates=%+v want one eligible generation", candidates)
	}
	if candidates[0].SessionID != "sess_auto_eligible" ||
		candidates[0].GenerationID != eligible.GenerationID ||
		candidates[0].BridgeDirPath != eligibleResource.BridgeDirPath {
		t.Fatalf("unexpected candidate: %+v eligible=%+v disabled=%s busy=%s",
			candidates[0], eligible, disabled.GenerationID, busy.GenerationID)
	}
}

func TestGenerationCheckpointTransitionsAndMetadata(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	now := time.Now().UTC()
	allocation := createAutoCheckpointGeneration(t, ctx, st, cfg, "sess_auto_complete", GenerationLeaseOwner(owner.UUID), now)
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generation_resources
SET resource_state = 'ready'
WHERE generation_id = ?`, allocation.GenerationID); err != nil {
		t.Fatalf("make checkpoint resource state stale: %v", err)
	}

	if err := st.BeginGenerationCheckpoint(ctx, "sess_auto_complete", allocation.GenerationID, allocation.Owner, now.Add(2*time.Minute)); err != nil {
		t.Fatalf("begin checkpoint: %v", err)
	}
	var generationStatus, sessionStatus string
	if err := st.db.QueryRowContext(ctx, `
SELECT g.status, s.status
FROM runtime_generations g
JOIN sessions s ON s.active_generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&generationStatus, &sessionStatus); err != nil {
		t.Fatalf("query checkpointing state: %v", err)
	}
	if generationStatus != "checkpointing" || sessionStatus != string(sessionstate.Checkpointing) {
		t.Fatalf("unexpected checkpointing state: generation=%s session=%s", generationStatus, sessionStatus)
	}
	if err := st.CompleteGenerationCheckpoint(ctx, CompleteCheckpointParams{
		SessionID:                       "sess_auto_complete",
		GenerationID:                    allocation.GenerationID,
		Owner:                           allocation.Owner,
		CheckpointPath:                  filepath.Join(cfg.RunDir, "checkpoint"),
		RunscPlatform:                   "systrap",
		RunscVersion:                    "runsc auto",
		RunscBinaryPath:                 "/usr/local/bin/runsc-auto",
		RunscBinaryDigest:               "sha256:runsc-auto",
		CheckpointBundleDigest:          "bundle_digest",
		CheckpointRuntimeConfigDigest:   "runtime_config_digest",
		CheckpointControlManifestDigest: "projected_manifest_digest",
		Now:                             now.Add(3 * time.Minute),
	}); err != nil {
		t.Fatalf("complete checkpoint: %v", err)
	}
	var networkState, resourceState, checkpointPath, checkpointBundle, checkpointManifest string
	if err := st.db.QueryRowContext(ctx, `
SELECT g.status, s.status, n.allocation_state, r.resource_state, COALESCE(r.checkpoint_path, ''),
       COALESCE(g.checkpoint_bundle_digest, ''), COALESCE(g.checkpoint_control_manifest_digest, '')
FROM runtime_generations g
JOIN sessions s ON s.active_generation_id = g.generation_id
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(
		&generationStatus, &sessionStatus, &networkState, &resourceState, &checkpointPath, &checkpointBundle, &checkpointManifest,
	); err != nil {
		t.Fatalf("query checkpoint complete state: %v", err)
	}
	if generationStatus != "checkpointed" ||
		sessionStatus != string(sessionstate.Checkpointed) ||
		networkState != "reserved_checkpointed" ||
		resourceState != "reserved_checkpointed" ||
		checkpointPath == "" ||
		checkpointBundle != "bundle_digest" ||
		checkpointManifest != "projected_manifest_digest" {
		t.Fatalf("unexpected completed checkpoint state: generation=%s session=%s network=%s resource=%s path=%s bundle=%s manifest=%s",
			generationStatus, sessionStatus, networkState, resourceState, checkpointPath, checkpointBundle, checkpointManifest)
	}
}

func TestCompleteGenerationCheckpointRequiresRunscMetadata(t *testing.T) {
	ctx := context.Background()
	st, _ := openOwnedStore(t, ctx)
	params := CompleteCheckpointParams{
		SessionID:                       "sess_checkpoint_metadata",
		GenerationID:                    "gen_checkpoint_metadata",
		Owner:                           "owner",
		CheckpointPath:                  "/tmp/checkpoint",
		RunscPlatform:                   "systrap",
		RunscVersion:                    "runsc test",
		RunscBinaryPath:                 "/usr/local/bin/runsc-test",
		RunscBinaryDigest:               "sha256:runsc-test",
		CheckpointBundleDigest:          "bundle_digest",
		CheckpointRuntimeConfigDigest:   "runtime_config_digest",
		CheckpointControlManifestDigest: "manifest_digest",
		Now:                             time.Now().UTC(),
	}
	tests := []struct {
		name string
		want string
		edit func(*CompleteCheckpointParams)
	}{
		{name: "runsc version", want: "checkpoint runsc version is required", edit: func(p *CompleteCheckpointParams) { p.RunscVersion = "" }},
		{name: "runsc platform", want: "checkpoint runsc platform is required", edit: func(p *CompleteCheckpointParams) { p.RunscPlatform = "" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := params
			tt.edit(&p)
			err := st.CompleteGenerationCheckpoint(ctx, p)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("CompleteGenerationCheckpoint error=%v want %q", err, tt.want)
			}
		})
	}
}

func TestGenerationCheckpointAbortRestoresIdleState(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	now := time.Now().UTC()
	allocation := createAutoCheckpointGeneration(t, ctx, st, cfg, "sess_auto_abort", GenerationLeaseOwner(owner.UUID), now)

	if err := st.BeginGenerationCheckpoint(ctx, "sess_auto_abort", allocation.GenerationID, allocation.Owner, now.Add(2*time.Minute)); err != nil {
		t.Fatalf("begin checkpoint: %v", err)
	}
	if err := st.AbortGenerationCheckpoint(ctx, "sess_auto_abort", allocation.GenerationID, allocation.Owner, now.Add(3*time.Minute)); err != nil {
		t.Fatalf("abort checkpoint: %v", err)
	}
	var generationStatus, sessionStatus, networkState, resourceState string
	if err := st.db.QueryRowContext(ctx, `
SELECT g.status, s.status, n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN sessions s ON s.active_generation_id = g.generation_id
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&generationStatus, &sessionStatus, &networkState, &resourceState); err != nil {
		t.Fatalf("query aborted checkpoint state: %v", err)
	}
	if generationStatus != "idle" ||
		sessionStatus != string(sessionstate.RunningIdle) ||
		networkState != "live" ||
		resourceState != "live" {
		t.Fatalf("unexpected aborted checkpoint state: generation=%s session=%s network=%s resource=%s", generationStatus, sessionStatus, networkState, resourceState)
	}
}

func TestRetireExpiredCheckpointsClearsSessionAndMakesGenerationDestroyable(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	now := time.Now().UTC()
	allocation := createAutoCheckpointGeneration(t, ctx, st, cfg, "sess_retire_checkpoint", GenerationLeaseOwner(owner.UUID), now.Add(-48*time.Hour))
	checkpointedGeneration(t, ctx, st, "sess_retire_checkpoint", allocation.GenerationID, now.Add(-36*time.Hour))
	checkpointPath := filepath.Join(t.TempDir(), "checkpoint")
	if _, err := st.db.ExecContext(ctx, `
UPDATE sessions
SET checkpoint_path = ?,
    restore_ms = 123,
    last_activity_at = ?
WHERE id = ?`, checkpointPath, formatTime(now.Add(-30*time.Hour)), "sess_retire_checkpoint"); err != nil {
		t.Fatalf("seed session checkpoint metadata: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generation_resources
SET checkpoint_path = ?
WHERE generation_id = ?`, checkpointPath, allocation.GenerationID); err != nil {
		t.Fatalf("seed resource checkpoint path: %v", err)
	}

	normal := createAutoCheckpointGeneration(t, ctx, st, cfg, "sess_recent_failed", GenerationLeaseOwner(owner.UUID), now.Add(-30*time.Minute))
	if err := st.FailGeneration(ctx, FailGenerationParams{
		SessionID:    "sess_recent_failed",
		GenerationID: normal.GenerationID,
		Owner:        normal.Owner,
		ErrorClass:   "recent_failure",
		Reason:       "recent failure",
		Now:          now,
	}); err != nil {
		t.Fatalf("fail recent generation: %v", err)
	}

	retired, err := st.RetireExpiredCheckpoints(ctx, RetireExpiredCheckpointsParams{
		OwnerUUID:                owner.UUID,
		Now:                      now,
		CheckpointImageRetention: 24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("retire expired checkpoints: %v", err)
	}
	if len(retired) != 1 || retired[0].SessionID != "sess_retire_checkpoint" || retired[0].GenerationID != allocation.GenerationID || retired[0].EventID == 0 {
		t.Fatalf("unexpected retired checkpoints: %+v", retired)
	}

	var generationStatus, generationError, sessionStatus, networkState, resourceState string
	var sessionCheckpointPath, sessionRestoreMS sql.NullString
	if err := st.db.QueryRowContext(ctx, `
SELECT g.status, COALESCE(g.error_class, ''), s.status, s.checkpoint_path, s.restore_ms,
       n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN sessions s ON s.active_generation_id = g.generation_id
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(
		&generationStatus,
		&generationError,
		&sessionStatus,
		&sessionCheckpointPath,
		&sessionRestoreMS,
		&networkState,
		&resourceState,
	); err != nil {
		t.Fatalf("query retired checkpoint state: %v", err)
	}
	if generationStatus != "failed" ||
		generationError != "checkpoint_retired" ||
		sessionStatus != string(sessionstate.RunningIdle) ||
		sessionCheckpointPath.Valid ||
		sessionRestoreMS.Valid ||
		networkState != "reclaimable" ||
		resourceState != "reclaimable" {
		t.Fatalf("unexpected retired checkpoint state: generation=%s error=%s session=%s checkpoint_valid=%v restore_valid=%v network=%s resource=%s",
			generationStatus, generationError, sessionStatus, sessionCheckpointPath.Valid, sessionRestoreMS.Valid, networkState, resourceState)
	}
	var eventType, eventPayload string
	if err := st.db.QueryRowContext(ctx, `SELECT type, payload FROM events WHERE event_id = ?`, retired[0].EventID).Scan(&eventType, &eventPayload); err != nil {
		t.Fatalf("query retirement event: %v", err)
	}
	if eventType != "session.checkpoint_retired" ||
		!strings.Contains(eventPayload, `"restore_ms":null`) ||
		!strings.Contains(eventPayload, `"session_status":"running_idle"`) ||
		strings.Contains(eventPayload, `"checkpoint_path"`) {
		t.Fatalf("unexpected retirement event: type=%s payload=%s", eventType, eventPayload)
	}

	destroyable, err := st.ListDestroyableReclaimableGenerations(ctx, now, time.Hour)
	if err != nil {
		t.Fatalf("list destroyable generations: %v", err)
	}
	if !hasReclaimableGeneration(destroyable, "sess_retire_checkpoint", allocation.GenerationID) {
		t.Fatalf("checkpoint-retired generation should be immediately destroyable: %+v", destroyable)
	}
	if hasReclaimableGeneration(destroyable, "sess_recent_failed", normal.GenerationID) {
		t.Fatalf("recent ordinary failed generation should still honor failed_retention: %+v", destroyable)
	}
}

func TestRetireExpiredCheckpointsUsesCheckpointCreatedAtWhenSessionActivityIsMissing(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	now := time.Now().UTC()
	old := createAutoCheckpointGeneration(t, ctx, st, cfg, "sess_retire_null_activity", GenerationLeaseOwner(owner.UUID), now.Add(-3*time.Hour))
	checkpointedGeneration(t, ctx, st, "sess_retire_null_activity", old.GenerationID, now.Add(-2*time.Hour))
	young := createAutoCheckpointGeneration(t, ctx, st, cfg, "sess_keep_null_activity", GenerationLeaseOwner(owner.UUID), now.Add(-30*time.Minute))
	checkpointedGeneration(t, ctx, st, "sess_keep_null_activity", young.GenerationID, now.Add(-30*time.Minute))
	for _, sessionID := range []string{"sess_retire_null_activity", "sess_keep_null_activity"} {
		if _, err := st.db.ExecContext(ctx, `
UPDATE sessions
SET last_activity_at = NULL,
    checkpoint_path = ?
WHERE id = ?`, filepath.Join(t.TempDir(), sessionID, "checkpoint"), sessionID); err != nil {
			t.Fatalf("clear last activity for %s: %v", sessionID, err)
		}
	}
	if _, err := st.RetireExpiredCheckpoints(ctx, RetireExpiredCheckpointsParams{
		OwnerUUID:                "wrong-owner",
		Now:                      now,
		CheckpointImageRetention: time.Hour,
	}); err == nil {
		t.Fatalf("owner mismatch should reject checkpoint retirement")
	}
	retired, err := st.RetireExpiredCheckpoints(ctx, RetireExpiredCheckpointsParams{
		OwnerUUID:                owner.UUID,
		Now:                      now,
		CheckpointImageRetention: time.Hour,
	})
	if err != nil {
		t.Fatalf("retire expired checkpoints: %v", err)
	}
	if len(retired) != 1 || retired[0].SessionID != "sess_retire_null_activity" {
		t.Fatalf("unexpected checkpoint-created-at retirements: %+v", retired)
	}
	oldSession, err := st.GetSession(ctx, "sess_retire_null_activity")
	if err != nil {
		t.Fatalf("get old session: %v", err)
	}
	youngSession, err := st.GetSession(ctx, "sess_keep_null_activity")
	if err != nil {
		t.Fatalf("get young session: %v", err)
	}
	if oldSession.Status != string(sessionstate.RunningIdle) || youngSession.Status != string(sessionstate.Checkpointed) {
		t.Fatalf("unexpected checkpoint-created-at statuses: old=%s young=%s", oldSession.Status, youngSession.Status)
	}
}

func TestClaimCheckpointedGenerationForRestoreReturnsStaleAfterCheckpointRetirement(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	now := time.Now().UTC()
	leaseOwner := GenerationLeaseOwner(owner.UUID)
	allocation := createAutoCheckpointGeneration(t, ctx, st, cfg, "sess_restore_stale", leaseOwner, now.Add(-3*time.Hour))
	checkpointedGeneration(t, ctx, st, "sess_restore_stale", allocation.GenerationID, now.Add(-2*time.Hour))
	if _, err := st.db.ExecContext(ctx, `
UPDATE sessions
SET checkpoint_path = ?,
    last_activity_at = ?
WHERE id = ?`, filepath.Join(t.TempDir(), "checkpoint"), formatTime(now.Add(-2*time.Hour)), "sess_restore_stale"); err != nil {
		t.Fatalf("seed stale checkpoint metadata: %v", err)
	}
	retired, err := st.RetireExpiredCheckpoints(ctx, RetireExpiredCheckpointsParams{
		OwnerUUID:                owner.UUID,
		Now:                      now,
		CheckpointImageRetention: time.Hour,
	})
	if err != nil {
		t.Fatalf("retire checkpoint: %v", err)
	}
	if len(retired) != 1 {
		t.Fatalf("expected one retired checkpoint, got %+v", retired)
	}

	_, err = st.ClaimCheckpointedGenerationForRestore(ctx, ClaimCheckpointedGenerationParams{
		SessionID:    "sess_restore_stale",
		GenerationID: allocation.GenerationID,
		Owner:        leaseOwner,
		LeaseTTL:     time.Minute,
		Now:          now.Add(time.Second),
	})
	if !errors.Is(err, ErrStaleCheckpointRestore) {
		t.Fatalf("expected stale checkpoint restore error, got %v", err)
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
		DriverID:  "claude_code",
		Mode:      ModeForDriver("claude_code"),
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
		DriverID:  "claude_code",
		Mode:      ModeForDriver("claude_code"),
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

func TestSweepExpiredSessionsIgnoresNullExpiry(t *testing.T) {
	ctx := context.Background()
	st, _ := openOwnedStore(t, ctx)
	now := time.Now().UTC()
	if err := st.CreateSession(ctx, Session{
		ID:        "sess_no_expiry",
		UserID:    "lab",
		Status:    string(sessionstate.Created),
		DriverID:  "claude_code",
		Mode:      ModeForDriver("claude_code"),
		CreatedAt: now.Add(-time.Hour),
		UpdatedAt: now.Add(-time.Hour),
		ExpiresAt: nil,
	}); err != nil {
		t.Fatalf("create no-expiry session: %v", err)
	}

	changed, err := st.SweepExpiredSessions(ctx, now.Add(24*time.Hour))
	if err != nil {
		t.Fatalf("sweep expired sessions: %v", err)
	}
	if changed != 0 {
		t.Fatalf("expected no sessions swept, got %d", changed)
	}
	got, err := st.GetSession(ctx, "sess_no_expiry")
	if err != nil {
		t.Fatalf("get no-expiry session: %v", err)
	}
	if got.Status != string(sessionstate.Created) {
		t.Fatalf("expected session to remain created, got %s", got.Status)
	}
}

func TestClearActiveSessionExpiryClearsOnlyActiveSessions(t *testing.T) {
	ctx := context.Background()
	st, _ := openOwnedStore(t, ctx)
	now := time.Now().UTC()
	expiredAt := now.Add(-time.Hour)
	sessions := []Session{
		{
			ID:        "sess_active_retained_expiry",
			UserID:    "lab",
			Status:    string(sessionstate.RunningIdle),
			DriverID:  "claude_code",
			Mode:      ModeForDriver("claude_code"),
			CreatedAt: now.Add(-2 * time.Hour),
			UpdatedAt: now.Add(-2 * time.Hour),
			ExpiresAt: &expiredAt,
		},
		{
			ID:        "sess_failed_retained_expiry",
			UserID:    "lab",
			Status:    string(sessionstate.Failed),
			DriverID:  "claude_code",
			Mode:      ModeForDriver("claude_code"),
			CreatedAt: now.Add(-2 * time.Hour),
			UpdatedAt: now.Add(-2 * time.Hour),
			ExpiresAt: &expiredAt,
		},
		{
			ID:        "sess_destroyed_retained_expiry",
			UserID:    "lab",
			Status:    string(sessionstate.Destroyed),
			DriverID:  "claude_code",
			Mode:      ModeForDriver("claude_code"),
			CreatedAt: now.Add(-2 * time.Hour),
			UpdatedAt: now.Add(-2 * time.Hour),
			ExpiresAt: &expiredAt,
		},
	}
	for _, session := range sessions {
		if err := st.CreateSession(ctx, session); err != nil {
			t.Fatalf("create session %s: %v", session.ID, err)
		}
	}

	changed, err := st.ClearActiveSessionExpiry(ctx, now)
	if err != nil {
		t.Fatalf("clear active session expiry: %v", err)
	}
	if changed != 1 {
		t.Fatalf("expected one active session cleared, got %d", changed)
	}
	changed, err = st.SweepExpiredSessions(ctx, now.Add(24*time.Hour))
	if err != nil {
		t.Fatalf("sweep after clear: %v", err)
	}
	if changed != 0 {
		t.Fatalf("expected cleared active session to survive sweep, swept %d", changed)
	}

	var activeExpiry, failedExpiry, destroyedExpiry sql.NullString
	if err := st.db.QueryRowContext(ctx, `SELECT expires_at FROM sessions WHERE id = 'sess_active_retained_expiry'`).Scan(&activeExpiry); err != nil {
		t.Fatalf("query active expiry: %v", err)
	}
	if err := st.db.QueryRowContext(ctx, `SELECT expires_at FROM sessions WHERE id = 'sess_failed_retained_expiry'`).Scan(&failedExpiry); err != nil {
		t.Fatalf("query failed expiry: %v", err)
	}
	if err := st.db.QueryRowContext(ctx, `SELECT expires_at FROM sessions WHERE id = 'sess_destroyed_retained_expiry'`).Scan(&destroyedExpiry); err != nil {
		t.Fatalf("query destroyed expiry: %v", err)
	}
	if activeExpiry.Valid {
		t.Fatalf("expected active expiry cleared, got %s", activeExpiry.String)
	}
	if !failedExpiry.Valid || !destroyedExpiry.Valid {
		t.Fatalf("expected terminal expiries preserved, failed=%v destroyed=%v", failedExpiry.Valid, destroyedExpiry.Valid)
	}
}

func TestSweepExpiredSessionsCancelsUnstartedTurnsButPreservesAckStartedLease(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	now := time.Now().UTC()
	expiredAt := now.Add(-time.Second)

	if err := st.CreateSession(ctx, Session{
		ID:        "sess_expired_queued",
		UserID:    "lab",
		Status:    string(sessionstate.RunningIdle),
		DriverID:  "claude_code",
		Mode:      ModeForDriver("claude_code"),
		CreatedAt: now.Add(-time.Hour),
		UpdatedAt: now.Add(-time.Hour),
		ExpiresAt: &expiredAt,
	}); err != nil {
		t.Fatalf("create expired queued session: %v", err)
	}
	queuedTurnID, err := st.EnqueueTurn(ctx, "sess_expired_queued", "queued", now.Add(-time.Minute))
	if err != nil {
		t.Fatalf("enqueue queued turn: %v", err)
	}

	if err := st.CreateSession(ctx, Session{
		ID:        "sess_expired_ack",
		UserID:    "lab",
		Status:    string(sessionstate.RunningActive),
		DriverID:  "claude_code",
		Mode:      ModeForDriver("claude_code"),
		CreatedAt: now.Add(-time.Hour),
		UpdatedAt: now.Add(-time.Hour),
		ExpiresAt: &expiredAt,
	}); err != nil {
		t.Fatalf("create expired ack session: %v", err)
	}
	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_expired_ack",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now.Add(-30 * time.Second),
		Config:    testAllocatorConfig(t),
	})
	if err != nil {
		t.Fatalf("allocate ack generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, "sess_expired_ack", allocation.GenerationID, allocation.Owner, now.Add(-29*time.Second)); err != nil {
		t.Fatalf("mark ack resources live: %v", err)
	}
	createLiveRuntimeResourceInstanceForAllocation(t, ctx, st, "sess_expired_ack", allocation, owner.UUID, "host-expired-ack", now.Add(-28*time.Second))
	ackTurnID, err := st.EnqueueTurn(ctx, "sess_expired_ack", "started", now.Add(-28*time.Second))
	if err != nil {
		t.Fatalf("enqueue ack turn: %v", err)
	}
	if grant, ok, err := st.ClaimNextTurn(ctx, ClaimNextTurnParams{
		SessionID:    "sess_expired_ack",
		GenerationID: allocation.GenerationID,
		Owner:        allocation.Owner,
		RequestID:    "claim_expired_ack",
		LeaseTTL:     time.Minute,
		Now:          now.Add(-27 * time.Second),
	}); err != nil || !ok || grant.TurnID != ackTurnID {
		t.Fatalf("claim ack turn: ok=%v grant=%+v err=%v", ok, grant, err)
	}
	if _, err := st.AckTurnStarted(ctx, AckStartedParams{
		SessionID:       "sess_expired_ack",
		GenerationID:    allocation.GenerationID,
		TurnID:          ackTurnID,
		Owner:           allocation.Owner,
		SandboxSourceIP: sandboxSourceIPForGeneration(t, ctx, st, allocation.GenerationID),
		LeaseTTL:        time.Minute,
		Now:             now.Add(-26 * time.Second),
	}); err != nil {
		t.Fatalf("ack turn started: %v", err)
	}

	changed, err := st.SweepExpiredSessions(ctx, now)
	if err != nil {
		t.Fatalf("sweep expired sessions: %v", err)
	}
	if changed != 2 {
		t.Fatalf("expired sessions changed=%d want 2", changed)
	}

	var queuedStatus, queuedError, ackStatus, generationStatus, networkState, resourceState string
	if err := st.db.QueryRowContext(ctx, `
SELECT status, COALESCE(error_class, '')
FROM turns
WHERE id = ?`, queuedTurnID).Scan(&queuedStatus, &queuedError); err != nil {
		t.Fatalf("query queued turn: %v", err)
	}
	if err := st.db.QueryRowContext(ctx, `
SELECT t.status, g.status, n.allocation_state, r.resource_state
FROM turns t
JOIN runtime_generations g ON g.generation_id = t.generation_id
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE t.id = ?`, ackTurnID).Scan(&ackStatus, &generationStatus, &networkState, &resourceState); err != nil {
		t.Fatalf("query ack-started state: %v", err)
	}
	if queuedStatus != "canceled" || queuedError != "session_expired" {
		t.Fatalf("queued turn not canceled by TTL: status=%s error=%s", queuedStatus, queuedError)
	}
	if ackStatus != "running" || generationStatus != "active" || networkState != "live" || resourceState != "live" {
		t.Fatalf("ack-started lease should be preserved: turn=%s generation=%s network=%s resource=%s", ackStatus, generationStatus, networkState, resourceState)
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

func TestDestroySessionCancelsPendingTurnsAndReclaimsGeneration(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	sessionID := "sess_destroy_pending"
	createStoreSession(t, ctx, st, sessionID)
	now := time.Now().UTC()
	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: sessionID,
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    testAllocatorConfig(t),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, sessionID, allocation.GenerationID, allocation.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark generation live: %v", err)
	}
	enqueued, err := st.EnqueueTurnMessage(ctx, EnqueueTurnMessageParams{
		SessionID: sessionID,
		Content:   "hello",
		Now:       now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("enqueue turn: %v", err)
	}

	destroyedAt := now.Add(3 * time.Second)
	result, err := st.DestroySession(ctx, sessionID, destroyedAt)
	if err != nil {
		t.Fatalf("destroy session: %v", err)
	}
	if len(result.GenerationIDs) != 1 || result.GenerationIDs[0] != allocation.GenerationID || result.EventID == 0 {
		t.Fatalf("unexpected destroy session result: %+v", result)
	}

	var sessionStatus, turnStatus, turnErrorClass, generationStatus, generationErrorClass, networkState, resourceState string
	if err := st.db.QueryRowContext(ctx, `
SELECT s.status, t.status, COALESCE(t.error_class, ''), g.status, COALESCE(g.error_class, ''),
       n.allocation_state, r.resource_state
FROM sessions s
JOIN turns t ON t.session_id = s.id
JOIN runtime_generations g ON g.session_id = s.id
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE s.id = ?
  AND t.id = ?`, sessionID, enqueued.TurnID).Scan(
		&sessionStatus,
		&turnStatus,
		&turnErrorClass,
		&generationStatus,
		&generationErrorClass,
		&networkState,
		&resourceState,
	); err != nil {
		t.Fatalf("query destroyed state: %v", err)
	}
	if sessionStatus != string(sessionstate.Destroyed) ||
		turnStatus != "canceled" ||
		turnErrorClass != "session_destroyed" ||
		generationStatus != "failed" ||
		generationErrorClass != "session_destroyed" ||
		networkState != "reclaimable" ||
		resourceState != "reclaimable" {
		t.Fatalf("unexpected destroyed state: session=%s turn=%s turn_error=%s generation=%s generation_error=%s network=%s resource=%s",
			sessionStatus, turnStatus, turnErrorClass, generationStatus, generationErrorClass, networkState, resourceState)
	}
	var eventType, eventPayload string
	if err := st.db.QueryRowContext(ctx, `SELECT type, payload FROM events WHERE event_id = ?`, result.EventID).Scan(&eventType, &eventPayload); err != nil {
		t.Fatalf("query destroyed event: %v", err)
	}
	if eventType != "session.destroyed" || !strings.Contains(eventPayload, `"terminal":true`) {
		t.Fatalf("unexpected destroyed event: type=%s payload=%s", eventType, eventPayload)
	}
}

func TestCancelTerminalSessionPendingTurnsRepairsTerminalQueue(t *testing.T) {
	ctx := context.Background()
	st, _ := openOwnedStore(t, ctx)
	sessionID := "sess_terminal_queue"
	createStoreSession(t, ctx, st, sessionID)
	now := time.Now().UTC()
	enqueued, err := st.EnqueueTurnMessage(ctx, EnqueueTurnMessageParams{
		SessionID: sessionID,
		Content:   "hello",
		Now:       now,
	})
	if err != nil {
		t.Fatalf("enqueue turn: %v", err)
	}
	if err := st.UpdateSessionStatus(ctx, sessionID, string(sessionstate.Destroyed), nil); err != nil {
		t.Fatalf("destroy session: %v", err)
	}

	canceled, err := st.CancelTerminalSessionPendingTurns(ctx, now.Add(time.Second))
	if err != nil {
		t.Fatalf("cancel terminal pending turns: %v", err)
	}
	if canceled != 1 {
		t.Fatalf("canceled=%d want 1", canceled)
	}

	var status, errorClass, errText string
	if err := st.db.QueryRowContext(ctx, `
SELECT status, COALESCE(error_class, ''), COALESCE(error, '')
FROM turns
WHERE id = ?`, enqueued.TurnID).Scan(&status, &errorClass, &errText); err != nil {
		t.Fatalf("query turn: %v", err)
	}
	if status != "canceled" || errorClass != "session_destroyed" || errText != "session_destroyed" {
		t.Fatalf("unexpected repaired turn: status=%s error_class=%s error=%s", status, errorClass, errText)
	}
}

func checkpointedGeneration(t *testing.T, ctx context.Context, st *Store, sessionID, generationID string, now time.Time) {
	t.Helper()
	fence := checkpointDriverStateFenceForTest(t, ctx, st, sessionID, generationID)
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'checkpointed',
    checkpoint_created_at = ?,
    checkpoint_network_profile_id = network_profile_id,
    checkpoint_agent_runtime_profile_id = agent_runtime_profile_id,
    checkpoint_runsc_version = runsc_version,
    checkpoint_runsc_platform = runsc_platform,
    checkpoint_runsc_binary_path = (
      SELECT runsc_binary_path
      FROM runtime_generation_resources
      WHERE runtime_generation_resources.generation_id = runtime_generations.generation_id
    ),
    checkpoint_runsc_binary_digest = (
      SELECT runsc_binary_digest
      FROM runtime_generation_resources
      WHERE runtime_generation_resources.generation_id = runtime_generations.generation_id
    ),
    checkpoint_bundle_digest = 'bundle_digest',
    checkpoint_runtime_config_digest = 'runtime_config_digest',
    checkpoint_control_manifest_digest = (
      SELECT control_manifest_digest
      FROM runtime_generation_resources
      WHERE runtime_generation_resources.generation_id = runtime_generations.generation_id
    ),
    checkpoint_driver_states_digest = ?,
    lease_owner = NULL,
    lease_expires_at = NULL,
    last_seen_at = ?
WHERE generation_id = ?
  AND session_id = ?`, formatTime(now), fence, formatTime(now), generationID, sessionID); err != nil {
		t.Fatalf("set checkpointed generation: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `
UPDATE network_profiles
SET allocation_state = 'reserved_checkpointed'
WHERE generation_id = ?
  AND session_id = ?`, generationID, sessionID); err != nil {
		t.Fatalf("reserve checkpointed network: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generation_resources
SET resource_state = 'reserved_checkpointed'
WHERE generation_id = ?`, generationID); err != nil {
		t.Fatalf("reserve checkpointed resources: %v", err)
	}
	if err := st.UpdateSessionStatus(ctx, sessionID, string(sessionstate.Checkpointed), nil); err != nil {
		t.Fatalf("set checkpointed session: %v", err)
	}
}

func checkpointDriverStateFenceForTest(t *testing.T, ctx context.Context, st *Store, sessionID, generationID string) string {
	t.Helper()
	var driverID, stateDigest string
	var stateVersion int
	if err := st.db.QueryRowContext(ctx, `
SELECT ds.driver_id, ds.state_digest, ds.state_version
FROM session_driver_states ds
JOIN runtime_generations g ON g.session_id = ds.session_id
JOIN agent_runtime_profiles a ON a.agent_runtime_profile_id = g.agent_runtime_profile_id
WHERE g.session_id = ?
  AND g.generation_id = ?
  AND ds.driver_id = a.driver_id`, sessionID, generationID).Scan(&driverID, &stateDigest, &stateVersion); err != nil {
		t.Fatalf("query driver state fence input: %v", err)
	}
	fence, err := CheckpointDriverStatesDigest(generationID, []DriverStateToken{{
		DriverID:     driverID,
		StateDigest:  stateDigest,
		StateVersion: stateVersion,
	}})
	if err != nil {
		t.Fatalf("compute driver state fence: %v", err)
	}
	return fence
}

func hasReclaimableGeneration(generations []ReclaimableGeneration, sessionID, generationID string) bool {
	for _, generation := range generations {
		if generation.SessionID == sessionID && generation.GenerationID == generationID {
			return true
		}
	}
	return false
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

func recoverCleanedAllocations(t *testing.T, ctx context.Context, st *Store, p StartupRecoveryParams) (StartupRecoveryResult, error) {
	t.Helper()
	candidates, err := st.ListExpiredRuntimeRecoveryCandidates(ctx, p)
	if err != nil {
		return StartupRecoveryResult{}, err
	}
	return st.RepairExpiredRuntimeRecovery(ctx, p, candidates)
}

func createAutoCheckpointGeneration(t *testing.T, ctx context.Context, st *Store, cfg ResourceAllocatorConfig, sessionID, owner string, now time.Time) GenerationAllocation {
	t.Helper()
	if err := st.CreateSession(ctx, Session{
		ID:                    sessionID,
		UserID:                "lab",
		Status:                string(sessionstate.Created),
		DriverID:              "claude_code",
		Mode:                  ModeForDriver("claude_code"),
		AutoCheckpointEnabled: true,
		CreatedAt:             now.Add(-2 * time.Minute),
		UpdatedAt:             now.Add(-2 * time.Minute),
	}); err != nil {
		t.Fatalf("create session %s: %v", sessionID, err)
	}
	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: sessionID,
		Owner:     owner,
		LeaseTTL:  time.Hour,
		Now:       now.Add(-2 * time.Minute),
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate generation for %s: %v", sessionID, err)
	}
	if err := st.RecordGenerationRuntimeArtifactDigests(ctx, allocation.GenerationID, GenerationRuntimeArtifactDigests{
		ControlManifestDigest:          "manifest_digest",
		ProjectedControlManifestDigest: "projected_manifest_digest",
		BundleDigest:                   "bundle_digest",
		RuntimeConfigDigest:            "runtime_config_digest",
		SpecDigest:                     "spec_digest",
		RunscVersion:                   "runsc auto",
		RunscBinaryPath:                "/usr/local/bin/runsc-auto",
		RunscBinaryDigest:              "sha256:runsc-auto",
	}); err != nil {
		t.Fatalf("record artifacts for %s: %v", sessionID, err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, sessionID, allocation.GenerationID, allocation.Owner, now.Add(-time.Minute)); err != nil {
		t.Fatalf("mark generation live for %s: %v", sessionID, err)
	}
	createLiveRuntimeResourceInstanceForAllocation(t, ctx, st, sessionID, allocation, ownerUUIDFromLeaseOwner(owner), "host-auto-checkpoint", now.Add(-time.Minute+time.Second))
	if err := st.UpdateSessionStatusAndActivity(ctx, sessionID, string(sessionstate.RunningIdle), nil, now.Add(-2*time.Minute)); err != nil {
		t.Fatalf("mark session idle for %s: %v", sessionID, err)
	}
	return allocation
}

func createExpiredAckStartedTurn(t *testing.T, ctx context.Context, st *Store, ownerUUID string, cfg ResourceAllocatorConfig, sessionID string, now time.Time, expiredFor time.Duration) (GenerationAllocation, int64) {
	t.Helper()
	owner := GenerationLeaseOwner(ownerUUID)
	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: sessionID,
		Owner:     owner,
		LeaseTTL:  time.Minute,
		Now:       now.Add(-expiredFor - time.Minute),
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, sessionID, allocation.GenerationID, allocation.Owner, now.Add(-expiredFor-time.Minute+time.Second)); err != nil {
		t.Fatalf("mark resources live: %v", err)
	}
	createLiveRuntimeResourceInstanceForAllocation(t, ctx, st, sessionID, allocation, ownerUUID, "host-expired-"+sessionID, now.Add(-expiredFor-time.Minute+2*time.Second))
	turnID, err := st.EnqueueTurn(ctx, sessionID, "maybe already ran", now.Add(-expiredFor-time.Minute+2*time.Second))
	if err != nil {
		t.Fatalf("enqueue turn: %v", err)
	}
	claimAt := now.Add(-expiredFor - time.Minute + 3*time.Second)
	if grant, ok, err := st.ClaimNextTurn(ctx, ClaimNextTurnParams{
		SessionID:    sessionID,
		GenerationID: allocation.GenerationID,
		Owner:        allocation.Owner,
		RequestID:    "req_" + sessionID,
		LeaseTTL:     30 * time.Second,
		Now:          claimAt,
	}); err != nil || !ok || grant.TurnID != turnID {
		t.Fatalf("claim setup: ok=%v grant=%+v err=%v", ok, grant, err)
	}
	sandboxSourceIP := sandboxSourceIPForGeneration(t, ctx, st, allocation.GenerationID)
	if _, err := st.AckTurnStarted(ctx, AckStartedParams{
		SessionID:       sessionID,
		GenerationID:    allocation.GenerationID,
		TurnID:          turnID,
		Owner:           allocation.Owner,
		SandboxSourceIP: sandboxSourceIP,
		LeaseTTL:        30 * time.Second,
		Now:             claimAt.Add(time.Second),
	}); err != nil {
		t.Fatalf("ack started setup: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generations
SET lease_expires_at = ?
WHERE generation_id = ?`, formatTime(now.Add(-expiredFor)), allocation.GenerationID); err != nil {
		t.Fatalf("expire generation lease: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `
UPDATE turns
SET lease_expires_at = ?
WHERE id = ?`, formatTime(now.Add(-expiredFor)), turnID); err != nil {
		t.Fatalf("expire turn lease: %v", err)
	}
	return allocation, turnID
}

func createLiveRuntimeResourceInstanceForAllocation(t *testing.T, ctx context.Context, st *Store, sessionID string, allocation GenerationAllocation, ownerUUID, hostID string, now time.Time) RuntimeResourceInstance {
	t.Helper()
	contractID := "contract_" + allocation.GenerationID
	if _, err := st.StoreSandboxContract(ctx, StoreSandboxContractParams{
		ContractID:   contractID,
		SessionID:    sessionID,
		GenerationID: allocation.GenerationID,
		Payload:      testSandboxContractPayload(t, sessionID, allocation),
		Now:          now,
	}); err != nil {
		t.Fatalf("store sandbox contract: %v", err)
	}
	details, err := st.GetRuntimeGenerationDetails(ctx, sessionID, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get generation details: %v", err)
	}
	sandboxIP := sandboxIPFromCIDRForTest(t, details.SandboxIPCIDR)
	runscPath := filepath.Join(t.TempDir(), "runsc")
	instance, err := st.CreateRuntimeResourceInstance(ctx, RuntimeResourceInstanceParams{
		GenerationID:           allocation.GenerationID,
		SessionID:              sessionID,
		ContractID:             contractID,
		SandboxContractVersion: SandboxContractVersion,
		HostID:                 hostID,
		RunscContainerID:       details.RunscContainerID,
		RunscPlatform:          "systrap",
		RunscVersion:           "runsc test",
		RunscBinaryPath:        runscPath,
		RunscBinaryDigest:      "sha256:runsc",
		NetworkProfileID:       allocation.NetworkProfileID,
		NetnsName:              details.NetnsName,
		NetnsPath:              details.NetnsPath,
		HostVeth:               details.HostVeth,
		SandboxVeth:            details.SandboxVeth,
		HostGatewayIP:          details.HostGatewayIP,
		SandboxIP:              sandboxIP,
		SandboxIPCIDR:          details.SandboxIPCIDR,
		HostSideCIDR:           details.HostSideCIDR,
		NftTableName:           "harness_gen_" + strings.TrimPrefix(allocation.GenerationID, "gen_"),
		ControlDirPath:         details.ControlDirPath,
		ControlManifestPath:    details.ControlManifestPath,
		BundleDirPath:          details.BundleDirPath,
		SpecPath:               details.SpecPath,
		CheckpointPath:         details.CheckpointPath,
		BridgeDirPath:          details.BridgeDirPath,
		LogDirPath:             details.LogDirPath,
		RootPrefixes: map[string]string{
			"run_dir":      filepath.Dir(filepath.Dir(details.ControlDirPath)),
			"control_root": filepath.Dir(details.ControlDirPath),
			"bridge_root":  filepath.Dir(details.BridgeDirPath),
		},
		Now: now,
	})
	if err != nil {
		t.Fatalf("create runtime resource instance: %v", err)
	}
	workerID := strings.TrimSpace(ownerUUID)
	if workerID == "" {
		workerID = strings.TrimSuffix(strings.TrimSpace(allocation.Owner), ":"+RuntimeManagerRoleTag)
	}
	if err := st.ClaimRuntimeResourceMaterialization(ctx, RuntimeResourceMaterializationClaimParams{
		GenerationID:     allocation.GenerationID,
		WorkerID:         workerID,
		HostID:           hostID,
		LeaseExpiresAt:   now.Add(time.Minute),
		IdempotencyToken: "test:" + allocation.GenerationID,
		Now:              now.Add(time.Millisecond),
	}); err != nil {
		t.Fatalf("claim runtime resource materialization: %v", err)
	}
	if err := st.MarkRuntimeResourceReady(ctx, RuntimeResourceWorkerTransitionParams{
		GenerationID: allocation.GenerationID,
		WorkerID:     workerID,
		HostID:       hostID,
		Now:          now.Add(2 * time.Millisecond),
	}); err != nil {
		t.Fatalf("mark runtime resource ready: %v", err)
	}
	if err := st.MarkRuntimeResourceLive(ctx, RuntimeResourceWorkerTransitionParams{
		GenerationID: allocation.GenerationID,
		WorkerID:     workerID,
		HostID:       hostID,
		PostStart:    runtimeResourcePostStartProofForTest(instance),
		Now:          now.Add(3 * time.Millisecond),
	}); err != nil {
		t.Fatalf("mark runtime resource live: %v", err)
	}
	return instance
}

func testAllocatorConfig(t *testing.T) ResourceAllocatorConfig {
	t.Helper()
	modelAccessAllowed := true
	return ResourceAllocatorConfig{
		RunDir:                      filepath.Join(t.TempDir(), "run"),
		CIDRPool:                    netip.MustParsePrefix("10.240.0.0/29"),
		EgressDorisFEHosts:          []string{"172.16.0.138"},
		EgressDorisBEHosts:          []string{"172.16.0.139"},
		EgressDorisPorts:            []int{9030, 8040},
		EgressDNSPolicy:             "hostnames_only",
		HostProxyBindURL:            "http://0.0.0.0:8082",
		ProxyPort:                   8082,
		DriverID:                    "claude_code",
		Model:                       "sonnet",
		OutputFormat:                "stream-json",
		DisableNonessentialTraffic:  true,
		SandboxUID:                  7000,
		SandboxGID:                  7001,
		SandboxSupplementalGIDs:     []int{44, 43},
		ModelAccessAllowed:          &modelAccessAllowed,
		ProviderCredentialsHostOnly: true,
		SandboxModelProxyBaseURL:    "http://harness-model-proxy.internal:8082",
	}
}

func assertJSONStrings(t *testing.T, raw string, want []string) {
	t.Helper()
	var got []string
	if err := json.Unmarshal([]byte(raw), &got); err != nil {
		t.Fatalf("parse JSON string list %q: %v", raw, err)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("JSON string list = %#v, want %#v", got, want)
	}
}
