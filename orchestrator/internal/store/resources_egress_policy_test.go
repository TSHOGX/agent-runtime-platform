package store

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"
)

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
