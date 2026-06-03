package store

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

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
