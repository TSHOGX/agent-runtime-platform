package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/config"
	"harness-platform/orchestrator/internal/events"
	"harness-platform/orchestrator/internal/runtime"
	"harness-platform/orchestrator/internal/store"
)

func TestResolveDeploymentRequiresExplicitRuntimeProviderID(t *testing.T) {
	dir := t.TempDir()
	cfg := testServerConfig(dir)
	runtimeProvider := cfg.Harness.RuntimeProviders["local_runsc"]
	runtimeProvider.ProviderID = ""
	cfg.Harness.RuntimeProviders["local_runsc"] = runtimeProvider
	srv := &Server{cfg: cfg}

	if _, err := srv.resolveModeDeployment("agent"); err == nil || err.code != "provider_unsupported" {
		t.Fatalf("expected missing provider_id to make deployment unavailable, got %v", err)
	}
}

func TestDeploymentCapabilitiesAreProductSafe(t *testing.T) {
	dir := t.TempDir()
	srv := &Server{
		cfg: testServerConfig(dir),
	}
	req := httptest.NewRequest(http.MethodGet, "/api/deployment-capabilities", nil)
	rec := httptest.NewRecorder()
	srv.deploymentCapabilities(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	for _, forbidden := range []string{"driver_id", "claude_code", "provider_id", "local_runsc", "host_path", "agent_manifest_digest"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("capability response exposed %q: %s", forbidden, body)
		}
	}
	var payload struct {
		SchemaVersion int    `json:"schema_version"`
		DefaultMode   string `json:"default_mode"`
		SessionModes  []struct {
			Mode           string  `json:"mode"`
			Label          string  `json:"label"`
			Visible        bool    `json:"visible"`
			CreateEnabled  bool    `json:"create_enabled"`
			DisabledReason *string `json:"disabled_reason"`
		} `json:"session_modes"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode capabilities: %v", err)
	}
	if payload.SchemaVersion != 1 || payload.DefaultMode != "agent" || len(payload.SessionModes) != 2 {
		t.Fatalf("unexpected capabilities: %+v", payload)
	}
	for _, mode := range payload.SessionModes {
		if mode.Mode != "agent" && mode.Mode != "shell" {
			t.Fatalf("capabilities exposed non-product mode: %+v", mode)
		}
		if !mode.CreateEnabled || mode.DisabledReason != nil {
			t.Fatalf("expected test deployment mode enabled, got %+v", mode)
		}
	}
}

func TestDeploymentCapabilitiesUseImageManifestGate(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	rootfs := filepath.Join(dir, "rootfs")
	writeServerTestAgentImageManifest(t, rootfs, agents.ClaudeCode)
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	cfg := config.Config{
		SessionsRoot:     dir,
		SessionRetention: time.Hour,
		MaxSessions:      10,
		DefaultAgent:     "claude_code",
		RootFSPath:       rootfs,
	}
	applyServerTestDeploymentConfig(&cfg)
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: runtime.New(runtime.Config{}),
		watcher: newServerTestWatcher(t, dir, st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}

	req := httptest.NewRequest(http.MethodGet, "/api/deployment-capabilities", nil)
	rec := httptest.NewRecorder()
	srv.deploymentCapabilities(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var capabilities struct {
		SessionModes []struct {
			Mode           string  `json:"mode"`
			Visible        bool    `json:"visible"`
			CreateEnabled  bool    `json:"create_enabled"`
			DisabledReason *string `json:"disabled_reason"`
		} `json:"session_modes"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &capabilities); err != nil {
		t.Fatalf("decode capabilities: %v", err)
	}
	var sawAgent, sawShell bool
	for _, mode := range capabilities.SessionModes {
		switch mode.Mode {
		case "agent":
			sawAgent = true
			if !mode.Visible || !mode.CreateEnabled || mode.DisabledReason != nil {
				t.Fatalf("agent should remain available without sh in image: %+v", mode)
			}
		case "shell":
			sawShell = true
			if mode.Visible || mode.CreateEnabled || mode.DisabledReason == nil || *mode.DisabledReason != "missing_from_image" {
				t.Fatalf("shell should be hidden when sh is absent from image: %+v", mode)
			}
		}
	}
	if !sawAgent || !sawShell {
		t.Fatalf("expected agent and shell modes: %+v", capabilities.SessionModes)
	}

	shellReq := httptest.NewRequest(http.MethodPost, "/api/sessions", strings.NewReader(`{"mode":"shell"}`))
	shellRec := httptest.NewRecorder()
	srv.createSession(shellRec, shellReq)
	if shellRec.Code != http.StatusBadRequest || !strings.Contains(shellRec.Body.String(), "shell mode unavailable") {
		t.Fatalf("expected shell rejection before persistence, got status=%d body=%s", shellRec.Code, shellRec.Body.String())
	}
	var count int
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM sessions`).Scan(&count); err != nil {
		t.Fatalf("count sessions: %v", err)
	}
	if count != 0 {
		t.Fatalf("shell rejection should not persist a session, got %d", count)
	}

	agentReq := httptest.NewRequest(http.MethodPost, "/api/sessions", strings.NewReader(`{"mode":"agent"}`))
	agentRec := httptest.NewRecorder()
	srv.createSession(agentRec, agentReq)
	if agentRec.Code != http.StatusCreated {
		t.Fatalf("expected agent session creation, got status=%d body=%s", agentRec.Code, agentRec.Body.String())
	}
}

func TestCreateSessionFailsClosedWhenRequiredManifestMissing(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := &Server{
		cfg: config.Config{
			SessionsRoot:     dir,
			SessionRetention: time.Hour,
			MaxSessions:      10,
			DefaultAgent:     "claude_code",
			RootFSPath:       filepath.Join(dir, "missing-rootfs"),
		},
		store:   st,
		runtime: runtime.New(runtime.Config{}),
		watcher: newServerTestWatcher(t, dir, st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}

	req := httptest.NewRequest(http.MethodPost, "/api/sessions", strings.NewReader(`{"mode":"agent"}`))
	rec := httptest.NewRecorder()
	srv.createSession(rec, req)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "agent mode unavailable") {
		t.Fatalf("expected fail-closed agent rejection, got status=%d body=%s", rec.Code, rec.Body.String())
	}
	var count int
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM sessions`).Scan(&count); err != nil {
		t.Fatalf("count sessions: %v", err)
	}
	if count != 0 {
		t.Fatalf("manifest rejection should not persist a session, got %d", count)
	}
}

func TestDriverManifestInputDigestsUseSourceConfigAndImageManifest(t *testing.T) {
	dir := t.TempDir()
	rootfs := filepath.Join(dir, "rootfs")
	writeServerTestAgentImageManifest(t, rootfs, agents.ClaudeCode, agents.Shell)
	cfg := config.Config{
		DefaultAgent: "claude_code",
		RootFSPath:   rootfs,
	}
	applyServerTestDeploymentConfig(&cfg)
	srv := &Server{cfg: cfg}
	deployment, capabilityErr := srv.resolveModeDeployment("shell")
	if capabilityErr != nil {
		t.Fatalf("resolve shell deployment: %v", capabilityErr)
	}
	manifest, err := srv.loadAgentImageManifest()
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	digests, err := srv.driverManifestInputDigests(deployment)
	if err != nil {
		t.Fatalf("driver manifest input digests: %v", err)
	}
	if digests.AgentManifestDigest != manifest.Digest {
		t.Fatalf("agent manifest digest = %s want %s", digests.AgentManifestDigest, manifest.Digest)
	}
	want, err := runtimeConfigDigest(deployment.runtimeConfigPreimage("claude_code"))
	if err != nil {
		t.Fatalf("runtime config digest: %v", err)
	}
	if digests.RuntimeConfigDigest != want {
		t.Fatalf("runtime config digest = %s want %s", digests.RuntimeConfigDigest, want)
	}

	cfg.DefaultAgent = "pi"
	srv = &Server{cfg: cfg}
	deploymentWithPiDefault, capabilityErr := srv.resolveModeDeployment("shell")
	if capabilityErr != nil {
		t.Fatalf("resolve shell deployment with pi default: %v", capabilityErr)
	}
	changed, err := srv.driverManifestInputDigests(deploymentWithPiDefault)
	if err != nil {
		t.Fatalf("driver manifest input digests with pi default: %v", err)
	}
	if changed.RuntimeConfigDigest == digests.RuntimeConfigDigest {
		t.Fatalf("shell runtime config digest should change when deployment default agent changes")
	}
	if changed.AgentManifestDigest != digests.AgentManifestDigest {
		t.Fatalf("agent manifest digest should not change when manifest is unchanged: %s vs %s", changed.AgentManifestDigest, digests.AgentManifestDigest)
	}
}

func TestDriverManifestInputDigestsRequireExplicitDefaultAgent(t *testing.T) {
	dir := t.TempDir()
	cfg := testServerConfig(dir)
	srv := &Server{cfg: cfg}
	deployment, capabilityErr := srv.resolveModeDeployment("shell")
	if capabilityErr != nil {
		t.Fatalf("resolve shell deployment: %v", capabilityErr)
	}

	cfg.DefaultAgent = ""
	srv = &Server{cfg: cfg}
	if _, err := srv.driverManifestInputDigests(deployment); err == nil || !strings.Contains(err.Error(), "default agent is required") {
		t.Fatalf("expected missing default agent error, got %v", err)
	}

	cfg.DefaultAgent = "not-a-driver"
	srv = &Server{cfg: cfg}
	if _, err := srv.driverManifestInputDigests(deployment); err == nil || !strings.Contains(err.Error(), "default agent") {
		t.Fatalf("expected invalid default agent error, got %v", err)
	}
}

func TestDriverManifestHelpersFailClosedForUnknownDriver(t *testing.T) {
	unknown := agents.ID("opencode")
	if _, err := expectedDriverBinaryPath(unknown); err == nil || !strings.Contains(err.Error(), `unsupported driver "opencode"`) {
		t.Fatalf("expected unknown driver binary path error, got %v", err)
	}
}

func TestResourceAllocatorConfigUsesHostOnlyClaudeCredentials(t *testing.T) {
	cfg := testServerConfig(t.TempDir())
	cfg.ModelProxy.SandboxBaseURL = "http://harness-model-proxy.internal:8082"
	srv := &Server{cfg: cfg}

	claudeConfig := srv.resourceAllocatorConfig("claude_code")
	if !claudeConfig.ProviderCredentialsHostOnly {
		t.Fatalf("claude allocator should keep provider credentials host-only: %+v", claudeConfig)
	}
	if claudeConfig.SandboxModelProxyBaseURL != "http://harness-model-proxy.internal:8082" {
		t.Fatalf("claude sandbox model proxy base url = %q", claudeConfig.SandboxModelProxyBaseURL)
	}
	if claudeConfig.SandboxUID != cfg.Harness.SandboxIdentity.UID ||
		claudeConfig.SandboxGID != cfg.Harness.SandboxIdentity.GID {
		t.Fatalf("claude allocator sandbox identity = %+v", claudeConfig)
	}

	shellConfig := srv.resourceAllocatorConfig("sh")
	if shellConfig.ProviderCredentialsHostOnly || shellConfig.SandboxModelProxyBaseURL != "http://harness-model-proxy.internal:8082" {
		t.Fatalf("shell allocator should not request host-only provider credentials: %+v", shellConfig)
	}
}

func TestResourceAllocatorConfigUsesModelProxyPort(t *testing.T) {
	cfg := testServerConfig(t.TempDir())
	cfg.ModelProxy = config.ModelProxyConfig{
		BindURL:        "http://0.0.0.0:8083",
		SandboxBaseURL: "http://harness-model-proxy.internal:8083",
		BindPort:       8083,
	}
	srv := &Server{cfg: cfg}

	allocatorConfig := srv.resourceAllocatorConfig("claude_code")
	if allocatorConfig.HostProxyBindURL != "http://0.0.0.0:8083" ||
		allocatorConfig.ProxyPort != 8083 ||
		allocatorConfig.SandboxModelProxyBaseURL != "http://harness-model-proxy.internal:8083" {
		t.Fatalf("allocator model proxy config = %+v", allocatorConfig)
	}
}

func TestResourceAllocatorConfigUsesHarnessDeploymentConfig(t *testing.T) {
	cfg := testServerConfig(t.TempDir())
	enabled := true
	disableNonessentialTraffic := false
	cfg.Harness.Agents = map[string]config.AgentConfig{
		"claude_code": {
			Enabled:                    &enabled,
			DriverID:                   "claude_code",
			ModelProfile:               "custom_anthropic",
			RuntimeProvider:            "local_runsc",
			DisableNonessentialTraffic: &disableNonessentialTraffic,
		},
	}
	cfg.Harness.ModelProfiles = map[string]config.ModelProfileConfig{
		"custom_anthropic": {
			Enabled:  &enabled,
			Provider: "anthropic_messages",
			Model:    "opus",
			ProxyRef: config.DefaultModelProxyRef,
		},
	}
	srv := &Server{cfg: cfg}

	allocatorConfig := srv.resourceAllocatorConfig("claude_code")
	if allocatorConfig.Model != "opus" ||
		allocatorConfig.OutputFormat != "stream-json" ||
		allocatorConfig.DisableNonessentialTraffic ||
		!allocatorConfig.ProviderCredentialsHostOnly {
		t.Fatalf("allocator deployment config = %+v", allocatorConfig)
	}
}
