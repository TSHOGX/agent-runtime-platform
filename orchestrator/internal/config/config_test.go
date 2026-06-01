package config

import (
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadProjectConfigUsesHarnessSchema(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "harness.yaml")
	if err := os.WriteFile(path, []byte(`harness:
  run_dir: /tmp/harness-run
  session_retention: 3h
  max_sessions: 10
  model_proxy:
    bind_url: http://0.0.0.0:8083
    sandbox_base_url: http://harness-model-proxy.internal:8083
  sandbox_identity:
    uid: 7000
    gid: 7001
    supplemental_gids: [44, 43]
  proxy_service_identity:
    uid: 7100
    gid: 7101
  network:
    cidr_pool: 10.210.0.0/24
    egress:
      doris_fe_hosts: [172.16.0.138]
      doris_be_hosts: [172.16.0.139]
      doris_ports: [9030, 8040]
      dns_policy: hostnames_only
  events:
    retention_window: 12h
    retention_rows: 500
    emit_output_batch_max_rows: 16
    emit_output_batch_max_age: 250ms
  probe:
    accept_status:
      get_healthz: [200, 204]
      post_v1_messages:
        unauthorized: [401, 403]
        malformed_authenticated: [400, 422]
    pre_start_attempts: 2
    pre_start_interval: 200ms
    post_start_attempts: 4
    post_start_interval: 750ms
  bridge:
    lease_ttl: 45s
    heartbeat_interval: 20s
    poll_interval: 15ms
    ack_started_grace: 50s
    reconnect_grace: 25s
  checkpoint:
    auto_enabled: true
    idle_threshold: 7m
    monitor_interval: 11s
  reaper:
    failed_retention: 0s
    checkpoint_image_retention: 720h
`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := loadProjectConfig(path)
	if err != nil {
		t.Fatalf("load project config: %v", err)
	}

	harness := cfg.Harness
	if harness.RunDir != "/tmp/harness-run" {
		t.Fatalf("run_dir: %q", harness.RunDir)
	}
	if harness.ModelProxy.BindURL != "http://0.0.0.0:8083" ||
		harness.ModelProxy.BindPort != 8083 ||
		harness.ModelProxy.SandboxBaseURL != "http://harness-model-proxy.internal:8083" {
		t.Fatalf("unexpected model proxy config: %+v", harness.ModelProxy)
	}
	if harness.SessionRetention.Duration != 3*time.Hour || harness.MaxSessions != 10 {
		t.Fatalf("unexpected retention/max: %s %d", harness.SessionRetention.Duration, harness.MaxSessions)
	}
	if harness.SandboxIdentity.UID != 7000 ||
		harness.SandboxIdentity.GID != 7001 ||
		!sameInts(harness.SandboxIdentity.SupplementalGIDs, []int{43, 44}) {
		t.Fatalf("unexpected sandbox identity: %+v", harness.SandboxIdentity)
	}
	if harness.ProxyServiceIdentity.UID != 7100 || harness.ProxyServiceIdentity.GID != 7101 {
		t.Fatalf("unexpected proxy service identity: %+v", harness.ProxyServiceIdentity)
	}
	if got := harness.Network.CIDRPool.String(); got != "10.210.0.0/24" {
		t.Fatalf("cidr_pool: %q", got)
	}
	if !sameStrings(harness.Network.Egress.DorisFEHosts, []string{"172.16.0.138"}) ||
		!sameStrings(harness.Network.Egress.DorisBEHosts, []string{"172.16.0.139"}) ||
		!sameInts(harness.Network.Egress.DorisPorts, []int{9030, 8040}) {
		t.Fatalf("unexpected egress: %+v", harness.Network.Egress)
	}
	if harness.Network.Egress.DNSPolicy != DNSPolicyHostnamesOnly {
		t.Fatalf("dns policy: %q", harness.Network.Egress.DNSPolicy)
	}
	if harness.Events.RetentionRows != 500 || harness.Events.RetentionWindow.Duration != 12*time.Hour {
		t.Fatalf("unexpected events config: %+v", harness.Events)
	}
	if !sameInts(harness.Probe.AcceptStatus.GetHealthz, []int{200, 204}) ||
		!sameInts(harness.Probe.AcceptStatus.PostV1Messages.Unauthorized, []int{401, 403}) ||
		!sameInts(harness.Probe.AcceptStatus.PostV1Messages.MalformedAuthenticated, []int{400, 422}) {
		t.Fatalf("unexpected probe statuses: %+v", harness.Probe.AcceptStatus)
	}
	if harness.Bridge.HeartbeatInterval.Duration != 20*time.Second ||
		harness.Bridge.ReconnectGrace.Duration != 25*time.Second ||
		harness.Reaper.FailedRetention.Duration != 0 ||
		harness.Reaper.CheckpointImageRetention.Duration != 720*time.Hour {
		t.Fatalf("unexpected bridge/reaper config: bridge=%+v reaper=%+v", harness.Bridge, harness.Reaper)
	}
	if !harness.Checkpoint.AutoEnabled ||
		harness.Checkpoint.IdleThreshold.Duration != 7*time.Minute ||
		harness.Checkpoint.MonitorInterval.Duration != 11*time.Second {
		t.Fatalf("unexpected checkpoint config: %+v", harness.Checkpoint)
	}
	if agent := harness.Agents["claude_code"]; agent.DisableNonessentialTraffic == nil || !*agent.DisableNonessentialTraffic {
		t.Fatalf("expected default nonessential traffic setting to be true")
	}
}

func TestLoadProjectConfigDerivesModelProxySandboxBaseURLPort(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "harness.yaml")
	if err := os.WriteFile(path, []byte(`harness:
  model_proxy:
    bind_url: http://0.0.0.0:8083
`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := loadProjectConfig(path)
	if err != nil {
		t.Fatalf("load project config: %v", err)
	}
	if cfg.Harness.ModelProxy.BindPort != 8083 ||
		cfg.Harness.ModelProxy.SandboxBaseURL != "http://harness-model-proxy.internal:8083" {
		t.Fatalf("model proxy sandbox base URL was not derived from bind port: %+v", cfg.Harness.ModelProxy)
	}
}

func TestLoadProjectConfigUsesGenericDeploymentConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "harness.yaml")
	if err := os.WriteFile(path, []byte(`harness:
  default_agent: pi
  agents:
    claude_code:
      enabled: true
      driver_id: claude_code
      model_profile: anthropic_default
      runtime_provider: local_runsc
      disable_nonessential_traffic: false
    pi:
      enabled: true
      driver_id: pi
      model_profile: anthropic_default
      runtime_provider: local_runsc
      disable_nonessential_traffic: true
    sh:
      enabled: false
      driver_id: sh
      runtime_provider: local_runsc
  model_profiles:
    anthropic_default:
      enabled: true
      provider: anthropic_messages
      model: opus
      proxy_ref: model_proxy
  runtime_providers:
    local_runsc:
      enabled: true
      provider_id: local_runsc
      profile_id: local_runsc_default
`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := loadProjectConfig(path)
	if err != nil {
		t.Fatalf("load project config: %v", err)
	}
	if cfg.Harness.DefaultAgent != "pi" {
		t.Fatalf("default agent = %q", cfg.Harness.DefaultAgent)
	}
	if agent := cfg.Harness.Agents["pi"]; agent.DriverID != "pi" ||
		agent.ModelProfile != "anthropic_default" ||
		agent.RuntimeProvider != "local_runsc" ||
		agent.Enabled == nil || !*agent.Enabled {
		t.Fatalf("unexpected pi agent config: %+v", agent)
	}
	if profile := cfg.Harness.ModelProfiles["anthropic_default"]; profile.Model != "opus" ||
		profile.ProxyRef != "model_proxy" ||
		profile.Enabled == nil || !*profile.Enabled {
		t.Fatalf("unexpected model profile: %+v", profile)
	}
	if claude := cfg.Harness.Agents["claude_code"]; claude.DisableNonessentialTraffic == nil || *claude.DisableNonessentialTraffic {
		t.Fatalf("unexpected claude_code traffic config: %+v", claude)
	}
}

func TestLoadProjectConfigRejectsDisabledDefaultAgent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "harness.yaml")
	if err := os.WriteFile(path, []byte(`harness:
  default_agent: pi
  agents:
    pi:
      enabled: false
      driver_id: pi
      model_profile: anthropic_default
      runtime_provider: local_runsc
`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, err := loadProjectConfig(path)
	if err == nil || !strings.Contains(err.Error(), `default agent "pi" is not enabled`) {
		t.Fatalf("expected disabled default agent error, got %v", err)
	}
}

func TestLoadProjectConfigRejectsInvalidModelProxyBindURL(t *testing.T) {
	tests := []struct {
		name    string
		bindURL string
		want    string
	}{
		{
			name:    "missing port",
			bindURL: "http://0.0.0.0",
			want:    "must include an explicit port",
		},
		{
			name:    "invalid port",
			bindURL: "http://0.0.0.0:70000",
			want:    "invalid port",
		},
		{
			name:    "non http scheme",
			bindURL: "https://0.0.0.0:8082",
			want:    "must use http scheme",
		},
		{
			name:    "non local host",
			bindURL: "http://192.0.2.1:8082",
			want:    "host must be an unspecified address",
		},
		{
			name:    "loopback ipv4 host",
			bindURL: "http://127.0.0.1:8082",
			want:    "host must be an unspecified address",
		},
		{
			name:    "localhost",
			bindURL: "http://localhost:8082",
			want:    "host must be an unspecified address",
		},
		{
			name:    "loopback ipv6 host",
			bindURL: "http://[::1]:8082",
			want:    "host must be an unspecified address",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "harness.yaml")
			if err := os.WriteFile(path, []byte(`harness:
  model_proxy:
    bind_url: `+tt.bindURL+`
`), 0o644); err != nil {
				t.Fatalf("write config: %v", err)
			}

			_, err := loadProjectConfig(path)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected %q error, got %v", tt.want, err)
			}
		})
	}
}

func TestLoadProjectConfigRejectsMismatchedModelProxySandboxPort(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "harness.yaml")
	if err := os.WriteFile(path, []byte(`harness:
  model_proxy:
    bind_url: http://0.0.0.0:8083
    sandbox_base_url: http://harness-model-proxy.internal:8082
`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, err := loadProjectConfig(path)
	if err == nil || !strings.Contains(err.Error(), "sandbox_base_url port 8082 must match bind_url port 8083") {
		t.Fatalf("expected sandbox port mismatch rejection, got %v", err)
	}
}

func TestLoadProjectConfigRejectsMalformedModelProxySandboxBaseURL(t *testing.T) {
	tests := []struct {
		name           string
		sandboxBaseURL string
		want           string
	}{
		{
			name:           "non http scheme",
			sandboxBaseURL: "https://harness-model-proxy.internal:8082",
			want:           "sandbox_base_url must use http scheme",
		},
		{
			name:           "path",
			sandboxBaseURL: "http://harness-model-proxy.internal:8082/v1",
			want:           "sandbox_base_url must not include a path",
		},
		{
			name:           "userinfo",
			sandboxBaseURL: "http://user@harness-model-proxy.internal:8082",
			want:           "sandbox_base_url must not include userinfo, query, or fragment",
		},
		{
			name:           "query",
			sandboxBaseURL: "http://harness-model-proxy.internal:8082?target=v1",
			want:           "sandbox_base_url must not include userinfo, query, or fragment",
		},
		{
			name:           "fragment",
			sandboxBaseURL: "http://harness-model-proxy.internal:8082#v1",
			want:           "sandbox_base_url must not include userinfo, query, or fragment",
		},
		{
			name:           "missing host",
			sandboxBaseURL: "http://:8082",
			want:           "sandbox_base_url must include a host",
		},
		{
			name:           "missing port",
			sandboxBaseURL: "http://harness-model-proxy.internal",
			want:           "sandbox_base_url must include an explicit port matching bind_url",
		},
		{
			name:           "invalid port",
			sandboxBaseURL: "http://harness-model-proxy.internal:70000",
			want:           "sandbox_base_url contains invalid port",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "harness.yaml")
			if err := os.WriteFile(path, []byte(`harness:
  model_proxy:
    bind_url: http://0.0.0.0:8082
    sandbox_base_url: `+tt.sandboxBaseURL+`
`), 0o644); err != nil {
				t.Fatalf("write config: %v", err)
			}

			_, err := loadProjectConfig(path)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected %q error, got %v", tt.want, err)
			}
		})
	}
}

func TestLoadProjectConfigRejectsRemovedRuntimeSection(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "harness.yaml")
	if err := os.WriteFile(path, []byte(`harness:
  max_sessions: 10
runtime:
  runsc_network: sandbox
`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, err := loadProjectConfig(path)
	if err == nil || !strings.Contains(err.Error(), "field runtime not found") {
		t.Fatalf("expected removed runtime section rejection, got %v", err)
	}
}

func TestLoadProjectConfigRejectsUnknownKeys(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "harness.yaml")
	if err := os.WriteFile(path, []byte(`harness:
  session_retention: 1h
  surprise: true
`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, err := loadProjectConfig(path)
	if err == nil || !strings.Contains(err.Error(), "field surprise not found") {
		t.Fatalf("expected unknown field error, got %v", err)
	}
}

func TestLoadProjectConfigRejectsRemovedSessionTTLKey(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "harness.yaml")
	if err := os.WriteFile(path, []byte(`harness:
  session_ttl: 1h
`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, err := loadProjectConfig(path)
	if err == nil || !strings.Contains(err.Error(), "field session_ttl not found") {
		t.Fatalf("expected removed session_ttl error, got %v", err)
	}
}

func TestLoadProjectConfigRejectsRemovedSecretsConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "harness.yaml")
	if err := os.WriteFile(path, []byte(`harness:
  secrets:
    root: /var/lib/harness/secrets
    readers_gid: 65501
`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, err := loadProjectConfig(path)
	if err == nil || !strings.Contains(err.Error(), "field secrets not found") {
		t.Fatalf("expected removed secrets rejection, got %v", err)
	}
}

func TestValidateHarnessConfig(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*HarnessConfig)
		want   string
	}{
		{
			name: "run dir",
			mutate: func(cfg *HarnessConfig) {
				cfg.RunDir = ""
			},
			want: "harness.run_dir is required",
		},
		{
			name: "session retention",
			mutate: func(cfg *HarnessConfig) {
				cfg.SessionRetention.Duration = -time.Second
			},
			want: "harness.session_retention must be >= 0",
		},
		{
			name: "max sessions",
			mutate: func(cfg *HarnessConfig) {
				cfg.MaxSessions = 0
			},
			want: "harness.max_sessions must be > 0",
		},
		{
			name: "missing cidr",
			mutate: func(cfg *HarnessConfig) {
				cfg.Network.CIDRPool.Prefix = netip.Prefix{}
			},
			want: "harness.network.cidr_pool is required",
		},
		{
			name: "ipv6 cidr",
			mutate: func(cfg *HarnessConfig) {
				cfg.Network.CIDRPool.Prefix = netip.MustParsePrefix("fd00::/120")
			},
			want: "harness.network.cidr_pool must be IPv4",
		},
		{
			name: "too narrow cidr",
			mutate: func(cfg *HarnessConfig) {
				cfg.Network.CIDRPool.Prefix = netip.MustParsePrefix("10.0.0.0/31")
			},
			want: "harness.network.cidr_pool prefix length must be <= 30",
		},
		{
			name: "missing doris hosts",
			mutate: func(cfg *HarnessConfig) {
				cfg.Network.Egress.DorisFEHosts = nil
			},
			want: "doris_fe_hosts must be non-empty",
		},
		{
			name: "missing doris be hosts",
			mutate: func(cfg *HarnessConfig) {
				cfg.Network.Egress.DorisBEHosts = nil
			},
			want: "doris_be_hosts must be non-empty",
		},
		{
			name: "missing doris ports",
			mutate: func(cfg *HarnessConfig) {
				cfg.Network.Egress.DorisPorts = nil
			},
			want: "doris_ports must be non-empty",
		},
		{
			name: "invalid doris port",
			mutate: func(cfg *HarnessConfig) {
				cfg.Network.Egress.DorisPorts = []int{0}
			},
			want: "doris_ports contains invalid port 0",
		},
		{
			name: "hostname needs dns",
			mutate: func(cfg *HarnessConfig) {
				cfg.Network.Egress.DorisFEHosts = []string{"doris-fe.local"}
				cfg.Network.Egress.DNSPolicy = DNSPolicyOff
			},
			want: "dns_policy must not be off",
		},
		{
			name: "probe statuses",
			mutate: func(cfg *HarnessConfig) {
				cfg.Probe.AcceptStatus.GetHealthz = nil
			},
			want: "get_healthz must be non-empty",
		},
		{
			name: "missing unauthorized probe statuses",
			mutate: func(cfg *HarnessConfig) {
				cfg.Probe.AcceptStatus.PostV1Messages.Unauthorized = nil
			},
			want: "post_v1_messages.unauthorized must be non-empty",
		},
		{
			name: "missing malformed probe statuses",
			mutate: func(cfg *HarnessConfig) {
				cfg.Probe.AcceptStatus.PostV1Messages.MalformedAuthenticated = nil
			},
			want: "post_v1_messages.malformed_authenticated must be non-empty",
		},
		{
			name: "pre start attempts",
			mutate: func(cfg *HarnessConfig) {
				cfg.Probe.PreStartAttempts = 0
			},
			want: "pre_start_attempts must be > 0",
		},
		{
			name: "pre start interval",
			mutate: func(cfg *HarnessConfig) {
				cfg.Probe.PreStartInterval.Duration = 0
			},
			want: "pre_start_interval must be > 0",
		},
		{
			name: "post start attempts",
			mutate: func(cfg *HarnessConfig) {
				cfg.Probe.PostStartAttempts = 0
			},
			want: "post_start_attempts must be > 0",
		},
		{
			name: "post start interval",
			mutate: func(cfg *HarnessConfig) {
				cfg.Probe.PostStartInterval.Duration = 0
			},
			want: "post_start_interval must be > 0",
		},
		{
			name: "lease ttl",
			mutate: func(cfg *HarnessConfig) {
				cfg.Bridge.LeaseTTL.Duration = 0
			},
			want: "lease_ttl must be > 0",
		},
		{
			name: "heartbeat lease",
			mutate: func(cfg *HarnessConfig) {
				cfg.Bridge.LeaseTTL.Duration = 30 * time.Second
				cfg.Bridge.HeartbeatInterval.Duration = 30 * time.Second
			},
			want: "heartbeat_interval must be > 0 and <",
		},
		{
			name: "poll interval",
			mutate: func(cfg *HarnessConfig) {
				cfg.Bridge.PollInterval.Duration = 0
			},
			want: "poll_interval must be > 0",
		},
		{
			name: "ack grace positive",
			mutate: func(cfg *HarnessConfig) {
				cfg.Bridge.AckStartedGrace.Duration = 0
			},
			want: "ack_started_grace must be > 0",
		},
		{
			name: "reconnect grace",
			mutate: func(cfg *HarnessConfig) {
				cfg.Bridge.ReconnectGrace.Duration = -time.Second
			},
			want: "reconnect_grace must be >= 0",
		},
		{
			name: "ack reconnect grace",
			mutate: func(cfg *HarnessConfig) {
				cfg.Bridge.AckStartedGrace.Duration = 10 * time.Second
				cfg.Bridge.ReconnectGrace.Duration = 20 * time.Second
			},
			want: "ack_started_grace must be >=",
		},
		{
			name: "events bounds",
			mutate: func(cfg *HarnessConfig) {
				cfg.Events.RetentionWindow.Duration = 0
				cfg.Events.RetentionRows = 0
			},
			want: "cannot both be zero",
		},
		{
			name: "negative event retention window",
			mutate: func(cfg *HarnessConfig) {
				cfg.Events.RetentionWindow.Duration = -time.Second
			},
			want: "retention_window must be >= 0",
		},
		{
			name: "negative event retention rows",
			mutate: func(cfg *HarnessConfig) {
				cfg.Events.RetentionRows = -1
			},
			want: "retention_rows must be >= 0",
		},
		{
			name: "emit output batch rows",
			mutate: func(cfg *HarnessConfig) {
				cfg.Events.EmitOutputBatchMaxRows = 0
			},
			want: "emit_output_batch_max_rows must be > 0",
		},
		{
			name: "emit output batch age",
			mutate: func(cfg *HarnessConfig) {
				cfg.Events.EmitOutputBatchMaxAge.Duration = 0
			},
			want: "emit_output_batch_max_age must be > 0",
		},
		{
			name: "checkpoint monitor",
			mutate: func(cfg *HarnessConfig) {
				cfg.Checkpoint.MonitorInterval.Duration = 0
			},
			want: "monitor_interval must be > 0",
		},
		{
			name: "checkpoint idle threshold",
			mutate: func(cfg *HarnessConfig) {
				cfg.Checkpoint.IdleThreshold.Duration = -time.Second
			},
			want: "idle_threshold must be >= 0",
		},
		{
			name: "negative reaper",
			mutate: func(cfg *HarnessConfig) {
				cfg.Reaper.FailedRetention.Duration = -time.Second
			},
			want: "failed_retention must be >= 0",
		},
		{
			name: "zero checkpoint image retention",
			mutate: func(cfg *HarnessConfig) {
				cfg.Reaper.CheckpointImageRetention.Duration = 0
			},
			want: "",
		},
		{
			name: "negative checkpoint image retention",
			mutate: func(cfg *HarnessConfig) {
				cfg.Reaper.CheckpointImageRetention.Duration = -time.Second
			},
			want: "checkpoint_image_retention must be >= 0",
		},
		{
			name: "sandbox uid",
			mutate: func(cfg *HarnessConfig) {
				cfg.SandboxIdentity.UID = 0
			},
			want: "sandbox_identity.uid must be > 0",
		},
		{
			name: "sandbox gid",
			mutate: func(cfg *HarnessConfig) {
				cfg.SandboxIdentity.GID = 0
			},
			want: "sandbox_identity.gid must be > 0",
		},
		{
			name: "sandbox supplemental root gid",
			mutate: func(cfg *HarnessConfig) {
				cfg.SandboxIdentity.SupplementalGIDs = []int{0}
			},
			want: "supplemental_gids must contain only positive non-root gids",
		},
		{
			name: "sandbox supplemental duplicate",
			mutate: func(cfg *HarnessConfig) {
				cfg.SandboxIdentity.SupplementalGIDs = []int{1234, 1234}
			},
			want: "supplemental_gids contains duplicate gid 1234",
		},
		{
			name: "missing model proxy bind port",
			mutate: func(cfg *HarnessConfig) {
				cfg.ModelProxy.BindURL = "http://0.0.0.0"
			},
			want: "model_proxy.bind_url must include an explicit port",
		},
		{
			name: "invalid model proxy bind port",
			mutate: func(cfg *HarnessConfig) {
				cfg.ModelProxy.BindURL = "http://0.0.0.0:70000"
			},
			want: "model_proxy.bind_url contains invalid port",
		},
		{
			name: "model proxy bind scheme",
			mutate: func(cfg *HarnessConfig) {
				cfg.ModelProxy.BindURL = "https://0.0.0.0:8082"
			},
			want: "model_proxy.bind_url must use http scheme",
		},
		{
			name: "model proxy bind host",
			mutate: func(cfg *HarnessConfig) {
				cfg.ModelProxy.BindURL = "http://192.0.2.1:8082"
			},
			want: "model_proxy.bind_url host must be an unspecified address",
		},
		{
			name: "model proxy loopback bind host",
			mutate: func(cfg *HarnessConfig) {
				cfg.ModelProxy.BindURL = "http://127.0.0.1:8082"
			},
			want: "model_proxy.bind_url host must be an unspecified address",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := defaultHarnessConfig()
			tt.mutate(&cfg)

			err := validateHarnessConfig(cfg)
			if tt.want == "" {
				if err != nil {
					t.Fatalf("expected valid config, got %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected %q error, got %v", tt.want, err)
			}
		})
	}
}

func TestNormalizeSandboxIdentitySortsSupplementalGIDs(t *testing.T) {
	got := NormalizeSandboxIdentity(SandboxIdentity{
		UID:              7000,
		GID:              7001,
		SupplementalGIDs: []int{9, 7, 8},
	})
	if !sameInts(got.SupplementalGIDs, []int{7, 8, 9}) {
		t.Fatalf("supplemental gids not sorted: %+v", got)
	}
}

func TestValidateHarnessConfigAllowsZeroSessionRetention(t *testing.T) {
	cfg := defaultHarnessConfig()
	cfg.SessionRetention.Duration = 0

	if err := validateHarnessConfig(cfg); err != nil {
		t.Fatalf("zero session retention should be valid: %v", err)
	}
}

func TestValidateIsolationRootsAllowsReservedSubroots(t *testing.T) {
	roots := isolationRootsForTest(t)

	canonical, err := ValidateIsolationRoots(roots)
	if err != nil {
		t.Fatalf("validate isolation roots: %v", err)
	}
	if canonical.DataVolumeEvidenceRoot != filepath.Clean(roots.DataVolumeEvidenceRoot) ||
		canonical.ProxyInternalRoot != filepath.Clean(roots.ProxyInternalRoot) ||
		canonical.DBStateRoot != filepath.Dir(filepath.Clean(roots.DBPath)) {
		t.Fatalf("unexpected canonical roots: %+v", canonical)
	}
}

func TestValidateIsolationRootsRejectsDBUnderSandboxRoot(t *testing.T) {
	roots := isolationRootsForTest(t)
	roots.DBPath = filepath.Join(roots.SessionsRoot, "orchestrator.db")

	_, err := ValidateIsolationRoots(roots)
	if err == nil || !strings.Contains(err.Error(), "overlaps") {
		t.Fatalf("expected overlapping db root rejection, got %v", err)
	}
}

func TestValidateIsolationRootsRejectsProxyInternalUnderControlRoot(t *testing.T) {
	roots := isolationRootsForTest(t)
	roots.ProxyInternalRoot = filepath.Join(roots.RunDir, "control", "proxy-internal")

	_, err := ValidateIsolationRoots(roots)
	if err == nil || !strings.Contains(err.Error(), "overlaps sandbox-bindable run control root") {
		t.Fatalf("expected proxy internal overlap rejection, got %v", err)
	}
}

func TestValidateIsolationRootsRejectsProxyInternalUnderLogsRoot(t *testing.T) {
	roots := isolationRootsForTest(t)
	roots.ProxyInternalRoot = filepath.Join(roots.RunDir, "logs", "proxy-internal")

	_, err := ValidateIsolationRoots(roots)
	if err == nil || !strings.Contains(err.Error(), "overlaps sandbox-bindable run logs root") {
		t.Fatalf("expected proxy internal overlap rejection, got %v", err)
	}
}

func TestValidateIsolationRootsRejectsEvidenceUnderSandboxRoot(t *testing.T) {
	roots := isolationRootsForTest(t)
	roots.DataVolumeEvidenceRoot = filepath.Join(roots.AgentHomesRoot, "evidence")

	_, err := ValidateIsolationRoots(roots)
	if err == nil || !strings.Contains(err.Error(), "overlaps") {
		t.Fatalf("expected evidence overlap rejection, got %v", err)
	}
}

func TestValidateIsolationRootsRejectsRelativeRoot(t *testing.T) {
	roots := isolationRootsForTest(t)
	roots.RootFSPath = "relative/rootfs"

	_, err := ValidateIsolationRoots(roots)
	if err == nil || !strings.Contains(err.Error(), "must be absolute") {
		t.Fatalf("expected absolute path rejection, got %v", err)
	}
}

func TestValidateHarnessConfigAllowsMaxSessionsAboveCIDRCapacity(t *testing.T) {
	cfg := defaultHarnessConfig()
	cfg.MaxSessions = 10
	cfg.Network.CIDRPool.Prefix = netip.MustParsePrefix("10.0.0.0/30")

	if err := validateHarnessConfig(cfg); err != nil {
		t.Fatalf("max_sessions should be independent from /30 capacity: %v", err)
	}
}

func TestSessionRetentionEnvRejectsRemovedSessionTTL(t *testing.T) {
	unsetEnvForTest(t, "HARNESS_SESSION_RETENTION")
	t.Setenv("HARNESS_SESSION_TTL", "2h")

	_, err := sessionRetentionEnv(time.Hour)
	if err == nil || !strings.Contains(err.Error(), "HARNESS_SESSION_TTL has been removed; use HARNESS_SESSION_RETENTION") {
		t.Fatalf("expected removed env error, got %v", err)
	}
}

func TestSessionRetentionEnvRejectsRemovedSessionTTLEvenWithReplacement(t *testing.T) {
	t.Setenv("HARNESS_SESSION_TTL", "2h")
	t.Setenv("HARNESS_SESSION_RETENTION", "720h")

	_, err := sessionRetentionEnv(time.Hour)
	if err == nil || !strings.Contains(err.Error(), "HARNESS_SESSION_TTL has been removed") {
		t.Fatalf("expected removed env error, got %v", err)
	}
}

func TestSessionRetentionEnvStrictParsing(t *testing.T) {
	unsetEnvForTest(t, "HARNESS_SESSION_TTL")
	tests := []struct {
		name  string
		value string
		want  time.Duration
		err   string
	}{
		{name: "zero", value: "0s", want: 0},
		{name: "normal", value: "720h", want: 720 * time.Hour},
		{name: "days rejected", value: "30d", err: "invalid HARNESS_SESSION_RETENTION duration"},
		{name: "typo rejected", value: "forever", err: "invalid HARNESS_SESSION_RETENTION duration"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("HARNESS_SESSION_RETENTION", tt.value)
			got, err := sessionRetentionEnv(time.Hour)
			if tt.err != "" {
				if err == nil || !strings.Contains(err.Error(), tt.err) {
					t.Fatalf("expected %q error, got %v", tt.err, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("session retention env: %v", err)
			}
			if got != tt.want {
				t.Fatalf("retention=%s want %s", got, tt.want)
			}
		})
	}
}

func TestLoadRejectsRemovedSessionTTLEnv(t *testing.T) {
	repo := writeMinimalLoadConfig(t)
	chdirForLoadTest(t, repo)
	t.Setenv("HARNESS_SESSION_TTL", "2h")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "HARNESS_SESSION_TTL has been removed; use HARNESS_SESSION_RETENTION") {
		t.Fatalf("expected removed env load error, got %v", err)
	}
}

func TestLoadRejectsInvalidSessionRetentionEnv(t *testing.T) {
	repo := writeMinimalLoadConfig(t)
	chdirForLoadTest(t, repo)
	unsetEnvForTest(t, "HARNESS_SESSION_TTL")
	t.Setenv("HARNESS_SESSION_RETENTION", "30d")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "invalid HARNESS_SESSION_RETENTION duration") {
		t.Fatalf("expected invalid retention env load error, got %v", err)
	}
}

func TestLoadExposesModelProxyConfig(t *testing.T) {
	repo := t.TempDir()
	configDir := filepath.Join(repo, "config")
	if err := os.Mkdir(configDir, 0o755); err != nil {
		t.Fatalf("mkdir config dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(configDir, "harness.yaml"), []byte(`harness:
  model_proxy:
    bind_url: http://0.0.0.0:8083
    sandbox_base_url: http://harness-model-proxy.internal:8083
`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	chdirForLoadTest(t, repo)
	unsetEnvForTest(t, "HARNESS_SESSION_TTL")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.ModelProxy.BindURL != "http://0.0.0.0:8083" ||
		cfg.ModelProxy.BindPort != 8083 ||
		cfg.ModelProxy.SandboxBaseURL != "http://harness-model-proxy.internal:8083" {
		t.Fatalf("unexpected exposed model proxy config: %+v", cfg.ModelProxy)
	}
}

func TestLoadRejectsShellDefaultAgent(t *testing.T) {
	repo := writeMinimalLoadConfig(t)
	chdirForLoadTest(t, repo)
	unsetEnvForTest(t, "HARNESS_SESSION_TTL")
	t.Setenv("HARNESS_DEFAULT_AGENT", "sh")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "default agent must be an agent-capable driver") {
		t.Fatalf("expected default agent validation error, got %v", err)
	}
}

func TestLoadDefaultDBPathIsOutsideSessionsRoot(t *testing.T) {
	repo := writeMinimalLoadConfig(t)
	chdirForLoadTest(t, repo)
	t.Setenv("HARNESS_SESSIONS_ROOT", filepath.Join(t.TempDir(), "sessions"))
	t.Setenv("HARNESS_DB_PATH", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if isolationPathWithin(cfg.DBPath, cfg.SessionsRoot) {
		t.Fatalf("default DB path %q must not be under sessions root %q", cfg.DBPath, cfg.SessionsRoot)
	}
	if _, err := ValidateIsolationRoots(cfg.IsolationRoots()); err != nil {
		t.Fatalf("default roots should satisfy isolation validation: %v", err)
	}
}

func TestLoadProjectConfigRejectsMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.yaml")
	_, err := loadProjectConfig(path)
	if err == nil || !strings.Contains(err.Error(), "config file is required") {
		t.Fatalf("expected missing config rejection, got %v", err)
	}
}

func TestLoadProjectConfigRejectsEmptyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "harness.yaml")
	if err := os.WriteFile(path, []byte(" \n\t\n"), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, err := loadProjectConfig(path)
	if err == nil || !strings.Contains(err.Error(), "config file is empty") {
		t.Fatalf("expected empty config rejection, got %v", err)
	}
}

func writeMinimalLoadConfig(t *testing.T) string {
	t.Helper()
	repo := t.TempDir()
	configDir := filepath.Join(repo, "config")
	if err := os.Mkdir(configDir, 0o755); err != nil {
		t.Fatalf("mkdir config dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(configDir, "harness.yaml"), []byte(`harness:
  max_sessions: 30
`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return repo
}

func chdirForLoadTest(t *testing.T, repo string) {
	t.Helper()
	oldWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(repo); err != nil {
		t.Fatalf("chdir repo: %v", err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(oldWD); err != nil {
			t.Fatalf("restore wd: %v", err)
		}
	})
}

func unsetEnvForTest(t *testing.T, key string) {
	t.Helper()
	old, ok := os.LookupEnv(key)
	if err := os.Unsetenv(key); err != nil {
		t.Fatalf("unset %s: %v", key, err)
	}
	t.Cleanup(func() {
		if ok {
			_ = os.Setenv(key, old)
		} else {
			_ = os.Unsetenv(key)
		}
	})
}

func isolationRootsForTest(t *testing.T) IsolationRoots {
	t.Helper()
	base := t.TempDir()
	roots := IsolationRoots{
		SessionsRoot:           filepath.Join(base, "sessions"),
		AgentHomesRoot:         filepath.Join(base, "agent-homes"),
		RunDir:                 filepath.Join(base, "run"),
		PreparedBundleRoot:     filepath.Join(base, "prepared-bundles"),
		RootFSPath:             filepath.Join(base, "rootfs"),
		DBPath:                 filepath.Join(base, "state", "orchestrator.db"),
		SchemaPackRoot:         filepath.Join(base, "schema-pack"),
		DataVolumeEvidenceRoot: filepath.Join(base, "state", "volume-evidence"),
		ProxyInternalRoot:      filepath.Join(base, "run", "proxy-internal"),
	}
	for _, path := range []string{
		roots.SessionsRoot,
		roots.AgentHomesRoot,
		roots.RunDir,
		roots.PreparedBundleRoot,
		roots.RootFSPath,
		filepath.Dir(roots.DBPath),
		roots.SchemaPackRoot,
	} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatalf("mkdir isolation test root %s: %v", path, err)
		}
	}
	return roots
}

func TestCheckedInHarnessConfigLoads(t *testing.T) {
	cfg, err := loadProjectConfig(filepath.Join("..", "..", "..", "config", "harness.yaml"))
	if err != nil {
		t.Fatalf("load checked-in harness config: %v", err)
	}
	if err := validateHarnessConfig(cfg.Harness); err != nil {
		t.Fatalf("validate checked-in harness config: %v", err)
	}
}

func TestLoadValidatesMergedHarnessConfig(t *testing.T) {
	repo := t.TempDir()
	configDir := filepath.Join(repo, "config")
	if err := os.Mkdir(configDir, 0o755); err != nil {
		t.Fatalf("mkdir config dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(configDir, "harness.yaml"), []byte(`harness:
  network:
    cidr_pool: 10.0.0.0/30
`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	oldWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(repo); err != nil {
		t.Fatalf("chdir repo: %v", err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(oldWD); err != nil {
			t.Fatalf("restore wd: %v", err)
		}
	})
	unsetEnvForTest(t, "HARNESS_SESSION_TTL")
	t.Setenv("HARNESS_SESSION_RETENTION", "-1s")

	_, err = Load()
	if err == nil || !strings.Contains(err.Error(), "harness.session_retention must be >= 0") {
		t.Fatalf("expected merged validation error, got %v", err)
	}
}

func TestLoadAutoCheckpointEnvOverride(t *testing.T) {
	repo := writeMinimalLoadConfig(t)
	chdirForLoadTest(t, repo)
	t.Setenv("HARNESS_AUTO_CHECKPOINT_ENABLED", "true")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if !cfg.Harness.Checkpoint.AutoEnabled {
		t.Fatalf("expected env override to enable checkpoint policy: %+v", cfg.Harness.Checkpoint)
	}
}

func TestLoadRejectsInvalidMaxSessionsEnv(t *testing.T) {
	tests := []string{"many", "0", "-1"}

	for _, value := range tests {
		t.Run(value, func(t *testing.T) {
			repo := writeMinimalLoadConfig(t)
			chdirForLoadTest(t, repo)
			t.Setenv("HARNESS_MAX_SESSIONS", value)

			_, err := Load()
			if err == nil || !strings.Contains(err.Error(), "invalid HARNESS_MAX_SESSIONS") {
				t.Fatalf("expected invalid max sessions env error, got %v", err)
			}
		})
	}
}

func TestLoadRejectsInvalidAutoCheckpointEnv(t *testing.T) {
	repo := writeMinimalLoadConfig(t)
	chdirForLoadTest(t, repo)
	t.Setenv("HARNESS_AUTO_CHECKPOINT_ENABLED", "maybe")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "invalid HARNESS_AUTO_CHECKPOINT_ENABLED") {
		t.Fatalf("expected invalid auto checkpoint env error, got %v", err)
	}
}

func sameStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func sameInts(got, want []int) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
