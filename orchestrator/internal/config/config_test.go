package config

import (
	"errors"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadProjectConfigUsesPhase7HarnessSchema(t *testing.T) {
	dir := t.TempDir()
	secretsRoot := prepareSecretsRoot(t, dir, 1234)
	path := filepath.Join(dir, "harness.yaml")
	if err := os.WriteFile(path, []byte(`harness:
  run_dir: /tmp/harness-run
  session_retention: 3h
  max_sessions: 10
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
  secrets:
    root: `+secretsRoot+`
    readers_gid: 1234
`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := loadProjectConfig(path)
	if err != nil {
		t.Fatalf("load project config: %v", err)
	}

	phase7 := cfg.Phase7
	if phase7.RunDir != "/tmp/harness-run" {
		t.Fatalf("run_dir: %q", phase7.RunDir)
	}
	if phase7.SessionRetention.Duration != 3*time.Hour || phase7.MaxSessions != 10 {
		t.Fatalf("unexpected retention/max: %s %d", phase7.SessionRetention.Duration, phase7.MaxSessions)
	}
	if got := phase7.Network.CIDRPool.String(); got != "10.210.0.0/24" {
		t.Fatalf("cidr_pool: %q", got)
	}
	if !sameStrings(phase7.Network.Egress.DorisFEHosts, []string{"172.16.0.138"}) ||
		!sameStrings(phase7.Network.Egress.DorisBEHosts, []string{"172.16.0.139"}) ||
		!sameInts(phase7.Network.Egress.DorisPorts, []int{9030, 8040}) {
		t.Fatalf("unexpected egress: %+v", phase7.Network.Egress)
	}
	if phase7.Network.Egress.DNSPolicy != DNSPolicyHostnamesOnly {
		t.Fatalf("dns policy: %q", phase7.Network.Egress.DNSPolicy)
	}
	if phase7.Events.RetentionRows != 500 || phase7.Events.RetentionWindow.Duration != 12*time.Hour {
		t.Fatalf("unexpected events config: %+v", phase7.Events)
	}
	if !sameInts(phase7.Probe.AcceptStatus.GetHealthz, []int{200, 204}) ||
		!sameInts(phase7.Probe.AcceptStatus.PostV1Messages.Unauthorized, []int{401, 403}) ||
		!sameInts(phase7.Probe.AcceptStatus.PostV1Messages.MalformedAuthenticated, []int{400, 422}) {
		t.Fatalf("unexpected probe statuses: %+v", phase7.Probe.AcceptStatus)
	}
	if phase7.Bridge.HeartbeatInterval.Duration != 20*time.Second ||
		phase7.Bridge.ReconnectGrace.Duration != 25*time.Second ||
		phase7.Reaper.FailedRetention.Duration != 0 ||
		phase7.Reaper.CheckpointImageRetention.Duration != 720*time.Hour {
		t.Fatalf("unexpected bridge/reaper config: bridge=%+v reaper=%+v", phase7.Bridge, phase7.Reaper)
	}
	if !phase7.Checkpoint.AutoEnabled ||
		phase7.Checkpoint.IdleThreshold.Duration != 7*time.Minute ||
		phase7.Checkpoint.MonitorInterval.Duration != 11*time.Second {
		t.Fatalf("unexpected checkpoint config: %+v", phase7.Checkpoint)
	}
	if phase7.Secrets.Root != secretsRoot || phase7.Secrets.ReadersGID != 1234 {
		t.Fatalf("unexpected secrets config: %+v", phase7.Secrets)
	}
	if cfg.Runtime.RunscNetwork != "sandbox" || cfg.Runtime.RunscOverlay2 != "none" {
		t.Fatalf("unexpected runtime defaults: %+v", cfg.Runtime)
	}
	if !cfg.Claude.DisableNonessentialTraffic {
		t.Fatalf("expected default nonessential traffic setting to be true")
	}
}

func TestLoadProjectConfigUsesLegacyHarnessProxyConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "harness.yaml")
	if err := os.WriteFile(path, []byte(`runtime:
  runsc_network: sandbox
  runsc_overlay2: none

claude:
  proxy_bind_url: http://0.0.0.0:8082
  sandbox_base_url: http://10.200.1.1:8082
  api_key: "123"
  auth_token: "123"
  model: sonnet
  output_format: stream-json
  disable_nonessential_traffic: true
`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := loadProjectConfig(path)
	if err != nil {
		t.Fatalf("load project config: %v", err)
	}
	if cfg.Runtime.RunscNetwork != "sandbox" || cfg.Runtime.RunscOverlay2 != "none" {
		t.Fatalf("unexpected runtime config: %+v", cfg.Runtime)
	}
	if cfg.Claude.ProxyBindURL != "http://0.0.0.0:8082" ||
		cfg.Claude.SandboxBaseURL != "http://10.200.1.1:8082" ||
		cfg.Claude.APIKey != "123" ||
		cfg.Claude.AuthToken != "123" {
		t.Fatalf("unexpected claude config: %+v", cfg.Claude)
	}
	if !cfg.Claude.DisableNonessentialTraffic {
		t.Fatalf("expected nonessential traffic disabled: %+v", cfg.Claude)
	}
}

func TestLoadProjectConfigRejectsMixedHarnessAndLegacySections(t *testing.T) {
	dir := t.TempDir()
	secretsRoot := prepareSecretsRoot(t, dir, 1234)
	path := filepath.Join(dir, "harness.yaml")
	if err := os.WriteFile(path, []byte(`harness:
  secrets:
    root: `+secretsRoot+`
    readers_gid: 1234
runtime:
  runsc_network: sandbox
`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, err := loadProjectConfig(path)
	if err == nil || !strings.Contains(err.Error(), "mixed harness and legacy") {
		t.Fatalf("expected mixed section error, got %v", err)
	}
}

func TestLoadProjectConfigRejectsUnknownKeys(t *testing.T) {
	dir := t.TempDir()
	secretsRoot := prepareSecretsRoot(t, dir, 1234)
	path := filepath.Join(dir, "harness.yaml")
	if err := os.WriteFile(path, []byte(`harness:
  session_retention: 1h
  surprise: true
  secrets:
    root: `+secretsRoot+`
    readers_gid: 1234
`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, err := loadProjectConfig(path)
	if err == nil || !strings.Contains(err.Error(), "field surprise not found") {
		t.Fatalf("expected unknown field error, got %v", err)
	}
}

func TestLoadProjectConfigRejectsObsoleteSessionTTLKey(t *testing.T) {
	dir := t.TempDir()
	secretsRoot := prepareSecretsRoot(t, dir, 1234)
	path := filepath.Join(dir, "harness.yaml")
	if err := os.WriteFile(path, []byte(`harness:
  session_ttl: 1h
  secrets:
    root: `+secretsRoot+`
    readers_gid: 1234
`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, err := loadProjectConfig(path)
	if err == nil || !strings.Contains(err.Error(), "field session_ttl not found") {
		t.Fatalf("expected obsolete session_ttl error, got %v", err)
	}
}

func TestValidatePhase7Config(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Phase7Config)
		want   string
	}{
		{
			name: "run dir",
			mutate: func(cfg *Phase7Config) {
				cfg.RunDir = ""
			},
			want: "harness.run_dir is required",
		},
		{
			name: "session retention",
			mutate: func(cfg *Phase7Config) {
				cfg.SessionRetention.Duration = -time.Second
			},
			want: "harness.session_retention must be >= 0",
		},
		{
			name: "max sessions",
			mutate: func(cfg *Phase7Config) {
				cfg.MaxSessions = 0
			},
			want: "harness.max_sessions must be > 0",
		},
		{
			name: "missing cidr",
			mutate: func(cfg *Phase7Config) {
				cfg.Network.CIDRPool.Prefix = netip.Prefix{}
			},
			want: "harness.network.cidr_pool is required",
		},
		{
			name: "ipv6 cidr",
			mutate: func(cfg *Phase7Config) {
				cfg.Network.CIDRPool.Prefix = netip.MustParsePrefix("fd00::/120")
			},
			want: "harness.network.cidr_pool must be IPv4",
		},
		{
			name: "too narrow cidr",
			mutate: func(cfg *Phase7Config) {
				cfg.Network.CIDRPool.Prefix = netip.MustParsePrefix("10.0.0.0/31")
			},
			want: "harness.network.cidr_pool prefix length must be <= 30",
		},
		{
			name: "missing doris hosts",
			mutate: func(cfg *Phase7Config) {
				cfg.Network.Egress.DorisFEHosts = nil
			},
			want: "doris_fe_hosts must be non-empty",
		},
		{
			name: "missing doris be hosts",
			mutate: func(cfg *Phase7Config) {
				cfg.Network.Egress.DorisBEHosts = nil
			},
			want: "doris_be_hosts must be non-empty",
		},
		{
			name: "missing doris ports",
			mutate: func(cfg *Phase7Config) {
				cfg.Network.Egress.DorisPorts = nil
			},
			want: "doris_ports must be non-empty",
		},
		{
			name: "invalid doris port",
			mutate: func(cfg *Phase7Config) {
				cfg.Network.Egress.DorisPorts = []int{0}
			},
			want: "doris_ports contains invalid port 0",
		},
		{
			name: "hostname needs dns",
			mutate: func(cfg *Phase7Config) {
				cfg.Network.Egress.DorisFEHosts = []string{"doris-fe.local"}
				cfg.Network.Egress.DNSPolicy = DNSPolicyOff
			},
			want: "dns_policy must not be off",
		},
		{
			name: "probe statuses",
			mutate: func(cfg *Phase7Config) {
				cfg.Probe.AcceptStatus.GetHealthz = nil
			},
			want: "get_healthz must be non-empty",
		},
		{
			name: "missing unauthorized probe statuses",
			mutate: func(cfg *Phase7Config) {
				cfg.Probe.AcceptStatus.PostV1Messages.Unauthorized = nil
			},
			want: "post_v1_messages.unauthorized must be non-empty",
		},
		{
			name: "missing malformed probe statuses",
			mutate: func(cfg *Phase7Config) {
				cfg.Probe.AcceptStatus.PostV1Messages.MalformedAuthenticated = nil
			},
			want: "post_v1_messages.malformed_authenticated must be non-empty",
		},
		{
			name: "pre start attempts",
			mutate: func(cfg *Phase7Config) {
				cfg.Probe.PreStartAttempts = 0
			},
			want: "pre_start_attempts must be > 0",
		},
		{
			name: "pre start interval",
			mutate: func(cfg *Phase7Config) {
				cfg.Probe.PreStartInterval.Duration = 0
			},
			want: "pre_start_interval must be > 0",
		},
		{
			name: "post start attempts",
			mutate: func(cfg *Phase7Config) {
				cfg.Probe.PostStartAttempts = 0
			},
			want: "post_start_attempts must be > 0",
		},
		{
			name: "post start interval",
			mutate: func(cfg *Phase7Config) {
				cfg.Probe.PostStartInterval.Duration = 0
			},
			want: "post_start_interval must be > 0",
		},
		{
			name: "lease ttl",
			mutate: func(cfg *Phase7Config) {
				cfg.Bridge.LeaseTTL.Duration = 0
			},
			want: "lease_ttl must be > 0",
		},
		{
			name: "heartbeat lease",
			mutate: func(cfg *Phase7Config) {
				cfg.Bridge.LeaseTTL.Duration = 30 * time.Second
				cfg.Bridge.HeartbeatInterval.Duration = 30 * time.Second
			},
			want: "heartbeat_interval must be > 0 and <",
		},
		{
			name: "poll interval",
			mutate: func(cfg *Phase7Config) {
				cfg.Bridge.PollInterval.Duration = 0
			},
			want: "poll_interval must be > 0",
		},
		{
			name: "ack grace positive",
			mutate: func(cfg *Phase7Config) {
				cfg.Bridge.AckStartedGrace.Duration = 0
			},
			want: "ack_started_grace must be > 0",
		},
		{
			name: "reconnect grace",
			mutate: func(cfg *Phase7Config) {
				cfg.Bridge.ReconnectGrace.Duration = -time.Second
			},
			want: "reconnect_grace must be >= 0",
		},
		{
			name: "ack reconnect grace",
			mutate: func(cfg *Phase7Config) {
				cfg.Bridge.AckStartedGrace.Duration = 10 * time.Second
				cfg.Bridge.ReconnectGrace.Duration = 20 * time.Second
			},
			want: "ack_started_grace must be >=",
		},
		{
			name: "events bounds",
			mutate: func(cfg *Phase7Config) {
				cfg.Events.RetentionWindow.Duration = 0
				cfg.Events.RetentionRows = 0
			},
			want: "cannot both be zero",
		},
		{
			name: "negative event retention window",
			mutate: func(cfg *Phase7Config) {
				cfg.Events.RetentionWindow.Duration = -time.Second
			},
			want: "retention_window must be >= 0",
		},
		{
			name: "negative event retention rows",
			mutate: func(cfg *Phase7Config) {
				cfg.Events.RetentionRows = -1
			},
			want: "retention_rows must be >= 0",
		},
		{
			name: "emit output batch rows",
			mutate: func(cfg *Phase7Config) {
				cfg.Events.EmitOutputBatchMaxRows = 0
			},
			want: "emit_output_batch_max_rows must be > 0",
		},
		{
			name: "emit output batch age",
			mutate: func(cfg *Phase7Config) {
				cfg.Events.EmitOutputBatchMaxAge.Duration = 0
			},
			want: "emit_output_batch_max_age must be > 0",
		},
		{
			name: "checkpoint monitor",
			mutate: func(cfg *Phase7Config) {
				cfg.Checkpoint.MonitorInterval.Duration = 0
			},
			want: "monitor_interval must be > 0",
		},
		{
			name: "checkpoint idle threshold",
			mutate: func(cfg *Phase7Config) {
				cfg.Checkpoint.IdleThreshold.Duration = -time.Second
			},
			want: "idle_threshold must be >= 0",
		},
		{
			name: "negative reaper",
			mutate: func(cfg *Phase7Config) {
				cfg.Reaper.FailedRetention.Duration = -time.Second
			},
			want: "failed_retention must be >= 0",
		},
		{
			name: "zero checkpoint image retention",
			mutate: func(cfg *Phase7Config) {
				cfg.Reaper.CheckpointImageRetention.Duration = 0
			},
			want: "",
		},
		{
			name: "negative checkpoint image retention",
			mutate: func(cfg *Phase7Config) {
				cfg.Reaper.CheckpointImageRetention.Duration = -time.Second
			},
			want: "checkpoint_image_retention must be >= 0",
		},
		{
			name: "secrets root required",
			mutate: func(cfg *Phase7Config) {
				cfg.Secrets.Root = ""
			},
			want: "harness.secrets.root is required",
		},
		{
			name: "secrets readers gid",
			mutate: func(cfg *Phase7Config) {
				cfg.Secrets.ReadersGID = 0
			},
			want: "harness.secrets.readers_gid must be > 0",
		},
		{
			name: "secrets root missing",
			mutate: func(cfg *Phase7Config) {
				cfg.Secrets.Root = filepath.Join(t.TempDir(), "missing")
			},
			want: "must exist",
		},
		{
			name: "secrets root file",
			mutate: func(cfg *Phase7Config) {
				path := filepath.Join(t.TempDir(), "secrets-file")
				if err := os.WriteFile(path, []byte("not a directory"), 0o640); err != nil {
					t.Fatalf("write secrets file: %v", err)
				}
				cfg.Secrets.Root = path
			},
			want: "must be a directory",
		},
		{
			name: "secrets root mode",
			mutate: func(cfg *Phase7Config) {
				dir := t.TempDir()
				root := filepath.Join(dir, "secrets")
				if err := os.Mkdir(root, 0o755); err != nil {
					t.Fatalf("mkdir secrets root: %v", err)
				}
				cfg.Secrets.Root = root
				cfg.Secrets.ReadersGID = 1234
			},
			want: "must have mode 0750",
		},
		{
			name: "secrets root group",
			mutate: func(cfg *Phase7Config) {
				cfg.Secrets.ReadersGID = 4321
			},
			want: "must have group 4321",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			cfg := defaultPhase7Config()
			cfg.Secrets.Root = prepareSecretsRoot(t, dir, 1234)
			cfg.Secrets.ReadersGID = 1234
			tt.mutate(&cfg)

			err := validatePhase7Config(cfg)
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

func TestValidatePhase7ConfigAllowsZeroSessionRetention(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultPhase7Config()
	cfg.SessionRetention.Duration = 0
	cfg.Secrets.Root = prepareSecretsRoot(t, dir, 1234)
	cfg.Secrets.ReadersGID = 1234

	if err := validatePhase7Config(cfg); err != nil {
		t.Fatalf("zero session retention should be valid: %v", err)
	}
}

func TestValidatePhase7ConfigAllowsMaxSessionsAboveCIDRCapacity(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultPhase7Config()
	cfg.MaxSessions = 10
	cfg.Network.CIDRPool.Prefix = netip.MustParsePrefix("10.0.0.0/30")
	cfg.Secrets.Root = prepareSecretsRoot(t, dir, 1234)
	cfg.Secrets.ReadersGID = 1234

	if err := validatePhase7Config(cfg); err != nil {
		t.Fatalf("max_sessions should be independent from /30 capacity: %v", err)
	}
}

func TestSessionRetentionEnvRejectsObsoleteSessionTTL(t *testing.T) {
	unsetEnvForTest(t, "HARNESS_SESSION_RETENTION")
	t.Setenv("HARNESS_SESSION_TTL", "2h")

	_, err := sessionRetentionEnv(time.Hour)
	if err == nil || !strings.Contains(err.Error(), "HARNESS_SESSION_TTL is obsolete; use HARNESS_SESSION_RETENTION") {
		t.Fatalf("expected obsolete env error, got %v", err)
	}
}

func TestSessionRetentionEnvRejectsObsoleteSessionTTLEvenWithReplacement(t *testing.T) {
	t.Setenv("HARNESS_SESSION_TTL", "2h")
	t.Setenv("HARNESS_SESSION_RETENTION", "720h")

	_, err := sessionRetentionEnv(time.Hour)
	if err == nil || !strings.Contains(err.Error(), "HARNESS_SESSION_TTL is obsolete") {
		t.Fatalf("expected obsolete env error, got %v", err)
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

func TestLoadRejectsObsoleteSessionTTLEnv(t *testing.T) {
	repo := writeMinimalLoadConfig(t)
	chdirForLoadTest(t, repo)
	t.Setenv("HARNESS_SESSION_TTL", "2h")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "HARNESS_SESSION_TTL is obsolete; use HARNESS_SESSION_RETENTION") {
		t.Fatalf("expected obsolete env load error, got %v", err)
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

func TestLoadProjectConfigMissingFileUsesDefaults(t *testing.T) {
	cfg, err := loadProjectConfig(filepath.Join(t.TempDir(), "missing.yaml"))
	if err != nil {
		t.Fatalf("missing config should not fail: %v", err)
	}
	if !cfg.Claude.DisableNonessentialTraffic {
		t.Fatalf("expected default nonessential traffic setting to be true")
	}
	if cfg.Phase7.ControlRoot() != "/var/lib/harness/run/control" ||
		cfg.Phase7.BundleRoot() != "/var/lib/harness/run/runtime" ||
		cfg.Phase7.BridgeRoot() != "/var/lib/harness/run/bridge" {
		t.Fatalf("unexpected derived roots: control=%s bundle=%s bridge=%s", cfg.Phase7.ControlRoot(), cfg.Phase7.BundleRoot(), cfg.Phase7.BridgeRoot())
	}
	if cfg.Phase7.Reaper.CheckpointImageRetention.Duration != 720*time.Hour {
		t.Fatalf("checkpoint image retention default=%s want 720h", cfg.Phase7.Reaper.CheckpointImageRetention.Duration)
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
  secrets:
    root: `+prepareSecretsRoot(t, repo, 1234)+`
    readers_gid: 1234
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

func TestCheckedInHarnessConfigLoads(t *testing.T) {
	cfg, err := loadProjectConfig(filepath.Join("..", "..", "..", "config", "harness.yaml"))
	if err != nil {
		t.Fatalf("load checked-in harness config: %v", err)
	}
	root := prepareSecretsRoot(t, t.TempDir(), cfg.Phase7.Secrets.ReadersGID)
	cfg.Phase7.Secrets.Root = root
	if err := validatePhase7Config(cfg.Phase7); err != nil {
		t.Fatalf("validate checked-in harness config: %v", err)
	}
}

func TestLoadValidatesMergedPhase7Config(t *testing.T) {
	repo := t.TempDir()
	configDir := filepath.Join(repo, "config")
	if err := os.Mkdir(configDir, 0o755); err != nil {
		t.Fatalf("mkdir config dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(configDir, "harness.yaml"), []byte(`harness:
  network:
    cidr_pool: 10.0.0.0/30
  secrets:
    root: `+prepareSecretsRoot(t, repo, 1234)+`
    readers_gid: 1234
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
	repo := t.TempDir()
	configDir := filepath.Join(repo, "config")
	if err := os.Mkdir(configDir, 0o755); err != nil {
		t.Fatalf("mkdir config dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(configDir, "harness.yaml"), []byte(`harness:
  checkpoint:
    auto_enabled: false
  secrets:
    root: `+prepareSecretsRoot(t, repo, 1234)+`
    readers_gid: 1234
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
	t.Setenv("HARNESS_AUTO_CHECKPOINT_ENABLED", "true")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if !cfg.Phase7.Checkpoint.AutoEnabled {
		t.Fatalf("expected env override to enable checkpoint policy: %+v", cfg.Phase7.Checkpoint)
	}
}

func prepareSecretsRoot(t *testing.T, parent string, gid int) string {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("secret root ownership validation requires root")
	}
	root := filepath.Join(parent, "secrets")
	if err := os.MkdirAll(root, 0o750); err != nil {
		t.Fatalf("mkdir secrets root: %v", err)
	}
	if err := os.Chmod(root, 0o750); err != nil {
		t.Fatalf("chmod secrets root: %v", err)
	}
	if err := os.Chown(root, os.Getuid(), gid); err != nil {
		if errors.Is(err, os.ErrPermission) {
			t.Skipf("chown secrets root: %v", err)
		}
		t.Fatalf("chown secrets root: %v", err)
	}
	return root
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
