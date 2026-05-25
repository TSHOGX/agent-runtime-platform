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
  session_ttl: 3h
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
	if phase7.SessionTTL.Duration != 3*time.Hour || phase7.MaxSessions != 10 {
		t.Fatalf("unexpected ttl/max: %s %d", phase7.SessionTTL.Duration, phase7.MaxSessions)
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
		phase7.Reaper.FailedRetention.Duration != 0 {
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
  session_ttl: 1h
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

func TestValidatePhase7Config(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Phase7Config)
		want   string
	}{
		{
			name: "session ttl",
			mutate: func(cfg *Phase7Config) {
				cfg.SessionTTL.Duration = 0
			},
			want: "harness.session_ttl must be > 0",
		},
		{
			name: "cidr capacity",
			mutate: func(cfg *Phase7Config) {
				cfg.MaxSessions = 1
				cfg.Network.CIDRPool.Prefix = netip.MustParsePrefix("10.0.0.0/30")
			},
			want: "must be less than /30 capacity",
		},
		{
			name: "missing doris hosts",
			mutate: func(cfg *Phase7Config) {
				cfg.Network.Egress.DorisFEHosts = nil
			},
			want: "doris_fe_hosts must be non-empty",
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
			name: "heartbeat lease",
			mutate: func(cfg *Phase7Config) {
				cfg.Bridge.LeaseTTL.Duration = 30 * time.Second
				cfg.Bridge.HeartbeatInterval.Duration = 30 * time.Second
			},
			want: "heartbeat_interval must be > 0 and <",
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			cfg := defaultPhase7Config()
			cfg.Secrets.Root = prepareSecretsRoot(t, dir, 1234)
			cfg.Secrets.ReadersGID = 1234
			tt.mutate(&cfg)

			err := validatePhase7Config(cfg)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected %q error, got %v", tt.want, err)
			}
		})
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
	t.Setenv("HARNESS_MAX_SESSIONS", "2")

	_, err = Load()
	if err == nil || !strings.Contains(err.Error(), "must be less than /30 capacity") {
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
