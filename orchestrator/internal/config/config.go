package config

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Addr            string
	SharedSecret    string
	CookieName      string
	SessionTTL      time.Duration
	RepoRoot        string
	RestoreScript   string
	RunscRoot       string
	SessionsRoot    string
	AgentHomesRoot  string
	CheckpointsRoot string
	BundleRoot      string
	DBPath          string
	DefaultAgent    string
	MaxSessions     int
	RunscNetwork    string
	RunscOverlay2   string
	Claude          ClaudeConfig
	Phase7          Phase7Config
	Warnings        []string
}

type ClaudeConfig struct {
	ProxyBindURL               string `yaml:"proxy_bind_url"`
	SandboxBaseURL             string `yaml:"sandbox_base_url"`
	APIKey                     string `yaml:"api_key"`
	AuthToken                  string `yaml:"auth_token"`
	Model                      string `yaml:"model"`
	OutputFormat               string `yaml:"output_format"`
	DisableNonessentialTraffic bool   `yaml:"disable_nonessential_traffic"`
}

type Phase7Config struct {
	RunDir      string           `yaml:"run_dir"`
	SessionTTL  Duration         `yaml:"session_ttl"`
	MaxSessions int              `yaml:"max_sessions"`
	Network     NetworkConfig    `yaml:"network"`
	Events      EventsConfig     `yaml:"events"`
	Probe       ProbeConfig      `yaml:"probe"`
	Bridge      BridgeConfig     `yaml:"bridge"`
	Checkpoint  CheckpointConfig `yaml:"checkpoint"`
	Reaper      ReaperConfig     `yaml:"reaper"`
	Secrets     SecretsConfig    `yaml:"secrets"`
}

func (c Phase7Config) ControlRoot() string {
	return filepath.Join(c.RunDir, "control")
}

func (c Phase7Config) BundleRoot() string {
	return filepath.Join(c.RunDir, "runtime")
}

func (c Phase7Config) BridgeRoot() string {
	return filepath.Join(c.RunDir, "bridge")
}

type RuntimeYAMLConfig struct {
	RunscNetwork  string `yaml:"runsc_network"`
	RunscOverlay2 string `yaml:"runsc_overlay2"`
}

type NetworkConfig struct {
	CIDRPool CIDRPrefix   `yaml:"cidr_pool"`
	Egress   EgressConfig `yaml:"egress"`
}

type EgressConfig struct {
	DorisFEHosts []string  `yaml:"doris_fe_hosts"`
	DorisBEHosts []string  `yaml:"doris_be_hosts"`
	DorisPorts   []int     `yaml:"doris_ports"`
	DNSPolicy    DNSPolicy `yaml:"dns_policy"`
}

type DNSPolicy string

const (
	DNSPolicyOff           DNSPolicy = "off"
	DNSPolicyHostnamesOnly DNSPolicy = "hostnames_only"
	DNSPolicyAlways        DNSPolicy = "always"
)

func (p *DNSPolicy) UnmarshalYAML(value *yaml.Node) error {
	var raw string
	if err := value.Decode(&raw); err != nil {
		return err
	}
	switch DNSPolicy(raw) {
	case DNSPolicyOff, DNSPolicyHostnamesOnly, DNSPolicyAlways:
		*p = DNSPolicy(raw)
		return nil
	default:
		return fmt.Errorf("invalid dns_policy %q", raw)
	}
}

type EventsConfig struct {
	RetentionWindow        Duration `yaml:"retention_window"`
	RetentionRows          int64    `yaml:"retention_rows"`
	EmitOutputBatchMaxRows int      `yaml:"emit_output_batch_max_rows"`
	EmitOutputBatchMaxAge  Duration `yaml:"emit_output_batch_max_age"`
}

type ProbeConfig struct {
	AcceptStatus      ProbeAcceptStatusConfig `yaml:"accept_status"`
	PreStartAttempts  int                     `yaml:"pre_start_attempts"`
	PreStartInterval  Duration                `yaml:"pre_start_interval"`
	PostStartAttempts int                     `yaml:"post_start_attempts"`
	PostStartInterval Duration                `yaml:"post_start_interval"`
}

type ProbeAcceptStatusConfig struct {
	GetHealthz     []int                  `yaml:"get_healthz"`
	PostV1Messages PostV1MessagesStatuses `yaml:"post_v1_messages"`
}

type PostV1MessagesStatuses struct {
	Unauthorized           []int `yaml:"unauthorized"`
	MalformedAuthenticated []int `yaml:"malformed_authenticated"`
}

type BridgeConfig struct {
	LeaseTTL          Duration `yaml:"lease_ttl"`
	HeartbeatInterval Duration `yaml:"heartbeat_interval"`
	PollInterval      Duration `yaml:"poll_interval"`
	AckStartedGrace   Duration `yaml:"ack_started_grace"`
	ReconnectGrace    Duration `yaml:"reconnect_grace"`
}

type CheckpointConfig struct {
	AutoEnabled     bool     `yaml:"auto_enabled"`
	IdleThreshold   Duration `yaml:"idle_threshold"`
	MonitorInterval Duration `yaml:"monitor_interval"`
}

type ReaperConfig struct {
	FailedRetention Duration `yaml:"failed_retention"`
}

type SecretsConfig struct {
	Root       string `yaml:"root"`
	ReadersGID int    `yaml:"readers_gid"`
}

type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var raw string
	if err := value.Decode(&raw); err != nil {
		return err
	}
	if strings.TrimSpace(raw) == "0" {
		d.Duration = 0
		return nil
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", raw, err)
	}
	d.Duration = parsed
	return nil
}

type CIDRPrefix struct {
	netip.Prefix
}

func (p *CIDRPrefix) UnmarshalYAML(value *yaml.Node) error {
	var raw string
	if err := value.Decode(&raw); err != nil {
		return err
	}
	parsed, err := netip.ParsePrefix(raw)
	if err != nil {
		return fmt.Errorf("invalid CIDR %q: %w", raw, err)
	}
	p.Prefix = parsed
	return nil
}

func Load() (Config, error) {
	repoRoot, err := os.Getwd()
	if err != nil {
		return Config{}, err
	}
	if filepath.Base(repoRoot) == "orchestrator" {
		repoRoot = filepath.Dir(repoRoot)
	}
	projectConfig, err := loadProjectConfig(filepath.Join(repoRoot, "config", "harness.yaml"))
	if err != nil {
		return Config{}, err
	}

	sessionsRoot := getenv("HARNESS_SESSIONS_ROOT", "/var/lib/harness/sessions")
	sessionTTL := durationEnv("HARNESS_SESSION_TTL", projectConfig.Phase7.SessionTTL.Duration)
	maxSessions := intEnv("HARNESS_MAX_SESSIONS", projectConfig.Phase7.MaxSessions)
	cfg := Config{
		Addr:            getenv("HARNESS_ORCHESTRATOR_ADDR", ":8090"),
		SharedSecret:    os.Getenv("HARNESS_LAB_PASSWORD"),
		CookieName:      getenv("HARNESS_COOKIE_NAME", "harness_auth"),
		SessionTTL:      sessionTTL,
		RepoRoot:        getenv("HARNESS_REPO_ROOT", repoRoot),
		RestoreScript:   getenv("HARNESS_RESTORE_SCRIPT", filepath.Join(repoRoot, "bundle", "restore-sandbox.sh")),
		RunscRoot:       getenv("RUNSC_ROOT", "/var/lib/harness/runsc"),
		SessionsRoot:    sessionsRoot,
		AgentHomesRoot:  getenv("HARNESS_AGENT_HOMES_ROOT", "/var/lib/harness/agent-homes"),
		CheckpointsRoot: getenv("HARNESS_CHECKPOINTS_ROOT", "/var/lib/harness/checkpoints"),
		BundleRoot:      getenv("HARNESS_BUNDLE_ROOT", filepath.Join(repoRoot, "bundle", "out")),
		DBPath:          getenv("HARNESS_DB_PATH", filepath.Join(sessionsRoot, "orchestrator.db")),
		DefaultAgent:    getenv("HARNESS_DEFAULT_AGENT", "claude"),
		MaxSessions:     maxSessions,
		RunscNetwork:    defaultString(projectConfig.Runtime.RunscNetwork, "sandbox"),
		RunscOverlay2:   defaultString(projectConfig.Runtime.RunscOverlay2, "none"),
		Claude:          projectConfig.Claude,
		Phase7:          projectConfig.Phase7,
	}
	cfg.Phase7.SessionTTL = Duration{Duration: sessionTTL}
	cfg.Phase7.MaxSessions = maxSessions
	if value, ok := boolEnv("HARNESS_AUTO_CHECKPOINT_ENABLED"); ok {
		cfg.Phase7.Checkpoint.AutoEnabled = value
	}
	if err := validatePhase7Config(cfg.Phase7); err != nil {
		return Config{}, err
	}
	cfg.Claude = normalizeClaudeConfig(cfg.Claude)
	cfg.Warnings = phase7ConfigWarnings(cfg.Phase7)
	return cfg, nil
}

type projectConfig struct {
	Phase7  Phase7Config
	Runtime RuntimeYAMLConfig
	Claude  ClaudeConfig
}

func loadProjectConfig(path string) (projectConfig, error) {
	cfg := projectConfig{
		Phase7: defaultPhase7Config(),
		Runtime: RuntimeYAMLConfig{
			RunscNetwork:  "sandbox",
			RunscOverlay2: "none",
		},
		Claude: ClaudeConfig{
			ProxyBindURL:               "http://0.0.0.0:8082",
			SandboxBaseURL:             "http://10.200.1.1:8082",
			APIKey:                     "123",
			AuthToken:                  "123",
			Model:                      "sonnet",
			OutputFormat:               "stream-json",
			DisableNonessentialTraffic: true,
		},
	}

	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return cfg, nil
	}
	if err != nil {
		return cfg, err
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return cfg, nil
	}

	hasHarness, hasLegacy, err := inspectProjectConfigTopLevel(data)
	if err != nil {
		return cfg, fmt.Errorf("load %s: %w", path, err)
	}
	if hasHarness && hasLegacy {
		return cfg, fmt.Errorf("load %s: mixed harness and legacy runtime/claude sections are not allowed", path)
	}
	if hasHarness {
		var target struct {
			Harness Phase7Config `yaml:"harness"`
		}
		target.Harness = cfg.Phase7
		decoder := yaml.NewDecoder(bytes.NewReader(data))
		decoder.KnownFields(true)
		if err := decoder.Decode(&target); err != nil {
			return cfg, fmt.Errorf("load %s: %w", path, err)
		}
		cfg.Phase7 = target.Harness
	} else if hasLegacy {
		var target struct {
			Runtime RuntimeYAMLConfig `yaml:"runtime"`
			Claude  ClaudeConfig      `yaml:"claude"`
		}
		target.Runtime = cfg.Runtime
		target.Claude = cfg.Claude
		decoder := yaml.NewDecoder(bytes.NewReader(data))
		decoder.KnownFields(true)
		if err := decoder.Decode(&target); err != nil {
			return cfg, fmt.Errorf("load %s: %w", path, err)
		}
		cfg.Runtime = target.Runtime
		cfg.Claude = target.Claude
	}
	cfg.Claude = normalizeClaudeConfig(cfg.Claude)
	return cfg, nil
}

func inspectProjectConfigTopLevel(data []byte) (hasHarness bool, hasLegacy bool, err error) {
	var doc yaml.Node
	if err := yaml.NewDecoder(bytes.NewReader(data)).Decode(&doc); err != nil {
		return false, false, err
	}
	if len(doc.Content) == 0 {
		return false, false, nil
	}
	root := doc.Content[0]
	if root.Kind != yaml.MappingNode {
		return false, false, fmt.Errorf("top-level config must be a mapping")
	}
	for i := 0; i < len(root.Content); i += 2 {
		key := root.Content[i].Value
		switch key {
		case "harness":
			hasHarness = true
		case "runtime", "claude":
			hasLegacy = true
		default:
			return false, false, fmt.Errorf("line %d: unknown top-level config key %q", root.Content[i].Line, key)
		}
	}
	return hasHarness, hasLegacy, nil
}

func defaultPhase7Config() Phase7Config {
	return Phase7Config{
		RunDir:      "/var/lib/harness/run",
		SessionTTL:  Duration{Duration: 2 * time.Hour},
		MaxSessions: 30,
		Network: NetworkConfig{
			CIDRPool: CIDRPrefix{Prefix: netip.MustParsePrefix("10.200.0.0/16")},
			Egress: EgressConfig{
				DorisFEHosts: []string{"172.16.0.138"},
				DorisBEHosts: []string{"172.16.0.138"},
				DorisPorts:   []int{9030},
				DNSPolicy:    DNSPolicyHostnamesOnly,
			},
		},
		Events: EventsConfig{
			RetentionWindow:        Duration{Duration: 24 * time.Hour},
			RetentionRows:          1_000_000,
			EmitOutputBatchMaxRows: 64,
			EmitOutputBatchMaxAge:  Duration{Duration: 100 * time.Millisecond},
		},
		Probe: ProbeConfig{
			AcceptStatus: ProbeAcceptStatusConfig{
				GetHealthz: []int{200},
				PostV1Messages: PostV1MessagesStatuses{
					Unauthorized:           []int{401},
					MalformedAuthenticated: []int{400},
				},
			},
			PreStartAttempts:  3,
			PreStartInterval:  Duration{Duration: 500 * time.Millisecond},
			PostStartAttempts: 5,
			PostStartInterval: Duration{Duration: time.Second},
		},
		Bridge: BridgeConfig{
			LeaseTTL:          Duration{Duration: time.Minute},
			HeartbeatInterval: Duration{Duration: 30 * time.Second},
			PollInterval:      Duration{Duration: 5 * time.Millisecond},
			AckStartedGrace:   Duration{Duration: 90 * time.Second},
			ReconnectGrace:    Duration{Duration: 30 * time.Second},
		},
		Checkpoint: CheckpointConfig{
			AutoEnabled:     false,
			IdleThreshold:   Duration{Duration: 30 * time.Minute},
			MonitorInterval: Duration{Duration: 5 * time.Minute},
		},
		Reaper: ReaperConfig{
			FailedRetention: Duration{Duration: 10 * time.Minute},
		},
		Secrets: SecretsConfig{
			Root:       "/var/lib/harness/secrets",
			ReadersGID: 65501,
		},
	}
}

func validatePhase7Config(cfg Phase7Config) error {
	if strings.TrimSpace(cfg.RunDir) == "" {
		return fmt.Errorf("harness.run_dir is required")
	}
	if cfg.SessionTTL.Duration <= 0 {
		return fmt.Errorf("harness.session_ttl must be > 0")
	}
	if cfg.MaxSessions <= 0 {
		return fmt.Errorf("harness.max_sessions must be > 0")
	}
	pool := cfg.Network.CIDRPool.Prefix
	if !pool.IsValid() {
		return fmt.Errorf("harness.network.cidr_pool is required")
	}
	if !pool.Addr().Is4() {
		return fmt.Errorf("harness.network.cidr_pool must be IPv4")
	}
	if pool.Bits() > 30 {
		return fmt.Errorf("harness.network.cidr_pool prefix length must be <= 30")
	}
	if capacity := cidrPool30Capacity(pool); uint64(cfg.MaxSessions) >= capacity {
		return fmt.Errorf("harness.max_sessions %d must be less than /30 capacity %d for harness.network.cidr_pool", cfg.MaxSessions, capacity)
	}
	if len(cfg.Network.Egress.DorisFEHosts) == 0 {
		return fmt.Errorf("harness.network.egress.doris_fe_hosts must be non-empty")
	}
	if err := validateHosts("harness.network.egress.doris_fe_hosts", cfg.Network.Egress.DorisFEHosts); err != nil {
		return err
	}
	if len(cfg.Network.Egress.DorisBEHosts) == 0 {
		return fmt.Errorf("harness.network.egress.doris_be_hosts must be non-empty")
	}
	if err := validateHosts("harness.network.egress.doris_be_hosts", cfg.Network.Egress.DorisBEHosts); err != nil {
		return err
	}
	if len(cfg.Network.Egress.DorisPorts) == 0 {
		return fmt.Errorf("harness.network.egress.doris_ports must be non-empty")
	}
	for _, port := range cfg.Network.Egress.DorisPorts {
		if port <= 0 || port > 65535 {
			return fmt.Errorf("harness.network.egress.doris_ports contains invalid port %d", port)
		}
	}
	if cfg.Network.Egress.DNSPolicy == DNSPolicyOff && anyHostname(cfg.Network.Egress.DorisFEHosts, cfg.Network.Egress.DorisBEHosts) {
		return fmt.Errorf("harness.network.egress.dns_policy must not be off when Doris hosts include hostnames")
	}
	if len(cfg.Probe.AcceptStatus.GetHealthz) == 0 {
		return fmt.Errorf("harness.probe.accept_status.get_healthz must be non-empty")
	}
	if len(cfg.Probe.AcceptStatus.PostV1Messages.Unauthorized) == 0 {
		return fmt.Errorf("harness.probe.accept_status.post_v1_messages.unauthorized must be non-empty")
	}
	if len(cfg.Probe.AcceptStatus.PostV1Messages.MalformedAuthenticated) == 0 {
		return fmt.Errorf("harness.probe.accept_status.post_v1_messages.malformed_authenticated must be non-empty")
	}
	if cfg.Probe.PreStartAttempts <= 0 {
		return fmt.Errorf("harness.probe.pre_start_attempts must be > 0")
	}
	if cfg.Probe.PreStartInterval.Duration <= 0 {
		return fmt.Errorf("harness.probe.pre_start_interval must be > 0")
	}
	if cfg.Probe.PostStartAttempts <= 0 {
		return fmt.Errorf("harness.probe.post_start_attempts must be > 0")
	}
	if cfg.Probe.PostStartInterval.Duration <= 0 {
		return fmt.Errorf("harness.probe.post_start_interval must be > 0")
	}
	if cfg.Bridge.LeaseTTL.Duration <= 0 {
		return fmt.Errorf("harness.bridge.lease_ttl must be > 0")
	}
	if cfg.Bridge.HeartbeatInterval.Duration <= 0 || cfg.Bridge.HeartbeatInterval.Duration >= cfg.Bridge.LeaseTTL.Duration {
		return fmt.Errorf("harness.bridge.heartbeat_interval must be > 0 and < harness.bridge.lease_ttl")
	}
	if cfg.Bridge.PollInterval.Duration <= 0 {
		return fmt.Errorf("harness.bridge.poll_interval must be > 0")
	}
	if cfg.Bridge.AckStartedGrace.Duration <= 0 {
		return fmt.Errorf("harness.bridge.ack_started_grace must be > 0")
	}
	if cfg.Bridge.ReconnectGrace.Duration < 0 {
		return fmt.Errorf("harness.bridge.reconnect_grace must be >= 0")
	}
	if cfg.Bridge.AckStartedGrace.Duration < cfg.Bridge.ReconnectGrace.Duration {
		return fmt.Errorf("harness.bridge.ack_started_grace must be >= harness.bridge.reconnect_grace")
	}
	if cfg.Checkpoint.IdleThreshold.Duration < 0 {
		return fmt.Errorf("harness.checkpoint.idle_threshold must be >= 0")
	}
	if cfg.Checkpoint.MonitorInterval.Duration <= 0 {
		return fmt.Errorf("harness.checkpoint.monitor_interval must be > 0")
	}
	if cfg.Events.RetentionWindow.Duration == 0 && cfg.Events.RetentionRows == 0 {
		return fmt.Errorf("harness.events.retention_window and harness.events.retention_rows cannot both be zero")
	}
	if cfg.Events.RetentionWindow.Duration < 0 {
		return fmt.Errorf("harness.events.retention_window must be >= 0")
	}
	if cfg.Events.RetentionRows < 0 {
		return fmt.Errorf("harness.events.retention_rows must be >= 0")
	}
	if cfg.Events.EmitOutputBatchMaxRows <= 0 {
		return fmt.Errorf("harness.events.emit_output_batch_max_rows must be > 0")
	}
	if cfg.Events.EmitOutputBatchMaxAge.Duration <= 0 {
		return fmt.Errorf("harness.events.emit_output_batch_max_age must be > 0")
	}
	if cfg.Reaper.FailedRetention.Duration < 0 {
		return fmt.Errorf("harness.reaper.failed_retention must be >= 0")
	}
	if strings.TrimSpace(cfg.Secrets.Root) == "" {
		return fmt.Errorf("harness.secrets.root is required")
	}
	if cfg.Secrets.ReadersGID <= 0 {
		return fmt.Errorf("harness.secrets.readers_gid must be > 0")
	}
	if err := validateSecretsRoot(cfg.Secrets); err != nil {
		return err
	}
	return nil
}

func phase7ConfigWarnings(cfg Phase7Config) []string {
	if cfg.Bridge.HeartbeatInterval.Duration >= cfg.Bridge.LeaseTTL.Duration/2 &&
		cfg.Bridge.HeartbeatInterval.Duration < cfg.Bridge.LeaseTTL.Duration {
		return []string{"harness.bridge.heartbeat_interval is at least half of harness.bridge.lease_ttl"}
	}
	return nil
}

func validateSecretsRoot(cfg SecretsConfig) error {
	info, err := os.Stat(cfg.Root)
	if errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("harness.secrets.root %q must exist", cfg.Root)
	}
	if err != nil {
		return fmt.Errorf("stat harness.secrets.root %q: %w", cfg.Root, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("harness.secrets.root %q must be a directory", cfg.Root)
	}
	if mode := info.Mode().Perm(); mode != 0o750 {
		return fmt.Errorf("harness.secrets.root %q must have mode 0750, got %04o", cfg.Root, mode)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("harness.secrets.root %q group could not be inspected", cfg.Root)
	}
	if int(stat.Gid) != cfg.ReadersGID {
		return fmt.Errorf("harness.secrets.root %q must have group %d, got %d", cfg.Root, cfg.ReadersGID, stat.Gid)
	}
	return nil
}

func cidrPool30Capacity(prefix netip.Prefix) uint64 {
	bits := prefix.Addr().BitLen()
	if prefix.Bits() > 30 || bits < 30 {
		return 0
	}
	return uint64(1) << uint(30-prefix.Bits())
}

func anyHostname(groups ...[]string) bool {
	for _, group := range groups {
		for _, host := range group {
			if strings.TrimSpace(host) == "" {
				return true
			}
			if ip := net.ParseIP(host); ip == nil {
				return true
			}
		}
	}
	return false
}

func validateHosts(field string, values []string) error {
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s contains an empty host", field)
		}
	}
	return nil
}

func normalizeClaudeConfig(cfg ClaudeConfig) ClaudeConfig {
	cfg.ProxyBindURL = defaultString(cfg.ProxyBindURL, "http://0.0.0.0:8082")
	cfg.SandboxBaseURL = defaultString(cfg.SandboxBaseURL, "http://10.200.1.1:8082")
	cfg.APIKey = defaultString(cfg.APIKey, "123")
	cfg.AuthToken = defaultString(cfg.AuthToken, cfg.APIKey)
	cfg.Model = defaultString(cfg.Model, "sonnet")
	cfg.OutputFormat = defaultString(cfg.OutputFormat, "stream-json")
	return cfg
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func intEnv(key string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(key))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func boolEnv(key string) (bool, bool) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return false, false
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, false
	}
	return parsed, true
}

func durationEnv(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return duration
}
