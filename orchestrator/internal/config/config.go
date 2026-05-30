package config

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"harness-platform/orchestrator/internal/agents"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Addr                 string
	SharedSecret         string
	CookieName           string
	SessionRetention     time.Duration
	RepoRoot             string
	RestoreScript        string
	RunscRoot            string
	SessionsRoot         string
	AgentHomesRoot       string
	CheckpointsRoot      string
	BundleRoot           string
	RootFSPath           string
	AgentManifestPath    string
	RequireAgentManifest bool
	DBPath               string
	DefaultAgent         string
	MaxSessions          int
	RunscNetwork         string
	RunscOverlay2        string
	Agents               map[string]AgentConfig
	ModelProfiles        map[string]ModelProfileConfig
	RuntimeProviders     map[string]RuntimeProviderConfig
	Claude               ClaudeConfig
	ModelProxy           ModelProxyConfig
	Phase7               Phase7Config
	Warnings             []string
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

type ModelProxyConfig struct {
	BindURL        string `yaml:"bind_url"`
	SandboxBaseURL string `yaml:"sandbox_base_url"`
	BindPort       int    `yaml:"-"`
}

type AgentConfig struct {
	Enabled                    *bool  `yaml:"enabled" json:"enabled,omitempty"`
	DriverID                   string `yaml:"driver_id" json:"driver_id,omitempty"`
	ModelProfile               string `yaml:"model_profile" json:"model_profile,omitempty"`
	RuntimeProvider            string `yaml:"runtime_provider" json:"runtime_provider,omitempty"`
	DisableNonessentialTraffic *bool  `yaml:"disable_nonessential_traffic" json:"disable_nonessential_traffic,omitempty"`
}

type ModelProfileConfig struct {
	Enabled  *bool  `yaml:"enabled" json:"enabled,omitempty"`
	Provider string `yaml:"provider" json:"provider,omitempty"`
	Model    string `yaml:"model" json:"model,omitempty"`
	ProxyRef string `yaml:"proxy_ref" json:"proxy_ref,omitempty"`
}

type RuntimeProviderConfig struct {
	Enabled    *bool  `yaml:"enabled" json:"enabled,omitempty"`
	ProviderID string `yaml:"provider_id" json:"provider_id,omitempty"`
	ProfileID  string `yaml:"profile_id" json:"profile_id,omitempty"`
}

const defaultModelProxyBindPort = 8082
const defaultModelProxyBindURL = "http://0.0.0.0:8082"
const defaultSandboxModelProxyHost = "harness-model-proxy.internal"
const defaultSandboxModelProxyBaseURL = "http://harness-model-proxy.internal:8082"

type Phase7Config struct {
	DefaultAgent         string                           `yaml:"default_agent"`
	Agents               map[string]AgentConfig           `yaml:"agents"`
	ModelProfiles        map[string]ModelProfileConfig    `yaml:"model_profiles"`
	RuntimeProviders     map[string]RuntimeProviderConfig `yaml:"runtime_providers"`
	RunDir               string                           `yaml:"run_dir"`
	SessionRetention     Duration                         `yaml:"session_retention"`
	MaxSessions          int                              `yaml:"max_sessions"`
	Network              NetworkConfig                    `yaml:"network"`
	Events               EventsConfig                     `yaml:"events"`
	Probe                ProbeConfig                      `yaml:"probe"`
	Bridge               BridgeConfig                     `yaml:"bridge"`
	Checkpoint           CheckpointConfig                 `yaml:"checkpoint"`
	Reaper               ReaperConfig                     `yaml:"reaper"`
	SandboxIdentity      SandboxIdentity                  `yaml:"sandbox_identity"`
	ProxyServiceIdentity ProxyServiceIdentity             `yaml:"proxy_service_identity"`
	ModelProxy           ModelProxyConfig                 `yaml:"model_proxy"`
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
	FailedRetention          Duration `yaml:"failed_retention"`
	CheckpointImageRetention Duration `yaml:"checkpoint_image_retention"`
}

type SandboxIdentity struct {
	UID              int   `yaml:"uid"`
	GID              int   `yaml:"gid"`
	SupplementalGIDs []int `yaml:"supplemental_gids"`
}

type ProxyServiceIdentity struct {
	UID int `yaml:"uid"`
	GID int `yaml:"gid"`
}

type Phase8IsolationRoots struct {
	SessionsRoot           string
	AgentHomesRoot         string
	RunDir                 string
	CheckpointsRoot        string
	PreparedBundleRoot     string
	RootFSPath             string
	DBPath                 string
	SchemaPackRoot         string
	DataVolumeEvidenceRoot string
	ProxyInternalRoot      string
	ProviderCredentialRoot string
}

type CanonicalPhase8IsolationRoots struct {
	SessionsRoot           string
	AgentHomesRoot         string
	RunDir                 string
	CheckpointsRoot        string
	PreparedBundleRoot     string
	RootFSPath             string
	DBStateRoot            string
	SchemaPackRoot         string
	DataVolumeEvidenceRoot string
	ProxyInternalRoot      string
	ProviderCredentialRoot string
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
	sessionRetention, err := sessionRetentionEnv(projectConfig.Phase7.SessionRetention.Duration)
	if err != nil {
		return Config{}, err
	}
	defaultDriver, err := agents.CanonicalDriverID(getenv("HARNESS_DEFAULT_AGENT", projectConfig.Phase7.DefaultAgent))
	if err != nil {
		return Config{}, fmt.Errorf("HARNESS_DEFAULT_AGENT: %w", err)
	}
	if err := validateDefaultAgentDriver(defaultDriver, projectConfig.Phase7.Agents); err != nil {
		return Config{}, fmt.Errorf("HARNESS_DEFAULT_AGENT: %w", err)
	}
	maxSessions := intEnv("HARNESS_MAX_SESSIONS", projectConfig.Phase7.MaxSessions)
	cfg := Config{
		Addr:                 getenv("HARNESS_ORCHESTRATOR_ADDR", ":8090"),
		SharedSecret:         os.Getenv("HARNESS_LAB_PASSWORD"),
		CookieName:           getenv("HARNESS_COOKIE_NAME", "harness_auth"),
		SessionRetention:     sessionRetention,
		RepoRoot:             getenv("HARNESS_REPO_ROOT", repoRoot),
		RestoreScript:        getenv("HARNESS_RESTORE_SCRIPT", filepath.Join(repoRoot, "bundle", "restore-sandbox.sh")),
		RunscRoot:            getenv("RUNSC_ROOT", "/var/lib/harness/runsc"),
		SessionsRoot:         sessionsRoot,
		AgentHomesRoot:       getenv("HARNESS_AGENT_HOMES_ROOT", "/var/lib/harness/agent-homes"),
		CheckpointsRoot:      getenv("HARNESS_CHECKPOINTS_ROOT", "/var/lib/harness/checkpoints"),
		BundleRoot:           getenv("HARNESS_BUNDLE_ROOT", filepath.Join(repoRoot, "bundle", "out")),
		RootFSPath:           getenv("HARNESS_ROOTFS_PATH", filepath.Join(repoRoot, "sandbox-image", "rootfs")),
		AgentManifestPath:    os.Getenv("HARNESS_AGENT_IMAGE_MANIFEST_PATH"),
		RequireAgentManifest: true,
		DBPath:               getenv("HARNESS_DB_PATH", "/var/lib/harness/state/orchestrator.db"),
		DefaultAgent:         string(defaultDriver),
		MaxSessions:          maxSessions,
		RunscNetwork:         "sandbox",
		RunscOverlay2:        "none",
		Claude:               projectConfig.Claude,
		ModelProxy:           projectConfig.Phase7.ModelProxy,
		Phase7:               projectConfig.Phase7,
	}
	cfg.Phase7.SessionRetention = Duration{Duration: sessionRetention}
	cfg.Phase7.MaxSessions = maxSessions
	cfg.Phase7.DefaultAgent = string(defaultDriver)
	cfg.Phase7 = normalizePhase7Config(cfg.Phase7)
	cfg.Agents = cloneAgentConfigs(cfg.Phase7.Agents)
	cfg.ModelProfiles = cloneModelProfileConfigs(cfg.Phase7.ModelProfiles)
	cfg.RuntimeProviders = cloneRuntimeProviderConfigs(cfg.Phase7.RuntimeProviders)
	if value, ok := boolEnv("HARNESS_AUTO_CHECKPOINT_ENABLED"); ok {
		cfg.Phase7.Checkpoint.AutoEnabled = value
	}
	if err := validatePhase7Config(cfg.Phase7); err != nil {
		return Config{}, err
	}
	cfg.ModelProxy = cfg.Phase7.ModelProxy
	cfg.Claude = normalizeClaudeConfig(cfg.Claude)
	cfg.Claude = syncClaudeModelProxy(cfg.Claude, cfg.ModelProxy)
	cfg.Claude = syncClaudeDeploymentConfig(cfg.Claude, cfg.Phase7)
	cfg.Warnings = phase7ConfigWarnings(cfg.Phase7)
	return cfg, nil
}

func validateDefaultAgentDriver(driverID agents.ID, agentConfigs map[string]AgentConfig) error {
	spec, ok := agents.DriverSpecFor(string(driverID))
	if !ok {
		return fmt.Errorf("unsupported driver %q", driverID)
	}
	if spec.Kind != agents.DriverKindAgent {
		return fmt.Errorf("default agent must be an agent-capable driver, got %q", driverID)
	}
	if _, _, ok := EnabledAgentConfigForDriver(agentConfigs, string(driverID)); !ok {
		return fmt.Errorf("default agent %q is not enabled in harness.agents", driverID)
	}
	return nil
}

type projectConfig struct {
	Phase7 Phase7Config
	Claude ClaudeConfig
}

func loadProjectConfig(path string) (projectConfig, error) {
	cfg := projectConfig{
		Phase7: defaultPhase7Config(),
		Claude: defaultClaudeConfig(),
	}

	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return finalizeProjectConfig(path, cfg)
	}
	if err != nil {
		return cfg, err
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return finalizeProjectConfig(path, cfg)
	}

	var target struct {
		Harness Phase7Config `yaml:"harness"`
	}
	target.Harness = cfg.Phase7
	target.Harness.ModelProxy.SandboxBaseURL = ""
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&target); err != nil {
		return cfg, fmt.Errorf("load %s: %w", path, err)
	}
	cfg.Phase7 = target.Harness
	return finalizeProjectConfig(path, cfg)
}

func finalizeProjectConfig(path string, cfg projectConfig) (projectConfig, error) {
	cfg.Claude = normalizeClaudeConfig(cfg.Claude)
	cfg.Phase7 = normalizePhase7Config(cfg.Phase7)
	if err := validateDeploymentConfig(cfg.Phase7); err != nil {
		return cfg, fmt.Errorf("load %s: %w", path, err)
	}
	if err := validateModelProxyConfig(cfg.Phase7.ModelProxy); err != nil {
		return cfg, fmt.Errorf("load %s: %w", path, err)
	}
	cfg.Claude = syncClaudeModelProxy(cfg.Claude, cfg.Phase7.ModelProxy)
	cfg.Claude = syncClaudeDeploymentConfig(cfg.Claude, cfg.Phase7)
	return cfg, nil
}

func defaultPhase7Config() Phase7Config {
	return Phase7Config{
		DefaultAgent:     string(agents.ClaudeCode),
		Agents:           defaultAgentConfigs(defaultClaudeConfig()),
		ModelProfiles:    defaultModelProfileConfigs(defaultClaudeConfig()),
		RuntimeProviders: defaultRuntimeProviderConfigs(),
		RunDir:           "/var/lib/harness/run",
		SessionRetention: Duration{Duration: 0},
		MaxSessions:      30,
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
			FailedRetention:          Duration{Duration: 10 * time.Minute},
			CheckpointImageRetention: Duration{Duration: 720 * time.Hour},
		},
		SandboxIdentity: SandboxIdentity{
			UID: 65534,
			GID: 65534,
		},
		ProxyServiceIdentity: ProxyServiceIdentity{
			UID: os.Geteuid(),
			GID: os.Getegid(),
		},
		ModelProxy: ModelProxyConfig{
			BindURL:        defaultModelProxyBindURL,
			SandboxBaseURL: defaultSandboxModelProxyBaseURL,
			BindPort:       8082,
		},
	}
}

func normalizePhase7Config(cfg Phase7Config) Phase7Config {
	cfg.DefaultAgent = defaultString(cfg.DefaultAgent, string(agents.ClaudeCode))
	if canonical, err := agents.CanonicalDriverID(cfg.DefaultAgent); err == nil {
		cfg.DefaultAgent = string(canonical)
	}
	cfg.Agents = normalizeAgentConfigs(cfg.Agents, defaultClaudeConfig())
	cfg.ModelProfiles = normalizeModelProfileConfigs(cfg.ModelProfiles, defaultClaudeConfig())
	cfg.RuntimeProviders = normalizeRuntimeProviderConfigs(cfg.RuntimeProviders)
	cfg.SandboxIdentity = NormalizeSandboxIdentity(cfg.SandboxIdentity)
	cfg.ModelProxy = normalizeModelProxyConfig(cfg.ModelProxy)
	return cfg
}

func NormalizeSandboxIdentity(identity SandboxIdentity) SandboxIdentity {
	normalized := identity
	normalized.SupplementalGIDs = append([]int(nil), identity.SupplementalGIDs...)
	sort.Ints(normalized.SupplementalGIDs)
	return normalized
}

func validatePhase7Config(cfg Phase7Config) error {
	if strings.TrimSpace(cfg.RunDir) == "" {
		return fmt.Errorf("harness.run_dir is required")
	}
	if cfg.SessionRetention.Duration < 0 {
		return fmt.Errorf("harness.session_retention must be >= 0")
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
	if cfg.Reaper.CheckpointImageRetention.Duration < 0 {
		return fmt.Errorf("harness.reaper.checkpoint_image_retention must be >= 0")
	}
	if err := ValidateSandboxIdentity(cfg.SandboxIdentity); err != nil {
		return err
	}
	if err := ValidateProxyServiceIdentity(cfg.ProxyServiceIdentity); err != nil {
		return err
	}
	if err := validateModelProxyConfig(cfg.ModelProxy); err != nil {
		return err
	}
	return nil
}

func ValidateSandboxIdentity(identity SandboxIdentity) error {
	if identity.UID <= 0 {
		return fmt.Errorf("harness.sandbox_identity.uid must be > 0")
	}
	if identity.GID <= 0 {
		return fmt.Errorf("harness.sandbox_identity.gid must be > 0")
	}
	seen := map[int]struct{}{}
	for _, gid := range identity.SupplementalGIDs {
		if gid <= 0 {
			return fmt.Errorf("harness.sandbox_identity.supplemental_gids must contain only positive non-root gids")
		}
		if _, ok := seen[gid]; ok {
			return fmt.Errorf("harness.sandbox_identity.supplemental_gids contains duplicate gid %d", gid)
		}
		seen[gid] = struct{}{}
	}
	return nil
}

func ValidateProxyServiceIdentity(identity ProxyServiceIdentity) error {
	if identity.UID < 0 {
		return fmt.Errorf("harness.proxy_service_identity.uid must be >= 0")
	}
	if identity.GID < 0 {
		return fmt.Errorf("harness.proxy_service_identity.gid must be >= 0")
	}
	return nil
}

func (c Config) Phase8IsolationRoots() Phase8IsolationRoots {
	schemaPackRoot := filepath.Join(c.RepoRoot, "schema-pack")
	if _, err := os.Stat(schemaPackRoot); err != nil {
		schemaPackRoot = ""
	}
	return Phase8IsolationRoots{
		SessionsRoot:           c.SessionsRoot,
		AgentHomesRoot:         c.AgentHomesRoot,
		RunDir:                 c.Phase7.RunDir,
		CheckpointsRoot:        c.CheckpointsRoot,
		PreparedBundleRoot:     c.BundleRoot,
		RootFSPath:             c.RootFSPath,
		DBPath:                 c.DBPath,
		SchemaPackRoot:         schemaPackRoot,
		DataVolumeEvidenceRoot: filepath.Join(filepath.Dir(c.DBPath), "volume-evidence"),
		ProxyInternalRoot:      filepath.Join(c.Phase7.RunDir, "proxy-internal"),
	}
}

func ValidatePhase8IsolationRoots(roots Phase8IsolationRoots) (CanonicalPhase8IsolationRoots, error) {
	canonical := CanonicalPhase8IsolationRoots{}
	required := []struct {
		label string
		value string
		set   func(string)
	}{
		{label: "sessions root", value: roots.SessionsRoot, set: func(path string) { canonical.SessionsRoot = path }},
		{label: "agent homes root", value: roots.AgentHomesRoot, set: func(path string) { canonical.AgentHomesRoot = path }},
		{label: "run dir", value: roots.RunDir, set: func(path string) { canonical.RunDir = path }},
		{label: "checkpoints root", value: roots.CheckpointsRoot, set: func(path string) { canonical.CheckpointsRoot = path }},
		{label: "prepared bundle root", value: roots.PreparedBundleRoot, set: func(path string) { canonical.PreparedBundleRoot = path }},
		{label: "rootfs path", value: roots.RootFSPath, set: func(path string) { canonical.RootFSPath = path }},
	}
	for _, root := range required {
		path, err := canonicalPhase8Root(root.label, root.value)
		if err != nil {
			return CanonicalPhase8IsolationRoots{}, err
		}
		root.set(path)
	}
	dbPath, err := canonicalPhase8Root("db path", roots.DBPath)
	if err != nil {
		return CanonicalPhase8IsolationRoots{}, err
	}
	canonical.DBStateRoot = filepath.Dir(dbPath)
	if strings.TrimSpace(roots.SchemaPackRoot) != "" {
		canonical.SchemaPackRoot, err = canonicalPhase8Root("schema pack root", roots.SchemaPackRoot)
		if err != nil {
			return CanonicalPhase8IsolationRoots{}, err
		}
	}
	if strings.TrimSpace(roots.DataVolumeEvidenceRoot) != "" {
		canonical.DataVolumeEvidenceRoot, err = canonicalPhase8Root("data volume evidence root", roots.DataVolumeEvidenceRoot)
		if err != nil {
			return CanonicalPhase8IsolationRoots{}, err
		}
	} else {
		canonical.DataVolumeEvidenceRoot = filepath.Join(canonical.DBStateRoot, "volume-evidence")
	}
	if strings.TrimSpace(roots.ProxyInternalRoot) != "" {
		canonical.ProxyInternalRoot, err = canonicalPhase8Root("proxy internal root", roots.ProxyInternalRoot)
		if err != nil {
			return CanonicalPhase8IsolationRoots{}, err
		}
	} else {
		canonical.ProxyInternalRoot = filepath.Join(canonical.RunDir, "proxy-internal")
	}
	if strings.TrimSpace(roots.ProviderCredentialRoot) != "" {
		canonical.ProviderCredentialRoot, err = canonicalPhase8Root("provider credential root", roots.ProviderCredentialRoot)
		if err != nil {
			return CanonicalPhase8IsolationRoots{}, err
		}
	}

	topLevel := []phase8Root{
		{label: "sessions root", path: canonical.SessionsRoot},
		{label: "agent homes root", path: canonical.AgentHomesRoot},
		{label: "run dir", path: canonical.RunDir},
		{label: "checkpoints root", path: canonical.CheckpointsRoot},
		{label: "prepared bundle root", path: canonical.PreparedBundleRoot},
		{label: "rootfs path", path: canonical.RootFSPath},
		{label: "db state root", path: canonical.DBStateRoot},
	}
	if canonical.SchemaPackRoot != "" {
		topLevel = append(topLevel, phase8Root{label: "schema pack root", path: canonical.SchemaPackRoot})
	}
	if canonical.ProviderCredentialRoot != "" {
		topLevel = append(topLevel, phase8Root{label: "provider credential root", path: canonical.ProviderCredentialRoot})
	}
	if !phase8PathWithin(canonical.DataVolumeEvidenceRoot, canonical.DBStateRoot) {
		topLevel = append(topLevel, phase8Root{label: "data volume evidence root", path: canonical.DataVolumeEvidenceRoot})
	}
	if !phase8PathWithin(canonical.ProxyInternalRoot, canonical.RunDir) {
		topLevel = append(topLevel, phase8Root{label: "proxy internal root", path: canonical.ProxyInternalRoot})
	}
	if err := validatePhase8TopLevelDisjoint(topLevel); err != nil {
		return CanonicalPhase8IsolationRoots{}, err
	}
	if err := validateReservedHostRoot("data volume evidence root", canonical.DataVolumeEvidenceRoot, canonical, canonical.DBStateRoot); err != nil {
		return CanonicalPhase8IsolationRoots{}, err
	}
	if err := validateReservedHostRoot("proxy internal root", canonical.ProxyInternalRoot, canonical, canonical.RunDir); err != nil {
		return CanonicalPhase8IsolationRoots{}, err
	}
	return canonical, nil
}

type phase8Root struct {
	label string
	path  string
}

func canonicalPhase8Root(label, path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", fmt.Errorf("phase8 %s is required", label)
	}
	if !filepath.IsAbs(path) {
		return "", fmt.Errorf("phase8 %s %q must be absolute", label, path)
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("phase8 %s %q must be absolute: %w", label, path, err)
	}
	cleaned := filepath.Clean(absolute)
	if cleaned == string(filepath.Separator) {
		return "", fmt.Errorf("phase8 %s must not be filesystem root", label)
	}
	if resolved, err := filepath.EvalSymlinks(cleaned); err == nil {
		return filepath.Clean(resolved), nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return "", fmt.Errorf("resolve phase8 %s %q: %w", label, cleaned, err)
	}
	existing, missing, err := deepestExistingRoot(cleaned)
	if err != nil {
		return "", fmt.Errorf("resolve phase8 %s %q: %w", label, cleaned, err)
	}
	if existing == "" {
		return cleaned, nil
	}
	resolved, err := filepath.EvalSymlinks(existing)
	if err != nil {
		return "", fmt.Errorf("resolve phase8 %s existing prefix %q: %w", label, existing, err)
	}
	return filepath.Clean(filepath.Join(append([]string{resolved}, missing...)...)), nil
}

func deepestExistingRoot(path string) (string, []string, error) {
	var missing []string
	for current := filepath.Clean(path); ; current = filepath.Dir(current) {
		if _, err := os.Lstat(current); err == nil {
			for i, j := 0, len(missing)-1; i < j; i, j = i+1, j-1 {
				missing[i], missing[j] = missing[j], missing[i]
			}
			return current, missing, nil
		} else if !errors.Is(err, fs.ErrNotExist) {
			return "", nil, err
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", nil, nil
		}
		missing = append(missing, filepath.Base(current))
	}
}

func validatePhase8TopLevelDisjoint(roots []phase8Root) error {
	for i := 0; i < len(roots); i++ {
		for j := i + 1; j < len(roots); j++ {
			if phase8RootsOverlap(roots[i].path, roots[j].path) {
				return fmt.Errorf("phase8 %s %q overlaps %s %q", roots[i].label, roots[i].path, roots[j].label, roots[j].path)
			}
		}
	}
	return nil
}

func validateReservedHostRoot(label, path string, roots CanonicalPhase8IsolationRoots, allowedParent string) error {
	if !phase8PathWithin(path, allowedParent) && phase8RootsOverlap(path, allowedParent) {
		return fmt.Errorf("phase8 %s %q must not contain reserved parent %q", label, path, allowedParent)
	}
	sandboxBindable := []phase8Root{
		{label: "sessions root", path: roots.SessionsRoot},
		{label: "agent homes root", path: roots.AgentHomesRoot},
		{label: "run control root", path: filepath.Join(roots.RunDir, "control")},
		{label: "run bridge root", path: filepath.Join(roots.RunDir, "bridge")},
		{label: "run network root", path: filepath.Join(roots.RunDir, "network")},
	}
	if roots.SchemaPackRoot != "" {
		sandboxBindable = append(sandboxBindable, phase8Root{label: "schema pack root", path: roots.SchemaPackRoot})
	}
	for _, root := range sandboxBindable {
		if phase8RootsOverlap(path, root.path) {
			return fmt.Errorf("phase8 %s %q overlaps sandbox-bindable %s %q", label, path, root.label, root.path)
		}
	}
	if path == roots.RunDir || path == roots.DBStateRoot {
		return fmt.Errorf("phase8 %s must be a reserved subroot, got top-level root %q", label, path)
	}
	return nil
}

func phase8RootsOverlap(a, b string) bool {
	return phase8PathWithin(a, b) || phase8PathWithin(b, a)
}

func phase8PathWithin(path, root string) bool {
	path = filepath.Clean(path)
	root = filepath.Clean(root)
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

func phase7ConfigWarnings(cfg Phase7Config) []string {
	if cfg.Bridge.HeartbeatInterval.Duration >= cfg.Bridge.LeaseTTL.Duration/2 &&
		cfg.Bridge.HeartbeatInterval.Duration < cfg.Bridge.LeaseTTL.Duration {
		return []string{"harness.bridge.heartbeat_interval is at least half of harness.bridge.lease_ttl"}
	}
	return nil
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

func defaultClaudeConfig() ClaudeConfig {
	return ClaudeConfig{
		ProxyBindURL:               defaultModelProxyBindURL,
		SandboxBaseURL:             defaultSandboxModelProxyBaseURL,
		APIKey:                     "123",
		AuthToken:                  "123",
		Model:                      "sonnet",
		OutputFormat:               "stream-json",
		DisableNonessentialTraffic: true,
	}
}

func defaultAgentConfigs(claude ClaudeConfig) map[string]AgentConfig {
	claude = normalizeClaudeConfig(claude)
	return map[string]AgentConfig{
		string(agents.ClaudeCode): {
			Enabled:                    boolPtr(true),
			DriverID:                   string(agents.ClaudeCode),
			ModelProfile:               defaultModelProfileID,
			RuntimeProvider:            "local_runsc",
			DisableNonessentialTraffic: boolPtr(claude.DisableNonessentialTraffic),
		},
		string(agents.Shell): {
			Enabled:         boolPtr(true),
			DriverID:        string(agents.Shell),
			RuntimeProvider: "local_runsc",
		},
		string(agents.Pi): {
			Enabled:                    boolPtr(true),
			DriverID:                   string(agents.Pi),
			ModelProfile:               defaultModelProfileID,
			RuntimeProvider:            "local_runsc",
			DisableNonessentialTraffic: boolPtr(claude.DisableNonessentialTraffic),
		},
	}
}

// Default model-access identifiers. The legacy Claude migration, the
// model-profile validator, and the server-side deployment resolver all
// reference these, so they must stay in sync.
const (
	defaultModelProfileID = "anthropic_default"
	defaultModelProvider  = "anthropic_messages"
	// DefaultModelProxyRef is the only proxy reference accepted on model
	// profiles; it is exported because the server re-validates the same
	// invariant when resolving a deployment.
	DefaultModelProxyRef = "model_proxy"
)

func defaultModelProfileConfigs(claude ClaudeConfig) map[string]ModelProfileConfig {
	claude = normalizeClaudeConfig(claude)
	return map[string]ModelProfileConfig{
		defaultModelProfileID: {
			Enabled:  boolPtr(true),
			Provider: defaultModelProvider,
			Model:    claude.Model,
			ProxyRef: DefaultModelProxyRef,
		},
	}
}

func defaultRuntimeProviderConfigs() map[string]RuntimeProviderConfig {
	return map[string]RuntimeProviderConfig{
		"local_runsc": {
			Enabled:    boolPtr(true),
			ProviderID: "local_runsc",
			ProfileID:  "local_runsc_default",
		},
	}
}

func normalizeAgentConfigs(raw map[string]AgentConfig, claude ClaudeConfig) map[string]AgentConfig {
	defaults := defaultAgentConfigs(claude)
	if len(raw) == 0 {
		return cloneAgentConfigs(defaults)
	}
	normalized := cloneAgentConfigs(defaults)
	for id, cfg := range raw {
		key := strings.TrimSpace(id)
		if key == "" {
			continue
		}
		base, ok := normalized[key]
		if !ok {
			base = AgentConfig{}
		}
		if cfg.Enabled != nil {
			base.Enabled = boolPtr(*cfg.Enabled)
		}
		if strings.TrimSpace(cfg.DriverID) != "" {
			base.DriverID = strings.TrimSpace(cfg.DriverID)
		} else if strings.TrimSpace(base.DriverID) == "" {
			base.DriverID = key
		}
		if canonical, err := agents.CanonicalDriverID(base.DriverID); err == nil {
			base.DriverID = string(canonical)
		}
		if strings.TrimSpace(cfg.ModelProfile) != "" {
			base.ModelProfile = strings.TrimSpace(cfg.ModelProfile)
		}
		if strings.TrimSpace(cfg.RuntimeProvider) != "" {
			base.RuntimeProvider = strings.TrimSpace(cfg.RuntimeProvider)
		}
		if cfg.DisableNonessentialTraffic != nil {
			base.DisableNonessentialTraffic = boolPtr(*cfg.DisableNonessentialTraffic)
		}
		if base.Enabled == nil {
			base.Enabled = boolPtr(false)
		}
		normalized[key] = base
	}
	return normalized
}

func normalizeModelProfileConfigs(raw map[string]ModelProfileConfig, claude ClaudeConfig) map[string]ModelProfileConfig {
	defaults := defaultModelProfileConfigs(claude)
	if len(raw) == 0 {
		return cloneModelProfileConfigs(defaults)
	}
	normalized := cloneModelProfileConfigs(defaults)
	for id, cfg := range raw {
		key := strings.TrimSpace(id)
		if key == "" {
			continue
		}
		base, ok := normalized[key]
		if !ok {
			base = ModelProfileConfig{}
		}
		if cfg.Enabled != nil {
			base.Enabled = boolPtr(*cfg.Enabled)
		}
		if strings.TrimSpace(cfg.Provider) != "" {
			base.Provider = strings.TrimSpace(cfg.Provider)
		}
		if strings.TrimSpace(cfg.Model) != "" {
			base.Model = strings.TrimSpace(cfg.Model)
		}
		if strings.TrimSpace(cfg.ProxyRef) != "" {
			base.ProxyRef = strings.TrimSpace(cfg.ProxyRef)
		}
		if base.Enabled == nil {
			base.Enabled = boolPtr(false)
		}
		normalized[key] = base
	}
	return normalized
}

func normalizeRuntimeProviderConfigs(raw map[string]RuntimeProviderConfig) map[string]RuntimeProviderConfig {
	defaults := defaultRuntimeProviderConfigs()
	if len(raw) == 0 {
		return cloneRuntimeProviderConfigs(defaults)
	}
	normalized := cloneRuntimeProviderConfigs(defaults)
	for id, cfg := range raw {
		key := strings.TrimSpace(id)
		if key == "" {
			continue
		}
		base, ok := normalized[key]
		if !ok {
			base = RuntimeProviderConfig{}
		}
		if cfg.Enabled != nil {
			base.Enabled = boolPtr(*cfg.Enabled)
		}
		if strings.TrimSpace(cfg.ProviderID) != "" {
			base.ProviderID = strings.TrimSpace(cfg.ProviderID)
		} else if strings.TrimSpace(base.ProviderID) == "" {
			base.ProviderID = key
		}
		if strings.TrimSpace(cfg.ProfileID) != "" {
			base.ProfileID = strings.TrimSpace(cfg.ProfileID)
		}
		if base.Enabled == nil {
			base.Enabled = boolPtr(false)
		}
		normalized[key] = base
	}
	return normalized
}

func validateDeploymentConfig(cfg Phase7Config) error {
	defaultDriver, err := agents.CanonicalDriverID(cfg.DefaultAgent)
	if err != nil {
		return fmt.Errorf("harness.default_agent: %w", err)
	}
	if err := validateDefaultAgentDriver(defaultDriver, cfg.Agents); err != nil {
		return fmt.Errorf("harness.default_agent: %w", err)
	}
	if len(cfg.Agents) == 0 {
		return fmt.Errorf("harness.agents must be non-empty")
	}
	for id, agentCfg := range cfg.Agents {
		driverID, err := agents.CanonicalDriverID(defaultString(agentCfg.DriverID, id))
		if err != nil {
			return fmt.Errorf("harness.agents.%s.driver_id: %w", id, err)
		}
		spec, ok := agents.DriverSpecFor(string(driverID))
		if !ok {
			return fmt.Errorf("harness.agents.%s.driver_id has no registered driver spec", id)
		}
		if strings.TrimSpace(agentCfg.RuntimeProvider) == "" {
			return fmt.Errorf("harness.agents.%s.runtime_provider is required", id)
		}
		runtimeCfg, ok := cfg.RuntimeProviders[agentCfg.RuntimeProvider]
		if !ok {
			return fmt.Errorf("harness.agents.%s.runtime_provider %q is not defined", id, agentCfg.RuntimeProvider)
		}
		if enabled(agentCfg.Enabled) && !enabled(runtimeCfg.Enabled) {
			return fmt.Errorf("harness.agents.%s.runtime_provider %q is disabled", id, agentCfg.RuntimeProvider)
		}
		providerID := defaultString(runtimeCfg.ProviderID, agentCfg.RuntimeProvider)
		if err := agents.EnsureDriverSupportedByProvider(string(driverID), providerID); err != nil {
			return fmt.Errorf("harness.agents.%s.runtime_provider: %w", id, err)
		}
		if spec.ModelAccess {
			if strings.TrimSpace(agentCfg.ModelProfile) == "" {
				return fmt.Errorf("harness.agents.%s.model_profile is required for model-access drivers", id)
			}
			profile, ok := cfg.ModelProfiles[agentCfg.ModelProfile]
			if !ok {
				return fmt.Errorf("harness.agents.%s.model_profile %q is not defined", id, agentCfg.ModelProfile)
			}
			if enabled(agentCfg.Enabled) && !enabled(profile.Enabled) {
				return fmt.Errorf("harness.agents.%s.model_profile %q is disabled", id, agentCfg.ModelProfile)
			}
			if strings.TrimSpace(profile.Model) == "" {
				return fmt.Errorf("harness.model_profiles.%s.model is required", agentCfg.ModelProfile)
			}
			if strings.TrimSpace(profile.ProxyRef) != DefaultModelProxyRef {
				return fmt.Errorf("harness.model_profiles.%s.proxy_ref must be %s", agentCfg.ModelProfile, DefaultModelProxyRef)
			}
		}
	}
	return nil
}

func syncClaudeDeploymentConfig(claude ClaudeConfig, phase7 Phase7Config) ClaudeConfig {
	if _, agentCfg, ok := EnabledAgentConfigForDriver(phase7.Agents, string(agents.ClaudeCode)); ok {
		if agentCfg.DisableNonessentialTraffic != nil {
			claude.DisableNonessentialTraffic = *agentCfg.DisableNonessentialTraffic
		}
		if profile, ok := phase7.ModelProfiles[agentCfg.ModelProfile]; ok && strings.TrimSpace(profile.Model) != "" {
			claude.Model = strings.TrimSpace(profile.Model)
		}
	}
	return claude
}

// EnabledAgentConfigForDriver returns the agent-config key and config of the
// first enabled agent (in sorted key order) whose canonical driver matches
// driverID.
func EnabledAgentConfigForDriver(agentConfigs map[string]AgentConfig, driverID string) (string, AgentConfig, bool) {
	canonical, err := agents.CanonicalDriverID(driverID)
	if err != nil {
		return "", AgentConfig{}, false
	}
	keys := make([]string, 0, len(agentConfigs))
	for key := range agentConfigs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		cfg := agentConfigs[key]
		if !enabled(cfg.Enabled) {
			continue
		}
		candidate, err := agents.CanonicalDriverID(defaultString(cfg.DriverID, key))
		if err == nil && candidate == canonical {
			return key, cfg, true
		}
	}
	return "", AgentConfig{}, false
}

func (c Config) DeploymentAgents() map[string]AgentConfig {
	return normalizeAgentConfigs(c.Agents, c.Claude)
}

func (c Config) DeploymentModelProfiles() map[string]ModelProfileConfig {
	return normalizeModelProfileConfigs(c.ModelProfiles, c.Claude)
}

func (c Config) DeploymentRuntimeProviders() map[string]RuntimeProviderConfig {
	return normalizeRuntimeProviderConfigs(c.RuntimeProviders)
}

func cloneAgentConfigs(input map[string]AgentConfig) map[string]AgentConfig {
	if input == nil {
		return nil
	}
	output := make(map[string]AgentConfig, len(input))
	for key, value := range input {
		output[key] = cloneAgentConfig(value)
	}
	return output
}

func cloneAgentConfig(value AgentConfig) AgentConfig {
	if value.Enabled != nil {
		value.Enabled = boolPtr(*value.Enabled)
	}
	if value.DisableNonessentialTraffic != nil {
		value.DisableNonessentialTraffic = boolPtr(*value.DisableNonessentialTraffic)
	}
	return value
}

func cloneModelProfileConfigs(input map[string]ModelProfileConfig) map[string]ModelProfileConfig {
	if input == nil {
		return nil
	}
	output := make(map[string]ModelProfileConfig, len(input))
	for key, value := range input {
		if value.Enabled != nil {
			value.Enabled = boolPtr(*value.Enabled)
		}
		output[key] = value
	}
	return output
}

func cloneRuntimeProviderConfigs(input map[string]RuntimeProviderConfig) map[string]RuntimeProviderConfig {
	if input == nil {
		return nil
	}
	output := make(map[string]RuntimeProviderConfig, len(input))
	for key, value := range input {
		if value.Enabled != nil {
			value.Enabled = boolPtr(*value.Enabled)
		}
		output[key] = value
	}
	return output
}

func enabled(value *bool) bool {
	return value != nil && *value
}

func boolPtr(value bool) *bool {
	return &value
}

func normalizeClaudeConfig(cfg ClaudeConfig) ClaudeConfig {
	cfg.ProxyBindURL = defaultString(cfg.ProxyBindURL, defaultModelProxyBindURL)
	cfg.SandboxBaseURL = defaultString(cfg.SandboxBaseURL, defaultSandboxModelProxyBaseURLForBindURL(cfg.ProxyBindURL))
	cfg.APIKey = defaultString(cfg.APIKey, "123")
	cfg.AuthToken = defaultString(cfg.AuthToken, cfg.APIKey)
	cfg.Model = defaultString(cfg.Model, "sonnet")
	cfg.OutputFormat = defaultString(cfg.OutputFormat, "stream-json")
	return cfg
}

func syncClaudeModelProxy(claude ClaudeConfig, modelProxy ModelProxyConfig) ClaudeConfig {
	claude.ProxyBindURL = modelProxy.BindURL
	claude.SandboxBaseURL = modelProxy.SandboxBaseURL
	return claude
}

func normalizeModelProxyConfig(cfg ModelProxyConfig) ModelProxyConfig {
	cfg.BindURL = defaultString(cfg.BindURL, defaultModelProxyBindURL)
	if port, err := parseModelProxyBindPort(cfg.BindURL); err == nil {
		cfg.BindPort = port
	} else {
		cfg.BindPort = 0
	}
	cfg.SandboxBaseURL = defaultString(cfg.SandboxBaseURL, defaultSandboxModelProxyBaseURLForPort(cfg.BindPort))
	return cfg
}

func validateModelProxyConfig(cfg ModelProxyConfig) error {
	bindPort, err := parseModelProxyBindPort(cfg.BindURL)
	if err != nil {
		return err
	}
	return validateModelProxySandboxBaseURL(cfg.SandboxBaseURL, bindPort)
}

func defaultSandboxModelProxyBaseURLForBindURL(bindURL string) string {
	port, err := parseModelProxyBindPort(bindURL)
	if err != nil {
		return defaultSandboxModelProxyBaseURL
	}
	return defaultSandboxModelProxyBaseURLForPort(port)
}

func defaultSandboxModelProxyBaseURLForPort(port int) string {
	if port <= 0 {
		port = defaultModelProxyBindPort
	}
	return fmt.Sprintf("http://%s:%d", defaultSandboxModelProxyHost, port)
}

func validateModelProxySandboxBaseURL(raw string, bindPort int) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("harness.model_proxy.sandbox_base_url is invalid: %w", err)
	}
	if parsed.Scheme != "http" {
		return fmt.Errorf("harness.model_proxy.sandbox_base_url must use http scheme")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("harness.model_proxy.sandbox_base_url must not include userinfo, query, or fragment")
	}
	if parsed.Path != "" && parsed.Path != "/" {
		return fmt.Errorf("harness.model_proxy.sandbox_base_url must not include a path")
	}
	if strings.TrimSpace(parsed.Hostname()) == "" {
		return fmt.Errorf("harness.model_proxy.sandbox_base_url must include a host")
	}
	portRaw := parsed.Port()
	if portRaw == "" {
		return fmt.Errorf("harness.model_proxy.sandbox_base_url must include an explicit port matching bind_url")
	}
	port, err := strconv.Atoi(portRaw)
	if err != nil || port <= 0 || port > 65535 {
		return fmt.Errorf("harness.model_proxy.sandbox_base_url contains invalid port %q", portRaw)
	}
	if port != bindPort {
		return fmt.Errorf("harness.model_proxy.sandbox_base_url port %d must match bind_url port %d", port, bindPort)
	}
	return nil
}

func parseModelProxyBindPort(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("harness.model_proxy.bind_url is required")
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return 0, fmt.Errorf("harness.model_proxy.bind_url is invalid: %w", err)
	}
	if parsed.Scheme != "http" {
		return 0, fmt.Errorf("harness.model_proxy.bind_url must use http scheme")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return 0, fmt.Errorf("harness.model_proxy.bind_url must not include userinfo, query, or fragment")
	}
	if parsed.Path != "" && parsed.Path != "/" {
		return 0, fmt.Errorf("harness.model_proxy.bind_url must not include a path")
	}
	host, portRaw, err := net.SplitHostPort(parsed.Host)
	if err != nil || strings.TrimSpace(portRaw) == "" {
		return 0, fmt.Errorf("harness.model_proxy.bind_url must include an explicit port")
	}
	if strings.Trim(strings.TrimSpace(host), "[]") == "" {
		return 0, fmt.Errorf("harness.model_proxy.bind_url must include a host")
	}
	if !isUnspecifiedModelProxyBindHost(host) {
		return 0, fmt.Errorf("harness.model_proxy.bind_url host must be an unspecified address")
	}
	port, err := strconv.Atoi(portRaw)
	if err != nil || port <= 0 || port > 65535 {
		return 0, fmt.Errorf("harness.model_proxy.bind_url contains invalid port %q", portRaw)
	}
	return port, nil
}

func isUnspecifiedModelProxyBindHost(host string) bool {
	host = strings.Trim(strings.ToLower(strings.TrimSpace(host)), "[]")
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}
	return addr.IsUnspecified()
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

func sessionRetentionEnv(fallback time.Duration) (time.Duration, error) {
	if _, ok := os.LookupEnv("HARNESS_SESSION_TTL"); ok {
		return 0, fmt.Errorf("HARNESS_SESSION_TTL is obsolete; use HARNESS_SESSION_RETENTION")
	}
	value, ok := os.LookupEnv("HARNESS_SESSION_RETENTION")
	if !ok {
		return fallback, nil
	}
	duration, err := time.ParseDuration(strings.TrimSpace(value))
	if err != nil {
		return 0, fmt.Errorf("invalid HARNESS_SESSION_RETENTION duration %q: %w", value, err)
	}
	return duration, nil
}
