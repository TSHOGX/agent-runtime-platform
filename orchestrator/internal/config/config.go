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
	"sort"
	"strings"
	"time"

	"harness-platform/orchestrator/internal/agents"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Addr              string
	SharedSecret      string
	CookieName        string
	SessionRetention  time.Duration
	RepoRoot          string
	RunscRoot         string
	SessionsRoot      string
	AgentHomesRoot    string
	BundleRoot        string
	RootFSPath        string
	AgentManifestPath string
	DBPath            string
	DefaultAgent      string
	MaxSessions       int
	RunscNetwork      string
	RunscOverlay2     string
	Agents            map[string]AgentConfig
	ModelProfiles     map[string]ModelProfileConfig
	RuntimeProviders  map[string]RuntimeProviderConfig
	ModelProxy        ModelProxyConfig
	Harness           HarnessConfig
	Warnings          []string
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

type HarnessConfig struct {
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

func (c HarnessConfig) ControlRoot() string {
	return filepath.Join(c.RunDir, "control")
}

func (c HarnessConfig) BundleRoot() string {
	return filepath.Join(c.RunDir, "runtime")
}

func (c HarnessConfig) BridgeRoot() string {
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
	sessionRetention, err := sessionRetentionEnv(projectConfig.Harness.SessionRetention.Duration)
	if err != nil {
		return Config{}, err
	}
	defaultDriver, err := agents.CanonicalDriverID(getenv("HARNESS_DEFAULT_AGENT", projectConfig.Harness.DefaultAgent))
	if err != nil {
		return Config{}, fmt.Errorf("HARNESS_DEFAULT_AGENT: %w", err)
	}
	if err := validateDefaultAgentDriver(defaultDriver, projectConfig.Harness.Agents); err != nil {
		return Config{}, fmt.Errorf("HARNESS_DEFAULT_AGENT: %w", err)
	}
	maxSessions, err := maxSessionsEnv(projectConfig.Harness.MaxSessions)
	if err != nil {
		return Config{}, err
	}
	cfg := Config{
		Addr:              getenv("HARNESS_ORCHESTRATOR_ADDR", ":8090"),
		SharedSecret:      os.Getenv("HARNESS_LAB_PASSWORD"),
		CookieName:        getenv("HARNESS_COOKIE_NAME", "harness_auth"),
		SessionRetention:  sessionRetention,
		RepoRoot:          getenv("HARNESS_REPO_ROOT", repoRoot),
		RunscRoot:         getenv("RUNSC_ROOT", "/var/lib/harness/runsc"),
		SessionsRoot:      sessionsRoot,
		AgentHomesRoot:    getenv("HARNESS_AGENT_HOMES_ROOT", "/var/lib/harness/agent-homes"),
		BundleRoot:        getenv("HARNESS_BUNDLE_ROOT", filepath.Join(repoRoot, "bundle", "out")),
		RootFSPath:        getenv("HARNESS_ROOTFS_PATH", filepath.Join(repoRoot, "sandbox-image", "rootfs")),
		AgentManifestPath: os.Getenv("HARNESS_AGENT_IMAGE_MANIFEST_PATH"),
		DBPath:            getenv("HARNESS_DB_PATH", "/var/lib/harness/state/orchestrator.db"),
		DefaultAgent:      string(defaultDriver),
		MaxSessions:       maxSessions,
		RunscNetwork:      "sandbox",
		RunscOverlay2:     "none",
		ModelProxy:        projectConfig.Harness.ModelProxy,
		Harness:           projectConfig.Harness,
	}
	cfg.Harness.SessionRetention = Duration{Duration: sessionRetention}
	cfg.Harness.MaxSessions = maxSessions
	cfg.Harness.DefaultAgent = string(defaultDriver)
	cfg.Harness = normalizeHarnessConfig(cfg.Harness)
	cfg.Agents = cloneAgentConfigs(cfg.Harness.Agents)
	cfg.ModelProfiles = cloneModelProfileConfigs(cfg.Harness.ModelProfiles)
	cfg.RuntimeProviders = cloneRuntimeProviderConfigs(cfg.Harness.RuntimeProviders)
	if value, ok, err := boolEnv("HARNESS_AUTO_CHECKPOINT_ENABLED"); err != nil {
		return Config{}, err
	} else if ok {
		cfg.Harness.Checkpoint.AutoEnabled = value
	}
	if err := validateHarnessConfig(cfg.Harness); err != nil {
		return Config{}, err
	}
	cfg.ModelProxy = cfg.Harness.ModelProxy
	cfg.Warnings = harnessConfigWarnings(cfg.Harness)
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
	Harness HarnessConfig
}

func loadProjectConfig(path string) (projectConfig, error) {
	cfg := projectConfig{}

	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return cfg, fmt.Errorf("load %s: config file is required", path)
	}
	if err != nil {
		return cfg, err
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return cfg, fmt.Errorf("load %s: config file is empty", path)
	}

	var target struct {
		Harness HarnessConfig `yaml:"harness"`
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&target); err != nil {
		return cfg, fmt.Errorf("load %s: %w", path, err)
	}
	cfg.Harness = target.Harness
	return finalizeProjectConfig(path, cfg)
}

func finalizeProjectConfig(path string, cfg projectConfig) (projectConfig, error) {
	cfg.Harness = normalizeHarnessConfig(cfg.Harness)
	if err := validateDeploymentConfig(cfg.Harness); err != nil {
		return cfg, fmt.Errorf("load %s: %w", path, err)
	}
	if err := validateModelProxyConfig(cfg.Harness.ModelProxy); err != nil {
		return cfg, fmt.Errorf("load %s: %w", path, err)
	}
	return cfg, nil
}

func normalizeHarnessConfig(cfg HarnessConfig) HarnessConfig {
	cfg.DefaultAgent = strings.TrimSpace(cfg.DefaultAgent)
	if canonical, err := agents.CanonicalDriverID(cfg.DefaultAgent); err == nil {
		cfg.DefaultAgent = string(canonical)
	}
	cfg.Agents = normalizeAgentConfigs(cfg.Agents)
	cfg.ModelProfiles = normalizeModelProfileConfigs(cfg.ModelProfiles)
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

func validateHarnessConfig(cfg HarnessConfig) error {
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

func harnessConfigWarnings(cfg HarnessConfig) []string {
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

// DefaultModelProxyRef is the only proxy reference accepted on model profiles;
// it is exported because the server re-validates the same invariant when
// resolving a deployment.
const DefaultModelProxyRef = "model_proxy"

func normalizeAgentConfigs(raw map[string]AgentConfig) map[string]AgentConfig {
	normalized := make(map[string]AgentConfig, len(raw))
	for id, cfg := range raw {
		key := strings.TrimSpace(id)
		if key == "" {
			continue
		}
		base := AgentConfig{}
		if cfg.Enabled != nil {
			base.Enabled = boolPtr(*cfg.Enabled)
		}
		base.DriverID = strings.TrimSpace(cfg.DriverID)
		if canonical, err := agents.CanonicalDriverID(base.DriverID); err == nil {
			base.DriverID = string(canonical)
		}
		base.ModelProfile = strings.TrimSpace(cfg.ModelProfile)
		base.RuntimeProvider = strings.TrimSpace(cfg.RuntimeProvider)
		if cfg.DisableNonessentialTraffic != nil {
			base.DisableNonessentialTraffic = boolPtr(*cfg.DisableNonessentialTraffic)
		}
		normalized[key] = base
	}
	return normalized
}

func normalizeModelProfileConfigs(raw map[string]ModelProfileConfig) map[string]ModelProfileConfig {
	normalized := make(map[string]ModelProfileConfig, len(raw))
	for id, cfg := range raw {
		key := strings.TrimSpace(id)
		if key == "" {
			continue
		}
		base := ModelProfileConfig{}
		if cfg.Enabled != nil {
			base.Enabled = boolPtr(*cfg.Enabled)
		}
		base.Provider = strings.TrimSpace(cfg.Provider)
		base.Model = strings.TrimSpace(cfg.Model)
		base.ProxyRef = strings.TrimSpace(cfg.ProxyRef)
		normalized[key] = base
	}
	return normalized
}

func normalizeRuntimeProviderConfigs(raw map[string]RuntimeProviderConfig) map[string]RuntimeProviderConfig {
	normalized := make(map[string]RuntimeProviderConfig, len(raw))
	for id, cfg := range raw {
		key := strings.TrimSpace(id)
		if key == "" {
			continue
		}
		base := RuntimeProviderConfig{}
		if cfg.Enabled != nil {
			base.Enabled = boolPtr(*cfg.Enabled)
		}
		base.ProviderID = strings.TrimSpace(cfg.ProviderID)
		base.ProfileID = strings.TrimSpace(cfg.ProfileID)
		normalized[key] = base
	}
	return normalized
}

func validateDeploymentConfig(cfg HarnessConfig) error {
	if strings.TrimSpace(cfg.DefaultAgent) == "" {
		return fmt.Errorf("harness.default_agent is required")
	}
	defaultDriver, err := agents.CanonicalDriverID(cfg.DefaultAgent)
	if err != nil {
		return fmt.Errorf("harness.default_agent: %w", err)
	}
	if len(cfg.Agents) == 0 {
		return fmt.Errorf("harness.agents must be non-empty")
	}
	if len(cfg.ModelProfiles) == 0 {
		return fmt.Errorf("harness.model_profiles must be non-empty")
	}
	if len(cfg.RuntimeProviders) == 0 {
		return fmt.Errorf("harness.runtime_providers must be non-empty")
	}
	for id, profile := range cfg.ModelProfiles {
		if profile.Enabled == nil {
			return fmt.Errorf("harness.model_profiles.%s.enabled is required", id)
		}
		if strings.TrimSpace(profile.Provider) == "" {
			return fmt.Errorf("harness.model_profiles.%s.provider is required", id)
		}
		if strings.TrimSpace(profile.Model) == "" {
			return fmt.Errorf("harness.model_profiles.%s.model is required", id)
		}
		if strings.TrimSpace(profile.ProxyRef) == "" {
			return fmt.Errorf("harness.model_profiles.%s.proxy_ref is required", id)
		}
		if strings.TrimSpace(profile.ProxyRef) != DefaultModelProxyRef {
			return fmt.Errorf("harness.model_profiles.%s.proxy_ref must be %s", id, DefaultModelProxyRef)
		}
	}
	for id, runtimeCfg := range cfg.RuntimeProviders {
		if runtimeCfg.Enabled == nil {
			return fmt.Errorf("harness.runtime_providers.%s.enabled is required", id)
		}
		if strings.TrimSpace(runtimeCfg.ProviderID) == "" {
			return fmt.Errorf("harness.runtime_providers.%s.provider_id is required", id)
		}
		if strings.TrimSpace(runtimeCfg.ProfileID) == "" {
			return fmt.Errorf("harness.runtime_providers.%s.profile_id is required", id)
		}
	}
	for id, agentCfg := range cfg.Agents {
		if agentCfg.Enabled == nil {
			return fmt.Errorf("harness.agents.%s.enabled is required", id)
		}
		if strings.TrimSpace(agentCfg.DriverID) == "" {
			return fmt.Errorf("harness.agents.%s.driver_id is required", id)
		}
		driverID, err := agents.CanonicalDriverID(agentCfg.DriverID)
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
		providerID := strings.TrimSpace(runtimeCfg.ProviderID)
		if err := agents.EnsureDriverSupportedByProvider(string(driverID), providerID); err != nil {
			return fmt.Errorf("harness.agents.%s.runtime_provider: %w", id, err)
		}
		if spec.ModelAccess {
			if enabled(agentCfg.Enabled) && agentCfg.DisableNonessentialTraffic == nil {
				return fmt.Errorf("harness.agents.%s.disable_nonessential_traffic is required for enabled model-access drivers", id)
			}
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
		}
	}
	if err := validateDefaultAgentDriver(defaultDriver, cfg.Agents); err != nil {
		return fmt.Errorf("harness.default_agent: %w", err)
	}
	return nil
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
		if strings.TrimSpace(cfg.DriverID) == "" {
			continue
		}
		candidate, err := agents.CanonicalDriverID(cfg.DriverID)
		if err == nil && candidate == canonical {
			return key, cfg, true
		}
	}
	return "", AgentConfig{}, false
}

func (c Config) DeploymentAgents() map[string]AgentConfig {
	raw := c.Agents
	if len(raw) == 0 && len(c.Harness.Agents) > 0 {
		raw = c.Harness.Agents
	}
	return normalizeAgentConfigs(raw)
}

func (c Config) DeploymentModelProfiles() map[string]ModelProfileConfig {
	raw := c.ModelProfiles
	if len(raw) == 0 && len(c.Harness.ModelProfiles) > 0 {
		raw = c.Harness.ModelProfiles
	}
	return normalizeModelProfileConfigs(raw)
}

func (c Config) DeploymentRuntimeProviders() map[string]RuntimeProviderConfig {
	raw := c.RuntimeProviders
	if len(raw) == 0 && len(c.Harness.RuntimeProviders) > 0 {
		raw = c.Harness.RuntimeProviders
	}
	return normalizeRuntimeProviderConfigs(raw)
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
