package server

import (
	"log/slog"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/artifacts"
	"harness-platform/orchestrator/internal/config"
	"harness-platform/orchestrator/internal/events"
	"harness-platform/orchestrator/internal/store"
)

func newServerTestWatcher(t *testing.T, sessionsRoot string, st *store.Store, hub *events.Hub) *artifacts.Watcher {
	t.Helper()
	return artifacts.New(store.DataVolumeProvisionerConfig{
		SessionsRoot:   sessionsRoot,
		AgentHomesRoot: filepath.Join(t.TempDir(), "agent-homes"),
		EvidenceRoot:   filepath.Join(t.TempDir(), "volume-evidence"),
		RuntimeIdentity: store.RuntimeIdentity{
			UID: serverTestSandboxUID(),
			GID: serverTestSandboxGID(),
		},
	}, st, hub, slog.Default())
}

func serverDataVolumeConfigForTest(cfg config.Config) (store.DataVolumeProvisionerConfig, error) {
	roots, err := config.ValidateIsolationRoots(cfg.IsolationRoots())
	if err != nil {
		return store.DataVolumeProvisionerConfig{}, err
	}
	identity := cfg.Harness.SandboxIdentity
	return store.DataVolumeProvisionerConfig{
		SessionsRoot:   roots.SessionsRoot,
		AgentHomesRoot: roots.AgentHomesRoot,
		EvidenceRoot:   roots.DataVolumeEvidenceRoot,
		LayoutVersion:  store.DataVolumeLayoutVersion,
		RuntimeIdentity: store.RuntimeIdentity{
			UID:              identity.UID,
			GID:              identity.GID,
			SupplementalGIDs: identity.SupplementalGIDs,
		},
	}, nil
}

func applyServerTestDeploymentConfig(cfg *config.Config) {
	enabled := true
	disableNonessentialTraffic := true
	cfg.Harness.DefaultAgent = cfg.DefaultAgent
	cfg.Harness.Agents = map[string]config.AgentConfig{
		"claude_code": {
			Enabled:                    &enabled,
			DriverID:                   "claude_code",
			ModelProfile:               "anthropic_default",
			RuntimeProvider:            "local_runsc",
			DisableNonessentialTraffic: &disableNonessentialTraffic,
		},
		"pi": {
			Enabled:                    &enabled,
			DriverID:                   "pi",
			ModelProfile:               "anthropic_default",
			RuntimeProvider:            "local_runsc",
			DisableNonessentialTraffic: &disableNonessentialTraffic,
		},
		"sh": {
			Enabled:         &enabled,
			DriverID:        "sh",
			RuntimeProvider: "local_runsc",
		},
	}
	cfg.Harness.ModelProfiles = map[string]config.ModelProfileConfig{
		"anthropic_default": {
			Enabled:  &enabled,
			Provider: "anthropic_messages",
			Model:    "sonnet",
			ProxyRef: config.DefaultModelProxyRef,
		},
	}
	cfg.Harness.RuntimeProviders = map[string]config.RuntimeProviderConfig{
		"local_runsc": {
			Enabled:    &enabled,
			ProviderID: "local_runsc",
			ProfileID:  "local_runsc_default",
		},
	}
}

func testServerConfig(dir string) config.Config {
	rootfs := filepath.Join(dir, "rootfs")
	mustWriteServerTestAgentImageManifest(rootfs, agents.ClaudeCode, agents.Pi, agents.Shell)
	cfg := config.Config{
		SessionsRoot:     filepath.Join(dir, "sessions"),
		AgentHomesRoot:   filepath.Join(dir, "agent-homes"),
		BundleRoot:       filepath.Join(dir, "bundle"),
		RootFSPath:       rootfs,
		DBPath:           filepath.Join(dir, "state", "orchestrator.db"),
		RepoRoot:         dir,
		SessionRetention: time.Hour,
		MaxSessions:      10,
		DefaultAgent:     "claude_code",
		ModelProxy: config.ModelProxyConfig{
			BindURL:        "http://0.0.0.0:8082",
			SandboxBaseURL: "http://harness-model-proxy.internal:8082",
			BindPort:       8082,
		},
		Harness: config.HarnessConfig{
			RunDir: filepath.Join(dir, "run"),
			ModelProxy: config.ModelProxyConfig{
				BindURL:        "http://0.0.0.0:8082",
				SandboxBaseURL: "http://harness-model-proxy.internal:8082",
				BindPort:       8082,
			},
			Network: config.NetworkConfig{
				CIDRPool: config.CIDRPrefix{Prefix: netip.MustParsePrefix("10.241.0.0/29")},
				Egress: config.EgressConfig{
					DorisFEHosts: []string{"172.16.0.138"},
					DorisBEHosts: []string{"172.16.0.139"},
					DorisPorts:   []int{9030},
					DNSPolicy:    config.DNSPolicyHostnamesOnly,
				},
			},
			Bridge: config.BridgeConfig{
				LeaseTTL:          config.Duration{Duration: time.Minute},
				HeartbeatInterval: config.Duration{Duration: 10 * time.Millisecond},
				PollInterval:      config.Duration{Duration: 10 * time.Millisecond},
				AckStartedGrace:   config.Duration{Duration: 90 * time.Second},
				ReconnectGrace:    config.Duration{Duration: 30 * time.Second},
			},
			Events: config.EventsConfig{
				RetentionWindow:        config.Duration{Duration: time.Hour},
				RetentionRows:          1_000,
				EmitOutputBatchMaxRows: 64,
				EmitOutputBatchMaxAge:  config.Duration{Duration: 100 * time.Millisecond},
			},
			Reaper: config.ReaperConfig{
				FailedRetention: config.Duration{Duration: 0},
			},
			SandboxIdentity: config.SandboxIdentity{
				UID: serverTestSandboxUID(),
				GID: serverTestSandboxGID(),
			},
			ProxyServiceIdentity: config.ProxyServiceIdentity{
				UID: os.Geteuid(),
				GID: os.Getegid(),
			},
		},
	}
	applyServerTestDeploymentConfig(&cfg)
	return cfg
}

func serverTestSandboxUID() int {
	uid := os.Getuid()
	if uid > 0 {
		return uid
	}
	return 65534
}

func serverTestSandboxGID() int {
	gid := os.Getgid()
	if gid > 0 {
		return gid
	}
	return 65534
}

func serverTestAllocatorConfig(cfg config.Config, driverID string) store.ResourceAllocatorConfig {
	if canonical, err := agents.CanonicalDriverID(driverID); err == nil {
		driverID = string(canonical)
	}
	outputFormat := ""
	modelAccess := false
	if spec, ok := agents.DriverSpecFor(driverID); ok {
		outputFormat = spec.OutputFormat
		modelAccess = spec.ModelAccess
	}
	model := ""
	disableNonessentialTraffic := false
	if _, agentCfg, ok := config.EnabledAgentConfigForDriver(cfg.DeploymentAgents(), driverID); ok {
		if agentCfg.DisableNonessentialTraffic != nil {
			disableNonessentialTraffic = *agentCfg.DisableNonessentialTraffic
		}
		if strings.TrimSpace(agentCfg.ModelProfile) != "" {
			if profile, ok := cfg.DeploymentModelProfiles()[agentCfg.ModelProfile]; ok && strings.TrimSpace(profile.Model) != "" {
				model = strings.TrimSpace(profile.Model)
			}
		}
	}
	return store.ResourceAllocatorConfig{
		RunDir:                      cfg.Harness.RunDir,
		CIDRPool:                    cfg.Harness.Network.CIDRPool.Prefix,
		EgressDorisFEHosts:          cfg.Harness.Network.Egress.DorisFEHosts,
		EgressDorisBEHosts:          cfg.Harness.Network.Egress.DorisBEHosts,
		EgressDorisPorts:            cfg.Harness.Network.Egress.DorisPorts,
		EgressDNSPolicy:             string(cfg.Harness.Network.Egress.DNSPolicy),
		HostProxyBindURL:            cfg.ModelProxy.BindURL,
		ProxyPort:                   cfg.ModelProxy.BindPort,
		DriverID:                    driverID,
		Model:                       model,
		OutputFormat:                outputFormat,
		DisableNonessentialTraffic:  disableNonessentialTraffic,
		SandboxUID:                  cfg.Harness.SandboxIdentity.UID,
		SandboxGID:                  cfg.Harness.SandboxIdentity.GID,
		SandboxSupplementalGIDs:     cfg.Harness.SandboxIdentity.SupplementalGIDs,
		ModelAccessAllowed:          &modelAccess,
		ProviderCredentialsHostOnly: modelAccess,
		SandboxModelProxyBaseURL:    cfg.ModelProxy.SandboxBaseURL,
	}
}
