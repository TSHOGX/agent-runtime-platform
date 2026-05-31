package agents

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

type ID string

const (
	ClaudeCode ID = "claude_code"
	Pi         ID = "pi"
	Shell      ID = "sh"
)

const LegacyClaudeToken = "claude"

const (
	PiCodingAgentDir       = "/agent-home/.pi/agent"
	PiSessionDir           = PiCodingAgentDir + "/sessions"
	PiControlConfigDir     = "/harness-control/driver/pi"
	PiModelsConfigPath     = PiControlConfigDir + "/models.json"
	PiSettingsConfigPath   = PiControlConfigDir + "/settings.json"
	PiModelsSandboxPath    = PiCodingAgentDir + "/models.json"
	PiSettingsSandboxPath  = PiCodingAgentDir + "/settings.json"
	PiHarnessProxyProvider = "harness_anthropic_proxy"
	PiPackageName          = "@earendil-works/pi-coding-agent"
	PiPackageVersion       = "0.77.0"
	PiPackageShasum        = "627664c042507babf8a134a3770285272ccae5d8"
	PiPackageIntegrity     = "sha512-huS+k+dhQRR9PlTK7crLfeSRUw3a96V6JYfP0ZH3Zkko/m10gsYk8dKQmwScSy5Dll516pXorz19BURfD6S2qQ=="
	PiEventSchemaVersion   = "pi_rpc_events_v1.0"
	PiBinaryPath           = "/usr/local/bin/pi"

	ClaudeCodePackageName = "@anthropic-ai/claude-code"
	ClaudeCodeBinaryPath  = "/usr/local/bin/claude"

	ShellBinaryPath = "/usr/local/bin/harness-shell-agent"
)

type Protocol string

const (
	ProtocolClaudeStreamJSON Protocol = "claude_stream_json"
	ProtocolPiRPC            Protocol = "pi_rpc"
	ProtocolShellPTY         Protocol = "shell_pty"
)

type DriverKind string

const (
	DriverKindAgent DriverKind = "agent"
	DriverKindShell DriverKind = "shell"
)

type Definition struct {
	ID       ID
	Label    string
	Protocol Protocol
}

// DriverPackageFacts captures the verifiable package identity for a driver as
// it is installed into the agent image. Empty fields mean the driver does not
// pin that fact (e.g. a bundled driver carries only a name, a shell driver
// carries none).
type DriverPackageFacts struct {
	Name               string
	Version            string
	Shasum             string
	Integrity          string
	EventSchemaVersion string
}

type DriverSpec struct {
	ID                          ID
	Label                       string
	Kind                        DriverKind
	BridgeProtocol              string
	BridgeProtocolVersion       int
	TurnInputSchema             string
	OutputSchema                string
	RequiredRuntimeCapabilities []string
	ModelAccess                 bool
	OutputFormat                string
	SupportsInterrupt           bool
	SupportsCompaction          bool
	Phase10Support              []string
	BinaryPath                  string
	PackageFacts                DriverPackageFacts
}

type SnapshotPolicySpec struct {
	ProviderSupportsSnapshotDisk   bool
	ProviderSupportsSnapshotMemory bool
	ProviderSupportsBranch         bool
	BranchCountLimit               int
	MustQuiesceProcesses           bool
	StreamDisconnectsOnSnapshot    bool
	SnapshotSemantic               string
}

type RuntimeProviderSpec struct {
	ID                   string
	Label                string
	IsolationKind        string
	ProviderProfileID    string
	TemplateRef          string
	CapabilityVocabulary string
	Capabilities         []string
	SnapshotPolicy       SnapshotPolicySpec
}

type SecretGrantSpec struct {
	Domain        string
	GrantID       string
	Scope         string
	ExposureMode  string
	TTLMaxSeconds int64
}

var driverSpecs = map[ID]DriverSpec{
	ClaudeCode: normalizeDriverSpec(DriverSpec{
		ID:                    ClaudeCode,
		Label:                 "Claude Code",
		Kind:                  DriverKindAgent,
		BridgeProtocol:        "harness_bridge_v2",
		BridgeProtocolVersion: 2,
		TurnInputSchema:       "RunTurn",
		OutputSchema:          "claude_stream_json_v1",
		RequiredRuntimeCapabilities: []string{
			"exec_stream",
			"filesystem_rw",
			"kill",
			"logs",
			"network_policy",
			"snapshot_disk",
			"stdin",
		},
		ModelAccess:        true,
		OutputFormat:       "stream-json",
		SupportsInterrupt:  false,
		SupportsCompaction: true,
		Phase10Support:     []string{"single_driver_turns"},
		BinaryPath:         ClaudeCodeBinaryPath,
		PackageFacts: DriverPackageFacts{
			Name: ClaudeCodePackageName,
		},
	}),
	Pi: normalizeDriverSpec(DriverSpec{
		ID:                    Pi,
		Label:                 "Pi",
		Kind:                  DriverKindAgent,
		BridgeProtocol:        "harness_bridge_v2",
		BridgeProtocolVersion: 2,
		TurnInputSchema:       "RunTurn",
		OutputSchema:          PiEventSchemaVersion,
		RequiredRuntimeCapabilities: []string{
			"exec_stream",
			"filesystem_rw",
			"kill",
			"logs",
			"network_policy",
			"snapshot_disk",
			"stdin",
		},
		ModelAccess:        true,
		OutputFormat:       PiEventSchemaVersion,
		SupportsInterrupt:  false,
		SupportsCompaction: false,
		Phase10Support: []string{
			"single_driver_turns",
			"system_prompt:unsupported",
			"compaction:unsupported",
			"skills:unsupported",
			"hooks_mcp:unsupported",
			"interrupt:unsupported",
		},
		BinaryPath: PiBinaryPath,
		PackageFacts: DriverPackageFacts{
			Name:               PiPackageName,
			Version:            PiPackageVersion,
			Shasum:             PiPackageShasum,
			Integrity:          PiPackageIntegrity,
			EventSchemaVersion: PiEventSchemaVersion,
		},
	}),
	Shell: normalizeDriverSpec(DriverSpec{
		ID:                    Shell,
		Label:                 "Shell",
		Kind:                  DriverKindShell,
		BridgeProtocol:        "harness_bridge_v2",
		BridgeProtocolVersion: 2,
		TurnInputSchema:       "RunTurn",
		OutputSchema:          "shell_pty_v1",
		RequiredRuntimeCapabilities: []string{
			"exec_stream",
			"filesystem_rw",
			"kill",
			"logs",
			"network_policy",
			"snapshot_disk",
			"stdin",
		},
		ModelAccess:        false,
		OutputFormat:       "shell_pty",
		SupportsInterrupt:  true,
		SupportsCompaction: false,
		Phase10Support:     []string{"single_driver_turns"},
		BinaryPath:         ShellBinaryPath,
	}),
}

var runtimeProviderSpecs = map[string]RuntimeProviderSpec{
	"local_runsc": normalizeRuntimeProviderSpec(RuntimeProviderSpec{
		ID:                   "local_runsc",
		Label:                "Local runsc",
		IsolationKind:        "gvisor",
		ProviderProfileID:    "local_runsc_default",
		TemplateRef:          "default",
		CapabilityVocabulary: "1",
		Capabilities: []string{
			"exec_stream",
			"filesystem_rw",
			"kill",
			"logs",
			"network_policy",
			"snapshot_disk",
			"stdin",
		},
		SnapshotPolicy: SnapshotPolicySpec{
			ProviderSupportsSnapshotDisk:   true,
			ProviderSupportsSnapshotMemory: false,
			ProviderSupportsBranch:         false,
			BranchCountLimit:               0,
			MustQuiesceProcesses:           true,
			StreamDisconnectsOnSnapshot:    true,
			SnapshotSemantic:               "generation_checkpoint_restore",
		},
	}),
}

var secretGrantSpecs = map[string]SecretGrantSpec{
	secretGrantSpecKey("model_provider", "anthropic_messages"): {
		Domain:        "model_provider",
		GrantID:       "model_provider:anthropic_proxy",
		Scope:         "anthropic_messages",
		ExposureMode:  "proxy_only",
		TTLMaxSeconds: 86400,
	},
}

func Lookup(value string) (Definition, bool) {
	spec, ok := DriverSpecFor(value)
	if !ok {
		return Definition{}, false
	}
	protocol := ProtocolClaudeStreamJSON
	switch spec.ID {
	case Pi:
		protocol = ProtocolPiRPC
	case Shell:
		protocol = ProtocolShellPTY
	}
	return Definition{ID: spec.ID, Label: spec.Label, Protocol: protocol}, true
}

func DriverSpecFor(value string) (DriverSpec, bool) {
	spec, ok := driverSpecs[ID(strings.TrimSpace(value))]
	if !ok {
		return DriverSpec{}, false
	}
	return cloneDriverSpec(spec), true
}

func AllDriverSpecs() []DriverSpec {
	ids := make([]string, 0, len(driverSpecs))
	for id := range driverSpecs {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)
	specs := make([]DriverSpec, 0, len(ids))
	for _, id := range ids {
		specs = append(specs, cloneDriverSpec(driverSpecs[ID(id)]))
	}
	return specs
}

func RuntimeProviderSpecFor(value string) (RuntimeProviderSpec, bool) {
	spec, ok := runtimeProviderSpecs[strings.TrimSpace(value)]
	if !ok {
		return RuntimeProviderSpec{}, false
	}
	return cloneRuntimeProviderSpec(spec), true
}

func SecretGrantSpecFor(domain, scope string) (SecretGrantSpec, bool) {
	spec, ok := secretGrantSpecs[secretGrantSpecKey(domain, scope)]
	if !ok {
		return SecretGrantSpec{}, false
	}
	return spec, true
}

func EnsureDriverSupportedByProvider(driverID, providerID string) error {
	driver, ok := DriverSpecFor(driverID)
	if !ok {
		return fmt.Errorf("unsupported driver %q", driverID)
	}
	provider, ok := RuntimeProviderSpecFor(providerID)
	if !ok {
		return fmt.Errorf("unsupported runtime provider %q", providerID)
	}
	capabilities := map[string]struct{}{}
	for _, capability := range provider.Capabilities {
		capabilities[capability] = struct{}{}
	}
	for _, required := range driver.RequiredRuntimeCapabilities {
		if _, ok := capabilities[required]; !ok {
			return fmt.Errorf("runtime provider %s missing capability %s for driver %s", provider.ID, required, driver.ID)
		}
	}
	return nil
}

func CapabilityDigest(provider RuntimeProviderSpec) string {
	payload := map[string]any{
		"provider_id":        provider.ID,
		"capabilities":       append([]string(nil), provider.Capabilities...),
		"vocabulary_version": provider.CapabilityVocabulary,
	}
	data, _ := json.Marshal(payload)
	sum := sha256.Sum256(data)
	return "sha256:" + fmt.Sprintf("%x", sum[:])
}

func CanonicalDriverID(value string) (ID, error) {
	trimmed := strings.TrimSpace(value)
	switch ID(trimmed) {
	case ClaudeCode, Pi, Shell:
		return ID(trimmed), nil
	case ID(LegacyClaudeToken):
		return ClaudeCode, nil
	default:
		return "", fmt.Errorf("unsupported driver %q", value)
	}
}

func PublicAgentForDriver(value string) (string, bool) {
	switch ID(strings.TrimSpace(value)) {
	case ClaudeCode:
		return LegacyClaudeToken, true
	case Pi:
		return string(Pi), true
	case Shell:
		return string(Shell), true
	default:
		return "", false
	}
}

func SandboxAgentForDriver(value string) (string, bool) {
	return PublicAgentForDriver(value)
}

func normalizeDriverSpec(spec DriverSpec) DriverSpec {
	sort.Strings(spec.RequiredRuntimeCapabilities)
	sort.Strings(spec.Phase10Support)
	return spec
}

func normalizeRuntimeProviderSpec(spec RuntimeProviderSpec) RuntimeProviderSpec {
	sort.Strings(spec.Capabilities)
	return spec
}

func secretGrantSpecKey(domain, scope string) string {
	return strings.TrimSpace(domain) + "\x00" + strings.TrimSpace(scope)
}

func cloneDriverSpec(spec DriverSpec) DriverSpec {
	spec.RequiredRuntimeCapabilities = append([]string(nil), spec.RequiredRuntimeCapabilities...)
	spec.Phase10Support = append([]string(nil), spec.Phase10Support...)
	return spec
}

func cloneRuntimeProviderSpec(spec RuntimeProviderSpec) RuntimeProviderSpec {
	spec.Capabilities = append([]string(nil), spec.Capabilities...)
	return spec
}
