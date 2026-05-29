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
	Shell      ID = "sh"
)

const LegacyClaudeToken = "claude"

type Protocol string

const (
	ProtocolClaudeStreamJSON Protocol = "claude_stream_json"
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

func Lookup(value string) (Definition, bool) {
	spec, ok := DriverSpecFor(value)
	if !ok {
		return Definition{}, false
	}
	protocol := ProtocolClaudeStreamJSON
	if spec.ID == Shell {
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
	case ClaudeCode, Shell:
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

func cloneDriverSpec(spec DriverSpec) DriverSpec {
	spec.RequiredRuntimeCapabilities = append([]string(nil), spec.RequiredRuntimeCapabilities...)
	spec.Phase10Support = append([]string(nil), spec.Phase10Support...)
	return spec
}

func cloneRuntimeProviderSpec(spec RuntimeProviderSpec) RuntimeProviderSpec {
	spec.Capabilities = append([]string(nil), spec.Capabilities...)
	return spec
}
