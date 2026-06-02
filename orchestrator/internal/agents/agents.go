package agents

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
)

type ID string

const (
	ClaudeCode ID = "claude_code"
	Pi         ID = "pi"
	Shell      ID = "sh"
)

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

const DriverCapabilitySchemaVersion = 1

type FeatureID string

const (
	FeatureOperatorPolicyPrompt  FeatureID = "operator_policy_prompt"
	FeatureContextUsageReporting FeatureID = "context_usage_reporting"
	FeatureHardContextBudget     FeatureID = "hard_context_budget"
	FeatureCompaction            FeatureID = "compaction"
	FeatureSkillsSnapshot        FeatureID = "skills_snapshot"
	FeatureManagedSettings       FeatureID = "managed_settings"
	FeatureHooks                 FeatureID = "hooks"
	FeatureRemoteMCPRegistration FeatureID = "remote_mcp_registration"
	FeatureInterrupt             FeatureID = "interrupt"
)

type SubCapabilityID string

const (
	SubCapabilityOperatorPolicyPromptAdapter  SubCapabilityID = "operator_policy_prompt_adapter"
	SubCapabilityContextUsageReporter         SubCapabilityID = "context_usage_reporter"
	SubCapabilityHardContextBudgetEnforcer    SubCapabilityID = "hard_context_budget_enforcer"
	SubCapabilityCompactionAdapter            SubCapabilityID = "compaction_adapter"
	SubCapabilitySkillsSnapshotAdapter        SubCapabilityID = "skills_snapshot_adapter"
	SubCapabilityManagedSettingsRenderer      SubCapabilityID = "managed_settings_renderer"
	SubCapabilityHooksRenderer                SubCapabilityID = "hooks_renderer"
	SubCapabilityRemoteMCPRegistrationAdapter SubCapabilityID = "remote_mcp_registration_adapter"
	SubCapabilityInterruptAdapter             SubCapabilityID = "interrupt_adapter"
)

type CapabilitySupportState string

const (
	CapabilitySupported     CapabilitySupportState = "supported"
	CapabilityUnsupported   CapabilitySupportState = "unsupported"
	CapabilityNotApplicable CapabilitySupportState = "not_applicable"
)

type FeaturePolicyState string

const (
	FeaturePolicyRequired    FeaturePolicyState = "required"
	FeaturePolicyDisabled    FeaturePolicyState = "disabled"
	FeaturePolicyUnsupported FeaturePolicyState = "unsupported"
)

type FeatureDefinition struct {
	ID                            FeatureID         `json:"id"`
	DriverRequirements            []SubCapabilityID `json:"driver_requirements"`
	ProviderRequirements          []string          `json:"provider_requirements"`
	AdapterRenderer               string            `json:"adapter_renderer"`
	ProducedArtifacts             []string          `json:"produced_artifacts"`
	CredentialBearingMCPSupported bool              `json:"credential_bearing_mcp_supported"`
}

type DriverCapabilities struct {
	SchemaVersion   int                                        `json:"schema_version"`
	Features        map[FeatureID]CapabilitySupportState       `json:"features"`
	SubCapabilities map[SubCapabilityID]CapabilitySupportState `json:"sub_capabilities"`
}

type RuntimeProviderCapabilities struct {
	VocabularyVersion string   `json:"vocabulary_version"`
	Capabilities      []string `json:"capabilities"`
}

type FeaturePolicy map[FeatureID]FeaturePolicyState

var allFeatureIDs = []FeatureID{
	FeatureOperatorPolicyPrompt,
	FeatureContextUsageReporting,
	FeatureHardContextBudget,
	FeatureCompaction,
	FeatureSkillsSnapshot,
	FeatureManagedSettings,
	FeatureHooks,
	FeatureRemoteMCPRegistration,
	FeatureInterrupt,
}

var allSubCapabilityIDs = []SubCapabilityID{
	SubCapabilityOperatorPolicyPromptAdapter,
	SubCapabilityContextUsageReporter,
	SubCapabilityHardContextBudgetEnforcer,
	SubCapabilityCompactionAdapter,
	SubCapabilitySkillsSnapshotAdapter,
	SubCapabilityManagedSettingsRenderer,
	SubCapabilityHooksRenderer,
	SubCapabilityRemoteMCPRegistrationAdapter,
	SubCapabilityInterruptAdapter,
}

var featureDefinitions = map[FeatureID]FeatureDefinition{
	FeatureOperatorPolicyPrompt: {
		ID:                   FeatureOperatorPolicyPrompt,
		DriverRequirements:   []SubCapabilityID{SubCapabilityOperatorPolicyPromptAdapter},
		ProviderRequirements: []string{"filesystem_rw"},
		AdapterRenderer:      "DriverOperatorPolicyPromptAdapter",
		ProducedArtifacts:    []string{"control_manifest", "operator_policy_prompt_sidecar"},
	},
	FeatureContextUsageReporting: {
		ID:                   FeatureContextUsageReporting,
		DriverRequirements:   []SubCapabilityID{SubCapabilityContextUsageReporter},
		ProviderRequirements: []string{"logs"},
		AdapterRenderer:      "DriverContextUsageAdapter",
		ProducedArtifacts:    []string{"events"},
	},
	FeatureHardContextBudget: {
		ID:                   FeatureHardContextBudget,
		DriverRequirements:   []SubCapabilityID{SubCapabilityHardContextBudgetEnforcer},
		ProviderRequirements: []string{"kill"},
		AdapterRenderer:      "DriverContextBudgetAdapter",
		ProducedArtifacts:    []string{"control_manifest", "driver_policy"},
	},
	FeatureCompaction: {
		ID:                   FeatureCompaction,
		DriverRequirements:   []SubCapabilityID{SubCapabilityCompactionAdapter},
		ProviderRequirements: []string{"stdin"},
		AdapterRenderer:      "DriverCompactionAdapter",
		ProducedArtifacts:    []string{"bridge_command", "driver_state"},
	},
	FeatureSkillsSnapshot: {
		ID:                   FeatureSkillsSnapshot,
		DriverRequirements:   []SubCapabilityID{SubCapabilitySkillsSnapshotAdapter},
		ProviderRequirements: []string{"filesystem_rw"},
		AdapterRenderer:      "DriverSkillsAdapter",
		ProducedArtifacts:    []string{"content_snapshot_mount", "control_manifest"},
	},
	FeatureManagedSettings: {
		ID:                   FeatureManagedSettings,
		DriverRequirements:   []SubCapabilityID{SubCapabilityManagedSettingsRenderer},
		ProviderRequirements: []string{"filesystem_rw"},
		AdapterRenderer:      "DriverManagedSettingsAdapter",
		ProducedArtifacts:    []string{"driver_config", "content_snapshot_mount"},
	},
	FeatureHooks: {
		ID:                   FeatureHooks,
		DriverRequirements:   []SubCapabilityID{SubCapabilityHooksRenderer},
		ProviderRequirements: []string{"exec_stream"},
		AdapterRenderer:      "DriverPolicyHookAdapter",
		ProducedArtifacts:    []string{"driver_config"},
	},
	FeatureRemoteMCPRegistration: {
		ID:                   FeatureRemoteMCPRegistration,
		DriverRequirements:   []SubCapabilityID{SubCapabilityRemoteMCPRegistrationAdapter},
		ProviderRequirements: []string{"network_policy"},
		AdapterRenderer:      "DriverRemoteMCPAdapter",
		ProducedArtifacts:    []string{"driver_config"},
	},
	FeatureInterrupt: {
		ID:                   FeatureInterrupt,
		DriverRequirements:   []SubCapabilityID{SubCapabilityInterruptAdapter},
		ProviderRequirements: []string{"stdin"},
		AdapterRenderer:      "DriverInterruptAdapter",
		ProducedArtifacts:    []string{"bridge_command"},
	},
}

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

type DriverConfigMaterializationSpec struct {
	Name                        string
	MountName                   string
	ControlRelativePath         string
	SourceProjectionPath        string
	SandboxDestination          string
	DestinationMutableBySandbox bool
	MountType                   string
	MountMode                   string
	MountExact                  bool
}

func (s DriverConfigMaterializationSpec) HostSourcePath(controlDir string) string {
	return filepath.Join(strings.TrimSpace(controlDir), filepath.FromSlash(s.ControlRelativePath))
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
	FeatureSupport              []string
	Capabilities                DriverCapabilities
	BinaryPath                  string
	PackageFacts                DriverPackageFacts
	ConfigMaterializationSpecs  []DriverConfigMaterializationSpec
}

func DriverConfigMaterializationSpecsFor(driver ID) []DriverConfigMaterializationSpec {
	spec, ok := driverSpecs[ID(strings.TrimSpace(string(driver)))]
	if !ok || len(spec.ConfigMaterializationSpecs) == 0 {
		return nil
	}
	return cloneDriverConfigMaterializationSpecs(spec.ConfigMaterializationSpecs)
}

func AllDriverConfigMaterializationSpecs() []DriverConfigMaterializationSpec {
	var out []DriverConfigMaterializationSpec
	drivers := make([]string, 0, len(driverSpecs))
	for driver, spec := range driverSpecs {
		if len(spec.ConfigMaterializationSpecs) == 0 {
			continue
		}
		drivers = append(drivers, string(driver))
	}
	sort.Strings(drivers)
	for _, driver := range drivers {
		out = append(out, DriverConfigMaterializationSpecsFor(ID(driver))...)
	}
	return out
}

func AllFeatureIDs() []FeatureID {
	return append([]FeatureID(nil), allFeatureIDs...)
}

func AllSubCapabilityIDs() []SubCapabilityID {
	return append([]SubCapabilityID(nil), allSubCapabilityIDs...)
}

func FeatureDefinitionFor(feature FeatureID) (FeatureDefinition, bool) {
	definition, ok := featureDefinitions[feature]
	if !ok {
		return FeatureDefinition{}, false
	}
	return cloneFeatureDefinition(definition), true
}

func AllFeatureDefinitions() []FeatureDefinition {
	definitions := make([]FeatureDefinition, 0, len(allFeatureIDs))
	for _, feature := range allFeatureIDs {
		definition, ok := FeatureDefinitionFor(feature)
		if ok {
			definitions = append(definitions, definition)
		}
	}
	return definitions
}

func DriverSupportsFeature(spec DriverSpec, feature FeatureID) bool {
	state, ok := spec.Capabilities.Features[feature]
	return ok && state == CapabilitySupported
}

func DriverSupportsSubCapability(spec DriverSpec, capability SubCapabilityID) bool {
	state, ok := spec.Capabilities.SubCapabilities[capability]
	return ok && state == CapabilitySupported
}

func DefaultFeaturePolicyForDriver(spec DriverSpec) FeaturePolicy {
	policy := disabledFeaturePolicy()
	for _, feature := range []FeatureID{FeatureCompaction, FeatureInterrupt} {
		if DriverSupportsFeature(spec, feature) {
			policy[feature] = FeaturePolicyRequired
		} else {
			policy[feature] = FeaturePolicyUnsupported
		}
	}
	return policy
}

func NormalizeFeaturePolicy(policy FeaturePolicy) (FeaturePolicy, error) {
	normalized := disabledFeaturePolicy()
	for feature, state := range policy {
		if !knownFeatureID(feature) {
			return nil, fmt.Errorf("unknown feature %q", feature)
		}
		if !knownFeaturePolicyState(state) {
			return nil, fmt.Errorf("feature %s has invalid policy state %q", feature, state)
		}
		normalized[feature] = state
	}
	return normalized, nil
}

func ValidateFeaturePolicy(policy FeaturePolicy, driver DriverSpec, provider RuntimeProviderSpec) error {
	normalized, err := NormalizeFeaturePolicy(policy)
	if err != nil {
		return err
	}
	providerCapabilities := map[string]struct{}{}
	for _, capability := range provider.Capabilities {
		providerCapabilities[capability] = struct{}{}
	}
	for _, feature := range allFeatureIDs {
		state := normalized[feature]
		if state != FeaturePolicyRequired {
			continue
		}
		definition, ok := featureDefinitions[feature]
		if !ok {
			return fmt.Errorf("feature %s has no registered definition", feature)
		}
		if driver.Capabilities.Features[feature] != CapabilitySupported {
			return fmt.Errorf("feature %s requires driver %s support, got %s", feature, driver.ID, driver.Capabilities.Features[feature])
		}
		for _, required := range definition.DriverRequirements {
			if driver.Capabilities.SubCapabilities[required] != CapabilitySupported {
				return fmt.Errorf("feature %s requires driver %s sub-capability %s, got %s", feature, driver.ID, required, driver.Capabilities.SubCapabilities[required])
			}
		}
		for _, required := range definition.ProviderRequirements {
			if _, ok := providerCapabilities[required]; !ok {
				return fmt.Errorf("feature %s requires runtime provider %s capability %s", feature, provider.ID, required)
			}
		}
	}
	return nil
}

func FeaturePolicyPayload(policy FeaturePolicy) (map[string]string, error) {
	normalized, err := NormalizeFeaturePolicy(policy)
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for _, feature := range allFeatureIDs {
		out[string(feature)] = string(normalized[feature])
	}
	return out, nil
}

func DriverCapabilityPayload(spec DriverSpec) map[string]any {
	features := map[string]string{}
	for _, feature := range allFeatureIDs {
		features[string(feature)] = string(spec.Capabilities.Features[feature])
	}
	subCapabilities := map[string]string{}
	for _, capability := range allSubCapabilityIDs {
		subCapabilities[string(capability)] = string(spec.Capabilities.SubCapabilities[capability])
	}
	return map[string]any{
		"schema_version":   spec.Capabilities.SchemaVersion,
		"features":         features,
		"sub_capabilities": subCapabilities,
	}
}

func RuntimeProviderCapabilityPayload(spec RuntimeProviderSpec) map[string]any {
	return map[string]any{
		"vocabulary_version": spec.CapabilitySnapshot.VocabularyVersion,
		"capabilities":       append([]string(nil), spec.CapabilitySnapshot.Capabilities...),
	}
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
	CapabilitySnapshot   RuntimeProviderCapabilities
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
		ModelAccess:    true,
		OutputFormat:   "stream-json",
		FeatureSupport: []string{"single_driver_turns"},
		Capabilities:   claudeCodeCapabilities(),
		BinaryPath:     ClaudeCodeBinaryPath,
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
		ModelAccess:  true,
		OutputFormat: PiEventSchemaVersion,
		FeatureSupport: []string{
			"single_driver_turns",
			"system_prompt:unsupported",
			"compaction:unsupported",
			"skills:unsupported",
			"hooks_mcp:unsupported",
			"interrupt:unsupported",
		},
		Capabilities: piCapabilities(),
		BinaryPath:   PiBinaryPath,
		PackageFacts: DriverPackageFacts{
			Name:               PiPackageName,
			Version:            PiPackageVersion,
			Shasum:             PiPackageShasum,
			Integrity:          PiPackageIntegrity,
			EventSchemaVersion: PiEventSchemaVersion,
		},
		ConfigMaterializationSpecs: []DriverConfigMaterializationSpec{
			{
				Name:                        "models",
				MountName:                   "pi_models_config",
				ControlRelativePath:         "driver/pi/models.json",
				SourceProjectionPath:        PiModelsConfigPath,
				SandboxDestination:          PiModelsSandboxPath,
				DestinationMutableBySandbox: false,
				MountType:                   "bind",
				MountMode:                   "ro",
				MountExact:                  true,
			},
			{
				Name:                        "settings",
				MountName:                   "pi_settings_config",
				ControlRelativePath:         "driver/pi/settings.json",
				SourceProjectionPath:        PiSettingsConfigPath,
				SandboxDestination:          PiSettingsSandboxPath,
				DestinationMutableBySandbox: false,
				MountType:                   "bind",
				MountMode:                   "ro",
				MountExact:                  true,
			},
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
		ModelAccess:    false,
		OutputFormat:   "shell_pty",
		FeatureSupport: []string{"single_driver_turns"},
		Capabilities:   shellCapabilities(),
		BinaryPath:     ShellBinaryPath,
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

func claudeCodeCapabilities() DriverCapabilities {
	capabilities := agentDriverCapabilities(CapabilityUnsupported)
	capabilities.Features[FeatureCompaction] = CapabilitySupported
	capabilities.SubCapabilities[SubCapabilityCompactionAdapter] = CapabilitySupported
	return capabilities
}

func piCapabilities() DriverCapabilities {
	return agentDriverCapabilities(CapabilityUnsupported)
}

func shellCapabilities() DriverCapabilities {
	capabilities := agentDriverCapabilities(CapabilityNotApplicable)
	capabilities.Features[FeatureInterrupt] = CapabilitySupported
	capabilities.SubCapabilities[SubCapabilityInterruptAdapter] = CapabilitySupported
	return capabilities
}

func agentDriverCapabilities(defaultState CapabilitySupportState) DriverCapabilities {
	capabilities := DriverCapabilities{
		SchemaVersion:   DriverCapabilitySchemaVersion,
		Features:        map[FeatureID]CapabilitySupportState{},
		SubCapabilities: map[SubCapabilityID]CapabilitySupportState{},
	}
	for _, feature := range allFeatureIDs {
		capabilities.Features[feature] = defaultState
	}
	for _, capability := range allSubCapabilityIDs {
		capabilities.SubCapabilities[capability] = defaultState
	}
	return capabilities
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
	if err := ValidateFeaturePolicy(DefaultFeaturePolicyForDriver(driver), driver, provider); err != nil {
		return fmt.Errorf("default feature policy unsupported: %w", err)
	}
	return nil
}

func CapabilityDigest(provider RuntimeProviderSpec) string {
	payload := map[string]any{
		"provider_id":        provider.ID,
		"capabilities":       append([]string(nil), provider.CapabilitySnapshot.Capabilities...),
		"vocabulary_version": provider.CapabilitySnapshot.VocabularyVersion,
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
	default:
		return "", fmt.Errorf("unsupported driver %q", value)
	}
}

func normalizeDriverSpec(spec DriverSpec) DriverSpec {
	spec.Capabilities = normalizeDriverCapabilities(spec.Capabilities, spec.Kind)
	spec.SupportsInterrupt = DriverSupportsFeature(spec, FeatureInterrupt)
	spec.SupportsCompaction = DriverSupportsFeature(spec, FeatureCompaction)
	sort.Strings(spec.RequiredRuntimeCapabilities)
	sort.Strings(spec.FeatureSupport)
	return spec
}

func normalizeRuntimeProviderSpec(spec RuntimeProviderSpec) RuntimeProviderSpec {
	sort.Strings(spec.Capabilities)
	spec.CapabilitySnapshot = RuntimeProviderCapabilities{
		VocabularyVersion: spec.CapabilityVocabulary,
		Capabilities:      append([]string(nil), spec.Capabilities...),
	}
	return spec
}

func normalizeDriverCapabilities(capabilities DriverCapabilities, kind DriverKind) DriverCapabilities {
	if capabilities.SchemaVersion == 0 {
		capabilities.SchemaVersion = DriverCapabilitySchemaVersion
	}
	if capabilities.Features == nil {
		capabilities.Features = map[FeatureID]CapabilitySupportState{}
	}
	if capabilities.SubCapabilities == nil {
		capabilities.SubCapabilities = map[SubCapabilityID]CapabilitySupportState{}
	}
	defaultState := CapabilityUnsupported
	if kind == DriverKindShell {
		defaultState = CapabilityNotApplicable
	}
	for _, feature := range allFeatureIDs {
		state := capabilities.Features[feature]
		if !knownCapabilitySupportState(state) {
			state = defaultState
		}
		capabilities.Features[feature] = state
	}
	for _, capability := range allSubCapabilityIDs {
		state := capabilities.SubCapabilities[capability]
		if !knownCapabilitySupportState(state) {
			state = defaultState
		}
		capabilities.SubCapabilities[capability] = state
	}
	return capabilities
}

func secretGrantSpecKey(domain, scope string) string {
	return strings.TrimSpace(domain) + "\x00" + strings.TrimSpace(scope)
}

func cloneDriverSpec(spec DriverSpec) DriverSpec {
	spec.RequiredRuntimeCapabilities = append([]string(nil), spec.RequiredRuntimeCapabilities...)
	spec.FeatureSupport = append([]string(nil), spec.FeatureSupport...)
	spec.Capabilities = cloneDriverCapabilities(spec.Capabilities)
	spec.ConfigMaterializationSpecs = cloneDriverConfigMaterializationSpecs(spec.ConfigMaterializationSpecs)
	return spec
}

func cloneDriverCapabilities(capabilities DriverCapabilities) DriverCapabilities {
	capabilities.Features = cloneFeatureSupportStates(capabilities.Features)
	capabilities.SubCapabilities = cloneSubCapabilitySupportStates(capabilities.SubCapabilities)
	return capabilities
}

func cloneFeatureSupportStates(values map[FeatureID]CapabilitySupportState) map[FeatureID]CapabilitySupportState {
	if len(values) == 0 {
		return nil
	}
	out := make(map[FeatureID]CapabilitySupportState, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}

func cloneSubCapabilitySupportStates(values map[SubCapabilityID]CapabilitySupportState) map[SubCapabilityID]CapabilitySupportState {
	if len(values) == 0 {
		return nil
	}
	out := make(map[SubCapabilityID]CapabilitySupportState, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}

func cloneDriverConfigMaterializationSpecs(specs []DriverConfigMaterializationSpec) []DriverConfigMaterializationSpec {
	if len(specs) == 0 {
		return nil
	}
	out := make([]DriverConfigMaterializationSpec, len(specs))
	copy(out, specs)
	return out
}

func cloneRuntimeProviderSpec(spec RuntimeProviderSpec) RuntimeProviderSpec {
	spec.Capabilities = append([]string(nil), spec.Capabilities...)
	spec.CapabilitySnapshot.Capabilities = append([]string(nil), spec.CapabilitySnapshot.Capabilities...)
	return spec
}

func cloneFeatureDefinition(definition FeatureDefinition) FeatureDefinition {
	definition.DriverRequirements = append([]SubCapabilityID(nil), definition.DriverRequirements...)
	definition.ProviderRequirements = append([]string(nil), definition.ProviderRequirements...)
	definition.ProducedArtifacts = append([]string(nil), definition.ProducedArtifacts...)
	return definition
}

func disabledFeaturePolicy() FeaturePolicy {
	policy := FeaturePolicy{}
	for _, feature := range allFeatureIDs {
		policy[feature] = FeaturePolicyDisabled
	}
	return policy
}

func knownFeatureID(feature FeatureID) bool {
	for _, known := range allFeatureIDs {
		if feature == known {
			return true
		}
	}
	return false
}

func knownCapabilitySupportState(state CapabilitySupportState) bool {
	switch state {
	case CapabilitySupported, CapabilityUnsupported, CapabilityNotApplicable:
		return true
	default:
		return false
	}
}

func knownFeaturePolicyState(state FeaturePolicyState) bool {
	switch state {
	case FeaturePolicyRequired, FeaturePolicyDisabled, FeaturePolicyUnsupported:
		return true
	default:
		return false
	}
}
