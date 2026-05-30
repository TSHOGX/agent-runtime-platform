package server

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/config"
	"harness-platform/orchestrator/internal/store"
)

const runtimeConfigDigestPrefix = "runtime_config_digest_v1\n"

type deploymentResolution struct {
	Mode                    string
	DriverID                agents.ID
	DriverSpec              agents.DriverSpec
	AgentConfigID           string
	AgentConfig             config.AgentConfig
	ModelProfileID          string
	ModelProfile            config.ModelProfileConfig
	RuntimeProviderConfigID string
	RuntimeProviderConfig   config.RuntimeProviderConfig
	ProviderSpec            agents.RuntimeProviderSpec
	AgentManifest           imageAgentManifest
	AgentManifestDriver     imageManifestDriver
}

type sandboxContractInputEvidence struct {
	RuntimeConfigDigest   string
	RuntimeConfigPreimage map[string]any
	AgentManifestDigest   string
	AgentManifestPayload  map[string]any
}

type deploymentCapabilityError struct {
	code    string
	message string
}

func (e *deploymentCapabilityError) Error() string {
	return e.message
}

func (s *Server) resolveModeDeployment(mode string) (deploymentResolution, *deploymentCapabilityError) {
	mode = strings.TrimSpace(mode)
	if mode == "" {
		mode = "agent"
	}
	switch mode {
	case "agent":
		defaultAgent := strings.TrimSpace(s.cfg.DefaultAgent)
		if defaultAgent == "" {
			defaultAgent = string(agents.ClaudeCode)
		}
		driverID, err := agents.CanonicalDriverID(defaultAgent)
		if err != nil {
			return deploymentResolution{}, capabilityError("default_unavailable", "agent mode unavailable")
		}
		return s.resolveDriverDeployment("agent", driverID)
	case "shell":
		return s.resolveDriverDeployment("shell", agents.Shell)
	default:
		return deploymentResolution{}, capabilityError("unsupported_mode", "unsupported mode")
	}
}

func (s *Server) resolveDriverDeployment(mode string, driverID agents.ID) (deploymentResolution, *deploymentCapabilityError) {
	spec, ok := agents.DriverSpecFor(string(driverID))
	if !ok {
		return deploymentResolution{}, capabilityError(modeUnavailableCode(mode), modeUnavailableMessage(mode))
	}
	if mode == "agent" && spec.Kind != agents.DriverKindAgent {
		return deploymentResolution{}, capabilityError("default_unavailable", "agent mode unavailable")
	}
	if mode == "shell" && spec.Kind != agents.DriverKindShell {
		return deploymentResolution{}, capabilityError("disabled", "shell mode unavailable")
	}
	agentConfigID, agentCfg, ok := s.enabledAgentConfigForDriver(driverID)
	if !ok {
		return deploymentResolution{}, capabilityError(modeUnavailableCode(mode), modeUnavailableMessage(mode))
	}
	runtimeProviderID := strings.TrimSpace(agentCfg.RuntimeProvider)
	runtimeConfigs := s.cfg.DeploymentRuntimeProviders()
	runtimeCfg, ok := runtimeConfigs[runtimeProviderID]
	if !ok || !deploymentConfigEnabled(runtimeCfg.Enabled) {
		return deploymentResolution{}, capabilityError("provider_unsupported", modeUnavailableMessage(mode))
	}
	providerID := strings.TrimSpace(runtimeCfg.ProviderID)
	if providerID == "" {
		providerID = runtimeProviderID
	}
	providerSpec, ok := agents.RuntimeProviderSpecFor(providerID)
	if !ok {
		return deploymentResolution{}, capabilityError("provider_unsupported", modeUnavailableMessage(mode))
	}
	if err := agents.EnsureDriverSupportedByProvider(string(driverID), providerID); err != nil {
		return deploymentResolution{}, capabilityError("provider_unsupported", modeUnavailableMessage(mode))
	}

	var modelProfileID string
	var modelProfile config.ModelProfileConfig
	if spec.ModelAccess {
		modelProfileID = strings.TrimSpace(agentCfg.ModelProfile)
		profiles := s.cfg.DeploymentModelProfiles()
		var ok bool
		modelProfile, ok = profiles[modelProfileID]
		if !ok || !deploymentConfigEnabled(modelProfile.Enabled) ||
			strings.TrimSpace(modelProfile.Model) == "" ||
			strings.TrimSpace(modelProfile.ProxyRef) != config.DefaultModelProxyRef {
			return deploymentResolution{}, capabilityError("operator_unavailable", modeUnavailableMessage(mode))
		}
	}

	manifest, err := s.loadAgentImageManifest()
	if err != nil {
		return deploymentResolution{}, capabilityError("operator_unavailable", modeUnavailableMessage(mode))
	}
	manifestDriver, ok := manifest.driver(string(driverID))
	if !ok {
		return deploymentResolution{}, capabilityError("missing_from_image", modeUnavailableMessage(mode))
	}
	if err := s.validateManifestDriver(manifest, manifestDriver, spec); err != nil {
		return deploymentResolution{}, capabilityError("missing_from_image", modeUnavailableMessage(mode))
	}

	return deploymentResolution{
		Mode:                    mode,
		DriverID:                driverID,
		DriverSpec:              spec,
		AgentConfigID:           agentConfigID,
		AgentConfig:             agentCfg,
		ModelProfileID:          modelProfileID,
		ModelProfile:            modelProfile,
		RuntimeProviderConfigID: runtimeProviderID,
		RuntimeProviderConfig:   runtimeCfg,
		ProviderSpec:            providerSpec,
		AgentManifest:           manifest,
		AgentManifestDriver:     manifestDriver,
	}, nil
}

func (s *Server) enabledAgentConfigForDriver(driverID agents.ID) (string, config.AgentConfig, bool) {
	return config.EnabledAgentConfigForDriver(s.cfg.DeploymentAgents(), string(driverID))
}

func deploymentConfigEnabled(value *bool) bool {
	return value != nil && *value
}

func capabilityError(code, message string) *deploymentCapabilityError {
	return &deploymentCapabilityError{code: code, message: message}
}

func modeUnavailableCode(mode string) string {
	if mode == "agent" {
		return "default_unavailable"
	}
	return "disabled"
}

func modeUnavailableMessage(mode string) string {
	if mode == "agent" {
		return "agent mode unavailable"
	}
	return "shell mode unavailable"
}

type imageAgentManifest struct {
	SchemaVersion int                   `json:"schema_version"`
	BuildInput    map[string]any        `json:"build_input"`
	Drivers       []imageManifestDriver `json:"drivers"`
	Payload       map[string]any        `json:"-"`
	Path          string                `json:"-"`
	Digest        string                `json:"-"`
	Synthetic     bool                  `json:"-"`
}

type imageManifestDriver struct {
	DriverID              string   `json:"driver_id"`
	Label                 string   `json:"label"`
	Kind                  string   `json:"kind"`
	BinaryPath            string   `json:"binary_path"`
	InstalledBinaryDigest string   `json:"installed_binary_digest"`
	BridgeProtocol        string   `json:"bridge_protocol"`
	BridgeProtocolVersion int      `json:"bridge_protocol_version"`
	TurnInputSchema       string   `json:"turn_input_schema"`
	OutputSchema          string   `json:"output_schema"`
	ModelAccess           bool     `json:"model_access"`
	PackageName           string   `json:"package_name"`
	PackageVersion        string   `json:"package_version"`
	PackageShasum         string   `json:"package_shasum"`
	PackageIntegrity      string   `json:"package_integrity"`
	EventSchemaVersion    string   `json:"event_schema_version"`
	NodeVersion           string   `json:"node_version"`
	InstalledConfigPaths  []string `json:"installed_config_paths"`
	WritableStatePaths    []string `json:"writable_state_paths"`
}

func (m imageAgentManifest) driver(driverID string) (imageManifestDriver, bool) {
	for _, driver := range m.Drivers {
		if strings.TrimSpace(driver.DriverID) == driverID {
			return driver, true
		}
	}
	return imageManifestDriver{}, false
}

func (s *Server) loadAgentImageManifest() (imageAgentManifest, error) {
	path := s.agentImageManifestPath()
	data, err := os.ReadFile(path)
	if err != nil {
		if !s.cfg.RequireAgentManifest && (errors.Is(err, os.ErrNotExist) || path == "") {
			return s.syntheticAgentImageManifest()
		}
		return imageAgentManifest{}, fmt.Errorf("load agent image manifest: %w", err)
	}
	manifest, err := parseAgentImageManifest(data)
	if err != nil {
		return imageAgentManifest{}, err
	}
	manifest.Path = path
	return manifest, nil
}

func (s *Server) agentImageManifestPath() string {
	if path := strings.TrimSpace(s.cfg.AgentManifestPath); path != "" {
		return path
	}
	rootfs := strings.TrimSpace(s.cfg.RootFSPath)
	if rootfs == "" {
		return ""
	}
	return filepath.Join(rootfs, "etc", "harness-image", "agents.json")
}

func parseAgentImageManifest(data []byte) (imageAgentManifest, error) {
	var canonicalSource map[string]any
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.UseNumber()
	if err := decoder.Decode(&canonicalSource); err != nil {
		return imageAgentManifest{}, fmt.Errorf("decode agent image manifest: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return imageAgentManifest{}, fmt.Errorf("decode agent image manifest: trailing JSON")
	}
	canonical, err := store.CanonicalSandboxContractPayload(canonicalSource)
	if err != nil {
		return imageAgentManifest{}, fmt.Errorf("canonicalize agent image manifest: %w", err)
	}
	var manifest imageAgentManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return imageAgentManifest{}, fmt.Errorf("decode agent image manifest: %w", err)
	}
	if err := validateAgentImageManifestShape(manifest); err != nil {
		return imageAgentManifest{}, err
	}
	manifest.Digest = store.SandboxContractDigest(canonical)
	manifest.Payload = canonicalSource
	return manifest, nil
}

func validateAgentImageManifestShape(manifest imageAgentManifest) error {
	if manifest.SchemaVersion != 1 {
		return fmt.Errorf("agent image manifest schema_version = %d, want 1", manifest.SchemaVersion)
	}
	if len(manifest.Drivers) == 0 {
		return fmt.Errorf("agent image manifest has no drivers")
	}
	seen := map[string]struct{}{}
	for _, driver := range manifest.Drivers {
		driverID, err := agents.CanonicalDriverID(driver.DriverID)
		if err != nil {
			return fmt.Errorf("agent image manifest driver_id: %w", err)
		}
		key := string(driverID)
		if _, ok := seen[key]; ok {
			return fmt.Errorf("agent image manifest duplicate driver %q", key)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func (s *Server) syntheticAgentImageManifest() (imageAgentManifest, error) {
	agentConfigs := s.cfg.DeploymentAgents()
	keys := make([]string, 0, len(agentConfigs))
	for key, cfg := range agentConfigs {
		if deploymentConfigEnabled(cfg.Enabled) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	drivers := make([]imageManifestDriver, 0, len(keys))
	buildDrivers := make([]string, 0, len(keys))
	for _, key := range keys {
		cfg := agentConfigs[key]
		driverID, err := agents.CanonicalDriverID(effectiveString(cfg.DriverID, key))
		if err != nil {
			continue
		}
		spec, ok := agents.DriverSpecFor(string(driverID))
		if !ok {
			continue
		}
		drivers = append(drivers, syntheticManifestDriver(spec))
		buildDrivers = append(buildDrivers, string(driverID))
	}
	manifest := imageAgentManifest{
		SchemaVersion: 1,
		BuildInput:    map[string]any{"sandbox_agent_drivers": buildDrivers, "source": "registry_fallback"},
		Drivers:       drivers,
		Synthetic:     true,
	}
	if len(manifest.Drivers) == 0 {
		return imageAgentManifest{}, fmt.Errorf("synthetic agent manifest has no drivers")
	}
	payload := map[string]any{
		"schema_version": manifest.SchemaVersion,
		"build_input":    manifest.BuildInput,
		"drivers":        drivers,
	}
	canonical, err := store.CanonicalSandboxContractPayload(payload)
	if err != nil {
		return imageAgentManifest{}, err
	}
	manifest.Digest = store.SandboxContractDigest(canonical)
	manifest.Payload = payload
	return manifest, nil
}

func syntheticManifestDriver(spec agents.DriverSpec) imageManifestDriver {
	driver := imageManifestDriver{
		DriverID:              string(spec.ID),
		Label:                 spec.Label,
		Kind:                  string(spec.Kind),
		BinaryPath:            expectedDriverBinaryPath(spec.ID),
		InstalledBinaryDigest: driverInstallDigest(spec.ID),
		BridgeProtocol:        spec.BridgeProtocol,
		BridgeProtocolVersion: spec.BridgeProtocolVersion,
		TurnInputSchema:       spec.TurnInputSchema,
		OutputSchema:          spec.OutputSchema,
		ModelAccess:           spec.ModelAccess,
	}
	switch spec.ID {
	case agents.ClaudeCode:
		driver.PackageName = "@anthropic-ai/claude-code"
		driver.PackageVersion = "bundled"
	case agents.Pi:
		driver.PackageName = agents.PiPackageName
		driver.PackageVersion = agents.PiPackageVersion
		driver.PackageShasum = agents.PiPackageShasum
		driver.PackageIntegrity = agents.PiPackageIntegrity
		driver.EventSchemaVersion = agents.PiEventSchemaVersion
	case agents.Shell:
	}
	return driver
}

func (s *Server) validateManifestDriver(manifest imageAgentManifest, driver imageManifestDriver, spec agents.DriverSpec) error {
	if strings.TrimSpace(driver.DriverID) != string(spec.ID) ||
		strings.TrimSpace(driver.Kind) != string(spec.Kind) ||
		strings.TrimSpace(driver.BridgeProtocol) != spec.BridgeProtocol ||
		driver.BridgeProtocolVersion != spec.BridgeProtocolVersion ||
		strings.TrimSpace(driver.TurnInputSchema) != spec.TurnInputSchema ||
		strings.TrimSpace(driver.OutputSchema) != spec.OutputSchema ||
		driver.ModelAccess != spec.ModelAccess {
		return fmt.Errorf("agent image manifest driver %s does not match registry facts", spec.ID)
	}
	if strings.TrimSpace(driver.BinaryPath) != expectedDriverBinaryPath(spec.ID) {
		return fmt.Errorf("agent image manifest driver %s binary path mismatch", spec.ID)
	}
	if !strings.HasPrefix(strings.TrimSpace(driver.InstalledBinaryDigest), "sha256:") {
		return fmt.Errorf("agent image manifest driver %s missing installed binary digest", spec.ID)
	}
	if err := validateDriverPackageFacts(driver, spec.ID); err != nil {
		return err
	}
	if manifest.Synthetic || strings.TrimSpace(s.cfg.RootFSPath) == "" {
		return nil
	}
	actualDigest, err := rootfsFileDigest(s.cfg.RootFSPath, driver.BinaryPath)
	if err != nil {
		return err
	}
	if driver.InstalledBinaryDigest != actualDigest {
		return fmt.Errorf("agent image manifest driver %s installed binary digest mismatch", spec.ID)
	}
	return nil
}

func validateDriverPackageFacts(driver imageManifestDriver, driverID agents.ID) error {
	switch driverID {
	case agents.ClaudeCode:
		if strings.TrimSpace(driver.PackageName) != "@anthropic-ai/claude-code" ||
			strings.TrimSpace(driver.PackageVersion) == "" {
			return fmt.Errorf("agent image manifest claude_code package facts mismatch")
		}
	case agents.Pi:
		if strings.TrimSpace(driver.PackageName) != agents.PiPackageName ||
			strings.TrimSpace(driver.PackageVersion) != agents.PiPackageVersion ||
			strings.TrimSpace(driver.PackageShasum) != agents.PiPackageShasum ||
			strings.TrimSpace(driver.PackageIntegrity) != agents.PiPackageIntegrity ||
			strings.TrimSpace(driver.EventSchemaVersion) != agents.PiEventSchemaVersion {
			return fmt.Errorf("agent image manifest pi package facts mismatch")
		}
	case agents.Shell:
	}
	return nil
}

func expectedDriverBinaryPath(driverID agents.ID) string {
	switch driverID {
	case agents.Pi:
		return "/usr/local/bin/pi"
	case agents.Shell:
		return "/usr/local/bin/harness-shell-agent"
	default:
		return "/usr/local/bin/claude"
	}
}

func rootfsFileDigest(rootfs, sandboxPath string) (string, error) {
	resolved, err := resolveRootfsPath(rootfs, sandboxPath)
	if err != nil {
		return "", err
	}
	file, err := os.Open(resolved)
	if err != nil {
		return "", fmt.Errorf("open manifest binary %s: %w", sandboxPath, err)
	}
	defer func() { _ = file.Close() }()
	sum := sha256.New()
	if _, err := io.Copy(sum, file); err != nil {
		return "", fmt.Errorf("hash manifest binary %s: %w", sandboxPath, err)
	}
	return fmt.Sprintf("sha256:%x", sum.Sum(nil)), nil
}

func resolveRootfsPath(rootfs, sandboxPath string) (string, error) {
	if !filepath.IsAbs(sandboxPath) {
		return "", fmt.Errorf("manifest binary path %q must be absolute", sandboxPath)
	}
	root, err := filepath.Abs(rootfs)
	if err != nil {
		return "", err
	}
	root = filepath.Clean(root)
	current := filepath.Join(root, strings.TrimPrefix(sandboxPath, "/"))
	seen := map[string]struct{}{}
	for i := 0; i < 40; i++ {
		absolute, err := filepath.Abs(current)
		if err != nil {
			return "", err
		}
		absolute = filepath.Clean(absolute)
		if !pathWithinRoot(absolute, root) {
			return "", fmt.Errorf("manifest binary path %q escapes rootfs", sandboxPath)
		}
		info, err := os.Lstat(absolute)
		if err != nil {
			return "", fmt.Errorf("stat manifest binary %s: %w", sandboxPath, err)
		}
		if info.Mode()&os.ModeSymlink == 0 {
			return absolute, nil
		}
		if _, ok := seen[absolute]; ok {
			return "", fmt.Errorf("manifest binary path %q symlink cycle", sandboxPath)
		}
		seen[absolute] = struct{}{}
		target, err := os.Readlink(absolute)
		if err != nil {
			return "", err
		}
		if filepath.IsAbs(target) {
			current = filepath.Join(root, strings.TrimPrefix(target, "/"))
		} else {
			current = filepath.Join(filepath.Dir(absolute), target)
		}
	}
	return "", fmt.Errorf("manifest binary path %q symlink resolution exceeded limit", sandboxPath)
}

func pathWithinRoot(path, root string) bool {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

func (r deploymentResolution) runtimeConfigPreimage(defaultAgent string) map[string]any {
	selectedModelProfileID := any(nil)
	modelProfile := map[string]any{}
	if strings.TrimSpace(r.ModelProfileID) != "" {
		selectedModelProfileID = r.ModelProfileID
		modelProfile = modelProfileConfigPreimage(r.ModelProfile)
	}
	return map[string]any{
		"schema_version":               1,
		"product_mode":                 r.Mode,
		"selected_driver_id":           string(r.DriverID),
		"selected_agent_config_id":     r.AgentConfigID,
		"selected_model_profile_id":    selectedModelProfileID,
		"selected_runtime_provider_id": r.RuntimeProviderConfigID,
		"deployment_defaults":          map[string]any{"default_agent": defaultAgent},
		"agent_config":                 agentConfigPreimage(r.AgentConfig),
		"model_profile":                modelProfile,
		"runtime_provider_config":      runtimeProviderConfigPreimage(r.RuntimeProviderConfig),
	}
}

func (s *Server) sandboxContractInputEvidenceFor(session store.Session, driverID string) (sandboxContractInputEvidence, error) {
	mode := strings.TrimSpace(session.Mode)
	if mode == "" {
		mode = store.ModeForDriver(driverID)
	}
	deployment, capabilityErr := s.resolveDriverDeployment(mode, agents.ID(driverID))
	if capabilityErr != nil {
		return sandboxContractInputEvidence{}, capabilityErr
	}
	defaultAgent := strings.TrimSpace(s.cfg.DefaultAgent)
	if defaultAgent == "" {
		defaultAgent = string(agents.ClaudeCode)
	}
	if canonical, err := agents.CanonicalDriverID(defaultAgent); err == nil {
		defaultAgent = string(canonical)
	}
	preimage := deployment.runtimeConfigPreimage(defaultAgent)
	return sandboxContractInputEvidence{
		RuntimeConfigDigest:   runtimeConfigDigest(preimage),
		RuntimeConfigPreimage: preimage,
		AgentManifestDigest:   deployment.AgentManifest.Digest,
		AgentManifestPayload:  deployment.AgentManifest.Payload,
	}, nil
}

func agentConfigPreimage(cfg config.AgentConfig) map[string]any {
	return map[string]any{
		"enabled":                      deploymentConfigEnabled(cfg.Enabled),
		"driver_id":                    strings.TrimSpace(cfg.DriverID),
		"model_profile":                nullableString(strings.TrimSpace(cfg.ModelProfile)),
		"runtime_provider":             strings.TrimSpace(cfg.RuntimeProvider),
		"disable_nonessential_traffic": nullableBool(cfg.DisableNonessentialTraffic),
	}
}

func modelProfileConfigPreimage(cfg config.ModelProfileConfig) map[string]any {
	return map[string]any{
		"enabled":   deploymentConfigEnabled(cfg.Enabled),
		"provider":  strings.TrimSpace(cfg.Provider),
		"model":     strings.TrimSpace(cfg.Model),
		"proxy_ref": strings.TrimSpace(cfg.ProxyRef),
	}
}

func runtimeProviderConfigPreimage(cfg config.RuntimeProviderConfig) map[string]any {
	return map[string]any{
		"enabled":     deploymentConfigEnabled(cfg.Enabled),
		"provider_id": strings.TrimSpace(cfg.ProviderID),
		"profile_id":  strings.TrimSpace(cfg.ProfileID),
	}
}

func runtimeConfigDigest(preimage map[string]any) string {
	canonical, err := store.CanonicalSandboxContractPayload(preimage)
	if err != nil {
		return "sha256:invalid"
	}
	sum := sha256.Sum256(append([]byte(runtimeConfigDigestPrefix), canonical...))
	return fmt.Sprintf("sha256:%x", sum[:])
}

func nullableString(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return strings.TrimSpace(value)
}

func nullableBool(value *bool) any {
	if value == nil {
		return nil
	}
	return *value
}
