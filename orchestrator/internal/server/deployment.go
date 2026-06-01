package server

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/config"
	"harness-platform/orchestrator/internal/store"
)

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
			return deploymentResolution{}, capabilityError("default_unavailable", "agent mode unavailable")
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
		return deploymentResolution{}, capabilityError("provider_unsupported", modeUnavailableMessage(mode))
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
	if strings.TrimSpace(path) == "" {
		return imageAgentManifest{}, fmt.Errorf("agent image manifest path is required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return imageAgentManifest{}, fmt.Errorf("load agent image manifest %s: %w", path, err)
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

func manifestDriverFromSpec(spec agents.DriverSpec) (imageManifestDriver, error) {
	binaryPath, err := expectedDriverBinaryPath(spec.ID)
	if err != nil {
		return imageManifestDriver{}, err
	}
	driver := imageManifestDriver{
		DriverID:              string(spec.ID),
		Label:                 spec.Label,
		Kind:                  string(spec.Kind),
		BinaryPath:            binaryPath,
		InstalledBinaryDigest: "",
		BridgeProtocol:        spec.BridgeProtocol,
		BridgeProtocolVersion: spec.BridgeProtocolVersion,
		TurnInputSchema:       spec.TurnInputSchema,
		OutputSchema:          spec.OutputSchema,
		ModelAccess:           spec.ModelAccess,
	}
	facts := spec.PackageFacts
	driver.PackageName = facts.Name
	driver.PackageVersion = manifestPackageVersion(facts)
	driver.PackageShasum = facts.Shasum
	driver.PackageIntegrity = facts.Integrity
	driver.EventSchemaVersion = facts.EventSchemaVersion
	return driver, nil
}

// manifestPackageVersion reports the version recorded in the agent image
// manifest for a driver. A pinned version is used verbatim; a named package
// with no pinned version (e.g. the bundled claude_code driver) is recorded as
// "bundled"; a driver with no package (e.g. shell) has no version.
func manifestPackageVersion(facts agents.DriverPackageFacts) string {
	switch {
	case strings.TrimSpace(facts.Version) != "":
		return facts.Version
	case strings.TrimSpace(facts.Name) != "":
		return "bundled"
	default:
		return ""
	}
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
	expectedBinaryPath, err := expectedDriverBinaryPath(spec.ID)
	if err != nil {
		return err
	}
	if strings.TrimSpace(driver.BinaryPath) != expectedBinaryPath {
		return fmt.Errorf("agent image manifest driver %s binary path mismatch", spec.ID)
	}
	if !strings.HasPrefix(strings.TrimSpace(driver.InstalledBinaryDigest), "sha256:") {
		return fmt.Errorf("agent image manifest driver %s missing installed binary digest", spec.ID)
	}
	if err := validateDriverPackageFacts(driver, spec.ID); err != nil {
		return err
	}
	if strings.TrimSpace(s.cfg.RootFSPath) == "" {
		return fmt.Errorf("rootfs path is required to validate agent image manifest driver %s", spec.ID)
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
	spec, ok := agents.DriverSpecFor(string(driverID))
	if !ok {
		return nil
	}
	facts := spec.PackageFacts
	mismatch := func() error {
		return fmt.Errorf("agent image manifest %s package facts mismatch", driverID)
	}
	if strings.TrimSpace(facts.Name) != "" {
		if strings.TrimSpace(driver.PackageName) != facts.Name ||
			strings.TrimSpace(driver.PackageVersion) == "" {
			return mismatch()
		}
	}
	if strings.TrimSpace(facts.Version) != "" && strings.TrimSpace(driver.PackageVersion) != facts.Version {
		return mismatch()
	}
	if strings.TrimSpace(facts.Shasum) != "" && strings.TrimSpace(driver.PackageShasum) != facts.Shasum {
		return mismatch()
	}
	if strings.TrimSpace(facts.Integrity) != "" && strings.TrimSpace(driver.PackageIntegrity) != facts.Integrity {
		return mismatch()
	}
	if strings.TrimSpace(facts.EventSchemaVersion) != "" && strings.TrimSpace(driver.EventSchemaVersion) != facts.EventSchemaVersion {
		return mismatch()
	}
	return nil
}

func expectedDriverBinaryPath(driverID agents.ID) (string, error) {
	spec, ok := agents.DriverSpecFor(string(driverID))
	if !ok {
		return "", fmt.Errorf("unsupported driver %q", driverID)
	}
	path := strings.TrimSpace(spec.BinaryPath)
	if path == "" {
		return "", fmt.Errorf("driver %s missing binary path", driverID)
	}
	return path, nil
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

func (s *Server) explicitDefaultAgent() (string, error) {
	defaultAgent := strings.TrimSpace(s.cfg.DefaultAgent)
	if defaultAgent == "" {
		return "", fmt.Errorf("default agent is required")
	}
	canonical, err := agents.CanonicalDriverID(defaultAgent)
	if err != nil {
		return "", fmt.Errorf("default agent: %w", err)
	}
	return string(canonical), nil
}

func (s *Server) sandboxContractInputEvidenceFor(session store.Session, driverID string) (sandboxContractInputEvidence, error) {
	mode := strings.TrimSpace(session.Mode)
	if mode == "" {
		return sandboxContractInputEvidence{}, fmt.Errorf("session mode is required")
	}
	deployment, capabilityErr := s.resolveDriverDeployment(mode, agents.ID(driverID))
	if capabilityErr != nil {
		return sandboxContractInputEvidence{}, capabilityErr
	}
	defaultAgent, err := s.explicitDefaultAgent()
	if err != nil {
		return sandboxContractInputEvidence{}, err
	}
	preimage := deployment.runtimeConfigPreimage(defaultAgent)
	digest, err := runtimeConfigDigest(preimage)
	if err != nil {
		return sandboxContractInputEvidence{}, err
	}
	return sandboxContractInputEvidence{
		RuntimeConfigDigest:   digest,
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

func runtimeConfigDigest(preimage map[string]any) (string, error) {
	canonical, err := store.CanonicalSandboxContractPayload(preimage)
	if err != nil {
		return "", err
	}
	return store.RuntimeConfigInputDigest(canonical), nil
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
