package runtime

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/driveradapter"
	"harness-platform/orchestrator/internal/store"
)

type controlManifest struct {
	SessionID                string         `json:"session_id"`
	GenerationID             string         `json:"generation_id"`
	SandboxContractVersion   string         `json:"sandbox_contract_version"`
	CreatedAt                string         `json:"created_at"`
	AttemptID                string         `json:"attempt_id"`
	NetworkProfileID         string         `json:"network_profile_id"`
	AgentRuntimeProfileID    string         `json:"agent_runtime_profile_id"`
	DriverID                 string         `json:"driver_id"`
	BridgeProtocolVersion    int            `json:"bridge_protocol_version"`
	TurnInputSchema          string         `json:"turn_input_schema"`
	RunscPlatform            string         `json:"runsc_platform"`
	RunscVersion             string         `json:"runsc_version"`
	SandboxModelProxyBaseURL string         `json:"sandbox_model_proxy_base_url,omitempty"`
	Model                    string         `json:"model,omitempty"`
	OutputFormat             string         `json:"output_format"`
	WorkspacePath            string         `json:"workspace_path"`
	AgentHomePath            string         `json:"agent_home_path"`
	BundleDigest             string         `json:"bundle_digest"`
	RuntimeConfigDigest      string         `json:"runtime_config_digest"`
	SpecDigest               string         `json:"spec_digest"`
	EgressPolicyDigest       string         `json:"egress_policy_digest"`
	ManifestVersion          int            `json:"manifest_version"`
	DriverRuntime            map[string]any `json:"driver_runtime,omitempty"`
}

type controlManifestFile struct {
	Payload controlManifest `json:"payload"`
	Digest  string          `json:"digest"`
}

func (r *Runtime) buildGenerationManifest(req StartRequest, driverSpec agents.DriverSpec, runscVersion, bundleDigest, runtimeConfigDigest, specDigest string) (controlManifest, error) {
	details := req.Generation
	selectedDriver := driverID(req)
	if selectedDriver == "" {
		return controlManifest{}, fmt.Errorf("driver id is required")
	}
	if string(driverSpec.ID) != selectedDriver {
		return controlManifest{}, fmt.Errorf("generation driver mismatch")
	}
	if !isSandboxIsolatedDriverSpec(driverSpec) {
		return controlManifest{}, fmt.Errorf("unsupported driver %q", selectedDriver)
	}
	if err := validateSandboxContractVersion(details); err != nil {
		return controlManifest{}, err
	}
	runscPlatform, err := requiredRunscPlatform(details)
	if err != nil {
		return controlManifest{}, err
	}
	manifest := controlManifest{
		SessionID:                req.SessionID,
		GenerationID:             details.GenerationID,
		SandboxContractVersion:   strings.TrimSpace(details.SandboxContractVersion),
		CreatedAt:                controlManifestCreatedAt(details),
		AttemptID:                "attempt-0",
		NetworkProfileID:         details.NetworkProfileID,
		AgentRuntimeProfileID:    details.AgentRuntimeProfileID,
		DriverID:                 selectedDriver,
		BridgeProtocolVersion:    driverSpec.BridgeProtocolVersion,
		TurnInputSchema:          driverSpec.TurnInputSchema,
		RunscPlatform:            runscPlatform,
		RunscVersion:             runscVersion,
		SandboxModelProxyBaseURL: details.ManifestAnthropicBaseURL,
		Model:                    details.Model,
		OutputFormat:             details.OutputFormat,
		WorkspacePath:            "/workspace",
		AgentHomePath:            "/agent-home",
		BundleDigest:             bundleDigest,
		RuntimeConfigDigest:      runtimeConfigDigest,
		SpecDigest:               specDigest,
		EgressPolicyDigest:       details.EgressPolicyDigest,
		ManifestVersion:          1,
	}
	driverRuntimeFields, err := driveradapter.RuntimeControlManifestFieldsFor(agents.ID(selectedDriver), details)
	if err != nil {
		return controlManifest{}, err
	}
	applyDriverControlManifestFields(&manifest, driverRuntimeFields)
	return manifest, nil
}

func controlManifestCreatedAt(details store.RuntimeGenerationDetails) string {
	if createdAt := strings.TrimSpace(details.RuntimeResourceCreatedAt); createdAt != "" {
		return createdAt
	}
	return time.Now().UTC().Format(time.RFC3339Nano)
}

func applyDriverControlManifestFields(manifest *controlManifest, fields map[string]any) {
	if len(fields) == 0 {
		return
	}
	ensureDriverRuntimeManifest(manifest)
	for key, value := range fields {
		manifest.DriverRuntime[key] = value
	}
}

func ensureDriverRuntimeManifest(manifest *controlManifest) {
	if manifest.DriverRuntime == nil {
		manifest.DriverRuntime = make(map[string]any)
	}
}

func validateGenerationDetails(req StartRequest) error {
	details := req.Generation
	if strings.TrimSpace(details.SessionID) != "" && strings.TrimSpace(req.SessionID) != "" && details.SessionID != req.SessionID {
		return fmt.Errorf("generation session mismatch")
	}
	if strings.TrimSpace(req.GenerationID) != "" && req.GenerationID != details.GenerationID {
		return fmt.Errorf("generation id mismatch")
	}
	if strings.TrimSpace(details.DriverID) != "" && strings.TrimSpace(req.DriverID) != "" && details.DriverID != req.DriverID {
		return fmt.Errorf("generation driver mismatch")
	}
	selectedDriver := driverID(req)
	if selectedDriver == "" {
		return fmt.Errorf("driver id is required")
	}
	if _, ok := agents.Lookup(selectedDriver); !ok {
		return fmt.Errorf("unsupported driver %q", selectedDriver)
	}
	if !isSandboxIsolatedRequest(req) {
		return fmt.Errorf("unsupported driver %q", selectedDriver)
	}
	if err := validateSandboxContractVersion(details); err != nil {
		return err
	}
	if _, err := requiredRunscPlatform(details); err != nil {
		return err
	}
	if details.RequiresSecretDrop ||
		strings.TrimSpace(details.SecretsDirPath) != "" ||
		strings.TrimSpace(details.AnthropicAPIKeySecretID) != "" ||
		strings.TrimSpace(details.AnthropicAuthTokenSecretID) != "" ||
		strings.TrimSpace(details.SecretVersion) != "" {
		return fmt.Errorf("sandbox_secret_disallowed")
	}
	return nil
}

func wrapControlManifest(manifest controlManifest) (string, controlManifestFile, error) {
	payloadBytes, err := canonicalJSON(manifest)
	if err != nil {
		return "", controlManifestFile{}, err
	}
	digest := digestHex(payloadBytes)
	return digest, controlManifestFile{Payload: manifest, Digest: digest}, nil
}

func projectedControlManifestDigest(manifest controlManifest) (string, error) {
	return projectedControlManifestPayloadDigest(manifest)
}

func projectedControlManifestPayloadDigest(payload any) (string, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	var fields map[string]any
	if err := json.Unmarshal(data, &fields); err != nil {
		return "", err
	}
	strictFields, regenerableFields := controlManifestProjectionFields()
	projected := map[string]any{}
	for key, value := range fields {
		if _, ok := regenerableFields[key]; ok {
			continue
		}
		if _, ok := strictFields[key]; !ok {
			return "", fmt.Errorf("unclassified control manifest field %q", key)
		}
		projected[key] = value
	}
	payloadBytes, err := canonicalJSON(projected)
	if err != nil {
		return "", err
	}
	return digestHex(payloadBytes), nil
}

func controlManifestProjectionFields() (map[string]struct{}, map[string]struct{}) {
	strictFields := map[string]struct{}{
		"session_id":                   {},
		"generation_id":                {},
		"sandbox_contract_version":     {},
		"network_profile_id":           {},
		"agent_runtime_profile_id":     {},
		"driver_id":                    {},
		"bridge_protocol_version":      {},
		"turn_input_schema":            {},
		"runsc_platform":               {},
		"runsc_version":                {},
		"sandbox_model_proxy_base_url": {},
		"model":                        {},
		"output_format":                {},
		"workspace_path":               {},
		"agent_home_path":              {},
		"bundle_digest":                {},
		"runtime_config_digest":        {},
		"spec_digest":                  {},
		"egress_policy_digest":         {},
		"manifest_version":             {},
		"driver_runtime":               {},
	}
	regenerableFields := map[string]struct{}{
		"created_at": {},
		"attempt_id": {},
	}
	return strictFields, regenerableFields
}

func validateSandboxContractVersion(details store.RuntimeGenerationDetails) error {
	contract := strings.TrimSpace(details.SandboxContractVersion)
	if contract == "" {
		return fmt.Errorf("sandbox contract version is required")
	}
	if contract != store.SandboxContractVersion {
		return fmt.Errorf("unsupported sandbox contract version %q", contract)
	}
	return nil
}
