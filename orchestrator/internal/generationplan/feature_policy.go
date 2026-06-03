package generationplan

import (
	"fmt"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/driveradapter"
)

func RenderFeaturePolicyPayload(driverSpec agents.DriverSpec, providerSpec agents.RuntimeProviderSpec) (map[string]any, error) {
	policy := agents.DefaultFeaturePolicyForDriver(driverSpec)
	if err := agents.ValidateFeaturePolicy(policy, driverSpec, providerSpec); err != nil {
		return nil, fmt.Errorf("feature policy validation: %w", err)
	}
	if err := driveradapter.ValidateRequiredFeatureAdapters(policy, driverSpec); err != nil {
		return nil, fmt.Errorf("feature adapter validation: %w", err)
	}
	policyPayload, err := agents.FeaturePolicyPayload(policy)
	if err != nil {
		return nil, fmt.Errorf("feature policy payload: %w", err)
	}
	payload := map[string]any{}
	for key, value := range policyPayload {
		payload[key] = value
	}
	payload["capability_schema_version"] = agents.DriverCapabilitySchemaVersion
	payload["capability_vocab_version"] = providerSpec.CapabilityVocabulary
	payload["driver_capabilities"] = agents.DriverCapabilityPayload(driverSpec)
	payload["runtime_provider_capabilities"] = agents.RuntimeProviderCapabilityPayload(providerSpec)
	payload["legacy_supports_interrupt"] = driverSpec.SupportsInterrupt
	payload["legacy_supports_compaction"] = driverSpec.SupportsCompaction
	payload["unsupported_features_fail"] = true
	payload["credential_bearing_mcp_scope"] = "out_of_scope"
	return payload, nil
}

func validateFeaturePolicy(policy map[string]any, driver agents.DriverSpec, provider agents.RuntimeProviderSpec) error {
	typedPolicy := agents.FeaturePolicy{}
	for _, feature := range agents.AllFeatureIDs() {
		value := stringField(policy, string(feature))
		if value == "" {
			return fmt.Errorf("generation plan feature_policy.%s is required", feature)
		}
		typedPolicy[feature] = agents.FeaturePolicyState(value)
	}
	if err := agents.ValidateFeaturePolicy(typedPolicy, driver, provider); err != nil {
		return fmt.Errorf("generation plan feature policy invalid: %w", err)
	}
	if numberField(policy, "capability_schema_version") != int64(agents.DriverCapabilitySchemaVersion) {
		return fmt.Errorf("generation plan feature_policy.capability_schema_version is required")
	}
	if stringField(policy, "capability_vocab_version") != provider.CapabilityVocabulary {
		return fmt.Errorf("generation plan feature_policy.capability_vocab_version mismatch")
	}
	if boolField(policy, "unsupported_features_fail") != true {
		return fmt.Errorf("generation plan feature_policy.unsupported_features_fail must be true")
	}
	if stringField(policy, "credential_bearing_mcp_scope") != "out_of_scope" {
		return fmt.Errorf("generation plan feature_policy.credential_bearing_mcp_scope must be out_of_scope")
	}
	driverCapabilities, err := requireObject(policy, "driver_capabilities")
	if err != nil {
		return err
	}
	if err := validateDriverCapabilitySnapshot(driverCapabilities, driver); err != nil {
		return err
	}
	providerCapabilities, err := requireObject(policy, "runtime_provider_capabilities")
	if err != nil {
		return err
	}
	if err := validateProviderCapabilitySnapshot(providerCapabilities, provider); err != nil {
		return err
	}
	return nil
}

func validateDriverCapabilitySnapshot(snapshot map[string]any, spec agents.DriverSpec) error {
	if numberField(snapshot, "schema_version") != int64(agents.DriverCapabilitySchemaVersion) {
		return fmt.Errorf("generation plan driver capability schema_version is required")
	}
	features, err := requireObject(snapshot, "features")
	if err != nil {
		return err
	}
	for _, feature := range agents.AllFeatureIDs() {
		if stringField(features, string(feature)) != string(spec.Capabilities.Features[feature]) {
			return fmt.Errorf("generation plan driver capability feature %s mismatch", feature)
		}
	}
	subCapabilities, err := requireObject(snapshot, "sub_capabilities")
	if err != nil {
		return err
	}
	for _, capability := range agents.AllSubCapabilityIDs() {
		if stringField(subCapabilities, string(capability)) != string(spec.Capabilities.SubCapabilities[capability]) {
			return fmt.Errorf("generation plan driver sub-capability %s mismatch", capability)
		}
	}
	return nil
}

func validateProviderCapabilitySnapshot(snapshot map[string]any, spec agents.RuntimeProviderSpec) error {
	if stringField(snapshot, "vocabulary_version") != spec.CapabilitySnapshot.VocabularyVersion {
		return fmt.Errorf("generation plan runtime provider capability vocabulary mismatch")
	}
	capabilities, ok := snapshot["capabilities"].([]any)
	if !ok || len(capabilities) != len(spec.CapabilitySnapshot.Capabilities) {
		return fmt.Errorf("generation plan runtime provider capability list mismatch")
	}
	for i, expected := range spec.CapabilitySnapshot.Capabilities {
		if fmt.Sprint(capabilities[i]) != expected {
			return fmt.Errorf("generation plan runtime provider capability list mismatch")
		}
	}
	return nil
}
