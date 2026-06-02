package generationplan

import (
	"fmt"

	"harness-platform/orchestrator/internal/agents"
)

func RenderFeaturePolicyPayload(driverSpec agents.DriverSpec, providerSpec agents.RuntimeProviderSpec) (map[string]any, error) {
	policy := agents.DefaultFeaturePolicyForDriver(driverSpec)
	if err := agents.ValidateFeaturePolicy(policy, driverSpec, providerSpec); err != nil {
		return nil, fmt.Errorf("feature policy validation: %w", err)
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
