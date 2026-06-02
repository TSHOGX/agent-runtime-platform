package generationplan

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"harness-platform/orchestrator/internal/store"
)

const (
	AdapterInputDigestDriverAdapter  = "driver_adapter"
	AdapterInputDigestRuntimeAdapter = "runtime_adapter"
)

var adapterInputDigestKinds = []string{
	AdapterInputDigestDriverAdapter,
	AdapterInputDigestRuntimeAdapter,
}

func AdapterInputDigestKinds() []string {
	return append([]string(nil), adapterInputDigestKinds...)
}

func AdapterInputDigestsFromSandboxContract(payload any) (map[string]string, error) {
	object, err := decodeSandboxContractObject(payload)
	if err != nil {
		return nil, err
	}
	inputs := map[string][]string{
		AdapterInputDigestDriverAdapter: {
			"driver",
			"model_access",
			"driver_runtime",
		},
		AdapterInputDigestRuntimeAdapter: {
			"runtime_provider",
			"runtime_adapter",
		},
	}
	kinds := make([]string, 0, len(inputs))
	for kind := range inputs {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)

	out := make(map[string]string, len(kinds))
	for _, kind := range kinds {
		sections := map[string]any{}
		for _, section := range inputs[kind] {
			value, ok := object[section]
			if !ok {
				return nil, fmt.Errorf("sandbox contract adapter input %s missing %s section", kind, section)
			}
			if _, ok := value.(map[string]any); !ok {
				return nil, fmt.Errorf("sandbox contract adapter input %s section %s must be an object", kind, section)
			}
			sections[section] = value
		}
		digest, err := adapterInputDigest(kind, sections)
		if err != nil {
			return nil, err
		}
		out[kind] = digest
	}
	return out, nil
}

func cloneAdapterInputDigests(values map[string]string) map[string]string {
	out := make(map[string]string, len(values))
	for _, kind := range AdapterInputDigestKinds() {
		if value := strings.TrimSpace(values[kind]); value != "" {
			out[kind] = value
		}
	}
	for kind, value := range values {
		if _, ok := out[kind]; ok {
			continue
		}
		if strings.TrimSpace(value) != "" {
			out[kind] = strings.TrimSpace(value)
		}
	}
	return out
}

func adapterInputDigest(kind string, sections map[string]any) (string, error) {
	payload := map[string]any{
		"adapter_input_schema_version": 1,
		"adapter_input_kind":           strings.TrimSpace(kind),
		"projection_version":           store.GenerationPlanProjectionVersion,
		"sections":                     sections,
	}
	canonical, err := store.CanonicalSandboxContractPayload(payload)
	if err != nil {
		return "", fmt.Errorf("canonicalize adapter input digest %s: %w", kind, err)
	}
	return store.SandboxContractDigest(canonical), nil
}

func decodeSandboxContractObject(payload any) (map[string]any, error) {
	var data []byte
	switch v := payload.(type) {
	case []byte:
		data = v
	default:
		canonical, err := store.CanonicalSandboxContractPayload(payload)
		if err != nil {
			return nil, fmt.Errorf("canonicalize sandbox contract adapter inputs: %w", err)
		}
		data = canonical
	}
	var object map[string]any
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.UseNumber()
	if err := decoder.Decode(&object); err != nil {
		return nil, fmt.Errorf("decode sandbox contract adapter inputs: %w", err)
	}
	return object, nil
}
