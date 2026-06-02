package driveradapter

import (
	"fmt"
	"strings"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/store"
)

type ConfigProjectionRenderer func(store.RuntimeGenerationDetails) (map[string]any, error)

var configProjectionRenderers = map[agents.ID]ConfigProjectionRenderer{
	agents.Pi: renderPiConfigProjection,
}

func ConfigProjectionRendererFor(driver agents.ID) (ConfigProjectionRenderer, bool) {
	renderer, ok := configProjectionRenderers[driver]
	return renderer, ok
}

func renderPiConfigProjection(details store.RuntimeGenerationDetails) (map[string]any, error) {
	model := strings.TrimSpace(details.Model)
	if model == "" {
		return nil, fmt.Errorf("pi model is required")
	}
	baseURL := strings.TrimSpace(details.ManifestAnthropicBaseURL)
	if baseURL == "" {
		return nil, fmt.Errorf("pi sandbox model proxy base url is required")
	}
	return map[string]any{
		"models": map[string]any{
			"providers": map[string]any{
				agents.PiHarnessProxyProvider: map[string]any{
					"baseUrl": baseURL,
					"api":     "anthropic-messages",
					"apiKey":  "harness-model-proxy-dummy-key",
					"models": []map[string]any{
						{
							"id": model,
						},
					},
				},
			},
		},
		"settings": map[string]any{
			"schema_version":      1,
			"coding_agent_dir":    agents.PiCodingAgentDir,
			"session_dir":         agents.PiSessionDir,
			"offline":             true,
			"skip_version_check":  true,
			"telemetry":           false,
			"provider":            agents.PiHarnessProxyProvider,
			"model":               model,
			"production_sessions": true,
		},
	}, nil
}
