package driveradapter

import (
	"strings"
	"testing"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/store"
)

func TestPiConfigProjectionRenderer(t *testing.T) {
	renderer, ok := ConfigProjectionRendererFor(agents.Pi)
	if !ok {
		t.Fatal("pi config projection renderer is not registered")
	}
	payloads, err := renderer(store.RuntimeGenerationDetails{
		Model:                    "claude-test",
		ManifestAnthropicBaseURL: "http://harness-model-proxy.local:8080",
	})
	if err != nil {
		t.Fatalf("render pi config projection: %v", err)
	}
	models, ok := payloads["models"].(map[string]any)
	if !ok {
		t.Fatalf("missing models payload: %+v", payloads)
	}
	providers := models["providers"].(map[string]any)
	harnessProvider := providers[agents.PiHarnessProxyProvider].(map[string]any)
	modelList := harnessProvider["models"].([]map[string]any)
	if harnessProvider["baseUrl"] != "http://harness-model-proxy.local:8080" ||
		harnessProvider["apiKey"] != "harness-model-proxy-dummy-key" ||
		modelList[0]["id"] != "claude-test" {
		t.Fatalf("unexpected pi models payload: %+v", models)
	}
	settings := payloads["settings"].(map[string]any)
	if settings["coding_agent_dir"] != agents.PiCodingAgentDir ||
		settings["session_dir"] != agents.PiSessionDir ||
		settings["provider"] != agents.PiHarnessProxyProvider ||
		settings["model"] != "claude-test" ||
		settings["offline"] != true ||
		settings["telemetry"] != false {
		t.Fatalf("unexpected pi settings payload: %+v", settings)
	}
}

func TestConfigProjectionRendererFailsClosed(t *testing.T) {
	if _, ok := ConfigProjectionRendererFor(agents.Shell); ok {
		t.Fatalf("shell should not have a config projection renderer")
	}
	renderer, ok := ConfigProjectionRendererFor(agents.Pi)
	if !ok {
		t.Fatal("pi config projection renderer is not registered")
	}
	_, err := renderer(store.RuntimeGenerationDetails{ManifestAnthropicBaseURL: "http://harness-model-proxy.local:8080"})
	if err == nil || !strings.Contains(err.Error(), "pi model is required") {
		t.Fatalf("expected missing model error, got %v", err)
	}
	_, err = renderer(store.RuntimeGenerationDetails{Model: "claude-test"})
	if err == nil || !strings.Contains(err.Error(), "pi sandbox model proxy base url is required") {
		t.Fatalf("expected missing base url error, got %v", err)
	}
}
