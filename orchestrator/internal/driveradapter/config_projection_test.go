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

func TestPiRuntimeLayoutSpec(t *testing.T) {
	layout, ok := RuntimeLayoutSpecFor(agents.Pi)
	if !ok {
		t.Fatalf("pi runtime layout spec missing")
	}
	env := map[string]string{}
	for _, item := range layout.Env {
		env[item.Name] = item.Value
	}
	if env["PI_CODING_AGENT_DIR"] != agents.PiCodingAgentDir ||
		env["PI_CODING_AGENT_SESSION_DIR"] != agents.PiSessionDir ||
		env["PI_OFFLINE"] != "1" ||
		env["PI_SKIP_VERSION_CHECK"] != "1" ||
		env["PI_TELEMETRY"] != "0" {
		t.Fatalf("unexpected pi env layout: %+v", layout.Env)
	}
	if len(layout.HomeDirs) != 3 ||
		layout.HomeDirs[0].AgentHomeRelativePath != ".pi" ||
		layout.HomeDirs[1].AgentHomeRelativePath != ".pi/agent" ||
		layout.HomeDirs[2].AgentHomeRelativePath != ".pi/agent/sessions" {
		t.Fatalf("unexpected pi home dir layout: %+v", layout.HomeDirs)
	}
	if layout.ControlManifest.Fields["pi_coding_agent_dir"] != agents.PiCodingAgentDir ||
		layout.ControlManifest.Fields["pi_coding_agent_session_dir"] != agents.PiSessionDir ||
		layout.ControlManifest.Fields["pi_offline"] != true ||
		layout.ControlManifest.Fields["pi_skip_version_check"] != true ||
		layout.ControlManifest.Fields["pi_telemetry_disabled"] != true {
		t.Fatalf("unexpected pi manifest layout: %+v", layout.ControlManifest)
	}

	layout.Env[0].Value = "mutated"
	layout.ControlManifest.Fields["pi_coding_agent_dir"] = "mutated"
	layoutAgain, ok := RuntimeLayoutSpecFor(agents.Pi)
	if !ok ||
		layoutAgain.Env[0].Value != agents.PiCodingAgentDir ||
		layoutAgain.ControlManifest.Fields["pi_coding_agent_dir"] != agents.PiCodingAgentDir {
		t.Fatalf("runtime layout spec should be cloned, got %+v/%+v/%v", layoutAgain.Env, layoutAgain.ControlManifest, ok)
	}
}

func TestRuntimeLayoutSpecForUnsupportedDriver(t *testing.T) {
	if _, ok := RuntimeLayoutSpecFor(agents.Shell); ok {
		t.Fatalf("shell should not have a runtime layout spec")
	}
}
