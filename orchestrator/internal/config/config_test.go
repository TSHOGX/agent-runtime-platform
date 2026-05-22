package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadProjectConfigUsesHarnessProxyConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "harness.yaml")
	if err := os.WriteFile(path, []byte(`runtime:
  runsc_network: sandbox
  runsc_overlay2: none

claude:
  proxy_bind_url: http://0.0.0.0:8082
  sandbox_base_url: http://10.200.1.1:8082
  api_key: "123"
  auth_token: "123"
  model: sonnet
  output_format: stream-json
  disable_nonessential_traffic: true
`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := loadProjectConfig(path)
	if err != nil {
		t.Fatalf("load project config: %v", err)
	}
	if cfg.Runtime.RunscNetwork != "sandbox" || cfg.Runtime.RunscOverlay2 != "none" {
		t.Fatalf("unexpected runtime config: %+v", cfg.Runtime)
	}
	if cfg.Claude.ProxyBindURL != "http://0.0.0.0:8082" ||
		cfg.Claude.SandboxBaseURL != "http://10.200.1.1:8082" ||
		cfg.Claude.APIKey != "123" ||
		cfg.Claude.AuthToken != "123" {
		t.Fatalf("unexpected claude config: %+v", cfg.Claude)
	}
	if !cfg.Claude.DisableNonessentialTraffic {
		t.Fatalf("expected nonessential traffic disabled: %+v", cfg.Claude)
	}
}

func TestLoadProjectConfigMissingFileUsesDefaults(t *testing.T) {
	cfg, err := loadProjectConfig(filepath.Join(t.TempDir(), "missing.yaml"))
	if err != nil {
		t.Fatalf("missing config should not fail: %v", err)
	}
	if !cfg.Claude.DisableNonessentialTraffic {
		t.Fatalf("expected default nonessential traffic setting to be true")
	}
}
