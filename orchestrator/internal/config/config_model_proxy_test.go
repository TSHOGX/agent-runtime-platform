package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadProjectConfigRejectsMissingModelProxySandboxBaseURL(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "harness.yaml")
	if err := os.WriteFile(path, []byte(minimalHarnessYAMLWithModelProxy(`  model_proxy:
    bind_url: http://0.0.0.0:8083
`)), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, err := loadProjectConfig(path)
	if err == nil || !strings.Contains(err.Error(), "harness.model_proxy.sandbox_base_url is required") {
		t.Fatalf("expected missing sandbox_base_url rejection, got %v", err)
	}
}

func TestLoadProjectConfigRejectsInvalidModelProxyBindURL(t *testing.T) {
	tests := []struct {
		name    string
		bindURL string
		want    string
	}{
		{
			name:    "missing port",
			bindURL: "http://0.0.0.0",
			want:    "must include an explicit port",
		},
		{
			name:    "invalid port",
			bindURL: "http://0.0.0.0:70000",
			want:    "invalid port",
		},
		{
			name:    "non http scheme",
			bindURL: "https://0.0.0.0:8082",
			want:    "must use http scheme",
		},
		{
			name:    "non local host",
			bindURL: "http://192.0.2.1:8082",
			want:    "host must be an unspecified address",
		},
		{
			name:    "loopback ipv4 host",
			bindURL: "http://127.0.0.1:8082",
			want:    "host must be an unspecified address",
		},
		{
			name:    "localhost",
			bindURL: "http://localhost:8082",
			want:    "host must be an unspecified address",
		},
		{
			name:    "loopback ipv6 host",
			bindURL: "http://[::1]:8082",
			want:    "host must be an unspecified address",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "harness.yaml")
			if err := os.WriteFile(path, []byte(minimalHarnessYAMLWithModelProxy(`  model_proxy:
    bind_url: `+tt.bindURL+`
    sandbox_base_url: http://harness-model-proxy.internal:8082
`)), 0o644); err != nil {
				t.Fatalf("write config: %v", err)
			}

			_, err := loadProjectConfig(path)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected %q error, got %v", tt.want, err)
			}
		})
	}
}

func TestLoadProjectConfigRejectsMismatchedModelProxySandboxPort(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "harness.yaml")
	if err := os.WriteFile(path, []byte(minimalHarnessYAMLWithModelProxy(`  model_proxy:
    bind_url: http://0.0.0.0:8083
    sandbox_base_url: http://harness-model-proxy.internal:8082
`)), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, err := loadProjectConfig(path)
	if err == nil || !strings.Contains(err.Error(), "sandbox_base_url port 8082 must match bind_url port 8083") {
		t.Fatalf("expected sandbox port mismatch rejection, got %v", err)
	}
}

func TestLoadProjectConfigRejectsMalformedModelProxySandboxBaseURL(t *testing.T) {
	tests := []struct {
		name           string
		sandboxBaseURL string
		want           string
	}{
		{
			name:           "non http scheme",
			sandboxBaseURL: "https://harness-model-proxy.internal:8082",
			want:           "sandbox_base_url must use http scheme",
		},
		{
			name:           "path",
			sandboxBaseURL: "http://harness-model-proxy.internal:8082/v1",
			want:           "sandbox_base_url must not include a path",
		},
		{
			name:           "userinfo",
			sandboxBaseURL: "http://user@harness-model-proxy.internal:8082",
			want:           "sandbox_base_url must not include userinfo, query, or fragment",
		},
		{
			name:           "query",
			sandboxBaseURL: "http://harness-model-proxy.internal:8082?target=v1",
			want:           "sandbox_base_url must not include userinfo, query, or fragment",
		},
		{
			name:           "fragment",
			sandboxBaseURL: "http://harness-model-proxy.internal:8082#v1",
			want:           "sandbox_base_url must not include userinfo, query, or fragment",
		},
		{
			name:           "missing host",
			sandboxBaseURL: "http://:8082",
			want:           "sandbox_base_url must include a host",
		},
		{
			name:           "missing port",
			sandboxBaseURL: "http://harness-model-proxy.internal",
			want:           "sandbox_base_url must include an explicit port matching bind_url",
		},
		{
			name:           "invalid port",
			sandboxBaseURL: "http://harness-model-proxy.internal:70000",
			want:           "sandbox_base_url contains invalid port",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "harness.yaml")
			if err := os.WriteFile(path, []byte(minimalHarnessYAMLWithModelProxy(`  model_proxy:
    bind_url: http://0.0.0.0:8082
    sandbox_base_url: `+tt.sandboxBaseURL+`
`)), 0o644); err != nil {
				t.Fatalf("write config: %v", err)
			}

			_, err := loadProjectConfig(path)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected %q error, got %v", tt.want, err)
			}
		})
	}
}

func TestLoadExposesModelProxyConfig(t *testing.T) {
	repo := t.TempDir()
	configDir := filepath.Join(repo, "config")
	if err := os.Mkdir(configDir, 0o755); err != nil {
		t.Fatalf("mkdir config dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(configDir, "harness.yaml"), []byte(minimalHarnessYAMLWithModelProxy(`  model_proxy:
    bind_url: http://0.0.0.0:8083
    sandbox_base_url: http://harness-model-proxy.internal:8083
`)), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	chdirForLoadTest(t, repo)
	unsetEnvForTest(t, "HARNESS_SESSION_TTL")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.ModelProxy.BindURL != "http://0.0.0.0:8083" ||
		cfg.ModelProxy.BindPort != 8083 ||
		cfg.ModelProxy.SandboxBaseURL != "http://harness-model-proxy.internal:8083" {
		t.Fatalf("unexpected exposed model proxy config: %+v", cfg.ModelProxy)
	}
}
