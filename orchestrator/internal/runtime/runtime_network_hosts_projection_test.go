package runtime

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestRenderNetworkHostsProjectionRejectsNonAliasModelProxyHosts(t *testing.T) {
	dir := t.TempDir()
	tests := []struct {
		name    string
		baseURL string
		want    string
	}{
		{
			name:    "gateway literal",
			baseURL: "http://10.200.1.1:8082",
			want:    "IP literal",
		},
		{
			name:    "localhost",
			baseURL: "http://localhost:8082",
			want:    "host-local",
		},
		{
			name:    "provider upstream",
			baseURL: "http://api.anthropic.com",
			want:    "provider upstream",
		},
		{
			name:    "path",
			baseURL: "http://harness-model-proxy.internal:8082/v1",
			want:    "must not include a path",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			details := testGenerationDetails(dir, "gen_hosts_"+strings.ReplaceAll(tt.name, " ", "_"))
			details.ManifestAnthropicBaseURL = tt.baseURL
			details.HostGatewayIP = "10.200.1.1"
			if _, err := renderNetworkHostsProjection(details); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected %q rejection, got %v", tt.want, err)
			}
		})
	}
}

func TestRenderOptionalNetworkHostsProjectionRejectsNonCanonicalPath(t *testing.T) {
	dir := t.TempDir()
	details := testGenerationDetails(dir, "gen_hosts_path")
	networkHostsPath := filepath.Join(dir, "run", "network", "gen-"+details.GenerationID, "hosts")
	details.NetworkHostsPath = filepath.Dir(networkHostsPath) + string(filepath.Separator) + "same" + string(filepath.Separator) + ".." + string(filepath.Separator) + filepath.Base(networkHostsPath)

	_, err := renderOptionalNetworkHostsProjection(details)
	if err == nil || !strings.Contains(err.Error(), "network hosts path") || !strings.Contains(err.Error(), "canonical absolute") {
		t.Fatalf("expected non-canonical network hosts path error, got %v", err)
	}
}
