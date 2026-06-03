package runtime

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRenderRuntimeSpecRequiresExplicitBridgeProbeConfig(t *testing.T) {
	dir := t.TempDir()
	rt := New(Config{
		SessionsRoot:   filepath.Join(dir, "sessions"),
		AgentHomesRoot: filepath.Join(dir, "agent-homes"),
		BundleRoot:     filepath.Join(dir, "bundle", "out"),
		RootFSPath:     filepath.Join(dir, "rootfs"),
	})
	details := testGenerationDetails(dir, "gen_missing_bridge_config")

	_, _, err := rt.renderRuntimeSpec(withDataVolumePathsForTest(dir, StartRequest{
		SessionID:    details.SessionID,
		GenerationID: details.GenerationID,
		DriverID:     "claude_code",
		Generation:   details,
	}))
	if err == nil || !strings.Contains(err.Error(), "bridge mode is required") {
		t.Fatalf("expected missing bridge config error, got %v", err)
	}
}

func TestProjectedControlManifestDigestIgnoresRegenerableFields(t *testing.T) {
	base := testControlManifest()
	first, err := projectedControlManifestDigest(base)
	if err != nil {
		t.Fatalf("project base manifest: %v", err)
	}
	changed := base
	changed.CreatedAt = "2030-01-01T00:00:00Z"
	changed.AttemptID = "attempt-2"
	second, err := projectedControlManifestDigest(changed)
	if err != nil {
		t.Fatalf("project changed manifest: %v", err)
	}
	if first != second {
		t.Fatalf("regenerable fields changed projected digest: %s != %s", first, second)
	}
	strictChanged := base
	strictChanged.EgressPolicyDigest = "rotated_egress_digest"
	third, err := projectedControlManifestDigest(strictChanged)
	if err != nil {
		t.Fatalf("project strict changed manifest: %v", err)
	}
	if first == third {
		t.Fatalf("strict field change did not change projected digest: %s", first)
	}
}

func TestProjectedControlManifestDigestRejectsHostOnlyFields(t *testing.T) {
	tests := []struct {
		name  string
		field string
		value string
	}{
		{name: "host hostname", field: "host_hostname", value: "host-a"},
		{name: "netns name", field: "netns_name", value: "harness-gen-a"},
		{name: "netns path", field: "netns_path", value: "/var/run/netns/harness-gen-a"},
		{name: "host veth", field: "host_veth", value: "hgenah"},
		{name: "sandbox veth", field: "sandbox_veth", value: "hgenas"},
		{name: "nft table", field: "nft_table_name", value: "harness_gen_a"},
		{name: "host gateway", field: "host_gateway_ip", value: "10.200.1.1"},
		{name: "sandbox source", field: "sandbox_source_ip", value: "10.200.1.2"},
		{name: "bridge dir", field: "bridge_dir_path", value: "/tmp/bridge-a"},
		{name: "proxy bind", field: "proxy_bind_url", value: "http://0.0.0.0:8082"},
		{name: "runsc path", field: "runsc_binary_path", value: "/usr/local/bin/runsc"},
		{name: "checkpoint path", field: "checkpoint_path", value: "/tmp/checkpoint"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data, err := json.Marshal(testControlManifest())
			if err != nil {
				t.Fatalf("marshal test manifest: %v", err)
			}
			var payload map[string]any
			if err := json.Unmarshal(data, &payload); err != nil {
				t.Fatalf("unmarshal test manifest: %v", err)
			}
			payload[tc.field] = tc.value
			_, err = projectedControlManifestPayloadDigest(payload)
			if err == nil || !strings.Contains(err.Error(), `unclassified control manifest field "`+tc.field+`"`) {
				t.Fatalf("expected %s rejection, got %v", tc.field, err)
			}
		})
	}
}

func TestCanonicalManifestDigestMatchesSandboxProjectionFixture(t *testing.T) {
	data := mustReadFile(t, filepath.Join("testdata", "control-manifest-payload.sandbox-isolation-v1.json"))
	var payload any
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatalf("read canonical manifest fixture: %v", err)
	}
	canonical, err := canonicalJSON(payload)
	if err != nil {
		t.Fatalf("canonicalize manifest fixture: %v", err)
	}
	const wantCanonical = `{"agent_home_path":"/agent-home","agent_runtime_profile_id":"arp_fixture","attempt_id":"attempt_fixture","bridge_protocol_version":2,"bundle_digest":"bundle_digest_fixture","created_at":"2026-05-25T00:00:00Z","driver_id":"sh","egress_policy_digest":"egress_digest_fixture","generation_id":"gen_fixture","manifest_version":1,"network_profile_id":"net_fixture","output_format":"stream-json","runsc_platform":"systrap","runsc_version":"runsc release-20260511.0","runtime_config_digest":"runtime_config_digest_fixture","sandbox_contract_version":"sandbox-isolation-v1","session_id":"sess_fixture","spec_digest":"spec_digest_fixture","turn_input_schema":"RunTurn","workspace_path":"/workspace"}`
	const wantDigest = "a027f6f46bfb30bd0f4a400a6d90def318ac881e383ec59693aee4c57d47d68c"
	if string(canonical) != wantCanonical {
		t.Fatalf("canonical fixture mismatch:\ngot  %s\nwant %s", canonical, wantCanonical)
	}
	if got := digestHex(canonical); got != wantDigest {
		t.Fatalf("canonical fixture digest=%s want %s", got, wantDigest)
	}
}

func TestRenderRuntimeSpecUsesGenerationNetnsPath(t *testing.T) {
	dir := t.TempDir()
	rt := New(Config{
		SessionsRoot:         filepath.Join(dir, "sessions"),
		AgentHomesRoot:       filepath.Join(dir, "agent-homes"),
		BundleRoot:           filepath.Join(dir, "bundle", "out"),
		RootFSPath:           filepath.Join(dir, "rootfs"),
		RunscNetwork:         "sandbox",
		BridgeMode:           "claim-loop",
		BridgeHeartbeat:      20 * time.Second,
		BridgePollInterval:   5 * time.Millisecond,
		ProbeHealthzStatuses: []int{200},
	})
	details := testGenerationDetails(dir, "gen_netns")
	details.RunscNetwork = "sandbox"
	details.NetnsPath = "/var/run/netns/harness-gen-netns"
	spec, _, err := rt.renderRuntimeSpec(withDataVolumePathsForTest(dir, StartRequest{
		SessionID:    "sess_1",
		GenerationID: details.GenerationID,
		DriverID:     "claude_code",
		Generation:   details,
	}))
	if err != nil {
		t.Fatalf("render runtime spec: %v", err)
	}
	if !strings.Contains(string(spec.Linux), details.NetnsPath) {
		t.Fatalf("spec linux must contain generation netns path %q: %s", details.NetnsPath, spec.Linux)
	}
	if strings.Contains(string(spec.Linux), "shared-demo-netns") {
		t.Fatalf("spec linux must not contain shared netns path: %s", spec.Linux)
	}
}
