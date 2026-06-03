package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/bridge"
	"harness-platform/orchestrator/internal/store"
)

func TestPrepareGenerationConcurrentSessionsUseDistinctControlManifests(t *testing.T) {
	dir := t.TempDir()
	rt := New(Config{
		SessionsRoot:         filepath.Join(dir, "sessions"),
		AgentHomesRoot:       filepath.Join(dir, "agent-homes"),
		BundleRoot:           filepath.Join(dir, "bundle", "out"),
		RootFSPath:           filepath.Join(dir, "rootfs"),
		BridgeMode:           "claim-loop",
		BridgeHeartbeat:      20 * time.Second,
		BridgePollInterval:   5 * time.Millisecond,
		ProbeHealthzStatuses: []int{200},
	})
	type prepareCase struct {
		sessionID string
		details   store.RuntimeGenerationDetails
	}
	cases := []prepareCase{
		{sessionID: "sess_a", details: testGenerationDetails(dir, "gen_a")},
		{sessionID: "sess_b", details: testGenerationDetails(dir, "gen_b")},
	}
	cases[0].details.SessionID = cases[0].sessionID
	cases[1].details.SessionID = cases[1].sessionID

	var wg sync.WaitGroup
	errs := make(chan error, len(cases))
	for _, tc := range cases {
		tc := tc
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := rt.PrepareGeneration(context.Background(), withDataVolumePathsForTest(dir, StartRequest{
				SessionID:    tc.sessionID,
				GenerationID: tc.details.GenerationID,
				DriverID:     "claude_code",
				Generation:   tc.details,
			}))
			if err != nil {
				errs <- fmt.Errorf("prepare %s: %w", tc.sessionID, err)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if cases[0].details.ControlManifestPath == cases[1].details.ControlManifestPath {
		t.Fatalf("control manifest paths must be distinct: %s", cases[0].details.ControlManifestPath)
	}
	for _, tc := range cases {
		var manifestFile controlManifestFile
		if err := json.Unmarshal(mustReadFile(t, tc.details.ControlManifestPath), &manifestFile); err != nil {
			t.Fatalf("read manifest %s: %v", tc.details.ControlManifestPath, err)
		}
		if manifestFile.Payload.SessionID != tc.sessionID ||
			manifestFile.Payload.GenerationID != tc.details.GenerationID ||
			manifestFile.Payload.NetworkProfileID != tc.details.NetworkProfileID ||
			manifestFile.Payload.AgentRuntimeProfileID != tc.details.AgentRuntimeProfileID {
			t.Fatalf("manifest %s has wrong identity: %+v want session=%s generation=%s network=%s runtime=%s",
				tc.details.ControlManifestPath,
				manifestFile.Payload,
				tc.sessionID,
				tc.details.GenerationID,
				tc.details.NetworkProfileID,
				tc.details.AgentRuntimeProfileID)
		}
	}
}

func TestPrepareShellGenerationHasNoSecretMount(t *testing.T) {
	dir := t.TempDir()
	rt := New(Config{
		SessionsRoot:         filepath.Join(dir, "sessions"),
		AgentHomesRoot:       filepath.Join(dir, "agent-homes"),
		BundleRoot:           filepath.Join(dir, "bundle", "out"),
		RootFSPath:           filepath.Join(dir, "rootfs"),
		SandboxUID:           testSandboxUID(),
		SandboxGID:           testSandboxGID(),
		BridgeMode:           "claim-loop",
		BridgeHeartbeat:      20 * time.Second,
		BridgePollInterval:   5 * time.Millisecond,
		ProbeHealthzStatuses: []int{200},
	})
	details := testGenerationDetails(dir, "gen_shell")
	details.SessionID = "sess_shell"
	details.DriverID = "sh"
	details.RequiresSecretDrop = false
	details.ManifestAnthropicBaseURL = ""
	details.AnthropicAPIKeySecretID = ""
	details.AnthropicAuthTokenSecretID = ""
	details.SecretVersion = ""
	details.SecretsDirPath = ""

	workspacePath, agentHomePath := dataVolumePathsForTest(dir, "sess_shell", "sh")
	if _, err := rt.PrepareGeneration(context.Background(), withDataVolumePathsForTest(dir, StartRequest{
		SessionID:    "sess_shell",
		GenerationID: details.GenerationID,
		DriverID:     "sh",
		Generation:   details,
	})); err != nil {
		t.Fatalf("prepare shell generation: %v", err)
	}
	specData, err := os.ReadFile(details.SpecPath)
	if err != nil {
		t.Fatalf("read shell spec: %v", err)
	}
	if strings.Contains(string(specData), "/harness-secrets") {
		t.Fatalf("shell spec must not mount secrets: %s", specData)
	}
	var spec runtimeSpec
	if err := json.Unmarshal(specData, &spec); err != nil {
		t.Fatalf("read shell spec json: %v", err)
	}
	if !spec.Root.Readonly {
		t.Fatalf("shell isolated rootfs must be read-only: %+v", spec.Root)
	}
	if spec.Process.User.UID != testSandboxUID() || spec.Process.User.GID != testSandboxGID() {
		t.Fatalf("shell isolated user=%+v want %d:%d", spec.Process.User, testSandboxUID(), testSandboxGID())
	}
	assertRuntimeSpecCapabilityPolicy(t, spec)
	if strings.Contains(mustJSONForTest(t, spec.Process.Capabilities), "CAP_") {
		t.Fatalf("shell isolated capabilities must be empty: %+v", spec.Process.Capabilities)
	}
	for _, destination := range []string{"/sessions", "/agent-homes", "/harness-secrets"} {
		if mountByDestination(spec.Mounts, destination) != nil {
			t.Fatalf("shell isolated spec must not mount %s: %+v", destination, spec.Mounts)
		}
	}
	for _, mount := range spec.Mounts {
		if slices.Contains(mount.Options, "rbind") {
			t.Fatalf("shell isolated mount %s uses recursive bind: %+v", mount.Destination, mount)
		}
	}
	if mountSource(spec.Mounts, "/workspace") != workspacePath {
		t.Fatalf("workspace mount = %q", mountSource(spec.Mounts, "/workspace"))
	}
	if mountSource(spec.Mounts, "/agent-home") != agentHomePath {
		t.Fatalf("agent-home mount = %q", mountSource(spec.Mounts, "/agent-home"))
	}
	if control := mountByDestination(spec.Mounts, "/harness-control"); control == nil || strings.Join(control.Options, ",") != "bind,ro,nosuid,nodev,noexec" {
		t.Fatalf("unexpected isolated control mount: %+v", control)
	}
	bridgeMount := mountByDestination(spec.Mounts, "/harness-control/bridge")
	if bridgeMount == nil ||
		bridgeMount.Source != details.BridgeDirPath ||
		strings.Join(bridgeMount.Options, ",") != "bind,rw,nosuid,nodev,noexec" ||
		bridgeMount.Annotations["dev.gvisor.spec.mount./harness-control/bridge.share"] != "exclusive" {
		t.Fatalf("unexpected isolated bridge mount: %+v", bridgeMount)
	}
	assertReadOnlyBridgeSubmount(t, spec.Mounts, details, bridge.InboxDir)
	assertReadOnlyBridgeSubmount(t, spec.Mounts, details, bridge.HostTmpDir)
	assertNoBridgeSubmount(t, spec.Mounts, bridge.HostHeartbeatDir)
	env := specEnv(spec.Process.Env)
	if env["HARNESS_AGENT_UID"] != fmt.Sprint(testSandboxUID()) ||
		env["HARNESS_AGENT_GID"] != fmt.Sprint(testSandboxGID()) ||
		env["SESSION_WORKSPACE"] != "/workspace" ||
		env["HARNESS_AGENT_HOME"] != "/agent-home" {
		t.Fatalf("shell isolated identity/workspace env missing: %+v", env)
	}
	var manifestFile controlManifestFile
	if err := json.Unmarshal(mustReadFile(t, details.ControlManifestPath), &manifestFile); err != nil {
		t.Fatalf("read shell manifest: %v", err)
	}
	if manifestFile.Payload.WorkspacePath != "/workspace" || manifestFile.Payload.AgentHomePath != "/agent-home" {
		t.Fatalf("shell manifest must use isolated sandbox paths: %+v", manifestFile.Payload)
	}
	if manifestFile.Payload.SandboxModelProxyBaseURL != "" {
		t.Fatalf("shell manifest must not require Claude base URL: %+v", manifestFile.Payload)
	}
	assertControlManifestOmitsHostOnlyFields(t, mustReadFile(t, details.ControlManifestPath), controlManifestForbiddenHostValues(details)...)
}

func TestPrepareGenerationUsesProvidedDataVolumePaths(t *testing.T) {
	dir := t.TempDir()
	rt := New(Config{
		SessionsRoot:         filepath.Join(dir, "unused-sessions"),
		AgentHomesRoot:       filepath.Join(dir, "unused-agent-homes"),
		BundleRoot:           filepath.Join(dir, "bundle", "out"),
		RootFSPath:           filepath.Join(dir, "rootfs"),
		RunscNetwork:         "host",
		SandboxUID:           testSandboxUID(),
		SandboxGID:           testSandboxGID(),
		BridgeMode:           "claim-loop",
		BridgeHeartbeat:      20 * time.Second,
		BridgePollInterval:   5 * time.Millisecond,
		ProbeHealthzStatuses: []int{200},
	})
	details := testGenerationDetails(dir, "gen_data_volume_paths")
	details.SessionID = "sess_data_volume_paths"
	details.DriverID = "sh"
	details.OutputFormat = "shell_pty"
	details.RequiresSecretDrop = false
	details.ManifestAnthropicBaseURL = ""
	workspacePath := filepath.Join(dir, "volumes", "workspaces", details.SessionID)
	agentHomePath := filepath.Join(dir, "volumes", "driver-homes", details.SessionID, "sh")

	if _, err := rt.PrepareGeneration(context.Background(), StartRequest{
		SessionID:         details.SessionID,
		GenerationID:      details.GenerationID,
		DriverID:          "sh",
		Generation:        details,
		WorkspaceHostPath: workspacePath,
		AgentHomeHostPath: agentHomePath,
	}); err != nil {
		t.Fatalf("prepare generation: %v", err)
	}
	var spec runtimeSpec
	if err := json.Unmarshal(mustReadFile(t, details.SpecPath), &spec); err != nil {
		t.Fatalf("read runtime spec: %v", err)
	}
	if mountSource(spec.Mounts, "/workspace") != workspacePath {
		t.Fatalf("workspace mount source=%q want %q", mountSource(spec.Mounts, "/workspace"), workspacePath)
	}
	if mountSource(spec.Mounts, "/agent-home") != agentHomePath {
		t.Fatalf("agent-home mount source=%q want %q", mountSource(spec.Mounts, "/agent-home"), agentHomePath)
	}
}

func TestPrepareGenerationRequiresDataVolumePaths(t *testing.T) {
	dir := t.TempDir()
	rt := New(Config{
		SessionsRoot:   filepath.Join(dir, "unused-sessions"),
		AgentHomesRoot: filepath.Join(dir, "unused-agent-homes"),
		BundleRoot:     filepath.Join(dir, "bundle", "out"),
		RootFSPath:     filepath.Join(dir, "rootfs"),
		RunscNetwork:   "host",
		SandboxUID:     testSandboxUID(),
		SandboxGID:     testSandboxGID(),
	})
	details := testGenerationDetails(dir, "gen_missing_data_volume_paths")
	details.SessionID = "sess_missing_data_volume_paths"
	details.DriverID = "sh"
	details.OutputFormat = "shell_pty"
	details.RequiresSecretDrop = false
	details.ManifestAnthropicBaseURL = ""

	_, err := rt.PrepareGeneration(context.Background(), StartRequest{
		SessionID:    details.SessionID,
		GenerationID: details.GenerationID,
		DriverID:     "sh",
		Generation:   details,
	})
	if err == nil || !strings.Contains(err.Error(), "data volume paths are required") {
		t.Fatalf("expected data volume path rejection, got %v", err)
	}
}

func TestPrepareSandboxGenerationRejectsSecretReferences(t *testing.T) {
	tests := []struct {
		name         string
		sessionID    string
		generationID string
		agent        string
		outputFormat string
	}{
		{name: "claude", sessionID: "sess_claude", generationID: "gen_claude_bad", agent: "claude_code", outputFormat: "stream-json"},
		{name: "shell", sessionID: "sess_shell", generationID: "gen_shell_bad", agent: "sh", outputFormat: "shell_pty"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			rt := New(Config{
				SessionsRoot:   filepath.Join(dir, "sessions"),
				AgentHomesRoot: filepath.Join(dir, "agent-homes"),
				BundleRoot:     filepath.Join(dir, "bundle", "out"),
				RootFSPath:     filepath.Join(dir, "rootfs"),
			})
			details := testGenerationDetails(dir, tc.generationID)
			details.SessionID = tc.sessionID
			details.DriverID = tc.agent
			details.OutputFormat = tc.outputFormat
			details.RequiresSecretDrop = true
			details.SecretsDirPath = filepath.Join(dir, "run", "control", "gen-"+details.GenerationID, "secrets")
			details.AnthropicAPIKeySecretID = "anthropic_api_key"
			details.AnthropicAuthTokenSecretID = "anthropic_auth_token"
			details.SecretVersion = "local"

			_, err := rt.PrepareGeneration(context.Background(), StartRequest{
				SessionID:    tc.sessionID,
				GenerationID: details.GenerationID,
				DriverID:     tc.agent,
				Generation:   details,
			})
			if err == nil {
				t.Fatal("expected sandbox secret rejection")
			}
			if !strings.Contains(err.Error(), "sandbox_secret_disallowed") {
				t.Fatalf("expected sandbox_secret_disallowed, got %v", err)
			}
		})
	}
}

func TestPrepareGenerationRejectsMismatchedIdentity(t *testing.T) {
	dir := t.TempDir()
	details := testGenerationDetails(dir, "gen_mismatch")
	rt := New(Config{
		SessionsRoot:   filepath.Join(dir, "sessions"),
		AgentHomesRoot: filepath.Join(dir, "agent-homes"),
		BundleRoot:     filepath.Join(dir, "bundle", "out"),
		RootFSPath:     filepath.Join(dir, "rootfs"),
	})

	_, err := rt.PrepareGeneration(context.Background(), StartRequest{
		SessionID:    "sess_wrong",
		GenerationID: details.GenerationID,
		DriverID:     "claude_code",
		Generation:   details,
	})
	if err == nil {
		t.Fatal("expected identity mismatch error")
	}
	if !strings.Contains(err.Error(), "generation session mismatch") {
		t.Fatalf("expected generation session mismatch, got %v", err)
	}
}

func TestRuntimeBridgeMetadataComesFromDriverSpec(t *testing.T) {
	dir := t.TempDir()
	rt := New(Config{
		SessionsRoot:         filepath.Join(dir, "sessions"),
		AgentHomesRoot:       filepath.Join(dir, "agent-homes"),
		BundleRoot:           filepath.Join(dir, "bundle", "out"),
		RootFSPath:           filepath.Join(dir, "rootfs"),
		BridgeMode:           "claim-loop",
		BridgeHeartbeat:      20 * time.Second,
		BridgePollInterval:   5 * time.Millisecond,
		ProbeHealthzStatuses: []int{200},
	})
	details := testGenerationDetails(dir, "gen_driver_metadata")
	driverSpec, ok := agents.DriverSpecFor("claude_code")
	if !ok {
		t.Fatal("missing claude_code driver spec")
	}
	driverSpec.BridgeProtocolVersion = 42
	driverSpec.TurnInputSchema = "SpecTurn"
	req := withDataVolumePathsForTest(dir, StartRequest{
		SessionID:    details.SessionID,
		GenerationID: details.GenerationID,
		DriverID:     "claude_code",
		Generation:   details,
	})

	manifest, err := rt.buildGenerationManifest(req, driverSpec, "runsc test", "bundle_digest", "runtime_config_digest", "spec_digest")
	if err != nil {
		t.Fatalf("build generation manifest: %v", err)
	}
	if manifest.BridgeProtocolVersion != 42 || manifest.TurnInputSchema != "SpecTurn" {
		t.Fatalf("manifest bridge metadata = %d/%q, want spec values", manifest.BridgeProtocolVersion, manifest.TurnInputSchema)
	}

	spec, _, err := rt.renderRuntimeSpecWithDriverSpec(req, driverSpec)
	if err != nil {
		t.Fatalf("render runtime spec: %v", err)
	}
	env := specEnv(spec.Process.Env)
	if env["HARNESS_BRIDGE_PROTOCOL_VERSION"] != "42" || env["HARNESS_TURN_INPUT_SCHEMA"] != "SpecTurn" {
		t.Fatalf("runtime spec bridge env = %+v, want spec values", env)
	}
}

func TestPrepareClaudeHostOnlyGenerationHasNoSecretMount(t *testing.T) {
	dir := t.TempDir()
	rt := New(Config{
		SessionsRoot:         filepath.Join(dir, "sessions"),
		AgentHomesRoot:       filepath.Join(dir, "agent-homes"),
		BundleRoot:           filepath.Join(dir, "bundle", "out"),
		RootFSPath:           filepath.Join(dir, "rootfs"),
		BridgeMode:           "claim-loop",
		BridgeHeartbeat:      20 * time.Second,
		BridgePollInterval:   5 * time.Millisecond,
		ProbeHealthzStatuses: []int{200},
	})
	details := testGenerationDetails(dir, "gen_host_only")
	details.RequiresSecretDrop = false
	details.ManifestAnthropicBaseURL = "http://harness-model-proxy.internal:8082"
	details.AnthropicAPIKeySecretID = ""
	details.AnthropicAuthTokenSecretID = ""
	details.SecretVersion = ""
	details.SecretsDirPath = ""
	details.NetworkHostsPath = filepath.Join(dir, "run", "network", "gen-"+details.GenerationID, "hosts")

	if _, err := rt.PrepareGeneration(context.Background(), withDataVolumePathsForTest(dir, StartRequest{
		SessionID:    "sess_1",
		GenerationID: details.GenerationID,
		DriverID:     "claude_code",
		Generation:   details,
	})); err != nil {
		t.Fatalf("prepare host-only claude generation: %v", err)
	}

	manifestData := mustReadFile(t, details.ControlManifestPath)
	var manifestFile controlManifestFile
	if err := json.Unmarshal(manifestData, &manifestFile); err != nil {
		t.Fatalf("read host-only manifest: %v", err)
	}
	manifest := manifestFile.Payload
	if manifest.SandboxModelProxyBaseURL != "http://harness-model-proxy.internal:8082" {
		t.Fatalf("unexpected host-only base url: %+v", manifest)
	}
	assertControlManifestOmitsHostOnlyFields(t, manifestData, controlManifestForbiddenHostValues(details)...)
	if strings.Contains(string(manifestData), "/harness-secrets") ||
		strings.Contains(string(manifestData), "anthropic_api_key") ||
		strings.Contains(string(manifestData), "anthropic_auth_token") {
		t.Fatalf("host-only manifest contains removed secret references: %s", manifestData)
	}

	specData := mustReadFile(t, details.SpecPath)
	if strings.Contains(string(specData), "/harness-secrets") ||
		strings.Contains(string(specData), "anthropic_api_key") ||
		strings.Contains(string(specData), "anthropic_auth_token") {
		t.Fatalf("host-only spec contains removed secret references: %s", specData)
	}
	var spec runtimeSpec
	if err := json.Unmarshal(specData, &spec); err != nil {
		t.Fatalf("read host-only spec: %v", err)
	}
	if mountByDestination(spec.Mounts, "/harness-secrets") != nil {
		t.Fatalf("host-only spec must not mount secrets: %+v", spec.Mounts)
	}
	if mountSource(spec.Mounts, "/etc/hosts") != details.NetworkHostsPath {
		t.Fatalf("host-only spec must mount network hosts projection: %+v", spec.Mounts)
	}
	hostsData := mustReadFile(t, details.NetworkHostsPath)
	if string(hostsData) != "127.0.0.1 localhost\n::1 localhost ip6-localhost ip6-loopback\n10.200.1.1 harness-model-proxy.internal\n" {
		t.Fatalf("unexpected network hosts projection: %s", hostsData)
	}
}

func TestPreparePiGenerationMaterializesReadOnlyConfig(t *testing.T) {
	dir := t.TempDir()
	rt := New(Config{
		SessionsRoot:         filepath.Join(dir, "sessions"),
		AgentHomesRoot:       filepath.Join(dir, "agent-homes"),
		BundleRoot:           filepath.Join(dir, "bundle", "out"),
		RootFSPath:           filepath.Join(dir, "rootfs"),
		BridgeMode:           "claim-loop",
		BridgeHeartbeat:      20 * time.Second,
		BridgePollInterval:   5 * time.Millisecond,
		ProbeHealthzStatuses: []int{200},
	})
	details := testGenerationDetails(dir, "gen_pi_config")
	details.SessionID = "sess_pi"
	details.DriverID = "pi"
	details.OutputFormat = "pi_rpc_events_v1.0"
	details.Model = "sonnet"
	details.ManifestAnthropicBaseURL = "http://harness-model-proxy.internal:8082"

	artifacts, err := rt.PrepareGeneration(context.Background(), withDataVolumePathsForTest(dir, StartRequest{
		SessionID:    "sess_pi",
		GenerationID: details.GenerationID,
		DriverID:     "pi",
		Generation:   details,
	}))
	if err != nil {
		t.Fatalf("prepare pi generation: %v", err)
	}
	if len(artifacts.MaterializedDriverConfig) != 2 {
		t.Fatalf("materialized pi config = %+v", artifacts.MaterializedDriverConfig)
	}
	entries := map[string]DriverConfigMaterialization{}
	for _, entry := range artifacts.MaterializedDriverConfig {
		entries[entry.Name] = entry
		data := mustReadFile(t, entry.HostSourcePath)
		if prefixedSHA256(data) != entry.SourceDigest {
			t.Fatalf("%s digest mismatch: entry=%s data=%s", entry.Name, entry.SourceDigest, data)
		}
		if entry.DestinationMutableBySandbox {
			t.Fatalf("%s config destination must be immutable: %+v", entry.Name, entry)
		}
	}
	if entries["models"].SourceProjectionPath != agents.PiModelsConfigPath ||
		entries["models"].SandboxDestination != agents.PiModelsSandboxPath ||
		entries["settings"].SourceProjectionPath != agents.PiSettingsConfigPath ||
		entries["settings"].SandboxDestination != agents.PiSettingsSandboxPath {
		t.Fatalf("unexpected pi materialization entries: %+v", entries)
	}

	var models map[string]any
	if err := json.Unmarshal(mustReadFile(t, entries["models"].HostSourcePath), &models); err != nil {
		t.Fatalf("parse pi models config: %v", err)
	}
	providers, ok := models["providers"].(map[string]any)
	if !ok {
		t.Fatalf("pi models config providers must be an object: %+v", models)
	}
	provider, ok := providers[agents.PiHarnessProxyProvider].(map[string]any)
	if !ok {
		t.Fatalf("pi models config missing harness proxy provider: %+v", models)
	}
	if provider["baseUrl"] != "http://harness-model-proxy.internal:8082" ||
		provider["api"] != "anthropic-messages" ||
		provider["apiKey"] != "harness-model-proxy-dummy-key" {
		t.Fatalf("unexpected pi provider config: %+v", provider)
	}
	modelEntries, ok := provider["models"].([]any)
	if !ok || len(modelEntries) != 1 {
		t.Fatalf("unexpected pi provider models: %+v", provider["models"])
	}
	modelEntry, ok := modelEntries[0].(map[string]any)
	if !ok || modelEntry["id"] != "sonnet" {
		t.Fatalf("unexpected pi model entry: %+v", modelEntries[0])
	}
	if _, ok := models["schema_version"]; ok {
		t.Fatalf("pi models config must use Pi native schema without harness schema_version: %+v", models)
	}
	if _, ok := models["models"]; ok {
		t.Fatalf("pi models config must not use removed top-level models array: %+v", models)
	}
	if _, ok := providers["anthropic"]; ok {
		t.Fatalf("pi models config must not use built-in anthropic provider: %+v", models)
	}
	modelsJSON := string(mustJSONForTest(t, models))
	if strings.Contains(modelsJSON, "sk-ant-") || strings.Contains(modelsJSON, "ANTHROPIC_API_KEY") {
		t.Fatalf("pi models config leaked provider credentials: %s", modelsJSON)
	}
	var settings map[string]any
	if err := json.Unmarshal(mustReadFile(t, entries["settings"].HostSourcePath), &settings); err != nil {
		t.Fatalf("parse pi settings config: %v", err)
	}
	if settings["coding_agent_dir"] != agents.PiCodingAgentDir ||
		settings["session_dir"] != agents.PiSessionDir ||
		settings["offline"] != true ||
		settings["skip_version_check"] != true ||
		settings["telemetry"] != false {
		t.Fatalf("unexpected pi settings config: %+v", settings)
	}
	_, piAgentHomePath := dataVolumePathsForTest(dir, "sess_pi", "pi")
	for _, check := range []struct {
		path string
		mode os.FileMode
	}{
		{path: filepath.Join(piAgentHomePath, ".pi"), mode: 0o750},
		{path: filepath.Join(piAgentHomePath, ".pi", "agent"), mode: 0o750},
		{path: filepath.Join(piAgentHomePath, ".pi", "agent", "sessions"), mode: 0o750},
	} {
		assertDirOwnership(t, check.path, testSandboxUID(), testSandboxGID(), check.mode)
	}

	var spec runtimeSpec
	if err := json.Unmarshal(mustReadFile(t, details.SpecPath), &spec); err != nil {
		t.Fatalf("parse pi runtime spec: %v", err)
	}
	modelsMount := mountByDestination(spec.Mounts, agents.PiModelsSandboxPath)
	settingsMount := mountByDestination(spec.Mounts, agents.PiSettingsSandboxPath)
	if modelsMount == nil || modelsMount.Source != entries["models"].HostSourcePath || strings.Join(modelsMount.Options, ",") != "bind,ro,nosuid,nodev,noexec" {
		t.Fatalf("unexpected pi models mount: %+v", modelsMount)
	}
	if settingsMount == nil || settingsMount.Source != entries["settings"].HostSourcePath || strings.Join(settingsMount.Options, ",") != "bind,ro,nosuid,nodev,noexec" {
		t.Fatalf("unexpected pi settings mount: %+v", settingsMount)
	}
	env := specEnv(spec.Process.Env)
	if env["PI_CODING_AGENT_DIR"] != agents.PiCodingAgentDir ||
		env["PI_CODING_AGENT_SESSION_DIR"] != agents.PiSessionDir ||
		env["PI_OFFLINE"] != "1" ||
		env["PI_SKIP_VERSION_CHECK"] != "1" ||
		env["PI_TELEMETRY"] != "0" {
		t.Fatalf("runtime spec missing pi env: %+v", env)
	}

	var manifestFile controlManifestFile
	if err := json.Unmarshal(mustReadFile(t, details.ControlManifestPath), &manifestFile); err != nil {
		t.Fatalf("parse pi control manifest: %v", err)
	}
	driverRuntime := manifestFile.Payload.DriverRuntime
	if driverRuntime["pi_coding_agent_dir"] != agents.PiCodingAgentDir ||
		driverRuntime["pi_coding_agent_session_dir"] != agents.PiSessionDir ||
		driverRuntime["pi_offline"] != true ||
		driverRuntime["pi_skip_version_check"] != true ||
		driverRuntime["pi_telemetry_disabled"] != true {
		t.Fatalf("control manifest missing pi startup gates: %+v", manifestFile.Payload)
	}
	var rawManifestFile struct {
		Payload map[string]json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(mustReadFile(t, details.ControlManifestPath), &rawManifestFile); err != nil {
		t.Fatalf("parse raw pi control manifest: %v", err)
	}
	if _, ok := rawManifestFile.Payload["driver_runtime"]; !ok {
		t.Fatalf("control manifest missing driver_runtime: %s", mustReadFile(t, details.ControlManifestPath))
	}
	for _, field := range []string{
		"pi_coding_agent_dir",
		"pi_coding_agent_session_dir",
		"pi_offline",
		"pi_skip_version_check",
		"pi_telemetry_disabled",
	} {
		if _, ok := rawManifestFile.Payload[field]; ok {
			t.Fatalf("control manifest must not contain top-level %s: %s", field, mustReadFile(t, details.ControlManifestPath))
		}
	}
}
