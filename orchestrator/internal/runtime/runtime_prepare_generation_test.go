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
