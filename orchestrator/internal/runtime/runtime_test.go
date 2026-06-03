package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/bridge"
	"harness-platform/orchestrator/internal/store"
)

func TestRuntimeStartRejectsUnsupportedDriver(t *testing.T) {
	rt := New(Config{})
	res := rt.Start(context.Background(), StartRequest{
		SessionID: "sess_1",
		DriverID:  "opencode",
	}, nil)
	if res.Err == nil {
		t.Fatal("expected unsupported driver error")
	}
	if !strings.Contains(res.Err.Error(), "unsupported driver") {
		t.Fatalf("expected unsupported driver error, got %v", res.Err)
	}
}

func TestRuntimeStartRequiresExplicitDriverID(t *testing.T) {
	rt := New(Config{})
	res := rt.Start(context.Background(), StartRequest{
		SessionID:    "sess_1",
		GenerationID: "gen_1",
		Generation: store.RuntimeGenerationDetails{
			SessionID:    "sess_1",
			GenerationID: "gen_1",
			DriverID:     "claude_code",
		},
	}, nil)
	if res.Err == nil {
		t.Fatal("expected missing driver id error")
	}
	if !strings.Contains(res.Err.Error(), "driver id is required") {
		t.Fatalf("expected driver id required error, got %v", res.Err)
	}
}

func TestRuntimeResourceIdentifiersFailClosed(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "empty", value: ""},
		{name: "whitespace only", value: " \t\n"},
		{name: "all invalid", value: "!!!"},
		{name: "underscore only", value: "___"},
	}
	for _, tc := range tests {
		t.Run("short id "+tc.name, func(t *testing.T) {
			if _, err := shortID(tc.value); err == nil || !strings.Contains(err.Error(), "short generation id is required") {
				t.Fatalf("expected short id error, got %v", err)
			}
		})
		t.Run("nft identifier "+tc.name, func(t *testing.T) {
			if _, err := hostEgressTableName(tc.value); err == nil || !strings.Contains(err.Error(), "nft identifier is required") {
				t.Fatalf("expected nft identifier error, got %v", err)
			}
		})
	}
}

func TestPathIsMountPointDetectsRootAndTempDir(t *testing.T) {
	rootIsMount, err := pathIsMountPoint(string(filepath.Separator))
	if err != nil {
		t.Fatalf("inspect filesystem root mountpoint: %v", err)
	}
	if !rootIsMount {
		t.Fatal("filesystem root should be detected as a mountpoint")
	}
	tempIsMount, err := pathIsMountPoint(t.TempDir())
	if err != nil {
		t.Fatalf("inspect temp dir mountpoint: %v", err)
	}
	if tempIsMount {
		t.Fatal("plain temp dir should not be detected as a mountpoint")
	}
}

func TestRuntimeStartRequiresGenerationDetailsForColdPath(t *testing.T) {
	rt := New(Config{
		SessionsRoot:   filepath.Join(t.TempDir(), "sessions"),
		AgentHomesRoot: filepath.Join(t.TempDir(), "agent-homes"),
		BundleRoot:     filepath.Join(t.TempDir(), "bundle", "out"),
		RunscNetwork:   "host",
	})
	res := rt.Start(context.Background(), StartRequest{
		SessionID: "sess_1",
		DriverID:  "claude_code",
	}, nil)
	if res.Err == nil {
		t.Fatal("expected missing generation details error")
	}
	if !strings.Contains(res.Err.Error(), "generation details are required") {
		t.Fatalf("expected generation details error, got %v", res.Err)
	}
}

func TestValidateGenerationDetailsRequiresExplicitSandboxContractVersion(t *testing.T) {
	details := testGenerationDetails(t.TempDir(), "gen_missing_contract")
	details.SandboxContractVersion = ""

	err := validateGenerationDetails(StartRequest{
		SessionID:    details.SessionID,
		GenerationID: details.GenerationID,
		DriverID:     "claude_code",
		Generation:   details,
	})
	if err == nil || !strings.Contains(err.Error(), "sandbox contract version is required") {
		t.Fatalf("expected missing sandbox contract error, got %v", err)
	}

	details.SandboxContractVersion = "sandbox-legacy-v0"
	err = validateGenerationDetails(StartRequest{
		SessionID:    details.SessionID,
		GenerationID: details.GenerationID,
		DriverID:     "claude_code",
		Generation:   details,
	})
	if err == nil || !strings.Contains(err.Error(), `unsupported sandbox contract version "sandbox-legacy-v0"`) {
		t.Fatalf("expected unsupported sandbox contract error, got %v", err)
	}
}

func TestValidateGenerationDetailsRequiresExplicitRunscPlatform(t *testing.T) {
	details := testGenerationDetails(t.TempDir(), "gen_missing_platform")
	details.RunscPlatform = ""

	err := validateGenerationDetails(StartRequest{
		SessionID:    details.SessionID,
		GenerationID: details.GenerationID,
		DriverID:     "claude_code",
		Generation:   details,
	})
	if err == nil || !strings.Contains(err.Error(), "runsc platform is required") {
		t.Fatalf("expected missing runsc platform error, got %v", err)
	}
}

func TestCleanupExitedContainerDoesNotRemoveReplacement(t *testing.T) {
	rt := New(Config{})
	oldContainer := &Container{SessionID: "sess_1", RunscContainerID: "harness-gen-gen_old"}
	newContainer := &Container{SessionID: "sess_1", RunscContainerID: "harness-gen-gen_new"}

	rt.containers["sess_1"] = newContainer
	rt.cleanupExitedContainer(oldContainer)

	if got := rt.containers["sess_1"]; got != newContainer {
		t.Fatalf("replacement container was removed: got %+v", got)
	}
}

func TestRuntimeStartDoesNotReuseContainerForDifferentGeneration(t *testing.T) {
	rt := New(Config{})
	stdin := &recordingWriteCloser{}
	canceled := make(chan struct{})
	rt.containers["sess_1"] = &Container{
		SessionID:        "sess_1",
		GenerationID:     "gen_old",
		RunscContainerID: "harness-gen-gen_old",
		DriverID:         "claude_code",
		Stdin:            stdin,
		OutputHub:        NewOutputHub(),
		Cancel:           func() { close(canceled) },
	}

	res := rt.Start(context.Background(), StartRequest{
		SessionID:    "sess_1",
		GenerationID: "gen_new",
		DriverID:     "claude_code",
		Generation: store.RuntimeGenerationDetails{
			SessionID:              "sess_1",
			GenerationID:           "gen_new",
			SandboxContractVersion: store.SandboxContractVersion,
			RunscPlatform:          "systrap",
		},
	}, nil)
	if res.Err == nil || !strings.Contains(res.Err.Error(), "generation resource paths are required") {
		t.Fatalf("expected cold-start validation error after stale container eviction, got %v", res.Err)
	}
	select {
	case <-canceled:
	default:
		t.Fatal("stale generation container was not canceled")
	}
	if _, exists := rt.containers["sess_1"]; exists {
		t.Fatalf("stale generation container remains in runtime map")
	}
	stdin.mu.Lock()
	written := stdin.buf.String()
	stdin.mu.Unlock()
	if written != "" {
		t.Fatalf("message was written to stale generation stdin: %q", written)
	}
}

func TestEvictContainerByRunscIDCancelsAndRemovesMatchingContainer(t *testing.T) {
	rt := New(Config{})
	canceled := make(chan struct{})
	rt.containers["sess_1"] = &Container{
		SessionID:        "sess_1",
		RunscContainerID: "harness-gen-gen_1",
		Cancel:           func() { close(canceled) },
	}
	rt.containers["sess_2"] = &Container{SessionID: "sess_2", RunscContainerID: "harness-gen-gen_2"}

	rt.evictContainerByRunscID("harness-gen-gen_1")

	select {
	case <-canceled:
	default:
		t.Fatal("matching restore container was not canceled")
	}
	if _, exists := rt.containers["sess_1"]; exists {
		t.Fatal("matching restore container remains in runtime map")
	}
	if _, exists := rt.containers["sess_2"]; !exists {
		t.Fatal("non-matching restore container was removed")
	}
}

func TestRuntimeStartReusesExistingGenerationWithoutStdinTurn(t *testing.T) {
	rt := New(Config{
		RunscNetwork: "sandbox",
	})
	hub := NewOutputHub()
	stdin := &recordingWriteCloser{}
	container := &Container{
		SessionID:        "sess_1",
		GenerationID:     "gen_a",
		RunscContainerID: "harness-gen-gen_a",
		DriverID:         "claude_code",
		Stdin:            stdin,
		OutputHub:        hub,
	}
	rt.containers["sess_1"] = container

	outputs := 0
	res := rt.Start(context.Background(), StartRequest{
		SessionID:    "sess_1",
		GenerationID: "gen_a",
		DriverID:     "claude_code",
	}, func(Output) { outputs++ })
	if res.Err != nil {
		t.Fatalf("existing generation start failed: %v", res.Err)
	}
	if outputs != 0 {
		t.Fatalf("existing generation start should not forward process output, got %d callbacks", outputs)
	}
	stdin.mu.Lock()
	written := stdin.buf.String()
	stdin.mu.Unlock()
	if written != "" {
		t.Fatalf("existing generation start wrote direct stdin turn: %q", written)
	}
	if got := rt.containers["sess_1"]; got != container {
		t.Fatalf("existing generation container was replaced: got %+v", got)
	}
}

func TestRunscHelpersRequireExplicitBinary(t *testing.T) {
	rt := New(Config{CommandRunner: &recordingCommandRunner{}})
	ctx := context.Background()
	for name, run := range map[string]func() error{
		"running evidence": func() error {
			_, err := rt.runscContainerRunningEvidence(ctx, "", "harness-gen-missing")
			return err
		},
		"absence evidence": func() error {
			_, err := rt.runscContainerAbsenceEvidence(ctx, " ", "harness-gen-missing")
			return err
		},
		"delete": func() error {
			_, err := rt.deleteRunscContainerDetailed(ctx, "", "harness-gen-missing")
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := run(); err == nil || !strings.Contains(err.Error(), "runsc binary path is required") {
				t.Fatalf("expected explicit runsc binary error, got %v", err)
			}
		})
	}
}

func TestDestroyTreatsMissingRunscContainerAsAbsent(t *testing.T) {
	runner := &recordingCommandRunner{
		sequence: map[string][]commandResult{
			"runsc -root /runsc delete -force harness-gen-missing": {
				{out: []byte("container harness-gen-missing not found"), err: errors.New("exit status 1")},
			},
		},
	}
	rt := New(Config{RunscRoot: "/runsc", CommandRunner: runner})

	if err := rt.Destroy(context.Background(), "harness-gen-missing"); err != nil {
		t.Fatalf("destroy missing runsc container: %v", err)
	}
	want := []string{
		"runsc -root /runsc kill harness-gen-missing KILL",
		"runsc -root /runsc delete -force harness-gen-missing",
	}
	if got := runner.Commands(); !slices.Equal(got, want) {
		t.Fatalf("commands=%v want %v", got, want)
	}
}

type recordingWriteCloser struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *recordingWriteCloser) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *recordingWriteCloser) Close() error {
	return nil
}

var _ io.WriteCloser = (*recordingWriteCloser)(nil)

type recordingCommandRunner struct {
	mu       sync.Mutex
	outputs  map[string][]byte
	fail     map[string]error
	sequence map[string][]commandResult
	calls    []string
}

type commandResult struct {
	out []byte
	err error
}

func (r *recordingCommandRunner) CombinedOutput(_ context.Context, name string, args ...string) ([]byte, error) {
	key := strings.Join(append([]string{name}, args...), " ")
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, key)
	if len(r.sequence[key]) > 0 {
		result := r.sequence[key][0]
		r.sequence[key] = r.sequence[key][1:]
		return result.out, result.err
	}
	if err := r.fail[key]; err != nil {
		return nil, errors.New(err.Error())
	}
	if out, ok := r.outputs[key]; ok {
		return out, nil
	}
	return nil, nil
}

func (r *recordingCommandRunner) Commands() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

var _ CommandRunner = (*recordingCommandRunner)(nil)

func installFakeRunsc(t *testing.T, dir, label string) (string, string) {
	t.Helper()
	binDir := filepath.Join(dir, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("create fake runsc bin dir: %v", err)
	}
	path := filepath.Join(binDir, "runsc")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n# "+label+"\n"), 0o755); err != nil {
		t.Fatalf("write fake runsc: %v", err)
	}
	digest, err := fileSHA256(path)
	if err != nil {
		t.Fatalf("digest fake runsc: %v", err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return path, "sha256:" + digest
}

func TestWriteInterruptShellJSONFraming(t *testing.T) {
	var buf bytes.Buffer
	if err := writeInterrupt(&buf, "sh"); err != nil {
		t.Fatalf("writeInterrupt: %v", err)
	}
	var frame struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &frame); err != nil {
		t.Fatalf("invalid interrupt frame %q: %v", buf.String(), err)
	}
	if frame.Type != "interrupt" {
		t.Fatalf("unexpected interrupt frame: %+v", frame)
	}
}

func TestPrepareGenerationWritesPerGenerationSpecManifestAndIsolatedRuntime(t *testing.T) {
	t.Setenv("HARNESS_CLAUDE_BASE_URL", "http://bad.invalid")
	t.Setenv("HARNESS_ANTHROPIC_BASE_URL", "http://bad.invalid")
	t.Setenv("ANTHROPIC_BASE_URL", "http://bad.invalid")
	t.Setenv("HARNESS_CLAUDE_API_KEY", "bad")
	t.Setenv("HARNESS_ANTHROPIC_API_KEY", "bad")

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
	details := testGenerationDetails(dir, "gen_a")

	workspacePath, agentHomePath := dataVolumePathsForTest(dir, "sess_1", "claude_code")
	artifacts, err := rt.PrepareGeneration(context.Background(), withDataVolumePathsForTest(dir, StartRequest{
		SessionID:    "sess_1",
		GenerationID: details.GenerationID,
		DriverID:     "claude_code",
		Generation:   details,
	}))
	if err != nil {
		t.Fatalf("prepare generation: %v", err)
	}
	if artifacts.BundleDir != details.BundleDirPath || artifacts.SpecPath != details.SpecPath || artifacts.ManifestPath != details.ControlManifestPath {
		t.Fatalf("unexpected artifacts: %+v", artifacts)
	}

	var manifestFile controlManifestFile
	data, err := os.ReadFile(details.ControlManifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if err := json.Unmarshal(data, &manifestFile); err != nil {
		t.Fatalf("control manifest is not valid JSON: %v\n%s", err, data)
	}
	payloadBytes, err := canonicalJSON(manifestFile.Payload)
	if err != nil {
		t.Fatalf("canonical manifest: %v", err)
	}
	if got := digestHex(payloadBytes); got != manifestFile.Digest || got != artifacts.ManifestDigest {
		t.Fatalf("manifest digest mismatch got=%s file=%s artifacts=%s", got, manifestFile.Digest, artifacts.ManifestDigest)
	}
	manifest := manifestFile.Payload
	if manifest.SessionID != "sess_1" {
		t.Fatalf("unexpected session id: %+v", manifest)
	}
	if manifest.GenerationID != details.GenerationID || manifest.NetworkProfileID != details.NetworkProfileID || manifest.AgentRuntimeProfileID != details.AgentRuntimeProfileID {
		t.Fatalf("manifest missing identity: %+v", manifest)
	}
	if manifest.ManifestVersion != 1 {
		t.Fatalf("manifest_version=%d want 1", manifest.ManifestVersion)
	}
	if manifest.SandboxContractVersion != store.SandboxContractVersion {
		t.Fatalf("sandbox_contract_version=%q want %q", manifest.SandboxContractVersion, store.SandboxContractVersion)
	}
	if manifest.WorkspacePath != "/workspace" || manifest.AgentHomePath != "/agent-home" {
		t.Fatalf("unexpected workspace/home paths: %+v", manifest)
	}
	if manifest.SandboxModelProxyBaseURL != "http://harness-model-proxy.internal:8082" {
		t.Fatalf("unexpected sandbox base URL: %+v", manifest)
	}
	if strings.Contains(string(data), `"anthropic_api_key":`) || strings.Contains(string(data), `"anthropic_auth_token":`) {
		t.Fatalf("manifest must not contain plaintext credential fields: %s", data)
	}
	if manifest.DriverRuntime["claude_code_disable_nonessential_traffic"] != true {
		t.Fatalf("expected nonessential traffic to be disabled: %+v", manifest.DriverRuntime)
	}
	if manifest.Model != "sonnet" || manifest.OutputFormat != "stream-json" {
		t.Fatalf("unexpected Claude defaults: %+v", manifest)
	}
	assertControlManifestOmitsHostOnlyFields(t, data, controlManifestForbiddenHostValues(details)...)

	var spec runtimeSpec
	specData, err := os.ReadFile(details.SpecPath)
	if err != nil {
		t.Fatalf("read spec: %v", err)
	}
	if err := json.Unmarshal(specData, &spec); err != nil {
		t.Fatalf("runtime spec is not valid JSON: %v\n%s", err, specData)
	}
	if strings.Contains(string(specData), "removed-template") {
		t.Fatalf("runtime spec hot path must not reference removed-template: %s", specData)
	}
	if strings.Contains(string(specData), "/harness-secrets") ||
		strings.Contains(string(specData), "anthropic_api_key") ||
		strings.Contains(string(specData), "anthropic_auth_token") {
		t.Fatalf("runtime spec must not contain removed secret references: %s", specData)
	}
	if !spec.Root.Readonly {
		t.Fatalf("isolated rootfs must be read-only: %+v", spec.Root)
	}
	if spec.Process.User.UID != testSandboxUID() || spec.Process.User.GID != testSandboxGID() {
		t.Fatalf("isolated user=%+v want %d:%d", spec.Process.User, testSandboxUID(), testSandboxGID())
	}
	assertRuntimeSpecCapabilityPolicy(t, spec)
	if strings.Contains(mustJSONForTest(t, spec.Process.Capabilities), "CAP_") {
		t.Fatalf("isolated capabilities must be empty: %+v", spec.Process.Capabilities)
	}
	for _, destination := range []string{"/sessions", "/agent-homes", "/harness-secrets"} {
		if mountByDestination(spec.Mounts, destination) != nil {
			t.Fatalf("isolated spec must not mount %s: %+v", destination, spec.Mounts)
		}
	}
	for _, mount := range spec.Mounts {
		if slices.Contains(mount.Options, "rbind") {
			t.Fatalf("isolated mount %s uses recursive bind: %+v", mount.Destination, mount)
		}
	}
	if mountSource(spec.Mounts, "/workspace") != workspacePath {
		t.Fatalf("workspace mount = %q", mountSource(spec.Mounts, "/workspace"))
	}
	if mountSource(spec.Mounts, "/agent-home") != agentHomePath {
		t.Fatalf("agent-home mount = %q", mountSource(spec.Mounts, "/agent-home"))
	}
	if mountSource(spec.Mounts, "/harness-control") != details.ControlDirPath {
		t.Fatalf("control mount = %q, want %q", mountSource(spec.Mounts, "/harness-control"), details.ControlDirPath)
	}
	if control := mountByDestination(spec.Mounts, "/harness-control"); control == nil || strings.Join(control.Options, ",") != "bind,ro,nosuid,nodev,noexec" {
		t.Fatalf("unexpected control mount: %+v", control)
	}
	bridgeMount := mountByDestination(spec.Mounts, "/harness-control/bridge")
	if bridgeMount == nil {
		t.Fatalf("runtime spec missing bridge mount: %+v", spec.Mounts)
	}
	if bridgeMount.Source != details.BridgeDirPath || strings.Join(bridgeMount.Options, ",") != "bind,rw,nosuid,nodev,noexec" {
		t.Fatalf("unexpected bridge mount: %+v", bridgeMount)
	}
	if bridgeMount.Annotations["dev.gvisor.spec.mount./harness-control/bridge.share"] != "exclusive" ||
		bridgeMount.Annotations["dev.gvisor.spec.mount./harness-control/bridge.type"] != "bind" {
		t.Fatalf("bridge mount missing exclusive annotation: %+v", bridgeMount.Annotations)
	}
	assertReadOnlyBridgeSubmount(t, spec.Mounts, details, bridge.InboxDir)
	assertReadOnlyBridgeSubmount(t, spec.Mounts, details, bridge.HostTmpDir)
	assertNoBridgeSubmount(t, spec.Mounts, bridge.HostHeartbeatDir)
	env := specEnv(spec.Process.Env)
	if env["HARNESS_BRIDGE_DIR"] != "/harness-control/bridge" ||
		env["HARNESS_BRIDGE_MODE"] != "claim-loop" ||
		env["HARNESS_EXPECTED_MANIFEST_VERSION"] != "1" ||
		env["HARNESS_BRIDGE_HEARTBEAT_INTERVAL"] != "20" ||
		env["HARNESS_BRIDGE_POLL_INTERVAL"] != "0.005" ||
		env["HARNESS_BRIDGE_IDLE_INTERVAL"] != "0.005" ||
		env["HARNESS_PROBE_HEALTHZ_STATUSES"] != "200" {
		t.Fatalf("runtime spec missing bridge/probe env: %+v", env)
	}
	if _, ok := env["HARNESS_PROBE_MESSAGE_STATUSES"]; ok {
		t.Fatalf("runtime spec must not configure pre-turn model endpoint probes: %+v", env)
	}
	if env["HARNESS_DRIVER_ID"] != "claude_code" ||
		env["HARNESS_AGENT_UID"] != fmt.Sprint(testSandboxUID()) ||
		env["HARNESS_AGENT_GID"] != fmt.Sprint(testSandboxGID()) ||
		env["SESSION_WORKSPACE"] != "/workspace" ||
		env["HARNESS_AGENT_HOME"] != "/agent-home" {
		t.Fatalf("runtime spec missing isolated agent env: %+v", env)
	}
	if env["HARNESS_BRIDGE_PROTOCOL_VERSION"] != fmt.Sprint(manifest.BridgeProtocolVersion) ||
		env["HARNESS_TURN_INPUT_SCHEMA"] != manifest.TurnInputSchema {
		t.Fatalf("runtime spec bridge env must match control manifest: env=%+v manifest=%+v", env, manifest)
	}
	for _, key := range []string{"HARNESS_EXPECTED_API_KEY_SECRET_ID", "HARNESS_EXPECTED_AUTH_TOKEN_SECRET_ID", "HARNESS_EXPECTED_SECRET_VERSION", "HARNESS_SECRET_READERS_GID"} {
		if _, ok := env[key]; ok {
			t.Fatalf("runtime spec must not include removed secret env %s: %+v", key, env)
		}
	}
	for _, name := range []string{"inbox", "outbox", "heartbeat", "tmp"} {
		if info, err := os.Stat(filepath.Join(details.BridgeDirPath, name)); err != nil || !info.IsDir() {
			t.Fatalf("bridge dir %s not initialized: info=%v err=%v", name, info, err)
		}
	}
	hostUID := 0
	if os.Geteuid() != 0 {
		hostUID = os.Geteuid()
	}
	for _, check := range []struct {
		name string
		uid  int
		gid  int
		mode os.FileMode
	}{
		{name: ".", uid: hostUID, gid: testSandboxGID(), mode: 0o750},
		{name: "inbox", uid: hostUID, gid: testSandboxGID(), mode: 0o550},
		{name: "host-heartbeat", uid: hostUID, gid: testSandboxGID(), mode: 0o550},
		{name: "host-tmp", uid: hostUID, gid: testSandboxGID(), mode: 0o550},
		{name: "outbox", uid: testSandboxUID(), gid: testSandboxGID(), mode: 0o770},
		{name: "tmp", uid: testSandboxUID(), gid: testSandboxGID(), mode: 0o770},
		{name: "heartbeat", uid: testSandboxUID(), gid: testSandboxGID(), mode: 0o770},
	} {
		assertBridgeDirOwnership(t, filepath.Join(details.BridgeDirPath, check.name), check.uid, check.gid, check.mode)
	}
	for _, check := range []struct {
		path string
		uid  int
		gid  int
		mode os.FileMode
	}{
		{path: bridge.HostControlRoot(details.BridgeDirPath), uid: hostUID, gid: testSandboxGID(), mode: 0o750},
		{path: bridge.HostOwnedPath(details.BridgeDirPath, bridge.InboxDir), uid: hostUID, gid: testSandboxGID(), mode: 0o750},
		{path: bridge.HostOwnedPath(details.BridgeDirPath, bridge.HostHeartbeatDir), uid: hostUID, gid: testSandboxGID(), mode: 0o750},
		{path: bridge.HostOwnedPath(details.BridgeDirPath, bridge.HostTmpDir), uid: hostUID, gid: testSandboxGID(), mode: 0o750},
	} {
		assertBridgeDirOwnership(t, check.path, check.uid, check.gid, check.mode)
	}
}

func withDataVolumePathsForTest(dir string, req StartRequest) StartRequest {
	sessionID := req.SessionID
	if sessionID == "" {
		sessionID = req.Generation.SessionID
	}
	agent := driverID(req)
	workspacePath, agentHomePath := dataVolumePathsForTest(dir, sessionID, agent)
	req.WorkspaceHostPath = workspacePath
	req.AgentHomeHostPath = agentHomePath
	return req
}

func dataVolumePathsForTest(dir, sessionID, agent string) (string, string) {
	return filepath.Join(dir, "volumes", "workspaces", sessionID),
		filepath.Join(dir, "volumes", "driver-homes", sessionID, agent)
}

func testGenerationDetails(dir, generationID string) store.RuntimeGenerationDetails {
	return store.RuntimeGenerationDetails{
		SessionID:                  "sess_1",
		GenerationID:               generationID,
		NetworkProfileID:           "net_" + generationID,
		AgentRuntimeProfileID:      "arp_" + generationID,
		RunscPlatform:              "systrap",
		RunscNetwork:               "host",
		RunscOverlay2:              "none",
		SandboxContractVersion:     store.SandboxContractVersion,
		ControlDirPath:             filepath.Join(dir, "run", "control", "gen-"+generationID),
		ControlManifestPath:        filepath.Join(dir, "run", "control", "gen-"+generationID, "session.json"),
		BundleDirPath:              filepath.Join(dir, "run", "runtime", "gen-"+generationID),
		SpecPath:                   filepath.Join(dir, "run", "runtime", "gen-"+generationID, "config.json"),
		CheckpointPath:             filepath.Join(dir, "run", "gen-"+generationID, "checkpoint"),
		RunscContainerID:           "harness-gen-" + generationID,
		SecretsDirPath:             "",
		BridgeDirPath:              filepath.Join(dir, "run", "bridge", "gen-"+generationID),
		NetworkHostsPath:           "",
		LogDirPath:                 filepath.Join(dir, "run", "logs", "gen-"+generationID),
		HostGatewayIP:              "10.200.1.1",
		SandboxIPCIDR:              "10.200.1.2/30",
		SandboxBaseURL:             "http://10.200.1.1:8082",
		NetnsName:                  "harness-gen-" + generationID,
		NftTableName:               mustHostEgressTableName(generationID),
		EgressPolicyDigest:         "egress_digest",
		DriverID:                   "claude_code",
		Model:                      "sonnet",
		OutputFormat:               "stream-json",
		DisableNonessentialTraffic: true,
		SandboxUID:                 testSandboxUID(),
		SandboxGID:                 testSandboxGID(),
		RequiresSecretDrop:         false,
		ManifestAnthropicBaseURL:   "http://harness-model-proxy.internal:8082",
	}
}

func restorePreparedArtifacts(details store.RuntimeGenerationDetails, runscVersion, runscPath, runscDigest string) GenerationArtifacts {
	return GenerationArtifacts{
		BundleDir:               details.BundleDirPath,
		SpecPath:                details.SpecPath,
		ManifestPath:            details.ControlManifestPath,
		ManifestDigest:          "control_manifest_digest",
		ProjectedManifestDigest: "control_manifest_digest",
		BundleDigest:            "bundle_digest",
		RuntimeConfigDigest:     "runtime_config_digest",
		SpecDigest:              "spec_digest",
		RunscVersion:            runscVersion,
		RunscBinaryPath:         runscPath,
		RunscBinaryDigest:       runscDigest,
	}
}

func generationFilesystemPaths(details store.RuntimeGenerationDetails) []string {
	paths := []string{
		details.CheckpointPath,
		details.ControlDirPath,
		details.BundleDirPath,
		details.BridgeDirPath,
		details.LogDirPath,
	}
	if strings.TrimSpace(details.NetworkHostsPath) != "" {
		paths = append(paths, filepath.Dir(details.NetworkHostsPath))
	}
	return paths
}

func createGenerationFilesystem(t *testing.T, details store.RuntimeGenerationDetails) {
	t.Helper()
	for _, path := range generationFilesystemPaths(details) {
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatalf("create generation filesystem path %s: %v", path, err)
		}
		if err := os.WriteFile(filepath.Join(path, ".keep"), []byte("keep"), 0o644); err != nil {
			t.Fatalf("write generation filesystem marker %s: %v", path, err)
		}
	}
}

func assertGenerationFilesystemMissing(t *testing.T, paths []string) {
	t.Helper()
	for _, path := range paths {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatalf("expected cleanup path %s to be missing, stat err=%v", path, err)
		}
	}
}

func assertBridgeDirOwnership(t *testing.T, path string, uid, gid int, mode os.FileMode) {
	t.Helper()
	assertDirOwnership(t, path, uid, gid, mode)
}

func assertDirOwnership(t *testing.T, path string, uid, gid int, mode os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat dir %s: %v", path, err)
	}
	if !info.IsDir() || info.Mode().Perm() != mode {
		t.Fatalf("dir %s mode=%#o is_dir=%v want mode=%#o", path, info.Mode().Perm(), info.IsDir(), mode)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("dir %s stat type = %T", path, info.Sys())
	}
	if int(stat.Uid) != uid || int(stat.Gid) != gid {
		t.Fatalf("dir %s owner=%d:%d want %d:%d", path, stat.Uid, stat.Gid, uid, gid)
	}
}

func assertReadOnlyBridgeSubmount(t *testing.T, mounts []specMount, details store.RuntimeGenerationDetails, name string) {
	t.Helper()
	destination := filepath.Join(bridge.BridgeMountDestination, name)
	mount := mountByDestination(mounts, destination)
	if mount == nil {
		t.Fatalf("missing bridge submount %s: %+v", destination, mounts)
	}
	if mount.Source != bridge.HostOwnedPath(details.BridgeDirPath, name) ||
		strings.Join(mount.Options, ",") != "bind,ro,nosuid,nodev,noexec" {
		t.Fatalf("unexpected bridge submount %s: %+v", destination, mount)
	}
}

func assertNoBridgeSubmount(t *testing.T, mounts []specMount, name string) {
	t.Helper()
	destination := filepath.Join(bridge.BridgeMountDestination, name)
	if mountByDestination(mounts, destination) != nil {
		t.Fatalf("bridge submount %s should not be mounted: %+v", destination, mounts)
	}
}

func assertGenerationFilesystemPresent(t *testing.T, paths []string) {
	t.Helper()
	for _, path := range paths {
		if _, err := os.Lstat(path); err != nil {
			t.Fatalf("expected cleanup path %s to remain, stat err=%v", path, err)
		}
	}
}

func testControlManifest() controlManifest {
	return controlManifest{
		SessionID:                "sess_1",
		GenerationID:             "gen_a",
		SandboxContractVersion:   store.SandboxContractVersion,
		CreatedAt:                "2026-01-01T00:00:00Z",
		AttemptID:                "attempt-1",
		NetworkProfileID:         "net_a",
		AgentRuntimeProfileID:    "arp_a",
		DriverID:                 "claude_code",
		BridgeProtocolVersion:    2,
		TurnInputSchema:          "RunTurn",
		RunscPlatform:            "systrap",
		RunscVersion:             "runsc test",
		SandboxModelProxyBaseURL: "http://harness-model-proxy.internal:8082",
		Model:                    "sonnet",
		OutputFormat:             "stream-json",
		WorkspacePath:            "/workspace",
		AgentHomePath:            "/agent-home",
		BundleDigest:             "bundle_digest",
		RuntimeConfigDigest:      "runtime_config_digest",
		SpecDigest:               "spec_digest",
		EgressPolicyDigest:       "egress_digest",
		ManifestVersion:          1,
		DriverRuntime: map[string]any{
			"claude_code_disable_nonessential_traffic": true,
		},
	}
}

func writeCheckpointFiles(t *testing.T, checkpointPath string) string {
	t.Helper()
	writeCheckpointFilesWithoutManifest(t, checkpointPath)
	if err := writeCheckpointImageManifest(checkpointPath); err != nil {
		t.Fatalf("write checkpoint image manifest: %v", err)
	}
	digest, err := CheckpointImageManifestDigest(checkpointPath)
	if err != nil {
		t.Fatalf("digest checkpoint image manifest: %v", err)
	}
	return digest
}

func writeCheckpointFilesWithoutManifest(t *testing.T, checkpointPath string) {
	t.Helper()
	if err := os.MkdirAll(checkpointPath, 0o755); err != nil {
		t.Fatalf("create checkpoint path: %v", err)
	}
	for _, name := range requiredCheckpointImageFiles {
		if err := os.WriteFile(filepath.Join(checkpointPath, name), []byte("x"), 0o644); err != nil {
			t.Fatalf("write checkpoint file %s: %v", name, err)
		}
	}
}

func mountSource(mounts []specMount, destination string) string {
	mount := mountByDestination(mounts, destination)
	if mount == nil {
		return ""
	}
	return mount.Source
}

func mountByDestination(mounts []specMount, destination string) *specMount {
	for _, mount := range mounts {
		if mount.Destination == destination {
			return &mount
		}
	}
	return nil
}

func specEnv(values []string) map[string]string {
	env := map[string]string{}
	for _, value := range values {
		key, raw, ok := strings.Cut(value, "=")
		if ok {
			env[key] = raw
		}
	}
	return env
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return data
}

func mustJSONForTest(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	return string(data)
}

func assertRuntimeSpecCapabilityPolicy(t *testing.T, spec runtimeSpec) {
	t.Helper()
	if !spec.Process.NoNewPrivileges {
		t.Fatalf("runtime spec must set noNewPrivileges: %+v", spec.Process)
	}
	capabilities, ok := spec.Process.Capabilities.(map[string]any)
	if !ok {
		t.Fatalf("runtime spec capabilities must be an object: %+v", spec.Process.Capabilities)
	}
	for _, name := range []string{"bounding", "effective", "inheritable", "permitted", "ambient"} {
		values, ok := capabilities[name].([]any)
		if !ok {
			t.Fatalf("runtime spec capability set %s must be an array: %+v", name, capabilities)
		}
		if len(values) != 0 {
			t.Fatalf("runtime spec capability set %s must be empty: %+v", name, capabilities)
		}
	}
}

func assertControlManifestOmitsHostOnlyFields(t *testing.T, data []byte, forbiddenValues ...string) {
	t.Helper()
	var file struct {
		Payload map[string]json.RawMessage `json:"payload"`
		Digest  string                     `json:"digest"`
	}
	if err := json.Unmarshal(data, &file); err != nil {
		t.Fatalf("control manifest json: %v", err)
	}
	strictFields, regenerableFields := controlManifestProjectionFields()
	for field := range file.Payload {
		if _, ok := strictFields[field]; ok {
			continue
		}
		if _, ok := regenerableFields[field]; ok {
			continue
		}
		t.Fatalf("control manifest contains unclassified field %s: %s", field, data)
	}
	for _, forbidden := range []string{
		"host_hostname",
		"netns_name",
		"netns_path",
		"host_veth",
		"sandbox_veth",
		"host_gateway_ip",
		"nft_table_name",
		"sandbox_source_ip",
		"bridge_dir_path",
		"proxy_bind_url",
		"runsc_binary_path",
		"checkpoint_path",
		"log_dir_path",
		"rootfs_path",
	} {
		if _, ok := file.Payload[forbidden]; ok {
			t.Fatalf("control manifest must omit host-only field %s: %s", forbidden, data)
		}
		if strings.Contains(string(data), `"`+forbidden+`"`) {
			t.Fatalf("control manifest must omit host-only field %s: %s", forbidden, data)
		}
	}
	for _, forbidden := range forbiddenValues {
		forbidden = strings.TrimSpace(forbidden)
		if forbidden == "" {
			continue
		}
		if strings.Contains(string(data), forbidden) {
			t.Fatalf("control manifest must omit host-only value %q: %s", forbidden, data)
		}
	}
}

func controlManifestForbiddenHostValues(details store.RuntimeGenerationDetails) []string {
	values := []string{
		details.ControlDirPath,
		details.BundleDirPath,
		details.SpecPath,
		details.CheckpointPath,
		details.BridgeDirPath,
		details.NetworkHostsPath,
		details.LogDirPath,
		details.SecretsDirPath,
		details.HostGatewayIP,
		details.SandboxIPCIDR,
		details.HostSideCIDR,
		details.NetnsName,
		details.NetnsPath,
		details.HostVeth,
		details.SandboxVeth,
		details.NftTableName,
		details.SandboxBaseURL,
		details.HostProxyBindURL,
	}
	if sandboxIP, _, ok := strings.Cut(details.SandboxIPCIDR, "/"); ok {
		values = append(values, sandboxIP)
	}
	if table := mustGenerationNftTableName(details); table != "" {
		values = append(values, table)
	}
	return values
}

func mustHostEgressTableName(generationID string) string {
	tableName, err := hostEgressTableName(generationID)
	if err != nil {
		panic(err)
	}
	return tableName
}

func mustGenerationNftTableName(details store.RuntimeGenerationDetails) string {
	tableName, err := generationNftTableName(details)
	if err != nil {
		panic(err)
	}
	return tableName
}

func testSandboxUID() int {
	uid := os.Getuid()
	if uid > 0 {
		return uid
	}
	return 65534
}

func testSandboxGID() int {
	gid := os.Getgid()
	if gid > 0 {
		return gid
	}
	return 65534
}
