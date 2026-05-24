package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/store"
)

func TestRuntimeStartRejectsUnsupportedAgent(t *testing.T) {
	rt := New(Config{DefaultAgent: "claude"})
	res := rt.Start(context.Background(), StartRequest{
		SessionID: "sess_1",
		Agent:     "opencode",
	}, nil)
	if res.Err == nil {
		t.Fatal("expected unsupported agent error")
	}
	if !strings.Contains(res.Err.Error(), "unsupported agent") {
		t.Fatalf("expected unsupported agent error, got %v", res.Err)
	}
}

func TestRuntimeStartRequiresGenerationDetailsForColdPath(t *testing.T) {
	rt := New(Config{
		DefaultAgent:     "claude",
		SessionsRoot:     filepath.Join(t.TempDir(), "sessions"),
		AgentHomesRoot:   filepath.Join(t.TempDir(), "agent-homes"),
		CheckpointsRoot:  filepath.Join(t.TempDir(), "checkpoints"),
		BundleRoot:       filepath.Join(t.TempDir(), "bundle", "out"),
		RunscNetwork:     "host",
		SecretReadersGID: testSecretReadersGID(),
	})
	res := rt.Start(context.Background(), StartRequest{
		SessionID: "sess_1",
		Agent:     "claude",
		Done:      closedDone(),
	}, nil)
	if res.Err == nil {
		t.Fatal("expected missing generation details error")
	}
	if !strings.Contains(res.Err.Error(), "generation details are required") {
		t.Fatalf("expected generation details error, got %v", res.Err)
	}
}

func TestCleanupExitedContainerDoesNotRemoveReplacement(t *testing.T) {
	rt := New(Config{})
	oldContainer := &Container{SessionID: "sess_1", RestoreID: "phase3-sess_1"}
	newContainer := &Container{SessionID: "sess_1", RestoreID: "phase3-sess_1"}

	rt.containers["sess_1"] = newContainer
	rt.cleanupExitedContainer(oldContainer)

	if got := rt.containers["sess_1"]; got != newContainer {
		t.Fatalf("replacement container was removed: got %+v", got)
	}
}

func TestSendMessageDoesNotReconfigureLiveSandboxNetwork(t *testing.T) {
	rt := New(Config{
		DefaultAgent: "claude",
		RunscNetwork: "sandbox",
	})
	hub := NewOutputHub()
	stdin := &recordingWriteCloser{}
	container := &Container{
		SessionID: "sess_1",
		RestoreID: "phase3-sess_1",
		Agent:     "claude",
		Stdin:     stdin,
		OutputHub: hub,
	}

	done := make(chan struct{})
	go func() {
		for {
			stdin.mu.Lock()
			written := stdin.buf.Len() > 0
			stdin.mu.Unlock()
			if written {
				hub.Publish(OutputEvent{Stream: "stdout", Line: `{"type":"result","subtype":"success","result":"ok"}`})
				close(done)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	res := rt.sendMessage(context.Background(), container, "hello", done, nil)
	if res.Err != nil {
		t.Fatalf("sendMessage should not attempt live sandbox network reconfiguration: %v", res.Err)
	}
}

func TestEnsureSandboxNetworkUsesGenerationAllocationAndProbes(t *testing.T) {
	runner := &recordingCommandRunner{
		outputs: map[string][]byte{
			"runsc --version": []byte("runsc test"),
			"ip netns exec harness-gen-a nft list table inet harness_egress":                                                []byte("table exists"),
			"nft list table inet harness_gen_gen_a":                                                                         []byte("table exists"),
			"ip netns exec harness-gen-a curl -sS --max-time 2 -o /dev/null -w %{http_code} http://10.250.0.1:8082/healthz": []byte("200"),
			"ip netns exec harness-gen-a curl -sS --max-time 2 -o /dev/null -w %{http_code} -X POST -H content-type: application/json -H x-api-key: 123 --data {} http://10.250.0.1:8082/v1/messages": []byte("400"),
		},
	}
	rt := New(Config{
		RunscNetwork:  "sandbox",
		RunscOverlay2: "none",
		CommandRunner: runner,
		Claude: ClaudeConfig{
			APIKey: "123",
		},
	})
	details := testGenerationDetails(t.TempDir(), "gen_a")
	details.RunscNetwork = "sandbox"
	details.RunscOverlay2 = "none"
	details.NetnsName = "harness-gen-a"
	details.NetnsPath = "/var/run/netns/harness-gen-a"
	details.HostVeth = "hgenah"
	details.SandboxVeth = "hgenas"
	details.HostSideCIDR = "10.250.0.0/30"
	details.SandboxIPCIDR = "10.250.0.2/30"
	details.HostGatewayIP = "10.250.0.1"
	details.ProbeURL = "http://10.250.0.1:8082"
	details.AllowedEgressRules = `["tcp:10.250.0.1:8082","tcp:172.16.0.138:9030","udp:53"]`

	if err := rt.ensureSandboxNetwork(context.Background(), details); err != nil {
		t.Fatalf("ensure sandbox network: %v", err)
	}

	want := []string{
		"ip netns add harness-gen-a",
		"ip link delete hgenah",
		"ip netns exec harness-gen-a ip link delete hgenas",
		"ip link add hgenah type veth peer name hgenas",
		"ip link set hgenas netns harness-gen-a",
		"ip addr replace 10.250.0.1/30 dev hgenah",
		"ip link set hgenah up",
		"ip netns exec harness-gen-a ip addr replace 10.250.0.2/30 dev hgenas",
		"ip netns exec harness-gen-a ip link set lo up",
		"ip netns exec harness-gen-a ip link set hgenas up",
		"ip netns exec harness-gen-a ip route replace default via 10.250.0.1 dev hgenas",
		"ip netns exec harness-gen-a nft list table inet harness_egress",
		"ip netns exec harness-gen-a nft delete table inet harness_egress",
		"ip netns exec harness-gen-a nft add table inet harness_egress",
		"ip netns exec harness-gen-a nft add chain inet harness_egress output { type filter hook output priority 0 ; policy drop ; }",
		"ip netns exec harness-gen-a nft add rule inet harness_egress output oifname lo accept",
		"ip netns exec harness-gen-a nft add rule inet harness_egress output ip daddr 10.250.0.1 tcp dport 8082 accept",
		"ip netns exec harness-gen-a nft add rule inet harness_egress output ip daddr 172.16.0.138 tcp dport 9030 accept",
		"ip netns exec harness-gen-a nft add rule inet harness_egress output udp dport 53 accept",
		"sysctl -w net.ipv4.ip_forward=1",
		"nft list table inet harness_gen_gen_a",
		"nft delete table inet harness_gen_gen_a",
		"nft add table inet harness_gen_gen_a",
		"nft add chain inet harness_gen_gen_a forward { type filter hook forward priority 0 ; policy accept ; }",
		"nft add chain inet harness_gen_gen_a postrouting { type nat hook postrouting priority 100 ; policy accept ; }",
		"nft add rule inet harness_gen_gen_a forward iifname hgenah ip daddr 172.16.0.138 tcp dport 9030 accept",
		"nft add rule inet harness_gen_gen_a forward iifname hgenah udp dport 53 accept",
		"nft add rule inet harness_gen_gen_a forward oifname hgenah ct state established,related accept",
		"nft add rule inet harness_gen_gen_a forward iifname hgenah drop",
		"nft add rule inet harness_gen_gen_a postrouting ip saddr 10.250.0.0/30 masquerade",
		"ip netns exec harness-gen-a curl -sS --max-time 2 -o /dev/null -w %{http_code} http://10.250.0.1:8082/healthz",
		"ip netns exec harness-gen-a curl -sS --max-time 2 -o /dev/null -w %{http_code} -X POST -H content-type: application/json -H x-api-key: 123 --data {} http://10.250.0.1:8082/v1/messages",
	}
	if got := runner.Commands(); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("unexpected commands:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestProbeSandboxNetworkRetriesAndUsesConfiguredStatuses(t *testing.T) {
	healthz := "ip netns exec harness-gen-a curl -sS --max-time 2 -o /dev/null -w %{http_code} http://10.250.0.1:8082/healthz"
	postMessages := "ip netns exec harness-gen-a curl -sS --max-time 2 -o /dev/null -w %{http_code} -X POST -H content-type: application/json -H x-api-key: 123 --data {} http://10.250.0.1:8082/v1/messages"
	runner := &recordingCommandRunner{
		sequence: map[string][]commandResult{
			healthz: {
				{out: []byte("503")},
				{out: []byte("204")},
			},
			postMessages: {
				{out: []byte("422")},
			},
		},
	}
	rt := New(Config{
		CommandRunner:         runner,
		PreStartProbeAttempts: 2,
		PreStartProbeInterval: time.Nanosecond,
		ProbeHealthzStatuses:  []int{204},
		ProbeMessageStatuses:  []int{422},
		Claude: ClaudeConfig{
			APIKey: "123",
		},
	})
	details := testGenerationDetails(t.TempDir(), "gen_a")
	details.NetnsName = "harness-gen-a"
	details.ProbeURL = "http://10.250.0.1:8082"

	if err := rt.probeSandboxNetwork(context.Background(), details); err != nil {
		t.Fatalf("probe sandbox network: %v", err)
	}
	want := []string{healthz, healthz, postMessages}
	if got := runner.Commands(); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("unexpected commands:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestRenderRuntimeSpecUsesGenerationNetnsPath(t *testing.T) {
	dir := t.TempDir()
	rt := New(Config{
		SessionsRoot:   filepath.Join(dir, "sessions"),
		AgentHomesRoot: filepath.Join(dir, "agent-homes"),
		BundleRoot:     filepath.Join(dir, "bundle", "out"),
		RootFSPath:     filepath.Join(dir, "rootfs"),
		RunscNetwork:   "sandbox",
	})
	details := testGenerationDetails(dir, "gen_netns")
	details.NetnsPath = "/var/run/netns/harness-gen-netns"
	spec, _, err := rt.renderRuntimeSpec(StartRequest{
		SessionID:    "sess_1",
		GenerationID: details.GenerationID,
		Agent:        "claude",
		Generation:   details,
	})
	if err != nil {
		t.Fatalf("render runtime spec: %v", err)
	}
	if !strings.Contains(string(spec.Linux), details.NetnsPath) {
		t.Fatalf("spec linux must contain generation netns path %q: %s", details.NetnsPath, spec.Linux)
	}
	if strings.Contains(string(spec.Linux), "phase1-demo") {
		t.Fatalf("spec linux must not contain shared netns path: %s", spec.Linux)
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

func TestWriteUserTurnClaudeJSONLFraming(t *testing.T) {
	var buf bytes.Buffer
	if err := writeUserTurn(&buf, "claude", "hello world"); err != nil {
		t.Fatalf("writeUserTurn: %v", err)
	}

	out := buf.String()
	if !strings.HasSuffix(out, "\n") {
		t.Fatalf("expected trailing newline for JSONL framing, got %q", out)
	}
	if strings.Count(out, "\n") != 1 {
		t.Fatalf("expected exactly one JSONL frame, got %q", out)
	}

	var frame struct {
		Type    string `json:"type"`
		Message struct {
			Role    string `json:"role"`
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"message"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSuffix(out, "\n")), &frame); err != nil {
		t.Fatalf("invalid JSON frame %q: %v", out, err)
	}
	if frame.Type != "user" || frame.Message.Role != "user" {
		t.Fatalf("unexpected frame: %+v", frame)
	}
	if len(frame.Message.Content) != 1 {
		t.Fatalf("expected one content block, got %+v", frame.Message.Content)
	}
	if frame.Message.Content[0].Type != "text" || frame.Message.Content[0].Text != "hello world" {
		t.Fatalf("unexpected content block: %+v", frame.Message.Content[0])
	}
}

func TestWriteUserTurnClaudeEscapesNewlines(t *testing.T) {
	// Multi-line user input must stay on a single JSONL line.
	var buf bytes.Buffer
	if err := writeUserTurn(&buf, "claude", "line1\nline2"); err != nil {
		t.Fatalf("writeUserTurn: %v", err)
	}
	if strings.Count(buf.String(), "\n") != 1 {
		t.Fatalf("multi-line input must produce one JSONL frame, got %q", buf.String())
	}
	var frame struct {
		Message struct {
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
		} `json:"message"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSuffix(buf.String(), "\n")), &frame); err != nil {
		t.Fatalf("invalid JSON frame %q: %v", buf.String(), err)
	}
	if len(frame.Message.Content) != 1 || frame.Message.Content[0].Text != "line1\nline2" {
		t.Fatalf("unexpected multi-line content: %+v", frame.Message.Content)
	}
}

func TestWriteUserTurnShellJSONFraming(t *testing.T) {
	var buf bytes.Buffer
	if err := writeUserTurn(&buf, "sh", "ls -la"); err != nil {
		t.Fatalf("writeUserTurn: %v", err)
	}
	out := buf.String()
	if !strings.HasSuffix(out, "\n") {
		t.Fatalf("expected trailing newline for shell JSON framing, got %q", out)
	}
	var frame struct {
		Type    string `json:"type"`
		Content string `json:"content"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSuffix(out, "\n")), &frame); err != nil {
		t.Fatalf("invalid JSON frame %q: %v", out, err)
	}
	if frame.Type != "turn" || frame.Content != "ls -la" {
		t.Fatalf("unexpected shell frame: %+v", frame)
	}
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

func TestWriteUserTurnRejectsUnsupportedAgent(t *testing.T) {
	var buf bytes.Buffer
	err := writeUserTurn(&buf, "opencode", "hello")
	if err == nil {
		t.Fatal("expected unsupported agent error")
	}
	if !strings.Contains(err.Error(), "unsupported agent") {
		t.Fatalf("expected unsupported agent error, got %v", err)
	}
}

func TestPrepareGenerationWritesPerGenerationSpecManifestAndSecrets(t *testing.T) {
	t.Setenv("HARNESS_CLAUDE_BASE_URL", "http://bad.invalid")
	t.Setenv("HARNESS_ANTHROPIC_BASE_URL", "http://bad.invalid")
	t.Setenv("ANTHROPIC_BASE_URL", "http://bad.invalid")
	t.Setenv("HARNESS_CLAUDE_API_KEY", "bad")
	t.Setenv("HARNESS_ANTHROPIC_API_KEY", "bad")

	dir := t.TempDir()
	secretsRoot := filepath.Join(dir, "secrets")
	rt := New(Config{
		SessionsRoot:     filepath.Join(dir, "sessions"),
		AgentHomesRoot:   filepath.Join(dir, "agent-homes"),
		BundleRoot:       filepath.Join(dir, "bundle", "out"),
		RootFSPath:       filepath.Join(dir, "rootfs"),
		SecretsRoot:      secretsRoot,
		SecretReadersGID: testSecretReadersGID(),
		Claude: ClaudeConfig{
			ProxyBindURL:               "http://0.0.0.0:8082",
			APIKey:                     "123",
			AuthToken:                  "123",
			Model:                      "sonnet",
			OutputFormat:               "stream-json",
			DisableNonessentialTraffic: true,
		},
	})
	details := testGenerationDetails(dir, "gen_a")

	artifacts, err := rt.PrepareGeneration(context.Background(), StartRequest{
		SessionID:         "sess_1",
		GenerationID:      details.GenerationID,
		Agent:             "claude",
		ClaudeSessionUUID: "11111111-2222-3333-4444-555555555555",
		ResumeClaude:      true,
		Generation:        details,
	})
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
	if manifest.WorkspacePath != "/sessions/sess_1" || manifest.AgentHomePath != "/agent-homes/sess_1" {
		t.Fatalf("unexpected workspace/home paths: %+v", manifest)
	}
	if !manifest.ResumeClaude {
		t.Fatalf("expected resume flag to be set: %+v", manifest)
	}
	if manifest.AnthropicBaseURL != "http://10.200.1.1:8082" {
		t.Fatalf("unexpected sandbox base URL: %+v", manifest)
	}
	if manifest.AnthropicAPIKeySecretID != "anthropic_api_key" || manifest.AnthropicAuthTokenSecretID != "anthropic_auth_token" || manifest.SecretVersion != "local" {
		t.Fatalf("unexpected secret refs: %+v", manifest)
	}
	if strings.Contains(string(data), `"anthropic_api_key":`) || strings.Contains(string(data), `"anthropic_auth_token":`) {
		t.Fatalf("manifest must not contain plaintext credential fields: %s", data)
	}
	if !manifest.ClaudeCodeDisableNonessentialTraffic {
		t.Fatalf("expected nonessential traffic to be disabled: %+v", manifest)
	}
	if manifest.Model != "sonnet" || manifest.OutputFormat != "stream-json" {
		t.Fatalf("unexpected Claude defaults: %+v", manifest)
	}

	var spec runtimeSpec
	specData, err := os.ReadFile(details.SpecPath)
	if err != nil {
		t.Fatalf("read spec: %v", err)
	}
	if err := json.Unmarshal(specData, &spec); err != nil {
		t.Fatalf("runtime spec is not valid JSON: %v\n%s", err, specData)
	}
	if strings.Contains(string(specData), "phase2-template") {
		t.Fatalf("runtime spec hot path must not reference phase2-template: %s", specData)
	}
	if mountSource(spec.Mounts, "/harness-control") != details.ControlDirPath {
		t.Fatalf("control mount = %q, want %q", mountSource(spec.Mounts, "/harness-control"), details.ControlDirPath)
	}
	bridgeMount := mountByDestination(spec.Mounts, "/harness-control/bridge")
	if bridgeMount == nil {
		t.Fatalf("runtime spec missing bridge mount: %+v", spec.Mounts)
	}
	if bridgeMount.Source != details.BridgeDirPath || strings.Join(bridgeMount.Options, ",") != "rbind,rw" {
		t.Fatalf("unexpected bridge mount: %+v", bridgeMount)
	}
	if bridgeMount.Annotations["dev.gvisor.spec.mount./harness-control/bridge.share"] != "exclusive" ||
		bridgeMount.Annotations["dev.gvisor.spec.mount./harness-control/bridge.type"] != "bind" {
		t.Fatalf("bridge mount missing exclusive annotation: %+v", bridgeMount.Annotations)
	}
	for _, name := range []string{"inbox", "outbox", "heartbeat", "tmp"} {
		if info, err := os.Stat(filepath.Join(details.BridgeDirPath, name)); err != nil || !info.IsDir() {
			t.Fatalf("bridge dir %s not initialized: info=%v err=%v", name, info, err)
		}
	}
	if mountSource(spec.Mounts, "/harness-secrets") != details.SecretsDirPath {
		t.Fatalf("secret mount = %q, want %q", mountSource(spec.Mounts, "/harness-secrets"), details.SecretsDirPath)
	}
	if _, err := os.Stat(filepath.Join(details.SecretsDirPath, "anthropic_api_key", "local")); err != nil {
		t.Fatalf("materialized api key secret: %v", err)
	}
	if _, err := os.Stat(filepath.Join(details.SecretsDirPath, "anthropic_auth_token", "local")); err != nil {
		t.Fatalf("materialized auth token secret: %v", err)
	}
	assertSecretPath(t, secretsRoot)
	assertSecretPath(t, filepath.Join(secretsRoot, "anthropic_api_key"))
	assertSecretFile(t, filepath.Join(secretsRoot, "anthropic_api_key", "local"))
	assertSecretPath(t, details.SecretsDirPath)
	assertSecretPath(t, filepath.Join(details.SecretsDirPath, "anthropic_api_key"))
	assertSecretFile(t, filepath.Join(details.SecretsDirPath, "anthropic_api_key", "local"))
}

func TestPrepareShellGenerationHasNoSecretMount(t *testing.T) {
	dir := t.TempDir()
	rt := New(Config{
		SessionsRoot:   filepath.Join(dir, "sessions"),
		AgentHomesRoot: filepath.Join(dir, "agent-homes"),
		BundleRoot:     filepath.Join(dir, "bundle", "out"),
		RootFSPath:     filepath.Join(dir, "rootfs"),
	})
	details := testGenerationDetails(dir, "gen_shell")
	details.SessionID = "sess_shell"
	details.Agent = "sh"
	details.RequiresSecretDrop = false
	details.ManifestAnthropicBaseURL = ""
	details.AnthropicAPIKeySecretID = ""
	details.AnthropicAuthTokenSecretID = ""
	details.SecretVersion = ""
	details.SecretsDirPath = ""

	if _, err := rt.PrepareGeneration(context.Background(), StartRequest{
		SessionID:    "sess_shell",
		GenerationID: details.GenerationID,
		Agent:        "sh",
		Generation:   details,
	}); err != nil {
		t.Fatalf("prepare shell generation: %v", err)
	}
	specData, err := os.ReadFile(details.SpecPath)
	if err != nil {
		t.Fatalf("read shell spec: %v", err)
	}
	if strings.Contains(string(specData), "/harness-secrets") {
		t.Fatalf("shell spec must not mount secrets: %s", specData)
	}
	var manifestFile controlManifestFile
	if err := json.Unmarshal(mustReadFile(t, details.ControlManifestPath), &manifestFile); err != nil {
		t.Fatalf("read shell manifest: %v", err)
	}
	if manifestFile.Payload.SecretMountPath != "" || manifestFile.Payload.AnthropicAPIKeySecretID != "" {
		t.Fatalf("shell manifest must not reference secrets: %+v", manifestFile.Payload)
	}
	if manifestFile.Payload.AnthropicBaseURL != "" {
		t.Fatalf("shell manifest must not require Claude base URL: %+v", manifestFile.Payload)
	}
}

func TestPrepareShellGenerationRejectsSecretReferences(t *testing.T) {
	dir := t.TempDir()
	rt := New(Config{
		SessionsRoot:     filepath.Join(dir, "sessions"),
		AgentHomesRoot:   filepath.Join(dir, "agent-homes"),
		BundleRoot:       filepath.Join(dir, "bundle", "out"),
		RootFSPath:       filepath.Join(dir, "rootfs"),
		SecretsRoot:      filepath.Join(dir, "secrets"),
		SecretReadersGID: testSecretReadersGID(),
	})
	details := testGenerationDetails(dir, "gen_shell_bad")
	details.SessionID = "sess_shell"
	details.Agent = "sh"
	details.RequiresSecretDrop = false

	_, err := rt.PrepareGeneration(context.Background(), StartRequest{
		SessionID:    "sess_shell",
		GenerationID: details.GenerationID,
		Agent:        "sh",
		Generation:   details,
	})
	if err == nil {
		t.Fatal("expected shell secret rejection")
	}
	if !strings.Contains(err.Error(), "shell_secret_disallowed") {
		t.Fatalf("expected shell_secret_disallowed, got %v", err)
	}
}

func TestPrepareGenerationRejectsMismatchedIdentity(t *testing.T) {
	dir := t.TempDir()
	details := testGenerationDetails(dir, "gen_mismatch")
	rt := New(Config{
		SessionsRoot:     filepath.Join(dir, "sessions"),
		AgentHomesRoot:   filepath.Join(dir, "agent-homes"),
		BundleRoot:       filepath.Join(dir, "bundle", "out"),
		RootFSPath:       filepath.Join(dir, "rootfs"),
		SecretsRoot:      filepath.Join(dir, "secrets"),
		SecretReadersGID: testSecretReadersGID(),
		Claude: ClaudeConfig{
			APIKey:    "123",
			AuthToken: "123",
		},
	})

	_, err := rt.PrepareGeneration(context.Background(), StartRequest{
		SessionID:    "sess_wrong",
		GenerationID: details.GenerationID,
		Agent:        "claude",
		Generation:   details,
	})
	if err == nil {
		t.Fatal("expected identity mismatch error")
	}
	if !strings.Contains(err.Error(), "generation session mismatch") {
		t.Fatalf("expected generation session mismatch, got %v", err)
	}
}

func testGenerationDetails(dir, generationID string) store.RuntimeGenerationDetails {
	return store.RuntimeGenerationDetails{
		SessionID:                  "sess_1",
		GenerationID:               generationID,
		NetworkProfileID:           "net_" + generationID,
		AgentRuntimeProfileID:      "arp_" + generationID,
		RunscPlatform:              "systrap",
		ControlDirPath:             filepath.Join(dir, "run", "control", "gen-"+generationID),
		ControlManifestPath:        filepath.Join(dir, "run", "control", "gen-"+generationID, "session.json"),
		BundleDirPath:              filepath.Join(dir, "run", "runtime", "gen-"+generationID),
		SpecPath:                   filepath.Join(dir, "run", "runtime", "gen-"+generationID, "config.json"),
		SecretsDirPath:             filepath.Join(dir, "run", "control", "gen-"+generationID, "secrets"),
		BridgeDirPath:              filepath.Join(dir, "run", "bridge", "gen-"+generationID),
		LogDirPath:                 filepath.Join(dir, "run", "logs", "gen-"+generationID),
		HostGatewayIP:              "10.200.1.1",
		SandboxBaseURL:             "http://10.200.1.1:8082",
		NetnsName:                  "harness-gen-" + generationID,
		EgressPolicyDigest:         "egress_digest",
		Agent:                      "claude",
		Model:                      "sonnet",
		OutputFormat:               "stream-json",
		DisableNonessentialTraffic: true,
		RequiresSecretDrop:         true,
		ManifestAnthropicBaseURL:   "http://10.200.1.1:8082",
		AnthropicAPIKeySecretID:    "anthropic_api_key",
		AnthropicAuthTokenSecretID: "anthropic_auth_token",
		SecretVersion:              "local",
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

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return data
}

func assertSecretPath(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat secret dir %s: %v", path, err)
	}
	if !info.IsDir() {
		t.Fatalf("secret path %s is not a directory", path)
	}
	if mode := info.Mode().Perm(); mode != 0o750 {
		t.Fatalf("secret dir %s mode=%04o want 0750", path, mode)
	}
	assertPathGID(t, info, path, testSecretReadersGID())
}

func assertSecretFile(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat secret file %s: %v", path, err)
	}
	if info.IsDir() {
		t.Fatalf("secret file %s is a directory", path)
	}
	if mode := info.Mode().Perm(); mode != 0o440 {
		t.Fatalf("secret file %s mode=%04o want 0440", path, mode)
	}
	assertPathGID(t, info, path, testSecretReadersGID())
}

func assertPathGID(t *testing.T, info os.FileInfo, path string, want int) {
	t.Helper()
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("stat ownership unavailable for %s", path)
	}
	if int(stat.Gid) != want {
		t.Fatalf("%s gid=%d want %d", path, stat.Gid, want)
	}
}

func closedDone() <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}

func testSecretReadersGID() int {
	gid := os.Getgid()
	if gid > 0 {
		return gid
	}
	return 1
}
