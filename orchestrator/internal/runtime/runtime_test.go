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

func TestResolveCheckpointPathPrefersGenerationPath(t *testing.T) {
	dir := t.TempDir()
	generationPath := filepath.Join(dir, "run", "gen_a", "checkpoint")
	legacyPath := filepath.Join(dir, "checkpoints", "sess_1")
	if err := os.MkdirAll(generationPath, 0o755); err != nil {
		t.Fatalf("create generation checkpoint: %v", err)
	}
	if err := os.MkdirAll(legacyPath, 0o755); err != nil {
		t.Fatalf("create legacy checkpoint: %v", err)
	}
	rt := New(Config{CheckpointsRoot: filepath.Join(dir, "checkpoints")})

	got, err := rt.resolveCheckpointPath(StartRequest{
		SessionID: "sess_1",
		Generation: store.RuntimeGenerationDetails{
			CheckpointPath: generationPath,
		},
	})
	if err != nil {
		t.Fatalf("resolve checkpoint path: %v", err)
	}
	if got != generationPath {
		t.Fatalf("checkpoint path=%q want generation path %q", got, generationPath)
	}
}

func TestRuntimeStartRestoreRequiresCheckpointPath(t *testing.T) {
	rt := New(Config{DefaultAgent: "claude"})
	res := rt.Start(context.Background(), StartRequest{
		SessionID:             "sess_missing_checkpoint",
		Agent:                 "claude",
		RestoreFromCheckpoint: true,
		Generation: store.RuntimeGenerationDetails{
			SessionID:    "sess_missing_checkpoint",
			GenerationID: "gen_missing",
		},
		PreparedArtifacts: GenerationArtifacts{
			BundleDir:      filepath.Join(t.TempDir(), "bundle"),
			SpecPath:       filepath.Join(t.TempDir(), "bundle", "config.json"),
			ManifestPath:   filepath.Join(t.TempDir(), "control", "session.json"),
			ManifestDigest: "digest",
		},
	}, nil)
	if res.Err == nil {
		t.Fatal("expected missing checkpoint error")
	}
	if !strings.Contains(res.Err.Error(), "checkpoint path is required") {
		t.Fatalf("expected checkpoint path error, got %v", res.Err)
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
	changed.HostHostname = "other-host"
	changed.BridgeDirPath = "/tmp/other-bridge"
	changed.NetnsName = "other-netns"
	changed.HostGatewayIP = "10.1.2.3"
	changed.SandboxSourceIP = "10.1.2.4"
	second, err := projectedControlManifestDigest(changed)
	if err != nil {
		t.Fatalf("project changed manifest: %v", err)
	}
	if first != second {
		t.Fatalf("regenerable fields changed projected digest: %s != %s", first, second)
	}
	strictChanged := base
	strictChanged.SecretVersion = "rotated"
	third, err := projectedControlManifestDigest(strictChanged)
	if err != nil {
		t.Fatalf("project strict changed manifest: %v", err)
	}
	if first == third {
		t.Fatalf("strict field change did not change projected digest: %s", first)
	}
}

func TestCanonicalManifestDigestMatchesSandboxFixture(t *testing.T) {
	data := mustReadFile(t, filepath.Join("..", "..", "..", "docs", "phase7", "fixtures", "control-manifest-payload.json"))
	var payload any
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatalf("read canonical manifest fixture: %v", err)
	}
	canonical, err := canonicalJSON(payload)
	if err != nil {
		t.Fatalf("canonicalize manifest fixture: %v", err)
	}
	const wantCanonical = `{"agent":"sh","agent_home_path":"/agent-homes/sess_fixture","agent_runtime_profile_id":"arp_fixture","attempt_id":"attempt_fixture","bridge_dir_path":"/run/bridge/gen_fixture","bundle_digest":"bundle_digest_fixture","claude_code_disable_nonessential_traffic":true,"created_at":"2026-05-25T00:00:00Z","egress_policy_digest":"egress_digest_fixture","generation_id":"gen_fixture","host_gateway_ip":"10.240.0.1","host_hostname":"host-fixture","manifest_version":1,"netns_name":"hns-fixture","network_profile_id":"net_fixture","output_format":"stream-json","proxy_bind_url":"http://10.240.0.1:8082","resume_claude":false,"runsc_platform":"systrap","runsc_version":"runsc release-20260511.0","runtime_config_digest":"runtime_config_digest_fixture","sandbox_source_ip":"10.240.0.2","session_id":"sess_fixture","spec_digest":"spec_digest_fixture","workspace_path":"/sessions/sess_fixture"}`
	const wantDigest = "2dcc2b3e69e7792c65fb521284d627253787e77f60202482e2839fe1fd97a341"
	if string(canonical) != wantCanonical {
		t.Fatalf("canonical fixture mismatch:\ngot  %s\nwant %s", canonical, wantCanonical)
	}
	if got := digestHex(canonical); got != wantDigest {
		t.Fatalf("canonical fixture digest=%s want %s", got, wantDigest)
	}
}

func TestValidateCheckpointRestoreRejectsMetadataMismatch(t *testing.T) {
	dir := t.TempDir()
	checkpointPath := filepath.Join(dir, "checkpoint")
	writeCheckpointFiles(t, checkpointPath)
	details := testGenerationDetails(dir, "gen_restore")
	details.CheckpointNetworkProfileID = details.NetworkProfileID
	details.CheckpointAgentRuntimeProfileID = details.AgentRuntimeProfileID
	details.CheckpointRunscPlatform = details.RunscPlatform
	details.CheckpointRunscVersion = "runsc test"
	details.CheckpointBundleDigest = "bundle_digest"
	details.CheckpointRuntimeConfigDigest = "runtime_config_digest"
	details.CheckpointControlManifestDigest = "control_manifest_digest"
	artifacts := GenerationArtifacts{
		RunscVersion:            "runsc test",
		BundleDigest:            "bundle_digest",
		RuntimeConfigDigest:     "runtime_config_digest",
		ProjectedManifestDigest: "other_control_manifest_digest",
	}

	err := validateCheckpointRestore(details, artifacts, checkpointPath)
	if err == nil {
		t.Fatal("expected checkpoint metadata mismatch")
	}
	if !strings.Contains(err.Error(), "checkpoint_control_manifest_digest") {
		t.Fatalf("expected manifest digest mismatch, got %v", err)
	}
}

func TestValidateCheckpointRestoreRequiresCheckpointImageManifest(t *testing.T) {
	dir := t.TempDir()
	checkpointPath := filepath.Join(dir, "checkpoint")
	writeCheckpointFilesWithoutManifest(t, checkpointPath)

	err := validateCheckpointImageManifest(checkpointPath)
	if err == nil {
		t.Fatal("expected missing checkpoint image manifest")
	}
	if !strings.Contains(err.Error(), "checkpoint image manifest missing") {
		t.Fatalf("expected checkpoint image manifest missing, got %v", err)
	}
}

func TestValidateCheckpointRestoreRejectsCheckpointImageManifestMismatch(t *testing.T) {
	dir := t.TempDir()
	checkpointPath := filepath.Join(dir, "checkpoint")
	writeCheckpointFiles(t, checkpointPath)
	if err := os.WriteFile(filepath.Join(checkpointPath, "pages.img"), []byte("y"), 0o644); err != nil {
		t.Fatalf("mutate checkpoint file: %v", err)
	}

	err := validateCheckpointImageManifest(checkpointPath)
	if err == nil {
		t.Fatal("expected checkpoint image manifest mismatch")
	}
	if !strings.Contains(err.Error(), "checkpoint image manifest sha256 mismatch") {
		t.Fatalf("expected checkpoint image manifest sha256 mismatch, got %v", err)
	}
}

func TestValidateCheckpointRestoreRejectsExtraCheckpointImageManifestMismatch(t *testing.T) {
	dir := t.TempDir()
	checkpointPath := filepath.Join(dir, "checkpoint")
	writeCheckpointFiles(t, checkpointPath)
	extraPath := filepath.Join(checkpointPath, "memory_extra.img")
	if err := os.WriteFile(extraPath, []byte("x"), 0o644); err != nil {
		t.Fatalf("write extra checkpoint file: %v", err)
	}
	manifest, err := buildCheckpointImageManifest(checkpointPath)
	if err != nil {
		t.Fatalf("build checkpoint image manifest: %v", err)
	}
	entry, err := checkpointImageManifestEntry(checkpointPath, "memory_extra.img")
	if err != nil {
		t.Fatalf("build extra checkpoint image manifest entry: %v", err)
	}
	manifest.Files = append(manifest.Files, entry)
	if err := writeJSONFileAtomic(filepath.Join(checkpointPath, checkpointImageManifestFileName), manifest, 0o644); err != nil {
		t.Fatalf("write checkpoint image manifest: %v", err)
	}
	if err := os.WriteFile(extraPath, []byte("y"), 0o644); err != nil {
		t.Fatalf("mutate extra checkpoint file: %v", err)
	}

	err = validateCheckpointImageManifest(checkpointPath)
	if err == nil {
		t.Fatal("expected extra checkpoint image manifest mismatch")
	}
	if !strings.Contains(err.Error(), "checkpoint image manifest sha256 mismatch for memory_extra.img") {
		t.Fatalf("expected extra checkpoint image manifest sha256 mismatch, got %v", err)
	}
}

func TestRuntimeStartRestoreRejectsMetadataBeforeRunscRestore(t *testing.T) {
	dir := t.TempDir()
	checkpointPath := filepath.Join(dir, "checkpoint")
	writeCheckpointFiles(t, checkpointPath)
	runner := &recordingCommandRunner{
		outputs: map[string][]byte{
			"runsc --version": []byte("runsc current"),
		},
	}
	rt := New(Config{
		DefaultAgent:   "claude",
		SessionsRoot:   filepath.Join(dir, "sessions"),
		AgentHomesRoot: filepath.Join(dir, "agent-homes"),
		RootFSPath:     filepath.Join(dir, "rootfs"),
		RunscNetwork:   "host",
		CommandRunner:  runner,
		SandboxUID:     testSandboxUID(),
		SandboxGID:     testSandboxGID(),
	})
	details := testGenerationDetails(dir, "gen_restore_mismatch")
	details.Agent = "sh"
	details.OutputFormat = "shell_pty"
	details.RequiresSecretDrop = false
	details.ManifestAnthropicBaseURL = ""
	details.AnthropicAPIKeySecretID = ""
	details.AnthropicAuthTokenSecretID = ""
	details.SecretVersion = ""
	details.SecretsDirPath = ""
	details.CheckpointPath = checkpointPath
	details.CheckpointNetworkProfileID = details.NetworkProfileID
	details.CheckpointAgentRuntimeProfileID = details.AgentRuntimeProfileID
	details.CheckpointRunscPlatform = details.RunscPlatform
	details.CheckpointRunscVersion = "runsc old"

	res := rt.Start(context.Background(), StartRequest{
		SessionID:             "sess_1",
		GenerationID:          details.GenerationID,
		Agent:                 "sh",
		RestoreFromCheckpoint: true,
		Generation:            details,
	}, nil)
	if res.Err == nil {
		t.Fatal("expected restore metadata mismatch")
	}
	if !strings.Contains(res.Err.Error(), "checkpoint_runsc_version") {
		t.Fatalf("expected runsc version mismatch, got %v", res.Err)
	}
	for _, command := range runner.Commands() {
		if strings.Contains(command, " restore ") {
			t.Fatalf("runsc restore ran despite metadata mismatch: %v", runner.Commands())
		}
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

func TestRuntimeStartDoesNotReuseContainerForDifferentGeneration(t *testing.T) {
	rt := New(Config{DefaultAgent: "claude"})
	stdin := &recordingWriteCloser{}
	canceled := make(chan struct{})
	rt.containers["sess_1"] = &Container{
		SessionID:    "sess_1",
		GenerationID: "gen_old",
		RestoreID:    "phase3-sess_1",
		Agent:        "claude",
		Stdin:        stdin,
		OutputHub:    NewOutputHub(),
		Cancel:       func() { close(canceled) },
	}

	res := rt.Start(context.Background(), StartRequest{
		SessionID:    "sess_1",
		GenerationID: "gen_new",
		Agent:        "claude",
		FirstMessage: "hello stale generation",
		WaitForTurn:  true,
		Done:         closedDone(),
		Generation: store.RuntimeGenerationDetails{
			SessionID:    "sess_1",
			GenerationID: "gen_new",
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

func TestEvictContainerByRestoreIDCancelsAndRemovesMatchingContainer(t *testing.T) {
	rt := New(Config{})
	canceled := make(chan struct{})
	rt.containers["sess_1"] = &Container{
		SessionID: "sess_1",
		RestoreID: "phase3-sess_1",
		Cancel:    func() { close(canceled) },
	}
	rt.containers["sess_2"] = &Container{SessionID: "sess_2", RestoreID: "phase3-sess_2"}

	rt.evictContainerByRestoreID("phase3-sess_1")

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

func TestCheckpointRequiresGenerationIdentity(t *testing.T) {
	rt := New(Config{})
	rt.containers["sess_1"] = &Container{SessionID: "sess_1", GenerationID: "gen_a", RestoreID: "phase3-sess_1"}

	err := rt.Checkpoint(context.Background(), CheckpointRequest{SessionID: "sess_1"})
	if err == nil || !strings.Contains(err.Error(), "generation id is required") {
		t.Fatalf("expected missing generation id error, got %v", err)
	}
	err = rt.Checkpoint(context.Background(), CheckpointRequest{SessionID: "sess_1", GenerationID: "gen_b"})
	if err == nil || !strings.Contains(err.Error(), "container generation mismatch") {
		t.Fatalf("expected generation mismatch error, got %v", err)
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
		},
	}
	rt := New(Config{
		RunscNetwork:  "sandbox",
		RunscOverlay2: "none",
		CommandRunner: runner,
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
	}
	if got := runner.Commands(); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("unexpected commands:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	for _, command := range runner.Commands() {
		if strings.Contains(command, "/v1/messages") || strings.Contains(command, "x-api-key") {
			t.Fatalf("pre-start sandbox network probe must not call model endpoints or pass API keys: %s", command)
		}
	}
}

func TestProbeSandboxNetworkRetriesAndUsesConfiguredHealthzStatuses(t *testing.T) {
	healthz := "ip netns exec harness-gen-a curl -sS --max-time 2 -o /dev/null -w %{http_code} http://10.250.0.1:8082/healthz"
	runner := &recordingCommandRunner{
		sequence: map[string][]commandResult{
			healthz: {
				{out: []byte("503")},
				{out: []byte("204")},
			},
		},
	}
	rt := New(Config{
		CommandRunner:         runner,
		PreStartProbeAttempts: 2,
		PreStartProbeInterval: time.Nanosecond,
		ProbeHealthzStatuses:  []int{204},
	})
	details := testGenerationDetails(t.TempDir(), "gen_a")
	details.NetnsName = "harness-gen-a"
	details.ProbeURL = "http://10.250.0.1:8082"

	if err := rt.probeSandboxNetwork(context.Background(), details); err != nil {
		t.Fatalf("probe sandbox network: %v", err)
	}
	want := []string{healthz, healthz}
	if got := runner.Commands(); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("unexpected commands:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestDestroyGenerationResourcesDeletesPerGenerationNetwork(t *testing.T) {
	runner := &recordingCommandRunner{}
	dir := t.TempDir()
	rt := New(Config{
		RunscNetwork:    "sandbox",
		RunscOverlay2:   "none",
		Phase7RunDir:    filepath.Join(dir, "run"),
		CheckpointsRoot: filepath.Join(dir, "checkpoints"),
		CommandRunner:   runner,
	})
	details := testGenerationDetails(dir, "gen_a")
	details.RunscNetwork = "sandbox"
	details.NetnsName = "harness-gen-a"
	details.HostVeth = "hgenah"

	cleanup, err := rt.DestroyGenerationResources(context.Background(), details)
	if err != nil {
		t.Fatalf("destroy generation resources: %v", err)
	}
	if !cleanup.NftTableDeleted || !cleanup.HostVethDeleted || !cleanup.NetnsDeleted {
		t.Fatalf("unexpected cleanup result: %+v", cleanup)
	}

	want := []string{
		"nft delete table inet harness_gen_gen_a",
		"ip link delete hgenah",
		"ip netns delete harness-gen-a",
	}
	if got := runner.Commands(); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("unexpected commands:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestDestroyGenerationResourcesDeletesFilesystemInNonSandboxMode(t *testing.T) {
	dir := t.TempDir()
	rt := New(Config{
		RunscNetwork:    "host",
		Phase7RunDir:    filepath.Join(dir, "run"),
		CheckpointsRoot: filepath.Join(dir, "checkpoints"),
		CommandRunner:   &recordingCommandRunner{},
	})
	details := testGenerationDetails(dir, "gen_cleanup")
	details.RunscNetwork = "host"
	details.NetworkHostsPath = filepath.Join(dir, "run", "network", "gen-"+details.GenerationID, "hosts")
	createGenerationFilesystem(t, details)

	cleanup, err := rt.DestroyGenerationResources(context.Background(), details)
	if err != nil {
		t.Fatalf("destroy generation resources: %v", err)
	}
	if !cleanup.CheckpointDeleted || !cleanup.ControlDirDeleted || !cleanup.BundleDirDeleted || !cleanup.BridgeDirDeleted || !cleanup.NetworkDirDeleted || !cleanup.LogDirDeleted {
		t.Fatalf("unexpected filesystem cleanup result: %+v", cleanup)
	}
	assertGenerationFilesystemMissing(t, generationFilesystemPaths(details))

	cleanup, err = rt.DestroyGenerationResources(context.Background(), details)
	if err != nil {
		t.Fatalf("destroy generation resources second pass: %v", err)
	}
	if cleanup.CheckpointDeleted || cleanup.ControlDirDeleted || cleanup.BundleDirDeleted || cleanup.BridgeDirDeleted || cleanup.NetworkDirDeleted || cleanup.LogDirDeleted {
		t.Fatalf("missing paths should be idempotent, got cleanup result: %+v", cleanup)
	}
}

func TestDestroyGenerationResourcesDeletesLegacyCheckpointPath(t *testing.T) {
	dir := t.TempDir()
	checkpointsRoot := filepath.Join(dir, "checkpoints")
	rt := New(Config{
		RunscNetwork:    "host",
		Phase7RunDir:    filepath.Join(dir, "run"),
		CheckpointsRoot: checkpointsRoot,
		CommandRunner:   &recordingCommandRunner{},
	})
	details := testGenerationDetails(dir, "gen_legacy")
	details.RunscNetwork = "host"
	details.CheckpointPath = filepath.Join(checkpointsRoot, details.SessionID)
	createGenerationFilesystem(t, details)

	cleanup, err := rt.DestroyGenerationResources(context.Background(), details)
	if err != nil {
		t.Fatalf("destroy generation resources: %v", err)
	}
	if !cleanup.CheckpointDeleted {
		t.Fatalf("legacy checkpoint path was not deleted: %+v", cleanup)
	}
	assertGenerationFilesystemMissing(t, generationFilesystemPaths(details))
}

func TestDestroyGenerationResourcesRejectsUnsafeFilesystemPaths(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(t *testing.T, dir string, details *store.RuntimeGenerationDetails)
	}{
		{
			name: "empty checkpoint",
			mutate: func(_ *testing.T, _ string, details *store.RuntimeGenerationDetails) {
				details.CheckpointPath = ""
			},
		},
		{
			name: "outside phase7 root",
			mutate: func(_ *testing.T, dir string, details *store.RuntimeGenerationDetails) {
				details.BridgeDirPath = filepath.Join(dir, "outside", "gen-"+details.GenerationID)
			},
		},
		{
			name: "dotdot escape",
			mutate: func(_ *testing.T, _ string, details *store.RuntimeGenerationDetails) {
				details.BundleDirPath = filepath.Join(filepath.Dir(details.BundleDirPath), "x") + string(filepath.Separator) + ".." + string(filepath.Separator) + filepath.Base(details.BundleDirPath)
			},
		},
		{
			name: "wrong generation component",
			mutate: func(_ *testing.T, dir string, details *store.RuntimeGenerationDetails) {
				details.LogDirPath = filepath.Join(dir, "run", "logs", "gen-other")
			},
		},
		{
			name: "arbitrary checkpoint path",
			mutate: func(_ *testing.T, dir string, details *store.RuntimeGenerationDetails) {
				details.CheckpointPath = filepath.Join(dir, "run", "gen-"+details.GenerationID, "checkpoint-other")
			},
		},
		{
			name: "symlink escape",
			mutate: func(t *testing.T, dir string, details *store.RuntimeGenerationDetails) {
				t.Helper()
				outside := filepath.Join(dir, "outside-target")
				if err := os.MkdirAll(outside, 0o755); err != nil {
					t.Fatalf("create outside target: %v", err)
				}
				if err := os.RemoveAll(details.ControlDirPath); err != nil {
					t.Fatalf("remove control path before symlink: %v", err)
				}
				if err := os.Symlink(outside, details.ControlDirPath); err != nil {
					t.Fatalf("create symlink escape: %v", err)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			rt := New(Config{
				RunscNetwork:    "host",
				Phase7RunDir:    filepath.Join(dir, "run"),
				CheckpointsRoot: filepath.Join(dir, "checkpoints"),
				CommandRunner:   &recordingCommandRunner{},
			})
			details := testGenerationDetails(dir, "gen_unsafe")
			details.RunscNetwork = "host"
			createGenerationFilesystem(t, details)
			originalPaths := generationFilesystemPaths(details)
			tc.mutate(t, dir, &details)

			if _, err := rt.DestroyGenerationResources(context.Background(), details); err == nil {
				t.Fatal("expected unsafe cleanup path error")
			}
			assertGenerationFilesystemPresent(t, originalPaths)
		})
	}
}

func TestDestroyGenerationResourcesRejectsUnsafeLegacyCheckpointPaths(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(t *testing.T, dir, checkpointsRoot string, details *store.RuntimeGenerationDetails)
		check  func(t *testing.T, dir, checkpointsRoot string, details store.RuntimeGenerationDetails)
	}{
		{
			name: "wrong session",
			mutate: func(t *testing.T, _ string, checkpointsRoot string, details *store.RuntimeGenerationDetails) {
				t.Helper()
				details.CheckpointPath = filepath.Join(checkpointsRoot, "sess_other")
				if err := os.MkdirAll(details.CheckpointPath, 0o755); err != nil {
					t.Fatalf("create wrong-session checkpoint: %v", err)
				}
			},
		},
		{
			name: "outside checkpoint root",
			mutate: func(t *testing.T, dir, _ string, details *store.RuntimeGenerationDetails) {
				t.Helper()
				details.CheckpointPath = filepath.Join(dir, "outside-checkpoints", details.SessionID)
				if err := os.MkdirAll(details.CheckpointPath, 0o755); err != nil {
					t.Fatalf("create outside checkpoint: %v", err)
				}
			},
		},
		{
			name: "symlink escape",
			mutate: func(t *testing.T, dir, checkpointsRoot string, details *store.RuntimeGenerationDetails) {
				t.Helper()
				outside := filepath.Join(dir, "outside-legacy-target")
				if err := os.MkdirAll(outside, 0o755); err != nil {
					t.Fatalf("create outside target: %v", err)
				}
				details.CheckpointPath = filepath.Join(checkpointsRoot, details.SessionID)
				if err := os.MkdirAll(filepath.Dir(details.CheckpointPath), 0o755); err != nil {
					t.Fatalf("create legacy checkpoint parent: %v", err)
				}
				if err := os.RemoveAll(details.CheckpointPath); err != nil {
					t.Fatalf("remove legacy checkpoint path before symlink: %v", err)
				}
				if err := os.Symlink(outside, details.CheckpointPath); err != nil {
					t.Fatalf("create legacy checkpoint symlink: %v", err)
				}
			},
			check: func(t *testing.T, dir, _ string, details store.RuntimeGenerationDetails) {
				t.Helper()
				if _, err := os.Lstat(details.CheckpointPath); err != nil {
					t.Fatalf("legacy symlink should remain after rejected cleanup: %v", err)
				}
				if _, err := os.Stat(filepath.Join(dir, "outside-legacy-target")); err != nil {
					t.Fatalf("outside symlink target should remain: %v", err)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			checkpointsRoot := filepath.Join(dir, "checkpoints")
			rt := New(Config{
				RunscNetwork:    "host",
				Phase7RunDir:    filepath.Join(dir, "run"),
				CheckpointsRoot: checkpointsRoot,
				CommandRunner:   &recordingCommandRunner{},
			})
			details := testGenerationDetails(dir, "gen_legacy_unsafe")
			details.RunscNetwork = "host"
			createGenerationFilesystem(t, details)
			originalPaths := generationFilesystemPaths(details)
			tc.mutate(t, dir, checkpointsRoot, &details)

			if _, err := rt.DestroyGenerationResources(context.Background(), details); err == nil {
				t.Fatal("expected unsafe legacy checkpoint path error")
			}
			assertGenerationFilesystemPresent(t, originalPaths)
			if tc.check != nil {
				tc.check(t, dir, checkpointsRoot, details)
			}
		})
	}
}

func TestDestroyGenerationResourcesCleansFilesystemWithIncompleteSandboxMetadata(t *testing.T) {
	runner := &recordingCommandRunner{}
	dir := t.TempDir()
	rt := New(Config{
		RunscNetwork:    "sandbox",
		Phase7RunDir:    filepath.Join(dir, "run"),
		CheckpointsRoot: filepath.Join(dir, "checkpoints"),
		CommandRunner:   runner,
	})
	details := testGenerationDetails(dir, "gen_missing_net")
	details.RunscNetwork = "sandbox"
	details.NetnsName = ""
	details.HostVeth = ""
	createGenerationFilesystem(t, details)

	cleanup, err := rt.DestroyGenerationResources(context.Background(), details)
	if err != nil {
		t.Fatalf("destroy generation resources: %v", err)
	}
	if !cleanup.CheckpointDeleted || !cleanup.ControlDirDeleted || !cleanup.BundleDirDeleted || !cleanup.BridgeDirDeleted || !cleanup.LogDirDeleted {
		t.Fatalf("filesystem cleanup did not run with missing sandbox metadata: %+v", cleanup)
	}
	if !cleanup.NftTableDeleted || cleanup.HostVethDeleted || cleanup.NetnsDeleted {
		t.Fatalf("unexpected network cleanup result with missing metadata: %+v", cleanup)
	}
	assertGenerationFilesystemMissing(t, generationFilesystemPaths(details))

	want := []string{"nft delete table inet harness_gen_gen_missing_net"}
	if got := runner.Commands(); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("unexpected commands:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestDestroyTreatsMissingRunscContainerAsAbsent(t *testing.T) {
	runner := &recordingCommandRunner{
		sequence: map[string][]commandResult{
			"runsc -root /runsc delete phase3-missing": {
				{out: []byte("container phase3-missing not found"), err: errors.New("exit status 1")},
			},
		},
	}
	rt := New(Config{RunscRoot: "/runsc", CommandRunner: runner})

	if err := rt.Destroy(context.Background(), "phase3-missing"); err != nil {
		t.Fatalf("destroy missing runsc container: %v", err)
	}
	want := []string{
		"runsc -root /runsc kill phase3-missing KILL",
		"runsc -root /runsc delete phase3-missing",
	}
	if got := runner.Commands(); !slices.Equal(got, want) {
		t.Fatalf("commands=%v want %v", got, want)
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
		SessionsRoot:       filepath.Join(dir, "sessions"),
		AgentHomesRoot:     filepath.Join(dir, "agent-homes"),
		BundleRoot:         filepath.Join(dir, "bundle", "out"),
		RootFSPath:         filepath.Join(dir, "rootfs"),
		SecretsRoot:        secretsRoot,
		SecretReadersGID:   testSecretReadersGID(),
		BridgeHeartbeat:    20 * time.Second,
		BridgePollInterval: 5 * time.Millisecond,
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
	if manifest.ManifestVersion != 1 {
		t.Fatalf("manifest_version=%d want 1", manifest.ManifestVersion)
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
	if manifest.SandboxSourceIP != "10.200.1.2" {
		t.Fatalf("unexpected sandbox source ip: %+v", manifest)
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
	env := specEnv(spec.Process.Env)
	if env["HARNESS_BRIDGE_DIR"] != "/harness-control/bridge" ||
		env["HARNESS_BRIDGE_MODE"] != "claim-loop" ||
		env["HARNESS_EXPECTED_MANIFEST_VERSION"] != "1" ||
		env["HARNESS_BRIDGE_HEARTBEAT_INTERVAL"] != "20" ||
		env["HARNESS_BRIDGE_POLL_INTERVAL"] != "0.005" ||
		env["HARNESS_BRIDGE_IDLE_INTERVAL"] != "0.005" ||
		env["HARNESS_PROBE_HEALTHZ_STATUSES"] != "200" ||
		env["HARNESS_PROBE_MESSAGE_STATUSES"] != "400" {
		t.Fatalf("runtime spec missing bridge/probe env: %+v", env)
	}
	for _, name := range []string{"inbox", "outbox", "heartbeat", "tmp"} {
		if info, err := os.Stat(filepath.Join(details.BridgeDirPath, name)); err != nil || !info.IsDir() {
			t.Fatalf("bridge dir %s not initialized: info=%v err=%v", name, info, err)
		}
	}
	if mountSource(spec.Mounts, "/harness-secrets") != details.SecretsDirPath {
		t.Fatalf("secret mount = %q, want %q", mountSource(spec.Mounts, "/harness-secrets"), details.SecretsDirPath)
	}
	secretMount := mountByDestination(spec.Mounts, "/harness-secrets")
	if secretMount == nil || strings.Join(secretMount.Options, ",") != "rbind,ro,nosuid,nodev,noexec" {
		t.Fatalf("secret mount missing read-only hardening options: %+v", secretMount)
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

func TestPrepareClaudeHostOnlyGenerationHasNoSecretMount(t *testing.T) {
	dir := t.TempDir()
	rt := New(Config{
		SessionsRoot:       filepath.Join(dir, "sessions"),
		AgentHomesRoot:     filepath.Join(dir, "agent-homes"),
		BundleRoot:         filepath.Join(dir, "bundle", "out"),
		RootFSPath:         filepath.Join(dir, "rootfs"),
		BridgeHeartbeat:    20 * time.Second,
		BridgePollInterval: 5 * time.Millisecond,
		Claude: ClaudeConfig{
			ProxyBindURL:               "http://0.0.0.0:8082",
			Model:                      "sonnet",
			OutputFormat:               "stream-json",
			DisableNonessentialTraffic: true,
		},
	})
	details := testGenerationDetails(dir, "gen_host_only")
	details.RequiresSecretDrop = false
	details.ManifestAnthropicBaseURL = "http://harness-model-proxy.internal:8082"
	details.AnthropicAPIKeySecretID = ""
	details.AnthropicAuthTokenSecretID = ""
	details.SecretVersion = ""
	details.SecretsDirPath = ""
	details.NetworkHostsPath = filepath.Join(dir, "run", "network", "gen-"+details.GenerationID, "hosts")

	if _, err := rt.PrepareGeneration(context.Background(), StartRequest{
		SessionID:         "sess_1",
		GenerationID:      details.GenerationID,
		Agent:             "claude",
		ClaudeSessionUUID: "11111111-2222-3333-4444-555555555555",
		Generation:        details,
	}); err != nil {
		t.Fatalf("prepare host-only claude generation: %v", err)
	}

	manifestData := mustReadFile(t, details.ControlManifestPath)
	var manifestFile controlManifestFile
	if err := json.Unmarshal(manifestData, &manifestFile); err != nil {
		t.Fatalf("read host-only manifest: %v", err)
	}
	manifest := manifestFile.Payload
	if manifest.AnthropicBaseURL != "http://harness-model-proxy.internal:8082" {
		t.Fatalf("unexpected host-only base url: %+v", manifest)
	}
	if manifest.SecretMountPath != "" ||
		manifest.AnthropicAPIKeySecretID != "" ||
		manifest.AnthropicAuthTokenSecretID != "" ||
		manifest.SecretVersion != "" {
		t.Fatalf("host-only manifest must not reference secrets: %+v", manifest)
	}
	if strings.Contains(string(manifestData), "/harness-secrets") ||
		strings.Contains(string(manifestData), "anthropic_api_key") ||
		strings.Contains(string(manifestData), "anthropic_auth_token") {
		t.Fatalf("host-only manifest contains legacy secret references: %s", manifestData)
	}

	specData := mustReadFile(t, details.SpecPath)
	if strings.Contains(string(specData), "/harness-secrets") ||
		strings.Contains(string(specData), "anthropic_api_key") ||
		strings.Contains(string(specData), "anthropic_auth_token") {
		t.Fatalf("host-only spec contains legacy secret references: %s", specData)
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

func TestPrepareGenerationConcurrentSessionsUseDistinctControlManifests(t *testing.T) {
	dir := t.TempDir()
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
			_, err := rt.PrepareGeneration(context.Background(), StartRequest{
				SessionID:    tc.sessionID,
				GenerationID: tc.details.GenerationID,
				Agent:        "claude",
				Generation:   tc.details,
			})
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

func TestPublishLocalSecretVersionDoesNotOverwriteExistingVersion(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "secrets", "anthropic_api_key", "local")
	readersGID := testSecretReadersGID()
	if err := publishLocalSecretVersion(path, "first-value", readersGID); err != nil {
		t.Fatalf("publish first secret version: %v", err)
	}
	if err := publishLocalSecretVersion(path, "second-value", readersGID); err != nil {
		t.Fatalf("republish existing secret version: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read secret version: %v", err)
	}
	if string(data) != "first-value" {
		t.Fatalf("existing secret version was overwritten: %q", data)
	}
	assertSecretFile(t, path)
}

func TestPrepareShellGenerationHasNoSecretMount(t *testing.T) {
	dir := t.TempDir()
	rt := New(Config{
		SessionsRoot:   filepath.Join(dir, "sessions"),
		AgentHomesRoot: filepath.Join(dir, "agent-homes"),
		BundleRoot:     filepath.Join(dir, "bundle", "out"),
		RootFSPath:     filepath.Join(dir, "rootfs"),
		SandboxUID:     testSandboxUID(),
		SandboxGID:     testSandboxGID(),
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
	var spec runtimeSpec
	if err := json.Unmarshal(specData, &spec); err != nil {
		t.Fatalf("read shell spec json: %v", err)
	}
	if !spec.Root.Readonly {
		t.Fatalf("shell phase8 rootfs must be read-only: %+v", spec.Root)
	}
	if spec.Process.User.UID != testSandboxUID() || spec.Process.User.GID != testSandboxGID() {
		t.Fatalf("shell phase8 user=%+v want %d:%d", spec.Process.User, testSandboxUID(), testSandboxGID())
	}
	if strings.Contains(mustJSONForTest(t, spec.Process.Capabilities), "CAP_") {
		t.Fatalf("shell phase8 capabilities must be empty: %+v", spec.Process.Capabilities)
	}
	for _, destination := range []string{"/sessions", "/agent-homes", "/harness-secrets"} {
		if mountByDestination(spec.Mounts, destination) != nil {
			t.Fatalf("shell phase8 spec must not mount %s: %+v", destination, spec.Mounts)
		}
	}
	for _, mount := range spec.Mounts {
		if slices.Contains(mount.Options, "rbind") {
			t.Fatalf("shell phase8 mount %s uses recursive bind: %+v", mount.Destination, mount)
		}
	}
	if mountSource(spec.Mounts, "/workspace") != filepath.Join(dir, "sessions", "sess_shell") {
		t.Fatalf("workspace mount = %q", mountSource(spec.Mounts, "/workspace"))
	}
	if mountSource(spec.Mounts, "/agent-home") != filepath.Join(dir, "agent-homes", "sess_shell", "sh") {
		t.Fatalf("agent-home mount = %q", mountSource(spec.Mounts, "/agent-home"))
	}
	if control := mountByDestination(spec.Mounts, "/harness-control"); control == nil || strings.Join(control.Options, ",") != "bind,ro,nosuid,nodev,noexec" {
		t.Fatalf("unexpected phase8 control mount: %+v", control)
	}
	bridgeMount := mountByDestination(spec.Mounts, "/harness-control/bridge")
	if bridgeMount == nil ||
		bridgeMount.Source != details.BridgeDirPath ||
		strings.Join(bridgeMount.Options, ",") != "bind,rw,nosuid,nodev,noexec" ||
		bridgeMount.Annotations["dev.gvisor.spec.mount./harness-control/bridge.share"] != "exclusive" {
		t.Fatalf("unexpected phase8 bridge mount: %+v", bridgeMount)
	}
	env := specEnv(spec.Process.Env)
	if env["HARNESS_AGENT_UID"] != fmt.Sprint(testSandboxUID()) ||
		env["HARNESS_AGENT_GID"] != fmt.Sprint(testSandboxGID()) ||
		env["SESSION_WORKSPACE"] != "/workspace" ||
		env["HARNESS_AGENT_HOME"] != "/agent-home" {
		t.Fatalf("shell phase8 identity/workspace env missing: %+v", env)
	}
	var manifestFile controlManifestFile
	if err := json.Unmarshal(mustReadFile(t, details.ControlManifestPath), &manifestFile); err != nil {
		t.Fatalf("read shell manifest: %v", err)
	}
	if manifestFile.Payload.WorkspacePath != "/workspace" || manifestFile.Payload.AgentHomePath != "/agent-home" {
		t.Fatalf("shell manifest must use phase8 sandbox paths: %+v", manifestFile.Payload)
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
		CheckpointPath:             filepath.Join(dir, "run", "gen-"+generationID, "checkpoint"),
		SecretsDirPath:             filepath.Join(dir, "run", "control", "gen-"+generationID, "secrets"),
		BridgeDirPath:              filepath.Join(dir, "run", "bridge", "gen-"+generationID),
		NetworkHostsPath:           "",
		LogDirPath:                 filepath.Join(dir, "run", "logs", "gen-"+generationID),
		HostGatewayIP:              "10.200.1.1",
		SandboxIPCIDR:              "10.200.1.2/30",
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
		SessionID:                            "sess_1",
		GenerationID:                         "gen_a",
		CreatedAt:                            "2026-01-01T00:00:00Z",
		AttemptID:                            "attempt-1",
		NetworkProfileID:                     "net_a",
		AgentRuntimeProfileID:                "arp_a",
		Agent:                                "claude",
		ClaudeSessionUUID:                    "11111111-2222-3333-4444-555555555555",
		ResumeClaude:                         true,
		RunscPlatform:                        "systrap",
		RunscVersion:                         "runsc test",
		AnthropicBaseURL:                     "http://10.200.1.1:8082",
		AnthropicAPIKeySecretID:              "anthropic_api_key",
		AnthropicAuthTokenSecretID:           "anthropic_auth_token",
		SecretVersion:                        "local",
		SecretMountPath:                      "/harness-secrets",
		Model:                                "sonnet",
		OutputFormat:                         "stream-json",
		WorkspacePath:                        "/sessions/sess_1",
		AgentHomePath:                        "/agent-homes/sess_1",
		HostHostname:                         "host-a",
		NetnsName:                            "harness-gen-a",
		HostGatewayIP:                        "10.200.1.1",
		SandboxSourceIP:                      "10.200.1.2",
		BridgeDirPath:                        "/tmp/bridge-a",
		BundleDigest:                         "bundle_digest",
		RuntimeConfigDigest:                  "runtime_config_digest",
		SpecDigest:                           "spec_digest",
		EgressPolicyDigest:                   "egress_digest",
		ManifestVersion:                      1,
		ClaudeCodeDisableNonessentialTraffic: true,
		ProxyBindURL:                         "http://0.0.0.0:8082",
	}
}

func writeCheckpointFiles(t *testing.T, checkpointPath string) {
	t.Helper()
	writeCheckpointFilesWithoutManifest(t, checkpointPath)
	if err := writeCheckpointImageManifest(checkpointPath); err != nil {
		t.Fatalf("write checkpoint image manifest: %v", err)
	}
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

func TestEnsureSecretDirPreservesExistingOwner(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root to set up non-current owner")
	}
	dir := t.TempDir()
	secretsRoot := filepath.Join(dir, "secrets")
	if err := os.Mkdir(secretsRoot, 0o750); err != nil {
		t.Fatalf("mkdir secrets root: %v", err)
	}
	const ownerUID = 12345
	readersGID := testSecretReadersGID()
	if err := os.Chown(secretsRoot, ownerUID, readersGID); err != nil {
		t.Fatalf("chown secrets root: %v", err)
	}
	if err := ensureSecretDir(filepath.Join(secretsRoot, "secret_id"), readersGID); err != nil {
		t.Fatalf("ensure secret dir: %v", err)
	}
	info, err := os.Stat(secretsRoot)
	if err != nil {
		t.Fatalf("stat secrets root: %v", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("stat ownership unavailable for %s", secretsRoot)
	}
	if int(stat.Uid) != ownerUID {
		t.Fatalf("secrets root uid=%d want preserved uid %d", stat.Uid, ownerUID)
	}
}

func closedDone() <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
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

func testSecretReadersGID() int {
	gid := os.Getgid()
	if gid > 0 {
		return gid
	}
	return 1
}
