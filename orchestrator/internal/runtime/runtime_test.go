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

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/bridge"
	"harness-platform/orchestrator/internal/store"
)

func TestRuntimeStartRejectsUnsupportedAgent(t *testing.T) {
	rt := New(Config{DefaultAgent: "claude_code"})
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
		DefaultAgent:   "claude_code",
		SessionsRoot:   filepath.Join(t.TempDir(), "sessions"),
		AgentHomesRoot: filepath.Join(t.TempDir(), "agent-homes"),
		BundleRoot:     filepath.Join(t.TempDir(), "bundle", "out"),
		RunscNetwork:   "host",
	})
	res := rt.Start(context.Background(), StartRequest{
		SessionID: "sess_1",
		Agent:     "claude_code",
	}, nil)
	if res.Err == nil {
		t.Fatal("expected missing generation details error")
	}
	if !strings.Contains(res.Err.Error(), "generation details are required") {
		t.Fatalf("expected generation details error, got %v", res.Err)
	}
}

func TestResolveCheckpointPathUsesGenerationPath(t *testing.T) {
	dir := t.TempDir()
	generationPath := filepath.Join(dir, "run", "gen_a", "checkpoint")
	if err := os.MkdirAll(generationPath, 0o755); err != nil {
		t.Fatalf("create generation checkpoint: %v", err)
	}
	rt := New(Config{})

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

func TestResolveCheckpointPathRequiresGenerationPath(t *testing.T) {
	rt := New(Config{})

	_, err := rt.resolveCheckpointPath(StartRequest{
		SessionID: "sess_1",
	})
	if err == nil {
		t.Fatal("expected missing checkpoint path error")
	}
	if !strings.Contains(err.Error(), "checkpoint path is required") {
		t.Fatalf("expected checkpoint path error, got %v", err)
	}
}

func TestRuntimeStartRestoreRequiresCheckpointPath(t *testing.T) {
	rt := New(Config{DefaultAgent: "claude_code"})
	res := rt.Start(context.Background(), StartRequest{
		SessionID:             "sess_missing_checkpoint",
		Agent:                 "claude_code",
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

func TestRuntimeStartRequiresExplicitRestoreEvenWhenCheckpointExists(t *testing.T) {
	dir := t.TempDir()
	details := testGenerationDetails(dir, "gen_no_implicit_restore")
	writeCheckpointFiles(t, details.CheckpointPath)
	details.RunscPlatform = "ptrace"
	rt := New(Config{DefaultAgent: "claude_code"})

	res := rt.Start(context.Background(), StartRequest{
		SessionID:    details.SessionID,
		GenerationID: details.GenerationID,
		Agent:        "claude_code",
		Generation:   details,
	}, nil)
	if res.Err == nil {
		t.Fatal("expected cold start validation error")
	}
	if !strings.Contains(res.Err.Error(), `unsupported runsc platform "ptrace"`) {
		t.Fatalf("expected cold start validation error, got %v", res.Err)
	}
}

func TestRuntimeStartRestoreRequiresStoredArtifacts(t *testing.T) {
	dir := t.TempDir()
	checkpointPath := filepath.Join(dir, "checkpoint")
	writeCheckpointFiles(t, checkpointPath)
	runner := &recordingCommandRunner{}
	rt := New(Config{DefaultAgent: "claude_code", CommandRunner: runner})
	details := testGenerationDetails(dir, "gen_restore_missing_artifacts")
	details.CheckpointPath = checkpointPath

	res := rt.Start(context.Background(), StartRequest{
		SessionID:             details.SessionID,
		GenerationID:          details.GenerationID,
		Agent:                 "claude_code",
		RestoreFromCheckpoint: true,
		Generation:            details,
	}, nil)
	if res.Err == nil {
		t.Fatal("expected missing stored artifact error")
	}
	if !strings.Contains(res.Err.Error(), "restore requires stored generation artifact") {
		t.Fatalf("expected stored artifact error, got %v", res.Err)
	}
	if got := runner.Commands(); len(got) != 0 {
		t.Fatalf("restore should reject before runsc commands, got %v", got)
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
	data := mustReadFile(t, filepath.Join("testdata", "control-manifest-payload.phase8.json"))
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

func TestValidateCheckpointRestoreRejectsMetadataMismatch(t *testing.T) {
	dir := t.TempDir()
	checkpointPath := filepath.Join(dir, "checkpoint")
	writeCheckpointFiles(t, checkpointPath)
	details := testGenerationDetails(dir, "gen_restore")
	details.CheckpointNetworkProfileID = details.NetworkProfileID
	details.CheckpointAgentRuntimeProfileID = details.AgentRuntimeProfileID
	details.CheckpointRunscPlatform = details.RunscPlatform
	details.CheckpointRunscVersion = "runsc test"
	details.CheckpointRunscBinaryPath = "/usr/local/bin/runsc-test"
	details.CheckpointRunscBinaryDigest = "sha256:runsc-test"
	details.CheckpointBundleDigest = "bundle_digest"
	details.CheckpointRuntimeConfigDigest = "runtime_config_digest"
	details.CheckpointControlManifestDigest = "control_manifest_digest"
	artifacts := GenerationArtifacts{
		RunscVersion:            "runsc test",
		RunscBinaryPath:         "/usr/local/bin/runsc-test",
		RunscBinaryDigest:       "sha256:runsc-test",
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
		DefaultAgent:   "claude_code",
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
	details.CheckpointRunscBinaryPath = "runsc"
	details.CheckpointRunscBinaryDigest = "sha256:runsc"
	details.CheckpointBundleDigest = "bundle_digest"
	details.CheckpointRuntimeConfigDigest = "runtime_config_digest"
	details.CheckpointControlManifestDigest = "control_manifest_digest"
	currentRunscPath, currentRunscDigest := runscBinaryMetadata()
	artifacts := restorePreparedArtifacts(details, "runsc current", currentRunscPath, currentRunscDigest)

	res := rt.Start(context.Background(), StartRequest{
		SessionID:             "sess_1",
		GenerationID:          details.GenerationID,
		Agent:                 "sh",
		RestoreFromCheckpoint: true,
		Generation:            details,
		PreparedArtifacts:     artifacts,
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

func TestRuntimeStartRejectsRunscPinMismatchBeforeRunscRun(t *testing.T) {
	dir := t.TempDir()
	runscPath, runscDigest := installFakeRunsc(t, dir, "current")
	runner := &recordingCommandRunner{
		outputs: map[string][]byte{
			"runsc --version": []byte("runsc current"),
		},
	}
	rt := New(Config{
		DefaultAgent:  "claude_code",
		CommandRunner: runner,
	})
	details := testGenerationDetails(dir, "gen_start_pin_mismatch")
	details.RunscVersion = "runsc current"
	details.RunscBinaryPath = runscPath
	details.RunscBinaryDigest = runscDigest
	artifacts := GenerationArtifacts{
		BundleDir:               details.BundleDirPath,
		SpecPath:                details.SpecPath,
		ManifestPath:            details.ControlManifestPath,
		ManifestDigest:          "manifest_digest",
		ProjectedManifestDigest: "projected_manifest_digest",
		BundleDigest:            "bundle_digest",
		RuntimeConfigDigest:     "runtime_config_digest",
		SpecDigest:              "spec_digest",
		RunscVersion:            "runsc current",
		RunscBinaryPath:         runscPath,
		RunscBinaryDigest:       "sha256:stale",
	}

	res := rt.Start(context.Background(), StartRequest{
		SessionID:         details.SessionID,
		GenerationID:      details.GenerationID,
		Agent:             "claude_code",
		Generation:        details,
		PreparedArtifacts: artifacts,
	}, nil)
	if res.Err == nil {
		t.Fatal("expected runsc pin mismatch")
	}
	if !strings.Contains(res.Err.Error(), "fresh launch") || !strings.Contains(res.Err.Error(), "runsc_binary_digest") {
		t.Fatalf("expected fresh launch runsc digest mismatch, got %v", res.Err)
	}
	for _, command := range runner.Commands() {
		if strings.Contains(command, " run ") {
			t.Fatalf("runsc run executed despite pin mismatch: %v", runner.Commands())
		}
	}
}

func TestRuntimeStartRestoreRejectsRunscBinaryMismatchBeforeRunscRestore(t *testing.T) {
	dir := t.TempDir()
	runscPath, digest := installFakeRunsc(t, dir, "current")
	checkpointPath := filepath.Join(dir, "checkpoint")
	writeCheckpointFiles(t, checkpointPath)
	runner := &recordingCommandRunner{
		outputs: map[string][]byte{
			"runsc --version": []byte("runsc current"),
		},
	}
	rt := New(Config{
		DefaultAgent:   "claude_code",
		SessionsRoot:   filepath.Join(dir, "sessions"),
		AgentHomesRoot: filepath.Join(dir, "agent-homes"),
		RootFSPath:     filepath.Join(dir, "rootfs"),
		RunscNetwork:   "host",
		CommandRunner:  runner,
		SandboxUID:     testSandboxUID(),
		SandboxGID:     testSandboxGID(),
	})
	details := testGenerationDetails(dir, "gen_restore_pin_mismatch")
	details.CheckpointPath = checkpointPath
	details.CheckpointNetworkProfileID = details.NetworkProfileID
	details.CheckpointAgentRuntimeProfileID = details.AgentRuntimeProfileID
	details.CheckpointRunscPlatform = details.RunscPlatform
	details.CheckpointRunscVersion = "runsc current"
	details.CheckpointRunscBinaryPath = runscPath
	details.CheckpointRunscBinaryDigest = "sha256:stale"
	details.CheckpointBundleDigest = "bundle_digest"
	details.CheckpointRuntimeConfigDigest = "runtime_config_digest"
	details.CheckpointControlManifestDigest = "control_manifest_digest"
	artifacts := restorePreparedArtifacts(details, "runsc current", runscPath, digest)

	res := rt.Start(context.Background(), StartRequest{
		SessionID:             details.SessionID,
		GenerationID:          details.GenerationID,
		Agent:                 "claude_code",
		RestoreFromCheckpoint: true,
		Generation:            details,
		PreparedArtifacts:     artifacts,
	}, nil)
	if res.Err == nil {
		t.Fatal("expected restore runsc binary mismatch")
	}
	if !strings.Contains(res.Err.Error(), "checkpoint_runsc_binary_digest") {
		t.Fatalf("expected checkpoint runsc binary digest mismatch, got %v", res.Err)
	}
	for _, command := range runner.Commands() {
		if strings.Contains(command, " restore ") {
			t.Fatalf("runsc restore executed despite pin mismatch: %v", runner.Commands())
		}
	}
}

func TestCheckpointRejectsRunscPinMismatchBeforeFilesystemMutation(t *testing.T) {
	dir := t.TempDir()
	runscPath, _ := installFakeRunsc(t, dir, "current")
	runner := &recordingCommandRunner{
		outputs: map[string][]byte{
			"runsc --version": []byte("runsc current"),
		},
	}
	rt := New(Config{
		CommandRunner: runner,
	})
	details := testGenerationDetails(dir, "gen_checkpoint_pin_mismatch")
	details.RunscVersion = "runsc current"
	details.RunscBinaryPath = runscPath
	details.RunscBinaryDigest = "sha256:stale"
	rt.containers[details.SessionID] = &Container{
		SessionID:        details.SessionID,
		GenerationID:     details.GenerationID,
		RunscContainerID: details.RunscContainerID,
		Cancel:           func() {},
	}
	checkpointPath := filepath.Join(dir, "checkpoint", "image")
	if err := os.MkdirAll(checkpointPath, 0o755); err != nil {
		t.Fatalf("create checkpoint path: %v", err)
	}
	marker := filepath.Join(checkpointPath, "existing")
	if err := os.WriteFile(marker, []byte("keep"), 0o644); err != nil {
		t.Fatalf("write checkpoint marker: %v", err)
	}
	details.CheckpointPath = checkpointPath

	err := rt.Checkpoint(context.Background(), CheckpointRequest{
		SessionID:      details.SessionID,
		GenerationID:   details.GenerationID,
		CheckpointPath: checkpointPath,
		Generation:     details,
	})
	if err == nil {
		t.Fatal("expected checkpoint runsc pin mismatch")
	}
	if !strings.Contains(err.Error(), "checkpoint") || !strings.Contains(err.Error(), "runsc_binary_digest") {
		t.Fatalf("expected checkpoint runsc digest mismatch, got %v", err)
	}
	if _, statErr := os.Stat(marker); statErr != nil {
		t.Fatalf("checkpoint marker should remain after rejected checkpoint: %v", statErr)
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
	rt := New(Config{DefaultAgent: "claude_code"})
	stdin := &recordingWriteCloser{}
	canceled := make(chan struct{})
	rt.containers["sess_1"] = &Container{
		SessionID:        "sess_1",
		GenerationID:     "gen_old",
		RunscContainerID: "harness-gen-gen_old",
		Agent:            "claude_code",
		Stdin:            stdin,
		OutputHub:        NewOutputHub(),
		Cancel:           func() { close(canceled) },
	}

	res := rt.Start(context.Background(), StartRequest{
		SessionID:    "sess_1",
		GenerationID: "gen_new",
		Agent:        "claude_code",
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

func TestCheckpointRequiresGenerationIdentity(t *testing.T) {
	rt := New(Config{})
	rt.containers["sess_1"] = &Container{SessionID: "sess_1", GenerationID: "gen_a", RunscContainerID: "harness-gen-gen_a"}

	err := rt.Checkpoint(context.Background(), CheckpointRequest{SessionID: "sess_1"})
	if err == nil || !strings.Contains(err.Error(), "generation id is required") {
		t.Fatalf("expected missing generation id error, got %v", err)
	}
	err = rt.Checkpoint(context.Background(), CheckpointRequest{SessionID: "sess_1", GenerationID: "gen_b"})
	if err == nil || !strings.Contains(err.Error(), "container generation mismatch") {
		t.Fatalf("expected generation mismatch error, got %v", err)
	}
	err = rt.Checkpoint(context.Background(), CheckpointRequest{
		SessionID:    "sess_1",
		GenerationID: "gen_a",
		Generation:   store.RuntimeGenerationDetails{SessionID: "sess_other", GenerationID: "gen_a"},
	})
	if err == nil || !strings.Contains(err.Error(), "checkpoint generation session mismatch") {
		t.Fatalf("expected generation session mismatch error, got %v", err)
	}
	err = rt.Checkpoint(context.Background(), CheckpointRequest{
		SessionID:    "sess_1",
		GenerationID: "gen_a",
		Generation:   store.RuntimeGenerationDetails{SessionID: "sess_1", GenerationID: "gen_a", RunscContainerID: "harness-gen-other"},
	})
	if err == nil || !strings.Contains(err.Error(), "checkpoint runsc container mismatch") {
		t.Fatalf("expected runsc container mismatch error, got %v", err)
	}
}

func TestCheckpointRequiresGenerationScopedPath(t *testing.T) {
	dir := t.TempDir()
	runscPath, runscDigest := installFakeRunsc(t, dir, "checkpoint-path")
	runner := &recordingCommandRunner{
		outputs: map[string][]byte{
			"runsc --version": []byte("runsc checkpoint-path"),
		},
	}
	rt := New(Config{
		CommandRunner: runner,
	})
	details := testGenerationDetails(dir, "gen_checkpoint_path")
	details.RunscVersion = "runsc checkpoint-path"
	details.RunscBinaryPath = runscPath
	details.RunscBinaryDigest = runscDigest
	details.CheckpointPath = ""
	rt.containers[details.SessionID] = &Container{
		SessionID:        details.SessionID,
		GenerationID:     details.GenerationID,
		RunscContainerID: details.RunscContainerID,
		Cancel:           func() {},
	}

	err := rt.Checkpoint(context.Background(), CheckpointRequest{
		SessionID:    details.SessionID,
		GenerationID: details.GenerationID,
		Generation:   details,
	})
	if err == nil || !strings.Contains(err.Error(), "generation checkpoint path is required") {
		t.Fatalf("expected missing generation checkpoint path error, got %v", err)
	}

	details.CheckpointPath = filepath.Join(dir, "run", "gen-"+details.GenerationID, "checkpoint")
	err = rt.Checkpoint(context.Background(), CheckpointRequest{
		SessionID:      details.SessionID,
		GenerationID:   details.GenerationID,
		CheckpointPath: filepath.Join(dir, "run", "gen-"+details.GenerationID, "checkpoint-other"),
		Generation:     details,
	})
	if err == nil || !strings.Contains(err.Error(), "checkpoint path mismatch") {
		t.Fatalf("expected checkpoint path mismatch error, got %v", err)
	}
}

func TestRuntimeStartReusesExistingGenerationWithoutStdinTurn(t *testing.T) {
	rt := New(Config{
		DefaultAgent: "claude_code",
		RunscNetwork: "sandbox",
	})
	hub := NewOutputHub()
	stdin := &recordingWriteCloser{}
	container := &Container{
		SessionID:        "sess_1",
		GenerationID:     "gen_a",
		RunscContainerID: "harness-gen-gen_a",
		Agent:            "claude_code",
		Stdin:            stdin,
		OutputHub:        hub,
	}
	rt.containers["sess_1"] = container

	outputs := 0
	res := rt.Start(context.Background(), StartRequest{
		SessionID:    "sess_1",
		GenerationID: "gen_a",
		Agent:        "claude_code",
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
	dir := t.TempDir()
	runscRoot := filepath.Join(dir, "runsc-root")
	runner := &recordingCommandRunner{
		fail: map[string]error{
			"runsc -root " + runscRoot + " state harness-gen-gen_a": errors.New("not found"),
			"ip link show hgenah":                   errors.New("does not exist"),
			"nft list table inet harness_gen_gen_a": errors.New("No such table"),
		},
	}
	rt := New(Config{
		RunscNetwork:  "sandbox",
		RunscOverlay2: "none",
		RunscRoot:     runscRoot,
		RunDir:        filepath.Join(dir, "run"),
		CommandRunner: runner,
	})
	details := testGenerationDetails(dir, "gen_a")
	details.RunscNetwork = "sandbox"
	details.NetnsName = "harness-gen-a"
	details.HostVeth = "hgenah"

	cleanup, err := rt.DestroyGenerationResources(context.Background(), details)
	if err != nil {
		t.Fatalf("destroy generation resources: %v", err)
	}
	if !cleanup.RunscDeleted || !cleanup.NftTableDeleted || !cleanup.HostVethDeleted || !cleanup.NetnsDeleted {
		t.Fatalf("unexpected cleanup result: %+v", cleanup)
	}
	if cleanup.RunscState == "" || cleanup.IPNetns == "" || cleanup.IPLink == "" || cleanup.NFT == "" || len(cleanup.FilesystemLstat) == 0 {
		t.Fatalf("cleanup did not record absence evidence: %+v", cleanup)
	}

	want := []string{
		"runsc -root " + filepath.Join(dir, "runsc-root") + " kill harness-gen-gen_a KILL",
		"runsc -root " + filepath.Join(dir, "runsc-root") + " delete -force harness-gen-gen_a",
		"nft delete table inet harness_gen_gen_a",
		"ip link delete hgenah",
		"ip netns delete harness-gen-a",
		"runsc -root " + filepath.Join(dir, "runsc-root") + " state harness-gen-gen_a",
		"ip netns list",
		"ip link show hgenah",
		"nft list table inet harness_gen_gen_a",
	}
	if got := runner.Commands(); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("unexpected commands:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestDestroyGenerationResourcesFallsBackToRecordedRunscOnPinMismatch(t *testing.T) {
	dir := t.TempDir()
	oldRunscPath, oldRunscDigest := installFakeRunsc(t, filepath.Join(dir, "old-runsc"), "old")
	currentRunscPath, _ := installFakeRunsc(t, filepath.Join(dir, "current-runsc"), "current")
	runscRoot := filepath.Join(dir, "runsc-root")
	runner := &recordingCommandRunner{
		outputs: map[string][]byte{
			"runsc --version": []byte("runsc current"),
		},
		fail: map[string]error{
			currentRunscPath + " -root " + runscRoot + " delete -force harness-gen-gen_pin": errors.New("incompatible runsc root"),
			oldRunscPath + " -root " + runscRoot + " state harness-gen-gen_pin":             errors.New("not found"),
		},
	}
	rt := New(Config{
		RunscNetwork:  "host",
		RunscRoot:     runscRoot,
		RunDir:        filepath.Join(dir, "run"),
		CommandRunner: runner,
	})
	details := testGenerationDetails(dir, "gen_pin")
	details.RunscNetwork = "host"
	details.RunscVersion = "runsc old"
	details.RunscBinaryPath = oldRunscPath
	details.RunscBinaryDigest = oldRunscDigest

	cleanup, err := rt.DestroyGenerationResources(context.Background(), details)
	if err != nil {
		t.Fatalf("destroy generation resources: %v", err)
	}
	if !cleanup.RunscDeleted {
		t.Fatalf("expected runsc deletion through recorded binary: %+v", cleanup)
	}
	if !strings.Contains(cleanup.RunscPinEvidence, "runsc_pin:mismatch") ||
		!strings.Contains(cleanup.RunscPinEvidence, "cleanup_binary=recorded") ||
		!strings.Contains(cleanup.RunscState, "cleanup_binary=recorded") {
		t.Fatalf("cleanup did not record runsc mismatch evidence: %+v", cleanup)
	}

	want := []string{
		"runsc --version",
		currentRunscPath + " -root " + runscRoot + " kill harness-gen-gen_pin KILL",
		currentRunscPath + " -root " + runscRoot + " delete -force harness-gen-gen_pin",
		oldRunscPath + " -root " + runscRoot + " kill harness-gen-gen_pin KILL",
		oldRunscPath + " -root " + runscRoot + " delete -force harness-gen-gen_pin",
		oldRunscPath + " -root " + runscRoot + " state harness-gen-gen_pin",
	}
	if got := runner.Commands(); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("unexpected commands:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestDestroyGenerationResourcesRejectsRecordedRunscDigestMismatch(t *testing.T) {
	dir := t.TempDir()
	oldRunscPath, _ := installFakeRunsc(t, filepath.Join(dir, "old-runsc"), "old")
	currentRunscPath, _ := installFakeRunsc(t, filepath.Join(dir, "current-runsc"), "current")
	runscRoot := filepath.Join(dir, "runsc-root")
	runner := &recordingCommandRunner{
		outputs: map[string][]byte{
			"runsc --version": []byte("runsc current"),
		},
		fail: map[string]error{
			currentRunscPath + " -root " + runscRoot + " delete -force harness-gen-gen_pin_bad": errors.New("incompatible runsc root"),
		},
	}
	rt := New(Config{
		RunscNetwork:  "host",
		RunscRoot:     runscRoot,
		RunDir:        filepath.Join(dir, "run"),
		CommandRunner: runner,
	})
	details := testGenerationDetails(dir, "gen_pin_bad")
	details.RunscNetwork = "host"
	details.RunscVersion = "runsc old"
	details.RunscBinaryPath = oldRunscPath
	details.RunscBinaryDigest = "sha256:stale"

	cleanup, err := rt.DestroyGenerationResources(context.Background(), details)
	if err == nil {
		t.Fatal("expected recorded runsc digest mismatch")
	}
	if !strings.Contains(err.Error(), "recorded runsc binary digest") {
		t.Fatalf("expected recorded runsc digest mismatch, got %v", err)
	}
	if !strings.Contains(cleanup.RunscPinEvidence, "runsc_pin:mismatch") {
		t.Fatalf("cleanup did not retain mismatch evidence: %+v", cleanup)
	}
	for _, command := range runner.Commands() {
		if strings.HasPrefix(command, oldRunscPath+" -root ") {
			t.Fatalf("recorded runsc with bad digest must not execute, commands: %v", runner.Commands())
		}
	}
}

func TestDestroyGenerationResourcesDeletesFilesystemInNonSandboxMode(t *testing.T) {
	dir := t.TempDir()
	runscRoot := filepath.Join(dir, "runsc-root")
	rt := New(Config{
		RunscNetwork: "host",
		RunscRoot:    runscRoot,
		RunDir:       filepath.Join(dir, "run"),
		CommandRunner: &recordingCommandRunner{fail: map[string]error{
			"runsc -root " + runscRoot + " state harness-gen-gen_cleanup": errors.New("not found"),
		}},
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
			name: "outside runtime root",
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
				RunscNetwork:  "host",
				RunDir:        filepath.Join(dir, "run"),
				CommandRunner: &recordingCommandRunner{},
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

func TestDestroyGenerationResourcesCleansFilesystemWithIncompleteSandboxMetadata(t *testing.T) {
	dir := t.TempDir()
	runscRoot := filepath.Join(dir, "runsc-root")
	runner := &recordingCommandRunner{
		fail: map[string]error{
			"runsc -root " + runscRoot + " state harness-gen-gen_missing_net": errors.New("not found"),
			"nft list table inet harness_gen_gen_missing_net":                 errors.New("No such table"),
		},
	}
	rt := New(Config{
		RunscNetwork:  "sandbox",
		RunscRoot:     runscRoot,
		RunDir:        filepath.Join(dir, "run"),
		CommandRunner: runner,
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
	if !cleanup.RunscDeleted || !cleanup.NftTableDeleted || cleanup.HostVethDeleted || cleanup.NetnsDeleted {
		t.Fatalf("unexpected network cleanup result with missing metadata: %+v", cleanup)
	}
	assertGenerationFilesystemMissing(t, generationFilesystemPaths(details))

	want := []string{
		"runsc -root " + filepath.Join(dir, "runsc-root") + " kill harness-gen-gen_missing_net KILL",
		"runsc -root " + filepath.Join(dir, "runsc-root") + " delete -force harness-gen-gen_missing_net",
		"nft delete table inet harness_gen_gen_missing_net",
		"runsc -root " + filepath.Join(dir, "runsc-root") + " state harness-gen-gen_missing_net",
		"nft list table inet harness_gen_gen_missing_net",
	}
	if got := runner.Commands(); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("unexpected commands:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestRuntimePostStartProofRecordsContainerAndNetworkEvidence(t *testing.T) {
	dir := t.TempDir()
	runscRoot := filepath.Join(dir, "runsc-root")
	details := testGenerationDetails(dir, "gen_post_start")
	details.RunscNetwork = "sandbox"
	details.NetnsName = "harness-gen-post-start"
	details.HostVeth = "hgenpsh"
	pin := runscPin{
		Platform:     "systrap",
		Version:      "runsc proof",
		BinaryPath:   "/usr/local/bin/runsc-proof",
		BinaryDigest: "sha256:runsc-proof",
	}
	containerID := details.RunscContainerID
	runner := &recordingCommandRunner{
		outputs: map[string][]byte{
			pin.BinaryPath + " -root " + runscRoot + " state " + containerID: []byte(`{"id":"` + containerID + `","status":"running"}`),
			"ip netns list":                                          []byte(details.NetnsName + "\n"),
			"ip link show " + details.HostVeth:                       []byte("42: " + details.HostVeth + ": <BROADCAST,UP>"),
			"nft list table inet " + generationNftTableName(details): []byte("table inet " + generationNftTableName(details)),
		},
	}
	rt := New(Config{
		RunscRoot:     runscRoot,
		RunscNetwork:  "sandbox",
		CommandRunner: runner,
	})

	proof, err := rt.runtimePostStartProof(context.Background(), details, pin, containerID)
	if err != nil {
		t.Fatalf("runtime post-start proof: %v", err)
	}
	if proof.GenerationID != details.GenerationID ||
		proof.RunscContainerID != containerID ||
		proof.RunscPlatform != pin.Platform ||
		proof.RunscVersion != pin.Version ||
		proof.RunscBinaryPath != pin.BinaryPath ||
		proof.RunscBinaryDigest != pin.BinaryDigest {
		t.Fatalf("post-start proof missing runsc identity: %+v", proof)
	}
	for label, value := range map[string]string{
		"runsc":  proof.RunscState,
		"netns":  proof.IPNetns,
		"iplink": proof.IPLink,
		"nft":    proof.NFT,
	} {
		if strings.TrimSpace(value) == "" {
			t.Fatalf("post-start proof missing %s evidence: %+v", label, proof)
		}
	}
	if !strings.Contains(proof.RunscState, containerID) || !strings.Contains(proof.RunscState, "running") {
		t.Fatalf("post-start proof did not record running container: %+v", proof)
	}
}

func TestRunscRunningEvidenceRetriesTransientStateMiss(t *testing.T) {
	dir := t.TempDir()
	runscRoot := filepath.Join(dir, "runsc-root")
	containerID := "harness-gen-gen_retry"
	runscBinary := "/usr/local/bin/runsc-proof"
	command := runscBinary + " -root " + runscRoot + " state " + containerID
	runner := &recordingCommandRunner{
		sequence: map[string][]commandResult{
			command: {
				{out: []byte("FetchSpec failed: loading container: file does not exist"), err: errors.New("exit status 128")},
				{out: []byte(`{"id":"` + containerID + `","status":"running"}`)},
			},
		},
	}
	rt := New(Config{
		RunscRoot:     runscRoot,
		CommandRunner: runner,
	})

	evidence, err := rt.runscContainerRunningEvidence(context.Background(), runscBinary, containerID)
	if err != nil {
		t.Fatalf("runsc running evidence: %v", err)
	}
	if !strings.Contains(evidence, containerID) || !strings.Contains(evidence, "running") {
		t.Fatalf("unexpected evidence: %s", evidence)
	}
	if got := runner.Commands(); len(got) != 2 || got[0] != command || got[1] != command {
		t.Fatalf("unexpected retry commands: %v", got)
	}
}

func TestDestroyTreatsMissingRunscContainerAsAbsent(t *testing.T) {
	runner := &recordingCommandRunner{
		sequence: map[string][]commandResult{
			"runsc -root /runsc delete -force phase3-missing": {
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
		"runsc -root /runsc delete -force phase3-missing",
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
	spec, _, err := rt.renderRuntimeSpec(withDataVolumePathsForTest(dir, StartRequest{
		SessionID:    "sess_1",
		GenerationID: details.GenerationID,
		Agent:        "claude_code",
		Generation:   details,
	}))
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
		SessionsRoot:       filepath.Join(dir, "sessions"),
		AgentHomesRoot:     filepath.Join(dir, "agent-homes"),
		BundleRoot:         filepath.Join(dir, "bundle", "out"),
		RootFSPath:         filepath.Join(dir, "rootfs"),
		BridgeHeartbeat:    20 * time.Second,
		BridgePollInterval: 5 * time.Millisecond,
	})
	details := testGenerationDetails(dir, "gen_a")

	workspacePath, agentHomePath := dataVolumePathsForTest(dir, "sess_1", "claude_code")
	artifacts, err := rt.PrepareGeneration(context.Background(), withDataVolumePathsForTest(dir, StartRequest{
		SessionID:    "sess_1",
		GenerationID: details.GenerationID,
		Agent:        "claude_code",
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
	if strings.Contains(string(specData), "phase2-template") {
		t.Fatalf("runtime spec hot path must not reference phase2-template: %s", specData)
	}
	if strings.Contains(string(specData), "/harness-secrets") ||
		strings.Contains(string(specData), "anthropic_api_key") ||
		strings.Contains(string(specData), "anthropic_auth_token") {
		t.Fatalf("runtime spec must not contain legacy secret references: %s", specData)
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
			t.Fatalf("runtime spec must not include legacy secret env %s: %+v", key, env)
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

func TestRuntimeBridgeMetadataComesFromDriverSpec(t *testing.T) {
	dir := t.TempDir()
	rt := New(Config{
		SessionsRoot:   filepath.Join(dir, "sessions"),
		AgentHomesRoot: filepath.Join(dir, "agent-homes"),
		BundleRoot:     filepath.Join(dir, "bundle", "out"),
		RootFSPath:     filepath.Join(dir, "rootfs"),
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
		Agent:        "claude_code",
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
		SessionsRoot:       filepath.Join(dir, "sessions"),
		AgentHomesRoot:     filepath.Join(dir, "agent-homes"),
		BundleRoot:         filepath.Join(dir, "bundle", "out"),
		RootFSPath:         filepath.Join(dir, "rootfs"),
		BridgeHeartbeat:    20 * time.Second,
		BridgePollInterval: 5 * time.Millisecond,
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
		Agent:        "claude_code",
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

func TestPreparePiGenerationMaterializesReadOnlyConfig(t *testing.T) {
	dir := t.TempDir()
	rt := New(Config{
		SessionsRoot:       filepath.Join(dir, "sessions"),
		AgentHomesRoot:     filepath.Join(dir, "agent-homes"),
		BundleRoot:         filepath.Join(dir, "bundle", "out"),
		RootFSPath:         filepath.Join(dir, "rootfs"),
		BridgeHeartbeat:    20 * time.Second,
		BridgePollInterval: 5 * time.Millisecond,
	})
	details := testGenerationDetails(dir, "gen_pi_config")
	details.SessionID = "sess_pi"
	details.Agent = "pi"
	details.OutputFormat = "pi_rpc_events_v1.0"
	details.Model = "sonnet"
	details.ManifestAnthropicBaseURL = "http://harness-model-proxy.internal:8082"

	artifacts, err := rt.PrepareGeneration(context.Background(), withDataVolumePathsForTest(dir, StartRequest{
		SessionID:    "sess_pi",
		GenerationID: details.GenerationID,
		Agent:        "pi",
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
		t.Fatalf("pi models config must not use legacy top-level models array: %+v", models)
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

func TestWriteDriverConfigProjectionReturnsNilWithoutSpecsOrRenderer(t *testing.T) {
	dir := t.TempDir()
	rt := New(Config{})
	details := testGenerationDetails(dir, "gen_shell_no_driver_config")
	details.Agent = "sh"

	entries, err := rt.writeDriverConfigProjection(StartRequest{
		SessionID:    details.SessionID,
		GenerationID: details.GenerationID,
		Agent:        "sh",
		Generation:   details,
	})
	if err != nil {
		t.Fatalf("write shell driver config projection: %v", err)
	}
	if entries != nil {
		t.Fatalf("shell driver config projection = %+v, want nil", entries)
	}
}

func TestDriverConfigProjectionRenderersMatchMaterializationSpecs(t *testing.T) {
	driversWithSpecs := map[agents.ID]struct{}{}
	for _, driver := range agents.AllDriverSpecs() {
		specs := agents.DriverConfigMaterializationSpecsFor(driver.ID)
		if len(specs) == 0 {
			continue
		}
		driversWithSpecs[driver.ID] = struct{}{}

		renderer, ok := driverConfigProjectionRenderers[driver.ID]
		if !ok {
			t.Errorf("%s driver has config materialization specs but no renderer", driver.ID)
			continue
		}

		details := testGenerationDetails(t.TempDir(), "gen_"+string(driver.ID)+"_config_renderer")
		details.Agent = string(driver.ID)
		payloads, err := renderer(details)
		if err != nil {
			t.Errorf("%s driver config renderer failed for baseline generation details: %v", driver.ID, err)
			continue
		}
		for _, spec := range specs {
			if _, ok := payloads[spec.Name]; !ok {
				t.Errorf("%s driver config renderer missing %q payload for materialization spec", driver.ID, spec.Name)
			}
		}
	}

	for driver := range driverConfigProjectionRenderers {
		if _, ok := driversWithSpecs[driver]; !ok {
			t.Errorf("%s driver has config renderer but no materialization specs", driver)
		}
	}
}

func TestWriteDriverConfigProjectionFailsClosedWhenSpecsHaveNoRenderer(t *testing.T) {
	dir := t.TempDir()
	rt := New(Config{})
	details := testGenerationDetails(dir, "gen_pi_missing_renderer")
	details.SessionID = "sess_pi_missing_renderer"
	details.Agent = "pi"

	renderer, ok := driverConfigProjectionRenderers[agents.Pi]
	if !ok {
		t.Fatal("pi driver config projection renderer is not registered")
	}
	delete(driverConfigProjectionRenderers, agents.Pi)
	t.Cleanup(func() {
		driverConfigProjectionRenderers[agents.Pi] = renderer
	})

	_, err := rt.writeDriverConfigProjection(StartRequest{
		SessionID:    details.SessionID,
		GenerationID: details.GenerationID,
		Agent:        "pi",
		Generation:   details,
	})
	if err == nil {
		t.Fatal("expected missing renderer error")
	}
	if !strings.Contains(err.Error(), "pi driver config projection renderer is missing") {
		t.Fatalf("expected missing pi renderer error, got %v", err)
	}
}

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

func TestPrepareGenerationConcurrentSessionsUseDistinctControlManifests(t *testing.T) {
	dir := t.TempDir()
	rt := New(Config{
		SessionsRoot:   filepath.Join(dir, "sessions"),
		AgentHomesRoot: filepath.Join(dir, "agent-homes"),
		BundleRoot:     filepath.Join(dir, "bundle", "out"),
		RootFSPath:     filepath.Join(dir, "rootfs"),
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
				Agent:        "claude_code",
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

	workspacePath, agentHomePath := dataVolumePathsForTest(dir, "sess_shell", "sh")
	if _, err := rt.PrepareGeneration(context.Background(), withDataVolumePathsForTest(dir, StartRequest{
		SessionID:    "sess_shell",
		GenerationID: details.GenerationID,
		Agent:        "sh",
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
		SessionsRoot:   filepath.Join(dir, "legacy-sessions"),
		AgentHomesRoot: filepath.Join(dir, "legacy-agent-homes"),
		BundleRoot:     filepath.Join(dir, "bundle", "out"),
		RootFSPath:     filepath.Join(dir, "rootfs"),
		RunscNetwork:   "host",
		SandboxUID:     testSandboxUID(),
		SandboxGID:     testSandboxGID(),
	})
	details := testGenerationDetails(dir, "gen_data_volume_paths")
	details.SessionID = "sess_data_volume_paths"
	details.Agent = "sh"
	details.OutputFormat = "shell_pty"
	details.RequiresSecretDrop = false
	details.ManifestAnthropicBaseURL = ""
	workspacePath := filepath.Join(dir, "volumes", "workspaces", details.SessionID)
	agentHomePath := filepath.Join(dir, "volumes", "driver-homes", details.SessionID, "sh")

	if _, err := rt.PrepareGeneration(context.Background(), StartRequest{
		SessionID:         details.SessionID,
		GenerationID:      details.GenerationID,
		Agent:             "sh",
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
		SessionsRoot:   filepath.Join(dir, "legacy-sessions"),
		AgentHomesRoot: filepath.Join(dir, "legacy-agent-homes"),
		BundleRoot:     filepath.Join(dir, "bundle", "out"),
		RootFSPath:     filepath.Join(dir, "rootfs"),
		RunscNetwork:   "host",
		SandboxUID:     testSandboxUID(),
		SandboxGID:     testSandboxGID(),
	})
	details := testGenerationDetails(dir, "gen_missing_data_volume_paths")
	details.SessionID = "sess_missing_data_volume_paths"
	details.Agent = "sh"
	details.OutputFormat = "shell_pty"
	details.RequiresSecretDrop = false
	details.ManifestAnthropicBaseURL = ""

	_, err := rt.PrepareGeneration(context.Background(), StartRequest{
		SessionID:    details.SessionID,
		GenerationID: details.GenerationID,
		Agent:        "sh",
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
			details.Agent = tc.agent
			details.OutputFormat = tc.outputFormat
			details.RequiresSecretDrop = true
			details.SecretsDirPath = filepath.Join(dir, "run", "control", "gen-"+details.GenerationID, "secrets")
			details.AnthropicAPIKeySecretID = "anthropic_api_key"
			details.AnthropicAuthTokenSecretID = "anthropic_auth_token"
			details.SecretVersion = "local"

			_, err := rt.PrepareGeneration(context.Background(), StartRequest{
				SessionID:    tc.sessionID,
				GenerationID: details.GenerationID,
				Agent:        tc.agent,
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
		Agent:        "claude_code",
		Generation:   details,
	})
	if err == nil {
		t.Fatal("expected identity mismatch error")
	}
	if !strings.Contains(err.Error(), "generation session mismatch") {
		t.Fatalf("expected generation session mismatch, got %v", err)
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
		NftTableName:               hostEgressTableName(generationID),
		EgressPolicyDigest:         "egress_digest",
		Agent:                      "claude_code",
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
	if table := generationNftTableName(details); table != "" {
		values = append(values, table)
	}
	return values
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
