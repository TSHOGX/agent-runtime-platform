package runtime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"harness-platform/orchestrator/internal/store"
)

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

func TestResolveCheckpointPathRejectsNonCanonicalGenerationPath(t *testing.T) {
	dir := t.TempDir()
	generationPath := filepath.Join(dir, "run", "gen_a", "checkpoint")
	if err := os.MkdirAll(generationPath, 0o755); err != nil {
		t.Fatalf("create generation checkpoint: %v", err)
	}
	rt := New(Config{})

	_, err := rt.resolveCheckpointPath(StartRequest{
		SessionID: "sess_1",
		Generation: store.RuntimeGenerationDetails{
			CheckpointPath: filepath.Dir(generationPath) + string(filepath.Separator) + "same" + string(filepath.Separator) + ".." + string(filepath.Separator) + filepath.Base(generationPath),
		},
	})
	if err == nil || !strings.Contains(err.Error(), "checkpoint path") || !strings.Contains(err.Error(), "canonical absolute") {
		t.Fatalf("expected non-canonical checkpoint path error, got %v", err)
	}
}

func TestRuntimeStartRestoreRequiresCheckpointPath(t *testing.T) {
	rt := New(Config{})
	res := rt.Start(context.Background(), StartRequest{
		SessionID:             "sess_missing_checkpoint",
		DriverID:              "claude_code",
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
	rt := New(Config{})

	res := rt.Start(context.Background(), StartRequest{
		SessionID:    details.SessionID,
		GenerationID: details.GenerationID,
		DriverID:     "claude_code",
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
	rt := New(Config{CommandRunner: runner})
	details := testGenerationDetails(dir, "gen_restore_missing_artifacts")
	details.CheckpointPath = checkpointPath

	res := rt.Start(context.Background(), StartRequest{
		SessionID:             details.SessionID,
		GenerationID:          details.GenerationID,
		DriverID:              "claude_code",
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

func TestValidateCheckpointRestoreRejectsMetadataMismatch(t *testing.T) {
	dir := t.TempDir()
	checkpointPath := filepath.Join(dir, "checkpoint")
	imageManifestDigest := writeCheckpointFiles(t, checkpointPath)
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
	details.CheckpointImageManifestDigest = imageManifestDigest
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

func TestValidateCheckpointRestoreRejectsImageManifestDigestMismatch(t *testing.T) {
	dir := t.TempDir()
	checkpointPath := filepath.Join(dir, "checkpoint")
	writeCheckpointFiles(t, checkpointPath)
	details := testGenerationDetails(dir, "gen_restore_image_manifest")
	details.CheckpointNetworkProfileID = details.NetworkProfileID
	details.CheckpointAgentRuntimeProfileID = details.AgentRuntimeProfileID
	details.CheckpointRunscPlatform = details.RunscPlatform
	details.CheckpointRunscVersion = "runsc test"
	details.CheckpointRunscBinaryPath = "/usr/local/bin/runsc-test"
	details.CheckpointRunscBinaryDigest = "sha256:runsc-test"
	details.CheckpointBundleDigest = "bundle_digest"
	details.CheckpointRuntimeConfigDigest = "runtime_config_digest"
	details.CheckpointControlManifestDigest = "control_manifest_digest"
	details.CheckpointImageManifestDigest = "sha256:stale-checkpoint-image-manifest"
	artifacts := GenerationArtifacts{
		RunscVersion:            "runsc test",
		RunscBinaryPath:         "/usr/local/bin/runsc-test",
		RunscBinaryDigest:       "sha256:runsc-test",
		BundleDigest:            "bundle_digest",
		RuntimeConfigDigest:     "runtime_config_digest",
		ProjectedManifestDigest: "control_manifest_digest",
	}

	err := validateCheckpointRestore(details, artifacts, checkpointPath)
	if err == nil {
		t.Fatal("expected checkpoint image manifest digest mismatch")
	}
	if !strings.Contains(err.Error(), "checkpoint_image_manifest_digest") {
		t.Fatalf("expected image manifest digest mismatch, got %v", err)
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
	currentRunscPath, currentRunscDigest := installFakeRunsc(t, dir, "current")
	checkpointPath := filepath.Join(dir, "checkpoint")
	imageManifestDigest := writeCheckpointFiles(t, checkpointPath)
	runner := &recordingCommandRunner{
		outputs: map[string][]byte{
			"runsc --version": []byte("runsc current"),
		},
	}
	rt := New(Config{
		SessionsRoot:   filepath.Join(dir, "sessions"),
		AgentHomesRoot: filepath.Join(dir, "agent-homes"),
		RootFSPath:     filepath.Join(dir, "rootfs"),
		RunscNetwork:   "host",
		CommandRunner:  runner,
		SandboxUID:     testSandboxUID(),
		SandboxGID:     testSandboxGID(),
	})
	details := testGenerationDetails(dir, "gen_restore_mismatch")
	details.DriverID = "sh"
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
	details.CheckpointImageManifestDigest = imageManifestDigest
	details.RunscVersion = "runsc current"
	details.RunscBinaryPath = currentRunscPath
	details.RunscBinaryDigest = currentRunscDigest
	artifacts := restorePreparedArtifacts(details, "runsc current", currentRunscPath, currentRunscDigest)

	res := rt.Start(context.Background(), StartRequest{
		SessionID:             "sess_1",
		GenerationID:          details.GenerationID,
		DriverID:              "sh",
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

func TestRestoreGenerationArtifactsRejectsNonCanonicalArtifactPath(t *testing.T) {
	dir := t.TempDir()
	runscPath, runscDigest := installFakeRunsc(t, dir, "current")
	details := testGenerationDetails(dir, "gen_restore_artifact_path")
	artifacts := restorePreparedArtifacts(details, "runsc current", runscPath, runscDigest)
	artifacts.SpecPath = filepath.Dir(details.SpecPath) + string(filepath.Separator) + "same" + string(filepath.Separator) + ".." + string(filepath.Separator) + filepath.Base(details.SpecPath)

	_, err := restoreGenerationArtifacts(StartRequest{
		SessionID:             details.SessionID,
		GenerationID:          details.GenerationID,
		DriverID:              details.DriverID,
		RestoreFromCheckpoint: true,
		Generation:            details,
		PreparedArtifacts:     artifacts,
	})
	if err == nil || !strings.Contains(err.Error(), "restore artifact spec path must be canonical absolute") {
		t.Fatalf("expected restore artifact path rejection, got %v", err)
	}
}
