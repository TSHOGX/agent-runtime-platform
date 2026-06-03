package runtime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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

func TestRuntimeStartRejectsRunscPinMismatchBeforeRunscRun(t *testing.T) {
	dir := t.TempDir()
	runscPath, runscDigest := installFakeRunsc(t, dir, "current")
	runner := &recordingCommandRunner{
		outputs: map[string][]byte{
			"runsc --version": []byte("runsc current"),
		},
	}
	rt := New(Config{
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
		DriverID:          "claude_code",
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
	details.CheckpointImageManifestDigest = imageManifestDigest
	details.RunscVersion = "runsc current"
	details.RunscBinaryPath = runscPath
	details.RunscBinaryDigest = digest
	artifacts := restorePreparedArtifacts(details, "runsc current", runscPath, digest)

	res := rt.Start(context.Background(), StartRequest{
		SessionID:             details.SessionID,
		GenerationID:          details.GenerationID,
		DriverID:              "claude_code",
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

	_, err := rt.Checkpoint(context.Background(), CheckpointRequest{
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
