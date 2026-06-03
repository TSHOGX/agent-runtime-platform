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
