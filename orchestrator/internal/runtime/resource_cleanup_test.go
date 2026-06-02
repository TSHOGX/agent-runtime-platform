package runtime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"harness-platform/orchestrator/internal/store"
)

func TestDestroyGenerationResourcesDeletesPerGenerationNetwork(t *testing.T) {
	dir := t.TempDir()
	runscPath, runscDigest := installFakeRunsc(t, dir, "cleanup")
	runscRoot := filepath.Join(dir, "runsc-root")
	runner := &recordingCommandRunner{
		outputs: map[string][]byte{
			"runsc --version": []byte("runsc cleanup"),
		},
		fail: map[string]error{
			runscPath + " -root " + runscRoot + " state harness-gen-gen_a": errors.New("not found"),
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
	details.RunscVersion = "runsc cleanup"
	details.RunscBinaryPath = runscPath
	details.RunscBinaryDigest = runscDigest

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
		"runsc --version",
		runscPath + " -root " + filepath.Join(dir, "runsc-root") + " kill harness-gen-gen_a KILL",
		runscPath + " -root " + filepath.Join(dir, "runsc-root") + " delete -force harness-gen-gen_a",
		"nft delete table inet harness_gen_gen_a",
		"ip link delete hgenah",
		"ip netns delete harness-gen-a",
		runscPath + " -root " + filepath.Join(dir, "runsc-root") + " state harness-gen-gen_a",
		"ip netns list",
		"ip link show hgenah",
		"nft list table inet harness_gen_gen_a",
	}
	if got := runner.Commands(); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("unexpected commands:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestDestroyGenerationResourcesRejectsMissingRunscPin(t *testing.T) {
	dir := t.TempDir()
	runscRoot := filepath.Join(dir, "runsc-root")
	runner := &recordingCommandRunner{}
	rt := New(Config{
		RunscNetwork:  "host",
		RunscRoot:     runscRoot,
		RunDir:        filepath.Join(dir, "run"),
		CommandRunner: runner,
	})
	details := testGenerationDetails(dir, "gen_missing_pin")
	details.RunscNetwork = "host"

	cleanup, err := rt.DestroyGenerationResources(context.Background(), details)
	if err == nil {
		t.Fatal("expected missing runsc pin error")
	}
	if !strings.Contains(err.Error(), "runsc pin missing") || !strings.Contains(err.Error(), "runsc_version") {
		t.Fatalf("expected missing runsc pin error, got %v", err)
	}
	if cleanup.RunscDeleted {
		t.Fatalf("runsc deletion should fail without recorded pin: %+v", cleanup)
	}
	for _, command := range runner.Commands() {
		if strings.Contains(command, " -root "+runscRoot+" ") {
			t.Fatalf("runsc command executed despite missing pin: %v", runner.Commands())
		}
	}
}

func TestDestroyGenerationResourcesReturnsCurrentPinMismatchWhenCurrentDeleteFails(t *testing.T) {
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
	if err == nil {
		t.Fatal("expected current runsc pin mismatch")
	}
	if !strings.Contains(err.Error(), "current runsc pin mismatch") ||
		!strings.Contains(err.Error(), "incompatible runsc root") {
		t.Fatalf("expected current runsc mismatch delete error, got %v", err)
	}
	if cleanup.RunscDeleted {
		t.Fatalf("runsc deletion should fail on current pin mismatch: %+v", cleanup)
	}
	if !strings.Contains(cleanup.RunscPinEvidence, "runsc_pin:mismatch") ||
		!strings.Contains(cleanup.RunscPinEvidence, "cleanup_binary=current") {
		t.Fatalf("cleanup did not record runsc mismatch evidence: %+v", cleanup)
	}

	want := []string{
		"runsc --version",
		currentRunscPath + " -root " + runscRoot + " kill harness-gen-gen_pin KILL",
		currentRunscPath + " -root " + runscRoot + " delete -force harness-gen-gen_pin",
	}
	if got := runner.Commands(); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("unexpected commands:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	for _, command := range runner.Commands() {
		if strings.HasPrefix(command, oldRunscPath+" -root ") {
			t.Fatalf("recorded runsc must not execute, commands: %v", runner.Commands())
		}
	}
}

func TestDestroyGenerationResourcesDeletesFilesystemInNonSandboxMode(t *testing.T) {
	dir := t.TempDir()
	runscPath, runscDigest := installFakeRunsc(t, dir, "cleanup")
	runscRoot := filepath.Join(dir, "runsc-root")
	rt := New(Config{
		RunscNetwork: "host",
		RunscRoot:    runscRoot,
		RunDir:       filepath.Join(dir, "run"),
		CommandRunner: &recordingCommandRunner{
			outputs: map[string][]byte{
				"runsc --version": []byte("runsc cleanup"),
			},
			fail: map[string]error{
				runscPath + " -root " + runscRoot + " state harness-gen-gen_cleanup": errors.New("not found"),
			},
		},
	})
	details := testGenerationDetails(dir, "gen_cleanup")
	details.RunscNetwork = "host"
	details.NetworkHostsPath = filepath.Join(dir, "run", "network", "gen-"+details.GenerationID, "hosts")
	details.RunscVersion = "runsc cleanup"
	details.RunscBinaryPath = runscPath
	details.RunscBinaryDigest = runscDigest
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
	runscPath, runscDigest := installFakeRunsc(t, dir, "cleanup")
	runscRoot := filepath.Join(dir, "runsc-root")
	runner := &recordingCommandRunner{
		outputs: map[string][]byte{
			"runsc --version": []byte("runsc cleanup"),
		},
		fail: map[string]error{
			runscPath + " -root " + runscRoot + " state harness-gen-gen_missing_net": errors.New("not found"),
			"nft list table inet harness_gen_gen_missing_net":                        errors.New("No such table"),
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
	details.RunscVersion = "runsc cleanup"
	details.RunscBinaryPath = runscPath
	details.RunscBinaryDigest = runscDigest
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
		"runsc --version",
		runscPath + " -root " + filepath.Join(dir, "runsc-root") + " kill harness-gen-gen_missing_net KILL",
		runscPath + " -root " + filepath.Join(dir, "runsc-root") + " delete -force harness-gen-gen_missing_net",
		"nft delete table inet harness_gen_gen_missing_net",
		runscPath + " -root " + filepath.Join(dir, "runsc-root") + " state harness-gen-gen_missing_net",
		"nft list table inet harness_gen_gen_missing_net",
	}
	if got := runner.Commands(); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("unexpected commands:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}
