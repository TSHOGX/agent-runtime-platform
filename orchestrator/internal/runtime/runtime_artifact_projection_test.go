package runtime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/agents"
)

func TestRenderGenerationArtifactProjectionIsPure(t *testing.T) {
	dir := t.TempDir()
	runscPath, runscDigest := installFakeRunsc(t, dir, "render-generation-artifacts")
	runner := &recordingCommandRunner{
		outputs: map[string][]byte{"runsc --version": []byte("runsc render")},
	}
	rt := New(Config{
		SessionsRoot:         filepath.Join(dir, "sessions"),
		AgentHomesRoot:       filepath.Join(dir, "agent-homes"),
		BundleRoot:           filepath.Join(dir, "bundle", "out"),
		RootFSPath:           filepath.Join(dir, "rootfs"),
		BridgeMode:           "claim-loop",
		BridgeHeartbeat:      20 * time.Second,
		BridgePollInterval:   5 * time.Millisecond,
		ProbeHealthzStatuses: []int{200},
		CommandRunner:        runner,
	})
	details := testGenerationDetails(dir, "gen_render_projection")
	details.SessionID = "sess_pi_render"
	details.DriverID = string(agents.Pi)
	details.OutputFormat = agents.PiEventSchemaVersion
	details.NetworkHostsPath = filepath.Join(dir, "run", "network", "gen-"+details.GenerationID, "hosts")
	req := withDataVolumePathsForTest(dir, StartRequest{
		SessionID:    details.SessionID,
		GenerationID: details.GenerationID,
		DriverID:     string(agents.Pi),
		Generation:   details,
	})

	projection, err := rt.RenderGenerationArtifacts(context.Background(), req)
	if err != nil {
		t.Fatalf("render generation artifact projection: %v", err)
	}
	artifacts := projection.Artifacts
	if artifacts.BundleDir != details.BundleDirPath ||
		artifacts.SpecPath != details.SpecPath ||
		artifacts.ManifestPath != details.ControlManifestPath {
		t.Fatalf("unexpected artifact paths: %+v", artifacts)
	}
	if artifacts.RunscVersion != "runsc render" ||
		artifacts.RunscBinaryPath != runscPath ||
		artifacts.RunscBinaryDigest != runscDigest {
		t.Fatalf("unexpected runsc evidence: %+v", artifacts)
	}
	if artifacts.SpecDigest == "" ||
		artifacts.ManifestDigest == "" ||
		artifacts.ProjectedManifestDigest == "" ||
		artifacts.BundleDigest == "" ||
		artifacts.RuntimeConfigDigest == "" {
		t.Fatalf("projection missing artifact digests: %+v", artifacts)
	}
	if projection.NetworkHosts.Path != details.NetworkHostsPath ||
		string(projection.NetworkHosts.Payload) != "127.0.0.1 localhost\n::1 localhost ip6-localhost ip6-loopback\n10.200.1.1 harness-model-proxy.internal\n" {
		t.Fatalf("unexpected network hosts projection: %+v", projection.NetworkHosts)
	}
	if len(projection.DriverConfig.Entries) != 2 || len(artifacts.MaterializedDriverConfig) != 2 {
		t.Fatalf("unexpected driver config projection: projection=%+v artifacts=%+v", projection.DriverConfig, artifacts.MaterializedDriverConfig)
	}
	assertGenerationFilesystemMissing(t, generationFilesystemPaths(details))
	for _, entry := range projection.DriverConfig.Entries {
		if _, err := os.Stat(entry.HostSourcePath); !os.IsNotExist(err) {
			t.Fatalf("render should not write %s, stat err=%v", entry.HostSourcePath, err)
		}
	}

	materializeReq := req
	materializeReq.PreparedArtifacts = artifacts
	if err := rt.MaterializeGenerationArtifacts(materializeReq, projection); err != nil {
		t.Fatalf("materialize generation artifact projection: %v", err)
	}
	for _, path := range []string{details.SpecPath, details.ControlManifestPath, details.NetworkHostsPath} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("materialize should write %s: %v", path, err)
		}
	}
	for _, entry := range projection.DriverConfig.Entries {
		if _, err := os.Stat(entry.HostSourcePath); err != nil {
			t.Fatalf("materialize should write %s: %v", entry.HostSourcePath, err)
		}
	}
}

func TestMaterializeGenerationArtifactsRejectsProjectionDigestMismatch(t *testing.T) {
	dir := t.TempDir()
	installFakeRunsc(t, dir, "render-generation-artifacts")
	runner := &recordingCommandRunner{
		outputs: map[string][]byte{"runsc --version": []byte("runsc render")},
	}
	rt := New(Config{
		SessionsRoot:         filepath.Join(dir, "sessions"),
		AgentHomesRoot:       filepath.Join(dir, "agent-homes"),
		BundleRoot:           filepath.Join(dir, "bundle", "out"),
		RootFSPath:           filepath.Join(dir, "rootfs"),
		BridgeMode:           "claim-loop",
		BridgeHeartbeat:      20 * time.Second,
		BridgePollInterval:   5 * time.Millisecond,
		ProbeHealthzStatuses: []int{200},
		CommandRunner:        runner,
	})
	details := testGenerationDetails(dir, "gen_render_projection_mismatch")
	req := withDataVolumePathsForTest(dir, StartRequest{
		SessionID:    details.SessionID,
		GenerationID: details.GenerationID,
		DriverID:     details.DriverID,
		Generation:   details,
	})
	projection, err := rt.RenderGenerationArtifacts(context.Background(), req)
	if err != nil {
		t.Fatalf("render generation artifact projection: %v", err)
	}
	req.PreparedArtifacts = projection.Artifacts
	projection.RuntimeSpec.Hostname = "changed-hostname"

	err = rt.MaterializeGenerationArtifacts(req, projection)
	if err == nil || !strings.Contains(err.Error(), "materialization projection spec digest mismatch") {
		t.Fatalf("expected materialization projection digest mismatch, got %v", err)
	}
	assertGenerationFilesystemMissing(t, generationFilesystemPaths(details))
}

func TestMaterializeGenerationArtifactsRejectsNonCanonicalExpectedPath(t *testing.T) {
	dir := t.TempDir()
	installFakeRunsc(t, dir, "render-generation-artifacts")
	runner := &recordingCommandRunner{
		outputs: map[string][]byte{"runsc --version": []byte("runsc render")},
	}
	rt := New(Config{
		SessionsRoot:         filepath.Join(dir, "sessions"),
		AgentHomesRoot:       filepath.Join(dir, "agent-homes"),
		BundleRoot:           filepath.Join(dir, "bundle", "out"),
		RootFSPath:           filepath.Join(dir, "rootfs"),
		BridgeMode:           "claim-loop",
		BridgeHeartbeat:      20 * time.Second,
		BridgePollInterval:   5 * time.Millisecond,
		ProbeHealthzStatuses: []int{200},
		CommandRunner:        runner,
	})
	details := testGenerationDetails(dir, "gen_render_projection_path")
	req := withDataVolumePathsForTest(dir, StartRequest{
		SessionID:    details.SessionID,
		GenerationID: details.GenerationID,
		DriverID:     details.DriverID,
		Generation:   details,
	})
	projection, err := rt.RenderGenerationArtifacts(context.Background(), req)
	if err != nil {
		t.Fatalf("render generation artifact projection: %v", err)
	}
	req.PreparedArtifacts = projection.Artifacts
	req.PreparedArtifacts.SpecPath = filepath.Dir(details.SpecPath) + string(filepath.Separator) + "same" + string(filepath.Separator) + ".." + string(filepath.Separator) + filepath.Base(details.SpecPath)

	err = rt.MaterializeGenerationArtifacts(req, projection)
	if err == nil || !strings.Contains(err.Error(), "materialization projection expected spec path must be canonical absolute") {
		t.Fatalf("expected materialization projection path rejection, got %v", err)
	}
	assertGenerationFilesystemMissing(t, generationFilesystemPaths(details))
}
