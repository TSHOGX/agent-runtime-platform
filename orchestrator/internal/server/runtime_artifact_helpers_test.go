package server

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/bridge"
	"harness-platform/orchestrator/internal/runtime"
	"harness-platform/orchestrator/internal/store"
)

func testGenerationArtifacts() runtime.GenerationArtifacts {
	return runtime.GenerationArtifacts{
		BundleDir:               "/tmp/bundle",
		SpecPath:                "/tmp/bundle/config.json",
		ManifestPath:            "/tmp/control/session.json",
		ManifestDigest:          "manifest_digest",
		ProjectedManifestDigest: "projected_manifest_digest",
		BundleDigest:            "bundle_digest",
		RuntimeConfigDigest:     "runtime_config_digest",
		SpecDigest:              "spec_digest",
		RunscVersion:            "runsc test",
		RunscBinaryPath:         "/usr/local/bin/runsc-test",
		RunscBinaryDigest:       "sha256:runsc-test",
	}
}

func serverRuntimeStartResult(req runtime.StartRequest) runtime.Result {
	if err := writeServerBridgeBootstrapForRequest(req); err != nil {
		return runtime.Result{Err: err}
	}
	return runtime.Result{
		ControlManifestDigest: req.PreparedArtifacts.ManifestDigest,
		RunscVersion:          req.PreparedArtifacts.RunscVersion,
		PostStartProof:        serverPostStartProofForRequest(req),
	}
}

func writeServerBridgeBootstrapForRequest(req runtime.StartRequest) error {
	if strings.TrimSpace(req.Generation.BridgeDirPath) == "" {
		return nil
	}
	if err := bridge.EnsureLayout(req.Generation.BridgeDirPath); err != nil {
		return err
	}
	if err := bridge.TouchHeartbeat(req.Generation.BridgeDirPath, bridge.BridgeHeartbeatFile, time.Now().UTC()); err != nil {
		return err
	}
	outbox, err := bridge.OpenQueue(req.Generation.BridgeDirPath, bridge.OutboxDir)
	if err != nil {
		return err
	}
	ctx := context.Background()
	helloPayload, err := json.Marshal(map[string]any{"driver_id": req.DriverID, "protocol_version": 2, "turn_input_schema": "RunTurn"})
	if err != nil {
		return err
	}
	for _, envelope := range []bridge.Envelope{
		{
			RequestID:    "test_heartbeat",
			Type:         bridge.TypeHeartbeat,
			SessionID:    req.SessionID,
			GenerationID: req.GenerationID,
		},
		{
			RequestID:    "test_hello",
			Type:         bridge.TypeHello,
			SessionID:    req.SessionID,
			GenerationID: req.GenerationID,
			Payload:      helloPayload,
		},
		{
			RequestID:    "test_probe",
			Type:         bridge.TypeProbeNetwork,
			SessionID:    req.SessionID,
			GenerationID: req.GenerationID,
		},
	} {
		if _, err := outbox.Write(ctx, envelope); err != nil {
			return err
		}
	}
	return nil
}

func serverPostStartProofForRequest(req runtime.StartRequest) *store.RuntimeResourcePostStartProof {
	containerID := strings.TrimSpace(req.Generation.RunscContainerID)
	if containerID == "" {
		containerID = "harness-gen-" + req.GenerationID
	}
	runscPlatform := strings.TrimSpace(req.Generation.RunscPlatform)
	if runscPlatform == "" {
		runscPlatform = "systrap"
	}
	runscVersion := strings.TrimSpace(req.Generation.RunscVersion)
	if runscVersion == "" {
		runscVersion = req.PreparedArtifacts.RunscVersion
	}
	runscBinaryPath := strings.TrimSpace(req.Generation.RunscBinaryPath)
	if runscBinaryPath == "" {
		runscBinaryPath = req.PreparedArtifacts.RunscBinaryPath
	}
	runscBinaryDigest := strings.TrimSpace(req.Generation.RunscBinaryDigest)
	if runscBinaryDigest == "" {
		runscBinaryDigest = req.PreparedArtifacts.RunscBinaryDigest
	}
	return &store.RuntimeResourcePostStartProof{
		GenerationID:      req.Generation.GenerationID,
		RunscContainerID:  containerID,
		RunscState:        "runsc_container:" + containerID + ":running; check=test",
		RunscPlatform:     runscPlatform,
		RunscVersion:      runscVersion,
		RunscBinaryPath:   runscBinaryPath,
		RunscBinaryDigest: runscBinaryDigest,
		IPNetns:           "netns:present; check=test",
		IPLink:            "host_veth:present; check=test",
		NFT:               "nft_table:present; check=test",
	}
}

func serverBridgeHelloPayload(t *testing.T, driverID string) json.RawMessage {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"driver_id":         driverID,
		"protocol_version":  2,
		"turn_input_schema": "RunTurn",
	})
	if err != nil {
		t.Fatalf("marshal bridge hello payload: %v", err)
	}
	return payload
}

func recordServerRuntimeArtifacts(t *testing.T, ctx context.Context, st *store.Store, generationID, manifestDigest, runscVersion string) {
	t.Helper()
	artifacts := testGenerationArtifacts()
	recordServerRuntimeArtifactsWithRunsc(t, ctx, st, generationID, manifestDigest, runscVersion, artifacts.RunscBinaryPath, artifacts.RunscBinaryDigest)
}

func recordServerRuntimeArtifactsWithRunsc(t *testing.T, ctx context.Context, st *store.Store, generationID, manifestDigest, runscVersion, runscPath, runscDigest string) {
	t.Helper()
	artifacts := testGenerationArtifacts()
	artifacts.ManifestDigest = manifestDigest
	artifacts.ProjectedManifestDigest = manifestDigest
	artifacts.RunscVersion = runscVersion
	artifacts.RunscBinaryPath = runscPath
	artifacts.RunscBinaryDigest = runscDigest
	if err := st.RecordGenerationRuntimeArtifactDigests(ctx, generationID, store.GenerationRuntimeArtifactDigests{
		ControlManifestDigest:          artifacts.ManifestDigest,
		ProjectedControlManifestDigest: artifacts.ProjectedManifestDigest,
		BundleDigest:                   artifacts.BundleDigest,
		RuntimeConfigDigest:            artifacts.RuntimeConfigDigest,
		SpecDigest:                     artifacts.SpecDigest,
		RunscVersion:                   artifacts.RunscVersion,
		RunscBinaryPath:                artifacts.RunscBinaryPath,
		RunscBinaryDigest:              artifacts.RunscBinaryDigest,
	}); err != nil {
		t.Fatalf("record runtime artifacts: %v", err)
	}
	var sessionID string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT session_id
FROM runtime_generations
WHERE generation_id = ?`, generationID).Scan(&sessionID); err != nil {
		t.Fatalf("query generation session: %v", err)
	}
	storeServerGenerationPlanForArtifacts(t, ctx, st, sessionID, generationID, artifacts)
}

func mutateServerRuntimeArtifactDigestMirrors(t *testing.T, ctx context.Context, st *store.Store, generationID string) {
	t.Helper()
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generation_resources
SET control_manifest_digest = 'mutated_manifest_digest',
    projected_control_manifest_digest = 'mutated_projected_manifest_digest',
    bundle_digest = 'mutated_bundle_digest',
    runtime_config_digest = 'mutated_runtime_config_digest',
    spec_digest = 'mutated_spec_digest'
WHERE generation_id = ?`, generationID); err != nil {
		t.Fatalf("mutate runtime artifact digest mirrors: %v", err)
	}
}

func currentRunscBinaryMetadataForServerTest(t *testing.T) (string, string) {
	t.Helper()
	path, err := exec.LookPath("runsc")
	if err != nil {
		t.Fatalf("lookup runsc binary: %v", err)
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatalf("resolve runsc binary %q: %v", path, err)
	}
	data, err := os.ReadFile(canonical)
	if err != nil {
		t.Fatalf("read runsc binary %q: %v", canonical, err)
	}
	sum := sha256.Sum256(data)
	return canonical, fmt.Sprintf("sha256:%x", sum[:])
}
