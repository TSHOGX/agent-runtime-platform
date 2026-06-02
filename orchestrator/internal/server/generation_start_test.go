package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/bridge"
	"harness-platform/orchestrator/internal/config"
	"harness-platform/orchestrator/internal/events"
	"harness-platform/orchestrator/internal/sessionstate"
	"harness-platform/orchestrator/internal/store"
)

func TestStartEnsuredGenerationRenewsLeaseDuringSlowPrepare(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_slow_start", string(sessionstate.Created), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	cfg.Harness.Bridge.LeaseTTL = config.Duration{Duration: 200 * time.Millisecond}
	now := time.Now().UTC()
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  cfg.Harness.Bridge.LeaseTTL.Duration,
		Now:       now,
		Config:    serverTestAllocatorConfig(cfg, session.DriverID),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	rt := newBlockingPrepareRuntime()
	t.Cleanup(rt.release)
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.startEnsuredGeneration(ctx, session, ensuredGeneration{
			Allocation: allocation,
			IsNew:      true,
		}, startFailureInputAcceptable)
	}()

	select {
	case <-rt.prepareStarted:
	case <-time.After(time.Second):
		t.Fatalf("prepare did not start")
	}
	waitForGenerationLeaseAfter(t, ctx, st, allocation.GenerationID, allocation.LeaseExpiresAt)
	rt.release()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("start ensured generation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("start ensured generation did not finish")
	}
	waitForGenerationStatus(t, ctx, st, allocation.GenerationID, "idle")
	volumeConfig, err := srv.dataVolumeProvisionerConfig()
	if err != nil {
		t.Fatalf("data volume config: %v", err)
	}
	workspaceVolume, err := st.VerifySessionWorkspaceVolume(ctx, store.VerifySessionWorkspaceVolumeParams{
		SessionID: session.ID,
		Config:    volumeConfig,
	})
	if err != nil {
		t.Fatalf("verify workspace volume: %v", err)
	}
	driverHomeVolume, err := st.VerifySessionDriverHomeVolume(ctx, store.VerifySessionDriverHomeVolumeParams{
		SessionID: session.ID,
		Driver:    session.DriverID,
		Config:    volumeConfig,
	})
	if err != nil {
		t.Fatalf("verify driver home volume: %v", err)
	}
	prepares, starts := rt.requests()
	if len(prepares) != 2 || len(starts) != 1 {
		t.Fatalf("runtime requests prepare=%d start=%d", len(prepares), len(starts))
	}
	for _, prepare := range prepares {
		if prepare.WorkspaceHostPath != workspaceVolume.HostPath ||
			prepare.AgentHomeHostPath != driverHomeVolume.HostPath {
			t.Fatalf("runtime render did not receive data volume paths: prepare=%+v workspace=%+v home=%+v",
				prepare, workspaceVolume, driverHomeVolume)
		}
	}
	if starts[0].WorkspaceHostPath != workspaceVolume.HostPath ||
		starts[0].AgentHomeHostPath != driverHomeVolume.HostPath {
		t.Fatalf("runtime start did not receive data volume paths: start=%+v workspace=%+v home=%+v",
			starts[0], workspaceVolume, driverHomeVolume)
	}
	instance, err := st.GetRuntimeResourceInstance(ctx, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get runtime resource instance: %v", err)
	}
	if instance.State != store.RuntimeResourceLive ||
		instance.WorkerID != owner.UUID ||
		instance.RunscContainerID != serverRunscContainerID(t, ctx, st, session.ID, allocation.GenerationID) ||
		instance.RunscBinaryPath != "/usr/local/bin/runsc-test" ||
		instance.RunscBinaryDigest != "sha256:runsc-test" ||
		instance.NftTableName != mustRuntimeResourceNftTableName(t, allocation.GenerationID) {
		t.Fatalf("unexpected runtime resource instance: %+v", instance)
	}
	contract, err := st.GetSandboxContractForGeneration(ctx, session.ID, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get sandbox contract: %v", err)
	}
	if contract.SandboxContractVersion != store.SandboxContractVersion ||
		contract.ContractID != sandboxContractID(allocation.GenerationID) ||
		contract.ContractGateVersion != store.SandboxContractGateDriverManifest {
		t.Fatalf("unexpected sandbox contract: %+v", contract)
	}
	var payload map[string]any
	if err := json.Unmarshal(contract.CanonicalPayload, &payload); err != nil {
		t.Fatalf("decode sandbox contract payload: %v", err)
	}
	if payload["contract_gate_version"] != store.SandboxContractGateDriverManifest {
		t.Fatalf("sandbox contract gate version should be driver_manifest_v1: %s", contract.CanonicalPayload)
	}
	inputDigests, ok := payload["input_digests"].(map[string]any)
	if !ok {
		t.Fatalf("sandbox contract missing input digests: %s", contract.CanonicalPayload)
	}
	for _, key := range []string{"runtime_config_digest", "agent_manifest_digest"} {
		value, _ := inputDigests[key].(string)
		if !strings.HasPrefix(value, "sha256:") {
			t.Fatalf("sandbox contract missing %s: %s", key, contract.CanonicalPayload)
		}
	}
	evidence, err := st.GetSandboxContractInputEvidence(ctx, contract.ContractID)
	if err != nil {
		t.Fatalf("get sandbox contract input evidence: %v", err)
	}
	if evidence.RuntimeConfigDigest != inputDigests["runtime_config_digest"] ||
		evidence.AgentManifestDigest != inputDigests["agent_manifest_digest"] ||
		!json.Valid(evidence.RuntimeConfigPreimage) ||
		!json.Valid(evidence.AgentManifestPayload) {
		t.Fatalf("sandbox contract input evidence mismatch: evidence=%+v input=%+v", evidence, inputDigests)
	}
	if inputDigests["rootfs_image_digest"] != nil {
		t.Fatalf("rootfs digest should remain null until rootfs evidence is available: %s", contract.CanonicalPayload)
	}
	adapter, ok := payload["runtime_adapter"].(map[string]any)
	if !ok || adapter["runsc_container_id"] != serverRunscContainerID(t, ctx, st, session.ID, allocation.GenerationID) {
		t.Fatalf("sandbox contract missing runsc identity: %s", contract.CanonicalPayload)
	}
	if adapter["runsc_binary_path"] != "/usr/local/bin/runsc-test" ||
		adapter["runsc_binary_digest"] != "sha256:runsc-test" {
		t.Fatalf("sandbox contract missing runsc binary metadata: %s", contract.CanonicalPayload)
	}
	ambientCaps, ok := adapter["ambient_capabilities"].([]any)
	if adapter["no_new_privileges"] != true || !ok || len(ambientCaps) != 0 {
		t.Fatalf("sandbox contract missing runtime capability policy: %s", contract.CanonicalPayload)
	}
	forbiddenCaps, ok := adapter["forbidden_capabilities"].([]any)
	if !ok || !jsonArrayContainsAll(forbiddenCaps, "CAP_NET_ADMIN", "CAP_NET_RAW", "CAP_SYS_ADMIN") {
		t.Fatalf("sandbox contract missing forbidden capability policy: %s", contract.CanonicalPayload)
	}
	requiredAnnotations, ok := adapter["required_annotations"].(map[string]any)
	if !ok {
		t.Fatalf("sandbox contract missing required annotations: %s", contract.CanonicalPayload)
	}
	bridgeAnnotations, ok := requiredAnnotations[bridge.BridgeMountDestination].(map[string]any)
	if !ok ||
		bridgeAnnotations["dev.gvisor.spec.mount./harness-control/bridge.type"] != "bind" ||
		bridgeAnnotations["dev.gvisor.spec.mount./harness-control/bridge.share"] != "exclusive" {
		t.Fatalf("sandbox contract missing bridge required annotation policy: %s", contract.CanonicalPayload)
	}
	networkIdentity, ok := payload["network_identity"].(map[string]any)
	if !ok ||
		networkIdentity["sandbox_ip"] != instance.SandboxIP ||
		networkIdentity["nft_table_name"] != instance.NftTableName {
		t.Fatalf("sandbox contract missing runtime network identity: %s instance=%+v", contract.CanonicalPayload, instance)
	}
	resourceIdentity, ok := payload["resource_identity"].(map[string]any)
	if !ok || resourceIdentity["resource_identity_digest"] != instance.ResourceIdentityDigest {
		t.Fatalf("sandbox contract missing resource identity digest: %s instance=%+v", contract.CanonicalPayload, instance)
	}
	mountPlan, ok := payload["mount_plan"].(map[string]any)
	if !ok {
		t.Fatalf("sandbox contract missing mount plan: %s", contract.CanonicalPayload)
	}
	workspaceMount, ok := mountPlan["workspace"].(map[string]any)
	if !ok || workspaceMount["source"] != workspaceVolume.HostPath {
		t.Fatalf("sandbox contract workspace mount does not use data volume: %s", contract.CanonicalPayload)
	}
	agentHomeMount, ok := mountPlan["agent_home"].(map[string]any)
	if !ok || agentHomeMount["source"] != driverHomeVolume.HostPath {
		t.Fatalf("sandbox contract agent home mount does not use data volume: %s", contract.CanonicalPayload)
	}
	dataVolumes, ok := payload["data_volumes"].(map[string]any)
	if !ok {
		t.Fatalf("sandbox contract missing data volume ownership: %s", contract.CanonicalPayload)
	}
	workspacePayload, ok := dataVolumes["workspace"].(map[string]any)
	if !ok || workspacePayload["provisioning_marker_digest"] != workspaceVolume.ProvisioningMarkerDigest {
		t.Fatalf("sandbox contract workspace data volume evidence mismatch: %s", contract.CanonicalPayload)
	}
	driverHomePayload, ok := dataVolumes["agent_home"].(map[string]any)
	if !ok || driverHomePayload["provisioning_marker_digest"] != driverHomeVolume.ProvisioningMarkerDigest {
		t.Fatalf("sandbox contract driver home data volume evidence mismatch: %s", contract.CanonicalPayload)
	}
	var manifestDigest, specDigest, bundleDigest string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT control_manifest_digest, oci_spec_digest, bundle_digest
FROM sandbox_contract_artifacts
WHERE contract_id = ?`, contract.ContractID).Scan(&manifestDigest, &specDigest, &bundleDigest); err != nil {
		t.Fatalf("query sandbox contract artifacts: %v", err)
	}
	if manifestDigest != "manifest_digest" || specDigest != "spec_digest" || bundleDigest != "bundle_digest" {
		t.Fatalf("unexpected sandbox contract artifact digests: manifest=%s spec=%s bundle=%s", manifestDigest, specDigest, bundleDigest)
	}
}
