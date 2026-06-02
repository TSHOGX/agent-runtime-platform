package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/events"
	"harness-platform/orchestrator/internal/planprojection"
	"harness-platform/orchestrator/internal/sessionstate"
	"harness-platform/orchestrator/internal/store"
)

func TestExistingStartVerifiesStoredGenerationPlanEvidence(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_plan_verify_start", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, "claude_code"),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	rt := &recordingRuntime{}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)
	if err := srv.startEnsuredGeneration(ctx, session, ensuredGeneration{Allocation: allocation, IsNew: true}, startFailureInputAcceptable); err != nil {
		t.Fatalf("initial start: %v", err)
	}
	if err := st.UpdateSessionStatusAndActivity(ctx, session.ID, string(sessionstate.RunningIdle), nil, time.Now().UTC()); err != nil {
		t.Fatalf("mark session idle: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'idle'
WHERE generation_id = ?`, allocation.GenerationID); err != nil {
		t.Fatalf("mark generation idle: %v", err)
	}
	plan, err := st.GetGenerationPlan(ctx, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get generation plan: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(plan.CanonicalPayload, &payload); err != nil {
		t.Fatalf("decode generation plan: %v", err)
	}
	payload["runsc_pin"].(map[string]any)["binary_digest"] = "sha256:changed-runsc"
	canonical, err := store.CanonicalGenerationPlanPayload(payload)
	if err != nil {
		t.Fatalf("canonical corrupt plan: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE generation_plans
SET canonical_payload = ?,
    plan_digest = ?
WHERE generation_id = ?`, string(canonical), store.GenerationPlanDigest(canonical), allocation.GenerationID); err != nil {
		t.Fatalf("corrupt stored plan: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE generation_plan_projections
SET plan_digest = ?
WHERE generation_id = ?`, store.GenerationPlanDigest(canonical), allocation.GenerationID); err != nil {
		t.Fatalf("align corrupt plan projection digests: %v", err)
	}
	rt = &recordingRuntime{}
	srv = &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"hello"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)
	if rec.Code != http.StatusInternalServerError ||
		!strings.Contains(rec.Body.String(), "generation plan runsc pin mismatch") {
		t.Fatalf("expected frozen evidence failure, got status %d body %s", rec.Code, rec.Body.String())
	}
	_, starts := rt.requests()
	if len(starts) != 0 {
		t.Fatalf("runtime start should not run after frozen evidence mismatch: %+v", starts)
	}
}

func TestFreshStartStoresGenerationPlanBeforeMaterializationAndNetworkPrepare(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_plan_before_network", string(sessionstate.Created), time.Now().UTC(), nil)
	rt := &planOrderRuntime{store: st, t: t}
	srv := &Server{
		cfg:     testServerConfig(dir),
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"hello"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d body %s", rec.Code, rec.Body.String())
	}
	if !rt.planSeenBeforeMaterializeRender {
		t.Fatalf("runtime artifact materialization render ran before generation plan rows were stored")
	}
	if !rt.planSeenBeforeMaterialize {
		t.Fatalf("runtime artifact materialization ran before generation plan rows were stored")
	}
	if !rt.projectionVerificationBeforeMaterialize {
		t.Fatalf("runtime artifact materialization ran before generation plan projections were verified")
	}
	if !rt.runtimeResourceClaimedBeforeMaterialize {
		t.Fatalf("runtime artifact materialization ran before claiming the runtime resource")
	}
	if !rt.planSeenBeforeNetwork {
		t.Fatalf("network preparation ran before generation plan rows were stored")
	}
	if !rt.projectionVerificationObserved {
		t.Fatalf("network preparation ran before generation plan projections were verified")
	}
	if !rt.runtimeResourceClaimedBeforeNetwork {
		t.Fatalf("network preparation ran before claiming the runtime resource")
	}
}

func TestFreshStartReverifiesStoredGenerationPlanProjectionsBeforeMaterialization(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_projection_reverify_materialize", string(sessionstate.Created), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, session.DriverID),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	rt := &corruptProjectionBeforeMaterializeRuntime{store: st, t: t}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	err = srv.startEnsuredGeneration(ctx, session, ensuredGeneration{Allocation: allocation, IsNew: true}, startFailureInputAcceptable)
	if err == nil || !strings.Contains(err.Error(), "generation plan projection bundle digest mismatch") {
		t.Fatalf("expected pre-materialization projection mismatch, got %v", err)
	}
	if !rt.corrupted {
		t.Fatalf("test runtime did not corrupt stored projection row")
	}
	if rt.materialized {
		t.Fatalf("materialization should not run after stored projection mismatch")
	}
}

func TestVerifyStoredGenerationPlanProjectionsChecksExistingRows(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, _ := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_projection_verify", string(sessionstate.Created), time.Now().UTC(), nil)
	generationID := "gen_projection_verify"
	if _, err := st.DBForTest().ExecContext(ctx, `
INSERT INTO runtime_generations (generation_id, session_id, status, lease_owner, lease_expires_at)
VALUES (?, ?, 'allocating', 'owner', ?)`, generationID, session.ID, time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("insert runtime generation: %v", err)
	}
	plan, err := st.StoreGenerationPlan(ctx, store.StoreGenerationPlanParams{
		GenerationID: generationID,
		Payload:      map[string]any{"generation_id": generationID, "plan_version": 1},
	})
	if err != nil {
		t.Fatalf("store generation plan: %v", err)
	}
	artifacts := testGenerationArtifacts()
	details := store.RuntimeGenerationDetails{
		GenerationID:        generationID,
		ControlManifestPath: artifacts.ManifestPath,
		SpecPath:            artifacts.SpecPath,
		BundleDirPath:       artifacts.BundleDir,
	}
	for _, expectation := range planprojection.ExpectationsForDetails(details, artifacts) {
		if _, err := st.StoreGenerationPlanProjection(ctx, store.StoreGenerationPlanProjectionParams{
			GenerationID:      generationID,
			PlanDigest:        plan.PlanDigest,
			ProjectionKind:    expectation.ProjectionKind,
			ProjectionVersion: 1,
			PayloadDigest:     expectation.PayloadDigest,
			MaterializedPath:  expectation.MaterializedPath,
		}); err != nil {
			t.Fatalf("store projection %s: %v", expectation.ProjectionKind, err)
		}
	}
	if _, err := st.StoreGenerationPlanProjection(ctx, store.StoreGenerationPlanProjectionParams{
		GenerationID:      generationID,
		PlanDigest:        plan.PlanDigest,
		ProjectionKind:    store.GenerationPlanProjectionSandboxContract,
		ProjectionVersion: store.GenerationPlanProjectionVersion,
		PayloadDigest:     "sha256:sandbox-contract",
	}); err != nil {
		t.Fatalf("store sandbox contract projection: %v", err)
	}

	srv := &Server{store: st}
	verified, err := srv.verifyStoredGenerationPlanProjections(ctx, details, artifacts, "sha256:sandbox-contract")
	if err != nil {
		t.Fatalf("verify matching projections: %v", err)
	}
	if !verified {
		t.Fatalf("expected existing plan projections to verify")
	}
	mismatch := artifacts
	mismatch.SpecDigest = "changed_spec_digest"
	if _, err := srv.verifyStoredGenerationPlanProjections(ctx, details, mismatch, "sha256:sandbox-contract"); err == nil ||
		!strings.Contains(err.Error(), "oci_spec digest mismatch") {
		t.Fatalf("expected projection mismatch, got %v", err)
	}
	if _, err := srv.verifyStoredGenerationPlanProjections(ctx, details, artifacts, "sha256:changed-contract"); err == nil ||
		!strings.Contains(err.Error(), "sandbox_contract digest mismatch") {
		t.Fatalf("expected sandbox contract projection mismatch, got %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE generation_plan_projections
SET materialized_path = ?
WHERE generation_id = ?
  AND projection_kind = ?`, "/tmp/changed-config.json", generationID, store.GenerationPlanProjectionOCISpec); err != nil {
		t.Fatalf("corrupt projection path: %v", err)
	}
	if _, err := srv.verifyStoredGenerationPlanProjections(ctx, details, artifacts, "sha256:sandbox-contract"); err == nil ||
		!strings.Contains(err.Error(), "oci_spec materialized path mismatch") {
		t.Fatalf("expected projection materialized path mismatch, got %v", err)
	}
}

func TestVerifyStoredGenerationPlanProjectionsChecksProjectionVersion(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, _ := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_projection_version", string(sessionstate.Created), time.Now().UTC(), nil)
	generationID := "gen_projection_version"
	if _, err := st.DBForTest().ExecContext(ctx, `
INSERT INTO runtime_generations (generation_id, session_id, status, lease_owner, lease_expires_at)
VALUES (?, ?, 'allocating', 'owner', ?)`, generationID, session.ID, time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("insert runtime generation: %v", err)
	}
	plan, err := st.StoreGenerationPlan(ctx, store.StoreGenerationPlanParams{
		GenerationID: generationID,
		Payload:      map[string]any{"generation_id": generationID, "plan_version": 1},
	})
	if err != nil {
		t.Fatalf("store generation plan: %v", err)
	}
	artifacts := testGenerationArtifacts()
	for _, expectation := range planprojection.Expectations(artifacts) {
		if _, err := st.StoreGenerationPlanProjection(ctx, store.StoreGenerationPlanProjectionParams{
			GenerationID:      generationID,
			PlanDigest:        plan.PlanDigest,
			ProjectionKind:    expectation.ProjectionKind,
			ProjectionVersion: expectation.ProjectionVersion,
			PayloadDigest:     expectation.PayloadDigest,
		}); err != nil {
			t.Fatalf("store projection %s: %v", expectation.ProjectionKind, err)
		}
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE generation_plan_projections
SET projection_version = 2
WHERE generation_id = ?
  AND projection_kind = ?`, generationID, store.GenerationPlanProjectionOCISpec); err != nil {
		t.Fatalf("corrupt stored projection version: %v", err)
	}

	srv := &Server{store: st}
	if _, err := srv.verifyStoredGenerationPlanProjections(ctx, store.RuntimeGenerationDetails{GenerationID: generationID}, artifacts, ""); err == nil ||
		!strings.Contains(err.Error(), "generation plan projection oci_spec version = 2, want 1") {
		t.Fatalf("expected projection version mismatch, got %v", err)
	}
}

func TestGenerationPlanProjectionExpectationsIncludesSandboxContractWhenProvided(t *testing.T) {
	withoutContract := generationPlanProjectionExpectations(testGenerationArtifacts(), "")
	for _, expectation := range withoutContract {
		if expectation.ProjectionKind == store.GenerationPlanProjectionSandboxContract {
			t.Fatalf("empty contract digest should not add sandbox contract expectation: %+v", withoutContract)
		}
	}

	withContract := generationPlanProjectionExpectations(testGenerationArtifacts(), "sha256:sandbox-contract")
	if len(withContract) != len(withoutContract)+1 {
		t.Fatalf("expectation count = %d want %d", len(withContract), len(withoutContract)+1)
	}
	first := withContract[0]
	if first.ProjectionKind != store.GenerationPlanProjectionSandboxContract ||
		first.ProjectionVersion != store.GenerationPlanProjectionVersion ||
		first.PayloadDigest != "sha256:sandbox-contract" {
		t.Fatalf("sandbox contract expectation = %+v", first)
	}
}

func TestVerifyStoredGenerationPlanProjectionsRequiresPlan(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, _ := openServerOwnedStore(t, ctx, dir)
	srv := &Server{store: st}
	_, err := srv.verifyStoredGenerationPlanProjections(ctx, store.RuntimeGenerationDetails{GenerationID: "missing_plan_generation"}, testGenerationArtifacts(), "")
	if err == nil || !strings.Contains(err.Error(), "generation plan is required") {
		t.Fatalf("expected required missing plan error, got %v", err)
	}
}

func TestVerifyGenerationPlanFrozenEvidenceChecksExistingPlan(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, _ := openServerOwnedStore(t, ctx, dir)
	srv := &Server{store: st}
	storeServerFrozenEvidencePlan(t, ctx, st, dir, validServerGenerationPlanPayload())

	details := serverGenerationPlanFrozenEvidenceDetails()
	artifacts := serverGenerationPlanFrozenEvidenceArtifacts()
	if err := srv.verifyGenerationPlanFrozenEvidence(ctx, "gen_frozen_evidence", details, artifacts); err != nil {
		t.Fatalf("verify frozen evidence: %v", err)
	}
	artifacts.RunscBinaryDigest = "sha256:changed"
	if err := srv.verifyGenerationPlanFrozenEvidence(ctx, "gen_frozen_evidence", details, artifacts); err == nil ||
		!strings.Contains(err.Error(), "runsc pin mismatch") {
		t.Fatalf("expected runsc mismatch, got %v", err)
	}
	details = serverGenerationPlanFrozenEvidenceDetails()
	details.CheckpointPlanDigest = "sha256:changed"
	artifacts = serverGenerationPlanFrozenEvidenceArtifacts()
	if err := srv.verifyGenerationPlanFrozenEvidence(ctx, "gen_frozen_evidence", details, artifacts); err == nil ||
		!strings.Contains(err.Error(), "checkpoint plan digest mismatch") {
		t.Fatalf("expected checkpoint plan digest mismatch, got %v", err)
	}
	details = serverGenerationPlanFrozenEvidenceDetails()
	details.CheckpointDriverStatesDigest = ""
	details.CheckpointPlanDigest = store.GenerationPlanDigest(storeServerFrozenEvidenceCanonicalPayload(t))
	artifacts = serverGenerationPlanFrozenEvidenceArtifacts()
	if err := srv.verifyGenerationPlanFrozenEvidence(ctx, "gen_frozen_evidence", details, artifacts); err == nil ||
		!strings.Contains(err.Error(), "checkpoint driver-state digest is required") {
		t.Fatalf("expected checkpoint driver-state fence error, got %v", err)
	}
}

func TestVerifyGenerationPlanDataVolumesChecksStoredPlan(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, _ := openServerOwnedStore(t, ctx, dir)
	srv := &Server{store: st}
	storeServerFrozenEvidencePlan(t, ctx, st, dir, validServerGenerationPlanPayload())

	volumes := sessionRuntimeDataVolumes{
		Workspace: store.SessionWorkspaceVolume{
			HostPath:              "/var/lib/harness/sessions/sess_frozen_evidence",
			RuntimeIdentityDigest: "sha256:identity",
		},
		DriverHome: store.SessionDriverHomeVolume{
			HostPath:              "/var/lib/harness/agent-homes/sess_frozen_evidence/claude_code",
			RuntimeIdentityDigest: "sha256:identity",
		},
	}
	if err := srv.verifyGenerationPlanDataVolumes(ctx, "gen_frozen_evidence", volumes); err != nil {
		t.Fatalf("verify data volumes: %v", err)
	}

	volumes.Workspace.HostPath = "/var/lib/harness/sessions/changed"
	if err := srv.verifyGenerationPlanDataVolumes(ctx, "gen_frozen_evidence", volumes); err == nil ||
		!strings.Contains(err.Error(), "data_volumes.workspace.host_path mismatch") {
		t.Fatalf("expected workspace host path mismatch, got %v", err)
	}
}

func TestVerifyGenerationPlanNetworkEvidenceChecksStoredPlan(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, _ := openServerOwnedStore(t, ctx, dir)
	srv := &Server{store: st}
	payload := validServerGenerationPlanPayload()
	network := payload["network"].(map[string]any)
	network["proxy_port"] = 8080
	network["nft_table_name"] = mustRuntimeResourceNftTableName(t, "gen_frozen_evidence")
	storeServerFrozenEvidencePlan(t, ctx, st, dir, payload)

	details := store.RuntimeGenerationDetails{
		GenerationID:       "gen_frozen_evidence",
		NetworkProfileID:   "net_gen_frozen_evidence",
		RunscNetwork:       "sandbox",
		RunscOverlay2:      "none",
		HostProxyBindURL:   "http://127.0.0.1:8080",
		ProxyPort:          8080,
		HostGatewayIP:      "10.240.0.1",
		SandboxBaseURL:     "http://10.240.0.1:8080",
		NetnsName:          "harness-gen-frozen",
		NetnsPath:          "/var/run/netns/harness-gen-frozen",
		HostVeth:           "vh-frozen",
		SandboxVeth:        "vs-frozen",
		SandboxIPCIDR:      "10.240.0.2/30",
		HostSideCIDR:       "10.240.0.1/30",
		EgressPolicyID:     "egress_frozen",
		EgressPolicyDigest: "egress_digest",
		DNSPolicy:          "off",
	}
	if err := srv.verifyGenerationPlanNetworkEvidence(ctx, "gen_frozen_evidence", details); err != nil {
		t.Fatalf("verify network evidence: %v", err)
	}

	details.HostVeth = "changed-veth"
	if err := srv.verifyGenerationPlanNetworkEvidence(ctx, "gen_frozen_evidence", details); err == nil ||
		!strings.Contains(err.Error(), "network.host_veth mismatch") {
		t.Fatalf("expected host veth mismatch, got %v", err)
	}
}

func TestVerifyGenerationPlanRuntimeArtifactPathsChecksStoredPlan(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, _ := openServerOwnedStore(t, ctx, dir)
	srv := &Server{store: st}
	storeServerFrozenEvidencePlan(t, ctx, st, dir, validServerGenerationPlanPayload())

	details := store.RuntimeGenerationDetails{
		ControlDirPath:      "/var/lib/harness/run/control/gen_frozen_evidence",
		ControlManifestPath: "/var/lib/harness/run/control/gen_frozen_evidence/session.json",
		BundleDirPath:       "/var/lib/harness/run/runtime/gen_frozen_evidence",
		SpecPath:            "/var/lib/harness/run/runtime/gen_frozen_evidence/config.json",
		BridgeDirPath:       "/var/lib/harness/run/bridge/gen_frozen_evidence",
		LogDirPath:          "/var/lib/harness/logs/gen_frozen_evidence",
	}
	if err := srv.verifyGenerationPlanRuntimeArtifactPaths(ctx, "gen_frozen_evidence", details); err != nil {
		t.Fatalf("verify runtime artifact paths: %v", err)
	}

	details.SpecPath = "/var/lib/harness/run/runtime/changed/config.json"
	if err := srv.verifyGenerationPlanRuntimeArtifactPaths(ctx, "gen_frozen_evidence", details); err == nil ||
		!strings.Contains(err.Error(), "runtime_artifacts.spec_path mismatch") {
		t.Fatalf("expected spec path mismatch, got %v", err)
	}
}

func TestVerifyGenerationPlanMountPlanEvidenceChecksStoredPlan(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, _ := openServerOwnedStore(t, ctx, dir)
	srv := &Server{store: st}
	storeServerFrozenEvidencePlan(t, ctx, st, dir, validServerGenerationPlanPayload())

	details := store.RuntimeGenerationDetails{
		GenerationID:   "gen_frozen_evidence",
		DriverID:       "claude_code",
		ControlDirPath: "/var/lib/harness/run/control/gen_frozen_evidence",
		BridgeDirPath:  "/var/lib/harness/run/bridge/gen_frozen_evidence",
	}
	volumes := sessionRuntimeDataVolumes{
		Workspace: store.SessionWorkspaceVolume{
			HostPath: "/var/lib/harness/sessions/sess_frozen_evidence",
		},
		DriverHome: store.SessionDriverHomeVolume{
			HostPath: "/var/lib/harness/agent-homes/sess_frozen_evidence/claude_code",
		},
	}
	if err := srv.verifyGenerationPlanMountPlanEvidence(ctx, "gen_frozen_evidence", details, volumes, nil); err != nil {
		t.Fatalf("verify mount plan evidence: %v", err)
	}

	volumes.Workspace.HostPath = "/var/lib/harness/sessions/changed"
	if err := srv.verifyGenerationPlanMountPlanEvidence(ctx, "gen_frozen_evidence", details, volumes, nil); err == nil ||
		!strings.Contains(err.Error(), "mounts.workspace.source mismatch") {
		t.Fatalf("expected workspace mount mismatch, got %v", err)
	}
}

func TestVerifyGenerationPlanRuntimeResourceEvidenceChecksStoredPlan(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, _ := openServerOwnedStore(t, ctx, dir)
	srv := &Server{store: st}
	storeServerFrozenEvidencePlan(t, ctx, st, dir, validServerGenerationPlanPayload())

	if err := srv.verifyGenerationPlanRuntimeResourceEvidence(ctx, "gen_frozen_evidence", "sha256:resource"); err != nil {
		t.Fatalf("verify runtime resource evidence: %v", err)
	}
	if err := srv.verifyGenerationPlanRuntimeResourceEvidence(ctx, "gen_frozen_evidence", "sha256:changed"); err == nil ||
		!strings.Contains(err.Error(), "runtime_artifacts.resource_identity_digest mismatch") {
		t.Fatalf("expected resource identity mismatch, got %v", err)
	}
}

func TestVerifyGenerationPlanSourceDigestEvidenceChecksStoredInputEvidence(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, _ := openServerOwnedStore(t, ctx, dir)
	srv := &Server{store: st}
	plan := storeServerFrozenEvidencePlan(t, ctx, st, dir, validServerGenerationPlanPayload())
	storeServerSyntheticSandboxContractParentForPlan(t, ctx, st, plan)
	storeServerSandboxContractInputEvidenceFromPlan(t, ctx, st, plan)

	if err := srv.verifyGenerationPlanSourceDigestEvidence(ctx, "sess_frozen_evidence", "gen_frozen_evidence"); err != nil {
		t.Fatalf("verify source digest evidence: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE sandbox_contract_input_evidence
SET agent_manifest_digest = 'sha256:changed'
WHERE contract_id = ?`, sandboxContractID("gen_frozen_evidence")); err != nil {
		t.Fatalf("mutate sandbox contract input evidence: %v", err)
	}
	if err := srv.verifyGenerationPlanSourceDigestEvidence(ctx, "sess_frozen_evidence", "gen_frozen_evidence"); err == nil ||
		!strings.Contains(err.Error(), "source_digests.agent_manifest_digest mismatch") {
		t.Fatalf("expected agent manifest source digest mismatch, got %v", err)
	}
}

func TestVerifyGenerationPlanSandboxContractEvidenceChecksStoredRows(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_sandbox_contract_verify", string(sessionstate.Created), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, session.DriverID),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	rt := &recordingRuntime{}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)
	if err := srv.startEnsuredGeneration(ctx, session, ensuredGeneration{Allocation: allocation, IsNew: true}, startFailureInputAcceptable); err != nil {
		t.Fatalf("start generation: %v", err)
	}
	if err := srv.verifyGenerationPlanSandboxContractEvidence(ctx, allocation.GenerationID, session.ID); err != nil {
		t.Fatalf("verify sandbox contract evidence: %v", err)
	}

	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE generation_plan_projections
SET payload_digest = 'sha256:changed'
WHERE generation_id = ?
  AND projection_kind = ?`, allocation.GenerationID, store.GenerationPlanProjectionSandboxContract); err != nil {
		t.Fatalf("corrupt sandbox contract projection: %v", err)
	}
	if err := srv.verifyGenerationPlanSandboxContractEvidence(ctx, allocation.GenerationID, session.ID); err == nil ||
		!strings.Contains(err.Error(), "sandbox_contract projection digest mismatch") {
		t.Fatalf("expected sandbox contract projection mismatch, got %v", err)
	}
}

func TestVerifyGenerationPlanFrozenEvidenceChecksStoredProjectionRows(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, _ := openServerOwnedStore(t, ctx, dir)
	srv := &Server{store: st}
	storeServerFrozenEvidencePlan(t, ctx, st, dir, validServerGenerationPlanPayload())
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE generation_plan_projections
SET payload_digest = 'sha256:changed'
WHERE generation_id = ?
  AND projection_kind = ?`, "gen_frozen_evidence", store.GenerationPlanProjectionBundle); err != nil {
		t.Fatalf("corrupt stored projection row: %v", err)
	}

	err := srv.verifyGenerationPlanFrozenEvidence(ctx, "gen_frozen_evidence", serverGenerationPlanFrozenEvidenceDetails(), serverGenerationPlanFrozenEvidenceArtifacts())
	if err == nil || !strings.Contains(err.Error(), "generation plan checkpoint bundle digest mismatch") {
		t.Fatalf("expected stored projection row checkpoint mismatch, got %v", err)
	}
}

func TestVerifyGenerationPlanFrozenEvidenceUsesStoredProjectionRows(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, _ := openServerOwnedStore(t, ctx, dir)
	srv := &Server{store: st}
	storeServerFrozenEvidencePlan(t, ctx, st, dir, validServerGenerationPlanPayload())

	artifacts := serverGenerationPlanFrozenEvidenceArtifacts()
	artifacts.ManifestDigest = "sha256:mutated-control-manifest-row"
	artifacts.ProjectedManifestDigest = "sha256:mutated-projected-manifest-row"
	artifacts.BundleDigest = "sha256:mutated-bundle-row"
	artifacts.RuntimeConfigDigest = "sha256:mutated-runtime-config-row"
	artifacts.SpecDigest = "sha256:mutated-spec-row"

	if err := srv.verifyGenerationPlanFrozenEvidence(ctx, "gen_frozen_evidence", serverGenerationPlanFrozenEvidenceDetails(), artifacts); err != nil {
		t.Fatalf("verify frozen evidence from stored projection rows: %v", err)
	}
}

func TestVerifyGenerationPlanFrozenEvidenceChecksPlanIdentity(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, _ := openServerOwnedStore(t, ctx, dir)
	srv := &Server{store: st}
	storeServerFrozenEvidencePlan(t, ctx, st, dir, validServerGenerationPlanPayload())
	plan, err := st.GetGenerationPlan(ctx, "gen_frozen_evidence")
	if err != nil {
		t.Fatalf("get generation plan: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(plan.CanonicalPayload, &payload); err != nil {
		t.Fatalf("decode generation plan: %v", err)
	}
	payload["identity"].(map[string]any)["session_id"] = "sess_drifted"
	canonical, err := store.CanonicalGenerationPlanPayload(payload)
	if err != nil {
		t.Fatalf("canonical generation plan: %v", err)
	}
	planDigest := store.GenerationPlanDigest(canonical)
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE generation_plans
SET canonical_payload = ?,
    plan_digest = ?
WHERE generation_id = ?`, string(canonical), planDigest, "gen_frozen_evidence"); err != nil {
		t.Fatalf("mutate generation plan identity: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE generation_plan_projections
SET plan_digest = ?
WHERE generation_id = ?`, planDigest, "gen_frozen_evidence"); err != nil {
		t.Fatalf("align projection plan digests: %v", err)
	}

	err = srv.verifyGenerationPlanFrozenEvidence(ctx, "gen_frozen_evidence", serverGenerationPlanFrozenEvidenceDetails(), serverGenerationPlanFrozenEvidenceArtifacts())
	if err == nil || !strings.Contains(err.Error(), "identity.session_id mismatch") {
		t.Fatalf("expected plan identity mismatch, got %v", err)
	}
}
