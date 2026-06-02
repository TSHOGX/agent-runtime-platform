package server

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/generationplan"
	"harness-platform/orchestrator/internal/planprojection"
	"harness-platform/orchestrator/internal/sessionstate"
	"harness-platform/orchestrator/internal/store"
)

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

func TestVerifyGenerationPlanFrozenEvidenceChecksContentSnapshots(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, _ := openServerOwnedStore(t, ctx, dir)
	srv := &Server{store: st}
	snapshotPath := filepath.Join(dir, "content", "skills", "sha256-skills")
	snapshotDigest := writeServerContentSnapshotFixture(t, snapshotPath)
	planPayload := validServerGenerationPlanPayload()
	contentSnapshots := planPayload["content_snapshots"].(map[string]any)
	contentSnapshots["skills"] = map[string]any{
		"kind":                   "skills",
		"digest":                 snapshotDigest,
		"immutable_host_path":    snapshotPath,
		"mount_destination":      "/harness-skills",
		"source_evidence_digest": "sha256:skills-source",
		"retention_class":        "generation_plan",
	}
	planPayload["mounts"].(map[string]any)["content_snapshots"] = map[string]any{
		"skills": map[string]any{
			"mount_name":  "skills_snapshot",
			"type":        "bind",
			"mode":        "ro",
			"exact":       true,
			"source":      snapshotPath,
			"destination": "/harness-skills",
			"digest":      snapshotDigest,
		},
	}
	workspaceVolume := planPayload["data_volumes"].(map[string]any)["workspace"].(map[string]any)
	workspaceVolume["platform_content_mount_scope"] = "immutable_content_snapshots"
	plan := storeServerFrozenEvidencePlan(t, ctx, st, dir, planPayload)
	if _, err := st.StoreContentSnapshot(ctx, store.StoreContentSnapshotParams{
		Kind:                 store.ContentSnapshotKindSkills,
		Digest:               snapshotDigest,
		ImmutableHostPath:    snapshotPath,
		MountDestination:     "/harness-skills",
		SourceEvidenceDigest: "sha256:skills-source",
		RetentionClass:       "generation_plan",
	}); err != nil {
		t.Fatalf("store content snapshot: %v", err)
	}

	details := serverGenerationPlanFrozenEvidenceDetails()
	details.CheckpointPlanDigest = plan.PlanDigest
	artifacts := serverGenerationPlanFrozenEvidenceArtifacts()
	if err := srv.verifyGenerationPlanFrozenEvidence(ctx, "gen_frozen_evidence", details, artifacts); err != nil {
		t.Fatalf("verify content snapshot frozen evidence: %v", err)
	}

	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE content_snapshots
SET mount_destination = '/harness-skills-drifted'
WHERE snapshot_kind = ?
  AND snapshot_digest = ?`, store.ContentSnapshotKindSkills, snapshotDigest); err != nil {
		t.Fatalf("mutate stored content snapshot: %v", err)
	}
	if err := srv.verifyGenerationPlanFrozenEvidence(ctx, "gen_frozen_evidence", details, artifacts); err == nil ||
		!strings.Contains(err.Error(), "skills content snapshot mount destination must be /harness-skills") {
		t.Fatalf("expected content snapshot metadata mismatch, got %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE content_snapshots
SET mount_destination = '/harness-skills'
WHERE snapshot_kind = ?
  AND snapshot_digest = ?`, store.ContentSnapshotKindSkills, snapshotDigest); err != nil {
		t.Fatalf("restore stored content snapshot: %v", err)
	}

	if err := os.WriteFile(filepath.Join(snapshotPath, "README.md"), []byte("mutated skills"), 0o644); err != nil {
		t.Fatalf("mutate content snapshot payload: %v", err)
	}
	if err := srv.verifyGenerationPlanFrozenEvidence(ctx, "gen_frozen_evidence", details, artifacts); err == nil ||
		!strings.Contains(err.Error(), "content snapshot skills digest mismatch") {
		t.Fatalf("expected content snapshot digest mismatch, got %v", err)
	}

	if _, err := st.DBForTest().ExecContext(ctx, `
DELETE FROM content_snapshots
WHERE snapshot_kind = ?
  AND snapshot_digest = ?`, store.ContentSnapshotKindSkills, snapshotDigest); err != nil {
		t.Fatalf("delete stored content snapshot: %v", err)
	}
	if err := srv.verifyGenerationPlanFrozenEvidence(ctx, "gen_frozen_evidence", details, artifacts); err == nil ||
		!strings.Contains(err.Error(), "generation plan content snapshot skills") {
		t.Fatalf("expected content snapshot mismatch, got %v", err)
	}
}

func TestVerifyGenerationPlanFrozenEvidenceRequiresPlan(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, _ := openServerOwnedStore(t, ctx, dir)
	srv := &Server{store: st}
	session := createServerTestSession(t, ctx, st, dir, "sess_missing_plan", string(sessionstate.Created), time.Now().UTC(), nil)
	if _, err := st.DBForTest().ExecContext(ctx, `
INSERT INTO runtime_generations (generation_id, session_id, status, lease_owner, lease_expires_at)
VALUES (?, ?, 'starting', 'owner', ?)`, "gen_missing_plan", session.ID, time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("insert runtime generation: %v", err)
	}
	if err := srv.verifyGenerationPlanFrozenEvidence(ctx, "gen_missing_plan", serverGenerationPlanFrozenEvidenceDetails(), serverGenerationPlanFrozenEvidenceArtifacts()); err == nil ||
		!strings.Contains(err.Error(), "generation plan is required") {
		t.Fatalf("expected required missing plan error, got %v", err)
	}
}

func TestGenerationPlanContentSnapshotRefs(t *testing.T) {
	payload := validServerGenerationPlanPayload()
	contentSnapshots := payload["content_snapshots"].(map[string]any)
	contentSnapshots["skills"] = map[string]any{
		"kind":                   "skills",
		"digest":                 "sha256:skills",
		"immutable_host_path":    "/var/lib/harness/content/skills/sha256-skills",
		"mount_destination":      "/harness-skills",
		"source_evidence_digest": "sha256:skills-source",
		"retention_class":        "generation_plan",
	}
	canonical, err := store.CanonicalGenerationPlanPayload(payload)
	if err != nil {
		t.Fatalf("canonical generation plan payload: %v", err)
	}
	digests := generationplan.ContentSnapshotRefs(canonical)
	if len(digests) != 1 || digests["skills"] != "sha256:skills" {
		t.Fatalf("content snapshot digests = %+v", digests)
	}
}

func TestGenerationPlanRuntimeArtifactsValidatesStoredPlan(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, _ := openServerOwnedStore(t, ctx, dir)
	srv := &Server{store: st}
	storeServerFrozenEvidencePlan(t, ctx, st, dir, validServerGenerationPlanPayload())

	payload := validServerGenerationPlanPayload()
	payload["runtime_artifacts"].(map[string]any)["materialized_driver_config"] = "invalid"
	canonical, err := store.CanonicalGenerationPlanPayload(payload)
	if err != nil {
		t.Fatalf("canonical invalid generation plan payload: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE generation_plans
SET canonical_payload = ?,
    plan_digest = ?
WHERE generation_id = ?`, string(canonical), store.GenerationPlanDigest(canonical), "gen_frozen_evidence"); err != nil {
		t.Fatalf("corrupt generation plan payload: %v", err)
	}

	if _, err := srv.generationPlanRuntimeArtifacts(ctx, "gen_frozen_evidence"); err == nil ||
		!strings.Contains(err.Error(), "runtime_artifacts.materialized_driver_config must be an array") {
		t.Fatalf("expected stored plan validation error, got %v", err)
	}
}

func TestGenerationContentSnapshotsForStartValidatesStoredPlan(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, _ := openServerOwnedStore(t, ctx, dir)
	srv := &Server{store: st}
	storeServerFrozenEvidencePlan(t, ctx, st, dir, validServerGenerationPlanPayload())

	payload := validServerGenerationPlanPayload()
	payload["runtime_artifacts"].(map[string]any)["materialized_driver_config"] = "invalid"
	canonical, err := store.CanonicalGenerationPlanPayload(payload)
	if err != nil {
		t.Fatalf("canonical invalid generation plan payload: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE generation_plans
SET canonical_payload = ?,
    plan_digest = ?
WHERE generation_id = ?`, string(canonical), store.GenerationPlanDigest(canonical), "gen_frozen_evidence"); err != nil {
		t.Fatalf("corrupt generation plan payload: %v", err)
	}

	_, err = srv.generationContentSnapshotsForStart(ctx, store.Session{}, store.RuntimeGenerationDetails{GenerationID: "gen_frozen_evidence"}, false)
	if err == nil || !strings.Contains(err.Error(), "runtime_artifacts.materialized_driver_config must be an array") {
		t.Fatalf("expected stored plan validation error, got %v", err)
	}
}
