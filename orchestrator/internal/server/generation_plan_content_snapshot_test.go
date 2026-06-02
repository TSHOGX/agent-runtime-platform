package server

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/generationplan"
	"harness-platform/orchestrator/internal/sessionstate"
	"harness-platform/orchestrator/internal/store"
)

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

func TestServerRenderersThreadContentSnapshots(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	cfg := testServerConfig(dir)
	now := time.Now().UTC()
	session := createServerTestSession(t, ctx, st, dir, "sess_content_thread", string(sessionstate.Created), now, nil)
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    serverTestAllocatorConfig(cfg, session.DriverID),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	details, err := st.GetRuntimeGenerationDetails(ctx, session.ID, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get generation details: %v", err)
	}
	volumes := sessionRuntimeDataVolumes{
		Workspace: store.SessionWorkspaceVolume{
			SessionID:                session.ID,
			HostPath:                 filepath.Join(dir, "volumes", "workspaces", session.ID),
			LayoutVersion:            store.DataVolumeLayoutVersion,
			RuntimeIdentityDigest:    "sha256:workspace-identity",
			ProvisioningMarkerPath:   filepath.Join(dir, "evidence", "workspaces", session.ID+".json"),
			ProvisioningMarkerDigest: "sha256:workspace-marker",
		},
		DriverHome: store.SessionDriverHomeVolume{
			SessionID:                session.ID,
			Driver:                   session.DriverID,
			HostPath:                 filepath.Join(dir, "volumes", "driver-homes", session.ID, session.DriverID),
			LayoutVersion:            store.DataVolumeLayoutVersion,
			RuntimeIdentityDigest:    "sha256:driver-home-identity",
			ProvisioningMarkerPath:   filepath.Join(dir, "evidence", "driver-homes", session.ID, session.DriverID+".json"),
			ProvisioningMarkerDigest: "sha256:driver-home-marker",
		},
	}
	snapshots := []store.ContentSnapshotRecord{{
		Kind:                 store.ContentSnapshotKindSkills,
		Digest:               "sha256:skills",
		ImmutableHostPath:    filepath.Join(dir, "content", "skills", "sha256-skills"),
		MountDestination:     store.ContentSnapshotSkillsMount,
		SourceEvidenceDigest: "sha256:skills-source",
		RetentionClass:       "generation_plan",
	}}
	artifacts := testGenerationArtifacts()
	srv := &Server{cfg: cfg, store: st}

	req := srv.runtimeStartRequest(session, allocation.GenerationID, details, artifacts, volumes, snapshots)
	if len(req.ContentSnapshots) != 1 || req.ContentSnapshots[0].Digest != "sha256:skills" {
		t.Fatalf("runtime start request content snapshots = %+v", req.ContentSnapshots)
	}
	driftedSession := session
	driftedSession.DriverID = string(agents.Shell)
	req = srv.runtimeStartRequest(driftedSession, allocation.GenerationID, details, artifacts, volumes, snapshots)
	if req.DriverID != details.DriverID {
		t.Fatalf("runtime start request driver id = %q want generation driver %q", req.DriverID, details.DriverID)
	}

	contractPayload, err := srv.sandboxContractPayload(session, details, artifacts, "sha256:resource-identity", volumes, snapshots)
	if err != nil {
		t.Fatalf("render sandbox contract with content snapshot: %v", err)
	}
	contractMounts := contractPayload["mount_plan"].(map[string]any)["content_snapshots"].(map[string]any)
	contractSkills := contractMounts[store.ContentSnapshotKindSkills].(map[string]any)
	if contractSkills["source"] != snapshots[0].ImmutableHostPath ||
		contractSkills["destination"] != store.ContentSnapshotSkillsMount ||
		contractSkills["digest"] != "sha256:skills" ||
		contractSkills["mode"] != "ro" ||
		contractSkills["exact"] != true {
		t.Fatalf("sandbox contract skills snapshot mount = %+v", contractSkills)
	}

	inputEvidence, err := srv.sandboxContractInputEvidenceFor(session, details.DriverID)
	if err != nil {
		t.Fatalf("input evidence: %v", err)
	}
	planPayload, err := srv.shadowGenerationPlanPayload(session, details, artifacts, contractPayload, "sha256:resource-identity", volumes, snapshots, inputEvidence)
	if err != nil {
		t.Fatalf("render generation plan with content snapshot: %v", err)
	}
	if err := generationplan.Validate(generationplan.ValidateParams{Payload: planPayload}); err != nil {
		t.Fatalf("validate content snapshot plan: %v", err)
	}
	planSnapshots := planPayload["content_snapshots"].(map[string]any)
	planSkills := planSnapshots[store.ContentSnapshotKindSkills].(map[string]any)
	if planSkills["digest"] != "sha256:skills" ||
		planSkills["immutable_host_path"] != snapshots[0].ImmutableHostPath ||
		planSkills["mount_destination"] != store.ContentSnapshotSkillsMount {
		t.Fatalf("generation plan skills snapshot = %+v", planSkills)
	}
	planMounts := planPayload["mounts"].(map[string]any)["content_snapshots"].(map[string]any)
	planMountSkills := planMounts[store.ContentSnapshotKindSkills].(map[string]any)
	if planMountSkills["digest"] != "sha256:skills" ||
		planMountSkills["destination"] != store.ContentSnapshotSkillsMount ||
		planMountSkills["exact"] != true {
		t.Fatalf("generation plan skills snapshot mount = %+v", planMountSkills)
	}
	workspace := planPayload["data_volumes"].(map[string]any)["workspace"].(map[string]any)
	if workspace["platform_content_mount_scope"] != "immutable_content_snapshots" {
		t.Fatalf("workspace platform content scope = %+v", workspace)
	}
}

func TestSelectGenerationContentSnapshotsRequiresSingleImmutableSnapshot(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, _ := openServerOwnedStore(t, ctx, dir)
	srv := &Server{store: st}
	policy := agents.FeaturePolicy{
		agents.FeatureSkillsSnapshot:  agents.FeaturePolicyRequired,
		agents.FeatureManagedSettings: agents.FeaturePolicyDisabled,
	}

	if _, err := srv.selectGenerationContentSnapshots(ctx, policy); err == nil ||
		!strings.Contains(err.Error(), "required feature skills_snapshot has no skills snapshot") {
		t.Fatalf("expected missing skills snapshot selection error, got %v", err)
	}

	skills, err := st.StoreContentSnapshot(ctx, store.StoreContentSnapshotParams{
		Kind:                 store.ContentSnapshotKindSkills,
		Digest:               "sha256:skills",
		ImmutableHostPath:    "/var/lib/harness/content/skills/sha256-skills",
		MountDestination:     store.ContentSnapshotSkillsMount,
		SourceEvidenceDigest: "sha256:skills-source",
		RetentionClass:       "generation_plan",
	})
	if err != nil {
		t.Fatalf("store skills snapshot: %v", err)
	}
	selected, err := srv.selectGenerationContentSnapshots(ctx, policy)
	if err != nil {
		t.Fatalf("select single skills snapshot: %v", err)
	}
	if len(selected) != 1 || selected[0].Digest != skills.Digest {
		t.Fatalf("selected snapshots = %+v want %+v", selected, skills)
	}

	if _, err := st.StoreContentSnapshot(ctx, store.StoreContentSnapshotParams{
		Kind:                 store.ContentSnapshotKindSkills,
		Digest:               "sha256:skills-other",
		ImmutableHostPath:    "/var/lib/harness/content/skills/sha256-skills-other",
		MountDestination:     store.ContentSnapshotSkillsMount,
		SourceEvidenceDigest: "sha256:skills-source-other",
		RetentionClass:       "generation_plan",
	}); err != nil {
		t.Fatalf("store second skills snapshot: %v", err)
	}
	if _, err := srv.selectGenerationContentSnapshots(ctx, policy); err == nil ||
		!strings.Contains(err.Error(), "required feature skills_snapshot is ambiguous") {
		t.Fatalf("expected ambiguous skills snapshot selection error, got %v", err)
	}

	policy[agents.FeatureSkillsSnapshot] = agents.FeaturePolicyDisabled
	policy[agents.FeatureManagedSettings] = agents.FeaturePolicyRequired
	managed, err := st.StoreContentSnapshot(ctx, store.StoreContentSnapshotParams{
		Kind:                 store.ContentSnapshotKindManagedSettings,
		Digest:               "sha256:settings",
		ImmutableHostPath:    "/var/lib/harness/content/managed-settings/sha256-settings",
		MountDestination:     store.ContentSnapshotManagedSettingsMount,
		SourceEvidenceDigest: "sha256:settings-source",
		RetentionClass:       "generation_plan",
	})
	if err != nil {
		t.Fatalf("store managed settings snapshot: %v", err)
	}
	selected, err = srv.selectGenerationContentSnapshots(ctx, policy)
	if err != nil {
		t.Fatalf("select managed settings snapshot: %v", err)
	}
	if len(selected) != 1 || selected[0].Digest != managed.Digest {
		t.Fatalf("selected managed settings snapshots = %+v want %+v", selected, managed)
	}

	policy[agents.FeatureManagedSettings] = agents.FeaturePolicyUnsupported
	selected, err = srv.selectGenerationContentSnapshots(ctx, policy)
	if err != nil {
		t.Fatalf("unsupported snapshot feature should not select: %v", err)
	}
	if len(selected) != 0 {
		t.Fatalf("unsupported snapshot feature selected snapshots: %+v", selected)
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
