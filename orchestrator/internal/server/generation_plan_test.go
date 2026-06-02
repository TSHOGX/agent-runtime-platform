package server

import (
	"context"
	"strings"
	"testing"
	"time"

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
