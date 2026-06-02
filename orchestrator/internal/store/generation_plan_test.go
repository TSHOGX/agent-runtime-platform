package store

import (
	"bytes"
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStoreGenerationPlanCanonicalizesAndIsImmutable(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	createStoreSession(t, ctx, st, "sess_plan")
	createRuntimeGenerationForPlanTest(t, ctx, st, "sess_plan", "gen_plan", "allocating")

	payload := map[string]any{
		"generation_id": "gen_plan",
		"plan_version":  float64(1),
		"driver": map[string]any{
			"driver_id": "pi",
		},
	}
	record, err := st.StoreGenerationPlan(ctx, StoreGenerationPlanParams{
		GenerationID: "gen_plan",
		Payload:      payload,
		Now:          time.Date(2026, 6, 2, 1, 2, 3, 4, time.UTC),
	})
	if err != nil {
		t.Fatalf("store plan: %v", err)
	}
	if record.PlanVersion != GenerationPlanVersion {
		t.Fatalf("plan version=%d want %d", record.PlanVersion, GenerationPlanVersion)
	}
	if !bytes.Equal(record.CanonicalPayload, []byte(`{"driver":{"driver_id":"pi"},"generation_id":"gen_plan","plan_version":1}`)) {
		t.Fatalf("canonical payload mismatch: %s", record.CanonicalPayload)
	}
	if record.PlanDigest != GenerationPlanDigest(record.CanonicalPayload) {
		t.Fatalf("plan digest=%s want %s", record.PlanDigest, GenerationPlanDigest(record.CanonicalPayload))
	}

	loaded, err := st.GetGenerationPlan(ctx, "gen_plan")
	if err != nil {
		t.Fatalf("get plan: %v", err)
	}
	if loaded.PlanDigest != record.PlanDigest || !bytes.Equal(loaded.CanonicalPayload, record.CanonicalPayload) {
		t.Fatalf("loaded plan mismatch: got %+v want %+v", loaded, record)
	}

	replayed, err := st.StoreGenerationPlan(ctx, StoreGenerationPlanParams{
		GenerationID: "gen_plan",
		Payload:      []byte(`{"driver":{"driver_id":"pi"},"generation_id":"gen_plan","plan_version":1}`),
		PlanDigest:   record.PlanDigest,
		Now:          record.CreatedAt.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("idempotent store plan: %v", err)
	}
	if replayed.CreatedAt != record.CreatedAt || replayed.PlanDigest != record.PlanDigest {
		t.Fatalf("idempotent replay changed record: got %+v want %+v", replayed, record)
	}

	_, err = st.StoreGenerationPlan(ctx, StoreGenerationPlanParams{
		GenerationID: "gen_plan",
		Payload: map[string]any{
			"generation_id": "gen_plan",
			"plan_version":  1,
			"driver":        map[string]any{"driver_id": "claude_code"},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "different immutable payload") {
		t.Fatalf("expected immutable payload rejection, got %v", err)
	}
}

func TestStoreGenerationPlanRejectsDigestMismatch(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	createStoreSession(t, ctx, st, "sess_plan_digest")
	createRuntimeGenerationForPlanTest(t, ctx, st, "sess_plan_digest", "gen_plan_digest", "allocating")

	_, err = st.StoreGenerationPlan(ctx, StoreGenerationPlanParams{
		GenerationID: "gen_plan_digest",
		Payload:      map[string]any{"generation_id": "gen_plan_digest", "plan_version": 1},
		PlanDigest:   "sha256:wrong",
	})
	if err == nil || !strings.Contains(err.Error(), "generation plan digest mismatch") {
		t.Fatalf("expected digest mismatch, got %v", err)
	}
}

func TestGenerationPlanProjectionKindsAreFixedAndCloned(t *testing.T) {
	kinds := GenerationPlanProjectionKinds()
	want := []string{
		GenerationPlanProjectionSandboxContract,
		GenerationPlanProjectionControlManifest,
		GenerationPlanProjectionControlManifestProjected,
		GenerationPlanProjectionOCISpec,
		GenerationPlanProjectionBundle,
		GenerationPlanProjectionRuntimeConfig,
	}
	if len(kinds) != len(want) {
		t.Fatalf("projection kinds=%+v want %+v", kinds, want)
	}
	for i := range want {
		if kinds[i] != want[i] {
			t.Fatalf("projection kinds=%+v want %+v", kinds, want)
		}
		version, ok := GenerationPlanProjectionVersionFor(kinds[i])
		if !ok || version != GenerationPlanProjectionVersion {
			t.Fatalf("projection version for %s = %d/%v", kinds[i], version, ok)
		}
	}
	kinds[0] = "mutated"
	again := GenerationPlanProjectionKinds()
	if again[0] != GenerationPlanProjectionSandboxContract {
		t.Fatalf("projection kinds should be cloned, got %+v", again)
	}
	if version, ok := GenerationPlanProjectionVersionFor("unknown"); ok || version != 0 {
		t.Fatalf("unknown projection version = %d/%v", version, ok)
	}
}

func TestRequireGenerationPlanForLaunchRejectsNonTerminalWithoutPlan(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	createStoreSession(t, ctx, st, "sess_launch_plan")
	createRuntimeGenerationForPlanTest(t, ctx, st, "sess_launch_plan", "gen_without_plan", "starting")

	if _, err := st.RequireGenerationPlanForLaunch(ctx, "gen_without_plan"); err == nil ||
		!strings.Contains(err.Error(), "generation plan is required for non-terminal generation") {
		t.Fatalf("expected missing plan launch guard, got %v", err)
	}

	plan, err := st.StoreGenerationPlan(ctx, StoreGenerationPlanParams{
		GenerationID: "gen_without_plan",
		Payload:      map[string]any{"generation_id": "gen_without_plan", "plan_version": 1},
	})
	if err != nil {
		t.Fatalf("store plan: %v", err)
	}
	got, err := st.RequireGenerationPlanForLaunch(ctx, "gen_without_plan")
	if err != nil {
		t.Fatalf("require plan after store: %v", err)
	}
	if got.PlanDigest != plan.PlanDigest {
		t.Fatalf("launch guard returned digest=%s want %s", got.PlanDigest, plan.PlanDigest)
	}
}

func TestStoreGenerationPlanProjectionIsImmutableAndListsByKind(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	createStoreSession(t, ctx, st, "sess_projection")
	createRuntimeGenerationForPlanTest(t, ctx, st, "sess_projection", "gen_projection", "allocating")
	plan, err := st.StoreGenerationPlan(ctx, StoreGenerationPlanParams{
		GenerationID: "gen_projection",
		Payload:      map[string]any{"generation_id": "gen_projection", "plan_version": 1},
	})
	if err != nil {
		t.Fatalf("store plan: %v", err)
	}

	manifest, err := st.StoreGenerationPlanProjection(ctx, StoreGenerationPlanProjectionParams{
		GenerationID:      plan.GenerationID,
		PlanDigest:        plan.PlanDigest,
		ProjectionKind:    GenerationPlanProjectionControlManifest,
		ProjectionVersion: GenerationPlanProjectionVersion,
		PayloadDigest:     "sha256:manifest",
		MaterializedPath:  "/var/lib/harness/run/gen_projection/control/manifest.json",
	})
	if err != nil {
		t.Fatalf("store manifest projection: %v", err)
	}
	contract, err := st.StoreGenerationPlanProjection(ctx, StoreGenerationPlanProjectionParams{
		GenerationID:      plan.GenerationID,
		PlanDigest:        plan.PlanDigest,
		ProjectionKind:    GenerationPlanProjectionSandboxContract,
		ProjectionVersion: GenerationPlanProjectionVersion,
		PayloadDigest:     "sha256:contract",
	})
	if err != nil {
		t.Fatalf("store contract projection: %v", err)
	}
	replayed, err := st.StoreGenerationPlanProjection(ctx, StoreGenerationPlanProjectionParams{
		GenerationID:      plan.GenerationID,
		PlanDigest:        plan.PlanDigest,
		ProjectionKind:    GenerationPlanProjectionControlManifest,
		ProjectionVersion: GenerationPlanProjectionVersion,
		PayloadDigest:     "sha256:manifest",
		MaterializedPath:  "/var/lib/harness/run/gen_projection/control/manifest.json",
	})
	if err != nil {
		t.Fatalf("idempotent projection: %v", err)
	}
	if replayed.CreatedAt != manifest.CreatedAt || replayed.PayloadDigest != manifest.PayloadDigest {
		t.Fatalf("idempotent projection changed record: got %+v want %+v", replayed, manifest)
	}

	_, err = st.StoreGenerationPlanProjection(ctx, StoreGenerationPlanProjectionParams{
		GenerationID:      plan.GenerationID,
		PlanDigest:        plan.PlanDigest,
		ProjectionKind:    GenerationPlanProjectionControlManifest,
		ProjectionVersion: GenerationPlanProjectionVersion,
		PayloadDigest:     "sha256:changed",
		MaterializedPath:  "/var/lib/harness/run/gen_projection/control/manifest.json",
	})
	if err == nil || !strings.Contains(err.Error(), "different immutable payload") {
		t.Fatalf("expected immutable projection rejection, got %v", err)
	}

	records, err := st.ListGenerationPlanProjections(ctx, plan.GenerationID)
	if err != nil {
		t.Fatalf("list projections: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("projection count=%d want 2: %+v", len(records), records)
	}
	if records[0].ProjectionKind != manifest.ProjectionKind || records[1].ProjectionKind != contract.ProjectionKind {
		t.Fatalf("projections not ordered by kind: %+v", records)
	}
}

func TestStoreGenerationPlanProjectionRejectsPlanDigestMismatch(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	createStoreSession(t, ctx, st, "sess_projection_digest")
	createRuntimeGenerationForPlanTest(t, ctx, st, "sess_projection_digest", "gen_projection_digest", "allocating")
	if _, err := st.StoreGenerationPlan(ctx, StoreGenerationPlanParams{
		GenerationID: "gen_projection_digest",
		Payload:      map[string]any{"generation_id": "gen_projection_digest", "plan_version": 1},
	}); err != nil {
		t.Fatalf("store plan: %v", err)
	}

	_, err = st.StoreGenerationPlanProjection(ctx, StoreGenerationPlanProjectionParams{
		GenerationID:      "gen_projection_digest",
		PlanDigest:        "sha256:wrong",
		ProjectionKind:    "control_manifest",
		ProjectionVersion: 1,
		PayloadDigest:     "sha256:manifest",
	})
	if err == nil || !strings.Contains(err.Error(), "projection plan digest mismatch") {
		t.Fatalf("expected plan digest mismatch, got %v", err)
	}
}

func TestStoreGenerationPlanProjectionRejectsNonCanonicalMaterializedPath(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	createStoreSession(t, ctx, st, "sess_projection_path")
	createRuntimeGenerationForPlanTest(t, ctx, st, "sess_projection_path", "gen_projection_path", "allocating")
	plan, err := st.StoreGenerationPlan(ctx, StoreGenerationPlanParams{
		GenerationID: "gen_projection_path",
		Payload:      map[string]any{"generation_id": "gen_projection_path", "plan_version": 1},
	})
	if err != nil {
		t.Fatalf("store plan: %v", err)
	}

	for _, tc := range []struct {
		name string
		path string
	}{
		{name: "relative", path: "var/lib/harness/run/control/manifest.json"},
		{name: "unclean", path: "/var/lib/harness/run/control/../manifest.json"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err = st.StoreGenerationPlanProjection(ctx, StoreGenerationPlanProjectionParams{
				GenerationID:      plan.GenerationID,
				PlanDigest:        plan.PlanDigest,
				ProjectionKind:    GenerationPlanProjectionControlManifest,
				ProjectionVersion: GenerationPlanProjectionVersion,
				PayloadDigest:     "sha256:manifest",
				MaterializedPath:  tc.path,
			})
			if err == nil || !strings.Contains(err.Error(), "materialized path must be canonical absolute") {
				t.Fatalf("expected canonical materialized path error, got %v", err)
			}
		})
	}
}

func TestStoreGenerationPlanProjectionRejectsUnsupportedKindAndVersion(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	createStoreSession(t, ctx, st, "sess_projection_schema")
	createRuntimeGenerationForPlanTest(t, ctx, st, "sess_projection_schema", "gen_projection_schema", "allocating")
	plan, err := st.StoreGenerationPlan(ctx, StoreGenerationPlanParams{
		GenerationID: "gen_projection_schema",
		Payload:      map[string]any{"generation_id": "gen_projection_schema", "plan_version": 1},
	})
	if err != nil {
		t.Fatalf("store plan: %v", err)
	}

	_, err = st.StoreGenerationPlanProjection(ctx, StoreGenerationPlanProjectionParams{
		GenerationID:      plan.GenerationID,
		PlanDigest:        plan.PlanDigest,
		ProjectionKind:    "driver_config",
		ProjectionVersion: GenerationPlanProjectionVersion,
		PayloadDigest:     "sha256:driver-config",
	})
	if err == nil || !strings.Contains(err.Error(), `unsupported generation plan projection kind "driver_config"`) {
		t.Fatalf("expected unsupported projection kind error, got %v", err)
	}

	_, err = st.StoreGenerationPlanProjection(ctx, StoreGenerationPlanProjectionParams{
		GenerationID:      plan.GenerationID,
		PlanDigest:        plan.PlanDigest,
		ProjectionKind:    GenerationPlanProjectionControlManifest,
		ProjectionVersion: GenerationPlanProjectionVersion + 1,
		PayloadDigest:     "sha256:manifest",
	})
	if err == nil || !strings.Contains(err.Error(), "generation plan projection control_manifest version = 2, want 1") {
		t.Fatalf("expected projection version error, got %v", err)
	}
}

func TestListGenerationPlanProjectionsRejectsUnsupportedKindAndVersion(t *testing.T) {
	for _, tt := range []struct {
		name      string
		corrupt   func(context.Context, *Store, GenerationPlanRecord) error
		wantError string
	}{
		{
			name: "unsupported kind",
			corrupt: func(ctx context.Context, st *Store, plan GenerationPlanRecord) error {
				if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE generation_plan_projections
SET projection_kind = 'driver_config'
WHERE generation_id = ?
  AND projection_kind = ?`, plan.GenerationID, GenerationPlanProjectionControlManifest); err != nil {
					return err
				}
				return nil
			},
			wantError: `unsupported generation plan projection kind "driver_config"`,
		},
		{
			name: "version drift",
			corrupt: func(ctx context.Context, st *Store, plan GenerationPlanRecord) error {
				if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE generation_plan_projections
SET projection_version = 2
WHERE generation_id = ?
  AND projection_kind = ?`, plan.GenerationID, GenerationPlanProjectionControlManifest); err != nil {
					return err
				}
				return nil
			},
			wantError: "generation plan projection control_manifest version = 2, want 1",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			st, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			t.Cleanup(func() { _ = st.Close() })
			createStoreSession(t, ctx, st, "sess_projection_read_schema")
			createRuntimeGenerationForPlanTest(t, ctx, st, "sess_projection_read_schema", "gen_projection_read_schema", "allocating")
			plan, err := st.StoreGenerationPlan(ctx, StoreGenerationPlanParams{
				GenerationID: "gen_projection_read_schema",
				Payload:      map[string]any{"generation_id": "gen_projection_read_schema", "plan_version": 1},
			})
			if err != nil {
				t.Fatalf("store plan: %v", err)
			}
			if _, err := st.StoreGenerationPlanProjection(ctx, StoreGenerationPlanProjectionParams{
				GenerationID:      plan.GenerationID,
				PlanDigest:        plan.PlanDigest,
				ProjectionKind:    GenerationPlanProjectionControlManifest,
				ProjectionVersion: GenerationPlanProjectionVersion,
				PayloadDigest:     "sha256:manifest",
			}); err != nil {
				t.Fatalf("store projection: %v", err)
			}

			if err := tt.corrupt(ctx, st, plan); err != nil {
				t.Fatalf("corrupt projection row: %v", err)
			}
			if _, err := st.ListGenerationPlanProjections(ctx, plan.GenerationID); err == nil ||
				!strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("expected %q, got %v", tt.wantError, err)
			}
		})
	}
}

func TestVerifyGenerationPlanProjectionsMatchesStoredDigests(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	createStoreSession(t, ctx, st, "sess_verify_projection")
	createRuntimeGenerationForPlanTest(t, ctx, st, "sess_verify_projection", "gen_verify_projection", "allocating")
	plan, err := st.StoreGenerationPlan(ctx, StoreGenerationPlanParams{
		GenerationID: "gen_verify_projection",
		Payload:      map[string]any{"generation_id": "gen_verify_projection", "plan_version": 1},
	})
	if err != nil {
		t.Fatalf("store plan: %v", err)
	}
	for _, projection := range []StoreGenerationPlanProjectionParams{
		{GenerationID: plan.GenerationID, PlanDigest: plan.PlanDigest, ProjectionKind: GenerationPlanProjectionControlManifest, ProjectionVersion: GenerationPlanProjectionVersion, PayloadDigest: "sha256:manifest", MaterializedPath: "/var/lib/harness/run/control/gen_verify_projection/session.json"},
		{GenerationID: plan.GenerationID, PlanDigest: plan.PlanDigest, ProjectionKind: GenerationPlanProjectionOCISpec, ProjectionVersion: GenerationPlanProjectionVersion, PayloadDigest: "sha256:spec", MaterializedPath: "/var/lib/harness/run/runtime/gen_verify_projection/config.json"},
	} {
		if _, err := st.StoreGenerationPlanProjection(ctx, projection); err != nil {
			t.Fatalf("store projection %s: %v", projection.ProjectionKind, err)
		}
	}

	verified, err := st.VerifyGenerationPlanProjections(ctx, VerifyGenerationPlanProjectionsParams{
		GenerationID: plan.GenerationID,
		PlanDigest:   plan.PlanDigest,
		Expected: []GenerationPlanProjectionExpectation{
			{ProjectionKind: GenerationPlanProjectionControlManifest, PayloadDigest: "sha256:manifest", MaterializedPath: "/var/lib/harness/run/control/gen_verify_projection/session.json"},
			{ProjectionKind: GenerationPlanProjectionOCISpec, PayloadDigest: "sha256:spec", MaterializedPath: "/var/lib/harness/run/runtime/gen_verify_projection/config.json"},
		},
	})
	if err != nil {
		t.Fatalf("verify matching projections: %v", err)
	}
	if !verified {
		t.Fatalf("expected verification to report stored plan")
	}

	if _, err := st.VerifyGenerationPlanProjections(ctx, VerifyGenerationPlanProjectionsParams{
		GenerationID: plan.GenerationID,
		PlanDigest:   plan.PlanDigest,
		Expected: []GenerationPlanProjectionExpectation{
			{ProjectionKind: GenerationPlanProjectionControlManifest, PayloadDigest: "sha256:changed"},
		},
	}); err == nil || !strings.Contains(err.Error(), "control_manifest digest mismatch") {
		t.Fatalf("expected projection digest mismatch, got %v", err)
	}
	if _, err := st.VerifyGenerationPlanProjections(ctx, VerifyGenerationPlanProjectionsParams{
		GenerationID: plan.GenerationID,
		PlanDigest:   plan.PlanDigest,
		Expected: []GenerationPlanProjectionExpectation{
			{ProjectionKind: GenerationPlanProjectionControlManifest, ProjectionVersion: 2, PayloadDigest: "sha256:manifest"},
		},
	}); err == nil || !strings.Contains(err.Error(), "control_manifest version mismatch") {
		t.Fatalf("expected projection version mismatch, got %v", err)
	}
	if _, err := st.VerifyGenerationPlanProjections(ctx, VerifyGenerationPlanProjectionsParams{
		GenerationID: plan.GenerationID,
		PlanDigest:   plan.PlanDigest,
		Expected: []GenerationPlanProjectionExpectation{
			{ProjectionKind: GenerationPlanProjectionControlManifest, PayloadDigest: "sha256:manifest", MaterializedPath: "/var/lib/harness/run/control/gen_verify_projection/changed.json"},
		},
	}); err == nil || !strings.Contains(err.Error(), "control_manifest materialized path mismatch") {
		t.Fatalf("expected projection materialized path mismatch, got %v", err)
	}
	if _, err := st.VerifyGenerationPlanProjections(ctx, VerifyGenerationPlanProjectionsParams{
		GenerationID: plan.GenerationID,
		PlanDigest:   "sha256:wrong",
		Expected:     []GenerationPlanProjectionExpectation{},
	}); err == nil || !strings.Contains(err.Error(), "generation plan digest mismatch") {
		t.Fatalf("expected plan digest mismatch, got %v", err)
	}
	if _, err := st.VerifyGenerationPlanProjections(ctx, VerifyGenerationPlanProjectionsParams{
		GenerationID: plan.GenerationID,
		Expected: []GenerationPlanProjectionExpectation{
			{ProjectionKind: "missing", PayloadDigest: "sha256:missing"},
		},
	}); err == nil || !strings.Contains(err.Error(), "projection missing is required") {
		t.Fatalf("expected missing projection error, got %v", err)
	}
}

func TestVerifyGenerationPlanProjectionsCanRequirePlan(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	createStoreSession(t, ctx, st, "sess_verify_missing_plan")
	createRuntimeGenerationForPlanTest(t, ctx, st, "sess_verify_missing_plan", "gen_verify_missing_plan", "allocating")

	verified, err := st.VerifyGenerationPlanProjections(ctx, VerifyGenerationPlanProjectionsParams{
		GenerationID: "gen_verify_missing_plan",
		Expected:     []GenerationPlanProjectionExpectation{{ProjectionKind: GenerationPlanProjectionControlManifest, PayloadDigest: "sha256:manifest"}},
	})
	if err != nil {
		t.Fatalf("optional missing plan should not fail: %v", err)
	}
	if verified {
		t.Fatalf("optional missing plan should report verified=false")
	}
	if _, err := st.VerifyGenerationPlanProjections(ctx, VerifyGenerationPlanProjectionsParams{
		GenerationID: "gen_verify_missing_plan",
		RequirePlan:  true,
	}); err == nil || !strings.Contains(err.Error(), "generation plan is required") {
		t.Fatalf("expected required missing plan error, got %v", err)
	}
}

func createRuntimeGenerationForPlanTest(t *testing.T, ctx context.Context, st *Store, sessionID, generationID, status string) {
	t.Helper()
	if _, err := st.db.ExecContext(ctx, `
INSERT INTO runtime_generations (generation_id, session_id, status, lease_owner, lease_expires_at)
VALUES (?, ?, ?, 'owner', ?)`, generationID, sessionID, status, formatTime(time.Now().UTC().Add(time.Hour))); err != nil {
		t.Fatalf("insert runtime generation %s: %v", generationID, err)
	}
}

func TestGetGenerationPlanRejectsCorruptStoredDigest(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	createStoreSession(t, ctx, st, "sess_corrupt_plan")
	createRuntimeGenerationForPlanTest(t, ctx, st, "sess_corrupt_plan", "gen_corrupt_plan", "allocating")
	if _, err := st.db.ExecContext(ctx, `
INSERT INTO generation_plans (generation_id, plan_version, canonical_payload, plan_digest, created_at)
VALUES (?, 1, ?, 'sha256:wrong', ?)`,
		"gen_corrupt_plan", `{"generation_id":"gen_corrupt_plan","plan_version":1}`, formatTime(time.Now().UTC())); err != nil {
		t.Fatalf("insert corrupt plan: %v", err)
	}
	if _, err := st.GetGenerationPlan(ctx, "gen_corrupt_plan"); err == nil ||
		!strings.Contains(err.Error(), "generation plan digest mismatch") {
		t.Fatalf("expected corrupt digest rejection, got %v", err)
	}
}

func TestGetGenerationPlanNoRows(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if _, err := st.GetGenerationPlan(ctx, "missing"); err != sql.ErrNoRows {
		t.Fatalf("GetGenerationPlan missing error=%v want sql.ErrNoRows", err)
	}
}
