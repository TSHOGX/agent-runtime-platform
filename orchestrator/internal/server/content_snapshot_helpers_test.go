package server

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"harness-platform/orchestrator/internal/store"
)

func addServerGenerationPlanSkillsSnapshot(t *testing.T, ctx context.Context, st *store.Store, generationID string) store.ContentSnapshotRecord {
	t.Helper()
	snapshotPath := filepath.Join(t.TempDir(), "skills", generationID)
	snapshotDigest := writeServerContentSnapshotFixture(t, snapshotPath)
	snapshot, err := st.StoreContentSnapshot(ctx, store.StoreContentSnapshotParams{
		Kind:                 store.ContentSnapshotKindSkills,
		Digest:               snapshotDigest,
		ImmutableHostPath:    snapshotPath,
		MountDestination:     store.ContentSnapshotSkillsMount,
		SourceEvidenceDigest: "sha256:skills-source-" + generationID,
		RetentionClass:       "generation_plan",
	})
	if err != nil {
		t.Fatalf("store generation plan skills snapshot: %v", err)
	}
	plan, err := st.GetGenerationPlan(ctx, generationID)
	if err != nil {
		t.Fatalf("get generation plan for snapshot: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(plan.CanonicalPayload, &payload); err != nil {
		t.Fatalf("decode generation plan for snapshot: %v", err)
	}
	contentSnapshots := payload["content_snapshots"].(map[string]any)
	contentSnapshots[store.ContentSnapshotKindSkills] = map[string]any{
		"kind":                   snapshot.Kind,
		"digest":                 snapshot.Digest,
		"immutable_host_path":    snapshot.ImmutableHostPath,
		"mount_destination":      snapshot.MountDestination,
		"source_evidence_digest": snapshot.SourceEvidenceDigest,
		"retention_class":        snapshot.RetentionClass,
	}
	mounts := payload["mounts"].(map[string]any)
	snapshotMounts, ok := mounts["content_snapshots"].(map[string]any)
	if !ok {
		snapshotMounts = map[string]any{}
		mounts["content_snapshots"] = snapshotMounts
	}
	snapshotMounts[store.ContentSnapshotKindSkills] = map[string]any{
		"mount_name":  "skills_snapshot",
		"type":        "bind",
		"mode":        "ro",
		"exact":       true,
		"source":      snapshot.ImmutableHostPath,
		"destination": snapshot.MountDestination,
		"digest":      snapshot.Digest,
	}
	workspace := payload["data_volumes"].(map[string]any)["workspace"].(map[string]any)
	workspace["platform_content_mount_scope"] = "immutable_content_snapshots"
	canonical, err := store.CanonicalGenerationPlanPayload(payload)
	if err != nil {
		t.Fatalf("canonical generation plan with snapshot: %v", err)
	}
	planDigest := store.GenerationPlanDigest(canonical)
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE generation_plans
SET canonical_payload = ?,
    plan_digest = ?
WHERE generation_id = ?`, string(canonical), planDigest, generationID); err != nil {
		t.Fatalf("update generation plan snapshot payload: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE generation_plan_projections
SET plan_digest = ?
WHERE generation_id = ?`, planDigest, generationID); err != nil {
		t.Fatalf("update projection plan digests for snapshot payload: %v", err)
	}
	return snapshot
}

func writeServerContentSnapshotFixture(t *testing.T, root string) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(root, "skill"), 0o755); err != nil {
		t.Fatalf("create content snapshot fixture: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "README.md"), []byte("skills fixture\n"), 0o644); err != nil {
		t.Fatalf("write content snapshot readme: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "skill", "SKILL.md"), []byte("# Fixture\n"), 0o644); err != nil {
		t.Fatalf("write content snapshot skill: %v", err)
	}
	digest, err := contentSnapshotPathDigest(root)
	if err != nil {
		t.Fatalf("digest content snapshot fixture: %v", err)
	}
	return digest
}
