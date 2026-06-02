package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStoreContentSnapshotIsImmutableAndListsByKind(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	now := time.Date(2026, 6, 2, 2, 3, 4, 5, time.UTC)
	skills, err := st.StoreContentSnapshot(ctx, StoreContentSnapshotParams{
		Kind:                 ContentSnapshotKindSkills,
		Digest:               "sha256:skills",
		ImmutableHostPath:    "/var/lib/harness/content/skills/sha256-skills",
		MountDestination:     "/harness-skills",
		SourceEvidenceDigest: "sha256:skills-source",
		RetentionClass:       "generation_plan",
		Now:                  now,
	})
	if err != nil {
		t.Fatalf("store skills snapshot: %v", err)
	}
	if skills.CreatedAt != now ||
		skills.Kind != ContentSnapshotKindSkills ||
		skills.Digest != "sha256:skills" {
		t.Fatalf("unexpected skills snapshot: %+v", skills)
	}
	managed, err := st.StoreContentSnapshot(ctx, StoreContentSnapshotParams{
		Kind:                 ContentSnapshotKindManagedSettings,
		Digest:               "sha256:settings",
		ImmutableHostPath:    "/var/lib/harness/content/managed-settings/sha256-settings",
		MountDestination:     "/harness-managed-settings",
		SourceEvidenceDigest: "sha256:settings-source",
		RetentionClass:       "generation_plan",
		Now:                  now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("store managed settings snapshot: %v", err)
	}

	replayed, err := st.StoreContentSnapshot(ctx, StoreContentSnapshotParams{
		Kind:                 ContentSnapshotKindSkills,
		Digest:               "sha256:skills",
		ImmutableHostPath:    "/var/lib/harness/content/skills/sha256-skills",
		MountDestination:     "/harness-skills",
		SourceEvidenceDigest: "sha256:skills-source",
		RetentionClass:       "generation_plan",
		Now:                  now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("idempotent snapshot store: %v", err)
	}
	if replayed.CreatedAt != skills.CreatedAt || replayed.ImmutableHostPath != skills.ImmutableHostPath {
		t.Fatalf("idempotent replay changed record: got %+v want %+v", replayed, skills)
	}

	_, err = st.StoreContentSnapshot(ctx, StoreContentSnapshotParams{
		Kind:                 ContentSnapshotKindSkills,
		Digest:               "sha256:skills",
		ImmutableHostPath:    "/var/lib/harness/content/skills/changed",
		MountDestination:     "/harness-skills",
		SourceEvidenceDigest: "sha256:skills-source",
		RetentionClass:       "generation_plan",
	})
	if err == nil || !strings.Contains(err.Error(), "different immutable payload") {
		t.Fatalf("expected immutable snapshot rejection, got %v", err)
	}

	loaded, err := st.GetContentSnapshot(ctx, ContentSnapshotKindSkills, "sha256:skills")
	if err != nil {
		t.Fatalf("get snapshot: %v", err)
	}
	if loaded.Digest != skills.Digest || loaded.MountDestination != skills.MountDestination {
		t.Fatalf("loaded snapshot mismatch: got %+v want %+v", loaded, skills)
	}

	records, err := st.ListContentSnapshots(ctx, "")
	if err != nil {
		t.Fatalf("list snapshots: %v", err)
	}
	if len(records) != 2 ||
		records[0].Kind != managed.Kind ||
		records[1].Kind != skills.Kind {
		t.Fatalf("snapshots not ordered by kind/digest: %+v; managed=%+v skills=%+v", records, managed, skills)
	}
	skillRecords, err := st.ListContentSnapshots(ctx, ContentSnapshotKindSkills)
	if err != nil {
		t.Fatalf("list skills snapshots: %v", err)
	}
	if len(skillRecords) != 1 || skillRecords[0].Digest != skills.Digest {
		t.Fatalf("skills snapshots = %+v want %+v", skillRecords, skills)
	}
}

func TestStoreContentSnapshotRejectsInvalidReferences(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	base := StoreContentSnapshotParams{
		Kind:                 ContentSnapshotKindSkills,
		Digest:               "sha256:skills",
		ImmutableHostPath:    "/var/lib/harness/content/skills/sha256-skills",
		MountDestination:     "/harness-skills",
		SourceEvidenceDigest: "sha256:skills-source",
		RetentionClass:       "generation_plan",
	}
	for _, tc := range []struct {
		name string
		edit func(*StoreContentSnapshotParams)
		want string
	}{
		{name: "kind", edit: func(p *StoreContentSnapshotParams) { p.Kind = "workspace" }, want: "unsupported content snapshot kind"},
		{name: "digest", edit: func(p *StoreContentSnapshotParams) { p.Digest = "skills" }, want: "content snapshot digest is required"},
		{name: "host path", edit: func(p *StoreContentSnapshotParams) { p.ImmutableHostPath = "relative/skills" }, want: "immutable host path must be absolute"},
		{name: "mount destination", edit: func(p *StoreContentSnapshotParams) { p.MountDestination = "harness-skills" }, want: "mount destination must be absolute"},
		{name: "skills mount destination", edit: func(p *StoreContentSnapshotParams) { p.MountDestination = "/other-skills" }, want: "skills content snapshot mount destination must be /harness-skills"},
		{name: "source evidence", edit: func(p *StoreContentSnapshotParams) { p.SourceEvidenceDigest = "source" }, want: "source evidence digest is required"},
		{name: "retention class", edit: func(p *StoreContentSnapshotParams) { p.RetentionClass = "" }, want: "retention class is required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params := base
			tc.edit(&params)
			_, err := st.StoreContentSnapshot(ctx, params)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected %q, got %v", tc.want, err)
			}
		})
	}
	if _, err := st.ListContentSnapshots(ctx, "workspace"); err == nil ||
		!strings.Contains(err.Error(), "unsupported content snapshot kind") {
		t.Fatalf("expected invalid list kind error, got %v", err)
	}
}

func TestGetContentSnapshotRejectsCorruptSkillsMountDestination(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if _, err := st.StoreContentSnapshot(ctx, StoreContentSnapshotParams{
		Kind:                 ContentSnapshotKindSkills,
		Digest:               "sha256:skills",
		ImmutableHostPath:    "/var/lib/harness/content/skills/sha256-skills",
		MountDestination:     ContentSnapshotSkillsMount,
		SourceEvidenceDigest: "sha256:skills-source",
		RetentionClass:       "generation_plan",
	}); err != nil {
		t.Fatalf("store skills snapshot: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE content_snapshots
SET mount_destination = '/other-skills'
WHERE snapshot_kind = ?
  AND snapshot_digest = ?`, ContentSnapshotKindSkills, "sha256:skills"); err != nil {
		t.Fatalf("corrupt skills mount destination: %v", err)
	}
	if _, err := st.GetContentSnapshot(ctx, ContentSnapshotKindSkills, "sha256:skills"); err == nil ||
		!strings.Contains(err.Error(), "skills content snapshot mount destination must be /harness-skills") {
		t.Fatalf("expected corrupt skills mount rejection, got %v", err)
	}
}

func TestGetContentSnapshotNoRows(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if _, err := st.GetContentSnapshot(ctx, ContentSnapshotKindSkills, "sha256:missing"); err != sql.ErrNoRows {
		t.Fatalf("GetContentSnapshot missing error=%v want sql.ErrNoRows", err)
	}
}
