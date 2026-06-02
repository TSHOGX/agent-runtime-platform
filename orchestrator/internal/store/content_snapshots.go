package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	ContentSnapshotKindSkills           = "skills"
	ContentSnapshotKindManagedSettings  = "managed_settings"
	ContentSnapshotSkillsMount          = "/harness-skills"
	ContentSnapshotManagedSettingsMount = "/harness-managed-settings"
)

type ContentSnapshotRecord struct {
	Kind                 string
	Digest               string
	ImmutableHostPath    string
	MountDestination     string
	SourceEvidenceDigest string
	RetentionClass       string
	CreatedAt            time.Time
}

type RetainedContentSnapshotReference struct {
	GenerationID         string
	GenerationStatus     string
	PlanDigest           string
	Kind                 string
	Digest               string
	ImmutableHostPath    string
	MountDestination     string
	SourceEvidenceDigest string
	RetentionClass       string
}

type StoreContentSnapshotParams struct {
	Kind                 string
	Digest               string
	ImmutableHostPath    string
	MountDestination     string
	SourceEvidenceDigest string
	RetentionClass       string
	Now                  time.Time
}

func (s *Store) StoreContentSnapshot(ctx context.Context, p StoreContentSnapshotParams) (ContentSnapshotRecord, error) {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	p.Kind = strings.TrimSpace(p.Kind)
	p.Digest = strings.TrimSpace(p.Digest)
	p.ImmutableHostPath = strings.TrimSpace(p.ImmutableHostPath)
	p.MountDestination = strings.TrimSpace(p.MountDestination)
	p.SourceEvidenceDigest = strings.TrimSpace(p.SourceEvidenceDigest)
	p.RetentionClass = strings.TrimSpace(p.RetentionClass)
	if err := validateContentSnapshotParams(p); err != nil {
		return ContentSnapshotRecord{}, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ContentSnapshotRecord{}, err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `
INSERT INTO content_snapshots (
  snapshot_kind, snapshot_digest, immutable_host_path, mount_destination,
  source_evidence_digest, retention_class, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(snapshot_kind, snapshot_digest) DO NOTHING`,
		p.Kind, p.Digest, p.ImmutableHostPath, p.MountDestination,
		p.SourceEvidenceDigest, p.RetentionClass, formatTime(p.Now)); err != nil {
		return ContentSnapshotRecord{}, err
	}
	record, err := getContentSnapshotTx(ctx, tx, p.Kind, p.Digest)
	if err != nil {
		return ContentSnapshotRecord{}, err
	}
	if record.ImmutableHostPath != p.ImmutableHostPath ||
		record.MountDestination != p.MountDestination ||
		record.SourceEvidenceDigest != p.SourceEvidenceDigest ||
		record.RetentionClass != p.RetentionClass {
		return ContentSnapshotRecord{}, fmt.Errorf("content snapshot already exists with different immutable payload")
	}
	if err := tx.Commit(); err != nil {
		return ContentSnapshotRecord{}, err
	}
	return record, nil
}

func (s *Store) GetContentSnapshot(ctx context.Context, kind, digest string) (ContentSnapshotRecord, error) {
	return getContentSnapshotTx(ctx, s.db, strings.TrimSpace(kind), strings.TrimSpace(digest))
}

func (s *Store) ListContentSnapshots(ctx context.Context, kind string) ([]ContentSnapshotRecord, error) {
	kind = strings.TrimSpace(kind)
	if kind != "" && !knownContentSnapshotKind(kind) {
		return nil, fmt.Errorf("unsupported content snapshot kind %q", kind)
	}
	query := `
SELECT snapshot_kind, snapshot_digest, immutable_host_path, mount_destination,
       source_evidence_digest, retention_class, created_at
FROM content_snapshots`
	args := []any{}
	if kind != "" {
		query += `
WHERE snapshot_kind = ?`
		args = append(args, kind)
	}
	query += `
ORDER BY snapshot_kind, snapshot_digest`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	records := []ContentSnapshotRecord{}
	for rows.Next() {
		record, err := scanContentSnapshot(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func (s *Store) ListRetainedContentSnapshotReferences(ctx context.Context) ([]RetainedContentSnapshotReference, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT p.generation_id, g.status, p.plan_digest, p.canonical_payload
FROM generation_plans p
JOIN runtime_generations g ON g.generation_id = p.generation_id
ORDER BY p.generation_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	references := []RetainedContentSnapshotReference{}
	for rows.Next() {
		var generationID, generationStatus, planDigest, canonicalPayload string
		if err := rows.Scan(&generationID, &generationStatus, &planDigest, &canonicalPayload); err != nil {
			return nil, err
		}
		if err := VerifyGenerationPlanDigest([]byte(canonicalPayload), planDigest); err != nil {
			return nil, fmt.Errorf("generation plan %s: %w", generationID, err)
		}
		refs, err := retainedContentSnapshotReferencesFromPlan([]byte(canonicalPayload))
		if err != nil {
			return nil, fmt.Errorf("generation plan %s content snapshot references: %w", generationID, err)
		}
		for _, ref := range refs {
			ref.GenerationID = generationID
			ref.GenerationStatus = generationStatus
			ref.PlanDigest = planDigest
			references = append(references, ref)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(references, func(i, j int) bool {
		left, right := references[i], references[j]
		if left.Kind != right.Kind {
			return left.Kind < right.Kind
		}
		if left.Digest != right.Digest {
			return left.Digest < right.Digest
		}
		return left.GenerationID < right.GenerationID
	})
	return references, nil
}

func validateContentSnapshotParams(p StoreContentSnapshotParams) error {
	if !knownContentSnapshotKind(p.Kind) {
		return fmt.Errorf("unsupported content snapshot kind %q", p.Kind)
	}
	if p.Digest == "" || !strings.HasPrefix(p.Digest, "sha256:") {
		return fmt.Errorf("content snapshot digest is required")
	}
	if p.ImmutableHostPath == "" {
		return fmt.Errorf("content snapshot immutable host path is required")
	}
	if !filepath.IsAbs(p.ImmutableHostPath) {
		return fmt.Errorf("content snapshot immutable host path must be absolute")
	}
	if p.MountDestination == "" {
		return fmt.Errorf("content snapshot mount destination is required")
	}
	if !filepath.IsAbs(p.MountDestination) {
		return fmt.Errorf("content snapshot mount destination must be absolute")
	}
	if p.Kind == ContentSnapshotKindSkills && p.MountDestination != ContentSnapshotSkillsMount {
		return fmt.Errorf("skills content snapshot mount destination must be %s", ContentSnapshotSkillsMount)
	}
	if p.Kind == ContentSnapshotKindManagedSettings && p.MountDestination != ContentSnapshotManagedSettingsMount {
		return fmt.Errorf("managed settings content snapshot mount destination must be %s", ContentSnapshotManagedSettingsMount)
	}
	if p.SourceEvidenceDigest == "" || !strings.HasPrefix(p.SourceEvidenceDigest, "sha256:") {
		return fmt.Errorf("content snapshot source evidence digest is required")
	}
	if p.RetentionClass == "" {
		return fmt.Errorf("content snapshot retention class is required")
	}
	return nil
}

func retainedContentSnapshotReferencesFromPlan(canonicalPayload []byte) ([]RetainedContentSnapshotReference, error) {
	var object map[string]any
	decoder := json.NewDecoder(strings.NewReader(string(canonicalPayload)))
	decoder.UseNumber()
	if err := decoder.Decode(&object); err != nil {
		return nil, err
	}
	rawSnapshots, ok := object["content_snapshots"].(map[string]any)
	if !ok {
		return nil, nil
	}
	references := []RetainedContentSnapshotReference{}
	for kind, rawSnapshot := range rawSnapshots {
		if rawSnapshot == nil {
			continue
		}
		snapshot, ok := rawSnapshot.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("content_snapshots.%s must be an object or null", kind)
		}
		ref := RetainedContentSnapshotReference{
			Kind:                 strings.TrimSpace(contentSnapshotPlanString(snapshot["kind"])),
			Digest:               strings.TrimSpace(contentSnapshotPlanString(snapshot["digest"])),
			ImmutableHostPath:    strings.TrimSpace(contentSnapshotPlanString(snapshot["immutable_host_path"])),
			MountDestination:     strings.TrimSpace(contentSnapshotPlanString(snapshot["mount_destination"])),
			SourceEvidenceDigest: strings.TrimSpace(contentSnapshotPlanString(snapshot["source_evidence_digest"])),
			RetentionClass:       strings.TrimSpace(contentSnapshotPlanString(snapshot["retention_class"])),
		}
		if ref.Kind != strings.TrimSpace(kind) {
			return nil, fmt.Errorf("content_snapshots.%s.kind must be %s", kind, strings.TrimSpace(kind))
		}
		if err := validateContentSnapshotParams(StoreContentSnapshotParams{
			Kind:                 ref.Kind,
			Digest:               ref.Digest,
			ImmutableHostPath:    ref.ImmutableHostPath,
			MountDestination:     ref.MountDestination,
			SourceEvidenceDigest: ref.SourceEvidenceDigest,
			RetentionClass:       ref.RetentionClass,
		}); err != nil {
			return nil, fmt.Errorf("content_snapshots.%s: %w", kind, err)
		}
		references = append(references, ref)
	}
	return references, nil
}

func contentSnapshotPlanString(value any) string {
	if s, ok := value.(string); ok {
		return s
	}
	return ""
}

func knownContentSnapshotKind(kind string) bool {
	switch strings.TrimSpace(kind) {
	case ContentSnapshotKindSkills, ContentSnapshotKindManagedSettings:
		return true
	default:
		return false
	}
}

func getContentSnapshotTx(ctx context.Context, db dbRunner, kind, digest string) (ContentSnapshotRecord, error) {
	row := db.QueryRowContext(ctx, `
SELECT snapshot_kind, snapshot_digest, immutable_host_path, mount_destination,
       source_evidence_digest, retention_class, created_at
FROM content_snapshots
WHERE snapshot_kind = ?
  AND snapshot_digest = ?`, strings.TrimSpace(kind), strings.TrimSpace(digest))
	return scanContentSnapshot(row)
}

func scanContentSnapshot(row scanner) (ContentSnapshotRecord, error) {
	var record ContentSnapshotRecord
	var createdAt string
	err := row.Scan(
		&record.Kind, &record.Digest, &record.ImmutableHostPath,
		&record.MountDestination, &record.SourceEvidenceDigest,
		&record.RetentionClass, &createdAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return ContentSnapshotRecord{}, err
		}
		return ContentSnapshotRecord{}, err
	}
	record.CreatedAt = parseTime(createdAt)
	if !knownContentSnapshotKind(record.Kind) {
		return ContentSnapshotRecord{}, fmt.Errorf("unsupported content snapshot kind %q", record.Kind)
	}
	if record.Digest == "" || !strings.HasPrefix(record.Digest, "sha256:") {
		return ContentSnapshotRecord{}, fmt.Errorf("content snapshot digest is invalid")
	}
	if record.SourceEvidenceDigest == "" || !strings.HasPrefix(record.SourceEvidenceDigest, "sha256:") {
		return ContentSnapshotRecord{}, fmt.Errorf("content snapshot source evidence digest is invalid")
	}
	if record.ImmutableHostPath == "" || !filepath.IsAbs(record.ImmutableHostPath) {
		return ContentSnapshotRecord{}, fmt.Errorf("content snapshot immutable host path is invalid")
	}
	if record.MountDestination == "" || !filepath.IsAbs(record.MountDestination) {
		return ContentSnapshotRecord{}, fmt.Errorf("content snapshot mount destination is invalid")
	}
	if record.Kind == ContentSnapshotKindSkills && record.MountDestination != ContentSnapshotSkillsMount {
		return ContentSnapshotRecord{}, fmt.Errorf("skills content snapshot mount destination must be %s", ContentSnapshotSkillsMount)
	}
	if record.Kind == ContentSnapshotKindManagedSettings && record.MountDestination != ContentSnapshotManagedSettingsMount {
		return ContentSnapshotRecord{}, fmt.Errorf("managed settings content snapshot mount destination must be %s", ContentSnapshotManagedSettingsMount)
	}
	if record.RetentionClass == "" {
		return ContentSnapshotRecord{}, fmt.Errorf("content snapshot retention class is invalid")
	}
	return record, nil
}
